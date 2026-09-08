package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	storageconfigadapter "github.com/whhhh1500/auto-agent/pkg/adapter/storageconfig"
	artifactmigration "github.com/whhhh1500/auto-agent/pkg/app/artifactmigration"
	appsettings "github.com/whhhh1500/auto-agent/pkg/app/settings"
	appstorageconfig "github.com/whhhh1500/auto-agent/pkg/app/storageconfig"
	"github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/storage"
)

// configuredStorage is the startup-selected storage surface. It deliberately
// contains concrete values rather than lookup methods, so composition remains
// explicit and no runtime service locator is introduced.
type configuredStorage struct {
	Sessions             core.SessionStore
	Resources            storage.ObjectStore
	EmbeddedResources    storage.ObjectStore
	UseCases             appstorageconfig.UseCases
	SessionObjects       storage.ObjectStore
	RunResourceMigration func(context.Context)
	SetMigrationTrigger  func(func())
}

// configureStorage resolves the database-authoritative resource and session
// configurations, creates their selected backends, and records the resulting
// active-state evidence. The legacy environment provider remains lazy: it is
// invoked only after its corresponding persisted row is confirmed absent.
func configureStorage(
	ctx context.Context,
	dataDir string,
	settingsRepository appsettings.Repository,
	embeddedSessions core.SessionStore,
	lookupEnv func(string) (string, bool),
) (configuredStorage, error) {
	return configureStorageWithMigration(ctx, dataDir, settingsRepository, embeddedSessions, lookupEnv, nil, storage.SQLDialectSQLite)
}

func configureStorageWithMigration(
	ctx context.Context, dataDir string, settingsRepository appsettings.Repository,
	embeddedSessions core.SessionStore, lookupEnv func(string) (string, bool), db *sql.DB, dialect storage.SQLDialect,
) (configuredStorage, error) {
	embeddedResources, err := storage.NewFileObjectStore(filepath.Join(dataDir, "resources"))
	if err != nil {
		return configuredStorage{}, err
	}
	resources := storage.NewDynamicObjectStore(embeddedResources, "embedded")
	var runtime *storageRuntime
	if db != nil {
		runtime, err = newStorageRuntimeWithMigration(resources, embeddedResources, db, dialect, "server-artifact-migration", nil)
	} else {
		runtime = newStorageRuntime(resources, embeddedResources)
	}
	if err != nil {
		return configuredStorage{}, err
	}
	repository, err := storageconfigadapter.New(settingsRepository)
	if err != nil {
		return configuredStorage{}, err
	}
	if runtime.migration != nil {
		runtime.activationCallback = func(callbackCtx context.Context, migration artifactmigration.Migration) error {
			// The coordinator has already durably activated and swapped the router
			// before invoking this callback. Reflect that fact in process state first;
			// projection persistence is observability and must not roll the runtime
			// back to local if it fails.
			runtime.mu.Lock()
			runtime.resourceActive = appstorageconfig.ActiveState{Backend: appstorageconfig.BackendS3, Revision: migration.DesiredRevision}
			runtime.mu.Unlock()
			_, err := repository.SaveEvidenceIfDesiredRevision(callbackCtx, appstorageconfig.KindResources, appstorageconfig.ResolutionEvidence{
				Kind: appstorageconfig.KindResources, Source: appstorageconfig.SourceDB, Status: appstorageconfig.StatusActive, Found: true,
				DesiredType: appstorageconfig.BackendS3, ActiveType: appstorageconfig.BackendS3,
				DesiredRevision: migration.DesiredRevision, ActiveRevision: migration.DesiredRevision, ObservedAt: time.Now().UTC(),
			})
			return err
		}
	}
	// Compatibility-only rollback helper loadPersistedStorageConfig remains
	// outside the production path; startup resolves typed storage rows below.
	resolver := appstorageconfig.Resolver{Repository: repository}
	resourceResolution, err := resolver.ResolveWithLegacyProvider(
		ctx, appstorageconfig.KindResources,
		func(context.Context, appstorageconfig.Kind) (*appstorageconfig.LegacyInput, error) {
			return legacyResourceInputFromEnv(lookupEnv)
		}, appstorageconfig.ActiveState{},
	)
	if err != nil {
		return configuredStorage{}, err
	}
	var resourceActive appstorageconfig.ActiveState
	var resourceMigration appstorageconfig.MigrationProgress
	var resourcePrepareErr error
	if resourceResolution.Desired.Backend == appstorageconfig.BackendS3 && runtime.migration != nil {
		status, prepareErr := runtime.PrepareResourceMigration(ctx, resourceResolution.Desired)
		if prepareErr != nil {
			resourcePrepareErr = prepareErr
			resourceActive = appstorageconfig.ActiveState{Backend: appstorageconfig.BackendEmbedded}
		} else {
			resourceMigration = status
			if status.State == string(artifactmigration.StateS3Active) {
				resourceActive = appstorageconfig.ActiveState{Backend: appstorageconfig.BackendS3, Revision: resourceResolution.Desired.DesiredRevision}
			} else {
				resourceActive = appstorageconfig.ActiveState{Backend: appstorageconfig.BackendEmbedded}
			}
		}
	} else {
		resourceActive, err = runtime.Apply(ctx, appstorageconfig.KindResources, resourceResolution.Desired)
		if err != nil {
			return configuredStorage{}, err
		}
	}
	runtime.mu.Lock()
	runtime.resourceActive = resourceActive
	runtime.mu.Unlock()
	logStorageBackend("resource store", resourceResolution.Desired, resourceActive)

	sessionResolution, err := resolver.ResolveWithLegacyProvider(
		ctx, appstorageconfig.KindSessions,
		func(context.Context, appstorageconfig.Kind) (*appstorageconfig.LegacyInput, error) {
			return legacySessionInputFromEnv(lookupEnv)
		}, appstorageconfig.ActiveState{},
	)
	if err != nil {
		return configuredStorage{}, err
	}
	var sessionObjects storage.ObjectStore
	if sessionResolution.Desired.Backend == appstorageconfig.BackendS3 {
		sessionObjects, err = storage.NewS3ObjectStore(s3ConfigFromStored(sessionResolution.Desired))
		if err != nil {
			return configuredStorage{}, err
		}
		if err := sessionObjects.(*storage.S3ObjectStore).EnsureBucket(ctx); err != nil {
			return configuredStorage{}, err
		}
	}
	sessions, sessionActive, _, err := sessionBackendFromConfigWithObject(sessionResolution.Desired, embeddedSessions, sessionObjects)
	if err != nil {
		return configuredStorage{}, err
	}
	runtime.setSessionActive(sessionActive, sessionObjects)
	logStorageBackend("session store", sessionResolution.Desired, sessionActive)
	useCases, err := appstorageconfig.NewService(repository, runtime)
	if err != nil {
		return configuredStorage{}, err
	}

	// Startup construction succeeded; refresh evidence with the actual active
	// revisions after the runtime objects have been selected.
	resourceEvidence := startupResourceEvidence(resourceResolution.Evidence, resourceActive, resourceMigration, resourcePrepareErr)
	saved, err := repository.SaveEvidenceIfDesiredRevision(ctx, appstorageconfig.KindResources, resourceEvidence)
	if err != nil {
		return configuredStorage{}, err
	}
	if resourceResolution.Evidence.Found && !saved {
		return configuredStorage{}, fmt.Errorf("resource storage configuration changed during startup")
	}
	sessionEvidence := evidenceForActive(sessionResolution.Evidence, sessionActive)
	saved, err = repository.SaveEvidenceIfDesiredRevision(ctx, appstorageconfig.KindSessions, sessionEvidence)
	if err != nil {
		return configuredStorage{}, err
	}
	if sessionResolution.Evidence.Found && !saved {
		return configuredStorage{}, fmt.Errorf("session storage configuration changed during startup")
	}

	configured := configuredStorage{
		Sessions:          sessions,
		Resources:         resources,
		EmbeddedResources: embeddedResources,
		UseCases:          useCases,
		SessionObjects:    sessionObjects,
	}
	if runtime.migration != nil {
		configured.SetMigrationTrigger = runtime.SetMigrationTrigger
		configured.RunResourceMigration = func(runCtx context.Context) {
			backoff := 100 * time.Millisecond
			for {
				progress, found, loadErr := runtime.MigrationStatus(runCtx)
				if loadErr != nil || !found || progress.State == string(artifactmigration.StateS3Active) || artifactmigration.State(progress.State).Terminal() {
					return
				}
				_, runErr := runtime.RunMigrationOnce(runCtx)
				if runErr != nil {
					if !errors.Is(runErr, artifactmigration.ErrMigrationBusy) {
						return
					}
					timer := time.NewTimer(backoff)
					select {
					case <-runCtx.Done():
						timer.Stop()
						return
					case <-timer.C:
					}
					if backoff < 2*time.Second {
						backoff *= 2
						if backoff > 2*time.Second {
							backoff = 2 * time.Second
						}
					}
					continue
				}
				latest, ok, loadErr := runtime.MigrationStatus(runCtx)
				if loadErr != nil || !ok || latest.State == string(artifactmigration.StateS3Active) || artifactmigration.State(latest.State).Terminal() {
					return
				}
			}
		}
	}
	return configured, nil
}

func startupResourceEvidence(previous appstorageconfig.ResolutionEvidence, active appstorageconfig.ActiveState, migration appstorageconfig.MigrationProgress, prepareErr error) appstorageconfig.ResolutionEvidence {
	if prepareErr != nil {
		previous.Status = appstorageconfig.StatusApplyFailed
		previous.ErrorCode = appstorageconfig.ErrorCodeApplyFailed
		previous.RestartPending = false
		previous.ActiveType, previous.ActiveRevision = active.Backend, active.Revision
		return previous
	}
	if migration.State != "" && migration.State != string(artifactmigration.StateS3Active) {
		if migration.State == string(artifactmigration.StateApplyFailed) {
			previous.Status = appstorageconfig.StatusApplyFailed
			previous.ErrorCode = appstorageconfig.ErrorCodeApplyFailed
		} else if migration.State == string(artifactmigration.StateCancelled) {
			previous.Status = appstorageconfig.StatusApplyFailed
			previous.ErrorCode = appstorageconfig.ErrorCodeApplyFailed
		} else {
			previous.Status = appstorageconfig.StatusMigrationPending
		}
		previous.RestartPending = false
		previous.ActiveType, previous.ActiveRevision = active.Backend, active.Revision
		return previous
	}
	return evidenceForActive(previous, active)
}
