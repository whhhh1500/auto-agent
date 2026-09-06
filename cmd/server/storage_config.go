package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	artifactmigrationadapter "github.com/cc-auto-agent/harness-core/pkg/adapter/sql/artifactmigration"
	artifactstoragemigration "github.com/cc-auto-agent/harness-core/pkg/adapter/storage/artifactmigration"
	artifactmigration "github.com/cc-auto-agent/harness-core/pkg/app/artifactmigration"
	appstorageconfig "github.com/cc-auto-agent/harness-core/pkg/app/storageconfig"
	"github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
)

type storageRuntime struct {
	resources          *storage.DynamicObjectStore
	embeddedResources  storage.ObjectStore
	mu                 sync.RWMutex
	resourceActive     appstorageconfig.ActiveState
	sessionActive      appstorageconfig.ActiveState
	sessionObjects     storage.ObjectStore
	migrationRepo      artifactmigration.Repository
	migration          *artifactmigration.Coordinator
	activationCallback artifactmigration.ActivationCallback
	triggerMigration   func()
	migrationMu        sync.Mutex
}

var _ appstorageconfig.RuntimePort = (*storageRuntime)(nil)

func newStorageRuntime(resources *storage.DynamicObjectStore, embeddedResources storage.ObjectStore) *storageRuntime {
	return &storageRuntime{resources: resources, embeddedResources: embeddedResources, resourceActive: appstorageconfig.ActiveState{Backend: appstorageconfig.BackendEmbedded}}
}

func newStorageRuntimeWithMigration(resources *storage.DynamicObjectStore, embedded storage.ObjectStore, db *sql.DB, dialect storage.SQLDialect, owner string, onActivated artifactmigration.ActivationCallback) (*storageRuntime, error) {
	runtime := newStorageRuntime(resources, embedded)
	if db == nil {
		return runtime, nil
	}
	repo, err := artifactmigrationadapter.New(db, dialect)
	if err != nil {
		return nil, err
	}
	coordinator, err := artifactmigration.NewCoordinator(repo, owner, 30*time.Second)
	if err != nil {
		return nil, err
	}
	runtime.migrationRepo, runtime.migration, runtime.activationCallback = repo, coordinator, onActivated
	return runtime, nil
}

func (runtime *storageRuntime) PrepareResourceMigration(ctx context.Context, configuration appstorageconfig.StoredConfiguration) (appstorageconfig.MigrationProgress, error) {
	if runtime.migration == nil {
		return appstorageconfig.MigrationProgress{}, fmt.Errorf("artifact migration coordinator is unavailable")
	}
	target, err := storage.NewS3ObjectStore(s3ConfigFromStored(configuration))
	if err != nil {
		return appstorageconfig.MigrationProgress{}, err
	}
	if err := target.EnsureBucket(ctx); err != nil {
		return appstorageconfig.MigrationProgress{}, err
	}
	binding, err := artifactstoragemigration.NewBinding(runtime.resources, target, "s3", storage.ArtifactCopyOptions{})
	if err != nil {
		return appstorageconfig.MigrationProgress{}, err
	}
	status, err := runtime.migration.Prepare(ctx, artifactmigration.PrepareRequest{
		DesiredRevision: configuration.DesiredRevision,
		Source:          artifactmigration.BackendIdentity{Backend: artifactmigration.BackendLocal, Identity: "resources"},
		Target:          artifactmigration.BackendIdentity{Backend: artifactmigration.BackendS3, Identity: "configured"},
		Runtime:         binding, Prefix: "",
		OnActivated: runtime.activationCallback,
	})
	if err != nil {
		return appstorageconfig.MigrationProgress{}, err
	}
	runtime.migrationMu.Lock()
	trigger := runtime.triggerMigration
	runtime.migrationMu.Unlock()
	if status.Migration.State != artifactmigration.StateS3Active && trigger != nil {
		trigger()
	}
	return migrationProgress(status.Migration), nil
}

func (runtime *storageRuntime) MigrationStatus(ctx context.Context) (appstorageconfig.MigrationProgress, bool, error) {
	if runtime.migrationRepo == nil {
		return appstorageconfig.MigrationProgress{}, false, nil
	}
	migration, found, err := runtime.migrationRepo.Load(ctx)
	if err != nil || !found {
		return appstorageconfig.MigrationProgress{}, found, err
	}
	return migrationProgress(migration), true, nil
}

func (runtime *storageRuntime) RunMigrationOnce(ctx context.Context) (appstorageconfig.MigrationProgress, error) {
	if runtime.migration == nil {
		return appstorageconfig.MigrationProgress{}, nil
	}
	status, err := runtime.migration.RunOnce(ctx)
	return migrationProgress(status.Migration), err
}

func (runtime *storageRuntime) CancelResourceMigration(ctx context.Context, expectedRevision string) (appstorageconfig.MigrationProgress, error) {
	if runtime.migration == nil {
		return appstorageconfig.MigrationProgress{}, nil
	}
	status, err := runtime.migration.CancelCurrent(ctx, expectedRevision)
	return migrationProgress(status.Migration), err
}

func (runtime *storageRuntime) SetMigrationTrigger(trigger func()) {
	runtime.migrationMu.Lock()
	defer runtime.migrationMu.Unlock()
	runtime.triggerMigration = trigger
}

func migrationProgress(m artifactmigration.Migration) appstorageconfig.MigrationProgress {
	return appstorageconfig.MigrationProgress{State: string(m.State), CopiedObjects: m.CopiedObjects, CopiedBytes: m.CopiedBytes, VerifiedObjects: m.VerifiedObjects, VerifiedBytes: m.VerifiedBytes, ErrorCode: m.ErrorCode, CreatedAt: m.CreatedAt, UpdatedAt: m.UpdatedAt, VerifiedAt: m.VerifiedAt, ActivatedAt: m.ActivatedAt}
}

func (runtime *storageRuntime) Active(_ context.Context, kind appstorageconfig.Kind) (appstorageconfig.ActiveState, error) {
	runtime.mu.RLock()
	defer runtime.mu.RUnlock()
	switch kind {
	case appstorageconfig.KindResources:
		return runtime.resourceActive, nil
	case appstorageconfig.KindSessions:
		return runtime.sessionActive, nil
	default:
		return appstorageconfig.ActiveState{}, fmt.Errorf("%w: %q", appstorageconfig.ErrInvalidKind, kind)
	}
}

func (runtime *storageRuntime) Apply(ctx context.Context, kind appstorageconfig.Kind, configuration appstorageconfig.StoredConfiguration) (appstorageconfig.ActiveState, error) {
	if kind != appstorageconfig.KindResources {
		return appstorageconfig.ActiveState{}, fmt.Errorf("storage runtime does not hot-apply %s", kind)
	}
	if runtime.resources == nil {
		return appstorageconfig.ActiveState{}, fmt.Errorf("resources dynamic store is nil")
	}
	normalized, err := appstorageconfig.Normalize(kind, configuration)
	if err != nil {
		return appstorageconfig.ActiveState{}, err
	}
	if normalized.Status == appstorageconfig.StatusInactive {
		return appstorageconfig.ActiveState{}, fmt.Errorf("resources storage configuration is inactive")
	}
	if normalized.Backend == appstorageconfig.BackendEmbedded && runtime.migrationRepo != nil {
		if migration, found, statusErr := runtime.MigrationStatus(ctx); statusErr != nil {
			return appstorageconfig.ActiveState{}, statusErr
		} else if found && migration.State == string(artifactmigration.StateS3Active) {
			return appstorageconfig.ActiveState{}, fmt.Errorf("resources cannot switch from s3 to embedded without a reverse migration")
		}
	}
	var candidate storage.ObjectStore
	var label string
	switch normalized.Backend {
	case appstorageconfig.BackendEmbedded:
		candidate, label = runtime.embeddedResources, string(appstorageconfig.BackendEmbedded)
		if candidate == nil {
			return appstorageconfig.ActiveState{}, fmt.Errorf("resources embedded store is nil")
		}
	case appstorageconfig.BackendS3:
		candidate, err = storage.NewS3ObjectStore(s3ConfigFromStored(normalized))
		if err != nil {
			return appstorageconfig.ActiveState{}, fmt.Errorf("build resources s3 backend: %w", err)
		}
		if err := candidate.(*storage.S3ObjectStore).EnsureBucket(ctx); err != nil {
			return appstorageconfig.ActiveState{}, fmt.Errorf("ensure resources s3 bucket: %w", err)
		}
		label = string(appstorageconfig.BackendS3)
	default:
		return appstorageconfig.ActiveState{}, fmt.Errorf("backend %q is not available for resources", normalized.Backend)
	}
	if err := runtime.resources.TrySwap(candidate, label); err != nil {
		return appstorageconfig.ActiveState{}, fmt.Errorf("swap resources backend: %w", err)
	}
	active := activeStateForStorage(normalized)
	runtime.mu.Lock()
	runtime.resourceActive = active
	runtime.mu.Unlock()
	return active, nil
}

func (runtime *storageRuntime) setSessionActive(active appstorageconfig.ActiveState, objects storage.ObjectStore) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	runtime.sessionActive = active
	runtime.sessionObjects = objects
}

// legacyResourceInputFromEnv translates the legacy environment surface into a
// candidate configuration. The caller must invoke it only after confirming
// that the resources row is absent. A resource-specific variable wins as a
// group: a partial resource group is deliberately not completed from the
// session group, so it reaches the typed validator and fails closed.
func legacyResourceInputFromEnv(lookup func(string) (string, bool)) (*appstorageconfig.LegacyInput, error) {
	if lookup == nil {
		return nil, nil
	}
	const resourcePrefix = "HARNESS_RESOURCE_S3_"
	if envAny(lookup,
		resourcePrefix+"ENDPOINT",
		resourcePrefix+"REGION",
		resourcePrefix+"BUCKET",
		resourcePrefix+"ACCESS_KEY_ID",
		resourcePrefix+"SECRET_ACCESS_KEY",
		resourcePrefix+"PATH_STYLE",
	) {
		configuration, err := s3ConfigurationFromEnv(lookup, resourcePrefix, false)
		if err != nil {
			return nil, err
		}
		return &appstorageconfig.LegacyInput{Configuration: configuration}, nil
	}
	if envAny(lookup, resourceFallbackS3EnvNames()...) {
		configuration, err := s3ConfigurationFromEnv(lookup, "HARNESS_S3_", false)
		if err != nil {
			return nil, err
		}
		return &appstorageconfig.LegacyInput{Configuration: configuration}, nil
	}
	return nil, nil
}

// legacySessionInputFromEnv translates the legacy session environment surface
// into a candidate configuration. Any S3 variable takes precedence over the
// directory variable, including an incomplete S3 group; the typed validator
// then reports the incomplete configuration instead of silently changing the
// backend.
func legacySessionInputFromEnv(lookup func(string) (string, bool)) (*appstorageconfig.LegacyInput, error) {
	if lookup == nil {
		return nil, nil
	}
	if envAny(lookup, sessionS3EnvNames()...) {
		configuration, err := s3ConfigurationFromEnv(lookup, "HARNESS_S3_", true)
		if err != nil {
			return nil, err
		}
		return &appstorageconfig.LegacyInput{Configuration: configuration}, nil
	}
	if value, present := lookup("HARNESS_SESSION_DIR"); present {
		return &appstorageconfig.LegacyInput{Configuration: appstorageconfig.StoredConfiguration{
			Backend: appstorageconfig.BackendFile,
			Path:    value,
		}}, nil
	}
	return nil, nil
}

func resourceFallbackS3EnvNames() []string {
	return []string{
		"HARNESS_S3_ENDPOINT",
		"HARNESS_S3_REGION",
		"HARNESS_S3_BUCKET",
		"HARNESS_S3_ACCESS_KEY_ID",
		"HARNESS_S3_SECRET_ACCESS_KEY",
		"HARNESS_S3_PATH_STYLE",
	}
}

func sessionS3EnvNames() []string {
	return append(resourceFallbackS3EnvNames(), "HARNESS_S3_DISABLE_CONDITIONAL_WRITES")
}

func envAny(lookup func(string) (string, bool), names ...string) bool {
	for _, name := range names {
		if _, present := lookup(name); present {
			return true
		}
	}
	return false
}

func s3ConfigurationFromEnv(lookup func(string) (string, bool), prefix string, includeConditionalWrites bool) (appstorageconfig.StoredConfiguration, error) {
	value := func(suffix string) string {
		v, _ := lookup(prefix + suffix)
		return v
	}
	pathStyle, err := strictBoolEnv(lookup, prefix+"PATH_STYLE")
	if err != nil {
		return appstorageconfig.StoredConfiguration{}, err
	}
	conditionalWrites := false
	if includeConditionalWrites {
		conditionalWrites, err = strictBoolEnv(lookup, prefix+"DISABLE_CONDITIONAL_WRITES")
		if err != nil {
			return appstorageconfig.StoredConfiguration{}, err
		}
	}
	configuration := appstorageconfig.StoredConfiguration{
		Backend:   appstorageconfig.BackendS3,
		Endpoint:  value("ENDPOINT"),
		Region:    value("REGION"),
		Bucket:    value("BUCKET"),
		AccessKey: value("ACCESS_KEY_ID"),
		SecretKey: value("SECRET_ACCESS_KEY"),
		PathStyle: pathStyle,
	}
	if includeConditionalWrites {
		configuration.DisableConditionalWrites = conditionalWrites
	}
	return configuration, nil
}

func strictBoolEnv(lookup func(string) (string, bool), name string) (bool, error) {
	value, present := lookup(name)
	if !present || value == "" || value == "false" {
		return false, nil
	}
	if value == "true" {
		return true, nil
	}
	return false, fmt.Errorf("environment variable %s must be empty, false, or true", name)
}

// resourceBackendFromConfig builds the resources backend without provisioning
// it. In particular, the S3 constructor validates and stores credentials but
// does not contact the bucket; bucket creation/health belongs to activation.
func resourceBackendFromConfig(configuration appstorageconfig.StoredConfiguration, embedded storage.ObjectStore) (storage.ObjectStore, appstorageconfig.ActiveState, string, error) {
	normalized, err := appstorageconfig.Normalize(appstorageconfig.KindResources, configuration)
	if err != nil {
		return nil, appstorageconfig.ActiveState{}, "", err
	}
	if normalized.Status == appstorageconfig.StatusInactive {
		return nil, appstorageconfig.ActiveState{}, "", fmt.Errorf("resources storage configuration is inactive")
	}
	if normalized.DisableConditionalWrites {
		return nil, appstorageconfig.ActiveState{}, "", fmt.Errorf("resources storage cannot disable conditional writes")
	}
	active := activeStateForStorage(normalized)
	switch normalized.Backend {
	case appstorageconfig.BackendEmbedded:
		if embedded == nil {
			return nil, appstorageconfig.ActiveState{}, "", fmt.Errorf("resources embedded store is nil")
		}
		return embedded, active, string(normalized.Backend), nil
	case appstorageconfig.BackendS3:
		objects, err := storage.NewS3ObjectStore(s3ConfigFromStored(normalized))
		if err != nil {
			return nil, appstorageconfig.ActiveState{}, "", fmt.Errorf("build resources s3 backend: %w", err)
		}
		return objects, active, string(normalized.Backend), nil
	default:
		return nil, appstorageconfig.ActiveState{}, "", fmt.Errorf("backend %q is not available for resources", normalized.Backend)
	}
}

// sessionBackendFromConfig builds the sessions backend without applying it.
// The embedded argument is the already-owned SQL session store; this helper
// never creates a second database connection.
func sessionBackendFromConfig(configuration appstorageconfig.StoredConfiguration, embedded core.SessionStore) (core.SessionStore, appstorageconfig.ActiveState, string, error) {
	return sessionBackendFromConfigWithObject(configuration, embedded, nil)
}

func sessionBackendFromConfigWithObject(configuration appstorageconfig.StoredConfiguration, embedded core.SessionStore, objects storage.ObjectStore) (core.SessionStore, appstorageconfig.ActiveState, string, error) {
	normalized, err := appstorageconfig.Normalize(appstorageconfig.KindSessions, configuration)
	if err != nil {
		return nil, appstorageconfig.ActiveState{}, "", err
	}
	if normalized.Status == appstorageconfig.StatusInactive {
		return nil, appstorageconfig.ActiveState{}, "", fmt.Errorf("sessions storage configuration is inactive")
	}
	active := activeStateForStorage(normalized)
	switch normalized.Backend {
	case appstorageconfig.BackendEmbedded:
		if embedded == nil {
			return nil, appstorageconfig.ActiveState{}, "", fmt.Errorf("sessions embedded store is nil")
		}
		return embedded, active, string(normalized.Backend), nil
	case appstorageconfig.BackendFile:
		store, err := storage.NewFileSessionStore(normalized.Path)
		if err != nil {
			return nil, appstorageconfig.ActiveState{}, "", fmt.Errorf("build sessions file backend: %w", err)
		}
		return store, active, string(normalized.Backend), nil
	case appstorageconfig.BackendS3:
		if objects == nil {
			objects, err = storage.NewS3ObjectStore(s3ConfigFromStored(normalized))
			if err != nil {
				return nil, appstorageconfig.ActiveState{}, "", fmt.Errorf("build sessions s3 backend: %w", err)
			}
		}
		store, err := storage.NewS3SessionStore(objects)
		if err != nil {
			return nil, appstorageconfig.ActiveState{}, "", fmt.Errorf("build sessions s3 backend: %w", err)
		}
		store.DisableConditionalWrites = normalized.DisableConditionalWrites
		return store, active, string(normalized.Backend), nil
	default:
		return nil, appstorageconfig.ActiveState{}, "", fmt.Errorf("backend %q is not available for sessions", normalized.Backend)
	}
}

func s3ConfigFromStored(configuration appstorageconfig.StoredConfiguration) storage.S3Config {
	return storage.S3Config{
		Endpoint:  configuration.Endpoint,
		Region:    configuration.Region,
		Bucket:    configuration.Bucket,
		AccessKey: configuration.AccessKey,
		SecretKey: configuration.SecretKey,
		PathStyle: configuration.PathStyle,
	}
}

func activeStateForStorage(configuration appstorageconfig.StoredConfiguration) appstorageconfig.ActiveState {
	return appstorageconfig.ActiveState{
		Backend:  configuration.Backend,
		Revision: configuration.DesiredRevision,
	}
}

func logStorageBackend(label string, configuration appstorageconfig.StoredConfiguration, active appstorageconfig.ActiveState) {
	switch configuration.Backend {
	case appstorageconfig.BackendS3:
		log.Printf("%s: s3 bucket %s active revision %s", label, configuration.Bucket, active.Revision)
	case appstorageconfig.BackendFile:
		log.Printf("%s: file path %s active revision %s", label, configuration.Path, active.Revision)
	default:
		log.Printf("%s: embedded active revision %s", label, active.Revision)
	}
}

func evidenceForActive(evidence appstorageconfig.ResolutionEvidence, active appstorageconfig.ActiveState) appstorageconfig.ResolutionEvidence {
	evidence.Status = appstorageconfig.StatusActive
	evidence.RestartPending = false
	evidence.ErrorCode = ""
	evidence.ActiveType = active.Backend
	evidence.ActiveRevision = active.Revision
	return evidence
}
