package storageconfig

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	appidentity "github.com/whhhh1500/auto-agent/pkg/app/identity"
	"github.com/whhhh1500/auto-agent/pkg/app/secretview"
)

// Service serializes operations per storage kind within this process. The
// repository CAS remains the cross-process ordering authority.
type Service struct {
	repository Repository
	runtime    RuntimePort
	locks      [2]sync.Mutex
}

var _ UseCases = (*Service)(nil)

func NewService(repository Repository, runtime RuntimePort) (*Service, error) {
	if repository == nil {
		return nil, fmt.Errorf("storage configuration service requires a repository")
	}
	if runtime == nil {
		return nil, fmt.Errorf("storage configuration service requires a runtime port")
	}
	return &Service{repository: repository, runtime: runtime}, nil
}

func (service *Service) Get(ctx context.Context, command GetCommand) (GetResult, error) {
	unlock, err := service.lock(command.Kind)
	if err != nil {
		return GetResult{}, err
	}
	defer unlock()
	if err := requireAdmin(command.Actor); err != nil {
		return GetResult{}, err
	}
	active, err := service.runtime.Active(ctx, command.Kind)
	if err != nil {
		return GetResult{}, err
	}
	desired, found, err := service.repository.Load(ctx, command.Kind)
	if err != nil {
		return GetResult{}, err
	}
	result := GetResult{Found: found, Active: active}
	if migrationRuntime, ok := service.runtime.(MigrationRuntimePort); ok && command.Kind == KindResources {
		migration, migrationFound, migrationErr := migrationRuntime.MigrationStatus(ctx)
		if migrationErr != nil {
			return GetResult{}, migrationErr
		}
		if migrationFound {
			result.Migration = migration
		}
	}
	if !found {
		return result, nil
	}
	desired, err = Normalize(command.Kind, desired)
	if err != nil {
		return GetResult{}, err
	}
	result.Config = viewOf(desired)
	evidence, evidenceFound, err := service.repository.LoadEvidence(ctx, command.Kind)
	if err != nil {
		return GetResult{}, err
	}
	// Evidence is a projection and may lag a desired update. Never surface a
	// stale projection as if it described the current desired revision.
	if evidenceFound && evidence.DesiredRevision == desired.DesiredRevision {
		result.Evidence = evidence
	}
	return result, nil
}

func (service *Service) Put(ctx context.Context, command PutCommand) (PutResult, error) {
	unlock, err := service.lock(command.Kind)
	if err != nil {
		return PutResult{}, err
	}
	defer unlock()
	if err := requireAdmin(command.Actor); err != nil {
		return PutResult{}, err
	}
	if command.ClearSecret && command.SecretKey != nil {
		return PutResult{}, fmt.Errorf("%w: secret_key and clear_secret cannot be combined", ErrInvalidInput)
	}
	status := command.Status
	if status == "" {
		status = StatusActive
	}
	if status != StatusActive && status != StatusInactive {
		return PutResult{}, fmt.Errorf("%w: status %q is not writable", ErrInvalidInput, status)
	}
	current, found, err := service.repository.Load(ctx, command.Kind)
	if err != nil {
		return PutResult{}, err
	}
	if found {
		current, err = Normalize(command.Kind, current)
		if err != nil {
			return PutResult{}, err
		}
		if current.Status == StatusInactive {
			return PutResult{}, ErrInactiveConfiguration
		}
	}
	expected := command.ExpectedRevision
	if expected == "" && found {
		expected = current.DesiredRevision
	}
	if command.ExpectedRevision != "" && (!found || command.ExpectedRevision != current.DesiredRevision) {
		return PutResult{}, ErrConflict
	}
	secret := current.SecretKey
	switch {
	case command.ClearSecret:
		secret = ""
	case command.SecretKey != nil:
		secret = *command.SecretKey
	case !found:
		secret = ""
	}
	disableConditionalWrites := command.DisableConditionalWrites
	if found && !command.DisableConditionalWritesPresent {
		disableConditionalWrites = current.DisableConditionalWrites
	}
	desired := StoredConfiguration{
		Backend: command.Backend, Path: command.Path, Endpoint: command.Endpoint,
		Region: command.Region, Bucket: command.Bucket, AccessKey: current.AccessKey,
		SecretKey: secret, PathStyle: command.PathStyle, DisableConditionalWrites: disableConditionalWrites,
		Source: SourceDB, Status: status,
	}
	// Non-empty programmatic commands remain backward compatible; typed HTTP
	// requests use AccessKeyPresent to distinguish an omitted field.
	if command.AccessKeyPresent || command.AccessKey != "" {
		desired.AccessKey = command.AccessKey
	}
	desired, err = PrepareCreate(command.Kind, desired)
	if err != nil {
		return PutResult{}, err
	}
	active, err := service.runtime.Active(ctx, command.Kind)
	if err != nil {
		return PutResult{}, err
	}
	if found && command.Kind == KindResources && active.Backend == BackendS3 && !sameBackendConfiguration(current, desired) {
		// The persisted S3 credentials are the only restart-recovery source for
		// the active route. Do not overwrite them until a dedicated reverse/
		// rebind migration exists.
		return PutResult{}, fmt.Errorf("%w: active S3 resources require a dedicated rebind migration", ErrUnsupportedTransition)
	}
	// Re-submitting the exact S3 configuration is safe only while the current
	// active route is already S3, or while its non-terminal migration is still
	// in progress. In either case, preserve the existing desired revision so a
	// retry cannot restart a copy. Failed/cancelled migrations intentionally do
	// not enter this fast path: re-saving them creates a new generation.
	if found && command.Kind == KindResources && desired.Backend == BackendS3 && sameBackendConfiguration(current, desired) {
		migrationRuntime, hasMigration := service.runtime.(MigrationRuntimePort)
		migration, migrationFound := MigrationProgress{}, false
		if hasMigration {
			migration, migrationFound, err = migrationRuntime.MigrationStatus(ctx)
			if err != nil {
				return PutResult{}, err
			}
		}
		pending := migrationFound && migration.State != "" && migration.State != "s3_active" && migration.State != "apply_failed" && migration.State != "cancelled"
		if active.Backend == BackendS3 || pending {
			fallbackStatus := StatusActive
			if pending {
				fallbackStatus = StatusMigrationPending
			}
			result := PutResult{Config: viewOf(current), Evidence: ResolutionEvidence{Kind: command.Kind, Source: SourceDB, Status: fallbackStatus, Found: true, DesiredType: current.Backend, DesiredRevision: current.DesiredRevision, ActiveType: active.Backend, ActiveRevision: active.Revision, ObservedAt: nowUTC()}}
			if migrationFound {
				result.Migration = migration
			}
			if evidence, evidenceFound, loadErr := service.repository.LoadEvidence(ctx, command.Kind); loadErr != nil {
				return PutResult{}, loadErr
			} else if evidenceFound && evidence.DesiredRevision == current.DesiredRevision {
				result.Evidence = evidence
			}
			return result, nil
		}
	}
	// A resources switch back to embedded must fence a pending migration before
	// changing the desired row. The expected revision prevents cancelling a
	// newer migration created by another process between our read and CAS.
	if found && command.Kind == KindResources {
		if migrationRuntime, ok := service.runtime.(MigrationCancellationPort); ok {
			cancelPending := desired.Backend == BackendEmbedded
			if statusRuntime, hasStatus := service.runtime.(MigrationRuntimePort); hasStatus {
				progress, migrationFound, statusErr := statusRuntime.MigrationStatus(ctx)
				if statusErr != nil {
					return PutResult{}, statusErr
				}
				cancelPending = migrationFound && progress.State != "" && progress.State != "s3_active" && progress.State != "apply_failed" && progress.State != "cancelled"
			}
			if cancelPending {
				if _, cancelErr := migrationRuntime.CancelResourceMigration(ctx, current.DesiredRevision); cancelErr != nil {
					return PutResult{}, errors.Join(ErrApplyFailed, cancelErr)
				}
			}
		}
	}
	updated, err := service.repository.UpdateIfRevision(ctx, command.Kind, expected, desired)
	if err != nil {
		return PutResult{}, err
	}
	if !updated {
		return PutResult{}, ErrConflict
	}
	evidence := ResolutionEvidence{
		Kind: command.Kind, Source: SourceDB, Found: true,
		DesiredType: desired.Backend, DesiredRevision: desired.DesiredRevision,
		ActiveType: active.Backend, ActiveRevision: active.Revision,
		ObservedAt: nowUTC(),
	}
	var primary error
	result := PutResult{Config: viewOf(desired)}
	switch {
	case desired.Status == StatusInactive:
		evidence.Status = StatusInactive
		evidence.ErrorCode = ErrorCodeInactive
	case command.Kind == KindSessions:
		evidence.Status = StatusRestartPending
		evidence.RestartPending = true
	default:
		if migrationRuntime, ok := service.runtime.(MigrationRuntimePort); ok && desired.Backend == BackendS3 {
			migration, prepareErr := migrationRuntime.PrepareResourceMigration(ctx, desired)
			if prepareErr != nil {
				evidence.Status = StatusApplyFailed
				evidence.ErrorCode = ErrorCodeApplyFailed
				primary = errors.Join(ErrApplyFailed, prepareErr)
			} else {
				result.Migration = migration
				evidence.Status = ConfigStatus(migrationStateToStatus(migration.State))
				evidence.RestartPending = false
			}
		} else {
			applied, applyErr := service.runtime.Apply(ctx, command.Kind, desired)
			if applyErr != nil {
				evidence.Status = StatusApplyFailed
				evidence.ErrorCode = ErrorCodeApplyFailed
				primary = errors.Join(ErrApplyFailed, applyErr)
			} else {
				active = applied
				evidence.ActiveType = active.Backend
				evidence.ActiveRevision = active.Revision
				evidence.Status = StatusActive
			}
		}
	}
	evidenceSaved, saveErr := service.repository.SaveEvidenceIfDesiredRevision(ctx, command.Kind, evidence)
	if saveErr != nil {
		primary = errors.Join(primary, saveErr)
	} else if !evidenceSaved {
		primary = errors.Join(primary, ErrConflict)
	}
	result.Evidence = evidence
	return result, primary
}

func migrationStateToStatus(state string) string {
	if state == "s3_active" {
		return string(StatusActive)
	}
	return string(StatusMigrationPending)
}

func sameBackendConfiguration(left, right StoredConfiguration) bool {
	return left.Backend == right.Backend && left.Path == right.Path && left.Endpoint == right.Endpoint &&
		left.Region == right.Region && left.Bucket == right.Bucket && left.AccessKey == right.AccessKey &&
		left.SecretKey == right.SecretKey && left.PathStyle == right.PathStyle &&
		left.DisableConditionalWrites == right.DisableConditionalWrites
}

var ErrApplyFailed = errors.New("storage configuration apply failed")

func (service *Service) lock(kind Kind) (func(), error) {
	var lock *sync.Mutex
	switch kind {
	case KindResources:
		lock = &service.locks[0]
	case KindSessions:
		lock = &service.locks[1]
	default:
		return nil, fmt.Errorf("%w: %q", ErrInvalidKind, kind)
	}
	lock.Lock()
	return lock.Unlock, nil
}

func requireAdmin(actor appidentity.AdminActor) error {
	if actor.Role != appidentity.RoleAdmin {
		return ErrForbidden
	}
	return nil
}

func viewOf(configuration StoredConfiguration) ConfigView {
	return ConfigView{
		Backend: configuration.Backend, Path: configuration.Path,
		Endpoint: configuration.Endpoint, Region: configuration.Region,
		Bucket:    configuration.Bucket,
		PathStyle: configuration.PathStyle, DisableConditionalWrites: configuration.DisableConditionalWrites, Source: configuration.Source,
		Status: configuration.Status, DesiredRevision: configuration.DesiredRevision,
		HasSecret:        configuration.SecretKey != "",
		SecretPreview:    preview(configuration.SecretKey),
		HasAccessKey:     configuration.AccessKey != "",
		AccessKeyPreview: preview(configuration.AccessKey),
	}
}

func preview(value string) string {
	if value == "" {
		return ""
	}
	return secretview.Preview(value)
}

func nowUTC() time.Time { return time.Now().UTC() }
