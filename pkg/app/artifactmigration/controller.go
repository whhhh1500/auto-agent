package artifactmigration

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	MutationRecordTimeout = 5 * time.Second
	// DurableOperationTimeout bounds cancellation-independent cleanup and
	// projection calls. It prevents a detached database or callback operation
	// from surviving a request indefinitely.
	DurableOperationTimeout = 5 * time.Second
	MaxReplayPageSize       = 256
	MaxVerificationPasses   = 8
	maxCancelCASAttempts    = 3
)

var errActivationDeltaTooLarge = errors.New("artifact activation delta exceeds bounded limit")

// ActivationCallback projects an already activated migration to another
// evidence surface. It runs after durable activation and route switching; a
// projection failure never rolls either back.
type ActivationCallback func(context.Context, Migration) error

// PrepareRequest injects one runtime binding. The durable package never
// constructs an object-store client or reads backend configuration itself.
type PrepareRequest struct {
	MigrationID     string
	DesiredRevision string
	Source          BackendIdentity
	Target          BackendIdentity
	Runtime         ArtifactRuntime
	Prefix          string
	OnActivated     ActivationCallback
}

// Status is a safe snapshot of one coordinator. It deliberately excludes
// object keys, backend credentials, and target implementation details.
type Status struct {
	Migration Migration
	Prepared  bool
	Fatal     bool
}

// Coordinator has no goroutine or timer. The server explicitly calls Prepare
// to bind runtime stores, then invokes RunOnce from its own bounded worker.
type Coordinator struct {
	repository Repository
	owner      string
	ttl        time.Duration

	mu      sync.RWMutex
	runtime *coordinatorRuntime
	fatal   bool
}

type coordinatorRuntime struct {
	migrationID string
	runtime     ArtifactRuntime
	copy        ArtifactCopy
	prefix      string
	activate    ActivationCallback
	recorder    *mutationRecorder
}

// NewCoordinator validates the durable worker identity. It does not claim a
// lease, access a store, or start any background work.
func NewCoordinator(repository Repository, owner string, ttl time.Duration) (*Coordinator, error) {
	if repository == nil || len(owner) == 0 || len(owner) > MaxLeaseOwnerBytes || !validTTL(ttl) {
		return nil, ErrInvalidMigration
	}
	return &Coordinator{repository: repository, owner: owner, ttl: ttl}, nil
}

// Prepare creates, replaces, or resumes the desired migration and installs a
// foreground mutation recorder on the supplied DynamicObjectStore. It keeps
// the current route authoritative; it never copies or switches synchronously.
func (coordinator *Coordinator) Prepare(ctx context.Context, request PrepareRequest) (Status, error) {
	if coordinator == nil || request.Runtime == nil {
		return Status{}, ErrInvalidMigration
	}
	if request.MigrationID == "" {
		id, err := newMigrationID()
		if err != nil {
			return Status{}, err
		}
		request.MigrationID = id
	}
	candidate := Migration{ID: request.MigrationID, DesiredRevision: request.DesiredRevision, Source: request.Source, Target: request.Target, State: StateSyncing, Generation: 1}
	if err := ValidateMigration(candidate); err != nil {
		return Status{}, err
	}
	current, found, err := coordinator.repository.Load(controllerContext(ctx))
	if err != nil {
		return Status{}, err
	}
	if !found {
		current, err = coordinator.repository.CompareAndSwap(ctx, 0, candidate)
		if err != nil {
			return Status{}, err
		}
	} else {
		if current.State.Terminal() {
			// A terminal failure/cancellation can outlive its process-local
			// candidate attachment. Clearing only the matching ID never changes
			// the current route and lets a later desired revision begin cleanly.
			request.Runtime.Cancel(current.ID)
		}
		if current.DesiredRevision != request.DesiredRevision {
			if current.State == StateS3Active {
				// The active route is authoritative. A desired-revision replacement
				// cannot pretend it is still local and start a local-to-S3 copy.
				return Status{Migration: current}, fmt.Errorf("%w: migration is already s3_active", ErrMigrationConflict)
			}
			if !current.State.Terminal() {
				cancelled := current
				cancelled.State = StateCancelled
				cancelled.LeaseOwner, cancelled.LeaseExpiresAt = "", time.Time{}
				current, err = coordinator.repository.CompareAndSwap(ctx, current.StoreRevision, cancelled)
				if err != nil {
					return Status{}, err
				}
				// Do not detach the recorder until the old generation is durably
				// terminal. A CAS conflict means another worker/process still owns
				// that generation and must continue recording mutations.
				request.Runtime.Cancel(current.ID)
			}
			candidate.Generation = current.Generation + 1
			candidate.StoreRevision = current.StoreRevision
			current, err = coordinator.repository.CompareAndSwap(ctx, current.StoreRevision, candidate)
			if err != nil {
				return Status{}, err
			}
		}
	}
	if current.State == StateS3Active {
		if err := request.Runtime.RestoreActive(ctx); err != nil {
			return Status{}, err
		}
		return Status{Migration: current}, nil
	}
	if current.State.Terminal() {
		return Status{Migration: current}, nil
	}
	recorder := &mutationRecorder{repository: coordinator.repository, token: current.MutationToken()}
	copier, err := request.Runtime.Bind(ctx, current.ID, recorder)
	if err != nil {
		return Status{}, err
	}
	coordinator.mu.Lock()
	coordinator.runtime = &coordinatorRuntime{migrationID: current.ID, runtime: request.Runtime, copy: copier, prefix: request.Prefix, activate: request.OnActivated, recorder: recorder}
	coordinator.fatal = false
	coordinator.mu.Unlock()
	return Status{Migration: current, Prepared: true}, nil
}

// RunOnce completes one migration pass. Copying is streaming and page-bounded;
// no loop persists after this call returns. A restart runs Reconcile again.
func (coordinator *Coordinator) RunOnce(ctx context.Context) (Status, error) {
	runtime, fatal := coordinator.currentRuntime()
	if runtime == nil {
		return Status{}, ErrInvalidMigration
	}
	migration, found, err := coordinator.repository.Load(controllerContext(ctx))
	if err != nil || !found || migration.ID != runtime.migrationID {
		if err != nil {
			return Status{}, err
		}
		return Status{}, ErrMigrationConflict
	}
	if fatal || runtime.recorder.Fatal() {
		coordinator.clearRuntimeMigration(migration.ID)
		return Status{Migration: migration, Prepared: true, Fatal: true}, ErrMutationConflict
	}
	if migration.State.Terminal() {
		// A same-ID Prepare retry can reuse the existing DynamicObjectStore
		// attachment (and therefore its original recorder). If that original
		// recorder terminally fails after the retry, this coordinator observes
		// the durable terminal state rather than its own fatal bit. Clear the
		// matching runtime attachment here so local remains usable and a later
		// desired revision can begin.
		coordinator.clearRuntimeMigration(migration.ID)
		return Status{Migration: migration, Prepared: true}, nil
	}
	lease, err := coordinator.repository.ClaimLease(ctx, coordinator.owner, coordinator.ttl)
	if err != nil {
		return Status{Migration: migration, Prepared: true}, err
	}
	defer func() {
		releaseCtx, cancel := durableContext(ctx)
		defer cancel()
		_ = coordinator.repository.ReleaseLease(releaseCtx, lease)
	}()
	if migration, found, err = coordinator.repository.Load(ctx); err != nil || !found {
		if err != nil {
			return Status{}, err
		}
		return Status{}, ErrMigrationConflict
	}
	switch migration.State {
	case StateSyncing:
		progress := &copyProgress{repository: coordinator.repository, lease: lease, migration: migration, renewAfter: coordinator.ttl / 2, ttl: coordinator.ttl}
		if _, err := runtime.copy.Copy(ctx, runtime.prefix, migration.ListingCursor, progress.record); err != nil {
			return coordinator.fail(ctx, progress.lease, progress.migration, "copy_failed", err)
		}
		migration = progress.migration
		lease = progress.lease
		next := migration
		next.State = StateCatchingUp
		migration, err = coordinator.repository.UpdateLeaseHeld(ctx, lease, migration.StoreRevision, next)
		if err != nil {
			return Status{Migration: migration, Prepared: true}, err
		}
		fallthrough
	case StateCatchingUp:
		lease, err = coordinator.replay(ctx, runtime.copy, lease, migration, migration.MutationHighWater)
		if err != nil {
			return coordinator.fail(ctx, lease, migration, "journal_replay_failed", err)
		}
		migration = migrationWithLease(migration, lease)
		next := migration
		next.State = StateVerifying
		migration, err = coordinator.repository.UpdateLeaseHeld(ctx, lease, migration.StoreRevision, next)
		if err != nil {
			return Status{Migration: migration, Prepared: true}, err
		}
		fallthrough
	case StateVerifying:
		repairedFromAuthoritativeSource := false
		for pass := 0; pass < MaxVerificationPasses; pass++ {
			latest, found, loadErr := coordinator.repository.Load(ctx)
			if loadErr != nil || !found {
				if loadErr != nil {
					return Status{}, loadErr
				}
				return Status{}, ErrMigrationConflict
			}
			migration = latest
			base := migration.MutationHighWater
			verification := &verifyProgress{repository: coordinator.repository, lease: lease, renewAfter: coordinator.ttl / 2, ttl: coordinator.ttl}
			stats, reconcileErr := runtime.copy.Reconcile(ctx, runtime.prefix, verification.record)
			if reconcileErr != nil {
				latest, found, loadErr := coordinator.repository.Load(ctx)
				if loadErr != nil {
					return Status{}, loadErr
				}
				if found && latest.MutationHighWater > base {
					lease, err = coordinator.replay(ctx, runtime.copy, verification.lease, latest, latest.MutationHighWater)
					if err != nil {
						return coordinator.fail(ctx, lease, latest, "journal_replay_failed", err)
					}
					continue
				}
				if !repairedFromAuthoritativeSource {
					// A process can crash after an authoritative local mutation but
					// before its journal append. Re-copying every source key is safe:
					// local remains authoritative and this only repairs missing or
					// mismatched target bodies. It deliberately never removes target
					// extras; a subsequent reconciliation still fails closed because
					// their ownership cannot be proved from the local source.
					repair := &copyProgress{repository: coordinator.repository, lease: verification.lease, migration: migrationWithLease(migration, verification.lease), renewAfter: coordinator.ttl / 2, ttl: coordinator.ttl}
					if _, repairErr := runtime.copy.Copy(ctx, runtime.prefix, "", repair.record); repairErr != nil {
						return coordinator.fail(ctx, repair.lease, repair.migration, "reconcile_repair_failed", repairErr)
					}
					lease = repair.lease
					repairedFromAuthoritativeSource = true
					continue
				}
				return coordinator.fail(ctx, verification.lease, migration, "verify_failed", reconcileErr)
			}
			lease = verification.lease
			migration = migrationWithLease(migration, lease)
			next := recordVerifiedStats(migration, uint64(stats.Objects), uint64(stats.Bytes), time.Now().UTC())
			migration, err = coordinator.repository.UpdateLeaseHeld(ctx, lease, migration.StoreRevision, next)
			if err != nil {
				return Status{Migration: migration, Prepared: true}, err
			}
			active, activationErr := coordinator.activate(ctx, runtime, lease, migration, base)
			if errors.Is(activationErr, errActivationDeltaTooLarge) {
				latest, found, loadErr = coordinator.repository.Load(ctx)
				if loadErr != nil || !found {
					if loadErr != nil {
						return Status{}, loadErr
					}
					return Status{}, ErrMigrationConflict
				}
				lease, err = coordinator.replay(ctx, runtime.copy, lease, latest, latest.MutationHighWater)
				if err != nil {
					return coordinator.fail(ctx, lease, latest, "journal_replay_failed", err)
				}
				continue
			}
			if activationErr != nil {
				var projection projectionError
				if errors.As(activationErr, &projection) {
					return Status{Migration: active, Prepared: true}, activationErr
				}
				return coordinator.fail(ctx, lease, migration, "activation_failed", activationErr)
			}
			return Status{Migration: active, Prepared: true}, nil
		}
		return Status{Migration: migration, Prepared: true}, ErrMigrationBusy
	default:
		return Status{Migration: migration, Prepared: true}, ErrInvalidMigration
	}
}

func (coordinator *Coordinator) activate(ctx context.Context, runtime *coordinatorRuntime, lease Lease, migration Migration, base uint64) (Migration, error) {
	err := runtime.runtime.Activate(ctx, migration.ID, func(barrier context.Context, copy ArtifactCopy) error {
		changes, sequences, err := coordinator.finalChanges(barrier, migration.ID, base)
		if err != nil {
			return err
		}
		if err := copy.ApplyAndVerify(barrier, changes); err != nil {
			return err
		}
		if len(sequences) != 0 {
			if err := coordinator.repository.MarkMutationsThrough(barrier, lease, sequences[len(sequences)-1]); err != nil {
				return err
			}
		}
		next := migration
		next.State = StateS3Active
		next.LeaseOwner, next.LeaseExpiresAt = "", time.Time{}
		next.ActivatedAt = time.Now().UTC()
		_, err = coordinator.repository.UpdateLeaseHeld(barrier, lease, migration.StoreRevision, next)
		return err
	})
	if err != nil {
		return migration, err
	}
	loadCtx, cancel := durableContext(ctx)
	active, found, err := coordinator.repository.Load(loadCtx)
	cancel()
	if err != nil || !found {
		if err != nil {
			return migration, err
		}
		return migration, ErrMigrationConflict
	}
	if runtime.activate != nil {
		projectionCtx, cancel := durableContext(ctx)
		err := runtime.activate(projectionCtx, active)
		cancel()
		if err != nil {
			return active, projectionError{err}
		}
	}
	return active, nil
}

func (coordinator *Coordinator) replay(ctx context.Context, copier ArtifactCopy, lease Lease, migration Migration, highWater uint64) (Lease, error) {
	var after uint64
	for after < highWater {
		entries, err := coordinator.repository.ListMutations(ctx, migration.ID, after, MaxReplayPageSize)
		if err != nil {
			return lease, err
		}
		if len(entries) == 0 {
			return lease, ErrMutationConflict
		}
		changes := make(map[string]KeyChange, len(entries))
		for _, entry := range entries {
			if entry.Sequence > highWater {
				break
			}
			changes[entry.ObjectKey] = KeyChange{Key: entry.ObjectKey, Deleted: entry.Operation == MutationDelete}
			after = entry.Sequence
		}
		ordered := make([]KeyChange, 0, len(changes))
		for _, change := range changes {
			ordered = append(ordered, change)
		}
		if err := copier.ApplyAndVerify(ctx, ordered); err != nil {
			return lease, err
		}
		if after != 0 {
			if err := coordinator.repository.MarkMutationsThrough(ctx, lease, after); err != nil {
				return lease, err
			}
		}
		lease, err = renewLeaseIfNeeded(ctx, coordinator.repository, lease, coordinator.ttl/2, coordinator.ttl)
		if err != nil {
			return lease, err
		}
	}
	return lease, nil
}

func (coordinator *Coordinator) finalChanges(ctx context.Context, migrationID string, after uint64) ([]KeyChange, []uint64, error) {
	entries, err := coordinator.repository.ListMutations(ctx, migrationID, after, MaxReplayPageSize)
	if err != nil {
		return nil, nil, err
	}
	if len(entries) == MaxReplayPageSize {
		more, err := coordinator.repository.ListMutations(ctx, migrationID, entries[len(entries)-1].Sequence, 1)
		if err != nil {
			return nil, nil, err
		}
		if len(more) != 0 {
			return nil, nil, fmt.Errorf("%w: %d entries", errActivationDeltaTooLarge, MaxReplayPageSize)
		}
	}
	changes := make(map[string]KeyChange, len(entries))
	sequences := make([]uint64, 0, len(entries))
	for _, entry := range entries {
		changes[entry.ObjectKey] = KeyChange{Key: entry.ObjectKey, Deleted: entry.Operation == MutationDelete}
		sequences = append(sequences, entry.Sequence)
	}
	if len(changes) > MaxArtifactActivationKeys {
		return nil, nil, fmt.Errorf("%w: %d keys", errActivationDeltaTooLarge, MaxArtifactActivationKeys)
	}
	result := make([]KeyChange, 0, len(changes))
	for _, change := range changes {
		result = append(result, change)
	}
	return result, sequences, nil
}

func (coordinator *Coordinator) fail(ctx context.Context, lease Lease, migration Migration, code string, cause error) (Status, error) {
	next := migration
	next.State, next.ErrorCode, next.ErrorDetail = StateApplyFailed, code, "migration operation failed"
	next.LeaseOwner, next.LeaseExpiresAt = "", time.Time{}
	failCtx, cancel := durableContext(ctx)
	defer cancel()
	failed, err := coordinator.repository.UpdateLeaseHeld(failCtx, lease, migration.StoreRevision, next)
	if err != nil {
		return Status{Migration: migration, Prepared: true}, errors.Join(cause, err)
	}
	coordinator.clearRuntimeMigration(failed.ID)
	return Status{Migration: failed, Prepared: true, Fatal: true}, cause
}

// CancelCurrent stops the currently prepared local-to-S3 migration only if it
// still has expectedDesiredRevision. The explicit desired-revision fence keeps
// a stale process from cancelling a different migration that another process
// has already installed in the singleton durable slot. It uses a controller
// CAS rather than a worker lease: once cancellation is durable, every
// in-flight worker write is fenced out by the stale revision or cleared lease.
// A route already durable as s3_active is never switched back to local by this
// control-plane operation.
func (coordinator *Coordinator) CancelCurrent(ctx context.Context, expectedDesiredRevision string) (Status, error) {
	if coordinator == nil {
		return Status{}, ErrInvalidMigration
	}
	if err := validateText("expected desired revision", expectedDesiredRevision, MaxDesiredRevisionBytes, true); err != nil {
		return Status{}, err
	}
	ctx = controllerContext(ctx)
	for attempt := 0; attempt < maxCancelCASAttempts; attempt++ {
		current, found, err := coordinator.repository.Load(ctx)
		if err != nil {
			return Status{}, err
		}
		if !found {
			return Status{}, ErrMigrationConflict
		}
		if current.DesiredRevision != expectedDesiredRevision {
			return Status{Migration: current}, ErrMigrationConflict
		}
		if current.State == StateS3Active {
			return Status{Migration: current}, fmt.Errorf("%w: migration is already s3_active", ErrMigrationConflict)
		}
		if current.State.Terminal() {
			coordinator.clearRuntimeMigration(current.ID)
			return Status{Migration: current}, nil
		}
		next := current
		next.State = StateCancelled
		next.LeaseOwner, next.LeaseExpiresAt = "", time.Time{}
		cancelled, err := coordinator.repository.CompareAndSwap(ctx, current.StoreRevision, next)
		if err == nil {
			coordinator.clearRuntimeMigration(cancelled.ID)
			return Status{Migration: cancelled}, nil
		}
		if !errors.Is(err, ErrMigrationConflict) {
			return Status{Migration: current}, err
		}
	}
	current, found, err := coordinator.repository.Load(ctx)
	if err != nil {
		return Status{}, err
	}
	if found && current.State == StateS3Active {
		return Status{Migration: current}, fmt.Errorf("%w: migration is already s3_active", ErrMigrationConflict)
	}
	if found {
		return Status{Migration: current}, ErrMigrationConflict
	}
	return Status{}, ErrMigrationConflict
}

func (coordinator *Coordinator) currentRuntime() (*coordinatorRuntime, bool) {
	coordinator.mu.RLock()
	defer coordinator.mu.RUnlock()
	return coordinator.runtime, coordinator.fatal
}

func (coordinator *Coordinator) clearRuntimeMigration(migrationID string) {
	coordinator.mu.Lock()
	runtime := coordinator.runtime
	if runtime != nil && runtime.migrationID == migrationID {
		coordinator.runtime = nil
	} else {
		runtime = nil
	}
	coordinator.mu.Unlock()
	if runtime != nil {
		runtime.runtime.Cancel(migrationID)
	}
}

func newMigrationID() (string, error) {
	bytes := make([]byte, 12)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return "mig_" + base64.RawURLEncoding.EncodeToString(bytes), nil
}

func controllerContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func durableContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(controllerContext(ctx)), DurableOperationTimeout)
}

type copyProgress struct {
	repository Repository
	lease      Lease
	migration  Migration
	renewAfter time.Duration
	ttl        time.Duration
}

func (progress *copyProgress) record(ctx context.Context, copied TransferProgress) error {
	lease, err := renewLeaseIfNeeded(ctx, progress.repository, progress.lease, progress.renewAfter, progress.ttl)
	if err != nil {
		return err
	}
	progress.lease = lease
	progress.migration = migrationWithLease(progress.migration, lease)
	next := progress.migration
	next.ListingCursor = copied.LastKey
	next.CopiedObjects += uint64(copied.Objects)
	next.CopiedBytes += uint64(copied.Bytes)
	updated, err := progress.repository.UpdateLeaseHeld(ctx, progress.lease, progress.migration.StoreRevision, next)
	if err != nil {
		return err
	}
	progress.migration = updated
	return nil
}

type verifyProgress struct {
	repository      Repository
	lease           Lease
	renewAfter, ttl time.Duration
}

func (progress *verifyProgress) record(ctx context.Context, _ TransferProgress) error {
	lease, err := renewLeaseIfNeeded(ctx, progress.repository, progress.lease, progress.renewAfter, progress.ttl)
	if err == nil {
		progress.lease = lease
	}
	return err
}

func renewLeaseIfNeeded(ctx context.Context, repository Repository, lease Lease, after, ttl time.Duration) (Lease, error) {
	if after <= 0 || time.Until(lease.ExpiresAt) > after {
		return lease, nil
	}
	return repository.RenewLease(ctx, lease, ttl)
}

func migrationWithLease(migration Migration, lease Lease) Migration {
	migration.LeaseOwner, migration.LeaseExpiresAt = lease.Owner, lease.ExpiresAt
	return migration
}

// recordVerifiedStats stores the current full-reconcile snapshot. Copied
// counters are cumulative lower bounds for data proven present in the target,
// not a unique source-object cardinality: mutations replayed after the bulk
// copy can introduce new keys. Raising the lower bound to the verified
// snapshot preserves the durable invariant without weakening it.
func recordVerifiedStats(migration Migration, objects, bytes uint64, verifiedAt time.Time) Migration {
	migration.VerifiedObjects, migration.VerifiedBytes, migration.VerifiedAt = objects, bytes, verifiedAt
	if migration.CopiedObjects < objects {
		migration.CopiedObjects = objects
	}
	if migration.CopiedBytes < bytes {
		migration.CopiedBytes = bytes
	}
	return migration
}

type mutationRecorder struct {
	repository Repository
	token      MutationToken
	mu         sync.RWMutex
	fatal      bool
}

type projectionError struct{ error }

func (err projectionError) Unwrap() error { return err.error }

func (recorder *mutationRecorder) Fatal() bool {
	recorder.mu.RLock()
	defer recorder.mu.RUnlock()
	return recorder.fatal
}
func (recorder *mutationRecorder) Record(ctx context.Context, mutation MutationEvidence) error {
	operation := MutationPut
	if mutation.Operation == MutationDelete {
		operation = MutationDelete
	}
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), MutationRecordTimeout)
	defer cancel()
	_, err := recorder.repository.AppendMutation(writeCtx, MutationAppend{Token: recorder.token, Operation: operation, ObjectKey: mutation.Key, ETag: mutation.ETag, Digest: mutation.Digest})
	if err == nil {
		return nil
	}
	recorder.mu.Lock()
	recorder.fatal = true
	recorder.mu.Unlock()
	failCtx, failCancel := context.WithTimeout(context.WithoutCancel(ctx), MutationRecordTimeout)
	defer failCancel()
	_ = recorder.repository.FailMutation(failCtx, recorder.token, "mutation_journal_failed")
	return err
}
