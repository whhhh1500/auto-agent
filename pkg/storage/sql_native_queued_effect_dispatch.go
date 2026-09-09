package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/whhhh1500/auto-agent/pkg/app/effectreceipt"
	"github.com/whhhh1500/auto-agent/pkg/core"
)

// NativeQueuedEffectDispatchAdmission is the caller-observed live authority
// that a native queued worker must bind atomically to one already-prepared
// v50 effect receipt. Intent is supplied only when BeginDispatch runs; this
// configuration cannot create an intent or authorize a provider crossing by
// itself.
type NativeQueuedEffectDispatchAdmission struct {
	Fence                      SessionWriteFence
	ExpectedSessionVersion     int64
	ExpectedAuthorizationEpoch int64
}

// NativeQueuedEffectDispatchAdmitter atomically verifies a native queued
// admission before exactly one prepared receipt can cross a provider boundary.
// Construct it from the same SQLSessionStore that owns the queue, Session,
// witness, journal, and v50 receipt ledger.
type NativeQueuedEffectDispatchAdmitter struct {
	store *SQLSessionStore
	input NativeQueuedEffectDispatchAdmission
}

var _ effectreceipt.DispatchAdmitter = (*NativeQueuedEffectDispatchAdmitter)(nil)

// Native queued admission takes several fenced locks before it reaches the
// receipt. Under SQLite, many workers can contend for that one write
// transaction at once. Keep this retry budget local to admission: ordinary
// receipt mutations retain their shorter budget, while a losing native worker
// waits long enough to observe the committed dispatching receipt and return a
// canonical response-lost replay.
const nativeQueuedEffectDispatchSQLRetries = 32

// NewNativeQueuedEffectDispatchAdmitter creates a context-bindable native
// admission port. It validates static fence inputs early; each BeginDispatch
// still locks and revalidates the current epoch and leases in one transaction.
func (s *SQLSessionStore) NewNativeQueuedEffectDispatchAdmitter(input NativeQueuedEffectDispatchAdmission) (*NativeQueuedEffectDispatchAdmitter, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("native queued effect dispatch requires an SQL session store")
	}
	if err := validateNativeQueuedEffectDispatchAdmission(input); err != nil {
		return nil, err
	}
	return &NativeQueuedEffectDispatchAdmitter{store: s, input: input}, nil
}

// BeginDispatch admits only an exact, already-prepared effect receipt. The
// transaction lock order is authorization epoch, queued run control/claim and
// Session lease, Session, tool journal and native witness, then receipt. A
// response-lost retry observes dispatching and returns begun=false; it never
// inserts a receipt and it never asks a caller to dispatch again.
func (a *NativeQueuedEffectDispatchAdmitter) BeginDispatch(ctx context.Context, intent effectreceipt.Intent) (effectreceipt.Record, bool, error) {
	if ctx == nil {
		return effectreceipt.Record{}, false, effectreceipt.ErrInvalidContext
	}
	if a == nil || a.store == nil || a.store.db == nil {
		return effectreceipt.Record{}, false, effectreceipt.ErrInvalidService
	}
	if effectreceipt.ValidateIntent(intent) != nil {
		return effectreceipt.Record{}, false, effectreceipt.ErrInvalidIntent
	}
	if err := validateNativeQueuedEffectDispatchAdmission(a.input); err != nil {
		return effectreceipt.Record{}, false, err
	}
	for attempt := 0; attempt < nativeQueuedEffectDispatchSQLRetries; attempt++ {
		record, begun, retry, err := a.beginDispatch(ctx, intent)
		if !retry {
			return record, begun, err
		}
		if err := waitExternalEffectRetry(ctx, attempt); err != nil {
			return effectreceipt.Record{}, false, err
		}
	}
	return effectreceipt.Record{}, false, effectreceipt.ErrDispatchAdmission
}

func (a *NativeQueuedEffectDispatchAdmitter) beginDispatch(ctx context.Context, intent effectreceipt.Intent) (effectreceipt.Record, bool, bool, error) {
	tx, err := a.store.db.BeginTx(ctx, nil)
	if err != nil {
		return effectreceipt.Record{}, false, isSQLiteMigrationBusy(err), err
	}
	defer func() { _ = tx.Rollback() }()
	input := a.input
	if err := lockAuthorizationEpoch(ctx, tx, a.store.dialect, input.ExpectedAuthorizationEpoch); err != nil {
		return effectreceipt.Record{}, false, isSQLiteMigrationBusy(err), err
	}
	header, committed, err := a.store.acquireCompletedToolResultRecoverySidecarFence(ctx, tx, input.Fence, input.ExpectedSessionVersion)
	if err != nil {
		return effectreceipt.Record{}, false, isSQLiteMigrationBusy(err), err
	}
	if committed != input.ExpectedSessionVersion {
		return effectreceipt.Record{}, false, false, fmt.Errorf("%w: expected %d, found %d", core.ErrSessionConflict, input.ExpectedSessionVersion, committed)
	}
	options, err := completedToolResultRecoverySidecarSessionOptions(header, input.Fence.SessionID)
	if err != nil {
		return effectreceipt.Record{}, false, false, err
	}
	if options.Principal.TenantID != input.Fence.TenantID || options.Principal.SubjectID != input.Fence.SubjectID {
		return effectreceipt.Record{}, false, false, sessionWriteFenceLost("fenced identity does not own the session")
	}

	journal, err := selectNativeQueuedToolInvocation(ctx, tx, a.store.dialect, intent.Invocation)
	if errors.Is(err, errToolInvocationNotFound) {
		return effectreceipt.Record{}, false, false, completedToolResultProofInvalid()
	}
	if err != nil {
		return effectreceipt.Record{}, false, isSQLiteMigrationBusy(err), err
	}
	if !sameToolInvocation(journal.ToolInvocation, intent.Invocation) || journal.State != core.ToolInvocationStarted || journal.Result != nil || journal.ErrorCode != "" {
		return effectreceipt.Record{}, false, false, completedToolResultProofInvalid()
	}
	witness, found, err := loadNativeQueuedToolEffectWitness(ctx, tx, a.store.dialect, intent.Invocation, input.Fence.QueueGeneration, true)
	if err != nil {
		return effectreceipt.Record{}, false, isSQLiteMigrationBusy(err), err
	}
	if !found || !nativeQueuedEffectWitnessAdmits(witness, input, intent.Invocation) {
		return effectreceipt.Record{}, false, false, completedToolResultProofInvalid()
	}

	receipt, err := selectNativeQueuedExternalEffectReceipt(ctx, tx, a.store.dialect, intent)
	if errors.Is(err, errExternalEffectReceiptNotFound) {
		return effectreceipt.Record{}, false, false, effectreceipt.ErrNotFound
	}
	if err != nil {
		return effectreceipt.Record{}, false, isSQLiteMigrationBusy(err), err
	}
	if !sameExternalEffectIntent(receipt.Intent, intent) {
		return effectreceipt.Record{}, false, false, effectreceipt.ErrConflict
	}

	begun := false
	switch receipt.State {
	case effectreceipt.StatePrepared:
		args := append([]any{time.Now().UTC().UnixMilli()}, externalEffectIdentityArgs(intent)...)
		args = append(args, effectreceipt.MaxDispatchAttempts)
		write, err := tx.ExecContext(ctx, sqlBeginExternalEffectDispatch.bind(a.store.dialect), args...)
		if err != nil {
			return effectreceipt.Record{}, false, isSQLiteMigrationBusy(err), err
		}
		affected, err := write.RowsAffected()
		if err != nil {
			return effectreceipt.Record{}, false, false, err
		}
		if affected != 1 {
			return effectreceipt.Record{}, false, false, effectreceipt.ErrConflict
		}
		begun = true
	case effectreceipt.StateDispatching:
		// Exact response-lost replay. It must not reacquire provider ownership.
	default:
		return effectreceipt.Record{}, false, false, effectreceipt.ErrConflict
	}

	stored, err := selectNativeQueuedExternalEffectReceipt(ctx, tx, a.store.dialect, intent)
	if err != nil {
		return effectreceipt.Record{}, false, isSQLiteMigrationBusy(err), err
	}
	if !sameExternalEffectIntent(stored.Intent, intent) || stored.State != effectreceipt.StateDispatching || stored.DispatchAttempts == 0 {
		return effectreceipt.Record{}, false, false, effectreceipt.ErrConflict
	}
	if err := lockAuthorizationEpoch(ctx, tx, a.store.dialect, input.ExpectedAuthorizationEpoch); err != nil {
		return effectreceipt.Record{}, false, isSQLiteMigrationBusy(err), err
	}
	write, err := tx.ExecContext(ctx, nativeQueuedToolEffectFenceUpdate(a.store.dialect), input.Fence.SessionID, input.ExpectedSessionVersion,
		input.Fence.TenantID, input.Fence.SubjectID, input.Fence.RunID, input.Fence.SessionID, input.Fence.TenantID, input.Fence.SubjectID,
		input.Fence.WorkerID, input.Fence.QueueGeneration, input.Fence.LeaseHolder)
	if err != nil {
		return effectreceipt.Record{}, false, isSQLiteMigrationBusy(err), err
	}
	affected, err := write.RowsAffected()
	if err != nil {
		return effectreceipt.Record{}, false, false, err
	}
	if affected != 1 {
		return effectreceipt.Record{}, false, false, sessionWriteFenceLost("claim, cancellation state, or session lease expired before native queued effect dispatch admission")
	}
	if err := tx.Commit(); err != nil {
		return effectreceipt.Record{}, false, isSQLiteMigrationBusy(err), err
	}
	return stored.Record, begun, false, nil
}

func validateNativeQueuedEffectDispatchAdmission(input NativeQueuedEffectDispatchAdmission) error {
	if err := validateSessionWriteFence(input.Fence); err != nil {
		return err
	}
	if input.ExpectedSessionVersion < 1 || input.ExpectedAuthorizationEpoch < 0 {
		return completedToolResultProofInvalid()
	}
	return nil
}

func nativeQueuedEffectWitnessAdmits(witness nativeQueuedToolEffectWitness, input NativeQueuedEffectDispatchAdmission, invocation core.ToolInvocation) bool {
	return sameToolInvocation(witness.input.Invocation, invocation) && witness.input.AuthorizationEpoch == input.ExpectedAuthorizationEpoch &&
		witness.queueGeneration == input.Fence.QueueGeneration && witness.sessionVersionAfterCall == input.ExpectedSessionVersion &&
		witness.input.Invocation.TenantID == input.Fence.TenantID && witness.input.Invocation.SubjectID == input.Fence.SubjectID &&
		witness.input.Invocation.SessionID == input.Fence.SessionID && witness.input.Invocation.RunID == input.Fence.RunID
}

func selectNativeQueuedExternalEffectReceipt(ctx context.Context, tx *sql.Tx, dialect SQLDialect, intent effectreceipt.Intent) (effectreceipt.RecoveryRecord, error) {
	query := sqlSelectExternalEffectReceipt
	if dialect == SQLDialectPostgres {
		query = sqlQuery{text: sqlSelectExternalEffectReceipt.text + " FOR UPDATE"}
	}
	stored, err := scanExternalEffectReceipt(tx.QueryRowContext(ctx, query.bind(dialect), intent.Invocation.SessionID, intent.Invocation.RunID, intent.Invocation.CallID))
	if err != nil {
		return effectreceipt.RecoveryRecord{}, err
	}
	return stored, nil
}
