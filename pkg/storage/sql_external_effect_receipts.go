package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/whhhh1500/auto-agent/pkg/app/effectreceipt"
	"github.com/whhhh1500/auto-agent/pkg/core"
)

// SQLExternalEffectReceiptStore is the SQL implementation of the
// provider-neutral external-effect ledger. Its table contains identity and
// digest fields only; provider receipt bodies and request material do not have
// a persistence column in this store.
type SQLExternalEffectReceiptStore struct {
	db      *sql.DB
	dialect SQLDialect
}

const externalEffectSQLRetries = 8

var errExternalEffectReceiptNotFound = errors.New("external effect receipt not found")

var (
	sqlInsertExternalEffectReceipt = sqlQuery{`INSERT INTO external_effect_receipts
		(tenant_id, subject_id, session_id, run_id, call_id, capability_id, args_digest, idempotent,
		 protocol, driver_id, driver_version, target_digest, payload_digest, intent_digest, operation_key,
		 state, dispatch_attempts, receipt_digest, evidence_digest, error_code, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'prepared', 0, '', '', '', ?, ?)`}
	sqlSelectExternalEffectReceipt = sqlQuery{`SELECT tenant_id, subject_id, session_id, run_id, call_id, capability_id, args_digest, idempotent,
		protocol, driver_id, driver_version, target_digest, payload_digest, intent_digest, operation_key,
		state, dispatch_attempts, receipt_digest, evidence_digest, error_code, created_at, updated_at
		FROM external_effect_receipts WHERE session_id = ? AND run_id = ? AND call_id = ?`}
	sqlSelectExactExternalEffectReceipt = sqlQuery{`SELECT tenant_id, subject_id, session_id, run_id, call_id, capability_id, args_digest, idempotent,
		protocol, driver_id, driver_version, target_digest, payload_digest, intent_digest, operation_key,
		state, dispatch_attempts, receipt_digest, evidence_digest, error_code, created_at, updated_at
		FROM external_effect_receipts
		WHERE tenant_id = ? AND subject_id = ? AND session_id = ? AND run_id = ? AND call_id = ?
			AND capability_id = ? AND args_digest = ? AND idempotent = ? AND protocol = ?
			AND driver_id = ? AND driver_version = ? AND target_digest = ? AND payload_digest = ?
			AND intent_digest = ? AND operation_key = ?`}
	sqlBeginExternalEffectDispatch = sqlQuery{`UPDATE external_effect_receipts
		SET state = 'dispatching', dispatch_attempts = dispatch_attempts + 1, updated_at = ?
		WHERE tenant_id = ? AND subject_id = ? AND session_id = ? AND run_id = ? AND call_id = ?
			AND capability_id = ? AND args_digest = ? AND idempotent = ? AND protocol = ?
			AND driver_id = ? AND driver_version = ? AND target_digest = ? AND payload_digest = ?
			AND intent_digest = ? AND operation_key = ?
			AND state = 'prepared' AND dispatch_attempts < ?`}
	sqlMarkExternalEffectAccepted = sqlQuery{`UPDATE external_effect_receipts
		SET state = 'accepted', receipt_digest = ?, updated_at = ?
		WHERE tenant_id = ? AND subject_id = ? AND session_id = ? AND run_id = ? AND call_id = ?
			AND capability_id = ? AND args_digest = ? AND idempotent = ? AND protocol = ?
			AND driver_id = ? AND driver_version = ? AND target_digest = ? AND payload_digest = ?
			AND intent_digest = ? AND operation_key = ? AND state = 'dispatching'`}
	sqlMarkExternalEffectConfirmed = sqlQuery{`UPDATE external_effect_receipts
		SET state = 'confirmed', evidence_digest = ?, error_code = '', updated_at = ?
		WHERE tenant_id = ? AND subject_id = ? AND session_id = ? AND run_id = ? AND call_id = ?
			AND capability_id = ? AND args_digest = ? AND idempotent = ? AND protocol = ?
			AND driver_id = ? AND driver_version = ? AND target_digest = ? AND payload_digest = ?
			AND intent_digest = ? AND operation_key = ? AND state IN ('dispatching', 'accepted', 'unknown')`}
	sqlMarkExternalEffectRejected = sqlQuery{`UPDATE external_effect_receipts
		SET state = 'rejected', evidence_digest = ?, error_code = '', updated_at = ?
		WHERE tenant_id = ? AND subject_id = ? AND session_id = ? AND run_id = ? AND call_id = ?
			AND capability_id = ? AND args_digest = ? AND idempotent = ? AND protocol = ?
			AND driver_id = ? AND driver_version = ? AND target_digest = ? AND payload_digest = ?
			AND intent_digest = ? AND operation_key = ? AND state IN ('dispatching', 'accepted', 'unknown')`}
	sqlMarkExternalEffectUnknown = sqlQuery{`UPDATE external_effect_receipts
		SET state = 'unknown', evidence_digest = '', error_code = ?, updated_at = ?
		WHERE tenant_id = ? AND subject_id = ? AND session_id = ? AND run_id = ? AND call_id = ?
			AND capability_id = ? AND args_digest = ? AND idempotent = ? AND protocol = ?
			AND driver_id = ? AND driver_version = ? AND target_digest = ? AND payload_digest = ?
			AND intent_digest = ? AND operation_key = ? AND state IN ('dispatching', 'accepted', 'unknown')`}
	sqlListUnresolvedExternalEffects = sqlQuery{`SELECT tenant_id, subject_id, session_id, run_id, call_id, capability_id, args_digest, idempotent,
		protocol, driver_id, driver_version, target_digest, payload_digest, intent_digest, operation_key,
		state, dispatch_attempts, receipt_digest, evidence_digest, error_code, created_at, updated_at
		FROM external_effect_receipts
		WHERE driver_id = ? AND driver_version = ? AND state IN ('dispatching', 'accepted', 'unknown')
			AND (updated_at > ? OR (updated_at = ? AND tenant_id > ?)
				OR (updated_at = ? AND tenant_id = ? AND subject_id > ?)
				OR (updated_at = ? AND tenant_id = ? AND subject_id = ? AND session_id > ?)
				OR (updated_at = ? AND tenant_id = ? AND subject_id = ? AND session_id = ? AND run_id > ?)
				OR (updated_at = ? AND tenant_id = ? AND subject_id = ? AND session_id = ? AND run_id = ? AND call_id > ?))
		ORDER BY updated_at, tenant_id, subject_id, session_id, run_id, call_id LIMIT ?`}
	sqlListRunExternalEffects = sqlQuery{`SELECT tenant_id, subject_id, session_id, run_id, call_id, capability_id, args_digest, idempotent,
		protocol, driver_id, driver_version, target_digest, payload_digest, intent_digest, operation_key,
		state, dispatch_attempts, receipt_digest, evidence_digest, error_code, created_at, updated_at
		FROM external_effect_receipts
		WHERE tenant_id = ? AND subject_id = ? AND session_id = ? AND run_id = ? AND call_id > ?
		ORDER BY call_id LIMIT ?`}
)

// NewSQLExternalEffectReceiptStore binds an already-open SQL database to the
// v50 ledger. OpenSQLSessionStore is responsible for initializing or migrating
// the shared schema before this store is used.
func NewSQLExternalEffectReceiptStore(db *sql.DB, dialect SQLDialect) (*SQLExternalEffectReceiptStore, error) {
	if db == nil {
		return nil, fmt.Errorf("external effect receipt store requires a database handle")
	}
	if err := validateSQLDialect(dialect); err != nil {
		return nil, err
	}
	return &SQLExternalEffectReceiptStore{db: db, dialect: dialect}, nil
}

// Ensure atomically creates a prepared ledger row or returns the exact
// canonical row. The session/run/call key is deliberately queried without a
// principal predicate after a duplicate, so unsafe cross-principal or changed
// digest reuse fails closed as ErrConflict.
func (s *SQLExternalEffectReceiptStore) Ensure(ctx context.Context, intent effectreceipt.Intent) (effectreceipt.Record, error) {
	if err := effectreceipt.ValidateIntent(intent); err != nil {
		return effectreceipt.Record{}, effectreceipt.ErrInvalidIntent
	}
	now := time.Now().UTC()
	for attempt := 0; attempt < externalEffectSQLRetries; attempt++ {
		record, retry, err := s.ensure(ctx, intent, now)
		if !retry {
			return record, err
		}
		if err := waitExternalEffectRetry(ctx, attempt); err != nil {
			return effectreceipt.Record{}, err
		}
	}
	return effectreceipt.Record{}, fmt.Errorf("external effect receipt creation conflicted repeatedly")
}

func (s *SQLExternalEffectReceiptStore) ensure(ctx context.Context, intent effectreceipt.Intent, now time.Time) (effectreceipt.Record, bool, error) {
	_, err := s.db.ExecContext(ctx, sqlInsertExternalEffectReceipt.bind(s.dialect), externalEffectIntentArgs(intent, now.UnixMilli(), now.UnixMilli())...)
	if err == nil {
		return effectreceipt.Record{Intent: intent, State: effectreceipt.StatePrepared}, false, nil
	}
	if !isDuplicateConstraint(err) {
		return effectreceipt.Record{}, isSQLiteMigrationBusy(err), err
	}
	stored, err := s.selectLogical(ctx, intent.Invocation.SessionID, intent.Invocation.RunID, intent.Invocation.CallID)
	if errors.Is(err, errExternalEffectReceiptNotFound) {
		return effectreceipt.Record{}, true, err
	}
	if err != nil {
		return effectreceipt.Record{}, isSQLiteMigrationBusy(err), err
	}
	if !sameExternalEffectIntent(stored.Intent, intent) {
		return effectreceipt.Record{}, false, effectreceipt.ErrConflict
	}
	return stored.Record, false, nil
}

// Get reads only an exact immutable identity. A missing row and a logical-key
// conflict both return found=false so recovery cannot enumerate another effect.
func (s *SQLExternalEffectReceiptStore) Get(ctx context.Context, intent effectreceipt.Intent) (effectreceipt.Record, bool, error) {
	if err := effectreceipt.ValidateIntent(intent); err != nil {
		return effectreceipt.Record{}, false, effectreceipt.ErrInvalidIntent
	}
	stored, err := s.selectExact(ctx, intent)
	if errors.Is(err, errExternalEffectReceiptNotFound) {
		return effectreceipt.Record{}, false, nil
	}
	if err != nil {
		return effectreceipt.Record{}, false, err
	}
	return stored.Record, true, nil
}

// BeginDispatch is the provider-crossing fence. A true begun value belongs to
// the only caller that atomically changed prepared to dispatching; every other
// caller receives a canonical row and must read back instead of redispatching.
func (s *SQLExternalEffectReceiptStore) BeginDispatch(ctx context.Context, intent effectreceipt.Intent) (effectreceipt.Record, bool, error) {
	if err := effectreceipt.ValidateIntent(intent); err != nil {
		return effectreceipt.Record{}, false, effectreceipt.ErrInvalidIntent
	}
	for attempt := 0; attempt < externalEffectSQLRetries; attempt++ {
		record, begun, retry, err := s.beginDispatch(ctx, intent)
		if !retry {
			return record, begun, err
		}
		if err := waitExternalEffectRetry(ctx, attempt); err != nil {
			return effectreceipt.Record{}, false, err
		}
	}
	return effectreceipt.Record{}, false, fmt.Errorf("external effect dispatch transition conflicted repeatedly")
}

func (s *SQLExternalEffectReceiptStore) beginDispatch(ctx context.Context, intent effectreceipt.Intent) (effectreceipt.Record, bool, bool, error) {
	args := append([]any{time.Now().UTC().UnixMilli()}, externalEffectIdentityArgs(intent)...)
	args = append(args, effectreceipt.MaxDispatchAttempts)
	result, err := s.db.ExecContext(ctx, sqlBeginExternalEffectDispatch.bind(s.dialect), args...)
	if err != nil {
		return effectreceipt.Record{}, false, isSQLiteMigrationBusy(err), err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return effectreceipt.Record{}, false, false, err
	}
	stored, err := s.selectLogical(ctx, intent.Invocation.SessionID, intent.Invocation.RunID, intent.Invocation.CallID)
	if errors.Is(err, errExternalEffectReceiptNotFound) {
		return effectreceipt.Record{}, false, false, effectreceipt.ErrNotFound
	}
	if err != nil {
		return effectreceipt.Record{}, false, isSQLiteMigrationBusy(err), err
	}
	if !sameExternalEffectIntent(stored.Intent, intent) {
		return effectreceipt.Record{}, false, false, effectreceipt.ErrConflict
	}
	if affected == 1 {
		if stored.State != effectreceipt.StateDispatching {
			return effectreceipt.Record{}, false, false, effectreceipt.ErrConflict
		}
		return stored.Record, true, false, nil
	}
	return stored.Record, false, false, nil
}

// MarkAccepted persists only a bound acknowledgement digest. It does not
// confirm the external effect and can succeed only from dispatching.
func (s *SQLExternalEffectReceiptStore) MarkAccepted(ctx context.Context, intent effectreceipt.Intent, submission effectreceipt.Submission) (effectreceipt.Record, error) {
	if err := validateExternalEffectSubmission(intent, submission); err != nil {
		return effectreceipt.Record{}, err
	}
	args := append([]any{submission.ReceiptDigest, time.Now().UTC().UnixMilli()}, externalEffectIdentityArgs(intent)...)
	result, err := s.db.ExecContext(ctx, sqlMarkExternalEffectAccepted.bind(s.dialect), args...)
	if err != nil {
		return effectreceipt.Record{}, err
	}
	return s.afterTransition(ctx, intent, result, effectreceipt.StateAccepted, func(record effectreceipt.Record) bool {
		return record.ReceiptDigest == submission.ReceiptDigest
	})
}

// MarkConfirmed accepts only a bound, terminal read-back observation.
func (s *SQLExternalEffectReceiptStore) MarkConfirmed(ctx context.Context, intent effectreceipt.Intent, observation effectreceipt.Observation) (effectreceipt.Record, error) {
	if err := validateExternalEffectObservation(intent, observation, effectreceipt.ObservationConfirmed); err != nil {
		return effectreceipt.Record{}, err
	}
	return s.markTerminal(ctx, intent, observation, effectreceipt.StateConfirmed)
}

// MarkRejected accepts only a bound, terminal read-back observation.
func (s *SQLExternalEffectReceiptStore) MarkRejected(ctx context.Context, intent effectreceipt.Intent, observation effectreceipt.Observation) (effectreceipt.Record, error) {
	if err := validateExternalEffectObservation(intent, observation, effectreceipt.ObservationRejected); err != nil {
		return effectreceipt.Record{}, err
	}
	return s.markTerminal(ctx, intent, observation, effectreceipt.StateRejected)
}

func (s *SQLExternalEffectReceiptStore) markTerminal(ctx context.Context, intent effectreceipt.Intent, observation effectreceipt.Observation, state effectreceipt.State) (effectreceipt.Record, error) {
	query := sqlMarkExternalEffectConfirmed
	if state == effectreceipt.StateRejected {
		query = sqlMarkExternalEffectRejected
	}
	args := append([]any{observation.EvidenceDigest, time.Now().UTC().UnixMilli()}, externalEffectIdentityArgs(intent)...)
	result, err := s.db.ExecContext(ctx, query.bind(s.dialect), args...)
	if err != nil {
		return effectreceipt.Record{}, err
	}
	return s.afterTransition(ctx, intent, result, state, func(record effectreceipt.Record) bool {
		return record.EvidenceDigest == observation.EvidenceDigest
	})
}

// MarkUnknown records a fixed non-terminal outcome. The SQL check and
// validation probe reject arbitrary error strings before any write occurs.
func (s *SQLExternalEffectReceiptStore) MarkUnknown(ctx context.Context, intent effectreceipt.Intent, code string) (effectreceipt.Record, error) {
	probe := effectreceipt.Record{Intent: intent, State: effectreceipt.StateUnknown, DispatchAttempts: 1, ErrorCode: code}
	if effectreceipt.ValidateRecord(probe) != nil {
		return effectreceipt.Record{}, effectreceipt.ErrInvalidState
	}
	args := append([]any{code, time.Now().UTC().UnixMilli()}, externalEffectIdentityArgs(intent)...)
	result, err := s.db.ExecContext(ctx, sqlMarkExternalEffectUnknown.bind(s.dialect), args...)
	if err != nil {
		return effectreceipt.Record{}, err
	}
	return s.afterTransition(ctx, intent, result, effectreceipt.StateUnknown, func(record effectreceipt.Record) bool {
		return record.ErrorCode == code
	})
}

func (s *SQLExternalEffectReceiptStore) afterTransition(ctx context.Context, intent effectreceipt.Intent, result sql.Result, want effectreceipt.State, matches func(effectreceipt.Record) bool) (effectreceipt.Record, error) {
	affected, err := result.RowsAffected()
	if err != nil {
		return effectreceipt.Record{}, err
	}
	stored, err := s.selectLogical(ctx, intent.Invocation.SessionID, intent.Invocation.RunID, intent.Invocation.CallID)
	if errors.Is(err, errExternalEffectReceiptNotFound) {
		return effectreceipt.Record{}, effectreceipt.ErrNotFound
	}
	if err != nil {
		return effectreceipt.Record{}, err
	}
	if !sameExternalEffectIntent(stored.Intent, intent) || stored.State != want || !matches(stored.Record) {
		return effectreceipt.Record{}, effectreceipt.ErrConflict
	}
	if affected != 1 && affected != 0 {
		return effectreceipt.Record{}, effectreceipt.ErrConflict
	}
	return stored.Record, nil
}

// ListUnresolved returns a bounded, deterministic page of records requiring
// provider read-back. It never creates or dispatches an effect.
func (s *SQLExternalEffectReceiptStore) ListUnresolved(ctx context.Context, query effectreceipt.RecoveryQuery, cursor effectreceipt.RecoveryCursor, limit int) ([]effectreceipt.RecoveryRecord, effectreceipt.RecoveryCursor, error) {
	if !validExternalEffectDriverRef(query.Driver) {
		return nil, effectreceipt.RecoveryCursor{}, effectreceipt.ErrInvalidIntent
	}
	if err := validateExternalEffectRecoveryCursor(cursor); err != nil {
		return nil, effectreceipt.RecoveryCursor{}, err
	}
	if limit < 1 || limit > effectreceipt.MaxRecoveryPageSize {
		return nil, effectreceipt.RecoveryCursor{}, fmt.Errorf("external effect recovery limit must be between 1 and %d", effectreceipt.MaxRecoveryPageSize)
	}
	updatedAt := cursor.UpdatedAt.UTC().UnixMilli()
	args := []any{
		query.Driver.ID, query.Driver.Version,
		updatedAt,
		updatedAt, cursor.TenantID,
		updatedAt, cursor.TenantID, cursor.SubjectID,
		updatedAt, cursor.TenantID, cursor.SubjectID, cursor.SessionID,
		updatedAt, cursor.TenantID, cursor.SubjectID, cursor.SessionID, cursor.RunID,
		updatedAt, cursor.TenantID, cursor.SubjectID, cursor.SessionID, cursor.RunID, cursor.CallID,
		limit,
	}
	rows, err := s.db.QueryContext(ctx, sqlListUnresolvedExternalEffects.bind(s.dialect), args...)
	if err != nil {
		return nil, effectreceipt.RecoveryCursor{}, err
	}
	defer rows.Close()
	records := make([]effectreceipt.RecoveryRecord, 0, limit)
	next := cursor
	for rows.Next() {
		stored, err := scanExternalEffectReceipt(rows)
		if err != nil {
			return nil, effectreceipt.RecoveryCursor{}, err
		}
		records = append(records, stored)
		next = recoveryCursorFor(stored)
	}
	if err := rows.Err(); err != nil {
		return nil, effectreceipt.RecoveryCursor{}, err
	}
	return records, next, nil
}

// ListRun returns all states for one exact principal/session/run scope through
// a bounded call-id keyset page. It cannot broaden a partial caller scope.
func (s *SQLExternalEffectReceiptStore) ListRun(ctx context.Context, scope effectreceipt.RunScope, cursor effectreceipt.RunCursor, limit int) ([]effectreceipt.RecoveryRecord, effectreceipt.RunCursor, error) {
	if err := validateExternalEffectRunScope(scope); err != nil {
		return nil, effectreceipt.RunCursor{}, err
	}
	if cursor.CallID != "" && !validExternalEffectCallID(cursor.CallID) {
		return nil, effectreceipt.RunCursor{}, fmt.Errorf("external effect run cursor is invalid")
	}
	if limit < 1 || limit > effectreceipt.MaxRecoveryPageSize {
		return nil, effectreceipt.RunCursor{}, fmt.Errorf("external effect run limit must be between 1 and %d", effectreceipt.MaxRecoveryPageSize)
	}
	rows, err := s.db.QueryContext(ctx, sqlListRunExternalEffects.bind(s.dialect), scope.TenantID, scope.SubjectID, scope.SessionID, scope.RunID, cursor.CallID, limit)
	if err != nil {
		return nil, effectreceipt.RunCursor{}, err
	}
	defer rows.Close()
	records := make([]effectreceipt.RecoveryRecord, 0, limit)
	next := cursor
	for rows.Next() {
		stored, err := scanExternalEffectReceipt(rows)
		if err != nil {
			return nil, effectreceipt.RunCursor{}, err
		}
		records = append(records, stored)
		next = effectreceipt.RunCursor{CallID: stored.Intent.Invocation.CallID}
	}
	if err := rows.Err(); err != nil {
		return nil, effectreceipt.RunCursor{}, err
	}
	return records, next, nil
}

func (s *SQLExternalEffectReceiptStore) selectLogical(ctx context.Context, sessionID, runID, callID string) (effectreceipt.RecoveryRecord, error) {
	return scanExternalEffectReceipt(s.db.QueryRowContext(ctx, sqlSelectExternalEffectReceipt.bind(s.dialect), sessionID, runID, callID))
}

func (s *SQLExternalEffectReceiptStore) selectExact(ctx context.Context, intent effectreceipt.Intent) (effectreceipt.RecoveryRecord, error) {
	return scanExternalEffectReceipt(s.db.QueryRowContext(ctx, sqlSelectExactExternalEffectReceipt.bind(s.dialect), externalEffectIdentityArgs(intent)...))
}

func scanExternalEffectReceipt(scanner interface{ Scan(...any) error }) (effectreceipt.RecoveryRecord, error) {
	var stored effectreceipt.RecoveryRecord
	var idempotent int
	var state string
	var attempts, createdAt, updatedAt int64
	err := scanner.Scan(
		&stored.Intent.Invocation.TenantID, &stored.Intent.Invocation.SubjectID, &stored.Intent.Invocation.SessionID,
		&stored.Intent.Invocation.RunID, &stored.Intent.Invocation.CallID, &stored.Intent.Invocation.CapabilityID,
		&stored.Intent.Invocation.ArgsDigest, &idempotent, &stored.Intent.Protocol, &stored.Intent.Driver.ID,
		&stored.Intent.Driver.Version, &stored.Intent.TargetDigest, &stored.Intent.PayloadDigest, &stored.Intent.IntentDigest,
		&stored.Intent.OperationKey, &state, &attempts, &stored.ReceiptDigest, &stored.EvidenceDigest,
		&stored.ErrorCode, &createdAt, &updatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return effectreceipt.RecoveryRecord{}, errExternalEffectReceiptNotFound
	}
	if err != nil {
		return effectreceipt.RecoveryRecord{}, err
	}
	if idempotent != 0 && idempotent != 1 || attempts < 0 || attempts > int64(effectreceipt.MaxDispatchAttempts) || createdAt <= 0 || updatedAt <= 0 {
		return effectreceipt.RecoveryRecord{}, fmt.Errorf("external effect receipt contains invalid durable fields")
	}
	stored.Intent.Invocation.Idempotent = idempotent == 1
	stored.State = effectreceipt.State(state)
	stored.DispatchAttempts = uint32(attempts)
	if err := effectreceipt.ValidateRecord(stored.Record); err != nil {
		return effectreceipt.RecoveryRecord{}, fmt.Errorf("validate external effect receipt: %w", err)
	}
	stored.CreatedAt = time.UnixMilli(createdAt).UTC()
	stored.UpdatedAt = time.UnixMilli(updatedAt).UTC()
	return stored, nil
}

func externalEffectIntentArgs(intent effectreceipt.Intent, createdAt, updatedAt int64) []any {
	return append(externalEffectIdentityArgs(intent), createdAt, updatedAt)
}

func externalEffectIdentityArgs(intent effectreceipt.Intent) []any {
	invocation := intent.Invocation
	return []any{
		invocation.TenantID, invocation.SubjectID, invocation.SessionID, invocation.RunID, invocation.CallID,
		invocation.CapabilityID, invocation.ArgsDigest, boolInt(invocation.Idempotent), intent.Protocol,
		intent.Driver.ID, intent.Driver.Version, intent.TargetDigest, intent.PayloadDigest, intent.IntentDigest, intent.OperationKey,
	}
}

func sameExternalEffectIntent(left, right effectreceipt.Intent) bool {
	return left == right
}

func validateExternalEffectSubmission(intent effectreceipt.Intent, submission effectreceipt.Submission) error {
	if effectreceipt.ValidateIntent(intent) != nil || submission.OperationKey != intent.OperationKey || submission.IntentDigest != intent.IntentDigest {
		return effectreceipt.ErrInvalidSubmission
	}
	probe := effectreceipt.Record{Intent: intent, State: effectreceipt.StateAccepted, DispatchAttempts: 1, ReceiptDigest: submission.ReceiptDigest}
	if effectreceipt.ValidateRecord(probe) != nil {
		return effectreceipt.ErrInvalidSubmission
	}
	return nil
}

func validateExternalEffectObservation(intent effectreceipt.Intent, observation effectreceipt.Observation, want effectreceipt.ObservationState) error {
	if effectreceipt.ValidateIntent(intent) != nil || observation.OperationKey != intent.OperationKey || observation.IntentDigest != intent.IntentDigest {
		return effectreceipt.ErrReadBackMismatch
	}
	if observation.State != want {
		return effectreceipt.ErrInvalidObservation
	}
	state := effectreceipt.StateConfirmed
	if want == effectreceipt.ObservationRejected {
		state = effectreceipt.StateRejected
	}
	probe := effectreceipt.Record{Intent: intent, State: state, DispatchAttempts: 1, EvidenceDigest: observation.EvidenceDigest}
	if effectreceipt.ValidateRecord(probe) != nil {
		return effectreceipt.ErrInvalidObservation
	}
	return nil
}

func validateExternalEffectRecoveryCursor(cursor effectreceipt.RecoveryCursor) error {
	values := []string{cursor.TenantID, cursor.SubjectID, cursor.SessionID, cursor.RunID, cursor.CallID}
	empty := 0
	for _, value := range values {
		if value == "" {
			empty++
		}
	}
	if empty == len(values) && cursor.UpdatedAt.IsZero() {
		return nil
	}
	if empty != 0 || cursor.UpdatedAt.IsZero() || cursor.UpdatedAt.UnixMilli() <= 0 || validateExternalEffectRunScope(effectreceipt.RunScope{
		TenantID: cursor.TenantID, SubjectID: cursor.SubjectID, SessionID: cursor.SessionID, RunID: cursor.RunID,
	}) != nil || !validExternalEffectCallID(cursor.CallID) {
		return fmt.Errorf("external effect recovery cursor is invalid")
	}
	return nil
}

func recoveryCursorFor(record effectreceipt.RecoveryRecord) effectreceipt.RecoveryCursor {
	invocation := record.Intent.Invocation
	return effectreceipt.RecoveryCursor{UpdatedAt: record.UpdatedAt, TenantID: invocation.TenantID, SubjectID: invocation.SubjectID,
		SessionID: invocation.SessionID, RunID: invocation.RunID, CallID: invocation.CallID}
}

func validateExternalEffectRunScope(scope effectreceipt.RunScope) error {
	if !validExternalEffectPrincipalPart(scope.TenantID) || !validExternalEffectPrincipalPart(scope.SubjectID) ||
		core.ValidateSessionID(scope.SessionID) != nil || core.ValidateRunID(scope.RunID) != nil {
		return fmt.Errorf("external effect run scope is invalid")
	}
	return nil
}

func validExternalEffectPrincipalPart(value string) bool {
	return strings.TrimSpace(value) != "" && len(value) <= 512 && utf8.ValidString(value) && !strings.ContainsAny(value, "\r\n\x00")
}

func validExternalEffectCallID(value string) bool {
	return strings.TrimSpace(value) != "" && len(value) <= 256 && utf8.ValidString(value) && !strings.ContainsAny(value, "\r\n\x00")
}

func validExternalEffectDriverRef(ref effectreceipt.DriverRef) bool {
	return validExternalEffectText(ref.ID, 128) && validExternalEffectText(ref.Version, 64)
}

func validExternalEffectText(value string, max int) bool {
	return value == strings.TrimSpace(value) && value != "" && len(value) <= max && utf8.ValidString(value) && !strings.ContainsAny(value, "\r\n\x00")
}

func waitExternalEffectRetry(ctx context.Context, attempt int) error {
	delay := time.Duration(attempt+1) * time.Millisecond
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

var _ effectreceipt.Store = (*SQLExternalEffectReceiptStore)(nil)
var _ effectreceipt.RecoveryReader = (*SQLExternalEffectReceiptStore)(nil)
var _ effectreceipt.RunReader = (*SQLExternalEffectReceiptStore)(nil)
