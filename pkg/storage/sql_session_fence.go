package storage

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

var _ FencedSessionAppender = (*SQLSessionStore)(nil)

var (
	sqlFenceSQLiteAcquire = sqlQuery{`UPDATE sessions SET updated_at = updated_at
		WHERE id = ? AND version = ? AND tenant_id = ? AND user_id = ? AND EXISTS (
			SELECT 1 FROM run_control rc
			JOIN run_queue q ON q.run_id = rc.run_id
			JOIN session_leases sl ON sl.session_id = rc.session_id
			WHERE rc.run_id = ? AND rc.session_id = ?
			AND rc.tenant_id = ? AND rc.subject_id = ?
			AND rc.status = 'running' AND rc.cancel_requested = 0
			AND q.worker_id = ? AND q.generation = ?
			AND q.lease_expires_at > CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)
			AND sl.holder = ?
			AND sl.expires_at > CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)
		)`}
	sqlFencePostgresLockSession = sqlQuery{"SELECT version, header, tenant_id, user_id FROM sessions WHERE id = ? FOR UPDATE"}
	sqlFencePostgresLockRun     = sqlQuery{"SELECT session_id, tenant_id, subject_id, status, cancel_requested FROM run_control WHERE run_id = ? FOR UPDATE"}
	sqlFencePostgresLockQueue   = sqlQuery{"SELECT worker_id, generation, lease_expires_at FROM run_queue WHERE run_id = ? FOR UPDATE"}
	sqlFencePostgresLockLease   = sqlQuery{"SELECT holder, expires_at FROM session_leases WHERE session_id = ? FOR UPDATE"}
	sqlFencePostgresNow         = sqlQuery{"SELECT CAST(EXTRACT(EPOCH FROM clock_timestamp()) * 1000 AS BIGINT)"}
	sqlFenceSelectSession       = sqlQuery{"SELECT version, header, tenant_id, user_id FROM sessions WHERE id = ?"}
)

// AppendEventsFenced implements FencedSessionAppender only because the SQL
// Session store shares one database transaction with run_control, run_queue,
// and session_leases. It must not be emulated across independent databases.
func (s *SQLSessionStore) AppendEventsFenced(ctx context.Context, fence SessionWriteFence, expectedVersion int64, events []core.SessionEvent) error {
	return s.appendEventsFenced(ctx, fence, expectedVersion, events)
}

func (s *SQLSessionStore) appendEventsFenced(ctx context.Context, fence SessionWriteFence, expectedVersion int64, events []core.SessionEvent) error {
	if ctx == nil {
		return fmt.Errorf("fenced session append requires a context")
	}
	if s == nil || s.db == nil {
		return fmt.Errorf("fenced session append requires an SQL session store")
	}
	if err := validateFencedAppend(fence, expectedVersion, events); err != nil {
		return err
	}
	// An empty suffix is deliberately a no-op. In particular, it must not
	// advance updated_at, turn an already-lost fence into an unrelated error,
	// or be treated by callers as a proof that the fence is still live.
	if len(events) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	var header string
	switch s.dialect {
	case SQLDialectSQLite:
		header, err = s.acquireSQLiteSessionWriteFence(ctx, tx, fence, expectedVersion)
	case SQLDialectPostgres:
		header, err = s.acquirePostgresSessionWriteFence(ctx, tx, fence, expectedVersion)
	default:
		err = fmt.Errorf("unsupported SQL dialect %q", s.dialect.String())
	}
	if err != nil {
		return err
	}
	var options core.SessionOptions
	if err := json.Unmarshal([]byte(header), &options); err != nil {
		return fmt.Errorf("decode session %s header for fence: %w", fence.SessionID, err)
	}
	if options.Principal.TenantID != fence.TenantID || options.Principal.SubjectID != fence.SubjectID {
		return sessionWriteFenceLost("fenced identity does not own the session")
	}

	if len(events) > 0 {
		if err := s.insertChunk(ctx, tx, fence.SessionID, expectedVersion, events); err != nil {
			return err
		}
		if err := insertRunEvidence(ctx, tx, s.dialect, fence.SessionID, options, events, s.evidenceCap()); err != nil {
			return fmt.Errorf("index session %s run evidence: %w", fence.SessionID, err)
		}
	}

	targetVersion := expectedVersion + int64(len(events))
	result, err := tx.ExecContext(ctx, fencedSessionTipUpdate(s.dialect),
		targetVersion, targetVersion, fence.SessionID, expectedVersion, fence.TenantID, fence.SubjectID,
		fence.RunID, fence.SessionID, fence.TenantID, fence.SubjectID, fence.WorkerID, fence.QueueGeneration, fence.LeaseHolder,
	)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return sessionWriteFenceLost("claim, cancellation state, or session lease expired before commit")
	}
	return tx.Commit()
}

// appendInterruptedSessionRepairFenced is the storage-private companion to
// RepairInterruptedSessionFenced. It treats events as a candidate only: after
// acquiring the current queued-run fence, it reconstructs the committed
// Session prefix in the same transaction, derives the one legal repair suffix
// with core.RepairInterrupted, and writes its own newly assigned events.
//
// This prevents a caller with a valid fence from using interrupted-run repair
// as an arbitrary cross-Run append capability. The candidate is retained only
// to make a stale in-memory repair decision fail closed rather than silently
// repairing a newer, different tail.
func (s *SQLSessionStore) appendInterruptedSessionRepairFenced(ctx context.Context, fence SessionWriteFence, expectedVersion int64, candidate []core.SessionEvent) (*core.Session, bool, error) {
	if ctx == nil {
		return nil, false, fmt.Errorf("fenced session repair requires a context")
	}
	if s == nil || s.db == nil {
		return nil, false, fmt.Errorf("fenced session repair requires an SQL session store")
	}
	if err := validateSessionWriteFence(fence); err != nil {
		return nil, false, err
	}
	if expectedVersion < 0 {
		return nil, false, fmt.Errorf("session repair expected version must not be negative")
	}
	if len(candidate) == 0 {
		return nil, false, fmt.Errorf("fenced session repair candidate is empty")
	}
	if err := validateAppendEvents(expectedVersion, candidate); err != nil {
		return nil, false, err
	}
	for index, event := range candidate {
		if event.Time.IsZero() {
			return nil, false, fmt.Errorf("fenced repair candidate event %d has no append timestamp", index)
		}
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = tx.Rollback() }()

	var header string
	switch s.dialect {
	case SQLDialectSQLite:
		header, err = s.acquireSQLiteSessionWriteFence(ctx, tx, fence, expectedVersion)
	case SQLDialectPostgres:
		header, err = s.acquirePostgresSessionWriteFence(ctx, tx, fence, expectedVersion)
	default:
		err = fmt.Errorf("unsupported SQL dialect %q", s.dialect.String())
	}
	if err != nil {
		return nil, false, err
	}
	var options core.SessionOptions
	if err := json.Unmarshal([]byte(header), &options); err != nil {
		return nil, false, fmt.Errorf("decode session %s header for fenced repair: %w", fence.SessionID, err)
	}
	if options.Principal.TenantID != fence.TenantID || options.Principal.SubjectID != fence.SubjectID {
		return nil, false, sessionWriteFenceLost("fenced identity does not own the session")
	}

	committed, err := s.restoreFencedSession(ctx, tx, fence.SessionID, options, expectedVersion)
	if err != nil {
		return nil, false, err
	}
	synthetic := core.RepairInterrupted(committed.Events())
	if len(synthetic) == 0 {
		return nil, false, fmt.Errorf("fenced repair candidate does not match an interrupted committed session tail")
	}
	if err := validateFencedRepairCandidate(expectedVersion, candidate, synthetic); err != nil {
		return nil, false, err
	}

	repaired, err := committed.Clone()
	if err != nil {
		return nil, false, err
	}
	if err := core.AppendRepair(repaired, synthetic, nil); err != nil {
		return nil, false, err
	}
	events := repaired.EventsFrom(expectedVersion)
	if err := s.insertChunk(ctx, tx, fence.SessionID, expectedVersion, events); err != nil {
		return nil, false, err
	}
	if err := insertRunEvidence(ctx, tx, s.dialect, fence.SessionID, options, events, s.evidenceCap()); err != nil {
		return nil, false, fmt.Errorf("index session %s fenced repair evidence: %w", fence.SessionID, err)
	}
	targetVersion := expectedVersion + int64(len(events))
	result, err := tx.ExecContext(ctx, fencedSessionTipUpdate(s.dialect),
		targetVersion, targetVersion, fence.SessionID, expectedVersion, fence.TenantID, fence.SubjectID,
		fence.RunID, fence.SessionID, fence.TenantID, fence.SubjectID, fence.WorkerID, fence.QueueGeneration, fence.LeaseHolder,
	)
	if err != nil {
		return nil, false, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return nil, false, err
	}
	if affected == 0 {
		return nil, false, sessionWriteFenceLost("claim, cancellation state, or session lease expired before fenced repair commit")
	}
	if err := tx.Commit(); err != nil {
		return nil, false, err
	}
	return repaired, true, nil
}

func validateFencedRepairCandidate(expectedVersion int64, candidate, synthetic []core.SessionEvent) error {
	if len(candidate) != len(synthetic) {
		return fmt.Errorf("fenced repair candidate has %d events, committed repair requires %d", len(candidate), len(synthetic))
	}
	for index := range synthetic {
		candidateEvent := candidate[index]
		syntheticEvent := synthetic[index]
		if candidateEvent.Seq != expectedVersion+int64(index) {
			return fmt.Errorf("fenced repair candidate event %d has sequence %d, want %d", index, candidateEvent.Seq, expectedVersion+int64(index))
		}
		if candidateEvent.Time.IsZero() {
			return fmt.Errorf("fenced repair candidate event %d has no append timestamp", index)
		}
		if candidateEvent.RunID != syntheticEvent.RunID || candidateEvent.Type != syntheticEvent.Type || !bytes.Equal(candidateEvent.Data, syntheticEvent.Data) {
			return fmt.Errorf("fenced repair candidate event %d does not match the committed repair suffix", index)
		}
	}
	return nil
}

func (s *SQLSessionStore) restoreFencedSession(ctx context.Context, tx *sql.Tx, sessionID string, options core.SessionOptions, committed int64) (*core.Session, error) {
	if err := validateCommittedSessionVersion(sessionID, committed); err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, sqlSelectChunks.bind(s.dialect), sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	events := make([]core.SessionEvent, 0, committed)
	nextSeq := int64(0)
	for rows.Next() {
		var startSeq int64
		var payload string
		if err := rows.Scan(&startSeq, &payload); err != nil {
			return nil, err
		}
		if startSeq != nextSeq {
			return nil, fmt.Errorf("session %s chunk sequence discontinuous at %d", sessionID, startSeq)
		}
		decoded := int64(0)
		for _, line := range strings.Split(payload, "\n") {
			if strings.TrimSpace(line) == "" {
				continue
			}
			if len(line) > core.MaxSessionEventDataBytes+(1<<20) {
				return nil, fmt.Errorf("decode session %s event: encoded event exceeds size limit", sessionID)
			}
			var event core.SessionEvent
			if err := json.Unmarshal([]byte(line), &event); err != nil {
				return nil, fmt.Errorf("decode session %s event: %w", sessionID, err)
			}
			events = append(events, event)
			decoded++
		}
		nextSeq += decoded
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if int64(len(events)) > committed {
		events = events[:committed]
	}
	if int64(len(events)) != committed {
		return nil, fmt.Errorf("session %s committed version %d exceeds restored event count %d", sessionID, committed, len(events))
	}
	return core.RestoreSession(options, events)
}

// SQLite has one writer at a time. Making the first statement a conditional
// UPDATE obtains the writer reservation and validates the fence atomically;
// no other queue, cancellation, lease, or Session writer can pass it before
// this transaction commits or rolls back.
func (s *SQLSessionStore) acquireSQLiteSessionWriteFence(ctx context.Context, tx *sql.Tx, fence SessionWriteFence, expectedVersion int64) (string, error) {
	result, err := tx.ExecContext(ctx, sqlFenceSQLiteAcquire.bind(s.dialect),
		fence.SessionID, expectedVersion, fence.TenantID, fence.SubjectID, fence.RunID, fence.SessionID,
		fence.TenantID, fence.SubjectID, fence.WorkerID, fence.QueueGeneration, fence.LeaseHolder,
	)
	if err != nil {
		return "", err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return "", err
	}
	if affected == 0 {
		return "", s.classifySessionFenceFailure(ctx, tx, fence, expectedVersion)
	}
	var committed int64
	var header string
	if err := tx.QueryRowContext(ctx, sqlSelectSession.bind(s.dialect), fence.SessionID).Scan(&committed, &header); err != nil {
		return "", err
	}
	return header, nil
}

// PostgreSQL locks ownership rows in the same run-control, queue, lease order
// used by the claim lifecycle before locking the Session row. This avoids a
// run-control/queue lock inversion with claim renewal. Claim replacement,
// cancellation, and lease mutation therefore serialize behind this append.
// The final conditional tip update rechecks expiry against clock_timestamp so
// a lease that expires while the transaction is working cannot authorize the
// durable write.
func (s *SQLSessionStore) acquirePostgresSessionWriteFence(ctx context.Context, tx *sql.Tx, fence SessionWriteFence, expectedVersion int64) (string, error) {
	header, committed, err := s.lockPostgresSessionWriteFence(ctx, tx, fence)
	if err != nil {
		return "", err
	}
	if committed != expectedVersion {
		return "", fmt.Errorf("%w: expected %d, found %d", core.ErrSessionConflict, expectedVersion, committed)
	}
	return header, nil
}

// lockPostgresSessionWriteFence locks queued ownership in the same order as
// claim lifecycle mutations before the Session row. Callers that need an
// idempotent response-lost branch may inspect the locked committed version
// themselves; writers still make the final database-time fence recheck.
func (s *SQLSessionStore) lockPostgresSessionWriteFence(ctx context.Context, tx *sql.Tx, fence SessionWriteFence) (string, int64, error) {
	var runSessionID, runTenantID, runSubjectID, status string
	var cancelRequested int
	if err := tx.QueryRowContext(ctx, sqlFencePostgresLockRun.bind(s.dialect), fence.RunID).Scan(&runSessionID, &runTenantID, &runSubjectID, &status, &cancelRequested); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", 0, sessionWriteFenceLost("run control row is missing")
		}
		return "", 0, err
	}
	var workerID string
	var generation, queueExpiry int64
	if err := tx.QueryRowContext(ctx, sqlFencePostgresLockQueue.bind(s.dialect), fence.RunID).Scan(&workerID, &generation, &queueExpiry); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", 0, sessionWriteFenceLost("run queue row is missing")
		}
		return "", 0, err
	}
	var leaseHolder string
	var sessionLeaseExpiry int64
	if err := tx.QueryRowContext(ctx, sqlFencePostgresLockLease.bind(s.dialect), fence.SessionID).Scan(&leaseHolder, &sessionLeaseExpiry); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", 0, sessionWriteFenceLost("session lease row is missing")
		}
		return "", 0, err
	}
	var committed int64
	var header, sessionTenantID, sessionUserID string
	if err := tx.QueryRowContext(ctx, sqlFencePostgresLockSession.bind(s.dialect), fence.SessionID).Scan(&committed, &header, &sessionTenantID, &sessionUserID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", 0, fmt.Errorf("%w: %s", core.ErrSessionNotFound, fence.SessionID)
		}
		return "", 0, err
	}
	var now int64
	if err := tx.QueryRowContext(ctx, sqlFencePostgresNow.bind(s.dialect)).Scan(&now); err != nil {
		return "", 0, err
	}
	switch {
	case runSessionID != fence.SessionID:
		return "", 0, sessionWriteFenceLost("run belongs to another session")
	case runTenantID != fence.TenantID || runSubjectID != fence.SubjectID:
		return "", 0, sessionWriteFenceLost("run owner changed")
	case status != RunStatusRunning:
		return "", 0, sessionWriteFenceLost("run is not running")
	case cancelRequested != 0:
		return "", 0, sessionWriteFenceLost("run cancellation was requested")
	case workerID != fence.WorkerID:
		return "", 0, sessionWriteFenceLost("run queue worker changed")
	case generation != fence.QueueGeneration:
		return "", 0, sessionWriteFenceLost("run queue generation changed")
	case queueExpiry <= now:
		return "", 0, sessionWriteFenceLost("run queue lease expired")
	case leaseHolder != fence.LeaseHolder:
		return "", 0, sessionWriteFenceLost("session lease holder changed")
	case sessionLeaseExpiry <= now:
		return "", 0, sessionWriteFenceLost("session lease expired")
	case sessionTenantID != fence.TenantID || sessionUserID != fence.SubjectID:
		return "", 0, sessionWriteFenceLost("session catalog identity does not match fence")
	default:
		return header, committed, nil
	}
}

func (s *SQLSessionStore) classifySessionFenceFailure(ctx context.Context, tx *sql.Tx, fence SessionWriteFence, expectedVersion int64) error {
	var committed int64
	var header, sessionTenantID, sessionUserID string
	err := tx.QueryRowContext(ctx, sqlFenceSelectSession.bind(s.dialect), fence.SessionID).Scan(&committed, &header, &sessionTenantID, &sessionUserID)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: %s", core.ErrSessionNotFound, fence.SessionID)
	}
	if err != nil {
		return err
	}
	if sessionTenantID != fence.TenantID || sessionUserID != fence.SubjectID {
		return sessionWriteFenceLost("session catalog identity does not match fence")
	}
	var options core.SessionOptions
	if err := json.Unmarshal([]byte(header), &options); err != nil {
		return fmt.Errorf("decode session %s header for fence classification: %w", fence.SessionID, err)
	}
	if options.Principal.TenantID != fence.TenantID || options.Principal.SubjectID != fence.SubjectID {
		return sessionWriteFenceLost("fenced identity does not own the session")
	}
	var owns int
	ownershipQuery := sqlQuery{`SELECT CASE WHEN EXISTS (
		SELECT 1 FROM run_control rc
		JOIN run_queue q ON q.run_id = rc.run_id
		JOIN session_leases sl ON sl.session_id = rc.session_id
		WHERE rc.run_id = ? AND rc.session_id = ?
		AND rc.tenant_id = ? AND rc.subject_id = ?
		AND rc.status = 'running' AND rc.cancel_requested = 0
		AND q.worker_id = ? AND q.generation = ?
		AND q.lease_expires_at > CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)
		AND sl.holder = ?
		AND sl.expires_at > CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)
	) THEN 1 ELSE 0 END`}
	// This classifier is used only after the SQLite conditional writer update.
	// PostgreSQL failures are classified while holding explicit row locks.
	if err := tx.QueryRowContext(ctx, ownershipQuery.bind(s.dialect),
		fence.RunID, fence.SessionID, fence.TenantID, fence.SubjectID, fence.WorkerID, fence.QueueGeneration, fence.LeaseHolder,
	).Scan(&owns); err != nil {
		return err
	}
	if owns == 0 {
		return sessionWriteFenceLost("run claim, cancellation state, or session lease does not match")
	}
	if committed != expectedVersion {
		return fmt.Errorf("%w: expected %d, found %d", core.ErrSessionConflict, expectedVersion, committed)
	}
	return sessionWriteFenceLost("session write fence did not match")
}

func fencedSessionTipUpdate(dialect SQLDialect) string {
	now := "CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)"
	if dialect == SQLDialectPostgres {
		now = "CAST(EXTRACT(EPOCH FROM clock_timestamp()) * 1000 AS BIGINT)"
	}
	query := fmt.Sprintf(`UPDATE sessions SET version = ?, event_count = ?, updated_at = %s
		WHERE id = ? AND version = ? AND tenant_id = ? AND user_id = ? AND EXISTS (
			SELECT 1 FROM run_control rc
			JOIN run_queue q ON q.run_id = rc.run_id
			JOIN session_leases sl ON sl.session_id = rc.session_id
			WHERE rc.run_id = ? AND rc.session_id = ?
			AND rc.tenant_id = ? AND rc.subject_id = ?
			AND rc.status = 'running' AND rc.cancel_requested = 0
			AND q.worker_id = ? AND q.generation = ? AND q.lease_expires_at > %s
			AND sl.holder = ? AND sl.expires_at > %s
	)`, now, now, now)
	return (sqlQuery{query}).bind(dialect)
}
