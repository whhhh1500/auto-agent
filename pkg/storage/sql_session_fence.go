package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

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
	var runSessionID, runTenantID, runSubjectID, status string
	var cancelRequested int
	if err := tx.QueryRowContext(ctx, sqlFencePostgresLockRun.bind(s.dialect), fence.RunID).Scan(&runSessionID, &runTenantID, &runSubjectID, &status, &cancelRequested); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", sessionWriteFenceLost("run control row is missing")
		}
		return "", err
	}
	var workerID string
	var generation, queueExpiry int64
	if err := tx.QueryRowContext(ctx, sqlFencePostgresLockQueue.bind(s.dialect), fence.RunID).Scan(&workerID, &generation, &queueExpiry); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", sessionWriteFenceLost("run queue row is missing")
		}
		return "", err
	}
	var leaseHolder string
	var sessionLeaseExpiry int64
	if err := tx.QueryRowContext(ctx, sqlFencePostgresLockLease.bind(s.dialect), fence.SessionID).Scan(&leaseHolder, &sessionLeaseExpiry); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", sessionWriteFenceLost("session lease row is missing")
		}
		return "", err
	}
	var committed int64
	var header, sessionTenantID, sessionUserID string
	if err := tx.QueryRowContext(ctx, sqlFencePostgresLockSession.bind(s.dialect), fence.SessionID).Scan(&committed, &header, &sessionTenantID, &sessionUserID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", fmt.Errorf("%w: %s", core.ErrSessionNotFound, fence.SessionID)
		}
		return "", err
	}
	var now int64
	if err := tx.QueryRowContext(ctx, sqlFencePostgresNow.bind(s.dialect)).Scan(&now); err != nil {
		return "", err
	}
	switch {
	case runSessionID != fence.SessionID:
		return "", sessionWriteFenceLost("run belongs to another session")
	case runTenantID != fence.TenantID || runSubjectID != fence.SubjectID:
		return "", sessionWriteFenceLost("run owner changed")
	case status != RunStatusRunning:
		return "", sessionWriteFenceLost("run is not running")
	case cancelRequested != 0:
		return "", sessionWriteFenceLost("run cancellation was requested")
	case workerID != fence.WorkerID:
		return "", sessionWriteFenceLost("run queue worker changed")
	case generation != fence.QueueGeneration:
		return "", sessionWriteFenceLost("run queue generation changed")
	case queueExpiry <= now:
		return "", sessionWriteFenceLost("run queue lease expired")
	case leaseHolder != fence.LeaseHolder:
		return "", sessionWriteFenceLost("session lease holder changed")
	case sessionLeaseExpiry <= now:
		return "", sessionWriteFenceLost("session lease expired")
	case sessionTenantID != fence.TenantID || sessionUserID != fence.SubjectID:
		return "", sessionWriteFenceLost("session catalog identity does not match fence")
	case committed != expectedVersion:
		return "", fmt.Errorf("%w: expected %d, found %d", core.ErrSessionConflict, expectedVersion, committed)
	default:
		return header, nil
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
