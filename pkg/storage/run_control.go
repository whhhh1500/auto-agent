package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

const (
	RunStatusQueued          = "queued"
	RunStatusRunning         = "running"
	RunStatusWaitingApproval = "waiting_approval"
)

const (
	DefaultRunMaxAttempts = 3
	HardRunMaxAttempts    = 10
	MaxQueuedMessageBytes = 1 << 20
	MaxInFlightRuns       = 1024
	MaxRunSubmissions     = 8192
)

var (
	ErrRunNotFound           = errors.New("run not found")
	ErrRunSubmissionConflict = errors.New("run submission idempotency conflict")
	ErrInvalidRunSubmission  = errors.New("invalid run submission idempotency")
)

// RunRecord is the durable control-plane view of one run. Session events stay
// the canonical execution log; this record exists for status, discovery and
// cross-instance cancellation.
type RunRecord struct {
	RunID           string    `json:"run_id"`
	SessionID       string    `json:"session_id"`
	TenantID        string    `json:"tenant_id"`
	SubjectID       string    `json:"subject_id"`
	Status          string    `json:"status"`
	CancelRequested bool      `json:"cancel_requested"`
	ErrorCode       string    `json:"error_code,omitempty"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
	CompletedAt     time.Time `json:"completed_at,omitempty"`
}

// RunControlStore persists run lifecycle independently from the request and
// worker process handling it.
type RunControlStore interface {
	CreateRun(ctx context.Context, record RunRecord) error
	GetRun(ctx context.Context, runID string) (RunRecord, error)
	FindActiveRun(ctx context.Context, sessionID string) (RunRecord, error)
	ListRuns(ctx context.Context, sessionID string, limit int) ([]RunRecord, error)
	RequestRunCancel(ctx context.Context, runID string) (bool, error)
	RunCancelRequested(ctx context.Context, runID string) (bool, error)
	HeartbeatRun(ctx context.Context, runID string) error
	FinishRun(ctx context.Context, runID string, status core.RunStatus, errorCode string) error
	FailStaleRuns(ctx context.Context, olderThan time.Time) (int64, error)
}

// QueuedRun is one durable asynchronous run request and its current claim.
// Attempt counts failure-bearing deliveries; Generation is the monotonic
// fencing token and also advances across approval pause/resume cycles.
type QueuedRun struct {
	RunRecord
	Message        string                     `json:"message"`
	AvailableAt    time.Time                  `json:"available_at"`
	WorkerID       string                     `json:"worker_id,omitempty"`
	LeaseExpiresAt time.Time                  `json:"lease_expires_at,omitempty"`
	Attempt        int                        `json:"attempt"`
	MaxAttempts    int                        `json:"max_attempts"`
	Generation     int64                      `json:"generation"`
	TraceContext   core.TelemetryTraceContext `json:"trace_context,omitempty"`
}

// RunQueueStore extends durable run status with worker claim and retry
// semantics. Claim leases are separate from session leases: the queue lease
// owns the task, while SessionLeaser serializes session history mutation.
type RunQueueStore interface {
	RunControlStore
	EnqueueRun(ctx context.Context, run QueuedRun) error
	EnqueueRunOnce(ctx context.Context, run QueuedRun, idempotencyKey, requestDigest string) (record RunRecord, created bool, err error)
	ClaimRun(ctx context.Context, workerID string, leaseTTL time.Duration) (QueuedRun, bool, error)
	RenewRunClaim(ctx context.Context, runID, workerID string, generation int64, leaseTTL time.Duration) (bool, error)
	RetryRunClaim(ctx context.Context, runID, workerID string, generation int64, availableAt time.Time, errorCode string) (bool, error)
	PauseRunClaim(ctx context.Context, runID, workerID string, generation int64) (bool, error)
	PauseRun(ctx context.Context, runID, message string, maxAttempts int, traceContext core.TelemetryTraceContext) (bool, error)
	FinishRunClaim(ctx context.Context, runID, workerID string, generation int64, status core.RunStatus, errorCode string) (bool, error)
	RecoverExpiredRunClaims(ctx context.Context, now time.Time) (requeued int64, failed int64, err error)
}

type RunQueueMetrics struct {
	Queued          int64
	Running         int64
	WaitingApproval int64
	OldestQueuedAt  time.Time
}

// RunQueueMetricsStore is an optional read-only operational snapshot. It stays
// outside RunQueueStore so third-party queues do not need metrics to execute.
type RunQueueMetricsStore interface {
	RunQueueMetrics(ctx context.Context) (RunQueueMetrics, error)
}

type SQLRunControlStore struct {
	db             *sql.DB
	dialect        SQLDialect
	maxInFlight    int
	maxSubmissions int
}

var (
	sqlCreateRun = sqlQuery{`INSERT INTO run_control
		(run_id, session_id, tenant_id, subject_id, status, cancel_requested, error_code, created_at, updated_at, completed_at)
		VALUES (?, ?, ?, ?, ?, 0, '', ?, ?, 0)`}
	sqlGetRun = sqlQuery{`SELECT run_id, session_id, tenant_id, subject_id, status,
		cancel_requested, error_code, created_at, updated_at, completed_at
		FROM run_control WHERE run_id = ?`}
	sqlFindActiveRun = sqlQuery{`SELECT run_id, session_id, tenant_id, subject_id, status,
		cancel_requested, error_code, created_at, updated_at, completed_at
		FROM run_control WHERE session_id = ? AND status IN ('running', 'waiting_approval')
		ORDER BY created_at DESC LIMIT 1`}
	sqlListRuns = sqlQuery{`SELECT run_id, session_id, tenant_id, subject_id, status,
		cancel_requested, error_code, created_at, updated_at, completed_at
		FROM run_control WHERE session_id = ? ORDER BY created_at DESC LIMIT ?`}
	sqlRequestRunCancel = sqlQuery{`UPDATE run_control SET cancel_requested = 1, updated_at = ?
		WHERE run_id = ? AND status = 'running'`}
	sqlRunCancelRequested = sqlQuery{`SELECT cancel_requested FROM run_control WHERE run_id = ?`}
	sqlHeartbeatRun       = sqlQuery{`UPDATE run_control SET updated_at = ? WHERE run_id = ? AND status = 'running'`}
	sqlFinishRun          = sqlQuery{`UPDATE run_control SET status = ?, error_code = ?, updated_at = ?, completed_at = ?
		WHERE run_id = ? AND status = 'running'`}
	sqlFailStaleRuns = sqlQuery{`UPDATE run_control SET status = 'failed', error_code = 'worker_lost',
		updated_at = ?, completed_at = ? WHERE status = 'running' AND updated_at < ?
		AND NOT EXISTS (SELECT 1 FROM run_queue WHERE run_queue.run_id = run_control.run_id)`}
	sqlEnqueueRunControl = sqlQuery{`INSERT INTO run_control
		(run_id, session_id, tenant_id, subject_id, status, cancel_requested, error_code, created_at, updated_at, completed_at)
		VALUES (?, ?, ?, ?, 'queued', 0, '', ?, ?, 0)`}
	sqlEnqueueRun = sqlQuery{`INSERT INTO run_queue
		(run_id, message, available_at, worker_id, lease_expires_at, attempt, max_attempts, trace_parent, trace_state)
		VALUES (?, ?, ?, '', 0, 0, ?, ?, ?)`}
	sqlCountInFlightRuns = sqlQuery{`SELECT COUNT(*) FROM run_control
		WHERE status IN ('queued', 'running', 'waiting_approval')`}
	sqlInsertRunSubmission = sqlQuery{`INSERT INTO run_submissions
		(tenant_id, subject_id, session_id, key_hash, request_digest, run_id, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING`}
	sqlGetRunSubmission = sqlQuery{`SELECT request_digest, run_id FROM run_submissions
		WHERE tenant_id = ? AND subject_id = ? AND session_id = ? AND key_hash = ?`}
	// The threshold query returns one scalar instead of streaming up to the
	// whole cap through database/sql. OFFSET still has to walk rows up to the
	// threshold, but keeps the result and Go-side allocations bounded.
	sqlSubmissionCapReached = sqlQuery{`SELECT CASE WHEN EXISTS
		(SELECT 1 FROM run_submissions LIMIT 1 OFFSET ?) THEN 1 ELSE 0 END`}
	sqlClaimCandidate = sqlQuery{`SELECT rc.run_id, rc.session_id, rc.tenant_id, rc.subject_id,
		rc.status, rc.cancel_requested, rc.error_code, rc.created_at, rc.updated_at, rc.completed_at,
		q.message, q.available_at, q.worker_id, q.lease_expires_at, q.attempt, q.max_attempts, q.generation,
		q.trace_parent, q.trace_state
		FROM run_control rc JOIN run_queue q ON q.run_id = rc.run_id
		WHERE rc.status = 'queued' AND rc.cancel_requested = 0 AND q.available_at <= ?
		AND NOT EXISTS (SELECT 1 FROM run_control active
			WHERE active.session_id = rc.session_id AND active.status IN ('running', 'waiting_approval'))
		ORDER BY q.available_at, rc.created_at LIMIT 1`}
	sqlClaimCandidatePostgres = sqlQuery{`SELECT rc.run_id, rc.session_id, rc.tenant_id, rc.subject_id,
		rc.status, rc.cancel_requested, rc.error_code, rc.created_at, rc.updated_at, rc.completed_at,
		q.message, q.available_at, q.worker_id, q.lease_expires_at, q.attempt, q.max_attempts, q.generation,
		q.trace_parent, q.trace_state
		FROM run_control rc JOIN run_queue q ON q.run_id = rc.run_id
		WHERE rc.status = 'queued' AND rc.cancel_requested = 0 AND q.available_at <= ?
		AND NOT EXISTS (SELECT 1 FROM run_control active
			WHERE active.session_id = rc.session_id AND active.status IN ('running', 'waiting_approval'))
		ORDER BY q.available_at, rc.created_at LIMIT 1
		FOR UPDATE OF rc SKIP LOCKED`}
	sqlClaimRunControl = sqlQuery{`UPDATE run_control SET status = 'running', updated_at = ?, error_code = ''
		WHERE run_id = ? AND status = 'queued' AND cancel_requested = 0`}
	sqlClaimRun = sqlQuery{`UPDATE run_queue SET worker_id = ?, lease_expires_at = ?, attempt = attempt + 1, generation = generation + 1
		WHERE run_id = ?`}
	sqlRenewRunClaim = sqlQuery{`UPDATE run_queue SET lease_expires_at = ? WHERE run_id = ? AND worker_id = ? AND generation = ? AND lease_expires_at > ?
		AND EXISTS (SELECT 1 FROM run_control WHERE run_control.run_id = run_queue.run_id AND status = 'running')`}
	sqlRetryClaimedRunControl = sqlQuery{`UPDATE run_control SET status = 'queued', error_code = ?, updated_at = ?
		WHERE run_id = ? AND status = 'running'
		AND EXISTS (SELECT 1 FROM run_queue WHERE run_queue.run_id = run_control.run_id
			AND worker_id = ? AND generation = ? AND lease_expires_at > ?)`}
	sqlRetryClaimedRun = sqlQuery{`UPDATE run_queue SET worker_id = '', lease_expires_at = 0, available_at = ?
		WHERE run_id = ? AND worker_id = ? AND generation = ? AND lease_expires_at > ?`}
	sqlPauseClaimedRunControl = sqlQuery{`UPDATE run_control SET status = 'waiting_approval', error_code = 'approval_pending', updated_at = ?
		WHERE run_id = ? AND status = 'running'
		AND EXISTS (SELECT 1 FROM run_queue WHERE run_queue.run_id = run_control.run_id
			AND worker_id = ? AND generation = ? AND lease_expires_at > ?)`}
	sqlPauseClaimedRun = sqlQuery{`UPDATE run_queue SET worker_id = '', lease_expires_at = 0,
		attempt = CASE WHEN attempt > 0 THEN attempt - 1 ELSE 0 END
		WHERE run_id = ? AND worker_id = ? AND generation = ? AND lease_expires_at > ?`}
	sqlPauseDirectRun = sqlQuery{`UPDATE run_control SET status = 'waiting_approval', error_code = 'approval_pending', updated_at = ?
		WHERE run_id = ? AND status = 'running'`}
	sqlInsertPausedRun = sqlQuery{`INSERT INTO run_queue
		(run_id, message, available_at, worker_id, lease_expires_at, attempt, max_attempts, generation, trace_parent, trace_state)
		VALUES (?, ?, ?, '', 0, 0, ?, 0, ?, ?)`}
	sqlFinishClaimedRun = sqlQuery{`UPDATE run_control SET status = ?, error_code = ?, updated_at = ?, completed_at = ?
		WHERE run_id = ? AND status = 'running'
		AND EXISTS (SELECT 1 FROM run_queue WHERE run_queue.run_id = run_control.run_id
			AND worker_id = ? AND generation = ? AND lease_expires_at > ?)`}
	sqlDeleteClaimedRun = sqlQuery{`DELETE FROM run_queue WHERE run_id = ? AND worker_id = ? AND generation = ?`}
	sqlDeleteRunQueue   = sqlQuery{`DELETE FROM run_queue WHERE run_id = ?`}
	sqlExpiredRunClaims = sqlQuery{`SELECT rc.run_id, rc.cancel_requested, q.attempt, q.max_attempts
		FROM run_control rc JOIN run_queue q ON q.run_id = rc.run_id
		WHERE rc.status = 'running' AND q.worker_id <> '' AND q.lease_expires_at <= ?`}
	sqlExpiredRunClaimsPostgres = sqlQuery{`SELECT rc.run_id, rc.cancel_requested, q.attempt, q.max_attempts
		FROM run_control rc JOIN run_queue q ON q.run_id = rc.run_id
		WHERE rc.status = 'running' AND q.worker_id <> '' AND q.lease_expires_at <= ?
		ORDER BY q.lease_expires_at, rc.run_id
		FOR UPDATE OF rc SKIP LOCKED`}
	sqlRequeueRunControl = sqlQuery{`UPDATE run_control SET status = 'queued', error_code = 'worker_lost_retry',
		updated_at = ? WHERE run_id = ? AND status = 'running'`}
	sqlRequeueRun = sqlQuery{`UPDATE run_queue SET worker_id = '', lease_expires_at = 0, available_at = ?
		WHERE run_id = ?`}
	sqlCancelQueuedRun = sqlQuery{`UPDATE run_control SET status = 'cancelled', cancel_requested = 1,
		error_code = 'cancel_requested', updated_at = ?, completed_at = ? WHERE run_id = ? AND status IN ('queued', 'waiting_approval')`}
	sqlClearRunClaim      = sqlQuery{`UPDATE run_queue SET worker_id = '', lease_expires_at = 0 WHERE run_id = ?`}
	sqlCancelRunApprovals = sqlQuery{`UPDATE approval_requests SET status = 'denied', decided_at = ?, decided_by = 'system:run_cancel'
		WHERE run_id = ? AND status = 'pending'`}
	sqlRunQueueMetrics = sqlQuery{`SELECT
		COALESCE(SUM(CASE WHEN rc.status = 'queued' THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN rc.status = 'running' THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN rc.status = 'waiting_approval' THEN 1 ELSE 0 END), 0),
		MIN(CASE WHEN rc.status = 'queued' THEN q.available_at ELSE NULL END)
		FROM run_control rc LEFT JOIN run_queue q ON q.run_id = rc.run_id`}
)

func NewSQLRunControlStore(db *sql.DB, dialect SQLDialect) (*SQLRunControlStore, error) {
	if db == nil {
		return nil, fmt.Errorf("run control store requires a database handle")
	}
	if err := validateSQLDialect(dialect); err != nil {
		return nil, err
	}
	return &SQLRunControlStore{db: db, dialect: dialect}, nil
}

func (s *SQLRunControlStore) inFlightCap() int {
	if s != nil && s.maxInFlight > 0 {
		return s.maxInFlight
	}
	return MaxInFlightRuns
}

func (s *SQLRunControlStore) ensureInFlightCapacity(ctx context.Context, tx *sql.Tx) error {
	var count int
	if err := tx.QueryRowContext(ctx, sqlCountInFlightRuns.bind(s.dialect)).Scan(&count); err != nil {
		return err
	}
	if count >= s.inFlightCap() {
		return fmt.Errorf("in-flight runs exceed maximum of %d", s.inFlightCap())
	}
	return nil
}

func (s *SQLRunControlStore) RunQueueMetrics(ctx context.Context) (RunQueueMetrics, error) {
	var metrics RunQueueMetrics
	var oldest sql.NullInt64
	err := s.db.QueryRowContext(ctx, sqlRunQueueMetrics.bind(s.dialect)).Scan(
		&metrics.Queued, &metrics.Running, &metrics.WaitingApproval, &oldest,
	)
	if err != nil {
		return RunQueueMetrics{}, err
	}
	if oldest.Valid && oldest.Int64 > 0 {
		metrics.OldestQueuedAt = time.UnixMilli(oldest.Int64).UTC()
	}
	return metrics, nil
}

func (s *SQLRunControlStore) CreateRun(ctx context.Context, record RunRecord) error {
	if err := validateRunRecord(record); err != nil {
		return err
	}
	now := record.CreatedAt
	if now.IsZero() {
		now = time.Now().UTC()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := s.ensureInFlightCapacity(ctx, tx); err != nil {
		if _, getErr := scanRun(tx.QueryRowContext(ctx, sqlGetRun.bind(s.dialect), record.RunID)); getErr == nil {
			return fmt.Errorf("%w: %s", core.ErrSessionConflict, record.RunID)
		}
		return err
	}
	if _, err := tx.ExecContext(ctx, sqlCreateRun.bind(s.dialect),
		record.RunID, record.SessionID, record.TenantID, record.SubjectID,
		RunStatusRunning, now.UnixMilli(), now.UnixMilli(),
	); err != nil {
		return duplicateAsConflict(record.RunID, err)
	}
	return tx.Commit()
}

func (s *SQLRunControlStore) EnqueueRun(ctx context.Context, run QueuedRun) error {
	if err := validateQueuedRun(run); err != nil {
		return err
	}
	now := run.CreatedAt
	if now.IsZero() {
		now = time.Now().UTC()
	}
	available := run.AvailableAt
	if available.IsZero() {
		available = now
	}
	maxAttempts := run.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = DefaultRunMaxAttempts
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := s.ensureInFlightCapacity(ctx, tx); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, sqlEnqueueRunControl.bind(s.dialect),
		run.RunID, run.SessionID, run.TenantID, run.SubjectID,
		now.UnixMilli(), now.UnixMilli(),
	); err != nil {
		return duplicateAsConflict(run.RunID, err)
	}
	if _, err := tx.ExecContext(ctx, sqlEnqueueRun.bind(s.dialect),
		run.RunID, run.Message, available.UnixMilli(), maxAttempts,
		run.TraceContext.TraceParent, run.TraceContext.TraceState,
	); err != nil {
		return duplicateAsConflict(run.RunID, err)
	}
	return tx.Commit()
}

// EnqueueRunOnce atomically binds one client idempotency key to a durable run.
// Replaying the same request returns the original current RunRecord. Reusing
// the key for another request fails without creating a second queue item.
func (s *SQLRunControlStore) submissionCap() int {
	if s != nil && s.maxSubmissions > 0 {
		return s.maxSubmissions
	}
	return MaxRunSubmissions
}

func (s *SQLRunControlStore) submissionCapReached(ctx context.Context, tx *sql.Tx) (bool, error) {
	threshold := s.submissionCap() - 1
	if threshold < 0 {
		threshold = 0
	}
	var reached int
	if err := tx.QueryRowContext(ctx, sqlSubmissionCapReached.bind(s.dialect), threshold).Scan(&reached); err != nil {
		return false, err
	}
	return reached != 0, nil
}

func (s *SQLRunControlStore) EnqueueRunOnce(ctx context.Context, run QueuedRun, idempotencyKey, requestDigest string) (RunRecord, bool, error) {
	if err := validateQueuedRun(run); err != nil {
		return RunRecord{}, false, err
	}
	if err := validateRunSubmission(idempotencyKey, requestDigest); err != nil {
		return RunRecord{}, false, err
	}
	now := run.CreatedAt
	if now.IsZero() {
		now = time.Now().UTC()
	}
	available := run.AvailableAt
	if available.IsZero() {
		available = now
	}
	maxAttempts := run.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = DefaultRunMaxAttempts
	}
	keyHash := hashIdempotencyKey(idempotencyKey)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return RunRecord{}, false, err
	}
	defer func() { _ = tx.Rollback() }()
	full, err := s.submissionCapReached(ctx, tx)
	if err != nil {
		return RunRecord{}, false, err
	}
	if full {
		var existingDigest, existingRunID string
		err := tx.QueryRowContext(ctx, sqlGetRunSubmission.bind(s.dialect),
			run.TenantID, run.SubjectID, run.SessionID, keyHash,
		).Scan(&existingDigest, &existingRunID)
		if errors.Is(err, sql.ErrNoRows) {
			return RunRecord{}, false, fmt.Errorf("run submissions exceed maximum of %d", s.submissionCap())
		}
		if err != nil {
			return RunRecord{}, false, err
		}
		if existingDigest != requestDigest {
			return RunRecord{}, false, fmt.Errorf("%w: key was already used for a different request", ErrRunSubmissionConflict)
		}
		record, err := scanRun(tx.QueryRowContext(ctx, sqlGetRun.bind(s.dialect), existingRunID))
		if err != nil {
			return RunRecord{}, false, err
		}
		if err := tx.Commit(); err != nil {
			return RunRecord{}, false, err
		}
		return record, false, nil
	}
	result, err := tx.ExecContext(ctx, sqlInsertRunSubmission.bind(s.dialect),
		run.TenantID, run.SubjectID, run.SessionID, keyHash, requestDigest, run.RunID, now.UnixMilli(),
	)
	if err != nil {
		return RunRecord{}, false, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return RunRecord{}, false, err
	}
	if affected == 0 {
		var existingDigest, existingRunID string
		err := tx.QueryRowContext(ctx, sqlGetRunSubmission.bind(s.dialect),
			run.TenantID, run.SubjectID, run.SessionID, keyHash,
		).Scan(&existingDigest, &existingRunID)
		if errors.Is(err, sql.ErrNoRows) {
			return RunRecord{}, false, fmt.Errorf("%w: key conflicts with another run id", ErrRunSubmissionConflict)
		}
		if err != nil {
			return RunRecord{}, false, err
		}
		if existingDigest != requestDigest {
			return RunRecord{}, false, fmt.Errorf("%w: key was already used for a different request", ErrRunSubmissionConflict)
		}
		record, err := scanRun(tx.QueryRowContext(ctx, sqlGetRun.bind(s.dialect), existingRunID))
		if err != nil {
			return RunRecord{}, false, err
		}
		if err := tx.Commit(); err != nil {
			return RunRecord{}, false, err
		}
		return record, false, nil
	}
	if err := s.ensureInFlightCapacity(ctx, tx); err != nil {
		return RunRecord{}, false, err
	}
	if _, err := tx.ExecContext(ctx, sqlEnqueueRunControl.bind(s.dialect),
		run.RunID, run.SessionID, run.TenantID, run.SubjectID,
		now.UnixMilli(), now.UnixMilli(),
	); err != nil {
		return RunRecord{}, false, duplicateAsConflict(run.RunID, err)
	}
	if _, err := tx.ExecContext(ctx, sqlEnqueueRun.bind(s.dialect),
		run.RunID, run.Message, available.UnixMilli(), maxAttempts,
		run.TraceContext.TraceParent, run.TraceContext.TraceState,
	); err != nil {
		return RunRecord{}, false, duplicateAsConflict(run.RunID, err)
	}
	if err := tx.Commit(); err != nil {
		return RunRecord{}, false, err
	}
	return RunRecord{
		RunID: run.RunID, SessionID: run.SessionID, TenantID: run.TenantID,
		SubjectID: run.SubjectID, Status: RunStatusQueued, CreatedAt: now, UpdatedAt: now,
	}, true, nil
}

// RunMessageDigest returns the stable v1 request digest used by async-submit
// idempotency. The version prefix permits future request-shape evolution.
func RunMessageDigest(message string) string {
	sum := sha256.Sum256([]byte("harness.run.async/v1\x00" + message))
	return hex.EncodeToString(sum[:])
}

func hashIdempotencyKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

func (s *SQLRunControlStore) ClaimRun(ctx context.Context, workerID string, leaseTTL time.Duration) (QueuedRun, bool, error) {
	if err := validateWorkerID(workerID); err != nil {
		return QueuedRun{}, false, err
	}
	if leaseTTL <= 0 {
		return QueuedRun{}, false, fmt.Errorf("run claim lease ttl must be positive")
	}
	for attempt := 0; attempt < 3; attempt++ {
		now := time.Now().UTC()
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return QueuedRun{}, false, err
		}
		candidate := sqlClaimCandidate
		if s.dialect == SQLDialectPostgres {
			candidate = sqlClaimCandidatePostgres
		}
		run, err := scanQueuedRun(tx.QueryRowContext(ctx, candidate.bind(s.dialect), now.UnixMilli()))
		if errors.Is(err, ErrRunNotFound) {
			_ = tx.Rollback()
			return QueuedRun{}, false, nil
		}
		if err != nil {
			_ = tx.Rollback()
			return QueuedRun{}, false, err
		}
		result, err := tx.ExecContext(ctx, sqlClaimRunControl.bind(s.dialect), now.UnixMilli(), run.RunID)
		if err != nil {
			_ = tx.Rollback()
			if isDuplicateConstraint(err) {
				continue
			}
			return QueuedRun{}, false, err
		}
		affected, err := result.RowsAffected()
		if err != nil {
			_ = tx.Rollback()
			return QueuedRun{}, false, err
		}
		if affected == 0 {
			_ = tx.Rollback()
			continue
		}
		leaseExpiry := now.Add(leaseTTL)
		if _, err := tx.ExecContext(ctx, sqlClaimRun.bind(s.dialect), workerID, leaseExpiry.UnixMilli(), run.RunID); err != nil {
			_ = tx.Rollback()
			return QueuedRun{}, false, err
		}
		if err := tx.Commit(); err != nil {
			if isDuplicateConstraint(err) {
				continue
			}
			return QueuedRun{}, false, err
		}
		run.Status = RunStatusRunning
		run.WorkerID = workerID
		run.LeaseExpiresAt = leaseExpiry
		run.Attempt++
		run.Generation++
		run.UpdatedAt = now
		return run, true, nil
	}
	return QueuedRun{}, false, nil
}

func (s *SQLRunControlStore) RenewRunClaim(ctx context.Context, runID, workerID string, generation int64, leaseTTL time.Duration) (bool, error) {
	if err := core.ValidateRunID(runID); err != nil {
		return false, err
	}
	if err := validateWorkerID(workerID); err != nil {
		return false, err
	}
	if generation < 1 {
		return false, fmt.Errorf("run claim generation must be positive")
	}
	if leaseTTL <= 0 {
		return false, fmt.Errorf("run claim lease ttl must be positive")
	}
	now := time.Now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, sqlRenewRunClaim.bind(s.dialect),
		now.Add(leaseTTL).UnixMilli(), runID, workerID, generation, now.UnixMilli(),
	)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	if err != nil || affected == 0 {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, sqlHeartbeatRun.bind(s.dialect), now.UnixMilli(), runID); err != nil {
		return false, err
	}
	return true, tx.Commit()
}

// RetryRunClaim releases one live claim back to the queue. Generation is the
// fencing token: a worker whose lease expired cannot release a newer claim.
func (s *SQLRunControlStore) RetryRunClaim(ctx context.Context, runID, workerID string, generation int64, availableAt time.Time, errorCode string) (bool, error) {
	if err := validateClaimMutation(runID, workerID, generation); err != nil {
		return false, err
	}
	if availableAt.IsZero() {
		return false, fmt.Errorf("run retry availability time is zero")
	}
	errorCode = boundedRunErrorCode(errorCode)
	now := time.Now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, sqlRetryClaimedRunControl.bind(s.dialect),
		errorCode, now.UnixMilli(), runID, workerID, generation, now.UnixMilli(),
	)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	if err != nil || affected == 0 {
		return false, err
	}
	result, err = tx.ExecContext(ctx, sqlRetryClaimedRun.bind(s.dialect),
		availableAt.UTC().UnixMilli(), runID, workerID, generation, now.UnixMilli(),
	)
	if err != nil {
		return false, err
	}
	affected, err = result.RowsAffected()
	if err != nil || affected == 0 {
		return false, err
	}
	return true, tx.Commit()
}

// PauseRunClaim releases a live worker claim into waiting_approval without
// consuming a failure attempt. Generation remains monotonic for fencing.
func (s *SQLRunControlStore) PauseRunClaim(ctx context.Context, runID, workerID string, generation int64) (bool, error) {
	if err := validateClaimMutation(runID, workerID, generation); err != nil {
		return false, err
	}
	now := time.Now().UTC().UnixMilli()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, sqlPauseClaimedRunControl.bind(s.dialect), now, runID, workerID, generation, now)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	if err != nil || affected == 0 {
		return false, err
	}
	result, err = tx.ExecContext(ctx, sqlPauseClaimedRun.bind(s.dialect), runID, workerID, generation, now)
	if err != nil {
		return false, err
	}
	affected, err = result.RowsAffected()
	if err != nil || affected == 0 {
		return false, err
	}
	return true, tx.Commit()
}

// PauseRun converts a synchronous running record into a durable queued
// continuation after the Session has persisted approval/requested.
func (s *SQLRunControlStore) PauseRun(ctx context.Context, runID, message string, maxAttempts int, traceContext core.TelemetryTraceContext) (bool, error) {
	if err := core.ValidateRunID(runID); err != nil {
		return false, err
	}
	if strings.TrimSpace(message) == "" || len(message) > MaxQueuedMessageBytes {
		return false, fmt.Errorf("paused run message is empty or too large")
	}
	if maxAttempts == 0 {
		maxAttempts = DefaultRunMaxAttempts
	}
	if maxAttempts < 1 || maxAttempts > HardRunMaxAttempts {
		return false, fmt.Errorf("paused run max attempts must be between 1 and %d", HardRunMaxAttempts)
	}
	if err := validateTraceContext(traceContext); err != nil {
		return false, err
	}
	now := time.Now().UTC().UnixMilli()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, sqlPauseDirectRun.bind(s.dialect), now, runID)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	if err != nil || affected == 0 {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, sqlInsertPausedRun.bind(s.dialect),
		runID, message, now, maxAttempts, traceContext.TraceParent, traceContext.TraceState,
	); err != nil {
		return false, duplicateAsConflict(runID, err)
	}
	return true, tx.Commit()
}

// FinishRunClaim commits a terminal state only for the worker that still owns
// the live claim. Generation is the fencing token for successive claims.
func (s *SQLRunControlStore) FinishRunClaim(ctx context.Context, runID, workerID string, generation int64, status core.RunStatus, errorCode string) (bool, error) {
	if err := validateClaimMutation(runID, workerID, generation); err != nil {
		return false, err
	}
	if err := validateTerminalRunStatus(status); err != nil {
		return false, err
	}
	errorCode = boundedRunErrorCode(errorCode)
	now := time.Now().UTC().UnixMilli()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, sqlFinishClaimedRun.bind(s.dialect),
		string(status), errorCode, now, now, runID, workerID, generation, now,
	)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	if err != nil || affected == 0 {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, sqlDeleteClaimedRun.bind(s.dialect), runID, workerID, generation); err != nil {
		return false, err
	}
	return true, tx.Commit()
}

func (s *SQLRunControlStore) RecoverExpiredRunClaims(ctx context.Context, now time.Time) (int64, int64, error) {
	if now.IsZero() {
		return 0, 0, fmt.Errorf("claim recovery time is zero")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = tx.Rollback() }()
	expiredQuery := sqlExpiredRunClaims
	if s.dialect == SQLDialectPostgres {
		expiredQuery = sqlExpiredRunClaimsPostgres
	}
	rows, err := tx.QueryContext(ctx, expiredQuery.bind(s.dialect), now.UTC().UnixMilli())
	if err != nil {
		return 0, 0, err
	}
	type expiredClaim struct {
		runID           string
		cancelRequested int
		attempt         int
		maxAttempts     int
	}
	claims := []expiredClaim{}
	for rows.Next() {
		var claim expiredClaim
		if err := rows.Scan(&claim.runID, &claim.cancelRequested, &claim.attempt, &claim.maxAttempts); err != nil {
			_ = rows.Close()
			return 0, 0, err
		}
		claims = append(claims, claim)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, 0, err
	}
	_ = rows.Close()
	var requeued, failed int64
	for _, claim := range claims {
		nowMillis := now.UTC().UnixMilli()
		switch {
		case claim.cancelRequested != 0:
			if _, err := tx.ExecContext(ctx, sqlFinishRun.bind(s.dialect), string(core.RunCancelled), "cancel_requested", nowMillis, nowMillis, claim.runID); err != nil {
				return 0, 0, err
			}
			failed++
		case claim.attempt < claim.maxAttempts:
			if _, err := tx.ExecContext(ctx, sqlRequeueRunControl.bind(s.dialect), nowMillis, claim.runID); err != nil {
				return 0, 0, err
			}
			if _, err := tx.ExecContext(ctx, sqlRequeueRun.bind(s.dialect), nowMillis, claim.runID); err != nil {
				return 0, 0, err
			}
			requeued++
		default:
			if _, err := tx.ExecContext(ctx, sqlFinishRun.bind(s.dialect), string(core.RunFailed), "worker_lost", nowMillis, nowMillis, claim.runID); err != nil {
				return 0, 0, err
			}
			failed++
		}
		if claim.cancelRequested != 0 || claim.attempt >= claim.maxAttempts {
			if _, err := tx.ExecContext(ctx, sqlDeleteRunQueue.bind(s.dialect), claim.runID); err != nil {
				return 0, 0, err
			}
		} else if _, err := tx.ExecContext(ctx, sqlClearRunClaim.bind(s.dialect), claim.runID); err != nil {
			return 0, 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, 0, err
	}
	return requeued, failed, nil
}

func (s *SQLRunControlStore) GetRun(ctx context.Context, runID string) (RunRecord, error) {
	if err := core.ValidateRunID(runID); err != nil {
		return RunRecord{}, err
	}
	return scanRun(s.db.QueryRowContext(ctx, sqlGetRun.bind(s.dialect), runID))
}

func (s *SQLRunControlStore) FindActiveRun(ctx context.Context, sessionID string) (RunRecord, error) {
	if err := core.ValidateSessionID(sessionID); err != nil {
		return RunRecord{}, err
	}
	return scanRun(s.db.QueryRowContext(ctx, sqlFindActiveRun.bind(s.dialect), sessionID))
}

func (s *SQLRunControlStore) ListRuns(ctx context.Context, sessionID string, limit int) ([]RunRecord, error) {
	if err := core.ValidateSessionID(sessionID); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}
	rows, err := s.db.QueryContext(ctx, sqlListRuns.bind(s.dialect), sessionID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []RunRecord{}
	for rows.Next() {
		record, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	return out, rows.Err()
}

func (s *SQLRunControlStore) RequestRunCancel(ctx context.Context, runID string) (bool, error) {
	if err := core.ValidateRunID(runID); err != nil {
		return false, err
	}
	now := time.Now().UTC().UnixMilli()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, sqlRequestRunCancel.bind(s.dialect), now, runID)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if affected > 0 {
		if err := tx.Commit(); err != nil {
			return false, err
		}
		return true, nil
	}
	result, err = tx.ExecContext(ctx, sqlCancelQueuedRun.bind(s.dialect), now, now, runID)
	if err != nil {
		return false, err
	}
	affected, err = result.RowsAffected()
	if err != nil {
		return false, err
	}
	if affected > 0 {
		if _, err := tx.ExecContext(ctx, sqlCancelRunApprovals.bind(s.dialect), now, runID); err != nil {
			return false, err
		}
		if _, err := tx.ExecContext(ctx, sqlDeleteRunQueue.bind(s.dialect), runID); err != nil {
			return false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return affected > 0, nil
}

func (s *SQLRunControlStore) RunCancelRequested(ctx context.Context, runID string) (bool, error) {
	if err := core.ValidateRunID(runID); err != nil {
		return false, err
	}
	var requested int
	err := s.db.QueryRowContext(ctx, sqlRunCancelRequested.bind(s.dialect), runID).Scan(&requested)
	if errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("%w: %s", ErrRunNotFound, runID)
	}
	return requested != 0, err
}

func (s *SQLRunControlStore) HeartbeatRun(ctx context.Context, runID string) error {
	if err := core.ValidateRunID(runID); err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx, sqlHeartbeatRun.bind(s.dialect), time.Now().UTC().UnixMilli(), runID)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return fmt.Errorf("%w or already terminal: %s", ErrRunNotFound, runID)
	}
	return nil
}

func (s *SQLRunControlStore) FinishRun(ctx context.Context, runID string, status core.RunStatus, errorCode string) error {
	if err := core.ValidateRunID(runID); err != nil {
		return err
	}
	if err := validateTerminalRunStatus(status); err != nil {
		return err
	}
	errorCode = boundedRunErrorCode(errorCode)
	now := time.Now().UTC().UnixMilli()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, sqlFinishRun.bind(s.dialect), string(status), errorCode, now, now, runID)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return fmt.Errorf("%w or already terminal: %s", ErrRunNotFound, runID)
	}
	if _, err := tx.ExecContext(ctx, sqlDeleteRunQueue.bind(s.dialect), runID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SQLRunControlStore) FailStaleRuns(ctx context.Context, olderThan time.Time) (int64, error) {
	if olderThan.IsZero() {
		return 0, fmt.Errorf("stale run cutoff is zero")
	}
	now := time.Now().UTC().UnixMilli()
	result, err := s.db.ExecContext(ctx, sqlFailStaleRuns.bind(s.dialect), now, now, olderThan.UTC().UnixMilli())
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

type runScanner interface{ Scan(...any) error }

func scanRun(scanner runScanner) (RunRecord, error) {
	var record RunRecord
	var cancelRequested int
	var createdMillis, updatedMillis, completedMillis int64
	err := scanner.Scan(
		&record.RunID, &record.SessionID, &record.TenantID, &record.SubjectID,
		&record.Status, &cancelRequested, &record.ErrorCode,
		&createdMillis, &updatedMillis, &completedMillis,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return RunRecord{}, ErrRunNotFound
	}
	if err != nil {
		return RunRecord{}, err
	}
	record.CancelRequested = cancelRequested != 0
	record.CreatedAt = time.UnixMilli(createdMillis).UTC()
	record.UpdatedAt = time.UnixMilli(updatedMillis).UTC()
	if completedMillis > 0 {
		record.CompletedAt = time.UnixMilli(completedMillis).UTC()
	}
	return record, nil
}

func scanQueuedRun(scanner runScanner) (QueuedRun, error) {
	var run QueuedRun
	var cancelRequested int
	var createdMillis, updatedMillis, completedMillis int64
	var availableMillis, leaseMillis int64
	err := scanner.Scan(
		&run.RunID, &run.SessionID, &run.TenantID, &run.SubjectID,
		&run.Status, &cancelRequested, &run.ErrorCode,
		&createdMillis, &updatedMillis, &completedMillis,
		&run.Message, &availableMillis, &run.WorkerID, &leaseMillis,
		&run.Attempt, &run.MaxAttempts, &run.Generation,
		&run.TraceContext.TraceParent, &run.TraceContext.TraceState,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return QueuedRun{}, ErrRunNotFound
	}
	if err != nil {
		return QueuedRun{}, err
	}
	run.CancelRequested = cancelRequested != 0
	run.CreatedAt = time.UnixMilli(createdMillis).UTC()
	run.UpdatedAt = time.UnixMilli(updatedMillis).UTC()
	if completedMillis > 0 {
		run.CompletedAt = time.UnixMilli(completedMillis).UTC()
	}
	run.AvailableAt = time.UnixMilli(availableMillis).UTC()
	if leaseMillis > 0 {
		run.LeaseExpiresAt = time.UnixMilli(leaseMillis).UTC()
	}
	return run, nil
}

func validateRunRecord(record RunRecord) error {
	if err := core.ValidateRunID(record.RunID); err != nil {
		return err
	}
	if err := core.ValidateSessionID(record.SessionID); err != nil {
		return err
	}
	if record.TenantID == "" || record.SubjectID == "" {
		return fmt.Errorf("run record ownership is incomplete")
	}
	if record.Status != "" && record.Status != RunStatusRunning {
		return fmt.Errorf("new run status must be %q", RunStatusRunning)
	}
	return nil
}

func validateQueuedRun(run QueuedRun) error {
	if err := core.ValidateRunID(run.RunID); err != nil {
		return err
	}
	if err := core.ValidateSessionID(run.SessionID); err != nil {
		return err
	}
	if run.TenantID == "" || run.SubjectID == "" {
		return fmt.Errorf("queued run ownership is incomplete")
	}
	if strings.TrimSpace(run.Message) == "" {
		return fmt.Errorf("queued run message is empty")
	}
	if len(run.Message) > MaxQueuedMessageBytes {
		return fmt.Errorf("queued run message exceeds %d bytes", MaxQueuedMessageBytes)
	}
	if run.Status != "" && run.Status != RunStatusQueued {
		return fmt.Errorf("queued run status must be %q", RunStatusQueued)
	}
	if run.MaxAttempts < 0 || run.MaxAttempts > HardRunMaxAttempts {
		return fmt.Errorf("queued run max attempts must be between 0 and %d", HardRunMaxAttempts)
	}
	if err := validateTraceContext(run.TraceContext); err != nil {
		return err
	}
	return nil
}

func validateTraceContext(traceContext core.TelemetryTraceContext) error {
	if len(traceContext.TraceParent) > 256 || len(traceContext.TraceState) > 512 ||
		containsASCIIControl(traceContext.TraceParent) || containsASCIIControl(traceContext.TraceState) {
		return fmt.Errorf("durable trace context is too large or contains control characters")
	}
	return nil
}

func validateWorkerID(workerID string) error {
	workerID = strings.TrimSpace(workerID)
	if workerID == "" || len(workerID) > 256 || strings.ContainsAny(workerID, "\r\n\x00") {
		return fmt.Errorf("worker id is empty, too long, or contains control characters")
	}
	return nil
}

func validateRunSubmission(idempotencyKey, requestDigest string) error {
	if len(idempotencyKey) < 1 || len(idempotencyKey) > 256 || strings.TrimSpace(idempotencyKey) == "" || containsASCIIControl(idempotencyKey) {
		return fmt.Errorf("%w: key must be 1-256 non-control characters", ErrInvalidRunSubmission)
	}
	if len(requestDigest) != sha256.Size*2 {
		return fmt.Errorf("%w: request digest is not SHA-256", ErrInvalidRunSubmission)
	}
	if _, err := hex.DecodeString(requestDigest); err != nil || strings.ToLower(requestDigest) != requestDigest {
		return fmt.Errorf("%w: request digest is invalid", ErrInvalidRunSubmission)
	}
	return nil
}

func containsASCIIControl(value string) bool {
	for _, char := range value {
		if unicode.IsControl(char) {
			return true
		}
	}
	return false
}

func validateClaimMutation(runID, workerID string, generation int64) error {
	if err := core.ValidateRunID(runID); err != nil {
		return err
	}
	if err := validateWorkerID(workerID); err != nil {
		return err
	}
	if generation < 1 {
		return fmt.Errorf("run claim generation must be positive")
	}
	return nil
}

func validateTerminalRunStatus(status core.RunStatus) error {
	switch status {
	case core.RunCompleted, core.RunLimited, core.RunFailed, core.RunCancelled:
		return nil
	default:
		return fmt.Errorf("invalid terminal run status %q", status)
	}
}

func boundedRunErrorCode(errorCode string) string {
	if len(errorCode) > 128 {
		return errorCode[:128]
	}
	return errorCode
}
