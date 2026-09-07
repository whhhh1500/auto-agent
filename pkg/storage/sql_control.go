package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"sync"
	"time"
)

// BindingJournal makes console-applied dynamic bindings durable: policy
// layers, capability disables, HTTP capability binds, and env credentials are
// journaled so a restart can re-apply them. Static credential values are
// deliberately NOT journaled (secret values never enter the database twice).
type BindingRecord struct {
	ID      string          `json:"id"`
	Kind    string          `json:"kind"`
	Summary json.RawMessage `json:"summary,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

type BindingJournal interface {
	Record(ctx context.Context, record BindingRecord) error
	Delete(ctx context.Context, id string) error
	List(ctx context.Context) ([]BindingRecord, error)
}

// BindingJournalReplacer is an optional atomic replacement capability for a
// durable binding. Callers that need replacement must not emulate it with a
// Delete followed by Record, because that exposes an absent durable binding
// and advances the authorization epoch twice.
type BindingJournalReplacer interface {
	Replace(ctx context.Context, oldID string, record BindingRecord) error
}

const (
	MaxAdminBindings       = 256
	MaxBindingPayloadBytes = 256 << 10
)

var (
	sqlInsertBinding = sqlQuery{"INSERT INTO admin_bindings (id, kind, summary, payload, created_at) VALUES (?, ?, ?, ?, ?)"}
	sqlDeleteBinding = sqlQuery{"DELETE FROM admin_bindings WHERE id = ?"}
	sqlUpdateBinding = sqlQuery{"UPDATE admin_bindings SET kind = ?, summary = ?, payload = ? WHERE id = ?"}
	sqlBindingExists = sqlQuery{"SELECT 1 FROM admin_bindings WHERE id = ?"}
	sqlListBindings  = sqlQuery{"SELECT id, kind, summary, payload FROM admin_bindings ORDER BY created_at, id"}
	sqlCountBindings = sqlQuery{"SELECT COUNT(*) FROM admin_bindings"}
)

// SQLBindingJournal implements BindingJournal on the shared schema.
type SQLBindingJournal struct {
	db          *sql.DB
	dialect     SQLDialect
	maxBindings int
}

func (s *SQLBindingJournal) atomicSessionFenceDomain() atomicSessionFenceDomain {
	domain, _ := newAtomicSessionFenceDomain(s.db, s.dialect)
	return domain
}

var _ BindingJournalReplacer = (*SQLBindingJournal)(nil)

func NewSQLBindingJournal(db *sql.DB, dialect SQLDialect) (*SQLBindingJournal, error) {
	if db == nil {
		return nil, fmt.Errorf("binding journal requires a database handle")
	}
	if err := validateSQLDialect(dialect); err != nil {
		return nil, err
	}
	return &SQLBindingJournal{db: db, dialect: dialect}, nil
}

func (s *SQLBindingJournal) bindingCap() int {
	if s != nil && s.maxBindings > 0 {
		return s.maxBindings
	}
	return MaxAdminBindings
}

func (s *SQLBindingJournal) Record(ctx context.Context, record BindingRecord) error {
	summary, payload, err := validateBindingRecord(record)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var count int
	if err := tx.QueryRowContext(ctx, sqlCountBindings.bind(s.dialect)).Scan(&count); err != nil {
		return err
	}
	if count >= s.bindingCap() {
		return fmt.Errorf("admin bindings exceed maximum of %d", s.bindingCap())
	}
	_, err = tx.ExecContext(ctx, sqlInsertBinding.bind(s.dialect),
		record.ID, record.Kind, summary, payload, time.Now().UTC().UnixMilli(),
	)
	if err != nil {
		return duplicateAsConflict(record.ID, err)
	}
	if err := bumpAuthorizationEpoch(ctx, tx, s.dialect); err != nil {
		return err
	}
	return tx.Commit()
}

// Replace atomically replaces oldID with record and advances the authorization
// epoch exactly once. A same-ID replacement preserves created_at and therefore
// List ordering; a different-ID replacement has no transient missing row.
func (s *SQLBindingJournal) Replace(ctx context.Context, oldID string, record BindingRecord) error {
	if err := validateBindingID(oldID); err != nil {
		return err
	}
	summary, payload, err := validateBindingRecord(record)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var exists int
	if err := tx.QueryRowContext(ctx, sqlBindingExists.bind(s.dialect), oldID).Scan(&exists); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("binding %s not found", oldID)
		}
		return err
	}
	if oldID == record.ID {
		result, err := tx.ExecContext(ctx, sqlUpdateBinding.bind(s.dialect), record.Kind, summary, payload, oldID)
		if err != nil {
			return err
		}
		if err := requireAffected(result, "binding "+oldID+" not found"); err != nil {
			return err
		}
	} else {
		if _, err := tx.ExecContext(ctx, sqlInsertBinding.bind(s.dialect), record.ID, record.Kind, summary, payload, time.Now().UTC().UnixMilli()); err != nil {
			return duplicateAsConflict(record.ID, err)
		}
		result, err := tx.ExecContext(ctx, sqlDeleteBinding.bind(s.dialect), oldID)
		if err != nil {
			return err
		}
		if err := requireAffected(result, "binding "+oldID+" not found"); err != nil {
			return err
		}
	}
	if err := bumpAuthorizationEpoch(ctx, tx, s.dialect); err != nil {
		return err
	}
	return tx.Commit()
}

func validateBindingID(id string) error {
	if err := validateSQLTextFilter("binding id", id); err != nil {
		return err
	}
	if id == "" {
		return fmt.Errorf("binding id is empty")
	}
	return nil
}

func validateBindingRecord(record BindingRecord) (summary, payload string, _ error) {
	if err := validateSQLTextFilter("binding id", record.ID); err != nil {
		return "", "", err
	}
	if record.ID == "" {
		return "", "", fmt.Errorf("binding id is empty")
	}
	if err := validateSQLTextFilter("binding kind", record.Kind); err != nil {
		return "", "", err
	}
	if record.Kind == "" {
		return "", "", fmt.Errorf("binding kind is empty")
	}
	summaryJSON, err := json.Marshal(record.Summary)
	if err != nil {
		return "", "", fmt.Errorf("encode binding summary: %w", err)
	}
	payloadJSON, err := json.Marshal(record.Payload)
	if err != nil {
		return "", "", fmt.Errorf("encode binding payload: %w", err)
	}
	if len(summaryJSON)+len(payloadJSON) > MaxBindingPayloadBytes {
		return "", "", fmt.Errorf("binding payload exceeds %d bytes", MaxBindingPayloadBytes)
	}
	return string(summaryJSON), string(payloadJSON), nil
}

func (s *SQLBindingJournal) Delete(ctx context.Context, id string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, sqlDeleteBinding.bind(s.dialect), id)
	if err != nil {
		return err
	}
	if err := requireAffected(result, "binding "+id+" not found"); err != nil {
		return err
	}
	if err := bumpAuthorizationEpoch(ctx, tx, s.dialect); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SQLBindingJournal) List(ctx context.Context) ([]BindingRecord, error) {
	rows, err := s.db.QueryContext(ctx, sqlListBindings.bind(s.dialect))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []BindingRecord{}
	for rows.Next() {
		var record BindingRecord
		var summaryJSON, payloadJSON string
		if err := rows.Scan(&record.ID, &record.Kind, &summaryJSON, &payloadJSON); err != nil {
			return nil, err
		}
		record.Summary = json.RawMessage(summaryJSON)
		record.Payload = json.RawMessage(payloadJSON)
		out = append(out, record)
	}
	return out, rows.Err()
}

// Retention prunes: bounded-table hygiene for audit, hits, and leases.

var (
	sqlPruneAudit  = sqlQuery{"DELETE FROM audit_events WHERE time < ?"}
	sqlPruneHits   = sqlQuery{"DELETE FROM obs_hits WHERE time < ?"}
	sqlPruneLeases = sqlQuery{"DELETE FROM session_leases WHERE expires_at < ?"}
	// Sidecars currently have no GC path. Retain every completed journal proof
	// they reference rather than risk deleting an eligible historical result.
	// A future retention transaction may collect only terminal or superseded
	// sidecars and their journal rows together.
	sqlListPrunableToolInvocations = sqlQuery{`SELECT tenant_id, subject_id, session_id, run_id,
		call_id, capability_id, args_digest, idempotent
		FROM tool_invocations
		WHERE state = 'completed' AND completed_at > 0 AND completed_at < ?
		ORDER BY session_id, run_id, call_id, capability_id, args_digest, idempotent LIMIT 8192`}
	sqlListPrunableToolInvocationsPostgres = sqlQuery{`SELECT tenant_id, subject_id, session_id, run_id,
		call_id, capability_id, args_digest, idempotent
		FROM tool_invocations
		WHERE state = 'completed' AND completed_at > 0 AND completed_at < ?
		ORDER BY session_id, run_id, call_id, capability_id, args_digest, idempotent
		LIMIT 8192 FOR UPDATE`}
	sqlDeleteToolInvocation = sqlQuery{`DELETE FROM tool_invocations
		WHERE tenant_id = ? AND subject_id = ? AND session_id = ? AND run_id = ?
		AND call_id = ? AND capability_id = ? AND args_digest = ? AND idempotent = ?`}
	sqlSidecarExistsForToolInvocation = sqlQuery{`SELECT 1 FROM completed_tool_result_recovery_sidecars
		WHERE tenant_id = ? AND subject_id = ? AND session_id = ? AND run_id = ?
		AND call_id = ? AND capability_id = ? AND args_digest = ? AND idempotent = ?`}
	sqlPruneApprovals = sqlQuery{`DELETE FROM approval_requests
		WHERE status <> 'pending' AND decided_at > 0 AND decided_at < ?`}
	sqlPruneRunSubmissions = sqlQuery{`DELETE FROM run_submissions
		WHERE created_at < ? AND EXISTS (
			SELECT 1 FROM run_control rc WHERE rc.run_id = run_submissions.run_id
			AND rc.status NOT IN ('queued', 'running', 'waiting_approval'))`}
)

// PruneAudit deletes audit events older than the cutoff.
func (s *SQLSessionStore) PruneAudit(ctx context.Context, olderThan time.Time) (int64, error) {
	result, err := s.db.ExecContext(ctx, sqlPruneAudit.bind(s.dialect), olderThan.UnixMilli())
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// PruneHits deletes observability hits older than the cutoff.
func (s *SQLSessionStore) PruneHits(ctx context.Context, olderThan time.Time) (int64, error) {
	result, err := s.db.ExecContext(ctx, sqlPruneHits.bind(s.dialect), olderThan.UnixMilli())
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// PruneExpiredLeases deletes expired session lease rows.
func (s *SQLSessionStore) PruneExpiredLeases(ctx context.Context) (int64, error) {
	result, err := s.db.ExecContext(ctx, sqlPruneLeases.bind(s.dialect), time.Now().UTC().UnixMilli())
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// PruneToolInvocations deletes only old completed outcomes not referenced by
// a V3 recovery sidecar. Started and uncertain rows are retained because
// deleting them could permit a duplicate non-idempotent side effect. Sidecar
// rows are intentionally not collected in this first storage-only slice.
func (s *SQLSessionStore) PruneToolInvocations(ctx context.Context, olderThan time.Time) (int64, error) {
	if s == nil || s.db == nil {
		return 0, fmt.Errorf("tool invocation pruning requires an SQL session store")
	}
	epoch, err := s.AuthorizationEpoch(ctx)
	if err != nil {
		return 0, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := lockAuthorizationEpoch(ctx, tx, s.dialect, epoch); err != nil {
		return 0, err
	}
	query := sqlListPrunableToolInvocations
	if s.dialect == SQLDialectPostgres {
		query = sqlListPrunableToolInvocationsPostgres
	}
	rows, err := tx.QueryContext(ctx, query.bind(s.dialect), olderThan.UTC().UnixMilli())
	if err != nil {
		return 0, err
	}
	type toolInvocationKey struct {
		tenantID, subjectID, sessionID, runID, callID, capabilityID, argsDigest string
		idempotent                                                              int
	}
	var candidates []toolInvocationKey
	for rows.Next() {
		var candidate toolInvocationKey
		if err := rows.Scan(&candidate.tenantID, &candidate.subjectID, &candidate.sessionID, &candidate.runID, &candidate.callID, &candidate.capabilityID, &candidate.argsDigest, &candidate.idempotent); err != nil {
			_ = rows.Close()
			return 0, err
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, err
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	var deleted int64
	for _, candidate := range candidates {
		var present int
		err := tx.QueryRowContext(ctx, sqlSidecarExistsForToolInvocation.bind(s.dialect), candidate.tenantID, candidate.subjectID, candidate.sessionID, candidate.runID, candidate.callID, candidate.capabilityID, candidate.argsDigest, candidate.idempotent).Scan(&present)
		if err == nil {
			continue
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return 0, err
		}
		result, err := tx.ExecContext(ctx, sqlDeleteToolInvocation.bind(s.dialect), candidate.tenantID, candidate.subjectID, candidate.sessionID, candidate.runID, candidate.callID, candidate.capabilityID, candidate.argsDigest, candidate.idempotent)
		if err != nil {
			return 0, err
		}
		count, err := result.RowsAffected()
		if err != nil {
			return 0, err
		}
		deleted += count
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return deleted, nil
}

// PruneApprovals deletes old decided requests. Pending approvals remain until
// they are decided or expired by the approval worker.
func (s *SQLSessionStore) PruneApprovals(ctx context.Context, olderThan time.Time) (int64, error) {
	result, err := s.db.ExecContext(ctx, sqlPruneApprovals.bind(s.dialect), olderThan.UTC().UnixMilli())
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// PruneRunSubmissions removes old idempotency mappings only after their Run is
// terminal. Active mappings remain fences regardless of age.
func (s *SQLSessionStore) PruneRunSubmissions(ctx context.Context, olderThan time.Time) (int64, error) {
	result, err := s.db.ExecContext(ctx, sqlPruneRunSubmissions.bind(s.dialect), olderThan.UTC().UnixMilli())
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// RunStat records the outcome of one finished run for metrics aggregation.
type RunStat struct {
	RunID        string    `json:"run_id"`
	SessionID    string    `json:"session_id"`
	TenantID     string    `json:"tenant"`
	Status       string    `json:"status"`
	InputTokens  int64     `json:"input_tokens"`
	OutputTokens int64     `json:"output_tokens"`
	DurationMS   int64     `json:"duration_ms"`
	CreatedAt    time.Time `json:"created_at"`
}

// RunMetrics is the aggregated view served to the console.
type RunMetrics struct {
	TotalRuns     int64   `json:"total_runs"`
	CompletedRuns int64   `json:"completed_runs"`
	FailedRuns    int64   `json:"failed_runs"`
	TokensIn      int64   `json:"tokens_in"`
	TokensOut     int64   `json:"tokens_out"`
	AvgDurationMS float64 `json:"avg_duration_ms"`
}

// RunStatsStore records finished runs and serves aggregated metrics.
type RunStatsStore interface {
	RecordRunStat(ctx context.Context, stat RunStat) error
	// Metrics aggregates over one tenant; empty tenant means all.
	Metrics(ctx context.Context, tenantID string) (RunMetrics, error)
}

var (
	sqlInsertRunStat = sqlQuery{"INSERT INTO run_stats (run_id, session_id, tenant, status, input_tokens, output_tokens, duration_ms, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"}
	sqlCountRunStats = sqlQuery{"SELECT COUNT(*) FROM run_stats"}
	sqlGetRunStat    = sqlQuery{"SELECT run_id FROM run_stats WHERE run_id = ?"}
	sqlMetricsRuns   = sqlQuery{"SELECT COUNT(*), COALESCE(SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END), 0), COALESCE(SUM(CASE WHEN status IN ('failed','cancelled') THEN 1 ELSE 0 END), 0), COALESCE(SUM(input_tokens), 0), COALESCE(SUM(output_tokens), 0), COALESCE(AVG(duration_ms), 0) FROM run_stats"}
)

// SQLRunStatsStore implements RunStatsStore on the shared schema.
const MaxRunStats = 8192

type SQLRunStatsStore struct {
	db       *sql.DB
	dialect  SQLDialect
	maxStats int
}

func NewSQLRunStatsStore(db *sql.DB, dialect SQLDialect) (*SQLRunStatsStore, error) {
	if db == nil {
		return nil, fmt.Errorf("run stats store requires a database handle")
	}
	if err := validateSQLDialect(dialect); err != nil {
		return nil, err
	}
	return &SQLRunStatsStore{db: db, dialect: dialect}, nil
}

func (s *SQLRunStatsStore) statsCap() int {
	if s != nil && s.maxStats > 0 {
		return s.maxStats
	}
	return MaxRunStats
}

func (s *SQLRunStatsStore) RecordRunStat(ctx context.Context, stat RunStat) error {
	if stat.CreatedAt.IsZero() {
		stat.CreatedAt = time.Now().UTC()
	}
	var stored int
	if err := s.db.QueryRowContext(ctx, sqlCountRunStats.bind(s.dialect)).Scan(&stored); err != nil {
		return err
	}
	if stored >= s.statsCap() {
		var existing string
		getErr := s.db.QueryRowContext(ctx, sqlGetRunStat.bind(s.dialect), stat.RunID).Scan(&existing)
		if getErr == nil {
			return fmt.Errorf("%w: %s", core.ErrSessionConflict, stat.RunID)
		}
		if !errors.Is(getErr, sql.ErrNoRows) {
			return getErr
		}
		return fmt.Errorf("run stats exceed maximum of %d", s.statsCap())
	}
	_, err := s.db.ExecContext(ctx, sqlInsertRunStat.bind(s.dialect),
		stat.RunID, stat.SessionID, stat.TenantID, stat.Status,
		stat.InputTokens, stat.OutputTokens, stat.DurationMS, stat.CreatedAt.UnixMilli(),
	)
	return duplicateAsConflict(stat.RunID, err)
}

// Metrics aggregates run outcomes and token metering, optionally per tenant.
func (s *SQLRunStatsStore) Metrics(ctx context.Context, tenantID string) (RunMetrics, error) {
	query := sqlMetricsRuns
	args := []any{}
	if tenantID != "" {
		query = sqlQuery{sqlMetricsRuns.text + " WHERE tenant = ?"}
		args = append(args, tenantID)
	}
	var metrics RunMetrics
	err := s.db.QueryRowContext(ctx, query.bind(s.dialect), args...).Scan(
		&metrics.TotalRuns, &metrics.CompletedRuns, &metrics.FailedRuns,
		&metrics.TokensIn, &metrics.TokensOut, &metrics.AvgDurationMS,
	)
	return metrics, err
}

// NamedLocks provides reference-counted named mutexes: the map entry for an
// id is removed when the last holder releases, so per-session locks no
// longer accumulate for the lifetime of the process.
type NamedLocks struct {
	mu    sync.Mutex
	locks map[string]*namedMutex
}

type namedMutex struct {
	mu   sync.Mutex
	refs int
}

// Lock and Unlock forward to the embedded mutex so callers can treat the
// named mutex like a regular one.
func (m *namedMutex) Lock()   { m.mu.Lock() }
func (m *namedMutex) Unlock() { m.mu.Unlock() }

func NewNamedLocks() *NamedLocks {
	return &NamedLocks{locks: map[string]*namedMutex{}}
}

// Acquire registers a reference and returns the mutex plus a release
// function. The caller owns Lock/Unlock on the returned mutex and must call
// release exactly once.
func (n *NamedLocks) Acquire(id string) (*namedMutex, func()) {
	n.mu.Lock()
	defer n.mu.Unlock()
	lock := n.locks[id]
	if lock == nil {
		lock = &namedMutex{}
		n.locks[id] = lock
	}
	lock.refs++
	return lock, func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		lock.refs--
		if lock.refs <= 0 && n.locks[id] == lock {
			delete(n.locks, id)
		}
	}
}
