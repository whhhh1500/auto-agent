package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/extensions/runner"
)

// SQLRunnerStore is the durable SQL implementation of runner.Store. The
// worker generation is a fencing token: every successful claim increments it,
// and claim mutations require the exact worker, generation, and a live lease.
type SQLRunnerStore struct {
	db          *sql.DB
	dialect     SQLDialect
	maxInFlight int
	maxStored   int
}

var (
	sqlInsertRunnerTask = sqlQuery{`INSERT INTO runner_tasks
		(id, retried_from_id, tenant_id, subject_id, scope, capability, idempotency_key,
		 args_digest, args_json, trace_parent, trace_state, state, cancel_requested, worker_id,
		 lease_expires_at, available_at, attempt, max_attempts, generation,
		 result_json, error_code, created_at, updated_at, completed_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', 0, '', 0, ?, 0, ?, 0, '', '', ?, ?, 0)`}
	sqlSelectRunnerTask = sqlQuery{`SELECT id, retried_from_id, tenant_id, subject_id, scope,
		capability, idempotency_key, args_digest, args_json, state,
		trace_parent, trace_state,
		cancel_requested, worker_id, lease_expires_at, available_at, attempt,
		max_attempts, generation, result_json, error_code, created_at,
		updated_at, completed_at
		FROM runner_tasks WHERE id = ?`}
	sqlSelectRunnerTaskByKey = sqlQuery{`SELECT id, retried_from_id, tenant_id, subject_id, scope,
		capability, idempotency_key, args_digest, args_json, state,
		trace_parent, trace_state,
		cancel_requested, worker_id, lease_expires_at, available_at, attempt,
		max_attempts, generation, result_json, error_code, created_at,
		updated_at, completed_at
		FROM runner_tasks
		WHERE tenant_id = ? AND subject_id = ? AND scope = ?
		AND capability = ? AND idempotency_key = ?`}
	sqlSelectRunnerTaskByRetryParent = sqlQuery{`SELECT id, retried_from_id, tenant_id, subject_id, scope,
		capability, idempotency_key, args_digest, args_json, state, trace_parent, trace_state,
		cancel_requested, worker_id, lease_expires_at, available_at, attempt, max_attempts, generation,
		result_json, error_code, created_at, updated_at, completed_at FROM runner_tasks WHERE retried_from_id = ?`}
	sqlClaimRunnerTask = sqlQuery{`UPDATE runner_tasks SET state = 'claimed',
		worker_id = ?, lease_expires_at = ?, attempt = attempt + 1,
		generation = generation + 1, updated_at = ?, error_code = ''
		WHERE id = ? AND state = 'queued' AND cancel_requested = 0
		AND available_at <= ? AND attempt < max_attempts`}
	sqlRenewRunnerTask = sqlQuery{`UPDATE runner_tasks SET lease_expires_at = ?, updated_at = ?
		WHERE id = ? AND state = 'claimed' AND worker_id = ? AND generation = ?
		AND lease_expires_at > ?`}
	sqlCompleteRunnerTask = sqlQuery{`UPDATE runner_tasks SET state = 'completed',
		worker_id = '', lease_expires_at = 0, result_json = ?, error_code = '',
		updated_at = ?, completed_at = ?
		WHERE id = ? AND state = 'claimed' AND worker_id = ? AND generation = ?
		AND lease_expires_at > ?`}
	sqlCancelQueuedRunnerTask = sqlQuery{`UPDATE runner_tasks SET state = 'cancelled',
		cancel_requested = 1, worker_id = '', lease_expires_at = 0,
		updated_at = ?, completed_at = ? WHERE id = ? AND state = 'queued'`}
	sqlRequestClaimedRunnerCancel = sqlQuery{`UPDATE runner_tasks SET cancel_requested = 1,
		updated_at = ? WHERE id = ? AND state = 'claimed'`}
	sqlExpiredRunnerTasks = sqlQuery{`SELECT id, cancel_requested, attempt, max_attempts
		FROM runner_tasks WHERE state = 'claimed' AND lease_expires_at <= ?
		ORDER BY lease_expires_at, id`}
	sqlExpiredRunnerTasksPostgres = sqlQuery{`SELECT id, cancel_requested, attempt, max_attempts
		FROM runner_tasks WHERE state = 'claimed' AND lease_expires_at <= ?
		ORDER BY lease_expires_at, id FOR UPDATE SKIP LOCKED`}
	sqlRecoverCancelledRunnerTask = sqlQuery{`UPDATE runner_tasks SET state = 'cancelled',
		worker_id = '', lease_expires_at = 0, result_json = '', error_code = '',
		updated_at = ?, completed_at = ?
		WHERE id = ? AND state = 'claimed' AND lease_expires_at <= ?
		AND cancel_requested = 1`}
	sqlRecoverQueuedRunnerTask = sqlQuery{`UPDATE runner_tasks SET state = 'queued',
		worker_id = '', lease_expires_at = 0, available_at = ?, result_json = '',
		error_code = '', updated_at = ?, completed_at = 0
		WHERE id = ? AND state = 'claimed' AND lease_expires_at <= ?
		AND cancel_requested = 0 AND attempt < max_attempts`}
	sqlRecoverFailedRunnerTask = sqlQuery{`UPDATE runner_tasks SET state = 'failed',
		worker_id = '', lease_expires_at = 0, result_json = '', error_code = 'worker_lost',
		updated_at = ?, completed_at = ?
		WHERE id = ? AND state = 'claimed' AND lease_expires_at <= ?
		AND cancel_requested = 0 AND attempt >= max_attempts`}
	sqlCountPendingRunnerTasks  = sqlQuery{`SELECT COUNT(*) FROM runner_tasks WHERE state = 'queued'`}
	sqlCountInFlightRunnerTasks = sqlQuery{`SELECT COUNT(*) FROM runner_tasks WHERE state IN ('queued', 'claimed')`}
	sqlCountRunnerTasks         = sqlQuery{`SELECT COUNT(*) FROM runner_tasks`}
	sqlPruneRunnerTasks         = sqlQuery{`DELETE FROM runner_tasks
		WHERE idempotency_key = ''
		AND state IN ('completed', 'cancelled', 'failed')
		AND completed_at > 0 AND completed_at < ?`}
)

func NewSQLRunnerStore(db *sql.DB, dialect SQLDialect) (*SQLRunnerStore, error) {
	if db == nil {
		return nil, fmt.Errorf("runner store requires a database handle")
	}
	if err := validateSQLDialect(dialect); err != nil {
		return nil, err
	}
	return &SQLRunnerStore{db: db, dialect: dialect}, nil
}

func (s *SQLRunnerStore) inFlightCap() int {
	if s != nil && s.maxInFlight > 0 {
		return s.maxInFlight
	}
	return runner.MaxInFlightTasks
}

func (s *SQLRunnerStore) storedCap() int {
	if s != nil && s.maxStored > 0 {
		return s.maxStored
	}
	return runner.MaxStoredTasks
}

func (s *SQLRunnerStore) CreateTask(ctx context.Context, task runner.Task) (runner.Task, bool, error) {
	prepared, argsJSON, err := prepareSQLRunnerTask(task)
	if err != nil {
		return runner.Task{}, false, err
	}
	if prepared.IdempotencyKey != "" {
		existing, err := s.getTaskByKey(ctx, s.db, prepared)
		switch {
		case err == nil:
			return replaySQLRunnerTask(existing, prepared)
		case !errors.Is(err, runner.ErrTaskNotFound):
			return runner.Task{}, false, err
		}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return runner.Task{}, false, err
	}
	defer func() { _ = tx.Rollback() }()
	var inFlight int
	if err := tx.QueryRowContext(ctx, sqlCountInFlightRunnerTasks.bind(s.dialect)).Scan(&inFlight); err != nil {
		return runner.Task{}, false, err
	}
	if inFlight >= s.inFlightCap() {
		return runner.Task{}, false, fmt.Errorf("runner in-flight tasks exceed maximum of %d", s.inFlightCap())
	}
	var stored int
	if err := tx.QueryRowContext(ctx, sqlCountRunnerTasks.bind(s.dialect)).Scan(&stored); err != nil {
		return runner.Task{}, false, err
	}
	if stored >= s.storedCap() {
		return runner.Task{}, false, fmt.Errorf("runner tasks exceed maximum of %d", s.storedCap())
	}
	result, err := tx.ExecContext(ctx, sqlInsertRunnerTask.bind(s.dialect),
		prepared.ID, prepared.RetriedFromID, prepared.TenantID, prepared.SubjectID, prepared.Scope,
		prepared.Capability, prepared.IdempotencyKey, prepared.ArgsDigest,
		argsJSON, prepared.TraceContext.TraceParent, prepared.TraceContext.TraceState,
		prepared.AvailableAt.UnixMilli(), prepared.MaxAttempts,
		prepared.CreatedAt.UnixMilli(), prepared.UpdatedAt.UnixMilli(),
	)
	if err != nil {
		if isDuplicateConstraint(err) {
			_ = tx.Rollback()
			return s.resolveCreateConflict(ctx, prepared)
		}
		return runner.Task{}, false, err
	}
	if err := requireRowsAffected(result, 1, "insert runner task"); err != nil {
		return runner.Task{}, false, err
	}
	created, err := getSQLRunnerTask(ctx, tx, s.dialect, prepared.ID)
	if err != nil {
		return runner.Task{}, false, err
	}
	if err := tx.Commit(); err != nil {
		if isDuplicateConstraint(err) {
			return s.resolveCreateConflict(ctx, prepared)
		}
		return runner.Task{}, false, err
	}
	return created, true, nil
}

func (s *SQLRunnerStore) resolveCreateConflict(ctx context.Context, prepared runner.Task) (runner.Task, bool, error) {
	if prepared.IdempotencyKey != "" {
		existing, err := s.getTaskByKey(ctx, s.db, prepared)
		if err == nil {
			return replaySQLRunnerTask(existing, prepared)
		}
		if !errors.Is(err, runner.ErrTaskNotFound) {
			return runner.Task{}, false, err
		}
	}
	return runner.Task{}, false, runner.ErrSubmissionConflict
}

func replaySQLRunnerTask(existing, submitted runner.Task) (runner.Task, bool, error) {
	if existing.ArgsDigest != submitted.ArgsDigest {
		return runner.Task{}, false, runner.ErrSubmissionConflict
	}
	return existing, false, nil
}

func (s *SQLRunnerStore) GetTask(ctx context.Context, id string) (runner.Task, error) {
	if err := validateSQLRunnerTaskID(id); err != nil {
		return runner.Task{}, err
	}
	return getSQLRunnerTask(ctx, s.db, s.dialect, id)
}

func (s *SQLRunnerStore) RetryTask(ctx context.Context, id string) (runner.Task, bool, error) {
	original, err := s.GetTask(ctx, id)
	if err != nil {
		return runner.Task{}, false, err
	}
	if original.State != runner.TaskFailed {
		return runner.Task{}, false, fmt.Errorf("runner task %s is not failed", id)
	}
	if existing, err := s.getTaskByRetryParent(ctx, s.db, id); err == nil {
		return existing, false, nil
	} else if !errors.Is(err, runner.ErrTaskNotFound) {
		return runner.Task{}, false, err
	}
	child, created, err := s.CreateTask(ctx, runner.Task{RetriedFromID: id, Capability: original.Capability, TenantID: original.TenantID, SubjectID: original.SubjectID, Scope: original.Scope, Args: original.Args, ArgsDigest: original.ArgsDigest, TraceContext: original.TraceContext, MaxAttempts: original.MaxAttempts})
	if err != nil {
		if existing, getErr := s.getTaskByRetryParent(ctx, s.db, id); getErr == nil {
			return existing, false, nil
		}
	}
	return child, created, err
}

func (s *SQLRunnerStore) ClaimTask(ctx context.Context, options runner.ClaimOptions) (runner.Task, bool, error) {
	options, err := normalizeSQLRunnerClaimOptions(options)
	if err != nil {
		return runner.Task{}, false, err
	}
	now := time.Now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return runner.Task{}, false, err
	}
	defer func() { _ = tx.Rollback() }()
	if _, _, err := s.recoverExpiredTasksTx(ctx, tx, now); err != nil {
		return runner.Task{}, false, err
	}
	claimQuery := buildRunnerClaimCandidate(s.dialect, options.Capabilities)
	claimArgs := make([]any, 0, 1+len(options.Capabilities))
	claimArgs = append(claimArgs, now.UnixMilli())
	for _, capability := range options.Capabilities {
		claimArgs = append(claimArgs, capability)
	}
	var id string
	err = tx.QueryRowContext(ctx, claimQuery.bind(s.dialect), claimArgs...).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		if err := tx.Commit(); err != nil {
			return runner.Task{}, false, err
		}
		return runner.Task{}, false, nil
	}
	if err != nil {
		return runner.Task{}, false, err
	}
	result, err := tx.ExecContext(ctx, sqlClaimRunnerTask.bind(s.dialect),
		options.WorkerID, sqlRunnerLeaseExpiryMillis(now, options.LeaseTTL), now.UnixMilli(), id, now.UnixMilli(),
	)
	if err != nil {
		return runner.Task{}, false, err
	}
	if err := requireRowsAffected(result, 1, "claim runner task"); err != nil {
		return runner.Task{}, false, err
	}
	claimed, err := getSQLRunnerTask(ctx, tx, s.dialect, id)
	if err != nil {
		return runner.Task{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return runner.Task{}, false, err
	}
	return claimed, true, nil
}

func buildRunnerClaimCandidate(dialect SQLDialect, capabilities []string) sqlQuery {
	var query strings.Builder
	query.WriteString(`SELECT id FROM runner_tasks
		WHERE state = 'queued' AND cancel_requested = 0 AND available_at <= ?
		AND attempt < max_attempts`)
	if len(capabilities) > 0 {
		query.WriteString(" AND capability IN (")
		for index := range capabilities {
			if index > 0 {
				query.WriteString(", ")
			}
			query.WriteByte('?')
		}
		query.WriteByte(')')
	}
	query.WriteString(" ORDER BY available_at, created_at, id LIMIT 1")
	if dialect == SQLDialectPostgres {
		query.WriteString(" FOR UPDATE SKIP LOCKED")
	}
	return sqlQuery{text: query.String()}
}

func (s *SQLRunnerStore) RenewTaskClaim(ctx context.Context, id, worker string, generation int64, ttl time.Duration) (bool, error) {
	if err := validateSQLRunnerClaim(id, worker, generation, ttl); err != nil {
		return false, err
	}
	now := time.Now().UTC()
	result, err := s.db.ExecContext(ctx, sqlRenewRunnerTask.bind(s.dialect),
		sqlRunnerLeaseExpiryMillis(now, ttl), now.UnixMilli(), id, worker, generation, now.UnixMilli(),
	)
	if err != nil {
		return false, err
	}
	matched, err := rowsAffectedZeroOrOne(result, "renew runner task claim")
	if err != nil || matched {
		return matched, err
	}
	if _, err := s.GetTask(ctx, id); errors.Is(err, runner.ErrTaskNotFound) {
		return false, err
	} else if err != nil {
		return false, err
	}
	return false, nil
}

func (s *SQLRunnerStore) CompleteTask(ctx context.Context, id, worker string, generation int64, outcome core.CapabilityResult) (runner.Task, bool, error) {
	if err := validateSQLRunnerClaim(id, worker, generation, time.Second); err != nil {
		return runner.Task{}, false, err
	}
	resultJSON, err := encodeSQLRunnerResult(outcome)
	if err != nil {
		return runner.Task{}, false, err
	}
	now := time.Now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return runner.Task{}, false, err
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, sqlCompleteRunnerTask.bind(s.dialect),
		resultJSON, now.UnixMilli(), now.UnixMilli(), id, worker, generation, now.UnixMilli(),
	)
	if err != nil {
		return runner.Task{}, false, err
	}
	matched, err := rowsAffectedZeroOrOne(result, "complete runner task")
	if err != nil {
		return runner.Task{}, false, err
	}
	current, getErr := getSQLRunnerTask(ctx, tx, s.dialect, id)
	if getErr != nil {
		return runner.Task{}, false, getErr
	}
	if !matched {
		return current, false, nil
	}
	if err := tx.Commit(); err != nil {
		return runner.Task{}, false, err
	}
	return current, true, nil
}

func (s *SQLRunnerStore) CancelTask(ctx context.Context, id string) (runner.CancelDisposition, error) {
	if err := validateSQLRunnerTaskID(id); err != nil {
		return "", err
	}
	now := time.Now().UTC().UnixMilli()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, sqlCancelQueuedRunnerTask.bind(s.dialect), now, now, id)
	if err != nil {
		return "", err
	}
	matched, err := rowsAffectedZeroOrOne(result, "cancel queued runner task")
	if err != nil {
		return "", err
	}
	if matched {
		if err := tx.Commit(); err != nil {
			return "", err
		}
		return runner.CancelDispositionCancelled, nil
	}
	result, err = tx.ExecContext(ctx, sqlRequestClaimedRunnerCancel.bind(s.dialect), now, id)
	if err != nil {
		return "", err
	}
	matched, err = rowsAffectedZeroOrOne(result, "request claimed runner cancellation")
	if err != nil {
		return "", err
	}
	if matched {
		if err := tx.Commit(); err != nil {
			return "", err
		}
		return runner.CancelDispositionRequested, nil
	}
	current, err := getSQLRunnerTask(ctx, tx, s.dialect, id)
	if err != nil {
		return "", err
	}
	switch current.State {
	case runner.TaskCompleted, runner.TaskCancelled, runner.TaskFailed:
		return runner.CancelDispositionTerminal, nil
	default:
		return "", fmt.Errorf("runner task %s has invalid state %q", id, current.State)
	}
}

func (s *SQLRunnerStore) RecoverExpiredTasks(ctx context.Context, now time.Time) (int64, int64, error) {
	if now.IsZero() {
		return 0, 0, fmt.Errorf("runner recovery time is zero")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = tx.Rollback() }()
	requeued, terminal, err := s.recoverExpiredTasksTx(ctx, tx, now.UTC())
	if err != nil {
		return 0, 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, 0, err
	}
	return requeued, terminal, nil
}

func (s *SQLRunnerStore) recoverExpiredTasksTx(ctx context.Context, tx *sql.Tx, now time.Time) (int64, int64, error) {
	query := sqlExpiredRunnerTasks
	if s.dialect == SQLDialectPostgres {
		query = sqlExpiredRunnerTasksPostgres
	}
	rows, err := tx.QueryContext(ctx, query.bind(s.dialect), now.UnixMilli())
	if err != nil {
		return 0, 0, err
	}
	type expiredTask struct {
		id              string
		cancelRequested int64
		attempt         int
		maxAttempts     int
	}
	var expired []expiredTask
	for rows.Next() {
		var task expiredTask
		if err := rows.Scan(&task.id, &task.cancelRequested, &task.attempt, &task.maxAttempts); err != nil {
			_ = rows.Close()
			return 0, 0, err
		}
		if task.cancelRequested != 0 && task.cancelRequested != 1 {
			_ = rows.Close()
			return 0, 0, fmt.Errorf("runner task %s has invalid cancel_requested %d", task.id, task.cancelRequested)
		}
		if task.attempt < 0 || task.maxAttempts < 1 || task.maxAttempts > runner.HardMaxAttempts {
			_ = rows.Close()
			return 0, 0, fmt.Errorf("runner task %s has invalid attempt bounds %d/%d", task.id, task.attempt, task.maxAttempts)
		}
		expired = append(expired, task)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, 0, err
	}
	if err := rows.Close(); err != nil {
		return 0, 0, err
	}
	var requeued, terminal int64
	for _, task := range expired {
		var result sql.Result
		switch {
		case task.cancelRequested == 1:
			result, err = tx.ExecContext(ctx, sqlRecoverCancelledRunnerTask.bind(s.dialect),
				now.UnixMilli(), now.UnixMilli(), task.id, now.UnixMilli())
		case task.attempt < task.maxAttempts:
			result, err = tx.ExecContext(ctx, sqlRecoverQueuedRunnerTask.bind(s.dialect),
				now.UnixMilli(), now.UnixMilli(), task.id, now.UnixMilli())
		default:
			result, err = tx.ExecContext(ctx, sqlRecoverFailedRunnerTask.bind(s.dialect),
				now.UnixMilli(), now.UnixMilli(), task.id, now.UnixMilli())
		}
		if err != nil {
			return 0, 0, err
		}
		matched, err := rowsAffectedZeroOrOne(result, "recover expired runner task")
		if err != nil {
			return 0, 0, err
		}
		if !matched {
			continue
		}
		if task.cancelRequested == 0 && task.attempt < task.maxAttempts {
			requeued++
		} else {
			terminal++
		}
	}
	return requeued, terminal, nil
}

func (s *SQLRunnerStore) PendingTasks(ctx context.Context) (int64, error) {
	var count int64
	if err := s.db.QueryRowContext(ctx, sqlCountPendingRunnerTasks.bind(s.dialect)).Scan(&count); err != nil {
		return 0, err
	}
	if count < 0 {
		return 0, fmt.Errorf("runner pending task count is negative: %d", count)
	}
	return count, nil
}

// PruneTasks removes non-idempotent terminal tasks completed strictly before
// olderThan. The shared SQL schema stores task timestamps in Unix milliseconds.
func (s *SQLRunnerStore) PruneTasks(ctx context.Context, olderThan time.Time) (int64, error) {
	if olderThan.IsZero() {
		return 0, fmt.Errorf("runner retention cutoff is zero")
	}
	result, err := s.db.ExecContext(ctx, sqlPruneRunnerTasks.bind(s.dialect), olderThan.UTC().UnixMilli())
	if err != nil {
		return 0, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	if affected < 0 {
		return 0, fmt.Errorf("prune runner tasks affected %d rows, expected a non-negative count", affected)
	}
	return affected, nil
}

// ListTasks returns a filtered metadata-only task page. The count and page
// share one parameterized predicate set, while the page query only projects
// catalog fields and presence flags.
func (s *SQLRunnerStore) ListTasks(ctx context.Context, query runner.TaskQuery) ([]runner.TaskSummary, int, error) {
	query, err := query.Normalize()
	if err != nil {
		return nil, 0, err
	}
	countQuery, countArgs := buildSQLRunnerCatalogQuery(query, false)
	var count int64
	if err := s.db.QueryRowContext(ctx, countQuery.bind(s.dialect), countArgs...).Scan(&count); err != nil {
		return nil, 0, err
	}
	if count < 0 || count > int64(^uint(0)>>1) {
		return nil, 0, fmt.Errorf("runner catalog count is outside int range: %d", count)
	}
	pageQuery, pageArgs := buildSQLRunnerCatalogQuery(query, true)
	rows, err := s.db.QueryContext(ctx, pageQuery.bind(s.dialect), pageArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	items := make([]runner.TaskSummary, 0, query.Limit)
	for rows.Next() {
		summary, err := scanSQLRunnerTaskSummary(rows)
		if err != nil {
			return nil, 0, err
		}
		items = append(items, summary)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return items, int(count), nil
}

func buildSQLRunnerCatalogQuery(query runner.TaskQuery, page bool) (sqlQuery, []any) {
	where, args := buildSQLRunnerCatalogWhere(query)
	if !page {
		return sqlQuery{"SELECT COUNT(*) FROM runner_tasks" + where}, args
	}
	args = append(args, query.Limit, query.Offset)
	return sqlQuery{`SELECT id, retried_from_id, capability, tenant_id, subject_id, scope, state,
		cancel_requested, worker_id, lease_expires_at, available_at, attempt,
		max_attempts, generation, error_code, created_at, updated_at, completed_at,
		CASE WHEN result_json <> '' THEN 1 ELSE 0 END,
		CASE WHEN trace_parent <> '' OR trace_state <> '' THEN 1 ELSE 0 END
		FROM runner_tasks` + where + ` ORDER BY updated_at DESC, id ASC LIMIT ? OFFSET ?`}, args
}

func buildSQLRunnerCatalogWhere(query runner.TaskQuery) (string, []any) {
	predicates := make([]string, 0, 7)
	args := make([]any, 0, 11+len(query.States))
	if query.ID != "" {
		predicates = append(predicates, "id = ?")
		args = append(args, query.ID)
	}
	if query.TenantID != "" {
		predicates = append(predicates, "tenant_id = ?")
		args = append(args, query.TenantID)
	}
	if query.SubjectID != "" {
		predicates = append(predicates, "subject_id = ?")
		args = append(args, query.SubjectID)
	}
	if query.ScopePrefix != "" {
		predicates = append(predicates, "(scope = ? OR (substr(scope, 1, length(?)) = ? AND substr(scope, length(?) + 1, 1) = '/'))")
		args = append(args, query.ScopePrefix, query.ScopePrefix, query.ScopePrefix, query.ScopePrefix)
	}
	if query.Capability != "" {
		predicates = append(predicates, "capability = ?")
		args = append(args, query.Capability)
	}
	if query.WorkerID != "" {
		predicates = append(predicates, "worker_id = ?")
		args = append(args, query.WorkerID)
	}
	if len(query.States) > 0 {
		placeholders := make([]string, len(query.States))
		for i, state := range query.States {
			placeholders[i] = "?"
			args = append(args, string(state))
		}
		predicates = append(predicates, "state IN ("+strings.Join(placeholders, ", ")+")")
	}
	if len(predicates) == 0 {
		return "", args
	}
	return " WHERE " + strings.Join(predicates, " AND "), args
}

type sqlRunnerQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func getSQLRunnerTask(ctx context.Context, db sqlRunnerQuerier, dialect SQLDialect, id string) (runner.Task, error) {
	task, err := scanSQLRunnerTask(db.QueryRowContext(ctx, sqlSelectRunnerTask.bind(dialect), id))
	if errors.Is(err, sql.ErrNoRows) {
		return runner.Task{}, runner.ErrTaskNotFound
	}
	return task, err
}

func (s *SQLRunnerStore) getTaskByKey(ctx context.Context, db sqlRunnerQuerier, key runner.Task) (runner.Task, error) {
	task, err := scanSQLRunnerTask(db.QueryRowContext(ctx, sqlSelectRunnerTaskByKey.bind(s.dialect),
		key.TenantID, key.SubjectID, key.Scope, key.Capability, key.IdempotencyKey,
	))
	if errors.Is(err, sql.ErrNoRows) {
		return runner.Task{}, runner.ErrTaskNotFound
	}
	return task, err
}

func (s *SQLRunnerStore) getTaskByRetryParent(ctx context.Context, db sqlRunnerQuerier, id string) (runner.Task, error) {
	task, err := scanSQLRunnerTask(db.QueryRowContext(ctx, sqlSelectRunnerTaskByRetryParent.bind(s.dialect), id))
	if errors.Is(err, sql.ErrNoRows) {
		return runner.Task{}, runner.ErrTaskNotFound
	}
	return task, err
}

type sqlRunnerScanner interface{ Scan(...any) error }

func scanSQLRunnerTaskSummary(scanner sqlRunnerScanner) (runner.TaskSummary, error) {
	var summary runner.TaskSummary
	var state string
	var cancelRequested, hasResult, hasTraceContext int64
	var leaseMillis, availableMillis, createdMillis, updatedMillis, completedMillis int64
	err := scanner.Scan(
		&summary.ID, &summary.RetriedFromID, &summary.Capability, &summary.TenantID, &summary.SubjectID, &summary.Scope,
		&state, &cancelRequested, &summary.WorkerID, &leaseMillis, &availableMillis,
		&summary.Attempt, &summary.MaxAttempts, &summary.Generation, &summary.ErrorCode,
		&createdMillis, &updatedMillis, &completedMillis, &hasResult, &hasTraceContext,
	)
	if err != nil {
		return runner.TaskSummary{}, err
	}
	if err := validateSQLRunnerTaskID(summary.ID); err != nil {
		return runner.TaskSummary{}, fmt.Errorf("decode runner task summary: %w", err)
	}
	if err := core.ValidateNamespacedID(summary.Capability); err != nil {
		return runner.TaskSummary{}, fmt.Errorf("decode runner task summary %s capability: %w", summary.ID, err)
	}
	if err := validateSQLRunnerIdentity(summary.TenantID, summary.SubjectID, summary.Scope); err != nil {
		return runner.TaskSummary{}, fmt.Errorf("decode runner task summary %s identity: %w", summary.ID, err)
	}
	if cancelRequested != 0 && cancelRequested != 1 || hasResult != 0 && hasResult != 1 || hasTraceContext != 0 && hasTraceContext != 1 {
		return runner.TaskSummary{}, fmt.Errorf("runner task summary %s has invalid boolean fields", summary.ID)
	}
	summary.State = runner.TaskState(state)
	switch summary.State {
	case runner.TaskQueued, runner.TaskClaimed, runner.TaskCompleted, runner.TaskCancelled, runner.TaskFailed:
	default:
		return runner.TaskSummary{}, fmt.Errorf("runner task summary %s has invalid state %q", summary.ID, state)
	}
	if summary.Attempt < 0 || summary.MaxAttempts < 1 || summary.MaxAttempts > runner.HardMaxAttempts || summary.Generation < 0 {
		return runner.TaskSummary{}, fmt.Errorf("runner task summary %s has invalid attempt/generation values", summary.ID)
	}
	summary.CancelRequested = cancelRequested == 1
	summary.HasResult = hasResult == 1
	summary.HasTraceContext = hasTraceContext == 1
	summary.AvailableAt = time.UnixMilli(availableMillis).UTC()
	summary.CreatedAt = time.UnixMilli(createdMillis).UTC()
	summary.UpdatedAt = time.UnixMilli(updatedMillis).UTC()
	if leaseMillis != 0 {
		summary.LeaseExpiresAt = time.UnixMilli(leaseMillis).UTC()
	}
	if completedMillis != 0 {
		summary.CompletedAt = time.UnixMilli(completedMillis).UTC()
	}
	terminal := summary.State == runner.TaskCompleted || summary.State == runner.TaskCancelled || summary.State == runner.TaskFailed
	if terminal != (completedMillis != 0) {
		return runner.TaskSummary{}, fmt.Errorf("runner task summary %s has inconsistent terminal timestamp", summary.ID)
	}
	if summary.State == runner.TaskClaimed {
		if strings.TrimSpace(summary.WorkerID) == "" || leaseMillis == 0 || summary.Attempt < 1 || summary.Generation < 1 {
			return runner.TaskSummary{}, fmt.Errorf("runner task summary %s has an incomplete claim", summary.ID)
		}
	} else if summary.WorkerID != "" || leaseMillis != 0 {
		return runner.TaskSummary{}, fmt.Errorf("runner task summary %s has claim data outside claimed state", summary.ID)
	}
	if summary.State == runner.TaskCompleted && !summary.HasResult {
		return runner.TaskSummary{}, fmt.Errorf("runner task summary %s completed without a result", summary.ID)
	}
	if summary.State != runner.TaskCompleted && summary.HasResult {
		return runner.TaskSummary{}, fmt.Errorf("runner task summary %s has a result outside completed state", summary.ID)
	}
	return summary, nil
}

func scanSQLRunnerTask(scanner sqlRunnerScanner) (runner.Task, error) {
	var task runner.Task
	var state string
	var argsJSON, resultJSON string
	var cancelRequested int64
	var leaseMillis, availableMillis, createdMillis, updatedMillis, completedMillis int64
	err := scanner.Scan(
		&task.ID, &task.RetriedFromID, &task.TenantID, &task.SubjectID, &task.Scope,
		&task.Capability, &task.IdempotencyKey, &task.ArgsDigest, &argsJSON,
		&state, &task.TraceContext.TraceParent, &task.TraceContext.TraceState,
		&cancelRequested, &task.WorkerID, &leaseMillis, &availableMillis,
		&task.Attempt, &task.MaxAttempts, &task.Generation, &resultJSON,
		&task.ErrorCode, &createdMillis, &updatedMillis, &completedMillis,
	)
	if err != nil {
		return runner.Task{}, err
	}
	if err := validateSQLRunnerTaskID(task.ID); err != nil {
		return runner.Task{}, fmt.Errorf("decode runner task: %w", err)
	}
	if err := core.ValidateNamespacedID(task.Capability); err != nil {
		return runner.Task{}, fmt.Errorf("decode runner task %s capability: %w", task.ID, err)
	}
	if err := validateSQLRunnerIdentity(task.TenantID, task.SubjectID, task.Scope); err != nil {
		return runner.Task{}, fmt.Errorf("decode runner task %s identity: %w", task.ID, err)
	}
	if err := validateSQLRunnerIdempotencyKey(task.IdempotencyKey); err != nil {
		return runner.Task{}, fmt.Errorf("decode runner task %s key: %w", task.ID, err)
	}
	if err := validateSQLRunnerTraceContext(task.TraceContext); err != nil {
		return runner.Task{}, fmt.Errorf("decode runner task %s trace context: %w", task.ID, err)
	}
	if cancelRequested != 0 && cancelRequested != 1 {
		return runner.Task{}, fmt.Errorf("runner task %s has invalid cancel_requested %d", task.ID, cancelRequested)
	}
	task.CancelRequested = cancelRequested == 1
	task.State = runner.TaskState(state)
	switch task.State {
	case runner.TaskQueued, runner.TaskClaimed, runner.TaskCompleted, runner.TaskCancelled, runner.TaskFailed:
	default:
		return runner.Task{}, fmt.Errorf("runner task %s has invalid state %q", task.ID, state)
	}
	if task.Attempt < 0 || task.MaxAttempts < 1 || task.MaxAttempts > runner.HardMaxAttempts || task.Generation < 0 {
		return runner.Task{}, fmt.Errorf("runner task %s has invalid attempt/generation values", task.ID)
	}
	args, digest, err := decodeSQLRunnerArgs(argsJSON)
	if err != nil {
		return runner.Task{}, fmt.Errorf("decode runner task %s args: %w", task.ID, err)
	}
	if task.ArgsDigest != digest {
		return runner.Task{}, fmt.Errorf("runner task %s args digest mismatch", task.ID)
	}
	task.Args = args
	task.AvailableAt = time.UnixMilli(availableMillis).UTC()
	task.CreatedAt = time.UnixMilli(createdMillis).UTC()
	task.UpdatedAt = time.UnixMilli(updatedMillis).UTC()
	if leaseMillis != 0 {
		task.LeaseExpiresAt = time.UnixMilli(leaseMillis).UTC()
	}
	if completedMillis != 0 {
		task.CompletedAt = time.UnixMilli(completedMillis).UTC()
	}
	if resultJSON != "" {
		result, err := decodeSQLRunnerResult(resultJSON)
		if err != nil {
			return runner.Task{}, fmt.Errorf("decode runner task %s result: %w", task.ID, err)
		}
		task.Result = &result
	}
	if err := validateSQLRunnerPersistedState(task, leaseMillis, completedMillis, resultJSON); err != nil {
		return runner.Task{}, err
	}
	return task, nil
}

func validateSQLRunnerPersistedState(task runner.Task, leaseMillis, completedMillis int64, resultJSON string) error {
	terminal := task.State == runner.TaskCompleted || task.State == runner.TaskCancelled || task.State == runner.TaskFailed
	if terminal != (completedMillis != 0) {
		return fmt.Errorf("runner task %s has inconsistent terminal timestamp", task.ID)
	}
	if task.State == runner.TaskClaimed {
		if strings.TrimSpace(task.WorkerID) == "" || leaseMillis == 0 || task.Attempt < 1 || task.Generation < 1 {
			return fmt.Errorf("runner task %s has an incomplete claim", task.ID)
		}
	} else if task.WorkerID != "" || leaseMillis != 0 {
		return fmt.Errorf("runner task %s has claim data outside claimed state", task.ID)
	}
	if task.State == runner.TaskCompleted && resultJSON == "" {
		return fmt.Errorf("runner task %s completed without a result", task.ID)
	}
	if task.State != runner.TaskCompleted && resultJSON != "" {
		return fmt.Errorf("runner task %s has a result outside completed state", task.ID)
	}
	return nil
}

func prepareSQLRunnerTask(task runner.Task) (runner.Task, string, error) {
	if err := core.ValidateNamespacedID(task.Capability); err != nil {
		return runner.Task{}, "", err
	}
	if task.ID == "" {
		id, err := core.NewID("rtask_")
		if err != nil {
			return runner.Task{}, "", err
		}
		task.ID = id
	}
	if err := validateSQLRunnerTaskID(task.ID); err != nil {
		return runner.Task{}, "", err
	}
	if err := validateSQLRunnerIdentity(task.TenantID, task.SubjectID, task.Scope); err != nil {
		return runner.Task{}, "", err
	}
	if err := validateSQLRunnerIdempotencyKey(task.IdempotencyKey); err != nil {
		return runner.Task{}, "", err
	}
	if err := validateSQLRunnerTraceContext(task.TraceContext); err != nil {
		return runner.Task{}, "", err
	}
	args, argsJSON, digest, err := encodeSQLRunnerArgs(task.Args)
	if err != nil {
		return runner.Task{}, "", err
	}
	if task.ArgsDigest != "" && task.ArgsDigest != digest {
		return runner.Task{}, "", runner.ErrSubmissionConflict
	}
	maxAttempts := task.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = runner.DefaultMaxAttempts
	}
	if maxAttempts < 1 || maxAttempts > runner.HardMaxAttempts {
		return runner.Task{}, "", fmt.Errorf("runner max attempts must be between 1 and %d", runner.HardMaxAttempts)
	}
	now := time.Now().UTC()
	if task.CreatedAt.IsZero() {
		task.CreatedAt = now
	}
	if task.AvailableAt.IsZero() {
		task.AvailableAt = task.CreatedAt
	}
	task.UpdatedAt = task.CreatedAt
	task.Args, task.ArgsDigest = args, digest
	task.State = runner.TaskQueued
	task.MaxAttempts = maxAttempts
	task.CancelRequested, task.WorkerID, task.ErrorCode = false, "", ""
	task.LeaseExpiresAt, task.CompletedAt = time.Time{}, time.Time{}
	task.Attempt, task.Generation, task.Result = 0, 0, nil
	return task, argsJSON, nil
}

func encodeSQLRunnerArgs(args map[string]any) (map[string]any, string, string, error) {
	if args == nil {
		args = map[string]any{}
	}
	encoded, err := json.Marshal(args)
	if err != nil {
		return nil, "", "", fmt.Errorf("encode runner args: %w", err)
	}
	if len(encoded) > core.MaxToolArgumentBytes {
		return nil, "", "", fmt.Errorf("runner args exceed %d bytes", core.MaxToolArgumentBytes)
	}
	cloned, digest, err := decodeSQLRunnerArgs(string(encoded))
	if err != nil {
		return nil, "", "", err
	}
	return cloned, string(encoded), digest, nil
}

func decodeSQLRunnerArgs(encoded string) (map[string]any, string, error) {
	if len(encoded) > core.MaxToolArgumentBytes {
		return nil, "", fmt.Errorf("runner args exceed %d bytes", core.MaxToolArgumentBytes)
	}
	var args map[string]any
	if err := json.Unmarshal([]byte(encoded), &args); err != nil {
		return nil, "", err
	}
	if args == nil {
		return nil, "", fmt.Errorf("runner args must be a JSON object")
	}
	digest := sha256.Sum256([]byte(encoded))
	return args, hex.EncodeToString(digest[:]), nil
}

func encodeSQLRunnerResult(result core.CapabilityResult) (string, error) {
	if err := core.ValidateCapabilityResult(result); err != nil {
		return "", err
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return "", fmt.Errorf("encode runner result: %w", err)
	}
	if _, err := decodeSQLRunnerResult(string(encoded)); err != nil {
		return "", err
	}
	return string(encoded), nil
}

func decodeSQLRunnerResult(encoded string) (core.CapabilityResult, error) {
	var result core.CapabilityResult
	if err := json.Unmarshal([]byte(encoded), &result); err != nil {
		return core.CapabilityResult{}, err
	}
	if err := core.ValidateCapabilityResult(result); err != nil {
		return core.CapabilityResult{}, err
	}
	return result, nil
}

func validateSQLRunnerTaskID(id string) error {
	if len(id) <= len("rtask_") || core.ValidateRunID(id) != nil {
		return fmt.Errorf("invalid runner task id %q", id)
	}
	if !strings.HasPrefix(id, "rtask_") || len(id) > 128 || strings.ContainsAny(id, "/\\") || containsControl(id) {
		return fmt.Errorf("invalid runner task id %q", id)
	}
	return nil
}

func validateSQLRunnerIdentity(tenant, subject, scope string) error {
	if tenant == "" && subject == "" && scope == "" {
		return nil
	}
	for name, value := range map[string]string{"tenant": tenant, "subject": subject, "scope": scope} {
		if strings.TrimSpace(value) == "" || len(value) > 4096 || containsControl(value) {
			return fmt.Errorf("runner task %s is empty, too long, or contains control characters", name)
		}
	}
	return nil
}

func validateSQLRunnerIdempotencyKey(key string) error {
	if key != "" && (len(key) > runner.MaxIdempotencyKey || containsControl(key)) {
		return fmt.Errorf("runner idempotency key is too long or contains control characters")
	}
	return nil
}

func validateSQLRunnerTraceContext(traceContext core.TelemetryTraceContext) error {
	if len(traceContext.TraceParent) > 256 || len(traceContext.TraceState) > 512 ||
		containsControl(traceContext.TraceParent) || containsControl(traceContext.TraceState) {
		return fmt.Errorf("runner trace context is too large or contains control characters")
	}
	return nil
}

func validateSQLRunnerWorkerLease(worker string, ttl time.Duration) error {
	if strings.TrimSpace(worker) == "" || len(worker) > runner.MaxWorkerID || containsControl(worker) {
		return fmt.Errorf("runner worker id is empty, too long, or contains control characters")
	}
	if ttl <= 0 || ttl > 24*time.Hour {
		return fmt.Errorf("runner claim ttl must be between 1ns and 24h")
	}
	return nil
}

func normalizeSQLRunnerClaimOptions(options runner.ClaimOptions) (runner.ClaimOptions, error) {
	if err := validateSQLRunnerWorkerLease(options.WorkerID, options.LeaseTTL); err != nil {
		return runner.ClaimOptions{}, err
	}
	if len(options.Capabilities) == 0 {
		options.Capabilities = nil
		return options, nil
	}
	if len(options.Capabilities) > runner.MaxClaimCapabilities {
		return runner.ClaimOptions{}, fmt.Errorf("runner claim supports at most %d capabilities", runner.MaxClaimCapabilities)
	}
	unique := make(map[string]struct{}, len(options.Capabilities))
	for _, capability := range options.Capabilities {
		capability = strings.TrimSpace(capability)
		if capability == "*" {
			return runner.ClaimOptions{}, fmt.Errorf("runner claim capability selector cannot include %q", capability)
		}
		if err := core.ValidateNamespacedID(capability); err != nil {
			return runner.ClaimOptions{}, fmt.Errorf("invalid runner claim capability %q: %w", capability, err)
		}
		unique[capability] = struct{}{}
	}
	options.Capabilities = make([]string, 0, len(unique))
	for capability := range unique {
		options.Capabilities = append(options.Capabilities, capability)
	}
	sort.Strings(options.Capabilities)
	return options, nil
}

func validateSQLRunnerClaim(id, worker string, generation int64, ttl time.Duration) error {
	if err := validateSQLRunnerTaskID(id); err != nil {
		return err
	}
	if generation < 1 {
		return fmt.Errorf("runner claim generation must be positive")
	}
	return validateSQLRunnerWorkerLease(worker, ttl)
}

// runner.Store accepts sub-millisecond leases, while the shared SQL schema
// represents time in Unix milliseconds. Round up so persistence never
// shortens a requested positive lease or makes it expire in its claim call.
func sqlRunnerLeaseExpiryMillis(now time.Time, ttl time.Duration) int64 {
	millis := ttl / time.Millisecond
	if ttl%time.Millisecond != 0 {
		millis++
	}
	if millis < 1 {
		millis = 1
	}
	return now.UnixMilli() + int64(millis)
}

func containsControl(value string) bool {
	return strings.IndexFunc(value, unicode.IsControl) >= 0
}

func rowsAffectedZeroOrOne(result sql.Result, operation string) (bool, error) {
	affected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if affected < 0 || affected > 1 {
		return false, fmt.Errorf("%s affected %d rows, expected at most one", operation, affected)
	}
	return affected == 1, nil
}

func requireRowsAffected(result sql.Result, expected int64, operation string) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != expected {
		return fmt.Errorf("%s affected %d rows, expected %d", operation, affected, expected)
	}
	return nil
}

var _ runner.Store = (*SQLRunnerStore)(nil)
var _ runner.TaskCatalog = (*SQLRunnerStore)(nil)
var _ runner.TaskRetention = (*SQLRunnerStore)(nil)
