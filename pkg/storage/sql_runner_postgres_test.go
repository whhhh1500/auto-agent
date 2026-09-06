package storage

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/extensions/runner"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

func TestPostgresRunnerStoreLifecycleFencingRecoveryAndIdempotency(t *testing.T) {
	db := newPostgresTestDB(t)
	ctx := context.Background()
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres); err != nil {
		t.Fatal(err)
	}
	store, err := NewSQLRunnerStore(db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	keyed := runner.Task{
		Capability: "private.postgres", IdempotencyKey: "pg-key",
		TenantID: "acme", SubjectID: "alice", Scope: "global/product/acme/alice",
		Args: map[string]any{"value": "same"}, AvailableAt: time.Now().UTC().Add(time.Hour),
		TraceContext: core.TelemetryTraceContext{
			TraceParent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
			TraceState:  "vendor=original",
		},
	}
	created, made, err := store.CreateTask(ctx, keyed)
	if err != nil || !made || created.TraceContext != keyed.TraceContext {
		t.Fatalf("postgres keyed create failed: task=%#v made=%t err=%v", created, made, err)
	}
	keyed.TraceContext = core.TelemetryTraceContext{
		TraceParent: "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
		TraceState:  "vendor=replayed",
	}
	replayed, made, err := store.CreateTask(ctx, keyed)
	if err != nil || made || replayed.ID != created.ID || replayed.TraceContext.TraceState != "vendor=original" {
		t.Fatalf("postgres replay failed: task=%#v made=%t err=%v", replayed, made, err)
	}
	keyed.Args = map[string]any{"value": "different"}
	if _, _, err := store.CreateTask(ctx, keyed); !errors.Is(err, runner.ErrSubmissionConflict) {
		t.Fatalf("postgres idempotency conflict missing: %v", err)
	}

	retry, made, err := store.CreateTask(ctx, runner.Task{
		ID: "rtask_pg_retry", Capability: "private.postgres", Args: map[string]any{"retry": true}, MaxAttempts: 2,
		TraceContext: core.TelemetryTraceContext{
			TraceParent: "00-11111111111111111111111111111111-2222222222222222-01",
			TraceState:  "vendor=claimed",
		},
	})
	if err != nil || !made {
		t.Fatalf("postgres retry create failed: task=%#v made=%t err=%v", retry, made, err)
	}
	first, ok, err := store.ClaimTask(ctx, sqlRunnerClaimOptions("pg-worker-1", time.Minute, "private.postgres"))
	if err != nil || !ok || first.ID != retry.ID || first.Attempt != 1 || first.Generation != 1 ||
		first.TraceContext.TraceState != "vendor=claimed" {
		t.Fatalf("postgres first claim failed: task=%#v ok=%t err=%v", first, ok, err)
	}
	if renewed, err := store.RenewTaskClaim(ctx, first.ID, first.WorkerID, first.Generation, time.Minute); err != nil || !renewed {
		t.Fatalf("postgres renewal failed: renewed=%t err=%v", renewed, err)
	}
	if stale, completed, err := store.CompleteTask(ctx, first.ID, first.WorkerID, first.Generation+1, core.CapabilityResult{OK: true}); err != nil || completed || stale.State != runner.TaskClaimed {
		t.Fatalf("postgres stale completion crossed fence: task=%#v completed=%t err=%v", stale, completed, err)
	}
	if _, err := db.ExecContext(ctx, "UPDATE runner_tasks SET lease_expires_at = 1 WHERE id = $1", first.ID); err != nil {
		t.Fatal(err)
	}
	if requeued, terminal, err := store.RecoverExpiredTasks(ctx, time.Now().UTC()); err != nil || requeued != 1 || terminal != 0 {
		t.Fatalf("postgres requeue recovery failed: requeued=%d terminal=%d err=%v", requeued, terminal, err)
	}
	second, ok, err := store.ClaimTask(ctx, sqlRunnerClaimOptions("pg-worker-2", time.Minute, "private.postgres"))
	if err != nil || !ok || second.ID != retry.ID || second.Attempt != 2 || second.Generation != 2 {
		t.Fatalf("postgres reclaim failed: task=%#v ok=%t err=%v", second, ok, err)
	}
	if _, completed, err := store.CompleteTask(ctx, first.ID, first.WorkerID, first.Generation, core.CapabilityResult{OK: true}); err != nil || completed {
		t.Fatalf("postgres old owner completed reclaimed task: completed=%t err=%v", completed, err)
	}
	finished, completed, err := store.CompleteTask(ctx, second.ID, second.WorkerID, second.Generation,
		core.CapabilityResult{Content: "postgres-done", OK: true})
	if err != nil || !completed || finished.State != runner.TaskCompleted || finished.Result == nil || finished.Result.Content != "postgres-done" {
		t.Fatalf("postgres completion failed: task=%#v completed=%t err=%v", finished, completed, err)
	}

	exhausted, _, err := store.CreateTask(ctx, runner.Task{ID: "rtask_pg_exhausted", Capability: "private.postgres", Args: map[string]any{}, MaxAttempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	exhaustedClaim, ok, err := store.ClaimTask(ctx, sqlRunnerClaimOptions("pg-worker-lost", time.Minute, "private.postgres"))
	if err != nil || !ok || exhaustedClaim.ID != exhausted.ID {
		t.Fatalf("postgres exhausted claim failed: task=%#v ok=%t err=%v", exhaustedClaim, ok, err)
	}
	if _, err := db.ExecContext(ctx, "UPDATE runner_tasks SET lease_expires_at = 1 WHERE id = $1", exhausted.ID); err != nil {
		t.Fatal(err)
	}
	if requeued, terminal, err := store.RecoverExpiredTasks(ctx, time.Now().UTC()); err != nil || requeued != 0 || terminal != 1 {
		t.Fatalf("postgres exhausted recovery failed: requeued=%d terminal=%d err=%v", requeued, terminal, err)
	}
	failed, err := store.GetTask(ctx, exhausted.ID)
	if err != nil || failed.State != runner.TaskFailed || failed.ErrorCode != "worker_lost" {
		t.Fatalf("postgres worker-lost terminal wrong: task=%#v err=%v", failed, err)
	}

	cancelled, _, err := store.CreateTask(ctx, runner.Task{ID: "rtask_pg_cancel", Capability: "private.postgres", Args: map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	cancelClaim, ok, err := store.ClaimTask(ctx, sqlRunnerClaimOptions("pg-worker-cancel", time.Minute, "private.postgres"))
	if err != nil || !ok || cancelClaim.ID != cancelled.ID {
		t.Fatalf("postgres cancellation claim failed: task=%#v ok=%t err=%v", cancelClaim, ok, err)
	}
	if disposition, err := store.CancelTask(ctx, cancelClaim.ID); err != nil || disposition != runner.CancelDispositionRequested {
		t.Fatalf("postgres claimed cancellation failed: disposition=%q err=%v", disposition, err)
	}
	if _, err := db.ExecContext(ctx, "UPDATE runner_tasks SET lease_expires_at = 1 WHERE id = $1", cancelClaim.ID); err != nil {
		t.Fatal(err)
	}
	if requeued, terminal, err := store.RecoverExpiredTasks(ctx, time.Now().UTC()); err != nil || requeued != 0 || terminal != 1 {
		t.Fatalf("postgres cancellation recovery failed: requeued=%d terminal=%d err=%v", requeued, terminal, err)
	}
	current, err := store.GetTask(ctx, cancelClaim.ID)
	if err != nil || current.State != runner.TaskCancelled || !current.CancelRequested {
		t.Fatalf("postgres cancellation terminal wrong: task=%#v err=%v", current, err)
	}
}

func TestPostgresRunnerRetryCreatesOneDerivedTask(t *testing.T) {
	db := newPostgresTestDB(t)
	ctx := context.Background()
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres); err != nil {
		t.Fatal(err)
	}
	store, err := NewSQLRunnerStore(db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	original, _, err := store.CreateTask(ctx, runner.Task{ID: "rtask_pg_retry_parent", Capability: "private.postgres", TenantID: "acme", SubjectID: "alice", Scope: "global/product/acme/alice", Args: map[string]any{"secret": "keep"}, MaxAttempts: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "UPDATE runner_tasks SET state='failed', completed_at=$1, error_code='worker_lost' WHERE id=$2", time.Now().UTC().UnixMilli(), original.ID); err != nil {
		t.Fatal(err)
	}
	child, created, err := store.RetryTask(ctx, original.ID)
	if err != nil || !created || child.RetriedFromID != original.ID || child.State != runner.TaskQueued || child.Attempt != 0 || child.Generation != 0 || child.ErrorCode != "" {
		t.Fatalf("retry child=%#v created=%t err=%v", child, created, err)
	}
	replay, created, err := store.RetryTask(ctx, original.ID)
	if err != nil || created || replay.ID != child.ID {
		t.Fatalf("retry replay=%#v created=%t err=%v", replay, created, err)
	}
}

func TestPostgresRunnerStorePruneTasksDeletesOnlyEligibleTasks(t *testing.T) {
	db := newPostgresTestDB(t)
	ctx := context.Background()
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres); err != nil {
		t.Fatal(err)
	}
	store, err := NewSQLRunnerStore(db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	cutoff := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	old := cutoff.Add(-time.Millisecond)
	recent := cutoff.Add(time.Millisecond)
	tasks := []struct {
		id          string
		key         string
		state       runner.TaskState
		completedAt time.Time
		updatedAt   time.Time
		worker      string
		leaseAt     time.Time
		removed     bool
	}{
		{id: "rtask_pg_retention_completed", state: runner.TaskCompleted, completedAt: old, updatedAt: old, removed: true},
		{id: "rtask_pg_retention_cancelled", state: runner.TaskCancelled, completedAt: old, updatedAt: old, removed: true},
		{id: "rtask_pg_retention_failed", state: runner.TaskFailed, completedAt: old, updatedAt: old, removed: true},
		{id: "rtask_pg_retention_keyed", key: "retain-key", state: runner.TaskCompleted, completedAt: old, updatedAt: old},
		{id: "rtask_pg_retention_queued", state: runner.TaskQueued, updatedAt: old},
		{id: "rtask_pg_retention_claimed", state: runner.TaskClaimed, updatedAt: old, worker: "retention-worker", leaseAt: cutoff.Add(time.Hour)},
		{id: "rtask_pg_retention_recent", state: runner.TaskCompleted, completedAt: recent, updatedAt: recent},
		{id: "rtask_pg_retention_equal", state: runner.TaskFailed, completedAt: cutoff, updatedAt: cutoff},
		{id: "rtask_pg_retention_zero", state: runner.TaskCancelled, updatedAt: old},
	}
	update := sqlQuery{`UPDATE runner_tasks
		SET state = ?, worker_id = ?, lease_expires_at = ?, updated_at = ?, completed_at = ?
		WHERE id = ?`}
	for _, task := range tasks {
		createSQLRunnerRetentionTask(t, store, ctx, task.id, task.key)
		completedMillis := int64(0)
		if !task.completedAt.IsZero() {
			completedMillis = task.completedAt.UnixMilli()
		}
		leaseMillis := int64(0)
		if !task.leaseAt.IsZero() {
			leaseMillis = task.leaseAt.UnixMilli()
		}
		if _, err := db.ExecContext(ctx, update.bind(SQLDialectPostgres),
			string(task.state), task.worker, leaseMillis, task.updatedAt.UnixMilli(), completedMillis, task.id); err != nil {
			t.Fatal(err)
		}
	}
	pruned, err := store.PruneTasks(ctx, cutoff)
	if err != nil || pruned != 3 {
		t.Fatalf("postgres prune count = %d, %v; want 3, nil", pruned, err)
	}
	countQuery := sqlQuery{`SELECT COUNT(*) FROM runner_tasks WHERE id = ?`}
	for _, task := range tasks {
		var count int
		if err := db.QueryRowContext(ctx, countQuery.bind(SQLDialectPostgres), task.id).Scan(&count); err != nil {
			t.Fatal(err)
		}
		want := 1
		if task.removed {
			want = 0
		}
		if count != want {
			t.Fatalf("postgres retention row %s count = %d, want %d", task.id, count, want)
		}
	}
}

func TestPostgresRunnerTaskCatalogFiltersAndIsolation(t *testing.T) {
	db := newPostgresTestDB(t)
	ctx := context.Background()
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres); err != nil {
		t.Fatal(err)
	}
	store, err := NewSQLRunnerStore(db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	prefix := "global/product/acme%_org/alice/jobs%_root"
	create := func(id, tenant, subject, scope string, createdAt time.Time) {
		t.Helper()
		if _, fresh, err := store.CreateTask(ctx, runner.Task{
			ID: id, TenantID: tenant, SubjectID: subject, Scope: scope, Capability: "private.postgres",
			Args: map[string]any{"secret": id + "-args-secret"}, CreatedAt: createdAt, AvailableAt: createdAt,
		}); err != nil || !fresh {
			t.Fatalf("create %s: fresh=%t err=%v", id, fresh, err)
		}
	}
	create("rtask_pg_catalog_root", "acme", "alice", prefix, base)
	create("rtask_pg_catalog_child", "acme", "alice", prefix+"/child", base.Add(time.Minute))
	create("rtask_pg_catalog_like_sibling", "acme", "alice", "global/product/acmeXorg/alice/jobsZZroot/child", base.Add(2*time.Minute))
	create("rtask_pg_catalog_other_tenant", "other", "alice", prefix, base.Add(3*time.Minute))

	items, total, err := store.ListTasks(ctx, runner.TaskQuery{
		TenantID: "acme", SubjectID: "alice", ScopePrefix: prefix, Capability: "private.postgres", States: []runner.TaskState{runner.TaskQueued},
	})
	if err != nil || total != 2 || len(items) != 2 || items[0].ID != "rtask_pg_catalog_child" || items[1].ID != "rtask_pg_catalog_root" {
		t.Fatalf("postgres catalog page = %#v total=%d err=%v", items, total, err)
	}
	for _, item := range items {
		if item.HasResult || item.HasTraceContext || item.TenantID != "acme" || item.SubjectID != "alice" {
			t.Fatalf("postgres catalog metadata/isolation wrong: %#v", item)
		}
	}
	items, total, err = store.ListTasks(ctx, runner.TaskQuery{
		ID: " rtask_pg_catalog_child ", TenantID: "acme", SubjectID: "alice", ScopePrefix: prefix,
		Capability: "private.postgres", States: []runner.TaskState{runner.TaskQueued}, Limit: 1,
	})
	if err != nil || total != 1 || len(items) != 1 || items[0].ID != "rtask_pg_catalog_child" {
		t.Fatalf("postgres catalog ID filter = %#v total=%d err=%v", items, total, err)
	}
	items, total, err = store.ListTasks(ctx, runner.TaskQuery{
		ID: "rtask_pg_catalog_child", TenantID: "acme", SubjectID: "alice", ScopePrefix: prefix,
		Capability: "private.postgres", States: []runner.TaskState{runner.TaskQueued}, Limit: 1, Offset: 1,
	})
	if err != nil || total != 1 || len(items) != 0 {
		t.Fatalf("postgres catalog ID paged filter = %#v total=%d err=%v", items, total, err)
	}
}

func TestPostgresRunnerSchemaV24ToV25Migration(t *testing.T) {
	db := newPostgresTestDB(t)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, sqlSchemaV15); err != nil {
		t.Fatal(err)
	}
	if err := migrateSQLSchema(ctx, db, SQLDialectPostgres, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, sqlSchemaV24); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, sqlInsertMetaRow.bind(SQLDialectPostgres), "24"); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres); err != nil {
		t.Fatalf("postgres v24 to v25 migration failed: %v", err)
	}
	var version string
	if err := db.QueryRowContext(ctx, sqlSelectMetaRow.bind(SQLDialectPostgres)).Scan(&version); err != nil || version != strconv.Itoa(SQLSchemaVersion) {
		t.Fatalf("postgres current version missing after runner migration: version=%q err=%v", version, err)
	}
	var table string
	if err := db.QueryRowContext(ctx, "SELECT to_regclass('runner_tasks')").Scan(&table); err != nil || !strings.HasSuffix(table, "runner_tasks") {
		t.Fatalf("postgres runner_tasks table missing: table=%q err=%v", table, err)
	}
	for _, column := range []string{"trace_parent", "trace_state"} {
		var count int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.columns
			WHERE table_schema = current_schema() AND table_name = 'runner_tasks' AND column_name = $1`, column).Scan(&count); err != nil || count != 1 {
			t.Fatalf("postgres v25 column %s missing: count=%d err=%v", column, count, err)
		}
	}
}

func TestPostgresZZNoResidualHarnessTestSchemas(t *testing.T) {
	dsn := os.Getenv("HARNESS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("HARNESS_TEST_PG_DSN is not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	db := stdlib.OpenDB(*config)
	defer db.Close()
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pg_namespace
		WHERE nspname LIKE 'harness_test\_%' ESCAPE '\'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("postgres test schemas remain after cleanup: %d", count)
	}
}
