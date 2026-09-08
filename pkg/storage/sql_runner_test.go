package storage

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/extensions/runner"

	_ "modernc.org/sqlite"
)

func sqlRunnerClaimOptions(worker string, ttl time.Duration, capabilities ...string) runner.ClaimOptions {
	return runner.ClaimOptions{WorkerID: worker, Capabilities: append([]string(nil), capabilities...), LeaseTTL: ttl}
}

func newTestSQLRunnerStore(t *testing.T) (*SQLRunnerStore, *SQLSessionStore) {
	t.Helper()
	sessions := newTestSQLStore(t)
	store, err := NewSQLRunnerStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	return store, sessions
}

func TestSQLRunnerRetryCreatesOneDerivedTask(t *testing.T) {
	store, sessions := newTestSQLRunnerStore(t)
	ctx := context.Background()
	original, _, err := store.CreateTask(ctx, runner.Task{ID: "rtask_sql_retry_source", Capability: "runner.render", TenantID: "acme", SubjectID: "alice", Scope: "global/global/tenant/acme/user/alice", Args: map[string]any{"secret": "keep"}, MaxAttempts: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sessions.db.ExecContext(ctx, "UPDATE runner_tasks SET state='failed', completed_at=?, error_code='worker_lost' WHERE id=?", time.Now().UTC().UnixMilli(), original.ID); err != nil {
		t.Fatal(err)
	}
	child, created, err := store.RetryTask(ctx, original.ID)
	if err != nil || !created || child.RetriedFromID != original.ID || child.State != runner.TaskQueued || child.Attempt != 0 || child.Generation != 0 || child.ErrorCode != "" || child.Result != nil {
		t.Fatalf("retry child=%#v created=%t err=%v", child, created, err)
	}
	replayed, created, err := store.RetryTask(ctx, original.ID)
	if err != nil || created || replayed.ID != child.ID {
		t.Fatalf("retry replay=%#v created=%t err=%v", replayed, created, err)
	}
	page, _, err := store.ListTasks(ctx, runner.TaskQuery{ID: child.ID, Limit: 1})
	if err != nil || len(page) != 1 || page[0].RetriedFromID != original.ID {
		t.Fatalf("retry catalog=%#v err=%v", page, err)
	}
}

func createSQLRunnerRetentionTask(t *testing.T, store *SQLRunnerStore, ctx context.Context, id, key string) {
	t.Helper()
	if _, fresh, err := store.CreateTask(ctx, runner.Task{
		ID: id, Capability: "private.retention", IdempotencyKey: key, Args: map[string]any{"id": id},
	}); err != nil || !fresh {
		t.Fatalf("create retention task %s: fresh=%t err=%v", id, fresh, err)
	}
}

func TestSQLRunnerStorePruneTasksDeletesOnlyEligibleTasks(t *testing.T) {
	store, sessions := newTestSQLRunnerStore(t)
	ctx := context.Background()
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
		{id: "rtask_retention_completed", state: runner.TaskCompleted, completedAt: old, updatedAt: old, removed: true},
		{id: "rtask_retention_cancelled", state: runner.TaskCancelled, completedAt: old, updatedAt: old, removed: true},
		{id: "rtask_retention_failed", state: runner.TaskFailed, completedAt: old, updatedAt: old, removed: true},
		{id: "rtask_retention_keyed", key: "retain-key", state: runner.TaskCompleted, completedAt: old, updatedAt: old},
		{id: "rtask_retention_queued", state: runner.TaskQueued, updatedAt: old},
		{id: "rtask_retention_claimed", state: runner.TaskClaimed, updatedAt: old, worker: "retention-worker", leaseAt: cutoff.Add(time.Hour)},
		{id: "rtask_retention_recent", state: runner.TaskCompleted, completedAt: recent, updatedAt: recent},
		{id: "rtask_retention_equal", state: runner.TaskFailed, completedAt: cutoff, updatedAt: cutoff},
		{id: "rtask_retention_zero", state: runner.TaskCancelled, updatedAt: old},
	}
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
		if _, err := sessions.db.ExecContext(ctx, `UPDATE runner_tasks
			SET state = ?, worker_id = ?, lease_expires_at = ?, updated_at = ?, completed_at = ?
			WHERE id = ?`, string(task.state), task.worker, leaseMillis, task.updatedAt.UnixMilli(), completedMillis, task.id); err != nil {
			t.Fatal(err)
		}
	}
	if pruned, err := store.PruneTasks(ctx, time.Time{}); err == nil || pruned != 0 {
		t.Fatalf("zero cutoff prune = %d, %v", pruned, err)
	}
	pruned, err := store.PruneTasks(ctx, cutoff)
	if err != nil || pruned != 3 {
		t.Fatalf("prune count = %d, %v; want 3, nil", pruned, err)
	}
	for _, task := range tasks {
		var count int
		if err := sessions.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM runner_tasks WHERE id = ?", task.id).Scan(&count); err != nil {
			t.Fatal(err)
		}
		want := 1
		if task.removed {
			want = 0
		}
		if count != want {
			t.Fatalf("retention row %s count = %d, want %d", task.id, count, want)
		}
	}
}

func TestBuildRunnerClaimCandidateUsesCapabilityParametersAndStableOrder(t *testing.T) {
	for name, test := range map[string]struct {
		dialect SQLDialect
		want    string
	}{
		"sqlite":   {dialect: SQLDialectSQLite, want: "capability IN (?, ?)"},
		"postgres": {dialect: SQLDialectPostgres, want: "capability IN ($2, $3)"},
	} {
		t.Run(name, func(t *testing.T) {
			query := buildRunnerClaimCandidate(test.dialect, []string{"runner.analytics", "runner.renderer"}).bind(test.dialect)
			if !strings.Contains(query, test.want) || !strings.Contains(query, "ORDER BY available_at, created_at, id LIMIT 1") {
				t.Fatalf("claim candidate missing parameters or stable order: %q", query)
			}
			if test.dialect == SQLDialectPostgres && !strings.Contains(query, "FOR UPDATE SKIP LOCKED") {
				t.Fatalf("postgres claim candidate lost SKIP LOCKED: %q", query)
			}
		})
	}
	all := buildRunnerClaimCandidate(SQLDialectSQLite, nil).bind(SQLDialectSQLite)
	if strings.Contains(all, "capability IN") {
		t.Fatalf("all-capability candidate retained a selector: %q", all)
	}
}

func TestBuildSQLRunnerCatalogQueryUsesBoundFiltersAndSafeScopePrefix(t *testing.T) {
	query := runner.TaskQuery{
		ID: "rtask_sql_catalog_id", TenantID: "tenant-a", SubjectID: "user-a", ScopePrefix: "global/product/tenant%_a/user_a",
		Capability: "private.catalog", WorkerID: "worker-a", States: []runner.TaskState{runner.TaskClaimed, runner.TaskQueued}, Limit: 2, Offset: 3,
	}
	for name, dialect := range map[string]SQLDialect{"sqlite": SQLDialectSQLite, "postgres": SQLDialectPostgres} {
		t.Run(name, func(t *testing.T) {
			catalogQuery, args := buildSQLRunnerCatalogQuery(query, true)
			bound := catalogQuery.bind(dialect)
			if strings.Contains(strings.ToUpper(bound), " LIKE ") ||
				(!strings.Contains(bound, "id = ?") && !strings.Contains(bound, "id = $1")) ||
				!strings.Contains(bound, "substr(scope") || strings.Contains(bound, "args_json") {
				t.Fatalf("unsafe or non-summary catalog query: %q", bound)
			}
			if !strings.Contains(bound, "CASE WHEN result_json <> '' THEN 1 ELSE 0 END") ||
				!strings.Contains(bound, "CASE WHEN trace_parent <> '' OR trace_state <> '' THEN 1 ELSE 0 END") ||
				!strings.Contains(bound, "ORDER BY updated_at DESC, id ASC") {
				t.Fatalf("catalog summary projection missing: %q", bound)
			}
			if dialect == SQLDialectPostgres && (!strings.Contains(bound, "$1") || !strings.Contains(bound, "$13")) {
				t.Fatalf("postgres catalog query did not bind parameters: %q", bound)
			}
			if len(args) != 13 || args[0] != "rtask_sql_catalog_id" || args[len(args)-2] != 2 || args[len(args)-1] != 3 {
				t.Fatalf("catalog query arguments = %#v", args)
			}
		})
	}
}

func TestSQLRunnerStoreTaskCatalogFiltersOrderingAndMetadata(t *testing.T) {
	store, sessions := newTestSQLRunnerStore(t)
	ctx := context.Background()
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	prefix := "global/product/tenant%_a/user_a/jobs%_root"
	create := func(id, tenant, subject, scope, capability string, createdAt time.Time, trace bool) runner.Task {
		t.Helper()
		task := runner.Task{
			ID: id, TenantID: tenant, SubjectID: subject, Scope: scope, Capability: capability,
			Args: map[string]any{"secret": id + "-args-secret"}, CreatedAt: createdAt, AvailableAt: createdAt,
		}
		if trace {
			task.TraceContext = core.TelemetryTraceContext{TraceParent: id + "-trace-secret"}
		}
		created, fresh, err := store.CreateTask(ctx, task)
		if err != nil || !fresh {
			t.Fatalf("create %s: task=%#v fresh=%t err=%v", id, created, fresh, err)
		}
		return created
	}
	create("rtask_sql_catalog_a", "tenant-a", "user-a", prefix, "private.catalog", base, true)
	create("rtask_sql_catalog_b", "tenant-a", "user-a", prefix, "private.catalog", base, false)
	create("rtask_sql_catalog_newer", "tenant-a", "user-a", prefix+"/child", "private.catalog", base.Add(time.Minute), false)
	create("rtask_sql_catalog_like_sibling", "tenant-a", "user-a", "global/product/tenantQaa/user_a/jobsZZroot/child", "private.catalog", base.Add(2*time.Minute), false)
	create("rtask_sql_catalog_other_tenant", "tenant-b", "user-a", prefix, "private.catalog", base.Add(3*time.Minute), false)
	create("rtask_sql_catalog_other_subject", "tenant-a", "user-b", prefix, "private.catalog", base.Add(4*time.Minute), false)
	create("rtask_sql_catalog_other_capability", "tenant-a", "user-a", prefix, "private.other", base.Add(5*time.Minute), false)
	worker := create("rtask_sql_catalog_worker", "tenant-a", "user-a", prefix, "private.worker", base.Add(6*time.Minute), false)
	claimed, ok, err := store.ClaimTask(ctx, sqlRunnerClaimOptions("worker-a", time.Hour, "private.worker"))
	if err != nil || !ok || claimed.ID != worker.ID {
		t.Fatalf("claim catalog worker task: task=%#v ok=%t err=%v", claimed, ok, err)
	}
	completed := create("rtask_sql_catalog_completed", "tenant-a", "user-a", prefix, "private.done", base.Add(7*time.Minute), true)
	completedClaim, ok, err := store.ClaimTask(ctx, sqlRunnerClaimOptions("worker-done", time.Hour, "private.done"))
	if err != nil || !ok || completedClaim.ID != completed.ID {
		t.Fatalf("claim catalog completed task: task=%#v ok=%t err=%v", completedClaim, ok, err)
	}
	if _, committed, err := store.CompleteTask(ctx, completedClaim.ID, completedClaim.WorkerID, completedClaim.Generation,
		core.CapabilityResult{Content: "completed-result-secret", OK: true}); err != nil || !committed {
		t.Fatalf("complete catalog task: committed=%t err=%v", committed, err)
	}
	if _, err := sessions.db.ExecContext(ctx, "UPDATE runner_tasks SET args_json = ?, result_json = ? WHERE id = ?", "{", "not-json", completed.ID); err != nil {
		t.Fatal(err)
	}

	query := runner.TaskQuery{
		TenantID: " tenant-a ", SubjectID: " user-a ", ScopePrefix: " " + prefix + " ", Capability: " private.catalog ",
		States: []runner.TaskState{runner.TaskQueued, runner.TaskQueued}, Limit: 2,
	}
	page, total, err := store.ListTasks(ctx, query)
	if err != nil || total != 3 || len(page) != 2 || page[0].ID != "rtask_sql_catalog_newer" || page[1].ID != "rtask_sql_catalog_a" || !page[1].HasTraceContext {
		t.Fatalf("catalog first page = %#v total=%d err=%v", page, total, err)
	}
	page, total, err = store.ListTasks(ctx, runner.TaskQuery{
		TenantID: "tenant-a", SubjectID: "user-a", ScopePrefix: prefix, Capability: "private.catalog", States: []runner.TaskState{runner.TaskQueued}, Limit: 2, Offset: 2,
	})
	if err != nil || total != 3 || len(page) != 1 || page[0].ID != "rtask_sql_catalog_b" {
		t.Fatalf("catalog second page = %#v total=%d err=%v", page, total, err)
	}
	idPage, idTotal, err := store.ListTasks(ctx, runner.TaskQuery{
		ID: " rtask_sql_catalog_newer ", TenantID: "tenant-a", SubjectID: "user-a", ScopePrefix: prefix,
		Capability: "private.catalog", States: []runner.TaskState{runner.TaskQueued}, Limit: 1,
	})
	if err != nil || idTotal != 1 || len(idPage) != 1 || idPage[0].ID != "rtask_sql_catalog_newer" {
		t.Fatalf("catalog ID filter = %#v total=%d err=%v", idPage, idTotal, err)
	}
	idPage, idTotal, err = store.ListTasks(ctx, runner.TaskQuery{
		ID: "rtask_sql_catalog_newer", TenantID: "tenant-a", SubjectID: "user-a", ScopePrefix: prefix,
		Capability: "private.catalog", States: []runner.TaskState{runner.TaskQueued}, Limit: 1, Offset: 1,
	})
	if err != nil || idTotal != 1 || len(idPage) != 0 {
		t.Fatalf("catalog ID paged filter = %#v total=%d err=%v", idPage, idTotal, err)
	}
	workerPage, workerTotal, err := store.ListTasks(ctx, runner.TaskQuery{WorkerID: "worker-a", States: []runner.TaskState{runner.TaskClaimed}})
	if err != nil || workerTotal != 1 || len(workerPage) != 1 || workerPage[0].ID != worker.ID || workerPage[0].WorkerID != "worker-a" {
		t.Fatalf("worker catalog filter = %#v total=%d err=%v", workerPage, workerTotal, err)
	}
	donePage, doneTotal, err := store.ListTasks(ctx, runner.TaskQuery{States: []runner.TaskState{runner.TaskCompleted}})
	if err != nil || doneTotal != 1 || len(donePage) != 1 || !donePage[0].HasResult || !donePage[0].HasTraceContext {
		t.Fatalf("completed catalog metadata = %#v total=%d err=%v", donePage, doneTotal, err)
	}
}

func TestSQLRunnerStoreStateMachineIdempotencyAndFencing(t *testing.T) {
	store, sessions := newTestSQLRunnerStore(t)
	ctx := context.Background()
	future := time.Now().UTC().Add(time.Hour)
	identity := runner.Task{
		Capability: "private.echo", IdempotencyKey: "stable-call",
		TenantID: "acme", SubjectID: "alice", Scope: "global/product/acme/alice",
		Args: map[string]any{"value": "same"}, MaxAttempts: 2, AvailableAt: future,
	}
	created, wasCreated, err := store.CreateTask(ctx, identity)
	if err != nil || !wasCreated || !strings.HasPrefix(created.ID, "rtask_") || created.State != runner.TaskQueued {
		t.Fatalf("create runner task failed: task=%#v created=%t err=%v", created, wasCreated, err)
	}
	created.Args["value"] = "mutated"
	stored, err := store.GetTask(ctx, created.ID)
	if err != nil || stored.Args["value"] != "same" || stored.ArgsDigest == "" {
		t.Fatalf("stored task was not canonical and cloned: task=%#v err=%v", stored, err)
	}
	replayed, wasCreated, err := store.CreateTask(ctx, identity)
	if err != nil || wasCreated || replayed.ID != created.ID {
		t.Fatalf("same-key replay failed: task=%#v created=%t err=%v", replayed, wasCreated, err)
	}
	conflict := identity
	conflict.Args = map[string]any{"value": "different"}
	if _, _, err := store.CreateTask(ctx, conflict); !errors.Is(err, runner.ErrSubmissionConflict) {
		t.Fatalf("different args reused idempotency key without conflict: %v", err)
	}

	available := time.Now().UTC().Add(-time.Second)
	for _, id := range []string{"rtask_order_b", "rtask_order_a"} {
		if _, made, err := store.CreateTask(ctx, runner.Task{
			ID: id, Capability: "private.echo", Args: map[string]any{"id": id},
			AvailableAt: available, CreatedAt: available,
		}); err != nil || !made {
			t.Fatalf("create unkeyed task %s: made=%t err=%v", id, made, err)
		}
	}
	claimed, ok, err := store.ClaimTask(ctx, sqlRunnerClaimOptions("worker-a", time.Minute))
	if err != nil || !ok || claimed.ID != "rtask_order_a" || claimed.Attempt != 1 || claimed.Generation != 1 || claimed.State != runner.TaskClaimed {
		t.Fatalf("ordered claim failed: task=%#v ok=%t err=%v", claimed, ok, err)
	}
	if renewed, err := store.RenewTaskClaim(ctx, claimed.ID, "wrong-worker", claimed.Generation, time.Minute); err != nil || renewed {
		t.Fatalf("wrong worker renewed claim: renewed=%t err=%v", renewed, err)
	}
	if renewed, err := store.RenewTaskClaim(ctx, claimed.ID, claimed.WorkerID, claimed.Generation+1, time.Minute); err != nil || renewed {
		t.Fatalf("stale generation renewed claim: renewed=%t err=%v", renewed, err)
	}
	if renewed, err := store.RenewTaskClaim(ctx, claimed.ID, claimed.WorkerID, claimed.Generation, time.Minute); err != nil || !renewed {
		t.Fatalf("owner failed to renew claim: renewed=%t err=%v", renewed, err)
	}
	if stale, completed, err := store.CompleteTask(ctx, claimed.ID, claimed.WorkerID, claimed.Generation+1, core.CapabilityResult{OK: true}); err != nil || completed || stale.State != runner.TaskClaimed {
		t.Fatalf("stale generation completed task: task=%#v completed=%t err=%v", stale, completed, err)
	}
	finished, completed, err := store.CompleteTask(ctx, claimed.ID, claimed.WorkerID, claimed.Generation, core.CapabilityResult{
		Content: "done", OK: true, Metadata: map[string]any{"source": "sqlite"},
	})
	if err != nil || !completed || finished.State != runner.TaskCompleted || finished.Result == nil || finished.Result.Content != "done" {
		t.Fatalf("complete runner task failed: task=%#v completed=%t err=%v", finished, completed, err)
	}
	again, completed, err := store.CompleteTask(ctx, claimed.ID, claimed.WorkerID, claimed.Generation, core.CapabilityResult{Content: "replacement", OK: true})
	if err != nil || completed || again.Result == nil || again.Result.Content != "done" {
		t.Fatalf("terminal completion was not idempotent: task=%#v completed=%t err=%v", again, completed, err)
	}
	if disposition, err := store.CancelTask(ctx, "rtask_order_b"); err != nil || disposition != runner.CancelDispositionCancelled {
		t.Fatalf("queued cancellation failed: disposition=%q err=%v", disposition, err)
	}
	if disposition, err := store.CancelTask(ctx, "rtask_order_b"); err != nil || disposition != runner.CancelDispositionTerminal {
		t.Fatalf("terminal cancellation was not idempotent: disposition=%q err=%v", disposition, err)
	}
	if pending, err := store.PendingTasks(ctx); err != nil || pending != 1 {
		t.Fatalf("pending count wrong: pending=%d err=%v", pending, err)
	}

	// The partial index permits every empty-key submission to create a new row.
	first, made, err := store.CreateTask(ctx, runner.Task{Capability: "private.empty", Args: map[string]any{"n": 1}, AvailableAt: future})
	if err != nil || !made {
		t.Fatalf("first empty-key create failed: made=%t err=%v", made, err)
	}
	second, made, err := store.CreateTask(ctx, runner.Task{Capability: "private.empty", Args: map[string]any{"n": 1}, AvailableAt: future})
	if err != nil || !made || second.ID == first.ID {
		t.Fatalf("empty-key create was deduplicated: first=%q second=%q made=%t err=%v", first.ID, second.ID, made, err)
	}

	// A result over the provider-neutral hard limit is rejected before mutation.
	if _, err := sessions.db.ExecContext(ctx, "UPDATE runner_tasks SET available_at = 1 WHERE id = ?", first.ID); err != nil {
		t.Fatal(err)
	}
	largeClaim, ok, err := store.ClaimTask(ctx, sqlRunnerClaimOptions("worker-limit", time.Minute))
	if err != nil || !ok || largeClaim.ID != first.ID {
		t.Fatalf("claim result-limit task failed: task=%#v ok=%t err=%v", largeClaim, ok, err)
	}
	if _, completed, err := store.CompleteTask(ctx, largeClaim.ID, largeClaim.WorkerID, largeClaim.Generation,
		core.CapabilityResult{Content: strings.Repeat("x", core.HardMaxCapabilityOutputBytes+1)}); err == nil || completed {
		t.Fatalf("oversized result was accepted: completed=%t err=%v", completed, err)
	}
	stillClaimed, err := store.GetTask(ctx, largeClaim.ID)
	if err != nil || stillClaimed.State != runner.TaskClaimed {
		t.Fatalf("oversized result mutated task: task=%#v err=%v", stillClaimed, err)
	}
}

func TestSQLRunnerStoreClaimSelectsCapabilitiesAndPreservesUnmatchedTasks(t *testing.T) {
	store, _ := newTestSQLRunnerStore(t)
	ctx := context.Background()
	available := time.Now().UTC().Add(-time.Minute).Truncate(time.Millisecond)
	for _, task := range []runner.Task{
		{ID: "rtask_selector_renderer_z", Capability: "runner.renderer", Args: map[string]any{}, AvailableAt: available, CreatedAt: available},
		{ID: "rtask_selector_renderer_a", Capability: "runner.renderer", Args: map[string]any{}, AvailableAt: available, CreatedAt: available.Add(time.Millisecond)},
		{ID: "rtask_selector_analytics", Capability: "runner.analytics", Args: map[string]any{}, AvailableAt: available, CreatedAt: available},
	} {
		if _, created, err := store.CreateTask(ctx, task); err != nil || !created {
			t.Fatalf("create %s: created=%t err=%v", task.ID, created, err)
		}
	}
	capabilities := []string{" runner.renderer ", "runner.renderer"}
	claimed, ok, err := store.ClaimTask(ctx, runner.ClaimOptions{WorkerID: "renderer", Capabilities: capabilities, LeaseTTL: time.Minute})
	if err != nil || !ok || claimed.ID != "rtask_selector_renderer_z" {
		t.Fatalf("renderer selector claim = %#v ok=%t err=%v", claimed, ok, err)
	}
	if got := strings.Join(capabilities, ","); got != " runner.renderer ,runner.renderer" {
		t.Fatalf("claim selector mutated caller capabilities: %q", got)
	}
	if pending, err := store.PendingTasks(ctx); err != nil || pending != 2 {
		t.Fatalf("pending must remain global: pending=%d err=%v", pending, err)
	}
	before, err := store.GetTask(ctx, "rtask_selector_analytics")
	if err != nil {
		t.Fatal(err)
	}
	if noMatch, ok, err := store.ClaimTask(ctx, sqlRunnerClaimOptions("none", time.Minute, "runner.unmatched")); err != nil || ok || noMatch.ID != "" {
		t.Fatalf("unmatched selector claim = %#v ok=%t err=%v", noMatch, ok, err)
	}
	after, err := store.GetTask(ctx, before.ID)
	if err != nil || after.State != runner.TaskQueued || after.Attempt != before.Attempt || after.Generation != before.Generation {
		t.Fatalf("unmatched selector mutated task: before=%#v after=%#v err=%v", before, after, err)
	}
	analytics, ok, err := store.ClaimTask(ctx, sqlRunnerClaimOptions("analytics", time.Minute, "runner.analytics"))
	if err != nil || !ok || analytics.ID != "rtask_selector_analytics" {
		t.Fatalf("analytics selector claim = %#v ok=%t err=%v", analytics, ok, err)
	}
	tooMany := make([]string, runner.MaxClaimCapabilities+1)
	for index := range tooMany {
		tooMany[index] = "runner.renderer"
	}
	for name, options := range map[string]runner.ClaimOptions{
		"wildcard": {WorkerID: "worker", Capabilities: []string{"*"}, LeaseTTL: time.Minute},
		"invalid":  {WorkerID: "worker", Capabilities: []string{"not a capability"}, LeaseTTL: time.Minute},
		"too_many": {WorkerID: "worker", Capabilities: tooMany, LeaseTTL: time.Minute},
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := store.ClaimTask(ctx, options); err == nil {
				t.Fatal("invalid capability selector was accepted")
			}
		})
	}
}

func TestSQLRunnerStoreTraceCarrier(t *testing.T) {
	store, sessions := newTestSQLRunnerStore(t)
	ctx := context.Background()
	carrier := core.TelemetryTraceContext{
		TraceParent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		TraceState:  "vendor=original",
	}
	created, made, err := store.CreateTask(ctx, runner.Task{
		ID: "rtask_trace_roundtrip", Capability: "private.trace", Args: map[string]any{"trace": true},
		TraceContext: carrier,
	})
	if err != nil || !made || created.TraceContext != carrier {
		t.Fatalf("trace carrier create round trip failed: task=%#v made=%t err=%v", created, made, err)
	}
	loaded, err := store.GetTask(ctx, created.ID)
	if err != nil || loaded.TraceContext != carrier {
		t.Fatalf("trace carrier get round trip failed: task=%#v err=%v", loaded, err)
	}
	claimed, ok, err := store.ClaimTask(ctx, sqlRunnerClaimOptions("trace-worker", time.Minute))
	if err != nil || !ok || claimed.ID != created.ID || claimed.TraceContext != carrier {
		t.Fatalf("trace carrier claim round trip failed: task=%#v ok=%t err=%v", claimed, ok, err)
	}

	canonical, made, err := store.CreateTask(ctx, runner.Task{
		ID: "rtask_trace_canonical", Capability: "private.trace", IdempotencyKey: "trace-replay",
		TenantID: "acme", SubjectID: "alice", Scope: "global/product/acme/alice",
		Args: map[string]any{"same": true}, AvailableAt: time.Now().UTC().Add(time.Hour), TraceContext: carrier,
	})
	if err != nil || !made {
		t.Fatalf("canonical trace task create failed: task=%#v made=%t err=%v", canonical, made, err)
	}
	replayed, made, err := store.CreateTask(ctx, runner.Task{
		ID: "rtask_trace_replay", Capability: "private.trace", IdempotencyKey: "trace-replay",
		TenantID: "acme", SubjectID: "alice", Scope: "global/product/acme/alice",
		Args: map[string]any{"same": true}, AvailableAt: time.Now().UTC().Add(time.Hour),
		TraceContext: core.TelemetryTraceContext{
			TraceParent: "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
			TraceState:  "vendor=replayed",
		},
	})
	if err != nil || made || replayed.ID != canonical.ID || replayed.TraceContext != carrier {
		t.Fatalf("trace carrier canonical replay failed: task=%#v made=%t err=%v", replayed, made, err)
	}

	empty, made, err := store.CreateTask(ctx, runner.Task{
		ID: "rtask_trace_empty", Capability: "private.trace", Args: map[string]any{},
		AvailableAt: time.Now().UTC().Add(time.Hour),
	})
	if err != nil || !made || empty.TraceContext != (core.TelemetryTraceContext{}) {
		t.Fatalf("empty trace carrier was not compatible: task=%#v made=%t err=%v", empty, made, err)
	}

	var rowsBefore int
	if err := sessions.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM runner_tasks").Scan(&rowsBefore); err != nil {
		t.Fatal(err)
	}
	for name, trace := range map[string]core.TelemetryTraceContext{
		"parent_too_long": {TraceParent: strings.Repeat("x", 257)},
		"state_too_long":  {TraceState: strings.Repeat("x", 513)},
		"parent_control":  {TraceParent: "00-valid\n"},
		"state_control":   {TraceState: "vendor=bad\x00"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := store.CreateTask(ctx, runner.Task{
				Capability: "private.trace", Args: map[string]any{}, TraceContext: trace,
			}); err == nil {
				t.Fatal("invalid incoming trace carrier was accepted")
			}
		})
	}
	var rowsAfter int
	if err := sessions.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM runner_tasks").Scan(&rowsAfter); err != nil {
		t.Fatal(err)
	}
	if rowsAfter != rowsBefore {
		t.Fatalf("invalid trace carrier wrote runner tasks: before=%d after=%d", rowsBefore, rowsAfter)
	}

	if _, err := sessions.db.ExecContext(ctx, "UPDATE runner_tasks SET trace_state = ? WHERE id = ?", "vendor=bad\x00", canonical.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetTask(ctx, canonical.ID); err == nil {
		t.Fatal("corrupt stored trace carrier was accepted")
	}
}

func TestSQLRunnerStoreRecoveryCancellationAndGeneration(t *testing.T) {
	store, sessions := newTestSQLRunnerStore(t)
	ctx := context.Background()
	created, _, err := store.CreateTask(ctx, runner.Task{
		ID: "rtask_retry", Capability: "private.retry", Args: map[string]any{"work": true}, MaxAttempts: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	first, ok, err := store.ClaimTask(ctx, sqlRunnerClaimOptions("worker-first", time.Minute))
	if err != nil || !ok || first.ID != created.ID {
		t.Fatalf("first claim failed: task=%#v ok=%t err=%v", first, ok, err)
	}
	if _, err := sessions.db.ExecContext(ctx, "UPDATE runner_tasks SET lease_expires_at = 1 WHERE id = ?", first.ID); err != nil {
		t.Fatal(err)
	}
	if requeued, terminal, err := store.RecoverExpiredTasks(ctx, time.Now().UTC()); err != nil || requeued != 1 || terminal != 0 {
		t.Fatalf("first recovery failed: requeued=%d terminal=%d err=%v", requeued, terminal, err)
	}
	second, ok, err := store.ClaimTask(ctx, sqlRunnerClaimOptions("worker-second", time.Minute))
	if err != nil || !ok || second.ID != first.ID || second.Attempt != 2 || second.Generation != 2 {
		t.Fatalf("second claim did not advance fence: task=%#v ok=%t err=%v", second, ok, err)
	}
	if renewed, err := store.RenewTaskClaim(ctx, first.ID, first.WorkerID, first.Generation, time.Minute); err != nil || renewed {
		t.Fatalf("expired owner renewed after reclaim: renewed=%t err=%v", renewed, err)
	}
	if stale, completed, err := store.CompleteTask(ctx, first.ID, first.WorkerID, first.Generation, core.CapabilityResult{OK: true}); err != nil || completed || stale.Generation != 2 {
		t.Fatalf("expired owner crossed generation fence: task=%#v completed=%t err=%v", stale, completed, err)
	}
	if _, err := sessions.db.ExecContext(ctx, "UPDATE runner_tasks SET lease_expires_at = 1 WHERE id = ?", second.ID); err != nil {
		t.Fatal(err)
	}
	if requeued, terminal, err := store.RecoverExpiredTasks(ctx, time.Now().UTC()); err != nil || requeued != 0 || terminal != 1 {
		t.Fatalf("attempt exhaustion recovery failed: requeued=%d terminal=%d err=%v", requeued, terminal, err)
	}
	failed, err := store.GetTask(ctx, second.ID)
	if err != nil || failed.State != runner.TaskFailed || failed.ErrorCode != "worker_lost" || failed.CompletedAt.IsZero() {
		t.Fatalf("exhausted task not failed: task=%#v err=%v", failed, err)
	}

	cancelTask, _, err := store.CreateTask(ctx, runner.Task{ID: "rtask_cancel_claim", Capability: "private.cancel", Args: map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	claimed, ok, err := store.ClaimTask(ctx, sqlRunnerClaimOptions("worker-cancel", time.Minute))
	if err != nil || !ok || claimed.ID != cancelTask.ID {
		t.Fatalf("cancel task claim failed: task=%#v ok=%t err=%v", claimed, ok, err)
	}
	if disposition, err := store.CancelTask(ctx, claimed.ID); err != nil || disposition != runner.CancelDispositionRequested {
		t.Fatalf("claimed cancel was not requested: disposition=%q err=%v", disposition, err)
	}
	if _, err := sessions.db.ExecContext(ctx, "UPDATE runner_tasks SET lease_expires_at = 1 WHERE id = ?", claimed.ID); err != nil {
		t.Fatal(err)
	}
	if requeued, terminal, err := store.RecoverExpiredTasks(ctx, time.Now().UTC()); err != nil || requeued != 0 || terminal != 1 {
		t.Fatalf("cancel recovery failed: requeued=%d terminal=%d err=%v", requeued, terminal, err)
	}
	cancelled, err := store.GetTask(ctx, claimed.ID)
	if err != nil || cancelled.State != runner.TaskCancelled || !cancelled.CancelRequested || cancelled.CompletedAt.IsZero() {
		t.Fatalf("cancel-requested task not terminal: task=%#v err=%v", cancelled, err)
	}
}

func TestSQLRunnerStoreRejectsCorruptJSONAndClaimProjection(t *testing.T) {
	store, sessions := newTestSQLRunnerStore(t)
	ctx := context.Background()
	created, _, err := store.CreateTask(ctx, runner.Task{ID: "rtask_corrupt", Capability: "private.decode", Args: map[string]any{"ok": true}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sessions.db.ExecContext(ctx, "UPDATE runner_tasks SET args_json = '{' WHERE id = ?", created.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetTask(ctx, created.ID); err == nil {
		t.Fatal("corrupt args JSON was accepted")
	}
	if _, err := sessions.db.ExecContext(ctx, "UPDATE runner_tasks SET args_json = '{}', args_digest = ? WHERE id = ?",
		"44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a", created.ID); err != nil {
		t.Fatal(err)
	}
	claimed, ok, err := store.ClaimTask(ctx, sqlRunnerClaimOptions("worker-corrupt", time.Minute))
	if err != nil || !ok {
		t.Fatalf("claim repaired task: task=%#v ok=%t err=%v", claimed, ok, err)
	}
	if _, err := sessions.db.ExecContext(ctx, "UPDATE runner_tasks SET lease_expires_at = 0 WHERE id = ?", claimed.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetTask(ctx, claimed.ID); err == nil {
		t.Fatal("claimed task with missing lease was accepted")
	}
}

func TestSQLRunnerStoreRoundsUpSubMillisecondLease(t *testing.T) {
	store, _ := newTestSQLRunnerStore(t)
	ctx := context.Background()
	created, _, err := store.CreateTask(ctx, runner.Task{
		ID: "rtask_short_lease", Capability: "private.short", Args: map[string]any{},
	})
	if err != nil {
		t.Fatal(err)
	}
	claimed, ok, err := store.ClaimTask(ctx, sqlRunnerClaimOptions("worker-short", time.Nanosecond))
	if err != nil || !ok || claimed.ID != created.ID || !claimed.LeaseExpiresAt.After(claimed.UpdatedAt) {
		t.Fatalf("sub-millisecond lease was truncated: task=%#v ok=%t err=%v", claimed, ok, err)
	}
}

func TestSQLRunnerSchemaV24MigrationAndFutureRefusal(t *testing.T) {
	t.Run("v24_to_v25", func(t *testing.T) {
		db, err := sql.Open("sqlite", t.TempDir()+"/v24-runner.db")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		ctx := context.Background()
		if _, err := db.ExecContext(ctx, sqlSchemaV15); err != nil {
			t.Fatal(err)
		}
		if err := migrateSQLSchema(ctx, db, SQLDialectSQLite, 0); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, sqlSchemaV24); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, sqlInsertMetaRow.bind(SQLDialectSQLite), "24"); err != nil {
			t.Fatal(err)
		}
		if _, err := OpenSQLSessionStore(ctx, db, SQLDialectSQLite); err != nil {
			t.Fatal(err)
		}
		var version string
		if err := db.QueryRowContext(ctx, sqlSelectMetaRow.bind(SQLDialectSQLite)).Scan(&version); err != nil || version != strconv.Itoa(SQLSchemaVersion) {
			t.Fatalf("current schema version missing after runner migration: version=%q err=%v", version, err)
		}
		for _, column := range []string{"trace_parent", "trace_state"} {
			exists, err := sqlColumnExists(ctx, db, SQLDialectSQLite, "runner_tasks", column)
			if err != nil || !exists {
				t.Fatalf("v25 runner task column %s missing: exists=%t err=%v", column, exists, err)
			}
		}
	})
	t.Run("fresh_v25_has_carrier_columns", func(t *testing.T) {
		db, err := sql.Open("sqlite", t.TempDir()+"/fresh-runner.db")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		ctx := context.Background()
		if _, err := OpenSQLSessionStore(ctx, db, SQLDialectSQLite); err != nil {
			t.Fatal(err)
		}
		for _, column := range []string{"trace_parent", "trace_state"} {
			exists, err := sqlColumnExists(ctx, db, SQLDialectSQLite, "runner_tasks", column)
			if err != nil || !exists {
				t.Fatalf("fresh v25 runner task column %s missing: exists=%t err=%v", column, exists, err)
			}
		}
	})
	t.Run("future_refusal_before_v25_ddl", func(t *testing.T) {
		db, err := sql.Open("sqlite", t.TempDir()+"/future-runner.db")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		ctx := context.Background()
		if _, err := db.ExecContext(ctx, `CREATE TABLE store_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
			INSERT INTO store_meta (key, value) VALUES ('schema_version', '999');`); err != nil {
			t.Fatal(err)
		}
		if _, err := OpenSQLSessionStore(ctx, db, SQLDialectSQLite); err == nil {
			t.Fatal("future schema was accepted")
		}
		var count int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='runner_tasks'").Scan(&count); err != nil || count != 0 {
			t.Fatalf("v25 DDL ran before future refusal: count=%d err=%v", count, err)
		}
	})
}

func TestSQLRunnerStoreRejectsInFlightOverflow(t *testing.T) {
	store, _ := newTestSQLRunnerStore(t)
	store.maxInFlight = 2
	ctx := context.Background()
	first, _, err := store.CreateTask(ctx, runner.Task{Capability: "private.render", IdempotencyKey: "one", Args: map[string]any{"n": 1}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.CreateTask(ctx, runner.Task{Capability: "private.render", IdempotencyKey: "two", Args: map[string]any{"n": 2}}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.CreateTask(ctx, runner.Task{Capability: "private.render", IdempotencyKey: "three", Args: map[string]any{"n": 3}}); err == nil {
		t.Fatal("sql in-flight overflow was accepted")
	}
	replayed, fresh, err := store.CreateTask(ctx, runner.Task{Capability: "private.render", IdempotencyKey: "one", Args: map[string]any{"n": 1}})
	if err != nil || fresh || replayed.ID != first.ID {
		t.Fatalf("sql idempotent replay must still work at cap: %#v fresh=%t err=%v", replayed, fresh, err)
	}
	if _, err := store.CancelTask(ctx, first.ID); err != nil {
		t.Fatal(err)
	}
	if _, fresh, err := store.CreateTask(ctx, runner.Task{Capability: "private.render", IdempotencyKey: "three", Args: map[string]any{"n": 3}}); err != nil || !fresh {
		t.Fatalf("sql cancelled slot was not freed: fresh=%t err=%v", fresh, err)
	}
}

func TestSQLRunnerStoreRejectsStoredOverflow(t *testing.T) {
	store, _ := newTestSQLRunnerStore(t)
	store.maxStored = 2
	ctx := context.Background()
	first, _, err := store.CreateTask(ctx, runner.Task{Capability: "private.render", IdempotencyKey: "one", Args: map[string]any{"n": 1}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.CreateTask(ctx, runner.Task{Capability: "private.render", IdempotencyKey: "two", Args: map[string]any{"n": 2}}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.CreateTask(ctx, runner.Task{Capability: "private.render", IdempotencyKey: "three", Args: map[string]any{"n": 3}}); err == nil {
		t.Fatal("sql stored runner overflow was accepted")
	}
	replayed, fresh, err := store.CreateTask(ctx, runner.Task{Capability: "private.render", IdempotencyKey: "one", Args: map[string]any{"n": 1}})
	if err != nil || fresh || replayed.ID != first.ID {
		t.Fatalf("idempotent replay was blocked by the stored cap: %#v fresh=%t err=%v", replayed, fresh, err)
	}
}
