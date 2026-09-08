package runner

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

func runnerIdentity(t *testing.T) (core.Principal, core.ScopePath) {
	t.Helper()
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	tenant, err := global.Child(core.ScopeRef{Kind: core.ScopeTenant, ID: "acme"})
	if err != nil {
		t.Fatal(err)
	}
	user, err := tenant.Child(core.ScopeRef{Kind: core.ScopeUser, ID: "alice"})
	if err != nil {
		t.Fatal(err)
	}
	return core.Principal{TenantID: "acme", SubjectID: "alice", Scope: user}, user
}

func TestNextAdaptivePollBoundsRunnerIdleWait(t *testing.T) {
	base := 50 * time.Millisecond
	max := 250 * time.Millisecond
	current := base
	for _, expected := range []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, max, max} {
		current = nextAdaptivePoll(current, base, max)
		if current != expected {
			t.Fatalf("next adaptive poll = %s, want %s", current, expected)
		}
	}
}

func runnerTask(t *testing.T, key string, args map[string]any, maxAttempts int) Task {
	t.Helper()
	principal, scope := runnerIdentity(t)
	return Task{
		Capability: "runner.render", IdempotencyKey: key,
		TenantID: principal.TenantID, SubjectID: principal.SubjectID, Scope: scope.String(),
		Args: args, MaxAttempts: maxAttempts,
	}
}

func claimOptions(worker string, ttl time.Duration, capabilities ...string) ClaimOptions {
	return ClaimOptions{WorkerID: worker, Capabilities: append([]string(nil), capabilities...), LeaseTTL: ttl}
}

type runnerTraceContextKey struct{}

type runnerTraceTelemetry struct{}

type runnerTraceSpan struct{}

func (runnerTraceSpan) End(error, core.TelemetryAttributes) {}

func (runnerTraceTelemetry) Start(ctx context.Context, _ string, _ core.TelemetryAttributes) (context.Context, core.TelemetrySpan) {
	return ctx, runnerTraceSpan{}
}

func (runnerTraceTelemetry) AddCounter(context.Context, string, int64, core.TelemetryAttributes) {}

func (runnerTraceTelemetry) RecordHistogram(context.Context, string, float64, string, core.TelemetryAttributes) {
}

func (runnerTraceTelemetry) SetGauge(context.Context, string, float64, string, core.TelemetryAttributes) {
}

func (runnerTraceTelemetry) InjectTraceContext(ctx context.Context) core.TelemetryTraceContext {
	traceContext, _ := ctx.Value(runnerTraceContextKey{}).(core.TelemetryTraceContext)
	return traceContext
}

func (runnerTraceTelemetry) ExtractTraceContext(ctx context.Context, _ core.TelemetryTraceContext) context.Context {
	return ctx
}

type completeOnReadStore struct {
	*MemoryStore
	created []Task
}

type queueOnlyStore struct{ Store }

type cancelDelegatingStore struct {
	Store
	ctx         context.Context
	id          string
	disposition CancelDisposition
	err         error
}

func (s *cancelDelegatingStore) CancelTask(ctx context.Context, id string) (CancelDisposition, error) {
	s.ctx = ctx
	s.id = id
	return s.disposition, s.err
}

func (s *completeOnReadStore) CreateTask(ctx context.Context, task Task) (Task, bool, error) {
	created, fresh, err := s.MemoryStore.CreateTask(ctx, task)
	if err == nil && fresh {
		s.created = append(s.created, created)
	}
	return created, fresh, err
}

func (s *completeOnReadStore) GetTask(ctx context.Context, id string) (Task, error) {
	task, err := s.MemoryStore.GetTask(ctx, id)
	if err != nil || task.State != TaskQueued {
		return task, err
	}
	claim, claimed, err := s.MemoryStore.ClaimTask(ctx, claimOptions("runner-trace-test", time.Hour))
	if err != nil || !claimed || claim.ID != id {
		return task, err
	}
	if _, _, err := s.MemoryStore.CompleteTask(ctx, id, claim.WorkerID, claim.Generation, core.CapabilityResult{Content: "done", OK: true}); err != nil {
		return Task{}, err
	}
	return s.MemoryStore.GetTask(ctx, id)
}

func TestTaskQueryNormalize(t *testing.T) {
	query, err := (TaskQuery{
		ID: " rtask_catalog_id ", TenantID: " acme ", SubjectID: " alice ", ScopePrefix: " global/product/acme/alice ",
		Capability: " runner.render ", WorkerID: " worker-a ",
		States: []TaskState{TaskQueued, TaskCompleted, TaskQueued},
	}).Normalize()
	if err != nil {
		t.Fatal(err)
	}
	if query.ID != "rtask_catalog_id" || query.TenantID != "acme" || query.SubjectID != "alice" || query.ScopePrefix != "global/product/acme/alice" ||
		query.Capability != "runner.render" || query.WorkerID != "worker-a" || query.Limit != DefaultCatalogLimit || query.Offset != 0 ||
		len(query.States) != 2 || query.States[0] != TaskCompleted || query.States[1] != TaskQueued {
		t.Fatalf("normalized query = %#v", query)
	}
	for name, query := range map[string]TaskQuery{
		"control":      {TenantID: "acme\ncorp"},
		"capability":   {Capability: "not namespaced"},
		"id prefix":    {ID: "task_catalog_id"},
		"id empty":     {ID: "rtask_"},
		"id unsafe":    {ID: "rtask_catalog/id"},
		"id backslash": {ID: "rtask_catalog\\id"},
		"id control":   {ID: "rtask_catalog\nid"},
		"id too long":  {ID: "rtask_" + strings.Repeat("a", 123)},
		"limit":        {Limit: MaxCatalogLimit + 1},
		"offset":       {Offset: MaxCatalogOffset + 1},
		"state":        {States: []TaskState{"unknown"}},
	} {
		t.Run(name, func(t *testing.T) {
			if err := query.Validate(); err == nil {
				t.Fatalf("invalid catalog query was accepted: %#v", query)
			}
		})
	}
}

func TestMemoryStoreTaskCatalogFiltersOrderingAndMetadata(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	base := time.Now().UTC().Add(-time.Hour)
	prefix := "global/product/tenant%_a/user_a/jobs%_root"
	create := func(id, tenant, subject, scope, capability string, createdAt time.Time, trace bool) Task {
		t.Helper()
		task := Task{
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
	create("rtask_catalog_a", "tenant-a", "user-a", prefix, "private.catalog", base, true)
	create("rtask_catalog_b", "tenant-a", "user-a", prefix, "private.catalog", base, false)
	create("rtask_catalog_newer", "tenant-a", "user-a", prefix+"/child", "private.catalog", base.Add(time.Minute), false)
	create("rtask_catalog_like_sibling", "tenant-a", "user-a", "global/product/tenantQaa/user_a/jobsZZroot/child", "private.catalog", base.Add(2*time.Minute), false)
	create("rtask_catalog_other_tenant", "tenant-b", "user-a", prefix, "private.catalog", base.Add(3*time.Minute), false)
	create("rtask_catalog_other_subject", "tenant-a", "user-b", prefix, "private.catalog", base.Add(4*time.Minute), false)
	create("rtask_catalog_other_capability", "tenant-a", "user-a", prefix, "private.other", base.Add(5*time.Minute), false)
	worker := create("rtask_catalog_worker", "tenant-a", "user-a", prefix, "private.worker", base.Add(6*time.Minute), false)
	claimed, ok, err := store.ClaimTask(ctx, claimOptions("worker-a", time.Hour, "private.worker"))
	if err != nil || !ok || claimed.ID != worker.ID {
		t.Fatalf("claim catalog worker task: task=%#v ok=%t err=%v", claimed, ok, err)
	}
	completed := create("rtask_catalog_completed", "tenant-a", "user-a", prefix, "private.done", base.Add(7*time.Minute), true)
	completedClaim, ok, err := store.ClaimTask(ctx, claimOptions("worker-done", time.Hour, "private.done"))
	if err != nil || !ok || completedClaim.ID != completed.ID {
		t.Fatalf("claim catalog completed task: task=%#v ok=%t err=%v", completedClaim, ok, err)
	}
	if _, committed, err := store.CompleteTask(ctx, completedClaim.ID, completedClaim.WorkerID, completedClaim.Generation,
		core.CapabilityResult{Content: "completed-result-secret", OK: true}); err != nil || !committed {
		t.Fatalf("complete catalog task: committed=%t err=%v", committed, err)
	}

	query := TaskQuery{
		TenantID: " tenant-a ", SubjectID: " user-a ", ScopePrefix: " " + prefix + " ", Capability: " private.catalog ",
		States: []TaskState{TaskQueued, TaskQueued}, Limit: 2,
	}
	page, total, err := store.ListTasks(ctx, query)
	if err != nil || total != 3 || len(page) != 2 || page[0].ID != "rtask_catalog_newer" || page[1].ID != "rtask_catalog_a" {
		t.Fatalf("catalog first page = %#v total=%d err=%v", page, total, err)
	}
	page, total, err = store.ListTasks(ctx, TaskQuery{
		TenantID: "tenant-a", SubjectID: "user-a", ScopePrefix: prefix, Capability: "private.catalog", States: []TaskState{TaskQueued}, Limit: 2, Offset: 2,
	})
	if err != nil || total != 3 || len(page) != 1 || page[0].ID != "rtask_catalog_b" {
		t.Fatalf("catalog second page = %#v total=%d err=%v", page, total, err)
	}
	idPage, idTotal, err := store.ListTasks(ctx, TaskQuery{
		ID: " rtask_catalog_newer ", TenantID: "tenant-a", SubjectID: "user-a", ScopePrefix: prefix,
		Capability: "private.catalog", States: []TaskState{TaskQueued}, Limit: 1,
	})
	if err != nil || idTotal != 1 || len(idPage) != 1 || idPage[0].ID != "rtask_catalog_newer" {
		t.Fatalf("catalog ID filter = %#v total=%d err=%v", idPage, idTotal, err)
	}
	idPage, idTotal, err = store.ListTasks(ctx, TaskQuery{
		ID: "rtask_catalog_newer", TenantID: "tenant-a", SubjectID: "user-a", ScopePrefix: prefix,
		Capability: "private.catalog", States: []TaskState{TaskQueued}, Limit: 1, Offset: 1,
	})
	if err != nil || idTotal != 1 || len(idPage) != 0 {
		t.Fatalf("catalog ID paged filter = %#v total=%d err=%v", idPage, idTotal, err)
	}
	workerPage, workerTotal, err := store.ListTasks(ctx, TaskQuery{WorkerID: "worker-a", States: []TaskState{TaskClaimed}})
	if err != nil || workerTotal != 1 || len(workerPage) != 1 || workerPage[0].ID != worker.ID || workerPage[0].WorkerID != "worker-a" {
		t.Fatalf("worker catalog filter = %#v total=%d err=%v", workerPage, workerTotal, err)
	}
	donePage, doneTotal, err := store.ListTasks(ctx, TaskQuery{States: []TaskState{TaskCompleted}})
	if err != nil || doneTotal != 1 || len(donePage) != 1 || !donePage[0].HasResult || !donePage[0].HasTraceContext {
		t.Fatalf("completed catalog metadata = %#v total=%d err=%v", donePage, doneTotal, err)
	}
	encoded, err := json.Marshal(donePage[0])
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"args", "result", "idempotency_key", "args_digest", "trace_context"} {
		if _, exists := fields[field]; exists {
			t.Fatalf("catalog serialization exposed %q: %s", field, encoded)
		}
	}
	for _, secret := range []string{"rtask_catalog_completed-args-secret", "completed-result-secret", "rtask_catalog_completed-trace-secret"} {
		if strings.Contains(string(encoded), secret) {
			t.Fatalf("catalog serialization exposed secret %q: %s", secret, encoded)
		}
	}
}

func TestHubListTasksReturnsCatalogUnsupportedForQueueOnlyStore(t *testing.T) {
	hub := NewHubWithStore(queueOnlyStore{Store: NewMemoryStore()})
	if _, _, err := hub.ListTasks(context.Background(), TaskQuery{}); !errors.Is(err, ErrCatalogUnsupported) {
		t.Fatalf("queue-only store catalog error = %v, want ErrCatalogUnsupported", err)
	}
}

func TestHubPruneTasksReturnsRetentionUnsupportedForQueueOnlyStore(t *testing.T) {
	hub := NewHubWithStore(queueOnlyStore{Store: NewMemoryStore()})
	if _, err := hub.PruneTasks(context.Background(), time.Now().UTC()); !errors.Is(err, ErrRetentionUnsupported) {
		t.Fatalf("queue-only store retention error = %v, want ErrRetentionUnsupported", err)
	}
}

func setMemoryRetentionTask(t *testing.T, store *MemoryStore, task Task, state TaskState, completedAt, updatedAt time.Time) Task {
	t.Helper()
	created, fresh, err := store.CreateTask(context.Background(), task)
	if err != nil || !fresh {
		t.Fatalf("create retention task = %#v fresh=%t err=%v", created, fresh, err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	stored := store.tasks[created.ID]
	stored.State = state
	stored.CompletedAt = completedAt
	stored.UpdatedAt = updatedAt
	if state == TaskClaimed {
		stored.WorkerID = "retention-worker"
		stored.LeaseExpiresAt = updatedAt.Add(time.Hour)
	}
	store.tasks[created.ID] = stored
	return stored
}

func TestMemoryStorePruneTasksRetainsRequiredTasksAndCleansCatalog(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	cutoff := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	old := cutoff.Add(-time.Millisecond)
	recent := cutoff.Add(time.Millisecond)

	oldCompleted := setMemoryRetentionTask(t, store, runnerTask(t, "", map[string]any{"state": "completed"}, 1), TaskCompleted, old, old)
	oldCancelled := setMemoryRetentionTask(t, store, runnerTask(t, "", map[string]any{"state": "cancelled"}, 1), TaskCancelled, old, old)
	oldFailed := setMemoryRetentionTask(t, store, runnerTask(t, "", map[string]any{"state": "failed"}, 1), TaskFailed, old, old)
	keyed := setMemoryRetentionTask(t, store, runnerTask(t, "retain-key", map[string]any{"state": "keyed"}, 1), TaskCompleted, old, old)
	queued := setMemoryRetentionTask(t, store, runnerTask(t, "", map[string]any{"state": "queued"}, 1), TaskQueued, time.Time{}, old)
	claimed := setMemoryRetentionTask(t, store, runnerTask(t, "", map[string]any{"state": "claimed"}, 1), TaskClaimed, time.Time{}, old)
	recentCompleted := setMemoryRetentionTask(t, store, runnerTask(t, "", map[string]any{"state": "recent"}, 1), TaskCompleted, recent, recent)
	equalCutoff := setMemoryRetentionTask(t, store, runnerTask(t, "", map[string]any{"state": "equal"}, 1), TaskFailed, cutoff, cutoff)
	missingCompletion := setMemoryRetentionTask(t, store, runnerTask(t, "", map[string]any{"state": "missing-completion"}, 1), TaskCancelled, time.Time{}, old)

	// Empty-key tasks should never have a dedupe record, but prune defensively
	// clears any accidental record that points to a removed task.
	store.mu.Lock()
	store.dedupe[taskDedupeKey(oldCompleted)] = oldCompleted.ID
	store.mu.Unlock()

	if pruned, err := store.PruneTasks(ctx, time.Time{}); err == nil || pruned != 0 {
		t.Fatalf("zero cutoff prune = %d, %v", pruned, err)
	}
	pruned, err := store.PruneTasks(ctx, cutoff)
	if err != nil || pruned != 3 {
		t.Fatalf("prune count = %d, %v; want 3, nil", pruned, err)
	}

	removed := []string{oldCompleted.ID, oldCancelled.ID, oldFailed.ID}
	for _, id := range removed {
		if _, err := store.GetTask(ctx, id); !errors.Is(err, ErrTaskNotFound) {
			t.Fatalf("pruned task %s lookup error = %v, want ErrTaskNotFound", id, err)
		}
	}
	retained := []Task{keyed, queued, claimed, recentCompleted, equalCutoff, missingCompletion}
	for _, task := range retained {
		if got, err := store.GetTask(ctx, task.ID); err != nil || got.ID != task.ID {
			t.Fatalf("retained task %s = %#v, %v", task.ID, got, err)
		}
	}
	if _, exists := store.dedupe[taskDedupeKey(oldCompleted)]; exists {
		t.Fatal("pruned empty-key task left a dedupe mapping")
	}
	if got := store.dedupe[taskDedupeKey(keyed)]; got != keyed.ID {
		t.Fatalf("keyed task dedupe mapping = %q, want %q", got, keyed.ID)
	}
	for _, id := range store.ordered {
		if _, exists := store.tasks[id]; !exists {
			t.Fatalf("ordered task id %q has no retained task", id)
		}
		for _, prunedID := range removed {
			if id == prunedID {
				t.Fatalf("ordered task id %q was not pruned", id)
			}
		}
	}
	page, total, err := store.ListTasks(ctx, TaskQuery{Limit: MaxCatalogLimit})
	if err != nil || total != len(retained) || len(page) != len(retained) {
		t.Fatalf("catalog after prune = %#v total=%d err=%v", page, total, err)
	}
	for _, summary := range page {
		for _, prunedID := range removed {
			if summary.ID == prunedID {
				t.Fatalf("catalog retained pruned task %q", summary.ID)
			}
		}
	}
}

func TestHubCancelDelegatesToStore(t *testing.T) {
	ctx := context.WithValue(context.Background(), runnerTraceContextKey{}, "cancel")
	wantErr := errors.New("cancel failed")
	store := &cancelDelegatingStore{
		Store: NewMemoryStore(), disposition: CancelDispositionRequested, err: wantErr,
	}
	disposition, err := NewHubWithStore(store).Cancel(ctx, "not-a-runner-id")
	if disposition != CancelDispositionRequested || !errors.Is(err, wantErr) {
		t.Fatalf("hub cancel = %q, %v", disposition, err)
	}
	if store.ctx != ctx || store.id != "not-a-runner-id" {
		t.Fatalf("hub cancel delegate = ctx=%#v id=%q", store.ctx, store.id)
	}
}

func TestMemoryStoreCreateDeduplicatesAndDefensivelyClones(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	input := runnerTask(t, "call-render", map[string]any{"job": map[string]any{"format": "png"}}, 3)
	created, fresh, err := store.CreateTask(ctx, input)
	if err != nil || !fresh || created.State != TaskQueued || created.ArgsDigest == "" {
		t.Fatalf("create = %#v fresh=%t err=%v", created, fresh, err)
	}
	input.Args["job"].(map[string]any)["format"] = "mutated"
	created.Args["job"].(map[string]any)["format"] = "also-mutated"
	stored, err := store.GetTask(ctx, created.ID)
	if err != nil || stored.Args["job"].(map[string]any)["format"] != "png" {
		t.Fatalf("canonical args were mutated: %#v err=%v", stored.Args, err)
	}
	replayed, fresh, err := store.CreateTask(ctx, runnerTask(t, "call-render", map[string]any{"job": map[string]any{"format": "png"}}, 3))
	if err != nil || fresh || replayed.ID != created.ID {
		t.Fatalf("dedupe = %#v fresh=%t err=%v", replayed, fresh, err)
	}
	if _, _, err := store.CreateTask(ctx, runnerTask(t, "call-render", map[string]any{"job": "different"}, 3)); !errors.Is(err, ErrSubmissionConflict) {
		t.Fatalf("changed args reused key: %v", err)
	}
	second, fresh, err := store.CreateTask(ctx, runnerTask(t, "", map[string]any{"job": "png"}, 3))
	if err != nil || !fresh || second.ID == created.ID {
		t.Fatalf("empty-key task was not unique: %#v fresh=%t err=%v", second, fresh, err)
	}
}

func TestMemoryStoreValidatesAndClonesDurableTraceContext(t *testing.T) {
	store := NewMemoryStore()
	traceContext := core.TelemetryTraceContext{
		TraceParent: strings.Repeat("p", 256),
		TraceState:  strings.Repeat("s", 512),
	}
	input := runnerTask(t, "trace-valid", map[string]any{}, 3)
	input.TraceContext = traceContext
	created, _, err := store.CreateTask(context.Background(), input)
	if err != nil || created.TraceContext != traceContext {
		t.Fatalf("create trace context = %#v err=%v", created.TraceContext, err)
	}
	stored, err := store.GetTask(context.Background(), created.ID)
	if err != nil || stored.TraceContext != traceContext {
		t.Fatalf("stored trace context = %#v err=%v", stored.TraceContext, err)
	}

	for name, invalid := range map[string]core.TelemetryTraceContext{
		"traceparent too long": {TraceParent: strings.Repeat("p", 257)},
		"tracestate too long":  {TraceState: strings.Repeat("s", 513)},
		"traceparent control":  {TraceParent: "trace\nparent"},
		"tracestate control":   {TraceState: "trace\x00state"},
	} {
		t.Run(name, func(t *testing.T) {
			task := runnerTask(t, "trace-invalid", map[string]any{}, 3)
			task.TraceContext = invalid
			if _, _, err := NewMemoryStore().CreateTask(context.Background(), task); err == nil {
				t.Fatal("invalid durable trace context was accepted")
			}
		})
	}
}

func TestHubSubmitScopedPreservesInitialTraceContext(t *testing.T) {
	store := &completeOnReadStore{MemoryStore: NewMemoryStore()}
	hub := NewHubWithStore(store)
	hub.Telemetry = runnerTraceTelemetry{}
	principal, scope := runnerIdentity(t)
	initialTrace := core.TelemetryTraceContext{TraceParent: "00-initial", TraceState: "vendor=initial"}
	initialCtx := context.WithValue(context.Background(), runnerTraceContextKey{}, initialTrace)
	result, err := hub.SubmitScoped(initialCtx, principal, scope, "runner.render", "trace-reconnect", map[string]any{"job": "render"}, 3)
	if err != nil || !result.OK || result.Content != "done" {
		t.Fatalf("initial submit = %#v err=%v", result, err)
	}
	if len(store.created) != 1 || store.created[0].TraceContext != initialTrace {
		t.Fatalf("initial created task = %#v", store.created)
	}

	reconnectTrace := core.TelemetryTraceContext{TraceParent: "00-reconnect", TraceState: "vendor=reconnect"}
	reconnectCtx := context.WithValue(context.Background(), runnerTraceContextKey{}, reconnectTrace)
	result, err = hub.SubmitScoped(reconnectCtx, principal, scope, "runner.render", "trace-reconnect", map[string]any{"job": "render"}, 3)
	if err != nil || !result.OK || result.Content != "done" {
		t.Fatalf("reconnect submit = %#v err=%v", result, err)
	}
	if len(store.created) != 1 {
		t.Fatalf("duplicate submission created %d tasks", len(store.created))
	}
	stored, err := store.MemoryStore.GetTask(context.Background(), store.created[0].ID)
	if err != nil || stored.TraceContext != initialTrace {
		t.Fatalf("canonical trace context = %#v err=%v", stored.TraceContext, err)
	}

	withoutTelemetry := &completeOnReadStore{MemoryStore: NewMemoryStore()}
	noTelemetryHub := NewHubWithStore(withoutTelemetry)
	result, err = noTelemetryHub.SubmitScoped(context.Background(), principal, scope, "runner.render", "trace-empty", map[string]any{"job": "render"}, 3)
	if err != nil || !result.OK || len(withoutTelemetry.created) != 1 || withoutTelemetry.created[0].TraceContext != (core.TelemetryTraceContext{}) {
		t.Fatalf("submit without telemetry = %#v created=%#v err=%v", result, withoutTelemetry.created, err)
	}
}

func TestProviderArtifactRevision(t *testing.T) {
	if got := (Provider{}).ArtifactRevision(); got != "private-runner-provider/v3" {
		t.Fatalf("artifact revision = %q", got)
	}
}

func TestMemoryStoreClaimSelectsCapabilitiesWithStableOrder(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	available := time.Now().UTC().Add(-time.Minute)
	for _, task := range []Task{
		{ID: "rtask_claim_renderer_b", Capability: "runner.renderer", Args: map[string]any{}, AvailableAt: available, CreatedAt: available},
		{ID: "rtask_claim_renderer_a", Capability: "runner.renderer", Args: map[string]any{}, AvailableAt: available, CreatedAt: available},
		{ID: "rtask_claim_analytics", Capability: "runner.analytics", Args: map[string]any{}, AvailableAt: available, CreatedAt: available},
	} {
		if _, created, err := store.CreateTask(ctx, task); err != nil || !created {
			t.Fatalf("create %s: created=%t err=%v", task.ID, created, err)
		}
	}
	capabilities := []string{" runner.renderer ", "runner.renderer"}
	claimed, ok, err := store.ClaimTask(ctx, ClaimOptions{WorkerID: "renderer", Capabilities: capabilities, LeaseTTL: time.Minute})
	if err != nil || !ok || claimed.ID != "rtask_claim_renderer_a" {
		t.Fatalf("renderer selector claim = %#v ok=%t err=%v", claimed, ok, err)
	}
	if got := strings.Join(capabilities, ","); got != " runner.renderer ,runner.renderer" {
		t.Fatalf("claim selector mutated caller capabilities: %q", got)
	}
	analytics, err := store.GetTask(ctx, "rtask_claim_analytics")
	if err != nil || analytics.State != TaskQueued || analytics.Attempt != 0 || analytics.Generation != 0 {
		t.Fatalf("unmatched analytics task changed: task=%#v err=%v", analytics, err)
	}
	if pending, err := store.PendingTasks(ctx); err != nil || pending != 2 {
		t.Fatalf("pending must remain global: pending=%d err=%v", pending, err)
	}
	claimed, ok, err = store.ClaimTask(ctx, claimOptions("analytics", time.Minute, "runner.analytics"))
	if err != nil || !ok || claimed.ID != "rtask_claim_analytics" {
		t.Fatalf("analytics selector claim = %#v ok=%t err=%v", claimed, ok, err)
	}
	claimed, ok, err = store.ClaimTask(ctx, ClaimOptions{WorkerID: "all-nil", LeaseTTL: time.Minute})
	if err != nil || !ok || claimed.ID != "rtask_claim_renderer_b" {
		t.Fatalf("nil selector claim = %#v ok=%t err=%v", claimed, ok, err)
	}
	if _, created, err := store.CreateTask(ctx, Task{ID: "rtask_claim_empty", Capability: "runner.analytics", Args: map[string]any{}, AvailableAt: available, CreatedAt: available}); err != nil || !created {
		t.Fatalf("create empty-selector task: created=%t err=%v", created, err)
	}
	claimed, ok, err = store.ClaimTask(ctx, ClaimOptions{WorkerID: "all-empty", Capabilities: []string{}, LeaseTTL: time.Minute})
	if err != nil || !ok || claimed.ID != "rtask_claim_empty" {
		t.Fatalf("empty selector claim = %#v ok=%t err=%v", claimed, ok, err)
	}
}

func TestMemoryStoreClaimSelectorRecoversAndSkipsUnmatchedTasks(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	available := time.Now().UTC().Add(-time.Minute)
	for _, task := range []Task{
		{ID: "rtask_recover_analytics", Capability: "runner.analytics", Args: map[string]any{}, AvailableAt: available, CreatedAt: available},
		{ID: "rtask_recover_renderer", Capability: "runner.renderer", Args: map[string]any{}, AvailableAt: available, CreatedAt: available.Add(time.Nanosecond)},
	} {
		if _, created, err := store.CreateTask(ctx, task); err != nil || !created {
			t.Fatalf("create %s: created=%t err=%v", task.ID, created, err)
		}
	}
	first, ok, err := store.ClaimTask(ctx, claimOptions("renderer-first", time.Minute, "runner.renderer"))
	if err != nil || !ok || first.ID != "rtask_recover_renderer" {
		t.Fatalf("first renderer claim = %#v ok=%t err=%v", first, ok, err)
	}
	store.mu.Lock()
	expired := store.tasks[first.ID]
	expired.LeaseExpiresAt = time.Now().UTC().Add(-time.Second)
	store.tasks[first.ID] = expired
	store.mu.Unlock()
	reclaimed, ok, err := store.ClaimTask(ctx, claimOptions("renderer-second", time.Minute, "runner.renderer"))
	if err != nil || !ok || reclaimed.ID != first.ID || reclaimed.Attempt != 2 || reclaimed.Generation != 2 {
		t.Fatalf("recovered renderer claim = %#v ok=%t err=%v", reclaimed, ok, err)
	}
	analytics, err := store.GetTask(ctx, "rtask_recover_analytics")
	if err != nil || analytics.State != TaskQueued || analytics.Attempt != 0 || analytics.Generation != 0 {
		t.Fatalf("recovery selector changed analytics task: task=%#v err=%v", analytics, err)
	}
}

func TestClaimOptionsNormalizeAndValidateCapabilities(t *testing.T) {
	capabilities := []string{" runner.renderer ", "runner.analytics", "runner.renderer"}
	normalized, err := normalizeClaimOptions(ClaimOptions{WorkerID: "worker", Capabilities: capabilities, LeaseTTL: time.Minute})
	if err != nil || len(normalized.Capabilities) != 2 || normalized.Capabilities[0] != "runner.analytics" || normalized.Capabilities[1] != "runner.renderer" {
		t.Fatalf("normalize capabilities = %#v err=%v", normalized.Capabilities, err)
	}
	normalized.Capabilities[0] = "mutated"
	if got := strings.Join(capabilities, ","); got != " runner.renderer ,runner.analytics,runner.renderer" {
		t.Fatalf("normalized selector shared caller storage: %q", got)
	}
	tooMany := make([]string, MaxClaimCapabilities+1)
	for index := range tooMany {
		tooMany[index] = "runner.renderer"
	}
	for name, options := range map[string]ClaimOptions{
		"wildcard": {WorkerID: "worker", Capabilities: []string{"*"}, LeaseTTL: time.Minute},
		"invalid":  {WorkerID: "worker", Capabilities: []string{"not a capability"}, LeaseTTL: time.Minute},
		"too_many": {WorkerID: "worker", Capabilities: tooMany, LeaseTTL: time.Minute},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := normalizeClaimOptions(options); err == nil {
				t.Fatal("invalid capability selector was accepted")
			}
		})
	}
}

func TestMemoryStoreClaimRenewCompleteUsesGenerationFence(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	created, _, err := store.CreateTask(ctx, runnerTask(t, "fenced-call", map[string]any{"job": "render"}, 3))
	if err != nil {
		t.Fatal(err)
	}
	claim, ok, err := store.ClaimTask(ctx, claimOptions("worker-a", time.Hour))
	if err != nil || !ok || claim.ID != created.ID || claim.Generation != 1 || claim.Attempt != 1 {
		t.Fatalf("claim = %#v ok=%t err=%v", claim, ok, err)
	}
	if renewed, err := store.RenewTaskClaim(ctx, claim.ID, "worker-b", claim.Generation, time.Hour); err != nil || renewed {
		t.Fatalf("foreign renew = %t, %v", renewed, err)
	}
	if completed, committed, err := store.CompleteTask(ctx, claim.ID, "worker-a", claim.Generation+1, core.CapabilityResult{Content: "wrong", OK: true}); err != nil || committed || completed.State != TaskClaimed {
		t.Fatalf("stale complete = %#v committed=%t err=%v", completed, committed, err)
	}
	result := core.CapabilityResult{Content: "rendered", OK: true, Metadata: map[string]any{"nested": map[string]any{"n": float64(1)}}}
	completed, committed, err := store.CompleteTask(ctx, claim.ID, "worker-a", claim.Generation, result)
	if err != nil || !committed || completed.State != TaskCompleted || completed.Result == nil {
		t.Fatalf("complete = %#v committed=%t err=%v", completed, committed, err)
	}
	completed.Result.Metadata["nested"].(map[string]any)["n"] = float64(9)
	replayed, committed, err := store.CompleteTask(ctx, claim.ID, "worker-a", claim.Generation, core.CapabilityResult{Content: "ignored", OK: true})
	if err != nil || committed || replayed.Result == nil || replayed.Result.Content != "rendered" || replayed.Result.Metadata["nested"].(map[string]any)["n"] != float64(1) {
		t.Fatalf("terminal replay = %#v committed=%t err=%v", replayed, committed, err)
	}
}

func TestMemoryStoreRecoveryRequeuesThenFencesExhaustedClaim(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	created, _, err := store.CreateTask(ctx, runnerTask(t, "recover-call", map[string]any{}, 2))
	if err != nil {
		t.Fatal(err)
	}
	first, ok, err := store.ClaimTask(ctx, claimOptions("worker-a", time.Millisecond))
	if err != nil || !ok {
		t.Fatalf("first claim: %#v %t %v", first, ok, err)
	}
	time.Sleep(2 * time.Millisecond)
	requeued, terminal, err := store.RecoverExpiredTasks(ctx, time.Now().UTC())
	if err != nil || requeued != 1 || terminal != 0 {
		t.Fatalf("first recovery: requeued=%d terminal=%d err=%v", requeued, terminal, err)
	}
	second, ok, err := store.ClaimTask(ctx, claimOptions("worker-b", time.Millisecond))
	if err != nil || !ok || second.ID != created.ID || second.Generation != 2 || second.Attempt != 2 {
		t.Fatalf("second claim: %#v %t %v", second, ok, err)
	}
	if stale, committed, err := store.CompleteTask(ctx, second.ID, "worker-a", first.Generation, core.CapabilityResult{OK: true}); err != nil || committed || stale.State != TaskClaimed {
		t.Fatalf("old generation crossed fence: %#v committed=%t err=%v", stale, committed, err)
	}
	time.Sleep(2 * time.Millisecond)
	requeued, terminal, err = store.RecoverExpiredTasks(ctx, time.Now().UTC())
	if err != nil || requeued != 0 || terminal != 1 {
		t.Fatalf("exhausted recovery: requeued=%d terminal=%d err=%v", requeued, terminal, err)
	}
	failed, err := store.GetTask(ctx, created.ID)
	if err != nil || failed.State != TaskFailed || failed.ErrorCode != "worker_lost" {
		t.Fatalf("failed task = %#v err=%v", failed, err)
	}
}

func TestHubMapsExhaustedTaskToStableDeniedResult(t *testing.T) {
	store := NewMemoryStore()
	principal, scope := runnerIdentity(t)
	ctx := context.Background()
	task, _, err := store.CreateTask(ctx, runnerTask(t, "failed-replay", map[string]any{}, 1))
	if err != nil {
		t.Fatal(err)
	}
	claim, ok, err := store.ClaimTask(ctx, claimOptions("worker-a", time.Millisecond))
	if err != nil || !ok || claim.ID != task.ID {
		t.Fatalf("claim = %#v %t %v", claim, ok, err)
	}
	time.Sleep(2 * time.Millisecond)
	if _, terminal, err := store.RecoverExpiredTasks(ctx, time.Now().UTC()); err != nil || terminal != 1 {
		t.Fatalf("recovery terminal=%d err=%v", terminal, err)
	}
	result, err := NewHubWithStore(store).SubmitScoped(ctx, principal, scope, "runner.render", "failed-replay", map[string]any{}, 1)
	if err != nil || result.OK || result.Metadata["code"] != "worker_lost" {
		t.Fatalf("failed replay = %#v err=%v", result, err)
	}
}

func TestMemoryStoreCancellationDistinguishesQueuedAndClaimed(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	queued, _, _ := store.CreateTask(ctx, runnerTask(t, "queued-cancel", map[string]any{}, 3))
	if disposition, err := store.CancelTask(ctx, queued.ID); err != nil || disposition != CancelDispositionCancelled {
		t.Fatalf("queued cancel = %q, %v", disposition, err)
	}
	cancelled, _ := store.GetTask(ctx, queued.ID)
	if cancelled.State != TaskCancelled {
		t.Fatalf("queued state = %q", cancelled.State)
	}

	claimedTask, _, _ := store.CreateTask(ctx, runnerTask(t, "claimed-cancel", map[string]any{}, 3))
	claim, ok, err := store.ClaimTask(ctx, claimOptions("worker-a", time.Hour))
	if err != nil || !ok || claim.ID != claimedTask.ID {
		t.Fatalf("claim = %#v %t %v", claim, ok, err)
	}
	if disposition, err := store.CancelTask(ctx, claim.ID); err != nil || disposition != CancelDispositionRequested {
		t.Fatalf("claimed cancel = %q, %v", disposition, err)
	}
	completed, committed, err := store.CompleteTask(ctx, claim.ID, "worker-a", claim.Generation, core.CapabilityResult{Content: "canonical", OK: true})
	if err != nil || !committed || completed.State != TaskCompleted || completed.Result.Content != "canonical" {
		t.Fatalf("cancel-requested completion = %#v committed=%t err=%v", completed, committed, err)
	}
}

func TestHubReconnectsToCompletedDurableTask(t *testing.T) {
	store := NewMemoryStore()
	principal, scope := runnerIdentity(t)
	ctx := context.Background()
	task, _, err := store.CreateTask(ctx, runnerTask(t, "reconnect-call", map[string]any{"job": "render"}, 3))
	if err != nil {
		t.Fatal(err)
	}
	claim, ok, err := store.ClaimTask(ctx, claimOptions("worker-a", time.Hour))
	if err != nil || !ok || claim.ID != task.ID {
		t.Fatalf("claim = %#v %t %v", claim, ok, err)
	}
	if _, committed, err := store.CompleteTask(ctx, claim.ID, "worker-a", claim.Generation, core.CapabilityResult{Content: "restored", OK: true}); err != nil || !committed {
		t.Fatalf("complete committed=%t err=%v", committed, err)
	}
	restarted := NewHubWithStore(store)
	result, err := restarted.SubmitScoped(ctx, principal, scope, "runner.render", "reconnect-call", map[string]any{"job": "render"}, 3)
	if err != nil || !result.OK || result.Content != "restored" {
		t.Fatalf("reconnected submit = %#v err=%v", result, err)
	}
}

func TestProviderTimeoutCancelsQueuedTaskSafely(t *testing.T) {
	hub := NewHub()
	hub.PollInterval = time.Millisecond
	provider := Provider{Hub: hub, Capability: "runner.render", Timeout: 10 * time.Millisecond}
	principal, scope := runnerIdentity(t)
	result, err := provider.Execute(context.Background(), core.CapabilityRequest{
		CallID: "call-timeout", Args: map[string]any{"job": "render"},
		Context: core.CapabilityContext{Principal: principal, Scope: scope},
	})
	if err != nil || result.OK || result.Metadata["code"] != "runner_timeout" {
		t.Fatalf("queued timeout = %#v err=%v", result, err)
	}
	if pending, err := hub.Pending(context.Background()); err != nil || pending != 0 {
		t.Fatalf("cancelled queued task remained pending: %d %v", pending, err)
	}
}

func TestProviderClaimedTimeoutReturnsOutcomeUnknown(t *testing.T) {
	store := NewMemoryStore()
	principal, scope := runnerIdentity(t)
	ctx := context.Background()
	task, _, err := store.CreateTask(ctx, runnerTask(t, "claimed-timeout", map[string]any{"job": "render"}, 3))
	if err != nil {
		t.Fatal(err)
	}
	claim, ok, err := store.ClaimTask(ctx, claimOptions("worker-a", time.Hour))
	if err != nil || !ok || claim.ID != task.ID {
		t.Fatalf("claim = %#v %t %v", claim, ok, err)
	}
	hub := NewHubWithStore(store)
	hub.PollInterval = time.Millisecond
	provider := Provider{Hub: hub, Capability: "runner.render", Timeout: 10 * time.Millisecond}
	result, err := provider.Execute(ctx, core.CapabilityRequest{
		CallID: "call-timeout", IdempotencyKey: "claimed-timeout", Args: map[string]any{"job": "render"},
		Context: core.CapabilityContext{Principal: principal, Scope: scope},
	})
	if !errors.Is(err, ErrOutcomeUnknown) || result.OK || result.Content != "" {
		t.Fatalf("claimed timeout = %#v err=%v", result, err)
	}
	after, err := store.GetTask(ctx, task.ID)
	if err != nil || after.State != TaskClaimed || !after.CancelRequested {
		t.Fatalf("claimed timeout did not request cancellation: %#v err=%v", after, err)
	}
}

func TestMemoryStoreRejectsOversizedArgsAndResult(t *testing.T) {
	store := NewMemoryStore()
	oversized := strings.Repeat("x", core.MaxToolArgumentBytes+1)
	if _, _, err := store.CreateTask(context.Background(), runnerTask(t, "too-large", map[string]any{"value": oversized}, 3)); err == nil {
		t.Fatal("oversized runner args were accepted")
	}
	task, _, err := store.CreateTask(context.Background(), runnerTask(t, "large-result", map[string]any{}, 3))
	if err != nil {
		t.Fatal(err)
	}
	claim, _, _ := store.ClaimTask(context.Background(), claimOptions("worker-a", time.Hour))
	if claim.ID != task.ID {
		t.Fatalf("claimed %s, want %s", claim.ID, task.ID)
	}
	if _, _, err := store.CompleteTask(context.Background(), claim.ID, "worker-a", claim.Generation, core.CapabilityResult{Content: strings.Repeat("x", core.HardMaxCapabilityOutputBytes+1), OK: true}); err == nil {
		t.Fatal("oversized runner result was accepted")
	}
}

func TestMemoryStoreRejectsInFlightOverflow(t *testing.T) {
	store := NewMemoryStore()
	store.maxInFlight = 2
	ctx := context.Background()
	first, _, err := store.CreateTask(ctx, runnerTask(t, "one", map[string]any{"n": 1}, 3))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.CreateTask(ctx, runnerTask(t, "two", map[string]any{"n": 2}, 3)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.CreateTask(ctx, runnerTask(t, "three", map[string]any{"n": 3}, 3)); err == nil {
		t.Fatal("in-flight overflow was accepted")
	}
	replayed, fresh, err := store.CreateTask(ctx, runnerTask(t, "one", map[string]any{"n": 1}, 3))
	if err != nil || fresh || replayed.ID != first.ID {
		t.Fatalf("idempotent replay must still work at cap: %#v fresh=%t err=%v", replayed, fresh, err)
	}
	if _, err := store.CancelTask(ctx, first.ID); err != nil {
		t.Fatal(err)
	}
	if _, fresh, err := store.CreateTask(ctx, runnerTask(t, "three", map[string]any{"n": 3}, 3)); err != nil || !fresh {
		t.Fatalf("cancelled slot was not freed: fresh=%t err=%v", fresh, err)
	}
}

func TestMemoryStoreRejectsStoredOverflow(t *testing.T) {
	store := NewMemoryStore()
	store.maxStored = 2
	ctx := context.Background()
	first, _, err := store.CreateTask(ctx, runnerTask(t, "one", map[string]any{"n": 1}, 3))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.CreateTask(ctx, runnerTask(t, "two", map[string]any{"n": 2}, 3)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.CreateTask(ctx, runnerTask(t, "three", map[string]any{"n": 3}, 3)); err == nil {
		t.Fatal("stored runner overflow was accepted")
	}
	replayed, fresh, err := store.CreateTask(ctx, runnerTask(t, "one", map[string]any{"n": 1}, 3))
	if err != nil || fresh || replayed.ID != first.ID {
		t.Fatalf("idempotent replay was blocked by the stored cap: %#v fresh=%t err=%v", replayed, fresh, err)
	}
}

func TestMemoryStoreRetryCreatesOneCleanDerivedTask(t *testing.T) {
	store := NewMemoryStore()
	original, _, err := store.CreateTask(context.Background(), runnerTask(t, "retry-source", map[string]any{"secret": "keep"}, 2))
	if err != nil {
		t.Fatal(err)
	}
	store.tasks[original.ID] = Task{ID: original.ID, Capability: original.Capability, TenantID: original.TenantID, SubjectID: original.SubjectID, Scope: original.Scope, Args: original.Args, ArgsDigest: original.ArgsDigest, TraceContext: original.TraceContext, State: TaskFailed, MaxAttempts: original.MaxAttempts, CreatedAt: original.CreatedAt, UpdatedAt: time.Now().UTC(), CompletedAt: time.Now().UTC(), ErrorCode: "worker_lost"}
	child, created, err := store.RetryTask(context.Background(), original.ID)
	if err != nil || !created || child.RetriedFromID != original.ID || child.State != TaskQueued || child.Attempt != 0 || child.Generation != 0 || child.ErrorCode != "" || child.Result != nil || child.IdempotencyKey != "" {
		t.Fatalf("retry child=%#v created=%t err=%v", child, created, err)
	}
	replayed, created, err := store.RetryTask(context.Background(), original.ID)
	if err != nil || created || replayed.ID != child.ID {
		t.Fatalf("retry replay=%#v created=%t err=%v", replayed, created, err)
	}
	if taskSummary(child).RetriedFromID != original.ID {
		t.Fatal("summary lost retry parent")
	}
}
