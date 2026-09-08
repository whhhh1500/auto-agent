package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/extensions/runner"
	"github.com/whhhh1500/auto-agent/pkg/storage"
)

type runnerAdminAuditStore struct {
	mu     sync.Mutex
	events []storage.AuditEvent
}

type runnerAdminTool struct{}

func (runnerAdminTool) Execute(context.Context, core.CapabilityRequest) (core.CapabilityResult, error) {
	return core.CapabilityResult{OK: true}, nil
}

func (s *runnerAdminAuditStore) RecordAudit(_ context.Context, event storage.AuditEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
	return nil
}

func (s *runnerAdminAuditStore) ListAudit(context.Context, storage.AuditFilter) ([]storage.AuditEvent, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]storage.AuditEvent(nil), s.events...)
	return out, len(out), nil
}

func (s *runnerAdminAuditStore) last(t *testing.T) storage.AuditEvent {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.events) == 0 {
		t.Fatal("expected an audit event")
	}
	return s.events[len(s.events)-1]
}

type runnerAdminFixture struct {
	handler   http.Handler
	server    *Server
	store     *runner.MemoryStore
	audit     *runnerAdminAuditStore
	global    core.ScopePath
	acme      core.ScopePath
	acmeUser  core.ScopePath
	other     core.ScopePath
	otherUser core.ScopePath
}

func newRunnerAdminFixture(t *testing.T) runnerAdminFixture {
	t.Helper()
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product, err := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "runner"})
	if err != nil {
		t.Fatal(err)
	}
	acme, err := product.Child(core.ScopeRef{Kind: core.ScopeTenant, ID: "acme"})
	if err != nil {
		t.Fatal(err)
	}
	acmeUser, err := acme.Child(core.ScopeRef{Kind: core.ScopeUser, ID: "alice"})
	if err != nil {
		t.Fatal(err)
	}
	other, err := product.Child(core.ScopeRef{Kind: core.ScopeTenant, ID: "other"})
	if err != nil {
		t.Fatal(err)
	}
	otherUser, err := other.Child(core.ScopeRef{Kind: core.ScopeUser, ID: "bob"})
	if err != nil {
		t.Fatal(err)
	}
	store := runner.NewMemoryStore()
	audit := &runnerAdminAuditStore{}
	api, err := New(Config{
		Runtime: &core.Runtime{}, Sessions: core.NewMemorySessionStore(),
		Runners: runner.NewHubWithStore(store), Audit: audit,
		Authenticator: AuthenticatorFunc(func(r *http.Request) (core.Principal, error) {
			switch r.Header.Get("X-Test-Identity") {
			case "platform":
				return core.Principal{SubjectID: "platform", Scope: global, Attributes: map[string]string{
					"role": storage.RoleAccountAdmin,
				}}, nil
			case "acme-admin":
				return core.Principal{SubjectID: "acme-admin", TenantID: "acme", Scope: acme, Attributes: map[string]string{
					"role": storage.RoleAccountTenantAdmin,
				}}, nil
			case "other-admin":
				return core.Principal{SubjectID: "other-admin", TenantID: "other", Scope: other, Attributes: map[string]string{
					"role": storage.RoleAccountTenantAdmin,
				}}, nil
			case "user":
				return core.Principal{SubjectID: "alice", TenantID: "acme", Scope: acmeUser, Attributes: map[string]string{
					"role": storage.RoleAccountUser,
				}}, nil
			default:
				return core.Principal{}, fmt.Errorf("unknown test identity")
			}
		}),
		RunnerAuthenticator: AuthenticatorFunc(func(*http.Request) (core.Principal, error) {
			return core.Principal{}, fmt.Errorf("runner authenticator must not service admin task routes")
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	return runnerAdminFixture{
		handler: api.Handler(), server: api, store: store, audit: audit,
		global: global, acme: acme, acmeUser: acmeUser, other: other, otherUser: otherUser,
	}
}

func TestAdminRunnerTaskRetryReturnsMetadataOnlyIdempotentResult(t *testing.T) {
	fixture := newRunnerAdminFixture(t)
	capabilities := core.NewCapabilityRegistry()
	if _, err := capabilities.Mount(core.CapabilityBinding{
		Scope:    fixture.acmeUser,
		Manifest: core.CapabilityManifest{ID: "runner.retry", Version: "v1", Name: "Runner retry", Kind: core.KindTool, Idempotent: true},
		Provider: runnerAdminTool{},
	}); err != nil {
		t.Fatal(err)
	}
	fixture.server.runtime.Capabilities = capabilities
	fixture.server.runPrincipal = RunPrincipalResolverFunc(func(_ context.Context, tenantID, subjectID string) (core.Principal, error) {
		if tenantID != "acme" || subjectID != "alice" {
			return core.Principal{}, errors.New("unexpected runner task principal")
		}
		return core.Principal{TenantID: tenantID, SubjectID: subjectID, Scope: fixture.acmeUser}, nil
	})
	original, created, err := fixture.store.CreateTask(context.Background(), runner.Task{
		ID: "rtask_retry_http", Capability: "runner.retry", TenantID: "acme", SubjectID: "alice", Scope: fixture.acmeUser.String(),
		Args: map[string]any{"secret": "never-return-this-runner-argument"}, IdempotencyKey: "never-return-this-idempotency-key", MaxAttempts: 1,
	})
	if err != nil || !created {
		t.Fatalf("create retry source=%#v created=%t err=%v", original, created, err)
	}
	claim, claimed, err := fixture.store.ClaimTask(context.Background(), runner.ClaimOptions{WorkerID: "retry-worker", LeaseTTL: time.Nanosecond})
	if err != nil || !claimed || claim.ID != original.ID {
		t.Fatalf("claim retry source=%#v claimed=%t err=%v", claim, claimed, err)
	}
	if _, terminal, err := fixture.store.RecoverExpiredTasks(context.Background(), time.Now().UTC().Add(time.Second)); err != nil || terminal != 1 {
		t.Fatalf("fail retry source terminal=%d err=%v", terminal, err)
	}

	retry := func(reason string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, "/v1/admin/runners/tasks/"+original.ID+"/retry", strings.NewReader(`{"confirm":true,"reason":"`+reason+`"}`))
		request.Header.Set("X-Test-Identity", "acme-admin")
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		fixture.handler.ServeHTTP(response, request)
		return response
	}
	first := retry("transient runner outage")
	requireRunnerAdminStatus(t, first, http.StatusOK)
	var firstResult struct {
		Original runner.TaskSummary `json:"original"`
		Retry    runner.TaskSummary `json:"retry"`
		Created  bool               `json:"created"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &firstResult); err != nil {
		t.Fatal(err)
	}
	if !firstResult.Created || firstResult.Original.ID != original.ID || firstResult.Retry.ID == "" || firstResult.Retry.RetriedFromID != original.ID {
		t.Fatalf("first retry result=%#v", firstResult)
	}
	second := retry("same explicit retry")
	requireRunnerAdminStatus(t, second, http.StatusOK)
	var secondResult struct {
		Retry   runner.TaskSummary `json:"retry"`
		Created bool               `json:"created"`
	}
	if err := json.Unmarshal(second.Body.Bytes(), &secondResult); err != nil {
		t.Fatal(err)
	}
	if secondResult.Created || secondResult.Retry.ID != firstResult.Retry.ID {
		t.Fatalf("duplicate retry result=%#v; want existing %q", secondResult, firstResult.Retry.ID)
	}
	for _, payload := range []string{first.Body.String(), second.Body.String()} {
		for _, prohibited := range []string{"never-return-this", "\"args\"", "\"result\"", "idempotency_key", "args_digest", "traceparent", "tracestate"} {
			if strings.Contains(payload, prohibited) {
				t.Fatalf("retry response leaked %q: %s", prohibited, payload)
			}
		}
	}
}

func (f runnerAdminFixture) createTask(t *testing.T, id, tenantID, subjectID string, scope core.ScopePath, capability string, createdAt time.Time) runner.Task {
	t.Helper()
	task, fresh, err := f.store.CreateTask(context.Background(), runner.Task{
		ID: id, TenantID: tenantID, SubjectID: subjectID, Scope: scope.String(), Capability: capability,
		Args:           map[string]any{"secret": "never-return-this-runner-argument"},
		IdempotencyKey: "never-return-this-idempotency-key-" + id, MaxAttempts: 3,
		CreatedAt: createdAt, AvailableAt: createdAt,
	})
	if err != nil || !fresh {
		t.Fatalf("create runner task %q = %#v fresh=%t err=%v", id, task, fresh, err)
	}
	return task
}

func runnerAdminRequest(t *testing.T, handler http.Handler, method, path, identity string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, path, nil)
	request.Header.Set("X-Test-Identity", identity)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func requireRunnerAdminStatus(t *testing.T, response *httptest.ResponseRecorder, want int) {
	t.Helper()
	if response.Code != want {
		t.Fatalf("status=%d want=%d body=%s", response.Code, want, response.Body.String())
	}
}

func decodeRunnerAdminResponse[T any](t *testing.T, response *httptest.ResponseRecorder) T {
	t.Helper()
	var value T
	if err := json.Unmarshal(response.Body.Bytes(), &value); err != nil {
		t.Fatalf("decode %s: %v", response.Body.String(), err)
	}
	return value
}

func TestAdminRunnerTaskListTenantScopeFiltersPaginationAndRedaction(t *testing.T) {
	fixture := newRunnerAdminFixture(t)
	base := time.Now().UTC().Add(-time.Minute)
	claimed := fixture.createTask(t, "rtask_acme_claimed", "acme", "alice", fixture.acmeUser, "runner.render", base)
	claim, claimedOK, err := fixture.store.ClaimTask(context.Background(), runner.ClaimOptions{WorkerID: "worker-a", LeaseTTL: time.Hour})
	if err != nil || !claimedOK || claim.ID != claimed.ID {
		t.Fatalf("claim = %#v ok=%t err=%v", claim, claimedOK, err)
	}
	root := fixture.createTask(t, "rtask_acme_root", "acme", "bob", fixture.acme, "runner.render", base.Add(time.Second))
	child := fixture.createTask(t, "rtask_acme_child", "acme", "alice", fixture.acmeUser, "runner.render", base.Add(2*time.Second))
	other := fixture.createTask(t, "rtask_other", "other", "bob", fixture.otherUser, "runner.render", base.Add(3*time.Second))

	page := runnerAdminRequest(t, fixture.handler, http.MethodGet,
		"/v1/admin/runners/tasks?state=queued,claimed&state=queued&capability=runner.render&limit=1&offset=1", "acme-admin")
	requireRunnerAdminStatus(t, page, http.StatusOK)
	listed := decodeRunnerAdminResponse[struct {
		Tasks  []runner.TaskSummary `json:"tasks"`
		Total  int                  `json:"total"`
		Limit  int                  `json:"limit"`
		Offset int                  `json:"offset"`
	}](t, page)
	if listed.Total != 3 || listed.Limit != 1 || listed.Offset != 1 || len(listed.Tasks) != 1 {
		t.Fatalf("catalog page = %#v", listed)
	}
	for _, prohibited := range []string{"\"args\"", "\"result\"", "\"idempotency_key\"", "\"args_digest\"", "\"traceparent\"", "\"tracestate\"", "never-return-this"} {
		if strings.Contains(page.Body.String(), prohibited) {
			t.Fatalf("catalog response leaked %q: %s", prohibited, page.Body.String())
		}
	}

	worker := runnerAdminRequest(t, fixture.handler, http.MethodGet, "/v1/admin/runners/tasks?worker_id=worker-a&subject_id=alice&state=claimed", "acme-admin")
	requireRunnerAdminStatus(t, worker, http.StatusOK)
	workerPage := decodeRunnerAdminResponse[struct {
		Tasks []runner.TaskSummary `json:"tasks"`
		Total int                  `json:"total"`
	}](t, worker)
	if workerPage.Total != 1 || len(workerPage.Tasks) != 1 || workerPage.Tasks[0].ID != claimed.ID {
		t.Fatalf("worker filter page = %#v", workerPage)
	}

	descendant := runnerAdminRequest(t, fixture.handler, http.MethodGet, "/v1/admin/runners/tasks?scope="+fixture.acmeUser.String(), "acme-admin")
	requireRunnerAdminStatus(t, descendant, http.StatusOK)
	descendantPage := decodeRunnerAdminResponse[struct {
		Tasks []runner.TaskSummary `json:"tasks"`
		Total int                  `json:"total"`
	}](t, descendant)
	if descendantPage.Total != 2 || len(descendantPage.Tasks) != 2 {
		t.Fatalf("descendant scope page = %#v", descendantPage)
	}
	if got := runnerAdminRequest(t, fixture.handler, http.MethodGet, "/v1/admin/runners/tasks?scope="+fixture.global.String(), "acme-admin"); got.Code != http.StatusForbidden {
		t.Fatalf("ancestor scope status=%d body=%s", got.Code, got.Body.String())
	}
	if got := runnerAdminRequest(t, fixture.handler, http.MethodGet, "/v1/admin/runners/tasks?scope="+fixture.other.String(), "acme-admin"); got.Code != http.StatusForbidden {
		t.Fatalf("sibling scope status=%d body=%s", got.Code, got.Body.String())
	}
	if got := runnerAdminRequest(t, fixture.handler, http.MethodGet, "/v1/admin/runners/tasks?tenant=other", "acme-admin"); got.Code != http.StatusForbidden {
		t.Fatalf("cross-tenant status=%d body=%s", got.Code, got.Body.String())
	}
	if got := runnerAdminRequest(t, fixture.handler, http.MethodGet, "/v1/admin/runners/tasks?scope=not-a-scope", "acme-admin"); got.Code != http.StatusBadRequest {
		t.Fatalf("invalid scope status=%d body=%s", got.Code, got.Body.String())
	}
	if got := runnerAdminRequest(t, fixture.handler, http.MethodGet, "/v1/admin/runners/tasks?state=unknown", "acme-admin"); got.Code != http.StatusBadRequest {
		t.Fatalf("invalid state status=%d body=%s", got.Code, got.Body.String())
	}
	if got := runnerAdminRequest(t, fixture.handler, http.MethodGet, "/v1/admin/runners/tasks?limit=invalid", "acme-admin"); got.Code != http.StatusBadRequest {
		t.Fatalf("invalid limit status=%d body=%s", got.Code, got.Body.String())
	}
	if got := runnerAdminRequest(t, fixture.handler, http.MethodGet, "/v1/admin/runners/tasks", "user"); got.Code != http.StatusForbidden {
		t.Fatalf("user list status=%d body=%s", got.Code, got.Body.String())
	}

	platform := runnerAdminRequest(t, fixture.handler, http.MethodGet, "/v1/admin/runners/tasks?tenant=other&scope="+fixture.other.String(), "platform")
	requireRunnerAdminStatus(t, platform, http.StatusOK)
	platformPage := decodeRunnerAdminResponse[struct {
		Tasks []runner.TaskSummary `json:"tasks"`
		Total int                  `json:"total"`
	}](t, platform)
	if platformPage.Total != 1 || len(platformPage.Tasks) != 1 || platformPage.Tasks[0].ID != other.ID {
		t.Fatalf("platform cross-tenant page = %#v", platformPage)
	}
	if root.ID == "" || child.ID == "" {
		t.Fatal("fixture task IDs must be populated")
	}
}

type runnerAdminCatalogGuardStore struct {
	*runner.MemoryStore
}

func (runnerAdminCatalogGuardStore) GetTask(context.Context, string) (runner.Task, error) {
	panic("admin runner task route must not load a full Task")
}

func TestAdminRunnerTaskDetailUsesCatalogAndHidesUnauthorizedIDs(t *testing.T) {
	fixture := newRunnerAdminFixture(t)
	store := runnerAdminCatalogGuardStore{MemoryStore: fixture.store}
	api, err := New(Config{
		Runtime: &core.Runtime{}, Sessions: core.NewMemorySessionStore(), Runners: runner.NewHubWithStore(store),
		Authenticator: AuthenticatorFunc(func(r *http.Request) (core.Principal, error) {
			switch r.Header.Get("X-Test-Identity") {
			case "platform":
				return core.Principal{SubjectID: "platform", Scope: fixture.global, Attributes: map[string]string{"role": storage.RoleAccountAdmin}}, nil
			case "acme-admin":
				return core.Principal{SubjectID: "acme-admin", TenantID: "acme", Scope: fixture.acme, Attributes: map[string]string{"role": storage.RoleAccountTenantAdmin}}, nil
			default:
				return core.Principal{}, fmt.Errorf("unknown test identity")
			}
		}),
		RunnerAuthenticator: AuthenticatorFunc(func(*http.Request) (core.Principal, error) {
			return core.Principal{}, fmt.Errorf("runner authenticator must not service admin task routes")
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	fixture.handler = api.Handler()
	base := time.Now().UTC().Add(-time.Minute)
	visible := fixture.createTask(t, "rtask_detail_visible", "acme", "alice", fixture.acmeUser, "runner.render", base)
	hidden := fixture.createTask(t, "rtask_detail_hidden", "other", "bob", fixture.otherUser, "runner.render", base.Add(time.Second))

	detail := runnerAdminRequest(t, fixture.handler, http.MethodGet, "/v1/admin/runners/tasks/"+visible.ID, "acme-admin")
	requireRunnerAdminStatus(t, detail, http.StatusOK)
	if !strings.Contains(detail.Body.String(), visible.ID) || strings.Contains(detail.Body.String(), "never-return-this") {
		t.Fatalf("detail response = %s", detail.Body.String())
	}
	for _, prohibited := range []string{"\"args\"", "\"result\"", "\"idempotency_key\"", "\"args_digest\"", "\"traceparent\"", "\"tracestate\""} {
		if strings.Contains(detail.Body.String(), prohibited) {
			t.Fatalf("detail response leaked %q: %s", prohibited, detail.Body.String())
		}
	}
	for _, path := range []string{
		"/v1/admin/runners/tasks/" + hidden.ID,
		"/v1/admin/runners/tasks/rtask_missing",
		"/v1/admin/runners/tasks/" + hidden.ID + "/cancel",
		"/v1/admin/runners/tasks/rtask_missing/cancel",
	} {
		method := http.MethodGet
		if strings.HasSuffix(path, "/cancel") {
			method = http.MethodPost
		}
		response := runnerAdminRequest(t, fixture.handler, method, path, "acme-admin")
		if response.Code != http.StatusNotFound {
			t.Fatalf("hidden/missing path %s status=%d body=%s", path, response.Code, response.Body.String())
		}
	}
}

func TestAdminRunnerTaskCancelStateMachineAndAudit(t *testing.T) {
	fixture := newRunnerAdminFixture(t)
	base := time.Now().UTC().Add(-time.Minute)
	terminal := fixture.createTask(t, "rtask_cancel_terminal", "acme", "alice", fixture.acmeUser, "runner.render", base)
	claim, claimed, err := fixture.store.ClaimTask(context.Background(), runner.ClaimOptions{WorkerID: "terminal-worker", LeaseTTL: time.Hour})
	if err != nil || !claimed || claim.ID != terminal.ID {
		t.Fatalf("claim terminal = %#v claimed=%t err=%v", claim, claimed, err)
	}
	if _, committed, err := fixture.store.CompleteTask(context.Background(), terminal.ID, "terminal-worker", claim.Generation, core.CapabilityResult{Content: "done", OK: true}); err != nil || !committed {
		t.Fatalf("complete terminal committed=%t err=%v", committed, err)
	}
	claimedTask := fixture.createTask(t, "rtask_cancel_claimed", "acme", "alice", fixture.acmeUser, "runner.render", base.Add(time.Second))
	claim, claimed, err = fixture.store.ClaimTask(context.Background(), runner.ClaimOptions{WorkerID: "claimed-worker", LeaseTTL: time.Hour})
	if err != nil || !claimed || claim.ID != claimedTask.ID {
		t.Fatalf("claim claimed task = %#v claimed=%t err=%v", claim, claimed, err)
	}
	queued := fixture.createTask(t, "rtask_cancel_queued", "acme", "alice", fixture.acmeUser, "runner.render", base.Add(2*time.Second))

	tests := []struct {
		task        runner.Task
		disposition runner.CancelDisposition
		state       runner.TaskState
		requested   bool
		previous    runner.TaskState
	}{
		{task: queued, disposition: runner.CancelDispositionCancelled, state: runner.TaskCancelled, requested: true, previous: runner.TaskQueued},
		{task: claimedTask, disposition: runner.CancelDispositionRequested, state: runner.TaskClaimed, requested: true, previous: runner.TaskClaimed},
		{task: terminal, disposition: runner.CancelDispositionTerminal, state: runner.TaskCompleted, requested: false, previous: runner.TaskCompleted},
	}
	for _, test := range tests {
		t.Run(string(test.disposition), func(t *testing.T) {
			response := runnerAdminRequest(t, fixture.handler, http.MethodPost, "/v1/admin/runners/tasks/"+test.task.ID+"/cancel", "acme-admin")
			requireRunnerAdminStatus(t, response, http.StatusOK)
			output := decodeRunnerAdminResponse[struct {
				Disposition runner.CancelDisposition `json:"disposition"`
				Task        runner.TaskSummary       `json:"task"`
			}](t, response)
			if output.Disposition != test.disposition || output.Task.State != test.state || output.Task.CancelRequested != test.requested {
				t.Fatalf("cancel output = %#v", output)
			}
			event := fixture.audit.last(t)
			if event.Action != "runner.task.cancel" || event.Target != test.task.ID || len(event.Detail) != 4 ||
				event.Detail["task_id"] != test.task.ID || event.Detail["capability"] != test.task.Capability ||
				event.Detail["previous_state"] != test.previous || event.Detail["disposition"] != test.disposition {
				t.Fatalf("cancel audit = %#v", event)
			}
			encoded, err := json.Marshal(event.Detail)
			if err != nil {
				t.Fatal(err)
			}
			for _, prohibited := range []string{"args", "result", "idempotency_key", "args_digest", "traceparent", "tracestate", "never-return-this"} {
				if strings.Contains(string(encoded), prohibited) {
					t.Fatalf("cancel audit leaked %q: %s", prohibited, encoded)
				}
			}
		})
	}
}

type runnerAdminRecoverStore struct {
	runner.Store
	requeued int64
	terminal int64
	now      time.Time
	calls    int
}

func (s *runnerAdminRecoverStore) RecoverExpiredTasks(_ context.Context, now time.Time) (int64, int64, error) {
	s.calls++
	s.now = now
	return s.requeued, s.terminal, nil
}

func TestAdminRunnerRecoveryRequiresPlatformAdminAndAuditsCounts(t *testing.T) {
	fixture := newRunnerAdminFixture(t)
	recoveryStore := &runnerAdminRecoverStore{Store: runner.NewMemoryStore(), requeued: 1}
	api, err := New(Config{
		Runtime: &core.Runtime{}, Sessions: core.NewMemorySessionStore(),
		Runners: runner.NewHubWithStore(recoveryStore), Audit: fixture.audit,
		Authenticator: AuthenticatorFunc(func(r *http.Request) (core.Principal, error) {
			switch r.Header.Get("X-Test-Identity") {
			case "platform":
				return core.Principal{SubjectID: "platform", Scope: fixture.global, Attributes: map[string]string{"role": storage.RoleAccountAdmin}}, nil
			case "acme-admin":
				return core.Principal{SubjectID: "acme-admin", TenantID: "acme", Scope: fixture.acme, Attributes: map[string]string{"role": storage.RoleAccountTenantAdmin}}, nil
			default:
				return core.Principal{}, fmt.Errorf("unknown test identity")
			}
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := runnerAdminRequest(t, api.Handler(), http.MethodPost, "/v1/admin/runners/recover", "acme-admin"); got.Code != http.StatusForbidden {
		t.Fatalf("tenant recovery status=%d body=%s", got.Code, got.Body.String())
	}
	response := runnerAdminRequest(t, api.Handler(), http.MethodPost, "/v1/admin/runners/recover", "platform")
	requireRunnerAdminStatus(t, response, http.StatusOK)
	output := decodeRunnerAdminResponse[struct {
		Requeued int64 `json:"requeued"`
		Terminal int64 `json:"terminal"`
	}](t, response)
	if output.Requeued != 1 || output.Terminal != 0 {
		t.Fatalf("recovery output = %#v", output)
	}
	if recoveryStore.calls != 1 || recoveryStore.now.IsZero() || recoveryStore.now.Location() != time.UTC {
		t.Fatalf("recovery call count=%d now=%s", recoveryStore.calls, recoveryStore.now)
	}
	event := fixture.audit.last(t)
	if event.Action != "runner.tasks.recover" || event.Detail["requeued"] != int64(1) || event.Detail["terminal"] != int64(0) || len(event.Detail) != 2 {
		t.Fatalf("recovery audit = %#v", event)
	}
}

type runnerAdminCatalogUnsupportedStore struct {
	runner.Store
}

type runnerAdminCatalogErrorStore struct {
	*runner.MemoryStore
	err error
}

func (s runnerAdminCatalogErrorStore) ListTasks(context.Context, runner.TaskQuery) ([]runner.TaskSummary, int, error) {
	return nil, 0, s.err
}

func TestAdminRunnerTaskCatalogUnsupportedAndRunnerDisabled(t *testing.T) {
	fixture := newRunnerAdminFixture(t)
	unsupportedAPI, err := New(Config{
		Runtime: &core.Runtime{}, Sessions: core.NewMemorySessionStore(),
		Runners: runner.NewHubWithStore(runnerAdminCatalogUnsupportedStore{Store: runner.NewMemoryStore()}),
		Authenticator: AuthenticatorFunc(func(r *http.Request) (core.Principal, error) {
			if r.Header.Get("X-Test-Identity") != "platform" {
				return core.Principal{}, fmt.Errorf("unknown test identity")
			}
			return core.Principal{SubjectID: "platform", Scope: fixture.global, Attributes: map[string]string{"role": storage.RoleAccountAdmin}}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, request := range []struct {
		method string
		path   string
	}{
		{method: http.MethodGet, path: "/v1/admin/runners/tasks"},
		{method: http.MethodGet, path: "/v1/admin/runners/tasks/rtask_missing"},
		{method: http.MethodPost, path: "/v1/admin/runners/tasks/rtask_missing/cancel"},
	} {
		response := runnerAdminRequest(t, unsupportedAPI.Handler(), request.method, request.path, "platform")
		if response.Code != http.StatusNotImplemented {
			t.Fatalf("unsupported %s %s status=%d body=%s", request.method, request.path, response.Code, response.Body.String())
		}
	}

	catalogErr := errors.New("runner task catalog storage failed")
	failingAPI, err := New(Config{
		Runtime: &core.Runtime{}, Sessions: core.NewMemorySessionStore(),
		Runners: runner.NewHubWithStore(runnerAdminCatalogErrorStore{MemoryStore: runner.NewMemoryStore(), err: catalogErr}),
		Authenticator: AuthenticatorFunc(func(r *http.Request) (core.Principal, error) {
			if r.Header.Get("X-Test-Identity") != "platform" {
				return core.Principal{}, fmt.Errorf("unknown test identity")
			}
			return core.Principal{SubjectID: "platform", Scope: fixture.global, Attributes: map[string]string{"role": storage.RoleAccountAdmin}}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, request := range []struct {
		method string
		path   string
	}{
		{method: http.MethodGet, path: "/v1/admin/runners/tasks"},
		{method: http.MethodGet, path: "/v1/admin/runners/tasks/rtask_missing"},
		{method: http.MethodPost, path: "/v1/admin/runners/tasks/rtask_missing/cancel"},
	} {
		response := runnerAdminRequest(t, failingAPI.Handler(), request.method, request.path, "platform")
		if response.Code != http.StatusInternalServerError {
			t.Fatalf("catalog failure %s %s status=%d body=%s", request.method, request.path, response.Code, response.Body.String())
		}
	}

	disabledAPI, err := New(Config{
		Runtime: &core.Runtime{}, Sessions: core.NewMemorySessionStore(),
		Authenticator: AuthenticatorFunc(func(r *http.Request) (core.Principal, error) {
			return core.Principal{SubjectID: "platform", Scope: fixture.global, Attributes: map[string]string{"role": storage.RoleAccountAdmin}}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	response := runnerAdminRequest(t, disabledAPI.Handler(), http.MethodPost, "/v1/admin/runners/recover", "platform")
	if response.Code != http.StatusNotImplemented {
		t.Fatalf("disabled recovery status=%d body=%s", response.Code, response.Body.String())
	}
}
