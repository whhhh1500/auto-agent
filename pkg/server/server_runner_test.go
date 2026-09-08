package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/extensions/runner"
	"github.com/whhhh1500/auto-agent/pkg/storage"
)

func newRunnerProtocolServer(t *testing.T) (*Server, *runner.Hub, *runner.MemoryStore) {
	return newRunnerProtocolServerWithRunnerAuthenticator(t, AuthenticatorFunc(func(r *http.Request) (core.Principal, error) {
		return core.Principal{SubjectID: r.Header.Get("X-Runner-ID"), Attributes: map[string]string{
			RunnerCapabilitiesAttribute: "*",
		}}, nil
	}))
}

func newRunnerProtocolServerWithRunnerAuthenticator(t *testing.T, runnerAuthenticator Authenticator) (*Server, *runner.Hub, *runner.MemoryStore) {
	t.Helper()
	store := runner.NewMemoryStore()
	hub := runner.NewHubWithStore(store)
	hub.LeaseTTL = time.Hour
	api, err := New(Config{
		Runtime: &core.Runtime{}, Sessions: core.NewMemorySessionStore(), Runners: hub,
		Authenticator: AuthenticatorFunc(func(*http.Request) (core.Principal, error) {
			return core.Principal{}, context.Canceled
		}),
		RunnerAuthenticator: runnerAuthenticator,
	})
	if err != nil {
		t.Fatal(err)
	}
	return api, hub, store
}

func TestRunnerClaimCapabilityAuthorization(t *testing.T) {
	grant := "runner.render"
	api, _, store := newRunnerProtocolServerWithRunnerAuthenticator(t, AuthenticatorFunc(func(r *http.Request) (core.Principal, error) {
		return core.Principal{SubjectID: r.Header.Get("X-Runner-ID"), Attributes: map[string]string{
			RunnerCapabilitiesAttribute: grant,
		}}, nil
	}))
	base := time.Now().UTC().Add(-time.Minute)
	analytics, _, err := store.CreateTask(context.Background(), runner.Task{
		ID: "rtask_auth_analytics", Capability: "runner.analytics", Args: map[string]any{}, MaxAttempts: 3,
		CreatedAt: base, AvailableAt: base,
	})
	if err != nil {
		t.Fatal(err)
	}
	renderer, _, err := store.CreateTask(context.Background(), runner.Task{
		ID: "rtask_auth_renderer", Capability: "runner.render", Args: map[string]any{}, MaxAttempts: 3,
		CreatedAt: base.Add(time.Second), AvailableAt: base.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	handler := api.Handler()

	response := runnerRequest(t, handler, http.MethodPost, "/v1/runners/claim", "renderer-worker", nil)
	if response.Code != http.StatusOK {
		t.Fatalf("restricted claim status=%d body=%s", response.Code, response.Body.String())
	}
	var claim runnerClaimResponse
	if err := json.Unmarshal(response.Body.Bytes(), &claim); err != nil {
		t.Fatal(err)
	}
	if claim.ID != renderer.ID || claim.Capability != "runner.render" {
		t.Fatalf("restricted claim=%#v, want renderer task", claim)
	}
	queuedAnalytics, err := store.GetTask(context.Background(), analytics.ID)
	if err != nil || queuedAnalytics.State != runner.TaskQueued || queuedAnalytics.Attempt != 0 || queuedAnalytics.Generation != 0 {
		t.Fatalf("analytics changed by restricted claim: %#v err=%v", queuedAnalytics, err)
	}

	rendererSubset, _, err := store.CreateTask(context.Background(), runner.Task{
		ID: "rtask_auth_renderer_subset", Capability: "runner.render", Args: map[string]any{}, MaxAttempts: 3,
		CreatedAt: base.Add(2 * time.Second), AvailableAt: base.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	response = runnerRequest(t, handler, http.MethodPost, "/v1/runners/claim", "renderer-worker", map[string]any{
		"capabilities": []string{" runner.render ", "runner.render"},
	})
	if response.Code != http.StatusOK {
		t.Fatalf("subset claim status=%d body=%s", response.Code, response.Body.String())
	}
	if err := json.Unmarshal(response.Body.Bytes(), &claim); err != nil || claim.ID != rendererSubset.ID {
		t.Fatalf("subset claim=%#v err=%v", claim, err)
	}

	response = runnerRequest(t, handler, http.MethodPost, "/v1/runners/claim", "renderer-worker", map[string]any{
		"capabilities": []string{"runner.analytics"},
	})
	if response.Code != http.StatusForbidden {
		t.Fatalf("overreach claim status=%d body=%s", response.Code, response.Body.String())
	}
	queuedAnalytics, err = store.GetTask(context.Background(), analytics.ID)
	if err != nil || queuedAnalytics.State != runner.TaskQueued || queuedAnalytics.Attempt != 0 || queuedAnalytics.Generation != 0 {
		t.Fatalf("analytics changed by rejected overreach: %#v err=%v", queuedAnalytics, err)
	}

	response = runnerRequest(t, handler, http.MethodPost, "/v1/runners/claim", "renderer-worker", map[string]any{
		"capabilities": []string{"*"},
	})
	if response.Code != http.StatusBadRequest {
		t.Fatalf("wildcard request status=%d body=%s", response.Code, response.Body.String())
	}
	response = runnerRequest(t, handler, http.MethodPost, "/v1/runners/claim", "renderer-worker", map[string]any{
		"capabilities": []string{"not-namespaced"},
	})
	if response.Code != http.StatusBadRequest {
		t.Fatalf("invalid request capability status=%d body=%s", response.Code, response.Body.String())
	}
	overLimit := make([]string, runner.MaxClaimCapabilities+1)
	for index := range overLimit {
		overLimit[index] = "runner.render"
	}
	response = runnerRequest(t, handler, http.MethodPost, "/v1/runners/claim", "renderer-worker", map[string]any{
		"capabilities": overLimit,
	})
	if response.Code != http.StatusBadRequest {
		t.Fatalf("over-limit request capability status=%d body=%s", response.Code, response.Body.String())
	}
	response = runnerRequest(t, handler, http.MethodPost, "/v1/runners/claim", "renderer-worker", map[string]any{"unknown": true})
	if response.Code != http.StatusNoContent {
		t.Fatalf("forward-compatible claim status=%d body=%s", response.Code, response.Body.String())
	}

	grant = "runner.analytics"
	renew := runnerRequest(t, handler, http.MethodPost, "/v1/runners/tasks/"+renderer.ID+"/renew", "renderer-worker", map[string]any{"generation": 1})
	if renew.Code != http.StatusOK {
		t.Fatalf("renew after grant change status=%d body=%s", renew.Code, renew.Body.String())
	}
	complete := runnerRequest(t, handler, http.MethodPost, "/v1/runners/tasks/"+renderer.ID+"/complete", "renderer-worker", map[string]any{
		"generation": 1, "content": "canonical", "ok": true,
	})
	if complete.Code != http.StatusOK {
		t.Fatalf("complete after grant change status=%d body=%s", complete.Code, complete.Body.String())
	}
}

func TestRunnerClaimRejectsMissingOrInvalidDedicatedGrant(t *testing.T) {
	for _, test := range []struct {
		name       string
		attributes map[string]string
	}{
		{name: "missing"},
		{name: "mixed wildcard", attributes: map[string]string{RunnerCapabilitiesAttribute: "runner.render,*"}},
		{name: "invalid identifier", attributes: map[string]string{RunnerCapabilitiesAttribute: "not-namespaced"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			api, _, store := newRunnerProtocolServerWithRunnerAuthenticator(t, AuthenticatorFunc(func(r *http.Request) (core.Principal, error) {
				return core.Principal{SubjectID: r.Header.Get("X-Runner-ID"), Attributes: test.attributes}, nil
			}))
			task, _, err := store.CreateTask(context.Background(), runner.Task{ID: "rtask_auth_rejected", Capability: "runner.render", Args: map[string]any{}, MaxAttempts: 3})
			if err != nil {
				t.Fatal(err)
			}
			response := runnerRequest(t, api.Handler(), http.MethodPost, "/v1/runners/claim", "worker", nil)
			if response.Code != http.StatusForbidden {
				t.Fatalf("claim status=%d body=%s", response.Code, response.Body.String())
			}
			stored, err := store.GetTask(context.Background(), task.ID)
			if err != nil || stored.State != runner.TaskQueued || stored.Attempt != 0 || stored.Generation != 0 {
				t.Fatalf("rejected claim changed task: %#v err=%v", stored, err)
			}
		})
	}
}

func TestRunnerClaimWildcardAndAdminFallbackAuthorizeAll(t *testing.T) {
	wildcard, _, wildcardStore := newRunnerProtocolServer(t)
	first, _, err := wildcardStore.CreateTask(context.Background(), runner.Task{ID: "rtask_auth_wildcard", Capability: "runner.analytics", Args: map[string]any{}, MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	response := runnerRequest(t, wildcard.Handler(), http.MethodPost, "/v1/runners/claim", "all-worker", nil)
	if response.Code != http.StatusOK || !bytes.Contains(response.Body.Bytes(), []byte(first.ID)) {
		t.Fatalf("wildcard claim status=%d body=%s", response.Code, response.Body.String())
	}

	adminStore := runner.NewMemoryStore()
	adminHub := runner.NewHubWithStore(adminStore)
	adminAPI, err := New(Config{
		Runtime: &core.Runtime{}, Sessions: core.NewMemorySessionStore(), Runners: adminHub,
		Authenticator: AuthenticatorFunc(func(*http.Request) (core.Principal, error) {
			return core.Principal{SubjectID: "admin", Attributes: map[string]string{"role": storage.RoleAccountAdmin}}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	adminTask, _, err := adminStore.CreateTask(context.Background(), runner.Task{ID: "rtask_auth_admin", Capability: "runner.render", Args: map[string]any{}, MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	response = runnerRequest(t, adminAPI.Handler(), http.MethodPost, "/v1/runners/claim", "ignored", map[string]any{
		"capabilities": []string{"runner.render"},
	})
	if response.Code != http.StatusOK || !bytes.Contains(response.Body.Bytes(), []byte(adminTask.ID)) {
		t.Fatalf("admin fallback claim status=%d body=%s", response.Code, response.Body.String())
	}
}

func runnerRequest(t *testing.T, handler http.Handler, method, path, worker string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var payload bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&payload).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	request := httptest.NewRequest(method, path, &payload)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Runner-ID", worker)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func TestRunnerHTTPProtocolFencesClaimRenewAndComplete(t *testing.T) {
	api, _, store := newRunnerProtocolServer(t)
	created, fresh, err := store.CreateTask(context.Background(), runner.Task{
		Capability: "runner.render", Args: map[string]any{"job": "render"}, MaxAttempts: 3,
	})
	if err != nil || !fresh {
		t.Fatalf("create = %#v fresh=%t err=%v", created, fresh, err)
	}
	handler := api.Handler()

	claimedResponse := runnerRequest(t, handler, http.MethodPost, "/v1/runners/claim", "worker-a", nil)
	if claimedResponse.Code != http.StatusOK {
		t.Fatalf("claim status=%d body=%s", claimedResponse.Code, claimedResponse.Body.String())
	}
	var claim runnerClaimResponse
	if err := json.Unmarshal(claimedResponse.Body.Bytes(), &claim); err != nil {
		t.Fatal(err)
	}
	if claim.ID != created.ID || claim.Generation != 1 || claim.Attempt != 1 || claim.Capability != "runner.render" {
		t.Fatalf("claim payload = %#v", claim)
	}
	if claim.TraceContext != nil || bytes.Contains(claimedResponse.Body.Bytes(), []byte(`"trace_context"`)) {
		t.Fatalf("carrierless task claim unexpectedly included trace context: %s", claimedResponse.Body.String())
	}
	for _, leaked := range []string{"tenant_id", "subject_id", "scope", "worker_id", "args_digest", "state"} {
		if bytes.Contains(claimedResponse.Body.Bytes(), []byte(`"`+leaked+`"`)) {
			t.Fatalf("claim response leaked internal field %q: %s", leaked, claimedResponse.Body.String())
		}
	}

	foreignRenew := runnerRequest(t, handler, http.MethodPost, "/v1/runners/tasks/"+claim.ID+"/renew", "worker-b", map[string]any{"generation": claim.Generation})
	if foreignRenew.Code != http.StatusConflict {
		t.Fatalf("foreign renew status=%d body=%s", foreignRenew.Code, foreignRenew.Body.String())
	}
	if disposition, err := store.CancelTask(context.Background(), claim.ID); err != nil || disposition != runner.CancelDispositionRequested {
		t.Fatalf("cancel request = %q, %v", disposition, err)
	}
	renew := runnerRequest(t, handler, http.MethodPost, "/v1/runners/tasks/"+claim.ID+"/renew", "worker-a", map[string]any{"generation": claim.Generation})
	if renew.Code != http.StatusOK || !bytes.Contains(renew.Body.Bytes(), []byte(`"cancel_requested":true`)) {
		t.Fatalf("renew status=%d body=%s", renew.Code, renew.Body.String())
	}

	foreignComplete := runnerRequest(t, handler, http.MethodPost, "/v1/runners/tasks/"+claim.ID+"/complete", "worker-b", map[string]any{
		"generation": claim.Generation, "content": "wrong", "ok": true,
	})
	if foreignComplete.Code != http.StatusConflict {
		t.Fatalf("foreign complete status=%d body=%s", foreignComplete.Code, foreignComplete.Body.String())
	}
	complete := runnerRequest(t, handler, http.MethodPost, "/v1/runners/tasks/"+claim.ID+"/complete", "worker-a", map[string]any{
		"generation": claim.Generation, "content": "canonical", "ok": true,
	})
	if complete.Code != http.StatusOK {
		t.Fatalf("complete status=%d body=%s", complete.Code, complete.Body.String())
	}
	replay := runnerRequest(t, handler, http.MethodPost, "/v1/runners/tasks/"+claim.ID+"/complete", "worker-a", map[string]any{
		"generation": claim.Generation, "content": "ignored", "ok": true,
	})
	if replay.Code != http.StatusOK {
		t.Fatalf("completion replay status=%d body=%s", replay.Code, replay.Body.String())
	}
	stored, err := store.GetTask(context.Background(), claim.ID)
	if err != nil || stored.Result == nil || stored.Result.Content != "canonical" {
		t.Fatalf("canonical completion = %#v err=%v", stored, err)
	}

	empty := runnerRequest(t, handler, http.MethodPost, "/v1/runners/claim", "worker-a", map[string]any{})
	if empty.Code != http.StatusNoContent {
		t.Fatalf("empty-object claim status=%d body=%s", empty.Code, empty.Body.String())
	}
}

func TestRunnerHTTPProtocolRequiresStableWorkerIdentityAndGeneration(t *testing.T) {
	api, _, store := newRunnerProtocolServer(t)
	task, _, err := store.CreateTask(context.Background(), runner.Task{Capability: "runner.render", Args: map[string]any{}, MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	handler := api.Handler()
	missingIdentity := runnerRequest(t, handler, http.MethodPost, "/v1/runners/claim", "", nil)
	if missingIdentity.Code != http.StatusUnauthorized {
		t.Fatalf("missing identity status=%d body=%s", missingIdentity.Code, missingIdentity.Body.String())
	}
	claim := runnerRequest(t, handler, http.MethodPost, "/v1/runners/claim", "worker-a", nil)
	if claim.Code != http.StatusOK {
		t.Fatalf("claim status=%d body=%s", claim.Code, claim.Body.String())
	}
	zeroGeneration := runnerRequest(t, handler, http.MethodPost, "/v1/runners/tasks/"+task.ID+"/complete", "worker-a", map[string]any{"generation": 0, "ok": true})
	if zeroGeneration.Code != http.StatusBadRequest {
		t.Fatalf("zero generation status=%d body=%s", zeroGeneration.Code, zeroGeneration.Body.String())
	}
}
