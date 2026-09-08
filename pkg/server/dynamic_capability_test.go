package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/extensions/runner"
	"github.com/whhhh1500/auto-agent/pkg/extensions/subagent"
	"github.com/whhhh1500/auto-agent/pkg/storage"

	_ "modernc.org/sqlite"
)

type dynamicBindingJournal struct{ records []storage.BindingRecord }

type dynamicRunnerTraceTelemetry struct {
	trace core.TelemetryTraceContext
}

type dynamicRunnerTraceSpan struct{}

func (dynamicRunnerTraceSpan) End(error, core.TelemetryAttributes) {}

func (t dynamicRunnerTraceTelemetry) Start(ctx context.Context, _ string, _ core.TelemetryAttributes) (context.Context, core.TelemetrySpan) {
	return ctx, dynamicRunnerTraceSpan{}
}

func (dynamicRunnerTraceTelemetry) AddCounter(context.Context, string, int64, core.TelemetryAttributes) {
}

func (dynamicRunnerTraceTelemetry) RecordHistogram(context.Context, string, float64, string, core.TelemetryAttributes) {
}

func (dynamicRunnerTraceTelemetry) SetGauge(context.Context, string, float64, string, core.TelemetryAttributes) {
}

func (t dynamicRunnerTraceTelemetry) InjectTraceContext(context.Context) core.TelemetryTraceContext {
	return t.trace
}

func (dynamicRunnerTraceTelemetry) ExtractTraceContext(ctx context.Context, _ core.TelemetryTraceContext) context.Context {
	return ctx
}

func (j *dynamicBindingJournal) Record(_ context.Context, record storage.BindingRecord) error {
	record.Payload = append([]byte(nil), record.Payload...)
	j.records = append(j.records, record)
	return nil
}

func (j *dynamicBindingJournal) Delete(_ context.Context, id string) error {
	for i, record := range j.records {
		if record.ID == id {
			j.records = append(j.records[:i], j.records[i+1:]...)
			return nil
		}
	}
	return nil
}

func (j *dynamicBindingJournal) List(context.Context) ([]storage.BindingRecord, error) {
	return append([]storage.BindingRecord(nil), j.records...), nil
}

type dynamicBindingAudit struct{ events []storage.AuditEvent }

// dynamicSubagentModel delegates only when the composed profile exposes the
// dynamic capability. The child profile exposes no tools, so it completes
// directly and keeps this an adapter-level end-to-end test.
type dynamicSubagentModel struct{ childSessionID string }

func (dynamicSubagentModel) Provider() string { return "dynamic-subagent-test" }

func (m *dynamicSubagentModel) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	for _, message := range options.Messages {
		if message.Role == core.RoleTool {
			emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: "parent complete"})
			emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
			return nil
		}
	}
	if len(options.Tools) > 0 {
		args := map[string]any{"prompt": "delegate"}
		if m.childSessionID != "" {
			args = map[string]any{"session_id": m.childSessionID, "followup": "continue"}
		}
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &core.ToolCall{ID: "call_delegate", Name: "agent.delegate", Args: args}})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
		return nil
	}
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: "child complete"})
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
	return nil
}

func (a *dynamicBindingAudit) RecordAudit(_ context.Context, event storage.AuditEvent) error {
	a.events = append(a.events, event)
	return nil
}

func (a *dynamicBindingAudit) ListAudit(context.Context, storage.AuditFilter) ([]storage.AuditEvent, int, error) {
	return append([]storage.AuditEvent(nil), a.events...), len(a.events), nil
}

func newDynamicBindingServer(t *testing.T, hub *runner.Hub) (*Server, core.ScopePath, *dynamicBindingJournal, *dynamicBindingAudit) {
	t.Helper()
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	journal := &dynamicBindingJournal{}
	audit := &dynamicBindingAudit{}
	api, err := New(Config{
		Runtime:  &core.Runtime{Capabilities: core.NewCapabilityRegistry()},
		Sessions: core.NewMemorySessionStore(), Runners: hub,
		Authenticator: AuthenticatorFunc(func(*http.Request) (core.Principal, error) {
			return core.Principal{
				SubjectID: "operator", TenantID: "acme", Scope: global, Grants: core.NewPermissionSet(),
			}, nil
		}),
		RunnerAuthenticator: AuthenticatorFunc(func(r *http.Request) (core.Principal, error) {
			return core.Principal{SubjectID: r.Header.Get("X-Runner-ID"), Attributes: map[string]string{
				RunnerCapabilitiesAttribute: "runner.render",
			}}, nil
		}),
		BindingJournal: journal, Audit: audit,
	})
	if err != nil {
		t.Fatal(err)
	}
	return api, global, journal, audit
}

func dynamicCapabilityPayload(scope core.ScopePath, id string, execution map[string]any) map[string]any {
	return map[string]any{
		"scope": scope.Segments(),
		"manifest": map[string]any{
			"id": id, "version": "1.0.0", "name": id, "kind": "connector",
		},
		"execution": execution,
	}
}

func dynamicBindingRequest(t *testing.T, handler http.Handler, body any) *httptest.ResponseRecorder {
	t.Helper()
	var encoded bytes.Buffer
	if err := json.NewEncoder(&encoded).Encode(body); err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/admin/capabilities/bind", &encoded)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func resolvedDynamicSnapshot(t *testing.T, api *Server, global core.ScopePath) *core.CapabilitySnapshot {
	t.Helper()
	snapshot, err := (core.CapabilityResolver{Registry: api.runtime.Capabilities}).Resolve(core.Principal{
		SubjectID: "operator", TenantID: "acme", Scope: global, Grants: core.NewPermissionSet(),
	}, global)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func TestDynamicCapabilityBindHTTPDefaultsRuntime(t *testing.T) {
	api, global, journal, _ := newDynamicBindingServer(t, nil)
	response := dynamicBindingRequest(t, api.Handler(), dynamicCapabilityPayload(global, "remote.ping", map[string]any{
		"entrypoint": "https://93.184.216.34/ping", "method": "POST",
	}))
	if response.Code != http.StatusCreated {
		t.Fatalf("bind status=%d body=%s", response.Code, response.Body.String())
	}
	manifest, ok := resolvedDynamicSnapshot(t, api, global).ManifestFor("remote.ping")
	if !ok || manifest.Execution == nil || manifest.Execution.Runtime != "http" || manifest.Execution.Entrypoint != "https://93.184.216.34/ping" {
		t.Fatalf("HTTP binding execution = %#v, found=%t", manifest.Execution, ok)
	}
	if len(journal.records) != 1 || !strings.Contains(string(journal.records[0].Payload), `"runtime":"http"`) {
		t.Fatalf("HTTP journal did not preserve default runtime: %#v", journal.records)
	}
}

func TestDynamicCapabilityBindPreservesCompleteManifestAndRejectsInvalid(t *testing.T) {
	api, global, journal, _ := newDynamicBindingServer(t, nil)
	request := dynamicCapabilityPayload(global, "remote.complete", map[string]any{
		"runtime": "http", "entrypoint": "https://93.184.216.34/complete", "method": "POST",
		"headers": map[string]string{"Accept": "application/json"},
	})
	request["manifest"] = map[string]any{
		"id": "remote.complete", "version": "2.3.4", "name": "Complete manifest",
		"description": "retained metadata", "kind": "connector", "contract": "tool/v2",
		"required_permissions": []string{"data.read"}, "required_credentials": []string{"provider.token"},
		"input_schema":     map[string]any{"type": "object", "required": []string{"query"}},
		"output_schema":    map[string]any{"type": "object", "properties": map[string]any{"answer": map[string]string{"type": "string"}}},
		"max_output_bytes": 4096, "per_turn_budget": 3, "timeout_ms": 2500,
		"idempotent": true, "requires_approval": true,
		"tool":     map[string]any{"description": "search", "parameters": map[string]any{"type": "object"}},
		"metadata": map[string]string{"owner": "quality", "revision": "r1"},
	}
	response := dynamicBindingRequest(t, api.Handler(), request)
	if response.Code != http.StatusCreated {
		t.Fatalf("complete manifest bind status=%d body=%s", response.Code, response.Body.String())
	}
	entries, err := api.runtime.Capabilities.Entries(global)
	if err != nil {
		t.Fatal(err)
	}
	var manifest core.CapabilityManifest
	var ok bool
	for _, entry := range entries {
		if entry.Manifest.ID == "remote.complete" {
			manifest, ok = entry.Manifest, true
			break
		}
	}
	if !ok || manifest.Version != "2.3.4" || manifest.Description != "retained metadata" ||
		manifest.Contract != "tool/v2" || len(manifest.RequiredPermissions) != 1 ||
		manifest.RequiredPermissions[0] != "data.read" || len(manifest.RequiredCredentials) != 1 ||
		manifest.RequiredCredentials[0] != "provider.token" || manifest.MaxOutputBytes != 4096 ||
		manifest.PerTurnBudget != 3 || manifest.TimeoutMs != 2500 ||
		!manifest.Idempotent || !manifest.RequiresApproval || manifest.Tool == nil ||
		manifest.InputSchema["type"] != "object" || manifest.OutputSchema["type"] != "object" ||
		manifest.Execution == nil || manifest.Execution.Runtime != "http" ||
		manifest.Execution.Entrypoint != "https://93.184.216.34/complete" ||
		manifest.Execution.Method != "POST" || manifest.Metadata["revision"] != "r1" {
		t.Fatalf("complete manifest was not retained: %#v", manifest)
	}
	if len(journal.records) != 1 {
		t.Fatalf("complete manifest journal records=%d", len(journal.records))
	}

	invalid := dynamicCapabilityPayload(global, "invalid manifest", map[string]any{
		"runtime": "http", "entrypoint": "https://93.184.216.34/invalid",
	})
	invalidResponse := dynamicBindingRequest(t, api.Handler(), invalid)
	if invalidResponse.Code != http.StatusBadRequest {
		t.Fatalf("invalid manifest status=%d body=%s", invalidResponse.Code, invalidResponse.Body.String())
	}
	if len(journal.records) != 1 {
		t.Fatalf("invalid manifest created a journal binding: %#v", journal.records)
	}
	if _, ok := resolvedDynamicSnapshot(t, api, global).ManifestFor("invalid manifest"); ok {
		t.Fatal("invalid manifest created a live binding")
	}
}

func TestDynamicCapabilityBindRunnerRuntimeJournalAndAudit(t *testing.T) {
	hub := runner.NewHub()
	api, global, journal, audit := newDynamicBindingServer(t, hub)
	response := dynamicBindingRequest(t, api.Handler(), dynamicCapabilityPayload(global, "runner.render", map[string]any{
		"runtime": "runner",
	}))
	if response.Code != http.StatusCreated {
		t.Fatalf("bind status=%d body=%s", response.Code, response.Body.String())
	}
	snapshot := resolvedDynamicSnapshot(t, api, global)
	manifest, ok := snapshot.ManifestFor("runner.render")
	if !ok || manifest.Execution == nil || manifest.Execution.Runtime != "runner" || manifest.Execution.Entrypoint != "" {
		t.Fatalf("runner binding execution = %#v, found=%t", manifest.Execution, ok)
	}
	if !snapshot.Authorized("runner.render") {
		t.Fatal("runner binding is not tool-executable")
	}
	wantDigest := sha256.Sum256([]byte((runner.Provider{}).ArtifactRevision()))
	wantRevision := "sha256:" + hex.EncodeToString(wantDigest[:])
	for _, capability := range snapshot.Capabilities() {
		if capability.Manifest.ID == "runner.render" && capability.ProviderRevision != wantRevision {
			t.Fatalf("runner provider revision=%q want %q", capability.ProviderRevision, wantRevision)
		}
	}
	if len(journal.records) != 1 {
		t.Fatalf("journal records=%d", len(journal.records))
	}
	payload := string(journal.records[0].Payload)
	if !strings.Contains(payload, `"runtime":"runner"`) {
		t.Fatalf("runner journal omits runtime: %s", payload)
	}
	for _, forbidden := range []string{`"entrypoint"`, `"method"`, `"headers"`} {
		if strings.Contains(payload, forbidden) {
			t.Fatalf("runner journal contains HTTP field %s: %s", forbidden, payload)
		}
	}
	if len(audit.events) != 1 || audit.events[0].Detail["runtime"] != "runner" {
		t.Fatalf("runner audit detail=%#v", audit.events)
	}
}

func TestDynamicCapabilityBindRunnerValidation(t *testing.T) {
	missingAPI, global, _, _ := newDynamicBindingServer(t, nil)
	missing := dynamicBindingRequest(t, missingAPI.Handler(), dynamicCapabilityPayload(global, "runner.missing", map[string]any{
		"runtime": "runner",
	}))
	if missing.Code != http.StatusNotImplemented {
		t.Fatalf("runner without hub status=%d body=%s", missing.Code, missing.Body.String())
	}

	api, global, _, _ := newDynamicBindingServer(t, runner.NewHub())
	for _, test := range []struct {
		name      string
		execution map[string]any
	}{
		{name: "unknown runtime", execution: map[string]any{"runtime": "wasm"}},
		{name: "entrypoint", execution: map[string]any{"runtime": "runner", "entrypoint": "https://93.184.216.34/ping"}},
		{name: "method", execution: map[string]any{"runtime": "runner", "method": "POST"}},
		{name: "headers", execution: map[string]any{"runtime": "runner", "headers": map[string]string{"Accept": "application/json"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := dynamicBindingRequest(t, api.Handler(), dynamicCapabilityPayload(global, "runner."+strings.ReplaceAll(test.name, " ", "."), test.execution))
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
		})
	}
}

func newDynamicSubagentServer(t *testing.T, sessions core.SessionStore, links subagent.DelegationLinkStore, journal storage.BindingJournal, model *dynamicSubagentModel) (*Server, core.ScopePath) {
	t.Helper()
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	name, parentName := "child", "parent"
	selection := core.ModelSelection{Provider: "dynamic-subagent-test", Model: "deterministic"}
	profiles := core.NewAgentProfileRegistry()
	if err := profiles.Bind(core.AgentProfileLayer{Scope: global, ProfileID: "product.child", Name: &name, Model: &selection}); err != nil {
		t.Fatal(err)
	}
	if err := profiles.Bind(core.AgentProfileLayer{Scope: global, ProfileID: "product.parent", Name: &parentName, Model: &selection, AddCapabilities: []string{"agent.delegate"}}); err != nil {
		t.Fatal(err)
	}
	runtime := &core.Runtime{
		Capabilities: core.NewCapabilityRegistry(), Profiles: profiles,
		Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) {
			return model, nil
		}),
	}
	api, err := New(Config{
		Runtime: runtime, Sessions: sessions, DelegationLinks: links, BindingJournal: journal,
		Authenticator: AuthenticatorFunc(func(*http.Request) (core.Principal, error) {
			return core.Principal{SubjectID: "operator", TenantID: "acme", Scope: global, Grants: core.NewPermissionSet(core.PermRead)}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	return api, global
}

func TestDynamicCapabilityBindSubagentDurablyRestoresAndContinues(t *testing.T) {
	sessions := core.NewMemorySessionStore()
	links := subagent.NewMemoryDelegationLinkStore()
	journal := &dynamicBindingJournal{}
	model := &dynamicSubagentModel{}
	first, global := newDynamicSubagentServer(t, sessions, links, journal, model)
	request := dynamicCapabilityPayload(global, "agent.delegate", map[string]any{
		"runtime": "subagent", "entrypoint": "product.child",
	})
	request["manifest"] = map[string]any{
		"id": "agent.delegate", "version": "1.0.0", "name": "delegate", "kind": "agent",
		"metadata": map[string]string{"subagent.max_depth": "2", "subagent.max_children": "4", "subagent.max_tool_calls": "3"},
	}
	response := dynamicBindingRequest(t, first.Handler(), request)
	if response.Code != http.StatusCreated {
		t.Fatalf("subagent bind status=%d body=%s", response.Code, response.Body.String())
	}
	if len(journal.records) != 1 || !strings.Contains(string(journal.records[0].Payload), `"entrypoint":"product.child"`) {
		t.Fatalf("subagent binding was not durably journaled: %#v", journal.records)
	}
	created := httptest.NewRecorder()
	createRequest := httptest.NewRequest(http.MethodPost, "/v1/sessions", strings.NewReader(`{"profile_id":"product.parent"}`))
	createRequest.Header.Set("Content-Type", "application/json")
	first.Handler().ServeHTTP(created, createRequest)
	if created.Code != http.StatusCreated {
		t.Fatalf("parent session status=%d body=%s", created.Code, created.Body.String())
	}
	var parentSession struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &parentSession); err != nil || parentSession.ID == "" {
		t.Fatalf("parent session=%s err=%v", created.Body.String(), err)
	}
	run := httptest.NewRecorder()
	runRequest := httptest.NewRequest(http.MethodPost, "/v1/sessions/"+parentSession.ID+"/runs", strings.NewReader(`{"message":"delegate"}`))
	runRequest.Header.Set("Content-Type", "application/json")
	first.Handler().ServeHTTP(run, runRequest)
	if run.Code != http.StatusOK {
		t.Fatalf("parent run status=%d body=%s", run.Code, run.Body.String())
	}
	linked, err := links.List(context.Background(), subagent.DelegationLinkFilter{ParentSessionID: parentSession.ID, TenantID: "acme", Limit: 10})
	if err != nil || len(linked) != 1 || linked[0].ChildSessionID == "" {
		t.Fatalf("parent delegation catalog=%#v err=%v", linked, err)
	}
	childSessionID := linked[0].ChildSessionID

	restarted, _ := newDynamicSubagentServer(t, sessions, links, journal, model)
	if err := restarted.RestoreBindings(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Simulate an at-least-once replay of the same guarded parent call after
	// restart. A fresh in-memory parent object gives Runtime the original
	// session/run identity without appending a duplicate run-start event.
	persistedParent, err := sessions.Load(context.Background(), parentSession.ID)
	if err != nil {
		t.Fatal(err)
	}
	replayParent, err := core.NewSession(core.SessionOptions{
		ID: persistedParent.ID(), ProfileID: persistedParent.ProfileID(), Principal: persistedParent.Principal(), Scope: persistedParent.Scope(),
	})
	if err != nil {
		t.Fatal(err)
	}
	replayResult, err := restarted.runtime.RunTurn(context.Background(), persistedParent.Principal(), replayParent, core.TurnInput{
		RunID: linked[0].ParentRunID, Text: "replay must reuse the child",
	}, nil)
	if err != nil || replayResult.Status != core.RunCompleted {
		t.Fatalf("restart parent replay=%#v err=%v", replayResult, err)
	}
	replayedLinks, err := links.List(context.Background(), subagent.DelegationLinkFilter{ParentSessionID: parentSession.ID, TenantID: "acme", Limit: 10})
	if err != nil || len(replayedLinks) != 1 || replayedLinks[0].ChildSessionID != childSessionID {
		t.Fatalf("restart replay created a new child: %#v err=%v", replayedLinks, err)
	}
	model.childSessionID = childSessionID
	continued := httptest.NewRecorder()
	continueRequest := httptest.NewRequest(http.MethodPost, "/v1/sessions/"+parentSession.ID+"/runs", strings.NewReader(`{"message":"continue"}`))
	continueRequest.Header.Set("Content-Type", "application/json")
	restarted.Handler().ServeHTTP(continued, continueRequest)
	if continued.Code != http.StatusOK {
		t.Fatalf("restart continuation status=%d body=%s", continued.Code, continued.Body.String())
	}
	linkedAfter, err := links.List(context.Background(), subagent.DelegationLinkFilter{ChildSessionID: childSessionID, TenantID: "acme", Limit: 10})
	if err != nil || len(linkedAfter) != 1 || linkedAfter[0].ChildSessionID != childSessionID {
		t.Fatalf("restart continuation changed child: %#v err=%v", linkedAfter, err)
	}
}

func TestDynamicCapabilityBindSubagentValidationFailsClosed(t *testing.T) {
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	missing, _ := newDynamicSubagentServer(t, core.NewMemorySessionStore(), nil, nil, &dynamicSubagentModel{})
	request := dynamicCapabilityPayload(global, "agent.delegate", map[string]any{"runtime": "subagent", "entrypoint": "product.child"})
	request["manifest"] = map[string]any{"id": "agent.delegate", "version": "1.0.0", "name": "delegate", "kind": "agent"}
	if response := dynamicBindingRequest(t, missing.Handler(), request); response.Code != http.StatusNotImplemented {
		t.Fatalf("missing durable stores status=%d body=%s", response.Code, response.Body.String())
	}

	api, _ := newDynamicSubagentServer(t, core.NewMemorySessionStore(), subagent.NewMemoryDelegationLinkStore(), nil, &dynamicSubagentModel{})
	for _, test := range []struct {
		name     string
		manifest map[string]any
		exec     map[string]any
	}{
		{name: "non-agent kind", manifest: map[string]any{"kind": "tool"}, exec: map[string]any{"runtime": "subagent", "entrypoint": "product.child"}},
		{name: "invalid profile", manifest: map[string]any{"kind": "agent"}, exec: map[string]any{"runtime": "subagent", "entrypoint": "child"}},
		{name: "http fields", manifest: map[string]any{"kind": "agent"}, exec: map[string]any{"runtime": "subagent", "entrypoint": "product.child", "method": "POST"}},
		{name: "workdir", manifest: map[string]any{"kind": "agent"}, exec: map[string]any{"runtime": "subagent", "entrypoint": "product.child", "workdir": "/tmp"}},
		{name: "writes", manifest: map[string]any{"kind": "agent"}, exec: map[string]any{"runtime": "subagent", "entrypoint": "product.child", "writes": true}},
		{name: "sandbox", manifest: map[string]any{"kind": "agent"}, exec: map[string]any{"runtime": "subagent", "entrypoint": "product.child", "sandbox": map[string]string{"Mode": "workspace-write"}}},
		{name: "manifest execution", manifest: map[string]any{"kind": "agent", "execution": map[string]any{"runtime": "http", "entrypoint": "https://example.test"}}, exec: map[string]any{"runtime": "subagent", "entrypoint": "product.child"}},
		{name: "unknown metadata", manifest: map[string]any{"kind": "agent", "metadata": map[string]string{"subagent.unknown": "1"}}, exec: map[string]any{"runtime": "subagent", "entrypoint": "product.child"}},
		{name: "invalid limit", manifest: map[string]any{"kind": "agent", "metadata": map[string]string{"subagent.max_depth": "2.0"}}, exec: map[string]any{"runtime": "subagent", "entrypoint": "product.child"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			payload := dynamicCapabilityPayload(global, "agent."+strings.ReplaceAll(test.name, " ", "."), test.exec)
			payload["manifest"] = map[string]any{"id": "agent." + strings.ReplaceAll(test.name, " ", "."), "version": "1.0.0", "name": test.name, "kind": "agent"}
			for key, value := range test.manifest {
				payload["manifest"].(map[string]any)[key] = value
			}
			if response := dynamicBindingRequest(t, api.Handler(), payload); response.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
		})
	}
}

func dynamicBindingHTTPPost(t *testing.T, endpoint, workerID string, body any) *http.Response {
	t.Helper()
	var encoded bytes.Buffer
	if err := json.NewEncoder(&encoded).Encode(body); err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, endpoint, &encoded)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	if workerID != "" {
		request.Header.Set("X-Runner-ID", workerID)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func TestDynamicRunnerBindingExecutesThroughRunnerHTTPProtocol(t *testing.T) {
	wantTraceContext := core.TelemetryTraceContext{
		TraceParent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		TraceState:  "runner=dynamic",
	}
	hub := runner.NewHub()
	hub.Telemetry = dynamicRunnerTraceTelemetry{trace: wantTraceContext}
	api, global, _, _ := newDynamicBindingServer(t, hub)
	httpServer := httptest.NewServer(api.Handler())
	defer httpServer.Close()

	bound := dynamicBindingHTTPPost(t, httpServer.URL+"/v1/admin/capabilities/bind", "", dynamicCapabilityPayload(global, "runner.render", map[string]any{
		"runtime": "runner",
	}))
	if bound.StatusCode != http.StatusCreated {
		bound.Body.Close()
		t.Fatalf("bind status=%d", bound.StatusCode)
	}
	bound.Body.Close()

	snapshot := resolvedDynamicSnapshot(t, api, global)
	callContext, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	type executionOutcome struct {
		result core.CapabilityResult
		err    error
	}
	completed := make(chan executionOutcome, 1)
	go func() {
		result, err := snapshot.Execute(callContext, core.ToolCall{
			ID: "call-runner-render", Name: "runner.render", Args: map[string]any{"job": "render"},
		})
		completed <- executionOutcome{result: result, err: err}
	}()

	var claim runnerClaimResponse
	claimed := false
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		response := dynamicBindingHTTPPost(t, httpServer.URL+"/v1/runners/claim", "worker-1", map[string]any{})
		if response.StatusCode == http.StatusNoContent {
			response.Body.Close()
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if response.StatusCode != http.StatusOK {
			response.Body.Close()
			t.Fatalf("claim status=%d", response.StatusCode)
		}
		if err := json.NewDecoder(response.Body).Decode(&claim); err != nil {
			response.Body.Close()
			t.Fatal(err)
		}
		response.Body.Close()
		claimed = true
		break
	}
	if !claimed || claim.ID == "" || claim.Generation == 0 || claim.Capability != "runner.render" {
		t.Fatalf("runner claim=%#v claimed=%t", claim, claimed)
	}
	if claim.TraceContext == nil || *claim.TraceContext != wantTraceContext {
		t.Fatalf("runner claim trace context=%#v, want %#v", claim.TraceContext, wantTraceContext)
	}
	storedTask, err := hub.Task(context.Background(), claim.ID)
	if err != nil || storedTask.TraceContext != wantTraceContext {
		t.Fatalf("dynamic runner task trace context=%#v err=%v", storedTask.TraceContext, err)
	}

	delivered := dynamicBindingHTTPPost(t, httpServer.URL+"/v1/runners/tasks/"+claim.ID+"/complete", "worker-1", map[string]any{
		"generation": claim.Generation, "content": "canonical result", "ok": true,
	})
	if delivered.StatusCode != http.StatusOK {
		delivered.Body.Close()
		t.Fatalf("complete status=%d", delivered.StatusCode)
	}
	delivered.Body.Close()

	select {
	case outcome := <-completed:
		if outcome.err != nil || !outcome.result.OK || outcome.result.Content != "canonical result" {
			t.Fatalf("runner execution result=%#v err=%v", outcome.result, outcome.err)
		}
	case <-time.After(time.Second):
		t.Fatal("runner-backed capability did not receive the canonical completion")
	}
}

func TestDynamicRunnerBindingAndTaskSurviveServerRestartWithSQL(t *testing.T) {
	db, err := sql.Open("sqlite", t.TempDir()+"/runner-binding.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	sessions, err := storage.OpenSQLSessionStore(ctx, db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := storage.NewSQLBindingJournal(db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := storage.NewSQLRunnerStore(db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	wantTraceContext := core.TelemetryTraceContext{
		TraceParent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		TraceState:  "vendor=sql-restart",
	}
	newServer := func() *Server {
		hub := runner.NewHubWithStore(tasks)
		api, err := New(Config{
			Runtime: &core.Runtime{Capabilities: core.NewCapabilityRegistry()}, Sessions: sessions,
			Runners: hub, BindingJournal: journal,
			Authenticator: AuthenticatorFunc(func(*http.Request) (core.Principal, error) {
				return core.Principal{SubjectID: "operator", TenantID: "acme", Scope: global}, nil
			}),
			RunnerAuthenticator: AuthenticatorFunc(func(r *http.Request) (core.Principal, error) {
				return core.Principal{SubjectID: r.Header.Get("X-Runner-ID"), Attributes: map[string]string{
					RunnerCapabilitiesAttribute: "runner.persisted",
				}}, nil
			}),
		})
		if err != nil {
			t.Fatal(err)
		}
		return api
	}

	first := newServer()
	first.runners.Telemetry = dynamicRunnerTraceTelemetry{trace: wantTraceContext}
	bound := dynamicBindingRequest(t, first.Handler(), dynamicCapabilityPayload(global, "runner.persisted", map[string]any{"runtime": "runner"}))
	if bound.Code != http.StatusCreated {
		t.Fatalf("initial bind status=%d body=%s", bound.Code, bound.Body.String())
	}

	// Submit through Server A first so both the task and its W3C carrier are
	// committed to SQL before Server B restores the binding and claims it.
	snapshot := resolvedDynamicSnapshot(t, first, global)
	callCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	type executionOutcome struct {
		result core.CapabilityResult
		err    error
	}
	outcome := make(chan executionOutcome, 1)
	go func() {
		result, err := snapshot.Execute(callCtx, core.ToolCall{
			ID: "call-persisted-runner", Name: "runner.persisted", Args: map[string]any{"job": "persist"},
		})
		outcome <- executionOutcome{result: result, err: err}
	}()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		pending, err := tasks.PendingTasks(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if pending == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if pending, err := tasks.PendingTasks(ctx); err != nil || pending != 1 {
		t.Fatalf("SQL runner task was not durably queued: pending=%d err=%v", pending, err)
	}

	// Recreate the Server and Hub over the same SQL stores. Restoring the
	// binding proves provider selection survives, while the new Hub claims the
	// task created by Server A.
	restarted := newServer()
	if err := restarted.RestoreBindings(ctx); err != nil {
		t.Fatal(err)
	}
	if !resolvedDynamicSnapshot(t, restarted, global).Authorized("runner.persisted") {
		t.Fatal("restarted server did not restore the runner binding")
	}

	var claim runner.Task
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		claimed, ok, err := restarted.runners.Claim(ctx, runner.ClaimOptions{
			WorkerID: "worker-restart", Capabilities: []string{"runner.persisted"}, LeaseTTL: time.Minute,
		})
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			claim = claimed
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if claim.ID == "" || claim.Capability != "runner.persisted" || claim.TraceContext != wantTraceContext {
		t.Fatalf("persisted runner task was not claimable: %#v", claim)
	}
	if _, committed, err := restarted.runners.Complete(ctx, claim.ID, "worker-restart", claim.Generation,
		core.CapabilityResult{Content: "survived restart", OK: true}); err != nil || !committed {
		t.Fatalf("complete committed=%t err=%v", committed, err)
	}
	select {
	case completed := <-outcome:
		if completed.err != nil || !completed.result.OK || completed.result.Content != "survived restart" {
			t.Fatalf("restarted execution=%#v err=%v", completed.result, completed.err)
		}
	case <-time.After(time.Second):
		t.Fatal("restarted runner binding did not observe SQL completion")
	}
}
