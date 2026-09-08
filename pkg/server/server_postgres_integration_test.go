package server

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/whhhh1500/auto-agent/internal/testdb"
	"github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/provider/openai"
	"github.com/whhhh1500/auto-agent/pkg/storage"
)

type documentApprovalTool struct{ calls *atomic.Int32 }

type observedAcceptanceModel struct {
	inner        core.LlmAdapter
	calls        atomic.Int32
	inputTokens  atomic.Int64
	outputTokens atomic.Int64
}

func (model *observedAcceptanceModel) Provider() string { return model.inner.Provider() }
func (model *observedAcceptanceModel) Stream(ctx context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	model.calls.Add(1)
	return model.inner.Stream(ctx, options, func(chunk core.StreamChunk) {
		if chunk.Usage != nil {
			model.inputTokens.Add(chunk.Usage.InputTokens)
			model.outputTokens.Add(chunk.Usage.OutputTokens)
		}
		emit(chunk)
	})
}

func (documentApprovalTool) Manifest() core.CapabilityManifest {
	return core.CapabilityManifest{ID: "document.approve", Version: "1", Name: "Accept document", Kind: core.KindTool, Contract: "harness.tool/v1", RequiresApproval: true,
		RequiredPermissions: []core.Permission{core.PermWrite}, Tool: &core.ToolExposure{Parameters: map[string]any{"type": "object", "properties": map[string]any{"document_id": map[string]any{"type": "string", "enum": []any{"doc-acceptance"}}}, "required": []any{"document_id"}, "additionalProperties": false}}}
}
func (tool documentApprovalTool) Execute(context.Context, core.CapabilityRequest) (core.CapabilityResult, error) {
	tool.calls.Add(1)
	return core.CapabilityResult{Content: `{"accepted":true,"document_id":"doc-acceptance"}`, OK: true}, nil
}

type postgresAPI struct {
	server    *Server
	http      *httptest.Server
	db        *sql.DB
	sessions  *storage.SQLSessionStore
	queue     *storage.SQLRunControlStore
	approvals *storage.SQLApprovalStore
}

func newPostgresAPI(t *testing.T, db *sql.DB, model core.LlmAdapter, calls *atomic.Int32) *postgresAPI {
	t.Helper()
	ctx := context.Background()
	sessions, err := storage.OpenSQLSessionStore(ctx, db, storage.SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	queue, err := storage.NewSQLRunControlStore(db, storage.SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	approvals, err := storage.NewSQLApprovalStore(db, storage.SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := storage.NewSQLToolInvocationJournal(db, storage.SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	product := core.MustScopePath(core.ScopeRef{Kind: core.ScopeProduct, ID: "acceptance"})
	tenant, _ := product.Child(core.ScopeRef{Kind: core.ScopeTenant, ID: "acceptance-tenant"})
	user, _ := tenant.Child(core.ScopeRef{Kind: core.ScopeUser, ID: "acceptance-user"})
	principal := core.Principal{TenantID: "acceptance-tenant", SubjectID: "acceptance-user", Scope: user, Grants: core.NewPermissionSet(core.PermRead, core.PermWrite), Attributes: map[string]string{"role": storage.RoleAccountTenantAdmin}}
	capabilities := core.NewCapabilityRegistry()
	if err := capabilities.Register(product, documentApprovalTool{calls: calls}); err != nil {
		t.Fatal(err)
	}
	profiles := core.NewAgentProfileRegistry()
	name := "Document acceptance"
	selection := core.ModelSelection{Provider: model.Provider(), Model: "acceptance-model"}
	if configured := os.Getenv("HARNESS_LLM_MODEL"); configured != "" {
		selection.Model = configured
	}
	steps, toolLimit := 4, 1
	if err := profiles.Bind(core.AgentProfileLayer{Scope: product, ProfileID: "acceptance.agent", Name: &name, Model: &selection, MaxSteps: &steps, MaxToolCalls: &toolLimit,
		AddCapabilities: []string{"document.approve"}, PutFragments: []core.PromptFragment{{ID: "acceptance.instructions", Section: core.PromptInstructions,
			Content: "For each user request call document.approve exactly once with document_id doc-acceptance. Do not answer before the tool result. The runtime manages human approval. After the tool returns, briefly confirm its result without calling another tool."}}}); err != nil {
		t.Fatal(err)
	}
	runtime := &core.Runtime{Capabilities: capabilities, Profiles: profiles, Approver: approvals, ToolJournal: journal,
		Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) { return model, nil })}
	srv, err := New(Config{Runtime: runtime, Sessions: sessions, Authenticator: AuthenticatorFunc(func(*http.Request) (core.Principal, error) { return principal, nil }),
		DefaultProfileID: "acceptance.agent", Leaser: sessions, LeaseTTL: 30 * time.Second,
		RunControl: queue, RunQueue: queue, Approvals: approvals,
		RunPrincipalResolver: RunPrincipalResolverFunc(func(context.Context, string, string) (core.Principal, error) { return principal, nil }),
		RunWorkerConcurrency: 2, RunWorkerPollInterval: 10 * time.Millisecond, RunWorkerClaimTTL: 30 * time.Second, MaxWriteDelay: -1})
	if err != nil {
		t.Fatal(err)
	}
	api := &postgresAPI{server: srv, http: httptest.NewServer(srv.Handler()), db: db, sessions: sessions, queue: queue, approvals: approvals}
	t.Cleanup(func() { api.close(t) })
	return api
}

func (api *postgresAPI) close(t *testing.T) {
	t.Helper()
	api.http.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := api.server.Shutdown(ctx); err != nil {
		t.Errorf("shutdown: %v", err)
	}
	_ = api.db.Close()
}

func acceptanceRequest(client *http.Client, method, url string, body any, result any) error {
	encoded, err := json.Marshal(body)
	if err != nil {
		return err
	}
	request, err := http.NewRequest(method, url, bytes.NewReader(encoded))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("HTTP %s returned %d", method, response.StatusCode)
	}
	if result == nil {
		_, err = io.Copy(io.Discard, response.Body)
		return err
	}
	return json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(result)
}

func submitDocument(client *http.Client, base string) (string, storage.RunRecord, error) {
	var session struct {
		ID string `json:"id"`
	}
	if err := acceptanceRequest(client, http.MethodPost, base+"/v1/sessions", map[string]any{"profile_id": "acceptance.agent"}, &session); err != nil {
		return "", storage.RunRecord{}, err
	}
	var run storage.RunRecord
	err := acceptanceRequest(client, http.MethodPost, base+"/v1/sessions/"+session.ID+"/runs/async", map[string]any{"message": "Please accept document doc-acceptance through the required tool, then confirm the result."}, &run)
	return session.ID, run, err
}

// localProtocolModel is a real loopback HTTP protocol peer with deterministic
// output. It measures orchestration overhead without billing an external model.
func localProtocolModel(t *testing.T) core.LlmAdapter {
	t.Helper()
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Messages []struct {
				Role      string `json:"role"`
				ToolCalls []struct {
					ExtraContent struct {
						Google struct {
							Signature string `json:"thought_signature"`
						} `json:"google"`
					} `json:"extra_content"`
				} `json:"tool_calls"`
			} `json:"messages"`
		}
		if r.URL.Path != "/v1/chat/completions" || json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&request) != nil {
			http.Error(w, "invalid model request", 400)
			return
		}
		message := map[string]any{"role": "assistant", "content": "Document doc-acceptance accepted."}
		finish := "stop"
		if len(request.Messages) != 0 && request.Messages[len(request.Messages)-1].Role == "tool" {
			preserved := false
			for _, message := range request.Messages {
				for _, call := range message.ToolCalls {
					if call.ExtraContent.Google.Signature == "opaque-fixture-signature" {
						preserved = true
					}
				}
			}
			if !preserved {
				http.Error(w, "tool continuation was lost", http.StatusBadRequest)
				return
			}
		}
		if len(request.Messages) != 0 && request.Messages[len(request.Messages)-1].Role != "tool" {
			finish = "tool_calls"
			message = map[string]any{"role": "assistant", "content": nil, "tool_calls": []any{map[string]any{"id": "accept-document", "type": "function", "extra_content": map[string]any{"google": map[string]any{"thought_signature": "opaque-fixture-signature"}}, "function": map[string]any{"name": "document.approve", "arguments": `{"document_id":"doc-acceptance"}`}}}}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"id": "acceptance-response", "object": "chat.completion", "model": "acceptance-model", "choices": []any{map[string]any{"index": 0, "message": message, "finish_reason": finish}}, "usage": map[string]any{"prompt_tokens": 20, "completion_tokens": 10, "total_tokens": 30}})
	}))
	t.Cleanup(peer.Close)
	return openai.NewOpenAICompatibleAdapter(openai.OpenAIAdapterConfig{BaseURL: peer.URL + "/v1", APIKey: "local-test-fixture", Model: "acceptance-model", MaxTokens: 512})
}

func TestPostgresHTTPApprovalResumesOnReplacementInstance(t *testing.T) {
	verifyPostgresApprovalResume(t, localProtocolModel(t))
}

func TestLiveModelHTTPApprovalResumesOnReplacementInstance(t *testing.T) {
	if os.Getenv("HARNESS_ACCEPTANCE_LIVE_MODEL") != "1" {
		t.Skip("explicit live-model acceptance is not enabled")
	}
	if os.Getenv("HARNESS_TEST_PG_DSN") == "" {
		t.Fatal("live acceptance requires PostgreSQL")
	}
	model, err := openai.NewOpenAIAdapterFromEnv()
	if err != nil {
		t.Fatal("live model configuration is incomplete or invalid")
	}
	verifyPostgresApprovalResume(t, model)
}

func verifyPostgresApprovalResume(t *testing.T, model core.LlmAdapter) {
	t.Helper()
	observed := &observedAcceptanceModel{inner: model}
	model = observed
	open := testdb.Postgres(t)
	var calls atomic.Int32
	first := newPostgresAPI(t, open(), model, &calls)
	client := &http.Client{Timeout: 20 * time.Second}
	sessionID, run, err := submitDocument(client, first.http.URL)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	if claimed, err := first.server.RunWorkerOnce(ctx, "first-instance"); err != nil || !claimed {
		t.Fatalf("first claim=%t err=%v", claimed, err)
	}
	paused, err := first.queue.GetRun(ctx, run.RunID)
	if err != nil || paused.Status != storage.RunStatusWaitingApproval || calls.Load() != 0 {
		t.Fatalf("pause status=%s calls=%d err=%v", paused.Status, calls.Load(), err)
	}
	approvals, err := first.approvals.ListApprovals(ctx, storage.ApprovalFilter{TenantID: "acceptance-tenant", Status: core.ApprovalPending})
	if err != nil || len(approvals) != 1 {
		t.Fatalf("pending approvals=%d err=%v", len(approvals), err)
	}
	first.close(t)
	second := newPostgresAPI(t, open(), model, &calls)
	if err := acceptanceRequest(client, http.MethodPost, second.http.URL+"/v1/admin/approvals/"+approvals[0].ID+"/decision", map[string]any{"decision": "approved"}, nil); err != nil {
		t.Fatal(err)
	}
	if claimed, err := second.server.RunWorkerOnce(ctx, "replacement-instance"); err != nil || !claimed {
		t.Fatalf("resume claim=%t err=%v", claimed, err)
	}
	terminal, err := second.queue.GetRun(ctx, run.RunID)
	if err != nil || terminal.Status != string(core.RunCompleted) || calls.Load() != 1 {
		if session, loadErr := second.sessions.Load(ctx, sessionID); loadErr == nil {
			for _, event := range session.Events() {
				if event.Type == core.EvRunError || event.Type == core.EvStepError {
					diagnostic := string(event.Data)
					if secret := os.Getenv("HARNESS_LLM_API_KEY"); secret != "" {
						diagnostic = strings.ReplaceAll(diagnostic, secret, "[REDACTED]")
					}
					t.Logf("run failure evidence: %s", diagnostic)
				}
			}
		}
		t.Fatalf("terminal=%s calls=%d err=%v", terminal.Status, calls.Load(), err)
	}
	loaded, err := second.sessions.Load(ctx, sessionID)
	if err != nil {
		t.Fatal(err)
	}
	starts, resumes, ends, toolResults, assistantMessages := 0, 0, 0, 0, 0
	var usage core.RunUsageData
	var answer string
	for _, event := range loaded.Events() {
		if event.RunID != run.RunID {
			continue
		}
		switch event.Type {
		case core.EvRunStart:
			starts++
		case core.EvRunResume:
			resumes++
		case core.EvRunEnd:
			ends++
		case core.EvToolResult:
			toolResults++
		case core.EvAssistantMessage:
			assistantMessages++
			var data core.AssistantMessageData
			if json.Unmarshal(event.Data, &data) == nil && data.Text != "" {
				answer = data.Text
			}
		case core.EvRunUsage:
			_ = json.Unmarshal(event.Data, &usage)
		}
	}
	if starts != 1 || resumes != 1 || ends != 1 || toolResults != 1 || assistantMessages < 2 || strings.TrimSpace(answer) == "" || observed.calls.Load() != 2 {
		t.Fatalf("events start=%d resume=%d end=%d tool=%d assistant=%d", starts, resumes, ends, toolResults, assistantMessages)
	}
	if err := acceptanceRequest(client, http.MethodPost, second.http.URL+"/v1/admin/approvals/"+approvals[0].ID+"/decision", map[string]any{"decision": "approved"}, nil); err != nil {
		t.Fatal(err)
	}
	if claimed, err := second.server.RunWorkerOnce(ctx, "duplicate-check"); err != nil || claimed || calls.Load() != 1 {
		t.Fatalf("duplicate claim=%t calls=%d err=%v", claimed, calls.Load(), err)
	}
	t.Logf("HTTP session=%s run=%s: same run resumed on replacement instance; one tool effect; one terminal event; model_calls=%d model_input_tokens=%d model_output_tokens=%d last_segment_usage=%+v", sessionID, run.RunID, observed.calls.Load(), observed.inputTokens.Load(), observed.outputTokens.Load(), usage)
	t.Logf("final assistant response: %.512s", answer)
}

func TestPostgresHTTPConcurrentApprovalWorkload(t *testing.T) {
	open := testdb.Postgres(t)
	model := localProtocolModel(t)
	var calls atomic.Int32
	first := newPostgresAPI(t, open(), model, &calls)
	second := newPostgresAPI(t, open(), model, &calls)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	for _, api := range []*postgresAPI{first, second} {
		if err := api.server.StartRunWorkers(ctx); err != nil {
			t.Fatal(err)
		}
	}
	clients := acceptanceInt(t, "HARNESS_ACCEPTANCE_CLIENTS", 8, 128)
	operations := acceptanceInt(t, "HARNESS_ACCEPTANCE_RUNS", 32, 4096)
	client := &http.Client{Timeout: 10 * time.Second}
	jobs := make(chan int, operations)
	for index := 0; index < operations; index++ {
		jobs <- index
	}
	close(jobs)
	var mu sync.Mutex
	var latencies []time.Duration
	var failures []error
	var wg sync.WaitGroup
	started := time.Now()
	for worker := 0; worker < clients; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for range jobs {
				begin := time.Now()
				api := []*postgresAPI{first, second}[worker%2]
				_, run, err := submitDocument(client, api.http.URL)
				if err == nil {
					err = completeDocument(ctx, client, api, run)
				}
				mu.Lock()
				if err != nil {
					failures = append(failures, err)
				} else {
					latencies = append(latencies, time.Since(begin))
				}
				mu.Unlock()
			}
		}(worker)
	}
	wg.Wait()
	elapsed := time.Since(started)
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	if len(failures) != 0 || len(latencies) != operations || int(calls.Load()) != operations {
		t.Fatalf("completed=%d errors=%d calls=%d first_errors=%v", len(latencies), len(failures), calls.Load(), failures)
	}
	t.Logf("bounded workload: model=local-http-protocol clients=%d instances=2 worker_slots=4 runs=%d errors=0 elapsed=%s runs_per_second=%.2f p50=%s p95=%s p99=%s", clients, operations, elapsed, float64(operations)/elapsed.Seconds(), latencies[(len(latencies)*50+99)/100-1], latencies[(len(latencies)*95+99)/100-1], latencies[(len(latencies)*99+99)/100-1])
}

func acceptanceInt(t *testing.T, name string, fallback, maximum int) int {
	t.Helper()
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 1 || value > maximum {
		t.Fatalf("%s must be between 1 and %d", name, maximum)
	}
	return value
}

func completeDocument(ctx context.Context, client *http.Client, api *postgresAPI, run storage.RunRecord) error {
	ticker := time.NewTicker(15 * time.Millisecond)
	defer ticker.Stop()
	decided := false
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
		current, err := api.queue.GetRun(ctx, run.RunID)
		if err != nil {
			return err
		}
		if current.Status == string(core.RunCompleted) {
			return nil
		}
		if current.Status == storage.RunStatusWaitingApproval && !decided {
			pending, err := api.approvals.ListApprovals(ctx, storage.ApprovalFilter{TenantID: "acceptance-tenant", Status: core.ApprovalPending})
			if err != nil {
				return err
			}
			for _, approval := range pending {
				if approval.RunID == run.RunID {
					if err := acceptanceRequest(client, http.MethodPost, api.http.URL+"/v1/admin/approvals/"+approval.ID+"/decision", map[string]any{"decision": "approved"}, nil); err != nil {
						return err
					}
					decided = true
				}
			}
		}
		if current.CompletedAt.IsZero() {
			continue
		}
		return fmt.Errorf("run terminated as %s", strings.TrimSpace(current.Status))
	}
}
