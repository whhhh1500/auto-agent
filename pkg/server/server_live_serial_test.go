package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cc-auto-agent/harness-core/internal/testdb"
	"github.com/cc-auto-agent/harness-core/pkg/app/contextassembly"
	"github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/extensions/memory"
	"github.com/cc-auto-agent/harness-core/pkg/extensions/rag"
	"github.com/cc-auto-agent/harness-core/pkg/extensions/subagent"
	"github.com/cc-auto-agent/harness-core/pkg/extensions/workflow"
	"github.com/cc-auto-agent/harness-core/pkg/provider/openai"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
)

// This suite is deliberately opt-in, serial, and bounded. It never enables
// background workers or a RetryLlmAdapter. A failing case stops the suite.
// Only synthetic prompts/results are logged; credentials and raw upstream
// errors are never included in acceptance output.
func TestLiveModelSerialModuleAcceptance(t *testing.T) {
	if os.Getenv("HARNESS_ACCEPTANCE_LIVE_SERIAL") != "1" {
		t.Skip("explicit serial live-model acceptance is not enabled")
	}
	if os.Getenv("HARNESS_TEST_PG_DSN") == "" {
		t.Fatal("serial acceptance requires an isolated PostgreSQL database")
	}
	inner, err := openai.NewOpenAIAdapterFromEnv()
	if err != nil {
		t.Fatal("live model configuration is incomplete or invalid")
	}
	model := &serialAcceptanceModel{inner: inner}
	for _, tc := range []struct {
		name string
		run  func(*testing.T, *serialAcceptanceModel)
	}{
		{"conversation_restore", serialConversationRestore},
		{"memory_across_sessions", serialMemoryAcrossSessions},
		{"rag_scope_filter", serialRAGScopeFilter},
		{"protected_workflow", serialProtectedWorkflow},
		{"durable_subagent", serialDurableSubagent},
		{"tool_hook_denial", serialToolHookDenial},
		{"trace_history_restore", serialTraceHistoryRestore},
		{"tool_budget_history", serialToolBudgetHistory},
		{"wasm_computation", serialWASMComputation},
	} {
		if !t.Run(tc.name, func(t *testing.T) {
			model.test = t
			before := model.calls
			started := time.Now()
			t.Cleanup(func() {
				t.Logf("serial case=%s calls=%d peak_llm_inflight=%d elapsed_ms=%d failed=%t", tc.name, model.calls-before, model.peak, time.Since(started).Milliseconds(), t.Failed())
			})
			tc.run(t, model)
		}) {
			return
		}
	}
	if model.peak != 1 {
		t.Fatalf("expected exactly one maximum in-flight model request, got %d", model.peak)
	}
	t.Logf("serial suite model=%s calls=%d peak_llm_inflight=%d input_tokens=%d output_tokens=%d automatic_retries=0", os.Getenv("HARNESS_LLM_MODEL"), model.calls, model.peak, model.input, model.output)
}

type serialAcceptanceModel struct {
	inner                    core.LlmAdapter
	test                     *testing.T
	mu                       sync.Mutex
	last                     time.Time
	calls                    int
	peak                     int
	inflight                 int
	input, output            int64
	contextWindow, maxOutput int
	observations             []serialModelObservation
}

type serialModelObservation struct {
	InputTokens, OutputTokens int64
	Messages, Tools           int
}

func (m *serialAcceptanceModel) ModelContextLimits() (int, int) {
	if m.contextWindow > 0 {
		return m.contextWindow, m.maxOutput
	}
	if reporter, ok := m.inner.(interface{ ModelContextLimits() (int, int) }); ok {
		return reporter.ModelContextLimits()
	}
	return 0, 0 // core supplies conservative legacy defaults
}

func (m *serialAcceptanceModel) Provider() string { return m.inner.Provider() }
func (m *serialAcceptanceModel) Stream(ctx context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.calls >= 24 {
		return fmt.Errorf("serial acceptance model-call budget exhausted")
	}
	if wait := time.Until(m.last.Add(time.Second)); wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	m.calls++
	m.inflight++
	if m.inflight > m.peak {
		m.peak = m.inflight
	}
	started := time.Now()
	var input, output int64
	toolNames := make([]string, 0, len(options.Tools))
	for _, tool := range options.Tools {
		toolNames = append(toolNames, tool.Name)
	}
	m.test.Logf("serial model_call=%d started exposed_tools=%v", m.calls, toolNames)
	err := m.inner.Stream(ctx, options, func(chunk core.StreamChunk) {
		if chunk.Usage != nil {
			input += chunk.Usage.InputTokens
			output += chunk.Usage.OutputTokens
		}
		emit(chunk)
	})
	m.inflight--
	m.last = time.Now()
	m.input += input
	m.output += output
	m.observations = append(m.observations, serialModelObservation{InputTokens: input, OutputTokens: output, Messages: len(options.Messages), Tools: len(options.Tools)})
	m.test.Logf("serial model_call=%d elapsed_ms=%d input_tokens=%d output_tokens=%d failed=%t", m.calls, time.Since(started).Milliseconds(), input, output, err != nil)
	if err != nil {
		// Do not leak URL credentials or upstream response bodies into a log.
		return fmt.Errorf("serial model request %d failed; upstream error suppressed", m.calls)
	}
	return nil
}

func serialScopes() (core.ScopePath, core.ScopePath, core.ScopePath) {
	product := core.MustScopePath(core.ScopeRef{Kind: core.ScopeProduct, ID: "acceptance"})
	tenant, _ := product.Child(core.ScopeRef{Kind: core.ScopeTenant, ID: "acceptance-tenant"})
	user, _ := tenant.Child(core.ScopeRef{Kind: core.ScopeUser, ID: "acceptance-user"})
	return product, tenant, user
}

func serialProfile(t *testing.T, api *serialAuditedAPI, id, instructions string, caps ...string) {
	t.Helper()
	product, _, _ := serialScopes()
	steps, tools := 6, 8
	selection := core.ModelSelection{Provider: "openai-compatible", Model: os.Getenv("HARNESS_LLM_MODEL")}
	if err := api.server.runtime.Profiles.Bind(core.AgentProfileLayer{Scope: product, ProfileID: id, Model: &selection, MaxSteps: &steps, MaxToolCalls: &tools,
		AddCapabilities: caps, PutFragments: []core.PromptFragment{{ID: id + ".instructions", Section: core.PromptInstructions, Content: instructions}}}); err != nil {
		t.Fatal(err)
	}
	assembler, err := contextassembly.NewAssembler(contextassembly.Config{})
	if err != nil {
		t.Fatal(err)
	}
	api.server.runtime.ContextAssembler = assembler.AssembleModelContext
	api.server.runtime.Compactor = core.RecentTurnsCompactor{MaxMessages: 120, MaxToolResultChars: 4000}
	api.server.runtime.StreamChunks = true
}

func serialRegister(t *testing.T, api *serialAuditedAPI, caps ...core.Capability) {
	t.Helper()
	product, _, _ := serialScopes()
	for _, capability := range caps {
		if err := api.server.runtime.Capabilities.Register(product, capability); err != nil {
			t.Fatal(err)
		}
	}
}

func serialSession(t *testing.T, api *serialAuditedAPI, profile string) string {
	t.Helper()
	var result struct {
		ID string `json:"id"`
	}
	if err := acceptanceRequest(&http.Client{Timeout: 20 * time.Second}, http.MethodPost, api.http.URL+"/v1/sessions", map[string]any{"profile_id": profile}, &result); err != nil {
		t.Fatal(err)
	}
	return result.ID
}

type serialRunEvidence struct {
	runID, answer string
	calls         []core.ToolCallData
	results       []core.ToolResultData
	events        []core.SessionEvent
}

func serialRun(t *testing.T, api *serialAuditedAPI, session, prompt, want string) serialRunEvidence {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	var run storage.RunRecord
	if err := acceptanceRequest(&http.Client{Timeout: 20 * time.Second}, http.MethodPost, api.http.URL+"/v1/sessions/"+session+"/runs/async", map[string]any{"message": prompt}, &run); err != nil {
		t.Fatal(err)
	}
	defer serialAuditRun(t, api, session, run.RunID)
	if claimed, err := api.server.RunWorkerOnce(ctx, "serial-acceptance"); err != nil || !claimed {
		t.Fatalf("serial worker claimed=%t error=%v", claimed, err)
	}
	state, err := api.queue.GetRun(ctx, run.RunID)
	if err != nil || state.Status != string(core.RunCompleted) {
		if loaded, loadErr := api.sessions.Load(ctx, session); loadErr == nil {
			for _, event := range loaded.Events() {
				if event.RunID != run.RunID {
					continue
				}
				switch event.Type {
				case core.EvRunError, core.EvStepError, core.EvToolCall, core.EvToolResult:
					detail := string(event.Data)
					for _, name := range []string{"HARNESS_LLM_API_KEY", "HARNESS_LLM_BASE_URL"} {
						if secret := os.Getenv(name); secret != "" {
							detail = strings.ReplaceAll(detail, secret, "[REDACTED]")
						}
					}
					t.Logf("serial failure evidence event=%s data=%s", event.Type, detail)
				}
			}
		}
		t.Fatalf("serial run=%s status=%s store_error=%t", run.RunID, state.Status, err != nil)
	}
	loaded, err := api.sessions.Load(ctx, session)
	if err != nil {
		t.Fatal(err)
	}
	result := serialRunEvidence{runID: run.RunID}
	t.Cleanup(func() {
		if t.Failed() {
			for _, event := range result.events {
				if event.Type == core.EvToolCall || event.Type == core.EvToolResult {
					t.Logf("serial synthetic tool evidence event=%s data=%s", event.Type, event.Data)
				}
			}
		}
	})
	ends := 0
	for _, event := range loaded.Events() {
		if event.RunID != run.RunID {
			continue
		}
		result.events = append(result.events, event)
		switch event.Type {
		case core.EvAssistantMessage:
			var data core.AssistantMessageData
			if json.Unmarshal(event.Data, &data) == nil && data.Text != "" {
				result.answer = data.Text
			}
		case core.EvToolCall:
			var data core.ToolCallData
			if err := json.Unmarshal(event.Data, &data); err != nil {
				t.Fatal(err)
			}
			result.calls = append(result.calls, data)
			t.Logf("serial tool_call name=%s args=%v", data.Name, data.Args)
		case core.EvToolResult:
			var data core.ToolResultData
			if err := json.Unmarshal(event.Data, &data); err != nil {
				t.Fatal(err)
			}
			result.results = append(result.results, data)
			t.Logf("serial tool_result ok=%t content=%s", data.OK, data.Content)
		case core.EvRunEnd:
			ends++
		}
	}
	if ends != 1 || !strings.Contains(result.answer, want) {
		t.Fatalf("run=%s ends=%d expected=%q answer=%q", run.RunID, ends, want, result.answer)
	}
	t.Logf("serial session=%s run=%s tool_calls=%d tool_results=%d terminal_events=%d answer=%q", session, run.RunID, len(result.calls), len(result.results), ends, result.answer)
	return result
}

func serialWantTool(t *testing.T, evidence serialRunEvidence, id string, count int) {
	t.Helper()
	got := 0
	for _, call := range evidence.calls {
		if call.Name == id {
			got++
		}
	}
	if got != count {
		t.Fatalf("tool=%s calls=%d want=%d", id, got, count)
	}
}

func serialMarker(t *testing.T) string {
	t.Helper()
	value, err := core.NewID("proof_")
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func serialConversationRestore(t *testing.T, model *serialAcceptanceModel) {
	open := testdb.Postgres(t)
	first := newSerialAuditedAPI(t, open(), model, &atomic.Int32{})
	const prompt = "Remember user-provided facts in this conversation. Follow exact-output requests. Never invent a missing fact."
	serialProfile(t, first, "serial.chat", prompt)
	session := serialSession(t, first, "serial.chat")
	marker := serialMarker(t)
	initial := serialRun(t, first, session, "The verification marker is "+marker+". Remember it. Reply with ACK only.", "ACK")
	chunks := 0
	for _, event := range initial.events {
		if event.Type == core.EvAssistantChunk {
			chunks++
		}
	}
	if chunks == 0 || len(initial.calls) != 0 {
		t.Fatal("expected streamed text without a tool call")
	}
	first.close(t)
	second := newSerialAuditedAPI(t, open(), model, &atomic.Int32{})
	serialProfile(t, second, "serial.chat", prompt)
	serialRun(t, second, session, "What is the verification marker I gave you earlier? Reply with only that exact marker.", marker)
	if model.calls != 2 {
		t.Fatalf("conversation calls=%d want=2", model.calls)
	}
}

func serialMemoryAcrossSessions(t *testing.T, model *serialAcceptanceModel) {
	open := testdb.Postgres(t)
	first := newSerialAuditedAPI(t, open(), model, &atomic.Int32{})
	store, err := storage.NewSQLMemoryStore(first.db, storage.SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	caps, err := memory.NewStandardCapabilities(store)
	if err != nil {
		t.Fatal(err)
	}
	serialRegister(t, first, caps...)
	serialProfile(t, first, "serial.remember", "Call memory.remember exactly once using the user's exact key and content. After its success reply SAVED. Do not use any other tool.", memory.RememberCapabilityID)
	marker := serialMarker(t)
	write := serialRun(t, first, serialSession(t, first, "serial.remember"), "Remember key serial-favorite-color and content "+marker+".", "SAVED")
	serialWantTool(t, write, memory.RememberCapabilityID, 1)
	_, _, user := serialScopes()
	entries, err := store.Recall(context.Background(), user, "serial-favorite-color", nil, 4)
	if err != nil || len(entries) != 1 || entries[0].Content != marker {
		t.Fatal("memory write was not durably stored with its exact value")
	}
	first.close(t)
	second := newSerialAuditedAPI(t, open(), model, &atomic.Int32{})
	reopened, err := storage.NewSQLMemoryStore(second.db, storage.SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	caps, err = memory.NewStandardCapabilities(reopened)
	if err != nil {
		t.Fatal(err)
	}
	serialRegister(t, second, caps...)
	serialProfile(t, second, "serial.recall", "Call memory.recall exactly once with query serial-favorite-color. Reply with the exact stored content, without inventing it.", memory.RecallCapabilityID)
	read := serialRun(t, second, serialSession(t, second, "serial.recall"), "Retrieve my stored serial-favorite-color fact.", marker)
	serialWantTool(t, read, memory.RecallCapabilityID, 1)
}

func serialRAGScopeFilter(t *testing.T, model *serialAcceptanceModel) {
	api := newSerialAuditedAPI(t, testdb.Postgres(t)(), model, &atomic.Int32{})
	index, err := storage.NewSQLRagIndex(api.db, storage.SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	product, tenant, _ := serialScopes()
	other, _ := product.Child(core.ScopeRef{Kind: core.ScopeTenant, ID: "other-tenant"})
	marker, hidden := serialMarker(t), serialMarker(t)
	for _, fixture := range []struct {
		scope       core.ScopePath
		id, content string
	}{
		{tenant, "visible-quartz", "Quartz launch verification code: " + marker},
		{other, "hidden-quartz", "Quartz launch verification code: " + hidden},
	} {
		if err := index.Ingest(context.Background(), fixture.scope, core.RagDocument{ID: fixture.id, Source: "serial-fixture", Content: fixture.content}); err != nil {
			t.Fatal(err)
		}
	}
	capability, err := rag.NewStandardSearchCapability("rag.search", index)
	if err != nil {
		t.Fatal(err)
	}
	serialRegister(t, api, capability)
	serialProfile(t, api, "serial.rag", "Call rag.search exactly once with query Quartz and top_k 5. Return the exact verification code found in the retrieved content. Never invent a code.", "rag.search")
	evidence := serialRun(t, api, serialSession(t, api, "serial.rag"), "Find the Quartz launch verification code from my knowledge base.", marker)
	serialWantTool(t, evidence, "rag.search", 1)
	for _, result := range evidence.results {
		if !result.OK || strings.Contains(result.Content, hidden) {
			t.Fatal("RAG result failed or included another tenant's fixture")
		}
	}
	if strings.Contains(evidence.answer, hidden) {
		t.Fatal("answer included another tenant's fixture")
	}
}

type serialFunctionTool struct {
	id       string
	fn       func(core.CapabilityRequest) (core.CapabilityResult, error)
	internal bool
}

func (tool serialFunctionTool) Manifest() core.CapabilityManifest {
	schema := map[string]any{"type": "object", "properties": map[string]any{"n": map[string]any{"type": "integer"}}, "required": []any{"n"}, "additionalProperties": false}
	manifest := core.CapabilityManifest{ID: tool.id, Version: "1", Name: tool.id, Kind: core.KindTool, Contract: "harness.tool/v1", Idempotent: true, InputSchema: schema}
	if !tool.internal {
		manifest.Tool = &core.ToolExposure{Parameters: schema}
	}
	return manifest
}
func (tool serialFunctionTool) Execute(_ context.Context, request core.CapabilityRequest) (core.CapabilityResult, error) {
	if err := core.RequireAcceptedInvocation(request); err != nil {
		return core.CapabilityResult{}, err
	}
	return tool.fn(request)
}

func serialNumber(request core.CapabilityRequest) (int, error) {
	switch value := request.Args["n"].(type) {
	case float64:
		return int(value), nil
	case int:
		return value, nil
	default:
		return 0, fmt.Errorf("missing numeric fixture argument")
	}
}

func serialProtectedWorkflow(t *testing.T, model *serialAcceptanceModel) {
	open := testdb.Postgres(t)
	api := newSerialAuditedAPI(t, open(), model, &atomic.Int32{})
	var order []string
	serialRegister(t, api,
		serialFunctionTool{id: "serial.double", internal: true, fn: func(r core.CapabilityRequest) (core.CapabilityResult, error) {
			n, err := serialNumber(r)
			if err != nil {
				return core.CapabilityResult{}, err
			}
			order = append(order, "double")
			return core.CapabilityResult{Content: fmt.Sprintf(`{"value":%d}`, n*2), OK: true}, nil
		}},
		serialFunctionTool{id: "serial.plus", internal: true, fn: func(r core.CapabilityRequest) (core.CapabilityResult, error) {
			n, err := serialNumber(r)
			if err != nil {
				return core.CapabilityResult{}, err
			}
			order = append(order, "plus")
			return core.CapabilityResult{Content: fmt.Sprintf(`{"value":%d}`, n+3), OK: true}, nil
		}},
	)
	pipeline, err := workflow.NewCapability("serial.pipeline", workflow.Definition{Description: "Double n, then add three through protected steps.", InputSchema: serialFunctionTool{}.Manifest().Tool.Parameters,
		Steps: []workflow.Step{{Name: "double", CapabilityID: "serial.double", Args: map[string]any{"n": "$ref:input.n"}}, {Name: "plus", CapabilityID: "serial.plus", Args: map[string]any{"n": "$ref:double.data.value"}}}})
	if err != nil {
		t.Fatal(err)
	}
	serialRegister(t, api, pipeline)
	serialProfile(t, api, "serial.workflow", "Call serial.pipeline exactly once with n 7. serial.double and serial.plus are internal workflow steps: never call them directly. After the pipeline returns, reply only with its numeric value.", "serial.pipeline", "serial.double", "serial.plus")
	session := serialSession(t, api, "serial.workflow")
	evidence := serialRun(t, api, session, "Run the pipeline on 7 and return its result.", "17")
	serialWantTool(t, evidence, "serial.pipeline", 1)
	serialWantTool(t, evidence, "serial.double", 1)
	serialWantTool(t, evidence, "serial.plus", 1)
	if strings.Join(order, ",") != "double,plus" {
		t.Fatalf("workflow order=%v", order)
	}
	if len(evidence.results) != 3 {
		t.Fatalf("workflow tool results=%d want=3", len(evidence.results))
	}
	for _, result := range evidence.results {
		if !result.OK {
			t.Fatal("workflow tool result failed")
		}
	}
	before := serialAuditHistory(t, api, session)
	api.close(t)
	replacement := newSerialAuditedAPI(t, open(), model, &atomic.Int32{})
	after := serialAuditHistory(t, replacement, session)
	left, _ := json.Marshal(before)
	right, _ := json.Marshal(after)
	if string(left) != string(right) {
		t.Fatal("history changed after service and database-pool replacement")
	}
	t.Logf("serial history restored session=%s persisted_events=%d additional_model_calls=0", session, len(after))
}

func serialDurableSubagent(t *testing.T, model *serialAcceptanceModel) {
	api := newSerialAuditedAPI(t, testdb.Postgres(t)(), model, &atomic.Int32{})
	var effects atomic.Int32
	marker := serialMarker(t)
	serialRegister(t, api, serialFunctionTool{id: "serial.child_lookup", fn: func(core.CapabilityRequest) (core.CapabilityResult, error) {
		effects.Add(1)
		return core.CapabilityResult{Content: marker, OK: true}, nil
	}})
	serialProfile(t, api, "serial.child", "Call serial.child_lookup exactly once with n 1. Reply with the exact text returned by the tool.", "serial.child_lookup")
	links, err := storage.NewSQLDelegationLinkStore(api.db, storage.SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	delegate, err := subagent.NewCapability(api.server.runtime, "serial.delegate", subagent.Options{ProfileID: "serial.child", Sessions: api.sessions, Links: links, MaxDepth: 2, DelegationMaxToolCalls: 2})
	if err != nil {
		t.Fatal(err)
	}
	serialRegister(t, api, delegate)
	serialProfile(t, api, "serial.parent", "Call serial.delegate exactly once with prompt: retrieve the verification marker using your lookup tool. After the child returns, reply with the exact marker from the child. Never call serial.child_lookup directly.", "serial.delegate", "serial.child_lookup")
	session := serialSession(t, api, "serial.parent")
	evidence := serialRun(t, api, session, "Ask the specialist child agent to retrieve its verification marker.", marker)
	serialWantTool(t, evidence, "serial.delegate", 1)
	serialWantTool(t, evidence, "serial.child_lookup", 0)
	rows, err := links.List(context.Background(), subagent.DelegationLinkFilter{ParentSessionID: session, ParentRunID: evidence.runID, TenantID: "acceptance-tenant", Limit: 4})
	if err != nil || len(rows) != 1 || effects.Load() != 1 {
		t.Fatalf("delegation links=%d effects=%d error=%v", len(rows), effects.Load(), err)
	}
	child, err := api.sessions.Load(context.Background(), rows[0].ChildSessionID)
	if err != nil {
		t.Fatal(err)
	}
	ends := 0
	for _, event := range child.Events() {
		if event.RunID == rows[0].ChildRunID && event.Type == core.EvRunEnd {
			ends++
		}
	}
	if ends != 1 {
		t.Fatalf("child terminal events=%d", ends)
	}
	serialAuditRun(t, api, rows[0].ChildSessionID, rows[0].ChildRunID)
	var parentTrace, parentSpan, childTrace, childParent string
	for _, span := range api.exporter.GetSpans() {
		attrs := map[string]string{}
		for _, attr := range span.Attributes {
			attrs[string(attr.Key)] = attr.Value.AsString()
		}
		if span.Name == core.SpanToolCall && attrs["run.id"] == evidence.runID && attrs["capability.id"] == "serial.delegate" {
			parentTrace, parentSpan = span.SpanContext.TraceID().String(), span.SpanContext.SpanID().String()
		}
		if span.Name == core.SpanRunSegment && attrs["run.id"] == rows[0].ChildRunID {
			childTrace, childParent = span.SpanContext.TraceID().String(), span.Parent.SpanID().String()
		}
	}
	if parentSpan == "" || parentTrace != childTrace || parentSpan != childParent {
		t.Fatal("child run trace is detached from durable parent's delegation tool span")
	}
	t.Logf("serial delegation trace=%s parent_tool_span=%s child_parent_span=%s sql_link_matches_trace=true", parentTrace, parentSpan, childParent)
	t.Logf("durable child_session=%s child_run=%s parent_run=%s child_tool_effects=%d", rows[0].ChildSessionID, rows[0].ChildRunID, evidence.runID, effects.Load())
}

func serialToolHookDenial(t *testing.T, model *serialAcceptanceModel) {
	api := newSerialAuditedAPI(t, testdb.Postgres(t)(), model, &atomic.Int32{})
	var effects atomic.Int32
	serialRegister(t, api, serialFunctionTool{id: "serial.denied", fn: func(core.CapabilityRequest) (core.CapabilityResult, error) {
		effects.Add(1)
		return core.CapabilityResult{Content: "unexpected effect", OK: true}, nil
	}})
	api.server.runtime.Hooks = &core.RunHooksFuncs{OnBeforeToolFn: func(context.Context, core.RunInfo, core.ToolCall) error {
		return fmt.Errorf("serial policy denies this tool")
	}}
	serialProfile(t, api, "serial.guard", "Call serial.denied exactly once with n 1. If the tool is refused, do not retry it; reply BLOCKED.", "serial.denied")
	evidence := serialRun(t, api, serialSession(t, api, "serial.guard"), "Attempt the guarded tool and report its outcome.", "BLOCKED")
	serialWantTool(t, evidence, "serial.denied", 1)
	if effects.Load() != 0 || len(evidence.results) != 1 || evidence.results[0].OK {
		t.Fatal("hook denial did not prevent the tool effect")
	}
}

// This offline regression exercises the same HTTP/SQL/context fixture before
// spending another live request while diagnosing the memory path.
func TestPostgresSerialMemoryFixture(t *testing.T) {
	t.Setenv("HARNESS_LLM_MODEL", "serial-offline-fixture")
	model := &serialAcceptanceModel{inner: serialMemoryFixtureModel{}, test: t}
	serialMemoryAcrossSessions(t, model)
}

type serialMemoryFixtureModel struct{}

func TestPostgresSerialWorkflowFixture(t *testing.T) {
	t.Setenv("HARNESS_LLM_MODEL", "serial-offline-fixture")
	model := &serialAcceptanceModel{inner: serialWorkflowFixtureModel{}, test: t}
	serialProtectedWorkflow(t, model)
}

type serialWorkflowFixtureModel struct{}

func (serialWorkflowFixtureModel) Provider() string { return "openai-compatible" }
func (serialWorkflowFixtureModel) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	last := options.Messages[len(options.Messages)-1]
	if last.Role == core.RoleTool {
		for _, message := range options.Messages {
			if message.Role == core.RoleTool && message.ToolCallID != "serial-offline-pipeline" {
				return fmt.Errorf("nested audit tool result leaked into model context: %s", message.ToolCallID)
			}
		}
		if last.ToolCallID != "serial-offline-pipeline" || !strings.Contains(last.Content, `"value":17`) {
			return fmt.Errorf("workflow fixture expected outer result 17, got id=%s content=%s", last.ToolCallID, last.Content)
		}
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: "17"})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
		return nil
	}
	found := false
	for _, tool := range options.Tools {
		if tool.Name == "serial.pipeline" {
			found = true
		}
	}
	if !found {
		return fmt.Errorf("pipeline was not exposed to model")
	}
	call := core.ToolCall{ID: "serial-offline-pipeline", Name: "serial.pipeline", Args: map[string]any{"n": 7}}
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &call})
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
	return nil
}

func (serialMemoryFixtureModel) Provider() string { return "openai-compatible" }
func (serialMemoryFixtureModel) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	if len(options.Messages) == 0 {
		return fmt.Errorf("missing fixture input")
	}
	last := options.Messages[len(options.Messages)-1]
	if last.Role == core.RoleTool {
		answer := "SAVED"
		if strings.Contains(last.Content, `"entries"`) {
			var result struct {
				Entries []core.MemoryEntry `json:"entries"`
			}
			if err := json.Unmarshal([]byte(last.Content), &result); err != nil || len(result.Entries) != 1 {
				return fmt.Errorf("unexpected memory fixture result")
			}
			answer = result.Entries[0].Content
		}
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: answer})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
		return nil
	}
	call := core.ToolCall{ID: "serial-offline-call", Name: memory.RecallCapabilityID, Args: map[string]any{"query": "serial-favorite-color"}}
	if strings.Contains(options.System, "memory.remember") {
		parts := strings.Fields(last.Content)
		call.Name = memory.RememberCapabilityID
		call.Args = map[string]any{"key": "serial-favorite-color", "content": strings.TrimSuffix(parts[len(parts)-1], ".")}
	}
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &call})
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
	return nil
}
