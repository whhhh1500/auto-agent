package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/whhhh1500/auto-agent/internal/testdb"
	"github.com/whhhh1500/auto-agent/pkg/app/contextassembly"
	"github.com/whhhh1500/auto-agent/pkg/core"
)

type serialCatalogTool struct{ id string }

func (tool serialCatalogTool) Manifest() core.CapabilityManifest {
	m := (serialFunctionTool{id: tool.id}).Manifest()
	m.Description = strings.Repeat("Optional catalog lookup; use only when the user asks to query this catalog. ", 14)
	m.Tool.Description = m.Description
	return m
}
func (serialCatalogTool) Execute(context.Context, core.CapabilityRequest) (core.CapabilityResult, error) {
	return core.CapabilityResult{}, fmt.Errorf("catalog tool must not be executed in this conversation-history fixture")
}

// Controlled A/B: only omitting/preserving Tools in the assembler request
// changes. Both requests use the same schemas, seeded history, prompt and model.
// The 16K context policy is a fixture limit, not a claim about Gemini capacity.
func serialToolBudgetHistory(t *testing.T, model *serialAcceptanceModel) {
	previousWindow, previousOutput := model.contextWindow, model.maxOutput
	model.contextWindow, model.maxOutput = 16000, 2048
	t.Cleanup(func() { model.contextWindow, model.maxOutput = previousWindow, previousOutput })
	marker := serialMarker(t)
	start := len(model.observations)
	for _, legacy := range []bool{true, false} {
		api := newSerialAuditedAPI(t, testdb.Postgres(t)(), model, &atomic.Int32{})
		caps := make([]string, 8)
		for i := range caps {
			caps[i] = fmt.Sprintf("serial.catalog_%d", i)
			serialRegister(t, api, serialCatalogTool{id: caps[i]})
		}
		serialProfile(t, api, "serial.budget", "Read the user's latest verification marker from the conversation and return only that exact marker. Catalog tools are unrelated to this request and must not be called.", caps...)
		assembler := api.server.runtime.ContextAssembler
		if legacy {
			api.server.runtime.ContextAssembler = func(ctx context.Context, request core.ModelContext) (core.ModelContext, error) {
				request.Tools = nil // reproduce the pre-fix budget omission only
				return assembler(ctx, request)
			}
		}
		sessionID := serialSession(t, api, "serial.budget")
		session, err := api.sessions.Load(context.Background(), sessionID)
		if err != nil {
			t.Fatal(err)
		}
		version := session.Version()
		appendEvent := func(kind core.SessionEventType, data any) {
			if _, err := session.Append("seed-history", kind, data); err != nil {
				t.Fatal(err)
			}
		}
		appendEvent(core.EvRunStart, core.RunStartData{})
		for i := 0; i < 20; i++ {
			appendEvent(core.EvUserMessage, core.UserMessageData{Text: fmt.Sprintf("Archived unrelated note %d: ", i) + strings.Repeat("This historical catalog note is unrelated to the verification task. ", 7)})
			appendEvent(core.EvAssistantMessage, core.AssistantMessageData{Text: "Archived."})
		}
		appendEvent(core.EvUserMessage, core.UserMessageData{Text: "The verification marker is " + marker + ". Remember it for the next question."})
		appendEvent(core.EvAssistantMessage, core.AssistantMessageData{Text: "ACK"})
		appendEvent(core.EvRunEnd, core.RunEndData{Status: core.RunCompleted})
		if err := api.sessions.Save(context.Background(), session, version); err != nil {
			t.Fatal(err)
		}
		before := model.calls
		evidence := serialRun(t, api, sessionID, "What is the verification marker I gave you? Return only the marker from our conversation.", marker)
		if model.calls-before != 1 || len(evidence.calls) != 0 {
			t.Fatal("budget comparison must use one model request and no tool execution per arm")
		}
		observation := model.observations[len(model.observations)-1]
		t.Logf("context_budget legacy_omission=%t messages=%d tools=%d input_tokens=%d output_tokens=%d model_context_policy=16000 output_reserve=2048", legacy, observation.Messages, observation.Tools, observation.InputTokens, observation.OutputTokens)
	}
	before, after := model.observations[start], model.observations[start+1]
	if after.Tools != before.Tools || after.Messages >= before.Messages {
		t.Fatal("tool budget did not reduce optional history while preserving the tool set")
	}
	if before.InputTokens > 0 && after.InputTokens >= before.InputTokens {
		t.Fatal("upstream did not report an input-token reduction for this controlled fixture")
	}
	t.Logf("context_budget comparison before_input_tokens=%d after_input_tokens=%d same_answer=true same_tools=true", before.InputTokens, after.InputTokens)
}

func TestPostgresSerialToolBudgetHistory(t *testing.T) {
	t.Setenv("HARNESS_LLM_MODEL", "serial-offline-fixture")
	model := &serialAcceptanceModel{inner: serialBudgetHistoryModel{}, test: t}
	serialToolBudgetHistory(t, model)
}

func TestPostgresSerialToolBudgetRejectsBeforeModel(t *testing.T) {
	t.Setenv("HARNESS_LLM_MODEL", "serial-offline-fixture")
	model := &serialAcceptanceModel{inner: serialBudgetHistoryModel{}, test: t, contextWindow: 512, maxOutput: 64}
	api := newSerialAuditedAPI(t, testdb.Postgres(t)(), model, &atomic.Int32{})
	serialRegister(t, api, serialCatalogTool{id: "serial.large_schema"})
	serialProfile(t, api, "serial.preflight", "Follow the user request.", "serial.large_schema")
	assembler, err := contextassembly.NewAssembler(contextassembly.Config{SafetyMarginTokens: 4})
	if err != nil {
		t.Fatal(err)
	}
	api.server.runtime.ContextAssembler = assembler.AssembleModelContext
	session := serialSession(t, api, "serial.preflight")
	var run struct {
		ID string `json:"run_id"`
	}
	if err := acceptanceRequest(&http.Client{}, http.MethodPost, api.http.URL+"/v1/sessions/"+session+"/runs/async", map[string]any{"message": "hello"}, &run); err != nil {
		t.Fatal(err)
	}
	if claimed, err := api.server.RunWorkerOnce(context.Background(), "preflight-serial"); err != nil || !claimed {
		t.Fatalf("preflight claim=%t error=%v", claimed, err)
	}
	state, err := api.queue.GetRun(context.Background(), run.ID)
	if err != nil || state.Status != string(core.RunFailed) || model.calls != 0 {
		t.Fatalf("oversized schema reached model: status=%s calls=%d error=%v", state.Status, model.calls, err)
	}
	found := false
	for _, event := range serialAuditHistory(t, api, session) {
		if event.Type == core.EvRunError {
			var detail struct {
				Code string `json:"code"`
			}
			if err := json.Unmarshal(event.Data, &detail); err != nil {
				t.Fatal(err)
			}
			found = detail.Code == "model_context_failed"
		}
	}
	if !found {
		t.Fatal("missing durable context-budget failure")
	}
	t.Log("tool schema budget preflight: model_calls=0 run_status=failed error_code=model_context_failed")
}

type serialBudgetHistoryModel struct{}

func (serialBudgetHistoryModel) Provider() string { return "openai-compatible" }
func (serialBudgetHistoryModel) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	for _, message := range options.Messages {
		if strings.HasPrefix(message.Content, "The verification marker is proof_") {
			marker := strings.TrimSuffix(strings.Fields(message.Content)[4], ".")
			emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: marker})
			emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
			return nil
		}
	}
	return fmt.Errorf("required recent verification fact was removed")
}
