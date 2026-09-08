package server

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/whhhh1500/auto-agent/internal/testdb"
	"github.com/whhhh1500/auto-agent/pkg/app/contextassembly"
	"github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/storage"
)

// The seeded turns are synthetic, not paid model runs. Both arms use the same
// history, three distinct facts and prompts. Disable mechanical truncation in
// both arms to isolate full history vs the default 120/60 extractive policy.
func serialRollingSummary(t *testing.T, model *serialAcceptanceModel) {
	serialSummaryComparison(t, model, false)
}

func serialLLMSummary(t *testing.T, model *serialAcceptanceModel) {
	serialSummaryComparison(t, model, true)
}

func serialSummaryComparison(t *testing.T, model *serialAcceptanceModel, llmComparison bool) {
	names := []string{"ALPHA", "BETA", "GAMMA"}
	markers := []string{serialMarker(t), serialMarker(t), serialMarker(t)}
	var arms []map[string]any
	for _, variant := range []bool{false, true} {
		enabled, semantic := variant || llmComparison, variant && llmComparison
		open := testdb.Postgres(t)
		var sent []core.ChatMessage
		configure := func() *serialAuditedAPI {
			api := newSerialAuditedAPI(t, open(), model, &atomic.Int32{})
			serialProfile(t, api, "serial.summary", "Read the requested warehouse verification code from the existing conversation. Reply only with its exact code. Codes are independent for each warehouse; do not infer one from another. Do not repeat any other warehouse's code.")
			api.server.runtime.Compactor = core.RecentTurnsCompactor{}
			stats, err := storage.NewSQLRunStatsStore(api.db, storage.SQLDialectPostgres)
			if err != nil {
				t.Fatal(err)
			}
			api.server.runStats = stats
			if enabled {
				extractive, err := contextassembly.NewExtractiveSummarizer(contextassembly.ExtractiveSummarizerConfig{})
				if err != nil {
					t.Fatal(err)
				}
				api.server.runtime.Summarizer = &contextassembly.RollingSummarizer{MaxMessages: 120, KeepTail: 60, Summarizer: extractive}
				if semantic {
					api.server.runtime.Summarizer = &contextassembly.RollingSummarizer{MaxMessages: 120, KeepTail: 60, Summarizer: contextassembly.LlmSummarizer{
						Adapter: model, Telemetry: api.server.runtime.Telemetry, ModelCallGate: serialSummaryGate{},
						RequestIdentity: core.ModelCallRequest{Provider: "openai-compatible", Model: os.Getenv("HARNESS_LLM_MODEL")},
					}}
				}
			}
			assemble := api.server.runtime.ContextAssembler
			api.server.runtime.ContextAssembler = func(ctx context.Context, request core.ModelContext) (core.ModelContext, error) {
				result, err := assemble(ctx, request)
				sent = result.Messages
				return result, err
			}
			return api
		}
		api := configure()
		sessionID := serialSession(t, api, "serial.summary")
		started, start := time.Now(), len(model.observations)
		var runs []string
		var projections []map[string]any
		for turn, name := range names {
			if turn > 0 {
				history := serialAuditHistory(t, api, sessionID)
				loaded, err := api.sessions.Load(context.Background(), sessionID)
				if err != nil {
					t.Fatal(err)
				}
				before, err := loaded.DeriveMessages()
				if err != nil {
					t.Fatal(err)
				}
				api.http.Close()
				if err := api.db.Close(); err != nil {
					t.Fatal(err)
				}
				api = configure()
				if !reflect.DeepEqual(history, serialAuditHistory(t, api, sessionID)) {
					t.Fatal("summary history differs after service/pool replacement")
				}
				loaded, err = api.sessions.Load(context.Background(), sessionID)
				if err != nil {
					t.Fatal(err)
				}
				after, err := loaded.DeriveMessages()
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatal("summary projection differs after service/pool replacement")
				}
			}
			serialSeedSummaryHistory(t, api, sessionID, turn, names, markers, llmComparison)
			before := model.calls
			observationStart := len(model.observations)
			evidence := serialRun(t, api, sessionID, "What is the verification code for warehouse "+name+"? Return only that code.", markers[turn])
			wantCalls := 1
			if semantic {
				wantCalls = 2
			}
			if model.calls-before != wantCalls || len(evidence.calls) != 0 || strings.TrimSpace(evidence.answer) != markers[turn] {
				t.Fatal("summary fixture needs one correct model answer without tools per turn")
			}
			var summaries []core.ContextSummaryData
			for _, event := range evidence.events {
				if event.Type == core.EvContextSummary {
					var data core.ContextSummaryData
					if err := json.Unmarshal(event.Data, &data); err != nil {
						t.Fatal(err)
					}
					summaries = append(summaries, data)
				}
			}
			if (enabled && len(summaries) != 1) || (!enabled && len(summaries) != 0) {
				t.Fatal("unexpected durable summary count")
			}
			summaryMessages, factSources := 0, 0
			for _, message := range sent {
				isSummary := message.Provenance != nil && message.Provenance.Kind == "summary"
				if isSummary {
					summaryMessages++
				}
				if strings.Contains(message.Content, markers[turn]) {
					factSources++
					if enabled && !isSummary {
						t.Fatal("asked fact leaked into unsummarized tail")
					}
				}
			}
			if factSources != 1 || (enabled && (summaryMessages != 1 || len(sent) > 120 || (!llmComparison && len(summaries[0].Summary) <= 1024))) {
				t.Fatal("required fact missing, duplicated, or summary fixture did not exercise large prior context")
			}
			observation := model.observations[len(model.observations)-1]
			if len(sent) != observation.Messages {
				t.Fatal("assembled request differs from model observation")
			}
			serialSummaryUsageAudit(t, api, evidence, model.observations[observationStart:])
			runs = append(runs, evidence.runID)
			projections = append(projections, map[string]any{"run_id": evidence.runID, "turn": turn + 1, "summaries": summaries, "model_messages": sent})
		}
		requests := model.observations[start:]
		var input, output int64
		for _, request := range requests {
			input += request.InputTokens
			output += request.OutputTokens
		}
		elapsed := time.Since(started).Milliseconds()
		arms = append(arms, map[string]any{"summary_enabled": enabled, "llm_summary": semantic, "run_ids": runs, "input_tokens": input, "output_tokens": output, "elapsed_ms": elapsed, "requests": requests, "projections": projections, "service_pool_replacements": 2})
		file, policy := "rolling_summary_comparison", "full history vs extractive 120/60"
		if llmComparison {
			file, policy = "llm_summary_comparison", "extractive vs LLM 120/60; tool-fact history"
		}
		serialWriteAudit(t, file, map[string]any{"policy": policy + "; mechanical compactor disabled in both arms", "seed_turns": []int{64, 32, 32}, "distinct_facts": 3, "arms": arms})
		t.Logf("rolling_summary enabled=%t llm_summary=%t model_calls=%d input_tokens=%d output_tokens=%d elapsed_ms=%d correct_answers=3 restores=2", enabled, semantic, len(requests), input, output, elapsed)
	}
}

func serialSeedSummaryHistory(t *testing.T, api *serialAuditedAPI, sessionID string, batch int, names, markers []string, toolFacts bool) {
	t.Helper()
	session, err := api.sessions.Load(context.Background(), sessionID)
	if err != nil {
		t.Fatal(err)
	}
	version := session.Version()
	run := fmt.Sprintf("seed-summary-%d", batch)
	appendEvent := func(kind core.SessionEventType, data any) {
		if _, err := session.Append(run, kind, data); err != nil {
			t.Fatal(err)
		}
	}
	appendEvent(core.EvRunStart, core.RunStartData{})
	count := 32
	if batch == 0 {
		count = 64
	}
	for i := 0; i < count; i++ {
		text := fmt.Sprintf("Audit entry %d/%d: inventory reconciliation checked; no verification code changed.", batch, i)
		// Place three records in the first archived prefix's latest four user
		// goals. This tests rolling retention, not arbitrary extractive recall.
		if batch == 0 && i >= 32 && i <= 34 {
			if toolFacts {
				call := core.ToolCall{ID: fmt.Sprintf("source-call-%d", i), Name: "warehouse.lookup", Args: map[string]any{"warehouse": names[i-32]}}
				appendEvent(core.EvUserMessage, core.UserMessageData{Text: "Record the approved warehouse handoff from the lookup."})
				appendEvent(core.EvAssistantMessage, core.AssistantMessageData{ToolCall: &call, ToolCalls: []core.ToolCall{call}})
				appendEvent(core.EvToolCall, core.ToolCallData{CallID: call.ID, Name: call.Name, Args: call.Args})
				appendEvent(core.EvToolResult, core.ToolResultData{CallID: call.ID, OK: true, Content: markers[i-32]})
				appendEvent(core.EvAssistantMessage, core.AssistantMessageData{Text: "Logged."})
				continue
			}
			text = "Warehouse " + names[i-32] + " verification code: " + markers[i-32] + ". " +
				"This approved handoff record remains valid for the rest of this conversation. The receiving team checks this code against the signed inventory register before accepting a delivery. Store each warehouse's code separately: neighboring sites do not share codes. Subsequent routine audit entries confirm reconciliation status only and do not change this record. When the operator asks for this warehouse, return its exact code without adding reconciliation notes or another site's identifier."
		}
		appendEvent(core.EvUserMessage, core.UserMessageData{Text: text})
		appendEvent(core.EvAssistantMessage, core.AssistantMessageData{Text: "Logged."})
	}
	appendEvent(core.EvRunEnd, core.RunEndData{Status: core.RunCompleted})
	if err := api.sessions.Save(context.Background(), session, version); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresSerialRollingSummary(t *testing.T) {
	t.Setenv("HARNESS_LLM_MODEL", "serial-offline-fixture")
	model := &serialAcceptanceModel{inner: serialSummaryFixtureModel{}, test: t}
	serialRollingSummary(t, model)
}

type serialSummaryFixtureModel struct{}

func (serialSummaryFixtureModel) Provider() string { return "openai-compatible" }
func (serialSummaryFixtureModel) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	last := options.Messages[len(options.Messages)-1].Content
	for _, name := range []string{"ALPHA", "BETA", "GAMMA"} {
		if !strings.Contains(last, "warehouse "+name+"?") {
			continue
		}
		for _, message := range options.Messages {
			_, rest, found := strings.Cut(message.Content, "Warehouse "+name+" verification code: ")
			if !found {
				continue
			}
			code, _, _ := strings.Cut(rest, ".")
			emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: code})
			emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
			return nil
		}
	}
	return fmt.Errorf("summary fixture lost the requested historical fact")
}
