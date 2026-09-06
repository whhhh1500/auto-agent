package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cc-auto-agent/harness-core/internal/testdb"
	"github.com/cc-auto-agent/harness-core/pkg/app/contextassembly"
	"github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
)

type serialSummaryGate struct{}

func (serialSummaryGate) AuthorizeModelCall(_ context.Context, request core.ModelCallRequest) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if !strings.HasPrefix(request.RunID, "run_") || request.Principal.TenantID != "acceptance-tenant" || request.Budget.MaxInputTokens <= 0 || request.Budget.MaxOutputTokens <= 0 {
		return fmt.Errorf("summary gate rejected wrong identity or budget")
	}
	return nil
}

func serialSummaryUsageAudit(t *testing.T, api *serialAuditedAPI, evidence serialRunEvidence, requests []serialModelObservation) {
	t.Helper()
	var observed, durable, stat core.TokenUsage
	for _, request := range requests {
		observed.InputTokens += request.InputTokens
		observed.OutputTokens += request.OutputTokens
	}
	for _, event := range evidence.events {
		if event.Type == core.EvRunUsage {
			var usage core.RunUsageData
			if err := json.Unmarshal(event.Data, &usage); err != nil {
				t.Fatal(err)
			}
			durable.InputTokens += usage.InputTokens
			durable.OutputTokens += usage.OutputTokens
		}
	}
	if err := api.db.QueryRowContext(context.Background(), "SELECT input_tokens, output_tokens FROM run_stats WHERE run_id=$1", evidence.runID).Scan(&stat.InputTokens, &stat.OutputTokens); err != nil {
		t.Fatal(err)
	}
	if observed != durable || durable != stat {
		t.Fatalf("usage mismatch model=%+v durable=%+v SQL=%+v", observed, durable, stat)
	}
	serialWriteAudit(t, evidence.runID+"_usage", map[string]any{"run_id": evidence.runID, "reported_model_calls": requests, "upstream": observed, "history_usage_sum": durable, "sql_run_stat": stat})
}

func TestPostgresSerialLLMSummaryAccounting(t *testing.T) {
	t.Setenv("HARNESS_LLM_MODEL", "serial-offline-fixture")
	model := &serialAcceptanceModel{inner: serialMeteredSummaryModel{}, test: t}
	serialLLMSummary(t, model)
}

type serialMeteredSummaryModel struct{}

func (serialMeteredSummaryModel) Provider() string { return "openai-compatible" }
func (serialMeteredSummaryModel) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	facts := serialSummaryFixtureFacts(options.Messages)
	text := ""
	usage := core.TokenUsage{InputTokens: 100, OutputTokens: 10}
	if options.System == contextassembly.DefaultSummarizerPrompt {
		if err := core.RequireAcceptedModelCall(options); err != nil {
			return err
		}
		if len(facts) != 3 {
			return fmt.Errorf("summary transcript lost tool identity or prior facts")
		}
		text = "ALPHA=" + facts["ALPHA"] + " BETA=" + facts["BETA"] + " GAMMA=" + facts["GAMMA"]
		usage = core.TokenUsage{InputTokens: 200, OutputTokens: 20}
	} else {
		last := options.Messages[len(options.Messages)-1].Content
		for _, name := range []string{"ALPHA", "BETA", "GAMMA"} {
			if strings.Contains(last, "warehouse "+name+"?") {
				text = facts[name]
			}
		}
	}
	if text == "" {
		return fmt.Errorf("requested warehouse fact missing")
	}
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: text})
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop, Usage: &usage})
	return nil
}

func serialSummaryFixtureFacts(messages []core.ChatMessage) map[string]string {
	facts, byID := map[string]string{}, map[string]string{}
	compact := regexp.MustCompile(`(ALPHA|BETA|GAMMA)=(proof_[a-f0-9]+)`)
	for _, message := range messages {
		for _, line := range strings.Split(message.Content, "\n") {
			var record struct {
				Role    string          `json:"role"`
				Content string          `json:"content"`
				Calls   []core.ToolCall `json:"tool_calls"`
				CallID  string          `json:"tool_call_id"`
			}
			if json.Unmarshal([]byte(line), &record) == nil && record.Role != "" {
				for _, call := range record.Calls {
					byID[call.ID], _ = call.Args["warehouse"].(string)
				}
				if record.Role == "tool" && byID[record.CallID] != "" {
					facts[byID[record.CallID]] = record.Content
				}
				line = record.Content
			}
			for _, match := range compact.FindAllStringSubmatch(line, -1) {
				facts[match[1]] = match[2]
			}
			if strings.HasPrefix(line, "tool_call id=") {
				fields := strings.Fields(line)
				id := strings.TrimPrefix(fields[1], "id=")
				_, args, _ := strings.Cut(line, " args=")
				var decoded map[string]string
				if json.Unmarshal([]byte(args), &decoded) == nil {
					byID[id] = decoded["warehouse"]
				}
			}
			if strings.HasPrefix(line, "tool_result id=") {
				fields := strings.Fields(line)
				id := strings.TrimPrefix(fields[1], "id=")
				_, result, _ := strings.Cut(line, " result=")
				if byID[id] != "" {
					facts[byID[id]] = result
				}
			}
		}
	}
	return facts
}

func TestPostgresSummaryFailureKeepsUsageAndTrace(t *testing.T) {
	t.Setenv("HARNESS_LLM_MODEL", "serial-offline-fixture")
	model := &serialAcceptanceModel{test: t, inner: serialFailingSummaryModel{}}
	api := newSerialAuditedAPI(t, testdb.Postgres(t)(), model, &atomic.Int32{})
	serialProfile(t, api, "serial.summary-failure", "Answer the request.")
	stats, err := storage.NewSQLRunStatsStore(api.db, storage.SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	api.server.runStats = stats
	api.server.runtime.Summarizer = &contextassembly.RollingSummarizer{MaxMessages: 4, KeepTail: 2, Summarizer: contextassembly.LlmSummarizer{
		Adapter: model, Telemetry: api.server.runtime.Telemetry, ModelCallGate: serialSummaryGate{}, RequestIdentity: core.ModelCallRequest{Provider: "openai-compatible", Model: "serial-offline-fixture"},
	}}
	session := serialSession(t, api, "serial.summary-failure")
	serialSeedSummaryHistory(t, api, session, 0, []string{"ALPHA", "BETA", "GAMMA"}, []string{"a", "b", "c"}, false)
	var run struct {
		ID string `json:"run_id"`
	}
	if err := acceptanceRequest(&http.Client{}, http.MethodPost, api.http.URL+"/v1/sessions/"+session+"/runs/async", map[string]any{"message": "continue"}, &run); err != nil {
		t.Fatal(err)
	}
	if claimed, err := api.server.RunWorkerOnce(context.Background(), "summary-failure"); err != nil || !claimed {
		t.Fatalf("claim=%t error=%v", claimed, err)
	}
	events, spans, _ := serialCollectAudit(t, api, session, run.ID)
	evidence := serialRunEvidence{runID: run.ID}
	ends, summarySpans := 0, 0
	for _, event := range events {
		if event.RunID == run.ID {
			evidence.events = append(evidence.events, event)
			if event.Type == core.EvContextSummary {
				t.Fatal("failed model summary persisted replacement")
			}
			if event.Type == core.EvRunEnd {
				ends++
				var end core.RunEndData
				if json.Unmarshal(event.Data, &end) != nil || end.Status != core.RunFailed {
					t.Fatal("wrong failure terminal")
				}
			}
			if strings.Contains(string(event.Data), "TOP-SECRET") {
				t.Fatal("upstream error leaked into history")
			}
		}
	}
	var parent *serialSpanEvidence
	for i := range spans {
		if spans[i].Name == core.SpanRunSegment {
			parent = &spans[i]
		}
	}
	for _, span := range spans {
		if span.Name == core.SpanModelCall {
			summarySpans++
			if parent == nil || span.TraceID != parent.TraceID || span.ParentID != parent.SpanID || span.Attributes["model.purpose"] != "context_summary" || span.Status != "Error" || span.Attributes["model.usage.input_tokens"] != "23" {
				t.Fatal("failed summary trace or reported usage missing")
			}
		}
	}
	if model.calls != 1 || ends != 1 || summarySpans != 1 {
		t.Fatal("failure proceeded to ordinary model call or lost its terminal")
	}
	serialSummaryUsageAudit(t, api, evidence, model.observations)
}

type serialFailingSummaryModel struct{}

func (serialFailingSummaryModel) Provider() string { return "openai-compatible" }
func (serialFailingSummaryModel) Stream(_ context.Context, _ core.GenerateOptions, emit func(core.StreamChunk)) error {
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: "partial summary", Usage: &core.TokenUsage{InputTokens: 23, OutputTokens: 5}})
	return errors.New("TOP-SECRET failed upstream body")
}
