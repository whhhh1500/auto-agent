package contextassembly

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/whhhh1500/auto-agent/pkg/core"
)

func TestRollingLlmSummaryPersistsReportedUsageOnSuccessAndFailure(t *testing.T) {
	for _, fail := range []bool{false, true} {
		session, _ := rollingTestSession(t)
		for i := 0; i < 3; i++ {
			rollingAppend(t, session, i)
		}
		r := &RollingSummarizer{MaxMessages: 4, KeepTail: 2, Summarizer: LlmSummarizer{Adapter: summaryAdapterFunc(func(_ context.Context, _ core.GenerateOptions, emit func(core.StreamChunk)) error {
			emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: "summary"})
			emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop, Usage: &core.TokenUsage{InputTokens: 17, OutputTokens: 3}})
			if fail {
				return errors.New("TOP-SECRET upstream failure")
			}
			return nil
		})}}
		messages, _ := session.DeriveMessages()
		_, err := r.EnsureSummarized(context.Background(), session, "summary-run", nil, messages)
		if (err != nil) != fail || (err != nil && strings.Contains(err.Error(), "TOP-SECRET")) {
			t.Fatalf("wrong error: %v", err)
		}
		usages, summaries := 0, 0
		usageSeq, summarySeq := int64(-1), int64(-1)
		usageID := ""
		for _, event := range session.Events() {
			if event.Type == core.EvContextSummary {
				summaries++
				summarySeq = event.Seq
			}
			if event.Type == core.EvRunUsage {
				usages++
				var usage core.RunUsageData
				if json.Unmarshal(event.Data, &usage) != nil || usage.InputTokens != 17 || usage.OutputTokens != 3 {
					t.Fatal("incorrect summary usage")
				}
				usageID = usage.InvocationID
				usageSeq = event.Seq
			}
		}
		if usages != 1 || (!fail && summaries != 1) || (fail && summaries != 0) {
			t.Fatalf("failure=%t usage_events=%d summary_events=%d", fail, usages, summaries)
		}
		if !strings.HasPrefix(usageID, "summary:") {
			t.Fatalf("summary usage id=%q", usageID)
		}
		if !fail && usageSeq >= summarySeq {
			t.Fatalf("summary order usage=%d summary=%d", usageSeq, summarySeq)
		}
	}
}

func TestSummaryUsageRetriesUseDistinctPreCallIdentities(t *testing.T) {
	session, _ := rollingTestSession(t)
	if _, err := session.Append("summary-run", core.EvRunStart, core.RunStartData{}); err != nil {
		t.Fatal(err)
	}
	var calls int
	policy := LlmSummarizer{Adapter: summaryAdapterFunc(func(_ context.Context, _ core.GenerateOptions, emit func(core.StreamChunk)) error {
		calls++
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: "summary"})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop, Usage: &core.TokenUsage{InputTokens: 2, OutputTokens: 1}})
		return nil
	})}
	for range 2 {
		if _, err := summarizeAndRecordUsage(context.Background(), policy, session, "summary-run", nil, []core.ChatMessage{{Role: core.RoleUser, Content: "history"}}, 3, 4); err != nil {
			t.Fatal(err)
		}
	}
	var ids []string
	for _, event := range session.Events() {
		if event.Type == core.EvRunUsage {
			var usage core.RunUsageData
			if err := json.Unmarshal(event.Data, &usage); err != nil {
				t.Fatal(err)
			}
			ids = append(ids, usage.InvocationID)
		}
	}
	if calls != 2 || strings.Join(ids, ",") != "summary:3:4:1,summary:3:4:2" {
		t.Fatalf("calls=%d identities=%v", calls, ids)
	}
}

func TestMeteredSummaryRecordsZeroUsageIdentity(t *testing.T) {
	session, _ := rollingTestSession(t)
	for i := 0; i < 3; i++ {
		rollingAppend(t, session, i)
	}
	r := &RollingSummarizer{MaxMessages: 4, KeepTail: 2, Summarizer: LlmSummarizer{Adapter: summaryAdapterFunc(func(_ context.Context, _ core.GenerateOptions, emit func(core.StreamChunk)) error {
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: "summary"})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
		return nil
	})}}
	messages, _ := session.DeriveMessages()
	if _, err := r.EnsureSummarized(context.Background(), session, "summary-run", nil, messages); err != nil {
		t.Fatal(err)
	}
	for _, event := range session.Events() {
		if event.Type != core.EvRunUsage {
			continue
		}
		var usage core.RunUsageData
		if json.Unmarshal(event.Data, &usage) != nil || usage.InvocationID == "" || usage.InputTokens != 0 || usage.OutputTokens != 0 {
			t.Fatalf("zero summary ledger=%#v", usage)
		}
		return
	}
	t.Fatal("missing zero summary usage ledger")
}

type summaryGateFunc func(context.Context, core.ModelCallRequest) error

func (f summaryGateFunc) AuthorizeModelCall(ctx context.Context, r core.ModelCallRequest) error {
	return f(ctx, r)
}

type summaryTelemetryProbe struct {
	starts, calls int
	end           core.TelemetryAttributes
	err           error
}

func (p *summaryTelemetryProbe) Start(ctx context.Context, _ string, _ core.TelemetryAttributes) (context.Context, core.TelemetrySpan) {
	p.starts++
	return ctx, p
}
func (p *summaryTelemetryProbe) End(err error, attrs core.TelemetryAttributes) {
	p.err, p.end = err, attrs
}
func (p *summaryTelemetryProbe) AddCounter(_ context.Context, name string, n int64, _ core.TelemetryAttributes) {
	if name == core.MetricModelCalls {
		p.calls += int(n)
	}
}
func (*summaryTelemetryProbe) RecordHistogram(context.Context, string, float64, string, core.TelemetryAttributes) {
}
func (*summaryTelemetryProbe) SetGauge(context.Context, string, float64, string, core.TelemetryAttributes) {
}

func TestLlmSummaryPreflightGateIdentityAndTelemetry(t *testing.T) {
	for _, mode := range []string{"ok", "preflight", "gate_denied", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			session, _ := rollingTestSession(t)
			for i := 0; i < 3; i++ {
				rollingAppend(t, session, i)
			}
			if _, err := session.Append("summary-run", core.EvStepStart, core.StepData{Index: 2}); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			probe := &summaryTelemetryProbe{}
			gates, calls := 0, 0
			policy := LlmSummarizer{RequestIdentity: core.ModelCallRequest{RunID: "stale", SessionID: "stale", Provider: "p", Model: "m"}, Telemetry: probe}
			policy.ModelCallGate = summaryGateFunc(func(_ context.Context, request core.ModelCallRequest) error {
				gates++
				if request.RunID != "summary-run" || request.SessionID != session.ID() || request.Step != 2 || request.Principal.SubjectID != session.Principal().SubjectID || request.Budget.MaxInputTokens <= 0 {
					t.Fatal("gate received stale or incomplete run identity")
				}
				if mode == "gate_denied" {
					return errors.New("TOP-SECRET gate details")
				}
				return nil
			})
			policy.Adapter = summaryAdapterFunc(func(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
				calls++
				if err := core.RequireAcceptedModelCall(options); err != nil {
					return err
				}
				emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: "summary", Usage: &core.TokenUsage{InputTokens: 9, OutputTokens: 2}})
				if mode == "cancelled" {
					cancel()
					return ctx.Err()
				}
				emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
				return nil
			})
			if mode == "preflight" {
				policy.MaxInputBytes = 128
				policy.System = strings.Repeat("x", 129)
			}
			rolling := &RollingSummarizer{MaxMessages: 4, KeepTail: 2, Summarizer: policy}
			messages, _ := session.DeriveMessages()
			_, err := rolling.EnsureSummarized(ctx, session, "summary-run", nil, messages)
			if (err == nil) != (mode == "ok") || (err != nil && strings.Contains(err.Error(), "TOP-SECRET")) {
				t.Fatalf("wrong error=%v", err)
			}
			if mode == "cancelled" && !errors.Is(err, context.Canceled) {
				t.Fatal("cancellation classification lost")
			}
			wantCalls := 1
			if mode == "preflight" || mode == "gate_denied" {
				wantCalls = 0
			}
			if calls != wantCalls || probe.calls != wantCalls {
				t.Fatal("rejected call reached model or model metric")
			}
			if mode == "preflight" && (gates != 0 || probe.starts != 0) {
				t.Fatal("preflight performed authorization or model span")
			}
			if mode != "preflight" && (probe.starts != 1 || probe.end["model.usage.reported"] != map[bool]string{true: "true", false: "false"}[wantCalls > 0]) {
				t.Fatal("missing trace usage evidence")
			}
		})
	}
}

type summaryJSONBomb struct{}

func (summaryJSONBomb) MarshalJSON() ([]byte, error) { panic("custom marshaler must not run") }

func TestSummaryTranscriptBoundsArgumentGraphs(t *testing.T) {
	cycle := map[string]any{}
	cycle["self"] = cycle
	for _, args := range []map[string]any{{"huge": strings.Repeat("x", 2<<20)}, {"custom": summaryJSONBomb{}}, cycle} {
		text, err := boundedSummaryTranscript(context.Background(), []core.ChatMessage{{Role: core.RoleAssistant, ToolCalls: []core.ToolCall{{ID: "call", Name: "lookup", Args: args}}}}, 16, 1024)
		if err != nil || len(text) > 1024 || !strings.Contains(text, `"args_omitted":true`) {
			t.Fatalf("argument bounding failed: len=%d err=%v", len(text), err)
		}
	}
}

func TestExtractiveSummaryRetainsToolArguments(t *testing.T) {
	s, _ := NewExtractiveSummarizer(ExtractiveSummarizerConfig{})
	text, err := s.Summarize(context.Background(), []core.ChatMessage{
		{Role: core.RoleAssistant, ToolCalls: []core.ToolCall{{ID: "lookup-8", Name: "warehouse.lookup", Args: map[string]any{"warehouse": "ALPHA"}}}},
		{Role: core.RoleTool, ToolCallID: "lookup-8", Content: "proof_78"},
	})
	if err != nil || !strings.Contains(text, `args={"warehouse":"ALPHA"}`) || !strings.Contains(text, "result=proof_78") {
		t.Fatal("extractive lost tool argument/result meaning")
	}
}

func TestLlmSummaryRejectsNegativeBudgetBeforeCall(t *testing.T) {
	for _, budget := range []core.ModelCallBudget{{MaxInputTokens: -1}, {MaxOutputTokens: -1}} {
		calls := 0
		s := LlmSummarizer{RequestIdentity: core.ModelCallRequest{Budget: budget}, Adapter: summaryAdapterFunc(func(context.Context, core.GenerateOptions, func(core.StreamChunk)) error { calls++; return nil })}
		if _, err := s.Summarize(context.Background(), []core.ChatMessage{{Role: core.RoleUser, Content: "history"}}); err == nil || calls != 0 {
			t.Fatal("negative configured budget reached model")
		}
	}
}

func TestLlmSummaryTranscriptPreservesToolMeaning(t *testing.T) {
	transcript, err := boundedSummaryTranscript(context.Background(), []core.ChatMessage{
		{Role: core.RoleAssistant, SourceSeq: 3, ToolCalls: []core.ToolCall{{ID: "lookup-8", Name: "warehouse.lookup", Args: map[string]any{"warehouse": "ALPHA"}, Continuation: "PRIVATE-PROTOCOL"}}},
		{Role: core.RoleTool, SourceSeq: 4, ToolCallID: "lookup-8", Content: "proof_78"},
	}, 16, 2048)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"lookup-8", "warehouse.lookup", "ALPHA", "proof_78"} {
		if !strings.Contains(transcript, want) {
			t.Fatalf("transcript missing %s", want)
		}
	}
	if strings.Contains(transcript, "PRIVATE-PROTOCOL") {
		t.Fatal("vendor continuation exposed to summarizer")
	}
}
