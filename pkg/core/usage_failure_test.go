package core

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestAgentPreservesReportedUsageWhenModelFails(t *testing.T) {
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	snapshot, err := (CapabilityResolver{Registry: NewCapabilityRegistry()}).Resolve(principal, user)
	if err != nil {
		t.Fatal(err)
	}
	session := mustSession(t, user, principal)
	agent, err := NewAgent(AgentOptions{Session: session, Tools: snapshot, LLM: streamAdapterFunc(func(_ context.Context, _ GenerateOptions, emit func(StreamChunk)) error {
		emit(StreamChunk{Kind: StreamKindAssistant, Text: "partial", Usage: &TokenUsage{InputTokens: 13, OutputTokens: 2}})
		return errors.New("stream interrupted")
	})})
	if err != nil {
		t.Fatal(err)
	}
	result, err := agent.RunTurn(context.Background(), TurnInput{RunID: "run-failed-usage", Text: "hello"})
	if err == nil || result.Status != RunFailed {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	var total RunUsageData
	for _, event := range session.Events() {
		if event.Type == EvRunUsage {
			var usage RunUsageData
			if err := json.Unmarshal(event.Data, &usage); err != nil {
				t.Fatal(err)
			}
			total.InputTokens += usage.InputTokens
			total.OutputTokens += usage.OutputTokens
		}
	}
	if total.InputTokens != 13 || total.OutputTokens != 2 {
		t.Fatalf("failed call lost reported usage: %+v", total)
	}
}

func TestAgentUsageAppendErrorKeepsPendingUsage(t *testing.T) {
	modelFailure := errors.New("model failed")
	for _, test := range []struct {
		name string
		call func(*Agent, RunInfo) (TurnResult, error)
	}{
		{
			name: "complete",
			call: func(agent *Agent, info RunInfo) (TurnResult, error) {
				return agent.complete(info, RunCompleted)
			},
		},
		{
			name: "fail",
			call: func(agent *Agent, info RunInfo) (TurnResult, error) {
				return agent.fail(info, "model_failed", modelFailure, false)
			},
		},
		{
			name: "approval_pause",
			call: func(agent *Agent, info RunInfo) (TurnResult, error) {
				return agent.pauseForApproval(info, &ApprovalPendingError{
					Request:    ApprovalRequest{ToolCall: ToolCall{ID: "call-usage", Name: "market.quote"}},
					Resolution: ApprovalResolution{ApprovalID: "apr_0123456789abcdef0123456789abcdef", Decision: ApprovalPending},
				}, ToolCall{ID: "call-usage", Name: "market.quote"}, nil, false)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, _, _, user := testScopes()
			principal := testPrincipal(user)
			snapshot, err := (CapabilityResolver{Registry: NewCapabilityRegistry()}).Resolve(principal, user)
			if err != nil {
				t.Fatal(err)
			}
			session := mustSession(t, user, principal)
			agent, err := NewAgent(AgentOptions{
				LLM: MockLlmAdapter{}, Tools: snapshot, Session: session,
				OnEvent: func(event SessionEvent) {
					if event.Type == EvRunUsage {
						panic("usage delivery failed")
					}
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			const runID = "run-usage-append-error"
			if _, err := session.Append(runID, EvRunStart, RunStartData{}); err != nil {
				t.Fatal(err)
			}
			agent.usage = TokenUsage{InputTokens: 3, OutputTokens: 2}

			_, err = test.call(agent, RunInfo{RunID: runID})
			if err == nil || !strings.Contains(err.Error(), "event callback panicked") {
				t.Fatalf("error = %v", err)
			}
			if test.name == "fail" && !errors.Is(err, modelFailure) {
				t.Fatalf("joined error no longer preserves original failure: %v", err)
			}
			if agent.usage != (TokenUsage{InputTokens: 3, OutputTokens: 2}) {
				t.Fatalf("pending usage was cleared: %#v", agent.usage)
			}
			usageEvents, approvalEvents := 0, 0
			for _, event := range session.Events() {
				switch event.Type {
				case EvRunUsage:
					usageEvents++
				case EvApprovalRequested:
					approvalEvents++
				}
			}
			if usageEvents != 1 {
				t.Fatalf("usage events = %d, want 1", usageEvents)
			}
			if test.name == "approval_pause" && approvalEvents != 0 {
				t.Fatalf("approval was appended after usage callback failure: %d", approvalEvents)
			}
		})
	}
}

func TestConsumeModelStreamUsageKeepsOnlyFirstValidReport(t *testing.T) {
	for _, tc := range []struct {
		name    string
		reports []*TokenUsage
		fail    bool
		want    *TokenUsage
	}{
		{"missing", nil, false, nil},
		{"zero", []*TokenUsage{{}}, false, &TokenUsage{}},
		{"finish", []*TokenUsage{{InputTokens: 11, OutputTokens: 2}}, false, &TokenUsage{InputTokens: 11, OutputTokens: 2}},
		{"error_after_usage", []*TokenUsage{{InputTokens: 11}}, true, &TokenUsage{InputTokens: 11}},
		{"duplicate", []*TokenUsage{{InputTokens: 11}, {InputTokens: 99}}, true, &TokenUsage{InputTokens: 11}},
		{"negative", []*TokenUsage{{InputTokens: -1}}, true, nil},
		{"overflow", []*TokenUsage{{InputTokens: MaxReportedTokensPerCall + 1}}, true, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			adapter := streamAdapterFunc(func(_ context.Context, _ GenerateOptions, emit func(StreamChunk)) error {
				emit(StreamChunk{Kind: StreamKindAssistant, Text: "answer"})
				for _, usage := range tc.reports {
					emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishStop, Usage: usage})
				}
				if len(tc.reports) == 0 {
					emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishStop})
				}
				if tc.name == "error_after_usage" {
					return errors.New("after report")
				}
				return nil
			})
			usage, err := ConsumeModelStreamUsage(context.Background(), adapter, GenerateOptions{}, nil)
			if (err != nil) != tc.fail || (usage == nil) != (tc.want == nil) || (usage != nil && *usage != *tc.want) {
				t.Fatalf("usage=%+v error=%v", usage, err)
			}
		})
	}
}

func BenchmarkRejectedModelChunk(b *testing.B) {
	large := strings.Repeat("x", 2<<20)
	adapter := streamAdapterFunc(func(_ context.Context, _ GenerateOptions, emit func(StreamChunk)) error {
		emit(StreamChunk{Kind: StreamKindAssistant, Text: large})
		return nil
	})
	reject := func(StreamChunk) error { return errors.New("bounded consumer rejected chunk") }
	b.ReportAllocs()
	for b.Loop() {
		if _, err := ConsumeModelStreamUsage(context.Background(), adapter, GenerateOptions{}, reject); err == nil {
			b.Fatal("oversize accepted")
		}
	}
}
