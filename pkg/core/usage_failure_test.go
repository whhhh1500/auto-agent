package core

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
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
	var ids []string
	for _, event := range session.Events() {
		if event.Type == EvRunUsage {
			var usage RunUsageData
			if err := json.Unmarshal(event.Data, &usage); err != nil {
				t.Fatal(err)
			}
			total.InputTokens += usage.InputTokens
			total.OutputTokens += usage.OutputTokens
			ids = append(ids, usage.InvocationID)
		}
	}
	if total.InputTokens != 13 || total.OutputTokens != 2 {
		t.Fatalf("failed call lost reported usage: %+v", total)
	}
	if !reflect.DeepEqual(ids, []string{"model:2"}) {
		t.Fatalf("usage ids=%v", ids)
	}
}

func TestAssistantUsageBatchNotifiesAllEventsAfterCallbackFailure(t *testing.T) {
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	snapshot, err := (CapabilityResolver{Registry: NewCapabilityRegistry()}).Resolve(principal, user)
	if err != nil {
		t.Fatal(err)
	}
	session := mustSession(t, user, principal)
	var delivered []SessionEventType
	agent, err := NewAgent(AgentOptions{LLM: MockLlmAdapter{}, Tools: snapshot, Session: session, OnEvent: func(event SessionEvent) {
		delivered = append(delivered, event.Type)
		if event.Type == EvAssistantMessage {
			panic("assistant delivery failed")
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	const runID = "run-usage-batch"
	if _, err := session.Append(runID, EvRunStart, RunStartData{}); err != nil {
		t.Fatal(err)
	}
	err = agent.appendAssistantUsage(runID, 0, AssistantMessageData{Text: "answer"}, TokenUsage{InputTokens: 3, OutputTokens: 2})
	if err == nil || !strings.Contains(err.Error(), "event callback panicked") {
		t.Fatalf("error = %v", err)
	}
	if got, want := delivered, []SessionEventType{EvAssistantMessage, EvRunUsage}; !reflect.DeepEqual(got, want) {
		t.Fatalf("delivery=%v want=%v", got, want)
	}
	events := session.Events()
	if len(events) != 3 || events[1].Type != EvAssistantMessage || events[2].Type != EvRunUsage {
		t.Fatalf("batch was not fully committed: %#v", events)
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
