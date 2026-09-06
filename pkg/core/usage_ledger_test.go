package core

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
)

func TestSessionUsageLedgerValidatesAndRestores(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	session := mustSession(t, user, principal)
	const runID = "run-usage-ledger"
	if _, err := session.Append(runID, EvRunStart, RunStartData{}); err != nil {
		t.Fatal(err)
	}
	for _, usage := range []RunUsageData{{InputTokens: 2, OutputTokens: 1}, {InputTokens: 3, OutputTokens: 2, InvocationID: "model:0"}, {InputTokens: 5, OutputTokens: 4, InvocationID: "summary:0:2:3"}} {
		if _, err := session.Append(runID, EvRunUsage, usage); err != nil {
			t.Fatal(err)
		}
	}
	if got := session.runUsageTotal(runID); got != (TokenUsage{InputTokens: 10, OutputTokens: 7}) {
		t.Fatalf("total=%+v", got)
	}
	version := session.Version()
	if _, err := session.Append(runID, EvRunUsage, RunUsageData{InvocationID: "model:0"}); err == nil || session.Version() != version {
		t.Fatalf("duplicate usage accepted: %v", err)
	}
	for _, bad := range []RunUsageData{{InvocationID: ""}, {InvocationID: "other:1"}, {InvocationID: "model:-1"}, {InvocationID: "model:01"}, {InvocationID: "model:1:2"}, {InvocationID: "summary:01:2:3"}, {InvocationID: "summary:2:1:3"}, {InvocationID: "summary:1:2"}, {InvocationID: "model:1", InputTokens: MaxReportedTokensPerCall + 1}, {InvocationID: strings.Repeat("x", 97)}} {
		if bad.InvocationID == "" {
			continue
		}
		if _, err := session.Append(runID, EvRunUsage, bad); err == nil {
			t.Fatalf("invalid usage accepted: %#v", bad)
		}
	}
	options := SessionOptions{ID: session.ID(), ProfileID: session.ProfileID(), Principal: session.Principal(), Scope: session.Scope()}
	restored, err := RestoreSession(options, session.Events())
	if err != nil || restored.runUsageTotal(runID) != (TokenUsage{InputTokens: 10, OutputTokens: 7}) {
		t.Fatalf("restore=%+v err=%v", restored, err)
	}
	events := session.Events()
	duplicate := events[len(events)-1]
	duplicate.Seq = int64(len(events))
	if _, err := RestoreSession(options, append(events, duplicate)); err == nil {
		t.Fatal("restore accepted duplicate usage identity")
	}
	_ = product
}

func TestSessionAppendBatchCommitsUsageAtomically(t *testing.T) {
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	session := mustSession(t, user, principal)
	const runID = "run-usage-batch"
	if _, err := session.Append(runID, EvRunStart, RunStartData{}); err != nil {
		t.Fatal(err)
	}
	version := session.Version()
	if _, err := session.appendBatch([]sessionAppendEntry{{runID: runID, kind: EvAssistantMessage, data: AssistantMessageData{Text: "answer"}}, {runID: runID, kind: EvRunUsage, data: RunUsageData{InvocationID: "model:01"}}}); err == nil {
		t.Fatal("invalid batch was accepted")
	}
	if session.Version() != version {
		t.Fatalf("invalid batch changed version from %d to %d", version, session.Version())
	}
	if _, err := session.Append(runID, EvRunUsage, RunUsageData{InvocationID: "model:0"}); err != nil {
		t.Fatalf("invalid batch mutated usage index: %v", err)
	}
	version = session.Version()
	events, err := session.appendBatch([]sessionAppendEntry{{runID: runID, kind: EvAssistantMessage, data: AssistantMessageData{Text: "answer"}}, {runID: runID, kind: EvRunUsage, data: RunUsageData{InputTokens: 2, OutputTokens: 1, InvocationID: "model:1"}}})
	if err != nil {
		t.Fatal(err)
	}
	for i, event := range events {
		if event.Seq != version+int64(i) || event.Time.IsZero() || len(event.Data) == 0 {
			t.Fatalf("batch event %d=%#v", i, event)
		}
	}
	clone, err := session.Clone()
	if err != nil {
		t.Fatal(err)
	}
	if clone.Version() != session.Version() || clone.runUsageTotal(runID) != (TokenUsage{InputTokens: 2, OutputTokens: 1}) {
		t.Fatalf("clone lost committed batch: version=%d usage=%+v", clone.Version(), clone.runUsageTotal(runID))
	}
}

func TestSessionUsageLedgerRejectsOverflow(t *testing.T) {
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	session := mustSession(t, user, principal)
	const runID = "run-usage-overflow"
	if _, err := session.Append(runID, EvRunStart, RunStartData{}); err != nil {
		t.Fatal(err)
	}
	max := int64(^uint64(0) >> 1)
	if _, err := session.Append(runID, EvRunUsage, RunUsageData{InputTokens: max}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append(runID, EvRunUsage, RunUsageData{InputTokens: 1}); err == nil {
		t.Fatal("usage overflow was accepted")
	}
}

func TestAgentWritesAssistantUsageBeforeToolCall(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	registry := NewCapabilityRegistry()
	if err := registry.Register(product, staticTool{manifest: toolManifest("market.quote", "1.0.0"), content: "42"}); err != nil {
		t.Fatal(err)
	}
	tools, err := (CapabilityResolver{Registry: registry}).Resolve(principal, user)
	if err != nil {
		t.Fatal(err)
	}
	var modelCalls atomic.Int32
	model := streamAdapterFunc(func(_ context.Context, options GenerateOptions, emit func(StreamChunk)) error {
		if modelCalls.Add(1) == 1 {
			call := ToolCall{ID: "call-usage-order", Name: "market.quote"}
			emit(StreamChunk{Kind: StreamKindAssistant, ToolCall: &call})
			emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishToolCalls})
			return nil
		}
		emit(StreamChunk{Kind: StreamKindAssistant, Text: "done"})
		emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishStop, Usage: &TokenUsage{InputTokens: 2, OutputTokens: 1}})
		return nil
	})
	session := mustSession(t, user, principal)
	agent, err := NewAgent(AgentOptions{LLM: model, Tools: tools, Session: session, MaxSteps: 3})
	if err != nil {
		t.Fatal(err)
	}
	if result, err := agent.RunTurn(context.Background(), TurnInput{RunID: "run-usage-order", Text: "quote"}); err != nil || result.Status != RunCompleted {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	events := session.Events()
	assistant, usage, tool := -1, -1, -1
	usageIDs := map[string]RunUsageData{}
	for i, event := range events {
		switch event.Type {
		case EvAssistantMessage:
			if assistant < 0 {
				assistant = i
			}
		case EvRunUsage:
			var data RunUsageData
			if err := json.Unmarshal(event.Data, &data); err != nil {
				t.Fatal(err)
			}
			usageIDs[data.InvocationID] = data
			if usage < 0 {
				usage = i
			}
		case EvToolCall:
			if tool < 0 {
				tool = i
			}
		}
	}
	if assistant < 0 || usage != assistant+1 || tool != usage+1 {
		t.Fatalf("first model prefix order is not assistant/usage/tool: assistant=%d usage=%d tool=%d events=%#v", assistant, usage, tool, events)
	}
	if first := usageIDs[fmt.Sprintf("model:%d", events[assistant-1].Seq)]; first.InputTokens != 0 || first.OutputTokens != 0 {
		t.Fatalf("unreported model usage=%+v", first)
	}
	terminalStep, terminalAssistant := int64(-1), -1
	for i, event := range events {
		if event.Type == EvAssistantMessage && i > 0 {
			var data AssistantMessageData
			if json.Unmarshal(event.Data, &data) == nil && data.Text == "done" {
				terminalAssistant = i
				terminalStep = events[i-1].Seq
			}
		}
	}
	if terminalAssistant < 0 || terminalAssistant+1 >= len(events) || events[terminalAssistant+1].Type != EvRunUsage {
		t.Fatalf("terminal assistant/usage not batched: events=%#v", events)
	}
	if last := usageIDs[fmt.Sprintf("model:%d", terminalStep)]; terminalStep < 0 || last.InputTokens != 2 || last.OutputTokens != 1 {
		t.Fatalf("terminal model usage=%+v ids=%#v", last, usageIDs)
	}
}

func TestAgentUsageLimitUsesDurableLedger(t *testing.T) {
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	snapshot, err := (CapabilityResolver{Registry: NewCapabilityRegistry()}).Resolve(principal, user)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name             string
		prior, wantCalls int64
	}{{"preflight", MaxReportedTokensPerRun, 0}, {"crossing", MaxReportedTokensPerRun - 1, 1}} {
		t.Run(tc.name, func(t *testing.T) {
			session := mustSession(t, user, principal)
			const runID = "run-usage-limit"
			if _, err := session.Append(runID, EvRunStart, RunStartData{}); err != nil {
				t.Fatal(err)
			}
			if _, err := session.Append(runID, EvRunUsage, RunUsageData{InputTokens: tc.prior}); err != nil {
				t.Fatal(err)
			}
			restored, err := session.Clone()
			if err != nil {
				t.Fatal(err)
			}
			var calls atomic.Int32
			agent, err := NewAgent(AgentOptions{LLM: streamAdapterFunc(func(_ context.Context, _ GenerateOptions, emit func(StreamChunk)) error {
				calls.Add(1)
				emit(StreamChunk{Kind: StreamKindAssistant, Text: "answer"})
				emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishStop, Usage: &TokenUsage{InputTokens: 1}})
				return nil
			}), Tools: snapshot, Session: restored, MaxSteps: 2})
			if err != nil {
				t.Fatal(err)
			}
			agent.restoreRunState(runID)
			result, err := agent.runModelSteps(context.Background(), RunInfo{RunID: runID, SessionID: restored.ID(), Principal: principal}, 0)
			if err != nil || result.Status != RunLimited || int64(calls.Load()) != tc.wantCalls {
				t.Fatalf("result=%+v err=%v calls=%d", result, err, calls.Load())
			}
			if got := restored.runUsageTotal(runID).InputTokens; got != MaxReportedTokensPerRun {
				t.Fatalf("durable total=%d", got)
			}
		})
	}
}
