package core

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestFusedRecentProjectionMatchesDeriveThenCompact(t *testing.T) {
	tests := []struct {
		name      string
		compactor RecentTurnsCompactor
		append    func(t *testing.T, session *Session)
	}{
		{name: "all messages with unicode old tool result", compactor: RecentTurnsCompactor{MaxToolResultChars: 4}, append: appendFusedProjectionFixture},
		{name: "window retains earliest fitting turn", compactor: RecentTurnsCompactor{MaxMessages: 5, MaxToolResultChars: 4}, append: appendFusedProjectionFixture},
		{name: "oversized latest turn remains whole", compactor: RecentTurnsCompactor{MaxMessages: 2}, append: appendFusedProjectionFixture},
		{name: "zero limits", compactor: RecentTurnsCompactor{}, append: appendFusedProjectionFixture},
		{name: "no user boundary", compactor: RecentTurnsCompactor{MaxMessages: 1, MaxToolResultChars: 2}, append: appendFusedNoUserFixture},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			session := fusedProjectionSession(t, "session-fused-"+strings.ReplaceAll(test.name, " ", "-"))
			test.append(t, session)

			want, err := session.DeriveMessages()
			if err != nil {
				t.Fatal(err)
			}
			want = test.compactor.Compact(want)
			got, handled := session.deriveRecentCompactedMessages(test.compactor)
			if !handled {
				t.Fatal("fused projection unexpectedly fell back")
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("fused projection differs\n got: %#v\nwant: %#v", got, want)
			}
		})
	}
}

func TestFusedRecentProjectionKeepsPreambleWhenWindowFits(t *testing.T) {
	for _, limit := range []int{4, 5, 99} {
		session := fusedProjectionSession(t, fmt.Sprintf("session-fused-fit-%d", limit))
		appendFusedProjectionFixture(t, session)
		want, err := session.DeriveMessages()
		if err != nil {
			t.Fatal(err)
		}
		want = (RecentTurnsCompactor{MaxMessages: limit}).Compact(want)
		got, handled := session.deriveRecentCompactedMessages(RecentTurnsCompactor{MaxMessages: limit})
		if !handled || !reflect.DeepEqual(got, want) {
			t.Fatalf("limit=%d fused=%#v want=%#v", limit, got, want)
		}
	}
}

func TestFusedRecentProjectionDeterministicSequenceEquivalence(t *testing.T) {
	sequences := []string{
		"UA", "UAT", "AUA", "NUTA", "UATUAT", "AUUATT", "NANUAT", "UAAUTUAT",
	}
	for index, sequence := range sequences {
		for _, limit := range []int{0, 1, 2, 3, 8, 64} {
			session := fusedProjectionSession(t, fmt.Sprintf("session-fused-seq-%d-%d", index, limit))
			for position, kind := range sequence {
				var eventType SessionEventType
				var data any
				switch kind {
				case 'U':
					eventType, data = EvUserMessage, UserMessageData{Text: fmt.Sprintf("u-%d", position)}
				case 'A':
					eventType, data = EvAssistantMessage, AssistantMessageData{Text: fmt.Sprintf("a-%d", position)}
				case 'T':
					eventType, data = EvToolResult, ToolResultData{CallID: fmt.Sprintf("c-%d", position), Content: "tool"}
				default:
					eventType, data = EvRunUsage, RunUsageData{InputTokens: 1, OutputTokens: 1}
				}
				if _, err := session.Append("run-seq", eventType, data); err != nil {
					t.Fatal(err)
				}
			}
			want, err := session.DeriveMessages()
			if err != nil {
				t.Fatal(err)
			}
			want = (RecentTurnsCompactor{MaxMessages: limit}).Compact(want)
			got, handled := session.deriveRecentCompactedMessages(RecentTurnsCompactor{MaxMessages: limit})
			if !handled || !reflect.DeepEqual(got, want) {
				t.Fatalf("sequence=%q limit=%d fused=%#v want=%#v", sequence, limit, got, want)
			}
		}
	}
}

func TestFusedRecentProjectionDetachesLegacyToolCallMaps(t *testing.T) {
	session := fusedProjectionSession(t, "session-fused-maps")
	if _, err := session.Append("run-fused", EvUserMessage, UserMessageData{Text: "first"}); err != nil {
		t.Fatal(err)
	}
	call := ToolCall{ID: "legacy-call", Name: "tool.legacy", Args: map[string]any{"symbol": "BTC"}}
	if _, err := session.Append("run-fused", EvAssistantMessage, AssistantMessageData{ToolCall: &call}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append("run-fused", EvUserMessage, UserMessageData{Text: "latest"}); err != nil {
		t.Fatal(err)
	}

	got, handled := session.deriveRecentCompactedMessages(RecentTurnsCompactor{})
	if !handled || len(got) != 3 || got[1].ToolCall == nil || len(got[1].ToolCalls) != 1 {
		t.Fatalf("unexpected fused messages: handled=%t %#v", handled, got)
	}
	got[1].ToolCall.Args["symbol"] = "MUTATED"
	if got[1].ToolCalls[0].Args["symbol"] != "BTC" {
		t.Fatalf("legacy tool-call fields share mutable args: %#v", got[1])
	}
	replayed, err := session.DeriveMessages()
	if err != nil {
		t.Fatal(err)
	}
	if replayed[1].ToolCall.Args["symbol"] != "BTC" || replayed[1].ToolCalls[0].Args["symbol"] != "BTC" {
		t.Fatalf("fused caller mutation reached event projection: %#v", replayed[1])
	}
}

func TestFusedRecentProjectionFallsBackForSummary(t *testing.T) {
	session := fusedProjectionSession(t, "session-fused-summary")
	if _, err := session.Append("run-fused", EvUserMessage, UserMessageData{Text: "old"}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append("run-fused", EvContextSummary, ContextSummaryData{Op: "replace", Start: 0, End: 0, Summary: "old summary"}); err != nil {
		t.Fatal(err)
	}
	if messages, handled := session.deriveRecentCompactedMessages(RecentTurnsCompactor{MaxMessages: 1}); handled || messages != nil {
		t.Fatalf("summary projection must use full fallback: handled=%t messages=%#v", handled, messages)
	}
}

func TestAgentFusedRecentCompactorMatchesLegacyProjection(t *testing.T) {
	session := fusedProjectionSession(t, "session-agent-fused")
	appendFusedProjectionFixture(t, session)
	compactor := &RecentTurnsCompactor{MaxMessages: 6, MaxToolResultChars: 4}
	var mismatch error
	model := streamAdapterFunc(func(_ context.Context, options GenerateOptions, emit func(StreamChunk)) error {
		want, err := session.DeriveMessages()
		if err != nil {
			mismatch = err
		} else {
			want = compactor.Compact(want)
			if !reflect.DeepEqual(options.Messages, want) {
				mismatch = fmt.Errorf("model messages differ: got %#v want %#v", options.Messages, want)
			}
		}
		emit(StreamChunk{Kind: StreamKindAssistant, Text: "done"})
		emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishStop})
		return nil
	})
	agent, err := NewAgent(AgentOptions{
		LLM: model, Tools: benchmarkToolRuntime{}, Session: session, MaxSteps: 1, Compactor: compactor,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := agent.RunTurn(context.Background(), TurnInput{RunID: "run-agent-fused", Text: "new request"})
	if err != nil || result.Status != RunCompleted {
		t.Fatalf("run result = %#v, err = %v", result, err)
	}
	if mismatch != nil {
		t.Fatal(mismatch)
	}
}

func TestAgentCustomCompactorRemainsOnLegacyPath(t *testing.T) {
	session := fusedProjectionSession(t, "session-agent-custom")
	appendFusedProjectionFixture(t, session)
	called := false
	compactor := ContextCompactorFunc(func(messages []ChatMessage) []ChatMessage {
		called = true
		return (RecentTurnsCompactor{MaxMessages: 6, MaxToolResultChars: 4}).Compact(messages)
	})
	model := streamAdapterFunc(func(_ context.Context, _ GenerateOptions, emit func(StreamChunk)) error {
		emit(StreamChunk{Kind: StreamKindAssistant, Text: "done"})
		emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishStop})
		return nil
	})
	agent, err := NewAgent(AgentOptions{
		LLM: model, Tools: benchmarkToolRuntime{}, Session: session, MaxSteps: 1, Compactor: compactor,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := agent.RunTurn(context.Background(), TurnInput{RunID: "run-agent-custom", Text: "new request"}); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("custom compactor was bypassed")
	}
}

func TestAgentSummarizerDisablesFusedRecentCompactor(t *testing.T) {
	session := fusedProjectionSession(t, "session-agent-summarizer")
	appendFusedProjectionFixture(t, session)
	called := false
	summarizer := runSummarizerFunc(func(_ context.Context, _ *Session, _ string, _ func(SessionEvent), messages []ChatMessage) ([]ChatMessage, error) {
		called = true
		return messages, nil
	})
	model := streamAdapterFunc(func(_ context.Context, _ GenerateOptions, emit func(StreamChunk)) error {
		emit(StreamChunk{Kind: StreamKindAssistant, Text: "done"})
		emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishStop})
		return nil
	})
	agent, err := NewAgent(AgentOptions{
		LLM: model, Tools: benchmarkToolRuntime{}, Session: session, MaxSteps: 1,
		Compactor: RecentTurnsCompactor{MaxMessages: 6}, Summarizer: summarizer,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := agent.RunTurn(context.Background(), TurnInput{RunID: "run-agent-summarizer", Text: "new request"}); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("summarizer was bypassed by fused compaction")
	}
}

func fusedProjectionSession(t *testing.T, id string) *Session {
	t.Helper()
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	scope, err := user.Child(ScopeRef{Kind: ScopeSession, ID: id})
	if err != nil {
		t.Fatal(err)
	}
	session, err := NewSession(SessionOptions{ID: id, ProfileID: "product.agent", Principal: principal, Scope: scope})
	if err != nil {
		t.Fatal(err)
	}
	return session
}

func appendFusedProjectionFixture(t *testing.T, session *Session) {
	t.Helper()
	appendEvent := func(eventType SessionEventType, data any) {
		t.Helper()
		if _, err := session.Append("run-fused", eventType, data); err != nil {
			t.Fatal(err)
		}
	}
	appendEvent(EvAssistantMessage, AssistantMessageData{Text: "preamble"})
	appendEvent(EvUserMessage, UserMessageData{Text: "old request"})
	legacy := ToolCall{ID: "old-call", Name: "tool.old", Args: map[string]any{"key": "old"}}
	appendEvent(EvAssistantMessage, AssistantMessageData{ToolCall: &legacy})
	appendEvent(EvToolResult, ToolResultData{CallID: "old-call", Content: "\u00e9\u00e9\u00e9\u00e9\u00e9"})
	appendEvent(EvUserMessage, UserMessageData{Text: "middle request"})
	appendEvent(EvAssistantMessage, AssistantMessageData{Text: "middle answer"})
	appendEvent(EvUserMessage, UserMessageData{Text: "latest request"})
	latest := ToolCall{ID: "latest-call", Name: "tool.latest", Args: map[string]any{"key": "latest"}}
	appendEvent(EvAssistantMessage, AssistantMessageData{ToolCall: &latest, ToolCalls: []ToolCall{latest}})
	appendEvent(EvToolResult, ToolResultData{CallID: "latest-call", Content: strings.Repeat("z", 16)})
}

func appendFusedNoUserFixture(t *testing.T, session *Session) {
	t.Helper()
	if _, err := session.Append("run-fused", EvAssistantMessage, AssistantMessageData{Text: "preamble"}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append("run-fused", EvToolResult, ToolResultData{CallID: "call-none", Content: "tool output"}); err != nil {
		t.Fatal(err)
	}
}

type runSummarizerFunc func(context.Context, *Session, string, func(SessionEvent), []ChatMessage) ([]ChatMessage, error)

func (f runSummarizerFunc) EnsureSummarized(ctx context.Context, session *Session, runID string, emit func(SessionEvent), messages []ChatMessage) ([]ChatMessage, error) {
	return f(ctx, session, runID, emit, messages)
}
