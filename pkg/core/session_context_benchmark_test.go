package core

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func BenchmarkContextPayloadSizes(b *testing.B) {
	for _, size := range []int{16 << 10, 1 << 20, 15 << 20} {
		b.Run(fmt.Sprintf("%dKiB", size/1024), func(b *testing.B) {
			payload := strings.Repeat("x", size)
			b.Run("CachedDerive", func(b *testing.B) {
				session := benchmarkPayloadSession(b, payload)
				if _, err := session.DeriveMessages(); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := session.DeriveMessages(); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("FusedRecent", func(b *testing.B) {
				session := benchmarkPayloadSession(b, payload)
				compactor := RecentTurnsCompactor{MaxMessages: 1}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if messages, handled := session.deriveRecentCompactedMessages(compactor); !handled || len(messages) == 0 {
						b.Fatal("fused projection failed")
					}
				}
			})
		})
	}
}

func benchmarkPayloadSession(b *testing.B, payload string) *Session {
	b.Helper()
	_, _, _, user := testScopes()
	scope, err := user.Child(ScopeRef{Kind: ScopeSession, ID: "payload-bench"})
	if err != nil {
		b.Fatal(err)
	}
	session, err := NewSession(SessionOptions{ID: "payload-bench", ProfileID: "product.agent", Principal: testPrincipal(user), Scope: scope})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := session.Append("run-payload", EvUserMessage, UserMessageData{Text: payload}); err != nil {
		b.Fatal(err)
	}
	return session
}

// These benchmarks intentionally use an event log large enough for projection
// and defensive-copy costs to dominate setup. They cover the three execution
// modes used by the agent loop: an unchanged projection, one appended event,
// and a summary-triggered full rebuild.
func BenchmarkSessionDeriveMessagesCached(b *testing.B) {
	session := benchmarkSession(b, 2048, false)
	if _, err := session.DeriveMessages(); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := session.DeriveMessages(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSessionDeriveMessagesIncremental(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		session := benchmarkSession(b, 2048, false)
		if _, err := session.DeriveMessages(); err != nil {
			b.Fatal(err)
		}
		if _, err := session.Append("run-bench", EvUserMessage, UserMessageData{Text: "one appended message"}); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if _, err := session.DeriveMessages(); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

func BenchmarkSessionDeriveMessagesSummaryRebuild(b *testing.B) {
	session := benchmarkSession(b, 2048, true)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		session.mu.Lock()
		session.projValid = false
		session.mu.Unlock()
		if _, err := session.DeriveMessages(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSessionSummaryManyRanges4096(b *testing.B) {
	session := benchmarkSession(b, 4096, false)
	for start := 0; start < 4096; start += 8 {
		if err := appendBenchmarkEvent(session, EvContextSummary, ContextSummaryData{Op: "replace", Start: int64(start), End: int64(start + 3), Summary: "bounded summary"}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		session.mu.Lock()
		session.projValid = false
		session.mu.Unlock()
		if _, err := session.DeriveMessages(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSessionRunStatus(b *testing.B) {
	session := benchmarkSession(b, 2048, false)
	if err := appendBenchmarkEvent(session, EvRunStart, RunStartData{}); err != nil {
		b.Fatal(err)
	}
	if err := appendBenchmarkEvent(session, EvRunEnd, RunEndData{Status: RunCompleted}); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		status, exists := session.RunStatus("run-bench")
		if !exists || status != RunCompleted {
			b.Fatalf("status = %q, exists = %t", status, exists)
		}
	}
}

func BenchmarkSessionLastAssistantText(b *testing.B) {
	session := benchmarkSession(b, 2048, false)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if got := session.LastAssistantText("run-bench"); got == "" {
			b.Fatal("last assistant text is empty")
		}
	}
}

func BenchmarkSessionEvents(b *testing.B) {
	session := benchmarkSession(b, 2048, false)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if got := session.Events(); len(got) != 2048 {
			b.Fatalf("event count = %d", len(got))
		}
	}
}

func BenchmarkSessionDeriveThenRecentCompactLongHistory(b *testing.B) {
	session := benchmarkSession(b, 8192, false)
	compactor := RecentTurnsCompactor{MaxMessages: 128, MaxToolResultChars: 512}
	if _, err := session.DeriveMessages(); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		messages, err := session.DeriveMessages()
		if err != nil {
			b.Fatal(err)
		}
		if got := compactor.Compact(messages); len(got) > compactor.MaxMessages {
			b.Fatalf("compacted message count = %d", len(got))
		}
	}
}

func BenchmarkSessionFusedRecentCompactLongHistory(b *testing.B) {
	session := benchmarkSession(b, 8192, false)
	compactor := RecentTurnsCompactor{MaxMessages: 128, MaxToolResultChars: 512}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		messages, handled := session.deriveRecentCompactedMessages(compactor)
		if !handled || len(messages) > compactor.MaxMessages {
			b.Fatalf("handled=%t message count=%d", handled, len(messages))
		}
	}
}

func BenchmarkAgentRunTurnRecentCompactorLongHistory(b *testing.B) {
	compactor := RecentTurnsCompactor{MaxMessages: 128, MaxToolResultChars: 512}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		session := benchmarkSession(b, 8192, false)
		model := streamAdapterFunc(func(_ context.Context, _ GenerateOptions, emit func(StreamChunk)) error {
			emit(StreamChunk{Kind: StreamKindAssistant, Text: "done"})
			emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishStop})
			return nil
		})
		agent, err := NewAgent(AgentOptions{
			LLM: model, Tools: benchmarkToolRuntime{}, Session: session, MaxSteps: 1, Compactor: compactor,
		})
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		result, err := agent.RunTurn(context.Background(), TurnInput{RunID: "run-agent-bench", Text: "latest request"})
		b.StopTimer()
		if err != nil || result.Status != RunCompleted {
			b.Fatalf("run result = %#v, err = %v", result, err)
		}
	}
}

func BenchmarkAgentRunTurnRecentCompactorLongHistoryFallback(b *testing.B) {
	compactor := RecentTurnsCompactor{MaxMessages: 128, MaxToolResultChars: 512}
	legacy := ContextCompactorFunc(func(messages []ChatMessage) []ChatMessage {
		return compactor.Compact(messages)
	})
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		session := benchmarkSession(b, 8192, false)
		model := streamAdapterFunc(func(_ context.Context, _ GenerateOptions, emit func(StreamChunk)) error {
			emit(StreamChunk{Kind: StreamKindAssistant, Text: "done"})
			emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishStop})
			return nil
		})
		agent, err := NewAgent(AgentOptions{
			LLM: model, Tools: benchmarkToolRuntime{}, Session: session, MaxSteps: 1, Compactor: legacy,
		})
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		result, err := agent.RunTurn(context.Background(), TurnInput{RunID: "run-agent-bench", Text: "latest request"})
		b.StopTimer()
		if err != nil || result.Status != RunCompleted {
			b.Fatalf("run result = %#v, err = %v", result, err)
		}
	}
}

func BenchmarkRecentTurnsCompactorNoPrunableResults(b *testing.B) {
	messages := benchmarkCompactionMessages(2048, 480)
	compactor := RecentTurnsCompactor{MaxToolResultChars: 512}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if got := compactor.Compact(messages); len(got) != len(messages) {
			b.Fatalf("unexpected message count: got %d want %d", len(got), len(messages))
		}
	}
}

func BenchmarkRecentTurnsCompactorWindowTurns(b *testing.B) {
	messages := benchmarkCompactionMessages(4096, 64)
	compactor := RecentTurnsCompactor{MaxMessages: 128}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if got := compactor.Compact(messages); len(got) > compactor.MaxMessages {
			b.Fatalf("window retained %d messages", len(got))
		}
	}
}

func benchmarkSession(b *testing.B, events int, summaries bool) *Session {
	b.Helper()
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	scope, err := user.Child(ScopeRef{Kind: ScopeSession, ID: "session-bench"})
	if err != nil {
		b.Fatal(err)
	}
	session, err := NewSession(SessionOptions{
		ID: "session-bench", ProfileID: "product.agent", Principal: principal, Scope: scope,
	})
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < events; i++ {
		if i%2 == 0 {
			err = appendBenchmarkEvent(session, EvUserMessage, UserMessageData{Text: fmt.Sprintf("user message %d: %s", i, strings.Repeat("u", 48))})
		} else {
			data := AssistantMessageData{Text: fmt.Sprintf("assistant message %d: %s", i, strings.Repeat("a", 48))}
			if i%32 == 1 {
				call := ToolCall{ID: fmt.Sprintf("call-%d", i), Name: "tool.benchmark", Args: map[string]any{"index": i, "query": "benchmark"}}
				data.ToolCall = &call
				data.ToolCalls = []ToolCall{call}
			}
			err = appendBenchmarkEvent(session, EvAssistantMessage, data)
		}
		if err != nil {
			b.Fatal(err)
		}
	}
	if summaries {
		for start := 0; start < events; start += 256 {
			end := start + 127
			if end >= events {
				end = events - 1
			}
			if err := appendBenchmarkEvent(session, EvContextSummary, ContextSummaryData{
				Op: "replace", Start: int64(start), End: int64(end), Summary: fmt.Sprintf("summary %d", start/256),
			}); err != nil {
				b.Fatal(err)
			}
		}
	}
	return session
}

func appendBenchmarkEvent(session *Session, eventType SessionEventType, data any) error {
	_, err := session.Append("run-bench", eventType, data)
	return err
}

func benchmarkCompactionMessages(count, toolResultChars int) []ChatMessage {
	messages := make([]ChatMessage, 0, count)
	for i := 0; i < count; i++ {
		switch {
		case i%16 == 0:
			messages = append(messages, ChatMessage{Role: RoleUser, Content: fmt.Sprintf("turn %d", i/16), SourceSeq: int64(i)})
		case i%4 == 0:
			messages = append(messages, ChatMessage{Role: RoleTool, ToolCallID: fmt.Sprintf("call-%d", i), Content: strings.Repeat("t", toolResultChars), SourceSeq: int64(i)})
		default:
			messages = append(messages, ChatMessage{Role: RoleAssistant, Content: "assistant", SourceSeq: int64(i)})
		}
	}
	return messages
}

type benchmarkToolRuntime struct{}

func (benchmarkToolRuntime) Schemas() []ToolSchema { return nil }

func (benchmarkToolRuntime) Execute(context.Context, ToolCall) (CapabilityResult, error) {
	return CapabilityResult{}, nil
}

func (benchmarkToolRuntime) Authorized(string) bool { return false }

func (benchmarkToolRuntime) MaxCallBudget() int { return 0 }
