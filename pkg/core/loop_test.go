package core

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

type explicitlyRetryableError struct{}

func (explicitlyRetryableError) Error() string   { return "temporary" }
func (explicitlyRetryableError) Retryable() bool { return true }

type streamAdapterFunc func(context.Context, GenerateOptions, func(StreamChunk)) error

func (streamAdapterFunc) Provider() string { return "test-stream" }
func (f streamAdapterFunc) Stream(ctx context.Context, opts GenerateOptions, emit func(StreamChunk)) error {
	return f(ctx, opts, emit)
}

func TestRetryabilityDefaultsFailClosed(t *testing.T) {
	if isRetryable(errors.New("unknown")) {
		t.Fatal("unknown errors must not be marked retryable")
	}
	if !isRetryable(explicitlyRetryableError{}) {
		t.Fatal("typed retryable error was not recognized")
	}
}

func TestModelStreamProtocolFailsClosed(t *testing.T) {
	usage := &TokenUsage{InputTokens: 1, OutputTokens: 1}
	tests := []struct {
		name    string
		adapter LlmAdapter
	}{
		{name: "panic", adapter: streamAdapterFunc(func(context.Context, GenerateOptions, func(StreamChunk)) error {
			panic("secret model panic")
		})},
		{name: "missing-finish", adapter: streamAdapterFunc(func(_ context.Context, _ GenerateOptions, emit func(StreamChunk)) error {
			emit(StreamChunk{Kind: StreamKindAssistant, Text: "partial"})
			return nil
		})},
		{name: "empty-stop", adapter: streamAdapterFunc(func(_ context.Context, _ GenerateOptions, emit func(StreamChunk)) error {
			emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishStop})
			return nil
		})},
		{name: "duplicate-usage", adapter: streamAdapterFunc(func(_ context.Context, _ GenerateOptions, emit func(StreamChunk)) error {
			emit(StreamChunk{Kind: StreamKindAssistant, Usage: usage})
			emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishStop, Usage: usage})
			return nil
		})},
		{name: "excessive-usage", adapter: streamAdapterFunc(func(_ context.Context, _ GenerateOptions, emit func(StreamChunk)) error {
			emit(StreamChunk{Kind: StreamKindAssistant, Text: "ok", Usage: &TokenUsage{InputTokens: MaxReportedTokensPerCall + 1}})
			emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishStop})
			return nil
		})},
		{name: "unknown-kind", adapter: streamAdapterFunc(func(_ context.Context, _ GenerateOptions, emit func(StreamChunk)) error {
			emit(StreamChunk{Kind: "mystery"})
			return nil
		})},
		{name: "duplicate-call-id", adapter: streamAdapterFunc(func(_ context.Context, _ GenerateOptions, emit func(StreamChunk)) error {
			emit(StreamChunk{Kind: StreamKindAssistant, ToolCalls: []ToolCall{
				{ID: "call-1", Name: "tool.one"}, {ID: "call-1", Name: "tool.two"},
			}})
			emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishToolCalls})
			return nil
		})},
		{name: "invalid-tool-name", adapter: streamAdapterFunc(func(_ context.Context, _ GenerateOptions, emit func(StreamChunk)) error {
			emit(StreamChunk{Kind: StreamKindAssistant, ToolCall: &ToolCall{ID: "call-1", Name: "not_namespaced"}})
			emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishToolCalls})
			return nil
		})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := consumeModelStream(context.Background(), test.adapter, GenerateOptions{}, nil); err == nil {
				t.Fatal("invalid model stream was accepted")
			}
		})
	}
}

func TestModelStreamConsumerPanicIsContained(t *testing.T) {
	adapter := streamAdapterFunc(func(_ context.Context, _ GenerateOptions, emit func(StreamChunk)) error {
		emit(StreamChunk{Kind: StreamKindAssistant, Text: "hello"})
		emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishStop})
		return nil
	})
	_, err := consumeModelStream(context.Background(), adapter, GenerateOptions{}, func(StreamChunk) error {
		panic("transport panic")
	})
	if err == nil || !strings.Contains(err.Error(), "consumer panicked") {
		t.Fatalf("consumer panic was not contained: %v", err)
	}
}

func TestEventCallbackPanicIsContained(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	registry := NewCapabilityRegistry()
	if err := registry.Register(product, staticTool{manifest: toolManifest("market.quote", "1.0.0"), content: "42"}); err != nil {
		t.Fatal(err)
	}
	snapshot, _ := (CapabilityResolver{Registry: registry}).Resolve(principal, user)
	agent, err := NewAgent(AgentOptions{
		LLM: MockLlmAdapter{}, Tools: snapshot, Session: mustSession(t, user, principal),
		OnEvent: func(SessionEvent) { panic("transport failed") },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := agent.RunTurn(context.Background(), TurnInput{RunID: "run-callback", Text: "hello"}); err == nil {
		t.Fatal("event callback panic did not become an error")
	}
}

func TestHookPanicIsContained(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	registry := NewCapabilityRegistry()
	if err := registry.Register(product, staticTool{manifest: toolManifest("market.quote", "1.0.0"), content: "42"}); err != nil {
		t.Fatal(err)
	}
	snapshot, _ := (CapabilityResolver{Registry: registry}).Resolve(principal, user)
	agent, _ := NewAgent(AgentOptions{
		LLM: MockLlmAdapter{}, Tools: snapshot, Session: mustSession(t, user, principal),
		Hooks: &RunHooksFuncs{OnRunStartFn: func(context.Context, RunInfo) error { panic("hook panic") }},
	})
	result, err := agent.RunTurn(context.Background(), TurnInput{RunID: "run-hook-panic", Text: "hello"})
	if err == nil || result.Status != RunFailed || !strings.Contains(err.Error(), "hook") {
		t.Fatalf("hook panic was not contained: %#v %v", result, err)
	}
}

func TestAgentRejectsInvalidLimitsAndNilContext(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	registry := NewCapabilityRegistry()
	if err := registry.Register(product, staticTool{manifest: toolManifest("limit.tool", "1.0.0"), content: "ok"}); err != nil {
		t.Fatal(err)
	}
	snapshot, _ := (CapabilityResolver{Registry: registry}).Resolve(principal, user)
	for _, options := range []AgentOptions{
		{LLM: MockLlmAdapter{}, Tools: snapshot, Session: mustSession(t, user, principal), MaxSteps: -1},
		{LLM: MockLlmAdapter{}, Tools: snapshot, Session: mustSession(t, user, principal), MaxToolCalls: HardMaxToolCalls + 1},
	} {
		if _, err := NewAgent(options); err == nil {
			t.Fatal("invalid agent limits were accepted")
		}
	}
	agent, _ := NewAgent(AgentOptions{LLM: MockLlmAdapter{}, Tools: snapshot, Session: mustSession(t, user, principal)})
	var ctx context.Context
	if _, err := agent.RunTurn(ctx, TurnInput{RunID: "run-nil-context", Text: "hello"}); err == nil {
		t.Fatal("nil run context was accepted")
	}
}

func TestRunTokenUsageOverflowIsRejected(t *testing.T) {
	total := TokenUsage{InputTokens: MaxReportedTokensPerRun}
	if err := addTokenUsage(&total, TokenUsage{InputTokens: 1}); err == nil {
		t.Fatal("run token usage overflow was accepted")
	}
}

func TestRunEndHookCannotBlockTerminalResult(t *testing.T) {
	oldTimeout := terminalHookTimeout
	terminalHookTimeout = 10 * time.Millisecond
	t.Cleanup(func() { terminalHookTimeout = oldTimeout })
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	registry := NewCapabilityRegistry()
	if err := registry.Register(product, staticTool{manifest: toolManifest("market.quote", "1.0.0"), content: "42"}); err != nil {
		t.Fatal(err)
	}
	snapshot, _ := (CapabilityResolver{Registry: registry}).Resolve(principal, user)
	hooks := &RunHooksFuncs{OnRunEndFn: func(ctx context.Context, _ RunInfo, _ RunStatus) { <-ctx.Done() }}
	agent, err := NewAgent(AgentOptions{LLM: MockLlmAdapter{}, Tools: snapshot, Session: mustSession(t, user, principal), Hooks: hooks})
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if _, err := agent.RunTurn(context.Background(), TurnInput{RunID: "run-hook", Text: "hello"}); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > 200*time.Millisecond {
		t.Fatalf("terminal hook blocked the run for %s", elapsed)
	}
}

type scriptedTurn struct {
	text  string
	calls []ToolCall
	usage *TokenUsage
}

// scriptedAdapter replays one step per call, then answers with plain text.
type scriptedAdapter struct {
	steps []scriptedTurn
	index int
}

func (s *scriptedAdapter) Provider() string { return "scripted" }

func (s *scriptedAdapter) Stream(_ context.Context, _ GenerateOptions, emit func(StreamChunk)) error {
	if s.index >= len(s.steps) {
		s.index++
		emit(StreamChunk{Kind: "assistant", Text: "done"})
		emit(StreamChunk{Kind: "finish", FinishKind: "stop"})
		return nil
	}
	step := s.steps[s.index]
	s.index++
	if step.text != "" {
		emit(StreamChunk{Kind: "assistant", Text: step.text})
	}
	for i := range step.calls {
		call := step.calls[i]
		emit(StreamChunk{Kind: "assistant", ToolCall: &call, ToolCalls: []ToolCall{call}})
	}
	finish := FinishStop
	if len(step.calls) > 0 {
		finish = FinishToolCalls
	}
	emit(StreamChunk{Kind: StreamKindFinish, FinishKind: finish, Usage: step.usage})
	return nil
}

func TestAgentLoopExecutesMultipleToolCallsPerStep(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	registry := NewCapabilityRegistry()
	if err := registry.Register(product, staticTool{manifest: toolManifest("market.quote", "1.0.0"), content: "quote-result"}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(product, staticTool{manifest: toolManifest("market.candles", "1.0.0"), content: "candles-result"}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := (CapabilityResolver{Registry: registry}).Resolve(principal, user)
	if err != nil {
		t.Fatal(err)
	}
	llm := &scriptedAdapter{steps: []scriptedTurn{{
		calls: []ToolCall{
			{ID: "call-1", Name: "market.quote", Args: map[string]any{}},
			{ID: "call-2", Name: "market.candles", Args: map[string]any{}},
		},
		usage: &TokenUsage{InputTokens: 12, OutputTokens: 8},
	}}}
	session := mustSession(t, user, principal)
	agent, err := NewAgent(AgentOptions{
		LLM: llm, Tools: snapshot, Session: session, MaxSteps: 3,
		ProfileSnapshotID: "profile", CapabilitySnapshotID: "caps",
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := agent.RunTurn(context.Background(), TurnInput{RunID: "run-a", Text: "look"})
	if err != nil || result.Status != RunCompleted {
		t.Fatalf("unexpected result: %#v, %v", result, err)
	}
	events := session.Events()
	toolCalls, toolResults := 0, 0
	sawUsage := false
	for _, event := range events {
		switch event.Type {
		case EvToolCall:
			toolCalls++
		case EvToolResult:
			toolResults++
		case EvRunUsage:
			sawUsage = true
		}
	}
	if toolCalls != 2 || toolResults != 2 {
		t.Fatalf("expected 2 calls and 2 results, got %d/%d: %#v", toolCalls, toolResults, events)
	}
	if !sawUsage {
		t.Fatal("run/usage event missing")
	}
	if result.Answer != "done" {
		t.Fatalf("unexpected final answer: %q", result.Answer)
	}
}

func TestAgentRunTurnResetsRunScopedStateWhenReused(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	registry := NewCapabilityRegistry()
	manifest := toolManifest("market.quote", "1.0.0")
	manifest.PerTurnBudget = 1
	if err := registry.Register(product, staticTool{manifest: manifest, content: "quote-result"}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := (CapabilityResolver{Registry: registry}).Resolve(principal, user)
	if err != nil {
		t.Fatal(err)
	}

	llm := &scriptedAdapter{steps: []scriptedTurn{
		{calls: []ToolCall{{ID: "call-a", Name: "market.quote", Args: map[string]any{}}}, usage: &TokenUsage{InputTokens: 10, OutputTokens: 4}},
		{text: "first answer", usage: &TokenUsage{InputTokens: 2, OutputTokens: 1}},
		{calls: []ToolCall{{ID: "call-b", Name: "market.quote", Args: map[string]any{}}}, usage: &TokenUsage{InputTokens: 3, OutputTokens: 2}},
		{text: "second answer", usage: &TokenUsage{InputTokens: 1, OutputTokens: 1}},
	}}
	var toolRunIDs []string
	hooks := &RunHooksFuncs{OnBeforeToolFn: func(_ context.Context, info RunInfo, _ ToolCall) error {
		toolRunIDs = append(toolRunIDs, info.RunID)
		return nil
	}}
	session := mustSession(t, user, principal)
	agent, err := NewAgent(AgentOptions{
		LLM: llm, Tools: snapshot, Session: session, MaxSteps: 3, Hooks: hooks,
	})
	if err != nil {
		t.Fatal(err)
	}

	first, err := agent.RunTurn(context.Background(), TurnInput{RunID: "run-a", Text: "first"})
	if err != nil || first.Status != RunCompleted || first.Answer != "first answer" {
		t.Fatalf("unexpected first result: %#v, %v", first, err)
	}
	second, err := agent.RunTurn(context.Background(), TurnInput{RunID: "run-b", Text: "second"})
	if err != nil || second.Status != RunCompleted || second.Answer != "second answer" {
		t.Fatalf("unexpected second result: %#v, %v", second, err)
	}

	usageByRun := make(map[string]RunUsageData)
	var secondToolResult ToolResultData
	for _, event := range session.Events() {
		switch event.Type {
		case EvRunUsage:
			var usage RunUsageData
			if err := json.Unmarshal(event.Data, &usage); err != nil {
				t.Fatal(err)
			}
			usageByRun[event.RunID] = usage
		case EvToolResult:
			if event.RunID == "run-b" {
				if err := json.Unmarshal(event.Data, &secondToolResult); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	if got := usageByRun["run-a"]; got.InputTokens != 12 || got.OutputTokens != 5 {
		t.Fatalf("unexpected first-run usage: %#v", got)
	}
	if got := usageByRun["run-b"]; got.InputTokens != 4 || got.OutputTokens != 3 {
		t.Fatalf("second-run usage leaked from first run: %#v", got)
	}
	if !secondToolResult.OK || secondToolResult.Content != "quote-result" {
		t.Fatalf("second run inherited the first run's tool budget: %#v", secondToolResult)
	}
	if len(toolRunIDs) != 2 || toolRunIDs[0] != "run-a" || toolRunIDs[1] != "run-b" {
		t.Fatalf("guarded tool runtime observed stale run ids: %#v", toolRunIDs)
	}
	if agent.runID != "run-b" || agent.counts["market.quote"] != 1 || agent.usage.InputTokens != 4 || agent.usage.OutputTokens != 3 {
		t.Fatalf("agent retained incorrect run-scoped state: run=%q counts=%#v usage=%#v", agent.runID, agent.counts, agent.usage)
	}
}

func TestStreamChunksPersistDeltasButNotModelHistory(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	registry := NewCapabilityRegistry()
	if err := registry.Register(product, staticTool{manifest: toolManifest("market.quote", "1.0.0"), content: "42"}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := (CapabilityResolver{Registry: registry}).Resolve(principal, user)
	if err != nil {
		t.Fatal(err)
	}
	llm := &scriptedAdapter{steps: []scriptedTurn{{text: "he"}}}
	session := mustSession(t, user, principal)
	agent, _ := NewAgent(AgentOptions{
		LLM: &streamingTextAdapter{next: llm}, Tools: snapshot, Session: session,
		StreamChunks: true,
	})
	agent.runID = "run-a"
	if _, err := agent.RunTurn(context.Background(), TurnInput{RunID: "run-a", Text: "hi"}); err != nil {
		t.Fatal(err)
	}
	chunks := 0
	for _, event := range session.Events() {
		if event.Type == EvAssistantChunk {
			chunks++
		}
	}
	if chunks != 2 {
		t.Fatalf("expected 2 assistant/chunk events, got %d", chunks)
	}
	messages, err := session.DeriveMessages()
	if err != nil {
		t.Fatal(err)
	}
	// Chunk events are transport fidelity only: the projected model history
	// still contains exactly one user message and one complete assistant text.
	if len(messages) != 2 || messages[1].Content != "he" {
		t.Fatalf("model history polluted by chunk events: %#v", messages)
	}
}

// streamingTextAdapter splits one scripted text step into per-character deltas.
type streamingTextAdapter struct {
	next *scriptedAdapter
}

func (s *streamingTextAdapter) Provider() string { return s.next.Provider() }

func (s *streamingTextAdapter) Stream(ctx context.Context, opts GenerateOptions, emit func(StreamChunk)) error {
	step := s.next.steps[s.next.index%len(s.next.steps)]
	s.next.index++
	for _, char := range step.text {
		emit(StreamChunk{Kind: "assistant", Text: string(char)})
	}
	emit(StreamChunk{Kind: "finish", FinishKind: "stop"})
	return nil
}
