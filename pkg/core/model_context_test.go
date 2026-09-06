package core

import (
	"context"
	"errors"
	"testing"
)

type recordingContextAssembler struct {
	calls   int
	request ModelContext
	err     error
	panic   bool
}

func (a *recordingContextAssembler) assemble(_ context.Context, request ModelContext) (ModelContext, error) {
	a.calls++
	a.request = request
	if a.panic {
		panic("private assembly panic")
	}
	if a.err != nil {
		return ModelContext{}, a.err
	}
	return ModelContext{System: request.System, Messages: request.Messages, ContextWindowTokens: request.ContextWindowTokens, MaxOutputTokens: request.MaxOutputTokens}, nil
}

func TestAgentAssemblesContextOnceAfterProjectionBeforeModel(t *testing.T) {
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	assembler := &recordingContextAssembler{}
	recorder := &recordingTelemetry{}
	var received GenerateOptions
	model := streamAdapterFunc(func(_ context.Context, options GenerateOptions, emit func(StreamChunk)) error {
		received = options
		emit(StreamChunk{Kind: StreamKindAssistant, Text: "ok"})
		emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishStop})
		return nil
	})
	agent, err := NewAgent(AgentOptions{LLM: model, Tools: benchmarkToolRuntime{}, Session: mustSession(t, user, principal), ContextAssembler: assembler.assemble, System: "system", MaxSteps: 1, Telemetry: recorder})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := agent.RunTurn(context.Background(), TurnInput{RunID: "run-context", Text: "hello"}); err != nil {
		t.Fatal(err)
	}
	if assembler.calls != 1 || assembler.request.System != "system" || len(assembler.request.Messages) != 1 || received.System != "system" || len(received.Messages) != 1 {
		t.Fatalf("assembly=%#v received=%#v", assembler, received)
	}
	if assembler.request.ContextWindowTokens != conservativeContextWindowTokens || assembler.request.MaxOutputTokens != conservativeMaxOutputTokens {
		t.Fatalf("legacy limits=%#v", assembler.request)
	}
	attributes := recorder.spanAttrs[SpanModelCall][0]
	if attributes["model.context.input_bytes"] == "" || attributes["model.context.input_tokens"] == "" || attributes["model.context.dropped_groups"] == "" {
		t.Fatalf("context evidence missing from span: %#v", attributes)
	}
	for _, name := range []string{MetricModelContextInputBytes, MetricModelContextInputTokens, MetricModelContextDroppedGroups} {
		if !containsTelemetryName(recorder.counters, name) {
			t.Fatalf("context metric %s missing: %#v", name, recorder.counters)
		}
	}
}

func TestAgentContextAssemblyPanicAndErrorFailClosed(t *testing.T) {
	for name, assembler := range map[string]*recordingContextAssembler{"error": {err: errors.New("private error")}, "panic": {panic: true}} {
		t.Run(name, func(t *testing.T) {
			_, _, _, user := testScopes()
			principal := testPrincipal(user)
			agent, err := NewAgent(AgentOptions{LLM: MockLlmAdapter{}, Tools: benchmarkToolRuntime{}, Session: mustSession(t, user, principal), ContextAssembler: assembler.assemble, MaxSteps: 1})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := agent.RunTurn(context.Background(), TurnInput{RunID: "run-context-" + name, Text: "hello"}); !errors.Is(err, errModelContextAssembly) {
				t.Fatalf("assembly failure=%v", err)
			}
		})
	}
}

func TestSafeModelContextAssemblyClonesPluginResultAndChecksEvidence(t *testing.T) {
	request := ModelContext{System: "system", ContextWindowTokens: 100, MaxOutputTokens: 10, Messages: []ChatMessage{{Role: RoleUser, Content: "input"}}}
	shared := []ChatMessage{{Role: RoleAssistant, Content: "selected"}}
	got, err := safeAssembleModelContext(func(context.Context, ModelContext) (ModelContext, error) {
		return ModelContext{System: request.System, Messages: shared, ContextWindowTokens: request.ContextWindowTokens, MaxOutputTokens: request.MaxOutputTokens, InputBytes: 7, InputTokens: 3, DroppedGroups: 1}, nil
	}, context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	shared[0].Content = "plugin mutated its returned slice"
	if got.Messages[0].Content != "selected" {
		t.Fatalf("result aliases plugin memory: %#v", got.Messages)
	}

	for name, result := range map[string]ModelContext{
		"limits":  {System: request.System, Messages: shared, ContextWindowTokens: 101, MaxOutputTokens: request.MaxOutputTokens},
		"bytes":   {System: request.System, Messages: shared, ContextWindowTokens: request.ContextWindowTokens, MaxOutputTokens: request.MaxOutputTokens, InputBytes: MaxSessionEventDataBytes + 1},
		"tokens":  {System: request.System, Messages: shared, ContextWindowTokens: request.ContextWindowTokens, MaxOutputTokens: request.MaxOutputTokens, InputTokens: int64(MaxSessionEventDataBytes) + 1},
		"dropped": {System: request.System, Messages: shared, ContextWindowTokens: request.ContextWindowTokens, MaxOutputTokens: request.MaxOutputTokens, DroppedGroups: 2},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := safeAssembleModelContext(func(context.Context, ModelContext) (ModelContext, error) { return result, nil }, context.Background(), request); !errors.Is(err, errModelContextAssembly) {
				t.Fatalf("invalid result accepted: %v", err)
			}
		})
	}
}

func TestSafeModelContextAssemblyValidatesNilAssemblerRequest(t *testing.T) {
	request := ModelContext{System: "system", Messages: []ChatMessage{}, ContextWindowTokens: 10, MaxOutputTokens: 10}
	if _, err := safeAssembleModelContext(nil, context.Background(), request); !errors.Is(err, errModelContextAssembly) {
		t.Fatalf("invalid nil-assembler request accepted: %v", err)
	}
}

func TestSafeModelContextAssemblyRejectsCanceledContextWithoutAssembler(t *testing.T) {
	request := ModelContext{System: "system", Messages: []ChatMessage{}, ContextWindowTokens: 20, MaxOutputTokens: 10}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := safeAssembleModelContext(nil, ctx, request); !errors.Is(err, errModelContextAssembly) {
		t.Fatalf("canceled nil-assembler request accepted: %v", err)
	}
}
