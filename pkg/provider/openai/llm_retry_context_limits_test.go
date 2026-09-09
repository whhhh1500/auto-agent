package openai

import (
	"context"
	"sync"
	"testing"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

func TestRetryLlmAdapterModelContextLimits(t *testing.T) {
	tests := []struct {
		name    string
		adapter *RetryLlmAdapter
		window  int
		output  int
	}{
		{name: "nil retry adapter"},
		{name: "nil next adapter", adapter: &RetryLlmAdapter{}},
		{name: "missing next limits", adapter: &RetryLlmAdapter{Next: retryNoContextLimitsAdapter{}}},
		{name: "valid next limits", adapter: &RetryLlmAdapter{Next: &retryContextLimitsAdapter{window: 128000, output: 4096}}, window: 128000, output: 4096},
		{name: "next panic", adapter: &RetryLlmAdapter{Next: &retryContextLimitsAdapter{panic: true}}},
		{name: "zero window", adapter: &RetryLlmAdapter{Next: &retryContextLimitsAdapter{window: 0, output: 4096}}},
		{name: "negative window", adapter: &RetryLlmAdapter{Next: &retryContextLimitsAdapter{window: -1, output: 4096}}},
		{name: "zero output", adapter: &RetryLlmAdapter{Next: &retryContextLimitsAdapter{window: 128000, output: 0}}},
		{name: "negative output", adapter: &RetryLlmAdapter{Next: &retryContextLimitsAdapter{window: 128000, output: -1}}},
		{name: "output equals window", adapter: &RetryLlmAdapter{Next: &retryContextLimitsAdapter{window: 4096, output: 4096}}},
		{name: "output exceeds window", adapter: &RetryLlmAdapter{Next: &retryContextLimitsAdapter{window: 4095, output: 4096}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.adapter == nil {
				var nilAdapter *RetryLlmAdapter
				window, output := nilAdapter.ModelContextLimits()
				if window != 0 || output != 0 {
					t.Fatalf("nil limits=(%d,%d), want zero fallback", window, output)
				}
				return
			}
			window, output := test.adapter.ModelContextLimits()
			if window != test.window || output != test.output {
				t.Fatalf("limits=(%d,%d), want (%d,%d)", window, output, test.window, test.output)
			}
		})
	}
}

func TestRetryLlmAdapterForwardsContextLimitsToRuntimeAssembler(t *testing.T) {
	inner := &retryContextLimitsAdapter{window: 128000, output: 4096}
	retry := &RetryLlmAdapter{Next: inner}
	if got, want := retry.ArtifactRevision(), "retry/v2/retry-context-limits/v1"; got != want {
		t.Fatalf("ArtifactRevision()=%q, want %q", got, want)
	}

	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	user, err := global.Child(core.ScopeRef{Kind: core.ScopeUser, ID: "user"})
	if err != nil {
		t.Fatal(err)
	}
	sessionScope, err := user.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "retry-limits-session"})
	if err != nil {
		t.Fatal(err)
	}
	principal := core.Principal{TenantID: "tenant", SubjectID: "user", Scope: user}
	session, err := core.NewSession(core.SessionOptions{ID: "retry-limits-session", ProfileID: "retry.limits", Principal: principal, Scope: sessionScope})
	if err != nil {
		t.Fatal(err)
	}
	steps, toolCalls := 1, 1
	selection := core.ModelSelection{Provider: "retry-test", Model: "limits"}
	profiles := core.NewAgentProfileRegistry()
	if err := profiles.Bind(core.AgentProfileLayer{Scope: global, ProfileID: "retry.limits", Model: &selection, MaxSteps: &steps, MaxToolCalls: &toolCalls}); err != nil {
		t.Fatal(err)
	}
	recorder := &retryContextLimitsRecorder{}
	runtime := &core.Runtime{
		Capabilities: core.NewCapabilityRegistry(), Profiles: profiles,
		Models:           core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) { return retry, nil }),
		ContextAssembler: recorder.assemble,
	}
	result, err := runtime.RunTurn(context.Background(), principal, session, core.TurnInput{RunID: "run-retry-context-limits", Text: "hello"}, nil)
	if err != nil || result.Status != core.RunCompleted {
		t.Fatalf("runtime result=%#v err=%v", result, err)
	}
	requests := recorder.requests()
	if len(requests) != 1 {
		t.Fatalf("context assembly calls=%d, want one", len(requests))
	}
	if request := requests[0]; request.ContextWindowTokens != 128000 || request.MaxOutputTokens != 4096 {
		t.Fatalf("context limits=(%d,%d), want inner (128000,4096)", request.ContextWindowTokens, request.MaxOutputTokens)
	}
}

type retryNoContextLimitsAdapter struct{}

func (retryNoContextLimitsAdapter) Provider() string { return "retry-limits-test" }
func (retryNoContextLimitsAdapter) Stream(context.Context, core.GenerateOptions, func(core.StreamChunk)) error {
	return nil
}

type retryContextLimitsAdapter struct {
	window int
	output int
	panic  bool
}

func (*retryContextLimitsAdapter) Provider() string         { return "retry-limits-test" }
func (*retryContextLimitsAdapter) ArtifactRevision() string { return "retry-context-limits/v1" }
func (a *retryContextLimitsAdapter) ModelContextLimits() (int, int) {
	if a.panic {
		panic("context limits panic")
	}
	return a.window, a.output
}
func (*retryContextLimitsAdapter) Stream(_ context.Context, _ core.GenerateOptions, emit func(core.StreamChunk)) error {
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: "ok"})
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
	return nil
}

type retryContextLimitsRecorder struct {
	mu       sync.Mutex
	contexts []core.ModelContext
}

func (r *retryContextLimitsRecorder) assemble(_ context.Context, request core.ModelContext) (core.ModelContext, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.contexts = append(r.contexts, request)
	return request, nil
}

func (r *retryContextLimitsRecorder) requests() []core.ModelContext {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]core.ModelContext(nil), r.contexts...)
}
