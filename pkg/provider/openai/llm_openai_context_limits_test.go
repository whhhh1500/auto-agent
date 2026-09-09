package openai

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/whhhh1500/auto-agent/pkg/app/contextassembly"
	core "github.com/whhhh1500/auto-agent/pkg/core"
)

func TestOpenAICompatibleAdapterModelContextLimitsMatchLazyCompiledPlan(t *testing.T) {
	for _, test := range []struct {
		name      string
		maxTokens int
		window    int
		output    int
	}{
		{name: "default output", window: legacyOpenAIContextWindowTokens, output: legacyOpenAIDefaultMaxOutputTokens},
		{name: "configured output", maxTokens: 4096, window: legacyOpenAIContextWindowTokens, output: 4096},
	} {
		t.Run(test.name, func(t *testing.T) {
			adapter := NewOpenAICompatibleAdapter(OpenAIAdapterConfig{
				BaseURL: "https://api.example.test", APIKey: "test-secret", Model: "wire", MaxTokens: test.maxTokens,
			})
			window, output := adapter.ModelContextLimits()
			if window != test.window || output != test.output {
				t.Fatalf("facade limits=(%d,%d), want=(%d,%d)", window, output, test.window, test.output)
			}
			if adapter.bridge != nil || adapter.bridgeErr != nil || adapter.credentials != nil {
				t.Fatalf("reading limits initialized lazy bridge or credentials: %#v", adapter)
			}

			bridge, err := adapter.m2Bridge()
			if err != nil {
				t.Fatal(err)
			}
			defer adapter.credentials.Clear()
			window, output = bridge.ModelContextLimits()
			if window != test.window || output != test.output {
				t.Fatalf("compiled plan limits=(%d,%d), want=(%d,%d)", window, output, test.window, test.output)
			}
		})
	}
}

func TestOpenAICompatibleAdapterModelContextLimitsFailClosedWithoutLazyBridge(t *testing.T) {
	var nilAdapter *OpenAICompatibleAdapter
	if window, output := nilAdapter.ModelContextLimits(); window != 0 || output != 0 {
		t.Fatalf("nil facade limits=(%d,%d), want zero fallback", window, output)
	}
	for _, maxTokens := range []int{-1, legacyOpenAIContextWindowTokens, legacyOpenAIContextWindowTokens + 1} {
		adapter := NewOpenAICompatibleAdapter(OpenAIAdapterConfig{
			BaseURL: "https://api.example.test", APIKey: "test-secret", Model: "wire", MaxTokens: maxTokens,
		})
		if window, output := adapter.ModelContextLimits(); window != 0 || output != 0 {
			t.Fatalf("max_tokens=%d limits=(%d,%d), want zero fallback", maxTokens, window, output)
		}
		if adapter.bridge != nil || adapter.bridgeErr != nil || adapter.credentials != nil {
			t.Fatalf("max_tokens=%d reading limits initialized lazy state", maxTokens)
		}
		if _, err := adapter.m2Bridge(); err == nil || adapter.credentials != nil {
			t.Fatalf("max_tokens=%d invalid plan err=%v credentials=%#v", maxTokens, err, adapter.credentials)
		}
	}
}

func TestOpenAICompatibleAdapterForwardsConfiguredLimitsThroughRuntimeAssembler(t *testing.T) {
	requestLog := &openAIContextLimitRequestLog{}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var payload struct {
			MaxTokens int `json:"max_tokens"`
		}
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		requestLog.addWire(payload.MaxTokens)
		writer.Header().Set("Content-Type", "text/event-stream")
		_, _ = writer.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1}}\n\ndata: [DONE]\n\n"))
	}))
	defer server.Close()

	adapter := NewOpenAICompatibleAdapter(OpenAIAdapterConfig{BaseURL: server.URL, APIKey: "test-secret", Model: "wire", MaxTokens: 4096})
	assembler, err := contextassembly.NewAssembler(contextassembly.Config{})
	if err != nil {
		t.Fatal(err)
	}
	recorder := &openAIContextAssemblyRecorder{next: assembler.AssembleModelContext}

	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	user, err := global.Child(core.ScopeRef{Kind: core.ScopeUser, ID: "user"})
	if err != nil {
		t.Fatal(err)
	}
	sessionScope, err := user.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "openai-context-limits-session"})
	if err != nil {
		t.Fatal(err)
	}
	principal := core.Principal{TenantID: "tenant", SubjectID: "user", Scope: user}
	session, err := core.NewSession(core.SessionOptions{ID: "openai-context-limits-session", ProfileID: "openai.context.limits", Principal: principal, Scope: sessionScope})
	if err != nil {
		t.Fatal(err)
	}
	steps, toolCalls := 1, 1
	selection := core.ModelSelection{Provider: "openai-compatible", Model: "wire"}
	profiles := core.NewAgentProfileRegistry()
	if err := profiles.Bind(core.AgentProfileLayer{Scope: global, ProfileID: "openai.context.limits", Model: &selection, MaxSteps: &steps, MaxToolCalls: &toolCalls}); err != nil {
		t.Fatal(err)
	}
	runtime := &core.Runtime{
		Capabilities: core.NewCapabilityRegistry(),
		Profiles:     profiles,
		Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) {
			return adapter, nil
		}),
		ContextAssembler: recorder.assemble,
	}
	result, err := runtime.RunTurn(context.Background(), principal, session, core.TurnInput{RunID: "run-openai-context-limits", Text: "hello"}, nil)
	if err != nil || result.Status != core.RunCompleted {
		t.Fatalf("runtime result=%#v err=%v", result, err)
	}
	if adapter.credentials != nil {
		defer adapter.credentials.Clear()
	}
	requests, results := recorder.snapshot()
	if len(requests) != 1 || len(results) != 1 {
		t.Fatalf("context assembly calls=(%d,%d), want one", len(requests), len(results))
	}
	if request := requests[0]; request.ContextWindowTokens != legacyOpenAIContextWindowTokens || request.MaxOutputTokens != 4096 {
		t.Fatalf("runtime context limits=(%d,%d), want=(%d,4096)", request.ContextWindowTokens, request.MaxOutputTokens, legacyOpenAIContextWindowTokens)
	}
	if assembled := results[0]; assembled.InputBytes <= 0 || assembled.InputTokens <= 0 || assembled.ContextWindowTokens != legacyOpenAIContextWindowTokens || assembled.MaxOutputTokens != 4096 {
		t.Fatalf("assembled context=%#v", assembled)
	}
	if calls, maxTokens := requestLog.snapshot(); calls != 1 || maxTokens != 4096 {
		t.Fatalf("wire calls=%d max_tokens=%d, want=(1,4096)", calls, maxTokens)
	}
}

type openAIContextAssemblyRecorder struct {
	mu       sync.Mutex
	next     core.ModelContextAssembler
	requests []core.ModelContext
	results  []core.ModelContext
}

func (r *openAIContextAssemblyRecorder) assemble(ctx context.Context, request core.ModelContext) (core.ModelContext, error) {
	result, err := r.next(ctx, request)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.requests = append(r.requests, request)
	if err == nil {
		r.results = append(r.results, result)
	}
	return result, err
}

func (r *openAIContextAssemblyRecorder) snapshot() ([]core.ModelContext, []core.ModelContext) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]core.ModelContext(nil), r.requests...), append([]core.ModelContext(nil), r.results...)
}

type openAIContextLimitRequestLog struct {
	mu        sync.Mutex
	calls     int
	maxTokens int
}

func (l *openAIContextLimitRequestLog) addWire(maxTokens int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls++
	l.maxTokens = maxTokens
}

func (l *openAIContextLimitRequestLog) snapshot() (int, int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.calls, l.maxTokens
}
