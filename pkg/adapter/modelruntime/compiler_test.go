package modelruntime

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cc-auto-agent/harness-core/pkg/app/modelcatalog"
	"github.com/cc-auto-agent/harness-core/pkg/app/modelcontrol"
	"github.com/cc-auto-agent/harness-core/pkg/app/modelexecution"
	appmodelsettings "github.com/cc-auto-agent/harness-core/pkg/app/modelsettings"
	"github.com/cc-auto-agent/harness-core/pkg/core"
)

func TestBuiltinCompilerRunsEverySupportedProtocolAndCachesExactConfiguration(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/chat/completions":
			if got := r.Header.Get("Authorization"); got != "Bearer test-key" {
				t.Errorf("openai auth=%q", got)
			}
			_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"chat"}}],"usage":{"prompt_tokens":1,"completion_tokens":2}}`))
		case "/v1/responses":
			_, _ = w.Write([]byte(`{"id":"resp_1","status":"completed","output":[{"id":"msg_1","type":"message","content":[{"type":"output_text","text":"responses"}]}],"usage":{"input_tokens":1,"output_tokens":2}}`))
		case "/v1/messages":
			if got := r.Header.Get("x-api-key"); got != "test-key" {
				t.Errorf("anthropic auth=%q", got)
			}
			_, _ = w.Write([]byte(`{"content":[{"type":"text","text":"anthropic"}],"usage":{"input_tokens":1,"output_tokens":2},"stop_reason":"end_turn"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	plugins, err := NewBuiltinPluginRegistry()
	if err != nil {
		t.Fatal(err)
	}
	compiler, err := NewCompiler(CompilerOptions{Plugins: plugins})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name     string
		provider appmodelsettings.ProviderID
		protocol appmodelsettings.ProtocolID
		want     string
	}{
		{"chat", appmodelsettings.ProviderOpenAI, appmodelsettings.ProtocolOpenAIChatCompletions, "chat"},
		{"responses", appmodelsettings.ProviderOpenAI, appmodelsettings.ProtocolOpenAIResponses, "responses"},
		{"anthropic", appmodelsettings.ProviderAnthropic, appmodelsettings.ProtocolAnthropicMessages, "anthropic"},
	} {
		t.Run(test.name, func(t *testing.T) {
			configuration := appmodelsettings.StoredConfiguration{BaseURL: server.URL + "/v1", APIKey: "test-key", Model: "model-a", Provider: test.provider, Protocol: test.protocol}
			if test.provider == appmodelsettings.ProviderAnthropic {
				configuration.BaseURL = server.URL
			}
			adapter, err := compiler.Resolve(context.Background(), configuration, core.ModelSelection{Provider: string(test.provider), Model: "model-a"})
			if err != nil {
				t.Fatal(err)
			}
			var text strings.Builder
			if err := adapter.Stream(context.Background(), core.GenerateOptions{Messages: []core.ChatMessage{{Role: core.RoleUser, Content: "hello"}}}, func(chunk core.StreamChunk) { text.WriteString(chunk.Text) }); err != nil {
				t.Fatal(err)
			}
			if text.String() != test.want {
				t.Fatalf("text=%q want %q", text.String(), test.want)
			}
			second, err := compiler.Resolve(context.Background(), configuration, core.ModelSelection{Provider: string(test.provider), Model: "model-a"})
			if err != nil || second != adapter {
				t.Fatalf("cache adapter=%p second=%p err=%v", adapter, second, err)
			}
		})
	}
}

func TestCompilerFailsClosedAndReplacesChangedConfiguration(t *testing.T) {
	plugins, err := NewBuiltinPluginRegistry()
	if err != nil {
		t.Fatal(err)
	}
	compiler, err := NewCompiler(CompilerOptions{Plugins: plugins, DefaultBaseURLs: map[appmodelsettings.ProviderID]string{appmodelsettings.ProviderOpenAI: "https://api.openai.com/v1", appmodelsettings.ProviderAnthropic: "https://api.anthropic.com"}})
	if err != nil {
		t.Fatal(err)
	}
	configuration := appmodelsettings.StoredConfiguration{APIKey: "key-one", Model: "model-a", Provider: appmodelsettings.ProviderOpenAI, Protocol: appmodelsettings.ProtocolOpenAIChatCompletions}
	first, err := compiler.Resolve(context.Background(), configuration, core.ModelSelection{Provider: "openai", Model: "model-a"})
	if err != nil {
		t.Fatal(err)
	}
	configuration.APIKey = "key-two"
	second, err := compiler.Resolve(context.Background(), configuration, core.ModelSelection{Provider: "openai", Model: "model-a"})
	firstRevision := first.(core.ArtifactRevisioner).ArtifactRevision()
	secondRevision := second.(core.ArtifactRevisioner).ArtifactRevision()
	if err != nil || first == second || firstRevision == secondRevision {
		t.Fatalf("changed configuration did not replace exact snapshot first=%p second=%p err=%v", first, second, err)
	}
	for name, selection := range map[string]core.ModelSelection{
		"provider": {Provider: "anthropic", Model: "model-a"}, "model": {Provider: "openai", Model: "other"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := compiler.Resolve(context.Background(), configuration, selection); err == nil {
				t.Fatal("mismatched selection accepted")
			}
		})
	}
	unknown := configuration
	unknown.Provider, unknown.Protocol = "vendor", "vendor-wire"
	if _, err := compiler.Resolve(context.Background(), unknown, core.ModelSelection{Provider: "vendor", Model: "model-a"}); err == nil {
		t.Fatal("unknown plugin accepted")
	}
	empty := configuration
	empty.APIKey = ""
	if _, err := compiler.Resolve(context.Background(), empty, core.ModelSelection{Provider: "openai", Model: "model-a"}); err == nil {
		t.Fatal("empty key accepted")
	}
}

func TestResolverAndCompilerAreConcurrentAndNoConfigurationFallback(t *testing.T) {
	plugins, err := NewBuiltinPluginRegistry()
	if err != nil {
		t.Fatal(err)
	}
	compiler, err := NewCompiler(CompilerOptions{Plugins: plugins, DefaultBaseURLs: map[appmodelsettings.ProviderID]string{appmodelsettings.ProviderOpenAI: "https://api.openai.com/v1"}})
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := NewResolver(func(context.Context) (appmodelsettings.StoredConfiguration, bool, error) {
		return appmodelsettings.StoredConfiguration{APIKey: "key", Model: "model-a", Provider: appmodelsettings.ProviderOpenAI, Protocol: appmodelsettings.ProtocolOpenAIChatCompletions}, true, nil
	}, compiler)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := resolver.ResolveModel(context.Background(), core.ModelSelection{Provider: "openai", Model: "model-a"})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	emptyResolver, err := NewResolver(func(context.Context) (appmodelsettings.StoredConfiguration, bool, error) {
		return appmodelsettings.StoredConfiguration{}, false, nil
	}, compiler)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := emptyResolver.ResolveModel(context.Background(), core.ModelSelection{Provider: "openai", Model: "model-a"}); err == nil {
		t.Fatal("absent configuration fell back")
	}
	if _, err := NewResolver(nil, compiler); err == nil {
		t.Fatal("nil source accepted")
	}
}

func TestCompilerBuildsOneOpaqueSnapshotAndContainsPluginFailures(t *testing.T) {
	var providerBuilds, protocolBuilds atomic.Int32
	providers := []ProviderPlugin{{ID: "vendor", Version: "1", ImplementationRevision: "vendor-provider-v1", Build: func(input ProviderBuildInput) (modelexecution.ProviderRegistration, error) {
		providerBuilds.Add(1)
		return modelexecution.ProviderRegistration{Binding: input.Binding, Factory: func() (modelexecution.Provider, error) { return runtimeTestProvider{}, nil }}, nil
	}}}
	protocols := []ProtocolPlugin{{ID: "vendor-wire", Version: "1", ImplementationRevision: "vendor-protocol-v1", Build: func(input ProtocolBuildInput) (modelexecution.ProtocolRegistration, error) {
		protocolBuilds.Add(1)
		return modelexecution.ProtocolRegistration{Binding: input.Binding, Factory: func() (modelexecution.Protocol, error) { return runtimeTestProtocol{}, nil }}, nil
	}}}
	plugins, err := NewPluginRegistryWithPolicy(providers, protocols, []modelcatalog.Compatibility{{Provider: modelcatalog.Ref{ID: "vendor", Version: "1"}, Protocol: modelcatalog.Ref{ID: "vendor-wire", Version: "1"}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	compiler, err := NewCompiler(CompilerOptions{Plugins: plugins})
	if err != nil {
		t.Fatal(err)
	}
	configuration := appmodelsettings.StoredConfiguration{BaseURL: "https://private.endpoint.test/v1", APIKey: "private-model-secret", Model: "model-a", Provider: "vendor", Protocol: "vendor-wire"}
	var wg sync.WaitGroup
	for range 24 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := compiler.Resolve(context.Background(), configuration, core.ModelSelection{Provider: "vendor", Model: "model-a"}); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if providerBuilds.Load() != 1 || protocolBuilds.Load() != 1 {
		t.Fatalf("concurrent builds provider=%d protocol=%d", providerBuilds.Load(), protocolBuilds.Load())
	}
	adapter, err := compiler.Resolve(context.Background(), configuration, core.ModelSelection{Provider: "vendor", Model: "model-a"})
	if err != nil {
		t.Fatal(err)
	}
	revision := adapter.(core.ArtifactRevisioner).ArtifactRevision()
	for _, forbidden := range []string{"private.endpoint.test", "private-model-secret"} {
		if strings.Contains(revision, forbidden) {
			t.Fatalf("artifact revision leaked %q: %s", forbidden, revision)
		}
	}
	panicPlugins, err := NewPluginRegistry([]ProviderPlugin{{ID: "panic-vendor", Version: "1", ImplementationRevision: "p1", Build: func(ProviderBuildInput) (modelexecution.ProviderRegistration, error) { panic("private-secret") }}}, protocols)
	if err != nil {
		t.Fatal(err)
	}
	panicCompiler, err := NewCompiler(CompilerOptions{Plugins: panicPlugins})
	if err != nil {
		t.Fatal(err)
	}
	panicConfig := configuration
	panicConfig.Provider = "panic-vendor"
	if _, err := panicCompiler.Resolve(context.Background(), panicConfig, core.ModelSelection{Provider: "panic-vendor", Model: "model-a"}); err == nil || strings.Contains(err.Error(), "private-secret") {
		t.Fatalf("builder panic error=%v", err)
	}
	if _, err := NewPluginRegistry([]ProviderPlugin{{ID: "bad provider", Version: "1", ImplementationRevision: "v", Build: providers[0].Build}}, protocols); err == nil {
		t.Fatal("unsafe plugin ID accepted")
	}
}

type runtimeTestProvider struct{}

func (runtimeTestProvider) Send(context.Context, modelcontrol.ProviderPlan, modelexecution.OutboundRequest) (modelexecution.InboundResponse, error) {
	return modelexecution.InboundResponse{}, nil
}

type runtimeTestProtocol struct{}

func (runtimeTestProtocol) Execute(_ context.Context, _ modelexecution.Request, _ modelexecution.Provider, emit modelexecution.Emit) error {
	return emit(modelexecution.Event{Kind: modelexecution.EventFinish, Finish: modelexecution.FinishStop})
}
