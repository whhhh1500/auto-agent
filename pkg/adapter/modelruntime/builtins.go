package modelruntime

import (
	"context"
	"fmt"

	"github.com/whhhh1500/auto-agent/pkg/adapter/modelexecution/anthropic"
	adapteropenai "github.com/whhhh1500/auto-agent/pkg/adapter/modelexecution/openai"
	"github.com/whhhh1500/auto-agent/pkg/app/modelcatalog"
	"github.com/whhhh1500/auto-agent/pkg/app/modelcontrol"
	"github.com/whhhh1500/auto-agent/pkg/app/modelexecution"
	appmodelsettings "github.com/whhhh1500/auto-agent/pkg/app/modelsettings"
)

const (
	builtinVersion                  = "1"
	openAIProviderImplementation    = "openai-http-v1"
	anthropicProviderImplementation = "anthropic-http-v1"
	openAIChatImplementation        = "openai-chat-completions-v3-wire-tool-names"
	openAIResponsesImplementation   = "openai-responses-v3-batched-tool-calls"
	anthropicMessagesImplementation = "anthropic-messages-v1"
)

// NewBuiltinPluginRegistry explicitly constructs the supported local HTTP
// integrations. It is not a global default: applications can create their own
// registry, add a third-party builder, and publish it through a new Compiler.
func NewBuiltinPluginRegistry() (*PluginRegistry, error) {
	providers := []ProviderPlugin{
		{ID: appmodelsettings.ProviderOpenAI, Version: builtinVersion, ImplementationRevision: openAIProviderImplementation, Build: buildOpenAIProvider},
		{ID: appmodelsettings.ProviderAnthropic, Version: builtinVersion, ImplementationRevision: anthropicProviderImplementation, Build: buildAnthropicProvider},
	}
	protocols := []ProtocolPlugin{
		{ID: appmodelsettings.ProtocolOpenAIChatCompletions, Version: builtinVersion, ImplementationRevision: openAIChatImplementation, Build: buildOpenAIChat},
		{ID: appmodelsettings.ProtocolOpenAIResponses, Version: builtinVersion, ImplementationRevision: openAIResponsesImplementation, Build: buildOpenAIResponses},
		{ID: appmodelsettings.ProtocolAnthropicMessages, Version: builtinVersion, ImplementationRevision: anthropicMessagesImplementation, Build: buildAnthropicMessages},
	}
	compat := []modelcatalog.Compatibility{
		{Provider: modelcatalog.Ref{ID: "openai", Version: "1"}, Protocol: modelcatalog.Ref{ID: "openai-chat-completions", Version: "1"}},
		{Provider: modelcatalog.Ref{ID: "openai", Version: "1"}, Protocol: modelcatalog.Ref{ID: "openai-responses", Version: "1"}},
		{Provider: modelcatalog.Ref{ID: "anthropic", Version: "1"}, Protocol: modelcatalog.Ref{ID: "anthropic-messages", Version: "1"}},
	}
	defaults := []modelcatalog.Default{
		{Provider: modelcatalog.Ref{ID: "openai", Version: "1"}, Protocol: modelcatalog.Ref{ID: "openai-chat-completions", Version: "1"}},
		{Provider: modelcatalog.Ref{ID: "anthropic", Version: "1"}, Protocol: modelcatalog.Ref{ID: "anthropic-messages", Version: "1"}},
	}
	return NewPluginRegistryWithPolicy(providers, protocols, compat, defaults)
}

func buildOpenAIProvider(input ProviderBuildInput) (modelexecution.ProviderRegistration, error) {
	resolver := newCredentialResolver(input.APIKey)
	provider, err := adapteropenai.NewHTTPProvider(adapteropenai.HTTPProviderConfig{Endpoint: input.Endpoint, BaseURL: input.BaseURL, Credentials: resolver})
	if err != nil {
		resolver.Clear()
		return modelexecution.ProviderRegistration{}, err
	}
	return modelexecution.ProviderRegistration{Binding: input.Binding, Factory: func() (modelexecution.Provider, error) { return provider, nil }}, nil
}
func buildAnthropicProvider(input ProviderBuildInput) (modelexecution.ProviderRegistration, error) {
	resolver := newCredentialResolver(input.APIKey)
	registration, err := anthropic.NewHTTPProviderRegistration(input.Binding, anthropic.HTTPProviderConfig{Endpoint: input.Endpoint, BaseURL: input.BaseURL, Credentials: resolver})
	if err != nil {
		resolver.Clear()
		return modelexecution.ProviderRegistration{}, err
	}
	return registration, nil
}
func buildOpenAIChat(input ProtocolBuildInput) (modelexecution.ProtocolRegistration, error) {
	if input.MaxTokens < 0 {
		return modelexecution.ProtocolRegistration{}, fmt.Errorf("openai chat max tokens is invalid")
	}
	protocol := adapteropenai.ChatCompletionsProtocol{MaxTokens: input.MaxTokens}
	return modelexecution.ProtocolRegistration{Binding: input.Binding, Factory: func() (modelexecution.Protocol, error) { return protocol, nil }}, nil
}
func buildOpenAIResponses(input ProtocolBuildInput) (modelexecution.ProtocolRegistration, error) {
	return adapteropenai.NewResponsesProtocolRegistration(input.Binding, input.MaxTokens)
}
func buildAnthropicMessages(input ProtocolBuildInput) (modelexecution.ProtocolRegistration, error) {
	return anthropic.NewMessagesProtocolRegistration(input.Binding, input.MaxTokens)
}

// credentialResolver keeps a private byte copy for a compiled snapshot.
// Resolve returns a second copy, which modelexecution providers clear after the
// HTTP request. The snapshot copy is intentionally retained until no adapter
// can reference it; Compiler does not clear replaced snapshots without a
// reference-counted lifecycle.
type credentialResolver struct{ value []byte }

func newCredentialResolver(value string) *credentialResolver {
	return &credentialResolver{value: append([]byte(nil), value...)}
}
func (r *credentialResolver) ResolveCredential(ctx context.Context, ref modelcontrol.CredentialRef) (modelexecution.CredentialMaterial, error) {
	if r == nil || ctx == nil || ref.ID == "" || ref.Revision == "" || len(r.value) == 0 {
		return modelexecution.CredentialMaterial{}, fmt.Errorf("model runtime credential resolution rejected")
	}
	return modelexecution.NewCredentialMaterial(r.value), nil
}
func (r *credentialResolver) Clear() {
	if r != nil {
		clear(r.value)
		r.value = nil
	}
}
