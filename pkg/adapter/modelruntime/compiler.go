package modelruntime

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"

	"github.com/cc-auto-agent/harness-core/pkg/adapter/modelexecution/corebridge"
	"github.com/cc-auto-agent/harness-core/pkg/app/modelcatalog"
	"github.com/cc-auto-agent/harness-core/pkg/app/modelcontrol"
	"github.com/cc-auto-agent/harness-core/pkg/app/modelexecution"
	appmodelsettings "github.com/cc-auto-agent/harness-core/pkg/app/modelsettings"
	"github.com/cc-auto-agent/harness-core/pkg/core"
)

const (
	defaultContextWindow = 128000
	defaultMaxOutput     = 16384
)

// ConfigurationSource makes database-first and one-time environment-import
// policy explicit at the composition root. A false found value is not a model
// fallback: Resolver rejects it.
type ConfigurationSource func(context.Context) (appmodelsettings.StoredConfiguration, bool, error)

// CompilerOptions supplies composition-only endpoint defaults. Protocol and
// provider implementations never invent an endpoint URL themselves.
type CompilerOptions struct {
	Plugins         *PluginRegistry
	DefaultBaseURLs map[appmodelsettings.ProviderID]string
}

// Compiler builds and caches the sole active immutable snapshot. The cache is
// keyed by a private digest that includes secret and endpoint values but never
// exposes them; a configuration change replaces the active cache atomically.
// Old adapters deliberately remain usable for concurrent callers. Their
// in-memory credential copies cannot be cleared safely without lifetime
// tracking, while temporary byte copies are cleared during construction.
type Compiler struct {
	plugins  *PluginRegistry
	defaults map[appmodelsettings.ProviderID]string
	mu       sync.Mutex
	active   *compiled
}

type compiled struct {
	key     string
	adapter core.LlmAdapter
}

func NewCompiler(options CompilerOptions) (*Compiler, error) {
	if options.Plugins == nil {
		return nil, fmt.Errorf("model runtime compiler requires plugins")
	}
	defaults := make(map[appmodelsettings.ProviderID]string, len(options.DefaultBaseURLs))
	for id, url := range options.DefaultBaseURLs {
		if id == "" || url == "" {
			return nil, fmt.Errorf("model runtime default endpoint is invalid")
		}
		defaults[id] = url
	}
	return &Compiler{plugins: options.Plugins, defaults: defaults}, nil
}

// Resolve compiles configuration only after exact selection and allow-list
// checks. Unknown plugins, empty credentials/models, and incompatible pairs
// all fail before a request can invoke a provider.
func (c *Compiler) Resolve(ctx context.Context, configuration appmodelsettings.StoredConfiguration, selection core.ModelSelection) (core.LlmAdapter, error) {
	if c == nil || ctx == nil {
		return nil, fmt.Errorf("model runtime compiler is unavailable")
	}
	configuration, err := appmodelsettings.NormalizeStoredConfiguration(configuration)
	if err != nil {
		return nil, err
	}
	if selection.Model == "" {
		return nil, fmt.Errorf("selected model is empty")
	}
	if selection.Provider != string(configuration.Provider) {
		return nil, fmt.Errorf("selected provider %q does not match configured provider %q", selection.Provider, configuration.Provider)
	}
	if selection.Model != configuration.Model {
		return nil, fmt.Errorf("selected model %q does not match configured model %q", selection.Model, configuration.Model)
	}
	if configuration.APIKey == "" {
		return nil, fmt.Errorf("configured model credential is empty")
	}
	if len(configuration.AllowedModels) > 0 && !allowed(configuration.Model, configuration.AllowedModels) {
		return nil, fmt.Errorf("configured model is not in allowed_models")
	}
	key := configurationDigest(configuration)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.active != nil && c.active.key == key {
		return c.active.adapter, nil
	}
	// Construct while holding the narrow cache mutex. Builders are explicitly
	// required to be local/no-I/O, so this prevents concurrent first resolves
	// from duplicating clients and retained credential snapshots.
	next, err := c.compile(configuration)
	if err != nil {
		return nil, err
	}
	c.active = &compiled{key: key, adapter: next}
	return next, nil
}

func (c *Compiler) compile(configuration appmodelsettings.StoredConfiguration) (core.LlmAdapter, error) {
	providerPlugin, err := c.plugins.provider(configuration.Provider)
	if err != nil {
		return nil, err
	}
	protocolPlugin, err := c.plugins.protocol(configuration.Protocol)
	if err != nil {
		return nil, err
	}
	knownProvider, knownProtocol, compatible := c.plugins.Compatible(modelcatalog.Ref{ID: string(providerPlugin.ID), Version: providerPlugin.Version}, modelcatalog.Ref{ID: string(protocolPlugin.ID), Version: protocolPlugin.Version})
	if !knownProvider || !knownProtocol || !compatible {
		return nil, fmt.Errorf("configured model provider and protocol are incompatible")
	}
	baseURL := configuration.BaseURL
	if baseURL == "" {
		baseURL = c.defaults[configuration.Provider]
	}
	if baseURL == "" {
		return nil, fmt.Errorf("configured provider endpoint is empty")
	}
	endpointRevision, err := opaqueRevision()
	if err != nil {
		return nil, err
	}
	credentialRevision, err := opaqueRevision()
	if err != nil {
		return nil, err
	}
	compositionRevision, err := opaqueRevision()
	if err != nil {
		return nil, err
	}
	endpoint := modelcontrol.EndpointRef{ID: string(configuration.Provider) + "/endpoint", Revision: endpointRevision}
	credential := modelcontrol.CredentialRef{ID: string(configuration.Provider) + "/credential", Revision: credentialRevision}
	provider := modelcontrol.ProviderSpec{Ref: modelcontrol.Ref{ID: string(providerPlugin.ID), Version: providerPlugin.Version}, Endpoint: endpoint, ImplementationRevision: providerPlugin.ImplementationRevision}
	protocol := modelcontrol.ProtocolSpec{Ref: modelcontrol.Ref{ID: string(protocolPlugin.ID), Version: protocolPlugin.Version}, ImplementationRevision: protocolPlugin.ImplementationRevision}
	maxOutput := configuration.MaxTokens
	if maxOutput == 0 {
		maxOutput = defaultMaxOutput
	}
	catalog := modelcontrol.CatalogModel{Ref: modelcontrol.Ref{ID: configuration.Model, Version: "settings-v1"}, WireModel: configuration.Model, Provider: provider.Ref, Protocol: protocol.Ref, Credential: credential, Capabilities: modelcontrol.ModelCapabilities{ContextWindowTokens: defaultContextWindow, MaxOutputTokens: maxOutput, ToolCalls: true, Modalities: []modelcontrol.Modality{modelcontrol.ModalityText}}}
	metadata, err := modelcontrol.NewRegistry([]modelcontrol.CatalogModel{catalog}, []modelcontrol.ProviderSpec{provider}, []modelcontrol.ProtocolSpec{protocol}, []modelcontrol.Compatibility{{Provider: provider.Ref, Protocol: protocol.Ref}})
	if err != nil {
		return nil, fmt.Errorf("compile model metadata: %w", err)
	}
	plan, err := metadata.Resolve(modelcontrol.ResolveInput{Catalog: catalog.Ref, CompositionRevision: compositionRevision})
	if err != nil {
		return nil, fmt.Errorf("resolve model plan: %w", err)
	}
	providerRegistration, err := buildProvider(providerPlugin, ProviderBuildInput{Binding: plan.Provider, Endpoint: endpoint, BaseURL: baseURL, Credential: credential, APIKey: configuration.APIKey})
	if err != nil {
		return nil, fmt.Errorf("build model provider: %w", err)
	}
	protocolRegistration, err := buildProtocol(protocolPlugin, ProtocolBuildInput{Binding: plan.Protocol, MaxTokens: maxOutput})
	if err != nil {
		return nil, fmt.Errorf("build model protocol: %w", err)
	}
	execution, err := modelexecution.NewRegistry(plan.SnapshotRevision, []modelcontrol.ProviderPlan{plan}, []modelexecution.ProviderRegistration{providerRegistration}, []modelexecution.ProtocolRegistration{protocolRegistration})
	if err != nil {
		return nil, fmt.Errorf("bind model execution: %w", err)
	}
	return &corebridge.Adapter{Registry: execution, Plan: plan}, nil
}

// Resolver is a core.ModelResolver whose configuration source is supplied by
// composition. It performs no persistence, env lookup, or provider fallback.
type Resolver struct {
	source   ConfigurationSource
	compiler *Compiler
}

func NewResolver(source ConfigurationSource, compiler *Compiler) (*Resolver, error) {
	if source == nil || compiler == nil {
		return nil, fmt.Errorf("model runtime resolver is incomplete")
	}
	return &Resolver{source: source, compiler: compiler}, nil
}
func (r *Resolver) ResolveModel(ctx context.Context, selection core.ModelSelection) (core.LlmAdapter, error) {
	if r == nil || ctx == nil {
		return nil, fmt.Errorf("model runtime resolver is unavailable")
	}
	configuration, found, err := r.source(ctx)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("model settings are not configured")
	}
	return r.compiler.Resolve(ctx, configuration, selection)
}

func allowed(model string, values []string) bool {
	for _, value := range values {
		if value == model {
			return true
		}
	}
	return false
}
func opaqueRevision() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("model runtime opaque revision generation failed")
	}
	return "opaque-" + hex.EncodeToString(value[:]), nil
}
func buildProvider(plugin ProviderPlugin, input ProviderBuildInput) (registration modelexecution.ProviderRegistration, err error) {
	defer func() {
		if recover() != nil {
			registration = modelexecution.ProviderRegistration{}
			err = fmt.Errorf("model runtime provider build failed")
		}
	}()
	registration, err = plugin.Build(input)
	if err != nil {
		return modelexecution.ProviderRegistration{}, fmt.Errorf("model runtime provider build failed")
	}
	if registration.Binding != input.Binding || registration.Factory == nil {
		return modelexecution.ProviderRegistration{}, fmt.Errorf("model runtime provider build failed")
	}
	return registration, nil
}
func buildProtocol(plugin ProtocolPlugin, input ProtocolBuildInput) (registration modelexecution.ProtocolRegistration, err error) {
	defer func() {
		if recover() != nil {
			registration = modelexecution.ProtocolRegistration{}
			err = fmt.Errorf("model runtime protocol build failed")
		}
	}()
	registration, err = plugin.Build(input)
	if err != nil {
		return modelexecution.ProtocolRegistration{}, fmt.Errorf("model runtime protocol build failed")
	}
	if registration.Binding != input.Binding || registration.Factory == nil {
		return modelexecution.ProtocolRegistration{}, fmt.Errorf("model runtime protocol build failed")
	}
	return registration, nil
}
func digest(scope, value string) string {
	sum := sha256.Sum256([]byte(scope + "\x00" + value))
	return "sha256:" + hex.EncodeToString(sum[:])
}
func configurationDigest(c appmodelsettings.StoredConfiguration) string {
	return digest("model-settings", string(c.Provider)+"\x00"+string(c.Protocol)+"\x00"+c.BaseURL+"\x00"+c.APIKey+"\x00"+c.Model+"\x00"+fmt.Sprint(c.MaxTokens)+"\x00"+strings.Join(c.AllowedModels, "\x00"))
}
func clear(value []byte) {
	for i := range value {
		value[i] = 0
	}
}
