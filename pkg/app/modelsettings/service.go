package modelsettings

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	appidentity "github.com/whhhh1500/auto-agent/pkg/app/identity"
	"github.com/whhhh1500/auto-agent/pkg/app/modelcatalog"
	"github.com/whhhh1500/auto-agent/pkg/app/secretview"
)

const (
	maxModelBytes      = 256
	maxIdentifierBytes = 256
	maxBaseURLBytes    = 4096
	maxAPIKeyBytes     = 4096
)

// Service validates and merges one authoritative database-backed LLM record.
type Service struct {
	repository Repository
	observer   PostSaveObserver
	policy     modelcatalog.View
}

var _ UseCases = (*Service)(nil)

// NewService constructs the model-settings application service. The optional
// observer is notified only after Repository.Save succeeds. Keeping it
// variadic preserves existing NewService(repository) call sites.
func NewService(repository Repository, observers ...PostSaveObserver) (*Service, error) {
	return newService(repository, nil, observers...)
}
func NewServiceWithCatalog(repository Repository, policy modelcatalog.View, observers ...PostSaveObserver) (*Service, error) {
	if policy == nil {
		return nil, fmt.Errorf("model settings service requires a model catalog")
	}
	return newService(repository, policy, observers...)
}
func newService(repository Repository, policy modelcatalog.View, observers ...PostSaveObserver) (*Service, error) {
	if repository == nil {
		return nil, fmt.Errorf("model settings service requires a repository")
	}
	if len(observers) > 1 {
		return nil, fmt.Errorf("model settings service accepts at most one post-save observer")
	}
	service := &Service{repository: repository, policy: policy}
	if len(observers) == 1 {
		service.observer = observers[0]
	}
	return service, nil
}

func (service *Service) Get(ctx context.Context, command GetCommand) (GetResult, error) {
	if err := requireAdmin(command.Actor); err != nil {
		return GetResult{}, err
	}
	configuration, found, err := service.repository.Load(ctx)
	if err != nil {
		return GetResult{}, err
	}
	if !found {
		// A missing record is still presented as an actionable configuration
		// surface. These are compatibility defaults only; no provider is
		// constructed or selected here.
		result := GetResult{AllowedModels: []string{}}
		if service.policy != nil {
			ds := service.policy.Defaults()
			if len(ds) > 0 {
				result.Provider = ProviderID(ds[0].Provider.ID)
				result.Protocol = ProtocolID(ds[0].Protocol.ID)
			}
		}
		if result.Provider == "" {
			result.Provider, result.Protocol = ProviderOpenAI, ProtocolOpenAIChatCompletions
		}
		result.Executable, result.ExecutionStatus = service.executionStatus(result.Provider, result.Protocol)
		if !result.Executable {
			result.ExecutionReason = result.ExecutionStatus
		}
		return result, nil
	}
	configuration, err = NormalizeStoredConfiguration(configuration)
	if err != nil {
		return GetResult{}, err
	}
	result := GetResult{
		Found: found, BaseURL: configuration.BaseURL, Model: configuration.Model,
		Provider: configuration.Provider, Protocol: configuration.Protocol,
		MaxTokens:     configuration.MaxTokens,
		AllowedModels: append([]string{}, configuration.AllowedModels...),
		HasAPIKey:     configuration.APIKey != "", APIKeyPreview: previewIfPresent(configuration.APIKey),
		ConfigSource: configuration.Source,
	}
	result.Executable, result.ExecutionStatus = service.executionStatus(result.Provider, result.Protocol)
	if !result.Executable {
		result.ExecutionReason = result.ExecutionStatus
	}
	return result, nil
}

func (service *Service) executionStatus(provider ProviderID, protocol ProtocolID) (bool, string) {
	if service.policy == nil {
		return true, "legacy"
	}
	kp, kq, ok := service.policy.Assess(string(provider), string(protocol))
	if kp && kq && ok {
		return true, "available"
	}
	if kp && kq {
		return false, "incompatible"
	}
	return false, "external_plugin_unavailable"
}

func (service *Service) Put(ctx context.Context, command PutCommand) error {
	if err := requireAdmin(command.Actor); err != nil {
		return err
	}
	if command.APIKey != nil && command.ClearAPIKey {
		return invalidInput("api_key and clear_api_key cannot be used together")
	}
	if command.APIKey != nil {
		if err := validateAPIKey(*command.APIKey); err != nil {
			return err
		}
	}
	if err := validateModel(command.Model); err != nil {
		return err
	}
	if err := validateBaseURL(command.BaseURL); err != nil {
		return err
	}
	if command.MaxTokensPresent && command.MaxTokens <= 0 {
		return invalidInput("max_tokens must be a positive integer when supplied")
	}

	current, found, err := service.repository.Load(ctx)
	if err != nil {
		return err
	}
	if found {
		current, err = NormalizeStoredConfiguration(current)
		if err != nil {
			return err
		}
	}
	provider, protocol, err := service.resolveProviderProtocol(current, found, command.Provider, command.Protocol)
	if err != nil {
		return err
	}
	allowed := current.AllowedModels
	maxTokens := current.MaxTokens
	if command.AllowedModelsPresent {
		allowed, err = NormalizeAllowedModels(command.AllowedModels)
		if err != nil {
			return err
		}
	}
	if command.MaxTokensPresent {
		maxTokens = command.MaxTokens
	}
	if len(allowed) > 0 && !contains(allowed, command.Model) {
		return invalidInput("model %q is not in allowed_models", command.Model)
	}

	apiKey := current.APIKey
	switch {
	case command.ClearAPIKey:
		apiKey = ""
	case command.APIKey != nil:
		apiKey = *command.APIKey
	case !found:
		return invalidInput("api_key is required for an initial model settings configuration")
	}
	persisted := StoredConfiguration{
		BaseURL: command.BaseURL, APIKey: apiKey, Model: command.Model, MaxTokens: maxTokens,
		Provider: provider, Protocol: protocol, AllowedModels: allowed, Source: ConfigSourceDB,
	}
	if err := service.repository.Save(ctx, persisted); err != nil {
		return err
	}
	if service.observer == nil {
		return nil
	}
	if err := service.observer.AfterSave(ctx); err != nil {
		return fmt.Errorf("refresh model settings after save: %w", err)
	}
	return nil
}

// NormalizeStoredConfiguration validates and normalizes an authoritative
// persisted record. It is shared by repository adapters and application reads
// so malformed or unsafe database state never reaches an API view or runtime.
func NormalizeStoredConfiguration(configuration StoredConfiguration) (StoredConfiguration, error) {
	if configuration.Source == "" {
		// The source field is additive. A record written before it existed is a
		// durable database configuration, not an invitation to re-read env.
		configuration.Source = ConfigSourceDB
	}
	provider, protocol, err := normalizeProviderProtocol(configuration.Provider, configuration.Protocol)
	if err != nil {
		return StoredConfiguration{}, invalidConfiguration("%v", err)
	}
	configuration.Provider, configuration.Protocol = provider, protocol
	if !configuration.Source.Valid() {
		return StoredConfiguration{}, invalidConfiguration("config_source %q is invalid", configuration.Source)
	}
	if err := validateBaseURLValue(configuration.BaseURL); err != nil {
		return StoredConfiguration{}, invalidConfiguration("%v", err)
	}
	if err := validateModelValue(configuration.Model); err != nil {
		return StoredConfiguration{}, invalidConfiguration("%v", err)
	}
	if configuration.APIKey != "" {
		if err := validateAPIKeyValue(configuration.APIKey); err != nil {
			return StoredConfiguration{}, invalidConfiguration("%v", err)
		}
	}
	if configuration.MaxTokens < 0 {
		return StoredConfiguration{}, invalidConfiguration("max_tokens must not be negative")
	}
	allowed, err := normalizeAllowedModels(configuration.AllowedModels)
	if err != nil {
		return StoredConfiguration{}, invalidConfiguration("%v", err)
	}
	if len(allowed) > 0 && !contains(allowed, configuration.Model) {
		return StoredConfiguration{}, invalidConfiguration("model %q is not in allowed_models", configuration.Model)
	}
	configuration.AllowedModels = allowed
	return configuration, nil
}

func resolveProviderProtocol(current StoredConfiguration, found bool, requestedProvider *ProviderID, requestedProtocol *ProtocolID) (ProviderID, ProtocolID, error) {
	provider := current.Provider
	if !found || requestedProvider != nil {
		provider = ProviderOpenAI
	}
	if requestedProvider != nil {
		provider = *requestedProvider
	}
	protocol := current.Protocol
	switch {
	case requestedProtocol != nil:
		protocol = *requestedProtocol
	case requestedProvider != nil:
		var known bool
		protocol, known = defaultProtocol(provider)
		if !known {
			return "", "", invalidInput("protocol is required when changing to an unknown provider")
		}
	case !found:
		protocol = ProtocolOpenAIChatCompletions
	}
	provider, protocol, err := normalizeProviderProtocol(provider, protocol)
	if err != nil {
		return "", "", invalidInput("%v", err)
	}
	return provider, protocol, nil
}

func normalizeProviderProtocol(provider ProviderID, protocol ProtocolID) (ProviderID, ProtocolID, error) {
	if provider == "" {
		provider = ProviderOpenAI
	}
	if protocol == "" {
		var known bool
		protocol, known = defaultProtocol(provider)
		if !known {
			return "", "", fmt.Errorf("protocol is required for unknown provider")
		}
	}
	if err := validateIdentifier(string(provider), "provider"); err != nil {
		return "", "", err
	}
	if err := validateIdentifier(string(protocol), "protocol"); err != nil {
		return "", "", err
	}
	if builtInIncompatible(provider, protocol) {
		return "", "", fmt.Errorf("provider %q is incompatible with protocol %q", provider, protocol)
	}
	return provider, protocol, nil
}

func (service *Service) resolveProviderProtocol(current StoredConfiguration, found bool, requestedProvider *ProviderID, requestedProtocol *ProtocolID) (ProviderID, ProtocolID, error) {
	if service.policy == nil {
		return resolveProviderProtocol(current, found, requestedProvider, requestedProtocol)
	}
	provider := current.Provider
	if !found || requestedProvider != nil {
		provider = ""
	}
	if requestedProvider != nil {
		provider = *requestedProvider
	}
	protocol := current.Protocol
	if requestedProtocol != nil {
		protocol = *requestedProtocol
	} else if !found || requestedProvider != nil {
		protocol = ""
	}
	return service.normalizeProviderProtocol(provider, protocol)
}
func (service *Service) normalizeProviderProtocol(provider ProviderID, protocol ProtocolID) (ProviderID, ProtocolID, error) {
	if service.policy == nil {
		return normalizeProviderProtocol(provider, protocol)
	}
	if provider == "" {
		ds := service.policy.Defaults()
		if len(ds) == 0 {
			return "", "", fmt.Errorf("provider is required")
		}
		provider = ProviderID(ds[0].Provider.ID)
	}
	if protocol == "" {
		defaults := service.policy.Defaults()
		for _, d := range defaults {
			if d.Provider.ID == string(provider) {
				protocol = ProtocolID(d.Protocol.ID)
				break
			}
		}
		if protocol == "" {
			return "", "", fmt.Errorf("protocol is required for provider %q", provider)
		}
	}
	if err := validateIdentifier(string(provider), "provider"); err != nil {
		return "", "", err
	}
	if err := validateIdentifier(string(protocol), "protocol"); err != nil {
		return "", "", err
	}
	knownProvider, knownProtocol, compatible := service.policy.Assess(string(provider), string(protocol))
	if knownProvider && knownProtocol && !compatible {
		return "", "", fmt.Errorf("provider %q is incompatible with protocol %q", provider, protocol)
	}
	return provider, protocol, nil
}

func defaultProtocol(provider ProviderID) (ProtocolID, bool) {
	switch provider {
	case ProviderOpenAI:
		return ProtocolOpenAIChatCompletions, true
	case ProviderAnthropic:
		return ProtocolAnthropicMessages, true
	default:
		return "", false
	}
}

func builtInIncompatible(provider ProviderID, protocol ProtocolID) bool {
	switch protocol {
	case ProtocolOpenAIChatCompletions, ProtocolOpenAIResponses:
		return provider == ProviderAnthropic
	case ProtocolAnthropicMessages:
		return provider == ProviderOpenAI
	default:
		return false
	}
}

// NormalizeAllowedModels trims, validates, de-duplicates, and deterministically
// sorts a deployment model allow-list. An empty list intentionally means no
// additional policy restriction.
func NormalizeAllowedModels(values []string) ([]string, error) {
	result, err := normalizeAllowedModels(values)
	if err != nil {
		return nil, invalidInput("%v", err)
	}
	return result, nil
}

func normalizeAllowedModels(values []string) ([]string, error) {
	unique := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if err := validateModelValue(value); err != nil {
			return nil, err
		}
		unique[value] = struct{}{}
	}
	result := make([]string, 0, len(unique))
	for value := range unique {
		result = append(result, value)
	}
	sort.Strings(result)
	return result, nil
}

func requireAdmin(actor appidentity.AdminActor) error {
	if actor.Role != appidentity.RoleAdmin {
		return classifiedError{kind: ErrForbidden, cause: fmt.Errorf("platform admin role required")}
	}
	return nil
}

func validateModel(value string) error {
	if err := validateModelValue(value); err != nil {
		return invalidInput("%v", err)
	}
	return nil
}

func validateBaseURL(value string) error {
	if err := validateBaseURLValue(value); err != nil {
		return invalidInput("%v", err)
	}
	return nil
}

func validateAPIKey(value string) error {
	if err := validateAPIKeyValue(value); err != nil {
		return invalidInput("%v", err)
	}
	return nil
}

func validateModelValue(value string) error {
	if strings.TrimSpace(value) == "" || len(value) > maxModelBytes || containsControl(value) {
		return fmt.Errorf("model is empty, too long, or contains control characters")
	}
	return nil
}

func validateIdentifier(value, label string) error {
	if value == "" || len(value) > maxIdentifierBytes || !utf8.ValidString(value) {
		return fmt.Errorf("%s is empty, too long, or invalid UTF-8", label)
	}
	for _, character := range value {
		if unicode.IsSpace(character) || unicode.IsControl(character) || unicode.Is(unicode.Cf, character) || unicode.Is(unicode.Cs, character) || unicode.Is(unicode.Co, character) {
			return fmt.Errorf("%s contains unsafe whitespace or Unicode", label)
		}
	}
	return nil
}

func validateBaseURLValue(value string) error {
	if value == "" {
		return nil
	}
	if len(value) > maxBaseURLBytes || containsControl(value) {
		return fmt.Errorf("base_url is too long or contains control characters")
	}
	parsed, err := url.Parse(value)
	if err != nil || !strings.EqualFold(parsed.Scheme, "http") && !strings.EqualFold(parsed.Scheme, "https") || parsed.Hostname() == "" || parsed.User != nil {
		return fmt.Errorf("base_url must be an http or https URL with a hostname and without userinfo")
	}
	return nil
}

func validateAPIKeyValue(value string) error {
	if value == "" || len(value) > maxAPIKeyBytes || containsControl(value) || strings.TrimSpace(value) != value {
		return fmt.Errorf("api_key must be non-empty, bounded, free of control characters, and without leading or trailing whitespace")
	}
	return nil
}

func containsControl(value string) bool {
	for _, character := range value {
		if unicode.IsControl(character) {
			return true
		}
	}
	return false
}

func contains(values []string, needle string) bool {
	for _, value := range values {
		if value == needle {
			return true
		}
	}
	return false
}

func previewIfPresent(value string) string {
	if value == "" {
		return ""
	}
	return secretview.Preview(value)
}

func invalidInput(format string, values ...any) error {
	return classifiedError{kind: ErrInvalidInput, cause: fmt.Errorf(format, values...)}
}

func invalidConfiguration(format string, values ...any) error {
	return classifiedError{kind: ErrInvalidConfiguration, cause: fmt.Errorf(format, values...)}
}

type classifiedError struct {
	kind  error
	cause error
}

func (err classifiedError) Error() string { return err.cause.Error() }
func (err classifiedError) Unwrap() error { return err.kind }
