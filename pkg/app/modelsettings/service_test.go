package modelsettings

import (
	"context"
	"errors"
	"reflect"
	"testing"

	appidentity "github.com/whhhh1500/auto-agent/pkg/app/identity"
)

func TestServiceRequiresPlatformAdminBeforeRepository(t *testing.T) {
	repository := &repositoryStub{}
	service, err := NewService(repository)
	if err != nil {
		t.Fatal(err)
	}
	actor := appidentity.AdminActor{Role: appidentity.RoleTenantAdmin}
	if _, err := service.Get(context.Background(), GetCommand{Actor: actor}); !errors.Is(err, ErrForbidden) {
		t.Fatalf("get error=%v; want forbidden", err)
	}
	if err := service.Put(context.Background(), PutCommand{Actor: actor, Model: "model", APIKey: stringPtr("key")}); !errors.Is(err, ErrForbidden) {
		t.Fatalf("put error=%v; want forbidden", err)
	}
	if repository.loads != 0 || repository.saves != 0 {
		t.Fatalf("unauthorized request reached repository: %#v", repository)
	}
}

func TestServiceMergesAPIKeyWithExplicitClearAndInitialRequirement(t *testing.T) {
	actor := appidentity.AdminActor{Role: appidentity.RoleAdmin}
	repository := &repositoryStub{}
	service, err := NewService(repository)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Put(context.Background(), PutCommand{Actor: actor, Model: "model"}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("initial omitted key error=%v; want invalid input", err)
	}
	if repository.saves != 0 {
		t.Fatal("initial omitted key persisted a configuration")
	}
	if err := service.Put(context.Background(), PutCommand{Actor: actor, Model: "model", APIKey: stringPtr("first-key")}); err != nil {
		t.Fatal(err)
	}
	if !repository.found || repository.configuration.APIKey != "first-key" {
		t.Fatalf("replacement configuration=%#v", repository.configuration)
	}
	if err := service.Put(context.Background(), PutCommand{Actor: actor, BaseURL: "https://gateway.test/v1", Model: "model"}); err != nil {
		t.Fatal(err)
	}
	if repository.configuration.APIKey != "first-key" || repository.configuration.BaseURL != "https://gateway.test/v1" {
		t.Fatalf("omitted key did not preserve existing state: %#v", repository.configuration)
	}
	if err := service.Put(context.Background(), PutCommand{Actor: actor, Model: "model", ClearAPIKey: true}); err != nil {
		t.Fatal(err)
	}
	if repository.configuration.APIKey != "" || !repository.found {
		t.Fatalf("clear did not retain authoritative inactive state: %#v", repository)
	}
	if err := service.Put(context.Background(), PutCommand{Actor: actor, Model: "model", ClearAPIKey: true}); err != nil {
		t.Fatalf("repeated clear must be idempotent: %v", err)
	}
	if err := service.Put(context.Background(), PutCommand{Actor: actor, Model: "model", APIKey: stringPtr("")}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("empty key error=%v; want invalid input", err)
	}
	if err := service.Put(context.Background(), PutCommand{Actor: actor, Model: "model", APIKey: stringPtr("new-key"), ClearAPIKey: true}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("conflicting key intent error=%v; want invalid input", err)
	}
}

func TestServicePreservesAndReplacesMaxTokensWithPresence(t *testing.T) {
	actor := appidentity.AdminActor{Role: appidentity.RoleAdmin}
	repository := &repositoryStub{found: true, configuration: StoredConfiguration{
		APIKey: "existing-key", Model: "model", MaxTokens: 128,
	}}
	service, err := NewService(repository)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Put(context.Background(), PutCommand{Actor: actor, Model: "model"}); err != nil {
		t.Fatal(err)
	}
	if repository.configuration.MaxTokens != 128 {
		t.Fatalf("omitted max_tokens=%d; want preservation", repository.configuration.MaxTokens)
	}
	if err := service.Put(context.Background(), PutCommand{Actor: actor, Model: "model", MaxTokens: 256, MaxTokensPresent: true}); err != nil {
		t.Fatal(err)
	}
	if repository.configuration.MaxTokens != 256 {
		t.Fatalf("replacement max_tokens=%d; want 256", repository.configuration.MaxTokens)
	}
	if err := service.Put(context.Background(), PutCommand{Actor: actor, Model: "model", MaxTokens: 0, MaxTokensPresent: true}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("explicit zero error=%v; want invalid input", err)
	}
	if _, err := NormalizeStoredConfiguration(StoredConfiguration{APIKey: "key", Model: "model", MaxTokens: -1}); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("negative persisted max_tokens error=%v; want invalid configuration", err)
	}
}

func TestServiceNormalizesAllowedModelsAndDoesNotLeakAPIKey(t *testing.T) {
	actor := appidentity.AdminActor{Role: appidentity.RoleAdmin}
	repository := &repositoryStub{}
	service, err := NewService(repository)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Put(context.Background(), PutCommand{
		Actor: actor, Model: "model-b", APIKey: stringPtr("pre-middle-secret-tail"),
		AllowedModels: []string{" model-b ", "model-a", "model-b"}, AllowedModelsPresent: true,
	}); err != nil {
		t.Fatal(err)
	}
	if want := []string{"model-a", "model-b"}; !reflect.DeepEqual(repository.configuration.AllowedModels, want) {
		t.Fatalf("allowed_models=%q; want %q", repository.configuration.AllowedModels, want)
	}
	if repository.configuration.Source != ConfigSourceDB {
		t.Fatalf("canonical save source=%q; want %q", repository.configuration.Source, ConfigSourceDB)
	}
	if err := service.Put(context.Background(), PutCommand{
		Actor: actor, Model: "other", APIKey: stringPtr("next-key"),
	}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("model outside stored allow-list error=%v; want invalid input", err)
	}
	result, err := service.Get(context.Background(), GetCommand{Actor: actor})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Found || !result.HasAPIKey || result.APIKeyPreview != "pre••••••••tail" || result.ConfigSource != ConfigSourceDB {
		t.Fatalf("safe result=%#v", result)
	}
	if result.APIKeyPreview == repository.configuration.APIKey {
		t.Fatal("safe result returned raw API key")
	}

	repository.configuration.APIKey = "short"
	result, err = service.Get(context.Background(), GetCommand{Actor: actor})
	if err != nil {
		t.Fatal(err)
	}
	if result.APIKeyPreview != "••••••••" {
		t.Fatalf("short key preview=%q", result.APIKeyPreview)
	}
}

func TestServiceRejectsUnsafeCommandAndPersistedConfigurationValues(t *testing.T) {
	actor := appidentity.AdminActor{Role: appidentity.RoleAdmin}
	service, err := NewService(&repositoryStub{})
	if err != nil {
		t.Fatal(err)
	}
	for name, command := range map[string]PutCommand{
		"ftp base URL":     {Actor: actor, BaseURL: "ftp://gateway.test", Model: "model", APIKey: stringPtr("key")},
		"missing URL host": {Actor: actor, BaseURL: "https:///v1", Model: "model", APIKey: stringPtr("key")},
		"URL userinfo":     {Actor: actor, BaseURL: "https://user:pass@gateway.test/v1", Model: "model", APIKey: stringPtr("key")},
		"key controls":     {Actor: actor, Model: "model", APIKey: stringPtr("key\nnewline")},
		"key whitespace":   {Actor: actor, Model: "model", APIKey: stringPtr(" key")},
	} {
		t.Run(name, func(t *testing.T) {
			if err := service.Put(context.Background(), command); !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("command error=%v; want invalid input", err)
			}
		})
	}
	for name, configuration := range map[string]StoredConfiguration{
		"URL userinfo":   {BaseURL: "https://user:pass@gateway.test/v1", APIKey: "key", Model: "model"},
		"key controls":   {APIKey: "key\rnewline", Model: "model"},
		"key whitespace": {APIKey: "key ", Model: "model"},
		"allow mismatch": {APIKey: "key", Model: "model", AllowedModels: []string{"other"}},
		"negative max":   {APIKey: "key", Model: "model", MaxTokens: -1},
		"unknown source": {APIKey: "key", Model: "model", Source: ConfigSource("untrusted")},
	} {
		t.Run("persisted "+name, func(t *testing.T) {
			if _, err := NormalizeStoredConfiguration(configuration); !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("stored configuration error=%v; want invalid configuration", err)
			}
		})
	}
	unsafeRepository := &repositoryStub{found: true, configuration: StoredConfiguration{
		BaseURL: "https://user:pass@gateway.test/v1", APIKey: "key", Model: "model",
	}}
	unsafeService, err := NewService(unsafeRepository)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := unsafeService.Get(context.Background(), GetCommand{Actor: actor}); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("unsafe persisted URL reached GET: %v", err)
	}
}

func TestNormalizeStoredConfigurationDefaultsLegacySourceToDatabase(t *testing.T) {
	configuration, err := NormalizeStoredConfiguration(StoredConfiguration{APIKey: "key", Model: "model"})
	if err != nil {
		t.Fatal(err)
	}
	if configuration.Source != ConfigSourceDB {
		t.Fatalf("legacy source=%q; want %q", configuration.Source, ConfigSourceDB)
	}
	if configuration.Provider != ProviderOpenAI || configuration.Protocol != ProtocolOpenAIChatCompletions {
		t.Fatalf("legacy provider/protocol=%q/%q; want %q/%q", configuration.Provider, configuration.Protocol, ProviderOpenAI, ProtocolOpenAIChatCompletions)
	}
	for _, source := range []ConfigSource{ConfigSourceBootstrap, ConfigSourceDB, ConfigSourceEnvImport, ConfigSourceLegacyFallback} {
		if !source.Valid() {
			t.Fatalf("documented source %q is invalid", source)
		}
	}
}

func TestServiceProviderProtocolMergeCompatibilityAndDefaults(t *testing.T) {
	actor := appidentity.AdminActor{Role: appidentity.RoleAdmin}
	repository := &repositoryStub{found: true, configuration: StoredConfiguration{
		APIKey: "existing-key", Model: "model", Provider: ProviderOpenAI, Protocol: ProtocolOpenAIResponses,
	}}
	service, err := NewService(repository)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Put(context.Background(), PutCommand{Actor: actor, Model: "model"}); err != nil {
		t.Fatal(err)
	}
	if repository.configuration.Provider != ProviderOpenAI || repository.configuration.Protocol != ProtocolOpenAIResponses {
		t.Fatalf("omitted provider/protocol did not preserve: %#v", repository.configuration)
	}
	anthropic := ProviderAnthropic
	if err := service.Put(context.Background(), PutCommand{Actor: actor, Model: "model", Provider: &anthropic}); err != nil {
		t.Fatal(err)
	}
	if repository.configuration.Provider != ProviderAnthropic || repository.configuration.Protocol != ProtocolAnthropicMessages {
		t.Fatalf("built-in provider default=%#v", repository.configuration)
	}
	unknownProvider := ProviderID("third-party")
	if err := service.Put(context.Background(), PutCommand{Actor: actor, Model: "model", Provider: &unknownProvider}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("unknown provider without protocol error=%v; want invalid input", err)
	}
	thirdPartyProtocol := ProtocolID("third-party-wire")
	if err := service.Put(context.Background(), PutCommand{Actor: actor, Model: "model", Provider: &unknownProvider, Protocol: &thirdPartyProtocol}); err != nil {
		t.Fatal(err)
	}
	if repository.configuration.Provider != unknownProvider || repository.configuration.Protocol != thirdPartyProtocol {
		t.Fatalf("third-party pair=%#v", repository.configuration)
	}

	newRepository := &repositoryStub{}
	newService, err := NewService(newRepository)
	if err != nil {
		t.Fatal(err)
	}
	responses := ProtocolOpenAIResponses
	if err := newService.Put(context.Background(), PutCommand{Actor: actor, Model: "model", APIKey: stringPtr("new-key"), Protocol: &responses}); err != nil {
		t.Fatal(err)
	}
	if newRepository.configuration.Provider != ProviderOpenAI || newRepository.configuration.Protocol != responses {
		t.Fatalf("new explicit protocol defaulted incorrectly: %#v", newRepository.configuration)
	}
	if _, err := newService.Get(context.Background(), GetCommand{Actor: actor}); err != nil {
		t.Fatal(err)
	}
}

func TestProviderProtocolValidationDoesNotCloseThirdPartyIDs(t *testing.T) {
	valid, err := NormalizeStoredConfiguration(StoredConfiguration{
		APIKey: "key", Model: "model", Provider: ProviderID("vendor-plugin"), Protocol: ProtocolID("vendor-wire-v2"),
	})
	if err != nil || valid.Provider != "vendor-plugin" || valid.Protocol != "vendor-wire-v2" {
		t.Fatalf("third-party identifiers normalized=%#v err=%v", valid, err)
	}
	for name, configuration := range map[string]StoredConfiguration{
		"incompatible openai":    {APIKey: "key", Model: "model", Provider: ProviderOpenAI, Protocol: ProtocolAnthropicMessages},
		"incompatible anthropic": {APIKey: "key", Model: "model", Provider: ProviderAnthropic, Protocol: ProtocolOpenAIResponses},
		"whitespace":             {APIKey: "key", Model: "model", Provider: ProviderID("open ai"), Protocol: ProtocolOpenAIChatCompletions},
		"format control":         {APIKey: "key", Model: "model", Provider: ProviderID("openai\u200b"), Protocol: ProtocolOpenAIChatCompletions},
		"private use":            {APIKey: "key", Model: "model", Provider: ProviderID("openai\ue000"), Protocol: ProtocolOpenAIChatCompletions},
		"control":                {APIKey: "key", Model: "model", Provider: ProviderOpenAI, Protocol: ProtocolID("chat\ncompletions")},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NormalizeStoredConfiguration(configuration); !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("configuration error=%v; want invalid configuration", err)
			}
		})
	}
}

func TestServiceGetAbsentUsesActionableProviderProtocolDefaults(t *testing.T) {
	service, err := NewService(&repositoryStub{})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Get(context.Background(), GetCommand{Actor: appidentity.AdminActor{Role: appidentity.RoleAdmin}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Found || result.Provider != ProviderOpenAI || result.Protocol != ProtocolOpenAIChatCompletions || result.AllowedModels == nil || len(result.AllowedModels) != 0 {
		t.Fatalf("absent result=%#v", result)
	}
}

func TestServiceNotifiesPostSaveObserverOnlyAfterPersistence(t *testing.T) {
	actor := appidentity.AdminActor{Role: appidentity.RoleAdmin}
	repository := &repositoryStub{}
	observer := &postSaveObserverStub{}
	service, err := NewService(repository, observer)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Put(context.Background(), PutCommand{
		Actor: actor, Model: "model-b", APIKey: stringPtr("observer-test-key"),
		AllowedModels: []string{"model-b", "model-a", "model-b"}, AllowedModelsPresent: true,
	}); err != nil {
		t.Fatal(err)
	}
	if observer.calls != 1 {
		t.Fatalf("post-save observer calls=%d; want one notification", observer.calls)
	}
	if !repository.found || repository.configuration.Model != "model-b" {
		t.Fatal("observer was called before the configuration was persisted")
	}

	observer.err = errors.New("profile refresh failed")
	if err := service.Put(context.Background(), PutCommand{Actor: actor, Model: "model-b", APIKey: stringPtr("next-observer-key")}); !errors.Is(err, observer.err) {
		t.Fatalf("observer failure=%v; want wrapped observer error", err)
	}
	if observer.calls != 2 || !repository.found || repository.configuration.APIKey != "next-observer-key" {
		t.Fatal("observer failure incorrectly rolled back or skipped the successful repository save")
	}

	failedRepository := &repositoryStub{saveErr: errors.New("save failed")}
	failedObserver := &postSaveObserverStub{}
	failedService, err := NewService(failedRepository, failedObserver)
	if err != nil {
		t.Fatal(err)
	}
	if err := failedService.Put(context.Background(), PutCommand{Actor: actor, Model: "model-b", APIKey: stringPtr("never-observed")}); !errors.Is(err, failedRepository.saveErr) {
		t.Fatalf("save failure=%v; want repository failure", err)
	}
	if failedObserver.calls != 0 {
		t.Fatalf("observer called %d times after failed persistence", failedObserver.calls)
	}
}

type repositoryStub struct {
	configuration StoredConfiguration
	found         bool
	err           error
	saveErr       error
	loads         int
	saves         int
}

func (repository *repositoryStub) Load(context.Context) (StoredConfiguration, bool, error) {
	repository.loads++
	return repository.configuration, repository.found, repository.err
}

func (repository *repositoryStub) Save(_ context.Context, configuration StoredConfiguration) error {
	repository.saves++
	if repository.saveErr != nil {
		return repository.saveErr
	}
	if repository.err != nil {
		return repository.err
	}
	repository.configuration = configuration
	repository.found = true
	return nil
}

type postSaveObserverStub struct {
	calls int
	err   error
}

func (observer *postSaveObserverStub) AfterSave(context.Context) error {
	observer.calls++
	return observer.err
}

func stringPtr(value string) *string { return &value }
