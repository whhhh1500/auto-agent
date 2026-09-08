package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/whhhh1500/auto-agent/pkg/adapter/modelruntime"
	"github.com/whhhh1500/auto-agent/pkg/app/modelcatalog"
	appmodelsettings "github.com/whhhh1500/auto-agent/pkg/app/modelsettings"
	core "github.com/whhhh1500/auto-agent/pkg/core"
	openai "github.com/whhhh1500/auto-agent/pkg/provider/openai"
)

type modelRuntimeBundle struct {
	Resolver core.ModelResolver
	Catalog  modelcatalog.View
}

func newModelRuntimeBundle(repository appmodelsettings.Repository) modelRuntimeBundle {
	plugins, err := modelruntime.NewBuiltinPluginRegistry()
	if err != nil {
		return modelRuntimeBundle{Resolver: modelResolverError{err: err}}
	}
	compiler, err := modelruntime.NewCompiler(modelruntime.CompilerOptions{Plugins: plugins, DefaultBaseURLs: map[appmodelsettings.ProviderID]string{appmodelsettings.ProviderOpenAI: "https://api.openai.com/v1", appmodelsettings.ProviderAnthropic: "https://api.anthropic.com"}})
	if err != nil {
		return modelRuntimeBundle{Resolver: modelResolverError{err: err}}
	}
	resolver, err := modelruntime.NewResolver(func(ctx context.Context) (appmodelsettings.StoredConfiguration, bool, error) {
		c, e := loadOrImportLegacyLLMConfiguration(ctx, repository)
		return c, true, e
	}, compiler)
	if err != nil {
		return modelRuntimeBundle{Resolver: modelResolverError{err: err}}
	}
	return modelRuntimeBundle{Resolver: core.ModelResolverFunc(func(ctx context.Context, selection core.ModelSelection) (core.LlmAdapter, error) {
		if selection.Provider == "mock" {
			return core.MockLlmAdapter{}, nil
		}
		return resolver.ResolveModel(ctx, selection)
	}), Catalog: plugins}
}

// newLegacyModelResolver retains its historical construction name while using
// the modelruntime compiler. The environment import policy stays here at the
// composition root; provider and protocol execution stay in adapters.
func newLegacyModelResolver(repository appmodelsettings.Repository) core.ModelResolver {
	plugins, err := modelruntime.NewBuiltinPluginRegistry()
	if err != nil {
		return modelResolverError{err: err}
	}
	compiler, err := modelruntime.NewCompiler(modelruntime.CompilerOptions{
		Plugins: plugins,
		DefaultBaseURLs: map[appmodelsettings.ProviderID]string{
			appmodelsettings.ProviderOpenAI:    "https://api.openai.com/v1",
			appmodelsettings.ProviderAnthropic: "https://api.anthropic.com",
		},
	})
	if err != nil {
		return modelResolverError{err: err}
	}
	resolver, err := modelruntime.NewResolver(func(ctx context.Context) (appmodelsettings.StoredConfiguration, bool, error) {
		configuration, err := loadOrImportLegacyLLMConfiguration(ctx, repository)
		if err != nil {
			return appmodelsettings.StoredConfiguration{}, false, err
		}
		return configuration, true, nil
	}, compiler)
	if err != nil {
		return modelResolverError{err: err}
	}
	return core.ModelResolverFunc(func(ctx context.Context, selection core.ModelSelection) (core.LlmAdapter, error) {
		if selection.Provider == "mock" {
			return core.MockLlmAdapter{}, nil
		}
		return resolver.ResolveModel(ctx, selection)
	})
}

type modelResolverError struct{ err error }

func (r modelResolverError) ResolveModel(context.Context, core.ModelSelection) (core.LlmAdapter, error) {
	return nil, r.err
}

// loadOrImportLegacyLLMConfiguration makes a present database record
// authoritative. Only a wholly absent record may be imported once from the
// legacy environment contract. The optional create capability is the durable,
// cross-process first-writer-wins boundary; a local mutex is insufficient.
func loadOrImportLegacyLLMConfiguration(ctx context.Context, repository appmodelsettings.Repository) (appmodelsettings.StoredConfiguration, error) {
	configuration, found, err := loadPersistedLLMConfig(ctx, repository)
	if err != nil || found {
		return configuration, err
	}
	configuration, err = legacyEnvironmentConfiguration()
	if err != nil {
		return appmodelsettings.StoredConfiguration{}, err
	}
	creator, ok := repository.(interface {
		CreateIfAbsent(context.Context, appmodelsettings.StoredConfiguration) (bool, error)
	})
	if !ok {
		return appmodelsettings.StoredConfiguration{}, fmt.Errorf("model settings repository does not support atomic create-if-absent")
	}
	created, err := creator.CreateIfAbsent(ctx, configuration)
	if err != nil {
		return appmodelsettings.StoredConfiguration{}, fmt.Errorf("persist legacy environment LLM configuration: %w", err)
	}
	if !created {
		winner, found, err := loadPersistedLLMConfig(ctx, repository)
		if err != nil {
			return winner, err
		}
		if !found {
			return appmodelsettings.StoredConfiguration{}, fmt.Errorf("atomic create-if-absent reported an existing LLM configuration but none was found")
		}
		return winner, nil
	}
	return configuration, nil
}

// legacyEnvironmentConfiguration reads the old HARNESS_LLM_* bootstrap
// contract strictly, then turns it into the same normalized configuration the
// database adapter writes. It returns no adapter and never logs a credential.
func legacyEnvironmentConfiguration() (appmodelsettings.StoredConfiguration, error) {
	// Keep the established environment contract, including its max-token and
	// allow-list validation, before accepting the values for a durable import.
	if _, err := openai.NewOpenAIAdapterFromEnv(); err != nil {
		return appmodelsettings.StoredConfiguration{}, fmt.Errorf("legacy environment LLM configuration: %w", err)
	}
	baseURL := os.Getenv("HARNESS_LLM_BASE_URL")
	if baseURL == "" {
		baseURL = "https://api.openai.com/v1"
	}
	allowed := []string{}
	for _, value := range strings.Split(os.Getenv("HARNESS_LLM_ALLOWED_MODELS"), ",") {
		if value = strings.TrimSpace(value); value != "" {
			allowed = append(allowed, value)
		}
	}
	maxTokens := 0
	if raw := os.Getenv("HARNESS_LLM_MAX_TOKENS"); raw != "" {
		// NewOpenAIAdapterFromEnv above already enforces this legacy contract.
		// Parse again only because its public adapter does not expose its config.
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			return appmodelsettings.StoredConfiguration{}, fmt.Errorf("legacy environment LLM configuration: HARNESS_LLM_MAX_TOKENS must be a positive integer")
		}
		maxTokens = parsed
	}
	configuration, err := appmodelsettings.NormalizeStoredConfiguration(appmodelsettings.StoredConfiguration{
		BaseURL: baseURL, APIKey: os.Getenv("HARNESS_LLM_API_KEY"), Model: os.Getenv("HARNESS_LLM_MODEL"),
		MaxTokens: maxTokens, AllowedModels: allowed, Source: appmodelsettings.ConfigSourceEnvImport,
	})
	if err != nil {
		return appmodelsettings.StoredConfiguration{}, fmt.Errorf("legacy environment LLM configuration: %w", err)
	}
	return configuration, nil
}
