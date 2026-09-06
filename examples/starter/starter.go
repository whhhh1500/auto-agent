// Package starter assembles a deliberately small, product-neutral server that
// deployers can copy before replacing its profile and connector definitions.
package starter

import (
	"context"
	"fmt"
	"net/http"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/execution"
	"github.com/cc-auto-agent/harness-core/pkg/server"
)

const (
	ProfileID        = "starter.assistant"
	HTTPCapabilityID = "starter.http.get"
	defaultHTTPURL   = "https://example.com/"
)

// Config supplies only deployment seams. It deliberately does not add product
// policy, accounts, or credentials to the foundation server.
type Config struct {
	LLM          core.LlmAdapter
	Model        string
	HTTPURL      string
	HTTPExecutor core.Executor
	Sessions     core.SessionStore
}

// NewServer returns a runnable development assembly. The caller owns its HTTP
// listener and may replace every infrastructure dependency through Config.
func NewServer(config Config) (*server.Server, error) {
	if config.LLM == nil {
		return nil, fmt.Errorf("starter LLM is required")
	}
	if config.Model == "" {
		return nil, fmt.Errorf("starter model is required")
	}
	if config.HTTPURL == "" {
		config.HTTPURL = defaultHTTPURL
	}
	if config.HTTPExecutor == nil {
		config.HTTPExecutor = execution.HTTPExecutor{}
	}
	if config.Sessions == nil {
		config.Sessions = core.NewMemorySessionStore()
	}

	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	deployment, err := global.Child(core.ScopeRef{Kind: core.ScopeDeployment, ID: "starter"})
	if err != nil {
		return nil, err
	}
	product, err := deployment.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "starter"})
	if err != nil {
		return nil, err
	}
	capabilities := core.NewCapabilityRegistry()
	profiles := core.NewAgentProfileRegistry()
	if err := Register(capabilities, profiles, product, config.Model, config.HTTPURL, config.HTTPExecutor); err != nil {
		return nil, err
	}
	runtime := &core.Runtime{
		Capabilities: capabilities,
		Profiles:     profiles,
		Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) {
			return config.LLM, nil
		}),
	}
	return server.New(server.Config{
		Runtime: runtime, Sessions: config.Sessions,
		Authenticator: server.HeaderAuthenticator{
			Root: product.Segments(), DefaultGrants: core.NewPermissionSet(core.PermRead),
		},
	})
}

// Register installs one read-only public HTTP example and one profile. The
// connector has no credentials, accepts no model arguments, and uses the HTTP
// executor's public-network and redirect protections by default.
func Register(capabilities *core.CapabilityRegistry, profiles *core.AgentProfileRegistry, scope core.ScopePath, model, endpoint string, executor core.Executor) error {
	if capabilities == nil || profiles == nil {
		return fmt.Errorf("starter registries are required")
	}
	if model == "" || endpoint == "" || executor == nil {
		return fmt.Errorf("starter model, HTTP endpoint, and executor are required")
	}
	manifest := core.CapabilityManifest{
		ID: HTTPCapabilityID, Version: "1.0.0", Name: "Read public HTTP example",
		Description: "Read a fixed public example endpoint. It sends no credentials or user-provided arguments.",
		Kind:        core.KindConnector, Contract: "harness.tool/v1", Idempotent: true,
		RequiredPermissions: []core.Permission{core.PermRead},
		Tool: &core.ToolExposure{Parameters: map[string]any{
			"type": "object", "properties": map[string]any{}, "additionalProperties": false,
		}},
		Execution: &core.ExecutionSpec{Runtime: "http", Entrypoint: endpoint, Method: http.MethodGet},
	}
	if err := capabilities.RegisterExecutor(scope, manifest, executor); err != nil {
		return err
	}
	name := "Starter Assistant"
	description := "A minimal OpenAI-compatible assistant with one safe read-only HTTP connector."
	selection := core.ModelSelection{Provider: "openai", Model: model}
	return profiles.Bind(core.AgentProfileLayer{
		Scope: scope, ProfileID: ProfileID, Name: &name, Description: &description, Model: &selection,
		AddCapabilities: []string{HTTPCapabilityID},
		PutFragments: []core.PromptFragment{{
			ID: "starter.instructions", Section: core.PromptInstructions,
			Content: "Answer directly when possible. Use the HTTP example only when it helps answer the request.",
		}},
	})
}
