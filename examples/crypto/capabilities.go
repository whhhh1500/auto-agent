// Package cryptoexample demonstrates product, tenant and user composition
// without adding cryptocurrency concepts to the harness runtime.
package cryptoexample

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

const (
	CapabilityQuote    = "crypto.market.quote"
	CapabilityDiscover = "crypto.token.discover"
	CapabilityAnalyze  = "crypto.token.analyze"
	ProfileAnalyst     = "crypto.agent.analyst"
	ProfileAvatar      = "crypto.agent.avatar"
)

// MarketQuote is the provider-neutral output used by the example market feed.
type MarketQuote struct {
	PriceUSD     float64 `json:"price_usd"`
	Change24HPct float64 `json:"change_24h_pct"`
}

type quoteCapability struct {
	source string
	quotes map[string]MarketQuote
}

func (quoteCapability) Manifest() core.CapabilityManifest {
	return core.CapabilityManifest{
		ID: CapabilityQuote, Version: "1.0.0", Name: "Market quote",
		Description: "Read a current cryptocurrency market quote from the configured provider.",
		Kind:        core.KindConnector, Contract: "core.tool/v1",
		RequiredPermissions: []core.Permission{core.PermRead}, PerTurnBudget: 8,
		Tool: &core.ToolExposure{Parameters: objectSchema(map[string]any{
			"symbol": map[string]any{"type": "string", "description": "Token symbol, for example BTC"},
		})},
	}
}

func (p quoteCapability) Execute(_ context.Context, request core.CapabilityRequest) (core.CapabilityResult, error) {
	symbol := upperArg(request.Args, "symbol", "BTC")
	quote, ok := p.quotes[symbol]
	if !ok {
		return jsonResult(map[string]any{
			"symbol": symbol, "source": p.source, "status": "not_available_in_demo_provider",
		})
	}
	return jsonResult(map[string]any{
		"symbol": symbol, "source": p.source,
		"price_usd": quote.PriceUSD, "change_24h_pct": quote.Change24HPct,
		"observed_at": time.Now().UTC().Format(time.RFC3339),
	})
}

type discoverCapability struct{}

func (discoverCapability) Manifest() core.CapabilityManifest {
	return core.CapabilityManifest{
		ID: CapabilityDiscover, Version: "1.0.0", Name: "Discover new tokens",
		Description: "Find recently listed tokens from configured live listing sources.",
		Kind:        core.KindConnector, Contract: "core.tool/v1",
		RequiredPermissions: []core.Permission{core.PermRead}, PerTurnBudget: 4,
		Tool: &core.ToolExposure{Parameters: objectSchema(map[string]any{
			"chain": map[string]any{"type": "string", "description": "Optional chain filter"},
		})},
	}
}

func (discoverCapability) Execute(_ context.Context, request core.CapabilityRequest) (core.CapabilityResult, error) {
	chain := stringArg(request.Args, "chain", "all")
	return jsonResult(map[string]any{
		"chain": chain,
		"tokens": []map[string]any{
			{"symbol": "NOVA", "chain": "solana", "age_hours": 18, "liquidity_usd": 920000},
			{"symbol": "LUMEN", "chain": "base", "age_hours": 31, "liquidity_usd": 510000},
		},
		"observed_at": time.Now().UTC().Format(time.RFC3339),
		"notice":      "demo data; replace this provider with a live listing connector",
	})
}

type analyzeCapability struct{}

func (analyzeCapability) Manifest() core.CapabilityManifest {
	return core.CapabilityManifest{
		ID: CapabilityAnalyze, Version: "1.0.0", Name: "Analyze token",
		Description: "Produce a structured analysis from market, tokenomics, contract, and on-chain inputs.",
		Kind:        core.KindSkill, Contract: "core.tool/v1",
		RequiredPermissions: []core.Permission{core.PermRead}, PerTurnBudget: 4,
		Tool: &core.ToolExposure{Parameters: objectSchema(map[string]any{
			"symbol": map[string]any{"type": "string"},
		})},
	}
}

func (analyzeCapability) Execute(_ context.Context, request core.CapabilityRequest) (core.CapabilityResult, error) {
	symbol := upperArg(request.Args, "symbol", "BTC")
	return jsonResult(map[string]any{
		"symbol": symbol,
		"dimensions": map[string]any{
			"liquidity": "medium", "holder_concentration": "unknown", "contract_risk": "requires_on_chain_provider",
		},
		"risk_level": "unverified",
		"disclaimer": "This capability returns structured analysis, not financial advice.",
	})
}

// RegisterProductCapabilities installs reusable product-level providers.
func RegisterProductCapabilities(registry *core.CapabilityRegistry, scope core.ScopePath) error {
	for _, capability := range []core.Capability{
		quoteCapability{
			source: "demo-product-market-feed",
			quotes: map[string]MarketQuote{
				"BTC": {PriceUSD: 100000, Change24HPct: 1.2},
				"ETH": {PriceUSD: 4000, Change24HPct: -0.4},
				"SOL": {PriceUSD: 200, Change24HPct: 2.6},
			},
		},
		discoverCapability{}, analyzeCapability{},
	} {
		if err := registry.Register(scope, capability); err != nil {
			return err
		}
	}
	return nil
}

// RegisterTenantMarketData replaces the product market feed for one tenant.
func RegisterTenantMarketData(
	registry *core.CapabilityRegistry,
	scope core.ScopePath,
	source string,
	quotes map[string]MarketQuote,
) error {
	provider := quoteCapability{source: source, quotes: quotes}
	return registry.Bind(core.CapabilityBinding{
		Scope: scope, Mode: core.BindingReplace, Manifest: provider.Manifest(), Provider: provider,
	})
}

// RegisterProfiles installs a product market analyst profile.
func RegisterProfiles(registry *core.AgentProfileRegistry, productScope core.ScopePath) error {
	name := "Crypto market analyst"
	description := "A composable cryptocurrency market agent built from live and structured capabilities."
	model := core.ModelSelection{Provider: "mock", Model: "mock-1"}
	steps := 12
	return registry.Bind(core.AgentProfileLayer{
		Scope: productScope, ProfileID: ProfileAnalyst, Name: &name, Description: &description,
		Model: &model, MaxSteps: &steps,
		AddCapabilities: []string{CapabilityQuote, CapabilityDiscover, CapabilityAnalyze},
		PutFragments: []core.PromptFragment{
			{ID: "crypto.identity", Section: core.PromptIdentity, Content: "You are a cryptocurrency market data analyst."},
			{ID: "crypto.goal", Section: core.PromptGoals, Content: "Separate observed data, inference, and uncertainty."},
			{ID: "crypto.boundary", Section: core.PromptBoundaries, Content: "Do not promise returns or present analysis as personalized financial advice."},
		},
	})
}

// RegisterTenantPersona adds a tenant-owned brand and data policy layer.
func RegisterTenantPersona(registry *core.AgentProfileRegistry, tenantScope core.ScopePath) error {
	return registry.Bind(core.AgentProfileLayer{
		Scope: tenantScope, ProfileID: ProfileAnalyst,
		PutFragments: []core.PromptFragment{
			{ID: "tenant.data-policy", Section: core.PromptInstructions, Content: "Prefer tenant-configured real-time market and on-chain providers."},
		},
	})
}

// RegisterUserAvatar creates a user-owned digital-human profile from the product agent.
func RegisterUserAvatar(registry *core.AgentProfileRegistry, userScope core.ScopePath) error {
	name := "My crypto digital human"
	return registry.Bind(core.AgentProfileLayer{
		Scope: userScope, ProfileID: ProfileAvatar, Extends: ProfileAnalyst, Name: &name,
		PutFragments: []core.PromptFragment{
			{ID: "user.style", Section: core.PromptStyle, Content: "Use concise Chinese with a calm, evidence-first tone."},
			{ID: "user.trait", Section: core.PromptTraits, Content: "Patient, skeptical, and explicit about missing data."},
		},
	})
}

// FastRouter demonstrates deterministic, zero-model dispatch for common intents.
func FastRouter() *core.FastRouter {
	router := &core.FastRouter{}
	router.Add(core.FastRule{
		Match: func(text string) bool {
			return strings.Contains(text, "新币") || strings.Contains(strings.ToLower(text), "new token")
		},
		Capability: CapabilityDiscover,
	})
	router.Add(core.FastRule{
		Match: func(text string) bool {
			return strings.Contains(text, "分析") || strings.Contains(strings.ToLower(text), "analyze")
		},
		Capability: CapabilityAnalyze,
		Args:       func(text string) map[string]any { return map[string]any{"symbol": lastWord(text)} },
	})
	router.Add(core.FastRule{
		Match: func(text string) bool {
			return strings.Contains(text, "价格") || strings.Contains(text, "看盘") || strings.Contains(strings.ToLower(text), "price")
		},
		Capability: CapabilityQuote,
		Args:       func(text string) map[string]any { return map[string]any{"symbol": lastWord(text)} },
	})
	return router
}

func objectSchema(properties map[string]any) map[string]any {
	return map[string]any{"type": "object", "properties": properties}
}

func jsonResult(value any) (core.CapabilityResult, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return core.CapabilityResult{}, err
	}
	return core.CapabilityResult{Content: string(data), OK: true}, nil
}

func stringArg(args map[string]any, key, fallback string) string {
	if value, ok := args[key].(string); ok && strings.TrimSpace(value) != "" {
		return strings.TrimSpace(value)
	}
	return fallback
}

func upperArg(args map[string]any, key, fallback string) string {
	return strings.ToUpper(stringArg(args, key, fallback))
}

func lastWord(text string) string {
	fields := strings.Fields(text)
	if len(fields) == 0 {
		return "BTC"
	}
	value := strings.Trim(fields[len(fields)-1], "，。,.!?！？")
	if len(value) > 12 || value == "分析" || value == "看盘" || value == "价格" {
		return "BTC"
	}
	return strings.ToUpper(value)
}
