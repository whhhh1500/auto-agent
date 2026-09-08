package main

import (
	"context"
	"encoding/json"
	"fmt"

	cryptoexample "github.com/whhhh1500/auto-agent/examples/crypto"
	core "github.com/whhhh1500/auto-agent/pkg/core"
)

func main() {
	ctx := context.Background()
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	deployment, _ := global.Child(core.ScopeRef{Kind: core.ScopeDeployment, ID: "demo"})
	product, _ := deployment.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "crypto"})
	tenant, _ := product.Child(core.ScopeRef{Kind: core.ScopeTenant, ID: "acme"})
	user, _ := tenant.Child(core.ScopeRef{Kind: core.ScopeUser, ID: "alice"})

	principal := core.Principal{
		SubjectID: "alice", TenantID: "acme", Scope: user,
		Grants: core.NewPermissionSet(core.PermRead),
	}

	capabilities := core.NewCapabilityRegistry()
	profiles := core.NewAgentProfileRegistry()
	must(cryptoexample.RegisterProductCapabilities(capabilities, product))
	must(cryptoexample.RegisterTenantMarketData(
		capabilities,
		tenant,
		"tenant-private-market-feed",
		map[string]cryptoexample.MarketQuote{
			"BTC": {PriceUSD: 101234.5, Change24HPct: 1.6},
			"ETH": {PriceUSD: 4123.4, Change24HPct: -0.2},
			"SOL": {PriceUSD: 207.8, Change24HPct: 3.0},
		},
	))
	must(cryptoexample.RegisterProfiles(profiles, product))
	must(cryptoexample.RegisterTenantPersona(profiles, tenant))
	must(cryptoexample.RegisterUserAvatar(profiles, user))

	runtime := &core.Runtime{
		Capabilities: capabilities, Profiles: profiles,
		Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) {
			return core.MockLlmAdapter{}, nil
		}),
		FastRouters: core.FastRouterResolverFunc(func(context.Context, *core.AgentProfileSnapshot) (*core.FastRouter, error) {
			return cryptoexample.FastRouter(), nil
		}),
	}

	sessionID, _ := core.NewID("sess_")
	sessionScope, _ := user.Child(core.ScopeRef{Kind: core.ScopeSession, ID: sessionID})
	session, err := core.NewSession(core.SessionOptions{
		ID: sessionID, ProfileID: cryptoexample.ProfileAvatar, Principal: principal, Scope: sessionScope,
	})
	must(err)

	for _, message := range []string{"帮我看盘 BTC", "发现新币", "分析 ETH"} {
		runID, _ := core.NewID("run_")
		result, err := runtime.RunTurn(ctx, principal, session, core.TurnInput{RunID: runID, Text: message}, func(event core.SessionEvent) {
			data, _ := json.Marshal(event)
			fmt.Println(string(data))
		})
		must(err)
		fmt.Printf("answer: %s\n\n", result.Answer)
	}
	fmt.Printf("session=%s version=%d runs=3\n", session.ID(), session.Version())
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
