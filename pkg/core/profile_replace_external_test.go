package core_test

import (
	"testing"

	"github.com/whhhh1500/auto-agent/pkg/core"
)

func TestExternalProfileRegistryReplaceExactSurfaceCompiles(t *testing.T) {
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	name := "old"
	model := core.ModelSelection{Provider: "mock", Model: "one"}
	old := core.AgentProfileLayer{Scope: scope, ProfileID: "external.agent", Name: &name, Model: &model}
	registry := core.NewAgentProfileRegistry()
	if _, err := registry.Mount(old); err != nil {
		t.Fatal(err)
	}
	nextName := "next"
	next := core.CloneAgentProfileLayer(old)
	next.Name = &nextName
	if err := registry.ReplaceExact(old, next); err != nil {
		t.Fatal(err)
	}
}
