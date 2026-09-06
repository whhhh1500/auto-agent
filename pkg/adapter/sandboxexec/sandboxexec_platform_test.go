//go:build !linux

package sandboxexec

import (
	"context"
	"testing"
	"time"

	"github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/execution/sandbox"
)

func TestLocalRegistrationSandboxExecFailsClosedWithoutHostFallback(t *testing.T) {
	providerRegistry, err := sandbox.NewRegistry(2, sandbox.NewLocalRegistration())
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	capability, err := New(Config{Registry: providerRegistry, ProviderID: "local-ephemeral", Version: "1", WorkRoot: root,
		Limits:         sandbox.Limits{WallTime: time.Second, MaxCPUTime: time.Second, MaxMemoryBytes: 1 << 20, MaxOutputBytes: 1 << 20},
		ArtifactPolicy: sandbox.ArtifactPolicy{MaxArtifacts: 1, MaxTotalBytes: 1 << 20}, ObjectStore: &testObjectStore{failAfter: -1}})
	if err != nil {
		t.Fatal(err)
	}
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	principal := core.Principal{TenantID: "tenant", SubjectID: "writer", Scope: global, Grants: core.NewPermissionSet(core.PermWrite)}
	caps := core.NewCapabilityRegistry()
	if err := caps.Register(global, capability); err != nil {
		t.Fatal(err)
	}
	profiles := core.NewAgentProfileRegistry()
	selection := core.ModelSelection{Provider: "test", Model: "test"}
	if err := profiles.Bind(core.AgentProfileLayer{Scope: global, ProfileID: "general", Model: &selection, AddCapabilities: []string{"sandbox.exec"}}); err != nil {
		t.Fatal(err)
	}
	scope, err := global.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "platform-session"})
	if err != nil {
		t.Fatal(err)
	}
	session, err := core.NewSession(core.SessionOptions{ID: "platform-session", ProfileID: "general", Principal: principal, Scope: scope})
	if err != nil {
		t.Fatal(err)
	}
	runtime := &core.Runtime{Capabilities: caps, Profiles: profiles, Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) { return &agentToolModel{}, nil }), ToolJournal: &testJournal{}}
	if _, err := runtime.RunTurn(context.Background(), principal, session, core.TurnInput{RunID: "platform-run", Text: "execute"}, nil); err != nil {
		t.Fatal(err)
	}
	result, ok, err := session.ToolResult("platform-run", "call-1")
	if err != nil || !ok || result.OK || result.Metadata["code"] == "ok" {
		t.Fatalf("unavailable result=%#v ok=%v err=%v", result, ok, err)
	}
	readOnly := principal
	readOnly.SubjectID = "reader"
	readOnly.Grants = core.NewPermissionSet(core.PermRead)
	readSnapshot, err := (core.CapabilityResolver{Registry: caps}).Resolve(readOnly, scope)
	if err != nil {
		t.Fatal(err)
	}
	for _, schema := range readSnapshot.Schemas() {
		if schema.Name == "sandbox.exec" {
			t.Fatal("sandbox.exec exposed without write permission")
		}
	}
}
