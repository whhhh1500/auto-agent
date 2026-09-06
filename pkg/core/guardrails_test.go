package core

import (
	"context"
	"strings"
	"testing"
)

func TestPolicyRegistryIntersectsPermissionsAcrossScopes(t *testing.T) {
	_, _, tenant, user := testScopes()
	principal := testPrincipal(user) // grants: read, write
	registry := NewPolicyRegistry()
	if err := registry.Bind(PolicyLayer{
		Scope: tenant, AllowPermissions: []Permission{PermRead, PermWrite},
	}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Bind(PolicyLayer{
		Scope: user, DenyPermissions: []Permission{PermWrite},
	}); err != nil {
		t.Fatal(err)
	}
	resolved, err := registry.Resolve(principal, user)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Permissions.Allows([]Permission{PermWrite}) {
		t.Fatal("denied write survived the intersection")
	}
	if !resolved.Permissions.Allows([]Permission{PermRead}) {
		t.Fatal("allowed read was removed")
	}
}

func TestPolicyRegistryCapsBudgetsAndNeverWidens(t *testing.T) {
	_, _, tenant, user := testScopes()
	principal := testPrincipal(user)
	registry := NewPolicyRegistry()
	steps := 5
	toolCalls := 2
	if err := registry.Bind(PolicyLayer{Scope: tenant, MaxSteps: &steps, MaxToolCalls: &toolCalls}); err != nil {
		t.Fatal(err)
	}
	resolved, err := registry.Resolve(principal, user)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.MaxSteps == nil || *resolved.MaxSteps != steps {
		t.Fatalf("expected step cap %d, got %v", steps, resolved.MaxSteps)
	}
	if resolved.MaxToolCalls == nil || *resolved.MaxToolCalls != toolCalls {
		t.Fatalf("expected tool cap %d, got %v", toolCalls, resolved.MaxToolCalls)
	}
	if resolved.Permissions.Allows([]Permission{PermSend}) {
		t.Fatal("a layer must not widen the principal grants")
	}
}

func TestPolicyNarrowsRunCapabilities(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	capabilities := NewCapabilityRegistry()
	profiles := NewAgentProfileRegistry()
	if err := capabilities.Register(product, staticTool{manifest: toolManifest("market.quote", "1.0.0"), content: "42"}); err != nil {
		t.Fatal(err)
	}
	name := "Agent"
	model := ModelSelection{Provider: "mock", Model: "mock-1"}
	if err := profiles.Bind(AgentProfileLayer{
		Scope: product, ProfileID: "product.agent", Name: &name, Model: &model,
		AddCapabilities: []string{"market.quote"},
	}); err != nil {
		t.Fatal(err)
	}
	policies := NewPolicyRegistry()
	if err := policies.Bind(PolicyLayer{Scope: user, AllowPermissions: []Permission{PermSend}}); err != nil {
		t.Fatal(err)
	}
	runtime := &Runtime{
		Capabilities: capabilities, Profiles: profiles, Policy: policies,
		Models: ModelResolverFunc(func(context.Context, ModelSelection) (LlmAdapter, error) {
			return MockLlmAdapter{}, nil
		}),
	}
	sessionScope, _ := user.Child(ScopeRef{Kind: ScopeSession, ID: "session-a"})
	session, _ := NewSession(SessionOptions{
		ID: "session-a", ProfileID: "product.agent", Principal: principal, Scope: sessionScope,
	})
	result, err := runtime.RunTurn(context.Background(), principal, session, TurnInput{RunID: "run-a", Text: "quote"}, nil)
	if err != nil || result.Status != RunCompleted {
		t.Fatalf("unexpected run result: %#v, %v", result, err)
	}
	// The mock answers "no capabilities" when the tool list is empty, proving
	// policy narrowing removed the read-only capability from the run.
	if result.Answer != "No model-facing capabilities are available for this run." {
		t.Fatalf("policy narrowing did not remove the capability, answer=%q", result.Answer)
	}
}

func TestSnapshotFilterByPermissionsDropsQuietly(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	registry := NewCapabilityRegistry()
	if err := registry.Register(product, staticTool{manifest: toolManifest("market.quote", "1.0.0"), content: "42"}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := (CapabilityResolver{Registry: registry}).Resolve(principal, user)
	if err != nil {
		t.Fatal(err)
	}
	narrowed := snapshot.FilterByPermissions(NewPermissionSet())
	if narrowed.Authorized("market.quote") {
		t.Fatal("narrowed snapshot kept the capability")
	}
	if narrowed.ID == snapshot.ID {
		t.Fatal("narrowed snapshot must have a distinct digest")
	}
}

func TestRegistryEntriesListsCatalogWithoutGrants(t *testing.T) {
	_, product, _, user := testScopes()
	registry := NewCapabilityRegistry()
	if err := registry.Register(product, staticTool{manifest: toolManifest("market.quote", "1.0.0"), content: "42"}); err != nil {
		t.Fatal(err)
	}
	entries, err := registry.Entries(user)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Manifest.ID != "market.quote" {
		t.Fatalf("unexpected catalog entries: %#v", entries)
	}
}

func TestProfileRegistryListProfiles(t *testing.T) {
	_, product, _, user := testScopes()
	profiles := NewAgentProfileRegistry()
	name := "Agent"
	model := ModelSelection{Provider: "mock", Model: "mock-1"}
	if err := profiles.Bind(AgentProfileLayer{
		Scope: product, ProfileID: "product.agent", Name: &name, Model: &model,
	}); err != nil {
		t.Fatal(err)
	}
	listed, err := profiles.ListProfiles(user)
	if err != nil || len(listed) != 1 || listed[0] != "product.agent" {
		t.Fatalf("unexpected profile listing: %#v, %v", listed, err)
	}
}

func TestProfileMaxToolCallsOverridesDefaultBudget(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	profiles := NewAgentProfileRegistry()
	name := "Agent"
	model := ModelSelection{Provider: "mock", Model: "mock-1"}
	toolCalls := 3
	if err := profiles.Bind(AgentProfileLayer{
		Scope: product, ProfileID: "product.agent", Name: &name, Model: &model, MaxToolCalls: &toolCalls,
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := profiles.Resolve(principal, user, "product.agent")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.MaxToolCalls != toolCalls {
		t.Fatalf("expected max tool calls %d, got %d", toolCalls, snapshot.MaxToolCalls)
	}
}

func TestCapabilityBudgetEnforcedPerRun(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	capabilities := NewCapabilityRegistry()
	manifest := toolManifest("market.quote", "1.0.0")
	manifest.PerTurnBudget = 1
	if err := capabilities.Register(product, staticTool{manifest: manifest, content: "42"}); err != nil {
		t.Fatal(err)
	}
	profiles := NewAgentProfileRegistry()
	name := "Agent"
	model := ModelSelection{Provider: "mock", Model: "mock-1"}
	if err := profiles.Bind(AgentProfileLayer{
		Scope: product, ProfileID: "product.agent", Name: &name, Model: &model,
		AddCapabilities: []string{"market.quote"},
	}); err != nil {
		t.Fatal(err)
	}
	runtime := &Runtime{
		Capabilities: capabilities, Profiles: profiles,
		Models: ModelResolverFunc(func(context.Context, ModelSelection) (LlmAdapter, error) {
			return MockLlmAdapter{}, nil
		}),
	}
	sessionScope, _ := user.Child(ScopeRef{Kind: ScopeSession, ID: "session-a"})
	session, _ := NewSession(SessionOptions{
		ID: "session-a", ProfileID: "product.agent", Principal: principal, Scope: sessionScope,
	})
	// First run: budget allows one call.
	if _, err := runtime.RunTurn(context.Background(), principal, session, TurnInput{RunID: "run-a", Text: "quote"}, nil); err != nil {
		t.Fatal(err)
	}
	// Second run: the budget counter is per-run, so the call is allowed again.
	result, err := runtime.RunTurn(context.Background(), principal, session, TurnInput{RunID: "run-b", Text: "quote"}, nil)
	if err != nil || result.Answer != "Capability result: 42" {
		t.Fatalf("per-run budget did not reset between runs: %#v, %v", result, err)
	}
}

func TestBudgetExceededReturnsDeniedToolResult(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	registry := NewCapabilityRegistry()
	manifest := toolManifest("market.quote", "1.0.0")
	manifest.PerTurnBudget = 1
	if err := registry.Register(product, staticTool{manifest: manifest, content: "42"}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := (CapabilityResolver{Registry: registry}).Resolve(principal, user)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate the guarded runtime by driving two calls through one agent.
	agent, err := NewAgent(AgentOptions{
		LLM: MockLlmAdapter{}, Tools: snapshot,
		Session:  mustSession(t, user, principal),
		MaxSteps: 1, MaxToolCalls: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	agent.runID = "run-a"
	first, _ := agent.tools.Execute(context.Background(), ToolCall{ID: "c1", Name: "market.quote"})
	second, _ := agent.tools.Execute(context.Background(), ToolCall{ID: "c2", Name: "market.quote"})
	if !first.OK {
		t.Fatalf("first call should pass: %#v", first)
	}
	if second.OK || second.Metadata["code"] != CodeBudgetExceeded {
		t.Fatalf("second call should be budget-denied: %#v", second)
	}
}

func mustSession(t *testing.T, user ScopePath, principal Principal) *Session {
	t.Helper()
	sessionScope, _ := user.Child(ScopeRef{Kind: ScopeSession, ID: "session-a"})
	session, err := NewSession(SessionOptions{
		ID: "session-a", ProfileID: "product.agent", Principal: principal, Scope: sessionScope,
	})
	if err != nil {
		t.Fatal(err)
	}
	return session
}

func TestApprovalFailClosedWithoutApprover(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	registry := NewCapabilityRegistry()
	manifest := toolManifest("trade.execute", "1.0.0")
	manifest.RequiresApproval = true
	if err := registry.Register(product, staticTool{manifest: manifest, content: "executed"}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := (CapabilityResolver{Registry: registry}).Resolve(principal, user)
	if err != nil {
		t.Fatal(err)
	}
	agent, err := NewAgent(AgentOptions{
		LLM: MockLlmAdapter{}, Tools: snapshot, Session: mustSession(t, user, principal),
	})
	if err != nil {
		t.Fatal(err)
	}
	agent.runID = "run-a"
	result, _ := agent.tools.Execute(context.Background(), ToolCall{ID: "c1", Name: "trade.execute"})
	if result.OK || result.Metadata["code"] != CodeApprovalUnavailable {
		t.Fatalf("missing approver must fail closed: %#v", result)
	}
}

func TestApprovalDeniedAndApproved(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	registry := NewCapabilityRegistry()
	manifest := toolManifest("trade.execute", "1.0.0")
	manifest.RequiresApproval = true
	if err := registry.Register(product, staticTool{manifest: manifest, content: "executed"}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := (CapabilityResolver{Registry: registry}).Resolve(principal, user)
	if err != nil {
		t.Fatal(err)
	}
	session := mustSession(t, user, principal)
	denier := ApproverFunc(func(context.Context, ApprovalRequest) (ApprovalDecision, error) {
		return ApprovalDenied, nil
	})
	agent, _ := NewAgent(AgentOptions{LLM: MockLlmAdapter{}, Tools: snapshot, Session: session, Approver: denier})
	agent.runID = "run-a"
	denied, _ := agent.tools.Execute(context.Background(), ToolCall{ID: "c1", Name: "trade.execute"})
	if denied.OK || denied.Metadata["code"] != CodeApprovalDenied {
		t.Fatalf("denial not enforced: %#v", denied)
	}

	approver := ApproverFunc(func(context.Context, ApprovalRequest) (ApprovalDecision, error) {
		return ApprovalApproved, nil
	})
	agent2, _ := NewAgent(AgentOptions{LLM: MockLlmAdapter{}, Tools: snapshot, Session: session, Approver: approver})
	agent2.runID = "run-a"
	approved, _ := agent2.tools.Execute(context.Background(), ToolCall{ID: "c2", Name: "trade.execute"})
	if !approved.OK || approved.Content != "executed" {
		t.Fatalf("approved call did not execute: %#v", approved)
	}

	// An out-of-vocabulary decision is normalized to fail-closed.
	rogue := ApproverFunc(func(context.Context, ApprovalRequest) (ApprovalDecision, error) {
		return "maybe", nil
	})
	agent3, _ := NewAgent(AgentOptions{LLM: MockLlmAdapter{}, Tools: snapshot, Session: session, Approver: rogue})
	agent3.runID = "run-a"
	rogued, _ := agent3.tools.Execute(context.Background(), ToolCall{ID: "c3", Name: "trade.execute"})
	if rogued.OK || rogued.Metadata["code"] != CodeApprovalUnavailable {
		t.Fatalf("rogue decision must fail closed: %#v", rogued)
	}
}

func TestInvalidArgsDeniedBeforeProvider(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	registry := NewCapabilityRegistry()
	manifest := toolManifest("market.quote", "1.0.0")
	manifest.Tool = &ToolExposure{Parameters: map[string]any{
		"type": "object",
		"properties": map[string]any{
			"symbol": map[string]any{"type": "string"},
		},
		"required":             []any{"symbol"},
		"additionalProperties": false,
	}}
	provider := &recordingTool{manifest: manifest}
	if err := registry.Register(product, provider); err != nil {
		t.Fatal(err)
	}
	snapshot, err := (CapabilityResolver{Registry: registry}).Resolve(principal, user)
	if err != nil {
		t.Fatal(err)
	}
	agent, _ := NewAgent(AgentOptions{LLM: MockLlmAdapter{}, Tools: snapshot, Session: mustSession(t, user, principal)})
	agent.runID = "run-a"
	result, _ := agent.tools.Execute(context.Background(), ToolCall{ID: "c1", Name: "market.quote", Args: map[string]any{"wrong": true}})
	if result.OK || result.Metadata["code"] != CodeInvalidArgs {
		t.Fatalf("invalid args must be denied with stable code: %#v", result)
	}
	if len(provider.calls) != 0 {
		t.Fatalf("provider must not see invalid args: %#v", provider.calls)
	}
	if !strings.Contains(result.Content, "symbol") {
		t.Fatalf("denial should name the violated property: %s", result.Content)
	}
}

type recordingTool struct {
	manifest CapabilityManifest
	calls    []ToolCall
}

func (r *recordingTool) Manifest() CapabilityManifest { return r.manifest }

func (r *recordingTool) Execute(_ context.Context, request CapabilityRequest) (CapabilityResult, error) {
	r.calls = append(r.calls, ToolCall{ID: request.CallID})
	return CapabilityResult{Content: "ok", OK: true}, nil
}

func TestHookDeniesToolCallAndObservesResult(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	registry := NewCapabilityRegistry()
	if err := registry.Register(product, staticTool{manifest: toolManifest("market.quote", "1.0.0"), content: "42"}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := (CapabilityResolver{Registry: registry}).Resolve(principal, user)
	if err != nil {
		t.Fatal(err)
	}
	var after []string
	hooks := &RunHooksFuncs{
		OnBeforeToolFn: func(_ context.Context, _ RunInfo, call ToolCall) error {
			if call.Name == "market.quote" {
				return errDenied
			}
			return nil
		},
		OnAfterToolFn: func(_ context.Context, _ RunInfo, _ ToolCall, result CapabilityResult) {
			after = append(after, result.Metadata["code"].(string))
		},
	}
	agent, _ := NewAgent(AgentOptions{LLM: MockLlmAdapter{}, Tools: snapshot, Session: mustSession(t, user, principal), Hooks: hooks})
	agent.runID = "run-a"
	result, _ := agent.tools.Execute(context.Background(), ToolCall{ID: "c1", Name: "market.quote"})
	if result.OK || result.Metadata["code"] != CodeHookDenied {
		t.Fatalf("hook denial not enforced: %#v", result)
	}
	if len(after) != 1 || after[0] != CodeHookDenied {
		t.Fatalf("OnAfterTool not observed: %#v", after)
	}
}

type deniedError struct{}

func (deniedError) Error() string { return "denied by policy" }

var errDenied = deniedError{}

func TestRunScopeBindingsAreOneShot(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	capabilities := NewCapabilityRegistry()
	profiles := NewAgentProfileRegistry()
	name := "Agent"
	model := ModelSelection{Provider: "mock", Model: "mock-1"}
	if err := profiles.Bind(AgentProfileLayer{
		Scope: product, ProfileID: "product.agent", Name: &name, Model: &model,
	}); err != nil {
		t.Fatal(err)
	}
	runtime := &Runtime{
		Capabilities: capabilities, Profiles: profiles,
		Models: ModelResolverFunc(func(context.Context, ModelSelection) (LlmAdapter, error) {
			return MockLlmAdapter{}, nil
		}),
	}
	sessionScope, _ := user.Child(ScopeRef{Kind: ScopeSession, ID: "session-a"})
	session, _ := NewSession(SessionOptions{
		ID: "session-a", ProfileID: "product.agent", Principal: principal, Scope: sessionScope,
	})
	oneShot := staticTool{manifest: toolManifest("run.verifier", "1.0.0"), content: "verified"}
	runBinding := CapabilityBinding{
		Mode: BindingProvide, Manifest: oneShot.Manifest(), Provider: oneShot,
	}
	profileSelectsVerifier := profiles.Bind(AgentProfileLayer{
		Scope: sessionScope, ProfileID: "product.agent",
		AddCapabilities: []string{"run.verifier"},
	})
	if profileSelectsVerifier != nil {
		t.Fatal(profileSelectsVerifier)
	}
	// Without run capabilities the profile selection fails composition.
	if _, err := runtime.RunTurn(context.Background(), principal, session, TurnInput{RunID: "run-a", Text: "check"}, nil); err == nil {
		t.Fatal("expected composition failure without the one-shot capability")
	}
	// With the run-scoped binding the same run succeeds.
	input := TurnInput{RunID: "run-b", Text: "check", RunCapabilities: []CapabilityBinding{runBinding}}
	if _, err := runtime.RunTurn(context.Background(), principal, session, input, nil); err != nil {
		t.Fatalf("run-scoped capability mount failed: %v", err)
	}
	// The binding is gone for the next run: composition fails again.
	if _, err := runtime.RunTurn(context.Background(), principal, session, TurnInput{RunID: "run-c", Text: "check"}, nil); err == nil {
		t.Fatal("run-scoped capability leaked into the next run")
	}
}
