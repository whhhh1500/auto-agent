package core

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestRuntimeGuardedInvocationExposesOnlyLazyFilteredCapabilityCatalog(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	principal.Grants = NewPermissionSet(PermRead)

	captureManifest := toolManifest("program.catalog", "1.0.0")
	captureManifest.Tool.Parameters = map[string]any{"type": "object"}
	capture := &catalogCaptureTool{manifest: captureManifest}
	registry := NewCapabilityRegistry()
	visible := toolManifest("visible.read", "1.0.0")
	visible.Execution = &ExecutionSpec{Headers: map[string]string{"X-Provider-Secret": "catalog-secret"}}
	profileHidden := toolManifest("profile.hidden", "1.0.0")
	permissionHidden := toolManifest("permission.hidden", "1.0.0")
	permissionHidden.RequiredPermissions = []Permission{PermWrite}
	predicateHidden := toolManifest("predicate.hidden", "1.0.0")
	for _, provider := range []Capability{
		capture,
		staticTool{manifest: visible, content: "visible"},
		staticTool{manifest: profileHidden, content: "profile-hidden"},
		staticTool{manifest: permissionHidden, content: "permission-hidden"},
		staticTool{manifest: predicateHidden, content: "predicate-hidden"},
	} {
		if err := registry.Register(product, provider); err != nil {
			t.Fatal(err)
		}
	}

	profiles := NewAgentProfileRegistry()
	name := "Programmatic catalog"
	model := ModelSelection{Provider: "scripted", Model: "catalog"}
	if err := profiles.Bind(AgentProfileLayer{
		Scope: product, ProfileID: "catalog.agent", Name: &name, Model: &model,
		AddCapabilities: []string{"program.catalog", "visible.read", "permission.hidden", "predicate.hidden"},
	}); err != nil {
		t.Fatal(err)
	}
	sessionScope, err := user.Child(ScopeRef{Kind: ScopeSession, ID: "catalog-session"})
	if err != nil {
		t.Fatal(err)
	}
	session, err := NewSession(SessionOptions{ID: "catalog-session", ProfileID: "catalog.agent", Principal: principal, Scope: sessionScope})
	if err != nil {
		t.Fatal(err)
	}
	llm := &scriptedAdapter{steps: []scriptedTurn{
		{calls: []ToolCall{{ID: "catalog-call", Name: "program.catalog", Args: map[string]any{}}}},
		{text: "done"},
	}}
	runtime := &Runtime{
		Capabilities: registry,
		Profiles:     profiles,
		Models: ModelResolverFunc(func(context.Context, ModelSelection) (LlmAdapter, error) {
			return llm, nil
		}),
	}
	result, err := runtime.RunTurn(context.Background(), principal, session, TurnInput{
		RunID: "catalog-run", Text: "show available capabilities",
		CapabilityFilter: CapabilityFilterFunc(func(manifest CapabilityManifest) bool {
			return manifest.ID != "predicate.hidden"
		}),
	}, nil)
	if err != nil || result.Status != RunCompleted || result.Answer != "done" {
		t.Fatalf("runtime result=%#v err=%v", result, err)
	}
	if !capture.guarded || capture.catalog == nil {
		t.Fatalf("catalog capability did not receive guarded lazy context: %#v", capture)
	}
	if capture.invocation != (Invocation{SessionID: session.ID(), RunID: "catalog-run", CallID: "catalog-call"}) {
		t.Fatalf("catalog invocation=%#v", capture.invocation)
	}

	first := capture.catalog()
	if got, want := catalogIDs(first), []string{"program.catalog", "visible.read"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("catalog IDs=%#v want=%#v", got, want)
	}
	for _, id := range []string{"profile.hidden", "permission.hidden", "predicate.hidden"} {
		if catalogHas(first, id) {
			t.Fatalf("catalog exposed excluded capability %q: %#v", id, first)
		}
	}
	encoded, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "catalog-secret") {
		t.Fatalf("catalog exposed provider execution secret: %s", encoded)
	}
	// Data is a method value, so no catalog is materialized until the adapter
	// deliberately calls it. Each call must produce a detached public snapshot.
	first[0].Manifest.Name = "mutated"
	first[0].Manifest.Tool.Parameters["mutated"] = true
	second := capture.catalog()
	if second[0].Manifest.Name == "mutated" || second[0].Manifest.Tool.Parameters["mutated"] != nil {
		t.Fatalf("lazy catalog result was not defensively copied: %#v", second)
	}
}

type catalogCaptureTool struct {
	manifest   CapabilityManifest
	guarded    bool
	invocation Invocation
	catalog    func() []SnapshotCapability
}

func (t *catalogCaptureTool) Manifest() CapabilityManifest { return t.manifest }

func (t *catalogCaptureTool) Execute(_ context.Context, request CapabilityRequest) (CapabilityResult, error) {
	catalog, ok := request.Context.Data.(func() []SnapshotCapability)
	if !ok {
		return CapabilityResult{Content: "catalog unavailable", OK: false}, nil
	}
	t.guarded = request.Context.Invocation.SessionID != "" && request.Context.Invocation.RunID != "" && request.Context.Invocation.CallID != "" && request.Context.RemainingToolCalls > 0
	t.invocation = request.Context.Invocation
	t.catalog = catalog
	return CapabilityResult{Content: "catalog captured", OK: true}, nil
}

func catalogIDs(catalog []SnapshotCapability) []string {
	ids := make([]string, len(catalog))
	for i := range catalog {
		ids[i] = catalog[i].Manifest.ID
	}
	return ids
}

func catalogHas(catalog []SnapshotCapability, id string) bool {
	for _, capability := range catalog {
		if capability.Manifest.ID == id {
			return true
		}
	}
	return false
}
