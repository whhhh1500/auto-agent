package core

import (
	"strings"
	"testing"
)

func TestAgentProfileInheritanceAndLayeredSoul(t *testing.T) {
	_, product, tenant, user := testScopes()
	principal := testPrincipal(user)
	registry := NewAgentProfileRegistry()

	// Register the child first to prove hierarchy outranks registration time.
	childName := "My avatar"
	if err := registry.Bind(AgentProfileLayer{
		Scope: user, ProfileID: "user.avatar", Extends: "product.analyst", Name: &childName,
		RemoveCapabilities: []string{"market.trade"},
		PutFragments: []PromptFragment{
			{ID: "voice", Section: PromptStyle, Content: "Speak concisely in Chinese."},
		},
	}); err != nil {
		t.Fatal(err)
	}
	baseName := "Analyst"
	model := ModelSelection{Provider: "mock", Model: "mock-1"}
	if err := registry.Bind(AgentProfileLayer{
		Scope: product, ProfileID: "product.analyst", Name: &baseName, Model: &model,
		AddCapabilities: []string{"market.quote", "market.trade"},
		PutFragments: []PromptFragment{
			{ID: "identity", Section: PromptIdentity, Content: "You are a market analyst."},
			{ID: "voice", Section: PromptStyle, Content: "Use a formal tone."},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Bind(AgentProfileLayer{
		Scope: tenant, ProfileID: "product.analyst",
		PutFragments: []PromptFragment{{ID: "boundary", Section: PromptBoundaries, Content: "Never promise returns."}},
	}); err != nil {
		t.Fatal(err)
	}

	snapshot, err := registry.Resolve(principal, user, "user.avatar")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Name != childName || len(snapshot.Capabilities) != 1 || snapshot.Capabilities[0] != "market.quote" {
		t.Fatalf("unexpected profile composition: %#v", snapshot)
	}
	prompt := snapshot.SystemPrompt()
	for _, expected := range []string{"market analyst", "Never promise returns", "concisely in Chinese"} {
		if !strings.Contains(prompt, expected) {
			t.Fatalf("prompt missing %q: %s", expected, prompt)
		}
	}
	if strings.Contains(prompt, "formal tone") {
		t.Fatalf("user fragment did not replace inherited fragment: %s", prompt)
	}
}

func TestProfileResolveRequiresCompleteModelSelection(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	registry := NewAgentProfileRegistry()
	name := "No model"
	if err := registry.Bind(AgentProfileLayer{Scope: product, ProfileID: "invalid.agent", Name: &name}); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Resolve(principal, user, "invalid.agent"); err == nil {
		t.Fatal("profile without a model selection resolved successfully")
	}
}

func TestProfileRegistryAllowsSafeBareProfileID(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	registry := NewAgentProfileRegistry()
	name := "General"
	model := ModelSelection{Provider: "mock", Model: "mock-1"}
	if err := registry.Bind(AgentProfileLayer{Scope: product, ProfileID: "general", Name: &name, Model: &model}); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Resolve(principal, user, "general"); err != nil {
		t.Fatalf("resolve safe bare profile ID: %v", err)
	}
	if err := registry.Bind(AgentProfileLayer{Scope: product, ProfileID: "not/a/profile", Name: &name, Model: &model}); err == nil {
		t.Fatal("unsafe bare profile ID was accepted")
	}
}

func TestValidateProfileIDMatchesRegistryVocabulary(t *testing.T) {
	for _, id := range []string{"general", "product.agent", "profile:beta_1"} {
		if err := ValidateProfileID(id); err != nil {
			t.Fatalf("ValidateProfileID(%q): %v", id, err)
		}
	}
	for _, id := range []string{"", "not/a/profile", " bad", "bad\nprofile"} {
		if err := ValidateProfileID(id); err == nil {
			t.Fatalf("ValidateProfileID(%q) unexpectedly accepted", id)
		}
	}
}

func TestProfileRejectsOversizedPromptFragment(t *testing.T) {
	_, product, _, _ := testScopes()
	registry := NewAgentProfileRegistry()
	name := "Large"
	model := ModelSelection{Provider: "mock", Model: "mock-1"}
	err := registry.Bind(AgentProfileLayer{
		Scope: product, ProfileID: "large.agent", Name: &name, Model: &model,
		PutFragments: []PromptFragment{{ID: "large", Section: PromptInstructions, Content: strings.Repeat("x", MaxPromptFragmentBytes+1)}},
	})
	if err == nil {
		t.Fatal("oversized prompt fragment was accepted")
	}
}

func TestCapabilityPredicateFilterIsSubtractiveAndPanicClosed(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	registry := NewCapabilityRegistry()
	for _, id := range []string{"eval.safe", "eval.blocked"} {
		manifest := toolManifest(id, "1.0.0")
		manifest.Idempotent = id == "eval.safe"
		if err := registry.Register(product, staticTool{manifest: manifest, content: id}); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := (CapabilityResolver{Registry: registry}).Resolve(principal, user)
	if err != nil {
		t.Fatal(err)
	}
	filtered := snapshot.FilterByPredicate(CapabilityFilterFunc(func(manifest CapabilityManifest) bool {
		if manifest.ID == "eval.blocked" {
			panic("filter panic")
		}
		return manifest.Idempotent
	}))
	if !filtered.Authorized("eval.safe") || filtered.Authorized("eval.blocked") {
		t.Fatalf("predicate filter wrong: %#v", filtered.Capabilities())
	}
	if !snapshot.Authorized("eval.safe") || !snapshot.Authorized("eval.blocked") {
		t.Fatal("predicate filter mutated the source snapshot")
	}
	if filtered.Authorized("eval.added") {
		t.Fatal("predicate filter added a capability")
	}
}

func TestProfileRegistryCloneAndLayerRevisionAreIndependent(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	registry := NewAgentProfileRegistry()
	name := "Original"
	description := "Original description"
	model := ModelSelection{Provider: "mock", Model: "mock-1"}
	maxSteps := 4
	layer := AgentProfileLayer{
		Scope: product, ProfileID: "clone.agent", Name: &name, Description: &description,
		Model: &model, MaxSteps: &maxSteps, Metadata: map[string]string{"channel": "stable"},
	}
	if err := registry.Bind(layer); err != nil {
		t.Fatal(err)
	}
	revision, err := ProfileLayerRevision(layer)
	if err != nil || revision == "" {
		t.Fatalf("layer revision failed: %q err=%v", revision, err)
	}
	name = "Mutated by caller"
	description = "Mutated"
	model.Model = "mutated"
	maxSteps = 99
	layer.Metadata["channel"] = "mutated"
	original, err := registry.Resolve(principal, user, "clone.agent")
	if err != nil || original.Name != "Original" || original.Model.Model != "mock-1" || original.MaxSteps != 4 || original.Metadata["channel"] != "stable" {
		t.Fatalf("registry retained caller pointers: %#v err=%v", original, err)
	}
	clone := registry.Clone()
	candidateName := "Candidate"
	if err := clone.Bind(AgentProfileLayer{Scope: product, ProfileID: "clone.agent", Name: &candidateName}); err != nil {
		t.Fatal(err)
	}
	candidate, err := clone.Resolve(principal, user, "clone.agent")
	if err != nil || candidate.Name != "Candidate" {
		t.Fatalf("clone candidate wrong: %#v err=%v", candidate, err)
	}
	original, _ = registry.Resolve(principal, user, "clone.agent")
	if original.Name != "Original" {
		t.Fatal("clone mutation changed source registry")
	}
	stableLayer := CloneAgentProfileLayer(layer)
	stableLayer.Scope = product
	stableName := "Original"
	stableDescription := "Original description"
	stableModel := ModelSelection{Provider: "mock", Model: "mock-1"}
	stableSteps := 4
	stableLayer.Name, stableLayer.Description, stableLayer.Model, stableLayer.MaxSteps = &stableName, &stableDescription, &stableModel, &stableSteps
	stableLayer.Metadata["channel"] = "stable"
	stableRevision, err := ProfileLayerRevision(stableLayer)
	if err != nil || stableRevision != revision {
		t.Fatalf("layer revision is not stable: %s vs %s err=%v", revision, stableRevision, err)
	}
	stableLayer.Metadata["channel"] = "candidate"
	changedRevision, _ := ProfileLayerRevision(stableLayer)
	if changedRevision == revision {
		t.Fatal("layer revision ignored artifact changes")
	}
}

func TestAgentProfileRegistryRejectsBindingOverflow(t *testing.T) {
	_, product, _, _ := testScopes()
	registry := NewAgentProfileRegistry()
	registry.maxBindings = 2
	mount := func(id string) (func(), error) {
		return registry.Mount(AgentProfileLayer{Scope: product, ProfileID: id})
	}
	unmount, err := mount("profile.one")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mount("profile.two"); err != nil {
		t.Fatal(err)
	}
	if _, err := mount("profile.three"); err == nil {
		t.Fatal("profile registry overflow was accepted")
	}
	unmount()
	if _, err := mount("profile.three"); err != nil {
		t.Fatalf("unmount did not free a registry slot: %v", err)
	}
}
