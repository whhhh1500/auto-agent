package core

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func replaceProfileLayer(scope ScopePath, id, name, pair string) AgentProfileLayer {
	model := ModelSelection{Provider: "mock", Model: "mock-1"}
	return AgentProfileLayer{
		Scope: scope, ProfileID: id, Name: &name, Model: &model,
		Metadata: map[string]string{"pair": pair},
	}
}

func TestAgentProfileRegistryReplaceExactPreservesOrderAndUnmount(t *testing.T) {
	scope := MustScopePath(ScopeRef{Kind: ScopeGlobal, ID: "global"})
	principal := Principal{Scope: scope}
	registry := NewAgentProfileRegistry()
	old := replaceProfileLayer(scope, "replace.agent", "old", "old")
	unmount, err := registry.Mount(old)
	if err != nil {
		t.Fatal(err)
	}
	later := replaceProfileLayer(scope, old.ProfileID, "later", "later")
	if _, err := registry.Mount(later); err != nil {
		t.Fatal(err)
	}
	newLayer := replaceProfileLayer(scope, old.ProfileID, "new", "new")
	if err := registry.ReplaceExact(CloneAgentProfileLayer(old), newLayer); err != nil {
		t.Fatal(err)
	}
	snapshot, err := registry.Resolve(principal, scope, old.ProfileID)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Name != "later" || snapshot.Metadata["pair"] != "later" {
		t.Fatalf("replacement changed later-layer precedence: %#v", snapshot)
	}
	unmount()
	snapshot, err = registry.Resolve(principal, scope, old.ProfileID)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Name != "later" || snapshot.Metadata["pair"] != "later" {
		t.Fatalf("old unmount did not remove replacement at its original order: %#v", snapshot)
	}
}

func TestAgentProfileRegistryReplaceExactFailsClosed(t *testing.T) {
	scope := MustScopePath(ScopeRef{Kind: ScopeGlobal, ID: "global"})
	other := MustScopePath(ScopeRef{Kind: ScopeDeployment, ID: "production"})
	registry := NewAgentProfileRegistry()
	old := replaceProfileLayer(scope, "replace.agent", "old", "old")
	if _, err := registry.Mount(old); err != nil {
		t.Fatal(err)
	}
	newLayer := replaceProfileLayer(scope, old.ProfileID, "new", "new")
	assertUnchanged := func(label string, before registryProfileState) {
		t.Helper()
		if after := captureRegistryProfileState(registry); !reflect.DeepEqual(after, before) {
			t.Fatalf("%s changed registry state\n got=%#v\nwant=%#v", label, after, before)
		}
	}
	before := captureRegistryProfileState(registry)
	if err := registry.ReplaceExact(replaceProfileLayer(scope, old.ProfileID, "missing", "missing"), newLayer); err == nil {
		t.Fatal("missing old layer replacement succeeded")
	}
	assertUnchanged("missing old layer", before)
	if err := registry.ReplaceExact(old, replaceProfileLayer(scope, "other.agent", "new", "new")); err == nil {
		t.Fatal("different profile id replacement succeeded")
	}
	assertUnchanged("profile id drift", before)
	if err := registry.ReplaceExact(old, replaceProfileLayer(other, old.ProfileID, "new", "new")); err == nil {
		t.Fatal("different scope replacement succeeded")
	}
	assertUnchanged("scope drift", before)
	invalid := CloneAgentProfileLayer(newLayer)
	invalid.Scope = ScopePath{}
	if err := registry.ReplaceExact(old, invalid); err == nil {
		t.Fatal("invalid replacement succeeded")
	}
	assertUnchanged("invalid next layer", before)
	invalid = CloneAgentProfileLayer(newLayer)
	invalid.Extends = "missing.agent"
	if err := registry.ReplaceExact(old, invalid); err == nil || !strings.Contains(err.Error(), "resolve parent") {
		t.Fatalf("invalid final projection error=%v", err)
	}
	assertUnchanged("invalid final projection", before)
	if _, err := registry.Mount(CloneAgentProfileLayer(old)); err != nil {
		t.Fatal(err)
	}
	before = captureRegistryProfileState(registry)
	if err := registry.ReplaceExact(old, newLayer); err == nil || !strings.Contains(err.Error(), "multiple") {
		t.Fatalf("ambiguous old layer error=%v", err)
	}
	assertUnchanged("ambiguous old layer", before)
}

func TestAgentProfileRegistryReplaceExactCopiesLayers(t *testing.T) {
	scope := MustScopePath(ScopeRef{Kind: ScopeGlobal, ID: "global"})
	principal := Principal{Scope: scope}
	registry := NewAgentProfileRegistry()
	old := replaceProfileLayer(scope, "copy.agent", "old", "old")
	stableOld := CloneAgentProfileLayer(old)
	if _, err := registry.Mount(old); err != nil {
		t.Fatal(err)
	}
	*old.Name = "caller mutation"
	old.Metadata["pair"] = "caller mutation"
	newLayer := replaceProfileLayer(scope, stableOld.ProfileID, "new", "new")
	if err := registry.ReplaceExact(stableOld, newLayer); err != nil {
		t.Fatal(err)
	}
	*newLayer.Name = "caller mutation"
	newLayer.Model.Model = "caller mutation"
	newLayer.Metadata["pair"] = "caller mutation"
	snapshot, err := registry.Resolve(principal, scope, stableOld.ProfileID)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Name != "new" || snapshot.Model.Model != "mock-1" || snapshot.Metadata["pair"] != "new" {
		t.Fatalf("replacement retained caller aliases: %#v", snapshot)
	}
}

func TestAgentProfileRegistryReplaceExactIsAtomicForConcurrentResolve(t *testing.T) {
	scope := MustScopePath(ScopeRef{Kind: ScopeGlobal, ID: "global"})
	principal := Principal{Scope: scope}
	registry := NewAgentProfileRegistry()
	old := replaceProfileLayer(scope, "atomic.agent", "old", "old")
	newLayer := replaceProfileLayer(scope, old.ProfileID, "new", "new")
	if _, err := registry.Mount(old); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	errs := make(chan error, 4)
	var readers sync.WaitGroup
	for range 4 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				snapshot, err := registry.Resolve(principal, scope, old.ProfileID)
				if err != nil {
					errs <- err
					return
				}
				if snapshot.Name != snapshot.Metadata["pair"] || (snapshot.Name != "old" && snapshot.Name != "new") {
					errs <- fmt.Errorf("mixed profile projection: %#v", snapshot)
					return
				}
			}
		}()
	}
	current, next := CloneAgentProfileLayer(old), CloneAgentProfileLayer(newLayer)
	for range 2000 {
		if err := registry.ReplaceExact(current, next); err != nil {
			close(done)
			readers.Wait()
			t.Fatal(err)
		}
		current, next = next, current
	}
	close(done)
	readers.Wait()
	select {
	case err := <-errs:
		t.Fatal(err)
	default:
	}
}

func TestAgentProfileRegistryReplaceExactCopiesEveryLayerField(t *testing.T) {
	scope := MustScopePath(ScopeRef{Kind: ScopeGlobal, ID: "global"})
	principal := Principal{Scope: scope}
	registry := NewAgentProfileRegistry()
	baseName, baseDescription := "base", "base description"
	baseModel := ModelSelection{Provider: "mock", Model: "base"}
	if err := registry.Bind(AgentProfileLayer{
		Scope: scope, ProfileID: "base.agent", Name: &baseName, Description: &baseDescription, Model: &baseModel,
		AddCapabilities: []string{"cap.alpha", "cap.remove"},
		PutFragments: []PromptFragment{
			{ID: "base.remove", Section: PromptIdentity, Content: "base remove"},
			{ID: "base.stay", Section: PromptInstructions, Content: "base stay"},
		}, Metadata: map[string]string{"base": "true"},
	}); err != nil {
		t.Fatal(err)
	}
	old := fullReplaceProfileLayer(scope, "full.agent", "old", "old")
	if _, err := registry.Mount(old); err != nil {
		t.Fatal(err)
	}
	next := fullReplaceProfileLayer(scope, old.ProfileID, "next", "next")
	expected := CloneAgentProfileLayer(next)

	candidate := registry.Clone()
	if err := candidate.ReplaceExact(CloneAgentProfileLayer(old), next); err != nil {
		t.Fatalf("replace candidate: %v", err)
	}
	candidateSnapshot, err := candidate.Resolve(principal, scope, old.ProfileID)
	if err != nil || candidateSnapshot.Name != "next" {
		t.Fatalf("candidate replace did not produce next projection: %#v err=%v", candidateSnapshot, err)
	}
	liveSnapshot, err := registry.Resolve(principal, scope, old.ProfileID)
	if err != nil || liveSnapshot.Name != "old" {
		t.Fatalf("candidate replacement mutated live registry: %#v err=%v", liveSnapshot, err)
	}

	if err := registry.ReplaceExact(CloneAgentProfileLayer(old), next); err != nil {
		t.Fatal(err)
	}
	*next.Name, *next.Description = "mutated", "mutated"
	next.Model.Provider, next.Model.Model = "mutated", "mutated"
	*next.MaxSteps, *next.MaxToolCalls = 99, 99
	next.Extends, next.ProfileID = "mutated.agent", "mutated.agent"
	next.AddCapabilities[0], next.RemoveCapabilities[0] = "mutated", "mutated"
	next.PutFragments[0].Content, next.RemoveFragments[0], next.Metadata["replace"] = "mutated", "mutated", "mutated"

	key := profileLayerKey(scope, expected.ProfileID)
	actual, err := canonicalProfileLayer(registry.layers[key][0].AgentProfileLayer)
	if err != nil {
		t.Fatal(err)
	}
	want, err := canonicalProfileLayer(expected)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(actual, want) {
		t.Fatalf("replacement did not retain a detached complete layer\n got=%s\nwant=%s", actual, want)
	}
	snapshot, err := registry.Resolve(principal, scope, expected.ProfileID)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Name != "next" || snapshot.Description != "next description" || snapshot.Model != (ModelSelection{Provider: "mock-next", Model: "next"}) || snapshot.MaxSteps != 7 || snapshot.MaxToolCalls != 9 {
		t.Fatalf("replacement scalar projection=%#v", snapshot)
	}
	if !reflect.DeepEqual(snapshot.Capabilities, []string{"cap.next", "cap.remove"}) || !reflect.DeepEqual(fragmentIDs(snapshot.Fragments), []string{"base.remove", "next.fragment"}) || !reflect.DeepEqual(snapshot.Metadata, map[string]string{"base": "true", "replace": "next"}) {
		t.Fatalf("replacement collection projection=%#v", snapshot)
	}
}

func TestAgentProfileRegistryReplaceExactCanonicalMatchAndCapacity(t *testing.T) {
	scope := MustScopePath(ScopeRef{Kind: ScopeGlobal, ID: "global"})
	registry := NewAgentProfileRegistry()
	registry.maxBindings = 1
	old := replaceProfileLayer(scope, "canonical.agent", "old", "old")
	old.Metadata = nil
	old.AddCapabilities = nil
	old.RemoveCapabilities = nil
	old.PutFragments = nil
	old.RemoveFragments = nil
	if _, err := registry.Mount(old); err != nil {
		t.Fatal(err)
	}
	beforeNext, beforeCount := registry.next, len(registry.flat)
	canonicalOld := CloneAgentProfileLayer(old)
	canonicalOld.Metadata = map[string]string{}
	canonicalOld.AddCapabilities = []string{}
	canonicalOld.RemoveCapabilities = []string{}
	canonicalOld.PutFragments = []PromptFragment{}
	canonicalOld.RemoveFragments = []string{}
	next := replaceProfileLayer(scope, old.ProfileID, "new", "new")
	if err := registry.ReplaceExact(canonicalOld, next); err != nil {
		t.Fatalf("canonical-value replacement failed: %v", err)
	}
	if registry.next != beforeNext || len(registry.flat) != beforeCount {
		t.Fatalf("replacement changed registry allocation state: next=%d/%d count=%d/%d", registry.next, beforeNext, len(registry.flat), beforeCount)
	}
}

func TestAgentProfileRegistryReplaceExactDistinguishesOptionalOverrides(t *testing.T) {
	scope := MustScopePath(ScopeRef{Kind: ScopeGlobal, ID: "global"})
	registry := NewAgentProfileRegistry()
	old := replaceProfileLayer(scope, "optional.agent", "old", "old")
	if _, err := registry.Mount(old); err != nil {
		t.Fatal(err)
	}
	empty := ""
	wrong := CloneAgentProfileLayer(old)
	wrong.Name = &empty
	if err := registry.ReplaceExact(wrong, replaceProfileLayer(scope, old.ProfileID, "new", "new")); err == nil {
		t.Fatal("nil and explicit empty scalar override matched")
	}
}

func fullReplaceProfileLayer(scope ScopePath, id, name, pair string) AgentProfileLayer {
	description := name + " description"
	model := ModelSelection{Provider: "mock-" + name, Model: name}
	steps, calls := 7, 9
	return AgentProfileLayer{
		Scope: scope, ProfileID: id, Extends: "base.agent", Name: &name, Description: &description, Model: &model, MaxSteps: &steps, MaxToolCalls: &calls,
		AddCapabilities: []string{"cap." + name}, RemoveCapabilities: []string{"cap.alpha"},
		PutFragments:    []PromptFragment{{ID: name + ".fragment", Section: PromptValues, Content: name + " fragment", Priority: 4}},
		RemoveFragments: []string{"base.stay"}, Metadata: map[string]string{"replace": pair},
	}
}

func fragmentIDs(fragments []ResolvedPromptFragment) []string {
	ids := make([]string, len(fragments))
	for index, fragment := range fragments {
		ids[index] = fragment.ID
	}
	return ids
}

type registryProfileState struct {
	next uint64
	flat []storedProfileLayer
}

func captureRegistryProfileState(registry *AgentProfileRegistry) registryProfileState {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	flat := make([]storedProfileLayer, len(registry.flat))
	for index, layer := range registry.flat {
		flat[index] = storedProfileLayer{AgentProfileLayer: cloneProfileLayer(layer.AgentProfileLayer), order: layer.order}
	}
	return registryProfileState{next: registry.next, flat: flat}
}
