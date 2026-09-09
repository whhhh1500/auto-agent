package main

import (
	"context"
	"strings"
	"testing"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

func TestMemoryAuthorityLookupV5PromptIsTheOnlyScenarioDifference(t *testing.T) {
	const entity = "Node-7.Alpha"
	v4 := memoryAuthorityLookupPrompt(entity)
	v5 := memoryAuthorityLookupV5Prompt(entity)
	if v4 == v5 || !strings.Contains(v5, "Call memory.lookup and authority.current in the same response") || !strings.Contains(v5, "both are independent read-only calls") {
		t.Fatal("v5 prompt does not require parallel independent reads")
	}
	if strings.Contains(v5, "then call authority.current") || strings.Contains(v4, "same response") || strings.Contains(v4, "independent read-only") {
		t.Fatal("v4 and v5 prompt distinction changed")
	}

	values, err := newMemoryAuthorityValuesForEntity(entity)
	if err != nil {
		t.Fatal(err)
	}
	for _, variant := range []struct {
		name, prompt string
	}{
		{name: "v4", prompt: v4},
		{name: "v5", prompt: v5},
	} {
		variant := variant
		t.Run(variant.name, func(t *testing.T) {
			model := newLiveModel(t, &memoryAuthorityLookupProbe{}, "offline-memory-authority-lookup-"+variant.name, 2, nil)
			observed := &memoryObservedModel{inner: model}
			fixture, err := newMemoryAuthorityLookupFixture(observed, "offline-memory-authority-lookup", values, "offline-prompt-"+variant.name)
			if err != nil {
				t.Fatal(err)
			}
			result, err := fixture.runtime.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{RunID: "offline-memory-authority-lookup-prompt-" + variant.name, Text: variant.prompt}, nil)
			if err != nil || result.Status != core.RunCompleted {
				t.Fatal("shared lookup authority fixture did not complete")
			}
			if err := assertMemoryAuthorityLookupRun(fixture, observed, model, fixture.session.Events(), result, values); err != nil || !lookupAuthorityProfileOnly(fixture.session.Events()) || model.rounds() != 2 {
				t.Fatal("v4/v5 changed a fixture, authority, capability, or budget contract")
			}
		})
	}
}

func TestMemoryAuthorityLookupV5OfflineFrozenEntities(t *testing.T) {
	for _, entity := range []string{"release.channel/v2", "billing-policy:eu_west", "Node-7.Alpha"} {
		entity := entity
		t.Run(entity, func(t *testing.T) {
			values, err := newMemoryAuthorityValuesForEntity(entity)
			if err != nil {
				t.Fatal(err)
			}
			model := newLiveModel(t, &memoryAuthorityLookupProbe{}, "offline-memory-authority-lookup-v5", 2, nil)
			observed := &memoryObservedModel{inner: model}
			fixture, err := newMemoryAuthorityLookupFixture(observed, "offline-memory-authority-lookup", values, "offline-v5")
			if err != nil {
				t.Fatal(err)
			}
			result, err := fixture.runtime.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{RunID: "offline-memory-authority-lookup-v5", Text: memoryAuthorityLookupV5Prompt(entity)}, nil)
			if err != nil || result.Status != core.RunCompleted {
				t.Fatal("v5 lookup authority fixture did not complete")
			}
			if err := assertMemoryAuthorityLookupRun(fixture, observed, model, fixture.session.Events(), result, values); err != nil {
				t.Fatal(err)
			}
		})
	}
}
