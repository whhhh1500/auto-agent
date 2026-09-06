package runtime

import (
	"context"
	"testing"
)

func TestRefreshInheritedEffectsKeepsGenerationOrder(t *testing.T) {
	journal := NewMemoryEffectJournal()
	descriptor := func(id, revision string) EffectDescriptor {
		return EffectDescriptor{ID: EffectID(id), ModuleID: ModuleID("module-" + id), ModuleRevision: Version{Major: 1}, CompositionRevision: revision, Phase: EffectPhaseActivate, Forward: EffectAction{Kind: "forward", Target: id}, Inverse: EffectAction{Kind: "inverse", Target: id}}
	}
	first, err := journal.Record(context.Background(), descriptor("first", "z-generation"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := journal.Record(context.Background(), descriptor("second", "a-generation"))
	if err != nil {
		t.Fatal(err)
	}
	host := &ModuleHost{journal: journal}
	refreshed, err := host.refreshInheritedEffects(context.Background(), []RecordedEffect{first, second})
	if err != nil {
		t.Fatal(err)
	}
	if len(refreshed) != 2 || refreshed[0].Descriptor.CompositionRevision != "z-generation" || refreshed[1].Descriptor.CompositionRevision != "a-generation" {
		t.Fatalf("refresh reordered generations unexpectedly: %#v", refreshed)
	}
}

func TestRefreshInheritedEffectsTreatsNilAndEmptyPayloadAsEquivalent(t *testing.T) {
	journal := NewMemoryEffectJournal()
	descriptor := EffectDescriptor{ID: "payload-effect", ModuleID: "payload-module", ModuleRevision: Version{Major: 1}, CompositionRevision: "payload-generation", Phase: EffectPhaseActivate, Forward: EffectAction{Kind: "forward", Target: "payload", Payload: nil}, Inverse: EffectAction{Kind: "inverse", Target: "payload", Payload: nil}}
	recorded, err := journal.Record(context.Background(), descriptor)
	if err != nil {
		t.Fatal(err)
	}
	expected := recorded
	expected.Descriptor.Forward.Payload = []byte{}
	expected.Descriptor.Inverse.Payload = []byte{}
	host := &ModuleHost{journal: journal}
	if _, err := host.refreshInheritedEffects(context.Background(), []RecordedEffect{expected}); err != nil {
		t.Fatalf("nil/empty payload was treated as descriptor tampering: %v", err)
	}
}
