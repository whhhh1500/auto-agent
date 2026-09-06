package runtime

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func extension(id ExtensionID, semantic ExtensionSemantic, contract ContractID, priority int) ProvidedExtension {
	return ProvidedExtension{ID: id, Semantic: semantic, Contract: contract, Version: Version{Major: 1}, Priority: priority, ResourceKey: string(id)}
}

func TestSemanticValidatorsRejectConflicts(t *testing.T) {
	tests := []struct {
		name       string
		check      func([]ProvidedExtension) error
		extensions []ProvidedExtension
	}{
		{"collection duplicate priority", ValidateCollection, []ProvidedExtension{
			extension("a", SemanticCollection, "catalog", 1), extension("b", SemanticCollection, "catalog", 1),
		}},
		{"single strategy duplicate role", ValidateSingleStrategy, []ProvidedExtension{
			{ID: "a", Semantic: SemanticSingleStrategy, Contract: "router", Version: Version{Major: 1}, ResourceKey: "primary"},
			{ID: "b", Semantic: SemanticSingleStrategy, Contract: "router", Version: Version{Major: 1}, ResourceKey: "primary"},
		}},
		{"stateful resource duplicate key", ValidateStatefulResource, []ProvidedExtension{
			{ID: "a", Semantic: SemanticStatefulResource, Contract: "worker", Version: Version{Major: 1}, ResourceKey: "shared"},
			{ID: "b", Semantic: SemanticStatefulResource, Contract: "worker", Version: Version{Major: 1}, ResourceKey: "shared"},
		}},
		{"wrong semantic", ValidateCollection, []ProvidedExtension{extension("a", SemanticPipeline, "catalog", 0)}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.check(test.extensions); !errors.Is(err, ErrSemanticConflict) {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestSnapshotExtensionBound(t *testing.T) {
	build := func(total int) []ModuleManifest {
		manifests := make([]ModuleManifest, 0, (total+MaxManifestProvides-1)/MaxManifestProvides)
		remaining := total
		for moduleIndex := 0; remaining > 0; moduleIndex++ {
			offset := total - remaining
			count := remaining
			if count > MaxManifestProvides {
				count = MaxManifestProvides
			}
			provides := make([]ProvidedExtension, 0, count)
			for extensionIndex := 0; extensionIndex < count; extensionIndex++ {
				provides = append(provides, extension(ExtensionID(fmt.Sprintf("m%d-e%d", moduleIndex, extensionIndex)), SemanticCollection, "catalog", offset+extensionIndex))
			}
			manifests = append(manifests, ModuleManifest{ID: ModuleID(fmt.Sprintf("m%d", moduleIndex)), Version: Version{Major: 1}, Provides: provides})
			remaining -= count
		}
		return manifests
	}
	if _, err := BuildSnapshot(build(MaxSnapshotExtensions)); err != nil {
		t.Fatalf("bound-sized snapshot rejected: %v", err)
	}
	if _, err := BuildSnapshot(build(MaxSnapshotExtensions + 1)); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("over-bound snapshot err=%v", err)
	}
}

func TestOrderedEffectsSortsAndRejectsMalformedJournalOutput(t *testing.T) {
	first := RecordedEffect{Descriptor: EffectDescriptor{ID: "first"}, Ordinal: 1}
	second := RecordedEffect{Descriptor: EffectDescriptor{ID: "second"}, Ordinal: 2}
	ordered, err := orderedEffects([]RecordedEffect{second, first})
	if err != nil || ordered[0].Descriptor.ID != "first" || ordered[1].Descriptor.ID != "second" {
		t.Fatalf("unordered journal output ordered=%v err=%v", ordered, err)
	}
	if _, err := orderedEffects([]RecordedEffect{{Descriptor: EffectDescriptor{ID: "missing"}}}); !errors.Is(err, ErrInvalidEffect) {
		t.Fatalf("missing ordinal err=%v", err)
	}
	if _, err := orderedEffects([]RecordedEffect{{Ordinal: 1}, {Ordinal: 1}}); !errors.Is(err, ErrInvalidEffect) {
		t.Fatalf("duplicate ordinal err=%v", err)
	}
}

func TestPipelineOrderingAndCycles(t *testing.T) {
	a := extension("a", SemanticPipeline, "context", 0)
	b := extension("b", SemanticPipeline, "context", 0)
	a.Before = []ExtensionID{"b"}
	b.After = []ExtensionID{"a"}
	if err := ValidatePipeline([]ProvidedExtension{a, b}); err != nil {
		t.Fatal(err)
	}
	b.After = nil
	b.Before = []ExtensionID{"a"}
	if err := ValidatePipeline([]ProvidedExtension{a, b}); !errors.Is(err, ErrDependencyCycle) {
		t.Fatalf("cycle err=%v", err)
	}
	duplicateTarget := extension("duplicate-target", SemanticPipeline, "context", 0)
	duplicateTarget.Before = []ExtensionID{"a", "a"}
	if err := ValidatePipeline([]ProvidedExtension{duplicateTarget, a}); !errors.Is(err, ErrSemanticConflict) {
		t.Fatalf("duplicate ordering target err=%v", err)
	}
	bothDirections := extension("both-directions", SemanticPipeline, "context", 0)
	bothDirections.Before = []ExtensionID{"a"}
	bothDirections.After = []ExtensionID{"a"}
	if err := ValidatePipeline([]ProvidedExtension{bothDirections, a}); !errors.Is(err, ErrSemanticConflict) {
		t.Fatalf("same target before and after err=%v", err)
	}
	left := extension("left", SemanticPipeline, "context", 1)
	right := extension("right", SemanticPipeline, "context", 1)
	if err := ValidatePipeline([]ProvidedExtension{left, right}); !errors.Is(err, ErrSemanticConflict) {
		t.Fatalf("undeclared equal-priority order err=%v", err)
	}
	telemetry := extension("telemetry", SemanticPipeline, "telemetry", 1)
	if err := ValidatePipeline([]ProvidedExtension{left, telemetry}); err != nil {
		t.Fatalf("different pipeline contracts should be independent: %v", err)
	}
	crossContract := extension("cross", SemanticPipeline, "telemetry", 0)
	crossContract.Before = []ExtensionID{"left"}
	if err := ValidatePipeline([]ProvidedExtension{left, crossContract}); !errors.Is(err, ErrSemanticConflict) {
		t.Fatalf("cross-contract ordering err=%v", err)
	}
	transitiveA := extension("transitive-a", SemanticPipeline, "context", 1)
	transitiveB := extension("transitive-b", SemanticPipeline, "context", 1)
	transitiveC := extension("transitive-c", SemanticPipeline, "context", 1)
	transitiveA.Before = []ExtensionID{transitiveB.ID}
	transitiveB.Before = []ExtensionID{transitiveC.ID}
	if err := ValidatePipeline([]ProvidedExtension{transitiveC, transitiveA, transitiveB}); err != nil {
		t.Fatalf("transitive order should disambiguate: %v", err)
	}
}

func TestCollectionPriorityConflictIsPerContract(t *testing.T) {
	left := extension("left", SemanticCollection, "catalog", 1)
	right := extension("right", SemanticCollection, "policy", 1)
	if err := ValidateCollection([]ProvidedExtension{left, right}); err != nil {
		t.Fatalf("unrelated collection contracts should not conflict: %v", err)
	}
}

func TestResourceKeyConflictIsPerContract(t *testing.T) {
	left := extension("left", SemanticSingleStrategy, "queue", 0)
	right := extension("right", SemanticSingleStrategy, "router", 0)
	left.ResourceKey, right.ResourceKey = "primary", "primary"
	if err := ValidateSingleStrategy([]ProvidedExtension{left, right}); err != nil {
		t.Fatalf("unrelated strategy contracts should not conflict: %v", err)
	}
	overlayLeft := extension("overlay-left", SemanticScopedOverlay, "policy", 0)
	overlayRight := extension("overlay-right", SemanticScopedOverlay, "credential", 0)
	overlayLeft.ResourceKey, overlayRight.ResourceKey = "default", "default"
	if err := ValidateScopedOverlay([]ProvidedExtension{overlayLeft, overlayRight}); err != nil {
		t.Fatalf("unrelated overlay contracts should not conflict: %v", err)
	}
}

func TestPipelineSnapshotUsesStableTopologicalOrder(t *testing.T) {
	a := extension("a", SemanticPipeline, "context", 10)
	b := extension("b", SemanticPipeline, "context", 1)
	c := extension("c", SemanticPipeline, "context", 0)
	a.Before = []ExtensionID{"b"}
	b.Before = []ExtensionID{"c"}
	manifest := ModuleManifest{ID: "pipeline", Version: Version{Major: 1}, Provides: []ProvidedExtension{c, a, b}}
	snapshot, err := BuildSnapshot([]ModuleManifest{manifest})
	if err != nil {
		t.Fatal(err)
	}
	got := snapshot.Extensions()
	if ids := []ExtensionID{got[0].ID, got[1].ID, got[2].ID}; !reflect.DeepEqual(ids, []ExtensionID{"a", "b", "c"}) {
		t.Fatalf("pipeline order=%v", ids)
	}
	left := extension("left", SemanticPipeline, "context", 0)
	right := extension("right", SemanticPipeline, "other", 0)
	left.Before = []ExtensionID{"right"}
	if err := ValidatePipeline([]ProvidedExtension{left, right}); !errors.Is(err, ErrSemanticConflict) {
		t.Fatalf("incompatible pipeline edge err=%v", err)
	}
	contextStage := extension("context-stage", SemanticPipeline, "context", 1)
	telemetryStage := extension("telemetry-stage", SemanticPipeline, "telemetry", 1)
	first, err := BuildSnapshot([]ModuleManifest{{ID: "multi-pipeline", Version: Version{Major: 1}, Provides: []ProvidedExtension{telemetryStage, contextStage}}})
	if err != nil {
		t.Fatal(err)
	}
	second, err := BuildSnapshot([]ModuleManifest{{ID: "multi-pipeline", Version: Version{Major: 1}, Provides: []ProvidedExtension{contextStage, telemetryStage}}})
	if err != nil {
		t.Fatal(err)
	}
	if first.Revision() != second.Revision() {
		t.Fatalf("independent contract declaration order changed revision: %s != %s", first.Revision(), second.Revision())
	}
	ordered := first.Extensions()
	if ids := []ExtensionID{ordered[0].ID, ordered[1].ID}; !reflect.DeepEqual(ids, []ExtensionID{"context-stage", "telemetry-stage"}) {
		t.Fatalf("independent pipeline order=%v", ids)
	}
}

func TestValidationRejectsGlobalIDsEmptyOverlayAndEffects(t *testing.T) {
	emptyOverlay := ProvidedExtension{ID: "overlay", Semantic: SemanticScopedOverlay, Contract: "policy", Version: Version{Major: 1}}
	if err := ValidateScopedOverlay([]ProvidedExtension{emptyOverlay}); !errors.Is(err, ErrSemanticConflict) {
		t.Fatalf("empty scoped overlay key err=%v", err)
	}
	first := extension("shared", SemanticCollection, "catalog", 0)
	second := extension("shared", SemanticSingleStrategy, "router", 1)
	if _, err := BuildSnapshot([]ModuleManifest{
		{ID: "one", Version: Version{Major: 1}, Provides: []ProvidedExtension{first}},
		{ID: "two", Version: Version{Major: 1}, Provides: []ProvidedExtension{second}},
	}); !errors.Is(err, ErrSemanticConflict) {
		t.Fatalf("global extension ID err=%v", err)
	}
	invalidEffect := ModuleManifest{ID: "effects", Version: Version{Major: 1}, Effects: []EffectKind{"bad\x00effect"}}
	if err := ValidateManifest(invalidEffect); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("control effect err=%v", err)
	}
	duplicateEffects := ModuleManifest{ID: "effects", Version: Version{Major: 1}, Effects: []EffectKind{"read", "read"}}
	if err := ValidateManifest(duplicateEffects); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("duplicate effect err=%v", err)
	}
	longKey := extension("long", SemanticStatefulResource, "worker", 0)
	longKey.ResourceKey = strings.Repeat("x", 129)
	if err := ValidateStatefulResource([]ProvidedExtension{longKey}); !errors.Is(err, ErrInvalidExtension) {
		t.Fatalf("long resource key err=%v", err)
	}
}

func TestManifestCloneDeepCopiesDependencyRange(t *testing.T) {
	max := Version{Major: 2}
	manifest := ModuleManifest{
		ID:       "consumer",
		Version:  Version{Major: 1},
		Requires: []Dependency{{ModuleID: "provider", Contract: "catalog", Version: VersionRange{Max: &max}}},
	}
	provider := ModuleManifest{ID: "provider", Version: Version{Major: 1}, Provides: []ProvidedExtension{extension("provider.ext", SemanticCollection, "catalog", 0)}}
	snapshot, err := BuildSnapshot([]ModuleManifest{manifest, provider})
	if err != nil {
		t.Fatal(err)
	}
	max.Major = 99
	got, ok := snapshot.Manifest("consumer")
	if !ok || got.Requires[0].Version.Max == nil || got.Requires[0].Version.Max.Major != 2 {
		t.Fatalf("snapshot dependency max changed after source mutation: %#v", got)
	}
	returned := snapshot.Manifests()
	returned[0].Requires[0].Version.Max.Major = 88
	got, _ = snapshot.Manifest("consumer")
	if got.Requires[0].Version.Max.Major != 2 {
		t.Fatalf("snapshot dependency max changed through returned clone: %#v", got)
	}
}

func TestDependencyValidationRequiredOptionalAndCycles(t *testing.T) {
	provider := ModuleManifest{ID: "provider", Version: Version{Major: 1}, Provides: []ProvidedExtension{extension("provider.ext", SemanticCollection, "catalog", 0)}}
	required := ModuleManifest{ID: "required", Version: Version{Major: 1}, Requires: []Dependency{{ModuleID: "provider", Contract: "catalog", Version: VersionRange{Min: Version{Major: 1}, Max: &Version{Major: 1}}}}}
	if _, err := BuildSnapshot([]ModuleManifest{required}); !errors.Is(err, ErrDependency) {
		t.Fatalf("missing required err=%v", err)
	}
	snapshot, err := BuildSnapshot([]ModuleManifest{provider, required})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Revision() == "" || len(snapshot.Manifests()) != 2 || len(snapshot.Extensions()) != 1 {
		t.Fatalf("snapshot=%#v", snapshot)
	}
	optional := ModuleManifest{ID: "optional", Version: Version{Major: 1}, Optional: []Dependency{{ModuleID: "missing", Contract: "catalog"}}}
	if _, err := BuildSnapshot([]ModuleManifest{optional}); err != nil {
		t.Fatalf("optional absence should be tolerated: %v", err)
	}
	a := ModuleManifest{ID: "a", Version: Version{Major: 1}, Provides: []ProvidedExtension{extension("a.ext", SemanticCollection, "catalog", 0)}, Requires: []Dependency{{ModuleID: "b", Contract: "catalog"}}}
	b := ModuleManifest{ID: "b", Version: Version{Major: 1}, Provides: []ProvidedExtension{extension("b.ext", SemanticCollection, "catalog", 0)}, Requires: []Dependency{{ModuleID: "a", Contract: "catalog"}}}
	if _, err := BuildSnapshot([]ModuleManifest{a, b}); !errors.Is(err, ErrDependencyCycle) {
		t.Fatalf("module cycle err=%v", err)
	}
}

func TestSnapshotIsDeterministicAndCloned(t *testing.T) {
	a := ModuleManifest{ID: "z", Version: Version{Major: 1}, Provides: []ProvidedExtension{extension("z.ext", SemanticCollection, "catalog", 2)}}
	b := ModuleManifest{ID: "a", Version: Version{Major: 1}, Provides: []ProvidedExtension{extension("a.ext", SemanticCollection, "catalog", 1)}}
	left, err := BuildSnapshot([]ModuleManifest{a, b})
	if err != nil {
		t.Fatal(err)
	}
	right, err := BuildSnapshot([]ModuleManifest{b, a})
	if err != nil {
		t.Fatal(err)
	}
	if left.Revision() != right.Revision() || !reflect.DeepEqual(left.Manifests(), right.Manifests()) {
		t.Fatalf("order changed snapshot: left=%s right=%s", left.Revision(), right.Revision())
	}
	manifests := left.Manifests()
	manifests[0].ID = "mutated"
	manifests[0].Provides[0].Before = append(manifests[0].Provides[0].Before, "x")
	if got, ok := left.Manifest("a"); !ok || got.ID != "a" || len(got.Provides[0].Before) != 0 {
		t.Fatalf("snapshot was mutable through clone: %#v", got)
	}
}

func TestSnapshotRevisionIgnoresDeclarationOrder(t *testing.T) {
	a := extension("a", SemanticCollection, "catalog", 0)
	b := extension("b", SemanticCollection, "catalog", 1)
	left, err := BuildSnapshot([]ModuleManifest{{ID: "module", Version: Version{Major: 1}, Provides: []ProvidedExtension{b, a}}})
	if err != nil {
		t.Fatal(err)
	}
	right, err := BuildSnapshot([]ModuleManifest{{ID: "module", Version: Version{Major: 1}, Provides: []ProvidedExtension{a, b}}})
	if err != nil {
		t.Fatal(err)
	}
	if left.Revision() != right.Revision() {
		t.Fatalf("declaration order changed revision: %s != %s", left.Revision(), right.Revision())
	}
	encoded, err := json.Marshal(left)
	if err != nil || !strings.Contains(string(encoded), left.Revision()) {
		t.Fatalf("snapshot canonical JSON=%s err=%v", encoded, err)
	}
}

func TestCrossModulePipelineAndHostAPICompatibility(t *testing.T) {
	a := extension("module-a-stage", SemanticPipeline, "context", 0)
	a.Before = []ExtensionID{"module-b-stage"}
	b := extension("module-b-stage", SemanticPipeline, "context", 0)
	if _, err := BuildSnapshotForAPI([]ModuleManifest{
		{ID: "module-a", Version: Version{Major: 1}, Provides: []ProvidedExtension{a}},
		{ID: "module-b", Version: Version{Major: 1}, Provides: []ProvidedExtension{b}},
	}, Version{Major: 1}); err != nil {
		t.Fatalf("cross-module pipeline should validate: %v", err)
	}
	manifest := ModuleManifest{ID: "api", Version: Version{Major: 1}, CompatibleAPI: VersionRange{Min: Version{Major: 2}}}
	if _, err := BuildSnapshotForAPI([]ModuleManifest{manifest}, Version{Major: 1}); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("incompatible host API err=%v", err)
	}
}

func TestManifestAndSnapshotBoundsFailFast(t *testing.T) {
	tooManyDeps := ModuleManifest{ID: "deps", Version: Version{Major: 1}, Requires: make([]Dependency, MaxManifestDependencies+1)}
	for index := range tooManyDeps.Requires {
		tooManyDeps.Requires[index] = Dependency{ModuleID: ModuleID(fmt.Sprintf("module-%d", index)), Contract: "contract"}
	}
	if err := ValidateManifest(tooManyDeps); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("dependency bound err=%v", err)
	}
	tooManyTargets := extension("targets", SemanticPipeline, "context", 0)
	tooManyTargets.Before = make([]ExtensionID, MaxExtensionOrderingTargets+1)
	for index := range tooManyTargets.Before {
		tooManyTargets.Before[index] = ExtensionID(fmt.Sprintf("target-%d", index))
	}
	if err := ValidateManifest(ModuleManifest{ID: "targets", Version: Version{Major: 1}, Provides: []ProvidedExtension{tooManyTargets}}); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("ordering bound err=%v", err)
	}
	if _, err := BuildSnapshot(make([]ModuleManifest, MaxModules+1)); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("module bound err=%v", err)
	}
}

func FuzzValidateExtensionNeverPanics(f *testing.F) {
	f.Add("id", "contract", int8(1))
	f.Add("../bad", "", int8(-1))
	f.Fuzz(func(t *testing.T, id, contract string, priority int8) {
		_ = ValidateExtension(ProvidedExtension{ID: ExtensionID(id), Semantic: SemanticCollection, Contract: ContractID(contract), Version: Version{Major: int(priority)}, Priority: int(priority)})
	})
}

func FuzzBuildSnapshotNeverPanics(f *testing.F) {
	f.Add("module", "extension", "contract")
	f.Add("../bad", "", "\x00")
	f.Fuzz(func(t *testing.T, module, extensionID, contract string) {
		_, _ = BuildSnapshot([]ModuleManifest{{
			ID:      ModuleID(module),
			Version: Version{Major: 1},
			Provides: []ProvidedExtension{{
				ID: ExtensionID(extensionID), Semantic: SemanticPipeline,
				Contract: ContractID(contract), Version: Version{Major: 1},
			}},
		}})
	})
}
