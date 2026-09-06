package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestDurableHostStateCloneAndDeterministicJSON(t *testing.T) {
	manifests := []ModuleManifest{
		testDurableManifest("z-module"),
		testDurableManifest("a-module"),
	}
	manifests[0].Effects = []EffectKind{"write", "read"}
	manifests[0].Optional = []Dependency{
		{ModuleID: "optional-b", Contract: "catalog", Version: VersionRange{Min: Version{Major: 1}}},
		{ModuleID: "optional-a", Contract: "catalog", Version: VersionRange{Min: Version{Major: 1}}},
	}
	manifests[0].Provides = []ProvidedExtension{
		{ID: "z-extension", Semantic: SemanticCollection, Contract: "catalog-z", Version: Version{Major: 1}},
		{ID: "a-extension", Semantic: SemanticCollection, Contract: "catalog-a", Version: Version{Major: 1}},
	}
	first := testDurableComposition(t, "composition-a", DurableCompositionActive, manifests, "")
	second := testDurableComposition(t, "composition-b", DurableCompositionBlocked, nil, first.CompositionRevision)
	left := DurableHostState{StoreRevision: "store-1", HostAPIVersion: Version{Major: 1}, Desired: manifests, Compositions: []DurableComposition{second, first}}
	reversed := DurableHostState{StoreRevision: "store-1", HostAPIVersion: Version{Major: 1}, Desired: []ModuleManifest{manifests[1], manifests[0]}, Compositions: []DurableComposition{first, second}}
	leftJSON, err := json.Marshal(left)
	if err != nil {
		t.Fatal(err)
	}
	rightJSON, err := json.Marshal(reversed)
	if err != nil {
		t.Fatal(err)
	}
	if string(leftJSON) != string(rightJSON) {
		t.Fatalf("non-deterministic JSON:\n%s\n%s", leftJSON, rightJSON)
	}

	clone := left.Clone()
	clone.Desired[0].ID = "mutated"
	clone.Desired[0].Effects[0] = "changed"
	clone.Compositions[1].Manifests[0].ID = "changed"
	if left.Desired[0].ID == "mutated" || left.Desired[0].Effects[0] == "changed" || left.Compositions[1].Manifests[0].ID == "changed" {
		t.Fatal("clone shares mutable state")
	}
}

func TestValidateDurableHostStateRejectsInvalidReferencesAndLimits(t *testing.T) {
	valid := testDurableComposition(t, "valid", DurableCompositionBlocked, nil, "")
	self := testDurableComposition(t, "self", DurableCompositionBlocked, nil, "self")
	missing := testDurableComposition(t, "child", DurableCompositionBlocked, nil, "missing")
	activeA := testDurableComposition(t, "active-a", DurableCompositionActive, nil, "")
	activeB := testDurableComposition(t, "active-b", DurableCompositionActive, nil, "")
	cycleA := testDurableComposition(t, "cycle-a", DurableCompositionBlocked, nil, "cycle-b")
	cycleB := testDurableComposition(t, "cycle-b", DurableCompositionBlocked, nil, "cycle-a")
	tests := []struct {
		name  string
		state DurableHostState
	}{
		{name: "empty store revision", state: DurableHostState{HostAPIVersion: Version{Major: 1}, Compositions: []DurableComposition{valid}}},
		{name: "invalid desired manifest", state: DurableHostState{StoreRevision: "s", HostAPIVersion: Version{Major: 1}, Desired: []ModuleManifest{{}}}},
		{name: "duplicate composition revision", state: DurableHostState{StoreRevision: "s", HostAPIVersion: Version{Major: 1}, Compositions: []DurableComposition{valid, valid}}},
		{name: "tampered snapshot", state: DurableHostState{StoreRevision: "s", HostAPIVersion: Version{Major: 1}, Compositions: []DurableComposition{{CompositionRevision: "tampered", SnapshotRevision: "rev_tampered", APIVersion: Version{Major: 1}, Status: DurableCompositionBlocked}}}},
		{name: "self supersedes", state: DurableHostState{StoreRevision: "s", HostAPIVersion: Version{Major: 1}, Compositions: []DurableComposition{self}}},
		{name: "missing supersedes", state: DurableHostState{StoreRevision: "s", HostAPIVersion: Version{Major: 1}, Compositions: []DurableComposition{missing}}},
		{name: "invalid status", state: DurableHostState{StoreRevision: "s", HostAPIVersion: Version{Major: 1}, Compositions: []DurableComposition{{CompositionRevision: "status", SnapshotRevision: valid.SnapshotRevision, APIVersion: Version{Major: 1}, Status: "unknown"}}}},
		{name: "two active", state: DurableHostState{StoreRevision: "s", HostAPIVersion: Version{Major: 1}, Compositions: []DurableComposition{activeA, activeB}}},
		{name: "supersedes cycle", state: DurableHostState{StoreRevision: "s", HostAPIVersion: Version{Major: 1}, Compositions: []DurableComposition{cycleA, cycleB}}},
		{name: "invalid revision", state: DurableHostState{StoreRevision: "bad revision", HostAPIVersion: Version{Major: 1}, Compositions: []DurableComposition{valid}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := ValidateDurableHostState(test.state); !errors.Is(err, ErrInvalidComposition) {
				t.Fatalf("validation error=%v", err)
			}
		})
	}

	tooMany := DurableHostState{StoreRevision: "s", HostAPIVersion: Version{Major: 1}, Desired: make([]ModuleManifest, MaxDurableManifestEntries+1)}
	for index := range tooMany.Desired {
		tooMany.Desired[index] = testDurableManifest(fmt.Sprintf("module-%d", index))
	}
	if err := ValidateDurableHostState(tooMany); !errors.Is(err, ErrInvalidComposition) {
		t.Fatalf("manifest aggregate limit error=%v", err)
	}

	tooManyCompositions := DurableHostState{StoreRevision: "s", HostAPIVersion: Version{Major: 1}, Compositions: make([]DurableComposition, MaxDurableCompositions+1)}
	for index := range tooManyCompositions.Compositions {
		tooManyCompositions.Compositions[index] = testDurableComposition(t, "composition-"+string(rune('a'+index%26))+string(rune('a'+index/26)), DurableCompositionBlocked, nil, "")
	}
	if err := ValidateDurableHostState(tooManyCompositions); !errors.Is(err, ErrInvalidComposition) {
		t.Fatalf("composition limit error=%v", err)
	}
}

func TestValidateDurableHostStateAllowsWaitingDesiredDependencies(t *testing.T) {
	waiting := testDurableManifest("waiting")
	waiting.Requires = []Dependency{{ModuleID: "missing", Contract: "catalog", Version: VersionRange{Min: Version{Major: 1}}}}
	state := DurableHostState{StoreRevision: "s", HostAPIVersion: Version{Major: 1}, Desired: []ModuleManifest{waiting}}
	if err := ValidateDurableHostState(state); err != nil {
		t.Fatalf("waiting desired state rejected: %v", err)
	}
}

func TestRetainedLineageBoundFailsClosed(t *testing.T) {
	compositions := make([]DurableComposition, MaxDurableCompositions+1)
	for index := range compositions {
		revision := fmt.Sprintf("retained-%d", index)
		previous := ""
		if index > 0 {
			previous = fmt.Sprintf("retained-%d", index-1)
		}
		compositions[index] = testDurableComposition(t, revision, DurableCompositionRetained, nil, previous)
	}
	state := DurableHostState{StoreRevision: "store-retained", HostAPIVersion: Version{Major: 1}, Compositions: compositions}
	if err := ValidateDurableHostState(state); !errors.Is(err, ErrInvalidComposition) {
		t.Fatalf("retained lineage was not bounded: %v", err)
	}
}

func TestMemoryCompositionStoreStrictCASAndContext(t *testing.T) {
	store := NewMemoryCompositionStore()
	if _, found, err := store.Load(context.Background()); err != nil || found {
		t.Fatalf("initial load state=%v found=%v", err, found)
	}
	first := DurableHostState{StoreRevision: "rev-1", HostAPIVersion: Version{Major: 1}, Desired: []ModuleManifest{testDurableManifest("one")}}
	if err := store.CompareAndSwap(context.Background(), "", first); err != nil {
		t.Fatal(err)
	}
	loaded, found, err := store.Load(context.Background())
	if err != nil || !found || loaded.StoreRevision != first.StoreRevision {
		t.Fatalf("loaded=%#v found=%v err=%v", loaded, found, err)
	}
	loaded.Desired[0].ID = "mutated"
	again, _, err := store.Load(context.Background())
	if err != nil || again.Desired[0].ID != "one" {
		t.Fatalf("load was not defensive: %#v err=%v", again, err)
	}
	second := first.Clone()
	second.StoreRevision = "rev-2"
	if err := store.CompareAndSwap(context.Background(), "wrong", second); !errors.Is(err, ErrCompositionConflict) {
		t.Fatalf("wrong expected error=%v", err)
	}
	if err := store.CompareAndSwap(context.Background(), first.StoreRevision, first); !errors.Is(err, ErrCompositionConflict) {
		t.Fatalf("same revision error=%v", err)
	}
	if err := store.CompareAndSwap(context.Background(), "", second); !errors.Is(err, ErrCompositionConflict) {
		t.Fatalf("empty expected on existing state error=%v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := store.Load(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled load error=%v", err)
	}
	if err := store.CompareAndSwap(canceled, first.StoreRevision, second); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled CAS error=%v", err)
	}
}

func TestMemoryCompositionStoreConcurrentCASHasSingleWinner(t *testing.T) {
	store := NewMemoryCompositionStore()
	initial := DurableHostState{StoreRevision: "initial", HostAPIVersion: Version{Major: 1}, Desired: []ModuleManifest{testDurableManifest("one")}}
	if err := store.CompareAndSwap(context.Background(), "", initial); err != nil {
		t.Fatal(err)
	}
	const writers = 8
	errs := make(chan error, writers)
	var wg sync.WaitGroup
	for index := 0; index < writers; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			next := initial.Clone()
			next.StoreRevision = "next-" + string(rune('a'+index))
			errs <- store.CompareAndSwap(context.Background(), initial.StoreRevision, next)
		}(index)
	}
	wg.Wait()
	close(errs)
	winners, conflicts := 0, 0
	for err := range errs {
		if err == nil {
			winners++
		} else if errors.Is(err, ErrCompositionConflict) {
			conflicts++
		} else {
			t.Fatalf("CAS error=%v", err)
		}
	}
	if winners != 1 || conflicts != writers-1 {
		t.Fatalf("winners=%d conflicts=%d", winners, conflicts)
	}
}

func testDurableManifest(id string) ModuleManifest {
	return ModuleManifest{ID: ModuleID(id), Version: Version{Major: 1}, CompatibleAPI: VersionRange{Min: Version{Major: 1}}}
}

func testDurableComposition(t *testing.T, revision string, status DurableCompositionStatus, manifests []ModuleManifest, supersedes string) DurableComposition {
	t.Helper()
	snapshot, err := BuildSnapshotForAPI(manifests, Version{Major: 1})
	if err != nil {
		t.Fatal(err)
	}
	return DurableComposition{CompositionRevision: revision, SnapshotRevision: snapshot.Revision(), APIVersion: Version{Major: 1}, Status: status, Supersedes: supersedes, Manifests: manifests}
}
