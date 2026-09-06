package graph

import (
	"strconv"
	"testing"
)

func TestRegistriesAreMetadataOnlyAndStable(t *testing.T) {
	nodes, err := NewNodeKindRegistry([]NodeKindMetadata{{ID: "b", Version: "1"}, {ID: "a", Version: "1"}})
	if err != nil {
		t.Fatal(err)
	}
	if got := nodes.IDs(); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("IDs=%v", got)
	}
	metadata, ok := nodes.Resolve("a")
	if !ok || metadata.Version != "1" {
		t.Fatalf("resolve=%#v ok=%v", metadata, ok)
	}
	if _, err := NewNodeKindRegistry([]NodeKindMetadata{{ID: "a", Version: "1"}, {ID: "a", Version: "1"}}); err == nil {
		t.Fatal("duplicate registry id accepted")
	}
	reducers, err := NewReducerRegistry(nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := reducers.Resolve(ReducerTopLevelJSONPatch); !ok {
		t.Fatal("built-in reducer metadata missing")
	}
	tooMany := make([]NodeKindMetadata, MaxRegistryEntries+1)
	for index := range tooMany {
		tooMany[index] = NodeKindMetadata{ID: "kind" + string(rune('a'+index%26)) + string(rune('A'+index/26)), Version: "1"}
	}
	if _, err := NewNodeKindRegistry(tooMany); err == nil {
		t.Fatal("registry entry limit accepted")
	}
	customReducers := make([]ReducerMetadata, MaxRegistryEntries-1)
	for index := range customReducers {
		customReducers[index] = ReducerMetadata{ID: "reducer" + strconv.Itoa(index), Version: "1"}
	}
	registry, err := NewReducerRegistry(customReducers)
	if err != nil || len(registry.IDs()) != MaxRegistryEntries {
		t.Fatalf("final reducer registry boundary err=%v ids=%d", err, len(registry.IDs()))
	}
	if _, err := NewReducerRegistry(append(customReducers, ReducerMetadata{ID: "one-too-many", Version: "1"})); err == nil {
		t.Fatal("reducer registry exceeded final-entry ceiling")
	}
}
