package graph

import (
	"fmt"
	"sort"
)

// Reducer is a G1 execution seam. Graph-G0 never invokes custom reducers;
// the only reducer it applies is its built-in top-level JSON patch contract.
type Reducer interface {
	Reduce(State, StatePatch) (State, error)
}

type ReducerMetadata struct {
	ID      string `json:"id"`
	Version string `json:"version"`
}

type NodeKindMetadata struct {
	ID      string `json:"id"`
	Version string `json:"version"`
}

type PredicateMetadata struct {
	ID      string `json:"id"`
	Version string `json:"version"`
}

// ReducerRegistry is immutable existence metadata, never a reducer locator.
type ReducerRegistry struct{ entries map[string]ReducerMetadata }

func NewReducerRegistry(entries []ReducerMetadata) (ReducerRegistry, error) {
	if len(entries) > MaxRegistryEntries-1 {
		return ReducerRegistry{}, fmt.Errorf("reducer count exceeds %d", MaxRegistryEntries)
	}
	registry := ReducerRegistry{entries: make(map[string]ReducerMetadata, len(entries)+1)}
	registry.entries[ReducerTopLevelJSONPatch] = ReducerMetadata{ID: ReducerTopLevelJSONPatch, Version: "1"}
	for _, entry := range entries {
		if err := validateRegistryMetadata(entry.ID, entry.Version); err != nil {
			return ReducerRegistry{}, fmt.Errorf("reducer: %w", err)
		}
		if _, exists := registry.entries[entry.ID]; exists {
			return ReducerRegistry{}, fmt.Errorf("reducer %q is duplicate", entry.ID)
		}
		registry.entries[entry.ID] = entry
	}
	return registry, nil
}

func (r ReducerRegistry) Resolve(id string) (ReducerMetadata, bool) {
	value, ok := r.entries[id]
	return value, ok
}
func (r ReducerRegistry) IDs() []string { return sortedRegistryIDs(r.entries) }

// NodeKindRegistry is immutable existence metadata, never a node locator.
type NodeKindRegistry struct{ entries map[string]NodeKindMetadata }

func NewNodeKindRegistry(entries []NodeKindMetadata) (NodeKindRegistry, error) {
	if len(entries) > MaxRegistryEntries {
		return NodeKindRegistry{}, fmt.Errorf("node kind count exceeds %d", MaxRegistryEntries)
	}
	registry := NodeKindRegistry{entries: make(map[string]NodeKindMetadata, len(entries))}
	for _, entry := range entries {
		if err := validateRegistryMetadata(entry.ID, entry.Version); err != nil {
			return NodeKindRegistry{}, fmt.Errorf("node kind: %w", err)
		}
		if _, exists := registry.entries[entry.ID]; exists {
			return NodeKindRegistry{}, fmt.Errorf("node kind %q is duplicate", entry.ID)
		}
		registry.entries[entry.ID] = entry
	}
	return registry, nil
}

func (r NodeKindRegistry) Resolve(id string) (NodeKindMetadata, bool) {
	value, ok := r.entries[id]
	return value, ok
}
func (r NodeKindRegistry) IDs() []string { return sortedRegistryIDs(r.entries) }

// PredicateRegistry is immutable predicate metadata. G0 only verifies that a
// conditional edge names an approved predicate contract.
type PredicateRegistry struct{ entries map[string]PredicateMetadata }

func NewPredicateRegistry(entries []PredicateMetadata) (PredicateRegistry, error) {
	if len(entries) > MaxRegistryEntries {
		return PredicateRegistry{}, fmt.Errorf("predicate count exceeds %d", MaxRegistryEntries)
	}
	registry := PredicateRegistry{entries: make(map[string]PredicateMetadata, len(entries))}
	for _, entry := range entries {
		if err := validateRegistryMetadata(entry.ID, entry.Version); err != nil {
			return PredicateRegistry{}, fmt.Errorf("predicate: %w", err)
		}
		if _, exists := registry.entries[entry.ID]; exists {
			return PredicateRegistry{}, fmt.Errorf("predicate %q is duplicate", entry.ID)
		}
		registry.entries[entry.ID] = entry
	}
	return registry, nil
}

func (r PredicateRegistry) Resolve(id string) (PredicateMetadata, bool) {
	value, ok := r.entries[id]
	return value, ok
}
func (r PredicateRegistry) IDs() []string { return sortedRegistryIDs(r.entries) }

func validateRegistryMetadata(id, version string) error {
	if err := validateIdentifier(id, MaxNodeKindBytes, "registry id"); err != nil {
		return err
	}
	return validateIdentifier(version, MaxGraphVersionBytes, "registry version")
}

func sortedRegistryIDs[T any](entries map[string]T) []string {
	ids := make([]string, 0, len(entries))
	for id := range entries {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
