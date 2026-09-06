package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
)

// DurableCompositionStatus is the persisted lifecycle checkpoint for one
// composition. Prepared covers the crash window before or during effect
// commit; active is the durable composition commit point.
type DurableCompositionStatus string

const (
	DurableCompositionPrepared DurableCompositionStatus = "prepared"
	DurableCompositionActive   DurableCompositionStatus = "active"
	DurableCompositionDraining DurableCompositionStatus = "draining"
	DurableCompositionBlocked  DurableCompositionStatus = "blocked"
	// Retained is an inactive lineage record kept while a newer composition
	// still references it through Supersedes for inverse/effect provenance.
	DurableCompositionRetained DurableCompositionStatus = "retained"
	// The bounded lineage is intentional. A longer retained chain fails
	// closed at validation rather than silently dropping inverse provenance.
	MaxDurableCompositions     = 64
	MaxDurableManifestEntries  = MaxModules * 4
	MaxDurableExtensionEntries = MaxSnapshotExtensions * 4
)

func (status DurableCompositionStatus) Valid() bool {
	switch status {
	case DurableCompositionPrepared, DurableCompositionActive, DurableCompositionDraining, DurableCompositionBlocked, DurableCompositionRetained:
		return true
	default:
		return false
	}
}

// DurableComposition is the immutable composition metadata retained in the
// host state document. SnapshotRevision must be the canonical revision of
// Manifests for APIVersion.
type DurableComposition struct {
	CompositionRevision string                   `json:"composition_revision"`
	SnapshotRevision    string                   `json:"snapshot_revision"`
	APIVersion          Version                  `json:"api_version"`
	Status              DurableCompositionStatus `json:"status"`
	Supersedes          string                   `json:"supersedes,omitempty"`
	Manifests           []ModuleManifest         `json:"manifests"`
}

// DurableHostState is the single lightweight CAS document for runtime
// composition state. Revisions are supplied by the owning repository and are
// opaque to runtime beyond the shared identifier safety rules.
type DurableHostState struct {
	StoreRevision  string               `json:"store_revision"`
	HostAPIVersion Version              `json:"host_api_version"`
	Desired        []ModuleManifest     `json:"desired"`
	Compositions   []DurableComposition `json:"compositions"`
}

// CompositionStore persists and atomically replaces DurableHostState. An
// empty expected revision is valid only for the first creation.
type CompositionStore interface {
	Load(ctx context.Context) (DurableHostState, bool, error)
	CompareAndSwap(ctx context.Context, expectedRevision string, next DurableHostState) error
}

var (
	ErrCompositionConflict = errors.New("composition state conflict")
	ErrInvalidComposition  = errors.New("invalid composition state")
)

// ValidateDurableHostState validates all document invariants without mutating
// state. Composition snapshots are checked against the canonical snapshot
// builder, so a tampered or stale SnapshotRevision is rejected.
func ValidateDurableHostState(state DurableHostState) error {
	if err := validateDurableRevision(state.StoreRevision, "store"); err != nil {
		return err
	}
	if !state.HostAPIVersion.Valid() {
		return fmt.Errorf("%w: host API version is invalid", ErrInvalidComposition)
	}
	if len(state.Compositions) > MaxDurableCompositions {
		return fmt.Errorf("%w: composition count exceeds %d", ErrInvalidComposition, MaxDurableCompositions)
	}
	seenModules := make(map[ModuleID]struct{}, len(state.Desired))
	totalManifests := len(state.Desired)
	totalExtensions := 0
	desiredByModule := make(map[ModuleID]ModuleManifest, len(state.Desired))
	for _, manifest := range state.Desired {
		if err := ValidateManifest(manifest); err != nil {
			return fmt.Errorf("%w: desired manifest: %v", ErrInvalidComposition, err)
		}
		if _, exists := seenModules[manifest.ID]; exists {
			return fmt.Errorf("%w: duplicate desired module %q", ErrInvalidComposition, manifest.ID)
		}
		seenModules[manifest.ID] = struct{}{}
		if !manifest.CompatibleAPI.Contains(state.HostAPIVersion) {
			return fmt.Errorf("%w: desired module %q is incompatible with host API %s", ErrInvalidComposition, manifest.ID, state.HostAPIVersion)
		}
		desiredByModule[manifest.ID] = manifest.Clone()
		totalExtensions += len(manifest.Provides)
	}
	// Missing required dependencies are allowed to remain waiting. The
	// routable subset still gets full cross-module and semantic validation.
	if _, err := BuildSnapshotForAPI(routableManifests(desiredByModule), state.HostAPIVersion); err != nil {
		return fmt.Errorf("%w: desired snapshot: %v", ErrInvalidComposition, err)
	}
	seenRevisions := make(map[string]struct{}, len(state.Compositions))
	active, prepared := 0, 0
	for _, composition := range state.Compositions {
		if err := validateDurableRevision(composition.CompositionRevision, "composition"); err != nil {
			return err
		}
		if err := validateDurableRevision(composition.SnapshotRevision, "snapshot"); err != nil {
			return err
		}
		if _, exists := seenRevisions[composition.CompositionRevision]; exists {
			return fmt.Errorf("%w: duplicate composition revision %q", ErrInvalidComposition, composition.CompositionRevision)
		}
		seenRevisions[composition.CompositionRevision] = struct{}{}
		if composition.Supersedes == composition.CompositionRevision {
			return fmt.Errorf("%w: composition %q supersedes itself", ErrInvalidComposition, composition.CompositionRevision)
		}
		if !composition.APIVersion.Valid() || !composition.Status.Valid() {
			return fmt.Errorf("%w: composition %q API version or status is invalid", ErrInvalidComposition, composition.CompositionRevision)
		}
		if composition.APIVersion != state.HostAPIVersion {
			return fmt.Errorf("%w: composition %q API version differs from host", ErrInvalidComposition, composition.CompositionRevision)
		}
		if composition.Status == DurableCompositionActive {
			active++
		}
		if composition.Status == DurableCompositionPrepared {
			prepared++
		}
		snapshot, err := BuildSnapshotForAPI(composition.Manifests, composition.APIVersion)
		if err != nil {
			return fmt.Errorf("%w: composition %q snapshot: %v", ErrInvalidComposition, composition.CompositionRevision, err)
		}
		if snapshot.Revision() != composition.SnapshotRevision {
			return fmt.Errorf("%w: composition %q snapshot revision does not match manifests", ErrInvalidComposition, composition.CompositionRevision)
		}
		totalManifests += len(composition.Manifests)
		for _, manifest := range composition.Manifests {
			totalExtensions += len(manifest.Provides)
		}
	}
	if active > 1 || prepared > 1 {
		return fmt.Errorf("%w: at most one active and one prepared composition are allowed", ErrInvalidComposition)
	}
	if totalManifests > MaxDurableManifestEntries || totalExtensions > MaxDurableExtensionEntries {
		return fmt.Errorf("%w: aggregate manifest or extension limit exceeded", ErrInvalidComposition)
	}
	for _, composition := range state.Compositions {
		if composition.Supersedes != "" {
			if _, exists := seenRevisions[composition.Supersedes]; !exists {
				return fmt.Errorf("%w: composition %q supersedes missing revision %q", ErrInvalidComposition, composition.CompositionRevision, composition.Supersedes)
			}
		}
	}
	if err := validateSupersedesCycles(state.Compositions); err != nil {
		return err
	}
	return nil
}

func validateDurableRevision(value, label string) error {
	if err := validateID(value, label+" revision"); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidComposition, err)
	}
	return nil
}

func validateSupersedesCycles(compositions []DurableComposition) error {
	graph := make(map[string]string, len(compositions))
	for _, composition := range compositions {
		if composition.Supersedes != "" {
			graph[composition.CompositionRevision] = composition.Supersedes
		}
	}
	for start := range graph {
		seen := make(map[string]struct{})
		for current := start; current != ""; current = graph[current] {
			if _, exists := seen[current]; exists {
				return fmt.Errorf("%w: supersedes cycle includes %q", ErrInvalidComposition, current)
			}
			seen[current] = struct{}{}
		}
	}
	return nil
}

// Clone returns a deep copy suitable for crossing the store boundary.
func (state DurableHostState) Clone() DurableHostState {
	clone := state
	clone.Desired = make([]ModuleManifest, len(state.Desired))
	for index, manifest := range state.Desired {
		clone.Desired[index] = manifest.Clone()
	}
	clone.Compositions = make([]DurableComposition, len(state.Compositions))
	for index, composition := range state.Compositions {
		clone.Compositions[index] = composition.Clone()
	}
	return clone
}

func (composition DurableComposition) Clone() DurableComposition {
	clone := composition
	clone.Manifests = make([]ModuleManifest, len(composition.Manifests))
	for index, manifest := range composition.Manifests {
		clone.Manifests[index] = manifest.Clone()
	}
	return clone
}

// MarshalJSON emits the validated canonical ordering and refuses invalid
// state rather than serializing a document that cannot be reloaded safely.
func (state DurableHostState) MarshalJSON() ([]byte, error) {
	canonical, err := canonicalDurableHostState(state)
	if err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		StoreRevision  string               `json:"store_revision"`
		HostAPIVersion Version              `json:"host_api_version"`
		Desired        []ModuleManifest     `json:"desired"`
		Compositions   []DurableComposition `json:"compositions"`
	}{canonical.StoreRevision, canonical.HostAPIVersion, canonical.Desired, canonical.Compositions})
}

func canonicalDurableHostState(state DurableHostState) (DurableHostState, error) {
	if err := ValidateDurableHostState(state); err != nil {
		return DurableHostState{}, err
	}
	canonical := state.Clone()
	sort.Slice(canonical.Desired, func(i, j int) bool { return canonical.Desired[i].ID < canonical.Desired[j].ID })
	for index := range canonical.Desired {
		canonicalizeManifest(&canonical.Desired[index])
	}
	sort.Slice(canonical.Compositions, func(i, j int) bool {
		return canonical.Compositions[i].CompositionRevision < canonical.Compositions[j].CompositionRevision
	})
	for index := range canonical.Compositions {
		snapshot, _ := BuildSnapshotForAPI(canonical.Compositions[index].Manifests, canonical.Compositions[index].APIVersion)
		canonical.Compositions[index].Manifests = snapshot.Manifests()
	}
	return canonical, nil
}
