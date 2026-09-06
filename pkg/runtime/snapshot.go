package runtime

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
)

type Snapshot struct {
	revision   string
	manifests  []ModuleManifest
	extensions []ProvidedExtension
}

func (snapshot Snapshot) Revision() string { return snapshot.revision }

func (snapshot Snapshot) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Revision   string              `json:"revision"`
		Manifests  []ModuleManifest    `json:"manifests"`
		Extensions []ProvidedExtension `json:"extensions"`
	}{snapshot.revision, snapshot.Manifests(), snapshot.Extensions()})
}

func (snapshot *Snapshot) UnmarshalJSON([]byte) error {
	return fmt.Errorf("snapshot is immutable; construct it with BuildSnapshotForAPI")
}

func (snapshot Snapshot) Manifests() []ModuleManifest {
	result := make([]ModuleManifest, len(snapshot.manifests))
	for index, manifest := range snapshot.manifests {
		result[index] = manifest.Clone()
	}
	return result
}

func (snapshot Snapshot) Extensions() []ProvidedExtension {
	result := make([]ProvidedExtension, len(snapshot.extensions))
	for index, extension := range snapshot.extensions {
		result[index] = extension.Clone()
	}
	return result
}

func (snapshot Snapshot) Manifest(id ModuleID) (ModuleManifest, bool) {
	for _, manifest := range snapshot.manifests {
		if manifest.ID == id {
			return manifest.Clone(), true
		}
	}
	return ModuleManifest{}, false
}

// BuildSnapshot validates manifests, dependencies, and all semantic groups,
// then returns a frozen deep copy in deterministic order.
func BuildSnapshot(manifests []ModuleManifest) (Snapshot, error) {
	return BuildSnapshotForAPI(manifests, Version{})
}

// BuildSnapshotForAPI validates that every manifest explicitly supports the
// host API version before constructing the immutable candidate snapshot.
func BuildSnapshotForAPI(manifests []ModuleManifest, apiVersion Version) (Snapshot, error) {
	if !apiVersion.Valid() {
		return Snapshot{}, fmt.Errorf("%w: host API version is invalid", ErrInvalidManifest)
	}
	if len(manifests) > MaxModules {
		return Snapshot{}, fmt.Errorf("%w: module count exceeds %d", ErrInvalidManifest, MaxModules)
	}
	clones := make([]ModuleManifest, len(manifests))
	seenModules := make(map[ModuleID]struct{}, len(manifests))
	seenExtensions := make(map[ExtensionID]struct{})
	totalOrderingEdges := 0
	for index, manifest := range manifests {
		if err := ValidateManifest(manifest); err != nil {
			return Snapshot{}, err
		}
		if !manifest.CompatibleAPI.Contains(apiVersion) {
			return Snapshot{}, fmt.Errorf("%w: module %q is incompatible with host API %s", ErrInvalidManifest, manifest.ID, apiVersion)
		}
		if _, duplicate := seenModules[manifest.ID]; duplicate {
			return Snapshot{}, fmt.Errorf("%w: duplicate module %q", ErrInvalidManifest, manifest.ID)
		}
		seenModules[manifest.ID] = struct{}{}
		for _, extension := range manifest.Provides {
			totalOrderingEdges += len(extension.Before) + len(extension.After)
			if totalOrderingEdges > MaxSnapshotOrderingEdges {
				return Snapshot{}, fmt.Errorf("%w: ordering edge count exceeds %d", ErrInvalidManifest, MaxSnapshotOrderingEdges)
			}
			if _, duplicate := seenExtensions[extension.ID]; duplicate {
				return Snapshot{}, fmt.Errorf("%w: extension ID %q is not globally unique", ErrSemanticConflict, extension.ID)
			}
			seenExtensions[extension.ID] = struct{}{}
		}
		clones[index] = manifest.Clone()
		canonicalizeManifest(&clones[index])
	}
	byModule := make(map[ModuleID]ModuleManifest, len(clones))
	for _, manifest := range clones {
		byModule[manifest.ID] = manifest
	}
	if err := validateDependencies(clones, byModule); err != nil {
		return Snapshot{}, err
	}
	if err := validateModuleCycles(clones, byModule); err != nil {
		return Snapshot{}, err
	}
	extensions := make([]ProvidedExtension, 0)
	for _, manifest := range clones {
		extensions = append(extensions, manifest.Provides...)
	}
	if len(extensions) > MaxSnapshotExtensions {
		return Snapshot{}, fmt.Errorf("%w: extension count exceeds snapshot bound %d", ErrInvalidManifest, MaxSnapshotExtensions)
	}
	if err := validateAllSemantics(extensions); err != nil {
		return Snapshot{}, err
	}
	sort.Slice(clones, func(i, j int) bool { return clones[i].ID < clones[j].ID })
	extensions, err := orderExtensions(extensions)
	if err != nil {
		return Snapshot{}, err
	}
	revision := snapshotRevision(clones, extensions)
	return Snapshot{revision: revision, manifests: clones, extensions: extensions}, nil
}

func canonicalizeManifest(manifest *ModuleManifest) {
	sort.Slice(manifest.Requires, func(i, j int) bool { return dependencyLess(manifest.Requires[i], manifest.Requires[j]) })
	sort.Slice(manifest.Optional, func(i, j int) bool { return dependencyLess(manifest.Optional[i], manifest.Optional[j]) })
	sort.Slice(manifest.Effects, func(i, j int) bool { return manifest.Effects[i] < manifest.Effects[j] })
	for index := range manifest.Provides {
		sort.Slice(manifest.Provides[index].Before, func(i, j int) bool { return manifest.Provides[index].Before[i] < manifest.Provides[index].Before[j] })
		sort.Slice(manifest.Provides[index].After, func(i, j int) bool { return manifest.Provides[index].After[i] < manifest.Provides[index].After[j] })
	}
	sort.Slice(manifest.Provides, func(i, j int) bool {
		left, right := manifest.Provides[i], manifest.Provides[j]
		if left.Semantic != right.Semantic {
			return left.Semantic < right.Semantic
		}
		if left.Priority != right.Priority {
			return left.Priority < right.Priority
		}
		return left.ID < right.ID
	})
}

func dependencyLess(left, right Dependency) bool {
	if left.ModuleID != right.ModuleID {
		return left.ModuleID < right.ModuleID
	}
	if left.Contract != right.Contract {
		return left.Contract < right.Contract
	}
	if compared := left.Version.Min.Compare(right.Version.Min); compared != 0 {
		return compared < 0
	}
	if left.Version.Max == nil {
		return right.Version.Max != nil
	}
	if right.Version.Max == nil {
		return false
	}
	return left.Version.Max.Compare(*right.Version.Max) < 0
}

func orderExtensions(extensions []ProvidedExtension) ([]ProvidedExtension, error) {
	groups := make(map[ExtensionSemantic][]ProvidedExtension)
	for _, extension := range extensions {
		groups[extension.Semantic] = append(groups[extension.Semantic], extension)
	}
	ordered := make([]ProvidedExtension, 0, len(extensions))
	for _, semantic := range []ExtensionSemantic{SemanticCollection, SemanticPipeline, SemanticScopedOverlay, SemanticSingleStrategy, SemanticStatefulResource} {
		group := groups[semantic]
		if semantic == SemanticPipeline {
			byContract := make(map[ContractID][]ProvidedExtension)
			for _, extension := range group {
				byContract[extension.Contract] = append(byContract[extension.Contract], extension)
			}
			contracts := make([]ContractID, 0, len(byContract))
			for contract := range byContract {
				contracts = append(contracts, contract)
			}
			sort.Slice(contracts, func(i, j int) bool { return contracts[i] < contracts[j] })
			for _, contract := range contracts {
				pipeline, err := pipelineOrder(byContract[contract])
				if err != nil {
					return nil, err
				}
				ordered = append(ordered, pipeline...)
			}
			continue
		}
		sort.Slice(group, func(i, j int) bool {
			if group[i].Priority != group[j].Priority {
				return group[i].Priority < group[j].Priority
			}
			return group[i].ID < group[j].ID
		})
		ordered = append(ordered, group...)
	}
	return ordered, nil
}

func snapshotRevision(manifests []ModuleManifest, extensions []ProvidedExtension) string {
	payload, _ := json.Marshal(struct {
		Manifests  []ModuleManifest    `json:"manifests"`
		Extensions []ProvidedExtension `json:"extensions"`
	}{manifests, extensions})
	digest := sha256.Sum256(payload)
	return "rev_" + hex.EncodeToString(digest[:16])
}
