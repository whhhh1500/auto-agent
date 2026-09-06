package runtime

import (
	"fmt"
	"sort"
)

// ValidateManifest validates one manifest and all semantic extension records.
func ValidateManifest(manifest ModuleManifest) error {
	if len(manifest.Requires)+len(manifest.Optional) > MaxManifestDependencies {
		return fmt.Errorf("%w: dependency count exceeds %d", ErrInvalidManifest, MaxManifestDependencies)
	}
	if len(manifest.Provides) > MaxManifestProvides {
		return fmt.Errorf("%w: provided extension count exceeds %d", ErrInvalidManifest, MaxManifestProvides)
	}
	if len(manifest.Effects) > MaxManifestEffects {
		return fmt.Errorf("%w: effect count exceeds %d", ErrInvalidManifest, MaxManifestEffects)
	}
	if err := validateID(string(manifest.ID), "module"); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidManifest, err)
	}
	if !manifest.Version.Valid() || !manifest.CompatibleAPI.Valid() {
		return fmt.Errorf("%w: version or compatible API range is invalid", ErrInvalidManifest)
	}
	if manifest.DrainTimeout < 0 || manifest.DrainTimeout > MaxDrainTimeout {
		return fmt.Errorf("%w: drain timeout is outside bounds", ErrInvalidManifest)
	}
	seenDependency := make(map[string]struct{}, len(manifest.Requires)+len(manifest.Optional))
	for _, dependency := range append(append([]Dependency(nil), manifest.Requires...), manifest.Optional...) {
		if err := validateDependency(dependency); err != nil {
			return err
		}
		key := string(dependency.moduleID()) + "\x00" + string(dependency.Contract)
		if _, duplicate := seenDependency[key]; duplicate {
			return fmt.Errorf("%w: duplicate dependency %s", ErrInvalidManifest, key)
		}
		seenDependency[key] = struct{}{}
	}
	seenEffects := make(map[EffectKind]struct{}, len(manifest.Effects))
	for _, effect := range manifest.Effects {
		if err := validateEffect(effect); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidManifest, err)
		}
		if _, duplicate := seenEffects[effect]; duplicate {
			return fmt.Errorf("%w: duplicate effect %q", ErrInvalidManifest, effect)
		}
		seenEffects[effect] = struct{}{}
	}
	seenIDs := make(map[ExtensionID]struct{}, len(manifest.Provides))
	for _, extension := range manifest.Provides {
		if len(extension.Before)+len(extension.After) > MaxExtensionOrderingTargets {
			return fmt.Errorf("%w: ordering targets for %q exceed %d", ErrInvalidManifest, extension.ID, MaxExtensionOrderingTargets)
		}
		if _, duplicate := seenIDs[extension.ID]; duplicate {
			return fmt.Errorf("%w: duplicate extension %q", ErrInvalidManifest, extension.ID)
		}
		seenIDs[extension.ID] = struct{}{}
		if err := ValidateExtension(extension); err != nil {
			return err
		}
	}
	// Cross-module pipeline targets and semantic conflicts are validated after
	// manifests are aggregated by BuildSnapshot. Validating them here would
	// incorrectly reject a legal before/after edge to another module.
	return nil
}

func validateDependency(dependency Dependency) error {
	if err := validateID(string(dependency.moduleID()), "dependency module"); err != nil {
		return fmt.Errorf("%w: %v", ErrDependency, err)
	}
	if err := validateID(string(dependency.Contract), "dependency contract"); err != nil {
		return fmt.Errorf("%w: %v", ErrDependency, err)
	}
	if !dependency.Version.Valid() {
		return fmt.Errorf("%w: version range is invalid", ErrDependency)
	}
	return nil
}

func validateAllSemantics(extensions []ProvidedExtension) error {
	if len(extensions) > MaxSnapshotExtensions {
		return fmt.Errorf("%w: extension count exceeds snapshot bound %d", ErrInvalidManifest, MaxSnapshotExtensions)
	}
	groups := make(map[ExtensionSemantic][]ProvidedExtension)
	for _, extension := range extensions {
		groups[extension.Semantic] = append(groups[extension.Semantic], extension)
	}
	for _, semantic := range []ExtensionSemantic{SemanticCollection, SemanticPipeline, SemanticScopedOverlay, SemanticSingleStrategy, SemanticStatefulResource} {
		if err := ValidateExtensions(semantic, groups[semantic]); err != nil {
			return err
		}
	}
	return nil
}

func validateDependencies(manifests []ModuleManifest, byModule map[ModuleID]ModuleManifest) error {
	for _, manifest := range manifests {
		for _, dependency := range manifest.Requires {
			if err := validateDependencyTarget(manifest, dependency, byModule, false); err != nil {
				return err
			}
		}
		for _, dependency := range manifest.Optional {
			if err := validateDependencyTarget(manifest, dependency, byModule, true); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateDependencyTarget(manifest ModuleManifest, dependency Dependency, byModule map[ModuleID]ModuleManifest, optional bool) error {
	module, found := byModule[dependency.moduleID()]
	if !found {
		if optional {
			return nil
		}
		return fmt.Errorf("%w: required module %q is missing", ErrDependency, dependency.moduleID())
	}
	for _, extension := range module.Provides {
		if extension.Contract == dependency.Contract && dependency.Version.Contains(extension.Version) {
			return nil
		}
	}
	if optional {
		return fmt.Errorf("%w: optional module %q is incompatible", ErrDependency, dependency.moduleID())
	}
	return fmt.Errorf("%w: module %q has no compatible contract %q", ErrDependency, dependency.moduleID(), dependency.Contract)
}

func validateModuleCycles(manifests []ModuleManifest, byModule map[ModuleID]ModuleManifest) error {
	graph := make(map[ModuleID][]ModuleID, len(manifests))
	for _, manifest := range manifests {
		for _, dependency := range append(append([]Dependency(nil), manifest.Requires...), manifest.Optional...) {
			if _, present := byModule[dependency.moduleID()]; present {
				graph[manifest.ID] = append(graph[manifest.ID], dependency.moduleID())
			}
		}
	}
	return detectModuleCycle(graph)
}

func detectModuleCycle(graph map[ModuleID][]ModuleID) error {
	indegree := make(map[ModuleID]int, len(graph))
	for node := range graph {
		indegree[node] = 0
	}
	for _, neighbors := range graph {
		for _, neighbor := range neighbors {
			if _, exists := indegree[neighbor]; !exists {
				indegree[neighbor] = 0
			}
			indegree[neighbor]++
		}
	}
	ready := make([]ModuleID, 0, len(indegree))
	for node, degree := range indegree {
		if degree == 0 {
			ready = append(ready, node)
		}
	}
	sort.Slice(ready, func(i, j int) bool { return ready[i] < ready[j] })
	visited := 0
	for len(ready) != 0 {
		node := ready[0]
		ready = ready[1:]
		visited++
		for _, neighbor := range graph[node] {
			indegree[neighbor]--
			if indegree[neighbor] == 0 {
				ready = append(ready, neighbor)
			}
		}
		sort.Slice(ready, func(i, j int) bool { return ready[i] < ready[j] })
	}
	if visited != len(indegree) {
		return ErrDependencyCycle
	}
	return nil
}
