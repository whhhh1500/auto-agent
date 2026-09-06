package runtime

import (
	"fmt"
	"sort"
)

func prepareDesiredModules(modules []Module, apiVersion Version) (map[ModuleID]Module, map[ModuleID]ModuleManifest, error) {
	desired := make(map[ModuleID]Module, len(modules))
	manifests := make(map[ModuleID]ModuleManifest, len(modules))
	if len(modules) > MaxModules {
		return nil, nil, fmt.Errorf("%w: module count exceeds %d", ErrInvalidManifest, MaxModules)
	}
	for index, module := range modules {
		if module == nil {
			return nil, nil, fmt.Errorf("%w: nil module", ErrInvalidHost)
		}
		manifest, err := safeModuleManifest(module)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: module at index %d: %w", ErrInvalidHost, index, err)
		}
		if _, duplicate := desired[manifest.ID]; duplicate {
			return nil, nil, fmt.Errorf("%w: duplicate module %q", ErrInvalidHost, manifest.ID)
		}
		if err := ValidateManifest(manifest); err != nil {
			return nil, nil, err
		}
		if !manifest.CompatibleAPI.Contains(apiVersion) {
			return nil, nil, fmt.Errorf("%w: module %q is incompatible with host API %s", ErrInvalidManifest, manifest.ID, apiVersion)
		}
		desired[manifest.ID] = module
		manifests[manifest.ID] = manifest.Clone()
	}
	return desired, manifests, nil
}

func routableManifests(manifests map[ModuleID]ModuleManifest) []ModuleManifest {
	active := make(map[ModuleID]ModuleManifest, len(manifests))
	for id, manifest := range manifests {
		active[id] = manifest.Clone()
	}
	changed := true
	for changed {
		changed = false
		for id, manifest := range active {
			for _, dependency := range manifest.Requires {
				provider, found := active[dependency.ModuleID]
				if !found || !hasCompatibleContract(provider, dependency) {
					delete(active, id)
					changed = true
					break
				}
			}
		}
	}
	result := make([]ModuleManifest, 0, len(active))
	for _, manifest := range active {
		result = append(result, manifest)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

func hasCompatibleContract(provider ModuleManifest, dependency Dependency) bool {
	for _, extension := range provider.Provides {
		if extension.Contract == dependency.Contract && dependency.Version.Contains(extension.Version) {
			return true
		}
	}
	return false
}
