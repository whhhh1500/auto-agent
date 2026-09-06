package runtime

import "fmt"

func newUnpersistedHostWithControls(apiVersion Version, modules []Module, journal EffectJournal, inverse InverseExecutor, controls HostControls) (*ModuleHost, error) {
	if !apiVersion.Valid() {
		return nil, fmt.Errorf("%w: invalid host API version", ErrInvalidHost)
	}
	if err := validateHostControls(controls); err != nil {
		return nil, err
	}
	if journal == nil || inverse == nil {
		return nil, fmt.Errorf("%w: journal and inverse executor are required", ErrInvalidHost)
	}
	desired, manifests, err := prepareDesiredModules(modules, apiVersion)
	if err != nil {
		return nil, err
	}
	routable := routableManifests(manifests)
	snapshot, err := BuildSnapshotForAPI(routable, apiVersion)
	if err != nil {
		return nil, err
	}
	host := &ModuleHost{
		modules: desired, manifests: manifests, journal: journal, inverse: inverse,
		apiVersion: apiVersion, compositions: make(map[string]*moduleComposition),
		fenceAuthorizer: controls.FenceAuthorizer, fenceJournal: controls.FenceJournal,
		states: desiredStates(manifests, snapshot),
	}
	for id, state := range host.states {
		if state == StateActive {
			host.states[id] = StateConstructed
		}
	}
	return host, nil
}
