package runtime

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"time"
)

// OpenModuleHost binds a host to a durable composition document. It performs
// no activation during opening. Any non-empty composition checkpoint is
// intentionally fail-closed: recovering an external owner is a separate
// operation and must not be approximated by replaying Activate.
func OpenModuleHost(ctx context.Context, apiVersion Version, modules []Module, journal EffectJournal, inverse InverseExecutor, store CompositionStore) (*ModuleHost, error) {
	return OpenModuleHostWithControls(ctx, apiVersion, modules, journal, inverse, store, HostControls{})
}

// OpenModuleHostWithControls binds immutable fence controls before durable
// bootstrap or recovery checks run.
func OpenModuleHostWithControls(ctx context.Context, apiVersion Version, modules []Module, journal EffectJournal, inverse InverseExecutor, store CompositionStore, controls HostControls) (*ModuleHost, error) {
	ctx = nonNilContext(ctx)
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if store == nil {
		return nil, fmt.Errorf("%w: composition store is required", ErrInvalidHost)
	}
	host, err := newUnpersistedHostWithControls(apiVersion, modules, journal, inverse, controls)
	if err != nil {
		return nil, err
	}
	owners, err := ownershipStore(store)
	if err != nil {
		return nil, err
	}
	holder, err := newHostHolder()
	if err != nil {
		return nil, err
	}
	host.ownershipStore, host.ownershipHolder = owners, holder
	loaded, found, err := safeCompositionLoad(store, ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: load: %v", ErrRecoveryBlocked, err)
	}
	if found {
		if err := validateOpenedState(loaded, apiVersion, host.manifests); err != nil {
			return nil, err
		}
		if durableStateNeedsRecovery(loaded) {
			return nil, fmt.Errorf("%w: durable composition checkpoint is present", ErrRecoveryRequired)
		}
	}
	if err := host.claimOwnership(ctx); err != nil {
		return nil, fmt.Errorf("%w: ownership claim: %v", ErrRecoveryBlocked, err)
	}
	claimed := true
	defer func() {
		if claimed {
			_ = host.releaseOwnershipLocked(context.Background())
		}
	}()
	// Re-read after claiming. A concurrent predecessor may have bootstrapped
	// state after our first load; Open never substitutes for recovery.
	loaded, found, err = safeCompositionLoad(store, ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: ownership reload: %v", ErrRecoveryBlocked, err)
	}
	if found {
		if err := validateOpenedState(loaded, apiVersion, host.manifests); err != nil {
			return nil, err
		}
		if durableStateNeedsRecovery(loaded) {
			return nil, fmt.Errorf("%w: durable composition checkpoint is present", ErrRecoveryRequired)
		}
	}
	if !found {
		initial, err := newInitialDurableState(apiVersion, host.manifests)
		if err != nil {
			return nil, err
		}
		if err := safeCompositionCAS(store, ctx, "", initial); err != nil {
			if !errors.Is(err, ErrCompositionConflict) {
				return nil, fmt.Errorf("%w: bootstrap: %v", ErrRecoveryBlocked, err)
			}
			// A concurrent creator wins the bootstrap race. Re-read and apply
			// the same binding/recovery checks to its document.
			loaded, found, err = safeCompositionLoad(store, ctx)
			if err != nil {
				return nil, fmt.Errorf("%w: bootstrap reload: %v", ErrRecoveryBlocked, err)
			}
			if !found {
				return nil, fmt.Errorf("%w: bootstrap winner disappeared", ErrRecoveryBlocked)
			}
		} else {
			host.compositionStore = store
			host.durableState = initial.Clone()
			claimed = false
			return host, nil
		}
	}
	if err := validateOpenedState(loaded, apiVersion, host.manifests); err != nil {
		return nil, err
	}
	host.compositionStore = store
	host.durableState = loaded.Clone()
	// Desired manifests are authoritative for state, while module instances
	// come from the constructor after exact ID/manifest binding above.
	routable := routableManifests(host.manifests)
	snapshot, err := BuildSnapshotForAPI(routable, apiVersion)
	if err != nil {
		return nil, fmt.Errorf("%w: desired snapshot: %v", ErrRecoveryBlocked, err)
	}
	host.states = desiredStates(host.manifests, snapshot)
	for id, state := range host.states {
		if state == StateActive {
			host.states[id] = StateConstructed
		}
	}
	claimed = false
	return host, nil
}

func newInitialDurableState(apiVersion Version, manifests map[ModuleID]ModuleManifest) (DurableHostState, error) {
	revision, err := newStoreRevision()
	if err != nil {
		return DurableHostState{}, err
	}
	return DurableHostState{StoreRevision: revision, HostAPIVersion: apiVersion, Desired: sortedManifests(manifests)}, nil
}

func newStoreRevision() (string, error) {
	var entropy [16]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return "", fmt.Errorf("%w: store revision entropy: %v", ErrInvalidHost, err)
	}
	return "store-" + hex.EncodeToString(entropy[:]), nil
}

func validateOpenedState(state DurableHostState, apiVersion Version, bound map[ModuleID]ModuleManifest) error {
	if err := ValidateDurableHostState(state); err != nil {
		return fmt.Errorf("%w: %v", ErrRecoveryBlocked, err)
	}
	if state.HostAPIVersion != apiVersion {
		return fmt.Errorf("%w: persisted host API %s differs from requested %s", ErrRecoveryBlocked, state.HostAPIVersion, apiVersion)
	}
	if len(state.Desired) != len(bound) {
		return fmt.Errorf("%w: persisted desired module set differs from constructor", ErrRecoveryBlocked)
	}
	for _, manifest := range state.Desired {
		boundManifest, found := bound[manifest.ID]
		if !found || manifestFingerprint(boundManifest) != manifestFingerprint(manifest) {
			return fmt.Errorf("%w: persisted desired module %q cannot bind to constructor", ErrRecoveryBlocked, manifest.ID)
		}
	}
	return nil
}

func durableStateNeedsRecovery(state DurableHostState) bool {
	// No persisted composition status is safe to reconstruct in this slice;
	// blocked/retained records also carry effect lineage that needs recovery.
	return len(state.Compositions) != 0
}

func sortedManifests(manifests map[ModuleID]ModuleManifest) []ModuleManifest {
	result := make([]ModuleManifest, 0, len(manifests))
	for _, manifest := range manifests {
		result = append(result, manifest.Clone())
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

func (host *ModuleHost) durablePrepare(ctx context.Context, snapshot Snapshot, compositionRevision, supersedes string) error {
	if host.compositionStore == nil {
		return nil
	}
	state := host.durableState.Clone()
	candidate := DurableComposition{CompositionRevision: compositionRevision, SnapshotRevision: snapshot.Revision(), APIVersion: host.apiVersion, Status: DurableCompositionPrepared, Supersedes: supersedes, Manifests: snapshot.Manifests()}
	state.Compositions = append(state.Compositions, candidate)
	return host.durableCAS(ctx, state)
}

func (host *ModuleHost) durablePromote(ctx context.Context, compositionRevision, supersedes string, desired map[ModuleID]ModuleManifest) error {
	if host.compositionStore == nil {
		return nil
	}
	state := host.durableState.Clone()
	found := false
	supersededFound := supersedes == ""
	for index := range state.Compositions {
		composition := &state.Compositions[index]
		if composition.CompositionRevision == compositionRevision {
			if composition.Status != DurableCompositionPrepared {
				return fmt.Errorf("%w: candidate %q is not prepared", ErrInvalidComposition, compositionRevision)
			}
			composition.Status = DurableCompositionActive
			found = true
		}
		if supersedes != "" && composition.CompositionRevision == supersedes {
			if composition.Status != DurableCompositionActive {
				return fmt.Errorf("%w: superseded composition %q is not active", ErrInvalidComposition, supersedes)
			}
			composition.Status = DurableCompositionDraining
			supersededFound = true
		}
	}
	if !found {
		return fmt.Errorf("%w: prepared composition %q is missing", ErrInvalidComposition, compositionRevision)
	}
	if !supersededFound {
		return fmt.Errorf("%w: superseded composition %q is missing", ErrInvalidComposition, supersedes)
	}
	state.Desired = sortedManifests(desired)
	return host.durableCAS(ctx, state)
}

// durableBlock records a fence/deactivation intent before any owner effect is
// attempted. A blocked composition is never recovered automatically.
func (host *ModuleHost) durableBlock(ctx context.Context, compositionRevision string) error {
	if host.compositionStore == nil {
		return nil
	}
	state := host.durableState.Clone()
	found := false
	for index := range state.Compositions {
		composition := &state.Compositions[index]
		if composition.CompositionRevision != compositionRevision {
			continue
		}
		found = true
		switch composition.Status {
		case DurableCompositionActive, DurableCompositionDraining, DurableCompositionRetained:
			composition.Status = DurableCompositionBlocked
		case DurableCompositionBlocked:
			return nil
		default:
			return fmt.Errorf("%w: composition %q cannot be blocked from %s", ErrInvalidComposition, compositionRevision, composition.Status)
		}
	}
	if !found {
		return fmt.Errorf("%w: composition %q is missing", ErrInvalidComposition, compositionRevision)
	}
	return host.durableCAS(ctx, state)
}

func (host *ModuleHost) durableRemove(ctx context.Context, compositionRevision string) error {
	if host.compositionStore == nil {
		return nil
	}
	state := host.durableState.Clone()
	filtered := make([]DurableComposition, 0, len(state.Compositions))
	found := false
	referenced := false
	for _, composition := range state.Compositions {
		if composition.Supersedes == compositionRevision {
			referenced = true
		}
	}
	for _, composition := range state.Compositions {
		if composition.CompositionRevision == compositionRevision {
			found = true
			if referenced {
				composition.Status = DurableCompositionRetained
				filtered = append(filtered, composition)
			}
			continue
		}
		filtered = append(filtered, composition)
	}
	if !found {
		return fmt.Errorf("%w: durable composition %q is missing", ErrInvalidComposition, compositionRevision)
	}
	state.Compositions = durableRetainedCleanup(filtered)
	return host.durableCAS(ctx, state)
}

func (host *ModuleHost) durableDesired(ctx context.Context, desired map[ModuleID]ModuleManifest) error {
	if host.compositionStore == nil {
		return nil
	}
	state := host.durableState.Clone()
	state.Desired = sortedManifests(desired)
	return host.durableCAS(ctx, state)
}

func (host *ModuleHost) durableCAS(ctx context.Context, next DurableHostState) error {
	if err := host.forceRenewOwnership(ctx); err != nil {
		return err
	}
	revision, err := newStoreRevision()
	if err != nil {
		return err
	}
	next.StoreRevision = revision
	if err := ValidateDurableHostState(next); err != nil {
		return err
	}
	if err := safeCompositionCAS(host.compositionStore, nonNilContext(ctx), host.durableState.StoreRevision, next); err != nil {
		return err
	}
	host.durableState = next.Clone()
	return nil
}

func (host *ModuleHost) durableRemoveCleanup(compositionRevision string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return host.durableRemove(ctx, compositionRevision)
}

func durableRetainedCleanup(compositions []DurableComposition) []DurableComposition {
	result := append([]DurableComposition(nil), compositions...)
	for {
		referenced := make(map[string]struct{}, len(result))
		for _, composition := range result {
			if composition.Supersedes != "" {
				referenced[composition.Supersedes] = struct{}{}
			}
		}
		changed := false
		filtered := result[:0]
		for _, composition := range result {
			if composition.Status == DurableCompositionRetained {
				if _, keep := referenced[composition.CompositionRevision]; !keep {
					changed = true
					continue
				}
			}
			filtered = append(filtered, composition)
		}
		result = filtered
		if !changed {
			return result
		}
	}
}
