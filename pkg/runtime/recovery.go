package runtime

import (
	"context"
	"errors"
	"fmt"
)

// RecoverModuleHost rebinds only resources that are already evidenced by the
// durable journal. It never calls Activate and never recreates public leases.
func RecoverModuleHost(ctx context.Context, apiVersion Version, modules []Module, journal EffectJournal, inverse InverseExecutor, store CompositionStore) (*ModuleHost, error) {
	return RecoverModuleHostWithControls(ctx, apiVersion, modules, journal, inverse, store, HostControls{})
}

// RecoverModuleHostWithControls fixes fence controls before reading recovery
// evidence, so a recovered host cannot acquire controls through a setter.
func RecoverModuleHostWithControls(ctx context.Context, apiVersion Version, modules []Module, journal EffectJournal, inverse InverseExecutor, store CompositionStore, controls HostControls) (*ModuleHost, error) {
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
	if err := host.claimOwnership(ctx); err != nil {
		return nil, fmt.Errorf("%w: ownership claim: %v", ErrRecoveryBlocked, err)
	}
	claimed := true
	defer func() {
		if claimed {
			_ = host.releaseOwnershipLocked(context.Background())
		}
	}()
	state, found, err := safeCompositionLoad(store, ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: load: %v", ErrRecoveryBlocked, err)
	}
	if !found {
		state, err = newInitialDurableState(apiVersion, host.manifests)
		if err != nil {
			return nil, err
		}
		if err := safeCompositionCAS(store, ctx, "", state); err != nil {
			if !errors.Is(err, ErrCompositionConflict) {
				return nil, fmt.Errorf("%w: bootstrap: %v", ErrRecoveryBlocked, err)
			}
			state, found, err = safeCompositionLoad(store, ctx)
			if err != nil || !found {
				if err == nil {
					err = errors.New("bootstrap winner disappeared")
				}
				return nil, fmt.Errorf("%w: bootstrap reload: %v", ErrRecoveryBlocked, err)
			}
		}
	}
	if err := validateOpenedState(state, apiVersion, host.manifests); err != nil {
		return nil, err
	}
	host.compositionStore = store
	host.durableState = state.Clone()
	if err := rejectBlockedCompositions(state); err != nil {
		return nil, err
	}
	if len(state.Compositions) == 0 {
		claimed = false
		return host, nil
	}
	if err := host.recoverPrepared(ctx); err != nil {
		return nil, err
	}
	// Clean unrelated durable generations before creating any new owner. A
	// failed orphan cleanup must never leave a newly recovered host published.
	if err := host.recoverOrphans(ctx); err != nil {
		return nil, err
	}
	if err := host.recoverActive(ctx); err != nil {
		return nil, err
	}
	claimed = false
	return host, nil
}

func rejectBlockedCompositions(state DurableHostState) error {
	for _, composition := range state.Compositions {
		if composition.Status == DurableCompositionBlocked {
			return fmt.Errorf("%w: composition %q is blocked", ErrRecoveryBlocked, composition.CompositionRevision)
		}
	}
	return nil
}

func (host *ModuleHost) recoverPrepared(ctx context.Context) error {
	for _, composition := range host.durableState.Compositions {
		if composition.Status != DurableCompositionPrepared {
			continue
		}
		if err := host.revertCompositionEffects(ctx, composition.CompositionRevision, composition.Manifests, true); err != nil {
			return fmt.Errorf("%w: prepared %q cleanup: %v", ErrRecoveryBlocked, composition.CompositionRevision, err)
		}
		if err := host.durableRemove(ctx, composition.CompositionRevision); err != nil {
			return fmt.Errorf("%w: prepared %q checkpoint: %v", ErrRecoveryBlocked, composition.CompositionRevision, err)
		}
	}
	return nil
}

func (host *ModuleHost) recoverActive(ctx context.Context) error {
	active, found := durableActive(host.durableState)
	if !found {
		return nil
	}
	snapshot, err := BuildSnapshotForAPI(active.Manifests, active.APIVersion)
	if err != nil {
		return fmt.Errorf("%w: active snapshot: %v", ErrRecoveryBlocked, err)
	}
	order, err := moduleOrder(snapshot.Manifests())
	if err != nil {
		return fmt.Errorf("%w: active order: %v", ErrRecoveryBlocked, err)
	}
	ancestors, err := host.recoveryAncestors(active.CompositionRevision)
	if err != nil {
		return err
	}
	inherited, err := host.recoverAncestorEffects(ctx, ancestors, snapshot)
	if err != nil {
		return err
	}
	currentEffects, err := host.loadCompositionEffects(ctx, active.CompositionRevision, snapshot.Manifests(), false)
	if err != nil {
		return fmt.Errorf("%w: active effects: %v", ErrRecoveryBlocked, err)
	}
	if err := host.retainRecoveryAncestors(ctx, ancestors); err != nil {
		return err
	}
	if err := host.forceRenewOwnership(ctx); err != nil {
		return err
	}
	owners, err := host.recoverOwners(ctx, active, snapshot, order, inherited, currentEffects)
	if err != nil {
		return err
	}
	if err := host.reconcileRecovered(ctx, active.CompositionRevision, snapshot, order, owners, inherited, currentEffects); err != nil {
		return err
	}
	if err := host.forceRenewOwnership(ctx); err != nil {
		return host.cleanupRecoveredOwners(ctx, owners, order, err)
	}
	latest, found, err := safeCompositionLoad(host.compositionStore, ctx)
	if err != nil || !found || latest.StoreRevision != host.durableState.StoreRevision || rejectBlockedCompositions(latest) != nil {
		if err == nil {
			err = ErrRecoveryBlocked
		}
		return host.cleanupRecoveredOwners(ctx, owners, order, fmt.Errorf("%w: recovery state changed before publish: %v", ErrRecoveryBlocked, err))
	}
	host.publishRecovered(active, snapshot, order, owners, inherited)
	return nil
}

func durableActive(state DurableHostState) (DurableComposition, bool) {
	for _, composition := range state.Compositions {
		if composition.Status == DurableCompositionActive {
			return composition, true
		}
	}
	return DurableComposition{}, false
}

func (host *ModuleHost) recoveryAncestors(activeRevision string) ([]DurableComposition, error) {
	byRevision := make(map[string]DurableComposition, len(host.durableState.Compositions))
	for _, composition := range host.durableState.Compositions {
		byRevision[composition.CompositionRevision] = composition
	}
	active := byRevision[activeRevision]
	newestFirst := make([]DurableComposition, 0)
	seen := make(map[string]struct{})
	for revision := active.Supersedes; revision != ""; {
		if _, duplicate := seen[revision]; duplicate {
			return nil, fmt.Errorf("%w: recovery supersedes cycle", ErrRecoveryBlocked)
		}
		seen[revision] = struct{}{}
		composition, found := byRevision[revision]
		if !found {
			return nil, fmt.Errorf("%w: recovery ancestor %q is missing", ErrRecoveryBlocked, revision)
		}
		newestFirst = append(newestFirst, composition)
		revision = composition.Supersedes
	}
	for left, right := 0, len(newestFirst)-1; left < right; left, right = left+1, right-1 {
		newestFirst[left], newestFirst[right] = newestFirst[right], newestFirst[left]
	}
	return newestFirst, nil
}

func (host *ModuleHost) retainRecoveryAncestors(ctx context.Context, ancestors []DurableComposition) error {
	if len(ancestors) == 0 {
		return nil
	}
	state := host.durableState.Clone()
	changed := false
	for _, ancestor := range ancestors {
		for index := range state.Compositions {
			if state.Compositions[index].CompositionRevision == ancestor.CompositionRevision && state.Compositions[index].Status != DurableCompositionRetained {
				state.Compositions[index].Status = DurableCompositionRetained
				changed = true
			}
		}
	}
	if !changed {
		return nil
	}
	if err := host.durableCAS(ctx, state); err != nil {
		return fmt.Errorf("%w: retain ancestors: %v", ErrRecoveryBlocked, err)
	}
	return nil
}

func (host *ModuleHost) publishRecovered(composition DurableComposition, snapshot Snapshot, order []ModuleID, owners map[ModuleID]ModuleLeaseOwner, inherited []RecordedEffect) {
	recovered := &moduleComposition{snapshot: snapshot, compositionRevision: composition.CompositionRevision, owners: owners, leases: make(map[string]leaseRecord), state: StateActive, moduleOrder: order, inherited: inherited}
	states := desiredStates(host.manifests, snapshot)
	for id := range owners {
		states[id] = StateActive
	}
	host.mu.Lock()
	host.states = states
	host.active = recovered
	host.compositions[composition.CompositionRevision] = recovered
	host.mu.Unlock()
}
