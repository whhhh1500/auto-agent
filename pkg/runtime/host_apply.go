package runtime

import (
	"context"
	"errors"
	"fmt"
)

// ApplyModules atomically installs the desired module set. Modules whose
// required dependencies are absent remain desired but are excluded from the
// published routing snapshot until a later ApplyModules supplies them.
func (host *ModuleHost) ApplyModules(ctx context.Context, modules []Module) error {
	ctx = nonNilContext(ctx)
	host.opMu.Lock()
	defer host.opMu.Unlock()
	return host.applyModulesLocked(ctx, modules)
}

// Activate reapplies the currently desired module set.
func (host *ModuleHost) Activate(ctx context.Context) error {
	ctx = nonNilContext(ctx)
	host.opMu.Lock()
	defer host.opMu.Unlock()
	host.mu.RLock()
	modules := make([]Module, 0, len(host.modules))
	for _, module := range host.modules {
		modules = append(modules, module)
	}
	host.mu.RUnlock()
	return host.applyModulesLocked(ctx, modules)
}

// RemoveModule is a thin desired-set update. Dependency closure and atomic
// publication are owned by ApplyModules.
func (host *ModuleHost) RemoveModule(ctx context.Context, id ModuleID) error {
	ctx = nonNilContext(ctx)
	host.opMu.Lock()
	defer host.opMu.Unlock()
	if err := contextError(ctx); err != nil {
		return err
	}
	host.mu.RLock()
	if _, found := host.modules[id]; !found {
		host.mu.RUnlock()
		return ErrModuleNotFound
	}
	modules := make([]Module, 0, len(host.modules)-1)
	for moduleID, module := range host.modules {
		if moduleID != id {
			modules = append(modules, module)
		}
	}
	host.mu.RUnlock()
	return host.applyModulesLocked(ctx, modules)
}

func (host *ModuleHost) applyModulesLocked(ctx context.Context, modules []Module) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := host.ensureOwnership(ctx); err != nil {
		return err
	}
	desired, manifests, err := prepareDesiredModules(modules, host.apiVersion)
	if err != nil {
		return err
	}
	routable := routableManifests(manifests)
	snapshot, err := BuildSnapshotForAPI(routable, host.apiVersion)
	if err != nil {
		return err
	}
	host.mu.RLock()
	old := host.active
	oldModules := host.modules
	oldManifests := host.manifests
	host.mu.RUnlock()
	if old != nil && desiredManifestSetEqual(oldManifests, manifests) {
		for id, nextModule := range desired {
			if !sameModuleInstance(oldModules[id], nextModule) {
				return fmt.Errorf("%w: module %q replacement requires a manifest or version change", ErrInvalidHost, id)
			}
		}
		return nil
	}
	if old != nil && old.snapshot.Revision() == snapshot.Revision() {
		states := desiredStates(manifests, snapshot)
		for id := range oldModules {
			if _, exists := manifests[id]; !exists {
				states[id] = StateInactive
			}
		}
		if err := host.durableDesired(ctx, manifests); err != nil {
			return err
		}
		host.mu.Lock()
		host.modules = desired
		host.manifests = manifests
		host.states = states
		host.mu.Unlock()
		return nil
	}
	if old != nil {
		for id, nextModule := range desired {
			oldModule := oldModules[id]
			oldManifest, exists := oldManifests[id]
			if exists && manifestFingerprint(oldManifest) == manifestFingerprint(manifests[id]) && !sameModuleInstance(oldModule, nextModule) {
				return fmt.Errorf("%w: module %q replacement requires a manifest or version change", ErrInvalidHost, id)
			}
		}
	}
	order, err := moduleOrder(snapshot.Manifests())
	if err != nil {
		return err
	}
	compositionRevision, err := host.nextCompositionRevision(snapshot.Revision())
	if err != nil {
		return err
	}
	if err := host.durablePrepare(ctx, snapshot, compositionRevision, compositionRevisionOf(old)); err != nil {
		return err
	}
	reuseOwners, retainedIDs, inherited, err := host.prepareReuse(ctx, desired, manifests, snapshot, old, oldModules, oldManifests)
	if err != nil {
		return errors.Join(err, host.durableRemoveCleanup(compositionRevision))
	}
	owners, newOwners, primaryErr, cleanupErr := host.stageCandidateWithRevision(ctx, desired, snapshot, order, compositionRevision, reuseOwners)
	if primaryErr != nil {
		if cleanupErr != nil {
			return errors.Join(primaryErr, cleanupErr)
		}
		return errors.Join(primaryErr, host.durableRemoveCleanup(compositionRevision))
	}
	if err := host.durablePromote(ctx, compositionRevision, compositionRevisionOf(old), manifests); err != nil {
		rollbackErr := host.rollback(ctx, compositionRevision, order, newOwners)
		if rollbackErr != nil {
			return errors.Join(err, rollbackErr)
		}
		return errors.Join(err, host.durableRemoveCleanup(compositionRevision))
	}
	return host.publishCandidate(desired, manifests, snapshot, order, owners, compositionRevision, old, oldManifests, inherited, retainedIDs)
}

func (host *ModuleHost) stageCandidateWithRevision(ctx context.Context, desired map[ModuleID]Module, snapshot Snapshot, order []ModuleID, compositionRevision string, reused map[ModuleID]ModuleLeaseOwner) (map[ModuleID]ModuleLeaseOwner, map[ModuleID]ModuleLeaseOwner, error, error) {
	owners := make(map[ModuleID]ModuleLeaseOwner, len(order))
	for id, owner := range reused {
		owners[id] = owner
	}
	newOwners := make(map[ModuleID]ModuleLeaseOwner, len(order))
	for _, id := range order {
		if owners[id] != nil {
			continue
		}
		if err := contextError(ctx); err != nil {
			return owners, newOwners, err, host.rollback(ctx, compositionRevision, order, newOwners)
		}
		manifest, _ := snapshot.Manifest(id)
		staged, err := safeModuleStage(desired[id], ctx, StageContext{ModuleID: id, ModuleRevision: manifest.Version, CompositionRevision: compositionRevision})
		if err != nil {
			return owners, newOwners, fmt.Errorf("%w: module %q: %w", ErrStageFailed, id, err), host.rollback(ctx, compositionRevision, order, newOwners)
		}
		if staged == nil {
			return owners, newOwners, fmt.Errorf("%w: module %q returned nil staged module", ErrStageFailed, id), host.rollback(ctx, compositionRevision, order, newOwners)
		}
		if err := safeStagedHealth(staged, ctx); err != nil {
			return owners, newOwners, fmt.Errorf("%w: module %q health: %w", ErrStageFailed, id, err), host.rollback(ctx, compositionRevision, order, newOwners)
		}
		tx := newActivationTransaction(host.journal, manifest, compositionRevision, EffectPhaseActivate)
		owner, err := safeStagedActivate(staged, ctx, tx)
		if owner != nil {
			owners[id] = owner
			newOwners[id] = owner
		}
		if err != nil || owner == nil {
			if err == nil {
				err = fmt.Errorf("%w: module %q returned nil owner", ErrActivationFailed, id)
			} else {
				err = fmt.Errorf("%w: module %q: %w", ErrActivationFailed, id, err)
			}
			return owners, newOwners, err, host.rollback(ctx, compositionRevision, order, newOwners)
		}
	}
	return owners, newOwners, nil, nil
}

func (host *ModuleHost) publishCandidate(desired map[ModuleID]Module, manifests map[ModuleID]ModuleManifest, snapshot Snapshot, order []ModuleID, owners map[ModuleID]ModuleLeaseOwner, compositionRevision string, old *moduleComposition, oldManifests map[ModuleID]ModuleManifest, inherited []RecordedEffect, retainedIDs map[ModuleID]struct{}) error {
	if err := host.forceRenewOwnership(context.Background()); err != nil {
		return err
	}
	candidate := &moduleComposition{snapshot: snapshot, compositionRevision: compositionRevision, owners: owners, leases: make(map[string]leaseRecord), state: StateActive, moduleOrder: order, inherited: inherited}
	states := desiredStates(manifests, snapshot)
	for id := range owners {
		states[id] = StateActive
	}
	if old != nil {
		for id := range oldManifests {
			if _, exists := manifests[id]; !exists {
				states[id] = StateInactive
			}
		}
	}
	host.mu.Lock()
	host.modules = desired
	host.manifests = manifests
	host.states = states
	host.active = candidate
	host.compositions[compositionRevision] = candidate
	if old != nil {
		old.state = StateDraining
		old.superseded = candidate
		old.retainedIDs = retainedIDs
	}
	host.mu.Unlock()
	return nil
}

func desiredStates(manifests map[ModuleID]ModuleManifest, snapshot Snapshot) map[ModuleID]LifecycleState {
	states := make(map[ModuleID]LifecycleState, len(manifests))
	routed := make(map[ModuleID]struct{}, len(snapshot.Manifests()))
	for _, manifest := range snapshot.Manifests() {
		routed[manifest.ID] = struct{}{}
	}
	for id := range manifests {
		if _, ok := routed[id]; ok {
			states[id] = StateActive
		} else {
			states[id] = StateWaitingDependencies
		}
	}
	return states
}

func desiredManifestSetEqual(left map[ModuleID]ModuleManifest, right map[ModuleID]ModuleManifest) bool {
	if len(left) != len(right) {
		return false
	}
	for id, manifest := range left {
		other, ok := right[id]
		if !ok || manifestFingerprint(manifest) != manifestFingerprint(other) {
			return false
		}
	}
	return true
}

func (host *ModuleHost) prepareReuse(ctx context.Context, desired map[ModuleID]Module, manifests map[ModuleID]ModuleManifest, snapshot Snapshot, old *moduleComposition, oldModules map[ModuleID]Module, oldManifests map[ModuleID]ModuleManifest) (map[ModuleID]ModuleLeaseOwner, map[ModuleID]struct{}, []RecordedEffect, error) {
	reused := make(map[ModuleID]ModuleLeaseOwner)
	retained := make(map[ModuleID]struct{})
	if old == nil {
		return reused, retained, nil, nil
	}
	for id, oldManifest := range oldManifests {
		if next, exists := manifests[id]; exists && manifestFingerprint(oldManifest) != manifestFingerprint(next) {
			// Any in-place manifest change invalidates reuse for the whole
			// composition; dependent owners must be rebuilt together.
			return reused, retained, nil, nil
		}
	}
	for id, manifest := range manifests {
		if _, routed := snapshot.Manifest(id); !routed {
			continue
		}
		owner := old.owners[id]
		if owner == nil || manifestFingerprint(oldManifests[id]) != manifestFingerprint(manifest) || !sameModuleInstance(oldModules[id], desired[id]) {
			continue
		}
		reused[id] = owner
		retained[id] = struct{}{}
	}
	records, err := safeEffectList(host.journal, ctx, old.compositionRevision)
	if err != nil {
		return nil, nil, nil, err
	}
	records, err = orderedEffects(records)
	if err != nil {
		return nil, nil, nil, err
	}
	inherited, err := host.refreshInheritedEffects(ctx, old.inherited)
	if err != nil {
		return nil, nil, nil, err
	}
	return reused, retained, mergeRetainedEffects(reused, inherited, records), nil
}

func sameModuleInstance(left, right Module) (same bool) {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	defer func() {
		if recover() != nil {
			same = false
		}
	}()
	return left == right
}

func manifestFingerprint(manifest ModuleManifest) string {
	clone := manifest.Clone()
	canonicalizeManifest(&clone)
	return snapshotRevision([]ModuleManifest{clone}, clone.Provides)
}
