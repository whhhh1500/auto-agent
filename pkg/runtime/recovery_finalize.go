package runtime

import (
	"context"
	"errors"
	"fmt"
	"sort"
)

func (host *ModuleHost) reconcileRecovered(ctx context.Context, revision string, snapshot Snapshot, order []ModuleID, owners map[ModuleID]ModuleLeaseOwner, inherited, current []RecordedEffect) error {
	before := make(map[EffectID]struct{}, len(current))
	for _, record := range current {
		before[record.Descriptor.ID] = struct{}{}
	}
	all := append(append([]RecordedEffect(nil), inherited...), current...)
	created := make(map[EffectID]struct{})
	for _, id := range order {
		owner := owners[id]
		manifest, found := snapshot.Manifest(id)
		if owner == nil || !found {
			return host.cleanupRecoveredOwners(ctx, owners, order, fmt.Errorf("%w: recovered owner %q missing", ErrRecoveryBlocked, id))
		}
		moduleRecords := filterEffectsByModule(all, id)
		tx := newActivationTransaction(host.journal, manifest, revision, EffectPhaseReconcile)
		desired := DesiredModuleState{ModuleID: id, CompositionRevision: revision, State: StateActive}
		err := safeOwnerReconcile(owner, ctx, desired, cloneRecordedEffects(moduleRecords), tx)
		for _, effectID := range tx.recordedSnapshot() {
			if _, existed := before[effectID]; !existed {
				created[effectID] = struct{}{}
			}
		}
		if err != nil {
			return host.cleanupRecoveredOwners(ctx, owners, order, errors.Join(fmt.Errorf("%w: reconcile %q: %v", ErrRecoveryBlocked, id, err), host.revertEffectSet(ctx, revision, snapshot.Manifests(), created)))
		}
	}
	return nil
}

func (host *ModuleHost) revertEffectSet(ctx context.Context, revision string, manifests []ModuleManifest, ids map[EffectID]struct{}) error {
	if len(ids) == 0 {
		return nil
	}
	records, err := host.loadCompositionEffects(ctx, revision, manifests, false)
	if err != nil {
		return err
	}
	var joined error
	for index := len(records) - 1; index >= 0; index-- {
		record := records[index]
		if _, selected := ids[record.Descriptor.ID]; !selected || record.State == EffectReverted {
			continue
		}
		if inverseErr := safeInverse(host.inverse, ctx, record); inverseErr != nil {
			joined = errors.Join(joined, inverseErr)
			continue
		}
		joined = errors.Join(joined, safeMarkReverted(host.journal, ctx, record.Descriptor.ID))
	}
	return joined
}

func (host *ModuleHost) cleanupRecoveredOwners(ctx context.Context, owners map[ModuleID]ModuleLeaseOwner, order []ModuleID, primary error) error {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), DefaultDrainTimeout)
	defer cancel()
	var joined error
	for index := len(order) - 1; index >= 0; index-- {
		if owner := owners[order[index]]; owner != nil {
			joined = errors.Join(joined, safeOwnerDeactivate(owner, cleanupCtx))
		}
	}
	return errors.Join(primary, joined)
}

func (host *ModuleHost) recoverOrphans(ctx context.Context) error {
	active, hasActive := durableActive(host.durableState)
	ancestors := make(map[string]struct{})
	if hasActive {
		chain, err := host.recoveryAncestors(active.CompositionRevision)
		if err != nil {
			return err
		}
		for _, composition := range chain {
			ancestors[composition.CompositionRevision] = struct{}{}
		}
	}
	for _, revision := range orphanCompositionOrder(host.durableState.Compositions) {
		_, isAncestor := ancestors[revision]
		if (hasActive && revision == active.CompositionRevision) || isAncestor {
			continue
		}
		composition := durableComposition(host.durableState, revision)
		if composition == nil {
			continue
		}
		if err := host.revertCompositionEffects(ctx, revision, composition.Manifests, false); err != nil {
			return fmt.Errorf("%w: orphan %q cleanup: %v", ErrRecoveryBlocked, revision, err)
		}
		if err := host.durableRemove(ctx, revision); err != nil {
			return fmt.Errorf("%w: orphan %q checkpoint: %v", ErrRecoveryBlocked, revision, err)
		}
	}
	return nil
}

// orphanCompositionOrder performs a deterministic child-before-parent walk.
// Revision strings are opaque and must never stand in for generation order.
func orphanCompositionOrder(compositions []DurableComposition) []string {
	children := make(map[string][]string, len(compositions))
	all := make([]string, 0, len(compositions))
	for _, composition := range compositions {
		all = append(all, composition.CompositionRevision)
		if composition.Supersedes != "" {
			children[composition.Supersedes] = append(children[composition.Supersedes], composition.CompositionRevision)
		}
	}
	for parent := range children {
		sort.Strings(children[parent])
	}
	sort.Strings(all)
	visited := make(map[string]struct{}, len(all))
	ordered := make([]string, 0, len(all))
	var visit func(string)
	visit = func(revision string) {
		if _, seen := visited[revision]; seen {
			return
		}
		visited[revision] = struct{}{}
		for _, child := range children[revision] {
			visit(child)
		}
		ordered = append(ordered, revision)
	}
	for _, revision := range all {
		visit(revision)
	}
	return ordered
}

func durableComposition(state DurableHostState, revision string) *DurableComposition {
	for index := range state.Compositions {
		if state.Compositions[index].CompositionRevision == revision {
			composition := state.Compositions[index].Clone()
			return &composition
		}
	}
	return nil
}
