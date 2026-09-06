package runtime

import (
	"context"
)

func (host *ModuleHost) Reconcile(ctx context.Context, compositionRevision string) error {
	ctx = nonNilContext(ctx)
	host.opMu.Lock()
	defer host.opMu.Unlock()
	if err := host.ensureOwnership(ctx); err != nil {
		return err
	}
	composition, err := host.composition(compositionRevision)
	if err != nil {
		return err
	}
	records, err := safeEffectList(host.journal, ctx, compositionRevision)
	if err != nil {
		return err
	}
	records, err = orderedEffects(records)
	if err != nil {
		return err
	}
	// Inherited effects precede this composition's effects. Refresh their
	// durable state before constructing reconciliation evidence.
	inherited, err := host.refreshInheritedEffects(ctx, composition.inherited)
	if err != nil {
		return err
	}
	composition.inherited = inherited
	records = append(append([]RecordedEffect(nil), inherited...), records...)
	host.mu.RLock()
	compositionState := composition.state
	host.mu.RUnlock()
	for _, id := range composition.moduleOrder {
		owner := composition.owners[id]
		manifest, found := composition.snapshot.Manifest(id)
		if !found {
			return ErrModuleNotFound
		}
		if owner == nil {
			continue
		}
		moduleRecords := make([]RecordedEffect, 0)
		for _, record := range records {
			if record.Descriptor.ModuleID == id {
				moduleRecords = append(moduleRecords, record)
			}
		}
		tx := newActivationTransaction(host.journal, manifest, compositionRevision, EffectPhaseReconcile)
		desired := DesiredModuleState{ModuleID: id, CompositionRevision: compositionRevision, State: compositionState}
		if reconcileErr := safeOwnerReconcile(owner, ctx, desired, moduleRecords, tx); reconcileErr != nil {
			return reconcileErr
		}
	}
	return nil
}

// mergeRetainedEffects carries effects across a module-set replacement. An
// earlier candidate may itself have inherited effects, so both generations
// are considered. Journal order is preserved and an effect is retained once.
func mergeRetainedEffects(owners map[ModuleID]ModuleLeaseOwner, prior, current []RecordedEffect) []RecordedEffect {
	result := make([]RecordedEffect, 0, len(prior)+len(current))
	seen := make(map[EffectID]struct{}, len(prior)+len(current))
	for _, records := range [][]RecordedEffect{prior, current} {
		for _, record := range records {
			if _, retained := owners[record.Descriptor.ModuleID]; !retained {
				continue
			}
			if _, duplicate := seen[record.Descriptor.ID]; duplicate {
				continue
			}
			seen[record.Descriptor.ID] = struct{}{}
			result = append(result, record)
		}
	}
	return result
}
