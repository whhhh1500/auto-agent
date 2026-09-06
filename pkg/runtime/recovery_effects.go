package runtime

import (
	"context"
	"errors"
	"fmt"
)

func (host *ModuleHost) loadCompositionEffects(ctx context.Context, revision string, manifests []ModuleManifest, activateOnly bool) ([]RecordedEffect, error) {
	records, err := safeEffectList(host.journal, ctx, revision)
	if err != nil {
		return nil, err
	}
	records, err = orderedEffects(records)
	if err != nil {
		return nil, err
	}
	bindings := make(map[ModuleID]ModuleManifest, len(manifests))
	allowedByModule := make(map[ModuleID]map[EffectKind]struct{}, len(manifests))
	bindingsProvided := manifests != nil
	for _, manifest := range manifests {
		bindings[manifest.ID] = manifest
		allowed := make(map[EffectKind]struct{}, len(manifest.Effects))
		for _, effect := range manifest.Effects {
			allowed[effect] = struct{}{}
		}
		allowedByModule[manifest.ID] = allowed
	}
	seenIDs := make(map[EffectID]struct{}, len(records))
	for _, record := range records {
		if record.Descriptor.CompositionRevision != revision {
			return nil, fmt.Errorf("%w: effect %q belongs to another composition", ErrInvalidEffect, record.Descriptor.ID)
		}
		if err := ValidateEffectDescriptor(record.Descriptor); err != nil {
			return nil, err
		}
		if record.Descriptor.Phase != EffectPhaseActivate && record.Descriptor.Phase != EffectPhaseReconcile {
			return nil, fmt.Errorf("%w: effect %q has unsupported recovery phase", ErrInvalidEffect, record.Descriptor.ID)
		}
		if !record.State.Valid() {
			return nil, fmt.Errorf("%w: effect %q has invalid state", ErrInvalidEffect, record.Descriptor.ID)
		}
		if _, duplicate := seenIDs[record.Descriptor.ID]; duplicate {
			return nil, fmt.Errorf("%w: duplicate effect %q", ErrInvalidEffect, record.Descriptor.ID)
		}
		seenIDs[record.Descriptor.ID] = struct{}{}
		if bindingsProvided {
			manifest, found := bindings[record.Descriptor.ModuleID]
			if !found || manifest.Version != record.Descriptor.ModuleRevision {
				return nil, fmt.Errorf("%w: effect %q module binding is invalid", ErrInvalidEffect, record.Descriptor.ID)
			}
			if activateOnly && record.Descriptor.Phase != EffectPhaseActivate {
				return nil, fmt.Errorf("%w: prepared effect %q is not an activation effect", ErrInvalidEffect, record.Descriptor.ID)
			}
			allowed := allowedByModule[record.Descriptor.ModuleID]
			if _, ok := allowed[EffectKind(record.Descriptor.Forward.Kind)]; !ok {
				return nil, fmt.Errorf("%w: effect %q forward kind is not declared", ErrInvalidEffect, record.Descriptor.ID)
			}
			if _, ok := allowed[EffectKind(record.Descriptor.Inverse.Kind)]; !ok {
				return nil, fmt.Errorf("%w: effect %q inverse kind is not declared", ErrInvalidEffect, record.Descriptor.ID)
			}
		}
	}
	return records, nil
}

func (host *ModuleHost) revertCompositionEffects(ctx context.Context, revision string, manifests []ModuleManifest, activateOnly bool) error {
	if manifests == nil {
		manifests = []ModuleManifest{}
	}
	records, err := host.loadCompositionEffects(ctx, revision, manifests, activateOnly)
	if err != nil {
		return err
	}
	var joined error
	for index := len(records) - 1; index >= 0; index-- {
		record := records[index]
		if record.State == EffectReverted {
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

func (host *ModuleHost) recoverAncestorEffects(ctx context.Context, ancestors []DurableComposition, active Snapshot) ([]RecordedEffect, error) {
	activeManifests := make(map[ModuleID]ModuleManifest, len(active.Manifests()))
	for _, manifest := range active.Manifests() {
		activeManifests[manifest.ID] = manifest
	}
	inherited := make([]RecordedEffect, 0)
	for _, ancestor := range ancestors {
		records, err := host.loadCompositionEffects(ctx, ancestor.CompositionRevision, ancestor.Manifests, false)
		if err != nil {
			return nil, fmt.Errorf("%w: ancestor %q effects: %v", ErrRecoveryBlocked, ancestor.CompositionRevision, err)
		}
		ancestorManifests := make(map[ModuleID]ModuleManifest, len(ancestor.Manifests))
		for _, manifest := range ancestor.Manifests {
			ancestorManifests[manifest.ID] = manifest
		}
		for _, record := range records {
			currentManifest, activeModule := activeManifests[record.Descriptor.ModuleID]
			ancestorManifest, sameModule := ancestorManifests[record.Descriptor.ModuleID]
			if activeModule && sameModule && manifestFingerprint(currentManifest) == manifestFingerprint(ancestorManifest) {
				inherited = append(inherited, record)
				continue
			}
			if record.State == EffectReverted {
				continue
			}
			if inverseErr := safeInverse(host.inverse, ctx, record); inverseErr != nil {
				return nil, fmt.Errorf("%w: ancestor %q effect %q inverse: %v", ErrRecoveryBlocked, ancestor.CompositionRevision, record.Descriptor.ID, inverseErr)
			}
			if markErr := safeMarkReverted(host.journal, ctx, record.Descriptor.ID); markErr != nil {
				return nil, fmt.Errorf("%w: ancestor %q effect %q mark: %v", ErrRecoveryBlocked, ancestor.CompositionRevision, record.Descriptor.ID, markErr)
			}
		}
	}
	return inherited, nil
}

func (host *ModuleHost) recoverOwners(ctx context.Context, composition DurableComposition, snapshot Snapshot, order []ModuleID, inherited, current []RecordedEffect) (map[ModuleID]ModuleLeaseOwner, error) {
	owners := make(map[ModuleID]ModuleLeaseOwner, len(order))
	cleanup := func(primary error) error {
		var joined error
		cleanupCtx, cancel := context.WithTimeout(context.Background(), DefaultDrainTimeout)
		defer cancel()
		for index := len(order) - 1; index >= 0; index-- {
			if owner := owners[order[index]]; owner != nil {
				joined = errors.Join(joined, safeOwnerDeactivate(owner, cleanupCtx))
			}
		}
		return errors.Join(primary, joined)
	}
	for _, id := range order {
		module := host.modules[id]
		manifest, found := snapshot.Manifest(id)
		if module == nil || !found {
			return nil, cleanup(fmt.Errorf("%w: recover module %q is not bound", ErrRecoveryBlocked, id))
		}
		staged, err := safeModuleStage(module, ctx, StageContext{ModuleID: id, ModuleRevision: manifest.Version, CompositionRevision: composition.CompositionRevision})
		if err != nil {
			return nil, cleanup(fmt.Errorf("%w: recover stage %q: %v", ErrRecoveryBlocked, id, err))
		}
		if staged == nil {
			return nil, cleanup(fmt.Errorf("%w: recover stage %q returned nil", ErrRecoveryBlocked, id))
		}
		if err := safeStagedHealth(staged, ctx); err != nil {
			return nil, cleanup(fmt.Errorf("%w: recover health %q: %v", ErrRecoveryBlocked, id, err))
		}
		recoverable, ok := staged.(RecoverableStagedModule)
		if !ok {
			return nil, cleanup(fmt.Errorf("%w: module %q is not recoverable", ErrRecoveryBlocked, id))
		}
		effects := filterEffectsByModule(inherited, id)
		effects = append(effects, filterEffectsByModule(current, id)...)
		desired := DesiredModuleState{ModuleID: id, CompositionRevision: composition.CompositionRevision, State: StateActive}
		owner, err := safeOwnerRecover(recoverable, ctx, RecoveryContext{StageContext: StageContext{ModuleID: id, ModuleRevision: manifest.Version, CompositionRevision: composition.CompositionRevision}, DesiredModuleState: desired, Effects: cloneRecordedEffects(effects), PriorLeasesLost: true})
		if err != nil || owner == nil {
			if err == nil {
				err = errors.New("recover returned nil owner")
			}
			return nil, cleanup(fmt.Errorf("%w: module %q: %v", ErrRecoveryBlocked, id, err))
		}
		owners[id] = owner
	}
	return owners, nil
}

func filterEffectsByModule(records []RecordedEffect, id ModuleID) []RecordedEffect {
	filtered := make([]RecordedEffect, 0)
	for _, record := range records {
		if record.Descriptor.ModuleID == id {
			filtered = append(filtered, cloneRecordedEffect(record))
		}
	}
	return filtered
}

func cloneRecordedEffects(records []RecordedEffect) []RecordedEffect {
	clones := make([]RecordedEffect, len(records))
	for index, record := range records {
		clones[index] = cloneRecordedEffect(record)
	}
	return clones
}
