package runtime

import (
	"context"
	"errors"
	"fmt"
	"unicode/utf8"
)

func (host *ModuleHost) Drain(ctx context.Context, compositionRevision string) (DrainResult, error) {
	ctx = nonNilContext(ctx)
	host.opMu.Lock()
	defer host.opMu.Unlock()
	if err := host.forceRenewOwnership(ctx); err != nil {
		return DrainResult{CompositionRevision: compositionRevision}, err
	}
	composition, err := host.composition(compositionRevision)
	if err != nil {
		return DrainResult{}, err
	}
	if err := host.durableBlock(ctx, compositionRevision); err != nil {
		return DrainResult{CompositionRevision: compositionRevision}, err
	}
	host.mu.Lock()
	composition.state = StateDraining
	host.mu.Unlock()
	var joined error
	for index := len(composition.moduleOrder) - 1; index >= 0; index-- {
		id := composition.moduleOrder[index]
		if owner := composition.owners[id]; owner != nil {
			manifest, found := host.manifestFor(composition, id)
			if !found {
				joined = errors.Join(joined, ErrModuleNotFound)
				continue
			}
			drainCtx, cancel := context.WithTimeout(nonNilContext(ctx), manifest.EffectiveDrainTimeout())
			drainResult, drainErr := safeOwnerDrain(owner, drainCtx, DrainRequest{CompositionRevision: compositionRevision})
			cancel()
			joined = errors.Join(joined, drainErr)
			if !drainResult.Completed || drainResult.RemainingLeases > 0 {
				joined = errors.Join(joined, ErrLeasesRemaining)
			}
		}
	}
	host.mu.RLock()
	remaining := len(composition.leases)
	host.mu.RUnlock()
	result := DrainResult{CompositionRevision: compositionRevision, RemainingLeases: remaining}
	if remaining != 0 {
		joined = errors.Join(joined, ErrLeasesRemaining)
	}
	if joined != nil {
		result.Completed = false
		if remaining != 0 || errors.Is(joined, ErrLeasesRemaining) {
			return result, errors.Join(joined, ErrLeasesRemaining)
		}
		return result, joined
	}
	if deactivateErr := host.deactivateComposition(nonNilContext(ctx), composition); deactivateErr != nil {
		return result, errors.Join(joined, deactivateErr)
	}
	result.Completed = true
	return result, joined
}

func (host *ModuleHost) Fence(ctx context.Context, compositionRevision, reason string) (DrainResult, error) {
	return DrainResult{}, ErrFenceUnauthorized
}

func (host *ModuleHost) Deactivate(ctx context.Context, compositionRevision string) error {
	ctx = nonNilContext(ctx)
	host.opMu.Lock()
	defer host.opMu.Unlock()
	if err := host.forceRenewOwnership(ctx); err != nil {
		return err
	}
	composition, err := host.composition(compositionRevision)
	if err != nil {
		return err
	}
	host.mu.RLock()
	remaining := len(composition.leases)
	host.mu.RUnlock()
	if remaining != 0 {
		return ErrLeasesRemaining
	}
	return host.deactivateComposition(nonNilContext(ctx), composition)
}

func (host *ModuleHost) composition(revision string) (*moduleComposition, error) {
	host.mu.RLock()
	defer host.mu.RUnlock()
	composition := host.compositions[revision]
	if composition == nil {
		return nil, fmt.Errorf("%w: composition %q", ErrLeaseNotFound, revision)
	}
	return composition, nil
}

func (host *ModuleHost) manifestFor(composition *moduleComposition, id ModuleID) (ModuleManifest, bool) {
	return composition.snapshot.Manifest(id)
}

func (host *ModuleHost) deactivateComposition(ctx context.Context, composition *moduleComposition) error {
	if err := host.durableBlock(ctx, composition.compositionRevision); err != nil {
		return err
	}
	if err := host.deactivateCompositionWork(ctx, composition); err != nil {
		return err
	}
	return host.finalizeDeactivation(ctx, composition)
}

func (host *ModuleHost) deactivateCompositionWork(ctx context.Context, composition *moduleComposition) error {
	composition.leaseMu.Lock()
	defer composition.leaseMu.Unlock()
	host.mu.Lock()
	composition.state = StateDeactivating
	host.mu.Unlock()
	records, err := host.loadCompositionEffects(ctx, composition.compositionRevision, composition.snapshot.Manifests(), false)
	if err != nil {
		host.mu.Lock()
		composition.state = StateDraining
		host.mu.Unlock()
		return err
	}
	// Inherited effects are older than effects recorded by this composition.
	// Refresh their state so a retry after a durable CAS failure cannot replay an
	// inverse that already completed.
	inherited, err := host.refreshInheritedEffects(ctx, composition.inherited)
	if err != nil {
		host.mu.Lock()
		composition.state = StateDraining
		host.mu.Unlock()
		return err
	}
	composition.inherited = inherited
	records = append(append([]RecordedEffect(nil), inherited...), records...)
	var joined error
	for index := len(records) - 1; index >= 0; index-- {
		record := records[index]
		if record.State == EffectReverted || composition.retained(record.Descriptor.ModuleID) {
			continue
		}
		if inverseErr := safeInverse(host.inverse, ctx, record); inverseErr != nil {
			joined = errors.Join(joined, inverseErr)
			continue
		}
		joined = errors.Join(joined, safeMarkReverted(host.journal, ctx, record.Descriptor.ID))
	}
	for index := len(composition.moduleOrder) - 1; index >= 0; index-- {
		id := composition.moduleOrder[index]
		if composition.retained(id) {
			continue
		}
		if owner := composition.owners[id]; owner != nil {
			joined = errors.Join(joined, safeOwnerDeactivate(owner, ctx))
		}
	}
	if joined != nil {
		host.mu.Lock()
		composition.state = StateDraining
		host.mu.Unlock()
		return joined
	}
	host.mu.Lock()
	composition.state = StateDraining
	host.mu.Unlock()
	return nil
}

func (host *ModuleHost) finalizeDeactivation(ctx context.Context, composition *moduleComposition) error {
	// Durable removal is the commit point for deactivation. If the store is
	// unavailable, retain the in-memory draining composition so callers cannot
	// observe an inactive state that was not durably recorded.
	if err := host.durableRemove(ctx, composition.compositionRevision); err != nil {
		host.mu.Lock()
		composition.state = StateDraining
		host.mu.Unlock()
		return err
	}
	host.mu.Lock()
	composition.state = StateInactive
	for id := range composition.owners {
		if !composition.retained(id) {
			// A newer desired composition may have already assigned a more
			// specific state (for example WaitingDependencies). Only retire
			// the state that this draining composition itself marked.
			if host.states[id] == StateDraining {
				host.states[id] = StateInactive
			}
		}
	}
	if host.active == composition {
		host.active = nil
	}
	delete(host.compositions, composition.compositionRevision)
	host.mu.Unlock()
	return nil
}

func (composition *moduleComposition) retained(id ModuleID) bool {
	_, retained := composition.retainedIDs[id]
	return retained
}

func nonNilContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func validateLeaseRequest(request LeaseRequest) error {
	if err := validateID(request.LeaseID, "lease"); err != nil && request.LeaseID != "" {
		return fmt.Errorf("%w: %v", ErrLeaseMismatch, err)
	}
	if err := validateID(request.RunID, "run"); err != nil {
		return fmt.Errorf("%w: %v", ErrLeaseMismatch, err)
	}
	if err := validateID(request.CompositionRevision, "composition"); err != nil {
		return fmt.Errorf("%w: %v", ErrLeaseMismatch, err)
	}
	if err := validateID(string(request.ModuleID), "module"); err != nil || !request.ModuleRevision.Valid() {
		return ErrLeaseMismatch
	}
	if err := validateOpaqueScope(request.Scope); err != nil {
		return fmt.Errorf("%w: %v", ErrLeaseMismatch, err)
	}
	return nil
}

func validateOpaqueScope(value string) error {
	if value == "" || len(value) > 512 || !utf8.ValidString(value) {
		return fmt.Errorf("scope is empty or exceeds 512 bytes")
	}
	for index, character := range value {
		if character <= ' ' || character == '\u007f' {
			return fmt.Errorf("scope contains control or whitespace at %d", index)
		}
	}
	return nil
}
