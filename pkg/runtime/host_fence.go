package runtime

import (
	"context"
	"errors"
	"time"
)

const fenceAuditTimeout = 5 * time.Second

// FenceAuthorized performs an authorized, request-idempotent fence. Owner
// implementations must make Fence idempotent by FenceRequest.RequestID and
// Deactivate idempotent for a retried fenced resource transition.
func (host *ModuleHost) FenceAuthorized(ctx context.Context, command FenceCommand) (DrainResult, error) {
	ctx = nonNilContext(ctx)
	host.opMu.Lock()
	defer host.opMu.Unlock()
	if err := contextError(ctx); err != nil {
		return DrainResult{}, err
	}
	if err := command.Validate(); err != nil {
		return DrainResult{}, err
	}
	if err := host.forceRenewOwnership(ctx); err != nil {
		return DrainResult{CompositionRevision: command.CompositionRevision}, err
	}
	if err := host.authorizeFence(ctx, command); err != nil {
		return DrainResult{}, err
	}
	if host.fenceJournal == nil {
		return DrainResult{}, ErrFenceUnauthorized
	}
	record, err := safeFenceBegin(host.fenceJournal, ctx, command)
	if err != nil {
		return DrainResult{}, err
	}
	switch record.Decision {
	case FenceDecisionReplay:
		return DrainResult{CompositionRevision: record.Result.CompositionRevision, Completed: record.Result.Completed, RemainingLeases: record.Result.RemainingLeases}, nil
	case FenceDecisionConflict:
		return DrainResult{}, ErrFenceConflict
	case FenceDecisionUnknown:
		return DrainResult{}, ErrFenceUnknown
	case FenceDecisionExecute, FenceDecisionRetry:
		composition, err := host.composition(command.CompositionRevision)
		if err != nil {
			return DrainResult{}, host.markFenceUnknown(ctx, command, err)
		}
		return host.executeFence(ctx, composition, command)
	default:
		return DrainResult{}, ErrFenceUnknown
	}
}

func (host *ModuleHost) authorizeFence(ctx context.Context, command FenceCommand) (err error) {
	if host.fenceAuthorizer == nil {
		return ErrFenceUnauthorized
	}
	defer func() {
		if recover() != nil {
			err = ErrFenceUnauthorized
		}
	}()
	if host.fenceAuthorizer.AuthorizeFence(ctx, command) != nil {
		return ErrFenceUnauthorized
	}
	return nil
}

func (host *ModuleHost) executeFence(ctx context.Context, composition *moduleComposition, command FenceCommand) (DrainResult, error) {
	if err := host.durableBlock(ctx, command.CompositionRevision); err != nil {
		return DrainResult{CompositionRevision: command.CompositionRevision}, host.markFenceUnknown(ctx, command, err)
	}
	composition.leaseMu.Lock()
	host.mu.Lock()
	composition.state = StateDraining
	for id := range composition.owners {
		if host.states[id] == StateActive {
			host.states[id] = StateDraining
		}
	}
	host.mu.Unlock()
	for index := len(composition.moduleOrder) - 1; index >= 0; index-- {
		if owner := composition.owners[composition.moduleOrder[index]]; owner != nil {
			if err := safeOwnerFence(owner, ctx, FenceRequest{RequestID: command.RequestID, CompositionRevision: command.CompositionRevision, Reason: command.Reason, HostGeneration: host.ownershipClaim.Generation}); err != nil {
				remaining := host.leaseCount(composition)
				composition.leaseMu.Unlock()
				return DrainResult{CompositionRevision: command.CompositionRevision, RemainingLeases: remaining}, host.markFenceUnknown(ctx, command, err)
			}
		}
	}
	host.mu.Lock()
	composition.leases = make(map[string]leaseRecord)
	host.mu.Unlock()
	composition.leaseMu.Unlock()
	if err := host.deactivateCompositionWork(ctx, composition); err != nil {
		return DrainResult{CompositionRevision: command.CompositionRevision}, host.markFenceUnknown(ctx, command, err)
	}
	if err := host.finalizeDeactivation(ctx, composition); err != nil {
		return DrainResult{CompositionRevision: command.CompositionRevision}, host.markFenceUnknown(ctx, command, err)
	}
	result := FenceResult{CompositionRevision: command.CompositionRevision, Completed: true}
	if err := safeFenceComplete(host.fenceJournal, ctx, command, result); err != nil {
		return DrainResult{CompositionRevision: command.CompositionRevision}, host.markFenceUnknown(ctx, command, err)
	}
	return DrainResult{CompositionRevision: command.CompositionRevision, Completed: true}, nil
}

func (host *ModuleHost) markFenceUnknown(_ context.Context, command FenceCommand, primary error) error {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), fenceAuditTimeout)
	defer cancel()
	return errors.Join(primary, safeFenceUnknown(host.fenceJournal, cleanupCtx, command))
}

func (host *ModuleHost) leaseCount(composition *moduleComposition) int {
	host.mu.RLock()
	defer host.mu.RUnlock()
	return len(composition.leases)
}
