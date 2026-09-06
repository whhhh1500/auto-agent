package runtime

import (
	"context"
	"fmt"
	"time"
)

func panicError(label string) error {
	return fmt.Errorf("%s panicked", label)
}

func safeFenceBegin(journal FenceJournal, ctx context.Context, command FenceCommand) (record FenceRecord, err error) {
	defer func() {
		if recover() != nil {
			record = FenceRecord{}
			err = panicError("fence journal begin")
		}
	}()
	return journal.Begin(ctx, command)
}

func safeFenceComplete(journal FenceJournal, ctx context.Context, command FenceCommand, result FenceResult) (err error) {
	defer func() {
		if recover() != nil {
			err = panicError("fence journal complete")
		}
	}()
	return journal.Complete(ctx, command, result)
}

func safeFenceUnknown(journal FenceJournal, ctx context.Context, command FenceCommand) (err error) {
	defer func() {
		if recover() != nil {
			err = panicError("fence journal unknown")
		}
	}()
	return journal.MarkUnknown(ctx, command)
}

func safeOwnerFence(owner ModuleLeaseOwner, ctx context.Context, request FenceRequest) (err error) {
	defer func() {
		if recover() != nil {
			err = panicError("module owner fence")
		}
	}()
	return owner.Fence(ctx, request)
}

func safeOwnerAcquireLease(owner ModuleLeaseOwner, ctx context.Context, request LeaseRequest) (lease Lease, err error) {
	defer func() {
		if recover() != nil {
			lease = Lease{}
			err = panicError("module owner acquire lease")
		}
	}()
	return owner.AcquireLease(ctx, request)
}

func safeOwnerReleaseLease(owner ModuleLeaseOwner, ctx context.Context, lease Lease) (err error) {
	defer func() {
		if recover() != nil {
			err = panicError("module owner release lease")
		}
	}()
	return owner.ReleaseLease(ctx, lease)
}

func safeOwnerDeactivate(owner ModuleLeaseOwner, ctx context.Context) (err error) {
	defer func() {
		if recover() != nil {
			err = panicError("module owner deactivate")
		}
	}()
	return owner.Deactivate(ctx)
}

func safeOwnerDrain(owner ModuleLeaseOwner, ctx context.Context, request DrainRequest) (result DrainResult, err error) {
	defer func() {
		if recover() != nil {
			result = DrainResult{}
			err = panicError("module owner drain")
		}
	}()
	return owner.Drain(ctx, request)
}

func safeOwnerReconcile(owner ModuleLeaseOwner, ctx context.Context, desired DesiredModuleState, records []RecordedEffect, transaction ActivationTransaction) (err error) {
	defer func() {
		if recover() != nil {
			err = panicError("module owner reconcile")
		}
	}()
	return owner.Reconcile(ctx, desired, records, transaction)
}

func safeOwnerRecover(staged RecoverableStagedModule, ctx context.Context, recovery RecoveryContext) (owner ModuleLeaseOwner, err error) {
	defer func() {
		if recover() != nil {
			owner = nil
			err = panicError("recoverable staged module")
		}
	}()
	return staged.Recover(ctx, recovery)
}

func safeInverse(inverse InverseExecutor, ctx context.Context, effect RecordedEffect) (err error) {
	defer func() {
		if recover() != nil {
			err = panicError("inverse executor")
		}
	}()
	return inverse.ExecuteInverse(ctx, effect)
}

func safeEffectList(journal EffectJournal, ctx context.Context, revision string) (records []RecordedEffect, err error) {
	defer func() {
		if recover() != nil {
			records = nil
			err = panicError("effect journal list")
		}
	}()
	return journal.List(ctx, revision)
}

func safeMarkReverted(journal EffectJournal, ctx context.Context, id EffectID) (err error) {
	defer func() {
		if recover() != nil {
			err = panicError("effect journal mark reverted")
		}
	}()
	return journal.MarkReverted(ctx, id)
}

func safeCompositionCAS(store CompositionStore, ctx context.Context, expected string, next DurableHostState) (err error) {
	defer func() {
		if recover() != nil {
			err = panicError("composition store compare-and-swap")
		}
	}()
	return store.CompareAndSwap(ctx, expected, next)
}

func safeCompositionLoad(store CompositionStore, ctx context.Context) (state DurableHostState, found bool, err error) {
	defer func() {
		if recover() != nil {
			state = DurableHostState{}
			found = false
			err = panicError("composition store load")
		}
	}()
	return store.Load(ctx)
}

func safeClaimHostOwnership(store HostOwnershipStore, ctx context.Context, holder string, ttl time.Duration) (claim HostOwnershipClaim, err error) {
	defer func() {
		if recover() != nil {
			claim = HostOwnershipClaim{}
			err = panicError("host ownership claim")
		}
	}()
	return store.ClaimHostOwnership(ctx, holder, ttl)
}

func safeRenewHostOwnership(store HostOwnershipStore, ctx context.Context, claim HostOwnershipClaim, ttl time.Duration) (next HostOwnershipClaim, err error) {
	defer func() {
		if recover() != nil {
			next = HostOwnershipClaim{}
			err = panicError("host ownership renew")
		}
	}()
	return store.RenewHostOwnership(ctx, claim, ttl)
}

func safeReleaseHostOwnership(store HostOwnershipStore, ctx context.Context, claim HostOwnershipClaim) (err error) {
	defer func() {
		if recover() != nil {
			err = panicError("host ownership release")
		}
	}()
	return store.ReleaseHostOwnership(ctx, claim)
}

// safeModuleStage isolates the host from third-party module code. Stage is
// deliberately effect-free, but a panic must still be converted into a
// candidate/recovery error so the host can run its normal cleanup path.
func safeModuleStage(module Module, ctx context.Context, stage StageContext) (staged StagedModule, err error) {
	defer func() {
		if recover() != nil {
			staged = nil
			err = panicError("module stage")
		}
	}()
	return module.Stage(ctx, stage)
}

func safeModuleManifest(module Module) (manifest ModuleManifest, err error) {
	defer func() {
		if recover() != nil {
			manifest = ModuleManifest{}
			err = panicError("module manifest")
		}
	}()
	return module.Manifest(), nil
}

func safeStagedHealth(staged StagedModule, ctx context.Context) (err error) {
	defer func() {
		if recover() != nil {
			err = panicError("staged module health")
		}
	}()
	return staged.Health(ctx)
}

func safeStagedActivate(staged StagedModule, ctx context.Context, transaction ActivationTransaction) (owner ModuleLeaseOwner, err error) {
	defer func() {
		if recover() != nil {
			owner = nil
			err = panicError("staged module activate")
		}
	}()
	return staged.Activate(ctx, transaction)
}
