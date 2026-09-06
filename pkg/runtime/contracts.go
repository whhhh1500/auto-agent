package runtime

import "context"

// LifecycleState is the host-observed state of a module generation.
type LifecycleState string

const (
	StateDiscovered          LifecycleState = "discovered"
	StateValidated           LifecycleState = "validated"
	StateConstructed         LifecycleState = "constructed"
	StateStaged              LifecycleState = "staged"
	StateHealthy             LifecycleState = "healthy"
	StateActive              LifecycleState = "active"
	StateDeactivating        LifecycleState = "deactivating"
	StateDraining            LifecycleState = "draining"
	StateWaitingDependencies LifecycleState = "waiting_dependencies"
	StateInactive            LifecycleState = "inactive"
	StateFailed              LifecycleState = "failed"
)

func (state LifecycleState) Valid() bool {
	switch state {
	case StateDiscovered, StateValidated, StateConstructed, StateStaged, StateHealthy, StateActive,
		StateDeactivating, StateDraining, StateWaitingDependencies, StateInactive, StateFailed:
		return true
	default:
		return false
	}
}

// StageContext contains host metadata only. Business dependencies are
// constructor-injected into a Module and are intentionally absent here.
type StageContext struct {
	ModuleID            ModuleID
	ModuleRevision      Version
	CompositionRevision string
}

// Module is executable behavior paired with immutable metadata.
type Module interface {
	Manifest() ModuleManifest
	// Stage must be pure with respect to durable/runtime effects. Effects are
	// recorded only through ActivationTransaction during Activate.
	Stage(context.Context, StageContext) (StagedModule, error)
}

type StagedModule interface {
	Health(context.Context) error
	// Activate must Record an EffectID before its corresponding external
	// effect, and retries must be idempotent by that EffectID. Host panic
	// isolation preserves locks and cleanup flow; it cannot undo an external
	// side effect that module code performed before recording evidence.
	Activate(context.Context, ActivationTransaction) (ModuleLeaseOwner, error)
}

// RecoveryContext is the bounded, defensive evidence supplied when rebinding
// an already durable resource. PriorLeasesLost is always true: public leases
// are process-local and are never resurrected after restart.
type RecoveryContext struct {
	StageContext
	DesiredModuleState
	Effects         []RecordedEffect
	PriorLeasesLost bool
}

// RecoverableStagedModule may rebind existing resources recorded by the
// journal. Recover must not call Activate as a substitute, create new durable
// or external resources, or receive a transaction/dependency lookup. If
// recovery fails, the host performs only best-effort local cleanup; module
// owners must make Deactivate idempotent so that a later recovery can retry.
type RecoverableStagedModule interface {
	StagedModule
	Recover(context.Context, RecoveryContext) (ModuleLeaseOwner, error)
}

type ModuleLeaseOwner interface {
	AcquireLease(context.Context, LeaseRequest) (Lease, error)
	ReleaseLease(context.Context, Lease) error
	Drain(context.Context, DrainRequest) (DrainResult, error)
	// Fence must be idempotent by FenceRequest.RequestID. The host can retry
	// cleanup after an ambiguous result, so Deactivate must also be idempotent
	// for the same fenced resource transition.
	Fence(context.Context, FenceRequest) error
	Deactivate(context.Context) error
	Reconcile(context.Context, DesiredModuleState, []RecordedEffect, ActivationTransaction) error
}

// ActivationTransaction exposes only concrete effect recording and state
// transitions. It has no dependency lookup or service-locator operation.
type ActivationTransaction interface {
	// Record must be called before the described external effect is attempted.
	Record(context.Context, EffectDescriptor) (EffectID, error)
	MarkApplied(context.Context, EffectID) error
	MarkUnknown(context.Context, EffectID) error
}

type DesiredModuleState struct {
	ModuleID            ModuleID
	CompositionRevision string
	State               LifecycleState
}
