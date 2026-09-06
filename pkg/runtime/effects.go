package runtime

import "context"

type EffectID string

type EffectPhase string

const (
	EffectPhaseStage     EffectPhase = "stage"
	EffectPhaseActivate  EffectPhase = "activate"
	EffectPhaseReconcile EffectPhase = "reconcile"
)

func (phase EffectPhase) Valid() bool {
	switch phase {
	case EffectPhaseStage, EffectPhaseActivate, EffectPhaseReconcile:
		return true
	default:
		return false
	}
}

type EffectState string

const (
	EffectPrepared EffectState = "prepared"
	EffectApplied  EffectState = "applied"
	EffectUnknown  EffectState = "unknown"
	EffectReverted EffectState = "reverted"
)

func (state EffectState) Valid() bool {
	switch state {
	case EffectPrepared, EffectApplied, EffectUnknown, EffectReverted:
		return true
	default:
		return false
	}
}

// EffectAction is a typed, inspectable descriptor. Its Kind is interpreted by
// an injected executor; the runtime package never executes arbitrary code.
type EffectAction struct {
	Kind    string
	Target  string
	Payload []byte
}

type EffectDescriptor struct {
	ID                  EffectID
	ModuleID            ModuleID
	ModuleRevision      Version
	CompositionRevision string
	Phase               EffectPhase
	Forward             EffectAction
	Inverse             EffectAction
}

type RecordedEffect struct {
	Descriptor EffectDescriptor
	State      EffectState
	// Ordinal is the durable insertion order within a composition. Journal
	// List implementations must return records oldest to newest by Ordinal.
	Ordinal uint64
}

// EffectJournal is the durable boundary for forward-effect evidence and its
// inverse. Implementations must make Record/Mark operations idempotent by ID.
// List must return records with durable ordinals; hosts normalize their order
// oldest-to-newest and reject missing or duplicated ordinals.
type EffectJournal interface {
	Record(context.Context, EffectDescriptor) (RecordedEffect, error)
	MarkApplied(context.Context, EffectID) error
	MarkUnknown(context.Context, EffectID) error
	MarkReverted(context.Context, EffectID) error
	List(context.Context, string) ([]RecordedEffect, error)
}

type InverseExecutor interface {
	// ExecuteInverse must be idempotent by Descriptor.ID. Recovery may retry
	// after a successful inverse whose journal MarkReverted failed.
	ExecuteInverse(context.Context, RecordedEffect) error
}
