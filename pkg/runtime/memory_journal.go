package runtime

import (
	"bytes"
	"context"
	"fmt"
	"sync"
)

// MemoryEffectJournal is a thread-safe test/local implementation. It keeps
// insertion order so inverse execution can deterministically run backwards.
type MemoryEffectJournal struct {
	mu      sync.Mutex
	records map[EffectID]RecordedEffect
	order   []EffectID
}

func NewMemoryEffectJournal() *MemoryEffectJournal {
	return &MemoryEffectJournal{records: make(map[EffectID]RecordedEffect)}
}

func (journal *MemoryEffectJournal) Record(ctx context.Context, descriptor EffectDescriptor) (RecordedEffect, error) {
	if err := contextError(ctx); err != nil {
		return RecordedEffect{}, err
	}
	if err := ValidateEffectDescriptor(descriptor); err != nil {
		return RecordedEffect{}, err
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if prior, found := journal.records[descriptor.ID]; found {
		if !sameEffectDescriptor(prior.Descriptor, descriptor) || prior.State == EffectReverted {
			return RecordedEffect{}, fmt.Errorf("%w: effect %q already has another record", ErrEffectConflict, descriptor.ID)
		}
		return cloneRecordedEffect(prior), nil
	}
	recorded := RecordedEffect{Descriptor: cloneEffectDescriptor(descriptor), State: EffectPrepared, Ordinal: uint64(len(journal.order) + 1)}
	journal.records[descriptor.ID] = recorded
	journal.order = append(journal.order, descriptor.ID)
	return cloneRecordedEffect(recorded), nil
}

func sameEffectDescriptor(left, right EffectDescriptor) bool {
	return left.ID == right.ID && left.ModuleID == right.ModuleID && left.ModuleRevision == right.ModuleRevision &&
		left.CompositionRevision == right.CompositionRevision && left.Phase == right.Phase &&
		left.Forward.Kind == right.Forward.Kind && left.Forward.Target == right.Forward.Target && bytes.Equal(left.Forward.Payload, right.Forward.Payload) &&
		left.Inverse.Kind == right.Inverse.Kind && left.Inverse.Target == right.Inverse.Target && bytes.Equal(left.Inverse.Payload, right.Inverse.Payload)
}

func (journal *MemoryEffectJournal) MarkApplied(ctx context.Context, id EffectID) error {
	return journal.mark(ctx, id, EffectApplied)
}

func (journal *MemoryEffectJournal) MarkUnknown(ctx context.Context, id EffectID) error {
	return journal.mark(ctx, id, EffectUnknown)
}

func (journal *MemoryEffectJournal) MarkReverted(ctx context.Context, id EffectID) error {
	return journal.mark(ctx, id, EffectReverted)
}

func (journal *MemoryEffectJournal) mark(ctx context.Context, id EffectID, state EffectState) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	record, found := journal.records[id]
	if !found {
		return fmt.Errorf("%w: effect %q", ErrEffectNotFound, id)
	}
	if record.State == EffectReverted {
		if state == EffectReverted {
			return nil
		}
		return fmt.Errorf("%w: effect %q is already reverted", ErrEffectConflict, id)
	}
	if record.State == state {
		return nil
	}
	if state == EffectApplied && record.State != EffectPrepared {
		return fmt.Errorf("%w: effect %q cannot transition from %s to applied", ErrEffectConflict, id, record.State)
	}
	if state == EffectUnknown && record.State != EffectPrepared {
		return fmt.Errorf("%w: effect %q cannot transition from %s to unknown", ErrEffectConflict, id, record.State)
	}
	record.State = state
	journal.records[id] = record
	return nil
}

func (journal *MemoryEffectJournal) List(ctx context.Context, compositionRevision string) ([]RecordedEffect, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	result := make([]RecordedEffect, 0)
	for _, id := range journal.order {
		record := journal.records[id]
		if record.Descriptor.CompositionRevision == compositionRevision {
			result = append(result, cloneRecordedEffect(record))
		}
	}
	return result, nil
}

// ValidateEffectDescriptor is the canonical validation boundary for effect
// descriptors. Adapters must call it before persisting or replaying effects.
func cloneEffectDescriptor(descriptor EffectDescriptor) EffectDescriptor {
	clone := descriptor
	clone.Forward.Payload = append([]byte(nil), descriptor.Forward.Payload...)
	clone.Inverse.Payload = append([]byte(nil), descriptor.Inverse.Payload...)
	return clone
}

func cloneRecordedEffect(record RecordedEffect) RecordedEffect {
	record.Descriptor = cloneEffectDescriptor(record.Descriptor)
	return record
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}
