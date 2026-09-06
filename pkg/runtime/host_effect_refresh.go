package runtime

import (
	"context"
	"fmt"
)

// refreshInheritedEffects re-reads every inherited generation from the
// journal. Inherited records are only a snapshot used to build a candidate;
// their state can change while a later durable CAS is retrying.
func (host *ModuleHost) refreshInheritedEffects(ctx context.Context, inherited []RecordedEffect) ([]RecordedEffect, error) {
	if len(inherited) == 0 {
		return nil, nil
	}
	expected := make(map[string]map[EffectID]EffectDescriptor)
	revisionOrder := make([]string, 0)
	for _, record := range inherited {
		revision := record.Descriptor.CompositionRevision
		if revision == "" {
			return nil, fmt.Errorf("%w: inherited effect %q has no composition", ErrInvalidEffect, record.Descriptor.ID)
		}
		if expected[revision] == nil {
			expected[revision] = make(map[EffectID]EffectDescriptor)
			revisionOrder = append(revisionOrder, revision)
		}
		if _, duplicate := expected[revision][record.Descriptor.ID]; duplicate {
			return nil, fmt.Errorf("%w: inherited effect %q is duplicated", ErrInvalidEffect, record.Descriptor.ID)
		}
		expected[revision][record.Descriptor.ID] = record.Descriptor
	}
	refreshed := make([]RecordedEffect, 0, len(inherited))
	for _, revision := range revisionOrder {
		records, err := safeEffectList(host.journal, ctx, revision)
		if err != nil {
			return nil, err
		}
		records, err = orderedEffects(records)
		if err != nil {
			return nil, err
		}
		seen := make(map[EffectID]struct{}, len(records))
		for _, record := range records {
			if record.Descriptor.CompositionRevision != revision {
				return nil, fmt.Errorf("%w: inherited effect %q belongs to another composition", ErrInvalidEffect, record.Descriptor.ID)
			}
			if err := ValidateEffectDescriptor(record.Descriptor); err != nil {
				return nil, err
			}
			if !record.State.Valid() {
				return nil, fmt.Errorf("%w: inherited effect %q has invalid state", ErrInvalidEffect, record.Descriptor.ID)
			}
			if _, duplicate := seen[record.Descriptor.ID]; duplicate {
				return nil, fmt.Errorf("%w: inherited effect %q is duplicated", ErrInvalidEffect, record.Descriptor.ID)
			}
			want, found := expected[revision][record.Descriptor.ID]
			if !found || !sameEffectDescriptor(want, record.Descriptor) {
				if !found {
					continue
				}
				return nil, fmt.Errorf("%w: inherited effect %q descriptor changed", ErrInvalidEffect, record.Descriptor.ID)
			}
			seen[record.Descriptor.ID] = struct{}{}
			refreshed = append(refreshed, record)
		}
		for id := range expected[revision] {
			if _, found := seen[id]; !found {
				return nil, fmt.Errorf("%w: inherited effect %q disappeared", ErrEffectNotFound, id)
			}
		}
	}
	return refreshed, nil
}
