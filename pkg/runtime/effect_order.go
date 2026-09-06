package runtime

import (
	"fmt"
	"sort"
)

func orderedEffects(records []RecordedEffect) ([]RecordedEffect, error) {
	ordered := append([]RecordedEffect(nil), records...)
	seen := make(map[uint64]struct{}, len(ordered))
	for _, record := range ordered {
		if record.Ordinal == 0 {
			return nil, fmt.Errorf("%w: effect %q has no durable ordinal", ErrInvalidEffect, record.Descriptor.ID)
		}
		if _, exists := seen[record.Ordinal]; exists {
			return nil, fmt.Errorf("%w: duplicate effect ordinal %d", ErrInvalidEffect, record.Ordinal)
		}
		seen[record.Ordinal] = struct{}{}
	}
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].Ordinal < ordered[j].Ordinal })
	return ordered, nil
}
