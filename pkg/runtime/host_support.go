package runtime

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

func moduleOrder(manifests []ModuleManifest) ([]ModuleID, error) {
	byID := make(map[ModuleID]ModuleManifest, len(manifests))
	for _, manifest := range manifests {
		byID[manifest.ID] = manifest
	}
	graph := make(map[ModuleID]map[ModuleID]struct{}, len(manifests))
	indegree := make(map[ModuleID]int, len(manifests))
	for _, manifest := range manifests {
		graph[manifest.ID] = make(map[ModuleID]struct{})
		indegree[manifest.ID] = 0
	}
	for _, manifest := range manifests {
		dependencies := manifest.Requires
		for _, dependency := range dependencies {
			if _, found := byID[dependency.ModuleID]; !found {
				return nil, fmt.Errorf("%w: required module %q is missing", ErrDependency, dependency.ModuleID)
			}
			if _, exists := graph[dependency.ModuleID][manifest.ID]; !exists {
				graph[dependency.ModuleID][manifest.ID] = struct{}{}
				indegree[manifest.ID]++
			}
		}
		for _, dependency := range manifest.Optional {
			if _, found := byID[dependency.ModuleID]; !found {
				continue
			}
			if _, exists := graph[dependency.ModuleID][manifest.ID]; !exists {
				graph[dependency.ModuleID][manifest.ID] = struct{}{}
				indegree[manifest.ID]++
			}
		}
	}
	ready := make([]ModuleID, 0)
	for id, degree := range indegree {
		if degree == 0 {
			ready = append(ready, id)
		}
	}
	sort.Slice(ready, func(i, j int) bool { return ready[i] < ready[j] })
	order := make([]ModuleID, 0, len(manifests))
	for len(ready) != 0 {
		id := ready[0]
		ready = ready[1:]
		order = append(order, id)
		for next := range graph[id] {
			indegree[next]--
			if indegree[next] == 0 {
				ready = append(ready, next)
			}
		}
		sort.Slice(ready, func(i, j int) bool { return ready[i] < ready[j] })
	}
	if len(order) != len(manifests) {
		return nil, ErrDependencyCycle
	}
	return order, nil
}

type activationTransaction struct {
	journal             EffectJournal
	moduleID            ModuleID
	moduleRevision      Version
	compositionRevision string
	phase               EffectPhase
	allowedEffects      map[EffectKind]struct{}
	recorded            map[EffectID]struct{}
	mu                  sync.Mutex
}

func newActivationTransaction(journal EffectJournal, manifest ModuleManifest, compositionRevision string, phase EffectPhase) *activationTransaction {
	allowed := make(map[EffectKind]struct{}, len(manifest.Effects))
	for _, effect := range manifest.Effects {
		allowed[effect] = struct{}{}
	}
	return &activationTransaction{journal: journal, moduleID: manifest.ID, moduleRevision: manifest.Version, compositionRevision: compositionRevision, phase: phase, allowedEffects: allowed}
}

func (transaction *activationTransaction) Record(ctx context.Context, descriptor EffectDescriptor) (EffectID, error) {
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	if descriptor.ModuleID != transaction.moduleID || descriptor.ModuleRevision != transaction.moduleRevision || descriptor.CompositionRevision != transaction.compositionRevision || descriptor.Phase != transaction.phase {
		return "", fmt.Errorf("%w: effect ownership does not match activation transaction", ErrInvalidEffect)
	}
	if _, allowed := transaction.allowedEffects[EffectKind(descriptor.Forward.Kind)]; !allowed {
		return "", fmt.Errorf("%w: forward effect kind %q is not declared by manifest", ErrInvalidEffect, descriptor.Forward.Kind)
	}
	if _, allowed := transaction.allowedEffects[EffectKind(descriptor.Inverse.Kind)]; !allowed {
		return "", fmt.Errorf("%w: inverse effect kind %q is not declared by manifest", ErrInvalidEffect, descriptor.Inverse.Kind)
	}
	recorded, err := transaction.journal.Record(ctx, descriptor)
	if err != nil {
		return "", err
	}
	if !sameEffectDescriptor(recorded.Descriptor, descriptor) {
		return "", fmt.Errorf("%w: journal returned a different descriptor", ErrInvalidEffect)
	}
	if transaction.recorded == nil {
		transaction.recorded = make(map[EffectID]struct{})
	}
	transaction.recorded[recorded.Descriptor.ID] = struct{}{}
	return recorded.Descriptor.ID, nil
}

func (transaction *activationTransaction) MarkApplied(ctx context.Context, id EffectID) error {
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	if _, recorded := transaction.recorded[id]; !recorded {
		return fmt.Errorf("%w: effect %q is outside this transaction", ErrInvalidEffect, id)
	}
	return transaction.journal.MarkApplied(ctx, id)
}

func (transaction *activationTransaction) MarkUnknown(ctx context.Context, id EffectID) error {
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	if _, recorded := transaction.recorded[id]; !recorded {
		return fmt.Errorf("%w: effect %q is outside this transaction", ErrInvalidEffect, id)
	}
	return transaction.journal.MarkUnknown(ctx, id)
}

func (transaction *activationTransaction) recordedSnapshot() []EffectID {
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	ids := make([]EffectID, 0, len(transaction.recorded))
	for id := range transaction.recorded {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}
