package server

import (
	"context"
	"time"
)

// nativeQueuedModelInvocationPruner is deliberately private so Native-static
// model evidence does not widen the generic retention surface.
type nativeQueuedModelInvocationPruner interface {
	PruneNativeQueuedModelInvocations(context.Context, time.Time) (int64, error)
}

// nativeQueuedRetentionTestHooks is package-private and immutable after
// StartRunWorkers. It makes worker-lifecycle tests observe a real retention
// tick without wrapping the sealed SQL pruner.
type nativeQueuedRetentionTestHooks struct {
	beforePrune func(context.Context)
}

func (s *Server) nativeQueuedModelInvocationRetentionPruner() (nativeQueuedModelInvocationPruner, bool, error) {
	store, enabled, err := s.nativeQueuedCompletedToolRecoveryStore()
	if err != nil || !enabled || store == nil {
		return nil, enabled, err
	}
	return store, true, nil
}

// runNativeQueuedModelInvocationRetention is blocking so StartRunWorkers can
// own it in workersWG. It only runs for the sealed Native-static SQL domain.
func (s *Server) runNativeQueuedModelInvocationRetention(ctx context.Context) {
	pruner, enabled, err := s.nativeQueuedModelInvocationRetentionPruner()
	if err != nil {
		s.logRetention("native_queued_model_invocations", 0, err)
		return
	}
	if !enabled {
		return
	}
	s.runRetentionLoop(ctx, s.nativeQueuedRetentionEvery, s.nativeQueuedRetentionWindow, func(ctx context.Context, cutoff time.Time) {
		if hooks := s.nativeQueuedRetentionTestHooks; hooks != nil && hooks.beforePrune != nil {
			hooks.beforePrune(ctx)
		}
		deleted, err := pruner.PruneNativeQueuedModelInvocations(ctx, cutoff)
		s.logRetention("native_queued_model_invocations", deleted, err)
	})
}
