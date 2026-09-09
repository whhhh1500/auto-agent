package server

import (
	"context"

	"github.com/whhhh1500/auto-agent/internal/executionroute"
	"github.com/whhhh1500/auto-agent/pkg/app/runexecutor"
	"github.com/whhhh1500/auto-agent/pkg/control"
	core "github.com/whhhh1500/auto-agent/pkg/core"
)

const (
	runExecutorIDKey          = executionroute.ExecutorIDKey
	runExecutorVersionKey     = executionroute.ExecutorVersionKey
	runExecutorImplementation = executionroute.ExecutorImplementationKey
)

var errExecutorSelection = executionroute.ErrSelection

// resolveRunExecutor is shared by synchronous and queued execution. It
// resolves profile selection after canary runtime selection, then freezes the
// selected executor identity into the run composition metadata.
func (s *Server) resolveRunExecutor(ctx context.Context, principal core.Principal, session *core.Session, runtime *core.Runtime, canary *control.CanaryAssignment, runID string, resume bool) (runexecutor.RunExecutor, map[string]string, error) {
	decision, err := executionroute.Resolve(ctx, executionroute.Request{
		Runtime: runtime, Registry: s.runExecutors, Principal: principal, Session: session,
		RunID: runID, Resume: resume, BaseMetadata: canaryCompositionMetadata(canary),
	})
	if err != nil {
		return nil, nil, err
	}
	return decision.Executor, decision.CompositionMetadata, nil
}

type executorSelection struct{ id, version, implementation string }

func (s *Server) executorSelection(ctx context.Context, principal core.Principal, session *core.Session, runtime *core.Runtime, runID string, resume bool) (executorSelection, error) {
	decision, err := executionroute.Resolve(ctx, executionroute.Request{
		Runtime: runtime, Registry: s.runExecutors, Principal: principal, Session: session,
		RunID: runID, Resume: resume,
	})
	if err != nil {
		return executorSelection{}, err
	}
	return executorSelection{
		id: decision.Metadata.ID, version: decision.Metadata.Version,
		implementation: decision.Metadata.ImplementationRevision,
	}, nil
}
