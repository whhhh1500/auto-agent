package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/whhhh1500/auto-agent/pkg/app/runexecutor"
	"github.com/whhhh1500/auto-agent/pkg/control"
	core "github.com/whhhh1500/auto-agent/pkg/core"
)

const (
	runExecutorIDKey          = "harness.executor.id"
	runExecutorVersionKey     = "harness.executor.version"
	runExecutorImplementation = "harness.executor.implementation_revision"
)

var errExecutorSelection = errors.New("run executor selection failed")

// resolveRunExecutor is shared by synchronous and queued execution. It
// resolves profile selection after canary runtime selection, then freezes the
// selected executor identity into the run composition metadata.
func (s *Server) resolveRunExecutor(ctx context.Context, principal core.Principal, session *core.Session, runtime *core.Runtime, canary *control.CanaryAssignment, runID string, resume bool) (runexecutor.RunExecutor, map[string]string, error) {
	if session == nil || runtime == nil || s.runExecutors == nil {
		return nil, nil, fmt.Errorf("%w: incomplete dependencies", errExecutorSelection)
	}
	selection, err := s.executorSelection(ctx, principal, session, runtime, runID, resume)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %v", errExecutorSelection, err)
	}
	executor, metadata, err := s.runExecutors.ResolveOrDefault(selection.id, selection.version, runexecutor.Dependencies{Runtime: runtime})
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %v", errExecutorSelection, err)
	}
	if selection.implementation != "" && selection.implementation != metadata.ImplementationRevision {
		return nil, nil, fmt.Errorf("%w: implementation revision drift", errExecutorSelection)
	}
	composed := canaryCompositionMetadata(canary)
	if composed == nil {
		composed = map[string]string{}
	}
	composed[runExecutorIDKey] = metadata.ID
	composed[runExecutorVersionKey] = metadata.Version
	composed[runExecutorImplementation] = metadata.ImplementationRevision
	if err := core.ValidateRunCompositionMetadata(composed); err != nil {
		return nil, nil, fmt.Errorf("%w: metadata: %v", errExecutorSelection, err)
	}
	return executor, composed, nil
}

type executorSelection struct{ id, version, implementation string }

func (s *Server) executorSelection(_ context.Context, principal core.Principal, session *core.Session, runtime *core.Runtime, runID string, resume bool) (executorSelection, error) {
	if resume {
		return frozenExecutorSelection(session, runID)
	}
	profile, err := runtime.Profiles.Resolve(principal, session.Scope(), session.ProfileID())
	if err != nil {
		return executorSelection{}, err
	}
	id, hasID := profile.Metadata[runExecutorIDKey]
	version, hasVersion := profile.Metadata[runExecutorVersionKey]
	if hasID != hasVersion {
		return executorSelection{}, errors.New("executor id and version must be configured together")
	}
	if !hasID {
		return executorSelection{}, nil
	}
	if id == "" || version == "" {
		return executorSelection{}, errors.New("executor id and version cannot be empty")
	}
	return executorSelection{id: id, version: version}, nil
}

func frozenExecutorSelection(session *core.Session, runID string) (executorSelection, error) {
	if err := core.ValidateRunID(runID); err != nil {
		return executorSelection{}, err
	}
	var found *executorSelection
	for _, event := range session.Events() {
		if event.Type != core.EvRunStart || event.RunID != runID {
			continue
		}
		var data core.RunStartData
		if err := json.Unmarshal(event.Data, &data); err != nil {
			return executorSelection{}, err
		}
		metadata := map[string]string{}
		if data.Composition != nil {
			metadata = data.Composition.Metadata
		}
		id, hasID := metadata[runExecutorIDKey]
		version, hasVersion := metadata[runExecutorVersionKey]
		implementation, hasImplementation := metadata[runExecutorImplementation]
		if !hasID && !hasVersion && !hasImplementation {
			continue
		}
		if !hasID || !hasVersion || !hasImplementation || id == "" || version == "" || implementation == "" {
			return executorSelection{}, errors.New("run start has incomplete executor evidence")
		}
		// Implementation is checked by resolveRunExecutor after resolving the
		// exact registration; retain it in the selection via the package-local
		// field below.
		if found != nil && (found.id != id || found.version != version || found.implementation != implementation) {
			return executorSelection{}, errors.New("run start executor evidence changed")
		}
		found = &executorSelection{id: id, version: version, implementation: implementation}
	}
	if found == nil {
		// Legacy runs predate executor evidence and are sequential by contract.
		return executorSelection{}, nil
	}
	return *found, nil
}
