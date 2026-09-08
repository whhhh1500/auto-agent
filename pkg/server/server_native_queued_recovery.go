package server

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/storage"
)

var errNativeQueuedCompletedToolRecoveryUnavailable = errors.New("native queued completed-tool recovery is unavailable")

// nativeQueuedRecoveryTestHooks exposes deterministic child-process fault
// boundaries only to package tests. It has no configuration or public API.
// Tests install it before starting workers and must not mutate it afterwards.
type nativeQueuedRecoveryTestHooks struct {
	afterToolJournalComplete   func()
	afterCompletedToolRecovery func()
	afterRecoveredModelAttempt func()
}

func (s *Server) nativeQueuedCompletedToolRecoveryStore() (*storage.SQLSessionStore, bool, error) {
	if s == nil || s.nativeStrict == nil {
		return nil, false, nil
	}
	if s.nativeStrict.phase != nativeStrictPhaseStaticBootstrap || s.nativeStrict.bootstrapRevision == "" || s.nativeStrict.db == nil {
		return nil, true, fmt.Errorf("%w: static native ownership is incomplete", errNativeQueuedCompletedToolRecoveryUnavailable)
	}
	store, ok := s.sessions.(*storage.SQLSessionStore)
	if !ok || store == nil || s.runtime == nil {
		return nil, true, fmt.Errorf("%w: native SQL runtime is incomplete", errNativeQueuedCompletedToolRecoveryUnavailable)
	}
	if err := storage.ValidateAuthorizationEpochSQLPrincipalAuthority(s.runPrincipal, s.sessions, s.runQueue, s.leaser, s.runtime.ToolJournal); err != nil {
		return nil, true, fmt.Errorf("%w: authority: %v", errNativeQueuedCompletedToolRecoveryUnavailable, err)
	}
	return store, true, nil
}

func (s *Server) recoverNativeQueuedCompletedToolResult(ctx context.Context, session *core.Session, principal core.Principal, fence storage.SessionWriteFence, lease *executionProjectionLease) (*core.Session, bool, error) {
	store, enabled, err := s.nativeQueuedCompletedToolRecoveryStore()
	if err != nil || !enabled {
		return session, false, err
	}
	if session == nil || !reflect.DeepEqual(session.Principal(), principal) {
		return session, false, fmt.Errorf("%w: principal does not match the durable session", errNativeQueuedCompletedToolRecoveryUnavailable)
	}
	epoch, err := lease.appliedEpoch()
	if err != nil {
		return session, false, err
	}
	recovered, applied, err := store.RecoverNativeQueuedCompletedToolResultFenced(ctx, fence, epoch, session.Version())
	if err != nil {
		return session, false, err
	}
	if !applied {
		return session, false, nil
	}
	if recovered == nil {
		return session, false, fmt.Errorf("%w: returned an empty session", errNativeQueuedCompletedToolRecoveryUnavailable)
	}
	if hooks := s.nativeQueuedRecoveryTestHooks; hooks != nil && hooks.afterCompletedToolRecovery != nil {
		hooks.afterCompletedToolRecovery()
	}
	return recovered, true, nil
}

func nativeQueuedCompletedToolRecoveryPermanent(err error) bool {
	return errors.Is(err, storage.ErrCompletedToolResultProofInvalid) || errors.Is(err, errNativeQueuedCompletedToolRecoveryUnavailable)
}

// closeOpenSessionFailureFenced terminalizes only an already-open run after a
// permanent native recovery proof failure. It deliberately records no tool
// result or step closer because that delivery was not proven.
func (s *Server) closeOpenSessionFailureFenced(ctx context.Context, session *core.Session, fence storage.SessionWriteFence, runID, errorCode string, cause error) error {
	if session == nil {
		return fmt.Errorf("native queued recovery close requires a session")
	}
	status, exists := session.RunStatus(runID)
	if !exists || status != "" {
		return fmt.Errorf("native queued recovery close requires an open run")
	}
	expectedVersion := session.Version()
	if _, err := session.Append(runID, core.EvRunError, core.NewRuntimeErrorData(errorCode, cause, false)); err != nil {
		return err
	}
	if _, err := session.Append(runID, core.EvRunEnd, core.RunEndData{Status: core.RunFailed}); err != nil {
		return err
	}
	persistCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), terminalPersistenceTimeout)
	defer cancel()
	appender, ok := s.sessions.(storage.FencedSessionAppender)
	if !ok {
		return fmt.Errorf("native queued recovery close requires fenced session append")
	}
	return appender.AppendEventsFenced(persistCtx, fence, expectedVersion, session.EventsFrom(expectedVersion))
}

func (s *Server) settleNativeQueuedCompletedToolRecoveryFailure(workerCtx context.Context, cancelRun context.CancelFunc, claim *queuedRunClaimMonitor, task storage.QueuedRun, workerID string, session *core.Session, fence storage.SessionWriteFence, cause error, errorCode string, permanent bool) error {
	if errors.Is(cause, storage.ErrSessionWriteFenceLost) || claim.reason.Load() == claimStopLost {
		return s.stopQueuedFencedWriter(cancelRun, claim, nil, cause)
	}
	if !permanent {
		claim.stop()
		return s.settleQueuedPreparationFailure(workerCtx, task, workerID, session, &fence, nil, false, errorCode, cause, false)
	}
	claim.stop()
	if err := s.closeOpenSessionFailureFenced(workerCtx, session, fence, task.RunID, errorCode, cause); err != nil {
		if errors.Is(err, storage.ErrSessionWriteFenceLost) {
			return s.stopQueuedFencedWriter(cancelRun, claim, nil, errors.Join(cause, err))
		}
		return errors.Join(cause, err)
	}
	return errors.Join(cause, s.finishQueuedRunClaim(task, workerID, core.RunFailed, errorCode))
}
