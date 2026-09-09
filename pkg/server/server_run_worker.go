package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/whhhh1500/auto-agent/pkg/app/runexecutor"
	"github.com/whhhh1500/auto-agent/pkg/control"
	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/storage"
)

var (
	errRunClaimLost                 = errors.New("run claim ownership lost")
	errProfileProjectionUnavailable = errors.New("profile projection is unavailable")
)

const (
	claimStopNone int32 = iota
	claimStopCancelled
	claimStopLost
)

type queuedRunClaimMonitor struct {
	reason atomic.Int32
	stop   func()
}

// PermanentRunError marks a queue preparation failure that retrying cannot
// repair, such as a disabled account or an ownership mismatch.
type PermanentRunError struct{ Err error }

func (e PermanentRunError) Error() string {
	if e.Err == nil {
		return "permanent queued run failure"
	}
	return e.Err.Error()
}

func (e PermanentRunError) Unwrap() error { return e.Err }

// PermanentRunFailure wraps an error returned by RunPrincipalResolver when
// the queued request must fail instead of being retried.
func PermanentRunFailure(err error) error { return PermanentRunError{Err: err} }

func permanentQueuedPrincipalError(err error) bool {
	return errors.Is(err, storage.ErrQueuedPrincipalNotFound) ||
		errors.Is(err, storage.ErrQueuedPrincipalIdentityMismatch) ||
		errors.Is(err, storage.ErrQueuedPrincipalInactive) ||
		errors.Is(err, storage.ErrQueuedPrincipalInvalid)
}

// StartRunWorkers starts the configured number of local durable queue
// consumers. It is idempotent while workers are already running.
func (s *Server) StartRunWorkers(ctx context.Context) error {
	if s.runQueue == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("run worker context is nil")
	}
	s.workersMu.Lock()
	if s.workersRunning {
		s.workersMu.Unlock()
		return nil
	}
	activeCtx, cancelActive := context.WithCancel(ctx)
	claimCtx, stopClaims := context.WithCancel(activeCtx)
	done := make(chan struct{})
	s.workersRunning = true
	s.workersStopClaims = stopClaims
	s.workersCancelActive = cancelActive
	s.workersDone = done
	s.workersMu.Unlock()
	if err := s.recoverExpiredRunClaims(claimCtx); err != nil {
		stopClaims()
		cancelActive()
		s.workersMu.Lock()
		s.workersRunning = false
		s.workersStopClaims = nil
		s.workersCancelActive = nil
		s.workersDone = nil
		s.workersMu.Unlock()
		return err
	}
	if s.approvals != nil {
		opCtx, cancel := context.WithTimeout(claimCtx, runControlOperationTimeout)
		_, err := s.approvals.ExpireApprovals(opCtx, time.Now().UTC())
		cancel()
		if err != nil {
			stopClaims()
			cancelActive()
			s.workersMu.Lock()
			s.workersRunning = false
			s.workersStopClaims = nil
			s.workersCancelActive = nil
			s.workersDone = nil
			s.workersMu.Unlock()
			return err
		}
		s.observeApprovalMetrics(claimCtx)
	}
	s.workersWG.Add(1)
	go func() {
		defer s.workersWG.Done()
		s.runClaimRecoveryLoop(claimCtx)
	}()
	if s.approvals != nil {
		s.workersWG.Add(1)
		go func() {
			defer s.workersWG.Done()
			s.runApprovalExpiryLoop(claimCtx)
		}()
	}
	if _, enabled, err := s.nativeQueuedModelInvocationRetentionPruner(); err != nil {
		s.logRetention("native_queued_model_invocations", 0, err)
	} else if enabled {
		s.workersWG.Add(1)
		go func() {
			defer s.workersWG.Done()
			s.runNativeQueuedModelInvocationRetention(claimCtx)
		}()
	}
	for index := 0; index < s.runWorkerCount; index++ {
		workerID := fmt.Sprintf("%s:run-worker:%d", s.instanceID, index)
		s.workersWG.Add(1)
		go func() {
			defer s.workersWG.Done()
			s.runWorkerLoop(claimCtx, activeCtx, workerID)
		}()
	}
	go func() {
		s.workersWG.Wait()
		cancelActive()
		s.workersMu.Lock()
		s.workersRunning = false
		s.workersStopClaims = nil
		s.workersCancelActive = nil
		s.workersDone = nil
		s.workersMu.Unlock()
		close(done)
	}()
	return nil
}

func (s *Server) runApprovalExpiryLoop(ctx context.Context) {
	interval := s.runWorkerPoll
	if interval < time.Second {
		interval = time.Second
	} else if interval > 30*time.Second {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			opCtx, cancel := context.WithTimeout(ctx, runControlOperationTimeout)
			expired, err := s.approvals.ExpireApprovals(opCtx, time.Now().UTC())
			cancel()
			if err != nil && ctx.Err() == nil && s.logger != nil {
				s.logger.Error("approval expiry failed", slog.String("error", err.Error()))
			} else if expired > 0 && s.logger != nil {
				s.logger.Info("expired approvals resumed", slog.Int64("count", expired))
			}
			if expired > 0 {
				core.AddTelemetryCounter(s.telemetry, ctx, core.MetricApprovalDecisions, expired,
					core.TelemetryAttributes{"approval.decision": string(core.ApprovalExpired)})
			}
			s.observeApprovalMetrics(ctx)
		}
	}
}

func (s *Server) runClaimRecoveryLoop(ctx context.Context) {
	interval := s.runWorkerClaimTTL / 2
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.recoverExpiredRunClaims(ctx); err != nil && ctx.Err() == nil && s.logger != nil {
				s.logger.Error("expired run claim recovery failed", slog.String("error", err.Error()))
			}
		}
	}
}

func (s *Server) recoverExpiredRunClaims(ctx context.Context) error {
	opCtx, cancel := context.WithTimeout(ctx, runControlOperationTimeout)
	defer cancel()
	requeued, failed, err := s.runQueue.RecoverExpiredRunClaims(opCtx, time.Now().UTC())
	if err == nil && (requeued > 0 || failed > 0) && s.logger != nil {
		s.logger.Warn("recovered expired run claims", slog.Int64("requeued", requeued), slog.Int64("failed", failed))
	}
	if requeued > 0 {
		core.AddTelemetryCounter(s.telemetry, ctx, core.MetricQueueRecoveries, requeued,
			core.TelemetryAttributes{"recovery.outcome": "requeued"})
	}
	if failed > 0 {
		core.AddTelemetryCounter(s.telemetry, ctx, core.MetricQueueRecoveries, failed,
			core.TelemetryAttributes{"recovery.outcome": "failed"})
	}
	if err != nil {
		core.AddTelemetryCounter(s.telemetry, ctx, core.MetricQueueRecoveries, 1,
			core.TelemetryAttributes{"recovery.outcome": "error"})
	}
	s.observeQueueMetrics(ctx)
	return err
}

func (s *Server) runWorkerLoop(claimCtx, activeCtx context.Context, workerID string) {
	s.runWorkerLoopWith(claimCtx, activeCtx, workerID, func(claimCtx, activeCtx context.Context, workerID string) (bool, error) {
		return s.runWorkerOnce(claimCtx, activeCtx, workerID)
	})
}

func (s *Server) runWorkerLoopWith(claimCtx, activeCtx context.Context, workerID string, once func(context.Context, context.Context, string) (bool, error)) {
	basePoll := s.runWorkerPoll
	if basePoll <= 0 {
		basePoll = 250 * time.Millisecond
	}
	maxPoll := time.Second
	if basePoll > maxPoll {
		maxPoll = basePoll
	}
	poll := basePoll
	for {
		if claimCtx.Err() != nil {
			return
		}
		claimed, err := once(claimCtx, activeCtx, workerID)
		if err != nil && s.logger != nil {
			s.logger.Error("async run worker iteration failed", slog.String("worker", workerID), slog.String("error", err.Error()))
		}
		if claimed {
			poll = basePoll
			continue
		}
		wait := poll
		timer := time.NewTimer(wait)
		select {
		case <-claimCtx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		case <-timer.C:
		}
		if err != nil {
			poll = basePoll
		} else {
			poll = nextWorkerPoll(poll, basePoll, maxPoll)
		}
	}
}

func nextWorkerPoll(current, base, max time.Duration) time.Duration {
	if base <= 0 {
		base = 250 * time.Millisecond
	}
	if max < base {
		max = base
	}
	if current < base {
		return base
	}
	if current >= max {
		return max
	}
	next := current * 2
	if next < current || next > max {
		return max
	}
	return next
}

// RunWorkerOnce claims and executes at most one queued run.
func (s *Server) RunWorkerOnce(ctx context.Context, workerID string) (bool, error) {
	return s.runWorkerOnce(ctx, ctx, workerID)
}

func (s *Server) runWorkerOnce(claimCtx, activeCtx context.Context, workerID string) (bool, error) {
	if s.runQueue == nil {
		return false, fmt.Errorf("run queue is not configured")
	}
	if s.requiresExecutionProjectionEpoch() {
		if err := s.refreshExecutionProjection(claimCtx); err != nil {
			return false, fmt.Errorf("%w: refresh control plane: %v", errExecutionProjectionUnavailable, err)
		}
	}
	projectionLease, err := s.acquireExecutionProjection(claimCtx)
	if err != nil {
		return false, err
	}
	defer projectionLease.Release()
	claimStarted := time.Now()
	opCtx, cancel := context.WithTimeout(claimCtx, runControlOperationTimeout)
	task, claimed, err := s.runQueue.ClaimRun(opCtx, workerID, s.runWorkerClaimTTL)
	cancel()
	outcome := "claimed"
	if err != nil {
		outcome = "error"
	} else if !claimed {
		outcome = "empty"
	}
	if claimed || err != nil {
		_, claimSpan := core.StartTelemetry(s.telemetry, claimCtx, core.SpanQueueClaim,
			core.TelemetryAttributes{
				"worker.id": workerID, "claim.outcome": outcome,
				"run.id": task.RunID, "session.id": task.SessionID,
			})
		claimSpan.End(err, nil)
	}
	core.AddTelemetryCounter(s.telemetry, claimCtx, core.MetricQueueClaims, 1,
		core.TelemetryAttributes{"claim.outcome": outcome})
	core.RecordTelemetryHistogram(s.telemetry, claimCtx, core.MetricQueueClaimDuration,
		time.Since(claimStarted).Seconds(), "s", core.TelemetryAttributes{"claim.outcome": outcome})
	if err != nil || !claimed {
		return claimed, err
	}
	return true, s.executeQueuedRun(activeCtx, workerID, task, projectionLease)
}

func (s *Server) executeQueuedRun(workerCtx context.Context, workerID string, task storage.QueuedRun, projectionLease *executionProjectionLease) error {
	unlock := s.sessionLock(task.SessionID)
	defer unlock()
	traceCtx := core.ExtractTelemetryTraceContext(s.telemetry, workerCtx, task.TraceContext)
	runCtx, cancelRun := context.WithCancel(traceCtx)
	defer cancelRun()
	claim := s.monitorQueuedRunClaim(runCtx, cancelRun, task, workerID)
	defer claim.stop()
	if claim.reason.Load() == claimStopLost {
		return errRunClaimLost
	}

	if s.leaser == nil {
		claim.stop()
		return fmt.Errorf("queued run requires a session leaser")
	}
	lease, acquired, err := s.acquireRunLease(runCtx, cancelRun, task.SessionID, task.RunID, task.Generation)
	if err != nil || !acquired {
		claim.stop()
		if claim.reason.Load() == claimStopLost {
			return s.stopQueuedFencedWriter(cancelRun, claim, nil, err)
		}
		if err == nil {
			err = fmt.Errorf("session lease is unavailable")
		}
		return s.settleQueuedPreparationFailure(workerCtx, task, workerID, nil, nil, nil, false, "session_lease_unavailable", err, false)
	}
	defer lease.Release()
	fence := storage.SessionWriteFence{
		SessionID: task.SessionID, RunID: task.RunID, TenantID: task.TenantID, SubjectID: task.SubjectID,
		WorkerID: workerID, QueueGeneration: task.Generation, LeaseHolder: lease.Holder(),
	}
	return s.executeQueuedRunWithFence(workerCtx, runCtx, cancelRun, claim, task, workerID, fence, projectionLease)
}

func (s *Server) executeQueuedRunWithFence(workerCtx, runCtx context.Context, cancelRun context.CancelFunc, claim *queuedRunClaimMonitor, task storage.QueuedRun, workerID string, fence storage.SessionWriteFence, projectionLease *executionProjectionLease) error {
	if claim.reason.Load() == claimStopLost {
		return errRunClaimLost
	}

	session, err := s.sessions.Load(runCtx, task.SessionID)
	if err != nil {
		claim.stop()
		if claim.reason.Load() == claimStopLost {
			return s.stopQueuedFencedWriter(cancelRun, claim, nil, err)
		}
		return s.settleQueuedPreparationFailure(workerCtx, task, workerID, nil, nil, nil, false, "session_load_failed", err, false)
	}
	if claim.reason.Load() == claimStopLost {
		return errRunClaimLost
	}
	owner := session.Principal()
	resume := false
	if status, exists := session.RunStatus(task.RunID); exists && status == core.RunWaitingApproval {
		resume = true
	}
	principal, err := s.resolveQueuedPrincipal(runCtx, task)
	if err != nil {
		claim.stop()
		if claim.reason.Load() == claimStopLost {
			return s.stopQueuedFencedWriter(cancelRun, claim, nil, err)
		}
		var permanent PermanentRunError
		return s.settleQueuedPreparationFailure(workerCtx, task, workerID, session, &fence, nil, resume, "principal_resolution_failed", err, errors.As(err, &permanent) || permanentQueuedPrincipalError(err))
	}
	if owner.SubjectID != task.SubjectID || owner.TenantID != task.TenantID || !owner.Scope.Equal(principal.Scope) {
		claim.stop()
		if claim.reason.Load() == claimStopLost {
			return s.stopQueuedFencedWriter(cancelRun, claim, nil, nil)
		}
		err := fmt.Errorf("queued run owner does not match session owner")
		return s.settleQueuedPreparationFailure(workerCtx, task, workerID, session, &fence, nil, resume, "session_owner_mismatch", err, true)
	}
	if claim.reason.Load() == claimStopLost {
		return errRunClaimLost
	}
	recoveredContinuation := false
	if recovered, applied, recoveryErr := s.recoverNativeQueuedCompletedToolResult(runCtx, session, principal, fence, projectionLease); recoveryErr != nil {
		claim.stop()
		if claim.reason.Load() == claimStopLost || errors.Is(recoveryErr, storage.ErrSessionWriteFenceLost) {
			return s.stopQueuedFencedWriter(cancelRun, claim, nil, recoveryErr)
		}
		return s.settleNativeQueuedCompletedToolRecoveryFailure(
			workerCtx, cancelRun, claim, task, workerID, session, fence, recoveryErr,
			"native_completed_tool_recovery_failed", nativeQueuedCompletedToolRecoveryPermanent(recoveryErr),
		)
	} else if applied {
		session = recovered
		recoveredContinuation = true
	} else {
		session, _, err = storage.RepairInterruptedSessionFenced(runCtx, s.sessions, fence, session)
		if err != nil {
			claim.stop()
			if claim.reason.Load() == claimStopLost || errors.Is(err, storage.ErrSessionWriteFenceLost) {
				return s.stopQueuedFencedWriter(cancelRun, claim, nil, err)
			}
			return s.settleQueuedPreparationFailure(workerCtx, task, workerID, session, &fence, nil, resume, "session_repair_failed", err, false)
		}
	}
	if existingStatus, exists := session.RunStatus(task.RunID); exists {
		if existingStatus == core.RunWaitingApproval {
			resume = true
		} else if recoveredContinuation && existingStatus == "" {
			// The sealed A/B recovery path has restored a canonical tool/result
			// into this still-open run. ContinueTurn owns its next step.
		} else {
			claim.stop()
			if claim.reason.Load() == claimStopLost {
				return s.stopQueuedFencedWriter(cancelRun, claim, nil, nil)
			}
			if existingStatus == "" {
				existingStatus = core.RunFailed
			}
			errorCode := runTerminalErrorCode(session, task.RunID)
			if errorCode == "" && existingStatus == core.RunFailed {
				errorCode = "run_already_started"
			}
			if err := s.finishQueuedRunClaim(task, workerID, existingStatus, errorCode); err != nil {
				return err
			}
			s.observeTerminalRouteEvidence(workerCtx, s.runtime, session, principal, task.RunID, existingStatus)
			return nil
		}
	}

	expectedVersion := session.Version()
	writerStore, err := s.nativeQueuedWriteBehindStore()
	if err != nil {
		claim.stop()
		if claim.reason.Load() == claimStopLost {
			return s.stopQueuedFencedWriter(cancelRun, claim, nil, err)
		}
		return s.settleQueuedPreparationFailure(workerCtx, task, workerID, session, &fence, nil, resume, "fenced_writer_init_failed", err, true)
	}
	writer, err := storage.NewFencedWriteBehind(writerStore, fence, session, expectedVersion, s.maxWriteDelay)
	if err != nil {
		claim.stop()
		if claim.reason.Load() == claimStopLost {
			return s.stopQueuedFencedWriter(cancelRun, claim, nil, err)
		}
		return s.settleQueuedPreparationFailure(workerCtx, task, workerID, session, &fence, nil, resume, "fenced_writer_init_failed", err, true)
	}
	writer.SetErrorObserver(func(persistErr error) {
		if errors.Is(persistErr, storage.ErrSessionWriteFenceLost) {
			claim.reason.Store(claimStopLost)
			cancelRun()
		}
	})
	var runRuntime *core.Runtime
	var canary *control.CanaryAssignment
	if s.requiresExecutionProjectionEpoch() {
		runRuntime, canary, err = s.runtimeForCurrent(principal, session.ProfileID())
	} else {
		runRuntime, canary, err = s.runtimeFor(runCtx, principal, session.ProfileID())
	}
	if err != nil {
		claim.stop()
		if claim.reason.Load() == claimStopLost {
			return s.stopQueuedFencedWriter(cancelRun, claim, writer, err)
		}
		return s.settleQueuedPreparationFailure(
			workerCtx, task, workerID, session, &fence, writer, resume, "control_plane_refresh_failed", err, false,
		)
	}
	if canary != nil && canary.Candidate {
		runCtx = core.WithTelemetrySpanAttributes(runCtx, core.TelemetryAttributes{telemetryCanaryID: canary.ID})
	}
	if canary != nil && canary.Candidate && s.logger != nil {
		s.logger.InfoContext(runCtx, "canary selected", slog.String("canary", canary.ID), slog.String("profile", canary.ProfileID), slog.String("run", task.RunID), slog.String("worker", workerID))
	}
	runRuntime, checkpointFailure, err := s.runtimeWithQueuedToolCheckpoint(runRuntime, writer, cancelRun, fence, session, principal, recoveredContinuation)
	if err != nil {
		claim.stop()
		if claim.reason.Load() == claimStopLost {
			return s.stopQueuedFencedWriter(cancelRun, claim, writer, err)
		}
		return s.settleQueuedPreparationFailure(workerCtx, task, workerID, session, &fence, writer, resume, "tool_checkpoint_init_failed", err, false)
	}
	var runExecutor runexecutor.RunExecutor
	var compositionMetadata map[string]string
	runExecutor, compositionMetadata, err = s.resolveRunExecutor(runCtx, principal, session, runRuntime, canary, task.RunID, resume || recoveredContinuation)
	if err != nil {
		claim.stop()
		if claim.reason.Load() == claimStopLost {
			return s.stopQueuedFencedWriter(cancelRun, claim, writer, err)
		}
		if recoveredContinuation {
			return s.settleNativeQueuedCompletedToolRecoveryFailure(
				workerCtx, cancelRun, claim, task, workerID, session, fence, err,
				"native_completed_tool_recovery_unsupported", true,
			)
		}
		return s.settleQueuedPreparationFailure(workerCtx, task, workerID, session, &fence, writer, resume, "executor_selection_failed", err, true)
	}
	if recoveredContinuation {
		if _, ok := runExecutor.(*runexecutor.Sequential); !ok {
			claim.stop()
			return s.settleNativeQueuedCompletedToolRecoveryFailure(
				workerCtx, cancelRun, claim, task, workerID, session, fence,
				fmt.Errorf("native queued completed-tool recovery requires the built-in sequential executor"),
				"native_completed_tool_recovery_unsupported", true,
			)
		}
	}
	if err := requireProjectionAwareExecutor(projectionLease, runExecutor); err != nil {
		claim.stop()
		if claim.reason.Load() == claimStopLost {
			return s.stopQueuedFencedWriter(cancelRun, claim, writer, err)
		}
		return s.settleQueuedPreparationFailure(workerCtx, task, workerID, session, &fence, writer, resume, "execution_projection_unavailable", err, false)
	}
	seenObsHits := map[string]bool{}
	emit := func(event core.SessionEvent) {
		projectionLease.releaseOnEvent(event)
		writer.MarkDirty()
		s.observeEventContext(runCtx, seenObsHits, session, event)
	}
	started := time.Now()
	var result core.TurnResult
	var runErr error
	if recoveredContinuation {
		sequential := runExecutor.(*runexecutor.Sequential)
		result, runErr = sequential.ContinueTurn(runCtx, principal, session, core.ResumeInput{
			RunID: task.RunID, CompositionMetadata: compositionMetadata,
		}, emit)
	} else if resume {
		result, runErr = runExecutor.ResumeTurn(runCtx, principal, session, core.ResumeInput{
			RunID: task.RunID, CompositionMetadata: compositionMetadata,
		}, emit)
	} else {
		result, runErr = runExecutor.RunTurn(runCtx, principal, session, core.TurnInput{
			RunID: task.RunID, Text: task.Message, CompositionMetadata: compositionMetadata,
		}, emit)
	}
	status := effectiveRunStatus(result, runErr)
	if claim.reason.Load() == claimStopLost || (runCtx.Err() != nil && claim.reason.Load() == claimStopNone && !checkpointFailure.Failed()) {
		return s.stopQueuedFencedWriter(cancelRun, claim, writer, nil)
	}
	flushCtx, cancelFlush := context.WithTimeout(context.WithoutCancel(runCtx), terminalPersistenceTimeout)
	flushErr := writer.Flush(flushCtx)
	cancelFlush()
	claim.stop()
	if errors.Is(flushErr, storage.ErrSessionWriteFenceLost) {
		return s.stopQueuedFencedWriter(cancelRun, claim, writer, flushErr)
	}
	if claim.reason.Load() == claimStopLost || (runCtx.Err() != nil && claim.reason.Load() == claimStopNone && !checkpointFailure.Failed()) {
		return s.stopQueuedFencedWriter(cancelRun, claim, writer, nil)
	}
	if flushErr != nil {
		_ = s.finishQueuedRunClaim(task, workerID, core.RunFailed, "store_error")
		return flushErr
	}
	if status == core.RunWaitingApproval {
		paused, err := s.runQueue.PauseRunClaim(context.WithoutCancel(runCtx), task.RunID, workerID, task.Generation)
		if err != nil {
			return err
		}
		if !paused {
			return errRunClaimLost
		}
		return nil
	}
	if err := s.finishQueuedRunClaim(task, workerID, status, runTerminalErrorCode(session, task.RunID)); err != nil {
		return err
	}
	s.observeTerminalRouteEvidence(runCtx, runRuntime, session, principal, task.RunID, status)
	if s.runStats != nil {
		s.recordRunStat(context.WithoutCancel(runCtx), session, task.RunID, principal, status, started)
	}
	return nil
}

func (s *Server) stopQueuedFencedWriter(cancelRun context.CancelFunc, claim *queuedRunClaimMonitor, writer *storage.WriteBehind, cause error) error {
	if claim != nil {
		claim.reason.Store(claimStopLost)
		claim.stop()
	}
	if cancelRun != nil {
		cancelRun()
	}
	if writer != nil {
		abortCtx, cancelAbort := context.WithTimeout(context.Background(), terminalPersistenceTimeout)
		abortErr := writer.Abort(abortCtx)
		cancelAbort()
		if cause == nil {
			cause = abortErr
		}
	}
	if cause == nil {
		return errRunClaimLost
	}
	return errors.Join(errRunClaimLost, cause)
}

func (s *Server) settleQueuedPreparationFailure(workerCtx context.Context, task storage.QueuedRun, workerID string, session *core.Session, fence *storage.SessionWriteFence, writer *storage.WriteBehind, waiting bool, errorCode string, cause error, permanent bool) error {
	if errors.Is(cause, storage.ErrSessionWriteFenceLost) {
		return errors.Join(errRunClaimLost, cause)
	}
	if workerCtx.Err() != nil {
		return errors.Join(cause, workerCtx.Err())
	}
	if permanent || task.Attempt >= task.MaxAttempts {
		var sessionErr error
		if waiting && session != nil {
			sessionErr = s.closeWaitingSessionValueFenced(workerCtx, session, fence, writer, task.RunID, core.RunFailed, errorCode, cause)
			if errors.Is(sessionErr, storage.ErrSessionWriteFenceLost) {
				return errors.Join(errRunClaimLost, cause, sessionErr)
			}
			if sessionErr != nil {
				return errors.Join(cause, sessionErr)
			}
		}
		controlErr := s.finishQueuedRunClaim(task, workerID, core.RunFailed, errorCode)
		return errors.Join(cause, controlErr, sessionErr)
	}
	delay := queuedRunRetryDelay(task.Attempt)
	opCtx, cancel := context.WithTimeout(context.Background(), runControlOperationTimeout)
	defer cancel()
	retried, err := s.runQueue.RetryRunClaim(opCtx, task.RunID, workerID, task.Generation, time.Now().UTC().Add(delay), errorCode)
	if err != nil {
		return errors.Join(cause, err)
	}
	if !retried {
		return errors.Join(cause, errRunClaimLost)
	}
	return cause
}

func queuedRunRetryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	shift := attempt - 1
	if shift > 7 {
		shift = 7
	}
	delay := 250 * time.Millisecond * time.Duration(1<<shift)
	if delay > 30*time.Second {
		return 30 * time.Second
	}
	return delay
}

func (s *Server) finishQueuedRunClaim(task storage.QueuedRun, workerID string, status core.RunStatus, errorCode string) error {
	opCtx, cancel := context.WithTimeout(context.Background(), runControlOperationTimeout)
	defer cancel()
	finished, err := s.runQueue.FinishRunClaim(opCtx, task.RunID, workerID, task.Generation, status, errorCode)
	if err != nil {
		return err
	}
	if !finished {
		return errRunClaimLost
	}
	return nil
}

func (s *Server) resolveQueuedPrincipal(ctx context.Context, task storage.QueuedRun) (principal core.Principal, err error) {
	if s.runPrincipal == nil {
		return core.Principal{}, fmt.Errorf("run principal resolver is not configured")
	}
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("run principal resolver panicked")
			principal = core.Principal{}
		}
	}()
	principal, err = s.runPrincipal.ResolveRunPrincipal(ctx, task.TenantID, task.SubjectID)
	if err != nil {
		return core.Principal{}, err
	}
	if principal.TenantID != task.TenantID || principal.SubjectID != task.SubjectID {
		return core.Principal{}, fmt.Errorf("resolved principal identity does not match queued owner")
	}
	return principal, nil
}

func (s *Server) monitorQueuedRunClaim(runCtx context.Context, cancelRun context.CancelFunc, task storage.QueuedRun, workerID string) *queuedRunClaimMonitor {
	monitor := &queuedRunClaimMonitor{}
	if s.liveness == nil {
		monitor.reason.Store(claimStopLost)
		cancelRun()
		monitor.stop = func() {}
		return monitor
	}
	stop, err := s.liveness.Register(runCtx, "queue-claim:"+task.RunID, leaseRenewInterval(s.runWorkerClaimTTL), func(ctx context.Context) error {
		opCtx, cancel := context.WithTimeout(ctx, runControlOperationTimeout)
		renewed, renewErr := s.runQueue.RenewRunClaim(opCtx, task.RunID, workerID, task.Generation, s.runWorkerClaimTTL)
		requested := false
		if renewErr == nil && renewed {
			requested, renewErr = s.runQueue.RunCancelRequested(opCtx, task.RunID)
		}
		cancel()
		if requested {
			monitor.reason.CompareAndSwap(claimStopNone, claimStopCancelled)
			cancelRun()
			return nil
		}
		if renewErr != nil || !renewed {
			if ctx.Err() != nil && (errors.Is(renewErr, context.Canceled) || errors.Is(renewErr, context.DeadlineExceeded)) {
				return nil
			}
			monitor.reason.CompareAndSwap(claimStopNone, claimStopLost)
			return fmt.Errorf("queued run claim renewal failed")
		}
		if ctx.Err() != nil {
			return nil
		}
		return nil
	}, func() {
		monitor.reason.CompareAndSwap(claimStopNone, claimStopLost)
		cancelRun()
	})
	if err != nil {
		monitor.reason.Store(claimStopLost)
		cancelRun()
		monitor.stop = func() {}
		return monitor
	}
	monitor.stop = stop
	return monitor
}

func (s *Server) handleEnqueueRun(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	session, ok := s.loadOwnedSession(w, r, principal)
	if !ok {
		return
	}
	if s.runQueue == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "run queue is not configured"})
		return
	}
	var request struct {
		Message string `json:"message"`
	}
	if !s.decodeJSON(w, r, &request) {
		return
	}
	if request.Message == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "message is required"})
		return
	}
	runID, err := core.NewID("run_")
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	record := storage.QueuedRun{
		RunRecord: storage.RunRecord{RunID: runID, SessionID: session.ID(), TenantID: principal.TenantID, SubjectID: principal.SubjectID, Status: storage.RunStatusQueued},
		Message:   request.Message, MaxAttempts: s.runWorkerAttempts,
		TraceContext: core.InjectTelemetryTraceContext(s.telemetry, r.Context()),
	}
	idempotencyKey := r.Header.Get("Idempotency-Key")
	created := true
	var durable storage.RunRecord
	if idempotencyKey == "" {
		if err := s.runQueue.EnqueueRun(r.Context(), record); err != nil {
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		durable, err = s.runQueue.GetRun(r.Context(), runID)
	} else {
		durable, created, err = s.runQueue.EnqueueRunOnce(
			r.Context(), record, idempotencyKey, storage.RunMessageDigest(request.Message),
		)
	}
	if err != nil {
		status := http.StatusInternalServerError
		switch {
		case errors.Is(err, storage.ErrInvalidRunSubmission):
			status = http.StatusBadRequest
		case errors.Is(err, storage.ErrRunSubmissionConflict), errors.Is(err, core.ErrSessionConflict):
			status = http.StatusConflict
		}
		writeJSON(w, status, map[string]string{"error": err.Error()})
		core.AddTelemetryCounter(s.telemetry, r.Context(), core.MetricSubmissions, 1,
			core.TelemetryAttributes{"submission.outcome": submissionTelemetryOutcome(err)})
		return
	}
	submissionOutcome := "created"
	if idempotencyKey == "" {
		submissionOutcome = "created_without_key"
	} else if !created {
		submissionOutcome = "replayed"
	}
	core.AddTelemetryCounter(s.telemetry, r.Context(), core.MetricSubmissions, 1,
		core.TelemetryAttributes{"submission.outcome": submissionOutcome})
	w.Header().Set("Location", "/v1/sessions/"+session.ID()+"/runs/"+durable.RunID)
	if idempotencyKey != "" {
		w.Header().Set("Idempotency-Replayed", strconv.FormatBool(!created))
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusAccepted, durable)
}

func submissionTelemetryOutcome(err error) string {
	switch {
	case errors.Is(err, storage.ErrInvalidRunSubmission):
		return "invalid"
	case errors.Is(err, storage.ErrRunSubmissionConflict), errors.Is(err, core.ErrSessionConflict):
		return "conflict"
	default:
		return "error"
	}
}

func (s *Server) observeOperationalMetrics(ctx context.Context) {
	s.observeQueueMetrics(ctx)
	s.observeApprovalMetrics(ctx)
}

func (s *Server) observeQueueMetrics(ctx context.Context) {
	provider, ok := s.runQueue.(storage.RunQueueMetricsStore)
	if !ok || s.telemetry == nil {
		return
	}
	opCtx, cancel := context.WithTimeout(ctx, runControlOperationTimeout)
	metrics, err := provider.RunQueueMetrics(opCtx)
	cancel()
	if err != nil {
		return
	}
	for status, value := range map[string]int64{
		storage.RunStatusQueued:          metrics.Queued,
		storage.RunStatusRunning:         metrics.Running,
		storage.RunStatusWaitingApproval: metrics.WaitingApproval,
	} {
		core.SetTelemetryGauge(s.telemetry, ctx, core.MetricQueueDepth, float64(value), "{run}",
			core.TelemetryAttributes{"run.status": status})
	}
	age := 0.0
	if !metrics.OldestQueuedAt.IsZero() {
		age = time.Since(metrics.OldestQueuedAt).Seconds()
		if age < 0 {
			age = 0
		}
	}
	core.SetTelemetryGauge(s.telemetry, ctx, core.MetricQueueOldestAge, age, "s", nil)
}

func (s *Server) observeApprovalMetrics(ctx context.Context) {
	provider, ok := s.approvals.(storage.ApprovalMetricsStore)
	if !ok || s.telemetry == nil {
		return
	}
	opCtx, cancel := context.WithTimeout(ctx, runControlOperationTimeout)
	metrics, err := provider.ApprovalMetrics(opCtx)
	cancel()
	if err != nil {
		return
	}
	core.SetTelemetryGauge(s.telemetry, ctx, core.MetricApprovalPending, float64(metrics.Pending), "{approval}", nil)
	age := 0.0
	if !metrics.OldestRequestedAt.IsZero() {
		age = time.Since(metrics.OldestRequestedAt).Seconds()
		if age < 0 {
			age = 0
		}
	}
	core.SetTelemetryGauge(s.telemetry, ctx, core.MetricApprovalOldestAge, age, "s", nil)
}

func (s *Server) handleCancelRun(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	session, ok := s.loadOwnedSession(w, r, principal)
	if !ok {
		return
	}
	if s.runControl == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "run control store is not configured"})
		return
	}
	runID := r.PathValue("runID")
	record, err := s.runControl.GetRun(r.Context(), runID)
	if err != nil || record.SessionID != session.ID() {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "run not found"})
		return
	}
	requested, err := s.runControl.RequestRunCancel(r.Context(), runID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if !requested {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "run is already terminal"})
		return
	}
	if s.approvals != nil {
		closed, _ := s.approvals.CancelRunApprovals(r.Context(), runID, principal.SubjectID)
		if closed > 0 {
			core.AddTelemetryCounter(s.telemetry, r.Context(), core.MetricApprovalDecisions, closed,
				core.TelemetryAttributes{"approval.decision": string(core.ApprovalDenied), "approval.reason": "run_cancel"})
		}
	}
	if record.Status == storage.RunStatusWaitingApproval {
		if err := s.closeWaitingSessionRun(r.Context(), session.ID(), runID); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
	}
	s.runsMu.Lock()
	active := s.runs[session.ID()]
	s.runsMu.Unlock()
	if active != nil && active.runID == runID {
		active.cancel()
	}
	if err := s.cancelDirectDelegations(r.Context(), session.ID(), runID, principal.TenantID); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "direct child cancellation failed"})
		return
	}
	responseStatus := "cancelling"
	if current, getErr := s.runControl.GetRun(r.Context(), runID); getErr == nil && current.Status == string(core.RunCancelled) {
		responseStatus = string(core.RunCancelled)
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"run_id": runID, "status": responseStatus})
}

func (s *Server) closeWaitingSessionRun(ctx context.Context, sessionID, runID string) error {
	unlock := s.sessionLock(sessionID)
	defer unlock()
	session, err := s.sessions.Load(ctx, sessionID)
	if err != nil {
		return err
	}
	return s.closeWaitingSessionValue(session, runID, core.RunCancelled, "cancel_requested", context.Canceled)
}

func (s *Server) closeWaitingSessionValue(session *core.Session, runID string, terminal core.RunStatus, errorCode string, cause error) error {
	expectedVersion, changed, err := s.appendWaitingSessionClosure(session, runID, terminal, errorCode, cause)
	if err != nil || !changed {
		return err
	}
	persistCtx, cancel := context.WithTimeout(context.Background(), terminalPersistenceTimeout)
	defer cancel()
	return s.sessions.Save(persistCtx, session, expectedVersion)
}

// closeWaitingSessionValueFenced closes a queued approval pause without
// escaping its claim/session-lease fence. A writer created after repair is
// preferred so the terminal suffix shares its normal batching state; early
// preparation failures use the same FencedSessionAppender directly.
func (s *Server) closeWaitingSessionValueFenced(ctx context.Context, session *core.Session, fence *storage.SessionWriteFence, writer *storage.WriteBehind, runID string, terminal core.RunStatus, errorCode string, cause error) error {
	expectedVersion, changed, err := s.appendWaitingSessionClosure(session, runID, terminal, errorCode, cause)
	if err != nil || !changed {
		return err
	}
	persistCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), terminalPersistenceTimeout)
	defer cancel()
	if writer != nil {
		writer.MarkDirty()
		return writer.Flush(persistCtx)
	}
	if fence == nil {
		return fmt.Errorf("fenced waiting-session close is missing a session write fence")
	}
	appender, ok := s.sessions.(storage.FencedSessionAppender)
	if !ok {
		return fmt.Errorf("queued session store %T does not implement fenced append", s.sessions)
	}
	return appender.AppendEventsFenced(persistCtx, *fence, expectedVersion, session.EventsFrom(expectedVersion))
}

func (s *Server) appendWaitingSessionClosure(session *core.Session, runID string, terminal core.RunStatus, errorCode string, cause error) (expectedVersion int64, changed bool, err error) {
	if session == nil {
		return 0, false, fmt.Errorf("waiting-session close requires a session")
	}
	status, exists := session.RunStatus(runID)
	if !exists || status != core.RunWaitingApproval {
		return session.Version(), false, nil
	}
	expectedVersion = session.Version()
	if pending, ok, err := session.PendingApproval(runID); err != nil {
		return 0, false, err
	} else if ok {
		decision := core.ApprovalDenied
		resolvedAt := time.Now().UTC()
		resolvedBy := "system:" + errorCode
		if s.approvals != nil {
			if approval, approvalErr := s.approvals.GetApproval(context.Background(), pending.ApprovalID); approvalErr == nil {
				decision = approval.Status
				if !approval.DecidedAt.IsZero() {
					resolvedAt = approval.DecidedAt
				}
				if approval.DecidedBy != "" {
					resolvedBy = approval.DecidedBy
				}
			}
		}
		if _, err := session.Append(runID, core.EvApprovalResolved, core.ApprovalResolvedData{
			ApprovalID: pending.ApprovalID, CallID: pending.ToolCall.ID,
			Decision: decision, ResolvedAt: resolvedAt, ResolvedBy: resolvedBy,
		}); err != nil {
			return 0, false, err
		}
	}
	if _, err := session.Append(runID, core.EvRunError, core.NewRuntimeErrorData(errorCode, cause, false)); err != nil {
		return 0, false, err
	}
	if _, err := session.Append(runID, core.EvRunEnd, core.RunEndData{Status: terminal}); err != nil {
		return 0, false, err
	}
	return expectedVersion, true, nil
}

func (s *Server) recordRunStat(ctx context.Context, session *core.Session, runID string, principal core.Principal, status core.RunStatus, started time.Time) {
	if s.runStats == nil {
		return
	}
	usage, err := sumRunUsageEvents(session.Events(), runID)
	if err != nil {
		if s.logger != nil {
			s.logger.ErrorContext(ctx, "run usage total overflow", slog.String("run", runID))
		}
		return
	}
	stat := storage.RunStat{
		RunID: runID, SessionID: session.ID(), TenantID: principal.TenantID,
		Status: string(status), InputTokens: usage.InputTokens, OutputTokens: usage.OutputTokens,
		DurationMS: time.Since(started).Milliseconds(),
	}
	if err := s.runStats.RecordRunStat(ctx, stat); err != nil && s.logger != nil {
		s.logger.ErrorContext(ctx, "run stat record failed", slog.String("run", runID), slog.String("error", err.Error()))
	}
}

// sumRunUsageEvents preserves legacy additive accounting while refusing to
// emit a wrapped metric when malformed historical input would overflow int64.
// Session append and restore already reject new overflowed histories; this is
// a final defensive boundary before an external stats projection is written.
func sumRunUsageEvents(events []core.SessionEvent, runID string) (core.TokenUsage, error) {
	total := core.TokenUsage{}
	const maxInt64 = int64(^uint64(0) >> 1)
	for _, event := range events {
		if event.Type == core.EvRunUsage && event.RunID == runID {
			var usage core.RunUsageData
			if json.Unmarshal(event.Data, &usage) == nil {
				if usage.InputTokens > maxInt64-total.InputTokens || usage.OutputTokens > maxInt64-total.OutputTokens {
					return core.TokenUsage{}, fmt.Errorf("run usage total overflows")
				}
				total.InputTokens += usage.InputTokens
				total.OutputTokens += usage.OutputTokens
			}
		}
	}
	return total, nil
}
