package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/cc-auto-agent/harness-core/pkg/control"
	"github.com/cc-auto-agent/harness-core/pkg/storage"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

func (s *Server) handleRun(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	unlock := s.sessionLock(r.PathValue("id"))
	defer unlock()

	session, ok := s.loadOwnedSession(w, r, principal)
	if !ok {
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
	runCtx, cancel := context.WithCancel(r.Context())
	defer cancel()
	if err := s.registerRun(session.ID(), runID, cancel); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
		return
	}
	defer s.clearRun(session.ID(), runID)

	// Cross-instance gate: only one deployment works a session at a time.
	if s.leaser != nil {
		releaseLease, acquired, err := s.acquireRunLease(runCtx, cancel, session.ID(), runID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		if !acquired {
			writeJSON(w, http.StatusConflict, map[string]string{"error": "session is busy on another instance"})
			return
		}
		defer releaseLease()

		// The ownership check above happened before the cross-instance gate.
		// Reload after acquisition so this run composes from the latest durable
		// session state rather than a snapshot another instance may have
		// advanced while we were waiting for its lease.
		session, ok = s.loadOwnedSession(w, r, principal)
		if !ok {
			return
		}
	}
	session, _, err = storage.RepairInterruptedSession(runCtx, s.sessions, session)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, core.ErrSessionConflict) {
			status = http.StatusConflict
		}
		writeJSON(w, status, map[string]string{"error": err.Error()})
		return
	}
	runRuntime, canary, err := s.runtimeFor(runCtx, principal, session.ProfileID())
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
		return
	}
	if canary != nil && canary.Candidate {
		runCtx = core.WithTelemetrySpanAttributes(runCtx, core.TelemetryAttributes{telemetryCanaryID: canary.ID})
		w.Header().Set("X-Harness-Canary", canary.ID)
		if s.logger != nil {
			s.logger.InfoContext(runCtx, "canary selected", slogString("canary", canary.ID), slogString("profile", canary.ProfileID), slogString("run", runID))
		}
	}
	expectedVersion := session.Version()
	writer := storage.NewWriteBehind(s.sessions, session, expectedVersion, s.maxWriteDelay)
	runRuntime, checkpointFailure := runtimeWithToolCheckpoint(runRuntime, writer, cancel)
	runExecutor, compositionMetadata, err := s.resolveRunExecutor(runCtx, principal, session, runRuntime, canary, runID, false)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
		return
	}
	durableRunCreated := false
	durableRunFinished := false
	stopRunControlMonitor := func() {}
	if s.runControl != nil {
		if err := s.createRunControl(runCtx, runID, session.ID(), principal); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		durableRunCreated = true
		stopRunControlMonitor = s.monitorRunCancel(runCtx, cancel, runID)
		defer stopRunControlMonitor()
		defer func() {
			if durableRunCreated && !durableRunFinished {
				stopRunControlMonitor()
				_ = s.finishRunControl(runID, core.RunFailed, "run_aborted")
			}
		}()
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	flusher, _ := w.(http.Flusher)

	// Write-behind: persist a stable ordered prefix at most maxWriteDelay
	// behind the producer, then flush everything synchronously before the
	// response completes. A crash loses at most one batching window.
	seenObsHits := map[string]bool{}
	emit := func(event core.SessionEvent) {
		// The core guard maps the cancellation used to stop after a checkpoint
		// failure to tool_cancelled. That is an execution detail, not the
		// authoritative terminal cause. Do not expose a contradictory terminal
		// before the final Flush reports store_error below.
		if !checkpointFailure.Failed() || (event.Type != core.EvRunError && event.Type != core.EvRunEnd) {
			writeSSE(w, flusher, string(event.Type), event)
		}
		writer.MarkDirty()
		s.observeEvent(r, seenObsHits, session, event)
	}

	runStart := time.Now()
	runResult, runErr := runExecutor.RunTurn(runCtx, principal, session, core.TurnInput{
		RunID: runID, Text: request.Message, CompositionMetadata: compositionMetadata,
	}, emit)
	status := effectiveRunStatus(runResult, runErr)
	// WithoutCancel retains context values, including the OTel SpanContext,
	// while terminal durability must outlive a client disconnect.
	flushCtx, cancelFlush := context.WithTimeout(context.WithoutCancel(runCtx), terminalPersistenceTimeout)
	defer cancelFlush()
	if flushErr := writer.Flush(flushCtx); flushErr != nil {
		if durableRunCreated {
			stopRunControlMonitor()
			durableRunFinished = true
			if err := s.finishRunControl(runID, core.RunFailed, "store_error"); err != nil && s.logger != nil {
				s.logger.ErrorContext(flushCtx, "finish durable run failed", slogString("run", runID), slogString("error", err.Error()))
			}
		}
		writeSSE(w, flusher, "store/error", map[string]string{
			"error": flushErr.Error(), "code": "store_error", "status": string(core.RunFailed),
		})
		return
	}
	if status == core.RunWaitingApproval {
		stopRunControlMonitor()
		durableRunFinished = true
		opCtx, cancelPause := context.WithTimeout(context.WithoutCancel(r.Context()), runControlOperationTimeout)
		paused, pauseErr := s.runQueue.PauseRun(opCtx, runID, request.Message, s.runWorkerAttempts,
			core.InjectTelemetryTraceContext(s.telemetry, r.Context()))
		cancelPause()
		if pauseErr != nil || !paused {
			if pauseErr == nil {
				pauseErr = fmt.Errorf("run could not enter waiting approval")
			}
			_ = s.finishRunControl(runID, core.RunFailed, "approval_pause_failed")
			writeSSE(w, flusher, "control/error", map[string]string{"error": pauseErr.Error()})
		}
		return
	}
	if durableRunCreated {
		stopRunControlMonitor()
		durableRunFinished = true
		if err := s.finishRunControl(runID, status, runTerminalErrorCode(session, runID)); err != nil {
			if s.logger != nil {
				s.logger.ErrorContext(context.WithoutCancel(runCtx), "finish durable run failed", slogString("run", runID), slogString("error", err.Error()))
			}
			writeSSE(w, flusher, "control/error", map[string]string{"error": err.Error()})
		}
	}
	if s.runStats != nil {
		s.recordRunStat(context.WithoutCancel(runCtx), session, runID, principal, status, runStart)
	}
	if runErr != nil {
		return // run/error and run/end were already emitted and persisted.
	}
}

func (s *Server) runtimeFor(ctx context.Context, principal core.Principal, profileID string) (*core.Runtime, *control.CanaryAssignment, error) {
	if err := s.refreshControlPlane(ctx); err != nil {
		return nil, nil, err
	}
	if s.canaries == nil {
		return s.runtime, nil, nil
	}
	profiles, assignment := s.canaries.Assign(principal, profileID)
	if assignment == nil {
		return s.runtime, nil, nil
	}
	runtime := *s.runtime
	runtime.Profiles = profiles
	return &runtime, assignment, nil
}

func canaryCompositionMetadata(assignment *control.CanaryAssignment) map[string]string {
	if assignment == nil {
		return nil
	}
	variant := storage.AssignmentLive
	if assignment.Candidate {
		variant = storage.AssignmentCandidate
	}
	return map[string]string{
		compositionCanaryID:           assignment.ID,
		compositionCanaryRevision:     assignment.Revision,
		compositionCanaryBaseRevision: assignment.BaseReleaseRevision,
		compositionCanaryBasisPoints:  strconv.Itoa(assignment.BasisPoints),
		compositionCanaryBucket:       strconv.Itoa(assignment.Bucket),
		compositionCanaryStatus:       string(assignment.Status),
		compositionCanaryCandidate:    strconv.FormatBool(assignment.Candidate),
		compositionAssignmentVariant:  string(variant),
	}
}

func (s *Server) refreshControlPlane(ctx context.Context) error {
	if s.canaries != nil {
		return s.canaries.Refresh(ctx)
	}
	if s.Releases != nil {
		return s.Releases.Sync(ctx)
	}
	return nil
}

func (s *Server) ensureControlPlane(w http.ResponseWriter, r *http.Request) bool {
	if err := s.refreshControlPlane(r.Context()); err != nil {
		status := http.StatusServiceUnavailable
		if errors.Is(err, control.ErrReleaseReserved) || errors.Is(err, control.ErrReleaseBaselineDrift) {
			status = http.StatusConflict
		}
		writeJSON(w, status, map[string]string{"error": err.Error()})
		return false
	}
	return true
}

func effectiveRunStatus(result core.TurnResult, runErr error) core.RunStatus {
	if result.Status != "" {
		return result.Status
	}
	if runErr == nil {
		return core.RunCompleted
	}
	return core.RunFailed
}

// handleCancel aborts the active run on one session. The run observes the
// cancellation through its context and reaches run/error plus run/end.
func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	sessionID := r.PathValue("id")
	if _, ok := s.loadOwnedSession(w, r, principal); !ok {
		return
	}
	s.runsMu.Lock()
	active := s.runs[sessionID]
	s.runsMu.Unlock()
	if s.runControl != nil {
		runID := ""
		waitingApproval := false
		if active != nil {
			runID = active.runID
		} else {
			record, err := s.runControl.FindActiveRun(r.Context(), sessionID)
			if err != nil {
				if errors.Is(err, storage.ErrRunNotFound) {
					writeJSON(w, http.StatusConflict, map[string]string{"error": "no active run"})
					return
				}
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
				return
			}
			runID = record.RunID
			waitingApproval = record.Status == storage.RunStatusWaitingApproval
		}
		requested, err := s.runControl.RequestRunCancel(r.Context(), runID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		if !requested && active == nil {
			writeJSON(w, http.StatusConflict, map[string]string{"error": "run is no longer active"})
			return
		}
		if requested && s.approvals != nil {
			closed, _ := s.approvals.CancelRunApprovals(r.Context(), runID, principal.SubjectID)
			if closed > 0 {
				core.AddTelemetryCounter(s.telemetry, r.Context(), core.MetricApprovalDecisions, closed,
					core.TelemetryAttributes{"approval.decision": string(core.ApprovalDenied), "approval.reason": "run_cancel"})
			}
		}
		if requested && waitingApproval {
			if err := s.closeWaitingSessionRun(r.Context(), sessionID, runID); err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
				return
			}
		}
		if active != nil {
			active.cancel()
		}
		if err := s.cancelDirectDelegations(r.Context(), sessionID, runID, principal.TenantID); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "direct child cancellation failed"})
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]string{"run_id": runID, "status": "cancelling"})
		return
	}
	if active == nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "no active run"})
		return
	}
	active.cancel()
	if err := s.cancelDirectDelegations(r.Context(), sessionID, active.runID, principal.TenantID); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "direct child cancellation failed"})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"run_id": active.runID, "status": "cancelling"})
}

func (s *Server) activeRunCap() int {
	if s != nil && s.maxActiveRuns > 0 {
		return s.maxActiveRuns
	}
	return MaxActiveRuns
}

func (s *Server) registerRun(sessionID, runID string, cancel context.CancelFunc) error {
	s.runsMu.Lock()
	defer s.runsMu.Unlock()
	if _, exists := s.runs[sessionID]; !exists && len(s.runs) >= s.activeRunCap() {
		return fmt.Errorf("active runs exceed maximum of %d", s.activeRunCap())
	}
	s.runs[sessionID] = &activeRun{runID: runID, cancel: cancel}
	return nil
}

func (s *Server) clearRun(sessionID, runID string) {
	s.runsMu.Lock()
	defer s.runsMu.Unlock()
	if active := s.runs[sessionID]; active != nil && active.runID == runID {
		delete(s.runs, sessionID)
	}
}
