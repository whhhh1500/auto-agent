package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
)

var runControlOperationTimeout = 5 * time.Second

func (s *Server) createRunControl(ctx context.Context, runID, sessionID string, principal core.Principal) error {
	if s.runControl == nil {
		return nil
	}
	opCtx, cancel := context.WithTimeout(ctx, runControlOperationTimeout)
	defer cancel()
	return s.runControl.CreateRun(opCtx, storage.RunRecord{
		RunID: runID, SessionID: sessionID, TenantID: principal.TenantID,
		SubjectID: principal.SubjectID, Status: storage.RunStatusRunning,
	})
}

// monitorRunCancel polls the durable flag so a cancel request accepted by any
// server instance reaches the worker currently executing the run.
func (s *Server) monitorRunCancel(runCtx context.Context, cancelRun context.CancelFunc, runID string) func() {
	if s.runControl == nil || s.liveness == nil {
		return func() {}
	}
	stop, err := s.liveness.Register(runCtx, "run-cancel:"+runID, s.runCancelPoll, func(ctx context.Context) error {
		opCtx, cancel := context.WithTimeout(ctx, runControlOperationTimeout)
		err := s.runControl.HeartbeatRun(opCtx, runID)
		requested := false
		if err == nil {
			requested, err = s.runControl.RunCancelRequested(opCtx, runID)
		}
		cancel()
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			if s.logger != nil {
				s.logger.ErrorContext(ctx, "run cancel polling failed", slog.String("run", runID), slog.String("error", err.Error()))
			}
			return fmt.Errorf("run cancel monitoring failed")
		}
		if requested {
			cancelRun()
		}
		return nil
	}, func() { cancelRun() })
	if err != nil {
		cancelRun()
		return func() {}
	}
	return stop
}

// RecoverStaleRuns marks running records without a recent worker heartbeat as
// failed. Call it during startup before accepting traffic.
func (s *Server) RecoverStaleRuns(ctx context.Context) error {
	if s.runControl == nil {
		return nil
	}
	opCtx, cancel := context.WithTimeout(ctx, runControlOperationTimeout)
	defer cancel()
	if s.runQueue != nil {
		requeued, failed, err := s.runQueue.RecoverExpiredRunClaims(opCtx, time.Now().UTC())
		if err != nil {
			return err
		}
		if (requeued > 0 || failed > 0) && s.logger != nil {
			s.logger.Warn("recovered expired run claims", slog.Int64("requeued", requeued), slog.Int64("failed", failed))
		}
	}
	count, err := s.runControl.FailStaleRuns(opCtx, time.Now().UTC().Add(-s.runStaleAfter))
	if err != nil {
		return err
	}
	if count > 0 && s.logger != nil {
		s.logger.Warn("recovered stale runs", slog.Int64("count", count))
	}
	return nil
}

func (s *Server) finishRunControl(runID string, status core.RunStatus, errorCode string) error {
	if s.runControl == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), runControlOperationTimeout)
	defer cancel()
	return s.runControl.FinishRun(ctx, runID, status, errorCode)
}

func runTerminalErrorCode(session *core.Session, runID string) string {
	events := session.Events()
	for index := len(events) - 1; index >= 0; index-- {
		event := events[index]
		if event.RunID != runID || event.Type != core.EvRunError {
			continue
		}
		var data core.RuntimeErrorData
		if json.Unmarshal(event.Data, &data) == nil {
			return data.Code
		}
	}
	return ""
}

func (s *Server) handleGetRun(w http.ResponseWriter, r *http.Request) {
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
	record, err := s.runControl.GetRun(r.Context(), r.PathValue("runID"))
	if err != nil || record.SessionID != session.ID() {
		status := http.StatusInternalServerError
		if errors.Is(err, storage.ErrRunNotFound) || record.SessionID != session.ID() {
			status = http.StatusNotFound
		}
		message := "run not found"
		if status == http.StatusInternalServerError {
			message = err.Error()
		}
		writeJSON(w, status, map[string]string{"error": message})
		return
	}
	writeJSON(w, http.StatusOK, record)
}

func (s *Server) handleListRuns(w http.ResponseWriter, r *http.Request) {
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
	limit := 50
	if raw := r.URL.Query().Get("limit"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 && parsed <= 500 {
			limit = parsed
		} else {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "limit must be between 1 and 500"})
			return
		}
	}
	runs, err := s.runControl.ListRuns(r.Context(), session.ID(), limit)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": runs})
}
