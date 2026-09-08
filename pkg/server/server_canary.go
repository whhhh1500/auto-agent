package server

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/whhhh1500/auto-agent/pkg/control"
	core "github.com/whhhh1500/auto-agent/pkg/core"
)

type stageCanaryRequest struct {
	Scope          core.ScopePath                `json:"scope"`
	Layer          core.AgentProfileLayer        `json:"layer"`
	BasisPoints    int                           `json:"basis_points"`
	EvaluationGate *releaseEvaluationGateRequest `json:"evaluation_gate"`
}

func (s *Server) handleStageCanary(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if s.canaries == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "canary surface is not configured"})
		return
	}
	if !s.ensureControlPlane(w, r) {
		return
	}
	var request stageCanaryRequest
	if !s.decodeJSON(w, r, &request) {
		return
	}
	profileID := r.PathValue("id")
	if request.Layer.ProfileID == "" {
		request.Layer.ProfileID = profileID
	} else if request.Layer.ProfileID != profileID {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "layer profile_id must match the route profile id"})
		return
	}
	if !canMutateScope(principal, request.Scope) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "principal does not own the canary scope"})
		return
	}
	if request.BasisPoints < 1 || request.BasisPoints > 10000 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "basis_points must be between 1 and 10000"})
		return
	}
	if request.EvaluationGate == nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "evaluation_gate is required"})
		return
	}
	evaluated, err := s.evaluateReleaseCandidate(r.Context(), principal, request.Scope, request.Layer, *request.EvaluationGate)
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, errReleaseGateUnavailable) {
			status = http.StatusNotImplemented
		}
		writeJSON(w, status, map[string]string{"error": err.Error()})
		return
	}
	if !evaluated.Gate.Passed {
		s.recordAudit(r, principal, "profile.canary.rejected", request.Layer.ProfileID, map[string]any{
			"scope": request.Scope.String(), "candidate_revision": evaluated.CandidateRevision,
			"base_release_revision": evaluated.BaseReleaseRevision,
			"evaluation_run_id":     evaluated.CandidateRun.ID, "gate": evaluated.Gate,
		})
		writeJSON(w, http.StatusConflict, evaluated)
		return
	}
	layer := core.CloneAgentProfileLayer(request.Layer)
	layer.Scope = request.Scope
	baselineRunID := request.EvaluationGate.BaselineRunID
	if evaluated.BaselineRun != nil {
		baselineRunID = evaluated.BaselineRun.ID
	}
	record, err := s.canaries.Stage(r.Context(), control.CanaryRecord{
		ProfileID: profileID, Scope: request.Scope, Layer: &layer,
		Revision: evaluated.CandidateRevision, BaseReleaseRevision: evaluated.BaseReleaseRevision,
		BasisPoints:             request.BasisPoints,
		BaselineEvaluationRunID: baselineRunID, CandidateEvaluationRunID: evaluated.CandidateRun.ID,
		Gate: evaluated.Gate,
	})
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, core.ErrSessionConflict) || errors.Is(err, control.ErrReleaseBaselineDrift) ||
			errors.Is(err, control.ErrReleaseReserved) || strings.Contains(err.Error(), "already has active canary") {
			status = http.StatusConflict
		}
		writeJSON(w, status, map[string]string{"error": err.Error()})
		return
	}
	s.recordAudit(r, principal, "profile.canary.stage", record.ID, map[string]any{
		"profile_id": record.ProfileID, "scope": record.Scope.String(), "revision": record.Revision,
		"base_release_revision": record.BaseReleaseRevision, "basis_points": record.BasisPoints,
		"evaluation_run_id": record.CandidateEvaluationRunID,
	})
	writeJSON(w, http.StatusCreated, map[string]any{"canary": record, "evaluation": evaluated})
}

func (s *Server) handleListCanaries(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if s.canaries == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "canary surface is not configured"})
		return
	}
	if !s.ensureControlPlane(w, r) {
		return
	}
	records, err := s.canaries.List(r.Context(), r.PathValue("id"), 100)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	visible := make([]control.CanaryRecord, 0, len(records))
	for _, record := range records {
		if canReadScope(principal, record.Scope) {
			visible = append(visible, record)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"canaries": visible})
}

func (s *Server) handleGetCanary(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if s.canaries == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "canary surface is not configured"})
		return
	}
	if !s.ensureControlPlane(w, r) {
		return
	}
	record, err := s.canaries.Get(r.Context(), r.PathValue("canaryID"))
	if err != nil || record.ProfileID != r.PathValue("id") || !canReadScope(principal, record.Scope) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "canary not found"})
		return
	}
	writeJSON(w, http.StatusOK, record)
}

func (s *Server) handleCanaryPercentage(w http.ResponseWriter, r *http.Request) {
	principal, record, ok := s.ownedCanary(w, r)
	if !ok {
		return
	}
	var request struct {
		BasisPoints int `json:"basis_points"`
	}
	if !s.decodeJSON(w, r, &request) {
		return
	}
	updated, err := s.canaries.SetBasisPoints(r.Context(), record.ID, request.BasisPoints)
	if err != nil {
		writeCanaryMutationError(w, err)
		return
	}
	s.recordAudit(r, principal, "profile.canary.percentage", record.ID, map[string]any{"basis_points": updated.BasisPoints})
	writeJSON(w, http.StatusOK, updated)
}

func (s *Server) handlePauseCanary(w http.ResponseWriter, r *http.Request) {
	s.mutateCanary(w, r, "profile.canary.pause", (*control.CanaryManager).Pause)
}

func (s *Server) handleResumeCanary(w http.ResponseWriter, r *http.Request) {
	s.mutateCanary(w, r, "profile.canary.resume", (*control.CanaryManager).Resume)
}

func (s *Server) handleRollbackCanary(w http.ResponseWriter, r *http.Request) {
	s.mutateCanary(w, r, "profile.canary.rollback", (*control.CanaryManager).Rollback)
}

func (s *Server) handlePromoteCanary(w http.ResponseWriter, r *http.Request) {
	s.mutateCanary(w, r, "profile.canary.promote", (*control.CanaryManager).Promote)
}

type canaryMutation func(*control.CanaryManager, context.Context, string) (control.CanaryRecord, error)

func (s *Server) mutateCanary(w http.ResponseWriter, r *http.Request, action string, mutate canaryMutation) {
	principal, record, ok := s.ownedCanary(w, r)
	if !ok {
		return
	}
	updated, err := mutate(s.canaries, r.Context(), record.ID)
	if err != nil {
		writeCanaryMutationError(w, err)
		return
	}
	s.recordAudit(r, principal, action, record.ID, map[string]any{
		"profile_id": record.ProfileID, "scope": record.Scope.String(), "status": updated.Status,
		"release_version": updated.ReleaseVersion,
	})
	writeJSON(w, http.StatusOK, updated)
}

func (s *Server) ownedCanary(w http.ResponseWriter, r *http.Request) (core.Principal, control.CanaryRecord, bool) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return core.Principal{}, control.CanaryRecord{}, false
	}
	if s.canaries == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "canary surface is not configured"})
		return core.Principal{}, control.CanaryRecord{}, false
	}
	if !s.ensureControlPlane(w, r) {
		return core.Principal{}, control.CanaryRecord{}, false
	}
	record, err := s.canaries.Get(r.Context(), r.PathValue("canaryID"))
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, control.ErrCanaryNotFound) {
			status = http.StatusNotFound
		}
		writeJSON(w, status, map[string]string{"error": err.Error()})
		return core.Principal{}, control.CanaryRecord{}, false
	}
	if record.ProfileID != r.PathValue("id") {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "canary not found"})
		return core.Principal{}, control.CanaryRecord{}, false
	}
	if !canMutateScope(principal, record.Scope) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "principal does not own the canary scope"})
		return core.Principal{}, control.CanaryRecord{}, false
	}
	return principal, record, true
}

func writeCanaryMutationError(w http.ResponseWriter, err error) {
	status := http.StatusConflict
	if errors.Is(err, control.ErrCanaryNotFound) {
		status = http.StatusNotFound
	} else if strings.Contains(err.Error(), "basis points") {
		status = http.StatusBadRequest
	}
	writeJSON(w, status, map[string]string{"error": err.Error()})
}
