package server

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/whhhh1500/auto-agent/pkg/control"
	core "github.com/whhhh1500/auto-agent/pkg/core"
)

// handleListProfiles returns the profile catalog visible to the caller.
func (s *Server) handleListProfiles(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !s.ensureControlPlane(w, r) {
		return
	}
	profiles, err := s.runtime.Profiles.ListProfiles(principal.Scope)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"profiles": profiles})
}

// handleListCapabilities returns the capability catalog visible to the caller
// at its own scope, including each capability's source scope.
func (s *Server) handleListCapabilities(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !s.ensureControlPlane(w, r) {
		return
	}
	entries, err := s.runtime.Capabilities.Entries(principal.Scope)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"capabilities": entries})
}

// canMutateScope permits writes only at the principal's own ownership scope or
// below it. Administrator principals must therefore be mapped to the global or
// tenant scope they manage; an end user cannot mutate inherited ancestors.
func canMutateScope(principal core.Principal, scope core.ScopePath) bool {
	return principal.Scope.IsAncestorOf(scope)
}

// canReadScope permits catalog inspection along the caller's own chain, but
// never across sibling tenants or users.
func canReadScope(principal core.Principal, scope core.ScopePath) bool {
	return principal.Scope.IsAncestorOf(scope) || scope.IsAncestorOf(principal.Scope)
}

// handlePublishProfile mounts one profile layer at one scope as a new
// release version. The caller must own the target scope.
func (s *Server) handlePublishProfile(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if s.Releases == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "release surface is not configured"})
		return
	}
	if !s.ensureControlPlane(w, r) {
		return
	}
	var request struct {
		Scope          core.ScopePath                `json:"scope"`
		Layer          core.AgentProfileLayer        `json:"layer"`
		EvaluationGate *releaseEvaluationGateRequest `json:"evaluation_gate,omitempty"`
	}
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
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "principal does not own the publish scope"})
		return
	}
	var gateEvaluation *releaseGateEvaluation
	if request.EvaluationGate != nil {
		var err error
		gateEvaluation, err = s.evaluateReleaseCandidate(r.Context(), principal, request.Scope, request.Layer, *request.EvaluationGate)
		if err != nil {
			status := http.StatusBadRequest
			if errors.Is(err, errReleaseGateUnavailable) {
				status = http.StatusNotImplemented
			}
			writeJSON(w, status, map[string]string{"error": err.Error()})
			return
		}
		if !gateEvaluation.Gate.Passed {
			s.recordAudit(r, principal, "profile.publish.rejected", request.Layer.ProfileID, map[string]any{
				"scope": request.Scope.String(), "candidate_revision": gateEvaluation.CandidateRevision,
				"base_release_revision": gateEvaluation.BaseReleaseRevision,
				"evaluation_run_id":     gateEvaluation.CandidateRun.ID, "gate": gateEvaluation.Gate,
			})
			writeJSON(w, http.StatusConflict, gateEvaluation)
			return
		}
	}
	var release control.ReleaseInfo
	var err error
	if gateEvaluation != nil {
		release, err = s.Releases.PublishAtBaseline(
			r.Context(), request.Scope, request.Layer, gateEvaluation.BaseReleaseRevision,
		)
	} else {
		release, err = s.Releases.Publish(r.Context(), request.Scope, request.Layer)
	}
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, control.ErrReleaseBaselineDrift) || errors.Is(err, control.ErrReleaseReserved) {
			status = http.StatusConflict
		}
		writeJSON(w, status, map[string]string{"error": err.Error()})
		return
	}
	auditDetail := map[string]any{"version": release.Version, "scope": release.Scope.String(), "revision": release.Revision}
	if gateEvaluation != nil {
		auditDetail["evaluation_run_id"] = gateEvaluation.CandidateRun.ID
		auditDetail["gate_passed"] = gateEvaluation.Gate.Passed
		auditDetail["base_release_revision"] = gateEvaluation.BaseReleaseRevision
		auditDetail["capability_compatibility"] = gateEvaluation.Gate.CapabilityCompatibility
	}
	s.recordAudit(r, principal, "profile.publish", release.ProfileID, auditDetail)
	if gateEvaluation != nil {
		writeJSON(w, http.StatusCreated, map[string]any{"release": release, "evaluation": gateEvaluation})
		return
	}
	writeJSON(w, http.StatusCreated, release)
}

func (s *Server) handleReleaseHistory(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if s.Releases == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "release surface is not configured"})
		return
	}
	if !s.ensureControlPlane(w, r) {
		return
	}
	visible := []control.ReleaseInfo{}
	for _, release := range s.Releases.History(r.Context(), r.PathValue("id")) {
		if canReadScope(principal, release.Scope) {
			visible = append(visible, release)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"releases": visible})
}

func (s *Server) handleGetRelease(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if s.Releases == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "release surface is not configured"})
		return
	}
	if !s.ensureControlPlane(w, r) {
		return
	}
	version, err := strconv.Atoi(r.PathValue("version"))
	if err != nil || version < 1 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "release version must be positive"})
		return
	}
	profileID := r.PathValue("id")
	for _, release := range s.Releases.History(r.Context(), profileID) {
		if release.Version != version {
			continue
		}
		if !canReadScope(principal, release.Scope) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "release not found"})
			return
		}
		writeJSON(w, http.StatusOK, release)
		return
	}
	writeJSON(w, http.StatusNotFound, map[string]string{"error": "release not found"})
}

func (s *Server) handleRollbackProfile(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if s.Releases == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "release surface is not configured"})
		return
	}
	if !s.ensureControlPlane(w, r) {
		return
	}
	profileID := r.PathValue("id")
	var request struct {
		ToVersion int `json:"to_version"`
	}
	if !s.decodeJSON(w, r, &request) {
		return
	}
	for _, release := range s.Releases.History(r.Context(), profileID) {
		if release.Version > request.ToVersion && !release.RolledBack && !canMutateScope(principal, release.Scope) {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "principal does not own the release scope"})
			return
		}
	}
	rolledBack, err := s.Releases.Rollback(r.Context(), profileID, request.ToVersion)
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, control.ErrReleaseReserved) {
			status = http.StatusConflict
		}
		writeJSON(w, status, map[string]string{"error": err.Error()})
		return
	}
	s.recordAudit(r, principal, "profile.rollback", profileID, map[string]any{"to_version": request.ToVersion, "count": len(rolledBack)})
	writeJSON(w, http.StatusOK, map[string]any{"rolled_back": rolledBack})
}

func (s *Server) handleProfileCapabilities(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !s.ensureControlPlane(w, r) {
		return
	}
	profile, err := s.runtime.Profiles.Resolve(principal, principal.Scope, r.PathValue("id"))
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	capabilities, err := (core.CapabilityResolver{Registry: s.runtime.Capabilities}).Resolve(principal, principal.Scope)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	capabilities, err = profile.FilterCapabilities(capabilities)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"profile": profile, "capability_snapshot_id": capabilities.ID,
		"capabilities": capabilities.Capabilities(),
	})
}
