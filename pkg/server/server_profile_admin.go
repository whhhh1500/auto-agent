package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
)

const profileBindingKind = "profile"

type adminProfileWriteRequest struct {
	Scope core.ScopePath         `json:"scope"`
	Layer core.AgentProfileLayer `json:"layer"`
}

type profileBindingPayload struct {
	Scope string                 `json:"scope"`
	Layer core.AgentProfileLayer `json:"layer"`
}

func (s *Server) registerProfileAdminRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/admin/profiles/{id}", s.handleAdminProfileGet)
	mux.HandleFunc("PUT /v1/admin/profiles/{id}", s.handleAdminProfilePut)
}

func profileBindingID(profileID string, scope core.ScopePath) string {
	sum := sha256.Sum256([]byte(profileID + "\x00" + scope.String()))
	return "profile_" + hex.EncodeToString(sum[:])
}

func profileAdminAllowed(principal core.Principal) bool {
	role := roleOf(principal)
	return role == storage.RoleAccountAdmin || role == storage.RoleAccountTenantAdmin
}

func parseProfileScopeQuery(raw string) (core.ScopePath, error) {
	var scope core.ScopePath
	if err := json.Unmarshal([]byte(raw), &scope); err != nil {
		return core.ScopePath{}, fmt.Errorf("scope must be a JSON ScopePath: %w", err)
	}
	return scope, nil
}

func (s *Server) handleAdminProfileGet(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !profileAdminAllowed(principal) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "platform or tenant administrator role required"})
		return
	}
	if !s.ensureControlPlane(w, r) {
		return
	}
	profileID := r.PathValue("id")
	if err := core.ValidateProfileID(profileID); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid profile id"})
		return
	}
	scope, err := parseProfileScopeQuery(r.URL.Query().Get("scope"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if !canMutateScope(principal, scope) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "principal does not own profile scope"})
		return
	}
	if s.runtime == nil || s.runtime.Profiles == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "profile registry is not configured"})
		return
	}
	effective, err := s.runtime.Profiles.Resolve(principal, scope, profileID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "profile not found for scope"})
		return
	}
	layer, found, err := s.profileLayerAt(r.Context(), profileID, scope)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if !found {
		// A static profile is readable but has no editable durable layer.
		layer = core.AgentProfileLayer{ProfileID: profileID}
	}
	writeJSON(w, http.StatusOK, map[string]any{"scope": scope, "layer": layer, "effective": effective})
}

func (s *Server) handleAdminProfilePut(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !profileAdminAllowed(principal) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "platform or tenant administrator role required"})
		return
	}
	if !s.ensureControlPlane(w, r) {
		return
	}
	profileID := r.PathValue("id")
	if err := core.ValidateProfileID(profileID); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid profile id"})
		return
	}
	var request adminProfileWriteRequest
	if !s.decodeJSON(w, r, &request) {
		return
	}
	if request.Scope.Depth() == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "scope is required"})
		return
	}
	if !canMutateScope(principal, request.Scope) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "principal does not own profile scope"})
		return
	}
	if request.Layer.ProfileID == "" {
		request.Layer.ProfileID = profileID
	} else if request.Layer.ProfileID != profileID {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "layer profile_id must match path id"})
		return
	}
	if s.journal == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "durable profile journal is required"})
		return
	}
	if s.runtime == nil || s.runtime.Profiles == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "profile registry is not configured"})
		return
	}
	request.Layer.Scope = request.Scope
	payload := profileBindingPayload{Scope: request.Scope.String(), Layer: core.CloneAgentProfileLayer(request.Layer)}
	encoded, err := json.Marshal(payload)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": fmt.Sprintf("encode profile layer: %v", err)})
		return
	}
	if len(encoded) > storage.MaxBindingPayloadBytes {
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "profile layer payload exceeds limit"})
		return
	}

	s.profileMu.Lock()
	defer s.profileMu.Unlock()
	existing, found, err := s.profileLayerRecord(r.Context(), profileID, request.Scope)
	if err != nil {
		s.writeProfileJournalError(w, r.Context(), "list", profileBindingID(profileID, request.Scope), err)
		return
	}
	id := profileBindingID(profileID, request.Scope)
	record := storage.BindingRecord{ID: id, Kind: profileBindingKind, Payload: encoded}

	// Reserve the local admin-state slot across the durable commit and live
	// publication. This serializes only admin bookkeeping; it does not claim to
	// make concurrent profile Resolve calls atomic.
	state := s.adminStateFor()
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.maxBindings <= 0 {
		state.maxBindings = storage.MaxAdminBindings
	}
	var oldBinding *adminBinding
	var oldLayer core.AgentProfileLayer
	if found {
		var exists bool
		oldBinding, exists = state.handles[existing.ID]
		if !exists {
			err := fmt.Errorf("profile projection is missing durable binding %q", existing.ID)
			s.setProfileProjectionError(existing.ID, err)
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "profile projection is unavailable"})
			return
		}
		if existing.ID != id {
			if _, exists := state.handles[id]; exists {
				err := fmt.Errorf("profile projection has conflicting binding %q", id)
				s.setProfileProjectionError(id, err)
				writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "profile projection is unavailable"})
				return
			}
		}
		oldPayload, ok := oldBinding.Summary.(profileBindingPayload)
		if !ok {
			err := fmt.Errorf("profile projection binding %q has invalid local payload", existing.ID)
			s.setProfileProjectionError(existing.ID, err)
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "profile projection is unavailable"})
			return
		}
		oldLayer = core.CloneAgentProfileLayer(oldPayload.Layer)
		oldLayer.Scope = request.Scope
		candidate := s.runtime.Profiles.Clone()
		if err := candidate.ReplaceExact(oldLayer, oldLayer); err != nil {
			s.handleProfileProjectionFailure(w, r.Context(), id, err)
			return
		}
		if err := candidate.ReplaceExact(oldLayer, request.Layer); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		if _, err := candidate.Resolve(principal, request.Scope, profileID); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
	} else {
		candidate := s.runtime.Profiles.Clone()
		candidateUnmount, err := candidate.Mount(request.Layer)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		_, candidateErr := candidate.Resolve(principal, request.Scope, profileID)
		candidateUnmount()
		if candidateErr != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": candidateErr.Error()})
			return
		}
	}
	if !found {
		if _, exists := state.handles[id]; exists {
			err := fmt.Errorf("profile projection has binding %q without a durable record", id)
			s.setProfileProjectionError(id, err)
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "profile projection is unavailable"})
			return
		}
		if len(state.handles) >= state.maxBindings {
			writeJSON(w, http.StatusConflict, map[string]string{"error": fmt.Sprintf("admin bindings exceed maximum of %d", state.maxBindings)})
			return
		}
	}
	durableMatches := found && existing.ID == id && profilePayloadEqual(existing.Payload, payload)
	if !durableMatches {
		if found {
			replacer, ok := s.journal.(storage.BindingJournalReplacer)
			if !ok {
				writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "durable profile replacement requires an atomic binding journal"})
				return
			}
			if err := replacer.Replace(r.Context(), existing.ID, record); err != nil {
				s.writeProfileJournalError(w, r.Context(), "replace", id, err)
				return
			}
		} else if err := s.journal.Record(r.Context(), record); err != nil {
			s.writeProfileJournalError(w, r.Context(), "record", id, err)
			return
		}
	}

	if found {
		if err := s.runtime.Profiles.ReplaceExact(oldLayer, request.Layer); err != nil {
			s.handleProfileProjectionFailure(w, r.Context(), id, err)
			return
		}
		if existing.ID != id {
			delete(state.handles, existing.ID)
			state.handles[id] = oldBinding
		}
		oldBinding.Kind = profileBindingKind
		oldBinding.Summary = payload
		oldBinding.Scope = request.Scope
	} else {
		unmount, err := s.runtime.Profiles.Mount(request.Layer)
		if err != nil {
			s.handleProfileProjectionFailure(w, r.Context(), id, err)
			return
		}
		state.handles[id] = &adminBinding{Kind: profileBindingKind, Summary: payload, Scope: request.Scope, unmount: unmount}
	}
	if _, err := s.runtime.Profiles.Resolve(principal, request.Scope, profileID); err != nil {
		s.handleProfileProjectionFailure(w, r.Context(), id, err)
		return
	}
	s.clearProfileProjectionError(id)
	s.recordAudit(r, principal, "profile.put", profileID, map[string]any{"scope": request.Scope.String(), "binding_id": id})
	s.writeAdminProfileResponse(w, principal, profileID, request.Scope, request.Layer)
}

func (s *Server) writeProfileJournalError(w http.ResponseWriter, ctx context.Context, operation, id string, err error) {
	if s.logger != nil {
		s.logger.ErrorContext(ctx, "durable profile update failed", slogString("operation", operation), slogString("binding", id), slogString("error", err.Error()))
	}
	writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "durable profile update failed"})
}

func (s *Server) handleProfileProjectionFailure(w http.ResponseWriter, ctx context.Context, id string, cause error) {
	err := fmt.Errorf("durable profile binding %q could not publish its live projection: %w", id, cause)
	s.setProfileProjectionError(id, err)
	if s.logger != nil {
		s.logger.ErrorContext(ctx, "profile projection is unavailable", slogString("binding", id), slogString("error", cause.Error()))
	}
	writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "profile projection is unavailable"})
}

func profilePayloadEqual(raw json.RawMessage, want profileBindingPayload) bool {
	var got profileBindingPayload
	if json.Unmarshal(raw, &got) != nil {
		return false
	}
	got.Layer.Scope = want.Layer.Scope
	want.Layer.Scope = got.Layer.Scope
	left, leftErr := json.Marshal(got)
	right, rightErr := json.Marshal(want)
	return leftErr == nil && rightErr == nil && bytes.Equal(left, right)
}

func (s *Server) profileLayerAt(ctx context.Context, profileID string, scope core.ScopePath) (core.AgentProfileLayer, bool, error) {
	record, found, err := s.profileLayerRecord(ctx, profileID, scope)
	if err != nil || !found {
		return core.AgentProfileLayer{}, found, err
	}
	var payload profileBindingPayload
	if err := json.Unmarshal(record.Payload, &payload); err != nil {
		return core.AgentProfileLayer{}, false, err
	}
	payload.Layer.Scope = scope
	return payload.Layer, true, nil
}

func (s *Server) profileLayerRecord(ctx context.Context, profileID string, scope core.ScopePath) (storage.BindingRecord, bool, error) {
	if s.journal == nil {
		return storage.BindingRecord{}, false, nil
	}
	records, err := s.journal.List(ctx)
	if err != nil {
		return storage.BindingRecord{}, false, err
	}
	var match storage.BindingRecord
	for _, record := range records {
		if record.Kind != profileBindingKind {
			continue
		}
		var payload profileBindingPayload
		if err := json.Unmarshal(record.Payload, &payload); err != nil {
			return storage.BindingRecord{}, false, fmt.Errorf("decode profile binding %s: %w", record.ID, err)
		}
		if payload.Layer.ProfileID == profileID && payload.Scope == scope.String() {
			if match.ID != "" {
				return storage.BindingRecord{}, false, fmt.Errorf("multiple durable profile bindings match profile %q at scope %q", profileID, scope.String())
			}
			match = record
		}
	}
	return match, match.ID != "", nil
}

func (s *Server) writeAdminProfileResponse(w http.ResponseWriter, principal core.Principal, profileID string, scope core.ScopePath, layer core.AgentProfileLayer) {
	effective, err := s.runtime.Profiles.Resolve(principal, scope, profileID)
	if err != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	layer.Scope = scope
	writeJSON(w, http.StatusOK, map[string]any{"scope": scope, "layer": layer, "effective": effective})
}
