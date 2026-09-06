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
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if found && profilePayloadEqual(existing.Payload, payload) {
		s.writeAdminProfileResponse(w, principal, profileID, request.Scope, request.Layer)
		return
	}

	unmountNew, err := s.runtime.Profiles.Mount(request.Layer)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	var oldBinding *adminBinding
	var oldLayer core.AgentProfileLayer
	if found {
		var oldPayload profileBindingPayload
		if err := json.Unmarshal(existing.Payload, &oldPayload); err != nil {
			unmountNew()
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "stored profile layer is invalid"})
			return
		}
		oldLayer = oldPayload.Layer
		oldLayer.Scope = request.Scope
		if binding, exists := s.adminStateFor().remove(existing.ID); exists {
			oldBinding = binding
			if binding.unmount != nil {
				binding.unmount()
			}
		}
		if err := s.journal.Delete(r.Context(), existing.ID); err != nil {
			unmountNew()
			if oldBinding != nil {
				if restoredUnmount, restoreErr := s.runtime.Profiles.Mount(oldLayer); restoreErr == nil {
					oldBinding.unmount = restoredUnmount
					_, _ = s.adminStateFor().add(oldBinding.Kind, existing.ID, oldBinding.Summary, request.Scope, restoredUnmount)
				}
			}
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
	}
	id := profileBindingID(profileID, request.Scope)
	if err := s.journal.Record(r.Context(), storage.BindingRecord{ID: id, Kind: profileBindingKind, Payload: encoded}); err != nil {
		unmountNew()
		s.restoreProfileBinding(r.Context(), existing, found, oldLayer, oldBinding)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if _, err := s.adminStateFor().add(profileBindingKind, id, payload, request.Scope, unmountNew); err != nil {
		_ = s.journal.Delete(r.Context(), id)
		unmountNew()
		s.restoreProfileBinding(r.Context(), existing, found, oldLayer, oldBinding)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	s.recordAudit(r, principal, "profile.put", profileID, map[string]any{"scope": request.Scope.String(), "binding_id": id})
	s.writeAdminProfileResponse(w, principal, profileID, request.Scope, request.Layer)
}

func (s *Server) restoreProfileBinding(ctx context.Context, existing storage.BindingRecord, found bool, oldLayer core.AgentProfileLayer, oldBinding *adminBinding) {
	if !found {
		return
	}
	unmount, err := s.runtime.Profiles.Mount(oldLayer)
	if err != nil {
		return
	}
	_ = s.journal.Record(ctx, existing)
	if oldBinding != nil {
		oldBinding.unmount = unmount
		_, _ = s.adminStateFor().add(oldBinding.Kind, existing.ID, oldBinding.Summary, oldBinding.Scope, unmount)
	}
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
	for _, record := range records {
		if record.Kind != profileBindingKind {
			continue
		}
		var payload profileBindingPayload
		if err := json.Unmarshal(record.Payload, &payload); err != nil {
			return storage.BindingRecord{}, false, fmt.Errorf("decode profile binding %s: %w", record.ID, err)
		}
		if payload.Layer.ProfileID == profileID && payload.Scope == scope.String() {
			return record, true, nil
		}
	}
	return storage.BindingRecord{}, false, nil
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
