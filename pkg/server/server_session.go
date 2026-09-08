package server

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/whhhh1500/auto-agent/pkg/storage"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

func (s *Server) handleCreateSession(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !s.ensureControlPlane(w, r) {
		return
	}
	var request struct {
		ProfileID string            `json:"profile_id"`
		Metadata  map[string]string `json:"metadata,omitempty"`
	}
	if !s.decodeJSON(w, r, &request) {
		return
	}
	profileID := request.ProfileID
	if profileID == "" {
		profileID = s.defaultProfileID
	}
	if profileID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "profile_id is required"})
		return
	}
	sessionID, err := core.NewID("sess_")
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	scope, err := principal.Scope.Child(core.ScopeRef{Kind: core.ScopeSession, ID: sessionID})
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if _, err := s.runtime.Profiles.Resolve(principal, scope, profileID); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	session, err := core.NewSession(core.SessionOptions{
		ID: sessionID, ProfileID: profileID, Principal: principal, Scope: scope, Metadata: request.Metadata,
	})
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if err := s.sessions.Create(r.Context(), session); err != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusCreated, sessionResponse(session))
}

func (s *Server) handleGetSession(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	session, ok := s.loadOwnedSession(w, r, principal)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, sessionResponse(session))
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	session, ok := s.loadOwnedSession(w, r, principal)
	if !ok {
		return
	}
	afterSeq := int64(-1)
	if raw := r.URL.Query().Get("after_seq"); raw != "" {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || value < -1 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "after_seq must be an integer >= -1"})
			return
		}
		afterSeq = value
	}
	limit := 500
	if raw := r.URL.Query().Get("limit"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value <= 0 || value > 2000 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "limit must be between 1 and 2000"})
			return
		}
		limit = value
	}
	events, hasMore := paginateSessionEvents(session, afterSeq, limit)
	nextAfter := afterSeq
	if len(events) > 0 {
		nextAfter = events[len(events)-1].Seq
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"session_id": session.ID(), "version": session.Version(), "events": events,
		"next_after_seq": nextAfter, "has_more": hasMore,
	})
}

func paginateSessionEvents(session *core.Session, afterSeq int64, limit int) ([]core.SessionEvent, bool) {
	start := afterSeq + 1
	if start < 0 {
		start = 0
	}
	events := session.EventsFrom(start)
	if len(events) <= limit {
		return events, false
	}
	return events[:limit], true
}

// handleListSessions returns the caller's session catalog. The store must
// implement storage.SessionLister; otherwise the route is not available.
func (s *Server) handleListSessions(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	lister, ok := s.sessions.(storage.SessionLister)
	if !ok {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "session store does not support listing"})
		return
	}
	limit := 50
	if raw := r.URL.Query().Get("limit"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 && parsed <= 500 {
			limit = parsed
		}
	}
	// Rich filters when the store supports them; plain listing otherwise.
	querier, rich := s.sessions.(storage.SessionQuerier)
	if rich {
		offset := 0
		if raw := r.URL.Query().Get("offset"); raw != "" {
			if parsed, err := strconv.Atoi(raw); err == nil && parsed >= 0 {
				offset = parsed
			}
		}
		filter := storage.SessionFilter{
			TenantID:  principal.TenantID,
			UserID:    principal.SubjectID,
			ProfileID: r.URL.Query().Get("profile"),
			Status:    r.URL.Query().Get("status"),
			IDPrefix:  r.URL.Query().Get("prefix"),
			Sort:      r.URL.Query().Get("sort"),
			Limit:     limit,
			Offset:    offset,
		}
		sessions, total, err := querier.QuerySessions(r.Context(), filter)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"sessions": sessions, "total": total, "limit": limit, "offset": offset})
		return
	}
	sessions, err := lister.ListSessions(r.Context(), principal.TenantID, principal.SubjectID, limit)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"sessions": sessions})
}

func (s *Server) loadOwnedSession(w http.ResponseWriter, r *http.Request, principal core.Principal) (*core.Session, bool) {
	session, err := s.sessions.Load(r.Context(), r.PathValue("id"))
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, core.ErrSessionNotFound) {
			status = http.StatusNotFound
		}
		writeJSON(w, status, map[string]string{"error": err.Error()})
		return nil, false
	}
	owner := session.Principal()
	if owner.SubjectID != principal.SubjectID || owner.TenantID != principal.TenantID || !owner.Scope.Equal(principal.Scope) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "session not found"})
		return nil, false
	}
	return session, true
}

// sessionLock acquires the reference-counted per-session mutex and returns
// the release function.
func (s *Server) sessionLock(id string) (unlock func()) {
	mutex, release := s.locks.Acquire(id)
	mutex.Lock()
	return func() {
		mutex.Unlock()
		release()
	}
}

func sessionResponse(session *core.Session) map[string]any {
	return map[string]any{
		"id": session.ID(), "profile_id": session.ProfileID(), "scope": session.Scope(),
		"version": session.Version(), "metadata": session.Metadata(),
	}
}
