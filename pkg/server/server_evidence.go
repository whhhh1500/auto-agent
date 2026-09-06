package server

import (
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
)

type evidenceResponseRecord struct {
	storage.EvidenceRecord
	DetailPath string `json:"detail_path,omitempty"`
}

func parseEvidenceQuery(w http.ResponseWriter, r *http.Request, principal core.Principal, pagination bool) (storage.EvidenceQuery, bool) {
	values := r.URL.Query()
	if !pagination && values.Get("cursor") != "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "cursor is not valid for evidence statistics"})
		return storage.EvidenceQuery{}, false
	}
	query := storage.EvidenceQuery{
		CompositionRevision: values.Get("composition_revision"),
		AssignmentRevision:  values.Get("assignment_revision"),
		SubjectID:           values.Get("subject_id"),
		ProfileID:           values.Get("profile_id"),
	}
	if pagination {
		query.Cursor = values.Get("cursor")
		query.Limit = 100
	}
	for _, raw := range values["kind"] {
		for _, value := range strings.Split(raw, ",") {
			value = strings.TrimSpace(value)
			if value != "" {
				query.Kinds = append(query.Kinds, storage.EvidenceKind(value))
			}
		}
	}
	for _, raw := range values["status"] {
		for _, value := range strings.Split(raw, ",") {
			value = strings.TrimSpace(value)
			if value != "" {
				query.Statuses = append(query.Statuses, value)
			}
		}
	}
	for _, filter := range []struct {
		name   string
		target *time.Time
	}{
		{name: "created_after", target: &query.CreatedAfter},
		{name: "created_before", target: &query.CreatedBefore},
	} {
		if raw := values.Get(filter.name); raw != "" {
			value, err := time.Parse(time.RFC3339, raw)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": filter.name + " must be RFC3339"})
				return storage.EvidenceQuery{}, false
			}
			*filter.target = value.UTC()
		}
	}
	if pagination {
		if raw := values.Get("limit"); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "limit must be an integer"})
				return storage.EvidenceQuery{}, false
			}
			query.Limit = parsed
		}
	}
	if roleOf(principal) == storage.RoleAccountAdmin {
		query.TenantID = values.Get("tenant")
	} else {
		if requestedTenant := values.Get("tenant"); requestedTenant != "" && requestedTenant != principal.TenantID {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "tenant administrator cannot query another tenant"})
			return storage.EvidenceQuery{}, false
		}
		query.TenantID = principal.TenantID
	}
	if err := query.Validate(); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return storage.EvidenceQuery{}, false
	}
	return query, true
}

func (s *Server) handleEvidenceQuery(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !requireTenantOperator(w, principal) {
		return
	}
	if s.evidence == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "evidence query is not configured"})
		return
	}
	query, ok := parseEvidenceQuery(w, r, principal, true)
	if !ok {
		return
	}
	var page storage.EvidencePage
	var err error
	if pager, ok := s.evidence.(storage.EvidencePager); ok {
		page, err = pager.QueryEvidencePage(r.Context(), query)
	} else {
		if query.Cursor != "" {
			writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "evidence cursor pagination is not supported by this store"})
			return
		}
		page.Records, err = s.evidence.QueryEvidence(r.Context(), query)
	}
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, storage.ErrEvidenceCursorInvalid) {
			status = http.StatusBadRequest
		}
		writeJSON(w, status, map[string]string{"error": err.Error()})
		return
	}
	visible := make([]evidenceResponseRecord, 0, len(page.Records))
	for _, record := range page.Records {
		if record.TenantID != "" && record.TenantID != principal.TenantID && roleOf(principal) != storage.RoleAccountAdmin {
			continue
		}
		if record.Scope != "" {
			scope, err := storage.ParseScopePath(record.Scope)
			if err != nil || !canReadScope(principal, scope) {
				continue
			}
		}
		visible = append(visible, evidenceResponseRecord{
			EvidenceRecord: record, DetailPath: evidenceDetailPath(record),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"evidence": visible, "count": len(visible), "next_cursor": page.NextCursor,
	})
}

func (s *Server) handleEvidenceStats(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !requireTenantOperator(w, principal) {
		return
	}
	query, ok := parseEvidenceQuery(w, r, principal, false)
	if !ok {
		return
	}
	store, ok := s.evidence.(storage.EvidenceStatsStore)
	if !ok {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "evidence statistics are not supported by this store"})
		return
	}
	stats, err := store.RunEvidenceStats(r.Context(), query)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": stats})
}

func evidenceDetailPath(record storage.EvidenceRecord) string {
	switch record.Kind {
	case storage.EvidenceRun, storage.EvidenceKind("backtest"):
		if record.SessionID == "" || record.ID == "" {
			return ""
		}
		return "/v1/admin/evidence/runs/" + url.PathEscape(record.ID) + "?session_id=" + url.QueryEscape(record.SessionID)
	case storage.EvidenceEvaluation:
		return "/v1/admin/evaluations/runs/" + url.PathEscape(record.ID)
	case storage.EvidenceCanary:
		if record.ProfileID == "" {
			return ""
		}
		return "/v1/profiles/" + url.PathEscape(record.ProfileID) + "/canaries/" + url.PathEscape(record.ID)
	case storage.EvidenceRelease:
		if record.ProfileID == "" || record.Version < 1 {
			return ""
		}
		return "/v1/profiles/" + url.PathEscape(record.ProfileID) + "/releases/" + strconv.Itoa(record.Version)
	default:
		return ""
	}
}

func (s *Server) handleEvidenceRunDetail(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !requireTenantOperator(w, principal) {
		return
	}
	sessionID := r.URL.Query().Get("session_id")
	if sessionID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "session_id is required"})
		return
	}
	session, err := s.sessions.Load(r.Context(), sessionID)
	if err != nil || !canReadScope(principal, sessionScopeOnSuccess(session)) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "run evidence not found"})
		return
	}
	runID := r.PathValue("runID")
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
		if err != nil || value < 1 || value > 2000 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "limit must be between 1 and 2000"})
			return
		}
		limit = value
	}
	filtered := make([]core.SessionEvent, 0)
	for _, event := range session.Events() {
		if event.RunID == runID && event.Seq > afterSeq {
			filtered = append(filtered, event)
		}
	}
	if len(filtered) == 0 && afterSeq < 0 {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "run evidence not found"})
		return
	}
	hasMore := len(filtered) > limit
	if hasMore {
		filtered = filtered[:limit]
	}
	nextAfter := afterSeq
	if len(filtered) > 0 {
		nextAfter = filtered[len(filtered)-1].Seq
	}
	evidence, found, evidenceErr := core.ExtractRunCompositionEvidence(session.Events(), runID)
	if evidenceErr != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": evidenceErr.Error()})
		return
	}
	status, _ := session.RunStatus(runID)
	writeJSON(w, http.StatusOK, map[string]any{
		"session": sessionResponse(session), "run_id": runID, "status": status,
		"composition_evidence": evidence, "composition_evidence_found": found,
		"events": filtered, "next_after_seq": nextAfter, "has_more": hasMore,
	})
}

func sessionScopeOnSuccess(session *core.Session) core.ScopePath {
	if session == nil {
		return core.ScopePath{}
	}
	return session.Scope()
}
