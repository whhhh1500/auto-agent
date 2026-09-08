package server

import (
	"errors"
	"net/http"
	"strconv"

	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/storage"
)

func (s *Server) registerApprovalRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/admin/approvals", s.handleListApprovals)
	mux.HandleFunc("GET /v1/admin/approvals/{approvalID}", s.handleGetApproval)
	mux.HandleFunc("POST /v1/admin/approvals/{approvalID}/decision", s.handleDecideApproval)
}

func (s *Server) handleListApprovals(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !requireTenantOperator(w, principal) {
		return
	}
	if s.approvals == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "approval store is not configured"})
		return
	}
	limit := 100
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 500 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "limit must be between 1 and 500"})
			return
		}
		limit = parsed
	}
	filter := storage.ApprovalFilter{Limit: limit}
	if raw := r.URL.Query().Get("status"); raw != "" {
		filter.Status = core.ApprovalDecision(raw)
	}
	if roleOf(principal) == storage.RoleAccountAdmin {
		filter.TenantID = r.URL.Query().Get("tenant")
	} else {
		filter.TenantID = principal.TenantID
	}
	records, err := s.approvals.ListApprovals(r.Context(), filter)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"approvals": records})
}

func (s *Server) handleGetApproval(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !requireTenantOperator(w, principal) {
		return
	}
	if s.approvals == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "approval store is not configured"})
		return
	}
	record, err := s.approvals.GetApproval(r.Context(), r.PathValue("approvalID"))
	if err != nil || !canOperateApproval(principal, record) {
		status := http.StatusInternalServerError
		message := "approval lookup failed"
		if errors.Is(err, storage.ErrApprovalNotFound) || (err == nil && !canOperateApproval(principal, record)) {
			status, message = http.StatusNotFound, "approval not found"
		} else if err != nil {
			message = err.Error()
		}
		writeJSON(w, status, map[string]string{"error": message})
		return
	}
	writeJSON(w, http.StatusOK, record)
}

func (s *Server) handleDecideApproval(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !requireTenantOperator(w, principal) {
		return
	}
	if s.approvals == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "approval store is not configured"})
		return
	}
	record, err := s.approvals.GetApproval(r.Context(), r.PathValue("approvalID"))
	if err != nil || !canOperateApproval(principal, record) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "approval not found"})
		return
	}
	var request struct {
		Decision core.ApprovalDecision `json:"decision"`
	}
	if !s.decodeJSON(w, r, &request) {
		return
	}
	decided, changed, err := s.approvals.DecideApproval(r.Context(), record.ID, request.Decision, principal.SubjectID)
	if err != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	if changed {
		attrs := core.TelemetryAttributes{"approval.decision": string(request.Decision)}
		core.AddTelemetryCounter(s.telemetry, r.Context(), core.MetricApprovalDecisions, 1, attrs)
		s.recordAudit(r, principal, "approval."+string(request.Decision), record.ID, map[string]any{
			"run_id": record.RunID, "session_id": record.SessionID, "capability": record.CapabilityID,
		})
	}
	writeJSON(w, http.StatusOK, decided)
}

func canOperateApproval(principal core.Principal, record storage.ApprovalRecord) bool {
	if roleOf(principal) == storage.RoleAccountAdmin {
		return true
	}
	return principal.TenantID != "" && principal.TenantID == record.TenantID
}
