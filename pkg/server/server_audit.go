// Audit instrumentation: one call per mutating control-plane operation. Each
// event is written to the durable audit store when configured and always to
// the structured log, so the trail survives even without SQL.
package server

import (
	"github.com/whhhh1500/auto-agent/pkg/storage"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

func timeNowUTC() time.Time            { return time.Now().UTC() }
func slogString(k, v string) slog.Attr { return slog.String(k, v) }
func strconvParseInt(raw string) (int64, error) {
	return strconv.ParseInt(raw, 10, 64)
}

// audit records one administrative action performed by principal on target.
// best-effort: store failures never break the request, but are logged.
func (s *Server) recordAudit(r *http.Request, principal core.Principal, action, target string, detail map[string]any) {
	event := storage.AuditEvent{
		Time:     timeNowUTC(),
		Actor:    principal.SubjectID,
		Role:     roleOf(principal),
		TenantID: principal.TenantID,
		Action:   action,
		Target:   target,
		Detail:   detail,
		RemoteIP: r.RemoteAddr,
	}
	if s.audit != nil {
		if err := s.audit.RecordAudit(r.Context(), event); err != nil && s.logger != nil {
			s.logger.ErrorContext(r.Context(), "audit record failed", slogString("action", action), slogString("error", err.Error()))
		}
	}
	if s.logger != nil {
		s.logger.InfoContext(r.Context(), "audit",
			slogString("action", action), slogString("actor", event.Actor),
			slogString("target", target), slogString("tenant", event.TenantID),
		)
	}
}

// handleAuditList serves the audit trail for the console. Platform admins see
// everything; tenant admins are pinned to their own tenant.
func (s *Server) handleAuditList(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !requireTenantOperator(w, principal) {
		return
	}
	if s.audit == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "audit store is not configured"})
		return
	}
	filter := storage.AuditFilter{
		Actor:  r.URL.Query().Get("actor"),
		Action: r.URL.Query().Get("action"),
	}
	if roleOf(principal) != storage.RoleAccountAdmin {
		filter.TenantID = principal.TenantID
	}
	if raw := r.URL.Query().Get("limit"); raw != "" {
		if parsed, err := strconvParseInt(raw); err == nil {
			filter.Limit = int(parsed)
		}
	}
	if raw := r.URL.Query().Get("offset"); raw != "" {
		if parsed, err := strconvParseInt(raw); err == nil {
			filter.Offset = int(parsed)
		}
	}
	events, total, err := s.audit.ListAudit(r.Context(), filter)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": events, "total": total})
}

// handleLoginAudit records login attempts (success and failure).
func (s *Server) auditLogin(r *http.Request, email string, success bool, reason string) {
	principal := core.Principal{SubjectID: email}
	detail := map[string]any{"success": success}
	if reason != "" {
		detail["reason"] = reason
	}
	s.recordAudit(r, principal, "auth.login", email, detail)
}
