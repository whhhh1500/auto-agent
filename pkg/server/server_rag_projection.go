package server

import (
	"net/http"

	"github.com/cc-auto-agent/harness-core/pkg/storage"
)

// handleAdminRagProjectionStats returns aggregate-only projection health.
// The maintenance store never exposes canonical document content, tags, token
// values, or document identifiers through this surface.
func (s *Server) handleAdminRagProjectionStats(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !requireAdmin(w, principal) {
		return
	}
	if !s.requireRagProjectionMaintenance(w) {
		return
	}
	stats, err := s.ragProjection.ProjectionStats(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "rag projection statistics failed"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"projection": stats})
}

// handleAdminRagProjectionRebuild atomically replaces derived projections
// from canonical documents. Audit detail is deliberately limited to counts.
func (s *Server) handleAdminRagProjectionRebuild(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !requireAdmin(w, principal) {
		return
	}
	if !s.requireRagProjectionMaintenance(w) {
		return
	}
	stats, err := s.ragProjection.RebuildProjection(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "rag projection rebuild failed"})
		return
	}
	s.recordAudit(r, principal, "rag.projection.rebuild", "rag/projection", ragProjectionAuditDetail(stats))
	writeJSON(w, http.StatusOK, map[string]any{
		"projection": stats,
		"status":     "rebuilt",
	})
}

func (s *Server) requireRagProjectionMaintenance(w http.ResponseWriter) bool {
	if s.ragProjection == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "rag projection maintenance is not configured"})
		return false
	}
	return true
}

func ragProjectionAuditDetail(stats storage.RagProjectionStats) map[string]any {
	return map[string]any{
		"canonical_documents": stats.CanonicalDocuments,
		"token_rows":          stats.TokenRows,
		"tag_rows":            stats.TagRows,
		"token_documents":     stats.TokenDocuments,
		"tag_documents":       stats.TagDocuments,
		"orphan_token_rows":   stats.OrphanTokenRows,
		"orphan_tag_rows":     stats.OrphanTagRows,
	}
}
