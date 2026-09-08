package server

import (
	"net/http"

	"github.com/whhhh1500/auto-agent/pkg/storage"
)

// handleAdminMemoryProjectionStats returns aggregate-only projection health.
// The maintenance store never exposes canonical Memory keys, content, or tag
// values through this surface.
func (s *Server) handleAdminMemoryProjectionStats(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !requireAdmin(w, principal) {
		return
	}
	if !s.requireMemoryProjectionMaintenance(w) {
		return
	}
	stats, err := s.memoryProjection.ProjectionStats(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "memory projection statistics failed"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"projection": stats})
}

// handleAdminMemoryProjectionRebuild atomically replaces derived projections
// from canonical Memory rows. Audit detail is deliberately limited to counts.
func (s *Server) handleAdminMemoryProjectionRebuild(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !requireAdmin(w, principal) {
		return
	}
	if !s.requireMemoryProjectionMaintenance(w) {
		return
	}
	stats, err := s.memoryProjection.RebuildProjection(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "memory projection rebuild failed"})
		return
	}
	s.recordAudit(r, principal, "memory.projection.rebuild", "memory/projection", memoryProjectionAuditDetail(stats))
	writeJSON(w, http.StatusOK, map[string]any{
		"projection": stats,
		"status":     "rebuilt",
	})
}

func (s *Server) requireMemoryProjectionMaintenance(w http.ResponseWriter) bool {
	if s.memoryProjection == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "memory projection maintenance is not configured"})
		return false
	}
	return true
}

func memoryProjectionAuditDetail(stats storage.MemoryProjectionStats) map[string]any {
	return map[string]any{
		"canonical_entries":        stats.CanonicalEntries,
		"tag_rows":                 stats.TagRows,
		"tagged_keys":              stats.TaggedKeys,
		"orphan_tag_rows":          stats.OrphanTagRows,
		"key_search_missing":       stats.KeySearchMissing,
		"content_search_missing":   stats.ContentSearchMissing,
		"search_projected_entries": stats.SearchProjectedEntries,
	}
}
