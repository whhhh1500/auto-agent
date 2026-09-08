package server

import (
	"net/http"

	"github.com/whhhh1500/auto-agent/pkg/app/modelcatalog"
)

type modelRuntimeCatalogResponse struct {
	SnapshotRevision string                       `json:"snapshot_revision"`
	Providers        []modelcatalog.Provider      `json:"providers"`
	Protocols        []modelcatalog.Protocol      `json:"protocols"`
	Compatibilities  []modelcatalog.Compatibility `json:"compatibilities"`
	Defaults         []modelcatalog.Default       `json:"defaults"`
}

func (s *Server) handleAdminModelRuntimes(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.authenticate(w, r); !ok || !s.ensureControlPlane(w, r) {
		return
	}
	if s.modelCatalog == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "model runtime catalog unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, modelRuntimeCatalogResponse{
		SnapshotRevision: s.modelCatalog.SnapshotRevision(),
		Providers:        s.modelCatalog.Providers(), Protocols: s.modelCatalog.Protocols(),
		Compatibilities: s.modelCatalog.Compatibilities(),
		Defaults:        s.modelCatalog.Defaults(),
	})
}
