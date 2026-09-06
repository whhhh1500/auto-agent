package server

import (
	"net/http"

	httpsandbox "github.com/cc-auto-agent/harness-core/pkg/adapter/httpapi/sandbox"
)

func (s *Server) registerSandboxRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/admin/sandbox/providers", s.handleSandboxProviders)
}

// handleSandboxProviders is discovery only. It never starts a session or
// reveals a provider object, local mount, host path, or secret configuration.
func (s *Server) handleSandboxProviders(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok || !requireAdmin(w, principal) {
		return
	}
	if s.sandboxProviders == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "sandbox providers are not configured"})
		return
	}
	metadata := s.sandboxProviders.Metadata()
	providers := make([]httpsandbox.ProviderView, 0, len(metadata))
	for _, item := range metadata {
		report, err := s.sandboxProviders.Probe(r.Context(), item.ID, item.Version)
		providers = append(providers, httpsandbox.View(item, report, err))
	}
	writeJSON(w, http.StatusOK, httpsandbox.ProvidersResponse{Providers: providers})
}
