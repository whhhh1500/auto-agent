package server

import (
	"bytes"
	"net/http"
)

// nativeStrictExecutionProjectionHeader preserves a successful, committed
// account response while telling the caller that new execution is fail-closed.
// The value is deliberately bounded and contains no reconciliation detail.
const nativeStrictExecutionProjectionHeader = "X-Harness-Execution-Projection"

// nativeStrictHandler intentionally does not reuse the generic route
// registration helpers. Phase 1 owns a static execution projection, so
// dynamic control-plane mutations must remain unavailable even if a future
// generic Config grows optional services with similar route names.
func (s *Server) nativeStrictHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /readyz", s.handleReady)
	mux.HandleFunc("POST /v1/sessions", s.handleCreateSession)
	mux.HandleFunc("GET /v1/sessions/{id}", s.handleGetSession)
	mux.HandleFunc("GET /v1/sessions/{id}/events", s.handleEvents)
	mux.HandleFunc("POST /v1/sessions/{id}/runs", nativeStrictQueuedOnly)
	mux.HandleFunc("POST /v1/sessions/{id}/runs/async", s.handleEnqueueRun)
	mux.HandleFunc("GET /v1/sessions/{id}/runs", s.handleListRuns)
	mux.HandleFunc("GET /v1/sessions/{id}/runs/{runID}", s.handleGetRun)
	mux.HandleFunc("POST /v1/sessions/{id}/runs/{runID}/cancel", s.handleCancelRun)
	mux.HandleFunc("POST /v1/sessions/{id}/cancel", s.handleCancel)
	mux.HandleFunc("GET /v1/sessions", s.handleListSessions)
	mux.HandleFunc("GET /v1/profiles/{id}/capabilities", s.handleProfileCapabilities)
	mux.HandleFunc("GET /v1/profiles", s.handleListProfiles)
	mux.HandleFunc("GET /v1/capabilities", s.handleListCapabilities)
	mux.HandleFunc("GET /v1/admin/profiles/{id}", s.handleAdminProfileGet)

	mux.HandleFunc("POST /v1/auth/login", s.handleLogin)
	mux.HandleFunc("POST /v1/auth/logout", s.handleLogout)
	mux.HandleFunc("POST /v1/auth/activate", s.nativeStrictAccountMutation(s.handleActivateAccount))
	mux.HandleFunc("GET /v1/admin/accounts", s.handleListAccounts)
	mux.HandleFunc("POST /v1/admin/accounts", s.nativeStrictAccountMutation(s.handleCreateAccount))
	mux.HandleFunc("POST /v1/admin/accounts/{account}/password", s.nativeStrictAccountMutation(s.handleSetPassword))
	mux.HandleFunc("POST /v1/admin/accounts/{account}/status", s.nativeStrictAccountMutation(s.handleSetAccountStatus))
	mux.HandleFunc("GET /v1/admin/tenants", s.handleListTenants)
	mux.HandleFunc("POST /v1/admin/tenants", s.nativeStrictAccountMutation(s.handleCreateTenant))
	s.registerApprovalRoutes(mux)

	// Dynamic control is explicit rather than falling through to 404 or an
	// optional generic service. Keep these registrations close to the native
	// surface so a review can audit every Phase 1 mutation boundary.
	for _, pattern := range []string{
		"POST /v1/admin/policies",
		"DELETE /v1/admin/bindings/{id}",
		"POST /v1/admin/credentials",
		"POST /v1/admin/capabilities/bind",
		"POST /v1/admin/capabilities/{id}/disable",
		"POST /v1/admin/capabilities/{id}/enable",
		"PUT /v1/admin/profiles/{id}",
		"POST /v1/profiles/{id}/publish",
		"POST /v1/profiles/{id}/rollback",
		"POST /v1/profiles/{id}/canaries",
		"POST /v1/profiles/{id}/canaries/{canaryID}/percentage",
		"POST /v1/profiles/{id}/canaries/{canaryID}/pause",
		"POST /v1/profiles/{id}/canaries/{canaryID}/resume",
		"POST /v1/profiles/{id}/canaries/{canaryID}/rollback",
		"POST /v1/profiles/{id}/canaries/{canaryID}/promote",
		"PUT /v1/admin/model-settings/llm",
		"PUT /v1/admin/settings/{key}",
		"PUT /v1/resources/{key...}",
		"DELETE /v1/resources/{key...}",
		"PUT /v1/admin/storage/resources",
		"PUT /v1/admin/storage/sessions",
		"POST /v1/admin/storage/test",
		"POST /v1/admin/notification-targets",
		"PUT /v1/admin/notification-targets",
		"DELETE /v1/admin/notification-targets",
	} {
		mux.HandleFunc(pattern, nativeStrictUnavailable)
	}

	mux.HandleFunc("GET /console", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/console/", http.StatusMovedPermanently)
	})
	mux.Handle("GET /console/", http.StripPrefix("/console", s.console))
	return s.accessLog(recoverMiddleware(s.logger, mux))
}

func nativeStrictUnavailable(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusNotImplemented, map[string]string{
		"error": "operation is unavailable in native strict Phase 1",
	})
}

func nativeStrictQueuedOnly(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusMethodNotAllowed, map[string]string{
		"error": "native strict execution requires the queued run endpoint",
	})
}

func (s *Server) nativeStrictAccountMutation(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		release := s.lockExecutionProjectionMutation()
		defer release()
		recorder := &nativeStrictBufferedResponse{header: make(http.Header)}
		next(recorder, r)
		if recorder.status >= http.StatusOK && recorder.status < http.StatusMultipleChoices {
			if err := s.reconcileNativeStrictExecutionProjectionLocked(r.Context()); err != nil {
				recorder.header.Set(nativeStrictExecutionProjectionHeader, "unavailable")
				if s.logger != nil {
					s.logger.ErrorContext(r.Context(), "native strict execution projection unavailable after committed account mutation")
				}
			}
		}
		recorder.flushTo(w)
	}
}

type nativeStrictBufferedResponse struct {
	header http.Header
	body   bytes.Buffer
	status int
}

func (w *nativeStrictBufferedResponse) Header() http.Header { return w.header }

func (w *nativeStrictBufferedResponse) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
}

func (w *nativeStrictBufferedResponse) Write(payload []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.body.Write(payload)
}

func (w *nativeStrictBufferedResponse) flushTo(target http.ResponseWriter) {
	for key, values := range w.header {
		target.Header()[key] = append([]string(nil), values...)
	}
	status := w.status
	if status == 0 {
		status = http.StatusOK
	}
	target.WriteHeader(status)
	_, _ = target.Write(w.body.Bytes())
}
