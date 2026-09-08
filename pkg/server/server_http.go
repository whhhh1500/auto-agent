package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strings"
	"time"

	"github.com/whhhh1500/auto-agent/pkg/adapter/httpapi/jsonbody"
	"github.com/whhhh1500/auto-agent/pkg/buildinfo"
	"github.com/whhhh1500/auto-agent/pkg/storage"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

// statusWriter captures the response status for access logging.
type statusWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (w *statusWriter) WriteHeader(code int) {
	if w.wroteHeader {
		return
	}
	w.wroteHeader = true
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(payload []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(payload)
}

// Flush preserves streaming behavior through the access-log wrapper.
func (w *statusWriter) Flush() {
	_ = w.FlushError()
}

// FlushError preserves a transport failure through the access-log wrapper so
// ResponseController does not stop at this wrapper and discard it.
func (w *statusWriter) FlushError() error {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	err := http.NewResponseController(w.ResponseWriter).Flush()
	if errors.Is(err, http.ErrNotSupported) {
		return nil
	}
	return err
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// accessLog records one line per request (assets excluded): method, path,
// status, duration, and caller. It never changes response semantics.
func (s *Server) accessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.logger == nil || strings.HasPrefix(r.URL.Path, "/console/assets/") {
			next.ServeHTTP(w, r)
			return
		}
		started := time.Now()
		recorder := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, r)
		s.logger.InfoContext(r.Context(), "http",
			slog.String("method", r.Method), slog.String("path", r.URL.Path),
			slog.Int("status", recorder.status),
			slog.Int64("duration_ms", time.Since(started).Milliseconds()),
			slog.String("remote", r.RemoteAddr),
		)
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ok", "version": buildinfo.Version, "commit": buildinfo.Commit,
	})
}

func (s *Server) handleReady(w http.ResponseWriter, _ *http.Request) {
	err := s.readinessError()
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready", "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) setReadyError(err error) {
	s.readyMu.Lock()
	s.readyErr = err
	s.readyMu.Unlock()
}

func (s *Server) setProfileProjectionError(bindingID string, err error) {
	s.setProfileProjectionFault(bindingID, err)
}

func (s *Server) clearProfileProjectionError(bindingID string) {
	s.clearProfileProjectionFault(bindingID)
}

func (s *Server) readinessError() error {
	s.readyMu.RLock()
	err := s.readyErr
	s.readyMu.RUnlock()
	if err != nil {
		return err
	}
	return s.executionProjectionCoordinator().fault("")
}

func (s *Server) profileProjectionError() error {
	return s.executionProjectionCoordinator().fault(profileProjectionSourcePrefix)
}

func (s *Server) ensureRunProjection(w http.ResponseWriter) bool {
	if s.profileProjectionError() == nil {
		return true
	}
	writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "profile projection is unavailable"})
	return false
}

func (s *Server) authenticate(w http.ResponseWriter, r *http.Request) (core.Principal, bool) {
	principal, err := s.authenticator.Authenticate(r)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": err.Error()})
		return core.Principal{}, false
	}
	if (principal.Attributes["account.status"] == storage.AccountPendingActivation ||
		principal.Attributes["account.must_change_password"] == "true") &&
		r.URL.Path != "/v1/auth/activate" && r.URL.Path != "/v1/auth/logout" {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "account activation required"})
		return core.Principal{}, false
	}
	return principal, true
}

func (s *Server) decodeJSON(w http.ResponseWriter, r *http.Request, target any) bool {
	if err := jsonbody.Decode(w, r, s.maxBody, target); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return false
	}
	return true
}

// decodeOptionalJSON uses the same compatibility and validation rules as
// decodeJSON, while preserving the runner claim endpoint's empty-body default.
func (s *Server) decodeOptionalJSON(w http.ResponseWriter, r *http.Request, target any) bool {
	if err := jsonbody.DecodeOptional(w, r, s.maxBody, target); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

const defaultSSEWriteTimeout = 5 * time.Second

// sseStream stops attempting delivery after the first transport failure. The
// run continues to append and persist events, but a blocked or closed client
// must not keep extending its write deadline through later terminal events.
type sseStream struct {
	writer  http.ResponseWriter
	timeout time.Duration
	failed  bool
}

func newSSEStream(writer http.ResponseWriter, timeout time.Duration) *sseStream {
	if timeout <= 0 {
		timeout = defaultSSEWriteTimeout
	}
	return &sseStream{writer: writer, timeout: timeout}
}

func (stream *sseStream) Write(event string, value any) error {
	if stream.failed {
		return nil
	}
	if err := writeSSEWithTimeout(stream.writer, event, value, stream.timeout); err != nil {
		stream.failed = true
		return err
	}
	return nil
}

func writeSSE(w http.ResponseWriter, event string, value any) error {
	return writeSSEWithTimeout(w, event, value, defaultSSEWriteTimeout)
}

func writeSSEWithTimeout(w http.ResponseWriter, event string, value any, timeout time.Duration) error {
	controller := http.NewResponseController(w)
	deadlineSet := false
	if timeout > 0 {
		if err := controller.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
			// Custom ResponseWriters used by integrations and tests need not own a
			// network connection. They retain compatibility, but cannot promise a
			// bounded transport write.
			if !errors.Is(err, http.ErrNotSupported) {
				return fmt.Errorf("set SSE write deadline: %w", err)
			}
		} else {
			deadlineSet = true
		}
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		encoded = []byte(`{"error":"failed to encode event"}`)
	}
	if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, encoded); err != nil {
		return fmt.Errorf("write SSE event: %w", err)
	}
	if err := controller.Flush(); err != nil {
		return fmt.Errorf("flush SSE event: %w", err)
	}
	if deadlineSet {
		// Keep an expired deadline after a transport failure. net/http may do a
		// final response flush while unwinding the handler; clearing it there
		// would reintroduce an unbounded blocked write. Successful SSE writes
		// must clear it so a keep-alive connection remains usable.
		_ = controller.SetWriteDeadline(time.Time{})
	}
	return nil
}

func recoverMiddleware(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				if logger != nil {
					logger.ErrorContext(r.Context(), "panic recovered",
						slog.String("path", r.URL.Path), slog.Any("panic", recovered),
						slog.String("stack", string(debug.Stack())),
					)
				}
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// HeaderAuthenticator is the development authenticator: identity comes from
// plaintext headers, so it must never face untrusted networks. It is a thin
// composition of HeaderIdentitySource and StaticPrincipalMapper — production
// deployments keep the same Authenticator interface and swap in their own
// chain (see package auth docs).
type HeaderAuthenticator struct {
	Root          []core.ScopeRef
	DefaultGrants core.PermissionSet
	// RoleGrants maps X-Harness-Roles entries to additional grants.
	RoleGrants map[string][]core.Permission
	// AllowGrantsHeader enables X-Harness-Grants. Development only.
	AllowGrantsHeader bool
}

func (a HeaderAuthenticator) Authenticate(r *http.Request) (core.Principal, error) {
	return AuthChain{
		Source: HeaderIdentitySource{AllowGrantsHeader: a.AllowGrantsHeader},
		Mapper: StaticPrincipalMapper{
			Root:                 a.Root,
			DefaultGrants:        a.DefaultGrants,
			RoleGrants:           a.RoleGrants,
			GrantsFromAttributes: []string{"dev.grants"},
		},
	}.Authenticate(r)
}
