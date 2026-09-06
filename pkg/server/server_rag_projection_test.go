package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
)

type ragProjectionMaintainerStub struct {
	mu           sync.Mutex
	stats        storage.RagProjectionStats
	statsErr     error
	rebuildErr   error
	statsCalls   int
	rebuildCalls int
}

func (s *ragProjectionMaintainerStub) ProjectionStats(context.Context) (storage.RagProjectionStats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statsCalls++
	return s.stats, s.statsErr
}

func (s *ragProjectionMaintainerStub) RebuildProjection(context.Context) (storage.RagProjectionStats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rebuildCalls++
	return s.stats, s.rebuildErr
}

func (s *ragProjectionMaintainerStub) calls() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.statsCalls, s.rebuildCalls
}

type ragProjectionAuditStore struct {
	mu     sync.Mutex
	events []storage.AuditEvent
}

func (s *ragProjectionAuditStore) RecordAudit(_ context.Context, event storage.AuditEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
	return nil
}

func (s *ragProjectionAuditStore) ListAudit(context.Context, storage.AuditFilter) ([]storage.AuditEvent, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	events := append([]storage.AuditEvent(nil), s.events...)
	return events, len(events), nil
}

func (s *ragProjectionAuditStore) last(t *testing.T) storage.AuditEvent {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.events) == 0 {
		t.Fatal("expected rag projection rebuild audit event")
	}
	return s.events[len(s.events)-1]
}

func TestAdminRagProjectionStatsAndRebuild(t *testing.T) {
	stats := storage.RagProjectionStats{
		CanonicalDocuments: 7, TokenRows: 31, TagRows: 13,
		TokenDocuments: 6, TagDocuments: 5, OrphanTokenRows: 2, OrphanTagRows: 1,
	}
	maintainer := &ragProjectionMaintainerStub{stats: stats}
	audit := &ragProjectionAuditStore{}
	api := newRagProjectionTestServer(t, maintainer, audit)

	get := ragProjectionRequest(t, api.Handler(), http.MethodGet, "/v1/admin/rag/projection", "admin")
	if get.Code != http.StatusOK {
		t.Fatalf("stats status=%d body=%s", get.Code, get.Body.String())
	}
	assertRagProjectionResponseSafe(t, get.Body.String())
	var statsResponse struct {
		Projection storage.RagProjectionStats `json:"projection"`
	}
	if err := json.Unmarshal(get.Body.Bytes(), &statsResponse); err != nil {
		t.Fatal(err)
	}
	if statsResponse.Projection != stats {
		t.Fatalf("stats response=%#v want=%#v", statsResponse.Projection, stats)
	}
	if statsCalls, rebuildCalls := maintainer.calls(); statsCalls != 1 || rebuildCalls != 0 {
		t.Fatalf("GET calls stats=%d rebuild=%d", statsCalls, rebuildCalls)
	}

	rebuild := ragProjectionRequest(t, api.Handler(), http.MethodPost, "/v1/admin/rag/projection/rebuild", "admin")
	if rebuild.Code != http.StatusOK {
		t.Fatalf("rebuild status=%d body=%s", rebuild.Code, rebuild.Body.String())
	}
	assertRagProjectionResponseSafe(t, rebuild.Body.String())
	var rebuildResponse struct {
		Projection storage.RagProjectionStats `json:"projection"`
		Status     string                     `json:"status"`
	}
	if err := json.Unmarshal(rebuild.Body.Bytes(), &rebuildResponse); err != nil {
		t.Fatal(err)
	}
	if rebuildResponse.Status != "rebuilt" || rebuildResponse.Projection != stats {
		t.Fatalf("rebuild response=%#v", rebuildResponse)
	}
	if statsCalls, rebuildCalls := maintainer.calls(); statsCalls != 1 || rebuildCalls != 1 {
		t.Fatalf("POST calls stats=%d rebuild=%d", statsCalls, rebuildCalls)
	}

	event := audit.last(t)
	if event.Action != "rag.projection.rebuild" || event.Target != "rag/projection" {
		t.Fatalf("audit action=%q target=%q", event.Action, event.Target)
	}
	wantDetail := ragProjectionAuditDetail(stats)
	if len(event.Detail) != len(wantDetail) {
		t.Fatalf("audit detail=%#v", event.Detail)
	}
	for key, want := range wantDetail {
		if event.Detail[key] != want {
			t.Fatalf("audit detail[%q]=%#v want=%#v", key, event.Detail[key], want)
		}
	}
}

func TestAdminRagProjectionAuthorizationDisabledAndErrors(t *testing.T) {
	for _, identity := range []string{"tenant-admin", "user"} {
		maintainer := &ragProjectionMaintainerStub{}
		api := newRagProjectionTestServer(t, maintainer, nil)
		for _, request := range []struct{ method, path string }{
			{http.MethodGet, "/v1/admin/rag/projection"},
			{http.MethodPost, "/v1/admin/rag/projection/rebuild"},
		} {
			response := ragProjectionRequest(t, api.Handler(), request.method, request.path, identity)
			if response.Code != http.StatusForbidden {
				t.Fatalf("%s %s identity=%s status=%d body=%s", request.method, request.path, identity, response.Code, response.Body.String())
			}
		}
		if statsCalls, rebuildCalls := maintainer.calls(); statsCalls != 0 || rebuildCalls != 0 {
			t.Fatalf("unauthorized calls stats=%d rebuild=%d", statsCalls, rebuildCalls)
		}
	}

	disabled := newRagProjectionTestServer(t, nil, nil)
	for _, request := range []struct{ method, path string }{
		{http.MethodGet, "/v1/admin/rag/projection"},
		{http.MethodPost, "/v1/admin/rag/projection/rebuild"},
	} {
		response := ragProjectionRequest(t, disabled.Handler(), request.method, request.path, "admin")
		if response.Code != http.StatusNotImplemented {
			t.Fatalf("disabled %s status=%d body=%s", request.path, response.Code, response.Body.String())
		}
	}

	secretErr := errors.New("projection failure secret-value document-secret token-secret tag-secret")
	for _, test := range []struct {
		name       string
		maintainer *ragProjectionMaintainerStub
		method     string
		path       string
	}{
		{"stats", &ragProjectionMaintainerStub{statsErr: secretErr}, http.MethodGet, "/v1/admin/rag/projection"},
		{"rebuild", &ragProjectionMaintainerStub{rebuildErr: secretErr}, http.MethodPost, "/v1/admin/rag/projection/rebuild"},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := ragProjectionRequest(t, newRagProjectionTestServer(t, test.maintainer, nil).Handler(), test.method, test.path, "admin")
			if response.Code != http.StatusInternalServerError {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			if strings.Contains(response.Body.String(), "secret-value") || strings.Contains(response.Body.String(), "document-secret") {
				t.Fatalf("storage error leaked: %s", response.Body.String())
			}
		})
	}
}

func newRagProjectionTestServer(t *testing.T, maintainer storage.RagProjectionMaintainer, audit storage.AuditStore) *Server {
	t.Helper()
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	api, err := New(Config{
		Runtime: &core.Runtime{}, Sessions: core.NewMemorySessionStore(),
		RagProjection: maintainer, Audit: audit,
		Authenticator: AuthenticatorFunc(func(r *http.Request) (core.Principal, error) {
			role := map[string]string{
				"admin": storage.RoleAccountAdmin, "tenant-admin": storage.RoleAccountTenantAdmin, "user": storage.RoleAccountUser,
			}[r.Header.Get("X-Test-Identity")]
			if role == "" {
				return core.Principal{}, errors.New("unknown test identity")
			}
			return core.Principal{SubjectID: r.Header.Get("X-Test-Identity"), TenantID: "acme", Scope: global, Attributes: map[string]string{"role": role}}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	return api
}

func ragProjectionRequest(t *testing.T, handler http.Handler, method, path, identity string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, path, nil)
	request.Header.Set("X-Test-Identity", identity)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func assertRagProjectionResponseSafe(t *testing.T, body string) {
	t.Helper()
	lower := strings.ToLower(body)
	for _, forbidden := range []string{"content", "tags_json", "document_id", "document-secret", "token-secret", "tag-secret", "secret-value"} {
		if strings.Contains(lower, forbidden) {
			t.Fatalf("rag projection response leaked %q: %s", forbidden, body)
		}
	}
}
