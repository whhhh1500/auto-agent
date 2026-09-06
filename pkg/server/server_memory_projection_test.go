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

type memoryProjectionMaintainerStub struct {
	mu           sync.Mutex
	stats        storage.MemoryProjectionStats
	statsErr     error
	rebuildErr   error
	statsCalls   int
	rebuildCalls int
}

func (s *memoryProjectionMaintainerStub) ProjectionStats(context.Context) (storage.MemoryProjectionStats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statsCalls++
	return s.stats, s.statsErr
}

func (s *memoryProjectionMaintainerStub) RebuildProjection(context.Context) (storage.MemoryProjectionStats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rebuildCalls++
	return s.stats, s.rebuildErr
}

func (s *memoryProjectionMaintainerStub) calls() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.statsCalls, s.rebuildCalls
}

type memoryProjectionAuditStore struct {
	mu     sync.Mutex
	events []storage.AuditEvent
}

func (s *memoryProjectionAuditStore) RecordAudit(_ context.Context, event storage.AuditEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
	return nil
}

func (s *memoryProjectionAuditStore) ListAudit(context.Context, storage.AuditFilter) ([]storage.AuditEvent, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	events := append([]storage.AuditEvent(nil), s.events...)
	return events, len(events), nil
}

func (s *memoryProjectionAuditStore) last(t *testing.T) storage.AuditEvent {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.events) == 0 {
		t.Fatal("expected memory projection rebuild audit event")
	}
	return s.events[len(s.events)-1]
}

func TestAdminMemoryProjectionStatsAndRebuild(t *testing.T) {
	stats := storage.MemoryProjectionStats{
		CanonicalEntries: 7, TagRows: 31, TaggedKeys: 13, OrphanTagRows: 2,
		KeySearchMissing: 3, ContentSearchMissing: 5, SearchProjectedEntries: 6,
	}
	maintainer := &memoryProjectionMaintainerStub{stats: stats}
	audit := &memoryProjectionAuditStore{}
	api := newMemoryProjectionTestServer(t, maintainer, audit)

	get := memoryProjectionRequest(t, api.Handler(), http.MethodGet, "/v1/admin/memory/projection", "admin")
	if get.Code != http.StatusOK {
		t.Fatalf("stats status=%d body=%s", get.Code, get.Body.String())
	}
	var statsResponse struct {
		Projection storage.MemoryProjectionStats `json:"projection"`
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

	rebuild := memoryProjectionRequest(t, api.Handler(), http.MethodPost, "/v1/admin/memory/projection/rebuild", "admin")
	if rebuild.Code != http.StatusOK {
		t.Fatalf("rebuild status=%d body=%s", rebuild.Code, rebuild.Body.String())
	}
	var rebuildResponse struct {
		Projection storage.MemoryProjectionStats `json:"projection"`
		Status     string                        `json:"status"`
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
	if event.Action != "memory.projection.rebuild" || event.Target != "memory/projection" {
		t.Fatalf("audit action=%q target=%q", event.Action, event.Target)
	}
	wantDetail := memoryProjectionAuditDetail(stats)
	if len(event.Detail) != len(wantDetail) {
		t.Fatalf("audit detail=%#v", event.Detail)
	}
	for key, want := range wantDetail {
		if event.Detail[key] != want {
			t.Fatalf("audit detail[%q]=%#v want=%#v", key, event.Detail[key], want)
		}
	}
}

func TestAdminMemoryProjectionAuthorizationDisabledAndErrors(t *testing.T) {
	for _, identity := range []string{"tenant-admin", "user"} {
		maintainer := &memoryProjectionMaintainerStub{}
		api := newMemoryProjectionTestServer(t, maintainer, nil)
		for _, request := range []struct{ method, path string }{
			{http.MethodGet, "/v1/admin/memory/projection"},
			{http.MethodPost, "/v1/admin/memory/projection/rebuild"},
		} {
			response := memoryProjectionRequest(t, api.Handler(), request.method, request.path, identity)
			if response.Code != http.StatusForbidden {
				t.Fatalf("%s %s identity=%s status=%d body=%s", request.method, request.path, identity, response.Code, response.Body.String())
			}
		}
		if statsCalls, rebuildCalls := maintainer.calls(); statsCalls != 0 || rebuildCalls != 0 {
			t.Fatalf("unauthorized calls stats=%d rebuild=%d", statsCalls, rebuildCalls)
		}
	}

	disabled := newMemoryProjectionTestServer(t, nil, nil)
	for _, request := range []struct{ method, path string }{
		{http.MethodGet, "/v1/admin/memory/projection"},
		{http.MethodPost, "/v1/admin/memory/projection/rebuild"},
	} {
		response := memoryProjectionRequest(t, disabled.Handler(), request.method, request.path, "admin")
		if response.Code != http.StatusNotImplemented {
			t.Fatalf("disabled %s status=%d body=%s", request.path, response.Code, response.Body.String())
		}
	}

	secretErr := errors.New("projection failure secret-key-value secret-content-value secret-tag-value")
	for _, test := range []struct {
		name       string
		maintainer *memoryProjectionMaintainerStub
		method     string
		path       string
	}{
		{"stats", &memoryProjectionMaintainerStub{statsErr: secretErr}, http.MethodGet, "/v1/admin/memory/projection"},
		{"rebuild", &memoryProjectionMaintainerStub{rebuildErr: secretErr}, http.MethodPost, "/v1/admin/memory/projection/rebuild"},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := memoryProjectionRequest(t, newMemoryProjectionTestServer(t, test.maintainer, nil).Handler(), test.method, test.path, "admin")
			if response.Code != http.StatusInternalServerError {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			for _, forbidden := range []string{"secret-key-value", "secret-content-value", "secret-tag-value"} {
				if strings.Contains(response.Body.String(), forbidden) {
					t.Fatalf("storage error leaked %q: %s", forbidden, response.Body.String())
				}
			}
			statsCalls, rebuildCalls := test.maintainer.calls()
			if test.name == "stats" && (statsCalls != 1 || rebuildCalls != 0) {
				t.Fatalf("stats error calls stats=%d rebuild=%d", statsCalls, rebuildCalls)
			}
			if test.name == "rebuild" && (statsCalls != 0 || rebuildCalls != 1) {
				t.Fatalf("rebuild error calls stats=%d rebuild=%d", statsCalls, rebuildCalls)
			}
		})
	}
}

func newMemoryProjectionTestServer(t *testing.T, maintainer storage.MemoryProjectionMaintainer, audit storage.AuditStore) *Server {
	t.Helper()
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	api, err := New(Config{
		Runtime: &core.Runtime{}, Sessions: core.NewMemorySessionStore(),
		MemoryProjection: maintainer, Audit: audit,
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

func memoryProjectionRequest(t *testing.T, handler http.Handler, method, path, identity string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, path, nil)
	request.Header.Set("X-Test-Identity", identity)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}
