package server_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	_ "modernc.org/sqlite"

	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/server"
	"github.com/whhhh1500/auto-agent/pkg/storage"
)

func TestMetricsHTTPAuthorizesTenantOverride(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", t.TempDir()+"/metrics.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	sessions, err := storage.OpenSQLSessionStore(ctx, db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	accounts, err := storage.NewSQLAccountStore(db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	platformEmail, platformPassword, created, err := storage.BootstrapAdmin(ctx, accounts, func(string, ...any) {})
	if err != nil || !created {
		t.Fatalf("bootstrap created=%t err=%v", created, err)
	}
	for _, tenant := range []struct{ id, name string }{{"acme", "Acme"}, {"other", "Other"}} {
		if err := accounts.CreateTenant(ctx, tenant.id, tenant.name); err != nil {
			t.Fatal(err)
		}
	}
	if err := accounts.CreateAccount(ctx, storage.Account{
		AccountID: "acme-metrics-admin", Email: "acme-metrics-admin@example.test", Role: storage.RoleAccountTenantAdmin,
		TenantID: "acme", Status: storage.AccountActive,
	}, "tenant-metrics-password"); err != nil {
		t.Fatal(err)
	}

	profiles := core.NewAgentProfileRegistry()
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	name := "Metrics Test Agent"
	model := core.ModelSelection{Provider: "mock", Model: "mock-1"}
	if err := profiles.Bind(core.AgentProfileLayer{Scope: global, ProfileID: "metrics.test", Name: &name, Model: &model}); err != nil {
		t.Fatal(err)
	}
	stats := &metricTenantSpy{}
	api, err := server.New(server.Config{
		Runtime: &core.Runtime{
			Capabilities: core.NewCapabilityRegistry(), Profiles: profiles, Policy: core.NewPolicyRegistry(),
			Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) { return core.MockLlmAdapter{}, nil }),
		},
		Sessions: sessions, Authenticator: server.AccountAuthenticator{Store: accounts}, Accounts: accounts, RunStats: stats,
	})
	if err != nil {
		t.Fatal(err)
	}
	base := httptest.NewServer(api.Handler())
	defer base.Close()
	platformToken := metricsLogin(t, base.URL, platformEmail, platformPassword)
	tenantToken := metricsLogin(t, base.URL, "acme-metrics-admin", "tenant-metrics-password")

	adminOverride := metricsRequest(t, base.URL+"/v1/admin/metrics?tenant=other", platformToken)
	if adminOverride.StatusCode != http.StatusOK {
		t.Fatalf("platform tenant override status=%d body=%s", adminOverride.StatusCode, metricsBody(t, adminOverride))
	}
	_ = adminOverride.Body.Close()
	if got := stats.tenants(); len(got) != 1 || got[0] != "other" {
		t.Fatalf("platform metrics tenants=%q, want [other]", got)
	}

	tenantOverride := metricsRequest(t, base.URL+"/v1/admin/metrics?tenant=other", tenantToken)
	if tenantOverride.StatusCode != http.StatusForbidden {
		t.Fatalf("tenant cross-tenant override status=%d body=%s", tenantOverride.StatusCode, metricsBody(t, tenantOverride))
	}
	_ = tenantOverride.Body.Close()
	if got := stats.tenants(); len(got) != 1 {
		t.Fatalf("forbidden tenant override called metrics store: %q", got)
	}

	tenantOwn := metricsRequest(t, base.URL+"/v1/admin/metrics", tenantToken)
	if tenantOwn.StatusCode != http.StatusOK {
		t.Fatalf("tenant own metrics status=%d body=%s", tenantOwn.StatusCode, metricsBody(t, tenantOwn))
	}
	_ = tenantOwn.Body.Close()
	if got := stats.tenants(); len(got) != 2 || got[1] != "acme" {
		t.Fatalf("tenant metrics tenants=%q, want [other acme]", got)
	}
}

type metricTenantSpy struct {
	mu          sync.Mutex
	tenantsSeen []string
}

func (*metricTenantSpy) RecordRunStat(context.Context, storage.RunStat) error { return nil }

func (s *metricTenantSpy) Metrics(_ context.Context, tenant string) (storage.RunMetrics, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tenantsSeen = append(s.tenantsSeen, tenant)
	return storage.RunMetrics{TotalRuns: 1}, nil
}

func (s *metricTenantSpy) tenants() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.tenantsSeen...)
}

func metricsLogin(t *testing.T, baseURL, email, password string) string {
	t.Helper()
	response := metricsJSON(t, http.MethodPost, baseURL+"/v1/auth/login", map[string]string{"email": email, "password": password}, "")
	if response.StatusCode != http.StatusOK {
		t.Fatalf("metrics login status=%d body=%s", response.StatusCode, metricsBody(t, response))
	}
	defer response.Body.Close()
	var payload struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	if payload.Token == "" {
		t.Fatal("metrics login returned empty token")
	}
	return payload.Token
}

func metricsRequest(t *testing.T, url, token string) *http.Response {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func metricsJSON(t *testing.T, method, url string, value any, token string) *http.Response {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(method, url, http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	request.Body = io.NopCloser(bytes.NewReader(encoded))
	request.Header.Set("Content-Type", "application/json")
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func metricsBody(t *testing.T, response *http.Response) string {
	t.Helper()
	defer response.Body.Close()
	content, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(content)
}
