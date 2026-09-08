package server_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	core "github.com/whhhh1500/auto-agent/pkg/core"
	executionsandbox "github.com/whhhh1500/auto-agent/pkg/execution/sandbox"
	"github.com/whhhh1500/auto-agent/pkg/server"
	"github.com/whhhh1500/auto-agent/pkg/storage"
)

type sandboxDiscoveryProvider struct {
	probes      atomic.Int32
	unavailable bool
	panicProbe  bool
}

func (*sandboxDiscoveryProvider) ID() executionsandbox.ProviderID { return "test-local" }

func (provider *sandboxDiscoveryProvider) Probe(context.Context) executionsandbox.AssuranceReport {
	provider.probes.Add(1)
	if provider.panicProbe {
		panic("secret=/private/sandbox/path")
	}
	if provider.unavailable {
		return executionsandbox.AssuranceReport{UnavailableCause: "secret=/private/sandbox/path"}
	}
	return executionsandbox.AssuranceReport{
		Available:         true,
		Actual:            executionsandbox.Assurance{Level: executionsandbox.AssuranceProcess, SharedKernel: true},
		Network:           executionsandbox.NetworkHost,
		SupportedNetworks: []executionsandbox.NetworkPolicy{executionsandbox.NetworkDisabled, executionsandbox.NetworkHost},
		LimitsEnforced:    true, MountsEnforced: true,
	}
}

func (*sandboxDiscoveryProvider) Start(context.Context, executionsandbox.SessionSpec) (executionsandbox.Session, error) {
	return nil, executionsandbox.ErrUnavailable
}

func newSandboxDiscoveryServer(t *testing.T, registry *executionsandbox.Registry) *httptest.Server {
	t.Helper()
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	api, err := server.New(server.Config{
		Runtime: &core.Runtime{}, Sessions: core.NewMemorySessionStore(), SandboxProviders: registry,
		Authenticator: server.AuthenticatorFunc(func(request *http.Request) (core.Principal, error) {
			return core.Principal{SubjectID: "admin", TenantID: "system", Scope: global, Attributes: map[string]string{"role": request.Header.Get("X-Test-Role")}}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	return httptest.NewServer(api.Handler())
}

func sandboxDiscoveryRegistry(t *testing.T, provider *sandboxDiscoveryProvider) *executionsandbox.Registry {
	t.Helper()
	registry, err := executionsandbox.NewRegistry(1, executionsandbox.Registration{
		Metadata: executionsandbox.Metadata{
			ID: "test-local", Version: "1", ImplementationRevision: "test-revision-1",
			Assurance: executionsandbox.Assurance{Level: executionsandbox.AssuranceProcess, SharedKernel: true},
		},
		Provider: provider,
	})
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

func TestSandboxProvidersRequirePlatformAdminAndReturnOnlySafeProbeFacts(t *testing.T) {
	provider := &sandboxDiscoveryProvider{unavailable: true}
	httpServer := newSandboxDiscoveryServer(t, sandboxDiscoveryRegistry(t, provider))
	defer httpServer.Close()

	request, err := http.NewRequest(http.MethodGet, httpServer.URL+"/v1/admin/sandbox/providers", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("X-Test-Role", storage.RoleAccountTenantAdmin)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusForbidden || provider.probes.Load() != 0 {
		t.Fatalf("tenant admin status=%d probes=%d", response.StatusCode, provider.probes.Load())
	}

	request, err = http.NewRequest(http.MethodGet, httpServer.URL+"/v1/admin/sandbox/providers", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("X-Test-Role", storage.RoleAccountAdmin)
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body := readBody(t, response)
	if response.StatusCode != http.StatusOK || provider.probes.Load() != 1 {
		t.Fatalf("platform admin status=%d probes=%d body=%s", response.StatusCode, provider.probes.Load(), body)
	}
	for _, required := range []string{"\"id\":\"test-local\"", "\"implementation_revision\":\"test-revision-1\"", "\"available\":false", "sandbox provider unavailable", "limits_enforced", "mounts_enforced"} {
		if !strings.Contains(body, required) {
			t.Fatalf("discovery response missing %q: %s", required, body)
		}
	}
	for _, forbidden := range []string{"private", "secret", "path"} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("discovery response leaked %q: %s", forbidden, body)
		}
	}
}

func TestSandboxProvidersReturn501WithoutInjectedRegistry(t *testing.T) {
	httpServer := newSandboxDiscoveryServer(t, nil)
	defer httpServer.Close()
	request, err := http.NewRequest(http.MethodGet, httpServer.URL+"/v1/admin/sandbox/providers", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("X-Test-Role", storage.RoleAccountAdmin)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body := readBody(t, response)
	if response.StatusCode != http.StatusNotImplemented || !strings.Contains(body, "sandbox providers are not configured") {
		t.Fatalf("status=%d body=%s", response.StatusCode, body)
	}
}

func TestSandboxProvidersSanitizeProbePanic(t *testing.T) {
	provider := &sandboxDiscoveryProvider{panicProbe: true}
	httpServer := newSandboxDiscoveryServer(t, sandboxDiscoveryRegistry(t, provider))
	defer httpServer.Close()
	request, err := http.NewRequest(http.MethodGet, httpServer.URL+"/v1/admin/sandbox/providers", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("X-Test-Role", storage.RoleAccountAdmin)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body := readBody(t, response)
	if response.StatusCode != http.StatusOK || !strings.Contains(body, "sandbox provider unavailable") || strings.Contains(body, "private") || strings.Contains(body, "secret") {
		t.Fatalf("panic discovery status=%d body=%s", response.StatusCode, body)
	}
}
