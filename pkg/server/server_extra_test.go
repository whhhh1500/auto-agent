package server_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	cryptoexample "github.com/whhhh1500/auto-agent/examples/crypto"
	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/server"
)

func newTestHTTPServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	httpServer := httptest.NewServer(handler)
	t.Cleanup(httpServer.Close)
	return httpServer
}

func newCatalogRuntime(t *testing.T) (*core.Runtime, core.ScopePath) {
	t.Helper()
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product, _ := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "crypto"})
	capabilities := core.NewCapabilityRegistry()
	profiles := core.NewAgentProfileRegistry()
	if err := cryptoexample.RegisterProductCapabilities(capabilities, product); err != nil {
		t.Fatal(err)
	}
	if err := cryptoexample.RegisterProfiles(profiles, product); err != nil {
		t.Fatal(err)
	}
	runtime := &core.Runtime{
		Capabilities: capabilities, Profiles: profiles,
		Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) {
			return core.MockLlmAdapter{}, nil
		}),
	}
	return runtime, product
}

func doJSONWithHeaders(t *testing.T, method, url, subject string, headers map[string]string, body string) *http.Response {
	t.Helper()
	request, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	// Caller-supplied headers win, so tests can impersonate other tenants.
	if request.Header.Get("X-Harness-Subject") == "" {
		request.Header.Set("X-Harness-Subject", subject)
	}
	if request.Header.Get("X-Harness-Tenant") == "" {
		request.Header.Set("X-Harness-Tenant", "acme")
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func TestCatalogEndpointsListScopedEntries(t *testing.T) {
	runtime, product := newCatalogRuntime(t)
	api, err := server.New(server.Config{
		Runtime: runtime, Sessions: core.NewMemorySessionStore(),
		Authenticator: server.HeaderAuthenticator{
			Root:          product.Segments(),
			DefaultGrants: core.NewPermissionSet(core.PermRead),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	httpServer := newTestHTTPServer(t, api.Handler())

	capabilityResponse := doJSON(t, http.MethodGet, httpServer.URL+"/v1/capabilities", "alice", "")
	body := readBody(t, capabilityResponse)
	if capabilityResponse.StatusCode != http.StatusOK || !strings.Contains(body, "crypto.market.quote") {
		t.Fatalf("capability catalog unexpected: status=%d body=%s", capabilityResponse.StatusCode, body)
	}

	profileResponse := doJSON(t, http.MethodGet, httpServer.URL+"/v1/profiles", "alice", "")
	profileBody := readBody(t, profileResponse)
	if profileResponse.StatusCode != http.StatusOK || !strings.Contains(profileBody, "crypto.agent.analyst") {
		t.Fatalf("profile catalog unexpected: status=%d body=%s", profileResponse.StatusCode, profileBody)
	}
}

func TestGrantsHeaderIgnoredByDefault(t *testing.T) {
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product, _ := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "crypto"})
	capabilities := core.NewCapabilityRegistry()
	profiles := core.NewAgentProfileRegistry()
	manifest := core.CapabilityManifest{
		ID: "crm.send", Version: "1.0.0", Name: "Send CRM message", Kind: core.KindConnector,
		RequiredPermissions: []core.Permission{core.PermWrite},
		Tool:                &core.ToolExposure{},
	}
	if err := capabilities.Register(product, staticContentTool{manifest: manifest, content: "sent"}); err != nil {
		t.Fatal(err)
	}
	name := "Agent"
	model := core.ModelSelection{Provider: "mock", Model: "mock-1"}
	if err := profiles.Bind(core.AgentProfileLayer{
		Scope: product, ProfileID: "crypto.agent.crm", Name: &name, Model: &model,
		AddCapabilities: []string{"crm.send"},
	}); err != nil {
		t.Fatal(err)
	}
	runtime := &core.Runtime{
		Capabilities: capabilities, Profiles: profiles,
		Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) {
			return core.MockLlmAdapter{}, nil
		}),
	}
	defaultAPI, err := server.New(server.Config{
		Runtime: runtime, Sessions: core.NewMemorySessionStore(),
		Authenticator: server.HeaderAuthenticator{
			Root:          product.Segments(),
			DefaultGrants: core.NewPermissionSet(core.PermRead),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defaultServer := newTestHTTPServer(t, defaultAPI.Handler())

	// The header claims data.write, but a default authenticator ignores it. The
	// profile still composes exactly, then the final execution snapshot removes
	// the write-gated capability before schemas or tool execution can see it.
	denied := runWithGrants(t, defaultServer.URL, "crypto.agent.crm", "data.write")
	if strings.Contains(denied, "profile_capability_mismatch") || strings.Contains(denied, "tool/call") || strings.Contains(denied, "data.write") || strings.Contains(denied, "sent") {
		t.Fatalf("client-supplied grants were trusted: %q", denied)
	}

	// Opting in makes the header effective for simulated local grants.
	optIn, err := server.New(server.Config{
		Runtime: runtime, Sessions: core.NewMemorySessionStore(),
		Authenticator: server.HeaderAuthenticator{
			Root:              product.Segments(),
			DefaultGrants:     core.NewPermissionSet(core.PermRead),
			AllowGrantsHeader: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	optInServer := newTestHTTPServer(t, optIn.Handler())
	granted := runWithGrants(t, optInServer.URL, "crypto.agent.crm", "data.write")
	if !strings.Contains(granted, "Capability result: sent") {
		t.Fatalf("AllowGrantsHeader=true did not apply the header grants: %q", granted)
	}
}

func runWithGrants(t *testing.T, baseURL, profileID, grants string) string {
	t.Helper()
	headers := map[string]string{"Content-Type": "application/json"}
	if grants != "" {
		headers["X-Harness-Grants"] = grants
	}
	created := doJSONWithHeaders(t, http.MethodPost, baseURL+"/v1/sessions", "alice", headers, `{"profile_id":"`+profileID+`"}`)
	var session struct {
		ID string `json:"id"`
	}
	decodeBody(t, created, &session)
	run := doJSONWithHeaders(t, http.MethodPost, baseURL+"/v1/sessions/"+session.ID+"/runs", "alice", headers, `{"message":"go"}`)
	return readBody(t, run)
}

type staticContentTool struct {
	manifest core.CapabilityManifest
	content  string
}

func (s staticContentTool) Manifest() core.CapabilityManifest { return s.manifest }

func (s staticContentTool) Execute(context.Context, core.CapabilityRequest) (core.CapabilityResult, error) {
	return core.CapabilityResult{Content: s.content, OK: true}, nil
}

type blockingTool struct{ started chan struct{} }

func (b blockingTool) Manifest() core.CapabilityManifest {
	return core.CapabilityManifest{
		ID: "slow.block", Version: "1.0.0", Name: "Blocking tool", Kind: core.KindTool,
		RequiredPermissions: []core.Permission{core.PermRead},
		Tool:                &core.ToolExposure{},
	}
}

func (b blockingTool) Execute(ctx context.Context, _ core.CapabilityRequest) (core.CapabilityResult, error) {
	close(b.started)
	<-ctx.Done()
	return core.CapabilityResult{Content: "cancelled", OK: false}, ctx.Err()
}

func TestCancelStopsActiveRun(t *testing.T) {
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product, _ := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "crypto"})
	capabilities := core.NewCapabilityRegistry()
	profiles := core.NewAgentProfileRegistry()
	started := make(chan struct{})
	if err := capabilities.Register(product, blockingTool{started: started}); err != nil {
		t.Fatal(err)
	}
	name := "Agent"
	model := core.ModelSelection{Provider: "mock", Model: "mock-1"}
	if err := profiles.Bind(core.AgentProfileLayer{
		Scope: product, ProfileID: "crypto.agent.block", Name: &name, Model: &model,
		AddCapabilities: []string{"slow.block"},
	}); err != nil {
		t.Fatal(err)
	}
	runtime := &core.Runtime{
		Capabilities: capabilities, Profiles: profiles,
		Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) {
			return core.MockLlmAdapter{}, nil
		}),
	}
	api, err := server.New(server.Config{
		Runtime: runtime, Sessions: core.NewMemorySessionStore(),
		Authenticator: server.HeaderAuthenticator{
			Root:          product.Segments(),
			DefaultGrants: core.NewPermissionSet(core.PermRead),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	httpServer := newTestHTTPServer(t, api.Handler())

	created := doJSON(t, http.MethodPost, httpServer.URL+"/v1/sessions", "alice", `{"profile_id":"crypto.agent.block"}`)
	var session struct {
		ID string `json:"id"`
	}
	decodeBody(t, created, &session)

	runDone := make(chan string, 1)
	go func() {
		request, err := http.NewRequest(http.MethodPost, httpServer.URL+"/v1/sessions/"+session.ID+"/runs", strings.NewReader(`{"message":"run"}`))
		if err != nil {
			runDone <- "error: " + err.Error()
			return
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("X-Harness-Subject", "alice")
		request.Header.Set("X-Harness-Tenant", "acme")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			runDone <- "error: " + err.Error()
			return
		}
		runDone <- readBody(t, response)
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("blocking tool never started")
	}
	cancelled := doJSON(t, http.MethodPost, httpServer.URL+"/v1/sessions/"+session.ID+"/cancel", "alice", "")
	if cancelled.StatusCode != http.StatusAccepted {
		t.Fatalf("cancel status=%d body=%s", cancelled.StatusCode, readBody(t, cancelled))
	}
	select {
	case body := <-runDone:
		if !strings.Contains(body, "run/error") || !strings.Contains(body, "run/end") {
			t.Fatalf("cancelled run did not reach terminal events: %s", body)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled run never returned")
	}

	conflict := doJSON(t, http.MethodPost, httpServer.URL+"/v1/sessions/"+session.ID+"/cancel", "alice", "")
	if conflict.StatusCode != http.StatusConflict {
		t.Fatalf("cancel without active run must conflict, got %d", conflict.StatusCode)
	}
}
