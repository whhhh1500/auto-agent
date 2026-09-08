package server_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	cryptoexample "github.com/whhhh1500/auto-agent/examples/crypto"
	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/server"
)

func newAdminServer(t *testing.T) *httptest.Server {
	t.Helper()
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product, _ := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "crypto"})
	capabilities := core.NewCapabilityRegistry()
	profiles := core.NewAgentProfileRegistry()
	credentials := core.NewCredentialRegistry()
	if err := cryptoexample.RegisterProductCapabilities(capabilities, product); err != nil {
		t.Fatal(err)
	}
	if err := cryptoexample.RegisterProfiles(profiles, product); err != nil {
		t.Fatal(err)
	}
	runtime := &core.Runtime{
		Capabilities: capabilities, Profiles: profiles, Credentials: credentials,
		Policy: core.NewPolicyRegistry(),
		Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) {
			return core.MockLlmAdapter{}, nil
		}),
	}
	api, err := server.New(server.Config{
		Runtime: runtime, Sessions: core.NewMemorySessionStore(),
		Authenticator: tenantOperatorAuthenticator(product.Segments()),
	})
	if err != nil {
		t.Fatal(err)
	}
	return newTestHTTPServer(t, api.Handler())
}

func TestAdminOverview(t *testing.T) {
	httpServer := newAdminServer(t)
	response := doJSON(t, http.MethodGet, httpServer.URL+"/v1/admin/overview", "alice", "")
	body := readBody(t, response)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("overview status=%d body=%s", response.StatusCode, body)
	}
	for _, want := range []string{`"capabilities":3`, `"profiles":1`, `"sessions_listable":false`, `"leases":false`} {
		if !strings.Contains(body, want) {
			t.Fatalf("overview missing %s: %s", want, body)
		}
	}
}

func TestAdminPolicyBindListUnbind(t *testing.T) {
	httpServer := newAdminServer(t)
	base := httpServer.URL
	scope := `{"scope":[{"kind":"global","id":"global"},{"kind":"product","id":"crypto"},{"kind":"tenant","id":"acme"}],
		"deny_permissions":["data.write"],"max_tool_calls":2}`

	created := doJSON(t, http.MethodPost, base+"/v1/admin/policies", "alice", scope)
	if created.StatusCode != http.StatusCreated {
		t.Fatalf("bind status=%d body=%s", created.StatusCode, readBody(t, created))
	}
	var bound struct {
		BindingID string `json:"binding_id"`
	}
	decodeBody(t, created, &bound)

	listed := readBody(t, doJSON(t, http.MethodGet, base+"/v1/admin/policies", "alice", ""))
	if !strings.Contains(listed, "data.write") || !strings.Contains(listed, `"max_tool_calls":2`) {
		t.Fatalf("policy layer missing from listing: %s", listed)
	}

	if unbound := doJSON(t, http.MethodDelete, base+"/v1/admin/bindings/"+bound.BindingID, "alice", ""); unbound.StatusCode != http.StatusOK {
		t.Fatalf("unbind status=%d", unbound.StatusCode)
	}
	listed = readBody(t, doJSON(t, http.MethodGet, base+"/v1/admin/policies", "alice", ""))
	if strings.Contains(listed, "data.write") {
		t.Fatalf("unbind did not remove the layer: %s", listed)
	}

	// Cross-tenant operator is forbidden.
	crossTenant := doJSONWithHeaders(t, http.MethodPost, base+"/v1/admin/policies", "bob",
		map[string]string{"Content-Type": "application/json", "X-Harness-Tenant": "otherco"},
		`{"scope":[{"kind":"global","id":"global"},{"kind":"product","id":"crypto"},{"kind":"tenant","id":"acme"}],
		"deny_permissions":["data.write"]}`)
	if crossTenant.StatusCode != http.StatusForbidden {
		t.Fatalf("cross-tenant bind status=%d", crossTenant.StatusCode)
	}
}

func TestAdminCredentialsNeverEchoValues(t *testing.T) {
	httpServer := newAdminServer(t)
	base := httpServer.URL
	bind := doJSON(t, http.MethodPost, base+"/v1/admin/credentials", "alice", `{
		"scope":[{"kind":"global","id":"global"},{"kind":"product","id":"crypto"},{"kind":"tenant","id":"acme"}],
		"ref":"crm.api_key","kind":"static","value":"super-secret-value","mode":"provide"
	}`)
	if bind.StatusCode != http.StatusCreated {
		t.Fatalf("bind status=%d body=%s", bind.StatusCode, readBody(t, bind))
	}

	// The listing surfaces must show the reference but never the value.
	listing := readBody(t, doJSON(t, http.MethodGet, base+"/v1/admin/credentials", "alice", ""))
	if !strings.Contains(listing, "crm.api_key") {
		t.Fatalf("listing must show the ref: %s", listing)
	}
	if strings.Contains(listing, "super-secret-value") {
		t.Fatal("credential value leaked through the listing")
	}
	bindings := readBody(t, doJSON(t, http.MethodGet, base+"/v1/admin/bindings", "alice", ""))
	if strings.Contains(bindings, "super-secret-value") {
		t.Fatal("credential value leaked through the bindings listing")
	}
}

func TestAdminCapabilityDisableEnable(t *testing.T) {
	httpServer := newAdminServer(t)
	base := httpServer.URL
	scope := `{"scope":[{"kind":"global","id":"global"},{"kind":"product","id":"crypto"},{"kind":"tenant","id":"acme"}]}`

	caps := readBody(t, doJSON(t, http.MethodGet, base+"/v1/capabilities", "alice", ""))
	if !strings.Contains(caps, "crypto.market.quote") {
		t.Fatalf("fixture capability missing: %s", caps)
	}

	disabled := doJSON(t, http.MethodPost, base+"/v1/admin/capabilities/crypto.market.quote/disable", "alice", scope)
	if disabled.StatusCode != http.StatusCreated {
		t.Fatalf("disable status=%d body=%s", disabled.StatusCode, readBody(t, disabled))
	}
	caps = readBody(t, doJSON(t, http.MethodGet, base+"/v1/capabilities", "alice", ""))
	if strings.Contains(caps, "crypto.market.quote") {
		t.Fatalf("disabled capability still listed: %s", caps)
	}

	var bound struct {
		BindingID string `json:"binding_id"`
	}
	decodeBody(t, disabled, &bound)
	enabled := doJSON(t, http.MethodPost, base+"/v1/admin/capabilities/crypto.market.quote/enable", "alice",
		`{"binding_id":"`+bound.BindingID+`"}`)
	if enabled.StatusCode != http.StatusOK {
		t.Fatalf("enable status=%d body=%s", enabled.StatusCode, readBody(t, enabled))
	}
	caps = readBody(t, doJSON(t, http.MethodGet, base+"/v1/capabilities", "alice", ""))
	if !strings.Contains(caps, "crypto.market.quote") {
		t.Fatalf("enabled capability not restored: %s", caps)
	}
}

func TestConsoleServesAppAndAssets(t *testing.T) {
	httpServer := newAdminServer(t)
	index := doJSON(t, http.MethodGet, httpServer.URL+"/console/", "alice", "")
	indexBody := readBody(t, index)
	if index.StatusCode != http.StatusOK || !strings.Contains(indexBody, "auto-agent Console") {
		t.Fatalf("console index wrong: status=%d", index.StatusCode)
	}
	match := regexp.MustCompile(`src="(/console/assets/alpine\.min\.js\?v=[0-9a-f]{64})"`).FindStringSubmatch(indexBody)
	if len(match) != 2 {
		t.Fatal("console index missing versioned Alpine asset")
	}
	asset := doJSON(t, http.MethodGet, httpServer.URL+match[1], "alice", "")
	assetBody := readBody(t, asset)
	if asset.StatusCode != http.StatusOK || !strings.Contains(assetHeader(t, asset), "immutable") {
		t.Fatalf("asset serving wrong: status=%d", asset.StatusCode)
	}
	if len(assetBody) < 10000 {
		t.Fatalf("alpine asset suspiciously small: %d bytes", len(assetBody))
	}
	stable := doJSON(t, http.MethodGet, httpServer.URL+"/console/assets/alpine.min.js", "alice", "")
	defer stable.Body.Close()
	if stable.StatusCode != http.StatusOK || assetHeader(t, stable) != "no-cache" {
		t.Fatalf("stable asset serving wrong: status=%d Cache-Control=%q", stable.StatusCode, assetHeader(t, stable))
	}
}

func assetHeader(t *testing.T, response *http.Response) string {
	t.Helper()
	return response.Header.Get("Cache-Control")
}
