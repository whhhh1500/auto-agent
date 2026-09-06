package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	cryptoexample "github.com/cc-auto-agent/harness-core/examples/crypto"
	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/server"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
)

func TestServerPreservesMultiTurnSessionAndOwnership(t *testing.T) {
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
		FastRouters: core.FastRouterResolverFunc(func(context.Context, *core.AgentProfileSnapshot) (*core.FastRouter, error) {
			return cryptoexample.FastRouter(), nil
		}),
	}
	api, err := server.New(server.Config{
		Runtime: runtime, Sessions: core.NewMemorySessionStore(),
		Authenticator: server.HeaderAuthenticator{
			Root: product.Segments(), DefaultGrants: core.NewPermissionSet(core.PermRead),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(api.Handler())
	defer httpServer.Close()

	created := doJSON(t, http.MethodPost, httpServer.URL+"/v1/sessions", "alice", `{"profile_id":"crypto.agent.analyst"}`)
	if created.StatusCode != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", created.StatusCode, readBody(t, created))
	}
	var session struct {
		ID string `json:"id"`
	}
	decodeBody(t, created, &session)
	if session.ID == "" {
		t.Fatal("server returned an empty session id")
	}

	for _, message := range []string{"帮我看盘 BTC", "发现新币"} {
		response := doJSON(t, http.MethodPost, httpServer.URL+"/v1/sessions/"+session.ID+"/runs", "alice", `{"message":"`+message+`"}`)
		body := readBody(t, response)
		if response.StatusCode != http.StatusOK || !strings.Contains(body, "event: tool/call") || !strings.Contains(body, "event: run/end") {
			t.Fatalf("unexpected run response: status=%d body=%s", response.StatusCode, body)
		}
	}

	eventsResponse := doJSON(t, http.MethodGet, httpServer.URL+"/v1/sessions/"+session.ID+"/events", "alice", "")
	var eventLog struct {
		Version int                 `json:"version"`
		Events  []core.SessionEvent `json:"events"`
	}
	decodeBody(t, eventsResponse, &eventLog)
	if eventLog.Version != 12 || len(eventLog.Events) != 12 || eventLog.Events[6].Seq != 6 {
		t.Fatalf("session was not appended across runs: %#v", eventLog)
	}

	otherUser := doJSON(t, http.MethodGet, httpServer.URL+"/v1/sessions/"+session.ID, "bob", "")
	if otherUser.StatusCode != http.StatusNotFound {
		t.Fatalf("cross-user access status=%d body=%s", otherUser.StatusCode, readBody(t, otherUser))
	}
}

func TestCreateSessionUsesConfiguredDefaultOnlyWhenProfileIsOmitted(t *testing.T) {
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product, _ := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "crypto"})
	capabilities := core.NewCapabilityRegistry()
	profiles := core.NewAgentProfileRegistry()
	if err := cryptoexample.RegisterProfiles(profiles, product); err != nil {
		t.Fatal(err)
	}
	altName := "Alternative profile"
	altModel := core.ModelSelection{Provider: "mock", Model: "mock-alt"}
	if err := profiles.Bind(core.AgentProfileLayer{
		Scope: product, ProfileID: "crypto.agent.alt", Name: &altName, Model: &altModel,
	}); err != nil {
		t.Fatal(err)
	}
	runtime := &core.Runtime{
		Capabilities: capabilities, Profiles: profiles,
		Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) {
			return core.MockLlmAdapter{}, nil
		}),
	}
	auth := server.HeaderAuthenticator{
		Root: product.Segments(), DefaultGrants: core.NewPermissionSet(core.PermRead),
	}
	api, err := server.New(server.Config{
		Runtime: runtime, Sessions: core.NewMemorySessionStore(), Authenticator: auth,
		DefaultProfileID: cryptoexample.ProfileAnalyst,
	})
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(api.Handler())
	defer httpServer.Close()

	omitted := doJSON(t, http.MethodPost, httpServer.URL+"/v1/sessions", "alice", `{"metadata":{"source":"default"}}`)
	if omitted.StatusCode != http.StatusCreated {
		t.Fatalf("omitted profile status=%d body=%s", omitted.StatusCode, readBody(t, omitted))
	}
	var omittedSession struct {
		ProfileID string `json:"profile_id"`
	}
	decodeBody(t, omitted, &omittedSession)
	if omittedSession.ProfileID != cryptoexample.ProfileAnalyst {
		t.Fatalf("omitted profile=%q want %q", omittedSession.ProfileID, cryptoexample.ProfileAnalyst)
	}

	explicit := doJSON(t, http.MethodPost, httpServer.URL+"/v1/sessions", "alice", `{"profile_id":"crypto.agent.alt","extra":"kept-compatible"}`)
	if explicit.StatusCode != http.StatusCreated {
		t.Fatalf("explicit profile status=%d body=%s", explicit.StatusCode, readBody(t, explicit))
	}
	var explicitSession struct {
		ProfileID string `json:"profile_id"`
	}
	decodeBody(t, explicit, &explicitSession)
	if explicitSession.ProfileID != "crypto.agent.alt" {
		t.Fatalf("explicit profile=%q", explicitSession.ProfileID)
	}

	unknown := doJSON(t, http.MethodPost, httpServer.URL+"/v1/sessions", "alice", `{"profile_id":"does.not.exist"}`)
	if unknown.StatusCode != http.StatusBadRequest {
		t.Fatalf("unknown explicit profile status=%d body=%s", unknown.StatusCode, readBody(t, unknown))
	}

	withoutDefault, err := server.New(server.Config{
		Runtime: runtime, Sessions: core.NewMemorySessionStore(), Authenticator: auth,
	})
	if err != nil {
		t.Fatal(err)
	}
	withoutDefaultHTTP := httptest.NewServer(withoutDefault.Handler())
	defer withoutDefaultHTTP.Close()
	missing := doJSON(t, http.MethodPost, withoutDefaultHTTP.URL+"/v1/sessions", "alice", `{}`)
	if missing.StatusCode != http.StatusBadRequest || !strings.Contains(readBody(t, missing), "profile_id is required") {
		t.Fatalf("missing profile without default status=%d", missing.StatusCode)
	}
}

func doJSON(t *testing.T, method, url, subject, body string) *http.Response {
	t.Helper()
	request, err := http.NewRequest(method, url, bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Harness-Subject", subject)
	request.Header.Set("X-Harness-Tenant", "acme")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func tenantOperatorAuthenticator(root []core.ScopeRef) server.Authenticator {
	return server.AuthenticatorFunc(func(r *http.Request) (core.Principal, error) {
		tenant := r.Header.Get("X-Harness-Tenant")
		if tenant == "" {
			tenant = "acme"
		}
		scope, err := core.NewScopePath(append(append([]core.ScopeRef(nil), root...), core.ScopeRef{Kind: core.ScopeTenant, ID: tenant})...)
		if err != nil {
			return core.Principal{}, err
		}
		return core.Principal{
			SubjectID: r.Header.Get("X-Harness-Subject"), TenantID: tenant, Scope: scope,
			Grants:     core.NewPermissionSet(core.PermRead, core.PermWrite),
			Attributes: map[string]string{"role": storage.RoleAccountTenantAdmin},
		}, nil
	})
}

func decodeBody(t *testing.T, response *http.Response, target any) {
	t.Helper()
	defer response.Body.Close()
	if err := json.NewDecoder(response.Body).Decode(target); err != nil {
		t.Fatal(err)
	}
}

func readBody(t *testing.T, response *http.Response) string {
	t.Helper()
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
