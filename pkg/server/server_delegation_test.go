package server_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/extensions/subagent"
	"github.com/whhhh1500/auto-agent/pkg/server"
)

func TestAdminDelegationsTenantIsolationAndMetadataOnly(t *testing.T) {
	root := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	links := subagent.NewMemoryDelegationLinkStore()
	if _, _, err := links.PutIfAbsent(context.Background(), subagent.Link{
		ParentSessionID: "sess-parent", ParentRunID: "run-parent", ParentCallID: "call-delegate",
		ChildSessionID: "sess-child", ChildRunID: "run-child", TenantID: "acme", SubjectID: "alice", Depth: 1,
	}); err != nil {
		t.Fatal(err)
	}
	api, err := server.New(server.Config{
		Runtime: &core.Runtime{}, Sessions: core.NewMemorySessionStore(),
		Authenticator: tenantOperatorAuthenticator(root.Segments()), DelegationLinks: links, DelegationLinkCatalog: links,
	})
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(api.Handler())
	defer httpServer.Close()
	request, err := http.NewRequest(http.MethodGet, httpServer.URL+"/v1/admin/delegations?tenant=acme", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("X-Harness-Subject", "alice")
	request.Header.Set("X-Harness-Tenant", "acme")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body := readBody(t, response)
	if response.StatusCode != http.StatusOK || !strings.Contains(body, "sess-child") || strings.Contains(body, "prompt") || strings.Contains(body, "args") || strings.Contains(body, "result") {
		t.Fatalf("unexpected delegation response: status=%d body=%s", response.StatusCode, body)
	}
	request, err = http.NewRequest(http.MethodGet, httpServer.URL+"/v1/admin/delegations?tenant=other", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("X-Harness-Subject", "alice")
	request.Header.Set("X-Harness-Tenant", "acme")
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("cross-tenant query status=%d body=%s", response.StatusCode, readBody(t, response))
	}
}
