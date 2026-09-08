package server_test

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"

	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/storage"
)

func TestAdminProfilePutGetIsIdempotentAndDurable(t *testing.T) {
	f := newControlFixture(t)
	token := f.login(f.adminEmail, f.adminPass)
	scope := `[{"kind":"global","id":"global"}]`
	body := map[string]any{
		"scope": json.RawMessage(scope),
		"layer": map[string]any{
			"profile_id": "test.agent",
			"name":       "Edited agent",
			"model":      map[string]string{"provider": "mock", "model": "mock-2"},
			"put_fragments": []map[string]string{{
				"id": "admin.instructions", "section": "instructions", "content": "Be concise.",
			}},
		},
	}
	put := f.do("PUT", "/v1/admin/profiles/test.agent", body, token)
	if put.StatusCode != http.StatusOK {
		t.Fatalf("put status=%d body=%s", put.StatusCode, readBody(t, put))
	}
	first := readBody(t, put)
	if !strings.Contains(first, `"name":"Edited agent"`) || !strings.Contains(first, `"admin.instructions"`) {
		t.Fatalf("effective profile missing update: %s", first)
	}

	// The same PUT must not create another journal row.
	second := f.do("PUT", "/v1/admin/profiles/test.agent", body, token)
	if second.StatusCode != http.StatusOK {
		t.Fatalf("idempotent put status=%d body=%s", second.StatusCode, readBody(t, second))
	}
	records, err := f.journal.List(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var profileRecords int
	for _, record := range records {
		if record.Kind == "profile" {
			profileRecords++
		}
	}
	if profileRecords != 1 {
		t.Fatalf("idempotent PUT created %d profile journal rows", profileRecords)
	}

	query := "/v1/admin/profiles/test.agent?scope=" + url.QueryEscape(scope)
	get := f.do("GET", query, nil, token)
	if get.StatusCode != http.StatusOK {
		t.Fatalf("get status=%d body=%s", get.StatusCode, readBody(t, get))
	}
	if body := readBody(t, get); !strings.Contains(body, `"profile_id":"test.agent"`) || !strings.Contains(body, `"Edited agent"`) {
		t.Fatalf("get response missing editable/effective profile: %s", body)
	}

	// A changed PUT replaces the scoped layer rather than accumulating a second
	// durable row or leaving the previous scalar value effective.
	body["layer"].(map[string]any)["name"] = "Replaced agent"
	replaced := f.do("PUT", "/v1/admin/profiles/test.agent", body, token)
	if replaced.StatusCode != http.StatusOK || !strings.Contains(readBody(t, replaced), `"Replaced agent"`) {
		t.Fatalf("replacement put status=%d", replaced.StatusCode)
	}
	records, err = f.journal.List(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	profileRecords = 0
	for _, record := range records {
		if record.Kind == "profile" {
			profileRecords++
		}
	}
	if profileRecords != 1 {
		t.Fatalf("replacement PUT left %d profile journal rows", profileRecords)
	}
}

func TestAdminProfileRejectsCrossTenantAndNonAdmin(t *testing.T) {
	f := newControlFixture(t)
	admin := f.login(f.adminEmail, f.adminPass)
	store := mustAccountStore(t, f.db)
	if err := store.CreateTenant(t.Context(), "acme", "Acme"); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateAccount(t.Context(), storage.Account{
		Email: "tenant-admin@acme.test", Role: storage.RoleAccountTenantAdmin, TenantID: "acme", Status: storage.AccountActive,
	}, "tenant-admin-password"); err != nil {
		t.Fatal(err)
	}
	tenantToken, role := f.loginAny("tenant-admin@acme.test", "tenant-admin-password")
	if role != storage.RoleAccountTenantAdmin {
		t.Fatalf("role=%q", role)
	}
	body := map[string]any{"scope": json.RawMessage(`[{"kind":"global","id":"global"}]`), "layer": map[string]any{"profile_id": "test.agent"}}
	for _, token := range []string{tenantToken} {
		response := f.do("PUT", "/v1/admin/profiles/test.agent", body, token)
		if response.StatusCode != http.StatusForbidden {
			t.Fatalf("cross-tenant/global write status=%d body=%s", response.StatusCode, readBody(t, response))
		}
		get := f.do("GET", "/v1/admin/profiles/test.agent?scope="+url.QueryEscape(string(body["scope"].(json.RawMessage))), nil, token)
		if get.StatusCode != http.StatusForbidden {
			t.Fatalf("cross-tenant/global read status=%d body=%s", get.StatusCode, readBody(t, get))
		}
	}
	// A platform admin can own the global scope, proving the role gate is not
	// an unconditional denial.
	if response := f.do("PUT", "/v1/admin/profiles/test.agent", body, admin); response.StatusCode != http.StatusOK {
		t.Fatalf("platform admin write status=%d body=%s", response.StatusCode, readBody(t, response))
	}
}

func TestAdminProfileRejectsPathMismatchAndMissingDurability(t *testing.T) {
	f := newControlFixture(t)
	token := f.login(f.adminEmail, f.adminPass)
	mismatch := f.do("PUT", "/v1/admin/profiles/test.agent", map[string]any{
		"scope": json.RawMessage(`[{"kind":"global","id":"global"}]`),
		"layer": map[string]any{"profile_id": "other.agent"},
	}, token)
	if mismatch.StatusCode != http.StatusBadRequest {
		t.Fatalf("path mismatch status=%d body=%s", mismatch.StatusCode, readBody(t, mismatch))
	}
	missingScope := f.do("GET", "/v1/admin/profiles/test.agent", nil, token)
	if missingScope.StatusCode != http.StatusBadRequest {
		t.Fatalf("missing scope status=%d body=%s", missingScope.StatusCode, readBody(t, missingScope))
	}
}

func TestAdminProfileGeneralUsesRegistryIDVocabulary(t *testing.T) {
	f := newControlFixture(t)
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	name := "General"
	model := core.ModelSelection{Provider: "mock", Model: "mock-1"}
	if err := f.profiles.Bind(core.AgentProfileLayer{
		Scope: global, ProfileID: "general", Name: &name, Model: &model,
	}); err != nil {
		t.Fatal(err)
	}
	token := f.login(f.adminEmail, f.adminPass)
	scope := `[{"kind":"global","id":"global"}]`
	body := map[string]any{
		"scope": json.RawMessage(scope),
		"layer": map[string]any{
			"metadata": map[string]string{
				"harness.executor.id":      "graph-core-turn",
				"harness.executor.version": "1",
			},
		},
	}
	put := f.do("PUT", "/v1/admin/profiles/general", body, token)
	if put.StatusCode != http.StatusOK {
		t.Fatalf("general put status=%d body=%s", put.StatusCode, readBody(t, put))
	}
	if response := readBody(t, put); !strings.Contains(response, `"harness.executor.id":"graph-core-turn"`) || !strings.Contains(response, `"harness.executor.version":"1"`) {
		t.Fatalf("general graph metadata missing: %s", response)
	}

	get := f.do("GET", "/v1/admin/profiles/general?scope="+url.QueryEscape(scope), nil, token)
	if get.StatusCode != http.StatusOK {
		t.Fatalf("general get status=%d body=%s", get.StatusCode, readBody(t, get))
	}
	if response := readBody(t, get); !strings.Contains(response, `"profile_id":"general"`) || !strings.Contains(response, `"harness.executor.id":"graph-core-turn"`) {
		t.Fatalf("general get did not resolve durable graph metadata: %s", response)
	}

	invalid := f.do("GET", "/v1/admin/profiles/unsafe%20profile?scope="+url.QueryEscape(scope), nil, token)
	if invalid.StatusCode != http.StatusBadRequest {
		t.Fatalf("unsafe profile id status=%d body=%s", invalid.StatusCode, readBody(t, invalid))
	}
}
