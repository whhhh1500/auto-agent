package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/storage"
)

func TestUserCannotMutateAncestorScopes(t *testing.T) {
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product, _ := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "product"})
	tenant, _ := product.Child(core.ScopeRef{Kind: core.ScopeTenant, ID: "acme"})
	user, _ := tenant.Child(core.ScopeRef{Kind: core.ScopeUser, ID: "alice"})
	principal := core.Principal{SubjectID: "alice", TenantID: "acme", Scope: user}
	if canMutateScope(principal, tenant) || canMutateScope(principal, product) || canMutateScope(principal, global) {
		t.Fatal("user principal can mutate an inherited ancestor scope")
	}
	session, _ := user.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "session-a"})
	if !canMutateScope(principal, user) || !canMutateScope(principal, session) {
		t.Fatal("user principal cannot mutate its own scope or descendants")
	}
}

func TestTenantAdminCanMutateOnlyItsTenantSubtree(t *testing.T) {
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product, _ := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "product"})
	tenant, _ := product.Child(core.ScopeRef{Kind: core.ScopeTenant, ID: "acme"})
	other, _ := product.Child(core.ScopeRef{Kind: core.ScopeTenant, ID: "other"})
	principal := core.Principal{SubjectID: "admin@acme", TenantID: "acme", Scope: tenant}
	if !canMutateScope(principal, tenant) || canMutateScope(principal, product) || canMutateScope(principal, other) {
		t.Fatal("tenant admin mutation boundary is incorrect")
	}
}

func TestTenantOperatorRequirementRejectsUser(t *testing.T) {
	response := httptest.NewRecorder()
	principal := core.Principal{Attributes: map[string]string{"role": storage.RoleAccountUser}}
	if requireTenantOperator(response, principal) {
		t.Fatal("ordinary user passed tenant-operator authorization")
	}
	if response.Code != http.StatusForbidden {
		t.Fatalf("unexpected status: %d", response.Code)
	}
}
