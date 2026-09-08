package server_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/server"
)

// The three integration shapes an existing auth system can take.

func TestAuthChainWithGatewayIdentitySource(t *testing.T) {
	// Shape 1: the integrator's gateway already verified the user and sets
	// signed identity headers. The IdentitySource only lifts them.
	source := server.IdentitySourceFunc(func(r *http.Request) (server.Identity, error) {
		subject := r.Header.Get("X-Gateway-User")
		tenant := r.Header.Get("X-Gateway-Org")
		if subject == "" || tenant == "" {
			return server.Identity{}, fmt.Errorf("gateway identity missing")
		}
		return server.Identity{
			Subject: subject, Tenant: tenant,
			Roles: strings.Split(r.Header.Get("X-Gateway-Roles"), ","),
			Attributes: map[string]string{
				"email": r.Header.Get("X-Gateway-Email"),
			},
			Source: "gateway",
		}, nil
	})
	mapper := server.StaticPrincipalMapper{
		Root: []core.ScopeRef{
			{Kind: core.ScopeGlobal, ID: "global"},
			{Kind: core.ScopeProduct, ID: "crypto"},
		},
		DefaultGrants: core.NewPermissionSet(core.PermRead),
		RoleGrants: map[string][]core.Permission{
			"trader": {core.PermWrite, core.PermSend},
			"admin":  {core.PermWrite, core.PermSend},
		},
	}
	authenticator := server.AuthChain{Source: source, Mapper: mapper}

	request, _ := http.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("X-Gateway-User", "alice")
	request.Header.Set("X-Gateway-Org", "acme")
	request.Header.Set("X-Gateway-Roles", "trader,auditor")
	request.Header.Set("X-Gateway-Email", "alice@acme.example")

	principal, err := authenticator.Authenticate(request)
	if err != nil {
		t.Fatal(err)
	}
	if principal.SubjectID != "alice" || principal.TenantID != "acme" {
		t.Fatalf("identity not mapped: %#v", principal)
	}
	if !principal.Grants.Allows([]core.Permission{core.PermRead, core.PermWrite, core.PermSend}) {
		t.Fatalf("role grants not applied: %#v", principal.Grants)
	}
	if principal.Attributes["email"] != "alice@acme.example" || principal.Attributes["auth.source"] != "gateway" {
		t.Fatalf("identity attributes not carried: %#v", principal.Attributes)
	}
	// Scope lands under the product the integrator declared.
	if !strings.Contains(principal.Scope.String(), "product:crypto/tenant:acme/user:alice") {
		t.Fatalf("unexpected scope: %s", principal.Scope.String())
	}

	// Missing gateway identity is refused.
	bad, _ := http.NewRequest(http.MethodGet, "/", nil)
	if _, err := authenticator.Authenticate(bad); err == nil {
		t.Fatal("missing identity must be refused")
	}
}

func TestAuthChainWithCustomAuthenticator(t *testing.T) {
	// Shape 2: the integrator owns everything — e.g. bearer tokens validated
	// against their own auth service, or an OIDC library. They implement the
	// single Authenticator method and skip the chain entirely.
	tokens := map[string]core.Principal{
		"tok-alice": {
			SubjectID: "alice", TenantID: "acme",
			Scope: core.MustScopePath(
				core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"},
				core.ScopeRef{Kind: core.ScopeProduct, ID: "crypto"},
				core.ScopeRef{Kind: core.ScopeTenant, ID: "acme"},
				core.ScopeRef{Kind: core.ScopeUser, ID: "alice"},
			),
			Grants:     core.NewPermissionSet(core.PermRead, core.PermWrite),
			Attributes: map[string]string{"auth.source": "external-idp"},
		},
	}
	authenticator := server.AuthenticatorFunc(func(r *http.Request) (core.Principal, error) {
		prefix := "Bearer "
		token := strings.TrimPrefix(r.Header.Get("Authorization"), prefix)
		principal, ok := tokens[token]
		if !ok {
			return core.Principal{}, fmt.Errorf("invalid token")
		}
		return principal, nil
	})

	request, _ := http.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("Authorization", "Bearer tok-alice")
	principal, err := authenticator.Authenticate(request)
	if err != nil || principal.SubjectID != "alice" {
		t.Fatalf("custom authenticator failed: %#v %v", principal, err)
	}

	bad, _ := http.NewRequest(http.MethodGet, "/", nil)
	bad.Header.Set("Authorization", "Bearer nope")
	if _, err := authenticator.Authenticate(bad); err == nil {
		t.Fatal("invalid token must be refused")
	}
}

func TestAuthChainExternalMapperCanCallAuthzService(t *testing.T) {
	// Shape 3: grants resolved by an external authorization service per
	// request. The mapper receives the request context, so it can call out.
	source := server.IdentitySourceFunc(func(r *http.Request) (server.Identity, error) {
		return server.Identity{Subject: "alice", Tenant: "acme"}, nil
	})
	mapper := server.PrincipalMapperFunc(func(ctx context.Context, identity server.Identity) (core.Principal, error) {
		// Stand-in for an authz lookup keyed by the request context.
		if ctx.Value(ctxKeyAuthz{}) == nil {
			return core.Principal{}, fmt.Errorf("authz service unavailable")
		}
		return core.Principal{
			SubjectID: identity.Subject, TenantID: identity.Tenant,
			Scope: core.MustScopePath(
				core.ScopeRef{Kind: core.ScopeTenant, ID: identity.Tenant},
				core.ScopeRef{Kind: core.ScopeUser, ID: identity.Subject},
			),
			Grants: core.NewPermissionSet(core.PermRead),
		}, nil
	})
	authenticator := server.AuthChain{Source: source, Mapper: mapper}

	request, _ := http.NewRequest(http.MethodGet, "/", nil)
	authorizedContext := context.WithValue(request.Context(), ctxKeyAuthz{}, true)
	if _, err := authenticator.Authenticate(request.WithContext(authorizedContext)); err != nil {
		t.Fatalf("authorized mapping failed: %v", err)
	}
	if _, err := authenticator.Authenticate(request); err == nil {
		t.Fatal("mapper must see request context and refuse when the service is down")
	}
}

type ctxKeyAuthz struct{}

func TestHeaderAuthenticatorRemainsCompatible(t *testing.T) {
	authenticator := server.HeaderAuthenticator{
		DefaultGrants:     core.NewPermissionSet(core.PermRead),
		RoleGrants:        map[string][]core.Permission{"ops": {core.PermWrite}},
		AllowGrantsHeader: false,
	}
	request, _ := http.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("X-Harness-Subject", "alice")
	request.Header.Set("X-Harness-Tenant", "acme")
	request.Header.Set("X-Harness-Roles", "ops,viewer")
	principal, err := authenticator.Authenticate(request)
	if err != nil {
		t.Fatal(err)
	}
	if !principal.Grants.Allows([]core.Permission{core.PermRead, core.PermWrite}) {
		t.Fatalf("role grants not applied through the chain: %#v", principal.Grants)
	}
	// Grants header stays ignored unless explicitly enabled.
	request.Header.Set("X-Harness-Grants", "data.send")
	principal, err = authenticator.Authenticate(request)
	if err != nil {
		t.Fatal(err)
	}
	if principal.Grants.Allows([]core.Permission{core.PermSend}) {
		t.Fatal("client-supplied grants must stay disabled by default")
	}
	// Identity source is also reachable standalone for tests that want it.
	identity, err := server.HeaderIdentitySource{AllowGrantsHeader: true}.Identity(request)
	if err != nil || identity.Subject != "alice" {
		t.Fatalf("standalone identity source broken: %#v %v", identity, err)
	}
	if identity.Attributes["dev.grants"] != "data.send" {
		t.Fatalf("dev grants attribute missing: %#v", identity.Attributes)
	}
}
