// Authentication seam for the transport adapter. The kernel only ever sees
// the resulting core.Principal; how a request becomes a Principal is
// entirely integrator-owned.
//
// Integration model for deployers with an existing auth system:
//
//	external auth (SSO/OAuth/gateway/mTLS)  →  IdentitySource  →  PrincipalMapper  →  Principal
//	                                       (already exists:     (small glue:        (scope + grants)
//	                                        just extract)       read claims)
//
// Implement IdentitySource to lift the identity your system already verified
// into this package's vocabulary, and PrincipalMapper to place it in the
// scope hierarchy and resolve effective grants. AuthChain composes the two
// into the single Authenticator the server consumes. Anything more exotic
// (an OIDC library, an authz service call, mTLS) can implement Authenticator
// directly — the chain is a convenience, not a boundary.
package server

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

// Identity is the external system's view of the caller, expressed in this
// package's vocabulary. It carries no harness concepts: Subjects and roles
// are whatever the external auth system already calls them.
type Identity struct {
	// Subject is the stable user identifier from the external system.
	Subject string
	// Tenant is the owning tenant or organization identifier.
	Tenant string
	// Roles lists the roles, groups, or scopes the external system asserts.
	Roles []string
	// Attributes carries verified claims (email, org, session id, ...) that
	// downstream mappers, hooks, or capabilities may read.
	Attributes map[string]string
	// Source names the authenticator that produced the identity, for audit.
	Source string
}

// IdentitySource extracts a trusted Identity from one request. Trusted means
// the implementation has already verified it: a gateway sets signed headers,
// a JWT is validated, a session cookie is looked up. Unverified identity
// material must never become an Identity.
type IdentitySource interface {
	Identity(r *http.Request) (Identity, error)
}

// IdentitySourceFunc adapts a function to IdentitySource.
type IdentitySourceFunc func(*http.Request) (Identity, error)

func (f IdentitySourceFunc) Identity(r *http.Request) (Identity, error) {
	return f(r)
}

// PrincipalMapper places an external identity in the harness scope hierarchy
// and resolves its effective grants. This is the only harness-specific code
// an integrator must own: which scopes the product uses, and what its roles
// are allowed to do.
type PrincipalMapper interface {
	MapPrincipal(ctx context.Context, identity Identity) (core.Principal, error)
}

// PrincipalMapperFunc adapts a function to PrincipalMapper.
type PrincipalMapperFunc func(context.Context, Identity) (core.Principal, error)

func (f PrincipalMapperFunc) MapPrincipal(ctx context.Context, identity Identity) (core.Principal, error) {
	return f(ctx, identity)
}

// AuthChain composes an IdentitySource and a PrincipalMapper into the single
// Authenticator the server consumes.
type AuthChain struct {
	Source IdentitySource
	Mapper PrincipalMapper
}

func (a AuthChain) Authenticate(r *http.Request) (core.Principal, error) {
	if a.Source == nil || a.Mapper == nil {
		return core.Principal{}, fmt.Errorf("auth chain is incomplete")
	}
	identity, err := a.Source.Identity(r)
	if err != nil {
		return core.Principal{}, err
	}
	principal, err := a.Mapper.MapPrincipal(r.Context(), identity)
	if err != nil {
		return core.Principal{}, err
	}
	if principal.Attributes == nil {
		principal.Attributes = map[string]string{}
	}
	// Keep the raw identity reachable for audit and downstream hooks without
	// letting it overwrite mapper-provided attributes.
	for key, value := range identity.Attributes {
		if _, exists := principal.Attributes[key]; !exists {
			principal.Attributes[key] = value
		}
	}
	if _, exists := principal.Attributes["auth.source"]; !exists && identity.Source != "" {
		principal.Attributes["auth.source"] = identity.Source
	}
	return principal, nil
}

// StaticPrincipalMapper is the standard mapper for the common deployment:
// principals sit under <Root>/tenant/<tenant>/user/<subject>, grants come
// from a default set plus role-based additions, and optional attributes can
// carry extra grants asserted by the identity source.
type StaticPrincipalMapper struct {
	// Root prefixes the scope path (typically the product scope).
	Root []core.ScopeRef
	// DefaultGrants are granted to every authenticated principal.
	DefaultGrants core.PermissionSet
	// RoleGrants maps an external role or group name to additional grants.
	RoleGrants map[string][]core.Permission
	// GrantsFromAttributes lists Identity attribute keys whose comma-separated
	// values are treated as grants. Use only with sources you trust to have
	// verified those attributes.
	GrantsFromAttributes []string
}

func (m StaticPrincipalMapper) MapPrincipal(_ context.Context, identity Identity) (core.Principal, error) {
	if strings.TrimSpace(identity.Subject) == "" || strings.TrimSpace(identity.Tenant) == "" {
		return core.Principal{}, fmt.Errorf("identity is missing subject or tenant")
	}
	segments := append([]core.ScopeRef(nil), m.Root...)
	segments = append(segments,
		core.ScopeRef{Kind: core.ScopeTenant, ID: identity.Tenant},
		core.ScopeRef{Kind: core.ScopeUser, ID: identity.Subject},
	)
	scope, err := core.NewScopePath(segments...)
	if err != nil {
		return core.Principal{}, err
	}
	grants := m.DefaultGrants.Clone()
	for _, role := range identity.Roles {
		for _, permission := range m.RoleGrants[role] {
			grants[permission] = true
		}
	}
	for _, key := range m.GrantsFromAttributes {
		for _, raw := range strings.Split(identity.Attributes[key], ",") {
			if permission := strings.TrimSpace(raw); permission != "" {
				grants[core.Permission(permission)] = true
			}
		}
	}
	attributes := map[string]string{}
	for key, value := range identity.Attributes {
		attributes[key] = value
	}
	return core.Principal{
		SubjectID: identity.Subject, TenantID: identity.Tenant, Scope: scope,
		Grants: grants, Attributes: attributes,
	}, nil
}

// HeaderIdentitySource is a development source reading X-Harness-Subject,
// X-Harness-Tenant, X-Harness-Roles, and (only when enabled)
// X-Harness-Grants from the request. Production deployments replace it with
// a source that reads identity their gateway or IdP already verified.
type HeaderIdentitySource struct {
	// AllowGrantsHeader enables the X-Harness-Grants header so local testing
	// can simulate grants. It defaults to false: clients must never be able
	// to grant themselves permissions.
	AllowGrantsHeader bool
}

const (
	headerSubject = "X-Harness-Subject"
	headerTenant  = "X-Harness-Tenant"
	headerRoles   = "X-Harness-Roles"
	headerGrants  = "X-Harness-Grants"
)

func (h HeaderIdentitySource) Identity(r *http.Request) (Identity, error) {
	subject := strings.TrimSpace(r.Header.Get(headerSubject))
	tenant := strings.TrimSpace(r.Header.Get(headerTenant))
	if subject == "" || tenant == "" {
		return Identity{}, fmt.Errorf("missing harness identity headers")
	}
	identity := Identity{
		Subject: subject, Tenant: tenant, Source: "header",
		Attributes: map[string]string{},
	}
	for _, raw := range strings.Split(r.Header.Get(headerRoles), ",") {
		if role := strings.TrimSpace(raw); role != "" {
			identity.Roles = append(identity.Roles, role)
		}
	}
	if h.AllowGrantsHeader {
		for _, raw := range strings.Split(r.Header.Get(headerGrants), ",") {
			if permission := strings.TrimSpace(raw); permission != "" {
				identity.Attributes["dev.grants"] = strings.TrimSpace(
					identity.Attributes["dev.grants"] + "," + permission)
			}
		}
		identity.Attributes["dev.grants"] = strings.TrimPrefix(identity.Attributes["dev.grants"], ",")
	}
	return identity, nil
}
