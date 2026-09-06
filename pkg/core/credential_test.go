package core

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

type credentialProbe struct {
	manifest CapabilityManifest
	ref      CredentialRef
}

type panicCredentialProvider struct{}

func (panicCredentialProvider) Resolve(context.Context, Principal) (CredentialValue, error) {
	panic("credential panic")
}

func (p credentialProbe) Manifest() CapabilityManifest { return p.manifest }

func (p credentialProbe) Execute(ctx context.Context, request CapabilityRequest) (CapabilityResult, error) {
	value, err := request.Context.Credentials.Resolve(ctx, p.ref)
	if err != nil {
		return CapabilityResult{}, err
	}
	return CapabilityResult{Content: value.Value, OK: true}, nil
}

func TestCredentialResolutionIsScopedAndNotSerialized(t *testing.T) {
	_, product, tenant, user := testScopes()
	principal := testPrincipal(user)
	credentials := NewCredentialRegistry()
	ref := CredentialRef("market.api-key")
	if err := credentials.Bind(CredentialBinding{
		Scope: product, Ref: ref, Mode: CredentialProvide,
		Provider: StaticCredentialProvider{Value: "product-secret", Source: "product"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := credentials.Bind(CredentialBinding{
		Scope: tenant, Ref: ref, Mode: CredentialReplace,
		Provider: StaticCredentialProvider{Value: "tenant-secret", Source: "tenant"},
	}); err != nil {
		t.Fatal(err)
	}

	manifest := toolManifest("market.private-quote", "1.0.0")
	manifest.RequiredCredentials = []CredentialRef{ref}
	registry := NewCapabilityRegistry()
	if err := registry.Register(product, credentialProbe{manifest: manifest, ref: ref}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := (CapabilityResolver{Registry: registry, Credentials: credentials}).Resolve(principal, user)
	if err != nil {
		t.Fatal(err)
	}
	result, err := snapshot.Execute(context.Background(), ToolCall{ID: "call", Name: manifest.ID})
	if err != nil || result.Content != "tenant-secret" {
		t.Fatalf("unexpected credential result: %#v, %v", result, err)
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "tenant-secret") || strings.Contains(string(encoded), "product-secret") {
		t.Fatalf("credential leaked through snapshot JSON: %s", encoded)
	}
}

func TestCapabilityCannotResolveUndeclaredCredential(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	credentials := NewCredentialRegistry()
	declared := CredentialRef("market.declared")
	undeclared := CredentialRef("market.undeclared")
	if err := credentials.Bind(CredentialBinding{
		Scope: product, Ref: declared, Provider: StaticCredentialProvider{Value: "declared-secret"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := credentials.Bind(CredentialBinding{
		Scope: product, Ref: undeclared, Provider: StaticCredentialProvider{Value: "undeclared-secret"},
	}); err != nil {
		t.Fatal(err)
	}
	manifest := toolManifest("market.probe", "1.0.0")
	manifest.RequiredCredentials = []CredentialRef{declared}
	registry := NewCapabilityRegistry()
	if err := registry.Register(product, credentialProbe{manifest: manifest, ref: undeclared}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := (CapabilityResolver{Registry: registry, Credentials: credentials}).Resolve(principal, user)
	if err != nil {
		t.Fatal(err)
	}
	_, err = snapshot.Execute(context.Background(), ToolCall{ID: "call", Name: manifest.ID})
	if err == nil || !strings.Contains(err.Error(), "not declared") {
		t.Fatalf("expected least-authority credential rejection, got %v", err)
	}
}

func TestCredentialProviderPanicIsContained(t *testing.T) {
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	registry := NewCredentialRegistry()
	if err := registry.Bind(CredentialBinding{
		Scope: user, Ref: "panic.secret", Provider: panicCredentialProvider{},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.ResolveCredential(context.Background(), principal, user, "panic.secret"); err == nil {
		t.Fatal("credential provider panic was not contained")
	}
}

func TestCredentialRegistryRejectsBindingOverflow(t *testing.T) {
	_, product, _, _ := testScopes()
	registry := NewCredentialRegistry()
	registry.maxBindings = 2
	mount := func(id string) (func(), error) {
		return registry.Mount(CredentialBinding{
			Scope: product, Ref: CredentialRef(id),
			Provider: StaticCredentialProvider{Value: "secret"},
		})
	}
	unmount, err := mount("cred.one")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mount("cred.two"); err != nil {
		t.Fatal(err)
	}
	if _, err := mount("cred.three"); err == nil {
		t.Fatal("credential registry overflow was accepted")
	}
	unmount()
	if _, err := mount("cred.three"); err != nil {
		t.Fatalf("unmount did not free a registry slot: %v", err)
	}
}
