package core

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

type panicTool struct{ manifest CapabilityManifest }

func (p panicTool) Manifest() CapabilityManifest { return p.manifest }
func (panicTool) Execute(context.Context, CapabilityRequest) (CapabilityResult, error) {
	panic("provider secret panic")
}

type revisionTool struct {
	manifest CapabilityManifest
	content  string
}

func (r revisionTool) Manifest() CapabilityManifest { return r.manifest }
func (r revisionTool) Execute(context.Context, CapabilityRequest) (CapabilityResult, error) {
	return CapabilityResult{Content: r.content, OK: true}, nil
}
func (revisionTool) ArtifactRevision() string { return "provider-build-secret-label" }

type panicManifestCapability struct{}

func (panicManifestCapability) Manifest() CapabilityManifest { panic("manifest panic") }
func (panicManifestCapability) Execute(context.Context, CapabilityRequest) (CapabilityResult, error) {
	return CapabilityResult{OK: true}, nil
}

func TestCapabilityResolverLayersAndFreezesSnapshot(t *testing.T) {
	_, product, tenant, user := testScopes()
	session, _ := user.Child(ScopeRef{Kind: ScopeSession, ID: "session-a"})
	principal := testPrincipal(user)
	registry := NewCapabilityRegistry()

	base := staticTool{manifest: toolManifest("market.quote", "1.0.0"), content: "product"}
	if err := registry.Register(product, base); err != nil {
		t.Fatal(err)
	}
	tenantProvider := staticTool{manifest: toolManifest("market.quote", "2.0.0"), content: "tenant"}
	if err := registry.Bind(CapabilityBinding{
		Scope: tenant, Mode: BindingReplace, Manifest: tenantProvider.Manifest(), Provider: tenantProvider,
	}); err != nil {
		t.Fatal(err)
	}

	first, err := (CapabilityResolver{Registry: registry}).Resolve(principal, session)
	if err != nil {
		t.Fatal(err)
	}
	result, err := first.Execute(context.Background(), ToolCall{ID: "call-a", Name: "market.quote"})
	if err != nil || result.Content != "tenant" {
		t.Fatalf("unexpected first result: %#v, %v", result, err)
	}

	userProvider := staticTool{manifest: toolManifest("market.quote", "3.0.0"), content: "user"}
	if err := registry.Bind(CapabilityBinding{
		Scope: user, Mode: BindingReplace, Manifest: userProvider.Manifest(), Provider: userProvider,
	}); err != nil {
		t.Fatal(err)
	}
	oldResult, _ := first.Execute(context.Background(), ToolCall{ID: "call-b", Name: "market.quote"})
	if oldResult.Content != "tenant" {
		t.Fatalf("existing snapshot changed after registry update: %#v", oldResult)
	}
	second, err := (CapabilityResolver{Registry: registry}).Resolve(principal, session)
	if err != nil {
		t.Fatal(err)
	}
	newResult, _ := second.Execute(context.Background(), ToolCall{ID: "call-c", Name: "market.quote"})
	if newResult.Content != "user" || first.ID == second.ID {
		t.Fatalf("new snapshot was not updated: %#v", newResult)
	}
}

func TestProtectedCapabilityRejectsLowerOverride(t *testing.T) {
	_, product, tenant, user := testScopes()
	principal := testPrincipal(user)
	registry := NewCapabilityRegistry()
	base := staticTool{manifest: toolManifest("core.policy", "1.0.0"), content: "core"}
	if err := registry.Bind(CapabilityBinding{
		Scope: product, Mode: BindingProvide, Manifest: base.Manifest(), Provider: base, Protected: true,
	}); err != nil {
		t.Fatal(err)
	}
	replacement := staticTool{manifest: toolManifest("core.policy", "2.0.0"), content: "tenant"}
	if err := registry.Bind(CapabilityBinding{
		Scope: tenant, Mode: BindingReplace, Manifest: replacement.Manifest(), Provider: replacement,
	}); err != nil {
		t.Fatal(err)
	}
	_, err := (CapabilityResolver{Registry: registry}).Resolve(principal, user)
	if err == nil || !strings.Contains(err.Error(), "protected") {
		t.Fatalf("expected protected override rejection, got %v", err)
	}
}

func TestCapabilityPermissionsAndTimeout(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	registry := NewCapabilityRegistry()
	manifest := toolManifest("network.slow", "1.0.0")
	manifest.TimeoutMs = 20
	if err := registry.Register(product, staticTool{manifest: manifest, wait: true}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := (CapabilityResolver{Registry: registry}).Resolve(principal, user)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	_, err = snapshot.Execute(context.Background(), ToolCall{ID: "call", Name: "network.slow"})
	if err == nil || time.Since(started) > time.Second {
		t.Fatalf("timeout was not enforced: %v", err)
	}

	principal.Grants = NewPermissionSet()
	withoutGrant, err := (CapabilityResolver{Registry: registry}).Resolve(principal, user)
	if err != nil {
		t.Fatal(err)
	}
	if withoutGrant.Authorized("network.slow") {
		t.Fatal("permission-filtered capability remained authorized")
	}
}

func TestRegistryRejectsToolManifestWithNonToolProvider(t *testing.T) {
	_, product, _, _ := testScopes()
	registry := NewCapabilityRegistry()
	manifest := toolManifest("broken.tool", "1.0.0")
	err := registry.Bind(CapabilityBinding{
		Scope: product, Mode: BindingProvide, Manifest: manifest, Provider: struct{}{},
	})
	if err == nil {
		t.Fatal("tool-exposed capability accepted a provider that cannot execute tools")
	}
}

func TestRegistryFreezesManifestAtMount(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	registry := NewCapabilityRegistry()
	parameters := map[string]any{"type": "object", "properties": map[string]any{"symbol": map[string]any{"type": "string"}}}
	headers := map[string]string{"X-Mode": "original"}
	manifest := toolManifest("frozen.tool", "1.0.0")
	manifest.Tool.Parameters = parameters
	manifest.Execution = &ExecutionSpec{Runtime: "http", Headers: headers}
	if err := registry.Register(product, staticTool{manifest: manifest, content: "ok"}); err != nil {
		t.Fatal(err)
	}
	parameters["type"] = "array"
	headers["X-Mode"] = "mutated"
	snapshot, err := (CapabilityResolver{Registry: registry}).Resolve(principal, user)
	if err != nil {
		t.Fatal(err)
	}
	stored, ok := snapshot.ManifestFor("frozen.tool")
	if !ok {
		t.Fatal("mounted manifest is missing")
	}
	if stored.Tool.Parameters["type"] != "object" || stored.Execution.Headers["X-Mode"] != "[redacted]" {
		t.Fatalf("caller mutation changed mounted manifest: %#v", stored)
	}
	if snapshot.capabilities[0].Manifest.Execution.Headers["X-Mode"] != "original" {
		t.Fatalf("caller mutation changed internal execution manifest: %#v", snapshot.capabilities[0].Manifest.Execution.Headers)
	}
}

func TestCapabilityProviderPanicIsContained(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	registry := NewCapabilityRegistry()
	manifest := toolManifest("panic.tool", "1.0.0")
	if err := registry.Register(product, panicTool{manifest: manifest}); err != nil {
		t.Fatal(err)
	}
	snapshot, _ := (CapabilityResolver{Registry: registry}).Resolve(principal, user)
	result, err := snapshot.Execute(context.Background(), ToolCall{ID: "call-panic", Name: manifest.ID})
	if err != nil || result.OK || result.Metadata["code"] != "capability_provider_panic" {
		t.Fatalf("provider panic escaped or was misclassified: %#v %v", result, err)
	}
	if strings.Contains(result.Content, "secret") {
		t.Fatalf("panic details leaked through result: %q", result.Content)
	}
}

func TestCapabilityOutputSchemaIsEnforced(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	manifest := toolManifest("schema.output", "1.0.0")
	manifest.OutputSchema = map[string]any{
		"type":       "object",
		"properties": map[string]any{"price": map[string]any{"type": "number"}},
		"required":   []any{"price"}, "additionalProperties": false,
	}
	for _, test := range []struct {
		name    string
		content string
		ok      bool
	}{
		{name: "valid", content: `{"price":42}`, ok: true},
		{name: "invalid-json", content: `not-json`},
		{name: "wrong-schema", content: `{"price":"forty-two"}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			registry := NewCapabilityRegistry()
			provider := revisionTool{manifest: manifest, content: test.content}
			if err := registry.Register(product, provider); err != nil {
				t.Fatal(err)
			}
			snapshot, _ := (CapabilityResolver{Registry: registry}).Resolve(principal, user)
			result, err := snapshot.Execute(context.Background(), ToolCall{ID: "call-output", Name: manifest.ID})
			if err != nil {
				t.Fatal(err)
			}
			if result.OK != test.ok {
				t.Fatalf("unexpected result: %#v", result)
			}
			if !test.ok && result.Metadata["code"] != "invalid_capability_output" {
				t.Fatalf("missing stable output error code: %#v", result)
			}
		})
	}
}

func TestProviderRevisionIsHashedIntoSnapshot(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	registry := NewCapabilityRegistry()
	provider := revisionTool{manifest: toolManifest("revision.tool", "1.0.0"), content: `{}`}
	if err := registry.Register(product, provider); err != nil {
		t.Fatal(err)
	}
	snapshot, _ := (CapabilityResolver{Registry: registry}).Resolve(principal, user)
	capabilities := snapshot.Capabilities()
	if len(capabilities) != 1 || !strings.HasPrefix(capabilities[0].ProviderRevision, "sha256:") {
		t.Fatalf("provider revision fingerprint missing: %#v", capabilities)
	}
	encoded, _ := json.Marshal(capabilities)
	if strings.Contains(string(encoded), "provider-build-secret-label") {
		t.Fatalf("raw provider revision leaked into snapshot: %s", encoded)
	}
}

func TestRegistryRejectsInvalidSchemasAndManifestPanic(t *testing.T) {
	_, product, _, _ := testScopes()
	registry := NewCapabilityRegistry()
	manifest := toolManifest("bad.schema", "1.0.0")
	manifest.Tool.Parameters = map[string]any{"type": "strng"}
	if err := registry.Register(product, staticTool{manifest: manifest}); err == nil {
		t.Fatal("invalid schema was accepted")
	}
	if err := registry.Register(product, panicManifestCapability{}); err == nil {
		t.Fatal("manifest panic escaped registration")
	}
}

func TestCapabilityRejectsNonJSONArgumentsAndMetadata(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	manifest := toolManifest("json.boundary", "1.0.0")
	registry := NewCapabilityRegistry()
	provider := revisionTool{manifest: manifest, content: `{}`}
	if err := registry.Register(product, provider); err != nil {
		t.Fatal(err)
	}
	snapshot, _ := (CapabilityResolver{Registry: registry}).Resolve(principal, user)
	result, err := snapshot.Execute(context.Background(), ToolCall{
		ID: "call-json", Name: manifest.ID, Args: map[string]any{"bad": make(chan int)},
	})
	if err != nil || result.OK || result.Metadata["code"] != CodeInvalidArgs {
		t.Fatalf("non-JSON arguments were not refused: %#v %v", result, err)
	}

	metadataProvider := metadataTool{manifest: manifest}
	registry = NewCapabilityRegistry()
	if err := registry.Register(product, metadataProvider); err != nil {
		t.Fatal(err)
	}
	snapshot, _ = (CapabilityResolver{Registry: registry}).Resolve(principal, user)
	result, err = snapshot.Execute(context.Background(), ToolCall{ID: "call-meta", Name: manifest.ID})
	if err != nil || result.OK || result.Metadata["code"] != "invalid_capability_output" {
		t.Fatalf("non-JSON metadata was not refused: %#v %v", result, err)
	}
}

type metadataTool struct{ manifest CapabilityManifest }

func (m metadataTool) Manifest() CapabilityManifest { return m.manifest }
func (metadataTool) Execute(context.Context, CapabilityRequest) (CapabilityResult, error) {
	return CapabilityResult{Content: `{}`, OK: true, Metadata: map[string]any{"bad": make(chan int)}}, nil
}

func TestSnapshotProviderDoesNotExposeToolProvider(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	registry := NewCapabilityRegistry()
	provider := staticTool{manifest: toolManifest("private.tool", "1.0.0"), content: "ok"}
	if err := registry.Register(product, provider); err != nil {
		t.Fatal(err)
	}
	snapshot, _ := (CapabilityResolver{Registry: registry}).Resolve(principal, user)
	if _, ok := snapshot.Provider(provider.manifest.ID); ok {
		t.Fatal("raw tool provider was exposed outside the guard funnel")
	}
}

func TestSnapshotManifestAndPrincipalAreDefensiveCopies(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	manifest := toolManifest("copy.tool", "1.0.0")
	manifest.Tool.Parameters = map[string]any{"type": "object"}
	registry := NewCapabilityRegistry()
	if err := registry.Register(product, staticTool{manifest: manifest, content: "ok"}); err != nil {
		t.Fatal(err)
	}
	snapshot, _ := (CapabilityResolver{Registry: registry}).Resolve(principal, user)
	first, _ := snapshot.ManifestFor(manifest.ID)
	first.Tool.Parameters["type"] = "array"
	second, _ := snapshot.ManifestFor(manifest.ID)
	if second.Tool.Parameters["type"] != "object" {
		t.Fatal("manifest mutation changed the frozen snapshot")
	}
	copyPrincipal := snapshot.Principal()
	delete(copyPrincipal.Grants, PermRead)
	if !snapshot.Principal().Grants[PermRead] {
		t.Fatal("principal mutation changed the frozen snapshot")
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"capabilities"`) || !strings.Contains(string(encoded), `"principal"`) {
		t.Fatalf("snapshot audit JSON is incomplete: %s", encoded)
	}
}

func TestPublicCapabilityViewsRedactLiteralHeadersAndSensitiveMetadata(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	manifest := toolManifest("redact.tool", "1.0.0")
	manifest.Metadata = map[string]string{"api_key": "metadata-secret", "team": "core"}
	manifest.Execution = &ExecutionSpec{Runtime: "http", Headers: map[string]string{
		"Authorization": "Bearer literal-secret",
		"X-Credential":  "$credential:remote.key",
	}}
	registry := NewCapabilityRegistry()
	if err := registry.Register(product, staticTool{manifest: manifest, content: "ok"}); err != nil {
		t.Fatal(err)
	}
	snapshot, _ := (CapabilityResolver{Registry: registry}).Resolve(principal, user)
	public, _ := snapshot.ManifestFor(manifest.ID)
	if public.Execution.Headers["Authorization"] != "[redacted]" || public.Execution.Headers["X-Credential"] != "$credential:remote.key" {
		t.Fatalf("public headers were not safely redacted: %#v", public.Execution.Headers)
	}
	if public.Metadata["api_key"] != "[redacted]" || public.Metadata["team"] != "core" {
		t.Fatalf("public metadata was not safely redacted: %#v", public.Metadata)
	}
	entries, err := registry.Entries(user)
	if err != nil {
		t.Fatal(err)
	}
	if entries[0].Manifest.Execution.Headers["Authorization"] != "[redacted]" {
		t.Fatalf("catalog entry leaked a literal header: %#v", entries[0])
	}
}

func TestCapabilityOutputAndArgumentSizeLimits(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	manifest := toolManifest("size.tool", "1.0.0")
	manifest.MaxOutputBytes = 4
	registry := NewCapabilityRegistry()
	if err := registry.Register(product, revisionTool{manifest: manifest, content: "12345"}); err != nil {
		t.Fatal(err)
	}
	snapshot, _ := (CapabilityResolver{Registry: registry}).Resolve(principal, user)
	result, err := snapshot.Execute(context.Background(), ToolCall{ID: "call-size", Name: manifest.ID})
	if err != nil || result.OK || result.Metadata["code"] != "capability_output_too_large" {
		t.Fatalf("oversized output was not refused: %#v %v", result, err)
	}

	large := strings.Repeat("x", MaxToolArgumentBytes+1)
	result, err = snapshot.Execute(context.Background(), ToolCall{
		ID: "call-args", Name: manifest.ID, Args: map[string]any{"value": large},
	})
	if err != nil || result.OK || result.Metadata["code"] != CodeInvalidArgs {
		t.Fatalf("oversized arguments were not refused: %#v %v", result, err)
	}
}

func TestCapabilityRegistryRejectsBindingOverflow(t *testing.T) {
	_, product, _, _ := testScopes()
	registry := NewCapabilityRegistry()
	registry.maxBindings = 2
	mount := func(id string) (func(), error) {
		manifest := toolManifest(id, "1.0.0")
		return registry.Mount(CapabilityBinding{Scope: product, Manifest: manifest, Provider: staticTool{manifest: manifest}})
	}
	unmount, err := mount("cap.one")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mount("cap.two"); err != nil {
		t.Fatal(err)
	}
	if _, err := mount("cap.three"); err == nil {
		t.Fatal("capability registry overflow was accepted")
	}
	unmount()
	if _, err := mount("cap.three"); err != nil {
		t.Fatalf("unmount did not free a registry slot: %v", err)
	}
}
