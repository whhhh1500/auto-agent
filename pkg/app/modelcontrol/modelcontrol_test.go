package modelcontrol

import (
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
)

func TestResolveBuildsExactImmutablePlan(t *testing.T) {
	r := mustRegistry(t)
	plan, err := r.Resolve(ResolveInput{Catalog: Ref{"general", "1"}, CompositionRevision: "composition-1"})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Catalog.WireModel != "wire-general" || plan.Provider.Ref != (Ref{"provider", "1"}) || plan.Endpoint != (EndpointRef{"endpoint.ref", "endpoint-1"}) || plan.Protocol.Ref != (Ref{"protocol", "1"}) || plan.Credential != (CredentialRef{"credential.ref", "credential-1"}) || plan.SnapshotRevision == "" {
		t.Fatalf("plan=%#v", plan)
	}
	plan.Catalog.Capabilities.Modalities[0] = ModalityVideo
	plan.Endpoint.Revision = "mutated"
	again, err := r.Resolve(ResolveInput{Catalog: Ref{"general", "1"}, CompositionRevision: "composition-1"})
	if err != nil || again.Catalog.Capabilities.Modalities[0] != ModalityText || again.Endpoint.Revision != "endpoint-1" {
		t.Fatalf("mutation leaked=%#v err=%v", again, err)
	}
}

func TestPlanJSONUsesStableLowercaseFields(t *testing.T) {
	plan, err := mustRegistry(t).Resolve(ResolveInput{Catalog: Ref{"general", "1"}, CompositionRevision: "composition"})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) == "" || !strings.Contains(string(encoded), `"endpoint"`) || !strings.Contains(string(encoded), `"snapshot_revision"`) || !strings.Contains(string(encoded), `"wire_model"`) {
		t.Fatalf("unstable plan JSON: %s", encoded)
	}
}

func TestRegistryRejectsBrokenClosure(t *testing.T) {
	c := testCatalog()
	if _, err := NewRegistry([]CatalogModel{c}, nil, []ProtocolSpec{testProtocol()}, nil); !errors.Is(err, ErrInvalidCatalog) {
		t.Fatalf("missing provider=%v", err)
	}
	if _, err := NewRegistry([]CatalogModel{c}, []ProviderSpec{testProvider()}, []ProtocolSpec{testProtocol()}, nil); !errors.Is(err, ErrIncompatible) {
		t.Fatalf("missing compatibility=%v", err)
	}
	if _, err := NewRegistry(nil, []ProviderSpec{testProvider()}, []ProtocolSpec{testProtocol()}, []Compatibility{{Provider: Ref{"unknown", "1"}, Protocol: testProtocol().Ref}}); !errors.Is(err, ErrInvalidCompatibility) {
		t.Fatalf("unknown compatibility=%v", err)
	}
}

func TestResolveFailsClosedForVersionsAndUnknown(t *testing.T) {
	r := mustRegistry(t)
	if _, err := r.Resolve(ResolveInput{Catalog: Ref{"general", "2"}, CompositionRevision: "composition"}); !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("version=%v", err)
	}
	if _, err := r.Resolve(ResolveInput{Catalog: Ref{"unknown", "1"}, CompositionRevision: "composition"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown=%v", err)
	}
}

func TestSnapshotRevisionBindsResolutionEvidence(t *testing.T) {
	base := mustRegistry(t).SnapshotRevision()
	checks := []struct {
		name   string
		mutate func(*CatalogModel, *ProviderSpec, *ProtocolSpec)
	}{
		{"wire", func(c *CatalogModel, _ *ProviderSpec, _ *ProtocolSpec) { c.WireModel = "other" }},
		{"credential", func(c *CatalogModel, _ *ProviderSpec, _ *ProtocolSpec) { c.Credential.Revision = "credential-2" }},
		{"capabilities", func(c *CatalogModel, _ *ProviderSpec, _ *ProtocolSpec) { c.Capabilities.Reasoning = true }},
		{"endpoint", func(_ *CatalogModel, p *ProviderSpec, _ *ProtocolSpec) { p.Endpoint.Revision = "endpoint-2" }},
		{"provider implementation", func(_ *CatalogModel, p *ProviderSpec, _ *ProtocolSpec) { p.ImplementationRevision = "provider-2" }},
		{"protocol implementation", func(_ *CatalogModel, _ *ProviderSpec, p *ProtocolSpec) { p.ImplementationRevision = "protocol-2" }},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			c, p, q := testCatalog(), testProvider(), testProtocol()
			check.mutate(&c, &p, &q)
			r, err := NewRegistry([]CatalogModel{c}, []ProviderSpec{p}, []ProtocolSpec{q}, []Compatibility{{Provider: p.Ref, Protocol: q.Ref}})
			if err != nil || r.SnapshotRevision() == base {
				t.Fatalf("err=%v revision=%q", err, r.SnapshotRevision())
			}
		})
	}
	extra := testProvider()
	extra.Ref = Ref{"provider-extra", "1"}
	r, err := NewRegistry([]CatalogModel{testCatalog()}, []ProviderSpec{testProvider(), extra}, []ProtocolSpec{testProtocol()}, []Compatibility{{Provider: testProvider().Ref, Protocol: testProtocol().Ref}, {Provider: extra.Ref, Protocol: testProtocol().Ref}})
	if err != nil || r.SnapshotRevision() == base {
		t.Fatalf("compatibility evidence err=%v revision=%q", err, r.SnapshotRevision())
	}
}

func TestSnapshotRevisionIsIndependentOfInputOrdering(t *testing.T) {
	p1, p2 := testProvider(), testProvider()
	p2.Ref, p2.Endpoint = Ref{"provider-alt", "1"}, EndpointRef{"endpoint-alt", "1"}
	q := testProtocol()
	c1, c2 := testCatalog(), testCatalog()
	c2.Ref, c2.Provider = Ref{"alternate", "1"}, p2.Ref
	c2.Capabilities.Modalities = []Modality{ModalityImage, ModalityText}
	compatibilities := []Compatibility{{Provider: p1.Ref, Protocol: q.Ref}, {Provider: p2.Ref, Protocol: q.Ref}}
	first, err := NewRegistry([]CatalogModel{c1, c2}, []ProviderSpec{p1, p2}, []ProtocolSpec{q}, compatibilities)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewRegistry([]CatalogModel{c2, c1}, []ProviderSpec{p2, p1}, []ProtocolSpec{q}, []Compatibility{compatibilities[1], compatibilities[0]})
	if err != nil {
		t.Fatal(err)
	}
	if first.SnapshotRevision() != second.SnapshotRevision() {
		t.Fatalf("ordering changed revision: %q != %q", first.SnapshotRevision(), second.SnapshotRevision())
	}
}

func TestRegistryListingsAreDefensiveAndDeterministic(t *testing.T) {
	catalogs := []CatalogModel{testCatalog(), {Ref: Ref{"alpha", "1"}, WireModel: "wire-alpha", Provider: testProvider().Ref, Protocol: testProtocol().Ref, Credential: CredentialRef{"credential.ref", "credential-1"}, Capabilities: testCapabilities()}}
	r, err := NewRegistry(catalogs, []ProviderSpec{testProvider()}, []ProtocolSpec{testProtocol()}, []Compatibility{{Provider: testProvider().Ref, Protocol: testProtocol().Ref}})
	if err != nil {
		t.Fatal(err)
	}
	catalogs[0].Capabilities.Modalities[0] = ModalityVideo
	listed := r.Catalogs()
	if len(listed) != 2 || listed[0].Ref.ID != "alpha" || listed[1].Capabilities.Modalities[0] != ModalityText {
		t.Fatalf("listed=%#v", listed)
	}
	listed[0].Capabilities.Modalities[0] = ModalityVideo
	if r.Catalogs()[0].Capabilities.Modalities[0] != ModalityText {
		t.Fatal("list leaked backing data")
	}
	if p := r.Providers(); len(p) != 1 || p[0].Endpoint.ID != "endpoint.ref" {
		t.Fatalf("providers=%#v", p)
	}
	if p := r.Protocols(); len(p) != 1 || p[0].Ref != testProtocol().Ref {
		t.Fatalf("protocols=%#v", p)
	}
}

func TestKeylessAndMixedProtocolCatalogsResolveExactly(t *testing.T) {
	p := testProvider()
	q2 := ProtocolSpec{Ref: Ref{"protocol-alt", "1"}, ImplementationRevision: "protocol-alt-1"}
	local := testCatalog()
	local.Ref = Ref{"local", "dynamic-2"}
	local.Protocol, local.Credential = q2.Ref, CredentialRef{}
	r, err := NewRegistry([]CatalogModel{testCatalog(), local}, []ProviderSpec{p}, []ProtocolSpec{testProtocol(), q2}, []Compatibility{{Provider: p.Ref, Protocol: testProtocol().Ref}, {Provider: p.Ref, Protocol: q2.Ref}})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := r.Resolve(ResolveInput{Catalog: local.Ref, CompositionRevision: "composition"})
	if err != nil || plan.Credential != (CredentialRef{}) || plan.Protocol.Ref != q2.Ref {
		t.Fatalf("plan=%#v err=%v", plan, err)
	}
	local.Ref, local.Credential = Ref{"bad", "1"}, CredentialRef{ID: "only-id"}
	if _, err := NewRegistry([]CatalogModel{local}, []ProviderSpec{p}, []ProtocolSpec{q2}, []Compatibility{{Provider: p.Ref, Protocol: q2.Ref}}); !errors.Is(err, ErrInvalidCatalog) {
		t.Fatalf("partial credential=%v", err)
	}
}

func TestBoundsAndCapabilities(t *testing.T) {
	if _, err := NewRegistry(make([]CatalogModel, DefaultMaxEntries+1), nil, nil, nil); !errors.Is(err, ErrInvalidCatalog) {
		t.Fatalf("bound=%v", err)
	}
	bad := testCatalog()
	bad.Capabilities.MaxOutputTokens = bad.Capabilities.ContextWindowTokens + 1
	if _, err := NewRegistry([]CatalogModel{bad}, []ProviderSpec{testProvider()}, []ProtocolSpec{testProtocol()}, []Compatibility{{Provider: testProvider().Ref, Protocol: testProtocol().Ref}}); !errors.Is(err, ErrInvalidCatalog) {
		t.Fatalf("capabilities=%v", err)
	}
	bad = testCatalog()
	bad.Capabilities.Modalities = []Modality{ModalityText, ModalityText}
	if _, err := NewRegistry([]CatalogModel{bad}, []ProviderSpec{testProvider()}, []ProtocolSpec{testProtocol()}, []Compatibility{{Provider: testProvider().Ref, Protocol: testProtocol().Ref}}); !errors.Is(err, ErrInvalidCatalog) {
		t.Fatalf("modalities=%v", err)
	}
}

func TestConcurrentResolve(t *testing.T) {
	r := mustRegistry(t)
	var group sync.WaitGroup
	errs := make(chan error, 64)
	for range 64 {
		group.Add(1)
		go func() {
			defer group.Done()
			p, err := r.Resolve(ResolveInput{Catalog: testCatalog().Ref, CompositionRevision: "composition"})
			if err != nil || p.Credential.ID != "credential.ref" {
				errs <- err
			}
		}()
	}
	group.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("resolve=%v", err)
	}
}

func BenchmarkResolve(b *testing.B) {
	r := mustRegistry(b)
	input := ResolveInput{Catalog: testCatalog().Ref, CompositionRevision: "composition"}
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		if _, err := r.Resolve(input); err != nil {
			b.Fatal(err)
		}
	}
}

func mustRegistry(t testing.TB) *Registry {
	t.Helper()
	r, err := NewRegistry([]CatalogModel{testCatalog()}, []ProviderSpec{testProvider()}, []ProtocolSpec{testProtocol()}, []Compatibility{{Provider: testProvider().Ref, Protocol: testProtocol().Ref}})
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func testCatalog() CatalogModel {
	return CatalogModel{Ref: Ref{"general", "1"}, WireModel: "wire-general", Provider: testProvider().Ref, Protocol: testProtocol().Ref, Credential: CredentialRef{"credential.ref", "credential-1"}, Capabilities: testCapabilities()}
}
func testCapabilities() ModelCapabilities {
	return ModelCapabilities{ContextWindowTokens: 32768, MaxOutputTokens: 4096, ToolCalls: true, Modalities: []Modality{ModalityText}}
}
func testProvider() ProviderSpec {
	return ProviderSpec{Ref: Ref{"provider", "1"}, Endpoint: EndpointRef{"endpoint.ref", "endpoint-1"}, ImplementationRevision: "provider-impl-1"}
}
func testProtocol() ProtocolSpec {
	return ProtocolSpec{Ref: Ref{"protocol", "1"}, ImplementationRevision: "protocol-impl-1"}
}
