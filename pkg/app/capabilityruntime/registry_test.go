package capabilityruntime

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

type testFactory struct {
	id, version string
	got         Request
}
type panicFactory struct{}

func (panicFactory) ID() string                                   { panic("secret-id") }
func (panicFactory) Version() string                              { return "1" }
func (panicFactory) ImplementationRevision() string               { return "test/v1" }
func (panicFactory) New(context.Context, Request) (Result, error) { panic("secret-new") }

type panicNewFactory struct{}

func (panicNewFactory) ID() string                                   { return "panic-new" }
func (panicNewFactory) Version() string                              { return "1" }
func (panicNewFactory) ImplementationRevision() string               { return "panic/v1" }
func (panicNewFactory) New(context.Context, Request) (Result, error) { panic("new-secret") }

func (f *testFactory) ID() string                     { return f.id }
func (f *testFactory) Version() string                { return f.version }
func (f *testFactory) ImplementationRevision() string { return "test/v1" }
func (f *testFactory) New(_ context.Context, r Request) (Result, error) {
	f.got = r
	return Result{Provider: struct{}{}, Manifest: r.Manifest}, nil
}

func TestRegistryImmutableAndVersioned(t *testing.T) {
	f := &testFactory{id: "fake", version: "1"}
	r, err := New(2, f)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Resolve("fake", "2"); !errors.Is(err, ErrRuntimeVersion) {
		t.Fatalf("err=%v", err)
	}
	if _, err := r.Resolve("unknown", "1"); !errors.Is(err, ErrRuntimeNotFound) {
		t.Fatalf("err=%v", err)
	}
	req := Request{Manifest: core.CapabilityManifest{ID: "x", Version: "1"}, Headers: map[string]string{"x": "y"}}
	if _, err := r.Create(context.Background(), "fake", "1", req); err != nil {
		t.Fatal(err)
	}
	req.Headers["x"] = "mutated"
	if f.got.Headers["x"] != "y" {
		t.Fatal("factory received caller-owned headers")
	}
}

func TestRegistryRejectsDuplicateAndCapacity(t *testing.T) {
	if _, err := New(1, &testFactory{id: "x", version: "1"}, &testFactory{id: "x", version: "1"}); !errors.Is(err, ErrInvalidRegistry) {
		t.Fatalf("err=%v", err)
	}
	if _, err := New(1, &testFactory{id: "x", version: "1"}, &testFactory{id: "y", version: "1"}); !errors.Is(err, ErrInvalidRegistry) {
		t.Fatalf("err=%v", err)
	}
}

func TestRegistryCreateContextAndFactoryPanicFailClosed(t *testing.T) {
	f := &testFactory{id: "ctx", version: "1"}
	r, err := New(1, f)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.Create(ctx, "ctx", "1", Request{}); err == nil {
		t.Fatal("canceled context accepted")
	}
	if _, err := New(1, panicFactory{}); !errors.Is(err, ErrFactoryPanic) {
		t.Fatalf("panic err=%v", err)
	}
	r, err = New(1, panicNewFactory{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = r.Create(context.Background(), "panic-new", "1", Request{})
	if !errors.Is(err, ErrFactoryPanic) || strings.Contains(err.Error(), "new-secret") {
		t.Fatalf("panic leaked: %v", err)
	}
}

func TestRegistryListStableMultiVersionAndBounds(t *testing.T) {
	a := &testFactory{id: "same", version: "2"}
	b := &testFactory{id: "same", version: "1"}
	c := &testFactory{id: "other", version: "1"}
	r, err := New(MaxFactories, a, b, c)
	if err != nil {
		t.Fatal(err)
	}
	list := r.List()
	if len(list) != 3 || list[0].ID != "other" || list[1].Version != "1" || list[2].Version != "2" {
		t.Fatalf("list=%#v", list)
	}
	tooMany := make([]Factory, MaxFactories+1)
	for i := range tooMany {
		tooMany[i] = &testFactory{id: string(rune('a'+i%26)) + string(rune(i/26)), version: "1"}
	}
	if _, err := New(MaxFactories+1, tooMany...); !errors.Is(err, ErrInvalidRegistry) {
		t.Fatalf("hard cap err=%v", err)
	}
}

func TestRegistryRejectsInvalidIdentityAndNilKinds(t *testing.T) {
	for _, f := range []Factory{
		&testFactory{id: " bad", version: "1"}, &testFactory{id: "bad", version: "1\n"}, &testFactory{id: string([]byte{0xff}), version: "1"},
		&testFactory{id: strings.Repeat("x", 129), version: "1"}, &testFactory{id: "ok", version: " "},
	} {
		if _, err := New(1, f); !errors.Is(err, ErrInvalidRegistry) {
			t.Fatalf("accepted invalid factory %#v: %v", f, err)
		}
	}
	var p *testFactory
	if _, err := New(1, p); !errors.Is(err, ErrInvalidRegistry) {
		t.Fatalf("typed nil accepted: %v", err)
	}
	for _, v := range []any{(chan int)(nil), (func())(nil), (map[string]any)(nil), ([]string)(nil), (*int)(nil), (interface{})(nil)} {
		if !isNil(v) {
			t.Fatalf("nil kind not detected: %T", v)
		}
	}
}

func TestCloneSchemaNumbersAndInvalidValues(t *testing.T) {
	f := &testFactory{id: "clone", version: "1"}
	r, err := New(1, f)
	if err != nil {
		t.Fatal(err)
	}
	nested := map[string]any{"i": int8(1), "u": uint32(2), "f": float32(3.5), "nested": map[string]any{"x": []any{"y"}}}
	req := Request{Manifest: core.CapabilityManifest{ID: "x", Version: "1", InputSchema: nested, Tool: &core.ToolExposure{Parameters: nested}, Metadata: map[string]string{"k": "v"}}, Headers: map[string]string{"Accept": "application/json"}}
	got, err := r.Create(context.Background(), "clone", "1", req)
	if err != nil {
		t.Fatal(err)
	}
	nested["i"] = "changed"
	nested["nested"].(map[string]any)["x"].([]any)[0] = "changed"
	req.Headers["Accept"] = "changed"
	if got.Manifest.InputSchema["i"] != int8(1) || got.Manifest.InputSchema["nested"].(map[string]any)["x"].([]any)[0] != "y" {
		t.Fatal("nested alias leaked")
	}
	for _, bad := range []any{math.NaN(), math.Inf(1), make(chan int), func() {}} {
		if _, e := r.Create(context.Background(), "clone", "1", Request{Manifest: core.CapabilityManifest{ID: "x", Version: "1", InputSchema: map[string]any{"bad": bad}}}); !errors.Is(e, ErrInvalidRequest) {
			t.Fatalf("bad value accepted: %v", e)
		}
	}
}
