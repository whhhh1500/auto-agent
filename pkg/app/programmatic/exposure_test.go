package programmatic

import (
	"errors"
	"testing"

	"github.com/whhhh1500/auto-agent/pkg/core"
)

func TestProjectFiltersByExplicitVersionAndUsesToolContract(t *testing.T) {
	projected := capability("program.read")
	projected.Manifest.Metadata = map[string]string{ExposureKey: ExposureVersion}
	projected.Manifest.Tool.Parameters = map[string]any{"type": "object", "properties": map[string]any{"tool": map[string]any{"type": "string"}}}
	projected.Manifest.InputSchema = map[string]any{"type": "object", "properties": map[string]any{"input": map[string]any{"type": "string"}}}
	projected.Manifest.OutputSchema = map[string]any{"type": "object", "properties": map[string]any{"ok": map[string]any{"type": "boolean"}}}
	projected.Manifest.MaxOutputBytes = 77
	ignored := capability("model.only")

	catalog, err := Project([]core.SnapshotCapability{ignored, projected})
	if err != nil {
		t.Fatal(err)
	}
	descriptors := catalog.Descriptors()
	if len(descriptors) != 1 {
		t.Fatalf("descriptors=%#v", descriptors)
	}
	got := descriptors[0]
	if got.Schema.Name != projected.Manifest.ID || got.Schema.Parameters["properties"].(map[string]any)["tool"] == nil || got.CapabilityVersion != "1.0.0" || got.MaxOutputBytes != 77 || got.BindingDigest == "" {
		t.Fatalf("unexpected descriptor: %#v", got)
	}
	if got.OutputSchema["properties"].(map[string]any)["ok"] == nil {
		t.Fatalf("output contract was not copied: %#v", got.OutputSchema)
	}
}

func TestProjectFailsClosedForUnknownMarkerAndDuplicates(t *testing.T) {
	unknown := capability("program.unknown")
	unknown.Manifest.Metadata = map[string]string{ExposureKey: "2"}
	if _, err := Project([]core.SnapshotCapability{unknown}); !errors.Is(err, ErrInvalidCatalog) {
		t.Fatalf("unknown marker error=%v", err)
	}
	first := capability("program.duplicate")
	second := capability("program.duplicate")
	if _, err := Project([]core.SnapshotCapability{first, second}); !errors.Is(err, ErrInvalidCatalog) {
		t.Fatalf("duplicate error=%v", err)
	}
}

func TestCatalogBindingSubsetAndDefensiveCopies(t *testing.T) {
	first := capability("program.first")
	first.Manifest.Metadata = map[string]string{ExposureKey: ExposureVersion}
	first.Manifest.Tool.Parameters = map[string]any{"type": "object", "properties": map[string]any{"value": map[string]any{"type": "string"}}}
	second := capability("program.second")
	second.Manifest.Metadata = map[string]string{ExposureKey: ExposureVersion}
	catalog, err := Project([]core.SnapshotCapability{second, first})
	if err != nil {
		t.Fatal(err)
	}
	descriptors := catalog.Descriptors()
	if descriptors[0].Schema.Name != first.Manifest.ID || descriptors[1].Schema.Name != second.Manifest.ID {
		t.Fatalf("catalog order=%#v", descriptors)
	}
	binding := map[string]string{first.Manifest.ID: descriptors[0].BindingDigest}
	if err := catalog.ValidateBindings(binding); err != nil {
		t.Fatal(err)
	}
	if err := catalog.ValidateBindings(nil); err != nil {
		t.Fatalf("an empty program tool set should be valid: %v", err)
	}
	if err := catalog.ValidateBindings(map[string]string{"program.missing": descriptors[0].BindingDigest}); !errors.Is(err, ErrBindingMismatch) {
		t.Fatalf("missing binding error=%v", err)
	}
	descriptors[0].Schema.Parameters["type"] = "array"
	again, ok := catalog.Resolve(first.Manifest.ID, binding[first.Manifest.ID])
	if !ok || again.Schema.Parameters["type"] != "object" {
		t.Fatalf("descriptor mutation escaped: %#v", again)
	}
	changed := first
	changed.ProviderRevision = "provider-v2"
	changedCatalog, err := Project([]core.SnapshotCapability{changed})
	if err != nil {
		t.Fatal(err)
	}
	if changedCatalog.Descriptors()[0].BindingDigest == binding[first.Manifest.ID] {
		t.Fatal("provider revision did not change binding digest")
	}
}

func capability(id string) core.SnapshotCapability {
	return core.SnapshotCapability{Manifest: core.CapabilityManifest{
		ID: id, Version: "1.0.0", Name: id, Kind: core.KindTool,
		Tool: &core.ToolExposure{Description: "A public tool", Parameters: map[string]any{"type": "object"}},
	}, ProviderRevision: "provider-v1"}
}
