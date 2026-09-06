package main

import (
	"context"
	"path/filepath"
	"testing"
)

func TestSandboxProviderRegistryCompositionIsPlatformNeutral(t *testing.T) {
	registry, err := newSandboxProviderRegistry(t.TempDir())
	if err != nil {
		t.Fatalf("local sandbox registration must not block startup: %v", err)
	}
	metadata := registry.Metadata()
	if len(metadata) != 1 || metadata[0].ID != "local-ephemeral" || metadata[0].Version != "1" {
		t.Fatalf("unexpected sandbox registry metadata: %#v", metadata)
	}
	// A platform without bwrap/prlimit is represented by a safe unavailable
	// probe, not a composition error or an ordinary host-process fallback.
	report, err := registry.Probe(context.Background(), metadata[0].ID, metadata[0].Version)
	if err != nil {
		t.Fatalf("local sandbox probe must be reportable: %v", err)
	}
	if !report.Available && report.UnavailableCause != "sandbox provider unavailable" {
		t.Fatalf("unavailable probe cause=%q", report.UnavailableCause)
	}
}

func TestSandboxProviderRegistryRejectsUnresolvedRoot(t *testing.T) {
	if _, err := newSandboxProviderRegistry("relative-sandbox-root"); err == nil {
		t.Fatal("newSandboxProviderRegistry accepted a non-canonical root")
	}
}

func TestSandboxWorkRootForDataRootDerivesCanonicalSandboxChild(t *testing.T) {
	dataRoot := t.TempDir()
	workRoot, err := sandboxWorkRootForDataRoot(dataRoot)
	if err != nil {
		t.Fatalf("sandboxWorkRootForDataRoot(%q): %v", dataRoot, err)
	}
	want := filepath.Join(dataRoot, "sandbox")
	if workRoot != want || !filepath.IsAbs(workRoot) || filepath.Clean(workRoot) != workRoot {
		t.Fatalf("sandbox work root = %q, want canonical %q", workRoot, want)
	}
}
