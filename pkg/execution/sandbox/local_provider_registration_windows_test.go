//go:build windows

package sandbox

import "testing"

func TestNewLocalRegistrationForWorkRootKeepsWindowsRootPrivateAndUnavailable(t *testing.T) {
	const root = `C:\harness-data\sandbox`
	registration, err := NewLocalRegistrationForWorkRoot(root)
	if err != nil {
		t.Fatalf("NewLocalRegistrationForWorkRoot: %v", err)
	}
	provider, ok := registration.Provider.(LocalProvider)
	if !ok {
		t.Fatalf("provider type=%T", registration.Provider)
	}
	if provider.workRoot != root {
		t.Fatalf("configured root=%q want %q", provider.workRoot, root)
	}
	provider.backend = unavailableWindowsBackend{}
	if report := provider.Probe(t.Context()); report.Available {
		t.Fatalf("unwired configured provider must remain unavailable: %#v", report)
	}
}

func TestNewLocalRegistrationForWorkRootRejectsNonCanonicalWindowsPath(t *testing.T) {
	for _, root := range []string{
		`relative`,
		`C:\harness-data\sandbox\..\sandbox`,
		`\\server\share\sandbox`,
		`\\?\C:\harness-data\sandbox`,
		`C:\harness-data\sandbox:alternate`,
	} {
		if _, err := NewLocalRegistrationForWorkRoot(root); err == nil {
			t.Fatalf("NewLocalRegistrationForWorkRoot(%q) succeeded", root)
		}
	}
}
