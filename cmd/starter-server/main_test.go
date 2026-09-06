package main

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestStarterRejectsNonDevelopmentModes(t *testing.T) {
	for _, mode := range []string{"", "dev"} {
		if err := starterModeAllowed(mode); err != nil {
			t.Fatalf("mode %q rejected: %v", mode, err)
		}
	}
	for _, mode := range []string{"demo", "production", "unexpected"} {
		if err := starterModeAllowed(mode); err == nil {
			t.Fatalf("mode %q was accepted", mode)
		}
	}
}

func TestPrivateMkdirAllUsesPrivatePOSIXMode(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows uses host ACLs rather than POSIX mode bits")
	}
	dir := t.TempDir() + string(os.PathSeparator) + "nested"
	if err := privateMkdirAll(dir); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o700 {
		t.Fatalf("starter SQLite directory mode = %04o, want 0700", got)
	}
}

func TestSessionStoreUsesPrivateSQLiteFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "starter.db")
	t.Setenv("HARNESS_STARTER_SQLITE_PATH", path)
	_, closeStore := sessionStore()
	defer closeStore()
	if runtime.GOOS == "windows" {
		return
	}
	for _, test := range []struct {
		path string
		want os.FileMode
	}{{filepath.Dir(path), 0o700}, {path, 0o600}} {
		info, err := os.Stat(test.path)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != test.want {
			t.Fatalf("%s mode = %04o, want %04o", test.path, got, test.want)
		}
	}
}

func TestSessionStoreAcceptsSQLiteMemoryDSN(t *testing.T) {
	t.Setenv("HARNESS_STARTER_SQLITE_PATH", ":memory:")
	_, closeStore := sessionStore()
	defer closeStore()
}
