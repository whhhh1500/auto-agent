package storage

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestPrivateFilesystemHelpersRespectPlatformPolicy(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "private")
	if err := privateMkdirAll(dir); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "secret")
	if err := privateWriteFile(path, []byte("secret")); err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS == "windows" {
		if privateFilesystemEnforced() {
			t.Fatal("Windows must not claim POSIX mode enforcement")
		}
		return
	}
	if !privateFilesystemEnforced() {
		t.Fatal("non-Windows must enforce private modes")
	}
	for _, test := range []struct {
		path string
		want os.FileMode
	}{{dir, 0o700}, {path, 0o600}} {
		info, err := os.Stat(test.path)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != test.want {
			t.Fatalf("%s mode=%#o want %#o", test.path, got, test.want)
		}
	}
}

func TestFileStoresUsePrivatePaths(t *testing.T) {
	base := t.TempDir()
	sessions, err := NewFileSessionStore(filepath.Join(base, "sessions"))
	if err != nil {
		t.Fatal(err)
	}
	objects, err := NewFileObjectStore(filepath.Join(base, "objects"))
	if err != nil {
		t.Fatal(err)
	}
	if err := sessions.Create(context.Background(), mustNamedSession(t, "private-session")); err != nil {
		t.Fatal(err)
	}
	if _, err := objects.Put(context.Background(), "tenant/object", []byte("private"), PutOptions{}); err != nil {
		t.Fatal(err)
	}
	objectPath, err := objects.path("tenant/object")
	if err != nil {
		t.Fatal(err)
	}
	paths := []string{sessions.dir, filepath.Join(sessions.dir, "private-session.jsonl"), objects.root, filepath.Dir(objectPath), objectPath}
	if runtime.GOOS == "windows" {
		return
	}
	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		want := os.FileMode(0o600)
		if info.IsDir() {
			want = 0o700
		}
		if got := info.Mode().Perm(); got != want {
			t.Fatalf("%s mode=%#o want %#o", path, got, want)
		}
	}
}
