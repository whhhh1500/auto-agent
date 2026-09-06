//go:build windows

package sandboxexec

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestWindowsCanonicalDrivePath(t *testing.T) {
	validRoot := t.TempDir()
	for _, test := range []struct {
		name string
		path string
		want bool
	}{
		{name: "canonical drive", path: validRoot, want: true},
		{name: "relative", path: `artifact.txt`},
		{name: "relative traversal", path: `..\artifact.txt`},
		{name: "unc", path: `\\server\share\artifact.txt`},
		{name: "device", path: `\\?\C:\artifact.txt`},
		{name: "win32 device", path: `\\.\C:\artifact.txt`},
		{name: "alternate data stream", path: validRoot + `\artifact.txt:stream`},
		{name: "noncanonical traversal", path: validRoot + `\nested\..\artifact.txt`},
		{name: "noncanonical trailing separator", path: validRoot + `\`},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := windowsCanonicalDrivePath(test.path); got != test.want {
				t.Fatalf("windowsCanonicalDrivePath(%q)=%v, want %v", test.path, got, test.want)
			}
		})
	}
	for _, path := range []string{`artifact.txt`, `\\server\share\artifact.txt`, `\\?\C:\artifact.txt`, `C:\artifact.txt:stream`} {
		if _, ok := windowsArtifactPathIdentityFor(path, false); ok {
			t.Fatalf("unsafe path reached handle identity proof: %q", path)
		}
	}
	if strings.HasPrefix(validRoot, `\\`) {
		t.Fatalf("test root unexpectedly is not drive-rooted: %q", validRoot)
	}
}

func TestWindowsArtifactPathProofAcceptsOrdinaryNestedFile(t *testing.T) {
	root := t.TempDir()
	nested := filepath.Join(root, "nested")
	if err := os.Mkdir(nested, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(nested, "artifact.txt")
	if err := os.WriteFile(path, []byte("artifact"), 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	identity, ok := windowsArtifactPathIdentityFor(path, false)
	if !ok || identity.fileIndex == 0 || !artifactPlatformPathSafe(path, info) || !safeArtifactFile(path, info) {
		t.Fatalf("ordinary file path was not proven safe: identity=%#v ok=%v", identity, ok)
	}
}

func TestWindowsArtifactPathProofRejectsSymlinkIntermediateWhenAvailable(t *testing.T) {
	root := t.TempDir()
	target := t.TempDir()
	if err := os.WriteFile(filepath.Join(target, "escaped.txt"), []byte("escaped"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("directory symlink capability unavailable: %v", err)
	}
	escaped := filepath.Join(link, "escaped.txt")
	info, err := os.Lstat(escaped)
	if err != nil {
		t.Fatal(err)
	}
	if artifactPlatformPathSafe(escaped, info) || safeArtifactFile(escaped, info) {
		t.Fatal("artifact path proof followed an intermediate directory symlink")
	}
}

func TestWindowsArtifactPathProofRejectsJunctionIntermediateWhenAvailable(t *testing.T) {
	root := t.TempDir()
	target := t.TempDir()
	if err := os.WriteFile(filepath.Join(target, "escaped.txt"), []byte("escaped"), 0o600); err != nil {
		t.Fatal(err)
	}
	junction := filepath.Join(root, "junction")
	command := exec.Command("cmd.exe", "/d", "/c", "mklink", "/J", junction, target)
	if output, err := command.CombinedOutput(); err != nil {
		t.Skipf("junction capability unavailable: %v (%s)", err, output)
	}
	escaped := filepath.Join(junction, "escaped.txt")
	info, err := os.Lstat(escaped)
	if err != nil {
		t.Fatal(err)
	}
	if artifactPlatformPathSafe(escaped, info) || safeArtifactFile(escaped, info) {
		t.Fatal("artifact path proof followed an intermediate directory junction")
	}
}
