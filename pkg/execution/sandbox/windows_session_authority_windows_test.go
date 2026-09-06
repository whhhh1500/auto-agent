//go:build windows

package sandbox

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
)

func windowsSessionAuthorityTestPaths(t *testing.T) (workRoot, sessionRoot, workspace, artifacts string) {
	t.Helper()
	workRoot = filepath.Join(t.TempDir(), "sandbox")
	sessionRoot = filepath.Join(workRoot, "sessions", "session-0123456789abcdef0123456789abcdef")
	workspace = filepath.Join(sessionRoot, "workspace")
	artifacts = filepath.Join(sessionRoot, "artifacts")
	for _, path := range []string{
		workRoot,
		filepath.Dir(sessionRoot),
		sessionRoot,
		workspace,
		artifacts,
		filepath.Join(sessionRoot, "home"),
		filepath.Join(sessionRoot, "tmp"),
		filepath.Join(sessionRoot, "cache"),
	} {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatalf("mkdir %q: %v", path, err)
		}
	}
	return workRoot, sessionRoot, workspace, artifacts
}

func TestWindowsSessionAuthorityBindsCandidateToConfiguredRoot(t *testing.T) {
	workRoot, sessionRoot, workspace, artifacts := windowsSessionAuthorityTestPaths(t)
	authority, err := newWindowsSessionAuthority(context.Background(), workRoot, sessionRoot, workspace, artifacts)
	if err != nil {
		t.Fatalf("newWindowsSessionAuthority: %v", err)
	}
	t.Cleanup(func() { _ = authority.Close(context.Background()) })
	if err := authority.Verify(context.Background()); err != nil {
		t.Fatalf("Verify: %v", err)
	}

	otherVolume := `Z:`
	if volume := filepath.VolumeName(workRoot); len(volume) == 2 && volume[0] == 'Z' {
		otherVolume = `Y:`
	}
	for name, paths := range map[string][3]string{
		"sessions lookalike": {
			filepath.Join(workRoot, "sessionsx", filepath.Base(sessionRoot)), workspace, artifacts,
		},
		"outside configured root": {
			filepath.Join(t.TempDir(), "sessions", filepath.Base(sessionRoot)), workspace, artifacts,
		},
		"upper-case token": {
			filepath.Join(workRoot, "sessions", "session-0123456789ABCDEF0123456789ABCDEF"), workspace, artifacts,
		},
		"cross volume": {
			filepath.Join(otherVolume+`\`, "sandbox", "sessions", filepath.Base(sessionRoot)), workspace, artifacts,
		},
		"wrong workspace": {
			sessionRoot, filepath.Join(sessionRoot, "workspaces"), artifacts,
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := newWindowsSessionAuthority(context.Background(), workRoot, paths[0], paths[1], paths[2]); !errors.Is(err, ErrInvalidSpec) {
				t.Fatalf("newWindowsSessionAuthority error=%v, want ErrInvalidSpec", err)
			}
		})
	}
}

func TestWindowsSessionAuthorityRejectsMissingOrReplacedFixedDirectory(t *testing.T) {
	for _, name := range []string{"workspace", "artifacts", "home", "tmp", "cache"} {
		t.Run("missing "+name, func(t *testing.T) {
			workRoot, sessionRoot, workspace, artifacts := windowsSessionAuthorityTestPaths(t)
			authority, err := newWindowsSessionAuthority(context.Background(), workRoot, sessionRoot, workspace, artifacts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = authority.Close(context.Background()) })
			if err := os.Remove(filepath.Join(sessionRoot, name)); err != nil {
				t.Fatal(err)
			}
			if err := authority.Verify(context.Background()); !errors.Is(err, ErrArtifactUnverified) {
				t.Fatalf("missing %s accepted: %v", name, err)
			}
		})

		t.Run("replaced "+name, func(t *testing.T) {
			workRoot, sessionRoot, workspace, artifacts := windowsSessionAuthorityTestPaths(t)
			authority, err := newWindowsSessionAuthority(context.Background(), workRoot, sessionRoot, workspace, artifacts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = authority.Close(context.Background()) })
			path := filepath.Join(sessionRoot, name)
			if err := os.Rename(path, path+".replaced"); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(path, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := authority.Verify(context.Background()); !errors.Is(err, ErrArtifactUnverified) {
				t.Fatalf("replacement %s accepted: %v", name, err)
			}
		})
	}

	t.Run("junction candidate", func(t *testing.T) {
		junctionRoot := filepath.Join(t.TempDir(), "sandbox")
		sessions := filepath.Join(junctionRoot, "sessions")
		if err := os.Mkdir(junctionRoot, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(sessions, 0o700); err != nil {
			t.Fatal(err)
		}
		candidate := filepath.Join(sessions, "session-0123456789abcdef0123456789abcdef")
		target := t.TempDir()
		output, commandErr := exec.Command("cmd.exe", "/d", "/c", "mklink", "/J", candidate, target).CombinedOutput()
		if commandErr != nil {
			t.Skipf("junction creation unavailable: %v (%s)", commandErr, output)
		}
		if _, err := newWindowsSessionAuthority(context.Background(), junctionRoot, candidate, filepath.Join(candidate, "workspace"), filepath.Join(candidate, "artifacts")); !errors.Is(err, ErrArtifactUnverified) {
			t.Fatalf("junction candidate accepted: %v", err)
		}
	})
}

func TestWindowsSessionAuthorityCloseIsConcurrentAndContextAware(t *testing.T) {
	workRoot, sessionRoot, workspace, artifacts := windowsSessionAuthorityTestPaths(t)
	authority, err := newWindowsSessionAuthority(context.Background(), workRoot, sessionRoot, workspace, artifacts)
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := authority.Verify(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Verify error=%v", err)
	}
	if err := authority.Close(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Close error=%v", err)
	}

	const closers = 16
	errs := make(chan error, closers)
	var group sync.WaitGroup
	for range closers {
		group.Add(1)
		go func() {
			defer group.Done()
			errs <- authority.Close(context.Background())
		}()
	}
	group.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent Close error=%v", err)
		}
	}
	if err := authority.Verify(context.Background()); !errors.Is(err, ErrArtifactUnverified) {
		t.Fatalf("closed authority verified: %v", err)
	}
}

func TestWindowsSessionAuthorityCloseRetainsFailedHandleForRetry(t *testing.T) {
	workRoot, sessionRoot, workspace, artifacts := windowsSessionAuthorityTestPaths(t)
	authority, err := newWindowsSessionAuthority(context.Background(), workRoot, sessionRoot, workspace, artifacts)
	if err != nil {
		t.Fatal(err)
	}
	previous := windowsSessionAuthorityCloseDirectory
	failed := false
	windowsSessionAuthorityCloseDirectory = func(directory *windowsRetainedDirectory) error {
		if !failed && directory == authority.artifacts {
			failed = true
			return errors.New("injected close failure")
		}
		return directory.close()
	}
	t.Cleanup(func() {
		windowsSessionAuthorityCloseDirectory = previous
		_ = authority.Close(context.Background())
	})

	if err := authority.Close(context.Background()); !errors.Is(err, ErrArtifactUnverified) {
		t.Fatalf("first Close error=%v", err)
	}
	if authority.artifacts == nil || authority.closed {
		t.Fatalf("failed close discarded retry state: artifacts=%v closed=%v", authority.artifacts, authority.closed)
	}
	if err := authority.Close(context.Background()); err != nil {
		t.Fatalf("retry Close error=%v", err)
	}
	if !authority.closed || authority.artifacts != nil {
		t.Fatalf("successful retry did not close authority: artifacts=%v closed=%v", authority.artifacts, authority.closed)
	}
}
