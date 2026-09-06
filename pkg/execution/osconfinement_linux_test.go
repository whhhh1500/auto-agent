//go:build linux

package execution

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

func TestBwrapCommandUsesMinimalRootAndClearsEnvironment(t *testing.T) {
	if _, err := exec.LookPath("bwrap"); err != nil {
		t.Skipf("bwrap unavailable: %v", err)
	}
	argv, err := confineCommand("/bin/sh", nil, core.SandboxPolicy{Mode: core.SandboxReadOnly}, "")
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(argv, " ")
	if strings.Contains(joined, "--ro-bind / /") {
		t.Fatalf("host root was mounted: %s", joined)
	}
	for _, required := range []string{"--clearenv", "--ro-bind /usr /usr", "--proc /proc", "--dev /dev"} {
		if !strings.Contains(joined, required) {
			t.Fatalf("minimal bwrap argument missing %q: %s", required, joined)
		}
	}
}

func TestOSConfinementReadOnlyRunsCommand(t *testing.T) {
	output, err := (OSConfinementExecutor{}).Run(context.Background(), OSExecSpec{
		Command: "/bin/sh",
		Args:    []string{"-c", "printf confined"},
		Policy:  core.SandboxPolicy{Mode: core.SandboxReadOnly},
	})
	if err != nil || output != "confined" {
		t.Fatalf("read-only confinement failed: output=%q err=%v", output, err)
	}
}

func TestOSConfinementReadOnlyRejectsDeclaredWrites(t *testing.T) {
	_, err := (OSConfinementExecutor{}).Run(context.Background(), OSExecSpec{
		Command: "/bin/true",
		Policy:  core.SandboxPolicy{Mode: core.SandboxReadOnly},
		Writes:  true,
	})
	if err == nil || !strings.Contains(err.Error(), "write denied") {
		t.Fatalf("declared write was not denied: %v", err)
	}
}

func TestOSConfinementReadOnlyFilesystemRejectsActualWrite(t *testing.T) {
	target := filepath.Join(t.TempDir(), "must-not-exist.txt")
	_, err := (OSConfinementExecutor{}).Run(context.Background(), OSExecSpec{
		Command: "/bin/sh",
		Args:    []string{"-c", `printf denied > "$1"`, "_", target},
		Policy:  core.SandboxPolicy{Mode: core.SandboxReadOnly},
	})
	if err == nil {
		t.Fatal("read-only bubblewrap execution unexpectedly wrote to the filesystem")
	}
	if _, statErr := os.Stat(target); !os.IsNotExist(statErr) {
		t.Fatalf("read-only execution created the target: %v", statErr)
	}
}

func TestOSConfinementWorkspaceWriteCanWriteOnlyBoundWorkspace(t *testing.T) {
	workspace := t.TempDir()
	target := filepath.Join(workspace, "result.txt")
	_, err := (OSConfinementExecutor{}).Run(context.Background(), OSExecSpec{
		Command: "/bin/sh",
		Args:    []string{"-c", `printf ok > "$1"`, "_", target},
		Policy:  core.SandboxPolicy{Mode: core.SandboxWorkspaceWrite},
		Workdir: workspace,
		Writes:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(target)
	if err != nil || string(content) != "ok" {
		t.Fatalf("workspace output missing: content=%q err=%v", content, err)
	}
}
