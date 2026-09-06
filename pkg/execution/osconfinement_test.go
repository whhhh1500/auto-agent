package execution

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestOSConfinementNilContextFailsClosed(t *testing.T) {
	var nilContext context.Context
	_, err := (OSConfinementExecutor{}).Run(nilContext, OSExecSpec{Command: "/bin/true"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("nil context did not fail closed: %v", err)
	}
}

func TestValidateOSWorkdirRejectsSymlink(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink creation unavailable: %v", err)
	}
	if _, err := validateOSWorkdir(link); err == nil {
		t.Fatal("symlink workdir was accepted")
	}
}
