// Package testwasm builds the repository's Go WASI computation fixture.
package testwasm

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// Calc returns a compiled internal/calc module. An explicitly supplied artifact
// lets a serial acceptance run reuse the same bytes as its benchmarks.
func Calc(t testing.TB) string {
	t.Helper()
	if path := os.Getenv("HARNESS_TEST_WASM_CALC"); path != "" {
		info, err := os.Stat(path)
		if err != nil || !info.Mode().IsRegular() {
			t.Fatal("HARNESS_TEST_WASM_CALC must identify an existing regular module file")
		}
		return path
	}
	path := filepath.Join(t.TempDir(), "calc.wasm")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, "go", "build", "-p", "1", "-trimpath", "-o", path, "github.com/cc-auto-agent/harness-core/internal/calc")
	command.Env = append(os.Environ(), "GOOS=wasip1", "GOARCH=wasm", "CGO_ENABLED=0")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("compile repository WASI calc: %v\n%s", err, output)
	}
	return path
}
