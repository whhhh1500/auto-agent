package execution

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/whhhh1500/auto-agent/pkg/core"
)

func writeWasm(t testing.TB, module []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fixture.wasm")
	if err := os.WriteFile(path, module, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

// Small actual WASM binaries keep the boundary tests independent of a guest
// compiler. Each section uses the standard WASM unsigned LEB128 encoding.
func wasmU32(value uint32) []byte {
	var result []byte
	for value >= 128 {
		result = append(result, byte(value)|128)
		value >>= 7
	}
	return append(result, byte(value))
}

func wasmSection(id byte, content []byte) []byte {
	return append(append([]byte{id}, wasmU32(uint32(len(content)))...), content...)
}

func wasmStartModule(pages uint32, instructions ...byte) []byte {
	module := []byte("\x00asm\x01\x00\x00\x00")
	module = append(module, wasmSection(1, []byte{1, 0x60, 0, 0})...)
	module = append(module, wasmSection(3, []byte{1, 0})...)
	module = append(module, wasmSection(5, append([]byte{1, 0}, wasmU32(pages)...))...)
	module = append(module, wasmSection(7, []byte{1, 6, '_', 's', 't', 'a', 'r', 't', 0, 0})...)
	body := append([]byte{0}, instructions...) // no locals
	body = append(body, 0x0b)                  // end function
	code := append([]byte{1}, wasmU32(uint32(len(body)))...)
	module = append(module, wasmSection(10, append(code, body...))...)
	return module
}

func TestWazeroDefaultRejectsExcessInitialMemory(t *testing.T) {
	path := writeWasm(t, wasmStartModule(2049)) // 128 MiB plus one page
	_, err := (WazeroExecutor{}).Run(context.Background(), path, nil)
	if err == nil || !strings.Contains(err.Error(), "memory") {
		t.Fatalf("module exceeded the default linear-memory limit: %v", err)
	}
}

// A regression in loop cancellation must fail the test without leaving a
// forever-running guest goroutine in the parent test process.
func TestWazeroInterruptsGuestLoop(t *testing.T) {
	for _, mode := range []string{"deadline", "cancel", "executor_timeout", "cached_deadline"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestWazeroCancellationProcess$")
			childTemp := t.TempDir()
			command.Env = append(os.Environ(), "HARNESS_WASM_CANCEL_TEST="+mode, "TEMP="+childTemp, "TMP="+childTemp, "TMPDIR="+childTemp)
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("guest cancellation failed: parent_timeout=%t error=%v output=%s", ctx.Err() != nil, err, output)
			}
		})
	}
}

func TestWazeroCancellationProcess(t *testing.T) {
	mode := os.Getenv("HARNESS_WASM_CANCEL_TEST")
	if mode == "" {
		t.Skip("only run in the bounded cancellation subprocess")
	}
	path := writeWasm(t, wasmStartModule(1, 0x03, 0x40, 0x0c, 0, 0x0b)) // loop { br 0 }
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	want := context.DeadlineExceeded
	if mode == "cancel" {
		cancel()
		ctx, cancel = context.WithCancel(context.Background())
		timer := time.AfterFunc(100*time.Millisecond, cancel)
		defer timer.Stop()
		want = context.Canceled
	}
	defer cancel()
	executor := WazeroExecutor{}
	if mode == "executor_timeout" {
		ctx = context.Background()
		executor.Timeout = 100 * time.Millisecond
	}
	if mode == "cached_deadline" {
		cache := wazero.NewCompilationCache()
		defer cache.Close(context.Background())
		executor.CompilationCache = cache
		_, err := executor.Run(ctx, path, nil)
		if !errors.Is(err, want) {
			t.Fatalf("cold cache loop: %v", err)
		}
		ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
	}
	_, _ = os.Stdout.WriteString("entering actual guest loop\n")
	_, err := executor.Run(ctx, path, nil)
	if !errors.Is(err, want) {
		t.Fatalf("guest cancellation error=%v want=%v", err, want)
	}
	path = writeWasm(t, wasmStartModule(1))
	if _, err := executor.Run(context.Background(), path, nil); err != nil {
		t.Fatalf("next invocation after cancellation failed: %v", err)
	}
}

func TestWazeroMemoryGrowth(t *testing.T) {
	for _, tc := range []struct {
		name        string
		grow, prior byte
	}{
		{"within_limit", 1, 1},
		{"over_limit", 2, 0x7f}, // memory.grow returns i32(-1) on failure
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := writeWasm(t, wasmStartModule(1,
				0x41, tc.grow, 0x40, 0, 0x41, tc.prior, 0x47, 0x04, 0x40, 0, 0x0b))
			if _, err := (WazeroExecutor{MemoryLimitPages: 2}).Run(context.Background(), path, nil); err != nil {
				t.Fatalf("memory.grow did not observe limit: %v", err)
			}
		})
	}
}

func TestWazeroCacheKeepsInvocationsIsolated(t *testing.T) {
	ctx := context.Background()
	cache := wazero.NewCompilationCache()
	defer cache.Close(ctx)
	executor := WazeroExecutor{CompilationCache: cache, MemoryLimitPages: 4}
	// Increment memory[0] and trap unless it is exactly 1. Reusing an instance
	// instead of just compiled code makes the second invocation fail.
	path := writeWasm(t, wasmStartModule(1,
		0x41, 0, 0x41, 0, 0x28, 2, 0, 0x41, 1, 0x6a, 0x36, 2, 0,
		0x41, 0, 0x28, 2, 0, 0x41, 1, 0x47, 0x04, 0x40, 0, 0x0b))
	for i := 0; i < 3; i++ {
		if _, err := executor.Run(ctx, path, nil); err != nil {
			t.Fatalf("invocation %d reused guest state or closed caller cache: %v", i, err)
		}
	}
	// Also enforce growth at execution time when initial memory fits both
	// policies. Access to page 3 succeeds only if memory.grow(2) succeeded.
	growth := writeWasm(t, wasmStartModule(1,
		0x41, 2, 0x40, 0, 0x1a, 0x41, 0x80, 0x80, 8, 0x2d, 0, 0, 0x1a))
	if _, err := executor.Run(ctx, growth, nil); err != nil {
		t.Fatal(err)
	}
	strict := executor
	strict.MemoryLimitPages = 2
	if _, err := strict.Run(ctx, growth, nil); err == nil || !strings.Contains(err.Error(), "out of bounds") {
		t.Fatalf("cached code grew past the stricter runtime limit: %v", err)
	}
	path = writeWasm(t, wasmStartModule(3))
	if _, err := executor.Run(ctx, path, nil); err != nil {
		t.Fatal(err)
	}
	executor.MemoryLimitPages = 2
	if _, err := executor.Run(ctx, path, nil); err == nil || !strings.Contains(err.Error(), "memory") {
		t.Fatalf("cached code bypassed stricter memory policy: %v", err)
	}
	// Replacing bytes at an existing path must not reuse the old artifact.
	if err := os.WriteFile(path, wasmStartModule(1, 0), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := executor.Run(ctx, path, nil); err == nil || !strings.Contains(err.Error(), "unreachable") {
		t.Fatalf("cache ignored module content replacement: %v", err)
	}
}

func TestWazeroConfiguration(t *testing.T) {
	path := writeWasm(t, wasmStartModule(1))
	for _, executor := range []WazeroExecutor{{MemoryLimitPages: 65537}, {Timeout: -time.Second}} {
		if _, err := executor.Run(context.Background(), path, nil); err == nil {
			t.Fatal("invalid policy accepted")
		}
	}
	//lint:ignore SA1012 Exercise the executor's explicit nil-context rejection.
	if _, err := (WazeroExecutor{}).Run(nil, path, nil); err == nil {
		t.Fatal("nil context accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (WazeroExecutor{}).Run(ctx, "missing.wasm", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("already canceled call performed file I/O: %v", err)
	}
	defaultRevision := (WazeroExecutor{}).ArtifactRevision()
	if defaultRevision != (WazeroExecutor{MemoryLimitPages: 2048, Timeout: 30 * time.Second}).ArtifactRevision() {
		t.Fatal("equivalent resource policies have different revisions")
	}
	if defaultRevision == (WazeroExecutor{MemoryLimitPages: 1}).ArtifactRevision() || defaultRevision == (WazeroExecutor{Timeout: time.Second}).ArtifactRevision() {
		t.Fatal("resource policy change did not alter artifact revision")
	}
}

func wasmOutputModule(text string) []byte {
	// Imports fd_write, defines _start, and exports memory. fd_write receives
	// one iovec at address 0, with text at 16 and the written count at 8.
	module := []byte("\x00asm\x01\x00\x00\x00")
	module = append(module, wasmSection(1, []byte{2, 0x60, 4, 0x7f, 0x7f, 0x7f, 0x7f, 1, 0x7f, 0x60, 0, 0})...)
	imports := []byte{1, 22}
	imports = append(imports, "wasi_snapshot_preview1"...)
	imports = append(imports, 8)
	imports = append(imports, "fd_write"...)
	module = append(module, wasmSection(2, append(imports, 0, 0))...)
	module = append(module, wasmSection(3, []byte{1, 1})...)
	pages := uint32((len(text) + 16 + 65535) / 65536)
	module = append(module, wasmSection(5, append([]byte{1, 0}, wasmU32(pages)...))...)
	module = append(module, wasmSection(7, []byte{2, 6, '_', 's', 't', 'a', 'r', 't', 0, 1, 6, 'm', 'e', 'm', 'o', 'r', 'y', 2, 0})...)
	body := []byte{0, 0x41, 1, 0x41, 0, 0x41, 1, 0x41, 8, 0x10, 0, 0x1a, 0x0b}
	module = append(module, wasmSection(10, append([]byte{1, byte(len(body))}, body...))...)
	data := make([]byte, 16)
	binary.LittleEndian.PutUint32(data, 16)
	binary.LittleEndian.PutUint32(data[4:], uint32(len(text)))
	data = append(data, text...)
	segment := append([]byte{1, 0, 0x41, 0, 0x0b}, wasmU32(uint32(len(data)))...)
	return append(module, wasmSection(11, append(segment, data...))...)
}

func TestWazeroWASIOutputAndTrap(t *testing.T) {
	ctx := context.Background()
	cache := wazero.NewCompilationCache()
	defer cache.Close(ctx)
	executor := WazeroExecutor{CompilationCache: cache}
	for _, tc := range []struct {
		name, output string
		module       []byte
		wantError    string
	}{
		{"stdout", "fixture-甲", wasmOutputModule("fixture-甲"), ""},
		{"output_overflow", "", wasmOutputModule(strings.Repeat("x", maxLocalExecOutputBytes+1)), "output exceeds"},
		{"trap", "", wasmStartModule(1, 0), "unreachable"},
		{"invalid_binary", "", []byte("invalid"), "magic"},
		{"after_failure", "next", wasmOutputModule("next"), ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := executor.Execute(ctx, core.ExecutionSpec{Runtime: "wazero", Entrypoint: writeWasm(t, tc.module)}, core.CapabilityRequest{})
			if tc.wantError != "" {
				if err == nil || result.OK || !strings.Contains(err.Error(), tc.wantError) {
					t.Fatalf("expected failure %q: result=%+v error=%v", tc.wantError, result, err)
				}
			} else if err != nil || !result.OK || result.Content != tc.output {
				t.Fatalf("result=%+v error=%v", result, err)
			}
		})
	}
}

func TestWazeroModuleFileBounds(t *testing.T) {
	path := writeWasm(t, nil)
	if err := os.Truncate(path, maxWasmModuleBytes+1); err != nil {
		t.Fatal(err)
	}
	if _, err := (WazeroExecutor{}).Run(context.Background(), path, nil); err == nil || !strings.Contains(err.Error(), "module exceeds") {
		t.Fatalf("oversized module accepted: %v", err)
	}
	if _, err := (WazeroExecutor{}).Run(context.Background(), t.TempDir(), nil); err == nil || !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("non-regular module accepted: %v", err)
	}
}
