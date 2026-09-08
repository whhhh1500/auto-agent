package execution

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
	"github.com/whhhh1500/auto-agent/pkg/core"
)

// WazeroExecutor runs a pure-computation capability as an EMBEDDED WASM module.
// Each call owns an in-process runtime and fresh guest memory. The zero value
// limits linear memory to 128 MiB and guest execution to 30 seconds. These are
// not limits on total host RSS or hard preemption of file reads / compilation.
//
// Note: wazero is capability-secure by default (the module has no permissions
// unless you grant WASI). It cannot run arbitrary Python/native binaries, and it
// has no network unless you expose host functions — so it is for compute, not
// for a `connector` that needs to call an external API.
type WazeroExecutor struct {
	// MemoryLimitPages bounds guest linear memory in 64 KiB pages. Zero uses
	// 2048 pages (128 MiB); values greater than 65536 are rejected.
	MemoryLimitPages uint32
	// Timeout limits a call in addition to the caller's context. Zero uses
	// 30 seconds; negative values are rejected. Guest loops observe cancellation.
	Timeout time.Duration
	// CompilationCache optionally reuses compiled code across isolated calls.
	// The caller owns its lifetime, module population and Close. This executor
	// never closes it. A cache does not share guest memory, argv or output.
	CompilationCache wazero.CompilationCache
}

func (WazeroExecutor) Runtime() string { return "wazero" }
func (w WazeroExecutor) ArtifactRevision() string {
	pages, timeout := w.limits()
	return fmt.Sprintf("wazero-executor/v3-resource-bounds/pages=%d/timeout=%s", pages, timeout)
}

func (w WazeroExecutor) limits() (uint32, time.Duration) {
	pages, timeout := w.MemoryLimitPages, w.Timeout
	if pages == 0 {
		pages = 2048
	}
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	return pages, timeout
}

// Execute routes a computation capability to the embedded WASM runtime.
func (w WazeroExecutor) Execute(ctx context.Context, spec core.ExecutionSpec, request core.CapabilityRequest) (core.CapabilityResult, error) {
	args, err := argsToStrs(request.Args)
	if err != nil {
		return deniedResult("invalid_execution", err.Error()), nil
	}
	out, err := w.Run(ctx, spec.Entrypoint, args)
	if err != nil {
		return core.CapabilityResult{Content: err.Error(), OK: false}, err
	}
	return core.CapabilityResult{Content: out, OK: true}, nil
}

// Run executes a WASI module, passing args and capturing its stdout.
func (w WazeroExecutor) Run(ctx context.Context, modulePath string, args []string) (string, error) {
	if ctx == nil {
		return "", fmt.Errorf("wazero: context is nil")
	}
	pages, timeout := w.limits()
	if pages > 65536 || timeout < 0 {
		return "", fmt.Errorf("wazero: invalid memory limit or timeout")
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return "", fmt.Errorf("wazero: before execution: %w", err)
	}
	if err := validateLocalExecPath(modulePath); err != nil {
		return "", err
	}
	if err := validateExecArgs(args); err != nil {
		return "", err
	}
	module, err := os.Open(modulePath)
	if err != nil {
		return "", fmt.Errorf("wazero: read module: %w", err)
	}
	defer module.Close()
	info, err := module.Stat()
	if err != nil {
		return "", fmt.Errorf("wazero: stat module: %w", err)
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("wazero: module must be a regular file")
	}
	if info.Size() > maxWasmModuleBytes {
		return "", fmt.Errorf("wasm module exceeds %d bytes", maxWasmModuleBytes)
	}
	b, err := io.ReadAll(io.LimitReader(module, int64(maxWasmModuleBytes)+1))
	if err != nil {
		return "", fmt.Errorf("wazero: read module: %w", err)
	}
	if len(b) > maxWasmModuleBytes {
		return "", fmt.Errorf("wasm module exceeds %d bytes", maxWasmModuleBytes)
	}
	if err := ctx.Err(); err != nil {
		return "", fmt.Errorf("wazero: before compilation: %w", err)
	}
	runtimeConfig := wazero.NewRuntimeConfig().
		WithMemoryLimitPages(pages).
		WithCloseOnContextDone(true)
	if w.CompilationCache != nil {
		runtimeConfig = runtimeConfig.WithCompilationCache(w.CompilationCache)
	}
	r := wazero.NewRuntimeWithConfig(ctx, runtimeConfig)
	defer r.Close(context.WithoutCancel(ctx))

	if _, err := wasi_snapshot_preview1.Instantiate(ctx, r); err != nil {
		return "", fmt.Errorf("wazero: instantiate wasi: %w", err)
	}

	stdout := &limitedBuffer{limit: maxLocalExecOutputBytes}
	cfg := wazero.NewModuleConfig().
		WithStdout(stdout).
		WithArgs(append([]string{modulePath}, args...)...)

	// InstantiateWithConfig ties compiled-code cleanup to the guest instance.
	// A WASI proc_exit would then evict the code from the caller's shared cache.
	// Compile explicitly so Runtime.Close (uncached) or cache.Close (shared)
	// owns compiled-code lifetime, while every call still gets a new instance.
	compiled, err := r.CompileModule(ctx, b)
	if err != nil {
		return "", fmt.Errorf("wazero: compile module: %w", err)
	}
	if _, err := r.InstantiateModule(ctx, compiled, cfg); err != nil {
		if outErr := stdout.err(); outErr != nil {
			return "", outErr
		}
		return "", fmt.Errorf("wazero: run module: %w", err)
	}
	if err := stdout.err(); err != nil {
		return "", err
	}
	return stdout.String(), nil
}
