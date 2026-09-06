package execution

import (
	"context"
	"fmt"
	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"io"
	"os"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

// WazeroExecutor runs a pure-computation capability as an EMBEDDED WASM module.
// No OS process, no container, no KVM: wazero instantiates the module in-process
// in microseconds and the footprint is tiny relative to a VM. Ideal for the
// `computation` kind (评估算法 / 打分 / 换算).
//
// Note: wazero is capability-secure by default (the module has no permissions
// unless you grant WASI). It cannot run arbitrary Python/native binaries, and it
// has no network unless you expose host functions — so it is for compute, not
// for a `connector` that needs to call an external API.
type WazeroExecutor struct{}

func (WazeroExecutor) Runtime() string          { return "wazero" }
func (WazeroExecutor) ArtifactRevision() string { return "wazero-executor/v2-bounded-io" }

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
func (WazeroExecutor) Run(ctx context.Context, modulePath string, args []string) (string, error) {
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
	b, err := io.ReadAll(io.LimitReader(module, int64(maxWasmModuleBytes)+1))
	if err != nil {
		return "", fmt.Errorf("wazero: read module: %w", err)
	}
	if len(b) > maxWasmModuleBytes {
		return "", fmt.Errorf("wasm module exceeds %d bytes", maxWasmModuleBytes)
	}
	r := wazero.NewRuntime(ctx)
	defer r.Close(ctx)

	if _, err := wasi_snapshot_preview1.Instantiate(ctx, r); err != nil {
		return "", fmt.Errorf("wazero: instantiate wasi: %w", err)
	}

	stdout := &limitedBuffer{limit: maxLocalExecOutputBytes}
	cfg := wazero.NewModuleConfig().
		WithStdout(stdout).
		WithArgs(append([]string{modulePath}, args...)...)

	if _, err := r.InstantiateWithConfig(ctx, b, cfg); err != nil {
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
