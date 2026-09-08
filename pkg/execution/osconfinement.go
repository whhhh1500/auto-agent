package execution

import (
	"context"
	"fmt"
	core "github.com/whhhh1500/auto-agent/pkg/core"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

// OSExecSpec describes a REAL capability subprocess (node/python/go/binary) to
// run under OS-level confinement.
type OSExecSpec struct {
	Command string
	Args    []string
	Policy  core.SandboxPolicy
	Workdir string
	// Writes flags that the command may write to disk (used for fail-closed).
	Writes bool
}

// OSConfinementExecutor runs a real capability subprocess under OS-level
// confinement. On Linux it wraps the command via bubblewrap (read-only /
// workspace-write); elsewhere it FAILS CLOSED. This is "confinement, not
// isolation" — it shares the host kernel and filesystem, so it suits
// low-trust code; untrusted code should go to a container/microVM (E2B/AENV).
type OSConfinementExecutor struct{}

func (OSConfinementExecutor) Runtime() string          { return "subprocess" }
func (OSConfinementExecutor) ArtifactRevision() string { return "os-confinement/v3-minimal-root" }

// Execute routes a connector or worker capability to a confined subprocess.
func (o OSConfinementExecutor) Execute(ctx context.Context, execution core.ExecutionSpec, request core.CapabilityRequest) (core.CapabilityResult, error) {
	args, err := argsToStrs(request.Args)
	if err != nil {
		return deniedResult("invalid_execution", err.Error()), nil
	}
	out, err := o.Run(ctx, OSExecSpec{
		Command: execution.Entrypoint,
		Args:    args,
		Policy:  execution.Sandbox,
		Workdir: execution.Workdir,
		Writes:  execution.Writes,
	})
	if err != nil {
		return core.CapabilityResult{Content: err.Error(), OK: false}, err
	}
	return core.CapabilityResult{Content: out, OK: true}, nil
}

func (OSConfinementExecutor) Run(ctx context.Context, spec OSExecSpec) (string, error) {
	if ctx == nil {
		return "", context.Canceled
	}
	// Fail-closed gate before anything runs.
	if spec.Writes && spec.Policy.Mode == core.SandboxReadOnly {
		return "", core.NewSandboxUnavailableError("write denied under read-only policy")
	}
	command, err := validateOSCommandPath(spec.Command)
	if err != nil {
		return "", fmt.Errorf("execution command: %w", err)
	}
	spec.Command = command
	if spec.Workdir != "" {
		workdir, err := validateOSWorkdir(spec.Workdir)
		if err != nil {
			return "", fmt.Errorf("execution workdir: %w", err)
		}
		spec.Workdir = workdir
	}
	if err := validateExecArgs(spec.Args); err != nil {
		return "", err
	}

	argv, err := confineCommand(spec.Command, spec.Args, spec.Policy, spec.Workdir)
	if err != nil {
		return "", err
	}

	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Dir = spec.Workdir
	stdout := &limitedBuffer{limit: maxLocalExecOutputBytes}
	stderr := &limitedBuffer{limit: maxLocalExecOutputBytes}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		if outErr := stdout.err(); outErr != nil {
			return "", outErr
		}
		if err := stderr.err(); err != nil {
			return "", err
		}
		return stdout.String(), fmt.Errorf("confinement: %w (stderr=%s)", err, truncate(stderr.String(), 500))
	}
	if err := stdout.err(); err != nil {
		return "", err
	}
	return stdout.String(), nil
}

func validateOSCommandPath(value string) (string, error) {
	if err := validateLocalExecPath(value); err != nil {
		return "", err
	}
	if !filepath.IsAbs(value) || isHostRoot(value) || isUNCPath(value) {
		return "", fmt.Errorf("command must be a non-root absolute local path")
	}
	clean := filepath.Clean(value)
	resolved, err := filepath.EvalSymlinks(clean)
	if err != nil {
		return "", fmt.Errorf("command path cannot be resolved: %w", err)
	}
	resolved, err = filepath.Abs(resolved)
	if err != nil || isHostRoot(resolved) || isUNCPath(resolved) {
		return "", fmt.Errorf("command resolved outside a valid local path")
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return "", fmt.Errorf("command path cannot be inspected: %w", err)
	}
	if info.IsDir() {
		return "", fmt.Errorf("command path is a directory")
	}
	return filepath.Clean(resolved), nil
}

func validateOSWorkdir(value string) (string, error) {
	if err := validateLocalExecPath(value); err != nil {
		return "", err
	}
	if !filepath.IsAbs(value) || isHostRoot(value) || isUNCPath(value) {
		return "", fmt.Errorf("workdir must be a non-root absolute local path")
	}
	clean := filepath.Clean(value)
	resolved, err := filepath.EvalSymlinks(clean)
	if err != nil {
		return "", fmt.Errorf("workdir path cannot be resolved: %w", err)
	}
	resolved, err = filepath.Abs(resolved)
	if err != nil || isHostRoot(resolved) || isUNCPath(resolved) {
		return "", fmt.Errorf("workdir resolved outside a valid local path")
	}
	if !sameHostPath(clean, resolved) {
		return "", fmt.Errorf("workdir symlinks are forbidden")
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return "", fmt.Errorf("workdir cannot be inspected: %w", err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("workdir is not a directory")
	}
	return filepath.Clean(resolved), nil
}

func isHostRoot(value string) bool {
	clean := filepath.Clean(value)
	if clean == string(filepath.Separator) {
		return true
	}
	volume := filepath.VolumeName(clean)
	return volume != "" && clean == volume+string(filepath.Separator)
}

func isUNCPath(value string) bool {
	return strings.HasPrefix(value, `\\`) || strings.HasPrefix(value, "//")
}

func sameHostPath(left, right string) bool {
	if runtime.GOOS == "windows" {
		return strings.EqualFold(filepath.Clean(left), filepath.Clean(right))
	}
	return filepath.Clean(left) == filepath.Clean(right)
}
