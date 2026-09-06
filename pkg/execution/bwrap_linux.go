//go:build linux

package execution

import (
	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"os"
	"os/exec"
)

// confineCommand wraps a real capability subprocess under OS-level confinement
// on Linux. It exposes only standard runtime directories and an explicitly
// bound workdir; the host root, home directories, and arbitrary host paths are
// never made visible. Network policy is intentionally not claimed here: this
// adapter only supports the host-network execution contract.
//
// Resource limits are applied via a `sh -c 'ulimit ...; exec "$@"'` wrapper so
// the confined process inherits a bounded address space, CPU budget, and fd
// count. (seccomp / no_new_privs needs a C/preload helper and is the next
// hardening layer; a real deployment should also add it.)
func confineCommand(command string, args []string, policy core.SandboxPolicy, workdir string) ([]string, error) {
	if policy.Mode == core.SandboxDangerFullAccess {
		return append([]string{command}, args...), nil
	}
	if _, err := exec.LookPath("bwrap"); err != nil {
		return nil, core.NewSandboxUnavailableError("bwrap not found; OS confinement unavailable")
	}

	if policy.Mode != core.SandboxReadOnly && policy.Mode != core.SandboxWorkspaceWrite {
		return nil, core.NewSandboxUnavailableError("unsupported sandbox policy")
	}
	if workdir != "" && isBwrapRuntimePath(workdir) {
		return nil, core.NewSandboxUnavailableError("runtime directories cannot be used as a workspace")
	}

	argv := []string{"bwrap", "--unshare-pid", "--proc", "/proc", "--dev", "/dev", "--die-with-parent", "--tmpfs", "/tmp", "--clearenv", "--setenv", "PATH", "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"}
	for _, runtimePath := range []string{"/usr", "/bin", "/sbin", "/lib", "/lib64"} {
		if _, err := os.Stat(runtimePath); err == nil {
			argv = append(argv, "--ro-bind", runtimePath, runtimePath)
		}
	}
	if workdir != "" {
		mode := "--ro-bind"
		if policy.Mode == core.SandboxWorkspaceWrite {
			mode = "--bind"
		}
		argv = append(argv, mode, workdir, workdir, "--chdir", workdir)
	}
	argv = append(argv, command)
	argv = append(argv, args...)

	// Apply resource limits (inherited by the confined process) via a ulimit
	// wrapper, then exec the bwrap argv. `_` is $0, argv becomes $@.
	wrapped := []string{
		"sh", "-c",
		"ulimit -v 524288; ulimit -t 120; ulimit -n 256; exec \"$@\"",
		"_",
	}
	return append(wrapped, argv...), nil
}

func isBwrapRuntimePath(value string) bool {
	for _, runtimePath := range []string{"/usr", "/bin", "/sbin", "/lib", "/lib64"} {
		if value == runtimePath || len(value) > len(runtimePath) && value[:len(runtimePath)] == runtimePath && value[len(runtimePath)] == '/' {
			return true
		}
	}
	return false
}
