//go:build !linux

package execution

import core "github.com/cc-auto-agent/harness-core/pkg/core"

// confineCommand FAILS CLOSED on non-Linux: OS-level confinement needs a Linux
// host (bwrap + namespaces). Untrusted capability code should run in a
// container/microVM (E2B/AENV) rather than an unconfined subprocess here.
func confineCommand(command string, args []string, policy core.SandboxPolicy, workdir string) ([]string, error) {
	return nil, core.NewSandboxUnavailableError("OS confinement unsupported on this platform (Linux bwrap required)")
}
