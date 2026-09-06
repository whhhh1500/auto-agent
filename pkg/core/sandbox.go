package core

// SandboxPolicy rides each call (never the provider), so two consumers can run
// under different policies simultaneously and an escalated retry is a new call
// with a wider policy.
type SandboxPolicy struct {
	Mode SandboxMode
}

type ConfinedArgv struct {
	Argv             []string
	Enforcement      string   // full | partial
	DenialSignatures []string // backend's own stderr-denial dialect (diagnostics)
}

// SandboxUnavailableError is the fail-closed signal: a requested mode could not
// be enforced, so the call must NOT run unconfined.
type SandboxUnavailableError struct {
	msg string
}

func (e *SandboxUnavailableError) Error() string { return e.msg }

// NewSandboxUnavailableError reports that a requested confinement policy
// cannot be enforced by the selected execution backend.
func NewSandboxUnavailableError(message string) error {
	return &SandboxUnavailableError{msg: message}
}

// SandboxProvider is the process-confinement seam. A real backend wraps with
// OS-level confinement (bwrap/Landlock/Seatbelt/Windows restricted token).
// Containers / microVMs / remote executors are NOT providers — they replace
// whole capabilities (fs/subprocess) instead.
type SandboxProvider interface {
	Confine(argv []string, policy SandboxPolicy) (ConfinedArgv, error)
}

// ProcessSandbox is the skeleton backend. It cannot yet enforce OS-level file
// effects, so it reports Enforcement = "partial" and a marker signature. Swap in
// a real backend to get "full".
type ProcessSandbox struct{}

func (ProcessSandbox) Confine(argv []string, policy SandboxPolicy) (ConfinedArgv, error) {
	if policy.Mode == SandboxDangerFullAccess {
		return ConfinedArgv{Argv: argv, Enforcement: "full"}, nil
	}
	denials := []string{}
	if policy.Mode == SandboxReadOnly {
		denials = []string{"read-only-stub"}
	}
	return ConfinedArgv{Argv: argv, Enforcement: "partial", DenialSignatures: denials}, nil
}

// DenyUnauthorized is the fail-closed gate: a write-capable call under a
// read-only policy is rejected, never silently allowed.
func (ProcessSandbox) DenyUnauthorized(policy SandboxPolicy, needsWrite bool) error {
	if needsWrite && policy.Mode == SandboxReadOnly {
		return &SandboxUnavailableError{msg: "write denied under read-only policy"}
	}
	return nil
}
