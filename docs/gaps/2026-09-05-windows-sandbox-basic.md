# Windows Sandbox Basic — Evidence Ledger

The current-user acceptance command is:

```text
go run -tags sandboxacceptance ./cmd/windows-sandbox-basic-unelevated
```

It must run in an ordinary current-user Medium context. The backend rejects an
elevated source process; a server may still start while this provider is
unavailable with a clear reason. An existing test-only Medium launcher may
start the already-compiled command from an elevated terminal. That source
process rejection is not counted as a basic capability failure.

**Status: PASSED for the current-user Basic slice.** The real ordinary Medium
run and its owned-root cleanup passed on 2026-09-06. This ledger is evidence
for the bounded Basic contract; it is not evidence for advanced account,
firewall, performance, or WSL matrices.
This ledger records only the current-user Basic contract as of 2026-09-06. It
does not promote advanced account, WFP, or helper evidence into the basic gate.

## Product boundary

- The native Windows basic path has no runtime dependency on .NET, PowerShell
  7, or the Go toolchain. Go is a build dependency; the child is a precompiled
  self-test executable.
- Basic execution uses the current user's restricted token and a server-owned
  D-backed session root. It has no account creation, WFP mutation, machine
  installation, administrator prerequisite, or runtime elevation.
- The trusted Windows server selects existing `NetworkHost` with
  `RequestedIsolation=false`. The provider reports `network_isolation=false`
  and only advertises Host. Strict `NetworkDisabled` or `NetworkIsolated`
  requests fail admission with `ErrAssuranceTooWeak`; they are not silently
  downgraded.
- An offline environment notice is informational. The acceptance may verify
  the fixed proxy/offline hints and a controlled loopback connection to show
  that Host remains weakly connected. Neither proves network denial.
- `MountsEnforced=true` describes the fixed workspace/artifacts mount contract;
  it is not universal deny-read or deny-write assurance for every host path.
  The WRITE_RESTRICTED token contract retains its documented Everyone/logon
  writable exceptions. The acceptance bounds this with three owned files: a
  current-user-only file must be denied, while Everyone-only and current
  Logon-SID-only files must be writable.
- The Windows backend allows one active session per process. A later `Start`
  waits for the active session's `Close` under its context. This is a
  functional capacity limit and does not claim concurrent performance.

## Evidence obtained

- The network policy contract tests pass: `ExecRequest.Validate` accepts the
  three existing legal policies, while trusted `sandboxexec.Config` rejects
  unknown policies and assurance mismatches and preserves the empty-value
  Disabled compatibility default.
- The server composition test passes with a Windows Host selection and an
  empty compatibility selection on other platforms. The general profile now
  describes the provider-reported network policy and weak Host semantics.
- The new acceptance command builds for `windows/amd64` with the
  `sandboxacceptance` tag using D-backed temporary, build, and module paths.
  Its cases cover the source/target token relation, workspace write, the named
  outside private-file write, direct `cmd.exe` argv execution, bounded output,
  timeout, cancellation and Job descendant cleanup, offline hints, controlled
  loopback, strict network rejection, and default/explicit private-desktop
  compatibility.

## Current blocking result

- The real ordinary Medium end-to-end bundle passed: token, workspace, command,
  output, timeout, cancellation, descendant, weak-network, and final-cleanup
  results. The evidence report records the run hash, exit status, duration, and
  D-backed cleanup result.
- A run from an elevated source must remain a clear unavailable/rejected
  result. It is not evidence against the basic implementation; the required
  positive run is the existing test-only Medium launcher followed by the
  public provider/session/exec path.

## Outside this Basic gate

- Separate advanced pressure, tamper, repair, crash, performance, and WSL
  matrices remain future work. Historical account/WFP procedures are archived
  and remain outside the Basic evidence ledger.
