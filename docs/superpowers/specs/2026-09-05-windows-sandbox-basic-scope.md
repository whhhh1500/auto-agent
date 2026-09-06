# Historical record — superseded

This record is retained for traceability only. It is not the current Windows
runtime contract and contains no operational procedure. The current boundary
is documented in [`docs/runner.md`](../../runner.md),
[`docs/deployment.md`](../../deployment.md), and the
[current-user network ADR](../../decisions/2026-09-05-windows-current-user-basic-network.md).

The historical scope explored a native Windows process sandbox and recorded
limits around a shared Windows kernel, restricted child processes, workspace
roots, desktop selection, and bounded execution. Its references to broker
handshakes, helper binaries, dedicated accounts, HMAC, LSA, WFP, repair records,
or strong filesystem/network guarantees are superseded design discussion. They
must not be read as claims about the current implementation or as acceptance
evidence.

The current Basic slice is an ordinary current-user Medium path. It uses a
server-owned D-backed session root, a restricted child, the existing public
provider/session/exec contracts, and Host networking with
`NetworkIsolation=false`. It has no account creation, WFP mutation, machine
installation, runtime PowerShell/.NET/Go requirement, or host-execution
fallback. Its acceptance status is tracked only in the current evidence
ledger.