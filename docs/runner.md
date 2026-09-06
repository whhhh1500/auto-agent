# Private Runner protocol

The public service exposes the private worker contract at
`POST /v1/runners/claim`, `POST /v1/runners/tasks/{id}/renew`, and
`POST /v1/runners/tasks/{id}/complete`. Workers authenticate with a dedicated
Bearer token and an explicit capability grant. Claims are fenced by worker ID
and generation; a lost or expired claim is a `409` and must not repeat an
unknown side effect.

A retry is a separate operation: only a tenant-visible `failed` task marked
idempotent may be retried, and the admin request must contain literal
`confirm: true` plus a non-empty audit reason. The server creates at most one
derived retry for the failed task and never returns task arguments, results,
idempotency keys, or trace carriers from the metadata-only admin catalog.

See [examples/runner-worker/PROTOCOL.md](../examples/runner-worker/PROTOCOL.md)
for Go/Python worker framing and [deployment.md](deployment.md) for production
secret handling.

## Windows local sandbox acceptance runner

The Windows Basic runner is an explicit local-machine acceptance command. It is
not part of `go test ./...`, and it does not represent the former elevated
account/WFP runner. The current path runs from an ordinary current-user Medium
server and starts a native restricted child. It creates only an owned random
D-backed test root. The child is precompiled and has no runtime dependency on
Go, PowerShell 7, .NET, or C#.

Run it from the repository root with all Go temporary and cache locations on
D:

```powershell
$env:TEMP = 'D:\cc\auto_agent\.codex-tmp'
$env:TMP = $env:TEMP
$env:GOTMPDIR = $env:TEMP
$env:GOCACHE = 'D:\cc\auto_agent\.go-cache'
$env:GOMODCACHE = 'D:\cc\auto_agent\.go-mod-cache'
go run -tags sandboxacceptance ./cmd/windows-sandbox-basic-unelevated
```

The source must be an ordinary Medium current-user process. The backend
rejects an elevated source with an explicit unavailable result; that rejection
is not a Basic capability failure. An existing test-only Medium launcher may
start the already-compiled acceptance executable when the controlling terminal
is elevated, but production does not add an automatic downgrade path.

The acceptance checks restricted-token identity, workspace writes, the bounded
WRITE_RESTRICTED outside-file contract, direct `cmd.exe` argv execution, output
limits, timeout, cancellation, Job descendant cleanup, the Host network report,
a controlled loopback connection, strict network-policy admission rejection,
and default/explicit private-desktop compatibility. The network result is
intentionally weak: an offline environment hint describes test setup only, and
loopback demonstrates that Host remains connected. `NetworkIsolation=false`
and the single advertised Host mode must be reported. `MountsEnforced=true`
means fixed workspace/artifact mounts; it does not guarantee universal host
read or write denial. Everyone and current Logon SID writable exceptions remain
part of the documented outside-file contract.

The Windows backend allows one active session per server process. A later
`Start` waits for the active session to close under its context before it can
proceed. This is a functional capacity limit; it is not a claim of concurrent
execution or performance scalability. Use separate server processes when
parallel work is required.

The real ordinary Medium Basic run has passed every case and exactly cleaned
its owned root. The current evidence report is
[docs/verification/2026-09-06-windows-basic-acceptance.md](verification/2026-09-06-windows-basic-acceptance.md);
the current evidence ledger is
[gaps/2026-09-05-windows-sandbox-basic.md](gaps/2026-09-05-windows-sandbox-basic.md).

The former elevated account, Secondary Logon, helper, WFP, repair, tamper,
pressure, and performance procedures are archived historical work. They are
not operational setup instructions, are not a Basic prerequisite, and are not
reported as current acceptance evidence. The Linux/WSL gate is a separate
future validation and does not change this Windows Basic boundary.
