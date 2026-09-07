# Container deployment and recovery

This guide describes the production boundary for the Harness Core server. The
repository's `docker-compose.yml` is a development-only PostgreSQL fixture; it
is not a production topology.

## Production container

Deploy one immutable image reference (prefer a registry digest such as
`registry.example/harness-core@sha256:...`) for both Linux `amd64` and `arm64`.
Run the image as its built-in non-root `harness` user with a writable data
volume mounted at `/data`. The container listens on port `8080` by default.

Required production settings include:

- `HARNESS_MODE=production`;
- `HARNESS_MASTER_KEY`, supplied by a secret manager and retained across every
  restart and upgrade;
- `HARNESS_DATABASE_TYPE=postgres`; and
- `HARNESS_POSTGRES_DSN`, using TLS in production.

Never enable `HARNESS_DEV_HEADER_AUTH` in production. Do not put passwords,
master keys, DSNs, or provider keys in an image, compose file, command line, or
an issue. Use the orchestrator's secret references. The generated
administrator's one-time password is the sole deliberate process-log exception
on a database that lacked an `accounts` table. Capture that first process output
securely, assume a log collector can retain it, never copy it to a ticket, and
activate the account immediately before applying normal log-retention policy.

The Dockerfile's `GOPROXY` and `GOSUMDB` arguments affect only the build stage.
They default to the public Go proxy and checksum database. On a restricted
network, pass an approved mirror explicitly when building the development
compose service, for example:

```text
GOPROXY=https://goproxy.cn,direct GOSUMDB=sum.golang.org docker compose build harness
```

Do not bake credentials into either argument. The runtime image does not carry
these build-only settings.

## Startup, migrations, and probes

For local development, install Go 1.25.13 or newer and run `go version` before
starting. No `.env` is required: `go run ./cmd/server` selects SQLite at
`./data/core.db` and local resources at `./data/resources`. When the `accounts`
table did not exist, first boot prints one pending `admin_` account and one-time
password through process log output. Sign in at `/console`, change the password,
and sign in again to activate it; email is optional. The built-in `general`
profile is available without profile setup, but its first chat requires a saved
model connection in Console's **部署概览 → 模型连接**. Model and S3 business
settings are configured through the Console/database rather than normal
provider secrets in `.env`.

The default listener binds all interfaces on its configured port; use
`http://127.0.0.1:<port>/console` only as the local browser address and enforce
Windows firewall or reverse-proxy policy before exposing it beyond the machine.
To use PostgreSQL from Windows PowerShell, select it explicitly and do not
leave a SQLite path in the environment:

```powershell
$env:HARNESS_DATABASE_TYPE = "postgres"
$env:HARNESS_POSTGRES_DSN = "postgres://USER:PASSWORD@HOST:5432/harness?sslmode=require" # supply the real value through a secret manager
Remove-Item Env:HARNESS_SQLITE_PATH -ErrorAction SilentlyContinue
go run ./cmd/server
```

The process opens and pings PostgreSQL, runs the storage migrations, restores
durable control-plane state, and completes first-admin bootstrap before it
binds the HTTP listener. A failed migration or bootstrap exits the container;
it does not accept traffic against a partially initialized schema.

PostgreSQL startup and schema migration use session-level advisory locks. The
DSN must connect directly to PostgreSQL or through a proxy configured for
session pooling, so one backend session is retained for the lifetime of each
lock. PgBouncer transaction pooling is not supported for the control database:
it can switch backend sessions between lock and unlock and invalidate startup
serialization. If PgBouncer is required, configure this database/user in
`pool_mode=session` or bypass PgBouncer for the Harness control connection.
`HARNESS_DB_MAX_OPEN_CONNS=1` is supported for constrained deployments and
startup uses that same connection throughout, but normal runtime work is then
serialized behind one database connection and throughput will be limited.

### Native-static Phase 1

`server.NewNativeStrictServer` is a library construction mode, not the default
`cmd/server` startup path. It is a server-owned static-bootstrap and SQL-
authority foundation, not completed-result or strict recovery. A native-static
deployment may provide only static profiles, policies, capabilities, and one
fixed model adapter; dynamic bindings, Release/Canary, factories, plugins,
credential and model-settings control, and resource reconfiguration are out of
scope. Give it a dedicated control database/SQL authority that is never shared
with a generic `server.New` instance or any dynamic-control writer. Its
authorization-epoch check detects projection lag and fails closed; it is not a
transaction-level grant held to `run/start` or queue claim. Schema v43
sidecars provide only fenced SQL historical-delivery evidence; they do not
enable completed-result continuation, consume/ack state, or a recovery
coordinator. Detached control projection, current execution admission, and a
model-invocation journal are still required. See the
[runtime invariants](architecture.md#runtime-invariants) for the composition
boundary.

The bootstrap decision uses the pre-migration database state. If `accounts`
was absent, migration creates it and one pending `admin_12345`-shaped account;
the process prints its random one-time password once. If `accounts` already
existed, even empty, no account is generated. The pending token can call only
activation and logout until the password is replaced in `/console`.

The default server does not inject a cross-run rate limiter or run/capability
429 gate. The core limiter remains an opt-in composition seam. The independent
built-in login-abuse limiter still returns 429 after repeated failed
credentials. The default `general` profile
does mount guarded memory, RAG search, and approval-gated notification
capabilities; it uses deterministic rolling summaries and bounded model-context
assembly. Provider/protocol choice, notification targets, and S3/R2 object
storage are database/Console configuration. Sequential execution is the
default. The experimental Graph executor is registered for explicit profile
selection; its current server-facing adapter wraps one core `RunTurn` node, not
an arbitrary multi-node business graph. Sandbox use remains opt-in and
fail-closed (the local provider
is registered for platform-admin discovery but its Probe is unavailable on
Windows; Linux execution additionally needs `bwrap` plus `prlimit`).

`GET /healthz` is a liveness probe and reports build `version` and `commit`.
`GET /readyz` is the load-balancer/readiness gate. Route traffic only after a
successful readiness response, and remove an instance from traffic before
terminating it. A readiness failure is not a substitute for a backup or a
migration rollback plan.

## Rolling upgrades

1. Take and verify a PostgreSQL backup and preserve the exact master key.
2. Deploy the new image digest beside the old one with the same DSN and
   secrets. Wait for `/readyz` before shifting traffic.
3. Shift traffic gradually, watching application errors, database locks, and
   the health/readiness probes. Keep at least one schema-compatible old
   instance until the new image is healthy.
4. Drain and stop old instances only after the new pool is stable. Do not run
   two incompatible migration versions concurrently.
5. If application behavior must be reverted, roll back to the old image only
   after confirming its schema compatibility. Treat destructive migrations as
   forward-only and restore from backup rather than guessing a down migration.

## Backup and restore

Use the managed PostgreSQL provider's consistent backup facility, or an
operator-controlled dump, for example:

```text
pg_dump --format=custom --file=harness-YYYYMMDD.dump "$HARNESS_POSTGRES_DSN"
pg_restore --clean --if-exists --dbname="$HARNESS_POSTGRES_DSN" harness-YYYYMMDD.dump
```

Test restoration into an isolated database before relying on a backup. Restore
the same `HARNESS_MASTER_KEY` before starting the server: encrypted settings
cannot be read with a newly generated key. Keep the key backup and database
backup under separate access controls, and rotate only through a planned
re-encryption procedure. Never paste either backup into a ticket or repository.

## Configuration changes

### Advanced optional environment settings

The normal local path needs only the server port, mode, database type/DSN,
data directory, and (in production) the master key. The following settings are
optional operational controls and are intentionally omitted from the primary
`.env.example` template:

- run cancellation/recovery and worker polling/concurrency;
- private Runner token, worker pool, claim TTL and recovery intervals;
- approval TTL;
- OpenTelemetry OTLP endpoint, headers and metric interval.

These variables remain supported by `cmd/server`; configure them only when the
deployment needs the corresponding subsystem. Model provider/protocol choice,
model credentials, notification targets, and S3/R2 object storage are
database/Console configuration, not normal environment bootstrap settings. With
no persisted object-storage row, resources remain on the local data path.

The embedded Console uses only locally vendored JavaScript at runtime. The
maintenance instructions in `pkg/console/static/README.md` may download a
pinned dependency while updating the repository; that build-time download is
not a runtime CDN dependency and deployed instances need no CDN access.

Prefer immutable deployment revisions for configuration changes. Change one
secret or DSN at a time, restart through the normal readiness gate, and retain
the previous revision until the smoke and rollback checks pass. The local
compose file uses deliberately obvious development credentials and must not be
copied into production.

## Observability navigation

For queue, approval, run, and tool failure investigation, use the
[observability runbook](observability-runbook.md). The telemetry reference and
dashboard guidance are in [telemetry.md](telemetry.md); keep tenant, run, task,
request, payload, and credential values out of metric labels.

## Windows local sandbox acceptance

The Windows basic sandbox is part of the normal current-user server path. It
uses a D-backed server work root and a native restricted child process. It does
not require a dedicated account, WFP rules, administrator installation,
Secondary Logon, `sandboxctl`, .NET, PowerShell 7, or a runtime Go toolchain.
The child is a precompiled executable and the provider never falls back to
host `os/exec`.

Configure the ordinary server with its normal data directory and start it from
an unelevated Medium current-user process:

```powershell
$env:HARNESS_DATA_DIR = (Join-Path (Get-Location) 'data')
go run ./cmd/server
```

On Windows the trusted server selects `NetworkHost` with
`RequestedIsolation=false`. The provider reports `NetworkIsolation=false` and
advertises only Host. Requests that require `NetworkDisabled` or
`NetworkIsolated` fail admission with `ErrAssuranceTooWeak`; they are not
silently downgraded. Any offline environment notice is informational. Host
networking remains weakly connected, and the product does not claim WFP or
universal IPv4/IPv6 denial.

The current-user source must be ordinary Medium. An elevated source is rejected
by the provider even if a server can start. The acceptance command below is a
development verification entry point; it uses the existing test-only Medium
launcher only when the controlling terminal needs to start an already-compiled
child. It does not create a downgrade or elevation path in production:

```powershell
$env:TEMP = 'D:\cc\auto_agent\.codex-tmp'
$env:TMP = $env:TEMP
$env:GOTMPDIR = $env:TEMP
$env:GOCACHE = 'D:\cc\auto_agent\.go-cache'
$env:GOMODCACHE = 'D:\cc\auto_agent\.go-mod-cache'
go run -tags sandboxacceptance ./cmd/windows-sandbox-basic-unelevated
```

The Basic acceptance covers the restricted-token identity, workspace write,
bounded WRITE_RESTRICTED outside-file behavior, direct `cmd.exe` arguments,
output cap, timeout, cancellation and descendant cleanup, Host-network report,
controlled loopback, strict network-policy rejection, and default plus
explicit private-desktop compatibility. Its outside-file proof includes the
known Everyone and current Logon SID write exceptions. `MountsEnforced=true`
describes fixed workspace/artifact mounts; it is not universal host-path
read/write isolation.

The Windows current-user backend permits one active session per process. A
subsequent `Start` waits for the active session's `Close` under its supplied
context and is then admitted; this is a functional capacity limit, not a
concurrency or performance guarantee. Deployments that need parallel work must
use separate server processes.

The Basic acceptance status is maintained in the
[Windows Basic evidence ledger](gaps/2026-09-05-windows-sandbox-basic.md).
Compile success and focused tests do not constitute a real Medium acceptance.
Advanced account, firewall, tamper, repair, performance, and WSL matrices are
separate future work and are not required by this Basic contract.
