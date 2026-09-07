# Harness Core

Harness Core is an open-source Go foundation for building composable Agents and digital humans. It keeps the runtime small and deterministic while allowing deployers, products, tenants, users, sessions, and runs to contribute their own capabilities and Agent configuration.

The project is a runtime foundation with one built-in, provider-neutral `general`
profile, so a fresh server and Console need no profile setup. A conversation still
needs an explicitly configured model connection. Product-specific behavior lives
in plugins, providers, profiles, and examples.

> API status: pre-GA. Package paths and public APIs may change without a
> compatibility facade until the first stable release.

详细技术评估：[Agent 模块实现、性能与扩展点](docs/agent-module-assessment.md)
（44 个逻辑模块、74 个 Go 包）、[与主流框架的差异及选型](docs/agent-framework-comparison.md)、
[本地性能实测与证据边界](docs/performance/2026-09-06-module-benchmarks.md)。

## Zero-configuration local start

在仓库根目录打开 PowerShell，并确认已安装 Go 1.25.13 或更高版本：

```powershell
go version # 应显示 go1.25.13 或更高版本
```

无需创建 `.env` 即可启动开发实例；默认使用 SQLite 和 `./data`：

1. 运行 `go run ./cmd/server`。
2. 若终端打印首次管理员 account ID 和一次性密码，立即安全保存。它只会输出一次，但进程日志输出可能被日志采集；不要把凭据复制到工单、聊天记录或仓库。
3. 打开 `http://127.0.0.1:8080/console`，使用该 account ID 登录；email 是可选属性，不是必填登录格式。
4. 首次进入控制台立即修改密码；修改后使用新密码重新登录，账号才算激活。
5. 在控制台“部署概览 → 模型连接”填写 Provider、Protocol、模型名和 API Key 并保存；`general` profile 已存在，但没有模型配置时聊天会明确拒绝，而不会猜测或回退到某个厂商。
6. 默认数据库是 `./data/core.db`，资源目录是 `./data/resources`；模型、S3 和其他业务配置从控制台写入数据库。

PowerShell：

```powershell
go run ./cmd/server
# 然后打开 http://127.0.0.1:8080/console
```

如需改用空闲端口：

```powershell
$env:HARNESS_SERVER_PORT = "18081"
go run ./cmd/server
# 然后打开 http://127.0.0.1:18081/console
```

切换到 PostgreSQL 时必须显式选择类型；不要同时设置 SQLite 路径：

```powershell
$env:HARNESS_DATABASE_TYPE = "postgres"
$env:HARNESS_POSTGRES_DSN = "postgres://USER:PASSWORD@HOST:5432/harness?sslmode=require" # 替换为秘密管理器提供的 DSN
Remove-Item Env:HARNESS_SQLITE_PATH -ErrorAction SilentlyContinue
go run ./cmd/server
```

`.env` 是可选的。需要模板时复制 `.env.example` 为 `.env`；环境变量只保留启动基础设施配置，模型和 S3 不需要写入 `.env`。服务器监听所有网络接口；上面的 `127.0.0.1` 只是本机访问地址。Windows 防火墙、反向代理和生产身份认证由部署者配置，未计划对外开放时不要暴露该端口。

接下来可查看：[配置模型连接](#model-providers)、[配置本地或 S3/R2 资源存储](#storage-configuration-console)、以及[生产 PostgreSQL 部署](docs/deployment.md#production-container)。

## Design principles

- **Stable kernel, evolving capabilities.** The kernel owns run lifecycle, events, scope resolution, policy enforcement, snapshots, persistence interfaces, and execution interfaces.
- **Capability is an open umbrella.** A capability may be a tool, Skill, Prompt provider, RAG provider, memory, workflow, Agent, router, policy, model, sandbox, evaluator, presentation, or a third-party contract.
- **`kind` is descriptive; `contract` is behavioral.** The runtime does not switch on a closed list of kinds. Consumers select providers by a versioned contract such as `harness.tool/v1` or `harness.rag.search/v1`.
- **One hierarchy, one API.** Platform, tenant, user, and run capabilities use the same binding API. Scope and policy determine ownership and visibility.
- **Explicit overrides.** A lower scope must use `replace` or `disable`; duplicate implicit registration is rejected. Protected capabilities cannot be replaced by lower scopes.
- **Immutable runs.** Every run fixes a capability snapshot and Agent profile snapshot for replay, audit, canary rollout, and rollback. Run Composition also carries a bounded, string-only adapter metadata envelope; approval resumes append `run/resume` with the freshly resolved recipe, so a rollout assignment can change safely while a Run is suspended.
- **Product-neutral core.** Authentication, storage, model routing, business capabilities, and deployment infrastructure are injected interfaces.

## Package architecture

```text
pkg/core                 deterministic kernel and public contracts
pkg/app                  application seams: identity, model control, storage and context assembly
pkg/app/contextassembly  bounded context assembly plus deterministic/optional LLM summarizers
pkg/app/modelcontrol     model/provider/protocol control-plane contracts
pkg/adapter              transport, SQL, storage and in-memory bridges
pkg/control              control-plane orchestration (profile releases)
pkg/evaluation           isolated datasets, assertions and regression gates
pkg/execution            HTTP, MCP, WASM and OS-confinement executors
pkg/execution/graph      experimental Graph executor, selected explicitly per profile
pkg/execution/sandbox    fail-closed sandbox contracts; Linux uses bwrap + prlimit, Windows uses the current-user Medium restricted-child path
pkg/extensions/memory    scoped user-memory capabilities
pkg/extensions/rag       scoped retrieval capability
pkg/extensions/workflow  optional deterministic workflows
pkg/extensions/subagent  optional agent-as-capability composition
pkg/extensions/runner    optional private-runner queue
pkg/adapter/modelruntime database-backed OpenAI/Anthropic provider/protocol composition
pkg/storage              file, SQL, S3 and object-store implementations
pkg/server               HTTP/SSE transport and administrative routes
pkg/telemetry/otel       optional OpenTelemetry OTLP/HTTP adapter
pkg/app/notification     provider-neutral notification target and channel contracts
```

Dependency direction is inward: `core` imports none of the outer packages;
providers, extensions, storage and transports depend on `core`. Cross-package
behavior tests live in `pkg/integration`. The generic `cmd/server` mounts the
built-in `general` profile with guarded scoped memory, RAG search, and approved
notification capabilities. It also enables deterministic rolling summaries and
bounded model-context assembly. Provider/protocol choice, notification targets,
and object storage are database/Console configuration. Sequential execution is
the default; the experimental Graph executor is registered but used only when a
profile explicitly selects it. Its current server-facing adapter wraps one core
`RunTurn` node, not an arbitrary multi-node business graph. Sandbox use remains
explicit and fails closed. On Windows the local provider is available only from
an ordinary current-user Medium source and uses a restricted child with a
server-owned D-backed session root; it reports Host networking without network
isolation. Elevated callers are rejected, and the provider never falls back to
host execution. Linux retains its existing bwrap/prlimit behavior.
`cmd/demo`, `examples/crypto`, and
`cmd/starter-server` are sample assemblies.

For a SQL-backed multi-node workflow, see the separate
[draft/review/finalize example](examples/graph-review/README.md). Current public
and experimental integration boundaries are listed in the
[support matrix](docs/architecture.md#support-matrix). E2B is not bundled;
remote sandbox integrations must implement and register a sandbox provider.

The [2026-09-06 integration acceptance](docs/verification/2026-09-06-assessment-closure.md)
records the PostgreSQL gate, replacement-instance approval recovery, a real
`gemini-3.8-flash` conversation, and a bounded local concurrency measurement.

## Scope hierarchy

```text
global
└── deployment
    └── product
        └── tenant
            └── workspace
                └── user
                    └── session
                        └── run
```

Levels may be skipped, but their order cannot be reversed. A child inherits ancestor contributions.

Typical composition:

```text
product: crypto market data + token analysis + default persona
tenant:  private market-data replacement + brand policy
user:    personal watchlist + digital-human voice
session: temporary task context
run:     temporary verifier or one-shot capability
```

## Core concepts

### Capabilities

`CapabilityManifest` declares identity, version, contract, permissions, schemas, budgets, timeout, tool exposure, and optional execution metadata. `CapabilityBinding` attaches a provider to a `ScopePath`.

When `OutputSchema` is set, a successful capability result must contain valid
JSON matching that schema. `MaxOutputBytes` can narrow the default 1 MiB result
limit, up to the 16 MiB hard maximum. Provider panics, non-JSON arguments or
metadata, invalid output and oversized results become stable denied results.

`Idempotent` is an execution guarantee, not descriptive metadata. When a
`ToolInvocationJournal` is configured, every guarded provider call first
durably records its run/call identity and SHA-256 argument digest. Completed
results replay without invoking the provider. A non-idempotent call left in
`started` or `uncertain` fails closed as `tool_outcome_unknown`; an idempotent
call may retry with the same stable call ID. HTTP executors send that identity
as `Idempotency-Key`, and private-runner tasks carry it explicitly.

```go
registry.Bind(core.CapabilityBinding{
    Scope:    tenantScope,
    Mode:     core.BindingReplace,
    Manifest: tenantRAG.Manifest(),
    Provider: tenantRAG,
})
```

`CapabilityResolver.Resolve` walks the scope hierarchy and creates an immutable `CapabilitySnapshot`. The snapshot is the only capability set used by a run.

Providers and model adapters may implement `core.ArtifactRevisioner`. The run
composition records a SHA-256 fingerprint of the returned revision, never the
raw label, so deployments can correlate runs with implementation builds without
putting revision text or secrets into the event log.

### Agent profiles and soul/persona

`AgentProfileLayer` composes an Agent from existing capabilities and versioned prompt fragments. A layer can inherit another profile, add/remove capabilities, select a model, and put/remove persona fragments.

Persona fragments have stable IDs and sections:

- identity
- traits
- values
- goals
- style
- boundaries
- instructions

This allows a product to define the base identity, a tenant to add brand rules, and a user to customize a digital human without rewriting one opaque system prompt.

```go
profiles.Bind(core.AgentProfileLayer{
    Scope:     userScope,
    ProfileID: "user.crypto-avatar",
    Extends:   "crypto.agent.analyst",
    PutFragments: []core.PromptFragment{
        {
            ID:      "user.voice",
            Section: core.PromptStyle,
            Content: "Use concise Chinese with a calm, evidence-first tone.",
        },
    },
})
```

### Plugins

A plugin is a reversible installation unit that can contribute multiple capabilities and Agent profile layers at a deployer-selected scope.

```go
mounted, err := host.MountPlugin(ctx, productScope, plugin)
// Later: remove registrations and release resources.
err = mounted.Close(ctx)
```

Plugins do not receive permission to mutate the runtime kernel. Untrusted tenant or user code should be implemented through an out-of-process provider, WASM, container, microVM, MCP server, HTTP service, or private runner.

These execution options have different assurance and resource boundaries. The
current WASM adapter still needs explicit runtime memory and execution-cancellation
configuration before it can promise those limits; see the
[WASM assessment](docs/agent-module-assessment.md#m27--wasm-执行).

### Sessions and runs

Sessions contain immutable ownership metadata and an append-only JSON event log. Runs append to an existing session instead of replacing it.

Run and session IDs use a bounded safe-character vocabulary, and one Run ID may
start only once per Session. Restored logs reject unknown event types, malformed
payloads, invalid terminal status, negative usage and inconsistent tool-call
fields. File, SQL and S3 stores persist the same validated event vocabulary.

### Model stream contract

Every `core.LlmAdapter` must emit zero or more `assistant` chunks followed by
exactly one `finish` chunk (`stop` or `tool-calls`). Usage is reported at most
once. The kernel rejects missing finish frames, output after finish, duplicate
tool-call IDs, invalid tool names, contradictory finish reasons, negative or
excessive usage, oversized text and adapter panics. The Agent and LLM summarizer
share this same stream consumer.

Important events include:

```text
run/start
user/message
assistant/chunk
assistant/message
tool/call
tool/result
step/start
step/end
step/error
run/usage
run/error
run/end
```

`assistant/chunk` events are opt-in (`Runtime.StreamChunks`) and carry streamed text deltas for token-level transport; model history projection uses only complete messages. `run/usage` is a durable per-invocation ledger: legacy events with no `invocation_id` remain additive, while new `model:<step-start-seq>` and `summary:<range-start>:<range-end>:<pre-call-session-version>` IDs are canonical, bounded and unique within one Run. A completed model stream that reports no usage records `0/0`, meaning the outcome was observed but the upstream provider did not report metering; a failed request with no report does not invent a zero charge. Usage events are excluded from model-history projection. Both model and deterministic fast-path capability calls produce tool audit events. Every non-suspended run reaches `run/end`; a durable approval suspension instead ends its current worker lifetime at `approval/requested` and later resumes the same Run ID.

For a normal model response, the Agent commits `assistant/message` and its one `run/usage` ledger event as one in-memory Session batch, then appends `tool/call`. It notifies the run consumer only after the complete batch is present, so a pre-tool checkpoint sees `assistant/message → run/usage → tool/call` before ToolJournal `Begin` or a provider side effect. Consumers must therefore persist the complete Session suffix from their own saved version, rather than treating each callback as proof of exactly one new event; durable subagent delegation uses that saved-version rule and accepts a commit-response-lost retry only after an exact durable reload. This adds one small event per completed model or metered summary invocation, not one event per stream chunk, and no prompt tokens.

This Session ledger proves observed usage once it has reached durable history; it does not prove whether a model request billed or completed when a process dies before its assistant/usage outcome is durable. That window needs a future model-invocation journal. Completed ToolJournal results also continue to recover fail-closed as `run_interrupted`; this release does not implement completed-result automatic continuation.

`SessionStore` uses optimistic versions so storage adapters can reject lost updates. The repository includes a concurrency-safe in-memory reference implementation and a durable `FileSessionStore` (one JSONL file per session, append-only delta saves). `MemorySessionStore` keeps at most `core.MaxMemorySessions` (4096) sessions; creating an existing ID still conflicts, and Save of an existing session is not blocked by the cap. One session log keeps at most `core.MaxSessionEvents` (16384) events; Append and Restore fail closed at the cap. Production deployments should provide a database-backed store and a distributed run lease.

### Guardrails: hooks, approval, and budgets

Every tool call in a run — from the model loop and from deterministic fast routes alike — passes through one guarded funnel before a provider executes it:

```text
JSON Schema validation → RunHooks.OnBeforeTool → per-capability budget → approval → provider
```

- **Validation.** Arguments are checked against the manifest's tool parameters (a pragmatic JSON Schema subset). Refusals return a stable `invalid_tool_args` result to the model; the provider never sees malformed input.
- **Hooks.** `RunHooks` are automatic guardrail checks: `OnRunStart` may reject input, `OnBeforeStep` may stop a step, `OnBeforeTool` may deny one call (the denial becomes the tool result, so the loop recovers), and `OnAfterTool`/`OnRunEnd` observe outcomes.
- **Approval.** A capability whose manifest declares `RequiresApproval` must pass an `Approver`. Missing approver, errors, or out-of-vocabulary decisions all deny the call. `DurableApprover` returns a stable pending request: the Agent appends `approval/requested`, checkpoints usage, and returns `waiting_approval` without `run/end`. The server persists that prefix, releases the queue claim, worker slot, and Session lease, then atomically requeues the same Run ID when an administrator approves, denies, or the request expires. Resume re-resolves current identity, policy, capability and model state; the approval is bound to owner, run/call, arguments and the full public Capability Manifest digest. Pending durable requests are capped at `storage.MaxPendingApprovals` (1024); replaying an existing run/call still succeeds at the cap.
- **Budgets.** `PerTurnBudget` caps calls per capability per run; the profile's `MaxToolCalls` caps the run total. Exhaustion returns a denial result to the model, or ends the run as `limited`. The optional in-process `WindowRateLimiter` can additionally bound cross-run calls per tenant/capability; it is not injected by the default server and no default run/capability 429 gate is enabled. The independent built-in login-abuse limiter still returns 429 after repeated failed credentials.

Refusals carry stable metadata codes: `invalid_tool_args`, `hook_denied`, `capability_budget_exceeded`, `approval_denied`, `approval_unavailable`, `approval_failed`.

Durable approval events are `approval/requested` and `approval/resolved`. An unresolved request is a valid suspended log tail, so crash repair never closes it as `run_interrupted`. Deterministic workflows are resumable: completed child calls replay from the Session/Tool Journal and only the pending child continues.

### Layered policy intersection

`PolicyRegistry` stores one `PolicyLayer` per scope, mounted with the same reversible binding model as capabilities. At run composition the layers intersect root-to-leaf:

- `AllowPermissions` intersects the principal's grants; `DenyPermissions` removes grants. Layers cannot widen anything — narrowing is structural.
- `MaxSteps` and `MaxToolCalls` cap the run at the strictest value seen.
- Capabilities whose required permissions no longer pass are dropped from the snapshot silently; composition errors stay reserved for profile mistakes.

### Context archiving and compaction

Long sessions are archived in two layers, both anchored to the immutable log:

- **Durable rolling archive.** The default `RollingSummarizer` starts at 120 projected messages and preserves the latest 60 verbatim. Its default deterministic extractive summarizer is bounded to 12 KiB and does not call a model: it retains prior summaries, current user goals/constraints, tool-call/result evidence, and explicit omissions. `LlmSummarizer` remains an opt-in bounded replacement policy.
- **Ephemeral model assembly.** After archival and the mechanical `ContextCompactor`, `ModelContextAssembler` applies the selected model's context/output limits exactly once. It keeps system/current-turn and tool-call/result groups atomic, prefers a durable summary over older exact turns, turns oversized old tool results into bounded digest markers, and emits only bounded count/cost evidence. It fails closed if required system/current context cannot fit.

### Crash repair

A run killed mid-flight leaves an unbalanced log. `SessionStore.Load` is a pure read for Memory, File, SQL, and S3 stores; history and inspection requests therefore cannot terminate an active checkpointed run. After acquiring execution ownership, an executor may call `storage.RepairInterruptedSession`, which uses the store version as an optimistic CAS and appends the deterministic `core.RepairInterrupted` suffix — `tool_not_started` / `tool_outcome_unknown` error results, a `step/end`, then `run/error` (`run_interrupted`) plus `run/end`. An unresolved durable approval remains open. A repeated explicit repair is a no-op, and a conflicting still-open log is returned as `core.ErrSessionConflict` instead of being retried blindly.

Migration note (2026-09-06): embedders that relied on `Load` to repair crash tails must now call `storage.RepairInterruptedSession` only after obtaining their session execution lock or lease. The HTTP server does this before a synchronous new run and, for queued work, after the queue claim and session lease are both held. For event-log compatibility, the existing synthetic `run/error` message still contains the legacy phrase “appended on load”; consumers should use its stable `run_interrupted` code rather than parse that text.

For synchronous runs, the server's per-session mutex is process-local. A single server instance may omit `Config.Leaser`; deployments with multiple server instances sharing a SessionStore must configure a cross-instance `SessionLeaser`, otherwise no server-side contract prevents one instance from repairing a Run that is still active on another instance.

Queued execution has a stricter startup contract than synchronous execution. `server.Config.RunQueue` requires a `SessionLeaser`, a Session store that implements `storage.FencedSessionAppender`, and the sealed storage SQL fence-domain capability on the Session store, queue, and leaser. `RunControl` must be omitted (so the queue supplies it) or be that exact same queue object; a split control plane is rejected. Native `SQLSessionStore` and `SQLRunControlStore` (including transparent embedding wrappers) retain the fence capability only when they share one `*sql.DB` handle and dialect; independently implemented adapters fail closed. This is a conservative same-pool check, not a claim that arbitrary adapter code has been inspected. The core SQL stores use unqualified table names, so a PostgreSQL deployment must keep one stable shared schema/search path for that pool; changing `search_path` per connection or multiplexing schemas through a single pool is unsupported. The worker binds every acquisition to a unique holder containing the instance ID, a compact Run ID digest, queue generation, and a fresh nonce. It passes that holder and claim identity to fenced repair and to one `NewFencedWriteBehind`; every non-empty repair, model, tool, checkpoint, background, and terminal Session append verifies the live queue worker/generation/lease, cancellation state, Session lease holder/expiry, owner identity, and Session version in the same SQL transaction. A lost fence cancels and aborts the stale worker without appending a terminal Session event or finishing/retrying/pausing its old claim.

This is intentionally SQL-only. Memory, File, and S3 Session stores — and a queue or leaser backed by a different SQL handle — are rejected for `Config.RunQueue`; they must not be joined by a best-effort cross-store check. A custom integration can decorate and transparently embed the native SQL components, but a separately implemented queue or leaser cannot opt in merely by returning a token. Synchronous HTTP runs still use the ordinary writer, but each distributed session-lease acquisition has its own nonce-bearing holder so delayed renew/release calls cannot be confused with a later acquisition of the same instance and Run.

### Write-behind persistence

During a run the HTTP adapter persists a stable ordered prefix at most one batching window (200ms default, `Config.MaxWriteDelay`) behind the producer, then flushes synchronously before the response completes. A crash loses at most one window instead of the whole run; the hot path never blocks on storage.

### Tool side-effect journal

`SQLToolInvocationJournal` closes the gap between an external side effect and
the later Session flush. The final guard boundary writes `started` before the
provider runs and `completed` plus the canonical result afterwards. Reusing a
run/call identity with a different capability, arguments, owner or idempotency
contract is an explicit conflict. Journal failures are fail-closed: the
provider is not called when `Begin` is unavailable, and a result that cannot be
committed becomes `tool_outcome_unknown`. Retention deletes only old completed
rows; uncertain records remain as permanent duplicate-execution fences. The journal keeps at most `storage.MaxToolInvocations` (8192) rows; replaying an existing run/call still succeeds at the cap, and a new identity is refused until completed rows are pruned.

### Durable session stores

`SessionStore` implementations with optimistic concurrency:

- `MemorySessionStore` — in-process reference implementation. It keeps at most `core.MaxMemorySessions` (4096) sessions.
- `FileSessionStore` — one JSONL file per session, append-only delta saves, plus a temp-file-replaced `<session>.evidence.json` Run/Backtest Composition sidecar that can be rebuilt from the canonical JSONL file. The directory keeps at most `storage.MaxStoredSessions` (4096) `*.jsonl` session files. One JSONL file may not exceed `storage.MaxFileSessionBytes` (64 MiB), and a single append batch may not exceed 16 MiB.
- `SQLSessionStore` — generic `database/sql` store (at most `storage.MaxStoredSessions` (4096) catalog rows) for SQLite (demo/self-host) and PostgreSQL (production). `Save` is a single transaction (read version → compare → append chunk → advance tip), so persistence is atomic and orphan chunks cannot exist. The `sessions` table promotes tenant/user/profile/event-count/updated-at into columns, which makes it the session-catalog sidecar: `SessionLister.ListSessions` serves studio and tenant queries directly. Schema version is recorded in `store_meta` and future versions are refused fail-closed. The kernel imports no SQL driver — deployers register `modernc.org/sqlite` (pure Go) or `pgx/stdlib`; only the driver import and DSN change between demo and production. Each session keeps at most `storage.MaxEventChunksPerSession` (4096) event chunks, and one chunk payload may not exceed 16 MiB. Indexed `run_evidence` rows are capped at `storage.MaxEvidenceRecords` (8192); updating an existing session/run/segment still succeeds at the cap.
- `S3SessionStore` — any `ObjectStore` backend, with an S3-compatible layout: `sessions/{id}/meta.json` (version + header, the CAS commit point), `sessions/{id}/events/{seq:012d}.jsonl` (one immutable chunk per save batch), and `sessions/{id}/evidence.json` (committed-version Run/Backtest sidecar). Loads trust only the chunk/evidence prefix `meta.version` covers; stale or missing sidecars are rebuilt from canonical chunks. The in-process `MemoryObjectStore` keeps at most `storage.MaxMemoryObjects` (4096) keys; replacing an existing key does not consume another slot. `FileObjectStore` uses the same key cap. Object payloads are capped at `storage.MaxObjectBytes` (16 MiB). Create refuses a new `meta.json` once `storage.MaxStoredSessions` (4096) sessions already exist; an existing ID still conflicts.

The current SQL schema is v42. Earlier memory/RAG tag and token projections are rebuildable derived indexes over canonical entries/documents. Later additions include artifact-migration state and journal records (v36), experimental Graph checkpoint/transition facts (v37), encrypted tenant-scoped notification targets (v38), generation-fenced Graph segment leases (v39), approval/stale-run lookup indexes (v40), immutable Graph checkpoint-version history (v41), and a separate durable `authorization_epoch` control-plane row (v42). The v41 upgrade atomically backfills only each v40 head as a `migration_floor` and installs a write fence, so pre-v41 head-only writers fail rather than split a current head from its history. The v42 epoch is intentionally separate from `control_revision`: it advances with built-in SQL release, canary, persistent binding, and active-account authorization mutations, while control synchronizers keep using their narrower revision. It does not make external or in-memory profile, policy, capability, grant, or principal resolvers transactionally current. Migrations are additive and the store refuses a database newer than this binary; the complete versioned chain is recorded in the public inventory rather than duplicated as stale migration prose here.

Event payloads are JSONL TEXT in every durable store, so sessions migrate between backends by copying data. Loads restore only the committed prefix and never append repair events; crash-tail repair is an explicit execution-owner operation.

### Cold session-object compression

The example server runs `ColdSweeper` every six hours only when its selected
session store uses the immutable object layout (`S3SessionStore`). It retains
and passes the exact `ObjectStore` instance used to construct that session
store, and limits the sweep to `sessions/`. It never derives a store from, or
sweeps through, the independently hot-swappable resource `DynamicObjectStore`;
resource keys and `tenants/` are outside this job's scope.

The object-layout session store can be backed by either `FileObjectStore` or
an S3-compatible `ObjectStore`. In this server, the object layout is selected
by the database-backed S3 session configuration. A legacy `HARNESS_SESSION_DIR`
value may be imported only when the session settings row is absent; it selects
`FileSessionStore`, whose one-JSONL-file-per-session layout neither exposes an
`ObjectStore` nor has the object-layout compression contract, and SQL sessions
are likewise not swept. The server deliberately does not guess an object
backend for either case; retaining the instance at construction provides the
safe wiring without widening the storage API.

Compression is intentionally deny-by-default. A candidate must be exactly
`sessions/{valid-id}/events/{12-digit}.jsonl`, and its start sequence must be
strictly below that Session's committed `meta.version`; this excludes
`meta.json`, `evidence.json`, tenant resources, existing `.zst` objects, and
orphan chunks from an interrupted Save. Object reads use the `.zst` fallback
only for that immutable event-key shape, and session loaders normalize a plain
chunk and its `.zst` sibling to one logical chunk. The sweeper paginates past
ineligible entries, so a low sweep limit cannot starve a later eligible chunk.
Conditional `ObjectStore.Put` is deliberately fail-closed for that event-key
shape: `If-None-Match:*` and `If-Match` return a precondition failure instead
of trying to bridge the plain and cold representations without a cross-key
atomic CAS. `S3SessionStore` writes its immutable chunks unconditionally, so
its normal commit path is unaffected.

For historical generic `.zst` siblings, a successful File/S3 `Put` commits the
plain primary object first and then performs best-effort sibling cleanup: an
`If-Match` or `If-None-Match:*` success is never reported as a failure merely
because cleanup failed. For ordinary mutable keys, `Delete` removes the
`.zst` sibling before the plain object and leaves the plain object intact if
that first step fails. The sweeper's own immutable-event delete deliberately
preserves the sibling it has just written, making `PUT compressed` then
`DELETE plain` retry-safe.

The included server registers both SQLite and pgx and loads a working-directory
`.env` without replacing process environment variables. Set
`HARNESS_DATABASE_TYPE=postgres` with `HARNESS_POSTGRES_DSN` to use PostgreSQL;
an unset type defaults to SQLite and `HARNESS_SQLITE_PATH` defaults to
`./data/core.db`. PostgreSQL defaults to 20 open and
10 idle connections, while embedded SQLite defaults to one connection. Override
with `HARNESS_DB_MAX_OPEN_CONNS`, `HARNESS_DB_MAX_IDLE_CONNS`,
`HARNESS_DB_CONN_MAX_LIFETIME`, and `HARNESS_DB_CONN_MAX_IDLE_TIME`. Production
DSNs should require TLS and deployments should manage credentials outside the
repository. PostgreSQL can start with `HARNESS_DB_MAX_OPEN_CONNS=1`; when idle
connections are not configured separately, the idle limit is reduced to one as
well. A single connection is safe for startup but severely limits runtime
throughput and concurrency.

Harness PostgreSQL startup and schema migration use session-level advisory
locks. Connect directly or use session pooling. PgBouncer transaction pooling
is unsupported for the control database because it does not preserve one
backend session from lock through unlock; configure `pool_mode=session` or
bypass the proxy for this DSN.

The S3 backend speaks a minimal SigV4 REST client (no SDK dependency) against AWS S3, MinIO, R2, and S3-compatible gateways. Conditional writes (`x-amz-if-match` / `x-amz-if-none-match`) provide the CAS; for gateways without them, `DisableConditionalWrites` opts out behind an external single-writer lease. The same `ObjectStore` interface also has `FileObjectStore` (local disk, content-hash etags) and `MemoryObjectStore` (tests) implementations.

### Run-scoped capabilities

`TurnInput.RunCapabilities` mounts one-shot capability bindings at `<session-scope>/run/<run-id>` for that resolve only (product doc §5.8). The snapshot freezes the providers, the bindings are removed immediately after, and later runs cannot see them — useful for temporary verifiers and one-shot tools.

### Catalog queries

`CapabilityRegistry.Entries` and `AgentProfileRegistry.ListProfiles` return the declarations visible at a scope without a principal or permission filter; `GET /v1/capabilities` and `GET /v1/profiles` expose them for Agent Studio-style surfaces.

## Five-minute starter

`cmd/starter-server` is the smallest runnable assembly: one OpenAI-compatible
Profile and one fixed, read-only public HTTP connector. It is intentionally
separate from the product-neutral `cmd/server`; it accepts only unset or
`HARNESS_MODE=dev`, uses development header identity, and must not be deployed
as a SaaS service. Replace the example Profile and connector and use
`cmd/server` with a verified authenticator before production use.

```sh
export HARNESS_LLM_API_KEY=...
export HARNESS_LLM_MODEL=...
# Optional: durable local sessions instead of the safe in-memory default.
export HARNESS_STARTER_SQLITE_PATH=./data/starter.db
go run ./cmd/starter-server
```

### Private Runner worker examples

`examples/runner-worker` contains equivalent Go and Python-stdlib protocol
workers. They use a dedicated runner token and explicit capability allowlist,
renew a generation-fenced lease, observe cancellation, and never log task
payloads, results, idempotency keys, or trace carriers. See its `PROTOCOL.md`.

In another terminal:

```sh
curl -X POST http://localhost:8080/v1/sessions \
  -H 'content-type: application/json' \
  -H 'X-Harness-Tenant: acme' \
  -H 'X-Harness-Subject: alice' \
  -d '{"profile_id":"starter.assistant"}'
```

Use the returned session ID with `POST /v1/sessions/SESSION_ID/runs`. The header
authenticator is for local development only. After this module has been
published, external consumers install it with:

```sh
go get github.com/cc-auto-agent/harness-core/pkg/core
```

Before publication, run the local external-module smoke with
`powershell -ExecutionPolicy Bypass -File ./scripts/module-smoke.ps1`; it uses
a temporary consumer module and a local `replace`, never a network fetch.

## Cryptocurrency example

The example demonstrates the intended separation:

- `crypto.market.quote`: market quote tool
- `crypto.token.discover`: new-token discovery Skill
- `crypto.token.analyze`: token analysis Skill
- `crypto.agent.analyst`: product Agent profile
- `crypto.agent.avatar`: user-owned digital-human profile

The tenant example replaces the product market-data provider. Real-time quotes,
candles, listings, order books, and on-chain state should be modeled as live
connectors/tools rather than RAG. RAG is optional for whitepapers, audit reports,
historical research, and other document retrieval scenarios.

Run it:

```sh
go run ./cmd/demo
```

The output shows three runs appended to one session, including immutable snapshot IDs and audited fast-path tool calls.

## HTTP/SSE adapter

`pkg/server` is a thin transport adapter. It depends on injected authentication, runtime composition, and session storage. It contains no product capabilities.

Start the example server:

```sh
go run ./cmd/server
```

On a database that did not contain an `accounts` table before migration, the
server creates `admin_` plus five digits with a random one-time password and
prints both once after commit through process log output. Capture that output
securely: a production log collector may retain it, and it must not be copied
into tickets or source control. The account remains pending until that password
is changed in `/console`. An existing but empty `accounts` table is left empty.

For the production SaaS/container boundary, use the non-root `Dockerfile`,
managed PostgreSQL, immutable image digests, and `/healthz` plus `/readyz`
probes. The compose file is development-only. See
[deployment and recovery](docs/deployment.md) for secret handling, migration
startup, rolling upgrades, and PostgreSQL backup/restore.

The included header authenticator is development-only. Production deployments must replace it with verified JWT, mTLS, gateway identity, or another trusted authenticator. The default durable layout uses the embedded SQL database for sessions and `HARNESS_DATA_DIR/resources` for resources; database settings are authoritative, with legacy environment values used only when their row is absent. The example also enables streamed `assistant/chunk` events and context compaction.

```sh
curl -X POST http://localhost:8080/v1/sessions \
  -H 'content-type: application/json' \
  -H 'X-Harness-Tenant: acme' \
  -H 'X-Harness-Subject: alice' \
  -d '{"profile_id":"crypto.agent.avatar"}'

curl -N -X POST http://localhost:8080/v1/sessions/SESSION_ID/runs \
  -H 'content-type: application/json' \
  -H 'X-Harness-Tenant: acme' \
  -H 'X-Harness-Subject: alice' \
  -d '{"message":"帮我看盘 BTC"}'

# Queue a durable asynchronous run. Poll the returned run_id with GET below.
curl -X POST http://localhost:8080/v1/sessions/SESSION_ID/runs/async \
  -H 'content-type: application/json' \
  -H 'Idempotency-Key: client-operation-42' \
  -H 'X-Harness-Tenant: acme' \
  -H 'X-Harness-Subject: alice' \
  -d '{"message":"后台完成这项工作"}'

# Abort the active run; it still reaches run/error + run/end.
curl -X POST http://localhost:8080/v1/sessions/SESSION_ID/cancel
```

Routes:

- `GET /healthz`
- `GET /readyz`
- `GET /console` — embedded web admin console (zero build, vendored Alpine.js)
- `POST /v1/sessions`
- `GET /v1/sessions`
- `GET /v1/sessions/{id}`
- `GET /v1/sessions/{id}/events`
- `POST /v1/sessions/{id}/runs` (SSE stream)
- `POST /v1/sessions/{id}/runs/async` — durable enqueue; returns `202` and a queued run record
- `GET /v1/sessions/{id}/runs` — durable run history when `Config.RunControl` is configured
- `GET /v1/sessions/{id}/runs/{runID}` — one durable run status
- `POST /v1/sessions/{id}/runs/{runID}/cancel` — cancel one queued, running, or approval-waiting durable run
- `POST /v1/sessions/{id}/cancel`
- `GET /v1/profiles` — profile catalog visible to the caller
- `GET /v1/profiles/{id}/capabilities`
- `GET /v1/capabilities` — capability catalog visible to the caller
- `POST /v1/profiles/{id}/publish` / `GET /v1/profiles/{id}/releases` / `POST /v1/profiles/{id}/rollback` (requires `Config.Releases`)
- `POST/GET /v1/profiles/{id}/canaries` plus percentage, pause, resume, rollback, and promote actions (requires `Config.Canaries`)
- `POST /v1/runners/claim`, `POST /v1/runners/tasks/{id}/renew`, and `POST /v1/runners/tasks/{id}/complete` (requires `Config.Runners`)
- `GET /v1/admin/runners/tasks` / `GET /v1/admin/runners/tasks/{taskID}` — tenant-authorized metadata-only durable runner task catalog
- `POST /v1/admin/runners/tasks/{taskID}/cancel` — cancel a queued task or request cancellation of a claimed task
- `POST /v1/admin/runners/recover` — platform-admin expiration recovery for private-runner claims
- `POST /v1/admin/capabilities/bind` — dynamically bind an HTTP or private-runner capability; `GET /v1/admin/overview`, policy / credential / capability-disable admin routes (see `/v1/admin/bindings`)
- `GET /v1/admin/audit` — durable operation trail (logins, publishes, policy/credential/capability changes, resource writes) Durable rows are capped at `storage.MaxAuditEvents` (8192) and detail payloads at 16 KiB; retention prune frees slots.
- `GET/POST/PUT/DELETE /v1/admin/obs/*` — watch rules (tool / keyword) and their recorded hits
- `POST /v1/admin/backtests` — replay a recorded session's assembled context through a chosen profile
- `POST/GET /v1/admin/evaluations/datasets` and `GET /v1/admin/evaluations/datasets/{id}/versions/{version}` — immutable versioned datasets
- `POST/GET /v1/admin/evaluations/runs`, `GET /v1/admin/evaluations/runs/{id}`, and `POST /v1/admin/evaluations/runs/{id}/resume` — isolated durable evaluation execution and recovery
- `GET /v1/admin/evaluations/runs?composition_revision=<sha256>&assignment_revision=<sha256>` — indexed bounded lookup by durable Composition/Assignment evidence
- `POST /v1/admin/evaluations/runs/{id}/gate` — minimum-score/pass/regression gate against a baseline
- `GET /v1/admin/approvals` / `GET /v1/admin/approvals/{id}` — tenant-scoped durable approval queue
- `POST /v1/admin/approvals/{id}/decision` — atomically approve or deny and requeue the suspended run
- `GET/POST/PUT/DELETE /v1/admin/notification-targets` — tenant-scoped channel target metadata and revision-fenced mutations. Opaque target configuration is write-only and never returned; `notify.send` still requires approval.
- `POST /v1/auth/login` — account bearer login. Failed passwords are locked out after 10 tries / 15 minutes; the in-process tracker keeps at most 10,000 account IDs and never flushes live lockouts to make room for unseen identifiers.
- `POST /v1/auth/activate` — replace the generated administrator's one-time password, activate the account, revoke restricted tokens, and return a normal token
- `POST /v1/auth/logout` — revoke the presented bearer token; development header sessions without a token still succeed
- `GET/PUT/DELETE /v1/resources/{key...}` — S3-path resource management (S3 or embedded filesystem)

Built-in account tokens are opaque 32-byte hex values. The store persists only SHA-256 hashes, rejects empty/malformed tokens and non-positive TTLs, and caps TTL at `storage.MaxTokenTTL` (30 days). `Config.TokenTTL` defaults to 24h and is clamped to that maximum. Active accounts receive normal tokens; the generated pending administrator receives a token restricted by the server to activation and logout. Live unexpired tokens are capped at `storage.MaxLiveAuthTokens` (4096) after opportunistic expiry purge. Console accounts are capped at `storage.MaxAccounts` (4096) and tenants at `storage.MaxTenants` (1024); creating an existing row at the cap still returns the ordinary conflict. Deployment settings are capped at `storage.MaxSettings` (256) keys and 256 KiB per value; replacing an existing key does not consume another slot. `storage.NewSQLQueuedPrincipalResolver` is the native queued-worker resolver for this account store: it maps only an active account with an exact tenant/subject match into a `core.Principal`, reports unavailable, inactive, identity-mismatch, and corrupt-account states with non-disclosing typed errors, and leaves database failures retryable. `storage.ValidateAuthorizationEpochSQLPrincipalAuthority` can additionally verify that this native resolver shares the sealed SQL authority of queued Sessions, claims, leases, and Tool Journal rows.

### Dynamic capability binding

`POST /v1/admin/capabilities/bind` accepts a scope, a normal
`CapabilityManifest`, optional `replace`, and an `execution` object. Omitting
`execution.runtime` preserves the original HTTP behavior. HTTP bindings use
`runtime: "http"` and keep the public URL, method, and header validation
rules; credentials in sensitive HTTP headers remain `$credential:<ref>`
references.

Set `execution.runtime` to `"runner"` to turn the manifest into a durable
private-runner task without writing a Go registration. The server must have a
configured `server.Config.Runners`; runner bindings reject HTTP fields such as
`entrypoint`, `method`, and `headers`. The mounted manifest records
`execution.runtime: "runner"`, and each call is delivered through the normal
claim/generation/complete protocol. Dynamic bindings preserve their runtime in
the binding journal, so restart restore applies the same provider decision. The journal keeps at most `storage.MaxAdminBindings` (256) records and rejects payloads above 256 KiB, because restart restore loads the full set. Profile replacement requires the optional `storage.BindingJournalReplacer`: the SQL implementation replaces the old durable row in one transaction and advances the authorization epoch once, rather than exposing a `Delete`/`Record` gap. The profile admin route validates a detached candidate, commits that durable replacement, then calls `AgentProfileRegistry.ReplaceExact` to swap the uniquely matching live layer at its existing mount order under one registry write lock. Concurrent resolves therefore see a complete old or new projection, never the intermediate mount/unmount mix; the existing unmount handle still removes the replacement. If a durable replacement cannot publish, the server records a binding-specific projection failure, rejects new synchronous runs and queue claims with `503`, and leaves control/admin recovery routes available. A same-payload PUT retries reconciliation without rewriting the durable binding; a successful matching repair clears only that binding's projection failure, not unrelated readiness errors. If the original live slot is missing or ambiguous, the server keeps that gate closed rather than remounting at a new order; restart or another clean projection rebuild from the durable journal is the safe recovery path. `Config.AuthorizationEpochReader` is an opt-in local execution-admission foundation: it blocks new runs and queue claims when a known projection fault or durable authorization-epoch lag exists, while leaving active runs and admin/health routes alone. It deliberately does not replay bindings, releases, or canaries. An integration that has completely rebuilt its local projection from a durable control source must call `Server.MarkExecutionProjectionAppliedEpoch`; this local marker is neither a strict authorization proof nor a transaction fence. In-process Capability, Policy, Profile, and Credential registries keep at most `core.MaxRegistryBindings` (4096) live mounts; unmount frees a slot. The process-local admin handle table uses the same cap, including ephemeral static-credential binds that are never journaled.

When authorization-epoch admission is configured, a dynamic policy, disable,
environment-credential, or capability binding holds the server-local projection
write gate while it publishes locally and writes its durable binding record. New
server-owned executions therefore cannot observe the temporary local mount
before the SQL record/epoch commit. A known failed write rolls that mount back;
an ambiguous write response or a non-single-step epoch change leaves admission
faulted instead of guessing whether it is safe to publish or unmount. The
admin-handle table is updated only after both local and durable publication
succeed. This mode requires a native SQL binding journal and epoch reader in the
same sealed SQL authority as Sessions; static/ephemeral credentials and
third-party dynamic capability factories are rejected rather than treating
factory construction or in-memory state as epoch-fenced. It closes this
instance's mount-before-commit window only: it is not a cross-instance binding
replayer, a complete Release/Canary projection snapshot, or strict
completed-result recovery authorization.

```json
{
  "scope": [{"kind": "global", "id": "global"}],
  "manifest": {"id": "media.render", "version": "1.0.0", "kind": "connector"},
  "execution": {"runtime": "runner"}
}
```

## Observability watch rules and backtesting

Watch rules let operators mark conversations for review: a rule matches a **tool** (exact capability id at call time) or a **keyword** (case-insensitive substring over user and assistant text), and every hit durably records the session id, run id, actor, tenant, and a content snippet. The console lists rules and hits and can jump straight to the session's event log. Matching is best-effort and deduplicated per rule per run — it never alters run semantics. The store keeps at most `storage.MaxObsRules` (256) watch rules, because every run event scans the full enabled set. Recorded hits are capped at `storage.MaxObsHits` (8192) and snippets at 4 KiB; retention prune frees hit slots.

Backtesting closes the loop: `POST /v1/admin/backtests` copies a recorded session's assembled context (optionally truncated at a seq) into a NEW session under the source principal — so capability resolution and policies match the original tenant context — and synchronously replays a probe message through a chosen profile. Backtests use the Evaluation safety filter: only idempotent tools that do not require approval are exposed by default; this platform-admin-only endpoint may explicitly list non-idempotent capability IDs. Backtest runs are themselves observable, and each links back to its source session via metadata plus the source Composition/Assignment revisions returned in the response.

## Evaluation and regression gates

`pkg/evaluation` provides immutable Dataset versions, isolated Case Sessions,
deterministic Assertion specs, custom Evaluator registration, durable Run/Case
results, crash recovery, artifact snapshots, baseline comparison, and regression
gates. Evaluation concepts stay outside `pkg/core`; the kernel contributes only
the general subtractive `TurnInput.CapabilityFilter`. Concurrent `running` Evaluation Runs are capped at `evaluation.MaxRunningEvaluationRuns` (32); completing or failing a run frees a slot, and identical-header SQL replay still succeeds at the cap. Each Dataset ID keeps at most `evaluation.MaxDatasetVersionsPerID` (64) immutable versions; identical-version replay still succeeds at the cap. Distinct Dataset IDs are capped at `evaluation.MaxDatasetIDs` (256); a new version of an existing ID does not consume another ID slot. Stored Evaluation Runs (running plus terminal) are capped at `evaluation.MaxEvaluationRuns` (4096); identical-header SQL replay still succeeds at the cap. Case results per run are capped at `evaluation.MaxDatasetCases` (1000); identical SQL case rows still replay at the cap.

Release Evaluation also performs a provider-neutral Capability compatibility
gate over the Scope-visible declarations selected by the Live and Candidate
Profiles. This comparison intentionally occurs before Principal grant, Policy,
credential, and Evaluation safety filtering, so a low-privilege evaluation
subject cannot hide a breaking change that affects higher-privilege callers.
By default it rejects removed capabilities,
Kind/Contract/version or provider-artifact changes, new permissions or
credentials, idempotency downgrade, newly required approval, new write effects,
reduced budgets/timeouts/output limits, model-tool exposure changes, output
Schema changes, and input JSON Schema narrowing. New capabilities are accepted
only when idempotent and approval-free. Unsupported Candidate Schema constraints
fail closed. Evidence is bounded to 96 detailed issues and 32 IDs per summary
list while preserving total counts and truncation flags; worst-case artifacts
remain below the Canary Gate persistence limit.

Built-in assertions are:

- expected Run status;
- exact answer or contained text;
- answer JSON Schema;
- capability requested / capability not requested.

Custom assertion kinds use namespaced IDs such as `acme.semantic_judge` and are
provided through `evaluation.Registry`. Evaluator panics and errors become
failed assertions instead of crashing the suite. Dataset Revision is a stable
SHA-256 over the normalized definition; the same ID/version cannot be replaced
with another revision.

Each Case runs in a deterministic, separate Session carrying
`evaluation.run_id`, Dataset ID/version/revision, and Case ID metadata. The
result records Profile/Capability Snapshot IDs, resolved model provider and
model revision fingerprint. Dataset context accepts only user/assistant text;
it cannot inject Tool results, system events, or arbitrary Session facts.

Safety defaults are fail-closed:

- tools requiring approval are always removed;
- idempotent tools are allowed;
- non-idempotent tools require an explicit allowlist;
- the HTTP API allows that unsafe allowlist only to platform administrators;
- Backtest uses the same policy.

Case Result rows commit immediately. Eval Run becomes `completed` only after
all Case rows exist. On interruption it remains `running` with the last error;
`/resume` skips committed Cases and uses deterministic Session/Agent Run IDs.
If a Case Session completed before its Case row was written, Resume reconstructs
the result without invoking the model or provider again. Interrupted Case logs
use the normal Crash Repair path.

`GatePolicy` can require Dataset pass, a minimum score, and a maximum score
regression against a completed Run on the exact same Dataset Revision. Dataset
and Run list endpoints return bounded summaries; single-item GET returns full
Cases and assertions. Evaluation artifacts are not deleted by generic 90-day
operational retention, so baselines are never silently removed.

### OpenTelemetry

`core.Telemetry` is the SDK-neutral instrumentation seam. `pkg/core` imports no
OpenTelemetry packages; `pkg/telemetry/otel` translates the fixed operations
and metric vocabulary to OpenTelemetry traces and metrics. Telemetry provider
panics are contained and never change Run semantics.

Set `HARNESS_OTEL_ENABLED=true` (or any standard OTLP endpoint variable) to
enable OTLP/HTTP export in `cmd/server`. `HARNESS_OTEL_SERVICE_NAME` defaults to
`harness-core`, and `HARNESS_OTEL_METRIC_INTERVAL` defaults to 30 seconds. The
adapter honors standard `OTEL_EXPORTER_OTLP_*` variables and installs W3C
TraceContext plus Baggage propagation. HTTP requests are wrapped with standard
server instrumentation.

`core.WithTelemetrySpanAttributes` carries up to eight bounded correlation
attributes through the in-process Context tree. `StartTelemetry` attaches them
to every descendant Span while operation-local values win on key collisions;
the safety wrapper reattaches them even if a Telemetry provider returns a
Context that dropped application values. Counter, Histogram, and Gauge helpers
never read these attributes, keeping high-cardinality identifiers out of
Metrics. This mechanism is process-local and is not serialized as W3C Baggage.

Exported operations and metrics include:

- `harness.run.segment`, `harness.model.call`, `harness.tool.call`, and `harness.queue.claim` spans;
- `harness.evaluation.run` and `harness.evaluation.case` spans;
- Run-segment (initial/resume), model, tool, queue-claim, recovery, approval-request/decision, tool-journal, async-submission, Evaluation Run/Case, and Gate counters;
- Run/model/tool/claim/approval-wait/Evaluation duration and Evaluation score histograms;
- Queue depth by state, oldest queued age, pending approvals, and oldest pending approval age gauges.

Run, Session, and Call IDs are trace-only attributes. Metric attributes are
limited to bounded operational dimensions such as status, model, capability,
decision, and outcome. Prompt text, Tool arguments/results, credential values,
approval payloads, and async messages are never placed in telemetry.

Async Queue submissions and synchronous approval suspensions persist only
`traceparent` and `tracestate` (256/512 byte limits). Workers restore that
carrier before starting the next Run segment, so durable execution remains in
the originating trace. Baggage is intentionally not persisted.

## Logging and audit

Server logging goes through `pkg/logging`: JSON records to stdout plus a size-rotating file (`HARNESS_DATA_DIR/logs/harness.log`, 64 MiB per file, 14 backups, pruned automatically — zero dependencies). Access logs cover every request; audit records cover every administrative mutation (logins, account and tenant changes, publishes and rollbacks, policy/credential/capability bindings, resource writes). Audit events are durable in the SQL schema (`audit_events`, also queryable at `GET /v1/admin/audit` and viewable in the console) and are always mirrored to the file log, so the trail survives even without the SQL store. Secret values are never audited.

### Log/Trace correlation

Request- and Run-path server records use `slog.InfoContext`, `WarnContext`, or
`ErrorContext`. That includes HTTP access and panic-recovery records, audit
records, synchronous and durable-worker Canary selection, Run-control/lease
errors, watch-hit persistence, and finished-Run stat failures. `pkg/logging.New`
can therefore read the OTel `SpanContext` carried by the supplied `Context` and
emit `trace_id`, `span_id`, and `trace_flags` without coupling `pkg/server` to a
particular logging handler.

The OTel HTTP middleware must wrap the complete public Server handler, so the
access logger (which runs after the response) and panic recovery are inside the
active request span:

```go
handler := oteltelemetry.HTTPHandler(api.Handler())
```

Async submissions persist W3C `traceparent`/`tracestate`; the worker restores
that carrier before its Run logs are written. Terminal persistence and lease
release deliberately use `context.WithoutCancel` before applying a bounded
timeout: it detaches client cancellation while retaining context values,
including the OTel `SpanContext`. Startup recovery, retention, and polling-loop
logs intentionally remain uncorrelated because they do not have a real request
or Run trace context.

## Storage configuration (console)

Local-to-S3 migration requires exactly one resource-writing server instance
during the migration window. The migration lease fences duplicate workers but
does not coordinate foreground writes from another process, even on a shared
volume. Multi-replica deployments should pre-provision S3 or drain traffic to
one writer before migrating.

The console's overview page can configure the resource object store between the local filesystem and any S3-compatible service: endpoint, bucket, keys, path-style, with a **connection test** before saving. An opt-in live smoke test can target local RustFS/MinIO when explicit endpoint credentials are supplied; it is skipped by the default suite and is not evidence that this checkout has validated a live S3 backend. The database row is authoritative; with no row, resources always start at `<HARNESS_DATA_DIR>/resources`. Saving an S3 configuration records a pending migration and exposes redacted progress; the local store remains active until copy and verification complete, after which activation is durable. Sessions take effect on restart. Secrets are stored encrypted (AES-256-GCM via a master key generated on first boot). Storage GET never returns `secret_key`; it returns `has_secret` and, when configured, a fixed-length preview (first three characters, eight mask characters, final four characters; short keys are fully masked). The Console displays that preview but never sends it back as a secret. Storage writes are explicit: omit `secret_key` to preserve, send a non-empty key to replace, or send `clear_secret:true` to delete; an S3 configuration without an effective secret is rejected. The former `••••` input remains accepted only as a legacy preserve mapper and is no longer emitted; a persisted historical marker remains unusable and is omitted from previews. Generic settings GET is fail-closed for every key: a found setting is reported as `redacted`/`write_only` without a `value`; future non-sensitive reads use a domain-specific DTO. Legacy S3 environment variables are read only when the corresponding database row is absent and are not advertised by `.env.example`.

The migration target must be a dedicated empty bucket/namespace; unexpected target keys fail closed and are never deleted. Once S3 is active, changing its bucket, endpoint, credentials, or switching directly back to local is rejected until a dedicated rebind/reverse-migration implementation is installed.

## Web console

The runtime embeds a web admin console at `/console`: a zero-build single-page application (vendored Alpine.js served locally — no CDN, no npm) that covers deployment overview, capability catalog with disable/enable, profile publishing and rollback, layered policies, credential references (values are write-only and never echoed), session listing with event inspection, and reversible admin bindings. It talks only to the same `/v1` JSON API as every other client and authenticates through the same `Authenticator` seam — production deployments gate `/console` and `/v1/admin/*` with their own gateway. See [architecture.md](docs/architecture.md) for the current integration boundaries; [mcp.md](docs/mcp.md) and [runner.md](docs/runner.md) document the MCP and private-runner protocols.

## Subagents

`subagent.Capability` implements the `harness.agent/v1` contract: an agent can be consumed as a capability by another agent. Each new call creates a real child session under the caller's scope (own snapshot IDs, own event log, own budget), runs one turn with the child profile, and returns the final answer. Continuation requires both `session_id` and `followup`; a caller-supplied session ID cannot start or replace a child. In-process child sessions are bounded (`MaxChildren`, default 32) and the oldest child is dropped when the cap is exceeded. The child principal carries an advanced `agent.depth` attribute, so delegation chains are bounded and the child can never exceed the caller's authority — everything it resolves flows through the same scope-ownership checks as any run.

## Workflows, memory, and retrieval

- **Workflows (`harness.workflow/v1`).** `workflow.NewCapability` wires a deterministic step sequence by stable `CapabilityID`. Step names are bounded identifiers (no slash or control characters); nested call IDs stay `parent/step` and are rejected above 256 bytes. Every nested step re-enters the active run's protected tool invoker, so schema validation, hooks, approval, rate limits, credentials, timeouts and tool budgets cannot be bypassed. Argument values of the form `$ref:<dotted.path>` resolve from workflow input and prior results; a failing step stops the workflow and records the failed step.
- **Memory (`harness.memory/v1`).** `memory.Capability` exposes remember / recall / forget as one tool over a `memory.Store`, keyed by the caller's principal scope. `memory.SliceStore` is the in-process reference; production backs the same interface with durable storage. Empty scopes are rejected. Entry IDs are at most 256 bytes and may not contain control characters; forget requires a non-empty ID. Keys, content, queries, and tags reject only NUL, so ordinary newlines remain valid. Recall queries are bounded to 4096 Unicode code points, at most 64 tag filters of at most 256 code points each, and a direct-Store result limit of at most 100 (`0` retains the Store-level unlimited convention; the model-facing capability requests 10). Memory writes use the same tag bounds plus the existing key/content byte limits. Each scope is capped at `memory.MaxEntriesPerScope` (1024) unique keys; replacing an existing key does not consume another slot. The store keeps at most `memory.MaxScopes` (4096) occupied scopes; writing an existing scope at the cap still succeeds, and forgetting the last entry in a reference-store scope frees the slot. These checks run in both reference and SQL stores before scanning data or constructing SQL.
- **Retrieval (`harness.rag.search/v1`).** `rag.SearchCapability` exposes search over a `rag.Index`; ingestion is a provisioning API, never a model tool. `rag.KeywordIndex` is the dependency-free reference (token overlap scoring and tag filters). Empty scopes are rejected. Document IDs are at most 256 bytes and sources at most 4096 bytes; both reject control characters. Content, queries, and tags reject only NUL, so ordinary newlines remain valid. Search queries are bounded to 4096 Unicode code points, 128 unique keyword tokens, 128 tag filters of at most 256 code points each, and `top_k` from 1 to 100 (`0` on the direct Index API selects the default 5). Document tags use the same per-item bound. Each scope is capped at `rag.MaxDocumentsPerScope` (1024) documents; replacing an existing document ID does not consume another slot. The index keeps at most `rag.MaxScopes` (4096) occupied scopes; ingesting an existing scope at the cap still succeeds. One document may not exceed `rag.MaxDocumentTokens` (8192) unique keyword tokens; ingest and projection rebuild fail closed at the cap so `rag_document_tokens` cannot grow with a 1 MiB unique-token payload. Tool schemas publish the same limits, while reference and SQL indexes repeat validation so direct API calls cannot bypass them or generate unbounded SQL placeholder lists.
- **Tool library disclosure (`harness.tool.library/v1`).** When a run snapshot contains non-hot tools (connectors, skills, ordinary tools), the model-facing schema list keeps memory/agent/workflow/router/knowledge tools plus one library. Search/list return qualified ids and short descriptions; describe expands one schema; the model then calls that id. Shared short names return an `explore` candidate list instead of guessing. Each library result carries `library.*` metadata so query → candidate → choose can be improved later. SQL stores keep the same traces in `tool_library_observations` (at most 8192 rows; oldest drop at the cap). Catalog records may include `triggers` and `notFor` so same-named tools can be excluded by intent; platform operators can read the log at `GET /v1/admin/tool-library/observations`. Direct Execute of a discovered id still uses the original approval/budget funnel. Catalogs that are only hot tools are left unchanged. MCP servers use the same contract as a remote library. Remote tools live in `extensions/toollib.Catalog` (keyword search by default; `SetSearcher` installs a vector/hybrid ranker without changing search/list/describe/call; MCP uses `HybridSearcher`; `HashingSearcher` is an embedding-free hashed-token cosine rerank on the same candidate set (keyword candidates + character 3-gram rerank) and never reintroduces NotFor drops) (not user RAG tables): a full `tools/list` reconciles by id and content revision (add/update/delete); an empty or failed list keeps the last good catalog. A run still freezes its snapshot, so in-flight agents do not see mid-turn tool edits.

## Private runners

`runner.Hub` is the store-neutral platform side of the private-runner protocol.
`runner.NewHub()` uses the concurrency-safe in-memory reference Store;
`runner.NewHubWithStore(storage.NewSQLRunnerStore(...))` uses the shared SQLite
or PostgreSQL queue. `runner.RegisterCapability` mounts a runner-backed tool
without enlarging the kernel interfaces.

Tasks are durable records with queued/claimed/completed/cancelled/failed state,
stable argument digests, bounded canonical results, cancellation requests,
attempts, claim leases, and a monotonically increasing generation fence.
Queued plus claimed tasks are capped at `runner.MaxInFlightTasks` (1024);
idempotent replay still succeeds at the cap, and cancelling or finishing a task
frees a slot. All durable tasks, including terminal history, are capped at `runner.MaxStoredTasks` (4096).
Workers use `POST /v1/runners/claim`, renew long work through
`POST /v1/runners/tasks/{id}/renew`, and submit the exact returned generation
to `POST /v1/runners/tasks/{id}/complete`. A stale worker or generation cannot
renew or commit. Expired claims are recovered proactively by the server while
it remains online: cancellation requests become cancelled, retryable tasks
return to queued, and exhausted tasks become failed/worker_lost. Recovery no
longer depends on the next Claim request or a manual API call. Deployments with
multiple server instances are safe because PostgreSQL claims and recovery use
`FOR UPDATE SKIP LOCKED`; the platform-admin recovery endpoint remains
available for an explicit pass.

Non-empty idempotency keys are scoped to tenant + subject + Scope + capability.
The same arguments rejoin or replay the canonical Task; different arguments
fail with a stable conflict. If a provider times out before claim, the queued
Task is safely cancelled and returns `runner_timeout`. If it times out after
claim, the provider returns `ErrOutcomeUnknown`; the existing Tool Invocation
Journal therefore fences non-idempotent re-execution while an idempotent retry
can rejoin the same durable Task and eventually recover its canonical result.

The generic server's 90-day retention loop passes the same UTC cutoff used for
its bounded SQL tables to `runner.Hub.PruneTasks`. Stores that implement
`runner.TaskRetention` delete only unkeyed terminal Tasks completed strictly
before that cutoff. Tasks with an idempotency key are retained permanently as
duplicate-execution fences, and active queued or claimed Tasks are never
deleted. Runner Stores without the optional retention capability are skipped
without disrupting the existing SQL retention pass.

When the Hub has a `core.Telemetry` implementation that supports
`TelemetryContext`, the first submission persists its bounded W3C
`traceparent` and `tracestate` carrier on the durable Task. A later submission
that reuses the same idempotency key keeps the canonical Task's original
carrier; trace context is neither part of the argument digest nor an
idempotency-conflict input. `POST /v1/runners/claim` returns that carrier only
with the execution payload. An external private worker should restore it as
the parent Context before starting its own span and executing work. Baggage is
deliberately never persisted or returned by this protocol.

Configure `server.Config.RunnerAuthenticator` with a worker-specific identity
and its trusted `server.RunnerCapabilitiesAttribute` grant. This attribute is
set only by the trusted authenticator implementation, never from a
user-controlled HTTP header or request field. It is either the standalone
`"*"` wildcard or a comma-separated list of namespaced capability IDs (after
trim, deduplication, and lexical sorting; at most 64 IDs). A dedicated runner
without this attribute is denied Claim with `403`, which makes a missing pool
grant fail closed. This enables isolated worker pools: for example, a renderer
credential can receive only `media.render`, while an analytics credential can
receive only `analytics.report` even when the latter task was queued first.

`POST /v1/runners/claim` accepts an optional selector body. An omitted body,
empty body, or `{}` keeps backward compatibility. A dedicated runner with a
finite grant uses its complete grant when it omits the selector; a supplied
selector must be a subset and can only narrow the lease candidates:

```json
{
  "capabilities": ["media.render"]
}
```

The wildcard grant may request any valid selector, or omit it to claim every
capability. The platform-admin fallback used when no dedicated
`RunnerAuthenticator` is configured is likewise authorized for every
capability. Request selectors never grant additional access; wildcard values,
invalid IDs, mixed wildcard grants, unknown fields, and lists above 64 entries
are rejected.

Renew and Complete deliberately do not re-evaluate the Claim allowlist. They
remain fenced solely by the current worker identity and the exact returned
generation, so a grant change cannot block the canonical completion of work
that the worker already leased. A stale worker or generation cannot renew or
commit.

### Runner task administration

The normal `server.Config.Authenticator` protects the runner task administration
surface; `RunnerAuthenticator` is intentionally used only for worker Claim,
Renew, and Complete. `GET /v1/admin/runners/tasks`, task Detail, and task
Cancel require an account administrator or tenant administrator. List accepts
`tenant`, `subject_id`, `scope`, `capability`, `worker_id`, repeated or
comma-separated `state`, `limit`, and `offset` selectors. Account administrators
can query any tenant and any valid scope. Tenant administrators are forced to
their own tenant and, by default, their own Scope subtree; an explicit scope
must be that Scope or a descendant, so ancestor, global, and sibling Scope
queries are rejected.

These routes expose only the metadata `TaskSummary`: task arguments, results,
idempotency keys, argument digests, and W3C trace carriers are never returned.
Detail and Cancel use an ID-filtered metadata catalog lookup, so absent and
out-of-scope task IDs both return `404`. Cancel returns `cancelled` for queued
tasks, `requested` for claimed tasks, and `terminal` for already terminal
tasks; its `runner.task.cancel` audit detail records only task ID, capability,
previous state, and disposition. `POST /v1/admin/runners/recover` is restricted
to platform account administrators, calls one UTC expiration-recovery pass, and
audits its requeued and terminal counts. Task catalog endpoints return `501`
when runners are not configured or the Store does not opt into `TaskCatalog`.

The generic server uses `SQLRunnerStore`, accepts optional
`HARNESS_RUNNER_TOKEN` plus a stable `HARNESS_RUNNER_ID`, and requires
`HARNESS_RUNNER_CAPABILITIES` whenever that token is set. It exposes
`HARNESS_RUNNER_CLAIM_TTL`, `HARNESS_RUNNER_RECOVERY_INTERVAL`, and
`HARNESS_RUNNER_RESULT_POLL_INTERVAL`. The recovery interval defaults to half
the claim TTL, clamped to 1–30 seconds. Without a token, runner routes
intentionally fall back to platform-admin
authentication. The Claim response includes only the execution payload,
optional W3C trace carrier, and fence fields, not tenant/subject/Scope
ownership data. The generic server wires its OTel recorder into `runner.Hub`;
with OTel disabled the Hub receives nil and Tasks carry no trace carrier.

For dynamically managed capabilities, configuring `Config.Runners` is the only
server-side code integration required: an operator can bind a manifest with
`execution.runtime: "runner"` through the admin API, and a private worker that
already speaks the runner HTTP protocol can claim and complete it. Static Go
registration through `runner.RegisterCapability` remains available for
deployments that prefer compile-time composition.

Verification covers first-submit carrier injection, carrierless claims,
idempotency rejoin retaining the original carrier, and a dynamic runner
capability call whose HTTP Claim response contains the exact `traceparent` and
`tracestate` supplied by a `TelemetryContext` implementation.

## Multi-instance deployment primitives

- **Distributed session lease.** `SQLSessionStore` implements `SessionLeaser` (`AcquireSessionLease` / `RenewSessionLease` / `ReleaseSessionLease`): exactly one instance works a session at a time, long runs renew at `TTL/3`, expired leases are takeable, and ownership loss cancels the worker context. Every server acquisition has a fresh holder; queued holders additionally embed the queue generation, preventing delayed renewal/release from an old acquisition from matching a later one.
- **Durable run control and queue.** `SQLRunControlStore` records queued, running, `waiting_approval`, and terminal status, owner, timestamps, error code and cancel requests. Queued plus running plus `waiting_approval` runs are capped at `storage.MaxInFlightRuns` (1024); finishing or failing a run frees a slot, and idempotent async replay still succeeds at the cap. Queue claims use a lease plus a monotonically increasing generation as the fencing token; attempt remains the failure-retry counter, so an approval pause does not consume crash retries. A dedicated recovery loop requeues expired claims while the service remains online and marks exhausted claims `failed/worker_lost`. Queue-claim leases own task delivery; `SessionLeaser` independently serializes session-history mutation. Queued Session persistence is SQL-only and transactionally fences the claim, cancellation, Session lease, owner identity, and Session version. `server.New` rejects Memory/File/S3 Session stores, separate SQL handles, or custom adapters that do not retain the sealed native SQL fence capability; it does not provide a cross-store fallback. A run ID already present in the Session log is reconciled fail-closed without replaying model or tool side effects.
- **PostgreSQL queue locking.** PostgreSQL claims and expired-claim recovery use `FOR UPDATE ... SKIP LOCKED`, so one locked candidate does not stall unrelated workers. The partial unique index still enforces at most one active/waiting Run per Session. SQLite keeps its transaction-and-conditional-update path.
- **Operational telemetry.** Queue/Approval SQL stores expose optional read-only snapshots; the Worker records depth and oldest-age gauges. Empty Queue polls emit low-cost Counter/Histogram observations but no Span, avoiding unbounded idle trace volume. Finished-run `run_stats` rows are capped at `storage.MaxRunStats` (8192).
- **Worker lifecycle.** `StartRunWorkers` starts bounded local consumers and the claim reaper. `Shutdown` stops new claims first and drains claimed runs before returning; only an expired shutdown deadline force-cancels active work. The example server enables the SQL Session Lease and exposes `HARNESS_RUN_WORKER_POLL_INTERVAL`, `HARNESS_RUN_WORKER_CLAIM_TTL`, `HARNESS_RUN_WORKER_CONCURRENCY`, and `HARNESS_RUN_WORKER_MAX_ATTEMPTS`. Synchronous HTTP runs keep at most `server.MaxActiveRuns` (1024) cancellable session entries; replacing the same session does not consume another slot.
- **Tool side-effect fencing.** `SQLToolInvocationJournal` protects the provider boundary independently from Session and queue leases. Completed results are canonical and replayable; non-idempotent unknown outcomes cannot be automatically executed again. The example server enables it on the shared SQL database.
- **Durable approvals.** `SQLApprovalStore` implements `DurableApprover` and the administrative query/decision surface. Approval decision and `waiting_approval → queued` transition commit in one SQL transaction. Pending expiry is monitored by the worker service; `HARNESS_APPROVAL_TTL` defaults to 24 hours. Cancelling a suspended run atomically closes its approval fence and appends a cancelled terminal state to the Session.
- **Client submission idempotency.** `POST /runs/async` accepts an optional opaque `Idempotency-Key` of 1–256 non-control characters. The mapping is scoped to tenant + subject + Session and atomically commits with RunControl and Queue creation. A retry of the same message returns the original current RunRecord with `Idempotency-Replayed: true`; the first response carries `false`. Reusing the key with another request returns `409`. Responses include a canonical `Location`; only the key SHA-256 is stored. Retention removes old mappings only after their Run is terminal. Stored idempotency rows are capped at `storage.MaxRunSubmissions` (8192); replaying an existing tenant/subject/session/key still returns the canonical run.
- **Release journal.** `ReleaseManager` optionally records every publish and rollback to a `ReleaseJournal` (the SQL store implements it). Complete Layer artifacts and revisions are restored into the live registry after restart, and operation IDs make promotion-triggered publishes idempotent. Each profile keeps at most `storage.MaxReleaseHistoryPerProfile` (64) recorded versions because Sync loads the full history.
- **Durable canaries.** `CanaryManager` persists the gated Layer artifact, overlapping-Scope Release baseline revision, stable Subject bucket, basis points, evaluation evidence, and lifecycle (`active`, `paused`, `promoting`, `promoted`, `rolled_back`). A restart rebuilds open candidate registries and reconciles an interrupted promotion against its release operation ID. Open canaries (`active` / `paused` / `promoting`) are capped at `storage.MaxOpenCanaries` (64) because every control refresh loads that set. Independent server instances sharing SQL refresh Release/Canary state before profile-sensitive requests and Worker execution.
- **Control-plane revision.** Release and Canary mutations increment one durable monotonic revision in the same SQL transaction. Request-time refresh performs an O(1) revision read when nothing changed and scans durable artifacts only after a control-plane mutation.
- **Durable memory and retrieval.** `SQLMemoryStore` and `SQLRagIndex` back the `harness.memory/v1` and `harness.rag.search/v1` contracts on the same SQL schema, with identical semantics to the in-process reference implementations.

### RAG projection maintenance

The v27 token/tag lookup tables are derived state. A platform administrator can
inspect aggregate health or atomically rebuild them at runtime:

```text
GET  /v1/admin/rag/projection
POST /v1/admin/rag/projection/rebuild
```

Both routes use the ordinary `Authenticator` and require the platform
`admin` role because the projection spans every tenant. Tenant administrators
and users receive `403`; deployments without `Config.RagProjection` receive
`501`. Responses contain only seven aggregate counts: canonical documents,
token rows, tag rows, token-bearing documents, tag-bearing documents, orphan
token rows, and orphan tag rows. They never return document IDs, document
content, tag values, token values, or trace/secret material. A successful
rebuild records the `rag.projection.rebuild` audit action with only those
counts.

`SQLRagIndex.RebuildProjection` treats `rag_documents.content` and
`rag_documents.tags_json` as canonical and leaves both unchanged. It clears and
rebuilds only the derived lookup tables in one transaction, reading canonical
documents in bounded 16-row batches and applying the same public RAG
validators before writing derived rows. PostgreSQL acquires
`SHARE ROW EXCLUSIVE` locks on `rag_documents`, `rag_document_tokens`, and
`rag_document_tags` for the transaction, preventing concurrent projection
writers from committing an inconsistent mixed generation. SQLite obtains its
write lock before scanning canonical rows. Any decode, validation, insert,
statistics, or commit error rolls the rebuild back.

The generic `cmd/server` constructs one `SQLRagIndex` from the shared database
and injects it into `server.Config.RagProjection`. This is maintenance wiring
only: it does not automatically mount or expose a RAG capability. The embedded
Console shows the seven-count “RAG Projection / RAG 投影” page and its rebuild
button only to platform administrators.

### Memory projection maintenance

The v28 Memory search/tag tables are derived state. A platform administrator
can inspect aggregate health or atomically rebuild them at runtime:

```text
GET  /v1/admin/memory/projection
POST /v1/admin/memory/projection/rebuild
```

Both routes use the ordinary `Authenticator` and require the platform `admin`
role because the projection spans every tenant. Tenant administrators and users
receive `403`; deployments without `Config.MemoryProjection` receive `501`.
Responses contain only seven aggregate counts: `canonical_entries`, `tag_rows`,
`tagged_keys`, `orphan_tag_rows`, `key_search_missing`,
`content_search_missing`, and `search_projected_entries`. A successful rebuild
records the `memory.projection.rebuild` audit action with only those counts.

`search_projected_entries` is a coverage indicator: it counts rows with both
derived `key_search` and `content_search` fields non-empty. It is not proof
that those fields are consistent with canonical Memory content.

`SQLMemoryStore.RebuildProjection` leaves the canonical
`memory_entries.key`, `memory_entries.content`, and `memory_entries.tags_json`
unchanged. It rebuilds only derived lower-cased search text and exact-tag rows
in one transaction, reading canonical rows in bounded 32-row batches and
applying the same public Memory validators before writing derived state.
PostgreSQL acquires `SHARE ROW EXCLUSIVE` locks on `memory_entries` and
`memory_entry_tags` for the transaction; SQLite obtains its write lock before
scanning canonical rows. Any decode, validation, insert, statistics, or commit
error rolls the rebuild back.

The generic `cmd/server` constructs one `SQLMemoryStore` from the shared
database and injects it into `server.Config.MemoryProjection`. This is
maintenance wiring only: it does not automatically mount or expose a Memory
capability.

Schema versioning: the SQL schema is versioned in `store_meta` (currently v42). Higher versions are inspected and refused before current DDL runs. Historical migrations through v35 retain the original core/session/control-plane records. v36 adds the resource-object migration slot and metadata-only mutation journal; v37 adds experimental Graph checkpoint and append-only transition facts; v38 adds encrypted tenant-scoped notification targets; v39 adds generation-fenced Graph segment leases; v40 adds approval-by-run and stale-run recovery indexes; v41 adds immutable Graph checkpoint-version history; and v42 adds the separate `authorization_epoch` row. The v41 upgrade is one transaction: it backfills only the current v40 head as `migration_floor`, installs the head-write protocol fence, and records the v41 marker together; a v40 head-only writer is rejected after the fence instead of silently splitting history. The v42 epoch is an audit/fencing input for future SQL recovery work, not a grant for arbitrary external or in-memory authorization sources. The detailed historical chain is maintained in the machine-readable public inventory; upgrades from v22 still rebuild Assignment variants from canonical Session events before recording the current version.

## Profile releases

`ReleaseManager` adds publish / version / rollback on top of the live profile registry. Each publish records exactly which layers it mounted, so rollback unmounts precisely those contributions in reverse order (the same reversible primitive plugins use). Complete artifacts survive process restarts. HTTP surface (requires `Config.Releases`):

- `POST /v1/profiles/{id}/publish` — mount one layer at one scope as the next version; an optional `evaluation_gate` runs the candidate against an immutable Dataset before Live state changes. Writes are allowed only at the principal's own scope or below it.
- `GET /v1/profiles/{id}/releases` — release history with rollback status.
- `POST /v1/profiles/{id}/rollback` — unmount everything newer than `to_version` (`0` = pre-release state).

`CanaryManager` stages a passing evaluated Layer in an independent Profile Registry clone and routes a stable fraction of Subjects by SHA-256 bucket (`0..9999`). Paused and rolled-back canaries always use Live. Stage captures a digest of Release history at ancestor/equal/descendant scopes; sibling scopes remain independent. While the Canary is open, overlapping publish/rollback operations are reserved. Promotion checks the same digest before transitioning through `promoting`, publishes with the Canary ID as the Release operation ID, and completes idempotently after retry or restart. A changed baseline returns `409` and leaves an active/paused Canary recoverable by rollback or re-evaluation.

The required `evaluation_gate` also runs Capability compatibility. A platform
administrator may explicitly acknowledge reviewed breaks with
`allow_breaking_capabilities`; tenant operators cannot use this override. The
full result is returned in `gate.capability_compatibility`, persisted in Canary
Gate JSON and durable Audit records, and correlated to the Evaluation Run by
Live/Candidate Capability Snapshot IDs plus a stable compatibility revision.
Rejected Publish and Canary Stage attempts are audited without changing Live.

- `POST /v1/profiles/{id}/canaries` — evaluate and stage a candidate; `basis_points` is `1..10000` and `evaluation_gate` is required.
- `GET /v1/profiles/{id}/canaries` — list visible rollout history.
- `POST /v1/profiles/{id}/canaries/{canaryID}/percentage` — change `basis_points` while active or paused.
- `POST .../pause`, `POST .../resume`, `POST .../rollback`, `POST .../promote` — durable lifecycle transitions owned by the caller's Scope.

Synchronous selected runs include `X-Harness-Canary: <canary-id>`; durable asynchronous workers use the same Tenant + Subject bucketing rule. Selected synchronous, asynchronous, and approval-resumed runs attach `harness.canary.id` to Run, Model, and Tool Spans through the generic Span-only Context mechanism. Queue-claim Spans occur before rollout selection and do not carry it; no Canary ID is added to Metrics.

`ReleaseManager.Sync` and `CanaryManager.Refresh` make shared-SQL deployments converge without restart. New Releases are mounted locally, durable Rollbacks invoke the local unmount handle, new Canary candidates are rebuilt, and pause/resume/percentage/terminal transitions replace local routing state. Server request paths call Refresh before Session creation, Profile reads, synchronous/async execution, Evaluation, Backtest, and Release/Canary mutations. This is request-driven convergence rather than database push notification: an idle instance updates on its next relevant request or Worker claim.

`GET /v1/sessions` lists the caller's session catalog when the store implements `SessionLister` (the SQL store does). SQL stores that also implement `SessionQuerier` accept `prefix`, `profile`, `status`, `sort`, `limit`, and `offset`; `prefix` is a literal ID prefix (`_` and `%` do not act as LIKE wildcards).

The durable `run/start` and `run/resume` events contain the selected
provider-neutral Composition. The core validates event order and bounds
Composition metadata; adapters may record namespaced facts such as Canary ID,
status, basis points, deterministic bucket, candidate revision, and base
Release revision without teaching the kernel about Canary semantics.

Evaluation `CaseResult.Artifacts` carries the latest Composition Revision and
Assignment Revision for the Case Run. `RunRequest.CompositionMetadata` is
durable at the Evaluation Run level, and the SQL store persists it in
`composition_metadata_json` so `/resume` reconstructs the same association.
Backtests expose both their own revisions and the source Run's revisions.
The Evaluation Run list accepts exact `composition_revision` and
`assignment_revision` SHA-256 filters. SQL stores use indexed Case Artifact
columns; the Memory Store implements the same semantics for tests and demos.

The embedded Console's “评测关联” page uses the same query API and displays
per-Case Composition/Assignment Artifact revisions for an individual Run.

`GET /v1/admin/evidence` is the cross-object query surface. It returns bounded
references for ordinary Runs, Backtest Runs, Evaluation Runs, Canary records,
and Releases. Revision filters remain exact SHA-256 values; tenant operators
are restricted to their tenant and visible Scope, while platform operators may
select a tenant. Canonical event, Evaluation, Canary, and Release payloads are
still fetched through their owning APIs.

The optional `kind` parameter accepts repeated or comma-separated values from
`run`, `backtest`, `evaluation`, `canary`, and `release`. The normalized kind
set is part of the cursor fingerprint; changing it invalidates an existing
cursor. SQL skips unselected source queries, while File/S3 return only the
Run/Backtest kinds they actually own.

Every Evidence reference includes a permission-checked canonical `detail_path`:

- Run/Backtest → the matching Run events inside its Session;
- Evaluation → the complete Evaluation Run;
- Canary → the individual Canary record;
- Release → the individual Release version.

The Console detail button follows this path and renders the canonical JSON;
Evidence records therefore remain small references rather than duplicated
object payloads.
The detail panel also renders a type-specific summary: Session/Run events for
Run and Backtest, score/Cases for Evaluation, rollout state and revisions for
Canary, and version/Scope/rollback state for Release. Raw canonical JSON remains
available in a collapsible audit view.

Evidence responses include an opaque `next_cursor`. The cursor is bound to the
full filter set and page size, carries an integrity checksum, and fixes a
created-at snapshot watermark so newer writes do not shift later pages.
Changing a filter/limit, corrupting the cursor, or reusing it across another
query returns `400`. SQL, File, and S3 Evidence stores implement the same
pagination contract; the Console keeps a local cursor stack for previous/next.

`status` also accepts repeated or comma-separated values. Run/Backtest status
is projected from canonical `run/start`, approval, `run/resume`, and `run/end`
events; Evaluation and Canary use their native status, while Release maps to
`active` or `rolled_back`. `created_after` and `created_before` are inclusive
RFC3339 boundaries. Status and time filters are part of the cursor fingerprint.

`GET /v1/admin/evidence/stats` returns Run/Backtest aggregation for the same
revision, tenant, subject, profile, kind, status, and inclusive time filters;
it does not accept a cursor. Each `(session_id, run_id)` contributes only its
latest Composition segment, so an approval-resumed Run is counted once using
the resumed assignment. The response includes totals, terminal/completed/failed
counts, status distribution, and `candidate`, `live`, or `unassigned` Assignment
variant breakdowns. Terminal means `completed`, `failed`, `cancelled`, or
`limited`; `completion_rate` is `completed / terminal` and is zero when no
terminal Run matches. Variant/status are deliberately kept out of OTel metric
labels to avoid high-cardinality telemetry.

## Bringing your own authentication

The kernel never authenticates anything: the transport adapter consumes one injected `Authenticator` interface, and the kernel only sees the resulting `Principal` (subject, tenant, scope path, grants). Deployers with an existing auth system integrate in three shapes:

**1. Gateway / SSO already verifies the user** (the common enterprise case). Lift the verified identity and map it — the only harness-specific code you write is the mapper:

```go
auth := server.AuthChain{
    Source: server.IdentitySourceFunc(func(r *http.Request) (server.Identity, error) {
        // Your gateway already validated the session and set these.
        return server.Identity{
            Subject: r.Header.Get("X-Gateway-User"), Tenant: r.Header.Get("X-Gateway-Org"),
            Roles: strings.Split(r.Header.Get("X-Gateway-Roles"), ","),
            Attributes: map[string]string{"email": r.Header.Get("X-Gateway-Email")},
        }, nil
    }),
    Mapper: server.StaticPrincipalMapper{
        Root:          product.Segments(),                       // scope placement
        DefaultGrants: core.NewPermissionSet(core.PermRead),
        RoleGrants:    map[string][]core.Permission{"trader": {core.PermWrite, core.PermSend}},
    },
}
```

**2. You own everything** (bearer tokens against your IdP, an OIDC library, mTLS): implement the single `Authenticator` method directly and skip the chain.

**3. Authorization is external too** (a policy decision point per request): the `PrincipalMapper` receives the request context, so it can call your authz service while resolving grants; per-action denials belong in `RunHooks.OnBeforeTool`.

Identity attributes flow into `Principal.Attributes` untouched, so capabilities and hooks can read verified claims (email, org) without re-parsing tokens. For local development, `HeaderAuthenticator` composes the same `AuthChain` over dev headers, including an opt-in `AllowGrantsHeader` for simulating grants.

## Model providers

`core.LlmAdapter` is provider-neutral. `pkg/provider/openai` supplies the OpenAI-compatible adapter and bounded retry wrapper; `core.MockLlmAdapter` remains the keyless kernel test adapter. The adapter rejects API keys and model names with CR/LF/NUL before they can split the Authorization header, and the stream parser fails closed on out-of-range tool-call indexes or argument payloads larger than 1 MiB.

```text
# Legacy compatibility import only when the database `llm` row is absent:
HARNESS_LLM_BASE_URL=https://api.openai.com/v1
HARNESS_LLM_API_KEY=...
HARNESS_LLM_MODEL=...
HARNESS_LLM_ALLOWED_MODELS=model-a,model-b
HARNESS_LLM_MAX_TOKENS=4000
```

These variables are a legacy compatibility fallback for `cmd/server`, not the
active source once a database-backed LLM record exists. Configure the current
LLM through `GET`/`PUT /v1/admin/model-settings/llm`: its encrypted database
record owns `base_url`, `api_key`, `model`, and `allowed_models`. The dedicated
GET returns only `has_api_key` and a bounded recognition preview; it never
returns `api_key`. Canonical PUT distinguishes omission (preserve), a non-empty
`api_key` (replace), and `clear_api_key:true` (explicit deletion). A first
canonical save requires a non-empty key; an explicit clear persists an
authoritative inactive record, so an old environment key cannot revive it.

When the `llm` row is wholly absent, `HARNESS_LLM_*` is used for first-time
compatibility. Once the row exists, its normalized `allowed_models` policy is
authoritative; malformed, incomplete, or inactive records fail visibly rather
than falling back to or being overridden by `.env`. Database connection,
listener/address, data directory, and external master-key references remain
bootstrap configuration because they are needed before the encrypted database
settings can be read.

For control-plane setting encryption, set `HARNESS_MODE=production` and provide
`HARNESS_MASTER_KEY` as a random 32-byte base64 or hex key supplied by a KMS,
vault or OS secret store. Production startup fails without it and rejects
`HARNESS_DEV_HEADER_AUTH`. The embedded database-key fallback is available only
in explicit `dev`/`demo` mode and logs a warning; it does not protect a copied
database file. A database without a pre-migration `accounts` table receives a
random pending administrator; its one-time account and password are printed to
the process console only after commit and must be changed immediately. They are
not written to file logs, HTTP responses, or session snapshots.
Local session, object, and log directories are created as 0700 and their
private files as 0600 on Unix-like systems. Windows does not emulate POSIX
mode bits: operators must apply equivalent ACLs; the runtime documents this
as a platform security boundary rather than claiming enforcement it cannot
verify.

## Execution providers

- `execution.WazeroExecutor` runs WASI computation capabilities in-process without ambient filesystem or network access. Module paths cannot contain parent segments or NUL. WASI argv must be scalars (no nested objects), is capped at 64 entries / 8 KiB per argument, modules are capped at 32 MiB, and captured stdout is capped at 1 MiB.
- `execution.OSConfinementExecutor` uses Bubblewrap on Linux and fails closed on unsupported hosts. Command, workdir, and argv are validated the same way before `bwrap` or `exec`; stdout/stderr are capped at 1 MiB and stderr included in errors is truncated.
- `execution.HTTPExecutor` runs capabilities whose `core.ExecutionSpec.Runtime` is `"http"`. Header names and values, including resolved credentials and `Idempotency-Key`, are rejected at execute time if they contain CR, LF, or NUL, so bind-time checks and later secret resolution cannot smuggle a second request line. GET/DELETE query arguments must be scalars; nested objects are refused instead of being `fmt.Sprint`ed into the URL, and the encoded query string is capped at 8 KiB. POST/PUT/PATCH JSON bodies are capped at 1 MiB at execute time, independent of kernel argument limits. One request may carry at most 64 headers; names are capped at 256 bytes and values, including resolved credentials and `Idempotency-Key`, at 8 KiB.
- `core.Executor` remains open for Docker, Kubernetes Jobs, Firecracker, remote runners, and other execution backends.

### MCP (Model Context Protocol)

`execution.RegisterMCPServer` connects to an MCP server over stdio JSON-RPC, performs the initialize handshake, and indexes remote tools as a **tool-library knowledge base**. It mounts **one** capability at `Namespace` (contract `harness.tool.library/v1`, kind `knowledge`) instead of one model-facing tool per remote skill. The model searches or pages qualified ids (`library/name`) and short descriptions, describes a single schema on demand, then calls by id. A bare local name that exists in more than one library returns an `explore` candidate list (id + library + description) instead of executing a guess; the model picks a qualified id. Each search/list/describe/call result carries `library.*` metadata, and an optional `ToolLibraryObserver` records the query → candidate → choose path (bounded ring of 8192) so ranking can be improved from traces without changing the model protocol. Multi-token queries must match description tokens, so a shared name like `send` does not retrieve the email tool for a Slack intent. Empty or duplicate remote names are skipped; argv/env stay bounded (64 entries, 8 KiB) before the process starts; assembled call text is capped at 1 MiB.

Do not execute untrusted tenant or user code inside the shared server process.

## HTTP API contract

[`openapi/harness-core-v1.yaml`](openapi/harness-core-v1.yaml) is the
versioned OpenAPI 3.1 contract for the routes currently registered by
`pkg/server`. It is a SaaS service contract: bearer authentication resolves a
tenant-scoped Principal, session ownership is tenant/subject/scope isolated,
and failures use the uniform `{"error":"..."}` envelope. It covers synchronous
run SSE, durable async runs, approval decisions, private-runner operations,
health/readiness, and administrative endpoints. SSE has no `Last-Event-ID` or
stream replay; clients reconnect through the durable session-event cursor
(`after_seq`) and the run-status API. `v1` accepts additive changes while this
pre-GA API remains subject to reviewed breaking revisions. Optional adapters
are documented as returning `501`; planned routes are intentionally absent.
The Console calls this same `/v1` surface and the verifier rejects both
undocumented Console calls and specification-only routes.

## Verification

```sh
go test ./...
go vet ./...
go run ./scripts/verify-openapi openapi/harness-core-v1.yaml pkg/server pkg/console/static/index.html
go run ./cmd/demo
go run ./cmd/server

# Windows current-user Basic acceptance (ordinary Medium source; D-backed caches).
$env:TEMP='D:\cc\auto_agent\.codex-tmp'
$env:TMP=$env:TEMP
$env:GOTMPDIR=$env:TEMP
$env:GOCACHE='D:\cc\auto_agent\.go-cache'
$env:GOMODCACHE='D:\cc\auto_agent\.go-mod-cache'
go run -tags sandboxacceptance ./cmd/windows-sandbox-basic-unelevated

# Optional live PostgreSQL smoke against local 17.6+ binaries.
# Provide the local PostgreSQL bin directory for your machine; do not hardcode a fixed path.
powershell -ExecutionPolicy Bypass -File ./scripts/postgres-live-smoke.ps1 -PostgresBin "<path-to-postgres-bin>" -PostgresPort 55434
```

The tests cover scope inheritance, explicit replacement, protected capabilities, permission filtering, timeouts, immutable snapshots, layered persona composition, plugin unmount, session round-trip, optimistic persistence conflicts (memory, file, SQL, and S3-shaped stores), multi-turn runs, terminal error events, SSE behavior, cross-user isolation, argument validation, guardrail hooks, fail-closed and durable approval, approval expiry/cancellation/tenant isolation, same-Run resume, nested workflow continuation without repeated side effects, per-capability budgets, policy intersection, run-scoped one-shot capabilities, context compaction, durable context summaries with range shadowing, crash-tail repair, write-behind batching, multi-tool-call parsing and execution, usage metering, streamed chunk persistence, catalog listing, synchronous and asynchronous run cancellation, durable queue generation fencing and recovery, graceful worker draining, duplicate-run reconciliation, atomic async-submit replay/conflict/terminal-state idempotency, tool side-effect replay/conflict/unknown-outcome fencing, downstream idempotency-key propagation, durable Runner capability authorization, metadata-only task catalog isolation, safe unkeyed-terminal retention, runtime RAG projection statistics/rebuild authorization and audit redaction, immutable Evaluation datasets, safe capability filtering, deterministic assertions/custom evaluators, incremental SQL results, interruption recovery without model replay, Tenant isolation and regression gates, SDK-neutral telemetry panic isolation, in-memory OTel Span/Metric export, real OTLP/HTTP export, TraceContext extraction, grants-header gating, and the SigV4 client against a fake S3 server.

## Current boundary

The repository provides a stable vertical slice of the foundation. The next infrastructure adapters should be added without enlarging the kernel interfaces unnecessarily:

- gRPC provider protocol (HTTP, stdio MCP client/tool-library integration, and private runners are in; an MCP Server is not)
- SDK and Studio follow-up: the embedded console and HTTP/MCP/Runner boundaries are current; TypeScript/Python SDKs and a separate Agent Studio are roadmap items, not implemented public packages
- external durable memory/RAG scale-out beyond the shared schema
- PostgreSQL scale-out follow-up: tenant partitioning, backup/restore guidance, and an optional JSONB projection column for trace queries (the TEXT payload stays canonical); PostgreSQL 16 schema, migration, queue, approval, journal, and idempotency behavior already run in CI
- optional PostgreSQL notification/push invalidation and background warming on top of the request-time control revision, plus capability-evolution compatibility gates
- telemetry follow-up: exemplars, collector dashboards, and deployment-specific sampling policies

Each new public abstraction should have a current consumer, a default or example provider, and behavior tests.
