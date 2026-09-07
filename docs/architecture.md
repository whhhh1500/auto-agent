# Architecture

Harness Core uses a one-way dependency graph. The kernel is deliberately
unaware of transport, databases, model vendors and optional product features.

For the detailed implementation and extension audit, see the
[44-module Agent assessment](agent-module-assessment.md), including a complete
74-package coverage index. The companion
[framework comparison](agent-framework-comparison.md) explains scenario-based
tradeoffs, and the [2026-09-06 benchmarks](performance/2026-09-06-module-benchmarks.md)
separate measured local costs from historical integration evidence and unmeasured claims.
The [serial live-model acceptance](verification/2026-09-06-serial-live-agent-acceptance.md)
records actual Gemini requests, failures and fixes, and the HTTP history / SQL /
OpenTelemetry audit standard with sanitized evidence.
The follow-up [context budget optimization](performance/2026-09-06-context-budget-optimization.md)
includes final tool declarations in the budget, exposes an application-layer
`ContextEstimator`, and measures allocation/time and live input-token changes.

```text
                         +------------------+
                         |    cmd/server    |
                         +--------+---------+
                                  |
                 +----------------+----------------+
                 |                                 |
          +------v------+                  +-------v-------+
          | pkg/server  |                  | examples/*    |
          +------+------+                  +-------+-------+
                 |                                 |
       +---------+---------+---------+-------------+
       |                   |         |
+------v------+   +--------v---+  +--v----------------+
| pkg/control |   | pkg/storage|  | providers /       |
+------+------+   +--------+---+  | execution /       |
       |                   |      | extensions        |
       +-------------------+------+---------+---------+
                                          |
                                    +-----v----+
                                    | pkg/core |
                                    +----------+
```

## Kernel boundary

`pkg/core` owns only stable runtime concepts: scope, principal, capability and
profile registries, immutable run composition, policy, hooks, the model/tool
loop, session events and persistence/execution interfaces. Its standard-library
dependencies contain no SQL, HTTP client, subprocess, WASM or model-vendor
implementation.

## Outer packages

- `pkg/app/contextassembly` owns bounded context assembly plus deterministic
  extractive/optional LLM summarization policies behind a small core seam.
  Each model step obtains one final tool-schema snapshot before assembly.
  The assembler budgets its private copy as required input; it cannot rewrite
  tool authority. Hosts can supply one estimator for messages, fragments and
  tool schemas without adding tokenizer dependencies to the kernel.
  Its adapter normalizes matching legacy tool-call aliases and omits completed
  workflow child audit results from model input, while durable events retain
  every protected step. Conflicting aliases or missing outer results fail closed.
  Rolling summaries close the replacement range over original provenance and
  prior summary events before writing it, preserving an unshadowed tail and the
  append-only source log. The default extractive policy uses no additional model
  calls; [three-cycle live verification](performance/2026-09-06-rolling-summary.md)
  covers selected facts and service reconstruction, with explicit finite-budget
  and optional LLM-summary accounting limits.
  The follow-up [LLM summary accounting audit](performance/2026-09-06-llm-summary-accounting.md)
  adds an app-level metered policy, current run identity and separately tagged
  summary model spans. The current ledger writes one bounded, unique
  `summary:<range-start>:<range-end>:<pre-call-session-version>` usage event for
  each observed metered summary outcome; successful outcomes with no upstream
  report record zero usage, while failed requests with no report do not pretend
  to be free. Model and summary entries share the Session per-Run total, remain
  outside prompt projection, and are not written per stream chunk. The default
  remains local extraction after measuring total cost.
- `pkg/app/modelcontrol` owns immutable catalog/provider/protocol evidence;
  `pkg/adapter/modelruntime` compiles persisted settings through explicit
  provider and protocol plugins.
- `pkg/execution` implements remote and isolated capability execution; its
  Graph executor is experimental and selected only by an explicit orchestrator
  choice.
- `pkg/execution/sandbox` defines fail-closed sandbox contracts. Linux uses
  `bwrap`/`prlimit` when available. Windows supports current-user Basic:
  an ordinary Medium source creates a restricted child in a server-owned
  workspace, with bounded output and Job Object process-tree cleanup.
  Windows admits one active session per process and reports `NetworkHost`,
  `NetworkIsolation=false`; strict network-isolation requests and elevated
  source processes are rejected. Neither provider falls back to host execution.
- `pkg/extensions/*` contains optional capability consumers.
- `pkg/storage` implements persistence and SQL-backed operational stores.
- `pkg/control` owns control-plane orchestration such as restorable profile releases, evaluation-gated durable canaries, and revision-driven shared-store synchronization.
- `pkg/evaluation` owns immutable datasets, isolated Case execution,
  deterministic/custom evaluators, durable results, recovery, regression
  gates, and provider-neutral Capability declaration compatibility checks.
- `pkg/server` adapts these components to HTTP/SSE and administration routes.
- `pkg/telemetry/otel` adapts the kernel telemetry seam to OpenTelemetry; the
  dependency direction remains outward and the kernel imports no OTel SDK.

High-cardinality rollout correlation uses bounded Span-only Context attributes
in `pkg/core`. They inherit through Run/Model/Tool Span creation but are never
implicitly copied to Metrics or durable W3C Baggage.

The server command is a reference composition, not a product workflow. It
installs the built-in `general` profile, with scoped memory recall/remember/
approval-gated forget, RAG search, and approval-gated notification capabilities.
It builds each durable memory/RAG store once and shares those instances with
projection maintenance. Provider/protocol/model, notification target, and object
storage selection are database/Console state. `general` uses sequential
execution by default; a Graph executor is present only for explicit profile
selection. Its current server-facing adapter wraps one core `RunTurn` node,
not an arbitrary multi-node business graph. The crypto example remains
confined to `examples/crypto` and
`cmd/demo`.

The independent [graph-review example](../examples/graph-review/README.md)
assembles three nodes with SQL checkpoints and segment leases. Its approval
authority is injected by the embedding application. It does not widen the
generic server registration or add business concepts to the kernel.

## Support matrix

All public APIs remain pre-GA; "implemented" is not a stable compatibility promise.

| Surface | Current boundary | Verification entry |
| --- | --- | --- |
| Core sequential runtime, HTTP/JSON and SSE | Implemented; runtime contracts remain provider-neutral | `go test ./pkg/core ./pkg/server ./pkg/integration` and OpenAPI gate |
| SQLite and PostgreSQL stores/adapters | Implemented; PostgreSQL requires an explicit test database | `go run ./scripts/test-postgres` selects every `TestPostgres` test and rejects skips |
| Default server Graph executor | Experimental; one registered `core-turn` node | `pkg/adapter/runexecutor/graph` tests |
| Multi-node Graph engine | Experimental trusted Go assembly; SQL example with injected reviews | `go test ./examples/graph-review` |
| Windows local sandbox | Current-user Basic only; Medium source, restricted child, Job cleanup, one active session per process, Host networking | [Native Basic acceptance](verification/2026-09-06-windows-basic-acceptance.md) |
| Linux local sandbox | Existing `bwrap`/`prlimit` provider; required confinement dependencies must be available | Native Linux tests; not validated by a Windows test pass |
| E2B sandbox | No bundled provider, E2B API client, or E2B-compatible server endpoint | A separate adapter can implement `sandbox.Provider` / `Session` and register exact provider/version metadata |
| MCP | Outbound stdio tool integration | No inbound MCP server endpoint |
| WASM execution | Per-call isolated runtime, default 128 MiB guest linear memory / 30 s timeout, context cancellation enabled; optional caller-owned compilation cache | [Real WASI, resource and cache acceptance](performance/2026-09-06-wasm-resource-and-cache.md); limits do not cap total host RSS or hard-preempt arbitrary file reads / compilation |
| Default model context budget | Reserves final tool declarations before selecting history; System, messages and tools share an application-layer ContextEstimator | [Context budget boundary](agent-module-assessment.md#m14); conservative estimates are not an exact provider tokenizer |
| Optional tool disclosure | Restores up to 8 recent tool schemas from projected history and the current authorized snapshot; host-owned library dispatch is preserved | [4/24-tool real conversation comparison](performance/2026-09-06-tool-disclosure.md); extra discovery calls can increase total tokens and latency; `toollib.SetSearcher` does not replace the core searcher |
| Queued worker process-crash recovery | Before a journaled tool begins, the server durably checkpoints the assistant/tool-call prefix; an unknown non-idempotent outcome is repaired as `run_interrupted` without provider replay | [Hard-kill PostgreSQL acceptance](verification/2026-09-06-assessment-closure.md#worker-process-crash-recovery); recovery is fail-closed, including a journal row already marked completed |

An E2B adapter must map remote lifetime, command execution, artifacts and cleanup
to the existing sandbox contract and report actual assurance. Host networking
must never be advertised as isolated networking. Provider credentials belong
to the adapter configuration, outside the core contract.

## Runtime invariants

- Composite capabilities invoke nested tools through `core.ProtectedToolInvoker`;
  raw provider-to-provider calls are not part of the supported run path.
- Cross-instance session leases use a stable instance UUID, Run ID, queue
  generation (zero for synchronous runs), and a fresh per-acquisition nonce.
  They renew at `TTL/3` and cancel the run when ownership is lost. Renew and
  release use the exact acquired holder, so a delayed operation from an old
  acquisition cannot act on a later same-instance/same-Run acquisition. The
  in-process session mutex is sufficient only for a single server instance;
  synchronous multi-instance deployments sharing a SessionStore must configure
  a Leaser.
- Session loads are pure reads across Memory, File, SQL, and S3 stores. Crash
  repair is explicit: the execution owner calls `storage.RepairInterruptedSession`
  under its session lock/lease, and the helper persists the existing synthetic
  repair suffix with optimistic version CAS. Read-only history requests cannot
  close an active run, and unresolved approval checkpoints are not repaired.
- Durable run control stores queued/running/waiting-approval/terminal state and cancellation
  requests. Queue claims renew independently from Session leases, use the
  claim generation as a fencing token, and are recovered while the service stays
  online; an expired worker cannot finish a successor's claim. A configured
  durable RunQueue requires a SessionLeaser, `FencedSessionAppender`, and the
  sealed storage SQL fence-domain capability on Sessions, RunQueue, and
  SessionLeaser. RunControl is either omitted (and supplied by RunQueue) or the
  exact same queue object, so synchronous status/cancellation and worker claims
  cannot split across durable control planes. Native SQL components and transparent embedding wrappers retain
  that capability only when they share one `*sql.DB` handle and dialect; an
  independently implemented adapter cannot manufacture it. This is deliberately
  a same-pool provenance check, not a proof of arbitrary adapter behavior. Core
  SQL queries use unqualified tables, so PostgreSQL deployments must keep one
  stable shared schema/search path for that pool; per-connection search-path
  changes or schema multiplexing are unsupported. The worker acquires its
  generation-and-nonce-bound Session
  lease before its first Load, runs explicit repair through the SQL fence, then
  creates one fenced `WriteBehind` for executor resolution, normal/resume
  execution, tool checkpointing, background persistence, and terminal flush.
  Each non-empty append verifies current queue worker/generation/expiry,
  cancellation, Session lease holder/expiry, durable tenant/subject ownership,
  and Session version in the same SQL transaction. `ErrSessionWriteFenceLost`
  cancels and aborts the stale worker; it never becomes `store_error` and the
  stale worker does not append terminal events or mutate its old claim.
  Memory/File/S3 Session stores and separate queue/session SQL handles are
  rejected for queued execution rather than receiving a best-effort fallback.
- Durable approvals write an `approval/requested` continuation checkpoint,
  transition the Run to `waiting_approval`, and release every execution lease.
  Decisions atomically return the same Run ID to the queue; resume re-enters
  current policy and the protected Tool/Idempotency funnel.
- A normal execution segment records its resolved Composition on `run/start`;
  an approval resume records a new `run/resume` Composition. Segment metadata
  is bounded and adapter-defined, allowing rollout assignment evidence without
  coupling the kernel to a specific control plane.
- `AgentProfileRegistry.ReplaceExact` is a narrow single-layer publication
  primitive: after normal layer validation, it requires exactly one canonical
  old-layer match at the same profile and scope, validates the detached final
  projection, and swaps the layer under one registry write lock without
  changing its mount order. Existing `Mount` unmount closures therefore remove
  the replacement slot. It is not an atomic multi-layer Release/Canary
  projection swap, control-plane transaction, or authorization fence.
- Evaluation Case Artifacts and Backtest responses use the same stable
  Composition/Assignment revisions, so replay, regression evidence, and
  rollout audit can be correlated without copying arbitrary provider state.
- Evaluation Run artifact revisions have optional SQL indexes and an optional
  `QueryStore` adapter surface; the minimal Evaluation Store contract remains
  unchanged for lightweight integrations.
- The SQL SessionStore maintains a `run_evidence` projection for
  Composition/Assignment lookups across ordinary Runs and Backtests. The
  projection is rebuilt from canonical Session chunks during migration and is
  written atomically with new event chunks.
- Evidence pagination uses an opaque, query-bound cursor containing per-source
  offsets and a created-at snapshot watermark. Every page re-applies tenant and
  Scope authorization; the cursor is not an authorization token.
- Evidence references expose only permission-checked canonical detail paths;
  full Run, Evaluation, Canary, and Release payloads remain owned by their
  respective APIs.
- Evidence kind filters are normalized into the cursor fingerprint, and SQL
  omits unselected source queries rather than filtering a full union in the
  transport layer.
- Evidence time/status filters share the same cursor binding. Run status is a
  derived projection from canonical Session lifecycle events; Release status
  is normalized to `active` / `rolled_back`.
- Asynchronous submit idempotency is scoped by tenant, subject and Session.
  The hashed client key, request digest, RunControl row and Queue row commit in
  one SQL transaction, so an ambiguous HTTP retry cannot create a second Run.
- PostgreSQL queue selection and expired-claim recovery lock candidates with
  `FOR UPDATE ... SKIP LOCKED`; SQLite uses conditional updates inside its
  local transaction model. Future schema versions are rejected before this
  build executes its current DDL.
- File, SQL and S3 session stores implement `core.SessionAppender`, so live
  write-behind persistence commits only the new event suffix.
- Scope mutations are downward-only: a principal may mutate its own ownership
  scope or descendants, never inherited ancestors or sibling scopes.
- Durable profile replacement has two ordered atomicity boundaries. SQL
  `BindingJournalReplacer.Replace` replaces the old binding and advances the
  authorization epoch in one transaction; after that commit,
  `AgentProfileRegistry.ReplaceExact` replaces the uniquely matching in-memory
  layer at its existing mount order under one registry write lock. `Resolve`
  consequently observes a complete old or new projection, while the original
  unmount handle remains valid for the replacement. A post-commit projection
  failure is binding-specific readiness state: new synchronous Runs and queue
	claims are rejected until a matching profile PUT reconciles that binding, but
	control/admin recovery routes stay available. `RestoreBindings` is a startup
	projection path, not an in-process replacement reconciler. This closes
	profile-layer publication; Release/Canary managers still have their separate
	live-publication boundary. An optional server execution-projection coordinator
	adds a short read lease from runtime/executor composition through the emitted
	`run/start` or `run/resume` event, and a matching write lease for profile
	publication and restore. When configured with a durable authorization-epoch
	reader, it rejects new synchronous runs before Session load and queued workers
	before queue claim if a source/binding fault or desired/applied epoch lag is
	known; active runs, health, and control/admin routes remain outside this
	admission gate. An epoch mismatch is retryable and marks the projection
	unavailable; it does not auto-sync bindings, releases, or canaries. After an
	integration has completely rebuilt its own local projection, it must call
	`Server.MarkExecutionProjectionAppliedEpoch` before new epoch-gated work is
	admitted. The marker is local admission state, not a strict authorization
	proof. The current foundation therefore permits only the built-in Sequential executor in
	epoch-gated admission, because it can release the lease at the durable run
	boundary without holding it across a model call. It is not an authorization
	transaction fence or a replacement for a future detached control-plane
	reconciler. In this mode, native SQL dynamic binding publication is also
	serialized under the write lease: policy, disable, environment-credential and
	built-in dynamic-capability mounts are invisible to new server-owned
	executions until their binding row and epoch commit succeeds. A definite write
	failure rolls the local mount back; an ambiguous response, epoch read failure,
	or non-single-step epoch advance leaves admission faulted rather than guessing
	a rollback. Admin handles are published only after both durable and local
	publication succeed. Static/ephemeral credentials and third-party capability
	runtime factories are rejected because their construction or state cannot be
	proved epoch-bound. This only closes the local mount-before-commit window; it
	does not materialize remote instances, arbitrary registry mutation, or a
	complete binding/release/canary control-plane snapshot.
- Dynamic HTTP execution revalidates DNS at connect time, refuses redirects and
  proxies by default, and requires secret header values to use credential refs.
- Every model adapter emits a bounded `assistant* -> finish` stream. Missing or
  duplicate terminal/usage frames, invalid tool calls and adapter panics fail
  the run instead of producing partial ambiguous state.
- A `run/usage` event is a durable Session ledger contribution. Empty
  `invocation_id` preserves old additive history; new model and summary IDs are
  canonical and unique per Run, so Append and Restore reject duplicate identities
  and usage-total overflow. Agent commits an observed model response's
  `assistant/message` and `run/usage` as one in-memory Session batch before a
  subsequent `tool/call`; callbacks happen after the entire batch, and append
  consumers persist the suffix from their saved durable version. This makes the
  synchronous pre-tool checkpoint observe the complete assistant/usage/call
  prefix without writing one event for every stream chunk or adding usage to the
  next model prompt. It does not make a model request safe to replay if a process
  dies before an assistant/usage outcome is durable: there is no model-invocation
  journal yet.
- Tool providers execute behind panic isolation, JSON argument/result metadata
  boundaries and optional `OutputSchema` validation.
- Tool providers optionally execute behind a durable invocation journal. The
  run/call identity and argument digest are recorded before the provider sees
  the request; completed outcomes replay canonically, while unknown
  non-idempotent outcomes fail closed instead of repeating a side effect.
- The server creates one `WriteBehind` per synchronous Run and, after fenced
  repair, one `NewFencedWriteBehind` per queued Run before resolving its
  executor. `Checkpoint` is the repeatable synchronous operation:
  later `MarkDirty` calls still use background batching and a later checkpoint
  advances durability again while the writer is open. `Flush` is terminal: it
  drains the pending prefix, closes background scheduling, and later `MarkDirty`
  or `Checkpoint` calls neither persist new events nor reopen the writer. A
  shallow copy of that Run's `Runtime` privately wraps
  its journal and checkpoints the already appended assistant/usage/tool-call prefix
  before `BeginToolInvocation`; a failed checkpoint cancels the Run before the
  inner journal begin, tool provider, or second model call. The shared server
  Runtime is not mutated. This cancellation is internal stop control: synchronous
  SSE suppresses the resulting `tool_cancelled` / cancelled `run/end` pair and
  instead emits one structured `store/error` with `code=store_error`,
  `status=failed`, and the original persistence error. Synchronous and queued
  RunControl records settle as `failed/store_error`; the failed checkpoint does
  not leave a partial durable Session history.
- The wrapped Runtime is supplied through `RunExecutor` dependencies, so a
  registered custom executor gets the boundary only while it uses that
  Runtime's guarded tool path inside `RunTurn` / `ResumeTurn`. Direct provider
  execution, or asynchronous use of the Runtime or `emit` after the executor
  method returns, is outside the executor contract and this guarantee.
- FastRouter now constructs the matched call and, when invoked through the
  Agent's guarded Runtime, appends `EvToolCall` before guarded `Execute` reaches
  the journal and any side effect. The public `Dispatch` API remains compatible
  for other `ToolRuntime` implementations. This ordering has an offline unit
  test; it has not received a separate process-crash or live-model acceptance.
- The pre-tool checkpoint adds one durable append interaction per journaled,
  protected tool side effect. Ordinary stream chunks remain write-behind
  batched; they are not synchronously persisted one by one. Current recovery
  also treats a pre-crash `journal_completed` record as `run_interrupted` and
  does not automatically continue the model from its completed result. This is
  a deliberate safety boundary, not completed continuation support; its cost
  still requires a dedicated benchmark. The current live acceptance is one
  serial Gemini sample per fault point, not a performance distribution.
- Runtime and infrastructure paths emit through a bounded `core.Telemetry`
  seam. IDs stay trace-only, operational metric dimensions stay bounded, and
  telemetry failures or panics never affect execution semantics.
- Evaluation uses the general subtractive `TurnInput.CapabilityFilter`; the
  kernel contains no Dataset/Score/Gate concepts. By default only idempotent,
  non-approval tools remain visible. Case IDs deterministically derive their
  Session and Agent Run IDs, so recovery can reuse terminal Sessions and skip
  committed Case rows without replaying model/provider work.
- Durable Queue rows persist only bounded W3C `traceparent`/`tracestate` and
  restore them before execution; Baggage is never persisted.
- Capability and model implementations may expose `ArtifactRevision`; run
  composition stores only a SHA-256 fingerprint of that revision.
- Session restore validates every core event type and payload. Unknown or
  corrupt events are rejected rather than silently disappearing from history.
- Restoring a non-empty Session invalidates its initially empty projection cache.
  Appending a resume or tool result before the first model projection must retain
  the complete stored user/assistant/tool history.
- Tool calls can carry an optional opaque `continuation` string, bounded to
  64 KiB and included in context/request budgets. Protocol adapters interpret
  this state; the kernel only preserves it with the original assistant call.
  The Chat Completions adapter round-trips `extra_content` in a versioned
  envelope, including Gemini thought signatures. Unsupported protocols reject
  this state instead of silently discarding it. It is never a tool argument or
  authorization grant. Existing events without the optional field remain valid;
  previously omitted signatures cannot be reconstructed from old records.
- Capability manifests, snapshots, prompts, model output, tool arguments,
  capability results, token usage and event payloads all have explicit hard
  limits.
- Public snapshot/catalog manifests redact literal execution headers and
  sensitive metadata while the internal execution snapshot stays immutable.

## Integration boundaries and roadmap

The current integration surface is deliberately adapter-oriented: in-process Go
providers, OpenAI-compatible and Anthropic Messages protocol adapters, Wazero
executors, outbound HTTP executors, stdio MCP tool libraries, private Runner
workers, and Linux `bwrap` isolation all re-enter the same validation, policy,
approval, budget, and telemetry seams. Windows local execution intentionally
supports the current-user Basic contract only. It does not include dedicated
accounts, WFP/network isolation, administrator execution or elevation. Its
WRITE_RESTRICTED token retains Everyone/logon-writable exceptions; the fixed
workspace mount contract is not universal host filesystem isolation. Offline
environment hints do not enforce network denial. The native provider requires
no .NET, PowerShell 7 or Go installation at runtime. See the
[Basic acceptance boundary](verification/2026-09-06-windows-basic-acceptance.md).
The public service
boundary is the HTTP/JSON and SSE API plus the versioned private Runner
protocol; this repository does not expose an MCP server endpoint.

gRPC provider transport, language SDKs, and a separate Studio are future
boundaries, not shipped capabilities. Add them only when a real consumer and
behavior tests justify the new adapter. The embedded console can later be
replaced by a standalone Studio without changing the `/v1` API or the injected
`Authenticator` seam.
