# Architecture

Harness Core uses a one-way dependency graph. The kernel is deliberately
unaware of transport, databases, model vendors and optional product features.

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
- `pkg/app/modelcontrol` owns immutable catalog/provider/protocol evidence;
  `pkg/adapter/modelruntime` compiles persisted settings through explicit
  provider and protocol plugins.
- `pkg/execution` implements remote and isolated capability execution; its
  Graph executor is experimental and selected only by an explicit orchestrator
  choice.
- `pkg/execution/sandbox` defines fail-closed sandbox contracts. Its local
  provider is real Linux `bwrap`/`prlimit` execution when available and reports
  unavailable on Windows; it never falls back to ordinary host execution.
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

## Runtime invariants

- Composite capabilities invoke nested tools through `core.ProtectedToolInvoker`;
  raw provider-to-provider calls are not part of the supported run path.
- Cross-instance session leases use a stable instance UUID plus the run ID,
  renew at `TTL/3`, and cancel the run when ownership is lost.
- Durable run control stores queued/running/waiting-approval/terminal state and cancellation
  requests. Queue claims renew independently from Session leases, use the
  claim generation as a fencing token, and are recovered while the service stays
  online; an expired worker cannot finish a successor's claim.
- Durable approvals write an `approval/requested` continuation checkpoint,
  transition the Run to `waiting_approval`, and release every execution lease.
  Decisions atomically return the same Run ID to the queue; resume re-enters
  current policy and the protected Tool/Idempotency funnel.
- A normal execution segment records its resolved Composition on `run/start`;
  an approval resume records a new `run/resume` Composition. Segment metadata
  is bounded and adapter-defined, allowing rollout assignment evidence without
  coupling the kernel to a specific control plane.
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
- Dynamic HTTP execution revalidates DNS at connect time, refuses redirects and
  proxies by default, and requires secret header values to use credential refs.
- Every model adapter emits a bounded `assistant* -> finish` stream. Missing or
  duplicate terminal/usage frames, invalid tool calls and adapter panics fail
  the run instead of producing partial ambiguous state.
- Tool providers execute behind panic isolation, JSON argument/result metadata
  boundaries and optional `OutputSchema` validation.
- Tool providers optionally execute behind a durable invocation journal. The
  run/call identity and argument digest are recorded before the provider sees
  the request; completed outcomes replay canonically, while unknown
  non-idempotent outcomes fail closed instead of repeating a side effect.
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
approval, budget, and telemetry seams. The local sandbox is not available on
Windows and is not silently replaced by host execution. The public service
boundary is the HTTP/JSON and SSE API plus the versioned private Runner
protocol; this repository does not expose an MCP server endpoint.

gRPC provider transport, language SDKs, and a separate Studio are future
boundaries, not shipped capabilities. Add them only when a real consumer and
behavior tests justify the new adapter. The embedded console can later be
replaced by a standalone Studio without changing the `/v1` API or the injected
`Authenticator` seam.
