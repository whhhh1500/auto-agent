# Graph-G1 Durable Runtime Design

Status: review requested  
Date: 2026-09-04  
Scope: `D:\cc\auto_agent\harness-core`

## 1. Decision

Graph is an optional orchestrator, not a replacement for the default sequential
Agent loop. Graph-G1 adds the first durable, runnable Graph vertical while
preserving the accepted Graph-G0 and Context-C0 contracts.

The selected storage model is:

- SQLite is the zero-configuration default checkpoint, version, and transition store.
- PostgreSQL is the production, multi-process checkpoint, version, and transition store.
- Memory is a bounded test/reference adapter and is not described as durable.
- The local filesystem holds large artifacts and context bodies by default.
  After the database-configured artifact migration is verified and activated,
  an S3-compatible backend holds them. SQL checkpoints contain only bounded
  references, digests, revisions and state.
- Redis/Valkey may be added as a cache adapter later, but is not a Graph fact
  source in G1.

Graph does not import or embed LangGraph, Temporal, or another workflow runtime.

## 2. Goals

Graph-G1 must provide one complete execution path with:

1. exact definition and implementation binding;
2. deterministic state reduction and edge selection;
3. durable checkpoint compare-and-swap;
4. an immutable checkpoint-version and durable transition fact committed with
   each checkpoint change;
5. bounded retry, timeout, cancellation and visit/step enforcement;
6. fail-closed crash recovery;
7. approval suspension that releases the current execution segment lease;
8. same-run, new-segment resume;
9. model and tool calls through the existing protected gates;
10. redacted, bounded observability; and
11. a pluggable application-level run-executor seam that keeps sequential Agent
    execution as the default.

The design must keep idle CPU at zero apart from existing server workers. Merely
registering a Graph must not create goroutines, timers or open database
transactions.

## 3. Non-goals

G1 does not include:

- fan-out, join, subgraphs or distributed Graph scheduling;
- dynamic mutation of a running definition;
- a visual Graph editor or LangGraph-compatible API;
- provider/protocol catalog routing, which remains M2;
- cloud or self-hosted sandbox implementations;
- S3 checkpoints or S3-based compare-and-swap;
- Redis as an authoritative state store;
- automatic replay of an interrupted unknown node;
- unbounded event history or raw state in metric labels; or
- conversion of the existing sequential workflow extension into a Graph.

## 4. Package boundaries

```text
pkg/extensions/graph
  accepted G0/C0 contracts
  checkpoint, checkpoint-version, and transition value contracts
  hot checkpoint Store plus optional HistoryStore read port

pkg/execution/graph
  execution engine
  immutable implementation bindings
  node/predicate/reducer runtime ports
  retry, timeout, cancel, approval and recovery state machine
  observer port and core gate bridges

pkg/adapter/sql/graphcheckpoint
  SQLite/PostgreSQL Store implementation

pkg/adapter/memory/graphcheckpoint
  bounded test/reference Store implementation

pkg/app/runexecutor
  application-level executor registry and resolver
  sequential Agent adapter
  Graph adapter
```

Dependency direction is one way:

```text
app/runexecutor -> execution/graph -> extensions/graph
       |                  |
       |                  +-> core gate contracts
       +-> core Agent

adapter/sql/graphcheckpoint -------> extensions/graph
adapter/memory/graphcheckpoint ----> extensions/graph
```

`pkg/extensions/graph` remains standard-library-only. It does not import
`pkg/core`, `pkg/runtime`, SQL drivers, provider adapters or server packages.
The existing modulecheck rule remains in force.

## 5. Definition and implementation binding

The accepted Graph definition revision already binds every semantic definition
field and the exact node-kind, reducer and predicate contract versions. G1 adds
an immutable execution binding with:

- Graph definition revision;
- application composition revision;
- implementation-set revision;
- node kind ID, kind version and implementation revision;
- reducer ID, version and implementation revision;
- predicate ID, version and implementation revision; and
- module ID/version when the implementation is supplied by ModuleHost.

The implementation-set revision is calculated from sorted non-secret binding
metadata. Runtime object addresses, credentials and configuration secrets are
never revision inputs.

Start and resume fail closed if any definition, composition, implementation or
module revision differs from checkpoint evidence. G1 never silently resolves a
newer implementation for an existing run. Host generation is segment-scoped:
it may change only through the explicit suspension or recovery handoff
described below, never through an ordinary node commit.

## 6. Execution identity

A checkpoint key is the tuple:

```text
tenant_id + session_id + run_id
```

The checkpoint additionally records:

- segment ID;
- Graph ID and definition revision;
- composition and implementation-set revisions;
- host generation when ModuleHost owns the implementation;
- current node ID;
- node attempt and stable attempt ID;
- step count and per-node visit counts;
- checkpoint revision;
- canonical bounded state;
- last selected edge evidence;
- pending approval identity, when present;
- execution status; and
- sanitized failure code, when present.

IDs are bounded and validated before persistence. A run ID is stable across an
approval suspension; a segment ID and lease are not.

## 7. Checkpoint head, history, and transition Store

Graph has its own Store port. It does not reuse ModuleHost `CompositionStore`,
SessionStore, Redis, or object storage.

The semantic operations are:

```text
Load(key)
Create(initial checkpoint, initial transition)
CompareAndSwap(expected checkpoint revision, next checkpoint, transition)
ListTransitions(key, bounded page)
```

`Store` deliberately remains this narrow hot-path contract. `HistoryStore` is
an optional read capability, discovered by interface assertion rather than
added to `Store`:

```text
LoadVersion(key, revision) -> one validated CheckpointVersion
ListVersions(key, afterRevision, bounded page) -> CheckpointVersionInfo only
```

`CheckpointVersionInfo` contains deterministic `ID`, `ParentID`, explicit
`CheckpointVersionOrigin`, key, revision, canonical-document SHA-256
`CheckpointHash`, and storage-assigned `CreatedAt`; `CheckpointVersion` adds
the one full checkpoint document. `CheckpointVersionID`, `CheckpointDigest`,
clone helpers, and version validation are public stdlib-only Graph helpers.
`ListVersions` is ascending and exclusive of `afterRevision`, and deliberately
does not materialize checkpoint documents.

`Create` atomically writes the mutable revision-1 head, one immutable version,
and its transition. `CompareAndSwap` atomically replaces exactly one expected
head revision and appends exactly one version and transition fact. A stale
revision returns a typed conflict and changes nothing. The Store defensively
copies values and enforces the same validation at every adapter boundary.

Origins make retained-history lineage unambiguous: `commit` revision 1 has an
empty parent and later revisions reference the same-run deterministic prior
version; `migration_floor` has no parent at any revision; `fork` is revision 1
only and carries a distinct, strict 64-character lowercase-hex external source
version ID. Fork creation remains an application use case, not a Store method.

Transition identity is derived from execution key plus resulting checkpoint
revision. Repeating the same completed commit is idempotent; reusing that
identity with different contents is a conflict.

## 8. SQL persistence

Schema v37 introduced the mutable head and transition evidence; schema v41 adds
immutable checkpoint history:

```text
graph_checkpoints
graph_checkpoint_versions
graph_transitions
```

`graph_checkpoints` contains one mutable CAS-protected current canonical
checkpoint document per execution key. `graph_checkpoint_versions` and
`graph_transitions` are append-only, keyed by execution key and revision.
SQLite and PostgreSQL commit head CAS, version append, and transition append in
one database transaction. Exact replay returns `CommitReplayed` and appends no
second version.

The v40-to-v41 upgrade strictly decodes and validates every current head before
backfilling it as the only retained `migration_floor`. Its empty parent is
intentional even when the head revision exceeds one: earlier versions are not
invented from transitions. Corrupt v40 checkpoint JSON aborts the migration and
does not advance its marker.

The adapter must cover:

- fresh schema creation;
- historical v36 to v37 migration and v40 to v41 history-floor backfill;
- repeated migration;
- future-version refusal;
- stale CAS conflict;
- two independent database handles producing one winner;
- exact commit/version/transition idempotency and mutated replay conflict;
- corrupt, oversized or unknown credential-shaped document refusal.

PostgreSQL tests use disposable schemas and explicitly skip only when
`HARNESS_TEST_PG_DSN` is absent. A skip is reported as unverified, never passed.

G1 adds no Graph-specific environment variables. The application injects the
checkpoint adapter for the already selected primary database dialect: default
SQLite or configured PostgreSQL. Graph definitions, orchestrator selection and
adapter policy are database configuration records. The absence of a Graph
selection keeps the built-in `general` profile on the sequential executor.

## 9. Artifact and context storage

Canonical Graph state remains small and is capped by the accepted G0 limit.
Large model output, files, sandbox output, retrieval documents and long context
content are stored through an object-store port. The checkpoint stores only:

- object key;
- content digest;
- immutable revision;
- media/content type;
- bounded size metadata; and
- ownership/scope metadata.

S3 is therefore off the hot CAS path. A checkpoint commit never depends on
listing a bucket. Artifacts become visible to later nodes only after upload,
digest verification and an explicit checkpoint commit.

Graph does not select or migrate the artifact backend. It consumes the shared
object-store port described by
`2026-09-04-artifact-storage-migration-design.md`. With no remote configuration,
the active backend is `<HARNESS_DATA_DIR>/resources`. S3/R2 endpoint, bucket and
credentials are database configuration. Configuring a remote backend first
copies, catches up and verifies local objects; only then does the shared store
activate it. A Graph checkpoint therefore remains valid across backend changes
because it stores backend-independent keys and digests.

Each node receives only its declared `ContextView`. A ContextPlan contains typed
source references and token estimates, not eagerly copied source bodies.
Graph-G1 consumes a validated ContextPlan through a planner/materializer port;
its acceptance tests may use a bounded fixture. Context-C1 remains a separate
vertical that materializes the plan on demand, preserves stable-prefix
ordering, reserves output tokens first and compacts only eligible history layers.
Required current input, required state and pending approval evidence are never
silently removed by compaction.

## 10. State machine

Checkpoint status is a closed vocabulary:

```text
ready
executing
waiting_approval
completed
failed
cancelled
unknown
```

Legal transitions are:

```text
absent -> ready
ready -> executing
executing -> ready
executing -> waiting_approval
executing -> completed
executing -> failed
ready|executing -> cancelled
executing -> unknown          recovery only
waiting_approval -> ready     explicit approved resume, new segment
waiting_approval -> failed    denied or expired
```

Terminal states cannot transition. Every transition is validated before Store
I/O and validated again by the adapter. The approved-resume `ready` checkpoint
retains the suspended attempt ID and records a resume marker; its following
`ready -> executing` transition reuses that attempt ID rather than creating a
retry. A live retry creates a new attempt ID and increments the attempt count.

## 11. Node execution algorithm

For one segment, the executor performs:

1. validate request identity, definition and immutable bindings;
2. create or load the checkpoint;
3. verify definition, composition, binding and host-generation evidence;
4. verify the segment lease;
5. CAS `ready -> executing` with a stable attempt ID;
6. build the node-local ContextPlan;
7. execute the exact bound node with its configured timeout;
8. re-verify the active segment/generation lease before interpreting the node
   result;
9. reduce a successful StatePatch;
10. evaluate conditional predicates against the reduced state;
11. select one edge or fail closed;
12. re-verify the lease immediately before every checkpoint mutation, then
    atomically commit the head, version, and transition; and
13. repeat until terminal, cancelled, suspended or failed.

No database transaction remains open while node code, a model, a tool, object
storage or an observer is running.

If either post-node or pre-commit lease validation fails, the stale executor
returns the lease error and leaves the last authorized `executing` checkpoint;
it must not write a synthetic terminal failure. Transaction-local SQL lease
fencing remains a later optional adapter capability because the generic Store
does not carry lease-holder identity.

## 12. Edge semantics

After successful node execution:

- evaluate every conditional edge exactly once against the same reduced state;
- if exactly one is true, select it;
- if more than one is true, fail with an ambiguity error;
- if none is true, select the default edge;
- if none is true and no default exists, fail closed; and
- a predicate error fails edge selection and never falls through to default.

After final node failure:

- select the single error edge when declared;
- otherwise mark the Graph failed; and
- never evaluate success predicates against a failed result.

The accepted global step limit and per-node visit limit are checked before
entering a node. Error-edge cycles count exactly like success-edge cycles.

## 13. Retry, timeout and cancellation

`RetryPolicy.MaxAttempts` means total live-process attempts. Zero means one
attempt. Each attempt has a stable ID and its own timeout context. Backoff is
bounded and cancellation-aware.

Retries occur only when:

- the node returned a retryable error to the live executor;
- the execution context is not cancelled;
- the node timeout and Graph limits permit another attempt; and
- no approval or unknown external outcome was observed.

A timeout is a node failure and may follow the declared error edge after retry
exhaustion. Caller cancellation commits `cancelled` when checkpoint ownership is
still current. Cancellation never reports success merely because node code
ignored its context.

## 14. Crash and unknown outcomes

The executor commits `executing` before invoking a node. On restart, a loaded
`executing` checkpoint represents an unknown node outcome. G1 changes it to
`unknown` with a recovery transition and stops.

G1 does not automatically repeat unknown model calls, tool calls, messages or
arbitrary plugin code. A later recovery plugin may reconcile a node using its
stable attempt ID and external journal evidence, but absence of that evidence
remains fail closed.

Tool implementations that use the existing ToolInvocationJournal may return an
existing result for the same call ID, but Graph does not infer that result on
its own.

## 15. Approval and segment leases

An approval-pending result is not a normal retryable error. The executor:

1. validates the approval identity;
2. commits `waiting_approval` with current node and attempt evidence;
3. releases the current segment lease; and
4. returns a typed suspended result without holding a worker.

An approved resume uses the same run ID, a new segment ID and a newly validated
lease. It verifies the pending approval and all frozen revisions, then commits
`waiting_approval -> ready`. Completed nodes are not repeated. The pending node
is invoked with the same stable call/attempt identity so the protected approval
and tool journal paths can resolve it.

The newly validated segment lease may carry a newer HostGeneration. The resume
handoff atomically replaces the suspended segment ID and generation in the
checkpoint. Ordinary node commits must exactly match the checkpoint's current
segment and generation. Recovery from an abandoned `executing` checkpoint is
allowed only to an authorized owner with a strictly newer generation, and that
owner may commit only `unknown`, not resume execution.

Denied or expired approval commits `failed`. A stale segment or lease cannot
resume or mutate a newer checkpoint.

## 16. Protected model and tool calls

`pkg/core` adds one narrow public helper that validates a `ModelCallRequest`,
invokes `ModelCallGate` safely and returns a sealed `AcceptedModelCall`.
`ModelCallRequest` also gains one bounded, non-secret `ExecutionMetadata` map;
the existing claim-count, key/value and total-byte limits apply. Existing Agent
model calls are migrated to the helper so there is one signing path. Graph code
cannot mint the proof directly.

Graph model-call identity binds run, segment, Graph revision, composition and
implementation-set revisions, node ID, attempt ID, provider, model, deadline
and budget through the validated request. The sealed proof clones and compares
`ExecutionMetadata` exactly. Prompt text, state bodies and credentials are not
identity metadata.

Graph tool nodes receive only the existing protected tool invoker. They do not
receive a raw capability provider or construct an `AcceptedInvocation`.
Approval, policy, budget, ToolInvocationJournal and sandbox checks therefore
remain on the existing guarded path.

## 17. Observability

The durable transition log is the Graph fact source. An optional Observer gets
a copy after a successful commit for traces, metrics or logs; observer failure
cannot roll back an already committed checkpoint.

Events cover:

- Graph start/resume/end;
- node start/end/error;
- retry and timeout;
- edge selection and ambiguity;
- checkpoint revision/conflict;
- approval suspension/resolution;
- cancellation; and
- recovery to unknown.

Events contain bounded IDs, revisions, counts, duration, outcome codes,
ContextPlan revision and token estimates. Compaction is exposed only as an
allowlisted reason code or digest, never the free-form reason text. Events never
contain raw state, prompt bodies, credentials, tool arguments, artifact bodies
or approval payloads.

Metric labels use only bounded low-cardinality dimensions such as outcome,
edge kind and node kind. Run/session/segment IDs are trace fields, not metric
labels. Graph-specific facts remain in the Graph transition store; Session may
reuse existing generic run/step events but G1 does not add dozens of Graph
event constants to `pkg/core`.

## 18. Run-executor plug-in seam

The application layer resolves a `RunExecutor` by a stable orchestrator ID.
The built-in default is `sequential`; `graph` is an optional registration.
Server and worker code call the resolved interface and do not contain
Graph-specific branches.

The registry stores immutable metadata and factories, is bounded, and fails on
duplicates or version mismatches. Product-specific profile/database resolution
stays in application adapters rather than in `pkg/core` or the Graph engine.
Executor internals remain private; only the request/result, registry and
factory seams required by third-party adapters are exported.

If no orchestrator is configured, the existing sequential executor is chosen.
This preserves the low-understanding-cost default and backward compatibility.

## 19. Sandbox boundary

Graph-G1 can run non-sandbox nodes without Sandbox-S0. A node declaring
`UsesSandbox` cannot run unless an injected sandbox provider proves requested
assurance, limits, network and mount policy.

Windows currently must not advertise process confinement that it cannot
enforce. Unavailable or weaker assurance fails closed. Future local, container,
microVM, cloud and self-hosted providers implement the same sandbox port
outside the Graph contract and engine.

## 20. Performance constraints

The implementation must satisfy these structural constraints:

- no goroutine, timer or DB connection per registered Graph;
- no global scan of checkpoints or transitions on a run hot path;
- one checkpoint row load per segment start, followed by revision-scoped CAS;
- no SQL transaction held during external work;
- bounded registries, state, transitions, retry attempts and event pages;
- a bounded memory reference Store that fails closed when full;
- one parse/canonicalization pass for each changed state value;
- immutable definitions and bindings reused across runs;
- ContextPlan stores references rather than duplicated bodies;
- large data goes to the local artifact store by default and to S3-compatible
  storage only after a verified database-driven migration;
- observer and metrics data contain no high-cardinality metric labels; and
- background recovery, when later enabled, is one bounded service loop rather
  than one goroutine per run.

Memory and CPU benchmarks report both service RSS and sandbox-child RSS. No
429/admission gate is introduced by this design.

## 21. Verification gate

Acceptance requires:

1. definition/binding revision mismatch tests;
2. reducer and edge determinism tests;
3. multiple-true conditional ambiguity tests;
4. success-, error-edge and visit-limit tests;
5. retry, timeout and cancellation tests;
6. stale checkpoint CAS and two-handle one-winner tests;
7. crash-at-executing to unknown tests;
8. approval suspension, lease release and new-segment resume tests;
9. completed-node non-replay tests;
10. model gate and protected tool negative tests;
11. transition/event redaction and cardinality tests;
12. SQLite fresh/historical/repeat/future migration tests;
13. PostgreSQL disposable-schema migration and concurrency tests;
14. package dependency and public API inventory checks;
15. focused repeated and race tests;
16. full `go test ./...`, `go vet ./...`, gofmt and module smoke; and
17. a benchmark showing no idle Graph goroutines and bounded allocation growth
    across increasing state/context sizes.

## 22. Rollout and rollback

Implementation proceeds as one acceptance boundary but in reviewable slices:

1. value contracts and Store port;
2. bounded memory reference adapter;
3. executor state machine and transition facts;
4. retry/timeout/cancel and crash recovery;
5. approval and segment lease handling;
6. protected model/tool bridges;
7. SQLite/PostgreSQL schema v37 adapter;
8. application `RunExecutor` registry and default sequential adapter; and
9. integrated observability and performance verification.

Until the complete gate passes, Graph execution is not enabled by default.
Rollback disables new Graph assignments, fences and drains Graph modules,
preserves checkpoints/transitions for reconciliation and leaves the sequential
executor as the default. Rollback never deletes checkpoint, transition or
artifact data.

## 23. Rejected alternatives

### Pure-memory Graph runtime

Useful for tests, but it cannot support restart recovery, multi-process CAS or
durable approvals and therefore cannot be the production vertical.

### Reusing ModuleHost CompositionStore

Rejected because host composition state and per-run Graph execution state have
different ownership, lifecycle and migration semantics.

### S3 as the checkpoint CAS store

Rejected for the hot path because object-store conditional semantics and
latency do not provide the same portable transactional checkpoint-plus-event
commit as SQLite/PostgreSQL.

### Redis as the only fact source

Rejected because durability and recovery behavior would depend on deployment
configuration and would weaken the default fail-closed contract.

### Embedding Graph in core.Agent or workflow

Rejected because it strongly couples optional orchestration to the kernel or
the existing sequential workflow and forces server-specific branches.

### Importing LangGraph or Temporal

Rejected because it adds a heavyweight runtime and changes the project's
resource, deployment and extension model. Compatibility adapters may be built
later without making either runtime foundational.
