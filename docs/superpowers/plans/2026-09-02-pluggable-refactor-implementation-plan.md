# Pluggable Architecture Refactor Implementation Plan

Date: 2026-09-02  
Baseline:

- `docs/superpowers/specs/2026-09-02-pluggable-architecture-refactor-design.md`
- `docs/superpowers/specs/2026-09-02-agent-foundation-plugin-architecture-v2.md`
- `docs/superpowers/inventory/2026-09-02-ai-agents-book-gap-matrix.md`

The original refactor design remains the compatibility and package-convergence
baseline. V2 adds the approved Agent/Environment/Harness, module-lifecycle,
and model-control direction; the gap matrix is the evidence-backed scope
checklist. None of these documents replaces the public API, SQL, HTTP, or
runtime-invariant gates below.

## Execution contract

This is an incremental, behavior-preserving migration of the existing
domain-modular monolith. The current session remains the orchestrator: it
assigns bounded work, reconciles changes, runs the gates, and performs the
architecture and security review. A delegated task must identify its touched
files, public compatibility impact, tests, and rollback point before it is
accepted.

Only the approved worker models are used. Use `gpt-5.6-terra` for high-risk
architecture, SQL/migration, security-invariant, compatibility, and final
review work. Use `gpt-5.6-luna` for bounded implementation, test, inventory,
documentation, and mechanical adapter work. Select thinking strength from the
task risk: higher for cross-package or data-preserving changes, lower for
isolated mechanical edits. The orchestrator must record the selected model and
reasoning strength in the task handoff; no worker may silently switch models.

No phase may begin until its predecessor's verification gate is green. A phase
may be split into independently reviewable commits or worktree changes, but
the orchestrator integrates only after a clean full test and dependency check.

## Phase status at this snapshot

| Phase 0 deliverable | Status | Evidence or remaining work |
| --- | --- | --- |
| Current public package/symbol/constructor inventory | Done (baseline) | `docs/superpowers/inventory/2026-09-02-public-api-inventory.{md,json}`; main-session workspace scan found no external `harness-core` imports |
| `/v1` route and OpenAPI operation inventory | Done (baseline) | 95 `/v1` operations verified by `scripts/verify-openapi`; preserve resource-key normalization |
| SSE event-name inventory | Done (baseline) | 16 durable Session event names plus `store/error` and `control/error` recorded |
| SQL schema v1-v37 inventory | Done (baseline plus additive v36/v37) | Current version 37 and historical chain recorded; `docs/superpowers/sql-migration-verification-matrix.md` distinguishes verified SQLite paths from PostgreSQL environment skips |
| External compile compatibility fixture | Done (baseline) | `internal/modulecheck/external_compatibility_test.go`; current verifiable external-consumer fixture is `modulecheck_test`, not a production dependency |
| Module dependency guardrails | Done (baseline) | `internal/modulecheck/module_test.go`; only current foundational edges are locked |
| Public API inventory review with downstream consumers | Done (baseline) | Main-session workspace search found no external `harness-core` import; external compile fixture covers the current verifiable consumer contract |
| Critical HTTP/SSE/approval/queue/lease/journal black-box baseline | Done (baseline) | `docs/superpowers/inventory/2026-09-02-runtime-invariant-baseline.md` maps focused public-path and durable-adapter regression evidence |
| SQLite fresh/historical and PostgreSQL fixture matrix | SQLite done; PostgreSQL v35 ownership/Fence verification green, other historical gaps remain explicit | `docs/superpowers/sql-migration-verification-matrix.md`; SQLite fixtures passed. PostgreSQL v35 migration, FenceJournal, and host-ownership tests passed against isolated disposable schemas on 2026-09-04. Other PostgreSQL historical fixtures still retain their explicit environment-skip boundary |

Therefore **Phase 0 is complete for repository-controlled baseline work**.
PostgreSQL v35 migration, host ownership, and Fence verification passed against
isolated disposable schemas on 2026-09-04. Remaining historical PostgreSQL
fixture gaps, and any future run without an administrator-capable test DSN,
remain explicit skips; neither may be inferred from a locally running service.
The completed baseline rows permit Phase 1 to proceed while preserving those
external verification obligations.

### Completed slice ledger (2026-09-02)

This ledger records accepted bounded work so later phases can build on it
without claiming that the original plan is complete or deleting its tasks.

| Slice | Status | Compatibility boundary retained |
| --- | --- | --- |
| Identity/account activation and account/tenant administration | accepted reference slices | legacy storage/server composition remains supported |
| HTTP transport codec and server responsibility splits | accepted incremental extraction | route behavior, coarse authorization ordering, decoder/error envelope remain stable |
| Settings application service and encrypted SQL settings adapter/startup bootstrap | accepted incremental extraction | Config Accounts fallback and legacy generic settings writes remain compatible |
| SQL dialect/sqlkit, fail-closed dialect validation, and sqlstore mechanical file separation | accepted infrastructure prerequisite | schema v1-v37, query strings, constructors, and storage facade remain |
| Local Console per-domain asset separation and stale-secret reload regression | accepted incremental extraction | embedded/local asset model and current route wire remain |
| V2 architecture and book gap documentation | accepted planning baseline | this plan remains the execution record |
| M0c LLM configuration DB vertical | accepted / green (2026-09-03) | DB-present model configuration is authoritative; lazy env fallback/import, source evidence, presence-aware max-tokens and secret semantics are covered by the accepted evidence below |
| M0d storage configuration DB vertical | accepted / green (2026-09-03) | DB-present resources/sessions are authoritative; CAS, apply rollback, restart-pending state, typed HTTP/OpenAPI/Console and legacy fallback are covered by the accepted evidence below |
| M1a Module Host contracts, validators and snapshot | accepted / green (2026-09-03) | runtime contracts, five independent semantic validators and immutable snapshot construction are accepted; lifecycle follow-on slices are recorded as M1b-M1d4 below |
| M1 core public API/size budget | accepted / green (2026-09-04) | `internal/modulecheck` freezes `pkg/core` at 8,263 non-blank production lines / 33 files and 894 AST-counted public API items, with hard ceilings 8,500 / 36 / 900; only six compatibility-fix slots remain, this is an architecture-growth guard rather than automatic compatibility proof, and new domain capability must move to runtime/extensions/adapters |
| M1 core.Plugin compatibility facade | accepted / green (2026-09-04) | bounded process-local, non-recoverable `core.Plugin` facade requires declared runtime provides, permits no durable effects, and never replays legacy `Install`; full legacy-plugin migration/removal remains later work |
| M1 Provider/Protocol registration metadata seams | accepted / green (2026-09-03) | independent typed provider/protocol registrations emit validated runtime collection metadata only; M2 catalog/router/auth/model compatibility and transport remain out of scope |
| M1b Module Host lifecycle/effect ownership | accepted / green (2026-09-03) | lifecycle, lease identity, drain/fence/deactivate, effect ownership and retained-lineage seams are covered by the accepted M1 evidence below |
| M1c SQL EffectJournal schema v33 | accepted / green (2026-09-03) | runtime effect sequence/effect tables, ordered durable inverse evidence, idempotency and SQLite/PostgreSQL adapter paths are covered by the accepted M1 evidence below |
| M1d1 durable runtime state and memory store | accepted / green (2026-09-03) | validated DurableHostState, composition status/revision invariants, clone semantics and strict in-memory CAS are covered by the accepted M1 evidence below |
| M1d2 SQL CompositionStore schema v34 | accepted / green (2026-09-03) | one-document host-state CAS table, fresh/historical migration and SQL adapter CAS semantics are covered by the accepted M1 evidence below |
| M1d3 durable apply/promote/deactivate | accepted / green (2026-09-03) | prepared/active/draining/retained checkpoints, rollback boundaries, effect refresh and final lineage deactivation are covered by the accepted M1 evidence below |
| M1d4 startup recovery and SQLite fresh-handle integration | accepted / green (2026-09-03) | recovery never substitutes Activate, loses old leases, validates journal evidence, handles orphan/prepared/ancestor lineage, and crosses a fresh SQLite handle as covered below |
| M1 ownership/Fence/lifecycle bounds | accepted / green (2026-09-04) | v35 SQL host ownership and Fence journal rows, 30s default ownership TTL, ReleaseOwnership/Close ownership-only lifecycle boundary, and Lease/Fence HostGeneration propagation are covered by runtime/adapter evidence; PostgreSQL v35 migration, FenceJournal, and ownership behavior passed isolated disposable-schema tests |
| M1 completion gate | accepted / green (2026-09-04) | stop-the-line M1 contracts, validators, lifecycle/effect/ownership/Fence gates, core.Plugin compatibility facade, provider/protocol metadata seams, public API budget, and required modulecheck evidence are green; this does not claim server auto-wiring, full legacy-plugin removal, M2 model control, external exactly-once, or completion of the remaining historical PostgreSQL fixture gaps |
| Graph-G0 contract/validator | accepted / green (2026-09-04) | stdlib-only Definition/ValidatedDefinition, bounded node/edge/state/context/redaction contracts, metadata registries, canonical revision binding each node kind/reducer/conditional predicate ID plus exact registry version, and deterministic top-level patch validation passed focused/repeated/race/vet/modulecheck evidence; G1 must separately capture implementation-locator/composition evidence, and G1 execution, checkpoints, runs, events, telemetry, provider/protocol binding, workflow adaptation, and sandbox execution are not started |
| Sandbox-S0 contracts and local assurance | queued; independent of M0c | existing executor path remains; unavailable assurance is not advertised |
| Perf-P0 baseline / Perf-P1 hot path | baseline and first hotspot optimizations integrated; main-session final integrated-state validation remains pending and neither blocks M0c | no new admission or load-shed gate; the old 384 MiB 500-concurrent provisional target is invalid for max payload |

### Accepted M0c/M0d evidence (2026-09-03)

M0c and M0d are accepted bounded verticals, not completion of the overall
refactor. M1a contracts/validators/snapshot and the accepted M1b-M1d4 runtime
follow-on slices were the green slices at that earlier checkpoint. The
accepted evidence recorded there was:

* M0c: the DB-backed model-settings path reads a present row as authoritative,
  including inactive or malformed rows; the legacy environment provider is
  lazy and is consulted only for an absent row or a validated one-time import.
  `config_source` is persisted/returned as nonsecret evidence. MaxTokens
  presence is persisted across restart, API omission preserves the stored value,
  and the canonical model-settings path uses write-only secret input plus the
  fixed recognition preview (first three characters, fixed mask, last four;
  values shorter than eight are fully hidden). Relevant evidence includes
  `pkg/app/modelsettings/service_test.go`,
  `pkg/adapter/modelsettings/store_test.go`, and
  `pkg/server/server_settings_test.go`.
* M0d: the typed storage resolver and service make a present resources or
  sessions row authoritative, including inactive/malformed states; environment
  values are lazy absent-row legacy fallback only. Desired revision CAS,
  nonsecret resolution evidence, resource apply failure retaining the old active
  backend, and session `restart_pending`/restart-active behavior are covered by
  `pkg/app/storageconfig/storageconfig_test.go`,
  `pkg/app/storageconfig/service_test.go`, and
  `pkg/adapter/storageconfig/store_test.go`. The isolated SQLite HTTP/storage
  path and secret merge/preview/clear semantics are covered by
  `pkg/server/storage_test.go:TestStorageSettingsHotSwapAndSecretMerge`.
* Typed transport and local UI alignment is covered by
  `pkg/server/server_storageconfig_test.go`, the storage adapter tests,
  `pkg/console/console_test.go` (including revision-CAS reload), and
  `scripts/verify-openapi` (95 registered `/v1` operations). The accepted
  workspace gates run in this snapshot are `go test ./... -count=1`, targeted
  M0c/M0d and Console tests, targeted `go test -race` for those packages,
  `go vet ./...`, `go test ./internal/modulecheck -count=1`, and
  `scripts/module-smoke.ps1`; these all passed. In the final integrated
  workspace, full-repository `go test -race ./... -count=1` also passed with
  MinGW on PATH (exit 0; `cmd/server` 2.760s, `pkg/server` 46.984s,
  `pkg/storage` 87.772s). The earlier concurrent-run observation in
  `TestConfigureStorageDefaultsToEmbeddedBackends` is resolved in the final
  integrated state: absent rows must consult the legacy provider, while only
  DB-present tests require zero legacy lookups. No secret or configuration
  value was printed by these checks.
* PostgreSQL fresh/historical verification remains an explicit environment
  skip: no administrator-capable `HARNESS_TEST_PG_DSN` was available, so it is
  not counted as a pass. Gitleaks and Docker gates were unavailable in this
  run; neither is claimed green. The original broad migration tasks and their
  gates remain in the plan and are not superseded by accepting M0c/M0d.

### Accepted M1b-M1d4 evidence (2026-09-03)

These entries record the bounded runtime and SQL verticals at that earlier
2026-09-03 checkpoint; no automatic server wiring was implied by that evidence.

* M1b/M1c: Module Host lifecycle/effect ownership and the v33 SQL
  `EffectJournal` path are accepted with durable effect ordering, inverse
  ownership, lease identity, drain/fence/deactivate, idempotency, and
  SQLite/PostgreSQL adapter coverage.
* M1d1/M1d2: `DurableHostState` validation and the in-memory/SQL
  `CompositionStore` paths are accepted with strict revision CAS, clone and
  deterministic serialization checks, v34 fresh/historical migration, and
  16 MiB state-boundary coverage.
* M1d3: apply stages a Prepared checkpoint, promotes only after activation,
  preserves the checkpoint when rollback itself fails, refreshes inherited
  effects by generation/ordinal, and removes active/retained lineage only at
  the durable deactivation point.
* M1d4: `RecoverModuleHost` rebinds only journal-evidenced resources, never
  replays `Activate`, marks prior public leases lost, validates composition,
  phase, module/version, and manifest allow-set bindings, cleans orphan and
  Prepared generations before publishing an active owner, and performs
  Reconcile cleanup without publishing a failed recovery. The SQLite
  integration test uses a fresh database handle to model the persistence
  boundary; it is not an OS cross-process test.
* The bounded M1 ownership/Fence/lifecycle slice adds v35 SQL host ownership
  and Fence journal rows, generation-fenced Lease/Fence requests, a 30s
  default ownership TTL, and ownership-only `ReleaseOwnership`/`Close`.
  SQLite fresh/historical/repeat-open migration evidence and runtime tests are
  accepted. PostgreSQL v35 migration, FenceJournal, and host-ownership
  verification passed on 2026-09-04 against isolated disposable schemas;
  future runs without an administrator-capable `HARNESS_TEST_PG_DSN` remain an
  explicit skip. TTLs above 30s and legacy `HostGeneration=0` do not guarantee
  external exactly-once.
* The integrated workspace evidence is full `go test`, `go vet`, and
  `internal/modulecheck`; runtime and integration suites also passed repeated
  `-count=10` and race variants. Actual PostgreSQL v34 CompositionStore,
  EffectJournal, and migration tests passed when the configured test service
  was available. No credentials or secrets were printed.
* These accepted M1 slices do not complete the separate M2 model-control
  program. `pkg/adapter/coreplugin` is an accepted but deliberately bounded,
  process-local and non-recoverable `core.Plugin` compatibility facade: it
  requires declared runtime provides, permits no durable effects, and never
  replays legacy `Install` on recovery. Declared provides are a trusted legacy
  contract; the layered core registries allow duplicate contributions during
  mounting, so neither existing registrations nor registrations performed by
  `Install` can be preflighted. Core registry mounting remains transactional
  on installation errors, but this facade does not add service lookup, server
  wiring, or external-effect recovery guarantees. It echoes HostGeneration in
  leases for compatibility, but has no durable activation baseline and cannot
  independently fence downstream work. Full legacy-plugin migration and
  eventual removal of this compatibility facade remain separate follow-on work.
  The accepted
  Provider/Protocol slice is limited to independent typed registration
  metadata projected into runtime collection extensions; it does not bind
  providers to protocols or introduce transport, catalog, router, or auth
  behavior.
  Exactly-once inverse behavior still depends on a real executor being
  idempotent by `EffectID`; recovery owner local cleanup is best-effort and
  `Deactivate` must be idempotent. There is no background recovery goroutine
  or automatic server startup wiring in this slice.

### Current stop-the-line override: M0 -> M0c -> M0d -> M1 -> M2

The following approved gates take precedence over starting the original
Phase 4 and Phase 5 broad migrations. They are not a deletion or renumbering
of the original phases; each preserves the original verification gates and
feeds its resulting contracts back into them.

1. **M0 — correctness and secret stop-the-line.** Reject profile selection
   versus actual wire-model mismatch at the existing compatibility boundary.
   Do not introduce CatalogResolver or secret CAS in M0. Generic secret GET
   returns no complete value and is explicitly write-only/redacted; an
   designated recognition preview is first three characters + a fixed mask token
   + last four characters, with values shorter than eight fully hidden. The
   fixed token does not reveal source length; this fixed preview is the only
   permitted recognition information, with no additional hash/fingerprint/
   digest/reversible derivative. Console forms never hydrate, re-submit, or
   treat the preview as a secret. New secret commands distinguish
   omitted preserve, supplied replace, and clear_secret true delete; supplying
   both replace and clear is rejected.
2. **M0c — LLM configuration DB vertical.** Complete this bounded vertical
   after M0 and before M0d. Persist and read the current LLM runtime
   configuration through the DB-backed application/settings path. This
   establishes DB authority for provider/protocol/catalog/auth references,
   allowed models, router policy and storage policy without introducing the
   M2 CatalogResolver or credential CAS/revision. `.env` is accepted only for
   an absent row's legacy fallback or a one-time import; record the effective
   `config_source` as `bootstrap`, `db`, `env_import` or `legacy_fallback`.
   A DB row that exists but is inactive or malformed fails visibly and is never
   silently overridden by `.env`.
3. **M0d — storage configuration DB vertical.** Complete this bounded storage
   source-of-truth slice after M0c and before M1. The pre-M0d baseline showed
   the main composition preferring `HARNESS_S3`/`SESSION_DIR`, while the
   storage loader treated malformed values as absent; the accepted M0d wiring
   removes that precedence. Introduce typed `ConfigSource`,
   `ConfigStatus` and nonsecret resolution evidence. A resources or sessions
   row is authoritative when present, including embedded, S3, inactive or
   malformed state; consult `.env` only when the row is absent. A later
   one-time import must validate and encrypt legacy values before creating a
   row. Keep desired configuration separate from active/applied status: a
   failed dynamic resource swap retains the old active resource, while a
   session change is restart-pending. Do not migrate worker/runner/approval/
   telemetry environment configuration in M0d, and do not change secret
   semantics.
4. **M1 — minimum trusted module host and effect boundaries.** Establish the
   dependency DAG, five extension-point semantics and independent validators,
   staged snapshots, durable EffectJournal coverage for Stage/Activate/Reconcile,
   actual recorded inverse effects, health/drain/fence/deactivate/lease/reconcile
   behavior, AcceptedInvocation and ModelCallGate seams, and the
   Provider/Protocol registration shells. Keep core.Plugin as a compatibility
   facade during this stage. Agent tool/user communication effects, model calls,
   and application persistence remain separate boundaries; trusted adapters may
   hold transports, untrusted modules may not.
5. **M2 — model-control vertical and protocol migration.** Add the
   application/HTTP/Console model-control slice, contextual
   ProviderRegistry/ProviderPlan resolution, credential CAS/revision and
   opaque auth signer, migrate the legacy OpenAI Chat path into separate
   model-provider and model-protocol ownership, and add Responses and Anthropic
   protocol paths behind the M1 registry.

M0 must be green before M0c. M0c must be green before M0d. M0d must be green
before M1. M1 must be green
before M2. M2 is complete only
with its public resolver compatibility fixture and proof that the contextual
selected catalog model, ProviderPlan endpoint/signer, protocol wire model,
credential revision, module closure, composition evidence, and
evaluation/canary evidence agree without exposing credentials. A logical Run
may contain multiple approval-resumed execution segments; every segment keeps
an immutable composition and records previous/new composition revisions plus
the authorization basis. Only then may the original broad Phase 4 and Phase 5
work begin; their existing tasks, dependencies, rollback rules, and gates
remain in force.

### Near-term order and parallel workstreams

The dependency order is `M0 -> M0c -> M0d -> M1 host -> first Graph vertical ->
M2`. Sandbox-S0 and remaining Perf-P1 evidence work may run in parallel with
M0c/M0d after their input contracts are frozen; neither blocks M0c. Perf-P0
baseline collection and the first Perf-P1 hotspot changes are integrated, while
their final integrated-state verification remains non-functional for M1. M0d
must not start before M0c and must complete before M1. Sandbox-S0 is a
prerequisite only for a sandbox-using node/Graph vertical. M0c/M0d are deliberately before M1: runtime
configuration must have one DB source of truth before new module composition
or Graph routing can consume it. Only DB connection, listener/address, data
directory and an external root-key reference are bootstrap exceptions. Once
the DB is available, LLM/provider/protocol/catalog/auth refs, allowed models,
router policy, storage policy, Graph definitions and checkpoint configuration
are DB records. Environment values are limited to absent-row legacy fallback
or one-time import, with `config_source` evidence; a present inactive or
malformed row fails visibly and cannot fall through to `.env`.

After the M1 Orchestrator/effect Ports are frozen, the Graph contract and
validator can proceed in parallel with M1 lifecycle work. Graph belongs in
`pkg/extensions/graph`, not `pkg/core` or the generic ModuleHost. Its first
vertical is blocked until M1 has green dependency-DAG, snapshot, durable
EffectJournal, lease/fence/drain/deactivate and effect-gate negative tests.

The Graph contract slice includes a versioned definition, state schema,
deterministic reducer, node/edge registries, default/conditional/error edges,
cycle/visit/step budgets, retry/timeout/cancel, checkpoint CAS, approval
interrupt, segment lease release and new-segment resume. Each node declares a
ContextView and ContextBudget; no node receives global Graph state/history by
default. EventLog is fact source, Graph checkpoint is execution fact state,
and ContextView is materialized from current input, required state fields,
pending approval/tool pair and bounded episode/summary/artifact/retrieval
layers. ContextPlan records typed sources/revisions, segment and per-layer token
budgets, token estimate, output reserve, stable-prefix ordering and compaction
reason; its validator checks budget arithmetic, required invariants, source
isolation and redaction. Memory/RAG sources stay isolated and hierarchical
summaries cannot recursively
drift without a new source revision.

Guarded Commit must issue the host-only AcceptedInvocation for tool/user-
communication effects. Model calls must use the independent ModelCallGate;
application persistence uses UoW. No Graph node may call a raw transport or
construct an invocation. The first runnable Graph may use a fixture
ModelCallGate until M2, but it must exercise the same seam.

Graph execution and observability ship together: bounded events/traces/metrics
cover graph, node, edge, state patch, checkpoint, retry, approval, cancel and
error; state is redacted. Context-plan revision, token estimate/by-layer
allocation and compaction reason are allowed metadata. Session event grammar
must be extended through a validated registration/envelope, never bypassed.

Required Graph tests include validator/reducer determinism, edge ambiguity,
cycle and retry bounds, timeout/cancel, checkpoint crash/CAS/revision checks,
approval new-segment behavior, context source isolation, gate negative cases,
and event/trace/metric redaction. The first vertical can omit fan-out/join,
dynamic graph mutation, editor/DSL, distributed scheduling and subgraphs;
those are post-vertical extensions.

### M0c implementation slice

Ownership stays in the existing DB-backed settings/application path until M2
model-control has a domain service. The slice must:

1. read the DB row first and use it as the effective LLM runtime configuration;
2. import `.env` once or use legacy fallback only when the row is absent;
3. persist an explicit source/audit value (`bootstrap`, `db`, `env_import`, or
   `legacy_fallback`) without persisting raw secret values in responses or
   telemetry;
4. fail visibly on a present inactive/malformed row, with no environment
   override or silent fallback; and
5. retain the legacy mapper as a rollback facade, without adding CatalogResolver
   or credential CAS/revision before M2.

Its atomic boundary is DB read/validation plus effective-source recording. The
acceptance slice includes fresh/restart behavior, absent-row import/fallback,
DB-present precedence, malformed/inactive failure and source observability.

### M0d storage configuration DB slice

M0d is strictly after M0c and before M1. The pre-M0d main composition preferred
`HARNESS_S3`/`SESSION_DIR`, and the storage loader treated malformed
configuration as absent; both remain explicit regression cases, while the
accepted startup path is DB-first and fail-closed. Add typed
`ConfigSource`, `ConfigStatus` and nonsecret resolution evidence. For resources
and sessions, row existence is authoritative for embedded, S3, inactive and
malformed states. Environment is consulted only when the row is absent. A
future one-time import must validate and encrypt legacy values before creating
a row.

Keep desired configuration separate from active/applied status. A failed
dynamic resource swap preserves the previous active resource and records the
apply failure on the desired row. A session resource change is
restart-pending, not silently applied to a live session. Worker, runner,
approval and telemetry environment-to-DB migration is explicitly later work;
M0d does not alter secret write-only, preview or redaction semantics.

The atomic acceptance slice covers DB-row resolution plus status/evidence
recording. Tests must cover main/env precedence regression, malformed-as-absent
regression, every row-present state, absent-row fallback, validated encrypted
import, restart persistence, failed dynamic swap retaining the old active
resource, session restart-pending, source evidence and secret sentinel scans.
Rollback retains the legacy storage loader and main composition, disables DB
storage activation/import, preserves the old active resource after a failed
swap, and leaves pending session changes for explicit restart. No destructive
data or secret rollback is allowed.

### Sandbox-S0 implementation slice

Keep the sandbox implementation outside `pkg/core`, `pkg/extensions/graph` and
the generic ModuleHost. Its Sandbox Provider identity is distinct from the
model Provider identity. Add the generic contract under
`pkg/execution/sandbox` with Provider, Session, Assurance, Limits, Network,
Mount, Artifact and fenced Lease values. The default provider is local
ephemeral with warm-pool size zero. Linux bwrap is recorded as process
confinement on a shared kernel with network isolation absent. Windows is
currently unavailable; no facade may claim stronger assurance. Container,
microVM, cloud, self-hosted and remote adapters are future providers.

S0 must implement requested-versus-actual assurance comparison and fail closed
for unavailable/weaker assurance, unenforced limits or unmet network/mount
policy. Guarded Commit binds AcceptedInvocation, ToolInvocationJournal,
fenced lease and staged artifacts to the run/segment/module composition.
Artifacts are visible only after verification and explicit commit. Timeout or
cancel fences the lease and journals child-process outcome; unknown results are
quarantined for reconciliation. Negative tests prove that untrusted modules
cannot hold process handles, raw transports or root mounts.

S0's rollback is capability disablement plus the existing executor path; it
does not require a destructive storage migration and does not block M0c.

### Perf-P0/P1 implementation slice

Perf-P0's reproducible 10/100/500-concurrent plus soak baseline and Windows
RSS/CPU collection are implemented. Perf-P1's first narrowly scoped hotspot
optimizations are integrated. Their combined state awaits the main-session's
final verification gate; this plan does not add a performance admission gate.
During P0/P1, do not add an admission controller, weighted semaphore, queue,
backpressure or 429 gate: 500 means 500 genuinely live executions, split by
payload/context scenario with sandbox child RSS/startup reported separately.

The two GC-isolated production-shaped 500-concurrent Agent rounds record
161–198 MiB sampled heap peak and 162.6–200.1 ms p95 for a typical 16 KiB
payload, and 1.54–1.76 GiB sampled heap peak and 2.83–2.94 s p95 for max 1 MiB.
Both Agent rounds had zero errors. Max/500 case RSS ranged 1.84–3.53 GiB across
the same-process rounds; it includes preceding-case and Go-arena effects and
is not a per-case or permanent per-execution resident-memory number.

Two 500-concurrent HTTP `/healthz` rounds are not interchangeable with those
Agent results: one had zero errors and one had 200 transport errors. They are
not evidence that 500 HTTP is stably passing.

The previous 384 MiB 500-concurrent figure was an unmeasured provisional
target, not a newly missed gate. It is invalid for the max scenario: 500 * 1
MiB is 500 MiB of raw information before context, headers, maps, request/
response copies, runtime overhead, or allocator/GC retention. No replacement
budget is introduced in this slice.

Implemented P1 changes are fused default no-summary session projection plus
recent-turn compaction, incremental projection cache/read helpers, immutable
tool-library index plus bounded 240-rune summaries, adaptive runner idle
polling, and normal File/S3 resource streaming GET and PUT. A 16 MiB streaming
benchmark recorded about 60 KiB/op File and about 168 KiB/op S3 Go-heap
allocation, so payload does not linearly enter the heap. File/S3 PUT uses
bounded temporary storage where required to retain integrity/replacement
semantics; the legacy cold compressed-session fallback still materializes its
historical layout. In an 8,192-event / 128-message-window microbenchmark,
legacy `DeriveMessages` then `Compact` used about 1.16 MiB and 10,754
allocations per operation; fused projection used about 65.5 KiB and 1,061.
Separate 500-concurrent tool search recorded p95 about 4.32 ms for 128 tools
and 7.33 ms for 512, while post-GC retained index observations were about 0.24
MiB and 2.76 MiB. Search throughput excludes catalog construction and does not
prove a compact resident index.

An isolated temporary dev/bootstrap SQLite `cmd/server` executable was also
measured after its build process exited. In a 20-second idle window, Working
Set max was 19.69 MiB, private memory max 19.04 MiB, and normalized CPU 0%; no
public goroutine metric is exposed. A 20-second single-client `/healthz` sample
reached 21.95 MiB Working Set, 21.11 MiB private memory, and 0.0039% normalized
CPU, with 96 successful requests and client p95 1.778 ms. This is a short
idle/light-load observation, not a long soak or a 500-HTTP stability result.

Remaining work is artifact-reference handling for large results, a compact
resident tool-index representation, and ContextPlan token budgets with
hierarchical summaries. Observe RSS, heap, CPU, goroutines, GC, DB, S3 and
child-process metrics with bounded payload/context dimensions and remeasure
the identical P0 workloads after each further optimization.

For N live executions, payload P, materialized context C, retained metadata H,
runtime overhead R and sandbox-child RSS X, every report includes:

~~~text
RSS_min(N,P,C,H,R,X) >= host_base + N * (P + C + H + R) + X
~~~

This information lower bound is a sanity check, not an excuse to retain
unbounded history. A missed target is reported with measured values and
scenario; it is not hidden behind load shedding. P0/P1 rollback keeps the
baseline path or reverts the narrow optimization and changes no concurrency
semantics.

### M1-to-Graph and ContextPlan work packages

The Graph work is deliberately a separate extension package. The proposed
ownership is:

```text
pkg/extensions/graph/
  contract.go state.go registry.go validator.go
  checkpoint.go interrupt.go executor.go events.go telemetry.go
  workflow_adapter.go *_test.go
```

`pkg/core` exposes only the already-approved narrow Orchestrator/effect/event
Ports. `pkg/extensions/graph` owns GraphDefinition, NodeSpec, EdgeSpec,
StateSchema, StatePatch, deterministic Reducer, ContextPlan, checkpoint
envelope and Graph state machine. ModuleHost owns module lifecycle and leases;
Graph owns neither module registry nor Provider/Protocol lookup.

The slices are:

| Slice | Dependency | Must ship together | Can be deferred |
| --- | --- | --- | --- |
| Graph-G0 contract/validator | M1 Orchestrator Port shape frozen | version/revision, state schema/reducer, node/edge registry, default/conditional/error edge rules, cycle/visit/step limits, ContextView/ContextBudget, redaction schema | runtime execution |
| Graph-G1 first vertical | M1 snapshot, EffectJournal, lease/fence/drain/deactivate, AcceptedInvocation and ModelCallGate negative gates green; Sandbox-S0 additionally gates sandbox-using nodes | experimental memory-only contract/executor slice exists but is **not accepted**: reducer + edge selection + checkpoint CAS; retry/timeout/cancel; approval interrupt + lease release + new segment resume; complete events/traces/metrics still require the durable/runtime acceptance boundary | real providers, fan-out/join, dynamic mutation, editor/DSL, distributed scheduler |
| Context-C0 contract | EventLog/checkpoint facts and bounded telemetry Port | typed sources/revisions, layer budgets, estimator, output reserve, stable prefix, current-input/state/approval invariants, source isolation validator | policy tuning and UI |
| Context-C1 materialization | Context-C0 | structured history -> budget compaction -> durable summary -> memory/RAG isolation, with plan evidence | advanced retrieval and parallel context assembly |

Graph-G0 and Context-C0 may run in parallel with the latter half of M1 after
the host Ports are frozen. Graph-G1 and Context-C1 are not activated until the
M1 lifecycle/effect gate is green. A fixture ModelCallGate is sufficient for
Graph-G1; actual Provider/Protocol integration remains M2.

Graph-G1's node context is materialized per node and includes only current
input, schema-required state, pending approval/tool pair and bounded history
layers. The checkpoint is the fact state; ContextView is disposable. Plan
revision, token estimate/by-layer allocation and compaction reason are bounded
nonsecret evidence. Memory and RAG cannot rewrite canonical history, and
hierarchical summaries must carry source revisions to prevent recursive drift.

No Graph fallback may change a running segment to sequential semantics. To
roll back, stop new Graph assignments, fence/drain Graph modules, preserve
checkpoints for reconciliation, and leave the existing workflow/legacy Plugin
facade active. No destructive checkpoint migration is part of this slice.

## Phase 0 — Baseline and executable architecture guardrails

Dependencies: none.

Tasks:

1. Inventory public packages, exported symbols, constructors, `/v1` routes,
   OpenAPI operations, SSE event names, schema versions, and external compile
   consumers. Store the inventory as a reviewable artifact; distinguish
   observed behavior from intended target behavior.
2. Add compile fixtures that import the currently supported public paths and
   exercise representative constructors, interfaces, aliases, and response
   types. Keep fixtures outside production packages so they cannot become
   accidental runtime dependencies.
3. Lock critical HTTP, SSE, approval continuation, Run queue generation,
   Session lease, repair, and tool-journal behavior with focused black-box
   tests. Do not redesign wire or database contracts in this phase.
4. Add fresh and historical SQLite migration fixtures. Add PostgreSQL fixtures
   when a configured PostgreSQL test service is available; otherwise retain a
   deterministic skip with the required environment documented.
5. Extend `internal/modulecheck` with checks that are true in the current
   tree: `pkg/core` imports only the standard library, generic top-level
   `pkg/{common,enums,helpers,models,utils}` packages are rejected, and the
   existing foundational dependency direction is recorded. Do not enforce
   un-migrated `server`, `storage`, or future `adapter/app/runtime` rules yet.

Verification gate:

```text
go list ./...
go test ./...
go vet ./...
go test ./internal/modulecheck
OpenAPI route/schema verifier
fresh and historical SQLite fixtures
PostgreSQL fixtures or an explicit environment skip
```

Rollback point: revert only the baseline tests, fixtures, and modulecheck
changes. No production behavior or schema is changed in Phase 0.

Parallel boundaries: public API inventory, OpenAPI inventory, migration
fixtures, runtime invariant tests, and modulecheck can be prepared in parallel.
The fixture format and package inventory owner must be agreed before merging.

## Phase 1 — Identity/Auth reference vertical slice

Dependencies: Phase 0 public inventory, compile fixtures, and migration
fixtures.

Tasks:

1. Define identity domain values and typed `Role`/`Status` enums with parse,
   validity, and stable wire/database values.
2. Define application commands/results and narrow identity Ports. Keep request
   DTOs in the HTTP adapter and private SQL rows/mappers in the SQL adapter.
3. Implement activation, login gating, token revocation/creation, and
   throttling through a Unit of Work Port. Preserve atomic password and token
   updates and pending-administrator semantics.
4. Keep old `storage.Account` and constructors as deprecated facades backed by
   the new adapter. Do not add new feature ownership to the facade.
5. Update the embedded Console activation/login mapping so only documented
   request fields are sent.

Verification gate: compile fixtures, identity unit tests, public HTTP login and
activation smoke tests, SQLite fresh/upgrade tests, PostgreSQL fresh/upgrade
tests where available, and full Phase 0 gates.

Rollback point: retain the old identity path behind the facade and switch the
composition root back to it. No destructive schema operation is allowed without
a preservation fixture and a reversible migration.

Parallel boundaries: enum/domain definitions, HTTP DTO tests, SQL row/mappers,
and Console request mapping can proceed in parallel after the Port shape is
frozen. Transactional service integration and facade wiring are sequential.

## Phase 2 — HTTP transport and application-service extraction

Dependencies: Phase 1 identity conventions and compatibility fixtures.

Tasks:

1. Migrate one feature route family at a time to named request/response DTOs.
2. Map DTOs to application commands/results and typed error categories.
3. Replace ordinary success `map[string]any` responses with named responses;
   keep dynamic maps only where the wire contract is intentionally open.
4. Separate route registration, authentication, decoding, validation, error
   mapping, and response writing from worker lifecycle and infrastructure setup.
5. Keep `server.New` and existing route behavior as compatibility facades.

Verification gate: OpenAPI verification, route-by-route black-box tests, JSON
unknown-field/type/limit tests, SSE tests, and full Phase 1 gates.

Rollback point: route registration can select the previous handler while the
new application service and DTO tests remain isolated. Preserve status codes,
field names, and event ordering.

Parallel boundaries: independent route families may migrate in parallel when
they do not share a transaction or mutable ViewModel. Shared decoder/error
policy and composition-root changes are single-owner work.

## Phase 3 — SQL infrastructure and feature repositories

Dependencies: Phase 1 identity adapter and Phase 2 application Ports.

Completed prerequisite (2026-09-02): the legacy `pkg/storage/sqlstore.go`
was mechanically decomposed by dialect, startup lock, schema constants,
migration orchestration, Memory/RAG projection migrations, evidence
backfills, and Session persistence. This preserved the package, symbols, SQL
strings, schema v1-v37 chain, and execution order; it is not a feature
repository migration. PostgreSQL fixture execution remains an environment
skip until an administrator-capable test DSN is available.

Tasks:

1. Extract dialect binding, transaction helpers, migration registry, schema
   inspection, and startup locking into `adapter/sql/sqlkit` and
   `adapter/sql/migration`.
2. Move feature rows, queries, mappers, and repositories one feature at a time
   under `adapter/sql/<feature>`; rows remain private.
3. Implement application Unit of Work adapters for cross-aggregate operations:
   Run submission/queueing, approval decision/requeue, cancellation fences,
   and evidence reconciliation.
4. Preserve v1-v37 history, PostgreSQL advisory locks and `SKIP LOCKED`, and
   SQLite conditional transaction paths. Reject unknown dialects.
5. Keep `pkg/storage` constructors delegating to the new adapters until the
   major-version removal boundary.

Verification gate: fresh and every supported historical SQLite fixture,
PostgreSQL fresh/upgrade fixtures, row-preservation checks for rebuilds,
concurrency/race tests for queue and lease operations, and all prior gates.

Rollback point: select the legacy storage constructor at composition time. A
new migration must be retryable and must not be merged without a before/after
row-preservation fixture.

Parallel boundaries: unrelated repository slices can migrate in parallel after
`sqlkit` contracts are frozen. Migration registry, transaction semantics, and
cross-table Unit of Work changes require one serial integration owner.

## Phase 4 — Capability, invocation, execution, and Runner separation

Dependencies: Phase 2 application Ports and Phase 3 repository/Unit of Work
adapters.

Tasks:

1. Separate capability definition, executor, model ToolCall, runtime
   Invocation, canonical ToolResult, journal record, and adapter wire models.
2. Move HTTP/MCP/WASM/process/private Runner implementations beneath the
   execution adapter family with compile-time Port assertions.
3. Split Runner model/state machine/store/hub/provider/worker protocol/admin
   concerns while retaining compatibility wrappers.
4. Ensure every nested call re-enters the protected invocation funnel for
   schema, scope, permission intersection, approval, credentials, budgets,
   timeout, journal, and telemetry checks. Agent-proposed tool and
   user-communication effects receive a host-issued, unforgeable
   AcceptedInvocation from Guarded Commit. Model outbound calls use the
   separate ModelCallGate; repository/control-plane writes use application
   use-cases and Unit of Work. Trusted adapters may hold transports, while
   untrusted modules cannot.

Verification gate: capability contract tests, unknown-outcome/idempotency tests,
approval digest binding tests, nested-call bypass tests, queue-generation and
lease fencing concurrency tests, and all prior gates.

Rollback point: keep old executor selection and Runner facade available behind
the same protected funnel. Never roll back by exposing a raw provider shortcut.

Parallel boundaries: protocol adapters can proceed in parallel once the
canonical invocation/result contracts are frozen. Funnel and journal changes
are sequential and require the high-risk architecture/security review.

## Phase 5 — Runtime extraction and reversible module registry

Dependencies: Phase 4 canonical invocation contracts and compatibility fixtures.

Tasks:

1. Move generic Agent turn/resume, composition, context summary/repair, usage,
   telemetry orchestration, and plugin mounting mechanics into `pkg/runtime`.
   Define logical Run versus immutable execution segment; approval resume
   releases the segment lease and records previous/new composition revisions
   plus authorization basis.
2. Keep stable `pkg/core` aliases/wrappers where external type identity must
   remain unchanged; do not introduce a reflection-based service locator.
3. Define focused typed Ports and reversible `Module`/`ModuleRegistry` mounts.
   `ProvidedExtension` includes semantic, contract/version, priority/order and
   resource identity; each of the five semantics has an independent validator.
   Registrations must be idempotent, permission-scoped, and unmountable.
4. Define durable module desired-state and `EffectJournal` persistence. Stage is
   effect-free or journals every external effect; Activate and Reconcile use the
   same journal. Implement WaitingDependencies, DrainResult, Fence,
   Deactivate, host-issued lease identity (RunID/composition/module revision),
   and restart reconciliation. Do not infer actual effects from manifest
   `Effects`.
5. Ensure modules receive required Ports through typed constructors and cannot
   obtain concrete adapters, raw transports, service-locator lookups, or bypass
   protected runtime invariants. Extend modulecheck with facade-forwarding,
   protocol-provider lookup, raw-transport, and old-Plugin/new-Module duplicate
   registration negative fixtures; set an explicit core public API/size budget.

Verification gate: external compile fixtures against old and new paths,
module install/uninstall/idempotency tests, semantic conflict tests, lease
identity and segment-resume tests, crash-window/restart reconciliation tests,
protected invocation and ModelCallGate tests, modulecheck negative fixtures,
runtime race tests, and all prior gates.

Rollback point: composition root selects the old `core` implementation while
aliases remain. Unmounting a module must leave no registered capability, route,
or worker behind.

Parallel boundaries: independent module registrations and documentation can
proceed in parallel. Core/runtime type moves and compatibility aliases require
one serial owner.

## Phase 6 — Local embedded Console and developer experience

Dependencies: Phase 2 stable HTTP DTOs/OpenAPI contract and Phase 5 module
registration surfaces.

Tasks:

1. Split the embedded Console into local `lib.js`, `api.js`, `router.js`,
   `store.js`, `enums.js`, one entry file per business domain, and a thin
   `app.js` composition/bootstrap file. Follow the organizational reference in
   `antseer-monorepo-main/backend/app/static`, without copying CDN assets or
   global mutable coupling.
2. Keep Alpine, JavaScript, CSS, fonts, icons, and framework assets under the
   embedded static tree. Reject remote script URLs and verify every local
   script through the embedded HTTP handler.
3. Add offline Console smoke coverage for startup, login/activation,
   navigation, JSON, upload, and SSE paths. Verify deterministic script order,
   cache busting, and optional-module omission behavior.
4. Add extension authoring templates and documentation for capabilities, model
   providers, storage adapters, HTTP endpoints, and reversible modules.
5. Update architecture and package-layout documentation and starter examples;
   keep old public paths covered by compatibility tests.

Verification gate: local asset scanner, embedded asset HTTP smoke tests,
offline/no-network Console smoke tests, OpenAPI verifier, full Go test/vet/list,
SQLite/PostgreSQL fixtures, and the complete runtime invariant suite.

Rollback point: retain the previous embedded asset manifest and switch the
`go:embed` composition back if the new Console fails to boot. Never restore a
remote JavaScript dependency.

Parallel boundaries: each domain JavaScript file and documentation page can be
implemented independently once shared `api/router/store` contracts are frozen.
Asset manifest, bootstrap order, and offline smoke harness have a single owner.

## Final release audit

After Phase 6, perform a requirement-by-requirement audit against the approved
specification. Confirm that every production file added during the migration
normally remains below 500 lines; any file over 800 lines needs a documented
reason. Confirm no complete secret enters manifests, logs, HTTP responses, or
Console ViewModels; only the explicitly bounded first-three/fixed-mask/last-four
recognition preview may appear in the designated domain read view, and it must
never be supplemented by a hash/fingerprint/digest/reversible derivative or
re-submitted. Run the full verification matrix twice: once on fresh databases and
once on historical upgrades. Publish a compatibility report listing preserved
import paths, routes, fields, status values, event names, schema versions, and
known environment-limited checks.

The refactor is complete only when a new contributor can follow the documented
vertical path and replace a model, transport, repository, execution adapter, or
optional module without editing unrelated feature code.
