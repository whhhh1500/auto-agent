# Public API and Compatibility Inventory

Snapshot date: 2026-09-07  
Status: current observed surface; this is an inventory, not a target-package
claim.

The machine-readable source is
[`2026-09-02-public-api-inventory.json`](2026-09-02-public-api-inventory.json).
It records all public `pkg/...` packages found by `go list ./pkg/...`, package
level exported types/functions/variables/constants, known constructors, HTTP
operations, SSE event names, and SQL schema history.

## Public package surface

The current public package paths are:

```text
pkg/adapter/coreplugin
pkg/adapter/graphapproval
pkg/adapter/graphtelemetry
pkg/adapter/httpapi/accountadmin
pkg/adapter/httpapi/auth
pkg/adapter/httpapi/jsonbody
pkg/adapter/httpapi/modelsettings
pkg/adapter/httpapi/notificationtarget
pkg/adapter/httpapi/runner
pkg/adapter/httpapi/sandbox
pkg/adapter/httpapi/settings
pkg/adapter/httpapi/storage
pkg/adapter/memory/graphcheckpoint
pkg/adapter/modelexecution/anthropic
pkg/adapter/modelexecution/corebridge
pkg/adapter/modelexecution/openai
pkg/adapter/modelprotocol
pkg/adapter/modelprovider
pkg/adapter/modelruntime
pkg/adapter/modelsettings
pkg/adapter/notification/coretool
pkg/adapter/notification/webhook
pkg/adapter/notification/webhook/targetresolver
pkg/adapter/runexecutor/graph
pkg/adapter/sandboxexec
pkg/adapter/sql/artifactmigration
pkg/adapter/sql/compositionstore
pkg/adapter/sql/effectjournal
pkg/adapter/sql/fencejournal
pkg/adapter/sql/graphcheckpoint
pkg/adapter/sql/graphsegment
pkg/adapter/sql/notificationtarget
pkg/adapter/sql/settings
pkg/adapter/sql/sqlkit
pkg/adapter/storage/artifactmigration
pkg/adapter/storageconfig
pkg/app/artifactmigration
pkg/app/capabilityruntime
pkg/app/contextassembly
pkg/app/identity
pkg/app/modelcatalog
pkg/app/modelcontrol
pkg/app/modelexecution
pkg/app/modelsettings
pkg/app/notification
pkg/app/runexecutor
pkg/app/runliveness
pkg/app/secretview
pkg/app/settings
pkg/app/storageconfig
pkg/buildinfo
pkg/console
pkg/control
pkg/core
pkg/evaluation
pkg/execution
pkg/execution/graph
pkg/execution/sandbox
pkg/extensions
pkg/extensions/graph
pkg/extensions/memory
pkg/extensions/rag
pkg/extensions/runner
pkg/extensions/subagent
pkg/extensions/toollib
pkg/extensions/workflow
pkg/integration
pkg/logging
pkg/provider/openai
pkg/runtime
pkg/server
pkg/storage
pkg/telemetry/otel
```

The largest compatibility surfaces are `pkg/core` (runtime contracts and
value objects), `pkg/storage` (session/object/SQL stores), and `pkg/server`
(HTTP/SSE composition). Their complete top-level symbol snapshot is in the
JSON file; representative constructors and extension seams are:

| Package | Representative public contracts | Constructors/factories |
| --- | --- | --- |
| `pkg/core` | `Agent`, `Session`, `SessionStore`, `CapabilityRegistry`, `AgentProfileRegistry.ReplaceExact`, `LlmAdapter`, `ToolRuntime`, `Plugin`, `Permission`, `SessionEvent` | `NewAgent`, `NewSession`, `RestoreSession`, `NewCapabilityRegistry`, `NewAgentProfileRegistry`, `NewCredentialRegistry`, `NewPolicyRegistry` |
| `pkg/storage` | `AccountStore`, `SQLQueuedPrincipalResolver`, `ObjectStore`, optional streaming and SQL recovery seams (`AuthorizationEpochReader`, `FencedSessionAppender`, completed-result appenders), `SQLSessionStore`, `RunQueueStore`, `ApprovalStore`, `EvidenceStore`, `SQLDialect` (source-compatible alias; `SQLDialectSQLite`/`SQLDialectPostgres`) | `OpenSQLSessionStore`, `NewSQLAccountStore`, `NewSQLQueuedPrincipalResolver`, `NewMemoryObjectStore`, `NewFileObjectStore`, `NewS3ObjectStore`, `NewSQLRunControlStore`, `NewFencedWriteBehind` |
| `pkg/server` | `Config`, `Server`, `Authenticator`, `PrincipalMapper`, `RunPrincipalResolver` | `New` |
| `pkg/control` | `ReleaseManager`, `CanaryManager`, `ReleaseJournal`, `CanaryStore` | `NewReleaseManager`, `NewCanaryManager` |
| `pkg/evaluation` | `Store`, `Evaluator`, `Dataset`, `RunResult`, `GatePolicy` | `NewMemoryStore`, `NewRegistry` |
| `pkg/execution` | `Executor` implementations, MCP configuration, tool-library observer | `NewMemoryLibraryObserver`, `RegisterMCPServer` |
| `pkg/extensions/graph` | stdlib-only Graph/Context contracts: immutable definitions, bounded context/redaction/state, mutable CAS checkpoint heads, immutable checkpoint versions, append-only transitions, approval evidence, unchanged hot `Store`, and optional `HistoryStore`. New history surface is `CheckpointVersionInfo`, `CheckpointVersion`, `CheckpointVersionOrigin`, `CheckpointVersionID`, `CheckpointDigest`, clone helpers, and version validators. Implementations remain in sibling packages. | `NewNodeKindRegistry`, `NewPredicateRegistry`, `NewReducerRegistry`, `ValidateDefinition` |
| `pkg/execution/graph` | **experimental Graph-G1 executor** with immutable implementation bindings, bounded retry/timeout/cancel, explicit segment lease/context planner/sandbox authorizer/approval authorizer ports, fail-closed unknown recovery, and approval suspension/resume. It does not expose core/provider SDK contracts; sequential remains the default runtime. | `NewBindings`, `NewExecutorWithOptions` |
| `pkg/adapter/memory/graphcheckpoint` | bounded, defensive-copy reference Store and `HistoryStore` for tests/local embedding; it atomically records mutable heads with immutable versions and transitions, but is not durable and must not be selected as a production fact source. | `New`, `NewDefault` |
| `pkg/adapter/sql/graphcheckpoint` | experimental SQLite/PostgreSQL v41 durable Graph checkpoint/HistoryStore: atomic mutable-head CAS plus immutable version and append-only transition evidence, with bounded strict JSON decoding. It does not wire execution, leases, providers, or server routes. | `New` |
| `pkg/app/artifactmigration` | bounded durable resource-migration state, generation/CAS, worker lease, non-lease foreground mutation token, and metadata-only journal port. It has no copier, S3 client, route switch, server lifecycle, or credential configuration. | none; consumers implement/use `Repository` |
| `pkg/app/runexecutor` | experimental bounded application-level orchestrator registry. It exposes only `RunTurn`/`ResumeTurn`; the default sequential adapter delegates to an injected `core.Runtime`. Factories must be cheap and side-effect-free; their panics become typed errors. One ID has one active version, so an upgrade uses a new ID or drains then rebuilds the registry. Profile/database selection and Graph registration remain outside this package. | `NewRegistry`, `NewDefaultRegistry`, `NewSequential` |
| `pkg/app/runliveness` | shared deadline-aware scheduler for active-run cancellation and lease liveness. It has no per-run ticker, no per-run goroutine, and requires every registered callback to define its failure boundary. | `New` |
| `pkg/app/capabilityruntime` | bounded exact-version registry for dynamic capability runtime factories. It defensive-copies request/manifest values and contains factory panics; it owns neither a global registry nor transport policy. | `New` |
| `pkg/app/contextassembly` | bounded, deterministic context selection plus extractive/rolling summary policies behind core's optional assembler seam. It owns no background worker or durable writer outside its supplied session path. | `NewAssembler`, `NewExtractiveSummarizer` |
| `pkg/app/modelcatalog` | credential-free immutable provider/protocol catalog values and views. `View.Defaults`, `Compatible`, and `Assess` distinguish a known compatible pair from an unknown reference or known-incompatible pair; adapters retain executable factories privately. | none; consumers depend on `View` |
| `pkg/app/identity` | `UseCases`, `AdminUseCases`, `Service`, `AdminService`, ports, typed `Role`/`Status` | `NewService`, `NewAdminService` |
| `pkg/app/notification` | stdlib-only N0 provider-neutral notification contract: immutable exact-version channel registry, opaque target refs, bounded non-secret delivery/receipt metadata, defensive clones, and panic isolation. It owns no transport, credentials, persistence, global registry, reflection, or worker. | `NewRegistry`, `NewTargetRef` |
| `pkg/adapter/httpapi/notificationtarget` | strict tenant-scoped target mutation/list DTOs. Opaque `config` is accepted only for create/update and is never included in a response. | none; DTOs are constructed by handlers |
| `pkg/adapter/httpapi/sandbox` | read-only, sanitized sandbox-provider discovery DTOs. Provider objects, host/mount paths, configuration, credentials, and raw unavailable causes are excluded. | `View` |
| `pkg/adapter/sql/notificationtarget` | encrypted SQL persistence for opaque target configuration and metadata-only target records. | `New` |
| `pkg/adapter/notification/webhook` | explicit webhook channel adapter plus target resolver seam; endpoint and secret material never enter delivery receipts. | `New` |
| `pkg/app/settings` | `UseCases`, `Service`, narrow `Repository`, optional `AbsentSettingCreator` and `SettingCompareAndSwapper` atomic seams, typed commands/results; generic GET is fail-closed and generic PUT requires a present non-null value | `NewService` |
| `pkg/app/modelsettings` | dedicated LLM `UseCases`, safe read result with non-sensitive configuration provenance and output-token cap, presence-aware command, narrow authoritative-record port, and a configuration-free optional post-save observer seam | `NewService` |
| `pkg/app/modelcontrol` | **experimental M2-A** closed, immutable catalog/provider/protocol metadata snapshot with opaque credential and endpoint evidence, explicit exact compatibility, bounded model capability metadata, and deterministic snapshot-bound `ProviderPlan`. It has no secret values, factory/runtime object, transport, SDK, fallback, global registry, or server wiring. A keyless local provider has an empty `CredentialRef`; explicit asynchronous catalog refresh constructs and swaps a new snapshot. Protocol execution and cross-protocol message handoff remain M2-B. | `NewRegistry` |
| `pkg/app/modelexecution` | **experimental M2-B** protocol-neutral bounded request/event/credential and exact-binding contract. Provider owns endpoint/auth/transport; Protocol owns wire body/parser; Registry requires exact snapshot/provider/protocol implementation evidence. It has no HTTP, core bridge, server, DB, global registry, or fallback. | `NewRegistry`, `NewStreamValidator`, `NewCredentialMaterial` |
| `pkg/adapter/modelexecution/corebridge` | adapter that projects normalized M2-B events into the existing `core.LlmAdapter` vocabulary, collapsing tool deltas only at that legacy boundary. | none; construct `Adapter` with an immutable Registry and Plan |
| `pkg/adapter/notification/coretool` | core capability adapter exposing `notify.channels` and approval-required/idempotent `notify.send`; it requires core's accepted-invocation proof and derives identity from guarded context, so models receive only channel/opaque-target refs and content. | `New` |
| `pkg/execution/sandbox` | bounded exact-version sandbox provider registry and value contracts. The local registration is composition-only and may be unavailable on a platform; an ordinary process is never represented as a sandbox. On Windows, the current-user Basic provider uses the restricted-child implementation and reports its Host-only, non-isolated network contract; elevated callers are rejected and the provider never falls back to host execution. Historical private control-plane packages are not part of the current public runtime inventory. | `NewRegistry`, `NewLocalRegistration` |
| `pkg/adapter/sandboxexec` | provider-neutral `sandbox.exec` capability with accepted-invocation gating, bounded argv/output, exact configured sandbox-provider reference, server-owned mounts, and verified tenant-prefixed artifact publication. It never falls back to a host process. | `New` |
| `pkg/adapter/modelexecution/openai` | OpenAI-compatible HTTP Provider shared by distinct Chat Completions and Responses Protocol adapters, with bounded response parsing and lossless tool-delta fragments. | `NewHTTPProvider`, `NewStaticCredentialResolver` |
| `pkg/adapter/modelexecution/anthropic` | independent Anthropic Messages HTTP Provider and Protocol adapters with their own wire semantics and protected provider-owned authentication headers. | `NewHTTPProvider`, `NewStaticCredentialResolver` |
| `pkg/app/storageconfig` | typed DB-authoritative storage kinds/backends, source/status evidence, lazy absent-row-only legacy resolution, and admin-safe CAS application service | `NewService`, `NewDesiredRevision`, `PrepareCreate`, `Resolver.Resolve`, `Resolver.ResolveWithLegacyProvider` |
| `pkg/runtime` | dependency-free V2 module manifests, semantic validators, deterministic immutable snapshots, effect-journal contracts, ModuleHost lifecycle/lease seams, durable host ownership, and explicit restart recovery. `CompositionStore` enables durable opening through `OpenModuleHost`; `RecoverModuleHost` rebinds journal-evidenced resources without replaying `Activate`, while retained compositions and recovery errors keep incomplete recovery explicit. Fencing is authorization-first through immutable `HostControls`, `FenceCommand`, and bounded request-idempotent `FenceJournal`; legacy `ModuleHost.Fence` deliberately fails closed. `HostOwnershipClaim`/`HostOwnershipStore` provide a 30s default, 5m maximum ownership TTL; `ReleaseOwnership`/`Close` release ownership only. `LeaseRequest`, `LeaseIdentity`, `Lease`, and `FenceRequest` carry `HostGeneration`; legacy zero generation is compatibility-only and does not guarantee external exactly-once. | `BuildSnapshotForAPI`, `NewModuleHost`, `NewModuleHostWithControls`, `OpenModuleHost`, `OpenModuleHostWithControls`, `RecoverModuleHost`, `RecoverModuleHostWithControls`, `ModuleHost.Activate`, `ModuleHost.Drain`, `ModuleHost.FenceAuthorized`, `ModuleHost.Reconcile`, `ModuleHost.ReleaseOwnership`, `ModuleHost.Close` |
| `pkg/adapter/modelprovider` | typed provider registration metadata projected to a validated runtime collection extension; no catalog, router, auth, model resolution, or protocol binding | `Registration.Extension` |
| `pkg/adapter/modelprotocol` | typed protocol registration metadata projected to a validated runtime collection extension; no transport, provider lookup, catalog, router, or auth | `Registration.Extension` |
| `pkg/app/secretview` | fixed, bounded secret recognition preview | `Preview` |
| `pkg/adapter/httpapi/auth` | named `LoginRequest`, `ActivationRequest`, and responses | none; DTOs are constructed by handlers |
| `pkg/adapter/httpapi/jsonbody` | strict-nested, tolerant-top-level JSON request decoder | `Decode`, `DecodeOptional` |
| `pkg/adapter/httpapi/runner` | named `ClaimRequest`, `GenerationRequest`, `CompletionRequest`; explicit queue/result mappings | none; DTOs are constructed by handlers |
| `pkg/adapter/httpapi/settings` | named presence-aware `PutRequest`, `GetResponse`, `PutResponse` | none; DTOs are constructed by handlers |
| `pkg/adapter/httpapi/modelsettings` | dedicated LLM DTOs; API-key omission, replace, and clear intent remain distinct, and output-token cap omission is distinct from replacement | none; DTOs are constructed by handlers |
| `pkg/adapter/httpapi/storage` | named administrative storage DTOs; explicit optional secret and session-only conditional-write intent, desired/active revision state, and read-only secret state/preview | none; DTOs are constructed by handlers |
| `pkg/adapter/sql/settings` | SQL `Store`, bounded `Options`, narrow `Cipher` contract, first-writer-wins conditional creation, and plaintext compare-and-swap guarded by the stored ciphertext | `New` |
| `pkg/adapter/sql/compositionstore` | SQL `Store` for the one validated runtime durable-host-state document; strict independent-revision CAS and a 16 MiB no-credential JSON boundary | `New` |
| `pkg/adapter/sql/effectjournal` | SQL `Store` for durable runtime forward-effect and inverse evidence, ordered by composition-local ordinal | `New` |
| `pkg/adapter/sql/fencejournal` | SQL `Store` for durable Fence request identity and monotonic completion/unknown evidence | `New` |
| `pkg/adapter/sql/artifactmigration` | SQLite/PostgreSQL `Store` for the v36 bounded resource-migration record, generation-fenced worker lease, and metadata-only mutation journal; it assumes schema initialization and owns no copier/backend client | `New` |
| `pkg/adapter/coreplugin` | Process-local, non-recoverable facade from legacy `core.Plugin` registry mounts into `runtime.Module`; declared provides are trusted compatibility metadata | `New` |
| `pkg/adapter/modelsettings` | encrypted settings-repository adapter preserving the additive legacy LLM JSON record and forwarding conditional creation only when its settings port supports it | `New` |
| `pkg/adapter/storageconfig` | encrypted generic-settings adapter preserving `storage.resources`/`storage.sessions` keys, guarded revision updates, and non-secret resolution evidence | `New` |
| `pkg/adapter/sql/sqlkit` | driver-neutral `Dialect` (`SQLite`, `Postgres`), `Valid`/stable `String`, and placeholder rebinding | `Bind` (returns an explicit error for an unsupported dialect) |

## `pkg/core` foundation budget

The historical 2026-09-04 baseline is 34 Go files and 8,615 non-blank physical
source lines. The reviewed 2026-09-07 exception accepts
`AgentProfileRegistry.ReplaceExact`, a core-owned,
single-layer publication primitive that preserves a mounted layer's order and
unmount closure while validating the detached final projection under one
registry lock; it cannot be implemented correctly in control or server alone.
The implementation measures 8,817 lines; its hard ceiling is 8,821, leaving
four fixed lines. This is not a rolling “current baseline + 128” rebaseline.
The file ceiling remains 36 and the gate rejects any individual non-test,
non-generated core file above 1,500 non-blank physical lines. Tests and
generated Go files are excluded; comments are counted and blank formatting
lines are not.

The same gate measures 910 public API items with `go/ast`: 369 exported
top-level names, 113 exported methods on exported receiver types, 380 named
exported struct fields on exported types, and 48 named exported interface
methods. The hard limit is 910, leaving no API slot. The final item is
`AgentProfileRegistry.ReplaceExact`. Embedded unnamed members are excluded by
design so the rule remains deterministic.

This is an architecture-growth guard, not an automatic compatibility proof.
The accepted Graph, context, composition, schema, and performance refactors
explain this measured baseline; they are not a reason for open-ended growth.
New domain capability should move to `pkg/runtime`, `pkg/extensions`, or a
specific adapter. In particular, the single-file limit prevents another
session- or capability-sized concentration in `pkg/core`; exceeding a ceiling
requires architecture review and a split of the owning concern out of the
stable foundation.

## `/v1` HTTP and OpenAPI surface

The OpenAPI document declares 104 operations total: `/healthz`, `/readyz`, and
102 `/v1` operations. The checked-in verifier confirms that all 102 `/v1`
operations are registered by `pkg/server`:

```text
go run ./scripts/verify-openapi openapi/harness-core-v1.yaml pkg/server pkg/console/static/index.html
OpenAPI contract verified: 102 registered /v1 operations
```

The JSON inventory contains the complete method/path/operation-id list. The
domains represented by those operations are:

- authentication and activation;
- sessions, durable events, synchronous and asynchronous runs, and cancellation;
- profiles, capabilities, releases, rollbacks, and canaries;
- object resources;
- private runner claim/renew/complete operations;
- approvals, tenant-scoped non-secret notification targets, evidence, delegations, RAG/memory projections;
- accounts, tenants, settings, audit, policies, bindings, credentials;
- observability rules/hits, backtests, metrics, tool-library observations;
- storage administration and evaluation datasets/runs.

The Go 1.22 mux pattern for resource keys is
`/v1/resources/{key...}`. The OpenAPI contract uses `/v1/resources/{key}`;
`scripts/verify-openapi` normalizes the former before comparing them. This is
an intentional compatibility detail to preserve during transport extraction.

## SSE event names

Synchronous run streams use the durable `core.SessionEventType` values:

```text
run/start          run/resume          run/end             run/error
run/usage          step/start          step/end            step/error
user/message       assistant/message   assistant/chunk     tool/call
tool/result        approval/requested  approval/resolved   context/summary
```

Transport-only failure events are also emitted by the current server:
`store/error` and `control/error`. The stream has no event IDs or server-side
replay. A disconnected client replays durable events through
`GET /v1/sessions/{id}/events?after_seq=...`.

## SQL schema and migration chain

The durable schema is currently version **44**, exposed as
`storage.SQLSchemaVersion`. The complete v1-v44 history and tables introduced
or projected at each step are recorded in the JSON inventory. The current
canonical table set is:

```text
store_meta, sessions, event_chunks, session_leases, profile_releases,
memory_entries, rag_documents, accounts, auth_tokens, tenants, settings,
audit_events, obs_rules, obs_hits, admin_bindings, run_stats, run_control,
run_queue, tool_invocations, approval_requests, run_submissions,
evaluation_datasets, evaluation_runs, evaluation_case_results,
profile_canaries, run_evidence, runner_tasks, rag_document_tokens,
rag_document_tags, memory_entry_tags, tool_library_observations,
delegation_links, runtime_effect_sequences, runtime_effects,
runtime_host_state, runtime_host_ownership, runtime_fence_journal,
artifact_migrations, artifact_migration_mutations, graph_checkpoints,
graph_checkpoint_versions, graph_transitions, notification_targets,
graph_segment_leases, completed_tool_result_recovery_sidecars,
native_queued_tool_effect_witnesses
```

Versions 26-30 include projection/index and retryable migration steps even
when they do not add a canonical table. Versions 27-29 add rebuildable RAG,
Memory, and tool-library projections. Version 31 adds metadata-only
delegation links. Version 32 separates durable account identity from optional
email metadata and adds the account-token lookup index. Version 33 adds the
runtime effect journal; version 34 adds the single runtime host-state CAS
document with an independent revision column; version 35 adds durable host
ownership and request-idempotent Fence journal rows; version 36 adds the one
resource-migration slot and metadata-only mutation journal; version 37 adds
Graph checkpoint and transition evidence; version 38 adds encrypted
notification-target metadata; version 39 adds generation-fenced Graph segment
leases; version 40 adds `approval_requests_run_status` and `run_control_stale`
indexes; version 41 adds `graph_checkpoint_versions`; version 42 adds the
`store_meta.authorization_epoch` row; and version 43 adds immutable SQL-local
`completed_tool_result_recovery_sidecars`; version 44 adds generation-scoped
`native_queued_tool_effect_witnesses`. The V43 sidecar is historical
delivery evidence only: it does not grant continuation or make automatic
recovery available. A v44 witness records epoch-fenced queued effect admission
and journal Begin, not provider completion, current account state, or recovery eligibility. The v41 model is one
mutable CAS-protected `graph_checkpoints` head plus append-only versions and
transitions. A v40 upgrade backfills only its current head as a
`migration_floor`; it never manufactures older history. Existing schema names,
columns, and historical migration order are compatibility constraints.
SQLite v41-to-v44 epoch/sidecar/witness migration plus v41 fresh/upgrade/backfill evidence is covered by the storage suite.
PostgreSQL v35-v41 evidence passed on 2026-09-04 against isolated disposable
schemas using an administrator-capable `HARNESS_TEST_PG_DSN`; the v41 pass
explicitly covered history upgrade, atomic rollback, replay, and competing
CAS. A local listener alone establishes none of those facts.

## External compile compatibility fixture

[`internal/modulecheck/external_compatibility_test.go`](../../../internal/modulecheck/external_compatibility_test.go)
uses `package modulecheck_test`, imports the public module paths as an external
consumer would, and checks representative contracts and constructors from
`core`, `storage`, `server`, `control`, `evaluation`, and `execution`. It does
not open a database, start an HTTP server, or appear in any production import
graph.

The fixture intentionally checks interface satisfaction for a third-party
LLM, SessionStore, ObjectStore, Authenticator, CanaryStore, Evaluation Store,
and the public SQL `FenceJournal` adapter (`fencejournal.Store` via
`fencejournal.New`). Breaking a currently supported method or constructor
signature fails at compile time.

## Baseline status and limitations

This inventory and fixture are Phase 0 baseline artifacts plus the additive M1
registration metadata seam. They do not claim that the full migration is
complete. The provider and protocol packages intentionally expose only typed
runtime metadata; model catalog/router/auth/protocol compatibility remains an
M2 responsibility. Open items remain public API inventory review with
downstream consumers, historical PostgreSQL fixture execution, black-box
locking for every critical invariant, and later tightening of legacy-package
dependency rules as each migration phase lands.

## Accepted additive runtime inventory (2026-09-04)

The following bounded runtime slices are accepted/green in the integrated
workspace: M1b lifecycle/effect ownership, M1c SQL EffectJournal schema v33,
M1d1 durable state and memory CompositionStore, M1d2 SQL CompositionStore
schema v34, M1d3 durable apply/promote/deactivate, M1d4 explicit startup
recovery, and the bounded v35 host-ownership/Fence/lifecycle slice. Evidence
includes full Go test/vet/modulecheck, repeated runtime and integration tests
(`-count=10` and race variants), actual PostgreSQL v34
CompositionStore/EffectJournal/migration tests, and SQLite v35
fresh/historical/fresh-handle integration. The fresh SQLite handle models a
persistence boundary; it is not an OS cross-process test.

The stop-the-line M1 completion gate is accepted/green for the contracts,
validators, lifecycle/effect/ownership/Fence gates, bounded process-local
`core.Plugin` compatibility facade, provider/protocol metadata seams, public
API budget, and modulecheck evidence. This M1 completion boundary does not
claim server auto-wiring, full legacy-plugin migration/removal, M2 model
control, external exactly-once, or completion of the remaining historical
PostgreSQL fixture gaps. The v35 SQL Fence authorization/audit path and
PostgreSQL DSN-specific Fence/ownership verification are covered by the
2026-09-04 disposable-schema tests; a future run without
`HARNESS_TEST_PG_DSN` remains an explicit skip.
TTLs above the 30s default and legacy `HostGeneration=0` do not guarantee
external exactly-once. The `core.Plugin` facade is process-local,
non-recoverable, permits no durable effects, and never replays legacy
`Install`; full legacy-plugin migration/removal remains later work. The
accepted Provider/Protocol slice is limited to independent registration
metadata projected into runtime collection extensions; it does not bind
providers to protocols or introduce transport, catalog, router, or auth
behavior. Inverse exactly-once behavior depends on real executors being
idempotent by `EffectID`; recovery owner local cleanup is best-effort and
`Deactivate` must be idempotent. The runtime slices add no background recovery
goroutine or automatic server wiring.

Graph-G0 is also accepted/green: focused, repeated, and race Graph tests,
`go vet`, the stdlib-only modulecheck, the external compile fixture, and the
machine-readable inventory parse are green. This is historical **G0-only**
evidence for the static definition/context/state/redaction contract and
deterministic top-level JSON patch; it is not evidence that the later G1
executor was accepted.

The subsequently added G1 executor/checkpoint slice remains experimental, not
accepted/green. The current workspace does contain the v37 durable checkpoint
adapter, executor-to-store adapter, protected core turn boundary, and explicit
profile-selected server composition. That server-facing adapter wraps one core
`RunTurn` node rather than an arbitrary multi-node business graph. The complete
Graph-G1 acceptance matrix, a production multi-node workflow, and broad
end-to-end load evidence remain absent.
