# AI Agents in Depth Gap Matrix for Harness Core

Date: 2026-09-02  
Method: read-only architecture audit of Harness Core and the local Chinese
edition of AI Agents in Depth. Page references are the book's printed pages,
not PDF viewer indexes. This document records engineering implications without
copying book passages.

## Status vocabulary

- **已有**: a production-shaped seam and its behavior already exist.
- **部分**: useful primitives exist, but lifecycle, safety boundary, or
  composition seam is incomplete.
- **缺失**: no credible current seam; defer until earlier gates permit it.
- **设计不当**: a current seam creates a correctness or security boundary that
  must be repaired before extending it.

## Evidence matrix

| Book subject and reference | Current Harness evidence | Status | Recommended Port / registry / lifecycle | Kernel versus plugin |
| --- | --- | --- | --- | --- |
| Agent brain, context, tools, Harness constraints (ch. 1, p. 28) | core runtime/agent and capability funnel; README kernel/capability model | 部分 | unified Trigger to Event chain; Orchestrator | event/schema/permission/approval/budget kernel; orchestrator plugin |
| Stable prefix and prompt/KV cache (ch. 2, p. 48) | profile PromptFragment/SystemPrompt stable ordering; no provider cache hints | 部分 | ContextAssembler, ContextSegmentProvider, CachePolicy, PromptCacheHints | ordering/snapshot/bounds kernel; cache implementation/hints plugin |
| Dynamic Prompt, Skills, progressive disclosure (ch. 2, p. 56) | Skill/Prompt kinds; tool library disclosure; no SKILL.md source/catalog | 部分 | SkillCatalog, SkillSource, SkillResolver, SkillActivationPolicy | source/activation plugin; revision/bounds runtime |
| State bar (ch. 2, p. 60) | no model-visible structured runtime-state provider | 缺失 | RuntimeStateProvider to bounded StatusSnapshot | trusted tail placement runtime; state providers plugin |
| Compaction and isolation (ch. 2, p. 68) | ContextCompactor, RecentTurnsCompactor, RunSummarizer, RollingSummarizer; cmd currently enables only compactor | 部分 | existing compactor/summarizer plus IsolationPolicy | canonical source/bounds kernel; policies plugin |
| Memory lifecycle (ch. 3, p. 77) | extensions/memory Store, slice/SQL stores, scoped tool capability | 部分 | MemoryCandidateExtractor, MemoryProposalStore, MemoryReviewer, MemoryRetriever | scope/write evidence kernel; extraction/review plugin |
| RAG retrieval/provenance (ch. 3, p. 99) | extensions/rag Index, keyword/SQL indexes/projections; no embedding/rerank/citation policy | 部分 | DocumentSource, Chunker, Embedder, Retriever, Reranker, CitationPolicy | scope/provenance snapshot kernel; indexing/retrieval plugin |
| Tool taxonomy, discovery, MCP (ch. 4, p. 118) | core capability manifest/schema/permission/approval/journal; stdio MCP client; toollib catalog | 已有 / 部分 | CapabilityProvider, ToolCatalog, ToolSearcher, executor/MCP adapters | protected invocation is kernel; discovery/execution are plugins |
| Coding safety and recovery (ch. 5, p. 125) | Wazero, OSConfinementExecutor, retry, interrupted-session repair, journal/lease; no coding workspace/patch verifier vertical | 部分 | Workspace, Sandbox, CommandExecutor, PatchApplier, Verifier, RecoveryPolicy | fence/journal/no opaque retry kernel; sandbox/verifier plugin |
| External event, channel, interruption (ch. 6, pp. 157-158) | queue/lease/generation fence, SSE, durable approval; no webhook/cron inbox/channel abstraction or grammar-aware interrupt | 部分 | EventSource, EventInbox, Scheduler, Channel, InterruptPolicy, Delivery | event order/dedup/lease/cancel kernel; source/delivery plugin |
| Evaluation, observation, AB, simulation (ch. 7, pp. 206-210) | evaluation package, profile release/canary, core telemetry and OTEL; no generic flags/multi-arm/simulator | 部分 | ExperimentRegistry, Assignment, FeatureFlagResolver, Simulator, TelemetrySink | assignment/artifact evidence/release gates kernel-control; algorithms/sinks plugin |
| Continuous evolution/release (ch. 9, pp. 253-265) | profile release/canary/evaluation/rollback; no proposal/diff lifecycle across knowledge/prompt/skill/program/model | 部分 | EvidenceLink, ChangeProposal, ArtifactRepository, Validator, Publisher | approval/snapshot/rollback facts kernel-control; proposal generators plugin |
| Multi-agent topology/isolation/A2A (ch. 10, pp. 266-267, 295) | extensions/subagent has child sessions, bounds, durable delegation links; no peer bus, handoff workspace, fanout/join, A2A endpoint | 部分 | AgentSpawner, Handoff, AgentMessageBus, Topology, Workspace | scope/budget/cancel/journal/fences kernel; topology/A2A adapters plugin |
| LangGraph-style graph orchestration | workflow extension is sequential; core has run/resume/approval primitives but no versioned graph state, reducer, edge registry, or graph checkpoint | 缺失 / 可并行设计 | versioned GraphDefinition, StateSchema/Reducer, NodeRegistry, EdgeRegistry, GraphCheckpointStore, GraphOrchestrator | graph is an Orchestrator extension; Guarded Commit/ModelCallGate/checkpoint facts remain host boundaries |
| ContextPlan and node-local context | compaction/summarizer primitives exist, but no per-node ContextView/ContextBudget, typed source isolation, token accounting, or plan evidence | 部分 | ContextPlan, ContextView, ContextBudget, TokenEstimator, ContextCompactor, SummaryStore, IsolationPolicy | EventLog fact source and bounded context/revision checks are foundation; materializers and policies are extensions |
| Runtime configuration source of truth | settings/bootstrap paths and legacy environment loading exist; LLM/provider/router/storage ownership is not yet one DB-authoritative vertical | 设计不当 / M0c | DB-backed application settings/model-control records; `config_source` evidence and explicit import/fallback | only DB connection/listener/data directory/external root-key reference are bootstrap; env cannot override existing DB rows |
| Storage DB source of truth | main still gives `HARNESS_S3`/`SESSION_DIR` precedence; storage loader treats malformed values as absent | 设计不当 / M0d (after M0c) | typed ConfigSource/ConfigStatus/resolution evidence; resources/sessions row-authoritative resolver; desired/active apply status | row presence, including embedded/S3/inactive/malformed, is authoritative; env only fills absent rows |
| Sandbox execution and assurance | current executors exist, but no generic provider/session contract for assurance, limits, network, mounts, artifacts and leases | 缺失 / S0 | `pkg/execution/sandbox` Provider/Session/Assurance/Limits/Network/Mount/Artifact/Lease | Guarded Commit, journal and fenced lease are host boundaries; sandbox adapters are plugins |
| Extreme concurrency/performance | reproducible 10/100/500 plus soak baseline, Windows RSS/CPU sampling, and first hotspot optimizations are integrated; main-session final integrated-state validation remains | 部分 / Perf-P0-P1 | benchmark harness, payload/context scenarios, sandbox-child line item, bounded telemetry | measurement and safety facts are foundation; admission/backpressure/429 are explicitly not part of the current gate |

## Current model call-chain audit

~~~text
AgentProfile.ModelSelection
  -> core.Runtime.Models.ResolveModel
  -> core.LlmAdapter.Stream(GenerateOptions)
  -> provider/openai OpenAICompatibleAdapter
  -> OpenAI-compatible /chat/completions
~~~

| Layer | Current evidence | Status / gap |
| --- | --- | --- |
| Provider | pkg/provider/openai currently mixes vendor endpoint/auth policy with OpenAI-compatible wire construction | 设计不当: no provider catalog/auth registry and vendor/protocol are conflated |
| Protocol | core LlmAdapter, GenerateOptions, StreamChunk, consumeModelStream | 已有: retain stable public protocol |
| Catalog | ModelSelection only contains Provider and Model strings | 缺失: no record, wire model, status, revision, capability data |
| Registry | Runtime.Models is one resolver closure; PluginHost cannot mount providers | 缺失 |
| Auth | CredentialResolver is scoped, but ModelResolver has no principal/scope; cmd generic setting holds raw LLM config | 部分 / 设计不当 |
| Capability | OpenAI tool streaming plus AllowedModels only | 部分: no resolved compatibility matrix |
| Router | cmd resolver reads legacy setting/environment | 设计不当: selected and actual model can diverge |

The Router row is also a configuration-source issue: after M0c, the effective
LLM/provider/protocol/catalog/auth references, allowed models and router policy
come from DB rows. `.env` is permitted only for an absent row's legacy fallback
or one-time import, and the effective source is recorded as
`bootstrap`, `db`, `env_import` or `legacy_fallback`. A present but inactive or
malformed DB row must fail visibly; it must not silently fall back to or be
overridden by `.env`. Credential CAS/revision remains an M2 model-control
domain concern.

### M0d: storage DB source-of-truth

M0d follows M0c and must not be inserted before it. Current evidence requires
repair: the main composition still prefers `HARNESS_S3` and `SESSION_DIR`, and
the storage loader maps malformed configuration to absent. The M0d contract
introduces typed `ConfigSource`, `ConfigStatus` and nonsecret resolution
evidence. For resources and sessions, row existence is authoritative for
embedded, S3, inactive and malformed rows; `.env` is consulted only if the row
is absent. A later one-time import may validate and encrypt legacy environment
values before creating a row.

Desired configuration and active/applied status are distinct. A failed dynamic
resource swap keeps the previous resource active and records the apply failure
against the desired row. A session change becomes restart-pending rather than
being silently applied to a running session. Worker, runner, approval and
telemetry environment-to-DB migration remains later work. Secret write-only,
preview and redaction rules do not regress.

### M0: selected model versus actual wire model

cmd/server resolves non-mock models from persisted llm configuration or
environment. The OpenAI adapter serializes its configured model, while
core.Runtime records profile ModelSelection in composition and telemetry. An
httptest OpenAI-compatible endpoint with different synthetic configured and
selected model names proves the discrepancy without a live endpoint or
credential.

Minimum compatible repair: retain core.ModelResolver and reject a
legacy-config/profile mismatch at the existing compatibility boundary. M0 does
not introduce CatalogResolver. Target M2 repair: resolve a contextual
ModelSelection (principal and scope) to immutable nonsecret ModelSpec and
ProviderPlan, then create the adapter from its exact WireModel and credential
binding revision.

### M0: settings secret read and preview masking

The generic settings endpoint now fails closed for every key: it never returns
the decrypted setting value, and a found key is explicitly marked
redacted/write_only. This covers the legacy composite `llm`, storage composite
settings, and arbitrary names such as `oauth_config` and `config`; domain DTOs
own any bounded recognition preview. Generic PUT requires a present, non-null
`value`, while an explicit empty JSON string remains valid. Independently, the
preview rule is first three characters + a fixed mask token + last four
characters; values shorter than eight characters are fully hidden. The fixed
preview is the only permitted recognition information: do not additionally
return a hash, fingerprint, digest, or reversible derivative, and do not reveal
the original total length or complete value.

The Console does not hydrate the preview into a form, re-submit it, or treat it
as a secret. Storage writes are explicit: omitted preserves, non-empty replaces,
clear_secret true deletes; providing both a non-empty value and clear is
rejected. The former `••••` value remains an inbound compatibility mapper only;
persisted historical markers remain unusable and are omitted from previews.
Secret CAS/revision belongs to the M2 model-control domain service, not M0
generic settings. Regression coverage is in
`pkg/app/settings/service_test.go`, `pkg/server/server_settings_test.go`, and
the storage/Console tests.

### Graph Orchestrator slice

Graph is deliberately not a core or ModuleHost feature. The first runnable
vertical belongs in `pkg/extensions/graph` and requires the M1 host seams:
immutable module/composition snapshots, dependency validation, durable
EffectJournal, lease/fence/drain/deactivate, Guarded Commit with host-issued
AcceptedInvocation, and an independent ModelCallGate. The Graph contract may
be designed in parallel after those Ports are frozen, but activation waits for
the M1 negative bypass tests.

The minimum contract has a versioned definition, state schema, deterministic
reducer, node and edge registries, explicit default/conditional/error edges,
bounded cycles and visits, retry/timeout/cancel policy, checkpoint CAS,
approval interruption, and resume into a new execution segment with previous/
new composition revisions and authorization basis. Every node declares a
ContextView and ContextBudget; it receives current input, required state
fields, pending approval/tool invariants and bounded episode/summary/artifact/
retrieval layers, not global Graph state/history. EventLog is fact source;
ContextView is materialized per node.

Guarded Commit and ModelCallGate are mandatory and non-bypassable. Tool/user
communication cannot be executed directly by a node; model outbound calls are
not tool effects; persistence uses an app use case/UoW. Trusted adapters may
hold transports, untrusted modules may not. The first graph may use a fixture
ModelCallGate until M2 provider/protocol integration, but it must still run
the same seam.

Observability is part of the contract, not a post-M1 enhancement: bounded
events/traces/metrics cover graph, node, edge, state patch, checkpoint, retry,
approval, cancel and error. State is redacted; context-plan revision, token
estimate/by-layer allocation and compaction reason are observable metadata,
never raw state or secret-derived values. Since Session event types are
validated, Graph events require a validated extension-event registration seam
or a Graph envelope and may not bypass Session.Append.

Graph validator/test coverage must include schema/reducer determinism, edge
ambiguity, cycle/visit/step budgets, retry/timeout/cancel bounds, checkpoint
serialization/CAS and crash windows, revision mismatch, approval segment
resume, effect-gate negative cases, context source isolation, and event/trace/
metric redaction. Fan-out/join, dynamic graph mutation, editor/DSL,
distributed scheduling and subgraphs are post-vertical work.

### ContextPlan slice

ContextPlan is a derived node-local view. EventLog is authoritative and the
checkpoint is the execution fact state; ContextView is disposable materialized
input. The contract must include typed sources and source revisions, segment
and per-layer budgets, token estimator, output reserve, stable KV-prefix
ordering, and compaction reason. Its validator checks budget arithmetic,
source identity/revision, isolation, required invariants and redaction.
Required invariants include current input, schema-required
state fields and pending approval/tool pair. History is layered as episode,
tool, approval, summary, artifact and retrieval; memory/RAG remain isolated
with scope/provenance. Durable hierarchical summaries require source revisions
and cannot recursively drift.

Implementation order is Contract + Validator, structured history, budgeted
compaction, durable summary, then source isolation. A node never implicitly
receives global history. Plan revision, token estimate, layer allocation and
compaction reason are recorded as bounded nonsecret evidence.

### Sandbox-S0 slice

The generic sandbox contract belongs in `pkg/execution/sandbox`, outside core,
Graph and ModuleHost. Its Sandbox Provider identity is distinct from the model
Provider identity. It covers Provider/Session, requested and actual
Assurance, Limits, Network, Mount, staged Artifact and fenced Lease. The
default is local ephemeral with warm-pool size zero. Current Linux bwrap means
process confinement on a shared kernel; network is not isolated. Windows is
currently unavailable and must not be advertised as an equivalent backend.
Future container, microVM, cloud, self-hosted and remote adapters must report
their own actual assurance.

Guarded Commit compares requested versus actual assurance and fails closed on a
missing/weaker level, unenforced limits, unmet network policy or unavailable
adapter. AcceptedInvocation, ToolInvocationJournal, fenced sandbox lease and
staged artifact records share run/segment/module composition evidence. Artifacts
become visible only after verification and explicit commit. Timeout/cancel
fences the lease and journals child-process outcome; unknown outcomes are
quarantined for reconciliation. Untrusted modules cannot hold process handles,
raw transports or root mounts.

### Performance P0-P1 slice

The reproducible 10/100/500-concurrent plus soak baseline is now implemented,
including Windows working-set and CPU observations. First hotspot optimizations
are also integrated, but their combined state still awaits the main-session's
final verification gate. This status creates no new admission, semaphore,
queue, backpressure, rate-limit, or 429 behavior: 500 remains 500 genuinely
live executions, with results split by payload/context and sandbox-child
RSS/startup separately.

Two GC-isolated production-shaped 500-concurrent Agent rounds report 161–198 MiB
sampled heap peak and 162.6–200.1 ms p95 for the typical 16 KiB payload, and
1.54–1.76 GiB sampled heap peak and 2.83–2.94 s p95 for the max 1 MiB payload.
Both Agent rounds had zero errors. The max/500 case RSS varied from 1.84–3.53 GiB
across those same-process rounds. It is affected by preceding cases and Go
arena retention, so it is not an isolated per-case or
per-execution resident-memory claim.

The two 500-concurrent HTTP `/healthz` rounds must remain distinct from the
Agent result: one had zero errors, while one had 200 transport errors. This is
not evidence that 500 HTTP is stably passing.

The old 384 MiB 500-concurrent value was provisional rather than a pass/fail
gate and is invalid for the max scenario. 500 * 1 MiB is already 500 MiB of
raw information before copies, context, headers, maps, runtime overhead, and
allocator/GC retention. No replacement target or hidden load shedding is added
here.

Implemented P1 work includes default no-summary fused session event projection
and recent-turn compaction, incremental projection cache/read helpers,
immutable tool-library indexing with a 240-rune bounded result summary,
adaptive runner idle polling, and normal File/S3 resource streaming GET and
PUT. A 16 MiB streaming benchmark reported about 60 KiB/op File and about 168 KiB/op
S3 Go-heap allocation, so payload does not linearly enter the heap.
File/S3 PUT still retains required integrity/replacement semantics through
bounded temporary storage; the legacy cold compressed-session fallback still
materializes its historical layout.

For an 8,192-event / 128-message-window microbenchmark, legacy
`DeriveMessages` then `Compact` used about 1.16 MiB and 10,754 allocations per
operation, compared with about 65.5 KiB and 1,061 for fused projection. The
separate 500-concurrent tool search cases were p95 about 4.32 ms for 128 tools
and about 7.33 ms for 512 tools; post-GC retained catalog-index observations
were about 0.24 MiB and 2.76 MiB. Construction is excluded from search
throughput, so this is not a compact-resident-index claim.

An isolated temporary dev/bootstrap SQLite `cmd/server` short measurement,
after its build process had exited, recorded idle over 20 seconds at Working
Set max 19.69 MiB, private memory max 19.04 MiB, and normalized CPU 0%; no
public goroutine metric is exposed. A separate 20-second single-client
`/healthz` sample reached Working Set 21.95 MiB, private memory 21.11 MiB, and
normalized CPU 0.0039%, with 96 successful requests and client p95 1.778 ms.
It is a short idle/light-load observation, not a long soak or a 500-HTTP
stability claim.

Remaining P1 evidence/work is artifact-reference handling for large results,
a compact resident tool-index representation, and ContextPlan token budgeting
with hierarchical summaries. Observe RSS/heap/CPU/goroutines/GC, DB/S3 and
child-process metrics with bounded payload/context dimensions and remeasure
identical scenarios after every further optimization.

For N live executions, payload P, materialized context C, retained metadata H,
runtime overhead R and sandbox-child RSS X, record the information lower bound:

~~~text
RSS_min(N,P,C,H,R,X) >= host_base + N * (P + C + H + R) + X
~~~

This is a sanity bound, not permission to retain unlimited history. If a target
is not met, publish the measured miss and scenario; do not hide it with load
shedding or a new concurrency gate.

## Extension semantics gap

Current core.Plugin registration is a partial collection-style installer for
capabilities, profiles, credentials, and policies. It has no explicit semantic
for pipeline ordering, scoped overlays, single-strategy conflicts, or stateful
resource draining. M1 introduces these five registry semantics:

| Semantic | Current gap | Required control |
| --- | --- | --- |
| Collection | duplicate and priority behavior is not a general module contract | stable IDs, deterministic order, atomic record swap; semantic-specific validator |
| Pipeline | no general phase/order/cycle contract | explicit contract/version, phases and ordering edges, cycle rejection, per-segment snapshot; semantic-specific validator |
| Scoped Overlay | scope/profile selection exists in places but not as one registry rule | each registry defines its keyspace and merge rule; only ScopePath ancestry orders overlays, with permission intersection and frozen composition |
| Single Strategy | no standard owner replacement protocol | one role owner, stage/health/swap/drain |
| Stateful Resource | no module-level durable effect/lease/reconcile contract | actual inverse-effect records for Stage/Activate/Reconcile, leases, forced fencing, desired-state reconciler |

## Current package and dependency constraints

1. pkg/core is intended to be standard-library-only, but currently includes
   runtime/context/plugin mechanics as well as foundation vocabulary. Extract
   incrementally into pkg/runtime through aliases, not a one-shot import break;
   add an explicit core public API/size budget so domain registries do not grow
   the trust root.
2. docs/architecture.md documents the current downward dependency graph. The
   approved cmd -> adapter/httpapi -> app -> core/runtime target must remain
   modulecheck-enforced.
3. Approximate Go size makes vertical work safer than a horizontal rename:
   storage is about 25k lines, server about 16k, core about 11k, and extensions
   about 6k.
4. The Console is local embedded assets. Its storage setting reload clears
   stale secrets; future model settings may show only the bounded recognition
   preview, never hydrate a credential, and never re-submit a preview.
5. Agent-proposed tool/user communication, model outbound calls, and
   repository/control-plane persistence use separate effect boundaries:
   AcceptedInvocation/Guarded Commit, ModelCallGate, and application
   use-case/Unit of Work respectively. Trusted adapters may hold transports;
   untrusted modules may not.

## Migration gate checklist

| Priority | Work | Acceptance evidence |
| --- | --- | --- |
| M0 | selected/wire model alignment and secret stop-the-line; no CatalogResolver or secret CAS | httptest wire assertion, HTTP sentinel scan, bounded preview/short-secret/Console no-resubmit tests, omitted/replace/clear and ambiguous-input tests |
| M0c | LLM runtime configuration DB vertical before M1; DB precedence, absent-row-only env fallback/one-time import, `config_source` evidence, visible inactive/malformed-row failure; no CatalogResolver or credential CAS | fresh/restart DB read, import/fallback, DB-present precedence, malformed/inactive failure, source audit/telemetry, no silent env override |
| M0d | storage DB source-of-truth after M0c; typed ConfigSource/Status/resolution evidence; resources/sessions row-present authoritative for embedded/S3/inactive/malformed; env only when row absent; later worker/runner/approval/telemetry migration | HARNESS_S3/SESSION_DIR precedence regression, malformed-as-absent regression, row-present precedence, validated encrypted import, desired-vs-active swap, session restart-pending, source evidence and secret redaction | retain legacy storage loader/main composition; disable DB storage activation/import; preserve old active resource on failed swap and pending session restart |
| Sandbox-S0 | provider/session/assurance/limits/network/mount/artifact/lease contract and local assurance report; ephemeral default, warm pool 0; Linux bwrap network-not-isolated; Windows unavailable | requested/actual assurance fail-closed, limits/network/mount, Guarded Commit/journal/fenced lease/staged artifact, child timeout/cancel/reconcile tests | disable sandbox capability and retain existing executor; never claim unavailable assurance |
| Perf-P0/P1 | baseline plus first hot-path optimizations are implemented; final integrated-state validation remains pending, with no replacement performance gate | real 10/100/500 and soak, payload/context split, sandbox-child separate, RSS/heap/CPU/goroutine/GC/DB/S3 telemetry, information lower-bound report, and same-workload regressions | publish honest miss; no admission/weighted semaphore/queue/backpressure/429 gate |
| M1 | minimum trusted module host, three effect boundaries, five semantic validators, and Provider/Protocol registration seams | dependency DAG/cycle, lease identity, health/snapshot, durable Stage/Activate/Reconcile effect-inverse, crash-window/restart, drain/fence/deactivate, AcceptedInvocation/ModelCallGate negative, approval segment-resume, old/new duplicate registration tests |
| M2 | model-control vertical; contextual ProviderRegistry/ProviderPlan, credential CAS/revision, OpenAI Chat migration plus Responses/Anthropic protocol paths | app/DTO/factory tests, endpoint/signer binding, principal/scope and credential rotation tests, same-provider/multi-protocol and shared-protocol/multi-provider tests, exact composition evidence, external ModelResolver fixture |
| M3 | context/Skill/memory/RAG | prefix/cache, source revision, retrieval provenance/scope tests |
| M4 | executor/sandbox/events | cancellation, ambiguous outcome, no duplicate effect, verification/recovery tests |
| M5 | experiments/evolution/multi-agent | deterministic assignment, gated release, structured handoff/cascade/budget/cancel tests |

All implementation gates require target tests, full tests, vet, modulecheck,
OpenAPI verification, public compile fixtures, and SQLite/PostgreSQL migration
checks where a PostgreSQL DSN is configured. A PostgreSQL skip is reported as a
skip, never as a pass.

The M1/M2 modulecheck rules must verify that the legacy provider facade only
forwards to the new implementation, modelprovider does not own protocol wire,
modelprotocol never reverse-discovers a provider, untrusted modules cannot hold
raw transports, and old Plugin plus new Module cannot register the same stable
ID. Lifecycle tests inject crashes between effect and journal record, snapshot
publication and old-module drain, and desired-state reconciliation; restart
must surface an unknown effect rather than invent one.
