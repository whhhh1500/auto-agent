# Harness Core Agent Foundation and Plugin Architecture V2

Date: 2026-09-02  
Status: approved architecture direction; implement only through the M0-M5 gates in this document

## 1. Purpose and scope

Harness Core is an Agent foundation, not one fixed agent product. It must make
a safe default agent easy to compose while allowing a product to replace model
providers, knowledge sources, tools, execution environments, orchestration
policies, and operator experiences without bypassing correctness or security.

This V2 specification refines the approved pluggable-architecture refactor. It
does not reset SQL data, require a graph runtime, or break the current /v1
contract by default. It defines the missing runtime, module, and model-control
boundaries so that migration can be incremental and reversible.

The local Chinese edition of AI Agents in Depth is engineering input rather than
an implementation to copy: brain/context/tools and Harness constraints
(chapter 1, p. 28), stable prefixes (chapter 2, p. 48), Skills (p. 56), state
bars (p. 60), isolation before compaction (p. 68), memory/RAG (chapter 3,
pp. 77 and 99), tools (chapter 4, p. 118), recovery (chapter 5, p. 125),
interruption (chapter 6, pp. 157-158), evaluation (chapter 7, pp. 206-210),
evolution (chapter 9, pp. 253-265), and multi-agent handoff (chapter 10,
pp. 266-267 and 295).

## 2. Agent, Environment, and Harness

| Boundary | Owns | Must not own |
| --- | --- | --- |
| Agent | selected profile, model selection, context assembly, orchestration choice, requested capabilities for one run | databases, raw credentials, host execution, global policy mutation |
| Environment | external systems and effects: repositories, networks, sandboxes, workspaces, channels, queues, clocks, model endpoints | scope/permission law, canonical event grammar, approval decisions |
| Harness | composition, transition validation, scope/permission/budget/approval enforcement, evidence and lifecycle | product prompt wording, provider SDK semantics, tool business rules, mandatory graph topology |

An Agent requests work. An Environment performs allowed external work through
an adapter. The Harness decides whether a request is valid, bounded, observable,
and durable. No Agent, module, or adapter may bypass the Harness from a model
request to an Environment side effect.

There are three distinct effect boundaries; they are not one universal effect
API. An Agent-proposed tool invocation or user-communication delivery must pass
Guarded Commit and receive a host-issued, unforgeable `AcceptedInvocation`
before an executor or channel can perform it. A model outbound request passes a
separate `ModelCallGate` for routing, capability/step budget, credential
resolution, timeout, and telemetry. Repository and control-plane persistence
uses the owning application use case and its Unit of Work; it is not a tool
effect. A trusted adapter may hold its transport and perform the accepted
operation. An untrusted module receives only typed Ports and accepted tokens;
it cannot hold a raw transport or manufacture an accepted token.

## 3. Microkernel trust root

The eventual stable core is a trust root of value objects and verification
contracts, not a feature kitchen sink. It owns:

- identifiers, scope ancestry, principals, permission intersection, and
  redaction vocabulary;
- validated canonical session events and immutable composition/snapshot
  identity;
- tool call/result/invocation, approval, journal, queue/lease/fence, and
  streaming protocol contracts;
- hard limits and typed error categories; and
- Ports required to preserve these laws.

The following laws are non-pluggable:

1. Scope only moves downward and permission intersections fail closed.
2. Every capability call goes through manifest/schema, authorization,
   credentials, budgets, timeout, approval, journal, and telemetry.
3. An unknown non-idempotent outcome fails closed; a stale generation or
   expired lease cannot commit.
4. Events are validated, append-only, bounded, ordered, and redacted before
   public observation.
5. Every execution segment is bound to an immutable composition; registry
   mutation cannot silently change that segment. A logical Run may contain
   multiple explicitly evidenced segments across approval resume.
6. Credentials never appear in public manifests, run composition, logs,
   events, telemetry, or read responses.

The effect-boundary laws are enforced by the three gates above: nested tool and
user-communication calls re-enter Guarded Commit; model calls re-enter
ModelCallGate; application persistence re-enters its Unit of Work. A package
dependency check alone is not evidence of this property.

The generic run loop, context policies, profile composition, plugin mounting,
and repair mechanics move gradually to pkg/runtime. Core wrappers or aliases
preserve old imports until an explicitly approved major-version removal.

## 4. Unified execution chain

Every CLI, HTTP, webhook, scheduled, queue, and subagent run normalizes to one
observable chain:

~~~text
Trigger
  -> Context
  -> Orchestrator
  -> Guarded Commit
  -> Executor
  -> Verifier / Corrector
  -> append-only Event
~~~

| Stage | Responsibility | Allowed extensions | Harness commitment |
| --- | --- | --- | --- |
| Trigger | authenticate, deduplicate, bind subject/scope and queue semantics | HTTP, CLI, webhook, cron, broker, parent handoff | no run before principal, scope, idempotency, and limits are known |
| Context | ordered bounded input from profile, history, summaries, memory, RAG, Skills, trusted state | ContextSegmentProvider, SkillResolver, Retriever, RuntimeStateProvider | static prefix is deterministic; untrusted input is labelled/bounded; snapshot is recorded |
| Orchestrator | choose the next action | sequential loop, workflow, planner, reviewer, graph | proposes actions; never effects work directly |
| Guarded Commit | validate a proposed model/tool transition before external or durable work | policy, approval, budget, credential resolvers | one protected checkpoint for model choice, schema, permission, budget, approval and journal identity |
| Executor | perform allowed model/tool/sandbox/MCP/Runner/channel work | provider adapter, executor, sandbox, channel | effect identity and cancellation/timeout bind to accepted request |
| Verifier / Corrector | validate output and choose bounded retry, repair, escalation or rollback | Verifier, Corrector, RecoveryPolicy, evaluator | no hidden retry after ambiguous non-idempotent work; corrections add evidence |
| Event | append result/transition and publish views | stores, SSE, telemetry, audit | grammar, redaction, ordering, fences and retention are authoritative |

Graph is only an Orchestrator plugin. It is neither a replacement for this
chain nor permission to bypass Guarded Commit; a sequential agent remains a
valid and often lighter orchestrator.

## 5. Five extension-point semantics

The extension-point semantic determines conflict resolution and lifecycle. A
domain family is not itself an extension-point type. Every registry surface
must declare one of the following five semantics; a module may not choose
implicit last-write-wins behavior.

| Semantic | Use when | Conflict and ordering | Snapshot / replacement rule |
| --- | --- | --- | --- |
| **Collection** | many independent records can coexist, such as capabilities, evaluators, providers, document sources, or Console modules | stable ID is globally unique; explicit priority plus ID creates a deterministic order; duplicate ID or equal-priority ambiguity is rejected | snapshot contains the ordered immutable collection; replace is stage-new then atomically swap the one record |
| **Pipeline** | each contributor transforms or validates the same input, such as context assembly, guard checks, verifier/corrector, telemetry processors | every stage has an explicit before/after phase and stable order; cycles and incompatible adjacent contracts are rejected | existing runs keep their pipeline snapshot; replacement stages only new snapshots and old stages drain |
| **Scoped Overlay** | values are selected by scope/profile/tenant/run, such as policies, credentials, model routing, prompt fragments, and feature flags | most-specific scope wins only after permission intersection; equal specificity must be merged by declared rule or rejected | resolve once into immutable run composition; an overlay change affects only newly resolved runs |
| **Single Strategy** | exactly one implementation is active for a named role, such as primary queue backend, default model router, clock, or persistence codec | one stable role has one active owner; a second provider is a conflict unless an explicit router/selector owns the choice | replacement is staged health-check then atomic role switch; old strategy drains before close |
| **Stateful Resource** | a component owns leases, worker processes, files, durable projections, or external connections | one resource owner owns its lifecycle and must expose health, drain, reconcile, and reverse-effect records | no replacement/close until leases drain or an explicit force policy fences them; desired state is reconciled durably |

Foundational infrastructure is selected through typed constructor injection in
cmd/server. An optional module registers only what it provides. A module gets
each Port it consumes through its constructor; registration is not dependency
discovery.

### 5.1 Domain extension surfaces

| Family | May contribute | Principal Ports | May not do |
| --- | --- | --- | --- |
| Model control | catalog providers, authenticators, protocol factories, routing policy | ModelCatalog, Authenticator, ProtocolFactory, ModelRouter | read raw settings or choose credentials outside resolution |
| Context and knowledge | prompts, Skills, memory/RAG retrieval, trusted runtime state | ContextSegmentProvider, SkillSource, MemoryRetriever, Retriever, Reranker | rewrite canonical history, inject unbounded content, mutate memory without policy |
| Capability and execution | tools, discovery, MCP/HTTP/WASM/process/Runner executors, sandbox/workspace | CapabilityProvider, ToolCatalog, Executor, Sandbox, Workspace | bypass guarded invocation, approval, journal, or scope |
| Orchestration and interaction | planner/reviewer/graph, Trigger, Channel, interruption/delivery | Orchestrator, EventSource, Channel, InterruptPolicy, Delivery | commit effects directly or alter another run's events |
| Operations and product plane | evaluators, flags, releases, observers, Console modules, product routes | Evaluator, ExperimentRegistry, ReleasePolicy, TelemetrySink, ConsoleModule | use a service locator, load remote UI code, or mutate kernel laws |

`ProvidedExtension` carries the semantic contract rather than relying on the
domain family to imply one:

~~~go
type ProvidedExtension struct {
    ID          ExtensionID
    Semantic    ExtensionSemantic
    Contract    ContractID
    Version     Version
    Priority    int
    Before      []ExtensionID
    After       []ExtensionID
    ResourceKey string
}
~~~

Each semantic has its own validator and resolver. Collection, Pipeline, Scoped
Overlay, Single Strategy, and Stateful Resource records are never resolved by
implicit last-write-wins logic. Scoped Overlay keyspaces are registry-specific
(for example policy key, credential ref, or profile fragment key); only
`ScopePath` ancestry supplies ordering. Profile, tenant, run, and other
dimensions must declare a separate key and merge rule rather than being called
"more specific" by convention.

## 6. Module manifest, contract, and lifecycle

Current core.Plugin is a useful in-process capability/profile installer, but
cannot be the V2 module contract: it lacks typed dependency declarations,
model/evaluator/route ownership, staged health activation, lease draining, and
reconciliation. It remains compatible as a legacy facade. A V2 adapter maps
its existing registrations into the new registry.

A V2 manifest is immutable metadata, not executable configuration. It declares:

- stable ID, version, compatible API range, owner, optional feature flag;
- provided extension IDs and semantic contracts;
- required and optional Port contracts, never concrete package/global names;
- configuration schema and secret references, never secret values;
- permissions, scope, limits, and declared reverse effects;
- migration/reconciliation version, health probes, drain deadline; and
- upgrade compatibility and rollback preconditions.

It cannot name arbitrary Go objects, database handles, a filesystem path,
remote code URL, bearer token, or another module's mutable view model.

~~~text
Discovered -> Validated -> Constructed -> Staged -> Healthy -> Active
                                             |                    |
                                             v                    v
                                           Failed <--- Draining <- Deactivating
                                              ^          |
                                              |          v
                                              +--- WaitingDependencies
                                                           |
                                                           v
                                                       Inactive
~~~

- Validated checks uniqueness, versioned dependency compatibility, conflict
  rules, config, and secret references without effects.
- Constructed receives typed constructor dependencies but is not routable.
- Staged builds a private registry snapshot and runs health checks. It is pure
  preparation unless every external effect is written to the durable
  `EffectJournal`; an alternative implementation must make Stage effect-free.
  Activate and Reconcile use the same journal, so Stage/Activate/Reconcile
  effects all have actual inverse records.
- Active atomically publishes a snapshot for new runs; old runs retain their
  composition snapshot.
- Draining stops new assignments and waits for or cancels bounded leases while
  retaining journal recovery.
- Inactive has no routes/capabilities. Reconciliation evidence permits a
  deterministic retry, roll-forward, or rollback.

Upgrade is stage-new, health-check, snapshot swap, drain-old. Rollback is the
same sequence in reverse only when declared effects and migration compatibility
allow it. Close/deactivation must never delete durable data implicitly.

### 6.1 Dependency, lease, and desired-state rules

Dependencies form a directed acyclic graph of module ID, semantic contract,
and compatible version range nodes. Validation rejects a missing required
dependency, an incompatible version, a duplicate provider for a Single
Strategy role, and every cycle. An optional dependency is not a hidden
requirement: its absence or loss can only remove it from a newly resolved
snapshot and must not invalidate an already running composition.

If a required dependency is lost while a module is Active, the transition is
Active -> Draining -> WaitingDependencies. It stops accepting new work,
publishes a new snapshot without the unavailable route, and retains state for
reconciliation. It may return to Staged only after its dependency graph and
health checks are valid again.

The manifest describes possible reverse effects, but actual effects are
recorded at runtime as inverse operations with stable effect IDs. Examples
are a mounted route, a started worker, an acquired projection ownership
record, or a registered capability. Deactivation executes only recorded
inverses in reverse dependency order. It never trusts a manifest claim in
place of an actual effect record.

Every execution segment, worker, approval suspension, and resource operation
obtains a module lease from the composition snapshot. An approval suspension
releases its segment worker/module lease after persisting suspended state;
resume creates a new explicitly evidenced segment, records its previous and
new composition revisions plus the authorization basis, re-resolves the
current permitted composition, and obtains fresh leases before executor action.
A drain deadline with remaining leases is not completion: the module stays
Draining and resources stay allocated. Force is an explicit, audited
administrative operation that fences/cancels remaining leases before inverse
operations.

A durable desired-state reconciler compares persisted desired module state,
recorded actual effects, resource health, and active snapshots after startup
or a failed transition. It may retry declared idempotent reconciliation or
surface a blocked state; it must not silently reconstruct an unrecorded
external effect.

`WaitingDependencies` is durable and is entered when a required dependency is
lost. `Drain` reports whether leases remain; only `Fence` (an explicit audited
operation) may force a bounded drain. `Deactivate` runs recorded inverses in
reverse dependency order and is not permitted to infer effects from manifest
claims.

### 6.2 Contract skeletons

These are contract sketches, not a reflection-based framework or a commitment
to the exact final names:

~~~go
type ModuleManifest struct {
    ID           ModuleID
    Version      Version
    Requires     []Dependency
    Optional     []Dependency
    Provides     []ProvidedExtension
    Effects      []EffectKind
    DrainTimeout time.Duration
}

type Module interface {
    Manifest() ModuleManifest
    Stage(context.Context, ModuleDependencies) (StagedModule, error)
}

type StagedModule interface {
    Health(context.Context) error
    Activate(context.Context, RegistryTransaction) (ModuleLeaseOwner, error)
}

type ModuleLeaseOwner interface {
    AcquireLease(context.Context, LeaseRequest) (Lease, error)
    Drain(context.Context, DrainRequest) (DrainResult, error)
    Fence(context.Context, FenceRequest) error
    Deactivate(context.Context) error
    Reconcile(context.Context, DesiredModuleState, []RecordedEffect) error
}
~~~

`LeaseRequest` includes host-issued `RunID`, `CompositionRevision`,
`ModuleID`, `ModuleRevision`, and target scope; a lease identity records the
same fields and the host rejects a token from another snapshot. The durable
`EffectJournal` records every external effect and inverse during Stage,
Activate, and Reconcile. A module cannot retrieve arbitrary dependencies from
the journal or registry transaction; constructor injection remains the only
consumption mechanism. `ModuleDependencies` and `RegistryTransaction` are
typed, operation-specific contracts and must not expose `Get`, `Lookup`, or
`Resolve(string)` service-locator methods.

## 7. Model control has six layers

Provider, Protocol, Catalog, Auth, Capability, and Router are separate. They
must not collapse into one generic llm setting or one vendor-specific adapter.

| Layer | Owns | Boundary |
| --- | --- | --- |
| Provider | vendor catalog synchronization, endpoint selection, credential/auth policy, provider lifecycle | provides nonsecret catalog records and an Authenticator policy; it does not own protocol wire |
| Protocol | request/stream/result wire grammar and adapter construction | ProtocolFactory turns ResolvedModel plus Authenticator plus transport into core.LlmAdapter |
| Catalog | stable model ID, provider ID, ProtocolID, wire model, status, revision, capability declaration, credential reference | ModelCatalog resolves a ModelSpec, never an API key |
| Auth | credential-source policy bound to principal/scope/reference and protocol request signing | Authenticator resolves or applies credentials without exposing their bytes to callers |
| Capability | streaming, tools, structured output, context/cache/multimodal limits | resolver rejects a run that requires unsupported features |
| Router | permitted catalog selection for profile/run policy | ModelRouter returns a ResolvedModel with nonsecret evidence |

A Provider can offer models using several protocols; a ProtocolFactory can be
used by several Providers. For example, one vendor may expose both a
chat-completions-compatible protocol and Responses, while several independent
providers may expose the same OpenAI-compatible protocol. ModelSpec therefore
names ProtocolID explicitly; ProviderID is never used as an implicit protocol
switch.

Compatibility surface retained in phase one: core.ModelSelection, ModelResolver,
ModelResolverFunc, Runtime.Models, LlmAdapter, GenerateOptions, StreamChunk,
ArtifactRevisioner, and RunCompositionData. CatalogResolver implements the
existing ModelResolver instead of changing third-party implementers.

ModelSelection is the authoritative logical request. A contextual ResolveRequest
binds it to principal and scope. Router plus Catalog plus ProviderRegistry
produce an immutable ResolvedModel and ProviderPlan whose ModelSpec provides the
exact WireModel and ProtocolID. ProtocolFactory creates the adapter from that
resolved record and provider plan; it never looks up a Provider or endpoint by
itself. A profile selection must never be silently
overridden by an environment variable or unrelated setting. During
compatibility, legacy llm.model imports as one catalog/default record; a
mismatch fails visibly rather than calling a different model.

The compatibility `ModelResolver` remains a logical adapter seam only. New
model-control resolution always uses principal, scope, ProviderBinding, and
`CredentialBindingRevision`; no first vertical may resolve a model credential
without that context. `OpaqueAuthSigner` keeps credential bytes inside the Auth
boundary.

The immutable execution-segment composition evidence includes nonsecret
`ProtocolID`, `WireModel`, `CatalogRevision`, `ProviderBindingRevision`,
`CredentialBindingRevision`, and the module dependency-closure revision. The
legacy logical `ModelSelection` and adapter artifact revision remain for
compatibility, but are not sufficient evidence of the wire model or module
snapshot.

### 7.1 Model contract skeletons

The following sketches make the ownership boundary testable without deciding a
storage implementation:

~~~go
type ModelSpec struct {
    ID            ModelID
    ProviderID    ProviderID
    ProtocolID    ProtocolID
    WireModel     string
    Revision      string
    Capabilities  ModelCapabilities
    CredentialRef CredentialRef
    CredentialBindingRevision string
}

type ResolvedModel struct {
    Spec     ModelSpec
    Evidence ResolutionEvidence // nonsecret provider/model/revision only
}

type Provider interface {
    ProviderID() ProviderID
    Catalog(context.Context) ([]ModelSpec, error)
    AuthPolicy() AuthPolicy
}

type ProviderPlan struct {
    ProviderID ProviderID
    Endpoint   string
    Signer     OpaqueAuthSigner
    Transport  TransportBinding
}

type ProviderBinding struct {
    Provider Provider
    Plan     func(context.Context, ResolveRequest, ModelSpec) (ProviderPlan, error)
}

type ProviderRegistry interface {
    Resolve(ProviderID) (ProviderBinding, error)
}

type ProtocolFactory interface {
    ProtocolID() ProtocolID
    NewAdapter(context.Context, ResolvedModel, ProviderPlan) (core.LlmAdapter, error)
}

type ResolveRequest struct {
    Selection ModelSelection
    Principal Principal
    Scope     ScopePath
}

type CatalogResolver struct {
    Router    ModelRouter
    Catalog   ModelCatalog
    Providers ProviderRegistry
    Protocols ProtocolRegistry
    Auth      AuthenticatorResolver
}
~~~

Required tests include: one provider with two ProtocolID values; two providers
with one shared ProtocolID; a profile selection reaching exactly its
ResolvedModel WireModel over httptest; ProviderPlan endpoint/auth binding;
protocol rejection of provider lookup; contextual principal/scope resolution;
unsupported capability rejection; credential rotation revision behavior; and
no credential value in a resolved record, event, telemetry payload, or API
view.

### 7.2 Secret reads and writes

No new secret API returns a complete secret. A generic secret GET remains
write-only/redacted and returns no secret value. The designated domain read
preview exposes `has_secret` plus a fixed recognition mask: the first three characters,
a fixed mask token whose length is independent of the source, and the last four
characters; secrets shorter than eight characters are fully hidden. This fixed
preview is the only permitted recognition information: do not additionally
return a hash, fingerprint, digest, or reversible derivative, and do not reveal
the original total length. The Console never puts the preview back into a write
request and never re-submits it as a secret.

| Input | Meaning |
| --- | --- |
| secret omitted | preserve existing secret |
| non-empty secret supplied | replace |
| clear_secret: true | delete |

Supplying a non-empty secret together with `clear_secret: true` is rejected as
ambiguous. Empty strings and mask markers do not infer preserve or clear in a
new command. Secret writes use an expected binding revision/CAS in the domain
service; that CAS/revision behavior is introduced with the M2 model-control
vertical, not by M0's generic settings stop-the-line fix.
Legacy S3/LLM wire behavior may remain in a mapper for old clients, but generic
reads must be opaque. The Console never rehydrates secrets into a ViewModel.

## 8. Ports for each Agent-engineering domain

| Domain | Ports / registry | Snapshot and lifecycle rule |
| --- | --- | --- |
| Prompt/context/KV cache | ContextAssembler, ContextSegmentProvider, CachePolicy, PromptCacheHints | static prefix ordered/digestable; volatile state late; cache hints advisory |
| Dynamic Prompt/Skills | PromptSource, SkillCatalog, SkillSource, SkillResolver, SkillActivationPolicy | discover metadata, load on demand, freeze source revision, retire safely |
| Status bar | RuntimeStateProvider returning bounded StatusSnapshot | trusted dynamic tail, not a mutable system prompt or user message |
| Compaction/isolation | ContextCompactor, RunSummarizer, IsolationPolicy | preserve decisions/constraints/evidence; structured handoff beats transcript copying |
| Memory | MemoryCandidateExtractor, MemoryProposalStore, MemoryReviewer, MemoryRetriever | extract, review source/policy, versioned update, scoped recall |
| RAG | DocumentSource, Chunker, Embedder, Retriever, Reranker, CitationPolicy | document/chunk/retrieval revisions recorded; projections rebuildable |
| Tools/MCP | CapabilityProvider, ToolCatalog, ToolSearcher, Executor, MCPClient/future MCPServer | catalog progressive disclosure allowed; every direct call is guarded |
| Coding/sandbox | Workspace, Sandbox, CommandExecutor, PatchApplier, Verifier, RecoveryPolicy | workspace/version/verification are artifacts; no opaque retry |
| Async/channel/interrupt | EventSource, EventInbox, Scheduler, Channel, InterruptPolicy, Delivery | dedup before enqueue; interruption preserves grammar and fence semantics |
| Evaluation/flags/simulation | Evaluator, DatasetStore, ExperimentRegistry, Assignment, FeatureFlagResolver, Simulator, TelemetrySink | assignment/artifact revisions recorded pre-run; releases consume evidence |
| Evolution/release | EvidenceLink, ChangeProposal, ArtifactRepository, Validator, Publisher, ReleasePolicy | online evidence separate from offline proposal/validate/approve/release |
| Multi-agent/A2A | AgentSpawner, Handoff, AgentMessageBus, Topology, Workspace | isolated by default; handoff structured, bounded, permissioned, cancelable |

### 8.1 LangGraph-style Graph Orchestrator

Graph is an optional Orchestrator extension under `pkg/extensions/graph`; it
is not kernel vocabulary, not a generic ModuleHost feature, and not a
Provider/Protocol feature. The V2 non-goal of making a graph DSL universal
therefore remains: one sequential Orchestrator is valid, while this extension
offers a bounded graph contract for products that opt in.

The first contract is versioned and immutable per execution segment:

~~~go
type GraphDefinition struct {
    ID, Revision string
    StateSchema, Reducer string
    Nodes []NodeSpec
    Edges []EdgeSpec // default, conditional, error
    Limits GraphLimits
}
type NodeSpec struct {
    ID, Contract, Version string
    ContextView ContextViewSpec
    ContextBudget ContextBudget
    Retry RetryPolicy
    Timeout Duration
}
type GraphCheckpoint struct {
    GraphRevision, StateRevision, ReducerRevision string
    RunID, ExecutionSegmentID, CompositionRevision string
    CurrentNode, Attempt, PendingApprovalID string
}
~~~

These are contract sketches, not final Go API names. State is an immutable,
versioned value. A node returns a bounded `StatePatch`; a deterministic
reducer validates and applies it, producing the next state revision and
digest. Nodes do not mutate shared state or receive the entire graph history
by default. Each node declares a `ContextView` and `ContextBudget`; the Graph
materializes the node input from the checkpoint fact state, required schema
fields, the current input, pending approval/tool pair, and bounded history
layers (episode, summary, artifact, retrieval). EventLog remains the fact
source; ContextView is a node-local projection.

The context plan records its own revision, token estimate, per-layer token
allocation, output reserve, and compaction reason. Stable KV-prefix material
is ordered first; volatile state is late. Memory and RAG remain isolated
sources with provenance and scope. Hierarchical summaries are durable
artifacts and must not recursively summarize prior summaries without a source
revision. Context metadata is observable, but state, prompts, credentials and
secret values are redacted.

Node and edge registries use stable IDs plus contract/version. Edge selection
is deterministic: conditional edges read a bounded read-only state view,
`Priority` and ID provide a stable tie-break, and an explicit default edge is
used when no condition matches. Error edges have declared error classes and
cannot swallow unknown failures. Validators reject duplicate defaults,
unresolved references, unreachable nodes, ambiguous conditions, and paths
with no terminal or bounded error outcome. Cycles are allowed only with
global step/wall-time/state budgets and per-node visit limits; retry budgets
are separate and bounded.

Every node has a deadline, cancellation path, and explicit retry policy.
Unknown non-idempotent effects are reconciled/fail-closed rather than retried.
Checkpoint CAS covers node attempt, reduced state, selected edge and graph
revision. A crash or CAS conflict cannot produce a second interpretation of
the same state.

Approval is a durable Graph interruption. Suspension writes the pending
approval/tool pair, releases the current module lease, and ends the immutable
execution segment. Resume verifies Graph/schema/reducer/composition revisions
and the authorization basis, then creates a new execution segment and records
the previous/new composition revisions. A current definition cannot silently
reinterpret an old checkpoint.

Graph nodes have no effect escape hatch. Agent-proposed tool or user-
communication effects must pass Guarded Commit and receive a host-issued
`AcceptedInvocation`; model outbound calls must pass the independent
`ModelCallGate` for routing, budget, credentials and telemetry. Persistence
uses an application use-case/UoW. Nodes cannot construct an invocation token,
call a raw tool/model transport, or hold a trusted adapter transport.

The extension emits bounded, namespaced events for graph/node start and end,
edge selection, state patch, checkpoint, retry, approval, resume, cancel and
error. Traces cover graph, node, edge and checkpoint spans. Metrics use only
allowlisted low-cardinality dimensions. State patches and context plans are
redacted; raw state, prompts, credentials and secret-derived values never
enter events, traces or metrics. Because Session event types are validated,
`graph/*` events require an approved extension-event registration seam or a
validated Graph envelope; the extension may not bypass Session.Append.

Graph validator tests cover schema/reducer determinism, edge ambiguity,
cycle/visit/step budgets, retry/timeout/cancel bounds, checkpoint
serializability/CAS, revision mismatch, approval segment resume, effect-gate
negative cases, context source isolation and redaction. The smallest runnable
vertical has a pure start node, a fixture ModelCallGate, a conditional/default
edge, a Guarded Commit tool node, a bounded error path, checkpoint/resume and
the full event/trace/metric assertions. Parallel fan-out/join, dynamic graph
mutation, editor/DSL, distributed scheduling and subgraph composition are
post-M1 work, not prerequisites for the contract.

### 8.2 ContextPlan and configuration source of truth

ContextPlan is the node-local materialization policy, not a second fact store:
EventLog is authoritative; ContextView is derived and disposable. A plan has
typed sources, source revisions, segment and per-layer budgets, a token
estimator, output reserve, stable-prefix hints, and a compaction reason. Its
validator checks source identity/revision, budget arithmetic, required
invariants, isolation and redaction. It preserves current
input, required state fields, pending approval/tool invariants and explicit
artifact references. Sources are isolated by policy: memory/RAG retrieval
cannot rewrite canonical history, and a summary cannot recursively drift
without a new source revision.

The implementation slices are Contract + Validator, structured history,
budgeted compaction, durable summaries, and source isolation. Every node
declares its ContextView and ContextBudget; no node receives global Graph
state/history implicitly. Plan revision, token estimate, layer allocation and
compaction reason are nonsecret execution evidence.

Runtime configuration is DB-authoritative once bootstrap storage is
available. The only bootstrap exceptions are DB connection, listener/address,
data directory and an external root-key reference. Provider/Protocol/
Catalog/Auth references, allowed models, router policy, storage policy and
Graph definitions/checkpoints are runtime records, not hidden `.env` inputs.
Environment values are allowed only for one-time import or a legacy fallback
when the DB row is absent, and the effective source is recorded as
`bootstrap`, `db`, `env_import` or `legacy_fallback`. If a DB row exists but is
inactive or malformed, startup fails visibly or reports the row error; it must
never silently fall back to or be overridden by `.env`. This rule does not
move M2 credential CAS/revision into M0.

Storage-specific source-of-truth work is a later M0d slice, after M0c and not
before it. The current evidence is that the main composition still gives
`HARNESS_S3` and `SESSION_DIR` precedence, while the storage loader treats a
malformed value as absent. M0d replaces that ambiguity with typed
`ConfigSource`, `ConfigStatus` and nonsecret resolution evidence. For
resources and sessions, a row that exists is authoritative whether it is
embedded, S3, inactive or malformed; environment may be considered only when
the row is absent. A future one-time environment import must be validated and
encrypted before becoming a row.

Desired configuration and active/applied status are separate. If a dynamic
resource swap fails, the previous active resource remains active and the
desired row records the apply failure. A session resource change becomes
restart-pending rather than being silently applied to a live session. M0d does
not expand to worker/runner/approval/telemetry configuration; those follow a
later DB-authoritative migration. Secret read/write and redaction semantics
remain unchanged.

### 8.3 Sandbox contract and assurance

Sandbox is a generic execution provider under `pkg/execution/sandbox`, not a
Graph or ModuleHost primitive; its Sandbox Provider identity is distinct from
the model Provider identity in section 7. Its bounded contract covers
provider/session,
requested and actual assurance, resource limits, network policy, mounts,
staged artifacts, and fenced leases:

~~~go
type Provider interface {
    ID() ProviderID
    Start(context.Context, SessionSpec) (Session, error)
}
type Session interface {
    Assurance() AssuranceReport
    Limits() EffectiveLimits
    Run(context.Context, Command) (ArtifactSet, error)
    Close(context.Context) error
}
type SessionSpec struct {
    RequestedAssurance Assurance
    Limits Limits
    Network NetworkPolicy
    Mounts []Mount
    ArtifactPolicy ArtifactPolicy
    Lease LeaseIdentity
}
~~~

These are contract sketches, not final public names. The default is a local,
ephemeral session; the warm-pool default is zero. The current Linux bwrap
adapter provides process confinement on a shared kernel, but network isolation
is not provided and must be reported as such. Windows is currently
`unavailable`; it must not be represented as an equivalent confined backend.
Future container, microVM, cloud, self-hosted and remote adapters must report
their own assurance rather than inheriting a stronger label.

Requested assurance and actual assurance are separate values. Guarded Commit
must compare them and fail closed when the actual level is missing, weaker,
network policy is not met, limits are not enforceable, or the adapter is
unavailable. A tool/node cannot downgrade a request after acceptance. The
accepted invocation, ToolInvocationJournal identity, fenced sandbox lease and
staged artifact set must share the run/segment/module composition evidence.
Artifacts become visible only after verification and an explicit commit;
uncommitted or ambiguous child-process outcomes remain quarantined.

The sandbox provider/session owns no global admission queue or hidden retry
policy. Cancellation and timeout fence the lease, terminate or quarantine the
child according to the actual assurance, journal the outcome, and leave an
auditable artifact/reconciliation record. Untrusted modules receive this
contract only; they cannot hold host process handles, raw transports or root
mounts.

### 8.4 Performance measurement and first integrated optimizations

Perf-P0 now has a reproducible 10/100/500-concurrent and soak baseline, with
Windows working-set and CPU collection. The first Perf-P1 hot-path changes are
implemented and await the main-session's final integrated-state gate; this is
not a new performance admission gate. Each result remains labeled by
payload/context scenario, and sandbox child-process memory/startup remains a
separate line item.

The current production-shaped 500-concurrent measurements are ranges from two
GC-isolated rounds, rather than stale single-point values:

| Workload | Heap peak | p95 | Boundary |
| --- | ---: | ---: | --- |
| `agent_production_recent_compaction`, typical 16 KiB payload | 161–198 MiB | 162.6–200.1 ms | both rounds had zero errors |
| `agent_production_recent_compaction`, max 1 MiB payload | 1.54–1.76 GiB | 2.83–2.94 s | both rounds had zero errors |
| max/500 case RSS | 1.84–3.53 GiB | n/a | same-process working set; not an isolated case resident size |

The two 500-concurrent HTTP `/healthz` rounds are intentionally reported
separately: one had zero errors and one had 200 transport errors. These results
are not evidence that 500 HTTP is stably passing. In contrast, the two
production Agent workload rounds above both completed without errors.

The former 384 MiB 500-concurrent number was only a provisional target. It is
disproved for the max scenario by the information lower bound alone: 500 * 1
MiB is 500 MiB of raw input before context, headers, maps, request/response
copies, runtime overhead, or allocator/GC retention. It is not replaced by a
new gate in this slice. Case RSS varies substantially across same-process
rounds and must not be presented as a per-case or per-execution permanent
resident allocation, because the process runs preceding cases and retains Go
arenas.

Implemented first-round work is deliberately narrow and preserves EventLog as
the fact source: default no-summary `RecentTurnsCompactor` use fuses event
projection with the retained window, while summaries and custom compactors
fall back to the prior complete path; incremental projection cache/read helpers
avoid repeated full-history defensive copies; immutable tool-library indexes
and bounded summaries avoid repeated catalog construction; runner idle polling
backs off adaptively; and normal File/S3 resource GET and PUT use optional
streaming seams. File and S3 PUT stream through bounded temporary storage where
needed for integrity/replacement semantics. A 16 MiB streaming benchmark
reported about 60 KiB/op for File and about 168 KiB/op for S3 on the Go heap,
so payload does not enter that heap linearly; this does not remove the bounded
temporary storage required for object integrity/replacement or change the
older cold compressed-session fallback that materializes its legacy layout.

For an 8,192-event, 128-message-window benchmark, the direct legacy
`DeriveMessages` then `Compact` path used about 1.16 MiB and 10,754 allocations
per operation; the fused path used about 65.5 KiB and 1,061 allocations. The
separate 500-concurrent tool-catalog search cases recorded p95 about 4.32 ms
for the typical 128-tool scenario and about 7.33 ms for the max 512-tool
scenario; construction is excluded from those throughput cases. Its separately
measured post-GC retained footprint was about 0.24 MiB and 2.76 MiB
respectively. Result summaries have a 240-rune cap. These figures do not
claim that the current immutable index is a compact resident representation.

A separate short live-server measurement used an isolated temporary
dev/bootstrap SQLite `cmd/server` executable after the build process had
exited. Across 20 seconds idle, Working Set peaked at 19.69 MiB, private memory
at 19.04 MiB, and normalized CPU at 0%; the server exposes no public goroutine
metric. Across a 20-second single-client `/healthz` sample, the corresponding
peaks were 21.95 MiB and 21.11 MiB, normalized CPU was 0.0039%, and 96 requests
succeeded with client p95 1.778 ms. This is neither a long soak nor a 500-HTTP
stability result.

During this migration no admission controller, weighted semaphore, queue,
backpressure mechanism, or 429/load-shed gate may be introduced to make a
target appear to pass. A 500-concurrent run means 500 genuinely live concurrent
executions. Remaining work includes artifact-reference handling for large
results, a compact resident representation for tool indexes, and ContextPlan
token budgets with hierarchical summaries. Re-measure the same workloads after
each further change and record RSS, heap, CPU, goroutines, GC, DB, S3 and
child-process metrics with bounded dimensions.

The information lower bound must be stated beside every result. For N live
executions with payload P, materialized context C, retained metadata H,
per-execution runtime overhead R and sandbox-child RSS X:

~~~text
RSS_min(N,P,C,H,R,X) >= host_base + N * (P + C + H + R) + X
~~~

The formula is a lower-bound sanity check, not permission to retain unlimited
history or to count compressed/evicted state as free. Report measured RSS,
heap and child RSS separately; if a target is not met, report the miss and
scenario honestly rather than adding hidden concurrency gates.

## 9. Package convergence and facades

~~~text
pkg/
  core/                         trust-root vocabulary and Ports
  runtime/                      run loop, composition, context, module lifecycle
  app/{identity,settings,modelcontrol,...}/
  adapter/
    httpapi/{identity,settings,modelcontrol,...}/
    sql/{sqlkit,migration,identity,settings,...}/
    modelprotocol/{openai_chat,responses,anthropic,...}/
    modelprovider/{openai,anthropic,private,...}/
    execution/{http,mcp,wasm,process,runner}/
    objectstore/{file,s3}/
    telemetry/{otel,...}/
  extension/{memory,rag,toollib,subagent,...}/
  console/static/js/model-settings.js (one file per business domain)
~~~

cmd/server is the production composition root. It selects adapters, injects
Ports, builds registry snapshots, and wires compatibility facades. Legacy
server, storage, provider, execution, control, evaluation, telemetry, and
extensions import paths remain until a major version; facades receive
compatibility fixes, never new ownership.

The split is intentional: adapter/modelprovider contains catalog/endpoint/auth
policy for a vendor or private deployment, while adapter/modelprotocol contains
only one wire grammar and its ProtocolFactory. The provider adapter supplies a
ProviderPlan; the protocol adapter consumes it and never reverse-discovers the
provider. The current provider/openai
package remains a compatibility facade during migration; it must not become a
new mixed vendor-and-protocol ownership location.

app packages depend only on domain value packages and narrow Ports, never
server/storage/console/concrete adapters. An adapter implements a
consumer-owned Port. Modulecheck, API inventory, compile fixtures, OpenAPI,
and local-asset verification enforce this direction. M1/M2 modulecheck rules
also require the facade to forward to the new implementation, forbid
modelprovider -> modelprotocol ownership edges and protocol provider lookup,
reject old-Plugin/new-Module duplicate IDs, and forbid untrusted modules from
holding raw transports. `pkg/core` remains stdlib-only and gets an explicit
public API/size budget; runtime/module-host and domain-specific registries do
not accumulate in the trust-root package.

## 10. M0-M5 migration and rollback gates (including M0c/M0d)

| Gate | Change | Proof | Rollback |
| --- | --- | --- | --- |
| M0: correctness and secret stop-the-line | reject profile/legacy model mismatch; generic secret GET has no value and is redacted/write_only; replace ad-hoc masks with the designated fixed recognition preview; do not introduce CatalogResolver or secret CAS | httptest selected-to-wire equality; response sentinel scan; bounded preview/short-secret/Console no-resubmit tests; omitted/replace/clear plus ambiguous-input rejection tests | retain legacy PUT mapper and persisted data; disable only the new route/compatibility check |
| M0c: LLM DB source-of-truth vertical | after M0, persist/read the current LLM runtime configuration through the DB-backed settings/application path; permit `.env` only for absent-row legacy fallback or one-time import; record `config_source`; DB-present inactive/malformed rows fail visibly; no CatalogResolver or credential CAS | fresh/restart DB read, absent-row import/fallback, DB-present precedence, malformed/inactive row failure, source telemetry/audit, no secret response and no silent env override | disable the new LLM DB route/import and retain the legacy mapper; preserve imported rows for inspection; do not erase forward-only data |
| M0d: storage DB source-of-truth vertical | after M0c, typed ConfigSource/Status/resolution evidence; resources/sessions rows are authoritative for embedded/S3/inactive/malformed states; env only when row absent; worker/runner/approval/telemetry remain later | HARNESS_S3/SESSION_DIR precedence regression; malformed-as-absent regression; row-present precedence; validated encrypted import; desired-vs-active dynamic swap and session restart-pending tests; source evidence and secret redaction | retain legacy storage loader/main composition; disable DB storage activation/import; preserve old active resource on failed swap and pending session restart; no secret rollback |
| Sandbox-S0: contracts and local assurance | generic `pkg/execution/sandbox` provider/session/assurance/limits/network/mount/artifact/lease contract; local ephemeral default and warm pool 0; Linux bwrap reports process confinement/shared kernel/network not isolated; Windows unavailable | requested-versus-actual assurance fail-closed, limits/network/mount enforcement, accepted-invocation/journal/fenced-lease/staged-artifact binding, child timeout/cancel/reconcile tests | keep sandbox capability disabled and retain existing executor path; never claim Windows or network isolation; preserve staged artifacts for audit |
| Perf-P0: baseline | 10/100/500 plus soak baseline and Windows RSS/CPU sampling are implemented; final integrated-state validation remains pending | payload/context RSS/heap/CPU/goroutine/GC telemetry, information lower bound, and honest scenario miss report | publish measurements only; disable no feature and add no admission/queue/backpressure/429 gate |
| Perf-P1: hot paths | first integrated changes cover fused session projection, incremental cache/read helpers, immutable tool index/bounded summary, adaptive idle polling, and File/S3 resource streaming GET/PUT; final integrated-state validation remains pending | same-workload remeasurement and regression comparison; 500 means real concurrency | revert the narrow optimization or retain the baseline path; do not hide misses behind load shedding |
| M1: minimum trusted module host and effect boundaries | manifest dependency DAG, five semantic validators, staged snapshots, durable EffectJournal for Stage/Activate/Reconcile, actual inverse effects, health/drain/fence/deactivate/reconcile, AcceptedInvocation and ModelCallGate seams; old Plugin adapter | required/optional dependency, cycle, failed stage, atomic activation, crash windows, effect inverse, approval segment resume, lease identity/drain/fence, duplicate old/new registration, nested-call bypass, and rollback tests | old plugin path retained; deactivate/fence before snapshot publication; no unrecorded Stage effect |
| M2: model-control vertical and first protocols | app/modelcontrol, contextual resolver, ProviderRegistry/ProviderPlan, credential CAS/revision, typed DTO/Console model view, OpenAI Chat migration, Responses and Anthropic ProtocolFactory paths | same-provider/multi-protocol and multi-provider/shared-protocol tests; endpoint/signer binding; principal/scope and credential rotation tests; profile/canary/eval/module-closure evidence; old resolver compile fixture | core resolver facade and absent-row-only legacy env importer remain one imported catalog record; protocol/provider modules drain independently; rollback cannot erase forward-only SQL data |
| M3: context/knowledge/capability catalogs | segments, Skills, memory/RAG provenance, discovery registry | prefix/cache, source revision, scope/injection/citation tests | turn off optional providers, keep current compactor/tools |
| M4: execution/interaction | workspace/sandbox/verifier/recovery and event/channel/interrupt adapters | timeout/cancel/fence/idempotency, repair, no-duplicate-effect tests | route to existing executor/queue, preserve journal/events |
| M5: operations/collaboration | flags/experiments/simulation, evolution artifacts, handoff/A2A adapters | deterministic assignment, gate/rollback, structured handoff/budget/cancel tests | freeze promotion, roll back snapshot, disable topology/module |

Every gate runs gofmt, target tests, full tests, vet, modulecheck, OpenAPI,
external compile fixtures, SQLite fresh/historical checks, and PostgreSQL checks
when configured. A missing PostgreSQL DSN is a reported skip, never a pass.
Security gates additionally scan response/telemetry/log fixtures for synthetic
sentinels and run AcceptedInvocation, ModelCallGate, raw-transport, and
protocol-provider-lookup negative fixtures. Lifecycle gates inject crashes
between external effect and journal record, snapshot publication and old
drain, and desired-state reconciliation; restart must surface rather than
invent an unrecorded effect.

## 11. Non-goals and completion

V2 does not make a graph DSL universal, load remote Console code, create
generic models/utils/common/helpers packages, reset schema/migrations, or
allow reflection/service-locator dependency discovery.

It is complete when a contributor can add a provider, Skill, executor,
sandbox, evaluator, channel, or Console module through a bounded contract and
can prove through public/API/integration tests which model/spec/policy,
ProtocolID/WireModel/catalog/provider/credential/module revisions, and
nonsecret configuration produced each execution segment without bypassing the
three trust-root effect boundaries.
