# Harness Core Pluggable Architecture Refactor

Date: 2026-09-02

Status: approved direction; implementation requires review of this written specification

## 1. Purpose

Refactor `harness-core` into a small, stable, integration-friendly Agent
foundation whose external systems and variable product strategies are
replaceable without editing the kernel.

The refactor must make it obvious to a new contributor:

- where a domain model belongs;
- where an HTTP request or response belongs;
- where a SQL row and query belong;
- where a business use case belongs;
- where an Agent capability, tool call, invocation, and executor belong;
- which interfaces are extension points;
- which runtime invariants cannot be bypassed;
- how to add a provider, store, transport, evaluator, workflow, or product
  module without modifying unrelated packages.

This is a behavior-preserving architecture migration, not a rewrite.

## 2. Goals

1. Keep the public foundation small and stable.
2. Make external integrations and variable strategies pluggable.
3. Group packages into a small number of coherent package families.
4. Separate domain, application, transport, persistence, runtime, and UI
   models.
5. Preserve SQLite and PostgreSQL data and migration compatibility.
6. Preserve `/v1` HTTP/SSE behavior and existing Go integration points while
   compatibility facades are active.
7. Preserve approval continuation, queue generation fencing, session leases,
   canonical event logs, and tool side-effect journal semantics.
8. Make a complete feature path discoverable from one bounded module.
9. Add executable architecture checks so boundaries do not regress.
10. Keep simple integrations simple; optional production capabilities must not
    be mandatory dependencies.

## 3. Non-goals

- Do not replace the Agent runtime with a new graph runtime in this refactor.
- Do not redesign the `/v1` wire protocol except for explicitly approved
  additive compatibility fixes.
- Do not reset the SQL schema or replace the existing v1-v32 migration history.
- Do not introduce a dependency-injection framework or reflection-based
  service locator.
- Do not make every helper function an interface.
- Do not create generic `models`, `utils`, `common`, or `helpers` dumping-ground
  packages.
- Do not move every public type to a new import path in one change.
- Do not introduce React, Vite, or another frontend build tool merely to split
  the embedded Console.

## 4. Definition of pluggability

"Everything is pluggable" means every external effect and every genuinely
variable policy has a stable Port and one or more replaceable Adapters.

It does not mean that correctness and security invariants can be replaced or
bypassed.

### 4.1 Required pluggable seams

- model providers and model resolution;
- capabilities and capability execution;
- HTTP, MCP, WASM, process, and private Runner execution adapters;
- authentication and identity lookup;
- authorization policy contributions;
- credentials and secret resolution;
- Session persistence;
- Run control, queue, claim, lease, and cancellation persistence;
- durable approval persistence and decision delivery;
- tool invocation journal persistence;
- account, tenant, token, and settings repositories;
- object storage;
- Release, Canary, Evaluation, Evidence, Audit, and Observability stores;
- Memory and RAG stores and projections;
- telemetry and logging sinks;
- clock, ID generation, and bounded retry policies where deterministic tests
  need replacements;
- optional product modules and capabilities.

### 4.2 Non-pluggable runtime invariants

The following remain mandatory guarded paths:

- Scope mutations remain downward-only;
- permissions remain intersection-based and fail closed;
- every nested capability call re-enters the protected invocation funnel;
- approval identity, arguments, and manifest digests remain bound;
- non-idempotent unknown tool outcomes remain fail-closed;
- stale queue generations and expired Session leases cannot commit results;
- canonical Session events remain validated and append-only;
- configured hard limits remain enforced;
- secret values remain excluded from public manifests, logs, and responses.

Adapters may implement these contracts, but no adapter may disable the
invariants.

## 5. Chosen architecture

Use a domain-modular monolith with Ports and Adapters. Keep one Go module and a
small number of package families.

```text
cmd/server
    |
    v
adapter/httpapi  --->  app/*  --->  core + runtime
    |                              ^
    |                              |
    +------ adapter/sql -----------+
    +------ adapter/objectstore ---+
    +------ adapter/execution -----+
    +------ adapter/model ---------+
    +------ adapter/telemetry -----+

extension/* ---> core/runtime Ports
```

Dependency rules:

```text
core       imports only the standard library
runtime    imports core
app/*      imports core/runtime and the domain Ports it consumes
extension  imports core/runtime/app Ports, never concrete adapters
adapter/*  imports the Port-owning package and implements it
cmd/server imports all selected modules and is the only composition root
```

No package below `cmd/server` may discover arbitrary dependencies at runtime.
Dependencies are explicit constructor parameters.

## 6. Package convergence

The target uses six main package families rather than one top-level package per
technical concept:

```text
pkg/
  core/                 stable foundation contracts and value objects
  runtime/              generic Agent execution and composition
  app/                  product-neutral application use cases
  adapter/              external protocol and infrastructure implementations
  extension/            optional Agent/product modules
  console/              embedded administration UI
```

Feature subpackages exist only when they enforce a real dependency or ownership
boundary.

```text
pkg/app/
  identity/
  run/
  approval/
  capability/
  control/
  evaluation/
  evidence/

pkg/adapter/
  httpapi/{auth,run,approval,capability,runner,admin}/
  sql/{sqlkit,migration,identity,session,run,approval,control,evaluation}/
  objectstore/{file,s3}/
  execution/{http,mcp,wasm,process,runner}/
  model/openai/
  telemetry/otel/

pkg/extension/
  workflow/
  subagent/
  runner/
  memory/
  rag/
  toollib/
```

The current `pkg/core`, `pkg/server`, `pkg/storage`, `pkg/control`,
`pkg/evaluation`, `pkg/execution`, `pkg/provider`, `pkg/telemetry`, and
`pkg/extensions` import paths remain as compatibility facades while migration
is in progress. New behavior must not be added to a legacy facade.

## 7. Core and runtime ownership

### 7.1 `pkg/core`

`core` owns only stable, product-neutral contracts:

- identifiers, Scope, Principal, and Permission;
- Capability definition and execution Ports;
- ToolCall, ToolResult, Invocation identity, and journal Port;
- model adapter and streaming contracts;
- Session/Event contracts and storage Ports;
- telemetry contracts;
- stable error categories and bounded public limits.

`core` must not import SQL, HTTP implementations, provider SDKs, WASM, OTel,
Console code, Release, Canary, Evaluation, or product adapters.

### 7.2 `pkg/runtime`

`runtime` owns generic execution mechanics:

- Agent turn and resume loop;
- protected tool invocation funnel;
- capability/profile/policy/credential composition;
- immutable Run Composition;
- context compaction, summary, and repair;
- plugin/module mounting mechanics;
- approval suspension integration;
- usage and runtime telemetry orchestration.

Moving a symbol from `core` to `runtime` is initially implemented through a
compatibility alias or wrapper in `core`. Removal of old paths requires a major
version.

## 8. Model ownership rules

Every model belongs to exactly one of these categories.

### 8.1 Domain entity and value object

Owned by `core`, `runtime`, or its `app/*` domain. It contains business meaning
and typed enums. It does not contain SQL scan types, `http.Request`, UI state,
or persistence secrets.

### 8.2 Application command and result

Owned by `app/*`. It describes one use case. It has no JSON or SQL tags and no
transport-specific error codes.

### 8.3 Transport DTO

Owned by `adapter/httpapi/<feature>`. Requests and responses are named types.
Handlers may not use anonymous request structs for public routes and may not
return ordinary success payloads as `map[string]any`.

### 8.4 Persistence row

Owned privately by `adapter/sql/<feature>`. SQL rows are unexported and are
converted through explicit mappers. They may contain database timestamps,
nullable values, encrypted blobs, and password hashes. They may never be used
as HTTP responses.

### 8.5 Adapter wire model

Owned by the adapter whose protocol defines it, such as MCP wire DTOs, OpenAI
wire messages, or Runner worker payloads. Conversion to core models is explicit
and tested.

### 8.6 UI ViewModel

Owned by one Console feature. Loading, confirmation, and error fields remain UI
state and are never serialized by passing an entire ViewModel to the API
client.

## 9. Enum rules

Domain states use named string types with stable wire/database values:

```go
type Status string

const (
    StatusPendingActivation Status = "pending_activation"
    StatusActive            Status = "active"
    StatusDisabled          Status = "disabled"
)
```

Every externally parsed enum supplies:

```go
func ParseStatus(string) (Status, error)
func (Status) Valid() bool
func StatusValues() []Status
```

Enums live with their owning domain. There is no global `enum` package.
Semantically different statuses are not merged merely because they share
string values.

## 10. Capability, tool, invocation, and execution separation

The following are distinct contracts:

```text
CapabilityDefinition   what the capability exposes
CapabilityExecutor     how the runtime invokes it
ToolCall               model-requested call
Invocation             runtime-owned execution identity and context
ToolResult             canonical runtime result
JournalRecord          durable side-effect execution state
AdapterRequest         HTTP/MCP/WASM/Runner protocol model
```

`ExecutionSpec` is decomposed internally into runtime-specific options. The
existing public JSON shape remains supported by a mapper during compatibility.

All executions continue through the protected Capability invocation funnel.
A plugin or adapter cannot receive a raw provider shortcut around schema,
policy, approval, credentials, budget, timeout, journal, and telemetry checks.

## 11. Port and adapter rules

The consumer of a dependency owns the smallest interface it needs.

Examples:

```go
type AccountRepository interface {
    Find(context.Context, identity.AccountID) (identity.Account, error)
    Create(context.Context, identity.NewAccount) error
}

type SessionAppender interface {
    Append(context.Context, core.SessionID, core.Version, []core.Event) error
}
```

Rules:

- do not create one giant repository interface;
- separate read, write, query, and optional capability interfaces when callers
  consume different subsets;
- use compile-time interface assertions in adapters;
- constructors validate required Ports and reject nil or inconsistent
  combinations;
- optional capabilities are discovered through small capability interfaces,
  not concrete type assertions such as `*SQLSomething`;
- adapters do not call another adapter to reach a domain use case;
- cross-aggregate transactions belong to an application Unit of Work Port.

## 12. Plugin and module model

Use two extension mechanisms.

### 12.1 Typed constructor injection

Foundational infrastructure is selected in `cmd/server` and passed through
typed constructors. This covers databases, queues, stores, model providers,
telemetry, authentication, and transports.

### 12.2 Reversible modules

Optional product features implement a bounded module contract:

```go
type Module interface {
    ID() string
    Register(context.Context, ModuleRegistry) (Unmount, error)
}
```

`ModuleRegistry` exposes focused registration surfaces for capabilities,
profiles, policies, credentials, model adapters, evaluators, or HTTP route
modules only where explicitly allowed. Registrations are reversible and
idempotent.

The registry is not a service locator. Modules cannot request arbitrary stores
or concrete adapters from it. A module that needs persistence receives its Port
through its constructor.

## 13. HTTP API and decoding policy

HTTP handlers follow one path:

```text
authenticate
  -> decode named DTO
  -> validate and map command
  -> call application service
  -> map typed error
  -> write named response
```

The `/v1` JSON decoder accepts unknown object fields for additive client
compatibility while retaining:

- maximum body size;
- valid JSON enforcement;
- one JSON value per request body;
- required field validation;
- enum and semantic validation;
- type mismatch rejection;
- sensitive-field redaction.

The Console still sends only documented request fields. The initial activation
request sends only `password` and `confirm`; UI `error` and loading state never
enter the request body.

OpenAPI remains the wire-contract source. Named Go DTOs and Console API models
must be checked against it.

## 14. Identity and authentication reference slice

Identity/Auth is the first migrated vertical slice and becomes the template for
later modules.

```text
Console auth ViewModel
  -> HTTP ActivateRequest
  -> app/identity ActivateCommand
  -> identity Service
  -> Account/Token UnitOfWork Port
  -> adapter/sql/identity
  -> SQLite/PostgreSQL
```

Requirements:

- Account, Role, Status, Tenant, and Token semantics leave `storage`;
- Role and Status become typed enums;
- password hashes exist only in the identity SQL adapter or credential Port;
- public Account views never contain password hashes;
- activation updates password/status, revokes restricted tokens, and creates a
  normal token in one transaction;
- password replacement and token revocation are atomic;
- login throttling is injectable but fail-closed;
- the backend accepts unknown JSON fields;
- the Console sends only the activation DTO;
- the initial administrator remains pending until successful password change.

## 15. Run, approval, and durable execution reference slice

Separate these concepts:

```text
Run aggregate
Run API view
Queue item
Worker claim
Session lease
Approval request
Tool invocation record
SQL rows for each table
```

Application services own cross-aggregate behavior:

- submit Run and queue atomically;
- decide approval and requeue the same Run atomically;
- cancel suspended Runs and close approval fences atomically;
- reconcile completed Session evidence without replaying model/tool work.

The SQL adapter implements the Unit of Work. The domain and HTTP layers do not
know SQL transaction types.

## 16. SQL and migration architecture

Split SQL infrastructure from feature repositories:

```text
adapter/sql/sqlkit       dialect, binding, transaction helpers
adapter/sql/migration    v1-v32 migration registry and inspection
adapter/sql/<feature>    private rows, queries, mappers, repositories
```

Requirements:

- retain schema version 32 and the complete historical upgrade chain;
- do not rename existing tables or columns solely for code organization;
- retain PostgreSQL advisory startup locking;
- retain PostgreSQL `SKIP LOCKED` and SQLite conditional transaction paths;
- reject unknown SQL dialects at construction;
- keep structure migrations retryable;
- treat Memory/RAG indexes and other documented projections as rebuildable;
- validate row preservation for destructive table-rebuild migrations;
- maintain fresh and upgrade fixtures for SQLite and PostgreSQL.

Legacy `pkg/storage` constructors delegate to new adapters until the next major
version.

## 17. Console architecture

Keep the Console embedded, build-free, and fully local. Use
`D:/cc/auto_agent/antseer-monorepo-main/backend/app/static` as an organizational
reference: shared browser infrastructure lives in a small library file, each
business domain owns a separate JavaScript file, and one application file owns
bootstrap and navigation.

Do not copy the reference's CDN-loaded experimental assets. In particular,
`privy-test.html` loads React and Babel from public CDNs and `admin.html` loads
remote fonts. Harness Console JavaScript must work with no Internet access.

Target structure:

```text
pkg/console/static/
  index.html
  css/
    base.css
    console.css
  assets/
    alpine.min.js
  js/
    lib.js                 shared DOM, formatting, and notification helpers
    api.js                 request, auth header, upload, and SSE client
    router.js              domain registration and hash navigation
    store.js               identity, session, and notification shared state
    enums.js               verified wire constants only
    auth.js                login, logout, activation
    overview.js
    sessions.js
    runs.js
    capabilities.js
    profiles.js
    approvals.js
    evaluations.js
    evidence.js
    runners.js
    storage.js
    accounts.js
    resources.js
    observability.js
    app.js                 composition and bootstrap only
```

Every business domain has its own JavaScript entry file. If a large domain later
needs private supporting files, its public entry and registration still remain
owned by that single domain. Shared code may live only in `lib.js`, `api.js`,
`router.js`, `store.js`, or another narrowly named infrastructure file; it may
not be moved into a generic catch-all.

Borrow the reference Console's `registerModule(...)` pattern while avoiding its
global mutable coupling. Each domain registers itself through a local UI module
contract:

```js
registerConsoleModule({
  id: 'capabilities',
  navigation: [...],
  mount({ api, router, sharedStore, permissions }) {
    // Register only this domain's state, loaders, and actions.
  },
})
```

The registration context is deliberately narrow. A domain cannot obtain
arbitrary backend stores, another domain's mutable ViewModel, or the raw bearer
token. Adding or removing one domain file must not require changes to unrelated
domain implementations.

Rules:

- every JavaScript dependency is stored below `pkg/console/static` and embedded
  into the server binary;
- no `<script>` may load `http:`, `https:`, or protocol-relative JavaScript;
- JavaScript, CSS, fonts, icons, and framework assets required to render the
  Console are served locally; user-provided content URLs are not runtime
  dependencies;
- the Console must start and perform local administration with outbound
  Internet access disabled;
- Alpine remains vendored locally; a version and upstream source note must be
  recorded when it is updated;
- `go:embed static` remains the single asset inclusion boundary;
- script load and bootstrap order is deterministic and covered by a Console
  smoke test;
- one domain file owns its ViewModel, event handlers, rendering helpers, and
  calls to the shared API client;
- `app.js` may compose domain modules but may not reimplement their business
  behavior;
- a missing optional domain module does not prevent the remaining Console from
  starting; required modules fail with a clear local diagnostic;
- one API client owns auth headers, error envelopes, upload, and SSE helpers;
- ViewModels map explicitly to request DTOs;
- frontend enum constants are generated from or verified against the OpenAPI
  contract;
- no domain reaches into another domain's mutable state; cross-domain actions
  use explicit application events or narrow exported functions;
- no domain file calls `fetch` directly except the shared API client and an
  explicitly reviewed protocol adapter;
- external images or user-configured endpoints are data, not JavaScript
  dependencies, and remain subject to their existing validation rules;
- Console embedding and `/console` behavior remain unchanged.

Console verification includes:

- enumerate every script source in `index.html` and reject remote URLs;
- request every local JavaScript asset through the embedded HTTP handler and
  require a successful response and JavaScript content type;
- run login, first activation, navigation, ordinary JSON request, upload, and
  SSE smoke paths against the embedded assets;
- ensure no Console JavaScript file is silently omitted from the embedded
  filesystem;
- retain cache-busting versions or content hashes for changed local assets.

Optional backend modules may contribute Console UI through an explicit Console
module registry. Such a contribution supplies a local `fs.FS`/`embed.FS`, module
metadata, navigation declarations, and permission requirements. The central
Console mounts it under a bounded local asset prefix. A UI module cannot open
its own HTTP listener, inject remote script URLs, or bypass the shared API
client and authenticated server APIs.

## 18. Error handling

Errors flow outward as typed categories:

```text
adapter error
  -> domain/application error
  -> HTTP error mapper
  -> stable JSON error envelope
```

Application and domain packages do not return HTTP status codes. Adapters wrap
errors with operation context without exposing secrets. Error comparisons use
`errors.Is`/`errors.As`, not message matching.

Stable categories include invalid input, unauthenticated, forbidden, not
found, conflict, limit exceeded, unavailable, unknown external outcome, and
internal failure.

## 19. Executable architecture rules

Extend `internal/modulecheck` so CI rejects:

- `core` imports outside the standard library;
- domain/application imports of `net/http` or `database/sql`;
- HTTP adapter imports of concrete SQL adapter packages;
- SQL base packages importing app, HTTP, control, evaluation, or extensions;
- extension-to-extension imports unless explicitly documented;
- new anonymous request structs in public handlers;
- new normal responses built with `map[string]any`;
- exported persistence row types;
- new generic `utils`, `common`, `helpers`, `models`, or `enums` packages;
- new magic-string domain states in Service signatures.

New production files should normally remain below 500 lines. Files over 800
lines require a documented reason. Generated files and immutable migration
snapshots are exempt. Line limits are review signals, not substitutes for
cohesion.

## 20. Compatibility strategy

Before moving public symbols, create a public API inventory and external
compile fixtures.

Compatibility mechanisms:

- type aliases when type identity must remain unchanged;
- wrapper constructors when implementation packages move;
- facade methods for legacy Store types;
- deprecated comments that point to the new API;
- unchanged `/v1` route, JSON field, and status values;
- unchanged SQL schema and migration history;
- a major version boundary before removing old import paths.

Legacy facades receive bug fixes required for compatibility but no new feature
ownership.

## 21. Migration phases

### 21.0 Status ledger (2026-09-02; preserve completed work)

This ledger is additive. The phase definitions below remain the source of
intent; an unchecked item does not mean that established work is to be redone.

| Phase | Status | Established evidence and remaining boundary |
| --- | --- | --- |
| 0: baseline | substantially established | public API inventory and external compile fixtures, modulecheck, OpenAPI verifier, and storage fresh/historical checks exist; extend them rather than replace them |
| 1: Identity/Auth | reference slices established | app/identity, named auth/account DTOs, account-admin application boundary, activation behavior, and compatibility work are established; retain storage/server facades until a major version |
| 2: HTTP/application | in progress | shared JSON transport codec, account/tenant and settings use cases/DTOs, and server domain-file splits are established; remaining handlers still need vertical migration and named response cleanup |
| 3: SQL | in progress | adapter/sql/sqlkit, dialect validation, startup/settings adapter, and mechanical SQL store separation are established; feature repositories and Unit-of-Work convergence remain |
| 4: capability/execution/Runner | not started as a full vertical migration | protected capability and Runner behavior remain the compatibility baseline; preserve it while separating contracts |
| 5: runtime extraction | not started | runtime/agent/context/plugin mechanics are still predominantly in core; move each bounded slice through aliases/wrappers |
| 6: Console/docs/examples | in progress | Console uses local embedded per-domain JavaScript and security regression coverage; formal module registration and authoring guidance remain |

The next stop-the-line work is defined by
[Agent Foundation and Plugin Architecture V2](2026-09-02-agent-foundation-plugin-architecture-v2.md):
M0 repairs profile-model versus wire-model divergence and secret read semantics;
M1 establishes the minimum trusted module host and model extension points; M2
introduces the compatible model-control vertical and protocol migrations. These
gates precede broad runtime or plugin additions.

### Phase 0: behavior and compatibility baseline

- inventory public symbols and package dependencies;
- add external compile compatibility fixtures;
- lock OpenAPI and critical HTTP/SSE behavior;
- create SQLite/PostgreSQL fresh and historical migration fixtures;
- lock approval, Run queue, lease, Session repair, and journal invariants;
- add module dependency checks.

### Phase 1: Identity/Auth reference module

- fix activation request compatibility in backend and Console;
- introduce named DTOs, identity domain models, typed enums, Service Ports,
  private SQL rows, and SQL adapter;
- preserve old `storage.Account` and constructors through facades;
- verify real first login and activation on SQLite and PostgreSQL.

### Phase 2: HTTP transport and application services

- migrate handlers feature by feature to named DTOs and application services;
- replace normal `map[string]any` responses;
- split Server routing from worker lifecycle and infrastructure setup;
- keep `server.New` as a compatibility facade.

### Phase 3: SQL infrastructure and repositories

- extract dialect, transaction, migration, and inspection infrastructure;
- migrate Session, Run, Approval, Evidence, Evaluation, Runner, Audit,
  Observability, Memory, and RAG repositories;
- remove feature imports from SQL base infrastructure;
- preserve cross-table transaction behavior through Unit of Work adapters.

### Phase 4: Capability, invocation, execution, and Runner

- separate capability definition, executor, call, invocation, result, journal,
  and adapter wire models;
- split Runner model, state machine, Store, Hub, Provider, worker protocol, and
  admin service;
- move concrete execution adapters under the adapter family;
- preserve protected invocation behavior.

### Phase 5: Runtime extraction

- move Agent execution, composition, context, and plugin mechanics from the
  oversized `core` package into `runtime`;
- retain `core` aliases/wrappers;
- verify external integration fixtures.

### Phase 6: Console, documentation, and examples

- split Console assets so one business domain owns one local JavaScript file;
- retain vendored local Alpine and prohibit remote JavaScript dependencies;
- add embedded-asset and offline Console smoke tests;
- add extension authoring templates;
- document how to add a Capability, model adapter, repository, HTTP module,
  evaluator, and reversible product module;
- update starter examples to use the new public path while compatibility tests
  continue exercising old paths.

## 22. Verification gates

Every phase must pass:

```text
go list ./...
go test ./...
go vet ./...
OpenAPI verifier
module dependency checks
SQLite fresh start
SQLite historical migration
PostgreSQL fresh start
PostgreSQL historical migration
real login and first activation smoke
async Run / approval / Runner smoke
```

Sensitive phases additionally run targeted race and concurrency tests.

The phase is incomplete if behavior is only unit-tested but not verified
through the public API and real storage adapter.

## 23. Developer experience deliverables

The refactor is complete only when a new contributor can find and follow:

- `docs/architecture.md`: dependency direction and invariants;
- `docs/development/module-layout.md`: ownership rules;
- `docs/development/add-capability.md`;
- `docs/development/add-model-provider.md`;
- `docs/development/add-storage-adapter.md`;
- `docs/development/add-http-endpoint.md`;
- `docs/development/add-module.md`;
- starter examples that use only public contracts;
- modulecheck failures with actionable explanations.

## 24. Success criteria

1. Core remains standard-library-only and exposes a smaller stable surface.
2. External integrations can be replaced through typed Ports.
3. Optional product features can be installed and uninstalled as Modules.
4. HTTP handlers depend on application services, not concrete SQL stores.
5. SQL rows are private and never escape persistence adapters.
6. Domain enums are typed and validated at every external boundary.
7. The SQL base layer imports no product feature package.
8. Existing databases upgrade in place with no data loss.
9. Existing `/v1` clients and supported Go integrations continue to work.
10. Identity/Auth demonstrates the complete reference architecture.
11. Every subsequent feature follows the same discoverable vertical path.
12. Architecture rules are executable and enforced by CI.
