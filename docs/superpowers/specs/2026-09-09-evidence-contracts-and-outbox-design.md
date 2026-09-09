# Versioned coverage, external-effect receipts, and route-evidence outbox

Status: approved design. This document defines the first production contract for task-level coverage, provider-neutral external-effect verification, and durable delivery of route-evidence projections. It does not reinterpret existing Session events, Tool Journal rows, notification receipts, runtime effect records, or OTel spans as stronger evidence than they already provide.

## Goals

The implementation must answer three different questions without collapsing them into one success flag:

1. What evidence did this exact version of a task require?
2. What did a provider-authoritative read-back prove about an external effect?
3. Was a content-free route-evidence projection durably recorded and delivered to an explicitly acknowledging sink?

Every conclusion uses `satisfied`, `failed`, or `unavailable` semantics. Missing, conflicting, unsupported, timed-out, or historically absent evidence is `unavailable`. Release and canary gates fail closed when a required conclusion is not `satisfied`.

The design keeps `pkg/core` unchanged. New public contracts live in application or evaluation packages. SQL-only recovery stays in storage and server composition. Provider behavior remains behind dynamically registered interfaces; the framework does not enumerate platforms.

## Evidence hierarchy

The canonical sources remain:

- Session events for run identity, terminal state, route metadata, model usage, and archived context facts;
- Tool Invocation Journal for local guarded invocation state and canonical capability results;
- the external-effect ledger plus provider read-back for confirmed external effects;
- Dataset and Case definitions for evaluation requirements.

Coverage results and route receipts are immutable, content-free projections over those sources. They are useful for gates, audit, and export, but do not replace their sources. OTel spans are observations derived from projections and are never canonical audit records.

The following statements stay distinct:

| Statement | Required proof |
| --- | --- |
| A guarded tool invocation completed locally | completed Tool Invocation Journal record |
| A provider accepted a request | bounded provider submission receipt |
| The intended external effect exists | provider-authoritative read-back bound to the operation and intent digest |
| A route covered its frozen probe plan | Session plus Tool Journal reconstruction under the exact route protocol |
| Route evidence was durably materialized | immutable route receipt row bound to a terminal Session cursor and projection digest |
| Route evidence reached an audit sink | fenced outbox acknowledgement from a sink that synchronously accepts the stable receipt ID |

## CoverageContract v1

`CoverageContract` is an optional part of `evaluation.Case`. It is therefore frozen by the existing Dataset ID, version, and canonical Dataset revision. A Case without a contract remains readable for backward compatibility. A strict coverage gate cannot pass it.

The v1 contract has the exact identifier `coverage-contract/v1` and contains four independent requirement groups:

- quality: whether the individual Case must pass its frozen assertions;
- route: the exact supported route protocol and requirements for verified identity, complete target coverage, and exact-once local completion;
- effects: capability identity, bounded occurrence cardinality, and the required evidence level;
- cost: complete usage ledger requirements and optional maximum reported tokens and model calls.

The only v1 effect evidence levels are:

- `journal_completed`, which proves local Tool Journal completion and nothing about a target system;
- `provider_read_back`, which requires a confirmed record from the external-effect contract.

Validation rejects unknown contract versions, unknown requirement values, duplicate effect requirement identities, duplicate capability requirements, invalid cardinality, excessive collection sizes, empty enabled sections, and non-canonical revisions. Requirements are normalized into a map-free canonical form, effect requirements are sorted by ID, and SHA-256 is computed with the revision field omitted. An empty revision is populated during Dataset validation; a supplied mismatch is rejected.

`CaseResult` may contain a content-free `CoverageEvidence` snapshot. It records the contract version and revision, an overall three-state result, fixed reason codes, bounded counts, route protocol facts, receipt states, and reported resource totals. It never contains prompts, answers, tool arguments, tool results, external target identifiers, provider response bodies, URLs, credentials, raw errors, or trace IDs.

The evaluation runner constructs coverage evidence from the Case contract and host-observed sources. It does not accept caller-supplied route or external-effect claims. Release evaluation revalidates required coverage from the canonical Session, Tool Journal, and external-effect store before staging. Stored `CaseResult.Coverage` is an audit snapshot, not sufficient authority by itself.

The release order is:

1. existing quality gate;
2. existing capability compatibility check;
3. CoverageContract revalidation;
4. optional efficiency comparison.

Efficiency comparison runs only when every required candidate and baseline Case has satisfied coverage. A lower token count cannot compensate for an incomplete task or unavailable effect evidence. Canary artifacts persist the required coverage contract marker and the complete coverage gate result. Restore rejects immutable coverage marker or result drift.

The first version uses existing JSON storage for Dataset definitions and Case results, so it does not require indexed coverage columns. Old JSON remains readable. Query indexes may be added only after an actual query requirement exists.

## Provider-neutral external-effect contract

The new application package owns four concepts:

- an immutable `Intent`, bound to tenant, subject, session, run, call, capability, driver revision, request digest, target digest, and a host-derived operation key;
- a dynamically registered `Driver`, which can submit an intent and perform provider-authoritative read-back;
- a `Store`, which durably enforces identity, state transitions, fencing, and immutable evidence digests;
- a `Service`, which sequences persistence and provider operations.

The operation key is derived by the host from the complete invocation identity, driver identity, and intent digest. Models, capability arguments, and transport clients cannot choose it. The stored public projection contains only bounded identifiers, digests, state, fixed error codes, attempt counts, and timestamps. Any opaque provider query reference is private adapter state and must be sealed with authenticated encryption bound to tenant, session, run, call, driver, and intent digests. It is absent from public readers and logs.

The state machine is:

```text
prepared -> dispatching -> accepted -> confirmed
                         \-> rejected
                         \-> unknown
              \-> confirmed/rejected/unknown after read-back
accepted/unknown/dispatching -> confirmed/rejected/unknown after read-back
```

The Service must persist `dispatching` before crossing the provider boundary. A crash after this transition may have occurred before or after the provider accepted the request, so recovery never submits automatically from `dispatching`, `accepted`, or `unknown`. It performs read-back first.

`Dispatch` can return a bounded submission acknowledgement. This may move the ledger to `accepted`; it cannot move it to `confirmed`. Only `ReadBack` can confirm an effect, and only when its observation binds the exact operation key and intent digest. A definitive provider observation that the effect does not exist or conflicts with the intent may produce `rejected`. Pending, unsupported, malformed, mismatched, timed-out, or ambiguous observations produce `unknown` or remain unresolved.

Driver panics and arbitrary errors are isolated. Persistence receives fixed error classes, never raw provider text. A capability with no read-back Driver can continue using its existing behavior, but it cannot satisfy `provider_read_back` coverage. Existing notification `accepted` and `delivered` values keep their current compatibility meaning and are not silently promoted to confirmed external-effect proof.

SQL persistence is a sidecar ledger rather than an added flag on `tool_invocations`. It binds the complete invocation and intent identity, enforces legal state transitions and idempotent canonical replay, and indexes unresolved records by driver identity and state. The external-effect schema follows the route-outbox migration so each migration has one explicit responsibility.

Native strict execution needs a narrow SQL-only atomic admission extension before it can promise crash-safe automatic submission. That transaction must recheck the queue lease generation, current authorization epoch, native tool-effect witness, Tool Journal begin, and external intent transition to `dispatching`. Until that extension is active for a Driver, native recovery may read back unresolved effects but cannot automatically dispatch them.

## Route-evidence receipt and outbox

The route path uses two tables.

`route_evidence_receipts` is immutable. One row binds:

- protocol `route_evidence_receipt/v1`;
- session and run identities;
- terminal Session event sequence and terminal status;
- source Session version;
- canonical content-free projection JSON and its SHA-256;
- stable receipt ID and creation time.

The receipt ID is derived from the protocol, session ID, run ID, terminal event sequence, source Session version, and projection digest. Repeating the same materialization is a no-op that reads back and verifies every immutable field. The same source identity with a different payload or digest is a conflict and fails closed; no code overwrites an immutable receipt.

`route_evidence_outbox` is a mutable delivery ledger keyed by receipt ID. Its states are `pending`, `leased`, and `delivered`. It holds availability time, lease owner, monotonically increasing generation, lease expiry, bounded attempt count, fixed last-error class, and delivered time. Receipt creation and initial outbox insertion occur in one SQL transaction.

Terminal Session persistence and materialization do not share one transaction in the existing architecture. The Session and Tool Journal remain authoritative, and a reconciler closes that crash window. Immediate terminal paths try to materialize after durable completion. At startup and periodically, the reconciler scans terminal run candidates, reloads and validates the canonical Session and Journal, and idempotently materializes missing receipts. It may use `run_evidence` or `run_control` only to find candidates, never as the evidence source.

Outbox claiming is fenced by receipt ID, lease owner, and lease generation. PostgreSQL uses row locking with `FOR UPDATE SKIP LOCKED`; SQLite uses a conditional select-and-update transaction. Expired leases are reclaimable. Ack and retry updates fail when the owner or generation is stale.

Delivery is at least once. A process can be killed after a sink accepts a receipt but before SQL ack, so the same receipt may be delivered again. Every sink receives the immutable receipt ID as its deduplication key and must persist its own uniqueness boundary if duplicate effects are unacceptable.

A route-evidence delivery port returns success only after the sink synchronously accepts the receipt and stable ID. That success permits outbox ack. The existing generic OTel span API has no collector acknowledgement, so the default span projection remains best effort and cannot mark an outbox row delivered. Deployments without an acknowledging sink still gain immutable local route receipts; their outbox rows remain pending with bounded backoff rather than reporting false delivery.

Retention may delete delivered outbox delivery state after the configured window. It never deletes pending or leased work. Immutable receipts use the audit retention policy and are deleted only after their delivery state is delivered and outside the retention window. Provider-unknown external-effect records are retained until resolved or an explicit audit policy handles them; retention never converts unknown into success.

## Server composition

Generic servers may receive optional external-effect, route materialization, and acknowledging delivery ports. Their absence preserves current behavior and keeps strict coverage requirements unavailable.

`NewNativeStrictServer` constructs SQL implementations from its owned database and dialect. Its private ownership token carries those implementations into the server, so generic composition cannot acquire native-static recovery claims through ordinary `Config` fields. Native worker startup runs one bounded reconciliation pass before normal background loops, then starts route outbox claimers and periodic reconciliation under the existing worker lifecycle context.

Direct and queued terminal paths perform immediate best-effort materialization only after terminal persistence succeeds. Failure to materialize does not rewrite a completed run as failed because reconciliation can repair the derived projection. It is recorded with a fixed error class and observable counter. A verified conflict is a durable integrity incident and remains fail closed for gates and delivery.

## Failure behavior

| Failure window | Recovery behavior |
| --- | --- |
| terminal Session durable, route receipt absent | reconciler reconstructs and creates exactly one receipt/outbox pair |
| route receipt committed, process killed before claim | worker claims the existing pending row |
| sink accepted, process killed before ack | lease expires and the same receipt ID is delivered again |
| external intent persisted as dispatching, process killed before/after provider submission | recovery performs read-back; it does not submit again |
| provider accepted, ledger update lost | read-back resolves confirmed/rejected/unknown |
| read-back observation does not bind operation key and intent digest | record remains unknown and strict coverage is unavailable |
| Tool Journal says completed but no confirmed provider record exists | journal-level requirement may pass; provider-read-back requirement is unavailable |
| stored projection conflicts with recomputed canonical evidence | materialization and gate fail closed; immutable row is not overwritten |

## Validation

Pure contract tests cover canonical revisions, order normalization, unknown versions, invalid cardinality, three-state aggregation, effect evidence levels, cost bounds, and backward-compatible JSON.

Application tests use deterministic fake Drivers and Stores to cover every external-effect state transition, panic isolation, stable operation keys, duplicate intent replay, identity conflicts, dispatch-before-call ordering, crash windows, read-back mismatch, and the rule that accepted never implies confirmed.

SQLite and PostgreSQL tests cover fresh schema creation, each incremental migration, immutable receipt conflicts, external intent conflicts, transaction rollback, concurrent writers, two-worker claims, expired leases, stale generation ack, retry backoff, and retention eligibility. PostgreSQL tests run only against the repository's real configured test database; absence is reported as a skip rather than treated as PostgreSQL evidence.

Child-process hard-kill tests cover:

- terminal Session commit before route materialization;
- route sink accept before outbox ack;
- external dispatching commit before provider call;
- provider acceptance before accepted-state persistence;
- confirmed read-back before the corresponding local tool result reaches the Session.

The acceptance sequence is targeted package tests, SQLite integration, real PostgreSQL integration when configured, race tests for evaluation/storage/server/application packages, hard-kill tests, full `go test ./...`, `go vet ./...`, and `git diff --check`.

## Delivery batches

1. CoverageContract types, canonical revision, pure evaluation, JSON compatibility, and focused tests.
2. Provider-neutral external-effect application contract and deterministic state-machine tests.
3. Route receipt/outbox SQL migration, claim fencing, reconciliation, sink port, worker lifecycle, SQLite/PostgreSQL and hard-kill tests.
4. External-effect SQL ledger, one explicitly read-back-capable reference adapter, native strict admission integration, and recovery tests.
5. Release/canary coverage revalidation, documentation updates, full gates, and live-model regression only when the contract changes model-visible behavior.

Each substantial batch is committed after its focused checks pass. The final integrated branch is pushed only after the full repository gate succeeds.

## Explicit limits

Complete route-plan coverage does not prove that the plan is semantically sufficient for every user request. Frozen Case assertions remain the task oracle.

Reported model usage does not prove provider billing or hidden provider costs. A provider receipt without authoritative read-back does not prove the intended target state. At-least-once outbox delivery does not provide exactly-once remote processing without receiver-side deduplication. Historical records are not retroactively upgraded when their required evidence was never collected.
