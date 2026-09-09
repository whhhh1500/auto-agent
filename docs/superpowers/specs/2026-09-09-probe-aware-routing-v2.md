# Probe-aware automatic routing v2

Status: design approved for an opt-in implementation candidate. The general
profile remains `direct_only`. This protocol does not implement CodePTC and it
does not change the recovery semantics of route protocol v1.

## Problem

`auto_first_action` lets the model choose Direct or PTC without a classifier
request, but it locks Direct as soon as the first ordinary tool is called. A
discovery call such as inventory therefore fixes the route before the model has
seen fan-out or result pressure. Live wave 43 observed exactly this: the Luna
large-result PTC arm passed with 3,884 fewer reported tokens than the passing
auto Direct arm, while auto had already locked Direct on inventory.

Adding a route-classifier model request would increase cost and create a second
model outcome that must be recovered. Keeping `program.catalog` after a probe
would require four model requests: probe, catalog, execute, final. The v2
candidate must preserve three requests for its bounded shape:

1. the model calls one neutral probe;
2. the model chooses a validated Direct batch or a bounded PTC program;
3. the model produces the final answer with no tools exposed.

The model makes the semantic choice. The host determines the authorized and
budget-safe candidate set and validates the chosen action. Model text, user
text, memory, RAG, summaries, capability descriptions and tool result content
never grant authority or expand that set.

## Versioning

Add a new opt-in route mode `auto_probe_once` with a distinct protocol version
and implementation revision. The existing modes retain their exact v1
behavior and revision:

- `direct_only`
- `ptc_only`
- `auto_first_action`

The current v2 implementation revision is
`programmatic-route-projection/v2-probe-once-host-projected-ptc-choice-capacity-admission`.

Recovery first reads the implementation revision frozen in `run/start`
composition. It reconstructs a run only when that revision is currently
supported or an explicit audited migration or revision registry supports it.
The currently unsupported older v2 revisions permanently fail closed: there
is no automatic downgrade, reinterpretation or compatibility path. Unknown,
incomplete or mixed version evidence also fails closed. A v2 run requires a
readable tool journal.

## Capability contracts

A capability opts into neutral probing with the versioned manifest metadata
marker `harness.programmatic.neutral_probe=1`. The marker is a routing trait,
not an authorization grant. At composition and again when reconstructing a
probe receipt, the host requires:

- `Idempotent=true`;
- `RequiresApproval=false`;
- no write or send permission;
- no execution declaration with `Writes=true` or a write-capable sandbox;
- a positive, bounded `MaxOutputBytes` under the v2 probe ceiling;
- ordinary authorization in the current frozen capability snapshot;
- exclusion from the PTC program target catalog.

The probe provider returns normal model-visible `Content` and a trusted,
bounded result metadata value under `harness.programmatic.probe_facts`:

```json
{
  "version": "1",
  "followup_capability_id": "records.detail",
  "candidates": [
    {
      "label": "record-17",
      "args": {"id": "record-17"},
      "facts": {"active": true}
    }
  ],
  "max_model_return_bytes": 4096
}
```

`label` is bounded display data. `args` must validate against the follow-up
capability input schema. `facts` is optional bounded JSON-native data that a
PTC program may filter without receiving the raw probe result. Candidate
count, per-candidate bytes and aggregate bytes have hard ceilings. The
follow-up capability must be present in the same frozen snapshot, explicitly
programmatic, idempotent, approval-free and free of declared writes/send
permissions. Unknown fields or contract revisions fail closed.

The provider owns the conversion from external data to trusted probe facts.
The router never parses free-form probe `Content` to create authority.

## Derived route plan

After a completed probe, the host derives an immutable logical RoutePlan from:

- the v2 route configuration and implementation revision;
- the exact `run/start` composition and assignment revision;
- the capability snapshot and provider revisions;
- the assistant probe call, canonical tool call event and exact completed
  journal record;
- the canonical tool result and bounded probe facts contract;
- the fixed PTC grammar revision and route-plan algorithm revision.

The plan contains only bounded IDs, digests, limits and trusted facts. Its ID is
the SHA-256 digest of its canonical representation. It is reconstructed from
the authoritative session events, journal and frozen composition; no mutable
route-state table is added. A cache or observability projection may be rebuilt
from those facts and is never authoritative.

The plan exposes exactly one follow-up capability through two feasible forms:

- a Direct tool schema whose arguments must equal one of the plan candidates;
- a route-specific model schema for `program.execute` containing only a
  bounded candidate index selection and one finite output projection. The
projection is derived from required top-level bounded string or boolean
fields in the frozen follow-up OutputSchema; it never accepts a model path,
source, binding or child argument.

The Direct schema preserves the frozen follow-up description and appends fixed
route guidance. It exposes only the candidate count: the response is the only
unique Direct batch, selecting Direct locks the route, every task-required candidate
must be called in that same assistant response, and no later tool round is
available. Direct is for a small deliberate subset. When the dynamic execute
schema is eligible, the guidance prefers it for many/all candidates, repeated
follow-up work, or context pressure. If no execute schema is eligible, the
guidance instead states that all required calls must be made in that one Direct
batch. Candidate labels, arguments, facts, probe content and host bindings are
not included in this description.

The dynamic execute schema is recomputed from the frozen plan and has its own
digest, byte and JSON-depth ceilings. Probe content cannot alter the schema.
The model sees neither credentials nor manifest metadata, scope, provider
objects or raw journal data.

The generic `program.execute` capability remains unchanged. Before emitted
tool calls reach Core, the route adapter transforms a valid route-specific
execute call into the generic contract by replacing candidate indices with a
host-owned `input.targets` array containing the exact frozen candidate
`args/facts`, retaining the selected projection in `input`, and supplying a
host-generated canonical PTC source and binding. The source loops targets,
calls only the frozen follow-up, returns one compact projected value per
target, and is compiled against the injected real compiler while constructing
the plan. The transformed arguments are what Core persists and binds in the
tool journal. Duplicate, out-of-range or excessive indices fail before any
tool effect. A follow-up without a safe projection exposes Direct only.

## State machine

The reducer reads only the current user turn and authoritative proof callbacks.
Every selective assistant action is fully buffered and validated before any
tool call is emitted to Core.

| State | Model-visible tools | Valid transition |
| --- | --- | --- |
| `unselected` | catalog, ordinary Direct tools, eligible probes; execute hidden | singleton probe to `probe_ready`; singleton catalog to v1 PTC lock; ordinary-only batch to Direct lock; text to done |
| `probe_ready` | planned Direct follow-up and dynamic execute; probe/catalog hidden | candidate-only Direct batch to `final_no_tools`; singleton execute to `final_no_tools`; text to done |
| `direct_locked` | ordinary Direct tools for the legacy non-probe branch | existing Direct continuation only |
| `ptc_catalog_locked` | verified generic execute only | existing catalog/execute behavior |
| `final_no_tools` | none | one final model response only |
| `blocked` | none | terminate or require a new run |

The adapter rejects probe mixed with any other call, execute mixed with Direct,
a second probe, a hidden catalog, an unplanned Direct capability, candidate
argument forgery, duplicate calls exceeding the plan, malformed IDs, missing
or duplicate results and any event/journal/composition mismatch. Before it
emits a post-probe choice, it validates the full frozen run budget: Direct
validates its complete batch; execute validates its parent plus one child for
every selected target. The validation also checks frozen per-capability budgets.
If the existing call count or child count cannot be reconstructed, the adapter
rejects the whole batch before Core records any choice effect.

This is pre-effect validation for the normal serialized `RunTurn` path. It does
not write a durable reservation or perform a Session compare-and-swap, so it
does not claim to coordinate concurrent `ContinueTurn` attempts on the same
open run. A host that permits such recovery concurrency must use a run-scoped
lease/fence and test the competing-continuation case before making that claim.

## Recovery invariants

1. Probe success is neutral; the first durable non-probe action locks the
   selected route. A locked route never falls back to the other route.
2. A completed probe is reused from its exact journal result. It is never
   reissued under a new call ID. `Started`, `Uncertain`, missing or conflicting
   proof blocks the v2 path unless the native recovery witness can restore the
   exact canonical completed result.
3. Provider or capability revision drift, current authorization failure,
   assignment mismatch, stale lease/fence or an unapplied authorization epoch
   prevents another model/tool admission.
4. An unknown model outcome is never replayed. An unknown execute or child
   effect never falls back to Direct.
5. The designated probe cannot appear in PTC bindings or source. Every child
   call still crosses the existing protected invoker, policy, approval,
   journal and telemetry boundaries.
6. The route-specific execute result is serialized and checked against the
   plan's `max_model_return_bytes` before the final model request. Oversized
   output fails without inserting it into model context.

## Cost and observability

Every model request records content-free stage evidence:

- `probe`, `choice` or `final`;
- route protocol, plan and dynamic schema digests;
- tool count, encoded tool-schema bytes and assembled input bytes/tokens;
- model-call ordinal and complete provider-reported usage when present;
- top-level and protected child call/result counts;
- selected route and rejection reason code.

Raw prompts, answers, probe content, candidate arguments, host-generated PTC
source and tool results stay out of general metrics and safe evidence. Missing usage or ledger
mismatch is `inconclusive`, never zero cost.

Self-improvement may use sealed development/calibration data to propose a new
immutable policy or implementation revision. It cannot modify a live route,
promote itself or write production policy. Holdout quality passes before
paired efficiency comparison; canary remains explicit, reversible and
manually promoted.

## Initial implementation and release boundary

The first implementation supports one probe, one follow-up capability, one
Direct batch or one PTC execute, and one final response. It does not support
multiple probes, multi-phase Direct after a probe, side-effecting follow-ups,
approval after a probe, generic historical result access, dynamic CodePTC,
online threshold learning or automatic promotion.

Required tests cover state transitions, mixed batches, forged and duplicate
candidates, invalid facts, schema and result size/depth limits, journal
missing/conflict/uncertain states, provider and composition drift, probe and
execute hard-kill windows, authorization revocation, exact three-request
accounting and final zero-tool projection. Live evaluation uses the two models
configured by local `llm.txt`, frozen fixtures and one no-retry attempt per arm.
Quality and protected effects must pass before token or context savings count.
