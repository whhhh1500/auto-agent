# Native queued model worker — implementation boundary

Date: 2026-09-07. This note records the first-batch native queued model-worker
contract. Its implementation acceptance was recorded on 2026-09-08. The
completed-tool-result coordinator below is the accepted second-batch
native-static recovery boundary, verified on 2026-09-08.

## Atomic outcome batch

`SQLSessionStore.AppendNativeQueuedModelOutcomeBatchFenced` accepts a complete
pending Session batch and the event count of its model-outcome prefix. The
prefix contains exactly one canonical `assistant/message` followed by
`run/usage` with the durable `model:<step-start-seq>` identity. Its v46 row
continues to bind and hash only that prefix.

The storage method takes the existing SQL lock order: queued Session fence
(run control, queue generation, Session lease and Session CAS), v45 attempt,
event chunk, v46 outcome and final fenced Session-tip update. It validates the
whole same-Run event sequence by restoring it against the durable base Session,
then commits the full chunk, v46 evidence and target Session version in one
transaction. A failed suffix write rolls back the outcome prefix too.

A response-lost retry is a convergence only when the durable Session version
equals the exact full-batch target, every submitted event exactly matches that
durable suffix, and the stored v46 evidence matches the supplied outcome
prefix. A higher Session version, a changed suffix, a second model outcome, a
cross-Run event or an invalid restored Session is rejected.

## Historical model grammar

Before v45 admits a later `step/start`, storage now parses all completed prior
steps. It requires the assistant-declared tool-call sequence to agree exactly
with each durable `tool/call` ID, capability name and arguments, and requires a
matching `tool/result` before the step ends. This rejects damaged historical
steps instead of issuing a new model attempt from an ambiguous history.

Each approval segment accepted before a later native model admission has the
Core-produced sequence `approval/requested`, `run/resume`, `tool/result`, then
`approval/resolved(approved)`. Its request and resume calls, remaining
assistant-declared call tail, step, approval ID, result call ID and decision
must agree exactly. Fast routing, denied/expired decisions, a duplicate event
within that segment, and another ordering are not model-admissible. A later
tool call may form a separate, complete approval segment. The resume
composition carries nonempty profile and capability snapshot IDs; storage
validates its canonical composition and assignment revisions, retains the
sealed deployment's model selection and adapter identity, and uses that
composition as the v45 contract source for the later step. The capability
snapshot ID is an opaque Core digest here, not a value storage recomputes from
the audit composition. `run_start_seq` continues to identify the original Run
start; the existing v45 profile/capability/composition/assignment/model-contract
columns describe this latest validated source segment. No new schema field is
needed because admission and outcome validation re-derive that segment from
the exact durable Session prefix before accepting either a first write or a
response-lost convergence.

## Boundaries retained

This change does not add a Core API or schema version. It does not provide a
recovery coordinator, current continuation authorization, sidecar consume/ack,
or tool-result auto-continuation. Native static execution stays queued-only and
uses a dedicated SQL authority. An existing v45 row without v46 outcome remains
a permanent prohibition on model-provider replay.

## Completed-tool-result coordinator scope

This section records the accepted narrow native-static implementation. It does
not establish a generic or dynamic deployment acceptance claim. Its server
coordinator must run after a worker
holds its current queue claim and Session lease, has loaded and checked the
current owner, and before `RepairInterruptedSessionFenced`. Repair closes every
open Run, including an otherwise valid post-result tail.

The coordinator has only two candidate windows. In window A the authoritative
Session tail is one canonical `tool/call` and its exact SQL tool journal row is
`completed`, but the result has not been delivered to the Session. A matching
v44 native effect witness and its same-Run `run/start` composition must prove
the original invocation, call sequence, snapshots, revisions and composition
hash. The delivery transaction must then write exactly one canonical
`tool/result`, its v43 sidecar, and the Session tip together. A complete
journal alone is never enough.

In window B the authoritative Session tail is one `tool/result` with the
matching v43 sidecar and the same call's exact v44 native admission witness.
The current fence readback must prove the v43 sidecar, v44 lineage, journal
result, exact original call and result digest before `ContinueTurn`. V3 is a
generic historical-delivery record and cannot independently establish Native
lineage. The v43 authorization epoch and v44 queue generation are historical
delivery and admission witnesses; neither authorizes recovery. Each recovery
attempt locks the *current* expected SQL authorization epoch and the current
queue generation, Session lease and Session CAS. The server then resolves the
active principal and current native-static runtime. `ContinueTurn` itself
validates the Session owner, open Run, post-result grammar and current
capabilities; every future tool/model effect repeats its own current fenced
admission.

The SQL-only entry point is deliberately tail-derived: callers do not supply an
invocation, result or sidecar payload that could be forged, and it does not
expose a recovery mode. Those records are private storage-layer proof inputs,
rather than a second public representation for the worker to interpret.

```go
func (s *SQLSessionStore) RecoverNativeQueuedCompletedToolResultFenced(
    ctx context.Context,
    fence SessionWriteFence,
    expectedCurrentAuthorizationEpoch, expectedSessionVersion int64,
) (session *core.Session, recovered bool, err error)
```

`recovered` is true only after the method has atomically delivered window A or
has authoritatively confirmed window B. A nil Session with `recovered == false`
means there is no candidate tail. A candidate whose journal, sidecar, witness,
v45/v46 history, grammar or fence proof contradicts the required evidence is
an error: the caller fails closed and does not replay it.

The transaction locks, in native SQL order, current authorization epoch, run
control, queue generation, Session lease, and Session row/CAS. It validates
every same-Run v45/v46 pair before either candidate branch: a v46 row must
exist, bind the attempt request hash, and validate against the durable outcome
events and digest. For window A it then locks v44, journal and v43 in that
order, confirms that no result or sidecar is already durable, writes the exact
result chunk and v43 sidecar, re-reads journal, witness and current epoch, then
advances the fenced tip. For window B it first reads V3 only to obtain an
identity, then locks v44, journal and V3 in the same order; its final locked V3
row must exactly equal the preliminary identity before it can return the
authoritative Session. Any failure rolls back the whole delivery. Neither path
accepts a caller-provided invocation or sidecar.

This is idempotent consumption only within the existing native-static effect
fences. A crash before `ContinueTurn` can cause another owner to coordinate
the same sidecar, but no provider has run. Once continuation starts, v44 plus
the tool journal fence each subsequent tool effect; v45/v46 fence every model
effect, and a v45 attempt without v46 remains non-replayable. This is not a
durable `coordinator started` record or a general exactly-once scheduler. A
deployment that lacks those native guards must not claim this property.

The following tails are not eligible for automatic continuation and continue
through existing repair or terminal handling: terminal or waiting-approval
Runs; a tail other than exactly one `tool/call` (A) or sidecar-bound
`tool/result` (B); missing, duplicate or mismatched journal/sidecar/witness
evidence; more than one pending tool result; malformed or cross-Run grammar;
partial model chunks; an assistant/message without its paired model usage;
`step/start`, `step/end`, or a new step whose model attempt/outcome is absent;
and every unknown v45 model attempt. A current inactive/mismatched principal,
epoch/projection mismatch or lost queue/lease fence also blocks delivery and
continuation. The coordinator must never search historical calls to turn one
of these ineligible tails into a candidate.

After a current principal and live fence reach the coordinator, a permanent
recovery-proof failure can append only `run/error` and failed `run/end` through
that fence; it never fabricates a tool result or step closer. This terminal
suffix is distinct from two existing preflight boundaries. Cancellation
deliberately invalidates the current fence, so its old owner stops and appends
neither a recovery result nor continuation suffix; the expired-claim reaper can
terminalize run control but does not synthesize an already-open Session suffix.
Likewise, a revoked or unresolved current principal is rejected before the
coordinator and may terminalize run control as `principal_resolution_failed`
without closing the open Session. Reconciling these existing control/Session
preflight boundaries needs a separate design; this coordinator does not claim
SQL and HTTP terminal state are identical in either case.

## Second-batch verification accepted 2026-09-08

This second-batch coordinator has separate verification from the accepted
first-batch model-outcome work below. Its focused SQLite storage race gate
currently covers A delivery, B response-lost readback, generic V3 without v44,
missing or mismatched v44, unknown or corrupt v45/v46 history, current epoch,
queue generation, Session lease, Session CAS, cancellation rollback, and two
concurrent A/B callers delivering one result and one sidecar. It uses fixture
adapters only; no live or metered provider participates.

The following targeted PostgreSQL and race gates completed with native exit
`0` in the isolated local acceptance cluster:

```text
D:\cc\auto_agent\.codex-v46-audit\pg-debug-55441\second-batch-native-journal-crash.exit.txt
D:\cc\auto_agent\.codex-v46-audit\pg-debug-55441\second-batch-native-delivery-crash.exit.txt
D:\cc\auto_agent\.codex-v46-audit\pg-debug-55441\second-batch-native-postprovider-crash.exit.txt
D:\cc\auto_agent\.codex-v46-audit\pg-debug-55441\second-batch-storage-recovery-pg.exit.txt
D:\cc\auto_agent\.codex-v46-audit\pg-debug-55441\second-batch-server-native-race.exit.txt
D:\cc\auto_agent\.codex-v46-audit\pg-debug-55441\second-batch-storage-recovery-race.exit.txt
```

The three Native hard-kill cases cover a completed journal before delivery,
window-A delivery before response, and a post-provider model attempt. They use
fixture adapters, not a live or metered provider. The post-provider case leaves
two durable model attempts and one durable model outcome, so a successor must
not replay the unknown attempt. The PostgreSQL storage gate includes concurrent
A delivery and response-lost B readback. The strict PostgreSQL runner completed
with 101 tests, 64 subtests, 10 packages, and zero named-test skips; its test,
PostgreSQL-stop, and wrapper exits are all `0`:

```text
D:\cc\auto_agent\.codex-v46-audit\pg-debug-55441\second-batch-final-test-postgres.jsonl
D:\cc\auto_agent\.codex-v46-audit\pg-debug-55441\second-batch-final-test-postgres.console.log
D:\cc\auto_agent\.codex-v46-audit\pg-debug-55441\second-batch-final-test-postgres.exit.txt
```

The final non-PostgreSQL repository test log
`D:\cc\auto_agent\.codex-v46-audit\second-batch-full-test-native-2.log` and
its exit file report `0`. Build, vet, staticcheck v0.7.0, module check and its
Go 1.25.13 smoke check, OpenAPI, gofmt, and diff checks also completed with
native exit `0`. This evidence verifies the narrow Native-static A/B
coordinator and automatic sequential continuation only; it does not widen the
generic or dynamic recovery surface.

## Retention boundary

Only the existing v45/v46 pair pruner is in scope for later worker wiring. It
locks and verifies each pair and may remove it only when the Run is terminal
or a durable Session successor exists; it never selects v45 attempts without
outcomes, which remain permanent replay fences. v43 sidecars, v44 witnesses
and completed journal rows have no retention change in this slice. In
particular, no epoch, queue-generation or age-only cleanup may remove evidence
that remains a current recovery tail.

## First-batch acceptance recorded 2026-09-08

The accepted storage and worker tests cover SQLite and PostgreSQL success plus
response-lost convergence for an outcome with a tool/step/run suffix. They also
cover trigger-injected suffix and cancellation-fence rollback, stale generation,
changed full batches, a later Session successor, multiple outcomes, and strict
assistant/tool-call name and argument mismatches. The batch/race coverage also
includes ordinary non-model assistant suffixes and a reported model usage after
a provider failure, which does not create a v46 outcome.

The final disposable PostgreSQL runner executed `go run ./scripts/test-postgres`
against its isolated local cluster with `HARNESS_ACCEPTANCE_LIVE_SERIAL` unset.
It reported 100 tests, 62 subtests, 10 packages, and zero named-test skips.
Its test-process, PostgreSQL-stop, and wrapper exit files each contain `0`:

```text
D:\cc\auto_agent\.codex-v46-audit\pg-debug-55441\final-verified-pg.jsonl
D:\cc\auto_agent\.codex-v46-audit\pg-debug-55441\final-verified-pg.console.log
D:\cc\auto_agent\.codex-v46-audit\pg-debug-55441\final-verified-pg.exit.txt
```

The final full-repository gate artifacts report exit `0` for the post-crash
`go test`, build, vet, staticcheck, module check, OpenAPI, and gofmt gates.
The clean-snapshot gitleaks 8.28.0 run reported no leaks across 835 snapshot
files. Targeted server and storage race gates passed, including the PostgreSQL
batch cancellation rollback and fenced-append/claim-renew deadlock checks.

The offline hard-kill gate exercised the existing generic worker at both
`effect_committed` and `journal_completed`: the durable prefix ended at version
6, the business effect count remained `1 -> 1`, no additional model call ran,
and the Run became `run_interrupted`. That is evidence for fail-closed generic
crash handling only. It neither delivers a completed tool result into a Native
Session nor calls `ContinueTurn`, so it is not evidence of Native automatic
continuation. No live or metered model provider participated in these gates.
