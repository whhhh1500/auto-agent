# Graph checkpoint history and execution fencing design

Status: implemented and verified in the current checkout on SQLite and a live
local PostgreSQL 17 instance

## Intent

Adopt the useful part of GopherGraph v1.1.3 -- immutable checkpoint history --
without replacing Harness Core's stronger compare-and-swap, transition,
approval, lease, and multi-database semantics.

The default Graph persistence model becomes:

```text
graph_checkpoints            one mutable, CAS-protected current head per run
graph_checkpoint_versions    immutable full checkpoint versions
graph_transitions            immutable transition evidence
graph_segment_leases         current segment/generation execution authority
```

The current head remains the hot O(1) read path. History is an optional narrow
capability and does not enlarge the executor's `Store` dependency.

## Decisions

### Keep the current head

`graph_checkpoints` remains one row per `(tenant_id, session_id, run_id)`. A
conditional update on the expected revision selects the only writer that may
advance a run. The system will not use `SELECT MAX(version) + 1` and will not
depend on a process-local mutex for correctness.

### Append one immutable version per successful commit

Schema v41 adds `graph_checkpoint_versions`. `Store.Create` and
`Store.CompareAndSwap` must write the head, version, and transition in one
transaction. A partial commit is forbidden. Exact retries return
`CommitReplayed` and do not add another version.

Each version contains:

- a deterministic checkpoint version ID derived from the stable run key and
  revision;
- the previous version ID as its parent for revisions after one;
- a SHA-256 digest of the canonical checkpoint document;
- the bounded canonical checkpoint document;
- a database-assigned creation timestamp.

The version ID is an identity, not a content hash. The digest detects document
corruption and supports cheap comparisons without exposing state.

`CheckpointVersionOrigin` makes lineage explicit: `commit` is the ordinary
same-run chain, `migration_floor` is the honest v40 backfill floor, and `fork`
is a revision-one destination that references a distinct external source
version. A normal `commit` revision one has an empty parent; later commits name
the same-run prior deterministic ID. `migration_floor` always has an empty
parent. `fork` is revision one only and requires a strict lower-case SHA-256
parent ID that is not its own ID.

### History is an optional port

The hot `graph.Store` contract stays unchanged. A separate `graph.HistoryStore`
exposes bounded `LoadVersion` and `ListVersions` reads. Callers must capability-
check this interface. Storage plugins that only provide current-state CAS can
remain valid Store implementations.

`ListVersions` returns metadata only; it must not materialize up to 256 full
512 KiB checkpoints. `LoadVersion` returns one validated snapshot.

### No in-place rollback

Historical versions are immutable and a run head is never moved backwards.
Future recovery from history is an application-level Fork operation that
creates a new run and records the source version as lineage. It must obtain a
new segment lease, preserve frozen definition/composition/implementation
revisions, pass policy and approval checks, and never silently replay external
side effects.

Fork is deliberately not a method on the persistence Store: it is a use case
that coordinates identity, authorization, lease acquisition, audit, and the
initial destination commit.

### Upgrade truthfully

Existing schema-v40 databases contain only the current complete checkpoint.
The v41 migration backfills that current row as the first retained history
version. If its revision is greater than one, its empty parent explicitly marks
the historical floor; missing older versions are not fabricated from
transitions.

Corrupt existing checkpoint documents fail the migration closed.

### Fence execution immediately before persistence

The executor verifies the active segment/generation after node execution and
again immediately before a checkpoint mutation. Losing a lease must not be
converted into a terminal Graph write by the stale worker.

SQL transaction-level lease fencing is a later adapter capability because the
generic Store intentionally knows nothing about a lease holder. The common
executor verification closes the known post-node window without coupling
in-memory and third-party stores to SQL lease tables.

### Keep large bodies out of checkpoint history

The existing checkpoint limit remains authoritative. Large tool output,
documents, images, traces, and binary payloads are stored by the Artifact
extension and referenced by bounded IDs and digests. Object-store
synchronization stays outside the Graph commit transaction.

### Retention is policy, not a blind keep-N delete

Pruning is not part of v41's hot Store. A later retention use case must protect
the head, suspended approvals, executing/unknown states, fork ancestors, and
audit-retained versions. It must be bounded, observable, and safe under CAS.

## Public contract

The history capability uses standard-library-only value types:

```go
type CheckpointVersionInfo struct {
    ID             string
    ParentID       string
    Origin         CheckpointVersionOrigin
    Key            CheckpointKey
    Revision       uint64
    CheckpointHash string
    CreatedAt      int64
}

type CheckpointVersionOrigin string

const (
    CheckpointVersionOriginCommit         CheckpointVersionOrigin = "commit"
    CheckpointVersionOriginMigrationFloor CheckpointVersionOrigin = "migration_floor"
    CheckpointVersionOriginFork           CheckpointVersionOrigin = "fork"
)

type CheckpointVersion struct {
    Info       CheckpointVersionInfo
    Checkpoint Checkpoint
}

type HistoryStore interface {
    LoadVersion(context.Context, CheckpointKey, uint64) (CheckpointVersion, error)
    ListVersions(context.Context, CheckpointKey, uint64, int) ([]CheckpointVersionInfo, error)
}
```

Pagination is ascending by revision and exclusive of `afterRevision`, matching
transition pagination. Limits are validated before storage access.

## Required invariants

1. The head revision, version revision, checkpoint document revision, and
   transition revision are identical.
2. Every version has a valid explicit origin. A `commit` revision one has no
   parent and a later `commit` names deterministic version N-1; a
   `migration_floor` has no parent; a `fork` is revision one only and names a
   distinct strict external source-version ID.
3. A successful Create/CAS leaves exactly one head, version, and transition.
4. A failed Create/CAS leaves none of its proposed facts.
5. Exact replay creates no duplicate row and returns `CommitReplayed`.
6. Stored JSON is decoded strictly and validated against the public contract.
7. A digest mismatch is corruption and fails closed.
8. History APIs never mutate the head or execute nodes/tools.
9. Lease verification failure prevents the stale executor from committing.

## Verification

- contract validation and identity tests;
- in-memory Create/CAS/replay/history isolation tests;
- SQLite fresh schema, v40 upgrade, atomicity, pagination, corruption, and
  concurrent-CAS tests;
- PostgreSQL fresh schema, v40 upgrade, atomicity, and concurrent-CAS tests;
- executor lease-loss-after-node and lease-loss-before-commit tests;
- complete package tests, race test, vet, static analysis, and repository
  module/API checks.
