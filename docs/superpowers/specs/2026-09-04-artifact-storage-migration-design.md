# Artifact Storage Migration Design

Status: implemented and verified  
Date: 2026-09-04  
Scope: `D:\cc\auto_agent\harness-core`

## 1. Decision

Large content and generated artifacts use the local filesystem by default. A
single bootstrap path, `HARNESS_DATA_DIR` (default `./data`), owns the local
resource root at `<data-dir>/resources`.

S3-compatible configuration is runtime product configuration. Endpoint,
region, bucket, access key, encrypted secret key, path-style behavior, desired
revision and activation evidence are stored in the primary database. They are
not required environment variables.

Saving an S3/R2 configuration does not immediately replace the active object
store. The service copies and verifies existing local objects, catches up
mutations that occurred during the copy, then atomically activates the remote
backend. Until activation succeeds, local storage remains authoritative and
available.

## 2. Goals

1. zero-configuration startup with a real local filesystem backend;
2. database-authoritative S3/R2 configuration with encrypted credentials;
3. bounded-memory, resumable local-to-remote migration;
4. no missing historical objects after activation;
5. no online request dependency on the candidate remote backend while bulk
   migration is running;
6. explicit, redacted progress and failure evidence;
7. deterministic restart recovery and one active migration worker; and
8. an object-store seam reusable by Graph, sandbox, retrieval and future
   artifact-producing modules.

## 3. Non-goals

This vertical does not migrate Graph checkpoints, sessions, account data or
other SQL facts into object storage. It does not implement multi-region
replication, CDN behavior, arbitrary bidirectional replication, storage
quotas, request admission/429 gates, or automatic deletion of the local
rollback copy.

## 4. Configuration boundary

Bootstrap environment variables remain limited to infrastructure needed before
the database can be read:

- `HARNESS_DATA_DIR` for the local data root;
- database dialect, DSN and pool settings;
- server process settings and production master key.

The primary `.env.example` must not advertise `HARNESS_S3_*` or
`HARNESS_RESOURCE_S3_*` as normal configuration. Existing environment readers
may remain temporarily as a one-time compatibility import only when the
database row is absent. They must be documented as legacy and must never
override a database row.

No separate artifact path is required. The local resource path is derived from
`HARNESS_DATA_DIR`, which avoids another user-facing setting while preserving a
stable filesystem location.

## 5. Package boundaries

```text
pkg/storage
  ObjectStore and streaming object-store ports
  local, memory and S3 implementations
  migration copier and switching store

pkg/app/storageconfig
  desired storage configuration
  migration state/progress application contracts
  administrator use cases

pkg/adapter/sql/artifactmigration
  durable migration state, lease and mutation journal

cmd/server
  composition, startup recovery and bounded worker lifecycle
```

The Graph engine and feature modules depend only on `storage.ObjectStore`.
They do not import storage configuration, S3 clients or migration adapters.

## 6. Durable migration model

A migration is identified by its desired storage revision. At most one
non-terminal resource migration exists at a time.

Closed states:

```text
local_active
syncing
catching_up
verifying
s3_active
apply_failed
cancelled
```

The durable record contains bounded, non-sensitive data:

- migration ID and desired configuration revision;
- source and target backend identities without credentials;
- state and monotonically increasing generation;
- listing cursor and mutation high-water sequence;
- copied object/byte counters and verified object/byte counters;
- bounded error code and redacted detail;
- worker lease owner/expiry; and
- created, updated, verified and activated timestamps.

Credentials remain solely in the encrypted storage configuration record.

The mutation journal contains migration ID, sequence, operation (`put` or
`delete`), object key, resulting etag/digest when available, state and
timestamp. It never stores object bodies.

## 7. Migration algorithm

1. Validate and persist the desired S3 configuration by expected revision.
2. Construct the candidate store and verify bucket access without changing the
   active backend.
3. Atomically create or resume the migration record and acquire its bounded
   worker lease.
4. Keep all reads and writes on local storage. The switching store records
   successful local puts/deletes in the durable mutation journal while a
   migration is active.
5. List local objects in bounded lexical pages. Stream each body directly from
   local storage to the candidate store and record progress after each bounded
   batch.
6. Replay journal entries through a captured high-water sequence. A put copies
   the current local version; a delete removes the target key.
7. Capture a verification-base high-water sequence, then compare the complete
   local and target key sets and verify size plus content digest for every
   object without blocking online mutations. Listing count alone is not
   sufficient. Mutations after the captured sequence remain in the journal.
8. Enter a short activation barrier that prevents new object mutations. Replay
   and verify only journal entries after the verification-base sequence. The
   barrier must not perform a bucket-wide list or re-hash unchanged objects.
9. In one database transaction, mark the desired revision active and the
   migration `s3_active`. Then atomically switch new object operations to the
   candidate store and release the barrier.
10. Preserve local files as a rollback copy. Their later retention/garbage
   collection is a separate policy and never occurs in the activation
   transaction.

The journal reduces catch-up work but is not treated as a cross-filesystem/SQL
transaction. If the process can crash between a local mutation and journal
append, restart performs a full source/target reconciliation before activation.
This makes the filesystem comparison the final correctness proof.

## 8. Failure and restart behavior

- Connectivity, copy, verification or persistence failure leaves local active.
- A bounded redacted error is persisted as `apply_failed`; secrets, request
  headers and complete endpoints never appear in logs or evidence.
- Restart reclaims an expired migration lease, reconstructs the candidate from
  the database configuration and resumes from durable progress.
- Restart always performs final full reconciliation even when all journal rows
  were previously applied.
- A changed desired revision cancels the obsolete migration and starts a new
  generation; stale workers fail their generation/lease checks.
- Switching back from S3 to local is not a blind pointer swap. It requires a
  separately verified reverse migration and is outside this first vertical.

## 9. Concurrency and consistency

The active store has one routing snapshot per operation. In-flight operations
finish against the snapshot they acquired. The activation barrier covers only
the final post-verification mutation delta and atomic routing change, not the
bulk copy, complete listing or full digest verification. Its work is
proportional to mutations after the captured verification high-water, not to
the total object count.

The migration worker is single-owner through a database lease. Progress and
journal application use expected generation/revision checks. Duplicate copies
are safe: an object is accepted only when the target body matches the source
digest. Conflicting target content is replaced from the authoritative local
source before activation.

Direct writes into `<data-dir>/resources` outside `ObjectStore` are unsupported.
The final reconciliation detects such files, but callers must use the port for
normal mutation ordering and observability.

The first migration vertical requires exactly one resource-writing server
instance during the migration window. A shared local volume alone is not
sufficient: mutation capture and the final activation barrier are process
local, so another server could otherwise write during that barrier. A
multi-replica deployment must either provision S3 before serving resource
traffic or drain resource writes to one instance until activation completes.
The database lease fences duplicate migration workers; it is not a distributed
write barrier.

## 10. Performance constraints

- Copy bodies through `Open` and `PutStream`; never buffer a large migration
  object in process memory.
- Byte-slice `Put` remains capped at 16 MiB. Streaming local/S3 operations use
  a separate explicit 5 GiB hard cap so large artifacts do not enter process
  memory and callers cannot request an unbounded stream.
- Page size, batch size and worker concurrency are bounded constants with
  conservative defaults; they are not required environment settings.
- One migration uses one bounded worker loop, not one goroutine per object.
- No bucket-wide list occurs on normal artifact reads or writes.
- No bucket-wide list or unchanged-object digest pass occurs inside the final
  activation barrier.
- No idle migration goroutine or polling loop exists when no migration is
  pending.
- Progress persistence is batched but the activation proof is durable.
- Object keys, counters and error codes are bounded before persistence or
  telemetry emission.

## 11. Observability

The administrator view exposes desired backend/revision, active
backend/revision, migration state, copied and verified counters, last bounded
error code and timestamps. It never returns the secret. The access key and
secret preview use the existing first-three/last-four convention.

Metrics use bounded labels: source backend, target backend, state and result.
Migration ID, object key, bucket and endpoint are not metric labels. Traces may
carry hashed migration identity but not credentials or raw object bodies.

## 12. Verification gate

Acceptance requires:

1. no-config startup selects `<HARNESS_DATA_DIR>/resources`;
2. database config wins and legacy environment import runs only for an absent
   row;
3. saving S3 config does not switch before copy and verification;
4. historical objects are copied with streaming and matching digests;
5. concurrent put, overwrite and delete are reflected before activation;
6. crash/restart resumes and performs complete reconciliation;
7. stale migration worker/generation cannot update progress or activate;
8. target connectivity/copy/verification failures leave local active;
9. secrets and full endpoints are absent from API evidence, logs and telemetry;
10. migration-window documentation rejects multi-writer operation even when
    replicas share a local volume;
11. SQLite fresh/historical/repeat/future-version migration tests;
12. PostgreSQL disposable-schema migration and concurrency tests;
13. repeated and race tests for the routing/barrier implementation;
14. bounded allocation tests for increasing object counts and maximum object
    size; and
15. full `go test ./...`, `go vet ./...`, gofmt, module boundary and OpenAPI
    verification.

## 13. Rollout and rollback

Implementation is enabled in slices: contracts and schema, copier, mutation
capture, verification/activation, server lifecycle, then console/API evidence.
Until the full gate passes, the current local backend remains the startup
default and automatic remote activation is disabled.

Rollback stops claiming new migrations and leaves local active. If activation
has already completed, rollback does not pretend the stale local copy is
authoritative; a verified reverse migration is required. No rollback path
deletes object data.
