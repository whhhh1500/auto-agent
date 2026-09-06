# Graph checkpoint history and execution fencing implementation plan

Design: `docs/superpowers/specs/2026-09-04-graph-checkpoint-history-design.md`

Status: implementation complete in the current checkout. Contract, adapter,
migration, and executor tests pass. The v41 PostgreSQL migration, atomicity,
replay, and competing-CAS tests were also executed against a live local
PostgreSQL 17 instance and passed.

## Phase 1 -- History contract and reference adapter

- Completed: bounded checkpoint-version metadata with explicit `commit`,
  `migration_floor`, and `fork` origins; deterministic IDs, canonical digests,
  clone/validation helpers, and an optional `HistoryStore` without changing
  `Store`.
- Completed: the bounded memory adapter appends immutable versions in the same
  critical section as head and transition, rechecks cancellation under the
  write lock, validates metadata lists, and preserves replay/capacity atomicity.

## Phase 2 -- SQL schema v41 and atomic persistence

- Completed: `graph_checkpoint_versions` has run/revision identity, unique
  version ID, parent ID, explicit origin, checkpoint hash, bounded JSON, and
  creation time.
- Completed: v40-to-v41 validates and backfills exactly the current checkpoint
  as a `migration_floor`; it fabricates no earlier history.
- Completed: SQL Create/CAS atomically writes mutable head, immutable version,
  and transition; SQLite/PostgreSQL adapters provide validated `LoadVersion`
  and metadata-only `ListVersions`.

## Phase 3 -- Executor fencing

- Completed: the executor re-verifies the lease after node execution and
  immediately before every active-segment checkpoint mutation, including
  approval/resume and recovery paths. Lease loss returns the lease error and
  preserves the last authorized durable fact; deterministic after-node and
  before-commit loss tests cover this fence.

## Phase 4 -- Integration and audit

- Run gofmt on changed Go files.
- Run focused contract, memory, SQL, migration, and executor tests.
- Run `go test ./...`, `go test -race ./...`, `go vet ./...`, staticcheck, and
  the repository's format/module/public-API verifiers.
- Exercise PostgreSQL with an actual test DSN; an environment skip is reported
  as unverified, never as passing.
- Review the public API surface and update architecture/inventory documents
  only after code and tests establish the final names.

## Deferred, explicit follow-up

- Authorized Fork use case and lineage across run IDs.
- Protected retention/pruning policy.
- Optional transaction-local SQL lease-fence plugin.
- Snapshot/delta or cold-object history plugin if measured storage pressure
  justifies the added recovery complexity.
