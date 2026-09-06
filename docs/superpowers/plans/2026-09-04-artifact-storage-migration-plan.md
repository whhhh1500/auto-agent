# Artifact Storage Migration Implementation Plan

Status: implemented and verified  
Design: `docs/superpowers/specs/2026-09-04-artifact-storage-migration-design.md`

## Acceptance boundary

The resource store remains local until every existing object and every
mutation through the activation barrier is present and verified in the
database-configured S3/R2 target. Failure or restart must not make local
artifacts disappear. Bulk copying must remain bounded-memory.

## Work slices

### A. Durable migration contracts and SQL adapter

- Add closed migration state, record, mutation and repository/lease ports to
  `pkg/app/artifactmigration` without adding S3 client dependencies. The
  storage configuration package remains independent from migration execution.
- Add SQLite/PostgreSQL schema v36 tables and indexes for resource migrations
  and mutation journal rows.
- Implement the SQL adapter under `pkg/adapter/sql/artifactmigration`.
- Verify fresh, v35-to-v36, repeated, future refusal, CAS/generation and
  two-handle lease behavior for SQLite and PostgreSQL.

### B. Bounded copier and activation-safe routing

- Add a storage migration service that lists in bounded pages and copies only
  through streaming seams.
- Extend/replace direct `DynamicObjectStore.Swap` use with migration-aware
  routing, mutation capture and a final activation barrier.
- Reconcile complete key sets and verify size plus digest outside the write
  barrier; replay and verify only the post-high-water delta inside it.
- Verify put/overwrite/delete catch-up, crash resume, stale generation,
  verification failure, barrier work independent of total object count and
  bounded allocations.

### C. Server composition and configuration behavior

- Change resource storage `Apply` from immediate swap to create/resume
  migration and report migration evidence.
- Compose the SQL repository and bounded migration worker; do not create an
  idle poller when no migration is pending.
- Keep local resources at `<HARNESS_DATA_DIR>/resources`.
- Remove S3/R2 settings from the primary `.env.example`; preserve lazy legacy
  import compatibility in code and document it outside the primary template.
- Expose redacted migration progress through the existing administrator
  storage configuration surface.

### D. Integrated verification and documentation

- Run focused tests repeatedly and with the race detector.
- Run PostgreSQL migration/concurrency tests against the available local test
  DSN without printing credentials.
- Run `go test ./...`, `go vet ./...`, gofmt verification, module boundary,
  module smoke and OpenAPI verification.
- Update public API/inventory documents only for intentionally exported
  contracts; keep implementation details internal.

## Ownership

- Root session: architecture, interface conflict resolution, integration,
  review and final verification.
- Agent A: slice A.
- Agent B: slice B.
- Agent C: slice C plus focused server/config tests.

Agents must not edit another slice's owned files without first reporting the
required interface change to root. No agent may touch another repository.

## Verification record

Completed on 2026-09-04:

- full Go tests, build, vet and repository gofmt verification;
- module-boundary and external module smoke tests without relaxing the
  application dependency baseline;
- OpenAPI verification for 95 registered `/v1` operations;
- focused race tests with the installed MinGW GCC toolchain;
- real SQLite plus local RustFS migration of normal and 17 MiB streamed
  objects, including active-route restoration after process restart;
- administrator API checks for redacted key previews, idempotent resave and
  fail-closed rejection of active S3 reconfiguration; and
- a 1,000-request, concurrency-32 small-object smoke run with no non-2xx
  responses. The server ended that run at roughly 29 MiB working set and
  private memory on the verification machine.

The first vertical still intentionally requires one resource-writing server
instance during migration, a dedicated empty target bucket or namespace, and
a future verified rebind/reverse migration for changing an active S3 target or
returning to local storage.
