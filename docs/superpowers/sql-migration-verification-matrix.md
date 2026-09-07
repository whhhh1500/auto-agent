# SQL migration verification matrix

This matrix is the Phase 0 evidence map for the durable SQL schema. The
current schema is `storage.SQLSchemaVersion == 43`. A migration is forward
only: an opener rejects a stored version newer than 43 before it executes
current DDL.

The schema source is cumulative Go DDL plus version-gated transformations,
rather than a directory of immutable SQL files. Therefore this matrix names
the *historical fixture boundary* that exercises a migration, not an implied
independent database dump for every integer version.

| Historical range or boundary | SQLite fixture and assertion | PostgreSQL fixture and assertion |
| --- | --- | --- |
| fresh (no `store_meta`) | `TestSQLMemoryRAGSchemaV26Fresh`, `TestSQLMemorySearchProjectionSchemaV28SQLite` open fresh stores and repeat open where applicable | PG equivalents use `newPostgresTestDB`; execution requires `HARNESS_TEST_PG_DSN` |
| v1 (covers the v2-v8 cumulative-table path) | `TestSchemaV1DatabaseUpgradesInPlace`; `TestSchemaV1DatabasePreservesSessionRowsAcrossRepeatOpen` starts with genuine v1 DDL, retains a session/event chunk, opens twice, and checks the current v43 marker | No immutable v1 PG fixture; `TestPostgresSchemaMigrationAndFutureRefusal/v9_to_current` is the oldest PG DDL fixture |
| v9 (covers v10-v18 structural additions) | `TestSQLSchemaV9UpgradesQueueGenerationAndApprovals` checks generation, trace, approval, submission, evaluation, release, and control-revision artifacts | `TestPostgresSchemaMigrationAndFutureRefusal/v9_to_current` checks the corresponding columns, tables, index, and revision row |
| v19 -> v20 | `TestSQLSchemaV19BackfillsEvaluationRevisionProjections` retains real evaluation rows and checks queryable revision projections | The v9 PG fixture checks the resulting columns; no PG v19 row-backfill fixture yet |
| v22 -> v23 | `TestSQLSchemaV22UpgradesRunEvidenceAssignmentVariant` keeps run-evidence rows and verifies the backfilled variant/index | `TestPostgresSchemaMigrationAndFutureRefusal/v22_to_v23_run_evidence_variant_backfill` provides the equivalent PG row check |
| v24 -> v25 | `TestSQLRunnerSchemaV24MigrationAndFutureRefusal` verifies runner trace columns and version advance | `TestPostgresRunnerSchemaV24ToV25Migration` verifies the PG runner table and trace columns |
| v25 -> v26 | `TestSQLMemoryRAGSchemaV25ToV26Migration` preserves canonical rows, deterministically deduplicates Memory keys, repeats open, and has rollback/retry coverage | `TestPostgresMemoryRAGSchemaV26Migration/v25_to_v26` provides fresh, preserved-row, repeat-open, and future-version cases |
| v26 -> v27 | `TestSQLRagProjectionSchemaV27SQLite` checks fresh, migration, preservation, rollback/retry, and future refusal | `TestPostgresRagProjectionSchemaV27` mirrors these cases |
| v27 -> v28 | `TestSQLMemorySearchProjectionSchemaV28SQLite` checks fresh, migration, preservation, retry, and future refusal | `TestPostgresMemorySearchProjectionSchemaV28` mirrors those cases and asserts the canonical lock before projection replacement |
| v29 -> v30 | Runner retry behavior is covered after current open; there is not yet an isolated v29 historical database fixture | No isolated PG v29 fixture |
| v30 -> v31 | `TestSQLDelegationLinksSQLiteMigratesFromV30` verifies the delegation table is restored | PG delegation operation tests run on fresh schemas; no isolated PG v30 fixture |
| v31 -> v32 | `TestSQLSchemaV31MigratesAccountIdentity` and `TestSQLSchemaV31RepairsMissingAuthTokensDuringAccountMigration` preserve account/token ownership and repair a missing token table | `TestPostgresV31AccountIdentityMigrationPreservesTokens` preserves account/token ownership |
| v32 -> v33 | The cumulative opener adds runtime effect sequence/effect journal tables; `pkg/adapter/sql/effectjournal` state/idempotency tests verify the durable contract and existing business rows remain migration-safe | PG v33 EffectJournal migration/adapter coverage requires `HARNESS_TEST_PG_DSN`; absent DSN is an explicit skip |
| v33 -> v34 | The cumulative opener adds the runtime host-state CAS document with independent revision; `pkg/adapter/sql/compositionstore` CAS/round-trip tests verify the adapter contract | PG v34 CompositionStore migration/adapter coverage requires `HARNESS_TEST_PG_DSN`; absent DSN is an explicit skip |
| v34 -> v35 | `TestSQLiteRuntimeHostStateSchemaV35FreshHistoricalRepeatAndFutureRefusal` verifies ownership and Fence tables on fresh/historical/repeat open; future-version refusal explicitly proves both v35 tables remain absent before DDL | `TestPostgresRuntimeHostStateSchemaV35FreshHistoricalRepeatAndFutureRefusal` passed with an isolated disposable schema; `TestPostgresFenceJournalFreshSchemaAndMonotonicDecisions`, `TestPostgresFenceJournalFreshHandlesHaveOneExecutor`, and `TestPostgresCompositionStoreOwnershipGenerationFence` verify the PostgreSQL v35 ownership/Fence adapter behavior |
| v35 -> v36 | `TestSQLiteArtifactMigrationSchemaV36FreshHistoricalRepeatAndFutureRefusal` verifies artifact migration/journal tables on fresh, historical, repeat, and future-refusal paths | `TestPostgresArtifactMigrationSchemaV36FreshHistoricalRepeatAndFutureRefusal` passed against an isolated PostgreSQL 17.6 cluster on 2026-09-04 |
| v36 -> v37 | `TestSQLiteGraphCheckpointSchemaV37FreshHistoricalRepeatAndFutureRefusal` verifies Graph checkpoint/transition tables on fresh, historical, repeat, and future-refusal paths | `TestPostgresGraphCheckpointSchemaV37FreshHistoricalRepeatAndFutureRefusal` plus adapter CAS/replay tests passed against an isolated PostgreSQL 17.6 cluster on 2026-09-04; no skip |
| v37 -> v38 | `notification_targets` and `notification_targets_tenant_enabled` are created by the cumulative opener; target adapter tests cover encrypted configuration and metadata-only list/read behavior | `TestPostgresStoreTenantCASAndOpaqueConfig` passed on PostgreSQL 17.6 on 2026-09-06; fresh-schema adapter evidence, not an isolated historical v37 row fixture |
| v38 -> v39 | `graph_segment_leases` is created by the cumulative opener; adapter tests cover acquire conflict, expiry takeover, generation fencing, stale lease rejection, release idempotency, and context cancellation | `TestPostgresGraphSegmentSemantic` passed on PostgreSQL 17.6 on 2026-09-06; fresh-schema adapter evidence, not an isolated historical v38 row fixture |
| v39 -> v40 | `TestSQLiteSchemaV39ToV40IndexesAndReopen` verifies both current indexes after historical upgrade and repeat open | `TestPostgresSchemaV40Indexes` passed on PostgreSQL 17.6 on 2026-09-06 |
| v40 -> v41 | `TestSQLiteGraphCheckpointHistorySchemaV41FreshAndV40Upgrade` verifies fresh `graph_checkpoint_versions`, strict v40 current-head backfill as one `migration_floor`, no fabricated earlier revision, corrupt-document rollback, and marker stability | `TestPostgresGraphCheckpointHistorySchemaV41FreshAndV40Upgrade` passed against a live local PostgreSQL 17 instance; the same final pass also covered PostgreSQL atomic rollback, replay, and competing CAS |
| v41 -> v42 -> v43 | `TestSQLSchemaV41MigratesAuthorizationEpoch` verifies a pre-v42 marker gains the additive `store_meta.authorization_epoch=0` row and advances to v43; `TestSQLSessionStoreCompletedToolResultRecoverySidecarMigrationAndPrune` verifies the direct v42 -> v43 empty-sidecar upgrade | `TestPostgresSchemaV41MigratesAuthorizationEpoch` exercises the epoch path and `TestPostgresSQLSessionStoreCompletedToolResultRecoverySidecarFenced` verifies direct v42 -> v43 without backfill against a disposable schema |
| v35 historical repeat open | The v35 historical fixture repeats open and verifies the schema marker and ownership/Fence tables remain stable after cumulative opening | PG v35 repeat-open evidence passed with an isolated disposable schema and administrator-capable `HARNESS_TEST_PG_DSN`; this does not establish v36/v37 interoperability |
| future version | `TestSQLFutureSchemaIsRefusedBeforeCurrentDDL` and feature-specific refusal tests | `TestPostgresSchemaMigrationAndFutureRefusal/future_version_no_ddl` and feature-specific refusal tests |

## Interpretation and remaining gaps

- The SQLite suite gives concrete historical row-preservation evidence for v1,
  v19, v22, v25, v26, v27, v31, v33, v34, v35, v36, v37, v40, v41, v42, and v43. It exercises cumulative transition paths
  from v1 and v9, but does **not** claim one immutable fixture per every
  integer v2 through v30.
- PostgreSQL uses one disposable schema per test through
  `newPostgresTestDB`. Its entire suite intentionally skips when
  `HARNESS_TEST_PG_DSN` is absent; a skip is not an interoperability pass.
- The next targeted additions, if a release requires stronger historical
  guarantees, are real PG row fixtures for v19 -> v20 and v30 -> v31, plus a
  v29 -> v30 runner-row fixture for both dialects. PostgreSQL v35 ownership and
  Fence-specific verification is covered by the disposable-schema tests above;
  PostgreSQL v36/v37 now have isolated-cluster evidence recorded below. PostgreSQL
  v38-v41 DSN-specific tests passed in the 2026-09-06 all-package gate below;
  the v42/v43 PostgreSQL migration tests remain opt-in until a new DSN-backed run is recorded.
  The v38/v39 adapter checks do not add isolated historical row-migration
  fixtures beyond the specific boundaries listed in the table.
  These are deliberately listed as gaps instead of being represented by a
  rewound version marker on a fully current schema.

## Commands

Run the SQLite evidence without external services:

```powershell
go test ./pkg/storage
```

Run PostgreSQL evidence only against a disposable-schema-capable test DSN;
do not record that DSN in source control or logs:

```powershell
$env:HARNESS_TEST_PG_DSN = '<provided-out-of-band>'
go run ./scripts/test-postgres -log postgres-test.jsonl
Remove-Item Env:HARNESS_TEST_PG_DSN
```

The runner selects all `TestPostgres` tests in `./...`, including SQL adapters,
examples and service integration. It fails on a missing DSN, any selected skip,
zero executed tests, invalid output or test failure. CI uses the same runner.

## All-package PostgreSQL execution record (2026-09-06)

An owned, disposable PostgreSQL 17.6 cluster on loopback port 55436 ran the gate:
57 top-level tests, 21 subtests, 10 packages, zero skips and zero failures.
This includes storage and all seven SQL adapter packages previously omitted by
the storage-only CI command, plus the Graph example and HTTP service tests.
The v41 history-upgrade test was executed in this gate. This is local Windows
PostgreSQL 17.6 evidence; CI remains configured for PostgreSQL 16 and was not
triggered remotely. See the [integration acceptance report](../verification/2026-09-06-assessment-closure.md)
for commands and evidence limits.

## Phase 0 execution record (2026-09-02)

- `go test ./pkg/storage -count=1` passed, including the SQLite fixtures in
  this matrix.
- `go test ./...` and `go vet ./...` passed after the SQLite fixture was
  added.
- A local PostgreSQL process was already listening on port 5432, but
  `HARNESS_TEST_PG_DSN` was not configured. The local startup script was
  inspected without exposing its contents or credentials. A passwordless,
  locally derived disposable-schema probe did not establish an
  administrator-capable test connection, so no server restart or credential
  guessing was performed.
- `go test ./pkg/storage -run 'Postgres' -count=1 -json` recorded 40 skipped
  PostgreSQL test/subtest nodes and no failures. Four parent test containers
  reported pass solely because their selected subtests skipped. This is a
  verified skip state, not PostgreSQL migration validation.

## PostgreSQL v35 execution record (2026-09-04)

- With an out-of-band, passwordless local PostgreSQL test DSN, the following
  passed against unique temporary schemas (the DSN is intentionally not
  recorded):
  `go test -count=1 ./pkg/adapter/sql/fencejournal ./pkg/adapter/sql/compositionstore`
  and the v35 storage migration/future-refusal tests in `./pkg/storage`.
- The FenceJournal checks cover fresh PostgreSQL construction with
  `sqlkit.Postgres`, execute admission, in-progress `ErrFenceUnknown`, replay,
  no `MarkUnknown` downgrade, request-id conflict, and two independent handles
  admitting exactly one executor.
- The ownership checks cover one concurrent claimant, strictly increasing
  generations after release, same-generation renewal, and stale renew/validate/
  release attempts that cannot affect the new holder.

## PostgreSQL v36-v37 execution record (2026-09-04)

- A disposable PostgreSQL 17.6 cluster was initialized under the system
  temporary directory, bound only to `127.0.0.1:55432`, and kept separate from
  the existing PostgreSQL listener on port 5432. The cluster was stopped and
  its temporary data directory was removed after verification.
- `go test ./pkg/storage -run '^TestPostgres.*GraphCheckpoint|^TestPostgresGraphCheckpoint' -count=1 -v` passed with no skipped tests.
- `go test ./pkg/adapter/sql/graphcheckpoint -run '^TestPostgres' -count=10 -v` passed all ten repetitions, including concurrent divergent Create/CAS and zero-side-effect assertions.
- `go test ./pkg/storage -run '^TestPostgres' -count=1 -v` passed the complete PostgreSQL storage set, including artifact migration v36 and Graph checkpoint v37 fresh, historical, repeat-open, and future-refusal coverage.
- The Graph v37 migration test was corrected to open the fresh schema before asserting its tables, matching the initialization pattern used by the other migration fixtures.
