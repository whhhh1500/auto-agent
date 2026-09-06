# Round 1 - Database Bootstrap

## Scope

`.env` loading, database selection, v32 account migration, missing-table repair,
initial administrator generation, concurrency, and restart semantics.

## Findings

1. A legacy `accounts` table plus a missing `auth_tokens` table made v32 read a
   nonexistent legacy token column.
2. UTF-8 BOM in a Windows-created `.env` invalidated its first key.
3. PostgreSQL mode silently tolerated a stale `HARNESS_SQLITE_PATH`.

## Fixes

1. v32 now detects token identity columns and rebuilds old, new, or newly
   repaired empty token tables correctly.
2. The loader strips one UTF-8 BOM from the first line without weakening key
   validation.
3. SQLite/PostgreSQL path and DSN combinations now fail on contradictions.
4. Added legacy migration, missing-token repair, absent/existing-table
   bootstrap, activation, and concurrent bootstrap regression tests.

## Verification Evidence

- `go test ./cmd/server ./pkg/storage -count=1` passed.
- `go vet ./cmd/server ./pkg/storage` passed.
- Concurrent bootstrap test created exactly one account.
- Existing empty `accounts` test created none.

## Remaining Risks

Live PostgreSQL startup and migration remain for round 3 environment-dependent
verification.

## Conclusion

Database/bootstrap closure passed after the listed repairs.
