# Round 3 - Runtime Closure

## Scope

Process-level SQLite first start, one-time credential emission, restricted
login, activation, restart persistence, full-repository regression, and
operational verification usability.

## Findings

1. The repository had no reusable process-level test for the complete
   bootstrap, activation, and restart lifecycle.
2. The first smoke script draft used PowerShell 7-only behavior and a process
   wait pattern that races with fast process termination on Windows
   PowerShell 5.1.
3. Random account digits and password characters used cryptographic bytes but
   modulo selection introduced a small distribution bias.
4. Live PostgreSQL startup path had not yet been covered end-to-end in this
   closure, and there was no direct evidence of startup-lock behavior under
   concurrent opener calls.

## Fixes

1. Added `scripts/bootstrap-smoke.ps1`; it creates an isolated temporary
   database, parses credentials without displaying them, proves the pending
   authorization gate, activates the account, restarts the server, and checks
   that credentials are not printed again.
2. Made the smoke script compatible with Windows PowerShell 5.1 and 7 by using
   compatible HTTP error handling, random-byte generation, and process waits.
3. Replaced modulo character selection with rejection-based `crypto/rand.Int`
   sampling for uniform account suffixes and one-time passwords.
4. Documented the process-level verification command in the README.
5. Added startup-lock coverage for PostgreSQL startup sequence in
   storage/main (`postgres` startup + precheck + migration + bootstrap lock
   path); the smoke script executes the covered flow.
6. Added cleanup fixes for test teardown and the PostgreSQL smoke script.
7. Added PostgreSQL `v31` token migration coverage; a 4-opener concurrency
   test proved all callers succeed and exactly one pending administrator is
   created.

## Verification Evidence

- The live SQLite smoke test passed: database created, first credentials
  printed, pending overview returned `403`, activated overview returned `200`,
  restart printed no credentials, and active login survived restart.
- `go test ./pkg/storage ./pkg/server ./cmd/server -count=3` passed.
- The final full-repository test, vet, formatting, and OpenAPI checks are
  recorded in the completion handoff.
- PostgreSQL 17.6 live smoke on isolated port `55434` with
  `scripts/postgres-live-smoke.ps1` passed `^TestPostgres` end-to-end:
  first server start printed one-time credentials, `/v1/admin/overview`
  returned `403` in pending state, activation returned `200`, restart did not
  reprint credentials, and login with the rotated password succeeded.
- Startup-lock checks passed:
  advisory lock path covered precheck, migration, and bootstrap steps; 4 concurrent
  openers all succeeded and only 1 pending admin was created.
- Database hygiene checks in the smoke run:
  `harness_test_*` schema count was `0` (no leftover schemas).  
  The run targeted isolated port `55434`; `persistent_instance_touched = false`,
  and host `5432` PostgreSQL instance remained running but untouched.


## Remaining Risks

Only one external operational boundary remains: external tooling can still bypass
the application advisory lock and issue concurrent DDL/administrative operations
directly against PostgreSQL.

## Conclusion

The SQLite and PostgreSQL lifecycle start->activate->restart checks are closed with
repeatable, secret-safe smoke verification.  
The remaining risk is only the external operational boundary where outside tooling
can bypass application advisory lock and issue concurrent DDL/administrative
operations directly against PostgreSQL.
