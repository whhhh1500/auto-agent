# Database Bootstrap and Account Activation Implementation Plan

1. Add strict working-directory `.env` loading and database-type selection.
   Verify parser precedence/error redaction and the SQLite/PostgreSQL config
   matrix with command-package tests.
2. Add SQL schema v32 with independent `account_id`, optional `email`, pending
   activation state, token ownership migration, and pre-migration accounts-table
   inspection. Verify fresh and legacy SQLite migration plus PostgreSQL-aware
   migration tests.
3. Replace count-based bootstrap with an absent-table-only, durable-claim
   bootstrap that generates and prints one-time credentials after commit.
   Verify existing-empty-table, restart, collision, failure, and concurrency
   behavior.
4. Add pending-account login, backend activation gating, transactional password
   activation/token rotation, canonical account APIs, and legacy login-field
   compatibility. Verify direct API bypass is denied.
5. Update the embedded console, OpenAPI contract, `.env.example`, README,
   deployment docs, Compose, and affected tests so every layer uses the same
   account and database semantics.
6. Run review/fix round 1 for database/bootstrap closure and write
   `docs/gaps/round-1-database-bootstrap.md`.
7. Run review/fix round 2 for authentication/console/API closure and write
   `docs/gaps/round-2-auth-activation.md`.
8. Run review/fix round 3 for full regression and real fresh-SQLite
   start/login/activate/restart behavior and write
   `docs/gaps/round-3-full-closure.md`.
9. Perform a requirement-by-requirement completion audit, run the full test,
   vet, OpenAPI, and runtime gates, and report any PostgreSQL environment limit
   explicitly.
