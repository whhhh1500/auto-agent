# Assessment closure design

Status: implementation authorized by the request to resolve assessment items 1-3.

## Scope and choices

1. Replace the PostgreSQL CI package allowlist with one cross-platform runner
   selecting every `TestPostgres` test in `./...`. Missing DSN, skipped selected
   tests, failures, invalid output, or zero executed tests must fail the gate.
   Reuse this runner in the local PostgreSQL smoke script.
2. Keep Windows local execution at current-user Basic. Update architecture and
   support documentation to match the existing restricted-token/Job provider:
   Medium source, one active session per process, Host networking without
   network isolation. No account provisioning, WFP, elevation or strong-mode
   work is included.
3. Keep the default `graph-core-turn` registration narrow. Supply a separate
   example using the existing Graph engine for a draft/review/finalize flow.
   Verify SQL checkpoint/history and approval resume after reconstructing the
   execution instance. Completed nodes must not repeat, approval must retain
   its attempt identity, and unknown executing outcomes must remain closed.
4. Add PostgreSQL service integration evidence using real HTTP, durable queue,
   two independent server/store assemblies and approval resume on a replacement
   instance. Add a bounded concurrency workload recording successes, failures,
   elapsed time and latency; label deterministic model behavior explicitly.

Keeping the current defaults and adding a separate example minimizes API
changes. Widening the generic Graph adapter would require new product-facing
configuration and security contracts; updating prose alone would leave the
integration evidence gap unresolved. Neither is the selected approach.

## Validation

Use an owned disposable PostgreSQL cluster on an unused loopback port. Keep all
build/cache/temp artifacts on D on this Windows host. Exercise the same runner
used by CI, then the affected SQLite/Graph/service tests and the repository's
build, vet, formatting, OpenAPI and full-test gates. Windows Basic retains its
existing tagged Medium acceptance; documentation changes do not expand it.

External-model checks use only an explicitly selected test configuration. The
repeatable local model service proves protocol and persistence integration, not
provider quality or production capacity. Record actual database version,
workload and measurement window; do not turn a short local pass into an SLA.

## Completion criteria

All previously omitted PostgreSQL adapter tests execute without skips. Current
architecture documentation has no contradictory Windows support statements.
The multi-node example and service recovery/concurrency checks are runnable
and pass against actual SQL adapters. Evidence distinguishes freshly executed
checks from historical reports and external checks not performed. Commit the
reviewed change locally after validation.
