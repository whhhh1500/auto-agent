# SQL draft/review/finalize example

This example assembles three nodes using the existing Graph engine:

```text
draft -> review (pause for approval) -> finalize
```

`draft` calls an injected Go function and stores its result. `review` requests
approval with the stable node attempt ID. After an authorized decision,
`finalize` records the accepted draft in SQL state. It performs no external
publication. The generic server's `graph-core-turn` registration remains limited
to its built-in single node.

The embedding application owns the SQL connection and migrations. Open the
schema with `storage.OpenSQLSessionStore` before calling `New`. Supply a
principal, a `DraftFunc`, and a separate `Reviews` authority that authenticates
decisions and binds them to the principal, checkpoint revision, run and attempt.
There is no allow-all approval default. Call `Workflow.Run` with a stable
`CheckpointKey`; after `ErrApprovalPending`, persist the decision in that
authority and call `Workflow.Resume` using the same key. `workflow_test.go`
contains the complete runnable assembly.

SQL checkpoints, transition history and segment leases survive reconstructing
the Workflow and opening an independent database pool. The owner marker rejects
another subject using an existing key. Unknown interrupted node outcomes remain
closed; the example does not blindly repeat an ambiguous draft callback.

```powershell
go test -count=1 -v ./examples/graph-review -run '^TestSQLite'
# Set HARNESS_TEST_PG_DSN to a disposable test database before this command:
go run ./scripts/test-postgres
```

The example's tests keep a small approval-service double outside the Workflow
instances. They prove SQL Graph reconstruction, not approval-service durability
or an OS process crash. The separate
[server integration tests](../../pkg/server/server_postgres_integration_test.go)
exercise actual HTTP, SQL approval decisions, run queue, session events and tool
journal across replacement server instances. Their opt-in real-model test is
documented in the [acceptance report](../../docs/verification/2026-09-06-assessment-closure.md).
