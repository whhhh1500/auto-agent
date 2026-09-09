# Durable evidence contracts — verification (2026-09-09)

This record covers the new versioned CoverageContract revalidation seam, the
provider-neutral external-effect read-back contract, and the durable route
evidence receipt/outbox. It records targeted tests plus the final local build,
vet, ordinary-test, race-test, staticcheck, and OpenAPI gates. It does not
claim a remote CI runner, OTLP collector, real effect provider, or production
PostgreSQL result.

## Package and boundary inventory

`go list ./pkg/...` reported 83 packages: 81 importable public `pkg/...`
packages and two `pkg/.../internal/...` support packages. The added public
surfaces are `pkg/app/effectreceipt` and `pkg/adapter/effectreceipt`.

The module dependency baseline was checked with:

```powershell
go test ./internal/modulecheck -run 'Test(FoundationalDependencyBaseline|PublicInventoryMatchesPackageList)' -count=1
```

It passed. The constrained edges are `pkg/evaluation` to
`pkg/app/effectreceipt`, and `pkg/adapter/effectreceipt` to only `pkg/core`
and `pkg/app/effectreceipt`; this is deliberately not an allow-list for an
entire application or adapter family.

## Route evidence durable outbox

The following tests passed against file SQLite, including separate OS child
processes rather than only injected error paths:

```powershell
go test ./pkg/server -run '^TestRouteEvidence(ReconcilesAfterProcessExitBeforeReceipt|RedeliversSameReceiptAfterProcessExitBeforeAck|DurableOutboxCrashHelper)$' -count=1 -timeout 45s
go test -race ./pkg/server -run '^TestRouteEvidence(ReconcilesAfterProcessExitBeforeReceipt|RedeliversSameReceiptAfterProcessExitBeforeAck|DurableOutboxCrashHelper)$' -count=1 -timeout 60s
```

They cover a terminal Session/run-control record surviving a crash before
receipt creation and subsequent reconciliation to one receipt, plus a sink
receiving a receipt before process exit and the expired lease causing a replay
of the same `receipt_id` before fenced acknowledgement. This proves
at-least-once delivery and receiver-side receipt-ID deduplication is required;
it does not prove exactly-once sink execution. The receipt is content-free and
is derived from canonical Session/Journal facts. The run-control candidate
query is discovery only, and neither the receipt nor OTel replaces the source
Session and Tool Journal as audit authority.

## External-effect read-back recovery

The server recovery, authorization and evaluation-gate seams passed their
targeted coverage:

```powershell
go test ./pkg/server -run '^(TestEffectReceiptRecovery|TestNativeStrictAutomaticallyWiresEffectReceiptRecovery|TestNativeStrictEffectRecoveryAuthorizerUsesCanonicalSessionScope)$' -count=1
go test ./pkg/server -run 'TestRelease(Efficiency|StrictGate)' -count=1
```

`accepted` means only a provider acknowledgement was stored. A record becomes
`confirmed` or `rejected` only through a read-back bound to its exact
operation key and intent digest. Recovery schedules registered exact drivers,
rechecks current Session-derived authority, and has no Dispatch path.

NativeStrict queued dispatch also passed an end-to-end Bridge path. Immediately
before a provider crossing, the server checkpoints the complete Session prefix
and re-resolves the current principal and execution-projection epoch. One SQL
transaction then checks the epoch, queue claim generation, Session lease and
version, Tool Journal begin, native witness, and exact prepared v50 receipt
before changing that receipt to `dispatching`. Admission failure or panic
cancels the owning run and uses a fixed public error class; it never reaches
the provider. The public NativeStrict synchronous run route remains unavailable.

```powershell
go test ./pkg/server -run 'TestNativeStrict(QueuedEffectDispatch|PublicSynchronousRunRoute)|TestNativeQueuedEffectDispatchAdmissionPanic' -count=1
go test -race ./pkg/server -run 'TestNativeStrict(QueuedEffectDispatch|PublicSynchronousRunRoute)|TestNativeQueuedEffectDispatchAdmissionPanic' -count=1
```

These tests cover a first dispatch followed by authoritative read-back and a
v50 `confirmed` record; epoch, claim and Session-version changes before
admission with zero provider calls; a response-lost `dispatching` receipt that
the successor only reads back; and panic cancellation. Recovery also maintains
one independent bounded retry sweep per driver. Each tick processes at most
one forward page and one retry page of 64 records, so continuously arriving
new rows cannot permanently hide an older denied row after its authority is
restored.

SQLite storage coverage also passed:

```powershell
go test ./pkg/storage -run '^(TestSQLRouteEvidence|TestPostgresRouteEvidenceOutbox|TestSQLExternalEffectReceipt|TestPostgresExternalEffectReceiptStore|TestPostgresNativeQueuedEffectDispatchAdmission)' -count=1 -timeout 45s
```

The SQLite cases cover the v49/v50 migration, immutable identity conflict,
claim/lease-generation fencing and content-free external-effect columns. The
three PostgreSQL-named cases are DSN-gated and skipped in this run as described
below.

The process-crash cases use a file SQLite database and a file-backed fake
provider:

```powershell
go test ./pkg/integration -run '^TestEffectReceiptHardKillRecovery$' -count=1 -timeout 30s
go test -race ./pkg/integration -run '^TestEffectReceiptHardKillRecovery$' -count=1 -timeout 45s
```

They cover exit after `dispatching` persists before a provider call; exit after
the provider write succeeds but before local acceptance; and exit after
read-back confirmation reaches the external ledger but before local Session or
Tool Journal completion. Restart recovery reads back instead of dispatching
again. The latter case deliberately keeps external ledger proof independent
from the missing local completion proof.

## PostgreSQL boundary

No `HARNESS_TEST_PG_DSN` was configured for this targeted pass. PostgreSQL
route-outbox, external-effect receipt, and atomic dispatch-admission tests
therefore remain explicit skips
in their normal DSN-gated test paths. SQLite, race, and child-process results
above do not substitute for a PostgreSQL migration, locking, or crash-recovery
run. A later PostgreSQL-enabled verification must record its disposable schema
and exact command before widening this evidence claim.

## Final local quality gates

The following current-worktree commands passed on Windows amd64:

```powershell
go build ./...
go vet ./...
go test -count=1 -timeout 600s ./...
go test -race -count=1 -timeout 600s ./...
go run honnef.co/go/tools/cmd/staticcheck@v0.7.0 ./...
go run golang.org/x/vuln/cmd/govulncheck@v1.7.0 ./...
go run ./scripts/verify-openapi openapi/harness-core-v1.yaml pkg/server pkg/console/static/index.html
$packages = @(go list -f '{{if or .TestGoFiles .XTestGoFiles}}{{.ImportPath}}{{end}}' ./... | Where-Object { $_ -and $_.Trim() -ne '' })
go test -count=1 -timeout 600s -covermode atomic -coverprofile '.tmp\durable-evidence-coverage.out' @packages
go run ./scripts/verify-coverage.go '.tmp\durable-evidence-coverage.out'
```

The ordinary full test completed with all non-DSN paths green. The full race
gate also completed green; its longest package was `pkg/storage` at 567.200s.
Staticcheck v0.7.0 selected Go 1.26.8 through Go's toolchain mechanism on this
host, while the repository build, vet and tests used the configured Go
1.25.13 toolchain. The OpenAPI verifier matched all 102 registered `/v1`
operations. `govulncheck` found no vulnerabilities called by the code; it
reported three vulnerable required modules whose affected symbols are not
called. The repository coverage gate passed all configured floors: `pkg/core`
79.1%, `pkg/evaluation` 76.9%, `pkg/execution` 69.0%, `pkg/server` 68.7%,
`pkg/storage` 72.7%, `pkg/provider/openai` 76.6%, and
`pkg/extensions/workflow` 57.5%. Like staticcheck, the pinned govulncheck tool
selected Go 1.26.8 through Go's toolchain mechanism.

No live-model request was run for this change. The default prompt, disclosed
tool menu and execution-route policy did not change; deterministic model
fixtures exercise the Bridge through the real guarded tool loop. No local
Docker or `gitleaks` executable was available, so the CI secret-scan job remains
the authority for that gate.
