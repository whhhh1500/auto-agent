# Contributing to auto-agent

auto-agent is an Apache-2.0 open-source Agent foundation. Keep public
interfaces small and integration-friendly; product-specific behavior belongs
in adapters or examples rather than the kernel.

## Before opening a change

- Use the patched Go 1.25 toolchain declared by `go.mod` (currently 1.25.13).
- Keep changes focused and include regression tests for behavior changes.
- Do not include credentials, personal data, generated build output, or
  `.credentials` material.
- Preserve the documented security defaults and fail-closed behavior.

Run the local checks from the repository root:

```text
go test -count=1 -timeout 600s ./...
go build ./...
go vet ./...
bash scripts/verify-gofmt.sh       # Unix-like systems
./scripts/verify-gofmt.ps1         # PowerShell
```

For changes involving durable SQL behavior, run the SQLite tests and, when a
PostgreSQL 16 instance is available, set `HARNESS_TEST_PG_DSN` and run the
PostgreSQL integration gate with `go run ./scripts/test-postgres`. It selects
`TestPostgres` across all packages, including SQL adapters and integration
examples, and rejects missing configuration, skips and zero-test results.
Name new DSN-dependent integration tests with the `TestPostgres` prefix so the
same command runs them locally and in CI. Do not weaken assertions or add skips
to make a check green. Use a disposable-schema-capable test database; the DSN
must remain outside source control.

## Review expectations

Explain compatibility impact, persistence/migration behavior, security impact,
and the commands used to verify the change. New public API needs a concise
contract and tests. Keep commits/reviews independently understandable.
