# Private Runner worker

Set `HARNESS_RUNNER_URL`, `HARNESS_RUNNER_TOKEN`, and the comma-separated
`HARNESS_RUNNER_CAPABILITIES` allowlist, then run `go run ./examples/runner-worker`.
The token is the dedicated runner credential, not an administrator token. The
example claims, checks cancellation, and completes tasks without logging task
arguments, results, idempotency keys, or trace carriers. Production executors
must renew the lease while work is in progress before calling complete.
