# Runner protocol

Workers use a dedicated Bearer token: POST `/v1/runners/claim` with an explicit
capability allowlist; POST `/v1/runners/tasks/{id}/renew` with `generation` while
long work runs; inspect `cancel_requested`; then POST `complete` with the same
generation. Treat 409 as a lost lease and never repeat the side effect. Back off
on transport/5xx failures. SIGTERM stops new claims and lets the lease expire if
work cannot be safely completed. Payload, result, idempotency key and trace
carrier are protocol data, never log fields.
