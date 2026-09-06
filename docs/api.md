# HTTP/SSE API

The canonical public contract is [openapi/harness-core-v1.yaml](../openapi/harness-core-v1.yaml),
validated against every `/v1` route by `scripts/verify-openapi`. The service is
HTTP/SSE; no internal Go handler or console-only endpoint is a public contract.

Authenticate with the deployment's injected authenticator or built-in bearer
token. Tenant and subject ownership come from the authenticated Principal, and
cross-tenant session access is intentionally indistinguishable from a missing
resource. Errors use the JSON `{ "error": "..." }` envelope.

Create a session with `POST /v1/sessions`, then submit a synchronous turn to
`POST /v1/sessions/{id}/runs` with `Accept: text/event-stream`. The stream emits
named `event:` and JSON `data:` frames. If a client disconnects, read durable
events with `GET /v1/sessions/{id}/events?after_seq=...`; the stream itself does
not claim Last-Event-ID replay. Use `/async` for a durable queued run and its
same-run status/recovery behavior.

The `/v1/admin` profile, capability, approval, evidence, and runner operations
are optional adapter surfaces protected by the same versioned authentication
boundary. See the OpenAPI document for request/response schemas and the
operation-specific authorization rules.
