# MCP Streamable HTTP design

Status: implementation authorized for the bounded MCP client extension.

## Scope

Harness Core already mounts a stdio MCP server as one progressive-disclosure
tool-library capability. This change adds an independent Streamable HTTP client
transport. It preserves the existing stdio configuration and does not change
Core, schemas, the tool-library contract, or the service's public HTTP API.

The HTTP registration has its own configuration and entry point. A URL is not
accepted as a stdio command, and a command is not accepted as an HTTP endpoint.
The two transports share only a private JSON-RPC transport interface and the
existing tool catalog/gateway.

## Protocol and security contract

The implementation supports MCP Streamable HTTP revisions `2025-06-18` and
`2025-11-25`, defaulting to the latter. It follows the current `2025-11-25`
transport requirements:

* Each outbound JSON-RPC message uses a POST to the configured endpoint with
  `Accept: application/json, text/event-stream`. Requests accept either a
  single JSON result or an SSE stream.
* Initialization negotiates a protocol version. Subsequent messages carry the
  negotiated `MCP-Protocol-Version` and, when supplied by the initialization
  response, the validated `Mcp-Session-Id`.
* Notifications require HTTP 202 with no response body. A server-initiated
  request is not a supported capability and fails the current call explicitly;
  server notifications are ignored as they are on the existing stdio path.
* Request and response bodies, SSE events, and session headers are bounded.
  Malformed JSON, an unexpected response id, unsupported content type, a
  redirect, or an HTTP authorization failure fails closed.
* The default client validates public HTTP(S) endpoints at bind time and again
  during dial, using the existing SSRF-safe transport. A private endpoint
  requires both explicit private-network opt-in and an injected trusted client;
  a custom client is never accepted implicitly. The generic HTTP capability executor is not reused because its
  request/response format is not MCP JSON-RPC.
* This transport does not implement OAuth discovery, dynamic client
  registration, or token storage. A deployer that injects authorization via a
  trusted custom client owns audience-bound token handling. Literal tokens are
  neither stored in the configuration nor logged by this package.

## Lifecycle, cancellation, and recovery

Calls remain serialized per MCP server, matching stdio. A request context or
call timeout stops waiting and sends one best-effort
`notifications/cancelled` POST for the in-flight non-initialize request; it
never replays the original request. The connection then closes its local
session state. A later user action starts a new initialization session.

`tools/call`, `initialize`, and `tools/list` are never automatically retried:
after a transport failure the outcome can be unknown. HTTP 404 for a session,
401/403, malformed responses, and SSE disconnection similarly discard the
session and return an error. GET listening, Last-Event-ID resumption, and
concurrent SSE streams are optional MCP features and are deliberately not
implemented in this initial client. Unmount first cancels an in-flight local
HTTP request, then sends one bounded best-effort DELETE for a server-issued
session id when it can acquire the serialized connection; it does not claim a
cancel notification was delivered during that shutdown race.

## Local verification

Use a local HTTP protocol peer with explicit private-network test opt-in to
exercise JSON and SSE responses, initialization/session/version headers,
notifications, cancellation, malformed or oversized frames, session expiry,
and no replay. Preserve the existing independent stdio child integration test
for initialize/list/call/mount/unmount and add timeout/crash coverage only if
the extraction changes it. These are local transport-interoperability checks,
not an OAuth or hosted-MCP acceptance claim.

References: [MCP Streamable HTTP transport, revision 2025-11-25](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports) and [the compatible 2025-06-18 transport revision](https://modelcontextprotocol.io/specification/2025-06-18/basic/transports).
