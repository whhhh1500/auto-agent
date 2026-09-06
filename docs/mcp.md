# MCP integration

Harness Core acts as an MCP client over stdio JSON-RPC. It does not expose an
MCP server endpoint: public integrations use the versioned HTTP/SSE API and
private workers use the Runner protocol.

## Runtime contract

`execution.RegisterMCPServer` performs the MCP initialize handshake and mounts
one `harness.tool.library/v1` knowledge capability. A remote `tools/list`
response is indexed as a tool-library catalog; remote tools are not copied into
the model's top-level tool list. The model searches or lists qualified IDs,
describes one ID when it needs the full JSON Schema, and then calls that ID.

Qualified IDs have the form `library/name`. If a bare name is present in more
than one library, the call returns an `explore` candidate list and does not
guess. Empty or duplicate remote names are ignored. A failed or empty refresh
keeps the last good catalog, and each run keeps the immutable snapshot it
started with.

## Process and payload limits

The stdio command and environment are bounded before a process starts (at most
64 arguments/environment entries and 8 KiB of environment data). Assembled
call text is capped at 1 MiB. Tool arguments and results still pass through the
normal core validation, permission, approval, budget, timeout, and optional
ToolJournal paths; MCP does not provide a bypass.

Do not put credentials in MCP arguments or command lines. Use a capability
credential reference and let the configured adapter resolve it. Catalog and
observer records contain bounded IDs, descriptions, and outcome metadata, not
secrets, arguments, or results.

## Search and observation

Keyword search is the dependency-free default. `Catalog.SetSearcher` can add a
vector or hybrid ranker without changing the search/list/describe/call
contract. Optional `ToolLibraryObserver` records bounded search, explore, and
choose events for ranking diagnostics; it is disabled unless injected. The
server's read-only observation endpoint is
`GET /v1/admin/tool-library/observations`.

The behavior above is covered by `pkg/execution/mcp_test.go` and
`pkg/extensions/toollib/catalog_test.go`. When
an MCP server is unavailable, integration tests use a local stdio fixture; no
external MCP service is required for the normal test suite.
