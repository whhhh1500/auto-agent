# OpenTelemetry

`pkg/telemetry/otel` is optional. With OTel disabled, no exporter or background
export goroutine is created and execution semantics are unchanged. When
enabled, configure the standard OTLP/HTTP environment variables and optionally
`HARNESS_OTEL_SERVICE_NAME` and `HARNESS_OTEL_METRIC_INTERVAL`.

The adapter emits low-cardinality run status/count and duration, tool
result/denial, queue claim/recovery, and approval pending/resolved metrics.
Run and session correlation identifiers may remain trace-only. Task, request,
payload, header, credential and raw error content are omitted from both metrics
and spans. HTTP trace context uses W3C `traceparent`/`tracestate`; Baggage
is disabled by default and durable queue rows persist only those bounded
carriers.

Span error status descriptions and standard `exception.message` events use the
fixed value `error`; the original `err.Error()` text is never exported. The
`exception.type` field is one of `error.unknown`, `context.canceled`, or
`context.deadline_exceeded`, so cancellation and deadline behavior remain
distinguishable without exporting provider or request details. If an aggregate
error contains both cancellation and deadline causes, deadline takes precedence.

Telemetry attribute maps are an internal metadata boundary, not a general
redaction engine. Callers must pass only bounded nonsecret identifiers and
enumerated outcomes; prompt, result, header, credential and raw error content
must never be placed in an attribute. `OTEL_RESOURCE_ATTRIBUTES` is likewise
trusted operator configuration and must contain deployment metadata only.

Terminal `auto_probe_once` runs also emit a
`harness.programmatic.route.evidence` span. Its attributes contain only the
frozen route revision plus selection, probe-candidate coverage, local Journal,
Session usage-ledger and context-archive statuses and counts. The span never
contains prompts, answers, tool arguments/results, provider receipts or
credentials. `coverage=complete` describes the trusted probe plan's candidate
set; it is not a general claim that the final answer is semantically complete.
This span is a bounded, best-effort projection emitted after the run or queue
control reaches a durable terminal state. It may be missing or duplicated
across process failure and response-loss windows; the canonical Session and
Tool Journal remain authoritative audit evidence. A run that advertises a
frozen v2 marker but fails complete identity verification emits only a minimal
`identity.status=unavailable` receipt; ordinary non-v2 runs emit no route
receipt.

The route-evidence span is not an input to the release quality gate or the
optional `efficiency-gate/v1`. It also does not supply the task semantics,
external-effect receipt, quality oracle, provider billing or reproducible pair
identity required by a future `CoverageContract`; those conclusions must come
from versioned evaluation records and their canonical host evidence.

Import the dashboard from [grafana-harness-core.json](grafana-harness-core.json)
and follow [observability-runbook.md](observability-runbook.md) for alert
triage. Collector/export failures are isolated from request and worker
execution; inspect redacted logs and the queue/approval snapshots when an
exporter is unavailable.
