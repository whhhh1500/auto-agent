# OpenTelemetry

`pkg/telemetry/otel` is optional. With OTel disabled, no exporter or background
export goroutine is created and execution semantics are unchanged. When
enabled, configure the standard OTLP/HTTP environment variables and optionally
`HARNESS_OTEL_SERVICE_NAME` and `HARNESS_OTEL_METRIC_INTERVAL`.

The adapter emits low-cardinality run status/count and duration, tool
result/denial, queue claim/recovery, and approval pending/resolved metrics.
Run, session, task, request, payload, and credential values remain trace-only
or are omitted. HTTP trace context uses W3C `traceparent`/`tracestate`; durable
queue rows persist only bounded carriers, never Baggage.

Span error status descriptions and standard `exception.message` events are
limited to 1024 bytes without splitting valid UTF-8. This is a telemetry-size
bound, not secret detection or redaction: callers must still avoid putting
credentials or other sensitive values in errors.

Import the dashboard from [grafana-harness-core.json](grafana-harness-core.json)
and follow [observability-runbook.md](observability-runbook.md) for alert
triage. Collector/export failures are isolated from request and worker
execution; inspect redacted logs and the queue/approval snapshots when an
exporter is unavailable.
