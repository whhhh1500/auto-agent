package core

import (
	"context"
	"sort"
	"strings"
)

const (
	SpanRunSegment     = "harness.run.segment"
	SpanModelCall      = "harness.model.call"
	SpanToolCall       = "harness.tool.call"
	SpanQueueClaim     = "harness.queue.claim"
	SpanEvaluationRun  = "harness.evaluation.run"
	SpanEvaluationCase = "harness.evaluation.case"

	MetricRuns                      = "harness.run.segments"
	MetricRunDuration               = "harness.run.duration"
	MetricModelCalls                = "harness.model.calls"
	MetricModelDuration             = "harness.model.duration"
	MetricModelContextInputBytes    = "harness.model.context.input_bytes"
	MetricModelContextInputTokens   = "harness.model.context.input_tokens"
	MetricModelContextDroppedGroups = "harness.model.context.dropped_groups"
	MetricToolCalls                 = "harness.tool.calls"
	MetricToolDuration              = "harness.tool.duration"
	MetricToolJournalDecisions      = "harness.tool.journal.decisions"
	MetricQueueClaims               = "harness.queue.claims"
	MetricQueueClaimDuration        = "harness.queue.claim.duration"
	MetricQueueRecoveries           = "harness.queue.recoveries"
	MetricQueueDepth                = "harness.queue.depth"
	MetricQueueOldestAge            = "harness.queue.oldest.age"
	MetricApprovalDecisions         = "harness.approval.decisions"
	MetricApprovalRequests          = "harness.approval.requests"
	MetricApprovalWait              = "harness.approval.wait"
	MetricApprovalPending           = "harness.approval.pending"
	MetricApprovalOldestAge         = "harness.approval.oldest.age"
	MetricSubmissions               = "harness.run.submissions"
	MetricEvaluationRuns            = "harness.evaluation.run.segments"
	MetricEvaluationCases           = "harness.evaluation.cases"
	MetricEvaluationGates           = "harness.evaluation.gates"
	MetricEvaluationDuration        = "harness.evaluation.duration"
	MetricEvaluationCaseDuration    = "harness.evaluation.case.duration"
	MetricEvaluationScore           = "harness.evaluation.score"
)

const (
	MaxTelemetryAttributes            = 32
	MaxTelemetrySpanContextAttributes = 8
	MaxTelemetryAttributeKey          = 128
	MaxTelemetryAttributeValue        = 1024
)

// TelemetryAttributes is intentionally string-only and bounded at the safe
// wrappers below. Callers should keep metric attributes low-cardinality; run,
// session and call IDs belong on spans only.
type TelemetryAttributes map[string]string

type telemetrySpanAttributesContextKey struct{}

// WithTelemetrySpanAttributes attaches bounded attributes inherited by every
// StartTelemetry call below this context. Counter, Histogram and Gauge helpers
// deliberately ignore them, so high-cardinality correlation identifiers stay
// on traces only. New values replace inherited values with the same key;
// operation-local StartTelemetry attributes take final precedence.
func WithTelemetrySpanAttributes(ctx context.Context, attributes TelemetryAttributes) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	current := telemetrySpanAttributesFromContext(ctx)
	next := mergeTelemetryAttributes(current, attributes, MaxTelemetrySpanContextAttributes, true)
	if len(next) == 0 {
		return ctx
	}
	return context.WithValue(ctx, telemetrySpanAttributesContextKey{}, next)
}

// TelemetrySpan is the SDK-neutral lifetime returned by Telemetry.Start.
type TelemetrySpan interface {
	End(err error, attributes TelemetryAttributes)
}

// Telemetry is the optional instrumentation seam shared by the kernel and
// outer adapters. pkg/core imports no observability SDK; providers translate
// these fixed operations and metrics to OpenTelemetry or another backend.
type Telemetry interface {
	Start(ctx context.Context, operation string, attributes TelemetryAttributes) (context.Context, TelemetrySpan)
	AddCounter(ctx context.Context, name string, delta int64, attributes TelemetryAttributes)
	RecordHistogram(ctx context.Context, name string, value float64, unit string, attributes TelemetryAttributes)
	SetGauge(ctx context.Context, name string, value float64, unit string, attributes TelemetryAttributes)
}

// TelemetryTraceContext is the minimal durable W3C carrier. Baggage is
// deliberately excluded because it may contain product or user data.
type TelemetryTraceContext struct {
	TraceParent string `json:"traceparent,omitempty"`
	TraceState  string `json:"tracestate,omitempty"`
}

// TelemetryContext optionally propagates trace context through durable queues.
// It is separate from Telemetry so non-tracing metric adapters stay small.
type TelemetryContext interface {
	InjectTraceContext(ctx context.Context) TelemetryTraceContext
	ExtractTraceContext(ctx context.Context, trace TelemetryTraceContext) context.Context
}

type noopTelemetrySpan struct{}

func (noopTelemetrySpan) End(error, TelemetryAttributes) {}

// StartTelemetry isolates telemetry-provider panics and always returns a valid
// context plus idempotent-safe no-op span fallback.
func StartTelemetry(telemetry Telemetry, ctx context.Context, operation string, attributes TelemetryAttributes) (out context.Context, span TelemetrySpan) {
	if ctx == nil {
		ctx = context.Background()
	}
	inherited := telemetrySpanAttributesFromContext(ctx)
	spanAttributes := mergeTelemetrySpanStartAttributes(inherited, attributes)
	out, span = ctx, noopTelemetrySpan{}
	if telemetry == nil {
		return out, span
	}
	defer func() {
		if recover() != nil {
			out, span = ctx, noopTelemetrySpan{}
		}
	}()
	started, candidate := telemetry.Start(ctx, boundedTelemetryText(operation, MaxTelemetryAttributeKey), spanAttributes)
	if started != nil {
		out = started
	}
	if len(inherited) > 0 {
		// Providers normally preserve Context values, but the safety seam does
		// not trust that behavior. Reattach only inherited Span attributes;
		// operation-local fields must not leak into child spans.
		out = context.WithValue(out, telemetrySpanAttributesContextKey{}, inherited)
	}
	if candidate != nil {
		span = safeTelemetrySpan{inner: candidate}
	}
	return out, span
}

type safeTelemetrySpan struct{ inner TelemetrySpan }

func (s safeTelemetrySpan) End(err error, attributes TelemetryAttributes) {
	defer func() { _ = recover() }()
	if s.inner != nil {
		s.inner.End(err, sanitizeTelemetryAttributes(attributes))
	}
}

func AddTelemetryCounter(telemetry Telemetry, ctx context.Context, name string, delta int64, attributes TelemetryAttributes) {
	if telemetry == nil {
		return
	}
	defer func() { _ = recover() }()
	telemetry.AddCounter(ctx, boundedTelemetryText(name, MaxTelemetryAttributeKey), delta, sanitizeTelemetryAttributes(attributes))
}

func RecordTelemetryHistogram(telemetry Telemetry, ctx context.Context, name string, value float64, unit string, attributes TelemetryAttributes) {
	if telemetry == nil {
		return
	}
	defer func() { _ = recover() }()
	telemetry.RecordHistogram(ctx, boundedTelemetryText(name, MaxTelemetryAttributeKey), value,
		boundedTelemetryText(unit, 32), sanitizeTelemetryAttributes(attributes))
}

func SetTelemetryGauge(telemetry Telemetry, ctx context.Context, name string, value float64, unit string, attributes TelemetryAttributes) {
	if telemetry == nil {
		return
	}
	defer func() { _ = recover() }()
	telemetry.SetGauge(ctx, boundedTelemetryText(name, MaxTelemetryAttributeKey), value,
		boundedTelemetryText(unit, 32), sanitizeTelemetryAttributes(attributes))
}

func InjectTelemetryTraceContext(telemetry Telemetry, ctx context.Context) (trace TelemetryTraceContext) {
	propagator, ok := telemetry.(TelemetryContext)
	if !ok {
		return TelemetryTraceContext{}
	}
	defer func() {
		if recover() != nil {
			trace = TelemetryTraceContext{}
		}
	}()
	trace = propagator.InjectTraceContext(ctx)
	trace.TraceParent = boundedTelemetryText(trace.TraceParent, 256)
	trace.TraceState = boundedTelemetryText(trace.TraceState, 512)
	return trace
}

func ExtractTelemetryTraceContext(telemetry Telemetry, ctx context.Context, trace TelemetryTraceContext) (out context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	out = ctx
	propagator, ok := telemetry.(TelemetryContext)
	if !ok || (trace.TraceParent == "" && trace.TraceState == "") {
		return out
	}
	defer func() {
		if recover() != nil || out == nil {
			out = ctx
		}
	}()
	return propagator.ExtractTraceContext(ctx, trace)
}

func sanitizeTelemetryAttributes(attributes TelemetryAttributes) TelemetryAttributes {
	return mergeTelemetryAttributes(nil, attributes, MaxTelemetryAttributes, true)
}

func telemetrySpanAttributesFromContext(ctx context.Context) TelemetryAttributes {
	if ctx == nil {
		return nil
	}
	attributes, _ := ctx.Value(telemetrySpanAttributesContextKey{}).(TelemetryAttributes)
	return mergeTelemetryAttributes(nil, attributes, MaxTelemetrySpanContextAttributes, true)
}

func mergeTelemetrySpanStartAttributes(inherited, local TelemetryAttributes) TelemetryAttributes {
	base := cleanTelemetryAttributes(inherited)
	overlay := cleanTelemetryAttributes(local)
	if len(base) == 0 {
		return takeTelemetryAttributes(overlay, MaxTelemetryAttributes)
	}
	out := make(TelemetryAttributes, min(MaxTelemetryAttributes, len(base)+len(overlay)))
	for _, key := range sortedTelemetryAttributeKeys(base) {
		if len(out) >= MaxTelemetrySpanContextAttributes {
			break
		}
		out[key] = base[key]
	}
	// Collision overrides are applied before capacity filtering, so a local
	// run.id can never be displaced by unrelated local keys.
	for _, key := range sortedTelemetryAttributeKeys(overlay) {
		if _, exists := out[key]; exists {
			out[key] = overlay[key]
		}
	}
	for _, key := range sortedTelemetryAttributeKeys(overlay) {
		if _, exists := out[key]; exists {
			continue
		}
		if len(out) >= MaxTelemetryAttributes {
			break
		}
		out[key] = overlay[key]
	}
	return out
}

// mergeTelemetryAttributes sanitizes deterministically. When preferOverlay is
// true, overlay values replace base values and are retained ahead of base-only
// keys when the bound is reached.
func mergeTelemetryAttributes(base, overlay TelemetryAttributes, limit int, preferOverlay bool) TelemetryAttributes {
	if limit <= 0 || (len(base) == 0 && len(overlay) == 0) {
		return nil
	}
	baseClean, overlayClean := cleanTelemetryAttributes(base), cleanTelemetryAttributes(overlay)
	first, second := baseClean, overlayClean
	if preferOverlay {
		first, second = overlayClean, baseClean
	}
	out := make(TelemetryAttributes, min(limit, len(baseClean)+len(overlayClean)))
	// Preserve explicit replacements before choosing among new keys.
	for _, key := range sortedTelemetryAttributeKeys(first) {
		if _, collision := second[key]; collision {
			out[key] = first[key]
		}
	}
	for _, values := range []TelemetryAttributes{first, second} {
		for _, key := range sortedTelemetryAttributeKeys(values) {
			if _, exists := out[key]; exists {
				continue
			}
			if len(out) >= limit {
				break
			}
			out[key] = values[key]
		}
	}
	return out
}

func cleanTelemetryAttributes(values TelemetryAttributes) TelemetryAttributes {
	out := TelemetryAttributes{}
	for _, key := range sortedTelemetryAttributeKeys(values) {
		cleanKey := strings.TrimSpace(boundedTelemetryText(key, MaxTelemetryAttributeKey))
		if cleanKey == "" {
			continue
		}
		out[cleanKey] = boundedTelemetryText(values[key], MaxTelemetryAttributeValue)
	}
	return out
}

func sortedTelemetryAttributeKeys(values TelemetryAttributes) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func takeTelemetryAttributes(values TelemetryAttributes, limit int) TelemetryAttributes {
	if limit <= 0 || len(values) == 0 {
		return nil
	}
	out := make(TelemetryAttributes, min(limit, len(values)))
	for _, key := range sortedTelemetryAttributeKeys(values) {
		if len(out) >= limit {
			break
		}
		out[key] = values[key]
	}
	return out
}

func boundedTelemetryText(value string, limit int) string {
	value = strings.Map(func(char rune) rune {
		if char < 0x20 || char == 0x7f {
			return -1
		}
		return char
	}, value)
	if len(value) > limit {
		return value[:limit]
	}
	return value
}
