// Package oteltelemetry adapts the SDK-neutral core.Telemetry seam to
// OpenTelemetry traces and metrics.
package oteltelemetry

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	core "github.com/cc-auto-agent/harness-core/pkg/core"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationName = "harness-core"

const maxTelemetryErrorBytes = 1024

type Recorder struct {
	tracer     trace.Tracer
	counters   map[string]metric.Int64Counter
	histograms map[string]metric.Float64Histogram
	gauges     map[string]metric.Float64Gauge
}

// New creates a recorder over caller-owned providers. Instruments are fixed at
// construction so unknown metric names cannot grow SDK state dynamically.
func New(tracerProvider trace.TracerProvider, meterProvider metric.MeterProvider) (*Recorder, error) {
	if tracerProvider == nil || meterProvider == nil {
		return nil, fmt.Errorf("OpenTelemetry providers are incomplete")
	}
	meter := meterProvider.Meter(instrumentationName)
	recorder := &Recorder{
		tracer:     tracerProvider.Tracer(instrumentationName),
		counters:   map[string]metric.Int64Counter{},
		histograms: map[string]metric.Float64Histogram{},
		gauges:     map[string]metric.Float64Gauge{},
	}
	for _, name := range []string{
		core.MetricRuns, core.MetricModelCalls, core.MetricToolCalls,
		core.MetricToolJournalDecisions, core.MetricQueueClaims,
		core.MetricQueueRecoveries, core.MetricApprovalDecisions,
		core.MetricApprovalRequests, core.MetricSubmissions,
		core.MetricEvaluationRuns, core.MetricEvaluationCases, core.MetricEvaluationGates,
		core.MetricModelContextInputBytes, core.MetricModelContextInputTokens, core.MetricModelContextDroppedGroups,
	} {
		instrument, err := meter.Int64Counter(name)
		if err != nil {
			return nil, fmt.Errorf("create counter %s: %w", name, err)
		}
		recorder.counters[name] = instrument
	}
	for _, definition := range []struct{ name, unit string }{
		{core.MetricRunDuration, "s"},
		{core.MetricModelDuration, "s"},
		{core.MetricToolDuration, "s"},
		{core.MetricQueueClaimDuration, "s"},
		{core.MetricApprovalWait, "s"},
		{core.MetricEvaluationDuration, "s"},
		{core.MetricEvaluationCaseDuration, "s"},
		{core.MetricEvaluationScore, "1"},
	} {
		instrument, err := meter.Float64Histogram(definition.name, metric.WithUnit(definition.unit))
		if err != nil {
			return nil, fmt.Errorf("create histogram %s: %w", definition.name, err)
		}
		recorder.histograms[definition.name] = instrument
	}
	for _, definition := range []struct{ name, unit string }{
		{core.MetricQueueDepth, "{run}"},
		{core.MetricQueueOldestAge, "s"},
		{core.MetricApprovalPending, "{approval}"},
		{core.MetricApprovalOldestAge, "s"},
	} {
		instrument, err := meter.Float64Gauge(definition.name, metric.WithUnit(definition.unit))
		if err != nil {
			return nil, fmt.Errorf("create gauge %s: %w", definition.name, err)
		}
		recorder.gauges[definition.name] = instrument
	}
	return recorder, nil
}

func (r *Recorder) Start(ctx context.Context, operation string, attributes core.TelemetryAttributes) (context.Context, core.TelemetrySpan) {
	ctx, span := r.tracer.Start(ctx, operation, trace.WithAttributes(otelAttributes(attributes)...))
	return ctx, spanEnder{span: span}
}

func (r *Recorder) AddCounter(ctx context.Context, name string, delta int64, attributes core.TelemetryAttributes) {
	if instrument, ok := r.counters[name]; ok {
		instrument.Add(ctx, delta, metric.WithAttributes(otelAttributes(attributes)...))
	}
}

func (r *Recorder) RecordHistogram(ctx context.Context, name string, value float64, _ string, attributes core.TelemetryAttributes) {
	if instrument, ok := r.histograms[name]; ok {
		instrument.Record(ctx, value, metric.WithAttributes(otelAttributes(attributes)...))
	}
}

func (r *Recorder) SetGauge(ctx context.Context, name string, value float64, _ string, attributes core.TelemetryAttributes) {
	if instrument, ok := r.gauges[name]; ok {
		instrument.Record(ctx, value, metric.WithAttributes(otelAttributes(attributes)...))
	}
}

type spanEnder struct{ span trace.Span }

func (s spanEnder) End(err error, attributes core.TelemetryAttributes) {
	if s.span == nil {
		return
	}
	s.span.SetAttributes(otelAttributes(attributes)...)
	if err != nil {
		message := boundedError(err)
		// Preserve the standard RecordError exception event semantics while
		// bounding its message before the SDK records it.
		s.span.AddEvent("exception", trace.WithAttributes(
			attribute.String("exception.type", telemetryErrorType(err)),
			attribute.String("exception.message", message),
		))
		s.span.SetStatus(codes.Error, message)
	}
	s.span.End()
}

func boundedError(err error) string {
	if err == nil {
		return ""
	}
	message := err.Error()
	if len(message) <= maxTelemetryErrorBytes {
		return message
	}
	limit := maxTelemetryErrorBytes
	for limit > 0 && !utf8.RuneStart(message[limit]) {
		limit--
	}
	return message[:limit]
}

// telemetryErrorType matches the standard OTel RecordError exception.type
// formatting so existing error-type aggregation remains stable.
func telemetryErrorType(err error) string {
	typeOf := reflect.TypeOf(err)
	if typeOf.PkgPath() == "" && typeOf.Name() == "" {
		return typeOf.String()
	}
	return fmt.Sprintf("%s.%s", typeOf.PkgPath(), typeOf.Name())
}

func otelAttributes(values core.TelemetryAttributes) []attribute.KeyValue {
	if len(values) == 0 {
		return nil
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]attribute.KeyValue, 0, len(keys))
	for _, key := range keys {
		out = append(out, attribute.String(key, values[key]))
	}
	return out
}

// Config controls the optional SDK/exporter bootstrap. OTLP exporters still
// honor the standard OTEL_EXPORTER_OTLP_* environment variables.
type Config struct {
	ServiceName    string
	MetricInterval time.Duration
}

// NewFromEnv builds OTLP/HTTP trace and metric providers and installs W3C
// TraceContext+Baggage propagation globally. The returned shutdown must be
// called during service termination.
func NewFromEnv(ctx context.Context, config Config) (*Recorder, func(context.Context) error, error) {
	serviceName := strings.TrimSpace(config.ServiceName)
	if serviceName == "" {
		serviceName = "harness-core"
	}
	interval := config.MetricInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	traceExporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("create OTLP trace exporter: %w", err)
	}
	metricExporter, err := otlpmetrichttp.New(ctx)
	if err != nil {
		_ = traceExporter.Shutdown(ctx)
		return nil, nil, fmt.Errorf("create OTLP metric exporter: %w", err)
	}
	res, err := resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
		resource.WithAttributes(attribute.String("service.name", serviceName)),
	)
	if err != nil {
		_ = metricExporter.Shutdown(ctx)
		_ = traceExporter.Shutdown(ctx)
		return nil, nil, fmt.Errorf("create OpenTelemetry resource: %w", err)
	}
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter),
		sdktrace.WithResource(res),
	)
	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter, sdkmetric.WithInterval(interval))),
		sdkmetric.WithResource(res),
	)
	recorder, err := New(tracerProvider, meterProvider)
	if err != nil {
		_ = meterProvider.Shutdown(ctx)
		_ = tracerProvider.Shutdown(ctx)
		return nil, nil, err
	}
	otel.SetTracerProvider(tracerProvider)
	otel.SetMeterProvider(meterProvider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))
	shutdown := func(shutdownCtx context.Context) error {
		return errors.Join(meterProvider.Shutdown(shutdownCtx), tracerProvider.Shutdown(shutdownCtx))
	}
	return recorder, shutdown, nil
}

// HTTPHandler adds standard HTTP server spans and extracts incoming W3C trace
// context. The operation name is fixed to avoid path-cardinality explosions.
func HTTPHandler(handler http.Handler) http.Handler {
	return otelhttp.NewHandler(handler, "harness.http")
}

func (r *Recorder) InjectTraceContext(ctx context.Context) core.TelemetryTraceContext {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return core.TelemetryTraceContext{
		TraceParent: carrier.Get("traceparent"), TraceState: carrier.Get("tracestate"),
	}
}

func (r *Recorder) ExtractTraceContext(ctx context.Context, traceContext core.TelemetryTraceContext) context.Context {
	carrier := propagation.MapCarrier{}
	if traceContext.TraceParent != "" {
		carrier.Set("traceparent", traceContext.TraceParent)
	}
	if traceContext.TraceState != "" {
		carrier.Set("tracestate", traceContext.TraceState)
	}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

var _ core.Telemetry = (*Recorder)(nil)
var _ core.TelemetryContext = (*Recorder)(nil)
