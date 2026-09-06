package oteltelemetry

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	core "github.com/cc-auto-agent/harness-core/pkg/core"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestRecorderExportsFixedSpansAndMetrics(t *testing.T) {
	traceExporter := tracetest.NewInMemoryExporter()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(traceExporter))
	defer tracerProvider.Shutdown(context.Background())
	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer meterProvider.Shutdown(context.Background())
	recorder, err := New(tracerProvider, meterProvider)
	if err != nil {
		t.Fatal(err)
	}
	ctx, span := recorder.Start(context.Background(), core.SpanRunSegment,
		core.TelemetryAttributes{"run.id": "run-otel", "run.resume": "false"})
	recorder.AddCounter(ctx, core.MetricRuns, 1, core.TelemetryAttributes{"run.status": "completed"})
	recorder.RecordHistogram(ctx, core.MetricRunDuration, 0.25, "s", core.TelemetryAttributes{"run.status": "completed"})
	recorder.SetGauge(ctx, core.MetricQueueDepth, 3, "{run}", core.TelemetryAttributes{"run.status": "queued"})
	recorder.AddCounter(ctx, core.MetricEvaluationRuns, 1, core.TelemetryAttributes{"evaluation.status": "completed"})
	for _, name := range []string{core.MetricModelContextInputBytes, core.MetricModelContextInputTokens, core.MetricModelContextDroppedGroups} {
		recorder.AddCounter(ctx, name, 7, nil)
		recorder.AddCounter(ctx, name, 11, nil)
	}
	recorder.RecordHistogram(ctx, core.MetricEvaluationScore, 1, "1", core.TelemetryAttributes{"evaluation.passed": "true"})
	span.End(errors.New("recorded test error"), core.TelemetryAttributes{"run.status": "failed"})

	spans := traceExporter.GetSpans()
	if len(spans) != 1 || spans[0].Name != core.SpanRunSegment || spans[0].Status.Code.String() != "Error" {
		t.Fatalf("unexpected spans: %#v", spans)
	}
	var metrics metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &metrics); err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, scope := range metrics.ScopeMetrics {
		for _, measurement := range scope.Metrics {
			names[measurement.Name] = true
			if measurement.Name == core.MetricModelContextInputBytes || measurement.Name == core.MetricModelContextInputTokens || measurement.Name == core.MetricModelContextDroppedGroups {
				sum, ok := measurement.Data.(metricdata.Sum[int64])
				if !ok || len(sum.DataPoints) != 1 || sum.DataPoints[0].Value != 18 {
					t.Fatalf("context metric %s did not preserve accumulated cost", measurement.Name)
				}
			}
		}
	}
	for _, name := range []string{
		core.MetricRuns, core.MetricRunDuration, core.MetricQueueDepth,
		core.MetricEvaluationRuns, core.MetricEvaluationScore,
		core.MetricModelContextInputBytes, core.MetricModelContextInputTokens, core.MetricModelContextDroppedGroups,
	} {
		if !names[name] {
			t.Fatalf("metric %s missing from %#v", name, names)
		}
	}
}

func TestCoreSpanContextAttributesReachOTelChildSpans(t *testing.T) {
	traceExporter := tracetest.NewInMemoryExporter()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(traceExporter))
	defer tracerProvider.Shutdown(context.Background())
	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer meterProvider.Shutdown(context.Background())
	recorder, err := New(tracerProvider, meterProvider)
	if err != nil {
		t.Fatal(err)
	}
	ctx := core.WithTelemetrySpanAttributes(context.Background(), core.TelemetryAttributes{
		"harness.canary.id": "canary-otel-context",
	})
	ctx, runSpan := core.StartTelemetry(recorder, ctx, core.SpanRunSegment, core.TelemetryAttributes{"run.id": "run-otel-context"})
	core.AddTelemetryCounter(recorder, ctx, core.MetricRuns, 1, core.TelemetryAttributes{"run.status": "completed"})
	_, modelSpan := core.StartTelemetry(recorder, ctx, core.SpanModelCall, core.TelemetryAttributes{"model.name": "test"})
	modelSpan.End(nil, nil)
	runSpan.End(nil, nil)
	spans := traceExporter.GetSpans()
	if len(spans) != 2 {
		t.Fatalf("spans=%d, want 2", len(spans))
	}
	for _, span := range spans {
		found := false
		for _, attribute := range span.Attributes {
			if string(attribute.Key) == "harness.canary.id" && attribute.Value.AsString() == "canary-otel-context" {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("span %s lost canary attribute: %#v", span.Name, span.Attributes)
		}
	}
	var metrics metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &metrics); err != nil {
		t.Fatal(err)
	}
	foundMetric := false
	for _, scope := range metrics.ScopeMetrics {
		for _, measurement := range scope.Metrics {
			if measurement.Name != core.MetricRuns {
				continue
			}
			foundMetric = true
			sum, ok := measurement.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("run counter data=%T", measurement.Data)
			}
			for _, point := range sum.DataPoints {
				for _, attribute := range point.Attributes.ToSlice() {
					if string(attribute.Key) == "harness.canary.id" {
						t.Fatalf("OTel metric inherited canary id: %#v", point.Attributes.ToSlice())
					}
				}
			}
		}
	}
	if !foundMetric {
		t.Fatal("run counter metric was not collected")
	}
}

func TestHTTPHandlerExtractsTraceContext(t *testing.T) {
	previous := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	defer otel.SetTextMapPropagator(previous)
	valid := false
	handler := HTTPHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		valid = trace.SpanContextFromContext(r.Context()).IsValid()
		w.WriteHeader(http.StatusNoContent)
	}))
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	request.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent || !valid {
		t.Fatalf("trace context was not extracted: status=%d valid=%t", response.Code, valid)
	}
}

func TestNewFromEnvExportsOTLPHTTP(t *testing.T) {
	var mu sync.Mutex
	received := map[string]int{}
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		received[r.URL.Path]++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer collector.Close()
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", collector.URL)
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
	previousTracer := otel.GetTracerProvider()
	previousMeter := otel.GetMeterProvider()
	previousPropagator := otel.GetTextMapPropagator()
	defer func() {
		otel.SetTracerProvider(previousTracer)
		otel.SetMeterProvider(previousMeter)
		otel.SetTextMapPropagator(previousPropagator)
	}()
	recorder, shutdown, err := NewFromEnv(context.Background(), Config{
		ServiceName: "harness-core-test", MetricInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, span := recorder.Start(context.Background(), core.SpanRunSegment, nil)
	recorder.AddCounter(ctx, core.MetricRuns, 1, core.TelemetryAttributes{"run.status": "completed"})
	span.End(nil, core.TelemetryAttributes{"run.status": "completed"})
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := shutdown(shutdownCtx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if received["/v1/traces"] == 0 || received["/v1/metrics"] == 0 {
		t.Fatalf("OTLP exports missing: %#v", received)
	}
}

func TestDurableTraceContextRoundTripKeepsTrace(t *testing.T) {
	traceExporter := tracetest.NewInMemoryExporter()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(traceExporter))
	defer tracerProvider.Shutdown(context.Background())
	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer meterProvider.Shutdown(context.Background())
	recorder, err := New(tracerProvider, meterProvider)
	if err != nil {
		t.Fatal(err)
	}
	previous := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	defer otel.SetTextMapPropagator(previous)
	parentCtx, parent := tracerProvider.Tracer("test").Start(context.Background(), "submit")
	carrier := recorder.InjectTraceContext(parentCtx)
	if carrier.TraceParent == "" {
		t.Fatal("traceparent was not injected")
	}
	workerCtx := recorder.ExtractTraceContext(context.Background(), carrier)
	_, child := recorder.Start(workerCtx, core.SpanRunSegment, nil)
	child.End(nil, nil)
	parent.End()
	spans := traceExporter.GetSpans()
	if len(spans) != 2 {
		t.Fatalf("spans=%d, want 2", len(spans))
	}
	var parentSpan, childSpan tracetest.SpanStub
	for _, span := range spans {
		if span.Name == "submit" {
			parentSpan = span
		} else if span.Name == core.SpanRunSegment {
			childSpan = span
		}
	}
	if parentSpan.SpanContext.TraceID() != childSpan.SpanContext.TraceID() || childSpan.Parent.SpanID() != parentSpan.SpanContext.SpanID() {
		t.Fatalf("trace continuity lost: parent=%s/%s child=%s parent-id=%s",
			parentSpan.SpanContext.TraceID(), parentSpan.SpanContext.SpanID(),
			childSpan.SpanContext.TraceID(), childSpan.Parent.SpanID())
	}
}
