package server

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

type capturedServerLog struct {
	message     string
	spanContext trace.SpanContext
}

type capturingServerLogHandler struct {
	mu      sync.Mutex
	records []capturedServerLog
}

func (*capturingServerLogHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *capturingServerLogHandler) Handle(ctx context.Context, record slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, capturedServerLog{
		message: record.Message, spanContext: trace.SpanContextFromContext(ctx),
	})
	return nil
}

func (h *capturingServerLogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *capturingServerLogHandler) WithGroup(string) slog.Handler      { return h }

func (h *capturingServerLogHandler) spanContextFor(message string) (trace.SpanContext, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, record := range h.records {
		if record.message == message {
			return record.spanContext, true
		}
	}
	return trace.SpanContext{}, false
}

func TestContextualServerLogsCarryOTelSpanContext(t *testing.T) {
	tracerProvider := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = tracerProvider.Shutdown(context.Background()) })

	tests := []struct {
		name       string
		message    string
		wantStatus int
		handler    func(*slog.Logger) http.Handler
	}{
		{
			name: "access log beneath otelhttp", message: "http", wantStatus: http.StatusNoContent,
			handler: func(logger *slog.Logger) http.Handler {
				return (&Server{logger: logger}).accessLog(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(http.StatusNoContent)
				}))
			},
		},
		{
			name: "audit", message: "audit", wantStatus: http.StatusNoContent,
			handler: func(logger *slog.Logger) http.Handler {
				server := &Server{logger: logger}
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					server.recordAudit(r, core.Principal{SubjectID: "alice", TenantID: "acme"}, "test.audit", "target", nil)
					w.WriteHeader(http.StatusNoContent)
				})
			},
		},
		{
			name: "panic recovery", message: "panic recovered", wantStatus: http.StatusInternalServerError,
			handler: func(logger *slog.Logger) http.Handler {
				return recoverMiddleware(logger, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
					panic("test panic")
				}))
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			capture := &capturingServerLogHandler{}
			handler := otelhttp.NewHandler(test.handler(slog.New(capture)), "server.log-context",
				otelhttp.WithTracerProvider(tracerProvider))
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/test", nil))
			if response.Code != test.wantStatus {
				t.Fatalf("status=%d want=%d", response.Code, test.wantStatus)
			}
			spanContext, ok := capture.spanContextFor(test.message)
			if !ok {
				t.Fatalf("log %q was not captured", test.message)
			}
			if !spanContext.IsValid() {
				t.Fatalf("log %q received invalid SpanContext: %#v", test.message, spanContext)
			}
		})
	}
}

func TestWithoutCancelPreservesOTelSpanContext(t *testing.T) {
	tracerProvider := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = tracerProvider.Shutdown(context.Background()) })

	ctx, span := tracerProvider.Tracer("server-log-test").Start(context.Background(), "request")
	defer span.End()
	want := trace.SpanContextFromContext(ctx)
	cancelledCtx, cancel := context.WithCancel(ctx)
	cancel()
	preservedCtx := context.WithoutCancel(cancelledCtx)
	got := trace.SpanContextFromContext(preservedCtx)
	if !got.IsValid() || got.TraceID() != want.TraceID() || got.SpanID() != want.SpanID() || got.TraceFlags() != want.TraceFlags() {
		t.Fatalf("WithoutCancel lost SpanContext: got=%#v want=%#v", got, want)
	}
	if err := preservedCtx.Err(); err != nil {
		t.Fatalf("WithoutCancel retained cancellation: %v", err)
	}
}
