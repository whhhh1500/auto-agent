package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func TestRotatingWriterRotatesAndPrunes(t *testing.T) {
	dir := t.TempDir()
	writer, err := NewRotatingWriter(Options{Dir: dir, Name: "harness.log", MaxBytes: 300, MaxBackups: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()

	// Write well past three rotations.
	for i := 0; i < 40; i++ {
		line := strings.Repeat("x", 100) + "\n"
		if _, err := writer.Write([]byte(line)); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	names := []string{}
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	rotated := 0
	active := false
	for _, name := range names {
		if name == "harness.log" {
			active = true
			continue
		}
		if strings.HasPrefix(name, "harness-") && strings.HasSuffix(name, ".log") {
			rotated++
		}
	}
	if !active {
		t.Fatalf("active log missing: %v", names)
	}
	if rotated > 2 {
		t.Fatalf("prune kept %d backups beyond the limit: %v", rotated, names)
	}
	if rotated < 2 {
		t.Fatalf("expected rotations to happen: %v", names)
	}

	// Rotation preserves earlier content in backups: at least one backup holds
	// the filler bytes.
	found := false
	for _, name := range names {
		if !strings.HasPrefix(name, "harness-") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err == nil && bytes.Contains(data, []byte(strings.Repeat("x", 100))) {
			found = true
		}
	}
	if !found {
		t.Fatal("backup files lost their content")
	}
}

func TestNewProducesWorkingJSONLogger(t *testing.T) {
	dir := t.TempDir()
	logger, closer, err := New(Options{Dir: dir, MaxBytes: 1 << 20, MaxBackups: 3})
	if err != nil {
		t.Fatal(err)
	}
	defer closer.Close()
	logger.Info("audit", "action", "policy.bind", "actor", "admin-1@sabot.com")

	data, err := os.ReadFile(filepath.Join(dir, "harness.log"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"action":"policy.bind"`) || !strings.Contains(string(data), `"msg":"audit"`) {
		t.Fatalf("structured record missing: %s", data)
	}
	// The same logger serves many records without rotation issues.
	for i := 0; i < 10; i++ {
		logger.Info("http", "path", "/v1/sessions")
	}
}

func TestLogPathsArePrivateOnUnix(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "logs")
	logger, closer, err := New(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer closer.Close()
	logger.Info("private")
	if runtime.GOOS == "windows" {
		return
	}
	for _, test := range []struct {
		path string
		want os.FileMode
	}{{dir, 0o700}, {filepath.Join(dir, "harness.log"), 0o600}} {
		info, err := os.Stat(test.path)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != test.want {
			t.Fatalf("%s mode=%#o want %#o", test.path, got, test.want)
		}
	}
}

func TestNewAddsTraceFieldsFromContext(t *testing.T) {
	dir := t.TempDir()
	logger, closer, err := New(Options{Dir: dir, MaxBytes: 1 << 20, MaxBackups: 3})
	if err != nil {
		t.Fatal(err)
	}
	if closer == nil {
		t.Fatal("New returned a discard logger")
	}

	spanContext := testRemoteSpanContext(t, trace.FlagsSampled)
	logger.InfoContext(trace.ContextWithRemoteSpanContext(context.Background(), spanContext), "audit")
	if err := closer.Close(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(filepath.Join(dir, "harness.log"))
	if err != nil {
		t.Fatal(err)
	}
	record := decodeSingleJSONRecord(t, data)
	assertTraceFields(t, record, spanContext, "01")
}

func TestTraceHandlerAddsSampledLocalAndUnsampledRemoteSpanContexts(t *testing.T) {
	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	localContext, localSpan := provider.Tracer("logging-test").Start(context.Background(), "request")
	t.Cleanup(func() { localSpan.End() })
	localSpanContext := localSpan.SpanContext()
	if !localSpanContext.IsSampled() || localSpanContext.IsRemote() {
		t.Fatalf("unexpected local span context: sampled=%t remote=%t", localSpanContext.IsSampled(), localSpanContext.IsRemote())
	}

	remoteSpanContext := testRemoteSpanContext(t, 0)
	if !remoteSpanContext.IsRemote() || remoteSpanContext.IsSampled() {
		t.Fatalf("unexpected remote span context: sampled=%t remote=%t", remoteSpanContext.IsSampled(), remoteSpanContext.IsRemote())
	}

	tests := []struct {
		name      string
		ctx       context.Context
		span      trace.SpanContext
		traceFlag string
		errorLog  bool
	}{
		{
			name:      "sampled local span from SDK tracer provider",
			ctx:       localContext,
			span:      localSpanContext,
			traceFlag: "01",
		},
		{
			name:      "ErrorContext with unsampled valid remote span context",
			ctx:       trace.ContextWithRemoteSpanContext(context.Background(), remoteSpanContext),
			span:      remoteSpanContext,
			traceFlag: "00",
			errorLog:  true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			logger := newTraceTestLogger(&output)
			if test.errorLog {
				logger.ErrorContext(test.ctx, "request")
			} else {
				logger.InfoContext(test.ctx, "request")
			}

			record := decodeSingleJSONRecord(t, output.Bytes())
			assertTraceFields(t, record, test.span, test.traceFlag)
		})
	}
}

func TestTraceHandlerOmitsInvalidOrMissingSpanContext(t *testing.T) {
	for _, test := range []struct {
		name string
		ctx  context.Context
	}{
		{name: "missing", ctx: context.Background()},
		{name: "invalid", ctx: trace.ContextWithSpanContext(context.Background(), trace.SpanContext{})},
	} {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			newTraceTestLogger(&output).InfoContext(test.ctx, "request")

			record := decodeSingleJSONRecord(t, output.Bytes())
			for _, key := range []string{"trace_id", "span_id", "trace_flags"} {
				if _, exists := record[key]; exists {
					t.Fatalf("invalid SpanContext emitted %q: %#v", key, record)
				}
			}
		})
	}
}

func TestTraceHandlerPreservesBoundAttrsAndGroups(t *testing.T) {
	spanContext := testRemoteSpanContext(t, trace.FlagsSampled)
	ctx := trace.ContextWithRemoteSpanContext(context.Background(), spanContext)

	var output bytes.Buffer
	logger := newTraceTestLogger(&output).
		With("service", "gateway").
		WithGroup("request").
		With("method", "GET")
	logger.InfoContext(ctx, "served", "status", 201)

	record := decodeSingleJSONRecord(t, output.Bytes())
	if got := requireString(t, record, "service"); got != "gateway" {
		t.Fatalf("service = %q, want gateway", got)
	}
	request := requireObject(t, record, "request")
	if got := requireString(t, request, "method"); got != "GET" {
		t.Fatalf("request.method = %q, want GET", got)
	}
	if got, ok := request["status"].(float64); !ok || got != 201 {
		t.Fatalf("request.status = %#v, want 201", request["status"])
	}
	if count := bytes.Count(output.Bytes(), []byte(`"request":`)); count != 1 {
		t.Fatalf("request group appeared %d times: %s", count, output.Bytes())
	}
	assertTraceFields(t, record, spanContext, "01")
	for _, key := range []string{"trace_id", "span_id", "trace_flags"} {
		if _, exists := request[key]; exists {
			t.Fatalf("trace field %q was incorrectly placed in request group: %#v", key, request)
		}
	}
}

func TestTraceHandlerExplicitTraceFieldsWinWithoutDuplicates(t *testing.T) {
	spanContext := testRemoteSpanContext(t, trace.FlagsSampled)
	ctx := trace.ContextWithRemoteSpanContext(context.Background(), spanContext)

	var output bytes.Buffer
	logger := newTraceTestLogger(&output).
		With("trace_id", "bound-trace").
		WithGroup("request").
		With("span_id", "bound-span")
	logger.InfoContext(ctx, "audit", slog.Group("client", slog.String("trace_flags", "record-flags")))

	record := decodeSingleJSONRecord(t, output.Bytes())
	if got := requireString(t, record, "trace_id"); got != "bound-trace" {
		t.Fatalf("trace_id = %q, want bound-trace", got)
	}
	request := requireObject(t, record, "request")
	if got := requireString(t, request, "span_id"); got != "bound-span" {
		t.Fatalf("request.span_id = %q, want bound-span", got)
	}
	client := requireObject(t, request, "client")
	if got := requireString(t, client, "trace_flags"); got != "record-flags" {
		t.Fatalf("client.trace_flags = %q, want record-flags", got)
	}

	for _, key := range []string{"trace_id", "span_id", "trace_flags"} {
		if count := bytes.Count(output.Bytes(), []byte(`"`+key+`":`)); count != 1 {
			t.Fatalf("%q appeared %d times, want exactly one: %s", key, count, output.Bytes())
		}
	}
	if _, exists := record["span_id"]; exists {
		t.Fatalf("automatic span_id was emitted despite explicit grouped field: %#v", record)
	}
	if _, exists := record["trace_flags"]; exists {
		t.Fatalf("automatic trace_flags was emitted despite explicit record field: %#v", record)
	}
}

func TestTraceHandlerExplicitTraceFieldGroupWinsWithoutDuplicates(t *testing.T) {
	spanContext := testRemoteSpanContext(t, trace.FlagsSampled)
	ctx := trace.ContextWithRemoteSpanContext(context.Background(), spanContext)

	var output bytes.Buffer
	newTraceTestLogger(&output).
		WithGroup("trace_id").
		With("source", "user").
		InfoContext(ctx, "audit")

	record := decodeSingleJSONRecord(t, output.Bytes())
	traceID := requireObject(t, record, "trace_id")
	if got := requireString(t, traceID, "source"); got != "user" {
		t.Fatalf("trace_id.source = %q, want user", got)
	}
	if count := bytes.Count(output.Bytes(), []byte(`"trace_id":`)); count != 1 {
		t.Fatalf("trace_id appeared %d times, want exactly one: %s", count, output.Bytes())
	}
	if _, exists := record["trace_flags"]; !exists {
		t.Fatalf("trace_flags was not generated: %#v", record)
	}
	if _, exists := record["span_id"]; !exists {
		t.Fatalf("span_id was not generated: %#v", record)
	}
}

func newTraceTestLogger(writer *bytes.Buffer) *slog.Logger {
	return slog.New(newTraceHandler(slog.NewJSONHandler(writer, nil)))
}

func testRemoteSpanContext(t *testing.T, flags trace.TraceFlags) trace.SpanContext {
	t.Helper()
	traceID, err := trace.TraceIDFromHex("0123456789abcdef0123456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	spanID, err := trace.SpanIDFromHex("0123456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: flags,
		Remote:     true,
	})
}

func decodeSingleJSONRecord(t *testing.T, data []byte) map[string]any {
	t.Helper()
	var record map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(data), &record); err != nil {
		t.Fatalf("decode JSON log record: %v\n%s", err, data)
	}
	return record
}

func assertTraceFields(t *testing.T, record map[string]any, spanContext trace.SpanContext, traceFlags string) {
	t.Helper()
	if got := requireString(t, record, "trace_id"); got != spanContext.TraceID().String() {
		t.Fatalf("trace_id = %q, want %q", got, spanContext.TraceID().String())
	} else if got != strings.ToLower(got) {
		t.Fatalf("trace_id = %q, want lowercase hexadecimal", got)
	}
	if got := requireString(t, record, "span_id"); got != spanContext.SpanID().String() {
		t.Fatalf("span_id = %q, want %q", got, spanContext.SpanID().String())
	} else if got != strings.ToLower(got) {
		t.Fatalf("span_id = %q, want lowercase hexadecimal", got)
	}
	if got := requireString(t, record, "trace_flags"); got != traceFlags {
		t.Fatalf("trace_flags = %q, want %q", got, traceFlags)
	} else if got != strings.ToLower(got) {
		t.Fatalf("trace_flags = %q, want lowercase hexadecimal", got)
	}
}

func requireString(t *testing.T, record map[string]any, key string) string {
	t.Helper()
	value, ok := record[key].(string)
	if !ok {
		t.Fatalf("%q = %#v, want string", key, record[key])
	}
	return value
}

func requireObject(t *testing.T, record map[string]any, key string) map[string]any {
	t.Helper()
	value, ok := record[key].(map[string]any)
	if !ok {
		t.Fatalf("%q = %#v, want object", key, record[key])
	}
	return value
}

func TestDiscardFallbackOnBadDir(t *testing.T) {
	// Place a FILE where the log directory is wanted: directory creation must
	// fail, and the logger must degrade to discard instead of panicking.
	base := t.TempDir()
	blocker := filepath.Join(base, "file.txt")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	logger, closer, err := New(Options{Dir: blocker})
	if err != nil {
		t.Fatalf("New must not fail on an unusable dir: %v", err)
	}
	if closer != nil {
		t.Fatal("discard fallback must not own a file")
	}
	logger.Info("still alive")
	if _, ok := logger.Handler().(discardHandler); !ok {
		t.Fatalf("expected discard fallback, got %T", logger.Handler())
	}
}
