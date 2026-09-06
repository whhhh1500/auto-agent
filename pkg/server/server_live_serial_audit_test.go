package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cc-auto-agent/harness-core/pkg/core"
	oteltelemetry "github.com/cc-auto-agent/harness-core/pkg/telemetry/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type serialAuditedAPI struct {
	*postgresAPI
	exporter *tracetest.InMemoryExporter
	metrics  *sdkmetric.ManualReader
}

func newSerialAuditedAPI(t *testing.T, db *sql.DB, model core.LlmAdapter, effects *atomic.Int32) *serialAuditedAPI {
	t.Helper()
	api := newPostgresAPI(t, db, model, effects)
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = meterProvider.Shutdown(context.Background()) })
	recorder, err := oteltelemetry.New(provider, meterProvider)
	if err != nil {
		t.Fatal(err)
	}
	api.server.runtime.Telemetry = recorder
	api.server.telemetry = recorder
	return &serialAuditedAPI{postgresAPI: api, exporter: exporter, metrics: reader}
}

// Read the same canonical event API used to reconstruct chat history and the
// Console event timeline. Small pages exercise after_seq, including seq zero.
func serialAuditHistory(t *testing.T, api *serialAuditedAPI, session string) []core.SessionEvent {
	t.Helper()
	client := &http.Client{Timeout: 20 * time.Second}
	var header struct {
		ID      string `json:"id"`
		Version int64  `json:"version"`
	}
	if err := acceptanceRequest(client, http.MethodGet, api.http.URL+"/v1/sessions/"+session, nil, &header); err != nil {
		t.Fatal(err)
	}
	var all []core.SessionEvent
	after, pages := int64(-1), 0
	for {
		var page struct {
			SessionID string              `json:"session_id"`
			Version   int64               `json:"version"`
			Events    []core.SessionEvent `json:"events"`
			Next      int64               `json:"next_after_seq"`
			More      bool                `json:"has_more"`
		}
		url := fmt.Sprintf("%s/v1/sessions/%s/events?after_seq=%d&limit=3", api.http.URL, session, after)
		if err := acceptanceRequest(client, http.MethodGet, url, nil, &page); err != nil {
			t.Fatal(err)
		}
		pages++
		if page.SessionID != session || page.Version != header.Version || pages > core.MaxSessionEvents {
			t.Fatal("unstable history pagination")
		}
		for _, event := range page.Events {
			if event.Seq != int64(len(all)) {
				t.Fatalf("history gap or duplicate at seq=%d", event.Seq)
			}
			all = append(all, event)
		}
		if !page.More {
			break
		}
		if page.Next <= after {
			t.Fatal("history cursor did not advance")
		}
		after = page.Next
	}
	stored, err := api.sessions.Load(context.Background(), session)
	if err != nil {
		t.Fatal(err)
	}
	if header.ID != session || int64(len(all)) != header.Version || !reflect.DeepEqual(all, stored.Events()) {
		t.Fatal("HTTP history differs from durable SQL events")
	}
	t.Logf("serial history session=%s events=%d pages=%d http_matches_sql=true", session, len(all), pages)
	return all
}

type serialSpanEvidence struct {
	Name       string            `json:"name"`
	TraceID    string            `json:"trace_id"`
	SpanID     string            `json:"span_id"`
	ParentID   string            `json:"parent_id"`
	DurationMS int64             `json:"duration_ms"`
	Status     string            `json:"status"`
	Attributes map[string]string `json:"attributes"`
}

func serialAuditRun(t *testing.T, api *serialAuditedAPI, session, runID string) {
	t.Helper()
	events := serialAuditHistory(t, api, session)
	spans := make([]serialSpanEvidence, 0)
	for _, span := range api.exporter.GetSpans() {
		attrs := map[string]string{}
		for _, attr := range span.Attributes {
			attrs[string(attr.Key)] = attr.Value.AsString()
		}
		if attrs["run.id"] != runID {
			continue
		}
		spans = append(spans, serialSpanEvidence{span.Name, span.SpanContext.TraceID().String(), span.SpanContext.SpanID().String(), span.Parent.SpanID().String(), span.EndTime.Sub(span.StartTime).Milliseconds(), span.Status.Code.String(), attrs})
	}
	// Export before assertions so failed acceptance is still reviewable after
	// the isolated SQL schema is dropped. Only synthetic fixtures use this path.
	var collected metricdata.ResourceMetrics
	if err := api.metrics.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	contextMetrics := map[string]int64{}
	for _, scope := range collected.ScopeMetrics {
		for _, measurement := range scope.Metrics {
			if measurement.Name == core.MetricModelContextInputBytes || measurement.Name == core.MetricModelContextInputTokens || measurement.Name == core.MetricModelContextDroppedGroups {
				if sum, ok := measurement.Data.(metricdata.Sum[int64]); ok {
					for _, point := range sum.DataPoints {
						contextMetrics[measurement.Name] += point.Value
					}
				}
			}
		}
	}
	serialWriteAudit(t, runID, map[string]any{"session_id": session, "run_id": runID, "events": events, "spans": spans, "context_metrics_process_cumulative": contextMetrics})
	if contextMetrics[core.MetricModelContextInputBytes] <= 0 || contextMetrics[core.MetricModelContextInputTokens] <= 0 {
		t.Fatal("OTel reader did not collect model context cost")
	}
	var runSpan *serialSpanEvidence
	modelSpans, steps, users, assistants, ends := 0, 0, 0, 0, 0
	toolSpans := map[string]bool{}
	for i := range spans {
		span := &spans[i]
		if span.Attributes["session.id"] != session || span.DurationMS < 0 {
			t.Fatal("invalid trace correlation or duration")
		}
		switch span.Name {
		case core.SpanRunSegment:
			runSpan = span
		case core.SpanModelCall:
			modelSpans++
		case core.SpanToolCall:
			toolSpans[span.Attributes["call.id"]] = true
		}
	}
	if runSpan == nil {
		t.Fatal("missing run trace")
	}
	for _, span := range spans {
		if span.Name == core.SpanModelCall && (span.TraceID != runSpan.TraceID || span.ParentID != runSpan.SpanID) {
			t.Fatal("model span detached from run")
		}
		if span.Name == core.SpanToolCall {
			parentFound := false
			for _, parent := range spans {
				if parent.SpanID == span.ParentID && parent.TraceID == span.TraceID {
					parentFound = true
				}
			}
			if span.TraceID != runSpan.TraceID || !parentFound {
				t.Fatal("tool execution span detached from run or workflow parent")
			}
		}
	}
	calls := map[string]bool{}
	for _, event := range events {
		if event.RunID != runID {
			continue
		}
		switch event.Type {
		case core.EvStepStart:
			steps++
		case core.EvUserMessage:
			users++
		case core.EvAssistantMessage:
			assistants++
		case core.EvRunEnd:
			ends++
		case core.EvToolCall:
			var call core.ToolCallData
			if err := json.Unmarshal(event.Data, &call); err != nil {
				t.Fatal(err)
			}
			if calls[call.CallID] {
				t.Fatal("duplicate tool call in audit")
			}
			calls[call.CallID] = true
		case core.EvToolResult:
			var result core.ToolResultData
			if err := json.Unmarshal(event.Data, &result); err != nil {
				t.Fatal(err)
			}
			if !calls[result.CallID] || (result.OK && !toolSpans[result.CallID]) {
				t.Fatal("tool result missing its call or successful execution span")
			}
			delete(calls, result.CallID)
		}
	}
	if modelSpans != steps || users != 1 || assistants < 1 || ends != 1 || len(calls) != 0 {
		t.Fatalf("trace/history mismatch model_spans=%d steps=%d users=%d assistants=%d ends=%d pending_tools=%d", modelSpans, steps, users, assistants, ends, len(calls))
	}
	t.Logf("serial trace run=%s trace_id=%s model_spans=%d executed_tool_spans=%d history_audit=pass", runID, runSpan.TraceID, modelSpans, len(toolSpans))
}

func serialWriteAudit(t *testing.T, runID string, evidence any) {
	t.Helper()
	dir := os.Getenv("HARNESS_ACCEPTANCE_EVIDENCE_DIR")
	if dir == "" {
		return
	}
	encoded, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal(err)
	}
	var decoded any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	var sanitize func(any) any
	sanitize = func(value any) any {
		switch v := value.(type) {
		case map[string]any:
			for key, child := range v {
				if key == "continuation" {
					v[key] = "[REDACTED_PROTOCOL_STATE]"
				} else {
					v[key] = sanitize(child)
				}
			}
		case []any:
			for i := range v {
				v[i] = sanitize(v[i])
			}
		case string:
			for _, name := range []string{"HARNESS_LLM_API_KEY", "HARNESS_LLM_BASE_URL"} {
				if secret := os.Getenv(name); secret != "" {
					v = strings.ReplaceAll(v, secret, "[REDACTED]")
				}
			}
			return v
		}
		return value
	}
	encoded, err = json.MarshalIndent(sanitize(decoded), "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, runID+".json")
	if err := os.WriteFile(path, encoded, 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("serial audit artifact=%s", path)
}

// A fresh live workflow also proves the strengthened audit standard. Reading
// history again through a replacement API performs no extra model requests.
func serialTraceHistoryRestore(t *testing.T, model *serialAcceptanceModel) {
	serialProtectedWorkflow(t, model)
}

func TestPostgresSerialAuditHistoryRestore(t *testing.T) {
	t.Setenv("HARNESS_LLM_MODEL", "serial-offline-fixture")
	model := &serialAcceptanceModel{inner: serialWorkflowFixtureModel{}, test: t}
	serialTraceHistoryRestore(t, model)
}

func TestPostgresSerialSubagentAudit(t *testing.T) {
	t.Setenv("HARNESS_LLM_MODEL", "serial-offline-fixture")
	model := &serialAcceptanceModel{inner: serialSubagentFixtureModel{}, test: t}
	serialDurableSubagent(t, model)
}

type serialSubagentFixtureModel struct{}

func (serialSubagentFixtureModel) Provider() string { return "openai-compatible" }
func (serialSubagentFixtureModel) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	last := options.Messages[len(options.Messages)-1]
	if last.Role == core.RoleTool {
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: last.Content})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
		return nil
	}
	call := core.ToolCall{ID: "serial-child-call", Name: "serial.child_lookup", Args: map[string]any{"n": 1}}
	for _, tool := range options.Tools {
		if tool.Name == "serial.delegate" {
			call = core.ToolCall{ID: "serial-parent-call", Name: "serial.delegate", Args: map[string]any{"prompt": "retrieve the verification marker using your lookup tool"}}
		}
	}
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &call})
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
	return nil
}
