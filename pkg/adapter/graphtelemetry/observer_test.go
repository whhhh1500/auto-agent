package graphtelemetry

import (
	"context"
	"strings"
	"testing"
	"time"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
	execgraph "github.com/cc-auto-agent/harness-core/pkg/execution/graph"
)

type telemetryCapture struct {
	names []string
	attrs []core.TelemetryAttributes
	panic bool
}
type captureSpan struct{}

func (captureSpan) End(error, core.TelemetryAttributes) {}
func (t *telemetryCapture) Start(ctx context.Context, _ string, _ core.TelemetryAttributes) (context.Context, core.TelemetrySpan) {
	return ctx, captureSpan{}
}
func (t *telemetryCapture) AddCounter(_ context.Context, name string, _ int64, a core.TelemetryAttributes) {
	if t.panic {
		panic("secret telemetry")
	}
	t.names = append(t.names, name)
	t.attrs = append(t.attrs, a)
}
func (t *telemetryCapture) RecordHistogram(_ context.Context, name string, _ float64, _ string, a core.TelemetryAttributes) {
	if t.panic {
		panic("secret telemetry")
	}
	t.names = append(t.names, name)
	t.attrs = append(t.attrs, a)
}
func (t *telemetryCapture) SetGauge(context.Context, string, float64, string, core.TelemetryAttributes) {
}
func TestObserverBoundsAndDoesNotLeakIdentity(t *testing.T) {
	cap := &telemetryCapture{}
	err := (Observer{Telemetry: cap}).Observe(context.Background(), execgraph.Event{Kind: execgraph.EventNodeEnd, GraphID: "graph-secret", DefinitionRevision: "def-secret", NodeID: "node-secret", NodeKind: "plugin-secret", OutcomeCode: "secret-outcome", TenantID: "tenant-secret", SessionID: "session-secret", RunID: "run-secret", SegmentID: "segment-secret", AttemptID: "attempt-secret", ContextPlanRevision: "plan-secret", Duration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range cap.names {
		if strings.Contains(n, "secret") {
			t.Fatal(n)
		}
	}
	for _, a := range cap.attrs {
		for _, v := range a {
			if strings.Contains(v, "secret") || v == "plugin-secret" || v == "secret-outcome" {
				t.Fatalf("leaked attrs=%v", a)
			}
		}
	}
	if cap.attrs[0]["graph.event"] != "node_end" || cap.attrs[0]["graph.node_kind"] != "unknown" || cap.attrs[0]["graph.outcome"] != "unknown" {
		t.Fatalf("attrs=%v", cap.attrs[0])
	}
}
func TestObserverPanicAndNilNoDuration(t *testing.T) {
	if err := (Observer{}).Observe(context.Background(), execgraph.Event{}); err != nil {
		t.Fatal(err)
	}
	cap := &telemetryCapture{panic: true}
	if err := (Observer{Telemetry: cap}).Observe(context.Background(), execgraph.Event{Kind: execgraph.EventStarted}); err != nil {
		t.Fatalf("panic escaped=%v", err)
	}
	before := len(cap.names)
	cap.panic = false
	if err := (Observer{Telemetry: cap}).Observe(context.Background(), execgraph.Event{Kind: execgraph.EventNodeEnd}); err != nil {
		t.Fatalf("nil duration failed=%v", err)
	}
	if len(cap.names) != before+1 {
		t.Fatalf("duration=0 emitted unexpected histogram: before=%d after=%d", before, len(cap.names))
	}
}

func TestBoundedOutcomeAllowsEngineCodes(t *testing.T) {
	valid := []string{"created", "node_started", "approval_pending", "retry", "error_edge", "edge_selected", "completed", "cancelled", "recovery_unknown", "interrupted_unknown", "node_timeout", "node_failed", "preflight_failed", "binding_missing", "lease_unavailable", "sandbox_unavailable", "sandbox_denied", "context_unavailable", "invalid_approval", "reducer_failed", "edge_selection_failed", "step_limit", "visit_limit", "unknown_node", "approval_approved", "approval_denied", "unknown"}
	for _, value := range valid {
		if got := string(execgraph.NormalizeOutcomeCode(execgraph.OutcomeCode(value))); got != value {
			t.Errorf("boundedOutcome(%q)=%q", value, got)
		}
	}
	for _, value := range []string{"ok", "arbitrary-secret", "node_error"} {
		if got := string(execgraph.NormalizeOutcomeCode(execgraph.OutcomeCode(value))); got != "unknown" {
			t.Errorf("boundedOutcome(%q)=%q, want unknown", value, got)
		}
	}
}
