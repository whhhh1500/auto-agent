package core

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

type recordedTelemetrySpan struct {
	recorder *recordingTelemetry
	name     string
}

func (s recordedTelemetrySpan) End(err error, attributes TelemetryAttributes) {
	s.recorder.mu.Lock()
	defer s.recorder.mu.Unlock()
	s.recorder.ended = append(s.recorder.ended, s.name)
}

type recordingTelemetry struct {
	mu          sync.Mutex
	started     []string
	ended       []string
	counters    []string
	histograms  []string
	gauges      []string
	attrs       TelemetryAttributes
	spanAttrs   map[string][]TelemetryAttributes
	metricAttrs map[string][]TelemetryAttributes
	dropContext bool
}

func (r *recordingTelemetry) Start(ctx context.Context, operation string, attributes TelemetryAttributes) (context.Context, TelemetrySpan) {
	r.mu.Lock()
	r.started = append(r.started, operation)
	r.attrs = cloneTelemetryTestAttributes(attributes)
	if r.spanAttrs == nil {
		r.spanAttrs = map[string][]TelemetryAttributes{}
	}
	r.spanAttrs[operation] = append(r.spanAttrs[operation], cloneTelemetryTestAttributes(attributes))
	r.mu.Unlock()
	if r.dropContext {
		ctx = context.Background()
	}
	return ctx, recordedTelemetrySpan{recorder: r, name: operation}
}
func (r *recordingTelemetry) AddCounter(_ context.Context, name string, _ int64, attributes TelemetryAttributes) {
	r.mu.Lock()
	r.counters = append(r.counters, name)
	r.recordMetricAttributes(name, attributes)
	r.mu.Unlock()
}
func (r *recordingTelemetry) RecordHistogram(_ context.Context, name string, _ float64, _ string, attributes TelemetryAttributes) {
	r.mu.Lock()
	r.histograms = append(r.histograms, name)
	r.recordMetricAttributes(name, attributes)
	r.mu.Unlock()
}
func (r *recordingTelemetry) SetGauge(_ context.Context, name string, _ float64, _ string, attributes TelemetryAttributes) {
	r.mu.Lock()
	r.gauges = append(r.gauges, name)
	r.recordMetricAttributes(name, attributes)
	r.mu.Unlock()
}

func (r *recordingTelemetry) recordMetricAttributes(name string, attributes TelemetryAttributes) {
	if r.metricAttrs == nil {
		r.metricAttrs = map[string][]TelemetryAttributes{}
	}
	r.metricAttrs[name] = append(r.metricAttrs[name], cloneTelemetryTestAttributes(attributes))
}

func cloneTelemetryTestAttributes(attributes TelemetryAttributes) TelemetryAttributes {
	if attributes == nil {
		return nil
	}
	out := TelemetryAttributes{}
	for key, value := range attributes {
		out[key] = value
	}
	return out
}

type telemetryToolRuntime struct{}

func (telemetryToolRuntime) Schemas() []ToolSchema {
	return []ToolSchema{{Name: "telemetry.tool", Parameters: map[string]any{"type": "object"}}}
}
func (telemetryToolRuntime) Execute(context.Context, ToolCall) (CapabilityResult, error) {
	return CapabilityResult{Content: "ok", OK: true}, nil
}
func (telemetryToolRuntime) Authorized(string) bool { return true }
func (telemetryToolRuntime) MaxCallBudget() int     { return 4 }
func (telemetryToolRuntime) ManifestFor(string) (CapabilityManifest, bool) {
	return CapabilityManifest{ID: "telemetry.tool", Version: "1", Name: "Telemetry", Kind: KindTool, Tool: &ToolExposure{}}, true
}

type telemetryModel struct{ step int }

func (*telemetryModel) Provider() string { return "telemetry-model" }
func (m *telemetryModel) Stream(_ context.Context, options GenerateOptions, emit func(StreamChunk)) error {
	if m.step == 0 {
		m.step++
		call := ToolCall{ID: "call-telemetry", Name: "telemetry.tool", Args: map[string]any{}}
		emit(StreamChunk{Kind: StreamKindAssistant, ToolCall: &call})
		emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishToolCalls})
		return nil
	}
	emit(StreamChunk{Kind: StreamKindAssistant, Text: "done"})
	emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishStop})
	return nil
}

func TestAgentEmitsRuntimeTelemetry(t *testing.T) {
	recorder := &recordingTelemetry{}
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	scope, _ := user.Child(ScopeRef{Kind: ScopeSession, ID: "session-telemetry"})
	session, err := NewSession(SessionOptions{ID: "session-telemetry", ProfileID: "telemetry.agent", Principal: principal, Scope: scope})
	if err != nil {
		t.Fatal(err)
	}
	agent, err := NewAgent(AgentOptions{
		LLM: &telemetryModel{}, Tools: telemetryToolRuntime{}, Session: session,
		Provider: "telemetry", Model: "test", Telemetry: recorder,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := agent.RunTurn(context.Background(), TurnInput{RunID: "run-telemetry", Text: "go"})
	if err != nil || result.Status != RunCompleted {
		t.Fatalf("run failed: %#v err=%v", result, err)
	}
	for _, name := range []string{SpanRunSegment, SpanModelCall, SpanToolCall} {
		if !containsTelemetryName(recorder.started, name) || !containsTelemetryName(recorder.ended, name) {
			t.Fatalf("span %s missing: started=%#v ended=%#v", name, recorder.started, recorder.ended)
		}
	}
	for _, name := range []string{MetricRuns, MetricModelCalls, MetricToolCalls} {
		if !containsTelemetryName(recorder.counters, name) {
			t.Fatalf("counter %s missing: %#v", name, recorder.counters)
		}
	}
	for _, name := range []string{MetricRunDuration, MetricModelDuration, MetricToolDuration} {
		if !containsTelemetryName(recorder.histograms, name) {
			t.Fatalf("histogram %s missing: %#v", name, recorder.histograms)
		}
	}
}

func TestSpanContextAttributesPropagateWithoutMetricCardinality(t *testing.T) {
	recorder := &recordingTelemetry{dropContext: true}
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	scope, _ := user.Child(ScopeRef{Kind: ScopeSession, ID: "session-span-context"})
	session, err := NewSession(SessionOptions{
		ID: "session-span-context", ProfileID: "telemetry.agent", Principal: principal, Scope: scope,
	})
	if err != nil {
		t.Fatal(err)
	}
	agent, err := NewAgent(AgentOptions{
		LLM: &telemetryModel{}, Tools: telemetryToolRuntime{}, Session: session,
		Provider: "telemetry", Model: "test", Telemetry: recorder,
	})
	if err != nil {
		t.Fatal(err)
	}
	attributes := TelemetryAttributes{"harness.canary.id": "canary-span-context", "run.id": "spoofed"}
	ctx := WithTelemetrySpanAttributes(context.Background(), attributes)
	attributes["harness.canary.id"] = "mutated-after-attach"
	result, err := agent.RunTurn(ctx, TurnInput{RunID: "run-span-context", Text: "go"})
	if err != nil || result.Status != RunCompleted {
		t.Fatalf("run failed: %#v err=%v", result, err)
	}
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	for _, operation := range []string{SpanRunSegment, SpanModelCall, SpanToolCall} {
		starts := recorder.spanAttrs[operation]
		if len(starts) == 0 {
			t.Fatalf("span %s has no attributes: %#v", operation, recorder.spanAttrs)
		}
		for _, values := range starts {
			if values["harness.canary.id"] != "canary-span-context" {
				t.Fatalf("span %s lost inherited canary: %#v", operation, values)
			}
			if values["run.id"] != "run-span-context" {
				t.Fatalf("span %s allowed inherited run.id override: %#v", operation, values)
			}
		}
	}
	for name, observations := range recorder.metricAttrs {
		for _, values := range observations {
			if _, exists := values["harness.canary.id"]; exists {
				t.Fatalf("metric %s inherited high-cardinality canary id: %#v", name, values)
			}
		}
	}
}

func TestSpanContextAttributesAreBoundedAndDeterministic(t *testing.T) {
	attributes := TelemetryAttributes{}
	for index := 0; index < MaxTelemetrySpanContextAttributes+10; index++ {
		attributes[fmt.Sprintf("context.%02d", index)] = fmt.Sprintf("value-%02d", index)
	}
	ctx := WithTelemetrySpanAttributes(context.Background(), attributes)
	recorder := &recordingTelemetry{}
	StartTelemetry(recorder, ctx, SpanRunSegment, TelemetryAttributes{"local": "kept"})
	if len(recorder.attrs) != MaxTelemetrySpanContextAttributes+1 {
		t.Fatalf("span context bound wrong: %#v", recorder.attrs)
	}
	if recorder.attrs["local"] != "kept" {
		t.Fatalf("local attribute was displaced by inherited values: %#v", recorder.attrs)
	}
	for index := 0; index < MaxTelemetrySpanContextAttributes; index++ {
		key := fmt.Sprintf("context.%02d", index)
		if recorder.attrs[key] == "" {
			t.Fatalf("deterministic context attribute %s missing: %#v", key, recorder.attrs)
		}
	}
}

func TestSpanContextCorrelationSurvivesFullLocalAttributeSet(t *testing.T) {
	ctx := WithTelemetrySpanAttributes(context.Background(), TelemetryAttributes{
		"harness.canary.id": "canary-capacity", "run.id": "spoofed",
	})
	local := TelemetryAttributes{"run.id": "real-run"}
	for index := 0; index < MaxTelemetryAttributes+10; index++ {
		local[fmt.Sprintf("local.%02d", index)] = "value"
	}
	recorder := &recordingTelemetry{}
	StartTelemetry(recorder, ctx, SpanRunSegment, local)
	if len(recorder.attrs) != MaxTelemetryAttributes {
		t.Fatalf("span attribute bound=%d, want %d", len(recorder.attrs), MaxTelemetryAttributes)
	}
	if recorder.attrs["harness.canary.id"] != "canary-capacity" || recorder.attrs["run.id"] != "real-run" {
		t.Fatalf("correlation or local precedence lost at capacity: %#v", recorder.attrs)
	}
}

func containsTelemetryName(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

type panicTelemetry struct{}

func (panicTelemetry) Start(context.Context, string, TelemetryAttributes) (context.Context, TelemetrySpan) {
	panic("telemetry start")
}
func (panicTelemetry) AddCounter(context.Context, string, int64, TelemetryAttributes) {
	panic("telemetry counter")
}
func (panicTelemetry) RecordHistogram(context.Context, string, float64, string, TelemetryAttributes) {
	panic("telemetry histogram")
}
func (panicTelemetry) SetGauge(context.Context, string, float64, string, TelemetryAttributes) {
	panic("telemetry gauge")
}

func TestTelemetryPanicIsolationAndAttributeBounds(t *testing.T) {
	ctx, span := StartTelemetry(panicTelemetry{}, context.Background(), SpanRunSegment, TelemetryAttributes{"x": "y"})
	span.End(nil, nil)
	AddTelemetryCounter(panicTelemetry{}, ctx, MetricRuns, 1, nil)
	RecordTelemetryHistogram(panicTelemetry{}, ctx, MetricRunDuration, 1, "s", nil)
	SetTelemetryGauge(panicTelemetry{}, ctx, MetricQueueDepth, 1, "{run}", nil)

	recorder := &recordingTelemetry{}
	attributes := TelemetryAttributes{}
	for index := 0; index < MaxTelemetryAttributes+10; index++ {
		attributes[string(rune('a'+index%26))+string(rune('A'+index/26))] = string(make([]byte, MaxTelemetryAttributeValue+100))
	}
	_, boundedSpan := StartTelemetry(recorder, context.Background(), SpanRunSegment+"\x00bad", attributes)
	boundedSpan.End(nil, nil)
	if len(recorder.attrs) > MaxTelemetryAttributes {
		t.Fatalf("telemetry attributes were not bounded: %d", len(recorder.attrs))
	}
	for key, value := range recorder.attrs {
		if len(key) > MaxTelemetryAttributeKey || len(value) > MaxTelemetryAttributeValue {
			t.Fatalf("unbounded telemetry attribute %q=%d", key, len(value))
		}
	}
}
