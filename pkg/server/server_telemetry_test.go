package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/whhhh1500/auto-agent/pkg/control"
	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/evaluation"
	"github.com/whhhh1500/auto-agent/pkg/storage"
)

type serverTelemetrySpan struct {
	recorder *serverTelemetryRecorder
	name     string
}

type traceContextKey struct{}

type queueTraceTelemetry struct{ *serverTelemetryRecorder }

func (q queueTraceTelemetry) InjectTraceContext(context.Context) core.TelemetryTraceContext {
	return core.TelemetryTraceContext{
		TraceParent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		TraceState:  "vendor=value",
	}
}

func (q queueTraceTelemetry) ExtractTraceContext(ctx context.Context, traceContext core.TelemetryTraceContext) context.Context {
	if traceContext.TraceParent == "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" && traceContext.TraceState == "vendor=value" {
		return context.WithValue(ctx, traceContextKey{}, true)
	}
	return ctx
}

type traceAwareWorkerModel struct{ seen *atomic.Bool }

func (traceAwareWorkerModel) Provider() string { return "trace-aware" }
func (m traceAwareWorkerModel) Stream(ctx context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	if seen, _ := ctx.Value(traceContextKey{}).(bool); !seen {
		return errors.New("durable trace context missing")
	}
	m.seen.Store(true)
	return (core.MockLlmAdapter{}).Stream(ctx, options, emit)
}

func (s serverTelemetrySpan) End(error, core.TelemetryAttributes) {
	s.recorder.mu.Lock()
	s.recorder.spanEnds[s.name]++
	s.recorder.mu.Unlock()
}

type serverTelemetryRecorder struct {
	mu               sync.Mutex
	spanStarts       map[string]int
	spanEnds         map[string]int
	spanAttributes   map[string][]core.TelemetryAttributes
	metricAttributes map[string][]core.TelemetryAttributes
	counters         map[string]int64
	histograms       map[string]int
	gauges           map[string]int
}

func newServerTelemetryRecorder() *serverTelemetryRecorder {
	return &serverTelemetryRecorder{
		spanStarts: map[string]int{}, spanEnds: map[string]int{}, counters: map[string]int64{},
		spanAttributes: map[string][]core.TelemetryAttributes{}, metricAttributes: map[string][]core.TelemetryAttributes{},
		histograms: map[string]int{}, gauges: map[string]int{},
	}
}

func (r *serverTelemetryRecorder) Start(ctx context.Context, operation string, attributes core.TelemetryAttributes) (context.Context, core.TelemetrySpan) {
	r.mu.Lock()
	r.spanStarts[operation]++
	r.spanAttributes[operation] = append(r.spanAttributes[operation], cloneServerTelemetryAttributes(attributes))
	r.mu.Unlock()
	return ctx, serverTelemetrySpan{recorder: r, name: operation}
}
func (r *serverTelemetryRecorder) AddCounter(_ context.Context, name string, delta int64, attributes core.TelemetryAttributes) {
	r.mu.Lock()
	r.counters[name] += delta
	r.metricAttributes[name] = append(r.metricAttributes[name], cloneServerTelemetryAttributes(attributes))
	r.mu.Unlock()
}
func (r *serverTelemetryRecorder) RecordHistogram(_ context.Context, name string, _ float64, _ string, attributes core.TelemetryAttributes) {
	r.mu.Lock()
	r.histograms[name]++
	r.metricAttributes[name] = append(r.metricAttributes[name], cloneServerTelemetryAttributes(attributes))
	r.mu.Unlock()
}
func (r *serverTelemetryRecorder) SetGauge(_ context.Context, name string, _ float64, _ string, attributes core.TelemetryAttributes) {
	r.mu.Lock()
	r.gauges[name]++
	r.metricAttributes[name] = append(r.metricAttributes[name], cloneServerTelemetryAttributes(attributes))
	r.mu.Unlock()
}

func cloneServerTelemetryAttributes(attributes core.TelemetryAttributes) core.TelemetryAttributes {
	if attributes == nil {
		return nil
	}
	out := core.TelemetryAttributes{}
	for key, value := range attributes {
		out[key] = value
	}
	return out
}

func assertCanaryTraceOnly(t *testing.T, recorder *serverTelemetryRecorder, canaryID string, operations ...string) {
	t.Helper()
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	for _, operation := range operations {
		observations := recorder.spanAttributes[operation]
		if len(observations) == 0 {
			t.Fatalf("span %s was not recorded: %#v", operation, recorder.spanAttributes)
		}
		for _, attributes := range observations {
			if attributes[telemetryCanaryID] != canaryID {
				t.Fatalf("span %s lost canary id: %#v", operation, attributes)
			}
		}
	}
	for name, observations := range recorder.metricAttributes {
		for _, attributes := range observations {
			if _, exists := attributes[telemetryCanaryID]; exists {
				t.Fatalf("metric %s contains canary id: %#v", name, attributes)
			}
		}
	}
}

func stageTelemetryCanary(t *testing.T, fixture *runWorkerFixture, id string) control.CanaryRecord {
	record, _ := stageTelemetryCanaryWithManager(t, fixture, id)
	return record
}

func stageTelemetryCanaryWithManager(t *testing.T, fixture *runWorkerFixture, id string) (control.CanaryRecord, *control.CanaryManager) {
	t.Helper()
	segments := fixture.principal.Scope.Segments()
	product := core.MustScopePath(segments[:2]...)
	releases, err := control.NewReleaseManager(fixture.server.runtime.Profiles)
	if err != nil {
		t.Fatal(err)
	}
	store, err := storage.NewSQLCanaryStore(fixture.db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	canaries, err := control.NewCanaryManager(releases, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := canaries.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	description := "Telemetry canary"
	layer := core.AgentProfileLayer{
		Scope: product, ProfileID: fixture.session.ProfileID(), Description: &description,
	}
	revision, err := core.ProfileLayerRevision(layer)
	if err != nil {
		t.Fatal(err)
	}
	record, err := canaries.Stage(context.Background(), control.CanaryRecord{
		ID: id, ProfileID: layer.ProfileID, Scope: product, Layer: &layer,
		Revision: revision, BasisPoints: 10000, CandidateEvaluationRunID: "evaluation-" + id,
		Gate: evaluation.GateResult{Passed: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	fixture.server.Releases = releases
	fixture.server.canaries = canaries
	return record, canaries
}

func runCompositionSegments(t *testing.T, session *core.Session, runID string) map[core.SessionEventType][]map[string]string {
	t.Helper()
	out := map[core.SessionEventType][]map[string]string{}
	for _, event := range session.Events() {
		if event.RunID != runID || (event.Type != core.EvRunStart && event.Type != core.EvRunResume) {
			continue
		}
		var composition *core.RunCompositionData
		if event.Type == core.EvRunStart {
			var data core.RunStartData
			if err := json.Unmarshal(event.Data, &data); err != nil {
				t.Fatal(err)
			}
			composition = data.Composition
		} else {
			var data core.RunResumeData
			if err := json.Unmarshal(event.Data, &data); err != nil {
				t.Fatal(err)
			}
			composition = data.Composition
		}
		if composition == nil {
			t.Fatalf("%s has no composition", event.Type)
		}
		metadata := map[string]string{}
		for key, value := range composition.Metadata {
			metadata[key] = value
		}
		out[event.Type] = append(out[event.Type], metadata)
	}
	return out
}

func assertCanaryCompositionMetadata(t *testing.T, metadata map[string]string, assignment *control.CanaryAssignment) {
	t.Helper()
	if assignment == nil {
		t.Fatal("expected canary assignment")
	}
	expected := map[string]string{
		compositionCanaryID:           assignment.ID,
		compositionCanaryRevision:     assignment.Revision,
		compositionCanaryBaseRevision: assignment.BaseReleaseRevision,
		compositionCanaryBasisPoints:  strconv.Itoa(assignment.BasisPoints),
		compositionCanaryBucket:       strconv.Itoa(assignment.Bucket),
		compositionCanaryStatus:       string(assignment.Status),
		compositionCanaryCandidate:    strconv.FormatBool(assignment.Candidate),
	}
	for key, value := range expected {
		if metadata[key] != value {
			t.Fatalf("composition metadata %s=%q, want %q: %#v", key, metadata[key], value, metadata)
		}
	}
}

func TestSynchronousCanaryAddsTraceOnlyAttributes(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	record := stageTelemetryCanary(t, fixture, "canary-sync-telemetry")
	recorder := newServerTelemetryRecorder()
	fixture.server.telemetry = recorder
	fixture.server.runtime.Telemetry = recorder
	request := httptest.NewRequest(http.MethodPost,
		"/v1/sessions/"+fixture.session.ID()+"/runs", bytes.NewBufferString(`{"message":"trace canary"}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	fixture.server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Header().Get("X-Harness-Canary") != record.ID {
		t.Fatalf("sync canary status=%d header=%q body=%s", response.Code, response.Header().Get("X-Harness-Canary"), response.Body.String())
	}
	_, assignment := fixture.server.canaries.Assign(fixture.principal, fixture.session.ProfileID())
	loaded, err := fixture.sessions.Load(context.Background(), fixture.session.ID())
	if err != nil {
		t.Fatal(err)
	}
	segments := runCompositionSegments(t, loaded, loaded.Events()[0].RunID)
	if len(segments[core.EvRunStart]) != 1 {
		t.Fatalf("sync run/start segments=%#v", segments)
	}
	assertCanaryCompositionMetadata(t, segments[core.EvRunStart][0], assignment)
	assertCanaryTraceOnly(t, recorder, record.ID, core.SpanRunSegment, core.SpanModelCall)
}

func TestAsyncCanaryAddsTraceOnlyAttributes(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	record := stageTelemetryCanary(t, fixture, "canary-async-telemetry")
	recorder := newServerTelemetryRecorder()
	fixture.server.telemetry = recorder
	fixture.server.runtime.Telemetry = recorder
	run := enqueueRunHTTP(t, fixture, "trace async canary")
	claimed, err := fixture.server.RunWorkerOnce(context.Background(), "canary-telemetry-worker")
	if err != nil || !claimed {
		t.Fatalf("async canary worker failed: claimed=%t err=%v", claimed, err)
	}
	_, assignment := fixture.server.canaries.Assign(fixture.principal, fixture.session.ProfileID())
	loaded, err := fixture.sessions.Load(context.Background(), fixture.session.ID())
	if err != nil {
		t.Fatal(err)
	}
	segments := runCompositionSegments(t, loaded, run.RunID)
	if len(segments[core.EvRunStart]) != 1 {
		t.Fatalf("async run/start segments=%#v", segments)
	}
	assertCanaryCompositionMetadata(t, segments[core.EvRunStart][0], assignment)
	assertCanaryTraceOnly(t, recorder, record.ID, core.SpanRunSegment, core.SpanModelCall)
}

func TestCanaryApprovalResumeKeepsTraceOnlyAttributes(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	providerCalls := enableApprovalWorker(t, fixture)
	record := stageTelemetryCanary(t, fixture, "canary-resume-telemetry")
	recorder := newServerTelemetryRecorder()
	fixture.server.telemetry = recorder
	fixture.server.runtime.Telemetry = recorder
	run := enqueueRunHTTP(t, fixture, "approve canary trace")
	if claimed, err := fixture.server.RunWorkerOnce(context.Background(), "canary-approval-first"); err != nil || !claimed {
		t.Fatalf("first approval worker failed: claimed=%t err=%v", claimed, err)
	}
	approvals, err := fixture.approvals.ListApprovals(context.Background(), storage.ApprovalFilter{
		TenantID: fixture.principal.TenantID, Status: core.ApprovalPending,
	})
	if err != nil || len(approvals) != 1 || approvals[0].RunID != run.RunID {
		t.Fatalf("pending approval missing: %#v err=%v", approvals, err)
	}
	if _, changed, err := fixture.approvals.DecideApproval(context.Background(), approvals[0].ID, core.ApprovalApproved, "admin@acme"); err != nil || !changed {
		t.Fatalf("approval decision failed: changed=%t err=%v", changed, err)
	}
	if claimed, err := fixture.server.RunWorkerOnce(context.Background(), "canary-approval-resume"); err != nil || !claimed {
		t.Fatalf("resume approval worker failed: claimed=%t err=%v", claimed, err)
	}
	if providerCalls.Load() != 1 {
		t.Fatalf("provider calls=%d, want 1", providerCalls.Load())
	}
	_, assignment := fixture.server.canaries.Assign(fixture.principal, fixture.session.ProfileID())
	loaded, err := fixture.sessions.Load(context.Background(), fixture.session.ID())
	if err != nil {
		t.Fatal(err)
	}
	segments := runCompositionSegments(t, loaded, run.RunID)
	if len(segments[core.EvRunStart]) != 1 || len(segments[core.EvRunResume]) != 1 {
		t.Fatalf("approval composition segments=%#v", segments)
	}
	assertCanaryCompositionMetadata(t, segments[core.EvRunStart][0], assignment)
	assertCanaryCompositionMetadata(t, segments[core.EvRunResume][0], assignment)
	assertCanaryTraceOnly(t, recorder, record.ID, core.SpanRunSegment, core.SpanModelCall, core.SpanToolCall)
}

func TestCanaryBucketMissPersistsLiveAssignment(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	var record control.CanaryRecord
	var manager *control.CanaryManager
	var assignment *control.CanaryAssignment
	for index := 0; index < 10; index++ {
		record, manager = stageTelemetryCanaryWithManager(t, fixture, fmt.Sprintf("canary-bucket-miss-%d", index))
		_, assignment = manager.Assign(fixture.principal, fixture.session.ProfileID())
		if assignment != nil && assignment.Bucket > 0 {
			break
		}
		if _, err := manager.Rollback(context.Background(), record.ID); err != nil {
			t.Fatal(err)
		}
		assignment = nil
	}
	if assignment == nil {
		t.Fatal("could not construct a non-zero deterministic canary bucket")
	}
	if _, err := manager.SetBasisPoints(context.Background(), record.ID, assignment.Bucket); err != nil {
		t.Fatal(err)
	}
	_, assignment = manager.Assign(fixture.principal, fixture.session.ProfileID())
	if assignment == nil || assignment.Candidate {
		t.Fatalf("expected a live bucket miss: %#v", assignment)
	}
	request := httptest.NewRequest(http.MethodPost,
		"/v1/sessions/"+fixture.session.ID()+"/runs", bytes.NewBufferString(`{"message":"live bucket"}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	fixture.server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Header().Get("X-Harness-Canary") != "" {
		t.Fatalf("bucket miss status=%d header=%q body=%s", response.Code, response.Header().Get("X-Harness-Canary"), response.Body.String())
	}
	loaded, err := fixture.sessions.Load(context.Background(), fixture.session.ID())
	if err != nil {
		t.Fatal(err)
	}
	segments := runCompositionSegments(t, loaded, loaded.Events()[0].RunID)
	assertCanaryCompositionMetadata(t, segments[core.EvRunStart][0], assignment)
}

func TestCanaryApprovalResumePersistsPausedLiveAssignment(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	providerCalls := enableApprovalWorker(t, fixture)
	record, manager := stageTelemetryCanaryWithManager(t, fixture, "canary-resume-paused-evidence")
	run := enqueueRunHTTP(t, fixture, "pause before resume")
	if claimed, err := fixture.server.RunWorkerOnce(context.Background(), "paused-evidence-first"); err != nil || !claimed {
		t.Fatalf("first worker failed: claimed=%t err=%v", claimed, err)
	}
	_, initialAssignment := manager.Assign(fixture.principal, fixture.session.ProfileID())
	if initialAssignment == nil || !initialAssignment.Candidate {
		t.Fatalf("initial assignment is not candidate: %#v", initialAssignment)
	}
	if _, err := manager.Pause(context.Background(), record.ID); err != nil {
		t.Fatal(err)
	}
	_, resumedAssignment := manager.Assign(fixture.principal, fixture.session.ProfileID())
	if resumedAssignment == nil || resumedAssignment.Candidate || resumedAssignment.Status != control.CanaryPaused {
		t.Fatalf("paused assignment wrong: %#v", resumedAssignment)
	}
	approvals, err := fixture.approvals.ListApprovals(context.Background(), storage.ApprovalFilter{
		TenantID: fixture.principal.TenantID, Status: core.ApprovalPending,
	})
	if err != nil || len(approvals) != 1 {
		t.Fatalf("pending approval missing: %#v err=%v", approvals, err)
	}
	if _, changed, err := fixture.approvals.DecideApproval(context.Background(), approvals[0].ID, core.ApprovalApproved, "admin@acme"); err != nil || !changed {
		t.Fatalf("approval decision failed: changed=%t err=%v", changed, err)
	}
	if claimed, err := fixture.server.RunWorkerOnce(context.Background(), "paused-evidence-resume"); err != nil || !claimed {
		t.Fatalf("resume worker failed: claimed=%t err=%v", claimed, err)
	}
	if providerCalls.Load() != 1 {
		t.Fatalf("provider calls=%d, want 1", providerCalls.Load())
	}
	loaded, err := fixture.sessions.Load(context.Background(), fixture.session.ID())
	if err != nil {
		t.Fatal(err)
	}
	segments := runCompositionSegments(t, loaded, run.RunID)
	if len(segments[core.EvRunStart]) != 1 || len(segments[core.EvRunResume]) != 1 {
		t.Fatalf("paused resume composition segments=%#v", segments)
	}
	assertCanaryCompositionMetadata(t, segments[core.EvRunStart][0], initialAssignment)
	assertCanaryCompositionMetadata(t, segments[core.EvRunResume][0], resumedAssignment)
}

func TestServerEmitsQueueSubmissionAndRuntimeTelemetry(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	recorder := newServerTelemetryRecorder()
	fixture.server.telemetry = recorder
	fixture.server.runtime.Telemetry = recorder
	first := enqueueRunRecorder(t, fixture, "telemetry work", "telemetry-submit-key")
	if first.Code != 202 {
		t.Fatalf("submit status=%d", first.Code)
	}
	replay := enqueueRunRecorder(t, fixture, "telemetry work", "telemetry-submit-key")
	if replay.Code != 202 {
		t.Fatalf("replay status=%d", replay.Code)
	}
	claimed, err := fixture.server.RunWorkerOnce(context.Background(), "telemetry-worker")
	if err != nil || !claimed {
		t.Fatalf("worker failed: claimed=%t err=%v", claimed, err)
	}
	fixture.server.observeOperationalMetrics(context.Background())
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if recorder.counters[core.MetricSubmissions] != 2 {
		t.Fatalf("submission counter=%d", recorder.counters[core.MetricSubmissions])
	}
	for _, name := range []string{core.MetricQueueClaims, core.MetricRuns, core.MetricModelCalls} {
		if recorder.counters[name] == 0 {
			t.Fatalf("counter %s missing: %#v", name, recorder.counters)
		}
	}
	for _, name := range []string{core.MetricQueueClaimDuration, core.MetricRunDuration, core.MetricModelDuration} {
		if recorder.histograms[name] == 0 {
			t.Fatalf("histogram %s missing: %#v", name, recorder.histograms)
		}
	}
	for _, name := range []string{core.MetricQueueDepth, core.MetricQueueOldestAge, core.MetricApprovalPending} {
		if recorder.gauges[name] == 0 {
			t.Fatalf("gauge %s missing: %#v", name, recorder.gauges)
		}
	}
	for _, name := range []string{core.SpanQueueClaim, core.SpanRunSegment, core.SpanModelCall} {
		if recorder.spanStarts[name] == 0 || recorder.spanStarts[name] != recorder.spanEnds[name] {
			t.Fatalf("span %s mismatch: starts=%#v ends=%#v", name, recorder.spanStarts, recorder.spanEnds)
		}
	}
}

func TestEmptyQueueClaimDoesNotCreateSpan(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	recorder := newServerTelemetryRecorder()
	fixture.server.telemetry = recorder
	claimed, err := fixture.server.RunWorkerOnce(context.Background(), "empty-worker")
	if err != nil || claimed {
		t.Fatalf("empty claim wrong: claimed=%t err=%v", claimed, err)
	}
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if recorder.spanStarts[core.SpanQueueClaim] != 0 {
		t.Fatalf("empty queue emitted claim spans: %#v", recorder.spanStarts)
	}
	if recorder.counters[core.MetricQueueClaims] != 1 || recorder.histograms[core.MetricQueueClaimDuration] != 1 {
		t.Fatalf("empty claim metrics missing: counters=%#v histograms=%#v", recorder.counters, recorder.histograms)
	}
}

func TestAsyncQueuePersistsAndRestoresTraceContext(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	recorder := queueTraceTelemetry{serverTelemetryRecorder: newServerTelemetryRecorder()}
	fixture.server.telemetry = recorder
	fixture.server.runtime.Telemetry = recorder
	var seen atomic.Bool
	fixture.server.runtime.Models = core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) {
		return traceAwareWorkerModel{seen: &seen}, nil
	})
	response := enqueueRunRecorder(t, fixture, "trace work", "trace-submit-key")
	if response.Code != 202 {
		t.Fatalf("submit status=%d body=%s", response.Code, response.Body.String())
	}
	var traceParent, traceState string
	if err := fixture.db.QueryRow("SELECT trace_parent, trace_state FROM run_queue").Scan(&traceParent, &traceState); err != nil {
		t.Fatal(err)
	}
	if traceParent == "" || traceState != "vendor=value" {
		t.Fatalf("trace carrier was not persisted: parent=%q state=%q", traceParent, traceState)
	}
	claimed, err := fixture.server.RunWorkerOnce(context.Background(), "trace-worker")
	if err != nil || !claimed || !seen.Load() {
		t.Fatalf("trace context was not restored: claimed=%t seen=%t err=%v", claimed, seen.Load(), err)
	}
}
