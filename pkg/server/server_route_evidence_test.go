package server

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/whhhh1500/auto-agent/internal/executionroute"
	"github.com/whhhh1500/auto-agent/pkg/app/programmatic"
	"github.com/whhhh1500/auto-agent/pkg/core"
)

type routeEvidenceTelemetry struct {
	operations []string
	attributes []core.TelemetryAttributes
}

type routeEvidenceTelemetrySpan struct{}

func (routeEvidenceTelemetrySpan) End(error, core.TelemetryAttributes) {}

func (r *routeEvidenceTelemetry) Start(ctx context.Context, operation string, attributes core.TelemetryAttributes) (context.Context, core.TelemetrySpan) {
	r.operations = append(r.operations, operation)
	copyOf := core.TelemetryAttributes{}
	for key, value := range attributes {
		copyOf[key] = value
	}
	r.attributes = append(r.attributes, copyOf)
	return ctx, routeEvidenceTelemetrySpan{}
}

func (*routeEvidenceTelemetry) AddCounter(context.Context, string, int64, core.TelemetryAttributes) {}
func (*routeEvidenceTelemetry) RecordHistogram(context.Context, string, float64, string, core.TelemetryAttributes) {
}
func (*routeEvidenceTelemetry) SetGauge(context.Context, string, float64, string, core.TelemetryAttributes) {
}

func TestRouteEvidenceAttributesAreBoundedContentFreeFacts(t *testing.T) {
	evidence := executionroute.RouteEvidenceProjection{
		Identity:   executionroute.RouteIdentityEvidence{Status: executionroute.EvidenceStatusVerified, Version: executionroute.RouteProbeVersion, Mode: "auto_probe_once", Implementation: executionroute.RouteProbeImplementationRevision},
		Selection:  executionroute.SelectedRouteObservation{Route: executionroute.SelectedRoutePTC, Status: executionroute.SelectionStatusVerified},
		Coverage:   executionroute.RouteCoverageEvidence{Status: executionroute.CoverageStatusComplete, CandidateCount: 8, SelectedTargetCount: 8, ExecutedTargetCount: 8, ExactOnce: true},
		Effects:    executionroute.RouteEffectsEvidence{JournalStatus: executionroute.EvidenceStatusVerified, CompletedJournalInvocations: 10, ExternalStatus: executionroute.EvidenceStatusUnavailable},
		Accounting: executionroute.RouteAccountingEvidence{LedgerStatus: executionroute.EvidenceStatusVerified, ModelCallCount: 3, InputTokens: 12293, OutputTokens: 234},
		Context:    executionroute.RouteContextEvidence{ArchiveStatus: executionroute.EvidenceStatusVerified, AssemblyStatus: executionroute.EvidenceStatusUnavailable},
	}
	attributes := routeEvidenceAttributes("session-safe", "run-safe", core.RunCompleted, evidence)

	want := map[string]string{
		"run.id": "run-safe", "session.id": "session-safe", "run.status": "completed",
		"programmatic.route.identity.status": "verified",
		"programmatic.route.version":         "2", "programmatic.route.mode": "auto_probe_once",
		"programmatic.route.selection": "ptc", "programmatic.route.selection.status": "verified",
		"programmatic.route.coverage.status": "complete", "programmatic.route.coverage.candidates": "8",
		"programmatic.route.coverage.selected": "8", "programmatic.route.coverage.executed": "8",
		"programmatic.route.coverage.duplicate": "false", "programmatic.route.coverage.exact_once": "true",
		"programmatic.route.effects.journal.status": "verified", "programmatic.route.effects.journal.completed": "10",
		"programmatic.route.effects.external.status": "unavailable", "programmatic.route.accounting.status": "verified",
		"programmatic.route.accounting.model_calls": "3", "programmatic.route.context.archive.status": "verified",
		"programmatic.route.context.assembly.status": "unavailable", "programmatic.route.context.summary_events": "0",
		"programmatic.route.implementation": executionroute.RouteProbeImplementationRevision,
	}
	if !reflect.DeepEqual(map[string]string(attributes), want) {
		t.Fatalf("route evidence attributes=%#v want=%#v", attributes, want)
	}
	for key, value := range attributes {
		joined := strings.ToLower(key + "=" + value)
		for _, forbidden := range []string{"prompt", "answer", "argument", "result", "credential", "api_key", "secret"} {
			if strings.Contains(joined, forbidden) {
				t.Fatalf("route evidence exported forbidden content key/value %q", joined)
			}
		}
	}
}

func TestObserveTerminalRouteEvidenceSkipsUnsupportedRun(t *testing.T) {
	telemetry := &routeEvidenceTelemetry{}
	server := &Server{telemetry: telemetry}
	principalScope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeProduct, ID: "route-evidence"})
	sessionScope, err := principalScope.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "route-evidence-session"})
	if err != nil {
		t.Fatal(err)
	}
	session, err := core.NewSession(core.SessionOptions{
		ID: "route-evidence-session", ProfileID: "route.evidence", Scope: sessionScope,
		Principal: core.Principal{TenantID: "tenant", SubjectID: "subject", Scope: principalScope},
	})
	if err != nil {
		t.Fatal(err)
	}
	server.observeTerminalRouteEvidence(context.Background(), &core.Runtime{}, session, session.Principal(), "route-evidence-run", core.RunCompleted)
	if len(telemetry.operations) != 0 || len(telemetry.attributes) != 0 {
		t.Fatalf("unsupported run emitted route evidence: operations=%#v attributes=%#v", telemetry.operations, telemetry.attributes)
	}
}

type deadlineRouteEvidenceReader struct {
	countingToolJournal
	hadDeadline bool
	initialErr  error
	finalErr    error
}

func (r *deadlineRouteEvidenceReader) GetToolInvocation(ctx context.Context, _ core.ToolInvocation) (core.ToolInvocationRecord, bool, error) {
	_, r.hadDeadline = ctx.Deadline()
	r.initialErr = ctx.Err()
	<-ctx.Done()
	r.finalErr = ctx.Err()
	return core.ToolInvocationRecord{}, false, r.finalErr
}

func TestObserveTerminalRouteEvidenceReaderGetsIndependentDeadline(t *testing.T) {
	session, principal, runID := routeEvidenceProbeReceiptFixture(t)
	if isProbe, err := executionroute.IsNeutralProbe(session, principal, runID, "evidence.inventory"); err != nil || !isProbe {
		t.Fatalf("fixture neutral probe=%t err=%v", isProbe, err)
	}
	telemetry := &routeEvidenceTelemetry{}
	server := &Server{telemetry: telemetry}
	reader := &deadlineRouteEvidenceReader{}
	runtime := &core.Runtime{ToolJournal: reader}

	caller, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()
	server.observeTerminalRouteEvidence(caller, runtime, session, principal, runID, core.RunCompleted)
	if elapsed := time.Since(started); elapsed < routeEvidenceObservationTimeout/2 || elapsed > routeEvidenceObservationTimeout+2*time.Second {
		t.Fatalf("reader observation duration=%s, want bounded by the independent timeout", elapsed)
	}
	if !reader.hadDeadline || reader.initialErr != nil || reader.finalErr != context.DeadlineExceeded {
		t.Fatalf("reader context deadline=%t initial=%v final=%v", reader.hadDeadline, reader.initialErr, reader.finalErr)
	}
	// A verified frozen v2 identity intentionally remains reportable even when
	// staged Journal evidence timed out. The span therefore records identity
	// only; selection, coverage, and effects remain unavailable.
	if len(telemetry.operations) != 1 || telemetry.operations[0] != routeEvidenceSpan {
		t.Fatalf("identity evidence span operations=%v", telemetry.operations)
	}
	if got := telemetry.attributes[0]["programmatic.route.selection.status"]; got != string(executionroute.SelectionStatusUnavailable) {
		t.Fatalf("timed out reader selection status=%q", got)
	}
}

func TestObserveTerminalRouteEvidenceSurfacesExpectedV2WithUnavailableIdentity(t *testing.T) {
	session, principal, runID := routeEvidenceProbeReceiptFixture(t)
	telemetry := &routeEvidenceTelemetry{}
	server := &Server{telemetry: telemetry}
	wrongPrincipal := principal
	wrongPrincipal.SubjectID = "other-subject"

	server.observeTerminalRouteEvidence(context.Background(), &core.Runtime{}, session, wrongPrincipal, runID, core.RunFailed)

	if len(telemetry.operations) != 1 || telemetry.operations[0] != routeEvidenceSpan || len(telemetry.attributes) != 1 {
		t.Fatalf("unavailable evidence operations=%v attributes=%v", telemetry.operations, telemetry.attributes)
	}
	want := map[string]string{
		"run.id": runID, "session.id": session.ID(), "run.status": string(core.RunFailed),
		"programmatic.route.identity.status": string(executionroute.EvidenceStatusUnavailable),
	}
	if !reflect.DeepEqual(map[string]string(telemetry.attributes[0]), want) {
		t.Fatalf("unavailable evidence attributes=%v want=%v", telemetry.attributes[0], want)
	}
}

// routeEvidenceProbeReceiptFixture contains only the frozen v2 composition
// and one completed neutral-probe receipt. It is sufficient to drive the
// reader branch without a model, a server, or any external tool effect.
func routeEvidenceProbeReceiptFixture(t *testing.T) (*core.Session, core.Principal, string) {
	t.Helper()
	const runID = "route-evidence-deadline"
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product, err := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "route-evidence"})
	if err != nil {
		t.Fatal(err)
	}
	sessionScope, err := product.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "route-evidence-deadline-session"})
	if err != nil {
		t.Fatal(err)
	}
	principal := core.Principal{TenantID: "tenant", SubjectID: "subject", Scope: product, Grants: core.NewPermissionSet()}
	session, err := core.NewSession(core.SessionOptions{ID: "route-evidence-deadline-session", ProfileID: "route.evidence", Scope: sessionScope, Principal: principal})
	if err != nil {
		t.Fatal(err)
	}
	objectSchema := map[string]any{"type": "object", "additionalProperties": false}
	composition := core.RunCompositionData{
		Profile: core.AgentProfileSnapshot{ID: "fixture-profile", ProfileID: "route.evidence", Scope: product, Model: core.ModelSelection{Provider: "fixture", Model: "fixture"}},
		Capabilities: []core.SnapshotCapability{
			{Source: product, ProviderRevision: "probe/v1", Manifest: core.CapabilityManifest{
				ID: "evidence.inventory", Version: "1", Kind: core.KindTool, Idempotent: true, MaxOutputBytes: 4096,
				Tool: &core.ToolExposure{Parameters: objectSchema}, Metadata: map[string]string{programmatic.ProbeManifestKey: programmatic.ProbeManifestVersion},
			}},
			{Source: product, ProviderRevision: "detail/v1", Manifest: core.CapabilityManifest{
				ID: "evidence.detail", Version: "1", Description: "Read an approved record", Kind: core.KindTool, Idempotent: true,
				InputSchema: map[string]any{"type": "object", "additionalProperties": false, "required": []any{"id"}, "properties": map[string]any{"id": map[string]any{"type": "string"}}},
				Tool:        &core.ToolExposure{Description: "Read an approved record", Parameters: map[string]any{"type": "object", "additionalProperties": false, "required": []any{"id"}, "properties": map[string]any{"id": map[string]any{"type": "string"}}}},
				Metadata:    map[string]string{programmatic.ExposureKey: programmatic.ExposureVersion},
			}},
			{Source: product, ProviderRevision: "execute/v1", Manifest: core.CapabilityManifest{ID: programmatic.DefaultExecuteToolID, Version: "1", Kind: core.KindTool, Tool: &core.ToolExposure{Parameters: objectSchema}}},
		},
		Model: core.ModelSelection{Provider: "fixture", Model: "fixture"}, ResolvedProvider: "fixture", ModelRevision: "fixture/v1",
		Metadata: map[string]string{
			executionroute.RouteVersionKey:        executionroute.RouteProbeVersion,
			executionroute.RouteModeKey:           string(programmatic.RouteAutoProbeOnce),
			executionroute.RouteCatalogToolIDKey:  programmatic.DefaultCatalogToolID,
			executionroute.RouteExecuteToolIDKey:  programmatic.DefaultExecuteToolID,
			executionroute.RouteImplementationKey: executionroute.RouteProbeImplementationRevision,
		},
	}
	compositionRevision, err := core.CompositionRevision(&composition)
	if err != nil {
		t.Fatal(err)
	}
	assignmentRevision, err := core.CompositionMetadataRevision(composition.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append(runID, core.EvRunStart, core.RunStartData{Composition: &composition, CompositionRevision: compositionRevision, AssignmentRevision: assignmentRevision}); err != nil {
		t.Fatal(err)
	}
	call := core.ToolCall{ID: "probe-call", Name: "evidence.inventory", Args: map[string]any{}}
	if _, err := session.Append(runID, core.EvAssistantMessage, core.AssistantMessageData{ToolCall: &call}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append(runID, core.EvToolCall, core.ToolCallData{CallID: call.ID, Name: call.Name, Args: call.Args}); err != nil {
		t.Fatal(err)
	}
	result := core.ToolResultData{CallID: call.ID, OK: true, Content: "inventory complete", Metadata: map[string]any{
		programmatic.ProbeFactsMetadataKey: map[string]any{
			"version": programmatic.ProbeFactsVersion, "followup_capability_id": "evidence.detail", "max_model_return_bytes": float64(4096),
			"candidates": []any{map[string]any{"label": "record-1", "args": map[string]any{"id": "record-1"}, "facts": map[string]any{"active": true}}},
		},
	}}
	if _, err := session.Append(runID, core.EvToolResult, result); err != nil {
		t.Fatal(err)
	}
	return session, principal, runID
}
