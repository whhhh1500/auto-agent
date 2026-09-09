package executionroute

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/whhhh1500/auto-agent/pkg/app/programmatic"
	"github.com/whhhh1500/auto-agent/pkg/core"
)

func TestObserveRouteEvidenceProjectsVerifiedDirectWithoutOverclaimingCoverageOrEffects(t *testing.T) {
	fixture := newV2RuntimeFixture(t, v2RuntimeDirect, false, false)
	result, err := fixture.run(t, "evidence-direct")
	if err != nil || result.Status != core.RunCompleted {
		t.Fatalf("run result=%#v err=%v", result, err)
	}

	got := ObserveRouteEvidence(context.Background(), fixture.session, fixture.principal, fixture.journal, "evidence-direct")
	if got.Identity != verifiedV2RouteIdentity() {
		t.Fatalf("identity=%+v", got.Identity)
	}
	if got.Selection != (SelectedRouteObservation{Route: SelectedRouteDirect, Status: SelectionStatusVerified}) {
		t.Fatalf("selection=%+v", got.Selection)
	}
	wantCoverage := RouteCoverageEvidence{Status: CoverageStatusComplete, CandidateCount: 2, SelectedTargetCount: 2, ExecutedTargetCount: 2, ExactOnce: true}
	if got.Coverage != wantCoverage {
		t.Fatalf("coverage=%+v want=%+v", got.Coverage, wantCoverage)
	}
	if got.Effects != (RouteEffectsEvidence{JournalStatus: EvidenceStatusVerified, CompletedJournalInvocations: 3, ExternalStatus: EvidenceStatusUnavailable}) {
		t.Fatalf("effects=%+v", got.Effects)
	}
	if got.Accounting != (RouteAccountingEvidence{LedgerStatus: EvidenceStatusVerified, ModelCallCount: 3}) {
		t.Fatalf("accounting=%+v", got.Accounting)
	}
	if got.Context != (RouteContextEvidence{ArchiveStatus: EvidenceStatusVerified, AssemblyStatus: EvidenceStatusUnavailable}) {
		t.Fatalf("context=%+v", got.Context)
	}
}

func TestObserveRouteEvidenceProjectsVerifiedPTCSelectedTargetCoverage(t *testing.T) {
	fixture := newV2RuntimeFixture(t, v2RuntimePTC, false, false)
	result, err := fixture.run(t, "evidence-ptc")
	if err != nil || result.Status != core.RunCompleted {
		t.Fatalf("run result=%#v err=%v", result, err)
	}

	got := ObserveRouteEvidence(context.Background(), fixture.session, fixture.principal, fixture.journal, "evidence-ptc")
	if got.Selection != (SelectedRouteObservation{Route: SelectedRoutePTC, Status: SelectionStatusVerified}) {
		t.Fatalf("selection=%+v", got.Selection)
	}
	wantCoverage := RouteCoverageEvidence{Status: CoverageStatusComplete, CandidateCount: 2, SelectedTargetCount: 2, ExecutedTargetCount: 2, ExactOnce: true}
	if got.Coverage != wantCoverage {
		t.Fatalf("coverage=%+v want=%+v", got.Coverage, wantCoverage)
	}
	if got.Effects != (RouteEffectsEvidence{JournalStatus: EvidenceStatusVerified, CompletedJournalInvocations: 4, ExternalStatus: EvidenceStatusUnavailable}) {
		t.Fatalf("effects=%+v", got.Effects)
	}
}

func TestObserveRouteEvidenceDoesNotClaimPTCCandidateCompleteness(t *testing.T) {
	fixture := newV2RuntimeFixture(t, v2RuntimePTC, false, false)
	fixture.model.choice = []core.ToolCall{{ID: "execute-one", Name: programmatic.DefaultExecuteToolID, Args: map[string]any{
		"projection": "name", "selection": []any{float64(0)},
	}}}
	result, err := fixture.run(t, "evidence-ptc-one")
	if err != nil || result.Status != core.RunCompleted {
		t.Fatalf("run result=%#v err=%v", result, err)
	}

	got := ObserveRouteEvidence(context.Background(), fixture.session, fixture.principal, fixture.journal, "evidence-ptc-one")
	want := RouteCoverageEvidence{Status: CoverageStatusIncomplete, CandidateCount: 2, SelectedTargetCount: 1, ExecutedTargetCount: 1, ExactOnce: true}
	if got.Coverage != want {
		t.Fatalf("coverage=%+v want=%+v", got.Coverage, want)
	}
}

func TestObserveRouteEvidenceMarksPartialDirectCandidateCoverageIncomplete(t *testing.T) {
	fixture := newV2RuntimeFixture(t, v2RuntimeDirect, false, false)
	fixture.model.choice = []core.ToolCall{{ID: "detail-one", Name: "records.detail", Args: map[string]any{"id": "record-17"}}}
	result, err := fixture.run(t, "evidence-direct-one")
	if err != nil || result.Status != core.RunCompleted {
		t.Fatalf("run result=%#v err=%v", result, err)
	}

	got := ObserveRouteEvidence(context.Background(), fixture.session, fixture.principal, fixture.journal, "evidence-direct-one")
	want := RouteCoverageEvidence{Status: CoverageStatusIncomplete, CandidateCount: 2, SelectedTargetCount: 1, ExecutedTargetCount: 1, ExactOnce: true}
	if got.Coverage != want {
		t.Fatalf("coverage=%+v want=%+v", got.Coverage, want)
	}
}

func TestObserveRouteEvidenceFailsClosedForMissingJournalProof(t *testing.T) {
	const runID = "evidence-missing-journal"
	fixture := newV2RuntimeFixture(t, v2RuntimeDirect, false, false)
	result, err := fixture.run(t, runID)
	if err != nil || result.Status != core.RunCompleted {
		t.Fatalf("run result=%#v err=%v", result, err)
	}
	mutateV2ChoiceRecord(t, fixture.journal, runID, "detail-18", func(records map[string]core.ToolInvocationRecord, key string) {
		delete(records, key)
	})

	got := ObserveRouteEvidence(context.Background(), fixture.session, fixture.principal, fixture.journal, runID)
	if got.Identity != verifiedV2RouteIdentity() || got.Selection != (SelectedRouteObservation{Route: SelectedRouteUnavailable, Status: SelectionStatusUnavailable}) {
		t.Fatalf("missing choice journal changed identity or selection=%+v", got)
	}
	if got.Coverage.Status != CoverageStatusUnavailable || got.Effects != (RouteEffectsEvidence{JournalStatus: EvidenceStatusVerified, CompletedJournalInvocations: 1, ExternalStatus: EvidenceStatusUnavailable}) {
		t.Fatalf("missing choice journal overclaimed coverage/effects=%+v", got)
	}
	if got.Accounting != (RouteAccountingEvidence{LedgerStatus: EvidenceStatusVerified, ModelCallCount: 3}) || got.Context.ArchiveStatus != EvidenceStatusVerified {
		t.Fatalf("missing choice journal lost independent accounting/context=%+v", got)
	}
}

func TestObserveRouteEvidenceRetainsIndependentEvidenceWhenFrozenArtifactsAreMissing(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*core.RunCompositionData)
	}{
		{name: "model revision", mutate: func(composition *core.RunCompositionData) { composition.ModelRevision = "" }},
		{name: "resolved provider", mutate: func(composition *core.RunCompositionData) { composition.ResolvedProvider = "" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			const runID = "evidence-missing-artifact"
			fixture := newV2RuntimeFixture(t, v2RuntimeDirect, false, false)
			result, err := fixture.run(t, runID)
			if err != nil || result.Status != core.RunCompleted {
				t.Fatalf("run result=%#v err=%v", result, err)
			}
			session := sessionWithMutatedStartComposition(t, fixture, runID, test.mutate)

			got := ObserveRouteEvidence(context.Background(), session, fixture.principal, fixture.journal, runID)
			if got.Identity != verifiedV2RouteIdentity() {
				t.Fatalf("identity=%+v", got.Identity)
			}
			if got.Selection != (SelectedRouteObservation{Route: SelectedRouteUnavailable, Status: SelectionStatusUnavailable}) ||
				got.Coverage.Status != CoverageStatusUnavailable || got.Effects.JournalStatus != EvidenceStatusUnavailable {
				t.Fatalf("missing frozen artifact allowed staged evidence=%+v", got)
			}
			if got.Accounting != (RouteAccountingEvidence{LedgerStatus: EvidenceStatusVerified, ModelCallCount: 3}) || got.Context.ArchiveStatus != EvidenceStatusVerified {
				t.Fatalf("missing frozen artifact lost independent evidence=%+v", got)
			}
		})
	}
}

func TestObserveRouteEvidenceFailsClosedForResumeArtifactDrift(t *testing.T) {
	const runID = "evidence-resume-artifact-drift"
	fixture := newV2RuntimeFixture(t, v2RuntimeDirect, false, false)
	result, err := fixture.run(t, runID)
	if err != nil || result.Status != core.RunCompleted {
		t.Fatalf("run result=%#v err=%v", result, err)
	}
	composition := runStartComposition(t, fixture.session.Events(), runID)
	composition.Capabilities[0].ProviderRevision = "probe-revision-drift"
	compositionRevision, err := core.CompositionRevision(&composition)
	if err != nil {
		t.Fatal(err)
	}
	assignmentRevision, err := core.CompositionMetadataRevision(composition.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.session.Append(runID, core.EvRunResume, core.RunResumeData{
		Composition: &composition, CompositionRevision: compositionRevision, AssignmentRevision: assignmentRevision,
	}); err != nil {
		t.Fatal(err)
	}

	got := ObserveRouteEvidence(context.Background(), fixture.session, fixture.principal, fixture.journal, runID)
	if got.Selection != (SelectedRouteObservation{Route: SelectedRouteUnavailable, Status: SelectionStatusUnavailable}) ||
		got.Coverage.Status != CoverageStatusUnavailable || got.Effects.JournalStatus != EvidenceStatusUnavailable {
		t.Fatalf("resume artifact drift allowed staged evidence=%+v", got)
	}
}

func TestObserveRouteEvidenceKeepsV2IdentityAndIndependentEvidenceWithoutChoice(t *testing.T) {
	const runID = "evidence-no-choice"
	fixture := newV2RuntimeFixture(t, v2RuntimeDirect, false, false)
	result, err := fixture.run(t, runID)
	if err != nil || result.Status != core.RunCompleted {
		t.Fatalf("run result=%#v err=%v", result, err)
	}
	prefix := sessionThroughProbe(t, fixture, runID)

	got := ObserveRouteEvidence(context.Background(), prefix, fixture.principal, fixture.journal, runID)
	if got.Identity != verifiedV2RouteIdentity() || got.Selection != (SelectedRouteObservation{Route: SelectedRouteUnavailable, Status: SelectionStatusUnavailable}) {
		t.Fatalf("missing choice identity/selection=%+v", got)
	}
	wantCoverage := RouteCoverageEvidence{Status: CoverageStatusIncomplete, CandidateCount: 2}
	if got.Coverage != wantCoverage {
		t.Fatalf("missing choice coverage=%+v want=%+v", got.Coverage, wantCoverage)
	}
	if got.Effects != (RouteEffectsEvidence{JournalStatus: EvidenceStatusVerified, CompletedJournalInvocations: 1, ExternalStatus: EvidenceStatusUnavailable}) {
		t.Fatalf("missing choice effects=%+v", got.Effects)
	}
	if got.Accounting != (RouteAccountingEvidence{LedgerStatus: EvidenceStatusVerified, ModelCallCount: 1}) || got.Context.ArchiveStatus != EvidenceStatusVerified {
		t.Fatalf("missing choice lost independent evidence=%+v", got)
	}
}

func TestObserveRouteEvidenceRejectsV1DirectRoute(t *testing.T) {
	fixture := newV2RuntimeFixture(t, v2RuntimeV1Direct, false, false)
	result, err := fixture.run(t, "evidence-v1-direct")
	if err != nil || result.Status != core.RunCompleted {
		t.Fatalf("run result=%#v err=%v", result, err)
	}

	got := ObserveRouteEvidence(context.Background(), fixture.session, fixture.principal, fixture.journal, "evidence-v1-direct")
	if got != unavailableRouteEvidence() {
		t.Fatalf("v1 direct produced v2 evidence=%+v", got)
	}
}

func TestExpectsRouteEvidenceDistinguishesV2IntentFromV1(t *testing.T) {
	for _, test := range []struct {
		name    string
		mode    v2RuntimeMode
		expects bool
	}{
		{name: "v2", mode: v2RuntimeDirect, expects: true},
		{name: "v1", mode: v2RuntimeV1Direct, expects: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			const runID = "evidence-expectation"
			fixture := newV2RuntimeFixture(t, test.mode, false, false)
			result, err := fixture.run(t, runID)
			if err != nil || result.Status != core.RunCompleted {
				t.Fatalf("run result=%#v err=%v", result, err)
			}
			if got := ExpectsRouteEvidence(fixture.session, runID); got != test.expects {
				t.Fatalf("ExpectsRouteEvidence=%t, want %t", got, test.expects)
			}
		})
	}
}

func TestObservedV2UsageLedgerRejectsMissingOutcomeUsage(t *testing.T) {
	fixture := newV2RuntimeFixture(t, v2RuntimeDirect, false, false)
	result, err := fixture.run(t, "evidence-missing-usage")
	if err != nil || result.Status != core.RunCompleted {
		t.Fatalf("run result=%#v err=%v", result, err)
	}
	events := fixture.session.Events()
	for index, event := range events {
		if event.RunID == "evidence-missing-usage" && event.Type == core.EvRunUsage {
			events = append(events[:index], events[index+1:]...)
			break
		}
	}
	if calls, _, _, ok := observedV2UsageLedger(events, "evidence-missing-usage"); ok || calls != 0 {
		t.Fatalf("missing usage ledger accepted: calls=%d ok=%t", calls, ok)
	}
}

func verifiedV2RouteIdentity() RouteIdentityEvidence {
	return RouteIdentityEvidence{
		Status: EvidenceStatusVerified, Version: RouteProbeVersion, Mode: probeRouteMode,
		Implementation: RouteProbeImplementationRevision,
	}
}

func sessionThroughProbe(t *testing.T, fixture *v2RuntimeFixture, runID string) *core.Session {
	t.Helper()
	events := fixture.session.Events()
	end := -1
	for index, event := range events {
		if event.RunID == runID && event.Type == core.EvStepEnd {
			end = index
			break
		}
	}
	if end < 0 {
		t.Fatal("probe step end was not recorded")
	}
	prefix := append([]core.SessionEvent(nil), events[:end+1]...)
	restored, err := core.RestoreSession(core.SessionOptions{
		ID: fixture.session.ID(), ProfileID: fixture.session.ProfileID(), Principal: fixture.principal, Scope: fixture.session.Scope(),
	}, prefix)
	if err != nil {
		t.Fatalf("restore probe prefix: %v", err)
	}
	return restored
}

func sessionWithMutatedStartComposition(t *testing.T, fixture *v2RuntimeFixture, runID string, mutate func(*core.RunCompositionData)) *core.Session {
	t.Helper()
	events := fixture.session.Events()
	found := false
	for index := range events {
		if events[index].RunID != runID || events[index].Type != core.EvRunStart {
			continue
		}
		var start core.RunStartData
		if err := json.Unmarshal(events[index].Data, &start); err != nil || start.Composition == nil {
			t.Fatalf("decode frozen run composition: %v", err)
		}
		composition := *start.Composition
		composition.Capabilities = append([]core.SnapshotCapability(nil), composition.Capabilities...)
		mutate(&composition)
		compositionRevision, err := core.CompositionRevision(&composition)
		if err != nil {
			t.Fatal(err)
		}
		assignmentRevision, err := core.CompositionMetadataRevision(composition.Metadata)
		if err != nil {
			t.Fatal(err)
		}
		start.Composition, start.CompositionRevision, start.AssignmentRevision = &composition, compositionRevision, assignmentRevision
		data, err := json.Marshal(start)
		if err != nil {
			t.Fatal(err)
		}
		events[index].Data = data
		found = true
		break
	}
	if !found {
		t.Fatal("run start composition was not recorded")
	}
	return restoreEvidenceSession(t, fixture, events)
}

func runStartComposition(t *testing.T, events []core.SessionEvent, runID string) core.RunCompositionData {
	t.Helper()
	for _, event := range events {
		if event.RunID != runID || event.Type != core.EvRunStart {
			continue
		}
		var start core.RunStartData
		if err := json.Unmarshal(event.Data, &start); err != nil || start.Composition == nil {
			t.Fatalf("decode frozen run composition: %v", err)
		}
		composition := *start.Composition
		composition.Capabilities = append([]core.SnapshotCapability(nil), composition.Capabilities...)
		return composition
	}
	t.Fatal("run start composition was not recorded")
	return core.RunCompositionData{}
}

func restoreEvidenceSession(t *testing.T, fixture *v2RuntimeFixture, events []core.SessionEvent) *core.Session {
	t.Helper()
	restored, err := core.RestoreSession(core.SessionOptions{
		ID: fixture.session.ID(), ProfileID: fixture.session.ProfileID(), Principal: fixture.principal, Scope: fixture.session.Scope(),
	}, events)
	if err != nil {
		t.Fatalf("restore evidence session: %v", err)
	}
	return restored
}
