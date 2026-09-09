package server

import (
	"strings"
	"testing"
	"time"

	"github.com/whhhh1500/auto-agent/internal/executionroute"
	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/evaluation"
)

func TestReleaseEfficiencyGateIsOptionalAndFailClosed(t *testing.T) {
	for name, test := range map[string]struct {
		candidateInput int64
		mutate         func(*evaluation.RunResult, *evaluation.RunResult)
		policy         *evaluation.EfficiencyGatePolicy
		wantPassed     bool
		wantVerdict    evaluation.EfficiencyGateVerdict
		wantReason     evaluation.EfficiencyGateReasonCode
	}{
		"passed": {
			candidateInput: 80,
			policy:         efficiencyPolicyPointer(evaluation.DefaultEfficiencyGatePolicy()),
			wantPassed:     true,
			wantVerdict:    evaluation.EfficiencyGatePassed,
		},
		"failed": {
			candidateInput: 101,
			policy:         efficiencyPolicyPointer(evaluation.DefaultEfficiencyGatePolicy()),
			wantPassed:     false,
			wantVerdict:    evaluation.EfficiencyGateFailed,
			wantReason:     evaluation.EfficiencyReasonPairResourceRegression,
		},
		"inconclusive": {
			candidateInput: 80,
			mutate: func(candidate, _ *evaluation.RunResult) {
				candidate.Cases[0].Ledger = nil
			},
			policy:      efficiencyPolicyPointer(evaluation.DefaultEfficiencyGatePolicy()),
			wantPassed:  false,
			wantVerdict: evaluation.EfficiencyGateInconclusive,
			wantReason:  evaluation.EfficiencyReasonLedgerMissing,
		},
		"not requested": {
			candidateInput: 80,
			policy:         efficiencyPolicyPointer(evaluation.EfficiencyGatePolicy{}),
			wantPassed:     true,
			wantVerdict:    evaluation.EfficiencyGateNotRequested,
			wantReason:     evaluation.EfficiencyReasonPolicyDisabled,
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate, baseline := releaseEfficiencyRuns(t, test.candidateInput, 100)
			if test.mutate != nil {
				test.mutate(&candidate, &baseline)
			}
			gate := evaluation.GateResult{Passed: true}
			if err := applyReleaseEfficiencyGate(&gate, candidate, &baseline, test.policy); err != nil {
				t.Fatal(err)
			}
			if gate.Passed != test.wantPassed || gate.Efficiency == nil || gate.Efficiency.Verdict != test.wantVerdict {
				t.Fatalf("gate=%#v, want passed=%t verdict=%q", gate, test.wantPassed, test.wantVerdict)
			}
			if test.wantReason != "" && (len(gate.Efficiency.ReasonCodes) != 1 || gate.Efficiency.ReasonCodes[0] != test.wantReason) {
				t.Fatalf("efficiency reasons=%#v, want %q", gate.Efficiency.ReasonCodes, test.wantReason)
			}
		})
	}
}

func TestReleaseEfficiencyGateBindsBaselineIdentityAndRunsAfterQuality(t *testing.T) {
	candidate, baseline := releaseEfficiencyRuns(t, 80, 100)
	baseline.SubjectID = "other"
	policy := efficiencyPolicyPointer(evaluation.DefaultEfficiencyGatePolicy())
	gate := evaluation.GateResult{Passed: true}
	if err := applyReleaseEfficiencyGate(&gate, candidate, &baseline, policy); err != nil {
		t.Fatal(err)
	}
	if gate.Passed || gate.Efficiency == nil || gate.Efficiency.Verdict != evaluation.EfficiencyGateInconclusive ||
		len(gate.Efficiency.ReasonCodes) != 1 || gate.Efficiency.ReasonCodes[0] != evaluation.EfficiencyReasonRunInvalid {
		t.Fatalf("identity mismatch did not fail closed: %#v", gate)
	}

	qualityRejected := evaluation.GateResult{Passed: false, Reasons: []string{"quality"}}
	if err := applyReleaseEfficiencyGate(&qualityRejected, candidate, &baseline, policy); err != nil {
		t.Fatal(err)
	}
	if qualityRejected.Efficiency != nil {
		t.Fatalf("efficiency was evaluated before quality rejection: %#v", qualityRejected)
	}
}

func TestReleaseEfficiencyGateRejectsUnboundCaseArtifact(t *testing.T) {
	candidate, baseline := releaseEfficiencyRuns(t, 80, 100)
	candidate.Cases[0].Artifacts.AssignmentRevision = strings.Repeat("f", 64)
	gate := evaluation.GateResult{Passed: true}
	if err := applyReleaseEfficiencyGate(&gate, candidate, &baseline, efficiencyPolicyPointer(evaluation.DefaultEfficiencyGatePolicy())); err != nil {
		t.Fatal(err)
	}
	if gate.Passed || gate.Efficiency == nil || gate.Efficiency.Verdict != evaluation.EfficiencyGateInconclusive ||
		len(gate.Efficiency.ReasonCodes) != 1 || gate.Efficiency.ReasonCodes[0] != evaluation.EfficiencyReasonRunInvalid {
		t.Fatalf("unbound artifact did not fail closed: %#v", gate)
	}
}

func efficiencyPolicyPointer(policy evaluation.EfficiencyGatePolicy) *evaluation.EfficiencyGatePolicy {
	return &policy
}

func releaseEfficiencyRuns(t *testing.T, candidateInput, baselineInput int64) (evaluation.RunResult, evaluation.RunResult) {
	t.Helper()
	metadata := map[string]string{
		evaluation.EfficiencyCohortBenchmarkID:         "benchmark",
		evaluation.EfficiencyCohortTaskFamilyID:        "task-family",
		evaluation.EfficiencyCohortScenarioID:          "scenario",
		evaluation.EfficiencyCohortToolSurfaceRevision: "tool-surface",
		evaluation.EfficiencyCohortContextConfigRev:    "context-config",
		evaluation.EfficiencyCohortProtocolRevision:    "protocol",
		evaluation.EfficiencyCohortProvider:            "provider",
		evaluation.EfficiencyCohortModelRevision:       "model-revision",
		evaluation.EfficiencyCohortDevelopmentSet:      evaluation.EfficiencyCohortHoldout,
	}
	candidate := releaseEfficiencyRun(t, "eval_release_candidate", candidateInput, "direct_only", metadata)
	baseline := releaseEfficiencyRun(t, "eval_release_baseline", baselineInput, "ptc_only", metadata)
	candidate.BaselineRunID = baseline.ID
	return candidate, baseline
}

func releaseEfficiencyRun(t *testing.T, id string, input int64, mode string, metadata map[string]string) evaluation.RunResult {
	t.Helper()
	composition := map[string]string{
		executionroute.RouteVersionKey:        executionroute.RouteVersion,
		executionroute.RouteModeKey:           mode,
		executionroute.RouteCatalogToolIDKey:  "program.catalog",
		executionroute.RouteExecuteToolIDKey:  "program.execute",
		executionroute.RouteImplementationKey: executionroute.RouteImplementationRevision,
	}
	routeID, err := core.CompositionMetadataRevision(composition)
	if err != nil {
		t.Fatal(err)
	}
	assignmentRevision, err := core.CompositionMetadataRevision(composition)
	if err != nil {
		t.Fatal(err)
	}
	metadataCopy := make(map[string]string, len(metadata)+1)
	for key, value := range metadata {
		metadataCopy[key] = value
	}
	metadataCopy[evaluation.EfficiencyCohortRouteCandidateID] = routeID
	now := time.Now().UTC()
	cases := make([]evaluation.CaseResult, 0, 3)
	for index := 1; index <= 3; index++ {
		cases = append(cases, evaluation.CaseResult{
			CaseID: "case-" + string(rune('0'+index)), SessionID: "sess_release_" + string(rune('0'+index)), AgentRunID: "run_release_" + string(rune('0'+index)),
			Status: core.RunCompleted, Score: 1, Passed: true, CompletedAt: now,
			Artifacts: evaluation.ArtifactSnapshot{CapabilitySnapshotID: "tool-surface", ResolvedProvider: "provider", ModelRevision: "model-revision", CompositionRevision: strings.Repeat("b", 64), AssignmentRevision: assignmentRevision},
			Evidence:  &evaluation.ExecutionEvidence{ReportedInputTokens: input, UsageReports: 1},
			Ledger: &evaluation.ExecutionLedger{Complete: true,
				ModelCalls:        []evaluation.ExecutionLedgerModelCall{{Step: 0, Invoked: true, Outcome: "ok", UsageReports: 1, Usage: &evaluation.ExecutionLedgerUsage{InputTokens: input}}},
				ContextAssemblies: []evaluation.ExecutionLedgerContext{{ContextWindowTokens: 32768, MaxOutputTokens: 1024, Outcome: "ok"}},
			},
		})
	}
	return evaluation.RunResult{
		ID: id, DatasetID: "release.efficiency", DatasetVersion: 1, DatasetRevision: strings.Repeat("a", 64),
		TenantID: "acme", SubjectID: "alice", ProfileID: "release.agent", Status: evaluation.RunCompleted,
		Score: 1, Passed: true, TotalCases: len(cases), PassedCases: len(cases), Cases: cases,
		AssignmentRevision: assignmentRevision, CompositionMetadata: composition, Metadata: metadataCopy,
		CreatedAt: now, CompletedAt: now,
	}
}
