package evaluation

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/whhhh1500/auto-agent/internal/executionroute"
	"github.com/whhhh1500/auto-agent/pkg/app/programmatic"
	"github.com/whhhh1500/auto-agent/pkg/app/runexecutor"
	core "github.com/whhhh1500/auto-agent/pkg/core"
)

func TestEvaluateEfficiencyGatePassesStrictHoldoutAndAllowsRouteDifference(t *testing.T) {
	candidate, baseline := efficiencyGateRuns(80, 100)
	policy := DefaultEfficiencyGatePolicy()

	result, err := EvaluateEfficiencyGate(candidate, baseline, policy)
	if err != nil {
		t.Fatal(err)
	}
	if result.Verdict != EfficiencyGatePassed || len(result.Pairs) != 3 || len(result.Strata) != 1 {
		t.Fatalf("unexpected passing result: %#v", result)
	}
	if result.Strata[0].CandidateTotalTokens != 270 || result.Strata[0].BaselineTotalTokens != 330 || result.Strata[0].MedianTokenReductionNumerator != 20 || result.Strata[0].MedianTokenReductionDenominator != 1 {
		t.Fatalf("wrong stratum projection: %#v", result.Strata[0])
	}
}

func TestEvaluateEfficiencyGateAcceptsV2AutoProbeOnce(t *testing.T) {
	candidate, baseline := efficiencyGateRuns(80, 100)
	setEfficiencyRouteCandidateV2(&candidate)

	result, err := EvaluateEfficiencyGate(candidate, baseline, DefaultEfficiencyGatePolicy())
	if err != nil {
		t.Fatal(err)
	}
	if result.Verdict != EfficiencyGatePassed {
		t.Fatalf("v2 auto_probe_once route did not pass the gate: %#v", result)
	}
}

func TestEvaluateEfficiencyGateRejectsUnboundOrDriftedComparisonRuns(t *testing.T) {
	for name, test := range map[string]struct {
		mutate func(*RunResult, *RunResult)
		reason EfficiencyGateReasonCode
	}{
		"same run": {
			mutate: func(candidate, baseline *RunResult) {
				candidate.ID = baseline.ID
			},
			reason: EfficiencyReasonRunInvalid,
		},
		"wrong baseline reference": {
			mutate: func(candidate, _ *RunResult) {
				candidate.BaselineRunID = "evaluation-other-baseline"
			},
			reason: EfficiencyReasonRunInvalid,
		},
		"executor revision drift": {
			mutate: func(candidate, _ *RunResult) {
				candidate.CompositionMetadata["harness.executor.implementation_revision"] = "other-executor-revision"
				refreshEfficiencyCompositionRevision(candidate)
			},
			reason: EfficiencyReasonRouteCandidateMismatch,
		},
		"extra composition metadata drift": {
			mutate: func(candidate, _ *RunResult) {
				candidate.CompositionMetadata["harness.execution.variant"] = "candidate-only"
				refreshEfficiencyCompositionRevision(candidate)
			},
			reason: EfficiencyReasonRouteCandidateMismatch,
		},
		"empty composition metadata is not absent": {
			mutate: func(candidate, _ *RunResult) {
				candidate.CompositionMetadata["harness.execution.empty"] = ""
				refreshEfficiencyCompositionRevision(candidate)
			},
			reason: EfficiencyReasonRouteCandidateMismatch,
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate, baseline := efficiencyGateRuns(80, 100)
			test.mutate(&candidate, &baseline)

			result, err := EvaluateEfficiencyGate(candidate, baseline, DefaultEfficiencyGatePolicy())
			if err != nil {
				t.Fatal(err)
			}
			assertEfficiencyResult(t, result, EfficiencyGateInconclusive, test.reason)
		})
	}
}

func TestEvaluateEfficiencyGateRejectsV1V2RouteVersionModeRevisionCrosses(t *testing.T) {
	for name, mutate := range map[string]func(*RunResult){
		"v2 with v1 mode": func(candidate *RunResult) {
			setEfficiencyRouteCandidateV2(candidate)
			candidate.CompositionMetadata[executionroute.RouteModeKey] = string(programmatic.RouteDirectOnly)
			refreshEfficiencyCompositionRevision(candidate)
		},
		"v2 with v1 revision": func(candidate *RunResult) {
			setEfficiencyRouteCandidateV2(candidate)
			candidate.CompositionMetadata[executionroute.RouteImplementationKey] = executionroute.RouteImplementationRevision
			refreshEfficiencyCompositionRevision(candidate)
		},
		"v1 with v2 mode": func(candidate *RunResult) {
			candidate.CompositionMetadata[executionroute.RouteModeKey] = string(programmatic.RouteAutoProbeOnce)
			refreshEfficiencyCompositionRevision(candidate)
		},
		"v1 with v2 revision": func(candidate *RunResult) {
			candidate.CompositionMetadata[executionroute.RouteImplementationKey] = executionroute.RouteProbeImplementationRevision
			refreshEfficiencyCompositionRevision(candidate)
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate, baseline := efficiencyGateRuns(80, 100)
			mutate(&candidate)

			result, err := EvaluateEfficiencyGate(candidate, baseline, DefaultEfficiencyGatePolicy())
			if err != nil {
				t.Fatal(err)
			}
			assertEfficiencyResult(t, result, EfficiencyGateInconclusive, EfficiencyReasonRouteCandidateMismatch)
		})
	}
}

func TestCanonicalEfficiencyRouteIDRejectsVersionModeAndRevisionMismatches(t *testing.T) {
	base := efficiencyRouteMetadata(executionroute.RouteProbeVersion, string(programmatic.RouteAutoProbeOnce), executionroute.RouteProbeImplementationRevision)
	for name, mutate := range map[string]func(map[string]string){
		"v2 mode is v1 mode": func(metadata map[string]string) {
			metadata[executionroute.RouteModeKey] = string(programmatic.RouteDirectOnly)
		},
		"v2 revision is v1 revision": func(metadata map[string]string) {
			metadata[executionroute.RouteImplementationKey] = executionroute.RouteImplementationRevision
		},
		"v1 revision is v2 revision": func(metadata map[string]string) {
			metadata[executionroute.RouteVersionKey] = executionroute.RouteVersion
			metadata[executionroute.RouteModeKey] = string(programmatic.RouteDirectOnly)
			metadata[executionroute.RouteImplementationKey] = executionroute.RouteProbeImplementationRevision
		},
		"unknown version": func(metadata map[string]string) {
			metadata[executionroute.RouteVersionKey] = "3"
		},
	} {
		t.Run(name, func(t *testing.T) {
			metadata := cloneEfficiencyMetadata(base)
			mutate(metadata)
			if routeID, ok := canonicalEfficiencyRouteID(metadata); ok || routeID != "" {
				t.Fatalf("invalid route metadata was accepted: id=%q metadata=%#v", routeID, metadata)
			}
		})
	}
}

func TestEvaluateEfficiencyGateRequiresDistinctCanonicalRouteCandidates(t *testing.T) {
	for name, mutate := range map[string]struct {
		mutate func(*RunResult, *RunResult)
		reason EfficiencyGateReasonCode
	}{
		"missing": {
			mutate: func(candidate, _ *RunResult) { delete(candidate.Metadata, EfficiencyCohortRouteCandidateID) },
			reason: EfficiencyReasonRouteCandidateMissing,
		},
		"baseline missing": {
			mutate: func(_, baseline *RunResult) { delete(baseline.Metadata, EfficiencyCohortRouteCandidateID) },
			reason: EfficiencyReasonRouteCandidateMissing,
		},
		"same": {
			mutate: func(candidate, _ *RunResult) { setEfficiencyRouteCandidate(candidate, "direct_only") },
			reason: EfficiencyReasonRouteCandidateSame,
		},
		"composition mismatch": {
			mutate: func(candidate, _ *RunResult) {
				candidate.CompositionMetadata[executionroute.RouteModeKey] = "direct_only"
			},
			reason: EfficiencyReasonRouteCandidateMismatch,
		},
		"artifact revision mismatch": {
			mutate: func(candidate, _ *RunResult) {
				candidate.Cases[0].Artifacts.AssignmentRevision = strings.Repeat("0", 64)
			},
			reason: EfficiencyReasonRouteCandidateMismatch,
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate, baseline := efficiencyGateRuns(80, 100)
			mutate.mutate(&candidate, &baseline)
			result, err := EvaluateEfficiencyGate(candidate, baseline, DefaultEfficiencyGatePolicy())
			if err != nil {
				t.Fatal(err)
			}
			assertEfficiencyResult(t, result, EfficiencyGateInconclusive, mutate.reason)
		})
	}
}

func TestEvaluateEfficiencyGateDoesNotTradeQualityForCost(t *testing.T) {
	candidate, baseline := efficiencyGateRuns(0, 100)
	candidate.Cases[0].Passed = false

	result, err := EvaluateEfficiencyGate(candidate, baseline, DefaultEfficiencyGatePolicy())
	if err != nil {
		t.Fatal(err)
	}
	assertEfficiencyResult(t, result, EfficiencyGateFailed, EfficiencyReasonCandidateQualityFailed)
}

func TestEvaluateEfficiencyGateRejectsIncompleteUsageAndSummaryAccounting(t *testing.T) {
	for name, test := range map[string]struct {
		mutate func(*RunResult)
		reason EfficiencyGateReasonCode
	}{
		"missing ledger": {mutate: func(run *RunResult) { run.Cases[0].Ledger = nil }, reason: EfficiencyReasonLedgerMissing},
		"incomplete ledger": {mutate: func(run *RunResult) {
			run.Cases[0].Ledger.Complete = false
			run.Cases[0].Ledger.IncompleteReasons = []string{"adapter_error"}
		}, reason: EfficiencyReasonLedgerIncomplete},
		"missing model usage":    {mutate: func(run *RunResult) { run.Cases[0].Ledger.ModelCalls[0].Usage = nil }, reason: EfficiencyReasonRunInvalid},
		"bad context count":      {mutate: func(run *RunResult) { run.Cases[0].Ledger.ContextAssemblies = nil }, reason: EfficiencyReasonRunInvalid},
		"summary usage count":    {mutate: func(run *RunResult) { run.Cases[0].Evidence.UsageReports++ }, reason: EfficiencyReasonEvidenceUsageMismatch},
		"summary token mismatch": {mutate: func(run *RunResult) { run.Cases[0].Evidence.ReportedInputTokens++ }, reason: EfficiencyReasonEvidenceTokenMismatch},
		"non-ok tool result":     {mutate: func(run *RunResult) { run.Cases[0].Evidence.ToolResultEventsNotOK = 1 }, reason: EfficiencyReasonToolResultNotOK},
		"incomplete gate":        {mutate: func(run *RunResult) { run.Cases[0].Ledger.GateCalls[0].Outcome = "denied" }, reason: EfficiencyReasonGateIncomplete},
	} {
		t.Run(name, func(t *testing.T) {
			candidate, baseline := efficiencyGateRuns(80, 100)
			test.mutate(&candidate)
			result, err := EvaluateEfficiencyGate(candidate, baseline, DefaultEfficiencyGatePolicy())
			if err != nil {
				t.Fatal(err)
			}
			assertEfficiencyResult(t, result, EfficiencyGateInconclusive, test.reason)
		})
	}
}

func TestEvaluateEfficiencyGateUsesStrictRunValidation(t *testing.T) {
	for name, mutate := range map[string]func(*RunResult){
		"duplicate case": func(run *RunResult) {
			run.Cases = append(run.Cases, run.Cases[0])
			run.TotalCases = 2
			run.PassedCases = 2
		},
		"non-finite score": func(run *RunResult) { run.Score = math.NaN() },
	} {
		t.Run(name, func(t *testing.T) {
			candidate, baseline := efficiencyGateRuns(80, 100)
			mutate(&candidate)
			result, err := EvaluateEfficiencyGate(candidate, baseline, DefaultEfficiencyGatePolicy())
			if err != nil {
				t.Fatal(err)
			}
			assertEfficiencyResult(t, result, EfficiencyGateInconclusive, EfficiencyReasonRunInvalid)
		})
	}
}

func TestEvaluateEfficiencyGateCohortAndArtifactRequirements(t *testing.T) {
	for name, mutate := range map[string]func(*RunResult, *RunResult){
		"missing benchmark": func(candidate, baseline *RunResult) { delete(candidate.Metadata, EfficiencyCohortBenchmarkID) },
		"different cohort": func(candidate, baseline *RunResult) {
			candidate.Metadata[EfficiencyCohortScenarioID] = "other-scenario"
		},
		"cross model": func(candidate, baseline *RunResult) {
			candidate.Metadata[EfficiencyCohortModelRevision] = "model-other"
			candidate.Cases[0].Artifacts.ModelRevision = "model-other"
		},
		"artifact mismatch": func(candidate, baseline *RunResult) { candidate.Cases[0].Artifacts.ResolvedProvider = "other-provider" },
		"tool surface snapshot mismatch": func(candidate, baseline *RunResult) {
			candidate.Cases[0].Artifacts.CapabilitySnapshotID = "tools-other"
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate, baseline := efficiencyGateRuns(80, 100)
			mutate(&candidate, &baseline)
			result, err := EvaluateEfficiencyGate(candidate, baseline, DefaultEfficiencyGatePolicy())
			if err != nil {
				t.Fatal(err)
			}
			if result.Verdict != EfficiencyGateInconclusive {
				t.Fatalf("invalid cohort was not inconclusive: %#v", result)
			}
		})
	}
}

func TestEvaluateEfficiencyGateRejectsPerPairRegression(t *testing.T) {
	candidate, baseline := efficiencyGateRuns(101, 100)
	policy := DefaultEfficiencyGatePolicy()

	result, err := EvaluateEfficiencyGate(candidate, baseline, policy)
	if err != nil {
		t.Fatal(err)
	}
	assertEfficiencyResult(t, result, EfficiencyGateFailed, EfficiencyReasonPairResourceRegression)
}

func TestEvaluateEfficiencyGateSupportsExplicitZeroUsageReport(t *testing.T) {
	candidate, baseline := efficiencyGateRuns(0, 0)
	setEfficiencyCaseUsage(&candidate.Cases[0], 0, 0)
	setEfficiencyCaseUsage(&baseline.Cases[0], 0, 0)
	policy := DefaultEfficiencyGatePolicy()
	policy.MinImproved = 0
	policy.MinMedianTokenReduction = 0

	result, err := EvaluateEfficiencyGate(candidate, baseline, policy)
	if err != nil {
		t.Fatal(err)
	}
	if result.Verdict != EfficiencyGatePassed {
		t.Fatalf("explicit 0/0 provider report was not accepted: %#v", result)
	}
}

func TestEvaluateEfficiencyGateBenefitAndSampleThresholds(t *testing.T) {
	t.Run("sample insufficient is inconclusive", func(t *testing.T) {
		candidate, baseline := efficiencyGateRuns(80, 100)
		policy := DefaultEfficiencyGatePolicy()
		policy.MinPaired = 4
		policy.MinImproved = 1
		result, err := EvaluateEfficiencyGate(candidate, baseline, policy)
		if err != nil {
			t.Fatal(err)
		}
		assertEfficiencyResult(t, result, EfficiencyGateInconclusive, EfficiencyReasonSampleInsufficient)
	})
	t.Run("improvement threshold uses actual pair count", func(t *testing.T) {
		candidate, baseline := efficiencyGateRuns(80, 100)
		policy := DefaultEfficiencyGatePolicy()
		policy.MinPaired = 1
		policy.MinImproved = 4
		result, err := EvaluateEfficiencyGate(candidate, baseline, policy)
		if err != nil {
			t.Fatal(err)
		}
		assertEfficiencyResult(t, result, EfficiencyGateInconclusive, EfficiencyReasonSampleInsufficient)
	})
	t.Run("improvements may exceed policy minimum paired when actual sample supports them", func(t *testing.T) {
		candidate, baseline := efficiencyGateRuns(80, 100)
		policy := DefaultEfficiencyGatePolicy()
		policy.MinPaired = 1
		policy.MinImproved = 2
		result, err := EvaluateEfficiencyGate(candidate, baseline, policy)
		if err != nil {
			t.Fatal(err)
		}
		if result.Verdict != EfficiencyGatePassed {
			t.Fatalf("actual three-pair improvement threshold did not pass: %#v", result)
		}
	})
	t.Run("minimum improvements is a demonstrated failure", func(t *testing.T) {
		candidate, baseline := efficiencyGateRuns(100, 100)
		policy := DefaultEfficiencyGatePolicy()
		policy.MinMedianTokenReduction = 0
		result, err := EvaluateEfficiencyGate(candidate, baseline, policy)
		if err != nil {
			t.Fatal(err)
		}
		assertEfficiencyResult(t, result, EfficiencyGateFailed, EfficiencyReasonImprovementInsufficient)
	})
	t.Run("median threshold is a demonstrated failure", func(t *testing.T) {
		candidate, baseline := efficiencyGateRuns(99, 100)
		policy := DefaultEfficiencyGatePolicy()
		policy.MinMedianTokenReduction = 2
		result, err := EvaluateEfficiencyGate(candidate, baseline, policy)
		if err != nil {
			t.Fatal(err)
		}
		assertEfficiencyResult(t, result, EfficiencyGateFailed, EfficiencyReasonMedianReductionInsufficient)
	})
}

func TestEvaluateEfficiencyGateNeverPassesDevelopmentSet(t *testing.T) {
	candidate, baseline := efficiencyGateRuns(80, 100)
	candidate.Metadata[EfficiencyCohortDevelopmentSet] = EfficiencyCohortDevelopment
	baseline.Metadata[EfficiencyCohortDevelopmentSet] = EfficiencyCohortDevelopment

	result, err := EvaluateEfficiencyGate(candidate, baseline, DefaultEfficiencyGatePolicy())
	if err != nil {
		t.Fatal(err)
	}
	assertEfficiencyResult(t, result, EfficiencyGateInconclusive, EfficiencyReasonDevelopmentSet)
}

func TestEvaluateEfficiencyGatePolicyValidationAndDisabledMode(t *testing.T) {
	for name, policy := range map[string]EfficiencyGatePolicy{
		"empty contract":     {Enabled: true, MinPaired: 1},
		"unknown contract":   {Enabled: true, ContractID: "efficiency-gate/v9", MinPaired: 1},
		"negative bps":       {Enabled: true, ContractID: EfficiencyGateContractV1, MaxPairRegressionBPS: -1, MinPaired: 1},
		"excessive bps":      {Enabled: true, ContractID: EfficiencyGateContractV1, MaxPairRegressionBPS: MaxEfficiencyGateBPS + 1, MinPaired: 1},
		"negative absolute":  {Enabled: true, ContractID: EfficiencyGateContractV1, MaxPairRegressionAbsolute: -1, MinPaired: 1},
		"excessive absolute": {Enabled: true, ContractID: EfficiencyGateContractV1, MaxPairRegressionAbsolute: MaxEfficiencyGateAbsoluteDelta + 1, MinPaired: 1},
		"zero paired":        {Enabled: true, ContractID: EfficiencyGateContractV1, MinPaired: 0},
		"too many improved":  {Enabled: true, ContractID: EfficiencyGateContractV1, MinPaired: 1, MinImproved: MaxDatasetCases + 1},
		"negative median":    {Enabled: true, ContractID: EfficiencyGateContractV1, MinPaired: 1, MinMedianTokenReduction: -1},
	} {
		t.Run(name, func(t *testing.T) {
			candidate, baseline := efficiencyGateRuns(80, 100)
			if _, err := EvaluateEfficiencyGate(candidate, baseline, policy); err == nil {
				t.Fatal("invalid policy was accepted")
			}
		})
	}
	candidate, baseline := efficiencyGateRuns(80, 100)
	result, err := EvaluateEfficiencyGate(candidate, baseline, EfficiencyGatePolicy{})
	if err != nil {
		t.Fatal(err)
	}
	assertEfficiencyResult(t, result, EfficiencyGateNotRequested, EfficiencyReasonPolicyDisabled)
}

func TestEvaluateEfficiencyGateRejectsMalformedOverflowWithoutWrapping(t *testing.T) {
	candidate, baseline := efficiencyGateRuns(80, 100)
	max := int64(^uint64(0) >> 1)
	ledger := candidate.Cases[0].Ledger
	ledger.ModelCalls = append(ledger.ModelCalls, ExecutionLedgerModelCall{Step: 1, Invoked: true, Outcome: "ok", UsageReports: 1, Usage: &ExecutionLedgerUsage{InputTokens: 1}})
	ledger.ContextAssemblies = append(ledger.ContextAssemblies, ExecutionLedgerContext{Outcome: "ok"})
	ledger.GateCalls = append(ledger.GateCalls, ExecutionLedgerGateCall{Step: 1, Outcome: "accepted"})
	ledger.ModelCalls[0].Usage.InputTokens = max
	candidate.Cases[0].Evidence.ReportedInputTokens = max
	candidate.Cases[0].Evidence.UsageReports = 2

	result, err := EvaluateEfficiencyGate(candidate, baseline, DefaultEfficiencyGatePolicy())
	if err != nil {
		t.Fatal(err)
	}
	assertEfficiencyResult(t, result, EfficiencyGateInconclusive, EfficiencyReasonRunInvalid)
	if withinResourceTolerance(max, 0, 0, 0) {
		t.Fatal("overflowing resource comparison accepted a max-int regression")
	}
}

func TestEfficiencyGateResourceToleranceUsesExactIntegerArithmetic(t *testing.T) {
	max := int64(^uint64(0) >> 1)
	for name, test := range map[string]struct {
		candidate, baseline, bps, absolute int64
		want                               bool
	}{
		"exact five percent": {candidate: 105, baseline: 100, bps: 500, want: true},
		"over five percent":  {candidate: 106, baseline: 100, bps: 500, want: false},
		"absolute from zero": {candidate: 1, baseline: 0, absolute: 1, want: true},
		"max values":         {candidate: max, baseline: max, bps: MaxEfficiencyGateBPS, want: true},
		"max regression":     {candidate: max, baseline: 0, want: false},
	} {
		t.Run(name, func(t *testing.T) {
			if got := withinResourceTolerance(test.candidate, test.baseline, test.bps, test.absolute); got != test.want {
				t.Fatalf("withinResourceTolerance(%d, %d, %d, %d) = %t, want %t", test.candidate, test.baseline, test.bps, test.absolute, got, test.want)
			}
		})
	}
}

func TestEfficiencyGateResultDoesNotExposeCaseContent(t *testing.T) {
	candidate, baseline := efficiencyGateRuns(80, 100)
	candidate.Cases[0].Answer = "ANSWER_SECRET"
	candidate.Cases[0].Error = "ERROR_SECRET"
	candidate.Cases[0].Ledger.IncompleteReasons = []string{"adapter_error"}
	candidate.Cases[0].ToolCalls = []string{"TOOL_ARGS_SECRET"}
	result, err := EvaluateEfficiencyGate(candidate, baseline, DefaultEfficiencyGatePolicy())
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"ANSWER_SECRET", "ERROR_SECRET", "TOOL_ARGS_SECRET", "adapter_error", "dataset-revision-hash"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("gate result exposed %q: %s", forbidden, encoded)
		}
	}
}

func efficiencyGateRuns(candidateInput, baselineInput int64) (RunResult, RunResult) {
	metadata := map[string]string{
		EfficiencyCohortBenchmarkID:         "bench-v1",
		EfficiencyCohortTaskFamilyID:        "task-family-v1",
		EfficiencyCohortScenarioID:          "scenario-v1",
		EfficiencyCohortToolSurfaceRevision: "tools-v1",
		EfficiencyCohortContextConfigRev:    "context-v1",
		EfficiencyCohortProtocolRevision:    "protocol-v1",
		EfficiencyCohortProvider:            "provider-v1",
		EfficiencyCohortModelRevision:       "model-v1",
		EfficiencyCohortDevelopmentSet:      EfficiencyCohortHoldout,
	}
	now := time.Unix(1_700_000_000, 0).UTC()
	candidate := RunResult{
		ID: "evaluation-candidate", DatasetID: "efficiency.dataset", DatasetVersion: 1, DatasetRevision: strings.Repeat("a", 64),
		TenantID: "tenant", SubjectID: "subject", ProfileID: "efficiency.profile", Score: 1,
		Status: RunCompleted, Passed: true, TotalCases: 3, PassedCases: 3, Metadata: cloneEfficiencyMetadata(metadata), CreatedAt: now, CompletedAt: now,
		Cases: []CaseResult{efficiencyGateCase("case-1", candidateInput), efficiencyGateCase("case-2", candidateInput), efficiencyGateCase("case-3", candidateInput)},
	}
	baseline := RunResult{
		ID: "evaluation-baseline", DatasetID: "efficiency.dataset", DatasetVersion: 1, DatasetRevision: strings.Repeat("a", 64),
		TenantID: "tenant", SubjectID: "subject", ProfileID: "efficiency.profile", Score: 1,
		Status: RunCompleted, Passed: true, TotalCases: 3, PassedCases: 3, Metadata: cloneEfficiencyMetadata(metadata), CreatedAt: now, CompletedAt: now,
		Cases: []CaseResult{efficiencyGateCase("case-1", baselineInput), efficiencyGateCase("case-2", baselineInput), efficiencyGateCase("case-3", baselineInput)},
	}
	setEfficiencyRouteCandidate(&candidate, "ptc_only")
	setEfficiencyRouteCandidate(&baseline, "direct_only")
	candidate.BaselineRunID = baseline.ID
	return candidate, baseline
}

func setEfficiencyRouteCandidate(run *RunResult, mode string) {
	setEfficiencyRouteCandidateMetadata(run, efficiencyRouteMetadata(executionroute.RouteVersion, mode, executionroute.RouteImplementationRevision))
}

func setEfficiencyRouteCandidateV2(run *RunResult) {
	setEfficiencyRouteCandidateMetadata(run, efficiencyRouteMetadata(
		executionroute.RouteProbeVersion,
		string(programmatic.RouteAutoProbeOnce),
		executionroute.RouteProbeImplementationRevision,
	))
}

func efficiencyRouteMetadata(version, mode, implementation string) map[string]string {
	return map[string]string{
		"harness.executor.id":                      runexecutor.SequentialID,
		"harness.executor.version":                 runexecutor.SequentialVersion,
		"harness.executor.implementation_revision": runexecutor.SequentialImplementationRevision,
		executionroute.RouteVersionKey:             version,
		executionroute.RouteModeKey:                mode,
		executionroute.RouteCatalogToolIDKey:       "program.catalog",
		executionroute.RouteExecuteToolIDKey:       "program.execute",
		executionroute.RouteImplementationKey:      implementation,
	}
}

func setEfficiencyRouteCandidateMetadata(run *RunResult, metadata map[string]string) {
	revision, err := core.CompositionMetadataRevision(metadata)
	if err != nil {
		panic(err)
	}
	routeID, ok := canonicalEfficiencyRouteID(metadata)
	if !ok {
		panic("efficiency route metadata is invalid")
	}
	run.CompositionMetadata = metadata
	run.AssignmentRevision = revision
	run.Metadata[EfficiencyCohortRouteCandidateID] = routeID
	for index := range run.Cases {
		run.Cases[index].Artifacts.AssignmentRevision = revision
	}
}

func refreshEfficiencyCompositionRevision(run *RunResult) {
	revision, err := core.CompositionMetadataRevision(run.CompositionMetadata)
	if err != nil {
		panic(err)
	}
	run.AssignmentRevision = revision
	for index := range run.Cases {
		run.Cases[index].Artifacts.AssignmentRevision = revision
	}
}

func efficiencyGateCase(id string, input int64) CaseResult {
	output := int64(10)
	return CaseResult{
		CaseID: id, SessionID: "session-" + id, AgentRunID: "agent-" + id, Status: core.RunCompleted, Score: 1, CompletedAt: time.Unix(1_700_000_000, 0).UTC(), Passed: true,
		Artifacts: ArtifactSnapshot{ResolvedProvider: "provider-v1", ModelRevision: "model-v1", CapabilitySnapshotID: "tools-v1"},
		Ledger: &ExecutionLedger{
			Complete: true,
			ModelCalls: []ExecutionLedgerModelCall{{
				Step: 0, Invoked: true, Outcome: "ok", UsageReports: 1,
				Usage: &ExecutionLedgerUsage{InputTokens: input, OutputTokens: output},
			}},
			ContextAssemblies: []ExecutionLedgerContext{{ContextWindowTokens: 100, MaxOutputTokens: 10, InputBytes: 120, Outcome: "ok", InputTokens: 30, DroppedGroups: 1}},
			GateCalls:         []ExecutionLedgerGateCall{{Step: 0, Outcome: "accepted"}},
		},
		Evidence: &ExecutionEvidence{
			ReportedInputTokens: input, ReportedOutputTokens: output, UsageReports: 1,
			TopLevelToolCalls: 1,
		},
	}
}

func setEfficiencyCaseUsage(result *CaseResult, input, output int64) {
	result.Ledger.ModelCalls[0].Usage.InputTokens = input
	result.Ledger.ModelCalls[0].Usage.OutputTokens = output
	result.Evidence.ReportedInputTokens = input
	result.Evidence.ReportedOutputTokens = output
}

func cloneEfficiencyMetadata(source map[string]string) map[string]string {
	copy := make(map[string]string, len(source))
	for key, value := range source {
		copy[key] = value
	}
	return copy
}

func assertEfficiencyResult(t *testing.T, result EfficiencyGateResult, verdict EfficiencyGateVerdict, reason EfficiencyGateReasonCode) {
	t.Helper()
	if result.Verdict != verdict || len(result.ReasonCodes) != 1 || result.ReasonCodes[0] != reason {
		t.Fatalf("result = %#v, want verdict=%q reason=%q", result, verdict, reason)
	}
}
