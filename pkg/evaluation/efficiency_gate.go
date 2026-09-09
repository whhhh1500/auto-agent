package evaluation

import (
	"fmt"
	"math/big"
	"sort"
	"strings"

	"github.com/whhhh1500/auto-agent/internal/executionroute"
	core "github.com/whhhh1500/auto-agent/pkg/core"
)

// EfficiencyGateContractV1 is the only contract accepted by the offline
// efficiency gate. Later contracts must use a new identifier and evaluator.
const EfficiencyGateContractV1 = "efficiency-gate/v1"

const (
	// MaxEfficiencyGateBPS caps both allowed per-pair regression and required
	// aggregate reductions at 100 percent. This keeps comparisons bounded and
	// makes a policy's tolerance meaningful.
	MaxEfficiencyGateBPS int64 = 10_000
	// MaxEfficiencyGateAbsoluteDelta bounds a single resource tolerance. It is
	// intentionally independent of a provider's advertised limits because the
	// gate only consumes recorded evidence.
	MaxEfficiencyGateAbsoluteDelta int64 = 1_000_000_000
)

// EfficiencyGateVerdict is deliberately distinct from the legacy release
// GateResult. The offline gate does not alter existing release behaviour.
type EfficiencyGateVerdict string

const (
	EfficiencyGatePassed       EfficiencyGateVerdict = "passed"
	EfficiencyGateFailed       EfficiencyGateVerdict = "failed"
	EfficiencyGateInconclusive EfficiencyGateVerdict = "inconclusive"
	EfficiencyGateNotRequested EfficiencyGateVerdict = "not_requested"
)

// EfficiencyGateReasonCode is safe for APIs and audit logs. Values must never
// embed case input, answers, tool arguments, provider errors, or hashes.
type EfficiencyGateReasonCode string

const (
	EfficiencyReasonPolicyDisabled              EfficiencyGateReasonCode = "policy_disabled"
	EfficiencyReasonCandidateQualityFailed      EfficiencyGateReasonCode = "candidate_quality_failed"
	EfficiencyReasonBaselineQualityFailed       EfficiencyGateReasonCode = "baseline_quality_failed"
	EfficiencyReasonRunInvalid                  EfficiencyGateReasonCode = "run_invalid"
	EfficiencyReasonDatasetMismatch             EfficiencyGateReasonCode = "dataset_mismatch"
	EfficiencyReasonCaseSetIncomplete           EfficiencyGateReasonCode = "case_set_incomplete"
	EfficiencyReasonCaseSetMismatch             EfficiencyGateReasonCode = "case_set_mismatch"
	EfficiencyReasonLedgerMissing               EfficiencyGateReasonCode = "ledger_missing"
	EfficiencyReasonLedgerIncomplete            EfficiencyGateReasonCode = "ledger_incomplete"
	EfficiencyReasonModelCallsMissing           EfficiencyGateReasonCode = "model_calls_missing"
	EfficiencyReasonModelCallInvalid            EfficiencyGateReasonCode = "model_call_invalid"
	EfficiencyReasonContextIncomplete           EfficiencyGateReasonCode = "context_incomplete"
	EfficiencyReasonGateIncomplete              EfficiencyGateReasonCode = "gate_incomplete"
	EfficiencyReasonEvidenceMissing             EfficiencyGateReasonCode = "evidence_missing"
	EfficiencyReasonEvidenceUsageMismatch       EfficiencyGateReasonCode = "evidence_usage_mismatch"
	EfficiencyReasonEvidenceTokenMismatch       EfficiencyGateReasonCode = "evidence_token_mismatch"
	EfficiencyReasonToolResultNotOK             EfficiencyGateReasonCode = "tool_result_not_ok"
	EfficiencyReasonCohortMissing               EfficiencyGateReasonCode = "cohort_missing"
	EfficiencyReasonCohortMismatch              EfficiencyGateReasonCode = "cohort_mismatch"
	EfficiencyReasonRouteCandidateMissing       EfficiencyGateReasonCode = "route_candidate_missing"
	EfficiencyReasonRouteCandidateSame          EfficiencyGateReasonCode = "route_candidate_same"
	EfficiencyReasonRouteCandidateMismatch      EfficiencyGateReasonCode = "route_candidate_mismatch"
	EfficiencyReasonArtifactMismatch            EfficiencyGateReasonCode = "artifact_mismatch"
	EfficiencyReasonDevelopmentSet              EfficiencyGateReasonCode = "development_set"
	EfficiencyReasonPairResourceRegression      EfficiencyGateReasonCode = "pair_resource_regression"
	EfficiencyReasonSampleInsufficient          EfficiencyGateReasonCode = "sample_insufficient"
	EfficiencyReasonImprovementInsufficient     EfficiencyGateReasonCode = "improvement_insufficient"
	EfficiencyReasonMedianReductionInsufficient EfficiencyGateReasonCode = "median_reduction_insufficient"
	EfficiencyReasonNumericOverflow             EfficiencyGateReasonCode = "numeric_overflow"
)

// These metadata keys form the v1 comparison cohort. A candidate route may
// differ only in EfficiencyCohortRouteCandidateID; every other run metadata
// key and every listed cohort value must agree exactly between paired runs.
// ToolSurfaceRevision is cross-checked against every case's artifact snapshot.
// ContextConfigRev and ProtocolRevision are operator-declared frozen contracts:
// this package has no runtime artifact that can independently prove either.
const (
	EfficiencyCohortBenchmarkID         = "efficiency.benchmark_id"
	EfficiencyCohortTaskFamilyID        = "efficiency.task_family_id"
	EfficiencyCohortScenarioID          = "efficiency.scenario_id"
	EfficiencyCohortToolSurfaceRevision = "efficiency.tool_surface_revision"
	EfficiencyCohortContextConfigRev    = "efficiency.context_config_revision"
	EfficiencyCohortProtocolRevision    = "efficiency.protocol_revision"
	EfficiencyCohortProvider            = "efficiency.provider"
	EfficiencyCohortModelRevision       = "efficiency.model_revision"
	EfficiencyCohortDevelopmentSet      = "efficiency.development_set"
	EfficiencyCohortRouteCandidateID    = "efficiency.route_candidate_id"
	EfficiencyCohortDevelopment         = "development"
	EfficiencyCohortHoldout             = "holdout"
)

var efficiencyRequiredCohortKeys = []string{
	EfficiencyCohortBenchmarkID,
	EfficiencyCohortTaskFamilyID,
	EfficiencyCohortScenarioID,
	EfficiencyCohortToolSurfaceRevision,
	EfficiencyCohortContextConfigRev,
	EfficiencyCohortProtocolRevision,
	EfficiencyCohortProvider,
	EfficiencyCohortModelRevision,
	EfficiencyCohortDevelopmentSet,
}

// EfficiencyGatePolicy configures the standalone offline comparison.
// MinPaired is required for an enabled policy. MinImproved and
// MinMedianTokenReduction can be zero when equality is an acceptable result.
// AllowToolResultFailures relaxes the default strict requirement that every
// observed tool-result event was OK.
type EfficiencyGatePolicy struct {
	Enabled                   bool   `json:"enabled,omitempty"`
	ContractID                string `json:"contract_id,omitempty"`
	MaxPairRegressionBPS      int64  `json:"max_pair_regression_bps,omitempty"`
	MaxPairRegressionAbsolute int64  `json:"max_pair_regression_absolute,omitempty"`
	MinPaired                 int    `json:"min_paired,omitempty"`
	MinImproved               int    `json:"min_improved,omitempty"`
	MinMedianTokenReduction   int64  `json:"min_median_token_reduction,omitempty"`
	AllowToolResultFailures   bool   `json:"allow_tool_result_failures,omitempty"`
}

// DefaultEfficiencyGatePolicy requires three holdout pairs, two improved
// pairs, no resource regression, and a median reduction of at least one
// reported token. One pair is not enough to pass by default.
func DefaultEfficiencyGatePolicy() EfficiencyGatePolicy {
	return EfficiencyGatePolicy{
		Enabled:                 true,
		ContractID:              EfficiencyGateContractV1,
		MinPaired:               3,
		MinImproved:             2,
		MinMedianTokenReduction: 1,
	}
}

// EfficiencyGateResource is the content-free resource vector used for one
// paired case. Every field is derived from verified ledger/evidence counts.
type EfficiencyGateResource struct {
	ReportedInputTokens  int64 `json:"reported_input_tokens"`
	ReportedOutputTokens int64 `json:"reported_output_tokens"`
	ReportedTotalTokens  int64 `json:"reported_total_tokens"`
	ModelCalls           int64 `json:"model_calls"`
	ContextInputTokens   int64 `json:"context_input_tokens"`
	DroppedGroups        int64 `json:"dropped_groups"`
	TopLevelToolCalls    int64 `json:"top_level_tool_calls"`
}

// EfficiencyGatePair is a content-free, paired resource comparison. CaseID
// is the already-public evaluation case identifier; no case text is retained.
type EfficiencyGatePair struct {
	CaseID        string                 `json:"case_id"`
	Candidate     EfficiencyGateResource `json:"candidate"`
	Baseline      EfficiencyGateResource `json:"baseline"`
	NonInferior   bool                   `json:"non_inferior"`
	TokenImproved bool                   `json:"token_improved"`
}

// EfficiencyGateStratum keeps v1 extensible for future cohort partitioning.
// v1 emits one "run" stratum only. MedianTokenReduction is represented as an
// exact integer rational so no float, NaN, or rounding rule enters a verdict.
type EfficiencyGateStratum struct {
	ID                              string `json:"id"`
	PairCount                       int    `json:"pair_count"`
	ImprovedPairs                   int    `json:"improved_pairs"`
	CandidateTotalTokens            int64  `json:"candidate_total_tokens"`
	BaselineTotalTokens             int64  `json:"baseline_total_tokens"`
	MedianTokenReductionNumerator   int64  `json:"median_token_reduction_numerator"`
	MedianTokenReductionDenominator int64  `json:"median_token_reduction_denominator"`
}

// EfficiencyGateResult contains only fixed verdicts, fixed reason codes, and
// bounded numeric projections. It intentionally has no free-form message.
type EfficiencyGateResult struct {
	ContractID  string                     `json:"contract_id,omitempty"`
	Verdict     EfficiencyGateVerdict      `json:"verdict"`
	ReasonCodes []EfficiencyGateReasonCode `json:"reason_codes,omitempty"`
	Pairs       []EfficiencyGatePair       `json:"pairs,omitempty"`
	Strata      []EfficiencyGateStratum    `json:"strata,omitempty"`
}

// EvaluateEfficiencyGate evaluates an offline, strict candidate/baseline
// comparison. Invalid enabled policies return an error. Missing, malformed,
// incompatible, or incomplete evidence produces an inconclusive verdict;
// quality failures and demonstrated resource/benefit failures produce failed.
func EvaluateEfficiencyGate(candidate, baseline RunResult, policy EfficiencyGatePolicy) (EfficiencyGateResult, error) {
	if !policy.Enabled {
		return efficiencyResult(EfficiencyGateNotRequested, "", EfficiencyReasonPolicyDisabled), nil
	}
	if err := validateEfficiencyGatePolicy(policy); err != nil {
		return EfficiencyGateResult{}, err
	}

	if !runQualityPassed(candidate) {
		return efficiencyResult(EfficiencyGateFailed, policy.ContractID, EfficiencyReasonCandidateQualityFailed), nil
	}
	if !runQualityPassed(baseline) {
		return efficiencyResult(EfficiencyGateFailed, policy.ContractID, EfficiencyReasonBaselineQualityFailed), nil
	}
	if ValidateRunResult(candidate, true) != nil || ValidateRunResult(baseline, true) != nil {
		return efficiencyResult(EfficiencyGateInconclusive, policy.ContractID, EfficiencyReasonRunInvalid), nil
	}
	if candidate.ID == baseline.ID || candidate.BaselineRunID != baseline.ID {
		return efficiencyResult(EfficiencyGateInconclusive, policy.ContractID, EfficiencyReasonRunInvalid), nil
	}
	if !sameDataset(candidate, baseline) {
		return efficiencyResult(EfficiencyGateInconclusive, policy.ContractID, EfficiencyReasonDatasetMismatch), nil
	}
	candidateCases, reason := completeEfficiencyCases(candidate)
	if reason != "" {
		return efficiencyResult(EfficiencyGateInconclusive, policy.ContractID, EfficiencyReasonCaseSetIncomplete), nil
	}
	baselineCases, reason := completeEfficiencyCases(baseline)
	if reason != "" {
		return efficiencyResult(EfficiencyGateInconclusive, policy.ContractID, EfficiencyReasonCaseSetIncomplete), nil
	}
	if !sameEfficiencyCaseIDs(candidateCases, baselineCases) {
		return efficiencyResult(EfficiencyGateInconclusive, policy.ContractID, EfficiencyReasonCaseSetMismatch), nil
	}
	if cohortReason := validateEfficiencyCohort(candidate, baseline, candidateCases, baselineCases); cohortReason != "" {
		return efficiencyResult(EfficiencyGateInconclusive, policy.ContractID, cohortReason), nil
	}
	if routeReason := validateEfficiencyRouteCandidates(candidate, baseline, candidateCases, baselineCases); routeReason != "" {
		return efficiencyResult(EfficiencyGateInconclusive, policy.ContractID, routeReason), nil
	}
	if candidate.Metadata[EfficiencyCohortDevelopmentSet] != EfficiencyCohortHoldout {
		return efficiencyResult(EfficiencyGateInconclusive, policy.ContractID, EfficiencyReasonDevelopmentSet), nil
	}

	pairs := make([]EfficiencyGatePair, 0, len(candidateCases))
	caseIDs := make([]string, 0, len(candidateCases))
	for id := range candidateCases {
		caseIDs = append(caseIDs, id)
	}
	sort.Strings(caseIDs)
	for _, id := range caseIDs {
		candidateResource, resourceReason := efficiencyResource(candidateCases[id], policy)
		if resourceReason != "" {
			return efficiencyResult(EfficiencyGateInconclusive, policy.ContractID, resourceReason), nil
		}
		baselineResource, resourceReason := efficiencyResource(baselineCases[id], policy)
		if resourceReason != "" {
			return efficiencyResult(EfficiencyGateInconclusive, policy.ContractID, resourceReason), nil
		}
		nonInferior := resourceNonInferior(candidateResource, baselineResource, policy)
		pairs = append(pairs, EfficiencyGatePair{
			CaseID: id, Candidate: candidateResource, Baseline: baselineResource,
			NonInferior:   nonInferior,
			TokenImproved: candidateResource.ReportedTotalTokens < baselineResource.ReportedTotalTokens,
		})
	}
	for _, pair := range pairs {
		if !pair.NonInferior {
			return EfficiencyGateResult{ContractID: policy.ContractID, Verdict: EfficiencyGateFailed, ReasonCodes: []EfficiencyGateReasonCode{EfficiencyReasonPairResourceRegression}, Pairs: pairs}, nil
		}
	}
	if len(pairs) < policy.MinPaired || policy.MinImproved > len(pairs) {
		return EfficiencyGateResult{ContractID: policy.ContractID, Verdict: EfficiencyGateInconclusive, ReasonCodes: []EfficiencyGateReasonCode{EfficiencyReasonSampleInsufficient}, Pairs: pairs}, nil
	}

	stratum, stratumReason := summarizeEfficiencyPairs(pairs)
	if stratumReason != "" {
		return EfficiencyGateResult{ContractID: policy.ContractID, Verdict: EfficiencyGateInconclusive, ReasonCodes: []EfficiencyGateReasonCode{stratumReason}, Pairs: pairs}, nil
	}
	result := EfficiencyGateResult{ContractID: policy.ContractID, Pairs: pairs, Strata: []EfficiencyGateStratum{stratum}}
	if stratum.ImprovedPairs < policy.MinImproved {
		result.Verdict = EfficiencyGateFailed
		result.ReasonCodes = []EfficiencyGateReasonCode{EfficiencyReasonImprovementInsufficient}
		return result, nil
	}
	if !medianAtLeast(stratum.MedianTokenReductionNumerator, stratum.MedianTokenReductionDenominator, policy.MinMedianTokenReduction) {
		result.Verdict = EfficiencyGateFailed
		result.ReasonCodes = []EfficiencyGateReasonCode{EfficiencyReasonMedianReductionInsufficient}
		return result, nil
	}
	result.Verdict = EfficiencyGatePassed
	return result, nil
}

func validateEfficiencyGatePolicy(policy EfficiencyGatePolicy) error {
	if strings.TrimSpace(policy.ContractID) == "" {
		return fmt.Errorf("efficiency gate contract id is required")
	}
	if policy.ContractID != EfficiencyGateContractV1 {
		return fmt.Errorf("unknown efficiency gate contract")
	}
	if policy.MaxPairRegressionBPS < 0 || policy.MaxPairRegressionBPS > MaxEfficiencyGateBPS {
		return fmt.Errorf("efficiency gate max pair regression bps is outside bounds")
	}
	if policy.MaxPairRegressionAbsolute < 0 || policy.MaxPairRegressionAbsolute > MaxEfficiencyGateAbsoluteDelta {
		return fmt.Errorf("efficiency gate max pair regression absolute is outside bounds")
	}
	if policy.MinPaired < 1 || policy.MinPaired > MaxDatasetCases {
		return fmt.Errorf("efficiency gate minimum paired cases is outside bounds")
	}
	if policy.MinImproved < 0 || policy.MinImproved > MaxDatasetCases {
		return fmt.Errorf("efficiency gate minimum improved cases is outside bounds")
	}
	if policy.MinMedianTokenReduction < 0 || policy.MinMedianTokenReduction > MaxEfficiencyGateAbsoluteDelta {
		return fmt.Errorf("efficiency gate minimum median token reduction is outside bounds")
	}
	return nil
}

func efficiencyResult(verdict EfficiencyGateVerdict, contract string, reason EfficiencyGateReasonCode) EfficiencyGateResult {
	return EfficiencyGateResult{ContractID: contract, Verdict: verdict, ReasonCodes: []EfficiencyGateReasonCode{reason}}
}

func runQualityPassed(run RunResult) bool {
	if run.Status != RunCompleted || !run.Passed {
		return false
	}
	for _, result := range run.Cases {
		if !result.Passed {
			return false
		}
	}
	return true
}

func sameDataset(candidate, baseline RunResult) bool {
	return candidate.DatasetID != "" && candidate.DatasetVersion > 0 && candidate.DatasetRevision != "" &&
		candidate.DatasetID == baseline.DatasetID && candidate.DatasetVersion == baseline.DatasetVersion && candidate.DatasetRevision == baseline.DatasetRevision
}

func completeEfficiencyCases(run RunResult) (map[string]CaseResult, string) {
	if run.TotalCases < 1 || run.TotalCases != len(run.Cases) || run.PassedCases != run.TotalCases {
		return nil, "incomplete"
	}
	cases := make(map[string]CaseResult, len(run.Cases))
	for _, result := range run.Cases {
		if !caseIDPattern.MatchString(result.CaseID) || !result.Passed {
			return nil, "incomplete"
		}
		if _, exists := cases[result.CaseID]; exists {
			return nil, "incomplete"
		}
		cases[result.CaseID] = result
	}
	return cases, ""
}

func sameEfficiencyCaseIDs(candidate, baseline map[string]CaseResult) bool {
	if len(candidate) != len(baseline) {
		return false
	}
	for id := range candidate {
		if _, ok := baseline[id]; !ok {
			return false
		}
	}
	return true
}

func validateEfficiencyCohort(candidate, baseline RunResult, candidateCases, baselineCases map[string]CaseResult) EfficiencyGateReasonCode {
	for _, key := range efficiencyRequiredCohortKeys {
		candidateValue, candidateOK := candidate.Metadata[key]
		baselineValue, baselineOK := baseline.Metadata[key]
		if !candidateOK || !baselineOK || strings.TrimSpace(candidateValue) == "" || strings.TrimSpace(baselineValue) == "" {
			return EfficiencyReasonCohortMissing
		}
		if candidateValue != baselineValue {
			return EfficiencyReasonCohortMismatch
		}
	}
	developmentSet := candidate.Metadata[EfficiencyCohortDevelopmentSet]
	if developmentSet != EfficiencyCohortDevelopment && developmentSet != EfficiencyCohortHoldout {
		return EfficiencyReasonCohortMissing
	}
	if !sameCohortMetadata(candidate.Metadata, baseline.Metadata) {
		return EfficiencyReasonCohortMismatch
	}
	for _, cases := range []map[string]CaseResult{candidateCases, baselineCases} {
		for _, result := range cases {
			if result.Artifacts.ResolvedProvider != candidate.Metadata[EfficiencyCohortProvider] ||
				result.Artifacts.ModelRevision != candidate.Metadata[EfficiencyCohortModelRevision] ||
				result.Artifacts.CapabilitySnapshotID == "" ||
				result.Artifacts.CapabilitySnapshotID != candidate.Metadata[EfficiencyCohortToolSurfaceRevision] {
				return EfficiencyReasonArtifactMismatch
			}
		}
	}
	return ""
}

func sameCohortMetadata(candidate, baseline map[string]string) bool {
	for key, value := range candidate {
		if key == EfficiencyCohortRouteCandidateID {
			continue
		}
		baselineValue, ok := baseline[key]
		if !ok || value != baselineValue {
			return false
		}
	}
	for key := range baseline {
		if key == EfficiencyCohortRouteCandidateID {
			continue
		}
		if _, ok := candidate[key]; !ok {
			return false
		}
	}
	return true
}

// validateEfficiencyRouteCandidates rejects operator labels that are missing,
// identical, or unrelated to the frozen route projection. The ID is the
// canonical revision of exactly the route fields; the run assignment revision
// then binds that route projection to the entire frozen composition recorded
// by every paired case artifact.
func validateEfficiencyRouteCandidates(candidate, baseline RunResult, candidateCases, baselineCases map[string]CaseResult) EfficiencyGateReasonCode {
	candidateID, candidateReason := canonicalEfficiencyRouteCandidateID(candidate, candidateCases)
	if candidateReason != "" {
		return candidateReason
	}
	baselineID, baselineReason := canonicalEfficiencyRouteCandidateID(baseline, baselineCases)
	if baselineReason != "" {
		return baselineReason
	}
	if candidateID == baselineID {
		return EfficiencyReasonRouteCandidateSame
	}
	if !sameEfficiencyCompositionMetadata(candidate.CompositionMetadata, baseline.CompositionMetadata) {
		return EfficiencyReasonRouteCandidateMismatch
	}
	return ""
}

// sameEfficiencyCompositionMetadata allows only the five route selection
// fields to differ. Every other frozen composition field can affect execution
// and therefore must be present with the exact same value in both runs. Map
// membership is checked independently from the value so missing and empty are
// never treated as equivalent.
func sameEfficiencyCompositionMetadata(candidate, baseline map[string]string) bool {
	if candidate == nil || baseline == nil {
		return false
	}
	for key, candidateValue := range candidate {
		if isEfficiencyRouteMetadataKey(key) {
			continue
		}
		baselineValue, found := baseline[key]
		if !found || candidateValue != baselineValue {
			return false
		}
	}
	for key := range baseline {
		if isEfficiencyRouteMetadataKey(key) {
			continue
		}
		if _, found := candidate[key]; !found {
			return false
		}
	}
	return true
}

func isEfficiencyRouteMetadataKey(key string) bool {
	switch key {
	case executionroute.RouteVersionKey,
		executionroute.RouteModeKey,
		executionroute.RouteCatalogToolIDKey,
		executionroute.RouteExecuteToolIDKey,
		executionroute.RouteImplementationKey:
		return true
	default:
		return false
	}
}

func canonicalEfficiencyRouteCandidateID(run RunResult, cases map[string]CaseResult) (string, EfficiencyGateReasonCode) {
	rawID, found := run.Metadata[EfficiencyCohortRouteCandidateID]
	if !found || strings.TrimSpace(rawID) == "" {
		return "", EfficiencyReasonRouteCandidateMissing
	}
	canonicalRouteID, ok := canonicalEfficiencyRouteID(run.CompositionMetadata)
	if !ok || rawID != canonicalRouteID {
		return "", EfficiencyReasonRouteCandidateMismatch
	}
	assignmentRevision, err := core.CompositionMetadataRevision(run.CompositionMetadata)
	if err != nil || assignmentRevision == "" || run.AssignmentRevision != assignmentRevision {
		return "", EfficiencyReasonRouteCandidateMismatch
	}
	for _, result := range cases {
		if result.Artifacts.AssignmentRevision != assignmentRevision {
			return "", EfficiencyReasonRouteCandidateMismatch
		}
	}
	return canonicalRouteID, ""
}

func canonicalEfficiencyRouteID(metadata map[string]string) (string, bool) {
	route := map[string]string{}
	for _, key := range []string{
		executionroute.RouteVersionKey,
		executionroute.RouteModeKey,
		executionroute.RouteCatalogToolIDKey,
		executionroute.RouteExecuteToolIDKey,
		executionroute.RouteImplementationKey,
	} {
		value, found := metadata[key]
		if !found || value == "" || strings.TrimSpace(value) != value {
			return "", false
		}
		route[key] = value
	}
	if !efficiencyRouteVersionKnown(route) ||
		route[executionroute.RouteCatalogToolIDKey] == route[executionroute.RouteExecuteToolIDKey] ||
		core.ValidateNamespacedID(route[executionroute.RouteCatalogToolIDKey]) != nil ||
		core.ValidateNamespacedID(route[executionroute.RouteExecuteToolIDKey]) != nil {
		return "", false
	}
	revision, err := core.CompositionMetadataRevision(route)
	return revision, err == nil && revision != ""
}

// efficiencyRouteVersionKnown accepts only frozen route metadata that the
// shared resolver itself supports. Each route version keeps its own exact mode
// and implementation revision check so accepting v2 cannot relax v1.
func efficiencyRouteVersionKnown(route map[string]string) bool {
	return executionroute.SupportsRouteIdentity(
		route[executionroute.RouteVersionKey],
		route[executionroute.RouteModeKey],
		route[executionroute.RouteImplementationKey],
	)
}

func efficiencyResource(result CaseResult, policy EfficiencyGatePolicy) (EfficiencyGateResource, EfficiencyGateReasonCode) {
	if result.Ledger == nil {
		return EfficiencyGateResource{}, EfficiencyReasonLedgerMissing
	}
	ledger := result.Ledger
	if !ledger.Complete {
		return EfficiencyGateResource{}, EfficiencyReasonLedgerIncomplete
	}
	if len(ledger.ModelCalls) == 0 {
		return EfficiencyGateResource{}, EfficiencyReasonModelCallsMissing
	}
	modelSteps := make(map[int]bool, len(ledger.ModelCalls))
	var inputTokens, outputTokens, contextInputTokens, droppedGroups int64
	for _, call := range ledger.ModelCalls {
		if call.Step < 0 || modelSteps[call.Step] || !call.Invoked || call.Outcome != "ok" || call.UsageReports != 1 || call.Usage == nil || call.Usage.InputTokens < 0 || call.Usage.OutputTokens < 0 {
			return EfficiencyGateResource{}, EfficiencyReasonModelCallInvalid
		}
		modelSteps[call.Step] = true
		var ok bool
		if inputTokens, ok = addNonNegative(inputTokens, call.Usage.InputTokens); !ok {
			return EfficiencyGateResource{}, EfficiencyReasonNumericOverflow
		}
		if outputTokens, ok = addNonNegative(outputTokens, call.Usage.OutputTokens); !ok {
			return EfficiencyGateResource{}, EfficiencyReasonNumericOverflow
		}
	}
	if len(ledger.ContextAssemblies) != len(ledger.ModelCalls) {
		return EfficiencyGateResource{}, EfficiencyReasonContextIncomplete
	}
	for _, context := range ledger.ContextAssemblies {
		if context.Outcome != "ok" || context.InputTokens < 0 || context.DroppedGroups < 0 {
			return EfficiencyGateResource{}, EfficiencyReasonContextIncomplete
		}
		var ok bool
		if contextInputTokens, ok = addNonNegative(contextInputTokens, context.InputTokens); !ok {
			return EfficiencyGateResource{}, EfficiencyReasonNumericOverflow
		}
		if droppedGroups, ok = addNonNegative(droppedGroups, int64(context.DroppedGroups)); !ok {
			return EfficiencyGateResource{}, EfficiencyReasonNumericOverflow
		}
	}
	if len(ledger.GateCalls) != 0 {
		if len(ledger.GateCalls) != len(ledger.ModelCalls) {
			return EfficiencyGateResource{}, EfficiencyReasonGateIncomplete
		}
		gateSteps := make(map[int]bool, len(ledger.GateCalls))
		for _, gate := range ledger.GateCalls {
			if gate.Step < 0 || gateSteps[gate.Step] || gate.Outcome != "accepted" || !modelSteps[gate.Step] {
				return EfficiencyGateResource{}, EfficiencyReasonGateIncomplete
			}
			gateSteps[gate.Step] = true
		}
		for step := range modelSteps {
			if !gateSteps[step] {
				return EfficiencyGateResource{}, EfficiencyReasonGateIncomplete
			}
		}
	}
	if result.Evidence == nil {
		return EfficiencyGateResource{}, EfficiencyReasonEvidenceMissing
	}
	evidence := result.Evidence
	if evidence.NoUsageReported || evidence.UsageReports != len(ledger.ModelCalls) || evidence.ReportedInputTokens < 0 || evidence.ReportedOutputTokens < 0 || evidence.StepsStarted < 0 || evidence.StepsEnded < 0 || evidence.TopLevelToolCalls < 0 || evidence.ToolResultEventsOK < 0 || evidence.ToolResultEventsNotOK < 0 {
		return EfficiencyGateResource{}, EfficiencyReasonEvidenceUsageMismatch
	}
	if evidence.ReportedInputTokens != inputTokens || evidence.ReportedOutputTokens != outputTokens {
		return EfficiencyGateResource{}, EfficiencyReasonEvidenceTokenMismatch
	}
	if !policy.AllowToolResultFailures && evidence.ToolResultEventsNotOK != 0 {
		return EfficiencyGateResource{}, EfficiencyReasonToolResultNotOK
	}
	totalTokens, ok := addNonNegative(inputTokens, outputTokens)
	if !ok {
		return EfficiencyGateResource{}, EfficiencyReasonNumericOverflow
	}
	return EfficiencyGateResource{
		ReportedInputTokens: inputTokens, ReportedOutputTokens: outputTokens, ReportedTotalTokens: totalTokens,
		ModelCalls: int64(len(ledger.ModelCalls)), ContextInputTokens: contextInputTokens,
		DroppedGroups: droppedGroups, TopLevelToolCalls: int64(evidence.TopLevelToolCalls),
	}, ""
}

func addNonNegative(left, right int64) (int64, bool) {
	if left < 0 || right < 0 || left > int64(^uint64(0)>>1)-right {
		return 0, false
	}
	return left + right, true
}

func resourceNonInferior(candidate, baseline EfficiencyGateResource, policy EfficiencyGatePolicy) bool {
	for _, values := range [][2]int64{
		{candidate.ReportedInputTokens, baseline.ReportedInputTokens},
		{candidate.ReportedOutputTokens, baseline.ReportedOutputTokens},
		{candidate.ReportedTotalTokens, baseline.ReportedTotalTokens},
		{candidate.ModelCalls, baseline.ModelCalls},
		{candidate.ContextInputTokens, baseline.ContextInputTokens},
		{candidate.DroppedGroups, baseline.DroppedGroups},
		{candidate.TopLevelToolCalls, baseline.TopLevelToolCalls},
	} {
		if !withinResourceTolerance(values[0], values[1], policy.MaxPairRegressionBPS, policy.MaxPairRegressionAbsolute) {
			return false
		}
	}
	return true
}

func withinResourceTolerance(candidate, baseline, bps, absolute int64) bool {
	var left, right, scale, allowed big.Int
	scale.SetInt64(MaxEfficiencyGateBPS)
	left.Mul(big.NewInt(candidate), &scale)
	allowed.SetInt64(MaxEfficiencyGateBPS + bps)
	right.Mul(big.NewInt(baseline), &allowed)
	allowed.SetInt64(absolute)
	allowed.Mul(&allowed, &scale)
	right.Add(&right, &allowed)
	return left.Cmp(&right) <= 0
}

func summarizeEfficiencyPairs(pairs []EfficiencyGatePair) (EfficiencyGateStratum, EfficiencyGateReasonCode) {
	stratum := EfficiencyGateStratum{ID: "run", PairCount: len(pairs)}
	reductions := make([]int64, 0, len(pairs))
	for _, pair := range pairs {
		var ok bool
		if stratum.CandidateTotalTokens, ok = addNonNegative(stratum.CandidateTotalTokens, pair.Candidate.ReportedTotalTokens); !ok {
			return EfficiencyGateStratum{}, EfficiencyReasonNumericOverflow
		}
		if stratum.BaselineTotalTokens, ok = addNonNegative(stratum.BaselineTotalTokens, pair.Baseline.ReportedTotalTokens); !ok {
			return EfficiencyGateStratum{}, EfficiencyReasonNumericOverflow
		}
		if pair.TokenImproved {
			stratum.ImprovedPairs++
		}
		reductions = append(reductions, pair.Baseline.ReportedTotalTokens-pair.Candidate.ReportedTotalTokens)
	}
	sort.Slice(reductions, func(i, j int) bool { return reductions[i] < reductions[j] })
	if len(reductions)%2 == 1 {
		stratum.MedianTokenReductionNumerator = reductions[len(reductions)/2]
		stratum.MedianTokenReductionDenominator = 1
		return stratum, ""
	}
	median := big.NewInt(reductions[len(reductions)/2-1])
	median.Add(median, big.NewInt(reductions[len(reductions)/2]))
	if !median.IsInt64() {
		return EfficiencyGateStratum{}, EfficiencyReasonNumericOverflow
	}
	stratum.MedianTokenReductionNumerator = median.Int64()
	stratum.MedianTokenReductionDenominator = 2
	return stratum, ""
}

func medianAtLeast(numerator, denominator, minimum int64) bool {
	if denominator <= 0 {
		return false
	}
	var left, right big.Int
	left.SetInt64(numerator)
	right.Mul(big.NewInt(minimum), big.NewInt(denominator))
	return left.Cmp(&right) >= 0
}
