// Package evaluation provides deterministic, provider-neutral regression
// evaluation over isolated harness Sessions.
package evaluation

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"time"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

const (
	MaxDatasetCases          = 1000
	MaxCaseAssertions        = 64
	MaxCaseContext           = 256
	MaxEvaluationText        = 1 << 20
	MaxEvaluationMetadata    = 64 << 10
	MaxDatasetBytes          = 64 << 20
	MaxMetadataKeyBytes      = 128
	MaxMetadataValueBytes    = 4096
	MaxRunningEvaluationRuns = 32
	MaxDatasetVersionsPerID  = 64
	MaxDatasetIDs            = 256
	MaxEvaluationRuns        = 4096
)

var caseIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

type AssertionKind string

const (
	AssertRunStatus      AssertionKind = "run_status"
	AssertAnswerExact    AssertionKind = "answer_exact"
	AssertAnswerContains AssertionKind = "answer_contains"
	AssertAnswerJSON     AssertionKind = "answer_json_schema"
	AssertToolCalled     AssertionKind = "tool_called"
	AssertToolNotCalled  AssertionKind = "tool_not_called"
)

type ContextMessage struct {
	Role core.ChatRole `json:"role"`
	Text string        `json:"text"`
}

type Assertion struct {
	ID             string            `json:"id"`
	Kind           AssertionKind     `json:"kind"`
	Weight         float64           `json:"weight,omitempty"`
	Expected       string            `json:"expected,omitempty"`
	ExpectedStatus core.RunStatus    `json:"expected_status,omitempty"`
	CapabilityID   string            `json:"capability_id,omitempty"`
	Schema         map[string]any    `json:"schema,omitempty"`
	CaseSensitive  bool              `json:"case_sensitive,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
}

type Case struct {
	ID            string           `json:"id"`
	Input         string           `json:"input"`
	ProfileID     string           `json:"profile_id,omitempty"`
	Context       []ContextMessage `json:"context,omitempty"`
	Assertions    []Assertion      `json:"assertions"`
	PassThreshold float64          `json:"pass_threshold,omitempty"`
	// Coverage is an optional, versioned task contract. When present it is
	// normalized before the containing Dataset revision is calculated.
	Coverage *CoverageContract `json:"coverage,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type Dataset struct {
	ID            string            `json:"id"`
	Version       int               `json:"version"`
	Name          string            `json:"name"`
	Description   string            `json:"description,omitempty"`
	ProfileID     string            `json:"profile_id"`
	PassThreshold float64           `json:"pass_threshold,omitempty"`
	Cases         []Case            `json:"cases"`
	Metadata      map[string]string `json:"metadata,omitempty"`
	Revision      string            `json:"revision"`
	CreatedAt     time.Time         `json:"created_at,omitempty"`
}

type DatasetSummary struct {
	ID        string    `json:"id"`
	Version   int       `json:"version"`
	Revision  string    `json:"revision"`
	Name      string    `json:"name"`
	ProfileID string    `json:"profile_id"`
	CaseCount int       `json:"case_count"`
	CreatedAt time.Time `json:"created_at"`
}

type AssertionResult struct {
	AssertionID string        `json:"assertion_id"`
	Kind        AssertionKind `json:"kind"`
	Score       float64       `json:"score"`
	Passed      bool          `json:"passed"`
	Message     string        `json:"message,omitempty"`
}

type ArtifactSnapshot struct {
	ProfileSnapshotID    string `json:"profile_snapshot_id,omitempty"`
	CapabilitySnapshotID string `json:"capability_snapshot_id,omitempty"`
	ResolvedProvider     string `json:"resolved_provider,omitempty"`
	ModelRevision        string `json:"model_revision,omitempty"`
	CompositionRevision  string `json:"composition_revision,omitempty"`
	AssignmentRevision   string `json:"assignment_revision,omitempty"`
}

// ExecutionEvidence is a bounded, read-only projection of one case Session's
// durable events. It reports model and summary usage contributions, but it
// does not infer provider requests, execution strategy, nested-tool shape, or
// external side effects. NoUsageReported only records that no EvRunUsage event
// was observed; false does not establish complete provider accounting.
type ExecutionEvidence struct {
	ReportedInputTokens  int64 `json:"reported_input_tokens"`
	ReportedOutputTokens int64 `json:"reported_output_tokens"`
	// UsageReports counts durable EvRunUsage events, including summaries.
	UsageReports    int  `json:"usage_reports"`
	NoUsageReported bool `json:"no_usage_reported"`
	// StepsStarted and StepsEnded count lifecycle events, not model or network calls.
	StepsStarted int `json:"steps_started"`
	StepsEnded   int `json:"steps_ended"`
	// TopLevelToolCalls counts distinct IDs declared in assistant events. This
	// prevents the legacy ToolCall and ToolCalls aliases from being double-counted.
	TopLevelToolCalls int `json:"top_level_tool_calls"`
	// ToolResultEventsOK and ToolResultEventsNotOK count result events. They do
	// not establish that an external tool side effect occurred or was repeated.
	ToolResultEventsOK    int `json:"tool_result_events_ok"`
	ToolResultEventsNotOK int `json:"tool_result_events_not_ok"`
}

type CaseResult struct {
	CaseID     string             `json:"case_id"`
	SessionID  string             `json:"session_id"`
	AgentRunID string             `json:"agent_run_id"`
	Status     core.RunStatus     `json:"status"`
	Answer     string             `json:"answer,omitempty"`
	Error      string             `json:"error,omitempty"`
	Score      float64            `json:"score"`
	Passed     bool               `json:"passed"`
	Assertions []AssertionResult  `json:"assertions"`
	Artifacts  ArtifactSnapshot   `json:"artifacts"`
	Evidence   *ExecutionEvidence `json:"execution_evidence,omitempty"`
	// Ledger is absent for historical case JSON where this evidence was not
	// collected. Current evaluation runs populate a content-free projection.
	Ledger *ExecutionLedger `json:"execution_ledger,omitempty"`
	// Coverage is an optional content-free host verification projection. It is
	// absent in historical case JSON and is not inferred from legacy evidence.
	Coverage    *CoverageEvidence `json:"coverage,omitempty"`
	DurationMS  int64             `json:"duration_ms"`
	ToolCalls   []string          `json:"tool_calls,omitempty"`
	CompletedAt time.Time         `json:"completed_at"`
}

// ExecutionLedger never contains prompt/messages, tools or arguments, model
// results, error text, or hashes. Complete is false when collection or strict
// durable model usage reconciliation could not establish full evidence.
type ExecutionLedger struct {
	Complete          bool                       `json:"complete"`
	IncompleteReasons []string                   `json:"incomplete_reasons,omitempty"`
	ModelCalls        []ExecutionLedgerModelCall `json:"model_calls,omitempty"`
	ContextAssemblies []ExecutionLedgerContext   `json:"context_assemblies,omitempty"`
	GateCalls         []ExecutionLedgerGateCall  `json:"gate_calls,omitempty"`
}
type ExecutionLedgerUsage struct {
	InputTokens  int64 `json:"input_tokens"`
	OutputTokens int64 `json:"output_tokens"`
}
type ExecutionLedgerModelCall struct {
	Step         int                   `json:"step"`
	Invoked      bool                  `json:"invoked"`
	Outcome      string                `json:"outcome"`
	Usage        *ExecutionLedgerUsage `json:"usage,omitempty"`
	UsageReports int                   `json:"usage_reports"`
}
type ExecutionLedgerContext struct {
	ContextWindowTokens int    `json:"context_window_tokens"`
	MaxOutputTokens     int    `json:"max_output_tokens"`
	InputBytes          int64  `json:"input_bytes"`
	InputTokens         int64  `json:"input_tokens"`
	DroppedGroups       int    `json:"dropped_groups"`
	Outcome             string `json:"outcome"`
}
type ExecutionLedgerGateCall struct {
	Step    int    `json:"step"`
	Outcome string `json:"outcome"`
}

type RunStatus string

const (
	RunRunning   RunStatus = "running"
	RunCompleted RunStatus = "completed"
	RunFailed    RunStatus = "failed"
)

type RunResult struct {
	ID                  string            `json:"id"`
	DatasetID           string            `json:"dataset_id"`
	DatasetVersion      int               `json:"dataset_version"`
	DatasetRevision     string            `json:"dataset_revision"`
	TenantID            string            `json:"tenant_id"`
	SubjectID           string            `json:"subject_id"`
	ProfileID           string            `json:"profile_id"`
	BaselineRunID       string            `json:"baseline_run_id,omitempty"`
	AssignmentRevision  string            `json:"assignment_revision,omitempty"`
	Status              RunStatus         `json:"status"`
	Score               float64           `json:"score"`
	Passed              bool              `json:"passed"`
	TotalCases          int               `json:"total_cases"`
	PassedCases         int               `json:"passed_cases"`
	Cases               []CaseResult      `json:"cases,omitempty"`
	AllowCapabilities   []string          `json:"allow_capabilities,omitempty"`
	CompositionMetadata map[string]string `json:"composition_metadata,omitempty"`
	Metadata            map[string]string `json:"metadata,omitempty"`
	Error               string            `json:"error,omitempty"`
	CreatedAt           time.Time         `json:"created_at"`
	CompletedAt         time.Time         `json:"completed_at,omitempty"`
}

type Comparison struct {
	CurrentRunID  string             `json:"current_run_id"`
	BaselineRunID string             `json:"baseline_run_id"`
	ScoreDelta    float64            `json:"score_delta"`
	Regressed     bool               `json:"regressed"`
	Tolerance     float64            `json:"tolerance"`
	CaseDeltas    map[string]float64 `json:"case_deltas,omitempty"`
}

type GatePolicy struct {
	RequirePassed bool `json:"require_passed,omitempty"`
	// RequireAllCases applies a stricter release-style rule without changing
	// RequirePassed's dataset-average threshold semantics.
	RequireAllCases bool     `json:"require_all_cases,omitempty"`
	MinScore        *float64 `json:"min_score,omitempty"`
	MaxRegression   *float64 `json:"max_regression,omitempty"`
}

type GateResult struct {
	Passed                  bool                           `json:"passed"`
	Reasons                 []string                       `json:"reasons,omitempty"`
	Comparison              *Comparison                    `json:"comparison,omitempty"`
	CapabilityCompatibility *CapabilityCompatibilityResult `json:"capability_compatibility,omitempty"`
	// Efficiency is populated only by release surfaces that explicitly opt in
	// to the independent efficiency comparison. It remains separate from the
	// legacy quality verdict so existing callers retain their prior semantics.
	Efficiency *EfficiencyGateResult `json:"efficiency,omitempty"`
	// RequiredEfficiencyContract records an enabled release efficiency policy
	// in a durable gate artifact. A non-empty value is both the required marker
	// and the contract expected from Efficiency during canary restoration.
	RequiredEfficiencyContract string `json:"required_efficiency_contract,omitempty"`
}

func ValidateDataset(dataset *Dataset) error {
	if dataset == nil {
		return fmt.Errorf("evaluation dataset is nil")
	}
	if err := core.ValidateNamespacedID(dataset.ID); err != nil {
		return fmt.Errorf("dataset id: %w", err)
	}
	if dataset.Version < 1 {
		return fmt.Errorf("dataset version must be positive")
	}
	if strings.TrimSpace(dataset.Name) == "" || len(dataset.Name) > 256 {
		return fmt.Errorf("dataset name is empty or too long")
	}
	if len(dataset.Description) > MaxEvaluationText {
		return fmt.Errorf("dataset description is too large")
	}
	if err := core.ValidateNamespacedID(dataset.ProfileID); err != nil {
		return fmt.Errorf("dataset profile id: %w", err)
	}
	if len(dataset.Cases) == 0 || len(dataset.Cases) > MaxDatasetCases {
		return fmt.Errorf("dataset cases must be between 1 and %d", MaxDatasetCases)
	}
	dataset.PassThreshold = normalizedThreshold(dataset.PassThreshold)
	if dataset.PassThreshold < 0 || dataset.PassThreshold > 1 {
		return fmt.Errorf("dataset pass threshold must be between 0 and 1")
	}
	if err := validateMetadata(dataset.Metadata); err != nil {
		return fmt.Errorf("dataset metadata: %w", err)
	}
	seen := map[string]bool{}
	for index := range dataset.Cases {
		if err := validateCase(&dataset.Cases[index], dataset.ProfileID); err != nil {
			return fmt.Errorf("case %d: %w", index, err)
		}
		if seen[dataset.Cases[index].ID] {
			return fmt.Errorf("duplicate case id %q", dataset.Cases[index].ID)
		}
		seen[dataset.Cases[index].ID] = true
	}
	revision, err := DatasetRevision(*dataset)
	if err != nil {
		return err
	}
	if dataset.Revision != "" && dataset.Revision != revision {
		return fmt.Errorf("dataset revision does not match definition")
	}
	dataset.Revision = revision
	return nil
}

func validateCase(evalCase *Case, defaultProfile string) error {
	if !caseIDPattern.MatchString(evalCase.ID) {
		return fmt.Errorf("case id %q is invalid", evalCase.ID)
	}
	if strings.TrimSpace(evalCase.Input) == "" || len(evalCase.Input) > MaxEvaluationText {
		return fmt.Errorf("case input is empty or too large")
	}
	if evalCase.ProfileID == "" {
		evalCase.ProfileID = defaultProfile
	}
	if err := core.ValidateNamespacedID(evalCase.ProfileID); err != nil {
		return fmt.Errorf("case profile id: %w", err)
	}
	if len(evalCase.Context) > MaxCaseContext {
		return fmt.Errorf("case context exceeds %d messages", MaxCaseContext)
	}
	for _, message := range evalCase.Context {
		if message.Role != core.RoleUser && message.Role != core.RoleAssistant {
			return fmt.Errorf("case context role %q is not supported", message.Role)
		}
		if strings.TrimSpace(message.Text) == "" || len(message.Text) > MaxEvaluationText {
			return fmt.Errorf("case context contains empty or oversized text")
		}
	}
	if len(evalCase.Assertions) == 0 || len(evalCase.Assertions) > MaxCaseAssertions {
		return fmt.Errorf("case assertions must be between 1 and %d", MaxCaseAssertions)
	}
	evalCase.PassThreshold = normalizedThreshold(evalCase.PassThreshold)
	if evalCase.PassThreshold < 0 || evalCase.PassThreshold > 1 {
		return fmt.Errorf("case pass threshold must be between 0 and 1")
	}
	assertions := map[string]bool{}
	for index := range evalCase.Assertions {
		assertion := &evalCase.Assertions[index]
		if assertion.ID == "" {
			assertion.ID = fmt.Sprintf("assert-%d", index+1)
		}
		if !caseIDPattern.MatchString(assertion.ID) || assertions[assertion.ID] {
			return fmt.Errorf("assertion id %q is invalid or duplicate", assertion.ID)
		}
		assertions[assertion.ID] = true
		if assertion.Weight == 0 {
			assertion.Weight = 1
		}
		if assertion.Weight < 0 || assertion.Weight > 1000 {
			return fmt.Errorf("assertion %s weight is invalid", assertion.ID)
		}
		if err := validateAssertion(*assertion); err != nil {
			return fmt.Errorf("assertion %s: %w", assertion.ID, err)
		}
	}
	if evalCase.Coverage != nil {
		if err := ValidateCoverageContract(evalCase.Coverage); err != nil {
			return fmt.Errorf("coverage contract: %w", err)
		}
	}
	return validateMetadata(evalCase.Metadata)
}

func validateAssertion(assertion Assertion) error {
	switch assertion.Kind {
	case AssertRunStatus:
		switch assertion.ExpectedStatus {
		case core.RunCompleted, core.RunLimited, core.RunFailed, core.RunCancelled, core.RunWaitingApproval:
		default:
			return fmt.Errorf("expected status %q is invalid", assertion.ExpectedStatus)
		}
	case AssertAnswerExact, AssertAnswerContains:
		if len(assertion.Expected) > MaxEvaluationText {
			return fmt.Errorf("expected answer is too large")
		}
		if assertion.Kind == AssertAnswerContains && assertion.Expected == "" {
			return fmt.Errorf("expected contained text is empty")
		}
	case AssertAnswerJSON:
		if err := core.ValidateSchema(assertion.Schema); err != nil {
			return fmt.Errorf("invalid JSON schema: %w", err)
		}
	case AssertToolCalled, AssertToolNotCalled:
		if err := core.ValidateNamespacedID(assertion.CapabilityID); err != nil {
			return fmt.Errorf("capability id: %w", err)
		}
	default:
		if err := core.ValidateNamespacedID(string(assertion.Kind)); err != nil {
			return fmt.Errorf("unknown assertion kind %q", assertion.Kind)
		}
		if assertion.Schema != nil {
			if err := core.ValidateSchema(assertion.Schema); err != nil {
				return fmt.Errorf("invalid custom assertion schema: %w", err)
			}
		}
	}
	return validateMetadata(assertion.Metadata)
}

func DatasetRevision(dataset Dataset) (string, error) {
	cases, err := canonicalCasesForDatasetRevision(dataset.Cases)
	if err != nil {
		return "", err
	}
	projection := struct {
		ID, Name, Description, ProfileID string
		Version                          int
		PassThreshold                    float64
		Cases                            []Case
		Metadata                         map[string]string
	}{dataset.ID, dataset.Name, dataset.Description, dataset.ProfileID, dataset.Version, normalizedThreshold(dataset.PassThreshold), cases, dataset.Metadata}
	encoded, err := json.Marshal(projection)
	if err != nil {
		return "", fmt.Errorf("encode dataset revision: %w", err)
	}
	if len(encoded) > MaxDatasetBytes {
		return "", fmt.Errorf("evaluation dataset exceeds %d bytes", MaxDatasetBytes)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

// canonicalCasesForDatasetRevision prevents direct DatasetRevision callers
// from obtaining a definition digest that depends on Coverage slice order or
// a caller-supplied Coverage revision. Full dataset validation remains the
// responsibility of ValidateDataset.
func canonicalCasesForDatasetRevision(cases []Case) ([]Case, error) {
	if cases == nil {
		return nil, nil
	}
	out := append([]Case(nil), cases...)
	for index := range out {
		if out[index].Coverage == nil {
			continue
		}
		contract := cloneCoverageContract(*out[index].Coverage)
		if err := ValidateCoverageContract(&contract); err != nil {
			return nil, fmt.Errorf("case %d coverage contract: %w", index, err)
		}
		out[index].Coverage = &contract
	}
	return out, nil
}

func Compare(current, baseline RunResult, tolerance float64) (Comparison, error) {
	if current.DatasetID != baseline.DatasetID || current.DatasetVersion != baseline.DatasetVersion || current.DatasetRevision != baseline.DatasetRevision {
		return Comparison{}, fmt.Errorf("evaluation runs use different dataset revisions")
	}
	if !finiteUnitScore(current.Score) || !finiteUnitScore(baseline.Score) || !finiteUnitScore(tolerance) {
		return Comparison{}, fmt.Errorf("comparison tolerance must be between 0 and 1")
	}
	for _, results := range [][]CaseResult{current.Cases, baseline.Cases} {
		for _, result := range results {
			if !finiteUnitScore(result.Score) {
				return Comparison{}, fmt.Errorf("comparison case score is invalid")
			}
		}
	}
	comparison := Comparison{
		CurrentRunID: current.ID, BaselineRunID: baseline.ID,
		ScoreDelta: current.Score - baseline.Score, Tolerance: tolerance,
		CaseDeltas: map[string]float64{},
	}
	baselineCases := map[string]float64{}
	for _, result := range baseline.Cases {
		baselineCases[result.CaseID] = result.Score
	}
	for _, result := range current.Cases {
		if score, ok := baselineCases[result.CaseID]; ok {
			comparison.CaseDeltas[result.CaseID] = result.Score - score
		}
	}
	comparison.Regressed = comparison.ScoreDelta < -tolerance || (baseline.Passed && !current.Passed)
	return comparison, nil
}

func EvaluateGate(current RunResult, baseline *RunResult, policy GatePolicy) (GateResult, error) {
	if current.Status != RunCompleted {
		return GateResult{}, fmt.Errorf("current evaluation run is not completed")
	}
	if !finiteUnitScore(current.Score) {
		return GateResult{}, fmt.Errorf("current evaluation score must be between 0 and 1")
	}
	if policy.MinScore != nil && !finiteUnitScore(*policy.MinScore) {
		return GateResult{}, fmt.Errorf("gate min score must be between 0 and 1")
	}
	if policy.MaxRegression != nil && !finiteUnitScore(*policy.MaxRegression) {
		return GateResult{}, fmt.Errorf("gate max regression must be between 0 and 1")
	}
	allCurrentCasesPassed := false
	if policy.RequireAllCases {
		var err error
		allCurrentCasesPassed, err = completeCaseResults(current)
		if err != nil {
			return GateResult{}, fmt.Errorf("current evaluation cases are incomplete: %w", err)
		}
	}
	result := GateResult{Passed: true}
	if policy.RequirePassed && !current.Passed {
		result.Passed = false
		result.Reasons = append(result.Reasons, "evaluation run did not pass its dataset threshold")
	}
	if policy.MinScore != nil && current.Score < *policy.MinScore {
		result.Passed = false
		result.Reasons = append(result.Reasons, fmt.Sprintf("score %.6f is below minimum %.6f", current.Score, *policy.MinScore))
	}
	if policy.RequireAllCases && !allCurrentCasesPassed {
		result.Passed = false
		result.Reasons = append(result.Reasons, "one or more evaluation cases did not pass")
	}
	if policy.MaxRegression != nil {
		if baseline == nil {
			return GateResult{}, fmt.Errorf("gate max regression requires a baseline run")
		}
		if policy.RequireAllCases {
			if baseline.Status != RunCompleted {
				return GateResult{}, fmt.Errorf("baseline evaluation run is not completed")
			}
			if _, err := completeCaseResults(*baseline); err != nil {
				return GateResult{}, fmt.Errorf("baseline evaluation cases are incomplete: %w", err)
			}
			if err := sameCaseIDs(current.Cases, baseline.Cases); err != nil {
				return GateResult{}, fmt.Errorf("current and baseline evaluation cases differ: %w", err)
			}
		}
		comparison, err := Compare(current, *baseline, *policy.MaxRegression)
		if err != nil {
			return GateResult{}, err
		}
		result.Comparison = &comparison
		if comparison.Regressed {
			result.Passed = false
			result.Reasons = append(result.Reasons,
				fmt.Sprintf("score regression %.6f exceeds tolerance %.6f", -comparison.ScoreDelta, *policy.MaxRegression))
		}
	}
	return result, nil
}

func completeCaseResults(run RunResult) (bool, error) {
	if run.TotalCases < 1 || len(run.Cases) != run.TotalCases {
		return false, fmt.Errorf("case count is %d, want %d", len(run.Cases), run.TotalCases)
	}
	seen := make(map[string]bool, len(run.Cases))
	passedCases := 0
	for _, result := range run.Cases {
		if !caseIDPattern.MatchString(result.CaseID) {
			return false, fmt.Errorf("case id %q is invalid", result.CaseID)
		}
		if !finiteUnitScore(result.Score) {
			return false, fmt.Errorf("case %q score is invalid", result.CaseID)
		}
		if seen[result.CaseID] {
			return false, fmt.Errorf("case id %q is duplicated", result.CaseID)
		}
		seen[result.CaseID] = true
		if result.Passed {
			passedCases++
		}
	}
	if run.PassedCases != passedCases {
		return false, fmt.Errorf("passed case count is %d, want %d", run.PassedCases, passedCases)
	}
	return passedCases == run.TotalCases, nil
}

func sameCaseIDs(current, baseline []CaseResult) error {
	if len(current) != len(baseline) {
		return fmt.Errorf("case counts differ")
	}
	baselineIDs := make(map[string]bool, len(baseline))
	for _, result := range baseline {
		baselineIDs[result.CaseID] = true
	}
	for _, result := range current {
		if !baselineIDs[result.CaseID] {
			return fmt.Errorf("case %q is missing from baseline", result.CaseID)
		}
	}
	return nil
}

func ValidateRunResult(run RunResult, final bool) error {
	if err := core.ValidateRunID(run.ID); err != nil {
		return fmt.Errorf("evaluation run id: %w", err)
	}
	if err := core.ValidateNamespacedID(run.DatasetID); err != nil {
		return fmt.Errorf("evaluation run dataset id: %w", err)
	}
	if run.DatasetVersion < 1 || len(run.DatasetRevision) != sha256.Size*2 {
		return fmt.Errorf("evaluation run dataset version or revision is invalid")
	}
	if _, err := hex.DecodeString(run.DatasetRevision); err != nil {
		return fmt.Errorf("evaluation run dataset revision is invalid")
	}
	if strings.TrimSpace(run.TenantID) == "" || strings.TrimSpace(run.SubjectID) == "" {
		return fmt.Errorf("evaluation run ownership is incomplete")
	}
	if err := core.ValidateNamespacedID(run.ProfileID); err != nil {
		return fmt.Errorf("evaluation run profile id: %w", err)
	}
	if run.BaselineRunID != "" {
		if err := core.ValidateRunID(run.BaselineRunID); err != nil {
			return fmt.Errorf("evaluation baseline run id: %w", err)
		}
	}
	switch run.Status {
	case RunRunning, RunCompleted, RunFailed:
	default:
		return fmt.Errorf("evaluation run status %q is invalid", run.Status)
	}
	if !final && run.Status != RunRunning {
		return fmt.Errorf("new evaluation run must be running")
	}
	if final && run.Status == RunRunning {
		return fmt.Errorf("finished evaluation run is still running")
	}
	if math.IsNaN(run.Score) || math.IsInf(run.Score, 0) || run.Score < 0 || run.Score > 1 ||
		run.TotalCases < 1 || run.TotalCases > MaxDatasetCases ||
		run.PassedCases < 0 || run.PassedCases > run.TotalCases {
		return fmt.Errorf("evaluation run scores or case counts are invalid")
	}
	if run.CreatedAt.IsZero() || (final && run.CompletedAt.IsZero()) {
		return fmt.Errorf("evaluation run timestamps are incomplete")
	}
	if len(run.Error) > MaxEvaluationText {
		return fmt.Errorf("evaluation run error is too large")
	}
	if err := validateMetadata(run.Metadata); err != nil {
		return fmt.Errorf("evaluation run metadata: %w", err)
	}
	if err := core.ValidateRunCompositionMetadata(run.CompositionMetadata); err != nil {
		return fmt.Errorf("evaluation composition metadata: %w", err)
	}
	if _, err := normalizeCapabilityAllowlistForValidation(run.AllowCapabilities); err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, result := range run.Cases {
		if err := ValidateCaseResult(result); err != nil {
			return err
		}
		if seen[result.CaseID] {
			return fmt.Errorf("duplicate evaluation case result %q", result.CaseID)
		}
		seen[result.CaseID] = true
	}
	return nil
}

func ValidateCaseResult(result CaseResult) error {
	if !caseIDPattern.MatchString(result.CaseID) {
		return fmt.Errorf("evaluation case result id %q is invalid", result.CaseID)
	}
	if err := core.ValidateSessionID(result.SessionID); err != nil {
		return err
	}
	if err := core.ValidateRunID(result.AgentRunID); err != nil {
		return err
	}
	switch result.Status {
	case core.RunCompleted, core.RunLimited, core.RunFailed, core.RunCancelled, core.RunWaitingApproval:
	default:
		return fmt.Errorf("evaluation case run status %q is invalid", result.Status)
	}
	if !finiteUnitScore(result.Score) || result.DurationMS < 0 || result.CompletedAt.IsZero() {
		return fmt.Errorf("evaluation case result score, duration, or time is invalid")
	}
	if len(result.Answer) > MaxEvaluationText || len(result.Error) > MaxEvaluationText {
		return fmt.Errorf("evaluation case result text is too large")
	}
	if result.Evidence != nil {
		if err := validateExecutionEvidence(*result.Evidence); err != nil {
			return err
		}
	}
	if result.Ledger != nil {
		if err := validateExecutionLedger(*result.Ledger); err != nil {
			return err
		}
	}
	if result.Coverage != nil {
		if err := ValidateCoverageEvidence(*result.Coverage); err != nil {
			return err
		}
	}
	for name, value := range map[string]string{
		"composition revision": result.Artifacts.CompositionRevision,
		"assignment revision":  result.Artifacts.AssignmentRevision,
	} {
		if value == "" {
			continue
		}
		if len(value) != sha256.Size*2 {
			return fmt.Errorf("evaluation artifact %s is not a SHA-256 digest", name)
		}
		if _, err := hex.DecodeString(value); err != nil {
			return fmt.Errorf("evaluation artifact %s: %w", name, err)
		}
	}
	seenAssertions := map[string]bool{}
	for _, assertion := range result.Assertions {
		if !caseIDPattern.MatchString(assertion.AssertionID) || seenAssertions[assertion.AssertionID] || !finiteUnitScore(assertion.Score) {
			return fmt.Errorf("evaluation assertion result is invalid")
		}
		seenAssertions[assertion.AssertionID] = true
		if len(assertion.Message) > 4096 {
			return fmt.Errorf("evaluation assertion message is too large")
		}
	}
	for _, capability := range result.ToolCalls {
		if err := core.ValidateNamespacedID(capability); err != nil {
			return fmt.Errorf("evaluation tool call: %w", err)
		}
	}
	return nil
}

func validateExecutionLedger(ledger ExecutionLedger) error {
	validOutcome := func(value string) bool { return value == "ok" || value == "error" }
	validGateOutcome := func(value string) bool { return value == "accepted" || value == "denied" }
	validReason := map[string]bool{
		"context_accounting_unavailable":    true,
		"adapter_calls_unavailable":         true,
		"model_resolver_error":              true,
		"model_resolver_nil":                true,
		"duplicate_adapter_step":            true,
		"adapter_usage_without_call":        true,
		"adapter_usage_duplicate":           true,
		"adapter_usage_invalid":             true,
		"adapter_usage_missing":             true,
		"adapter_error":                     true,
		"adapter_finish_without_call":       true,
		"context_assembly_error":            true,
		"model_gate_denied":                 true,
		"model_gate_accounting_unavailable": true,
		"durable_step_decode_error":         true,
		"durable_step_duplicate":            true,
		"durable_step_missing":              true,
		"durable_step_without_adapter":      true,
		"durable_usage_decode_error":        true,
		"durable_usage_invalid":             true,
		"durable_usage_duplicate":           true,
		"durable_usage_missing":             true,
		"durable_usage_conflict":            true,
		"durable_usage_without_adapter":     true,
	}
	if len(ledger.ModelCalls) > core.HardMaxSteps || len(ledger.ContextAssemblies) > core.HardMaxSteps || len(ledger.GateCalls) > core.HardMaxSteps {
		return fmt.Errorf("evaluation execution ledger exceeds bounded call evidence")
	}
	seenReasons := map[string]bool{}
	for _, reason := range ledger.IncompleteReasons {
		if !validReason[reason] || seenReasons[reason] {
			return fmt.Errorf("evaluation execution ledger reasons are invalid")
		}
		seenReasons[reason] = true
	}
	if ledger.Complete && len(ledger.IncompleteReasons) != 0 {
		return fmt.Errorf("complete evaluation execution ledger has incomplete reasons")
	}
	if !ledger.Complete && len(ledger.IncompleteReasons) == 0 {
		return fmt.Errorf("incomplete evaluation execution ledger has no reason")
	}
	if ledger.Complete && len(ledger.ModelCalls) == 0 {
		return fmt.Errorf("complete evaluation execution ledger has no model calls")
	}
	seenSteps := map[int]bool{}
	for _, call := range ledger.ModelCalls {
		if call.Step < 0 || call.Step > core.HardMaxSteps || seenSteps[call.Step] || !call.Invoked || !validOutcome(call.Outcome) || call.UsageReports < 0 || call.UsageReports > core.MaxSessionEvents {
			return fmt.Errorf("evaluation execution ledger model call is invalid")
		}
		seenSteps[call.Step] = true
		if call.Usage != nil && (call.Usage.InputTokens < 0 || call.Usage.OutputTokens < 0 || call.Usage.InputTokens > core.MaxReportedTokensPerCall || call.Usage.OutputTokens > core.MaxReportedTokensPerCall) {
			return fmt.Errorf("evaluation execution ledger usage is invalid")
		}
		if ledger.Complete && (call.UsageReports != 1 || call.Usage == nil) {
			return fmt.Errorf("evaluation execution ledger usage reporting is inconsistent")
		}
	}
	for _, assembly := range ledger.ContextAssemblies {
		if assembly.ContextWindowTokens <= 0 || assembly.MaxOutputTokens <= 0 || assembly.MaxOutputTokens >= assembly.ContextWindowTokens || assembly.InputBytes < 0 || assembly.InputTokens < 0 || assembly.DroppedGroups < 0 || !validOutcome(assembly.Outcome) {
			return fmt.Errorf("evaluation execution ledger context assembly is invalid")
		}
	}
	if ledger.Complete && len(ledger.ContextAssemblies) != len(ledger.ModelCalls) {
		return fmt.Errorf("complete evaluation execution ledger context count does not match model calls")
	}
	seenGateSteps := map[int]bool{}
	for _, gate := range ledger.GateCalls {
		if gate.Step < 0 || gate.Step > core.HardMaxSteps || seenGateSteps[gate.Step] || !validGateOutcome(gate.Outcome) {
			return fmt.Errorf("evaluation execution ledger gate call is invalid")
		}
		seenGateSteps[gate.Step] = true
	}
	if ledger.Complete && len(ledger.GateCalls) != 0 {
		if len(ledger.GateCalls) != len(ledger.ModelCalls) {
			return fmt.Errorf("complete evaluation execution ledger gate count does not match model calls")
		}
		for step := range seenSteps {
			if !seenGateSteps[step] {
				return fmt.Errorf("complete evaluation execution ledger gate steps do not match model calls")
			}
		}
	}
	return nil
}

func validateExecutionEvidence(evidence ExecutionEvidence) error {
	if evidence.ReportedInputTokens < 0 || evidence.ReportedOutputTokens < 0 {
		return fmt.Errorf("evaluation execution evidence has negative reported tokens")
	}
	if evidence.ReportedInputTokens > core.MaxReportedTokensPerRun || evidence.ReportedOutputTokens > core.MaxReportedTokensPerRun {
		return fmt.Errorf("evaluation execution evidence reported tokens exceed the bounded session maximum")
	}
	for name, value := range map[string]int{
		"usage reports":             evidence.UsageReports,
		"steps started":             evidence.StepsStarted,
		"steps ended":               evidence.StepsEnded,
		"top-level tool calls":      evidence.TopLevelToolCalls,
		"tool result events OK":     evidence.ToolResultEventsOK,
		"tool result events not OK": evidence.ToolResultEventsNotOK,
	} {
		if value < 0 || value > core.MaxSessionEvents {
			return fmt.Errorf("evaluation execution evidence %s is outside bounds", name)
		}
	}
	if evidence.NoUsageReported {
		if evidence.UsageReports != 0 || evidence.ReportedInputTokens != 0 || evidence.ReportedOutputTokens != 0 {
			return fmt.Errorf("evaluation execution evidence no-usage marker is inconsistent")
		}
	} else if evidence.UsageReports == 0 {
		return fmt.Errorf("evaluation execution evidence usage report presence is inconsistent")
	}
	return nil
}

func normalizeCapabilityAllowlistForValidation(values []string) ([]string, error) {
	seen := map[string]bool{}
	out := []string{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		if err := core.ValidateNamespacedID(value); err != nil {
			return nil, fmt.Errorf("evaluation allowed capability: %w", err)
		}
		seen[value] = true
		out = append(out, value)
	}
	if len(out) > core.HardMaxToolCalls {
		return nil, fmt.Errorf("evaluation capability allowlist exceeds %d", core.HardMaxToolCalls)
	}
	sort.Strings(out)
	return out, nil
}

func normalizedThreshold(value float64) float64 {
	if value == 0 {
		return 1
	}
	return value
}

func finiteUnitScore(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0) && value >= 0 && value <= 1
}

func validateMetadata(metadata map[string]string) error {
	for key, value := range metadata {
		if strings.TrimSpace(key) == "" || len(key) > MaxMetadataKeyBytes || containsEvaluationControl(key) {
			return fmt.Errorf("metadata key is empty, too long, or contains control characters")
		}
		if len(value) > MaxMetadataValueBytes || containsEvaluationControl(value) {
			return fmt.Errorf("metadata value is too long or contains control characters")
		}
	}
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	if len(encoded) > MaxEvaluationMetadata {
		return fmt.Errorf("metadata exceeds %d bytes", MaxEvaluationMetadata)
	}
	return nil
}

func containsEvaluationControl(value string) bool {
	for _, char := range value {
		if char < 0x20 || char == 0x7f {
			return true
		}
	}
	return false
}
