package evaluation

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/whhhh1500/auto-agent/internal/evaluationledger"
	"github.com/whhhh1500/auto-agent/internal/executionroute"
	"github.com/whhhh1500/auto-agent/pkg/app/effectreceipt"
	"github.com/whhhh1500/auto-agent/pkg/app/runexecutor"
	core "github.com/whhhh1500/auto-agent/pkg/core"
)

const evaluationPersistenceTimeout = 15 * time.Second

type RunRequest struct {
	DatasetID           string
	DatasetVersion      int
	Principal           core.Principal
	ProfileID           string
	BaselineRunID       string
	AllowCapabilities   []string
	CompositionMetadata map[string]string
	Metadata            map[string]string
}

type Runner struct {
	Runtime    *core.Runtime
	Sessions   core.SessionStore
	Store      Store
	Evaluators *Registry
	// Executors is the same application-level registry used by the server.
	// A nil registry preserves embedded-runner compatibility with sequential.
	Executors *runexecutor.Registry
	// EffectReceipts optionally provides scoped, provider-neutral external
	// effect read-back evidence for CoverageContract requirements. A nil reader
	// leaves those requirements unavailable; it never changes normal execution.
	EffectReceipts effectreceipt.RunReader
	Now            func() time.Time
}

func (r *Runner) Run(ctx context.Context, request RunRequest) (RunResult, error) {
	if ctx == nil {
		return RunResult{}, fmt.Errorf("evaluation context is nil")
	}
	if r == nil || r.Runtime == nil {
		return RunResult{}, fmt.Errorf("evaluation runtime is nil")
	}
	if request.Principal.SubjectID == "" || request.Principal.TenantID == "" || request.Principal.Scope.Depth() == 0 {
		return RunResult{}, fmt.Errorf("evaluation principal is incomplete")
	}
	store, sessions, registry, err := r.dependencies()
	if err != nil {
		return RunResult{}, err
	}
	executors, err := r.executors()
	if err != nil {
		return RunResult{}, err
	}
	dataset, err := store.GetDataset(ctx, request.DatasetID, request.DatasetVersion)
	if err != nil {
		return RunResult{}, err
	}
	if err := ValidateDataset(&dataset); err != nil {
		return RunResult{}, err
	}
	profileID := request.ProfileID
	if profileID == "" {
		profileID = dataset.ProfileID
	}
	if err := core.ValidateNamespacedID(profileID); err != nil {
		return RunResult{}, fmt.Errorf("evaluation profile id: %w", err)
	}
	allow, err := normalizeCapabilityAllowlist(request.AllowCapabilities)
	if err != nil {
		return RunResult{}, err
	}
	if err := core.ValidateRunCompositionMetadata(request.CompositionMetadata); err != nil {
		return RunResult{}, fmt.Errorf("evaluation composition metadata: %w", err)
	}
	assignmentRevision, err := core.CompositionMetadataRevision(request.CompositionMetadata)
	if err != nil {
		return RunResult{}, fmt.Errorf("evaluation assignment revision: %w", err)
	}
	evaluationID, err := core.NewID("eval_")
	if err != nil {
		return RunResult{}, err
	}
	metadata := cloneStringMap(request.Metadata)
	if metadata == nil {
		metadata = map[string]string{}
	}
	metadata["evaluation.principal_scope"] = request.Principal.Scope.String()
	run := RunResult{
		ID: evaluationID, DatasetID: dataset.ID, DatasetVersion: dataset.Version,
		DatasetRevision: dataset.Revision, TenantID: request.Principal.TenantID,
		SubjectID: request.Principal.SubjectID, ProfileID: profileID,
		AssignmentRevision: assignmentRevision,
		BaselineRunID:      request.BaselineRunID, Status: RunRunning,
		TotalCases: len(dataset.Cases), AllowCapabilities: allow,
		CompositionMetadata: cloneStringMap(request.CompositionMetadata),
		Metadata:            metadata, CreatedAt: r.now(),
	}
	if err := validateMetadata(run.Metadata); err != nil {
		return RunResult{}, fmt.Errorf("evaluation run metadata: %w", err)
	}
	if err := store.CreateRun(ctx, run); err != nil {
		return RunResult{}, err
	}
	return r.executeRun(ctx, store, sessions, registry, executors, run, dataset, request.Principal, false)
}

// Resume continues a running evaluation after process/request interruption.
// Completed case rows are skipped; completed deterministic Case Sessions can
// reconstruct missing case rows without rerunning the model or tools.
func (r *Runner) Resume(ctx context.Context, runID string, principal core.Principal) (RunResult, error) {
	if ctx == nil {
		return RunResult{}, fmt.Errorf("evaluation context is nil")
	}
	if r == nil || r.Runtime == nil {
		return RunResult{}, fmt.Errorf("evaluation runtime is nil")
	}
	store, sessions, registry, err := r.dependencies()
	if err != nil {
		return RunResult{}, err
	}
	executors, err := r.executors()
	if err != nil {
		return RunResult{}, err
	}
	run, err := store.GetRun(ctx, runID)
	if err != nil {
		return RunResult{}, err
	}
	if run.TenantID != principal.TenantID || run.SubjectID != principal.SubjectID ||
		run.Metadata["evaluation.principal_scope"] != principal.Scope.String() {
		return RunResult{}, fmt.Errorf("evaluation principal does not own run %s", runID)
	}
	if run.Status != RunRunning {
		return run, nil
	}
	dataset, err := store.GetDataset(ctx, run.DatasetID, run.DatasetVersion)
	if err != nil {
		return run, err
	}
	if dataset.Revision != run.DatasetRevision || run.TotalCases != len(dataset.Cases) {
		return run, fmt.Errorf("evaluation dataset changed before resume")
	}
	return r.executeRun(ctx, store, sessions, registry, executors, run, dataset, principal, true)
}

func (r *Runner) dependencies() (Store, core.SessionStore, *Registry, error) {
	if r.Store == nil {
		return nil, nil, nil, fmt.Errorf("evaluation store is nil")
	}
	sessions := r.Sessions
	if sessions == nil {
		sessions = core.NewMemorySessionStore()
	}
	registry := r.Evaluators
	if registry == nil {
		var err error
		registry, err = NewRegistry()
		if err != nil {
			return nil, nil, nil, err
		}
	}
	return r.Store, sessions, registry, nil
}

func (r *Runner) executors() (*runexecutor.Registry, error) {
	if r != nil && r.Executors != nil {
		return r.Executors, nil
	}
	return runexecutor.NewDefaultRegistry()
}

func (r *Runner) executeRun(
	ctx context.Context,
	store Store,
	sessions core.SessionStore,
	registry *Registry,
	executors *runexecutor.Registry,
	run RunResult,
	dataset Dataset,
	principal core.Principal,
	resume bool,
) (runResult RunResult, runErr error) {
	ctx, runSpan := core.StartTelemetry(r.Runtime.Telemetry, ctx, core.SpanEvaluationRun, core.TelemetryAttributes{
		"evaluation.run.id": run.ID, "evaluation.dataset.id": dataset.ID,
		"evaluation.dataset.version": fmt.Sprintf("%d", dataset.Version),
		"profile.id":                 run.ProfileID, "tenant.id": principal.TenantID,
		"evaluation.resume": fmt.Sprintf("%t", resume),
	})
	runStarted := time.Now()
	defer func() {
		status := string(runResult.Status)
		if status == "" {
			status = "error"
		}
		passed := fmt.Sprintf("%t", runResult.Passed)
		runSpan.End(runErr, core.TelemetryAttributes{"evaluation.status": status, "evaluation.passed": passed})
		attrs := core.TelemetryAttributes{
			"evaluation.status": status, "evaluation.passed": passed,
			"profile.id": run.ProfileID, "evaluation.resume": fmt.Sprintf("%t", resume),
		}
		core.AddTelemetryCounter(r.Runtime.Telemetry, ctx, core.MetricEvaluationRuns, 1, attrs)
		core.RecordTelemetryHistogram(r.Runtime.Telemetry, ctx, core.MetricEvaluationDuration,
			time.Since(runStarted).Seconds(), "s", attrs)
		if runResult.Status == RunCompleted {
			core.RecordTelemetryHistogram(r.Runtime.Telemetry, ctx, core.MetricEvaluationScore, runResult.Score, "1", attrs)
		}
	}()
	knownCases := map[string]Case{}
	for _, evalCase := range dataset.Cases {
		knownCases[evalCase.ID] = evalCase
	}
	completed := map[string]CaseResult{}
	run.PassedCases = 0
	for _, result := range run.Cases {
		if _, ok := knownCases[result.CaseID]; !ok {
			return run, fmt.Errorf("evaluation run contains case %s outside dataset", result.CaseID)
		}
		completed[result.CaseID] = result
		if result.Passed {
			run.PassedCases++
		}
	}
	for _, evalCase := range dataset.Cases {
		if _, ok := completed[evalCase.ID]; ok {
			continue
		}
		if err := ctx.Err(); err != nil {
			return r.interruptRun(store, run, err)
		}
		caseResult, caseErr := r.runCase(ctx, sessions, registry, executors, run, dataset, evalCase, principal, run.ProfileID, run.AllowCapabilities)
		if caseErr != nil {
			return r.interruptRun(store, run, caseErr)
		}
		if err := store.RecordCaseResult(ctx, run.ID, caseResult); err != nil {
			return r.interruptRun(store, run, err)
		}
		run.Cases = append(run.Cases, caseResult)
		completed[caseResult.CaseID] = caseResult
		if caseResult.Passed {
			run.PassedCases++
		}
	}
	ordered := make([]CaseResult, 0, len(dataset.Cases))
	for _, evalCase := range dataset.Cases {
		ordered = append(ordered, completed[evalCase.ID])
	}
	run.Cases = ordered
	total := 0.0
	for _, result := range run.Cases {
		total += result.Score
	}
	if len(run.Cases) > 0 {
		run.Score = total / float64(len(run.Cases))
	}
	run.Passed = run.Score >= dataset.PassThreshold
	run.Status, run.Error, run.CompletedAt = RunCompleted, "", r.now()
	if err := store.FinishRun(ctx, run); err != nil {
		return run, err
	}
	return run, nil
}

func (r *Runner) interruptRun(store Store, run RunResult, cause error) (RunResult, error) {
	run.Status = RunRunning
	run.Error = boundedEvaluationMessage(cause.Error())
	persistCtx, cancelPersist := context.WithTimeout(context.Background(), evaluationPersistenceTimeout)
	defer cancelPersist()
	persistErr := store.RecordRunError(persistCtx, run.ID, run.Error)
	if persistErr != nil {
		return run, errors.Join(cause, persistErr)
	}
	return run, cause
}

func (r *Runner) runCase(
	ctx context.Context,
	sessions core.SessionStore,
	registry *Registry,
	executors *runexecutor.Registry,
	evaluationRun RunResult,
	dataset Dataset,
	evalCase Case,
	principal core.Principal,
	defaultProfile string,
	allow []string,
) (caseResult CaseResult, caseErr error) {
	started := time.Now()
	sessionID, agentRunID, seedRunID := evaluationCaseIDs(evaluationRun.ID, evalCase.ID)
	profileID := evalCase.ProfileID
	if profileID == "" || profileID == dataset.ProfileID {
		profileID = defaultProfile
	}
	scope, err := principal.Scope.Child(core.ScopeRef{Kind: core.ScopeSession, ID: sessionID})
	if err != nil {
		return CaseResult{}, err
	}
	metadata := map[string]string{
		"evaluation.run_id":           evaluationRun.ID,
		"evaluation.dataset_id":       dataset.ID,
		"evaluation.dataset_version":  fmt.Sprintf("%d", dataset.Version),
		"evaluation.dataset_revision": dataset.Revision,
		"evaluation.case_id":          evalCase.ID,
	}
	for key, value := range evalCase.Metadata {
		metadata["evaluation.case."+key] = value
	}
	session, loadErr := sessions.Load(ctx, sessionID)
	if errors.Is(loadErr, core.ErrSessionNotFound) {
		session, loadErr = core.NewSession(core.SessionOptions{
			ID: sessionID, ProfileID: profileID, Principal: principal, Scope: scope, Metadata: metadata,
		})
		if loadErr != nil {
			return CaseResult{}, loadErr
		}
		if err := appendEvaluationContext(session, evalCase.Context, seedRunID); err != nil {
			return CaseResult{}, err
		}
		if err := sessions.Create(ctx, session); err != nil {
			return CaseResult{}, err
		}
	} else if loadErr != nil {
		return CaseResult{}, loadErr
	} else if err := validateEvaluationSession(session, principal, profileID, metadata); err != nil {
		return CaseResult{}, err
	}
	ctx, caseSpan := core.StartTelemetry(r.Runtime.Telemetry, ctx, core.SpanEvaluationCase, core.TelemetryAttributes{
		"evaluation.run.id": evaluationRun.ID, "evaluation.dataset.id": dataset.ID,
		"evaluation.case.id": evalCase.ID, "session.id": sessionID, "run.id": agentRunID,
		"profile.id": profileID,
	})
	defer func() {
		outcome := "error"
		passed := "false"
		if caseResult.CaseID != "" {
			outcome = string(caseResult.Status)
			passed = fmt.Sprintf("%t", caseResult.Passed)
		}
		attrs := core.TelemetryAttributes{"evaluation.case.status": outcome, "evaluation.case.passed": passed, "profile.id": profileID}
		caseSpan.End(caseErr, core.TelemetryAttributes{"evaluation.case.status": outcome, "evaluation.case.passed": passed})
		core.AddTelemetryCounter(r.Runtime.Telemetry, ctx, core.MetricEvaluationCases, 1, attrs)
		core.RecordTelemetryHistogram(r.Runtime.Telemetry, ctx, core.MetricEvaluationCaseDuration,
			time.Since(started).Seconds(), "s", attrs)
	}()
	var turn core.TurnResult
	var runErr error
	collector := evaluationledger.New()
	caseRuntime := collector.Runtime(r.Runtime)
	if status, exists := session.RunStatus(agentRunID); exists {
		if status == "" {
			expectedVersion := session.Version()
			synthetic := core.RepairInterrupted(session.Events())
			if len(synthetic) == 0 {
				return CaseResult{}, fmt.Errorf("evaluation case session has an unresolved non-terminal run")
			}
			if err := core.AppendRepair(session, synthetic, nil); err != nil {
				return CaseResult{}, err
			}
			persistCtx, cancelPersist := evaluationPersistenceContext(ctx)
			err := sessions.Save(persistCtx, session, expectedVersion)
			cancelPersist()
			if err != nil {
				return CaseResult{}, err
			}
			status, _ = session.RunStatus(agentRunID)
		}
		if status == core.RunWaitingApproval {
			return CaseResult{}, fmt.Errorf("evaluation case unexpectedly waits for approval")
		}
		turn = core.TurnResult{RunID: agentRunID, Status: status, Answer: session.LastAssistantText(agentRunID)}
		if status == core.RunFailed || status == core.RunCancelled {
			runErr = errors.New(evaluationRunError(session, agentRunID))
		}
	} else {
		expectedVersion := session.Version()
		decision, err := executionroute.Resolve(ctx, executionroute.Request{
			Runtime: caseRuntime, Registry: executors, Principal: principal, Session: session,
			RunID: agentRunID, BaseMetadata: evaluationRun.CompositionMetadata,
		})
		if err != nil {
			return CaseResult{}, err
		}
		turn, runErr = decision.Executor.RunTurn(ctx, principal, session, core.TurnInput{
			RunID: agentRunID, Text: evalCase.Input,
			CapabilityFilter:    safeEvaluationFilter(allow),
			CompositionMetadata: decision.CompositionMetadata,
		}, nil)
		persistCtx, cancelPersist := evaluationPersistenceContext(ctx)
		saveErr := sessions.Save(persistCtx, session, expectedVersion)
		cancelPersist()
		if saveErr != nil {
			return CaseResult{}, saveErr
		}
	}
	observation := observeEvaluationRun(session, agentRunID, turn, runErr)
	assertionResults := make([]AssertionResult, 0, len(evalCase.Assertions))
	weighted, totalWeight := 0.0, 0.0
	for _, assertion := range evalCase.Assertions {
		result, err := registry.Evaluate(ctx, assertion, observation)
		if err != nil {
			result = AssertionResult{
				AssertionID: assertion.ID, Kind: assertion.Kind, Score: 0, Passed: false,
				Message: boundedEvaluationMessage(err.Error()),
			}
		}
		assertionResults = append(assertionResults, result)
		weighted += result.Score * assertion.Weight
		totalWeight += assertion.Weight
	}
	score := 0.0
	if totalWeight > 0 {
		score = weighted / totalWeight
	}
	caseResult = CaseResult{
		CaseID: evalCase.ID, SessionID: sessionID, AgentRunID: agentRunID,
		Status: turn.Status, Answer: turn.Answer, Error: observation.Error,
		Score: score, Passed: score >= evalCase.PassThreshold,
		Assertions: assertionResults, Artifacts: extractArtifacts(session, agentRunID),
		DurationMS: time.Since(started).Milliseconds(), ToolCalls: observation.ToolCalls,
		CompletedAt: r.now(),
	}
	evidence, err := executionEvidence(session.Events(), agentRunID)
	if err != nil {
		return CaseResult{}, err
	}
	caseResult.Evidence = evidence
	ledger, err := collector.Project(session.Events(), agentRunID)
	if err != nil {
		return CaseResult{}, err
	}
	caseResult.Ledger = projectExecutionLedger(ledger)
	if evalCase.Coverage != nil {
		coverage, err := r.collectCoverageEvidence(ctx, session, principal, evalCase.Coverage, caseResult)
		if err != nil {
			return CaseResult{}, err
		}
		caseResult.Coverage = &coverage
	}
	return caseResult, nil
}

func projectExecutionLedger(source *evaluationledger.Ledger) *ExecutionLedger {
	if source == nil {
		return nil
	}
	ledger := &ExecutionLedger{Complete: source.Complete, IncompleteReasons: append([]string(nil), source.IncompleteReasons...)}
	ledger.ModelCalls = make([]ExecutionLedgerModelCall, 0, len(source.ModelCalls))
	for _, call := range source.ModelCalls {
		entry := ExecutionLedgerModelCall{Step: call.Step, Invoked: call.Invoked, Outcome: call.Outcome, UsageReports: call.UsageReports}
		if call.Usage != nil {
			entry.Usage = &ExecutionLedgerUsage{InputTokens: call.Usage.InputTokens, OutputTokens: call.Usage.OutputTokens}
		}
		ledger.ModelCalls = append(ledger.ModelCalls, entry)
	}
	for _, assembly := range source.ContextAssemblies {
		ledger.ContextAssemblies = append(ledger.ContextAssemblies, ExecutionLedgerContext{
			ContextWindowTokens: assembly.ContextWindowTokens, MaxOutputTokens: assembly.MaxOutputTokens,
			InputBytes: assembly.InputBytes, InputTokens: assembly.InputTokens, DroppedGroups: assembly.DroppedGroups, Outcome: assembly.Outcome,
		})
	}
	for _, gate := range source.GateCalls {
		ledger.GateCalls = append(ledger.GateCalls, ExecutionLedgerGateCall{Step: gate.Step, Outcome: gate.Outcome})
	}
	return ledger
}

func appendEvaluationContext(session *core.Session, messages []ContextMessage, runID string) error {
	if len(messages) == 0 {
		return nil
	}
	if _, err := session.Append(runID, core.EvRunStart, core.RunStartData{}); err != nil {
		return err
	}
	for _, message := range messages {
		switch message.Role {
		case core.RoleUser:
			if _, err := session.Append(runID, core.EvUserMessage, core.UserMessageData{Text: message.Text}); err != nil {
				return err
			}
		case core.RoleAssistant:
			if _, err := session.Append(runID, core.EvAssistantMessage, core.AssistantMessageData{Text: message.Text}); err != nil {
				return err
			}
		}
	}
	_, err := session.Append(runID, core.EvRunEnd, core.RunEndData{Status: core.RunCompleted})
	return err
}

func evaluationCaseIDs(evaluationRunID, caseID string) (sessionID, agentRunID, seedRunID string) {
	sum := sha256.Sum256([]byte(evaluationRunID + "\x00" + caseID))
	suffix := fmt.Sprintf("%x", sum[:16])
	return "evalsess_" + suffix, "evalcase_" + suffix, "evalseed_" + suffix
}

func validateEvaluationSession(session *core.Session, principal core.Principal, profileID string, expected map[string]string) error {
	owner := session.Principal()
	if owner.SubjectID != principal.SubjectID || owner.TenantID != principal.TenantID || !owner.Scope.Equal(principal.Scope) {
		return fmt.Errorf("evaluation case session owner changed")
	}
	if session.ProfileID() != profileID {
		return fmt.Errorf("evaluation case session profile changed")
	}
	metadata := session.Metadata()
	for key, value := range expected {
		if metadata[key] != value {
			return fmt.Errorf("evaluation case session metadata %s changed", key)
		}
	}
	return nil
}

func evaluationRunError(session *core.Session, runID string) string {
	events := session.Events()
	for index := len(events) - 1; index >= 0; index-- {
		event := events[index]
		if event.RunID != runID || event.Type != core.EvRunError {
			continue
		}
		var data core.RuntimeErrorData
		if json.Unmarshal(event.Data, &data) == nil && data.Message != "" {
			return data.Message
		}
	}
	return "evaluation agent run failed"
}

func evaluationPersistenceContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), evaluationPersistenceTimeout)
}

func safeEvaluationFilter(allow []string) core.CapabilityFilter {
	explicit := map[string]bool{}
	for _, id := range allow {
		explicit[id] = true
	}
	return core.CapabilityFilterFunc(func(manifest core.CapabilityManifest) bool {
		if manifest.Tool == nil {
			return true
		}
		if manifest.RequiresApproval {
			return false
		}
		if explicit[manifest.ID] {
			return true
		}
		return manifest.Idempotent
	})
}

// SafeCapabilityFilter returns the evaluation/backtest safety filter. Tool
// capabilities requiring approval are always removed; non-idempotent tools
// require an explicit namespaced allowlist entry.
func SafeCapabilityFilter(allow []string) (core.CapabilityFilter, error) {
	normalized, err := normalizeCapabilityAllowlist(allow)
	if err != nil {
		return nil, err
	}
	return safeEvaluationFilter(normalized), nil
}

func observeEvaluationRun(session *core.Session, runID string, turn core.TurnResult, runErr error) Observation {
	observation := Observation{Status: turn.Status, Answer: turn.Answer, Events: session.Events()}
	if runErr != nil {
		observation.Error = boundedEvaluationMessage(runErr.Error())
	}
	seen := map[string]bool{}
	for _, event := range observation.Events {
		if event.RunID != runID || event.Type != core.EvToolCall {
			continue
		}
		var data core.ToolCallData
		if json.Unmarshal(event.Data, &data) == nil && !seen[data.Name] {
			seen[data.Name] = true
			observation.ToolCalls = append(observation.ToolCalls, data.Name)
		}
	}
	sort.Strings(observation.ToolCalls)
	return observation
}

func executionEvidence(events []core.SessionEvent, runID string) (*ExecutionEvidence, error) {
	if err := core.ValidateRunID(runID); err != nil {
		return nil, err
	}
	evidence := &ExecutionEvidence{}
	topLevelCalls := map[string]bool{}
	maxTokens := core.MaxReportedTokensPerRun
	for _, event := range events {
		if event.RunID != runID {
			continue
		}
		switch event.Type {
		case core.EvRunUsage:
			var data core.RunUsageData
			if err := json.Unmarshal(event.Data, &data); err != nil {
				return nil, fmt.Errorf("decode execution evidence usage: %w", err)
			}
			if data.InputTokens < 0 || data.OutputTokens < 0 || data.InputTokens > maxTokens-evidence.ReportedInputTokens || data.OutputTokens > maxTokens-evidence.ReportedOutputTokens {
				return nil, fmt.Errorf("execution evidence usage exceeds bounds")
			}
			evidence.ReportedInputTokens += data.InputTokens
			evidence.ReportedOutputTokens += data.OutputTokens
			evidence.UsageReports++
		case core.EvStepStart:
			var data core.StepData
			if err := json.Unmarshal(event.Data, &data); err != nil {
				return nil, fmt.Errorf("decode execution evidence step start: %w", err)
			}
			evidence.StepsStarted++
		case core.EvStepEnd:
			var data core.StepData
			if err := json.Unmarshal(event.Data, &data); err != nil {
				return nil, fmt.Errorf("decode execution evidence step end: %w", err)
			}
			evidence.StepsEnded++
		case core.EvAssistantMessage:
			var data core.AssistantMessageData
			if err := json.Unmarshal(event.Data, &data); err != nil {
				return nil, fmt.Errorf("decode execution evidence assistant message: %w", err)
			}
			calls := data.ToolCalls
			if len(calls) == 0 && data.ToolCall != nil {
				calls = []core.ToolCall{*data.ToolCall}
			}
			for _, call := range calls {
				topLevelCalls[call.ID] = true
			}
		case core.EvToolResult:
			var data core.ToolResultData
			if err := json.Unmarshal(event.Data, &data); err != nil {
				return nil, fmt.Errorf("decode execution evidence tool result: %w", err)
			}
			if data.OK {
				evidence.ToolResultEventsOK++
			} else {
				evidence.ToolResultEventsNotOK++
			}
		}
	}
	evidence.NoUsageReported = evidence.UsageReports == 0
	evidence.TopLevelToolCalls = len(topLevelCalls)
	if err := validateExecutionEvidence(*evidence); err != nil {
		return nil, err
	}
	return evidence, nil
}

func extractArtifacts(session *core.Session, runID string) ArtifactSnapshot {
	evidence, found, _ := core.ExtractRunCompositionEvidence(session.Events(), runID)
	var artifacts ArtifactSnapshot
	seen := false
	for _, event := range session.Events() {
		if event.RunID != runID || (event.Type != core.EvRunStart && event.Type != core.EvRunResume) {
			continue
		}
		var profileSnapshotID, capabilitySnapshotID, compositionRevision, assignmentRevision string
		var composition *core.RunCompositionData
		if event.Type == core.EvRunStart {
			var data core.RunStartData
			if json.Unmarshal(event.Data, &data) != nil {
				return ArtifactSnapshot{}
			}
			profileSnapshotID, capabilitySnapshotID = data.ProfileSnapshotID, data.CapabilitySnapshotID
			compositionRevision, assignmentRevision, composition = data.CompositionRevision, data.AssignmentRevision, data.Composition
		} else {
			var data core.RunResumeData
			if json.Unmarshal(event.Data, &data) != nil {
				return ArtifactSnapshot{}
			}
			profileSnapshotID, capabilitySnapshotID = data.ProfileSnapshotID, data.CapabilitySnapshotID
			compositionRevision, assignmentRevision, composition = data.CompositionRevision, data.AssignmentRevision, data.Composition
		}
		artifacts = ArtifactSnapshot{
			ProfileSnapshotID: profileSnapshotID, CapabilitySnapshotID: capabilitySnapshotID,
			CompositionRevision: compositionRevision, AssignmentRevision: assignmentRevision,
		}
		if found {
			artifacts.CompositionRevision = evidence.CompositionRevision
			artifacts.AssignmentRevision = evidence.AssignmentRevision
		}
		if composition != nil {
			artifacts.ResolvedProvider = composition.ResolvedProvider
			artifacts.ModelRevision = composition.ModelRevision
			if artifacts.CompositionRevision == "" {
				artifacts.CompositionRevision, _ = core.CompositionRevision(composition)
			}
			if artifacts.AssignmentRevision == "" {
				artifacts.AssignmentRevision, _ = core.CompositionMetadataRevision(composition.Metadata)
			}
		}
		seen = true
	}
	if seen {
		return artifacts
	}
	return ArtifactSnapshot{}
}

func normalizeCapabilityAllowlist(values []string) ([]string, error) {
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

func (r *Runner) now() time.Time {
	if r != nil && r.Now != nil {
		return r.Now().UTC()
	}
	return time.Now().UTC()
}

func cloneStringMap(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	out := make(map[string]string, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}
