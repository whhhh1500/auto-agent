package evaluation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

type evaluationProbeTool struct {
	manifest core.CapabilityManifest
	calls    *atomic.Int32
	content  string
}

type failCaseOnceStore struct {
	*MemoryStore
	fail bool
}

func (s *failCaseOnceStore) RecordCaseResult(ctx context.Context, runID string, result CaseResult) error {
	if s.fail {
		s.fail = false
		return errors.New("simulated case result persistence failure")
	}
	return s.MemoryStore.RecordCaseResult(ctx, runID, result)
}

func (p evaluationProbeTool) Manifest() core.CapabilityManifest { return p.manifest }
func (p evaluationProbeTool) Execute(context.Context, core.CapabilityRequest) (core.CapabilityResult, error) {
	p.calls.Add(1)
	return core.CapabilityResult{Content: p.content, OK: true}, nil
}

type evaluationModel struct{ capability string }

func (evaluationModel) Provider() string         { return "evaluation-test" }
func (evaluationModel) ArtifactRevision() string { return "evaluation-test/v1" }
func (m evaluationModel) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	last := options.Messages[len(options.Messages)-1]
	if last.Role == core.RoleTool {
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: last.Content})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
		return nil
	}
	call := core.ToolCall{ID: "call-eval", Name: m.capability, Args: map[string]any{}}
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &call})
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
	return nil
}

type evaluationFixture struct {
	runner      *Runner
	store       *MemoryStore
	principal   core.Principal
	idempotent  *atomic.Int32
	destructive *atomic.Int32
}

type evaluationTelemetrySpan struct {
	recorder *evaluationTelemetry
	name     string
}

func (s evaluationTelemetrySpan) End(error, core.TelemetryAttributes) {
	s.recorder.spans[s.name]++
}

type evaluationTelemetry struct {
	spans      map[string]int
	counters   map[string]int64
	histograms map[string]int
}

func newEvaluationTelemetry() *evaluationTelemetry {
	return &evaluationTelemetry{spans: map[string]int{}, counters: map[string]int64{}, histograms: map[string]int{}}
}

func (r *evaluationTelemetry) Start(ctx context.Context, operation string, _ core.TelemetryAttributes) (context.Context, core.TelemetrySpan) {
	return ctx, evaluationTelemetrySpan{recorder: r, name: operation}
}
func (r *evaluationTelemetry) AddCounter(_ context.Context, name string, delta int64, _ core.TelemetryAttributes) {
	r.counters[name] += delta
}
func (r *evaluationTelemetry) RecordHistogram(_ context.Context, name string, _ float64, _ string, _ core.TelemetryAttributes) {
	r.histograms[name]++
}
func (*evaluationTelemetry) SetGauge(context.Context, string, float64, string, core.TelemetryAttributes) {
}

func newEvaluationFixture(t *testing.T, capability string) *evaluationFixture {
	t.Helper()
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product, _ := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "product"})
	tenant, _ := product.Child(core.ScopeRef{Kind: core.ScopeTenant, ID: "acme"})
	user, _ := tenant.Child(core.ScopeRef{Kind: core.ScopeUser, ID: "alice"})
	principal := core.Principal{
		SubjectID: "alice", TenantID: "acme", Scope: user,
		Grants: core.NewPermissionSet(core.PermRead, core.PermWrite),
	}
	var idempotentCalls, destructiveCalls atomic.Int32
	registry := core.NewCapabilityRegistry()
	idempotentManifest := core.CapabilityManifest{
		ID: "eval.lookup", Version: "1.0.0", Name: "Lookup", Kind: core.KindTool,
		Contract: "harness.tool/v1", RequiredPermissions: []core.Permission{core.PermRead},
		Idempotent: true, Tool: &core.ToolExposure{Parameters: map[string]any{"type": "object"}},
	}
	destructiveManifest := core.CapabilityManifest{
		ID: "eval.destroy", Version: "1.0.0", Name: "Destroy", Kind: core.KindTool,
		Contract: "harness.tool/v1", RequiredPermissions: []core.Permission{core.PermWrite},
		Tool: &core.ToolExposure{Parameters: map[string]any{"type": "object"}},
	}
	for _, provider := range []core.Capability{
		evaluationProbeTool{manifest: idempotentManifest, calls: &idempotentCalls, content: `{"value":"safe"}`},
		evaluationProbeTool{manifest: destructiveManifest, calls: &destructiveCalls, content: `{"destroyed":true}`},
	} {
		if err := registry.Register(product, provider); err != nil {
			t.Fatal(err)
		}
	}
	profiles := core.NewAgentProfileRegistry()
	name := "Evaluation Agent"
	model := core.ModelSelection{Provider: "evaluation-test", Model: "evaluation-test"}
	if err := profiles.Bind(core.AgentProfileLayer{
		Scope: product, ProfileID: "evaluation.agent", Name: &name, Model: &model,
		AddCapabilities: []string{idempotentManifest.ID, destructiveManifest.ID},
	}); err != nil {
		t.Fatal(err)
	}
	runtime := &core.Runtime{
		Capabilities: registry, Profiles: profiles,
		Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) {
			return evaluationModel{capability: capability}, nil
		}),
	}
	store := NewMemoryStore()
	return &evaluationFixture{
		runner: &Runner{Runtime: runtime, Sessions: core.NewMemorySessionStore(), Store: store},
		store:  store, principal: principal, idempotent: &idempotentCalls, destructive: &destructiveCalls,
	}
}

func baseDataset(capability string) Dataset {
	return Dataset{
		ID: "evaluation.dataset", Version: 1, Name: "Dataset", ProfileID: "evaluation.agent",
		Cases: []Case{{
			ID: "case-1", Input: "evaluate",
			Context: []ContextMessage{{Role: core.RoleUser, Text: "historical question"}, {Role: core.RoleAssistant, Text: "historical answer"}},
			Assertions: []Assertion{
				{ID: "status", Kind: AssertRunStatus, ExpectedStatus: core.RunCompleted},
				{ID: "contains", Kind: AssertAnswerContains, Expected: "safe"},
				{ID: "tool", Kind: AssertToolCalled, CapabilityID: capability},
			},
		}},
	}
}

func memoryStoreDataset(t *testing.T, store *MemoryStore, cases int) Dataset {
	t.Helper()
	if cases < 1 {
		t.Fatal("memory store dataset needs at least one case")
	}
	dataset := baseDataset("eval.lookup")
	for index := 2; index <= cases; index++ {
		evalCase := dataset.Cases[0]
		evalCase.ID = fmt.Sprintf("case-%d", index)
		evalCase.Input = fmt.Sprintf("evaluate %d", index)
		dataset.Cases = append(dataset.Cases, evalCase)
	}
	stored, created, err := store.PutDataset(context.Background(), dataset)
	if err != nil || !created {
		t.Fatalf("build memory store dataset: stored=%#v created=%t err=%v", stored, created, err)
	}
	return stored
}

func memoryStoreRun(dataset Dataset, id string) RunResult {
	return RunResult{
		ID: id, DatasetID: dataset.ID, DatasetVersion: dataset.Version, DatasetRevision: dataset.Revision,
		TenantID: "acme", SubjectID: "alice", ProfileID: "evaluation.override", Status: RunRunning,
		TotalCases: len(dataset.Cases), CompositionMetadata: map[string]string{"harness.assignment.id": "memory-assignment"},
		CreatedAt: time.Unix(1, 2_345_678).UTC(),
	}
}

func memoryStoreCaseResult(caseID string) CaseResult {
	return CaseResult{
		CaseID: caseID, SessionID: "evalsess_memory", AgentRunID: "evalcase_memory",
		Status: core.RunCompleted, Answer: "done", Score: 1, Passed: true,
		DurationMS: 1, CompletedAt: time.Unix(3, 4_567_890).UTC(),
	}
}

func evaluationMillis(value time.Time) time.Time {
	return time.UnixMilli(value.UnixMilli()).UTC()
}

func TestEvaluationRunnerExecutesIdempotentCapabilityAndPersistsArtifacts(t *testing.T) {
	fixture := newEvaluationFixture(t, "eval.lookup")
	dataset, created, err := fixture.store.PutDataset(context.Background(), baseDataset("eval.lookup"))
	if err != nil || !created || dataset.Revision == "" {
		t.Fatalf("dataset create failed: %#v created=%t err=%v", dataset, created, err)
	}
	run, err := fixture.runner.Run(context.Background(), RunRequest{
		DatasetID: dataset.ID, DatasetVersion: dataset.Version, Principal: fixture.principal,
	})
	if err != nil || run.Status != RunCompleted || !run.Passed || run.Score != 1 || run.PassedCases != 1 {
		t.Fatalf("evaluation run failed: %#v err=%v", run, err)
	}
	if fixture.idempotent.Load() != 1 || fixture.destructive.Load() != 0 {
		t.Fatalf("provider calls safe=%d destructive=%d", fixture.idempotent.Load(), fixture.destructive.Load())
	}
	if len(run.Cases) != 1 || run.Cases[0].Artifacts.ProfileSnapshotID == "" ||
		run.Cases[0].Artifacts.CapabilitySnapshotID == "" || run.Cases[0].Artifacts.ModelRevision == "" {
		t.Fatalf("artifact snapshot missing: %#v", run.Cases)
	}
	loaded, err := fixture.store.GetRun(context.Background(), run.ID)
	if err != nil || len(loaded.Cases) != 1 || loaded.Cases[0].SessionID == "" {
		t.Fatalf("stored run wrong: %#v err=%v", loaded, err)
	}
	session, err := fixture.runner.Sessions.Load(context.Background(), run.Cases[0].SessionID)
	if err != nil {
		t.Fatal(err)
	}
	metadata := session.Metadata()
	if metadata["evaluation.run_id"] != run.ID || metadata["evaluation.case_id"] != "case-1" || metadata["evaluation.dataset_revision"] != dataset.Revision {
		t.Fatalf("evaluation session metadata wrong: %#v", metadata)
	}
}

func TestEvaluationArtifactCarriesCompositionAndAssignmentRevisions(t *testing.T) {
	fixture := newEvaluationFixture(t, "eval.lookup")
	dataset, _, err := fixture.store.PutDataset(context.Background(), baseDataset("eval.lookup"))
	if err != nil {
		t.Fatal(err)
	}
	run, err := fixture.runner.Run(context.Background(), RunRequest{
		DatasetID: dataset.ID, DatasetVersion: dataset.Version, Principal: fixture.principal,
		CompositionMetadata: map[string]string{
			"harness.assignment.id":        "assignment-evaluation-1",
			"harness.assignment.candidate": "true",
		},
	})
	if err != nil || run.Status != RunCompleted || len(run.Cases) != 1 {
		t.Fatalf("evaluation failed: %#v err=%v", run, err)
	}
	artifacts := run.Cases[0].Artifacts
	if artifacts.CompositionRevision == "" || artifacts.AssignmentRevision == "" {
		t.Fatalf("composition evidence missing: %#v", artifacts)
	}
	loaded, err := fixture.store.GetRun(context.Background(), run.ID)
	if err != nil || loaded.CompositionMetadata["harness.assignment.id"] != "assignment-evaluation-1" {
		t.Fatalf("evaluation composition metadata was not durable: %#v err=%v", loaded, err)
	}
	caseSession, err := fixture.runner.Sessions.Load(context.Background(), run.Cases[0].SessionID)
	if err != nil {
		t.Fatal(err)
	}
	evidence, found, err := core.ExtractRunCompositionEvidence(caseSession.Events(), run.Cases[0].AgentRunID)
	if err != nil || !found || evidence.CompositionRevision != artifacts.CompositionRevision || evidence.AssignmentRevision != artifacts.AssignmentRevision {
		t.Fatalf("evaluation artifact does not match case composition evidence: evidence=%#v artifacts=%#v found=%t err=%v", evidence, artifacts, found, err)
	}
}

func TestMemoryStoreQueriesRunsByCompositionAndAssignmentRevision(t *testing.T) {
	fixture := newEvaluationFixture(t, "eval.lookup")
	dataset, _, err := fixture.store.PutDataset(context.Background(), baseDataset("eval.lookup"))
	if err != nil {
		t.Fatal(err)
	}
	firstMetadata := map[string]string{"harness.assignment.id": "first"}
	secondMetadata := map[string]string{"harness.assignment.id": "second"}
	first, err := fixture.runner.Run(context.Background(), RunRequest{
		DatasetID: dataset.ID, DatasetVersion: dataset.Version, Principal: fixture.principal,
		CompositionMetadata: firstMetadata,
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := fixture.runner.Run(context.Background(), RunRequest{
		DatasetID: dataset.ID, DatasetVersion: dataset.Version, Principal: fixture.principal,
		CompositionMetadata: secondMetadata,
	})
	if err != nil {
		t.Fatal(err)
	}
	firstAssignment, err := core.CompositionMetadataRevision(firstMetadata)
	if err != nil {
		t.Fatal(err)
	}
	firstComposition := first.Cases[0].Artifacts.CompositionRevision
	for name, query := range map[string]RunQuery{
		"assignment":  {DatasetID: dataset.ID, TenantID: fixture.principal.TenantID, AssignmentRevision: firstAssignment},
		"composition": {DatasetID: dataset.ID, TenantID: fixture.principal.TenantID, CompositionRevision: firstComposition},
	} {
		result, err := fixture.store.QueryRuns(context.Background(), query)
		if err != nil || len(result) != 1 || result[0].ID != first.ID {
			t.Fatalf("%s revision query wrong: %#v err=%v", name, result, err)
		}
	}
	if second.ID == first.ID || second.Cases[0].Artifacts.CompositionRevision == firstComposition {
		t.Fatal("fixture did not create distinct composition evidence")
	}
	if _, err := fixture.store.QueryRuns(context.Background(), RunQuery{TenantID: "acme\x00"}); err == nil {
		t.Fatal("NUL evaluation tenant query was accepted")
	}
	if _, err := fixture.store.QueryRuns(context.Background(), RunQuery{DatasetID: "not-namespaced"}); err == nil {
		t.Fatal("non-namespaced evaluation dataset query was accepted")
	}
}

func TestEvaluationRunnerBlocksNonIdempotentCapabilityByDefault(t *testing.T) {
	fixture := newEvaluationFixture(t, "eval.destroy")
	dataset := baseDataset("eval.destroy")
	dataset.Cases[0].Assertions = []Assertion{
		{ID: "status", Kind: AssertRunStatus, ExpectedStatus: core.RunCompleted},
		{ID: "blocked", Kind: AssertAnswerContains, Expected: "not available"},
	}
	stored, _, err := fixture.store.PutDataset(context.Background(), dataset)
	if err != nil {
		t.Fatal(err)
	}
	run, err := fixture.runner.Run(context.Background(), RunRequest{
		DatasetID: stored.ID, DatasetVersion: stored.Version, Principal: fixture.principal,
	})
	if err != nil || !run.Passed {
		t.Fatalf("safe evaluation failed: %#v err=%v", run, err)
	}
	if fixture.destructive.Load() != 0 {
		t.Fatalf("non-idempotent provider executed %d times", fixture.destructive.Load())
	}
}

func TestEvaluationRunnerRequiresExplicitAllowForNonIdempotentCapability(t *testing.T) {
	fixture := newEvaluationFixture(t, "eval.destroy")
	dataset := baseDataset("eval.destroy")
	dataset.Cases[0].Assertions[1].Expected = "destroyed"
	stored, _, err := fixture.store.PutDataset(context.Background(), dataset)
	if err != nil {
		t.Fatal(err)
	}
	run, err := fixture.runner.Run(context.Background(), RunRequest{
		DatasetID: stored.ID, DatasetVersion: stored.Version, Principal: fixture.principal,
		AllowCapabilities: []string{"eval.destroy"},
	})
	if err != nil || !run.Passed || fixture.destructive.Load() != 1 {
		t.Fatalf("explicit destructive evaluation failed: %#v calls=%d err=%v", run, fixture.destructive.Load(), err)
	}
}

func TestEvaluationFilterNeverAllowsApprovalCapability(t *testing.T) {
	filter := safeEvaluationFilter([]string{"eval.approval"})
	allowed := filter.AllowCapability(core.CapabilityManifest{
		ID: "eval.approval", Version: "1", Name: "Approval", Kind: core.KindTool,
		Idempotent: true, RequiresApproval: true, Tool: &core.ToolExposure{},
	})
	if allowed {
		t.Fatal("evaluation filter allowed a capability requiring approval")
	}
}

func TestDatasetRevisionAndComparisonAreDeterministic(t *testing.T) {
	left := baseDataset("eval.lookup")
	right := baseDataset("eval.lookup")
	if err := ValidateDataset(&left); err != nil {
		t.Fatal(err)
	}
	if err := ValidateDataset(&right); err != nil {
		t.Fatal(err)
	}
	if left.Revision != right.Revision {
		t.Fatalf("dataset revision changed: %s vs %s", left.Revision, right.Revision)
	}
	baseline := RunResult{ID: "eval-base", DatasetID: left.ID, DatasetVersion: left.Version, DatasetRevision: left.Revision, Status: RunCompleted, Score: 1, Passed: true, Cases: []CaseResult{{CaseID: "case-1", Score: 1}}}
	current := RunResult{ID: "eval-current", DatasetID: left.ID, DatasetVersion: left.Version, DatasetRevision: left.Revision, Status: RunCompleted, Score: 0.8, Passed: false, Cases: []CaseResult{{CaseID: "case-1", Score: 0.8}}}
	comparison, err := Compare(current, baseline, 0.05)
	if err != nil || !comparison.Regressed || math.Abs(comparison.ScoreDelta+0.2) > 1e-9 || math.Abs(comparison.CaseDeltas["case-1"]+0.2) > 1e-9 {
		t.Fatalf("comparison wrong: %#v err=%v", comparison, err)
	}
	minimum, tolerance := 0.9, 0.05
	gate, err := EvaluateGate(current, &baseline, GatePolicy{
		RequirePassed: true, MinScore: &minimum, MaxRegression: &tolerance,
	})
	if err != nil || gate.Passed || len(gate.Reasons) < 2 || gate.Comparison == nil || !gate.Comparison.Regressed {
		t.Fatalf("regression gate wrong: %#v err=%v", gate, err)
	}
}

func TestEvaluatorPanicIsContainedAsFailedAssertion(t *testing.T) {
	registry, err := NewRegistry(EvaluatorFunc{
		AssertionKind: AssertAnswerContains,
		EvaluateFunc:  func(context.Context, Assertion, Observation) (AssertionResult, error) { panic("judge panic") },
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = registry.Evaluate(context.Background(), Assertion{ID: "a", Kind: AssertAnswerContains}, Observation{})
	if err == nil {
		t.Fatal("evaluator panic was not contained")
	}
}

func TestCustomNamespacedEvaluatorKind(t *testing.T) {
	kind := AssertionKind("acme.semantic_judge")
	dataset := baseDataset("eval.lookup")
	dataset.Cases[0].Assertions = []Assertion{{ID: "custom", Kind: kind, Expected: "yes"}}
	if err := ValidateDataset(&dataset); err != nil {
		t.Fatalf("custom assertion kind was rejected: %v", err)
	}
	registry, err := NewRegistry(EvaluatorFunc{
		AssertionKind: kind,
		EvaluateFunc: func(context.Context, Assertion, Observation) (AssertionResult, error) {
			return AssertionResult{Score: 0.75, Passed: true, Message: "custom"}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := registry.Evaluate(context.Background(), dataset.Cases[0].Assertions[0], Observation{})
	if err != nil || result.Score != 0.75 || !result.Passed || result.Kind != kind {
		t.Fatalf("custom evaluator wrong: %#v err=%v", result, err)
	}
}

func TestDatasetJSONRoundTripPreservesRevision(t *testing.T) {
	dataset := baseDataset("eval.lookup")
	if err := ValidateDataset(&dataset); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(dataset)
	if err != nil {
		t.Fatal(err)
	}
	var restored Dataset
	if err := json.Unmarshal(encoded, &restored); err != nil {
		t.Fatal(err)
	}
	if err := ValidateDataset(&restored); err != nil || restored.Revision != dataset.Revision {
		t.Fatalf("dataset round trip failed: %#v err=%v", restored, err)
	}
}

func TestEvaluationResumeReconstructsCompletedSessionWithoutProviderReplay(t *testing.T) {
	fixture := newEvaluationFixture(t, "eval.lookup")
	flaky := &failCaseOnceStore{MemoryStore: fixture.store, fail: true}
	fixture.runner.Store = flaky
	dataset, _, err := flaky.PutDataset(context.Background(), baseDataset("eval.lookup"))
	if err != nil {
		t.Fatal(err)
	}
	interrupted, err := fixture.runner.Run(context.Background(), RunRequest{
		DatasetID: dataset.ID, DatasetVersion: dataset.Version, Principal: fixture.principal,
	})
	if err == nil || interrupted.Status != RunRunning || fixture.idempotent.Load() != 1 {
		t.Fatalf("evaluation interruption wrong: %#v calls=%d err=%v", interrupted, fixture.idempotent.Load(), err)
	}
	stored, err := flaky.GetRun(context.Background(), interrupted.ID)
	if err != nil || stored.Status != RunRunning || len(stored.Cases) != 0 || stored.Error == "" {
		t.Fatalf("durable interrupted run wrong: %#v err=%v", stored, err)
	}
	resumer := &Runner{
		Runtime: fixture.runner.Runtime, Sessions: fixture.runner.Sessions,
		Store: flaky, Evaluators: fixture.runner.Evaluators,
	}
	resumed, err := resumer.Resume(context.Background(), interrupted.ID, fixture.principal)
	if err != nil || resumed.Status != RunCompleted || !resumed.Passed || len(resumed.Cases) != 1 {
		t.Fatalf("evaluation resume failed: %#v err=%v", resumed, err)
	}
	if fixture.idempotent.Load() != 1 {
		t.Fatalf("resume replayed provider: calls=%d", fixture.idempotent.Load())
	}
	second, err := resumer.Resume(context.Background(), interrupted.ID, fixture.principal)
	if err != nil || second.Status != RunCompleted || fixture.idempotent.Load() != 1 {
		t.Fatalf("terminal resume was not idempotent: %#v calls=%d err=%v", second, fixture.idempotent.Load(), err)
	}
	changed := fixture.principal
	segments := changed.Scope.Segments()
	segments[len(segments)-1] = core.ScopeRef{Kind: core.ScopeUser, ID: "other@acme.test"}
	changed.Scope, _ = core.NewScopePath(segments...)
	changed.SubjectID = fixture.principal.SubjectID
	// Completed runs are returned idempotently; ownership is still checked
	// before that terminal shortcut.
	if _, err := resumer.Resume(context.Background(), interrupted.ID, changed); err == nil {
		t.Fatal("evaluation resume accepted a changed principal scope")
	}
}

func TestEvaluationRunnerEmitsRunAndCaseTelemetry(t *testing.T) {
	fixture := newEvaluationFixture(t, "eval.lookup")
	recorder := newEvaluationTelemetry()
	fixture.runner.Runtime.Telemetry = recorder
	dataset, _, err := fixture.store.PutDataset(context.Background(), baseDataset("eval.lookup"))
	if err != nil {
		t.Fatal(err)
	}
	run, err := fixture.runner.Run(context.Background(), RunRequest{
		DatasetID: dataset.ID, DatasetVersion: dataset.Version, Principal: fixture.principal,
	})
	if err != nil || !run.Passed {
		t.Fatalf("evaluation run failed: %#v err=%v", run, err)
	}
	for _, name := range []string{core.SpanEvaluationRun, core.SpanEvaluationCase} {
		if recorder.spans[name] == 0 {
			t.Fatalf("evaluation span %s missing: %#v", name, recorder.spans)
		}
	}
	for _, name := range []string{core.MetricEvaluationRuns, core.MetricEvaluationCases} {
		if recorder.counters[name] == 0 {
			t.Fatalf("evaluation counter %s missing: %#v", name, recorder.counters)
		}
	}
	for _, name := range []string{core.MetricEvaluationDuration, core.MetricEvaluationCaseDuration, core.MetricEvaluationScore} {
		if recorder.histograms[name] == 0 {
			t.Fatalf("evaluation histogram %s missing: %#v", name, recorder.histograms)
		}
	}
}

func TestEvaluationResumeRepairsInterruptedCaseSessionWithoutModelCall(t *testing.T) {
	fixture := newEvaluationFixture(t, "eval.lookup")
	dataset := baseDataset("eval.lookup")
	dataset.Cases[0].Assertions = []Assertion{{ID: "status", Kind: AssertRunStatus, ExpectedStatus: core.RunFailed}}
	storedDataset, _, err := fixture.store.PutDataset(context.Background(), dataset)
	if err != nil {
		t.Fatal(err)
	}
	evalRun := RunResult{
		ID: "eval_interrupted_case", DatasetID: storedDataset.ID, DatasetVersion: storedDataset.Version,
		DatasetRevision: storedDataset.Revision, TenantID: fixture.principal.TenantID,
		SubjectID: fixture.principal.SubjectID, ProfileID: storedDataset.ProfileID,
		Status: RunRunning, TotalCases: 1, CreatedAt: time.Now().UTC(),
		Metadata: map[string]string{"evaluation.principal_scope": fixture.principal.Scope.String()},
	}
	if err := fixture.store.CreateRun(context.Background(), evalRun); err != nil {
		t.Fatal(err)
	}
	sessionID, agentRunID, _ := evaluationCaseIDs(evalRun.ID, storedDataset.Cases[0].ID)
	scope, _ := fixture.principal.Scope.Child(core.ScopeRef{Kind: core.ScopeSession, ID: sessionID})
	session, err := core.NewSession(core.SessionOptions{
		ID: sessionID, ProfileID: storedDataset.ProfileID, Principal: fixture.principal, Scope: scope,
		Metadata: map[string]string{
			"evaluation.run_id": evalRun.ID, "evaluation.dataset_id": storedDataset.ID,
			"evaluation.dataset_version": "1", "evaluation.dataset_revision": storedDataset.Revision,
			"evaluation.case_id": storedDataset.Cases[0].ID,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append(agentRunID, core.EvRunStart, core.RunStartData{}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append(agentRunID, core.EvUserMessage, core.UserMessageData{Text: "interrupted"}); err != nil {
		t.Fatal(err)
	}
	if err := fixture.runner.Sessions.Create(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	fixture.runner.Runtime.Models = core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) {
		return nil, errors.New("model must not run during repaired resume")
	})
	resumed, err := fixture.runner.Resume(context.Background(), evalRun.ID, fixture.principal)
	if err != nil || resumed.Status != RunCompleted || !resumed.Passed || len(resumed.Cases) != 1 || resumed.Cases[0].Status != core.RunFailed {
		t.Fatalf("interrupted case repair failed: %#v err=%v", resumed, err)
	}
	repaired, err := fixture.runner.Sessions.Load(context.Background(), sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if status, exists := repaired.RunStatus(agentRunID); !exists || status != core.RunFailed {
		t.Fatalf("case session was not repaired: status=%q exists=%t", status, exists)
	}
}

func TestMemoryStoreBindsRunToStoredDataset(t *testing.T) {
	store := NewMemoryStore()
	dataset := memoryStoreDataset(t, store, 2)

	override := memoryStoreRun(dataset, "eval_memory_profile_override")
	if err := store.CreateRun(context.Background(), override); err != nil {
		t.Fatalf("explicit profile override was rejected: %v", err)
	}
	retry := override
	retry.CreatedAt = time.Unix(99, 100).UTC()
	if err := store.CreateRun(context.Background(), retry); err != nil {
		t.Fatalf("same immutable run retry was rejected: %v", err)
	}
	stored, err := store.GetRun(context.Background(), override.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !stored.CreatedAt.Equal(evaluationMillis(override.CreatedAt)) {
		t.Fatalf("run retry changed durable creation time: %#v", stored)
	}

	for _, test := range []struct {
		name   string
		id     string
		mutate func(*RunResult)
	}{
		{name: "unknown dataset", id: "eval_memory_bind_unknown", mutate: func(run *RunResult) { run.DatasetID = "evaluation.missing" }},
		{name: "version", id: "eval_memory_bind_version", mutate: func(run *RunResult) { run.DatasetVersion++ }},
		{name: "revision", id: "eval_memory_bind_revision", mutate: func(run *RunResult) {
			run.DatasetRevision = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		}},
		{name: "partial total", id: "eval_memory_bind_partial", mutate: func(run *RunResult) { run.TotalCases-- }},
		{name: "expanded total", id: "eval_memory_bind_expanded", mutate: func(run *RunResult) { run.TotalCases++ }},
	} {
		t.Run(test.name, func(t *testing.T) {
			run := memoryStoreRun(dataset, test.id)
			test.mutate(&run)
			if err := store.CreateRun(context.Background(), run); err == nil {
				t.Fatalf("run with changed %s was accepted", test.name)
			}
		})
	}
}

func TestMemoryStoreGuardsBoundCaseResultsAndTerminalReplay(t *testing.T) {
	store := NewMemoryStore()
	dataset := memoryStoreDataset(t, store, 2)
	run := memoryStoreRun(dataset, "eval_memory_case_guard")
	if err := store.CreateRun(context.Background(), run); err != nil {
		t.Fatal(err)
	}

	invalid := memoryStoreCaseResult(dataset.Cases[0].ID)
	invalid.SessionID = ""
	if err := store.RecordCaseResult(context.Background(), run.ID, invalid); err == nil {
		t.Fatal("invalid case result was accepted")
	}
	unknown := memoryStoreCaseResult("invented-case")
	if err := store.RecordCaseResult(context.Background(), run.ID, unknown); err == nil {
		t.Fatal("case outside immutable dataset was accepted")
	}
	result := memoryStoreCaseResult(dataset.Cases[0].ID)
	if err := store.RecordCaseResult(context.Background(), run.ID, result); err != nil {
		t.Fatal(err)
	}

	terminal := run
	terminal.Status, terminal.Error, terminal.CompletedAt = RunFailed, "interrupted", time.Unix(5, 6).UTC()
	if err := store.FinishRun(context.Background(), terminal); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCaseResult(context.Background(), run.ID, result); err != nil {
		t.Fatalf("canonical terminal replay failed: %v", err)
	}
	changed := result
	changed.Answer = "changed"
	if err := store.RecordCaseResult(context.Background(), run.ID, changed); err == nil {
		t.Fatal("changed terminal replay was accepted")
	}
	if err := store.RecordCaseResult(context.Background(), run.ID, memoryStoreCaseResult(dataset.Cases[1].ID)); err == nil {
		t.Fatal("terminal run accepted a new case result")
	}
}

func TestMemoryStoreFinishRequiresBoundHeaderAndPersistedCases(t *testing.T) {
	newRun := func(t *testing.T, id string) (*MemoryStore, Dataset, RunResult, []CaseResult) {
		t.Helper()
		store := NewMemoryStore()
		dataset := memoryStoreDataset(t, store, 2)
		run := memoryStoreRun(dataset, id)
		if err := store.CreateRun(context.Background(), run); err != nil {
			t.Fatal(err)
		}
		results := []CaseResult{memoryStoreCaseResult(dataset.Cases[0].ID), memoryStoreCaseResult(dataset.Cases[1].ID)}
		return store, dataset, run, results
	}
	completed := func(run RunResult) RunResult {
		run.Status, run.Score, run.Passed, run.PassedCases = RunCompleted, 1, true, 2
		run.CompletedAt = time.Unix(7, 8_901_234).UTC()
		return run
	}

	t.Run("completed requires every dataset case", func(t *testing.T) {
		store, _, run, results := newRun(t, "eval_memory_finish_missing")
		if err := store.RecordCaseResult(context.Background(), run.ID, results[0]); err != nil {
			t.Fatal(err)
		}
		if err := store.FinishRun(context.Background(), completed(run)); err == nil {
			t.Fatal("completed run with a missing dataset case was accepted")
		}
	})

	t.Run("immutable header and supplied cases", func(t *testing.T) {
		store, _, run, results := newRun(t, "eval_memory_finish_header")
		for _, result := range results {
			if err := store.RecordCaseResult(context.Background(), run.ID, result); err != nil {
				t.Fatal(err)
			}
		}
		changedHeader := completed(run)
		changedHeader.ProfileID = "evaluation.changed"
		if err := store.FinishRun(context.Background(), changedHeader); err == nil {
			t.Fatal("changed immutable header was accepted")
		}
		mismatchedCases := completed(run)
		mismatchedCases.Cases = []CaseResult{results[0]}
		if err := store.FinishRun(context.Background(), mismatchedCases); err == nil {
			t.Fatal("mismatched supplied case results were accepted")
		}
		matchingCases := completed(run)
		matchingCases.Cases = []CaseResult{results[1], results[0]}
		if err := store.FinishRun(context.Background(), matchingCases); err != nil {
			t.Fatalf("matching supplied case results were rejected: %v", err)
		}
	})

	t.Run("terminal replay leaves stored timestamps unchanged", func(t *testing.T) {
		store, _, run, results := newRun(t, "eval_memory_finish_replay")
		for _, result := range results {
			if err := store.RecordCaseResult(context.Background(), run.ID, result); err != nil {
				t.Fatal(err)
			}
		}
		first := completed(run)
		if err := store.FinishRun(context.Background(), first); err != nil {
			t.Fatal(err)
		}
		retry := first
		retry.CreatedAt = time.Unix(99, 100).UTC()
		retry.CompletedAt = time.Unix(101, 102).UTC()
		if err := store.FinishRun(context.Background(), retry); err != nil {
			t.Fatalf("equivalent terminal replay failed: %v", err)
		}
		stored, err := store.GetRun(context.Background(), run.ID)
		if err != nil {
			t.Fatal(err)
		}
		if !stored.CreatedAt.Equal(evaluationMillis(first.CreatedAt)) || !stored.CompletedAt.Equal(evaluationMillis(first.CompletedAt)) {
			t.Fatalf("terminal replay changed durable timestamps: %#v", stored)
		}
		if len(stored.Cases) != len(results) || !stored.Cases[0].CompletedAt.Equal(results[0].CompletedAt) {
			t.Fatalf("case result time was not preserved exactly: %#v", stored.Cases)
		}
		changed := first
		changed.Error = "changed"
		if err := store.FinishRun(context.Background(), changed); err == nil {
			t.Fatal("changed terminal result was accepted")
		}
	})
}

func TestMemoryStoreFinishRejectsNonFiniteScoreWithoutMutation(t *testing.T) {
	for _, test := range []struct {
		name  string
		id    string
		score float64
	}{
		{name: "nan", id: "eval_memory_nonfinite_nan", score: math.NaN()},
		{name: "positive infinity", id: "eval_memory_nonfinite_posinf", score: math.Inf(1)},
		{name: "negative infinity", id: "eval_memory_nonfinite_neginf", score: math.Inf(-1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := NewMemoryStore()
			dataset := memoryStoreDataset(t, store, 1)
			run := memoryStoreRun(dataset, test.id)
			if err := store.CreateRun(context.Background(), run); err != nil {
				t.Fatal(err)
			}
			terminal := run
			terminal.Status, terminal.Score, terminal.CompletedAt = RunFailed, test.score, time.Unix(9, 10).UTC()
			if err := store.FinishRun(context.Background(), terminal); err == nil {
				stored, getErr := store.GetRun(context.Background(), run.ID)
				t.Fatalf("non-finite score was accepted; stored=%#v getErr=%v", stored, getErr)
			}
			stored, err := store.GetRun(context.Background(), run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if stored.Status != RunRunning || stored.Score != 0 {
				t.Fatalf("rejected terminal score changed durable run: %#v", stored)
			}
		})
	}
}

func TestMemoryStoreRejectsRunningOverflow(t *testing.T) {
	store := NewMemoryStore()
	store.maxRunning = 2
	ctx := context.Background()
	dataset := memoryStoreDataset(t, store, 1)
	run := func(id string) RunResult {
		return memoryStoreRun(dataset, id)
	}
	if err := store.CreateRun(ctx, run("eval_run_1")); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateRun(ctx, run("eval_run_2")); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateRun(ctx, run("eval_run_3")); err == nil {
		t.Fatal("running evaluation overflow was accepted")
	}
	finished := run("eval_run_1")
	finished.Status = RunFailed
	finished.Error = "aborted"
	finished.CompletedAt = time.Now().UTC()
	if err := store.FinishRun(ctx, finished); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateRun(ctx, run("eval_run_3")); err != nil {
		t.Fatalf("failed evaluation run did not free a slot: %v", err)
	}
}

func TestMemoryStoreRejectsDatasetVersionOverflow(t *testing.T) {
	store := NewMemoryStore()
	store.maxDatasetVersions = 2
	ctx := context.Background()
	first, created, err := store.PutDataset(ctx, baseDataset("eval.lookup"))
	if err != nil || !created {
		t.Fatalf("first dataset version failed: %#v created=%t err=%v", first, created, err)
	}
	second := baseDataset("eval.lookup")
	second.Version = 2
	if _, _, err := store.PutDataset(ctx, second); err != nil {
		t.Fatal(err)
	}
	third := baseDataset("eval.lookup")
	third.Version = 3
	if _, _, err := store.PutDataset(ctx, third); err == nil {
		t.Fatal("dataset version overflow was accepted")
	}
	replayed, created, err := store.PutDataset(ctx, first)
	if err != nil || created || replayed.Revision != first.Revision {
		t.Fatalf("existing dataset version was blocked by the cap: %#v created=%t err=%v", replayed, created, err)
	}
}

func TestMemoryStoreRejectsDatasetIDOverflow(t *testing.T) {
	store := NewMemoryStore()
	store.maxDatasetIDs = 2
	ctx := context.Background()
	put := func(id string) error {
		dataset := baseDataset("eval.lookup")
		dataset.ID = id
		_, _, err := store.PutDataset(ctx, dataset)
		return err
	}
	if err := put("evaluation.one"); err != nil {
		t.Fatal(err)
	}
	if err := put("evaluation.two"); err != nil {
		t.Fatal(err)
	}
	if err := put("evaluation.three"); err == nil {
		t.Fatal("dataset id overflow was accepted")
	}
	second := baseDataset("eval.lookup")
	second.ID = "evaluation.one"
	second.Version = 2
	if _, _, err := store.PutDataset(ctx, second); err != nil {
		t.Fatalf("existing dataset id was blocked from a new version: %v", err)
	}
	replay := baseDataset("eval.lookup")
	replay.ID = "evaluation.one"
	if _, created, err := store.PutDataset(ctx, replay); err != nil || created {
		t.Fatalf("existing dataset version was blocked by the id cap: created=%t err=%v", created, err)
	}
}

func TestMemoryStoreRejectsStoredRunOverflow(t *testing.T) {
	store := NewMemoryStore()
	store.maxRuns = 2
	ctx := context.Background()
	dataset := memoryStoreDataset(t, store, 1)
	run := func(id string) RunResult {
		return memoryStoreRun(dataset, id)
	}
	if err := store.CreateRun(ctx, run("eval_stored_1")); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateRun(ctx, run("eval_stored_2")); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateRun(ctx, run("eval_stored_3")); err == nil {
		t.Fatal("stored evaluation run overflow was accepted")
	}
}

func TestMemoryStoreRejectsCaseOverflow(t *testing.T) {
	store := NewMemoryStore()
	store.maxCases = 2
	ctx := context.Background()
	dataset := memoryStoreDataset(t, store, 3)
	run := memoryStoreRun(dataset, "eval_case_cap")
	if err := store.CreateRun(ctx, run); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCaseResult(ctx, run.ID, memoryStoreCaseResult(dataset.Cases[0].ID)); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCaseResult(ctx, run.ID, memoryStoreCaseResult(dataset.Cases[1].ID)); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCaseResult(ctx, run.ID, memoryStoreCaseResult(dataset.Cases[2].ID)); err == nil {
		t.Fatal("memory case overflow was accepted")
	}
}
