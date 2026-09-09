package evaluation

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/whhhh1500/auto-agent/pkg/app/effectreceipt"
	core "github.com/whhhh1500/auto-agent/pkg/core"
)

// coverageRuntimeJournal is a test-only journal that also exposes the
// optional structural listing port used by CoverageContract collection.
type coverageRuntimeJournal struct {
	mu      sync.Mutex
	records map[core.ToolInvocation]core.ToolInvocationRecord
}

func newCoverageRuntimeJournal() *coverageRuntimeJournal {
	return &coverageRuntimeJournal{records: map[core.ToolInvocation]core.ToolInvocationRecord{}}
}

func (j *coverageRuntimeJournal) clear() {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.records = map[core.ToolInvocation]core.ToolInvocationRecord{}
}

func (j *coverageRuntimeJournal) BeginToolInvocation(_ context.Context, invocation core.ToolInvocation) (core.ToolInvocationRecord, core.ToolInvocationDecision, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if record, found := j.records[invocation]; found {
		switch record.State {
		case core.ToolInvocationCompleted:
			return core.CloneToolInvocationRecord(record), core.ToolInvocationReplay, nil
		case core.ToolInvocationUncertain:
			return core.CloneToolInvocationRecord(record), core.ToolInvocationUnknown, nil
		default:
			return core.CloneToolInvocationRecord(record), core.ToolInvocationExecuteRetry, nil
		}
	}
	now := time.Now().UTC()
	record := core.ToolInvocationRecord{ToolInvocation: invocation, State: core.ToolInvocationStarted, StartedAt: now, UpdatedAt: now}
	j.records[invocation] = record
	return core.CloneToolInvocationRecord(record), core.ToolInvocationExecuteNew, nil
}

func (j *coverageRuntimeJournal) CompleteToolInvocation(_ context.Context, invocation core.ToolInvocation, result core.CapabilityResult) (core.ToolInvocationRecord, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	record, found := j.records[invocation]
	if !found {
		return core.ToolInvocationRecord{}, errors.New("coverage test journal has no invocation")
	}
	if record.State != core.ToolInvocationStarted {
		return core.ToolInvocationRecord{}, errors.New("coverage test journal invocation is not started")
	}
	resultCopy := result
	record.Result = &resultCopy
	record.State = core.ToolInvocationCompleted
	record.UpdatedAt, record.CompletedAt = time.Now().UTC(), time.Now().UTC()
	j.records[invocation] = record
	return core.CloneToolInvocationRecord(record), nil
}

func (j *coverageRuntimeJournal) MarkToolInvocationUncertain(_ context.Context, invocation core.ToolInvocation, errorCode string) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	record, found := j.records[invocation]
	if !found {
		return errors.New("coverage test journal has no invocation")
	}
	record.State, record.ErrorCode, record.UpdatedAt = core.ToolInvocationUncertain, errorCode, time.Now().UTC()
	j.records[invocation] = record
	return nil
}

func (j *coverageRuntimeJournal) GetToolInvocation(_ context.Context, invocation core.ToolInvocation) (core.ToolInvocationRecord, bool, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	record, found := j.records[invocation]
	return core.CloneToolInvocationRecord(record), found, nil
}

func (j *coverageRuntimeJournal) ListRunToolInvocations(_ context.Context, principal core.Principal, sessionID, runID string) ([]core.ToolInvocationRecord, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	result := make([]core.ToolInvocationRecord, 0)
	for _, record := range j.records {
		if record.TenantID == principal.TenantID && record.SubjectID == principal.SubjectID && record.SessionID == sessionID && record.RunID == runID {
			result = append(result, core.CloneToolInvocationRecord(record))
		}
	}
	sort.Slice(result, func(i, k int) bool { return result[i].CallID < result[k].CallID })
	return result, nil
}

type coverageRuntimeReceiptReader struct {
	state effectreceipt.State
	err   error
	calls atomic.Int32
}

type coverageConflictingJournal struct {
	*coverageRuntimeJournal
	records []core.ToolInvocationRecord
}

func (j *coverageConflictingJournal) ListRunToolInvocations(context.Context, core.Principal, string, string) ([]core.ToolInvocationRecord, error) {
	return append([]core.ToolInvocationRecord(nil), j.records...), nil
}

func (r *coverageRuntimeReceiptReader) ListRun(_ context.Context, scope effectreceipt.RunScope, cursor effectreceipt.RunCursor, _ int) ([]effectreceipt.RecoveryRecord, effectreceipt.RunCursor, error) {
	r.calls.Add(1)
	if r.err != nil {
		return nil, effectreceipt.RunCursor{}, r.err
	}
	if cursor.CallID != "" {
		return nil, cursor, nil
	}
	invocation := core.ToolInvocation{
		TenantID: scope.TenantID, SubjectID: scope.SubjectID, SessionID: scope.SessionID, RunID: scope.RunID,
		CallID: "coverage-effect-call", CapabilityID: "eval.lookup", ArgsDigest: effectreceipt.SHA256Digest([]byte("coverage-args")), Idempotent: true,
	}
	intent, err := effectreceipt.NewIntent(invocation, effectreceipt.DriverRef{ID: "coverage-test", Version: "v1"}, []byte("target"), []byte("payload"))
	if err != nil {
		return nil, effectreceipt.RunCursor{}, err
	}
	record := effectreceipt.Record{Intent: intent, State: r.state, DispatchAttempts: 1}
	switch r.state {
	case effectreceipt.StateConfirmed:
		record.EvidenceDigest = effectreceipt.SHA256Digest([]byte("confirmed-evidence"))
	case effectreceipt.StateAccepted:
		record.ReceiptDigest = effectreceipt.SHA256Digest([]byte("accepted-receipt"))
	case effectreceipt.StateUnknown:
		record.ErrorCode = effectreceipt.CodeReadBackUnknown
	}
	return []effectreceipt.RecoveryRecord{{Record: record, CreatedAt: time.Unix(1, 0).UTC(), UpdatedAt: time.Unix(1, 0).UTC()}}, effectreceipt.RunCursor{CallID: invocation.CallID}, nil
}

func coverageEffectContract(level EffectReceiptLevel) *CoverageContract {
	return &CoverageContract{
		Version: CoverageContractV1,
		Effects: []EffectCoverageRequirement{{
			ID: "lookup", CapabilityID: "eval.lookup", MinOccurrences: 1, MaxOccurrences: 1, ReceiptLevel: level,
		}},
	}
}

func TestCoverageRuntimeCollectsCompletedJournalEffectsForNewRun(t *testing.T) {
	fixture := newEvaluationFixture(t, "eval.lookup")
	fixture.runner.Runtime.ToolJournal = newCoverageRuntimeJournal()
	dataset := baseDataset("eval.lookup")
	dataset.Cases[0].Coverage = coverageEffectContract(EffectReceiptJournalCompleted)
	stored, created, err := fixture.store.PutDataset(context.Background(), dataset)
	if err != nil || !created {
		t.Fatalf("store dataset: created=%t err=%v", created, err)
	}
	run, err := fixture.runner.Run(context.Background(), RunRequest{DatasetID: stored.ID, DatasetVersion: stored.Version, Principal: fixture.principal})
	if err != nil || len(run.Cases) != 1 || run.Cases[0].Coverage == nil {
		t.Fatalf("run with journal coverage: %#v err=%v", run, err)
	}
	coverage := run.Cases[0].Coverage
	if coverage.Status != CoverageSatisfied || len(coverage.Effects) != 1 || coverage.Effects[0].Occurrences != 1 || coverage.Effects[0].ReceiptLevel != EffectReceiptJournalCompleted {
		t.Fatalf("journal coverage does not prove one completed effect: %#v", coverage)
	}
}

func TestCoverageRuntimeJournalUncertainCallLeavesRequirementUnavailable(t *testing.T) {
	fixture := newEvaluationFixture(t, "eval.lookup")
	journal := newCoverageRuntimeJournal()
	fixture.runner.Runtime.ToolJournal = journal
	sessionID, runID, _ := evaluationCaseIDs("eval_coverage_uncertain", "case-1")
	invocation := core.ToolInvocation{
		TenantID: fixture.principal.TenantID, SubjectID: fixture.principal.SubjectID, SessionID: sessionID, RunID: runID,
		CallID: "coverage-uncertain-call", CapabilityID: "eval.lookup", ArgsDigest: effectreceipt.SHA256Digest([]byte("uncertain-args")), Idempotent: true,
	}
	if _, _, err := journal.BeginToolInvocation(context.Background(), invocation); err != nil {
		t.Fatal(err)
	}
	if err := journal.MarkToolInvocationUncertain(context.Background(), invocation, core.CodeToolJournalUnavailable); err != nil {
		t.Fatal(err)
	}
	effects := fixture.runner.coverageEffectEvidence(context.Background(), fixture.principal, sessionID, runID, []EffectCoverageRequirement{{
		ID: "lookup", CapabilityID: "eval.lookup", MinOccurrences: 0, MaxOccurrences: 1, ReceiptLevel: EffectReceiptJournalCompleted,
	}})
	if len(effects) != 1 || effects[0].Status != CoverageUnavailable {
		t.Fatalf("uncertain journal effect was treated as countable: %#v", effects)
	}
}

func TestCoverageRuntimeConflictingEvidenceIsUnavailableInsteadOfCaseExecutionError(t *testing.T) {
	fixture := newEvaluationFixture(t, "eval.lookup")
	sessionID, runID, _ := evaluationCaseIDs("eval_coverage_conflict", "case-1")
	conflicting := core.ToolInvocation{
		TenantID: "other-tenant", SubjectID: fixture.principal.SubjectID, SessionID: sessionID, RunID: runID,
		CallID: "coverage-conflicting-call", CapabilityID: "eval.lookup", ArgsDigest: effectreceipt.SHA256Digest([]byte("conflicting-args")), Idempotent: true,
	}
	result := core.CapabilityResult{OK: true}
	fixture.runner.Runtime.ToolJournal = &coverageConflictingJournal{
		coverageRuntimeJournal: newCoverageRuntimeJournal(),
		records: []core.ToolInvocationRecord{{
			ToolInvocation: conflicting, State: core.ToolInvocationCompleted, Result: &result,
		}},
	}
	effects := fixture.runner.coverageEffectEvidence(context.Background(), fixture.principal, sessionID, runID, []EffectCoverageRequirement{{
		ID: "lookup", CapabilityID: "eval.lookup", MinOccurrences: 1, MaxOccurrences: 1, ReceiptLevel: EffectReceiptJournalCompleted,
	}})
	if len(effects) != 1 || effects[0].Status != CoverageUnavailable {
		t.Fatalf("conflicting journal evidence was not unavailable: %#v", effects)
	}
}

func TestCoverageRuntimeProviderReadBackDoesNotTreatAcknowledgedOrUnknownAsEffect(t *testing.T) {
	for _, test := range []struct {
		name   string
		state  effectreceipt.State
		err    error
		status CoverageStatus
	}{
		{name: "confirmed", state: effectreceipt.StateConfirmed, status: CoverageSatisfied},
		{name: "accepted", state: effectreceipt.StateAccepted, status: CoverageUnavailable},
		{name: "unknown", state: effectreceipt.StateUnknown, status: CoverageUnavailable},
		{name: "reader-error", err: errors.New("receipt reader unavailable"), status: CoverageUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newEvaluationFixture(t, "eval.lookup")
			reader := &coverageRuntimeReceiptReader{state: test.state, err: test.err}
			fixture.runner.EffectReceipts = reader
			dataset := baseDataset("eval.lookup")
			dataset.Cases[0].Coverage = coverageEffectContract(EffectReceiptProviderReadBack)
			stored, created, err := fixture.store.PutDataset(context.Background(), dataset)
			if err != nil || !created {
				t.Fatalf("store dataset: created=%t err=%v", created, err)
			}
			run, err := fixture.runner.Run(context.Background(), RunRequest{DatasetID: stored.ID, DatasetVersion: stored.Version, Principal: fixture.principal})
			if err != nil || len(run.Cases) != 1 || run.Cases[0].Coverage == nil {
				t.Fatalf("run with provider coverage: %#v err=%v", run, err)
			}
			coverage := run.Cases[0].Coverage
			if coverage.Status != test.status || reader.calls.Load() == 0 {
				t.Fatalf("provider state %q produced %#v after %d reads", test.state, coverage, reader.calls.Load())
			}
		})
	}
}

func TestCoverageRuntimeResumeReconstructsTerminalSessionWithoutRerunning(t *testing.T) {
	fixture := newEvaluationFixture(t, "eval.lookup")
	reader := &coverageRuntimeReceiptReader{state: effectreceipt.StateConfirmed}
	fixture.runner.EffectReceipts = reader
	dataset := baseDataset("eval.lookup")
	dataset.Cases[0].Assertions = []Assertion{{ID: "status", Kind: AssertRunStatus, ExpectedStatus: core.RunCompleted}}
	dataset.Cases[0].Coverage = coverageEffectContract(EffectReceiptProviderReadBack)
	stored, created, err := fixture.store.PutDataset(context.Background(), dataset)
	if err != nil || !created {
		t.Fatalf("store dataset: created=%t err=%v", created, err)
	}
	evaluationRun := memoryStoreRun(stored, "eval_coverage_resume")
	evaluationRun.Metadata = map[string]string{"evaluation.principal_scope": fixture.principal.Scope.String()}
	if err := fixture.store.CreateRun(context.Background(), evaluationRun); err != nil {
		t.Fatal(err)
	}
	sessionID, agentRunID, _ := evaluationCaseIDs(evaluationRun.ID, stored.Cases[0].ID)
	scope, err := fixture.principal.Scope.Child(core.ScopeRef{Kind: core.ScopeSession, ID: sessionID})
	if err != nil {
		t.Fatal(err)
	}
	session, err := core.NewSession(core.SessionOptions{
		ID: sessionID, ProfileID: evaluationRun.ProfileID, Principal: fixture.principal, Scope: scope,
		Metadata: map[string]string{
			"evaluation.run_id": evaluationRun.ID, "evaluation.dataset_id": stored.ID,
			"evaluation.dataset_version": "1", "evaluation.dataset_revision": stored.Revision,
			"evaluation.case_id": stored.Cases[0].ID,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range []struct {
		kind core.SessionEventType
		data any
	}{
		{kind: core.EvRunStart, data: core.RunStartData{}},
		{kind: core.EvAssistantMessage, data: core.AssistantMessageData{Text: "safe"}},
		{kind: core.EvRunEnd, data: core.RunEndData{Status: core.RunCompleted}},
	} {
		if _, err := session.Append(agentRunID, event.kind, event.data); err != nil {
			t.Fatal(err)
		}
	}
	if err := fixture.runner.Sessions.Create(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	fixture.runner.Runtime.Models = core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) {
		return nil, errors.New("terminal coverage resume must not call model")
	})
	resumed, err := fixture.runner.Resume(context.Background(), evaluationRun.ID, fixture.principal)
	if err != nil || resumed.Status != RunCompleted || len(resumed.Cases) != 1 || resumed.Cases[0].Coverage == nil {
		t.Fatalf("resume result: %#v err=%v", resumed, err)
	}
	coverage := resumed.Cases[0].Coverage
	if coverage.Status != CoverageSatisfied || len(coverage.Effects) != 1 || coverage.Effects[0].Occurrences != 1 || reader.calls.Load() == 0 {
		t.Fatalf("resume did not reconstruct provider coverage: %#v calls=%d", coverage, reader.calls.Load())
	}
	gate, err := fixture.runner.RevalidateCoverageGate(context.Background(), stored, resumed, fixture.principal, CoverageGatePolicy{})
	if err != nil || gate.Status != CoverageSatisfied || gate.RequiredCases != 1 || gate.SatisfiedCases != 1 {
		t.Fatalf("terminal session coverage revalidation: %#v err=%v", gate, err)
	}
}

func TestCoverageRuntimeCostRequiresCompleteLedgerAndDurableUsageCrossCheck(t *testing.T) {
	result := ledgerRun(t, "", ledgerAssembler, nil)
	cost := coverageCostEvidence(result.Evidence, result.Ledger)
	if cost.Status != CoverageSatisfied || !cost.LedgerComplete || cost.ModelCalls != len(result.Ledger.ModelCalls) {
		t.Fatalf("complete ledger cost projection: %#v result=%#v", cost, result)
	}
	broken := *result.Evidence
	broken.ReportedInputTokens--
	if cost := coverageCostEvidence(&broken, result.Ledger); cost.Status != CoverageUnavailable {
		t.Fatalf("cost accepted evidence smaller than durable model accounting: %#v", cost)
	}
}

func TestRevalidateCoverageGateRebuildsJournalEvidenceInsteadOfTrustingSnapshot(t *testing.T) {
	fixture := newEvaluationFixture(t, "eval.lookup")
	journal := newCoverageRuntimeJournal()
	fixture.runner.Runtime.ToolJournal = journal
	dataset := baseDataset("eval.lookup")
	dataset.Cases[0].Coverage = coverageEffectContract(EffectReceiptJournalCompleted)
	stored, created, err := fixture.store.PutDataset(context.Background(), dataset)
	if err != nil || !created {
		t.Fatalf("store dataset: created=%t err=%v", created, err)
	}
	run, err := fixture.runner.Run(context.Background(), RunRequest{DatasetID: stored.ID, DatasetVersion: stored.Version, Principal: fixture.principal})
	if err != nil || run.Cases[0].Coverage == nil || run.Cases[0].Coverage.Status != CoverageSatisfied {
		t.Fatalf("initial coverage run: %#v err=%v", run, err)
	}
	gate, err := fixture.runner.RevalidateCoverageGate(context.Background(), stored, run, fixture.principal, CoverageGatePolicy{})
	if err != nil || gate.Status != CoverageSatisfied || gate.SatisfiedCases != 1 {
		t.Fatalf("initial revalidation gate: %#v err=%v", gate, err)
	}
	journal.clear()
	gate, err = fixture.runner.RevalidateCoverageGate(context.Background(), stored, run, fixture.principal, CoverageGatePolicy{})
	if err != nil || gate.Status != CoverageFailed || gate.FailedCases != 1 {
		t.Fatalf("stale satisfied snapshot authorized release: %#v err=%v", gate, err)
	}
}

func TestRevalidateCoverageGateFailsClosedForMissingReaderAndSnapshotDrift(t *testing.T) {
	fixture := newEvaluationFixture(t, "eval.lookup")
	journal := newCoverageRuntimeJournal()
	fixture.runner.Runtime.ToolJournal = journal
	dataset := baseDataset("eval.lookup")
	dataset.Cases[0].Coverage = coverageEffectContract(EffectReceiptJournalCompleted)
	stored, created, err := fixture.store.PutDataset(context.Background(), dataset)
	if err != nil || !created {
		t.Fatalf("store dataset: created=%t err=%v", created, err)
	}
	run, err := fixture.runner.Run(context.Background(), RunRequest{DatasetID: stored.ID, DatasetVersion: stored.Version, Principal: fixture.principal})
	if err != nil || run.Cases[0].Coverage == nil {
		t.Fatalf("initial run: %#v err=%v", run, err)
	}
	fixture.runner.Runtime.ToolJournal = nil
	gate, err := fixture.runner.RevalidateCoverageGate(context.Background(), stored, run, fixture.principal, CoverageGatePolicy{})
	if err != nil || gate.Status != CoverageUnavailable || gate.UnavailableCases != 1 {
		t.Fatalf("missing journal reader did not become unavailable: %#v err=%v", gate, err)
	}
	fixture.runner.Runtime.ToolJournal = journal
	drifted := run
	drifted.Cases = append([]CaseResult(nil), run.Cases...)
	driftedCoverage := *drifted.Cases[0].Coverage
	driftedCoverage.ContractRevision = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	drifted.Cases[0].Coverage = &driftedCoverage
	gate, err = fixture.runner.RevalidateCoverageGate(context.Background(), stored, drifted, fixture.principal, CoverageGatePolicy{})
	if err != nil || gate.Status != CoverageUnavailable || gate.UnavailableCases != 1 {
		t.Fatalf("coverage snapshot contract drift did not fail closed: %#v err=%v", gate, err)
	}
}

func TestRevalidateCoverageGateStrictPolicyKeepsLegacyDatasetsReadableButUnavailable(t *testing.T) {
	fixture := newEvaluationFixture(t, "eval.lookup")
	stored, created, err := fixture.store.PutDataset(context.Background(), baseDataset("eval.lookup"))
	if err != nil || !created {
		t.Fatalf("store legacy dataset: created=%t err=%v", created, err)
	}
	run, err := fixture.runner.Run(context.Background(), RunRequest{DatasetID: stored.ID, DatasetVersion: stored.Version, Principal: fixture.principal})
	if err != nil {
		t.Fatal(err)
	}
	compatible, err := fixture.runner.RevalidateCoverageGate(context.Background(), stored, run, fixture.principal, CoverageGatePolicy{})
	if err != nil || compatible.Status != CoverageSatisfied || compatible.RequiredCases != 0 {
		t.Fatalf("legacy compatibility gate: %#v err=%v", compatible, err)
	}
	strict, err := fixture.runner.RevalidateCoverageGate(context.Background(), stored, run, fixture.principal, CoverageGatePolicy{RequireContracts: true})
	if err != nil || strict.Status != CoverageUnavailable || strict.RequiredCases != 1 || strict.UnavailableCases != 1 {
		t.Fatalf("strict legacy coverage gate: %#v err=%v", strict, err)
	}
}

func TestRevalidateCoverageGateReconcilesPersistedCostEvidenceWithTerminalSession(t *testing.T) {
	fixture := newEvaluationFixture(t, "eval.lookup")
	fixture.runner.Runtime.Models = core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) {
		return ledgerModel{capability: "eval.lookup"}, nil
	})
	fixture.runner.Runtime.ContextAssembler = ledgerAssembler
	dataset := baseDataset("eval.lookup")
	dataset.Cases[0].Coverage = &CoverageContract{
		Version: CoverageContractV1,
		Cost:    &CostCoverageRequirement{RequireCompleteLedger: true},
	}
	stored, created, err := fixture.store.PutDataset(context.Background(), dataset)
	if err != nil || !created {
		t.Fatalf("store cost dataset: created=%t err=%v", created, err)
	}
	run, err := fixture.runner.Run(context.Background(), RunRequest{DatasetID: stored.ID, DatasetVersion: stored.Version, Principal: fixture.principal})
	if err != nil || run.Cases[0].Coverage == nil || run.Cases[0].Coverage.Status != CoverageSatisfied {
		t.Fatalf("cost run: %#v err=%v", run, err)
	}
	gate, err := fixture.runner.RevalidateCoverageGate(context.Background(), stored, run, fixture.principal, CoverageGatePolicy{})
	if err != nil || gate.Status != CoverageSatisfied {
		t.Fatalf("cost revalidation: %#v err=%v", gate, err)
	}
	mutated := run
	mutated.Cases = append([]CaseResult(nil), run.Cases...)
	evidence := *mutated.Cases[0].Evidence
	evidence.ReportedInputTokens--
	mutated.Cases[0].Evidence = &evidence
	gate, err = fixture.runner.RevalidateCoverageGate(context.Background(), stored, mutated, fixture.principal, CoverageGatePolicy{})
	if err != nil || gate.Status != CoverageUnavailable || gate.UnavailableCases != 1 {
		t.Fatalf("mutated persisted cost evidence passed revalidation: %#v err=%v", gate, err)
	}
}
