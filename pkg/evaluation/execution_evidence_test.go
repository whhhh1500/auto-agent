package evaluation

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

func TestExecutionEvidenceProjectsOnlyTheCaseRun(t *testing.T) {
	principal := core.Principal{
		TenantID: "tenant", SubjectID: "subject",
		Scope: core.MustScopePath(
			core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"},
			core.ScopeRef{Kind: core.ScopeProduct, ID: "product"},
			core.ScopeRef{Kind: core.ScopeUser, ID: "user"},
		),
	}
	scope := core.MustScopePath(append(principal.Scope.Segments(), core.ScopeRef{Kind: core.ScopeSession, ID: "evalsess-evidence"})...)
	session, err := core.NewSession(core.SessionOptions{ID: "evalsess-evidence", ProfileID: "evaluation.default", Principal: principal, Scope: scope})
	if err != nil {
		t.Fatal(err)
	}
	appendEvent := func(runID string, kind core.SessionEventType, value any) {
		t.Helper()
		if _, err := session.Append(runID, kind, value); err != nil {
			t.Fatal(err)
		}
	}
	caseRunID := "evalcase-evidence"
	appendEvent(caseRunID, core.EvRunStart, core.RunStartData{})
	appendEvent(caseRunID, core.EvUserMessage, core.UserMessageData{Text: "case"})
	appendEvent(caseRunID, core.EvStepStart, core.StepData{Index: 0})
	rootCall := core.ToolCall{ID: "root-call", Name: "tool.root", Args: map[string]any{}}
	appendEvent(caseRunID, core.EvAssistantMessage, core.AssistantMessageData{ToolCall: &rootCall, ToolCalls: []core.ToolCall{rootCall}})
	appendEvent(caseRunID, core.EvToolResult, core.ToolResultData{CallID: "root-call", OK: true})
	appendEvent(caseRunID, core.EvToolResult, core.ToolResultData{CallID: "root-call/child", OK: false})
	appendEvent(caseRunID, core.EvRunUsage, core.RunUsageData{InputTokens: 13, OutputTokens: 5, InvocationID: "model:4"})
	appendEvent(caseRunID, core.EvRunUsage, core.RunUsageData{InputTokens: 2, OutputTokens: 1, InvocationID: "summary:5:7:8"})
	appendEvent(caseRunID, core.EvStepEnd, core.StepData{Index: 0})
	appendEvent(caseRunID, core.EvRunEnd, core.RunEndData{Status: core.RunCompleted})

	appendEvent("other-run", core.EvRunStart, core.RunStartData{})
	appendEvent("other-run", core.EvRunUsage, core.RunUsageData{InputTokens: 100, OutputTokens: 100, InvocationID: "model:0"})
	appendEvent("other-run", core.EvAssistantMessage, core.AssistantMessageData{ToolCall: &core.ToolCall{ID: "other-call", Name: "tool.other", Args: map[string]any{}}})
	appendEvent("other-run", core.EvToolResult, core.ToolResultData{CallID: "other-call", OK: false})
	appendEvent("other-run", core.EvRunEnd, core.RunEndData{Status: core.RunCompleted})

	evidence, err := executionEvidence(session.Events(), caseRunID)
	if err != nil {
		t.Fatal(err)
	}
	want := &ExecutionEvidence{
		ReportedInputTokens: 15, ReportedOutputTokens: 6, UsageReports: 2,
		StepsStarted: 1, StepsEnded: 1, TopLevelToolCalls: 1,
		ToolResultEventsOK: 1, ToolResultEventsNotOK: 1,
	}
	if *evidence != *want {
		t.Fatalf("evidence=%#v want=%#v", evidence, want)
	}
	if evidence.NoUsageReported {
		t.Fatal("reported usage was marked absent")
	}
}

func TestExecutionEvidenceCountsRepeatedToolResultEvents(t *testing.T) {
	data, err := json.Marshal(core.ToolResultData{CallID: "replayed-call", OK: true})
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := executionEvidence([]core.SessionEvent{
		{RunID: "evalcase-replayed-results", Type: core.EvToolResult, Data: data},
		{RunID: "evalcase-replayed-results", Type: core.EvToolResult, Data: data},
	}, "evalcase-replayed-results")
	if err != nil {
		t.Fatalf("repeated durable result events must remain measurable: %v", err)
	}
	if evidence.ToolResultEventsOK != 2 || evidence.ToolResultEventsNotOK != 0 {
		t.Fatalf("evidence=%#v", evidence)
	}
}

func TestExecutionEvidenceValidationRejectsInconsistentOrUnboundedValues(t *testing.T) {
	valid := ExecutionEvidence{NoUsageReported: true}
	if err := validateExecutionEvidence(valid); err != nil {
		t.Fatalf("valid evidence rejected: %v", err)
	}
	for _, evidence := range []ExecutionEvidence{
		{UsageReports: 1, NoUsageReported: true},
		{ReportedInputTokens: 1, NoUsageReported: true},
		{UsageReports: 0, NoUsageReported: false},
		{UsageReports: -1},
		{StepsStarted: core.MaxSessionEvents + 1, NoUsageReported: true},
		{UsageReports: 1, ReportedInputTokens: core.MaxReportedTokensPerRun + 1},
	} {
		if err := validateExecutionEvidence(evidence); err == nil {
			t.Fatalf("invalid evidence accepted: %#v", evidence)
		}
	}
}

func TestExecutionEvidenceDoesNotRequireAUsageReport(t *testing.T) {
	evidence, err := executionEvidence(nil, "evalcase-no-usage")
	if err != nil {
		t.Fatal(err)
	}
	if evidence.UsageReports != 0 || !evidence.NoUsageReported {
		t.Fatalf("evidence=%#v", evidence)
	}
	result := CaseResult{
		CaseID: "case-no-usage", SessionID: "evalsess-no-usage", AgentRunID: "evalcase-no-usage",
		Status: core.RunCompleted, Score: 1, Passed: true, DurationMS: 1,
		CompletedAt: time.Unix(1, 0).UTC(), Evidence: evidence,
	}
	if err := ValidateCaseResult(result); err != nil {
		t.Fatal(err)
	}
}

func TestExecutionEvidenceMemoryStoreClonesOptionalEvidence(t *testing.T) {
	ctx := context.Background()
	fixture := newEvaluationFixture(t, "eval.lookup")
	dataset, created, err := fixture.store.PutDataset(ctx, baseDataset("eval.lookup"))
	if err != nil || !created {
		t.Fatalf("put dataset: created=%t err=%v", created, err)
	}
	run := memoryStoreRun(dataset, "evalrun-evidence-clone")
	if err := fixture.store.CreateRun(ctx, run); err != nil {
		t.Fatal(err)
	}
	result := memoryStoreCaseResult(dataset.Cases[0].ID)
	result.Evidence = &ExecutionEvidence{ReportedInputTokens: 13, ReportedOutputTokens: 5, UsageReports: 1, ToolResultEventsOK: 1}
	if err := fixture.store.RecordCaseResult(ctx, run.ID, result); err != nil {
		t.Fatal(err)
	}
	result.Evidence.ReportedInputTokens = 99

	stored, err := fixture.store.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := stored.Cases[0].Evidence.ReportedInputTokens; got != 13 {
		t.Fatalf("stored evidence aliased input result: %d", got)
	}
	stored.Cases[0].Evidence.ToolResultEventsOK = 99
	reloaded, err := fixture.store.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := reloaded.Cases[0].Evidence.ToolResultEventsOK; got != 1 {
		t.Fatalf("returned evidence aliased store state: %d", got)
	}
}

func TestExecutionEvidenceJSONRoundTripRemainsValid(t *testing.T) {
	original := CaseResult{
		CaseID: "case-evidence-json", SessionID: "evalsess-evidence-json", AgentRunID: "evalcase-evidence-json",
		Status: core.RunCompleted, Score: 1, Passed: true, DurationMS: 1, CompletedAt: time.Unix(2, 0).UTC(),
		Evidence: &ExecutionEvidence{ReportedInputTokens: 13, ReportedOutputTokens: 5, UsageReports: 2, StepsStarted: 1, StepsEnded: 1, TopLevelToolCalls: 1, ToolResultEventsOK: 1},
	}
	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var restored CaseResult
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	if err := ValidateCaseResult(restored); err != nil {
		t.Fatalf("round-tripped evidence is invalid: %v", err)
	}
	if restored.Evidence == nil || *restored.Evidence != *original.Evidence {
		t.Fatalf("restored evidence=%#v want=%#v", restored.Evidence, original.Evidence)
	}

	legacy := original
	legacy.Evidence = nil
	raw, err = json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	var legacyRestored CaseResult
	if err := json.Unmarshal(raw, &legacyRestored); err != nil {
		t.Fatal(err)
	}
	if legacyRestored.Evidence != nil {
		t.Fatalf("legacy result unexpectedly acquired evidence: %#v", legacyRestored.Evidence)
	}
	if err := ValidateCaseResult(legacyRestored); err != nil {
		t.Fatalf("legacy result without evidence is invalid: %v", err)
	}
}
