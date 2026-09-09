package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	programtools "github.com/whhhh1500/auto-agent/pkg/adapter/programmatic/toolcapability"
	core "github.com/whhhh1500/auto-agent/pkg/core"
)

func TestWriteLiveEvidenceFailedRecord(t *testing.T) {
	directory := t.TempDir()
	record := liveEvidenceRecord{
		Schema: "harness.programmatic.live-evidence/v1", CaseID: "failed-case",
		RequestedModel: "test-model", Protocol: "responses", SourceRevision: "test-revision",
		PromptSHA256: "abc123", RuntimeStatus: "failed", AcceptancePassed: false, TotalElapsedMS: 12,
		PacingWaitMS: 2, ProviderRequestMS: 10, ToolResults: liveEvidenceTools{Succeeded: 1, Failed: 1},
		Rounds: []liveEvidenceRound{{Round: 1, ToolNames: []string{"program.execute"}, ErrorClass: "http_status", ErrorType: "modelexecution_openai_http_status", ErrorCode: "http_status", SystemBytes: 7, MessageBytes: 11, ToolSchemaBytes: 13}},
	}
	if err := writeLiveEvidence(directory, record); err != nil {
		t.Fatal("failed evidence record was not written")
	}
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) != 1 {
		t.Fatal("failed evidence record did not create exactly one run-unique file")
	}
	payload, err := os.ReadFile(filepath.Join(directory, entries[0].Name()))
	if err != nil {
		t.Fatal("failed evidence record could not be read")
	}
	var decoded liveEvidenceRecord
	if json.Unmarshal(payload, &decoded) != nil || decoded.RuntimeStatus != "failed" || decoded.AcceptancePassed || decoded.ToolResults.Failed != 1 || len(decoded.Rounds) != 1 || decoded.Rounds[0].MessageBytes != 11 {
		t.Fatal("failed evidence record did not preserve structural outcome")
	}
	text := string(payload)
	if strings.Contains(text, "prompt\"") || strings.Contains(text, "assistant") || strings.Contains(text, "api_key") || strings.Contains(text, "\"memory\"") || strings.Contains(text, "\"summary\"") {
		t.Fatal("evidence record included a prohibited raw field")
	}
}

func TestWriteLiveEvidenceFileDoesNotOverwrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "already-written.json")
	if err := writeLiveEvidenceFile(path, []byte(`{"first":true}`)); err != nil {
		t.Fatal("initial evidence write failed")
	}
	if err := writeLiveEvidenceFile(path, []byte(`{"second":true}`)); err == nil {
		t.Fatal("evidence writer overwrote an existing record")
	}
	payload, err := os.ReadFile(path)
	if err != nil || string(payload) != `{"first":true}` {
		t.Fatal("existing evidence record changed after rejected overwrite")
	}
}

func TestLiveEvidenceCapturesCompletedAcceptanceFailure(t *testing.T) {
	model := &directModel{}
	fixture, err := newFixture(model, false, 1)
	if err != nil {
		t.Fatal("could not construct deterministic runtime fixture")
	}
	result, err := fixture.runtime.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{RunID: "offline-completed-acceptance-failure", Text: "find active inventory names"}, nil)
	if err != nil || result.Status != core.RunCompleted {
		t.Fatal("deterministic runtime did not complete")
	}
	// This is an actual completed runtime with a deliberately unmet business
	// expectation, not a hand-constructed evidence status.
	acceptancePassed := sameStrings(model.finalValue(), []string{"Wrong Item"})
	record := finalizedLiveEvidence(liveCase{name: "completed_acceptance_failure"}, result, "deterministic", "test", "test-revision", "non-secret prompt", fixture.session.Events(), nil, 0, acceptancePassed)
	record.FixtureEffects = liveFixtureEffectEvidenceFromFixture(fixture)
	if record.RuntimeStatus != string(core.RunCompleted) || record.AcceptancePassed || !record.FixtureEffects.Observed || record.FixtureEffects.InventoryCalls != 1 || record.FixtureEffects.DetailCalls != 1 {
		t.Fatal("completed runtime acceptance failure was not represented distinctly")
	}
	directory := t.TempDir()
	if err := writeLiveEvidence(directory, record); err != nil {
		t.Fatal("completed acceptance failure evidence was not written")
	}
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) != 1 {
		t.Fatal("completed acceptance failure did not produce one evidence file")
	}
	payload, err := os.ReadFile(filepath.Join(directory, entries[0].Name()))
	if err != nil {
		t.Fatal("completed acceptance failure evidence could not be read")
	}
	var decoded liveEvidenceRecord
	if json.Unmarshal(payload, &decoded) != nil || decoded.RuntimeStatus != string(core.RunCompleted) || decoded.AcceptancePassed || !decoded.FixtureEffects.Observed || decoded.FixtureEffects.InventoryCalls != 1 || decoded.FixtureEffects.DetailCalls != 1 {
		t.Fatal("persisted evidence collapsed acceptance failure into runtime success")
	}
}

func TestLiveEvidenceIncludesWhitelistedProgramDiagnostic(t *testing.T) {
	callData, _ := json.Marshal(core.ToolCallData{CallID: "program-root", Name: programtools.ExecuteID, Args: map[string]any{"source": `{"version":"not-ptc","body":[{"op":"return","value":{"op":"literal","value":null}}]}`, "bindings": map[string]any{}}})
	resultData, _ := json.Marshal(core.ToolResultData{CallID: "program-root", OK: false, Metadata: map[string]any{"code": "program_invalid", "diagnostic": "compile_version_invalid"}})
	events := []core.SessionEvent{{Type: core.EvToolCall, Data: callData}, {Type: core.EvToolResult, Data: resultData}}
	record := liveEvidenceFromCase(liveCase{name: "program_diagnostic"}, core.TurnResult{RunID: "diagnostic-run", Status: core.RunCompleted}, "test-model", "test", "test-revision", "non-secret prompt", events, nil, 0)
	if len(record.ProgramAttempts) != 1 || record.ProgramAttempts[0].Diagnostic != "compile_version_invalid" || record.ProgramAttempts[0].CompileDiagnostic != "compile_version_invalid" || record.ProgramAttempts[0].ResultOK {
		t.Fatal("program diagnostic was not preserved as whitelisted structured evidence")
	}
}

func TestLiveProgramAttemptBindingEvidenceSuccess(t *testing.T) {
	const source = `{"version":"ptc-ir/v1","body":[{"op":"call","tool":"fixture.lookup","args":{"op":"map","entries":{}}}]}`
	events := liveProgramAttemptEvents(t, source, map[string]any{"fixture.lookup": "binding-one"}, true, map[string]any{"code": "program_completed"})
	attempts := liveProgramAttempts(events)
	if len(attempts) != 1 {
		t.Fatalf("attempt count=%d want 1", len(attempts))
	}
	attempt := attempts[0]
	if !attempt.CompileOK || attempt.CompileDiagnostic != "absent" || !attempt.BindingsExact || !attempt.CompileToolsBindingsExact || !attempt.CatalogDigestMatching {
		t.Fatalf("valid compiled bindings were not recorded exactly: %#v", attempt)
	}
	if attempt.CompiledToolCount != 1 || attempt.SubmittedBindingCount != 1 || attempt.MissingCompiledToolKeys != 0 || attempt.ExtraneousSubmittedBindingKeys != 0 || attempt.CatalogDigestMatchCount != 1 || attempt.CatalogDigestMismatchCount != 0 {
		t.Fatalf("valid binding counts=%#v", attempt)
	}
	if attempt.WireAliasEvidenceAvailable || attempt.SubmittedBindingKeysUseWireAlias || attempt.SubmittedWireAliasKeyCount != 0 {
		t.Fatal("Core events must not be reported as wire-alias evidence")
	}
}

func TestLiveProgramAttemptBindingEvidenceFailure(t *testing.T) {
	const source = `{"version":"ptc-ir/v1","body":[{"op":"call","tool":"fixture.lookup","args":{"op":"map","entries":{}}}]}`
	events := liveProgramAttemptEvents(t, source, map[string]any{"fixture.extra": "binding-extra"}, false, map[string]any{"code": "program_bindings_mismatch"})
	attempts := liveProgramAttempts(events)
	if len(attempts) != 1 {
		t.Fatalf("attempt count=%d want 1", len(attempts))
	}
	attempt := attempts[0]
	if !attempt.CompileOK || attempt.BindingsExact || attempt.CompileToolsBindingsExact || attempt.CatalogDigestMatching || attempt.ResultOK || attempt.MetadataCode != "program_bindings_mismatch" {
		t.Fatalf("binding failure was not kept as a failure: %#v", attempt)
	}
	if attempt.CompiledToolCount != 1 || attempt.SubmittedBindingCount != 1 || attempt.MissingCompiledToolKeys != 1 || attempt.ExtraneousSubmittedBindingKeys != 1 || attempt.CatalogDigestMatchCount != 0 || attempt.CatalogDigestMismatchCount != 1 {
		t.Fatalf("binding mismatch counts=%#v", attempt)
	}
}

func TestLiveProgramAttemptDoesNotPersistUnknownDiagnostic(t *testing.T) {
	const source = `{"version":"ptc-ir/v1","body":[{"op":"call","tool":"fixture.lookup","args":{"op":"map","entries":{}}}]}`
	events := liveProgramAttemptEvents(t, source, map[string]any{"fixture.lookup": "binding-one"}, false, map[string]any{"code": "program_invalid", "diagnostic": "not-a-fixed-diagnostic"})
	attempts := liveProgramAttempts(events)
	if len(attempts) != 1 || attempts[0].Diagnostic != "unrecognized" || attempts[0].CompileDiagnostic != "absent" {
		t.Fatal("unrecognized program diagnostic was not reduced to a fixed evidence category")
	}
}

func TestLiveProgramAttemptRecordsOnlyCompositeLiteralCounts(t *testing.T) {
	const secret = "do-not-persist-composite-value"
	const source = `{"version":"ptc-ir/v1","body":[{"op":"return","value":{"op":"literal","value":["` + secret + `"]}},{"op":"return","value":{"op":"literal","value":{"` + secret + `":true}}}]}`
	events := liveProgramAttemptEvents(t, source, map[string]any{}, false, map[string]any{"code": "program_invalid", "diagnostic": "compile_expression_invalid"})
	record := liveEvidenceFromCase(liveCase{name: "composite_literal"}, core.TurnResult{RunID: "composite-run", Status: core.RunFailed}, "test-model", "test", "test-revision", "non-secret prompt", events, nil, 0)
	if len(record.ProgramAttempts) != 1 {
		t.Fatal("composite literal attempt was not recorded")
	}
	attempt := record.ProgramAttempts[0]
	if attempt.CompileDiagnostic != "compile_expression_invalid" || !attempt.CompositeLiteralScanApplicable || !attempt.CompositeLiteralScanObserved || attempt.CompositeLiteralArrayCount != 1 || attempt.CompositeLiteralMapCount != 1 {
		t.Fatalf("composite literal evidence=%#v", attempt)
	}
	payload, err := json.Marshal(record)
	if err != nil {
		t.Fatal("could not encode composite literal evidence")
	}
	if strings.Contains(string(payload), secret) {
		t.Fatal("composite literal evidence retained source text")
	}
}

func TestLiveCompositeLiteralScanMarksUnparseableSourceUnobserved(t *testing.T) {
	evidence := liveCompositeLiteralScan(`{"version":`, "compile_expression_invalid")
	if !evidence.applicable || evidence.observed || evidence.arrays != 0 || evidence.maps != 0 {
		t.Fatalf("unparseable composite scan=%#v", evidence)
	}
}

func TestLiveEvidenceCountsObservedBudgetDenials(t *testing.T) {
	encoded, err := json.Marshal(core.ToolResultData{CallID: "bounded-child", OK: false, Metadata: map[string]any{"code": core.CodeBudgetExceeded}})
	if err != nil {
		t.Fatal("could not encode budget denial")
	}
	record := liveEvidenceFromCase(liveCase{name: "budget_denial"}, core.TurnResult{RunID: "budget-run", Status: core.RunLimited}, "test-model", "test", "test-revision", "non-secret prompt", []core.SessionEvent{{Type: core.EvToolResult, Data: encoded}}, nil, 0)
	if !record.ToolResults.Observed || record.ToolResults.Failed != 1 || record.ToolResults.BudgetDeniedResults != 1 {
		t.Fatalf("budget denial evidence=%#v", record.ToolResults)
	}
	unknown := liveEvidenceFromCase(liveCase{name: "no-events"}, core.TurnResult{RunID: "no-events", Status: core.RunFailed}, "test-model", "test", "test-revision", "non-secret prompt", nil, nil, 0)
	if unknown.ToolResults.Observed || unknown.ToolResults.BudgetDeniedResults != 0 {
		t.Fatalf("unobserved result evidence=%#v", unknown.ToolResults)
	}
}

func TestLiveInvocationEvidenceReconcilesEachAdapterCallAndRetainsFailedUsage(t *testing.T) {
	rounds := []liveRound{
		{round: 1, runID: "invocation-run", step: 0, adapterStarted: true, usage: core.TokenUsage{InputTokens: 11, OutputTokens: 3}, hasUsage: true, usageConsistent: true},
		// A provider failure after a valid report still has to match the durable
		// model usage event; failure must not erase the reported subtotal.
		{round: 2, runID: "invocation-run", step: 1, adapterStarted: true, usage: core.TokenUsage{InputTokens: 7, OutputTokens: 2}, hasUsage: true, usageConsistent: true, errorClass: "transport"},
	}
	events := []core.SessionEvent{
		core.SessionEvent{Seq: 10, RunID: "invocation-run", Type: core.EvStepStart, Data: liveEvidenceJSON(t, core.StepData{Index: 0})},
		core.SessionEvent{Seq: 11, RunID: "invocation-run", Type: core.EvRunUsage, Data: liveEvidenceJSON(t, core.RunUsageData{InvocationID: "model:10", InputTokens: 11, OutputTokens: 3})},
		core.SessionEvent{Seq: 20, RunID: "invocation-run", Type: core.EvStepStart, Data: liveEvidenceJSON(t, core.StepData{Index: 1})},
		core.SessionEvent{Seq: 21, RunID: "invocation-run", Type: core.EvRunUsage, Data: liveEvidenceJSON(t, core.RunUsageData{InvocationID: "model:20", InputTokens: 7, OutputTokens: 2})},
	}
	evidence := liveInvocationEvidenceFromRounds(rounds, events)
	if evidence == nil || !evidence.AdapterCallsObserved || evidence.ActualAdapterCalls != 2 || evidence.PacingCanceledBeforeAdapter != 0 || !evidence.ReportedUsageComplete || !evidence.UsageProtocolConsistent || !evidence.LedgerUsageMatched || !evidence.UniqueLedgerInvocationIDs {
		t.Fatalf("per-invocation evidence=%#v", evidence)
	}

	model := &liveModel{observations: rounds}
	record := liveEvidenceFromCase(liveCase{name: "invocation-success"}, core.TurnResult{RunID: "invocation-run", Status: core.RunFailed}, "test-model", "test", "test-revision", "non-secret prompt", events, model, 0)
	if record.InvocationEvidence == nil || !record.InvocationEvidence.LedgerUsageMatched {
		t.Fatalf("record omitted invocation evidence: %#v", record.InvocationEvidence)
	}
	payload, err := json.Marshal(record)
	if err != nil || strings.Contains(string(payload), "model:10") || strings.Contains(string(payload), "model:20") {
		t.Fatalf("invocation identities leaked into JSON: %s", payload)
	}
}

func TestLiveInvocationEvidenceRejectsMissingUsageAndObservedProtocolConflicts(t *testing.T) {
	for _, test := range []struct {
		name  string
		round liveRound
	}{
		{name: "missing_usage_is_unknown", round: liveRound{round: 1, runID: "missing-usage", step: 0, adapterStarted: true, usageConsistent: true}},
		{name: "duplicate_or_invalid_usage_is_inconsistent", round: liveRound{round: 1, runID: "conflicting-usage", step: 0, adapterStarted: true, usage: core.TokenUsage{InputTokens: 4, OutputTokens: 1}, hasUsage: true, usageConsistent: false}},
	} {
		t.Run(test.name, func(t *testing.T) {
			events := []core.SessionEvent{
				core.SessionEvent{Seq: 5, RunID: test.round.runID, Type: core.EvStepStart, Data: liveEvidenceJSON(t, core.StepData{Index: 0})},
				core.SessionEvent{Seq: 6, RunID: test.round.runID, Type: core.EvRunUsage, Data: liveEvidenceJSON(t, core.RunUsageData{InvocationID: "model:5", InputTokens: test.round.usage.InputTokens, OutputTokens: test.round.usage.OutputTokens})},
			}
			evidence := liveInvocationEvidenceFromRounds([]liveRound{test.round}, events)
			if evidence == nil || evidence.ReportedUsageComplete || evidence.LedgerUsageMatched {
				t.Fatalf("incomplete or conflicting usage was accepted: %#v", evidence)
			}
		})
	}
}

func TestLiveInvocationEvidenceRejectsStepUsageSwapWithMatchingTotals(t *testing.T) {
	rounds := []liveRound{
		{round: 1, runID: "swapped-usage", step: 0, adapterStarted: true, usage: core.TokenUsage{InputTokens: 9, OutputTokens: 1}, hasUsage: true, usageConsistent: true},
		{round: 2, runID: "swapped-usage", step: 1, adapterStarted: true, usage: core.TokenUsage{InputTokens: 4, OutputTokens: 2}, hasUsage: true, usageConsistent: true},
	}
	events := []core.SessionEvent{
		{Seq: 10, RunID: "swapped-usage", Type: core.EvStepStart, Data: liveEvidenceJSON(t, core.StepData{Index: 0})},
		// The aggregate is still 13/3, but each invocation has the other
		// invocation's usage. A total-only check would incorrectly pass.
		{Seq: 11, RunID: "swapped-usage", Type: core.EvRunUsage, Data: liveEvidenceJSON(t, core.RunUsageData{InvocationID: "model:10", InputTokens: 4, OutputTokens: 2})},
		{Seq: 20, RunID: "swapped-usage", Type: core.EvStepStart, Data: liveEvidenceJSON(t, core.StepData{Index: 1})},
		{Seq: 21, RunID: "swapped-usage", Type: core.EvRunUsage, Data: liveEvidenceJSON(t, core.RunUsageData{InvocationID: "model:20", InputTokens: 9, OutputTokens: 1})},
	}
	evidence := liveInvocationEvidenceFromRounds(rounds, events)
	if evidence == nil || !evidence.ReportedUsageComplete || !evidence.UniqueLedgerInvocationIDs || evidence.LedgerUsageMatched {
		t.Fatalf("same-total usage swap was accepted: %#v", evidence)
	}
}

func TestLiveInvocationEvidenceMarksSummaryUsageOutsideOrdinarySubtotal(t *testing.T) {
	rounds := []liveRound{{round: 1, runID: "summary-usage", step: 0, adapterStarted: true, usage: core.TokenUsage{InputTokens: 5, OutputTokens: 1}, hasUsage: true, usageConsistent: true}}
	events := []core.SessionEvent{
		{Seq: 10, RunID: "summary-usage", Type: core.EvStepStart, Data: liveEvidenceJSON(t, core.StepData{Index: 0})},
		{Seq: 11, RunID: "summary-usage", Type: core.EvRunUsage, Data: liveEvidenceJSON(t, core.RunUsageData{InvocationID: "model:10", InputTokens: 5, OutputTokens: 1})},
		{Seq: 12, RunID: "summary-usage", Type: core.EvRunUsage, Data: liveEvidenceJSON(t, core.RunUsageData{InvocationID: "summary:1:2:3", InputTokens: 3, OutputTokens: 1})},
	}
	evidence := liveInvocationEvidenceFromRounds(rounds, events)
	if evidence == nil || !evidence.ReportedUsageComplete || !evidence.UniqueLedgerInvocationIDs || evidence.LedgerUsageMatched {
		t.Fatalf("summary usage was silently accepted into ordinary subtotal: %#v", evidence)
	}
}

func TestLiveModelPacingCancellationDoesNotCountAsAdapterCall(t *testing.T) {
	inner := &liveEvidenceCountingAdapter{}
	pacer := &liveRequestPacer{interval: time.Hour, lastStart: time.Now()}
	model := newLiveModel(t, inner, "pacing-cancel", 1, pacer)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := model.Stream(ctx, core.GenerateOptions{ModelCall: core.ModelCallRequest{RunID: "pacing-cancel-run", Step: 0}}, func(core.StreamChunk) {}); err == nil {
		t.Fatal("pacing cancellation was not surfaced")
	}
	if inner.calls != 0 {
		t.Fatalf("adapter calls=%d, want 0 before pacing cancellation", inner.calls)
	}
	evidence := liveInvocationEvidenceFromRounds(model.evidenceRounds(), nil)
	if evidence == nil || evidence.ActualAdapterCalls != 0 || evidence.PacingCanceledBeforeAdapter != 1 || evidence.ReportedUsageComplete || evidence.LedgerUsageMatched || evidence.UniqueLedgerInvocationIDs {
		t.Fatalf("pacing cancellation evidence=%#v", evidence)
	}
}

func TestLiveModelRetainsUsageReportedBeforeAdapterFailure(t *testing.T) {
	inner := &liveEvidenceUsageThenFailureAdapter{}
	model := newLiveModel(t, inner, "usage-before-failure", 1, nil)
	fixture, err := newFixture(model, false, 1)
	if err != nil {
		t.Fatal("could not construct failed-usage runtime fixture")
	}
	result, err := fixture.runtime.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{RunID: "usage-before-failure-run", Text: "find active inventory names"}, nil)
	if err == nil || result.Status != core.RunFailed {
		t.Fatalf("adapter failure did not fail the runtime: result=%#v err=%v", result, err)
	}
	rounds := model.evidenceRounds()
	if len(rounds) != 1 || !rounds[0].adapterStarted || !rounds[0].hasUsage || !rounds[0].usageConsistent || rounds[0].usage != (core.TokenUsage{InputTokens: 6, OutputTokens: 2}) {
		t.Fatalf("failed adapter usage was not retained: %#v", rounds)
	}
	evidence := liveInvocationEvidenceFromRounds(rounds, fixture.session.Events())
	if evidence == nil || !evidence.LedgerUsageMatched {
		t.Fatalf("failed adapter usage did not reconcile against the runtime ledger: %#v", evidence)
	}
}

type liveEvidenceCountingAdapter struct{ calls int }

func (*liveEvidenceCountingAdapter) Provider() string { return "live-evidence-counting" }

func (m *liveEvidenceCountingAdapter) Stream(context.Context, core.GenerateOptions, func(core.StreamChunk)) error {
	m.calls++
	return nil
}

type liveEvidenceUsageThenFailureAdapter struct{}

func (*liveEvidenceUsageThenFailureAdapter) Provider() string { return "live-evidence-usage-failure" }

func (*liveEvidenceUsageThenFailureAdapter) Stream(_ context.Context, _ core.GenerateOptions, emit func(core.StreamChunk)) error {
	usage := core.TokenUsage{InputTokens: 6, OutputTokens: 2}
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop, Usage: &usage})
	return errors.New("synthetic adapter failure after usage")
}

func liveEvidenceJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatal("could not encode live invocation evidence event")
	}
	return payload
}

func liveProgramAttemptEvents(t *testing.T, source string, bindings map[string]any, resultOK bool, metadata map[string]any) []core.SessionEvent {
	t.Helper()
	marshal := func(value any) json.RawMessage {
		payload, err := json.Marshal(value)
		if err != nil {
			t.Fatal("could not encode offline program evidence")
		}
		return payload
	}
	return []core.SessionEvent{
		{Type: core.EvToolCall, Data: marshal(core.ToolCallData{CallID: "catalog-root", Name: programtools.CatalogID, Args: map[string]any{}})},
		{Type: core.EvToolResult, Data: marshal(core.ToolResultData{CallID: "catalog-root", OK: true, Content: `{"tools":[{"schema":{"name":"fixture.lookup"},"binding_digest":"binding-one"}]}`})},
		{Type: core.EvToolCall, Data: marshal(core.ToolCallData{CallID: "program-root", Name: programtools.ExecuteID, Args: map[string]any{"source": source, "bindings": bindings}})},
		{Type: core.EvToolResult, Data: marshal(core.ToolResultData{CallID: "program-root", OK: resultOK, Metadata: metadata})},
	}
}

func TestTypedFixtureCatalogExposesValidOutputContracts(t *testing.T) {
	model := &typedCatalogProbeModel{}
	fixture, err := newFixtureWithOutputContracts(model, true, 1, true)
	if err != nil {
		t.Fatal("could not construct typed output-contract fixture")
	}
	result, err := fixture.runtime.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{RunID: "typed-output-contract-catalog", Text: "inspect catalog"}, nil)
	if err != nil || result.Status != core.RunCompleted {
		t.Fatal("typed fixture catalog run did not complete")
	}
	for _, test := range []struct {
		id   string
		args map[string]any
		tool *readOnlyFixtureTool
	}{
		{listToolID, map[string]any{}, fixture.list},
		{detailToolID, map[string]any{"id": "item-01"}, fixture.detail},
	} {
		schema := typedCatalogOutputSchema(model.catalog, test.id)
		if len(schema) == 0 {
			t.Fatalf("catalog did not expose output schema for %q", test.id)
		}
		content, err := test.tool.execute(test.args)
		if err != nil {
			t.Fatal("fixture output could not be generated")
		}
		var value any
		if json.Unmarshal([]byte(content), &value) != nil || core.ValidateJSONValue(schema, value) != nil {
			t.Fatalf("fixture output does not satisfy catalog schema for %q", test.id)
		}
	}
}

type typedCatalogProbeModel struct {
	phase   int
	catalog map[string]any
}

func (*typedCatalogProbeModel) Provider() string { return "typed-catalog-probe" }

func (m *typedCatalogProbeModel) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	switch m.phase {
	case 0:
		m.phase++
		emitTool(emit, core.ToolCall{ID: "typed-catalog", Name: programtools.CatalogID, Args: map[string]any{}})
		return nil
	case 1:
		m.phase++
		catalog, err := lastToolJSON(options.Messages, "typed-catalog")
		if err != nil {
			return err
		}
		m.catalog = catalog
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: "catalog inspected"})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
		return nil
	default:
		return errTypedCatalogProbeExtraRound
	}
}

var errTypedCatalogProbeExtraRound = errors.New("typed catalog probe received an extra round")

func typedCatalogOutputSchema(catalog map[string]any, id string) map[string]any {
	tools, _ := catalog["tools"].([]any)
	for _, raw := range tools {
		descriptor, _ := raw.(map[string]any)
		schema, _ := descriptor["schema"].(map[string]any)
		if schema["name"] != id {
			continue
		}
		output, _ := descriptor["output_schema"].(map[string]any)
		return output
	}
	return nil
}
