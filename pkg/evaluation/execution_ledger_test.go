package evaluation

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

type ledgerModel struct {
	capability string
	mode       string
}

func (ledgerModel) Provider() string         { return "ledger-test" }
func (ledgerModel) ArtifactRevision() string { return "ledger-test/v1" }
func (m ledgerModel) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	if m.mode == "adapter-error" {
		return errors.New("adapter secret: never serialize this")
	}
	usage := &core.TokenUsage{InputTokens: int64(11 + options.ModelCall.Step), OutputTokens: int64(7 + options.ModelCall.Step)}
	if m.mode == "missing-usage" && options.ModelCall.Step == 0 {
		usage = nil
	}
	if m.mode == "duplicate-usage" && options.ModelCall.Step == 0 {
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, Usage: usage})
	}
	for index := len(options.Messages) - 1; index >= 0; index-- {
		if options.Messages[index].Role == core.RoleTool {
			emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: `{"value":"ledger-result"}`, Usage: usage})
			emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
			return nil
		}
	}
	call := core.ToolCall{ID: "ledger-call", Name: m.capability, Args: map[string]any{"secret": "tool-arguments-stay-out"}}
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &call, Usage: usage})
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
	return nil
}

type ledgerGate struct{ deny bool }

func (g ledgerGate) AuthorizeModelCall(context.Context, core.ModelCallRequest) error {
	if g.deny {
		return errors.New("gate secret: never serialize this")
	}
	return nil
}

func ledgerAssembler(ctx context.Context, request core.ModelContext) (core.ModelContext, error) {
	if err := ctx.Err(); err != nil {
		return core.ModelContext{}, err
	}
	return core.ModelContext{
		System: request.System, Messages: append([]core.ChatMessage(nil), request.Messages...),
		ContextWindowTokens: request.ContextWindowTokens, MaxOutputTokens: request.MaxOutputTokens,
		InputBytes: 23, InputTokens: 19, DroppedGroups: 2,
	}, nil
}

func ledgerRun(t *testing.T, mode string, assembler core.ModelContextAssembler, gate core.ModelCallGate) CaseResult {
	t.Helper()
	fixture := newEvaluationFixture(t, "eval.lookup")
	fixture.runner.Runtime.Models = core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) {
		return ledgerModel{capability: "eval.lookup", mode: mode}, nil
	})
	fixture.runner.Runtime.ContextAssembler = assembler
	fixture.runner.Runtime.ModelCallGate = gate
	dataset, created, err := fixture.store.PutDataset(context.Background(), baseDataset("eval.lookup"))
	if err != nil || !created {
		t.Fatalf("store dataset: created=%t err=%v", created, err)
	}
	run, err := fixture.runner.Run(context.Background(), RunRequest{DatasetID: dataset.ID, DatasetVersion: dataset.Version, Principal: fixture.principal})
	if err != nil || len(run.Cases) != 1 {
		t.Fatalf("run evaluation: run=%#v err=%v", run, err)
	}
	return run.Cases[0]
}

func TestExecutionLedgerReconcilesTwoCallsWithoutContent(t *testing.T) {
	result := ledgerRun(t, "", ledgerAssembler, ledgerGate{})
	ledger := result.Ledger
	if ledger == nil || !ledger.Complete || len(ledger.ModelCalls) != 2 || len(ledger.ContextAssemblies) != 2 || len(ledger.GateCalls) != 2 {
		t.Fatalf("unexpected ledger: %#v", ledger)
	}
	for index, call := range ledger.ModelCalls {
		if call.Step != index || !call.Invoked || call.Outcome != "ok" || call.UsageReports != 1 || call.Usage == nil || call.Usage.InputTokens != int64(11+index) || call.Usage.OutputTokens != int64(7+index) {
			t.Fatalf("model call %d is not reconciled: %#v", index, call)
		}
	}
	for _, assembly := range ledger.ContextAssemblies {
		if assembly.Outcome != "ok" || assembly.ContextWindowTokens <= assembly.MaxOutputTokens || assembly.InputBytes != 23 || assembly.InputTokens != 19 || assembly.DroppedGroups != 2 {
			t.Fatalf("assembly details missing: %#v", assembly)
		}
	}
	for _, gate := range ledger.GateCalls {
		if gate.Outcome != "accepted" {
			t.Fatalf("gate result missing: %#v", ledger.GateCalls)
		}
	}
	encoded, err := json.Marshal(ledger)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"evaluate", "historical question", "tool-arguments-stay-out", "ledger-result", "adapter secret", "sha256"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("ledger serialized content %q: %s", forbidden, encoded)
		}
	}
}

func TestExecutionLedgerMarksMissingAndDuplicateUsageIncomplete(t *testing.T) {
	for _, mode := range []string{"missing-usage", "duplicate-usage"} {
		t.Run(mode, func(t *testing.T) {
			ledger := ledgerRun(t, mode, ledgerAssembler, nil).Ledger
			if ledger == nil || ledger.Complete || len(ledger.IncompleteReasons) == 0 {
				t.Fatalf("incomplete usage was accepted: %#v", ledger)
			}
		})
	}
}

func TestExecutionLedgerRequiresContextAccounting(t *testing.T) {
	ledger := ledgerRun(t, "", nil, nil).Ledger
	if ledger == nil || ledger.Complete || len(ledger.ContextAssemblies) != 0 || !strings.Contains(strings.Join(ledger.IncompleteReasons, ","), "context_accounting_unavailable") {
		t.Fatalf("nil assembler produced complete or fabricated context evidence: %#v", ledger)
	}
}

func TestExecutionLedgerAllowsNilGateWhenOtherEvidenceIsComplete(t *testing.T) {
	ledger := ledgerRun(t, "", ledgerAssembler, nil).Ledger
	if ledger == nil || !ledger.Complete || len(ledger.GateCalls) != 0 {
		t.Fatalf("nil gate did not retain a complete ledger: %#v", ledger)
	}
}

func TestExecutionLedgerMarksAdapterAndAssemblerErrorsIncomplete(t *testing.T) {
	t.Run("adapter", func(t *testing.T) {
		ledger := ledgerRun(t, "adapter-error", ledgerAssembler, ledgerGate{}).Ledger
		if ledger == nil || ledger.Complete || ledger.ModelCalls[0].Outcome != "error" || !strings.Contains(strings.Join(ledger.IncompleteReasons, ","), "adapter_error") {
			t.Fatalf("adapter error ledger=%#v", ledger)
		}
	})
	t.Run("assembler", func(t *testing.T) {
		assembler := func(context.Context, core.ModelContext) (core.ModelContext, error) {
			return core.ModelContext{}, errors.New("assembler secret: never serialize this")
		}
		ledger := ledgerRun(t, "", assembler, nil).Ledger
		if ledger == nil || ledger.Complete || len(ledger.ContextAssemblies) != 1 || ledger.ContextAssemblies[0].Outcome != "error" || !strings.Contains(strings.Join(ledger.IncompleteReasons, ","), "context_assembly_error") {
			t.Fatalf("assembler error ledger=%#v", ledger)
		}
	})
	t.Run("gate denied", func(t *testing.T) {
		ledger := ledgerRun(t, "", ledgerAssembler, ledgerGate{deny: true}).Ledger
		if ledger == nil || ledger.Complete || len(ledger.ModelCalls) != 0 || len(ledger.GateCalls) != 1 || ledger.GateCalls[0].Outcome != "denied" || !strings.Contains(strings.Join(ledger.IncompleteReasons, ","), "model_gate_denied") {
			t.Fatalf("gate denial ledger=%#v", ledger)
		}
	})
}

func TestExecutionLedgerLegacyCaseJSONAndStoreRoundTripRemainAvailable(t *testing.T) {
	legacy := memoryStoreCaseResult("case-1")
	encoded, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	var decoded CaseResult
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Ledger != nil || strings.Contains(string(encoded), "execution_ledger") {
		t.Fatalf("legacy case JSON changed: %s", encoded)
	}
	store := NewMemoryStore()
	dataset := memoryStoreDataset(t, store, 1)
	run := memoryStoreRun(dataset, "eval_ledger_legacy")
	if err := store.CreateRun(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCaseResult(context.Background(), run.ID, decoded); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.GetRun(context.Background(), run.ID)
	if err != nil || len(loaded.Cases) != 1 || loaded.Cases[0].Ledger != nil {
		t.Fatalf("legacy store round-trip: %#v err=%v", loaded, err)
	}
}

func TestExecutionLedgerValidationRejectsUnboundedOrInconsistentProof(t *testing.T) {
	result := memoryStoreCaseResult("case-1")
	validCall := ExecutionLedgerModelCall{Step: 0, Invoked: true, Outcome: "ok", UsageReports: 1, Usage: &ExecutionLedgerUsage{InputTokens: 1, OutputTokens: 1}}
	result.Ledger = &ExecutionLedger{Complete: true, ModelCalls: []ExecutionLedgerModelCall{validCall}}
	if err := ValidateCaseResult(result); err == nil {
		t.Fatal("complete ledger without matching context was accepted")
	}
	result.Ledger = &ExecutionLedger{Complete: true, ModelCalls: []ExecutionLedgerModelCall{validCall}, ContextAssemblies: []ExecutionLedgerContext{{ContextWindowTokens: 32, MaxOutputTokens: 8, Outcome: "ok"}}}
	result.Ledger.ModelCalls[0].Usage, result.Ledger.ModelCalls[0].UsageReports = nil, 0
	if err := ValidateCaseResult(result); err == nil {
		t.Fatal("complete ledger with missing usage was accepted")
	}
	result.Ledger = &ExecutionLedger{Complete: true, ModelCalls: []ExecutionLedgerModelCall{validCall}, ContextAssemblies: []ExecutionLedgerContext{{ContextWindowTokens: 32, MaxOutputTokens: 8, Outcome: "ok"}}, GateCalls: []ExecutionLedgerGateCall{{Step: 1, Outcome: "accepted"}}}
	if err := ValidateCaseResult(result); err == nil {
		t.Fatal("complete ledger with non-covering gate step was accepted")
	}
	result.Ledger = &ExecutionLedger{Complete: false, IncompleteReasons: []string{"model_gate_accounting_unavailable"}, GateCalls: []ExecutionLedgerGateCall{{Step: 0, Outcome: "accepted"}, {Step: 0, Outcome: "accepted"}}}
	if err := ValidateCaseResult(result); err == nil {
		t.Fatal("duplicate gate steps were accepted")
	}
	result.Ledger = &ExecutionLedger{Complete: false}
	if err := ValidateCaseResult(result); err == nil {
		t.Fatal("incomplete ledger without a fixed reason was accepted")
	}
	result.Ledger = &ExecutionLedger{Complete: false, IncompleteReasons: []string{"untrusted free-form reason"}}
	if err := ValidateCaseResult(result); err == nil {
		t.Fatal("free-form incomplete reason was accepted")
	}
	result.Ledger = &ExecutionLedger{Complete: false, IncompleteReasons: []string{"adapter_calls_unavailable"}, ContextAssemblies: make([]ExecutionLedgerContext, core.HardMaxSteps+1)}
	if err := ValidateCaseResult(result); err == nil {
		t.Fatal("unbounded context ledger was accepted")
	}
}
