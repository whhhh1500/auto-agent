package evaluationledger

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/whhhh1500/auto-agent/pkg/core"
)

func ledgerEvent(t *testing.T, seq int64, kind core.SessionEventType, data any) core.SessionEvent {
	t.Helper()
	raw, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	return core.SessionEvent{Seq: seq, RunID: "run_ledger", Type: kind, Data: raw}
}

func recordCompleteCall(c *Collector, step int, input, output int64) {
	c.modelStarted(step)
	c.modelUsage(step, core.TokenUsage{InputTokens: input, OutputTokens: output})
	c.modelFinished(step, true)
	c.context(core.ModelContext{ContextWindowTokens: 32, MaxOutputTokens: 8}, core.ModelContext{
		ContextWindowTokens: 32, MaxOutputTokens: 8, InputBytes: 5, InputTokens: 5,
	}, true)
}

type acceptingGate struct{}

func (acceptingGate) AuthorizeModelCall(context.Context, core.ModelCallRequest) error { return nil }

func TestProjectMatchesUsageByStepStartSequenceNotEventOrder(t *testing.T) {
	collector := New()
	recordCompleteCall(collector, 0, 11, 7)
	recordCompleteCall(collector, 1, 12, 8)
	events := []core.SessionEvent{
		// Projection input can be sliced or reordered by a store reader. The
		// sequence still binds this usage to the later step/start evidence.
		ledgerEvent(t, 48, core.EvRunUsage, core.RunUsageData{InvocationID: "model:47", InputTokens: 12, OutputTokens: 8}),
		ledgerEvent(t, 31, core.EvStepStart, core.StepData{Index: 0}),
		ledgerEvent(t, 47, core.EvStepStart, core.StepData{Index: 1}),
		ledgerEvent(t, 49, core.EvRunUsage, core.RunUsageData{InvocationID: "model:31", InputTokens: 11, OutputTokens: 7}),
	}
	ledger, err := collector.Project(events, "run_ledger")
	if err != nil || !ledger.Complete {
		t.Fatalf("exact step reconciliation failed: ledger=%#v err=%v", ledger, err)
	}
}

func TestProjectRejectsDuplicateDurableInvocationAndUnavailableEvidence(t *testing.T) {
	t.Run("duplicate durable invocation", func(t *testing.T) {
		collector := New()
		recordCompleteCall(collector, 0, 11, 7)
		events := []core.SessionEvent{
			ledgerEvent(t, 31, core.EvStepStart, core.StepData{Index: 0}),
			ledgerEvent(t, 32, core.EvRunUsage, core.RunUsageData{InvocationID: "model:31", InputTokens: 11, OutputTokens: 7}),
			ledgerEvent(t, 33, core.EvRunUsage, core.RunUsageData{InvocationID: "model:31", InputTokens: 11, OutputTokens: 7}),
		}
		ledger, err := collector.Project(events, "run_ledger")
		if err != nil || ledger.Complete || !strings.Contains(strings.Join(ledger.IncompleteReasons, ","), "durable_usage_duplicate") {
			t.Fatalf("duplicate durable usage was accepted: ledger=%#v err=%v", ledger, err)
		}
	})
	t.Run("no adapter call", func(t *testing.T) {
		collector := New()
		collector.Runtime(&core.Runtime{})
		ledger, err := collector.Project(nil, "run_ledger")
		if err != nil || ledger.Complete || !strings.Contains(strings.Join(ledger.IncompleteReasons, ","), "adapter_calls_unavailable") || !strings.Contains(strings.Join(ledger.IncompleteReasons, ","), "context_accounting_unavailable") {
			t.Fatalf("empty collector was accepted: ledger=%#v err=%v", ledger, err)
		}
	})
	t.Run("configured gate requires one result per model step", func(t *testing.T) {
		collector := New()
		collector.Runtime(&core.Runtime{ModelCallGate: acceptingGate{}})
		recordCompleteCall(collector, 0, 11, 7)
		events := []core.SessionEvent{
			ledgerEvent(t, 31, core.EvStepStart, core.StepData{Index: 0}),
			ledgerEvent(t, 32, core.EvRunUsage, core.RunUsageData{InvocationID: "model:31", InputTokens: 11, OutputTokens: 7}),
		}
		ledger, err := collector.Project(events, "run_ledger")
		if err != nil || ledger.Complete || !strings.Contains(strings.Join(ledger.IncompleteReasons, ","), "model_gate_accounting_unavailable") {
			t.Fatalf("missing configured-gate evidence was accepted: ledger=%#v err=%v", ledger, err)
		}
	})
}
