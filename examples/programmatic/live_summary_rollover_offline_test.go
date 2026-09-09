package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

// TestSummaryRolloverFixtureOffline verifies the exact Runtime route used by
// the live test after repeated high-volume extraction. The fixed corpus and
// correction placement intentionally exercise a fitting prior-summary boundary.
func TestSummaryRolloverFixtureOffline(t *testing.T) {
	values, err := newSummaryRolloverValues()
	if err != nil {
		t.Fatal("could not create opaque rollover fixture values")
	}
	probe := &summaryRolloverOfflineProbe{values: values}
	observed := newSummaryRolloverObservedAdapter(probe, 3)
	fixture, err := newSummaryRolloverFixture(observed, "offline-summary-rollover", values)
	if err != nil {
		t.Fatal("could not construct rollover fixture")
	}
	runIDs := []string{"offline-summary-rollover-correction", "offline-summary-rollover-neutral", "offline-summary-rollover-final"}
	inputs := []string{summaryRolloverCorrectionPrompt(values), summaryRolloverNeutralPrompt, summaryRolloverFinalPrompt}
	var results [3]core.TurnResult
	for index := range runIDs {
		result, runErr := fixture.runtime.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{RunID: runIDs[index], Text: inputs[index]}, nil)
		results[index] = result
		if runErr != nil || result.Status != core.RunCompleted {
			t.Fatalf("offline rollover run %d did not complete", index+1)
		}
	}
	events := fixture.session.Events()
	evidence := summaryRolloverEvidenceFromRun("offline_summary_rollover", fixture, observed, runIDs, results, events, values)
	t.Logf("offline rollover summary_byte_counts=%v omitted=%t new_in_final_context=%t", evidence.SummaryByteCounts, evidence.SummaryHasOmissionMarker, evidence.NewInFinalContext)
	evidence.AcceptancePassed = true
	assertSummaryRolloverAcceptance(t, fixture, observed, runIDs, results, events, values)
	if evidence.RequestedModel != "offline-summary-rollover" || evidence.BinaryRevision == "" || evidence.FinalPromptSHA256 == "" || !evidence.UsageComplete || !evidence.UsageLedgerMatches || !evidence.UniqueInvocationIDs || !evidence.AcknowledgementsExact || !evidence.EffectiveProjectionExact || !evidence.SummaryCoverageAdvanced || evidence.SummaryEvents != 3 || len(evidence.SummaryByteCounts) != 3 || evidence.SummaryByteCounts[0] < 11900 || evidence.SummaryByteCounts[0] > 12064 || !evidence.NewInFinalContext || !evidence.FinalAnswerCorrect || !evidence.DurableFinalAnswerCorrect || !evidence.AcceptancePassed {
		t.Fatalf("offline rollover evidence was incomplete: %+v", evidence)
	}
	directory := t.TempDir()
	if err := writeSummaryRolloverEvidence(directory, evidence); err != nil {
		t.Fatal("could not write offline rollover evidence")
	}
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) != 1 {
		t.Fatal("offline rollover evidence did not create one file")
	}
	payload, err := os.ReadFile(filepath.Join(directory, entries[0].Name()))
	if err != nil || strings.Contains(string(payload), values.Old) || strings.Contains(string(payload), values.New) {
		t.Fatal("offline rollover evidence exposed opaque fixture values")
	}
}

func TestSummaryRolloverUsageFactsRejectIncompleteOrFaultedUsage(t *testing.T) {
	validCall := summaryRolloverObservedCall{usage: core.TokenUsage{InputTokens: 17, OutputTokens: 3}, hasUsage: true}
	usageEvent := func(t *testing.T, runID, invocation string, input, output int64) core.SessionEvent {
		t.Helper()
		data, err := json.Marshal(core.RunUsageData{InvocationID: invocation, InputTokens: input, OutputTokens: output})
		if err != nil {
			t.Fatal("could not encode usage fixture")
		}
		return core.SessionEvent{RunID: runID, Type: core.EvRunUsage, Data: data}
	}
	for _, test := range []struct {
		name   string
		runs   []string
		events []core.SessionEvent
		calls  []summaryRolloverObservedCall
		unique bool
	}{
		{name: "missing", runs: []string{"run-a"}, calls: []summaryRolloverObservedCall{validCall}},
		{name: "malformed", runs: []string{"run-a"}, events: []core.SessionEvent{{RunID: "run-a", Type: core.EvRunUsage, Data: json.RawMessage("{")}}, calls: []summaryRolloverObservedCall{validCall}},
		{name: "summary_invocation", runs: []string{"run-a"}, events: []core.SessionEvent{usageEvent(t, "run-a", "summary:1:2:3", 17, 3)}, calls: []summaryRolloverObservedCall{validCall}},
		{name: "mismatched_call_usage", runs: []string{"run-a"}, events: []core.SessionEvent{usageEvent(t, "run-a", "model:7", 17, 4)}, calls: []summaryRolloverObservedCall{validCall}, unique: true},
		{name: "duplicate_across_runs", runs: []string{"run-a", "run-b"}, events: []core.SessionEvent{usageEvent(t, "run-a", "model:7", 17, 3), usageEvent(t, "run-b", "model:7", 17, 3)}, calls: []summaryRolloverObservedCall{validCall, validCall}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, complete, matches, unique := summaryRolloverUsageFacts(test.events, test.runs, test.calls)
			if complete || matches || unique != test.unique {
				t.Fatalf("faulted usage was accepted: complete=%t matches=%t unique=%t", complete, matches, unique)
			}
		})
	}
}

type summaryRolloverOfflineProbe struct {
	values summaryRolloverValues
	mu     sync.Mutex
	phase  int
}

func (*summaryRolloverOfflineProbe) Provider() string { return "offline-summary-rollover" }
func (m *summaryRolloverOfflineProbe) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	m.mu.Lock()
	phase := m.phase
	m.phase++
	m.mu.Unlock()
	text := "ACK"
	if phase == 2 {
		code, found := summaryRolloverCurrentCode(options.Messages)
		if !found || code != m.values.New {
			text = "CURRENT: UNKNOWN"
		} else {
			text = "CURRENT: " + code
		}
	} else if phase > 2 {
		return errors.New("offline rollover probe received an unexpected model call")
	}
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: text, Usage: &core.TokenUsage{InputTokens: 17, OutputTokens: 3}})
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
	return nil
}
func summaryRolloverCurrentCode(messages []core.ChatMessage) (string, bool) {
	const prefix = "Project Orion current launch code: "
	for _, message := range messages {
		start := strings.Index(message.Content, prefix)
		if start < 0 {
			continue
		}
		value := message.Content[start+len(prefix):]
		if end := strings.IndexByte(value, '.'); end >= 0 {
			value = value[:end]
		}
		value = strings.TrimSpace(value)
		if value != "" {
			return value, true
		}
	}
	return "", false
}
