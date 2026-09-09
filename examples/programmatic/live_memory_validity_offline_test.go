package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	core "github.com/whhhh1500/auto-agent/pkg/core"
	extmemory "github.com/whhhh1500/auto-agent/pkg/extensions/memory"
)

// TestMemoryValidityFixtureOffline exercises the production Runtime, standard
// memory.recall capability, journal, and assembler with a test-private
// validity ledger. It has no provider request or production TTL contract.
func TestMemoryValidityFixtureOffline(t *testing.T) {
	values, err := newMemoryValidityValues()
	if err != nil {
		t.Fatal("could not create opaque memory validity values")
	}
	prompt := memoryValidityPrompt(values.Lookup)
	if memoryValidityPromptLeaks(prompt, values) {
		t.Fatal("memory validity fixture value leaked into prompt")
	}
	for _, test := range []struct {
		name        string
		wantAnswer  string
		wantEntries int
		toolOK      bool
	}{
		{name: "fresh", wantEntries: 1, toolOK: true},
		{name: "expired", wantAnswer: "UNKNOWN", wantEntries: 0, toolOK: true},
		{name: "backend_error", wantAnswer: "UNKNOWN", toolOK: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.wantAnswer == "" {
				test.wantAnswer = values.Target
			}
			probe := &memoryValidityProbe{query: values.Lookup}
			model := newLiveModel(t, probe, "offline-memory-validity-"+test.name, 2, nil)
			observed := &memoryObservedModel{inner: model}
			fixture, err := newMemoryValidityFixture(observed, "offline-memory-validity", values, test.name, "offline-"+test.name)
			if err != nil {
				t.Fatal("could not construct offline memory validity fixture")
			}
			result, err := fixture.runtime.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{RunID: "offline-memory-validity-" + test.name, Text: prompt}, nil)
			if err != nil || result.Status != core.RunCompleted {
				t.Fatalf("offline memory validity case did not complete (status=%s)", result.Status)
			}
			if err := assertMemoryValidityArm(fixture, observed, model, fixture.session.Events(), result, values, test.wantAnswer, test.wantEntries, test.toolOK); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestMemoryValidityMissingUsageStaysUnknown proves the first, durable usage
// is retained even when the final adapter invocation reports no usage. The
// evidence therefore remains incomplete rather than inferring a zero value.
func TestMemoryValidityMissingUsageStaysUnknown(t *testing.T) {
	values, err := newMemoryValidityValues()
	if err != nil {
		t.Fatal("could not create opaque memory validity values")
	}
	probe := &memoryValidityProbe{query: values.Lookup, omitSecondUsage: true}
	model := newLiveModel(t, probe, "offline-memory-validity-missing-usage", 2, nil)
	observed := &memoryObservedModel{inner: model}
	fixture, err := newMemoryValidityFixture(observed, "offline-memory-validity", values, "backend_error", "offline-missing-usage")
	if err != nil {
		t.Fatal("could not construct offline memory validity fixture")
	}
	result, err := fixture.runtime.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{RunID: "offline-memory-validity-missing-usage", Text: memoryValidityPrompt(values.Lookup)}, nil)
	if err != nil || result.Status != core.RunCompleted || strings.TrimSpace(result.Answer) != "UNKNOWN" {
		t.Fatal("missing-usage fixture did not preserve the backend-error answer")
	}
	evidence := liveInvocationEvidenceFromRounds(model.evidenceRounds(), fixture.session.Events())
	if evidence == nil || !evidence.AdapterCallsObserved || evidence.ActualAdapterCalls != 2 || evidence.PacingCanceledBeforeAdapter != 0 || !evidence.UsageProtocolConsistent || evidence.ReportedUsageComplete || evidence.LedgerUsageMatched {
		t.Fatal("missing final usage was not represented as unknown invocation evidence")
	}
	usage, reports := memoryValidityUsage(fixture.session.Events())
	if reports != 2 || usage != (core.TokenUsage{InputTokens: 5, OutputTokens: 1}) {
		t.Fatal("reported usage before the completed failure was not retained durably")
	}
}

// TestMemoryValidityRejectsWrongAnswer proves that a completed Runtime result
// is insufficient when it does not use the exact, filtered recall evidence.
func TestMemoryValidityRejectsWrongAnswer(t *testing.T) {
	values, err := newMemoryValidityValues()
	if err != nil {
		t.Fatal("could not create opaque memory validity values")
	}
	probe := &memoryValidityProbe{query: values.Lookup, wrongAnswer: true}
	model := newLiveModel(t, probe, "offline-memory-validity-wrong-answer", 2, nil)
	observed := &memoryObservedModel{inner: model}
	fixture, err := newMemoryValidityFixture(observed, "offline-memory-validity", values, "fresh", "offline-wrong-answer")
	if err != nil {
		t.Fatal("could not construct offline memory validity fixture")
	}
	result, err := fixture.runtime.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{RunID: "offline-memory-validity-wrong-answer", Text: memoryValidityPrompt(values.Lookup)}, nil)
	if err != nil || result.Status != core.RunCompleted {
		t.Fatal("wrong-answer fixture did not complete")
	}
	if err := assertMemoryValidityArm(fixture, observed, model, fixture.session.Events(), result, values, values.Target, 1, true); err == nil {
		t.Fatal("memory validity acceptance accepted a wrong completed answer")
	}
}

type memoryValidityProbe struct {
	mu              sync.Mutex
	phase           int
	query           string
	omitSecondUsage bool
	wrongAnswer     bool
}

func (*memoryValidityProbe) Provider() string { return "offline-memory-validity" }

func (m *memoryValidityProbe) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	m.mu.Lock()
	phase := m.phase
	m.phase++
	omitSecondUsage := m.omitSecondUsage
	wrongAnswer := m.wrongAnswer
	m.mu.Unlock()
	switch phase {
	case 0:
		call := core.ToolCall{ID: "memory-validity-recall", Name: extmemory.RecallCapabilityID, Args: map[string]any{"query": m.query, "limit": 1}}
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &call, Usage: &core.TokenUsage{InputTokens: 5, OutputTokens: 1}})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
		return nil
	case 1:
		answer, err := memoryValidityProbeAnswer(options.Messages, "memory-validity-recall")
		if err != nil {
			return err
		}
		if wrongAnswer {
			answer = "WRONG"
		}
		chunk := core.StreamChunk{Kind: core.StreamKindAssistant, Text: answer}
		if !omitSecondUsage {
			chunk.Usage = &core.TokenUsage{InputTokens: 7, OutputTokens: 2}
		}
		emit(chunk)
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
		return nil
	default:
		return errors.New("offline memory validity probe received an unexpected extra round")
	}
}

func memoryValidityProbeAnswer(messages []core.ChatMessage, callID string) (string, error) {
	content, err := lastToolContent(messages, callID)
	if err != nil {
		return "", err
	}
	if content == "memory store operation failed" {
		return "UNKNOWN", nil
	}
	payload, err := lastToolJSON(messages, callID)
	if err != nil {
		return "", err
	}
	entries, ok := payload["entries"].([]any)
	if !ok {
		return "", errors.New("offline memory validity probe received an invalid result")
	}
	if len(entries) == 0 {
		return "UNKNOWN", nil
	}
	if len(entries) != 1 {
		return "", errors.New("offline memory validity probe received multiple entries")
	}
	entry, ok := entries[0].(map[string]any)
	if !ok {
		return "", errors.New("offline memory validity probe entry is invalid")
	}
	content, ok = entry["content"].(string)
	if !ok {
		return "", errors.New("offline memory validity probe content is invalid")
	}
	_, answer, found := strings.Cut(content, "code=")
	if !found || strings.TrimSpace(answer) == "" {
		return "", errors.New("offline memory validity probe found no code")
	}
	return strings.TrimSpace(answer), nil
}

func assertMemoryValidityArm(fixture *memoryValidityFixture, observed *memoryObservedModel, model *liveModel, events []core.SessionEvent, result core.TurnResult, values memoryValidityValues, expected string, wantEntries int, toolOK bool) error {
	if got := strings.TrimSpace(result.Answer); got != expected {
		return memoryValidityErrorf("final answer did not match the filtered recall arm")
	}
	if model.rounds() != 2 || fixture.assemblerCalls.Load() < 2 || fixture.journal.completed() != 1 {
		return memoryValidityErrorf("runtime did not retain two rounds, assembled continuation, and completed journal")
	}
	evidence := liveInvocationEvidenceFromRounds(model.evidenceRounds(), events)
	if evidence == nil || !evidence.AdapterCallsObserved || evidence.ActualAdapterCalls != 2 || evidence.PacingCanceledBeforeAdapter != 0 || !evidence.UsageProtocolConsistent || !evidence.ReportedUsageComplete || !evidence.LedgerUsageMatched || !evidence.UniqueLedgerInvocationIDs {
		return memoryValidityErrorf("per-invocation adapter usage did not match the durable usage ledger")
	}
	calls, results, _, durableAnswer := liveSessionEvidence(events)
	if strings.TrimSpace(durableAnswer) != expected || len(calls) != 1 || len(results) != 1 {
		return memoryValidityErrorf("durable tool or final-answer evidence is incomplete")
	}
	call, toolResult := calls[0], results[0]
	if call.Name != extmemory.RecallCapabilityID || call.CallID == "" || toolResult.CallID != call.CallID || toolResult.OK != toolOK {
		return memoryValidityErrorf("durable recall call/result pair did not match the arm")
	}
	if toolOK {
		contents, ok := memoryValidityEntryContents(toolResult.Content)
		if !ok || len(contents) != wantEntries {
			return memoryValidityErrorf("successful recall entries did not match the arm")
		}
		if wantEntries == 1 && !strings.Contains(contents[0], values.Target) {
			return memoryValidityErrorf("filtered recall did not return the valid same-scope entry")
		}
		if memoryValidityContainsAny(toolResult.Content, values.Expired, values.MissingValidity, values.Foreign, values.Unrelated) {
			return memoryValidityErrorf("successful recall leaked unavailable, foreign, or unrelated memory")
		}
	} else if toolResult.Content != "memory store operation failed" || toolResult.Metadata["code"] != "memory_recall_failed" {
		return memoryValidityErrorf("backend failure was conflated with a successful empty recall")
	}
	if !observed.firstTurnObserved() || memoryValidityObservedInitialLeak(observed, values) || !observed.finalContextHasRecallResult(call.CallID, toolResult.Content) {
		return memoryValidityErrorf("recall result was not isolated from initial context or replayed exactly")
	}
	if wantEntries == 0 || !toolOK {
		if memoryObservedContains(observed, values.Target) || memoryValidityContainsAny(result.Answer+toolResult.Content, values.Target) {
			return memoryValidityErrorf("unavailable target reached context or durable output")
		}
	}
	if memoryObservedContains(observed, values.Expired) || memoryObservedContains(observed, values.MissingValidity) || memoryObservedContains(observed, values.Foreign) || memoryObservedContains(observed, values.Unrelated) || memoryValidityContainsAny(result.Answer+toolResult.Content, values.Expired, values.MissingValidity, values.Foreign, values.Unrelated) {
		return memoryValidityErrorf("unavailable or foreign value reached durable output")
	}
	storeCalls := fixture.store.calls()
	if len(storeCalls) != 1 || storeCalls[0].scope != fixture.principal.Scope.String() || storeCalls[0].query != values.Lookup || storeCalls[0].limit != 1 {
		return memoryValidityErrorf("standard recall did not use the expected scoped, bounded Store call")
	}
	if current, peer := memoryValidityRawCurrentEntries(fixture, values), memoryValidityRawPeerIntact(fixture, values); !current || !peer {
		return memoryValidityErrorf("validity filtering changed raw storage or did not prove unavailable newest entries (current=%t peer=%t)", current, peer)
	}
	return nil
}

func memoryValidityEntryContents(content string) ([]string, bool) {
	var payload struct {
		Entries []struct {
			Content string `json:"content"`
		} `json:"entries"`
	}
	if json.Unmarshal([]byte(content), &payload) != nil {
		return nil, false
	}
	contents := make([]string, len(payload.Entries))
	for index, entry := range payload.Entries {
		contents[index] = entry.Content
	}
	return contents, true
}

func memoryValidityObservedInitialLeak(observed *memoryObservedModel, values memoryValidityValues) bool {
	return observed.initialContextContains(values.Target) || observed.initialContextContains(values.Expired) || observed.initialContextContains(values.MissingValidity) || observed.initialContextContains(values.Foreign) || observed.initialContextContains(values.Unrelated)
}

func memoryValidityContainsAny(value string, candidates ...string) bool {
	for _, candidate := range candidates {
		if candidate != "" && strings.Contains(value, candidate) {
			return true
		}
	}
	return false
}

func memoryValidityUsage(events []core.SessionEvent) (core.TokenUsage, int) {
	var usage core.TokenUsage
	reports := 0
	for _, event := range events {
		if event.Type != core.EvRunUsage {
			continue
		}
		var data core.RunUsageData
		if json.Unmarshal(event.Data, &data) != nil {
			continue
		}
		reports++
		usage.InputTokens += data.InputTokens
		usage.OutputTokens += data.OutputTokens
	}
	return usage, reports
}
