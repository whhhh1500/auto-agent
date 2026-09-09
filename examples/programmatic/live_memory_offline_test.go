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
	extmemory "github.com/whhhh1500/auto-agent/pkg/extensions/memory"
)

// TestMemoryAcceptanceFixtureOffline exercises the same production
// memory.recall capability and context assembler as the opt-in live test. The
// deterministic adapter merely makes the two fixture arms reproducible; it
// does not simulate a provider or a memory implementation.
func TestMemoryAcceptanceFixtureOffline(t *testing.T) {
	const nonce = "mem-offline-fixture-value"
	if strings.Contains(memoryAcceptancePrompt, nonce) {
		t.Fatal("offline memory fixture value leaked into prompt")
	}
	for _, arm := range []struct {
		name     string
		relevant bool
		expected string
		entries  int
	}{
		{name: "relevant", relevant: true, expected: "MEMORY: " + nonce, entries: 1},
		{name: "irrelevant", relevant: false, expected: "MEMORY: UNKNOWN", entries: 0},
	} {
		t.Run(arm.name, func(t *testing.T) {
			probe := &memoryProbeModel{}
			observed := &memoryObservedModel{inner: probe}
			fixture, err := newMemoryAcceptanceFixture(observed, "offline-memory-probe", arm.relevant, nonce)
			if err != nil {
				t.Fatal("could not construct memory acceptance fixture")
			}
			result, err := fixture.runtime.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{RunID: "offline-memory-" + arm.name, Text: memoryAcceptancePrompt}, nil)
			if err != nil || result.Status != core.RunCompleted {
				t.Fatalf("offline memory fixture did not complete (status=%s)", result.Status)
			}
			if got := strings.TrimSpace(result.Answer); got != arm.expected {
				t.Fatal("offline memory answer did not follow actual recall result")
			}
			if fixture.assemblerCalls.Load() < 2 || !probe.sawRecallResult() {
				t.Fatal("memory recall result was not injected through the production context assembler")
			}
			calls, results, _, _ := liveSessionEvidence(fixture.session.Events())
			recallCallID, recallContent, ok := memoryRecallResult(calls, results)
			if !ok {
				t.Fatal("offline fixture did not call memory.recall exactly once")
			}
			if arm.relevant && observed.initialContextContains(nonce) {
				t.Fatal("offline fixture leaked its memory value into initial model context")
			}
			if !observed.finalContextHasRecallResult(recallCallID, recallContent) {
				t.Fatal("offline fixture final context lost the exact durable recall result")
			}
			count, ok := recalledEntryCount([]core.ToolResultData{{CallID: recallCallID, Content: recallContent, OK: true}})
			if !ok || count != arm.entries {
				t.Fatal("offline fixture recall result did not match its configured arm")
			}
		})
	}
}

func TestMemoryAcceptanceEvidenceGuardsRejectMissingUsageAndMismatchedRecall(t *testing.T) {
	if memoryUsageMatchesLedger(core.TokenUsage{InputTokens: 3, OutputTokens: 1}, core.TokenUsage{InputTokens: 3, OutputTokens: 1}, false) {
		t.Fatal("missing stream usage must not pass memory acceptance")
	}
	if memoryUsageMatchesLedger(core.TokenUsage{InputTokens: 3, OutputTokens: 1}, core.TokenUsage{InputTokens: 4, OutputTokens: 1}, true) {
		t.Fatal("unequal stream and session usage must not pass memory acceptance")
	}
	if memoryFinalContextHasExactRecallResult([]core.ChatMessage{{Role: core.RoleTool, ToolCallID: "other", Content: `{"entries":[]}`}}, "memory-recall", `{"entries":[]}`) {
		t.Fatal("wrong recall call id must not be accepted as injected evidence")
	}
	if memoryFinalContextHasExactRecallResult([]core.ChatMessage{{Role: core.RoleTool, ToolCallID: "memory-recall", Content: `{"entries":[{}]}`}}, "memory-recall", `{"entries":[]}`) {
		t.Fatal("different recall content must not be accepted as injected evidence")
	}
	_, _, ok := memoryRecallResult(
		[]core.ToolCallData{{CallID: "memory-recall", Name: extmemory.RecallCapabilityID}},
		[]core.ToolResultData{{CallID: "other", Content: `{"entries":[]}`, OK: true}},
	)
	if ok {
		t.Fatal("tool result with a different durable call id must not be accepted")
	}
}

// TestMemoryEvidencePersistsCompletedAcceptanceFailure ensures Cleanup can
// retain the diagnostic predicates after a completed runtime fails its final
// answer acceptance. The evidence itself contains no fixture value or tool
// content.
func TestMemoryEvidencePersistsCompletedAcceptanceFailure(t *testing.T) {
	const nonce = "mem-offline-evidence-value"
	probe := &memoryProbeModel{}
	observed := &memoryObservedModel{inner: probe}
	fixture, err := newMemoryAcceptanceFixture(observed, "offline-memory-probe", true, nonce)
	if err != nil {
		t.Fatal("could not construct memory evidence fixture")
	}
	result, err := fixture.runtime.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{RunID: "offline-memory-evidence", Text: memoryAcceptancePrompt}, nil)
	if err != nil || result.Status != core.RunCompleted {
		t.Fatal("memory evidence fixture did not complete")
	}
	expected := "MEMORY: deliberately-wrong"
	record := finalizedLiveEvidence(liveCase{name: "memory_completed_acceptance_failure"}, result, "offline-memory-probe", "test", "test-revision", memoryAcceptancePrompt, fixture.session.Events(), nil, 0, false)
	record.Memory = memoryEvidence(fixture.session.Events(), result, nil, observed, expected, nonce, true)
	if record.Memory == nil || !record.Memory.RecallResultFound || !record.Memory.RecallEntriesParsed || record.Memory.RecallEntryCount != 1 || !record.Memory.ContextExactMatch || !record.Memory.FirstTurnObserved || !record.Memory.NonceCheckApplicable || !record.Memory.FirstTurnNonceAbsent || record.Memory.AnswerMatches || record.Memory.DurableAnswerMatches || record.Memory.UsageMatches {
		t.Fatal("memory evidence did not distinguish the completed acceptance failure")
	}
	directory := t.TempDir()
	if err := writeLiveEvidence(directory, record); err != nil {
		t.Fatal("memory acceptance failure evidence was not written")
	}
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) != 1 {
		t.Fatal("memory evidence did not create one run-unique file")
	}
	payload, err := os.ReadFile(filepath.Join(directory, entries[0].Name()))
	if err != nil || strings.Contains(string(payload), nonce) {
		t.Fatal("memory evidence was unreadable or exposed a fixture value")
	}
	var decoded liveEvidenceRecord
	if json.Unmarshal(payload, &decoded) != nil || decoded.AcceptancePassed || decoded.Memory == nil || !decoded.Memory.RecallResultFound || decoded.Memory.AnswerMatches {
		t.Fatal("persisted memory evidence did not retain the completed acceptance failure")
	}
}

type memoryProbeModel struct {
	mu    sync.Mutex
	phase int
	turns [][]core.ChatMessage
}

func (*memoryProbeModel) Provider() string { return "offline-memory-probe" }

func (m *memoryProbeModel) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	m.mu.Lock()
	m.turns = append(m.turns, cloneMemoryMessages(options.Messages))
	phase := m.phase
	m.phase++
	m.mu.Unlock()
	switch phase {
	case 0:
		emitTool(emit, core.ToolCall{ID: "memory-recall", Name: extmemory.RecallCapabilityID, Args: map[string]any{"query": "Project Cedar", "limit": 1}})
		return nil
	case 1:
		value, err := memoryRecallAnswer(options.Messages)
		if err != nil {
			return err
		}
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: value})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
		return nil
	default:
		return errMemoryProbeExtraRound
	}
}

func (m *memoryProbeModel) sawRecallResult() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.turns) < 2 {
		return false
	}
	for _, message := range m.turns[1] {
		if message.Role == core.RoleTool && message.ToolCallID == "memory-recall" {
			return true
		}
	}
	return false
}

func memoryRecallAnswer(messages []core.ChatMessage) (string, error) {
	payload, err := lastToolJSON(messages, "memory-recall")
	if err != nil {
		return "", err
	}
	entries, ok := payload["entries"].([]any)
	if !ok {
		return "", errMemoryProbeInvalidResult
	}
	if len(entries) == 0 {
		return "MEMORY: UNKNOWN", nil
	}
	entry, ok := entries[0].(map[string]any)
	if !ok {
		return "", errMemoryProbeInvalidResult
	}
	content, ok := entry["content"].(string)
	if !ok {
		return "", errMemoryProbeInvalidResult
	}
	code, ok := strings.CutPrefix(content, "Project Cedar release code: ")
	if !ok || code == "" {
		return "", errMemoryProbeInvalidResult
	}
	return "MEMORY: " + code, nil
}

var (
	errMemoryProbeExtraRound    = errors.New("memory probe received an unexpected extra round")
	errMemoryProbeInvalidResult = errors.New("memory probe received an invalid recall result")
)
