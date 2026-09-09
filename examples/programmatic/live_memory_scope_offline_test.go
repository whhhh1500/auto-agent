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

// TestMemoryScopeFixtureOffline proves the same Runtime, standard capability,
// journal, and context-assembly contract before a real provider is enabled.
// It deliberately uses new opaque values on every run and does not model a
// provider response.
func TestMemoryScopeFixtureOffline(t *testing.T) {
	for _, test := range []struct {
		name      string
		relevant  bool
		wantCount int
	}{
		{name: "relevant", relevant: true, wantCount: 1},
		{name: "no_match", relevant: false, wantCount: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			values, err := newMemoryScopeValues()
			if err != nil {
				t.Fatal("could not create opaque memory fixture values")
			}
			query := values.Lookup
			if !test.relevant {
				query = values.Missing
			}
			prompt := memoryScopePrompt(query)
			if memoryScopePromptLeaks(prompt, values) {
				t.Fatal("offline memory scope fixture value leaked into prompt")
			}
			probe := &memoryScopeProbe{query: query}
			observed := &memoryObservedModel{inner: probe}
			fixture, err := newMemoryScopeFixture(observed, "offline-memory-scope", values, "offline-"+test.name)
			if err != nil {
				t.Fatal("could not construct offline scoped memory fixture")
			}
			result, err := fixture.runtime.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{RunID: "offline-memory-scope-" + test.name, Text: prompt}, nil)
			if err != nil || result.Status != core.RunCompleted {
				t.Fatal("offline scoped memory case did not complete")
			}
			assertMemoryScopePredicates(t, fixture, observed, fixture.session.Events(), result, values, test.relevant, test.wantCount)
			usage, reports := memoryScopeUsage(fixture.session.Events())
			if reports != 2 || usage.InputTokens == 0 || usage.OutputTokens == 0 || fixture.assemblerCalls.Load() < 2 {
				t.Fatal("offline scoped memory case did not preserve two-round usage and context evidence")
			}
			evidence := memoryScopeEvidenceFromRun(test.name, result.RunID, fixture, observed, nil, fixture.session.Events(), result, values, test.relevant)
			expectedOutcome := evidence.TargetMatched
			if !test.relevant {
				expectedOutcome = evidence.NoMatchHonest
			}
			if evidence.RequestedModel != "offline-memory-scope" || !evidence.ResultObserved || !evidence.ContextObserved || !evidence.UsageComplete || evidence.ReportedInputTokens == 0 || evidence.ReportedOutputTokens == 0 || !expectedOutcome || !evidence.ForeignAbsentToolResult || !evidence.ForeignAbsentFinalContext || !evidence.ForeignAbsentAnswer || !evidence.UnrelatedAbsentToolResult || !evidence.UnrelatedAbsentFinalContext || !evidence.InitialTargetAbsent || !evidence.ContextExactPair || !evidence.UsageLedgerMatches || !evidence.PeerStoreIntact {
				t.Fatal("offline scoped memory evidence did not retain all safety predicates")
			}
			directory := t.TempDir()
			if err := writeMemoryScopeEvidence(directory, evidence); err != nil {
				t.Fatal("offline scoped memory evidence was not written")
			}
			entries, err := os.ReadDir(directory)
			if err != nil || len(entries) != 1 {
				t.Fatal("offline scoped memory evidence did not create one record")
			}
			payload, err := os.ReadFile(filepath.Join(directory, entries[0].Name()))
			if err != nil || strings.Contains(string(payload), values.Lookup) || strings.Contains(string(payload), values.Target) || strings.Contains(string(payload), values.Foreign) || strings.Contains(string(payload), values.Unrelated) {
				t.Fatal("offline scoped memory evidence exposed a fixture value")
			}
			var persisted memoryScopeEvidence
			if json.Unmarshal(payload, &persisted) != nil || persisted.Schema == "" || persisted.CaseID != test.name {
				t.Fatal("offline scoped memory evidence was malformed")
			}
		})
	}
}

type memoryScopeProbe struct {
	mu    sync.Mutex
	phase int
	query string
}

func (*memoryScopeProbe) Provider() string { return "offline-memory-scope" }

func (m *memoryScopeProbe) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	m.mu.Lock()
	phase := m.phase
	m.phase++
	m.mu.Unlock()
	switch phase {
	case 0:
		call := core.ToolCall{ID: "memory-scope-recall", Name: extmemory.RecallCapabilityID, Args: map[string]any{"query": m.query, "limit": 1}}
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &call, Usage: &core.TokenUsage{InputTokens: 5, OutputTokens: 1}})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
		return nil
	case 1:
		answer, err := memoryScopeProbeAnswer(options.Messages, "memory-scope-recall")
		if err != nil {
			return err
		}
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: answer, Usage: &core.TokenUsage{InputTokens: 7, OutputTokens: 2}})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
		return nil
	default:
		return errors.New("offline memory scope probe received an unexpected extra round")
	}
}

func memoryScopeProbeAnswer(messages []core.ChatMessage, callID string) (string, error) {
	payload, err := lastToolJSON(messages, callID)
	if err != nil {
		return "", err
	}
	entries, ok := payload["entries"].([]any)
	if !ok {
		return "", errors.New("offline memory scope probe received an invalid result")
	}
	if len(entries) == 0 {
		return "UNKNOWN", nil
	}
	if len(entries) != 1 {
		return "", errors.New("offline memory scope probe received multiple entries")
	}
	entry, ok := entries[0].(map[string]any)
	if !ok {
		return "", errors.New("offline memory scope probe entry is invalid")
	}
	content, ok := entry["content"].(string)
	if !ok {
		return "", errors.New("offline memory scope probe content is invalid")
	}
	_, code, found := strings.Cut(content, "code=")
	if !found || strings.TrimSpace(code) == "" {
		return "", errors.New("offline memory scope probe found no code")
	}
	return strings.TrimSpace(code), nil
}
