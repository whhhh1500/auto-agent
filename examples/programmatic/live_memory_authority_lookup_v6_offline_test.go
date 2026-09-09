package main

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	core "github.com/whhhh1500/auto-agent/pkg/core"
	extmemory "github.com/whhhh1500/auto-agent/pkg/extensions/memory"
)

func TestMemoryAuthorityLookupV6PromptOnlyRendersCanonicalArgument(t *testing.T) {
	const literal = ` For memory.lookup, use exactly {"key":"Node-7.Alpha"}; do not change any character.`
	v5, v6 := memoryAuthorityLookupV5Prompt("Node-7.Alpha"), memoryAuthorityLookupV6Prompt()
	if v6 != v5+literal {
		t.Fatal("v6 differs from v5 by more than the canonical lookup argument rendering")
	}
	if !strings.Contains(v6, `{"key":"Node-7.Alpha"}`) || strings.Contains(v6, "then call authority.current") {
		t.Fatal("v6 mixed-case literal or V5 parallel-read contract changed")
	}
}

func TestMemoryAuthorityLookupV6OfflineProbeUsesLiteralExactKey(t *testing.T) {
	values, err := newMemoryAuthorityValuesForEntity("Node-7.Alpha")
	if err != nil {
		t.Fatal(err)
	}
	prompt := memoryAuthorityLookupV6Prompt()
	probe := &memoryAuthorityLookupV6Probe{expectedPrompt: prompt}
	model := newLiveModel(t, probe, "offline-memory-authority-lookup-v6", 2, nil)
	observed := &memoryObservedModel{inner: model}
	fixture, err := newMemoryAuthorityLookupFixture(observed, "offline-memory-authority-lookup", values, "offline-v6-mixed-case")
	if err != nil {
		t.Fatal(err)
	}
	result, err := fixture.runtime.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{RunID: "offline-memory-authority-lookup-v6-mixed-case", Text: prompt}, nil)
	if err != nil || result.Status != core.RunCompleted {
		t.Fatal("v6 mixed-case fixture did not complete")
	}
	if !probe.exactPromptObserved() {
		t.Fatal("v6 probe did not receive the canonical-argument user prompt")
	}
	if err := assertMemoryAuthorityLookupRun(fixture, observed, model, fixture.session.Events(), result, values); err != nil {
		t.Fatal(err)
	}
	lookupCall, lookupResult, ok := memoryAuthorityLookupToolPair(fixture.session.Events())
	if !ok || len(lookupCall.Args) != 1 || lookupCall.Args["key"] != "Node-7.Alpha" {
		t.Fatal("v6 probe did not produce the literal exact lookup argument")
	}
	parsed := parseLookupAuthorityResult(lookupResult.Content)
	if !parsed.Valid || !parsed.Found || parsed.Entry == nil || parsed.Entry.Key != "Node-7.Alpha" {
		t.Fatal("v6 probe did not receive the exact mixed-case entry")
	}
}

type memoryAuthorityLookupV6Probe struct {
	mu             sync.Mutex
	phase          int
	expectedPrompt string
	promptSeen     bool
}

func (*memoryAuthorityLookupV6Probe) Provider() string { return "offline-memory-authority-lookup-v6" }

func (m *memoryAuthorityLookupV6Probe) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	m.mu.Lock()
	phase := m.phase
	m.phase++
	if phase == 0 {
		for _, message := range options.Messages {
			if message.Role == core.RoleUser && message.Content == m.expectedPrompt {
				m.promptSeen = true
			}
		}
	}
	m.mu.Unlock()
	entity, err := memoryAuthorityProbeEntity(options.Tools)
	if err != nil {
		return err
	}
	switch phase {
	case 0:
		if entity != "Node-7.Alpha" || !m.exactPromptObserved() {
			return errors.New("v6 probe did not receive its exact authority entity or prompt")
		}
		lookup := core.ToolCall{ID: "v6-lookup-historical", Name: extmemory.LookupCapabilityID, Args: map[string]any{"key": "Node-7.Alpha"}}
		authority := core.ToolCall{ID: "v6-lookup-current", Name: authorityCurrentID, Args: map[string]any{"entity": entity}}
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &lookup, ToolCalls: []core.ToolCall{lookup, authority}, Usage: &core.TokenUsage{InputTokens: 5, OutputTokens: 1}})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
		return nil
	case 1:
		if _, err := lastToolContent(options.Messages, "v6-lookup-historical"); err != nil {
			return err
		}
		content, err := lastToolContent(options.Messages, "v6-lookup-current")
		if err != nil {
			return err
		}
		available, code, valid := memoryAuthorityResult(content, "Node-7.Alpha")
		if !available || !valid {
			return errors.New("v6 probe received an invalid authority result")
		}
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: code, Usage: &core.TokenUsage{InputTokens: 7, OutputTokens: 2}})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
		return nil
	default:
		return errors.New("v6 probe received an unexpected extra round")
	}
}

func (m *memoryAuthorityLookupV6Probe) exactPromptObserved() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.promptSeen
}
