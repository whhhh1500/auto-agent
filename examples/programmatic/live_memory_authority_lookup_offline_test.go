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

func TestMemoryAuthorityLookupV4OfflineFrozenEntities(t *testing.T) {
	for _, entity := range []string{"release.channel/v2", "billing-policy:eu_west", "Node-7.Alpha"} {
		entity := entity
		t.Run(entity, func(t *testing.T) {
			values, err := newMemoryAuthorityValuesForEntity(entity)
			if err != nil {
				t.Fatal(err)
			}
			model := newLiveModel(t, &memoryAuthorityLookupProbe{}, "offline-memory-authority-lookup", 2, nil)
			observed := &memoryObservedModel{inner: model}
			fixture, err := newMemoryAuthorityLookupFixture(observed, "offline-memory-authority-lookup", values, "offline-lookup")
			if err != nil {
				t.Fatal(err)
			}
			result, err := fixture.runtime.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{RunID: "offline-memory-authority-lookup", Text: memoryAuthorityLookupPrompt(entity)}, nil)
			if err != nil || result.Status != core.RunCompleted {
				t.Fatal("lookup authority fixture did not complete")
			}
			if err := assertMemoryAuthorityLookupRun(fixture, observed, model, fixture.session.Events(), result, values); err != nil {
				t.Fatal(err)
			}
			if model.rounds() != 2 {
				t.Fatal("lookup authority exceeded two model rounds")
			}
		})
	}
}

func TestMemoryAuthorityLookupV4RejectsNegativeEvidence(t *testing.T) {
	for _, test := range []struct {
		name                                            string
		wrongKey, missing, historicalAnswer, peerAnswer bool
	}{
		{name: "wrong_key", wrongKey: true},
		{name: "found_false", missing: true},
		{name: "historical_answer", historicalAnswer: true},
		{name: "peer_scope_answer", peerAnswer: true},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			values, err := newMemoryAuthorityValuesForEntity("Node-7.Alpha")
			if err != nil {
				t.Fatal(err)
			}
			probe := &memoryAuthorityLookupProbe{wrongKey: test.wrongKey, missing: test.missing, historicalAnswer: test.historicalAnswer, peerAnswer: test.peerAnswer, foreign: values.Foreign}
			model := newLiveModel(t, probe, "offline-memory-authority-lookup-"+test.name, 2, nil)
			observed := &memoryObservedModel{inner: model}
			fixture, err := newMemoryAuthorityLookupFixture(observed, "offline-memory-authority-lookup", values, "offline-negative-"+test.name)
			if err != nil {
				t.Fatal(err)
			}
			result, err := fixture.runtime.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{RunID: "offline-memory-authority-lookup-" + test.name, Text: memoryAuthorityLookupPrompt(values.Entity)}, nil)
			if err != nil || result.Status != core.RunCompleted {
				t.Fatal("negative lookup authority fixture did not complete")
			}
			lookupCall, lookupResult, ok := memoryAuthorityLookupToolPair(fixture.session.Events())
			if !ok {
				t.Fatal("negative lookup authority fixture did not call lookup")
			}
			switch test.name {
			case "wrong_key":
				key, _ := lookupCall.Args["key"].(string)
				if key == values.Entity || parseLookupAuthorityResult(lookupResult.Content).Found {
					t.Fatal("wrong-key negative did not exercise an exact-key miss")
				}
			case "found_false":
				parsed := parseLookupAuthorityResult(lookupResult.Content)
				if !parsed.Valid || parsed.Found || parsed.Entry != nil {
					t.Fatal("found-false negative did not return the closed absent structure")
				}
			case "historical_answer":
				if strings.TrimSpace(result.Answer) != values.Historical {
					t.Fatal("historical-answer negative did not use the historical value")
				}
			case "peer_scope_answer":
				if strings.TrimSpace(result.Answer) != values.Foreign {
					t.Fatal("peer-scope negative did not attempt the peer value")
				}
			}
			if err := assertMemoryAuthorityLookupRun(fixture, observed, model, fixture.session.Events(), result, values); err == nil {
				t.Fatal("lookup authority acceptance accepted an invalid route")
			}
			if test.name == "peer_scope_answer" && !memoryAuthorityLookupPeerIntact(fixture) {
				t.Fatal("peer negative mutated the peer scope")
			}
			if test.name == "peer_scope_answer" {
				record := liveMemoryAuthorityLookupRecord(liveCase{name: "lookup-v4-peer-negative"}, result, "offline-model", model.Provider(), values.Entity, fixture.session.Events(), model, fixture, observed, false, 0)
				payload, marshalErr := json.Marshal(record)
				if marshalErr != nil || record.ForeignAbsent || strings.Contains(string(payload), values.Foreign) || strings.Contains(string(payload), fixture.peerSnapshot.Content) {
					t.Fatal("peer-code negative did not remain redacted while marking foreign evidence present")
				}
			}
		})
	}
}

func memoryAuthorityLookupToolPair(events []core.SessionEvent) (core.ToolCallData, core.ToolResultData, bool) {
	calls, results, _, _ := liveSessionEvidence(events)
	for _, call := range calls {
		if call.Name != extmemory.LookupCapabilityID {
			continue
		}
		for _, toolResult := range results {
			if toolResult.CallID == call.CallID {
				return call, toolResult, true
			}
		}
	}
	return core.ToolCallData{}, core.ToolResultData{}, false
}

func TestMemoryAuthorityLookupV4ProfileOmitsDefaultMemoryMenu(t *testing.T) {
	values, err := newMemoryAuthorityValuesForEntity("release.channel/v2")
	if err != nil {
		t.Fatal(err)
	}
	model := newLiveModel(t, &memoryAuthorityLookupProbe{}, "offline-memory-authority-lookup-menu", 2, nil)
	observed := &memoryObservedModel{inner: model}
	fixture, err := newMemoryAuthorityLookupFixture(observed, "offline-memory-authority-lookup", values, "offline-menu")
	if err != nil {
		t.Fatal(err)
	}
	result, err := fixture.runtime.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{RunID: "offline-memory-authority-lookup-menu", Text: memoryAuthorityLookupPrompt(values.Entity)}, nil)
	if err != nil || result.Status != core.RunCompleted || !lookupAuthorityProfileOnly(fixture.session.Events()) {
		t.Fatal("lookup profile did not expose exactly lookup and authority")
	}
	for _, event := range fixture.session.Events() {
		if event.Type != core.EvRunStart {
			continue
		}
		var start core.RunStartData
		if json.Unmarshal(event.Data, &start) != nil || start.Composition == nil {
			t.Fatal("run composition malformed")
		}
		for _, capability := range start.Composition.Capabilities {
			if capability.Manifest.ID == extmemory.RecallCapabilityID || capability.Manifest.ID == "memory.remember" || capability.Manifest.ID == "memory.forget" {
				t.Fatal("default memory capability entered lookup profile")
			}
		}
	}
}

func TestMemoryAuthorityLookupV4EvidenceRedactsFixtureValues(t *testing.T) {
	values, err := newMemoryAuthorityValuesForEntity("billing-policy:eu_west")
	if err != nil {
		t.Fatal(err)
	}
	model := newLiveModel(t, &memoryAuthorityLookupProbe{}, "offline-memory-authority-lookup-evidence", 2, nil)
	observed := &memoryObservedModel{inner: model}
	fixture, err := newMemoryAuthorityLookupFixture(observed, "offline-memory-authority-lookup", values, "offline-evidence")
	if err != nil {
		t.Fatal(err)
	}
	result, err := fixture.runtime.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{RunID: "offline-memory-authority-lookup-evidence", Text: memoryAuthorityLookupPrompt(values.Entity)}, nil)
	if err != nil || result.Status != core.RunCompleted {
		t.Fatal("lookup evidence fixture did not complete")
	}
	record := liveMemoryAuthorityLookupRecord(liveCase{name: "lookup-v4"}, result, "offline-model", model.Provider(), values.Entity, fixture.session.Events(), model, fixture, observed, true, 0)
	payload, err := json.Marshal(record)
	if err != nil || !record.UsageComplete || !record.LookupKeyExact || !record.LookupFound || !record.LookupEntryStructured || !record.AuthorityStructured || !record.ConflictRecognized || !record.ContextExactPair || !record.JournalExactPair || !record.CurrentAnswer || !record.CurrentDurableAnswer || !record.PeerScopeIsolated || !record.ForeignAbsent || record.BackendIdentity != "not_independently_verified" {
		t.Fatal("lookup evidence is incomplete")
	}
	for _, prohibited := range []string{values.Entity, values.Historical, values.Current, values.Foreign, result.Answer, fixture.historicalEntry.Content, fixture.peerSnapshot.Content} {
		if prohibited != "" && strings.Contains(string(payload), prohibited) {
			t.Fatal("lookup evidence retained raw fixture or answer text")
		}
	}
}

type memoryAuthorityLookupProbe struct {
	mu                                              sync.Mutex
	phase                                           int
	wrongKey, missing, historicalAnswer, peerAnswer bool
	foreign                                         string
}

func (*memoryAuthorityLookupProbe) Provider() string { return "offline-memory-authority-lookup" }

func (m *memoryAuthorityLookupProbe) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	m.mu.Lock()
	phase, wrongKey, missing, historicalAnswer, peerAnswer, foreign := m.phase, m.wrongKey, m.missing, m.historicalAnswer, m.peerAnswer, m.foreign
	m.phase++
	m.mu.Unlock()
	entity, err := memoryAuthorityProbeEntity(options.Tools)
	if err != nil {
		return err
	}
	switch phase {
	case 0:
		key := entity
		if wrongKey {
			key = "wrong-" + entity
		}
		if missing {
			key = entity + "-missing"
		}
		lookup := core.ToolCall{ID: "lookup-historical", Name: extmemory.LookupCapabilityID, Args: map[string]any{"key": key}}
		authority := core.ToolCall{ID: "lookup-current", Name: authorityCurrentID, Args: map[string]any{"entity": entity}}
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &lookup, ToolCalls: []core.ToolCall{lookup, authority}, Usage: &core.TokenUsage{InputTokens: 5, OutputTokens: 1}})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
		return nil
	case 1:
		lookupContent, err := lastToolContent(options.Messages, "lookup-historical")
		if err != nil {
			return err
		}
		var lookup parsedLookupAuthorityResult
		lookup = parseLookupAuthorityResult(lookupContent)
		authorityContent, err := lastToolContent(options.Messages, "lookup-current")
		if err != nil {
			return err
		}
		available, code, valid := memoryAuthorityResult(authorityContent, entity)
		if !available || !valid {
			return errors.New("lookup authority probe received invalid authority result")
		}
		answer := code
		if historicalAnswer && lookup.Entry != nil {
			answer = strings.TrimPrefix(strings.Split(lookup.Entry.Content, "; code=")[1], "")
		}
		if peerAnswer {
			answer = foreign
		}
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: answer, Usage: &core.TokenUsage{InputTokens: 7, OutputTokens: 2}})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
		return nil
	default:
		return errors.New("lookup authority probe received an unexpected extra round")
	}
}
