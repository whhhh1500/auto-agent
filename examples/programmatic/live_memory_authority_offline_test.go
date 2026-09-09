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

func TestMemoryAuthorityDiagnosticV3IsSingleDescriptionVariant(t *testing.T) {
	values, err := newMemoryAuthorityValues()
	if err != nil {
		t.Fatal("could not create opaque authority values")
	}
	arms := memoryAuthorityDiagnosticArms(values, "memory_authority_description_v3", "")
	if len(arms) != 1 {
		t.Fatal("description v3 diagnostic did not define exactly one arm")
	}
	arm := arms[0]
	if arm.arm != "read_both_conflict" || arm.caseKind != "diagnostic" || arm.fixtureVariant != "memory_authority_description_v3" || arm.prompt != memoryAuthorityPrompt(true) {
		t.Fatal("description v3 diagnostic diverged from the frozen read-both contract")
	}
	for _, ordinary := range []string{"automatic_match", "automatic_conflict", "authority_unavailable", "direct_only_conflict"} {
		if arm.arm == ordinary {
			t.Fatal("description v3 diagnostic unexpectedly scheduled an ordinary arm")
		}
	}
	if memoryAuthorityDiagnosticStrictPredicate(arm.fixtureVariant) == nil || memoryAuthorityDiagnosticStrictPredicate("memory_authority_heldout_v3") == nil || memoryAuthorityDiagnosticStrictPredicate("memory_authority_v1") != nil {
		t.Fatal("description v3 diagnostic strictness diverged from its fixture contract")
	}
}

func TestMemoryAuthorityQueryShapeClassification(t *testing.T) {
	for _, test := range []struct {
		name, query, entity string
		observed            bool
		want                memoryAuthorityQueryShape
	}{
		{name: "exact", query: "release.channel/v2", entity: "release.channel/v2", observed: true, want: memoryQueryShapeExact},
		{name: "ascii_separators", query: "release channel v2", entity: "release.channel/v2", observed: true, want: memoryQueryShapeSeparatorNormalizedEqual},
		{name: "unicode_case_and_punctuation", query: "BILLING—POLICY:EU_WEST", entity: "billing-policy:eu_west", observed: true, want: memoryQueryShapeSeparatorNormalizedEqual},
		{name: "query_contiguous_subset", query: "billing policy", entity: "billing-policy:eu_west", observed: true, want: memoryQueryShapeQueryTokensSubset},
		{name: "truncated_query", query: "node 7", entity: "node-7.alpha", observed: true, want: memoryQueryShapeQueryTokensSubset},
		{name: "entity_contiguous_subset_with_extra_words", query: "please release channel v2 now", entity: "release.channel/v2", observed: true, want: memoryQueryShapeEntityTokensSubset},
		{name: "token_set_equal", query: "v2 channel release", entity: "release.channel/v2", observed: true, want: memoryQueryShapeTokenSetEqual},
		{name: "other", query: "release v3", entity: "release.channel/v2", observed: true, want: memoryQueryShapeOther},
		{name: "punctuation_only", query: "---", entity: "release.channel/v2", observed: true, want: memoryQueryShapeOther},
		{name: "unobserved", query: "", entity: "release.channel/v2", observed: false, want: memoryQueryShapeUnobserved},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := classifyMemoryAuthorityQueryShape(test.query, test.entity, test.observed); got != test.want {
				t.Fatalf("shape=%q want=%q", got, test.want)
			}
		})
	}
}

func TestMemoryAuthorityShapeDiagnosticPredicateAllowsNonexactObservation(t *testing.T) {
	evidence := memoryAuthorityEvidence{
		MemoryQueryObserved: true, MemoryQueryShape: memoryQueryShapeQueryTokensSubset,
		FixtureObserved: true, InitialContextObserved: true, InitialValuesAbsent: true,
		MemoryCallObserved: true, MemoryResultObserved: true, MemoryResultOK: true, MemoryContextExactPair: true, MemoryResultEntriesParsed: true, MemoryResultEntryCount: 0,
		AuthorityCallObserved: true, AuthorityResultObserved: true, AuthorityResultOK: true, AuthorityContextExactPair: true,
		AuthorityAvailable: true, AuthorityResultValid: true, AuthorityCurrentValueObserved: true,
		ContextExactPair: true, ForeignAbsentContext: true, ForeignAbsentToolResults: true, ForeignAbsentAnswer: true,
		AnswerMatchesArm: true, DurableAnswerMatchesArm: true, PeerStoreIntact: true, JournalMatchesCalls: true,
		ModelRounds: 2, AssemblerCalls: 2,
	}
	if !memoryAuthorityShapeDiagnosticComplete(evidence) {
		t.Fatal("shape diagnostic rejected a complete nonexact observation")
	}
	evidence.AuthorityCurrentValueObserved = false
	if memoryAuthorityShapeDiagnosticComplete(evidence) {
		t.Fatal("shape diagnostic accepted missing authority evidence")
	}
	evidence.AuthorityCurrentValueObserved = true
	for _, test := range []struct {
		name   string
		mutate func(*memoryAuthorityEvidence)
	}{
		{name: "unobserved", mutate: func(e *memoryAuthorityEvidence) { e.MemoryQueryShape = memoryQueryShapeUnobserved }},
		{name: "query_not_observed", mutate: func(e *memoryAuthorityEvidence) { e.MemoryQueryObserved = false }},
		{name: "entries_not_parsed", mutate: func(e *memoryAuthorityEvidence) { e.MemoryResultEntriesParsed = false }},
		{name: "too_many_entries", mutate: func(e *memoryAuthorityEvidence) { e.MemoryResultEntryCount = 2 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate := evidence
			test.mutate(&candidate)
			if memoryAuthorityShapeDiagnosticComplete(candidate) {
				t.Fatal("shape diagnostic accepted incomplete memory observation")
			}
		})
	}
}

func TestMemoryAuthorityDescriptionV3UsesProductionRecallContract(t *testing.T) {
	values, err := newMemoryAuthorityValues()
	if err != nil {
		t.Fatal("could not create opaque authority values")
	}
	fixture, err := newMemoryAuthorityFixture(&memoryAuthorityProbe{readBoth: true}, "offline-memory-authority", values, "read_both_conflict", "offline-description-v3")
	if err != nil {
		t.Fatal("could not construct production description v3 authority fixture")
	}
	result, err := fixture.runtime.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{RunID: "offline-description-v3", Text: memoryAuthorityPrompt(true)}, nil)
	if err != nil || result.Status != core.RunCompleted {
		t.Fatal("production description v3 fixture did not complete")
	}
	recall := memoryAuthoritySnapshotCapability(t, fixture.session.Events(), extmemory.RecallCapabilityID)
	if recall.ProviderRevision != "sha256:062f61578f23e9e49387340fe7524c79d8eba706932fb82692f88343fb6cfae8" {
		t.Fatalf("production memory.recall revision=%q", recall.ProviderRevision)
	}
	if recall.Manifest.Tool == nil {
		t.Fatal("production memory.recall tool exposure is absent")
	}
	properties, ok := recall.Manifest.Tool.Parameters["properties"].(map[string]any)
	if !ok {
		t.Fatal("production memory.recall properties are absent")
	}
	query, ok := properties["query"].(map[string]any)
	description, ok := query["description"].(string)
	if !ok || !strings.Contains(description, "canonical identifier") || !strings.Contains(description, "copy it exactly") || !strings.Contains(description, "does not automatically rewrite") {
		t.Fatal("production memory.recall query description lacks the canonical exact-query contract")
	}
}

func memoryAuthoritySnapshotCapability(t *testing.T, events []core.SessionEvent, id string) core.SnapshotCapability {
	t.Helper()
	for _, event := range events {
		if event.Type != core.EvRunStart {
			continue
		}
		var start core.RunStartData
		if json.Unmarshal(event.Data, &start) != nil || start.Composition == nil {
			t.Fatal("authority fixture run composition was malformed")
		}
		for _, capability := range start.Composition.Capabilities {
			if capability.Manifest.ID == id {
				return capability
			}
		}
	}
	t.Fatal("authority fixture capability snapshot was absent")
	return core.SnapshotCapability{}
}

func TestMemoryAuthorityHeldoutEntitiesUseScenarioContract(t *testing.T) {
	for _, item := range []struct{ label, entity string }{
		{label: "heldout_1", entity: "release.channel/v2"},
		{label: "heldout_2", entity: "billing-policy:eu_west"},
		{label: "heldout_3", entity: "node-7.alpha"},
	} {
		t.Run(item.label, func(t *testing.T) {
			values, err := newMemoryAuthorityValuesForEntity(item.entity)
			if err != nil {
				t.Fatal("could not create opaque held-out authority values")
			}
			probe := &memoryAuthorityProbe{readBoth: true}
			model := newLiveModel(t, probe, "offline-memory-authority-"+item.label, 3, nil)
			observed := &memoryObservedModel{inner: model}
			fixture, err := newMemoryAuthorityFixture(observed, "offline-memory-authority", values, "read_both_conflict", "offline-"+item.label)
			if err != nil {
				t.Fatal("could not construct held-out authority fixture")
			}
			prompt := memoryAuthorityPromptForEntity(values.Entity, true)
			result, err := fixture.runtime.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{RunID: "offline-memory-authority-" + item.label, Text: prompt}, nil)
			if err != nil || result.Status != core.RunCompleted {
				t.Fatal("held-out authority fixture did not complete")
			}
			if err := assertMemoryAuthorityArm(fixture, observed, model, fixture.session.Events(), result, values, values.Current, true, true, false); err != nil {
				t.Fatal(err)
			}
			evidence := memoryAuthorityEvidenceFromRun(fixture, observed, model, fixture.session.Events(), result, values, values.Current, true, true, false, "diagnostic", "diagnostic_noncomparable")
			if !evidence.MemoryQueryEqualsEntity || !evidence.MemoryQueryNormalizedEqualsEntity || !memoryAuthorityExactQueryDiagnosticStrict(evidence) {
				t.Fatal("held-out entity did not satisfy exact-query conflict evidence")
			}
		})
	}
}

func TestMemoryAuthorityHeldoutRejectsLaunchCodeArguments(t *testing.T) {
	values, err := newMemoryAuthorityValuesForEntity("release.channel/v2")
	if err != nil {
		t.Fatal("could not create opaque held-out authority values")
	}
	probe := &memoryAuthorityProbe{readBoth: true, entityOverride: authorityEntity}
	model := newLiveModel(t, probe, "offline-memory-authority-heldout-launch-code", 3, nil)
	observed := &memoryObservedModel{inner: model}
	fixture, err := newMemoryAuthorityFixture(observed, "offline-memory-authority", values, "read_both_conflict", "offline-heldout-launch-code")
	if err != nil {
		t.Fatal("could not construct held-out authority negative fixture")
	}
	prompt := memoryAuthorityPromptForEntity(values.Entity, true)
	result, _ := fixture.runtime.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{RunID: "offline-memory-authority-heldout-launch-code", Text: prompt}, nil)
	evidence := memoryAuthorityEvidenceFromRun(fixture, observed, model, fixture.session.Events(), result, values, values.Current, true, true, false, "diagnostic", "diagnostic_noncomparable")
	if evidence.MemoryQueryEqualsEntity || evidence.MemoryQueryNormalizedEqualsEntity || memoryAuthorityExactQueryDiagnosticStrict(evidence) {
		t.Fatal("held-out fixture accepted a launch-code query for another canonical identifier")
	}
	calls, _, _, _ := liveSessionEvidence(fixture.session.Events())
	for _, call := range calls {
		if call.Name == extmemory.RecallCapabilityID {
			if query, ok := call.Args["query"].(string); !ok || query != authorityEntity {
				t.Fatal("negative probe did not exercise the launch-code argument")
			}
			return
		}
	}
	t.Fatal("negative probe did not call memory.recall")
}

func TestMemoryAuthorityFixtureOffline(t *testing.T) {
	values, err := newMemoryAuthorityValues()
	if err != nil {
		t.Fatal("could not create opaque authority values")
	}
	for _, arm := range []struct {
		name                             string
		readBoth, sequential, wantMemory bool
		expected                         string
	}{
		{name: "automatic_match", expected: values.Current},
		{name: "automatic_conflict", expected: values.Current},
		{name: "authority_unavailable", expected: "UNKNOWN"},
		{name: "direct_only_conflict", expected: values.Current},
		{name: "read_both_conflict", readBoth: true, wantMemory: true, expected: values.Current},
		{name: "read_both_conflict_sequential", readBoth: true, sequential: true, wantMemory: true, expected: values.Current},
	} {
		t.Run(arm.name, func(t *testing.T) {
			armValues := values
			if arm.name == "automatic_match" {
				armValues.Historical = armValues.Current
			}
			prompt := memoryAuthorityPrompt(arm.readBoth)
			probe := &memoryAuthorityProbe{readBoth: arm.readBoth, sequential: arm.sequential}
			model := newLiveModel(t, probe, "offline-memory-authority-"+arm.name, 3, nil)
			observed := &memoryObservedModel{inner: model}
			fixture, err := newMemoryAuthorityFixture(observed, "offline-memory-authority", armValues, arm.name, "offline-"+arm.name)
			if err != nil {
				t.Fatal("could not construct authority fixture")
			}
			result, err := fixture.runtime.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{RunID: "offline-memory-authority-" + arm.name, Text: prompt}, nil)
			if err != nil || result.Status != core.RunCompleted {
				t.Fatal("authority fixture did not complete")
			}
			if err := assertMemoryAuthorityArm(fixture, observed, model, fixture.session.Events(), result, armValues, arm.expected, arm.wantMemory, arm.readBoth, arm.name == "authority_unavailable"); err != nil {
				t.Fatal(err)
			}
			wantRounds := 2
			if arm.sequential {
				wantRounds = 3
			}
			if model.rounds() != wantRounds {
				t.Fatal("deterministic authority probe did not finish in the expected rounds")
			}
		})
	}
}

func TestMemoryAuthorityRejectsWrongOrGuessedAnswers(t *testing.T) {
	values, err := newMemoryAuthorityValues()
	if err != nil {
		t.Fatal("could not create opaque authority values")
	}
	for _, test := range []struct {
		name                    string
		probe                   *memoryAuthorityProbe
		fixtureArm, expected    string
		wantMemory, unavailable bool
	}{
		{name: "wrong_authority_answer", probe: &memoryAuthorityProbe{wrongAnswer: true}, fixtureArm: "automatic_conflict", expected: values.Current},
		{name: "guessed_without_authority", probe: &memoryAuthorityProbe{skipAuthority: true, guessed: values.Current}, fixtureArm: "automatic_conflict", expected: values.Current},
		{name: "historical_after_conflict", probe: &memoryAuthorityProbe{readBoth: true, guessed: values.Historical}, fixtureArm: "automatic_conflict", expected: values.Current, wantMemory: true},
		{name: "historical_after_unavailable", probe: &memoryAuthorityProbe{readBoth: true, guessed: values.Historical}, fixtureArm: "authority_unavailable", expected: "UNKNOWN", wantMemory: true, unavailable: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			model := newLiveModel(t, test.probe, "offline-memory-authority-"+test.name, 3, nil)
			observed := &memoryObservedModel{inner: model}
			fixture, err := newMemoryAuthorityFixture(observed, "offline-memory-authority", values, test.fixtureArm, "offline-"+test.name)
			if err != nil {
				t.Fatal("could not construct authority fixture")
			}
			result, err := fixture.runtime.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{RunID: "offline-memory-authority-" + test.name, Text: memoryAuthorityPrompt(test.probe.readBoth)}, nil)
			if err != nil || result.Status != core.RunCompleted {
				t.Fatal("authority rejection fixture did not complete")
			}
			if err := assertMemoryAuthorityArm(fixture, observed, model, fixture.session.Events(), result, values, test.expected, test.wantMemory, test.wantMemory, test.unavailable); err == nil {
				t.Fatal("authority acceptance accepted an invalid answer source")
			}
		})
	}
}

func TestMemoryAuthorityRejectsInvalidEntity(t *testing.T) {
	values, err := newMemoryAuthorityValues()
	if err != nil {
		t.Fatal("could not create opaque authority values")
	}
	probe := &memoryAuthorityProbe{invalidEntity: true}
	model := newLiveModel(t, probe, "offline-memory-authority-invalid-entity", 3, nil)
	observed := &memoryObservedModel{inner: model}
	fixture, err := newMemoryAuthorityFixture(observed, "offline-memory-authority", values, "direct_only_conflict", "offline-invalid-entity")
	if err != nil {
		t.Fatal("could not construct authority fixture")
	}
	result, err := fixture.runtime.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{RunID: "offline-memory-authority-invalid-entity", Text: memoryAuthorityPrompt(false)}, nil)
	if err != nil || result.Status != core.RunCompleted || strings.TrimSpace(result.Answer) != "UNKNOWN" {
		t.Fatal("invalid entity did not complete honestly")
	}
	calls, results, _, _ := liveSessionEvidence(fixture.session.Events())
	if len(calls) != 1 || len(results) != 1 || calls[0].Name != authorityCurrentID || results[0].OK || results[0].Metadata["code"] != core.CodeInvalidArgs || fixture.authority.callsN() != 0 {
		t.Fatal("invalid entity was not rejected by accepted authority invocation")
	}
}

type memoryAuthorityProbe struct {
	mu                                                              sync.Mutex
	phase                                                           int
	readBoth, sequential, wrongAnswer, skipAuthority, invalidEntity bool
	entityOverride                                                  string
	guessed                                                         string
}

func (*memoryAuthorityProbe) Provider() string { return "offline-memory-authority" }
func (m *memoryAuthorityProbe) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	m.mu.Lock()
	phase := m.phase
	m.phase++
	readBoth, sequential, wrong, skip, invalid, override, guessed := m.readBoth, m.sequential, m.wrongAnswer, m.skipAuthority, m.invalidEntity, m.entityOverride, m.guessed
	m.mu.Unlock()
	entity, err := memoryAuthorityProbeEntity(options.Tools)
	if err != nil {
		return err
	}
	if override != "" {
		entity = override
	}
	switch phase {
	case 0:
		calls := []core.ToolCall{}
		if readBoth || skip {
			calls = append(calls, core.ToolCall{ID: "authority-memory", Name: extmemory.RecallCapabilityID, Args: map[string]any{"query": entity, "limit": 1}})
		}
		if !skip && !sequential {
			if invalid {
				entity = "wrong-entity"
			}
			calls = append(calls, core.ToolCall{ID: "authority-current", Name: authorityCurrentID, Args: map[string]any{"entity": entity}})
		}
		if len(calls) == 0 {
			return errors.New("authority probe has no tool call")
		}
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &calls[0], ToolCalls: calls, Usage: &core.TokenUsage{InputTokens: 5, OutputTokens: 1}})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
		return nil
	case 1:
		if sequential {
			call := core.ToolCall{ID: "authority-current", Name: authorityCurrentID, Args: map[string]any{"entity": entity}}
			emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &call, Usage: &core.TokenUsage{InputTokens: 6, OutputTokens: 1}})
			emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
			return nil
		}
		answer, err := memoryAuthorityProbeAnswer(options.Messages, readBoth || skip, invalid, guessed)
		if err != nil {
			return err
		}
		if wrong {
			answer = "WRONG"
		}
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: answer, Usage: &core.TokenUsage{InputTokens: 7, OutputTokens: 2}})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
		return nil
	case 2:
		if !sequential {
			return errors.New("authority probe received an unexpected third round")
		}
		answer, err := memoryAuthorityProbeAnswer(options.Messages, true, false, "")
		if err != nil {
			return err
		}
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: answer, Usage: &core.TokenUsage{InputTokens: 7, OutputTokens: 2}})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
		return nil
	default:
		return errors.New("authority probe received an unexpected extra round")
	}
}

func memoryAuthorityProbeEntity(tools []core.ToolSchema) (string, error) {
	for _, tool := range tools {
		if tool.Name != authorityCurrentID {
			continue
		}
		properties, ok := tool.Parameters["properties"].(map[string]any)
		if !ok {
			break
		}
		entity, ok := properties["entity"].(map[string]any)
		if !ok {
			break
		}
		values, ok := entity["enum"].([]any)
		if !ok || len(values) != 1 {
			break
		}
		value, ok := values[0].(string)
		if ok && strings.TrimSpace(value) != "" {
			return value, nil
		}
		break
	}
	return "", errors.New("authority probe did not receive one entity enum")
}

func memoryAuthorityProbeAnswer(messages []core.ChatMessage, memoryRead, invalid bool, guessed string) (string, error) {
	if memoryRead {
		if _, err := lastToolJSON(messages, "authority-memory"); err != nil {
			return "", err
		}
	}
	if guessed != "" {
		return guessed, nil
	}
	content, err := lastToolContent(messages, "authority-current")
	if err != nil {
		return "", err
	}
	if invalid {
		return "UNKNOWN", nil
	}
	var payload map[string]any
	if json.Unmarshal([]byte(content), &payload) != nil {
		return "", errors.New("authority probe received an invalid authority result")
	}
	available, ok := payload["available"].(bool)
	if !ok {
		return "", errors.New("authority availability is invalid")
	}
	if !available {
		return "UNKNOWN", nil
	}
	code, ok := payload["code"].(string)
	if !ok || strings.TrimSpace(code) == "" {
		return "", errors.New("authority code is invalid")
	}
	return strings.TrimSpace(code), nil
}

func assertMemoryAuthorityArm(f *memoryAuthorityFixture, observed *memoryObservedModel, model *liveModel, events []core.SessionEvent, result core.TurnResult, values memoryAuthorityValues, expected string, wantMemory, diagnostic, unavailable bool) error {
	if strings.TrimSpace(result.Answer) != expected {
		return errors.New("authority final answer did not match current-source contract")
	}
	if model.rounds() < 2 || model.rounds() > 3 || f.assemblerCalls.Load() < int64(model.rounds()) {
		return errors.New("authority runtime budget or journal evidence is invalid")
	}
	inv := liveInvocationEvidenceFromRounds(model.evidenceRounds(), events)
	if inv == nil || inv.ActualAdapterCalls != model.rounds() || inv.PacingCanceledBeforeAdapter != 0 || !inv.ReportedUsageComplete || !inv.UsageProtocolConsistent || !inv.LedgerUsageMatched || !inv.UniqueLedgerInvocationIDs {
		return errors.New("authority invocation usage ledger is incomplete")
	}
	calls, results, _, durable := liveSessionEvidence(events)
	if strings.TrimSpace(durable) != expected || len(calls) == 0 || len(calls) > 2 || len(results) != len(calls) || f.authority.callsN() != 1 || f.journal.completed() != len(calls) {
		return errors.New("authority durable events are incomplete")
	}
	byName := map[string]core.ToolCallData{}
	for _, call := range calls {
		if call.CallID == "" || (call.Name != authorityCurrentID && call.Name != extmemory.RecallCapabilityID) {
			return errors.New("authority tool call is malformed or unexpected")
		}
		if _, exists := byName[call.Name]; exists {
			return errors.New("authority tool was called more than once")
		}
		byName[call.Name] = call
	}
	authorityCall, found := byName[authorityCurrentID]
	if !found {
		return errors.New("authority source was not called")
	}
	byID := map[string]core.ToolResultData{}
	for _, item := range results {
		if _, exists := byID[item.CallID]; exists {
			return errors.New("authority tool result repeated a call id")
		}
		byID[item.CallID] = item
	}
	authority, found := byID[authorityCall.CallID]
	if !found || !authority.OK || !observed.finalContextHasRecallResult(authorityCall.CallID, authority.Content) {
		return errors.New("authority result was not paired into final context")
	}
	available, code, valid := memoryAuthorityResult(authority.Content, values.Entity)
	if len(authority.Content) > 512 || !valid || available == unavailable || (available && code != values.Current) {
		return errors.New("authority result did not match its fixture arm")
	}
	allResultContent := ""
	for _, item := range results {
		allResultContent += item.Content
	}
	if unavailable && (code != "" || memoryObservedContains(observed, values.Current) || strings.Contains(allResultContent, values.Current) || strings.Contains(result.Answer+durable, values.Current)) {
		return errors.New("unavailable authority exposed a current code")
	}
	if wantMemory {
		memoryCall, found := byName[extmemory.RecallCapabilityID]
		if !found {
			return errors.New("diagnostic memory source was not called")
		}
		memory, found := byID[memoryCall.CallID]
		if !found || !memory.OK || !observed.finalContextHasRecallResult(memoryCall.CallID, memory.Content) || !strings.Contains(memory.Content, values.Historical) {
			return errors.New("diagnostic memory result was not observed exactly")
		}
		if diagnostic && values.Historical == values.Current {
			return errors.New("diagnostic conflict was not exposed")
		}
	}
	if !observed.firstTurnObserved() || observed.initialContextContains(values.Historical) || observed.initialContextContains(values.Current) || memoryObservedContains(observed, values.Foreign) || strings.Contains(result.Answer+durable, values.Foreign) || !memoryAuthorityPeerIntact(f, values) {
		return errors.New("authority fixture crossed a scope or initial-context boundary")
	}
	return nil
}

func memoryAuthorityResult(content, expectedEntity string) (available bool, code string, valid bool) {
	var value map[string]any
	if json.Unmarshal([]byte(content), &value) != nil {
		return false, "", false
	}
	available, valid = value["available"].(bool)
	if !valid {
		return false, "", false
	}
	if !available {
		if len(value) != 1 {
			return false, "", false
		}
		_, codePresent := value["code"]
		_, versionPresent := value["version"]
		return false, "", !codePresent && !versionPresent
	}
	if len(value) != 4 {
		return false, "", false
	}
	code, valid = value["code"].(string)
	version, versionOK := value["version"].(string)
	entity, entityOK := value["entity"].(string)
	return available, code, valid && version == authorityVersion && entity == expectedEntity && versionOK && entityOK
}
