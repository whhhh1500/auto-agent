package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode"

	core "github.com/whhhh1500/auto-agent/pkg/core"
	extmemory "github.com/whhhh1500/auto-agent/pkg/extensions/memory"
)

const memoryAuthorityEvidenceSchema = "harness.programmatic.live-memory-authority/v1"

func TestLiveMemoryAuthorityAcceptance(t *testing.T) {
	if os.Getenv("HARNESS_PROGRAMMATIC_LIVE") != "1" && os.Getenv("HARNESS_ACCEPTANCE_LIVE_PROGRAMMATIC") != "1" {
		t.Skip("set HARNESS_PROGRAMMATIC_LIVE=1 to make intentional live model requests")
	}
	adapter, err := newLiveAdapter(context.Background())
	if err != nil {
		t.Fatal("live model configuration is incomplete or invalid")
	}
	modelID := strings.TrimSpace(os.Getenv("HARNESS_LLM_MODEL"))
	if modelID == "" {
		t.Fatal("live model configuration is incomplete or invalid")
	}
	interval, err := liveMinRequestInterval(os.Getenv("HARNESS_PROGRAMMATIC_MIN_REQUEST_INTERVAL"))
	if err != nil {
		t.Fatal("live request interval is invalid")
	}
	values, err := newMemoryAuthorityValues()
	if err != nil {
		t.Fatal("could not create opaque authority values")
	}
	normalPrompt := memoryAuthorityPrompt(false)
	diagnosticPrompt := memoryAuthorityPrompt(true)
	pacer := &liveRequestPacer{interval: interval}
	continueAfterTransport := true
	for _, arm := range []string{"automatic_match", "automatic_conflict", "authority_unavailable", "direct_only_conflict"} {
		if !runLiveMemoryAuthorityArm(t, adapter, modelID, pacer, values, normalPrompt, arm, "ordinary", "memory_authority_v1", "") {
			continueAfterTransport = false
			break
		}
	}
	if continueAfterTransport {
		_ = runLiveMemoryAuthorityArm(t, adapter, modelID, pacer, values, diagnosticPrompt, "read_both_conflict", "diagnostic", "memory_authority_v1", "")
	}
}

// TestLiveMemoryAuthorityDiagnosticV2Acceptance repeats the existing
// diagnostic once and adds only safe recall-shape instrumentation.
func TestLiveMemoryAuthorityDiagnosticV2Acceptance(t *testing.T) {
	if os.Getenv("HARNESS_PROGRAMMATIC_LIVE") != "1" && os.Getenv("HARNESS_ACCEPTANCE_LIVE_PROGRAMMATIC") != "1" {
		t.Skip("set HARNESS_PROGRAMMATIC_LIVE=1 to make intentional live model requests")
	}
	adapter, err := newLiveAdapter(context.Background())
	if err != nil {
		t.Fatal("live model configuration is incomplete or invalid")
	}
	modelID := strings.TrimSpace(os.Getenv("HARNESS_LLM_MODEL"))
	if modelID == "" {
		t.Fatal("live model configuration is incomplete or invalid")
	}
	interval, err := liveMinRequestInterval(os.Getenv("HARNESS_PROGRAMMATIC_MIN_REQUEST_INTERVAL"))
	if err != nil {
		t.Fatal("live request interval is invalid")
	}
	values, err := newMemoryAuthorityValues()
	if err != nil {
		t.Fatal("could not create opaque authority values")
	}
	_ = runLiveMemoryAuthorityDiagnostic(t, adapter, modelID, &liveRequestPacer{interval: interval}, values, "memory_authority_diagnostic_v2", "")
}

// TestLiveMemoryAuthorityDiagnosticV3Acceptance repeats only the existing
// read-both diagnostic against the production memory.recall v3 contract.
func TestLiveMemoryAuthorityDiagnosticV3Acceptance(t *testing.T) {
	if os.Getenv("HARNESS_PROGRAMMATIC_LIVE") != "1" && os.Getenv("HARNESS_ACCEPTANCE_LIVE_PROGRAMMATIC") != "1" {
		t.Skip("set HARNESS_PROGRAMMATIC_LIVE=1 to make intentional live model requests")
	}
	adapter, err := newLiveAdapter(context.Background())
	if err != nil {
		t.Fatal("live model configuration is incomplete or invalid")
	}
	modelID := strings.TrimSpace(os.Getenv("HARNESS_LLM_MODEL"))
	if modelID == "" {
		t.Fatal("live model configuration is incomplete or invalid")
	}
	interval, err := liveMinRequestInterval(os.Getenv("HARNESS_PROGRAMMATIC_MIN_REQUEST_INTERVAL"))
	if err != nil {
		t.Fatal("live request interval is invalid")
	}
	values, err := newMemoryAuthorityValues()
	if err != nil {
		t.Fatal("could not create opaque authority values")
	}
	_ = runLiveMemoryAuthorityDiagnostic(t, adapter, modelID, &liveRequestPacer{interval: interval}, values, "memory_authority_description_v3", "")
}

// TestLiveMemoryAuthorityHeldoutV3Acceptance checks the production v3 recall
// contract on three pre-frozen canonical identifiers. Each case is attempted
// exactly once in this declared order; its opaque values never enter evidence.
func TestLiveMemoryAuthorityHeldoutV3Acceptance(t *testing.T) {
	if os.Getenv("HARNESS_PROGRAMMATIC_LIVE") != "1" && os.Getenv("HARNESS_ACCEPTANCE_LIVE_PROGRAMMATIC") != "1" {
		t.Skip("set HARNESS_PROGRAMMATIC_LIVE=1 to make intentional live model requests")
	}
	adapter, err := newLiveAdapter(context.Background())
	if err != nil {
		t.Fatal("live model configuration is incomplete or invalid")
	}
	modelID := strings.TrimSpace(os.Getenv("HARNESS_LLM_MODEL"))
	if modelID == "" {
		t.Fatal("live model configuration is incomplete or invalid")
	}
	interval, err := liveMinRequestInterval(os.Getenv("HARNESS_PROGRAMMATIC_MIN_REQUEST_INTERVAL"))
	if err != nil {
		t.Fatal("live request interval is invalid")
	}
	cases := []struct {
		label, entity string
		values        memoryAuthorityValues
	}{
		{label: "heldout_1", entity: "release.channel/v2"},
		{label: "heldout_2", entity: "billing-policy:eu_west"},
		{label: "heldout_3", entity: "node-7.alpha"},
	}
	for index := range cases {
		cases[index].values, err = newMemoryAuthorityValuesForEntity(cases[index].entity)
		if err != nil {
			t.Fatal("could not create opaque held-out authority values")
		}
	}
	pacer := &liveRequestPacer{interval: interval}
	for _, heldout := range cases {
		heldout := heldout
		continueAfterTransport := true
		t.Run(heldout.label, func(t *testing.T) {
			continueAfterTransport = runLiveMemoryAuthorityDiagnostic(t, adapter, modelID, pacer, heldout.values, "memory_authority_heldout_v3", heldout.label)
		})
		if !continueAfterTransport {
			break
		}
	}
}

// TestLiveMemoryAuthorityHeldoutShapeV3Acceptance records only the closed
// shape of Luna's two observed held-out query forms. It is a diagnostic: a
// nonexact or empty memory result remains a valid observation when the normal
// authority, answer, durable, scope, journal, and usage chain is complete.
func TestLiveMemoryAuthorityHeldoutShapeV3Acceptance(t *testing.T) {
	if os.Getenv("HARNESS_PROGRAMMATIC_LIVE") != "1" && os.Getenv("HARNESS_ACCEPTANCE_LIVE_PROGRAMMATIC") != "1" {
		t.Skip("set HARNESS_PROGRAMMATIC_LIVE=1 to make intentional live model requests")
	}
	adapter, err := newLiveAdapter(context.Background())
	if err != nil {
		t.Fatal("live model configuration is incomplete or invalid")
	}
	modelID := strings.TrimSpace(os.Getenv("HARNESS_LLM_MODEL"))
	if modelID == "" {
		t.Fatal("live model configuration is incomplete or invalid")
	}
	interval, err := liveMinRequestInterval(os.Getenv("HARNESS_PROGRAMMATIC_MIN_REQUEST_INTERVAL"))
	if err != nil {
		t.Fatal("live request interval is invalid")
	}
	cases := []struct {
		label, entity string
		values        memoryAuthorityValues
	}{
		{label: "heldout_1", entity: "release.channel/v2"},
		{label: "heldout_2", entity: "billing-policy:eu_west"},
	}
	for index := range cases {
		cases[index].values, err = newMemoryAuthorityValuesForEntity(cases[index].entity)
		if err != nil {
			t.Fatal("could not create opaque held-out authority values")
		}
	}
	pacer := &liveRequestPacer{interval: interval}
	for _, heldout := range cases {
		heldout := heldout
		continueAfterTransport := true
		t.Run(heldout.label, func(t *testing.T) {
			continueAfterTransport = runLiveMemoryAuthorityDiagnostic(t, adapter, modelID, pacer, heldout.values, "memory_authority_heldout_shape_v3", heldout.label)
		})
		if !continueAfterTransport {
			break
		}
	}
}

type memoryAuthorityDiagnosticArm struct {
	prompt, arm, caseKind, fixtureVariant, caseLabel string
}

func memoryAuthorityDiagnosticArms(values memoryAuthorityValues, fixtureVariant, caseLabel string) []memoryAuthorityDiagnosticArm {
	return []memoryAuthorityDiagnosticArm{{
		prompt: memoryAuthorityPromptForEntity(values.Entity, true), arm: "read_both_conflict", caseKind: "diagnostic", fixtureVariant: fixtureVariant, caseLabel: caseLabel,
	}}
}

func runLiveMemoryAuthorityDiagnostic(t *testing.T, adapter core.LlmAdapter, modelID string, pacer *liveRequestPacer, values memoryAuthorityValues, fixtureVariant, caseLabel string) bool {
	for _, arm := range memoryAuthorityDiagnosticArms(values, fixtureVariant, caseLabel) {
		if !runLiveMemoryAuthorityArm(t, adapter, modelID, pacer, values, arm.prompt, arm.arm, arm.caseKind, arm.fixtureVariant, arm.caseLabel) {
			return false
		}
	}
	return true
}

func runLiveMemoryAuthorityArm(t *testing.T, adapter core.LlmAdapter, modelID string, pacer *liveRequestPacer, values memoryAuthorityValues, prompt, arm, caseKind, fixtureVariant, caseLabel string) bool {
	stop := false
	passed := t.Run(arm, func(t *testing.T) {
		armValues := values
		if arm == "automatic_match" {
			armValues.Historical = armValues.Current
		}
		expected, wantMemory, diagnostic, unavailable := armValues.Current, false, caseKind == "diagnostic", arm == "authority_unavailable"
		if unavailable {
			expected = "UNKNOWN"
		}
		if diagnostic {
			wantMemory = true
		}
		caseName, runID := "memory_authority_"+arm, "live-memory-authority-"+arm
		if caseLabel != "" {
			caseName, runID = caseLabel, "live-memory-authority-"+caseLabel
		}
		caseDef := liveCase{name: caseName, maxModelRounds: 3, maxToolCalls: 2, prompt: prompt}
		model := newLiveModel(t, adapter, caseDef.name, 3, pacer)
		observed := &memoryObservedModel{inner: model}
		started := time.Now()
		var fixture *memoryAuthorityFixture
		var result core.TurnResult
		var events []core.SessionEvent
		var err error
		accepted := false
		comparisonGroup := "automatic_same_menu"
		if arm == "direct_only_conflict" {
			comparisonGroup = "capability_set_control"
		}
		if diagnostic {
			comparisonGroup = "diagnostic_noncomparable"
		}
		t.Cleanup(func() {
			if fixture != nil && events == nil {
				events = fixture.session.Events()
			}
			base := finalizedLiveEvidence(caseDef, result, modelID, liveProtocol(), liveSourceRevision(), prompt, events, model, time.Since(started), accepted)
			base.Schema, base.FixtureVariant = memoryAuthorityEvidenceSchema, fixtureVariant
			if base.RunID == "" {
				base.RunID = runID
			}
			record := liveMemoryAuthorityEvidence{liveEvidenceRecord: base, MemoryAuthority: memoryAuthorityEvidenceFromRun(fixture, observed, model, events, result, armValues, expected, wantMemory, diagnostic, unavailable, caseKind, comparisonGroup)}
			if err := writeLiveMemoryAuthorityEvidence(os.Getenv("HARNESS_PROGRAMMATIC_EVIDENCE_PATH"), record); err != nil {
				t.Error("could not write live memory authority evidence")
			}
		})
		fixture, err = newMemoryAuthorityFixture(observed, modelID, armValues, arm, "live-"+arm)
		if err != nil {
			t.Fatal("could not construct live authority fixture")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		result, err = fixture.runtime.RunTurn(ctx, fixture.principal, fixture.session, core.TurnInput{RunID: runID, Text: prompt}, nil)
		events = fixture.session.Events()
		if err != nil || result.Status != core.RunCompleted {
			stop = model.stopSubsequentCases()
			t.Fatal("live memory authority case did not complete")
		}
		if memoryAuthorityShapeDiagnosticVariant(fixtureVariant) {
			if err := assertMemoryAuthorityShapeDiagnosticArm(fixture, observed, model, events, result, armValues); err != nil {
				t.Fatal("live memory authority shape diagnostic is incomplete")
			}
		} else if err := assertMemoryAuthorityArm(fixture, observed, model, events, result, armValues, expected, wantMemory, diagnostic, unavailable); err != nil {
			t.Fatal("live memory authority acceptance failed")
		}
		if arm == "direct_only_conflict" {
			calls, _, _, _ := liveSessionEvidence(events)
			for _, call := range calls {
				if call.Name == extmemory.RecallCapabilityID {
					t.Fatal("direct-only authority control called memory")
				}
			}
		}
		if predicate := memoryAuthorityDiagnosticStrictPredicate(fixtureVariant); predicate != nil {
			evidence := memoryAuthorityEvidenceFromRun(fixture, observed, model, events, result, armValues, expected, wantMemory, diagnostic, unavailable, caseKind, comparisonGroup)
			if !predicate(evidence) {
				t.Fatal("live memory authority diagnostic evidence is incomplete")
			}
		}
		accepted = true
	})
	if !passed && stop {
		t.Log("live model HTTP, transport, or responses protocol failure: remaining memory authority arms skipped")
	}
	return passed || !stop
}

func memoryAuthorityDiagnosticStrictPredicate(fixtureVariant string) func(memoryAuthorityEvidence) bool {
	switch fixtureVariant {
	case "memory_authority_diagnostic_v2":
		return memoryAuthorityDiagnosticV2Strict
	case "memory_authority_description_v3", "memory_authority_heldout_v3":
		return memoryAuthorityExactQueryDiagnosticStrict
	default:
		return nil
	}
}

func memoryAuthorityShapeDiagnosticVariant(fixtureVariant string) bool {
	return fixtureVariant == "memory_authority_heldout_shape_v3"
}

type liveMemoryAuthorityEvidence struct {
	liveEvidenceRecord
	MemoryAuthority memoryAuthorityEvidence `json:"memory_authority"`
}
type memoryAuthorityEvidence struct {
	CaseKind                          string                    `json:"case_kind"`
	ComparisonGroup                   string                    `json:"comparison_group"`
	FixtureObserved                   bool                      `json:"fixture_observed"`
	InitialContextObserved            bool                      `json:"initial_context_observed"`
	InitialValuesAbsent               bool                      `json:"initial_values_absent"`
	MemoryCallObserved                bool                      `json:"memory_call_observed"`
	MemoryResultObserved              bool                      `json:"memory_result_observed"`
	MemoryResultOK                    bool                      `json:"memory_result_ok"`
	MemoryContextExactPair            bool                      `json:"memory_context_exact_pair"`
	MemoryQueryObserved               bool                      `json:"memory_query_observed"`
	MemoryQueryEqualsEntity           bool                      `json:"memory_query_equals_entity"`
	MemoryQueryNormalizedEqualsEntity bool                      `json:"memory_query_normalized_equals_entity"`
	MemoryQueryShape                  memoryAuthorityQueryShape `json:"memory_query_shape"`
	MemoryResultEntriesParsed         bool                      `json:"memory_result_entries_parsed"`
	MemoryResultEntryCount            int                       `json:"memory_result_entry_count"`
	MemoryHistoricalValueObserved     bool                      `json:"memory_historical_value_observed"`
	AuthorityCallObserved             bool                      `json:"authority_call_observed"`
	AuthorityResultObserved           bool                      `json:"authority_result_observed"`
	AuthorityResultOK                 bool                      `json:"authority_result_ok"`
	AuthorityContextExactPair         bool                      `json:"authority_context_exact_pair"`
	AuthorityAvailableObserved        bool                      `json:"authority_available_observed"`
	AuthorityAvailable                bool                      `json:"authority_available"`
	AuthorityResultValid              bool                      `json:"authority_result_valid"`
	AuthorityCurrentValueObserved     bool                      `json:"authority_current_value_observed"`
	ContextExactPair                  bool                      `json:"context_exact_pair"`
	ConflictApplicable                bool                      `json:"conflict_applicable"`
	ConflictExposed                   bool                      `json:"conflict_exposed"`
	ConflictExposedAndResolved        bool                      `json:"conflict_exposed_and_resolved"`
	ForeignAbsentContext              bool                      `json:"foreign_absent_context"`
	ForeignAbsentToolResults          bool                      `json:"foreign_absent_tool_results"`
	ForeignAbsentAnswer               bool                      `json:"foreign_absent_answer"`
	UnavailableCurrentAbsent          bool                      `json:"unavailable_current_absent"`
	AnswerMatchesArm                  bool                      `json:"answer_matches_arm"`
	DurableAnswerMatchesArm           bool                      `json:"durable_answer_matches_arm"`
	PeerStoreIntact                   bool                      `json:"peer_store_intact"`
	JournalMatchesCalls               bool                      `json:"journal_matches_calls"`
	ModelRounds                       int                       `json:"model_rounds"`
	AssemblerCalls                    int64                     `json:"assembler_calls"`
	ActualRoute                       string                    `json:"actual_route"`
}

func memoryAuthorityEvidenceFromRun(f *memoryAuthorityFixture, observed *memoryObservedModel, model *liveModel, events []core.SessionEvent, result core.TurnResult, values memoryAuthorityValues, expected string, wantMemory, diagnostic, unavailable bool, caseKind, comparisonGroup string) memoryAuthorityEvidence {
	e := memoryAuthorityEvidence{CaseKind: caseKind, ComparisonGroup: comparisonGroup, MemoryQueryShape: memoryQueryShapeUnobserved, ConflictApplicable: values.Historical != values.Current && !unavailable && (comparisonGroup == "automatic_same_menu" || comparisonGroup == "diagnostic_noncomparable")}
	if f == nil {
		return e
	}
	e.FixtureObserved, e.AssemblerCalls = true, f.assemblerCalls.Load()
	if model != nil {
		e.ModelRounds = model.rounds()
	}
	calls, results, _, durable := liveSessionEvidence(events)
	e.InitialContextObserved = observed != nil && observed.firstTurnObserved()
	e.InitialValuesAbsent = e.InitialContextObserved && !observed.initialContextContains(values.Historical) && !observed.initialContextContains(values.Current) && !observed.initialContextContains(values.Foreign)
	e.ForeignAbsentContext = e.InitialContextObserved && !memoryObservedContains(observed, values.Foreign)
	e.ForeignAbsentAnswer = !strings.Contains(result.Answer+durable, values.Foreign)
	e.AnswerMatchesArm, e.DurableAnswerMatchesArm = strings.TrimSpace(result.Answer) == expected, strings.TrimSpace(durable) == expected
	byID := map[string]core.ToolResultData{}
	byName := map[string]core.ToolCallData{}
	for _, call := range calls {
		if _, duplicate := byName[call.Name]; duplicate {
			e.ActualRoute = "unrecognized"
		} else {
			byName[call.Name] = call
		}
	}
	for _, item := range results {
		byID[item.CallID] = item
	}
	if memoryCall, ok := byName[extmemory.RecallCapabilityID]; ok {
		e.MemoryCallObserved = true
		if query, observed := memoryCall.Args["query"].(string); observed {
			e.MemoryQueryObserved = true
			e.MemoryQueryEqualsEntity = query == values.Entity
			e.MemoryQueryNormalizedEqualsEntity = strings.TrimSpace(query) == values.Entity
			e.MemoryQueryShape = classifyMemoryAuthorityQueryShape(query, values.Entity, true)
		}
		if memoryResult, found := byID[memoryCall.CallID]; found {
			e.MemoryResultObserved, e.MemoryResultOK = true, memoryResult.OK
			e.MemoryContextExactPair = observed != nil && observed.finalContextHasRecallResult(memoryCall.CallID, memoryResult.Content)
			e.ForeignAbsentToolResults = !strings.Contains(memoryResult.Content, values.Foreign)
			entries, envelopeParsed, entriesParsed := memoryAuthorityMemoryEntries(memoryResult.Content)
			e.MemoryResultEntriesParsed, e.MemoryResultEntryCount = envelopeParsed && entriesParsed, len(entries)
			for _, entry := range entries {
				if entry.Entity == values.Entity && entry.Code == values.Historical {
					e.MemoryHistoricalValueObserved = true
				}
			}
		}
	}
	if authorityCall, ok := byName[authorityCurrentID]; ok {
		e.AuthorityCallObserved = true
		if authorityResult, found := byID[authorityCall.CallID]; found {
			e.AuthorityResultObserved, e.AuthorityResultOK = true, authorityResult.OK
			parsed := parseMemoryAuthorityResult(authorityResult.Content, values.Entity)
			e.AuthorityAvailableObserved, e.AuthorityAvailable, e.AuthorityResultValid = true, parsed.Available, parsed.Valid
			e.AuthorityCurrentValueObserved = parsed.Valid && parsed.Available && parsed.Entity == values.Entity && parsed.Version == authorityVersion && parsed.Code == values.Current
			e.AuthorityContextExactPair = observed != nil && observed.finalContextHasRecallResult(authorityCall.CallID, authorityResult.Content)
			if !e.MemoryCallObserved {
				e.ForeignAbsentToolResults = !strings.Contains(authorityResult.Content, values.Foreign)
			}
		}
	}
	if e.MemoryCallObserved && e.AuthorityCallObserved {
		memoryResult, memoryOK := byID[byName[extmemory.RecallCapabilityID].CallID]
		authorityResult, authorityOK := byID[byName[authorityCurrentID].CallID]
		e.ConflictExposed = memoryOK && authorityOK && memoryResult.OK && authorityResult.OK && e.MemoryContextExactPair && e.AuthorityContextExactPair && e.MemoryResultEntriesParsed && e.MemoryResultEntryCount == 1 && e.MemoryHistoricalValueObserved && e.AuthorityCurrentValueObserved && values.Historical != values.Current
	}
	e.ContextExactPair = e.AuthorityCallObserved && e.AuthorityContextExactPair && (!e.MemoryCallObserved || e.MemoryContextExactPair)
	all := ""
	for _, item := range results {
		all += item.Content
	}
	e.ForeignAbsentToolResults = e.ForeignAbsentToolResults && !strings.Contains(all, values.Foreign)
	if unavailable {
		e.UnavailableCurrentAbsent = e.InitialContextObserved && !memoryObservedContains(observed, values.Current) && !strings.Contains(all+result.Answer+durable, values.Current)
	}
	switch {
	case e.MemoryCallObserved && e.AuthorityCallObserved:
		e.ActualRoute = "memory_and_authority"
	case e.AuthorityCallObserved:
		e.ActualRoute = "authority_only"
	case e.MemoryCallObserved:
		e.ActualRoute = "memory_only"
	default:
		e.ActualRoute = "none"
	}
	e.PeerStoreIntact, e.JournalMatchesCalls = memoryAuthorityPeerIntact(f, values), f.journal != nil && f.journal.completed() == len(calls)
	e.ConflictExposedAndResolved = e.ConflictExposed && e.AnswerMatchesArm && e.DurableAnswerMatchesArm && e.AuthorityAvailable && e.AuthorityResultValid
	return e
}

// memoryAuthorityDiagnosticV2Strict is deliberately a predicate over the
// already safe evidence view, so acceptance and Cleanup cannot diverge.
func memoryAuthorityDiagnosticV2Strict(e memoryAuthorityEvidence) bool {
	return e.MemoryResultEntriesParsed && e.MemoryResultEntryCount == 1 && e.MemoryHistoricalValueObserved && e.AuthorityCurrentValueObserved && e.ConflictExposed && e.ConflictExposedAndResolved
}

// memoryAuthorityExactQueryDiagnosticStrict adds the v3 contract without
// changing the historical v2 structural predicate or its recorded meaning.
func memoryAuthorityExactQueryDiagnosticStrict(e memoryAuthorityEvidence) bool {
	return memoryAuthorityDiagnosticV2Strict(e) && e.MemoryQueryObserved && e.MemoryQueryEqualsEntity && e.MemoryQueryNormalizedEqualsEntity
}

type memoryAuthorityQueryShape string

const (
	memoryQueryShapeExact                    memoryAuthorityQueryShape = "exact"
	memoryQueryShapeSeparatorNormalizedEqual memoryAuthorityQueryShape = "separator_normalized_equal"
	memoryQueryShapeQueryTokensSubset        memoryAuthorityQueryShape = "query_tokens_contiguous_subset"
	memoryQueryShapeEntityTokensSubset       memoryAuthorityQueryShape = "entity_tokens_contiguous_subset"
	memoryQueryShapeTokenSetEqual            memoryAuthorityQueryShape = "token_set_equal"
	memoryQueryShapeOther                    memoryAuthorityQueryShape = "other"
	memoryQueryShapeUnobserved               memoryAuthorityQueryShape = "unobserved"
)

// classifyMemoryAuthorityQueryShape returns only a closed label. It never
// escapes its raw query or entity inputs into live evidence.
func classifyMemoryAuthorityQueryShape(query, entity string, observed bool) memoryAuthorityQueryShape {
	if !observed {
		return memoryQueryShapeUnobserved
	}
	if query == entity {
		return memoryQueryShapeExact
	}
	queryTokens, entityTokens := memoryAuthorityQueryTokens(query), memoryAuthorityQueryTokens(entity)
	if len(queryTokens) == 0 || len(entityTokens) == 0 {
		return memoryQueryShapeOther
	}
	if sameMemoryAuthorityTokens(queryTokens, entityTokens) {
		return memoryQueryShapeSeparatorNormalizedEqual
	}
	if memoryAuthorityContiguousTokens(queryTokens, entityTokens) {
		return memoryQueryShapeQueryTokensSubset
	}
	if memoryAuthorityContiguousTokens(entityTokens, queryTokens) {
		return memoryQueryShapeEntityTokensSubset
	}
	if sameMemoryAuthorityTokenSet(queryTokens, entityTokens) {
		return memoryQueryShapeTokenSetEqual
	}
	return memoryQueryShapeOther
}

func memoryAuthorityQueryTokens(value string) []string {
	tokens, current := []string{}, []rune{}
	flush := func() {
		if len(current) > 0 {
			tokens = append(tokens, string(current))
			current = nil
		}
	}
	for _, character := range value {
		if unicode.IsLetter(character) || unicode.IsNumber(character) {
			current = append(current, unicode.ToLower(character))
			continue
		}
		flush()
	}
	flush()
	return tokens
}

func sameMemoryAuthorityTokens(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func memoryAuthorityContiguousTokens(needle, haystack []string) bool {
	if len(needle) == 0 || len(needle) > len(haystack) {
		return false
	}
	for start := 0; start+len(needle) <= len(haystack); start++ {
		if sameMemoryAuthorityTokens(needle, haystack[start:start+len(needle)]) {
			return true
		}
	}
	return false
}

func sameMemoryAuthorityTokenSet(left, right []string) bool {
	leftSet, rightSet := map[string]bool{}, map[string]bool{}
	for _, value := range left {
		leftSet[value] = true
	}
	for _, value := range right {
		rightSet[value] = true
	}
	if len(leftSet) != len(rightSet) {
		return false
	}
	for value := range leftSet {
		if !rightSet[value] {
			return false
		}
	}
	return true
}

func validMemoryAuthorityQueryShape(shape memoryAuthorityQueryShape) bool {
	switch shape {
	case memoryQueryShapeExact, memoryQueryShapeSeparatorNormalizedEqual, memoryQueryShapeQueryTokensSubset, memoryQueryShapeEntityTokensSubset, memoryQueryShapeTokenSetEqual, memoryQueryShapeOther, memoryQueryShapeUnobserved:
		return true
	default:
		return false
	}
}

func validObservedMemoryAuthorityQueryShape(shape memoryAuthorityQueryShape) bool {
	return shape != memoryQueryShapeUnobserved && validMemoryAuthorityQueryShape(shape)
}

// memoryAuthorityShapeDiagnosticComplete checks only that the diagnostic was
// safely observed. It intentionally does not require exact query matching,
// a non-empty memory result, or a resolved conflict.
func memoryAuthorityShapeDiagnosticComplete(e memoryAuthorityEvidence) bool {
	return e.MemoryQueryObserved && validObservedMemoryAuthorityQueryShape(e.MemoryQueryShape) &&
		e.FixtureObserved && e.InitialContextObserved && e.InitialValuesAbsent &&
		e.MemoryCallObserved && e.MemoryResultObserved && e.MemoryResultOK && e.MemoryContextExactPair && e.MemoryResultEntriesParsed && e.MemoryResultEntryCount >= 0 && e.MemoryResultEntryCount <= 1 &&
		e.AuthorityCallObserved && e.AuthorityResultObserved && e.AuthorityResultOK && e.AuthorityContextExactPair &&
		e.AuthorityAvailable && e.AuthorityResultValid && e.AuthorityCurrentValueObserved &&
		e.ContextExactPair && e.ForeignAbsentContext && e.ForeignAbsentToolResults && e.ForeignAbsentAnswer &&
		e.AnswerMatchesArm && e.DurableAnswerMatchesArm && e.PeerStoreIntact && e.JournalMatchesCalls &&
		e.ModelRounds >= 2 && e.ModelRounds <= 3 && e.AssemblerCalls >= int64(e.ModelRounds)
}

func assertMemoryAuthorityShapeDiagnosticArm(f *memoryAuthorityFixture, observed *memoryObservedModel, model *liveModel, events []core.SessionEvent, result core.TurnResult, values memoryAuthorityValues) error {
	if f == nil || model == nil {
		return errors.New("authority shape diagnostic fixture is incomplete")
	}
	invocation := liveInvocationEvidenceFromRounds(model.evidenceRounds(), events)
	if invocation == nil || invocation.ActualAdapterCalls != model.rounds() || invocation.PacingCanceledBeforeAdapter != 0 || !invocation.ReportedUsageComplete || !invocation.UsageProtocolConsistent || !invocation.LedgerUsageMatched || !invocation.UniqueLedgerInvocationIDs {
		return errors.New("authority shape diagnostic invocation usage is incomplete")
	}
	calls, results, _, _ := liveSessionEvidence(events)
	if len(calls) != 2 || len(results) != 2 || f.authority.callsN() != 1 {
		return errors.New("authority shape diagnostic did not complete both tools exactly once")
	}
	seenCalls, seenResults := map[string]bool{}, map[string]bool{}
	for _, call := range calls {
		if call.CallID == "" || (call.Name != extmemory.RecallCapabilityID && call.Name != authorityCurrentID) || seenCalls[call.Name] {
			return errors.New("authority shape diagnostic tool calls are malformed")
		}
		seenCalls[call.Name] = true
	}
	for _, toolResult := range results {
		if toolResult.CallID == "" || seenResults[toolResult.CallID] {
			return errors.New("authority shape diagnostic tool results are malformed")
		}
		seenResults[toolResult.CallID] = true
	}
	evidence := memoryAuthorityEvidenceFromRun(f, observed, model, events, result, values, values.Current, true, true, false, "diagnostic", "diagnostic_noncomparable")
	if !memoryAuthorityShapeDiagnosticComplete(evidence) {
		return errors.New("authority shape diagnostic evidence is incomplete")
	}
	return nil
}

type memoryAuthorityEntry struct{ Entity, Code string }

// memoryAuthorityMemoryEntries accepts only the standard closed {"entries":...}
// envelope and this fixture's closed entry grammar. It keeps values in memory.
func memoryAuthorityMemoryEntries(content string) ([]memoryAuthorityEntry, bool, bool) {
	var envelope map[string]json.RawMessage
	if json.Unmarshal([]byte(content), &envelope) != nil || len(envelope) != 1 || envelope["entries"] == nil {
		return nil, false, false
	}
	contents, parsed := memoryValidityEntryContents(content)
	if !parsed {
		return nil, false, false
	}
	entries := make([]memoryAuthorityEntry, 0, len(contents))
	for _, content := range contents {
		parts := strings.Split(content, "; ")
		if len(parts) != 2 || !strings.HasPrefix(parts[0], "entity=") || !strings.HasPrefix(parts[1], "code=") {
			return nil, true, false
		}
		entity, code := strings.TrimPrefix(parts[0], "entity="), strings.TrimPrefix(parts[1], "code=")
		if entity == "" || code == "" || strings.ContainsAny(entity, ";\n\r") || strings.ContainsAny(code, ";\n\r") {
			return nil, true, false
		}
		entries = append(entries, memoryAuthorityEntry{Entity: entity, Code: code})
	}
	return entries, true, true
}

type parsedMemoryAuthorityResult struct {
	Available             bool
	Entity, Version, Code string
	Valid                 bool
}

func parseMemoryAuthorityResult(content, expectedEntity string) parsedMemoryAuthorityResult {
	var value map[string]json.RawMessage
	if json.Unmarshal([]byte(content), &value) != nil {
		return parsedMemoryAuthorityResult{}
	}
	availableRaw, ok := value["available"]
	if !ok {
		return parsedMemoryAuthorityResult{}
	}
	var available bool
	if json.Unmarshal(availableRaw, &available) != nil {
		return parsedMemoryAuthorityResult{}
	}
	if !available {
		return parsedMemoryAuthorityResult{Available: false, Valid: len(value) == 1}
	}
	if len(value) != 4 {
		return parsedMemoryAuthorityResult{}
	}
	var entity, version, code string
	if json.Unmarshal(value["entity"], &entity) != nil || json.Unmarshal(value["version"], &version) != nil || json.Unmarshal(value["code"], &code) != nil || entity == "" || version == "" || code == "" {
		return parsedMemoryAuthorityResult{}
	}
	return parsedMemoryAuthorityResult{Available: true, Entity: entity, Version: version, Code: code, Valid: entity == expectedEntity && version == authorityVersion}
}

func writeLiveMemoryAuthorityEvidence(directory string, record liveMemoryAuthorityEvidence) error {
	if strings.TrimSpace(directory) == "" {
		return nil
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return errors.New("live memory authority evidence directory is unavailable")
	}
	payload, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return errors.New("live memory authority evidence encoding failed")
	}
	identity := sha256.Sum256([]byte(record.RunID + "\x00" + record.CaseID + "\x00" + record.PromptSHA256 + "\x00" + time.Now().UTC().Format(time.RFC3339Nano)))
	return writeLiveEvidenceFile(filepath.Join(directory, "programmatic-live-memory-authority-"+hex.EncodeToString(identity[:12])+".json"), append(payload, '\n'))
}

func TestMemoryAuthorityEvidenceRedactsOpaqueValues(t *testing.T) {
	values, err := newMemoryAuthorityValues()
	if err != nil {
		t.Fatal("could not create opaque authority values")
	}
	probe := &memoryAuthorityProbe{readBoth: true}
	model := newLiveModel(t, probe, "offline-memory-authority-evidence", 3, nil)
	observed := &memoryObservedModel{inner: model}
	fixture, err := newMemoryAuthorityFixture(observed, "offline-memory-authority", values, "read_both_conflict", "offline-evidence")
	if err != nil {
		t.Fatal("could not construct authority fixture")
	}
	prompt := memoryAuthorityPrompt(true)
	result, err := fixture.runtime.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{RunID: "offline-memory-authority-evidence", Text: prompt}, nil)
	if err != nil || result.Status != core.RunCompleted {
		t.Fatal("authority evidence fixture did not complete")
	}
	base := finalizedLiveEvidence(liveCase{name: "memory_authority_evidence", maxModelRounds: 3, maxToolCalls: 2, prompt: prompt}, result, "offline-memory-authority", "test", "test-revision", prompt, fixture.session.Events(), model, 0, true)
	base.Schema, base.FixtureVariant = memoryAuthorityEvidenceSchema, "memory_authority_v1"
	record := liveMemoryAuthorityEvidence{liveEvidenceRecord: base, MemoryAuthority: memoryAuthorityEvidenceFromRun(fixture, observed, model, fixture.session.Events(), result, values, values.Current, true, true, false, "diagnostic", "diagnostic_noncomparable")}
	dir := t.TempDir()
	if err := writeLiveMemoryAuthorityEvidence(dir, record); err != nil {
		t.Fatal("could not write authority evidence")
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 {
		t.Fatal("authority evidence did not create one record")
	}
	payload, err := os.ReadFile(filepath.Join(dir, entries[0].Name()))
	if err != nil || strings.Contains(string(payload), values.Historical) || strings.Contains(string(payload), values.Current) || strings.Contains(string(payload), values.Foreign) {
		t.Fatal("authority evidence was malformed or exposed opaque values")
	}
	var persisted liveMemoryAuthorityEvidence
	if json.Unmarshal(payload, &persisted) != nil || persisted.Schema != memoryAuthorityEvidenceSchema || persisted.MemoryAuthority.CaseKind != "diagnostic" || !persisted.InvocationEvidence.AdapterCallsObserved || !persisted.MemoryAuthority.ConflictExposed || !persisted.MemoryAuthority.MemoryQueryObserved || !persisted.MemoryAuthority.MemoryQueryEqualsEntity || !persisted.MemoryAuthority.MemoryQueryNormalizedEqualsEntity || persisted.MemoryAuthority.MemoryQueryShape != memoryQueryShapeExact || !persisted.MemoryAuthority.MemoryResultEntriesParsed || persisted.MemoryAuthority.MemoryResultEntryCount != 1 || !persisted.MemoryAuthority.MemoryHistoricalValueObserved || !persisted.MemoryAuthority.AuthorityCurrentValueObserved {
		t.Fatal("authority evidence did not retain required predicates")
	}
}

func TestMemoryAuthorityEvidenceV2MarksUnreadMemoryUnavailable(t *testing.T) {
	values, err := newMemoryAuthorityValues()
	if err != nil {
		t.Fatal("could not create opaque authority values")
	}
	probe := &memoryAuthorityProbe{}
	model := newLiveModel(t, probe, "offline-memory-authority-v2-negative", 3, nil)
	observed := &memoryObservedModel{inner: model}
	fixture, err := newMemoryAuthorityFixture(observed, "offline-memory-authority", values, "automatic_conflict", "offline-v2-negative")
	if err != nil {
		t.Fatal("could not construct authority fixture")
	}
	result, err := fixture.runtime.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{RunID: "offline-memory-authority-v2-negative", Text: memoryAuthorityPrompt(false)}, nil)
	if err != nil || result.Status != core.RunCompleted {
		t.Fatal("authority negative fixture did not complete")
	}
	e := memoryAuthorityEvidenceFromRun(fixture, observed, model, fixture.session.Events(), result, values, values.Current, false, false, false, "ordinary", "automatic_same_menu")
	if e.MemoryCallObserved || e.MemoryQueryObserved || e.MemoryResultEntriesParsed || e.MemoryHistoricalValueObserved || !e.AuthorityCurrentValueObserved {
		t.Fatal("authority v2 evidence did not distinguish an unread memory route")
	}
}

func TestMemoryAuthorityEvidenceV2RejectsSubstringValues(t *testing.T) {
	values, err := newMemoryAuthorityValues()
	if err != nil {
		t.Fatal("could not create opaque authority values")
	}
	memoryContent := `{"entries":[{"content":"entity=prefix-` + authorityEntity + `; code=prefix-` + values.Historical + `"}]}`
	entries, envelope, valid := memoryAuthorityMemoryEntries(memoryContent)
	if !envelope || !valid || len(entries) != 1 || (entries[0].Entity == authorityEntity && entries[0].Code == values.Historical) {
		t.Fatal("memory authority parser accepted a substring as an exact fixture entry")
	}
	authorityContent := `{"available":true,"entity":"` + authorityEntity + `","version":"` + authorityVersion + `","code":"prefix-` + values.Current + `"}`
	parsed := parseMemoryAuthorityResult(authorityContent, authorityEntity)
	if !parsed.Valid || (parsed.Available && parsed.Entity == authorityEntity && parsed.Version == authorityVersion && parsed.Code == values.Current) {
		t.Fatal("authority parser accepted a substring current code")
	}
}

func TestMemoryAuthorityDiagnosticV2StrictRejectsIncompleteEvidence(t *testing.T) {
	complete := memoryAuthorityEvidence{MemoryResultEntriesParsed: true, MemoryResultEntryCount: 1, MemoryHistoricalValueObserved: true, AuthorityCurrentValueObserved: true, ConflictExposed: true, ConflictExposedAndResolved: true}
	if !memoryAuthorityDiagnosticV2Strict(complete) {
		t.Fatal("complete diagnostic v2 evidence was rejected")
	}
	complete.MemoryResultEntryCount = 0
	if memoryAuthorityDiagnosticV2Strict(complete) {
		t.Fatal("diagnostic v2 accepted missing parsed memory entry")
	}
	complete.MemoryResultEntryCount = 1
	if !memoryAuthorityDiagnosticV2Strict(complete) || memoryAuthorityExactQueryDiagnosticStrict(complete) {
		t.Fatal("exact-query gate changed v2 semantics or accepted an unobserved query")
	}
	complete.MemoryQueryObserved, complete.MemoryQueryEqualsEntity, complete.MemoryQueryNormalizedEqualsEntity = true, false, false
	if !memoryAuthorityDiagnosticV2Strict(complete) || memoryAuthorityExactQueryDiagnosticStrict(complete) {
		t.Fatal("exact-query gate accepted a structurally complete nonexact query")
	}
}
