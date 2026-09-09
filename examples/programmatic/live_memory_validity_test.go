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

	core "github.com/whhhh1500/auto-agent/pkg/core"
	extmemory "github.com/whhhh1500/auto-agent/pkg/extensions/memory"
)

const memoryValidityEvidenceSchema = "harness.programmatic.live-memory-validity/v1"

// TestLiveMemoryValidityAcceptance is a development-contract experiment. It
// makes intentional model requests only under the existing live-test gate;
// production MemoryEntry and Store types retain no TTL semantics.
func TestLiveMemoryValidityAcceptance(t *testing.T) {
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
	values, err := newMemoryValidityValues()
	if err != nil {
		t.Fatal("could not create opaque memory validity values")
	}
	prompt := memoryValidityPrompt(values.Lookup)
	if memoryValidityPromptLeaks(prompt, values) {
		t.Fatal("memory validity fixture value leaked into prompt")
	}
	pacer := &liveRequestPacer{interval: interval}

	for _, arm := range []struct {
		name        string
		expected    string
		wantEntries int
		toolOK      bool
	}{
		{name: "fresh", expected: values.Target, wantEntries: 1, toolOK: true},
		{name: "expired", expected: "UNKNOWN", wantEntries: 0, toolOK: true},
		{name: "backend_error", expected: "UNKNOWN", toolOK: false},
	} {
		stop := false
		passed := t.Run(arm.name, func(t *testing.T) {
			caseDef := liveCase{name: "memory_validity_" + arm.name, maxModelRounds: 2, maxToolCalls: 1, prompt: prompt}
			model := newLiveModel(t, adapter, caseDef.name, 2, pacer)
			observed := &memoryObservedModel{inner: model}
			runID := "live-memory-validity-" + arm.name
			started := time.Now()
			var fixture *memoryValidityFixture
			var result core.TurnResult
			var events []core.SessionEvent
			acceptancePassed := false
			t.Cleanup(func() {
				if fixture != nil && events == nil {
					events = fixture.session.Events()
				}
				base := finalizedLiveEvidence(caseDef, result, modelID, liveProtocol(), liveSourceRevision(), prompt, events, model, time.Since(started), acceptancePassed)
				base.Schema = memoryValidityEvidenceSchema
				base.FixtureVariant = "memory_validity_v1"
				if base.RunID == "" {
					base.RunID = runID
				}
				record := liveMemoryValidityEvidence{liveEvidenceRecord: base, MemoryValidity: memoryValidityEvidenceFromRun(fixture, observed, model, events, result, values, arm.expected, arm.wantEntries, arm.toolOK)}
				if err := writeLiveMemoryValidityEvidence(os.Getenv("HARNESS_PROGRAMMATIC_EVIDENCE_PATH"), record); err != nil {
					t.Error("could not write live memory validity evidence")
				}
			})

			fixture, err = newMemoryValidityFixture(observed, modelID, values, arm.name, "live-"+arm.name)
			if err != nil {
				t.Fatal("could not construct live memory validity fixture")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			result, err = fixture.runtime.RunTurn(ctx, fixture.principal, fixture.session, core.TurnInput{RunID: runID, Text: prompt}, nil)
			events = fixture.session.Events()
			if err != nil || result.Status != core.RunCompleted {
				stop = model.stopSubsequentCases()
				t.Fatal("live memory validity case did not complete")
			}
			if err := assertMemoryValidityArm(fixture, observed, model, events, result, values, arm.expected, arm.wantEntries, arm.toolOK); err != nil {
				t.Fatal("live memory validity acceptance failed")
			}
			acceptancePassed = true
		})
		if !passed && stop {
			t.Log("live model HTTP, transport, or responses protocol failure: remaining memory validity arms skipped")
			break
		}
	}
}

// liveMemoryValidityEvidence embeds the ordinary record, so live invocation
// accounting remains in its established field while this experiment adds only
// closed predicates and counts.
type liveMemoryValidityEvidence struct {
	liveEvidenceRecord
	MemoryValidity memoryValidityEvidence `json:"memory_validity"`
}

// memoryValidityEvidence never retains fixture values, model text, tool
// arguments, tool content, or per-invocation identifiers.
type memoryValidityEvidence struct {
	FixtureObserved                   bool  `json:"fixture_observed"`
	StoreCallObserved                 bool  `json:"store_call_observed"`
	StoreCallMatchesScopeAndBudget    bool  `json:"store_call_matches_scope_and_budget"`
	RecallCalledOnce                  bool  `json:"recall_called_once"`
	RecallResultObserved              bool  `json:"recall_result_observed"`
	RecallSucceeded                   bool  `json:"recall_succeeded"`
	RecallFailureCodeMatched          bool  `json:"recall_failure_code_matched"`
	RecallEntriesParsed               bool  `json:"recall_entries_parsed"`
	RecallEntryCount                  int   `json:"recall_entry_count"`
	InitialContextObserved            bool  `json:"initial_context_observed"`
	InitialValuesAbsent               bool  `json:"initial_values_absent"`
	FinalContextExactPair             bool  `json:"final_context_exact_pair"`
	UnavailableValuesAbsentToolResult bool  `json:"unavailable_values_absent_tool_result"`
	UnavailableValuesAbsentContext    bool  `json:"unavailable_values_absent_context"`
	UnavailableValuesAbsentAnswer     bool  `json:"unavailable_values_absent_answer"`
	TargetAbsenceApplicable           bool  `json:"target_absence_applicable"`
	TargetAbsentWhenUnavailable       bool  `json:"target_absent_when_unavailable"`
	AnswerMatchesArm                  bool  `json:"answer_matches_arm"`
	DurableAnswerMatchesArm           bool  `json:"durable_answer_matches_arm"`
	RawCurrentObserved                bool  `json:"raw_current_observed"`
	NewestRawMatchUnavailable         bool  `json:"newest_raw_match_unavailable"`
	RawCurrentEntriesIntact           bool  `json:"raw_current_entries_intact"`
	PeerStoreIntact                   bool  `json:"peer_store_intact"`
	JournalCompletedOnce              bool  `json:"journal_completed_once"`
	ModelRounds                       int   `json:"model_rounds"`
	AssemblerCalls                    int64 `json:"assembler_calls"`
}

func memoryValidityEvidenceFromRun(fixture *memoryValidityFixture, observed *memoryObservedModel, model *liveModel, events []core.SessionEvent, result core.TurnResult, values memoryValidityValues, expected string, wantEntries int, toolOK bool) memoryValidityEvidence {
	evidence := memoryValidityEvidence{}
	if fixture == nil {
		return evidence
	}
	evidence.FixtureObserved = true
	evidence.AssemblerCalls = fixture.assemblerCalls.Load()
	evidence.JournalCompletedOnce = fixture.journal != nil && fixture.journal.completed() == 1
	if model != nil {
		evidence.ModelRounds = model.rounds()
	}

	calls, results, _, durableAnswer := liveSessionEvidence(events)
	evidence.InitialContextObserved = observed != nil && observed.firstTurnObserved()
	evidence.InitialValuesAbsent = evidence.InitialContextObserved && !memoryValidityObservedInitialLeak(observed, values)
	evidence.UnavailableValuesAbsentContext = evidence.InitialContextObserved && !memoryObservedContains(observed, values.Expired) && !memoryObservedContains(observed, values.MissingValidity) && !memoryObservedContains(observed, values.Foreign) && !memoryObservedContains(observed, values.Unrelated)
	evidence.AnswerMatchesArm = strings.TrimSpace(result.Answer) == expected
	evidence.DurableAnswerMatchesArm = strings.TrimSpace(durableAnswer) == expected
	evidence.TargetAbsenceApplicable = wantEntries == 0 || !toolOK
	if evidence.TargetAbsenceApplicable {
		evidence.TargetAbsentWhenUnavailable = evidence.InitialContextObserved && !memoryObservedContains(observed, values.Target) && !memoryValidityContainsAny(result.Answer+durableAnswer, values.Target)
	}

	storeCalls := fixture.store.calls()
	evidence.StoreCallObserved = len(storeCalls) == 1
	if evidence.StoreCallObserved {
		call := storeCalls[0]
		evidence.StoreCallMatchesScopeAndBudget = call.scope == fixture.principal.Scope.String() && call.query == values.Lookup && call.limit == 1
	}

	if len(calls) == 1 && calls[0].Name == extmemory.RecallCapabilityID && calls[0].CallID != "" {
		call := calls[0]
		evidence.RecallCalledOnce = true
		for _, toolResult := range results {
			if toolResult.CallID != call.CallID {
				continue
			}
			if evidence.RecallResultObserved {
				evidence.RecallResultObserved = false
				break
			}
			evidence.RecallResultObserved = true
			evidence.RecallSucceeded = toolResult.OK
			evidence.RecallFailureCodeMatched = !toolResult.OK && toolResult.Content == "memory store operation failed" && toolResult.Metadata["code"] == "memory_recall_failed"
			if toolResult.OK {
				entries, parsed := memoryValidityEntryContents(toolResult.Content)
				evidence.RecallEntriesParsed = parsed
				if parsed {
					evidence.RecallEntryCount = len(entries)
				}
			}
			evidence.FinalContextExactPair = observed != nil && observed.finalContextHasRecallResult(call.CallID, toolResult.Content)
			evidence.UnavailableValuesAbsentToolResult = !memoryValidityContainsAny(toolResult.Content, values.Expired, values.MissingValidity, values.Foreign, values.Unrelated)
			evidence.UnavailableValuesAbsentAnswer = !memoryValidityContainsAny(result.Answer+durableAnswer, values.Expired, values.MissingValidity, values.Foreign, values.Unrelated)
			if evidence.TargetAbsenceApplicable {
				evidence.TargetAbsentWhenUnavailable = evidence.TargetAbsentWhenUnavailable && !memoryValidityContainsAny(toolResult.Content, values.Target)
			}
		}
	}
	evidence.RawCurrentObserved, evidence.NewestRawMatchUnavailable, evidence.RawCurrentEntriesIntact = memoryValidityRawCurrentStatus(fixture, values)
	evidence.PeerStoreIntact = memoryValidityRawPeerIntact(fixture, values)
	return evidence
}

func writeLiveMemoryValidityEvidence(directory string, record liveMemoryValidityEvidence) error {
	if strings.TrimSpace(directory) == "" {
		return nil
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return errors.New("live memory validity evidence directory is unavailable")
	}
	payload, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return errors.New("live memory validity evidence encoding failed")
	}
	identity := sha256.Sum256([]byte(record.RunID + "\x00" + record.CaseID + "\x00" + record.PromptSHA256 + "\x00" + time.Now().UTC().Format(time.RFC3339Nano)))
	return writeLiveEvidenceFile(filepath.Join(directory, "programmatic-live-memory-validity-"+hex.EncodeToString(identity[:12])+".json"), append(payload, '\n'))
}

// TestMemoryValidityEvidenceRedactsOpaqueValues checks the live Cleanup
// record shape without issuing a live request.
func TestMemoryValidityEvidenceRedactsOpaqueValues(t *testing.T) {
	values, err := newMemoryValidityValues()
	if err != nil {
		t.Fatal("could not create opaque memory validity values")
	}
	probe := &memoryValidityProbe{query: values.Lookup}
	model := newLiveModel(t, probe, "offline-memory-validity-evidence", 2, nil)
	observed := &memoryObservedModel{inner: model}
	fixture, err := newMemoryValidityFixture(observed, "offline-memory-validity", values, "fresh", "offline-evidence")
	if err != nil {
		t.Fatal("could not construct offline memory validity fixture")
	}
	result, err := fixture.runtime.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{RunID: "offline-memory-validity-evidence", Text: memoryValidityPrompt(values.Lookup)}, nil)
	if err != nil || result.Status != core.RunCompleted {
		t.Fatal("offline memory validity evidence fixture did not complete")
	}
	caseDef := liveCase{name: "memory_validity_evidence", maxModelRounds: 2, maxToolCalls: 1, prompt: memoryValidityPrompt(values.Lookup)}
	base := finalizedLiveEvidence(caseDef, result, "offline-memory-validity", "test", "test-revision", caseDef.prompt, fixture.session.Events(), model, 0, true)
	base.Schema = memoryValidityEvidenceSchema
	base.FixtureVariant = "memory_validity_v1"
	record := liveMemoryValidityEvidence{liveEvidenceRecord: base, MemoryValidity: memoryValidityEvidenceFromRun(fixture, observed, model, fixture.session.Events(), result, values, values.Target, 1, true)}
	directory := t.TempDir()
	if err := writeLiveMemoryValidityEvidence(directory, record); err != nil {
		t.Fatal("could not write memory validity evidence")
	}
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) != 1 {
		t.Fatal("memory validity evidence did not create exactly one record")
	}
	payload, err := os.ReadFile(filepath.Join(directory, entries[0].Name()))
	if err != nil || memoryValidityContainsAny(string(payload), values.Lookup, values.Target, values.Expired, values.MissingValidity, values.Foreign, values.Unrelated) {
		t.Fatal("memory validity evidence was malformed or exposed an opaque value")
	}
	var persisted liveMemoryValidityEvidence
	if json.Unmarshal(payload, &persisted) != nil || persisted.Schema != memoryValidityEvidenceSchema || !persisted.InvocationEvidence.AdapterCallsObserved || !persisted.MemoryValidity.RecallCalledOnce || !persisted.MemoryValidity.AnswerMatchesArm || !persisted.MemoryValidity.PeerStoreIntact {
		t.Fatal("memory validity evidence did not retain required safe predicates")
	}
}
