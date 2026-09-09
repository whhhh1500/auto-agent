package main

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

// TestLiveMemoryAuthorityLookupV6MixedCaseAcceptance keeps the V5 fixture and
// budget, and changes only the user prompt with a literal mixed-case lookup
// argument. It intentionally has one frozen case and one RunTurn attempt.
func TestLiveMemoryAuthorityLookupV6MixedCaseAcceptance(t *testing.T) {
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
	values, err := newMemoryAuthorityValuesForEntity("Node-7.Alpha")
	if err != nil {
		t.Fatal("could not create opaque mixed-case authority values")
	}
	caseDef := liveCase{name: "v6_mixed_case", maxModelRounds: 2, maxToolCalls: 2, prompt: memoryAuthorityLookupV6Prompt()}
	model := newLiveModel(t, adapter, caseDef.name, 2, &liveRequestPacer{interval: interval})
	observed := &memoryObservedModel{inner: model}
	fixture, err := newMemoryAuthorityLookupFixture(observed, modelID, values, "live-lookup-v6-mixed-case")
	if err != nil {
		t.Fatal("could not construct v6 mixed-case fixture")
	}
	started := time.Now()
	var result core.TurnResult
	var events []core.SessionEvent
	accepted := false
	t.Cleanup(func() {
		if events == nil {
			events = fixture.session.Events()
		}
		record := liveMemoryAuthorityLookupRecord(caseDef, result, modelID, model.Provider(), values.Entity, events, model, fixture, observed, accepted, time.Since(started))
		if err := writeLiveMemoryAuthorityLookupEvidence(os.Getenv("HARNESS_PROGRAMMATIC_EVIDENCE_PATH"), record); err != nil {
			t.Error("could not write live memory authority lookup evidence")
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	result, err = fixture.runtime.RunTurn(ctx, fixture.principal, fixture.session, core.TurnInput{RunID: "live-memory-authority-lookup-v6-mixed-case", Text: caseDef.prompt}, nil)
	events = fixture.session.Events()
	if err != nil || result.Status != core.RunCompleted {
		t.Fatal("live lookup authority v6 mixed-case did not complete")
	}
	if err := assertMemoryAuthorityLookupRun(fixture, observed, model, events, result, values); err != nil {
		t.Fatal(err)
	}
	accepted = true
}

func memoryAuthorityLookupV6Prompt() string {
	return memoryAuthorityLookupV5Prompt("Node-7.Alpha") + ` For memory.lookup, use exactly {"key":"Node-7.Alpha"}; do not change any character.`
}
