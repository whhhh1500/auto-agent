package main

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

// TestLiveMemoryAuthorityLookupV5Acceptance differs from V4 only in its user
// prompt: it asks for the two independent read-only calls in one response.
// Each frozen entity gets one RunTurn attempt with the same two-round and
// two-tool profile budget as V4.
func TestLiveMemoryAuthorityLookupV5Acceptance(t *testing.T) {
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
	pacer := &liveRequestPacer{interval: interval}
	for index, entity := range []string{"release.channel/v2", "billing-policy:eu_west", "Node-7.Alpha"} {
		index, entity := index, entity
		t.Run("case_"+string(rune('1'+index)), func(t *testing.T) {
			values, err := newMemoryAuthorityValuesForEntity(entity)
			if err != nil {
				t.Fatal("could not create opaque lookup authority values")
			}
			caseDef := liveCase{name: "memory_authority_lookup_v5_case_" + string(rune('1'+index)), maxModelRounds: 2, maxToolCalls: 2, prompt: memoryAuthorityLookupV5Prompt(entity)}
			model := newLiveModel(t, adapter, caseDef.name, 2, pacer)
			observed := &memoryObservedModel{inner: model}
			fixture, err := newMemoryAuthorityLookupFixture(observed, modelID, values, "live-lookup-v5-"+string(rune('1'+index)))
			if err != nil {
				t.Fatal("could not construct lookup authority fixture")
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
			result, err = fixture.runtime.RunTurn(ctx, fixture.principal, fixture.session, core.TurnInput{RunID: "live-memory-authority-lookup-v5-" + string(rune('1'+index)), Text: caseDef.prompt}, nil)
			events = fixture.session.Events()
			if err != nil || result.Status != core.RunCompleted {
				t.Fatal("live lookup authority v5 case did not complete")
			}
			if err := assertMemoryAuthorityLookupRun(fixture, observed, model, events, result, values); err != nil {
				t.Fatal(err)
			}
			accepted = true
		})
	}
}

func memoryAuthorityLookupV5Prompt(entity string) string {
	return "Determine the current launch code for entity " + entity + ". Call memory.lookup and authority.current in the same response; both are independent read-only calls. Use the canonical entity copied exactly, including punctuation and case, for both. memory.lookup is historical context only. Answer only the current authority code."
}
