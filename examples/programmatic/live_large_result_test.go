package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	appcontextassembly "github.com/whhhh1500/auto-agent/pkg/app/contextassembly"
	core "github.com/whhhh1500/auto-agent/pkg/core"
)

// largeDetailFixtureVariant is v3: it retains v2's explicit prompt scope and
// makes catalog guidance conditional on choosing program.execute. v1/v2 are
// preserved historical development controls in the evidence whitelist.
const largeDetailFixtureVariant = "large_detail_v3_conditional_catalog"

const largeDetailOutputSizeFixtureVariant = "large_detail_v4_visible_output_size"

// largeDetailAdapterLimitsFixtureVariant retains the historical evidence label
// for the adapter-limit validation control.
const largeDetailAdapterLimitsFixtureVariant = "large_detail_v5_adapter_limits"

const largeDetailDirectOnlyAdapterLimitsFixtureVariant = "large_detail_v6_direct_only_adapter_limits"

type largeDetailLiveConfig struct {
	fixtureVariant      string
	caseNameLabel       string
	exposeOutputSize    bool
	paddingBytes        []int
	skipForcedPTC       bool
	includeProgrammatic bool
	onlyBatchedDirect   bool
}

// TestLiveLargeDetailProgrammaticBenefit is an opt-in, synthetic read-only
// pressure experiment. The 2 KiB and 4 KiB arms retain the same task and
// budgets while their evidence records the model adapter's actual context
// limits, selected slots, and dropped groups.
func TestLiveLargeDetailProgrammaticBenefit(t *testing.T) {
	runLiveLargeDetailProgrammaticBenefit(t, largeDetailLiveConfig{fixtureVariant: largeDetailFixtureVariant})
}

// TestLiveLargeDetailOutputSizeDisclosure is the v4 development control. It
// is identical to v3 except that the large detail tool's already-enforced
// output limit is visible in its public description.
func TestLiveLargeDetailOutputSizeDisclosure(t *testing.T) {
	runLiveLargeDetailProgrammaticBenefit(t, largeDetailLiveConfig{
		fixtureVariant:   largeDetailOutputSizeFixtureVariant,
		caseNameLabel:    "output_size",
		exposeOutputSize: true,
	})
}

// TestLiveLargeDetailAdapterLimits preserves the v5 evidence label for the
// public output-size control. liveModel now forwards the configured adapter's
// validated context report in every live acceptance path.
func TestLiveLargeDetailAdapterLimits(t *testing.T) {
	runLiveLargeDetailProgrammaticBenefit(t, largeDetailLiveConfig{
		fixtureVariant:      largeDetailAdapterLimitsFixtureVariant,
		caseNameLabel:       "adapter_limits",
		exposeOutputSize:    true,
		paddingBytes:        []int{4 << 10},
		skipForcedPTC:       true,
		includeProgrammatic: true,
	})
}

// TestLiveLargeDetailDirectOnlyAdapterLimits is a reduced-menu direct-mode cost
// control. Its capability menu deliberately contains only the two fixture
// tools, so it is reported as a menu-different baseline rather than a
// same-menu causal comparison with the v5 adapter-limits cases.
func TestLiveLargeDetailDirectOnlyAdapterLimits(t *testing.T) {
	runLiveLargeDetailProgrammaticBenefit(t, largeDetailLiveConfig{
		fixtureVariant:      largeDetailDirectOnlyAdapterLimitsFixtureVariant,
		caseNameLabel:       "direct_only_adapter_limits",
		exposeOutputSize:    true,
		paddingBytes:        []int{4 << 10},
		skipForcedPTC:       true,
		includeProgrammatic: false,
		onlyBatchedDirect:   true,
	})
}

func runLiveLargeDetailProgrammaticBenefit(t *testing.T, config largeDetailLiveConfig) {
	t.Helper()
	if os.Getenv("HARNESS_PROGRAMMATIC_LIVE") != "1" && os.Getenv("HARNESS_ACCEPTANCE_LIVE_PROGRAMMATIC") != "1" {
		t.Skip("set HARNESS_PROGRAMMATIC_LIVE=1 to make intentional live model requests")
	}
	if liveProtocol() != "responses" {
		t.Skip("set HARNESS_PROGRAMMATIC_PROTOCOL=responses to retain structural wire observations")
	}
	// The production Responses adapter resolves http.DefaultTransport at send
	// time. Serialize and restore this process-wide hook exactly as the wire
	// acceptance test does; the large test itself is never parallel.
	liveWireTransportMu.Lock()
	previousTransport := http.DefaultTransport
	if previousTransport == nil {
		liveWireTransportMu.Unlock()
		t.Fatal("default HTTP transport is unavailable")
	}
	transport := &liveWireAuditTransport{next: previousTransport}
	http.DefaultTransport = transport
	t.Cleanup(func() {
		http.DefaultTransport = previousTransport
		liveWireTransportMu.Unlock()
	})
	adapter, err := newLiveAdapter(context.Background())
	if err != nil {
		t.Fatal("live model configuration is incomplete or invalid")
	}
	limits, ok := liveAdapterLimits(adapter)
	if !ok {
		t.Fatal("live adapter did not report valid context limits")
	}
	adapterLimits := &limits
	modelID := strings.TrimSpace(os.Getenv("HARNESS_LLM_MODEL"))
	if modelID == "" {
		t.Fatal("live model configuration is incomplete or invalid")
	}
	interval, err := liveMinRequestInterval(os.Getenv("HARNESS_PROGRAMMATIC_MIN_REQUEST_INTERVAL"))
	if err != nil {
		t.Fatal("live request interval is invalid")
	}
	pacer := &liveRequestPacer{interval: interval}
	const activeRows = 8
	basePrompt := liveLargeDetailPrompt(activeRows)
	includeProgrammatic := config.includeProgrammatic
	if config.fixtureVariant == largeDetailFixtureVariant || config.fixtureVariant == largeDetailOutputSizeFixtureVariant {
		includeProgrammatic = true
	}

	paddings := []int{2 << 10, 4 << 10}
	if len(config.paddingBytes) != 0 {
		paddings = append([]int(nil), config.paddingBytes...)
	}
	arms := []struct {
		name, strategy                       string
		requireProgram, requireBatchedDirect bool
	}{
		{name: "auto"},
		{name: "batched_direct", strategy: "forced_batched_direct", requireBatchedDirect: true},
		{name: "forced_ptc", strategy: "forced_ptc", requireProgram: true},
	}
	if config.skipForcedPTC {
		arms = arms[:2]
	}
	if config.onlyBatchedDirect {
		arms = arms[1:2]
	}
	for _, paddingBytes := range paddings {
		for _, arm := range arms {
			strategy := arm.strategy
			if !includeProgrammatic && strategy == "forced_batched_direct" {
				strategy = "forced_batched_direct_only"
			}
			caseDef := liveCase{
				name:                 largeDetailCaseName(config.caseNameLabel, paddingBytes, arm.name),
				activeRows:           activeRows,
				maxModelRounds:       10,
				maxToolCalls:         11,
				requireProgram:       arm.requireProgram,
				requireBatchedDirect: arm.requireBatchedDirect,
				outputContracts:      true,
				fixtureVariant:       config.fixtureVariant,
				detailPaddingBytes:   paddingBytes,
				strategyLabel:        strategy,
				prompt:               basePrompt,
			}
			stopAfterTransportOrProtocolFailure := false
			passed := t.Run(caseDef.name, func(t *testing.T) {
				startRequest := transport.count()
				contextEvidence := &largeDetailContextRecorder{}
				model := newLiveModel(t, adapter, caseDef.name, caseDef.maxModelRounds, pacer)
				var runtimeModel core.LlmAdapter = model
				runID := "live-" + caseDef.name
				started := time.Now()
				var result core.TurnResult
				var events []core.SessionEvent
				var elapsed time.Duration
				var fixture *fixture
				t.Cleanup(func() {
					record := finalizedLiveEvidence(caseDef, result, modelID, liveProtocol(), liveSourceRevision(), caseDef.prompt, events, model, elapsed, !t.Failed())
					record.FixtureEffects = liveFixtureEffectEvidenceFromFixture(fixture)
					record.LargeDetail = contextEvidence.evidence()
					record.LargeDetail.HTTPRequests = transport.snapshotSince(startRequest)
					if record.RunID == "" {
						record.RunID = runID
					}
					if err := writeLiveEvidence(os.Getenv("HARNESS_PROGRAMMATIC_EVIDENCE_PATH"), record); err != nil {
						t.Error("could not write live experiment evidence")
					}
				})

				fixture, err = newFixtureWithDataOptions(runtimeModel, includeProgrammatic, caseDef.activeRows, fixtureDataOptions{
					OutputContracts:         true,
					DetailIrrelevantBytes:   caseDef.detailPaddingBytes,
					ExposeOutputSize:        config.exposeOutputSize,
					ObserveAssembledContext: contextEvidence.observe,
				})
				if err != nil {
					t.Fatal("could not construct large-detail fixture")
				}
				contextEvidence.setSession(fixture.session)
				configure := configureLiveLargeDetailProfile
				if !includeProgrammatic {
					configure = configureLiveLargeDetailDirectOnlyProfile
				}
				if err := configure(fixture, adapter.Provider(), modelID, caseDef.maxModelRounds, caseDef.maxToolCalls, caseDef.strategyLabel); err != nil {
					t.Fatal("could not configure live large-detail fixture profile")
				}

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
				defer cancel()
				result, err = fixture.runtime.RunTurn(ctx, fixture.principal, fixture.session, core.TurnInput{RunID: runID, Text: caseDef.prompt}, nil)
				elapsed = time.Since(started)
				events = fixture.session.Events()
				diagnoseLiveProgramExecutions(t, events)
				if err != nil || result.Status != core.RunCompleted {
					stopAfterTransportOrProtocolFailure = model.stopSubsequentCases()
					t.Fatalf("live large-detail case did not complete (status=%s, model_rounds=%d)", result.Status, model.rounds())
				}
				assertLiveEvidence(t, fixture, caseDef, model, result)
				assertLargeDetailContextEvidence(t, caseDef, contextEvidence.evidence(), adapterLimits)
				if !includeProgrammatic {
					assertLargeDetailDirectOnlyMenu(t, model, transport.snapshotSince(startRequest))
				}
			})
			if !passed && stopAfterTransportOrProtocolFailure {
				t.Log("live model HTTP, transport, or responses protocol failure: remaining large-detail cases skipped")
				return
			}
		}
	}
}

func largeDetailCaseName(label string, paddingBytes int, arm string) string {
	if label == "" {
		return fmt.Sprintf("batch_n8_large_detail_%dk_%s", paddingBytes>>10, arm)
	}
	return fmt.Sprintf("batch_n8_large_detail_%s_%dk_%s", label, paddingBytes>>10, arm)
}

func liveLargeDetailPrompt(activeRows int) string {
	return fmt.Sprintf(`Use only the currently exposed tools in this synthetic read-only fixture. program.catalog and program.execute are permitted when they are currently exposed. There are %d active inventory rows. Obtain every active item's name exactly once, ignore the inactive row, and do not request tools that are not exposed in this fixture. After the tools complete, reply with exactly one line and no Markdown in this format: FINAL: <active item names in inventory order, separated by comma and one space>.`, activeRows)
}

// liveLargeDetailEvidence contains only structural context-selection facts.
// It intentionally excludes tool result contents, tool call IDs, source, and
// model text. Expected model-visible results exclude protected PTC children,
// which the assembler correctly elides behind their completed execute parent.
type liveLargeDetailEvidence struct {
	FinalContextObserved          bool `json:"final_context_observed"`
	FinalContextAssemblySucceeded bool `json:"final_context_assembly_succeeded"`
	FinalContextBudgetExceeded    bool `json:"final_context_budget_exceeded"`
	FinalContextSlotsObserved     bool `json:"final_context_slots_observed"`
	ExpectedModelVisibleResults   int  `json:"expected_model_visible_results"`
	PresentModelVisibleResults    int  `json:"present_model_visible_results"`
	ModelVisibleResultsComplete   bool `json:"model_visible_results_complete"`
	FinalContextDroppedObserved   bool `json:"final_context_dropped_groups_observed"`
	FinalContextDroppedGroups     int  `json:"final_context_dropped_groups"`
	// The values below are the limits actually passed by Runtime to this
	// assembler request. They are not a claim that an adapter's plan is a
	// provider-authenticated context-window capability.
	FinalContextLimitsObserved  bool `json:"final_context_limits_observed"`
	FinalContextWindowTokens    int  `json:"final_context_window_tokens"`
	FinalContextMaxOutputTokens int  `json:"final_context_max_output_tokens"`
	// HTTPRequests is a GetBody-derived structural observation. It contains no
	// tool-call sequence, arguments, instructions text, headers, or body.
	HTTPRequests []liveWireRequestEvidence `json:"http_requests,omitempty"`
}

type largeDetailContextSlot struct {
	assemblySucceeded bool
	budgetExceeded    bool
	slotsObserved     bool
	expected          int
	present           int
	complete          bool
	droppedObserved   bool
	droppedGroups     int
	limitsObserved    bool
	contextWindow     int
	maxOutput         int
}

type largeDetailContextRecorder struct {
	mu      sync.Mutex
	session *core.Session
	slots   []largeDetailContextSlot
}

func (r *largeDetailContextRecorder) setSession(session *core.Session) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.session = session
	r.mu.Unlock()
}

func (r *largeDetailContextRecorder) observe(request core.ModelContext, assembled core.ModelContext, err error) {
	if r == nil {
		return
	}
	r.mu.Lock()
	session := r.session
	r.mu.Unlock()
	if session == nil {
		return
	}
	expected := completedModelVisibleResultIDs(session.Events())
	present := 0
	slotsObserved := err == nil
	if err == nil {
		seen := make(map[string]struct{}, len(expected))
		for _, message := range assembled.Messages {
			if message.Role == core.RoleTool {
				if _, wanted := expected[message.ToolCallID]; wanted {
					seen[message.ToolCallID] = struct{}{}
				}
			}
		}
		present = len(seen)
	}
	r.mu.Lock()
	r.slots = append(r.slots, largeDetailContextSlot{
		assemblySucceeded: err == nil,
		budgetExceeded:    errors.Is(err, appcontextassembly.ErrBudgetExceeded),
		slotsObserved:     slotsObserved,
		expected:          len(expected),
		present:           present,
		complete:          err == nil && present == len(expected),
		droppedObserved:   err == nil,
		droppedGroups:     assembled.DroppedGroups,
		limitsObserved:    request.ContextWindowTokens > 0 && request.MaxOutputTokens > 0 && request.MaxOutputTokens < request.ContextWindowTokens,
		contextWindow:     request.ContextWindowTokens,
		maxOutput:         request.MaxOutputTokens,
	})
	r.mu.Unlock()
}

func completedModelVisibleResultIDs(events []core.SessionEvent) map[string]struct{} {
	calls, results, _, _ := liveSessionEvidence(events)
	called := make(map[string]struct{}, len(calls))
	for _, call := range calls {
		if call.CallID != "" && !strings.Contains(call.CallID, "/") {
			called[call.CallID] = struct{}{}
		}
	}
	completed := make(map[string]struct{}, len(called))
	for _, result := range results {
		if result.OK {
			if _, found := called[result.CallID]; found {
				completed[result.CallID] = struct{}{}
			}
		}
	}
	return completed
}

func (r *largeDetailContextRecorder) evidence() *liveLargeDetailEvidence {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.slots) == 0 {
		return &liveLargeDetailEvidence{}
	}
	last := r.slots[len(r.slots)-1]
	return &liveLargeDetailEvidence{
		FinalContextObserved:          true,
		FinalContextAssemblySucceeded: last.assemblySucceeded,
		FinalContextBudgetExceeded:    last.budgetExceeded,
		FinalContextSlotsObserved:     last.slotsObserved,
		ExpectedModelVisibleResults:   last.expected,
		PresentModelVisibleResults:    last.present,
		ModelVisibleResultsComplete:   last.complete,
		FinalContextDroppedObserved:   last.droppedObserved,
		FinalContextDroppedGroups:     last.droppedGroups,
		FinalContextLimitsObserved:    last.limitsObserved,
		FinalContextWindowTokens:      last.contextWindow,
		FinalContextMaxOutputTokens:   last.maxOutput,
	}
}

type liveAdapterContextLimits struct {
	contextWindowTokens int
	maxOutputTokens     int
}

func liveAdapterLimits(adapter core.LlmAdapter) (liveAdapterContextLimits, bool) {
	reporter, ok := adapter.(interface{ ModelContextLimits() (int, int) })
	if !ok {
		return liveAdapterContextLimits{}, false
	}
	window, output := reporter.ModelContextLimits()
	if window <= 0 || output <= 0 || output >= window {
		return liveAdapterContextLimits{}, false
	}
	return liveAdapterContextLimits{contextWindowTokens: window, maxOutputTokens: output}, true
}

func assertLargeDetailContextEvidence(t *testing.T, test liveCase, evidence *liveLargeDetailEvidence, adapterLimits *liveAdapterContextLimits) {
	t.Helper()
	if evidence == nil || !evidence.FinalContextObserved {
		t.Fatal("large-detail final model context was not observed")
	}
	if adapterLimits != nil {
		if !evidence.FinalContextLimitsObserved || evidence.FinalContextWindowTokens != adapterLimits.contextWindowTokens || evidence.FinalContextMaxOutputTokens != adapterLimits.maxOutputTokens {
			t.Fatalf("large-detail final context limits=%#v, want adapter window=%d output=%d", evidence, adapterLimits.contextWindowTokens, adapterLimits.maxOutputTokens)
		}
	}
	if test.detailPaddingBytes == 2<<10 || adapterLimits != nil && test.detailPaddingBytes == 4<<10 {
		if !evidence.FinalContextAssemblySucceeded || evidence.FinalContextBudgetExceeded || !evidence.FinalContextSlotsObserved || !evidence.ModelVisibleResultsComplete || !evidence.FinalContextDroppedObserved || evidence.FinalContextDroppedGroups != 0 {
			t.Fatalf("large-detail final context did not retain every model-visible result: %#v", evidence)
		}
		if test.requireBatchedDirect && (evidence.ExpectedModelVisibleResults != 9 || evidence.PresentModelVisibleResults != 9) {
			t.Fatalf("large-detail direct control final context slots=%#v, want 9", evidence)
		}
	}
}

func assertLargeDetailDirectOnlyMenu(t *testing.T, model *liveModel, requests []liveWireRequestEvidence) {
	t.Helper()
	rounds := model.evidenceRounds()
	if len(rounds) == 0 {
		t.Fatal("direct-only large-detail model menu was not observed")
	}
	first := rounds[0].toolSchemaNames
	if len(first) != 2 {
		t.Fatalf("direct-only first model tool menu=%v, want inventory and detail only", first)
	}
	seen := make(map[string]struct{}, len(first))
	for _, name := range first {
		seen[name] = struct{}{}
	}
	if _, found := seen[listToolID]; !found {
		t.Fatalf("direct-only first model tool menu omitted %q: %v", listToolID, first)
	}
	if _, found := seen[detailToolID]; !found {
		t.Fatalf("direct-only first model tool menu omitted %q: %v", detailToolID, first)
	}
	assertLiveWireRequests(t, requests, model.rounds(), 2)
}

func TestLiveLargeDetailStrategyLabelsAreClosed(t *testing.T) {
	if _, ok := liveStrategyInstruction("forced_batched_direct"); !ok {
		t.Fatal("direct control strategy was not recognized")
	}
	if _, ok := liveStrategyInstruction("forced_batched_direct_only"); !ok {
		t.Fatal("direct-only control strategy was not recognized")
	}
	if _, ok := liveStrategyInstruction("forced_ptc"); !ok {
		t.Fatal("PTC control strategy was not recognized")
	}
	if _, ok := liveStrategyInstruction("untrusted strategy text"); ok {
		t.Fatal("untrusted strategy text was accepted")
	}
}
