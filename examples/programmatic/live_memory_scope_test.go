package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	appcontextassembly "github.com/whhhh1500/auto-agent/pkg/app/contextassembly"
	core "github.com/whhhh1500/auto-agent/pkg/core"
	extmemory "github.com/whhhh1500/auto-agent/pkg/extensions/memory"
)

// TestLiveMemoryScopeIsolationAcceptance is a development-contract sample,
// not a held-out evaluation. It makes intentional real model requests only
// when explicitly enabled. Its prompt contains the Project Orion entity and a
// lookup token, but never either scope's opaque fact value.
func TestLiveMemoryScopeIsolationAcceptance(t *testing.T) {
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

	for _, test := range []struct {
		name      string
		relevant  bool
		wantCount int
	}{
		{name: "scope_relevant", relevant: true, wantCount: 1},
		{name: "scope_no_match", relevant: false, wantCount: 0},
	} {
		stop := false
		passed := t.Run(test.name, func(t *testing.T) {
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
				t.Fatal("memory scope fixture value leaked into prompt")
			}
			model := newLiveModel(t, adapter, "memory_scope_"+test.name, 2, pacer)
			observed := &memoryObservedModel{inner: model}
			fixture, err := newMemoryScopeFixture(observed, modelID, values, test.name)
			if err != nil {
				t.Fatal("could not construct scoped memory fixture")
			}
			runID := "live-memory-scope-" + test.name
			var result core.TurnResult
			var events []core.SessionEvent
			t.Cleanup(func() {
				evidence := memoryScopeEvidenceFromRun(test.name, runID, fixture, observed, model, events, result, values, test.relevant)
				evidence.AcceptancePassed = !t.Failed()
				if err := writeMemoryScopeEvidence(os.Getenv("HARNESS_PROGRAMMATIC_EVIDENCE_PATH"), evidence); err != nil {
					t.Error("could not write live memory scope evidence")
				}
			})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			result, err = fixture.runtime.RunTurn(ctx, fixture.principal, fixture.session, core.TurnInput{RunID: runID, Text: prompt}, nil)
			events = fixture.session.Events()
			if err != nil || result.Status != core.RunCompleted {
				stop = model.stopSubsequentCases()
				t.Fatal("live scoped memory case did not complete")
			}
			assertMemoryScopeAcceptance(t, fixture, observed, model, events, result, values, test.relevant, test.wantCount)
		})
		if !passed && stop {
			t.Log("live model HTTP, transport, or responses protocol failure: remaining memory scope case skipped")
			break
		}
	}
}

type memoryScopeValues struct {
	Lookup, Missing, Target, Foreign, Unrelated string
}

func newMemoryScopeValues() (memoryScopeValues, error) {
	values := memoryScopeValues{}
	for _, target := range []*string{&values.Lookup, &values.Missing, &values.Target, &values.Foreign, &values.Unrelated} {
		value, err := memoryNonce()
		if err != nil {
			return memoryScopeValues{}, err
		}
		*target = value
	}
	return values, nil
}

func memoryScopePrompt(query string) string {
	return "For Project Orion, call memory.recall exactly once using lookup token \"" + query + "\". Do not call any other tool. If one matching entry is returned, answer only its code. If no matching entry is returned, answer exactly UNKNOWN."
}

func memoryScopePromptLeaks(prompt string, values memoryScopeValues) bool {
	return strings.Contains(prompt, values.Target) || strings.Contains(prompt, values.Foreign) || strings.Contains(prompt, values.Unrelated)
}

type memoryScopeFixture struct {
	runtime        *core.Runtime
	principal      core.Principal
	session        *core.Session
	store          extmemory.Store
	peerScope      core.ScopePath
	requestedModel string
	assemblerCalls atomic.Int64
}

func newMemoryScopeFixture(model core.LlmAdapter, modelID string, values memoryScopeValues, suffix string) (*memoryScopeFixture, error) {
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product, err := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "live-memory-scope"})
	if err != nil {
		return nil, err
	}
	tenant, err := product.Child(core.ScopeRef{Kind: core.ScopeTenant, ID: "memory-scope-tenant"})
	if err != nil {
		return nil, err
	}
	scopeA, err := tenant.Child(core.ScopeRef{Kind: core.ScopeUser, ID: "scope-a-" + values.Lookup[len("mem-"):len("mem-")+12]})
	if err != nil {
		return nil, err
	}
	peerScope, err := tenant.Child(core.ScopeRef{Kind: core.ScopeUser, ID: "scope-b-" + values.Lookup[len("mem-"):len("mem-")+12]})
	if err != nil {
		return nil, err
	}
	principal := core.Principal{TenantID: "memory-scope-tenant", SubjectID: "scope-a", Scope: scopeA, Grants: core.NewPermissionSet(core.PermRead)}
	sessionScope, err := scopeA.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "session-" + suffix})
	if err != nil {
		return nil, err
	}
	session, err := core.NewSession(core.SessionOptions{ID: "session-" + suffix, ProfileID: "example.memory.scope", Principal: principal, Scope: sessionScope})
	if err != nil {
		return nil, err
	}
	store := extmemory.NewSliceStore()
	for _, seed := range []struct {
		scope core.ScopePath
		entry extmemory.Entry
	}{
		{scope: scopeA, entry: extmemory.Entry{Key: "project-orion-launch-code", Content: "Project Orion lookup=" + values.Lookup + "; code=" + values.Target, Tags: []string{"orion"}}},
		{scope: scopeA, entry: extmemory.Entry{Key: "unrelated-record", Content: "Project Nimbus note=" + values.Unrelated, Tags: []string{"unrelated"}}},
		{scope: peerScope, entry: extmemory.Entry{Key: "project-orion-launch-code", Content: "Project Orion lookup=" + values.Lookup + "; code=" + values.Foreign, Tags: []string{"orion"}}},
	} {
		if _, err := store.Remember(context.Background(), seed.scope, seed.entry); err != nil {
			return nil, err
		}
	}
	capabilities, err := extmemory.NewStandardCapabilities(store)
	if err != nil {
		return nil, err
	}
	registry := core.NewCapabilityRegistry()
	for _, capability := range capabilities {
		if err := registry.Register(product, capability); err != nil {
			return nil, err
		}
	}
	profiles := core.NewAgentProfileRegistry()
	name := "Live scoped memory retrieval fixture"
	selection := core.ModelSelection{Provider: model.Provider(), Model: modelID}
	steps, calls := 2, 1
	instructions := core.PromptFragment{ID: "memory-scope-retrieval", Section: core.PromptInstructions, Content: "When the user asks for a remembered fact, use the returned memory entry as evidence before answering."}
	if err := profiles.Bind(core.AgentProfileLayer{Scope: product, ProfileID: session.ProfileID(), Name: &name, Model: &selection, MaxSteps: &steps, MaxToolCalls: &calls, AddCapabilities: []string{extmemory.RecallCapabilityID}, PutFragments: []core.PromptFragment{instructions}}); err != nil {
		return nil, err
	}
	assembler, err := appcontextassembly.NewAssembler(appcontextassembly.Config{})
	if err != nil {
		return nil, err
	}
	fixture := &memoryScopeFixture{principal: principal, session: session, store: store, peerScope: peerScope, requestedModel: modelID}
	fixture.runtime = &core.Runtime{
		Capabilities: registry, Profiles: profiles, ToolJournal: newMemoryJournal(),
		Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) { return model, nil }),
		ContextAssembler: func(ctx context.Context, input core.ModelContext) (core.ModelContext, error) {
			fixture.assemblerCalls.Add(1)
			return assembler.AssembleModelContext(ctx, input)
		},
	}
	return fixture, nil
}

func assertMemoryScopeAcceptance(t *testing.T, fixture *memoryScopeFixture, observed *memoryObservedModel, model *liveModel, events []core.SessionEvent, result core.TurnResult, values memoryScopeValues, relevant bool, wantEntries int) {
	t.Helper()
	assertMemoryScopePredicates(t, fixture, observed, events, result, values, relevant, wantEntries)
	if model.rounds() != 2 || fixture.assemblerCalls.Load() < 2 {
		t.Fatal("scoped memory case exceeded its two-round assembled context budget")
	}
	modelUsage, usageComplete := model.usage()
	sessionUsage, usages := memoryScopeUsage(events)
	if !usageComplete || usages != 2 || modelUsage != sessionUsage {
		t.Fatal("live scoped memory usage did not match the durable ledger")
	}
}

func assertMemoryScopePredicates(t *testing.T, fixture *memoryScopeFixture, observed *memoryObservedModel, events []core.SessionEvent, result core.TurnResult, values memoryScopeValues, relevant bool, wantEntries int) {
	t.Helper()
	calls, results, _, durableAnswer := liveSessionEvidence(events)
	callID, content, found := memoryRecallResult(calls, results)
	if !found || recalledMemoryScopeEntries(content) != wantEntries {
		t.Fatal("scoped memory recall did not return the expected entry count")
	}
	if !observed.initialContextNonceAbsent(values.Target) || memoryObservedContains(observed, values.Foreign) || memoryObservedContains(observed, values.Unrelated) {
		t.Fatal("scoped memory value entered model context before or across recall")
	}
	if !observed.finalContextHasRecallResult(callID, content) {
		t.Fatal("final model context did not contain the exact paired memory result")
	}
	if strings.Contains(content, values.Foreign) || strings.Contains(result.Answer, values.Foreign) || strings.Contains(durableAnswer, values.Foreign) {
		t.Fatal("peer-scope memory fact crossed the retrieval boundary")
	}
	if strings.Contains(content, values.Unrelated) || strings.Contains(result.Answer, values.Unrelated) || strings.Contains(durableAnswer, values.Unrelated) {
		t.Fatal("unrelated same-scope memory fact crossed the retrieval boundary")
	}
	if relevant {
		if strings.TrimSpace(result.Answer) != values.Target || strings.TrimSpace(durableAnswer) != values.Target {
			t.Fatal("relevant scoped memory fact was not used as the final answer")
		}
	} else if strings.TrimSpace(result.Answer) != "UNKNOWN" || strings.TrimSpace(durableAnswer) != "UNKNOWN" || strings.Contains(result.Answer, values.Target) {
		t.Fatal("missing scoped memory fact was invented")
	}
	peer, err := fixture.store.Recall(context.Background(), fixture.peerScope, values.Lookup, nil, 1)
	if err != nil || len(peer) != 1 || !strings.Contains(peer[0].Content, values.Foreign) {
		t.Fatal("peer scope memory changed during read-only evaluation")
	}
}

func recalledMemoryScopeEntries(content string) int {
	var payload struct {
		Entries []json.RawMessage `json:"entries"`
	}
	if json.Unmarshal([]byte(content), &payload) != nil || payload.Entries == nil {
		return -1
	}
	return len(payload.Entries)
}

func memoryObservedContains(observed *memoryObservedModel, value string) bool {
	if observed == nil || value == "" {
		return false
	}
	observed.mu.Lock()
	defer observed.mu.Unlock()
	for _, turn := range observed.turns {
		if memoryContextContains(turn, value) {
			return true
		}
	}
	return false
}

func memoryScopeUsage(events []core.SessionEvent) (total core.TokenUsage, reports int) {
	for _, event := range events {
		if event.Type != core.EvRunUsage {
			continue
		}
		var usage core.RunUsageData
		if json.Unmarshal(event.Data, &usage) != nil {
			return core.TokenUsage{}, -1
		}
		reports++
		total.InputTokens += usage.InputTokens
		total.OutputTokens += usage.OutputTokens
	}
	return total, reports
}

// memoryScopeEvidence records only safe predicates and aggregate counts. It
// deliberately excludes all fixture values, tool content, prompts, and model
// text, so the optional evidence directory cannot become a memory oracle.
type memoryScopeEvidence struct {
	Schema                      string `json:"schema"`
	CaseID                      string `json:"case_id"`
	RunID                       string `json:"run_id"`
	RequestedModel              string `json:"requested_model"`
	SourceRevision              string `json:"source_revision"`
	RuntimeStatus               string `json:"runtime_status"`
	ResultObserved              bool   `json:"result_observed"`
	ContextObserved             bool   `json:"context_observed"`
	TargetMatched               bool   `json:"target_matched"`
	NoMatchHonest               bool   `json:"no_match_honest"`
	ForeignAbsentToolResult     bool   `json:"foreign_absent_tool_result"`
	ForeignAbsentFinalContext   bool   `json:"foreign_absent_final_context"`
	ForeignAbsentAnswer         bool   `json:"foreign_absent_answer"`
	UnrelatedAbsentToolResult   bool   `json:"unrelated_absent_tool_result"`
	UnrelatedAbsentFinalContext bool   `json:"unrelated_absent_final_context"`
	InitialTargetAbsent         bool   `json:"initial_target_absent"`
	ContextExactPair            bool   `json:"context_exact_pair"`
	UsageLedgerMatches          bool   `json:"usage_ledger_matches"`
	PeerStoreIntact             bool   `json:"peer_store_intact"`
	RecallEntryCount            int    `json:"recall_entry_count"`
	ModelRounds                 int    `json:"model_rounds"`
	AssemblerCalls              int64  `json:"assembler_calls"`
	ReportedInputTokens         int64  `json:"reported_input_tokens"`
	ReportedOutputTokens        int64  `json:"reported_output_tokens"`
	UsageComplete               bool   `json:"usage_complete"`
	AcceptancePassed            bool   `json:"acceptance_passed"`
}

func memoryScopeEvidenceFromRun(caseID, runID string, fixture *memoryScopeFixture, observed *memoryObservedModel, model *liveModel, events []core.SessionEvent, result core.TurnResult, values memoryScopeValues, relevant bool) memoryScopeEvidence {
	evidence := memoryScopeEvidence{Schema: "harness.programmatic.live-memory-scope/v1", CaseID: caseID, RunID: runID, SourceRevision: liveSourceRevision(), RuntimeStatus: liveRunStatus(result)}
	if fixture == nil {
		return evidence
	}
	evidence.RequestedModel = fixture.requestedModel
	evidence.AssemblerCalls = fixture.assemblerCalls.Load()
	if model != nil {
		evidence.ModelRounds = model.rounds()
	}
	calls, results, ledger, durable := liveSessionEvidence(events)
	evidence.ResultObserved = result.RunID != "" || len(events) > 0
	evidence.ReportedInputTokens, evidence.ReportedOutputTokens = ledger.InputTokens, ledger.OutputTokens
	callID, content, found := memoryRecallResult(calls, results)
	evidence.RecallEntryCount = recalledMemoryScopeEntries(content)
	evidence.ContextObserved = observed != nil && observed.firstTurnObserved()
	evidence.InitialTargetAbsent = evidence.ContextObserved && observed.initialContextNonceAbsent(values.Target)
	evidence.ForeignAbsentFinalContext = evidence.ContextObserved && !memoryObservedContains(observed, values.Foreign)
	evidence.ContextExactPair = found && evidence.ContextObserved && observed.finalContextHasRecallResult(callID, content)
	evidence.ForeignAbsentToolResult = found && !strings.Contains(content, values.Foreign)
	evidence.ForeignAbsentAnswer = evidence.ResultObserved && !strings.Contains(result.Answer, values.Foreign) && !strings.Contains(durable, values.Foreign)
	evidence.UnrelatedAbsentToolResult = found && !strings.Contains(content, values.Unrelated)
	evidence.UnrelatedAbsentFinalContext = evidence.ContextObserved && !memoryObservedContains(observed, values.Unrelated)
	if relevant {
		evidence.TargetMatched = strings.TrimSpace(result.Answer) == values.Target && strings.TrimSpace(durable) == values.Target
	} else {
		evidence.NoMatchHonest = strings.TrimSpace(result.Answer) == "UNKNOWN" && strings.TrimSpace(durable) == "UNKNOWN" && !strings.Contains(result.Answer, values.Target)
	}
	if model != nil {
		modelUsage, complete := model.usage()
		_, usages := memoryScopeUsage(events)
		evidence.UsageLedgerMatches = complete && usages == 2 && modelUsage == ledger
		evidence.UsageComplete = complete && usages == 2
	} else {
		_, usages := memoryScopeUsage(events)
		evidence.UsageLedgerMatches = usages == 2 && (ledger.InputTokens > 0 || ledger.OutputTokens > 0)
		evidence.UsageComplete = usages == 2 && (ledger.InputTokens > 0 || ledger.OutputTokens > 0)
	}
	peer, err := fixture.store.Recall(context.Background(), fixture.peerScope, values.Lookup, nil, 1)
	evidence.PeerStoreIntact = err == nil && len(peer) == 1 && strings.Contains(peer[0].Content, values.Foreign)
	return evidence
}

func writeMemoryScopeEvidence(directory string, evidence memoryScopeEvidence) error {
	if strings.TrimSpace(directory) == "" {
		return nil
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return errors.New("live memory scope evidence directory is unavailable")
	}
	payload, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		return errors.New("live memory scope evidence encoding failed")
	}
	identity := sha256.Sum256([]byte(evidence.RunID + "\x00" + evidence.CaseID + "\x00" + time.Now().UTC().Format(time.RFC3339Nano)))
	return writeLiveEvidenceFile(filepath.Join(directory, "programmatic-live-memory-scope-"+fmt.Sprintf("%x", identity[:12])+".json"), append(payload, '\n'))
}
