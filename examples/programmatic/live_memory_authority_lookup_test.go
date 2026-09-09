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
	"sync/atomic"
	"testing"
	"time"

	appcontextassembly "github.com/whhhh1500/auto-agent/pkg/app/contextassembly"
	core "github.com/whhhh1500/auto-agent/pkg/core"
	extmemory "github.com/whhhh1500/auto-agent/pkg/extensions/memory"
)

const memoryAuthorityLookupEvidenceSchema = "harness.programmatic.live-memory-authority-lookup/v1"

// TestLiveMemoryAuthorityLookupV4Acceptance is deliberately independent from
// the earlier recall waves. Each frozen entity makes one RunTurn attempt only;
// a run may use at most two model rounds and two tool calls.
func TestLiveMemoryAuthorityLookupV4Acceptance(t *testing.T) {
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
			caseDef := liveCase{name: "memory_authority_lookup_v4_case_" + string(rune('1'+index)), maxModelRounds: 2, maxToolCalls: 2, prompt: memoryAuthorityLookupPrompt(entity)}
			model := newLiveModel(t, adapter, caseDef.name, 2, pacer)
			observed := &memoryObservedModel{inner: model}
			fixture, err := newMemoryAuthorityLookupFixture(observed, modelID, values, "live-lookup-v4-"+string(rune('1'+index)))
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
			result, err = fixture.runtime.RunTurn(ctx, fixture.principal, fixture.session, core.TurnInput{RunID: "live-memory-authority-lookup-v4-" + string(rune('1'+index)), Text: caseDef.prompt}, nil)
			events = fixture.session.Events()
			if err != nil || result.Status != core.RunCompleted {
				t.Fatal("live lookup authority case did not complete")
			}
			if err := assertMemoryAuthorityLookupRun(fixture, observed, model, events, result, values); err != nil {
				t.Fatal(err)
			}
			accepted = true
		})
	}
}

type memoryAuthorityLookupFixture struct {
	runtime         *core.Runtime
	principal       core.Principal
	session         *core.Session
	base            *extmemory.SliceStore
	peerScope       core.ScopePath
	peerSnapshot    extmemory.Entry
	foreignValue    string
	historicalEntry extmemory.Entry
	historicalValue string
	currentValue    string
	authority       *authorityCurrentTool
	journal         *memoryJournal
	assemblerCalls  atomic.Int64
}

// newMemoryAuthorityLookupFixture uses the production opt-in constructor with
// SliceStore. It deliberately does not install the standard memory menu.
func newMemoryAuthorityLookupFixture(model core.LlmAdapter, modelID string, values memoryAuthorityValues, suffix string) (*memoryAuthorityLookupFixture, error) {
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product, err := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "memory-authority-lookup"})
	if err != nil {
		return nil, err
	}
	tenant, err := product.Child(core.ScopeRef{Kind: core.ScopeTenant, ID: "memory-authority-lookup-tenant"})
	if err != nil {
		return nil, err
	}
	currentScope, err := tenant.Child(core.ScopeRef{Kind: core.ScopeUser, ID: "current"})
	if err != nil {
		return nil, err
	}
	peerScope, err := tenant.Child(core.ScopeRef{Kind: core.ScopeUser, ID: "peer"})
	if err != nil {
		return nil, err
	}
	principal := core.Principal{TenantID: "memory-authority-lookup-tenant", SubjectID: "current", Scope: currentScope, Grants: core.NewPermissionSet(core.PermRead)}
	sessionScope, err := currentScope.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "session-" + suffix})
	if err != nil {
		return nil, err
	}
	session, err := core.NewSession(core.SessionOptions{ID: "session-" + suffix, ProfileID: "example.memory.authority.lookup", Principal: principal, Scope: sessionScope})
	if err != nil {
		return nil, err
	}
	base := extmemory.NewSliceStore()
	historical, err := base.Remember(context.Background(), currentScope, extmemory.Entry{ID: "historical", Key: values.Entity, Content: "entity=" + values.Entity + "; code=" + values.Historical, Tags: []string{"historical"}})
	if err != nil {
		return nil, err
	}
	peer, err := base.Remember(context.Background(), peerScope, extmemory.Entry{ID: "peer", Key: values.Entity, Content: "entity=" + values.Entity + "; code=" + values.Foreign, Tags: []string{"peer"}})
	if err != nil {
		return nil, err
	}
	lookup, err := extmemory.NewLookupCapability(base)
	if err != nil {
		return nil, err
	}
	authority := &authorityCurrentTool{available: true, entity: values.Entity, code: values.Current}
	registry := core.NewCapabilityRegistry()
	if err := registry.Register(product, lookup); err != nil {
		return nil, err
	}
	if err := registry.Register(product, authority); err != nil {
		return nil, err
	}
	profiles := core.NewAgentProfileRegistry()
	name := "Memory authority exact lookup fixture"
	selection := core.ModelSelection{Provider: model.Provider(), Model: modelID}
	steps, calls := 2, 2
	instructions := core.PromptFragment{ID: "memory-authority-lookup", Section: core.PromptInstructions, Content: "Call memory.lookup with the entity copied exactly, including punctuation and case, and call authority.current. memory.lookup is historical only; answer only the available authority.current code."}
	if err := profiles.Bind(core.AgentProfileLayer{Scope: product, ProfileID: session.ProfileID(), Name: &name, Model: &selection, MaxSteps: &steps, MaxToolCalls: &calls, AddCapabilities: []string{extmemory.LookupCapabilityID, authorityCurrentID}, PutFragments: []core.PromptFragment{instructions}}); err != nil {
		return nil, err
	}
	assembler, err := appcontextassembly.NewAssembler(appcontextassembly.Config{})
	if err != nil {
		return nil, err
	}
	journal := newMemoryJournal()
	fixture := &memoryAuthorityLookupFixture{principal: principal, session: session, base: base, peerScope: peerScope, peerSnapshot: peer, foreignValue: values.Foreign, historicalEntry: historical, historicalValue: values.Historical, currentValue: values.Current, authority: authority, journal: journal}
	fixture.runtime = &core.Runtime{Capabilities: registry, Profiles: profiles, ToolJournal: journal, Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) { return model, nil }), ContextAssembler: func(ctx context.Context, input core.ModelContext) (core.ModelContext, error) {
		fixture.assemblerCalls.Add(1)
		return assembler.AssembleModelContext(ctx, input)
	}}
	return fixture, nil
}

func memoryAuthorityLookupPrompt(entity string) string {
	return "Determine the current launch code for entity " + entity + ". Call memory.lookup with that canonical entity copied exactly, including punctuation and case, then call authority.current. memory.lookup is historical context only. Answer only the current authority code."
}

type parsedLookupAuthorityResult struct {
	Found bool
	Entry *core.MemoryEntry
	Valid bool
}

func parseLookupAuthorityResult(content string) parsedLookupAuthorityResult {
	var envelope map[string]json.RawMessage
	if json.Unmarshal([]byte(content), &envelope) != nil || len(envelope) != 2 || envelope["found"] == nil || envelope["entry"] == nil {
		return parsedLookupAuthorityResult{}
	}
	var found bool
	if json.Unmarshal(envelope["found"], &found) != nil {
		return parsedLookupAuthorityResult{}
	}
	if !found {
		var entry *core.MemoryEntry
		if json.Unmarshal(envelope["entry"], &entry) != nil || entry != nil {
			return parsedLookupAuthorityResult{}
		}
		return parsedLookupAuthorityResult{Found: false, Valid: true}
	}
	var entry core.MemoryEntry
	if json.Unmarshal(envelope["entry"], &entry) != nil || entry.ID == "" || entry.Key == "" || entry.Content == "" || entry.CreatedAt.IsZero() {
		return parsedLookupAuthorityResult{}
	}
	return parsedLookupAuthorityResult{Found: true, Entry: &entry, Valid: true}
}

func sameLookupAuthorityEntry(left, right core.MemoryEntry) bool {
	return left.ID == right.ID && left.Key == right.Key && left.Content == right.Content && left.CreatedAt.Equal(right.CreatedAt) && strings.Join(left.Tags, "\x00") == strings.Join(right.Tags, "\x00")
}

func lookupAuthorityProfileOnly(events []core.SessionEvent) bool {
	found := false
	for _, event := range events {
		if event.Type != core.EvRunStart {
			continue
		}
		var start core.RunStartData
		if json.Unmarshal(event.Data, &start) != nil || start.Composition == nil || len(start.Composition.Capabilities) != 2 {
			return false
		}
		seen := map[string]bool{}
		for _, capability := range start.Composition.Capabilities {
			seen[capability.Manifest.ID] = true
		}
		found = seen[extmemory.LookupCapabilityID] && seen[authorityCurrentID] && len(seen) == 2
	}
	return found
}

func assertMemoryAuthorityLookupRun(f *memoryAuthorityLookupFixture, observed *memoryObservedModel, model *liveModel, events []core.SessionEvent, result core.TurnResult, values memoryAuthorityValues) error {
	if f == nil || observed == nil || model == nil || values.Historical == values.Current || f.historicalEntry.Key != values.Entity || f.historicalEntry.Content != "entity="+values.Entity+"; code="+values.Historical || strings.TrimSpace(result.Answer) != values.Current || model.rounds() != 2 || f.assemblerCalls.Load() < 2 {
		return errors.New("lookup authority final answer or round budget is invalid")
	}
	invocation := liveInvocationEvidenceFromRounds(model.evidenceRounds(), events)
	if invocation == nil || invocation.ActualAdapterCalls != 2 || invocation.PacingCanceledBeforeAdapter != 0 || !invocation.ReportedUsageComplete || !invocation.UsageProtocolConsistent || !invocation.LedgerUsageMatched || !invocation.UniqueLedgerInvocationIDs {
		return errors.New("lookup authority invocation usage is incomplete")
	}
	calls, results, _, durable := liveSessionEvidence(events)
	if strings.TrimSpace(durable) != values.Current || len(calls) != 2 || len(results) != 2 || f.journal.completed() != 2 || f.authority.callsN() != 1 || !lookupAuthorityProfileOnly(events) {
		return errors.New("lookup authority profile or durable evidence is incomplete")
	}
	byName, byID := map[string]core.ToolCallData{}, map[string]core.ToolResultData{}
	for _, call := range calls {
		if call.CallID == "" || (call.Name != extmemory.LookupCapabilityID && call.Name != authorityCurrentID) {
			return errors.New("lookup authority observed an unexpected tool")
		}
		if _, duplicate := byName[call.Name]; duplicate {
			return errors.New("lookup authority called a tool more than once")
		}
		byName[call.Name] = call
	}
	for _, toolResult := range results {
		if toolResult.CallID == "" || !toolResult.OK {
			return errors.New("lookup authority has a failed or malformed tool result")
		}
		if _, duplicate := byID[toolResult.CallID]; duplicate {
			return errors.New("lookup authority repeated a tool result")
		}
		byID[toolResult.CallID] = toolResult
	}
	lookupCall, lookupCalled := byName[extmemory.LookupCapabilityID]
	authorityCall, authorityCalled := byName[authorityCurrentID]
	lookupResult, lookupReturned := byID[lookupCall.CallID]
	authorityResult, authorityReturned := byID[authorityCall.CallID]
	if !lookupCalled || !authorityCalled || !lookupReturned || !authorityReturned || len(lookupCall.Args) != 1 {
		return errors.New("lookup authority did not produce the exact tool pair")
	}
	key, keyOK := lookupCall.Args["key"].(string)
	parsedLookup := parseLookupAuthorityResult(lookupResult.Content)
	if !keyOK || key != values.Entity || !parsedLookup.Valid || !parsedLookup.Found || parsedLookup.Entry == nil || !sameLookupAuthorityEntry(*parsedLookup.Entry, f.historicalEntry) {
		return errors.New("lookup authority historical lookup was not exact and structured")
	}
	available, code, authorityValid := memoryAuthorityResult(authorityResult.Content, values.Entity)
	if !available || !authorityValid || code != values.Current || !observed.finalContextHasRecallResult(lookupCall.CallID, lookupResult.Content) || !observed.finalContextHasRecallResult(authorityCall.CallID, authorityResult.Content) {
		return errors.New("lookup authority current result or context pair is invalid")
	}
	allResults := lookupResult.Content + authorityResult.Content
	if !observed.firstTurnObserved() || observed.initialContextContains(values.Historical) || observed.initialContextContains(values.Current) || observed.initialContextContains(values.Foreign) || memoryObservedContains(observed, values.Foreign) || strings.Contains(allResults+result.Answer+durable, values.Foreign) || !memoryAuthorityLookupPeerIntact(f) {
		return errors.New("lookup authority crossed a scope boundary")
	}
	return nil
}

func memoryAuthorityLookupPeerIntact(f *memoryAuthorityLookupFixture) bool {
	if f == nil {
		return false
	}
	entry, found, err := f.base.Lookup(context.Background(), f.peerScope, f.peerSnapshot.Key)
	return err == nil && found && sameLookupAuthorityEntry(entry, f.peerSnapshot)
}

type liveMemoryAuthorityLookupEvidence struct {
	Schema                string `json:"schema"`
	CaseID                string `json:"case_id"`
	RunIDHash             string `json:"run_id_hash"`
	EntitySHA256          string `json:"entity_sha256"`
	ProviderIdentifier    string `json:"provider_identifier"`
	RequestedModel        string `json:"requested_model"`
	Protocol              string `json:"protocol"`
	SourceRevision        string `json:"source_revision"`
	BackendIdentity       string `json:"backend_identity_evidence"`
	RunAttempts           int    `json:"run_attempts"`
	RuntimeStatus         string `json:"runtime_status"`
	AcceptancePassed      bool   `json:"acceptance_passed"`
	ModelRounds           int    `json:"model_rounds"`
	ToolCalls             int    `json:"tool_calls"`
	UsageInputTokens      int64  `json:"usage_input_tokens"`
	UsageOutputTokens     int64  `json:"usage_output_tokens"`
	UsageComplete         bool   `json:"usage_complete"`
	ProfileOnlyPair       bool   `json:"profile_only_pair"`
	LookupKeyExact        bool   `json:"lookup_key_exact"`
	LookupFound           bool   `json:"lookup_found"`
	LookupEntryStructured bool   `json:"lookup_entry_structured"`
	AuthorityStructured   bool   `json:"authority_structured"`
	ConflictRecognized    bool   `json:"conflict_recognized"`
	ContextExactPair      bool   `json:"context_exact_pair"`
	JournalExactPair      bool   `json:"journal_exact_pair"`
	CurrentAnswer         bool   `json:"current_answer"`
	CurrentDurableAnswer  bool   `json:"current_durable_answer"`
	PeerScopeIsolated     bool   `json:"peer_scope_isolated"`
	ForeignAbsent         bool   `json:"foreign_absent"`
	TotalElapsedMS        int64  `json:"total_elapsed_ms"`
}

func liveMemoryAuthorityLookupRecord(caseDef liveCase, result core.TurnResult, modelID, provider, entity string, events []core.SessionEvent, model *liveModel, fixture *memoryAuthorityLookupFixture, observed *memoryObservedModel, accepted bool, elapsed time.Duration) liveMemoryAuthorityLookupEvidence {
	entitySum, runSum := sha256.Sum256([]byte(entity)), sha256.Sum256([]byte(result.RunID))
	record := liveMemoryAuthorityLookupEvidence{Schema: memoryAuthorityLookupEvidenceSchema, CaseID: caseDef.name, RunIDHash: hex.EncodeToString(runSum[:]), EntitySHA256: hex.EncodeToString(entitySum[:]), ProviderIdentifier: provider, RequestedModel: modelID, Protocol: liveProtocol(), SourceRevision: liveSourceRevision(), BackendIdentity: "not_independently_verified", RunAttempts: 1, RuntimeStatus: liveRunStatus(result), AcceptancePassed: accepted, TotalElapsedMS: elapsed.Milliseconds()}
	calls, results, usage, durable := liveSessionEvidence(events)
	record.ToolCalls, record.UsageInputTokens, record.UsageOutputTokens = len(calls), usage.InputTokens, usage.OutputTokens
	if model != nil {
		record.ModelRounds = model.rounds()
		invocation := liveInvocationEvidenceFromRounds(model.evidenceRounds(), events)
		record.UsageComplete = invocation != nil && invocation.ActualAdapterCalls == record.ModelRounds && invocation.ReportedUsageComplete && invocation.UsageProtocolConsistent && invocation.LedgerUsageMatched && invocation.UniqueLedgerInvocationIDs
	}
	record.ProfileOnlyPair = lookupAuthorityProfileOnly(events)
	if fixture == nil {
		return record
	}
	byName, byID := map[string]core.ToolCallData{}, map[string]core.ToolResultData{}
	for _, call := range calls {
		byName[call.Name] = call
	}
	for _, toolResult := range results {
		byID[toolResult.CallID] = toolResult
	}
	lookupCall, authorityCall := byName[extmemory.LookupCapabilityID], byName[authorityCurrentID]
	lookupResult, authorityResult := byID[lookupCall.CallID], byID[authorityCall.CallID]
	if key, ok := lookupCall.Args["key"].(string); ok {
		record.LookupKeyExact = key == entity && len(lookupCall.Args) == 1
	}
	parsedLookup := parseLookupAuthorityResult(lookupResult.Content)
	record.LookupFound = parsedLookup.Valid && parsedLookup.Found
	record.LookupEntryStructured = record.LookupFound && parsedLookup.Entry != nil && sameLookupAuthorityEntry(*parsedLookup.Entry, fixture.historicalEntry)
	available, code, valid := memoryAuthorityResult(authorityResult.Content, entity)
	record.AuthorityStructured = authorityResult.OK && available && valid && code == fixture.currentValue
	historicalStructured := fixture.historicalEntry.Key == entity && fixture.historicalEntry.Content == "entity="+entity+"; code="+fixture.historicalValue && fixture.historicalValue != fixture.currentValue
	record.ConflictRecognized = record.LookupEntryStructured && historicalStructured && record.AuthorityStructured
	record.ContextExactPair = observed != nil && observed.finalContextHasRecallResult(lookupCall.CallID, lookupResult.Content) && observed.finalContextHasRecallResult(authorityCall.CallID, authorityResult.Content)
	record.JournalExactPair = fixture.journal != nil && fixture.journal.completed() == 2 && len(calls) == 2 && len(results) == 2
	record.CurrentAnswer = strings.TrimSpace(result.Answer) == fixture.currentValue
	record.CurrentDurableAnswer = strings.TrimSpace(durable) == fixture.currentValue
	record.PeerScopeIsolated = memoryAuthorityLookupPeerIntact(fixture)
	allVisible := lookupResult.Content + authorityResult.Content + result.Answer + durable
	record.ForeignAbsent = observed != nil && !memoryObservedContains(observed, fixture.peerSnapshot.Content) && !memoryObservedContains(observed, fixture.foreignValue) && !strings.Contains(allVisible, fixture.peerSnapshot.Content) && !strings.Contains(allVisible, fixture.foreignValue)
	return record
}

func writeLiveMemoryAuthorityLookupEvidence(directory string, record liveMemoryAuthorityLookupEvidence) error {
	if strings.TrimSpace(directory) == "" {
		return nil
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return errors.New("live memory authority lookup evidence directory is unavailable")
	}
	payload, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return errors.New("live memory authority lookup evidence encoding failed")
	}
	identity := sha256.Sum256([]byte(record.RunIDHash + "\x00" + record.EntitySHA256 + "\x00" + time.Now().UTC().Format(time.RFC3339Nano)))
	return writeLiveEvidenceFile(filepath.Join(directory, "programmatic-live-memory-authority-lookup-"+hex.EncodeToString(identity[:12])+".json"), append(payload, '\n'))
}
