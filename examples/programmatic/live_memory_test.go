package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	appcontextassembly "github.com/whhhh1500/auto-agent/pkg/app/contextassembly"
	core "github.com/whhhh1500/auto-agent/pkg/core"
	extmemory "github.com/whhhh1500/auto-agent/pkg/extensions/memory"
)

// TestLiveMemoryContextAcceptance accepts the real model's retrieval
// compliance and tool-result context injection. It does not evaluate tool
// routing, durable-memory quality, token savings, or memory-cost benefits.
//
// The same prompt and profile run twice. Only the real in-process memory store
// differs: one arm contains an unprompted nonce under Project Cedar, while the
// other contains an unrelated fact. No nonce, memory content, arguments, or
// model text is logged or persisted as evidence.
func TestLiveMemoryContextAcceptance(t *testing.T) {
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
	nonce, err := memoryNonce()
	if err != nil {
		t.Fatal("could not create opaque memory fixture value")
	}
	if strings.Contains(memoryAcceptancePrompt, nonce) {
		t.Fatal("memory fixture value leaked into prompt")
	}

	for _, arm := range []struct {
		name     string
		relevant bool
		expected string
		entries  int
	}{
		{name: "memory_relevant", relevant: true, expected: "MEMORY: " + nonce, entries: 1},
		{name: "memory_irrelevant", relevant: false, expected: "MEMORY: UNKNOWN", entries: 0},
	} {
		passed := t.Run(arm.name, func(t *testing.T) {
			caseDef := liveCase{name: arm.name, maxModelRounds: 2, maxToolCalls: 1}
			model := newLiveModel(t, adapter, arm.name, 2, pacer)
			observed := &memoryObservedModel{inner: model}
			runID := "live-" + arm.name
			started := time.Now()
			var fixture *memoryAcceptanceFixture
			var result core.TurnResult
			var events []core.SessionEvent
			var elapsed time.Duration
			t.Cleanup(func() {
				if fixture != nil && events == nil {
					events = fixture.session.Events()
				}
				record := finalizedLiveEvidence(caseDef, result, modelID, liveProtocol(), liveSourceRevision(), memoryAcceptancePrompt, events, model, elapsed, !t.Failed())
				record.Memory = memoryEvidence(events, result, model, observed, arm.expected, nonce, arm.relevant)
				if record.RunID == "" {
					record.RunID = runID
				}
				if err := writeLiveEvidence(os.Getenv("HARNESS_PROGRAMMATIC_EVIDENCE_PATH"), record); err != nil {
					t.Error("could not write live memory experiment evidence")
				}
			})

			fixture, err = newMemoryAcceptanceFixture(observed, modelID, arm.relevant, nonce)
			if err != nil {
				t.Fatal("could not construct live memory fixture")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			result, err = fixture.runtime.RunTurn(ctx, fixture.principal, fixture.session, core.TurnInput{RunID: runID, Text: memoryAcceptancePrompt}, nil)
			elapsed = time.Since(started)
			events = fixture.session.Events()
			if err != nil || result.Status != core.RunCompleted {
				t.Fatalf("live memory case did not complete (status=%s, model_rounds=%d)", result.Status, model.rounds())
			}
			secret := ""
			if arm.relevant {
				secret = nonce
			}
			assertMemoryAcceptance(t, fixture, observed, model, events, result, arm.expected, arm.entries, secret)
		})
		if !passed {
			break
		}
	}
}

const memoryAcceptancePrompt = "Before answering, call memory.recall exactly once to check the remembered release code for Project Cedar. Do not call any other tool. If recall returns a matching code, reply exactly MEMORY: <code>; otherwise reply exactly MEMORY: UNKNOWN."

type memoryAcceptanceFixture struct {
	runtime        *core.Runtime
	principal      core.Principal
	session        *core.Session
	assemblerCalls atomic.Int64
}

func newMemoryAcceptanceFixture(model core.LlmAdapter, modelID string, relevant bool, nonce string) (*memoryAcceptanceFixture, error) {
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product, err := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "live-memory-acceptance"})
	if err != nil {
		return nil, err
	}
	user, err := product.Child(core.ScopeRef{Kind: core.ScopeUser, ID: "fixture-user"})
	if err != nil {
		return nil, err
	}
	principal := core.Principal{TenantID: "fixture-tenant", SubjectID: "fixture-subject", Scope: user, Grants: core.NewPermissionSet(core.PermRead)}
	sessionScope, err := user.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "fixture-session"})
	if err != nil {
		return nil, err
	}
	session, err := core.NewSession(core.SessionOptions{ID: "fixture-session", ProfileID: "example.memory.live", Principal: principal, Scope: sessionScope})
	if err != nil {
		return nil, err
	}

	store := extmemory.NewSliceStore()
	entry := extmemory.Entry{Key: "project-maple-release-code", Content: "Project Maple release code: unrelated-memory", Tags: []string{"project"}}
	if relevant {
		// SliceStore recall is a bounded substring filter. Include the natural
		// language query in the stored fact so the live model can search for
		// "Project Cedar" without being forced to know the fixture key.
		entry = extmemory.Entry{Key: "project-cedar-release-code", Content: "Project Cedar release code: " + nonce, Tags: []string{"project", "cedar"}}
	}
	if _, err := store.Remember(context.Background(), principal.Scope, entry); err != nil {
		return nil, err
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
	name := "Live memory retrieval acceptance fixture"
	selection := core.ModelSelection{Provider: model.Provider(), Model: modelID}
	steps, calls := 2, 1
	instructions := core.PromptFragment{ID: "memory-retrieval-acceptance", Section: core.PromptInstructions, Content: "When asked for remembered facts, use memory.recall and treat its returned entries as evidence before answering."}
	if err := profiles.Bind(core.AgentProfileLayer{Scope: product, ProfileID: session.ProfileID(), Name: &name, Model: &selection, MaxSteps: &steps, MaxToolCalls: &calls, AddCapabilities: []string{extmemory.RecallCapabilityID}, PutFragments: []core.PromptFragment{instructions}}); err != nil {
		return nil, err
	}
	assembler, err := appcontextassembly.NewAssembler(appcontextassembly.Config{})
	if err != nil {
		return nil, err
	}
	fixture := &memoryAcceptanceFixture{principal: principal, session: session}
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

type memoryObservedModel struct {
	inner core.LlmAdapter
	mu    sync.Mutex
	turns []memoryObservedTurn
}

type memoryObservedTurn struct {
	system   string
	messages []core.ChatMessage
}

func (m *memoryObservedModel) Provider() string { return m.inner.Provider() }

func (m *memoryObservedModel) Stream(ctx context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	m.mu.Lock()
	m.turns = append(m.turns, memoryObservedTurn{system: options.System, messages: cloneMemoryMessages(options.Messages)})
	m.mu.Unlock()
	return m.inner.Stream(ctx, options, emit)
}

func (m *memoryObservedModel) initialContextContains(value string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.turns) == 0 {
		return false
	}
	return memoryContextContains(m.turns[0], value)
}

func (m *memoryObservedModel) initialContextNonceAbsent(value string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.turns) > 0 && value != "" && !memoryContextContains(m.turns[0], value)
}

func (m *memoryObservedModel) firstTurnObserved() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.turns) > 0
}

func memoryContextContains(turn memoryObservedTurn, value string) bool {
	if value == "" {
		return false
	}
	if strings.Contains(turn.system, value) {
		return true
	}
	encoded, err := json.Marshal(turn.messages)
	return err == nil && strings.Contains(string(encoded), value)
}

func (m *memoryObservedModel) finalContextHasRecallResult(callID, content string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.turns) < 2 {
		return false
	}
	return memoryFinalContextHasExactRecallResult(m.turns[len(m.turns)-1].messages, callID, content)
}

func cloneMemoryMessages(messages []core.ChatMessage) []core.ChatMessage {
	clone := append([]core.ChatMessage(nil), messages...)
	for index := range clone {
		clone[index].ToolCalls = append([]core.ToolCall(nil), clone[index].ToolCalls...)
	}
	return clone
}

func assertMemoryAcceptance(t *testing.T, fixture *memoryAcceptanceFixture, observed *memoryObservedModel, model *liveModel, events []core.SessionEvent, result core.TurnResult, expected string, entries int, secret string) {
	t.Helper()
	if got := strings.TrimSpace(result.Answer); got != expected {
		t.Fatal("memory final answer did not match the actual recall arm")
	}
	if model.rounds() > 2 || fixture.assemblerCalls.Load() < 2 {
		t.Fatal("memory recall did not complete within the configured assembled context budget")
	}
	calls, results, sessionUsage, final := liveSessionEvidence(events)
	modelUsage, completeUsage := model.usage()
	if !memoryUsageMatchesLedger(modelUsage, sessionUsage, completeUsage) {
		t.Fatal("memory model stream usage is incomplete or differs from the session usage ledger")
	}
	if got := strings.TrimSpace(final); got != expected {
		t.Fatal("durable final assistant result did not match the memory recall arm")
	}
	recallCallID, recallContent, ok := memoryRecallResult(calls, results)
	if !ok {
		t.Fatal("memory recall was not called exactly once")
	}
	if secret != "" && !observed.initialContextNonceAbsent(secret) {
		t.Fatal("memory fixture value entered the initial model context before recall")
	}
	if !observed.finalContextHasRecallResult(recallCallID, recallContent) {
		t.Fatal("final model context did not contain the exact durable memory.recall result")
	}
	count, ok := recalledEntryCount([]core.ToolResultData{{CallID: recallCallID, Content: recallContent, OK: true}})
	if !ok || count != entries {
		// The irrelevant arm checks the requested Project Cedar query only. It
		// intentionally does not claim that the store has no unrelated memory.
		t.Fatal("memory recall result did not match the configured fixture arm")
	}
}

func memoryUsageMatchesLedger(modelUsage, sessionUsage core.TokenUsage, complete bool) bool {
	return complete && modelUsage == sessionUsage
}

// memoryEvidence derives only safe predicates from the already durable
// session and the in-memory request observer. It is called by Cleanup, so a
// completed runtime that fails an acceptance assertion still writes evidence.
func memoryEvidence(events []core.SessionEvent, result core.TurnResult, model *liveModel, observed *memoryObservedModel, expected, nonce string, nonceCheckApplicable bool) *liveMemoryEvidence {
	evidence := &liveMemoryEvidence{NonceCheckApplicable: nonceCheckApplicable}
	calls, results, sessionUsage, durableAnswer := liveSessionEvidence(events)
	callID, content, found := memoryRecallResult(calls, results)
	evidence.RecallResultFound = found
	if found {
		evidence.RecallEntryCount, evidence.RecallEntriesParsed = recalledEntryCount([]core.ToolResultData{{CallID: callID, Content: content, OK: true}})
	}
	if observed != nil {
		evidence.FirstTurnObserved = observed.firstTurnObserved()
		if nonceCheckApplicable {
			evidence.FirstTurnNonceAbsent = observed.initialContextNonceAbsent(nonce)
		}
		evidence.ContextExactMatch = found && observed.finalContextHasRecallResult(callID, content)
	}
	evidence.AnswerMatches = strings.TrimSpace(result.Answer) == expected
	evidence.DurableAnswerMatches = strings.TrimSpace(durableAnswer) == expected
	if model != nil && model.rounds() > 0 {
		modelUsage, completeUsage := model.usage()
		evidence.UsageMatches = memoryUsageMatchesLedger(modelUsage, sessionUsage, completeUsage)
	}
	return evidence
}

func memoryRecallResult(calls []core.ToolCallData, results []core.ToolResultData) (string, string, bool) {
	var callID string
	for _, call := range calls {
		if call.Name != extmemory.RecallCapabilityID {
			continue
		}
		if callID != "" || call.CallID == "" {
			return "", "", false
		}
		callID = call.CallID
	}
	if callID == "" {
		return "", "", false
	}
	for _, result := range results {
		if result.CallID == callID && result.OK {
			return callID, result.Content, true
		}
	}
	return "", "", false
}

func memoryFinalContextHasExactRecallResult(messages []core.ChatMessage, callID, content string) bool {
	if callID == "" {
		return false
	}
	for _, message := range messages {
		if message.Role == core.RoleTool && message.ToolCallID == callID && message.Content == content {
			return true
		}
	}
	return false
}

func recalledEntryCount(results []core.ToolResultData) (int, bool) {
	for _, result := range results {
		if !result.OK {
			continue
		}
		var payload struct {
			Entries []any `json:"entries"`
		}
		if json.Unmarshal([]byte(result.Content), &payload) == nil && payload.Entries != nil {
			return len(payload.Entries), true
		}
	}
	return 0, false
}

func memoryNonce() (string, error) {
	bytes := make([]byte, 12)
	if _, err := rand.Read(bytes); err != nil {
		return "", errors.New("opaque fixture randomness unavailable")
	}
	return "mem-" + hex.EncodeToString(bytes), nil
}
