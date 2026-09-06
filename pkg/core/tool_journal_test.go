package core

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type memoryToolInvocationJournal struct {
	mu           sync.Mutex
	records      map[string]ToolInvocationRecord
	failBegin    bool
	failComplete bool
}

func newMemoryToolInvocationJournal() *memoryToolInvocationJournal {
	return &memoryToolInvocationJournal{records: map[string]ToolInvocationRecord{}}
}

func invocationTestKey(invocation ToolInvocation) string {
	return invocation.SessionID + "\x00" + invocation.RunID + "\x00" + invocation.CallID
}

func sameTestInvocation(left, right ToolInvocation) bool {
	return left == right
}

func (j *memoryToolInvocationJournal) BeginToolInvocation(_ context.Context, invocation ToolInvocation) (ToolInvocationRecord, ToolInvocationDecision, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.failBegin {
		return ToolInvocationRecord{}, "", errors.New("journal unavailable")
	}
	key := invocationTestKey(invocation)
	record, exists := j.records[key]
	if !exists {
		now := time.Now().UTC()
		record = ToolInvocationRecord{ToolInvocation: invocation, State: ToolInvocationStarted, StartedAt: now, UpdatedAt: now}
		j.records[key] = record
		return CloneToolInvocationRecord(record), ToolInvocationExecuteNew, nil
	}
	if !sameTestInvocation(record.ToolInvocation, invocation) {
		return CloneToolInvocationRecord(record), ToolInvocationConflict, nil
	}
	if record.State == ToolInvocationCompleted {
		return CloneToolInvocationRecord(record), ToolInvocationReplay, nil
	}
	if invocation.Idempotent {
		return CloneToolInvocationRecord(record), ToolInvocationExecuteRetry, nil
	}
	return CloneToolInvocationRecord(record), ToolInvocationUnknown, nil
}

func (j *memoryToolInvocationJournal) CompleteToolInvocation(_ context.Context, invocation ToolInvocation, result CapabilityResult) (ToolInvocationRecord, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.failComplete {
		return ToolInvocationRecord{}, errors.New("journal unavailable")
	}
	key := invocationTestKey(invocation)
	record, exists := j.records[key]
	if !exists || !sameTestInvocation(record.ToolInvocation, invocation) {
		return ToolInvocationRecord{}, errors.New("journal identity conflict")
	}
	if record.State != ToolInvocationCompleted {
		now := time.Now().UTC()
		canonical := cloneCapabilityResult(result)
		record.State, record.Result = ToolInvocationCompleted, &canonical
		record.UpdatedAt, record.CompletedAt = now, now
		j.records[key] = record
	}
	return CloneToolInvocationRecord(record), nil
}

func (j *memoryToolInvocationJournal) MarkToolInvocationUncertain(_ context.Context, invocation ToolInvocation, errorCode string) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	record, exists := j.records[invocationTestKey(invocation)]
	if !exists || !sameTestInvocation(record.ToolInvocation, invocation) {
		return errors.New("journal identity conflict")
	}
	if record.State != ToolInvocationCompleted {
		record.State, record.ErrorCode, record.UpdatedAt = ToolInvocationUncertain, errorCode, time.Now().UTC()
		j.records[invocationTestKey(invocation)] = record
	}
	return nil
}

type journalProbeTool struct {
	manifest CapabilityManifest
	calls    *atomic.Int32
	keys     *[]string
}

func (p journalProbeTool) Manifest() CapabilityManifest { return p.manifest }
func (p journalProbeTool) Execute(_ context.Context, request CapabilityRequest) (CapabilityResult, error) {
	p.calls.Add(1)
	if p.keys != nil {
		*p.keys = append(*p.keys, request.IdempotencyKey)
	}
	return CapabilityResult{Content: `{"side_effect":"committed"}`, OK: true}, nil
}

type journalCallAdapter struct{ call ToolCall }

func (journalCallAdapter) Provider() string { return "journal-test" }
func (a journalCallAdapter) Stream(_ context.Context, options GenerateOptions, emit func(StreamChunk)) error {
	last := options.Messages[len(options.Messages)-1]
	if last.Role == RoleTool {
		emit(StreamChunk{Kind: StreamKindAssistant, Text: "done"})
		emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishStop})
		return nil
	}
	call := a.call
	emit(StreamChunk{Kind: StreamKindAssistant, ToolCall: &call})
	emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishToolCalls})
	return nil
}

func journalRuntimeFixture(t *testing.T, journal ToolInvocationJournal, call ToolCall, idempotent bool, provider Capability) (*Runtime, Principal, func() *Session) {
	t.Helper()
	global, product, _, user := testScopes()
	_ = global
	principal := testPrincipal(user)
	registry := NewCapabilityRegistry()
	if provider.Manifest().Idempotent != idempotent {
		t.Fatalf("fixture manifest idempotent=%t, want %t", provider.Manifest().Idempotent, idempotent)
	}
	if err := registry.Register(product, provider); err != nil {
		t.Fatal(err)
	}
	profiles := NewAgentProfileRegistry()
	name := "Journal Agent"
	model := ModelSelection{Provider: "journal-test", Model: "journal-test"}
	if err := profiles.Bind(AgentProfileLayer{
		Scope: product, ProfileID: "journal.agent", Name: &name, Model: &model,
		AddCapabilities: []string{call.Name},
	}); err != nil {
		t.Fatal(err)
	}
	runtime := &Runtime{
		Capabilities: registry, Profiles: profiles, ToolJournal: journal,
		Models: ModelResolverFunc(func(context.Context, ModelSelection) (LlmAdapter, error) {
			return journalCallAdapter{call: call}, nil
		}),
	}
	newSession := func() *Session {
		scope, err := user.Child(ScopeRef{Kind: ScopeSession, ID: "session-journal-replay"})
		if err != nil {
			t.Fatal(err)
		}
		session, err := NewSession(SessionOptions{
			ID: "session-journal-replay", ProfileID: "journal.agent", Principal: principal, Scope: scope,
		})
		if err != nil {
			t.Fatal(err)
		}
		return session
	}
	return runtime, principal, newSession
}

func TestToolJournalReplaysCompletedSideEffectAfterSessionWriteFailure(t *testing.T) {
	journal := newMemoryToolInvocationJournal()
	var providerCalls atomic.Int32
	call := ToolCall{ID: "call-side-effect", Name: "payment.charge", Args: map[string]any{"amount": 42}}
	probe := journalProbeTool{manifest: func() CapabilityManifest {
		manifest := toolManifest(call.Name, "1.0.0")
		return manifest
	}(), calls: &providerCalls}
	runtime, principal, newSession := journalRuntimeFixture(t, journal, call, false, probe)
	first := newSession()
	_, err := runtime.RunTurn(context.Background(), principal, first, TurnInput{RunID: "run-journal-replay", Text: "charge"}, func(event SessionEvent) {
		if event.Type == EvToolResult {
			panic("session transport failed after provider completion")
		}
	})
	if err == nil {
		t.Fatal("first run unexpectedly survived the post-provider event failure")
	}
	if providerCalls.Load() != 1 {
		t.Fatalf("first run provider calls=%d", providerCalls.Load())
	}
	second := newSession()
	result, err := runtime.RunTurn(context.Background(), principal, second, TurnInput{RunID: "run-journal-replay", Text: "charge"}, nil)
	if err != nil || result.Status != RunCompleted {
		t.Fatalf("journal replay failed: result=%#v err=%v", result, err)
	}
	if providerCalls.Load() != 1 {
		t.Fatalf("completed side effect was replayed: provider calls=%d", providerCalls.Load())
	}
}

func TestToolJournalBlocksUnknownNonIdempotentOutcome(t *testing.T) {
	journal := newMemoryToolInvocationJournal()
	var providerCalls atomic.Int32
	call := ToolCall{ID: "call-unknown", Name: "payment.charge", Args: map[string]any{"amount": 42}}
	probe := journalProbeTool{manifest: toolManifest(call.Name, "1.0.0"), calls: &providerCalls}
	runtime, principal, newSession := journalRuntimeFixture(t, journal, call, false, probe)
	invocation, err := NewToolInvocation(RunInfo{
		RunID: "run-journal-unknown", SessionID: "session-journal-replay", Principal: principal,
	}, call, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, decision, err := journal.BeginToolInvocation(context.Background(), invocation); err != nil || decision != ToolInvocationExecuteNew {
		t.Fatalf("seed begin failed: decision=%q err=%v", decision, err)
	}
	session := newSession()
	result, err := runtime.RunTurn(context.Background(), principal, session, TurnInput{RunID: invocation.RunID, Text: "charge"}, nil)
	if err != nil || result.Status != RunCompleted {
		t.Fatalf("unknown outcome guard failed: result=%#v err=%v", result, err)
	}
	if providerCalls.Load() != 0 {
		t.Fatalf("unknown non-idempotent call reached provider: calls=%d", providerCalls.Load())
	}
	foundUnknown := false
	for _, event := range session.Events() {
		if event.Type != EvToolResult {
			continue
		}
		var data ToolResultData
		if jsonErr := json.Unmarshal(event.Data, &data); jsonErr == nil && data.Metadata["code"] == CodeToolOutcomeUnknown {
			foundUnknown = true
		}
	}
	if !foundUnknown {
		t.Fatal("unknown outcome was not recorded as a stable tool result")
	}
}

func TestIdempotentManifestPropagatesStableProviderKey(t *testing.T) {
	journal := newMemoryToolInvocationJournal()
	var providerCalls atomic.Int32
	keys := []string{}
	call := ToolCall{ID: "call-idempotent", Name: "catalog.lookup", Args: map[string]any{"id": "x"}}
	manifest := toolManifest(call.Name, "1.0.0")
	manifest.Idempotent = true
	probe := journalProbeTool{manifest: manifest, calls: &providerCalls, keys: &keys}
	runtime, principal, newSession := journalRuntimeFixture(t, journal, call, true, probe)
	result, err := runtime.RunTurn(context.Background(), principal, newSession(), TurnInput{RunID: "run-idempotent-key", Text: "lookup"}, nil)
	if err != nil || result.Status != RunCompleted {
		t.Fatalf("idempotent run failed: %#v err=%v", result, err)
	}
	if len(keys) != 1 || keys[0] != call.ID {
		t.Fatalf("provider idempotency keys=%#v", keys)
	}
}

func TestToolJournalCompletionFailureBecomesPermanentUnknownFence(t *testing.T) {
	journal := newMemoryToolInvocationJournal()
	journal.failComplete = true
	var providerCalls atomic.Int32
	call := ToolCall{ID: "call-commit-gap", Name: "payment.capture", Args: map[string]any{"amount": 7}}
	probe := journalProbeTool{manifest: toolManifest(call.Name, "1.0.0"), calls: &providerCalls}
	runtime, principal, newSession := journalRuntimeFixture(t, journal, call, false, probe)
	first := newSession()
	result, err := runtime.RunTurn(context.Background(), principal, first, TurnInput{RunID: "run-commit-gap", Text: "capture"}, nil)
	if err != nil || result.Status != RunCompleted {
		t.Fatalf("first uncertain run failed structurally: %#v err=%v", result, err)
	}
	if providerCalls.Load() != 1 {
		t.Fatalf("provider calls=%d, want 1", providerCalls.Load())
	}
	foundUnknown := false
	for _, event := range first.Events() {
		if event.Type != EvToolResult {
			continue
		}
		var data ToolResultData
		if json.Unmarshal(event.Data, &data) == nil && data.Metadata["code"] == CodeToolOutcomeUnknown {
			foundUnknown = true
		}
	}
	if !foundUnknown {
		t.Fatal("journal completion failure was not exposed as an unknown outcome")
	}
	journal.failComplete = false
	second := newSession()
	if _, err := runtime.RunTurn(context.Background(), principal, second, TurnInput{RunID: "run-commit-gap", Text: "capture"}, nil); err != nil {
		t.Fatal(err)
	}
	if providerCalls.Load() != 1 {
		t.Fatalf("unknown non-idempotent side effect was repeated: calls=%d", providerCalls.Load())
	}
}

func TestToolJournalBeginFailureIsFailClosed(t *testing.T) {
	journal := newMemoryToolInvocationJournal()
	journal.failBegin = true
	var providerCalls atomic.Int32
	call := ToolCall{ID: "call-journal-down", Name: "payment.refund", Args: map[string]any{"amount": 2}}
	probe := journalProbeTool{manifest: toolManifest(call.Name, "1.0.0"), calls: &providerCalls}
	runtime, principal, newSession := journalRuntimeFixture(t, journal, call, false, probe)
	session := newSession()
	if _, err := runtime.RunTurn(context.Background(), principal, session, TurnInput{RunID: "run-journal-down", Text: "refund"}, nil); err != nil {
		t.Fatal(err)
	}
	if providerCalls.Load() != 0 {
		t.Fatalf("provider ran while journal was unavailable: calls=%d", providerCalls.Load())
	}
	foundUnavailable := false
	for _, event := range session.Events() {
		if event.Type != EvToolResult {
			continue
		}
		var data ToolResultData
		if json.Unmarshal(event.Data, &data) == nil && data.Metadata["code"] == CodeToolJournalUnavailable {
			foundUnavailable = true
		}
	}
	if !foundUnavailable {
		t.Fatal("journal unavailability was not recorded as a stable denied result")
	}
}
