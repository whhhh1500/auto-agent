package core

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestScopeRejectsReservedPathDelimiter(t *testing.T) {
	if _, err := NewScopePath(ScopeRef{Kind: ScopeGlobal, ID: "global/escape"}); err == nil {
		t.Fatal("scope id containing slash must be rejected")
	}
}

func TestSummaryProjectionHandlesFutureAndMaxIntRanges(t *testing.T) {
	session := contextTestSession(t, "summary-future-ranges")
	maxInt := int64(^uint64(0) >> 1)
	for _, data := range []ContextSummaryData{
		{Op: "replace", Start: maxInt - 1, End: maxInt, Summary: "future-a"},
		{Op: "replace", Start: maxInt, End: maxInt, Summary: "future-b"},
	} {
		if _, err := session.Append("run", EvContextSummary, data); err != nil {
			t.Fatal(err)
		}
	}
	messages, err := session.DeriveMessages()
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 2 {
		t.Fatalf("future summaries lost: %#v", messages)
	}
}

func TestSessionRejectsDuplicateAndMalformedRunIDs(t *testing.T) {
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	session := mustSession(t, user, principal)
	if _, err := session.Append("run-a", EvRunStart, RunStartData{}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append("run-a", EvRunStart, RunStartData{}); err == nil {
		t.Fatal("duplicate run id was accepted")
	}
	if _, err := session.Append("bad/run", EvRunStart, RunStartData{}); err == nil {
		t.Fatal("malformed run id was accepted")
	}
}

func TestRunResumeRequiresStartAndValidCompositionMetadata(t *testing.T) {
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	session := mustSession(t, user, principal)
	composition := func(metadata map[string]string) *RunCompositionData {
		return &RunCompositionData{
			Profile: AgentProfileSnapshot{ProfileID: "product.agent", Scope: user}, Metadata: metadata,
		}
	}
	if _, err := session.Append("run-resume-order", EvRunResume, RunResumeData{}); err == nil {
		t.Fatal("run/resume before run/start was accepted")
	}
	invalid := composition(map[string]string{"bad\x00key": "value"})
	if _, err := session.Append("run-resume-order", EvRunStart, RunStartData{Composition: invalid}); err == nil {
		t.Fatal("invalid run/start composition metadata was accepted")
	}
	if _, err := session.Append("run-resume-order", EvRunStart, RunStartData{
		Composition: composition(map[string]string{"segment": "initial"}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append("run-resume-order", EvRunResume, RunResumeData{
		Composition: composition(map[string]string{"segment": strings.Repeat("x", MaxRunCompositionMetadataValueBytes+1)}),
	}); err == nil {
		t.Fatal("invalid run/resume composition metadata was accepted")
	}
	if _, err := session.Append("run-resume-order", EvRunResume, RunResumeData{
		Composition: composition(map[string]string{"segment": "resume"}),
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRestoreSessionRejectsRunResumeBeforeStart(t *testing.T) {
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	scope, _ := user.Child(ScopeRef{Kind: ScopeSession, ID: "session-resume-order"})
	_, err := RestoreSession(SessionOptions{
		ID: "session-resume-order", ProfileID: "product.agent", Principal: principal, Scope: scope,
	}, []SessionEvent{{
		Seq: 0, RunID: "run-resume-order", Type: EvRunResume, Data: mustJSON(t, RunResumeData{}),
	}})
	if err == nil {
		t.Fatal("restored run/resume before run/start")
	}
}

func TestSessionRejectsOversizedEventPayload(t *testing.T) {
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	session := mustSession(t, user, principal)
	if _, err := session.Append("run-large", EvUserMessage, UserMessageData{
		Text: strings.Repeat("x", MaxSessionEventDataBytes+1),
	}); err == nil {
		t.Fatal("oversized event payload was accepted")
	}
}

func TestSessionConstructorValidatesIdentityAndScopeConsistency(t *testing.T) {
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	wrongScope, _ := user.Child(ScopeRef{Kind: ScopeSession, ID: "other-session"})
	if _, err := NewSession(SessionOptions{ID: "session-a", ProfileID: "product.agent", Principal: principal, Scope: wrongScope}); err == nil {
		t.Fatal("session id/scope mismatch was accepted")
	}
	validScope, _ := user.Child(ScopeRef{Kind: ScopeSession, ID: "session-a"})
	missingIdentity := principal
	missingIdentity.SubjectID = ""
	if _, err := NewSession(SessionOptions{ID: "session-a", ProfileID: "product.agent", Principal: missingIdentity, Scope: validScope}); err == nil {
		t.Fatal("incomplete principal identity was accepted")
	}
	if _, err := NewID("bad/prefix"); err == nil {
		t.Fatal("unsafe id prefix was accepted")
	}
}

func TestRestoreSessionRejectsCorruptCoreEvents(t *testing.T) {
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	scope, _ := user.Child(ScopeRef{Kind: ScopeSession, ID: "session-corrupt"})
	options := SessionOptions{ID: "session-corrupt", ProfileID: "product.agent", Principal: principal, Scope: scope}
	tests := []struct {
		name  string
		event SessionEvent
	}{
		{name: "unknown-type", event: SessionEvent{RunID: "run-a", Type: SessionEventType("future/unknown"), Data: json.RawMessage(`{}`)}},
		{name: "invalid-status", event: SessionEvent{RunID: "run-a", Type: EvRunEnd, Data: mustJSON(t, RunEndData{Status: "mystery"})}},
		{name: "negative-usage", event: SessionEvent{RunID: "run-a", Type: EvRunUsage, Data: mustJSON(t, RunUsageData{InputTokens: -1})}},
		{name: "bad-json", event: SessionEvent{RunID: "run-a", Type: EvUserMessage, Data: json.RawMessage(`{`)}},
		{name: "mismatched-tool-fields", event: SessionEvent{RunID: "run-a", Type: EvAssistantMessage, Data: mustJSON(t, AssistantMessageData{
			ToolCall:  &ToolCall{ID: "call-1", Name: "tool.one"},
			ToolCalls: []ToolCall{{ID: "call-2", Name: "tool.two"}},
		})}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.event.Seq = 0
			if _, err := RestoreSession(options, []SessionEvent{test.event}); err == nil {
				t.Fatal("corrupt event was accepted")
			}
		})
	}
}

func TestRuntimeErrorMessageIsBounded(t *testing.T) {
	data := NewRuntimeErrorData("failure", errors.New(strings.Repeat("x", MaxRuntimeErrorMessageBytes+100)), false)
	if len(data.Message) > MaxRuntimeErrorMessageBytes+len("...[truncated]") || !strings.HasSuffix(data.Message, "...[truncated]") {
		t.Fatalf("runtime error was not bounded: %d bytes", len(data.Message))
	}
}

func TestDerivedMessagesAreDeepCopies(t *testing.T) {
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	session := mustSession(t, user, principal)
	call := ToolCall{ID: "call-1", Name: "tool.one", Args: map[string]any{"symbol": "BTC"}}
	if _, err := session.Append("run-copy", EvAssistantMessage, AssistantMessageData{ToolCall: &call, ToolCalls: []ToolCall{call}}); err != nil {
		t.Fatal(err)
	}
	first, err := session.DeriveMessages()
	if err != nil {
		t.Fatal(err)
	}
	first[0].ToolCall.Args["symbol"] = "MUTATED"
	first[0].ToolCalls[0].Args["symbol"] = "MUTATED"
	second, err := session.DeriveMessages()
	if err != nil {
		t.Fatal(err)
	}
	if second[0].ToolCall.Args["symbol"] != "BTC" || second[0].ToolCalls[0].Args["symbol"] != "BTC" {
		t.Fatalf("caller mutation corrupted projection cache: %#v", second[0])
	}
}

func TestIncrementalDerivedMessagesAreDeepCopies(t *testing.T) {
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	session := mustSession(t, user, principal)
	call := ToolCall{ID: "call-incremental", Name: "tool.one", Args: map[string]any{"symbol": "BTC"}}
	if _, err := session.Append("run-incremental", EvAssistantMessage, AssistantMessageData{ToolCall: &call, ToolCalls: []ToolCall{call}}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.DeriveMessages(); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append("run-incremental", EvUserMessage, UserMessageData{Text: "next request"}); err != nil {
		t.Fatal(err)
	}
	first, err := session.DeriveMessages()
	if err != nil {
		t.Fatal(err)
	}
	first[0].ToolCall.Args["symbol"] = "MUTATED"
	first[0].ToolCalls[0].Args["symbol"] = "MUTATED"
	second, err := session.DeriveMessages()
	if err != nil {
		t.Fatal(err)
	}
	if second[0].ToolCall.Args["symbol"] != "BTC" || second[0].ToolCalls[0].Args["symbol"] != "BTC" {
		t.Fatalf("caller mutation corrupted incremental projection cache: %#v", second[0])
	}
}

func TestDerivedMessagesRebuildsAfterAppendedSummary(t *testing.T) {
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	session := mustSession(t, user, principal)
	if _, err := session.Append("run-summary", EvUserMessage, UserMessageData{Text: "old request"}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append("run-summary", EvAssistantMessage, AssistantMessageData{Text: "old response"}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.DeriveMessages(); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append("run-summary", EvContextSummary, ContextSummaryData{
		Op: "replace", Start: 0, End: 1, Summary: "archived request and response",
	}); err != nil {
		t.Fatal(err)
	}
	messages, err := session.DeriveMessages()
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 || !strings.Contains(messages[0].Content, "archived request and response") {
		t.Fatalf("summary did not invalidate cached projection: %#v", messages)
	}
}

func mustJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func TestSessionRoundTripAndOptimisticStore(t *testing.T) {
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	sessionScope, _ := user.Child(ScopeRef{Kind: ScopeSession, ID: "session-a"})
	session, err := NewSession(SessionOptions{
		ID: "session-a", ProfileID: "product.agent", Principal: principal, Scope: sessionScope,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append("run-a", EvUserMessage, UserMessageData{Text: "hello"}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append("run-a", EvAssistantMessage, AssistantMessageData{Text: "world"}); err != nil {
		t.Fatal(err)
	}

	encoded, err := json.Marshal(session.Events())
	if err != nil {
		t.Fatal(err)
	}
	var events []SessionEvent
	if err := json.Unmarshal(encoded, &events); err != nil {
		t.Fatal(err)
	}
	restored, err := RestoreSession(SessionOptions{
		ID: session.ID(), ProfileID: session.ProfileID(), Principal: principal, Scope: sessionScope,
	}, events)
	if err != nil {
		t.Fatal(err)
	}
	messages, err := restored.DeriveMessages()
	if err != nil || len(messages) != 2 || messages[1].Content != "world" {
		t.Fatalf("unexpected restored messages: %#v, %v", messages, err)
	}

	store := NewMemorySessionStore()
	if err := store.Create(context.Background(), restored); err != nil {
		t.Fatal(err)
	}
	left, _ := store.Load(context.Background(), restored.ID())
	right, _ := store.Load(context.Background(), restored.ID())
	leftVersion := left.Version()
	_, _ = left.Append("run-b", EvUserMessage, UserMessageData{Text: "left"})
	if err := store.Save(context.Background(), left, leftVersion); err != nil {
		t.Fatal(err)
	}
	rightVersion := right.Version()
	_, _ = right.Append("run-c", EvUserMessage, UserMessageData{Text: "right"})
	if err := store.Save(context.Background(), right, rightVersion); !errors.Is(err, ErrSessionConflict) {
		t.Fatalf("expected version conflict, got %v", err)
	}
}

func testMemorySession(t *testing.T, id string) *Session {
	t.Helper()
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	sessionScope, err := user.Child(ScopeRef{Kind: ScopeSession, ID: id})
	if err != nil {
		t.Fatal(err)
	}
	session, err := NewSession(SessionOptions{
		ID: id, ProfileID: "product.agent", Principal: principal, Scope: sessionScope,
	})
	if err != nil {
		t.Fatal(err)
	}
	return session
}

func TestMemorySessionStoreRejectsOverflow(t *testing.T) {
	store := NewMemorySessionStore()
	store.maxSessions = 2
	ctx := context.Background()
	first := testMemorySession(t, "session-mem-1")
	if err := store.Create(ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(ctx, testMemorySession(t, "session-mem-2")); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(ctx, testMemorySession(t, "session-mem-3")); err == nil {
		t.Fatal("memory session overflow was accepted")
	} else if !strings.Contains(err.Error(), "memory sessions exceed maximum of 2") {
		t.Fatalf("unexpected overflow error: %v", err)
	}
	if err := store.Create(ctx, first); !errors.Is(err, ErrSessionConflict) {
		t.Fatalf("existing memory session must still conflict: %v", err)
	}
	loaded, err := store.Load(ctx, first.ID())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(ctx, loaded, loaded.Version()); err != nil {
		t.Fatalf("existing session save was blocked by the cap: %v", err)
	}
}

func TestSessionRejectsEventOverflow(t *testing.T) {
	session := testMemorySession(t, "session-event-cap")
	session.maxEvents = 2
	if _, err := session.Append("run-a", EvUserMessage, UserMessageData{Text: "one"}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append("run-a", EvUserMessage, UserMessageData{Text: "two"}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append("run-a", EvUserMessage, UserMessageData{Text: "three"}); err == nil {
		t.Fatal("session event overflow was accepted")
	}
}
