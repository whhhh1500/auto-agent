package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	. "github.com/cc-auto-agent/harness-core/pkg/core"
	"strings"
	"sync"
	"testing"
	"time"

	appcontextassembly "github.com/cc-auto-agent/harness-core/pkg/app/contextassembly"
)

func openRunEvents(t *testing.T, runID string) []SessionEvent {
	t.Helper()
	mustEvent := func(eventType SessionEventType, data any) SessionEvent {
		raw, err := json.Marshal(data)
		if err != nil {
			t.Fatal(err)
		}
		return SessionEvent{RunID: runID, Type: eventType, Data: raw}
	}
	return []SessionEvent{
		mustEvent(EvRunStart, RunStartData{}),
		mustEvent(EvUserMessage, UserMessageData{Text: "check"}),
		mustEvent(EvAssistantMessage, AssistantMessageData{Text: "", ToolCalls: []ToolCall{
			{ID: "call-lost", Name: "market.quote"},
		}}),
		mustEvent(EvToolCall, ToolCallData{CallID: "call-lost", Name: "market.quote"}),
		mustEvent(EvStepStart, map[string]int{"index": 0}),
	}
}

func TestRepairInterruptedBalancedLogYieldsNothing(t *testing.T) {
	runID := "run-a"
	events := []SessionEvent{
		{RunID: runID, Type: EvRunStart, Data: mustJSON(t, RunStartData{})},
		{RunID: runID, Type: EvUserMessage, Data: mustJSON(t, UserMessageData{Text: "hi"})},
		{RunID: runID, Type: EvRunEnd, Data: mustJSON(t, RunEndData{Status: RunCompleted})},
	}
	if synthetic := RepairInterrupted(events); synthetic != nil {
		t.Fatalf("balanced log must not be repaired: %#v", synthetic)
	}
}

func TestRepairInterruptedClosesOpenRun(t *testing.T) {
	events := openRunEvents(t, "run-x")
	synthetic := RepairInterrupted(events)
	if len(synthetic) == 0 {
		t.Fatal("expected synthetic closers")
	}
	types := []SessionEventType{}
	for _, event := range synthetic {
		types = append(types, event.Type)
		if event.RunID != "run-x" {
			t.Fatalf("synthetic event must target the open run: %#v", event)
		}
		if event.Seq != 0 || !event.Time.IsZero() {
			t.Fatalf("synthetic events must leave seq/time to Append: %#v", event)
		}
	}
	if types[0] != EvToolResult || types[len(types)-1] != EvRunEnd {
		t.Fatalf("unexpected closer order: %v", types)
	}
	if !containsEventType(types, EvRunError) || !containsEventType(types, EvStepEnd) {
		t.Fatalf("missing run/error or step/end closer: %v", types)
	}
	var result ToolResultData
	if err := json.Unmarshal(synthetic[0].Data, &result); err != nil {
		t.Fatal(err)
	}
	if result.CallID != "call-lost" || result.OK || result.Metadata["code"] != CodeToolOutcomeUnknown {
		t.Fatalf("in-flight call must get an unknown-outcome result: %#v", result)
	}
}

func TestRepairInterruptedNotStartedCall(t *testing.T) {
	runID := "run-x"
	events := []SessionEvent{
		{RunID: runID, Type: EvRunStart, Data: mustJSON(t, RunStartData{})},
		{RunID: runID, Type: EvUserMessage, Data: mustJSON(t, UserMessageData{Text: "hi"})},
		// Assistant requested a call but crashed before dispatching it.
		{RunID: runID, Type: EvAssistantMessage, Data: mustJSON(t, AssistantMessageData{Text: "", ToolCalls: []ToolCall{
			{ID: "call-ghost", Name: "market.quote"},
		}})},
	}
	synthetic := RepairInterrupted(events)
	if len(synthetic) == 0 {
		t.Fatal("expected synthetic closers")
	}
	var result ToolResultData
	if err := json.Unmarshal(synthetic[0].Data, &result); err != nil {
		t.Fatal(err)
	}
	if result.Metadata["code"] != CodeToolNotStarted {
		t.Fatalf("expected tool_not_started code, got %#v", result)
	}
}

func TestRepairAppliedOnFileStoreLoad(t *testing.T) {
	store, err := NewFileSessionStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	session := mustSession(t, user, principal)
	ctx := context.Background()
	if err := store.Create(ctx, session); err != nil {
		t.Fatal(err)
	}
	// Simulate a crash mid-run: append run/start + tool/call without closers.
	runID := "run-crash"
	if _, err := session.Append(runID, EvRunStart, RunStartData{}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append(runID, EvUserMessage, UserMessageData{Text: "hi"}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append(runID, EvToolCall, ToolCallData{CallID: "c1", Name: "x.tool"}); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(ctx, session, 0); err != nil {
		t.Fatal(err)
	}

	loaded, err := store.Load(ctx, session.ID())
	if err != nil {
		t.Fatal(err)
	}
	types := []SessionEventType{}
	for _, event := range loaded.Events() {
		types = append(types, event.Type)
	}
	last := types[len(types)-1]
	if last != EvRunEnd {
		t.Fatalf("loaded session must end with run/end after repair, got %v", types)
	}
	if !containsEventType(types, CodeRunInterruptedEventType()) {
		t.Fatalf("expected run/error closer: %v", types)
	}
}

func CodeRunInterruptedEventType() SessionEventType { return EvRunError }

func containsEventType(types []SessionEventType, target SessionEventType) bool {
	for _, eventType := range types {
		if eventType == target {
			return true
		}
	}
	return false
}

func TestRollingSummarizerArchivesPrefix(t *testing.T) {
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	session := mustSession(t, user, principal)
	runID := "run-a"
	for i := 0; i < 6; i++ {
		if _, err := session.Append(runID, EvUserMessage, UserMessageData{Text: "turn"}); err != nil {
			t.Fatal(err)
		}
		if _, err := session.Append(runID, EvAssistantMessage, AssistantMessageData{Text: "answer"}); err != nil {
			t.Fatal(err)
		}
	}
	messages, err := session.DeriveMessages()
	if err != nil {
		t.Fatal(err)
	}
	summarizer := &appcontextassembly.RollingSummarizer{
		MaxMessages: 4, KeepTail: 2,
		Summarizer: appcontextassembly.ContextSummarizerFunc(func(_ context.Context, archived []ChatMessage) (string, error) {
			return strings.Repeat("s", len(archived)), nil
		}),
	}
	emitCount := 0
	result, err := summarizer.EnsureSummarized(context.Background(), session, runID, func(SessionEvent) {
		emitCount++
	}, messages)
	if err != nil {
		t.Fatal(err)
	}
	if emitCount != 1 {
		t.Fatalf("expected exactly one summary event, got %d", emitCount)
	}
	// Projection is now summary + tail.
	if len(result) != 3 {
		t.Fatalf("expected summary + 2 tail messages, got %d: %#v", len(result), result)
	}
	if !strings.HasPrefix(result[0].Content, "[conversation summary of events 0–") {
		t.Fatalf("summary message malformed: %q", result[0].Content)
	}
	// Archived range shadowed: replaying from the log reproduces the same view.
	replayed, err := session.DeriveMessages()
	if err != nil {
		t.Fatal(err)
	}
	if len(replayed) != len(result) {
		t.Fatalf("projection not stable across replay: %d vs %d", len(replayed), len(result))
	}
	// Second call with the archived projection is a no-op.
	before := session.Version()
	again, err := summarizer.EnsureSummarized(context.Background(), session, runID, nil, result)
	if err != nil {
		t.Fatal(err)
	}
	if session.Version() != before {
		t.Fatalf("second call appended another summary: %#v", again)
	}
}

func TestLlmSummarizerUsesAdapter(t *testing.T) {
	summarizer := appcontextassembly.LlmSummarizer{Adapter: scriptedSummaryAdapter{}}
	text, err := summarizer.Summarize(context.Background(), []ChatMessage{
		{Role: RoleUser, Content: "hello"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if text != "summary-from-model" {
		t.Fatalf("unexpected summary: %q", text)
	}
	if _, err := (appcontextassembly.LlmSummarizer{}).Summarize(context.Background(), nil); err == nil {
		t.Fatal("nil adapter must error")
	}
}

type scriptedSummaryAdapter struct{}

func (scriptedSummaryAdapter) Provider() string { return "scripted" }

func (scriptedSummaryAdapter) Stream(_ context.Context, _ GenerateOptions, emit func(StreamChunk)) error {
	emit(StreamChunk{Kind: "assistant", Text: "summary-from-model"})
	emit(StreamChunk{Kind: "finish", FinishKind: "stop"})
	return nil
}

func TestAgentLoopWritesSummaryEvent(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	registry := NewCapabilityRegistry()
	if err := registry.Register(product, staticTool{manifest: toolManifest("market.quote", "1.0.0"), content: "42"}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := (CapabilityResolver{Registry: registry}).Resolve(principal, user)
	if err != nil {
		t.Fatal(err)
	}
	// The scripted model answers immediately without tools, so the run has
	// one step; the summarizer sees the projection with prior turns.
	session := mustSession(t, user, principal)
	runID := "run-warm"
	for i := 0; i < 4; i++ {
		if _, err := session.Append(runID, EvUserMessage, UserMessageData{Text: "t"}); err != nil {
			t.Fatal(err)
		}
		if _, err := session.Append(runID, EvAssistantMessage, AssistantMessageData{Text: "a"}); err != nil {
			t.Fatal(err)
		}
	}
	llm := &scriptedAdapter{steps: []scriptedTurn{{text: "final"}}}
	agent, err := NewAgent(AgentOptions{
		LLM: llm, Tools: snapshot, Session: session,
		Summarizer: &appcontextassembly.RollingSummarizer{
			MaxMessages: 4, KeepTail: 2,
			Summarizer: appcontextassembly.ContextSummarizerFunc(func(_ context.Context, archived []ChatMessage) (string, error) {
				return "archived", nil
			}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := agent.RunTurn(context.Background(), TurnInput{RunID: "run-live", Text: "go"}); err != nil {
		t.Fatal(err)
	}
	sawSummary := false
	for _, event := range session.Events() {
		if event.Type == EvContextSummary {
			sawSummary = true
			var data ContextSummaryData
			if err := json.Unmarshal(event.Data, &data); err != nil {
				t.Fatal(err)
			}
			if data.Op != "replace" || data.End < data.Start {
				t.Fatalf("malformed summary data: %#v", data)
			}
		}
	}
	if !sawSummary {
		t.Fatal("agent loop did not archive the prefix")
	}
}

func TestWriteBehindBatchesAndFlushes(t *testing.T) {
	store, err := NewFileSessionStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	session := mustSession(t, user, principal)
	ctx := context.Background()
	if err := store.Create(ctx, session); err != nil {
		t.Fatal(err)
	}

	writer := NewWriteBehind(store, session, 0, 30*time.Millisecond)
	defer writer.Flush(ctx)
	if _, err := session.Append("run-a", EvUserMessage, UserMessageData{Text: "one"}); err != nil {
		t.Fatal(err)
	}
	writer.MarkDirty()
	// Not durable immediately (batching window open) — tolerate either state
	// on slow machines, then require durability after Flush.
	if _, err := session.Append("run-a", EvRunEnd, RunEndData{Status: RunCompleted}); err != nil {
		t.Fatal(err)
	}
	writer.MarkDirty()
	if err := writer.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if writer.SavedVersion() != session.Version() {
		t.Fatalf("flush did not reach the tip: %d vs %d", writer.SavedVersion(), session.Version())
	}
	loaded, err := store.Load(ctx, session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Version() != session.Version() {
		t.Fatalf("store missing events after flush: %d vs %d", loaded.Version(), session.Version())
	}
}

func TestWriteBehindCheckpointKeepsBackgroundCheckpointingActive(t *testing.T) {
	store := NewMemorySessionStore()
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	session := mustSession(t, user, principal)
	ctx := context.Background()
	if err := store.Create(ctx, session); err != nil {
		t.Fatal(err)
	}

	writer := NewWriteBehind(store, session, 0, time.Millisecond)
	if _, err := session.Append("run-a", EvUserMessage, UserMessageData{Text: "before checkpoint"}); err != nil {
		t.Fatal(err)
	}
	writer.MarkDirty()
	if err := writer.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}

	if _, err := session.Append("run-a", EvAssistantMessage, AssistantMessageData{Text: "after checkpoint"}); err != nil {
		t.Fatal(err)
	}
	writer.MarkDirty()
	deadline := time.Now().Add(time.Second)
	for writer.SavedVersion() != session.Version() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if writer.SavedVersion() != session.Version() {
		t.Fatalf("background batching stopped after Checkpoint: saved=%d session=%d", writer.SavedVersion(), session.Version())
	}
	loaded, err := store.Load(ctx, session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Version() != session.Version() {
		t.Fatalf("store stopped at version %d; want %d", loaded.Version(), session.Version())
	}
}

func TestWriteBehindCheckpointWaitsForBackgroundFlushAndReschedules(t *testing.T) {
	store := newBlockingSnapshotStore()
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	session := mustSession(t, user, principal)
	ctx := context.Background()
	if err := store.Create(ctx, session); err != nil {
		t.Fatal(err)
	}

	writer := NewWriteBehind(store, session, 0, time.Millisecond)
	if _, err := session.Append("run-a", EvUserMessage, UserMessageData{Text: "one"}); err != nil {
		t.Fatal(err)
	}
	writer.MarkDirty()
	waitForSignal(t, store.firstSaveStarted, "background save to start")

	if _, err := session.Append("run-a", EvAssistantMessage, AssistantMessageData{Text: "two"}); err != nil {
		t.Fatal(err)
	}
	writer.MarkDirty()
	checkpointResult := make(chan error, 1)
	go func() { checkpointResult <- writer.Checkpoint(ctx) }()
	select {
	case err := <-checkpointResult:
		t.Fatalf("Checkpoint returned before the in-flight save completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	close(store.releaseFirstSave)
	select {
	case err := <-checkpointResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Checkpoint did not finish after the background save was released")
	}
	if got := store.SaveCalls(); got != 2 {
		t.Fatalf("Checkpoint did not drain the event appended during background flush: saves=%d", got)
	}
	if err := writer.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}
	if got := store.SaveCalls(); got != 2 {
		t.Fatalf("repeat Checkpoint wrote an already durable version: saves=%d", got)
	}

	if _, err := session.Append("run-a", EvAssistantMessage, AssistantMessageData{Text: "three"}); err != nil {
		t.Fatal(err)
	}
	writer.MarkDirty()
	deadline := time.Now().Add(time.Second)
	for writer.SavedVersion() != session.Version() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if writer.SavedVersion() != session.Version() {
		t.Fatalf("background scheduling did not resume after Checkpoint: saved=%d session=%d", writer.SavedVersion(), session.Version())
	}
	if got := store.SaveCalls(); got != 3 {
		t.Fatalf("resumed background scheduling saved %d times; want 3", got)
	}
}

func TestWriteBehindFlushStopsBackgroundCheckpointing(t *testing.T) {
	store := NewMemorySessionStore()
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	session := mustSession(t, user, principal)
	ctx := context.Background()
	if err := store.Create(ctx, session); err != nil {
		t.Fatal(err)
	}

	writer := NewWriteBehind(store, session, 0, 10*time.Millisecond)
	if _, err := session.Append("run-a", EvUserMessage, UserMessageData{Text: "terminal"}); err != nil {
		t.Fatal(err)
	}
	writer.MarkDirty()
	if err := writer.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append("run-a", EvAssistantMessage, AssistantMessageData{Text: "after close"}); err != nil {
		t.Fatal(err)
	}
	writer.MarkDirty()
	if err := writer.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)

	loaded, err := store.Load(ctx, session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Version() != 1 || writer.SavedVersion() != 1 {
		t.Fatalf("closed writer performed delayed persistence: store=%d saved=%d", loaded.Version(), writer.SavedVersion())
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if !writer.closed || writer.timer != nil {
		t.Fatalf("Flush did not close scheduling: closed=%t timer=%v", writer.closed, writer.timer)
	}
}

func TestWriteBehindReportsConflict(t *testing.T) {
	memory := NewMemorySessionStore()
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	session := mustSession(t, user, principal)
	ctx := context.Background()
	if err := memory.Create(ctx, session); err != nil {
		t.Fatal(err)
	}
	writer := NewWriteBehind(memory, session, 0, time.Hour) // long window: only Flush saves
	if _, err := session.Append("run-a", EvUserMessage, UserMessageData{Text: "one"}); err != nil {
		t.Fatal(err)
	}
	writer.MarkDirty()
	// An external writer advances the store behind our back.
	if _, err := session.Append("run-a", EvRunEnd, RunEndData{Status: RunCompleted}); err != nil {
		t.Fatal(err)
	}
	if err := memory.Save(ctx, session, 0); err != nil {
		t.Fatal(err)
	}
	// Now our savedVersion is stale relative to the store's tip.
	if err := writer.Flush(ctx); err == nil {
		t.Fatal("expected flush failure after external writer moved the store")
	} else if !IsConflict(err) {
		t.Fatalf("expected conflict, got %v", err)
	}
}

func TestWriteBehindPersistsEventsAppendedDuringBackgroundFlush(t *testing.T) {
	store := newBlockingSnapshotStore()
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	session := mustSession(t, user, principal)
	ctx := context.Background()
	if err := store.Create(ctx, session); err != nil {
		t.Fatal(err)
	}

	writer := NewWriteBehind(store, session, 0, time.Millisecond)
	if _, err := session.Append("run-a", EvUserMessage, UserMessageData{Text: "one"}); err != nil {
		t.Fatal(err)
	}
	writer.MarkDirty()
	waitForSignal(t, store.firstSaveStarted, "background save to start")

	// The store has already copied version 1. Add another event while that
	// stable prefix is blocked in Save; the first save must not claim it.
	if _, err := session.Append("run-a", EvRunEnd, RunEndData{Status: RunCompleted}); err != nil {
		t.Fatal(err)
	}
	writer.MarkDirty()

	flushResult := make(chan error, 1)
	go func() { flushResult <- writer.Flush(ctx) }()
	select {
	case err := <-flushResult:
		t.Fatalf("Flush returned before the in-flight save completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(store.releaseFirstSave)
	select {
	case err := <-flushResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Flush did not finish after the background save was released")
	}

	if got := store.SaveCalls(); got != 2 {
		t.Fatalf("expected two stable-prefix saves, got %d", got)
	}
	if writer.SavedVersion() != session.Version() {
		t.Fatalf("saved version %d does not match session version %d", writer.SavedVersion(), session.Version())
	}
	loaded, err := store.Load(ctx, session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Version() != session.Version() {
		t.Fatalf("store stopped at version %d; want %d", loaded.Version(), session.Version())
	}
}

func TestWriteBehindFlushCanCancelWhileBackgroundFlushIsRunning(t *testing.T) {
	store := newBlockingSnapshotStore()
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	session := mustSession(t, user, principal)
	if err := store.Create(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	writer := NewWriteBehind(store, session, 0, time.Millisecond)
	if _, err := session.Append("run-a", EvUserMessage, UserMessageData{Text: "one"}); err != nil {
		t.Fatal(err)
	}
	writer.MarkDirty()
	waitForSignal(t, store.firstSaveStarted, "background save to start")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	flushResult := make(chan error, 1)
	go func() { flushResult <- writer.Flush(ctx) }()

	select {
	case err := <-flushResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected canceled Flush, got %v", err)
		}
	case <-time.After(time.Second):
		close(store.releaseFirstSave)
		waitForSignal(t, store.firstSaveFinished, "background save to finish")
		<-flushResult
		t.Fatal("Flush did not observe cancellation while waiting for the in-flight save")
	}

	close(store.releaseFirstSave)
	waitForSignal(t, store.firstSaveFinished, "background save to finish")
}

func TestWriteBehindNegativeDelayDisablesBackgroundFlush(t *testing.T) {
	store := NewMemorySessionStore()
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	session := mustSession(t, user, principal)
	ctx := context.Background()
	if err := store.Create(ctx, session); err != nil {
		t.Fatal(err)
	}

	writer := NewWriteBehind(store, session, 0, -time.Millisecond)
	if _, err := session.Append("run-a", EvUserMessage, UserMessageData{Text: "one"}); err != nil {
		t.Fatal(err)
	}
	writer.MarkDirty()

	writer.mu.Lock()
	timerScheduled := writer.timer != nil
	backgroundDisabled := writer.backgroundDisabled
	writer.mu.Unlock()
	if !backgroundDisabled || timerScheduled {
		t.Fatalf("negative delay must disable background batching: disabled=%t timer=%t", backgroundDisabled, timerScheduled)
	}
	loaded, err := store.Load(ctx, session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Version() != 0 {
		t.Fatalf("event was saved before final Flush: version %d", loaded.Version())
	}

	if err := writer.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	loaded, err = store.Load(ctx, session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Version() != session.Version() {
		t.Fatalf("final Flush saved version %d; want %d", loaded.Version(), session.Version())
	}
}

func TestWriteBehindUsesIncrementalAppender(t *testing.T) {
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	session := mustSession(t, user, principal)
	store := &recordingAppender{}
	if _, err := session.Append("run-a", EvUserMessage, UserMessageData{Text: "hello"}); err != nil {
		t.Fatal(err)
	}
	writer := NewWriteBehind(store, session, 0, -1)
	writer.MarkDirty()
	if err := writer.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if store.saveCalled {
		t.Fatal("write-behind used full snapshot Save despite incremental appender support")
	}
	if store.sessionID != session.ID() || store.expectedVersion != 0 || len(store.events) != 1 {
		t.Fatalf("unexpected incremental append: %#v", store)
	}
}

func TestWriteBehindFinalFlushDetectsUnmarkedEvents(t *testing.T) {
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	session := mustSession(t, user, principal)
	store := NewMemorySessionStore()
	if err := store.Create(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append("run-a", EvUserMessage, UserMessageData{Text: "not marked dirty"}); err != nil {
		t.Fatal(err)
	}
	writer := NewWriteBehind(store, session, 0, -1)
	if err := writer.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load(context.Background(), session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Version() != 1 {
		t.Fatalf("final flush lost unmarked events: version=%d", loaded.Version())
	}
}

func TestWriteBehindBackgroundFlushHasDeadline(t *testing.T) {
	oldTimeout := backgroundFlushTimeout
	backgroundFlushTimeout = 20 * time.Millisecond
	t.Cleanup(func() { backgroundFlushTimeout = oldTimeout })
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	session := mustSession(t, user, principal)
	store := &deadlineObservingStore{seen: make(chan bool, 1)}
	if _, err := session.Append("run-a", EvUserMessage, UserMessageData{Text: "hello"}); err != nil {
		t.Fatal(err)
	}
	writer := NewWriteBehind(store, session, 0, time.Millisecond)
	writer.MarkDirty()
	select {
	case hasDeadline := <-store.seen:
		if !hasDeadline {
			t.Fatal("background flush context had no deadline")
		}
	case <-time.After(time.Second):
		t.Fatal("background flush did not run")
	}
}

type deadlineObservingStore struct{ seen chan bool }

func (*deadlineObservingStore) Create(context.Context, *Session) error { return nil }
func (*deadlineObservingStore) Load(context.Context, string) (*Session, error) {
	return nil, ErrSessionNotFound
}
func (s *deadlineObservingStore) Save(ctx context.Context, _ *Session, _ int64) error {
	_, hasDeadline := ctx.Deadline()
	s.seen <- hasDeadline
	return nil
}

type recordingAppender struct {
	saveCalled      bool
	sessionID       string
	expectedVersion int64
	events          []SessionEvent
}

func (*recordingAppender) Create(context.Context, *Session) error { return nil }
func (*recordingAppender) Load(context.Context, string) (*Session, error) {
	return nil, ErrSessionNotFound
}
func (s *recordingAppender) Save(context.Context, *Session, int64) error {
	s.saveCalled = true
	return nil
}
func (s *recordingAppender) AppendEvents(_ context.Context, sessionID string, expectedVersion int64, events []SessionEvent) error {
	s.sessionID = sessionID
	s.expectedVersion = expectedVersion
	s.events = append([]SessionEvent(nil), events...)
	return nil
}

type blockingSnapshotStore struct {
	mu                sync.Mutex
	stored            *Session
	saveCalls         int
	firstSaveStarted  chan struct{}
	releaseFirstSave  chan struct{}
	firstSaveFinished chan struct{}
}

func newBlockingSnapshotStore() *blockingSnapshotStore {
	return &blockingSnapshotStore{
		firstSaveStarted:  make(chan struct{}),
		releaseFirstSave:  make(chan struct{}),
		firstSaveFinished: make(chan struct{}),
	}
}

func (s *blockingSnapshotStore) Create(_ context.Context, session *Session) error {
	copyOf, err := session.Clone()
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stored != nil {
		return fmt.Errorf("%w: %s", ErrSessionConflict, session.ID())
	}
	s.stored = copyOf
	return nil
}

func (s *blockingSnapshotStore) Load(_ context.Context, id string) (*Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stored == nil || s.stored.ID() != id {
		return nil, fmt.Errorf("%w: %s", ErrSessionNotFound, id)
	}
	return s.stored.Clone()
}

func (s *blockingSnapshotStore) Save(ctx context.Context, session *Session, expectedVersion int64) error {
	copyOf, err := session.Clone()
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.saveCalls++
	call := s.saveCalls
	s.mu.Unlock()

	if call == 1 {
		close(s.firstSaveStarted)
		defer close(s.firstSaveFinished)
		select {
		case <-s.releaseFirstSave:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stored == nil {
		return fmt.Errorf("%w: %s", ErrSessionNotFound, session.ID())
	}
	if s.stored.Version() != expectedVersion {
		return fmt.Errorf("%w: expected %d, found %d", ErrSessionConflict, expectedVersion, s.stored.Version())
	}
	s.stored = copyOf
	return nil
}

func (s *blockingSnapshotStore) SaveCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saveCalls
}

func waitForSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func mustJSON(t *testing.T, data any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestFileSessionStoreRejectsOverflow(t *testing.T) {
	store, err := NewFileSessionStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	store.maxSessions = 2
	ctx := context.Background()
	if err := store.Create(ctx, mustNamedSession(t, "session-file-1")); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(ctx, mustNamedSession(t, "session-file-2")); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(ctx, mustNamedSession(t, "session-file-3")); err == nil {
		t.Fatal("file session overflow was accepted")
	} else if !strings.Contains(err.Error(), "stored sessions exceed maximum of 2") {
		t.Fatalf("unexpected overflow error: %v", err)
	}
	if err := store.Create(ctx, mustNamedSession(t, "session-file-1")); !errors.Is(err, ErrSessionConflict) {
		t.Fatalf("existing file session must still conflict: %v", err)
	}
}

func TestFileSessionStoreRejectsFileSizeOverflow(t *testing.T) {
	store, err := NewFileSessionStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	store.maxFileBytes = 2048
	ctx := context.Background()
	session := mustNamedSession(t, "session-file-size")
	if err := store.Create(ctx, session); err != nil {
		t.Fatal(err)
	}
	overflowed := false
	for i := 0; i < 32; i++ {
		if _, err := session.Append("run-a", EvUserMessage, UserMessageData{Text: "payload-" + strings.Repeat("x", 40)}); err != nil {
			t.Fatal(err)
		}
		if err := store.Save(ctx, session, session.Version()-1); err != nil {
			if !strings.Contains(err.Error(), "session file exceeds maximum") {
				t.Fatalf("unexpected save error: %v", err)
			}
			overflowed = true
			break
		}
	}
	if !overflowed {
		t.Fatal("file session size overflow was accepted")
	}
}
