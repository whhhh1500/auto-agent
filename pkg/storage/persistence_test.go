package storage

import (
	"context"
	"errors"
	. "github.com/cc-auto-agent/harness-core/pkg/core"
	"strings"
	"testing"
)

func TestRecentTurnsCompactorPrunesOldToolResults(t *testing.T) {
	compactor := RecentTurnsCompactor{MaxToolResultChars: 10}
	messages := []ChatMessage{
		{Role: RoleUser, Content: "turn one"},
		{Role: RoleAssistant, Content: "", ToolCall: &ToolCall{ID: "c1", Name: "search"}},
		{Role: RoleTool, ToolCallID: "c1", Content: strings.Repeat("x", 500)},
		{Role: RoleUser, Content: "turn two"},
	}
	compacted := compactor.Compact(messages)
	if len(compacted) != 4 {
		t.Fatalf("compaction must not drop messages when only pruning: %d", len(compacted))
	}
	if !strings.HasPrefix(compacted[2].Content, ElidedToolResultMarker) {
		t.Fatalf("old tool result not pruned: %q", compacted[2].Content)
	}
	if compacted[2].ToolCallID != "c1" {
		t.Fatal("tool result pairing lost")
	}
}

func TestRecentTurnsCompactorDropsWholeTurns(t *testing.T) {
	compactor := RecentTurnsCompactor{MaxMessages: 5}
	messages := []ChatMessage{
		{Role: RoleUser, Content: "t1"},
		{Role: RoleAssistant, Content: "a1"},
		{Role: RoleUser, Content: "t2"},
		{Role: RoleAssistant, Content: "a2", ToolCall: &ToolCall{ID: "c2", Name: "x"}},
		{Role: RoleTool, ToolCallID: "c2", Content: "r2"},
		{Role: RoleUser, Content: "t3"},
		{Role: RoleAssistant, Content: "a3"},
	}
	compacted := compactor.Compact(messages)
	if len(compacted) != 5 {
		t.Fatalf("expected exactly 5 kept messages, got %d: %#v", len(compacted), compacted)
	}
	if compacted[0].Content != "t2" {
		t.Fatalf("window must start at a turn boundary: %#v", compacted[0])
	}
	// The assistant tool_call and its tool result stay adjacent.
	if compacted[1].Role != RoleAssistant || compacted[1].ToolCall == nil || compacted[2].Role != RoleTool || compacted[2].ToolCallID != "c2" {
		t.Fatalf("tool pairing broken by compaction: %#v", compacted)
	}
}

func TestRecentTurnsCompactorKeepsOversizedLatestTurnWhole(t *testing.T) {
	compactor := RecentTurnsCompactor{MaxMessages: 2}
	messages := []ChatMessage{
		{Role: RoleUser, Content: "t1"},
		{Role: RoleAssistant, Content: "", ToolCall: &ToolCall{ID: "c1", Name: "x"}},
		{Role: RoleTool, ToolCallID: "c1", Content: "r1"},
		{Role: RoleAssistant, Content: "a1"},
	}
	compacted := compactor.Compact(messages)
	if compacted[0].Content != "t1" || len(compacted) != 4 {
		t.Fatalf("latest turn must be kept whole: %#v", compacted)
	}
}

func TestFileSessionStoreRoundTripAndConflict(t *testing.T) {
	t.TempDir()
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
	if err := store.Create(ctx, session); !errors.Is(err, ErrSessionConflict) {
		t.Fatalf("duplicate create must conflict, got %v", err)
	}

	session.Append("run-a", EvUserMessage, UserMessageData{Text: "hello"})
	version := session.Version()
	if err := store.Save(ctx, session, version-1); err != nil {
		t.Fatal(err)
	}

	loaded, err := store.Load(ctx, session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if loaded.ID() != session.ID() || loaded.ProfileID() != session.ProfileID() {
		t.Fatalf("restored session metadata mismatch: %#v", loaded)
	}
	if loaded.Version() != version {
		t.Fatalf("restored version mismatch: %d vs %d", loaded.Version(), version)
	}
	original := session.Events()
	restored := loaded.Events()
	if len(original) != len(restored) || string(original[0].Data) != string(restored[0].Data) {
		t.Fatalf("event payloads diverged: %#v vs %#v", original, restored)
	}

	// Optimistic concurrency: a stale expected version is rejected.
	session.Append("run-a", EvRunEnd, RunEndData{Status: RunCompleted})
	if err := store.Save(ctx, session, 0); !errors.Is(err, ErrSessionConflict) {
		t.Fatalf("stale save must conflict, got %v", err)
	}
	if err := store.Save(ctx, session, version); err != nil {
		t.Fatalf("fresh save failed: %v", err)
	}
	again, err := store.Load(ctx, session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if again.Version() != session.Version() {
		t.Fatalf("append-only save lost events: %d vs %d", again.Version(), session.Version())
	}
}

func TestFileSessionStoreRejectsPathTraversal(t *testing.T) {
	store, err := NewFileSessionStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(context.Background(), "../escape"); !errors.Is(err, ErrSessionNotFound) && err == nil {
		t.Fatalf("path traversal must not resolve, got %v", err)
	}
}
