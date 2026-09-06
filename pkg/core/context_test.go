package core

import (
	"strings"
	"testing"
)

func contextTestSession(t *testing.T, id string) *Session {
	t.Helper()
	_, _, _, user := testScopes()
	scope, err := user.Child(ScopeRef{Kind: ScopeSession, ID: id})
	if err != nil {
		t.Fatal(err)
	}
	session, err := NewSession(SessionOptions{ID: id, ProfileID: "profile", Principal: testPrincipal(user), Scope: scope})
	if err != nil {
		t.Fatal(err)
	}
	return session
}

func TestRecentTurnsCompactorPrunesOnlyOldToolResults(t *testing.T) {
	oldToolResult := "\u00e9\u00e9\u00e9\u00e9\u00e9"
	latestToolResult := strings.Repeat("z", 32)
	messages := []ChatMessage{
		{Role: RoleUser, Content: "older request", SourceSeq: 1},
		{Role: RoleAssistant, Content: "calling", ToolCall: &ToolCall{ID: "old-call", Name: "tool.old"}, SourceSeq: 2},
		{Role: RoleTool, ToolCallID: "old-call", Content: oldToolResult, SourceSeq: 3},
		{Role: RoleUser, Content: "latest request", SourceSeq: 4},
		{Role: RoleAssistant, Content: "calling", ToolCall: &ToolCall{ID: "latest-call", Name: "tool.latest"}, SourceSeq: 5},
		{Role: RoleTool, ToolCallID: "latest-call", Content: latestToolResult, SourceSeq: 6},
	}

	got := (RecentTurnsCompactor{MaxToolResultChars: 4}).Compact(messages)
	if len(got) != len(messages) {
		t.Fatalf("message count = %d, want %d", len(got), len(messages))
	}
	if got[2].Content != ElidedToolResultMarker+": 5 chars]" {
		t.Fatalf("old result = %q", got[2].Content)
	}
	if got[5].Content != latestToolResult {
		t.Fatalf("latest result changed: %q", got[5].Content)
	}
	if got[1].ToolCall.ID != got[2].ToolCallID || got[4].ToolCall.ID != got[5].ToolCallID {
		t.Fatalf("tool-call pairing changed: %#v", got)
	}
	if messages[2].Content != oldToolResult {
		t.Fatalf("compaction mutated caller input: %q", messages[2].Content)
	}
}

func TestRecentTurnsCompactorWindowKeepsWholeLatestTurn(t *testing.T) {
	messages := []ChatMessage{
		{Role: RoleAssistant, Content: "preamble", SourceSeq: 0},
		{Role: RoleUser, Content: "old request", SourceSeq: 1},
		{Role: RoleAssistant, Content: "old answer", SourceSeq: 2},
		{Role: RoleTool, ToolCallID: "old-call", Content: "old result", SourceSeq: 3},
		{Role: RoleUser, Content: "latest request", SourceSeq: 4},
		{Role: RoleAssistant, Content: "calling", ToolCall: &ToolCall{ID: "latest-call", Name: "tool.latest"}, SourceSeq: 5},
		{Role: RoleTool, ToolCallID: "latest-call", Content: "latest result", SourceSeq: 6},
	}

	got := (RecentTurnsCompactor{MaxMessages: 2}).Compact(messages)
	if len(got) != 3 {
		t.Fatalf("message count = %d, want complete 3-message latest turn", len(got))
	}
	if got[0].Content != "latest request" || got[1].ToolCall.ID != got[2].ToolCallID {
		t.Fatalf("latest turn or its tool pairing was split: %#v", got)
	}
}
