package core

import (
	"reflect"
	"strings"
	"testing"
)

func TestToolContinuationBoundary(t *testing.T) {
	call := ToolCall{ID: "call-a", Name: "test.lookup", Continuation: strings.Repeat("x", 64<<10)}
	if err := validateToolCall(call); err != nil {
		t.Fatal(err)
	}
	call.Continuation += "x"
	if err := validateToolCall(call); err == nil {
		t.Fatal("oversized continuation accepted")
	}
	call.Continuation = string([]byte{0xff})
	if err := validateToolCall(call); err == nil {
		t.Fatal("invalid continuation encoding accepted")
	}
}

func TestRestoredSessionAppendPreservesHistoryAndContinuation(t *testing.T) {
	for _, warmProjection := range []bool{false, true} {
		t.Run(map[bool]string{false: "cold", true: "warm"}[warmProjection], func(t *testing.T) {
			session := contextTestSession(t, "restored-history")
			call := ToolCall{ID: "call-a", Name: "test.lookup", Continuation: "opaque-protocol-state"}
			if _, err := session.Append("run-a", EvUserMessage, UserMessageData{Text: "lookup"}); err != nil {
				t.Fatal(err)
			}
			if _, err := session.Append("run-a", EvAssistantMessage, AssistantMessageData{ToolCall: &call, ToolCalls: []ToolCall{call}}); err != nil {
				t.Fatal(err)
			}
			restored, err := RestoreSession(SessionOptions{ID: session.ID(), ProfileID: session.ProfileID(), Principal: session.Principal(), Scope: session.Scope()}, session.Events())
			if err != nil {
				t.Fatal(err)
			}
			if warmProjection {
				if _, err := restored.DeriveMessages(); err != nil {
					t.Fatal(err)
				}
			}
			result := ToolResultData{CallID: call.ID, Content: "found", OK: true}
			for _, target := range []*Session{session, restored} {
				if _, err := target.Append("run-a", EvToolResult, result); err != nil {
					t.Fatal(err)
				}
			}
			// The default fused compactor must work before DeriveMessages warms the cache.
			compacted, ok := restored.deriveRecentCompactedMessages(RecentTurnsCompactor{MaxMessages: 20})
			want, err := session.DeriveMessages()
			if err != nil {
				t.Fatal(err)
			}
			got, err := restored.DeriveMessages()
			if err != nil || !ok || len(got) != 3 || !reflect.DeepEqual(got, want) || !reflect.DeepEqual(compacted, want) {
				t.Fatalf("restored projection lost transcript: messages=%d compacted=%d err=%v", len(got), len(compacted), err)
			}
			if got[1].ToolCalls[0].Continuation != call.Continuation {
				t.Fatal("continuation lost on restore")
			}
		})
	}
}
