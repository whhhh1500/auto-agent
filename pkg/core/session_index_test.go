package core

import (
	"testing"
	"time"
)

func TestSessionRunIndexTracksDurableFactsAndPreservesFirstResult(t *testing.T) {
	session := contextTestSession(t, "index-facts")
	if _, err := session.Append("run-index", EvRunStart, RunStartData{}); err != nil {
		t.Fatal(err)
	}
	if status, ok := session.RunStatus("run-index"); !ok || status != "" {
		t.Fatalf("started status=%q ok=%t", status, ok)
	}
	call := ToolCallData{CallID: "call_index", Name: "test.tool", Args: map[string]any{"nested": map[string]any{"n": "v"}}}
	if _, err := session.Append("run-index", EvToolCall, call); err != nil {
		t.Fatal(err)
	}
	if ok, err := session.HasToolCall("run-index", call.CallID); err != nil || !ok {
		t.Fatalf("tool call index ok=%t err=%v", ok, err)
	}
	if _, err := session.Append("run-index", EvToolResult, ToolResultData{CallID: call.CallID, Content: "first", OK: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append("run-index", EvToolResult, ToolResultData{CallID: call.CallID, Content: "second", OK: true}); err != nil {
		t.Fatal(err)
	}
	result, ok, err := session.ToolResult("run-index", call.CallID)
	if err != nil || !ok || result.Content != "first" {
		t.Fatalf("first result=%#v ok=%t err=%v", result, ok, err)
	}
	approval := ApprovalRequestedData{
		ApprovalID: "apr_0123456789abcdef0123456789abcdef",
		ToolCall:   ToolCall{ID: "call_approval", Name: "test.tool"},
		ResumeCall: ToolCall{ID: "call_resume", Name: "test.tool"},
	}
	if _, err := session.Append("run-index", EvApprovalRequested, approval); err != nil {
		t.Fatal(err)
	}
	if status, ok := session.RunStatus("run-index"); !ok || status != RunWaitingApproval {
		t.Fatalf("waiting status=%q ok=%t", status, ok)
	}
	pending, ok, err := session.PendingApproval("run-index")
	if err != nil || !ok || pending.ApprovalID != approval.ApprovalID {
		t.Fatalf("pending=%#v ok=%t err=%v", pending, ok, err)
	}
	if _, err := session.Append("run-index", EvApprovalResolved, ApprovalResolvedData{ApprovalID: approval.ApprovalID, CallID: approval.ToolCall.ID, Decision: ApprovalApproved, ResolvedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if status, ok := session.RunStatus("run-index"); !ok || status != "" {
		t.Fatalf("resolved status=%q ok=%t", status, ok)
	}
	if _, err := session.Append("run-index", EvRunEnd, RunEndData{Status: RunCompleted}); err != nil {
		t.Fatal(err)
	}
	if status, ok := session.RunStatus("run-index"); !ok || status != RunCompleted {
		t.Fatalf("completed status=%q ok=%t", status, ok)
	}
}

func TestSessionRunIndexRestoreCloneAndFailedAppendAreConsistent(t *testing.T) {
	session := contextTestSession(t, "index-roundtrip")
	if _, err := session.Append("run-roundtrip", EvRunStart, RunStartData{}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append("run-roundtrip", EvToolCall, ToolCallData{CallID: "call_roundtrip", Name: "test.tool"}); err != nil {
		t.Fatal(err)
	}
	before := session.Version()
	if _, err := session.Append("run-roundtrip", EvRunEnd, RunEndData{Status: "not-a-status"}); err == nil {
		t.Fatal("invalid event was accepted")
	}
	if session.Version() != before {
		t.Fatalf("failed append changed version: before=%d after=%d", before, session.Version())
	}
	if status, ok := session.RunStatus("run-roundtrip"); !ok || status != "" {
		t.Fatalf("failed append polluted run index: status=%q ok=%t", status, ok)
	}
	options := SessionOptions{ID: session.ID(), ProfileID: session.ProfileID(), Principal: session.Principal(), Scope: session.Scope()}
	restored, err := RestoreSession(options, session.Events())
	if err != nil {
		t.Fatal(err)
	}
	clone, err := session.Clone()
	if err != nil {
		t.Fatal(err)
	}
	for name, candidate := range map[string]*Session{"restored": restored, "clone": clone} {
		if status, ok := candidate.RunStatus("run-roundtrip"); !ok || status != "" {
			t.Errorf("%s status=%q ok=%t", name, status, ok)
		}
		if ok, err := candidate.HasToolCall("run-roundtrip", "call_roundtrip"); err != nil || !ok {
			t.Errorf("%s call indexed=%t err=%v", name, ok, err)
		}
	}
	// A separate restored value must not share the index or event storage.
	if _, err := restored.Append("run-roundtrip", EvRunEnd, RunEndData{Status: RunCancelled}); err != nil {
		t.Fatal(err)
	}
	if status, ok := session.RunStatus("run-roundtrip"); !ok || status != "" {
		t.Fatalf("restored mutation leaked: status=%q ok=%t", status, ok)
	}
}
