package storage

import (
	"context"
	"strings"
	"testing"
	"time"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

func testToolInvocation(t *testing.T, call core.ToolCall, idempotent bool) core.ToolInvocation {
	t.Helper()
	invocation, err := core.NewToolInvocation(core.RunInfo{
		RunID: "run-tool-journal", SessionID: "session-tool-journal",
		Principal: core.Principal{TenantID: "acme", SubjectID: "alice"},
	}, call, idempotent)
	if err != nil {
		t.Fatal(err)
	}
	return invocation
}

func TestSQLToolInvocationJournalFencesReplaysAndConflicts(t *testing.T) {
	sessions := newTestSQLStore(t)
	journal, err := NewSQLToolInvocationJournal(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	call := core.ToolCall{ID: "call-payment", Name: "payment.charge", Args: map[string]any{"amount": 42}}
	invocation := testToolInvocation(t, call, false)
	record, decision, err := journal.BeginToolInvocation(ctx, invocation)
	if err != nil || decision != core.ToolInvocationExecuteNew || record.State != core.ToolInvocationStarted {
		t.Fatalf("first begin wrong: record=%#v decision=%q err=%v", record, decision, err)
	}
	_, decision, err = journal.BeginToolInvocation(ctx, invocation)
	if err != nil || decision != core.ToolInvocationUnknown {
		t.Fatalf("non-idempotent in-flight call was replayable: decision=%q err=%v", decision, err)
	}
	if err := journal.MarkToolInvocationUncertain(ctx, invocation, "connection_lost"); err != nil {
		t.Fatal(err)
	}
	_, decision, err = journal.BeginToolInvocation(ctx, invocation)
	if err != nil || decision != core.ToolInvocationUnknown {
		t.Fatalf("uncertain non-idempotent call was replayable: decision=%q err=%v", decision, err)
	}
	completed, err := journal.CompleteToolInvocation(ctx, invocation, core.CapabilityResult{
		Content: `{"charge_id":"ch_1"}`, OK: true, Metadata: map[string]any{"provider": "test"},
	})
	if err != nil || completed.State != core.ToolInvocationCompleted || completed.Result == nil || !completed.Result.OK {
		t.Fatalf("complete failed: %#v err=%v", completed, err)
	}
	replayed, decision, err := journal.BeginToolInvocation(ctx, invocation)
	if err != nil || decision != core.ToolInvocationReplay || replayed.Result == nil || replayed.Result.Content != `{"charge_id":"ch_1"}` {
		t.Fatalf("completed result did not replay: %#v decision=%q err=%v", replayed, decision, err)
	}
	conflict := testToolInvocation(t, core.ToolCall{
		ID: call.ID, Name: call.Name, Args: map[string]any{"amount": 99},
	}, false)
	_, decision, err = journal.BeginToolInvocation(ctx, conflict)
	if err != nil || decision != core.ToolInvocationConflict {
		t.Fatalf("argument identity conflict was not detected: decision=%q err=%v", decision, err)
	}
}

func TestSQLToolInvocationJournalRetriesOnlyIdempotentCallsAndKeepsCanonicalResult(t *testing.T) {
	sessions := newTestSQLStore(t)
	journal, _ := NewSQLToolInvocationJournal(sessions.db, SQLDialectSQLite)
	ctx := context.Background()
	invocation := testToolInvocation(t, core.ToolCall{
		ID: "call-lookup", Name: "catalog.lookup", Args: map[string]any{"id": "x"},
	}, true)
	if _, decision, err := journal.BeginToolInvocation(ctx, invocation); err != nil || decision != core.ToolInvocationExecuteNew {
		t.Fatalf("first begin: decision=%q err=%v", decision, err)
	}
	if err := journal.MarkToolInvocationUncertain(ctx, invocation, "timeout"); err != nil {
		t.Fatal(err)
	}
	if _, decision, err := journal.BeginToolInvocation(ctx, invocation); err != nil || decision != core.ToolInvocationExecuteRetry {
		t.Fatalf("idempotent retry denied: decision=%q err=%v", decision, err)
	}
	first, err := journal.CompleteToolInvocation(ctx, invocation, core.CapabilityResult{Content: "first", OK: true})
	if err != nil || first.Result == nil || first.Result.Content != "first" {
		t.Fatalf("first completion failed: %#v err=%v", first, err)
	}
	second, err := journal.CompleteToolInvocation(ctx, invocation, core.CapabilityResult{Content: "second", OK: true})
	if err != nil || second.Result == nil || second.Result.Content != "first" {
		t.Fatalf("later completion replaced canonical result: %#v err=%v", second, err)
	}
}

func TestToolInvocationRetentionPreservesUncertainOutcomes(t *testing.T) {
	sessions := newTestSQLStore(t)
	journal, _ := NewSQLToolInvocationJournal(sessions.db, SQLDialectSQLite)
	ctx := context.Background()
	completed := testToolInvocation(t, core.ToolCall{ID: "call-old-complete", Name: "tool.complete"}, false)
	uncertain := testToolInvocation(t, core.ToolCall{ID: "call-old-unknown", Name: "tool.unknown"}, false)
	if _, _, err := journal.BeginToolInvocation(ctx, completed); err != nil {
		t.Fatal(err)
	}
	if _, err := journal.CompleteToolInvocation(ctx, completed, core.CapabilityResult{Content: "done", OK: true}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := journal.BeginToolInvocation(ctx, uncertain); err != nil {
		t.Fatal(err)
	}
	if err := journal.MarkToolInvocationUncertain(ctx, uncertain, "unknown"); err != nil {
		t.Fatal(err)
	}
	if _, err := sessions.db.ExecContext(ctx, "UPDATE tool_invocations SET completed_at = 1, updated_at = 1 WHERE call_id = ?", completed.CallID); err != nil {
		t.Fatal(err)
	}
	if _, err := sessions.db.ExecContext(ctx, "UPDATE tool_invocations SET updated_at = 1 WHERE call_id = ?", uncertain.CallID); err != nil {
		t.Fatal(err)
	}
	deleted, err := sessions.PruneToolInvocations(ctx, time.Now().UTC().Add(-time.Minute))
	if err != nil || deleted != 1 {
		t.Fatalf("completed retention wrong: deleted=%d err=%v", deleted, err)
	}
	var uncertainRows int
	if err := sessions.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM tool_invocations WHERE call_id = ?", uncertain.CallID).Scan(&uncertainRows); err != nil {
		t.Fatal(err)
	}
	if uncertainRows != 1 {
		t.Fatal("retention deleted an uncertain side-effect fence")
	}
}

func TestSQLToolInvocationJournalRejectsOverflow(t *testing.T) {
	sessions := newTestSQLStore(t)
	journal, err := NewSQLToolInvocationJournal(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	journal.maxInvocations = 2
	ctx := context.Background()
	call := func(id string) core.ToolCall {
		return core.ToolCall{ID: id, Name: "payment.charge", Args: map[string]any{"id": id}}
	}
	first := testToolInvocation(t, call("call-journal-1"), true)
	if _, decision, err := journal.BeginToolInvocation(ctx, first); err != nil || decision != core.ToolInvocationExecuteNew {
		t.Fatalf("first begin failed: decision=%q err=%v", decision, err)
	}
	if _, decision, err := journal.BeginToolInvocation(ctx, testToolInvocation(t, call("call-journal-2"), true)); err != nil || decision != core.ToolInvocationExecuteNew {
		t.Fatalf("second begin failed: decision=%q err=%v", decision, err)
	}
	if _, _, err := journal.BeginToolInvocation(ctx, testToolInvocation(t, call("call-journal-3"), true)); err == nil {
		t.Fatal("tool invocation overflow was accepted")
	} else if !strings.Contains(err.Error(), "tool invocations exceed maximum of 2") {
		t.Fatalf("unexpected overflow error: %v", err)
	}
	_, decision, err := journal.BeginToolInvocation(ctx, first)
	if err != nil || decision != core.ToolInvocationExecuteRetry {
		t.Fatalf("existing journal identity was blocked by the cap: decision=%q err=%v", decision, err)
	}
}
