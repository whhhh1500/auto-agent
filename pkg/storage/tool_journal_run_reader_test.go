package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"testing"

	"github.com/whhhh1500/auto-agent/pkg/core"
)

func TestSQLToolInvocationJournalListsExactRunScope(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", t.TempDir()+"/tool-journal-reader.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectSQLite); err != nil {
		t.Fatal(err)
	}
	journal, err := NewSQLToolInvocationJournal(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	principal := core.Principal{TenantID: "tenant-a", SubjectID: "subject-a"}
	first := core.ToolInvocation{TenantID: principal.TenantID, SubjectID: principal.SubjectID, SessionID: "session-a", RunID: "run-a", CallID: "call-b", CapabilityID: "tool.echo", ArgsDigest: digestText("first"), Idempotent: true}
	second := first
	second.CallID = "call-a"
	second.ArgsDigest = digestText("second")
	other := first
	other.RunID = "run-b"
	other.CallID = "call-c"
	other.ArgsDigest = digestText("other")
	for _, invocation := range []core.ToolInvocation{first, second, other} {
		if _, _, err := journal.BeginToolInvocation(ctx, invocation); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := journal.CompleteToolInvocation(ctx, second, core.CapabilityResult{OK: true, Content: "done"}); err != nil {
		t.Fatal(err)
	}

	records, err := journal.ListRunToolInvocations(ctx, principal, "session-a", "run-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || records[0].CallID != "call-a" || records[1].CallID != "call-b" || records[0].State != core.ToolInvocationCompleted || records[1].State != core.ToolInvocationStarted {
		t.Fatalf("records=%#v", records)
	}
	otherPrincipal := core.Principal{TenantID: "tenant-a", SubjectID: "subject-b"}
	if hidden, err := journal.ListRunToolInvocations(ctx, otherPrincipal, "session-a", "run-a"); err != nil || len(hidden) != 0 {
		t.Fatalf("cross-principal records=%#v err=%v", hidden, err)
	}
}

func digestText(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}
