package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

func TestSQLToolInvocationJournalReaderSQLite(t *testing.T) {
	sessions := newTestSQLStore(t)
	testSQLToolInvocationJournalReader(t, sessions.db, SQLDialectSQLite)
}

func TestPostgresSQLToolInvocationJournalReader(t *testing.T) {
	db := newPostgresTestDB(t)
	ctx := context.Background()
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres); err != nil {
		t.Fatal(err)
	}
	testSQLToolInvocationJournalReader(t, db, SQLDialectPostgres)
}

func testSQLToolInvocationJournalReader(t *testing.T, db *sql.DB, dialect SQLDialect) {
	t.Helper()
	ctx := context.Background()
	journal, err := NewSQLToolInvocationJournal(db, dialect)
	if err != nil {
		t.Fatal(err)
	}
	var reader core.ToolInvocationReader = journal
	invocation := testToolInvocation(t, core.ToolCall{
		ID: "reader-call", Name: "payments.capture", Args: map[string]any{"amount": 7},
	}, false)

	before := toolInvocationRowCount(t, ctx, db)
	if record, found, err := reader.GetToolInvocation(ctx, invocation); err != nil || found || record != (core.ToolInvocationRecord{}) {
		t.Fatalf("missing read: record=%#v found=%t err=%v", record, found, err)
	}
	if after := toolInvocationRowCount(t, ctx, db); after != before {
		t.Fatalf("missing read mutated journal rows: before=%d after=%d", before, after)
	}
	invalid := invocation
	invalid.ArgsDigest = "not-a-sha256-digest"
	if record, found, err := reader.GetToolInvocation(ctx, invalid); err == nil || found || record != (core.ToolInvocationRecord{}) {
		t.Fatalf("invalid identity read: record=%#v found=%t err=%v", record, found, err)
	}
	if after := toolInvocationRowCount(t, ctx, db); after != before {
		t.Fatalf("invalid identity read mutated journal rows: before=%d after=%d", before, after)
	}

	if _, decision, err := journal.BeginToolInvocation(ctx, invocation); err != nil || decision != core.ToolInvocationExecuteNew {
		t.Fatalf("begin: decision=%q err=%v", decision, err)
	}
	started, found, err := reader.GetToolInvocation(ctx, invocation)
	if err != nil || !found || started.State != core.ToolInvocationStarted || started.Result != nil {
		t.Fatalf("started read: record=%#v found=%t err=%v", started, found, err)
	}
	if started.StartedAt.IsZero() || started.UpdatedAt.IsZero() || !started.CompletedAt.IsZero() {
		t.Fatalf("started read has invalid timestamps: record=%#v", started)
	}
	if err := journal.MarkToolInvocationUncertain(ctx, invocation, "transport_lost"); err != nil {
		t.Fatal(err)
	}
	uncertain, found, err := reader.GetToolInvocation(ctx, invocation)
	if err != nil || !found || uncertain.State != core.ToolInvocationUncertain || uncertain.ErrorCode != "transport_lost" {
		t.Fatalf("uncertain read: record=%#v found=%t err=%v", uncertain, found, err)
	}
	completed, err := journal.CompleteToolInvocation(ctx, invocation, core.CapabilityResult{
		Content: "captured", OK: true, Metadata: map[string]any{"provider": "fixture"},
	})
	if err != nil || completed.Result == nil {
		t.Fatalf("complete: record=%#v err=%v", completed, err)
	}
	got, found, err := reader.GetToolInvocation(ctx, invocation)
	if err != nil || !found || got.State != core.ToolInvocationCompleted || got.Result == nil || got.Result.Content != "captured" {
		t.Fatalf("completed read: record=%#v found=%t err=%v", got, found, err)
	}
	if got.StartedAt.IsZero() || got.UpdatedAt.IsZero() || got.CompletedAt.IsZero() {
		t.Fatalf("completed read has invalid timestamps: record=%#v", got)
	}
	got.Result.Metadata["provider"] = "mutated"
	again, found, err := reader.GetToolInvocation(ctx, invocation)
	if err != nil || !found || again.Result == nil || again.Result.Metadata["provider"] != "fixture" {
		t.Fatalf("reader returned an aliased record: record=%#v found=%t err=%v", again, found, err)
	}

	identityMismatches := map[string]core.ToolInvocation{}
	argsMismatch := testToolInvocation(t, core.ToolCall{
		ID: invocation.CallID, Name: invocation.CapabilityID, Args: map[string]any{"amount": 8},
	}, invocation.Idempotent)
	identityMismatches["arguments"] = argsMismatch
	capabilityMismatch := invocation
	capabilityMismatch.CapabilityID = "payments.refund"
	identityMismatches["capability"] = capabilityMismatch
	idempotencyMismatch := invocation
	idempotencyMismatch.Idempotent = !invocation.Idempotent
	identityMismatches["idempotency"] = idempotencyMismatch
	crossTenant := invocation
	crossTenant.TenantID = "other-tenant"
	identityMismatches["tenant"] = crossTenant
	crossSubject := invocation
	crossSubject.SubjectID = "other-subject"
	identityMismatches["subject"] = crossSubject
	for name, mismatch := range identityMismatches {
		if record, found, err := reader.GetToolInvocation(ctx, mismatch); err != nil || found || record != (core.ToolInvocationRecord{}) {
			t.Fatalf("%s mismatch leaked durable row: record=%#v found=%t err=%v", name, record, found, err)
		}
	}
	if after := toolInvocationRowCount(t, ctx, db); after != before+1 {
		t.Fatalf("reader changed journal row count: before=%d after=%d", before, after)
	}

	const readers = 16
	const readsPerReader = 32
	errs := make(chan error, readers)
	var group sync.WaitGroup
	for worker := 0; worker < readers; worker++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for index := 0; index < readsPerReader; index++ {
				record, found, err := reader.GetToolInvocation(ctx, invocation)
				if err != nil || !found || record.State != core.ToolInvocationCompleted || record.Result == nil || record.Result.Content != "captured" {
					errs <- fmt.Errorf("concurrent read: record=%#v found=%t err=%v", record, found, err)
					return
				}
			}
		}()
	}
	group.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestSQLToolInvocationJournalReaderConcurrentLifecycleAndRetentionSQLite(t *testing.T) {
	sessions := newTestSQLStore(t)
	// SQLite serializes writes. One database connection makes this test exercise
	// concurrent reader goroutines without turning scheduler timing into a
	// spurious busy/locked failure.
	sessions.db.SetMaxOpenConns(1)
	sessions.db.SetMaxIdleConns(1)
	ctx := context.Background()
	journal, err := NewSQLToolInvocationJournal(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	var reader core.ToolInvocationReader = journal
	invocation := testToolInvocation(t, core.ToolCall{
		ID: "reader-concurrent", Name: "payments.capture", Args: map[string]any{"amount": 9},
	}, false)
	if _, decision, err := journal.BeginToolInvocation(ctx, invocation); err != nil || decision != core.ToolInvocationExecuteNew {
		t.Fatalf("begin: decision=%q err=%v", decision, err)
	}

	const readers = 12
	const readsPerReader = 32
	start := make(chan struct{})
	errs := make(chan error, readers+1)
	var group sync.WaitGroup
	for worker := 0; worker < readers; worker++ {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			for index := 0; index < readsPerReader; index++ {
				record, found, err := reader.GetToolInvocation(ctx, invocation)
				if err != nil || !found || record.ToolInvocation != invocation {
					errs <- fmt.Errorf("lifecycle read: record=%#v found=%t err=%v", record, found, err)
					return
				}
				switch record.State {
				case core.ToolInvocationStarted:
					if record.Result != nil {
						errs <- fmt.Errorf("started lifecycle record has result: %#v", record)
						return
					}
				case core.ToolInvocationCompleted:
					if record.Result == nil || record.Result.Content != "concurrent" {
						errs <- fmt.Errorf("completed lifecycle record is invalid: %#v", record)
						return
					}
				default:
					errs <- fmt.Errorf("unexpected lifecycle state %q", record.State)
					return
				}
			}
		}()
	}
	group.Add(1)
	go func() {
		defer group.Done()
		<-start
		if _, err := journal.CompleteToolInvocation(ctx, invocation, core.CapabilityResult{Content: "concurrent", OK: true}); err != nil {
			errs <- fmt.Errorf("complete: %w", err)
		}
	}()
	close(start)
	group.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if record, found, err := reader.GetToolInvocation(ctx, invocation); err != nil || !found || record.State != core.ToolInvocationCompleted || record.Result == nil {
		t.Fatalf("final lifecycle read: record=%#v found=%t err=%v", record, found, err)
	}

	if _, err := sessions.db.ExecContext(ctx, "UPDATE tool_invocations SET completed_at = 1, updated_at = 1 WHERE session_id = ? AND run_id = ? AND call_id = ?", invocation.SessionID, invocation.RunID, invocation.CallID); err != nil {
		t.Fatal(err)
	}
	retentionStart := make(chan struct{})
	errs = make(chan error, readers+1)
	for worker := 0; worker < readers; worker++ {
		group.Add(1)
		go func() {
			defer group.Done()
			<-retentionStart
			for index := 0; index < readsPerReader; index++ {
				record, found, err := reader.GetToolInvocation(ctx, invocation)
				if err != nil {
					errs <- fmt.Errorf("retention read: %w", err)
					return
				}
				if found && (record.ToolInvocation != invocation || record.State != core.ToolInvocationCompleted || record.Result == nil) {
					errs <- fmt.Errorf("retention returned a partial record: %#v", record)
					return
				}
			}
		}()
	}
	group.Add(1)
	go func() {
		defer group.Done()
		<-retentionStart
		if deleted, err := sessions.PruneToolInvocations(ctx, time.Now().UTC().Add(-time.Minute)); err != nil || deleted != 1 {
			errs <- fmt.Errorf("retention prune: deleted=%d err=%v", deleted, err)
		}
	}()
	close(retentionStart)
	group.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if record, found, err := reader.GetToolInvocation(ctx, invocation); err != nil || found || record != (core.ToolInvocationRecord{}) {
		t.Fatalf("pruned read: record=%#v found=%t err=%v", record, found, err)
	}
}

func TestSQLToolInvocationJournalReaderFailsClosedOnCorruptResultSQLite(t *testing.T) {
	sessions := newTestSQLStore(t)
	ctx := context.Background()
	journal, err := NewSQLToolInvocationJournal(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	var reader core.ToolInvocationReader = journal
	invocation := testToolInvocation(t, core.ToolCall{
		ID: "reader-corrupt", Name: "payments.capture", Args: map[string]any{"amount": 11},
	}, false)
	if _, decision, err := journal.BeginToolInvocation(ctx, invocation); err != nil || decision != core.ToolInvocationExecuteNew {
		t.Fatalf("begin: decision=%q err=%v", decision, err)
	}
	if _, err := journal.CompleteToolInvocation(ctx, invocation, core.CapabilityResult{Content: "valid", OK: true}); err != nil {
		t.Fatal(err)
	}
	before := toolInvocationRowCount(t, ctx, sessions.db)
	invalidMetadata := `{"content":"captured","ok":true,"metadata":{"oversized":"` + strings.Repeat("x", core.MaxCapabilityMetadataBytes+1) + `"}}`
	if _, err := sessions.db.ExecContext(ctx, "UPDATE tool_invocations SET result_json = ? WHERE session_id = ? AND run_id = ? AND call_id = ?", invalidMetadata, invocation.SessionID, invocation.RunID, invocation.CallID); err != nil {
		t.Fatal(err)
	}
	if record, found, err := reader.GetToolInvocation(ctx, invocation); err == nil || found || record != (core.ToolInvocationRecord{}) {
		t.Fatalf("corrupt result was exposed: record=%#v found=%t err=%v", record, found, err)
	}
	if after := toolInvocationRowCount(t, ctx, sessions.db); after != before {
		t.Fatalf("corrupt read mutated journal rows: before=%d after=%d", before, after)
	}
}

func TestSQLToolInvocationReaderBindsExactIdentity(t *testing.T) {
	sqlite := sqlSelectToolInvocationForReader.bind(SQLDialectSQLite)
	if placeholders := strings.Count(sqlite, "?"); placeholders != 8 {
		t.Fatalf("sqlite reader placeholders=%d, want 8: %s", placeholders, sqlite)
	}
	postgres := sqlSelectToolInvocationForReader.bind(SQLDialectPostgres)
	if strings.Contains(postgres, "?") {
		t.Fatalf("postgres reader query retained sqlite placeholder: %s", postgres)
	}
	for placeholder := 1; placeholder <= 8; placeholder++ {
		if !strings.Contains(postgres, fmt.Sprintf("$%d", placeholder)) {
			t.Fatalf("postgres reader query is missing $%d: %s", placeholder, postgres)
		}
	}
}

func toolInvocationRowCount(t *testing.T, ctx context.Context, db *sql.DB) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM tool_invocations").Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
