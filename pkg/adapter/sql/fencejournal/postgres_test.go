package fencejournal

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/cc-auto-agent/harness-core/pkg/adapter/sql/sqlkit"
	"github.com/cc-auto-agent/harness-core/pkg/runtime"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

func TestPostgresFenceJournalFreshSchemaAndMonotonicDecisions(t *testing.T) {
	db := newPostgresFenceJournalTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	journal, err := New(db, sqlkit.Postgres)
	if err != nil {
		t.Fatalf("new postgres fence journal: %v", err)
	}
	command := runtime.FenceCommand{
		RequestID:           "pg-fence-monotonic",
		CompositionRevision: "pg-composition",
		ActorID:             "pg-actor",
		Reason:              "postgres fence test",
	}

	record, err := journal.Begin(ctx, command)
	if err != nil || record.Decision != runtime.FenceDecisionExecute {
		t.Fatalf("first begin=%#v err=%v", record, err)
	}
	if _, err := journal.Begin(ctx, command); !errors.Is(err, runtime.ErrFenceUnknown) {
		t.Fatalf("in-progress begin error=%v, want ErrFenceUnknown", err)
	}

	result := runtime.FenceResult{
		CompositionRevision: command.CompositionRevision,
		Completed:           true,
	}
	if err := journal.Complete(ctx, command, result); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if err := journal.MarkUnknown(ctx, command); !errors.Is(err, runtime.ErrFenceConflict) {
		t.Fatalf("mark unknown downgraded replay: %v", err)
	}
	replay, err := journal.Begin(ctx, command)
	if err != nil || replay.Decision != runtime.FenceDecisionReplay || !replay.Result.Completed {
		t.Fatalf("replay=%#v err=%v", replay, err)
	}

	conflict := command
	conflict.Reason = "different postgres fence command"
	if _, err := journal.Begin(ctx, conflict); !errors.Is(err, runtime.ErrFenceConflict) {
		t.Fatalf("same request with changed command error=%v, want ErrFenceConflict", err)
	}
	if err := journal.Complete(ctx, command, result); err != nil {
		t.Fatalf("idempotent complete: %v", err)
	}
}

func TestPostgresFenceJournalFreshHandlesHaveOneExecutor(t *testing.T) {
	dsn := os.Getenv("HARNESS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("HARNESS_TEST_PG_DSN is not configured")
	}

	adminConfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	adminDB := stdlib.OpenDB(*adminConfig)
	if err := adminDB.PingContext(ctx); err != nil {
		_ = adminDB.Close()
		t.Fatalf("postgres ping failed: %v", err)
	}
	schema := fmt.Sprintf("fencejournal_test_%d", time.Now().UnixNano())
	if _, err := adminDB.ExecContext(ctx, "CREATE SCHEMA "+schema); err != nil {
		_ = adminDB.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if _, err := adminDB.ExecContext(cleanupCtx, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Errorf("drop postgres test schema: %v", err)
		}
		if err := adminDB.Close(); err != nil {
			t.Errorf("close postgres admin DB: %v", err)
		}
	})

	firstDB := openPostgresFenceJournalSchema(t, dsn, schema)
	secondDB := openPostgresFenceJournalSchema(t, dsn, schema)
	t.Cleanup(func() {
		_ = firstDB.Close()
		_ = secondDB.Close()
	})
	if _, err := storage.OpenSQLSessionStore(ctx, firstDB, storage.SQLDialectPostgres); err != nil {
		t.Fatal(err)
	}
	first, err := New(firstDB, sqlkit.Postgres)
	if err != nil {
		t.Fatal(err)
	}
	second, err := New(secondDB, sqlkit.Postgres)
	if err != nil {
		t.Fatal(err)
	}

	command := runtime.FenceCommand{
		RequestID:           "pg-fence-concurrent",
		CompositionRevision: "pg-composition",
		ActorID:             "pg-actor",
		Reason:              "concurrent postgres fence test",
	}
	start := make(chan struct{})
	results := make(chan struct {
		record runtime.FenceRecord
		err    error
	}, 2)
	var group sync.WaitGroup
	for _, journal := range []*Store{first, second} {
		group.Add(1)
		go func(journal *Store) {
			defer group.Done()
			<-start
			record, err := journal.Begin(ctx, command)
			results <- struct {
				record runtime.FenceRecord
				err    error
			}{record: record, err: err}
		}(journal)
	}
	close(start)
	group.Wait()
	close(results)

	execute, unknown := 0, 0
	for result := range results {
		switch {
		case result.err == nil && result.record.Decision == runtime.FenceDecisionExecute:
			execute++
		case errors.Is(result.err, runtime.ErrFenceUnknown) && result.record.Decision == runtime.FenceDecisionUnknown:
			unknown++
		default:
			t.Fatalf("concurrent begin result=%#v", result)
		}
	}
	if execute != 1 || unknown != 1 {
		t.Fatalf("execute=%d unknown=%d, want one each", execute, unknown)
	}
}

func newPostgresFenceJournalTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("HARNESS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("HARNESS_TEST_PG_DSN is not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	adminConfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	adminDB := stdlib.OpenDB(*adminConfig)
	if err := adminDB.PingContext(ctx); err != nil {
		_ = adminDB.Close()
		t.Fatalf("postgres ping failed: %v", err)
	}
	schema := fmt.Sprintf("fencejournal_test_%d", time.Now().UnixNano())
	if _, err := adminDB.ExecContext(ctx, "CREATE SCHEMA "+schema); err != nil {
		_ = adminDB.Close()
		t.Fatal(err)
	}
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.RuntimeParams["search_path"] = schema
	db := stdlib.OpenDB(*config)
	db.SetMaxOpenConns(4)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		_, _ = adminDB.ExecContext(ctx, "DROP SCHEMA "+schema+" CASCADE")
		_ = adminDB.Close()
		t.Fatal(err)
	}
	if _, err := storage.OpenSQLSessionStore(ctx, db, storage.SQLDialectPostgres); err != nil {
		_ = db.Close()
		_, _ = adminDB.ExecContext(ctx, "DROP SCHEMA "+schema+" CASCADE")
		_ = adminDB.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Close()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if _, err := adminDB.ExecContext(cleanupCtx, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Errorf("drop postgres test schema: %v", err)
		}
		_ = adminDB.Close()
	})
	return db
}

func openPostgresFenceJournalSchema(t *testing.T, dsn, schema string) *sql.DB {
	t.Helper()
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.RuntimeParams["search_path"] = schema
	db := stdlib.OpenDB(*config)
	db.SetMaxOpenConns(4)
	if err := db.PingContext(context.Background()); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	return db
}
