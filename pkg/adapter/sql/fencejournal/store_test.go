package fencejournal

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"database/sql"
	"github.com/whhhh1500/auto-agent/pkg/adapter/sql/sqlkit"
	"github.com/whhhh1500/auto-agent/pkg/runtime"
	"github.com/whhhh1500/auto-agent/pkg/storage"
	_ "modernc.org/sqlite"
)

func TestSQLiteFenceJournalMonotonicReplayAndInProgressFailClosed(t *testing.T) {
	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "fence.db")+"?_pragma=journal_mode%28WAL%29")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := storage.OpenSQLSessionStore(context.Background(), db, storage.SQLDialectSQLite); err != nil {
		t.Fatal(err)
	}
	store, err := New(db, sqlkit.SQLite)
	if err != nil {
		t.Fatal(err)
	}
	command := runtime.FenceCommand{RequestID: "fence-sql", CompositionRevision: "composition", ActorID: "actor", Reason: "test"}
	if record, err := store.Begin(context.Background(), command); err != nil || record.Decision != runtime.FenceDecisionExecute {
		t.Fatalf("first=%#v err=%v", record, err)
	}
	if _, err := store.Begin(context.Background(), command); !errors.Is(err, runtime.ErrFenceUnknown) {
		t.Fatalf("in-progress error=%v", err)
	}
	if err := store.Complete(context.Background(), command, runtime.FenceResult{CompositionRevision: command.CompositionRevision, Completed: true}); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkUnknown(context.Background(), command); !errors.Is(err, runtime.ErrFenceConflict) {
		t.Fatalf("replay downgrade=%v", err)
	}
	if record, err := store.Begin(context.Background(), command); err != nil || record.Decision != runtime.FenceDecisionReplay || !record.Result.Completed {
		t.Fatalf("replay=%#v err=%v", record, err)
	}
	conflict := command
	conflict.Reason = "other"
	if _, err := store.Begin(context.Background(), conflict); !errors.Is(err, runtime.ErrFenceConflict) {
		t.Fatalf("conflict error=%v", err)
	}
}

func TestSQLiteFenceJournalFailsClosedForCorruptPersistedDecision(t *testing.T) {
	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "fence-corrupt.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := storage.OpenSQLSessionStore(context.Background(), db, storage.SQLDialectSQLite); err != nil {
		t.Fatal(err)
	}
	store, err := New(db, sqlkit.SQLite)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO runtime_fence_journal
		(request_id, composition_revision, actor_id, reason, decision, completed, remaining_leases, created_at, updated_at)
		VALUES ('fence-corrupt', 'composition', 'actor', 'reason', 'replay', 0, 0, 1, 1)`); err != nil {
		t.Fatal(err)
	}
	command := runtime.FenceCommand{RequestID: "fence-corrupt", CompositionRevision: "composition", ActorID: "actor", Reason: "reason"}
	if _, err := store.Begin(context.Background(), command); !errors.Is(err, runtime.ErrFenceUnknown) {
		t.Fatalf("corrupt replay error=%v", err)
	}
}

func TestSQLiteFenceJournalFreshHandlesHaveOneExecutor(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fence-concurrent.db")
	open := func() (*sql.DB, *Store) {
		db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout%285000%29&_pragma=journal_mode%28WAL%29")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := storage.OpenSQLSessionStore(context.Background(), db, storage.SQLDialectSQLite); err != nil {
			t.Fatal(err)
		}
		store, err := New(db, sqlkit.SQLite)
		if err != nil {
			t.Fatal(err)
		}
		return db, store
	}
	firstDB, first := open()
	defer firstDB.Close()
	secondDB, second := open()
	defer secondDB.Close()
	command := runtime.FenceCommand{RequestID: "fence-fresh-handles", CompositionRevision: "composition", ActorID: "actor", Reason: "test"}
	start := make(chan struct{})
	decisions := make(chan runtime.FenceDecision, 2)
	var group sync.WaitGroup
	for _, store := range []*Store{first, second} {
		group.Add(1)
		go func(store *Store) {
			defer group.Done()
			<-start
			record, err := store.Begin(context.Background(), command)
			if err != nil && !errors.Is(err, runtime.ErrFenceUnknown) {
				t.Errorf("begin: %v", err)
				return
			}
			decisions <- record.Decision
		}(store)
	}
	close(start)
	group.Wait()
	close(decisions)
	execute, unknown := 0, 0
	for decision := range decisions {
		if decision == runtime.FenceDecisionExecute {
			execute++
		}
		if decision == runtime.FenceDecisionUnknown {
			unknown++
		}
	}
	if execute != 1 || unknown != 1 {
		t.Fatalf("execute=%d unknown=%d", execute, unknown)
	}
}
