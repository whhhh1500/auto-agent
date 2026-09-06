package storage

import (
	"context"
	"database/sql"
	"path/filepath"
	"sync"
	"testing"

	core "github.com/cc-auto-agent/harness-core/pkg/core"

	_ "modernc.org/sqlite"
)

// BenchmarkWriteBehindCheckpoint isolates the synchronous checkpoint call.
// Session/event construction and backend reset are outside the measured
// region. The contention arm starts with an appender already blocked in a
// background flush, then measures the handoff plus the remaining prefix.
func BenchmarkWriteBehindCheckpoint(b *testing.B) {
	ctx := context.Background()

	b.Run("noop/memory", func(b *testing.B) {
		session := benchmarkCheckpointSession(b, "checkpoint-noop")
		store := core.NewMemorySessionStore()
		if err := store.Create(ctx, session); err != nil {
			b.Fatal(err)
		}
		writer := NewWriteBehind(store, session, 0, -1)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := writer.Checkpoint(ctx); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("single_pending_prefix/memory", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			session := benchmarkCheckpointSession(b, "checkpoint-memory")
			store := core.NewMemorySessionStore()
			if err := store.Create(ctx, session); err != nil {
				b.Fatal(err)
			}
			if _, err := session.Append("run-checkpoint", core.EvUserMessage, core.UserMessageData{Text: "pending"}); err != nil {
				b.Fatal(err)
			}
			writer := NewWriteBehind(store, session, 0, -1)
			b.StartTimer()
			if err := writer.Checkpoint(ctx); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("single_pending_prefix/sqlite_file", func(b *testing.B) {
		db, store := benchmarkSQLiteSessionStore(b)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			benchmarkResetSQLSessions(b, db)
			session := benchmarkCheckpointSession(b, "checkpoint-sqlite")
			if err := store.Create(ctx, session); err != nil {
				b.Fatal(err)
			}
			if _, err := session.Append("run-checkpoint", core.EvUserMessage, core.UserMessageData{Text: "pending"}); err != nil {
				b.Fatal(err)
			}
			writer := NewWriteBehind(store, session, 0, -1)
			b.StartTimer()
			if err := writer.Checkpoint(ctx); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("background_flush_contention/in_memory_appender", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			session := benchmarkCheckpointSession(b, "checkpoint-contention")
			store := newBenchmarkBlockingAppender()
			if _, err := session.Append("run-checkpoint", core.EvUserMessage, core.UserMessageData{Text: "background"}); err != nil {
				b.Fatal(err)
			}
			writer := NewWriteBehind(store, session, 0, -1)
			writer.MarkDirty()
			backgroundDone := make(chan struct{})
			go func() {
				writer.flushOnce(ctx)
				close(backgroundDone)
			}()
			<-store.started
			if _, err := session.Append("run-checkpoint", core.EvAssistantMessage, core.AssistantMessageData{Text: "foreground"}); err != nil {
				b.Fatal(err)
			}
			writer.MarkDirty()
			checkpointStarted := make(chan struct{})
			checkpointDone := make(chan error, 1)
			b.StartTimer()
			go func() {
				close(checkpointStarted)
				checkpointDone <- writer.Checkpoint(ctx)
			}()
			<-checkpointStarted
			close(store.release)
			if err := <-checkpointDone; err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			<-backgroundDone
			if got := store.calls(); got != 2 {
				b.Fatalf("append calls=%d, want 2", got)
			}
		}
	})
}

func benchmarkCheckpointSession(tb testing.TB, id string) *core.Session {
	tb.Helper()
	global := core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"}
	product := core.ScopeRef{Kind: core.ScopeProduct, ID: "performance"}
	tenant := core.ScopeRef{Kind: core.ScopeTenant, ID: "tenant-perf"}
	user := core.ScopeRef{Kind: core.ScopeUser, ID: "user-perf"}
	sessionRef := core.ScopeRef{Kind: core.ScopeSession, ID: id}
	userScope := core.MustScopePath(global, product, tenant, user)
	sessionScope := core.MustScopePath(global, product, tenant, user, sessionRef)
	principal := core.Principal{
		SubjectID: "user-perf",
		TenantID:  "tenant-perf",
		Scope:     userScope,
		Grants:    core.NewPermissionSet(core.PermRead, core.PermWrite),
	}
	session, err := core.NewSession(core.SessionOptions{
		ID: id, ProfileID: "performance.agent", Principal: principal, Scope: sessionScope,
	})
	if err != nil {
		tb.Fatal(err)
	}
	return session
}

func benchmarkSQLiteSessionStore(b *testing.B) (*sql.DB, *SQLSessionStore) {
	b.Helper()
	db, err := sql.Open("sqlite", filepath.Join(b.TempDir(), "checkpoint.db"))
	if err != nil {
		b.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	b.Cleanup(func() { _ = db.Close() })
	store, err := OpenSQLSessionStore(context.Background(), db, SQLDialectSQLite)
	if err != nil {
		b.Fatal(err)
	}
	return db, store
}

func benchmarkResetSQLSessions(tb testing.TB, db *sql.DB) {
	tb.Helper()
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, "DELETE FROM event_chunks"); err != nil {
		tb.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM sessions"); err != nil {
		tb.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		tb.Fatal(err)
	}
}

type benchmarkBlockingAppender struct {
	started chan struct{}
	release chan struct{}

	mu          sync.Mutex
	appendCalls int
}

func newBenchmarkBlockingAppender() *benchmarkBlockingAppender {
	return &benchmarkBlockingAppender{started: make(chan struct{}), release: make(chan struct{})}
}

func (*benchmarkBlockingAppender) Create(context.Context, *core.Session) error { return nil }

func (*benchmarkBlockingAppender) Load(context.Context, string) (*core.Session, error) {
	return nil, core.ErrSessionNotFound
}

func (*benchmarkBlockingAppender) Save(context.Context, *core.Session, int64) error { return nil }

func (s *benchmarkBlockingAppender) AppendEvents(_ context.Context, _ string, _ int64, _ []core.SessionEvent) error {
	s.mu.Lock()
	s.appendCalls++
	call := s.appendCalls
	s.mu.Unlock()
	if call == 1 {
		close(s.started)
		<-s.release
	}
	return nil
}

func (s *benchmarkBlockingAppender) calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.appendCalls
}
