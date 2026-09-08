package artifactmigration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/whhhh1500/auto-agent/pkg/adapter/sql/sqlkit"
	app "github.com/whhhh1500/auto-agent/pkg/app/artifactmigration"
	"github.com/whhhh1500/auto-agent/pkg/storage"
	_ "modernc.org/sqlite"
)

func TestSQLiteStoreCASLeaseAndGenerationFence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "migration.db")
	firstDB, first := newSQLiteStore(t, path)
	defer firstDB.Close()
	secondDB, second := newSQLiteStore(t, path)
	defer secondDB.Close()
	initial, err := first.CompareAndSwap(context.Background(), 0, validMigration("one", 1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.CompareAndSwap(context.Background(), initial.StoreRevision, validMigration("stale", 1)); !errors.Is(err, app.ErrMigrationConflict) {
		t.Fatalf("wrong-store-revision error=%v", err)
	}
	lease, err := first.ClaimLease(context.Background(), "worker-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.ClaimLease(context.Background(), "worker-b", time.Minute); !errors.Is(err, app.ErrLeaseConflict) {
		t.Fatalf("second claim error=%v", err)
	}
	loaded, found, err := second.Load(context.Background())
	if err != nil || !found {
		t.Fatalf("load=%#v found=%t err=%v", loaded, found, err)
	}
	loaded.CopiedObjects = 1
	updated, err := first.UpdateLeaseHeld(context.Background(), lease, loaded.StoreRevision, loaded)
	if err != nil || updated.StoreRevision != loaded.StoreRevision+1 {
		t.Fatalf("worker update=%#v err=%v", updated, err)
	}
	if _, err := first.UpdateLeaseHeld(context.Background(), lease, loaded.StoreRevision, loaded); !errors.Is(err, app.ErrLeaseLost) {
		t.Fatalf("stale worker update error=%v", err)
	}
	for index := 0; index < 2; index++ {
		if _, err := first.AppendMutation(context.Background(), app.MutationAppend{Token: updated.MutationToken(), Operation: app.MutationPut, ObjectKey: fmt.Sprintf("terminal-%d", index)}); err != nil {
			t.Fatal(err)
		}
	}
	updated, found, err = first.Load(context.Background())
	if err != nil || !found || updated.MutationHighWater != 2 {
		t.Fatalf("load journal=%#v found=%t err=%v", updated, found, err)
	}
	terminal := updated
	terminal.State = app.StateCancelled
	terminal.LeaseOwner, terminal.LeaseExpiresAt = "", time.Time{}
	if _, err := first.UpdateLeaseHeld(context.Background(), lease, updated.StoreRevision, terminal); err != nil {
		t.Fatal(err)
	}
	if count := sqliteJournalCount(t, firstDB, updated.ID); count != 0 {
		t.Fatalf("terminal journal count=%d", count)
	}
	replacement := validMigration("two", 2)
	current, found, err := first.Load(context.Background())
	if err != nil || !found {
		t.Fatalf("load terminal=%#v found=%t err=%v", current, found, err)
	}
	// The replacement transaction also removes any residual metadata-only
	// journal rows for the retired generation. A journal has no foreign key so
	// this explicitly proves cleanup rather than relying on cascades.
	if _, err := firstDB.ExecContext(context.Background(), `INSERT INTO artifact_migration_mutations
		(migration_id, sequence, operation, object_key, etag, digest, state, created_at, updated_at)
		VALUES (?, ?, 'put', ?, '', '', 'applied', ?, ?)`, current.ID, 99, "residual", time.Now().UTC().UnixMilli(), time.Now().UTC().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	replacement.StoreRevision = current.StoreRevision
	if _, err := second.CompareAndSwap(context.Background(), current.StoreRevision, replacement); err != nil {
		t.Fatal(err)
	}
	if count := sqliteJournalCount(t, firstDB, current.ID); count != 0 {
		t.Fatalf("replacement journal count=%d", count)
	}
	if err := first.ValidateLease(context.Background(), lease); !errors.Is(err, app.ErrLeaseLost) {
		t.Fatalf("old generation lease error=%v", err)
	}
}

func TestSQLiteStoreFailMutationPurgesJournal(t *testing.T) {
	db, store := newSQLiteStore(t, filepath.Join(t.TempDir(), "fail-mutation.db"))
	defer db.Close()
	migration, err := store.CompareAndSwap(context.Background(), 0, validMigration("fail-mutation", 1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AppendMutation(context.Background(), app.MutationAppend{Token: migration.MutationToken(), Operation: app.MutationPut, ObjectKey: "local-committed"}); err != nil {
		t.Fatal(err)
	}
	if err := store.FailMutation(context.Background(), migration.MutationToken(), "mutation_journal_failed"); err != nil {
		t.Fatal(err)
	}
	stored, found, err := store.Load(context.Background())
	if err != nil || !found || stored.State != app.StateApplyFailed || stored.ErrorCode != "mutation_journal_failed" {
		t.Fatalf("failed migration=%#v found=%t err=%v", stored, found, err)
	}
	if count := sqliteJournalCount(t, db, migration.ID); count != 0 {
		t.Fatalf("fail mutation journal count=%d", count)
	}
}

func TestSQLiteStoreMutationTokenAppendsConcurrentlyWithoutWorkerLease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mutation.db")
	firstDB, first := newSQLiteStore(t, path)
	defer firstDB.Close()
	secondDB, second := newSQLiteStore(t, path)
	defer secondDB.Close()
	migration, err := first.CompareAndSwap(context.Background(), 0, validMigration("token", 1))
	if err != nil {
		t.Fatal(err)
	}
	token := migration.MutationToken()
	var group sync.WaitGroup
	start := make(chan struct{})
	results := make(chan app.Mutation, 2)
	errs := make(chan error, 2)
	for index, store := range []*Store{first, second} {
		group.Add(1)
		go func(index int, store *Store) {
			defer group.Done()
			<-start
			mutation, err := store.AppendMutation(context.Background(), app.MutationAppend{Token: token, Operation: app.MutationPut, ObjectKey: "key-" + string(rune('a'+index))})
			if err != nil {
				errs <- err
				return
			}
			results <- mutation
		}(index, store)
	}
	close(start)
	group.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Errorf("append=%v", err)
	}
	seen := map[uint64]bool{}
	for mutation := range results {
		seen[mutation.Sequence] = true
	}
	if len(seen) != 2 || !seen[1] || !seen[2] {
		t.Fatalf("allocated sequences=%v", seen)
	}
	loaded, found, err := first.Load(context.Background())
	if err != nil || !found || loaded.MutationHighWater != 2 {
		t.Fatalf("migration=%#v found=%t err=%v", loaded, found, err)
	}
	if _, err := second.AppendMutation(context.Background(), app.MutationAppend{Token: app.MutationToken{MigrationID: token.MigrationID, Generation: token.Generation + 1, DesiredRevision: token.DesiredRevision}, Operation: app.MutationDelete, ObjectKey: "stale"}); !errors.Is(err, app.ErrMutationConflict) {
		t.Fatalf("stale token error=%v", err)
	}
}

func TestSQLiteStoreWorkerProgressPreservesConcurrentJournalHighWater(t *testing.T) {
	db, store := newSQLiteStore(t, filepath.Join(t.TempDir(), "progress-journal.db"))
	defer db.Close()
	_, err := store.CompareAndSwap(context.Background(), 0, validMigration("progress", 1))
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.ClaimLease(context.Background(), "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	migration, found, err := store.Load(context.Background())
	if err != nil || !found {
		t.Fatalf("load claimed=%#v found=%t err=%v", migration, found, err)
	}
	for index := 0; index < 3; index++ {
		if _, err := store.AppendMutation(context.Background(), app.MutationAppend{Token: migration.MutationToken(), Operation: app.MutationPut, ObjectKey: fmt.Sprintf("key-%d", index)}); err != nil {
			t.Fatal(err)
		}
	}
	migration.CopiedObjects = 1
	updated, err := store.UpdateLeaseHeld(context.Background(), lease, migration.StoreRevision, migration)
	if err != nil || updated.MutationHighWater != 3 || updated.CopiedObjects != 1 {
		t.Fatalf("worker update=%#v err=%v", updated, err)
	}
}

func TestPostgresStoreCASAndTwoHandleLease(t *testing.T) {
	firstDB, secondDB := newPostgresStores(t)
	first, err := New(firstDB, sqlkit.Postgres)
	if err != nil {
		t.Fatal(err)
	}
	second, err := New(secondDB, sqlkit.Postgres)
	if err != nil {
		t.Fatal(err)
	}
	initial, err := first.CompareAndSwap(context.Background(), 0, validMigration("postgres", 1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.CompareAndSwap(context.Background(), initial.StoreRevision, validMigration("postgres-stale", 1)); !errors.Is(err, app.ErrMigrationConflict) {
		t.Fatalf("postgres stale CAS=%v", err)
	}
	start := make(chan struct{})
	type claimResult struct {
		lease app.Lease
		err   error
	}
	claims := make(chan claimResult, 2)
	var group sync.WaitGroup
	for index, store := range []*Store{first, second} {
		group.Add(1)
		go func(index int, store *Store) {
			defer group.Done()
			<-start
			lease, err := store.ClaimLease(context.Background(), fmt.Sprintf("postgres-worker-%d", index), time.Minute)
			claims <- claimResult{lease: lease, err: err}
		}(index, store)
	}
	close(start)
	group.Wait()
	close(claims)
	claimed, conflicted := 0, 0
	var claimedLease app.Lease
	for result := range claims {
		switch {
		case result.err == nil:
			claimed++
			claimedLease = result.lease
		case errors.Is(result.err, app.ErrLeaseConflict):
			conflicted++
		default:
			t.Fatalf("postgres lease claim=%v", result.err)
		}
	}
	if claimed != 1 || conflicted != 1 {
		t.Fatalf("postgres claims=%d conflicts=%d", claimed, conflicted)
	}
	// PostgreSQL executes the sequence allocation through UPDATE ... RETURNING
	// in AppendMutation. Use separate handles concurrently so this proves the
	// transaction and row lock, rather than only inspecting a bound query.
	appends := make(chan app.Mutation, 2)
	appendErrs := make(chan error, 2)
	start = make(chan struct{})
	for index, store := range []*Store{first, second} {
		group.Add(1)
		go func(index int, store *Store) {
			defer group.Done()
			<-start
			mutation, err := store.AppendMutation(context.Background(), app.MutationAppend{
				Token: initial.MutationToken(), Operation: app.MutationPut, ObjectKey: fmt.Sprintf("postgres-key-%d", index),
			})
			if err != nil {
				appendErrs <- err
				return
			}
			appends <- mutation
		}(index, store)
	}
	close(start)
	group.Wait()
	close(appends)
	close(appendErrs)
	for err := range appendErrs {
		t.Fatalf("postgres concurrent append=%v", err)
	}
	sequences := map[uint64]bool{}
	for mutation := range appends {
		sequences[mutation.Sequence] = true
	}
	if len(sequences) != 2 || !sequences[1] || !sequences[2] {
		t.Fatalf("postgres append sequences=%v", sequences)
	}
	loaded, found, err := first.Load(context.Background())
	if err != nil || !found || loaded.MutationHighWater != 2 {
		t.Fatalf("postgres append high-water=%#v found=%t err=%v", loaded, found, err)
	}
	for _, timestamp := range []time.Time{loaded.CreatedAt, loaded.UpdatedAt, claimedLease.ExpiresAt} {
		if timestamp.IsZero() || timestamp.Nanosecond()%int(time.Millisecond) != 0 {
			t.Fatalf("postgres timestamp is not millisecond precision: %s", timestamp)
		}
	}
	if err := second.ReleaseLease(context.Background(), claimedLease); err != nil {
		t.Fatalf("postgres release lease=%v", err)
	}
	loaded, found, err = first.Load(context.Background())
	if err != nil || !found || loaded.LeaseOwner != "" || !loaded.LeaseExpiresAt.IsZero() {
		t.Fatalf("postgres released lease=%#v found=%t err=%v", loaded, found, err)
	}
	claimedLease, err = first.ClaimLease(context.Background(), "postgres-terminal", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	loaded, found, err = first.Load(context.Background())
	if err != nil || !found {
		t.Fatalf("postgres terminal load=%#v found=%t err=%v", loaded, found, err)
	}
	terminal := loaded
	terminal.State = app.StateCancelled
	terminal.LeaseOwner, terminal.LeaseExpiresAt = "", time.Time{}
	if _, err := first.UpdateLeaseHeld(context.Background(), claimedLease, loaded.StoreRevision, terminal); err != nil {
		t.Fatal(err)
	}
	if count := postgresJournalCount(t, firstDB, initial.ID); count != 0 {
		t.Fatalf("postgres terminal journal count=%d", count)
	}
}

func sqliteJournalCount(t *testing.T, db *sql.DB, migrationID string) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM artifact_migration_mutations WHERE migration_id = ?`, migrationID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func postgresJournalCount(t *testing.T, db *sql.DB, migrationID string) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM artifact_migration_mutations WHERE migration_id = $1`, migrationID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func newSQLiteStore(t *testing.T, path string) (*sql.DB, *Store) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout%285000%29&_pragma=journal_mode%28WAL%29")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := storage.OpenSQLSessionStore(context.Background(), db, storage.SQLDialectSQLite); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	store, err := New(db, sqlkit.SQLite)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	return db, store
}

func validMigration(id string, generation uint64) app.Migration {
	return app.Migration{ID: "migration-" + id, DesiredRevision: "rev-" + id, Source: app.BackendIdentity{Backend: app.BackendLocal, Identity: "local"}, Target: app.BackendIdentity{Backend: app.BackendS3, Identity: "remote"}, State: app.StateSyncing, Generation: generation}
}

func newPostgresStores(t *testing.T) (*sql.DB, *sql.DB) {
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
		t.Fatal(err)
	}
	schema := fmt.Sprintf("artifactmigration_test_%d", time.Now().UnixNano())
	if _, err := adminDB.ExecContext(ctx, "CREATE SCHEMA "+schema); err != nil {
		_ = adminDB.Close()
		t.Fatal(err)
	}
	open := func() *sql.DB {
		config, err := pgx.ParseConfig(dsn)
		if err != nil {
			t.Fatal(err)
		}
		config.RuntimeParams["search_path"] = schema
		db := stdlib.OpenDB(*config)
		if err := db.PingContext(ctx); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
		return db
	}
	firstDB, secondDB := open(), open()
	if _, err := storage.OpenSQLSessionStore(ctx, firstDB, storage.SQLDialectPostgres); err != nil {
		_ = firstDB.Close()
		_ = secondDB.Close()
		_, _ = adminDB.ExecContext(ctx, "DROP SCHEMA "+schema+" CASCADE")
		_ = adminDB.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = firstDB.Close()
		_ = secondDB.Close()
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_, _ = adminDB.ExecContext(cleanupCtx, "DROP SCHEMA "+schema+" CASCADE")
		_ = adminDB.Close()
	})
	return firstDB, secondDB
}
