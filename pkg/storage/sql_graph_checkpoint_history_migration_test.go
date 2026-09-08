package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/whhhh1500/auto-agent/pkg/extensions/graph"
)

func TestSQLiteGraphCheckpointHistorySchemaV41FreshAndV40Upgrade(t *testing.T) {
	testGraphCheckpointHistorySchemaV41(t, SQLDialectSQLite, newTestSQLStore(t).db)
}

func TestSQLiteGraphCheckpointHistoryV41FreshPrivateMemoryDatabase(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectSQLite); err != nil {
		t.Fatalf("fresh private in-memory open: %v", err)
	}
	assertGraphCheckpointHistoryTable(t, ctx, db, SQLDialectSQLite)
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectSQLite); err != nil {
		t.Fatalf("repeat private in-memory open: %v", err)
	}
}

func TestPostgresGraphCheckpointHistorySchemaV41FreshAndV40Upgrade(t *testing.T) {
	testGraphCheckpointHistorySchemaV41(t, SQLDialectPostgres, newPostgresTestDB(t))
}

func TestSQLiteGraphCheckpointHistoryV41RejectsWeakShape(t *testing.T) {
	testGraphCheckpointHistoryV41RejectsWeakShape(t, SQLDialectSQLite, newTestSQLStore(t).db)
}

func TestPostgresGraphCheckpointHistoryV41RejectsWeakShape(t *testing.T) {
	testGraphCheckpointHistoryV41RejectsWeakShape(t, SQLDialectPostgres, newPostgresTestDB(t))
}

func TestSQLiteGraphCheckpointHistoryV41RejectsWeakenedWriteFence(t *testing.T) {
	testGraphCheckpointHistoryV41RejectsWeakenedWriteFence(t, SQLDialectSQLite, newTestSQLStore(t).db)
}

func TestPostgresGraphCheckpointHistoryV41RejectsWeakenedWriteFence(t *testing.T) {
	testGraphCheckpointHistoryV41RejectsWeakenedWriteFence(t, SQLDialectPostgres, newPostgresTestDB(t))
}

func testGraphCheckpointHistoryV41RejectsWeakShape(t *testing.T, dialect SQLDialect, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "missing_composite_primary_key",
			sql:  weakGraphCheckpointVersionTable(`UNIQUE (version_id)`),
			want: "composite primary key",
		},
		{
			name: "missing_version_id_unique",
			sql:  weakGraphCheckpointVersionTable(`PRIMARY KEY (tenant_id, session_id, run_id, revision)`),
			want: "version_id unique constraint",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			downgradeGraphCheckpointHistoryToV40(t, ctx, db, dialect)
			if _, err := db.ExecContext(ctx, sqlUpdateMetaRow.bind(dialect), strconv.Itoa(SQLSchemaVersion)); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, test.sql); err != nil {
				t.Fatal(err)
			}
			if _, err := OpenSQLSessionStore(ctx, db, dialect); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("weak v41 schema error=%v, want %q", err, test.want)
			}
		})
	}
}

func weakGraphCheckpointVersionTable(constraint string) string {
	return `CREATE TABLE graph_checkpoint_versions (
		tenant_id TEXT NOT NULL,
		session_id TEXT NOT NULL,
		run_id TEXT NOT NULL,
		revision BIGINT NOT NULL,
		version_id TEXT NOT NULL,
		parent_version_id TEXT NOT NULL,
		origin TEXT NOT NULL,
		checkpoint_hash TEXT NOT NULL,
		checkpoint_json TEXT NOT NULL,
		created_at BIGINT NOT NULL,
		` + constraint + `
	)`
}

func testGraphCheckpointHistoryV41RejectsWeakenedWriteFence(t *testing.T, dialect SQLDialect, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err != nil {
		t.Fatal(err)
	}
	seedGraphCheckpointHistoryFenceHead(t, ctx, db, dialect)
	var statements []string
	if dialect == SQLDialectSQLite {
		weakened := strings.Replace(sqlGraphCheckpointHistorySQLiteHeadUpdateTrigger, "\n\tBEGIN SELECT", " AND FALSE\n\tBEGIN SELECT", 1)
		if weakened == sqlGraphCheckpointHistorySQLiteHeadUpdateTrigger {
			t.Fatal("failed to construct weakened SQLite write fence")
		}
		statements = []string{
			`DROP TRIGGER graph_checkpoint_history_head_update`,
			weakened,
		}
	} else {
		weakened := strings.Replace(sqlGraphCheckpointHistoryPostgresHeadFenceFunction, ") THEN", ") AND FALSE THEN", 1)
		if weakened == sqlGraphCheckpointHistoryPostgresHeadFenceFunction {
			t.Fatal("failed to construct weakened PostgreSQL write fence")
		}
		statements = []string{
			weakened,
		}
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	legacyJSON := `{"head":"legacy"}`
	result, err := db.ExecContext(ctx, sqlQuery{`UPDATE graph_checkpoints SET revision = ?, checkpoint_json = ?
		WHERE tenant_id = ? AND session_id = ? AND run_id = ?`}.bind(dialect),
		2, legacyJSON, "fence-tenant", "fence-session", "fence-run")
	if err != nil {
		t.Fatalf("weak fence rejected legacy head-only update: %v", err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		t.Fatalf("weak fence legacy update changed=%d err=%v", changed, err)
	}
	var revision int
	if err := db.QueryRowContext(ctx, sqlQuery{`SELECT revision FROM graph_checkpoints
		WHERE tenant_id = ? AND session_id = ? AND run_id = ?`}.bind(dialect), "fence-tenant", "fence-session", "fence-run").Scan(&revision); err != nil || revision != 2 {
		t.Fatalf("legacy head update revision=%d err=%v", revision, err)
	}
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err == nil || !strings.Contains(err.Error(), "write fence") {
		t.Fatalf("weakened v41 write fence error=%v", err)
	}
}

func seedGraphCheckpointHistoryFenceHead(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect) {
	t.Helper()
	const tenantID = "fence-tenant"
	const sessionID = "fence-session"
	const runID = "fence-run"
	const checkpointJSON = `{"head":"before"}`
	if _, err := db.ExecContext(ctx, sqlQuery{`INSERT INTO graph_checkpoint_versions
		(tenant_id, session_id, run_id, revision, version_id, parent_version_id, origin, checkpoint_hash, checkpoint_json, created_at)
		VALUES (?, ?, ?, 1, ?, '', 'commit', ?, ?, 1)`}.bind(dialect),
		tenantID, sessionID, runID, strings.Repeat("a", 64), strings.Repeat("b", 64), checkpointJSON); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, sqlQuery{`INSERT INTO graph_transitions
		(tenant_id, session_id, run_id, revision, transition_id, transition_json)
		VALUES (?, ?, ?, 1, ?, '{}')`}.bind(dialect), tenantID, sessionID, runID, strings.Repeat("c", 64)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, sqlQuery{`INSERT INTO graph_checkpoints
		(tenant_id, session_id, run_id, revision, checkpoint_json) VALUES (?, ?, ?, 1, ?)`}.bind(dialect),
		tenantID, sessionID, runID, checkpointJSON); err != nil {
		t.Fatal(err)
	}
}

func testGraphCheckpointHistorySchemaV41(t *testing.T, dialect SQLDialect, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err != nil {
		t.Fatalf("open fresh schema: %v", err)
	}
	assertGraphCheckpointHistoryTable(t, ctx, db, dialect)

	checkpoint := migrationCheckpoint("run-history", 2)
	canonical := mustMigrationCheckpointJSON(t, checkpoint)
	raw := "\n" + canonical + "\n"
	downgradeGraphCheckpointHistoryToV40(t, ctx, db, dialect)
	if _, err := db.ExecContext(ctx, sqlQuery{`INSERT INTO graph_checkpoints
		(tenant_id, session_id, run_id, revision, checkpoint_json) VALUES (?, ?, ?, ?, ?)`}.bind(dialect),
		checkpoint.Key.TenantID, checkpoint.Key.SessionID, checkpoint.Key.RunID, checkpoint.Revision, raw); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err != nil {
		t.Fatalf("v40 to v41 migration: %v", err)
	}
	var id, parentID, origin, hash, stored string
	var revision uint64
	var createdAt int64
	if err := db.QueryRowContext(ctx, sqlQuery{`SELECT revision, version_id, parent_version_id, origin, checkpoint_hash, checkpoint_json, created_at
		FROM graph_checkpoint_versions WHERE tenant_id = ? AND session_id = ? AND run_id = ?`}.bind(dialect),
		checkpoint.Key.TenantID, checkpoint.Key.SessionID, checkpoint.Key.RunID).Scan(&revision, &id, &parentID, &origin, &hash, &stored, &createdAt); err != nil {
		t.Fatal(err)
	}
	version, err := graph.ValidateCheckpointVersion(graph.CheckpointVersion{
		Info:       graph.CheckpointVersionInfo{ID: id, ParentID: parentID, Origin: graph.CheckpointVersionOrigin(origin), Key: checkpoint.Key, Revision: revision, CheckpointHash: hash, CreatedAt: createdAt},
		Checkpoint: checkpoint,
	})
	if err != nil || version.Info.Origin != graph.CheckpointVersionOriginMigrationFloor || version.Info.ParentID != "" || version.Info.Revision != 2 || stored != canonical {
		t.Fatalf("migrated floor=%#v stored=%q err=%v", version, stored, err)
	}
	var count int
	if err := db.QueryRowContext(ctx, sqlQuery{`SELECT COUNT(*) FROM graph_checkpoint_versions
		WHERE tenant_id = ? AND session_id = ? AND run_id = ? AND revision < ?`}.bind(dialect),
		checkpoint.Key.TenantID, checkpoint.Key.SessionID, checkpoint.Key.RunID, checkpoint.Revision).Scan(&count); err != nil || count != 0 {
		t.Fatalf("migration fabricated older history count=%d err=%v", count, err)
	}
	legacy := migrationCheckpoint("run-history", 3)
	if _, err := db.ExecContext(ctx, sqlQuery{`UPDATE graph_checkpoints SET revision = ?, checkpoint_json = ?
		WHERE tenant_id = ? AND session_id = ? AND run_id = ?`}.bind(dialect),
		legacy.Revision, mustMigrationCheckpointJSON(t, legacy), legacy.Key.TenantID, legacy.Key.SessionID, legacy.Key.RunID); err == nil {
		t.Fatal("v41 write fence accepted a legacy head-only update")
	}

	corrupt := migrationCheckpoint("run-corrupt", 1)
	downgradeGraphCheckpointHistoryToV40(t, ctx, db, dialect)
	if _, err := db.ExecContext(ctx, sqlQuery{`INSERT INTO graph_checkpoints
		(tenant_id, session_id, run_id, revision, checkpoint_json) VALUES (?, ?, ?, ?, ?)`}.bind(dialect),
		corrupt.Key.TenantID, corrupt.Key.SessionID, corrupt.Key.RunID, corrupt.Revision, `{"unknown_field":true}`); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLSessionStore(ctx, db, dialect); !errors.Is(err, graph.ErrInvalidCheckpoint) {
		t.Fatalf("corrupt v40 checkpoint migration error=%v", err)
	}
	if exists, err := sqlTableExists(ctx, db, dialect, "graph_checkpoint_versions"); err != nil || exists {
		t.Fatalf("failed migration retained history table exists=%t err=%v", exists, err)
	}
	var marker string
	if err := db.QueryRowContext(ctx, sqlSelectMetaRow.bind(dialect)).Scan(&marker); err != nil || marker != "40" {
		t.Fatalf("failed migration advanced marker=%q err=%v", marker, err)
	}
}

func TestSQLiteGraphCheckpointHistoryV41ConcurrentOpenBackfillsOnce(t *testing.T) {
	path := t.TempDir() + "/concurrent-v41.db"
	setup, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := OpenSQLSessionStore(ctx, setup, SQLDialectSQLite); err != nil {
		_ = setup.Close()
		t.Fatal(err)
	}
	prepareGraphCheckpointHistoryV40Fixture(t, ctx, setup, SQLDialectSQLite)
	if err := setup.Close(); err != nil {
		t.Fatal(err)
	}
	first, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	second, err := sql.Open("sqlite", path)
	if err != nil {
		_ = first.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Close() })
	t.Cleanup(func() { _ = second.Close() })
	assertConcurrentGraphCheckpointHistoryOpen(t, ctx, SQLDialectSQLite, first, second, 2)
}

func TestPostgresGraphCheckpointHistoryV41ConcurrentOpenBackfillsOnce(t *testing.T) {
	ctx := context.Background()
	first := newPostgresTestDB(t)
	if _, err := OpenSQLSessionStore(ctx, first, SQLDialectPostgres); err != nil {
		t.Fatal(err)
	}
	prepareGraphCheckpointHistoryV40Fixture(t, ctx, first, SQLDialectPostgres)
	var schema string
	if err := first.QueryRowContext(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	second := newPostgresGraphCheckpointHistoryHandle(t, schema)
	second.SetMaxOpenConns(1)
	second.SetMaxIdleConns(1)
	assertConcurrentGraphCheckpointHistoryOpen(t, ctx, SQLDialectPostgres, first, second, 2)
}

func TestPostgresGraphCheckpointHistoryV41OpenUnderStartupLock(t *testing.T) {
	db := newPostgresTestDB(t)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	openUnderStartupLock := func() error {
		lock, err := AcquirePostgresStartupLock(ctx, db)
		if err != nil {
			return err
		}
		defer func() {
			releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer releaseCancel()
			if releaseErr := lock.Release(releaseCtx); releaseErr != nil && err == nil {
				err = releaseErr
			}
		}()
		_, err = lock.OpenSQLSessionStore(ctx)
		return err
	}
	if err := openUnderStartupLock(); err != nil {
		t.Fatalf("fresh open under startup lock: %v", err)
	}
	prepareGraphCheckpointHistoryV40Fixture(t, ctx, db, SQLDialectPostgres)
	if err := openUnderStartupLock(); err != nil {
		t.Fatalf("v40 open under startup lock: %v", err)
	}
	var floors int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM graph_checkpoint_versions WHERE origin = 'migration_floor'`).Scan(&floors); err != nil || floors != 2 {
		t.Fatalf("v40 floors under startup lock=%d err=%v", floors, err)
	}
}

func TestPostgresGraphCheckpointHistoryV41V40OpenWithSingleConnection(t *testing.T) {
	db := newPostgresTestDB(t)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres); err != nil {
		t.Fatalf("fresh single-connection open: %v", err)
	}
	prepareGraphCheckpointHistoryV40Fixture(t, ctx, db, SQLDialectPostgres)
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres); err != nil {
		t.Fatalf("v40 single-connection open: %v", err)
	}
	var floors int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM graph_checkpoint_versions WHERE origin = 'migration_floor'`).Scan(&floors); err != nil || floors != 2 {
		t.Fatalf("v40 floors with one connection=%d err=%v", floors, err)
	}
}

func TestPostgresSchemaMigrationLockSerializesDifferentSearchPathsInSameDatabase(t *testing.T) {
	ctx := context.Background()
	first := newPostgresTestDB(t)
	second := newPostgresTestDB(t)
	var firstSchema, secondSchema string
	if err := first.QueryRowContext(ctx, `SELECT current_schema()`).Scan(&firstSchema); err != nil {
		t.Fatal(err)
	}
	if err := second.QueryRowContext(ctx, `SELECT current_schema()`).Scan(&secondSchema); err != nil {
		t.Fatal(err)
	}
	if firstSchema == secondSchema {
		t.Fatalf("PostgreSQL test handles unexpectedly share search_path schema %q", firstSchema)
	}

	lock, err := acquirePostgresSchemaMigrationLock(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if releaseErr := lock.release(releaseCtx); releaseErr != nil {
			t.Errorf("release first schema migration lock: %v", releaseErr)
		}
	}()

	acquired, err := tryPostgresSchemaMigrationLock(ctx, second)
	if err != nil {
		t.Fatal(err)
	}
	if acquired {
		t.Fatal("independent handle acquired schema migration lock for the same PostgreSQL database")
	}
	if err := lock.release(ctx); err != nil {
		t.Fatal(err)
	}
	acquired, err = tryPostgresSchemaMigrationLock(ctx, second)
	if err != nil {
		t.Fatal(err)
	}
	if !acquired {
		t.Fatal("schema migration lock remained held after release")
	}
}

func TestPostgresSchemaMigrationLockIsScopedPerDatabase(t *testing.T) {
	ctx := context.Background()
	first := newPostgresTestDB(t)
	var currentDatabase string
	if err := first.QueryRowContext(ctx, `SELECT current_database()`).Scan(&currentDatabase); err != nil {
		t.Fatal(err)
	}
	otherDatabase := "template1"
	if currentDatabase == otherDatabase {
		otherDatabase = "postgres"
	}
	second := newPostgresDatabaseHandle(t, otherDatabase)
	lock, err := acquirePostgresSchemaMigrationLock(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if releaseErr := lock.release(releaseCtx); releaseErr != nil {
			t.Errorf("release first database schema migration lock: %v", releaseErr)
		}
	}()
	acquired, err := tryPostgresSchemaMigrationLock(ctx, second)
	if err != nil {
		t.Fatal(err)
	}
	if !acquired {
		t.Fatalf("schema migration lock in database %q blocked database %q", currentDatabase, otherDatabase)
	}
}

func TestPostgresSchemaMigrationUnlockFailureDiscardsSession(t *testing.T) {
	db := newPostgresTestDB(t)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	ctx := context.Background()
	var schema string
	if err := db.QueryRowContext(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	independent := newPostgresGraphCheckpointHistoryHandle(t, schema)
	lock, err := acquirePostgresSchemaMigrationLock(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	var lockedBackend, replacementBackend int
	if err := lock.conn.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&lockedBackend); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := lock.release(canceled); err == nil {
		t.Fatal("schema migration unlock unexpectedly succeeded with canceled context")
	}
	acquired, err := tryPostgresAdvisoryLock(ctx, independent, postgresSchemaMigrationLockKey)
	if err != nil {
		t.Fatal(err)
	}
	if !acquired {
		t.Fatal("discarded schema migration session retained its advisory lock")
	}
	if err := db.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&replacementBackend); err != nil {
		t.Fatal(err)
	}
	if replacementBackend == lockedBackend {
		t.Fatalf("schema migration pool reused discarded backend %d", lockedBackend)
	}
}

func TestPostgresSchemaMigrationLockReleasesAfterOpenFailure(t *testing.T) {
	db := newPostgresTestDB(t)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE TABLE store_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO store_meta (key, value) VALUES ('schema_version', '999')`); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres); err == nil {
		t.Fatal("future PostgreSQL schema was accepted")
	}
	acquired, err := tryPostgresSchemaMigrationLock(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if !acquired {
		t.Fatal("failed OpenSQLSessionStore retained the schema migration lock")
	}
}

func TestPostgresSchemaMigrationLockWaitCancellationDoesNotStrandOpen(t *testing.T) {
	first := newPostgresTestDB(t)
	ctx := context.Background()
	var schema string
	if err := first.QueryRowContext(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	second := newPostgresGraphCheckpointHistoryHandle(t, schema)
	second.SetMaxOpenConns(1)
	second.SetMaxIdleConns(1)
	independent := newPostgresGraphCheckpointHistoryHandle(t, schema)
	lock, err := acquirePostgresSchemaMigrationLock(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if releaseErr := lock.release(releaseCtx); releaseErr != nil {
			t.Errorf("release first schema migration lock: %v", releaseErr)
		}
	}()

	waitCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if _, err := OpenSQLSessionStore(waitCtx, second, SQLDialectPostgres); err == nil {
		t.Fatal("OpenSQLSessionStore succeeded while another handle held the schema migration lock")
	}
	if err := lock.release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLSessionStore(ctx, second, SQLDialectPostgres); err != nil {
		t.Fatalf("OpenSQLSessionStore after canceled lock wait: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		acquired, err := tryPostgresSchemaMigrationLock(ctx, independent)
		if err != nil {
			t.Fatal(err)
		}
		if acquired {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("canceled schema lock acquisition left a hidden session lock")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func tryPostgresSchemaMigrationLock(ctx context.Context, db *sql.DB) (acquired bool, err error) {
	return tryPostgresAdvisoryLock(ctx, db, postgresSchemaMigrationLockKey)
}

func tryPostgresAdvisoryLock(ctx context.Context, db *sql.DB, key int64) (acquired bool, err error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return false, err
	}
	defer func() {
		if closeErr := conn.Close(); err == nil && closeErr != nil {
			err = closeErr
		}
	}()
	if err := conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", key).Scan(&acquired); err != nil {
		return false, err
	}
	if !acquired {
		return false, nil
	}
	var unlocked bool
	if err := conn.QueryRowContext(ctx, "SELECT pg_advisory_unlock($1)", key).Scan(&unlocked); err != nil {
		return false, err
	}
	if !unlocked {
		return false, errors.New("postgres schema migration lock was not held after successful try lock")
	}
	return true, nil
}

func prepareGraphCheckpointHistoryV40Fixture(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect) {
	t.Helper()
	downgradeGraphCheckpointHistoryToV40(t, ctx, db, dialect)
	for _, checkpoint := range []graph.Checkpoint{
		migrationCheckpoint("run-concurrent-one", 2),
		migrationCheckpoint("run-concurrent-two", 3),
	} {
		if _, err := db.ExecContext(ctx, sqlQuery{`INSERT INTO graph_checkpoints
			(tenant_id, session_id, run_id, revision, checkpoint_json) VALUES (?, ?, ?, ?, ?)`}.bind(dialect),
			checkpoint.Key.TenantID, checkpoint.Key.SessionID, checkpoint.Key.RunID, checkpoint.Revision, mustMigrationCheckpointJSON(t, checkpoint)); err != nil {
			t.Fatal(err)
		}
	}
}

func assertConcurrentGraphCheckpointHistoryOpen(t *testing.T, ctx context.Context, dialect SQLDialect, first, second *sql.DB, floors int) {
	t.Helper()
	start := make(chan struct{})
	results := make(chan error, 2)
	for _, db := range []*sql.DB{first, second} {
		go func(db *sql.DB) {
			<-start
			_, err := OpenSQLSessionStore(ctx, db, dialect)
			results <- err
		}(db)
	}
	close(start)
	for index := 0; index < 2; index++ {
		if err := <-results; err != nil {
			t.Fatalf("concurrent v41 open %d: %v", index, err)
		}
	}
	var count int
	if err := first.QueryRowContext(ctx, `SELECT COUNT(*) FROM graph_checkpoint_versions WHERE origin = 'migration_floor'`).Scan(&count); err != nil || count != floors {
		t.Fatalf("migration floors=%d err=%v want=%d", count, err, floors)
	}
	if err := first.QueryRowContext(ctx, `SELECT COUNT(*) FROM graph_checkpoint_versions`).Scan(&count); err != nil || count != floors {
		t.Fatalf("migration versions=%d err=%v want=%d", count, err, floors)
	}
	assertGraphCheckpointHistoryTable(t, ctx, first, dialect)
}

func downgradeGraphCheckpointHistoryToV40(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect) {
	t.Helper()
	statements := []string{`DROP TRIGGER IF EXISTS graph_checkpoint_history_head_insert`, `DROP TRIGGER IF EXISTS graph_checkpoint_history_head_update`}
	if dialect == SQLDialectPostgres {
		statements = []string{`DROP TRIGGER IF EXISTS graph_checkpoint_history_head_fence ON graph_checkpoints`}
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(ctx, `DROP TABLE graph_checkpoint_versions`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, sqlUpdateMetaRow.bind(dialect), "40"); err != nil {
		t.Fatal(err)
	}
}

func newPostgresGraphCheckpointHistoryHandle(t *testing.T, schema string) *sql.DB {
	t.Helper()
	dsn := os.Getenv("HARNESS_TEST_PG_DSN")
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.RuntimeParams["search_path"] = schema
	db := stdlib.OpenDB(*config)
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(2)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func newPostgresDatabaseHandle(t *testing.T, database string) *sql.DB {
	t.Helper()
	dsn := os.Getenv("HARNESS_TEST_PG_DSN")
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.Database = database
	delete(config.RuntimeParams, "search_path")
	db := stdlib.OpenDB(*config)
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		t.Fatalf("connect PostgreSQL database %q: %v", database, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func assertGraphCheckpointHistoryTable(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect) {
	t.Helper()
	exists, err := sqlTableExists(ctx, db, dialect, "graph_checkpoint_versions")
	if err != nil || !exists {
		t.Fatalf("history table exists=%t err=%v", exists, err)
	}
	for _, column := range []string{"tenant_id", "session_id", "run_id", "revision", "version_id", "parent_version_id", "origin", "checkpoint_hash", "checkpoint_json", "created_at"} {
		exists, err := sqlColumnExists(ctx, db, dialect, "graph_checkpoint_versions", column)
		if err != nil || !exists {
			t.Fatalf("history column %s exists=%t err=%v", column, exists, err)
		}
	}
	if err := verifyGraphCheckpointHistoryV41(ctx, db, dialect); err != nil {
		t.Fatalf("history v41 fence/schema verification: %v", err)
	}
	var marker string
	if err := db.QueryRowContext(ctx, sqlSelectMetaRow.bind(dialect)).Scan(&marker); err != nil || marker != strconv.Itoa(SQLSchemaVersion) {
		t.Fatalf("fresh marker=%q err=%v", marker, err)
	}
}

func migrationCheckpoint(runID string, revision uint64) graph.Checkpoint {
	return graph.Checkpoint{
		Key:                    graph.CheckpointKey{TenantID: "tenant", SessionID: "session", RunID: runID},
		SegmentID:              "segment",
		GraphID:                "graph",
		DefinitionRevision:     "definition",
		CompositionRevision:    "composition",
		ImplementationRevision: "implementation",
		CurrentNodeID:          "node",
		Revision:               revision,
		Status:                 graph.CheckpointReady,
		Visits:                 map[string]int{"node": 1},
		State:                  graph.State{"value": []byte(`"ok"`)},
	}
}

func mustMigrationCheckpointJSON(t *testing.T, checkpoint graph.Checkpoint) string {
	t.Helper()
	validated, err := graph.ValidateCheckpoint(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(validated)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}
