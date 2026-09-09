package storage

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/whhhh1500/auto-agent/pkg/extensions/graph"
)

var (
	sqlInsertMetaRow         = sqlQuery{"INSERT INTO store_meta (key, value) VALUES ('schema_version', ?)"}
	sqlUpdateMetaRow         = sqlQuery{"UPDATE store_meta SET value = ? WHERE key = 'schema_version'"}
	sqlSelectMetaRow         = sqlQuery{"SELECT value FROM store_meta WHERE key = 'schema_version'"}
	sqlSelectControlRevision = sqlQuery{"SELECT value FROM store_meta WHERE key = 'control_revision'"}
	sqlBumpControlRevision   = sqlQuery{"UPDATE store_meta SET value = CAST(CAST(value AS BIGINT) + 1 AS TEXT) WHERE key = 'control_revision'"}
)

const graphCheckpointHistorySchemaVersionV41 = 41

const obsHitSummarySchemaVersionV48 = 48

// postgresSchemaMigrationLockKey serializes PostgreSQL OpenSQLSessionStore
// schema work. It deliberately differs from postgresStartupLockKey: callers
// can hold the initial-admin bootstrap lock while opening the store, and a
// nested acquisition of that same session-level key on another pooled
// connection would self-deadlock.
//
// This key is database-wide rather than derived from current_schema(). Schema
// inspection and DDL use search_path, but a global migration gate prevents two
// independently configured schemas from acquiring PostgreSQL catalog locks in
// different migration phases. Store open is a startup operation, so that
// conservative serialization is preferable to a catalog-lock deadlock.
const postgresSchemaMigrationLockKey int64 = 0x4843534348454D41 // "HCSCHEMA"

type postgresSchemaMigrationLock = postgresSessionAdvisoryLock

func acquirePostgresSchemaMigrationLock(ctx context.Context, db *sql.DB) (*postgresSchemaMigrationLock, error) {
	return acquirePostgresSessionAdvisoryLock(ctx, db, postgresSchemaMigrationLockKey, "postgres schema migration lock")
}

// OpenSQLSessionStore opens (and if needed creates) the schema on an open
// database handle. Schema version mismatches fail closed. PostgreSQL requires
// a direct server connection or a session-pooling proxy; transaction pooling
// cannot preserve the session-level migration lock.
func OpenSQLSessionStore(ctx context.Context, db *sql.DB, dialect SQLDialect) (store *SQLSessionStore, err error) {
	if db == nil {
		return nil, fmt.Errorf("sql session store requires a database handle")
	}
	if err := validateSQLDialect(dialect); err != nil {
		return nil, err
	}
	if dialect == SQLDialectSQLite {
		for _, pragma := range []string{
			"PRAGMA journal_mode=WAL", "PRAGMA busy_timeout=5000", "PRAGMA synchronous=NORMAL",
		} {
			if err := applySQLiteStartupPragma(ctx, db, pragma); err != nil {
				return nil, fmt.Errorf("apply sqlite pragma: %w", err)
			}
		}
	}
	var schema sqlSchemaExecutor = db
	if dialect == SQLDialectPostgres {
		lock, lockErr := acquirePostgresSchemaMigrationLock(ctx, db)
		if lockErr != nil {
			return nil, lockErr
		}
		schema = lock.conn
		defer func() {
			releaseCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if releaseErr := lock.release(releaseCtx); releaseErr != nil {
				store = nil
				err = errors.Join(err, releaseErr)
			}
		}()
	}
	return openSQLSessionStore(ctx, db, schema, dialect)
}

// OpenSQLSessionStore opens the PostgreSQL store on the same session that owns
// the startup lock. This is the required path while the startup lock is held:
// it remains safe when the database pool permits only one open connection.
func (l *PostgresStartupLock) OpenSQLSessionStore(ctx context.Context) (store *SQLSessionStore, err error) {
	conn, err := l.connection()
	if err != nil {
		return nil, err
	}
	if err := acquirePostgresAdvisoryLock(ctx, conn, postgresSchemaMigrationLockKey); err != nil {
		discardErr := l.discardConnection()
		return nil, errors.Join(
			fmt.Errorf("acquire postgres schema migration lock: %w", err),
			wrapSQLConnDiscardError("postgres startup lock", discardErr),
		)
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if releaseErr := releasePostgresAdvisoryLock(releaseCtx, conn, postgresSchemaMigrationLockKey); releaseErr != nil {
			discardErr := l.discardConnection()
			store = nil
			err = errors.Join(
				err,
				fmt.Errorf("release postgres schema migration lock: %w", releaseErr),
				wrapSQLConnDiscardError("postgres startup lock", discardErr),
			)
		}
	}()
	return openSQLSessionStore(ctx, l.db, conn, SQLDialectPostgres)
}

func openSQLSessionStore(ctx context.Context, storeDB *sql.DB, db sqlSchemaExecutor, dialect SQLDialect) (*SQLSessionStore, error) {
	store := &SQLSessionStore{db: storeDB, dialect: dialect}
	metaExists, err := sqlTableExists(ctx, db, dialect, "store_meta")
	if err != nil {
		return nil, fmt.Errorf("inspect sql schema: %w", err)
	}
	if !metaExists {
		if err := initializeSQLSchema(ctx, db, dialect); err != nil {
			return nil, err
		}
		return store, nil
	}
	var raw string
	err = db.QueryRowContext(ctx, sqlSelectMetaRow.bind(dialect)).Scan(&raw)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if err := initializeSQLSchema(ctx, db, dialect); err != nil {
			return nil, err
		}
	case err != nil:
		return nil, fmt.Errorf("read sql schema version: %w", err)
	default:
		stored, convErr := strconv.Atoi(raw)
		if convErr != nil {
			return nil, fmt.Errorf("sql schema version %q is not a number", raw)
		}
		if stored > SQLSchemaVersion {
			return nil, fmt.Errorf(
				"sql session store schema version %s is not supported (this build writes version %d)",
				raw, SQLSchemaVersion,
			)
		}
		// v40 already includes every pre-v41 object. Enter the v41 transaction
		// directly so concurrent openers serialize at store_meta and graph table
		// locks before any non-transactional cumulative DDL can deadlock.
		if stored == 40 {
			if err := migrateGraphCheckpointHistoryV41(ctx, db, dialect); err != nil {
				return nil, fmt.Errorf("backfill graph checkpoint history: %w", err)
			}
			stored = 41
		}
		if _, err := db.ExecContext(ctx, sqlSchemaV15); err != nil {
			return nil, fmt.Errorf("ensure sql schema: %w", err)
		}
		if stored < SQLSchemaVersion {
			if err := migrateSQLSchema(ctx, db, dialect, stored); err != nil {
				return nil, err
			}
		}
		if _, err := db.ExecContext(ctx, sqlSchemaV25); err != nil {
			return nil, fmt.Errorf("ensure current sql schema: %w", err)
		}
		if err := migrateRunnerTaskTraceV25(ctx, db, dialect); err != nil {
			return nil, err
		}
		if _, err := db.ExecContext(ctx, sqlSchemaV26); err != nil {
			return nil, fmt.Errorf("ensure current sql schema: %w", err)
		}
		if stored < 27 {
			if err := migrateRagProjectionV27(ctx, db, dialect); err != nil {
				return nil, fmt.Errorf("rebuild rag inverted projection: %w", err)
			}
		}
		if _, err := db.ExecContext(ctx, sqlSchemaV27); err != nil {
			return nil, fmt.Errorf("ensure current sql schema: %w", err)
		}
		if _, err := db.ExecContext(ctx, sqlSchemaV28); err != nil {
			return nil, fmt.Errorf("ensure current sql schema: %w", err)
		}
		if _, err := db.ExecContext(ctx, sqlSchemaV30); err != nil {
			return nil, fmt.Errorf("ensure current sql schema: %w", err)
		}
		if _, err := db.ExecContext(ctx, sqlSchemaV31); err != nil {
			return nil, fmt.Errorf("ensure current sql schema: %w", err)
		}
		if err := migrateRunnerTaskRetryV30(ctx, db, dialect); err != nil {
			return nil, err
		}
		if _, err := db.ExecContext(ctx, sqlSchemaV32); err != nil {
			return nil, fmt.Errorf("ensure current sql schema: %w", err)
		}
		if _, err := db.ExecContext(ctx, sqlSchemaV33); err != nil {
			return nil, fmt.Errorf("ensure current sql schema: %w", err)
		}
		if _, err := db.ExecContext(ctx, sqlSchemaV34); err != nil {
			return nil, fmt.Errorf("ensure current sql schema: %w", err)
		}
		if _, err := db.ExecContext(ctx, sqlSchemaV35); err != nil {
			return nil, fmt.Errorf("ensure current sql schema: %w", err)
		}
		if _, err := db.ExecContext(ctx, sqlSchemaV36); err != nil {
			return nil, fmt.Errorf("ensure current sql schema: %w", err)
		}
		if _, err := db.ExecContext(ctx, sqlSchemaV37); err != nil {
			return nil, fmt.Errorf("ensure current sql schema: %w", err)
		}
		if _, err := db.ExecContext(ctx, sqlSchemaV38); err != nil {
			return nil, fmt.Errorf("ensure current sql schema: %w", err)
		}
		if _, err := db.ExecContext(ctx, sqlSchemaV40); err != nil {
			return nil, fmt.Errorf("ensure current sql schema: %w", err)
		}
		if stored < 21 {
			if err := backfillRunEvidence(ctx, db, dialect); err != nil {
				return nil, fmt.Errorf("backfill run evidence: %w", err)
			}
		}
		if stored < 41 {
			if err := migrateGraphCheckpointHistoryV41(ctx, db, dialect); err != nil {
				return nil, fmt.Errorf("backfill graph checkpoint history: %w", err)
			}
		} else if err := verifyGraphCheckpointHistoryV41(ctx, db, dialect); err != nil {
			return nil, err
		}
		if _, err := db.ExecContext(ctx, sqlSchemaV42AuthorizationEpoch); err != nil {
			return nil, fmt.Errorf("ensure authorization epoch: %w", err)
		}
		if _, err := db.ExecContext(ctx, sqlSchemaV43CompletedToolResultRecoverySidecars); err != nil {
			return nil, fmt.Errorf("ensure completed tool result recovery sidecars: %w", err)
		}
		if _, err := db.ExecContext(ctx, sqlSchemaV44NativeQueuedToolEffectWitnesses); err != nil {
			return nil, fmt.Errorf("ensure native queued tool effect witnesses: %w", err)
		}
		if _, err := db.ExecContext(ctx, sqlSchemaV45NativeQueuedModelInvocations); err != nil {
			return nil, fmt.Errorf("ensure native queued model invocations: %w", err)
		}
		if _, err := db.ExecContext(ctx, sqlSchemaV46NativeQueuedModelOutcomes); err != nil {
			return nil, fmt.Errorf("ensure native queued model outcomes: %w", err)
		}
		// v27 itself rebuilds the projection with this binary's tokenizer while
		// upgrading pre-v27 stores. Only stores that already had the v27
		// projection need the v47 semantic reindex.
		if stored >= 27 && stored < ragTokenizerSchemaVersionV47 {
			if err := migrateRagTokenizerV47(ctx, db, dialect); err != nil {
				return nil, fmt.Errorf("rebuild RAG tokenizer projection: %w", err)
			}
			stored = ragTokenizerSchemaVersionV47
		}
		if stored < obsHitSummarySchemaVersionV48 {
			if err := migrateObsHitSummariesV48(ctx, db); err != nil {
				return nil, fmt.Errorf("clear legacy observability hit snippets: %w", err)
			}
		}
		if stored < SQLSchemaVersion {
			if _, err := db.ExecContext(ctx, sqlUpdateMetaRow.bind(dialect), strconv.Itoa(SQLSchemaVersion)); err != nil {
				return nil, fmt.Errorf("upgrade sql schema version: %w", err)
			}
		}
	}
	return store, nil
}

// migrateObsHitSummariesV48 removes legacy watch-hit content. The prior
// format could contain tool arguments or message text, while v48 admits only
// canonical non-sensitive summaries for newly written hits.
func migrateObsHitSummariesV48(ctx context.Context, db sqlSchemaExecutor) error {
	_, err := db.ExecContext(ctx, "UPDATE obs_hits SET snippet = '' WHERE snippet <> ''")
	return err
}

// applySQLiteStartupPragma makes concurrently opening handles wait through a
// short schema writer window. In particular, journal_mode needs an exclusive
// lock even when both handles request the same WAL mode.
func applySQLiteStartupPragma(ctx context.Context, db *sql.DB, pragma string) error {
	const attempts = 8
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		_, err := db.ExecContext(ctx, pragma)
		if err == nil {
			return nil
		}
		lastErr = err
		if !isSQLiteMigrationBusy(err) || attempt == attempts-1 {
			return err
		}
		timer := time.NewTimer(time.Duration(attempt+1) * 10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return lastErr
}

func sqlTableExists(ctx context.Context, db graphCheckpointHistoryExecutor, dialect SQLDialect, table string) (bool, error) {
	if dialect == SQLDialectPostgres {
		var relation sql.NullString
		if err := db.QueryRowContext(ctx, "SELECT to_regclass($1)", table).Scan(&relation); err != nil {
			return false, err
		}
		return relation.Valid && relation.String != "", nil
	}
	var count int
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?", table,
	).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

func initializeSQLSchema(ctx context.Context, db sqlSchemaExecutor, dialect SQLDialect) error {
	if _, err := db.ExecContext(ctx, sqlSchemaV15); err != nil {
		return fmt.Errorf("ensure sql schema: %w", err)
	}
	if err := migrateSQLSchema(ctx, db, dialect, 0); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, sqlSchemaV25); err != nil {
		return fmt.Errorf("ensure current sql schema: %w", err)
	}
	if err := migrateRunnerTaskTraceV25(ctx, db, dialect); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, sqlSchemaV26); err != nil {
		return fmt.Errorf("ensure current sql schema: %w", err)
	}
	if err := migrateRagProjectionV27(ctx, db, dialect); err != nil {
		return fmt.Errorf("rebuild rag inverted projection: %w", err)
	}
	if _, err := db.ExecContext(ctx, sqlSchemaV27); err != nil {
		return fmt.Errorf("ensure current sql schema: %w", err)
	}
	if _, err := db.ExecContext(ctx, sqlSchemaV28); err != nil {
		return fmt.Errorf("ensure current sql schema: %w", err)
	}
	if _, err := db.ExecContext(ctx, sqlSchemaV30); err != nil {
		return fmt.Errorf("ensure current sql schema: %w", err)
	}
	if _, err := db.ExecContext(ctx, sqlSchemaV31); err != nil {
		return fmt.Errorf("ensure current sql schema: %w", err)
	}
	if err := migrateRunnerTaskRetryV30(ctx, db, dialect); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, sqlSchemaV32); err != nil {
		return fmt.Errorf("ensure current sql schema: %w", err)
	}
	if _, err := db.ExecContext(ctx, sqlSchemaV33); err != nil {
		return fmt.Errorf("ensure current sql schema: %w", err)
	}
	if _, err := db.ExecContext(ctx, sqlSchemaV34); err != nil {
		return fmt.Errorf("ensure current sql schema: %w", err)
	}
	if _, err := db.ExecContext(ctx, sqlSchemaV35); err != nil {
		return fmt.Errorf("ensure current sql schema: %w", err)
	}
	if _, err := db.ExecContext(ctx, sqlSchemaV36); err != nil {
		return fmt.Errorf("ensure current sql schema: %w", err)
	}
	if _, err := db.ExecContext(ctx, sqlSchemaV37); err != nil {
		return fmt.Errorf("ensure current sql schema: %w", err)
	}
	if _, err := db.ExecContext(ctx, sqlSchemaV38); err != nil {
		return fmt.Errorf("ensure current sql schema: %w", err)
	}
	if _, err := db.ExecContext(ctx, sqlSchemaV40); err != nil {
		return fmt.Errorf("ensure current sql schema: %w", err)
	}
	if err := migrateGraphCheckpointHistoryV41(ctx, db, dialect); err != nil {
		return fmt.Errorf("backfill graph checkpoint history: %w", err)
	}
	if _, err := db.ExecContext(ctx, sqlSchemaV42AuthorizationEpoch); err != nil {
		return fmt.Errorf("ensure authorization epoch: %w", err)
	}
	if _, err := db.ExecContext(ctx, sqlSchemaV43CompletedToolResultRecoverySidecars); err != nil {
		return fmt.Errorf("ensure completed tool result recovery sidecars: %w", err)
	}
	if _, err := db.ExecContext(ctx, sqlSchemaV44NativeQueuedToolEffectWitnesses); err != nil {
		return fmt.Errorf("ensure native queued tool effect witnesses: %w", err)
	}
	if _, err := db.ExecContext(ctx, sqlSchemaV45NativeQueuedModelInvocations); err != nil {
		return fmt.Errorf("ensure native queued model invocations: %w", err)
	}
	if _, err := db.ExecContext(ctx, sqlSchemaV46NativeQueuedModelOutcomes); err != nil {
		return fmt.Errorf("ensure native queued model outcomes: %w", err)
	}
	result, err := db.ExecContext(ctx, sqlUpdateMetaRow.bind(dialect), strconv.Itoa(SQLSchemaVersion))
	if err != nil {
		return fmt.Errorf("record sql schema version: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("record sql schema version: %w", err)
	}
	if affected == 0 {
		if _, err := db.ExecContext(ctx, sqlInsertMetaRow.bind(dialect), strconv.Itoa(SQLSchemaVersion)); err != nil {
			return fmt.Errorf("record sql schema version: %w", err)
		}
	}
	return nil
}

func readControlRevision(ctx context.Context, db *sql.DB, dialect SQLDialect) (int64, error) {
	var raw string
	if err := db.QueryRowContext(ctx, sqlSelectControlRevision.bind(dialect)).Scan(&raw); err != nil {
		return 0, err
	}
	revision, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || revision < 0 {
		return 0, fmt.Errorf("control revision %q is invalid", raw)
	}
	return revision, nil
}

func bumpControlRevision(ctx context.Context, tx *sql.Tx, dialect SQLDialect) error {
	result, err := tx.ExecContext(ctx, sqlBumpControlRevision.bind(dialect))
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return fmt.Errorf("control revision row is missing")
	}
	return nil
}

func migrateSQLSchema(ctx context.Context, db sqlSchemaExecutor, dialect SQLDialect, stored int) error {
	if stored < 10 {
		if err := addSQLColumn(ctx, db, dialect, "run_queue", "generation", "BIGINT NOT NULL DEFAULT 0"); err != nil {
			return fmt.Errorf("upgrade run queue generation: %w", err)
		}
	}
	if stored < 12 {
		if err := addSQLColumn(ctx, db, dialect, "run_queue", "trace_parent", "TEXT NOT NULL DEFAULT ''"); err != nil {
			return fmt.Errorf("upgrade run queue trace parent: %w", err)
		}
		if err := addSQLColumn(ctx, db, dialect, "run_queue", "trace_state", "TEXT NOT NULL DEFAULT ''"); err != nil {
			return fmt.Errorf("upgrade run queue trace state: %w", err)
		}
	}
	if stored < 14 {
		if err := addSQLColumn(ctx, db, dialect, "profile_releases", "layer_json", "TEXT NOT NULL DEFAULT '{}'"); err != nil {
			return fmt.Errorf("upgrade release layer artifact: %w", err)
		}
		if err := addSQLColumn(ctx, db, dialect, "profile_releases", "revision", "TEXT NOT NULL DEFAULT ''"); err != nil {
			return fmt.Errorf("upgrade release revision: %w", err)
		}
	}
	if stored < 16 {
		if err := addSQLColumn(ctx, db, dialect, "profile_releases", "operation_id", "TEXT NOT NULL DEFAULT ''"); err != nil {
			return fmt.Errorf("upgrade release operation id: %w", err)
		}
		if err := addSQLColumn(ctx, db, dialect, "profile_canaries", "release_version", "INTEGER NOT NULL DEFAULT 0"); err != nil {
			return fmt.Errorf("upgrade canary release version: %w", err)
		}
	}
	if stored < 17 {
		if err := addSQLColumn(ctx, db, dialect, "profile_canaries", "base_release_revision", "TEXT NOT NULL DEFAULT ''"); err != nil {
			return fmt.Errorf("upgrade canary base release revision: %w", err)
		}
	}
	if stored < 19 {
		if err := addSQLColumn(ctx, db, dialect, "evaluation_runs", "composition_metadata_json", "TEXT NOT NULL DEFAULT '{}' "); err != nil {
			return fmt.Errorf("upgrade evaluation composition metadata: %w", err)
		}
	}
	if stored < 20 {
		if err := addSQLColumn(ctx, db, dialect, "evaluation_runs", "assignment_revision", "TEXT NOT NULL DEFAULT ''"); err != nil {
			return fmt.Errorf("upgrade evaluation assignment revision: %w", err)
		}
		if err := addSQLColumn(ctx, db, dialect, "evaluation_case_results", "composition_revision", "TEXT NOT NULL DEFAULT ''"); err != nil {
			return fmt.Errorf("upgrade evaluation case composition revision: %w", err)
		}
		if err := addSQLColumn(ctx, db, dialect, "evaluation_case_results", "assignment_revision", "TEXT NOT NULL DEFAULT ''"); err != nil {
			return fmt.Errorf("upgrade evaluation case assignment revision: %w", err)
		}
		if err := backfillEvaluationRevisions(ctx, db, dialect); err != nil {
			return fmt.Errorf("backfill evaluation artifact revisions: %w", err)
		}
	}
	if stored < 22 {
		exists, err := sqlTableExists(ctx, db, dialect, "run_evidence")
		if err != nil {
			return fmt.Errorf("inspect run evidence status migration: %w", err)
		}
		if exists {
			if err := addSQLColumn(ctx, db, dialect, "run_evidence", "status", "TEXT NOT NULL DEFAULT ''"); err != nil {
				return fmt.Errorf("upgrade run evidence status: %w", err)
			}
			if err := backfillRunEvidence(ctx, db, dialect); err != nil {
				return fmt.Errorf("backfill run evidence status: %w", err)
			}
		}
	}
	if stored < 23 {
		exists, err := sqlTableExists(ctx, db, dialect, "run_evidence")
		if err != nil {
			return fmt.Errorf("inspect run evidence variant migration: %w", err)
		}
		if exists {
			if err := addSQLColumn(ctx, db, dialect, "run_evidence", "assignment_variant", "TEXT NOT NULL DEFAULT 'unassigned'"); err != nil {
				return fmt.Errorf("upgrade run evidence assignment variant: %w", err)
			}
			if err := backfillRunEvidence(ctx, db, dialect); err != nil {
				return fmt.Errorf("backfill run evidence assignment variant: %w", err)
			}
		}
	}
	if stored < 25 {
		if err := migrateRunnerTaskTraceV25(ctx, db, dialect); err != nil {
			return err
		}
	}
	if stored < 26 {
		if err := migrateMemoryRagV26(ctx, db, dialect); err != nil {
			return fmt.Errorf("upgrade memory/rag tags: %w", err)
		}
	}
	if stored < 28 {
		if err := migrateMemorySearchProjectionV28(ctx, db, dialect); err != nil {
			return fmt.Errorf("rebuild memory search projection: %w", err)
		}
	}
	if stored < 30 {
		if err := migrateRunnerTaskRetryV30(ctx, db, dialect); err != nil {
			return err
		}
	}
	if stored < 31 {
		if _, err := db.ExecContext(ctx, sqlDelegationLinksV31); err != nil {
			return fmt.Errorf("upgrade delegation links: %w", err)
		}
	}
	if stored < 32 {
		if err := migrateAccountsV32(ctx, db, dialect); err != nil {
			return fmt.Errorf("upgrade account identity: %w", err)
		}
	}
	return nil
}

// migrateGraphCheckpointHistoryV41 preserves exactly the one checkpoint fact
// v40 retained for each run. The history schema, floors, write fence, and
// schema marker are one critical section: exposing only a subset would let a
// legacy head writer silently create a head/history split.
//
// SQLite uses a dedicated BEGIN IMMEDIATE connection because database/sql's
// default deferred transactions do not reserve the write lock before reading
// graph_checkpoints. PostgreSQL holds its locks in the same transaction as the
// DDL and backfill. Both paths re-read the marker after acquiring their lock,
// so concurrent Open calls converge on the one successful migration.
func migrateGraphCheckpointHistoryV41(ctx context.Context, db sqlSchemaExecutor, dialect SQLDialect) error {
	switch dialect {
	case SQLDialectSQLite:
		sqlDB, ok := db.(*sql.DB)
		if !ok {
			return fmt.Errorf("SQLite graph checkpoint history migration requires a database handle")
		}
		return migrateGraphCheckpointHistoryV41SQLite(ctx, sqlDB, dialect)
	case SQLDialectPostgres:
		return migrateGraphCheckpointHistoryV41Postgres(ctx, db, dialect)
	default:
		return fmt.Errorf("unsupported SQL dialect %q", dialect)
	}
}

type graphCheckpointHistoryExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type sqlSchemaExecutor interface {
	graphCheckpointHistoryExecutor
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
}

type graphCheckpointHistoryFloor struct {
	key        graph.CheckpointKey
	revision   uint64
	hash       string
	checkpoint string
}

func migrateGraphCheckpointHistoryV41SQLite(ctx context.Context, db *sql.DB, dialect SQLDialect) error {
	const attempts = 5
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		conn, err := db.Conn(ctx)
		if err != nil {
			return err
		}
		_, err = conn.ExecContext(ctx, "BEGIN IMMEDIATE")
		if err == nil {
			err = applyGraphCheckpointHistoryV41(ctx, conn, dialect)
			if err == nil {
				_, err = conn.ExecContext(ctx, "COMMIT")
			}
			if err != nil {
				_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
			}
		}
		closeErr := conn.Close()
		if err == nil && closeErr != nil {
			err = closeErr
		}
		if err == nil {
			return verifyGraphCheckpointHistoryV41(ctx, db, dialect)
		}
		lastErr = err
		if !isSQLiteMigrationBusy(err) || attempt == attempts-1 {
			return err
		}
		timer := time.NewTimer(time.Duration(attempt+1) * 10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return lastErr
}

func migrateGraphCheckpointHistoryV41Postgres(ctx context.Context, db sqlSchemaExecutor, dialect SQLDialect) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// store_meta serializes concurrent Open calls. The two graph tables block a
	// v40 process which already has a write transaction in progress, then keep
	// it blocked until the v41 fence and marker become visible together.
	for _, statement := range []string{
		"LOCK TABLE store_meta IN ACCESS EXCLUSIVE MODE",
		"LOCK TABLE graph_checkpoints IN ACCESS EXCLUSIVE MODE",
		"LOCK TABLE graph_transitions IN ACCESS EXCLUSIVE MODE",
	} {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	if err := applyGraphCheckpointHistoryV41(ctx, tx, dialect); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return verifyGraphCheckpointHistoryV41(ctx, db, dialect)
}

func applyGraphCheckpointHistoryV41(ctx context.Context, exec graphCheckpointHistoryExecutor, dialect SQLDialect) error {
	stored, found, err := graphCheckpointHistorySchemaVersion(ctx, exec, dialect)
	if err != nil {
		return err
	}
	if found && stored > SQLSchemaVersion {
		return fmt.Errorf("sql session store schema version %d is not supported (this build writes version %d)", stored, SQLSchemaVersion)
	}
	if found && stored >= graphCheckpointHistorySchemaVersionV41 {
		return nil
	}
	if _, err := exec.ExecContext(ctx, sqlSchemaV41CheckpointHistory); err != nil {
		return err
	}
	if err := installGraphCheckpointHistoryWriteFence(ctx, exec, dialect); err != nil {
		return err
	}
	floors, err := readGraphCheckpointHistoryFloors(ctx, exec)
	if err != nil {
		return err
	}
	for _, floor := range floors {
		if _, err := exec.ExecContext(ctx, sqlQuery{`INSERT INTO graph_checkpoint_versions
			(tenant_id, session_id, run_id, revision, version_id, parent_version_id, origin, checkpoint_hash, checkpoint_json, created_at)
			VALUES (?, ?, ?, ?, ?, '', ?, ?, ?, ?)
			ON CONFLICT (tenant_id, session_id, run_id, revision) DO NOTHING`}.bind(dialect),
			floor.key.TenantID, floor.key.SessionID, floor.key.RunID, floor.revision, graph.CheckpointVersionID(floor.key, floor.revision), graph.CheckpointVersionOriginMigrationFloor, floor.hash, floor.checkpoint, time.Now().UnixNano()); err != nil {
			return err
		}
	}
	if err := verifyGraphCheckpointHistoryFloors(ctx, exec, dialect, floors); err != nil {
		return err
	}
	result, err := exec.ExecContext(ctx, sqlUpdateMetaRow.bind(dialect), strconv.Itoa(graphCheckpointHistorySchemaVersionV41))
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		if _, err := exec.ExecContext(ctx, sqlInsertMetaRow.bind(dialect), strconv.Itoa(graphCheckpointHistorySchemaVersionV41)); err != nil {
			return err
		}
	}
	return nil
}

func graphCheckpointHistorySchemaVersion(ctx context.Context, exec graphCheckpointHistoryExecutor, dialect SQLDialect) (int, bool, error) {
	var raw string
	err := exec.QueryRowContext(ctx, sqlSelectMetaRow.bind(dialect)).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	stored, err := strconv.Atoi(raw)
	if err != nil {
		return 0, false, fmt.Errorf("sql schema version %q is not a number", raw)
	}
	return stored, true, nil
}

func readGraphCheckpointHistoryFloors(ctx context.Context, exec graphCheckpointHistoryExecutor) ([]graphCheckpointHistoryFloor, error) {
	rows, err := exec.QueryContext(ctx, `SELECT tenant_id, session_id, run_id, revision, checkpoint_json
		FROM graph_checkpoints ORDER BY tenant_id ASC, session_id ASC, run_id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	floors := make([]graphCheckpointHistoryFloor, 0)
	for rows.Next() {
		var key graph.CheckpointKey
		var revision uint64
		var raw string
		if err := rows.Scan(&key.TenantID, &key.SessionID, &key.RunID, &revision, &raw); err != nil {
			return nil, err
		}
		checkpoint, err := decodeGraphCheckpointDocument([]byte(raw))
		if err != nil || checkpoint.Key != key || checkpoint.Revision != revision {
			return nil, fmt.Errorf("%w: corrupt graph checkpoint document", graph.ErrInvalidCheckpoint)
		}
		canonical, err := json.Marshal(checkpoint)
		if err != nil || len(canonical) == 0 || len(canonical) > 640<<10 {
			return nil, fmt.Errorf("%w: canonical graph checkpoint document", graph.ErrInvalidCheckpoint)
		}
		hash, err := graph.CheckpointDigest(checkpoint)
		if err != nil {
			return nil, err
		}
		floors = append(floors, graphCheckpointHistoryFloor{key: key, revision: revision, hash: hash, checkpoint: string(canonical)})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return floors, nil
}

func verifyGraphCheckpointHistoryFloors(ctx context.Context, exec graphCheckpointHistoryExecutor, dialect SQLDialect, floors []graphCheckpointHistoryFloor) error {
	var count int
	if err := exec.QueryRowContext(ctx, `SELECT COUNT(*) FROM graph_checkpoint_versions`).Scan(&count); err != nil {
		return err
	}
	if count != len(floors) {
		return fmt.Errorf("%w: graph checkpoint history is not an exact v40 floor set", graph.ErrInvalidCheckpoint)
	}
	for _, floor := range floors {
		var id, parentID, origin, hash, raw string
		if err := exec.QueryRowContext(ctx, sqlQuery{`SELECT version_id, parent_version_id, origin, checkpoint_hash, checkpoint_json
			FROM graph_checkpoint_versions WHERE tenant_id = ? AND session_id = ? AND run_id = ? AND revision = ?`}.bind(dialect),
			floor.key.TenantID, floor.key.SessionID, floor.key.RunID, floor.revision).Scan(&id, &parentID, &origin, &hash, &raw); err != nil {
			return err
		}
		if id != graph.CheckpointVersionID(floor.key, floor.revision) || parentID != "" ||
			origin != string(graph.CheckpointVersionOriginMigrationFloor) || hash != floor.hash || raw != floor.checkpoint {
			return fmt.Errorf("%w: graph checkpoint history floor does not match head", graph.ErrInvalidCheckpoint)
		}
	}
	return nil
}

const sqlGraphCheckpointHistorySQLiteHeadInsertTrigger = `CREATE TRIGGER IF NOT EXISTS graph_checkpoint_history_head_insert
	BEFORE INSERT ON graph_checkpoints FOR EACH ROW
	WHEN NOT EXISTS (SELECT 1 FROM graph_checkpoint_versions AS v
		JOIN graph_transitions AS t ON t.tenant_id = v.tenant_id AND t.session_id = v.session_id AND t.run_id = v.run_id AND t.revision = v.revision
		WHERE v.tenant_id = NEW.tenant_id AND v.session_id = NEW.session_id AND v.run_id = NEW.run_id AND v.revision = NEW.revision
			AND v.origin = 'commit' AND v.checkpoint_json = NEW.checkpoint_json)
	BEGIN SELECT RAISE(ABORT, 'graph checkpoint history v41 requires version and transition before head'); END`

const sqlGraphCheckpointHistorySQLiteHeadUpdateTrigger = `CREATE TRIGGER IF NOT EXISTS graph_checkpoint_history_head_update
	BEFORE UPDATE OF revision, checkpoint_json ON graph_checkpoints FOR EACH ROW
	WHEN NOT EXISTS (SELECT 1 FROM graph_checkpoint_versions AS v
		JOIN graph_transitions AS t ON t.tenant_id = v.tenant_id AND t.session_id = v.session_id AND t.run_id = v.run_id AND t.revision = v.revision
		WHERE v.tenant_id = NEW.tenant_id AND v.session_id = NEW.session_id AND v.run_id = NEW.run_id AND v.revision = NEW.revision
			AND v.origin = 'commit' AND v.checkpoint_json = NEW.checkpoint_json)
	BEGIN SELECT RAISE(ABORT, 'graph checkpoint history v41 requires version and transition before head'); END`

const sqlGraphCheckpointHistoryPostgresHeadFenceFunctionBody = `
BEGIN
	IF NOT EXISTS (SELECT 1 FROM graph_checkpoint_versions AS v
		JOIN graph_transitions AS t ON t.tenant_id = v.tenant_id AND t.session_id = v.session_id AND t.run_id = v.run_id AND t.revision = v.revision
		WHERE v.tenant_id = NEW.tenant_id AND v.session_id = NEW.session_id AND v.run_id = NEW.run_id AND v.revision = NEW.revision
			AND v.origin = 'commit' AND v.checkpoint_json = NEW.checkpoint_json) THEN
		RAISE EXCEPTION 'graph checkpoint history v41 requires version and transition before head' USING ERRCODE = '55000';
	END IF;
	RETURN NEW;
END;
`

const sqlGraphCheckpointHistoryPostgresHeadFenceFunction = `CREATE OR REPLACE FUNCTION graph_checkpoint_history_head_fence() RETURNS trigger LANGUAGE plpgsql AS $$` + sqlGraphCheckpointHistoryPostgresHeadFenceFunctionBody + `$$`

const sqlGraphCheckpointHistoryPostgresHeadFenceTrigger = `CREATE TRIGGER graph_checkpoint_history_head_fence BEFORE INSERT OR UPDATE ON graph_checkpoints
	FOR EACH ROW EXECUTE FUNCTION graph_checkpoint_history_head_fence()`

func installGraphCheckpointHistoryWriteFence(ctx context.Context, exec graphCheckpointHistoryExecutor, dialect SQLDialect) error {
	if dialect == SQLDialectSQLite {
		for _, statement := range []string{
			sqlGraphCheckpointHistorySQLiteHeadInsertTrigger,
			sqlGraphCheckpointHistorySQLiteHeadUpdateTrigger,
		} {
			if _, err := exec.ExecContext(ctx, statement); err != nil {
				return err
			}
		}
		return nil
	}
	for _, statement := range []string{
		sqlGraphCheckpointHistoryPostgresHeadFenceFunction,
		`DROP TRIGGER IF EXISTS graph_checkpoint_history_head_fence ON graph_checkpoints`,
		sqlGraphCheckpointHistoryPostgresHeadFenceTrigger,
	} {
		if _, err := exec.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func verifyGraphCheckpointHistoryV41(ctx context.Context, db graphCheckpointHistoryExecutor, dialect SQLDialect) error {
	if dialect == SQLDialectSQLite {
		sqlDB, ok := db.(*sql.DB)
		if !ok {
			return fmt.Errorf("SQLite graph checkpoint history verification requires a database handle")
		}
		conn, err := sqlDB.Conn(ctx)
		if err != nil {
			return fmt.Errorf("reserve SQLite graph checkpoint history verifier connection: %w", err)
		}
		defer conn.Close()
		return verifySQLiteGraphCheckpointHistoryV41(ctx, conn)
	}
	exists, err := sqlTableExists(ctx, db, dialect, "graph_checkpoint_versions")
	if err != nil {
		return fmt.Errorf("inspect graph checkpoint history table: %w", err)
	}
	if !exists {
		return fmt.Errorf("graph checkpoint history v41 table is missing")
	}
	for _, column := range []string{"tenant_id", "session_id", "run_id", "revision", "version_id", "parent_version_id", "origin", "checkpoint_hash", "checkpoint_json", "created_at"} {
		exists, err := sqlColumnExists(ctx, db, dialect, "graph_checkpoint_versions", column)
		if err != nil {
			return fmt.Errorf("inspect graph checkpoint history column %s: %w", column, err)
		}
		if !exists {
			return fmt.Errorf("graph checkpoint history v41 column %s is missing", column)
		}
	}
	if err := verifyPostgresGraphCheckpointHistoryConstraints(ctx, db); err != nil {
		return err
	}
	return verifyPostgresGraphCheckpointHistoryWriteFence(ctx, db)
}

func verifySQLiteGraphCheckpointHistoryV41(ctx context.Context, conn *sql.Conn) error {
	var count int
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master
		WHERE type = 'table' AND name = 'graph_checkpoint_versions'`).Scan(&count); err != nil {
		return fmt.Errorf("inspect graph checkpoint history SQLite table: %w", err)
	}
	if count != 1 {
		return fmt.Errorf("graph checkpoint history v41 table is missing")
	}
	columns, err := sqliteTableColumns(ctx, conn, "graph_checkpoint_versions")
	if err != nil {
		return err
	}
	for _, column := range []string{"tenant_id", "session_id", "run_id", "revision", "version_id", "parent_version_id", "origin", "checkpoint_hash", "checkpoint_json", "created_at"} {
		if !columns[column] {
			return fmt.Errorf("graph checkpoint history v41 column %s is missing", column)
		}
	}
	if err := verifySQLiteGraphCheckpointHistoryIndexes(ctx, conn); err != nil {
		return err
	}
	return verifySQLiteGraphCheckpointHistoryWriteFence(ctx, conn)
}

func sqliteTableColumns(ctx context.Context, conn *sql.Conn, table string) (map[string]bool, error) {
	rows, err := conn.QueryContext(ctx, "PRAGMA table_info('"+strings.ReplaceAll(table, "'", "''")+"')")
	if err != nil {
		return nil, fmt.Errorf("inspect graph checkpoint history SQLite table %q: %w", table, err)
	}
	columns := make(map[string]bool)
	for rows.Next() {
		var columnID, notNull, primaryKey int
		var name, columnType string
		var defaultValue any
		if err := rows.Scan(&columnID, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan graph checkpoint history SQLite table %q: %w", table, err)
		}
		columns[name] = true
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("iterate graph checkpoint history SQLite table %q: %w", table, err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close graph checkpoint history SQLite table %q inspection: %w", table, err)
	}
	return columns, nil
}

func verifySQLiteGraphCheckpointHistoryIndexes(ctx context.Context, conn *sql.Conn) error {
	rows, err := conn.QueryContext(ctx, `PRAGMA index_list('graph_checkpoint_versions')`)
	if err != nil {
		return fmt.Errorf("inspect graph checkpoint history SQLite indexes: %w", err)
	}
	type sqliteIndex struct {
		name    string
		unique  int
		origin  string
		partial int
	}
	indexes := make([]sqliteIndex, 0)
	for rows.Next() {
		var sequence int
		var index sqliteIndex
		if err := rows.Scan(&sequence, &index.name, &index.unique, &index.origin, &index.partial); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan graph checkpoint history SQLite index: %w", err)
		}
		indexes = append(indexes, index)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("iterate graph checkpoint history SQLite indexes: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close graph checkpoint history SQLite index inspection: %w", err)
	}
	primaryKeyColumns := []string{"tenant_id", "session_id", "run_id", "revision"}
	hasPrimaryKey := false
	hasVersionIDUnique := false
	for _, index := range indexes {
		if index.unique != 1 || index.partial != 0 {
			continue
		}
		columns, err := sqliteIndexColumns(ctx, conn, index.name)
		if err != nil {
			return err
		}
		if index.origin == "pk" && sameSQLColumns(columns, primaryKeyColumns) {
			hasPrimaryKey = true
		}
		if sameSQLColumns(columns, []string{"version_id"}) {
			hasVersionIDUnique = true
		}
	}
	if !hasPrimaryKey {
		return fmt.Errorf("graph checkpoint history v41 SQLite composite primary key is missing")
	}
	if !hasVersionIDUnique {
		return fmt.Errorf("graph checkpoint history v41 SQLite version_id unique constraint is missing")
	}
	return nil
}

func sqliteIndexColumns(ctx context.Context, conn *sql.Conn, index string) ([]string, error) {
	// index names originate in SQLite's catalog, but still quote the literal so
	// a malformed catalog entry cannot alter the inspection statement.
	quoted := strings.ReplaceAll(index, "'", "''")
	rows, err := conn.QueryContext(ctx, "PRAGMA index_info('"+quoted+"')")
	if err != nil {
		return nil, fmt.Errorf("inspect graph checkpoint history SQLite index %q: %w", index, err)
	}
	defer rows.Close()
	columns := make([]string, 0)
	for rows.Next() {
		var sequence, columnID int
		var column string
		if err := rows.Scan(&sequence, &columnID, &column); err != nil {
			return nil, fmt.Errorf("scan graph checkpoint history SQLite index %q: %w", index, err)
		}
		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate graph checkpoint history SQLite index %q: %w", index, err)
	}
	return columns, nil
}

func verifyPostgresGraphCheckpointHistoryConstraints(ctx context.Context, db graphCheckpointHistoryExecutor) error {
	rows, err := db.QueryContext(ctx, `SELECT con.contype, string_agg(att.attname, ',' ORDER BY key.ordinality)
		FROM pg_catalog.pg_constraint AS con
		JOIN pg_catalog.pg_class AS rel ON rel.oid = con.conrelid
		JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = rel.relnamespace
		JOIN unnest(con.conkey) WITH ORDINALITY AS key(attnum, ordinality) ON true
		JOIN pg_catalog.pg_attribute AS att ON att.attrelid = rel.oid AND att.attnum = key.attnum
		WHERE namespace.nspname = current_schema() AND rel.relname = 'graph_checkpoint_versions'
			AND con.contype IN ('p', 'u')
		GROUP BY con.oid, con.contype`)
	if err != nil {
		return fmt.Errorf("inspect graph checkpoint history PostgreSQL constraints: %w", err)
	}
	defer rows.Close()
	primaryKeyColumns := []string{"tenant_id", "session_id", "run_id", "revision"}
	hasPrimaryKey := false
	hasVersionIDUnique := false
	for rows.Next() {
		var kind, listed string
		if err := rows.Scan(&kind, &listed); err != nil {
			return fmt.Errorf("scan graph checkpoint history PostgreSQL constraint: %w", err)
		}
		columns := strings.Split(listed, ",")
		if kind == "p" && sameSQLColumns(columns, primaryKeyColumns) {
			hasPrimaryKey = true
		}
		if kind == "u" && sameSQLColumns(columns, []string{"version_id"}) {
			hasVersionIDUnique = true
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate graph checkpoint history PostgreSQL constraints: %w", err)
	}
	if !hasPrimaryKey {
		return fmt.Errorf("graph checkpoint history v41 PostgreSQL composite primary key is missing")
	}
	if !hasVersionIDUnique {
		return fmt.Errorf("graph checkpoint history v41 PostgreSQL version_id unique constraint is missing")
	}
	return nil
}

func verifySQLiteGraphCheckpointHistoryWriteFence(ctx context.Context, conn *sql.Conn) error {
	triggers := map[string]string{
		"graph_checkpoint_history_head_insert": sqlGraphCheckpointHistorySQLiteHeadInsertTrigger,
		"graph_checkpoint_history_head_update": sqlGraphCheckpointHistorySQLiteHeadUpdateTrigger,
	}
	for name, expected := range triggers {
		var definition sql.NullString
		err := conn.QueryRowContext(ctx, `SELECT sql FROM sqlite_master
			WHERE type = 'trigger' AND name = ? AND tbl_name = 'graph_checkpoints'`, name).Scan(&definition)
		if errors.Is(err, sql.ErrNoRows) || !definition.Valid || definition.String == "" {
			return fmt.Errorf("graph checkpoint history v41 SQLite write fence %s is missing", name)
		}
		if err != nil {
			return fmt.Errorf("inspect graph checkpoint history SQLite write fence %s: %w", name, err)
		}
		actualNormalized := normalizeSQLiteTriggerDefinition(definition.String)
		expectedNormalized := normalizeSQLiteTriggerDefinition(expected)
		if actualNormalized != expectedNormalized {
			return fmt.Errorf("graph checkpoint history v41 SQLite write fence %s has an unexpected definition", name)
		}
	}
	return nil
}

func verifyPostgresGraphCheckpointHistoryWriteFence(ctx context.Context, db graphCheckpointHistoryExecutor) error {
	var triggerType int
	var functionName, definition string
	err := db.QueryRowContext(ctx, `SELECT trigger.tgtype::integer, procedure.proname, procedure.prosrc
		FROM pg_catalog.pg_trigger AS trigger
		JOIN pg_catalog.pg_class AS relation ON relation.oid = trigger.tgrelid
		JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
		JOIN pg_catalog.pg_proc AS procedure ON procedure.oid = trigger.tgfoid
		WHERE namespace.nspname = current_schema() AND relation.relname = 'graph_checkpoints'
			AND trigger.tgname = 'graph_checkpoint_history_head_fence' AND NOT trigger.tgisinternal`).Scan(&triggerType, &functionName, &definition)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("graph checkpoint history v41 PostgreSQL write fence is missing")
	}
	if err != nil {
		return fmt.Errorf("inspect graph checkpoint history PostgreSQL write fence: %w", err)
	}
	// PostgreSQL trigger bits: ROW=1, BEFORE=2, INSERT=4, UPDATE=16. Exact
	// equality also rejects a weakened INSERT-only trigger or extra events.
	if triggerType != 23 || functionName != "graph_checkpoint_history_head_fence" {
		return fmt.Errorf("graph checkpoint history v41 PostgreSQL write fence has an unexpected binding")
	}
	if normalizeSQLDefinition(definition) != normalizeSQLDefinition(sqlGraphCheckpointHistoryPostgresHeadFenceFunctionBody) {
		return fmt.Errorf("graph checkpoint history v41 PostgreSQL write fence has an unexpected definition")
	}
	return nil
}

func normalizeSQLiteTriggerDefinition(definition string) string {
	normalized := normalizeSQLDefinition(definition)
	return strings.Replace(normalized, "create trigger if not exists ", "create trigger ", 1)
}

func normalizeSQLDefinition(definition string) string {
	return strings.Join(strings.Fields(strings.ToLower(definition)), " ")
}

func sameSQLColumns(actual, expected []string) bool {
	if len(actual) != len(expected) {
		return false
	}
	for index := range expected {
		if actual[index] != expected[index] {
			return false
		}
	}
	return true
}

func isSQLiteMigrationBusy(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "database is locked") || strings.Contains(message, "database table is locked") || strings.Contains(message, "sqlite_busy")
}

func decodeGraphCheckpointDocument(raw []byte) (graph.Checkpoint, error) {
	if len(raw) == 0 || len(raw) > 640<<10 {
		return graph.Checkpoint{}, graph.ErrInvalidCheckpoint
	}
	var checkpoint graph.Checkpoint
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&checkpoint); err != nil {
		return graph.Checkpoint{}, fmt.Errorf("%w: %v", graph.ErrInvalidCheckpoint, err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			err = errors.New("multiple JSON values")
		}
		return graph.Checkpoint{}, fmt.Errorf("%w: %v", graph.ErrInvalidCheckpoint, err)
	}
	return graph.ValidateCheckpoint(checkpoint)
}

func migrateAccountsV32(ctx context.Context, db sqlSchemaExecutor, dialect SQLDialect) error {
	accountIDExists, err := sqlColumnExists(ctx, db, dialect, "accounts", "account_id")
	if err != nil {
		return fmt.Errorf("inspect accounts: %w", err)
	}
	if accountIDExists {
		if err := addSQLColumn(ctx, db, dialect, "accounts", "email", "TEXT"); err != nil {
			return fmt.Errorf("ensure account email: %w", err)
		}
		if err := addSQLColumn(ctx, db, dialect, "accounts", "must_change_password", "BIGINT NOT NULL DEFAULT 0"); err != nil {
			return fmt.Errorf("ensure account activation flag: %w", err)
		}
	}
	tokenAccountIDExists, err := sqlColumnExists(ctx, db, dialect, "auth_tokens", "account_id")
	if err != nil {
		return fmt.Errorf("inspect auth tokens: %w", err)
	}
	if accountIDExists && tokenAccountIDExists {
		return nil
	}
	tokenSource := "account_id"
	if !tokenAccountIDExists {
		tokenEmailExists, err := sqlColumnExists(ctx, db, dialect, "auth_tokens", "email")
		if err != nil {
			return fmt.Errorf("inspect legacy auth tokens: %w", err)
		}
		if !tokenEmailExists {
			return fmt.Errorf("auth_tokens has neither account_id nor legacy email identity")
		}
		tokenSource = "email"
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	statements := []string{}
	if !accountIDExists {
		statements = append(statements,
			"DROP TABLE IF EXISTS accounts_v32",
			`CREATE TABLE accounts_v32 (
			account_id TEXT PRIMARY KEY,
			email TEXT,
			password_hash TEXT NOT NULL,
			role TEXT NOT NULL,
			tenant_id TEXT NOT NULL,
			status TEXT NOT NULL DEFAULT 'active',
			must_change_password BIGINT NOT NULL DEFAULT 0,
			created_at BIGINT NOT NULL
		)`,
			`INSERT INTO accounts_v32
			(account_id, email, password_hash, role, tenant_id, status, must_change_password, created_at)
			SELECT email, email, password_hash, role, tenant_id, status, 0, created_at FROM accounts`)
	}
	statements = append(statements,
		"DROP TABLE IF EXISTS auth_tokens_v32",
		`CREATE TABLE auth_tokens_v32 (
			token_hash TEXT PRIMARY KEY,
			account_id TEXT NOT NULL,
			expires_at BIGINT NOT NULL
		)`,
		`INSERT INTO auth_tokens_v32 (token_hash, account_id, expires_at)
			SELECT token_hash, `+tokenSource+`, expires_at FROM auth_tokens`,
		"DROP TABLE auth_tokens")
	if !accountIDExists {
		statements = append(statements, "DROP TABLE accounts", "ALTER TABLE accounts_v32 RENAME TO accounts")
	}
	statements = append(statements,
		"ALTER TABLE auth_tokens_v32 RENAME TO auth_tokens",
		"CREATE INDEX auth_tokens_account_id ON auth_tokens (account_id)")
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// AccountsTableExists inspects the pre-migration database state used by the
// one-time initial administrator bootstrap decision.
func AccountsTableExists(ctx context.Context, db *sql.DB, dialect SQLDialect) (bool, error) {
	if db == nil {
		return false, fmt.Errorf("inspect accounts table requires a database handle")
	}
	if err := validateSQLDialect(dialect); err != nil {
		return false, err
	}
	return sqlTableExists(ctx, db, dialect, "accounts")
}

func migrateRunnerTaskTraceV25(ctx context.Context, db sqlSchemaExecutor, dialect SQLDialect) error {
	exists, err := sqlTableExists(ctx, db, dialect, "runner_tasks")
	if err != nil {
		return fmt.Errorf("inspect runner task trace migration: %w", err)
	}
	if !exists {
		return nil
	}
	if err := addSQLColumn(ctx, db, dialect, "runner_tasks", "trace_parent", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return fmt.Errorf("upgrade runner task trace parent: %w", err)
	}
	if err := addSQLColumn(ctx, db, dialect, "runner_tasks", "trace_state", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return fmt.Errorf("upgrade runner task trace state: %w", err)
	}
	return nil
}

func migrateRunnerTaskRetryV30(ctx context.Context, db sqlSchemaExecutor, dialect SQLDialect) error {
	exists, err := sqlTableExists(ctx, db, dialect, "runner_tasks")
	if err != nil || !exists {
		return err
	}
	if err := addSQLColumn(ctx, db, dialect, "runner_tasks", "retried_from_id", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return fmt.Errorf("upgrade runner retry parent: %w", err)
	}
	_, err = db.ExecContext(ctx, `CREATE UNIQUE INDEX IF NOT EXISTS runner_tasks_retried_from ON runner_tasks (retried_from_id) WHERE retried_from_id <> ''`)
	return err
}

func addSQLColumn(ctx context.Context, db graphCheckpointHistoryExecutor, dialect SQLDialect, table, column, definition string) error {
	exists, err := sqlColumnExists(ctx, db, dialect, table, column)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	query := "ALTER TABLE " + table + " ADD COLUMN " + column + " " + definition
	if _, err := db.ExecContext(ctx, query); err != nil {
		message := strings.ToLower(err.Error())
		if strings.Contains(message, "duplicate column") || strings.Contains(message, "already exists") {
			return nil
		}
		return err
	}
	return nil
}

func sqlColumnExists(ctx context.Context, db graphCheckpointHistoryExecutor, dialect SQLDialect, table, column string) (bool, error) {
	if dialect == SQLDialectPostgres {
		var count int
		err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.columns
			WHERE table_schema = current_schema() AND table_name = $1 AND column_name = $2`, table, column).Scan(&count)
		return count > 0, err
	}
	rows, err := db.QueryContext(ctx, "PRAGMA table_info("+table+")")
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, columnType string
		var notNull, primaryKey int
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			return false, err
		}
		if name == column {
			return true, nil
		}
	}
	return false, rows.Err()
}
