package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/extensions/memory"
	"github.com/whhhh1500/auto-agent/pkg/extensions/rag"
	"sort"
	"strings"
)

type memoryRagTagsRow struct {
	Scope string
	ID    string
	Tags  string
}

// migrateMemoryRagV26 adds tags_json outside the transaction, then atomically
// backfills the JSON projection, removes duplicate Memory keys, and creates
// the unique scope/key index. Keeping the data rewrite together ensures an
// error cannot advance schema_version with only part of the v26 data change.
func migrateMemoryRagV26(ctx context.Context, db sqlSchemaExecutor, dialect SQLDialect) error {
	memoryExists, err := sqlTableExists(ctx, db, dialect, "memory_entries")
	if err != nil {
		return fmt.Errorf("inspect memory entries: %w", err)
	}
	ragExists, err := sqlTableExists(ctx, db, dialect, "rag_documents")
	if err != nil {
		return fmt.Errorf("inspect rag documents: %w", err)
	}
	if memoryExists {
		if err := addSQLColumn(ctx, db, dialect, "memory_entries", "tags_json", "TEXT NOT NULL DEFAULT '[]'"); err != nil {
			return fmt.Errorf("add memory entries tags json: %w", err)
		}
	}
	if ragExists {
		if err := addSQLColumn(ctx, db, dialect, "rag_documents", "tags_json", "TEXT NOT NULL DEFAULT '[]'"); err != nil {
			return fmt.Errorf("add rag documents tags json: %w", err)
		}
	}
	if !memoryExists && !ragExists {
		return nil
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if memoryExists {
		if err := backfillMemoryRagTagsJSON(ctx, tx, dialect, "memory_entries"); err != nil {
			return err
		}
	}
	if ragExists {
		if err := backfillMemoryRagTagsJSON(ctx, tx, dialect, "rag_documents"); err != nil {
			return err
		}
	}
	if memoryExists {
		if _, err := tx.ExecContext(ctx, `DELETE FROM memory_entries
			WHERE EXISTS (
				SELECT 1 FROM memory_entries AS winner
				WHERE winner.scope = memory_entries.scope
					AND winner.key = memory_entries.key
					AND (winner.created_at > memory_entries.created_at
						OR (winner.created_at = memory_entries.created_at AND winner.id > memory_entries.id))
			)`); err != nil {
			return fmt.Errorf("dedupe memory entries: %w", err)
		}
		if _, err := tx.ExecContext(ctx, "CREATE UNIQUE INDEX IF NOT EXISTS memory_scope_key ON memory_entries (scope, key)"); err != nil {
			return fmt.Errorf("create memory scope/key index: %w", err)
		}
	}
	return tx.Commit()
}

func backfillMemoryRagTagsJSON(ctx context.Context, tx *sql.Tx, dialect SQLDialect, table string) error {
	if table != "memory_entries" && table != "rag_documents" {
		return fmt.Errorf("unsupported memory/rag table %q", table)
	}
	rows, err := tx.QueryContext(ctx, "SELECT scope, id, tags FROM "+table)
	if err != nil {
		return fmt.Errorf("read %s tags: %w", table, err)
	}
	legacy := []memoryRagTagsRow{}
	for rows.Next() {
		var row memoryRagTagsRow
		if err := rows.Scan(&row.Scope, &row.ID, &row.Tags); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan %s tags: %w", table, err)
		}
		legacy = append(legacy, row)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("read %s tags: %w", table, err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close %s tags: %w", table, err)
	}

	query := (sqlQuery{"UPDATE " + table + " SET tags_json = ? WHERE scope = ? AND id = ?"}).bind(dialect)
	for _, row := range legacy {
		tags := []string{}
		if row.Tags != "" {
			tags = strings.Split(row.Tags, ",")
		}
		encoded, err := json.Marshal(tags)
		if err != nil {
			return fmt.Errorf("encode %s %s/%s tags: %w", table, row.Scope, row.ID, err)
		}
		if _, err := tx.ExecContext(ctx, query, string(encoded), row.Scope, row.ID); err != nil {
			return fmt.Errorf("update %s %s/%s tags: %w", table, row.Scope, row.ID, err)
		}
	}
	return nil
}

type ragProjectionRow struct {
	Scope    string
	ID       string
	Source   string
	Content  string
	TagsJSON string
}

// migrateRagProjectionV27 rebuilds the derived RAG lookup tables from the
// canonical rag_documents rows. The whole operation is transactional so a
// malformed tags_json value or failed insert leaves a v26 database without a
// partially committed projection and makes retry safe.
func migrateRagProjectionV27(ctx context.Context, db sqlSchemaExecutor, dialect SQLDialect) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := rebuildRagProjectionTx(ctx, tx, dialect); err != nil {
		return err
	}
	return tx.Commit()
}

const ragTokenizerSchemaVersionV47 = 47

// migrateRagTokenizerV47 rebuilds the derived token/tag projection for the
// v47 tokenizer semantics. The marker and replacement projection commit in the
// same transaction: reporting v47 therefore proves every canonical document
// has been reindexed with the current tokenizer.
func migrateRagTokenizerV47(ctx context.Context, db sqlSchemaExecutor, dialect SQLDialect) error {
	switch dialect {
	case SQLDialectSQLite:
		sqlDB, ok := db.(*sql.DB)
		if !ok {
			return fmt.Errorf("SQLite RAG tokenizer migration requires a database handle")
		}
		return migrateRagTokenizerV47SQLite(ctx, sqlDB, dialect)
	case SQLDialectPostgres:
		return migrateRagTokenizerV47Postgres(ctx, db, dialect)
	default:
		return fmt.Errorf("unsupported SQL dialect %q", dialect)
	}
}

func migrateRagTokenizerV47SQLite(ctx context.Context, db *sql.DB, dialect SQLDialect) error {
	const attempts = 5
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		conn, err := db.Conn(ctx)
		if err != nil {
			return err
		}
		_, err = conn.ExecContext(ctx, "BEGIN IMMEDIATE")
		if err == nil {
			err = applyRagTokenizerV47(ctx, conn, dialect)
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
			return verifyRagTokenizerV47(ctx, db, dialect)
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

func migrateRagTokenizerV47Postgres(ctx context.Context, db sqlSchemaExecutor, dialect SQLDialect) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// store_meta serializes v47 openers. rebuildRagProjectionTx then acquires
	// the projection-table lock before it clears any derived rows.
	if _, err := tx.ExecContext(ctx, "LOCK TABLE store_meta IN ACCESS EXCLUSIVE MODE"); err != nil {
		return err
	}
	if err := applyRagTokenizerV47(ctx, tx, dialect); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return verifyRagTokenizerV47(ctx, db, dialect)
}

type ragTokenizerMigrationExecutor interface {
	ragProjectionExecutor
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func applyRagTokenizerV47(ctx context.Context, exec ragTokenizerMigrationExecutor, dialect SQLDialect) error {
	stored, found, err := ragTokenizerSchemaVersion(ctx, exec, dialect)
	if err != nil {
		return err
	}
	if found && stored > SQLSchemaVersion {
		return fmt.Errorf("sql schema version %d is not supported (this build writes version %d)", stored, SQLSchemaVersion)
	}
	if found && stored >= ragTokenizerSchemaVersionV47 {
		return nil
	}
	if err := rebuildRagProjectionTx(ctx, exec, dialect); err != nil {
		return err
	}
	if err := recordRagTokenizerSchemaVersionV47(ctx, exec, dialect); err != nil {
		return err
	}
	return nil
}

func verifyRagTokenizerV47(ctx context.Context, exec ragTokenizerMigrationExecutor, dialect SQLDialect) error {
	stored, found, err := ragTokenizerSchemaVersion(ctx, exec, dialect)
	if err != nil {
		return err
	}
	// A later semantic migration can advance the shared marker after the v47
	// projection transaction commits. Any current marker at or beyond v47 still
	// proves this tokenizer rebuild completed.
	if !found || stored < ragTokenizerSchemaVersionV47 || stored > SQLSchemaVersion {
		return fmt.Errorf("RAG tokenizer migration did not reach schema version %d", ragTokenizerSchemaVersionV47)
	}
	return nil
}

func ragTokenizerSchemaVersion(ctx context.Context, exec ragTokenizerMigrationExecutor, dialect SQLDialect) (int, bool, error) {
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

func recordRagTokenizerSchemaVersionV47(ctx context.Context, exec ragProjectionExecutor, dialect SQLDialect) error {
	result, err := exec.ExecContext(ctx, sqlUpdateMetaRow.bind(dialect), strconv.Itoa(ragTokenizerSchemaVersionV47))
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		if _, err := exec.ExecContext(ctx, sqlInsertMetaRow.bind(dialect), strconv.Itoa(ragTokenizerSchemaVersionV47)); err != nil {
			return err
		}
	}
	return nil
}

const memorySearchProjectionBatchSize = 32

var memorySearchProjectionSchemaStatements = []string{
	`CREATE TABLE IF NOT EXISTS memory_entry_tags (
		scope TEXT NOT NULL,
		key   TEXT NOT NULL,
		tag   TEXT NOT NULL,
		PRIMARY KEY (scope, key, tag)
	)`,
	`CREATE INDEX IF NOT EXISTS memory_tags_lookup
		ON memory_entry_tags (tag, scope, key)`,
}

var (
	sqlMemorySearchFirstBatch = sqlQuery{`SELECT scope, id, key, content, tags_json
		FROM memory_entries ORDER BY scope, id LIMIT ?`}
	sqlMemorySearchNextBatch = sqlQuery{`SELECT scope, id, key, content, tags_json
		FROM memory_entries
		WHERE scope > ? OR (scope = ? AND id > ?)
		ORDER BY scope, id LIMIT ?`}
	sqlMemorySearchUpdate = sqlQuery{`UPDATE memory_entries
		SET key_search = ?, content_search = ?
		WHERE scope = ? AND id = ?`}
	sqlMemorySearchInsertTag = sqlQuery{`INSERT INTO memory_entry_tags (scope, key, tag)
		VALUES (?, ?, ?)`}
)

type memorySearchProjectionRow struct {
	Scope    string
	ID       string
	Key      string
	Content  string
	TagsJSON string
}

// migrateMemorySearchProjectionV28 adds the derived lower-cased search
// columns outside its rebuild transaction, as SQLite has no portable ADD
// COLUMN IF NOT EXISTS form. The projection rebuild itself remains atomic:
// a failed decode, update, or insert leaves the version and old projection
// unchanged, while a retry safely observes already-added columns.
func migrateMemorySearchProjectionV28(ctx context.Context, db sqlSchemaExecutor, dialect SQLDialect) error {
	exists, err := sqlTableExists(ctx, db, dialect, "memory_entries")
	if err != nil {
		return fmt.Errorf("inspect memory search projection: %w", err)
	}
	if !exists {
		return nil
	}
	if err := addSQLColumn(ctx, db, dialect, "memory_entries", "key_search", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return fmt.Errorf("add memory key search: %w", err)
	}
	if err := addSQLColumn(ctx, db, dialect, "memory_entries", "content_search", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return fmt.Errorf("add memory content search: %w", err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := rebuildMemorySearchProjectionTx(ctx, tx, dialect); err != nil {
		return err
	}
	return tx.Commit()
}

func ensureMemorySearchProjectionSchemaTx(ctx context.Context, tx *sql.Tx) error {
	for _, statement := range memorySearchProjectionSchemaStatements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("ensure memory search projection schema: %w", err)
		}
	}
	return nil
}

// rebuildMemorySearchProjectionTx replaces only derived Memory search state.
// key, content, and tags_json are canonical and are never rewritten.
func rebuildMemorySearchProjectionTx(ctx context.Context, tx *sql.Tx, dialect SQLDialect) error {
	if err := ensureMemorySearchProjectionSchemaTx(ctx, tx); err != nil {
		return err
	}
	if dialect == SQLDialectPostgres {
		if _, err := tx.ExecContext(ctx, `LOCK TABLE memory_entries, memory_entry_tags
			IN SHARE ROW EXCLUSIVE MODE`); err != nil {
			return fmt.Errorf("lock memory search projection tables: %w", err)
		}
	}
	// The first DELETE obtains SQLite's write lock before any canonical read.
	if _, err := tx.ExecContext(ctx, "DELETE FROM memory_entry_tags"); err != nil {
		return fmt.Errorf("clear memory tag projection: %w", err)
	}

	var cursor *memorySearchProjectionRow
	for {
		entries, err := readMemorySearchProjectionBatch(ctx, tx, dialect, cursor)
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			return nil
		}
		if err := writeMemorySearchProjectionBatch(ctx, tx, dialect, entries); err != nil {
			return err
		}
		last := entries[len(entries)-1]
		cursor = &memorySearchProjectionRow{Scope: last.Scope, ID: last.ID}
	}
}

func readMemorySearchProjectionBatch(ctx context.Context, tx *sql.Tx, dialect SQLDialect, cursor *memorySearchProjectionRow) ([]memorySearchProjectionRow, error) {
	query := sqlMemorySearchFirstBatch.bind(dialect)
	args := []any{memorySearchProjectionBatchSize}
	if cursor != nil {
		query = sqlMemorySearchNextBatch.bind(dialect)
		args = []any{cursor.Scope, cursor.Scope, cursor.ID, memorySearchProjectionBatchSize}
	}
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("read memory entries for search projection: %w", err)
	}

	entries := make([]memorySearchProjectionRow, 0, memorySearchProjectionBatchSize)
	for rows.Next() {
		var entry memorySearchProjectionRow
		if err := rows.Scan(&entry.Scope, &entry.ID, &entry.Key, &entry.Content, &entry.TagsJSON); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan memory entry for search projection: %w", err)
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("read memory entries for search projection: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close memory entries for search projection: %w", err)
	}
	if len(entries) > memorySearchProjectionBatchSize {
		return nil, fmt.Errorf("read %d memory entries in a search projection batch; limit is %d", len(entries), memorySearchProjectionBatchSize)
	}
	return entries, nil
}

func writeMemorySearchProjectionBatch(ctx context.Context, tx *sql.Tx, dialect SQLDialect, entries []memorySearchProjectionRow) error {
	update := sqlMemorySearchUpdate.bind(dialect)
	insertTag := sqlMemorySearchInsertTag.bind(dialect)
	for _, entry := range entries {
		tags, err := decodeMemorySearchTags(entry.TagsJSON)
		if err != nil {
			return fmt.Errorf("decode memory entry %s/%s tags: %w", entry.Scope, entry.ID, err)
		}
		if entry.Scope == "" {
			if err := memory.ValidateScope(core.ScopePath{}); err != nil {
				return err
			}
		}
		if err := memory.ValidateEntry(memory.Entry{
			ID: entry.ID, Key: entry.Key, Content: entry.Content, Tags: tags,
		}); err != nil {
			return fmt.Errorf("memory entry %s/%s: %w", entry.Scope, entry.ID, err)
		}
		result, err := tx.ExecContext(ctx, update,
			normalizeMemorySearchText(entry.Key), normalizeMemorySearchText(entry.Content), entry.Scope, entry.ID,
		)
		if err != nil {
			return fmt.Errorf("update memory search text %s/%s: %w", entry.Scope, entry.ID, err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("inspect memory search text update %s/%s: %w", entry.Scope, entry.ID, err)
		}
		if affected != 1 {
			return fmt.Errorf("update memory search text %s/%s affected %d rows", entry.Scope, entry.ID, affected)
		}
		for _, tag := range tags {
			if _, err := tx.ExecContext(ctx, insertTag, entry.Scope, entry.Key, tag); err != nil {
				return fmt.Errorf("insert memory tag %s/%s: %w", entry.Scope, entry.ID, err)
			}
		}
	}
	return nil
}

// normalizeMemorySearchText is the canonical derived-text rule for Memory
// search. It is package-visible so a future SQL Memory query path can use the
// exact same Unicode-aware normalization as the migration.
func normalizeMemorySearchText(value string) string {
	return strings.ToLower(value)
}

// decodeJSONStringArray validates canonical tags_json as an array of strings.
// Null items are rejected. Empty arrays are valid; a JSON null is not.
func decodeJSONStringArray(raw string) ([]string, error) {
	var encodedTags []json.RawMessage
	if err := json.Unmarshal([]byte(raw), &encodedTags); err != nil {
		return nil, err
	}
	if encodedTags == nil {
		return nil, fmt.Errorf("must be a JSON array of strings")
	}
	tags := make([]string, len(encodedTags))
	for i, encodedTag := range encodedTags {
		if strings.TrimSpace(string(encodedTag)) == "null" {
			return nil, fmt.Errorf("must contain only strings")
		}
		if err := json.Unmarshal(encodedTag, &tags[i]); err != nil {
			return nil, fmt.Errorf("decode tag: %w", err)
		}
	}
	return tags, nil
}

// decodeMemorySearchTags validates canonical tags_json and returns the exact
// tag lookup projection: unique and lexicographically sorted. It deliberately
// preserves commas, whitespace, Unicode, and original case in every tag.
func decodeMemorySearchTags(raw string) ([]string, error) {
	tags, err := decodeJSONStringArray(raw)
	if err != nil {
		return nil, err
	}
	return sortedSet(stringSet(tags)), nil
}

// ragProjectionTokens uses the shared RAG keyword tokenizer so rebuilds stay
// aligned with SQL ingest and the in-process index.
func ragProjectionTokens(text string) []string {
	return sortedRagProjectionValues(rag.Tokenize(text))
}

func decodeRagProjectionTags(raw string) ([]string, error) {
	tags, err := decodeJSONStringArray(raw)
	if err != nil {
		return nil, err
	}
	return sortedSet(stringSet(tags)), nil
}

func sortedRagProjectionValues(unique map[string]bool) []string {
	values := make([]string, 0, len(unique))
	for value := range unique {
		values = append(values, value)
	}
	sort.Strings(values)
	return values
}
