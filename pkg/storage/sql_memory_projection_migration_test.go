package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"testing"
)

const (
	memorySearchProjectionScopeA = "global/product/acme"
	memorySearchProjectionScopeB = "global/product/acme/region/eu"
)

func TestSQLMemorySearchProjectionSchemaV28SQLite(t *testing.T) {
	t.Run("fresh", func(t *testing.T) {
		testMemorySearchProjectionV28Fresh(t, SQLDialectSQLite, newSQLiteMemorySearchProjectionTestDB(t))
	})
	t.Run("v27_to_v28", func(t *testing.T) {
		testMemorySearchProjectionV28Migration(t, SQLDialectSQLite, newSQLiteMemorySearchProjectionTestDB(t))
	})
	t.Run("corrupt_tags_json_rolls_back_and_retries", func(t *testing.T) {
		testMemorySearchProjectionV28CorruptTagsRollback(t, SQLDialectSQLite, newSQLiteMemorySearchProjectionTestDB(t))
	})
	t.Run("partial_add_column_retries", func(t *testing.T) {
		testMemorySearchProjectionV28PartialAddColumnRetry(t, SQLDialectSQLite, newSQLiteMemorySearchProjectionTestDB(t))
	})
	t.Run("future_version_refused_before_ddl", func(t *testing.T) {
		testMemorySearchProjectionV28FutureVersionRefusal(t, SQLDialectSQLite, newSQLiteMemorySearchProjectionTestDB(t))
	})
}

func TestPostgresMemorySearchProjectionSchemaV28(t *testing.T) {
	t.Run("fresh", func(t *testing.T) {
		testMemorySearchProjectionV28Fresh(t, SQLDialectPostgres, newPostgresTestDB(t))
	})
	t.Run("v27_to_v28", func(t *testing.T) {
		testMemorySearchProjectionV28Migration(t, SQLDialectPostgres, newPostgresTestDB(t))
	})
	t.Run("corrupt_tags_json_rolls_back_and_retries", func(t *testing.T) {
		testMemorySearchProjectionV28CorruptTagsRollback(t, SQLDialectPostgres, newPostgresTestDB(t))
	})
	t.Run("partial_add_column_retries", func(t *testing.T) {
		testMemorySearchProjectionV28PartialAddColumnRetry(t, SQLDialectPostgres, newPostgresTestDB(t))
	})
	t.Run("locks_canonical_before_projection_delete", func(t *testing.T) {
		testPostgresMemorySearchProjectionV28Lock(t)
	})
	t.Run("future_version_refused_before_ddl", func(t *testing.T) {
		testMemorySearchProjectionV28FutureVersionRefusal(t, SQLDialectPostgres, newPostgresTestDB(t))
	})
}

func testPostgresMemorySearchProjectionV28Lock(t *testing.T) {
	t.Helper()
	db := newPostgresTestDB(t)
	ctx := context.Background()
	seedV27MemorySearchProjectionSchema(t, ctx, db, SQLDialectPostgres)
	if _, err := db.ExecContext(ctx, `CREATE TABLE memory_entry_tags (
			scope TEXT NOT NULL,
			key TEXT NOT NULL,
			tag TEXT NOT NULL,
			PRIMARY KEY (scope, key, tag)
		);
		CREATE FUNCTION assert_memory_projection_lock() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF NOT EXISTS (
				SELECT 1 FROM pg_locks
				WHERE pid = pg_backend_pid()
					AND relation = 'memory_entries'::regclass
					AND mode = 'ShareRowExclusiveLock'
			) THEN
				RAISE EXCEPTION 'memory_entries ShareRowExclusiveLock is required before projection delete';
			END IF;
			RETURN NULL;
		END;
		$$;
		CREATE TRIGGER assert_memory_projection_lock
		BEFORE DELETE ON memory_entry_tags
		FOR EACH STATEMENT EXECUTE FUNCTION assert_memory_projection_lock()`); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres); err != nil {
		t.Fatalf("v28 migration did not lock canonical Memory before projection delete: %v", err)
	}
	requireMemorySearchProjectionV28Schema(t, ctx, db, SQLDialectPostgres)
}

func newSQLiteMemorySearchProjectionTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", t.TempDir()+"/memory-search-projection.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func testMemorySearchProjectionV28Fresh(t *testing.T, dialect SQLDialect, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err != nil {
		t.Fatalf("open fresh v28 schema: %v", err)
	}
	requireMemorySearchProjectionV28Schema(t, ctx, db, dialect)
	assertMemorySearchProjectionHasNoForeignKey(t, ctx, db, dialect)
	assertMemorySearchProjectionSchemaVersion(t, ctx, db, dialect, SQLSchemaVersion)
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err != nil {
		t.Fatalf("repeat fresh v28 open: %v", err)
	}
}

func testMemorySearchProjectionV28Migration(t *testing.T, dialect SQLDialect, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	seedV27MemorySearchProjectionSchema(t, ctx, db, dialect)
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err != nil {
		t.Fatalf("v27 to v28 migration: %v", err)
	}
	requireMemorySearchProjectionV28Schema(t, ctx, db, dialect)
	assertMemorySearchProjectionData(t, ctx, db, dialect)
	assertMemorySearchProjectionSchemaVersion(t, ctx, db, dialect, SQLSchemaVersion)
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err != nil {
		t.Fatalf("repeat v28 open: %v", err)
	}
	assertMemorySearchProjectionData(t, ctx, db, dialect)
}

func testMemorySearchProjectionV28CorruptTagsRollback(t *testing.T, dialect SQLDialect, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	seedV27MemorySearchProjectionSchema(t, ctx, db, dialect)
	updateTags := (sqlQuery{`UPDATE memory_entries SET tags_json = ?
		WHERE scope = ? AND id = ?`}).bind(dialect)
	if _, err := db.ExecContext(ctx, updateTags, `["valid",null]`, memorySearchProjectionScopeA, "memory-unicode"); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err == nil {
		t.Fatal("corrupt memory tags_json was accepted")
	}
	assertMemorySearchProjectionSchemaVersion(t, ctx, db, dialect, 27)
	for _, column := range []string{"key_search", "content_search"} {
		exists, err := sqlColumnExists(ctx, db, dialect, "memory_entries", column)
		if err != nil || !exists {
			t.Fatalf("failed v28 migration did not retain safely-added %s: exists=%t err=%v", column, exists, err)
		}
	}
	exists, err := sqlTableExists(ctx, db, dialect, "memory_entry_tags")
	if err != nil || exists {
		t.Fatalf("failed v28 migration partially committed tag projection: exists=%t err=%v", exists, err)
	}
	var keySearch, contentSearch string
	if err := db.QueryRowContext(ctx, (sqlQuery{`SELECT key_search, content_search FROM memory_entries
		WHERE scope = ? AND id = ?`}).bind(dialect), memorySearchProjectionScopeA, "memory-unicode").Scan(&keySearch, &contentSearch); err != nil || keySearch != "" || contentSearch != "" {
		t.Fatalf("failed v28 migration committed derived search text: key=%q content=%q err=%v", keySearch, contentSearch, err)
	}
	if _, err := db.ExecContext(ctx, updateTags, `["Team,Infra","标签,组","repeat","repeat"]`, memorySearchProjectionScopeA, "memory-unicode"); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err != nil {
		t.Fatalf("retry v28 migration after repairing tags_json: %v", err)
	}
	requireMemorySearchProjectionV28Schema(t, ctx, db, dialect)
	assertMemorySearchProjectionData(t, ctx, db, dialect)
	assertMemorySearchProjectionSchemaVersion(t, ctx, db, dialect, SQLSchemaVersion)
}

func testMemorySearchProjectionV28PartialAddColumnRetry(t *testing.T, dialect SQLDialect, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	seedV27MemorySearchProjectionSchema(t, ctx, db, dialect)
	if _, err := db.ExecContext(ctx, "ALTER TABLE memory_entries ADD COLUMN key_search TEXT NOT NULL DEFAULT ''"); err != nil {
		t.Fatalf("seed partial v28 key_search column: %v", err)
	}
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err != nil {
		t.Fatalf("retry after partial v28 add-column state: %v", err)
	}
	requireMemorySearchProjectionV28Schema(t, ctx, db, dialect)
	assertMemorySearchProjectionData(t, ctx, db, dialect)
	assertMemorySearchProjectionSchemaVersion(t, ctx, db, dialect, SQLSchemaVersion)
}

func testMemorySearchProjectionV28FutureVersionRefusal(t *testing.T, dialect SQLDialect, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE TABLE store_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, sqlInsertMetaRow.bind(dialect), strconv.Itoa(SQLSchemaVersion+1)); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err == nil {
		t.Fatal("future v28 schema version was accepted")
	}
	for _, table := range []string{"memory_entries", "memory_entry_tags"} {
		exists, err := sqlTableExists(ctx, db, dialect, table)
		if err != nil || exists {
			t.Fatalf("future-version refusal ran v28 DDL for %s: exists=%t err=%v", table, exists, err)
		}
	}
}

func seedV27MemorySearchProjectionSchema(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect) {
	t.Helper()
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "DROP TABLE memory_entry_tags"); err != nil {
		t.Fatalf("remove current memory tag projection: %v", err)
	}
	for _, column := range []string{"key_search", "content_search"} {
		if _, err := db.ExecContext(ctx, "ALTER TABLE memory_entries DROP COLUMN "+column); err != nil {
			t.Fatalf("remove current %s column: %v", column, err)
		}
	}
	if _, err := db.ExecContext(ctx, sqlUpdateMetaRow.bind(dialect), "27"); err != nil {
		t.Fatal(err)
	}
	insert := (sqlQuery{`INSERT INTO memory_entries
		(scope, id, key, content, tags, tags_json, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`}).bind(dialect)
	createdAt := int64(100)
	for _, row := range memorySearchProjectionSeedRows() {
		if _, err := db.ExecContext(ctx, insert,
			row.Scope, row.ID, row.Key, row.Content, "legacy-shadow", row.TagsJSON, createdAt,
		); err != nil {
			t.Fatal(err)
		}
		createdAt++
	}
}

type memorySearchProjectionCanonicalRow struct {
	Scope    string
	ID       string
	Key      string
	Content  string
	TagsJSON string
}

func memorySearchProjectionSeedRows() []memorySearchProjectionCanonicalRow {
	rows := make([]memorySearchProjectionCanonicalRow, 0, 36)
	for i := 0; i < 17; i++ {
		rows = append(rows, memorySearchProjectionCanonicalRow{
			Scope:   memorySearchProjectionScopeA,
			ID:      fmt.Sprintf("memory-a-%02d", i),
			Key:     fmt.Sprintf("Key A %02d", i),
			Content: fmt.Sprintf("Content A %02d", i),
			TagsJSON: `[
"a-tag"
]`,
		})
	}
	rows = append(rows, memorySearchProjectionCanonicalRow{
		Scope:    memorySearchProjectionScopeA,
		ID:       "memory-unicode",
		Key:      "ÄPFEL Σ",
		Content:  "MÜNCHEN Σ TEAM,INFRA",
		TagsJSON: `["Team,Infra","标签,组","repeat","repeat"]`,
	})
	for i := 0; i < 17; i++ {
		rows = append(rows, memorySearchProjectionCanonicalRow{
			Scope:   memorySearchProjectionScopeB,
			ID:      fmt.Sprintf("memory-b-%02d", i),
			Key:     fmt.Sprintf("Key B %02d", i),
			Content: fmt.Sprintf("Content B %02d", i),
			TagsJSON: `[
"b-tag"
]`,
		})
	}
	rows = append(rows, memorySearchProjectionCanonicalRow{
		Scope:    memorySearchProjectionScopeB,
		ID:       "memory-empty",
		Key:      "EMPTY",
		Content:  "NO TAGS",
		TagsJSON: `[]`,
	})
	return rows
}

func requireMemorySearchProjectionV28Schema(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect) {
	t.Helper()
	for _, column := range []string{"key_search", "content_search"} {
		exists, err := sqlColumnExists(ctx, db, dialect, "memory_entries", column)
		if err != nil || !exists {
			t.Fatalf("v28 memory_entries.%s missing: exists=%t err=%v", column, exists, err)
		}
	}
	exists, err := sqlTableExists(ctx, db, dialect, "memory_entry_tags")
	if err != nil || !exists {
		t.Fatalf("v28 memory_entry_tags missing: exists=%t err=%v", exists, err)
	}
	for _, index := range []string{"memory_scope_key", "memory_tags_lookup"} {
		var count int
		if dialect == SQLDialectPostgres {
			err = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pg_indexes
				WHERE schemaname = current_schema() AND indexname = $1`, index).Scan(&count)
		} else {
			err = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master
				WHERE type = 'index' AND name = ?`, index).Scan(&count)
		}
		if err != nil || count != 1 {
			t.Fatalf("v28 %s missing: count=%d err=%v", index, count, err)
		}
	}
}

func assertMemorySearchProjectionHasNoForeignKey(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect) {
	t.Helper()
	insert := (sqlQuery{`INSERT INTO memory_entry_tags (scope, key, tag) VALUES (?, ?, ?)`}).bind(dialect)
	if _, err := db.ExecContext(ctx, insert, "orphan-scope", "orphan-key", "orphan-tag"); err != nil {
		t.Fatalf("v28 memory tag projection unexpectedly has a foreign-key dependency: %v", err)
	}
	if _, err := db.ExecContext(ctx, insert, "orphan-scope", "orphan-key", "orphan-tag"); err == nil {
		t.Fatal("v28 memory tag projection primary key accepted a duplicate row")
	}
	if _, err := db.ExecContext(ctx, (sqlQuery{`DELETE FROM memory_entry_tags WHERE scope = ? AND key = ? AND tag = ?`}).bind(dialect), "orphan-scope", "orphan-key", "orphan-tag"); err != nil {
		t.Fatal(err)
	}
}

func assertMemorySearchProjectionData(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect) {
	t.Helper()
	seed := memorySearchProjectionSeedRows()
	var total, emptyDerived int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*), COUNT(*) FILTER (WHERE key_search = '' OR content_search = '') FROM memory_entries`).Scan(&total, &emptyDerived); err != nil {
		if dialect != SQLDialectSQLite {
			t.Fatal(err)
		}
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*), SUM(CASE WHEN key_search = '' OR content_search = '' THEN 1 ELSE 0 END) FROM memory_entries`).Scan(&total, &emptyDerived); err != nil {
			t.Fatal(err)
		}
	}
	if total != len(seed) || emptyDerived != 0 {
		t.Fatalf("v28 keyset rebuild count=%d empty-derived=%d, want %d/0", total, emptyDerived, len(seed))
	}

	var key, content, tagsJSON, keySearch, contentSearch string
	if err := db.QueryRowContext(ctx, (sqlQuery{`SELECT key, content, tags_json, key_search, content_search
		FROM memory_entries WHERE scope = ? AND id = ?`}).bind(dialect), memorySearchProjectionScopeA, "memory-unicode").Scan(
		&key, &content, &tagsJSON, &keySearch, &contentSearch,
	); err != nil {
		t.Fatal(err)
	}
	if key != "ÄPFEL Σ" || content != "MÜNCHEN Σ TEAM,INFRA" || tagsJSON != `["Team,Infra","标签,组","repeat","repeat"]` {
		t.Fatalf("v28 migration changed canonical Memory row: key=%q content=%q tags=%q", key, content, tagsJSON)
	}
	if keySearch != "äpfel σ" || contentSearch != "münchen σ team,infra" {
		t.Fatalf("v28 Unicode lower projection key=%q content=%q", keySearch, contentSearch)
	}

	assertMemorySearchProjectionRows(t, ctx, db, dialect, memorySearchProjectionScopeA, "ÄPFEL Σ", []string{"Team,Infra", "repeat", "标签,组"})
	assertMemorySearchProjectionRows(t, ctx, db, dialect, memorySearchProjectionScopeB, "EMPTY", []string{})
}

func assertMemorySearchProjectionRows(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect, scope, key string, want []string) {
	t.Helper()
	query := (sqlQuery{`SELECT tag FROM memory_entry_tags WHERE scope = ? AND key = ? ORDER BY tag`}).bind(dialect)
	rows, err := db.QueryContext(ctx, query, scope, key)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := []string{}
	for rows.Next() {
		var tag string
		if err := rows.Scan(&tag); err != nil {
			t.Fatal(err)
		}
		got = append(got, tag)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	sort.Strings(got)
	sortedWant := make([]string, len(want))
	copy(sortedWant, want)
	sort.Strings(sortedWant)
	if !reflect.DeepEqual(got, sortedWant) {
		t.Fatalf("v28 memory tags for %s/%q = %#v, want %#v", scope, key, got, sortedWant)
	}
}

func assertMemorySearchProjectionSchemaVersion(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect, want int) {
	t.Helper()
	var got string
	if err := db.QueryRowContext(ctx, sqlSelectMetaRow.bind(dialect)).Scan(&got); err != nil || got != strconv.Itoa(want) {
		t.Fatalf("schema version = %q, want %d: %v", got, want, err)
	}
}

func TestMemorySearchProjectionDecodeTagsStrictly(t *testing.T) {
	got, err := decodeMemorySearchTags(`["Team,Infra","标签,组","repeat","repeat"]`)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"Team,Infra", "repeat", "标签,组"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded projection tags=%#v want %#v", got, want)
	}
	for _, raw := range []string{"null", `["tag",null]`, `["tag",1]`, `{}`} {
		if _, err := decodeMemorySearchTags(raw); err == nil {
			t.Fatalf("invalid tags_json %s was accepted", raw)
		}
	}
	encoded, err := json.Marshal([]string{"Team,Infra", "标签,组", "repeat", "repeat"})
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != `["Team,Infra","标签,组","repeat","repeat"]` {
		t.Fatalf("unexpected canonical test tags JSON %q", encoded)
	}
}
