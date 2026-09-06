package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"strconv"
	"testing"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

const memoryRagMigrationScope = "global/product/acme"

func TestSQLMemoryRAGSchemaV26Fresh(t *testing.T) {
	db, err := sql.Open("sqlite", t.TempDir()+"/fresh-memory-rag.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectSQLite); err != nil {
		t.Fatalf("open fresh v26 schema: %v", err)
	}
	requireMemoryRagV26Schema(t, ctx, db, SQLDialectSQLite)
	assertMemoryScopeKeyIsUnique(t, ctx, db, SQLDialectSQLite)
}

func TestSQLMemoryRAGSchemaV25ToV26Migration(t *testing.T) {
	db, err := sql.Open("sqlite", t.TempDir()+"/v25-memory-rag.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	seedV25MemoryRagSchema(t, ctx, db, SQLDialectSQLite)

	for _, table := range []string{"memory_entries", "rag_documents"} {
		exists, err := sqlColumnExists(ctx, db, SQLDialectSQLite, table, "tags_json")
		if err != nil || exists {
			t.Fatalf("v25 %s.tags_json state: exists=%t err=%v", table, exists, err)
		}
	}
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectSQLite); err != nil {
		t.Fatalf("v25 to v26 migration failed: %v", err)
	}
	requireMemoryRagV26Schema(t, ctx, db, SQLDialectSQLite)
	assertMemoryRagV26Data(t, ctx, db, SQLDialectSQLite)
	assertMemoryScopeKeyIsUnique(t, ctx, db, SQLDialectSQLite)

	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectSQLite); err != nil {
		t.Fatalf("repeat v26 open failed: %v", err)
	}
	assertMemoryRagV26Data(t, ctx, db, SQLDialectSQLite)
	var version string
	if err := db.QueryRowContext(ctx, sqlSelectMetaRow.bind(SQLDialectSQLite)).Scan(&version); err != nil || version != strconv.Itoa(SQLSchemaVersion) {
		t.Fatalf("v26 schema version not recorded after repeat open: version=%q err=%v", version, err)
	}
}

func TestSQLMemoryRAGSchemaV26BackfillFailureDoesNotAdvanceVersion(t *testing.T) {
	db, err := sql.Open("sqlite", t.TempDir()+"/v25-memory-rag-failure.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	seedV25MemoryRagSchema(t, ctx, db, SQLDialectSQLite)
	if _, err := db.ExecContext(ctx, `CREATE TRIGGER reject_rag_tags_json
		BEFORE UPDATE ON rag_documents
		BEGIN
			SELECT RAISE(ABORT, 'reject v26 tags backfill');
		END`); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectSQLite); err == nil {
		t.Fatal("v26 tags backfill failure was accepted")
	}
	var version string
	if err := db.QueryRowContext(ctx, sqlSelectMetaRow.bind(SQLDialectSQLite)).Scan(&version); err != nil || version != "25" {
		t.Fatalf("failed v26 migration advanced schema version: version=%q err=%v", version, err)
	}
	var tagsJSON string
	if err := db.QueryRowContext(ctx, `SELECT tags_json FROM memory_entries
		WHERE scope = ? AND id = ?`, memoryRagMigrationScope, "memory-winner-z").Scan(&tagsJSON); err != nil || tagsJSON != "[]" {
		t.Fatalf("rag backfill failure committed Memory tags_json: tags_json=%q err=%v", tagsJSON, err)
	}
	var indexCount int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='memory_scope_key'").Scan(&indexCount); err != nil || indexCount != 0 {
		t.Fatalf("failed v26 migration created unique index: count=%d err=%v", indexCount, err)
	}
	if _, err := db.ExecContext(ctx, "DROP TRIGGER reject_rag_tags_json"); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectSQLite); err != nil {
		t.Fatalf("retry after partial v26 migration failed: %v", err)
	}
	requireMemoryRagV26Schema(t, ctx, db, SQLDialectSQLite)
	assertMemoryRagV26Data(t, ctx, db, SQLDialectSQLite)
	if err := db.QueryRowContext(ctx, sqlSelectMetaRow.bind(SQLDialectSQLite)).Scan(&version); err != nil || version != strconv.Itoa(SQLSchemaVersion) {
		t.Fatalf("retried v26 migration version=%q err=%v", version, err)
	}
}

func TestSQLMemoryRAGSchemaV26FutureVersionRefusalBeforeDDL(t *testing.T) {
	db, err := sql.Open("sqlite", t.TempDir()+"/future-memory-rag.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE TABLE store_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
		INSERT INTO store_meta (key, value) VALUES ('schema_version', ?);`, strconv.Itoa(SQLSchemaVersion+1)); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectSQLite); err == nil {
		t.Fatal("future schema version was accepted")
	}
	var tableCount, indexCount int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='memory_entries'").Scan(&tableCount); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='memory_scope_key'").Scan(&indexCount); err != nil {
		t.Fatal(err)
	}
	if tableCount != 0 || indexCount != 0 {
		t.Fatalf("v26 DDL ran before future-version refusal: tables=%d indexes=%d", tableCount, indexCount)
	}
}

func TestPostgresMemoryRAGSchemaV26Migration(t *testing.T) {
	t.Run("schema0_fresh", func(t *testing.T) {
		db := newPostgresTestDB(t)
		ctx := context.Background()
		if _, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres); err != nil {
			t.Fatalf("open fresh postgres v26 schema: %v", err)
		}
		requireMemoryRagV26Schema(t, ctx, db, SQLDialectPostgres)
		assertMemoryScopeKeyIsUnique(t, ctx, db, SQLDialectPostgres)
	})
	t.Run("v25_to_v26", func(t *testing.T) {
		db := newPostgresTestDB(t)
		ctx := context.Background()
		seedV25MemoryRagSchema(t, ctx, db, SQLDialectPostgres)
		for _, table := range []string{"memory_entries", "rag_documents"} {
			exists, err := sqlColumnExists(ctx, db, SQLDialectPostgres, table, "tags_json")
			if err != nil || exists {
				t.Fatalf("postgres v25 %s.tags_json state: exists=%t err=%v", table, exists, err)
			}
		}
		if _, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres); err != nil {
			t.Fatalf("postgres v25 to v26 migration failed: %v", err)
		}
		requireMemoryRagV26Schema(t, ctx, db, SQLDialectPostgres)
		assertMemoryRagV26Data(t, ctx, db, SQLDialectPostgres)
		assertMemoryScopeKeyIsUnique(t, ctx, db, SQLDialectPostgres)
		if _, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres); err != nil {
			t.Fatalf("repeat postgres v26 open failed: %v", err)
		}
		assertMemoryRagV26Data(t, ctx, db, SQLDialectPostgres)
		var version string
		if err := db.QueryRowContext(ctx, sqlSelectMetaRow.bind(SQLDialectPostgres)).Scan(&version); err != nil || version != strconv.Itoa(SQLSchemaVersion) {
			t.Fatalf("postgres v26 schema version not recorded after repeat open: version=%q err=%v", version, err)
		}
	})
	t.Run("future_version_before_v26_ddl", func(t *testing.T) {
		db := newPostgresTestDB(t)
		ctx := context.Background()
		if _, err := db.ExecContext(ctx, `CREATE TABLE store_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, "INSERT INTO store_meta (key, value) VALUES ('schema_version', $1)", strconv.Itoa(SQLSchemaVersion+1)); err != nil {
			t.Fatal(err)
		}
		if _, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres); err == nil {
			t.Fatal("future postgres schema version was accepted")
		}
		var table sql.NullString
		if err := db.QueryRowContext(ctx, "SELECT to_regclass('memory_entries')").Scan(&table); err != nil {
			t.Fatal(err)
		}
		if table.Valid {
			t.Fatal("postgres v26 DDL ran before future-version refusal")
		}
	})
}

func TestPostgresMemoryRAGV26StoreRoundTripAndUpsert(t *testing.T) {
	db := newPostgresTestDB(t)
	ctx := context.Background()
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres); err != nil {
		t.Fatal(err)
	}
	_, _, tenant, user := testScopes()
	memoryStore, err := NewSQLMemoryStore(db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	first, err := memoryStore.Remember(ctx, user, core.MemoryEntry{
		ID: "mem_pg_first", Key: "watchlist", Content: "SOL", Tags: []string{"team,infra", "标签", "duplicate", "duplicate"},
	})
	if err != nil {
		t.Fatal(err)
	}
	replacement, err := memoryStore.Remember(ctx, user, core.MemoryEntry{
		ID: "mem_pg_replacement", Key: "watchlist", Content: "ETH", Tags: []string{"new,tag", "替换"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if replacement.ID == first.ID {
		t.Fatal("postgres Memory replacement did not replace the canonical id")
	}
	entries, err := memoryStore.Recall(ctx, user, "eth", []string{"new,tag"}, 10)
	if err != nil || len(entries) != 1 || entries[0].ID != replacement.ID || !reflect.DeepEqual(entries[0].Tags, []string{"new,tag", "替换"}) {
		t.Fatalf("postgres Memory JSON tags/upsert = %#v err=%v", entries, err)
	}
	if old, err := memoryStore.Recall(ctx, user, "", []string{"team,infra"}, 10); err != nil || len(old) != 0 {
		t.Fatalf("postgres Memory upsert retained old tags: %#v err=%v", old, err)
	}
	if _, err := memoryStore.Remember(ctx, user, core.MemoryEntry{Key: "empty-tags", Content: "empty"}); err != nil {
		t.Fatal(err)
	}
	var tagsJSON string
	if err := db.QueryRowContext(ctx, `SELECT tags_json FROM memory_entries WHERE scope = $1 AND key = $2`, user.String(), "empty-tags").Scan(&tagsJSON); err != nil || tagsJSON != "[]" {
		t.Fatalf("postgres Memory empty tags_json=%q err=%v", tagsJSON, err)
	}

	ragIndex, err := NewSQLRagIndex(db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	if err := ragIndex.Ingest(ctx, tenant, core.RagDocument{
		ID: "pg-doc", Source: "old.md", Content: "Solana old document", Tags: []string{"team,infra", "标签"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ragIndex.Ingest(ctx, tenant, core.RagDocument{
		ID: "pg-doc", Source: "new.md", Content: "Solana replacement document", Tags: []string{"new,tag", "替换", "替换"},
	}); err != nil {
		t.Fatal(err)
	}
	hits, err := ragIndex.Search(ctx, user, core.RagQuery{Query: "solana", Tags: []string{"new,tag"}})
	if err != nil || len(hits) != 1 || hits[0].ID != "pg-doc" || hits[0].Source != "new.md" {
		t.Fatalf("postgres RAG JSON tags/upsert = %#v err=%v", hits, err)
	}
	if old, err := ragIndex.Search(ctx, user, core.RagQuery{Query: "solana", Tags: []string{"team,infra"}}); err != nil || len(old) != 0 {
		t.Fatalf("postgres RAG upsert retained old tags: %#v err=%v", old, err)
	}
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM rag_documents WHERE scope = $1 AND id = $2`, tenant.String(), "pg-doc").Scan(&count); err != nil || count != 1 {
		t.Fatalf("postgres RAG upsert row count=%d err=%v", count, err)
	}
}

func seedV25MemoryRagSchema(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect) {
	t.Helper()
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "DROP INDEX memory_scope_key"); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"memory_entries", "rag_documents"} {
		if _, err := db.ExecContext(ctx, "ALTER TABLE "+table+" DROP COLUMN tags_json"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(ctx, sqlUpdateMetaRow.bind(dialect), "25"); err != nil {
		t.Fatal(err)
	}
	insertMemory := (sqlQuery{`INSERT INTO memory_entries (scope, id, key, content, tags, created_at)
		VALUES (?, ?, ?, ?, ?, ?)`}).bind(dialect)
	for _, row := range []struct {
		id, key, content, tags string
		createdAt              int64
	}{
		{id: "memory-old", key: "shipping", content: "old content", tags: "old", createdAt: 100},
		{id: "memory-winner-a", key: "shipping", content: "same-time lower id", tags: "latest,a", createdAt: 200},
		{id: "memory-winner-z", key: "shipping", content: "same-time lexical winner", tags: "latest,z", createdAt: 200},
		{id: "memory-empty", key: "empty", content: "empty tags", tags: "", createdAt: 300},
	} {
		if _, err := db.ExecContext(ctx, insertMemory, memoryRagMigrationScope, row.id, row.key, row.content, row.tags, row.createdAt); err != nil {
			t.Fatal(err)
		}
	}
	insertRAG := (sqlQuery{`INSERT INTO rag_documents (scope, id, source, content, tags)
		VALUES (?, ?, ?, ?, ?)`}).bind(dialect)
	for _, row := range []struct{ id, tags string }{
		{id: "rag-split", tags: "guide,how-to"},
		{id: "rag-empty", tags: ""},
	} {
		if _, err := db.ExecContext(ctx, insertRAG, memoryRagMigrationScope, row.id, "migration-test", "document "+row.id, row.tags); err != nil {
			t.Fatal(err)
		}
	}
}

func requireMemoryRagV26Schema(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect) {
	t.Helper()
	for _, table := range []string{"memory_entries", "rag_documents"} {
		exists, err := sqlColumnExists(ctx, db, dialect, table, "tags_json")
		if err != nil || !exists {
			t.Fatalf("v26 %s.tags_json missing: exists=%t err=%v", table, exists, err)
		}
	}
	var indexCount int
	var err error
	if dialect == SQLDialectPostgres {
		err = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pg_indexes
			WHERE schemaname = current_schema() AND tablename = 'memory_entries' AND indexname = 'memory_scope_key'`).Scan(&indexCount)
	} else {
		err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='memory_scope_key'").Scan(&indexCount)
	}
	if err != nil || indexCount != 1 {
		t.Fatalf("v26 memory scope/key index missing: count=%d err=%v", indexCount, err)
	}
}

func assertMemoryRagV26Data(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect) {
	t.Helper()
	var count int
	if err := db.QueryRowContext(ctx, (sqlQuery{"SELECT COUNT(*) FROM memory_entries WHERE scope = ? AND key = ?"}).bind(dialect), memoryRagMigrationScope, "shipping").Scan(&count); err != nil || count != 1 {
		t.Fatalf("deduped shipping Memory rows: count=%d err=%v", count, err)
	}
	var id, content, tags, tagsJSON string
	var createdAt int64
	if err := db.QueryRowContext(ctx, (sqlQuery{`SELECT id, content, tags, tags_json, created_at
		FROM memory_entries WHERE scope = ? AND key = ?`}).bind(dialect), memoryRagMigrationScope, "shipping").Scan(&id, &content, &tags, &tagsJSON, &createdAt); err != nil {
		t.Fatal(err)
	}
	if id != "memory-winner-z" || content != "same-time lexical winner" || tags != "latest,z" || createdAt != 200 {
		t.Fatalf("dedupe did not preserve lexical latest winner: id=%q content=%q tags=%q created_at=%d", id, content, tags, createdAt)
	}
	assertJSONTags(t, tagsJSON, []string{"latest", "z"})

	if err := db.QueryRowContext(ctx, (sqlQuery{"SELECT tags_json FROM memory_entries WHERE scope = ? AND id = ?"}).bind(dialect), memoryRagMigrationScope, "memory-empty").Scan(&tagsJSON); err != nil {
		t.Fatal(err)
	}
	assertJSONTags(t, tagsJSON, []string{})
	if err := db.QueryRowContext(ctx, (sqlQuery{"SELECT tags_json FROM rag_documents WHERE scope = ? AND id = ?"}).bind(dialect), memoryRagMigrationScope, "rag-split").Scan(&tagsJSON); err != nil {
		t.Fatal(err)
	}
	assertJSONTags(t, tagsJSON, []string{"guide", "how-to"})
	if err := db.QueryRowContext(ctx, (sqlQuery{"SELECT tags_json FROM rag_documents WHERE scope = ? AND id = ?"}).bind(dialect), memoryRagMigrationScope, "rag-empty").Scan(&tagsJSON); err != nil {
		t.Fatal(err)
	}
	assertJSONTags(t, tagsJSON, []string{})
}

func assertMemoryScopeKeyIsUnique(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect) {
	t.Helper()
	insert := (sqlQuery{`INSERT INTO memory_entries (scope, id, key, content, tags, created_at)
		VALUES (?, ?, ?, ?, ?, ?)`}).bind(dialect)
	if _, err := db.ExecContext(ctx, insert, memoryRagMigrationScope, "memory-unique-index", "unique-index", "first", "", 1); err != nil {
		t.Fatalf("insert Memory row for unique-index check: %v", err)
	}
	if _, err := db.ExecContext(ctx, insert, memoryRagMigrationScope, "memory-unique-index-conflict", "unique-index", "second", "", 2); err == nil {
		t.Fatal("memory_scope_key accepted a duplicate scope/key")
	}
}

func assertJSONTags(t *testing.T, raw string, want []string) {
	t.Helper()
	var got []string
	if err := json.Unmarshal([]byte(raw), &got); err != nil {
		t.Fatalf("tags_json is not a JSON array: %q: %v", raw, err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("tags_json = %#v, want %#v", got, want)
	}
}
