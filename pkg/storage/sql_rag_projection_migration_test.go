package storage

import (
	"context"
	"database/sql"
	"reflect"
	"sort"
	"strconv"
	"testing"
)

const ragProjectionMigrationScope = "global/product/acme"

type ragProjectionValue struct {
	Scope      string
	DocumentID string
	Value      string
}

func TestSQLRagProjectionSchemaV27SQLite(t *testing.T) {
	t.Run("fresh", func(t *testing.T) {
		testRagProjectionV27Fresh(t, SQLDialectSQLite, newSQLiteRagProjectionTestDB(t))
	})
	t.Run("v26_to_v27", func(t *testing.T) {
		testRagProjectionV27Migration(t, SQLDialectSQLite, newSQLiteRagProjectionTestDB(t))
	})
	t.Run("corrupt_tags_json_rolls_back_and_retries", func(t *testing.T) {
		testRagProjectionV27CorruptTagsRollback(t, SQLDialectSQLite, newSQLiteRagProjectionTestDB(t))
	})
	t.Run("future_version_refused_before_ddl", func(t *testing.T) {
		testRagProjectionV27FutureVersionRefusal(t, SQLDialectSQLite, newSQLiteRagProjectionTestDB(t))
	})
}

func TestPostgresRagProjectionSchemaV27(t *testing.T) {
	t.Run("fresh", func(t *testing.T) {
		testRagProjectionV27Fresh(t, SQLDialectPostgres, newPostgresTestDB(t))
	})
	t.Run("v26_to_v27", func(t *testing.T) {
		testRagProjectionV27Migration(t, SQLDialectPostgres, newPostgresTestDB(t))
	})
	t.Run("corrupt_tags_json_rolls_back_and_retries", func(t *testing.T) {
		testRagProjectionV27CorruptTagsRollback(t, SQLDialectPostgres, newPostgresTestDB(t))
	})
	t.Run("future_version_refused_before_ddl", func(t *testing.T) {
		testRagProjectionV27FutureVersionRefusal(t, SQLDialectPostgres, newPostgresTestDB(t))
	})
}

func newSQLiteRagProjectionTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", t.TempDir()+"/rag-projection.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func testRagProjectionV27Fresh(t *testing.T, dialect SQLDialect, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err != nil {
		t.Fatalf("open fresh v27 schema: %v", err)
	}
	requireRagProjectionV27Schema(t, ctx, db, dialect)
	assertRagProjectionKeysHaveNoForeignKey(t, ctx, db, dialect)
	assertRagProjectionSchemaVersion(t, ctx, db, dialect, SQLSchemaVersion)
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err != nil {
		t.Fatalf("repeat fresh v27 open: %v", err)
	}
}

func testRagProjectionV27Migration(t *testing.T, dialect SQLDialect, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	seedV26RagProjectionSchema(t, ctx, db, dialect)
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err != nil {
		t.Fatalf("v26 to v27 migration: %v", err)
	}
	requireRagProjectionV27Schema(t, ctx, db, dialect)
	assertRagProjectionData(t, ctx, db, dialect)
	assertRagProjectionCanonicalDocuments(t, ctx, db, dialect)
	assertRagProjectionSchemaVersion(t, ctx, db, dialect, SQLSchemaVersion)
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err != nil {
		t.Fatalf("repeat v27 open: %v", err)
	}
	assertRagProjectionData(t, ctx, db, dialect)
	assertRagProjectionCanonicalDocuments(t, ctx, db, dialect)
}

func testRagProjectionV27CorruptTagsRollback(t *testing.T, dialect SQLDialect, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	seedV26RagProjectionSchema(t, ctx, db, dialect)
	updateTags := (sqlQuery{`UPDATE rag_documents SET tags_json = ?
		WHERE scope = ? AND id = ?`}).bind(dialect)
	if _, err := db.ExecContext(ctx, updateTags, `["other",null]`, ragProjectionMigrationScope, "document-mixed"); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err == nil {
		t.Fatal("corrupt rag tags_json was accepted")
	}
	assertRagProjectionSchemaVersion(t, ctx, db, dialect, 26)
	for _, table := range []string{"rag_document_tokens", "rag_document_tags"} {
		exists, err := sqlTableExists(ctx, db, dialect, table)
		if err != nil || exists {
			t.Fatalf("failed v27 migration partially committed %s: exists=%t err=%v", table, exists, err)
		}
	}
	if _, err := db.ExecContext(ctx, updateTags, `["other","标签,组"]`, ragProjectionMigrationScope, "document-mixed"); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err != nil {
		t.Fatalf("retry v27 migration after repairing tags_json: %v", err)
	}
	requireRagProjectionV27Schema(t, ctx, db, dialect)
	assertRagProjectionData(t, ctx, db, dialect)
	assertRagProjectionSchemaVersion(t, ctx, db, dialect, SQLSchemaVersion)
}

func testRagProjectionV27FutureVersionRefusal(t *testing.T, dialect SQLDialect, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE TABLE store_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, sqlInsertMetaRow.bind(dialect), strconv.Itoa(SQLSchemaVersion+1)); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err == nil {
		t.Fatal("future v27 schema version was accepted")
	}
	for _, table := range []string{"rag_document_tokens", "rag_document_tags"} {
		exists, err := sqlTableExists(ctx, db, dialect, table)
		if err != nil || exists {
			t.Fatalf("future-version refusal ran v27 DDL for %s: exists=%t err=%v", table, exists, err)
		}
	}
}

func seedV26RagProjectionSchema(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect) {
	t.Helper()
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"rag_document_tokens", "rag_document_tags"} {
		if _, err := db.ExecContext(ctx, "DROP TABLE "+table); err != nil {
			t.Fatalf("remove current %s projection: %v", table, err)
		}
	}
	if _, err := db.ExecContext(ctx, sqlUpdateMetaRow.bind(dialect), "26"); err != nil {
		t.Fatal(err)
	}
	insert := (sqlQuery{`INSERT INTO rag_documents (scope, id, source, content, tags, tags_json)
		VALUES (?, ?, ?, ?, ?, ?)`}).bind(dialect)
	for _, document := range []struct {
		id, content, tagsJSON string
	}{
		{
			id:       "document-alpha",
			content:  "Alpha, alpha! BETA; beta? ... C++",
			tagsJSON: `["Team,Infra","标签,组","repeat","repeat"]`,
		},
		{
			id:       "document-mixed",
			content:  "ALPHA: GAMMA.",
			tagsJSON: `["other","标签,组"]`,
		},
		{
			id:       "document-empty",
			content:  "!? ...",
			tagsJSON: `[]`,
		},
	} {
		if _, err := db.ExecContext(ctx, insert, ragProjectionMigrationScope, document.id, "projection-test", document.content, "legacy-shadow", document.tagsJSON); err != nil {
			t.Fatal(err)
		}
	}
}

func requireRagProjectionV27Schema(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect) {
	t.Helper()
	for _, table := range []string{"rag_document_tokens", "rag_document_tags"} {
		exists, err := sqlTableExists(ctx, db, dialect, table)
		if err != nil || !exists {
			t.Fatalf("v27 %s missing: exists=%t err=%v", table, exists, err)
		}
	}
	for _, index := range []string{"rag_tokens_lookup", "rag_tags_lookup"} {
		var count int
		var err error
		if dialect == SQLDialectPostgres {
			err = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pg_indexes
				WHERE schemaname = current_schema() AND indexname = $1`, index).Scan(&count)
		} else {
			err = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master
				WHERE type = 'index' AND name = ?`, index).Scan(&count)
		}
		if err != nil || count != 1 {
			t.Fatalf("v27 %s missing: count=%d err=%v", index, count, err)
		}
	}
}

func assertRagProjectionKeysHaveNoForeignKey(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect) {
	t.Helper()
	for _, projection := range []struct {
		table, valueColumn, value string
	}{
		{table: "rag_document_tokens", valueColumn: "token", value: "orphan-token"},
		{table: "rag_document_tags", valueColumn: "tag", value: "orphan-tag"},
	} {
		insert := (sqlQuery{"INSERT INTO " + projection.table + " (scope, document_id, " + projection.valueColumn + ") VALUES (?, ?, ?)"}).bind(dialect)
		if _, err := db.ExecContext(ctx, insert, "orphan-scope", "orphan-document", projection.value); err != nil {
			t.Fatalf("v27 %s unexpectedly has a foreign-key dependency: %v", projection.table, err)
		}
		if _, err := db.ExecContext(ctx, insert, "orphan-scope", "orphan-document", projection.value); err == nil {
			t.Fatalf("v27 %s primary key accepted duplicate row", projection.table)
		}
	}
}

func assertRagProjectionData(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect) {
	t.Helper()
	assertRagProjectionRows(t, ctx, db, dialect, "rag_document_tokens", "token", []ragProjectionValue{
		{ragProjectionMigrationScope, "document-alpha", "alpha"},
		{ragProjectionMigrationScope, "document-alpha", "beta"},
		{ragProjectionMigrationScope, "document-alpha", "c++"},
		{ragProjectionMigrationScope, "document-mixed", "alpha"},
		{ragProjectionMigrationScope, "document-mixed", "gamma"},
	})
	assertRagProjectionRows(t, ctx, db, dialect, "rag_document_tags", "tag", []ragProjectionValue{
		{ragProjectionMigrationScope, "document-alpha", "Team,Infra"},
		{ragProjectionMigrationScope, "document-alpha", "repeat"},
		{ragProjectionMigrationScope, "document-alpha", "标签,组"},
		{ragProjectionMigrationScope, "document-mixed", "other"},
		{ragProjectionMigrationScope, "document-mixed", "标签,组"},
	})
}

func assertRagProjectionRows(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect, table, valueColumn string, want []ragProjectionValue) {
	t.Helper()
	query := (sqlQuery{"SELECT scope, document_id, " + valueColumn + " FROM " + table + " ORDER BY scope, document_id, " + valueColumn}).bind(dialect)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := []ragProjectionValue{}
	for rows.Next() {
		var value ragProjectionValue
		if err := rows.Scan(&value.Scope, &value.DocumentID, &value.Value); err != nil {
			t.Fatal(err)
		}
		got = append(got, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	sort.Slice(got, func(i, j int) bool {
		if got[i].Scope != got[j].Scope {
			return got[i].Scope < got[j].Scope
		}
		if got[i].DocumentID != got[j].DocumentID {
			return got[i].DocumentID < got[j].DocumentID
		}
		return got[i].Value < got[j].Value
	})
	sort.Slice(want, func(i, j int) bool {
		if want[i].Scope != want[j].Scope {
			return want[i].Scope < want[j].Scope
		}
		if want[i].DocumentID != want[j].DocumentID {
			return want[i].DocumentID < want[j].DocumentID
		}
		return want[i].Value < want[j].Value
	})
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s rows = %#v, want %#v", table, got, want)
	}
}

func assertRagProjectionCanonicalDocuments(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect) {
	t.Helper()
	query := (sqlQuery{`SELECT id, content, tags_json FROM rag_documents
		WHERE scope = ? ORDER BY id`}).bind(dialect)
	rows, err := db.QueryContext(ctx, query, ragProjectionMigrationScope)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := [][3]string{}
	for rows.Next() {
		var id, content, tagsJSON string
		if err := rows.Scan(&id, &content, &tagsJSON); err != nil {
			t.Fatal(err)
		}
		got = append(got, [3]string{id, content, tagsJSON})
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want := [][3]string{
		{"document-alpha", "Alpha, alpha! BETA; beta? ... C++", `["Team,Infra","标签,组","repeat","repeat"]`},
		{"document-empty", "!? ...", `[]`},
		{"document-mixed", "ALPHA: GAMMA.", `["other","标签,组"]`},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("canonical rag documents changed: got %#v want %#v", got, want)
	}
}

func assertRagProjectionSchemaVersion(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect, want int) {
	t.Helper()
	var got string
	if err := db.QueryRowContext(ctx, sqlSelectMetaRow.bind(dialect)).Scan(&got); err != nil || got != strconv.Itoa(want) {
		t.Fatalf("schema version = %q, want %d: %v", got, want, err)
	}
}
