package storage

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

var _ RagProjectionMaintainer = (*SQLRagIndex)(nil)

type ragProjectionCanonicalRow struct {
	Scope    string
	ID       string
	Source   string
	Content  string
	TagsJSON string
}

func TestSQLRagIndexProjectionStatsAndRebuildSQLite(t *testing.T) {
	db := newSQLMemoryRagV26DB(t)
	index, err := NewSQLRagIndex(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	global, product, tenant, _ := testScopes()
	documents := []struct {
		scope    core.ScopePath
		document core.RagDocument
	}{
		{global, core.RagDocument{ID: "one", Source: "one.md", Content: "alpha alpha bravo", Tags: []string{"team", "team"}}},
		{product, core.RagDocument{ID: "two", Source: "two.md", Content: "charlie", Tags: []string{}}},
		{tenant, core.RagDocument{ID: "one", Source: "three.md", Content: "delta", Tags: []string{"ops"}}},
	}
	for _, entry := range documents {
		if err := index.Ingest(ctx, entry.scope, entry.document); err != nil {
			t.Fatal(err)
		}
	}
	canonicalBefore := readRagProjectionCanonicalRows(t, ctx, db, SQLDialectSQLite)
	if _, err := db.ExecContext(ctx, `DELETE FROM rag_document_tokens WHERE scope = ? AND document_id = ? AND token = ?`, global.String(), "one", "bravo"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO rag_document_tokens (scope, document_id, token) VALUES (?, ?, ?)`, "orphan/scope", "orphan", "lost"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO rag_document_tokens (scope, document_id, token) VALUES (?, ?, ?)`, global.String(), "one", "wrong-token"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO rag_document_tags (scope, document_id, tag) VALUES (?, ?, ?)`, "orphan/scope", "orphan", "lost"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO rag_document_tags (scope, document_id, tag) VALUES (?, ?, ?)`, global.String(), "one", "wrong-tag"); err != nil {
		t.Fatal(err)
	}
	if got := ragProjectionValues(t, db, "rag_document_tokens", "token", global.String(), "one"); !reflect.DeepEqual(got, []string{"alpha", "wrong-token"}) {
		t.Fatalf("corrupt token projection = %#v", got)
	}
	if got := ragProjectionValues(t, db, "rag_document_tags", "tag", global.String(), "one"); !reflect.DeepEqual(got, []string{"team", "wrong-tag"}) {
		t.Fatalf("corrupt tag projection = %#v", got)
	}

	stats, err := index.ProjectionStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	wantDamaged := RagProjectionStats{
		CanonicalDocuments: 3, TokenRows: 5, TagRows: 4,
		TokenDocuments: 4, TagDocuments: 3, OrphanTokenRows: 1, OrphanTagRows: 1,
	}
	if !reflect.DeepEqual(stats, wantDamaged) {
		t.Fatalf("damaged projection stats = %#v, want %#v", stats, wantDamaged)
	}

	rebuilt, err := index.RebuildProjection(ctx)
	if err != nil {
		t.Fatal(err)
	}
	wantRebuilt := RagProjectionStats{
		CanonicalDocuments: 3, TokenRows: 4, TagRows: 2,
		TokenDocuments: 3, TagDocuments: 2,
	}
	if !reflect.DeepEqual(rebuilt, wantRebuilt) {
		t.Fatalf("rebuild stats = %#v, want %#v", rebuilt, wantRebuilt)
	}
	if stats, err := index.ProjectionStats(ctx); err != nil || !reflect.DeepEqual(stats, wantRebuilt) {
		t.Fatalf("post-rebuild stats = %#v err=%v, want %#v", stats, err, wantRebuilt)
	}
	if got := readRagProjectionCanonicalRows(t, ctx, db, SQLDialectSQLite); !reflect.DeepEqual(got, canonicalBefore) {
		t.Fatalf("rebuild changed canonical documents: got %#v want %#v", got, canonicalBefore)
	}
	assertRagProjectionRows(t, ctx, db, SQLDialectSQLite, "rag_document_tokens", "token", []ragProjectionValue{
		{global.String(), "one", "alpha"}, {global.String(), "one", "bravo"},
		{product.String(), "two", "charlie"}, {tenant.String(), "one", "delta"},
	})
	assertRagProjectionRows(t, ctx, db, SQLDialectSQLite, "rag_document_tags", "tag", []ragProjectionValue{
		{global.String(), "one", "team"}, {tenant.String(), "one", "ops"},
	})
}

func TestSQLRagIndexRebuildProjectionInsertFailureRollsBackSQLite(t *testing.T) {
	db := newSQLMemoryRagV26DB(t)
	index, err := NewSQLRagIndex(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	global, _, _, _ := testScopes()
	if err := index.Ingest(ctx, global, core.RagDocument{ID: "atomic", Content: "old token", Tags: []string{"old"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE rag_documents SET content = ?, tags_json = ? WHERE scope = ? AND id = ?`, "explode replacement", `["new"]`, global.String(), "atomic"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TRIGGER fail_rag_projection_rebuild
		BEFORE INSERT ON rag_document_tokens WHEN NEW.token = 'explode'
		BEGIN SELECT RAISE(ABORT, 'projection rebuild insert failed'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := index.RebuildProjection(ctx); err == nil {
		t.Fatal("rebuild accepted a projection insert failure")
	}
	if got := ragProjectionValues(t, db, "rag_document_tokens", "token", global.String(), "atomic"); !reflect.DeepEqual(got, []string{"old", "token"}) {
		t.Fatalf("failed rebuild changed token projection: %#v", got)
	}
	if got := ragProjectionValues(t, db, "rag_document_tags", "tag", global.String(), "atomic"); !reflect.DeepEqual(got, []string{"old"}) {
		t.Fatalf("failed rebuild changed tag projection: %#v", got)
	}
}

func TestSQLRagIndexRebuildProjectionCorruptCanonicalTagsRollsBackSQLite(t *testing.T) {
	db := newSQLMemoryRagV26DB(t)
	index, err := NewSQLRagIndex(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	global, _, _, _ := testScopes()
	if err := index.Ingest(ctx, global, core.RagDocument{ID: "corrupt", Content: "old token", Tags: []string{"old"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE rag_documents SET tags_json = ? WHERE scope = ? AND id = ?`, `["new",null]`, global.String(), "corrupt"); err != nil {
		t.Fatal(err)
	}
	if _, err := index.RebuildProjection(ctx); err == nil {
		t.Fatal("rebuild accepted corrupt canonical tags_json")
	}
	if got := ragProjectionValues(t, db, "rag_document_tokens", "token", global.String(), "corrupt"); !reflect.DeepEqual(got, []string{"old", "token"}) {
		t.Fatalf("corrupt-tag rebuild changed token projection: %#v", got)
	}
	if got := ragProjectionValues(t, db, "rag_document_tags", "tag", global.String(), "corrupt"); !reflect.DeepEqual(got, []string{"old"}) {
		t.Fatalf("corrupt-tag rebuild changed tag projection: %#v", got)
	}
}

func TestSQLRagIndexRebuildProjectionKeysetBatchesSQLite(t *testing.T) {
	db := newSQLMemoryRagV26DB(t)
	index, err := NewSQLRagIndex(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	global, product, _, _ := testScopes()
	insert := `INSERT INTO rag_documents (scope, id, source, content, tags, tags_json)
		VALUES (?, ?, ?, ?, ?, ?)`
	for _, scope := range []struct {
		path   core.ScopePath
		prefix string
	}{
		{global, "g"},
		{product, "p"},
	} {
		for number := 0; number < 17; number++ {
			id := fmt.Sprintf("%02d", number)
			token := fmt.Sprintf("%s%s", scope.prefix, id)
			if _, err := db.ExecContext(ctx, insert, scope.path.String(), id, "batch.md", token, scope.prefix, fmt.Sprintf(`["%s"]`, scope.prefix)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := db.ExecContext(ctx, "DELETE FROM rag_document_tokens"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "DELETE FROM rag_document_tags"); err != nil {
		t.Fatal(err)
	}

	stats, err := index.RebuildProjection(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := RagProjectionStats{CanonicalDocuments: 34, TokenRows: 34, TagRows: 34, TokenDocuments: 34, TagDocuments: 34}
	if !reflect.DeepEqual(stats, want) {
		t.Fatalf("keyset rebuild stats = %#v, want %#v", stats, want)
	}
	for _, check := range []struct {
		scope, id, token string
	}{
		{global.String(), "15", "g15"}, {global.String(), "16", "g16"},
		{product.String(), "00", "p00"}, {product.String(), "15", "p15"}, {product.String(), "16", "p16"},
	} {
		if got := ragProjectionValues(t, db, "rag_document_tokens", "token", check.scope, check.id); !reflect.DeepEqual(got, []string{check.token}) {
			t.Fatalf("keyset boundary %s/%s = %#v, want %#v", check.scope, check.id, got, []string{check.token})
		}
	}
}

func readRagProjectionCanonicalRows(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect) []ragProjectionCanonicalRow {
	t.Helper()
	rows, err := db.QueryContext(ctx, (sqlQuery{`SELECT scope, id, source, content, tags_json
		FROM rag_documents ORDER BY scope, id`}).bind(dialect))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var documents []ragProjectionCanonicalRow
	for rows.Next() {
		var document ragProjectionCanonicalRow
		if err := rows.Scan(&document.Scope, &document.ID, &document.Source, &document.Content, &document.TagsJSON); err != nil {
			t.Fatal(err)
		}
		documents = append(documents, document)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return documents
}

func TestSQLRagIndexRebuildProjectionRejectsEmptyScopeControlAndNUL(t *testing.T) {
	ctx := context.Background()
	global, _, _, _ := testScopes()
	for _, tc := range []struct {
		name  string
		setup func(*sql.DB)
		want  string
	}{
		{
			name: "empty scope",
			setup: func(db *sql.DB) {
				if _, err := db.ExecContext(ctx, `INSERT INTO rag_documents
					(scope, id, source, content, tags, tags_json)
					VALUES ('', 'empty', '', 'content', '', '[]')`); err != nil {
					t.Fatal(err)
				}
			},
			want: "rag scope is empty",
		},
		{
			name: "control id",
			setup: func(db *sql.DB) {
				if _, err := db.ExecContext(ctx, `INSERT INTO rag_documents
					(scope, id, source, content, tags, tags_json)
					VALUES (?, ?, '', 'content', '', '[]')`, global.String(), "doc\n"); err != nil {
					t.Fatal(err)
				}
			},
			want: "control character",
		},
		{
			name: "control source",
			setup: func(db *sql.DB) {
				if _, err := db.ExecContext(ctx, `INSERT INTO rag_documents
					(scope, id, source, content, tags, tags_json)
					VALUES (?, 'src', ?, 'content', '', '[]')`, global.String(), "src\t"); err != nil {
					t.Fatal(err)
				}
			},
			want: "control character",
		},
		{
			name: "content NUL",
			setup: func(db *sql.DB) {
				if _, err := db.ExecContext(ctx, `INSERT INTO rag_documents
					(scope, id, source, content, tags, tags_json)
					VALUES (?, 'nul', '', ?, '', '[]')`, global.String(), "con\x00tent"); err != nil {
					t.Fatal(err)
				}
			},
			want: "NUL",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := newSQLMemoryRagV26DB(t)
			index, err := NewSQLRagIndex(db, SQLDialectSQLite)
			if err != nil {
				t.Fatal(err)
			}
			if err := index.Ingest(ctx, global, core.RagDocument{ID: "keep", Content: "keep token", Tags: []string{"old"}}); err != nil {
				t.Fatal(err)
			}
			keepBefore := readRagProjectionCanonicalRows(t, ctx, db, SQLDialectSQLite)
			tc.setup(db)
			if _, err := index.RebuildProjection(ctx); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("rebuild error=%v want %q", err, tc.want)
			}
			got := readRagProjectionCanonicalRows(t, ctx, db, SQLDialectSQLite)
			foundKeep := false
			for _, row := range got {
				if row.Scope == global.String() && row.ID == "keep" {
					if !reflect.DeepEqual(row, keepBefore[0]) {
						t.Fatalf("failed rebuild changed keep row: got %#v want %#v", row, keepBefore[0])
					}
					foundKeep = true
				}
			}
			if !foundKeep {
				t.Fatal("failed rebuild dropped the valid canonical document")
			}
			if got := ragProjectionValues(t, db, "rag_document_tokens", "token", global.String(), "keep"); !reflect.DeepEqual(got, []string{"keep", "token"}) {
				t.Fatalf("failed rebuild changed token projection: %#v", got)
			}
			if got := ragProjectionValues(t, db, "rag_document_tags", "tag", global.String(), "keep"); !reflect.DeepEqual(got, []string{"old"}) {
				t.Fatalf("failed rebuild changed tag projection: %#v", got)
			}
		})
	}
}
