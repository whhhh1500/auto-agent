package storage

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"testing"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

func TestSQLRagIndexV27ProjectsUniqueSortedTokensAndTags(t *testing.T) {
	db := newSQLMemoryRagV26DB(t)
	index, err := NewSQLRagIndex(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, scope := testScopes()
	ctx := context.Background()

	if err := index.Ingest(ctx, scope, core.RagDocument{
		ID: "projection", Content: "Bravo! alpha alpha, Zulu;",
		Tags: []string{"team,infra", "标签", "team,infra", "alpha"},
	}); err != nil {
		t.Fatal(err)
	}
	if got := ragProjectionValues(t, db, "rag_document_tokens", "token", scope.String(), "projection"); !reflect.DeepEqual(got, []string{"alpha", "bravo", "zulu"}) {
		t.Fatalf("token projection = %#v", got)
	}
	if got := ragProjectionValues(t, db, "rag_document_tags", "tag", scope.String(), "projection"); !reflect.DeepEqual(got, []string{"alpha", "team,infra", "标签"}) {
		t.Fatalf("tag projection = %#v", got)
	}

	if err := index.Ingest(ctx, scope, core.RagDocument{
		ID: "projection", Content: "gamma gamma!", Tags: []string{"new,tag", "新"},
	}); err != nil {
		t.Fatal(err)
	}
	if got := ragProjectionValues(t, db, "rag_document_tokens", "token", scope.String(), "projection"); !reflect.DeepEqual(got, []string{"gamma"}) {
		t.Fatalf("replacement retained old tokens: %#v", got)
	}
	if got := ragProjectionValues(t, db, "rag_document_tags", "tag", scope.String(), "projection"); !reflect.DeepEqual(got, []string{"new,tag", "新"}) {
		t.Fatalf("replacement retained old tags: %#v", got)
	}
}

func TestSQLRagIndexV27ProjectionFailureRollsBackCanonicalAndProjections(t *testing.T) {
	db := newSQLMemoryRagV26DB(t)
	index, err := NewSQLRagIndex(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, scope := testScopes()
	ctx := context.Background()
	if err := index.Ingest(ctx, scope, core.RagDocument{
		ID: "atomic", Source: "old.md", Content: "old token", Tags: []string{"old"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TRIGGER fail_rag_token_projection
		BEFORE INSERT ON rag_document_tokens WHEN NEW.token = 'explode'
		BEGIN SELECT RAISE(ABORT, 'token projection insert failed'); END`); err != nil {
		t.Fatal(err)
	}
	if err := index.Ingest(ctx, scope, core.RagDocument{
		ID: "atomic", Source: "new.md", Content: "explode replacement", Tags: []string{"new"},
	}); err == nil {
		t.Fatal("projection insert failure was accepted")
	}

	var source, content, tagsJSON string
	if err := db.QueryRow(`SELECT source, content, tags_json FROM rag_documents WHERE scope = ? AND id = ?`, scope.String(), "atomic").Scan(&source, &content, &tagsJSON); err != nil {
		t.Fatal(err)
	}
	if source != "old.md" || content != "old token" || tagsJSON != `["old"]` {
		t.Fatalf("canonical document was not rolled back: source=%q content=%q tags=%q", source, content, tagsJSON)
	}
	if got := ragProjectionValues(t, db, "rag_document_tokens", "token", scope.String(), "atomic"); !reflect.DeepEqual(got, []string{"old", "token"}) {
		t.Fatalf("token projection was not rolled back: %#v", got)
	}
	if got := ragProjectionValues(t, db, "rag_document_tags", "tag", scope.String(), "atomic"); !reflect.DeepEqual(got, []string{"old"}) {
		t.Fatalf("tag projection was not rolled back: %#v", got)
	}
}

func TestSQLRagIndexV27SearchUsesExactProjectionTokensAndTags(t *testing.T) {
	db := newSQLMemoryRagV26DB(t)
	index, err := NewSQLRagIndex(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, scope := testScopes()
	ctx := context.Background()
	if err := index.Ingest(ctx, scope, core.RagDocument{
		ID: "exact", Content: "alphabet alpha", Tags: []string{"team,infra", "标签"},
	}); err != nil {
		t.Fatal(err)
	}
	if hits, err := index.Search(ctx, scope, core.RagQuery{Query: "alp"}); err != nil || len(hits) != 0 {
		t.Fatalf("substring token matched: %#v err=%v", hits, err)
	}
	for _, tags := range [][]string{{"team,infra"}, {"标签"}, {"team,infra", "team,infra"}} {
		hits, err := index.Search(ctx, scope, core.RagQuery{Query: "alpha alpha", Tags: tags})
		if err != nil || len(hits) != 1 || hits[0].ID != "exact" || hits[0].Score != 1 {
			t.Fatalf("exact projection tag/query tokens %q = %#v err=%v", tags, hits, err)
		}
	}
	for _, tags := range [][]string{{"team"}, {" team,infra"}} {
		if hits, err := index.Search(ctx, scope, core.RagQuery{Query: "alpha", Tags: tags}); err != nil || len(hits) != 0 {
			t.Fatalf("non-exact/untrimmed tag %q matched: %#v err=%v", tags, hits, err)
		}
	}
	if _, err := db.Exec(`UPDATE rag_documents SET tags_json = '{broken' WHERE scope = ? AND id = ?`, scope.String(), "exact"); err != nil {
		t.Fatal(err)
	}
	if hits, err := index.Search(ctx, scope, core.RagQuery{Query: "alpha", Tags: []string{"标签"}}); err != nil || len(hits) != 1 || hits[0].ID != "exact" {
		t.Fatalf("search read corrupt canonical tags_json: %#v err=%v", hits, err)
	}
	if hits, err := index.Search(ctx, core.ScopePath{}, core.RagQuery{Query: "alpha"}); err == nil || !strings.Contains(err.Error(), "rag scope is empty") {
		t.Fatalf("empty scope must be rejected: %#v err=%v", hits, err)
	}
}

func TestSQLRagIndexV27SearchScopeOrderAndSQLLimit(t *testing.T) {
	db := newSQLMemoryRagV26DB(t)
	index, err := NewSQLRagIndex(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	global, product, tenant, user := testScopes()
	sibling, err := product.Child(core.ScopeRef{Kind: core.ScopeTenant, ID: "tenant-b"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, row := range []struct {
		scope core.ScopePath
		id    string
	}{
		{global, "z-root"},
		{tenant, "b-target"},
		{tenant, "a-target"},
		{sibling, "sibling"},
	} {
		if err := index.Ingest(ctx, row.scope, core.RagDocument{ID: row.id, Content: "tie"}); err != nil {
			t.Fatal(err)
		}
	}
	hits, err := index.Search(ctx, user, core.RagQuery{Query: "tie", TopK: 10})
	if err != nil {
		t.Fatal(err)
	}
	if got := ragChunkIDs(hits); !reflect.DeepEqual(got, []string{"z-root", "a-target", "b-target"}) {
		t.Fatalf("scope hierarchy/id tie order = %#v", got)
	}
	limited, err := index.Search(ctx, user, core.RagQuery{Query: "tie", TopK: 1})
	if err != nil || !reflect.DeepEqual(ragChunkIDs(limited), []string{"z-root"}) {
		t.Fatalf("SQL TopK limit = %#v err=%v", limited, err)
	}
	if hits, err := index.Search(ctx, user, core.RagQuery{Query: "tie", TopK: 0}); err != nil || !reflect.DeepEqual(ragChunkIDs(hits), []string{"z-root", "a-target", "b-target"}) {
		t.Fatalf("SQL default TopK = %#v err=%v", hits, err)
	}
}

func TestSQLRagIndexV27SearchBuildsParameterizedSQL(t *testing.T) {
	_, _, _, scope := testScopes()
	raw := `value'); DROP TABLE rag_documents; --`
	for _, dialect := range []SQLDialect{SQLDialectSQLite, SQLDialectPostgres} {
		statement, args := buildSQLRagSearch(dialect, []core.ScopePath{scope}, []string{raw}, []string{raw}, 1)
		if strings.Contains(statement, raw) {
			t.Fatalf("%v search statement contains raw query input: %q", dialect, statement)
		}
		if len(args) != 5 || args[2] != raw || args[3] != raw || args[4] != 1 {
			t.Fatalf("%v bound arguments = %#v", dialect, args)
		}
		placeholder := "?"
		if dialect == SQLDialectPostgres {
			placeholder = "$1"
		}
		if !strings.Contains(statement, placeholder) {
			t.Fatalf("%v search statement did not bind placeholders: %q", dialect, statement)
		}
	}
}

func ragProjectionValues(t *testing.T, db *sql.DB, table, column, scope, documentID string) []string {
	t.Helper()
	rows, err := db.Query(`SELECT `+column+` FROM `+table+` WHERE scope = ? AND document_id = ? ORDER BY `+column, scope, documentID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	values := []string{}
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			t.Fatal(err)
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return values
}

func ragChunkIDs(chunks []core.RagChunk) []string {
	ids := make([]string, len(chunks))
	for i := range chunks {
		ids[i] = chunks[i].ID
	}
	return ids
}
