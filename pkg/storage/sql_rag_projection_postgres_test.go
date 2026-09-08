package storage

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"testing"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

func TestPostgresSQLRagIndexV27ProjectionSearchAndAtomicity(t *testing.T) {
	db := newPostgresTestDB(t)
	ctx := context.Background()
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres); err != nil {
		t.Fatal(err)
	}
	installPostgresRagProjectionTables(t, db)
	index, err := NewSQLRagIndex(db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	global, product, tenant, user := testScopes()
	sibling, err := product.Child(core.ScopeRef{Kind: core.ScopeTenant, ID: "tenant-b"})
	if err != nil {
		t.Fatal(err)
	}

	if err := index.Ingest(ctx, global, core.RagDocument{ID: "z-root", Content: "tie"}); err != nil {
		t.Fatal(err)
	}
	if err := index.Ingest(ctx, tenant, core.RagDocument{
		ID: "exact", Source: "old.md", Content: "alphabet alpha tie", Tags: []string{"team,infra", "标签"},
	}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"b-target", "a-target"} {
		if err := index.Ingest(ctx, tenant, core.RagDocument{ID: id, Content: "tie"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := index.Ingest(ctx, sibling, core.RagDocument{ID: "sibling", Content: "tie"}); err != nil {
		t.Fatal(err)
	}

	if got := postgresRagProjectionValues(t, db, "rag_document_tokens", "token", tenant.String(), "exact"); !reflect.DeepEqual(got, []string{"alpha", "alphabet", "tie"}) {
		t.Fatalf("postgres token projection = %#v", got)
	}
	if got := postgresRagProjectionValues(t, db, "rag_document_tags", "tag", tenant.String(), "exact"); !reflect.DeepEqual(got, []string{"team,infra", "标签"}) {
		t.Fatalf("postgres tag projection = %#v", got)
	}
	if hits, err := index.Search(ctx, user, core.RagQuery{Query: "alp"}); err != nil || len(hits) != 0 {
		t.Fatalf("postgres substring token matched: %#v err=%v", hits, err)
	}
	for _, tags := range [][]string{{"team,infra"}, {"标签"}, {"missing", "team,infra"}} {
		hits, err := index.Search(ctx, user, core.RagQuery{Query: "alpha", Tags: tags})
		if err != nil || len(hits) != 1 || hits[0].ID != "exact" {
			t.Fatalf("postgres exact/ANY tag filter %q = %#v err=%v", tags, hits, err)
		}
	}
	if hits, err := index.Search(ctx, user, core.RagQuery{Query: "alpha", Tags: []string{"team"}}); err != nil || len(hits) != 0 {
		t.Fatalf("postgres comma tag matched by substring: %#v err=%v", hits, err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE rag_documents SET tags_json = '{corrupt' WHERE scope = $1 AND id = $2`, tenant.String(), "exact"); err != nil {
		t.Fatal(err)
	}
	if hits, err := index.Search(ctx, user, core.RagQuery{Query: "alpha", Tags: []string{"标签"}}); err != nil || len(hits) != 1 || hits[0].ID != "exact" {
		t.Fatalf("postgres search read corrupt tags_json: %#v err=%v", hits, err)
	}

	hits, err := index.Search(ctx, user, core.RagQuery{Query: "tie", TopK: 10})
	if err != nil {
		t.Fatal(err)
	}
	if got := ragChunkIDs(hits); !reflect.DeepEqual(got, []string{"z-root", "a-target", "b-target", "exact"}) {
		t.Fatalf("postgres scope hierarchy/id tie order = %#v", got)
	}
	if hits, err := index.Search(ctx, user, core.RagQuery{Query: "tie", TopK: 1}); err != nil || !reflect.DeepEqual(ragChunkIDs(hits), []string{"z-root"}) {
		t.Fatalf("postgres SQL TopK = %#v err=%v", hits, err)
	}
	if hits, err := index.Search(ctx, user, core.RagQuery{Query: "tie", TopK: 0}); err != nil || !reflect.DeepEqual(ragChunkIDs(hits), []string{"z-root", "a-target", "b-target", "exact"}) {
		t.Fatalf("postgres SQL default TopK = %#v err=%v", hits, err)
	}

	if err := index.Ingest(ctx, tenant, core.RagDocument{ID: "exact", Source: "new.md", Content: "omega tie", Tags: []string{"new,tag"}}); err != nil {
		t.Fatal(err)
	}
	if got := postgresRagProjectionValues(t, db, "rag_document_tokens", "token", tenant.String(), "exact"); !reflect.DeepEqual(got, []string{"omega", "tie"}) {
		t.Fatalf("postgres replacement retained old tokens: %#v", got)
	}
	if got := postgresRagProjectionValues(t, db, "rag_document_tags", "tag", tenant.String(), "exact"); !reflect.DeepEqual(got, []string{"new,tag"}) {
		t.Fatalf("postgres replacement retained old tags: %#v", got)
	}
	if hits, err := index.Search(ctx, user, core.RagQuery{Query: "omega", Tags: []string{"missing", "new,tag"}}); err != nil || len(hits) != 1 || hits[0].Source != "new.md" {
		t.Fatalf("postgres replacement search = %#v err=%v", hits, err)
	}

	if err := index.Ingest(ctx, tenant, core.RagDocument{ID: "atomic", Source: "old.md", Content: "old token", Tags: []string{"old"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `CREATE FUNCTION fail_rag_token_projection() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW.token = 'explode' THEN
				RAISE EXCEPTION 'token projection insert failed';
			END IF;
			RETURN NEW;
		END;
		$$;
		CREATE TRIGGER fail_rag_token_projection BEFORE INSERT ON rag_document_tokens
		FOR EACH ROW EXECUTE FUNCTION fail_rag_token_projection()`); err != nil {
		t.Fatal(err)
	}
	if err := index.Ingest(ctx, tenant, core.RagDocument{ID: "atomic", Source: "new.md", Content: "explode replacement", Tags: []string{"new"}}); err == nil {
		t.Fatal("postgres projection trigger failure was accepted")
	}
	var source, content, tagsJSON string
	if err := db.QueryRowContext(ctx, `SELECT source, content, tags_json FROM rag_documents WHERE scope = $1 AND id = $2`, tenant.String(), "atomic").Scan(&source, &content, &tagsJSON); err != nil {
		t.Fatal(err)
	}
	if source != "old.md" || content != "old token" || tagsJSON != `["old"]` {
		t.Fatalf("postgres canonical rollback = source=%q content=%q tags=%q", source, content, tagsJSON)
	}
	if got := postgresRagProjectionValues(t, db, "rag_document_tokens", "token", tenant.String(), "atomic"); !reflect.DeepEqual(got, []string{"old", "token"}) {
		t.Fatalf("postgres token rollback = %#v", got)
	}
	if got := postgresRagProjectionValues(t, db, "rag_document_tags", "tag", tenant.String(), "atomic"); !reflect.DeepEqual(got, []string{"old"}) {
		t.Fatalf("postgres tag rollback = %#v", got)
	}

	raw := `tag'); DROP TABLE rag_documents; --`
	statement, args := buildSQLRagSearch(SQLDialectPostgres, []core.ScopePath{user}, []string{"omega"}, []string{raw}, 1)
	if strings.Contains(statement, raw) || !strings.Contains(statement, "$1") || len(args) != 5 || args[3] != raw {
		t.Fatalf("postgres query is not fully parameterized: statement=%q args=%#v", statement, args)
	}
}

func installPostgresRagProjectionTables(t *testing.T, db *sql.DB) {
	t.Helper()
	for _, statement := range []string{
		`CREATE TABLE IF NOT EXISTS rag_document_tokens (
			scope TEXT NOT NULL,
			document_id TEXT NOT NULL,
			token TEXT NOT NULL,
			PRIMARY KEY (scope, document_id, token)
		)`,
		`CREATE TABLE IF NOT EXISTS rag_document_tags (
			scope TEXT NOT NULL,
			document_id TEXT NOT NULL,
			tag TEXT NOT NULL,
			PRIMARY KEY (scope, document_id, tag)
		)`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("create test-only postgres v27 RAG projection: %v", err)
		}
	}
}

func postgresRagProjectionValues(t *testing.T, db *sql.DB, table, column, scope, documentID string) []string {
	t.Helper()
	rows, err := db.Query(`SELECT `+column+` FROM `+table+` WHERE scope = $1 AND document_id = $2 ORDER BY `+column, scope, documentID)
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
