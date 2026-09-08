package storage

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	core "github.com/whhhh1500/auto-agent/pkg/core"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

func TestPostgresSQLRagIndexProjectionMaintenance(t *testing.T) {
	db := newPostgresTestDB(t)
	ctx := context.Background()
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres); err != nil {
		t.Fatal(err)
	}
	index, err := NewSQLRagIndex(db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	global, product, _, _ := testScopes()
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
			if err := index.Ingest(ctx, scope.path, core.RagDocument{ID: id, Source: "batch.md", Content: token, Tags: []string{scope.prefix}}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM rag_document_tokens WHERE scope = $1 AND document_id = $2 AND token = $3`, global.String(), "15", "g15"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO rag_document_tokens (scope, document_id, token) VALUES ($1, $2, $3)`, "orphan/scope", "orphan", "lost"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO rag_document_tags (scope, document_id, tag) VALUES ($1, $2, $3)`, "orphan/scope", "orphan", "lost"); err != nil {
		t.Fatal(err)
	}
	installPostgresRagProjectionLockAssertion(t, db)

	stats, err := index.ProjectionStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	wantDamaged := RagProjectionStats{
		CanonicalDocuments: 34, TokenRows: 34, TagRows: 35,
		TokenDocuments: 34, TagDocuments: 35, OrphanTokenRows: 1, OrphanTagRows: 1,
	}
	if !reflect.DeepEqual(stats, wantDamaged) {
		t.Fatalf("postgres damaged projection stats = %#v, want %#v", stats, wantDamaged)
	}

	rebuilt, err := index.RebuildProjection(ctx)
	if err != nil {
		t.Fatal(err)
	}
	wantRebuilt := RagProjectionStats{CanonicalDocuments: 34, TokenRows: 34, TagRows: 34, TokenDocuments: 34, TagDocuments: 34}
	if !reflect.DeepEqual(rebuilt, wantRebuilt) {
		t.Fatalf("postgres rebuild stats = %#v, want %#v", rebuilt, wantRebuilt)
	}
	for _, check := range []struct {
		scope, id, token string
	}{
		{global.String(), "15", "g15"}, {global.String(), "16", "g16"},
		{product.String(), "00", "p00"}, {product.String(), "15", "p15"}, {product.String(), "16", "p16"},
	} {
		if got := postgresRagProjectionValues(t, db, "rag_document_tokens", "token", check.scope, check.id); !reflect.DeepEqual(got, []string{check.token}) {
			t.Fatalf("postgres keyset boundary %s/%s = %#v, want %#v", check.scope, check.id, got, []string{check.token})
		}
	}
	if _, err := db.ExecContext(ctx, `UPDATE rag_documents SET content = $1, tags_json = $2 WHERE scope = $3 AND id = $4`, "explode replacement", `["new"]`, global.String(), "00"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `CREATE FUNCTION fail_rag_projection_rebuild() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW.token = 'explode' THEN
				RAISE EXCEPTION 'projection rebuild insert failed';
			END IF;
			RETURN NEW;
		END;
		$$;
		CREATE TRIGGER fail_rag_projection_rebuild BEFORE INSERT ON rag_document_tokens
		FOR EACH ROW EXECUTE FUNCTION fail_rag_projection_rebuild()`); err != nil {
		t.Fatal(err)
	}
	if _, err := index.RebuildProjection(ctx); err == nil {
		t.Fatal("postgres rebuild accepted a projection insert failure")
	}
	if got := postgresRagProjectionValues(t, db, "rag_document_tokens", "token", global.String(), "00"); !reflect.DeepEqual(got, []string{"g00"}) {
		t.Fatalf("postgres failed rebuild changed token projection: %#v", got)
	}
	if got := postgresRagProjectionValues(t, db, "rag_document_tags", "tag", global.String(), "00"); !reflect.DeepEqual(got, []string{"g"}) {
		t.Fatalf("postgres failed rebuild changed tag projection: %#v", got)
	}
}

func TestPostgresSQLRagIndexRebuildProjectionCorruptTagsRollsBack(t *testing.T) {
	db := newPostgresTestDB(t)
	ctx := context.Background()
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres); err != nil {
		t.Fatal(err)
	}
	index, err := NewSQLRagIndex(db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	global, _, _, _ := testScopes()
	if err := index.Ingest(ctx, global, core.RagDocument{ID: "corrupt", Content: "old token", Tags: []string{"old"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE rag_documents SET tags_json = $1 WHERE scope = $2 AND id = $3`, `["new",null]`, global.String(), "corrupt"); err != nil {
		t.Fatal(err)
	}
	if _, err := index.RebuildProjection(ctx); err == nil {
		t.Fatal("postgres rebuild accepted corrupt canonical tags_json")
	}
	if got := postgresRagProjectionValues(t, db, "rag_document_tokens", "token", global.String(), "corrupt"); !reflect.DeepEqual(got, []string{"old", "token"}) {
		t.Fatalf("postgres corrupt-tag rebuild changed token projection: %#v", got)
	}
	if got := postgresRagProjectionValues(t, db, "rag_document_tags", "tag", global.String(), "corrupt"); !reflect.DeepEqual(got, []string{"old"}) {
		t.Fatalf("postgres corrupt-tag rebuild changed tag projection: %#v", got)
	}
}

func installPostgresRagProjectionLockAssertion(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), `CREATE FUNCTION assert_rag_projection_lock() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF NOT EXISTS (
				SELECT 1 FROM pg_locks
				WHERE pid = pg_backend_pid()
					AND relation = 'rag_documents'::regclass
					AND mode = 'ShareRowExclusiveLock'
			) THEN
				RAISE EXCEPTION 'rag_documents ShareRowExclusiveLock is required before projection delete';
			END IF;
			RETURN NULL;
		END;
		$$;
		CREATE TRIGGER assert_rag_projection_lock BEFORE DELETE ON rag_document_tokens
		FOR EACH STATEMENT EXECUTE FUNCTION assert_rag_projection_lock()`); err != nil {
		t.Fatalf("install postgres projection lock assertion: %v", err)
	}
}

func TestPostgresZZZNoHarnessTestSchemaResidue(t *testing.T) {
	dsn := os.Getenv("HARNESS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("HARNESS_TEST_PG_DSN is not configured")
	}
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse postgres cleanup test configuration: %v", err)
	}
	db := stdlib.OpenDB(*config)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.schemata
		WHERE schema_name LIKE 'harness_test_%'`).Scan(&count); err != nil {
		t.Fatalf("count postgres test schema residue: %v", err)
	}
	if count != 0 {
		t.Fatalf("postgres test schema residue = %d, want 0", count)
	}
}
