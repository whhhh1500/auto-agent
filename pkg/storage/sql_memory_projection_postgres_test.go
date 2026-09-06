package storage

import (
	"context"
	"database/sql"
	"reflect"
	"testing"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

func TestPostgresSQLMemoryStoreV28ProjectionSearchAndAtomicity(t *testing.T) {
	db := newPostgresTestDB(t)
	ctx := context.Background()
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres); err != nil {
		t.Fatal(err)
	}
	installPostgresMemoryProjectionTables(t, db)
	store, err := NewSQLMemoryStore(db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, scope := testScopes()

	first, err := store.Remember(ctx, scope, core.MemoryEntry{
		ID: "mem_pg_projection_old", Key: "Watch ÅNGSTRÖM", Content: "Old Content", Tags: []string{"", "team,infra", "标签", "old", "old"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := postgresMemoryProjectionValues(t, db, scope.String(), first.Key); !reflect.DeepEqual(got, []string{"", "old", "team,infra", "标签"}) {
		t.Fatalf("postgres memory tag projection = %#v", got)
	}
	if hits, err := store.Recall(ctx, scope, "ångs", []string{"missing", "team,infra"}, 0); err != nil || len(hits) != 1 || hits[0].ID != first.ID {
		t.Fatalf("postgres projection Unicode/tag recall = %#v err=%v", hits, err)
	}
	if hits, err := store.Recall(ctx, scope, "ångs", []string{""}, 0); err != nil || len(hits) != 1 || hits[0].ID != first.ID {
		t.Fatalf("postgres empty exact tag recall = %#v err=%v", hits, err)
	}

	replacement, err := store.Remember(ctx, scope, core.MemoryEntry{
		ID: "mem_pg_projection_new", Key: first.Key, Content: "Replacement Content", Tags: []string{"new,tag", "替换", "new,tag"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := postgresMemoryProjectionValues(t, db, scope.String(), first.Key); !reflect.DeepEqual(got, []string{"new,tag", "替换"}) {
		t.Fatalf("postgres replacement retained stale tags: %#v", got)
	}
	if hits, err := store.Recall(ctx, scope, "old content", nil, 0); err != nil || len(hits) != 0 {
		t.Fatalf("postgres replacement retained old search: %#v err=%v", hits, err)
	}
	if hits, err := store.Recall(ctx, scope, "replacement", []string{"new,tag"}, 0); err != nil || len(hits) != 1 || hits[0].ID != replacement.ID {
		t.Fatalf("postgres replacement search/tag = %#v err=%v", hits, err)
	}

	if _, err := store.Remember(ctx, scope, core.MemoryEntry{
		ID: "mem_pg_atomic_old", Key: "atomic", Content: "Old Atomic", Tags: []string{"old"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `CREATE FUNCTION fail_memory_tag_projection() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW.tag = 'explode' THEN
				RAISE EXCEPTION 'memory tag projection insert failed';
			END IF;
			RETURN NEW;
		END;
		$$;
		CREATE TRIGGER fail_memory_tag_projection BEFORE INSERT ON memory_entry_tags
		FOR EACH ROW EXECUTE FUNCTION fail_memory_tag_projection()`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Remember(ctx, scope, core.MemoryEntry{
		ID: "mem_pg_atomic_new", Key: "atomic", Content: "New Atomic", Tags: []string{"explode"},
	}); err == nil {
		t.Fatal("postgres projection failure was accepted")
	}

	var id, content, tagsJSON, keySearch, contentSearch string
	if err := db.QueryRowContext(ctx, `SELECT id, content, tags_json, key_search, content_search
		FROM memory_entries WHERE scope = $1 AND key = $2`, scope.String(), "atomic").Scan(&id, &content, &tagsJSON, &keySearch, &contentSearch); err != nil {
		t.Fatal(err)
	}
	if id != "mem_pg_atomic_old" || content != "Old Atomic" || tagsJSON != `["old"]` || keySearch != "atomic" || contentSearch != "old atomic" {
		t.Fatalf("postgres canonical rollback = id=%q content=%q tags=%q key_search=%q content_search=%q", id, content, tagsJSON, keySearch, contentSearch)
	}
	if got := postgresMemoryProjectionValues(t, db, scope.String(), "atomic"); !reflect.DeepEqual(got, []string{"old"}) {
		t.Fatalf("postgres projection rollback = %#v", got)
	}
}

func installPostgresMemoryProjectionTables(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	for _, column := range []string{"key_search", "content_search"} {
		exists, err := sqlColumnExists(ctx, db, SQLDialectPostgres, "memory_entries", column)
		if err != nil {
			t.Fatalf("inspect test-only postgres v28 Memory projection column %q: %v", column, err)
		}
		if exists {
			continue
		}
		if _, err := db.Exec("ALTER TABLE memory_entries ADD COLUMN " + column + " TEXT NOT NULL DEFAULT ''"); err != nil {
			t.Fatalf("add test-only postgres v28 Memory projection column %q: %v", column, err)
		}
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS memory_entry_tags (
		scope TEXT NOT NULL,
		key TEXT NOT NULL,
		tag TEXT NOT NULL,
		PRIMARY KEY (scope, key, tag)
	)`); err != nil {
		t.Fatalf("create test-only postgres v28 Memory projection: %v", err)
	}
}

func postgresMemoryProjectionValues(t *testing.T, db *sql.DB, scope, key string) []string {
	t.Helper()
	rows, err := db.Query(`SELECT tag FROM memory_entry_tags WHERE scope = $1 AND key = $2 ORDER BY tag`, scope, key)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	values := []string{}
	for rows.Next() {
		var tag string
		if err := rows.Scan(&tag); err != nil {
			t.Fatal(err)
		}
		values = append(values, tag)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return values
}
