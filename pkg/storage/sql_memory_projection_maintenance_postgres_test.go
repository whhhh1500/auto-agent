package storage

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"testing"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

func TestPostgresSQLMemoryStoreProjectionMaintenance(t *testing.T) {
	db := newPostgresTestDB(t)
	ctx := context.Background()
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres); err != nil {
		t.Fatal(err)
	}
	store, err := NewSQLMemoryStore(db, SQLDialectPostgres)
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
			value := scope.prefix + id
			if _, err := store.Remember(ctx, scope.path, core.MemoryEntry{ID: id, Key: value, Content: value, Tags: []string{scope.prefix}}); err != nil {
				t.Fatal(err)
			}
		}
	}
	canonicalBefore := readMemoryProjectionCanonicalRows(t, ctx, db, SQLDialectPostgres)
	if _, err := db.ExecContext(ctx, `UPDATE memory_entries SET key_search = $1, content_search = $2
		WHERE scope = $3 AND key = $4`, "", "", global.String(), "g15"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE memory_entries SET key_search = $1, content_search = $2
		WHERE scope = $3 AND key = $4`, "wrong-key", "wrong-content", product.String(), "p00"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM memory_entry_tags WHERE scope = $1 AND key = $2 AND tag = $3`, global.String(), "g15", "g"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO memory_entry_tags (scope, key, tag) VALUES ($1, $2, $3)`, global.String(), "g15", "wrong-tag"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO memory_entry_tags (scope, key, tag) VALUES ($1, $2, $3)`, "orphan/scope", "orphan", "lost"); err != nil {
		t.Fatal(err)
	}
	installPostgresMemoryProjectionRebuildLockAssertion(t, db)

	stats, err := store.ProjectionStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	wantDamaged := MemoryProjectionStats{
		CanonicalEntries: 34, TagRows: 35, TaggedKeys: 35, OrphanTagRows: 1,
		KeySearchMissing: 1, ContentSearchMissing: 1, SearchProjectedEntries: 33,
	}
	if !reflect.DeepEqual(stats, wantDamaged) {
		t.Fatalf("postgres damaged projection stats = %#v, want %#v", stats, wantDamaged)
	}

	rebuilt, err := store.RebuildProjection(ctx)
	if err != nil {
		t.Fatal(err)
	}
	wantRebuilt := MemoryProjectionStats{CanonicalEntries: 34, TagRows: 34, TaggedKeys: 34, SearchProjectedEntries: 34}
	if !reflect.DeepEqual(rebuilt, wantRebuilt) {
		t.Fatalf("postgres rebuild stats = %#v, want %#v", rebuilt, wantRebuilt)
	}
	if got := readMemoryProjectionCanonicalRows(t, ctx, db, SQLDialectPostgres); !reflect.DeepEqual(got, canonicalBefore) {
		t.Fatalf("postgres rebuild changed canonical memory entries: got %#v want %#v", got, canonicalBefore)
	}
	for _, check := range []struct {
		scope, key, tag string
	}{
		{global.String(), "g15", "g"}, {global.String(), "g16", "g"},
		{product.String(), "p00", "p"}, {product.String(), "p15", "p"}, {product.String(), "p16", "p"},
	} {
		assertMemoryProjectionSearch(t, ctx, db, SQLDialectPostgres, check.scope, check.key, check.key, check.key)
		assertMemoryProjectionTagsForKey(t, ctx, db, SQLDialectPostgres, check.scope, check.key, []string{check.tag})
	}

	if _, err := db.ExecContext(ctx, `UPDATE memory_entries SET content = $1, tags = $2, tags_json = $3
		WHERE scope = $4 AND key = $5`, "explode replacement", "explode", `["explode"]`, global.String(), "g00"); err != nil {
		t.Fatal(err)
	}
	canonicalBeforeFailure := readMemoryProjectionCanonicalRows(t, ctx, db, SQLDialectPostgres)
	if _, err := db.ExecContext(ctx, `CREATE FUNCTION fail_memory_projection_rebuild() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW.tag = 'explode' THEN
				RAISE EXCEPTION 'memory projection rebuild insert failed';
			END IF;
			RETURN NEW;
		END;
		$$;
		CREATE TRIGGER fail_memory_projection_rebuild BEFORE INSERT ON memory_entry_tags
		FOR EACH ROW EXECUTE FUNCTION fail_memory_projection_rebuild()`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RebuildProjection(ctx); err == nil {
		t.Fatal("postgres rebuild accepted a projection insert failure")
	}
	if got := readMemoryProjectionCanonicalRows(t, ctx, db, SQLDialectPostgres); !reflect.DeepEqual(got, canonicalBeforeFailure) {
		t.Fatalf("postgres failed rebuild changed canonical memory entries: got %#v want %#v", got, canonicalBeforeFailure)
	}
	assertMemoryProjectionSearch(t, ctx, db, SQLDialectPostgres, global.String(), "g00", "g00", "g00")
	assertMemoryProjectionTagsForKey(t, ctx, db, SQLDialectPostgres, global.String(), "g00", []string{"g"})
}

func TestPostgresSQLMemoryStoreRebuildProjectionCorruptTagsRollsBack(t *testing.T) {
	db := newPostgresTestDB(t)
	ctx := context.Background()
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres); err != nil {
		t.Fatal(err)
	}
	store, err := NewSQLMemoryStore(db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	global, _, _, _ := testScopes()
	if _, err := store.Remember(ctx, global, core.MemoryEntry{ID: "corrupt", Key: "corrupt", Content: "Old Content", Tags: []string{"old"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE memory_entries SET tags = $1, tags_json = $2
		WHERE scope = $3 AND key = $4`, "new", `["new",null]`, global.String(), "corrupt"); err != nil {
		t.Fatal(err)
	}
	canonicalBefore := readMemoryProjectionCanonicalRows(t, ctx, db, SQLDialectPostgres)
	if _, err := store.RebuildProjection(ctx); err == nil {
		t.Fatal("postgres rebuild accepted corrupt canonical tags_json")
	}
	if got := readMemoryProjectionCanonicalRows(t, ctx, db, SQLDialectPostgres); !reflect.DeepEqual(got, canonicalBefore) {
		t.Fatalf("postgres corrupt-tag rebuild changed canonical memory entries: got %#v want %#v", got, canonicalBefore)
	}
	assertMemoryProjectionSearch(t, ctx, db, SQLDialectPostgres, global.String(), "corrupt", "corrupt", "old content")
	assertMemoryProjectionTagsForKey(t, ctx, db, SQLDialectPostgres, global.String(), "corrupt", []string{"old"})
}

func installPostgresMemoryProjectionRebuildLockAssertion(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), `CREATE FUNCTION assert_memory_projection_rebuild_lock() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF NOT EXISTS (
				SELECT 1 FROM pg_locks
				WHERE pid = pg_backend_pid()
					AND relation = 'memory_entries'::regclass
					AND mode = 'ShareRowExclusiveLock'
			) THEN
				RAISE EXCEPTION 'memory_entries ShareRowExclusiveLock is required before runtime projection delete';
			END IF;
			RETURN NULL;
		END;
		$$;
		CREATE TRIGGER assert_memory_projection_rebuild_lock BEFORE DELETE ON memory_entry_tags
		FOR EACH STATEMENT EXECUTE FUNCTION assert_memory_projection_rebuild_lock()`); err != nil {
		t.Fatalf("install postgres memory projection runtime lock assertion: %v", err)
	}
}
