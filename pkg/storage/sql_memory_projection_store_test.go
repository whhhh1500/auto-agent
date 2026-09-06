package storage

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"testing"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

func TestSQLMemoryStoreV28ProjectsSearchAndTagsOnReplacement(t *testing.T) {
	db := newSQLMemoryRagV26DB(t)
	store, err := NewSQLMemoryStore(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, scope := testScopes()
	ctx := context.Background()

	first, err := store.Remember(ctx, scope, core.MemoryEntry{
		ID: "mem_projection_old", Key: "Watch ÅNGSTRÖM", Content: "Obsolete NeedLe",
		Tags: []string{"team,infra", "标签", "b", "a", "a", ""},
	})
	if err != nil {
		t.Fatal(err)
	}
	var keySearch, contentSearch string
	if err := db.QueryRow(`SELECT key_search, content_search FROM memory_entries WHERE scope = ? AND key = ?`, scope.String(), first.Key).Scan(&keySearch, &contentSearch); err != nil {
		t.Fatal(err)
	}
	if keySearch != strings.ToLower(first.Key) || contentSearch != strings.ToLower(first.Content) {
		t.Fatalf("memory search projections = key=%q content=%q", keySearch, contentSearch)
	}
	if got := memoryProjectionValues(t, db, scope.String(), first.Key); !reflect.DeepEqual(got, []string{"", "a", "b", "team,infra", "标签"}) {
		t.Fatalf("memory tag projection = %#v", got)
	}

	for _, tags := range [][]string{{"team,infra"}, {"标签"}, {"missing", "标签", "标签"}, {"", "team,infra"}} {
		hits, err := store.Recall(ctx, scope, "  ångs  ", tags, 0)
		if err != nil || len(hits) != 1 || hits[0].ID != first.ID {
			t.Fatalf("exact Unicode/ANY tag recall %q = %#v err=%v", tags, hits, err)
		}
	}
	if hits, err := store.Recall(ctx, scope, "ångs", []string{""}, 0); err != nil || len(hits) != 1 || hits[0].ID != first.ID {
		t.Fatalf("empty exact tag recall = %#v err=%v", hits, err)
	}
	for _, tags := range [][]string{{"team"}, {" team,infra"}} {
		hits, err := store.Recall(ctx, scope, "  ångs  ", tags, 0)
		if err != nil || len(hits) != 0 {
			t.Fatalf("non-exact tag recall %q = %#v err=%v", tags, hits, err)
		}
	}

	replacement, err := store.Remember(ctx, scope, core.MemoryEntry{
		ID: "mem_projection_new", Key: first.Key, Content: "Replacement Content", Tags: []string{"new,tag", "替换", "new,tag"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := memoryProjectionValues(t, db, scope.String(), first.Key); !reflect.DeepEqual(got, []string{"new,tag", "替换"}) {
		t.Fatalf("replacement retained stale memory tags: %#v", got)
	}
	if err := db.QueryRow(`SELECT key_search, content_search FROM memory_entries WHERE scope = ? AND key = ?`, scope.String(), first.Key).Scan(&keySearch, &contentSearch); err != nil {
		t.Fatal(err)
	}
	if keySearch != strings.ToLower(first.Key) || contentSearch != strings.ToLower(replacement.Content) {
		t.Fatalf("replacement search projections = key=%q content=%q", keySearch, contentSearch)
	}
	if hits, err := store.Recall(ctx, scope, "obsolete", nil, 0); err != nil || len(hits) != 0 {
		t.Fatalf("replacement retained old content search: %#v err=%v", hits, err)
	}
	if hits, err := store.Recall(ctx, scope, "replacement", []string{"new,tag"}, 0); err != nil || len(hits) != 1 || hits[0].ID != replacement.ID {
		t.Fatalf("replacement search/tag recall = %#v err=%v", hits, err)
	}
	if hits, err := store.Recall(ctx, scope, "  ångs  ", []string{"team,infra"}, 0); err != nil || len(hits) != 0 {
		t.Fatalf("replacement retained old tag recall = %#v err=%v", hits, err)
	}
}

func TestSQLMemoryStoreV28ProjectionFailureRollsBackCanonicalAndTags(t *testing.T) {
	db := newSQLMemoryRagV26DB(t)
	store, err := NewSQLMemoryStore(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, scope := testScopes()
	ctx := context.Background()
	if _, err := store.Remember(ctx, scope, core.MemoryEntry{
		ID: "mem_atomic_old", Key: "atomic", Content: "Old Content", Tags: []string{"old"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TRIGGER fail_memory_entry_tag_projection
		BEFORE INSERT ON memory_entry_tags
		WHEN NEW.tag = 'explode'
		BEGIN
			SELECT RAISE(ABORT, 'memory tag projection insert failed');
		END`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Remember(ctx, scope, core.MemoryEntry{
		ID: "mem_atomic_new", Key: "atomic", Content: "New Content", Tags: []string{"explode"},
	}); err == nil {
		t.Fatal("projection failure was accepted")
	}

	var id, content, tagsJSON, keySearch, contentSearch string
	if err := db.QueryRow(`SELECT id, content, tags_json, key_search, content_search
		FROM memory_entries WHERE scope = ? AND key = ?`, scope.String(), "atomic").Scan(&id, &content, &tagsJSON, &keySearch, &contentSearch); err != nil {
		t.Fatal(err)
	}
	if id != "mem_atomic_old" || content != "Old Content" || tagsJSON != `["old"]` || keySearch != "atomic" || contentSearch != "old content" {
		t.Fatalf("canonical memory rollback = id=%q content=%q tags=%q key_search=%q content_search=%q", id, content, tagsJSON, keySearch, contentSearch)
	}
	if got := memoryProjectionValues(t, db, scope.String(), "atomic"); !reflect.DeepEqual(got, []string{"old"}) {
		t.Fatalf("tag projection rollback = %#v", got)
	}
}

func TestSQLMemoryStoreV28ForgetDeletesTagProjection(t *testing.T) {
	db := newSQLMemoryRagV26DB(t)
	store, err := NewSQLMemoryStore(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, scope := testScopes()
	ctx := context.Background()
	entry, err := store.Remember(ctx, scope, core.MemoryEntry{
		ID: "mem_forget_projection", Key: "forget-projection", Content: "remove projection", Tags: []string{"one", "two"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Forget(ctx, scope, entry.ID); err != nil {
		t.Fatal(err)
	}
	if got := memoryProjectionValues(t, db, scope.String(), entry.Key); len(got) != 0 {
		t.Fatalf("forgotten Memory entry left tag projection rows: %#v", got)
	}
}

func TestSQLMemoryStoreV28RecallFiltersBeforeDecodingAndOrdersInSQL(t *testing.T) {
	db := newSQLMemoryRagV26DB(t)
	store, err := NewSQLMemoryStore(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, scope := testScopes()
	ctx := context.Background()
	if _, err := store.Remember(ctx, scope, core.MemoryEntry{
		ID: "matching", Key: "matching", Content: "find this needle", Tags: []string{"matched"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Remember(ctx, scope, core.MemoryEntry{
		ID: "unrelated", Key: "unrelated", Content: "other content", Tags: []string{"other"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE memory_entries SET tags_json = '{broken' WHERE scope = ? AND id = ?`, scope.String(), "unrelated"); err != nil {
		t.Fatal(err)
	}
	if hits, err := store.Recall(ctx, scope, "needle", []string{"matched"}, 0); err != nil || len(hits) != 1 || hits[0].ID != "matching" {
		t.Fatalf("nonmatching corrupt tags_json affected recall: %#v err=%v", hits, err)
	}
	if _, err := db.Exec(`UPDATE memory_entries SET tags_json = '{broken' WHERE scope = ? AND id = ?`, scope.String(), "matching"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Recall(ctx, scope, "needle", []string{"matched"}, 0); err == nil {
		t.Fatal("matching corrupt tags_json did not fail closed")
	}

	orderScope, err := scope.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "ordering"})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct {
		id        string
		createdAt int64
	}{
		{id: "b", createdAt: 200},
		{id: "a", createdAt: 200},
		{id: "c", createdAt: 100},
	} {
		if _, err := store.Remember(ctx, orderScope, core.MemoryEntry{ID: row.id, Key: row.id, Content: "ordered"}); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`UPDATE memory_entries SET created_at = ? WHERE scope = ? AND id = ?`, row.createdAt, orderScope.String(), row.id); err != nil {
			t.Fatal(err)
		}
	}
	if hits, err := store.Recall(ctx, orderScope, "ordered", nil, 0); err != nil || !reflect.DeepEqual(memoryEntryIDs(hits), []string{"a", "b", "c"}) {
		t.Fatalf("unlimited/order recall = %#v err=%v", hits, err)
	}
	if hits, err := store.Recall(ctx, orderScope, "ordered", nil, 2); err != nil || !reflect.DeepEqual(memoryEntryIDs(hits), []string{"a", "b"}) {
		t.Fatalf("SQL limit/order recall = %#v err=%v", hits, err)
	}
}

func TestSQLMemoryStoreV28RecallBuildsParameterizedSQL(t *testing.T) {
	_, _, _, scope := testScopes()
	rawQuery := `needle'); DROP TABLE memory_entries; --`
	rawTag := `tag'); DROP TABLE memory_entry_tags; --`
	for _, dialect := range []SQLDialect{SQLDialectSQLite, SQLDialectPostgres} {
		statement, args := buildSQLMemoryRecall(dialect, scope.String(), rawQuery, []string{rawTag}, 1)
		if strings.Contains(statement, rawQuery) || strings.Contains(statement, rawTag) {
			t.Fatalf("%v recall statement contains raw input: %q", dialect, statement)
		}
		if !strings.Contains(statement, "memory_entry_tags") || !strings.Contains(statement, "ORDER BY created_at DESC, id ASC") || !strings.Contains(statement, "LIMIT") {
			t.Fatalf("%v recall projection query = %q", dialect, statement)
		}
		if !reflect.DeepEqual(args, []any{scope.String(), rawQuery, rawQuery, rawTag, 1}) {
			t.Fatalf("%v recall arguments = %#v", dialect, args)
		}
		placeholder := "?"
		searchFunction := "instr"
		if dialect == SQLDialectPostgres {
			placeholder = "$1"
			searchFunction = "strpos"
		}
		if !strings.Contains(statement, placeholder) || !strings.Contains(statement, searchFunction) {
			t.Fatalf("%v recall placeholders/search function = %q", dialect, statement)
		}

		unlimited, unlimitedArgs := buildSQLMemoryRecall(dialect, scope.String(), "", []string{"", "b", "a", "a"}, 0)
		if strings.Contains(unlimited, "LIMIT") || !reflect.DeepEqual(unlimitedArgs, []any{scope.String(), "", "a", "b"}) {
			t.Fatalf("%v unlimited/unique exact tags = statement=%q args=%#v", dialect, unlimited, unlimitedArgs)
		}
	}
}

func memoryProjectionValues(t *testing.T, db *sql.DB, scope, key string) []string {
	t.Helper()
	rows, err := db.Query(`SELECT tag FROM memory_entry_tags WHERE scope = ? AND key = ? ORDER BY tag`, scope, key)
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

func memoryEntryIDs(entries []core.MemoryEntry) []string {
	ids := make([]string, len(entries))
	for i := range entries {
		ids[i] = entries[i].ID
	}
	return ids
}
