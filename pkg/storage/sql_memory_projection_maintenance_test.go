package storage

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

var _ MemoryProjectionMaintainer = (*SQLMemoryStore)(nil)

type memoryProjectionCanonicalRow struct {
	Scope    string
	ID       string
	Key      string
	Content  string
	Tags     string
	TagsJSON string
	Created  int64
}

type memoryProjectionTagRow struct {
	Scope string
	Key   string
	Tag   string
}

func TestSQLMemoryStoreProjectionStatsAndRebuildSQLite(t *testing.T) {
	db := newSQLMemoryRagV26DB(t)
	store, err := NewSQLMemoryStore(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	global, product, tenant, _ := testScopes()
	for _, entry := range []struct {
		scope core.ScopePath
		entry core.MemoryEntry
	}{
		{global, core.MemoryEntry{ID: "one", Key: "ÄPFEL", Content: "MÜNCHEN", Tags: []string{"team", "team", ""}}},
		{product, core.MemoryEntry{ID: "two", Key: "BRAVO", Content: "CHARLIE", Tags: []string{}}},
		{tenant, core.MemoryEntry{ID: "one", Key: "DELTA", Content: "ECHO", Tags: []string{"ops"}}},
	} {
		if _, err := store.Remember(ctx, entry.scope, entry.entry); err != nil {
			t.Fatal(err)
		}
	}
	canonicalBefore := readMemoryProjectionCanonicalRows(t, ctx, db, SQLDialectSQLite)

	if _, err := db.ExecContext(ctx, `UPDATE memory_entries
		SET key_search = '', content_search = ''
		WHERE scope = ? AND key = ?`, global.String(), "ÄPFEL"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE memory_entries
		SET key_search = 'wrong-key', content_search = 'wrong-content'
		WHERE scope = ? AND key = ?`, product.String(), "BRAVO"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM memory_entry_tags
		WHERE scope = ? AND key = ? AND tag = ?`, global.String(), "ÄPFEL", "team"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO memory_entry_tags (scope, key, tag) VALUES (?, ?, ?)`, global.String(), "ÄPFEL", "wrong-tag"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO memory_entry_tags (scope, key, tag) VALUES (?, ?, ?)`, "orphan/scope", "orphan", "lost"); err != nil {
		t.Fatal(err)
	}

	stats, err := store.ProjectionStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	wantDamaged := MemoryProjectionStats{
		CanonicalEntries: 3, TagRows: 4, TaggedKeys: 3, OrphanTagRows: 1,
		KeySearchMissing: 1, ContentSearchMissing: 1, SearchProjectedEntries: 2,
	}
	if !reflect.DeepEqual(stats, wantDamaged) {
		t.Fatalf("damaged projection stats = %#v, want %#v", stats, wantDamaged)
	}

	rebuilt, err := store.RebuildProjection(ctx)
	if err != nil {
		t.Fatal(err)
	}
	wantRebuilt := MemoryProjectionStats{CanonicalEntries: 3, TagRows: 3, TaggedKeys: 2, SearchProjectedEntries: 3}
	if !reflect.DeepEqual(rebuilt, wantRebuilt) {
		t.Fatalf("rebuild stats = %#v, want %#v", rebuilt, wantRebuilt)
	}
	if stats, err := store.ProjectionStats(ctx); err != nil || !reflect.DeepEqual(stats, wantRebuilt) {
		t.Fatalf("post-rebuild stats = %#v err=%v, want %#v", stats, err, wantRebuilt)
	}
	if got := readMemoryProjectionCanonicalRows(t, ctx, db, SQLDialectSQLite); !reflect.DeepEqual(got, canonicalBefore) {
		t.Fatalf("rebuild changed canonical memory entries: got %#v want %#v", got, canonicalBefore)
	}
	assertMemoryProjectionSearch(t, ctx, db, SQLDialectSQLite, global.String(), "ÄPFEL", "äpfel", "münchen")
	assertMemoryProjectionSearch(t, ctx, db, SQLDialectSQLite, product.String(), "BRAVO", "bravo", "charlie")
	assertMemoryProjectionTags(t, ctx, db, SQLDialectSQLite, []memoryProjectionTagRow{
		{global.String(), "ÄPFEL", ""},
		{global.String(), "ÄPFEL", "team"},
		{tenant.String(), "DELTA", "ops"},
	})
}

func TestSQLMemoryStoreRebuildProjectionInsertFailureRollsBackSQLite(t *testing.T) {
	db := newSQLMemoryRagV26DB(t)
	store, err := NewSQLMemoryStore(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	global, _, _, _ := testScopes()
	if _, err := store.Remember(ctx, global, core.MemoryEntry{ID: "atomic", Key: "atomic", Content: "Old Content", Tags: []string{"old"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE memory_entries SET content = ?, tags = ?, tags_json = ?
		WHERE scope = ? AND key = ?`, "Replacement Content", "explode", `["explode"]`, global.String(), "atomic"); err != nil {
		t.Fatal(err)
	}
	canonicalBefore := readMemoryProjectionCanonicalRows(t, ctx, db, SQLDialectSQLite)
	if _, err := db.ExecContext(ctx, `CREATE TRIGGER fail_memory_projection_rebuild
		BEFORE INSERT ON memory_entry_tags WHEN NEW.tag = 'explode'
		BEGIN SELECT RAISE(ABORT, 'memory projection rebuild insert failed'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RebuildProjection(ctx); err == nil {
		t.Fatal("rebuild accepted a projection insert failure")
	}
	if got := readMemoryProjectionCanonicalRows(t, ctx, db, SQLDialectSQLite); !reflect.DeepEqual(got, canonicalBefore) {
		t.Fatalf("failed rebuild changed canonical memory entries: got %#v want %#v", got, canonicalBefore)
	}
	assertMemoryProjectionSearch(t, ctx, db, SQLDialectSQLite, global.String(), "atomic", "atomic", "old content")
	assertMemoryProjectionTags(t, ctx, db, SQLDialectSQLite, []memoryProjectionTagRow{{global.String(), "atomic", "old"}})
}

func TestSQLMemoryStoreRebuildProjectionCorruptTagsRollsBackSQLite(t *testing.T) {
	db := newSQLMemoryRagV26DB(t)
	store, err := NewSQLMemoryStore(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	global, _, _, _ := testScopes()
	if _, err := store.Remember(ctx, global, core.MemoryEntry{ID: "corrupt", Key: "corrupt", Content: "Old Content", Tags: []string{"old"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE memory_entries SET tags = ?, tags_json = ?
		WHERE scope = ? AND key = ?`, "new", `["new",null]`, global.String(), "corrupt"); err != nil {
		t.Fatal(err)
	}
	canonicalBefore := readMemoryProjectionCanonicalRows(t, ctx, db, SQLDialectSQLite)
	if _, err := store.RebuildProjection(ctx); err == nil {
		t.Fatal("rebuild accepted corrupt canonical tags_json")
	}
	if got := readMemoryProjectionCanonicalRows(t, ctx, db, SQLDialectSQLite); !reflect.DeepEqual(got, canonicalBefore) {
		t.Fatalf("corrupt-tag rebuild changed canonical memory entries: got %#v want %#v", got, canonicalBefore)
	}
	assertMemoryProjectionSearch(t, ctx, db, SQLDialectSQLite, global.String(), "corrupt", "corrupt", "old content")
	assertMemoryProjectionTags(t, ctx, db, SQLDialectSQLite, []memoryProjectionTagRow{{global.String(), "corrupt", "old"}})
}

func TestSQLMemoryStoreRebuildProjectionKeysetBatchesSQLite(t *testing.T) {
	db := newSQLMemoryRagV26DB(t)
	store, err := NewSQLMemoryStore(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
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
	if _, err := db.ExecContext(ctx, `UPDATE memory_entries SET key_search = '', content_search = ''`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM memory_entry_tags`); err != nil {
		t.Fatal(err)
	}

	stats, err := store.RebuildProjection(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := MemoryProjectionStats{CanonicalEntries: 34, TagRows: 34, TaggedKeys: 34, SearchProjectedEntries: 34}
	if !reflect.DeepEqual(stats, want) {
		t.Fatalf("keyset rebuild stats = %#v, want %#v", stats, want)
	}
	for _, check := range []struct {
		scope, key string
	}{
		{global.String(), "g15"}, {global.String(), "g16"},
		{product.String(), "p00"}, {product.String(), "p15"}, {product.String(), "p16"},
	} {
		assertMemoryProjectionSearch(t, ctx, db, SQLDialectSQLite, check.scope, check.key, check.key, check.key)
		assertMemoryProjectionTagsForKey(t, ctx, db, SQLDialectSQLite, check.scope, check.key, []string{string(check.key[0])})
	}
}

func TestValidateMemoryProjectionStatsRejectsNegative(t *testing.T) {
	for _, check := range []struct {
		name string
		set  func(*MemoryProjectionStats)
	}{
		{"canonical_entries", func(stats *MemoryProjectionStats) { stats.CanonicalEntries = -1 }},
		{"tag_rows", func(stats *MemoryProjectionStats) { stats.TagRows = -1 }},
		{"tagged_keys", func(stats *MemoryProjectionStats) { stats.TaggedKeys = -1 }},
		{"orphan_tag_rows", func(stats *MemoryProjectionStats) { stats.OrphanTagRows = -1 }},
		{"key_search_missing", func(stats *MemoryProjectionStats) { stats.KeySearchMissing = -1 }},
		{"content_search_missing", func(stats *MemoryProjectionStats) { stats.ContentSearchMissing = -1 }},
		{"search_projected_entries", func(stats *MemoryProjectionStats) { stats.SearchProjectedEntries = -1 }},
	} {
		t.Run(check.name, func(t *testing.T) {
			var stats MemoryProjectionStats
			check.set(&stats)
			if err := validateMemoryProjectionStats(stats); err == nil || !strings.Contains(err.Error(), check.name) {
				t.Fatalf("validate negative %s = %v", check.name, err)
			}
		})
	}
}

func readMemoryProjectionCanonicalRows(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect) []memoryProjectionCanonicalRow {
	t.Helper()
	rows, err := db.QueryContext(ctx, (sqlQuery{`SELECT scope, id, key, content, tags, tags_json, created_at
		FROM memory_entries ORDER BY scope, id`}).bind(dialect))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var entries []memoryProjectionCanonicalRow
	for rows.Next() {
		var entry memoryProjectionCanonicalRow
		if err := rows.Scan(&entry.Scope, &entry.ID, &entry.Key, &entry.Content, &entry.Tags, &entry.TagsJSON, &entry.Created); err != nil {
			t.Fatal(err)
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return entries
}

func assertMemoryProjectionSearch(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect, scope, key, wantKey, wantContent string) {
	t.Helper()
	var keySearch, contentSearch string
	if err := db.QueryRowContext(ctx, (sqlQuery{`SELECT key_search, content_search
		FROM memory_entries WHERE scope = ? AND key = ?`}).bind(dialect), scope, key).Scan(&keySearch, &contentSearch); err != nil {
		t.Fatal(err)
	}
	if keySearch != wantKey || contentSearch != wantContent {
		t.Fatalf("search projection for %s/%q = key=%q content=%q, want key=%q content=%q", scope, key, keySearch, contentSearch, wantKey, wantContent)
	}
}

func assertMemoryProjectionTags(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect, want []memoryProjectionTagRow) {
	t.Helper()
	rows, err := db.QueryContext(ctx, (sqlQuery{`SELECT scope, key, tag
		FROM memory_entry_tags ORDER BY scope, key, tag`}).bind(dialect))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := []memoryProjectionTagRow{}
	for rows.Next() {
		var row memoryProjectionTagRow
		if err := rows.Scan(&row.Scope, &row.Key, &row.Tag); err != nil {
			t.Fatal(err)
		}
		got = append(got, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	sort.Slice(want, func(i, j int) bool {
		if want[i].Scope != want[j].Scope {
			return want[i].Scope < want[j].Scope
		}
		if want[i].Key != want[j].Key {
			return want[i].Key < want[j].Key
		}
		return want[i].Tag < want[j].Tag
	})
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("memory tag projection = %#v, want %#v", got, want)
	}
}

func assertMemoryProjectionTagsForKey(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect, scope, key string, want []string) {
	t.Helper()
	rows, err := db.QueryContext(ctx, (sqlQuery{`SELECT tag FROM memory_entry_tags
		WHERE scope = ? AND key = ? ORDER BY tag`}).bind(dialect), scope, key)
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
	sortedWant := append([]string(nil), want...)
	sort.Strings(sortedWant)
	if !reflect.DeepEqual(got, sortedWant) {
		t.Fatalf("memory tags for %s/%q = %#v, want %#v", scope, key, got, sortedWant)
	}
}

func TestSQLMemoryStoreRebuildProjectionRejectsEmptyScopeControlAndNUL(t *testing.T) {
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
				if _, err := db.ExecContext(ctx, `INSERT INTO memory_entries
					(scope, id, key, content, tags, tags_json, created_at, key_search, content_search)
					VALUES ('', 'empty', 'key', 'content', '', '[]', 1, 'key', 'content')`); err != nil {
					t.Fatal(err)
				}
			},
			want: "memory scope is empty",
		},
		{
			name: "control id",
			setup: func(db *sql.DB) {
				if _, err := db.ExecContext(ctx, `INSERT INTO memory_entries
					(scope, id, key, content, tags, tags_json, created_at, key_search, content_search)
					VALUES (?, ?, 'key', 'content', '', '[]', 1, 'key', 'content')`, global.String(), "mem\n1"); err != nil {
					t.Fatal(err)
				}
			},
			want: "control character",
		},
		{
			name: "content NUL",
			setup: func(db *sql.DB) {
				if _, err := db.ExecContext(ctx, `INSERT INTO memory_entries
					(scope, id, key, content, tags, tags_json, created_at, key_search, content_search)
					VALUES (?, 'nul', 'key', ?, '', '[]', 1, 'key', 'content')`, global.String(), "con\x00tent"); err != nil {
					t.Fatal(err)
				}
			},
			want: "NUL",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := newSQLMemoryRagV26DB(t)
			store, err := NewSQLMemoryStore(db, SQLDialectSQLite)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.Remember(ctx, global, core.MemoryEntry{ID: "keep", Key: "keep", Content: "keep", Tags: []string{"old"}}); err != nil {
				t.Fatal(err)
			}
			keepBefore := readMemoryProjectionCanonicalRows(t, ctx, db, SQLDialectSQLite)
			tc.setup(db)
			if _, err := store.RebuildProjection(ctx); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("rebuild error=%v want %q", err, tc.want)
			}
			got := readMemoryProjectionCanonicalRows(t, ctx, db, SQLDialectSQLite)
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
				t.Fatal("failed rebuild dropped the valid canonical row")
			}
			assertMemoryProjectionSearch(t, ctx, db, SQLDialectSQLite, global.String(), "keep", "keep", "keep")
			assertMemoryProjectionTags(t, ctx, db, SQLDialectSQLite, []memoryProjectionTagRow{{global.String(), "keep", "old"}})
		})
	}
}
