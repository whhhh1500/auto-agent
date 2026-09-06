package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/extensions/memory"
	"github.com/cc-auto-agent/harness-core/pkg/extensions/rag"

	_ "modernc.org/sqlite"
)

// newSQLMemoryRagV26DB opens the regular store's canonical schema. Its legacy
// name remains for the existing v26 tests; the v27/v28 projections are created
// only in this helper, because production requires migration-installed tables.
func newSQLMemoryRagV26DB(t *testing.T) *sql.DB {
	t.Helper()
	db := newSQLBundle(t).db
	installSQLiteMemoryProjectionTables(t, db)
	installSQLiteRagProjectionTables(t, db)
	return db
}

func installSQLiteMemoryProjectionTables(t *testing.T, db *sql.DB) {
	t.Helper()
	for _, column := range []string{"key_search", "content_search"} {
		exists, err := sqlColumnExists(context.Background(), db, SQLDialectSQLite, "memory_entries", column)
		if err != nil {
			t.Fatalf("inspect test-only v28 Memory projection column %q: %v", column, err)
		}
		if exists {
			continue
		}
		if _, err := db.Exec("ALTER TABLE memory_entries ADD COLUMN " + column + " TEXT NOT NULL DEFAULT ''"); err != nil {
			t.Fatalf("add test-only v28 Memory projection column %q: %v", column, err)
		}
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS memory_entry_tags (
			scope TEXT NOT NULL,
			key TEXT NOT NULL,
			tag TEXT NOT NULL,
			PRIMARY KEY (scope, key, tag)
		)`); err != nil {
		t.Fatalf("create test-only v28 Memory projection: %v", err)
	}
}

func installSQLiteRagProjectionTables(t *testing.T, db *sql.DB) {
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
			t.Fatalf("create test-only v27 RAG projection: %v", err)
		}
	}
}

func TestSQLMemoryStoreV26TagsAndReplacement(t *testing.T) {
	db := newSQLMemoryRagV26DB(t)
	store, err := NewSQLMemoryStore(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, scope := testScopes()
	ctx := context.Background()
	tags := []string{"team,infra", "标签", "duplicate", "duplicate"}
	remembered, err := store.Remember(ctx, scope, core.MemoryEntry{
		ID: "mem_old", Key: "tagged", Content: "Solana notes", Tags: tags,
	})
	if err != nil {
		t.Fatal(err)
	}
	tags[0] = "caller-mutation"

	entries, err := store.Recall(ctx, scope, "", nil, 0)
	if err != nil || len(entries) != 1 || !reflect.DeepEqual(entries[0].Tags, []string{"team,infra", "标签", "duplicate", "duplicate"}) {
		t.Fatalf("tag round trip failed: %#v err=%v", entries, err)
	}
	for _, filter := range [][]string{{"team,infra"}, {"标签"}, {"duplicate"}} {
		filtered, err := store.Recall(ctx, scope, "", filter, 0)
		if err != nil || len(filtered) != 1 || filtered[0].ID != remembered.ID {
			t.Fatalf("exact tag filter %q failed: %#v err=%v", filter, filtered, err)
		}
	}
	if filtered, err := store.Recall(ctx, scope, "", []string{"team"}, 0); err != nil || len(filtered) != 0 {
		t.Fatalf("comma-containing tag was split: %#v err=%v", filtered, err)
	}

	replacement, err := store.Remember(ctx, scope, core.MemoryEntry{
		Key: "tagged", Content: "Replacement", Tags: []string{"new,tag", "替换"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if replacement.ID == remembered.ID {
		t.Fatalf("replacement must use its newly generated id: %q", replacement.ID)
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM memory_entries WHERE scope = ? AND key = ?`, scope.String(), "tagged").Scan(&count); err != nil || count != 1 {
		t.Fatalf("replacement must leave exactly one row: count=%d err=%v", count, err)
	}
	var storedID, legacyTags, tagsJSON string
	if err := db.QueryRow(`SELECT id, tags, tags_json FROM memory_entries WHERE scope = ? AND key = ?`, scope.String(), "tagged").Scan(&storedID, &legacyTags, &tagsJSON); err != nil {
		t.Fatal(err)
	}
	if storedID != replacement.ID || legacyTags != "new,tag,替换" || tagsJSON != `["new,tag","替换"]` {
		t.Fatalf("replacement row wrong: id=%q legacy=%q json=%q", storedID, legacyTags, tagsJSON)
	}

	if _, err := db.Exec(`UPDATE memory_entries SET tags_json = '{bad' WHERE scope = ? AND key = ?`, scope.String(), "tagged"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Recall(ctx, scope, "", nil, 0); err == nil {
		t.Fatal("corrupt memory tags_json must fail closed")
	}
}

func TestSQLMemoryStoreV26LimitsAndLimitSemantics(t *testing.T) {
	db := newSQLMemoryRagV26DB(t)
	store, err := NewSQLMemoryStore(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, scope := testScopes()
	ctx := context.Background()

	tooManyTags := make([]string, memory.MaxEntryTags+1)
	for i := range tooManyTags {
		tooManyTags[i] = "duplicate"
	}
	for name, entry := range map[string]core.MemoryEntry{
		"empty key":     {Key: "  ", Content: "content"},
		"empty content": {Key: "key", Content: "\t"},
		"long key":      {Key: strings.Repeat("k", memory.MaxEntryKeyBytes+1), Content: "content"},
		"long content":  {Key: "key", Content: strings.Repeat("c", memory.MaxEntryContentBytes+1)},
		"too many tags": {Key: "key", Content: "content", Tags: tooManyTags},
		"oversized Unicode tag": {Key: "key", Content: "content", Tags: []string{
			strings.Repeat("界", memory.MaxTagRunes+1),
		}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := store.Remember(ctx, scope, entry); err == nil {
				t.Fatal("invalid memory entry was accepted")
			}
		})
	}
	if _, err := store.Remember(ctx, core.ScopePath{}, core.MemoryEntry{Key: "key", Content: "content"}); err == nil {
		t.Fatal("empty memory scope was accepted")
	}
	if _, err := store.Remember(ctx, scope, core.MemoryEntry{Key: "max-tags", Content: "accepted", Tags: make([]string, memory.MaxEntryTags)}); err != nil {
		t.Fatalf("memory entry at the tag limit was rejected: %v", err)
	}
	exactUnicodeTag := strings.Repeat("界", memory.MaxTagRunes)
	if _, err := store.Remember(ctx, scope, core.MemoryEntry{Key: "max-rune-tag", Content: "accepted", Tags: []string{exactUnicodeTag}}); err != nil {
		t.Fatalf("memory entry at the exact Unicode tag rune limit was rejected: %v", err)
	}
	if entries, err := store.Recall(ctx, scope, "accepted", []string{exactUnicodeTag}, 0); err != nil || len(entries) != 1 || entries[0].Key != "max-rune-tag" {
		t.Fatalf("exact Unicode tag recall = %#v err=%v", entries, err)
	}
	if _, err := store.Remember(ctx, scope, core.MemoryEntry{Key: "one", Content: "content"}); err != nil {
		t.Fatal(err)
	}
	var emptyTagsJSON string
	if err := db.QueryRow(`SELECT tags_json FROM memory_entries WHERE scope = ? AND key = ?`, scope.String(), "one").Scan(&emptyTagsJSON); err != nil || emptyTagsJSON != "[]" {
		t.Fatalf("empty Memory tags must use canonical []: tags_json=%q err=%v", emptyTagsJSON, err)
	}
	if _, err := store.Remember(ctx, scope, core.MemoryEntry{Key: "two", Content: "content"}); err != nil {
		t.Fatal(err)
	}
	if entries, err := store.Recall(ctx, scope, "content", nil, 0); err != nil || len(entries) != 2 {
		t.Fatalf("limit 0 must be unlimited: %#v err=%v", entries, err)
	}
	for _, limit := range []int{-1, memory.MaxRecallLimit + 1} {
		if _, err := store.Recall(ctx, scope, "content", nil, limit); err == nil {
			t.Fatalf("invalid recall limit %d was accepted", limit)
		}
	}
	if entries, err := store.Recall(ctx, scope, "content", nil, 1); err != nil || len(entries) != 1 {
		t.Fatalf("positive limit was not applied: %#v err=%v", entries, err)
	}
}

func TestSQLRagIndexV26TagsAndUpsert(t *testing.T) {
	db := newSQLMemoryRagV26DB(t)
	index, err := NewSQLRagIndex(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, scope := testScopes()
	ctx := context.Background()
	tags := []string{"team,infra", "标签", "duplicate", "duplicate"}
	if err := index.Ingest(ctx, scope, core.RagDocument{
		ID: "tagged", Source: "first.md", Content: "Solana tag test", Tags: tags,
	}); err != nil {
		t.Fatal(err)
	}
	tags[0] = "caller-mutation"
	for _, filter := range [][]string{{"team,infra"}, {"标签"}, {"duplicate"}} {
		hits, err := index.Search(ctx, scope, core.RagQuery{Query: "solana", Tags: filter})
		if err != nil || len(hits) != 1 || hits[0].ID != "tagged" {
			t.Fatalf("exact tag filter %q failed: %#v err=%v", filter, hits, err)
		}
	}
	if hits, err := index.Search(ctx, scope, core.RagQuery{Query: "solana", Tags: []string{"team"}}); err != nil || len(hits) != 0 {
		t.Fatalf("comma-containing RAG tag was split: %#v err=%v", hits, err)
	}
	var storedJSON string
	if err := db.QueryRow(`SELECT tags_json FROM rag_documents WHERE scope = ? AND id = ?`, scope.String(), "tagged").Scan(&storedJSON); err != nil {
		t.Fatal(err)
	}
	var storedTags []string
	if err := json.Unmarshal([]byte(storedJSON), &storedTags); err != nil || !reflect.DeepEqual(storedTags, []string{"team,infra", "标签", "duplicate", "duplicate"}) {
		t.Fatalf("RAG tags did not round trip: %#v err=%v", storedTags, err)
	}

	if err := index.Ingest(ctx, scope, core.RagDocument{
		ID: "tagged", Source: "replacement.md", Content: "Solana replacement", Tags: []string{"new,tag", "替换"},
	}); err != nil {
		t.Fatal(err)
	}
	var count int
	var source, legacyTags string
	if err := db.QueryRow(`SELECT COUNT(*), MIN(source), MIN(tags) FROM rag_documents WHERE scope = ? AND id = ?`, scope.String(), "tagged").Scan(&count, &source, &legacyTags); err != nil {
		t.Fatal(err)
	}
	if count != 1 || source != "replacement.md" || legacyTags != "new,tag,替换" {
		t.Fatalf("RAG upsert left the wrong row: count=%d source=%q legacy=%q", count, source, legacyTags)
	}
	if hits, err := index.Search(ctx, scope, core.RagQuery{Query: "solana", Tags: []string{"team,infra"}}); err != nil || len(hits) != 0 {
		t.Fatalf("RAG upsert retained old tags: %#v err=%v", hits, err)
	}
	if hits, err := index.Search(ctx, scope, core.RagQuery{Query: "solana", Tags: []string{"new,tag"}}); err != nil || len(hits) != 1 || hits[0].Source != "replacement.md" {
		t.Fatalf("RAG upsert did not replace document: %#v err=%v", hits, err)
	}

	if _, err := db.Exec(`UPDATE rag_documents SET tags_json = '{bad' WHERE scope = ? AND id = ?`, scope.String(), "tagged"); err != nil {
		t.Fatal(err)
	}
	if hits, err := index.Search(ctx, scope, core.RagQuery{Query: "solana", Tags: []string{"new,tag"}}); err != nil || len(hits) != 1 || hits[0].ID != "tagged" {
		t.Fatalf("projection search must ignore corrupt canonical tags_json: %#v err=%v", hits, err)
	}
}

func TestSQLRagIndexV26LimitsTopKAndDeterminism(t *testing.T) {
	db := newSQLMemoryRagV26DB(t)
	index, err := NewSQLRagIndex(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, scope := testScopes()
	ctx := context.Background()
	tooManyTags := make([]string, rag.MaxDocumentTags+1)
	for i := range tooManyTags {
		tooManyTags[i] = "duplicate"
	}
	for name, document := range map[string]core.RagDocument{
		"empty content":          {ID: "empty", Content: "  "},
		"empty id":               {Content: "content"},
		"long content":           {ID: "long", Content: strings.Repeat("c", rag.MaxDocumentContentBytes+1)},
		"too many tags":          {ID: "tags", Content: "content", Tags: tooManyTags},
		"too many unique tokens": {ID: "tokens", Content: uniqueRagQuery(rag.MaxDocumentTokens + 1)},
		"oversized Unicode tag": {ID: "tag-runes", Content: "content", Tags: []string{
			strings.Repeat("界", rag.MaxTagRunes+1),
		}},
	} {
		t.Run(name, func(t *testing.T) {
			if err := index.Ingest(ctx, scope, document); err == nil {
				t.Fatal("invalid RAG document was accepted")
			}
		})
	}
	if err := index.Ingest(ctx, scope, core.RagDocument{ID: "max-tags", Content: "accepted", Tags: make([]string, rag.MaxDocumentTags)}); err != nil {
		t.Fatalf("document at the tag limit was rejected: %v", err)
	}
	if err := index.Ingest(ctx, scope, core.RagDocument{
		ID: "max-rune-tag", Content: "accepted", Tags: []string{strings.Repeat("界", rag.MaxTagRunes)},
	}); err != nil {
		t.Fatalf("document at the exact Unicode tag rune limit was rejected: %v", err)
	}
	for i := 0; i < rag.MaxSearchTopK+1; i++ {
		if err := index.Ingest(ctx, scope, core.RagDocument{
			ID: fmt.Sprintf("doc-%03d", i), Content: "ranking token",
		}); err != nil {
			t.Fatal(err)
		}
	}
	for _, topK := range []int{-1, rag.MaxSearchTopK + 1} {
		if _, err := index.Search(ctx, scope, core.RagQuery{Query: "ranking", TopK: topK}); err == nil {
			t.Fatalf("invalid TopK %d was accepted", topK)
		}
	}
	first, err := index.Search(ctx, scope, core.RagQuery{Query: "ranking", TopK: rag.MaxSearchTopK})
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != rag.MaxSearchTopK || first[0].ID != "doc-000" || first[len(first)-1].ID != "doc-099" {
		t.Fatalf("maximum TopK or deterministic ranking failed: first=%#v last=%#v len=%d", first[0], first[len(first)-1], len(first))
	}
	second, err := index.Search(ctx, scope, core.RagQuery{Query: "ranking", TopK: rag.MaxSearchTopK})
	if err != nil {
		t.Fatal(err)
	}
	for i := range first {
		if first[i].ID != second[i].ID || first[i].Score != second[i].Score {
			t.Fatalf("ranking was not deterministic at %d: %#v %#v", i, first[i], second[i])
		}
	}
}

func TestSQLMemoryAndRagPublicValidationFailsBeforeSQL(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	memoryStore, err := NewSQLMemoryStore(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	ragIndex, err := NewSQLRagIndex(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, scope := testScopes()
	ctx := context.Background()
	tooManyMemoryTags := make([]string, memory.MaxRecallTags+1)
	tooManyRagTags := make([]string, rag.MaxQueryTags+1)
	for i := range tooManyMemoryTags {
		tooManyMemoryTags[i] = "duplicate"
	}
	for i := range tooManyRagTags {
		tooManyRagTags[i] = "duplicate"
	}

	assertValidationError := func(name string, err error) {
		t.Helper()
		if err == nil {
			t.Fatalf("%s was accepted", name)
		}
		if strings.Contains(err.Error(), "no such table") {
			t.Fatalf("%s reached SQL before validation: %v", name, err)
		}
	}

	_, err = memoryStore.Remember(ctx, scope, core.MemoryEntry{
		Key: "key", Content: "content", Tags: []string{strings.Repeat("界", memory.MaxTagRunes+1)},
	})
	assertValidationError("memory oversized tag", err)
	for name, recall := range map[string]struct {
		query string
		tags  []string
		limit int
	}{
		"query rune limit": {query: strings.Repeat("界", memory.MaxQueryRunes+1)},
		"duplicate tags":   {tags: tooManyMemoryTags},
		"negative limit":   {limit: -1},
		"maximum limit":    {limit: memory.MaxRecallLimit + 1},
	} {
		t.Run("memory "+name, func(t *testing.T) {
			_, err := memoryStore.Recall(ctx, scope, recall.query, recall.tags, recall.limit)
			assertValidationError(name, err)
		})
	}

	assertValidationError("RAG oversized tag", ragIndex.Ingest(ctx, scope, core.RagDocument{
		ID: "tag-runes", Content: "content", Tags: []string{strings.Repeat("界", rag.MaxTagRunes+1)},
	}))
	for name, query := range map[string]core.RagQuery{
		"query rune limit":   {Query: strings.Repeat("界", rag.MaxQueryRunes+1)},
		"duplicate tags":     {Query: "query", Tags: tooManyRagTags},
		"unique token limit": {Query: uniqueRagQuery(rag.MaxQueryTokens + 1)},
		"negative TopK":      {Query: "query", TopK: -1},
		"maximum TopK":       {Query: "query", TopK: rag.MaxSearchTopK + 1},
	} {
		t.Run("RAG "+name, func(t *testing.T) {
			_, err := ragIndex.Search(ctx, scope, query)
			assertValidationError(name, err)
		})
	}

	if _, err := memoryStore.Recall(ctx, scope, "query", nil, 0); err == nil || !strings.Contains(err.Error(), "no such table") {
		t.Fatalf("valid memory recall should reach the missing schema: %v", err)
	}
	if _, err := ragIndex.Search(ctx, scope, core.RagQuery{Query: "query"}); err == nil || !strings.Contains(err.Error(), "no such table") {
		t.Fatalf("valid RAG search should reach the missing schema: %v", err)
	}
}

func uniqueRagQuery(count int) string {
	tokens := make([]string, count)
	for i := range tokens {
		tokens[i] = fmt.Sprintf("token-%d", i)
	}
	return strings.Join(tokens, " ")
}

func TestSQLMemoryAndRagBoundaryValidationFailsBeforeSQL(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	memoryStore, err := NewSQLMemoryStore(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	ragIndex, err := NewSQLRagIndex(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, scope := testScopes()
	ctx := context.Background()
	assertBeforeSQL := func(name string, err error, want string) {
		t.Helper()
		if err == nil {
			t.Fatalf("%s was accepted", name)
		}
		if strings.Contains(err.Error(), "no such table") {
			t.Fatalf("%s reached SQL before validation: %v", name, err)
		}
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("%s error=%v want %q", name, err, want)
		}
	}

	_, err = memoryStore.Remember(ctx, core.ScopePath{}, core.MemoryEntry{Key: "key", Content: "content"})
	assertBeforeSQL("empty memory scope remember", err, "memory scope is empty")
	_, err = memoryStore.Recall(ctx, core.ScopePath{}, "query", nil, 0)
	assertBeforeSQL("empty memory scope recall", err, "memory scope is empty")
	assertBeforeSQL("empty memory scope forget", memoryStore.Forget(ctx, core.ScopePath{}, "mem_1"), "memory scope is empty")
	_, err = memoryStore.Remember(ctx, scope, core.MemoryEntry{ID: "mem\n1", Key: "key", Content: "content"})
	assertBeforeSQL("memory id control", err, "control character")
	_, err = memoryStore.Remember(ctx, scope, core.MemoryEntry{ID: strings.Repeat("a", memory.MaxEntryIDBytes+1), Key: "key", Content: "content"})
	assertBeforeSQL("memory id length", err, "entry id exceeds")
	assertBeforeSQL("memory forget id control", memoryStore.Forget(ctx, scope, "mem\n1"), "control character")
	assertBeforeSQL("memory forget id empty", memoryStore.Forget(ctx, scope, ""), "memory entry id is empty")
	assertBeforeSQL("memory forget id whitespace", memoryStore.Forget(ctx, scope, "   "), "memory entry id is empty")
	_, err = memoryStore.Remember(ctx, scope, core.MemoryEntry{Key: "ke\x00y", Content: "content"})
	assertBeforeSQL("memory key NUL", err, "NUL")
	_, err = memoryStore.Remember(ctx, scope, core.MemoryEntry{Key: "key", Content: "con\x00tent"})
	assertBeforeSQL("memory content NUL", err, "NUL")
	_, err = memoryStore.Recall(ctx, scope, "que\x00ry", nil, 0)
	assertBeforeSQL("memory query NUL", err, "NUL")
	_, err = memoryStore.Remember(ctx, scope, core.MemoryEntry{Key: "key", Content: "content", Tags: []string{"ta\x00g"}})
	assertBeforeSQL("memory tag NUL", err, "NUL")

	assertBeforeSQL("empty rag scope ingest", ragIndex.Ingest(ctx, core.ScopePath{}, core.RagDocument{ID: "doc", Content: "content"}), "rag scope is empty")
	_, err = ragIndex.Search(ctx, core.ScopePath{}, core.RagQuery{Query: "content"})
	assertBeforeSQL("empty rag scope search", err, "rag scope is empty")
	assertBeforeSQL("rag id control", ragIndex.Ingest(ctx, scope, core.RagDocument{ID: "doc\n", Content: "content"}), "control character")
	assertBeforeSQL("rag id length", ragIndex.Ingest(ctx, scope, core.RagDocument{ID: strings.Repeat("a", rag.MaxDocumentIDBytes+1), Content: "content"}), "document id exceeds")
	assertBeforeSQL("rag source control", ragIndex.Ingest(ctx, scope, core.RagDocument{ID: "doc", Source: "src\t", Content: "content"}), "control character")
	assertBeforeSQL("rag source length", ragIndex.Ingest(ctx, scope, core.RagDocument{ID: "doc", Source: strings.Repeat("s", rag.MaxDocumentSourceBytes+1), Content: "content"}), "document source exceeds")
	assertBeforeSQL("rag content NUL", ragIndex.Ingest(ctx, scope, core.RagDocument{ID: "doc", Content: "con\x00tent"}), "NUL")
	assertBeforeSQL("rag unique tokens", ragIndex.Ingest(ctx, scope, core.RagDocument{ID: "doc", Content: uniqueRagQuery(rag.MaxDocumentTokens + 1)}), "unique tokens")
	_, err = ragIndex.Search(ctx, scope, core.RagQuery{Query: "que\x00ry"})
	assertBeforeSQL("rag query NUL", err, "NUL")
	assertBeforeSQL("rag tag NUL", ragIndex.Ingest(ctx, scope, core.RagDocument{ID: "doc", Content: "content", Tags: []string{"ta\x00g"}}), "NUL")
}

func TestSQLMemoryAndRagAcceptsNewlinesInContent(t *testing.T) {
	db := newSQLMemoryRagV26DB(t)
	store, err := NewSQLMemoryStore(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	index, err := NewSQLRagIndex(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, scope := testScopes()
	ctx := context.Background()
	remembered, err := store.Remember(ctx, scope, core.MemoryEntry{Key: "newline", Content: "line1\nline2"})
	if err != nil {
		t.Fatalf("newline memory content was rejected: %v", err)
	}
	hits, err := store.Recall(ctx, scope, "line1", nil, 0)
	if err != nil || len(hits) != 1 || hits[0].ID != remembered.ID || hits[0].Content != "line1\nline2" {
		t.Fatalf("newline memory recall = %#v err=%v", hits, err)
	}
	if err := index.Ingest(ctx, scope, core.RagDocument{ID: "newline", Content: "alpha\nbeta"}); err != nil {
		t.Fatalf("newline rag content was rejected: %v", err)
	}
	chunks, err := index.Search(ctx, scope, core.RagQuery{Query: "alpha"})
	if err != nil || len(chunks) != 1 || chunks[0].ID != "newline" || chunks[0].Content != "alpha\nbeta" {
		t.Fatalf("newline rag search = %#v err=%v", chunks, err)
	}
}

func TestSQLMemoryStoreRejectsScopeOverflow(t *testing.T) {
	db := newSQLMemoryRagV26DB(t)
	store, err := NewSQLMemoryStore(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	store.maxEntries = 2
	_, _, _, scope := testScopes()
	ctx := context.Background()
	if _, err := store.Remember(ctx, scope, core.MemoryEntry{Key: "a", Content: "one"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Remember(ctx, scope, core.MemoryEntry{Key: "b", Content: "two"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Remember(ctx, scope, core.MemoryEntry{Key: "c", Content: "three"}); err == nil {
		t.Fatal("sql memory scope overflow was accepted")
	}
	replaced, err := store.Remember(ctx, scope, core.MemoryEntry{Key: "a", Content: "updated"})
	if err != nil || replaced.Content != "updated" {
		t.Fatalf("replacing an existing sql memory key must not count as overflow: %#v err=%v", replaced, err)
	}
}

func TestSQLRagIndexRejectsScopeOverflow(t *testing.T) {
	db := newSQLMemoryRagV26DB(t)
	index, err := NewSQLRagIndex(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	index.maxDocuments = 2
	_, _, _, scope := testScopes()
	ctx := context.Background()
	if err := index.Ingest(ctx, scope, core.RagDocument{ID: "a", Content: "one alpha"}); err != nil {
		t.Fatal(err)
	}
	if err := index.Ingest(ctx, scope, core.RagDocument{ID: "b", Content: "two beta"}); err != nil {
		t.Fatal(err)
	}
	if err := index.Ingest(ctx, scope, core.RagDocument{ID: "c", Content: "three gamma"}); err == nil {
		t.Fatal("sql rag scope overflow was accepted")
	}
	if err := index.Ingest(ctx, scope, core.RagDocument{ID: "a", Content: "one alpha updated"}); err != nil {
		t.Fatalf("replacing an existing sql rag document must not count as overflow: %v", err)
	}
}

func TestSQLMemoryStoreRejectsTooManyScopes(t *testing.T) {
	db := newSQLMemoryRagV26DB(t)
	store, err := NewSQLMemoryStore(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	store.maxScopes = 2
	ctx := context.Background()
	global, product, tenant, user := testScopes()
	if _, err := store.Remember(ctx, global, core.MemoryEntry{Key: "a", Content: "one"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Remember(ctx, product, core.MemoryEntry{Key: "b", Content: "two"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Remember(ctx, tenant, core.MemoryEntry{Key: "c", Content: "three"}); err == nil {
		t.Fatal("sql memory store scope overflow was accepted")
	}
	if _, err := store.Remember(ctx, global, core.MemoryEntry{Key: "a", Content: "updated"}); err != nil {
		t.Fatalf("replacing an existing sql memory scope must not count as overflow: %v", err)
	}
	_ = user
}

func TestSQLRagIndexRejectsTooManyScopes(t *testing.T) {
	db := newSQLMemoryRagV26DB(t)
	index, err := NewSQLRagIndex(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	index.maxScopes = 2
	ctx := context.Background()
	global, product, tenant, _ := testScopes()
	if err := index.Ingest(ctx, global, core.RagDocument{ID: "a", Content: "one alpha"}); err != nil {
		t.Fatal(err)
	}
	if err := index.Ingest(ctx, product, core.RagDocument{ID: "b", Content: "two beta"}); err != nil {
		t.Fatal(err)
	}
	if err := index.Ingest(ctx, tenant, core.RagDocument{ID: "c", Content: "three gamma"}); err == nil {
		t.Fatal("sql rag index scope overflow was accepted")
	}
	if err := index.Ingest(ctx, global, core.RagDocument{ID: "a", Content: "one alpha updated"}); err != nil {
		t.Fatalf("replacing an existing sql rag scope must not count as overflow: %v", err)
	}
}
