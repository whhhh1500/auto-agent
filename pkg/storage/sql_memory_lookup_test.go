package storage

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"testing"
	"time"

	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/extensions/memory"
)

var _ memory.KeyStore = (*SQLMemoryStore)(nil)

func TestSQLMemoryStoreLookupExactKeySQLite(t *testing.T) {
	db := newSQLMemoryRagV26DB(t)
	store, err := NewSQLMemoryStore(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	testSQLMemoryStoreLookupExactKey(t, store, db, SQLDialectSQLite)
}

func TestPostgresSQLMemoryStoreLookupExactKey(t *testing.T) {
	db := newPostgresTestDB(t)
	ctx := context.Background()
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres); err != nil {
		t.Fatal(err)
	}
	store, err := NewSQLMemoryStore(db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	testSQLMemoryStoreLookupExactKey(t, store, db, SQLDialectPostgres)
}

func testSQLMemoryStoreLookupExactKey(t *testing.T, store *SQLMemoryStore, db *sql.DB, dialect SQLDialect) {
	t.Helper()
	ctx := context.Background()
	_, _, tenant, scope := testScopes()
	peer, err := tenant.Child(core.ScopeRef{Kind: core.ScopeUser, ID: "user-b"})
	if err != nil {
		t.Fatal(err)
	}
	key := "release.channel/v2"
	want, err := store.Remember(ctx, scope, core.MemoryEntry{
		ID: "mem_lookup_exact", Key: key, Content: "content-only lookup must not match", Tags: []string{"release", "v2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	peerEntry, err := store.Remember(ctx, peer, core.MemoryEntry{
		ID: "mem_lookup_peer", Key: key, Content: "peer scope value", Tags: []string{"peer"},
	})
	if err != nil {
		t.Fatal(err)
	}

	got, found, err := store.Lookup(ctx, scope, key)
	if err != nil || !found {
		t.Fatalf("exact lookup found=%t err=%v", found, err)
	}
	wantCreatedAt := time.UnixMilli(want.CreatedAt.UnixMilli()).UTC()
	if got.ID != want.ID || got.Key != want.Key || got.Content != want.Content || !reflect.DeepEqual(got.Tags, want.Tags) || !got.CreatedAt.Equal(wantCreatedAt) {
		t.Fatalf("exact lookup=%#v, want %#v", got, want)
	}

	for _, missingKey := range []string{
		"Release.channel/v2",                          // case differs
		"release-channel/v2",                          // punctuation differs
		"content-only lookup must not match",          // content is not a key
		"not-present'; DROP TABLE memory_entries; --", // remains a bound value
	} {
		entry, found, err := store.Lookup(ctx, scope, missingKey)
		if err != nil || found || !emptyMemoryEntry(entry) {
			t.Fatalf("lookup %q = %#v found=%t err=%v; want not found", missingKey, entry, found, err)
		}
	}

	entry, found, err := store.Lookup(ctx, peer, key)
	if err != nil || !found || entry.ID != peerEntry.ID || entry.Content != peerEntry.Content {
		t.Fatalf("peer exact lookup=%#v found=%t err=%v", entry, found, err)
	}
	entry, found, err = store.Lookup(ctx, scope, "peer scope value")
	if err != nil || found || !emptyMemoryEntry(entry) {
		t.Fatalf("peer content leaked across scope: %#v found=%t err=%v", entry, found, err)
	}

	if _, err := db.ExecContext(ctx, sqlQuery{"UPDATE memory_entries SET tags_json = ? WHERE scope = ? AND key = ?"}.bind(dialect), "{corrupt", scope.String(), key); err != nil {
		t.Fatal(err)
	}
	if entry, found, err := store.Lookup(ctx, scope, key); err == nil || found || !emptyMemoryEntry(entry) {
		t.Fatalf("corrupt tags lookup=%#v found=%t err=%v; want decode error", entry, found, err)
	}
}

func TestSQLMemoryStoreLookupRejectsInvalidScopeAndKey(t *testing.T) {
	db := newSQLMemoryRagV26DB(t)
	store, err := NewSQLMemoryStore(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_, _, _, scope := testScopes()
	for _, key := range []string{"", " \t", "key\x00suffix", strings.Repeat("k", memory.MaxEntryKeyBytes+1)} {
		if _, found, err := store.Lookup(ctx, scope, key); err == nil || found {
			t.Fatalf("invalid lookup key %q found=%t err=%v", key, found, err)
		}
	}
	if _, found, err := store.Lookup(ctx, core.ScopePath{}, "valid"); err == nil || found {
		t.Fatalf("empty scope found=%t err=%v", found, err)
	}
}

func TestSQLMemoryStoreLookupFailsClosedOnNOCASEMatch(t *testing.T) {
	ctx := context.Background()
	_, _, _, scope := testScopes()
	for _, test := range []struct {
		name        string
		storedScope string
		storedKey   string
		lookupKey   string
	}{
		{
			name:        "key_case_difference",
			storedScope: scope.String(),
			storedKey:   "Release.Channel/v2",
			lookupKey:   "release.channel/v2",
		},
		{
			name:        "scope_case_difference",
			storedScope: strings.ToUpper(scope.String()),
			storedKey:   "release.channel/v2",
			lookupKey:   "release.channel/v2",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, err := sql.Open("sqlite", t.TempDir()+"/nocase-memory.db")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			if _, err := db.ExecContext(ctx, `CREATE TABLE memory_entries (
				scope TEXT NOT NULL COLLATE NOCASE,
				id TEXT NOT NULL,
				key TEXT NOT NULL COLLATE NOCASE,
				content TEXT NOT NULL,
				tags_json TEXT NOT NULL,
				created_at INTEGER NOT NULL
			)`); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, `INSERT INTO memory_entries (scope, id, key, content, tags_json, created_at)
				VALUES (?, ?, ?, ?, ?, ?)`, test.storedScope, "mem_nocase", test.storedKey, "must not leak", "[]", int64(1)); err != nil {
				t.Fatal(err)
			}
			store, err := NewSQLMemoryStore(db, SQLDialectSQLite)
			if err != nil {
				t.Fatal(err)
			}
			entry, found, err := store.Lookup(ctx, scope, test.lookupKey)
			if err == nil || found || !emptyMemoryEntry(entry) {
				t.Fatalf("NOCASE lookup=%#v found=%t err=%v; want fail-closed mismatch", entry, found, err)
			}
		})
	}
}

func TestBuildSQLMemoryLookupUsesBoundParameters(t *testing.T) {
	statement := sqlMemoryLookup.bind(SQLDialectSQLite)
	if strings.Count(statement, "?") != 2 || strings.Contains(statement, "release.channel/v2") {
		t.Fatalf("lookup statement must keep scope and key as placeholders: %q", statement)
	}
	postgres := sqlMemoryLookup.bind(SQLDialectPostgres)
	if !strings.Contains(postgres, "$1") || !strings.Contains(postgres, "$2") {
		t.Fatalf("postgres lookup placeholders=%q", postgres)
	}
}

func TestSQLMemoryStoreLookupCreatedAtRoundTrip(t *testing.T) {
	db := newSQLMemoryRagV26DB(t)
	store, err := NewSQLMemoryStore(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, scope := testScopes()
	remembered, err := store.Remember(context.Background(), scope, core.MemoryEntry{Key: "created-at", Content: "round trip"})
	if err != nil {
		t.Fatal(err)
	}
	got, found, err := store.Lookup(context.Background(), scope, remembered.Key)
	wantCreatedAt := time.UnixMilli(remembered.CreatedAt.UnixMilli()).UTC()
	if err != nil || !found || got.CreatedAt.IsZero() || !got.CreatedAt.Equal(wantCreatedAt) || got.CreatedAt.Location() != time.UTC {
		t.Fatalf("created at lookup=%#v found=%t err=%v", got, found, err)
	}
}

func emptyMemoryEntry(entry core.MemoryEntry) bool {
	return entry.ID == "" && entry.Key == "" && entry.Content == "" && len(entry.Tags) == 0 && entry.CreatedAt.IsZero()
}
