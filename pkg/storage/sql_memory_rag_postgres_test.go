package storage

import (
	"context"
	"strings"
	"testing"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

func TestPostgresMemoryAndRagRejectNULAndControlBeforeDriver(t *testing.T) {
	db := newPostgresTestDB(t)
	ctx := context.Background()
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres); err != nil {
		t.Fatal(err)
	}
	store, err := NewSQLMemoryStore(db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	index, err := NewSQLRagIndex(db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, scope := testScopes()
	assertAppError := func(name string, err error, want string) {
		t.Helper()
		if err == nil {
			t.Fatalf("%s was accepted", name)
		}
		message := err.Error()
		if strings.Contains(strings.ToLower(message), "utf8") || strings.Contains(message, "invalid byte sequence") {
			t.Fatalf("%s reached the postgres driver: %v", name, err)
		}
		if !strings.Contains(message, want) {
			t.Fatalf("%s error=%v want %q", name, err, want)
		}
	}

	_, err = store.Remember(ctx, core.ScopePath{}, core.MemoryEntry{Key: "key", Content: "content"})
	assertAppError("empty memory scope", err, "memory scope is empty")
	_, err = store.Remember(ctx, scope, core.MemoryEntry{Key: "key", Content: "con\x00tent"})
	assertAppError("memory content NUL", err, "NUL")
	_, err = store.Remember(ctx, scope, core.MemoryEntry{ID: "mem\n1", Key: "key", Content: "content"})
	assertAppError("memory id control", err, "control character")
	assertAppError("rag content NUL", index.Ingest(ctx, scope, core.RagDocument{ID: "doc", Content: "con\x00tent"}), "NUL")
	assertAppError("rag id control", index.Ingest(ctx, scope, core.RagDocument{ID: "doc\n", Content: "content"}), "control character")
	assertAppError("empty rag scope", index.Ingest(ctx, core.ScopePath{}, core.RagDocument{ID: "doc", Content: "content"}), "rag scope is empty")

	remembered, err := store.Remember(ctx, scope, core.MemoryEntry{Key: "newline", Content: "line1\nline2"})
	if err != nil {
		t.Fatalf("postgres newline memory content was rejected: %v", err)
	}
	hits, err := store.Recall(ctx, scope, "line1", nil, 1)
	if err != nil || len(hits) != 1 || hits[0].ID != remembered.ID {
		t.Fatalf("postgres newline memory recall = %#v err=%v", hits, err)
	}
	if err := index.Ingest(ctx, scope, core.RagDocument{ID: "newline", Content: "alpha\nbeta"}); err != nil {
		t.Fatalf("postgres newline rag content was rejected: %v", err)
	}
}
