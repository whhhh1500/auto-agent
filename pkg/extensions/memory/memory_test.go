package memory

import (
	"context"
	"strings"
	"testing"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

func TestStoreRejectsEmptyScopeControlIDsAndNUL(t *testing.T) {
	store := NewSliceStore()
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	ctx := context.Background()

	if _, err := store.Remember(ctx, core.ScopePath{}, Entry{Key: "key", Content: "content"}); err == nil {
		t.Fatal("empty memory scope remember was accepted")
	}
	if _, err := store.Recall(ctx, core.ScopePath{}, "query", nil, 0); err == nil {
		t.Fatal("empty memory scope recall was accepted")
	}
	if err := store.Forget(ctx, core.ScopePath{}, "mem_1"); err == nil {
		t.Fatal("empty memory scope forget was accepted")
	}
	if _, err := store.Remember(ctx, scope, Entry{ID: "mem\n1", Key: "key", Content: "content"}); err == nil {
		t.Fatal("control character memory id was accepted")
	}
	if _, err := store.Remember(ctx, scope, Entry{ID: strings.Repeat("a", MaxEntryIDBytes+1), Key: "key", Content: "content"}); err == nil {
		t.Fatal("oversized memory id was accepted")
	}
	if err := store.Forget(ctx, scope, "mem\n1"); err == nil {
		t.Fatal("control character forget id was accepted")
	}
	if err := store.Forget(ctx, scope, ""); err == nil {
		t.Fatal("empty forget id was accepted")
	}
	if err := store.Forget(ctx, scope, "   "); err == nil {
		t.Fatal("whitespace forget id was accepted")
	}
	if _, err := store.Remember(ctx, scope, Entry{Key: "ke\x00y", Content: "content"}); err == nil {
		t.Fatal("NUL memory key was accepted")
	}
	if _, err := store.Remember(ctx, scope, Entry{Key: "key", Content: "con\x00tent"}); err == nil {
		t.Fatal("NUL memory content was accepted")
	}
	if _, err := store.Recall(ctx, scope, "que\x00ry", nil, 0); err == nil {
		t.Fatal("NUL memory query was accepted")
	}
	if _, err := store.Remember(ctx, scope, Entry{Key: "key", Content: "content", Tags: []string{"ta\x00g"}}); err == nil {
		t.Fatal("NUL memory tag was accepted")
	}
	remembered, err := store.Remember(ctx, scope, Entry{Key: "newline", Content: "line1\nline2"})
	if err != nil {
		t.Fatalf("newline memory content was rejected: %v", err)
	}
	hits, err := store.Recall(ctx, scope, "line1", nil, 0)
	if err != nil || len(hits) != 1 || hits[0].ID != remembered.ID || hits[0].Content != "line1\nline2" {
		t.Fatalf("newline memory recall = %#v err=%v", hits, err)
	}
}

func TestSliceStoreRejectsScopeOverflow(t *testing.T) {
	store := NewSliceStore()
	store.maxEntries = 2
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	ctx := context.Background()
	if _, err := store.Remember(ctx, scope, Entry{Key: "a", Content: "one"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Remember(ctx, scope, Entry{Key: "b", Content: "two"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Remember(ctx, scope, Entry{Key: "c", Content: "three"}); err == nil {
		t.Fatal("scope overflow was accepted")
	}
	replaced, err := store.Remember(ctx, scope, Entry{Key: "a", Content: "updated"})
	if err != nil || replaced.Content != "updated" {
		t.Fatalf("replacing an existing key must not count as overflow: %#v err=%v", replaced, err)
	}
}

func TestSliceStoreRejectsTooManyScopes(t *testing.T) {
	store := NewSliceStore()
	store.maxScopes = 2
	ctx := context.Background()
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	first, err := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "one"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "two"})
	if err != nil {
		t.Fatal(err)
	}
	third, err := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "three"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Remember(ctx, first, Entry{Key: "a", Content: "one"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Remember(ctx, second, Entry{Key: "b", Content: "two"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Remember(ctx, third, Entry{Key: "c", Content: "three"}); err == nil {
		t.Fatal("memory store scope overflow was accepted")
	}
	replaced, err := store.Remember(ctx, first, Entry{Key: "a", Content: "updated"})
	if err != nil {
		t.Fatalf("replacing an existing scope must not count as overflow: %v", err)
	}
	if err := store.Forget(ctx, first, replaced.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Remember(ctx, third, Entry{Key: "c", Content: "three"}); err != nil {
		t.Fatalf("forgetting the last entry in a scope must free a slot: %v", err)
	}
}
