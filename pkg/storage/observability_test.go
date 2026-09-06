package storage

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

func TestSQLObsStoreListHitsRejectsInvalidFiltersBeforeSQL(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store, err := NewSQLObsStore(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	assertBeforeSQL := func(name string, err error) {
		t.Helper()
		if err == nil {
			t.Fatalf("%s was accepted", name)
		}
		if strings.Contains(err.Error(), "no such table") {
			t.Fatalf("%s reached SQL before validation: %v", name, err)
		}
	}
	assertBeforeSQL("tenant NUL", func() error {
		_, _, err := store.ListHits(ctx, ObsHitFilter{TenantID: "acme\x00"})
		return err
	}())
	assertBeforeSQL("session id", func() error {
		_, _, err := store.ListHits(ctx, ObsHitFilter{SessionID: "bad id"})
		return err
	}())
	assertBeforeSQL("rule control", func() error {
		_, _, err := store.ListHits(ctx, ObsHitFilter{RuleID: "rule\n1"})
		return err
	}())
}

func TestSQLObsStoreRejectsInvalidWritesBeforeSQL(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store, err := NewSQLObsStore(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	assertBeforeSQL := func(name string, err error) {
		t.Helper()
		if err == nil {
			t.Fatalf("%s was accepted", name)
		}
		if strings.Contains(err.Error(), "no such table") {
			t.Fatalf("%s reached SQL before validation: %v", name, err)
		}
	}
	assertBeforeSQL("rule NUL name", func() error {
		_, err := store.CreateRule(ctx, ObsRule{Name: "bad\x00", Kind: "keyword", Pattern: "sol"})
		return err
	}())
	assertBeforeSQL("rule NUL pattern", func() error {
		_, err := store.CreateRule(ctx, ObsRule{Name: "watch", Kind: "keyword", Pattern: "sol\x00"})
		return err
	}())
	assertBeforeSQL("delete empty", store.DeleteRule(ctx, ""))
	assertBeforeSQL("hit session", store.RecordHit(ctx, ObsHit{RuleID: "obs_1", SessionID: "bad id"}))
	assertBeforeSQL("hit snippet NUL", store.RecordHit(ctx, ObsHit{RuleID: "obs_1", Snippet: "x\x00y"}))
}

func TestSQLObsStoreRejectsRuleOverflow(t *testing.T) {
	sessions := newTestSQLStore(t)
	store, err := NewSQLObsStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	store.maxRules = 2
	ctx := context.Background()
	if _, err := store.CreateRule(ctx, ObsRule{Name: "one", Kind: "keyword", Pattern: "alpha"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateRule(ctx, ObsRule{Name: "two", Kind: "keyword", Pattern: "beta"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateRule(ctx, ObsRule{Name: "three", Kind: "keyword", Pattern: "gamma"}); err == nil {
		t.Fatal("obs rule overflow was accepted")
	}
}

func TestSQLObsStoreRejectsHitOverflow(t *testing.T) {
	sessions := newTestSQLStore(t)
	store, err := NewSQLObsStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	store.maxHits = 2
	ctx := context.Background()
	if err := store.RecordHit(ctx, ObsHit{RuleID: "obs_1", Kind: "keyword", Snippet: "one"}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordHit(ctx, ObsHit{RuleID: "obs_1", Kind: "keyword", Snippet: "two"}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordHit(ctx, ObsHit{RuleID: "obs_1", Kind: "keyword", Snippet: "three"}); err == nil {
		t.Fatal("obs hit overflow was accepted")
	} else if !strings.Contains(err.Error(), "observability hits exceed maximum of 2") {
		t.Fatalf("unexpected overflow error: %v", err)
	}
	if err := store.RecordHit(ctx, ObsHit{RuleID: "obs_1", Kind: "keyword", Snippet: strings.Repeat("x", MaxObsSnippetBytes+1)}); err == nil {
		t.Fatal("oversized obs snippet was accepted")
	}
}
