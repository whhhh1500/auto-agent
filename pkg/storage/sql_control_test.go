package storage

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestSQLBindingJournalRejectsOverflowAndInvalidRecords(t *testing.T) {
	sessions := newTestSQLStore(t)
	journal, err := NewSQLBindingJournal(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := journal.Record(ctx, BindingRecord{Kind: "policy"}); err == nil {
		t.Fatal("empty binding id was accepted")
	}
	if err := journal.Record(ctx, BindingRecord{ID: "bind_1"}); err == nil {
		t.Fatal("empty binding kind was accepted")
	}
	if err := journal.Record(ctx, BindingRecord{ID: "bind\n1", Kind: "policy"}); err == nil {
		t.Fatal("binding id with a newline was accepted")
	}
	journal.maxBindings = 2
	payload, _ := json.Marshal(map[string]string{"scope": "global:global"})
	if err := journal.Record(ctx, BindingRecord{ID: "bind_1", Kind: "policy", Payload: payload}); err != nil {
		t.Fatal(err)
	}
	if err := journal.Record(ctx, BindingRecord{ID: "bind_2", Kind: "policy", Payload: payload}); err != nil {
		t.Fatal(err)
	}
	if err := journal.Record(ctx, BindingRecord{ID: "bind_3", Kind: "policy", Payload: payload}); err == nil {
		t.Fatal("binding overflow was accepted")
	}
	listed, err := journal.List(ctx)
	if err != nil || len(listed) != 2 {
		t.Fatalf("listed=%d err=%v", len(listed), err)
	}
}

func TestSQLRunStatsStoreRejectsOverflow(t *testing.T) {
	sessions := newTestSQLStore(t)
	store, err := NewSQLRunStatsStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	store.maxStats = 2
	ctx := context.Background()
	if err := store.RecordRunStat(ctx, RunStat{RunID: "run-stat-1", SessionID: "s1", TenantID: "acme", Status: "completed"}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordRunStat(ctx, RunStat{RunID: "run-stat-2", SessionID: "s2", TenantID: "acme", Status: "failed"}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordRunStat(ctx, RunStat{RunID: "run-stat-3", SessionID: "s3", TenantID: "acme", Status: "completed"}); err == nil {
		t.Fatal("run stats overflow was accepted")
	} else if !strings.Contains(err.Error(), "run stats exceed maximum of 2") {
		t.Fatalf("unexpected overflow error: %v", err)
	}
	if err := store.RecordRunStat(ctx, RunStat{RunID: "run-stat-1", SessionID: "s1", TenantID: "acme", Status: "completed"}); err == nil {
		t.Fatal("duplicate run stat at cap was not a conflict")
	}
}
