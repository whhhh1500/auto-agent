package storage

import (
	"context"
	"testing"
	"time"

	"github.com/whhhh1500/auto-agent/pkg/execution"
)

func TestSQLLibraryObserverPersistsSearchThenChoose(t *testing.T) {
	store := newTestSQLStore(t)
	observer := store.LibraryObserver()
	ctx := context.Background()
	if err := observer.Observe(ctx, execution.ToolLibraryObservation{
		Action: "search", Query: "send slack channel", Hits: 1,
		HitIDs: []string{"mcp.slack/send"}, CallID: "c-search", TenantID: "tenant-a",
	}); err != nil {
		t.Fatal(err)
	}
	if err := observer.Observe(ctx, execution.ToolLibraryObservation{
		Action: "describe", ToolID: "mcp.slack/send", Library: "mcp.slack", Name: "send",
		Hits: 1, HitIDs: []string{"mcp.slack/send"}, CallID: "c-describe",
	}); err != nil {
		t.Fatal(err)
	}
	records, err := observer.List(ctx, 10)
	if err != nil || len(records) != 2 {
		t.Fatalf("list = %#v err=%v", records, err)
	}
	byAction := map[string]execution.ToolLibraryObservation{}
	for _, rec := range records {
		byAction[rec.Action] = rec
	}
	if byAction["search"].Query != "send slack channel" || byAction["describe"].ToolID != "mcp.slack/send" {
		t.Fatalf("query/choose not stored: %#v", records)
	}
}

func TestSQLLibraryObserverDropsOldestAtCap(t *testing.T) {
	store := newTestSQLStore(t)
	observer := store.LibraryObserver()
	observer.max = 2
	ctx := context.Background()
	for i, action := range []string{"search", "explore", "describe"} {
		if err := observer.Observe(ctx, execution.ToolLibraryObservation{
			Time: time.UnixMilli(int64(1000 + i)).UTC(), Action: action, Query: action, Hits: 1,
		}); err != nil {
			t.Fatal(err)
		}
	}
	records, err := observer.List(ctx, 10)
	if err != nil || len(records) != 2 {
		t.Fatalf("capped list = %#v err=%v", records, err)
	}
	joined := records[0].Action + "," + records[1].Action
	if joined != "describe,explore" {
		t.Fatalf("oldest search should have been dropped: %s", joined)
	}
}

func TestSQLLibraryObserverTableExistsOnOpen(t *testing.T) {
	store := newTestSQLStore(t)
	ctx := context.Background()
	exists, err := sqlTableExists(ctx, store.db, SQLDialectSQLite, "tool_library_observations")
	if err != nil || !exists {
		t.Fatalf("v29 table missing: exists=%t err=%v", exists, err)
	}
	if SQLSchemaVersion != 48 {
		t.Fatalf("schema version = %d", SQLSchemaVersion)
	}
}
