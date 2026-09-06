package storage

import (
	"context"
	"sync"
	"testing"

	"github.com/cc-auto-agent/harness-core/pkg/extensions/subagent"
)

func TestSQLDelegationLinksSQLiteRoundTripListAndReplay(t *testing.T) {
	store := newTestSQLStore(t)
	links, err := NewSQLDelegationLinkStore(store.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	want := subagent.Link{ParentSessionID: "sess-parent", ParentRunID: "run-parent", ParentCallID: "call-delegate", ChildSessionID: "sess-child", ChildRunID: "run-child", TenantID: "acme", SubjectID: "alice", Depth: 1}
	got, created, err := links.PutIfAbsent(ctx, want)
	if err != nil || !created {
		t.Fatalf("put = %#v created=%v err=%v", got, created, err)
	}
	if got.CreatedAt.IsZero() || !got.CreatedAt.Equal(got.UpdatedAt) {
		t.Fatalf("timestamps not initialized: %#v", got)
	}
	replay, created, err := links.PutIfAbsent(ctx, want)
	if err != nil || created || replay.Key() != want.Key() {
		t.Fatalf("replay = %#v created=%v err=%v", replay, created, err)
	}
	byKey, found, err := links.Get(ctx, want.Key())
	if err != nil || !found || byKey.ChildSessionID != want.ChildSessionID {
		t.Fatalf("get = %#v found=%v err=%v", byKey, found, err)
	}
	byChild, found, err := links.GetByChild(ctx, want.ChildSessionID)
	if err != nil || !found || byChild.Key() != want.Key() {
		t.Fatalf("get child = %#v found=%v err=%v", byChild, found, err)
	}
	listed, err := links.List(ctx, subagent.DelegationLinkFilter{TenantID: "acme", Limit: 10})
	if err != nil || len(listed) != 1 || listed[0].ChildSessionID != want.ChildSessionID {
		t.Fatalf("list = %#v err=%v", listed, err)
	}
	if _, _, err := links.PutIfAbsent(ctx, subagent.Link{ParentSessionID: "sess-other", ParentRunID: "run-other", ParentCallID: "call-other", ChildSessionID: want.ChildSessionID, ChildRunID: "run-other", TenantID: "acme", SubjectID: "alice", Depth: 1}); err == nil {
		t.Fatal("reusing child session must conflict")
	}
}

func TestSQLDelegationLinksSQLiteConcurrentPutIfAbsent(t *testing.T) {
	store := newTestSQLStore(t)
	links, err := NewSQLDelegationLinkStore(store.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	base := subagent.Link{ParentSessionID: "sess-concurrent", ParentRunID: "run-concurrent", ParentCallID: "call-concurrent", ChildSessionID: "sess-concurrent-child", ChildRunID: "run-concurrent-child", TenantID: "acme", SubjectID: "alice", Depth: 1}
	var wg sync.WaitGroup
	results := make(chan bool, 12)
	errs := make(chan error, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, created, putErr := links.PutIfAbsent(context.Background(), base)
			results <- created
			errs <- putErr
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	created := 0
	for value := range results {
		if value {
			created++
		}
	}
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if created != 1 {
		t.Fatalf("created=%d, want exactly one", created)
	}
}

func TestSQLDelegationLinksSQLiteMigratesFromV30(t *testing.T) {
	store := newTestSQLStore(t)
	ctx := context.Background()
	if _, err := store.db.ExecContext(ctx, "DROP TABLE delegation_links"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, sqlUpdateMetaRow.bind(SQLDialectSQLite), "30"); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLSessionStore(ctx, store.db, SQLDialectSQLite); err != nil {
		t.Fatalf("v30 migration: %v", err)
	}
	if exists, err := sqlTableExists(ctx, store.db, SQLDialectSQLite, "delegation_links"); err != nil || !exists {
		t.Fatalf("delegation_links exists=%v err=%v", exists, err)
	}
}

func TestSQLDelegationLinksRejectInvalidMetadata(t *testing.T) {
	store := newTestSQLStore(t)
	links, err := NewSQLDelegationLinkStore(store.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	base := subagent.Link{ParentSessionID: "sess-parent", ParentRunID: "run-parent", ParentCallID: "call", ChildSessionID: "sess-child", ChildRunID: "run-child", TenantID: "acme", SubjectID: "alice", Depth: 1}
	for name, mutate := range map[string]func(*subagent.Link){
		"control":      func(link *subagent.Link) { link.ParentCallID = "bad\ncall" },
		"empty tenant": func(link *subagent.Link) { link.TenantID = "" },
		"depth":        func(link *subagent.Link) { link.Depth = 65 },
	} {
		t.Run(name, func(t *testing.T) {
			link := base
			mutate(&link)
			if _, _, err := links.PutIfAbsent(context.Background(), link); err == nil {
				t.Fatal("invalid link accepted")
			}
		})
	}
}

func TestPostgresDelegationLinksRoundTrip(t *testing.T) {
	store := newPostgresTestDB(t)
	if _, err := OpenSQLSessionStore(context.Background(), store, SQLDialectPostgres); err != nil {
		t.Fatal(err)
	}
	links, err := NewSQLDelegationLinkStore(store, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	want := subagent.Link{ParentSessionID: "sess-pg", ParentRunID: "run-pg", ParentCallID: "call-pg", ChildSessionID: "sess-pg-child", ChildRunID: "run-pg-child", TenantID: "acme", SubjectID: "alice", Depth: 2}
	got, created, err := links.PutIfAbsent(context.Background(), want)
	if err != nil || !created || got.ChildRunID != want.ChildRunID {
		t.Fatalf("put = %#v created=%v err=%v", got, created, err)
	}
	listed, err := links.List(context.Background(), subagent.DelegationLinkFilter{ParentSessionID: want.ParentSessionID})
	if err != nil || len(listed) != 1 {
		t.Fatalf("list = %#v err=%v", listed, err)
	}
}
