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

func TestSQLBindingJournalReplaceIsAtomicAndStable(t *testing.T) {
	store := newTestSQLStore(t)
	journal, err := NewSQLBindingJournal(store.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	old := BindingRecord{ID: "binding-old", Kind: "profile", Payload: json.RawMessage(`{"name":"old"}`)}
	if err := journal.Record(ctx, old); err != nil {
		t.Fatal(err)
	}
	if err := journal.Record(ctx, BindingRecord{ID: "binding-other", Kind: "policy", Payload: json.RawMessage(`{"name":"other"}`)}); err != nil {
		t.Fatal(err)
	}
	beforeEpoch, err := store.AuthorizationEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var createdBefore int64
	if err := store.db.QueryRowContext(ctx, "SELECT created_at FROM admin_bindings WHERE id = ?", old.ID).Scan(&createdBefore); err != nil {
		t.Fatal(err)
	}
	before, err := journal.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	replaced := BindingRecord{ID: old.ID, Kind: "profile", Payload: json.RawMessage(`{"name":"same-id replacement"}`)}
	if err := journal.Replace(ctx, old.ID, replaced); err != nil {
		t.Fatal(err)
	}
	afterEpoch, err := store.AuthorizationEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if afterEpoch != beforeEpoch+1 {
		t.Fatalf("same-id replace epoch=%d want=%d", afterEpoch, beforeEpoch+1)
	}
	var createdAfter int64
	if err := store.db.QueryRowContext(ctx, "SELECT created_at FROM admin_bindings WHERE id = ?", old.ID).Scan(&createdAfter); err != nil {
		t.Fatal(err)
	}
	if createdAfter != createdBefore {
		t.Fatalf("same-id replacement changed created_at: got=%d want=%d", createdAfter, createdBefore)
	}
	after, err := journal.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) || after[0].ID != before[0].ID || after[1].ID != before[1].ID {
		t.Fatalf("same-id replacement changed list order: before=%#v after=%#v", before, after)
	}
	if string(after[0].Payload) != `{"name":"same-id replacement"}` && string(after[1].Payload) != `{"name":"same-id replacement"}` {
		t.Fatalf("replacement payload missing from %#v", after)
	}
}

func TestSQLBindingJournalReplaceUsesNetCapacityAndOneEpoch(t *testing.T) {
	store := newTestSQLStore(t)
	journal, err := NewSQLBindingJournal(store.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	journal.maxBindings = 1
	ctx := context.Background()
	if err := journal.Record(ctx, BindingRecord{ID: "binding-old", Kind: "profile", Payload: json.RawMessage(`{"version":1}`)}); err != nil {
		t.Fatal(err)
	}
	before, err := store.AuthorizationEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.Replace(ctx, "binding-old", BindingRecord{ID: "binding-new", Kind: "profile", Payload: json.RawMessage(`{"version":2}`)}); err != nil {
		t.Fatalf("net-zero replacement at capacity failed: %v", err)
	}
	after, err := store.AuthorizationEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after != before+1 {
		t.Fatalf("different-id replace epoch=%d want=%d", after, before+1)
	}
	listed, err := journal.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0].ID != "binding-new" || string(listed[0].Payload) != `{"version":2}` {
		t.Fatalf("replacement list=%#v", listed)
	}
}

func TestSQLBindingJournalReplaceRollsBackFailures(t *testing.T) {
	t.Run("missing old and duplicate new preserve durable state", func(t *testing.T) {
		store := newTestSQLStore(t)
		journal, err := NewSQLBindingJournal(store.db, SQLDialectSQLite)
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		old := BindingRecord{ID: "binding-old", Kind: "profile", Payload: json.RawMessage(`{"version":1}`)}
		taken := BindingRecord{ID: "binding-taken", Kind: "policy", Payload: json.RawMessage(`{"version":2}`)}
		for _, record := range []BindingRecord{old, taken} {
			if err := journal.Record(ctx, record); err != nil {
				t.Fatal(err)
			}
		}
		assertUnchanged := func(label string, before int64) {
			t.Helper()
			after, err := store.AuthorizationEpoch(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if after != before {
				t.Fatalf("%s changed epoch: got=%d want=%d", label, after, before)
			}
			listed, err := journal.List(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(listed) != 2 || listed[0].ID != old.ID || listed[1].ID != taken.ID {
				t.Fatalf("%s changed bindings: %#v", label, listed)
			}
		}
		before, err := store.AuthorizationEpoch(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := journal.Replace(ctx, "missing", BindingRecord{ID: "binding-new", Kind: "profile"}); err == nil {
			t.Fatal("missing old binding replacement succeeded")
		}
		assertUnchanged("missing old", before)
		if err := journal.Replace(ctx, old.ID, BindingRecord{ID: taken.ID, Kind: "profile"}); err == nil {
			t.Fatal("duplicate replacement succeeded")
		}
		assertUnchanged("duplicate new", before)
	})

	t.Run("epoch failure rolls back same-id replacement", func(t *testing.T) {
		store := newTestSQLStore(t)
		journal, err := NewSQLBindingJournal(store.db, SQLDialectSQLite)
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		old := BindingRecord{ID: "binding-old", Kind: "profile", Payload: json.RawMessage(`{"version":1}`)}
		if err := journal.Record(ctx, old); err != nil {
			t.Fatal(err)
		}
		before, err := store.AuthorizationEpoch(ctx)
		if err != nil {
			t.Fatal(err)
		}
		const trigger = "abort_binding_replace_epoch"
		if _, err := store.db.ExecContext(ctx, `CREATE TRIGGER `+trigger+`
			BEFORE UPDATE OF value ON store_meta WHEN NEW.key = 'authorization_epoch'
			BEGIN SELECT RAISE(ABORT, 'binding replace epoch abort'); END`); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _, _ = store.db.ExecContext(context.Background(), "DROP TRIGGER IF EXISTS "+trigger) })
		if err := journal.Replace(ctx, old.ID, BindingRecord{ID: old.ID, Kind: old.Kind, Payload: json.RawMessage(`{"version":2}`)}); err == nil {
			t.Fatal("epoch failure replacement succeeded")
		}
		after, err := store.AuthorizationEpoch(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if after != before {
			t.Fatalf("epoch failure changed epoch: got=%d want=%d", after, before)
		}
		listed, err := journal.List(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(listed) != 1 || string(listed[0].Payload) != string(old.Payload) {
			t.Fatalf("epoch failure changed old binding: %#v", listed)
		}
	})

	for _, value := range []struct {
		name  string
		setup string
	}{
		{name: "missing", setup: "DELETE FROM store_meta WHERE key = 'authorization_epoch'"},
		{name: "invalid", setup: "UPDATE store_meta SET value = 'not-an-epoch' WHERE key = 'authorization_epoch'"},
		{name: "exhausted", setup: "UPDATE store_meta SET value = '9223372036854775807' WHERE key = 'authorization_epoch'"},
	} {
		t.Run("epoch "+value.name+" rolls back replacement", func(t *testing.T) {
			store := newTestSQLStore(t)
			journal, err := NewSQLBindingJournal(store.db, SQLDialectSQLite)
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			old := BindingRecord{ID: "binding-old", Kind: "profile", Payload: json.RawMessage(`{"version":1}`)}
			if err := journal.Record(ctx, old); err != nil {
				t.Fatal(err)
			}
			if _, err := store.db.ExecContext(ctx, value.setup); err != nil {
				t.Fatal(err)
			}
			if err := journal.Replace(ctx, old.ID, BindingRecord{ID: old.ID, Kind: old.Kind, Payload: json.RawMessage(`{"version":2}`)}); err == nil {
				t.Fatal("replacement succeeded with unusable authorization epoch")
			}
			listed, err := journal.List(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(listed) != 1 || string(listed[0].Payload) != string(old.Payload) {
				t.Fatalf("unusable epoch changed binding: %#v", listed)
			}
		})
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
