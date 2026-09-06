package effectjournal

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"testing"

	"github.com/cc-auto-agent/harness-core/pkg/runtime"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
	_ "modernc.org/sqlite"
)

func TestSQLiteFreshHistoricalReopenAndFutureRefusal(t *testing.T) {
	db := openSQLite(t)
	assertSchemaVersion(t, db, strconv.Itoa(storage.SQLSchemaVersion))
	for _, table := range []string{"runtime_effect_sequences", "runtime_effects"} {
		var found int
		if err := db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&found); err != nil || found != 1 {
			t.Fatalf("table %q found=%d err=%v", table, found, err)
		}
	}
	if _, err := storage.OpenSQLSessionStore(context.Background(), db, storage.SQLDialectSQLite); err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if _, err := db.Exec("INSERT INTO settings (key, value) VALUES (?, ?)", "effect-historical-business", "preserved"); err != nil {
		t.Fatal(err)
	}
	// Simulate a real v32 database with the later runtime DDL absent. Changing
	// only the marker after opening the current schema would not exercise the
	// upgrade DDL.
	if _, err := db.Exec("DROP TABLE runtime_effects"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("DROP TABLE runtime_effect_sequences"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("UPDATE store_meta SET value='32' WHERE key='schema_version'"); err != nil {
		t.Fatal(err)
	}
	if _, err := storage.OpenSQLSessionStore(context.Background(), db, storage.SQLDialectSQLite); err != nil {
		t.Fatalf("historical v32 upgrade: %v", err)
	}
	assertSchemaVersion(t, db, strconv.Itoa(storage.SQLSchemaVersion))
	for _, table := range []string{"runtime_effect_sequences", "runtime_effects"} {
		var found int
		if err := db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&found); err != nil || found != 1 {
			t.Fatalf("migrated table %q found=%d err=%v", table, found, err)
		}
	}
	var preserved string
	if err := db.QueryRow("SELECT value FROM settings WHERE key=?", "effect-historical-business").Scan(&preserved); err != nil || preserved != "preserved" {
		t.Fatalf("business data=%q err=%v", preserved, err)
	}
	if _, err := db.Exec("UPDATE store_meta SET value=? WHERE key='schema_version'", strconv.Itoa(storage.SQLSchemaVersion+1)); err != nil {
		t.Fatal(err)
	}
	if _, err := storage.OpenSQLSessionStore(context.Background(), db, storage.SQLDialectSQLite); err == nil {
		t.Fatal("future schema was accepted")
	}
}

func TestUniqueConstraintClassifierIsSpecific(t *testing.T) {
	for _, test := range []struct {
		name string
		text string
		want bool
	}{
		{name: "sqlite unique", text: "constraint failed: UNIQUE constraint failed: runtime_effects.effect_id", want: true},
		{name: "sqlite primary key", text: "constraint failed: PRIMARY KEY must be unique", want: true},
		{name: "postgres duplicate key", text: "duplicate key value violates unique constraint runtime_effects_pkey", want: true},
		{name: "check", text: "constraint failed: CHECK constraint failed: state", want: false},
		{name: "foreign key", text: "FOREIGN KEY constraint failed", want: false},
		{name: "not null", text: "NOT NULL constraint failed: runtime_effects.state", want: false},
		{name: "other duplicate", text: "duplicate key value violates exclusion constraint", want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := isUniqueConstraint(errors.New(test.text)); got != test.want {
				t.Fatalf("isUniqueConstraint(%q)=%v, want %v", test.text, got, test.want)
			}
		})
	}
}

func TestSQLiteRecordRoundTripOrderIsolationAndDefensiveCopy(t *testing.T) {
	store := newStore(t)
	first := descriptor("effect-b", "composition-a", []byte{0, 1, 255})
	first.Phase = runtime.EffectPhaseStage
	second := descriptor("effect-a", "composition-a", []byte("bytes"))
	second.Phase = runtime.EffectPhaseReconcile
	other := descriptor("effect-other", "composition-b", nil)
	if _, err := store.Record(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Record(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Record(context.Background(), other); err != nil {
		t.Fatal(err)
	}
	rows, err := store.List(context.Background(), "composition-a")
	if err != nil || len(rows) != 2 {
		t.Fatalf("list=%#v err=%v", rows, err)
	}
	if rows[0].Descriptor.ID != first.ID || rows[1].Descriptor.ID != second.ID {
		t.Fatalf("record order=%v,%v", rows[0].Descriptor.ID, rows[1].Descriptor.ID)
	}
	rows[0].Descriptor.Forward.Payload[0] = 99
	again, err := store.List(context.Background(), "composition-a")
	if err != nil || again[0].Descriptor.Forward.Payload[0] != 0 {
		t.Fatalf("payload was not defensive: %#v err=%v", again, err)
	}
	prior, err := store.Record(context.Background(), first)
	if err != nil || prior.State != runtime.EffectPrepared || !sameDescriptor(prior.Descriptor, first) {
		t.Fatalf("idempotent record=%#v err=%v", prior, err)
	}
	conflict := first
	conflict.Forward.Payload = []byte("different")
	if _, err := store.Record(context.Background(), conflict); !errors.Is(err, runtime.ErrEffectConflict) {
		t.Fatalf("descriptor conflict err=%v", err)
	}
}

func TestSQLiteEffectStateMatrix(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()
	prepared := descriptor("prepared", "state", nil)
	applied := descriptor("applied", "state", nil)
	unknown := descriptor("unknown", "state", nil)
	for _, item := range []runtime.EffectDescriptor{prepared, applied, unknown} {
		if _, err := store.Record(ctx, item); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.MarkApplied(ctx, applied.ID); err != nil || store.MarkApplied(ctx, applied.ID) != nil {
		t.Fatalf("applied idempotency err=%v", err)
	}
	if err := store.MarkUnknown(ctx, applied.ID); !errors.Is(err, runtime.ErrEffectConflict) {
		t.Fatalf("applied to unknown err=%v", err)
	}
	if err := store.MarkUnknown(ctx, unknown.ID); err != nil || store.MarkUnknown(ctx, unknown.ID) != nil {
		t.Fatalf("unknown idempotency err=%v", err)
	}
	if err := store.MarkReverted(ctx, unknown.ID); err != nil || store.MarkReverted(ctx, unknown.ID) != nil {
		t.Fatalf("unknown revert err=%v", err)
	}
	if err := store.MarkApplied(ctx, unknown.ID); !errors.Is(err, runtime.ErrEffectConflict) {
		t.Fatalf("reverted to applied err=%v", err)
	}
	if err := store.MarkReverted(ctx, prepared.ID); err != nil {
		t.Fatalf("prepared revert err=%v", err)
	}
	if err := store.MarkApplied(ctx, "missing"); !errors.Is(err, runtime.ErrEffectNotFound) {
		t.Fatalf("missing effect err=%v", err)
	}
}

func TestSQLiteRecordRejectsInvalidDescriptor(t *testing.T) {
	store := newStore(t)
	for _, item := range []runtime.EffectDescriptor{
		descriptor("", "invalid", nil),
		descriptor("bad..id", "invalid", nil),
		descriptor("too-large", "invalid", make([]byte, 1<<20+1)),
	} {
		if _, err := store.Record(context.Background(), item); !errors.Is(err, runtime.ErrInvalidEffect) {
			t.Fatalf("descriptor %#v err=%v", item.ID, err)
		}
	}
}

func TestSQLiteJournalHonorsCanceledContext(t *testing.T) {
	store := newStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Record(ctx, descriptor("canceled", "context", nil)); !errors.Is(err, context.Canceled) {
		t.Fatalf("record canceled err=%v", err)
	}
	if _, err := store.List(ctx, "context"); !errors.Is(err, context.Canceled) {
		t.Fatalf("list canceled err=%v", err)
	}
	if err := store.MarkApplied(ctx, "canceled"); !errors.Is(err, context.Canceled) {
		t.Fatalf("mark canceled err=%v", err)
	}
}

func TestSQLiteConcurrentSameIDAndOrdinalAllocation(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()
	const sameID = "same-effect"
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := store.Record(ctx, descriptor(string(sameID), "same", []byte("same")))
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	const count = 8
	ordinalErrs := make(chan error, count)
	for i := 0; i < count; i++ {
		id := runtime.EffectID("ordinal-" + string(rune('a'+i)))
		wg.Add(1)
		go func(id runtime.EffectID) {
			defer wg.Done()
			_, err := store.Record(ctx, descriptor(string(id), "ordered", nil))
			ordinalErrs <- err
		}(id)
	}
	wg.Wait()
	close(ordinalErrs)
	for err := range ordinalErrs {
		if err != nil {
			t.Fatalf("concurrent ordinal allocation: %v", err)
		}
	}
	rows, err := store.List(ctx, "ordered")
	if err != nil || len(rows) != count {
		t.Fatalf("ordered rows=%d err=%v", len(rows), err)
	}
	ordinals := make([]int64, count)
	for i := range rows {
		if err := store.db.QueryRow("SELECT ordinal FROM runtime_effects WHERE effect_id=?", rows[i].Descriptor.ID).Scan(&ordinals[i]); err != nil {
			t.Fatal(err)
		}
	}
	sort.Slice(ordinals, func(i, j int) bool { return ordinals[i] < ordinals[j] })
	for i, ordinal := range ordinals {
		if ordinal != int64(i+1) {
			t.Fatalf("ordinals=%v", ordinals)
		}
	}
}

func TestSQLiteConcurrentAppliedVsUnknownHasSingleWinner(t *testing.T) {
	store := newStore(t)
	d := descriptor("state-race", "state-race", nil)
	if _, err := store.Record(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, mark := range []func(context.Context, runtime.EffectID) error{store.MarkApplied, store.MarkUnknown} {
		wg.Add(1)
		go func(mark func(context.Context, runtime.EffectID) error) {
			defer wg.Done()
			<-start
			results <- mark(context.Background(), d.ID)
		}(mark)
	}
	close(start)
	wg.Wait()
	close(results)
	successes, conflicts := 0, 0
	for err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, runtime.ErrEffectConflict):
			conflicts++
		default:
			t.Fatalf("state race error: %v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("state race successes=%d conflicts=%d", successes, conflicts)
	}
	rows, err := store.List(context.Background(), d.CompositionRevision)
	if err != nil || len(rows) != 1 || (rows[0].State != runtime.EffectApplied && rows[0].State != runtime.EffectUnknown) {
		t.Fatalf("state race final rows=%#v err=%v", rows, err)
	}
}

func newStore(t *testing.T) *Store {
	db := openSQLite(t)
	store, err := New(db, 0)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func openSQLite(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "effects.db")+"?_pragma=busy_timeout%285000%29&_pragma=journal_mode%28WAL%29")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(8)
	t.Cleanup(func() { _ = db.Close() })
	if _, err := storage.OpenSQLSessionStore(context.Background(), db, storage.SQLDialectSQLite); err != nil {
		t.Fatal(err)
	}
	return db
}

func descriptor(id, composition string, payload []byte) runtime.EffectDescriptor {
	return runtime.EffectDescriptor{
		ID: runtime.EffectID(id), ModuleID: "module", ModuleRevision: runtime.Version{Major: 1},
		CompositionRevision: composition, Phase: runtime.EffectPhaseActivate,
		Forward: runtime.EffectAction{Kind: "forward", Target: "target", Payload: payload},
		Inverse: runtime.EffectAction{Kind: "inverse", Target: "target", Payload: []byte("inverse")},
	}
}

func assertSchemaVersion(t *testing.T, db *sql.DB, want string) {
	t.Helper()
	var got string
	if err := db.QueryRow("SELECT value FROM store_meta WHERE key='schema_version'").Scan(&got); err != nil || got != want {
		t.Fatalf("schema version=%q err=%v want=%q", got, err, want)
	}
}
