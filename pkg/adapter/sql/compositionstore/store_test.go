package compositionstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cc-auto-agent/harness-core/pkg/adapter/sql/sqlkit"
	"github.com/cc-auto-agent/harness-core/pkg/runtime"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
	_ "modernc.org/sqlite"
)

func TestNewValidatesDatabaseAndDialect(t *testing.T) {
	if _, err := New(nil, sqlkit.SQLite); err == nil {
		t.Fatal("nil database was accepted")
	}
	db := openSQLite(t)
	if _, err := New(db, sqlkit.Dialect(99)); err == nil {
		t.Fatal("unknown dialect was accepted")
	}
}

func TestSQLiteLoadCompareAndSwapAndDefensiveCopy(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()
	if state, found, err := store.Load(ctx); err != nil || found || state.StoreRevision != "" {
		t.Fatalf("initial load state=%#v found=%t err=%v", state, found, err)
	}
	first := validState("revision-1")
	if err := store.CompareAndSwap(ctx, "", first); err != nil {
		t.Fatal(err)
	}
	loaded, found, err := store.Load(ctx)
	if err != nil || !found || !reflect.DeepEqual(loaded, first.Clone()) {
		t.Fatalf("loaded=%#v found=%t err=%v", loaded, found, err)
	}
	loaded.Desired[0].ID = "mutated"
	again, found, err := store.Load(ctx)
	if err != nil || !found || again.Desired[0].ID != first.Desired[0].ID {
		t.Fatalf("load was not defensive: %#v found=%t err=%v", again, found, err)
	}
	second := validState("revision-2")
	if err := store.CompareAndSwap(ctx, first.StoreRevision, second); err != nil {
		t.Fatal(err)
	}
	if err := store.CompareAndSwap(ctx, first.StoreRevision, validState("revision-3")); !errors.Is(err, runtime.ErrCompositionConflict) {
		t.Fatalf("stale update err=%v", err)
	}
	if err := store.CompareAndSwap(ctx, second.StoreRevision, second); !errors.Is(err, runtime.ErrCompositionConflict) {
		t.Fatalf("same revision update err=%v", err)
	}
	if err := store.CompareAndSwap(ctx, "", validState("revision-3")); !errors.Is(err, runtime.ErrCompositionConflict) {
		t.Fatalf("duplicate create err=%v", err)
	}
}

func TestSQLiteLoadFailsClosedForCorruptOrUnsafeRows(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()
	state := validState("revision-1")
	if err := store.CompareAndSwap(ctx, "", state); err != nil {
		t.Fatal(err)
	}
	validJSON, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name     string
		revision string
		json     string
	}{
		{name: "corrupt json", revision: state.StoreRevision, json: `{"store_revision":`},
		{name: "unknown credential-shaped field", revision: state.StoreRevision, json: `{"store_revision":"revision-1","host_api_version":{"major":1,"minor":0,"patch":0},"desired":[],"compositions":[],"api_key":"sentinel"}`},
		{name: "column revision mismatch", revision: "other-revision", json: string(validJSON)},
		{name: "invalid state", revision: state.StoreRevision, json: `{"store_revision":"revision-1","host_api_version":{"major":-1,"minor":0,"patch":0},"desired":[],"compositions":[]}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := store.db.ExecContext(ctx, `UPDATE runtime_host_state SET revision = ?, state_json = ? WHERE host_key = 'module_host'`, test.revision, test.json); err != nil {
				t.Fatal(err)
			}
			if _, _, err := store.Load(ctx); !errors.Is(err, runtime.ErrInvalidComposition) {
				t.Fatalf("load err=%v", err)
			}
			if _, err := store.db.ExecContext(ctx, `UPDATE runtime_host_state SET revision = ?, state_json = ? WHERE host_key = 'module_host'`, state.StoreRevision, string(validJSON)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSQLiteStateJSONSizeIsBounded(t *testing.T) {
	store := newStore(t)
	if _, err := store.db.Exec(`INSERT INTO runtime_host_state (host_key, revision, state_json, updated_at) VALUES (NULL, ?, ?, ?)`, "null-key", "{}", 1); err == nil {
		t.Fatal("null host key was accepted")
	}
	if _, err := store.db.Exec(`INSERT INTO runtime_host_state (host_key, revision, state_json, updated_at) VALUES (?, ?, ?, ?)`, "other-host", "other-key", "{}", 1); err == nil {
		t.Fatal("noncanonical host key was accepted")
	}
	over := strings.Repeat("x", MaxStateJSONBytes+1)
	if _, err := decodeState(over); !errors.Is(err, runtime.ErrInvalidComposition) {
		t.Fatalf("oversized decode err=%v", err)
	}
	if _, err := store.db.Exec(`INSERT INTO runtime_host_state (host_key, revision, state_json, updated_at) VALUES ('module_host', ?, ?, ?)`, "oversized", over, 1); err == nil {
		t.Fatal("oversized database row was accepted")
	}
}

func TestSQLiteConcurrentStoresHaveSingleCASWinner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "composition-state.db")
	firstDB := openSQLitePath(t, path)
	secondDB := openSQLitePath(t, path)
	first, err := New(firstDB, sqlkit.SQLite)
	if err != nil {
		t.Fatal(err)
	}
	second, err := New(secondDB, sqlkit.SQLite)
	if err != nil {
		t.Fatal(err)
	}
	initial := validState("initial")
	if err := first.CompareAndSwap(context.Background(), "", initial); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	errs := make(chan error, 2)
	var group sync.WaitGroup
	for index, store := range []*Store{first, second} {
		group.Add(1)
		go func(store *Store, revision string) {
			defer group.Done()
			<-start
			errs <- store.CompareAndSwap(context.Background(), initial.StoreRevision, validState(revision))
		}(store, "next-"+string(rune('a'+index)))
	}
	close(start)
	group.Wait()
	close(errs)
	successes, conflicts := 0, 0
	for err := range errs {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, runtime.ErrCompositionConflict):
			conflicts++
		default:
			t.Fatalf("concurrent cas err=%v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("concurrent cas successes=%d conflicts=%d", successes, conflicts)
	}
}

func TestSQLiteHostOwnershipClaimsAreGenerationFenced(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()
	first, err := store.ClaimHostOwnership(ctx, "host-first", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimHostOwnership(ctx, "host-second", time.Second); !errors.Is(err, runtime.ErrHostOwnershipConflict) {
		t.Fatalf("live claim error=%v", err)
	}
	if err := store.ReleaseHostOwnership(ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := store.ReleaseHostOwnership(ctx, first); err != nil {
		t.Fatalf("idempotent release error=%v", err)
	}
	second, err := store.ClaimHostOwnership(ctx, "host-second", time.Second)
	if err != nil || second.Generation != first.Generation+1 {
		t.Fatalf("second claim=%+v err=%v", second, err)
	}
	if _, err := store.RenewHostOwnership(ctx, first, time.Second); !errors.Is(err, runtime.ErrHostOwnershipLost) {
		t.Fatalf("stale renew error=%v", err)
	}
}

func TestPostgresQueriesUseOrdinalPlaceholders(t *testing.T) {
	store := &Store{dialect: sqlkit.Postgres}
	if query := store.bind(selectState); strings.Contains(query, "?") {
		t.Fatalf("postgres parameter-free query was rebound incorrectly: %q", query)
	}
	for _, query := range []string{store.bind(insertState), store.bind(updateState)} {
		if strings.Contains(query, "?") || !strings.Contains(query, "$1") {
			t.Fatalf("postgres query was not rebound: %q", query)
		}
	}
}

func newStore(t *testing.T) *Store {
	t.Helper()
	store, err := New(openSQLite(t), sqlkit.SQLite)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func openSQLite(t *testing.T) *sql.DB {
	t.Helper()
	return openSQLitePath(t, filepath.Join(t.TempDir(), "composition-state.db"))
}

func openSQLitePath(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout%285000%29&_pragma=journal_mode%28WAL%29")
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

func validState(revision string) runtime.DurableHostState {
	return runtime.DurableHostState{
		StoreRevision:  revision,
		HostAPIVersion: runtime.Version{Major: 1},
		Desired: []runtime.ModuleManifest{{
			ID:            "module-a",
			Version:       runtime.Version{Major: 1},
			CompatibleAPI: runtime.VersionRange{Min: runtime.Version{Major: 1}},
		}},
	}
}
