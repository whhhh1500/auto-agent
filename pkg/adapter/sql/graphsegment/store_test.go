package graphsegment

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	sqlkit "github.com/cc-auto-agent/harness-core/pkg/adapter/sql/sqlkit"
	execgraph "github.com/cc-auto-agent/harness-core/pkg/execution/graph"
	graph "github.com/cc-auto-agent/harness-core/pkg/extensions/graph"
	storage "github.com/cc-auto-agent/harness-core/pkg/storage"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"
)

func testAuthority(t *testing.T) (*Authority, *sql.DB) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:graphsegment-test?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	_, err = db.Exec(`CREATE TABLE graph_segment_leases (tenant_id TEXT, session_id TEXT, run_id TEXT, segment_id TEXT, holder_id TEXT, host_generation INTEGER, expires_at INTEGER, released INTEGER, created_at INTEGER, updated_at INTEGER, PRIMARY KEY (tenant_id,session_id,run_id))`)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(100, 0)
	a, err := NewWithOptions(db, sqlkit.SQLite, 10*time.Minute, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	return a, db
}

func TestAcquireConflictReleaseReacquire(t *testing.T) {
	a, _ := testAuthority(t)
	key := graph.CheckpointKey{TenantID: "t", SessionID: "s", RunID: "r"}
	g1, err := a.Acquire(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if len(g1.SegmentID) != 32 || len(g1.Lease.(*lease).holder) != 32 {
		t.Fatalf("weak ids")
	}
	if _, err = a.Acquire(context.Background(), key); !errors.Is(err, ErrLeaseBusy) {
		t.Fatalf("conflict=%v", err)
	}
	if err = g1.Lease.Release(context.Background(), execgraph.LeaseRequest{Key: key, SegmentID: g1.SegmentID, HostGeneration: g1.HostGeneration}); err != nil {
		t.Fatal(err)
	}
	if err = g1.Lease.Release(context.Background(), execgraph.LeaseRequest{Key: key, SegmentID: g1.SegmentID, HostGeneration: g1.HostGeneration}); err != nil {
		t.Fatal(err)
	}
	g2, err := a.Acquire(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if g2.HostGeneration != g1.HostGeneration+1 || g2.SegmentID == g1.SegmentID {
		t.Fatalf("generation/ids not fenced")
	}
	if err = g1.Lease.Verify(context.Background(), execgraph.LeaseRequest{Key: key, SegmentID: g1.SegmentID, HostGeneration: g1.HostGeneration}); !errors.Is(err, ErrLeaseUnavailable) {
		t.Fatalf("stale verify=%v", err)
	}
	// Releasing the same already-released lease is idempotent; it cannot
	// affect the newer owner (the Verify assertion above proves the fence).
}

func TestExpiryTakeoverAndStaleLease(t *testing.T) {
	a, db := testAuthority(t)
	key := graph.CheckpointKey{TenantID: "t", SessionID: "s", RunID: "expiry"}
	g1, err := a.Acquire(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`UPDATE graph_segment_leases SET expires_at=0 WHERE tenant_id='t' AND session_id='s' AND run_id='expiry'`); err != nil {
		t.Fatal(err)
	}
	g2, err := a.Acquire(context.Background(), key)
	if err != nil || g2.HostGeneration != g1.HostGeneration+1 || g2.SegmentID == g1.SegmentID {
		t.Fatalf("takeover=%#v err=%v", g2, err)
	}
	request := execgraph.LeaseRequest{Key: key, SegmentID: g1.SegmentID, HostGeneration: g1.HostGeneration}
	if err = g1.Lease.Verify(context.Background(), request); !errors.Is(err, ErrLeaseUnavailable) {
		t.Fatalf("stale verify=%v", err)
	}
	if err = g1.Lease.Release(context.Background(), request); !errors.Is(err, ErrLeaseUnavailable) {
		t.Fatalf("stale release=%v", err)
	}
	if err = g2.Lease.Verify(context.Background(), execgraph.LeaseRequest{Key: key, SegmentID: g2.SegmentID, HostGeneration: g2.HostGeneration}); err != nil {
		t.Fatalf("new verify=%v", err)
	}
}

func TestLeaseWrongIdentityAndCancellation(t *testing.T) {
	a, _ := testAuthority(t)
	key := graph.CheckpointKey{TenantID: "t", SessionID: "s", RunID: "r"}
	g, err := a.Acquire(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	bad := execgraph.LeaseRequest{Key: key, SegmentID: "wrong", HostGeneration: g.HostGeneration}
	if err := g.Lease.Verify(context.Background(), bad); !errors.Is(err, ErrLeaseInvalid) {
		t.Fatalf("wrong=%v", err)
	}
	if err := g.Lease.Verify(context.Background(), execgraph.LeaseRequest{Key: graph.CheckpointKey{TenantID: "other", SessionID: "s", RunID: "r"}, SegmentID: g.SegmentID, HostGeneration: g.HostGeneration}); !errors.Is(err, ErrLeaseInvalid) {
		t.Fatalf("wrong key=%v", err)
	}
	if err := g.Lease.Verify(context.Background(), execgraph.LeaseRequest{Key: key, SegmentID: g.SegmentID, HostGeneration: g.HostGeneration + 1}); !errors.Is(err, ErrLeaseInvalid) {
		t.Fatalf("wrong generation=%v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := a.Acquire(ctx, graph.CheckpointKey{TenantID: "t", SessionID: "s", RunID: "other"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel=%v", err)
	}
}

func TestDifferentKeysParallel(t *testing.T) {
	a, _ := testAuthority(t)
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := a.Acquire(context.Background(), graph.CheckpointKey{TenantID: "t", SessionID: "s", RunID: string(rune('a' + i))})
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestDatabaseFailureIsStableAndRedacted(t *testing.T) {
	a, db := testAuthority(t)
	_ = db.Close()
	_, err := a.Acquire(context.Background(), graph.CheckpointKey{TenantID: "t", SessionID: "s", RunID: "closed"})
	if !errors.Is(err, ErrLeaseUnavailable) || err.Error() != ErrLeaseUnavailable.Error() {
		t.Fatalf("err=%v", err)
	}
	if _, err := a.Acquire(context.Background(), graph.CheckpointKey{TenantID: "t", SessionID: "s", RunID: "closed2"}); !errors.Is(err, ErrLeaseUnavailable) {
		t.Fatalf("repeat=%v", err)
	}
}

func TestSameKeyConcurrentOnlyOneSucceeds(t *testing.T) {
	a, _ := testAuthority(t)
	key := graph.CheckpointKey{TenantID: "t", SessionID: "s", RunID: "concurrent"}
	var wg sync.WaitGroup
	results := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := a.Acquire(context.Background(), key); results <- err }()
	}
	wg.Wait()
	close(results)
	success := 0
	for err := range results {
		if err == nil {
			success++
		} else if !errors.Is(err, ErrLeaseBusy) {
			t.Fatalf("non-winning acquire=%v", err)
		}
	}
	if success != 1 {
		t.Fatalf("successes=%d", success)
	}
}

func TestReleaseSQLFailureDoesNotMarkLocalReleased(t *testing.T) {
	a, db := testAuthority(t)
	key := graph.CheckpointKey{TenantID: "t", SessionID: "s", RunID: "release-failure"}
	g, err := a.Acquire(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`DROP TABLE graph_segment_leases`); err != nil {
		t.Fatal(err)
	}
	request := execgraph.LeaseRequest{Key: key, SegmentID: g.SegmentID, HostGeneration: g.HostGeneration}
	if err = g.Lease.Release(context.Background(), request); !errors.Is(err, ErrLeaseUnavailable) {
		t.Fatalf("release=%v", err)
	}
	if _, err = db.Exec(`CREATE TABLE graph_segment_leases (tenant_id TEXT, session_id TEXT, run_id TEXT, segment_id TEXT, holder_id TEXT, host_generation INTEGER, expires_at INTEGER, released INTEGER, created_at INTEGER, updated_at INTEGER, PRIMARY KEY (tenant_id,session_id,run_id))`); err != nil {
		t.Fatal(err)
	}
	// The local flag was not committed on the failed SQL operation; the second
	// call reaches SQL again rather than being silently treated as idempotent.
	if err = g.Lease.Release(context.Background(), request); !errors.Is(err, ErrLeaseUnavailable) {
		t.Fatalf("recovered release=%v", err)
	}
}

func TestRandomIDsAreStrongAndUnique(t *testing.T) {
	a, _ := testAuthority(t)
	seen := map[string]bool{}
	for i := 0; i < 256; i++ {
		g, err := a.Acquire(context.Background(), graph.CheckpointKey{TenantID: "t", SessionID: "s", RunID: string(rune(1000 + i))})
		if err != nil {
			t.Fatal(err)
		}
		if len(g.SegmentID) != 32 || seen[g.SegmentID] {
			t.Fatalf("bad/duplicate id")
		}
		seen[g.SegmentID] = true
		if err := g.Lease.Release(context.Background(), execgraph.LeaseRequest{Key: graph.CheckpointKey{TenantID: "t", SessionID: "s", RunID: string(rune(1000 + i))}, SegmentID: g.SegmentID, HostGeneration: g.HostGeneration}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestInvalidConstructionAndClockPanic(t *testing.T) {
	var nilLease *lease
	if !errors.Is(nilLease.Verify(context.Background(), execgraph.LeaseRequest{}), ErrLeaseInvalid) {
		t.Fatal("nil verify did not fail closed")
	}
	if !errors.Is(nilLease.Release(context.Background(), execgraph.LeaseRequest{}), ErrLeaseInvalid) {
		t.Fatal("nil release did not fail closed")
	}
	if _, err := New(nil, sqlkit.SQLite); !errors.Is(err, ErrLeaseInvalid) {
		t.Fatalf("nil db=%v", err)
	}
	if _, err := NewWithOptions(&sql.DB{}, sqlkit.SQLite, 10*time.Minute, nil); !errors.Is(err, ErrLeaseInvalid) {
		t.Fatalf("nil clock=%v", err)
	}
	db, err := sql.Open("sqlite", "file:panic-clock?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close db: %v", err)
		}
	})
	if _, err = db.Exec(`CREATE TABLE graph_segment_leases (tenant_id TEXT, session_id TEXT, run_id TEXT, segment_id TEXT, holder_id TEXT, host_generation INTEGER, expires_at INTEGER, released INTEGER, created_at INTEGER, updated_at INTEGER, PRIMARY KEY (tenant_id,session_id,run_id))`); err != nil {
		t.Fatal(err)
	}
	a, err := NewWithOptions(db, sqlkit.SQLite, 10*time.Minute, func() time.Time { panic("secret clock") })
	if err != nil {
		t.Fatal(err)
	}
	_, err = a.Acquire(context.Background(), graph.CheckpointKey{TenantID: "t", SessionID: "s", RunID: "panic"})
	if !errors.Is(err, ErrLeaseUnavailable) || strings.Contains(err.Error(), "secret") {
		t.Fatalf("clock=%v", err)
	}
}

func TestConcurrentReleaseIsIdempotent(t *testing.T) {
	a, _ := testAuthority(t)
	key := graph.CheckpointKey{TenantID: "t", SessionID: "s", RunID: "release-concurrent"}
	g, err := a.Acquire(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	req := execgraph.LeaseRequest{Key: key, SegmentID: g.SegmentID, HostGeneration: g.HostGeneration}
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- g.Lease.Release(context.Background(), req) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("release=%v", err)
		}
	}
}

func TestPostgresGraphSegmentSemantic(t *testing.T) {
	dsn := os.Getenv("HARNESS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("HARNESS_TEST_PG_DSN not configured")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close db: %v", err)
		}
	})
	db.SetMaxOpenConns(1)
	schema := fmt.Sprintf("graph_segment_semantic_%d", time.Now().UnixNano())
	if _, err = db.Exec(`CREATE SCHEMA ` + schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := db.Exec(`DROP SCHEMA ` + schema + ` CASCADE`); err != nil {
			t.Errorf("drop schema: %v", err)
		}
	})
	if _, err = db.Exec(`SET search_path TO ` + schema); err != nil {
		t.Fatal(err)
	}
	if _, err = storage.OpenSQLSessionStore(context.Background(), db, storage.SQLDialectPostgres); err != nil {
		t.Fatal(err)
	}
	a, err := New(db, sqlkit.Postgres)
	if err != nil {
		t.Fatal(err)
	}
	key := graph.CheckpointKey{TenantID: "pg-test", SessionID: "pg-test", RunID: "semantic"}
	g, err := a.Acquire(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = a.Acquire(context.Background(), key); !errors.Is(err, ErrLeaseBusy) {
		t.Fatalf("conflict=%v", err)
	}
	if err = g.Lease.Release(context.Background(), execgraph.LeaseRequest{Key: key, SegmentID: g.SegmentID, HostGeneration: g.HostGeneration}); err != nil {
		t.Fatal(err)
	}
	if err = g.Lease.Verify(context.Background(), execgraph.LeaseRequest{Key: key, SegmentID: g.SegmentID, HostGeneration: g.HostGeneration}); !errors.Is(err, ErrLeaseUnavailable) {
		t.Fatalf("old verify=%v", err)
	}
	g2, err := a.Acquire(context.Background(), key)
	if err != nil || g2.HostGeneration != g.HostGeneration+1 {
		t.Fatalf("reacquire=%#v err=%v", g2, err)
	}
	if err = g2.Lease.Verify(context.Background(), execgraph.LeaseRequest{Key: key, SegmentID: g2.SegmentID, HostGeneration: g2.HostGeneration}); err != nil {
		t.Fatalf("new verify=%v", err)
	}
}
