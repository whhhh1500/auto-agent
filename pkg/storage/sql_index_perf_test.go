package storage

import (
	"context"
	"strconv"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

// TestSQLiteHighFrequencyIndexPlans records the evidence for the two
// composite predicates that were previously absent. Other audited paths use
// an existing primary/composite index and are intentionally not duplicated.
func TestSQLiteHighFrequencyIndexPlans(t *testing.T) {
	s := newTestSQLStore(t)
	ctx := context.Background()
	cases := []struct {
		name, query, want string
	}{
		{"approval pending by run", "EXPLAIN QUERY PLAN SELECT id FROM approval_requests WHERE run_id = ? AND status = 'pending'", "approval_requests_run_status"},
		{"stale running controls", "EXPLAIN QUERY PLAN SELECT run_id FROM run_control WHERE status = 'running' AND updated_at < ?", "run_control_stale"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rows, err := s.db.QueryContext(ctx, tc.query, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			var detail string
			found := false
			for rows.Next() {
				var id, parent, unused int
				if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
					t.Fatal(err)
				}
				if strings.Contains(detail, tc.want) {
					found = true
				}
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if !found {
				t.Fatalf("plan did not use %s", tc.want)
			}
		})
	}
}

func TestSQLiteSchemaV39ToV40IndexesAndReopen(t *testing.T) {
	store := newTestSQLStore(t)
	db := store.db
	defer db.Close()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `DROP INDEX IF EXISTS approval_requests_run_status; DROP INDEX IF EXISTS run_control_stale; UPDATE store_meta SET value = '39' WHERE key = 'schema_version'`); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectSQLite); err != nil {
		t.Fatal(err)
	}
	assertIndex := func(name string) {
		var count int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name=?", name).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("index %s count=%d", name, count)
		}
	}
	assertIndex("approval_requests_run_status")
	assertIndex("run_control_stale")
	var version string
	if err := db.QueryRowContext(ctx, sqlSelectMetaRow.bind(SQLDialectSQLite)).Scan(&version); err != nil || version != strconv.Itoa(SQLSchemaVersion) {
		t.Fatalf("schema version=%q err=%v", version, err)
	}
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectSQLite); err != nil {
		t.Fatal(err)
	}
	assertIndex("approval_requests_run_status")
	assertIndex("run_control_stale")
}

func TestPostgresSchemaV40Indexes(t *testing.T) {
	db := newPostgresTestDB(t)
	db.SetMaxOpenConns(1)
	ctx := context.Background()
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"approval_requests_run_status", "run_control_stale"} {
		var count int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pg_indexes WHERE schemaname = current_schema() AND indexname = $1`, name).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("index %s count=%d", name, count)
		}
	}
	// The temporary schema is intentionally empty. Disable sequential scans
	// only for this plan assertion so the planner exposes the eligible index;
	// the SQL predicates remain the production queries and no runtime setting
	// is changed outside this transaction.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "SET LOCAL enable_seqscan = off"); err != nil {
		t.Fatal(err)
	}
	plans := []struct{ query, index string }{
		{"EXPLAIN SELECT id FROM approval_requests WHERE run_id = $1 AND status = 'pending'", "approval_requests_run_status"},
		{"EXPLAIN SELECT run_id FROM run_control WHERE status = 'running' AND updated_at < $1", "run_control_stale"},
	}
	for _, plan := range plans {
		arg := any("run-1")
		if strings.Contains(plan.index, "run_control") {
			arg = int64(1)
		}
		rows, err := tx.QueryContext(ctx, plan.query, arg)
		if err != nil {
			t.Fatal(err)
		}
		used := false
		for rows.Next() {
			var detail string
			if err := rows.Scan(&detail); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			if strings.Contains(detail, plan.index) {
				used = true
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		rows.Close()
		if !used {
			t.Fatalf("postgres plan did not use %s", plan.index)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var version string
	if err := db.QueryRowContext(ctx, sqlSelectMetaRow.bind(SQLDialectPostgres)).Scan(&version); err != nil || version != strconv.Itoa(SQLSchemaVersion) {
		t.Fatalf("schema version=%q err=%v", version, err)
	}
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres); err != nil {
		t.Fatal(err)
	}
}
