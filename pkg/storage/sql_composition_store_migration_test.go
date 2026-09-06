package storage

import (
	"context"
	"database/sql"
	"strconv"
	"testing"
)

func TestSQLiteRuntimeHostStateSchemaV35FreshHistoricalRepeatAndFutureRefusal(t *testing.T) {
	testRuntimeHostStateSchemaV35(t, SQLDialectSQLite, newTestSQLStore(t).db)
}

func TestPostgresRuntimeHostStateSchemaV35FreshHistoricalRepeatAndFutureRefusal(t *testing.T) {
	testRuntimeHostStateSchemaV35(t, SQLDialectPostgres, newPostgresTestDB(t))
}

func testRuntimeHostStateSchemaV35(t *testing.T, dialect SQLDialect, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err != nil {
		t.Fatalf("open fresh v35 schema: %v", err)
	}
	assertRuntimeHostStateTable(t, ctx, db, dialect)
	assertRuntimeOwnershipAndFenceTables(t, ctx, db, dialect)
	if _, err := db.ExecContext(ctx, sqlQuery{"INSERT INTO settings (key, value) VALUES (?, ?)"}.bind(dialect), "v35-business-data", "preserved"); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"runtime_fence_journal", "runtime_host_ownership"} {
		if _, err := db.ExecContext(ctx, "DROP TABLE "+table); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(ctx, sqlUpdateMetaRow.bind(dialect), "34"); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err != nil {
		t.Fatalf("v34 to v35 migration: %v", err)
	}
	assertRuntimeOwnershipAndFenceTables(t, ctx, db, dialect)
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err != nil {
		t.Fatalf("repeat v35 open: %v", err)
	}
	var value string
	if err := db.QueryRowContext(ctx, sqlQuery{"SELECT value FROM settings WHERE key = ?"}.bind(dialect), "v35-business-data").Scan(&value); err != nil || value != "preserved" {
		t.Fatalf("preserved business data=%q err=%v", value, err)
	}
	var version string
	if err := db.QueryRowContext(ctx, sqlSelectMetaRow.bind(dialect)).Scan(&version); err != nil || version != strconv.Itoa(SQLSchemaVersion) {
		t.Fatalf("schema version=%q err=%v", version, err)
	}
	for _, table := range []string{"runtime_fence_journal", "runtime_host_ownership"} {
		if _, err := db.ExecContext(ctx, "DROP TABLE "+table); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(ctx, sqlUpdateMetaRow.bind(dialect), strconv.Itoa(SQLSchemaVersion+1)); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err == nil {
		t.Fatal("future v35 marker was accepted before v35 DDL")
	}
	for _, table := range []string{"runtime_host_ownership", "runtime_fence_journal"} {
		found, err := sqlTableExists(ctx, db, dialect, table)
		if err != nil {
			t.Fatalf("future-version refusal table check %s: %v", table, err)
		}
		if found {
			t.Fatalf("future-version refusal executed v35 DDL for %s", table)
		}
	}
}

func assertRuntimeHostStateTable(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect) {
	t.Helper()
	found, err := sqlTableExists(ctx, db, dialect, "runtime_host_state")
	if err != nil || !found {
		t.Fatalf("runtime_host_state exists=%t err=%v", found, err)
	}
}

func assertRuntimeOwnershipAndFenceTables(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect) {
	t.Helper()
	for _, table := range []string{"runtime_host_ownership", "runtime_fence_journal"} {
		found, err := sqlTableExists(ctx, db, dialect, table)
		if err != nil || !found {
			t.Fatalf("%s exists=%t err=%v", table, found, err)
		}
	}
}
