package storage

import (
	"context"
	"database/sql"
	"strconv"
	"testing"
)

func TestSQLiteGraphCheckpointSchemaV37FreshHistoricalRepeatAndFutureRefusal(t *testing.T) {
	testGraphCheckpointSchemaV37(t, SQLDialectSQLite, newTestSQLStore(t).db)
}

func TestPostgresGraphCheckpointSchemaV37FreshHistoricalRepeatAndFutureRefusal(t *testing.T) {
	testGraphCheckpointSchemaV37(t, SQLDialectPostgres, newPostgresTestDB(t))
}

func testGraphCheckpointSchemaV37(t *testing.T, dialect SQLDialect, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err != nil {
		t.Fatalf("open fresh v37 schema: %v", err)
	}
	assertGraphCheckpointTables(t, ctx, db, dialect)
	if _, err := db.ExecContext(ctx, sqlQuery{"INSERT INTO settings (key, value) VALUES (?, ?)"}.bind(dialect), "v37-business-data", "preserved"); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"graph_transitions", "graph_checkpoints"} {
		if _, err := db.ExecContext(ctx, "DROP TABLE "+table); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(ctx, sqlUpdateMetaRow.bind(dialect), "36"); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err != nil {
		t.Fatalf("v36 to v37 migration: %v", err)
	}
	assertGraphCheckpointTables(t, ctx, db, dialect)
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err != nil {
		t.Fatalf("repeat v37 open: %v", err)
	}
	var value, version string
	if err := db.QueryRowContext(ctx, sqlQuery{"SELECT value FROM settings WHERE key = ?"}.bind(dialect), "v37-business-data").Scan(&value); err != nil || value != "preserved" {
		t.Fatalf("business data=%q err=%v", value, err)
	}
	if err := db.QueryRowContext(ctx, sqlSelectMetaRow.bind(dialect)).Scan(&version); err != nil || version != strconv.Itoa(SQLSchemaVersion) {
		t.Fatalf("version=%q err=%v", version, err)
	}
	for _, table := range []string{"graph_transitions", "graph_checkpoints"} {
		if _, err := db.ExecContext(ctx, "DROP TABLE "+table); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(ctx, sqlUpdateMetaRow.bind(dialect), strconv.Itoa(SQLSchemaVersion+1)); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err == nil {
		t.Fatal("future marker accepted")
	}
	for _, table := range []string{"graph_checkpoints", "graph_transitions"} {
		found, err := sqlTableExists(ctx, db, dialect, table)
		if err != nil || found {
			t.Fatalf("future refusal table=%s found=%t err=%v", table, found, err)
		}
	}
}

func assertGraphCheckpointTables(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect) {
	t.Helper()
	for _, table := range []string{"graph_checkpoints", "graph_transitions"} {
		found, err := sqlTableExists(ctx, db, dialect, table)
		if err != nil || !found {
			t.Fatalf("%s exists=%t err=%v", table, found, err)
		}
	}
}
