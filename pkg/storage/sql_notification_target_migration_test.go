package storage

import (
	"context"
	"database/sql"
	"strconv"
	"testing"
)

func TestSQLiteNotificationTargetSchemaV38FreshRepeatAndUpgrade(t *testing.T) {
	testNotificationTargetSchemaV38(t, SQLDialectSQLite, newTestSQLStore(t).db)
}

func TestPostgresNotificationTargetSchemaV38FreshRepeatAndUpgrade(t *testing.T) {
	testNotificationTargetSchemaV38(t, SQLDialectPostgres, newPostgresTestDB(t))
}

func testNotificationTargetSchemaV38(t *testing.T, dialect SQLDialect, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err != nil {
		t.Fatalf("open schema: %v", err)
	}
	found, err := sqlTableExists(ctx, db, dialect, "notification_targets")
	if err != nil || !found {
		t.Fatalf("notification_targets exists=%t err=%v", found, err)
	}
	if _, err := db.ExecContext(ctx, "DROP TABLE notification_targets"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, sqlUpdateMetaRow.bind(dialect), "37"); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err != nil {
		t.Fatalf("v37 to v38 migration: %v", err)
	}
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err != nil {
		t.Fatalf("repeat v38 open: %v", err)
	}
	var version string
	if err := db.QueryRowContext(ctx, sqlSelectMetaRow.bind(dialect)).Scan(&version); err != nil || version != strconv.Itoa(SQLSchemaVersion) {
		t.Fatalf("schema version=%q err=%v", version, err)
	}
}
