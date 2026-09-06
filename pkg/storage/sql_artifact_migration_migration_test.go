package storage

import (
	"context"
	"database/sql"
	"strconv"
	"testing"
)

func TestSQLiteArtifactMigrationSchemaV36FreshHistoricalRepeatAndFutureRefusal(t *testing.T) {
	testArtifactMigrationSchemaV36(t, SQLDialectSQLite, newTestSQLStore(t).db)
}

func TestPostgresArtifactMigrationSchemaV36FreshHistoricalRepeatAndFutureRefusal(t *testing.T) {
	testArtifactMigrationSchemaV36(t, SQLDialectPostgres, newPostgresTestDB(t))
}

func testArtifactMigrationSchemaV36(t *testing.T, dialect SQLDialect, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err != nil {
		t.Fatalf("open fresh v36 schema: %v", err)
	}
	assertArtifactMigrationTables(t, ctx, db, dialect)
	if _, err := db.ExecContext(ctx, sqlQuery{"INSERT INTO settings (key, value) VALUES (?, ?)"}.bind(dialect), "v36-business-data", "preserved"); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"artifact_migration_mutations", "artifact_migrations"} {
		if _, err := db.ExecContext(ctx, "DROP TABLE "+table); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(ctx, sqlUpdateMetaRow.bind(dialect), "35"); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err != nil {
		t.Fatalf("v35 to v36 migration: %v", err)
	}
	assertArtifactMigrationTables(t, ctx, db, dialect)
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err != nil {
		t.Fatalf("repeat v36 open: %v", err)
	}
	var value, version string
	if err := db.QueryRowContext(ctx, sqlQuery{"SELECT value FROM settings WHERE key = ?"}.bind(dialect), "v36-business-data").Scan(&value); err != nil || value != "preserved" {
		t.Fatalf("preserved business data=%q err=%v", value, err)
	}
	if err := db.QueryRowContext(ctx, sqlSelectMetaRow.bind(dialect)).Scan(&version); err != nil || version != strconv.Itoa(SQLSchemaVersion) {
		t.Fatalf("schema version=%q err=%v", version, err)
	}
	for _, table := range []string{"artifact_migration_mutations", "artifact_migrations"} {
		if _, err := db.ExecContext(ctx, "DROP TABLE "+table); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(ctx, sqlUpdateMetaRow.bind(dialect), strconv.Itoa(SQLSchemaVersion+1)); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLSessionStore(ctx, db, dialect); err == nil {
		t.Fatal("future v36 marker was accepted before v36 DDL")
	}
	assertArtifactMigrationTablesAbsent(t, ctx, db, dialect)
}

func assertArtifactMigrationTables(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect) {
	t.Helper()
	for _, table := range []string{"artifact_migrations", "artifact_migration_mutations"} {
		found, err := sqlTableExists(ctx, db, dialect, table)
		if err != nil || !found {
			t.Fatalf("%s exists=%t err=%v", table, found, err)
		}
	}
}

func assertArtifactMigrationTablesAbsent(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect) {
	t.Helper()
	for _, table := range []string{"artifact_migrations", "artifact_migration_mutations"} {
		found, err := sqlTableExists(ctx, db, dialect, table)
		if err != nil {
			t.Fatal(err)
		}
		if found {
			t.Fatalf("future-version refusal executed v36 DDL for %s", table)
		}
	}
}
