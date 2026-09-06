package effectjournal

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/cc-auto-agent/harness-core/pkg/runtime"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

func TestPostgresFreshMigrationAndAdapter(t *testing.T) {
	dsn := os.Getenv("HARNESS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("HARNESS_TEST_PG_DSN is not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	adminConfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	adminDB := stdlib.OpenDB(*adminConfig)
	if err := adminDB.PingContext(ctx); err != nil {
		_ = adminDB.Close()
		t.Fatalf("postgres ping failed: %v", err)
	}
	schema := fmt.Sprintf("effectjournal_test_%d", time.Now().UnixNano())
	if _, err := adminDB.ExecContext(ctx, "CREATE SCHEMA "+schema); err != nil {
		_ = adminDB.Close()
		t.Fatal(err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = adminDB.ExecContext(cleanupCtx, "DROP SCHEMA "+schema+" CASCADE")
		_ = adminDB.Close()
	}()

	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.RuntimeParams["search_path"] = schema
	db := stdlib.OpenDB(*config)
	defer db.Close()
	if _, err := storage.OpenSQLSessionStore(ctx, db, storage.SQLDialectPostgres); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO settings (key, value) VALUES ($1, $2)", "effect-historical-business", "preserved"); err != nil {
		t.Fatal(err)
	}
	store, err := New(db, 1)
	if err != nil {
		t.Fatal(err)
	}
	d := descriptor("pg-effect", "pg-composition", []byte{0, 255})
	if _, err := store.Record(ctx, d); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkApplied(ctx, d.ID); err != nil {
		t.Fatal(err)
	}
	rows, err := store.List(ctx, d.CompositionRevision)
	if err != nil || len(rows) != 1 || rows[0].State != runtime.EffectApplied {
		t.Fatalf("postgres rows=%#v err=%v", rows, err)
	}
	// Simulate a real v32 database with the v33 journal DDL absent, rather than
	// merely changing the version marker after opening a v33 database.
	if _, err := db.ExecContext(ctx, "DROP TABLE runtime_effects"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "DROP TABLE runtime_effect_sequences"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "UPDATE store_meta SET value='32' WHERE key='schema_version'"); err != nil {
		t.Fatal(err)
	}
	if _, err := storage.OpenSQLSessionStore(ctx, db, storage.SQLDialectPostgres); err != nil {
		t.Fatalf("postgres v32 migration: %v", err)
	}
	for _, table := range []string{"runtime_effect_sequences", "runtime_effects"} {
		var relation string
		if err := db.QueryRowContext(ctx, "SELECT to_regclass($1)", table).Scan(&relation); err != nil || relation == "" {
			t.Fatalf("migrated table %q relation=%q err=%v", table, relation, err)
		}
	}
	var preserved string
	if err := db.QueryRowContext(ctx, "SELECT value FROM settings WHERE key=$1", "effect-historical-business").Scan(&preserved); err != nil || preserved != "preserved" {
		t.Fatalf("business data=%q err=%v", preserved, err)
	}
}
