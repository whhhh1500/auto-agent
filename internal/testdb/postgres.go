// Package testdb owns isolated PostgreSQL fixtures for integration tests.
package testdb

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// Postgres creates one disposable schema. Each call to the returned opener
// creates an independent pool using that schema. Cleanup closes all pools
// before dropping only the schema allocated by this fixture.
func Postgres(t testing.TB) func() *sql.DB {
	t.Helper()
	dsn := os.Getenv("HARNESS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("HARNESS_TEST_PG_DSN is not configured")
	}
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal("invalid PostgreSQL test configuration")
	}
	admin := stdlib.OpenDB(*config)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := admin.PingContext(ctx); err != nil {
		_ = admin.Close()
		t.Fatal("PostgreSQL test database is unavailable")
	}
	var entropy [12]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		_ = admin.Close()
		t.Fatal(err)
	}
	schema := "harness_acceptance_" + hex.EncodeToString(entropy[:])
	if _, err := admin.ExecContext(ctx, "CREATE SCHEMA "+schema); err != nil {
		_ = admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := admin.ExecContext(ctx, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Errorf("drop owned PostgreSQL test schema: %v", err)
		}
		_ = admin.Close()
	})
	config.RuntimeParams["search_path"] = schema
	return func() *sql.DB {
		t.Helper()
		db := stdlib.OpenDB(*config.Copy())
		db.SetMaxOpenConns(12)
		t.Cleanup(func() { _ = db.Close() })
		return db
	}
}
