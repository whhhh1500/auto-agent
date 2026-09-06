package main

import (
	"context"
	"database/sql"
	"testing"

	sqlsettings "github.com/cc-auto-agent/harness-core/pkg/adapter/sql/settings"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
)

func TestConfigureStorageDefaultsToEmbeddedBackends(t *testing.T) {
	ctx, sessions, settings := newStorageBootstrapDependencies(t)
	lookups := 0
	configured, err := configureStorage(ctx, t.TempDir(), settings, sessions, func(string) (string, bool) {
		lookups++
		return "", false
	})
	if err != nil {
		t.Fatal(err)
	}
	if configured.Sessions != sessions {
		t.Fatalf("sessions = %T; want embedded SQL store %T", configured.Sessions, sessions)
	}
	if configured.Resources == nil || configured.EmbeddedResources == nil {
		t.Fatalf("resources=%T embedded=%T; want configured embedded resources", configured.Resources, configured.EmbeddedResources)
	}
	if configured.UseCases == nil {
		t.Fatal("storage configuration use cases were not constructed")
	}
	if configured.SessionObjects != nil {
		t.Fatalf("session objects = %T; want nil for embedded sessions", configured.SessionObjects)
	}
	if lookups == 0 {
		t.Fatal("legacy environment was not consulted after both authoritative rows were absent")
	}
}

func TestConfigureStorageDoesNotReadLegacyEnvironmentWhenRowsPresent(t *testing.T) {
	ctx, sessions, settings := newStorageBootstrapDependencies(t)
	for _, key := range []string{"storage.resources", "storage.sessions"} {
		if err := settings.SetSetting(ctx, key, `{"type":"embedded"}`); err != nil {
			t.Fatalf("seed %s: %v", key, err)
		}
	}
	lookups := 0
	configured, err := configureStorage(ctx, t.TempDir(), settings, sessions, func(string) (string, bool) {
		lookups++
		return "legacy configuration must remain unread", true
	})
	if err != nil {
		t.Fatal(err)
	}
	if lookups != 0 {
		t.Fatalf("legacy environment was read %d times despite authoritative database rows", lookups)
	}
	if configured.Sessions != sessions || configured.SessionObjects != nil {
		t.Fatalf("configured storage = %#v; want embedded sessions without object backend", configured)
	}
}

func newStorageBootstrapDependencies(t *testing.T) (context.Context, *storage.SQLSessionStore, *sqlsettings.Store) {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	sessions, err := storage.OpenSQLSessionStore(ctx, db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	settings, err := sqlsettings.New(sqlsettings.Options{DB: db, Dialect: storage.SQLDialectSQLite})
	if err != nil {
		t.Fatal(err)
	}
	return ctx, sessions, settings
}
