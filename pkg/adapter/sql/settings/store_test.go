package settings

import (
	"context"
	"database/sql"
	"encoding/base64"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/cc-auto-agent/harness-core/pkg/adapter/sql/sqlkit"

	_ "modernc.org/sqlite"
)

func TestStoreSQLiteCreateReadUpdateAndFoundEmpty(t *testing.T) {
	store, db := newSQLiteStore(t, Options{})
	defer db.Close()
	ctx := context.Background()

	if err := store.SetSetting(ctx, "llm", `{"model":"first"}`); err != nil {
		t.Fatal(err)
	}
	if err := store.SetSetting(ctx, "llm", `{"model":"second"}`); err != nil {
		t.Fatal(err)
	}
	if err := store.SetSetting(ctx, "empty", ""); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		key       string
		found     bool
		wantValue string
	}{
		{key: "llm", found: true, wantValue: `{"model":"second"}`},
		{key: "empty", found: true, wantValue: ""},
		{key: "missing", found: false, wantValue: ""},
	} {
		t.Run(test.key, func(t *testing.T) {
			value, found, err := store.GetSetting(ctx, test.key)
			if err != nil || found != test.found || value != test.wantValue {
				t.Fatalf("GetSetting(%q) = %q, %t, %v", test.key, value, found, err)
			}
		})
	}
}

func TestStoreCapacityAllowsOverwrite(t *testing.T) {
	store, db := newSQLiteStore(t, Options{MaxEntries: 2})
	defer db.Close()
	ctx := context.Background()
	for _, key := range []string{"one", "two"} {
		if err := store.SetSetting(ctx, key, key); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.SetSetting(ctx, "three", "three"); err == nil || !strings.Contains(err.Error(), "settings exceed maximum of 2") {
		t.Fatalf("new key at cap error=%v", err)
	}
	if err := store.SetSetting(ctx, "one", "replacement"); err != nil {
		t.Fatalf("overwrite at cap: %v", err)
	}
	if value, found, err := store.GetSetting(ctx, "one"); err != nil || !found || value != "replacement" {
		t.Fatalf("overwrite result=%q found=%t err=%v", value, found, err)
	}
}

func TestStoreSetSettingIfAbsentCreatesOnceWithoutOverwriting(t *testing.T) {
	store, db := newSQLiteStore(t, Options{MaxEntries: 1, Cipher: prefixCipher{}})
	defer db.Close()
	ctx := context.Background()

	created, err := store.SetSettingIfAbsent(ctx, "llm", `{"model":"first"}`)
	if err != nil || !created {
		t.Fatalf("first conditional create created=%t err=%v", created, err)
	}
	created, err = store.SetSettingIfAbsent(ctx, "llm", `{"model":"second"}`)
	if err != nil || created {
		t.Fatalf("second conditional create created=%t err=%v", created, err)
	}
	value, found, err := store.GetSetting(ctx, "llm")
	if err != nil || !found || value != `{"model":"first"}` {
		t.Fatalf("conditional create value=%q found=%t err=%v", value, found, err)
	}
	if _, err := store.SetSettingIfAbsent(ctx, "other", `{"model":"other"}`); err == nil || !strings.Contains(err.Error(), "settings exceed maximum of 1") {
		t.Fatalf("conditional create bypassed capacity: %v", err)
	}
	var persisted string
	if err := db.QueryRowContext(ctx, "SELECT value FROM settings WHERE key = ?", "llm").Scan(&persisted); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(persisted, `{"model":"first"}`) || !strings.HasPrefix(persisted, "sealed:") {
		t.Fatalf("conditional create did not seal at rest: %q", persisted)
	}
}

func TestStoreSetSettingIfAbsentBindsForSQLiteAndPostgres(t *testing.T) {
	for _, test := range []struct {
		name    string
		dialect sqlkit.Dialect
		want    string
	}{
		{
			name:    "sqlite",
			dialect: sqlkit.SQLite,
			want:    sqlSetSettingIfAbsent,
		},
		{
			name:    "postgres",
			dialect: sqlkit.Postgres,
			want:    "INSERT INTO settings (key, value) SELECT $1, $2 WHERE (SELECT COUNT(*) FROM settings) < $3 ON CONFLICT (key) DO NOTHING",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := sqlkit.Bind(sqlSetSettingIfAbsent, test.dialect)
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("conditional insert binding = %q, want %q", got, test.want)
			}
		})
	}
}

func TestStoreCompareAndSwapSettingUsesPlaintextComparisonAndSealsNextValue(t *testing.T) {
	store, db := newSQLiteStore(t, Options{MaxEntries: 1, Cipher: prefixCipher{}})
	defer db.Close()
	ctx := context.Background()
	if err := store.SetSetting(ctx, "llm", "expected"); err != nil {
		t.Fatal(err)
	}

	swapped, err := store.CompareAndSwapSetting(ctx, "llm", "expected", "replacement")
	if err != nil || !swapped {
		t.Fatalf("compare-and-swap swapped=%t err=%v", swapped, err)
	}
	var raw string
	if err := db.QueryRowContext(ctx, "SELECT value FROM settings WHERE key = ?", "llm").Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(raw, "replacement") || !strings.HasPrefix(raw, "sealed:") {
		t.Fatalf("compare-and-swap did not seal replacement")
	}
	value, found, err := store.GetSetting(ctx, "llm")
	if err != nil || !found || value != "replacement" {
		t.Fatalf("compare-and-swap result value=%q found=%t err=%v", value, found, err)
	}
	for _, test := range []struct {
		name     string
		key      string
		expected string
		next     string
	}{
		{name: "invalid key", key: "bad\nkey", expected: "replacement", next: "ignored"},
		{name: "mismatched", key: "llm", expected: "stale", next: "ignored"},
		{name: "missing", key: "missing", expected: "expected", next: "ignored"},
		{name: "invalid expected", key: "llm", expected: "bad\x00value", next: "ignored"},
		{name: "invalid next", key: "llm", expected: "replacement", next: "bad\x00value"},
	} {
		t.Run(test.name, func(t *testing.T) {
			swapped, err := store.CompareAndSwapSetting(ctx, test.key, test.expected, test.next)
			if strings.HasPrefix(test.name, "invalid") {
				if err == nil || swapped {
					t.Fatalf("invalid compare-and-swap swapped=%t err=%v", swapped, err)
				}
				return
			}
			if err != nil || swapped {
				t.Fatalf("stale compare-and-swap swapped=%t err=%v", swapped, err)
			}
		})
	}
}

func TestStoreCompareAndSwapSettingIsAtomicAcrossSQLiteConnections(t *testing.T) {
	path := filepath.Join(t.TempDir(), "settings.sqlite")
	first, firstDB := newFileSQLiteStore(t, path, Options{})
	defer firstDB.Close()
	second, secondDB := newFileSQLiteStore(t, path, Options{})
	defer secondDB.Close()
	ctx := context.Background()
	if err := first.SetSetting(ctx, "llm", "expected"); err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	results := make(chan struct {
		swapped bool
		err     error
	}, 2)
	var group sync.WaitGroup
	for index, store := range []*Store{first, second} {
		next := []string{"first", "second"}[index]
		group.Add(1)
		go func(store *Store, next string) {
			defer group.Done()
			<-start
			swapped, err := store.CompareAndSwapSetting(ctx, "llm", "expected", next)
			results <- struct {
				swapped bool
				err     error
			}{swapped: swapped, err: err}
		}(store, next)
	}
	close(start)
	group.Wait()
	close(results)

	count := 0
	for result := range results {
		if result.err != nil {
			t.Fatalf("concurrent compare-and-swap: %v", result.err)
		}
		if result.swapped {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("concurrent compare-and-swap winners=%d, want 1", count)
	}
	value, found, err := first.GetSetting(ctx, "llm")
	if err != nil || !found || (value != "first" && value != "second") {
		t.Fatalf("concurrent compare-and-swap value=%q found=%t err=%v", value, found, err)
	}
}

func TestStoreCompareAndSwapSettingBindsForSQLiteAndPostgres(t *testing.T) {
	for _, test := range []struct {
		name    string
		dialect sqlkit.Dialect
		want    string
	}{
		{name: "sqlite", dialect: sqlkit.SQLite, want: sqlCompareAndSwapSetting},
		{name: "postgres", dialect: sqlkit.Postgres, want: "UPDATE settings SET value = $1 WHERE key = $2 AND value = $3"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := sqlkit.Bind(sqlCompareAndSwapSetting, test.dialect)
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("compare-and-swap binding = %q, want %q", got, test.want)
			}
		})
	}
}

func TestStoreEncryptsAndDecryptsValues(t *testing.T) {
	cipher := prefixCipher{}
	store, db := newSQLiteStore(t, Options{Cipher: cipher})
	defer db.Close()
	ctx := context.Background()
	plaintext := `{"api_key":"secret-value"}`
	if err := store.SetSetting(ctx, "llm", plaintext); err != nil {
		t.Fatal(err)
	}
	var persisted string
	if err := db.QueryRowContext(ctx, "SELECT value FROM settings WHERE key = ?", "llm").Scan(&persisted); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(persisted, plaintext) || !strings.HasPrefix(persisted, "sealed:") {
		t.Fatalf("plaintext leaked in settings table: %q", persisted)
	}
	value, found, err := store.GetSetting(ctx, "llm")
	if err != nil || !found || value != plaintext {
		t.Fatalf("cipher roundtrip value=%q found=%t err=%v", value, found, err)
	}
}

func TestStoreRejectsInvalidOptionsAndInput(t *testing.T) {
	if _, err := New(Options{}); err == nil {
		t.Fatal("nil database was accepted")
	}
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, options := range []Options{
		{DB: db, Dialect: sqlkit.Dialect(99)},
		{DB: db, Dialect: sqlkit.SQLite, MaxEntries: -1},
		{DB: db, Dialect: sqlkit.SQLite, MaxEntries: maxConfigEntries + 1},
		{DB: db, Dialect: sqlkit.SQLite, MaxValueBytes: -1},
		{DB: db, Dialect: sqlkit.SQLite, MaxValueBytes: maxConfigValueBytes + 1},
	} {
		if _, err := New(options); err == nil {
			t.Fatalf("invalid options accepted: %#v", options)
		}
	}

	store, storeDB := newSQLiteStore(t, Options{MaxValueBytes: 4})
	defer storeDB.Close()
	for _, test := range []struct {
		name  string
		key   string
		value string
	}{
		{name: "empty key", key: " \t", value: "ok"},
		{name: "NUL key", key: "a\x00b", value: "ok"},
		{name: "control key", key: "a\nb", value: "ok"},
		{name: "long key", key: strings.Repeat("界", maxSettingKeyRunes+1), value: "ok"},
		{name: "NUL value", key: "value", value: "a\x00b"},
		{name: "long value", key: "value", value: "12345"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := store.SetSetting(context.Background(), test.key, test.value); err == nil {
				t.Fatalf("invalid setting accepted: %#v", test)
			}
		})
	}
}

func newSQLiteStore(t *testing.T, options Options) (*Store, *sql.DB) {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)"); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	options.DB = db
	if options.Dialect == 0 {
		options.Dialect = sqlkit.SQLite
	}
	store, err := New(options)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	return store, db
}

func newFileSQLiteStore(t *testing.T, path string, options Options) (*Store, *sql.DB) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)"); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	options.DB = db
	if options.Dialect == 0 {
		options.Dialect = sqlkit.SQLite
	}
	store, err := New(options)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	return store, db
}

type prefixCipher struct{}

func (prefixCipher) Encrypt(value string) (string, error) {
	if value == "" {
		return "", nil
	}
	return "sealed:" + base64.StdEncoding.EncodeToString([]byte(value)), nil
}

func (prefixCipher) Decrypt(value string) (string, error) {
	if !strings.HasPrefix(value, "sealed:") {
		return value, nil
	}
	plain, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(value, "sealed:"))
	return string(plain), err
}
