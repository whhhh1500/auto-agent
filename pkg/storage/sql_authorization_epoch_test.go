package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	core "github.com/cc-auto-agent/harness-core/pkg/core"

	_ "modernc.org/sqlite"
)

type transparentAuthorizationEpochSessionStore struct{ *SQLSessionStore }
type transparentAuthorizationEpochRunControlStore struct{ *SQLRunControlStore }
type transparentAuthorizationEpochToolJournal struct{ *SQLToolInvocationJournal }

type unprovenAuthorizationEpochToolJournal struct{ core.ToolInvocationJournal }

func TestSQLAuthorizationEpochTracksDurableAuthorizationChanges(t *testing.T) {
	ctx := context.Background()
	sessions := newTestSQLStore(t)
	accounts, err := NewSQLAccountStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	bindings, err := NewSQLBindingJournal(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	canaries, err := NewSQLCanaryStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product, err := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "epoch"})
	if err != nil {
		t.Fatal(err)
	}
	assertAuthorizationEpoch(t, sessions, 0)

	if err := sessions.RecordRelease(ctx, sqlReleaseInfo(t, "epoch.agent", 1, product)); err != nil {
		t.Fatal(err)
	}
	assertAuthorizationEpoch(t, sessions, 1)
	if err := sessions.MarkRolledBack(ctx, "epoch.agent", []int{1}); err != nil {
		t.Fatal(err)
	}
	assertAuthorizationEpoch(t, sessions, 2)
	if err := canaries.CreateCanary(ctx, sqlCanaryTestRecord(t, "epoch-canary", product)); err != nil {
		t.Fatal(err)
	}
	assertAuthorizationEpoch(t, sessions, 3)
	canary, err := canaries.GetCanary(ctx, "epoch-canary")
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := canaries.UpdateCanary(ctx, canary, canary.Status); err != nil || !changed {
		t.Fatalf("update canary changed=%t err=%v", changed, err)
	}
	assertAuthorizationEpoch(t, sessions, 4)
	for index, kind := range []string{"policy", "profile", "disable", "credential"} {
		if err := bindings.Record(ctx, BindingRecord{ID: fmt.Sprintf("epoch-%s-%d", kind, index), Kind: kind, Payload: []byte(`{"scope":"global/epoch"}`)}); err != nil {
			t.Fatal(err)
		}
	}
	assertAuthorizationEpoch(t, sessions, 8)

	if err := accounts.CreateAccount(ctx, Account{AccountID: "epoch-active", Role: RoleAccountUser, TenantID: "acme", Status: AccountActive}, "password-123"); err != nil {
		t.Fatal(err)
	}
	assertAuthorizationEpoch(t, sessions, 9)
	if err := accounts.CreateAccount(ctx, Account{AccountID: "epoch-pending", Role: RoleAccountUser, TenantID: "acme", Status: AccountPendingActivation, MustChangePassword: true}, "initial-password-123"); err != nil {
		t.Fatal(err)
	}
	assertAuthorizationEpoch(t, sessions, 9)
	if err := accounts.CreateAccount(ctx, Account{AccountID: "epoch-disabled", Role: RoleAccountUser, TenantID: "acme", Status: AccountDisabled}, "password-123"); err != nil {
		t.Fatal(err)
	}
	assertAuthorizationEpoch(t, sessions, 9)
	if _, err := accounts.ActivateInitialAccount(ctx, "epoch-pending", "replacement-password-456", time.Hour); err != nil {
		t.Fatal(err)
	}
	assertAuthorizationEpoch(t, sessions, 10)
	if err := accounts.SetAccountStatus(ctx, "epoch-active", AccountDisabled); err != nil {
		t.Fatal(err)
	}
	assertAuthorizationEpoch(t, sessions, 11)
	if err := accounts.SetAccountStatus(ctx, "epoch-active", AccountDisabled); err != nil {
		t.Fatal(err)
	}
	assertAuthorizationEpoch(t, sessions, 11)
	if err := accounts.SetAccountPassword(ctx, "epoch-active", "rotated-password-456"); err != nil {
		t.Fatal(err)
	}
	if err := accounts.CreateTenant(ctx, "epoch-tenant", "Epoch Tenant"); err != nil {
		t.Fatal(err)
	}
	assertAuthorizationEpoch(t, sessions, 11)
	for index, kind := range []string{"policy", "profile", "disable", "credential"} {
		if err := bindings.Delete(ctx, fmt.Sprintf("epoch-%s-%d", kind, index)); err != nil {
			t.Fatal(err)
		}
	}
	assertAuthorizationEpoch(t, sessions, 15)

	control, err := sessions.ControlRevision(ctx)
	if err != nil || control != 4 {
		t.Fatalf("control revision=%d err=%v, want release/rollback/canary revision 4", control, err)
	}
}

func TestSQLSchemaV41MigratesAuthorizationEpoch(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", t.TempDir()+"/authorization-epoch-v41.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	fresh, err := OpenSQLSessionStore(ctx, db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	assertAuthorizationEpoch(t, fresh, 0)
	var freshVersion string
	if err := db.QueryRowContext(ctx, "SELECT value FROM store_meta WHERE key = 'schema_version'").Scan(&freshVersion); err != nil || freshVersion != "47" {
		t.Fatalf("fresh schema version=%q err=%v, want 47", freshVersion, err)
	}
	if _, err := db.ExecContext(ctx, "DELETE FROM store_meta WHERE key = 'authorization_epoch'; UPDATE store_meta SET value = '41' WHERE key = 'schema_version'"); err != nil {
		t.Fatal(err)
	}
	store, err := OpenSQLSessionStore(ctx, db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	assertAuthorizationEpoch(t, store, 0)
	var version string
	if err := db.QueryRowContext(ctx, "SELECT value FROM store_meta WHERE key = 'schema_version'").Scan(&version); err != nil || version != "47" {
		t.Fatalf("schema version=%q err=%v, want 47", version, err)
	}
}

func TestPostgresSchemaV41MigratesAuthorizationEpoch(t *testing.T) {
	ctx := context.Background()
	db := newPostgresTestDB(t)
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "DELETE FROM store_meta WHERE key = 'authorization_epoch'; UPDATE store_meta SET value = '41' WHERE key = 'schema_version'"); err != nil {
		t.Fatal(err)
	}
	store, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	assertAuthorizationEpoch(t, store, 0)
	var version string
	if err := db.QueryRowContext(ctx, "SELECT value FROM store_meta WHERE key = 'schema_version'").Scan(&version); err != nil || version != "47" {
		t.Fatalf("schema version=%q err=%v, want 47", version, err)
	}
}

func TestSQLAuthorizationEpochMutationFailsClosedForMissingInvalidOrExhaustedValue(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name    string
		prepare func(*SQLSessionStore) error
	}{
		{
			name: "missing",
			prepare: func(store *SQLSessionStore) error {
				_, err := store.db.ExecContext(ctx, "DELETE FROM store_meta WHERE key = 'authorization_epoch'")
				return err
			},
		},
		{
			name: "invalid",
			prepare: func(store *SQLSessionStore) error {
				_, err := store.db.ExecContext(ctx, "UPDATE store_meta SET value = 'not-an-epoch' WHERE key = 'authorization_epoch'")
				return err
			},
		},
		{
			name: "noncanonical",
			prepare: func(store *SQLSessionStore) error {
				_, err := store.db.ExecContext(ctx, "UPDATE store_meta SET value = '01' WHERE key = 'authorization_epoch'")
				return err
			},
		},
		{
			name: "exhausted",
			prepare: func(store *SQLSessionStore) error {
				_, err := store.db.ExecContext(ctx, "UPDATE store_meta SET value = ? WHERE key = 'authorization_epoch'", strconv.FormatInt(maxAuthorizationEpoch, 10))
				return err
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newTestSQLStore(t)
			if err := test.prepare(store); err != nil {
				t.Fatal(err)
			}
			bindings, err := NewSQLBindingJournal(store.db, SQLDialectSQLite)
			if err != nil {
				t.Fatal(err)
			}
			if err := bindings.Record(ctx, BindingRecord{ID: "epoch-" + test.name, Kind: "policy", Payload: []byte(`{"scope":"global"}`)}); err == nil {
				t.Fatal("authorization mutation unexpectedly committed")
			}
			records, err := bindings.List(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(records) != 0 {
				t.Fatalf("failed authorization mutation persisted bindings: %#v", records)
			}
			if test.name == "exhausted" {
				assertAuthorizationEpoch(t, store, maxAuthorizationEpoch)
			} else if _, err := store.AuthorizationEpoch(ctx); err == nil {
				t.Fatal("invalid authorization epoch was accepted")
			}
		})
	}
}

func TestSQLAuthorizationEpochLockFailsClosedForMissingOrInvalidValue(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name string
		sql  string
	}{
		{name: "missing", sql: "DELETE FROM store_meta WHERE key = 'authorization_epoch'"},
		{name: "invalid", sql: "UPDATE store_meta SET value = 'not-an-epoch' WHERE key = 'authorization_epoch'"},
		{name: "noncanonical", sql: "UPDATE store_meta SET value = '01' WHERE key = 'authorization_epoch'"},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newTestSQLStore(t)
			if _, err := store.db.ExecContext(ctx, test.sql); err != nil {
				t.Fatal(err)
			}
			tx, err := store.db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			err = lockAuthorizationEpoch(ctx, tx, SQLDialectSQLite, 0)
			_ = tx.Rollback()
			if err == nil || errors.Is(err, ErrAuthorizationEpochChanged) {
				t.Fatalf("lock error=%v, want missing or invalid epoch error", err)
			}
		})
	}
}

func TestSQLAuthorizationEpochRollsBackWithAuthorizationMutation(t *testing.T) {
	ctx := context.Background()
	sessions := newTestSQLStore(t)
	bindings, err := NewSQLBindingJournal(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	const trigger = "abort_authorization_epoch_bump"
	if _, err := sessions.db.ExecContext(ctx, `CREATE TRIGGER `+trigger+`
		BEFORE UPDATE OF value ON store_meta WHEN NEW.key = 'authorization_epoch'
		BEGIN SELECT RAISE(ABORT, 'authorization epoch abort'); END`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = sessions.db.ExecContext(context.Background(), "DROP TRIGGER IF EXISTS "+trigger) })
	if err := bindings.Record(ctx, BindingRecord{ID: "rollback-policy", Kind: "policy", Payload: []byte(`{"scope":"global"}`)}); err == nil {
		t.Fatal("binding record unexpectedly committed")
	}
	assertAuthorizationEpoch(t, sessions, 0)
	records, err := bindings.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("rolled-back binding persisted: %#v", records)
	}
}

func TestSQLAuthorizationEpochRollsBackReleaseAndCanaryTogether(t *testing.T) {
	ctx := context.Background()
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product, err := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "epoch-rollback"})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name  string
		write func(*SQLSessionStore) error
		check func(*SQLSessionStore) error
	}{
		{
			name: "release",
			write: func(store *SQLSessionStore) error {
				return store.RecordRelease(ctx, sqlReleaseInfo(t, "epoch.rollback", 1, product))
			},
			check: func(store *SQLSessionStore) error {
				profiles, err := store.ListReleaseProfiles(ctx)
				if err != nil {
					return err
				}
				if len(profiles) != 0 {
					return errors.New("rolled-back release persisted")
				}
				return nil
			},
		},
		{
			name: "canary",
			write: func(store *SQLSessionStore) error {
				canaries, err := NewSQLCanaryStore(store.db, store.dialect)
				if err != nil {
					return err
				}
				return canaries.CreateCanary(ctx, sqlCanaryTestRecord(t, "epoch-rollback-canary", product))
			},
			check: func(store *SQLSessionStore) error {
				canaries, err := NewSQLCanaryStore(store.db, store.dialect)
				if err != nil {
					return err
				}
				_, err = canaries.GetCanary(ctx, "epoch-rollback-canary")
				if err == nil {
					return errors.New("rolled-back canary persisted")
				}
				return nil
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newTestSQLStore(t)
			const trigger = "abort_authorization_epoch_bump"
			if _, err := store.db.ExecContext(ctx, `CREATE TRIGGER `+trigger+`
				BEFORE UPDATE OF value ON store_meta WHEN NEW.key = 'authorization_epoch'
				BEGIN SELECT RAISE(ABORT, 'authorization epoch abort'); END`); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _, _ = store.db.ExecContext(context.Background(), "DROP TRIGGER IF EXISTS "+trigger) })
			if err := test.write(store); err == nil {
				t.Fatal("authorization mutation unexpectedly committed")
			}
			assertAuthorizationEpoch(t, store, 0)
			control, err := store.ControlRevision(ctx)
			if err != nil || control != 0 {
				t.Fatalf("rolled-back control revision=%d err=%v", control, err)
			}
			if err := test.check(store); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSQLAuthorizationEpochRollsBackAccountStatus(t *testing.T) {
	ctx := context.Background()
	store := newTestSQLStore(t)
	accounts, err := NewSQLAccountStore(store.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	if err := accounts.CreateAccount(ctx, Account{AccountID: "epoch-account", Role: RoleAccountUser, TenantID: "acme", Status: AccountActive}, "password-123"); err != nil {
		t.Fatal(err)
	}
	assertAuthorizationEpoch(t, store, 1)
	const trigger = "abort_authorization_epoch_status"
	if _, err := store.db.ExecContext(ctx, `CREATE TRIGGER `+trigger+`
		BEFORE UPDATE OF value ON store_meta WHEN NEW.key = 'authorization_epoch'
		BEGIN SELECT RAISE(ABORT, 'authorization epoch abort'); END`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = store.db.ExecContext(context.Background(), "DROP TRIGGER IF EXISTS "+trigger) })
	if err := accounts.SetAccountStatus(ctx, "epoch-account", AccountDisabled); err == nil {
		t.Fatal("account status unexpectedly committed")
	}
	account, err := accounts.GetAccount(ctx, "epoch-account")
	if err != nil || account.Status != AccountActive {
		t.Fatalf("rolled-back account=%#v err=%v", account, err)
	}
	assertAuthorizationEpoch(t, store, 1)
}

func TestSQLAuthorizationEpochRollsBackActiveAccountCreationAndActivation(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name  string
		setup func(*SQLAccountStore) error
		write func(*SQLAccountStore) error
		check func(*SQLAccountStore) error
	}{
		{
			name:  "active_create",
			setup: func(*SQLAccountStore) error { return nil },
			write: func(accounts *SQLAccountStore) error {
				return accounts.CreateAccount(ctx, Account{AccountID: "epoch-create", Role: RoleAccountUser, TenantID: "acme", Status: AccountActive}, "password-123")
			},
			check: func(accounts *SQLAccountStore) error {
				_, err := accounts.GetAccount(ctx, "epoch-create")
				if err == nil {
					return errors.New("rolled-back active account persisted")
				}
				return nil
			},
		},
		{
			name: "activation",
			setup: func(accounts *SQLAccountStore) error {
				return accounts.CreateAccount(ctx, Account{AccountID: "epoch-activate", Role: RoleAccountUser, TenantID: "acme", Status: AccountPendingActivation, MustChangePassword: true}, "initial-password-123")
			},
			write: func(accounts *SQLAccountStore) error {
				_, err := accounts.ActivateInitialAccount(ctx, "epoch-activate", "replacement-password-456", time.Hour)
				return err
			},
			check: func(accounts *SQLAccountStore) error {
				account, err := accounts.GetAccount(ctx, "epoch-activate")
				if err != nil || account.Status != AccountPendingActivation || !account.MustChangePassword {
					return fmt.Errorf("rolled-back activation account=%#v err=%v", account, err)
				}
				return nil
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newTestSQLStore(t)
			accounts, err := NewSQLAccountStore(store.db, SQLDialectSQLite)
			if err != nil {
				t.Fatal(err)
			}
			if err := test.setup(accounts); err != nil {
				t.Fatal(err)
			}
			const trigger = "abort_authorization_epoch_account"
			if _, err := store.db.ExecContext(ctx, `CREATE TRIGGER `+trigger+`
				BEFORE UPDATE OF value ON store_meta WHEN NEW.key = 'authorization_epoch'
				BEGIN SELECT RAISE(ABORT, 'authorization epoch abort'); END`); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _, _ = store.db.ExecContext(context.Background(), "DROP TRIGGER IF EXISTS "+trigger) })
			if err := test.write(accounts); err == nil {
				t.Fatal("authorization account mutation unexpectedly committed")
			}
			assertAuthorizationEpoch(t, store, 0)
			if err := test.check(accounts); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSQLAuthorizationEpochReadAndCompare(t *testing.T) {
	ctx := context.Background()
	sessions := newTestSQLStore(t)
	bindings, err := NewSQLBindingJournal(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	if err := bindings.Record(ctx, BindingRecord{ID: "snapshot-policy", Kind: "policy", Payload: []byte(`{"scope":"global"}`)}); err != nil {
		t.Fatal(err)
	}
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product, err := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "snapshot"})
	if err != nil {
		t.Fatal(err)
	}
	if err := sessions.RecordRelease(ctx, sqlReleaseInfo(t, "snapshot.agent", 1, product)); err != nil {
		t.Fatal(err)
	}
	canaries, err := NewSQLCanaryStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	if err := canaries.CreateCanary(ctx, sqlCanaryTestRecord(t, "snapshot-canary", product)); err != nil {
		t.Fatal(err)
	}
	before, err := sessions.AuthorizationEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bindings.List(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := sessions.ListReleaseProfiles(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := canaries.ListOpenCanaries(ctx); err != nil {
		t.Fatal(err)
	}
	after, err := sessions.AuthorizationEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatalf("read-only snapshot changed authorization epoch: before=%d after=%d", before, after)
	}

	tx, err := sessions.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := lockAuthorizationEpoch(ctx, tx, SQLDialectSQLite, before); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	assertAuthorizationEpoch(t, sessions, before)

	tx, err = sessions.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = lockAuthorizationEpoch(ctx, tx, SQLDialectSQLite, before-1)
	_ = tx.Rollback()
	if !errors.Is(err, ErrAuthorizationEpochChanged) {
		t.Fatalf("mismatched epoch error=%v, want ErrAuthorizationEpochChanged", err)
	}
}

func TestSQLAuthorizationEpochConcurrentBindingMutationsAreMonotonic(t *testing.T) {
	sessions := newTestSQLStore(t)
	// This test exercises concurrent callers while keeping SQLite's one writer
	// contract deterministic. The transaction still owns binding plus epoch.
	sessions.db.SetMaxOpenConns(1)
	sessions.db.SetMaxIdleConns(1)
	bindings, err := NewSQLBindingJournal(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, id := range []string{"concurrent-policy-a", "concurrent-policy-b"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			errs <- bindings.Record(context.Background(), BindingRecord{ID: id, Kind: "policy", Payload: []byte(`{"scope":"global"}`)})
		}(id)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	assertAuthorizationEpoch(t, sessions, 2)
}

func TestSQLAuthorizationEpochSQLiteLockBlocksConflictingWriter(t *testing.T) {
	path := t.TempDir() + "/authorization-epoch.db"
	first, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	second, err := sql.Open("sqlite", path)
	if err != nil {
		_ = first.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Close() })
	t.Cleanup(func() { _ = second.Close() })
	first.SetMaxOpenConns(1)
	second.SetMaxOpenConns(1)
	store, err := OpenSQLSessionStore(context.Background(), first, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLSessionStore(context.Background(), second, SQLDialectSQLite); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Exec(`PRAGMA busy_timeout=25`); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Exec(`PRAGMA busy_timeout=25`); err != nil {
		t.Fatal(err)
	}
	tx, err := first.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := lockAuthorizationEpoch(context.Background(), tx, SQLDialectSQLite, 0); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		tx, err := second.BeginTx(context.Background(), nil)
		if err == nil {
			err = lockAuthorizationEpoch(context.Background(), tx, SQLDialectSQLite, 0)
			_ = tx.Rollback()
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil || (!strings.Contains(strings.ToLower(err.Error()), "busy") && !strings.Contains(strings.ToLower(err.Error()), "locked")) {
			t.Fatalf("second SQLite epoch writer error=%v, want busy or locked", err)
		}
	case <-time.After(2 * time.Second):
		_ = tx.Rollback()
		t.Fatal("second SQLite epoch writer did not return")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	assertAuthorizationEpoch(t, store, 0)
}

func TestPostgresAuthorizationEpochLockAndCompare(t *testing.T) {
	ctx := context.Background()
	db := newPostgresTestDB(t)
	store, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := lockAuthorizationEpoch(ctx, tx, SQLDialectPostgres, 0); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		other, err := db.BeginTx(context.Background(), nil)
		if err == nil {
			err = lockAuthorizationEpoch(context.Background(), other, SQLDialectPostgres, 0)
			if err == nil {
				err = other.Commit()
			} else {
				_ = other.Rollback()
			}
		}
		done <- err
	}()
	select {
	case err := <-done:
		_ = tx.Rollback()
		t.Fatalf("second PostgreSQL epoch writer returned before lock release: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("second PostgreSQL epoch writer did not complete after lock release")
	}
	assertAuthorizationEpoch(t, store, 0)
}

func TestAuthorizationEpochSQLAuthority(t *testing.T) {
	sessions := newTestSQLStore(t)
	queue, err := NewSQLRunControlStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := NewSQLToolInvocationJournal(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateAuthorizationEpochSQLAuthority(sessions, queue, sessions, journal); err != nil {
		t.Fatalf("same SQL authority rejected: %v", err)
	}
	if err := ValidateAuthorizationEpochSQLAuthority(
		&transparentAuthorizationEpochSessionStore{sessions},
		&transparentAuthorizationEpochRunControlStore{queue},
		&transparentAuthorizationEpochSessionStore{sessions},
		&transparentAuthorizationEpochToolJournal{journal},
	); err != nil {
		t.Fatalf("transparent SQL wrappers rejected: %v", err)
	}
	if err := ValidateAuthorizationEpochSQLAuthority(sessions, queue, sessions, &unprovenAuthorizationEpochToolJournal{ToolInvocationJournal: journal}); err == nil {
		t.Fatal("unproven tool journal authority was accepted")
	}
	other := newTestSQLStore(t)
	otherJournal, err := NewSQLToolInvocationJournal(other.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateAuthorizationEpochSQLAuthority(sessions, queue, sessions, otherJournal); err == nil {
		t.Fatal("different SQL journal authority was accepted")
	}
}

func TestAuthorizationEpochPostgresBinding(t *testing.T) {
	query := sqlLockAuthorizationEpoch.bind(SQLDialectPostgres)
	if !strings.Contains(query, "UPDATE store_meta") || !strings.Contains(query, "$1") || strings.Contains(query, "FOR UPDATE") {
		t.Fatalf("unexpected PostgreSQL authorization epoch lock binding: %q", query)
	}
}

func assertAuthorizationEpoch(t *testing.T, reader AuthorizationEpochReader, want int64) {
	t.Helper()
	got, err := reader.AuthorizationEpoch(context.Background())
	if err != nil || got != want {
		t.Fatalf("authorization epoch=%d err=%v, want %d", got, err, want)
	}
}
