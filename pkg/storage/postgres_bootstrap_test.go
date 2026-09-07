package storage

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"
)

// openPostgresFirstBoot mirrors the server's PostgreSQL-only critical section:
// retain the advisory lock from the pre-migration accounts inspection through
// the durable initial-admin decision.
func openPostgresFirstBoot(ctx context.Context, db *sql.DB) (credentials InitialAdminCredentials, err error) {
	lock, err := AcquirePostgresStartupLock(ctx, db)
	if err != nil {
		return InitialAdminCredentials{}, err
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if releaseErr := lock.Release(releaseCtx); releaseErr != nil && err == nil {
			err = releaseErr
		}
	}()

	accountsTableExisted, err := lock.AccountsTableExists(ctx)
	if err != nil {
		return InitialAdminCredentials{}, err
	}
	if _, err := lock.OpenSQLSessionStore(ctx); err != nil {
		return InitialAdminCredentials{}, err
	}
	return lock.BootstrapInitialAdmin(ctx, accountsTableExisted)
}

func TestPostgresStartupBootstrapWithSingleConnection(t *testing.T) {
	db := newPostgresTestDB(t)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	credentials, err := openPostgresFirstBoot(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if !credentials.Created {
		t.Fatal("single-connection first boot did not create the initial administrator")
	}
	accounts, err := NewSQLAccountStore(db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	if count, err := accounts.CountAccounts(ctx); err != nil || count != 1 {
		t.Fatalf("single-connection account count = %d, %v; want 1", count, err)
	}
}

func TestPostgresStartupUnlockFailureDiscardsSession(t *testing.T) {
	db := newPostgresTestDB(t)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	independent := newPostgresTestDB(t)
	ctx := context.Background()
	lock, err := AcquirePostgresStartupLock(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	var lockedBackend, replacementBackend int
	if err := lock.conn.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&lockedBackend); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := lock.Release(canceled); err == nil {
		t.Fatal("startup unlock unexpectedly succeeded with canceled context")
	}
	acquired, err := tryPostgresAdvisoryLock(ctx, independent, postgresStartupLockKey)
	if err != nil {
		t.Fatal(err)
	}
	if !acquired {
		t.Fatal("discarded startup session retained its advisory lock")
	}
	if err := db.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&replacementBackend); err != nil {
		t.Fatal(err)
	}
	if replacementBackend == lockedBackend {
		t.Fatalf("startup pool reused discarded backend %d", lockedBackend)
	}
}

func TestPostgresStartupLockAcquireCancellationDiscardsWaitingSession(t *testing.T) {
	holderDB := newPostgresTestDB(t)
	waiterDB := newPostgresTestDB(t)
	waiterDB.SetMaxOpenConns(1)
	waiterDB.SetMaxIdleConns(1)
	ctx := context.Background()
	holder, err := AcquirePostgresStartupLock(ctx, holderDB)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if releaseErr := holder.Release(releaseCtx); releaseErr != nil {
			t.Errorf("release startup lock holder: %v", releaseErr)
		}
	}()

	waitCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if _, err := AcquirePostgresStartupLock(waitCtx, waiterDB); err == nil {
		t.Fatal("competing startup lock acquisition ignored context deadline")
	}
	if err := holder.Release(ctx); err != nil {
		t.Fatal(err)
	}
	acquired, err := tryPostgresAdvisoryLock(ctx, waiterDB, postgresStartupLockKey)
	if err != nil {
		t.Fatal(err)
	}
	if !acquired {
		t.Fatal("canceled startup lock acquisition left a hidden session lock")
	}
}

func TestPostgresStartupBootstrapSerializesConcurrentEmptySchema(t *testing.T) {
	db := newPostgresTestDB(t)
	const openerCount = 4
	db.SetMaxOpenConns(openerCount*2 + 2)
	db.SetMaxIdleConns(openerCount*2 + 2)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	start := make(chan struct{})
	results := make(chan InitialAdminCredentials, openerCount)
	errs := make(chan error, openerCount)
	var wait sync.WaitGroup
	for range openerCount {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			credentials, err := openPostgresFirstBoot(ctx, db)
			results <- credentials
			errs <- err
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	created := 0
	var first InitialAdminCredentials
	for credentials := range results {
		if credentials.Created {
			created++
			first = credentials
		}
	}
	if created != 1 {
		t.Fatalf("initial administrators created = %d, want 1", created)
	}
	accounts, err := NewSQLAccountStore(db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	if count, err := accounts.CountAccounts(ctx); err != nil || count != 1 {
		t.Fatalf("account count = %d, %v; want 1", count, err)
	}
	account, err := accounts.GetAccount(ctx, first.AccountID)
	if err != nil {
		t.Fatal(err)
	}
	if account.Status != AccountPendingActivation || !account.MustChangePassword {
		t.Fatalf("initial account = %#v; want pending activation", account)
	}
}

func TestPostgresExistingEmptyAccountsTableDoesNotBootstrap(t *testing.T) {
	db := newPostgresTestDB(t)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE TABLE accounts (
		account_id TEXT PRIMARY KEY,
		email TEXT,
		password_hash TEXT NOT NULL,
		role TEXT NOT NULL,
		tenant_id TEXT NOT NULL,
		status TEXT NOT NULL DEFAULT 'active',
		must_change_password BIGINT NOT NULL DEFAULT 0,
		created_at BIGINT NOT NULL
	)`); err != nil {
		t.Fatal(err)
	}
	existed, err := AccountsTableExists(ctx, db, SQLDialectPostgres)
	if err != nil || !existed {
		t.Fatalf("pre-migration accounts existence = %t, %v; want true", existed, err)
	}
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres); err != nil {
		t.Fatal(err)
	}
	accounts, err := NewSQLAccountStore(db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	credentials, err := BootstrapInitialAdmin(ctx, accounts, existed)
	if err != nil || credentials.Created {
		t.Fatalf("existing empty accounts bootstrap = %#v, %v; want no creation", credentials, err)
	}
	if count, err := accounts.CountAccounts(ctx); err != nil || count != 0 {
		t.Fatalf("account count = %d, %v; want 0", count, err)
	}
}

func TestPostgresPasswordRotationRevokesTokens(t *testing.T) {
	db := newPostgresTestDB(t)
	ctx := context.Background()
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres); err != nil {
		t.Fatal(err)
	}
	accounts, err := NewSQLAccountStore(db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	const (
		accountID   = "postgres-password-uow"
		oldPassword = "old-password-123"
		newPassword = "new-password-123"
	)
	if err := accounts.CreateAccount(ctx, Account{
		AccountID: accountID, Role: RoleAccountUser, TenantID: "acme", Status: AccountActive,
	}, oldPassword); err != nil {
		t.Fatal(err)
	}
	tokenOne, err := accounts.CreateToken(ctx, accountID, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	tokenTwo, err := accounts.CreateToken(ctx, accountID, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := accounts.SetAccountPassword(ctx, accountID, newPassword); err != nil {
		t.Fatal(err)
	}
	updated, err := accounts.GetAccount(ctx, accountID)
	if err != nil {
		t.Fatal(err)
	}
	if !VerifyPassword(updated.PasswordHash, newPassword) || VerifyPassword(updated.PasswordHash, oldPassword) {
		t.Fatal("postgres password rotation did not persist the new password")
	}
	for _, token := range []string{tokenOne, tokenTwo} {
		if _, err := accounts.ResolveToken(ctx, token); err == nil {
			t.Fatal("postgres password rotation did not revoke every outstanding token")
		}
	}
}

func TestPostgresV31AccountIdentityMigrationPreservesTokens(t *testing.T) {
	db := newPostgresTestDB(t)
	ctx := context.Background()
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres); err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		"DROP TABLE auth_tokens",
		"DROP TABLE accounts",
		`CREATE TABLE accounts (
			email TEXT PRIMARY KEY,
			password_hash TEXT NOT NULL,
			role TEXT NOT NULL,
			tenant_id TEXT NOT NULL,
			status TEXT NOT NULL DEFAULT 'active',
			created_at BIGINT NOT NULL
		)`,
		"CREATE TABLE auth_tokens (token_hash TEXT PRIMARY KEY, email TEXT NOT NULL, expires_at BIGINT NOT NULL)",
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(ctx,
		"INSERT INTO accounts VALUES ($1, $2, $3, $4, $5, $6)",
		"legacy@example.com", "hash", RoleAccountAdmin, "system", AccountActive, int64(1234)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx,
		"INSERT INTO auth_tokens VALUES ($1, $2, $3)", "legacy-token", "legacy@example.com", int64(9999999999999)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, sqlUpdateMetaRow.bind(SQLDialectPostgres), "31"); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres); err != nil {
		t.Fatal(err)
	}

	accounts, err := NewSQLAccountStore(db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	account, err := accounts.GetAccount(ctx, "legacy@example.com")
	if err != nil {
		t.Fatal(err)
	}
	if account.AccountID != "legacy@example.com" || account.Email != "legacy@example.com" {
		t.Fatalf("migrated account = %#v", account)
	}
	var tokenAccount string
	if err := db.QueryRowContext(ctx, "SELECT account_id FROM auth_tokens WHERE token_hash = $1", "legacy-token").Scan(&tokenAccount); err != nil {
		t.Fatal(err)
	}
	if tokenAccount != account.AccountID {
		t.Fatalf("token account = %q, want %q", tokenAccount, account.AccountID)
	}
	var version string
	if err := db.QueryRowContext(ctx, sqlSelectMetaRow.bind(SQLDialectPostgres)).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != fmt.Sprint(SQLSchemaVersion) {
		t.Fatalf("schema version = %q, want %d", version, SQLSchemaVersion)
	}
}
