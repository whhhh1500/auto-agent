package storage

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestInitialAdminBootstrapAndActivation(t *testing.T) {
	sessions := newTestSQLStore(t)
	accounts, err := NewSQLAccountStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	credentials, err := BootstrapInitialAdmin(context.Background(), accounts, false)
	if err != nil || !credentials.Created {
		t.Fatalf("bootstrap = %#v, %v", credentials, err)
	}
	if !regexp.MustCompile(`^admin_\d{5}$`).MatchString(credentials.AccountID) || len(credentials.Password) < 12 {
		t.Fatalf("generated credentials have wrong shape: account=%q password-len=%d", credentials.AccountID, len(credentials.Password))
	}
	account, err := accounts.GetAccount(context.Background(), credentials.AccountID)
	if err != nil {
		t.Fatal(err)
	}
	if account.Email != "" || account.Status != AccountPendingActivation || !account.MustChangePassword {
		t.Fatalf("initial account = %#v", account)
	}
	restricted, err := accounts.CreateToken(context.Background(), account.AccountID, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := accounts.ActivateInitialAccount(context.Background(), account.AccountID, credentials.Password, time.Hour); err == nil {
		t.Fatal("activation accepted the initial password")
	}
	normal, err := accounts.ActivateInitialAccount(context.Background(), account.AccountID, "a-new-password-with-12-chars", time.Hour)
	if err != nil || normal == "" {
		t.Fatalf("activation token=%q err=%v", normal, err)
	}
	if _, err := accounts.ResolveToken(context.Background(), restricted); err == nil {
		t.Fatal("restricted token survived activation")
	}
	activated, err := accounts.ResolveToken(context.Background(), normal)
	if err != nil || activated.Status != AccountActive || activated.MustChangePassword {
		t.Fatalf("activated account=%#v err=%v", activated, err)
	}
	second, err := BootstrapInitialAdmin(context.Background(), accounts, false)
	if err != nil || second.Created {
		t.Fatalf("second bootstrap = %#v, %v", second, err)
	}
}

func TestInitialAdminDoesNotBootstrapExistingEmptyAccountsTable(t *testing.T) {
	sessions := newTestSQLStore(t)
	accounts, err := NewSQLAccountStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	credentials, err := BootstrapInitialAdmin(context.Background(), accounts, true)
	if err != nil || credentials.Created {
		t.Fatalf("existing table bootstrap = %#v, %v", credentials, err)
	}
	if count, err := accounts.CountAccounts(context.Background()); err != nil || count != 0 {
		t.Fatalf("account count=%d err=%v", count, err)
	}
}

func TestAccountActivationInvariants(t *testing.T) {
	sessions := newTestSQLStore(t)
	accounts, err := NewSQLAccountStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	for _, account := range []Account{
		{AccountID: "pending-without-flag", Role: RoleAccountAdmin, TenantID: "system", Status: AccountPendingActivation},
		{AccountID: "active-with-flag", Role: RoleAccountAdmin, TenantID: "system", Status: AccountActive, MustChangePassword: true},
	} {
		if err := accounts.CreateAccount(context.Background(), account, "valid-password-123"); err == nil {
			t.Fatalf("invalid activation state was accepted: %#v", account)
		}
	}
	if err := accounts.CreateAccount(context.Background(), Account{
		AccountID: "pending-valid", Role: RoleAccountAdmin, TenantID: "system",
		Status: AccountPendingActivation, MustChangePassword: true,
	}, "valid-password-123"); err != nil {
		t.Fatal(err)
	}
	if err := accounts.SetAccountStatus(context.Background(), "pending-valid", AccountActive); err == nil {
		t.Fatal("generic status update activated a pending account")
	}
	if err := accounts.SetAccountPassword(context.Background(), "pending-valid", strings.Repeat("x", MaxAccountPasswordBytes+1)); err == nil {
		t.Fatal("oversized bcrypt password was accepted")
	}
}

func TestSetAccountPasswordIsAtomicWithTokenRevocation(t *testing.T) {
	sessions := newTestSQLStore(t)
	accounts, err := NewSQLAccountStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	const (
		accountID   = "password-uow@example.com"
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
	before, err := accounts.GetAccount(ctx, accountID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sessions.db.ExecContext(ctx, `
		CREATE TRIGGER fail_password_rotation_token_delete
		BEFORE DELETE ON auth_tokens
		WHEN OLD.account_id = 'password-uow@example.com'
		BEGIN
			SELECT RAISE(ABORT, 'injected token revocation failure');
		END`); err != nil {
		t.Fatal(err)
	}
	if err := accounts.SetAccountPassword(ctx, accountID, newPassword); err == nil {
		t.Fatal("password rotation succeeded despite token revocation failure")
	}
	afterFailure, err := accounts.GetAccount(ctx, accountID)
	if err != nil {
		t.Fatal(err)
	}
	if afterFailure.PasswordHash != before.PasswordHash {
		t.Fatal("password hash changed after token revocation rolled back")
	}
	if !VerifyPassword(afterFailure.PasswordHash, oldPassword) || VerifyPassword(afterFailure.PasswordHash, newPassword) {
		t.Fatal("password verification changed after token revocation rolled back")
	}
	for _, token := range []string{tokenOne, tokenTwo} {
		if _, err := accounts.ResolveToken(ctx, token); err != nil {
			t.Fatalf("token was revoked after password rotation rollback: %v", err)
		}
	}
	if _, err := sessions.db.ExecContext(ctx, "DROP TRIGGER fail_password_rotation_token_delete"); err != nil {
		t.Fatal(err)
	}
	if err := accounts.SetAccountPassword(ctx, accountID, newPassword); err != nil {
		t.Fatal(err)
	}
	afterSuccess, err := accounts.GetAccount(ctx, accountID)
	if err != nil {
		t.Fatal(err)
	}
	if afterSuccess.PasswordHash == before.PasswordHash || !VerifyPassword(afterSuccess.PasswordHash, newPassword) || VerifyPassword(afterSuccess.PasswordHash, oldPassword) {
		t.Fatal("password rotation did not persist the new password")
	}
	for _, token := range []string{tokenOne, tokenTwo} {
		if _, err := accounts.ResolveToken(ctx, token); err == nil {
			t.Fatal("password rotation did not revoke every outstanding token")
		}
	}
	var remaining int
	if err := sessions.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM auth_tokens WHERE account_id = ?", accountID).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 0 {
		t.Fatalf("outstanding tokens after password rotation = %d, want 0", remaining)
	}
}

func TestInitialAdminConcurrentBootstrapCreatesOneAccount(t *testing.T) {
	sessions := newTestSQLStore(t)
	// Match the production server's single-write SQLite setup so busy_timeout and
	// lock contention are exercised by one connection only.
	sessions.db.SetMaxOpenConns(1)
	sessions.db.SetMaxIdleConns(1)
	accounts, err := NewSQLAccountStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	results := make(chan InitialAdminCredentials, 4)
	errors := make(chan error, 4)
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			credentials, err := BootstrapInitialAdmin(context.Background(), accounts, false)
			results <- credentials
			errors <- err
		}()
	}
	wg.Wait()
	close(results)
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	created := 0
	for result := range results {
		if result.Created {
			created++
		}
	}
	if count, err := accounts.CountAccounts(context.Background()); err != nil || count != 1 || created != 1 {
		t.Fatalf("created=%d count=%d err=%v", created, count, err)
	}
}

func TestBootstrapAdminNeverLogsPassword(t *testing.T) {
	sessions := newTestSQLStore(t)
	accounts, err := NewSQLAccountStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	var log bytes.Buffer
	email, password, created, err := BootstrapAdmin(context.Background(), accounts, func(format string, args ...any) {
		log.WriteString(fmt.Sprintf(format, args...))
	})
	if err != nil || !created || email == "" || password == "" {
		t.Fatalf("bootstrap=%q created=%t password=%q err=%v", email, created, password, err)
	}
	if bytes.Contains(log.Bytes(), []byte(password)) {
		t.Fatalf("bootstrap password leaked to logger: %s", log.String())
	}
}

func TestBootstrapAdminWithPasswordRequiresExplicitSecret(t *testing.T) {
	sessions := newTestSQLStore(t)
	accounts, err := NewSQLAccountStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := BootstrapAdminWithPassword(context.Background(), accounts, "", nil); err == nil {
		t.Fatal("bootstrap accepted an empty password")
	}
	const password = "not-for-logs-123"
	var log bytes.Buffer
	email, created, err := BootstrapAdminWithPassword(context.Background(), accounts, password, func(format string, args ...any) {
		log.WriteString(fmt.Sprintf(format, args...))
	})
	if err != nil || !created || email == "" {
		t.Fatalf("bootstrap=%q created=%t err=%v", email, created, err)
	}
	if bytes.Contains(log.Bytes(), []byte(password)) {
		t.Fatalf("explicit bootstrap password leaked: %s", log.String())
	}
}

func TestSQLAccountStoreRejectsInvalidEmailAndSettingsBeforeSQL(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	accounts, err := NewSQLAccountStore(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	assertBeforeSQL := func(name string, err error) {
		t.Helper()
		if err == nil {
			t.Fatalf("%s was accepted", name)
		}
		if strings.Contains(err.Error(), "no such table") {
			t.Fatalf("%s reached SQL before validation: %v", name, err)
		}
	}
	assertBeforeSQL("get email NUL", func() error {
		_, err := accounts.GetAccount(ctx, "a\x00@x.com")
		return err
	}())
	assertBeforeSQL("get empty account id", func() error {
		_, err := accounts.GetAccount(ctx, "")
		return err
	}())
	assertBeforeSQL("status control", accounts.SetAccountStatus(ctx, "a\n@x.com", AccountActive))
	assertBeforeSQL("setting empty key", accounts.SetSetting(ctx, "  ", "v"))
	assertBeforeSQL("setting NUL key", accounts.SetSetting(ctx, "k\x00", "v"))
	assertBeforeSQL("setting NUL value", accounts.SetSetting(ctx, "llm", "{\x00}"))
	assertBeforeSQL("get setting empty", func() error {
		_, _, err := accounts.GetSetting(ctx, "")
		return err
	}())
	assertBeforeSQL("empty password", accounts.CreateAccount(ctx, Account{
		Email: "a@x.com", Role: RoleAccountUser, TenantID: "acme",
	}, "  "))
	assertBeforeSQL("NUL password", accounts.SetAccountPassword(ctx, "a@x.com", "pw\x00"))
	assertBeforeSQL("tenant NUL name", accounts.CreateTenant(ctx, "acme", "Acme\x00"))
	assertBeforeSQL("unknown status", accounts.SetAccountStatus(ctx, "a@x.com", "nope"))
	assertBeforeSQL("NUL status", accounts.SetAccountStatus(ctx, "a@x.com", "active\x00"))
	assertBeforeSQL("token ttl", func() error {
		_, err := accounts.CreateToken(ctx, "a@x.com", 0)
		return err
	}())
	assertBeforeSQL("token ttl max", func() error {
		_, err := accounts.CreateToken(ctx, "a@x.com", MaxTokenTTL+time.Second)
		return err
	}())
	assertBeforeSQL("empty token revoke", accounts.RevokeToken(ctx, ""))
	assertBeforeSQL("NUL token revoke", accounts.RevokeToken(ctx, "ab\x00cd"))
	assertBeforeSQL("space token revoke", accounts.RevokeToken(ctx, " token"))
	assertBeforeSQL("empty token resolve", func() error {
		_, err := accounts.ResolveToken(ctx, "")
		return err
	}())
	assertBeforeSQL("NUL token resolve", func() error {
		_, err := accounts.ResolveToken(ctx, "ab\x00cd")
		return err
	}())
}

func TestSQLAccountStoreRejectsLiveTokenOverflow(t *testing.T) {
	sessions := newTestSQLStore(t)
	accounts, err := NewSQLAccountStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	accounts.maxLiveTokens = 2
	ctx := context.Background()
	if err := accounts.CreateAccount(ctx, Account{
		Email: "token-cap@x.com", Role: RoleAccountUser, TenantID: "acme", Status: AccountActive,
	}, "password-123"); err != nil {
		t.Fatal(err)
	}
	if _, err := accounts.CreateToken(ctx, "token-cap@x.com", time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := accounts.CreateToken(ctx, "token-cap@x.com", time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := accounts.CreateToken(ctx, "token-cap@x.com", time.Hour); err == nil {
		t.Fatal("live token overflow was accepted")
	} else if !strings.Contains(err.Error(), "live auth tokens exceed maximum of 2") {
		t.Fatalf("unexpected overflow error: %v", err)
	}
	if _, err := sessions.db.ExecContext(ctx, "UPDATE auth_tokens SET expires_at = 1"); err != nil {
		t.Fatal(err)
	}
	if _, err := accounts.CreateToken(ctx, "token-cap@x.com", time.Hour); err != nil {
		t.Fatalf("expired tokens did not free a live slot: %v", err)
	}
	if _, err := accounts.CreateToken(ctx, "missing@x.com", time.Hour); err == nil {
		t.Fatal("unknown account was issued a token")
	}
}

func TestSQLAccountStoreRejectsAccountAndTenantOverflow(t *testing.T) {
	sessions := newTestSQLStore(t)
	accounts, err := NewSQLAccountStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	accounts.maxAccounts = 2
	accounts.maxTenants = 2
	ctx := context.Background()
	acct := func(email string) Account {
		return Account{Email: email, Role: RoleAccountUser, TenantID: "acme", Status: AccountActive}
	}
	if err := accounts.CreateAccount(ctx, acct("a1@x.com"), "password-123"); err != nil {
		t.Fatal(err)
	}
	if err := accounts.CreateAccount(ctx, acct("a2@x.com"), "password-123"); err != nil {
		t.Fatal(err)
	}
	if err := accounts.CreateAccount(ctx, acct("a3@x.com"), "password-123"); err == nil {
		t.Fatal("account overflow was accepted")
	} else if !strings.Contains(err.Error(), "accounts exceed maximum of 2") {
		t.Fatalf("unexpected account overflow error: %v", err)
	}
	if err := accounts.CreateAccount(ctx, acct("a1@x.com"), "password-123"); err == nil {
		t.Fatal("duplicate account at cap was not a conflict")
	} else if !strings.Contains(err.Error(), "a1@x.com") {
		t.Fatalf("duplicate account at cap wrong: %v", err)
	}
	if err := accounts.CreateTenant(ctx, "t1", "One"); err != nil {
		t.Fatal(err)
	}
	if err := accounts.CreateTenant(ctx, "t2", "Two"); err != nil {
		t.Fatal(err)
	}
	if err := accounts.CreateTenant(ctx, "t3", "Three"); err == nil {
		t.Fatal("tenant overflow was accepted")
	} else if !strings.Contains(err.Error(), "tenants exceed maximum of 2") {
		t.Fatalf("unexpected tenant overflow error: %v", err)
	}
	if err := accounts.CreateTenant(ctx, "t1", "One"); err == nil {
		t.Fatal("duplicate tenant at cap was not a conflict")
	} else if !strings.Contains(err.Error(), "t1") {
		t.Fatalf("duplicate tenant at cap wrong: %v", err)
	}
}

func TestSQLAccountStoreRejectsSettingOverflow(t *testing.T) {
	sessions := newTestSQLStore(t)
	accounts, err := NewSQLAccountStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	accounts.maxSettings = 2
	ctx := context.Background()
	if err := accounts.SetSetting(ctx, "one", "1"); err != nil {
		t.Fatal(err)
	}
	if err := accounts.SetSetting(ctx, "two", "2"); err != nil {
		t.Fatal(err)
	}
	if err := accounts.SetSetting(ctx, "three", "3"); err == nil {
		t.Fatal("settings overflow was accepted")
	} else if !strings.Contains(err.Error(), "settings exceed maximum of 2") {
		t.Fatalf("unexpected overflow error: %v", err)
	}
	if err := accounts.SetSetting(ctx, "one", "replaced"); err != nil {
		t.Fatalf("replacing an existing setting was blocked by the cap: %v", err)
	}
	if err := accounts.SetSetting(ctx, "one", strings.Repeat("x", MaxSettingValueBytes+1)); err == nil {
		t.Fatal("oversized setting value was accepted")
	}
}
