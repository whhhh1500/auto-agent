package storage

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	sqlsettings "github.com/whhhh1500/auto-agent/pkg/adapter/sql/settings"
	core "github.com/whhhh1500/auto-agent/pkg/core"
	"math/big"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"golang.org/x/crypto/bcrypt"
)

var (
	dummyPasswordOnce  sync.Once
	dummyPasswordHash  []byte
	errAccountNotFound = errors.New("account not found")
)

// accountNotFoundError keeps the public GetAccount diagnostic stable while
// allowing native storage callers to classify the absence without parsing text.
type accountNotFoundError struct{ accountID string }

func (e accountNotFoundError) Error() string { return fmt.Sprintf("account %q not found", e.accountID) }

func (e accountNotFoundError) Is(target error) bool { return target == errAccountNotFound }

// Account roles, from highest authority down.
const (
	RoleAccountAdmin       = "admin"        // platform operator: owns the global scope
	RoleAccountTenantAdmin = "tenant_admin" // tenant operator
	RoleAccountUser        = "user"         // end user inside one tenant
)

// Account statuses.
const (
	AccountPendingActivation = "pending_activation"
	AccountActive            = "active"
	AccountDisabled          = "disabled"
)

// Account is one console/identity record. PasswordHash is only populated by
// reads that need verification; listings never include it.
type Account struct {
	AccountID          string    `json:"account"`
	Email              string    `json:"email,omitempty"`
	PasswordHash       string    `json:"-"`
	Role               string    `json:"role"`
	TenantID           string    `json:"tenant_id"`
	Status             string    `json:"status"`
	MustChangePassword bool      `json:"must_change_password"`
	CreatedAt          time.Time `json:"created_at"`
}

// TenantView is one tenant record.
type TenantView struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

// AccountStore persists console accounts, tenants, and deployment settings.
// It is the durable half of the built-in identity store; the transport layer
// adds login routes and a bearer-token Authenticator on top.
type AccountStore interface {
	CreateAccount(ctx context.Context, account Account, password string) error
	GetAccount(ctx context.Context, accountID string) (Account, error)
	ListAccounts(ctx context.Context) ([]Account, error)
	SetAccountStatus(ctx context.Context, accountID, status string) error
	SetAccountPassword(ctx context.Context, accountID, password string) error
	ActivateInitialAccount(ctx context.Context, accountID, password string, ttl time.Duration) (token string, err error)
	CountAccounts(ctx context.Context) (int, error)

	CreateTenant(ctx context.Context, id, name string) error
	ListTenants(ctx context.Context) ([]TenantView, error)

	CreateToken(ctx context.Context, accountID string, ttl time.Duration) (token string, err error)
	ResolveToken(ctx context.Context, token string) (Account, error)

	SetSetting(ctx context.Context, key, value string) error
	GetSetting(ctx context.Context, key string) (string, bool, error)
}

var (
	sqlInsertAccount       = sqlQuery{"INSERT INTO accounts (account_id, email, password_hash, role, tenant_id, status, must_change_password, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"}
	sqlSelectAccount       = sqlQuery{"SELECT account_id, email, password_hash, role, tenant_id, status, must_change_password, created_at FROM accounts WHERE account_id = ?"}
	sqlListAccounts        = sqlQuery{"SELECT account_id, email, password_hash, role, tenant_id, status, must_change_password, created_at FROM accounts ORDER BY created_at"}
	sqlAccountStatus       = sqlQuery{"UPDATE accounts SET status = ? WHERE account_id = ?"}
	sqlAccountPass         = sqlQuery{"UPDATE accounts SET password_hash = ? WHERE account_id = ?"}
	sqlAccountActivate     = sqlQuery{"UPDATE accounts SET password_hash = ?, status = 'active', must_change_password = 0 WHERE account_id = ? AND status = 'pending_activation' AND must_change_password = 1"}
	sqlCountAccounts       = sqlQuery{"SELECT COUNT(*) FROM accounts"}
	sqlInsertTenant        = sqlQuery{"INSERT INTO tenants (id, name, created_at) VALUES (?, ?, ?)"}
	sqlListTenants         = sqlQuery{"SELECT id, name, created_at FROM tenants ORDER BY created_at"}
	sqlCountTenants        = sqlQuery{"SELECT COUNT(*) FROM tenants"}
	sqlGetTenant           = sqlQuery{"SELECT id FROM tenants WHERE id = ?"}
	sqlInsertToken         = sqlQuery{"INSERT INTO auth_tokens (token_hash, account_id, expires_at) VALUES (?, ?, ?)"}
	sqlPurgeTokens         = sqlQuery{"DELETE FROM auth_tokens WHERE expires_at <= ?"}
	sqlCountLiveTokens     = sqlQuery{"SELECT COUNT(*) FROM auth_tokens WHERE expires_at > ?"}
	sqlSelectToken         = sqlQuery{"SELECT t.account_id, t.expires_at, a.email, a.password_hash, a.role, a.tenant_id, a.status, a.must_change_password FROM auth_tokens t JOIN accounts a ON a.account_id = t.account_id WHERE t.token_hash = ?"}
	sqlDeleteToken         = sqlQuery{"DELETE FROM auth_tokens WHERE token_hash = ?"}
	sqlDeleteTokensAccount = sqlQuery{"DELETE FROM auth_tokens WHERE account_id = ?"}
)

// VerifyPassword reports whether a password matches a stored bcrypt hash.
func VerifyPassword(hash, password string) bool {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) == nil
}

// VerifyPasswordOrDummy performs one bcrypt comparison even when no account
// hash exists, reducing login timing differences between unknown accounts and
// wrong passwords. It returns false for the dummy path.
func VerifyPasswordOrDummy(hash, password string) bool {
	if hash != "" {
		return VerifyPassword(hash, password)
	}
	dummyPasswordOnce.Do(func() {
		dummyPasswordHash, _ = bcrypt.GenerateFromPassword([]byte("harness-dummy-password"), bcrypt.DefaultCost)
	})
	_ = bcrypt.CompareHashAndPassword(dummyPasswordHash, []byte(password))
	return false
}

// SQLAccountStore implements AccountStore on the shared schema.
type SQLAccountStore struct {
	db      *sql.DB
	dialect SQLDialect
	// Cipher, when set, transparently encrypts setting values at rest.
	Cipher        *Cipher
	maxLiveTokens int
	maxAccounts   int
	maxTenants    int
	maxSettings   int
}

func NewSQLAccountStore(db *sql.DB, dialect SQLDialect) (*SQLAccountStore, error) {
	if db == nil {
		return nil, fmt.Errorf("sql account store requires a database handle")
	}
	if err := validateSQLDialect(dialect); err != nil {
		return nil, err
	}
	return &SQLAccountStore{db: db, dialect: dialect}, nil
}

func (s *SQLAccountStore) CreateAccount(ctx context.Context, account Account, password string) error {
	// Pre-v32 callers used Email as identity. Preserve source compatibility
	// while all new APIs populate AccountID explicitly.
	if account.AccountID == "" && account.Email != "" {
		account.AccountID = account.Email
	}
	if err := validateAccountID(account.AccountID); err != nil {
		return err
	}
	if err := validateScopeIdentifier(core.ScopeUser, account.AccountID); err != nil {
		return fmt.Errorf("account id cannot be used as a scope id: %w", err)
	}
	if err := validateOptionalAccountEmail(account.Email); err != nil {
		return err
	}
	if err := validateScopeIdentifier(core.ScopeTenant, account.TenantID); err != nil {
		return fmt.Errorf("account tenant id is invalid: %w", err)
	}
	switch account.Role {
	case RoleAccountAdmin, RoleAccountTenantAdmin, RoleAccountUser:
	default:
		return fmt.Errorf("unknown account role %q", account.Role)
	}
	if err := validateAccountPassword(password); err != nil {
		return err
	}
	if account.Status == "" {
		account.Status = AccountActive
	}
	if err := validateAccountStatus(account.Status); err != nil {
		return err
	}
	if (account.Status == AccountPendingActivation) != account.MustChangePassword {
		return fmt.Errorf("pending activation status and password-change requirement must match")
	}
	count, err := s.CountAccounts(ctx)
	if err != nil {
		return err
	}
	if count >= s.accountCap() {
		_, getErr := s.GetAccount(ctx, account.AccountID)
		if getErr == nil {
			return fmt.Errorf("%w: %s", core.ErrSessionConflict, account.AccountID)
		}
		if !errors.Is(getErr, errAccountNotFound) {
			return getErr
		}
		return fmt.Errorf("accounts exceed maximum of %d", s.accountCap())
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	if account.CreatedAt.IsZero() {
		account.CreatedAt = time.Now().UTC()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	_, err = tx.ExecContext(ctx, sqlInsertAccount.bind(s.dialect),
		account.AccountID, nullableString(account.Email), string(hash), account.Role, account.TenantID,
		account.Status, boolInt(account.MustChangePassword), account.CreatedAt.UnixMilli(),
	)
	if err != nil {
		return duplicateAsConflict(account.AccountID, err)
	}
	// A newly active account can become the current queued-run principal. A
	// pending or disabled account cannot, so those creations do not invalidate
	// an existing delivery snapshot.
	if account.Status == AccountActive {
		if err := bumpAuthorizationEpoch(ctx, tx, s.dialect); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *SQLAccountStore) GetAccount(ctx context.Context, accountID string) (Account, error) {
	if err := validateAccountID(accountID); err != nil {
		return Account{}, err
	}
	row := s.db.QueryRowContext(ctx, sqlSelectAccount.bind(s.dialect), accountID)
	var account Account
	var email sql.NullString
	var createdMillis int64
	if err := row.Scan(&account.AccountID, &email, &account.PasswordHash, &account.Role, &account.TenantID,
		&account.Status, &account.MustChangePassword, &createdMillis); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Account{}, accountNotFoundError{accountID: accountID}
		}
		return Account{}, err
	}
	account.Email = email.String
	account.CreatedAt = time.UnixMilli(createdMillis).UTC()
	return account, nil
}

func (s *SQLAccountStore) ListAccounts(ctx context.Context) ([]Account, error) {
	rows, err := s.db.QueryContext(ctx, sqlListAccounts.bind(s.dialect))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Account{}
	for rows.Next() {
		var account Account
		var email sql.NullString
		var createdMillis int64
		if err := rows.Scan(&account.AccountID, &email, &account.PasswordHash, &account.Role, &account.TenantID,
			&account.Status, &account.MustChangePassword, &createdMillis); err != nil {
			return nil, err
		}
		account.Email = email.String
		account.CreatedAt = time.UnixMilli(createdMillis).UTC()
		out = append(out, account)
	}
	return out, rows.Err()
}

func (s *SQLAccountStore) SetAccountStatus(ctx context.Context, accountID, status string) error {
	if err := validateAccountID(accountID); err != nil {
		return err
	}
	if err := validateAccountStatus(status); err != nil {
		return err
	}
	if status == AccountPendingActivation {
		return fmt.Errorf("pending activation can only be set when an account is created")
	}
	account, err := s.GetAccount(ctx, accountID)
	if err != nil {
		return err
	}
	if account.MustChangePassword {
		return fmt.Errorf("pending account must change its password before status changes")
	}
	if account.Status == status {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, sqlAccountStatus.bind(s.dialect), status, accountID)
	if err != nil {
		return err
	}
	if err := requireAffected(result, fmt.Sprintf("account %q not found", accountID)); err != nil {
		return err
	}
	if err := bumpAuthorizationEpoch(ctx, tx, s.dialect); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SQLAccountStore) SetAccountPassword(ctx context.Context, accountID, password string) error {
	if err := validateAccountID(accountID); err != nil {
		return err
	}
	if err := validateAccountPassword(password); err != nil {
		return err
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	// Updating a password and revoking its sessions is one security boundary:
	// an error while revoking must not leave a new password with usable old
	// tokens. Keep both writes in the same unit of work.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, sqlAccountPass.bind(s.dialect), string(hash), accountID)
	if err != nil {
		return err
	}
	if err := requireAffected(result, fmt.Sprintf("account %q not found", accountID)); err != nil {
		return err
	}
	// Password rotation invalidates every outstanding session for the account.
	if _, err := tx.ExecContext(ctx, sqlDeleteTokensAccount.bind(s.dialect), accountID); err != nil {
		return err
	}
	return tx.Commit()
}

// ActivateInitialAccount replaces the one-time password, activates the
// account, revokes restricted tokens, and returns a normal token atomically.
func (s *SQLAccountStore) ActivateInitialAccount(ctx context.Context, accountID, password string, ttl time.Duration) (string, error) {
	if err := validateAccountID(accountID); err != nil {
		return "", err
	}
	if utf8.RuneCountInString(password) < MinInitialPasswordRunes {
		return "", fmt.Errorf("password must be at least 12 characters")
	}
	if err := validateAccountPassword(password); err != nil {
		return "", err
	}
	if ttl <= 0 || ttl > MaxTokenTTL {
		return "", fmt.Errorf("token ttl must be positive and not exceed %s", MaxTokenTTL)
	}
	account, err := s.GetAccount(ctx, accountID)
	if err != nil {
		return "", err
	}
	if account.Status != AccountPendingActivation || !account.MustChangePassword {
		return "", fmt.Errorf("account activation is not pending")
	}
	if VerifyPassword(account.PasswordHash, password) {
		return "", fmt.Errorf("new password must differ from the initial password")
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	token := hex.EncodeToString(raw)
	tokenHash := sha256.Sum256([]byte(token))
	now := time.Now().UTC()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, sqlAccountActivate.bind(s.dialect), string(hash), accountID)
	if err != nil {
		return "", err
	}
	if err := requireAffected(result, "account activation is no longer pending"); err != nil {
		return "", err
	}
	if err := bumpAuthorizationEpoch(ctx, tx, s.dialect); err != nil {
		return "", err
	}
	if _, err := tx.ExecContext(ctx, sqlDeleteTokensAccount.bind(s.dialect), accountID); err != nil {
		return "", err
	}
	if _, err := tx.ExecContext(ctx, sqlPurgeTokens.bind(s.dialect), now.UnixMilli()); err != nil {
		return "", err
	}
	var live int
	if err := tx.QueryRowContext(ctx, sqlCountLiveTokens.bind(s.dialect), now.UnixMilli()).Scan(&live); err != nil {
		return "", err
	}
	if live >= s.liveTokenCap() {
		return "", fmt.Errorf("live auth tokens exceed maximum of %d", s.liveTokenCap())
	}
	if _, err := tx.ExecContext(ctx, sqlInsertToken.bind(s.dialect),
		hex.EncodeToString(tokenHash[:]), accountID, now.Add(ttl).UnixMilli()); err != nil {
		return "", err
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	return token, nil
}

func (s *SQLAccountStore) CountAccounts(ctx context.Context) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, sqlCountAccounts.bind(s.dialect)).Scan(&count)
	return count, err
}

func (s *SQLAccountStore) CreateTenant(ctx context.Context, id, name string) error {
	if err := validateScopeIdentifier(core.ScopeTenant, id); err != nil {
		return fmt.Errorf("tenant id is invalid: %w", err)
	}
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("tenant name is empty")
	}
	if err := validateSQLTextFilter("tenant name", name); err != nil {
		return err
	}
	var count int
	if err := s.db.QueryRowContext(ctx, sqlCountTenants.bind(s.dialect)).Scan(&count); err != nil {
		return err
	}
	if count >= s.tenantCap() {
		var existing string
		getErr := s.db.QueryRowContext(ctx, sqlGetTenant.bind(s.dialect), id).Scan(&existing)
		if getErr == nil {
			return fmt.Errorf("%w: %s", core.ErrSessionConflict, id)
		}
		if !errors.Is(getErr, sql.ErrNoRows) {
			return getErr
		}
		return fmt.Errorf("tenants exceed maximum of %d", s.tenantCap())
	}
	_, err := s.db.ExecContext(ctx, sqlInsertTenant.bind(s.dialect), id, name, time.Now().UTC().UnixMilli())
	return duplicateAsConflict(id, err)
}

func validateScopeIdentifier(kind core.ScopeKind, id string) error {
	_, err := core.NewScopePath(core.ScopeRef{Kind: kind, ID: id})
	return err
}

func validateAccountID(accountID string) error {
	if strings.TrimSpace(accountID) == "" {
		return fmt.Errorf("account id is empty")
	}
	return validateSQLTextFilter("account id", accountID)
}

func validateOptionalAccountEmail(email string) error {
	if email == "" {
		return nil
	}
	if !strings.Contains(email, "@") {
		return fmt.Errorf("account email is not valid")
	}
	return validateSQLTextFilter("account email", email)
}

func validateAccountStatus(status string) error {
	switch status {
	case AccountPendingActivation, AccountActive, AccountDisabled:
		return nil
	default:
		return fmt.Errorf("unknown account status %q", status)
	}
}

func validateAccountPassword(password string) error {
	if strings.TrimSpace(password) == "" {
		return fmt.Errorf("account password is empty")
	}
	if strings.ContainsRune(password, '\x00') {
		return fmt.Errorf("account password contains NUL")
	}
	if len(password) > MaxAccountPasswordBytes {
		return fmt.Errorf("account password exceeds %d bytes", MaxAccountPasswordBytes)
	}
	return nil
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

const maxRawBearerTokenBytes = 128

func validateRawBearerToken(token string) error {
	if token == "" {
		return fmt.Errorf("empty token")
	}
	if len(token) > maxRawBearerTokenBytes {
		return fmt.Errorf("token is invalid")
	}
	for _, char := range token {
		if unicode.IsSpace(char) || unicode.IsControl(char) {
			return fmt.Errorf("token is invalid")
		}
	}
	return nil
}

func (s *SQLAccountStore) ListTenants(ctx context.Context) ([]TenantView, error) {
	rows, err := s.db.QueryContext(ctx, sqlListTenants.bind(s.dialect))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []TenantView{}
	for rows.Next() {
		var tenant TenantView
		var createdMillis int64
		if err := rows.Scan(&tenant.ID, &tenant.Name, &createdMillis); err != nil {
			return nil, err
		}
		tenant.CreatedAt = time.UnixMilli(createdMillis).UTC()
		out = append(out, tenant)
	}
	return out, rows.Err()
}

const (
	MaxTokenTTL             = 30 * 24 * time.Hour
	MaxLiveAuthTokens       = 4096
	MaxAccounts             = 4096
	MaxTenants              = 1024
	MaxSettings             = 256
	MaxSettingValueBytes    = 256 << 10
	MinInitialPasswordRunes = 12
	MaxAccountPasswordBytes = 72
)

// CreateToken issues an opaque bearer token and stores only its hash.
func (s *SQLAccountStore) CreateToken(ctx context.Context, accountID string, ttl time.Duration) (string, error) {
	if err := validateAccountID(accountID); err != nil {
		return "", err
	}
	if ttl <= 0 {
		return "", fmt.Errorf("token ttl must be positive")
	}
	if ttl > MaxTokenTTL {
		return "", fmt.Errorf("token ttl exceeds %s", MaxTokenTTL)
	}
	account, err := s.GetAccount(ctx, accountID)
	if err != nil {
		return "", err
	}
	if account.Status != AccountActive && account.Status != AccountPendingActivation {
		return "", fmt.Errorf("account %q is disabled", accountID)
	}
	now := time.Now().UTC().UnixMilli()
	// Opportunistic purge keeps the table bounded without a background job.
	if _, err := s.db.ExecContext(ctx, sqlPurgeTokens.bind(s.dialect), now); err != nil {
		return "", err
	}
	var live int
	if err := s.db.QueryRowContext(ctx, sqlCountLiveTokens.bind(s.dialect), now).Scan(&live); err != nil {
		return "", err
	}
	if live >= s.liveTokenCap() {
		return "", fmt.Errorf("live auth tokens exceed maximum of %d", s.liveTokenCap())
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	token := hex.EncodeToString(raw)
	sum := sha256.Sum256([]byte(token))
	_, err = s.db.ExecContext(ctx, sqlInsertToken.bind(s.dialect),
		hex.EncodeToString(sum[:]), accountID, time.Now().Add(ttl).UTC().UnixMilli(),
	)
	if err != nil {
		return "", err
	}
	return token, nil
}

func (s *SQLAccountStore) liveTokenCap() int {
	if s != nil && s.maxLiveTokens > 0 {
		return s.maxLiveTokens
	}
	return MaxLiveAuthTokens
}

func (s *SQLAccountStore) accountCap() int {
	if s != nil && s.maxAccounts > 0 {
		return s.maxAccounts
	}
	return MaxAccounts
}

func (s *SQLAccountStore) tenantCap() int {
	if s != nil && s.maxTenants > 0 {
		return s.maxTenants
	}
	return MaxTenants
}

func (s *SQLAccountStore) settingCap() int {
	if s != nil && s.maxSettings > 0 {
		return s.maxSettings
	}
	return MaxSettings
}

// RevokeToken deletes the token matching a raw bearer token (best-effort
// logout). Revoking an unknown token is not an error. Empty or malformed
// tokens fail closed before hashing.
func (s *SQLAccountStore) RevokeToken(ctx context.Context, token string) error {
	if err := validateRawBearerToken(token); err != nil {
		return err
	}
	sum := sha256.Sum256([]byte(token))
	_, err := s.db.ExecContext(ctx, sqlDeleteToken.bind(s.dialect), hex.EncodeToString(sum[:]))
	return err
}

// ResolveToken validates a bearer token and returns its account. Expired,
// unknown, and disabled accounts all fail closed.
func (s *SQLAccountStore) ResolveToken(ctx context.Context, token string) (Account, error) {
	if err := validateRawBearerToken(token); err != nil {
		return Account{}, err
	}
	sum := sha256.Sum256([]byte(token))
	row := s.db.QueryRowContext(ctx, sqlSelectToken.bind(s.dialect), hex.EncodeToString(sum[:]))
	var account Account
	var email sql.NullString
	var expires int64
	var passwordHash string
	if err := row.Scan(&account.AccountID, &expires, &email, &passwordHash, &account.Role, &account.TenantID,
		&account.Status, &account.MustChangePassword); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Account{}, fmt.Errorf("token is not valid")
		}
		return Account{}, err
	}
	account.Email = email.String
	_ = passwordHash
	if time.Now().UTC().UnixMilli() >= expires {
		return Account{}, fmt.Errorf("token expired")
	}
	if account.Status != AccountActive && account.Status != AccountPendingActivation {
		return Account{}, fmt.Errorf("account %q is disabled", account.AccountID)
	}
	return account, nil
}

func (s *SQLAccountStore) SetSetting(ctx context.Context, key, value string) error {
	repository, err := s.settingsRepository()
	if err != nil {
		return err
	}
	return repository.SetSetting(ctx, key, value)
}

func (s *SQLAccountStore) GetSetting(ctx context.Context, key string) (string, bool, error) {
	repository, err := s.settingsRepository()
	if err != nil {
		return "", false, err
	}
	return repository.GetSetting(ctx, key)
}

// settingsRepository is a compatibility bridge for AccountStore callers. It
// snapshots the current optional Cipher and configured setting cap on every
// call so legacy field mutation keeps its established behavior.
func (s *SQLAccountStore) settingsRepository() (*sqlsettings.Store, error) {
	options := sqlsettings.Options{
		DB: s.db, Dialect: s.dialect,
		MaxEntries: s.settingCap(), MaxValueBytes: MaxSettingValueBytes,
	}
	if s.Cipher != nil {
		options.Cipher = s.Cipher
	}
	return sqlsettings.New(options)
}

func requireAffected(result sql.Result, message string) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return fmt.Errorf("%s", message)
	}
	return nil
}

// PrincipalForAccount maps an account below the deployer's product root. The
// admin role owns the global prefix; tenant roles extend root with tenant/user
// ownership. An empty root defaults to the global scope for embedded setups.
func PrincipalForAccount(account Account, root []core.ScopeRef) (core.Principal, error) {
	if len(root) == 0 {
		root = []core.ScopeRef{{Kind: core.ScopeGlobal, ID: "global"}}
	}
	switch account.Role {
	case RoleAccountAdmin:
		scope, err := core.NewScopePath(root[0])
		if err != nil {
			return core.Principal{}, err
		}
		return core.Principal{
			SubjectID: account.AccountID, TenantID: account.TenantID, Scope: scope,
			Grants:     core.NewPermissionSet(core.PermRead, core.PermWrite, core.PermSend),
			Attributes: map[string]string{"role": account.Role, "account.status": account.Status},
		}, nil
	case RoleAccountTenantAdmin:
		segments := append([]core.ScopeRef(nil), root...)
		segments = append(segments, core.ScopeRef{Kind: core.ScopeTenant, ID: account.TenantID})
		scope, err := core.NewScopePath(segments...)
		if err != nil {
			return core.Principal{}, err
		}
		return core.Principal{
			SubjectID: account.AccountID, TenantID: account.TenantID, Scope: scope,
			Grants:     core.NewPermissionSet(core.PermRead, core.PermWrite),
			Attributes: map[string]string{"role": account.Role, "account.status": account.Status},
		}, nil
	case RoleAccountUser:
		segments := append([]core.ScopeRef(nil), root...)
		segments = append(segments,
			core.ScopeRef{Kind: core.ScopeTenant, ID: account.TenantID},
			core.ScopeRef{Kind: core.ScopeUser, ID: account.AccountID},
		)
		scope, err := core.NewScopePath(segments...)
		if err != nil {
			return core.Principal{}, err
		}
		return core.Principal{
			SubjectID: account.AccountID, TenantID: account.TenantID, Scope: scope,
			Grants:     core.NewPermissionSet(core.PermRead, core.PermWrite),
			Attributes: map[string]string{"role": account.Role, "account.status": account.Status},
		}, nil
	default:
		return core.Principal{}, fmt.Errorf("unknown account role %q", account.Role)
	}
}

// BootstrapAdmin remains as a compatibility helper for embedded tests. Server
// startup uses BootstrapInitialAdmin and the pre-migration table decision.
func BootstrapAdmin(ctx context.Context, store AccountStore, logf func(format string, args ...any)) (accountID, password string, created bool, err error) {
	password, err = randomPassword(16)
	if err != nil {
		return "", "", false, err
	}
	accountID, created, err = BootstrapAdminWithPassword(ctx, store, password, logf)
	if err != nil || !created {
		return accountID, "", created, err
	}
	return accountID, password, true, nil
}

// BootstrapAdminWithPassword creates the first administrator using a caller-
// supplied secret. It never logs or persists that secret outside the password
// hash; callers must obtain it from a protected configuration source.
func BootstrapAdminWithPassword(ctx context.Context, store AccountStore, password string, logf func(format string, args ...any)) (accountID string, created bool, err error) {
	count, err := store.CountAccounts(ctx)
	if err != nil {
		return "", false, err
	}
	if count > 0 {
		return "", false, nil
	}
	if strings.TrimSpace(password) == "" {
		return "", false, fmt.Errorf("initial administrator password is required")
	}
	digits, err := randomDigits(5)
	if err != nil {
		return "", false, err
	}
	accountID = "admin_" + digits
	// Seed the platform tenant rows; existing rows are not an error.
	_ = store.CreateTenant(ctx, "system", "Platform")
	_ = store.CreateTenant(ctx, "default", "Default Tenant")
	if err := store.CreateAccount(ctx, Account{
		AccountID: accountID, Role: RoleAccountAdmin, TenantID: "system", Status: AccountActive,
	}, password); err != nil {
		return "", false, err
	}
	if logf != nil {
		logf("auto-agent initial administrator created: %s", accountID)
		logf("The compatibility bootstrap password is returned to its caller and is never logged.")
	}
	return accountID, true, nil
}

type InitialAdminCredentials struct {
	AccountID string
	Password  string
	Created   bool
}

// BootstrapInitialAdmin creates a pending one-time administrator only when the
// accounts table was absent before schema migration. A store_meta write lock
// serializes concurrent first openers; the second observes the committed row.
func BootstrapInitialAdmin(ctx context.Context, store *SQLAccountStore, accountsTableExisted bool) (InitialAdminCredentials, error) {
	if accountsTableExisted {
		return InitialAdminCredentials{}, nil
	}
	if store == nil || store.db == nil {
		return InitialAdminCredentials{}, fmt.Errorf("initial administrator requires a SQL account store")
	}
	return bootstrapInitialAdmin(ctx, store.db, store.dialect, false)
}

func bootstrapInitialAdmin(ctx context.Context, db sqlSchemaExecutor, dialect SQLDialect, accountsTableExisted bool) (InitialAdminCredentials, error) {
	if accountsTableExisted {
		return InitialAdminCredentials{}, nil
	}
	password, err := randomPassword(20)
	if err != nil {
		return InitialAdminCredentials{}, err
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return InitialAdminCredentials{}, err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return InitialAdminCredentials{}, err
	}
	defer tx.Rollback()
	lockQuery := sqlQuery{"INSERT INTO store_meta (key, value) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value"}.bind(dialect)
	if _, err := tx.ExecContext(ctx, lockQuery, "initial_admin_lock", "1"); err != nil {
		return InitialAdminCredentials{}, fmt.Errorf("lock initial administrator bootstrap: %w", err)
	}
	var count int
	if err := tx.QueryRowContext(ctx, sqlCountAccounts.bind(dialect)).Scan(&count); err != nil {
		return InitialAdminCredentials{}, err
	}
	if count > 0 {
		if err := tx.Commit(); err != nil {
			return InitialAdminCredentials{}, err
		}
		return InitialAdminCredentials{}, nil
	}
	now := time.Now().UTC().UnixMilli()
	for _, tenant := range []struct{ id, name string }{{"system", "Platform"}, {"default", "Default Tenant"}} {
		query := sqlQuery{"INSERT INTO tenants (id, name, created_at) VALUES (?, ?, ?) ON CONFLICT (id) DO NOTHING"}.bind(dialect)
		if _, err := tx.ExecContext(ctx, query, tenant.id, tenant.name, now); err != nil {
			return InitialAdminCredentials{}, err
		}
	}
	var accountID string
	for attempt := 0; attempt < 16; attempt++ {
		digits, err := randomDigits(5)
		if err != nil {
			return InitialAdminCredentials{}, err
		}
		candidate := "admin_" + digits
		_, err = tx.ExecContext(ctx, sqlInsertAccount.bind(dialect), candidate, nil, string(hash),
			RoleAccountAdmin, "system", AccountPendingActivation, 1, now)
		if err == nil {
			accountID = candidate
			break
		}
		if !isDuplicateConstraint(err) {
			return InitialAdminCredentials{}, err
		}
	}
	if accountID == "" {
		return InitialAdminCredentials{}, fmt.Errorf("generate a unique initial administrator account")
	}
	if err := tx.Commit(); err != nil {
		return InitialAdminCredentials{}, err
	}
	return InitialAdminCredentials{AccountID: accountID, Password: password, Created: true}, nil
}

func randomDigits(n int) (string, error) {
	return randomAlphabet(n, "0123456789")
}

// randomPassword draws from an unambiguous alphabet (no 0/O/1/l/I).
func randomPassword(n int) (string, error) {
	const alphabet = "abcdefghijkmnpqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789"
	return randomAlphabet(n, alphabet)
}

func randomAlphabet(n int, alphabet string) (string, error) {
	if n < 0 || len(alphabet) == 0 {
		return "", fmt.Errorf("invalid random string parameters")
	}
	out := make([]byte, n)
	for i := range out {
		index, err := rand.Int(rand.Reader, big.NewInt(int64(len(alphabet))))
		if err != nil {
			return "", err
		}
		out[i] = alphabet[index.Int64()]
	}
	return string(out), nil
}
