package server

import (
	"context"
	"fmt"
	"time"

	appidentity "github.com/cc-auto-agent/harness-core/pkg/app/identity"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
)

// legacyIdentityCredentials is temporary migration glue owned by the legacy
// server facade. It keeps the old storage.AccountStore API usable while the
// application service and transport move to their new boundaries. It is not a
// SQL adapter: the concrete SQL repository migration remains a later phase.
type legacyIdentityCredentials struct{ store storage.AccountStore }

var _ appidentity.CredentialPort = (*legacyIdentityCredentials)(nil)

func newLegacyIdentityCredentials(store storage.AccountStore) (*legacyIdentityCredentials, error) {
	if store == nil {
		return nil, fmt.Errorf("legacy identity compatibility requires an account store")
	}
	return &legacyIdentityCredentials{store: store}, nil
}

func (credentials *legacyIdentityCredentials) Authenticate(ctx context.Context, command appidentity.LoginCommand) (appidentity.Account, error) {
	account, err := credentials.store.GetAccount(ctx, command.AccountID)
	if err != nil {
		// Preserve one bcrypt comparison for unknown-account timing parity.
		_ = storage.VerifyPasswordOrDummy("", command.Password)
		return appidentity.Account{}, appidentity.ErrInvalidCredentials
	}
	if !storage.VerifyPasswordOrDummy(account.PasswordHash, command.Password) {
		return appidentity.Account{}, appidentity.ErrInvalidCredentials
	}
	return legacyIdentityAccount(account)
}

func (credentials *legacyIdentityCredentials) IssueToken(ctx context.Context, accountID string, ttl time.Duration) (string, error) {
	return credentials.store.CreateToken(ctx, accountID, ttl)
}

func (credentials *legacyIdentityCredentials) ActivateInitial(ctx context.Context, command appidentity.ActivateCommand, ttl time.Duration) (appidentity.ActivationResult, error) {
	// Capture and map the public account view before the transaction begins.
	// After ActivateInitialAccount commits, no additional I/O is performed, so
	// a client can never see an error caused by a post-commit account read.
	account, err := credentials.store.GetAccount(ctx, command.AccountID)
	if err != nil {
		return appidentity.ActivationResult{}, err
	}
	view, err := legacyIdentityAccount(account)
	if err != nil {
		return appidentity.ActivationResult{}, err
	}
	token, err := credentials.store.ActivateInitialAccount(ctx, command.AccountID, command.Password, ttl)
	if err != nil {
		return appidentity.ActivationResult{}, err
	}
	view.Status = appidentity.StatusActive
	view.MustChangePassword = false
	return appidentity.ActivationResult{Token: token, Account: view}, nil
}

func legacyIdentityAccount(account storage.Account) (appidentity.Account, error) {
	role, err := appidentity.ParseRole(account.Role)
	if err != nil {
		return appidentity.Account{}, err
	}
	status, err := appidentity.ParseStatus(account.Status)
	if err != nil {
		return appidentity.Account{}, err
	}
	return appidentity.Account{ID: account.AccountID, Email: account.Email, Role: role,
		TenantID: account.TenantID, Status: status, MustChangePassword: account.MustChangePassword,
		CreatedAt: account.CreatedAt}, nil
}
