package server

import (
	"context"
	"fmt"

	appidentity "github.com/cc-auto-agent/harness-core/pkg/app/identity"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
)

// legacyIdentityAdminRepository is private migration glue for deployments
// which still provide Config.Accounts. It maps the legacy storage facade to
// identity.AdminRepository; no application package imports storage or SQL.
// A dedicated SQL adapter belongs to the later repository migration.
type legacyIdentityAdminRepository struct{ store storage.AccountStore }

var _ appidentity.AdminRepository = (*legacyIdentityAdminRepository)(nil)

func newLegacyIdentityAdminRepository(store storage.AccountStore) (*legacyIdentityAdminRepository, error) {
	if store == nil {
		return nil, fmt.Errorf("legacy identity admin compatibility requires an account store")
	}
	return &legacyIdentityAdminRepository{store: store}, nil
}

func (repository *legacyIdentityAdminRepository) ListAccounts(ctx context.Context) ([]appidentity.Account, error) {
	accounts, err := repository.store.ListAccounts(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]appidentity.Account, 0, len(accounts))
	for _, account := range accounts {
		// Deliberately preserve potentially corrupt persisted values for the
		// application service to validate and classify as an operational error.
		result = append(result, appidentity.Account{
			ID: account.AccountID, Email: account.Email, Role: appidentity.Role(account.Role),
			TenantID: account.TenantID, Status: appidentity.Status(account.Status),
			MustChangePassword: account.MustChangePassword, CreatedAt: account.CreatedAt,
		})
	}
	return result, nil
}

func (repository *legacyIdentityAdminRepository) CreateAccount(ctx context.Context, account appidentity.Account, password string) error {
	return repository.store.CreateAccount(ctx, storage.Account{
		AccountID: account.ID, Email: account.Email, Role: string(account.Role), TenantID: account.TenantID,
		Status: string(account.Status), MustChangePassword: account.MustChangePassword, CreatedAt: account.CreatedAt,
	}, password)
}

func (repository *legacyIdentityAdminRepository) SetAccountPassword(ctx context.Context, accountID, password string) error {
	return repository.store.SetAccountPassword(ctx, accountID, password)
}

func (repository *legacyIdentityAdminRepository) SetAccountStatus(ctx context.Context, accountID string, status appidentity.Status) error {
	return repository.store.SetAccountStatus(ctx, accountID, string(status))
}

func (repository *legacyIdentityAdminRepository) ListTenants(ctx context.Context) ([]appidentity.Tenant, error) {
	tenants, err := repository.store.ListTenants(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]appidentity.Tenant, 0, len(tenants))
	for _, tenant := range tenants {
		result = append(result, appidentity.Tenant{ID: tenant.ID, Name: tenant.Name, CreatedAt: tenant.CreatedAt})
	}
	return result, nil
}

func (repository *legacyIdentityAdminRepository) CreateTenant(ctx context.Context, id, name string) error {
	return repository.store.CreateTenant(ctx, id, name)
}
