package identity

import (
	"context"
	"fmt"
	"strings"
)

// AdminService coordinates the account and tenant administration use cases.
// Authorization decisions, tenant isolation, and input rules live here so an
// HTTP handler and a future CLI or RPC adapter share exactly one policy.
type AdminService struct{ repository AdminRepository }

var _ AdminUseCases = (*AdminService)(nil)

func NewAdminService(repository AdminRepository) (*AdminService, error) {
	if repository == nil {
		return nil, fmt.Errorf("identity admin service requires an admin repository")
	}
	return &AdminService{repository: repository}, nil
}

func (service *AdminService) ListAccounts(ctx context.Context, command ListAccountsCommand) (ListAccountsResult, error) {
	if err := requireTenantOperator(command.Actor); err != nil {
		return ListAccountsResult{}, err
	}
	accounts, err := service.repository.ListAccounts(ctx)
	if err != nil {
		return ListAccountsResult{}, err
	}
	filtered := make([]Account, 0, len(accounts))
	for _, account := range accounts {
		if !account.Role.Valid() || !account.Status.Valid() {
			return ListAccountsResult{}, fmt.Errorf("stored account has an invalid role or status")
		}
		if command.Actor.Role == RoleTenantAdmin && account.TenantID != command.Actor.TenantID {
			continue
		}
		filtered = append(filtered, account)
	}
	return ListAccountsResult{Accounts: filtered}, nil
}

func (service *AdminService) CreateAccount(ctx context.Context, command CreateAccountCommand) (CreateAccountResult, error) {
	if err := requirePlatformAdmin(command.Actor); err != nil {
		return CreateAccountResult{}, err
	}
	accountID := command.AccountID
	if accountID == "" {
		accountID = command.Email
	}
	if err := validateAdminAccountID(accountID); err != nil {
		return CreateAccountResult{}, err
	}
	if strings.ContainsRune(command.Email, '\x00') {
		return CreateAccountResult{}, invalidAdminInput("account identifier contains NUL")
	}
	if command.Email != "" && !strings.Contains(command.Email, "@") {
		return CreateAccountResult{}, invalidAdminInput("account email is not valid")
	}
	if !command.Role.Valid() {
		return CreateAccountResult{}, invalidAdminInput("unknown account role %q", command.Role)
	}
	if err := validateTenantID(command.TenantID); err != nil {
		return CreateAccountResult{}, err
	}
	if err := validateAdministrativePassword(command.Password); err != nil {
		return CreateAccountResult{}, err
	}
	account := Account{ID: accountID, Email: command.Email, Role: command.Role, TenantID: command.TenantID, Status: StatusActive}
	if err := service.repository.CreateAccount(ctx, account, command.Password); err != nil {
		return CreateAccountResult{}, err
	}
	return CreateAccountResult{Account: account}, nil
}

func (service *AdminService) SetAccountPassword(ctx context.Context, command SetAccountPasswordCommand) error {
	if command.Actor.AccountID != command.AccountID {
		if err := requirePlatformAdmin(command.Actor); err != nil {
			return err
		}
	}
	if err := validateAdminAccountID(command.AccountID); err != nil {
		return err
	}
	if err := validateAdministrativePassword(command.Password); err != nil {
		return err
	}
	return service.repository.SetAccountPassword(ctx, command.AccountID, command.Password)
}

func (service *AdminService) SetAccountStatus(ctx context.Context, command SetAccountStatusCommand) (SetAccountStatusResult, error) {
	if err := requirePlatformAdmin(command.Actor); err != nil {
		return SetAccountStatusResult{}, err
	}
	if err := validateAdminAccountID(command.AccountID); err != nil {
		return SetAccountStatusResult{}, err
	}
	if command.Status != StatusActive && command.Status != StatusDisabled {
		return SetAccountStatusResult{}, invalidAdminInput("status must be active or disabled")
	}
	if err := service.repository.SetAccountStatus(ctx, command.AccountID, command.Status); err != nil {
		return SetAccountStatusResult{}, err
	}
	return SetAccountStatusResult{AccountID: command.AccountID, Status: command.Status}, nil
}

func (service *AdminService) ListTenants(ctx context.Context, command ListTenantsCommand) (ListTenantsResult, error) {
	if err := requirePlatformAdmin(command.Actor); err != nil {
		return ListTenantsResult{}, err
	}
	tenants, err := service.repository.ListTenants(ctx)
	if err != nil {
		return ListTenantsResult{}, err
	}
	return ListTenantsResult{Tenants: tenants}, nil
}

func (service *AdminService) CreateTenant(ctx context.Context, command CreateTenantCommand) (CreateTenantResult, error) {
	if err := requirePlatformAdmin(command.Actor); err != nil {
		return CreateTenantResult{}, err
	}
	if err := validateTenantInput(command.ID, command.Name); err != nil {
		return CreateTenantResult{}, err
	}
	if err := service.repository.CreateTenant(ctx, command.ID, command.Name); err != nil {
		return CreateTenantResult{}, err
	}
	return CreateTenantResult{Tenant: Tenant{ID: command.ID, Name: command.Name}}, nil
}
