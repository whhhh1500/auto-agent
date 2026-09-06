package identity

import (
	"context"
	"errors"
	"testing"
)

func TestAdminServiceListAccountsAuthorizesAndFiltersTenant(t *testing.T) {
	repository := &fakeAdminRepository{accounts: []Account{
		{ID: "platform", Role: RoleAdmin, TenantID: "system", Status: StatusActive},
		{ID: "acme-admin", Role: RoleTenantAdmin, TenantID: "acme", Status: StatusActive},
		{ID: "acme-user", Role: RoleUser, TenantID: "acme", Status: StatusDisabled},
		{ID: "other-user", Role: RoleUser, TenantID: "other", Status: StatusActive},
	}}
	service, err := NewAdminService(repository)
	if err != nil {
		t.Fatal(err)
	}

	platform, err := service.ListAccounts(context.Background(), ListAccountsCommand{Actor: AdminActor{Role: RoleAdmin}})
	if err != nil || len(platform.Accounts) != 4 {
		t.Fatalf("platform accounts=%#v err=%v", platform.Accounts, err)
	}
	tenant, err := service.ListAccounts(context.Background(), ListAccountsCommand{Actor: AdminActor{Role: RoleTenantAdmin, TenantID: "acme"}})
	if err != nil || len(tenant.Accounts) != 2 {
		t.Fatalf("tenant accounts=%#v err=%v", tenant.Accounts, err)
	}
	for _, account := range tenant.Accounts {
		if account.TenantID != "acme" {
			t.Fatalf("tenant filter leaked %q", account.TenantID)
		}
	}
	_, err = service.ListAccounts(context.Background(), ListAccountsCommand{Actor: AdminActor{Role: RoleUser, TenantID: "acme"}})
	if !errors.Is(err, ErrForbidden) {
		t.Fatalf("ordinary user list error=%v; want forbidden", err)
	}
}

func TestAdminServiceAllowsOnlySelfOrPlatformPasswordRotation(t *testing.T) {
	repository := &fakeAdminRepository{}
	service, err := NewAdminService(repository)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	if err := service.SetAccountPassword(ctx, SetAccountPasswordCommand{
		Actor: AdminActor{AccountID: "alice", Role: RoleUser}, AccountID: "alice", Password: "password-123",
	}); err != nil {
		t.Fatalf("self password change rejected: %v", err)
	}
	if len(repository.passwordChanges) != 1 || repository.passwordChanges[0].accountID != "alice" {
		t.Fatalf("self password call=%#v", repository.passwordChanges)
	}

	err = service.SetAccountPassword(ctx, SetAccountPasswordCommand{
		Actor: AdminActor{AccountID: "alice", Role: RoleUser}, AccountID: "bob", Password: "password-123",
	})
	if !errors.Is(err, ErrForbidden) || len(repository.passwordChanges) != 1 {
		t.Fatalf("cross-account user result=%v calls=%#v", err, repository.passwordChanges)
	}
	err = service.SetAccountPassword(ctx, SetAccountPasswordCommand{
		Actor: AdminActor{AccountID: "tenant-admin", Role: RoleTenantAdmin, TenantID: "acme"}, AccountID: "bob", Password: "password-123",
	})
	if !errors.Is(err, ErrForbidden) || len(repository.passwordChanges) != 1 {
		t.Fatalf("cross-account tenant-admin result=%v calls=%#v", err, repository.passwordChanges)
	}
	if err := service.SetAccountPassword(ctx, SetAccountPasswordCommand{
		Actor: AdminActor{AccountID: "platform", Role: RoleAdmin}, AccountID: "bob", Password: "password-123",
	}); err != nil {
		t.Fatalf("platform password change rejected: %v", err)
	}
	if len(repository.passwordChanges) != 2 || repository.passwordChanges[1].accountID != "bob" {
		t.Fatalf("platform password call=%#v", repository.passwordChanges)
	}
	err = service.SetAccountPassword(ctx, SetAccountPasswordCommand{
		Actor: AdminActor{AccountID: "alice", Role: RoleUser}, AccountID: "alice", Password: "short",
	})
	if !errors.Is(err, ErrInvalidInput) || len(repository.passwordChanges) != 2 {
		t.Fatalf("short password result=%v calls=%#v", err, repository.passwordChanges)
	}
}

func TestAdminServiceOwnsRoleStatusAndTenantValidation(t *testing.T) {
	repository := &fakeAdminRepository{}
	service, err := NewAdminService(repository)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	platform := AdminActor{AccountID: "platform", Role: RoleAdmin}

	_, err = service.CreateAccount(ctx, CreateAccountCommand{
		Actor: platform, AccountID: "alice", Password: "password-123", Role: Role("operator"), TenantID: "acme",
	})
	if !errors.Is(err, ErrInvalidInput) || len(repository.createdAccounts) != 0 {
		t.Fatalf("unknown role result=%v accounts=%#v", err, repository.createdAccounts)
	}
	_, err = service.CreateAccount(ctx, CreateAccountCommand{
		Actor: platform, Email: "alice@example.test", Password: "password-123", Role: RoleUser, TenantID: "",
	})
	if !errors.Is(err, ErrInvalidInput) || len(repository.createdAccounts) != 0 {
		t.Fatalf("empty tenant result=%v accounts=%#v", err, repository.createdAccounts)
	}
	created, err := service.CreateAccount(ctx, CreateAccountCommand{
		Actor: platform, Email: "alice@example.test", Password: "password-123", Role: RoleUser, TenantID: "acme",
	})
	if err != nil || created.Account.ID != "alice@example.test" || created.Account.Status != StatusActive {
		t.Fatalf("created=%#v err=%v", created, err)
	}
	_, err = service.SetAccountStatus(ctx, SetAccountStatusCommand{
		Actor: platform, AccountID: "alice@example.test", Status: StatusPendingActivation,
	})
	if !errors.Is(err, ErrInvalidInput) || len(repository.statusChanges) != 0 {
		t.Fatalf("pending status result=%v changes=%#v", err, repository.statusChanges)
	}
	_, err = service.CreateTenant(ctx, CreateTenantCommand{Actor: AdminActor{Role: RoleTenantAdmin, TenantID: "acme"}, ID: "new", Name: "New"})
	if !errors.Is(err, ErrForbidden) || len(repository.createdTenants) != 0 {
		t.Fatalf("tenant-admin create result=%v tenants=%#v", err, repository.createdTenants)
	}
}

type passwordChange struct {
	accountID string
	password  string
}

type statusChange struct {
	accountID string
	status    Status
}

type fakeAdminRepository struct {
	accounts        []Account
	tenants         []Tenant
	createdAccounts []Account
	passwordChanges []passwordChange
	statusChanges   []statusChange
	createdTenants  []Tenant
}

func (repository *fakeAdminRepository) ListAccounts(context.Context) ([]Account, error) {
	return append([]Account(nil), repository.accounts...), nil
}

func (repository *fakeAdminRepository) CreateAccount(_ context.Context, account Account, _ string) error {
	repository.createdAccounts = append(repository.createdAccounts, account)
	return nil
}

func (repository *fakeAdminRepository) SetAccountPassword(_ context.Context, accountID, password string) error {
	repository.passwordChanges = append(repository.passwordChanges, passwordChange{accountID: accountID, password: password})
	return nil
}

func (repository *fakeAdminRepository) SetAccountStatus(_ context.Context, accountID string, status Status) error {
	repository.statusChanges = append(repository.statusChanges, statusChange{accountID: accountID, status: status})
	return nil
}

func (repository *fakeAdminRepository) ListTenants(context.Context) ([]Tenant, error) {
	return append([]Tenant(nil), repository.tenants...), nil
}

func (repository *fakeAdminRepository) CreateTenant(_ context.Context, id, name string) error {
	repository.createdTenants = append(repository.createdTenants, Tenant{ID: id, Name: name})
	return nil
}
