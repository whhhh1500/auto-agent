package identity

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// AdminActor is the trusted caller identity used by account administration.
// It deliberately carries only identity-domain fields: HTTP principals and
// storage records are mapped at the transport and composition boundaries.
type AdminActor struct {
	AccountID string
	TenantID  string
	Role      Role
}

// Tenant is the application-facing tenant view. It has no transport tags and
// no persistence-specific representation.
type Tenant struct {
	ID        string
	Name      string
	CreatedAt time.Time
}

// AdminUseCases is intentionally separate from UseCases. Login and
// activation implementations should not have to implement administration.
type AdminUseCases interface {
	ListAccounts(context.Context, ListAccountsCommand) (ListAccountsResult, error)
	CreateAccount(context.Context, CreateAccountCommand) (CreateAccountResult, error)
	SetAccountPassword(context.Context, SetAccountPasswordCommand) error
	SetAccountStatus(context.Context, SetAccountStatusCommand) (SetAccountStatusResult, error)
	ListTenants(context.Context, ListTenantsCommand) (ListTenantsResult, error)
	CreateTenant(context.Context, CreateTenantCommand) (CreateTenantResult, error)
}

// AdminRepository is the narrow persistence port for account and tenant
// administration. Credential hashes, SQL rows, and HTTP request types stay
// outside the application layer.
type AdminRepository interface {
	ListAccounts(context.Context) ([]Account, error)
	CreateAccount(context.Context, Account, string) error
	SetAccountPassword(context.Context, string, string) error
	SetAccountStatus(context.Context, string, Status) error
	ListTenants(context.Context) ([]Tenant, error)
	CreateTenant(context.Context, string, string) error
}

type ListAccountsCommand struct{ Actor AdminActor }

type ListAccountsResult struct{ Accounts []Account }

type CreateAccountCommand struct {
	Actor     AdminActor
	AccountID string
	Email     string
	Password  string
	Role      Role
	TenantID  string
}

type CreateAccountResult struct{ Account Account }

type SetAccountPasswordCommand struct {
	Actor     AdminActor
	AccountID string
	Password  string
}

type SetAccountStatusCommand struct {
	Actor     AdminActor
	AccountID string
	Status    Status
}

type SetAccountStatusResult struct {
	AccountID string
	Status    Status
}

type ListTenantsCommand struct{ Actor AdminActor }

type ListTenantsResult struct{ Tenants []Tenant }

type CreateTenantCommand struct {
	Actor AdminActor
	ID    string
	Name  string
}

type CreateTenantResult struct{ Tenant Tenant }

const (
	minAdministrativePasswordBytes = 8
	maxAdministrativePasswordBytes = 72
)

func requirePlatformAdmin(actor AdminActor) error {
	if actor.Role != RoleAdmin {
		return classifiedAdminError{kind: ErrForbidden, cause: fmt.Errorf("platform admin role required")}
	}
	return nil
}

func requireTenantOperator(actor AdminActor) error {
	if actor.Role != RoleAdmin && actor.Role != RoleTenantAdmin {
		return classifiedAdminError{kind: ErrForbidden, cause: fmt.Errorf("tenant administrator role required")}
	}
	return nil
}

func invalidAdminInput(format string, values ...any) error {
	return classifiedAdminError{kind: ErrInvalidInput, cause: fmt.Errorf(format, values...)}
}

// classifiedAdminError retains the established response message while letting
// transports map domain failures by errors.Is rather than string matching.
type classifiedAdminError struct {
	kind  error
	cause error
}

func (err classifiedAdminError) Error() string { return err.cause.Error() }
func (err classifiedAdminError) Unwrap() error { return err.kind }

func validateAdminAccountID(accountID string) error {
	if strings.TrimSpace(accountID) == "" {
		return invalidAdminInput("account id is empty")
	}
	if strings.ContainsRune(accountID, '\x00') {
		return invalidAdminInput("account identifier contains NUL")
	}
	return nil
}

func validateAdministrativePassword(password string) error {
	if len(password) < minAdministrativePasswordBytes {
		return invalidAdminInput("password must be at least 8 characters")
	}
	if strings.ContainsRune(password, '\x00') {
		return invalidAdminInput("account password contains NUL")
	}
	if len(password) > maxAdministrativePasswordBytes {
		return invalidAdminInput("account password exceeds 72 bytes")
	}
	return nil
}

func validateTenantID(id string) error {
	if strings.TrimSpace(id) == "" {
		return invalidAdminInput("tenant id is empty")
	}
	if strings.ContainsRune(id, '\x00') {
		return invalidAdminInput("tenant id contains NUL")
	}
	return nil
}

func validateTenantInput(id, name string) error {
	if err := validateTenantID(id); err != nil {
		return err
	}
	if strings.TrimSpace(name) == "" {
		return invalidAdminInput("tenant name is empty")
	}
	if strings.ContainsRune(name, '\x00') {
		return invalidAdminInput("tenant name contains NUL")
	}
	return nil
}
