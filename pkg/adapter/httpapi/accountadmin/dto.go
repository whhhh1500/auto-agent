// Package accountadmin owns the HTTP models for account and tenant
// administration. It intentionally does not depend on storage models so the
// transport contract cannot accidentally expose credential fields.
package accountadmin

import (
	"fmt"
	"strings"
	"time"

	appidentity "github.com/whhhh1500/auto-agent/pkg/app/identity"
)

// AccountView is the account representation returned by administration
// endpoints. Password hashes and other credential material are not part of the
// transport model.
type AccountView struct {
	Account            string             `json:"account"`
	Email              string             `json:"email,omitempty"`
	Role               appidentity.Role   `json:"role"`
	TenantID           string             `json:"tenant_id"`
	Status             appidentity.Status `json:"status"`
	MustChangePassword bool               `json:"must_change_password"`
	CreatedAt          time.Time          `json:"created_at"`
}

// AccountListResponse is the response for GET /v1/admin/accounts.
type AccountListResponse struct {
	Accounts []AccountView `json:"accounts"`
}

// CreateAccountRequest is the request for POST /v1/admin/accounts. Account
// may be omitted for legacy clients; the handler falls back to email.
type CreateAccountRequest struct {
	Account  string           `json:"account"`
	Email    string           `json:"email"`
	Password string           `json:"password"`
	Role     appidentity.Role `json:"role"`
	TenantID string           `json:"tenant_id"`
}

// Validate checks fields whose values are part of the public account
// contract. The existing handler retains the password length rule and account
// fallback behavior.
func (request CreateAccountRequest) Validate() error {
	if _, err := appidentity.ParseRole(string(request.Role)); err != nil {
		return err
	}
	if strings.ContainsRune(request.Account, '\x00') || strings.ContainsRune(request.Email, '\x00') {
		return fmt.Errorf("account identifier contains NUL")
	}
	return nil
}

// AccountCreatedResponse is the response for a successful account creation.
type AccountCreatedResponse struct {
	Account  string           `json:"account"`
	Email    string           `json:"email"`
	Role     appidentity.Role `json:"role"`
	TenantID string           `json:"tenant"`
}

// SetAccountPasswordRequest is the request for changing an account password.
type SetAccountPasswordRequest struct {
	Password string `json:"password"`
}

// PasswordUpdatedResponse is the response for a successful password update.
type PasswordUpdatedResponse struct {
	Status string `json:"status"`
}

// SetAccountStatusRequest is the request for changing an account lifecycle
// status. Initial activation is intentionally handled by the auth endpoint,
// not this administrative endpoint.
type SetAccountStatusRequest struct {
	Status appidentity.Status `json:"status"`
}

// Validate checks the administrative status transition input while retaining
// the existing active/disabled-only behavior.
func (request SetAccountStatusRequest) Validate() error {
	status, err := appidentity.ParseStatus(string(request.Status))
	if err != nil {
		// Keep the established HTTP error for all invalid administrative
		// transitions while still validating through the typed enum.
		return fmt.Errorf("status must be active or disabled")
	}
	if status != appidentity.StatusActive && status != appidentity.StatusDisabled {
		return fmt.Errorf("status must be active or disabled")
	}
	return nil
}

// AccountStatusResponse is the response for a successful status update.
type AccountStatusResponse struct {
	Status appidentity.Status `json:"status"`
}

// TenantView is the tenant representation returned by administration
// endpoints.
type TenantView struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

// TenantListResponse is the response for GET /v1/admin/tenants.
type TenantListResponse struct {
	Tenants []TenantView `json:"tenants"`
}

// CreateTenantRequest is the request for POST /v1/admin/tenants.
type CreateTenantRequest struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// TenantCreatedResponse is the response for a successful tenant creation.
type TenantCreatedResponse struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}
