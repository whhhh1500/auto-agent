// Package identity owns the application-facing account identity model and
// authentication use cases. It deliberately contains no HTTP, SQL, or
// password-hash representation.
package identity

import (
	"fmt"
	"strings"
	"time"
)

// Role determines an account's administrative authority.
type Role string

const (
	RoleAdmin       Role = "admin"
	RoleTenantAdmin Role = "tenant_admin"
	RoleUser        Role = "user"
)

// Valid reports whether role is a supported persisted and wire value.
func (role Role) Valid() bool {
	switch role {
	case RoleAdmin, RoleTenantAdmin, RoleUser:
		return true
	default:
		return false
	}
}

// ParseRole validates a role received at an external boundary.
func ParseRole(value string) (Role, error) {
	role := Role(value)
	if !role.Valid() {
		return "", fmt.Errorf("unknown account role %q", value)
	}
	return role, nil
}

// RoleValues returns the supported role values in stable wire order.
func RoleValues() []Role { return []Role{RoleAdmin, RoleTenantAdmin, RoleUser} }

// Status is an account lifecycle state.
type Status string

const (
	StatusPendingActivation Status = "pending_activation"
	StatusActive            Status = "active"
	StatusDisabled          Status = "disabled"
)

// Valid reports whether status is a supported persisted and wire value.
func (status Status) Valid() bool {
	switch status {
	case StatusPendingActivation, StatusActive, StatusDisabled:
		return true
	default:
		return false
	}
}

// ParseStatus validates a status received at an external boundary.
func ParseStatus(value string) (Status, error) {
	status := Status(value)
	if !status.Valid() {
		return "", fmt.Errorf("unknown account status %q", value)
	}
	return status, nil
}

// StatusValues returns the supported status values in stable wire order.
func StatusValues() []Status {
	return []Status{StatusPendingActivation, StatusActive, StatusDisabled}
}

// Account is the public identity view. Password credentials are intentionally
// absent: only an identity credential adapter may access password hashes.
type Account struct {
	ID                 string
	Email              string
	Role               Role
	TenantID           string
	Status             Status
	MustChangePassword bool
	CreatedAt          time.Time
}

// LoginCommand expresses the login use case without transport tags.
type LoginCommand struct {
	AccountID string
	Password  string
}

// ActivateCommand expresses replacement of a one-time initial password.
type ActivateCommand struct {
	AccountID string
	Password  string
}

// LoginResult is the token and account view returned after a successful login.
type LoginResult struct {
	Token   string
	Account Account
}

// ActivationResult is returned only after activation's atomic transaction
// committed, including the restricted-token revocation and normal token issue.
type ActivationResult struct {
	Token   string
	Account Account
}

func validateLogin(command LoginCommand) error {
	if strings.TrimSpace(command.AccountID) == "" {
		return fmt.Errorf("account identifier is required")
	}
	if strings.ContainsRune(command.AccountID, '\x00') {
		return fmt.Errorf("account identifier contains NUL")
	}
	if strings.TrimSpace(command.Password) == "" {
		return fmt.Errorf("password is required")
	}
	if strings.ContainsRune(command.Password, '\x00') || len(command.Password) > 72 {
		return fmt.Errorf("password is invalid")
	}
	return nil
}

func validateActivation(command ActivateCommand) error {
	if err := validateLogin(LoginCommand(command)); err != nil {
		return err
	}
	if len([]rune(command.Password)) < 12 {
		return fmt.Errorf("password must be at least 12 characters")
	}
	return nil
}
