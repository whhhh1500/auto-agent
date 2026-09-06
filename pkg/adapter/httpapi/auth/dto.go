// Package auth owns the public HTTP models for identity endpoints.
package auth

import (
	"fmt"
	"strings"
)

// LoginRequest accepts account and the legacy email identity field. Unknown
// fields are deliberately tolerated by the /v1 compatibility decoder.
type LoginRequest struct {
	Account  string `json:"account"`
	Email    string `json:"email"`
	Password string `json:"password"`
}

// AccountID validates the compatible identifier forms and returns one
// canonical application command value.
func (request LoginRequest) AccountID() (string, error) {
	accountID := strings.TrimSpace(request.Account)
	legacyEmail := strings.TrimSpace(request.Email)
	if accountID == "" {
		accountID = legacyEmail
	} else if legacyEmail != "" && legacyEmail != accountID {
		return "", fmt.Errorf("account and legacy email identifier disagree")
	}
	if accountID == "" {
		return "", fmt.Errorf("account or email is required")
	}
	if strings.ContainsRune(accountID, '\x00') {
		return "", fmt.Errorf("account identifier contains NUL")
	}
	return accountID, nil
}

// ActivationRequest contains only the documented password fields.
type ActivationRequest struct {
	Password string `json:"password"`
	Confirm  string `json:"confirm"`
}

// Validate checks the confirmation before mapping to the application command.
func (request ActivationRequest) Validate() error {
	if request.Password != request.Confirm {
		return fmt.Errorf("password confirmation does not match")
	}
	return nil
}

// LoginResponse is the stable successful login payload.
type LoginResponse struct {
	Token              string `json:"token"`
	Account            string `json:"account"`
	Email              string `json:"email,omitempty"`
	Role               string `json:"role"`
	Tenant             string `json:"tenant"`
	MustChangePassword bool   `json:"must_change_password"`
}

// ActivationResponse is the stable successful first-password-change payload.
type ActivationResponse struct {
	Token              string `json:"token"`
	Account            string `json:"account"`
	MustChangePassword bool   `json:"must_change_password"`
}
