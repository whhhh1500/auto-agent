package identity

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var (
	ErrInvalidInput = errors.New("invalid identity input")
	// ErrForbidden identifies an administration policy denial without exposing
	// a transport-specific status code to the application layer.
	ErrForbidden = errors.New("identity administration forbidden")
	// ErrInvalidCredentials intentionally combines unknown account, wrong
	// password, and disabled account to avoid account enumeration.
	ErrInvalidCredentials = errors.New("invalid account or password")
	ErrThrottled          = errors.New("too many failed attempts; try again later")
	ErrActivationPending  = errors.New("account activation is not pending")
)

// CredentialPort is implemented by the identity credential adapter. The
// port's implementation owns password hashes and token storage.
type CredentialPort interface {
	Authenticate(context.Context, LoginCommand) (Account, error)
	IssueToken(context.Context, string, time.Duration) (string, error)
	ActivateInitial(context.Context, ActivateCommand, time.Duration) (ActivationResult, error)
}

// LoginThrottle is a fail-closed policy seam. A backend failure from any
// method refuses login rather than allowing an unthrottled attempt.
type LoginThrottle interface {
	Check(context.Context, string) error
	RecordFailure(context.Context, string) error
	RecordSuccess(context.Context, string) error
}

// UseCases is the complete identity surface consumed by an HTTP transport.
// It lets adapters depend on application behavior rather than Service's
// concrete implementation or its credential and throttle collaborators.
type UseCases interface {
	Login(context.Context, LoginCommand) (LoginResult, error)
	Activate(context.Context, ActivateCommand) (ActivationResult, error)
}

// Service coordinates identity use cases. It has no database or HTTP
// dependency, so deployment policy and credential implementations are
// replaceable through narrow Ports.
type Service struct {
	credentials CredentialPort
	throttle    LoginThrottle
	tokenTTL    time.Duration
}

var _ UseCases = (*Service)(nil)

// NewService validates all required collaborators at composition time.
func NewService(credentials CredentialPort, throttle LoginThrottle, tokenTTL time.Duration) (*Service, error) {
	if credentials == nil {
		return nil, fmt.Errorf("identity service requires a credential port")
	}
	if throttle == nil {
		return nil, fmt.Errorf("identity service requires a login throttle")
	}
	if tokenTTL <= 0 {
		return nil, fmt.Errorf("identity service requires a positive token ttl")
	}
	return &Service{credentials: credentials, throttle: throttle, tokenTTL: tokenTTL}, nil
}

// Login verifies credentials, applies fail-closed throttling, and issues a
// token only for an active or pending-activation account.
func (service *Service) Login(ctx context.Context, command LoginCommand) (LoginResult, error) {
	if err := validateLogin(command); err != nil {
		return LoginResult{}, fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}
	if err := service.throttle.Check(ctx, command.AccountID); err != nil {
		return LoginResult{}, fmt.Errorf("check login throttle: %w", err)
	}
	account, err := service.credentials.Authenticate(ctx, command)
	if err != nil {
		if recordErr := service.throttle.RecordFailure(ctx, command.AccountID); recordErr != nil {
			return LoginResult{}, fmt.Errorf("record failed login: %w", recordErr)
		}
		if errors.Is(err, ErrInvalidCredentials) {
			return LoginResult{}, ErrInvalidCredentials
		}
		return LoginResult{}, err
	}
	if account.Status != StatusActive && account.Status != StatusPendingActivation {
		if err := service.throttle.RecordFailure(ctx, command.AccountID); err != nil {
			return LoginResult{}, fmt.Errorf("record failed login: %w", err)
		}
		return LoginResult{}, ErrInvalidCredentials
	}
	if err := service.throttle.RecordSuccess(ctx, command.AccountID); err != nil {
		return LoginResult{}, fmt.Errorf("record successful login: %w", err)
	}
	token, err := service.credentials.IssueToken(ctx, account.ID, service.tokenTTL)
	if err != nil {
		return LoginResult{}, err
	}
	return LoginResult{Token: token, Account: account}, nil
}

// Activate replaces the initial password and delegates the required atomic
// password/status/token mutation to the credential adapter Unit of Work.
func (service *Service) Activate(ctx context.Context, command ActivateCommand) (ActivationResult, error) {
	if err := validateActivation(command); err != nil {
		return ActivationResult{}, fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}
	result, err := service.credentials.ActivateInitial(ctx, command, service.tokenTTL)
	if err != nil {
		return ActivationResult{}, err
	}
	if result.Account.Status != StatusActive || result.Account.MustChangePassword {
		return ActivationResult{}, fmt.Errorf("activation adapter returned an invalid account state")
	}
	return result, nil
}
