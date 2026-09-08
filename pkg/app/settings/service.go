package settings

import (
	"context"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	appidentity "github.com/whhhh1500/auto-agent/pkg/app/identity"
)

const maxSettingKeyRunes = 128

// Service coordinates deployment setting use cases through a narrow
// repository port. It is independent of HTTP, SQL, and storage encryption.
type Service struct{ repository Repository }

var _ UseCases = (*Service)(nil)

// NewService validates the persistence collaborator at composition time.
func NewService(repository Repository) (*Service, error) {
	if repository == nil {
		return nil, fmt.Errorf("settings service requires a settings repository")
	}
	return &Service{repository: repository}, nil
}

// Get loads one setting without returning its value. The generic settings
// endpoint is fail-closed because arbitrary setting keys may contain secrets;
// domain-specific DTOs own any bounded recognition preview. Persisted values
// are never altered by a read.
func (service *Service) Get(ctx context.Context, command GetCommand) (GetResult, error) {
	if err := requirePlatformAdmin(command.Actor); err != nil {
		return GetResult{}, err
	}
	if err := validateKey(command.Key); err != nil {
		return GetResult{}, err
	}
	_, found, err := service.repository.GetSetting(ctx, command.Key)
	if err != nil {
		return GetResult{}, err
	}
	return GetResult{Key: command.Key, Found: found, Redacted: found, WriteOnly: found}, nil
}

// Put validates and persists one raw setting value.
func (service *Service) Put(ctx context.Context, command PutCommand) error {
	if err := requirePlatformAdmin(command.Actor); err != nil {
		return err
	}
	if err := validateKey(command.Key); err != nil {
		return err
	}
	if err := service.repository.SetSetting(ctx, command.Key, command.Value); err != nil {
		return err
	}
	return nil
}

func requirePlatformAdmin(actor appidentity.AdminActor) error {
	if actor.Role != appidentity.RoleAdmin {
		return classifiedError{kind: ErrForbidden, cause: fmt.Errorf("platform admin role required")}
	}
	return nil
}

func validateKey(key string) error {
	if strings.TrimSpace(key) == "" {
		return invalidInput("setting key is empty")
	}
	if strings.ContainsRune(key, '\x00') {
		return invalidInput("setting key contains NUL")
	}
	for _, character := range key {
		if unicode.IsControl(character) {
			return invalidInput("setting key contains a control character")
		}
	}
	if utf8.RuneCountInString(key) > maxSettingKeyRunes {
		return invalidInput("setting key exceeds %d runes", maxSettingKeyRunes)
	}
	return nil
}

func invalidInput(format string, values ...any) error {
	return classifiedError{kind: ErrInvalidInput, cause: fmt.Errorf(format, values...)}
}

type classifiedError struct {
	kind  error
	cause error
}

func (err classifiedError) Error() string { return err.cause.Error() }
func (err classifiedError) Unwrap() error { return err.kind }
