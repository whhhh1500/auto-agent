// Package settings owns application use cases for deployment settings.
// Persistence, HTTP wire models, and storage-specific encryption remain at
// their respective adapter boundaries.
package settings

import (
	"context"
	"errors"

	appidentity "github.com/cc-auto-agent/harness-core/pkg/app/identity"
)

var (
	// ErrForbidden identifies a caller that lacks platform administration
	// authority without exposing a transport-specific response status.
	ErrForbidden = errors.New("settings administration forbidden")
	// ErrInvalidInput identifies a command that violates the settings contract.
	ErrInvalidInput = errors.New("invalid settings input")
)

// Repository is the narrow persistence port for deployment settings. Values
// are persisted raw so encryption and storage representation remain adapter
// concerns.
type Repository interface {
	GetSetting(context.Context, string) (value string, found bool, err error)
	SetSetting(context.Context, string, string) error
}

// AbsentSettingCreator is an optional Repository capability for a durable
// create-if-absent operation. Callers that require first-writer-wins semantics
// must type-assert this capability and fail closed when it is unavailable;
// Repository deliberately remains source-compatible for existing adapters.
type AbsentSettingCreator interface {
	SetSettingIfAbsent(context.Context, string, string) (created bool, err error)
}

// SettingCompareAndSwapper is an optional Repository capability for a durable
// plaintext compare-and-swap operation. Callers that need optimistic updates
// must type-assert this capability and fail closed when it is unavailable;
// Repository deliberately remains source-compatible for existing adapters.
type SettingCompareAndSwapper interface {
	CompareAndSwapSetting(context.Context, string, string, string) (swapped bool, err error)
}

// UseCases is the application surface consumed by settings transports.
type UseCases interface {
	Get(context.Context, GetCommand) (GetResult, error)
	Put(context.Context, PutCommand) error
}

// GetCommand requests one setting as a trusted identity-domain actor.
type GetCommand struct {
	Actor appidentity.AdminActor
	Key   string
}

// GetResult is one setting read. Generic reads are fail-closed, so Value stays
// empty and a found result is Redacted and WriteOnly. The field remains for a
// future domain-specific non-sensitive read surface.
type GetResult struct {
	Key       string
	Found     bool
	Value     string
	Redacted  bool
	WriteOnly bool
}

// PutCommand persists a raw setting value as a trusted identity-domain actor.
type PutCommand struct {
	Actor appidentity.AdminActor
	Key   string
	Value string
}
