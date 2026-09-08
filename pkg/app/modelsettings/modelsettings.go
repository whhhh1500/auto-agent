// Package modelsettings owns the safe application surface for the one legacy
// model configuration. It does not own HTTP, SQL, protocol execution, or
// encryption; its repository port persists an additive-compatible llm record.
package modelsettings

import (
	"context"
	"errors"

	appidentity "github.com/whhhh1500/auto-agent/pkg/app/identity"
)

// ProviderID and ProtocolID are opaque, bounded integration identifiers. The
// application validates their wire safety but intentionally does not close the
// set: third-party providers and protocols can be configured later.
type ProviderID string
type ProtocolID string

const (
	ProviderOpenAI    ProviderID = "openai"
	ProviderAnthropic ProviderID = "anthropic"

	ProtocolOpenAIChatCompletions ProtocolID = "openai-chat-completions"
	ProtocolOpenAIResponses       ProtocolID = "openai-responses"
	ProtocolAnthropicMessages     ProtocolID = "anthropic-messages"
)

var (
	ErrForbidden            = errors.New("model settings administration forbidden")
	ErrInvalidInput         = errors.New("invalid model settings input")
	ErrInvalidConfiguration = errors.New("invalid persisted model settings configuration")
)

// ConfigSource is the non-sensitive provenance of an authoritative LLM
// configuration. It is persisted with the record and may be returned to an
// administrator; it never carries endpoint credentials or a secret-derived
// fingerprint.
type ConfigSource string

const (
	// ConfigSourceBootstrap is reserved for a future explicit built-in bootstrap.
	ConfigSourceBootstrap ConfigSource = "bootstrap"
	// ConfigSourceDB identifies a configuration written by an administrator or
	// a legacy record whose additive source field was absent.
	ConfigSourceDB ConfigSource = "db"
	// ConfigSourceEnvImport identifies the one-time import of a valid legacy
	// HARNESS_LLM_* configuration into the authoritative repository.
	ConfigSourceEnvImport ConfigSource = "env_import"
	// ConfigSourceLegacyFallback is reserved for an explicit external
	// compatibility composition that cannot persist its legacy configuration.
	ConfigSourceLegacyFallback ConfigSource = "legacy_fallback"
)

// Valid reports whether source is one of the stable, non-sensitive provenance
// values. An empty source is normalized to ConfigSourceDB for old records.
func (source ConfigSource) Valid() bool {
	switch source {
	case ConfigSourceBootstrap, ConfigSourceDB, ConfigSourceEnvImport, ConfigSourceLegacyFallback:
		return true
	default:
		return false
	}
}

// StoredConfiguration is the private-at-rest configuration carried through the
// application repository port. APIKey is never present in a transport result.
type StoredConfiguration struct {
	BaseURL  string
	APIKey   string
	Model    string
	Provider ProviderID
	Protocol ProtocolID
	// MaxTokens is zero when no output-token cap was configured. It remains
	// additive so historical persisted records retain their former behavior.
	MaxTokens     int
	AllowedModels []string
	Source        ConfigSource
}

// Repository persists the legacy llm record through an encryption-capable
// adapter. A found record is authoritative even when it is inactive.
type Repository interface {
	Load(context.Context) (StoredConfiguration, bool, error)
	Save(context.Context, StoredConfiguration) error
}

// PostSaveObserver reconciles an in-memory consumer after a configuration has
// been durably saved. The observer deliberately receives no configuration:
// implementations that need state must read it again through their own narrow
// authority, so APIKey never expands to an in-memory observer boundary.
// Returning an error makes the caller observe that the runtime refresh did not
// complete; the already-persisted configuration remains authoritative.
type PostSaveObserver interface {
	AfterSave(context.Context) error
}

// UseCases is the model-settings application surface consumed by transports
// and boot composition.
type UseCases interface {
	Get(context.Context, GetCommand) (GetResult, error)
	Put(context.Context, PutCommand) error
}

type GetCommand struct{ Actor appidentity.AdminActor }

// GetResult intentionally omits APIKey while retaining only bounded state for
// an explicit model-settings UI.
type GetResult struct {
	Found           bool
	BaseURL         string
	Model           string
	Provider        ProviderID
	Protocol        ProtocolID
	MaxTokens       int
	AllowedModels   []string
	HasAPIKey       bool
	APIKeyPreview   string
	ConfigSource    ConfigSource
	Executable      bool
	ExecutionStatus string
	ExecutionReason string
}

// PutCommand distinguishes omitted APIKey and AllowedModels from supplied
// values. ClearAPIKey is the sole explicit deletion operation.
type PutCommand struct {
	Actor                appidentity.AdminActor
	BaseURL              string
	Model                string
	Provider             *ProviderID
	Protocol             *ProtocolID
	MaxTokens            int
	MaxTokensPresent     bool
	AllowedModels        []string
	AllowedModelsPresent bool
	APIKey               *string
	ClearAPIKey          bool
}
