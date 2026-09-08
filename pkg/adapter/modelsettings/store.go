// Package modelsettings persists the legacy LLM record through a generic
// encrypted settings repository. It is deliberately not a SQL adapter: the
// caller chooses SQLite, PostgreSQL, or another Repository implementation.
package modelsettings

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	appmodelsettings "github.com/whhhh1500/auto-agent/pkg/app/modelsettings"
	appsettings "github.com/whhhh1500/auto-agent/pkg/app/settings"
)

const settingKey = "llm"

// Store maps the additive legacy llm JSON record to the model-settings
// application repository port.
type Store struct{ settings appsettings.Repository }

var _ appmodelsettings.Repository = (*Store)(nil)

func New(settings appsettings.Repository) (*Store, error) {
	if settings == nil {
		return nil, fmt.Errorf("model settings store requires a settings repository")
	}
	return &Store{settings: settings}, nil
}

func (store *Store) Load(ctx context.Context) (appmodelsettings.StoredConfiguration, bool, error) {
	raw, found, err := store.settings.GetSetting(ctx, settingKey)
	if err != nil || !found {
		return appmodelsettings.StoredConfiguration{}, found, err
	}
	configuration, err := decodePersistedConfiguration(raw)
	if err != nil {
		return appmodelsettings.StoredConfiguration{}, true, err
	}
	return configuration, true, nil
}

func (store *Store) Save(ctx context.Context, configuration appmodelsettings.StoredConfiguration) error {
	encoded, err := encodeStoredConfiguration(configuration)
	if err != nil {
		return err
	}
	return store.settings.SetSetting(ctx, settingKey, string(encoded))
}

// CreateIfAbsent atomically persists one normalized configuration when the
// legacy llm record is absent. It never falls back to Save: a caller that
// needs first-writer-wins semantics must receive an explicit failure if the
// wrapped settings repository lacks the optional atomic capability.
func (store *Store) CreateIfAbsent(ctx context.Context, configuration appmodelsettings.StoredConfiguration) (bool, error) {
	creator, ok := store.settings.(appsettings.AbsentSettingCreator)
	if !ok {
		return false, fmt.Errorf("model settings repository does not support atomic create-if-absent")
	}
	encoded, err := encodeStoredConfiguration(configuration)
	if err != nil {
		return false, err
	}
	return creator.SetSettingIfAbsent(ctx, settingKey, string(encoded))
}

func encodeStoredConfiguration(configuration appmodelsettings.StoredConfiguration) ([]byte, error) {
	normalized, err := appmodelsettings.NormalizeStoredConfiguration(configuration)
	if err != nil {
		return nil, err
	}
	return json.Marshal(persistedConfiguration{
		BaseURL: normalized.BaseURL, APIKey: normalized.APIKey,
		Model: normalized.Model, Provider: normalized.Provider, Protocol: normalized.Protocol, MaxTokens: normalized.MaxTokens,
		AllowedModels: normalized.AllowedModels, Source: normalized.Source,
	})
}

func decodePersistedConfiguration(raw string) (appmodelsettings.StoredConfiguration, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &fields); err != nil || fields == nil {
		return appmodelsettings.StoredConfiguration{}, invalidPersisted("llm JSON is malformed")
	}
	var persisted persistedConfiguration
	if err := json.Unmarshal([]byte(raw), &persisted); err != nil {
		return appmodelsettings.StoredConfiguration{}, invalidPersisted("llm JSON is malformed")
	}
	for _, name := range []string{"base_url", "api_key", "model", "provider", "protocol"} {
		if value, present := fields[name]; present && bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return appmodelsettings.StoredConfiguration{}, invalidPersisted(name + " must be a string when present")
		}
	}
	if allowed, present := fields["allowed_models"]; present {
		if bytes.Equal(bytes.TrimSpace(allowed), []byte("null")) {
			return appmodelsettings.StoredConfiguration{}, invalidPersisted("allowed_models must be an array when present")
		}
		var values []string
		if err := json.Unmarshal(allowed, &values); err != nil {
			return appmodelsettings.StoredConfiguration{}, invalidPersisted("allowed_models must be an array of strings when present")
		}
		persisted.AllowedModels = values
	}
	if source, present := fields["config_source"]; present {
		if bytes.Equal(bytes.TrimSpace(source), []byte("null")) {
			return appmodelsettings.StoredConfiguration{}, invalidPersisted("config_source must be a string when present")
		}
		if err := json.Unmarshal(source, &persisted.Source); err != nil {
			return appmodelsettings.StoredConfiguration{}, invalidPersisted("config_source must be a string when present")
		}
	}
	if maxTokens, present := fields["max_tokens"]; present {
		if bytes.Equal(bytes.TrimSpace(maxTokens), []byte("null")) {
			return appmodelsettings.StoredConfiguration{}, invalidPersisted("max_tokens must be an integer when present")
		}
		if err := json.Unmarshal(maxTokens, &persisted.MaxTokens); err != nil {
			return appmodelsettings.StoredConfiguration{}, invalidPersisted("max_tokens must be an integer when present")
		}
	}
	for _, field := range []struct {
		name   string
		target any
	}{{"provider", &persisted.Provider}, {"protocol", &persisted.Protocol}} {
		if value, present := fields[field.name]; present {
			if err := json.Unmarshal(value, field.target); err != nil {
				return appmodelsettings.StoredConfiguration{}, invalidPersisted(field.name + " must be a string when present")
			}
		}
	}
	normalized, err := appmodelsettings.NormalizeStoredConfiguration(appmodelsettings.StoredConfiguration{
		BaseURL: persisted.BaseURL, APIKey: persisted.APIKey, Model: persisted.Model, Provider: persisted.Provider, Protocol: persisted.Protocol, MaxTokens: persisted.MaxTokens,
		AllowedModels: persisted.AllowedModels, Source: persisted.Source,
	})
	if err != nil {
		return appmodelsettings.StoredConfiguration{}, err
	}
	return normalized, nil
}

func invalidPersisted(message string) error {
	return fmt.Errorf("%w: %s", appmodelsettings.ErrInvalidConfiguration, message)
}

type persistedConfiguration struct {
	BaseURL       string                        `json:"base_url"`
	APIKey        string                        `json:"api_key"`
	Model         string                        `json:"model"`
	Provider      appmodelsettings.ProviderID   `json:"provider,omitempty"`
	Protocol      appmodelsettings.ProtocolID   `json:"protocol,omitempty"`
	MaxTokens     int                           `json:"max_tokens,omitempty"`
	AllowedModels []string                      `json:"allowed_models,omitempty"`
	Source        appmodelsettings.ConfigSource `json:"config_source,omitempty"`
}
