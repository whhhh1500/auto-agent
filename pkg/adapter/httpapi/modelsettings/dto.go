// Package modelsettings owns HTTP DTOs for the dedicated legacy LLM settings
// route. It keeps secret write intent distinct from read-only secret state.
package modelsettings

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// OptionalString distinguishes an omitted field from a supplied string. Null
// is rejected rather than overloaded as a secret operation.
type OptionalString struct {
	Present bool
	Value   string
}

// OptionalProviderID and OptionalProtocolID preserve omission independently:
// update semantics need to distinguish retain from an explicit identifier.
type OptionalProviderID struct {
	Present bool
	Value   string
}
type OptionalProtocolID struct {
	Present bool
	Value   string
}

func (field *OptionalProviderID) UnmarshalJSON(data []byte) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return fmt.Errorf("provider must be a string")
	}
	if err := json.Unmarshal(data, &field.Value); err != nil {
		return fmt.Errorf("provider must be a string: %w", err)
	}
	field.Present = true
	return nil
}
func (field *OptionalProtocolID) UnmarshalJSON(data []byte) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return fmt.Errorf("protocol must be a string")
	}
	if err := json.Unmarshal(data, &field.Value); err != nil {
		return fmt.Errorf("protocol must be a string: %w", err)
	}
	field.Present = true
	return nil
}

func (field *OptionalString) UnmarshalJSON(data []byte) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return fmt.Errorf("api_key must be a string")
	}
	if err := json.Unmarshal(data, &field.Value); err != nil {
		return fmt.Errorf("api_key must be a string: %w", err)
	}
	field.Present = true
	return nil
}

// OptionalStrings distinguishes omission (preserve) from an explicit list
// replacement, including the empty list that clears a deployment allow-list.
type OptionalStrings struct {
	Present bool
	Values  []string
}

// OptionalBool rejects null and non-boolean values while preserving whether a
// field was provided. clear_api_key:false is explicitly equivalent to omission.
type OptionalBool struct {
	Present bool
	Value   bool
}

// OptionalInt distinguishes omission from an explicit integer replacement.
// Null, fractional values, and non-numeric values are rejected at the
// transport boundary rather than guessed as a preservation operation.
type OptionalInt struct {
	Present bool
	Value   int
}

func (field *OptionalInt) UnmarshalJSON(data []byte) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return fmt.Errorf("max_tokens must be an integer")
	}
	if err := json.Unmarshal(data, &field.Value); err != nil {
		return fmt.Errorf("max_tokens must be an integer: %w", err)
	}
	field.Present = true
	return nil
}

func (field *OptionalBool) UnmarshalJSON(data []byte) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return fmt.Errorf("clear_api_key must be a boolean")
	}
	if err := json.Unmarshal(data, &field.Value); err != nil {
		return fmt.Errorf("clear_api_key must be a boolean: %w", err)
	}
	field.Present = true
	return nil
}

func (field *OptionalStrings) UnmarshalJSON(data []byte) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return fmt.Errorf("allowed_models must be an array of strings")
	}
	var values []string
	if err := json.Unmarshal(data, &values); err != nil {
		return fmt.Errorf("allowed_models must be an array of strings: %w", err)
	}
	field.Present = true
	field.Values = values
	return nil
}

// PutRequest is the canonical dedicated LLM settings write payload.
type PutRequest struct {
	BaseURL       string             `json:"base_url"`
	Model         string             `json:"model"`
	Provider      OptionalProviderID `json:"provider,omitempty"`
	Protocol      OptionalProtocolID `json:"protocol,omitempty"`
	MaxTokens     OptionalInt        `json:"max_tokens,omitempty"`
	AllowedModels OptionalStrings    `json:"allowed_models,omitempty"`
	APIKey        OptionalString     `json:"api_key,omitempty"`
	ClearAPIKey   OptionalBool       `json:"clear_api_key,omitempty"`
}

// GetResponse is the safe read representation. APIKey is deliberately absent.
type GetResponse struct {
	Found           bool     `json:"found"`
	BaseURL         string   `json:"base_url"`
	Model           string   `json:"model"`
	Provider        string   `json:"provider"`
	Protocol        string   `json:"protocol"`
	MaxTokens       int      `json:"max_tokens"`
	AllowedModels   []string `json:"allowed_models"`
	HasAPIKey       bool     `json:"has_api_key"`
	APIKeyPreview   string   `json:"api_key_preview,omitempty"`
	ConfigSource    string   `json:"config_source,omitempty"`
	Executable      bool     `json:"executable"`
	ExecutionStatus string   `json:"execution_status,omitempty"`
	ExecutionReason string   `json:"execution_reason,omitempty"`
}

type PutResponse struct {
	Status string `json:"status"`
}

// LegacyPutRequest maps the historical generic settings value object. It is
// intentionally tolerant of additive nested fields. Only this mapper treats
// an explicitly empty nested api_key as an omitted preserve operation.
func LegacyPutRequest(value json.RawMessage) (PutRequest, error) {
	var legacy PutRequest
	if err := json.Unmarshal(value, &legacy); err != nil {
		return PutRequest{}, fmt.Errorf("legacy llm value must be an object: %w", err)
	}
	if legacy.APIKey.Present && legacy.APIKey.Value == "" {
		legacy.APIKey = OptionalString{}
	}
	return legacy, nil
}
