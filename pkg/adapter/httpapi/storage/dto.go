// Package storage owns HTTP DTOs for administrative object-storage settings.
package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"
)

// OptionalString distinguishes an omitted string field from a supplied one.
// A JSON null is not an omission and is rejected so callers cannot turn it
// into an ambiguous secret operation.
type OptionalString struct {
	Present bool
	Value   string
}

// UnmarshalJSON records field presence and accepts only JSON strings.
func (field *OptionalString) UnmarshalJSON(data []byte) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return fmt.Errorf("value must be a string")
	}
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return fmt.Errorf("value must be a string: %w", err)
	}
	field.Present = true
	field.Value = value
	return nil
}

// UnmarshalJSON preserves field-specific diagnostics while keeping the
// presence-aware OptionalString reusable for both credentials.
func (request *PutRequest) UnmarshalJSON(data []byte) error {
	type alias PutRequest
	var envelope struct {
		*alias
		AccessKey json.RawMessage `json:"access_key"`
		SecretKey json.RawMessage `json:"secret_key"`
	}
	envelope.alias = (*alias)(request)
	if err := json.Unmarshal(data, &envelope); err != nil {
		return err
	}
	parse := func(name string, raw json.RawMessage) (OptionalString, error) {
		if len(raw) == 0 {
			return OptionalString{}, nil
		}
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return OptionalString{}, fmt.Errorf("%s must be a string", name)
		}
		var value string
		if err := json.Unmarshal(raw, &value); err != nil {
			return OptionalString{}, fmt.Errorf("%s must be a string: %w", name, err)
		}
		return OptionalString{Present: true, Value: value}, nil
	}
	var err error
	if request.AccessKey, err = parse("access_key", envelope.AccessKey); err != nil {
		return err
	}
	if request.SecretKey, err = parse("secret_key", envelope.SecretKey); err != nil {
		return err
	}
	return nil
}

// OptionalBool distinguishes an omitted boolean field from an explicit false
// value. JSON null is rejected so a caller cannot turn it into an ambiguous
// storage update operation.
type OptionalBool struct {
	Present bool
	Value   bool
}

// UnmarshalJSON records field presence and accepts only JSON booleans.
func (field *OptionalBool) UnmarshalJSON(data []byte) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return fmt.Errorf("disable_conditional_writes must be a boolean")
	}
	var value bool
	if err := json.Unmarshal(data, &value); err != nil {
		return fmt.Errorf("disable_conditional_writes must be a boolean: %w", err)
	}
	field.Present = true
	field.Value = value
	return nil
}

// Configuration is the storage configuration returned by GET. HasSecret and
// SecretPreview describe secret state without exposing the complete secret.
type Configuration struct {
	Type                     string `json:"type"`
	Path                     string `json:"path,omitempty"`
	Endpoint                 string `json:"endpoint,omitempty"`
	Region                   string `json:"region,omitempty"`
	Bucket                   string `json:"bucket,omitempty"`
	HasAccessKey             bool   `json:"has_access_key"`
	AccessKeyPreview         string `json:"access_key_preview,omitempty"`
	PathStyle                bool   `json:"path_style,omitempty"`
	DisableConditionalWrites *bool  `json:"disable_conditional_writes,omitempty"`
	Source                   string `json:"config_source,omitempty"`
	Status                   string `json:"config_status,omitempty"`
	DesiredRevision          string `json:"desired_revision,omitempty"`
	HasSecret                bool   `json:"has_secret"`
	SecretPreview            string `json:"secret_preview,omitempty"`
}

// GetResponse is the response for one administrative storage setting.
type GetResponse struct {
	Key             string             `json:"key"`
	Found           bool               `json:"found"`
	Config          Configuration      `json:"config"`
	Active          string             `json:"active,omitempty"`
	DesiredType     string             `json:"desired_type,omitempty"`
	ActiveType      string             `json:"active_type,omitempty"`
	DesiredRevision string             `json:"desired_revision,omitempty"`
	ActiveRevision  string             `json:"active_revision,omitempty"`
	RestartPending  bool               `json:"restart_pending,omitempty"`
	ErrorCode       string             `json:"error_code,omitempty"`
	Migration       *MigrationProgress `json:"migration,omitempty"`
}

// PutRequest updates one administrative storage setting. SecretKey has
// explicit presence semantics: omitted preserves, a non-empty value replaces,
// and ClearSecret deletes. Supplying both is invalid.
type PutRequest struct {
	Type                     string         `json:"type"`
	Path                     string         `json:"path,omitempty"`
	Endpoint                 string         `json:"endpoint,omitempty"`
	Region                   string         `json:"region,omitempty"`
	Bucket                   string         `json:"bucket,omitempty"`
	AccessKey                OptionalString `json:"access_key,omitempty"`
	SecretKey                OptionalString `json:"secret_key,omitempty"`
	ClearSecret              bool           `json:"clear_secret,omitempty"`
	PathStyle                bool           `json:"path_style,omitempty"`
	DisableConditionalWrites OptionalBool   `json:"disable_conditional_writes,omitempty"`
	Status                   string         `json:"config_status,omitempty"`
	ExpectedRevision         string         `json:"expected_revision,omitempty"`
}

// PutResponse confirms a persisted storage configuration update.
type PutResponse struct {
	Status          string             `json:"status"`
	Applied         string             `json:"applied"`
	Config          *Configuration     `json:"config,omitempty"`
	DesiredType     string             `json:"desired_type,omitempty"`
	ActiveType      string             `json:"active_type,omitempty"`
	DesiredRevision string             `json:"desired_revision,omitempty"`
	ActiveRevision  string             `json:"active_revision,omitempty"`
	RestartPending  bool               `json:"restart_pending,omitempty"`
	ErrorCode       string             `json:"error_code,omitempty"`
	Migration       *MigrationProgress `json:"migration,omitempty"`
}

type MigrationProgress struct {
	State           string    `json:"state"`
	CopiedObjects   uint64    `json:"copied_objects,omitempty"`
	CopiedBytes     uint64    `json:"copied_bytes,omitempty"`
	VerifiedObjects uint64    `json:"verified_objects,omitempty"`
	VerifiedBytes   uint64    `json:"verified_bytes,omitempty"`
	ErrorCode       string    `json:"error_code,omitempty"`
	CreatedAt       time.Time `json:"created_at,omitempty"`
	UpdatedAt       time.Time `json:"updated_at,omitempty"`
	VerifiedAt      time.Time `json:"verified_at,omitempty"`
	ActivatedAt     time.Time `json:"activated_at,omitempty"`
}

// TestRequest contains an explicit secret for a one-off connectivity check.
// It never reads or modifies a persisted storage setting.
type TestRequest struct {
	Type      string `json:"type"`
	Endpoint  string `json:"endpoint,omitempty"`
	Region    string `json:"region,omitempty"`
	Bucket    string `json:"bucket,omitempty"`
	AccessKey string `json:"access_key,omitempty"`
	SecretKey string `json:"secret_key,omitempty"`
	PathStyle bool   `json:"path_style,omitempty"`
}

// TestResponse is the non-secret result of a storage connectivity check.
type TestResponse struct {
	Status  string `json:"status"`
	Backend string `json:"backend,omitempty"`
}
