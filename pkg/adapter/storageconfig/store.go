// Package storageconfig adapts typed storage configuration to the generic,
// encrypted application settings repository.
package storageconfig

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	appsettings "github.com/cc-auto-agent/harness-core/pkg/app/settings"
	appstorageconfig "github.com/cc-auto-agent/harness-core/pkg/app/storageconfig"
)

// Store maps the two historical storage settings keys to the typed application
// contract. Resolution evidence is additive and lives in a separate key so the
// legacy desired configuration JSON remains compatible.
type Store struct {
	settings appsettings.Repository
}

var _ appstorageconfig.Repository = (*Store)(nil)

func New(settings appsettings.Repository) (*Store, error) {
	if settings == nil {
		return nil, fmt.Errorf("storage configuration store requires a settings repository")
	}
	return &Store{settings: settings}, nil
}

func (store *Store) Load(ctx context.Context, kind appstorageconfig.Kind) (appstorageconfig.StoredConfiguration, bool, error) {
	key, err := settingKey(kind)
	if err != nil {
		return appstorageconfig.StoredConfiguration{}, false, err
	}
	raw, found, err := store.settings.GetSetting(ctx, key)
	if err != nil || !found {
		return appstorageconfig.StoredConfiguration{}, found, err
	}
	configuration, err := decodeConfiguration(raw)
	if err != nil {
		return configuration, true, err
	}
	configuration, err = appstorageconfig.Normalize(kind, configuration)
	if err != nil {
		return configuration, true, err
	}
	return configuration, true, nil
}

// CreateIfAbsent imports a legacy candidate without allowing concurrent
// processes to overwrite the database winner. Generic settings repositories
// that cannot provide this atomic primitive are rejected fail-closed.
func (store *Store) CreateIfAbsent(ctx context.Context, kind appstorageconfig.Kind, configuration appstorageconfig.StoredConfiguration) (bool, error) {
	key, err := settingKey(kind)
	if err != nil {
		return false, err
	}
	configuration, err = appstorageconfig.PrepareCreate(kind, configuration)
	if err != nil {
		return false, err
	}
	encoded, err := encodeConfiguration(configuration)
	if err != nil {
		return false, err
	}
	atomic, ok := store.settings.(appsettings.AbsentSettingCreator)
	if !ok {
		return false, appstorageconfig.ErrAtomicCreateUnsupported
	}
	return atomic.SetSettingIfAbsent(ctx, key, string(encoded))
}

func (store *Store) UpdateIfRevision(ctx context.Context, kind appstorageconfig.Kind, expected string, configuration appstorageconfig.StoredConfiguration) (bool, error) {
	cas, ok := store.settings.(appsettings.SettingCompareAndSwapper)
	if !ok {
		return false, appstorageconfig.ErrCASUnsupported
	}
	key, err := settingKey(kind)
	if err != nil {
		return false, err
	}
	raw, found, err := store.settings.GetSetting(ctx, key)
	if err != nil {
		return false, err
	}
	if found {
		current, decodeErr := decodeConfiguration(raw)
		if decodeErr != nil {
			return false, decodeErr
		}
		current, decodeErr = appstorageconfig.Normalize(kind, current)
		if decodeErr != nil {
			return false, decodeErr
		}
		if current.DesiredRevision != expected {
			return false, nil
		}
	} else if expected != "" {
		return false, nil
	}
	normalized, err := appstorageconfig.Normalize(kind, configuration)
	if err != nil {
		return false, err
	}
	encoded, err := encodeConfiguration(normalized)
	if err != nil {
		return false, err
	}
	if !found {
		creator, ok := store.settings.(appsettings.AbsentSettingCreator)
		if !ok {
			return false, appstorageconfig.ErrAtomicCreateUnsupported
		}
		return creator.SetSettingIfAbsent(ctx, key, string(encoded))
	}
	return cas.CompareAndSwapSetting(ctx, key, raw, string(encoded))
}

func (store *Store) LoadEvidence(ctx context.Context, kind appstorageconfig.Kind) (appstorageconfig.ResolutionEvidence, bool, error) {
	key, err := evidenceKey(kind)
	if err != nil {
		return appstorageconfig.ResolutionEvidence{}, false, err
	}
	raw, found, err := store.settings.GetSetting(ctx, key)
	if err != nil || !found {
		return appstorageconfig.ResolutionEvidence{}, found, err
	}
	var evidence appstorageconfig.ResolutionEvidence
	if err := json.Unmarshal([]byte(raw), &evidence); err != nil {
		return appstorageconfig.ResolutionEvidence{}, true, fmt.Errorf("%w: resolution evidence is malformed", appstorageconfig.ErrInvalidConfiguration)
	}
	if evidence.Kind != kind {
		return evidence, true, fmt.Errorf("%w: resolution evidence kind is invalid", appstorageconfig.ErrInvalidConfiguration)
	}
	if err := evidence.Validate(); err != nil {
		return evidence, true, err
	}
	return evidence, true, nil
}

func (store *Store) SaveEvidence(ctx context.Context, kind appstorageconfig.Kind, evidence appstorageconfig.ResolutionEvidence) error {
	key, err := evidenceKey(kind)
	if err != nil {
		return err
	}
	if evidence.Kind != kind {
		return fmt.Errorf("%w: resolution evidence kind is invalid", appstorageconfig.ErrInvalidConfiguration)
	}
	if err := evidence.Validate(); err != nil {
		return err
	}
	encoded, err := json.Marshal(evidence)
	if err != nil {
		return err
	}
	return store.settings.SetSetting(ctx, key, string(encoded))
}

func (store *Store) SaveEvidenceIfDesiredRevision(ctx context.Context, kind appstorageconfig.Kind, evidence appstorageconfig.ResolutionEvidence) (bool, error) {
	cas, ok := store.settings.(appsettings.SettingCompareAndSwapper)
	if !ok {
		return false, appstorageconfig.ErrCASUnsupported
	}
	if evidence.Kind != kind {
		return false, fmt.Errorf("%w: resolution evidence kind is invalid", appstorageconfig.ErrInvalidConfiguration)
	}
	if err := evidence.Validate(); err != nil {
		return false, err
	}
	key, err := settingKey(kind)
	if err != nil {
		return false, err
	}
	configurationRaw, found, err := store.settings.GetSetting(ctx, key)
	if err != nil || !found {
		return false, err
	}
	configuration, err := decodeConfiguration(configurationRaw)
	if err != nil {
		return false, err
	}
	configuration, err = appstorageconfig.Normalize(kind, configuration)
	if err != nil {
		return false, err
	}
	if configuration.DesiredRevision != evidence.DesiredRevision {
		return false, nil
	}
	evidenceKeyValue, err := evidenceKey(kind)
	if err != nil {
		return false, err
	}
	previousEvidence, _, err := store.settings.GetSetting(ctx, evidenceKeyValue)
	if err != nil {
		return false, err
	}
	encoded, err := json.Marshal(evidence)
	if err != nil {
		return false, err
	}
	return cas.CompareAndSwapSetting(ctx, evidenceKeyValue, previousEvidence, string(encoded))
}

const legacySecretMask = "••••"

type persistedConfiguration struct {
	Type                     appstorageconfig.Backend      `json:"type"`
	Path                     string                        `json:"path,omitempty"`
	Endpoint                 string                        `json:"endpoint,omitempty"`
	Region                   string                        `json:"region,omitempty"`
	Bucket                   string                        `json:"bucket,omitempty"`
	AccessKey                string                        `json:"access_key,omitempty"`
	SecretKey                string                        `json:"secret_key,omitempty"`
	PathStyle                bool                          `json:"path_style,omitempty"`
	DisableConditionalWrites bool                          `json:"disable_conditional_writes,omitempty"`
	Source                   appstorageconfig.ConfigSource `json:"config_source,omitempty"`
	Status                   appstorageconfig.ConfigStatus `json:"config_status,omitempty"`
	DesiredRevision          string                        `json:"desired_revision,omitempty"`
}

func encodeConfiguration(configuration appstorageconfig.StoredConfiguration) ([]byte, error) {
	return json.Marshal(persistedConfiguration{
		Type: configuration.Backend, Path: configuration.Path,
		Endpoint: configuration.Endpoint, Region: configuration.Region,
		Bucket: configuration.Bucket, AccessKey: configuration.AccessKey,
		SecretKey: configuration.SecretKey, PathStyle: configuration.PathStyle,
		DisableConditionalWrites: configuration.DisableConditionalWrites,
		Source:                   configuration.Source, Status: configuration.Status,
		DesiredRevision: configuration.DesiredRevision,
	})
}

func decodeConfiguration(raw string) (appstorageconfig.StoredConfiguration, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &fields); err != nil || fields == nil {
		return appstorageconfig.StoredConfiguration{}, fmt.Errorf("%w: storage JSON is malformed", appstorageconfig.ErrInvalidConfiguration)
	}
	var persisted persistedConfiguration
	if err := json.Unmarshal([]byte(raw), &persisted); err != nil {
		return appstorageconfig.StoredConfiguration{}, fmt.Errorf("%w: storage JSON is malformed", appstorageconfig.ErrInvalidConfiguration)
	}
	for _, name := range []string{"type", "path", "endpoint", "region", "bucket", "access_key", "secret_key", "config_source", "config_status", "desired_revision"} {
		if value, present := fields[name]; present && bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return appstorageconfig.StoredConfiguration{}, fmt.Errorf("%w: %s must be a string when present", appstorageconfig.ErrInvalidConfiguration, name)
		}
	}
	if pathStyle, present := fields["path_style"]; present && bytes.Equal(bytes.TrimSpace(pathStyle), []byte("null")) {
		return appstorageconfig.StoredConfiguration{}, fmt.Errorf("%w: path_style must be a boolean when present", appstorageconfig.ErrInvalidConfiguration)
	}
	if conditionalWrites, present := fields["disable_conditional_writes"]; present && bytes.Equal(bytes.TrimSpace(conditionalWrites), []byte("null")) {
		return appstorageconfig.StoredConfiguration{}, fmt.Errorf("%w: disable_conditional_writes must be a boolean when present", appstorageconfig.ErrInvalidConfiguration)
	}
	if persisted.SecretKey == legacySecretMask {
		// The historical UI mask is a preserve marker, never a credential.
		persisted.SecretKey = ""
	}
	configuration := appstorageconfig.StoredConfiguration{
		Backend: persisted.Type, Path: persisted.Path,
		Endpoint: persisted.Endpoint, Region: persisted.Region,
		Bucket: persisted.Bucket, AccessKey: persisted.AccessKey,
		SecretKey: persisted.SecretKey, PathStyle: persisted.PathStyle,
		DisableConditionalWrites: persisted.DisableConditionalWrites,
		Source:                   persisted.Source, Status: persisted.Status,
		DesiredRevision: persisted.DesiredRevision,
	}
	if configuration.Source != "" && !configuration.Source.Valid() {
		return configuration, fmt.Errorf("%w: unknown config_source %q", appstorageconfig.ErrInvalidConfiguration, configuration.Source)
	}
	if configuration.Status != "" && !configuration.Status.Valid() {
		return configuration, fmt.Errorf("%w: unknown config_status %q", appstorageconfig.ErrInvalidConfiguration, configuration.Status)
	}
	return configuration, nil
}

func settingKey(kind appstorageconfig.Kind) (string, error) {
	switch kind {
	case appstorageconfig.KindResources:
		return "storage.resources", nil
	case appstorageconfig.KindSessions:
		return "storage.sessions", nil
	default:
		return "", fmt.Errorf("%w: %q", appstorageconfig.ErrInvalidKind, kind)
	}
}

func evidenceKey(kind appstorageconfig.Kind) (string, error) {
	key, err := settingKey(kind)
	if err != nil {
		return "", err
	}
	return key + ".resolution", nil
}
