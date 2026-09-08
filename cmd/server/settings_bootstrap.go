package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	sqlsettings "github.com/whhhh1500/auto-agent/pkg/adapter/sql/settings"
	appmodelsettings "github.com/whhhh1500/auto-agent/pkg/app/modelsettings"
	appsettings "github.com/whhhh1500/auto-agent/pkg/app/settings"
	"github.com/whhhh1500/auto-agent/pkg/storage"
)

type persistedStorageConfig struct {
	Type      string `json:"type"`
	Endpoint  string `json:"endpoint"`
	Region    string `json:"region"`
	Bucket    string `json:"bucket"`
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
	PathStyle bool   `json:"path_style"`
}

func masterKeyForMode(mode deploymentMode, configured string) (key string, embedded bool, err error) {
	key = strings.TrimSpace(configured)
	if key != "" {
		return key, false, nil
	}
	if mode == deploymentModeProduction {
		return "", false, fmt.Errorf("HARNESS_MASTER_KEY is required in production mode")
	}
	return "", true, nil
}

// initializeSettingsRepository completes key selection and Cipher creation
// before any startup configuration is read. Main calls it only after the
// initial-admin decision and PostgreSQL startup-lock release.
func initializeSettingsRepository(ctx context.Context, mode deploymentMode, db *sql.DB, dialect storage.SQLDialect, accounts *storage.SQLAccountStore, configuredMasterKey string) (*sqlsettings.Store, bool, error) {
	if accounts == nil {
		return nil, false, fmt.Errorf("settings initialization requires an SQL account store")
	}
	masterKey, embedded, err := masterKeyForMode(mode, configuredMasterKey)
	if err != nil {
		return nil, false, err
	}
	if embedded {
		masterKey, err = storage.EnsureMasterKey(ctx, db, dialect)
		if err != nil {
			return nil, false, err
		}
	}
	cipher, err := storage.NewCipher(masterKey)
	if err != nil {
		return nil, false, err
	}
	repository, err := sqlsettings.New(sqlsettings.Options{
		DB: db, Dialect: dialect, Cipher: cipher,
		MaxEntries: storage.MaxSettings, MaxValueBytes: storage.MaxSettingValueBytes,
	})
	if err != nil {
		return nil, false, err
	}
	// AccountStore retains its Cipher field for source compatibility. Assign it
	// from the same instance used by the explicit application settings port.
	accounts.Cipher = cipher
	return repository, embedded, nil
}

// loadPersistedStorageConfig preserves legacy tolerance for an absent,
// malformed, or no-longer-valid storage setting. Repository failures are not
// configuration values: callers must fail startup rather than silently split
// durable storage between configured and embedded backends.
func loadPersistedStorageConfig(ctx context.Context, repository appsettings.Repository, key string) (*persistedStorageConfig, error) {
	if repository == nil {
		return nil, fmt.Errorf("load persisted storage setting %q: settings repository is not configured", key)
	}
	raw, found, err := repository.GetSetting(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("load persisted storage setting %q: %w", key, err)
	}
	if !found {
		return nil, nil
	}
	var config persistedStorageConfig
	if json.Unmarshal([]byte(raw), &config) != nil || config.Type != "s3" || config.Bucket == "" {
		return nil, nil
	}
	return &config, nil
}

// loadPersistedLLMConfig makes a present database record authoritative. Only
// a wholly absent record permits the legacy environment fallback; malformed,
// incomplete, or explicitly cleared records fail closed so an old env key
// cannot silently revive a disabled database configuration.
func loadPersistedLLMConfig(ctx context.Context, repository appmodelsettings.Repository) (appmodelsettings.StoredConfiguration, bool, error) {
	if repository == nil {
		return appmodelsettings.StoredConfiguration{}, false, fmt.Errorf("load persisted LLM settings: model settings repository is not configured")
	}
	config, found, err := repository.Load(ctx)
	if err != nil {
		return config, found, fmt.Errorf("load persisted LLM settings: %w", err)
	}
	if !found {
		return appmodelsettings.StoredConfiguration{}, false, nil
	}
	config, err = appmodelsettings.NormalizeStoredConfiguration(config)
	if err != nil {
		return config, true, fmt.Errorf("load persisted LLM settings: %w", err)
	}
	if strings.TrimSpace(config.APIKey) == "" {
		return config, true, fmt.Errorf("persisted LLM config is inactive: api_key is empty")
	}
	if strings.TrimSpace(config.Model) == "" {
		return config, true, fmt.Errorf("persisted LLM config is invalid: model is empty")
	}
	return config, true, nil
}
