package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	appmodelsettings "github.com/cc-auto-agent/harness-core/pkg/app/modelsettings"
	appsettings "github.com/cc-auto-agent/harness-core/pkg/app/settings"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
)

func TestLoadPersistedStorageConfigCompatibilityAndFailures(t *testing.T) {
	ctx := context.Background()
	getErr := errors.New("decrypt setting")
	for _, test := range []struct {
		name     string
		repo     appsettings.Repository
		wantNil  bool
		wantErr  string
		wantHost string
	}{
		{name: "repository error", repo: settingsRepositoryStub{err: getErr}, wantErr: "load persisted storage setting \"storage.sessions\": decrypt setting"},
		{name: "not found", repo: settingsRepositoryStub{}, wantNil: true},
		{name: "malformed", repo: settingsRepositoryStub{found: true, value: "{"}, wantNil: true},
		{name: "legacy non s3", repo: settingsRepositoryStub{found: true, value: `{"type":"embedded","bucket":"ignored"}`}, wantNil: true},
		{name: "missing bucket", repo: settingsRepositoryStub{found: true, value: `{"type":"s3"}`}, wantNil: true},
		{name: "valid", repo: settingsRepositoryStub{found: true, value: `{"type":"s3","endpoint":"https://s3.test","bucket":"bucket","access_key":"access","secret_key":"secret","path_style":true}`}, wantHost: "https://s3.test"},
	} {
		t.Run(test.name, func(t *testing.T) {
			config, err := loadPersistedStorageConfig(ctx, test.repo, "storage.sessions")
			if test.wantErr != "" {
				if err == nil || err.Error() != test.wantErr {
					t.Fatalf("error=%v; want %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if test.wantNil {
				if config != nil {
					t.Fatalf("config=%#v; want nil", config)
				}
				return
			}
			if config == nil || config.Endpoint != test.wantHost || config.Bucket != "bucket" || !config.PathStyle {
				t.Fatalf("config=%#v", config)
			}
		})
	}
}

func TestLoadPersistedLLMConfigFailsClosedForEveryPresentInvalidRecord(t *testing.T) {
	want := errors.New("open sealed setting")
	for name, test := range map[string]struct {
		repository settingsRepositoryStub
		found      bool
		wantError  string
	}{
		"repository failure": {repository: settingsRepositoryStub{err: want}, wantError: "load persisted LLM settings: open sealed setting"},
		"absent":             {repository: settingsRepositoryStub{}, found: false},
		"malformed":          {repository: settingsRepositoryStub{found: true, value: "{"}, found: true, wantError: "load persisted LLM settings:"},
		"inactive":           {repository: settingsRepositoryStub{found: true, value: `{"api_key":"","model":"model"}`}, found: true, wantError: "persisted LLM config is inactive"},
		"missing model":      {repository: settingsRepositoryStub{found: true, value: `{"api_key":"key","model":""}`}, found: true, wantError: "load persisted LLM settings: model is empty"},
		"valid":              {repository: settingsRepositoryStub{found: true, value: `{"api_key":"key","model":"model"}`}, found: true},
	} {
		t.Run(name, func(t *testing.T) {
			config, found, err := loadPersistedLLMConfig(context.Background(), test.repository)
			if found != test.found {
				t.Fatalf("found=%t; want %t", found, test.found)
			}
			if test.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantError) {
					t.Fatalf("error=%v; want contains %q", err, test.wantError)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if test.found && (config.APIKey != "key" || config.Model != "model") {
				t.Fatalf("LLM config=%#v", config)
			}
		})
	}
}

func TestInitializeSettingsRepositoryDecryptsStartupConfiguration(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := storage.OpenSQLSessionStore(ctx, db, storage.SQLDialectSQLite); err != nil {
		t.Fatal(err)
	}
	accounts, err := storage.NewSQLAccountStore(db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	const masterKey = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	writer, embedded, err := initializeSettingsRepository(ctx, deploymentModeDev, db, storage.SQLDialectSQLite, accounts, masterKey)
	if err != nil || embedded || accounts.Cipher == nil {
		t.Fatalf("initialize writer=%T embedded=%t cipher=%T err=%v", writer, embedded, accounts.Cipher, err)
	}
	stored := `{"type":"s3","bucket":"startup-bucket","secret_key":"startup-secret"}`
	if err := writer.SetSetting(ctx, "storage.sessions", stored); err != nil {
		t.Fatal(err)
	}
	var raw string
	if err := db.QueryRowContext(ctx, "SELECT value FROM settings WHERE key = ?", "storage.sessions").Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(raw, "startup-secret") {
		t.Fatalf("startup setting was stored in plaintext: %q", raw)
	}

	// Simulate the next process lifetime: Cipher starts unset, then startup
	// initialization must finish before persisted storage configuration can be
	// consumed.
	accounts.Cipher = nil
	reader, embedded, err := initializeSettingsRepository(ctx, deploymentModeDev, db, storage.SQLDialectSQLite, accounts, masterKey)
	if err != nil || embedded || accounts.Cipher == nil {
		t.Fatalf("initialize reader=%T embedded=%t cipher=%T err=%v", reader, embedded, accounts.Cipher, err)
	}
	config, err := loadPersistedStorageConfig(ctx, reader, "storage.sessions")
	if err != nil || config == nil || config.Bucket != "startup-bucket" || config.SecretKey != "startup-secret" {
		t.Fatalf("startup configuration config=%#v err=%v", config, err)
	}
}

func TestMainInitializesSettingsAfterBootstrapAndBeforeFirstPersistedRead(t *testing.T) {
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate test source")
	}
	mainSource, err := os.ReadFile(filepath.Join(filepath.Dir(source), "main.go"))
	if err != nil {
		t.Fatal(err)
	}
	contents := string(mainSource)
	bootstrap := strings.Index(contents, "storage.BootstrapInitialAdmin(")
	initialize := strings.Index(contents, "initializeSettingsRepository(")
	configure := strings.Index(contents, "configureStorage(")
	release := -1
	if initialize >= 0 {
		release = strings.LastIndex(contents[:initialize], "postgresStartupLock.Release(")
	}
	bootstrapSource, err := os.ReadFile(filepath.Join(filepath.Dir(source), "storage_bootstrap.go"))
	if err != nil {
		t.Fatal(err)
	}
	firstRead := strings.Index(string(bootstrapSource), "ResolveWithLegacyProvider(")
	if bootstrap < 0 || release < 0 || initialize < 0 || configure < 0 || firstRead < 0 || !(bootstrap < release && release < initialize && initialize < configure) {
		t.Fatalf("startup order bootstrap=%d release=%d initialize=%d configure=%d firstRead=%d", bootstrap, release, initialize, configure, firstRead)
	}
}

type settingsRepositoryStub struct {
	value string
	found bool
	err   error
}

func (stub settingsRepositoryStub) GetSetting(context.Context, string) (string, bool, error) {
	return stub.value, stub.found, stub.err
}

func (settingsRepositoryStub) SetSetting(context.Context, string, string) error { return nil }

func (stub settingsRepositoryStub) Load(context.Context) (appmodelsettings.StoredConfiguration, bool, error) {
	if stub.err != nil || !stub.found {
		return appmodelsettings.StoredConfiguration{}, stub.found, stub.err
	}
	var configuration struct {
		BaseURL       string   `json:"base_url"`
		APIKey        string   `json:"api_key"`
		Model         string   `json:"model"`
		AllowedModels []string `json:"allowed_models"`
	}
	if err := json.Unmarshal([]byte(stub.value), &configuration); err != nil {
		return appmodelsettings.StoredConfiguration{}, true, err
	}
	return appmodelsettings.StoredConfiguration{
		BaseURL: configuration.BaseURL, APIKey: configuration.APIKey,
		Model: configuration.Model, AllowedModels: configuration.AllowedModels,
	}, true, nil
}

func (settingsRepositoryStub) Save(context.Context, appmodelsettings.StoredConfiguration) error {
	return nil
}
