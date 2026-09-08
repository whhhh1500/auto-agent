package modelsettings

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"strings"
	"testing"

	sqlsettings "github.com/whhhh1500/auto-agent/pkg/adapter/sql/settings"
	"github.com/whhhh1500/auto-agent/pkg/adapter/sql/sqlkit"
	appmodelsettings "github.com/whhhh1500/auto-agent/pkg/app/modelsettings"

	_ "modernc.org/sqlite"
)

func TestStoreKeepsLegacyShapeAndEncryptedAPIKeyAtRest(t *testing.T) {
	ctx := context.Background()
	settings, db := newEncryptedSettingsStore(t)
	defer db.Close()
	store, err := New(settings)
	if err != nil {
		t.Fatal(err)
	}
	configuration := appmodelsettings.StoredConfiguration{
		BaseURL: "https://gateway.test/v1", APIKey: "model-key-sentinel", Model: "model-b",
		Provider: appmodelsettings.ProviderAnthropic, Protocol: appmodelsettings.ProtocolAnthropicMessages,
		MaxTokens: 321, AllowedModels: []string{"model-b", "model-a", "model-b"}, Source: appmodelsettings.ConfigSourceEnvImport,
	}
	if err := store.Save(ctx, configuration); err != nil {
		t.Fatal(err)
	}
	var raw string
	if err := db.QueryRowContext(ctx, "SELECT value FROM settings WHERE key = ?", settingKey).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(raw, configuration.APIKey) || !strings.HasPrefix(raw, "sealed:") {
		t.Fatalf("API key was not sealed at rest: %q", raw)
	}
	loaded, found, err := store.Load(ctx)
	if err != nil || !found {
		t.Fatalf("load found=%t err=%v", found, err)
	}
	if loaded.APIKey != configuration.APIKey || loaded.Model != configuration.Model || loaded.Provider != configuration.Provider || loaded.Protocol != configuration.Protocol || loaded.MaxTokens != 321 || strings.Join(loaded.AllowedModels, ",") != "model-a,model-b" || loaded.Source != appmodelsettings.ConfigSourceEnvImport {
		t.Fatalf("loaded configuration=%#v", loaded)
	}
	if err := settings.SetSetting(ctx, settingKey, `{"base_url":"https://legacy.test/v1","api_key":"legacy-key","model":"legacy-model"}`); err != nil {
		t.Fatal(err)
	}
	loaded, found, err = store.Load(ctx)
	if err != nil || !found || loaded.Model != "legacy-model" || loaded.APIKey != "legacy-key" || loaded.Provider != appmodelsettings.ProviderOpenAI || loaded.Protocol != appmodelsettings.ProtocolOpenAIChatCompletions || loaded.MaxTokens != 0 || len(loaded.AllowedModels) != 0 || loaded.Source != appmodelsettings.ConfigSourceDB {
		t.Fatalf("legacy record loaded=%#v found=%t err=%v", loaded, found, err)
	}
}

func TestStoreRejectsMalformedInvalidAndPresentNullAllowedModels(t *testing.T) {
	ctx := context.Background()
	settings, db := newEncryptedSettingsStore(t)
	defer db.Close()
	store, err := New(settings)
	if err != nil {
		t.Fatal(err)
	}
	for name, raw := range map[string]string{
		"malformed":              `{`,
		"null api key":           `{"api_key":null,"model":"model-a"}`,
		"null allowed models":    `{"api_key":"key","model":"model-a","allowed_models":null}`,
		"non array allow models": `{"api_key":"key","model":"model-a","allowed_models":"model-a"}`,
		"illegal allow models":   `{"api_key":"key","model":"model-a","allowed_models":[""]}`,
		"unsafe base url":        `{"base_url":"https://user:pass@example.test/v1","api_key":"key","model":"model-a"}`,
		"unsafe api key":         `{"api_key":"key\nnewline","model":"model-a"}`,
		"null config source":     `{"api_key":"key","model":"model-a","config_source":null}`,
		"invalid config source":  `{"api_key":"key","model":"model-a","config_source":"untrusted"}`,
		"null max tokens":        `{"api_key":"key","model":"model-a","max_tokens":null}`,
		"fractional max tokens":  `{"api_key":"key","model":"model-a","max_tokens":1.5}`,
		"negative max tokens":    `{"api_key":"key","model":"model-a","max_tokens":-1}`,
		"null provider":          `{"api_key":"key","model":"model-a","provider":null}`,
		"wrong provider":         `{"api_key":"key","model":"model-a","provider":17}`,
		"null protocol":          `{"api_key":"key","model":"model-a","protocol":null}`,
		"wrong protocol":         `{"api_key":"key","model":"model-a","protocol":17}`,
		"incompatible pair":      `{"api_key":"key","model":"model-a","provider":"anthropic","protocol":"openai-responses"}`,
		"unsafe provider":        `{"api_key":"key","model":"model-a","provider":"openai\u200b","protocol":"openai-chat-completions"}`,
	} {
		t.Run(name, func(t *testing.T) {
			if err := settings.SetSetting(ctx, settingKey, raw); err != nil {
				t.Fatal(err)
			}
			_, found, err := store.Load(ctx)
			if !found || !errors.Is(err, appmodelsettings.ErrInvalidConfiguration) {
				t.Fatalf("load found=%t err=%v", found, err)
			}
		})
	}
}

func TestStoreCreateIfAbsentUsesOptionalAtomicSettingsCapability(t *testing.T) {
	store, err := New(&nonAtomicSettingsRepository{values: map[string]string{}})
	if err != nil {
		t.Fatal(err)
	}
	configuration := appmodelsettings.StoredConfiguration{APIKey: "key", Model: "model"}
	if _, err := store.CreateIfAbsent(context.Background(), configuration); err == nil || !strings.Contains(err.Error(), "atomic create-if-absent") {
		t.Fatalf("non-atomic settings repository error=%v", err)
	}
}

type nonAtomicSettingsRepository struct{ values map[string]string }

func (repository *nonAtomicSettingsRepository) GetSetting(_ context.Context, key string) (string, bool, error) {
	value, found := repository.values[key]
	return value, found, nil
}

func (repository *nonAtomicSettingsRepository) SetSetting(_ context.Context, key, value string) error {
	repository.values[key] = value
	return nil
}

func TestStoreCreateIfAbsentDoesNotOverwrite(t *testing.T) {
	ctx := context.Background()
	settings, db := newEncryptedSettingsStore(t)
	defer db.Close()
	store, err := New(settings)
	if err != nil {
		t.Fatal(err)
	}
	first := appmodelsettings.StoredConfiguration{APIKey: "first-key", Model: "first-model"}
	created, err := store.CreateIfAbsent(ctx, first)
	if err != nil || !created {
		t.Fatalf("first conditional create created=%t err=%v", created, err)
	}
	created, err = store.CreateIfAbsent(ctx, appmodelsettings.StoredConfiguration{APIKey: "second-key", Model: "second-model"})
	if err != nil || created {
		t.Fatalf("second conditional create created=%t err=%v", created, err)
	}
	loaded, found, err := store.Load(ctx)
	if err != nil || !found || loaded.Model != first.Model || loaded.APIKey != first.APIKey {
		t.Fatalf("conditional load=%#v found=%t err=%v", loaded, found, err)
	}
}

func newEncryptedSettingsStore(t *testing.T) (*sqlsettings.Store, *sql.DB) {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)"); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	store, err := sqlsettings.New(sqlsettings.Options{DB: db, Dialect: sqlkit.SQLite, Cipher: testCipher{}})
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	return store, db
}

type testCipher struct{}

func (testCipher) Encrypt(value string) (string, error) {
	return "sealed:" + base64.StdEncoding.EncodeToString([]byte(value)), nil
}

func (testCipher) Decrypt(value string) (string, error) {
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(value, "sealed:"))
	return string(decoded), err
}
