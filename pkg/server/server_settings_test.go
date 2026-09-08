package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	storagehttp "github.com/whhhh1500/auto-agent/pkg/adapter/httpapi/storage"
	appmodelsettings "github.com/whhhh1500/auto-agent/pkg/app/modelsettings"
	appsettings "github.com/whhhh1500/auto-agent/pkg/app/settings"
	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/storage"
)

func TestSettingsRoutesAuthorizeBeforeAvailabilityAndDecoding(t *testing.T) {
	ordinary := core.Principal{SubjectID: "ordinary", Attributes: map[string]string{"role": storage.RoleAccountUser}}
	server := &Server{
		authenticator: AuthenticatorFunc(func(*http.Request) (core.Principal, error) { return ordinary, nil }),
		maxBody:       1 << 20,
	}

	request := httptest.NewRequest(http.MethodPut, "/v1/admin/settings/llm", strings.NewReader("{"))
	request.SetPathValue("key", "llm")
	response := httptest.NewRecorder()
	server.handlePutSetting(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("unauthorized malformed settings write status=%d body=%s", response.Code, response.Body.String())
	}

	request = httptest.NewRequest(http.MethodGet, "/v1/admin/settings/llm", nil)
	request.SetPathValue("key", "llm")
	response = httptest.NewRecorder()
	server.handleGetSetting(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("unauthorized disabled settings read status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestSettingsExplicitRepositoryWorksWithoutAccounts(t *testing.T) {
	repository := &settingsRepositoryStub{values: map[string]string{"empty": ""}}
	server := newSettingsTestServer(t, core.Principal{
		SubjectID: "platform", Attributes: map[string]string{"role": storage.RoleAccountAdmin},
	}, Config{SettingsRepository: repository})
	if server.accounts != nil {
		t.Fatal("explicit settings repository unexpectedly requires accounts")
	}
	if server.settingsRepository != repository || server.settingsUseCases == nil {
		t.Fatalf("settings composition repository=%#v usecases=%#v", server.settingsRepository, server.settingsUseCases)
	}

	put := httptest.NewRequest(http.MethodPut, "/v1/admin/settings/other", strings.NewReader(`{"value":{"model":"fixture"},"future_client_field":true}`))
	put.SetPathValue("key", "other")
	putResponse := httptest.NewRecorder()
	server.handlePutSetting(putResponse, put)
	if putResponse.Code != http.StatusOK || putResponse.Body.String() != `{"status":"saved"}`+"\n" {
		t.Fatalf("settings write status=%d body=%s", putResponse.Code, putResponse.Body.String())
	}
	if got := repository.values["other"]; got != `{"model":"fixture"}` {
		t.Fatalf("persisted raw value=%s", got)
	}
	for name, body := range map[string]string{
		"missing-value": `{"future_client_field":true}`,
		"null-value":    `{"value":null}`,
	} {
		t.Run(name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPut, "/v1/admin/settings/required", strings.NewReader(body))
			request.SetPathValue("key", "required")
			response := httptest.NewRecorder()
			server.handlePutSetting(response, request)
			if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "value is required") {
				t.Fatalf("missing/null value status=%d body=%s", response.Code, response.Body.String())
			}
			if _, found := repository.values["required"]; found {
				t.Fatal("invalid value write reached repository")
			}
		})
	}
	emptyValue := httptest.NewRequest(http.MethodPut, "/v1/admin/settings/explicit-empty", strings.NewReader(`{"value":""}`))
	emptyValue.SetPathValue("key", "explicit-empty")
	emptyResponse := httptest.NewRecorder()
	server.handlePutSetting(emptyResponse, emptyValue)
	if emptyResponse.Code != http.StatusOK || repository.values["explicit-empty"] != `""` {
		t.Fatalf("explicit empty value status=%d persisted=%q", emptyResponse.Code, repository.values["explicit-empty"])
	}
	invalid := httptest.NewRequest(http.MethodPut, "/v1/admin/settings/invalid", strings.NewReader(`{"value":"ignored"}`))
	invalid.SetPathValue("key", " \t")
	invalidResponse := httptest.NewRecorder()
	server.handlePutSetting(invalidResponse, invalid)
	if invalidResponse.Code != http.StatusBadRequest || !strings.Contains(invalidResponse.Body.String(), "setting key is empty") {
		t.Fatalf("typed invalid settings error status=%d body=%s", invalidResponse.Code, invalidResponse.Body.String())
	}

	get := httptest.NewRequest(http.MethodGet, "/v1/admin/settings/empty", nil)
	get.SetPathValue("key", "empty")
	getResponse := httptest.NewRecorder()
	server.handleGetSetting(getResponse, get)
	if getResponse.Code != http.StatusOK || getResponse.Body.String() != `{"key":"empty","found":true,"redacted":true,"write_only":true}`+"\n" {
		t.Fatalf("found-empty settings read status=%d body=%s", getResponse.Code, getResponse.Body.String())
	}

	missing := httptest.NewRequest(http.MethodGet, "/v1/admin/settings/missing", nil)
	missing.SetPathValue("key", "missing")
	missingResponse := httptest.NewRecorder()
	server.handleGetSetting(missingResponse, missing)
	if missingResponse.Code != http.StatusOK || missingResponse.Body.String() != `{"key":"missing","found":false}`+"\n" {
		t.Fatalf("missing settings read status=%d body=%s", missingResponse.Code, missingResponse.Body.String())
	}
}

func TestSettingsSensitiveGetOmitsCompositeAndScalarSecrets(t *testing.T) {
	secrets := map[string]string{
		"llm":               `{"api_key":"pre-middle-unique-tail"}`,
		"DEPLOY_TOKEN":      "abc-hidden-different-length-LAST",
		"storage.resources": `{"type":"s3","secret_key":"storage-resource-sentinel"}`,
		"storage.sessions":  `{"type":"s3","secret_key":"storage-session-sentinel"}`,
		"oauth_config":      `{"client_secret":"oauth-sentinel"}`,
		"config":            `{"token":"config-sentinel"}`,
	}
	repository := &settingsRepositoryStub{values: secrets}
	server := newSettingsTestServer(t, core.Principal{
		SubjectID: "platform", Attributes: map[string]string{"role": storage.RoleAccountAdmin},
	}, Config{SettingsRepository: repository})

	for key, secret := range secrets {
		t.Run(key, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "/v1/admin/settings/"+key, nil)
			request.SetPathValue("key", key)
			response := httptest.NewRecorder()
			server.handleGetSetting(response, request)
			body := response.Body.String()
			if response.Code != http.StatusOK || !strings.Contains(body, `"redacted":true`) ||
				!strings.Contains(body, `"write_only":true`) || strings.Contains(body, `"value"`) {
				t.Fatalf("sensitive response status=%d body=%s", response.Code, body)
			}
			for _, forbidden := range []string{secret, "pre", "tail", "middle", "hidden", "LAST", strconv.Itoa(len(secret))} {
				if strings.Contains(body, forbidden) {
					t.Fatalf("sensitive response exposed %q: %s", forbidden, body)
				}
			}
		})
	}
}

func TestModelSettingsCanonicalRoutesPreserveReplaceAndClearAPIKey(t *testing.T) {
	repository := &modelSettingsRepositoryStub{}
	server := newSettingsTestServer(t, core.Principal{
		SubjectID: "platform", Attributes: map[string]string{"role": storage.RoleAccountAdmin},
	}, Config{ModelSettingsRepository: repository})

	initialWithoutKey := httptest.NewRequest(http.MethodPut, "/v1/admin/model-settings/llm", strings.NewReader(`{"model":"model-a"}`))
	response := httptest.NewRecorder()
	server.handlePutModelSettings(response, initialWithoutKey)
	if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "api_key is required") {
		t.Fatalf("initial omission status=%d body=%s", response.Code, response.Body.String())
	}

	first := httptest.NewRequest(http.MethodPut, "/v1/admin/model-settings/llm", strings.NewReader(`{"base_url":"https://gateway.test/v1","model":"model-b","max_tokens":512,"allowed_models":["model-b","model-a","model-b"],"api_key":"pre-middle-sentinel-tail","future_client_field":true}`))
	response = httptest.NewRecorder()
	server.handlePutModelSettings(response, first)
	if response.Code != http.StatusOK || response.Body.String() != `{"status":"saved"}`+"\n" {
		t.Fatalf("initial canonical save status=%d body=%s", response.Code, response.Body.String())
	}
	if repository.configuration.APIKey != "pre-middle-sentinel-tail" || repository.configuration.Provider != appmodelsettings.ProviderOpenAI || repository.configuration.Protocol != appmodelsettings.ProtocolOpenAIChatCompletions || repository.configuration.MaxTokens != 512 || strings.Join(repository.configuration.AllowedModels, ",") != "model-a,model-b" || repository.configuration.Source != appmodelsettings.ConfigSourceDB {
		t.Fatalf("initial stored config=%#v", repository.configuration)
	}

	preserve := httptest.NewRequest(http.MethodPut, "/v1/admin/model-settings/llm", strings.NewReader(`{"base_url":"","model":"model-b"}`))
	response = httptest.NewRecorder()
	server.handlePutModelSettings(response, preserve)
	if response.Code != http.StatusOK || repository.configuration.APIKey != "pre-middle-sentinel-tail" || repository.configuration.MaxTokens != 512 {
		t.Fatalf("preserve status=%d config=%#v", response.Code, repository.configuration)
	}

	get := httptest.NewRequest(http.MethodGet, "/v1/admin/model-settings/llm", nil)
	response = httptest.NewRecorder()
	server.handleGetModelSettings(response, get)
	body := response.Body.String()
	if response.Code != http.StatusOK || !strings.Contains(body, `"has_api_key":true`) || !strings.Contains(body, `"api_key_preview":"pre••••••••tail"`) || !strings.Contains(body, `"provider":"openai"`) || !strings.Contains(body, `"protocol":"openai-chat-completions"`) || !strings.Contains(body, `"max_tokens":512`) || !strings.Contains(body, `"config_source":"db"`) || strings.Contains(body, "middle") || strings.Contains(body, "sentinel") || strings.Contains(body, `"api_key":"`) {
		t.Fatalf("safe model settings get status=%d body=%s", response.Code, body)
	}

	clear := httptest.NewRequest(http.MethodPut, "/v1/admin/model-settings/llm", strings.NewReader(`{"model":"model-b","clear_api_key":true}`))
	response = httptest.NewRecorder()
	server.handlePutModelSettings(response, clear)
	if response.Code != http.StatusOK || repository.configuration.APIKey != "" || !repository.found {
		t.Fatalf("clear status=%d config=%#v", response.Code, repository)
	}
	clearAgain := httptest.NewRequest(http.MethodPut, "/v1/admin/model-settings/llm", strings.NewReader(`{"model":"model-b","clear_api_key":true}`))
	response = httptest.NewRecorder()
	server.handlePutModelSettings(response, clearAgain)
	if response.Code != http.StatusOK || repository.configuration.APIKey != "" {
		t.Fatalf("idempotent clear status=%d config=%#v", response.Code, repository.configuration)
	}

	for name, payload := range map[string]string{
		"empty":      `{"model":"model-b","api_key":""}`,
		"null":       `{"model":"model-b","api_key":null}`,
		"clear null": `{"model":"model-b","clear_api_key":null}`,
		"max zero":   `{"model":"model-b","max_tokens":0}`,
		"max null":   `{"model":"model-b","max_tokens":null}`,
		"conflict":   `{"model":"model-b","api_key":"new-key","clear_api_key":true}`,
	} {
		t.Run(name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPut, "/v1/admin/model-settings/llm", strings.NewReader(payload))
			result := httptest.NewRecorder()
			server.handlePutModelSettings(result, request)
			if result.Code != http.StatusBadRequest {
				t.Fatalf("invalid canonical input status=%d body=%s", result.Code, result.Body.String())
			}
		})
	}
	falseClear := httptest.NewRequest(http.MethodPut, "/v1/admin/model-settings/llm", strings.NewReader(`{"model":"model-b","api_key":"replacement-key","clear_api_key":false}`))
	response = httptest.NewRecorder()
	server.handlePutModelSettings(response, falseClear)
	if response.Code != http.StatusOK || repository.configuration.APIKey != "replacement-key" {
		t.Fatalf("false clear must be equivalent to omission: status=%d config=%#v", response.Code, repository.configuration)
	}
	protocolChange := httptest.NewRequest(http.MethodPut, "/v1/admin/model-settings/llm", strings.NewReader(`{"model":"model-b","provider":"anthropic"}`))
	response = httptest.NewRecorder()
	server.handlePutModelSettings(response, protocolChange)
	if response.Code != http.StatusOK || repository.configuration.Provider != appmodelsettings.ProviderAnthropic || repository.configuration.Protocol != appmodelsettings.ProtocolAnthropicMessages {
		t.Fatalf("provider default protocol status=%d config=%#v", response.Code, repository.configuration)
	}
	for name, payload := range map[string]string{
		"known incompatible": `{"model":"model-b","provider":"openai","protocol":"anthropic-messages"}`,
		"null provider":      `{"model":"model-b","provider":null}`,
		"wrong protocol":     `{"model":"model-b","protocol":17}`,
	} {
		t.Run(name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPut, "/v1/admin/model-settings/llm", strings.NewReader(payload))
			result := httptest.NewRecorder()
			server.handlePutModelSettings(result, request)
			if result.Code != http.StatusBadRequest {
				t.Fatalf("invalid provider/protocol status=%d body=%s", result.Code, result.Body.String())
			}
		})
	}
}

func TestModelSettingsAbsentGetUsesAnEmptyAllowListWireValue(t *testing.T) {
	server := newSettingsTestServer(t, core.Principal{
		SubjectID: "platform", Attributes: map[string]string{"role": storage.RoleAccountAdmin},
	}, Config{ModelSettingsRepository: &modelSettingsRepositoryStub{}})
	response := httptest.NewRecorder()
	server.handleGetModelSettings(response, httptest.NewRequest(http.MethodGet, "/v1/admin/model-settings/llm", nil))
	if response.Code != http.StatusOK || response.Body.String() != `{"found":false,"base_url":"","model":"","provider":"openai","protocol":"openai-chat-completions","max_tokens":0,"allowed_models":[],"has_api_key":false,"executable":true,"execution_status":"legacy"}`+"\n" {
		t.Fatalf("absent model settings wire status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestModelSettingsLegacyGenericPutPreservesOnlyLegacyEmptyKey(t *testing.T) {
	repository := &modelSettingsRepositoryStub{found: true, configuration: appmodelsettings.StoredConfiguration{
		BaseURL: "https://legacy.test/v1", APIKey: "existing-key", Model: "model-a",
	}}
	server := newSettingsTestServer(t, core.Principal{
		SubjectID: "platform", Attributes: map[string]string{"role": storage.RoleAccountAdmin},
	}, Config{ModelSettingsRepository: repository, SettingsRepository: &settingsRepositoryStub{values: map[string]string{
		"llm": `{"api_key":"generic-route-secret"}`,
	}}})

	legacy := httptest.NewRequest(http.MethodPut, "/v1/admin/settings/llm", strings.NewReader(`{"value":{"base_url":"https://legacy.test/v1","model":"model-a","api_key":"","future":true},"future":true}`))
	legacy.SetPathValue("key", "llm")
	response := httptest.NewRecorder()
	server.handlePutSetting(response, legacy)
	if response.Code != http.StatusOK || repository.configuration.APIKey != "existing-key" {
		t.Fatalf("legacy preserve status=%d config=%#v", response.Code, repository.configuration)
	}

	genericGet := httptest.NewRequest(http.MethodGet, "/v1/admin/settings/llm", nil)
	genericGet.SetPathValue("key", "llm")
	response = httptest.NewRecorder()
	server.handleGetSetting(response, genericGet)
	body := response.Body.String()
	if response.Code != http.StatusOK || !strings.Contains(body, `"redacted":true`) || strings.Contains(body, "generic-route-secret") || strings.Contains(body, `"value"`) {
		t.Fatalf("generic LLM GET must remain fail closed: status=%d body=%s", response.Code, body)
	}
}

func TestSettingsRepositoryCompatibilityWiresModelSettingsWithoutRawGenericOverwrite(t *testing.T) {
	repository := &settingsRepositoryStub{values: map[string]string{}}
	server := newSettingsTestServer(t, core.Principal{
		SubjectID: "platform", Attributes: map[string]string{"role": storage.RoleAccountAdmin},
	}, Config{SettingsRepository: repository})
	if server.modelSettingsUseCases == nil {
		t.Fatal("SettingsRepository-only server did not compose model settings use cases")
	}
	legacy := httptest.NewRequest(http.MethodPut, "/v1/admin/settings/llm", strings.NewReader(`{"value":{"base_url":"https://gateway.test/v1","model":"model-a","api_key":"compatibility-key"}}`))
	legacy.SetPathValue("key", "llm")
	response := httptest.NewRecorder()
	server.handlePutSetting(response, legacy)
	if response.Code != http.StatusOK {
		t.Fatalf("legacy compatibility PUT status=%d body=%s", response.Code, response.Body.String())
	}
	persisted := repository.values["llm"]
	if !strings.Contains(persisted, `"base_url":"https://gateway.test/v1"`) || !strings.Contains(persisted, `"model":"model-a"`) || strings.Contains(persisted, `"value"`) {
		t.Fatalf("legacy route did not use typed persisted record: %s", persisted)
	}
}

func TestModelSettingsRoutesAuthorizeBeforeAvailabilityAndDecode(t *testing.T) {
	ordinary := core.Principal{SubjectID: "ordinary", Attributes: map[string]string{"role": storage.RoleAccountUser}}
	server := &Server{
		authenticator: AuthenticatorFunc(func(*http.Request) (core.Principal, error) { return ordinary, nil }),
		maxBody:       1 << 20,
	}
	for name, handler := range map[string]func(http.ResponseWriter, *http.Request){
		"get": server.handleGetModelSettings,
		"put": server.handlePutModelSettings,
	} {
		t.Run(name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPut, "/v1/admin/model-settings/llm", strings.NewReader("{"))
			if name == "get" {
				request = httptest.NewRequest(http.MethodGet, "/v1/admin/model-settings/llm", nil)
			}
			response := httptest.NewRecorder()
			handler(response, request)
			if response.Code != http.StatusForbidden {
				t.Fatalf("unauthorized %s status=%d body=%s", name, response.Code, response.Body.String())
			}
		})
	}
}

func TestModelSettingsRoutesDispatchThroughServerMux(t *testing.T) {
	repository := &modelSettingsRepositoryStub{}
	server := newSettingsTestServer(t, core.Principal{
		SubjectID: "platform", Attributes: map[string]string{"role": storage.RoleAccountAdmin},
	}, Config{ModelSettingsRepository: repository})
	handler := server.Handler()
	put := httptest.NewRequest(http.MethodPut, "/v1/admin/model-settings/llm", strings.NewReader(`{"model":"model-a","api_key":"dispatch-key"}`))
	putResponse := httptest.NewRecorder()
	handler.ServeHTTP(putResponse, put)
	if putResponse.Code != http.StatusOK || repository.configuration.APIKey != "dispatch-key" {
		t.Fatalf("mux PUT status=%d config=%#v body=%s", putResponse.Code, repository.configuration, putResponse.Body.String())
	}
	getResponse := httptest.NewRecorder()
	handler.ServeHTTP(getResponse, httptest.NewRequest(http.MethodGet, "/v1/admin/model-settings/llm", nil))
	if getResponse.Code != http.StatusOK || strings.Contains(getResponse.Body.String(), "dispatch-key") {
		t.Fatalf("mux GET status=%d body=%s", getResponse.Code, getResponse.Body.String())
	}
}

func TestSettingsLegacyAccountsFallbackAndExplicitUseCasesPrecedence(t *testing.T) {
	legacy := &legacySettingsAccountStore{settingsRepositoryStub: settingsRepositoryStub{values: map[string]string{}}}
	server := newSettingsTestServer(t, core.Principal{
		SubjectID: "platform", Attributes: map[string]string{"role": storage.RoleAccountAdmin},
	}, Config{Accounts: legacy})
	if server.settingsRepository != legacy || server.settingsUseCases == nil {
		t.Fatalf("legacy settings fallback repository=%#v usecases=%#v", server.settingsRepository, server.settingsUseCases)
	}
	request := httptest.NewRequest(http.MethodPut, "/v1/admin/settings/legacy", strings.NewReader(`{"value":"value"}`))
	request.SetPathValue("key", "legacy")
	response := httptest.NewRecorder()
	server.handlePutSetting(response, request)
	if response.Code != http.StatusOK || legacy.values["legacy"] != `"value"` {
		t.Fatalf("legacy fallback status=%d values=%#v", response.Code, legacy.values)
	}

	explicit := &settingsUseCasesStub{}
	server = newSettingsTestServer(t, core.Principal{}, Config{
		SettingsRepository: &settingsRepositoryStub{}, SettingsUseCases: explicit,
	})
	if server.settingsUseCases != explicit {
		t.Fatal("explicit settings use cases did not take precedence")
	}
}

func TestStorageSettingsUseExplicitRepositoryWithoutAccounts(t *testing.T) {
	repository := &settingsRepositoryStub{values: map[string]string{}}
	server := newSettingsTestServer(t, core.Principal{
		SubjectID: "platform", Attributes: map[string]string{"role": storage.RoleAccountAdmin},
	}, Config{SettingsRepository: repository})
	handler := storageSettingsHandler{server: server, key: storageSessionsKey}
	request := httptest.NewRequest(http.MethodPut, "/v1/admin/storage/sessions", strings.NewReader(`{"type":"embedded"}`))
	response := httptest.NewRecorder()
	handler.put(response, request)
	if response.Code != http.StatusOK || repository.values[storageSessionsKey] != `{"type":"embedded"}` {
		t.Fatalf("storage save status=%d values=%#v", response.Code, repository.values)
	}

	get := httptest.NewRequest(http.MethodGet, "/v1/admin/storage/sessions", nil)
	getResponse := httptest.NewRecorder()
	handler.get(getResponse, get)
	if getResponse.Code != http.StatusOK || !strings.Contains(getResponse.Body.String(), `"found":true`) {
		t.Fatalf("storage load status=%d body=%s", getResponse.Code, getResponse.Body.String())
	}
}

func TestLegacyStorageMaskOnlyMapsInboundAndPersistedMarkerReadsAsUnset(t *testing.T) {
	request := storagehttp.PutRequest{
		SecretKey: storagehttp.OptionalString{Present: true, Value: legacySecretMask},
	}
	mapLegacyStorageSecretMask(&request)
	if request.SecretKey.Present {
		t.Fatal("legacy mask was not mapped to omitted secret intent")
	}

	view := storageConfigView(StorageConfig{Type: "s3", SecretKey: legacySecretMask})
	if view.HasSecret || view.SecretPreview != "" {
		t.Fatalf("persisted legacy mask must remain unusable and undisclosed: %#v", view)
	}
}

func newSettingsTestServer(t *testing.T, principal core.Principal, config Config) *Server {
	t.Helper()
	config.Runtime = &core.Runtime{}
	config.Sessions = core.NewMemorySessionStore()
	config.Authenticator = AuthenticatorFunc(func(*http.Request) (core.Principal, error) { return principal, nil })
	server, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	return server
}

type settingsRepositoryStub struct{ values map[string]string }

func (repository *settingsRepositoryStub) GetSetting(_ context.Context, key string) (string, bool, error) {
	value, found := repository.values[key]
	return value, found, nil
}

func (repository *settingsRepositoryStub) SetSetting(_ context.Context, key, value string) error {
	if repository.values == nil {
		repository.values = map[string]string{}
	}
	repository.values[key] = value
	return nil
}

type legacySettingsAccountStore struct {
	storage.AccountStore
	settingsRepositoryStub
}

func (store *legacySettingsAccountStore) GetSetting(ctx context.Context, key string) (string, bool, error) {
	return store.settingsRepositoryStub.GetSetting(ctx, key)
}

func (store *legacySettingsAccountStore) SetSetting(ctx context.Context, key, value string) error {
	return store.settingsRepositoryStub.SetSetting(ctx, key, value)
}

type settingsUseCasesStub struct{}

func (*settingsUseCasesStub) Get(context.Context, appsettings.GetCommand) (appsettings.GetResult, error) {
	return appsettings.GetResult{}, nil
}

func (*settingsUseCasesStub) Put(context.Context, appsettings.PutCommand) error { return nil }

type modelSettingsRepositoryStub struct {
	configuration appmodelsettings.StoredConfiguration
	found         bool
	err           error
}

func (repository *modelSettingsRepositoryStub) Load(context.Context) (appmodelsettings.StoredConfiguration, bool, error) {
	return repository.configuration, repository.found, repository.err
}

func (repository *modelSettingsRepositoryStub) Save(_ context.Context, configuration appmodelsettings.StoredConfiguration) error {
	if repository.err != nil {
		return repository.err
	}
	repository.configuration = configuration
	repository.found = true
	return nil
}
