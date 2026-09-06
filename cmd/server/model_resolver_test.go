package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	modelsettingsadapter "github.com/cc-auto-agent/harness-core/pkg/adapter/modelsettings"
	sqlsettings "github.com/cc-auto-agent/harness-core/pkg/adapter/sql/settings"
	"github.com/cc-auto-agent/harness-core/pkg/adapter/sql/sqlkit"
	appmodelsettings "github.com/cc-auto-agent/harness-core/pkg/app/modelsettings"
	core "github.com/cc-auto-agent/harness-core/pkg/core"

	_ "modernc.org/sqlite"
)

func TestLegacyModelResolverWritesSelectedModelAndComposition(t *testing.T) {
	const wireModel = "profile-wire-model"
	t.Setenv("HARNESS_LLM_ALLOWED_MODELS", "")
	var captured struct {
		sync.Mutex
		models []string
	}
	endpoint := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/chat/completions" {
			http.NotFound(writer, request)
			return
		}
		var payload struct {
			Model string `json:"model"`
		}
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}
		captured.Lock()
		captured.models = append(captured.models, payload.Model)
		captured.Unlock()
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"choices":[{"message":{"content":"ok"}}]}`))
	}))
	defer endpoint.Close()

	resolver := newLegacyModelResolver(settingsRepositoryStub{found: true, value: `{"base_url":"` + endpoint.URL + `","api_key":"test-key","model":"` + wireModel + `"}`})
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	deployment, err := global.Child(core.ScopeRef{Kind: core.ScopeDeployment, ID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	product, err := deployment.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	sessionScope, err := product.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "wire-model"})
	if err != nil {
		t.Fatal(err)
	}
	principal := core.Principal{SubjectID: "wire-test", TenantID: "default", Scope: product}
	profiles := core.NewAgentProfileRegistry()
	selection := core.ModelSelection{Provider: "openai", Model: wireModel}
	if err := profiles.Bind(core.AgentProfileLayer{Scope: product, ProfileID: "wire.agent", Model: &selection}); err != nil {
		t.Fatal(err)
	}
	session, err := core.NewSession(core.SessionOptions{
		ID: "wire-model", ProfileID: "wire.agent", Principal: principal, Scope: sessionScope,
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime := &core.Runtime{
		Capabilities: core.NewCapabilityRegistry(), Profiles: profiles, Models: resolver,
	}
	if _, err := runtime.RunTurn(context.Background(), principal, session, core.TurnInput{RunID: "run-wire", Text: "hello"}, nil); err != nil {
		t.Fatal(err)
	}

	captured.Lock()
	models := append([]string(nil), captured.models...)
	captured.Unlock()
	if len(models) != 1 || models[0] != wireModel {
		t.Fatalf("outbound models=%q; want exactly %q", models, wireModel)
	}
	var start core.RunStartData
	if err := json.Unmarshal(session.Events()[0].Data, &start); err != nil {
		t.Fatal(err)
	}
	if start.Composition == nil || start.Composition.Model.Model != wireModel {
		t.Fatalf("composition model=%#v; want %q", start.Composition, wireModel)
	}
}

func TestLegacyModelResolverFailsClosedForEmptyOrMismatchedModel(t *testing.T) {
	t.Setenv("HARNESS_LLM_ALLOWED_MODELS", "")
	var requests atomic.Int32
	endpoint := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		requests.Add(1)
	}))
	defer endpoint.Close()
	resolver := newLegacyModelResolver(settingsRepositoryStub{found: true, value: `{"base_url":"` + endpoint.URL + `","api_key":"test-key","model":"configured-model"}`})

	for name, selection := range map[string]core.ModelSelection{
		"empty":      {Provider: "openai"},
		"mismatched": {Provider: "openai", Model: "selected-model"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := resolver.ResolveModel(context.Background(), selection)
			if err == nil {
				t.Fatal("invalid model selection was accepted")
			}
			if name == "empty" && err.Error() != "selected model is empty" {
				t.Fatalf("empty selection error=%q", err)
			}
			if name == "mismatched" && (!strings.Contains(err.Error(), "selected-model") || !strings.Contains(err.Error(), "configured-model")) {
				t.Fatalf("mismatch error must name selected and configured models: %q", err)
			}
		})
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("invalid selections reached an outbound endpoint %d times", got)
	}
}

func TestLegacyModelResolverPreservesMockCompatibility(t *testing.T) {
	resolver := newLegacyModelResolver(settingsRepositoryStub{err: context.DeadlineExceeded})
	adapter, err := resolver.ResolveModel(context.Background(), core.ModelSelection{Provider: "mock"})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := adapter.(core.MockLlmAdapter); !ok {
		t.Fatalf("mock adapter=%T; want core.MockLlmAdapter", adapter)
	}
}

func TestLegacyModelResolverUsesDatabaseAllowListWhenRecordExists(t *testing.T) {
	const model = "persisted-model"

	for _, test := range []struct {
		name        string
		database    string
		environment string
		wantErr     bool
	}{
		{name: "database permits despite environment rejection", database: "another-model, " + model, environment: "another-model"},
		{name: "empty database list adds no restriction", database: ", ,", environment: "another-model"},
		{name: "database rejection wins over environment permit", database: "another-model", environment: model, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("HARNESS_LLM_ALLOWED_MODELS", test.environment)
			resolver := newLegacyModelResolver(settingsRepositoryStub{found: true, value: `{"base_url":"https://api.example.test/v1","api_key":"test-key","model":"` + model + `","allowed_models":[` + allowedModelsJSON(test.database) + `]}`})
			_, err := resolver.ResolveModel(context.Background(), core.ModelSelection{Provider: "openai", Model: model})
			if (err != nil) != test.wantErr {
				t.Fatalf("resolve error=%v wantErr=%t", err, test.wantErr)
			}
			if test.wantErr && !strings.Contains(err.Error(), `model "persisted-model" is not in allowed_models`) {
				t.Fatalf("persisted allow-list error=%q", err)
			}
		})
	}
}

func TestLegacyModelResolverDoesNotFallBackToEnvironmentForInactiveOrMalformedDatabaseRecord(t *testing.T) {
	t.Setenv("HARNESS_LLM_BASE_URL", "https://api.example.test/v1")
	t.Setenv("HARNESS_LLM_API_KEY", "environment-key")
	t.Setenv("HARNESS_LLM_MODEL", "environment-model")
	t.Setenv("HARNESS_LLM_ALLOWED_MODELS", "environment-model")
	for name, repository := range map[string]settingsRepositoryStub{
		"inactive":  {found: true, value: `{"base_url":"https://db.example.test/v1","api_key":"","model":"database-model"}`},
		"malformed": {found: true, value: `{`},
	} {
		t.Run(name, func(t *testing.T) {
			resolver := newLegacyModelResolver(repository)
			_, err := resolver.ResolveModel(context.Background(), core.ModelSelection{Provider: "openai", Model: "environment-model"})
			if err == nil || !strings.Contains(err.Error(), "persisted LLM") {
				t.Fatalf("database %s unexpectedly fell back to environment: %v", name, err)
			}
		})
	}
}

func allowedModelsJSON(values string) string {
	parts := []string{}
	for _, value := range strings.Split(values, ",") {
		value = strings.TrimSpace(value)
		if value != "" {
			parts = append(parts, `"`+value+`"`)
		}
	}
	return strings.Join(parts, ",")
}

func TestLegacyModelResolverDoesNotOverrideEnvironmentAllowList(t *testing.T) {
	t.Setenv("HARNESS_LLM_BASE_URL", "https://api.example.test/v1")
	t.Setenv("HARNESS_LLM_API_KEY", "test-key")
	t.Setenv("HARNESS_LLM_MODEL", "configured-model")
	t.Setenv("HARNESS_LLM_ALLOWED_MODELS", "selected-model")
	resolver := newLegacyModelResolver(&recordingModelSettingsRepository{})

	_, err := resolver.ResolveModel(context.Background(), core.ModelSelection{Provider: "openai", Model: "selected-model"})
	if err == nil || !strings.Contains(err.Error(), `model "configured-model" not allowed by HARNESS_LLM_ALLOWED_MODELS`) {
		t.Fatalf("environment allow-list was bypassed or selection silently replaced default: %v", err)
	}
}

func TestLegacyModelResolverWritesAllowedEnvironmentModel(t *testing.T) {
	const wireModel = "environment-wire-model"
	var captured struct {
		sync.Mutex
		model string
	}
	endpoint := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var payload struct {
			Model string `json:"model"`
		}
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}
		captured.Lock()
		captured.model = payload.Model
		captured.Unlock()
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"choices":[{"message":{"content":"ok"}}]}`))
	}))
	defer endpoint.Close()
	t.Setenv("HARNESS_LLM_BASE_URL", endpoint.URL)
	t.Setenv("HARNESS_LLM_API_KEY", "test-key")
	t.Setenv("HARNESS_LLM_MODEL", wireModel)
	t.Setenv("HARNESS_LLM_ALLOWED_MODELS", "other-model, "+wireModel)

	resolver := newLegacyModelResolver(&recordingModelSettingsRepository{})
	adapter, err := resolver.ResolveModel(context.Background(), core.ModelSelection{Provider: "openai", Model: wireModel})
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.Stream(context.Background(), core.GenerateOptions{}, func(core.StreamChunk) {}); err != nil {
		t.Fatal(err)
	}
	captured.Lock()
	model := captured.model
	captured.Unlock()
	if model != wireModel {
		t.Fatalf("environment wire model=%q; want %q", model, wireModel)
	}
}

func TestLegacyModelResolverImportsCompleteEnvironmentOnlyOnce(t *testing.T) {
	t.Setenv("HARNESS_LLM_BASE_URL", "https://api.example.test/v1")
	t.Setenv("HARNESS_LLM_API_KEY", "import-test-key")
	t.Setenv("HARNESS_LLM_MODEL", "environment-model")
	t.Setenv("HARNESS_LLM_ALLOWED_MODELS", "other-model, environment-model, environment-model")
	t.Setenv("HARNESS_LLM_MAX_TOKENS", "321")
	repository := &recordingModelSettingsRepository{}
	resolver := newLegacyModelResolver(repository)
	selection := core.ModelSelection{Provider: "openai", Model: "environment-model"}
	if _, err := resolver.ResolveModel(context.Background(), selection); err != nil {
		t.Fatal(err)
	}
	if repository.createCalls != 1 || repository.created != 1 || !repository.found || repository.configuration.Source != appmodelsettings.ConfigSourceEnvImport {
		t.Fatalf("environment import creates=%d created=%d found=%t source=%q", repository.createCalls, repository.created, repository.found, repository.configuration.Source)
	}
	if strings.Join(repository.configuration.AllowedModels, ",") != "environment-model,other-model" {
		t.Fatalf("normalized imported allow-list=%q", repository.configuration.AllowedModels)
	}
	if repository.configuration.MaxTokens != 321 {
		t.Fatalf("imported max_tokens=%d; want 321", repository.configuration.MaxTokens)
	}
	if _, err := resolver.ResolveModel(context.Background(), selection); err != nil {
		t.Fatal(err)
	}
	if repository.createCalls != 1 {
		t.Fatalf("environment configuration re-imported %d times", repository.createCalls)
	}
}

func TestLegacyModelResolverFailsClosedWithoutAtomicCreateCapability(t *testing.T) {
	t.Setenv("HARNESS_LLM_BASE_URL", "https://api.example.test/v1")
	t.Setenv("HARNESS_LLM_API_KEY", "atomic-capability-test-key")
	t.Setenv("HARNESS_LLM_MODEL", "environment-model")
	t.Setenv("HARNESS_LLM_ALLOWED_MODELS", "environment-model")
	_, err := newLegacyModelResolver(settingsRepositoryStub{}).ResolveModel(context.Background(), core.ModelSelection{
		Provider: "openai", Model: "environment-model",
	})
	if err == nil || !strings.Contains(err.Error(), "atomic create-if-absent") {
		t.Fatalf("non-atomic repository error=%v", err)
	}
}

func TestLegacyModelResolverConcurrentImportUsesOneAtomicWinner(t *testing.T) {
	t.Setenv("HARNESS_LLM_BASE_URL", "https://api.example.test/v1")
	t.Setenv("HARNESS_LLM_API_KEY", "atomic-winner-test-key")
	t.Setenv("HARNESS_LLM_MODEL", "environment-model")
	t.Setenv("HARNESS_LLM_ALLOWED_MODELS", "environment-model")
	repository := &recordingModelSettingsRepository{createBarrier: make(chan struct{})}
	selection := core.ModelSelection{Provider: "openai", Model: "environment-model"}
	var group sync.WaitGroup
	errors := make(chan error, 2)
	for range 2 {
		group.Add(1)
		go func() {
			defer group.Done()
			_, err := newLegacyModelResolver(repository).ResolveModel(context.Background(), selection)
			errors <- err
		}()
	}
	group.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatalf("concurrent resolver error=%v", err)
		}
	}
	repository.mu.Lock()
	createCalls, created, found := repository.createCalls, repository.created, repository.found
	repository.mu.Unlock()
	if createCalls != 2 || created != 1 || !found {
		t.Fatalf("atomic import createCalls=%d created=%d found=%t", createCalls, created, found)
	}
}

func TestLegacyModelResolverConcurrentSQLiteImportUsesOneDurableWinner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "concurrent-model-settings.sqlite")
	openRepository := func(t *testing.T) (*sql.DB, appmodelsettings.Repository) {
		t.Helper()
		db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)")
		if err != nil {
			t.Fatal(err)
		}
		db.SetMaxOpenConns(1)
		if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)`); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
		settingsStore, err := sqlsettings.New(sqlsettings.Options{DB: db, Dialect: sqlkit.SQLite})
		if err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
		modelStore, err := modelsettingsadapter.New(settingsStore)
		if err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
		return db, modelStore
	}

	t.Setenv("HARNESS_LLM_BASE_URL", "https://api.example.test/v1")
	t.Setenv("HARNESS_LLM_API_KEY", "sqlite-concurrent-test-key")
	t.Setenv("HARNESS_LLM_MODEL", "environment-model")
	t.Setenv("HARNESS_LLM_ALLOWED_MODELS", "environment-model")
	firstDB, firstRepository := openRepository(t)
	defer firstDB.Close()
	secondDB, secondRepository := openRepository(t)
	defer secondDB.Close()

	selection := core.ModelSelection{Provider: legacyOpenAIProvider, Model: "environment-model"}
	start := make(chan struct{})
	errors := make(chan error, 2)
	for _, repository := range []appmodelsettings.Repository{firstRepository, secondRepository} {
		go func(repository appmodelsettings.Repository) {
			<-start
			_, err := newLegacyModelResolver(repository).ResolveModel(context.Background(), selection)
			errors <- err
		}(repository)
	}
	close(start)
	for range 2 {
		if err := <-errors; err != nil {
			t.Fatalf("concurrent SQLite resolver error=%v", err)
		}
	}
	configuration, found, err := firstRepository.Load(context.Background())
	if err != nil || !found || configuration.Model != "environment-model" {
		t.Fatalf("concurrent SQLite configuration found=%t model=%q err=%v", found, configuration.Model, err)
	}
}

func TestLegacyModelResolverPersistsAcrossTemporarySQLiteReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "model-settings.sqlite")
	openRepository := func(t *testing.T) (*sql.DB, appmodelsettings.Repository) {
		t.Helper()
		db, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)`); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
		settingsStore, err := sqlsettings.New(sqlsettings.Options{DB: db, Dialect: sqlkit.SQLite})
		if err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
		modelStore, err := modelsettingsadapter.New(settingsStore)
		if err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
		return db, modelStore
	}

	t.Setenv("HARNESS_LLM_BASE_URL", "https://api.example.test/v1")
	t.Setenv("HARNESS_LLM_API_KEY", "sqlite-reopen-test-key")
	t.Setenv("HARNESS_LLM_MODEL", "persisted-model")
	t.Setenv("HARNESS_LLM_ALLOWED_MODELS", "persisted-model")
	db, repository := openRepository(t)
	if _, err := newLegacyModelResolver(repository).ResolveModel(context.Background(), core.ModelSelection{
		Provider: "openai", Model: "persisted-model",
	}); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// The changed legacy environment must not override the row after reopen.
	t.Setenv("HARNESS_LLM_MODEL", "environment-model")
	t.Setenv("HARNESS_LLM_ALLOWED_MODELS", "environment-model")
	reopenedDB, reopenedRepository := openRepository(t)
	defer reopenedDB.Close()
	if _, err := newLegacyModelResolver(reopenedRepository).ResolveModel(context.Background(), core.ModelSelection{
		Provider: "openai", Model: "persisted-model",
	}); err != nil {
		t.Fatalf("reopened persisted resolver: %v", err)
	}
	configuration, found, err := reopenedRepository.Load(context.Background())
	if err != nil || !found || configuration.Model != "persisted-model" {
		t.Fatalf("reopened configuration found=%t model=%q err=%v", found, configuration.Model, err)
	}
}

func TestLegacyModelResolverRestartUsesPersistedImportedMaxTokens(t *testing.T) {
	const configuredMaxTokens = 321
	var captured struct {
		sync.Mutex
		values []int
	}
	endpoint := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var payload struct {
			MaxTokens int `json:"max_tokens"`
		}
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}
		captured.Lock()
		captured.values = append(captured.values, payload.MaxTokens)
		captured.Unlock()
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"choices":[{"message":{"content":"ok"}}]}`))
	}))
	defer endpoint.Close()

	t.Setenv("HARNESS_LLM_BASE_URL", endpoint.URL)
	t.Setenv("HARNESS_LLM_API_KEY", "import-max-tokens-key")
	t.Setenv("HARNESS_LLM_MODEL", "environment-model")
	t.Setenv("HARNESS_LLM_ALLOWED_MODELS", "environment-model")
	t.Setenv("HARNESS_LLM_MAX_TOKENS", "321")
	repository := &recordingModelSettingsRepository{}
	selection := core.ModelSelection{Provider: "openai", Model: "environment-model"}

	first, err := newLegacyModelResolver(repository).ResolveModel(context.Background(), selection)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Stream(context.Background(), core.GenerateOptions{}, func(core.StreamChunk) {}); err != nil {
		t.Fatal(err)
	}
	if repository.configuration.MaxTokens != configuredMaxTokens {
		t.Fatalf("imported max_tokens=%d; want %d", repository.configuration.MaxTokens, configuredMaxTokens)
	}

	// A new resolver models a process restart. Its environment cap must not
	// override the already-authoritative database record.
	t.Setenv("HARNESS_LLM_MAX_TOKENS", "999")
	restarted, err := newLegacyModelResolver(repository).ResolveModel(context.Background(), selection)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.Stream(context.Background(), core.GenerateOptions{}, func(core.StreamChunk) {}); err != nil {
		t.Fatal(err)
	}
	captured.Lock()
	values := append([]int(nil), captured.values...)
	captured.Unlock()
	if len(values) != 2 || values[0] != configuredMaxTokens || values[1] != configuredMaxTokens {
		t.Fatalf("outbound max_tokens=%v; want [%d %d]", values, configuredMaxTokens, configuredMaxTokens)
	}
	if repository.createCalls != 1 {
		t.Fatalf("restart re-imported max_tokens config %d times", repository.createCalls)
	}
}

func TestLegacyModelResolverEnvironmentImportSaveFailureIsVisible(t *testing.T) {
	t.Setenv("HARNESS_LLM_BASE_URL", "https://api.example.test/v1")
	t.Setenv("HARNESS_LLM_API_KEY", "import-test-key")
	t.Setenv("HARNESS_LLM_MODEL", "environment-model")
	t.Setenv("HARNESS_LLM_ALLOWED_MODELS", "environment-model")
	repository := &recordingModelSettingsRepository{createErr: errors.New("settings write unavailable")}
	resolver := newLegacyModelResolver(repository)
	_, err := resolver.ResolveModel(context.Background(), core.ModelSelection{Provider: "openai", Model: "environment-model"})
	if err == nil || !strings.Contains(err.Error(), "persist legacy environment LLM configuration") {
		t.Fatalf("environment import failure=%v", err)
	}
	if repository.createCalls != 1 || repository.found {
		t.Fatalf("failed import creates=%d found=%t", repository.createCalls, repository.found)
	}
}

func TestLegacyModelResolverNeverImportsWhenDatabaseRecordExists(t *testing.T) {
	t.Setenv("HARNESS_LLM_BASE_URL", "https://api.example.test/v1")
	t.Setenv("HARNESS_LLM_API_KEY", "environment-key")
	t.Setenv("HARNESS_LLM_MODEL", "environment-model")
	repository := &recordingModelSettingsRepository{found: true, configuration: appmodelsettings.StoredConfiguration{
		BaseURL: "https://db.example.test/v1", APIKey: "database-key", Model: "database-model", Source: appmodelsettings.ConfigSourceDB,
	}}
	resolver := newLegacyModelResolver(repository)
	if _, err := resolver.ResolveModel(context.Background(), core.ModelSelection{Provider: "openai", Model: "database-model"}); err != nil {
		t.Fatal(err)
	}
	if repository.createCalls != 0 {
		t.Fatalf("existing database record triggered %d environment imports", repository.createCalls)
	}
}

type recordingModelSettingsRepository struct {
	configuration appmodelsettings.StoredConfiguration
	found         bool
	saves         int
	createCalls   int
	created       int
	createErr     error
	createBarrier chan struct{}
	mu            sync.Mutex
}

func (repository *recordingModelSettingsRepository) Load(context.Context) (appmodelsettings.StoredConfiguration, bool, error) {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	return repository.configuration, repository.found, nil
}

func (repository *recordingModelSettingsRepository) Save(_ context.Context, configuration appmodelsettings.StoredConfiguration) error {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	repository.saves++
	repository.configuration = configuration
	repository.found = true
	return nil
}

func (repository *recordingModelSettingsRepository) CreateIfAbsent(_ context.Context, configuration appmodelsettings.StoredConfiguration) (bool, error) {
	repository.mu.Lock()
	repository.createCalls++
	barrier := repository.createBarrier
	if barrier != nil && repository.createCalls == 2 {
		close(barrier)
	}
	wait := barrier != nil && repository.createCalls == 1
	repository.mu.Unlock()
	if wait {
		<-barrier
	}
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if repository.createErr != nil {
		return false, repository.createErr
	}
	if repository.found {
		return false, nil
	}
	repository.configuration = configuration
	repository.found = true
	repository.created++
	return true, nil
}
