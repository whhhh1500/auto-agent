package main

import (
	"context"
	"database/sql"
	programmaticcatalog "github.com/whhhh1500/auto-agent/pkg/app/programmatic"
	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/extensions/runner"
	"github.com/whhhh1500/auto-agent/pkg/server"
	"github.com/whhhh1500/auto-agent/pkg/storage"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

type runnerTelemetryStub struct{}

type runnerTelemetrySpan struct{}

func (runnerTelemetrySpan) End(error, core.TelemetryAttributes) {}

func (runnerTelemetryStub) Start(ctx context.Context, _ string, _ core.TelemetryAttributes) (context.Context, core.TelemetrySpan) {
	return ctx, runnerTelemetrySpan{}
}

func (runnerTelemetryStub) AddCounter(context.Context, string, int64, core.TelemetryAttributes) {}

func (runnerTelemetryStub) RecordHistogram(context.Context, string, float64, string, core.TelemetryAttributes) {
}

func (runnerTelemetryStub) SetGauge(context.Context, string, float64, string, core.TelemetryAttributes) {
}

func TestTelemetryEnabledGate(t *testing.T) {
	for _, name := range []string{
		"HARNESS_OTEL_ENABLED",
		"OTEL_EXPORTER_OTLP_ENDPOINT",
		"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
		"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
	} {
		t.Setenv(name, "")
	}
	if telemetryEnabled() {
		t.Fatal("telemetry enabled without configuration")
	}
	t.Setenv("HARNESS_OTEL_ENABLED", "true")
	if !telemetryEnabled() {
		t.Fatal("explicit telemetry enable was ignored")
	}
	t.Setenv("HARNESS_OTEL_ENABLED", "")
	for _, name := range []string{
		"OTEL_EXPORTER_OTLP_ENDPOINT",
		"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
		"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
	} {
		t.Setenv(name, "http://collector:4318")
		if !telemetryEnabled() {
			t.Fatalf("%s did not enable telemetry", name)
		}
		t.Setenv(name, "")
	}
}

func TestDefaultNotificationAssemblyDeclaresWebhookOnly(t *testing.T) {
	assembly, err := defaultNotificationAssembly()
	if err != nil {
		t.Fatal(err)
	}
	refs := assembly.Refs()
	if len(refs) != 1 || refs[0].ID != "webhook" || refs[0].Version != "1" {
		t.Fatalf("notification refs=%#v", refs)
	}
	validators := assembly.Validators()
	if len(validators) != 1 || validators[0].Channel() != refs[0] {
		t.Fatalf("notification validators=%#v", validators)
	}
}

func TestBindProgrammaticExposuresMarksOnlyExplicitAllowlistAndPreservesProviderRevision(t *testing.T) {
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product, err := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "product"})
	if err != nil {
		t.Fatal(err)
	}
	registry := core.NewCapabilityRegistry()
	allowed := []*programmaticExposureTestCapability{
		newProgrammaticExposureTestCapability("memory.recall", "memory-revision"),
		newProgrammaticExposureTestCapability("rag.search", "rag-revision"),
		newProgrammaticExposureTestCapability("notify.send", "notification-revision"),
	}
	notExposed := []*programmaticExposureTestCapability{
		newProgrammaticExposureTestCapability("sandbox.exec", "sandbox-revision"),
		newProgrammaticExposureTestCapability("program.catalog", "catalog-revision"),
		newProgrammaticExposureTestCapability("program.execute", "execute-revision"),
	}
	for _, capability := range notExposed {
		if err := registry.Register(global, capability); err != nil {
			t.Fatal(err)
		}
	}
	// A separate reference registry measures provider identity without priming
	// the real startup registry with a layer that the binding helper requires.
	reference := core.NewCapabilityRegistry()
	for _, capability := range allowed {
		if err := reference.Register(global, capability); err != nil {
			t.Fatal(err)
		}
	}
	before, err := reference.Entries(product)
	if err != nil {
		t.Fatal(err)
	}
	if err := bindProgrammaticExposures(registry, global, capabilitiesFromExposureTest(allowed)); err != nil {
		t.Fatal(err)
	}
	after, err := registry.Entries(product)
	if err != nil {
		t.Fatal(err)
	}
	beforeByID, afterByID := exposureEntriesByID(before), exposureEntriesByID(after)
	for _, capability := range allowed {
		entry := afterByID[capability.manifest.ID]
		if entry.Manifest.Metadata[programmaticcatalog.ExposureKey] != programmaticcatalog.ExposureVersion || entry.Manifest.Metadata["existing"] != "preserved" {
			t.Fatalf("allowlisted capability %q metadata=%#v", capability.manifest.ID, entry.Manifest.Metadata)
		}
		if entry.ProviderRevision != beforeByID[capability.manifest.ID].ProviderRevision {
			t.Fatalf("provider revision changed for %q: before=%q after=%q", capability.manifest.ID, beforeByID[capability.manifest.ID].ProviderRevision, entry.ProviderRevision)
		}
		if _, mutated := capability.manifest.Metadata[programmaticcatalog.ExposureKey]; mutated {
			t.Fatalf("caller manifest metadata was mutated for %q: %#v", capability.manifest.ID, capability.manifest.Metadata)
		}
	}
	for _, capability := range notExposed {
		if _, marked := afterByID[capability.manifest.ID].Manifest.Metadata[programmaticcatalog.ExposureKey]; marked {
			t.Fatalf("non-allowlisted capability %q was exposed: %#v", capability.manifest.ID, afterByID[capability.manifest.ID].Manifest.Metadata)
		}
	}
}

type programmaticExposureTestCapability struct {
	manifest core.CapabilityManifest
	revision string
}

func newProgrammaticExposureTestCapability(id, revision string) *programmaticExposureTestCapability {
	return &programmaticExposureTestCapability{manifest: core.CapabilityManifest{
		ID: id, Version: "1", Name: id, Kind: core.KindTool, Contract: "test/programmatic-exposure/v1",
		Metadata: map[string]string{"existing": "preserved"},
		Tool:     &core.ToolExposure{Description: "test tool", Parameters: map[string]any{"type": "object", "additionalProperties": false}},
	}, revision: revision}
}

func (c *programmaticExposureTestCapability) Manifest() core.CapabilityManifest { return c.manifest }
func (c *programmaticExposureTestCapability) Execute(context.Context, core.CapabilityRequest) (core.CapabilityResult, error) {
	return core.CapabilityResult{OK: true}, nil
}
func (c *programmaticExposureTestCapability) ArtifactRevision() string { return c.revision }

func capabilitiesFromExposureTest(capabilities []*programmaticExposureTestCapability) []core.Capability {
	out := make([]core.Capability, len(capabilities))
	for i, capability := range capabilities {
		out[i] = capability
	}
	return out
}

func exposureEntriesByID(entries []core.SnapshotCapability) map[string]core.SnapshotCapability {
	out := make(map[string]core.SnapshotCapability, len(entries))
	for _, entry := range entries {
		out[entry.Manifest.ID] = entry
	}
	return out
}

func TestSecurityModeRejectsUnsafeProductionFallbacks(t *testing.T) {
	for _, test := range []struct {
		name       string
		mode       string
		masterKey  string
		headerAuth string
		wantErr    bool
	}{
		{name: "default dev permits embedded key", mode: "", wantErr: false},
		{name: "demo permits embedded key", mode: "demo", wantErr: false},
		{name: "production requires key", mode: "production", wantErr: true},
		{name: "production accepts key", mode: "production", masterKey: "configured", wantErr: false},
		{name: "production rejects header auth", mode: "production", masterKey: "configured", headerAuth: "true", wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			mode, err := deploymentModeFromEnv(test.mode)
			if err != nil {
				t.Fatal(err)
			}
			_, _, keyErr := masterKeyForMode(mode, test.masterKey)
			_, headerErr := devHeaderAuthEnabled(mode, test.headerAuth)
			if (keyErr != nil || headerErr != nil) != test.wantErr {
				t.Fatalf("mode=%q keyErr=%v headerErr=%v wantErr=%t", mode, keyErr, headerErr, test.wantErr)
			}
		})
	}
	if _, err := deploymentModeFromEnv("unsafe"); err == nil {
		t.Fatal("unknown deployment mode was accepted")
	}
}

func TestDatabaseConfigFromEnv(t *testing.T) {
	for _, test := range []struct {
		name         string
		mode         deploymentMode
		databaseType string
		postgresDSN  string
		sqlitePath   string
		wantDialect  storage.SQLDialect
		wantDSN      string
		wantErr      bool
	}{
		{name: "default sqlite", mode: deploymentModeDev, wantDialect: storage.SQLDialectSQLite, wantDSN: filepath.Join("data-root", "core.db")},
		{name: "configured sqlite", mode: deploymentModeDev, databaseType: "SQLITE", sqlitePath: "custom.db", wantDialect: storage.SQLDialectSQLite, wantDSN: "custom.db"},
		{name: "postgres", mode: deploymentModeDev, databaseType: "postgres", postgresDSN: "postgres://db/harness", wantDialect: storage.SQLDialectPostgres, wantDSN: "postgres://db/harness"},
		{name: "postgres requires dsn", mode: deploymentModeDev, databaseType: "postgres", wantErr: true},
		{name: "postgres rejects sqlite path", mode: deploymentModeDev, databaseType: "postgres", postgresDSN: "postgres://db/harness", sqlitePath: "stale.db", wantErr: true},
		{name: "sqlite rejects postgres dsn", mode: deploymentModeDev, databaseType: "sqlite", postgresDSN: "postgres://db/harness", wantErr: true},
		{name: "unknown type", mode: deploymentModeDev, databaseType: "mysql", wantErr: true},
		{name: "production requires postgres type", mode: deploymentModeProduction, databaseType: "sqlite", wantErr: true},
		{name: "production postgres", mode: deploymentModeProduction, databaseType: "postgres", postgresDSN: "postgres://db/harness", wantDialect: storage.SQLDialectPostgres, wantDSN: "postgres://db/harness"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("HARNESS_DATABASE_TYPE", test.databaseType)
			t.Setenv("HARNESS_POSTGRES_DSN", test.postgresDSN)
			t.Setenv("HARNESS_SQLITE_PATH", test.sqlitePath)
			got, err := databaseConfigFromEnv(test.mode, "data-root")
			if (err != nil) != test.wantErr {
				t.Fatalf("databaseConfigFromEnv() = %#v, %v; wantErr=%t", got, err, test.wantErr)
			}
			if err == nil && (got.Dialect != test.wantDialect || got.DSN != test.wantDSN) {
				t.Fatalf("database config = %#v, want dialect=%q dsn=%q", got, test.wantDialect, test.wantDSN)
			}
		})
	}
}

func TestDatabaseConnectionLimitsFromEnv(t *testing.T) {
	for _, test := range []struct {
		name     string
		dialect  storage.SQLDialect
		maxOpen  string
		maxIdle  string
		wantOpen int
		wantIdle int
		wantErr  bool
	}{
		{name: "postgres defaults", dialect: storage.SQLDialectPostgres, wantOpen: 20, wantIdle: 10},
		{name: "postgres single connection", dialect: storage.SQLDialectPostgres, maxOpen: "1", wantOpen: 1, wantIdle: 1},
		{name: "postgres explicit single idle", dialect: storage.SQLDialectPostgres, maxOpen: "1", maxIdle: "1", wantOpen: 1, wantIdle: 1},
		{name: "sqlite defaults", dialect: storage.SQLDialectSQLite, wantOpen: 1, wantIdle: 1},
		{name: "zero open rejected", dialect: storage.SQLDialectPostgres, maxOpen: "0", wantErr: true},
		{name: "negative open rejected", dialect: storage.SQLDialectPostgres, maxOpen: "-1", wantErr: true},
		{name: "invalid open rejected", dialect: storage.SQLDialectPostgres, maxOpen: "many", wantErr: true},
		{name: "zero idle rejected", dialect: storage.SQLDialectPostgres, maxIdle: "0", wantErr: true},
		{name: "negative idle rejected", dialect: storage.SQLDialectPostgres, maxIdle: "-1", wantErr: true},
		{name: "invalid idle rejected", dialect: storage.SQLDialectPostgres, maxIdle: "many", wantErr: true},
		{name: "explicit idle above open rejected", dialect: storage.SQLDialectPostgres, maxOpen: "1", maxIdle: "2", wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("HARNESS_DB_MAX_OPEN_CONNS", test.maxOpen)
			t.Setenv("HARNESS_DB_MAX_IDLE_CONNS", test.maxIdle)
			maxOpen, maxIdle, err := databaseConnectionLimitsFromEnv(test.dialect)
			if (err != nil) != test.wantErr {
				t.Fatalf("limits=(%d,%d) err=%v wantErr=%t", maxOpen, maxIdle, err, test.wantErr)
			}
			if err == nil && (maxOpen != test.wantOpen || maxIdle != test.wantIdle) {
				t.Fatalf("limits=(%d,%d), want (%d,%d)", maxOpen, maxIdle, test.wantOpen, test.wantIdle)
			}
		})
	}
}

func TestPrivatePathHelpersRespectPlatformBoundary(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "private")
	if err := privateMkdirAll(dir); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "state.db")
	if err := os.WriteFile(path, []byte("state"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := privateChmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS == "windows" {
		return
	}
	for _, test := range []struct {
		path string
		want os.FileMode
	}{{dir, 0o700}, {path, 0o600}} {
		info, err := os.Stat(test.path)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != test.want {
			t.Fatalf("%s mode=%#o want %#o", test.path, got, test.want)
		}
	}
}

func TestConfigureRunnerTelemetry(t *testing.T) {
	hub := runner.NewHub()
	configureRunnerTelemetry(hub, nil)
	if hub.Telemetry != nil {
		t.Fatalf("runner telemetry = %T, want nil when OTel is disabled", hub.Telemetry)
	}
	telemetry := runnerTelemetryStub{}
	configureRunnerTelemetry(hub, telemetry)
	if hub.Telemetry == nil {
		t.Fatal("runner telemetry was not assigned")
	}
	if _, ok := hub.Telemetry.(runnerTelemetryStub); !ok {
		t.Fatalf("runner telemetry = %T, want runnerTelemetryStub", hub.Telemetry)
	}
}

func TestConfigureRagProjectionMaintenance(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	config := server.Config{}
	if err := configureRagProjectionMaintenance(&config, db, storage.SQLDialectSQLite); err != nil {
		t.Fatal(err)
	}
	if config.RagProjection == nil {
		t.Fatal("rag projection maintainer was not injected")
	}
	if _, ok := config.RagProjection.(*storage.SQLRagIndex); !ok {
		t.Fatalf("rag projection maintainer = %T, want *storage.SQLRagIndex", config.RagProjection)
	}
	if err := configureRagProjectionMaintenance(nil, db, storage.SQLDialectSQLite); err == nil {
		t.Fatal("nil server config was accepted")
	}
}

func TestConfigureMemoryProjectionMaintenance(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	config := server.Config{}
	if err := configureMemoryProjectionMaintenance(&config, db, storage.SQLDialectSQLite); err != nil {
		t.Fatal(err)
	}
	if config.MemoryProjection == nil {
		t.Fatal("memory projection maintainer was not injected")
	}
	if _, ok := config.MemoryProjection.(*storage.SQLMemoryStore); !ok {
		t.Fatalf("memory projection maintainer = %T, want *storage.SQLMemoryStore", config.MemoryProjection)
	}
	if err := configureMemoryProjectionMaintenance(nil, db, storage.SQLDialectSQLite); err == nil {
		t.Fatal("nil server config was accepted")
	}
}

func TestConfigureProjectionMaintainersUsesCallerOwnedSharedInstances(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ragIndex, err := storage.NewSQLRagIndex(db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	memoryStore, err := storage.NewSQLMemoryStore(db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	config := server.Config{}
	if err := configureProjectionMaintainers(&config, ragIndex, memoryStore); err != nil {
		t.Fatal(err)
	}
	if config.RagProjection != ragIndex || config.MemoryProjection != memoryStore {
		t.Fatalf("projection instances were replaced: rag=%p/%p memory=%p/%p", config.RagProjection, ragIndex, config.MemoryProjection, memoryStore)
	}
}

func TestRunnerRecoveryIntervalEnvironmentFallback(t *testing.T) {
	hub := runner.NewHub()
	hub.LeaseTTL = 8 * time.Second
	t.Setenv("HARNESS_RUNNER_RECOVERY_INTERVAL", "")
	interval, err := durationEnv("HARNESS_RUNNER_RECOVERY_INTERVAL", server.DefaultRunnerRecoveryInterval(hub))
	if err != nil || interval != 4*time.Second {
		t.Fatalf("default recovery interval = %s, err=%v", interval, err)
	}
	t.Setenv("HARNESS_RUNNER_RECOVERY_INTERVAL", "3s")
	interval, err = durationEnv("HARNESS_RUNNER_RECOVERY_INTERVAL", server.DefaultRunnerRecoveryInterval(hub))
	if err != nil || interval != 3*time.Second {
		t.Fatalf("configured recovery interval = %s, err=%v", interval, err)
	}
}

func TestRunnerTokenAuthenticator(t *testing.T) {
	const token = "runner-token-for-tests-please-do-not-use"
	authenticator, err := runnerTokenAuthenticator(token, "runner-a", " runner.render, runner.analytics, runner.render ")
	if err != nil || authenticator == nil {
		t.Fatalf("authenticator=%T err=%v", authenticator, err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/runners/claim", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	principal, err := authenticator.Authenticate(request)
	if err != nil || principal.SubjectID != "runner-a" || principal.Attributes["role"] != "runner" ||
		principal.Attributes[server.RunnerCapabilitiesAttribute] != "runner.analytics,runner.render" {
		t.Fatalf("principal=%#v err=%v", principal, err)
	}
	for _, value := range []string{"", "Bearer wrong", "Basic " + token} {
		request := httptest.NewRequest(http.MethodPost, "/v1/runners/claim", nil)
		request.Header.Set("Authorization", value)
		if _, err := authenticator.Authenticate(request); err == nil {
			t.Fatalf("invalid authorization %q was accepted", value)
		}
	}
	wildcard, err := runnerTokenAuthenticator(token, "runner-all", "*")
	if err != nil {
		t.Fatal(err)
	}
	principal, err = wildcard.Authenticate(request)
	if err != nil || principal.Attributes[server.RunnerCapabilitiesAttribute] != "*" {
		t.Fatalf("wildcard principal=%#v err=%v", principal, err)
	}
}

func TestRunnerTokenAuthenticatorValidatesConfiguration(t *testing.T) {
	if authenticator, err := runnerTokenAuthenticator("", "runner-a", ""); err != nil || authenticator != nil {
		t.Fatalf("empty token fallback = %T, %v", authenticator, err)
	}
	for _, test := range []struct{ token, worker, grant string }{
		{token: "short", worker: "runner-a", grant: "runner.render"},
		{token: strings.Repeat("x", 32), worker: "", grant: "runner.render"},
		{token: strings.Repeat("x", 32), worker: "runner\ninvalid", grant: "runner.render"},
		{token: strings.Repeat("x", 32), worker: "runner-a", grant: ""},
		{token: strings.Repeat("x", 32), worker: "runner-a", grant: "runner.render,*"},
		{token: strings.Repeat("x", 32), worker: "runner-a", grant: "not-namespaced"},
	} {
		if _, err := runnerTokenAuthenticator(test.token, test.worker, test.grant); err == nil {
			t.Fatalf("invalid runner auth config token-len=%d worker=%q was accepted", len(test.token), test.worker)
		}
	}
}

func TestSessionColdSweeperTargetsSelectedSessionObjectStore(t *testing.T) {
	sessionObjects, err := storage.NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatalf("new session object store: %v", err)
	}
	resources := storage.NewMemoryObjectStore()

	sweeper := sessionColdSweeper(sessionObjects)
	if sweeper == nil {
		t.Fatal("expected cold sweeper for an object-layout session backend")
	}
	if sweeper.Store != sessionObjects {
		t.Fatal("cold sweeper did not retain the selected session object store")
	}
	if sweeper.Store == resources {
		t.Fatal("cold sweeper was pointed at the resource object store")
	}
	if sweeper.OlderThan != 30*24*time.Hour {
		t.Fatalf("OlderThan = %s, want 30d", sweeper.OlderThan)
	}
	if len(sweeper.Prefixes) != 1 || sweeper.Prefixes[0] != "sessions/" {
		t.Fatalf("Prefixes = %#v, want only sessions/", sweeper.Prefixes)
	}
}

func TestSessionColdSweeperDisabledWithoutSessionObjectStore(t *testing.T) {
	if sweeper := sessionColdSweeper(nil); sweeper != nil {
		t.Fatalf("cold sweeper = %#v, want nil", sweeper)
	}
}
