package main

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode"

	"github.com/cc-auto-agent/harness-core/pkg/buildinfo"
	"github.com/cc-auto-agent/harness-core/pkg/extensions/runner"
	"github.com/cc-auto-agent/harness-core/pkg/storage"

	modelsettingsadapter "github.com/cc-auto-agent/harness-core/pkg/adapter/modelsettings"
	notificationcoretool "github.com/cc-auto-agent/harness-core/pkg/adapter/notification/coretool"
	webhook "github.com/cc-auto-agent/harness-core/pkg/adapter/notification/webhook"
	webhooktargets "github.com/cc-auto-agent/harness-core/pkg/adapter/notification/webhook/targetresolver"
	sandboxexec "github.com/cc-auto-agent/harness-core/pkg/adapter/sandboxexec"
	notificationsql "github.com/cc-auto-agent/harness-core/pkg/adapter/sql/notificationtarget"
	appcontextassembly "github.com/cc-auto-agent/harness-core/pkg/app/contextassembly"
	appmodelsettings "github.com/cc-auto-agent/harness-core/pkg/app/modelsettings"
	appnotification "github.com/cc-auto-agent/harness-core/pkg/app/notification"
	"github.com/cc-auto-agent/harness-core/pkg/control"
	executionsandbox "github.com/cc-auto-agent/harness-core/pkg/execution/sandbox"
	"github.com/cc-auto-agent/harness-core/pkg/extensions/memory"
	"github.com/cc-auto-agent/harness-core/pkg/extensions/rag"
	"github.com/cc-auto-agent/harness-core/pkg/logging"
	oteltelemetry "github.com/cc-auto-agent/harness-core/pkg/telemetry/otel"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/server"
)

func main() {
	must(loadDotEnv(".env"))
	mode, err := deploymentModeFromEnv(os.Getenv("HARNESS_MODE"))
	must(err)
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	deploymentID := envOrDefault("HARNESS_DEPLOYMENT_ID", "default")
	productID := envOrDefault("HARNESS_PRODUCT_ID", "default")
	deployment, _ := global.Child(core.ScopeRef{Kind: core.ScopeDeployment, ID: deploymentID})
	product, _ := deployment.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: productID})

	capabilities := core.NewCapabilityRegistry()
	profiles := core.NewAgentProfileRegistry()
	// Storage selection is database-authoritative. Legacy environment values are
	// consulted only when their corresponding settings row is absent. With no
	// stored configuration, resources use dataDir/resources and sessions use
	// the embedded SQL store.
	dataDir := os.Getenv("HARNESS_DATA_DIR")
	if dataDir == "" {
		dataDir = "./data"
	}
	dataDir, err = filepath.Abs(dataDir)
	must(err)
	if err := privateMkdirAll(dataDir); err != nil {
		must(fmt.Errorf("create data directory: %w", err))
	}
	database, err := databaseConfigFromEnv(mode, dataDir)
	must(err)
	dialect, dbDriver, dbDSN, dbLabel := database.Dialect, database.Driver, database.DSN, database.Label
	if dialect == storage.SQLDialectSQLite {
		if dbDSN != ":memory:" {
			if err := privateMkdirAll(filepath.Dir(dbDSN)); err != nil {
				must(fmt.Errorf("create sqlite directory: %w", err))
			}
		}
	}
	db, err := sql.Open(dbDriver, dbDSN)
	must(err)
	defer db.Close()
	defaultMaxOpen, defaultMaxIdle := 1, 1
	if dialect == storage.SQLDialectPostgres {
		defaultMaxOpen, defaultMaxIdle = 20, 10
	}
	dbMaxOpen, err := intEnv("HARNESS_DB_MAX_OPEN_CONNS", defaultMaxOpen)
	must(err)
	dbMaxIdle, err := intEnv("HARNESS_DB_MAX_IDLE_CONNS", defaultMaxIdle)
	must(err)
	if dbMaxIdle > dbMaxOpen {
		must(fmt.Errorf("HARNESS_DB_MAX_IDLE_CONNS must not exceed HARNESS_DB_MAX_OPEN_CONNS"))
	}
	if dialect == storage.SQLDialectPostgres && dbMaxOpen < 2 {
		must(fmt.Errorf("HARNESS_DB_MAX_OPEN_CONNS must be at least 2 for PostgreSQL startup locking"))
	}
	dbConnLifetime, err := durationEnv("HARNESS_DB_CONN_MAX_LIFETIME", 30*time.Minute)
	must(err)
	dbConnIdleTime, err := durationEnv("HARNESS_DB_CONN_MAX_IDLE_TIME", 5*time.Minute)
	must(err)
	db.SetMaxOpenConns(dbMaxOpen)
	db.SetMaxIdleConns(dbMaxIdle)
	db.SetConnMaxLifetime(dbConnLifetime)
	db.SetConnMaxIdleTime(dbConnIdleTime)
	pingCtx, cancelPing := context.WithTimeout(context.Background(), 10*time.Second)
	must(db.PingContext(pingCtx))
	cancelPing()
	var postgresStartupLock *storage.PostgresStartupLock
	postgresStartupLockHeld := false
	if dialect == storage.SQLDialectPostgres {
		postgresStartupLock, err = storage.AcquirePostgresStartupLock(context.Background(), db)
		must(err)
		postgresStartupLockHeld = true
		defer func() {
			if !postgresStartupLockHeld {
				return
			}
			releaseCtx, cancelRelease := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancelRelease()
			if releaseErr := postgresStartupLock.Release(releaseCtx); releaseErr != nil {
				// Preserve a prior bootstrap panic/error; this is only best-effort
				// cleanup because closing the dedicated connection also releases
				// PostgreSQL advisory locks.
				log.Printf("release PostgreSQL startup lock during failed bootstrap: %v", releaseErr)
			}
		}()
	}
	accountsTableExisted, err := storage.AccountsTableExists(context.Background(), db, dialect)
	must(err)
	if dialect == storage.SQLDialectSQLite && dbDSN != ":memory:" {
		must(privateChmod(dbDSN, 0o600))
	}
	log.Printf("control database: %s", dbLabel)
	log.Printf("harness build: %s", buildinfo.String())
	sqlStore, err := storage.OpenSQLSessionStore(context.Background(), db, dialect)
	must(err)
	accounts, err := storage.NewSQLAccountStore(db, dialect)
	must(err)
	queuedPrincipal, err := storage.NewSQLQueuedPrincipalResolver(accounts, product.Segments())
	must(err)
	// Complete the pre-migration bootstrap decision before any unrelated
	// runtime/configuration restore can fail. Otherwise a failed first start
	// would leave an empty accounts table that suppresses this one-time path on
	// every retry.
	initialAdmin, err := storage.BootstrapInitialAdmin(context.Background(), accounts, accountsTableExisted)
	must(err)
	if initialAdmin.Created {
		log.Printf("ONE-TIME INITIAL ADMIN ACCOUNT: %s", initialAdmin.AccountID)
		log.Printf("ONE-TIME INITIAL ADMIN PASSWORD: %s", initialAdmin.Password)
		log.Printf("SENSITIVE: visit /console and change this password immediately; these credentials will not be printed again.")
	}
	if postgresStartupLockHeld {
		releaseCtx, cancelRelease := context.WithTimeout(context.Background(), 10*time.Second)
		releaseErr := postgresStartupLock.Release(releaseCtx)
		cancelRelease()
		postgresStartupLockHeld = false
		must(releaseErr)
	}
	// The bootstrap decision above is the only startup action protected by the
	// PostgreSQL advisory lock. Configure settings encryption immediately after
	// it so every subsequent startup read sees decrypted persisted values.
	settingsRepository, embeddedMasterKey, err := initializeSettingsRepository(
		context.Background(), mode, db, dialect, accounts, os.Getenv("HARNESS_MASTER_KEY"),
	)
	must(err)
	var modelSettingsRepository appmodelsettings.Repository
	modelSettingsRepository, err = modelsettingsadapter.New(settingsRepository)
	must(err)
	modelSettingsRepository, err = appmodelsettings.NewCachedRepository(modelSettingsRepository, nil)
	must(err)
	if embeddedMasterKey {
		log.Printf("warning: HARNESS_MASTER_KEY is unset; using the embedded database key in %s mode only", mode)
	}
	notificationChannelRef := appnotification.ChannelRef{ID: webhook.ChannelID, Version: webhook.ChannelVersion}
	notificationValidator := webhooktargets.NewConfigurationValidator()
	notificationTargetStore, err := notificationsql.New(notificationsql.Options{
		DB: db, Dialect: dialect, Cipher: settingsRepository.Cipher(),
		Channels: []appnotification.ChannelRef{notificationChannelRef},
	})
	must(err)
	notificationTargets, err := appnotification.NewService(notificationTargetStore, []appnotification.ChannelRef{notificationChannelRef}, notificationValidator)
	must(err)
	notificationResolver, err := webhooktargets.New(notificationTargets)
	must(err)
	notificationChannel, err := webhook.New(notificationResolver)
	must(err)
	notificationRegistry, err := appnotification.NewRegistry([]appnotification.Channel{notificationChannel})
	must(err)
	notificationCapabilities, err := notificationcoretool.NewWithDirectory(notificationRegistry, notificationResolver)
	must(err)
	for _, capability := range notificationCapabilities {
		must(capabilities.Register(global, capability))
	}
	// One durable memory store and one durable RAG index serve both projection
	// maintenance and the general profile's standard guarded capabilities.
	// Constructing them once prevents divergent in-process caches or rebuilders.
	memoryStore, err := storage.NewSQLMemoryStore(db, dialect)
	must(err)
	ragIndex, err := storage.NewSQLRagIndex(db, dialect)
	must(err)
	memoryCapabilities, err := memory.NewStandardCapabilities(memoryStore)
	must(err)
	for _, capability := range memoryCapabilities {
		must(capabilities.Register(global, capability))
	}
	ragSearch, err := rag.NewStandardSearchCapability("rag.search", ragIndex)
	must(err)
	must(capabilities.Register(global, ragSearch))
	configuredStorage, err := configureStorageWithMigration(
		context.Background(), dataDir, settingsRepository, sqlStore, os.LookupEnv, db, dialect,
	)
	must(err)
	sandboxRoot, err := sandboxWorkRootForDataRoot(dataDir)
	must(err)
	must(privateMkdirAll(sandboxRoot))
	sandboxProviders, err := newSandboxProviderRegistry(dataDir)
	must(err)
	// Windows currently-user basic execution intentionally requests the weak
	// host-network mode. Other platforms retain the empty compatibility value,
	// which sandboxexec normalizes to strict disabled networking.
	var sandboxNetwork executionsandbox.NetworkPolicy
	if runtime.GOOS == "windows" {
		sandboxNetwork = executionsandbox.NetworkHost
	}
	sandboxExec, err := sandboxexec.New(sandboxexec.Config{
		Registry: sandboxProviders, ProviderID: executionsandbox.ProviderID("local-ephemeral"), Version: "1",
		Network: sandboxNetwork, RequestedIsolation: false,
		Limits:         executionsandbox.Limits{WallTime: executionsandbox.DefaultExecWallTime, MaxCPUTime: executionsandbox.DefaultExecWallTime, MaxMemoryBytes: executionsandbox.DefaultExecMemoryBytes, MaxOutputBytes: executionsandbox.DefaultExecCombinedBytes},
		ArtifactPolicy: executionsandbox.ArtifactPolicy{MaxArtifacts: executionsandbox.DefaultExecArtifactCount, MaxTotalBytes: executionsandbox.DefaultExecArtifactBytes},
		WorkRoot:       sandboxRoot, ObjectStore: configuredStorage.Resources,
	})
	must(err)
	must(capabilities.Register(global, sandboxExec))
	generalProfileController, err := newGeneralProfileController(
		profiles, global, generalProfileModelSource(modelSettingsRepository), generalCapabilityIDs(),
	)
	must(err)
	must(generalProfileController.Mount(context.Background()))
	defer generalProfileController.Close()
	modelRuntime := newModelRuntimeBundle(modelSettingsRepository)
	modelSettingsUseCases, err := appmodelsettings.NewServiceWithCatalog(modelSettingsRepository, modelRuntime.Catalog, generalProfileController)
	must(err)

	// configureStorage(...) is database-authoritative and also restores any
	// non-terminal artifact migration before serving requests.
	contextAssembler, err := appcontextassembly.NewAssembler(appcontextassembly.Config{})
	must(err)
	extractiveSummarizer, err := appcontextassembly.NewExtractiveSummarizer(appcontextassembly.ExtractiveSummarizerConfig{})
	must(err)
	// Runtime: console-saved LLM settings take precedence over env at request time.
	runtime := &core.Runtime{
		Capabilities: capabilities,
		Profiles:     profiles,
		Models:       modelRuntime.Resolver,
		StreamChunks: true,
		Compactor: core.RecentTurnsCompactor{
			MaxMessages: 120, MaxToolResultChars: 4000,
		},
		Summarizer: &appcontextassembly.RollingSummarizer{
			MaxMessages: 120, KeepTail: 60, Summarizer: extractiveSummarizer,
		},
		ContextAssembler: contextAssembler.AssembleModelContext,
	}

	// Control-plane stores.
	auditStore, err := storage.NewSQLAuditStore(db, dialect)
	must(err)
	obsStore, err := storage.NewSQLObsStore(db, dialect)
	must(err)
	journal, err := storage.NewSQLBindingJournal(db, dialect)
	must(err)
	runStats, err := storage.NewSQLRunStatsStore(db, dialect)
	must(err)
	runnerStore, err := storage.NewSQLRunnerStore(db, dialect)
	must(err)
	runnerHub := runner.NewHubWithStore(runnerStore)
	runnerClaimTTL, err := durationEnv("HARNESS_RUNNER_CLAIM_TTL", 30*time.Second)
	must(err)
	if runnerClaimTTL <= 0 || runnerClaimTTL > 24*time.Hour {
		must(fmt.Errorf("HARNESS_RUNNER_CLAIM_TTL must be between 1ns and 24h"))
	}
	runnerHub.LeaseTTL = runnerClaimTTL
	runnerRecoveryInterval, err := durationEnv("HARNESS_RUNNER_RECOVERY_INTERVAL", server.DefaultRunnerRecoveryInterval(runnerHub))
	must(err)
	runnerPollInterval, err := durationEnv("HARNESS_RUNNER_RESULT_POLL_INTERVAL", runner.DefaultPollInterval)
	must(err)
	if runnerPollInterval <= 0 {
		must(fmt.Errorf("HARNESS_RUNNER_RESULT_POLL_INTERVAL must be positive"))
	}
	runnerHub.PollInterval = runnerPollInterval
	runnerAuth, err := runnerTokenAuthenticator(
		os.Getenv("HARNESS_RUNNER_TOKEN"), envOrDefault("HARNESS_RUNNER_ID", "runner-default"),
		os.Getenv("HARNESS_RUNNER_CAPABILITIES"),
	)
	must(err)
	if runnerAuth == nil {
		log.Printf("warning: HARNESS_RUNNER_TOKEN is unset; private-runner routes require a platform-admin account")
	}
	runControl, err := storage.NewSQLRunControlStore(db, dialect)
	must(err)
	delegationLinks, err := storage.NewSQLDelegationLinkStore(db, dialect)
	must(err)
	toolJournal, err := storage.NewSQLToolInvocationJournal(db, dialect)
	must(err)
	runtime.ToolJournal = toolJournal
	approvalStore, err := storage.NewSQLApprovalStore(db, dialect)
	must(err)
	evaluationStore, err := storage.NewSQLEvaluationStore(db, dialect)
	must(err)
	approvalTTL, err := durationEnv("HARNESS_APPROVAL_TTL", storage.DefaultApprovalTTL)
	must(err)
	if approvalTTL > storage.HardApprovalTTL {
		must(fmt.Errorf("HARNESS_APPROVAL_TTL must not exceed %s", storage.HardApprovalTTL))
	}
	approvalStore.TTL = approvalTTL
	runtime.Approver = approvalStore
	releases, err := control.NewReleaseManager(profiles)
	must(err)
	releases.Journal = sqlStore
	must(releases.Restore(context.Background()))
	canaryStore, err := storage.NewSQLCanaryStore(db, dialect)
	must(err)
	canaries, err := control.NewCanaryManager(releases, canaryStore)
	must(err)
	must(canaries.Restore(context.Background()))
	serverLogger, logCloser, err := logging.New(logging.Options{
		Dir:        filepath.Join(dataDir, "logs"),
		MaxBytes:   64 << 20,
		MaxBackups: 14,
	})
	must(err)
	defer logCloser.Close()
	var telemetryRecorder core.Telemetry
	var shutdownTelemetry func(context.Context) error
	if telemetryEnabled() {
		metricInterval, err := durationEnv("HARNESS_OTEL_METRIC_INTERVAL", 30*time.Second)
		must(err)
		recorder, shutdown, err := oteltelemetry.NewFromEnv(context.Background(), oteltelemetry.Config{
			ServiceName:    envOrDefault("HARNESS_OTEL_SERVICE_NAME", "harness-core"),
			MetricInterval: metricInterval,
		})
		must(err)
		telemetryRecorder, shutdownTelemetry = recorder, shutdown
		runtime.Telemetry = recorder
		log.Printf("OpenTelemetry OTLP/HTTP export enabled")
	}
	configureRunnerTelemetry(runnerHub, telemetryRecorder)
	runCancelPoll, err := durationEnv("HARNESS_RUN_CANCEL_POLL_INTERVAL", 500*time.Millisecond)
	must(err)
	runStaleAfter, err := durationEnv("HARNESS_RUN_STALE_AFTER", 2*time.Minute)
	must(err)
	runWorkerPoll, err := durationEnv("HARNESS_RUN_WORKER_POLL_INTERVAL", 250*time.Millisecond)
	must(err)
	runWorkerClaimTTL, err := durationEnv("HARNESS_RUN_WORKER_CLAIM_TTL", 30*time.Second)
	must(err)
	runWorkerConcurrency, err := intEnv("HARNESS_RUN_WORKER_CONCURRENCY", 1)
	must(err)
	runWorkerMaxAttempts, err := intEnv("HARNESS_RUN_WORKER_MAX_ATTEMPTS", storage.DefaultRunMaxAttempts)
	must(err)
	runExecutors, err := newRunExecutorRegistry(db, dialect, approvalStore, telemetryRecorder)
	must(err)

	devHeaderAuth, err := devHeaderAuthEnabled(mode, os.Getenv("HARNESS_DEV_HEADER_AUTH"))
	must(err)
	serverConfig := server.Config{
		Runtime:          runtime,
		RunExecutors:     runExecutors,
		Sessions:         configuredStorage.Sessions,
		DefaultProfileID: generalProfileID,
		Authenticator: server.AccountAuthenticator{
			Store:             accounts,
			Root:              product.Segments(),
			DevHeaderFallback: devHeaderAuth,
		},
		Accounts:                accounts,
		SettingsRepository:      settingsRepository,
		ModelSettingsRepository: modelSettingsRepository,
		ModelCatalog:            modelRuntime.Catalog,
		ModelSettingsUseCases:   modelSettingsUseCases,
		StorageConfigUseCases:   configuredStorage.UseCases,
		NotificationTargets:     notificationTargets,
		SandboxProviders:        sandboxProviders,
		Resources:               configuredStorage.Resources,
		EmbeddedResources:       configuredStorage.EmbeddedResources,
		Releases:                releases,
		Canaries:                canaries,
		Audit:                   auditStore,
		Obs:                     obsStore,
		LibraryObserver:         sqlStore.LibraryObserver(),
		BindingJournal:          journal,
		RunStats:                runStats,
		Runners:                 runnerHub,
		RunnerRecoveryInterval:  runnerRecoveryInterval,
		RunnerAuthenticator:     runnerAuth,
		RunControl:              runControl,
		RunQueue:                runControl,
		Approvals:               approvalStore,
		DelegationLinks:         delegationLinks,
		DelegationLinkCatalog:   delegationLinks,
		Evaluations:             evaluationStore,
		Evidence:                sqlStore,
		RunPrincipalResolver:    queuedPrincipal,
		RunCancelPollInterval:   runCancelPoll,
		RunStaleAfter:           runStaleAfter,
		RunWorkerPollInterval:   runWorkerPoll,
		RunWorkerClaimTTL:       runWorkerClaimTTL,
		RunWorkerConcurrency:    runWorkerConcurrency,
		RunWorkerMaxAttempts:    runWorkerMaxAttempts,
		Leaser:                  sqlStore,
		Retention:               sqlStore,
		Logger:                  serverLogger,
		Telemetry:               telemetryRecorder,
	}
	must(configureProjectionMaintainers(&serverConfig, ragIndex, memoryStore))
	api, err := server.New(serverConfig)
	must(err)

	port := os.Getenv("HARNESS_SERVER_PORT")
	if port == "" {
		port = "8080"
	}
	// Re-apply journaled dynamic bindings from the previous lifetime.
	must(api.RestoreBindings(context.Background()))
	must(api.RecoverStaleRuns(context.Background()))
	serviceCtx, stopService := context.WithCancel(context.Background())
	defer stopService()
	if configuredStorage.SetMigrationTrigger != nil {
		var migrationWorkerMu sync.Mutex
		var migrationWorkerRunning bool
		triggerMigration := func() {
			migrationWorkerMu.Lock()
			if migrationWorkerRunning {
				migrationWorkerMu.Unlock()
				return
			}
			migrationWorkerRunning = true
			migrationWorkerMu.Unlock()
			go func() {
				defer func() { migrationWorkerMu.Lock(); migrationWorkerRunning = false; migrationWorkerMu.Unlock() }()
				if configuredStorage.RunResourceMigration != nil {
					configuredStorage.RunResourceMigration(serviceCtx)
				}
			}()
		}
		// Use the same single-flight path for startup recovery and runtime PUTs.
		configuredStorage.SetMigrationTrigger(triggerMigration)
		if configuredStorage.RunResourceMigration != nil {
			triggerMigration()
		}
	}
	must(api.StartRunWorkers(serviceCtx))
	must(api.StartRunnerRecoveryLoop(serviceCtx))

	// Retention: prune bounded SQL records plus old unkeyed terminal Runner
	// tasks. Keyed task and uncertain tool fences remain durable. Cold
	// compression is deliberately scoped to the exact object backend selected
	// for object-layout sessions; it must never sweep the independently
	// hot-swappable resource store.
	api.StartRetentionLoop(serviceCtx, time.Hour, 90*24*time.Hour)
	if cold := sessionColdSweeper(configuredStorage.SessionObjects); cold != nil {
		go func() {
			ticker := time.NewTicker(6 * time.Hour)
			defer ticker.Stop()
			for {
				select {
				case <-serviceCtx.Done():
					return
				case <-ticker.C:
					if _, err := cold.Sweep(serviceCtx); err != nil && serverLogger != nil {
						serverLogger.Error("session cold sweep failed", "error", err.Error())
					}
				}
			}
		}()
	}

	handler := api.Handler()
	if telemetryRecorder != nil {
		handler = oteltelemetry.HTTPHandler(handler)
	}
	httpServer := &http.Server{
		Addr: ":" + port, Handler: handler,
		ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second,
	}

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(shutdown)
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- httpServer.ListenAndServe()
	}()

	log.Printf("harness server listening on %s", httpServer.Addr)
	nextStep := "open the Console to manage the deployment"
	if initialAdmin.Created {
		nextStep = "save the one-time administrator credentials printed above, then change the password in the Console"
	}
	log.Printf("startup summary: mode=%s database=%s resources=database-selected (local default %s) console=http://127.0.0.1:%s/console next=%s", mode, dbLabel, filepath.Join(dataDir, "resources"), port, nextStep)
	if mode != deploymentModeProduction {
		log.Printf("development identity headers: X-Harness-Tenant=acme X-Harness-Subject=alice")
	}
	select {
	case sig := <-shutdown:
		log.Printf("shutdown requested: %s", sig)
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("http server failed: %v", err)
		}
	}

	httpShutdownCtx, cancelHTTPShutdown := context.WithTimeout(context.Background(), 15*time.Second)
	if err := httpServer.Shutdown(httpShutdownCtx); err != nil {
		log.Printf("http shutdown incomplete: %v", err)
	}
	cancelHTTPShutdown()
	workerShutdownCtx, cancelWorkerShutdown := context.WithTimeout(context.Background(), 30*time.Second)
	if err := api.Shutdown(workerShutdownCtx); err != nil {
		log.Printf("run worker shutdown incomplete: %v", err)
	}
	cancelWorkerShutdown()
	stopService()
	if shutdownTelemetry != nil {
		telemetryShutdownCtx, cancelTelemetryShutdown := context.WithTimeout(context.Background(), 10*time.Second)
		if err := shutdownTelemetry(telemetryShutdownCtx); err != nil {
			log.Printf("telemetry shutdown incomplete: %v", err)
		}
		cancelTelemetryShutdown()
	}
}

func must(err error) {
	if err != nil {
		panic(fmt.Sprintf("bootstrap failed: %v", err))
	}
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

// privateMkdirAll and privateChmod secure server-owned local state. Windows
// ACLs are the operator's responsibility: Go's POSIX mode bits do not model
// Windows ACL semantics, so this code deliberately does not claim otherwise.
func privateMkdirAll(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	return privateChmod(path, 0o700)
}

func privateChmod(path string, mode os.FileMode) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	if err := os.Chmod(path, mode); err != nil {
		return err
	}
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.Mode().Perm() != mode.Perm() {
		return fmt.Errorf("private path %s mode=%#o want %#o", path, info.Mode().Perm(), mode.Perm())
	}
	return nil
}

type deploymentMode string

const (
	deploymentModeDev        deploymentMode = "dev"
	deploymentModeDemo       deploymentMode = "demo"
	deploymentModeProduction deploymentMode = "production"
)

func deploymentModeFromEnv(raw string) (deploymentMode, error) {
	switch deploymentMode(strings.ToLower(strings.TrimSpace(raw))) {
	case "", deploymentModeDev:
		return deploymentModeDev, nil
	case deploymentModeDemo:
		return deploymentModeDemo, nil
	case deploymentModeProduction:
		return deploymentModeProduction, nil
	default:
		return "", fmt.Errorf("HARNESS_MODE must be dev, demo, or production")
	}
}

type databaseConfig struct {
	Dialect storage.SQLDialect
	Driver  string
	DSN     string
	Label   string
}

func databaseConfigFromEnv(mode deploymentMode, dataDir string) (databaseConfig, error) {
	databaseType := strings.ToLower(strings.TrimSpace(os.Getenv("HARNESS_DATABASE_TYPE")))
	if databaseType == "" {
		databaseType = "sqlite"
	}
	postgresDSN := strings.TrimSpace(os.Getenv("HARNESS_POSTGRES_DSN"))
	switch databaseType {
	case "sqlite":
		if postgresDSN != "" {
			return databaseConfig{}, fmt.Errorf("HARNESS_POSTGRES_DSN requires HARNESS_DATABASE_TYPE=postgres")
		}
		if mode == deploymentModeProduction {
			return databaseConfig{}, fmt.Errorf("HARNESS_DATABASE_TYPE=postgres is required in production mode")
		}
		path := strings.TrimSpace(os.Getenv("HARNESS_SQLITE_PATH"))
		if path == "" {
			path = filepath.Join(dataDir, "core.db")
		}
		return databaseConfig{
			Dialect: storage.SQLDialectSQLite, Driver: "sqlite", DSN: path, Label: "sqlite " + path,
		}, nil
	case "postgres":
		if postgresDSN == "" {
			return databaseConfig{}, fmt.Errorf("HARNESS_POSTGRES_DSN is required when HARNESS_DATABASE_TYPE=postgres")
		}
		if strings.TrimSpace(os.Getenv("HARNESS_SQLITE_PATH")) != "" {
			return databaseConfig{}, fmt.Errorf("HARNESS_SQLITE_PATH cannot be set when HARNESS_DATABASE_TYPE=postgres")
		}
		return databaseConfig{
			Dialect: storage.SQLDialectPostgres, Driver: "pgx", DSN: postgresDSN, Label: "postgresql",
		}, nil
	default:
		return databaseConfig{}, fmt.Errorf("HARNESS_DATABASE_TYPE must be sqlite or postgres")
	}
}

func devHeaderAuthEnabled(mode deploymentMode, raw string) (bool, error) {
	value := strings.TrimSpace(raw)
	if value == "" || strings.EqualFold(value, "false") {
		return false, nil
	}
	if !strings.EqualFold(value, "true") {
		return false, fmt.Errorf("HARNESS_DEV_HEADER_AUTH must be true or false")
	}
	if mode == deploymentModeProduction {
		return false, fmt.Errorf("HARNESS_DEV_HEADER_AUTH is forbidden in production mode")
	}
	return true, nil
}

func durationEnv(name string, fallback time.Duration) (time.Duration, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", name)
	}
	return value, nil
}

func intEnv(name string, fallback int) (int, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return value, nil
}

func telemetryEnabled() bool {
	if strings.EqualFold(strings.TrimSpace(os.Getenv("HARNESS_OTEL_ENABLED")), "true") {
		return true
	}
	for _, name := range []string{
		"OTEL_EXPORTER_OTLP_ENDPOINT",
		"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
		"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
	} {
		if strings.TrimSpace(os.Getenv(name)) != "" {
			return true
		}
	}
	return false
}

// configureRunnerTelemetry gives private-runner submissions the same optional
// durable W3C carrier injector as the server runtime. Keeping it separate
// makes the nil (OTel-disabled) path explicit and directly testable.
func configureRunnerTelemetry(hub *runner.Hub, telemetry core.Telemetry) {
	if hub != nil {
		hub.Telemetry = telemetry
	}
}

// configureRagProjectionMaintenance wires the shared SQL RAG index only as a
// projection maintainer. It does not register or mount a RAG capability.
func configureRagProjectionMaintenance(config *server.Config, db *sql.DB, dialect storage.SQLDialect) error {
	if config == nil {
		return fmt.Errorf("rag projection maintenance requires a server config")
	}
	maintainer, err := storage.NewSQLRagIndex(db, dialect)
	if err != nil {
		return err
	}
	return configureProjectionMaintainers(config, maintainer, nil)
}

// configureMemoryProjectionMaintenance wires the shared SQL Memory store only
// as a projection maintainer. It does not register or mount a Memory capability.
func configureMemoryProjectionMaintenance(config *server.Config, db *sql.DB, dialect storage.SQLDialect) error {
	if config == nil {
		return fmt.Errorf("memory projection maintenance requires a server config")
	}
	maintainer, err := storage.NewSQLMemoryStore(db, dialect)
	if err != nil {
		return err
	}
	return configureProjectionMaintainers(config, nil, maintainer)
}

// configureProjectionMaintainers attaches already-constructed durable stores.
// The startup composition calls it with both pointers so the profile tools and
// maintenance projections share the same instance. The single-purpose helpers
// above stay for focused setup tests and callers that install one maintainer.
func configureProjectionMaintainers(config *server.Config, ragIndex *storage.SQLRagIndex, memoryStore *storage.SQLMemoryStore) error {
	if config == nil {
		return fmt.Errorf("projection maintenance requires a server config")
	}
	if ragIndex != nil {
		config.RagProjection = ragIndex
	}
	if memoryStore != nil {
		config.MemoryProjection = memoryStore
	}
	return nil
}

// sessionColdSweeper returns maintenance for the object-layout session store
// only. A FileSessionStore and SQLSessionStore do not expose an ObjectStore
// with the S3SessionStore layout, so they intentionally leave it disabled.
func sessionColdSweeper(sessionObjects storage.ObjectStore) *storage.ColdSweeper {
	if sessionObjects == nil {
		return nil
	}
	return &storage.ColdSweeper{
		Store:     sessionObjects,
		OlderThan: 30 * 24 * time.Hour,
		Prefixes:  []string{"sessions/"},
	}
}

func runnerTokenAuthenticator(token, workerID, capabilityGrant string) (server.Authenticator, error) {
	token = strings.TrimSpace(token)
	workerID = strings.TrimSpace(workerID)
	if token == "" {
		return nil, nil
	}
	if len(token) < 32 || len(token) > 4096 || strings.IndexFunc(token, unicode.IsControl) >= 0 {
		return nil, fmt.Errorf("HARNESS_RUNNER_TOKEN must contain 32-4096 non-control characters")
	}
	if workerID == "" || len(workerID) > runner.MaxWorkerID || strings.IndexFunc(workerID, unicode.IsControl) >= 0 {
		return nil, fmt.Errorf("HARNESS_RUNNER_ID is empty, too long, or contains control characters")
	}
	capabilities, all, err := server.ParseRunnerCapabilitiesGrant(capabilityGrant)
	if err != nil {
		return nil, fmt.Errorf("HARNESS_RUNNER_CAPABILITIES is invalid: %w", err)
	}
	canonicalGrant := strings.Join(capabilities, ",")
	if all {
		canonicalGrant = "*"
	}
	return server.AuthenticatorFunc(func(r *http.Request) (core.Principal, error) {
		presented, found := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !found || len(presented) != len(token) || subtle.ConstantTimeCompare([]byte(presented), []byte(token)) != 1 {
			return core.Principal{}, fmt.Errorf("invalid runner credential")
		}
		return core.Principal{SubjectID: workerID, Attributes: map[string]string{
			"role": "runner", server.RunnerCapabilitiesAttribute: canonicalGrant,
		}}, nil
	}), nil
}
