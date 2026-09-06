// Package server exposes the harness runtime through a small HTTP/SSE adapter.
// Authentication, persistence and runtime composition are injected interfaces.
package server

import (
	"context"
	"fmt"
	modelsettingsadapter "github.com/cc-auto-agent/harness-core/pkg/adapter/modelsettings"
	capabilityruntime "github.com/cc-auto-agent/harness-core/pkg/app/capabilityruntime"
	appidentity "github.com/cc-auto-agent/harness-core/pkg/app/identity"
	"github.com/cc-auto-agent/harness-core/pkg/app/modelcatalog"
	appmodelsettings "github.com/cc-auto-agent/harness-core/pkg/app/modelsettings"
	appnotification "github.com/cc-auto-agent/harness-core/pkg/app/notification"
	"github.com/cc-auto-agent/harness-core/pkg/app/runexecutor"
	"github.com/cc-auto-agent/harness-core/pkg/app/runliveness"
	appsettings "github.com/cc-auto-agent/harness-core/pkg/app/settings"
	appstorageconfig "github.com/cc-auto-agent/harness-core/pkg/app/storageconfig"
	"github.com/cc-auto-agent/harness-core/pkg/evaluation"
	executionsandbox "github.com/cc-auto-agent/harness-core/pkg/execution/sandbox"
	"github.com/cc-auto-agent/harness-core/pkg/extensions/runner"
	"github.com/cc-auto-agent/harness-core/pkg/extensions/subagent"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/cc-auto-agent/harness-core/pkg/console"
	"github.com/cc-auto-agent/harness-core/pkg/control"
	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

// Authenticator derives a trusted principal from an HTTP request.
type Authenticator interface {
	Authenticate(r *http.Request) (core.Principal, error)
}

// AuthenticatorFunc adapts a function to Authenticator.
type AuthenticatorFunc func(*http.Request) (core.Principal, error)

func (f AuthenticatorFunc) Authenticate(r *http.Request) (core.Principal, error) {
	return f(r)
}

// RunnerCapabilitiesAttribute is the trusted Principal attribute that limits
// which capability IDs a dedicated private runner may claim. Only a trusted
// RunnerAuthenticator may set it; never accept it from user-controlled
// request metadata. Its value is either "*" or a comma-separated list of
// namespaced capability IDs.
const RunnerCapabilitiesAttribute = "runner.capabilities"

const MaxActiveRuns = 1024

// RunPrincipalResolver refreshes an asynchronous run's principal at claim
// time, so queued work observes account disablement and grant changes.
type RunPrincipalResolver interface {
	ResolveRunPrincipal(ctx context.Context, tenantID, subjectID string) (core.Principal, error)
}

type RunPrincipalResolverFunc func(context.Context, string, string) (core.Principal, error)

func (f RunPrincipalResolverFunc) ResolveRunPrincipal(ctx context.Context, tenantID, subjectID string) (core.Principal, error) {
	return f(ctx, tenantID, subjectID)
}

// Config contains all infrastructure required by the transport adapter.
type Config struct {
	Runtime       *core.Runtime
	Sessions      core.SessionStore
	Authenticator Authenticator
	// RunExecutors selects the immutable run orchestration registry. When nil,
	// New installs the built-in sequential registry.
	RunExecutors *runexecutor.Registry
	// CapabilityRuntimeFactories extends the built-in dynamic capability runtimes.
	// Factories are validated once at startup and the resulting registry is read-only.
	CapabilityRuntimeFactories []capabilityruntime.Factory
	// DefaultProfileID is used for session creation only when the request does
	// not provide profile_id. An explicit request value always wins, including
	// an unknown value (it is never silently replaced by this default).
	DefaultProfileID string
	MaxRequestBody   int64
	// MaxWriteDelay bounds the write-behind durability window during a run.
	// Zero uses the 200ms default; a negative value disables batching and
	// saves once at run end (the legacy behavior).
	MaxWriteDelay time.Duration
	// Releases is the optional publish/rollback surface for profiles. When
	// nil, release routes answer 501 — transport-only deployments skip them.
	Releases *control.ReleaseManager
	// Canaries is the optional durable staged-rollout surface. It must share
	// the configured release manager and runtime profile registry.
	Canaries *control.CanaryManager
	// Leaser is the optional cross-instance session lease. When set, a run
	// only starts after acquiring the session's lease; a session already
	// leased by another instance answers 409. Long runs renew the lease every
	// LeaseTTL/3 and are cancelled if ownership is lost. LeaseTTL defaults to
	// 60s.
	Leaser   storage.SessionLeaser
	LeaseTTL time.Duration
	// Runners is the optional private-runner surface. When nil, runner
	// routes answer 501.
	Runners *runner.Hub
	// RunnerRecoveryInterval controls how often expired private-runner task
	// claims are recovered while the service is online. Zero derives a bounded
	// default from Runners.LeaseTTL; it is zero when Runners is nil.
	RunnerRecoveryInterval time.Duration
	// RunnerAuthenticator authenticates private runner workers independently
	// from end users. When nil, runner routes require the normal principal to
	// carry the platform-admin role.
	RunnerAuthenticator Authenticator
	// RagProjection is the optional global maintenance surface for derived RAG
	// token/tag projections. When nil, projection administration routes answer
	// 501. It is maintenance-only and does not mount a RAG capability.
	RagProjection storage.RagProjectionMaintainer
	// MemoryProjection is the optional global maintenance surface for derived
	// Memory search/tag projections. When nil, projection administration routes
	// answer 501. It is maintenance-only and does not mount a Memory capability.
	MemoryProjection storage.MemoryProjectionMaintainer
	// Accounts is the optional built-in account store (first-boot admin,
	// bearer login). When nil, /v1/auth/* answer 501.
	Accounts storage.AccountStore
	// IdentityUseCases is the optional application identity service surface.
	// When omitted with Accounts set, New wires the legacy account facade
	// through its private compatibility glue. Deployments that need a custom
	// throttle construct identity.Service and inject this interface directly.
	IdentityUseCases appidentity.UseCases
	// AccountAdminUseCases is the optional application surface for account and
	// tenant administration. It is deliberately separate from IdentityUseCases
	// so login/activation implementations remain source-compatible. When
	// omitted with Accounts set, New wires a private legacy-store adapter.
	AccountAdminUseCases appidentity.AdminUseCases
	// SettingsRepository is the optional persistence port for deployment
	// settings. When omitted with Accounts set, New uses Accounts as a
	// compatibility fallback. It is also used by the storage settings routes,
	// whose secret merge must see raw persisted values.
	SettingsRepository appsettings.Repository
	// SettingsUseCases is the optional application settings surface. When
	// omitted with a repository, New constructs the built-in application
	// service. An explicit use case implementation takes precedence.
	SettingsUseCases appsettings.UseCases
	// ModelSettingsRepository and ModelSettingsUseCases own the dedicated,
	// presence-aware legacy LLM configuration route. They are intentionally not
	// derived from generic settings: composition supplies the semantic adapter.
	ModelSettingsRepository appmodelsettings.Repository
	// ModelCatalog is the safe view of the same immutable model runtime
	// registry used by the compiler. It never exposes factories or secrets.
	ModelCatalog          modelcatalog.View
	ModelSettingsUseCases appmodelsettings.UseCases
	// StorageConfigUseCases is the optional typed, database-authoritative
	// storage configuration surface. When nil, storage routes retain their
	// legacy SettingsRepository handler path for compatibility.
	StorageConfigUseCases appstorageconfig.UseCases
	// Resources is the optional resource object store (S3 or the embedded
	// filesystem). When nil, /v1/resources answer 501. Pass a
	// *DynamicObjectStore to allow runtime re-pointing from the console.
	Resources storage.ObjectStore
	// EmbeddedResources is the fallback resource store for the console's
	// dynamic swap back to the embedded filesystem. Optional.
	EmbeddedResources storage.ObjectStore
	// MaxResourceBytes caps streaming resource uploads. The default allows the
	// large-object path; byte-buffer compatibility fallbacks remain capped at
	// storage.MaxObjectBytes.
	MaxResourceBytes int64
	// TokenTTL bounds issued bearer tokens; default 24h, clamped to
	// storage.MaxTokenTTL (30 days).
	TokenTTL time.Duration
	// Retention, when set, receives a prune facade for bounded tables.
	Retention storage.RetentionPruner
	// Audit durably records administrative actions. When nil, audit events
	// are only written to the log.
	Audit storage.AuditStore
	// LibraryObserver is the optional disclosure-flow log (search/explore/choose).
	LibraryObserver *storage.SQLLibraryObserver
	// Obs is the optional observability store backing watch rules and their
	// recorded hits. When nil, observability matching is disabled.
	Obs storage.ObsStore
	// BindingJournal makes console-applied dynamic bindings survive restarts.
	// When nil, dynamic bindings are process-local.
	BindingJournal storage.BindingJournal
	// RunStats records finished-run outcomes for the metrics endpoint. When
	// nil, metrics are unavailable.
	RunStats storage.RunStatsStore
	// RunControl persists live/terminal run status and cross-instance cancel
	// requests. When nil, status remains request-local for compatibility.
	RunControl storage.RunControlStore
	// RunQueue enables durable asynchronous submission and worker claims. A
	// RunQueueStore also serves as RunControl when RunControl is nil.
	RunQueue             storage.RunQueueStore
	RunPrincipalResolver RunPrincipalResolver
	// Approvals exposes durable pending approval queries and decisions. When
	// Runtime.Approver is durable this and RunQueue are required.
	Approvals storage.ApprovalStore
	// NotificationTargets exposes tenant-scoped, non-secret target management.
	// When nil, notification target administration answers 501.
	NotificationTargets *appnotification.Service
	// SandboxProviders is an immutable discovery registry supplied by the
	// composition root. The transport reads it only; it never creates a host
	// execution fallback or starts sandbox sessions.
	SandboxProviders *executionsandbox.Registry
	// DelegationLinks is the optional metadata-only parent/child link store.
	// DelegationLinkCatalog may be the same implementation and backs the
	// tenant-scoped administration list route.
	DelegationLinks       subagent.DelegationLinkStore
	DelegationLinkCatalog subagent.DelegationLinkCatalog
	// Evaluations stores immutable datasets and durable regression results.
	// EvaluationEvaluators optionally replaces or extends built-in assertions.
	Evaluations evaluation.Store
	// Evidence is the optional indexed cross-object Composition/Assignment
	// query surface. When nil, the evidence route answers 501.
	Evidence             storage.EvidenceStore
	EvaluationEvaluators *evaluation.Registry
	// RunCancelPollInterval controls how quickly a worker observes a cancel
	// request written by another instance. Zero defaults to 500ms.
	RunCancelPollInterval time.Duration
	// RunStaleAfter controls startup recovery of running records without a
	// heartbeat. Zero defaults to two minutes.
	RunStaleAfter time.Duration
	// RunWorkerPollInterval controls empty-queue polling. Zero defaults to 250ms.
	RunWorkerPollInterval time.Duration
	// RunWorkerClaimTTL is the durable worker-claim lease. Zero defaults to 30s.
	RunWorkerClaimTTL time.Duration
	// RunWorkerConcurrency controls local asynchronous worker slots. Zero uses 1.
	RunWorkerConcurrency int
	// RunWorkerMaxAttempts is stamped onto new async requests. Zero uses 3.
	RunWorkerMaxAttempts int
	// Logger receives server logs. Request and run-path calls use slog's
	// Context methods so handlers can correlate records with OTel spans. nil
	// disables access logging.
	Logger *slog.Logger
	// Telemetry receives bounded runtime and infrastructure observations.
	// Optional; implementations are isolated from request semantics.
	Telemetry core.Telemetry
}

// Server is a transport adapter; it contains no product capabilities.
type Server struct {
	runtime            *core.Runtime
	sessions           core.SessionStore
	authenticator      Authenticator
	defaultProfileID   string
	maxBody            int64
	maxWriteDelay      time.Duration
	runExecutors       *runexecutor.Registry
	capabilityRuntimes *capabilityruntime.Registry
	// Releases is the optional publish/rollback surface for profiles. When
	// nil, release routes answer 501 — transport-only deployments skip them.
	Releases                *control.ReleaseManager
	canaries                *control.CanaryManager
	leaser                  storage.SessionLeaser
	liveness                *runliveness.Scheduler
	leaseTTL                time.Duration
	leaseOpTimeout          time.Duration
	instanceID              string
	runners                 *runner.Hub
	runnerAuth              Authenticator
	runnerRecoveryInterval  time.Duration
	newRunnerRecoveryTicker leaseTickerFactory
	ragProjection           storage.RagProjectionMaintainer
	memoryProjection        storage.MemoryProjectionMaintainer

	accounts                 storage.AccountStore
	identityUseCases         appidentity.UseCases
	accountAdminUseCases     appidentity.AdminUseCases
	settingsRepository       appsettings.Repository
	settingsUseCases         appsettings.UseCases
	modelSettingsUseCases    appmodelsettings.UseCases
	modelCatalog             modelcatalog.View
	storageConfigUseCases    appstorageconfig.UseCases
	resources                storage.ObjectStore
	maxResourceBytes         int64
	maxResourceFallbackBytes int64
	tokenTTL                 time.Duration
	audit                    storage.AuditStore
	retention                storage.RetentionPruner
	journal                  storage.BindingJournal
	runStats                 storage.RunStatsStore
	runControl               storage.RunControlStore
	runQueue                 storage.RunQueueStore
	runPrincipal             RunPrincipalResolver
	approvals                storage.ApprovalStore
	notificationTargets      *appnotification.Service
	sandboxProviders         *executionsandbox.Registry
	delegationLinks          subagent.DelegationLinkStore
	delegationCatalog        subagent.DelegationLinkCatalog
	evaluations              evaluation.Store
	evidence                 storage.EvidenceStore
	evaluationRunner         *evaluation.Runner
	runCancelPoll            time.Duration
	runStaleAfter            time.Duration
	runWorkerPoll            time.Duration
	runWorkerClaimTTL        time.Duration
	runWorkerCount           int
	runWorkerAttempts        int
	embeddedResources        storage.ObjectStore
	obs                      storage.ObsStore
	libraryObserver          *storage.SQLLibraryObserver
	obsMatcher               *obsMatcher
	logger                   *slog.Logger
	telemetry                core.Telemetry
	adminOnce                sync.Once
	admin                    *adminState
	console                  http.Handler
	// profileMu serializes durable profile layer replacement. Readers may
	// resolve concurrently; each PUT has one journal commit boundary.
	profileMu sync.Mutex

	locks *storage.NamedLocks

	runsMu sync.Mutex
	// runs tracks one cancellable context per session with an active run so a
	// client can cancel long-running work. Sessions run one at a time.
	runs          map[string]*activeRun
	maxActiveRuns int

	readyMu  sync.RWMutex
	readyErr error

	workersMu           sync.Mutex
	workersRunning      bool
	workersStopClaims   context.CancelFunc
	workersCancelActive context.CancelFunc
	workersDone         chan struct{}
	workersWG           sync.WaitGroup

	runnerRecoveryMu      sync.Mutex
	runnerRecoveryRunning bool
	runnerRecoveryStop    context.CancelFunc
	runnerRecoveryDone    chan struct{}
}

type activeRun struct {
	runID  string
	cancel context.CancelFunc
}

var terminalPersistenceTimeout = 15 * time.Second

const telemetryCanaryID = "harness.canary.id"

const (
	compositionCanaryID           = "harness.canary.id"
	compositionCanaryRevision     = "harness.canary.revision"
	compositionCanaryBaseRevision = "harness.canary.base_release_revision"
	compositionCanaryBasisPoints  = "harness.canary.basis_points"
	compositionCanaryBucket       = "harness.canary.bucket"
	compositionCanaryStatus       = "harness.canary.status"
	compositionCanaryCandidate    = "harness.canary.candidate"
	compositionAssignmentVariant  = "harness.assignment.variant"
)

// New validates dependencies and creates a server.
func New(config Config) (*Server, error) {
	if config.Runtime == nil || config.Sessions == nil || config.Authenticator == nil {
		return nil, fmt.Errorf("server dependencies are incomplete")
	}
	if config.RunExecutors == nil {
		registry, err := runexecutor.NewDefaultRegistry()
		if err != nil {
			return nil, fmt.Errorf("default run executor registry: %w", err)
		}
		config.RunExecutors = registry
	}
	evidenceStore := config.Evidence
	if evidenceStore == nil {
		if discovered, ok := config.Sessions.(storage.EvidenceStore); ok {
			evidenceStore = discovered
		}
	}
	if config.Canaries != nil && (config.Releases == nil || config.Canaries.Releases != config.Releases || config.Runtime.Profiles != config.Releases.Profiles) {
		return nil, fmt.Errorf("canary manager must share the server release manager and runtime profiles")
	}
	runnerRecoveryInterval := config.RunnerRecoveryInterval
	if config.Runners == nil {
		runnerRecoveryInterval = 0
	} else {
		if config.Runners.Store == nil {
			return nil, fmt.Errorf("runner hub requires a task store")
		}
		if runnerRecoveryInterval == 0 {
			runnerRecoveryInterval = DefaultRunnerRecoveryInterval(config.Runners)
		}
		if runnerRecoveryInterval <= 0 || runnerRecoveryInterval > config.Runners.LeaseTTL {
			return nil, fmt.Errorf("runner recovery interval must be positive and no greater than the runner lease ttl")
		}
	}
	if config.MaxRequestBody <= 0 {
		config.MaxRequestBody = 1 << 20
	}
	writeDelay := config.MaxWriteDelay
	if writeDelay == 0 {
		writeDelay = 200 * time.Millisecond
	}
	leaseTTL := config.LeaseTTL
	if leaseTTL <= 0 {
		leaseTTL = 60 * time.Second
	}
	runControl := config.RunControl
	if runControl == nil && config.RunQueue != nil {
		runControl = config.RunQueue
	}
	instanceID := ""
	if config.Leaser != nil || config.RunQueue != nil {
		var err error
		instanceID, err = newInstanceUUID()
		if err != nil {
			return nil, fmt.Errorf("generate server instance id: %w", err)
		}
	}
	tokenTTL := config.TokenTTL
	if tokenTTL <= 0 {
		tokenTTL = 24 * time.Hour
	}
	if tokenTTL > storage.MaxTokenTTL {
		tokenTTL = storage.MaxTokenTTL
	}
	identityUseCases := config.IdentityUseCases
	if identityUseCases == nil && config.Accounts != nil {
		credentials, err := newLegacyIdentityCredentials(config.Accounts)
		if err != nil {
			return nil, err
		}
		identityUseCases, err = appidentity.NewService(credentials, newLoginLimiter(), tokenTTL)
		if err != nil {
			return nil, err
		}
	}
	accountAdminUseCases := config.AccountAdminUseCases
	if accountAdminUseCases == nil && config.Accounts != nil {
		repository, err := newLegacyIdentityAdminRepository(config.Accounts)
		if err != nil {
			return nil, err
		}
		accountAdminUseCases, err = appidentity.NewAdminService(repository)
		if err != nil {
			return nil, err
		}
	}
	settingsRepository := config.SettingsRepository
	if settingsRepository == nil && config.Accounts != nil {
		settingsRepository = config.Accounts
	}
	settingsUseCases := config.SettingsUseCases
	if settingsUseCases == nil && settingsRepository != nil {
		service, err := appsettings.NewService(settingsRepository)
		if err != nil {
			return nil, err
		}
		settingsUseCases = service
	}
	modelSettingsRepository := config.ModelSettingsRepository
	if modelSettingsRepository == nil && settingsRepository != nil {
		var err error
		modelSettingsRepository, err = modelsettingsadapter.New(settingsRepository)
		if err != nil {
			return nil, err
		}
	}
	modelSettingsUseCases := config.ModelSettingsUseCases
	if modelSettingsUseCases == nil && modelSettingsRepository != nil {
		var service *appmodelsettings.Service
		var err error
		if config.ModelCatalog != nil {
			service, err = appmodelsettings.NewServiceWithCatalog(modelSettingsRepository, config.ModelCatalog)
		} else {
			service, err = appmodelsettings.NewService(modelSettingsRepository)
		}
		if err != nil {
			return nil, err
		}
		modelSettingsUseCases = service
	}
	maxResourceBytes := config.MaxResourceBytes
	if maxResourceBytes <= 0 {
		maxResourceBytes = storage.MaxStreamingObjectBytes
	}
	if maxResourceBytes > storage.MaxStreamingObjectBytes {
		maxResourceBytes = storage.MaxStreamingObjectBytes
	}
	maxResourceFallbackBytes := maxResourceBytes
	if maxResourceFallbackBytes > storage.MaxObjectBytes {
		maxResourceFallbackBytes = storage.MaxObjectBytes
	}
	runCancelPoll := config.RunCancelPollInterval
	if runCancelPoll <= 0 {
		runCancelPoll = 500 * time.Millisecond
	}
	runStaleAfter := config.RunStaleAfter
	if runStaleAfter <= 0 {
		runStaleAfter = 2 * time.Minute
	}
	if config.RunControl != nil && runStaleAfter < 3*runCancelPoll {
		return nil, fmt.Errorf("run stale threshold must be at least three times the cancel poll interval")
	}
	runWorkerPoll := config.RunWorkerPollInterval
	if runWorkerPoll <= 0 {
		runWorkerPoll = 250 * time.Millisecond
	}
	runWorkerClaimTTL := config.RunWorkerClaimTTL
	if runWorkerClaimTTL <= 0 {
		runWorkerClaimTTL = 30 * time.Second
	}
	if config.RunQueue != nil && runWorkerClaimTTL < 3*runWorkerPoll {
		return nil, fmt.Errorf("run worker claim ttl must be at least three times the worker poll interval")
	}
	if config.RunQueue != nil && config.RunPrincipalResolver == nil {
		return nil, fmt.Errorf("run queue requires a current principal resolver")
	}
	if _, durable := config.Runtime.Approver.(core.DurableApprover); durable {
		if config.Approvals == nil || config.RunQueue == nil {
			return nil, fmt.Errorf("durable approval requires approval storage and a durable run queue")
		}
	}
	var evaluationRunner *evaluation.Runner
	if config.Evaluations != nil {
		if config.RunPrincipalResolver == nil {
			return nil, fmt.Errorf("evaluation requires a current principal resolver")
		}
		evaluators := config.EvaluationEvaluators
		if evaluators == nil {
			var err error
			evaluators, err = evaluation.NewRegistry()
			if err != nil {
				return nil, err
			}
		}
		evaluationRunner = &evaluation.Runner{
			Runtime: config.Runtime, Sessions: config.Sessions, Store: config.Evaluations, Evaluators: evaluators,
		}
	}
	runWorkerCount := config.RunWorkerConcurrency
	if runWorkerCount == 0 {
		runWorkerCount = 1
	}
	if runWorkerCount < 0 || runWorkerCount > 64 {
		return nil, fmt.Errorf("run worker concurrency must be between 1 and 64")
	}
	runWorkerAttempts := config.RunWorkerMaxAttempts
	if runWorkerAttempts == 0 {
		runWorkerAttempts = storage.DefaultRunMaxAttempts
	}
	if runWorkerAttempts < 1 || runWorkerAttempts > storage.HardRunMaxAttempts {
		return nil, fmt.Errorf("run worker max attempts must be between 1 and %d", storage.HardRunMaxAttempts)
	}
	consoleHandler := console.Handler()
	delegationCatalog := config.DelegationLinkCatalog
	if delegationCatalog == nil && config.DelegationLinks != nil {
		if catalog, ok := config.DelegationLinks.(subagent.DelegationLinkCatalog); ok {
			delegationCatalog = catalog
		}
	}
	server := &Server{
		runtime: config.Runtime, sessions: config.Sessions, authenticator: config.Authenticator,
		defaultProfileID: config.DefaultProfileID,
		maxBody:          config.MaxRequestBody, maxWriteDelay: writeDelay,
		runExecutors: config.RunExecutors,
		Releases:     config.Releases, canaries: config.Canaries,
		leaser: config.Leaser, leaseTTL: leaseTTL,
		leaseOpTimeout: defaultLeaseOperationTimeout(leaseTTL),
		instanceID:     instanceID,
		runners:        config.Runners, runnerAuth: config.RunnerAuthenticator,
		runnerRecoveryInterval: runnerRecoveryInterval, newRunnerRecoveryTicker: realLeaseTickerFactory,
		ragProjection:    config.RagProjection,
		memoryProjection: config.MemoryProjection,
		locks:            storage.NewNamedLocks(), runs: map[string]*activeRun{},
		console:  consoleHandler,
		accounts: config.Accounts, identityUseCases: identityUseCases, accountAdminUseCases: accountAdminUseCases,
		settingsRepository: settingsRepository, settingsUseCases: settingsUseCases,
		modelSettingsUseCases: modelSettingsUseCases,
		modelCatalog:          config.ModelCatalog,
		storageConfigUseCases: config.StorageConfigUseCases,
		resources:             config.Resources, embeddedResources: config.EmbeddedResources,
		maxResourceBytes: maxResourceBytes, maxResourceFallbackBytes: maxResourceFallbackBytes, tokenTTL: tokenTTL,
		audit: config.Audit, retention: config.Retention,
		journal: config.BindingJournal, runStats: config.RunStats,
		runControl: runControl, runQueue: config.RunQueue, runPrincipal: config.RunPrincipalResolver, approvals: config.Approvals,
		notificationTargets: config.NotificationTargets,
		sandboxProviders:    config.SandboxProviders,
		delegationLinks:     config.DelegationLinks, delegationCatalog: delegationCatalog,
		evaluations: config.Evaluations, evaluationRunner: evaluationRunner,
		evidence:      evidenceStore,
		runCancelPoll: runCancelPoll, runStaleAfter: runStaleAfter,
		runWorkerPoll: runWorkerPoll, runWorkerClaimTTL: runWorkerClaimTTL,
		runWorkerCount: runWorkerCount, runWorkerAttempts: runWorkerAttempts,
		obs: config.Obs, libraryObserver: config.LibraryObserver, logger: config.Logger, telemetry: config.Telemetry,
	}
	factories := append([]capabilityruntime.Factory{}, config.CapabilityRuntimeFactories...)
	factories = append(factories, server.builtinCapabilityRuntimeFactories()...)
	capabilityRuntimes, err := capabilityruntime.New(32, factories...)
	if err != nil {
		return nil, fmt.Errorf("capability runtime registry: %w", err)
	}
	server.capabilityRuntimes = capabilityRuntimes
	liveness, err := runliveness.New(runliveness.Config{})
	if err != nil {
		return nil, err
	}
	server.liveness = liveness
	if config.Obs != nil {
		server.obsMatcher = newObsMatcher(config.Obs)
	}
	return server, nil
}

// Handler returns the complete public HTTP surface.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /readyz", s.handleReady)
	mux.HandleFunc("POST /v1/sessions", s.handleCreateSession)
	mux.HandleFunc("GET /v1/sessions/{id}", s.handleGetSession)
	mux.HandleFunc("GET /v1/sessions/{id}/events", s.handleEvents)
	mux.HandleFunc("POST /v1/sessions/{id}/runs", s.handleRun)
	mux.HandleFunc("POST /v1/sessions/{id}/runs/async", s.handleEnqueueRun)
	mux.HandleFunc("GET /v1/sessions/{id}/runs", s.handleListRuns)
	mux.HandleFunc("GET /v1/sessions/{id}/runs/{runID}", s.handleGetRun)
	mux.HandleFunc("POST /v1/sessions/{id}/runs/{runID}/cancel", s.handleCancelRun)
	mux.HandleFunc("POST /v1/sessions/{id}/cancel", s.handleCancel)
	mux.HandleFunc("GET /v1/sessions", s.handleListSessions)
	mux.HandleFunc("GET /v1/admin/model-runtimes", s.handleAdminModelRuntimes)
	mux.HandleFunc("GET /v1/profiles/{id}/capabilities", s.handleProfileCapabilities)
	mux.HandleFunc("GET /v1/profiles", s.handleListProfiles)
	mux.HandleFunc("GET /v1/capabilities", s.handleListCapabilities)
	mux.HandleFunc("POST /v1/profiles/{id}/publish", s.handlePublishProfile)
	mux.HandleFunc("GET /v1/profiles/{id}/releases", s.handleReleaseHistory)
	mux.HandleFunc("GET /v1/profiles/{id}/releases/{version}", s.handleGetRelease)
	mux.HandleFunc("POST /v1/profiles/{id}/rollback", s.handleRollbackProfile)
	mux.HandleFunc("POST /v1/profiles/{id}/canaries", s.handleStageCanary)
	mux.HandleFunc("GET /v1/profiles/{id}/canaries", s.handleListCanaries)
	mux.HandleFunc("GET /v1/profiles/{id}/canaries/{canaryID}", s.handleGetCanary)
	mux.HandleFunc("POST /v1/profiles/{id}/canaries/{canaryID}/percentage", s.handleCanaryPercentage)
	mux.HandleFunc("POST /v1/profiles/{id}/canaries/{canaryID}/pause", s.handlePauseCanary)
	mux.HandleFunc("POST /v1/profiles/{id}/canaries/{canaryID}/resume", s.handleResumeCanary)
	mux.HandleFunc("POST /v1/profiles/{id}/canaries/{canaryID}/rollback", s.handleRollbackCanary)
	mux.HandleFunc("POST /v1/profiles/{id}/canaries/{canaryID}/promote", s.handlePromoteCanary)
	mux.HandleFunc("GET /v1/admin/evidence", s.handleEvidenceQuery)
	mux.HandleFunc("GET /v1/admin/evidence/stats", s.handleEvidenceStats)
	mux.HandleFunc("GET /v1/admin/evidence/runs/{runID}", s.handleEvidenceRunDetail)
	mux.HandleFunc("GET /v1/admin/runners/tasks", s.handleAdminRunnerTaskList)
	mux.HandleFunc("GET /v1/admin/delegations", s.handleAdminDelegations)
	mux.HandleFunc("GET /v1/admin/runners/tasks/{taskID}", s.handleAdminRunnerTaskDetail)
	mux.HandleFunc("POST /v1/admin/runners/tasks/{taskID}/cancel", s.handleAdminRunnerTaskCancel)
	mux.HandleFunc("POST /v1/admin/runners/tasks/{taskID}/retry", s.handleAdminRunnerTaskRetry)
	mux.HandleFunc("POST /v1/admin/runners/recover", s.handleAdminRunnerRecover)
	mux.HandleFunc("GET /v1/admin/rag/projection", s.handleAdminRagProjectionStats)
	mux.HandleFunc("POST /v1/admin/rag/projection/rebuild", s.handleAdminRagProjectionRebuild)
	mux.HandleFunc("GET /v1/admin/memory/projection", s.handleAdminMemoryProjectionStats)
	mux.HandleFunc("POST /v1/admin/memory/projection/rebuild", s.handleAdminMemoryProjectionRebuild)
	mux.HandleFunc("POST /v1/runners/claim", s.handleRunnerClaim)
	mux.HandleFunc("POST /v1/runners/tasks/{id}/renew", s.handleRunnerRenew)
	mux.HandleFunc("POST /v1/runners/tasks/{id}/complete", s.handleRunnerComplete)
	s.registerAdminRoutes(mux)
	s.registerProfileAdminRoutes(mux)
	s.registerControlRoutes(mux)
	s.registerObsRoutes(mux)
	s.registerStorageRoutes(mux)
	s.registerApprovalRoutes(mux)
	s.registerEvaluationRoutes(mux)
	s.registerNotificationTargetRoutes(mux)
	s.registerSandboxRoutes(mux)
	mux.HandleFunc("GET /console", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/console/", http.StatusMovedPermanently)
	})
	mux.Handle("GET /console/", http.StripPrefix("/console", s.console))
	return s.accessLog(recoverMiddleware(s.logger, mux))
}

// Shutdown first stops new queue claims while keeping liveness renewals active
// for work already draining. Once workers leave, it closes the shared
// scheduler. A caller deadline is an explicit forced-shutdown boundary: active
// workers are cancelled before the scheduler is stopped.
func (s *Server) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("shutdown context is nil")
	}
	s.workersMu.Lock()
	if !s.workersRunning {
		s.workersMu.Unlock()
		if s.liveness != nil {
			return s.liveness.Close(ctx)
		}
		return nil
	}
	stopClaims := s.workersStopClaims
	cancelActive := s.workersCancelActive
	done := s.workersDone
	s.workersMu.Unlock()
	if stopClaims != nil {
		stopClaims()
	}
	select {
	case <-done:
		if s.liveness != nil {
			return s.liveness.Close(ctx)
		}
		return nil
	case <-ctx.Done():
		if cancelActive != nil {
			cancelActive()
		}
		if s.liveness != nil {
			// Close still broadcasts cancellation even though the caller deadline
			// prevents waiting for its bounded workers.
			_ = s.liveness.Close(ctx)
		}
		return ctx.Err()
	}
}
