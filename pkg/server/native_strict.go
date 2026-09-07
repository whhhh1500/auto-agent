package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
)

// NativeStrictServerConfig is the closed, server-owned construction surface
// for native strict Phase 1. It intentionally accepts SQL authority and an
// immutable bootstrap catalogue, rather than caller-owned Core registries or
// runtime extension points. DB must be dedicated to native-static instances;
// sharing its authority with a generic server or any dynamic-control writer
// invalidates the Phase 1 deployment boundary.
type NativeStrictServerConfig struct {
	DB      *sql.DB
	Dialect storage.SQLDialect

	Bootstrap NativeStrictBootstrap

	MaxRequestBody        int64
	MaxWriteDelay         time.Duration
	LeaseTTL              time.Duration
	RunCancelPollInterval time.Duration
	RunStaleAfter         time.Duration
	RunWorkerPollInterval time.Duration
	RunWorkerClaimTTL     time.Duration
	RunWorkerConcurrency  int
	RunWorkerMaxAttempts  int

	Logger    *slog.Logger
	Telemetry core.Telemetry
}

// NativeStrictBootstrap is a static, caller-provided catalogue. The
// constructor copies all declarative values before mounting them; capability
// and model implementations remain trusted in-process provider ports.
type NativeStrictBootstrap struct {
	Revision         string
	Root             []core.ScopeRef
	DefaultProfileID string

	Profiles     []core.AgentProfileLayer
	Policies     []core.PolicyLayer
	Capabilities []NativeStrictCapability
	Model        NativeStrictModel
}

// NativeStrictCapability describes one static capability at its ownership
// scope. Native strict does not accept dynamic factories, plugins, or binders.
type NativeStrictCapability struct {
	Scope      core.ScopePath
	Capability core.Capability
}

// NativeStrictModel fixes the only model selection available to the runtime.
// Adapter configuration and its credentials are deployment bootstrap concerns;
// native strict never mounts a Core credential resolver.
type NativeStrictModel struct {
	Selection core.ModelSelection
	Adapter   core.LlmAdapter
}

type nativeStrictPhase uint8

const nativeStrictPhaseStaticBootstrap nativeStrictPhase = 1

// nativeStrictOwnership is an unexported constructor token. Generic New has
// no way to receive or create it through Config. It proves only server-owned
// static bootstrap and SQL-domain composition. It is not a transaction-level
// authorization grant, and a deployment sharing this SQL authority with a
// generic/dynamic-control writer is outside the native-static contract.
type nativeStrictOwnership struct {
	bootstrapRevision string
	phase             nativeStrictPhase
	db                *sql.DB
	dialect           storage.SQLDialect
}

const nativeStrictBootstrapAttempts = 3

const nativeStrictCandidateShutdownTimeout = 5 * time.Second

var errNativeStrictDynamicControlUnavailable = errors.New("native strict Phase 1 rejects dynamic control")

func (s *Server) rejectNativeStrictDynamicControl(operation string) error {
	if s == nil || s.nativeStrict == nil {
		return nil
	}
	return fmt.Errorf("%w: %s", errNativeStrictDynamicControlUnavailable, operation)
}

// NewNativeStrictServer creates a server that exclusively owns its Runtime,
// Core registries, native SQL execution stores, and execution projection.
// Phase 1 is static-only: it rejects dynamic binding, release, and canary
// projection. Queued completed-tool recovery is limited to this constructor's
// sealed SQL authority and never enables recovery for generic or dynamic
// servers. The configured SQL authority must not be shared with generic
// servers or dynamic-control writers.
func NewNativeStrictServer(ctx context.Context, cfg NativeStrictServerConfig) (*Server, error) {
	return newNativeStrictServer(ctx, cfg, nil)
}

func newNativeStrictServer(ctx context.Context, cfg NativeStrictServerConfig, afterCandidate func(*Server) error) (*Server, error) {
	if ctx == nil {
		return nil, fmt.Errorf("native strict constructor requires a context")
	}
	if cfg.DB == nil {
		return nil, fmt.Errorf("native strict constructor requires a database handle")
	}
	if err := validateNativeStrictBootstrap(cfg.Bootstrap); err != nil {
		return nil, err
	}

	sessions, err := storage.OpenSQLSessionStore(ctx, cfg.DB, cfg.Dialect)
	if err != nil {
		return nil, fmt.Errorf("open native strict session store: %w", err)
	}
	runControl, err := storage.NewSQLRunControlStore(cfg.DB, cfg.Dialect)
	if err != nil {
		return nil, fmt.Errorf("open native strict run control: %w", err)
	}
	toolJournal, err := storage.NewSQLToolInvocationJournal(cfg.DB, cfg.Dialect)
	if err != nil {
		return nil, fmt.Errorf("open native strict tool journal: %w", err)
	}
	accounts, err := storage.NewSQLAccountStore(cfg.DB, cfg.Dialect)
	if err != nil {
		return nil, fmt.Errorf("open native strict account store: %w", err)
	}
	queuedPrincipal, err := storage.NewSQLQueuedPrincipalResolver(accounts, cfg.Bootstrap.Root)
	if err != nil {
		return nil, fmt.Errorf("open native strict queued principal resolver: %w", err)
	}
	approvals, err := storage.NewSQLApprovalStore(cfg.DB, cfg.Dialect)
	if err != nil {
		return nil, fmt.Errorf("open native strict approval store: %w", err)
	}
	if err := storage.ValidateAuthorizationEpochSQLPrincipalAuthority(
		queuedPrincipal, sessions, runControl, sessions, toolJournal,
	); err != nil {
		return nil, fmt.Errorf("validate native strict SQL authority: %w", err)
	}

	for attempt := 0; attempt < nativeStrictBootstrapAttempts; attempt++ {
		before, err := queuedPrincipal.AuthorizationEpoch(ctx)
		if err != nil {
			return nil, fmt.Errorf("read native strict authorization epoch: %w", err)
		}
		if err := storage.VerifyNativeStrictStaticControl(ctx, cfg.DB, cfg.Dialect); err != nil {
			return nil, err
		}

		runtime, err := newNativeStrictRuntime(cfg.Bootstrap, toolJournal, approvals, cfg.Telemetry)
		if err != nil {
			return nil, err
		}
		ownership := &nativeStrictOwnership{
			bootstrapRevision: cfg.Bootstrap.Revision,
			phase:             nativeStrictPhaseStaticBootstrap,
			db:                cfg.DB,
			dialect:           cfg.Dialect,
		}
		server, err := newServer(Config{
			Runtime:       runtime,
			Sessions:      sessions,
			Authenticator: AccountAuthenticator{Store: accounts, Root: cloneNativeStrictRoot(cfg.Bootstrap.Root)},

			DefaultProfileID: cfg.Bootstrap.DefaultProfileID,
			MaxRequestBody:   cfg.MaxRequestBody,
			MaxWriteDelay:    cfg.MaxWriteDelay,

			AuthorizationEpochReader: queuedPrincipal,
			Leaser:                   sessions,
			LeaseTTL:                 cfg.LeaseTTL,
			Accounts:                 accounts,
			RunControl:               runControl,
			RunQueue:                 runControl,
			RunPrincipalResolver:     queuedPrincipal,
			Approvals:                approvals,

			RunCancelPollInterval: cfg.RunCancelPollInterval,
			RunStaleAfter:         cfg.RunStaleAfter,
			RunWorkerPollInterval: cfg.RunWorkerPollInterval,
			RunWorkerClaimTTL:     cfg.RunWorkerClaimTTL,
			RunWorkerConcurrency:  cfg.RunWorkerConcurrency,
			RunWorkerMaxAttempts:  cfg.RunWorkerMaxAttempts,
			Logger:                cfg.Logger,
			Telemetry:             cfg.Telemetry,
		}, ownership)
		if err != nil {
			return nil, fmt.Errorf("construct native strict server: %w", err)
		}
		if afterCandidate != nil {
			if err := afterCandidate(server); err != nil {
				return nil, shutdownNativeStrictCandidate(server, err)
			}
		}

		after, err := queuedPrincipal.AuthorizationEpoch(ctx)
		if err != nil {
			return nil, shutdownNativeStrictCandidate(server, fmt.Errorf("re-read native strict authorization epoch: %w", err))
		}
		if err := storage.VerifyNativeStrictStaticControl(ctx, cfg.DB, cfg.Dialect); err != nil {
			return nil, shutdownNativeStrictCandidate(server, err)
		}
		if before != after {
			if err := shutdownNativeStrictCandidate(server, nil); err != nil {
				return nil, err
			}
			continue
		}
		if err := server.initializeNativeStrictExecutionProjection(after); err != nil {
			return nil, shutdownNativeStrictCandidate(server, err)
		}
		return server, nil
	}
	return nil, fmt.Errorf("native strict authorization control changed during bootstrap")
}

func shutdownNativeStrictCandidate(server *Server, cause error) error {
	ctx, cancel := context.WithTimeout(context.Background(), nativeStrictCandidateShutdownTimeout)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		return errors.Join(cause, fmt.Errorf("shutdown rejected native strict candidate: %w", err))
	}
	return cause
}

func validateNativeStrictBootstrap(bootstrap NativeStrictBootstrap) error {
	if strings.TrimSpace(bootstrap.Revision) == "" {
		return fmt.Errorf("native strict bootstrap revision is required")
	}
	root, err := core.NewScopePath(bootstrap.Root...)
	if err != nil {
		return fmt.Errorf("native strict bootstrap root: %w", err)
	}
	if err := core.ValidateProfileID(bootstrap.DefaultProfileID); err != nil {
		return fmt.Errorf("native strict default profile: %w", err)
	}
	if strings.TrimSpace(bootstrap.Model.Selection.Provider) == "" || strings.TrimSpace(bootstrap.Model.Selection.Model) == "" {
		return fmt.Errorf("native strict model selection is incomplete")
	}
	if bootstrap.Model.Adapter == nil {
		return fmt.Errorf("native strict model adapter is required")
	}
	if bootstrap.Model.Adapter.Provider() != bootstrap.Model.Selection.Provider {
		return fmt.Errorf("native strict model adapter provider does not match selection")
	}
	for index, layer := range bootstrap.Profiles {
		if !root.IsAncestorOf(layer.Scope) {
			return fmt.Errorf("native strict profile layer %d is outside bootstrap root", index)
		}
		if layer.Model != nil && *layer.Model != bootstrap.Model.Selection {
			return fmt.Errorf("native strict profile layer %d selects a different model", index)
		}
	}
	for index, layer := range bootstrap.Policies {
		if !root.IsAncestorOf(layer.Scope) {
			return fmt.Errorf("native strict policy layer %d is outside bootstrap root", index)
		}
	}
	for index, descriptor := range bootstrap.Capabilities {
		if !root.IsAncestorOf(descriptor.Scope) {
			return fmt.Errorf("native strict capability %d is outside bootstrap root", index)
		}
		if descriptor.Capability == nil {
			return fmt.Errorf("native strict capability %d is nil", index)
		}
	}
	return nil
}

func newNativeStrictRuntime(bootstrap NativeStrictBootstrap, journal core.ToolInvocationJournal, approvals core.Approver, telemetry core.Telemetry) (*core.Runtime, error) {
	capabilities := core.NewCapabilityRegistry()
	profiles := core.NewAgentProfileRegistry()
	policies := core.NewPolicyRegistry()
	bootstrapRoot, err := core.NewScopePath(bootstrap.Root...)
	if err != nil {
		return nil, fmt.Errorf("clone native strict bootstrap root: %w", err)
	}
	root, err := cloneNativeStrictScope(bootstrapRoot)
	if err != nil {
		return nil, err
	}
	for index, descriptor := range bootstrap.Capabilities {
		capability, err := freezeNativeStrictCapability(descriptor.Capability)
		if err != nil {
			return nil, fmt.Errorf("freeze native strict capability %d: %w", index, err)
		}
		scope, err := cloneNativeStrictScope(descriptor.Scope)
		if err != nil {
			return nil, err
		}
		if _, err := capabilities.Mount(core.CapabilityBinding{
			Scope: scope, Mode: core.BindingProvide, Manifest: capability.Manifest(), Provider: capability,
		}); err != nil {
			return nil, fmt.Errorf("mount native strict capability %d: %w", index, err)
		}
	}
	type profileTarget struct {
		profileID string
		scope     core.ScopePath
	}
	profileTargets := make([]profileTarget, 0, len(bootstrap.Profiles))
	seenProfileTargets := make(map[string]struct{}, len(bootstrap.Profiles))
	for index, layer := range bootstrap.Profiles {
		mounted := core.CloneAgentProfileLayer(layer)
		if _, err := profiles.Mount(mounted); err != nil {
			return nil, fmt.Errorf("mount native strict profile layer %d: %w", index, err)
		}
		key := mounted.ProfileID + "\x00" + mounted.Scope.String()
		if _, exists := seenProfileTargets[key]; !exists {
			seenProfileTargets[key] = struct{}{}
			profileTargets = append(profileTargets, profileTarget{profileID: mounted.ProfileID, scope: mounted.Scope})
		}
	}
	for index, layer := range bootstrap.Policies {
		if _, err := policies.Mount(cloneNativeStrictPolicy(layer)); err != nil {
			return nil, fmt.Errorf("mount native strict policy layer %d: %w", index, err)
		}
	}
	principal := core.Principal{Scope: root}
	for _, target := range profileTargets {
		profile, err := profiles.Resolve(principal, target.scope, target.profileID)
		if err != nil {
			return nil, fmt.Errorf("resolve native strict profile %q at scope %q: %w", target.profileID, target.scope, err)
		}
		if profile.Model != bootstrap.Model.Selection {
			return nil, fmt.Errorf("native strict profile %q at scope %q does not resolve to the fixed model", target.profileID, target.scope)
		}
	}
	profile, err := profiles.Resolve(principal, root, bootstrap.DefaultProfileID)
	if err != nil {
		return nil, fmt.Errorf("resolve native strict default profile: %w", err)
	}
	if profile.Model != bootstrap.Model.Selection {
		return nil, fmt.Errorf("native strict default profile does not resolve to the fixed model")
	}
	return &core.Runtime{
		Capabilities: capabilities,
		Profiles:     profiles,
		Models: nativeStrictModelResolver{
			selection: bootstrap.Model.Selection,
			adapter:   bootstrap.Model.Adapter,
		},
		Policy:      policies,
		Approver:    approvals,
		ToolJournal: journal,
		Telemetry:   telemetry,
	}, nil
}

type nativeStrictModelResolver struct {
	selection core.ModelSelection
	adapter   core.LlmAdapter
}

func (r nativeStrictModelResolver) ResolveModel(_ context.Context, selection core.ModelSelection) (core.LlmAdapter, error) {
	if selection != r.selection {
		return nil, fmt.Errorf("native strict model selection is not declared by bootstrap")
	}
	return r.adapter, nil
}

type frozenNativeStrictCapability struct {
	manifest core.CapabilityManifest
	provider core.ToolProvider
	revision string
}

func freezeNativeStrictCapability(capability core.Capability) (frozenNativeStrictCapability, error) {
	manifest, err := cloneNativeStrictManifest(capability.Manifest())
	if err != nil {
		return frozenNativeStrictCapability{}, err
	}
	revision := ""
	if provider, ok := capability.(core.ArtifactRevisioner); ok {
		revision = provider.ArtifactRevision()
	}
	return frozenNativeStrictCapability{manifest: manifest, provider: capability, revision: revision}, nil
}

func (c frozenNativeStrictCapability) Manifest() core.CapabilityManifest {
	manifest, err := cloneNativeStrictManifest(c.manifest)
	if err != nil {
		return core.CapabilityManifest{}
	}
	return manifest
}

func (c frozenNativeStrictCapability) Execute(ctx context.Context, request core.CapabilityRequest) (core.CapabilityResult, error) {
	return c.provider.Execute(ctx, request)
}

func (c frozenNativeStrictCapability) ArtifactRevision() string { return c.revision }

func cloneNativeStrictManifest(manifest core.CapabilityManifest) (core.CapabilityManifest, error) {
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return core.CapabilityManifest{}, err
	}
	var copyOf core.CapabilityManifest
	if err := json.Unmarshal(encoded, &copyOf); err != nil {
		return core.CapabilityManifest{}, err
	}
	return copyOf, nil
}

func cloneNativeStrictRoot(root []core.ScopeRef) []core.ScopeRef {
	return append([]core.ScopeRef(nil), root...)
}

func cloneNativeStrictScope(scope core.ScopePath) (core.ScopePath, error) {
	return core.NewScopePath(scope.Segments()...)
}

func cloneNativeStrictPolicy(layer core.PolicyLayer) core.PolicyLayer {
	copyOf := layer
	copyOf.AllowPermissions = append([]core.Permission(nil), layer.AllowPermissions...)
	copyOf.DenyPermissions = append([]core.Permission(nil), layer.DenyPermissions...)
	if layer.MaxSteps != nil {
		value := *layer.MaxSteps
		copyOf.MaxSteps = &value
	}
	if layer.MaxToolCalls != nil {
		value := *layer.MaxToolCalls
		copyOf.MaxToolCalls = &value
	}
	return copyOf
}
