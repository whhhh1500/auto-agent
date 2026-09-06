package sandbox

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	MaxRegistrations               = 64
	MaxVersionBytes                = MaxIdentifierBytes
	MaxImplementationRevisionBytes = MaxIdentifierBytes
	sessionCloseTimeout            = 5 * time.Second
)

var (
	ErrRegistryCapacity        = errors.New("sandbox provider registry capacity exceeded")
	ErrInvalidRegistration     = errors.New("invalid sandbox provider registration")
	ErrProviderNotFound        = errors.New("sandbox provider not found")
	ErrProviderVersionMismatch = errors.New("sandbox provider version mismatch")
	ErrProviderIdentity        = errors.New("sandbox provider identity mismatch")
	ErrProviderPanic           = errors.New("sandbox provider callback panicked")
	ErrProviderFailure         = errors.New("sandbox provider failed")
)

// Metadata is stable, non-secret provider information. Assurance describes
// the implementation's advertised class; Start still verifies the live Probe
// report against each requested SessionSpec.
type Metadata struct {
	ID                     ProviderID `json:"id"`
	Version                string     `json:"version"`
	ImplementationRevision string     `json:"implementation_revision"`
	Assurance              Assurance  `json:"assurance"`
}

// Registration binds one exact provider implementation to immutable metadata.
type Registration struct {
	Metadata Metadata
	Provider Provider
}

type Registry struct {
	providers map[string]Registration
	byID      map[ProviderID]struct{}
	metadata  []Metadata
}

// NewRegistry constructs an immutable exact-version registry. It has no
// package-global state, goroutines, session cache, or credential storage.
func NewRegistry(limit int, registrations ...Registration) (*Registry, error) {
	if limit <= 0 || limit > MaxRegistrations || len(registrations) > limit {
		return nil, ErrRegistryCapacity
	}
	providers := make(map[string]Registration, len(registrations))
	byID := make(map[ProviderID]struct{}, len(registrations))
	metadata := make([]Metadata, 0, len(registrations))
	for _, registration := range registrations {
		if registration.Provider == nil {
			return nil, ErrInvalidRegistration
		}
		if err := validateMetadata(registration.Metadata); err != nil {
			return nil, err
		}
		providerID, err := safeProviderID(registration.Provider)
		if err != nil {
			return nil, err
		}
		if providerID != registration.Metadata.ID {
			return nil, ErrProviderIdentity
		}
		key := providerKey(registration.Metadata.ID, registration.Metadata.Version)
		if _, exists := providers[key]; exists {
			return nil, ErrInvalidRegistration
		}
		byID[registration.Metadata.ID] = struct{}{}
		providers[key] = Registration{Metadata: registration.Metadata, Provider: registration.Provider}
		metadata = append(metadata, registration.Metadata)
	}
	sort.Slice(metadata, func(i, j int) bool {
		if metadata[i].ID != metadata[j].ID {
			return metadata[i].ID < metadata[j].ID
		}
		return metadata[i].Version < metadata[j].Version
	})
	return &Registry{providers: providers, byID: byID, metadata: metadata}, nil
}

// Metadata returns a defensive copy in stable ID/version order.
func (registry *Registry) Metadata() []Metadata {
	if registry == nil {
		return nil
	}
	return append([]Metadata(nil), registry.metadata...)
}

// Resolve selects one exact provider version.
func (registry *Registry) Resolve(id ProviderID, version string) (Provider, error) {
	if registry == nil {
		return nil, ErrProviderNotFound
	}
	if err := validateRegistryToken(string(id), MaxIdentifierBytes); err != nil {
		return nil, ErrProviderNotFound
	}
	if err := validateRegistryToken(version, MaxVersionBytes); err != nil {
		return nil, ErrProviderVersionMismatch
	}
	if _, exists := registry.byID[id]; !exists {
		return nil, ErrProviderNotFound
	}
	registration, exists := registry.providers[providerKey(id, version)]
	if !exists {
		return nil, ErrProviderVersionMismatch
	}
	return registration.Provider, nil
}

// Probe executes one provider probe with panic and report sanitization at the
// registry boundary. Provider unavailable causes are never exposed verbatim.
func (registry *Registry) Probe(ctx context.Context, id ProviderID, version string) (report AssuranceReport, err error) {
	if ctx == nil {
		return AssuranceReport{}, context.Canceled
	}
	if err := ctx.Err(); err != nil {
		return AssuranceReport{}, err
	}
	provider, err := registry.Resolve(id, version)
	if err != nil {
		return AssuranceReport{}, err
	}
	return safeProviderProbe(provider, ctx)
}

// Start validates the complete request and live probe before invoking a
// provider. A provider cannot bypass assurance, network, mount, or limit
// requirements by returning a session directly.
func (registry *Registry) Start(ctx context.Context, id ProviderID, version string, spec SessionSpec) (session Session, err error) {
	session, _, err = registry.StartWithOwnership(ctx, id, version, spec)
	return session, err
}

// StartWithOwnership is the Start form for trusted lifecycle coordinators. The
// returned ownership fact is false until the registry has completed request
// validation and the one live Probe. A RootOwnershipStarter then returns the
// ownership fact from the exact Start invocation; ordinary providers always
// return false and retain adapter-managed cleanup.
func (registry *Registry) StartWithOwnership(ctx context.Context, id ProviderID, version string, spec SessionSpec) (session Session, rootOwned bool, err error) {
	if ctx == nil {
		return nil, false, context.Canceled
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	ownedSpec := cloneSessionSpec(spec)
	if err := ownedSpec.Validate(); err != nil {
		return nil, false, err
	}
	provider, err := registry.Resolve(id, version)
	if err != nil {
		return nil, false, err
	}
	report, err := safeProviderProbe(provider, ctx)
	if err != nil {
		return nil, false, err
	}
	if err := report.SatisfiesSpec(ownedSpec); err != nil {
		return nil, false, err
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	var raw Session
	if owner, ok := provider.(RootOwnershipStarter); ok {
		raw, rootOwned, err = safeRootOwnershipStart(owner, ctx, cloneSessionSpec(ownedSpec))
	} else {
		raw, err = safeProviderStart(provider, ctx, cloneSessionSpec(ownedSpec))
	}
	if err != nil {
		return nil, rootOwned, err
	}
	guarded, err := newGuardedSession(raw, cloneSessionSpec(ownedSpec))
	if err != nil {
		// Cleanup must not inherit a canceled request context: a provider that
		// honors cancellation still needs a short independent close window.
		safeSessionClose(raw, context.Background())
		return nil, rootOwned, err
	}
	if finalizer, ok := raw.(PublicationFinalizer); ok && finalizer != nil {
		return &guardedPublicationFinalizer{guardedSession: guarded, raw: finalizer}, rootOwned, nil
	}
	return guarded, rootOwned, nil
}

func validateMetadata(metadata Metadata) error {
	if err := validateRegistryToken(string(metadata.ID), MaxIdentifierBytes); err != nil {
		return ErrInvalidRegistration
	}
	if err := validateRegistryToken(metadata.Version, MaxVersionBytes); err != nil {
		return ErrInvalidRegistration
	}
	if err := validateRegistryToken(metadata.ImplementationRevision, MaxImplementationRevisionBytes); err != nil {
		return ErrInvalidRegistration
	}
	if err := metadata.Assurance.ValidateActual(); err != nil {
		return ErrInvalidRegistration
	}
	return nil
}

func validateRegistryToken(value string, max int) error {
	if value == "" || len(value) > max || !utf8.ValidString(value) || strings.TrimSpace(value) != value {
		return ErrInvalidRegistration
	}
	for _, r := range value {
		if unicode.IsSpace(r) || unicode.IsControl(r) || unicode.In(r, unicode.Cf, unicode.Cs, unicode.Co) {
			return ErrInvalidRegistration
		}
	}
	return nil
}

func safeProviderID(provider Provider) (id ProviderID, err error) {
	defer func() {
		if recover() != nil {
			id, err = "", ErrProviderPanic
		}
	}()
	id = provider.ID()
	if validateRegistryToken(string(id), MaxIdentifierBytes) != nil {
		return "", ErrInvalidRegistration
	}
	return id, nil
}

func safeProviderProbe(provider Provider, ctx context.Context) (report AssuranceReport, err error) {
	defer func() {
		if recover() != nil {
			report, err = AssuranceReport{}, ErrProviderPanic
		}
	}()
	report = provider.Probe(ctx)
	report, readinessErr := normalizeProviderReadiness(report)
	if readinessErr != nil {
		return AssuranceReport{}, ErrProviderFailure
	}
	if report.Available && report.Actual.ValidateActual() != nil {
		return AssuranceReport{}, ErrProviderFailure
	}
	if err := validateSupportedNetworks(report.SupportedNetworks); err != nil {
		return AssuranceReport{}, ErrProviderFailure
	}
	if !report.Available {
		report.UnavailableCause = "sandbox provider unavailable"
	}
	return report, nil
}

func normalizeProviderReadiness(report AssuranceReport) (AssuranceReport, error) {
	if report.Available {
		switch report.Readiness {
		case "", ProviderReadinessReady:
			report.Readiness = ProviderReadinessReady
		default:
			return AssuranceReport{}, ErrProviderFailure
		}
		return report, nil
	}
	if report.Readiness == "" {
		report.Readiness = ProviderReadinessUnavailable
	}
	if report.Readiness == ProviderReadinessReady {
		return AssuranceReport{}, ErrProviderFailure
	}
	switch report.Readiness {
	case ProviderReadinessNotInstalled, ProviderReadinessRepairRequired, ProviderReadinessUnavailable:
		return report, nil
	default:
		return AssuranceReport{}, ErrProviderFailure
	}
}

func validateSupportedNetworks(networks []NetworkPolicy) error {
	seen := make(map[NetworkPolicy]struct{}, len(networks))
	for _, network := range networks {
		if network != NetworkHost && network != NetworkDisabled && network != NetworkIsolated {
			return ErrInvalidSpec
		}
		if _, exists := seen[network]; exists {
			return ErrInvalidSpec
		}
		seen[network] = struct{}{}
	}
	return nil
}

func safeProviderStart(provider Provider, ctx context.Context, spec SessionSpec) (session Session, err error) {
	defer func() {
		if recover() != nil {
			if session != nil {
				safeSessionClose(session, context.Background())
			}
			session, err = nil, ErrProviderPanic
		}
	}()
	session, err = provider.Start(ctx, spec)
	if err != nil {
		if session != nil {
			safeSessionClose(session, context.Background())
		}
		return nil, sanitizeProviderError(err)
	}
	if session == nil {
		return nil, ErrProviderFailure
	}
	return session, nil
}

// safeRootOwnershipStart keeps the registry's panic/error boundary while
// preserving the provider's invocation-scoped root ownership result. A panic
// cannot truthfully report whether ACL/repair adoption already happened, so it
// is conservatively ownership=true; this prevents unsafe generic deletion.
func safeRootOwnershipStart(provider RootOwnershipStarter, ctx context.Context, spec SessionSpec) (session Session, rootOwned bool, err error) {
	defer func() {
		if recover() != nil {
			if session != nil {
				safeSessionClose(session, context.Background())
			}
			session, rootOwned, err = nil, true, ErrProviderPanic
		}
	}()
	session, rootOwned, err = provider.StartWithRootOwnership(ctx, spec)
	if err != nil {
		if session != nil {
			safeSessionClose(session, context.Background())
		}
		return nil, rootOwned, sanitizeProviderError(err)
	}
	if session == nil {
		return nil, rootOwned, ErrProviderFailure
	}
	return session, rootOwned, nil
}

// guardedSession is the only session that crosses the registry boundary. It
// rechecks provider-reported capabilities on every Run and owns a fenced
// lease, so a raw provider session cannot bypass lifecycle or artifact rules.
type guardedSession struct {
	raw                 Session
	spec                SessionSpec
	lease               *FencedLease
	assurance           AssuranceReport
	limits              Limits
	stateMu             sync.Mutex
	callbackMu          sync.Mutex
	runMu               sync.Mutex
	closed              bool
	publicationTerminal bool
	// afterBeginRunForTest holds the admitted-before-callback window only in
	// same-package tests.
	afterBeginRunForTest func()
	closeMu              sync.Mutex
	closeDone            chan struct{}
	closing              bool
	closeDoneOK          bool
}

// guardedPublicationFinalizer is intentionally distinct from guardedSession.
// Only a raw session which supplied the optional capability can cross the
// registry boundary with it. The mutex both serializes provider callbacks and
// permanently fences the first terminal outcome selected by the adapter.
type guardedPublicationFinalizer struct {
	*guardedSession
	raw PublicationFinalizer

	finalizeMu      sync.Mutex
	selectedOutcome PublicationOutcome
	finalized       bool
	// beforeRunLockForTest is used only by same-package tests.
	beforeRunLockForTest func()
}

func (session *guardedPublicationFinalizer) FinalizePublication(ctx context.Context, outcome PublicationOutcome) error {
	if ctx == nil {
		return context.Canceled
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := outcome.Validate(); err != nil {
		return err
	}
	if session == nil || session.guardedSession == nil || session.raw == nil {
		return ErrProviderFailure
	}

	// Run and Exec acquire runMu before stateMu and callbackMu. Holding it
	// first ensures terminal cleanup cannot overtake an already-admitted call.
	if session.beforeRunLockForTest != nil {
		session.beforeRunLockForTest()
	}
	session.runMu.Lock()
	defer session.runMu.Unlock()
	session.finalizeMu.Lock()
	defer session.finalizeMu.Unlock()
	if session.selectedOutcome != "" {
		if session.selectedOutcome != outcome {
			return ErrProviderFailure
		}
		if session.finalized {
			return nil
		}
	}
	if session.selectedOutcome == "" {
		if !session.beginPublicationTerminal() {
			return ErrLeaseTerminated
		}
		session.selectedOutcome = outcome
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	// Keep the same capability and lifecycle rechecks as Exec before entering
	// a raw provider callback. This prevents a provider from changing its
	// session contract between execution and terminal cleanup.
	session.callbackMu.Lock()
	defer session.callbackMu.Unlock()
	report, limits, err := inspectSession(session.raw)
	if err != nil {
		return err
	}
	if err := validateSessionContract(report, limits, session.spec); err != nil {
		return err
	}
	if session.isClosed() {
		return ErrLeaseTerminated
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := safeSessionFinalize(session.raw, ctx, outcome); err != nil {
		return sanitizeSessionError(err)
	}
	session.finalized = true
	return nil
}

func newGuardedSession(raw Session, spec SessionSpec) (*guardedSession, error) {
	if raw == nil {
		return nil, ErrProviderFailure
	}
	report, limits, err := inspectSession(raw)
	if err != nil {
		return nil, err
	}
	if err := validateSessionContract(report, limits, spec); err != nil {
		return nil, err
	}
	report = sanitizeSessionReport(report)
	lease, err := NewFencedLease(spec.Lease)
	if err != nil {
		return nil, ErrProviderFailure
	}
	return &guardedSession{raw: raw, spec: cloneSessionSpec(spec), lease: lease, assurance: cloneAssuranceReport(report), limits: limits}, nil
}

func (session *guardedSession) Assurance() AssuranceReport {
	if session == nil {
		return AssuranceReport{}
	}
	session.stateMu.Lock()
	defer session.stateMu.Unlock()
	if session.closed {
		return AssuranceReport{}
	}
	return cloneAssuranceReport(session.assurance)
}

func (session *guardedSession) Limits() Limits {
	if session == nil {
		return Limits{}
	}
	session.stateMu.Lock()
	defer session.stateMu.Unlock()
	if session.closed {
		return Limits{}
	}
	return session.limits
}

func (session *guardedSession) Run(ctx context.Context, command Command) (artifacts ArtifactSet, err error) {
	if ctx == nil {
		return ArtifactSet{}, context.Canceled
	}
	if err := ctx.Err(); err != nil {
		return ArtifactSet{}, err
	}
	if err := ValidateCommand(command); err != nil {
		return ArtifactSet{}, err
	}
	if session == nil || session.raw == nil || session.lease == nil {
		return ArtifactSet{}, ErrProviderFailure
	}
	session.runMu.Lock()
	defer session.runMu.Unlock()
	if !session.beginRun() {
		return ArtifactSet{}, ErrLeaseTerminated
	}
	if session.afterBeginRunForTest != nil {
		session.afterBeginRunForTest()
	}
	if err := ctx.Err(); err != nil {
		return ArtifactSet{}, err
	}
	session.callbackMu.Lock()
	defer session.callbackMu.Unlock()
	report, limits, err := inspectSession(session.raw)
	if err != nil {
		return ArtifactSet{}, err
	}
	if err := validateSessionContract(report, limits, session.spec); err != nil {
		return ArtifactSet{}, err
	}
	copyCommand := Command{Args: append([]string(nil), command.Args...)}
	artifacts, err = safeSessionRun(session.raw, ctx, copyCommand)
	if err != nil {
		return ArtifactSet{}, sanitizeSessionError(err)
	}
	if err := artifacts.Validate(session.spec.ArtifactPolicy); err != nil {
		return ArtifactSet{}, ErrArtifactUnverified
	}
	if session.isClosed() {
		return ArtifactSet{}, ErrLeaseTerminated
	}
	return cloneArtifactSet(artifacts), nil
}

// Exec is the detailed, additive execution path. The request is deliberately
// required to repeat the session policy: a provider cannot be widened or
// rebound by a caller after the session has been admitted.
func (session *guardedSession) Exec(ctx context.Context, request ExecRequest) (result ExecResult, err error) {
	if ctx == nil {
		return ExecResult{}, context.Canceled
	}
	if err := ctx.Err(); err != nil {
		return ExecResult{}, err
	}
	if session == nil || session.raw == nil || session.lease == nil {
		return ExecResult{}, ErrProviderFailure
	}
	if err := request.Validate(); err != nil {
		return ExecResult{}, err
	}
	if request.Limits != session.spec.Limits || request.Network != session.spec.Network || request.ArtifactPolicy != session.spec.ArtifactPolicy || request.Lease != session.spec.Lease {
		return ExecResult{}, ErrLeaseMismatch
	}
	raw, ok := session.raw.(ExecSession)
	if !ok || raw == nil {
		return ExecResult{}, ErrExecUnsupported
	}
	session.runMu.Lock()
	defer session.runMu.Unlock()
	if !session.beginRun() {
		return ExecResult{}, ErrLeaseTerminated
	}
	if session.afterBeginRunForTest != nil {
		session.afterBeginRunForTest()
	}
	session.callbackMu.Lock()
	defer session.callbackMu.Unlock()
	report, limits, err := inspectSession(session.raw)
	if err != nil {
		return ExecResult{}, err
	}
	if err := validateSessionContract(report, limits, session.spec); err != nil {
		return ExecResult{}, err
	}
	copyRequest := request
	copyRequest.Args = append([]string(nil), request.Args...)
	result, err = safeSessionExec(raw, ctx, copyRequest)
	if err != nil {
		safeErr := sanitizeExecError(err)
		if errors.Is(safeErr, ErrExecFailed) || errors.Is(safeErr, ErrExecTimeout) || errors.Is(safeErr, ErrExecOutputLimit) || errors.Is(safeErr, context.Canceled) {
			if validateErr := result.Validate(session.spec.ArtifactPolicy); validateErr != nil {
				return ExecResult{}, safeErr
			}
			return cloneExecResult(result), safeErr
		}
		return ExecResult{}, safeErr
	}
	if err := result.Validate(session.spec.ArtifactPolicy); err != nil {
		return ExecResult{}, err
	}
	if session.isClosed() {
		return ExecResult{}, ErrLeaseTerminated
	}
	return cloneExecResult(result), nil
}

func (session *guardedSession) Close(ctx context.Context) (err error) {
	if ctx == nil {
		return context.Canceled
	}
	if session == nil || session.raw == nil || session.lease == nil {
		return ErrLeaseTerminated
	}
	session.stateMu.Lock()
	if !session.closed {
		session.closed = true
		session.lease.Terminate(false)
	}
	session.stateMu.Unlock()

	for {
		session.closeMu.Lock()
		if session.closeDoneOK {
			session.closeMu.Unlock()
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			return nil
		}
		if session.closing {
			done := session.closeDone
			session.closeMu.Unlock()
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		session.closing = true
		session.closeDone = make(chan struct{})
		done := session.closeDone
		session.closeMu.Unlock()

		// The timeout bounds the context supplied to a cooperative provider. Go
		// cannot forcibly interrupt a callback that ignores its context. A
		// non-nil result does not prove termination, so later Close calls retry.
		closeCtx, cancel := context.WithTimeout(ctx, sessionCloseTimeout)
		closeErr := safeSessionCloseResult(session.raw, closeCtx)
		cancel()
		session.closeMu.Lock()
		session.closing = false
		session.closeDoneOK = closeErr == nil
		close(done)
		session.closeMu.Unlock()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return closeErr
	}
}

func (session *guardedSession) isClosed() bool {
	session.stateMu.Lock()
	defer session.stateMu.Unlock()
	return session.closed
}

func (session *guardedSession) beginRun() bool {
	session.stateMu.Lock()
	defer session.stateMu.Unlock()
	if session.closed || session.publicationTerminal {
		return false
	}
	if err := session.lease.Validate(session.spec.Lease); err != nil {
		return false
	}
	return true
}

// beginPublicationTerminal permanently rejects new Run and Exec calls before
// the finalizer waits behind an in-flight provider callback. It deliberately
// does not terminate the lease: the selected finalizer may safely retry after
// a provider failure, and Close retains its existing concurrent semantics.
func (session *guardedSession) beginPublicationTerminal() bool {
	session.stateMu.Lock()
	defer session.stateMu.Unlock()
	if session.closed || session.publicationTerminal {
		return false
	}
	session.publicationTerminal = true
	return true
}

func inspectSession(raw Session) (report AssuranceReport, limits Limits, err error) {
	defer func() {
		if recover() != nil {
			report, limits, err = AssuranceReport{}, Limits{}, ErrProviderPanic
		}
	}()
	report = raw.Assurance()
	limits = raw.Limits()
	return report, limits, nil
}

func sanitizeSessionReport(report AssuranceReport) AssuranceReport {
	if normalized, err := normalizeProviderReadiness(report); err == nil {
		report = normalized
	} else {
		report = AssuranceReport{Readiness: ProviderReadinessUnavailable}
	}
	if !report.Available {
		report.UnavailableCause = "sandbox provider unavailable"
	} else {
		report.UnavailableCause = ""
	}
	return report
}

func cloneAssuranceReport(report AssuranceReport) AssuranceReport {
	report.SupportedNetworks = append([]NetworkPolicy(nil), report.SupportedNetworks...)
	return report
}

func validateSessionContract(report AssuranceReport, limits Limits, spec SessionSpec) error {
	if _, err := normalizeProviderReadiness(report); err != nil {
		return err
	}
	if err := report.SatisfiesSpec(spec); err != nil {
		return err
	}
	if report.Network != spec.Network || limits != spec.Limits {
		return ErrProviderFailure
	}
	return nil
}

func safeSessionRun(raw Session, ctx context.Context, command Command) (artifacts ArtifactSet, err error) {
	defer func() {
		if recover() != nil {
			artifacts, err = ArtifactSet{}, ErrProviderPanic
		}
	}()
	return raw.Run(ctx, command)
}

func safeSessionExec(raw ExecSession, ctx context.Context, request ExecRequest) (result ExecResult, err error) {
	defer func() {
		if recover() != nil {
			result, err = ExecResult{}, ErrProviderPanic
		}
	}()
	return raw.Exec(ctx, request)
}

func safeSessionFinalize(raw PublicationFinalizer, ctx context.Context, outcome PublicationOutcome) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrProviderPanic
		}
	}()
	return raw.FinalizePublication(ctx, outcome)
}

func sanitizeExecError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if errors.Is(err, ErrExecUnsupported) {
		return ErrExecUnsupported
	}
	if errors.Is(err, ErrExecFailed) {
		return ErrExecFailed
	}
	if errors.Is(err, ErrExecTimeout) {
		return ErrExecTimeout
	}
	if errors.Is(err, ErrExecOutputLimit) {
		return ErrExecOutputLimit
	}
	if errors.Is(err, ErrProviderPanic) {
		return ErrProviderPanic
	}
	return ErrProviderFailure
}

func safeSessionCloseResult(raw Session, ctx context.Context) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrProviderPanic
		}
	}()
	err = raw.Close(ctx)
	return sanitizeSessionError(err)
}

func sanitizeSessionError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	if errors.Is(err, ErrLeaseMismatch) || errors.Is(err, ErrLeaseTerminated) {
		return err
	}
	if errors.Is(err, ErrProviderPanic) {
		return ErrProviderPanic
	}
	return ErrProviderFailure
}

func cloneArtifactSet(set ArtifactSet) ArtifactSet {
	set.Items = append([]Artifact(nil), set.Items...)
	return set
}

func cloneExecResult(result ExecResult) ExecResult {
	result.Stdout.Preview = append([]byte(nil), result.Stdout.Preview...)
	result.Stderr.Preview = append([]byte(nil), result.Stderr.Preview...)
	result.Artifacts = cloneArtifactSet(result.Artifacts)
	return result
}

func safeSessionClose(session Session, ctx context.Context) {
	defer func() { _ = recover() }()
	if session == nil {
		return
	}
	closeCtx, cancel := context.WithTimeout(ctx, sessionCloseTimeout)
	defer cancel()
	_ = session.Close(closeCtx)
}

func sanitizeProviderError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	if errors.Is(err, ErrAssuranceTooWeak) {
		return ErrAssuranceTooWeak
	}
	if errors.Is(err, ErrUnavailable) {
		return ErrUnavailable
	}
	return ErrProviderFailure
}

func providerKey(id ProviderID, version string) string { return string(id) + "\x00" + version }

// NewLocalRegistration returns the legacy rootless built-in local registration.
// It is retained for callers that deliberately do not configure a server-owned
// work root. On Windows that means the provider remains unavailable; it never
// falls back to ordinary host execution.
func NewLocalRegistration() Registration {
	return localRegistration(LocalProvider{})
}

// NewLocalRegistrationForWorkRoot returns the built-in local registration for
// a canonical, absolute server-owned work root. The platform implementation
// retains this configuration privately; it does not add provider state to the
// public session contracts. On Windows it is available only to an unelevated
// current-user process after the native Job and process-attribute support
// probe succeeds.
func NewLocalRegistrationForWorkRoot(absWorkRoot string) (Registration, error) {
	provider, err := newLocalProviderForWorkRoot(absWorkRoot)
	if err != nil {
		return Registration{}, err
	}
	return localRegistration(provider), nil
}

func localRegistration(provider LocalProvider) Registration {
	return Registration{
		Metadata: Metadata{
			ID: provider.ID(), Version: localProviderVersion, ImplementationRevision: localProviderRevision(),
			Assurance: Assurance{Level: AssuranceProcess, SharedKernel: true},
		},
		Provider: provider,
	}
}
