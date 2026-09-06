//go:build windows

package sandbox

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"path/filepath"
	"sync"
	"time"
)

const (
	windowsLocalProviderRevision = "current-user-restricted-job-v1"
	windowsBackendTimeout        = 5 * time.Second
)

// windowsSessionBackend is the private bridge from the portable provider
// lifecycle to the current-user restricted-token, session-authority, and Job
// implementation. It accepts only server-derived paths, limits, and fresh
// environment values; it has no host-command or caller-environment fallback.
// Probe reports current capability readiness. ProbeActive only checks an
// admitted session; it never starts another target. Start is the single
// admission point and owns capability-root verification, Job construction,
// target lifecycle, and bounded cleanup.
type windowsSessionBackend interface {
	Probe(context.Context) error
	ProbeActive(context.Context) error
	Start(context.Context, windowsBackendStart) (windowsBackendSession, error)
}

// windowsBackendRecovery is intentionally private and optional. A concrete
// backend may recover only after it has stopped and closed the previous
// current-user session and its Job. It must not launch work.
type windowsBackendRecovery interface {
	Recover(context.Context) error
}

type windowsBackendSession interface {
	Execute(context.Context, windowsBackendExec, io.Writer, io.Writer) (windowsBackendExecResult, error)
	Terminate(context.Context) error
	Finalize(context.Context, PublicationOutcome) error
	Abort(context.Context) error
	Close(context.Context) error
	Poison(context.Context) error
}

type windowsBackendStart struct {
	Spec        SessionSpec
	Root        string
	Workspace   string
	Artifacts   string
	Environment []string
}

type windowsBackendExec struct {
	Args           []string
	Limits         Limits
	ArtifactPolicy ArtifactPolicy
	Lease          LeaseIdentity
}

type windowsBackendExecResult struct {
	ExitCode  int
	Signal    string
	TimedOut  bool
	Artifacts ArtifactSet
}

type unavailableWindowsBackend struct{}

func (unavailableWindowsBackend) Probe(context.Context) error { return ErrUnavailable }
func (unavailableWindowsBackend) ProbeActive(context.Context) error {
	return ErrUnavailable
}
func (unavailableWindowsBackend) Start(context.Context, windowsBackendStart) (windowsBackendSession, error) {
	return nil, ErrUnavailable
}

type windowsAdmission struct {
	token chan struct{}

	mu            sync.Mutex
	epoch         uint64
	leasedEpoch   uint64
	active        bool
	compatible    bool
	poisoned      bool
	recovering    bool
	recoveryOwner windowsBackendRecovery
}

func newWindowsAdmission() *windowsAdmission {
	admission := &windowsAdmission{token: make(chan struct{}, 1)}
	admission.token <- struct{}{}
	return admission
}

func (admission *windowsAdmission) acquire(ctx context.Context) (uint64, error) {
	if ctx == nil {
		return 0, context.Canceled
	}
	admission.mu.Lock()
	unavailable := admission.poisoned || admission.recovering
	admission.mu.Unlock()
	if unavailable {
		return 0, ErrUnavailable
	}
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-admission.token:
		admission.mu.Lock()
		poisoned := admission.poisoned || admission.recovering
		if !poisoned {
			admission.epoch++
			if admission.epoch == 0 {
				admission.epoch++
			}
			admission.leasedEpoch = admission.epoch
		}
		admission.mu.Unlock()
		if poisoned {
			admission.token <- struct{}{}
			return 0, ErrUnavailable
		}
		return admission.epoch, nil
	}
}

func (admission *windowsAdmission) tryAcquire() bool {
	select {
	case <-admission.token:
		admission.mu.Lock()
		poisoned := admission.poisoned
		admission.mu.Unlock()
		if poisoned {
			admission.token <- struct{}{}
			return false
		}
		return true
	default:
		return false
	}
}

func (admission *windowsAdmission) releaseProbe() {
	admission.mu.Lock()
	poisoned := admission.poisoned || admission.recovering
	admission.mu.Unlock()
	if !poisoned {
		admission.token <- struct{}{}
	}
}

func (admission *windowsAdmission) poisonProbe(owner windowsBackendRecovery) bool {
	if owner == nil {
		return false
	}
	admission.mu.Lock()
	defer admission.mu.Unlock()
	if admission.recovering {
		return false
	}
	admission.poisoned, admission.active, admission.compatible = true, false, false
	admission.recoveryOwner = owner
	return true
}

func (admission *windowsAdmission) setActive(epoch uint64, active, compatible bool) bool {
	admission.mu.Lock()
	defer admission.mu.Unlock()
	if epoch == 0 || admission.leasedEpoch != epoch || admission.poisoned || admission.recovering {
		return false
	}
	admission.active = active
	admission.compatible = compatible
	return true
}

func (admission *windowsAdmission) activeCompatible() bool {
	admission.mu.Lock()
	defer admission.mu.Unlock()
	return admission.active && admission.compatible && !admission.poisoned
}

func (admission *windowsAdmission) poison(epoch uint64) bool {
	return admission.poisonWithOwner(epoch, nil)
}

func (admission *windowsAdmission) poisonWithOwner(epoch uint64, owner windowsBackendRecovery) bool {
	admission.mu.Lock()
	defer admission.mu.Unlock()
	if epoch == 0 || admission.leasedEpoch != epoch {
		return false
	}
	admission.poisoned = true
	admission.active = false
	admission.compatible = false
	if owner != nil {
		admission.recoveryOwner = owner
	}
	return true
}

// release returns the one session token only when this exact epoch still owns
// it. A poisoned epoch intentionally keeps it unavailable for recovery.
func (admission *windowsAdmission) release(epoch uint64) bool {
	admission.mu.Lock()
	if epoch == 0 || admission.leasedEpoch != epoch || admission.poisoned || admission.recovering {
		admission.mu.Unlock()
		return false
	}
	admission.leasedEpoch = 0
	admission.active = false
	admission.compatible = false
	admission.mu.Unlock()
	admission.token <- struct{}{}
	return true
}

// beginRecovery takes ownership of a poisoned, already-held token. It also
// invalidates every old session epoch before calling backend code.
func (admission *windowsAdmission) beginRecovery() windowsBackendRecovery {
	admission.mu.Lock()
	defer admission.mu.Unlock()
	if !admission.poisoned || admission.active || admission.recovering || admission.recoveryOwner == nil {
		return nil
	}
	admission.recovering = true
	admission.epoch++
	if admission.epoch == 0 {
		admission.epoch++
	}
	admission.leasedEpoch = 0
	return admission.recoveryOwner
}

func (admission *windowsAdmission) finishRecovery(success bool) {
	admission.mu.Lock()
	if !admission.recovering {
		admission.mu.Unlock()
		return
	}
	admission.recovering = false
	if !success {
		admission.mu.Unlock()
		return
	}
	admission.poisoned = false
	admission.active = false
	admission.compatible = false
	admission.recoveryOwner = nil
	admission.mu.Unlock()
	admission.token <- struct{}{}
}

var defaultWindowsAdmission = newWindowsAdmission()

// LocalProvider has only package-private dependencies. Its zero value is safe:
// without a configured direct current-user backend it remains unavailable
// rather than using an os/exec fallback.
type LocalProvider struct {
	// workRoot is server configuration retained solely for the native backend
	// factory. It is never accepted from SessionSpec or ExecRequest.
	workRoot  string
	backend   windowsSessionBackend
	admission *windowsAdmission
}

func (LocalProvider) ID() ProviderID { return "local-ephemeral" }

func (provider LocalProvider) windowsBackend() windowsSessionBackend {
	if provider.backend != nil {
		return provider.backend
	}
	return unavailableWindowsBackend{}
}

func (provider LocalProvider) windowsAdmission() *windowsAdmission {
	if provider.admission != nil {
		return provider.admission
	}
	return defaultWindowsAdmission
}

func (provider LocalProvider) Probe(ctx context.Context) AssuranceReport {
	if ctx == nil || ctx.Err() != nil {
		return unavailableWindowsProbe()
	}
	admission := provider.windowsAdmission()
	if admission.tryAcquire() {
		defer admission.releaseProbe()
		probeCtx, cancel := windowsBackendCallContext(ctx)
		probeErr := provider.windowsBackend().Probe(probeCtx)
		cancel()
		if probeErr != nil {
			if owner, ok := provider.windowsBackend().(interface {
				windowsBackendRecovery
				hasUncertainCleanup() bool
			}); ok && owner.hasUncertainCleanup() {
				admission.poisonProbe(owner)
			}
			return unavailableWindowsProbe()
		}
		return readyWindowsProbe()
	}
	if admission.activeCompatible() {
		probeCtx, cancel := windowsBackendCallContext(ctx)
		probeErr := provider.windowsBackend().ProbeActive(probeCtx)
		cancel()
		if probeErr == nil {
			return readyWindowsProbe()
		}
	}
	if recovery := admission.beginRecovery(); recovery != nil {
		recoveryCtx, cancel := windowsBackendCallContext(ctx)
		recoveryErr := recovery.Recover(recoveryCtx)
		cancel()
		admission.finishRecovery(recoveryErr == nil)
		if recoveryErr == nil {
			return readyWindowsProbe()
		}
	}
	return unavailableWindowsProbe()
}

func readyWindowsProbe() AssuranceReport {
	return AssuranceReport{
		Available:         true,
		Actual:            Assurance{Level: AssuranceProcess, SharedKernel: true, NetworkIsolation: false},
		Network:           NetworkHost,
		SupportedNetworks: []NetworkPolicy{NetworkHost},
		LimitsEnforced:    true,
		MountsEnforced:    true,
	}
}

func unavailableWindowsProbe() AssuranceReport {
	return AssuranceReport{UnavailableCause: "windows sandbox provider unavailable"}
}

func (provider LocalProvider) Start(ctx context.Context, spec SessionSpec) (Session, error) {
	session, _, err := provider.StartWithRootOwnership(ctx, spec)
	return session, err
}

// StartWithRootOwnership reports root ownership only after the exact native
// Start attempt. A clean pre-adoption failure leaves false so the adapter can
// remove its staging root; any session cleanup uncertainty retains true and
// transfers recovery to this provider's configured backend.
func (provider LocalProvider) StartWithRootOwnership(ctx context.Context, spec SessionSpec) (Session, bool, error) {
	spec = cloneSessionSpec(spec)
	if ctx == nil {
		return nil, false, context.Canceled
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if err := spec.Validate(); err != nil {
		return nil, false, err
	}
	// The zero value deliberately has no native backend wiring. Do not disclose
	// assurance details for an unavailable provider; a configured backend is
	// required before request-specific admission checks are meaningful.
	if provider.backend == nil {
		return nil, false, ErrUnavailable
	}
	if err := readyWindowsProbe().SatisfiesSpec(spec); err != nil {
		return nil, false, err
	}
	admission := provider.windowsAdmission()
	epoch, err := admission.acquire(ctx)
	if err != nil {
		return nil, false, err
	}
	release := true
	defer func() {
		if release {
			admission.release(epoch)
		}
	}()
	start, err := windowsStartRequest(spec)
	if err != nil {
		return nil, false, err
	}
	startCtx, cancel := windowsBackendCallContext(ctx)
	backendSession, startErr := provider.windowsBackend().Start(startCtx, start)
	cancel()
	if startErr != nil || backendSession == nil || ctx.Err() != nil {
		rootOwned := false
		if backendSession != nil && !windowsCleanupBackendSession(backendSession) {
			if owner, ok := provider.windowsBackend().(windowsBackendRecovery); ok {
				admission.poisonWithOwner(epoch, owner)
			} else {
				admission.poison(epoch)
			}
			release = false
			rootOwned = true
		}
		if err := ctx.Err(); err != nil {
			return nil, rootOwned, err
		}
		return nil, rootOwned, sanitizeWindowsBackendError(ctx, startErr)
	}
	if !admission.setActive(epoch, true, true) {
		rootOwned := false
		if !windowsCleanupBackendSession(backendSession) {
			if owner, ok := provider.windowsBackend().(windowsBackendRecovery); ok {
				admission.poisonWithOwner(epoch, owner)
			} else {
				admission.poison(epoch)
			}
			release = false
			rootOwned = true
		}
		return nil, rootOwned, ErrUnavailable
	}
	release = false
	var recoveryOwner windowsBackendRecovery
	if owner, ok := provider.windowsBackend().(windowsBackendRecovery); ok {
		recoveryOwner = owner
	}
	return &windowsLocalSession{
		spec:          spec,
		report:        readyWindowsProbe(),
		backend:       backendSession,
		admission:     admission,
		epoch:         epoch,
		recoveryOwner: recoveryOwner,
	}, true, nil
}

func windowsBackendCallContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, windowsBackendTimeout)
}

func windowsCleanupBackendSession(session windowsBackendSession) bool {
	return windowsCleanupBackendSessionWithin(session, windowsBackendTimeout)
}

func windowsCleanupBackendSessionWithin(session windowsBackendSession, timeout time.Duration) bool {
	if session == nil {
		return true
	}
	abortErr := windowsCleanupCallWithin(session.Abort, timeout)
	closeErr := windowsCleanupCallWithin(session.Close, timeout)
	if abortErr == nil && closeErr == nil {
		return true
	}
	_ = windowsCleanupCallWithin(session.Poison, timeout)
	return false
}

func windowsCleanupCall(call func(context.Context) error) error {
	return windowsCleanupCallWithin(call, windowsBackendTimeout)
}

func windowsCleanupCallWithin(call func(context.Context) error, timeout time.Duration) error {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return call(cleanupCtx)
}

func windowsStartRequest(spec SessionSpec) (windowsBackendStart, error) {
	var workspace, artifacts string
	for _, mount := range spec.Mounts {
		switch mount.Target {
		case "/workspace":
			workspace = mount.Source
		case "/artifacts":
			artifacts = mount.Source
		}
	}
	root := filepath.Clean(filepath.Dir(workspace))
	if workspace == "" || artifacts == "" || filepath.Clean(filepath.Dir(artifacts)) != root {
		return windowsBackendStart{}, ErrInvalidSpec
	}
	bindings, err := windowsFixedMountBindings(spec.Mounts, workspace, artifacts)
	if err != nil || !windowsPathIsStrictChild(root, bindings.workspace) || !windowsPathIsStrictChild(root, bindings.artifacts) {
		return windowsBackendStart{}, ErrInvalidSpec
	}
	environment, err := windowsProviderEnvironment(root, bindings.workspace, bindings.artifacts)
	if err != nil {
		return windowsBackendStart{}, err
	}
	return windowsBackendStart{
		Spec:        cloneSessionSpec(spec),
		Root:        root,
		Workspace:   bindings.workspace,
		Artifacts:   bindings.artifacts,
		Environment: append([]string(nil), environment...),
	}, nil
}

func windowsProviderEnvironment(root, workspace, artifacts string) ([]string, error) {
	environment, err := windowsFreshEnvironment(root, workspace, artifacts)
	if err != nil {
		return nil, err
	}
	home := filepath.Join(root, "home")
	cache := filepath.Join(root, "cache")
	return append(environment,
		"XDG_CONFIG_HOME="+filepath.Join(home, "xdg-config"),
		"XDG_CACHE_HOME="+filepath.Join(cache, "xdg-cache"),
		"XDG_DATA_HOME="+filepath.Join(home, "xdg-data"),
		"XDG_RUNTIME_DIR="+filepath.Join(root, "tmp", "xdg-runtime"),
		"GOPATH="+filepath.Join(cache, "go-path"),
		"PNPM_HOME="+filepath.Join(cache, "pnpm"),
		"npm_config_cache="+filepath.Join(cache, "npm"),
		"YARN_CACHE_FOLDER="+filepath.Join(cache, "yarn"),
		"CARGO_HOME="+filepath.Join(cache, "cargo"),
		"RUSTUP_HOME="+filepath.Join(cache, "rustup"),
	), nil
}

type windowsLocalSession struct {
	spec      SessionSpec
	report    AssuranceReport
	backend   windowsBackendSession
	admission *windowsAdmission
	epoch     uint64
	// recoveryOwner is the configured backend that owns the current-user Job
	// and session authority, not the per-session façade for one execution.
	recoveryOwner windowsBackendRecovery

	runMu sync.Mutex
	mu    sync.Mutex

	terminal  PublicationOutcome
	finalized bool
	closing   bool
	closed    bool
	released  bool
	closeDone chan struct{}
}

func (session *windowsLocalSession) Assurance() AssuranceReport {
	if session == nil || session.isClosed() {
		return AssuranceReport{}
	}
	return cloneAssuranceReport(session.report)
}

func (session *windowsLocalSession) Limits() Limits {
	if session == nil || session.isClosed() {
		return Limits{}
	}
	return session.spec.Limits
}

func (session *windowsLocalSession) isClosed() bool {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.closed || session.closing
}

func (session *windowsLocalSession) Run(ctx context.Context, command Command) (ArtifactSet, error) {
	if err := ValidateCommand(command); err != nil {
		return ArtifactSet{}, err
	}
	result, err := session.execute(ctx, ExecRequest{Args: append([]string(nil), command.Args...), Limits: session.spec.Limits, Network: session.spec.Network, ArtifactPolicy: session.spec.ArtifactPolicy, Lease: session.spec.Lease})
	if err != nil {
		return ArtifactSet{}, err
	}
	return cloneArtifactSet(result.Artifacts), nil
}

func (session *windowsLocalSession) Exec(ctx context.Context, request ExecRequest) (ExecResult, error) {
	return session.execute(ctx, request)
}

func (session *windowsLocalSession) execute(ctx context.Context, request ExecRequest) (ExecResult, error) {
	if ctx == nil {
		return ExecResult{}, context.Canceled
	}
	if err := ctx.Err(); err != nil {
		return ExecResult{}, err
	}
	if session == nil || session.backend == nil {
		return ExecResult{}, ErrProviderFailure
	}
	if err := request.Validate(); err != nil {
		return ExecResult{}, err
	}
	if request.Limits != session.spec.Limits || request.Network != session.spec.Network || request.ArtifactPolicy != session.spec.ArtifactPolicy || request.Lease != session.spec.Lease {
		return ExecResult{}, ErrLeaseMismatch
	}
	session.runMu.Lock()
	defer session.runMu.Unlock()
	session.mu.Lock()
	terminal := session.terminal != "" || session.closing || session.closed
	session.mu.Unlock()
	if terminal {
		return ExecResult{}, ErrLeaseTerminated
	}
	collector := newWindowsOutputCollector(request.Limits.MaxOutputBytes, session.backend)
	runCtx, cancel := context.WithTimeout(ctx, request.Limits.WallTime)
	backendResult, backendErr := session.backend.Execute(runCtx, windowsBackendExec{Args: append([]string(nil), request.Args...), Limits: request.Limits, ArtifactPolicy: request.ArtifactPolicy, Lease: request.Lease}, collector.stdout(), collector.stderr())
	runCtxErr := runCtx.Err()
	cancel()
	result := ExecResult{
		Stdout:    collector.stdoutResult(),
		Stderr:    collector.stderrResult(),
		ExitCode:  backendResult.ExitCode,
		Signal:    backendResult.Signal,
		TimedOut:  backendResult.TimedOut,
		Artifacts: cloneArtifactSet(backendResult.Artifacts),
	}
	if collector.exceeded() {
		collector.waitForTermination()
		return result, ErrExecOutputLimit
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if runCtxErr == context.DeadlineExceeded {
		return result, ErrExecTimeout
	}
	if backendErr != nil {
		return result, sanitizeWindowsBackendError(ctx, backendErr)
	}
	if err := result.Validate(request.ArtifactPolicy); err != nil {
		return ExecResult{}, ErrProviderFailure
	}
	return result, nil
}

func (session *windowsLocalSession) FinalizePublication(ctx context.Context, outcome PublicationOutcome) error {
	if ctx == nil {
		return context.Canceled
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := outcome.Validate(); err != nil {
		return err
	}
	if session == nil || session.backend == nil {
		return ErrProviderFailure
	}
	session.runMu.Lock()
	defer session.runMu.Unlock()
	session.mu.Lock()
	if session.closed || session.closing {
		session.mu.Unlock()
		return ErrLeaseTerminated
	}
	if session.terminal != "" {
		if session.terminal != outcome {
			session.mu.Unlock()
			return ErrProviderFailure
		}
		if session.finalized {
			session.mu.Unlock()
			return nil
		}
	} else {
		session.terminal = outcome
	}
	session.mu.Unlock()
	finalizeCtx, cancel := windowsBackendCallContext(ctx)
	finalizeErr := session.backend.Finalize(finalizeCtx, outcome)
	cancel()
	if finalizeErr != nil {
		session.poison(ctx)
		return sanitizeWindowsBackendError(ctx, finalizeErr)
	}
	session.mu.Lock()
	session.finalized = true
	session.mu.Unlock()
	return nil
}

func (session *windowsLocalSession) Close(ctx context.Context) error {
	if ctx == nil {
		return context.Canceled
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if session == nil || session.backend == nil || session.admission == nil {
		return ErrLeaseTerminated
	}
	for {
		session.mu.Lock()
		if session.closed {
			session.mu.Unlock()
			return nil
		}
		if session.closing {
			done := session.closeDone
			session.mu.Unlock()
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		session.closing = true
		session.closeDone = make(chan struct{})
		finalized := session.finalized
		session.mu.Unlock()

		session.runMu.Lock()
		return session.closeLocked(ctx, finalized)
	}
}

func (session *windowsLocalSession) closeLocked(ctx context.Context, finalized bool) error {
	var abortErr error
	if !finalized {
		abortErr = windowsCleanupCall(session.backend.Abort)
	}
	closeErr := windowsCleanupCall(session.backend.Close)
	err := errors.Join(abortErr, closeErr)
	if err != nil {
		session.poison(ctx)
		session.mu.Lock()
		session.closing = false
		done := session.closeDone
		session.closeDone = nil
		session.mu.Unlock()
		close(done)
		session.runMu.Unlock()
		return sanitizeWindowsBackendError(ctx, err)
	}
	session.mu.Lock()
	session.closed = true
	session.closing = false
	release := !session.released
	session.released = true
	done := session.closeDone
	session.closeDone = nil
	session.mu.Unlock()
	close(done)
	session.runMu.Unlock()
	if release {
		session.admission.release(session.epoch)
	}
	return nil
}

func (session *windowsLocalSession) poison(context.Context) {
	if session == nil || session.admission == nil || session.backend == nil {
		return
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), windowsBackendTimeout)
	defer cancel()
	// The session object is the only one that can terminate its active target;
	// do that before publishing the recovery owner. The configured backend then
	// retains the direct Job/session state for Probe-triggered recovery.
	poisonErr := session.backend.Poison(cleanupCtx)
	if !session.admission.poisonWithOwner(session.epoch, session.recoveryOwner) {
		return
	}
	if poisonErr != nil {
		return
	}
}

func sanitizeWindowsBackendError(ctx context.Context, err error) error {
	if err == nil {
		return ErrProviderFailure
	}
	if ctx != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	if errors.Is(err, ErrExecOutputLimit) || errors.Is(err, ErrOutputLimit) {
		return ErrExecOutputLimit
	}
	if errors.Is(err, ErrExecFailed) {
		return ErrExecFailed
	}
	if errors.Is(err, ErrExecTimeout) {
		return ErrExecTimeout
	}
	return ErrProviderFailure
}

type windowsOutputCollector struct {
	mu sync.Mutex

	limit         int64
	total         int64
	overflow      bool
	backend       windowsBackendSession
	stdoutData    windowsOutputData
	stderrData    windowsOutputData
	terminateOnce sync.Once
	terminateDone chan struct{}
}

type windowsOutputData struct {
	preview []byte
	size    int64
	hash    hash.Hash
}

func newWindowsOutputCollector(limit int64, backend windowsBackendSession) *windowsOutputCollector {
	return &windowsOutputCollector{limit: limit, backend: backend, terminateDone: make(chan struct{})}
}

func (collector *windowsOutputCollector) stdout() io.Writer {
	return windowsOutputWriter{collector: collector, stderr: false}
}
func (collector *windowsOutputCollector) stderr() io.Writer {
	return windowsOutputWriter{collector: collector, stderr: true}
}

type windowsOutputWriter struct {
	collector *windowsOutputCollector
	stderr    bool
}

func (writer windowsOutputWriter) Write(value []byte) (int, error) {
	if writer.collector == nil {
		return 0, ErrOutputLimit
	}
	return writer.collector.write(writer.stderr, value)
}

func (collector *windowsOutputCollector) write(stderr bool, value []byte) (int, error) {
	written := len(value)
	collector.mu.Lock()
	if collector.overflow || collector.total > collector.limit-int64(len(value)) {
		collector.overflow = true
		collector.mu.Unlock()
		collector.requestTerminate()
		return 0, ErrExecOutputLimit
	}
	collector.total += int64(len(value))
	data := &collector.stdoutData
	if stderr {
		data = &collector.stderrData
	}
	data.size += int64(len(value))
	if data.hash == nil {
		data.hash = sha256.New()
	}
	_, _ = data.hash.Write(value)
	if remaining := int(DefaultExecPreviewBytes) - len(data.preview); remaining > 0 {
		if len(value) > remaining {
			value = value[:remaining]
		}
		data.preview = append(data.preview, value...)
	}
	collector.mu.Unlock()
	return written, nil
}

func (collector *windowsOutputCollector) requestTerminate() {
	if collector == nil {
		return
	}
	collector.terminateOnce.Do(func() {
		go func() {
			defer close(collector.terminateDone)
			if collector.backend == nil {
				return
			}
			cleanupCtx, cancel := context.WithTimeout(context.Background(), windowsBackendTimeout)
			defer cancel()
			_ = collector.backend.Terminate(cleanupCtx)
		}()
	})
}

func (collector *windowsOutputCollector) waitForTermination() {
	if collector == nil || !collector.exceeded() {
		return
	}
	select {
	case <-collector.terminateDone:
	case <-time.After(windowsBackendTimeout):
	}
}

func (collector *windowsOutputCollector) exceeded() bool {
	collector.mu.Lock()
	defer collector.mu.Unlock()
	return collector.overflow
}

func (collector *windowsOutputCollector) output(data windowsOutputData) Output {
	var digest []byte
	if data.hash == nil {
		sum := sha256.Sum256(nil)
		digest = sum[:]
	} else {
		digest = data.hash.Sum(nil)
	}
	return Output{Preview: append([]byte(nil), data.preview...), Size: data.size, Digest: hex.EncodeToString(digest), Truncated: data.size > int64(len(data.preview))}
}

func (collector *windowsOutputCollector) stdoutResult() Output {
	collector.mu.Lock()
	defer collector.mu.Unlock()
	return collector.output(collector.stdoutData)
}

func (collector *windowsOutputCollector) stderrResult() Output {
	collector.mu.Lock()
	defer collector.mu.Unlock()
	return collector.output(collector.stderrData)
}

// newWindowsLocalProviderForTest keeps the native backend seam private while allowing
// Windows-only tests to exercise every provider lifecycle transition.
func newWindowsLocalProviderForTest(backend windowsSessionBackend) LocalProvider {
	return LocalProvider{backend: backend, admission: newWindowsAdmission()}
}

var _ PublicationFinalizer = (*windowsLocalSession)(nil)
var _ ExecSession = (*windowsLocalSession)(nil)
