package sandbox

import (
	"context"
	"errors"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type registryProvider struct {
	id          ProviderID
	probe       AssuranceReport
	probePanic  bool
	startPanic  bool
	startErr    error
	nilSession  bool
	session     Session
	startSpec   SessionSpec
	captureSpec bool
	cancel      context.CancelFunc
	probes      int
	starts      int
}

// rootOwnershipRegistryProvider models a provider that can decide per Start
// call whether it adopted the adapter's root. It is deliberately a wrapper so
// ordinary registryProvider coverage keeps exercising the legacy contract.
type rootOwnershipRegistryProvider struct {
	*registryProvider
	owned bool
}

func (provider *rootOwnershipRegistryProvider) StartWithRootOwnership(ctx context.Context, spec SessionSpec) (Session, bool, error) {
	session, err := provider.registryProvider.Start(ctx, spec)
	return session, provider.owned, err
}

func (provider *registryProvider) ID() ProviderID { return provider.id }
func (provider *registryProvider) Probe(context.Context) AssuranceReport {
	provider.probes++
	if provider.probePanic {
		panic("provider secret")
	}
	return provider.probe
}
func (provider *registryProvider) Start(ctx context.Context, spec SessionSpec) (Session, error) {
	provider.starts++
	if provider.captureSpec {
		provider.startSpec = spec
	}
	if provider.startPanic {
		panic("start secret")
	}
	if provider.cancel != nil {
		provider.cancel()
	}
	if provider.startErr != nil {
		return nil, provider.startErr
	}
	if provider.nilSession {
		return nil, nil
	}
	if provider.session != nil {
		return provider.session, nil
	}
	return registrySession{report: provider.probe, limits: spec.Limits}, nil
}

type registrySession struct {
	report AssuranceReport
	limits Limits
}

func (session registrySession) Assurance() AssuranceReport { return session.report }
func (session registrySession) Limits() Limits             { return session.limits }
func (session registrySession) Run(context.Context, Command) (ArtifactSet, error) {
	return ArtifactSet{}, nil
}
func (session registrySession) Close(context.Context) error { return nil }

type publicationSession struct {
	registrySession
	finalizePanic bool
	finalizeErrs  []error
	outcomes      []PublicationOutcome
}

type publicationFenceSession struct {
	report AssuranceReport
	limits Limits

	runs  int
	execs int

	finalizeErrs    []error
	execEntered     chan struct{}
	releaseExec     chan struct{}
	finalizeEntered chan struct{}
}

func (session *publicationFenceSession) Assurance() AssuranceReport { return session.report }
func (session *publicationFenceSession) Limits() Limits             { return session.limits }
func (session *publicationFenceSession) Run(context.Context, Command) (ArtifactSet, error) {
	session.runs++
	return ArtifactSet{}, nil
}
func (session *publicationFenceSession) Exec(context.Context, ExecRequest) (ExecResult, error) {
	session.execs++
	if session.execEntered != nil {
		close(session.execEntered)
		<-session.releaseExec
	}
	return validExecResult(), nil
}
func (session *publicationFenceSession) Close(context.Context) error { return nil }
func (session *publicationFenceSession) FinalizePublication(context.Context, PublicationOutcome) error {
	if session.finalizeEntered != nil {
		close(session.finalizeEntered)
	}
	if len(session.finalizeErrs) == 0 {
		return nil
	}
	err := session.finalizeErrs[0]
	session.finalizeErrs = session.finalizeErrs[1:]
	return err
}

func (session *publicationSession) FinalizePublication(_ context.Context, outcome PublicationOutcome) error {
	if session.finalizePanic {
		panic("publication secret")
	}
	session.outcomes = append(session.outcomes, outcome)
	if len(session.finalizeErrs) == 0 {
		return nil
	}
	err := session.finalizeErrs[0]
	session.finalizeErrs = session.finalizeErrs[1:]
	return err
}

type maliciousSession struct {
	report         AssuranceReport
	limits         Limits
	assurancePanic bool
	limitsPanic    bool
	runPanic       bool
	runErr         error
	result         ArtifactSet
	closePanic     bool
	closeErr       error
	runs           int
	closes         int
}

type blockingSession struct {
	report      AssuranceReport
	limits      Limits
	runEntered  chan struct{}
	releaseRun  chan struct{}
	closeCalled chan struct{}
	allowClose  chan struct{}
	runOnce     sync.Once
	closeOnce   sync.Once
}

type metadataTrackingSession struct {
	report         AssuranceReport
	limits         Limits
	assuranceCalls atomic.Int32
	limitsCalls    atomic.Int32
}

func (session *metadataTrackingSession) Assurance() AssuranceReport {
	session.assuranceCalls.Add(1)
	return session.report
}
func (session *metadataTrackingSession) Limits() Limits {
	session.limitsCalls.Add(1)
	return session.limits
}
func (session *metadataTrackingSession) Run(context.Context, Command) (ArtifactSet, error) {
	return ArtifactSet{}, nil
}
func (session *metadataTrackingSession) Close(context.Context) error { return nil }

func (session *blockingSession) Assurance() AssuranceReport { return session.report }
func (session *blockingSession) Limits() Limits             { return session.limits }
func (session *blockingSession) Run(context.Context, Command) (ArtifactSet, error) {
	session.runOnce.Do(func() { close(session.runEntered) })
	<-session.releaseRun
	return ArtifactSet{Items: []Artifact{{Key: "result", Digest: strings.Repeat("a", 64), Size: 1, Verified: true}}}, nil
}
func (session *blockingSession) Close(context.Context) error {
	session.closeOnce.Do(func() {
		close(session.closeCalled)
		<-session.allowClose
		close(session.releaseRun)
	})
	return nil
}

func (session *maliciousSession) Assurance() AssuranceReport {
	if session.assurancePanic {
		panic("assurance secret")
	}
	return session.report
}
func (session *maliciousSession) Limits() Limits {
	if session.limitsPanic {
		panic("limits secret")
	}
	return session.limits
}
func (session *maliciousSession) Run(context.Context, Command) (ArtifactSet, error) {
	session.runs++
	if session.runPanic {
		panic("run secret")
	}
	if session.runErr != nil {
		return ArtifactSet{}, session.runErr
	}
	return session.result, nil
}
func (session *maliciousSession) Close(context.Context) error {
	session.closes++
	if session.closePanic {
		panic("close secret")
	}
	return session.closeErr
}

type retryCloseSession struct {
	report        AssuranceReport
	limits        Limits
	closeCalls    atomic.Int32
	firstStarted  chan struct{}
	secondStarted chan struct{}
	releaseSecond chan struct{}
}

func (session *retryCloseSession) Assurance() AssuranceReport { return session.report }
func (session *retryCloseSession) Limits() Limits             { return session.limits }
func (session *retryCloseSession) Run(context.Context, Command) (ArtifactSet, error) {
	return ArtifactSet{}, nil
}
func (session *retryCloseSession) Close(ctx context.Context) error {
	switch session.closeCalls.Add(1) {
	case 1:
		close(session.firstStarted)
		return errors.New("first close failed")
	default:
		close(session.secondStarted)
		select {
		case <-session.releaseSecond:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

type blockingCloseSession struct {
	report       AssuranceReport
	limits       Limits
	closeCalls   atomic.Int32
	closeStarted chan struct{}
	releaseClose chan struct{}
	startOnce    sync.Once
}

func (session *blockingCloseSession) Assurance() AssuranceReport { return session.report }
func (session *blockingCloseSession) Limits() Limits             { return session.limits }
func (session *blockingCloseSession) Run(context.Context, Command) (ArtifactSet, error) {
	return ArtifactSet{}, nil
}
func (session *blockingCloseSession) Close(ctx context.Context) error {
	session.closeCalls.Add(1)
	session.startOnce.Do(func() { close(session.closeStarted) })
	select {
	case <-session.releaseClose:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func registryRegistration(id ProviderID, version string, provider Provider) Registration {
	return Registration{
		Metadata: Metadata{ID: id, Version: version, ImplementationRevision: "rev-1", Assurance: Assurance{Level: AssuranceProcess, SharedKernel: true}},
		Provider: provider,
	}
}

func registryProviderReady(id ProviderID) *registryProvider {
	return &registryProvider{id: id, probe: AssuranceReport{Available: true, Actual: Assurance{Level: AssuranceProcess, SharedKernel: true}, Network: NetworkHost, LimitsEnforced: true, MountsEnforced: true}}
}

func TestRegistryExactVersionMetadataAndDefensiveOrder(t *testing.T) {
	second := registryProviderReady("z-provider")
	first := registryProviderReady("a-provider")
	registry, err := NewRegistry(4,
		registryRegistration("z-provider", "2", second),
		registryRegistration("a-provider", "1", first),
	)
	if err != nil {
		t.Fatal(err)
	}
	metadata := registry.Metadata()
	if len(metadata) != 2 || metadata[0].ID != "a-provider" || metadata[1].ID != "z-provider" {
		t.Fatalf("metadata order=%#v", metadata)
	}
	metadata[0].ID = "changed"
	if registry.Metadata()[0].ID != "a-provider" {
		t.Fatal("metadata mutation escaped registry")
	}
	if _, err := registry.Resolve("a-provider", "2"); !errors.Is(err, ErrProviderVersionMismatch) {
		t.Fatalf("wrong version error=%v", err)
	}
	if _, err := registry.Resolve("missing", "1"); !errors.Is(err, ErrProviderNotFound) {
		t.Fatalf("unknown provider error=%v", err)
	}
	if _, err := registry.Resolve("a-provider", "bad\nversion"); !errors.Is(err, ErrProviderVersionMismatch) {
		t.Fatalf("invalid version error=%v", err)
	}
}

func TestRegistryRejectsDuplicateCapacityAndIdentity(t *testing.T) {
	first := registryProviderReady("same")
	if _, err := NewRegistry(2, registryRegistration("same", "1", first), registryRegistration("same", "1", registryProviderReady("same"))); !errors.Is(err, ErrInvalidRegistration) {
		t.Fatalf("duplicate error=%v", err)
	}
	if _, err := NewRegistry(1, registryRegistration("a", "1", registryProviderReady("a")), registryRegistration("b", "1", registryProviderReady("b"))); !errors.Is(err, ErrRegistryCapacity) {
		t.Fatalf("capacity error=%v", err)
	}
	if _, err := NewRegistry(1, registryRegistration("declared", "1", registryProviderReady("actual"))); !errors.Is(err, ErrProviderIdentity) {
		t.Fatalf("identity error=%v", err)
	}
	if _, err := NewRegistry(MaxRegistrations + 1); !errors.Is(err, ErrRegistryCapacity) {
		t.Fatalf("invalid limit error=%v", err)
	}
}

func TestRegistryProbeAndStartContainProviderFailures(t *testing.T) {
	panicProvider := registryProviderReady("panic")
	panicProvider.probePanic = true
	registry, err := NewRegistry(2, registryRegistration("panic", "1", panicProvider))
	if err != nil {
		t.Fatal(err)
	}
	report, err := registry.Probe(context.Background(), "panic", "1")
	if !errors.Is(err, ErrProviderPanic) || strings.Contains(report.UnavailableCause, "secret") {
		t.Fatalf("panic probe report=%#v err=%v", report, err)
	}

	unavailable := registryProviderReady("unavailable")
	unavailable.probe = AssuranceReport{UnavailableCause: "provider secret details"}
	registry, err = NewRegistry(2, registryRegistration("unavailable", "1", unavailable))
	if err != nil {
		t.Fatal(err)
	}
	report, err = registry.Probe(context.Background(), "unavailable", "1")
	if err != nil || report.UnavailableCause != "sandbox provider unavailable" {
		t.Fatalf("sanitized unavailable report=%#v err=%v", report, err)
	}
	if report.Readiness != ProviderReadinessUnavailable {
		t.Fatalf("unavailable readiness=%q", report.Readiness)
	}
}

func TestRegistryNormalizesAndPreservesProviderReadiness(t *testing.T) {
	for _, test := range []struct {
		name      string
		id        string
		report    AssuranceReport
		want      ProviderReadiness
		wantError error
	}{
		{name: "available default", id: "available-default", report: AssuranceReport{Available: true, Actual: Assurance{Level: AssuranceProcess, SharedKernel: true}}, want: ProviderReadinessReady},
		{name: "not installed", id: "not-installed", report: AssuranceReport{Readiness: ProviderReadinessNotInstalled}, want: ProviderReadinessNotInstalled},
		{name: "repair required", id: "repair-required", report: AssuranceReport{Readiness: ProviderReadinessRepairRequired}, want: ProviderReadinessRepairRequired},
		{name: "available cannot be repair", id: "available-repair", report: AssuranceReport{Available: true, Readiness: ProviderReadinessRepairRequired}, wantError: ErrProviderFailure},
		{name: "unavailable cannot be ready", id: "unavailable-ready", report: AssuranceReport{Readiness: ProviderReadinessReady}, wantError: ErrProviderFailure},
		{name: "unknown readiness", id: "unknown-readiness", report: AssuranceReport{Readiness: ProviderReadiness("unknown")}, wantError: ErrProviderFailure},
	} {
		t.Run(test.name, func(t *testing.T) {
			provider := registryProviderReady(ProviderID(test.id))
			provider.probe = test.report
			registry, err := NewRegistry(1, registryRegistration(ProviderID(test.id), "1", provider))
			if err != nil {
				t.Fatal(err)
			}
			report, probeErr := registry.Probe(context.Background(), ProviderID(test.id), "1")
			if test.wantError != nil {
				if !errors.Is(probeErr, test.wantError) {
					t.Fatalf("err=%v want %v report=%#v", probeErr, test.wantError, report)
				}
				return
			}
			if probeErr != nil || report.Readiness != test.want {
				t.Fatalf("report=%#v err=%v want readiness=%q", report, probeErr, test.want)
			}
		})
	}
}

func TestRegistryStartRevalidatesSpecProbeAndSession(t *testing.T) {
	provider := registryProviderReady("ready")
	registry, err := NewRegistry(2, registryRegistration("ready", "1", provider))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Start(context.Background(), "ready", "1", SessionSpec{}); !errors.Is(err, ErrInvalidSpec) {
		t.Fatalf("invalid spec error=%v", err)
	}
	if provider.probes != 0 || provider.starts != 0 {
		t.Fatal("provider called before spec validation")
	}
	weak := registryProviderReady("weak")
	weak.probe.Actual = Assurance{Level: AssuranceProcess, SharedKernel: true}
	weak.probe.LimitsEnforced = false
	registry, err = NewRegistry(2, registryRegistration("weak", "1", weak))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Start(context.Background(), "weak", "1", validSpec()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("weak probe error=%v", err)
	}
	if weak.starts != 0 {
		t.Fatal("provider start bypassed weak probe")
	}
	registry, err = NewRegistry(2, registryRegistration("ready", "1", provider))
	if err != nil {
		t.Fatal(err)
	}
	provider.nilSession = true
	if _, err := registry.Start(context.Background(), "ready", "1", validSpec()); !errors.Is(err, ErrProviderFailure) {
		t.Fatalf("nil session error=%v", err)
	}
}

func TestRegistryStartSanitizesProviderErrorAndPanic(t *testing.T) {
	provider := registryProviderReady("failing")
	provider.startErr = errors.New("secret endpoint token")
	registry, err := NewRegistry(2, registryRegistration("failing", "1", provider))
	if err != nil {
		t.Fatal(err)
	}
	_, err = registry.Start(context.Background(), "failing", "1", validSpec())
	if !errors.Is(err, ErrProviderFailure) || strings.Contains(err.Error(), "secret") {
		t.Fatalf("provider error=%v", err)
	}
	provider.startErr = nil
	provider.startPanic = true
	_, err = registry.Start(context.Background(), "failing", "1", validSpec())
	if !errors.Is(err, ErrProviderPanic) || strings.Contains(err.Error(), "secret") {
		t.Fatalf("provider panic error=%v", err)
	}
}

func TestRegistryStartWithOwnershipIsInvocationScoped(t *testing.T) {
	preflight := &rootOwnershipRegistryProvider{registryProvider: registryProviderReady("owned-preflight"), owned: true}
	preflight.probe = AssuranceReport{UnavailableCause: "not ready"}
	registry, err := NewRegistry(3, registryRegistration("owned-preflight", "1", preflight))
	if err != nil {
		t.Fatal(err)
	}
	if _, owned, err := registry.StartWithOwnership(context.Background(), "owned-preflight", "1", validSpec()); !errors.Is(err, ErrUnavailable) || owned {
		t.Fatalf("preflight StartWithOwnership owned=%v err=%v", owned, err)
	}
	if preflight.starts != 0 {
		t.Fatalf("preflight failure invoked provider Start %d times", preflight.starts)
	}

	partial := &rootOwnershipRegistryProvider{registryProvider: registryProviderReady("owned-partial"), owned: true}
	partial.startErr = errors.New("adopted root cleanup pending")
	registry, err = NewRegistry(3, registryRegistration("owned-partial", "1", partial))
	if err != nil {
		t.Fatal(err)
	}
	if _, owned, err := registry.StartWithOwnership(context.Background(), "owned-partial", "1", validSpec()); !errors.Is(err, ErrProviderFailure) || !owned {
		t.Fatalf("partial StartWithOwnership owned=%v err=%v", owned, err)
	}

	ordinary := registryProviderReady("ordinary-partial")
	ordinary.startErr = errors.New("ordinary cleanup")
	registry, err = NewRegistry(3, registryRegistration("ordinary-partial", "1", ordinary))
	if err != nil {
		t.Fatal(err)
	}
	if _, owned, err := registry.StartWithOwnership(context.Background(), "ordinary-partial", "1", validSpec()); !errors.Is(err, ErrProviderFailure) || owned {
		t.Fatalf("ordinary StartWithOwnership owned=%v err=%v", owned, err)
	}
}

func TestRegistryConditionallyGuardsPublicationFinalizer(t *testing.T) {
	regularProvider := registryProviderReady("regular")
	regularProvider.session = &registrySession{report: regularProvider.probe, limits: validSpec().Limits}
	registry, err := NewRegistry(2, registryRegistration("regular", "1", regularProvider))
	if err != nil {
		t.Fatal(err)
	}
	regular, err := registry.Start(context.Background(), "regular", "1", validSpec())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := regular.(PublicationFinalizer); ok {
		t.Fatal("ordinary raw session unexpectedly exposes PublicationFinalizer")
	}

	awareProvider := registryProviderReady("aware")
	aware := &publicationSession{registrySession: registrySession{report: awareProvider.probe, limits: validSpec().Limits}}
	awareProvider.session = aware
	registry, err = NewRegistry(2, registryRegistration("aware", "1", awareProvider))
	if err != nil {
		t.Fatal(err)
	}
	session, err := registry.Start(context.Background(), "aware", "1", validSpec())
	if err != nil {
		t.Fatal(err)
	}
	finalizer, ok := session.(PublicationFinalizer)
	if !ok {
		t.Fatal("publication-aware raw session lost its capability")
	}
	if err := finalizer.FinalizePublication(context.Background(), PublicationOutcome("invalid")); !errors.Is(err, ErrInvalidSpec) {
		t.Fatalf("invalid outcome err=%v", err)
	}
	if len(aware.outcomes) != 0 {
		t.Fatalf("invalid outcome reached provider: %#v", aware.outcomes)
	}

	aware.finalizeErrs = []error{errors.New("provider publication secret")}
	if err := finalizer.FinalizePublication(context.Background(), PublicationFailed); !errors.Is(err, ErrProviderFailure) || strings.Contains(err.Error(), "secret") {
		t.Fatalf("sanitized finalization err=%v", err)
	}
	if err := finalizer.FinalizePublication(context.Background(), PublicationFailed); err != nil {
		t.Fatalf("same failed outcome did not retry safely: %v", err)
	}
	if err := finalizer.FinalizePublication(context.Background(), PublicationFailed); err != nil {
		t.Fatalf("completed same outcome was not idempotent: %v", err)
	}
	if got := len(aware.outcomes); got != 2 {
		t.Fatalf("provider finalization calls=%d want 2", got)
	}
	if err := finalizer.FinalizePublication(context.Background(), PublicationPublished); !errors.Is(err, ErrProviderFailure) {
		t.Fatalf("different terminal outcome err=%v", err)
	}
	if got := len(aware.outcomes); got != 2 {
		t.Fatalf("different outcome reached provider: calls=%d", got)
	}
}

func TestRegistryPublicationFinalizerSanitizesPanic(t *testing.T) {
	provider := registryProviderReady("panic-finalizer")
	provider.session = &publicationSession{
		registrySession: registrySession{report: provider.probe, limits: validSpec().Limits},
		finalizePanic:   true,
	}
	registry, err := NewRegistry(1, registryRegistration("panic-finalizer", "1", provider))
	if err != nil {
		t.Fatal(err)
	}
	session, err := registry.Start(context.Background(), "panic-finalizer", "1", validSpec())
	if err != nil {
		t.Fatal(err)
	}
	finalizer := session.(PublicationFinalizer)
	if err := finalizer.FinalizePublication(context.Background(), PublicationFailed); !errors.Is(err, ErrProviderPanic) || strings.Contains(err.Error(), "secret") {
		t.Fatalf("panic finalization err=%v", err)
	}
}

func TestPublicationTerminalFenceRejectsNewRunAndExecAfterRetryableFailure(t *testing.T) {
	provider := registryProviderReady("publication-fence")
	spec := validSpec()
	spec.Network = NetworkDisabled
	spec.RequestedAssurance.NetworkIsolation = true
	provider.probe.Network = NetworkDisabled
	provider.probe.Actual.NetworkIsolation = true
	provider.probe.SupportedNetworks = []NetworkPolicy{NetworkDisabled}
	raw := &publicationFenceSession{
		report:       provider.probe,
		limits:       spec.Limits,
		finalizeErrs: []error{errors.New("first finalization failure")},
	}
	provider.session = raw
	registry, err := NewRegistry(1, registryRegistration("publication-fence", "1", provider))
	if err != nil {
		t.Fatal(err)
	}
	session, err := registry.Start(context.Background(), "publication-fence", "1", spec)
	if err != nil {
		t.Fatal(err)
	}
	finalizer := session.(PublicationFinalizer)
	if err := finalizer.FinalizePublication(context.Background(), PublicationFailed); !errors.Is(err, ErrProviderFailure) {
		t.Fatalf("first finalization err=%v", err)
	}
	if _, err := session.Run(context.Background(), Command{Args: []string{"echo"}}); !errors.Is(err, ErrLeaseTerminated) {
		t.Fatalf("Run after terminal selection err=%v", err)
	}
	execSession := session.(ExecSession)
	request := ExecRequest{Args: []string{"echo"}, Limits: spec.Limits, Network: spec.Network, ArtifactPolicy: spec.ArtifactPolicy, Lease: spec.Lease}
	if _, err := execSession.Exec(context.Background(), request); !errors.Is(err, ErrLeaseTerminated) {
		t.Fatalf("Exec after terminal selection err=%v", err)
	}
	if raw.runs != 0 || raw.execs != 0 {
		t.Fatalf("terminal fence reached raw callbacks: runs=%d execs=%d", raw.runs, raw.execs)
	}
	if err := finalizer.FinalizePublication(context.Background(), PublicationFailed); err != nil {
		t.Fatalf("same-outcome finalization retry err=%v", err)
	}
}

func TestPublicationFinalizerSerializesBehindActiveExec(t *testing.T) {
	provider := registryProviderReady("publication-serial")
	spec := validSpec()
	spec.Network = NetworkDisabled
	spec.RequestedAssurance.NetworkIsolation = true
	provider.probe.Network = NetworkDisabled
	provider.probe.Actual.NetworkIsolation = true
	provider.probe.SupportedNetworks = []NetworkPolicy{NetworkDisabled}
	raw := &publicationFenceSession{
		report:          provider.probe,
		limits:          spec.Limits,
		execEntered:     make(chan struct{}),
		releaseExec:     make(chan struct{}),
		finalizeEntered: make(chan struct{}),
	}
	provider.session = raw
	registry, err := NewRegistry(1, registryRegistration("publication-serial", "1", provider))
	if err != nil {
		t.Fatal(err)
	}
	session, err := registry.Start(context.Background(), "publication-serial", "1", spec)
	if err != nil {
		t.Fatal(err)
	}
	request := ExecRequest{Args: []string{"echo"}, Limits: spec.Limits, Network: spec.Network, ArtifactPolicy: spec.ArtifactPolicy, Lease: spec.Lease}
	execDone := make(chan error, 1)
	go func() {
		_, execErr := session.(ExecSession).Exec(context.Background(), request)
		execDone <- execErr
	}()
	select {
	case <-raw.execEntered:
	case <-time.After(time.Second):
		t.Fatal("raw Exec did not start")
	}
	finalizer := session.(PublicationFinalizer)
	wrapped := finalizer.(*guardedPublicationFinalizer)
	finalizerWaiting := make(chan struct{})
	wrapped.beforeRunLockForTest = func() { close(finalizerWaiting) }
	finalizeDone := make(chan error, 1)
	go func() { finalizeDone <- finalizer.FinalizePublication(context.Background(), PublicationFailed) }()
	select {
	case <-finalizerWaiting:
	case <-time.After(time.Second):
		t.Fatal("finalizer did not attempt to acquire runMu")
	}
	wrapped.stateMu.Lock()
	terminal := wrapped.publicationTerminal
	wrapped.stateMu.Unlock()
	if terminal {
		t.Fatal("finalizer selected its outcome before active Exec released runMu")
	}
	select {
	case <-raw.finalizeEntered:
		t.Fatal("raw finalizer ran before active Exec released its callback")
	default:
	}
	close(raw.releaseExec)
	if err := <-execDone; err != nil {
		t.Fatalf("Exec err=%v", err)
	}
	select {
	case <-raw.finalizeEntered:
	case <-time.After(time.Second):
		t.Fatal("raw finalizer did not run after Exec completed")
	}
	if err := <-finalizeDone; err != nil {
		t.Fatalf("FinalizePublication err=%v", err)
	}
}

func TestPublicationFinalizerDoesNotOvertakeExecAdmittedBeforeCallback(t *testing.T) {
	provider := registryProviderReady("publication-admission")
	spec := validSpec()
	spec.Network = NetworkDisabled
	spec.RequestedAssurance.NetworkIsolation = true
	provider.probe.Network = NetworkDisabled
	provider.probe.Actual.NetworkIsolation = true
	provider.probe.SupportedNetworks = []NetworkPolicy{NetworkDisabled}
	admitted := make(chan struct{})
	releaseAdmission := make(chan struct{})
	raw := &publicationFenceSession{
		report:          provider.probe,
		limits:          spec.Limits,
		finalizeEntered: make(chan struct{}),
	}
	provider.session = raw
	registry, err := NewRegistry(1, registryRegistration("publication-admission", "1", provider))
	if err != nil {
		t.Fatal(err)
	}
	session, err := registry.Start(context.Background(), "publication-admission", "1", spec)
	if err != nil {
		t.Fatal(err)
	}
	finalizer := session.(PublicationFinalizer)
	wrapped := finalizer.(*guardedPublicationFinalizer)
	wrapped.afterBeginRunForTest = func() {
		close(admitted)
		<-releaseAdmission
	}
	request := ExecRequest{Args: []string{"echo"}, Limits: spec.Limits, Network: spec.Network, ArtifactPolicy: spec.ArtifactPolicy, Lease: spec.Lease}
	execDone := make(chan error, 1)
	go func() {
		_, execErr := session.(ExecSession).Exec(context.Background(), request)
		execDone <- execErr
	}()
	select {
	case <-admitted:
	case <-time.After(time.Second):
		t.Fatal("Exec did not reach the admitted-before-callback seam")
	}
	finalizerWaiting := make(chan struct{})
	wrapped.beforeRunLockForTest = func() { close(finalizerWaiting) }
	finalizeDone := make(chan error, 1)
	go func() { finalizeDone <- finalizer.FinalizePublication(context.Background(), PublicationFailed) }()
	select {
	case <-finalizerWaiting:
	case <-time.After(time.Second):
		t.Fatal("finalizer did not attempt to acquire runMu")
	}
	wrapped.stateMu.Lock()
	terminal := wrapped.publicationTerminal
	wrapped.stateMu.Unlock()
	if terminal || raw.execs != 0 {
		t.Fatalf("finalizer overtook admitted Exec: terminal=%v raw_execs=%d", terminal, raw.execs)
	}
	select {
	case <-raw.finalizeEntered:
		t.Fatal("raw finalizer ran before admitted Exec reached callback")
	default:
	}
	close(releaseAdmission)
	select {
	case err := <-execDone:
		if err != nil {
			t.Fatalf("Exec err=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("admitted Exec did not complete")
	}
	select {
	case <-raw.finalizeEntered:
	case <-time.After(time.Second):
		t.Fatal("raw finalizer did not run after admitted Exec")
	}
	if raw.execs != 1 {
		t.Fatalf("raw Exec calls=%d want 1", raw.execs)
	}
	if err := <-finalizeDone; err != nil {
		t.Fatalf("FinalizePublication err=%v", err)
	}
}

func TestNewLocalRegistrationIsExplicitlyFailClosed(t *testing.T) {
	registration := NewLocalRegistration()
	if registration.Metadata.ID != (LocalProvider{}).ID() || registration.Metadata.Version == "" || registration.Provider == nil {
		t.Fatalf("invalid local registration=%#v", registration)
	}
	registry, err := NewRegistry(1, registration)
	if err != nil {
		t.Fatal(err)
	}
	if report, err := registry.Probe(context.Background(), registration.Metadata.ID, registration.Metadata.Version); err != nil || (runtime.GOOS == "windows" && report.Available) {
		t.Fatalf("local registry probe report=%#v err=%v", report, err)
	}
}

func TestNewLocalRegistrationForWorkRootRequiresCanonicalAbsolutePath(t *testing.T) {
	root := filepath.Join(t.TempDir(), "sandbox")
	registration, err := NewLocalRegistrationForWorkRoot(root)
	if err != nil {
		t.Fatalf("NewLocalRegistrationForWorkRoot(%q): %v", root, err)
	}
	if registration.Metadata.ID != (LocalProvider{}).ID() || registration.Metadata.Version == "" || registration.Provider == nil {
		t.Fatalf("invalid local registration=%#v", registration)
	}
	if _, err := NewRegistry(1, registration); err != nil {
		t.Fatalf("registration must compose into a registry: %v", err)
	}

	for _, invalid := range []string{
		"relative-sandbox-root",
		root + string(filepath.Separator) + ".",
		root + string([]byte{0}),
	} {
		if _, err := NewLocalRegistrationForWorkRoot(invalid); err == nil {
			t.Fatalf("NewLocalRegistrationForWorkRoot(%q) succeeded", invalid)
		}
	}
}

func TestRegistryRejectsSessionAssuranceAndLimitMismatch(t *testing.T) {
	provider := registryProviderReady("session")
	provider.session = &maliciousSession{
		limits: validSpec().Limits,
		report: AssuranceReport{Available: true, Actual: Assurance{Level: AssuranceProcess, SharedKernel: true}, Network: NetworkDisabled, LimitsEnforced: true, MountsEnforced: true},
	}
	registry, err := NewRegistry(1, registryRegistration("session", "1", provider))
	if err != nil {
		t.Fatal(err)
	}
	malicious := provider.session.(*maliciousSession)
	if _, err := registry.Start(context.Background(), "session", "1", validSpec()); !errors.Is(err, ErrAssuranceTooWeak) || malicious.closes != 1 {
		t.Fatalf("assurance mismatch err=%v closes=%d", err, malicious.closes)
	}
	malicious.report.Network = NetworkHost
	malicious.limits.MaxOutputBytes++
	if _, err := registry.Start(context.Background(), "session", "1", validSpec()); !errors.Is(err, ErrProviderFailure) || malicious.closes != 2 {
		t.Fatalf("limits mismatch err=%v closes=%d", err, malicious.closes)
	}
	malicious.limits = validSpec().Limits
	malicious.assurancePanic = true
	if _, err := registry.Start(context.Background(), "session", "1", validSpec()); !errors.Is(err, ErrProviderPanic) || malicious.closes != 3 {
		t.Fatalf("assurance panic err=%v closes=%d", err, malicious.closes)
	}
	malicious.assurancePanic = false
	malicious.limitsPanic = true
	if _, err := registry.Start(context.Background(), "session", "1", validSpec()); !errors.Is(err, ErrProviderPanic) || malicious.closes != 4 {
		t.Fatalf("limits panic err=%v closes=%d", err, malicious.closes)
	}
}

func TestGuardedSessionRunCloseAndArtifactContract(t *testing.T) {
	provider := registryProviderReady("guarded")
	malicious := &maliciousSession{
		limits: validSpec().Limits,
		report: AssuranceReport{Available: true, Actual: Assurance{Level: AssuranceProcess, SharedKernel: true}, Network: NetworkHost, LimitsEnforced: true, MountsEnforced: true},
	}
	provider.session = malicious
	registry, err := NewRegistry(1, registryRegistration("guarded", "1", provider))
	if err != nil {
		t.Fatal(err)
	}
	guarded, err := registry.Start(context.Background(), "guarded", "1", validSpec())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := guarded.Run(context.Background(), Command{}); !errors.Is(err, ErrInvalidSpec) || malicious.runs != 0 {
		t.Fatalf("invalid command err=%v runs=%d", err, malicious.runs)
	}
	malicious.runErr = errors.New("provider secret body")
	if _, err := guarded.Run(context.Background(), Command{Args: []string{"echo"}}); !errors.Is(err, ErrProviderFailure) || strings.Contains(err.Error(), "secret") {
		t.Fatalf("run error=%v", err)
	}
	malicious.runErr = nil
	malicious.runPanic = true
	if _, err := guarded.Run(context.Background(), Command{Args: []string{"echo"}}); !errors.Is(err, ErrProviderPanic) || strings.Contains(err.Error(), "secret") {
		t.Fatalf("run panic=%v", err)
	}
	malicious.runPanic = false
	digest := strings.Repeat("a", 64)
	malicious.result = ArtifactSet{Items: []Artifact{{Key: "result", Digest: digest, Size: 1, Verified: false}}}
	if _, err := guarded.Run(context.Background(), Command{Args: []string{"echo"}}); !errors.Is(err, ErrArtifactUnverified) {
		t.Fatalf("invalid artifact err=%v", err)
	}
	malicious.result = ArtifactSet{Items: []Artifact{{Key: "result", Digest: digest, Size: 1, Verified: true}}}
	result, err := guarded.Run(context.Background(), Command{Args: []string{"echo"}})
	if err != nil || len(result.Items) != 1 {
		t.Fatalf("valid artifact result=%#v err=%v", result, err)
	}
	result.Items[0].Key = "changed"
	if malicious.result.Items[0].Key == "changed" {
		t.Fatal("artifact result was not copied")
	}
	if err := guarded.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := guarded.Close(context.Background()); err != nil || malicious.closes != 1 {
		t.Fatalf("close idempotence err=%v closes=%d", err, malicious.closes)
	}
	if _, err := guarded.Run(context.Background(), Command{Args: []string{"echo"}}); !errors.Is(err, ErrLeaseTerminated) {
		t.Fatalf("run after close err=%v", err)
	}
}

func TestGuardedSessionCloseContainsPanicAndError(t *testing.T) {
	for _, tc := range []struct {
		name       string
		panicClose bool
		errorClose error
		want       error
	}{
		{name: "panic", panicClose: true, want: ErrProviderPanic},
		{name: "error", errorClose: errors.New("secret close error"), want: ErrProviderFailure},
	} {
		t.Run(tc.name, func(t *testing.T) {
			provider := registryProviderReady("close")
			malicious := &maliciousSession{limits: validSpec().Limits, report: AssuranceReport{Available: true, Actual: Assurance{Level: AssuranceProcess, SharedKernel: true}, Network: NetworkHost, LimitsEnforced: true, MountsEnforced: true}, closePanic: tc.panicClose, closeErr: tc.errorClose}
			provider.session = malicious
			registry, err := NewRegistry(1, registryRegistration("close", "1", provider))
			if err != nil {
				t.Fatal(err)
			}
			guarded, err := registry.Start(context.Background(), "close", "1", validSpec())
			if err != nil {
				t.Fatal(err)
			}
			if err := guarded.Close(context.Background()); !errors.Is(err, tc.want) || strings.Contains(err.Error(), "secret") {
				t.Fatalf("close err=%v", err)
			}
		})
	}
}

func TestGuardedCloseRetriesAfterUnconfirmedFailure(t *testing.T) {
	provider := registryProviderReady("close-retry")
	raw := &retryCloseSession{
		report:        provider.probe,
		limits:        validSpec().Limits,
		firstStarted:  make(chan struct{}),
		secondStarted: make(chan struct{}),
		releaseSecond: make(chan struct{}),
	}
	provider.session = raw
	registry, err := NewRegistry(1, registryRegistration("close-retry", "1", provider))
	if err != nil {
		t.Fatal(err)
	}
	guarded, err := registry.Start(context.Background(), "close-retry", "1", validSpec())
	if err != nil {
		t.Fatal(err)
	}
	if err := guarded.Close(context.Background()); !errors.Is(err, ErrProviderFailure) {
		t.Fatalf("first close error=%v", err)
	}
	if _, err := guarded.Run(context.Background(), Command{Args: []string{"echo"}}); !errors.Is(err, ErrLeaseTerminated) {
		t.Fatalf("run after failed close=%v", err)
	}
	secondDone := make(chan error, 1)
	go func() { secondDone <- guarded.Close(context.Background()) }()
	select {
	case <-raw.secondStarted:
	case <-time.After(time.Second):
		t.Fatal("second Close did not retry raw Close")
	}
	select {
	case err := <-secondDone:
		t.Fatalf("retry Close returned before raw termination: %v", err)
	default:
	}
	close(raw.releaseSecond)
	if err := <-secondDone; err != nil {
		t.Fatalf("retry Close error=%v", err)
	}
	if got := raw.closeCalls.Load(); got != 2 {
		t.Fatalf("raw Close calls=%d, want 2", got)
	}
	if err := guarded.Close(context.Background()); err != nil {
		t.Fatalf("completed Close error=%v", err)
	}
	if got := raw.closeCalls.Load(); got != 2 {
		t.Fatalf("completed Close retried raw Close: calls=%d", got)
	}
}

func TestGuardedConcurrentCloseSharesCompletion(t *testing.T) {
	provider := registryProviderReady("close-concurrent")
	raw := &blockingCloseSession{
		report:       provider.probe,
		limits:       validSpec().Limits,
		closeStarted: make(chan struct{}),
		releaseClose: make(chan struct{}),
	}
	provider.session = raw
	registry, err := NewRegistry(1, registryRegistration("close-concurrent", "1", provider))
	if err != nil {
		t.Fatal(err)
	}
	guarded, err := registry.Start(context.Background(), "close-concurrent", "1", validSpec())
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 2)
	go func() { results <- guarded.Close(context.Background()) }()
	<-raw.closeStarted
	go func() { results <- guarded.Close(context.Background()) }()
	select {
	case err := <-results:
		t.Fatalf("concurrent Close returned before shared completion: %v", err)
	default:
	}
	close(raw.releaseClose)
	for i := 0; i < 2; i++ {
		if err := <-results; err != nil {
			t.Fatalf("concurrent Close error=%v", err)
		}
	}
	if got := raw.closeCalls.Load(); got != 1 {
		t.Fatalf("concurrent Close invoked raw %d times", got)
	}
}

func TestRegistryStartClonesSessionSpecMounts(t *testing.T) {
	provider := registryProviderReady("spec-clone")
	provider.captureSpec = true
	registry, err := NewRegistry(1, registryRegistration("spec-clone", "1", provider))
	if err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(t.TempDir(), "source")
	mounts := []Mount{{Source: source, Target: "/workspace", ReadOnly: true}}
	spec := validSpec()
	spec.Mounts = mounts
	guarded, err := registry.Start(context.Background(), "spec-clone", "1", spec)
	if err != nil {
		t.Fatal(err)
	}
	mounts[0].Target = "/other"
	mounts[0].Source = filepath.Join(t.TempDir(), "changed")
	if provider.startSpec.Mounts[0].Target != "/workspace" || provider.startSpec.Mounts[0].Source != source {
		t.Fatalf("provider received aliased mounts: %#v", provider.startSpec.Mounts)
	}
	owned := guarded.(*guardedSession)
	if owned.spec.Mounts[0].Target != "/workspace" || owned.spec.Mounts[0].Source != source {
		t.Fatalf("guarded session received aliased mounts: %#v", owned.spec.Mounts)
	}
}

func TestGuardedCloseFencesAndInterruptsActiveRun(t *testing.T) {
	provider := registryProviderReady("blocking")
	blocking := &blockingSession{
		limits:     validSpec().Limits,
		report:     AssuranceReport{Available: true, Actual: Assurance{Level: AssuranceProcess, SharedKernel: true}, Network: NetworkHost, LimitsEnforced: true, MountsEnforced: true},
		runEntered: make(chan struct{}), releaseRun: make(chan struct{}), closeCalled: make(chan struct{}), allowClose: make(chan struct{}),
	}
	provider.session = blocking
	registry, err := NewRegistry(1, registryRegistration("blocking", "1", provider))
	if err != nil {
		t.Fatal(err)
	}
	guarded, err := registry.Start(context.Background(), "blocking", "1", validSpec())
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() {
		_, runErr := guarded.Run(context.Background(), Command{Args: []string{"echo"}})
		runDone <- runErr
	}()
	<-blocking.runEntered
	closeDone := make(chan error, 1)
	go func() { closeDone <- guarded.Close(context.Background()) }()
	<-blocking.closeCalled
	select {
	case err := <-closeDone:
		t.Fatalf("close returned before provider close completed: %v", err)
	default:
	}
	close(blocking.allowClose)
	if err := <-closeDone; err != nil {
		t.Fatalf("close error=%v", err)
	}
	if err := <-runDone; !errors.Is(err, ErrLeaseTerminated) {
		t.Fatalf("active run after fence error=%v", err)
	}
	if err := guarded.Close(context.Background()); err != nil {
		t.Fatalf("second close error=%v", err)
	}
}

func TestGuardedCloseCanceledCallerStillFencesAndReportsContext(t *testing.T) {
	provider := registryProviderReady("canceled-close")
	malicious := &maliciousSession{limits: validSpec().Limits, report: provider.probe}
	provider.session = malicious
	registry, err := NewRegistry(1, registryRegistration("canceled-close", "1", provider))
	if err != nil {
		t.Fatal(err)
	}
	guarded, err := registry.Start(context.Background(), "canceled-close", "1", validSpec())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := guarded.Close(ctx); !errors.Is(err, context.Canceled) || malicious.closes != 1 {
		t.Fatalf("canceled close err=%v closes=%d", err, malicious.closes)
	}
	if _, err := guarded.Run(context.Background(), Command{Args: []string{"echo"}}); !errors.Is(err, ErrLeaseTerminated) {
		t.Fatalf("run after canceled close err=%v", err)
	}
}

func TestContractMismatchCleanupUsesIndependentContext(t *testing.T) {
	provider := registryProviderReady("cleanup")
	malicious := &maliciousSession{limits: validSpec().Limits, report: AssuranceReport{Available: true, Actual: Assurance{Level: AssuranceProcess, SharedKernel: true}, Network: NetworkDisabled, LimitsEnforced: true, MountsEnforced: true}}
	provider.session = malicious
	ctx, cancel := context.WithCancel(context.Background())
	provider.cancel = cancel
	registry, err := NewRegistry(1, registryRegistration("cleanup", "1", provider))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Start(ctx, "cleanup", "1", validSpec()); !errors.Is(err, ErrAssuranceTooWeak) || malicious.closes != 1 {
		t.Fatalf("canceled contract cleanup err=%v closes=%d", err, malicious.closes)
	}
}

func TestGuardedPublicMetadataUsesStartSnapshotDuringClose(t *testing.T) {
	provider := registryProviderReady("metadata")
	tracked := &metadataTrackingSession{limits: validSpec().Limits, report: provider.probe}
	provider.session = tracked
	registry, err := NewRegistry(1, registryRegistration("metadata", "1", provider))
	if err != nil {
		t.Fatal(err)
	}
	guarded, err := registry.Start(context.Background(), "metadata", "1", validSpec())
	if err != nil {
		t.Fatal(err)
	}
	if tracked.assuranceCalls.Load() != 1 || tracked.limitsCalls.Load() != 1 {
		t.Fatalf("start metadata calls assurance=%d limits=%d", tracked.assuranceCalls.Load(), tracked.limitsCalls.Load())
	}
	var wait sync.WaitGroup
	for i := 0; i < 32; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_ = guarded.Assurance()
			_ = guarded.Limits()
		}()
	}
	wait.Add(1)
	go func() {
		defer wait.Done()
		_ = guarded.Close(context.Background())
	}()
	wait.Wait()
	if tracked.assuranceCalls.Load() != 1 || tracked.limitsCalls.Load() != 1 {
		t.Fatalf("public metadata called raw after start assurance=%d limits=%d", tracked.assuranceCalls.Load(), tracked.limitsCalls.Load())
	}
}
