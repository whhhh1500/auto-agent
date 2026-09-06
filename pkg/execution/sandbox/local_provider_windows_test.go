//go:build windows

package sandbox

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeWindowsBackend struct {
	mu sync.Mutex

	probeErr            error
	probeActiveErr      error
	startErr            error
	startSessionErr     error
	session             *fakeWindowsBackendSession
	starts              []windowsBackendStart
	probeCalls          int
	probeActiveCalls    int
	startCalls          int
	probeDeadline       bool
	probeActiveDeadline bool
	startDeadline       bool
	onStart             func()
}

type recoveringWindowsBackend struct {
	*fakeWindowsBackend

	recoverErr     error
	recoverCalls   int
	recoverEntered chan struct{}
	releaseRecover chan struct{}
}

func (backend *recoveringWindowsBackend) Recover(ctx context.Context) error {
	backend.mu.Lock()
	backend.recoverCalls++
	entered, release, err := backend.recoverEntered, backend.releaseRecover, backend.recoverErr
	backend.mu.Unlock()
	if entered != nil {
		close(entered)
	}
	if release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return err
}

func (backend *fakeWindowsBackend) Probe(ctx context.Context) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.probeCalls++
	_, backend.probeDeadline = ctx.Deadline()
	return backend.probeErr
}
func (backend *fakeWindowsBackend) ProbeActive(ctx context.Context) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.probeActiveCalls++
	_, backend.probeActiveDeadline = ctx.Deadline()
	return backend.probeActiveErr
}
func (backend *fakeWindowsBackend) Start(ctx context.Context, start windowsBackendStart) (windowsBackendSession, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.startCalls++
	_, backend.startDeadline = ctx.Deadline()
	onStart := backend.onStart
	if backend.startErr != nil {
		return nil, backend.startErr
	}
	backend.starts = append(backend.starts, start)
	if backend.session == nil {
		backend.session = &fakeWindowsBackendSession{}
	}
	session := backend.session
	err := backend.startSessionErr
	if onStart != nil {
		onStart()
	}
	return session, err
}

type fakeWindowsBackendSession struct {
	mu sync.Mutex

	stdout, stderr []byte
	artifacts      ArtifactSet
	executeErr     error
	finalizeErrs   []error
	abortErr       error
	abortWait      bool
	closeErrs      []error
	poisonErr      error
	events         []string
	executeEntered chan struct{}
	releaseExecute chan struct{}
}

func (session *fakeWindowsBackendSession) Execute(_ context.Context, _ windowsBackendExec, stdout, stderr io.Writer) (windowsBackendExecResult, error) {
	session.mu.Lock()
	session.events = append(session.events, "execute")
	standardOut := append([]byte(nil), session.stdout...)
	standardErr := append([]byte(nil), session.stderr...)
	err := session.executeErr
	artifacts := cloneArtifactSet(session.artifacts)
	executeEntered := session.executeEntered
	releaseExecute := session.releaseExecute
	session.mu.Unlock()
	if executeEntered != nil {
		close(executeEntered)
		<-releaseExecute
	}
	if len(standardOut) != 0 {
		_, _ = stdout.Write(standardOut)
	}
	if len(standardErr) != 0 {
		_, _ = stderr.Write(standardErr)
	}
	return windowsBackendExecResult{Artifacts: artifacts}, err
}
func (session *fakeWindowsBackendSession) Terminate(context.Context) error {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.events = append(session.events, "terminate")
	return nil
}
func (session *fakeWindowsBackendSession) Finalize(_ context.Context, outcome PublicationOutcome) error {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.events = append(session.events, "finalize:"+string(outcome))
	if len(session.finalizeErrs) == 0 {
		return nil
	}
	err := session.finalizeErrs[0]
	session.finalizeErrs = session.finalizeErrs[1:]
	return err
}
func (session *fakeWindowsBackendSession) Abort(ctx context.Context) error {
	session.mu.Lock()
	session.events = append(session.events, "abort")
	wait := session.abortWait
	err := session.abortErr
	session.mu.Unlock()
	if wait {
		<-ctx.Done()
		return ctx.Err()
	}
	return err
}
func (session *fakeWindowsBackendSession) Close(context.Context) error {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.events = append(session.events, "close")
	if len(session.closeErrs) == 0 {
		return nil
	}
	err := session.closeErrs[0]
	session.closeErrs = session.closeErrs[1:]
	return err
}
func (session *fakeWindowsBackendSession) Poison(context.Context) error {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.events = append(session.events, "poison")
	return session.poisonErr
}

func windowsProviderTestSpec(t *testing.T) (SessionSpec, string) {
	t.Helper()
	root := t.TempDir()
	for _, name := range []string{"workspace", "artifacts", "home", "tmp", "cache"} {
		if err := os.Mkdir(filepath.Join(root, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	workspace := filepath.Join(root, "workspace")
	artifacts := filepath.Join(root, "artifacts")
	return SessionSpec{
		RequestedAssurance: Assurance{Level: AssuranceProcess, SharedKernel: true, NetworkIsolation: false},
		Limits:             Limits{WallTime: time.Second, MaxCPUTime: time.Second, MaxMemoryBytes: 1 << 20, MaxOutputBytes: 8},
		Network:            NetworkHost,
		ArtifactPolicy:     ArtifactPolicy{MaxArtifacts: 2, MaxTotalBytes: 2048},
		Lease:              LeaseIdentity{RunID: "run", SegmentID: "segment", ModuleID: "module", CompositionRev: "revision", Token: "token"},
		Mounts:             []Mount{{Source: workspace, Target: "/workspace"}, {Source: artifacts, Target: "/artifacts"}},
	}, root
}

func TestWindowsLocalProviderFailsClosedAndUsesOneAdmission(t *testing.T) {
	if got := NewLocalRegistration().Metadata.ImplementationRevision; got != windowsLocalProviderRevision {
		t.Fatalf("windows provider revision=%q", got)
	}
	backend := &fakeWindowsBackend{}
	provider := newWindowsLocalProviderForTest(backend)
	spec, _ := windowsProviderTestSpec(t)
	if report := provider.Probe(context.Background()); !report.Available || report.Network != NetworkHost || report.Actual.NetworkIsolation || !report.LimitsEnforced || !report.MountsEnforced {
		t.Fatalf("ready report=%#v", report)
	}
	first, err := provider.Start(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	blocked, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := provider.Start(blocked, spec); !errors.Is(err, context.Canceled) {
		t.Fatalf("admission cancellation err=%v", err)
	}
	if err := first.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	second, err := provider.Start(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	starts := append([]windowsBackendStart(nil), backend.starts...)
	probeCalls := backend.probeCalls
	startCalls := backend.startCalls
	probeDeadline := backend.probeDeadline
	startDeadline := backend.startDeadline
	backend.mu.Unlock()
	if len(starts) != 2 || startCalls != 2 {
		t.Fatalf("backend starts=%d calls=%d", len(starts), startCalls)
	}
	if probeCalls != 1 || !probeDeadline || !startDeadline {
		t.Fatalf("backend calls were not bounded and atomic: probe=%d probeDeadline=%v startDeadline=%v", probeCalls, probeDeadline, startDeadline)
	}
	start := starts[0]
	if start.Workspace == "" || start.Artifacts == "" || filepath.Dir(start.Workspace) != start.Root || filepath.Dir(start.Artifacts) != start.Root {
		t.Fatalf("unsafe mount mapping=%#v", start)
	}
	environment := strings.Join(start.Environment, "\n")
	for _, required := range []string{"HARNESS_WORKSPACE=", "HARNESS_ARTIFACTS=", "HOME=", "USERPROFILE=", "TEMP=", "TMP=", "XDG_CACHE_HOME=", "XDG_RUNTIME_DIR=", "GOPATH=", "PNPM_HOME=", "CARGO_HOME=", "RUSTUP_HOME="} {
		if !strings.Contains(environment, required) {
			t.Fatalf("fresh environment missing %q: %q", required, environment)
		}
	}
	for _, forbidden := range []string{"HTTP_PROXY=", "HTTPS_PROXY=", "ALL_PROXY=", "NO_PROXY=", "AWS_", "AZURE_", "GITHUB_TOKEN=", "NPM_TOKEN="} {
		if strings.Contains(environment, forbidden) {
			t.Fatalf("caller state leaked into fresh environment: %q", forbidden)
		}
	}
}

func TestWindowsLocalProviderRejectsStrictNetworkModesWhenHostOnly(t *testing.T) {
	for _, policy := range []NetworkPolicy{NetworkDisabled, NetworkIsolated} {
		t.Run(string(policy), func(t *testing.T) {
			backend := &fakeWindowsBackend{}
			provider := newWindowsLocalProviderForTest(backend)
			spec, _ := windowsProviderTestSpec(t)
			spec.Network = policy
			spec.RequestedAssurance.NetworkIsolation = true
			if _, err := provider.Start(context.Background(), spec); !errors.Is(err, ErrAssuranceTooWeak) {
				t.Fatalf("Start(%q) = %v, want ErrAssuranceTooWeak", policy, err)
			}
			backend.mu.Lock()
			startCalls := backend.startCalls
			backend.mu.Unlock()
			if startCalls != 0 {
				t.Fatalf("strict %q request reached native Start: calls=%d", policy, startCalls)
			}
		})
	}
}

func TestWindowsLocalProviderProbeDoesNotAuthorizeStart(t *testing.T) {
	backend := &fakeWindowsBackend{}
	provider := newWindowsLocalProviderForTest(backend)
	spec, _ := windowsProviderTestSpec(t)
	if report := provider.Probe(context.Background()); !report.Available {
		t.Fatalf("Probe report=%#v", report)
	}
	backend.mu.Lock()
	backend.startErr = errors.New("native start rejected")
	backend.mu.Unlock()
	if _, err := provider.Start(context.Background(), spec); !errors.Is(err, ErrProviderFailure) {
		t.Fatalf("Start after successful Probe error=%v", err)
	}
	backend.mu.Lock()
	probeCalls := backend.probeCalls
	startCalls := backend.startCalls
	backend.mu.Unlock()
	if probeCalls != 1 || startCalls != 1 {
		t.Fatalf("unexpected backend calls: probe=%d start=%d", probeCalls, startCalls)
	}
}

func TestWindowsLocalProviderReportsDynamicStartRootOwnership(t *testing.T) {
	spec, _ := windowsProviderTestSpec(t)
	preAdopt := newWindowsLocalProviderForTest(&fakeWindowsBackend{startErr: errors.New("helper pre-adoption failure")})
	if session, owned, err := preAdopt.StartWithRootOwnership(context.Background(), spec); session != nil || owned || !errors.Is(err, ErrProviderFailure) {
		t.Fatalf("pre-adoption Start ownership session=%T owned=%v err=%v", session, owned, err)
	}

	partialBackend := &fakeWindowsBackend{startSessionErr: errors.New("durable partial failure"), session: &fakeWindowsBackendSession{abortErr: errors.New("cleanup uncertain")}}
	partial := newWindowsLocalProviderForTest(partialBackend)
	if session, owned, err := partial.StartWithRootOwnership(context.Background(), spec); session != nil || !owned || !errors.Is(err, ErrProviderFailure) {
		t.Fatalf("partial Start ownership session=%T owned=%v err=%v", session, owned, err)
	}

	ready := newWindowsLocalProviderForTest(&fakeWindowsBackend{})
	session, owned, err := ready.StartWithRootOwnership(context.Background(), spec)
	if err != nil || session == nil || !owned {
		t.Fatalf("successful Start ownership session=%T owned=%v err=%v", session, owned, err)
	}
	if err := session.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestWindowsLocalProviderStartDoesNotReuseHistoricalProbe(t *testing.T) {
	backend := &fakeWindowsBackend{probeErr: errors.New("probe unavailable")}
	provider := newWindowsLocalProviderForTest(backend)
	spec, _ := windowsProviderTestSpec(t)
	if first, err := provider.Start(context.Background(), spec); err != nil {
		t.Fatalf("Start without Probe: %v", err)
	} else if err := first.Close(context.Background()); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if report := provider.Probe(context.Background()); report.Available {
		t.Fatalf("failing live Probe reported ready: %#v", report)
	}
	if second, err := provider.Start(context.Background(), spec); err != nil {
		t.Fatalf("Start reused failing historical Probe: %v", err)
	} else if err := second.Close(context.Background()); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	backend.mu.Lock()
	probeCalls := backend.probeCalls
	startCalls := backend.startCalls
	backend.mu.Unlock()
	if probeCalls != 1 || startCalls != 2 {
		t.Fatalf("Start depended on Probe: probe=%d start=%d", probeCalls, startCalls)
	}
}

func TestWindowsLocalProviderProbeReadsBackActiveSession(t *testing.T) {
	backend := &fakeWindowsBackend{}
	provider := newWindowsLocalProviderForTest(backend)
	spec, _ := windowsProviderTestSpec(t)
	session, err := provider.Start(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if report := provider.Probe(context.Background()); !report.Available {
		t.Fatalf("active session Probe report=%#v", report)
	}
	backend.mu.Lock()
	probeCalls := backend.probeCalls
	probeActiveCalls := backend.probeActiveCalls
	probeActiveDeadline := backend.probeActiveDeadline
	backend.mu.Unlock()
	if probeCalls != 0 {
		t.Fatalf("Probe called inactive backend while admission was active: %d", probeCalls)
	}
	if probeActiveCalls != 1 || !probeActiveDeadline {
		t.Fatalf("active Probe did not use one bounded readback: calls=%d deadline=%v", probeActiveCalls, probeActiveDeadline)
	}
	if err := session.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestWindowsLocalProviderProbeRejectsActiveStaticDrift(t *testing.T) {
	backend := &fakeWindowsBackend{probeActiveErr: errors.New("static identity drift")}
	provider := newWindowsLocalProviderForTest(backend)
	spec, _ := windowsProviderTestSpec(t)
	session, err := provider.Start(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if report := provider.Probe(context.Background()); report.Available {
		t.Fatalf("active static drift reported ready: %#v", report)
	}
	backend.mu.Lock()
	probeCalls := backend.probeCalls
	probeActiveCalls := backend.probeActiveCalls
	backend.mu.Unlock()
	if probeCalls != 0 || probeActiveCalls != 1 {
		t.Fatalf("unexpected drift probe calls: inactive=%d active=%d", probeCalls, probeActiveCalls)
	}
	if !provider.windowsAdmission().activeCompatible() {
		t.Fatal("active static drift changed session admission")
	}
	if err := session.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestWindowsLocalProviderProbeDoesNotCallBackendWithoutAdmission(t *testing.T) {
	backend := &fakeWindowsBackend{}
	admission := newWindowsAdmission()
	if !admission.tryAcquire() {
		t.Fatal("test admission was not available")
	}
	defer admission.releaseProbe()
	provider := LocalProvider{backend: backend, admission: admission}
	if report := provider.Probe(context.Background()); report.Available {
		t.Fatalf("busy inactive Probe reported ready: %#v", report)
	}
	backend.mu.Lock()
	probeCalls := backend.probeCalls
	probeActiveCalls := backend.probeActiveCalls
	backend.mu.Unlock()
	if probeCalls != 0 || probeActiveCalls != 0 {
		t.Fatalf("busy inactive Probe called backend: inactive=%d active=%d", probeCalls, probeActiveCalls)
	}
}

func TestWindowsLocalProviderConcurrentActiveProbesPreserveAdmission(t *testing.T) {
	backend := &fakeWindowsBackend{}
	provider := newWindowsLocalProviderForTest(backend)
	spec, _ := windowsProviderTestSpec(t)
	session, err := provider.Start(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	const probes = 8
	var wait sync.WaitGroup
	wait.Add(probes)
	for range probes {
		go func() {
			defer wait.Done()
			if report := provider.Probe(context.Background()); !report.Available {
				t.Errorf("active concurrent Probe report=%#v", report)
			}
		}()
	}
	wait.Wait()
	backend.mu.Lock()
	probeCalls := backend.probeCalls
	probeActiveCalls := backend.probeActiveCalls
	backend.mu.Unlock()
	if probeCalls != 0 || probeActiveCalls != probes {
		t.Fatalf("concurrent active probes: inactive=%d active=%d", probeCalls, probeActiveCalls)
	}
	if !provider.windowsAdmission().activeCompatible() {
		t.Fatal("concurrent probes disturbed active admission")
	}
	if err := session.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestWindowsLocalSessionOutputFinalizationAndFailSafeAbort(t *testing.T) {
	backend := &fakeWindowsBackend{session: &fakeWindowsBackendSession{stdout: []byte("out"), stderr: []byte("err")}}
	provider := newWindowsLocalProviderForTest(backend)
	spec, _ := windowsProviderTestSpec(t)
	session, err := provider.Start(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	request := ExecRequest{Args: []string{"tool"}, Limits: spec.Limits, Network: spec.Network, ArtifactPolicy: spec.ArtifactPolicy, Lease: spec.Lease}
	result, err := session.(ExecSession).Exec(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Stdout.Size != 3 || result.Stderr.Size != 3 || result.Stdout.Digest == "" || result.Stderr.Digest == "" {
		t.Fatalf("separate output result=%#v", result)
	}
	if err := session.(PublicationFinalizer).FinalizePublication(context.Background(), PublicationPublished); err != nil {
		t.Fatal(err)
	}
	if err := session.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	backend.session.mu.Lock()
	events := append([]string(nil), backend.session.events...)
	backend.session.mu.Unlock()
	if got, want := strings.Join(events, ","), "execute,finalize:published,close"; got != want {
		t.Fatalf("publication ordering=%q want=%q", got, want)
	}

	abortBackend := &fakeWindowsBackend{session: &fakeWindowsBackendSession{}}
	abortProvider := newWindowsLocalProviderForTest(abortBackend)
	abortSession, err := abortProvider.Start(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := abortSession.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	abortBackend.session.mu.Lock()
	abortEvents := strings.Join(abortBackend.session.events, ",")
	abortBackend.session.mu.Unlock()
	if abortEvents != "abort,close" || strings.Contains(abortEvents, "published") {
		t.Fatalf("unfinalized close fabricated publication: %q", abortEvents)
	}
}

func TestWindowsLocalSessionOutputOverflowTerminatesAndFinalizationFailurePoisons(t *testing.T) {
	backend := &fakeWindowsBackend{session: &fakeWindowsBackendSession{stdout: []byte("12345"), stderr: []byte("67890"), finalizeErrs: []error{errors.New("backend cleanup secret")}}}
	provider := newWindowsLocalProviderForTest(backend)
	spec, _ := windowsProviderTestSpec(t)
	session, err := provider.Start(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	request := ExecRequest{Args: []string{"tool"}, Limits: spec.Limits, Network: spec.Network, ArtifactPolicy: spec.ArtifactPolicy, Lease: spec.Lease}
	if _, err := session.(ExecSession).Exec(context.Background(), request); !errors.Is(err, ErrExecOutputLimit) {
		t.Fatalf("overflow err=%v", err)
	}
	if err := session.(PublicationFinalizer).FinalizePublication(context.Background(), PublicationFailed); !errors.Is(err, ErrProviderFailure) || strings.Contains(err.Error(), "secret") {
		t.Fatalf("finalization err=%v", err)
	}
	if err := session.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	backend.session.mu.Lock()
	events := strings.Join(backend.session.events, ",")
	backend.session.mu.Unlock()
	if !strings.Contains(events, "terminate") || !strings.Contains(events, "finalize:failed,poison,abort,close") {
		t.Fatalf("overflow/failure lifecycle=%q", events)
	}
	if _, err := provider.Start(context.Background(), spec); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("poisoned provider Start err=%v", err)
	}
}

func TestWindowsAdmissionRecoveryEpochFencesOldSession(t *testing.T) {
	spec, _ := windowsProviderTestSpec(t)
	backend := &recoveringWindowsBackend{
		fakeWindowsBackend: &fakeWindowsBackend{session: &fakeWindowsBackendSession{
			finalizeErrs: []error{errors.New("durable failure")},
			closeErrs:    []error{errors.New("old close failure")},
		}},
		recoverEntered: make(chan struct{}),
		releaseRecover: make(chan struct{}),
	}
	provider := newWindowsLocalProviderForTest(backend)
	first, err := provider.Start(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.(PublicationFinalizer).FinalizePublication(context.Background(), PublicationFailed); !errors.Is(err, ErrProviderFailure) {
		t.Fatalf("Finalize poison err=%v", err)
	}
	firstEpoch := first.(*windowsLocalSession).epoch

	probeResults := make(chan bool, 2)
	for range 2 {
		go func() { probeResults <- provider.Probe(context.Background()).Available }()
	}
	<-backend.recoverEntered
	// An old Close races with recovery. It may clean its old backend objects,
	// but cannot release the recovery-held token or clear future admission.
	oldClose := make(chan error, 1)
	go func() { oldClose <- first.Close(context.Background()) }()
	close(backend.releaseRecover)
	available := 0
	for range 2 {
		if <-probeResults {
			available++
		}
	}
	if available < 1 || available > 2 {
		t.Fatalf("recovery probes available=%d", available)
	}
	if err := <-oldClose; !errors.Is(err, ErrProviderFailure) {
		t.Fatalf("old Close err=%v", err)
	}
	backend.mu.Lock()
	recoverCalls := backend.recoverCalls
	backend.mu.Unlock()
	if recoverCalls != 1 {
		t.Fatalf("concurrent recovery calls=%d", recoverCalls)
	}

	second, err := provider.Start(context.Background(), spec)
	if err != nil {
		t.Fatalf("Start after recovery: %v", err)
	}
	secondEpoch := second.(*windowsLocalSession).epoch
	if secondEpoch == firstEpoch || secondEpoch == 0 {
		t.Fatalf("epoch was reused: first=%d second=%d", firstEpoch, secondEpoch)
	}
	// A replayed old terminal call is fenced by its stale epoch and cannot
	// poison/release the current session.
	_ = first.(PublicationFinalizer).FinalizePublication(context.Background(), PublicationFailed)
	if !provider.windowsAdmission().activeCompatible() {
		t.Fatal("stale old session disturbed recovered active admission")
	}
	if err := second.Close(context.Background()); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	third, err := provider.Start(context.Background(), spec)
	if err != nil {
		t.Fatalf("exactly one recovery token was not restored: %v", err)
	}
	if err := third.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestWindowsAdmissionRecoveryFailureRetainsPoisonAndToken(t *testing.T) {
	spec, _ := windowsProviderTestSpec(t)
	backend := &recoveringWindowsBackend{
		fakeWindowsBackend: &fakeWindowsBackend{session: &fakeWindowsBackendSession{finalizeErrs: []error{errors.New("durable failure")}}},
		recoverErr:         errors.New("repair proof failed"),
	}
	provider := newWindowsLocalProviderForTest(backend)
	session, err := provider.Start(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := session.(PublicationFinalizer).FinalizePublication(context.Background(), PublicationFailed); !errors.Is(err, ErrProviderFailure) {
		t.Fatalf("Finalize poison err=%v", err)
	}
	if report := provider.Probe(context.Background()); report.Available {
		t.Fatalf("failed recovery reported ready: %#v", report)
	}
	if _, err := provider.Start(context.Background(), spec); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("failed recovery admitted Start: %v", err)
	}
	backend.mu.Lock()
	recoverCalls := backend.recoverCalls
	backend.mu.Unlock()
	if recoverCalls != 1 {
		t.Fatalf("failed recovery calls=%d", recoverCalls)
	}
}

func TestWindowsAdmissionWithoutRecoveryOwnerDoesNotEnterRecovering(t *testing.T) {
	admission := newWindowsAdmission()
	epoch, err := admission.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !admission.poisonWithOwner(epoch, nil) {
		t.Fatal("poison did not retain the active epoch")
	}
	if owner := admission.beginRecovery(); owner != nil {
		t.Fatalf("ownerless admission began recovery with owner=%T", owner)
	}
	admission.mu.Lock()
	poisoned, recovering, currentEpoch := admission.poisoned, admission.recovering, admission.epoch
	admission.mu.Unlock()
	if !poisoned || recovering || currentEpoch != epoch {
		t.Fatalf("ownerless recovery mutated admission: poisoned=%v recovering=%v epoch=%d want=%d", poisoned, recovering, currentEpoch, epoch)
	}
}

func TestWindowsLocalProviderPartialStartAndTerminalRetriesFailClosed(t *testing.T) {
	spec, _ := windowsProviderTestSpec(t)
	partial := &fakeWindowsBackend{
		startSessionErr: errors.New("launch returned a session and secret error"),
		session:         &fakeWindowsBackendSession{},
	}
	if _, err := newWindowsLocalProviderForTest(partial).Start(context.Background(), spec); !errors.Is(err, ErrProviderFailure) {
		t.Fatalf("partial Start err=%v", err)
	}
	partial.session.mu.Lock()
	partialEvents := strings.Join(partial.session.events, ",")
	partial.session.mu.Unlock()
	if partialEvents != "abort,close" {
		t.Fatalf("partial Start cleanup=%q", partialEvents)
	}

	backend := &fakeWindowsBackend{session: &fakeWindowsBackendSession{
		executeErr:   ErrExecFailed,
		finalizeErrs: []error{errors.New("first durable publication failure")},
		closeErrs:    []error{errors.New("first close failure")},
	}}
	provider := newWindowsLocalProviderForTest(backend)
	session, err := provider.Start(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	request := ExecRequest{Args: []string{"tool"}, Limits: spec.Limits, Network: spec.Network, ArtifactPolicy: spec.ArtifactPolicy, Lease: spec.Lease}
	if _, err := session.(ExecSession).Exec(context.Background(), request); !errors.Is(err, ErrExecFailed) {
		t.Fatalf("nonzero execution lost its neutral error: %v", err)
	}
	finalizer := session.(PublicationFinalizer)
	if err := finalizer.FinalizePublication(context.Background(), PublicationFailed); !errors.Is(err, ErrProviderFailure) {
		t.Fatalf("first finalization err=%v", err)
	}
	if _, err := session.(ExecSession).Exec(context.Background(), request); !errors.Is(err, ErrLeaseTerminated) {
		t.Fatalf("terminal finalization admitted execution: %v", err)
	}
	if err := finalizer.FinalizePublication(context.Background(), PublicationPublished); !errors.Is(err, ErrProviderFailure) {
		t.Fatalf("different outcome err=%v", err)
	}
	if err := finalizer.FinalizePublication(context.Background(), PublicationFailed); err != nil {
		t.Fatalf("same-outcome retry err=%v", err)
	}
	if err := session.Close(context.Background()); !errors.Is(err, ErrProviderFailure) {
		t.Fatalf("first close err=%v", err)
	}
	if err := session.Close(context.Background()); err != nil {
		t.Fatalf("idempotent close retry err=%v", err)
	}
}

func TestWindowsLocalProviderCleansSuccessfulStartCanceledBeforeReturn(t *testing.T) {
	spec, _ := windowsProviderTestSpec(t)
	ctx, cancel := context.WithCancel(context.Background())
	backend := &fakeWindowsBackend{session: &fakeWindowsBackendSession{}, onStart: cancel}
	provider := newWindowsLocalProviderForTest(backend)
	if _, err := provider.Start(ctx, spec); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Start err=%v", err)
	}
	backend.session.mu.Lock()
	events := strings.Join(backend.session.events, ",")
	backend.session.mu.Unlock()
	if events != "abort,close" {
		t.Fatalf("canceled Start did not clean partial session: %q", events)
	}
	if session, err := provider.Start(context.Background(), spec); err != nil {
		t.Fatalf("successful partial cleanup did not release admission: %v", err)
	} else if err := session.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestWindowsCleanupUsesIndependentBoundedContexts(t *testing.T) {
	session := &fakeWindowsBackendSession{abortWait: true}
	if windowsCleanupBackendSessionWithin(session, time.Millisecond) {
		t.Fatal("blocked abort unexpectedly reported cleanup success")
	}
	session.mu.Lock()
	events := strings.Join(session.events, ",")
	session.mu.Unlock()
	if events != "abort,close,poison" {
		t.Fatalf("cleanup did not continue after abort timeout: %q", events)
	}
}

func TestWindowsLocalSessionSerializesExecWithFinalizeAndClose(t *testing.T) {
	spec, _ := windowsProviderTestSpec(t)
	request := ExecRequest{Args: []string{"tool"}, Limits: spec.Limits, Network: spec.Network, ArtifactPolicy: spec.ArtifactPolicy, Lease: spec.Lease}

	finalizeBackend := &fakeWindowsBackend{session: &fakeWindowsBackendSession{executeEntered: make(chan struct{}), releaseExecute: make(chan struct{})}}
	finalizeSession, err := newWindowsLocalProviderForTest(finalizeBackend).Start(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	execDone := make(chan error, 1)
	go func() {
		_, execErr := finalizeSession.(ExecSession).Exec(context.Background(), request)
		execDone <- execErr
	}()
	<-finalizeBackend.session.executeEntered
	finalizeDone := make(chan error, 1)
	go func() {
		finalizeDone <- finalizeSession.(PublicationFinalizer).FinalizePublication(context.Background(), PublicationFailed)
	}()
	select {
	case err := <-finalizeDone:
		t.Fatalf("Finalize overtook active Exec: %v", err)
	default:
	}
	close(finalizeBackend.session.releaseExecute)
	if err := <-execDone; err != nil {
		t.Fatalf("Exec err=%v", err)
	}
	if err := <-finalizeDone; err != nil {
		t.Fatalf("Finalize err=%v", err)
	}
	if err := finalizeSession.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	closeBackend := &fakeWindowsBackend{session: &fakeWindowsBackendSession{executeEntered: make(chan struct{}), releaseExecute: make(chan struct{})}}
	closeSession, err := newWindowsLocalProviderForTest(closeBackend).Start(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	execDone = make(chan error, 1)
	go func() {
		_, execErr := closeSession.(ExecSession).Exec(context.Background(), request)
		execDone <- execErr
	}()
	<-closeBackend.session.executeEntered
	closeDone := make(chan error, 1)
	go func() { closeDone <- closeSession.Close(context.Background()) }()
	select {
	case err := <-closeDone:
		t.Fatalf("Close overtook active Exec: %v", err)
	default:
	}
	close(closeBackend.session.releaseExecute)
	if err := <-execDone; err != nil {
		t.Fatalf("Exec err=%v", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("Close err=%v", err)
	}
}
