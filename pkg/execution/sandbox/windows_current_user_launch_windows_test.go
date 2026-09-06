//go:build windows

package sandbox

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf16"
	"unsafe"

	"golang.org/x/sys/windows"
)

func TestWindowsCurrentUserSourceEligibilityRejectsElevatedAndUnknown(t *testing.T) {
	if windowsCurrentUserSourceEligible(true, nil) {
		t.Fatal("full/elevated caller was accepted")
	}
	if windowsCurrentUserSourceEligible(false, errors.New("token query failed")) {
		t.Fatal("unknown caller elevation was accepted")
	}
	if !windowsCurrentUserSourceEligible(false, nil) {
		t.Fatal("unelevated caller was rejected")
	}
}

func TestWindowsCurrentUserEnvironmentOfflineHints(t *testing.T) {
	base := []string{
		"SystemRoot=C:\\Windows", "ComSpec=C:\\Windows\\System32\\cmd.exe", "PATH=C:\\Windows\\System32",
		"HARNESS_WORKSPACE=C:\\owned\\workspace", "HARNESS_ARTIFACTS=C:\\owned\\artifacts", "HOME=C:\\owned\\home", "USERPROFILE=C:\\owned\\home", "TEMP=C:\\owned\\tmp", "TMP=C:\\owned\\tmp",
	}
	block, err := windowsCurrentUserEnvironmentBlock(base, true)
	if err != nil {
		t.Fatal(err)
	}
	values := strings.Join(strings.Split(string(utf16.Decode(block)), "\x00"), "\n")
	for _, expected := range []string{"HTTP_PROXY=http://127.0.0.1:9", "HTTPS_PROXY=http://127.0.0.1:9", "ALL_PROXY=http://127.0.0.1:9", "CARGO_NET_OFFLINE=true", "npm_config_offline=true"} {
		if !strings.Contains(values, expected) {
			t.Fatalf("missing offline hint %q", expected)
		}
	}
	withoutHints, err := windowsCurrentUserEnvironmentBlock(base, false)
	if err != nil {
		t.Fatal(err)
	}
	if values := strings.Join(strings.Split(string(utf16.Decode(withoutHints)), "\x00"), "\n"); strings.Contains(values, "PROXY=") || strings.Contains(values, "CARGO_NET_OFFLINE=") || strings.Contains(values, "npm_config_offline=") {
		t.Fatalf("disabled offline hints leaked into child environment")
	}
}

func TestWindowsCurrentUserDesktopCompatibilityModeIsExplicit(t *testing.T) {
	desktop, err := newWindowsCurrentUserDesktop("S-1-5-5-1-2", "S-1-5-21-1-2-3-4", false)
	if err != nil {
		t.Fatal(err)
	}
	defer desktop.Close()
	if desktop.handle != 0 || desktop.name != `Winsta0\Default` {
		t.Fatalf("compatibility desktop=%#v", desktop)
	}
}

// TestNativeWindowsCurrentUserLaunchUsesCapabilityRoot runs without a setup
// account, WFP, or fixture. It proves the actual restricted target can write
// a fresh owned workspace and remains an ordinary (not full/high/admin)
// target while the inherited Job and anonymous stdio are active.
func TestNativeWindowsCurrentUserLaunchUsesCapabilityRoot(t *testing.T) {
	elevated, err := windowsCurrentUserIsElevated()
	if err != nil {
		t.Fatal(err)
	}
	if elevated {
		t.Skip("current process is elevated; direct provider correctly rejects it")
	}
	root, workspace, artifacts := newWindowsCurrentUserTestLayout(t)
	capability, err := newWindowsCurrentUserCapabilitySID()
	if err != nil {
		t.Fatal(err)
	}
	owner, err := windowsCurrentUserSID()
	if err != nil {
		t.Fatal(err)
	}
	if err := windowsGrantCurrentUserCapabilityRoot(root, owner, capability); err != nil {
		t.Fatalf("grant owned root capability: %v", err)
	}
	if !windowsCurrentUserRootHasCapabilityACE(root, owner, capability) {
		t.Fatal("owned root lacks protected capability inheritance ACE")
	}
	token, err := newWindowsCurrentUserRestrictedToken(capability)
	if err != nil {
		t.Fatalf("restricted token: %v", err)
	}
	defer token.Close()
	desktop, err := newWindowsCurrentUserDesktop(token.logon, token.capability, true)
	if err != nil {
		t.Fatalf("private desktop: %v", err)
	}
	defer desktop.Close()
	environment, err := windowsFreshEnvironment(root, workspace, artifacts)
	if err != nil {
		t.Fatal(err)
	}
	block, err := windowsCurrentUserEnvironmentBlock(environment, true)
	if err != nil {
		t.Fatal(err)
	}
	job, err := newWindowsJob(windowsNativeJobAPI{}, windowsJobLimits{MemoryBytes: DefaultExecMemoryBytes, CPUTime: DefaultExecWallTime})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), windowsBackendTimeout)
		defer cancel()
		_ = job.Close(ctx)
	}()
	systemDirectory, err := windows.GetSystemDirectory()
	if err != nil {
		t.Fatal(err)
	}
	application := filepath.Join(systemDirectory, "cmd.exe")
	command, err := windowsEncodeArgv([]string{application, "/d", "/c", "echo allowed> allowed.txt & timeout /t 2 /nobreak >nul"})
	if err != nil {
		t.Fatal(err)
	}
	launch, err := newWindowsCurrentUserLaunch(token.token, job.jobHandle(), application, command, workspace, desktop.name, block)
	if err != nil {
		t.Fatalf("CreateProcessAsUser with JOB_LIST/HANDLE_LIST: %v", err)
	}
	defer launch.Close()
	if err := windowsCurrentUserAssertOrdinaryTarget(launch.process); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	drained := launch.copyOutput(&stdout, &stderr)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	waitErr := windowsCurrentUserWaitProcess(ctx, launch.process)
	terminateErr := job.Terminate(ctx)
	emptyErr := job.WaitEmpty(ctx)
	cancel()
	if waitErr != nil || terminateErr != nil || emptyErr != nil || !windowsCurrentUserDrainOutput(drained, launch) {
		t.Fatalf("native lifecycle wait=%v terminate=%v empty=%v", waitErr, terminateErr, emptyErr)
	}
	if _, err := os.Stat(filepath.Join(workspace, "allowed.txt")); err != nil {
		t.Fatalf("restricted target did not write owned workspace: %v", err)
	}
}

// TestNativeWindowsCurrentUserExecuteCleansJobAfterChildCloseFailure injects
// the only post-CreateProcess parent-side failure. Execute must retain the
// launch error while synchronously terminating its atomically admitted Job;
// the stdio pipe owner also must not retain readers after that failure.
func TestNativeWindowsCurrentUserExecuteCleansJobAfterChildCloseFailure(t *testing.T) {
	elevated, err := windowsCurrentUserIsElevated()
	if err != nil {
		t.Fatal(err)
	}
	if elevated {
		t.Skip("current process is elevated; direct provider correctly rejects it")
	}
	root, workspace, artifacts := newWindowsCurrentUserTestLayout(t)
	workRoot := filepath.Dir(filepath.Dir(root))
	environment, err := windowsProviderEnvironment(root, workspace, artifacts)
	if err != nil {
		t.Fatal(err)
	}
	limits := Limits{WallTime: 5 * time.Second, MaxCPUTime: 5 * time.Second, MaxMemoryBytes: DefaultExecMemoryBytes, MaxOutputBytes: DefaultExecCombinedBytes}
	policy := ArtifactPolicy{MaxArtifacts: 1, MaxTotalBytes: 1024}
	lease := LeaseIdentity{RunID: "current-user-run", SegmentID: "current-user-segment", ModuleID: "current-user-module", CompositionRev: "current-user-revision", Token: "current-user-token"}
	start := windowsBackendStart{
		Spec: SessionSpec{
			RequestedAssurance: Assurance{Level: AssuranceProcess, SharedKernel: true}, Limits: limits, Network: NetworkHost,
			Mounts: []Mount{{Source: workspace, Target: "/workspace"}, {Source: artifacts, Target: "/artifacts"}}, ArtifactPolicy: policy, Lease: lease,
		},
		Root: root, Workspace: workspace, Artifacts: artifacts, Environment: environment,
	}
	backend := &windowsCurrentUserBackend{workRoot: workRoot, privateDesktop: false, offlineHints: false}
	raw, err := backend.Start(context.Background(), start)
	if err != nil {
		t.Fatalf("start direct current-user session: %v", err)
	}
	session, ok := raw.(*windowsCurrentUserSession)
	if !ok || session == nil {
		t.Fatal("unexpected current-user session")
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), windowsBackendTimeout)
		defer cancel()
		if err := session.Close(ctx); err != nil {
			t.Errorf("close session: %v", err)
		}
	}()
	systemDirectory, err := windows.GetSystemDirectory()
	if err != nil {
		t.Fatal(err)
	}
	var observed *windowsCurrentUserStdioPipes
	original := windowsCurrentUserCloseChildPipes
	windowsCurrentUserCloseChildPipes = func(pipes *windowsCurrentUserStdioPipes) error {
		observed = pipes
		if err := pipes.closeChild(); err != nil {
			return err
		}
		return errors.New("injected child pipe close failure")
	}
	defer func() { windowsCurrentUserCloseChildPipes = original }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = session.Execute(ctx, windowsBackendExec{
		Args: []string{filepath.Join(systemDirectory, "cmd.exe"), "/d", "/c", "timeout /t 5 /nobreak >nul"}, Limits: limits, ArtifactPolicy: policy, Lease: lease,
	}, io.Discard, io.Discard)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Execute post-Create close failure=%v, want unavailable", err)
	}
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), windowsBackendTimeout)
	emptyErr := session.job.WaitEmpty(cleanupCtx)
	cleanupCancel()
	if emptyErr != nil {
		t.Fatalf("injected launch failure left active Job processes: %v", emptyErr)
	}
	if observed == nil || observed.childInput != 0 || observed.childStdout != 0 || observed.childStderr != 0 || observed.parentStdout != nil || observed.parentStderr != nil {
		t.Fatal("injected launch failure retained a child handle or output reader")
	}
}

// TestNativeWindowsCurrentUserSequentialPublicSessions verifies the public
// Registry path, including a checked Close between executions.  It prevents a
// completed first Job, authority, or backend active pointer from silently
// blocking the next Start.
func TestNativeWindowsCurrentUserSequentialPublicSessions(t *testing.T) {
	elevated, err := windowsCurrentUserIsElevated()
	if err != nil {
		t.Fatal(err)
	}
	if elevated {
		t.Skip("current process is elevated; direct provider correctly rejects it")
	}
	workRoot := filepath.Join(t.TempDir(), "sandbox")
	if err := os.MkdirAll(filepath.Join(workRoot, "sessions"), 0o700); err != nil {
		t.Fatal(err)
	}
	registration, err := NewLocalRegistrationForWorkRoot(workRoot)
	if err != nil {
		t.Fatalf("new current-user registration: %v", err)
	}
	registry, err := NewRegistry(1, registration)
	if err != nil {
		t.Fatalf("new current-user registry: %v", err)
	}
	systemDirectory, err := windows.GetSystemDirectory()
	if err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{
		"0123456789abcdef0123456789abcdef",
		"fedcba9876543210fedcba9876543210",
	} {
		spec := newWindowsCurrentUserPublicSessionSpec(t, workRoot, suffix)
		session, err := registry.Start(context.Background(), registration.Metadata.ID, registration.Metadata.Version, spec)
		if err != nil {
			t.Fatalf("Start(%s): %v", suffix, err)
		}
		execSession, ok := session.(ExecSession)
		if !ok {
			_ = session.Close(context.Background())
			t.Fatalf("Start(%s) did not return the public ExecSession extension", suffix)
		}
		result, execErr := execSession.Exec(context.Background(), ExecRequest{
			Args:           []string{filepath.Join(systemDirectory, "cmd.exe"), "/d", "/c", "exit 0"},
			Limits:         spec.Limits,
			Network:        spec.Network,
			ArtifactPolicy: spec.ArtifactPolicy,
			Lease:          spec.Lease,
		})
		closeErr := session.Close(context.Background())
		if execErr != nil {
			t.Fatalf("Exec(%s): %v", suffix, execErr)
		}
		if result.ExitCode != 0 || result.TimedOut || result.Signal != "" {
			t.Fatalf("Exec(%s) result=%#v", suffix, result)
		}
		if closeErr != nil {
			t.Fatalf("Close(%s): %v", suffix, closeErr)
		}
	}
}

// TestNativeWindowsCurrentUserTimeoutReturnsTimedResult proves that a target
// which has actually entered the Job is terminated at its execution deadline.
// The result must retain TimedOut after the mandatory post-cleanup authority
// proof completes under its own bounded context.
func TestNativeWindowsCurrentUserTimeoutReturnsTimedResult(t *testing.T) {
	elevated, err := windowsCurrentUserIsElevated()
	if err != nil {
		t.Fatal(err)
	}
	if elevated {
		t.Skip("current process is elevated; direct provider correctly rejects it")
	}
	workRoot := filepath.Join(t.TempDir(), "sandbox")
	if err := os.MkdirAll(filepath.Join(workRoot, "sessions"), 0o700); err != nil {
		t.Fatal(err)
	}
	registration, err := NewLocalRegistrationForWorkRoot(workRoot)
	if err != nil {
		t.Fatalf("new current-user registration: %v", err)
	}
	registry, err := NewRegistry(1, registration)
	if err != nil {
		t.Fatalf("new current-user registry: %v", err)
	}
	spec := newWindowsCurrentUserPublicSessionSpec(t, workRoot, "00112233445566778899aabbccddeeff")
	session, err := registry.Start(context.Background(), registration.Metadata.ID, registration.Metadata.Version, spec)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	execSession, ok := session.(ExecSession)
	if !ok {
		_ = session.Close(context.Background())
		t.Fatal("Start did not return the public ExecSession extension")
	}
	program, err := os.Executable()
	if err != nil {
		_ = session.Close(context.Background())
		t.Fatal(err)
	}
	result, execErr := execSession.Exec(context.Background(), ExecRequest{
		Args:           []string{program, "-test.run=^TestNativeWindowsCurrentUserTimeoutSleeper$"},
		Limits:         spec.Limits,
		Network:        spec.Network,
		ArtifactPolicy: spec.ArtifactPolicy,
		Lease:          spec.Lease,
	})
	closeErr := session.Close(context.Background())
	if !errors.Is(execErr, ErrExecTimeout) || !result.TimedOut {
		t.Fatalf("Exec timeout err=%v result=%#v", execErr, result)
	}
	if marker, markerErr := os.ReadFile(filepath.Join(workRoot, "sessions", "session-00112233445566778899aabbccddeeff", "workspace", "timeout.started")); markerErr != nil || string(marker) != "started\n" {
		t.Fatalf("timeout target marker=%q err=%v", marker, markerErr)
	}
	if closeErr != nil {
		t.Fatalf("Close after timeout: %v", closeErr)
	}
}

// TestNativeWindowsCurrentUserCancellationRetainsResult proves that a caller
// cancellation after the target enters the Job does not erase the bounded
// execution result during the mandatory post-cleanup authority proof.
func TestNativeWindowsCurrentUserCancellationRetainsResult(t *testing.T) {
	elevated, err := windowsCurrentUserIsElevated()
	if err != nil {
		t.Fatal(err)
	}
	if elevated {
		t.Skip("current process is elevated; direct provider correctly rejects it")
	}
	workRoot := filepath.Join(t.TempDir(), "sandbox")
	if err := os.MkdirAll(filepath.Join(workRoot, "sessions"), 0o700); err != nil {
		t.Fatal(err)
	}
	registration, err := NewLocalRegistrationForWorkRoot(workRoot)
	if err != nil {
		t.Fatalf("new current-user registration: %v", err)
	}
	registry, err := NewRegistry(1, registration)
	if err != nil {
		t.Fatalf("new current-user registry: %v", err)
	}
	const suffix = "ffeeddccbbaa99887766554433221100"
	spec := newWindowsCurrentUserPublicSessionSpec(t, workRoot, suffix)
	session, err := registry.Start(context.Background(), registration.Metadata.ID, registration.Metadata.Version, spec)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	execSession, ok := session.(ExecSession)
	if !ok {
		_ = session.Close(context.Background())
		t.Fatal("Start did not return the public ExecSession extension")
	}
	program, err := os.Executable()
	if err != nil {
		_ = session.Close(context.Background())
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type cancelOutcome struct {
		result ExecResult
		err    error
	}
	resultCh := make(chan cancelOutcome, 1)
	go func() {
		result, execErr := execSession.Exec(ctx, ExecRequest{
			Args:           []string{program, "-test.run=^TestNativeWindowsCurrentUserCancelSleeper$", "-test.v"},
			Limits:         spec.Limits,
			Network:        spec.Network,
			ArtifactPolicy: spec.ArtifactPolicy,
			Lease:          spec.Lease,
		})
		resultCh <- cancelOutcome{result: result, err: execErr}
	}()
	marker := filepath.Join(workRoot, "sessions", "session-"+suffix, "workspace", "cancel.started")
	if !waitForWindowsCurrentUserTestMarker(marker, "started\n", 5*time.Second) {
		cancel()
		resultOutcome := <-resultCh
		_ = session.Close(context.Background())
		t.Fatalf("cancel target did not reach marker: err=%v", resultOutcome.err)
	}
	cancel()
	resultOutcome := <-resultCh
	closeErr := session.Close(context.Background())
	if !errors.Is(resultOutcome.err, context.Canceled) {
		t.Fatalf("Exec cancel err=%v", resultOutcome.err)
	}
	if !bytes.Contains(resultOutcome.result.Stdout.Preview, []byte("started\n")) || resultOutcome.result.Stdout.Size == 0 {
		t.Fatalf("canceled Exec lost stdout result=%#v", resultOutcome.result)
	}
	if closeErr != nil {
		t.Fatalf("Close after cancellation: %v", closeErr)
	}
}

func TestNativeWindowsCurrentUserTimeoutSleeper(t *testing.T) {
	if !windowsCurrentUserSleeperSelected("TestNativeWindowsCurrentUserTimeoutSleeper") {
		t.Skip("restricted timeout target only")
	}
	workspace := os.Getenv("HARNESS_WORKSPACE")
	if workspace == "" {
		t.Fatal("missing workspace")
	}
	if err := os.WriteFile(filepath.Join(workspace, "timeout.started"), []byte("started\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Second)
}

func TestNativeWindowsCurrentUserCancelSleeper(t *testing.T) {
	if !windowsCurrentUserSleeperSelected("TestNativeWindowsCurrentUserCancelSleeper") {
		t.Skip("restricted cancellation target only")
	}
	workspace := os.Getenv("HARNESS_WORKSPACE")
	if workspace == "" {
		t.Fatal("missing workspace")
	}
	if _, err := fmt.Fprintln(os.Stdout, "started"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "cancel.started"), []byte("started\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Second)
}

func windowsCurrentUserSleeperSelected(name string) bool {
	want := "-test.run=^" + name + "$"
	for _, argument := range os.Args[1:] {
		if argument == want {
			return true
		}
	}
	return false
}

func waitForWindowsCurrentUserTestMarker(path, want string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if value, err := os.ReadFile(path); err == nil && string(value) == want {
			return true
		}
		time.Sleep(25 * time.Millisecond)
	}
	return false
}

func newWindowsCurrentUserPublicSessionSpec(t *testing.T, workRoot, suffix string) SessionSpec {
	t.Helper()
	root := filepath.Join(workRoot, "sessions", "session-"+suffix)
	workspace := filepath.Join(root, "workspace")
	artifacts := filepath.Join(root, "artifacts")
	for _, path := range []string{workspace, artifacts, filepath.Join(root, "home"), filepath.Join(root, "tmp"), filepath.Join(root, "cache")} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	return SessionSpec{
		RequestedAssurance: Assurance{Level: AssuranceProcess, SharedKernel: true},
		Limits:             Limits{WallTime: 5 * time.Second, MaxCPUTime: 5 * time.Second, MaxMemoryBytes: DefaultExecMemoryBytes, MaxOutputBytes: DefaultExecCombinedBytes},
		Network:            NetworkHost,
		ArtifactPolicy:     ArtifactPolicy{MaxArtifacts: 1, MaxTotalBytes: 1024},
		Lease:              LeaseIdentity{RunID: "current-user-run-" + suffix, SegmentID: "current-user-segment", ModuleID: "current-user-module", CompositionRev: "current-user-revision", Token: "current-user-token-" + suffix},
		Mounts:             []Mount{{Source: workspace, Target: "/workspace"}, {Source: artifacts, Target: "/artifacts"}},
	}
}

func newWindowsCurrentUserTestLayout(t *testing.T) (root, workspace, artifacts string) {
	t.Helper()
	root = filepath.Join(t.TempDir(), "sandbox", "sessions", "session-0123456789abcdef0123456789abcdef")
	workspace = filepath.Join(root, "workspace")
	artifacts = filepath.Join(root, "artifacts")
	for _, path := range []string{workspace, artifacts, filepath.Join(root, "home"), filepath.Join(root, "tmp"), filepath.Join(root, "cache")} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	return root, workspace, artifacts
}

func windowsCurrentUserRootHasCapabilityACE(root, owner, capability string) bool {
	descriptor, err := windows.GetNamedSecurityInfo(root, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
	if err != nil || descriptor == nil || !descriptor.IsValid() {
		return false
	}
	control, _, err := descriptor.Control()
	if err != nil || control&windows.SE_DACL_PROTECTED == 0 {
		return false
	}
	dacl, defaulted, err := descriptor.DACL()
	if err != nil || defaulted || dacl == nil {
		return false
	}
	if dacl.AceCount != 2 {
		return false
	}
	principals := map[string]bool{owner: false, capability: false}
	for index := uint32(0); index < uint32(dacl.AceCount); index++ {
		var ace *windows.ACCESS_ALLOWED_ACE
		if windows.GetAce(dacl, index, &ace) != nil || ace == nil || ace.Header.AceType != windows.ACCESS_ALLOWED_ACE_TYPE || ace.Header.AceFlags != windows.OBJECT_INHERIT_ACE|windows.CONTAINER_INHERIT_ACE || uint32(ace.Mask) != 0x001f01ff {
			return false
		}
		principal := (*windows.SID)(unsafe.Pointer(&ace.SidStart)).String()
		if _, known := principals[principal]; !known || principals[principal] {
			return false
		}
		principals[principal] = true
	}
	return principals[owner] && principals[capability]
}

func windowsCurrentUserAssertOrdinaryTarget(process windows.Handle) error {
	var token windows.Token
	if err := windows.OpenProcessToken(process, windows.TOKEN_QUERY, &token); err != nil {
		return err
	}
	defer token.Close()
	var elevated, returned uint32
	if err := windows.GetTokenInformation(token, windows.TokenElevation, (*byte)(unsafe.Pointer(&elevated)), uint32(unsafe.Sizeof(elevated)), &returned); err != nil || returned != uint32(unsafe.Sizeof(elevated)) || elevated != 0 {
		return errors.New("target is elevated")
	}
	var needed uint32
	err := windows.GetTokenInformation(token, windows.TokenIntegrityLevel, nil, 0, &needed)
	if !errors.Is(err, windows.ERROR_INSUFFICIENT_BUFFER) || needed < uint32(unsafe.Sizeof(windows.Tokenmandatorylabel{})) || needed > 64<<10 {
		return errors.New("target integrity unavailable")
	}
	buffer := make([]byte, needed)
	if err := windows.GetTokenInformation(token, windows.TokenIntegrityLevel, &buffer[0], uint32(len(buffer)), &needed); err != nil {
		return err
	}
	label := (*windows.Tokenmandatorylabel)(unsafe.Pointer(&buffer[0])).Label
	if label.Sid == nil || label.Sid.SubAuthorityCount() == 0 || label.Sid.SubAuthority(uint32(label.Sid.SubAuthorityCount()-1)) >= 0x3000 {
		return errors.New("target integrity is high")
	}
	groups, err := token.GetTokenGroups()
	if err != nil || groups == nil {
		return errors.New("target groups unavailable")
	}
	for _, group := range groups.AllGroups() {
		if group.Sid != nil && group.Sid.String() == windowsCurrentUserAdministratorsSID && (group.Attributes&windows.SE_GROUP_ENABLED != 0 || group.Attributes&windows.SE_GROUP_USE_FOR_DENY_ONLY == 0) {
			return errors.New("administrators is enabled")
		}
	}
	return nil
}
