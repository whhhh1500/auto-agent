//go:build windows && sandboxacceptance

// windows-sandbox-basic-unelevated is a development-only current-user
// acceptance command. The parent creates only a D-backed fixture tree and
// starts this already-compiled executable through the public sandbox API.
package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/cc-auto-agent/harness-core/internal/sandboxacceptance"
	"github.com/cc-auto-agent/harness-core/pkg/execution/sandbox"
	"golang.org/x/sys/windows"
)

const (
	basicTempRoot       = `D:\cc\auto_agent\.codex-tmp`
	basicRootPrefix     = "harness-windows-sandbox-basic-"
	basicOutputLimit    = int64(1024)
	basicFixtureContent = "basic-write-ok"
)

func main() {
	var err error
	if len(os.Args) > 1 && os.Args[1] == "--target" {
		err = runTarget(os.Args[2:])
	} else if len(os.Args) > 1 && os.Args[1] == "--medium-child" {
		err = runAcceptance()
	} else if windows.GetCurrentProcessToken().IsElevated() {
		program, lookupErr := os.Executable()
		if lookupErr != nil {
			err = acceptanceFailure("medium-launch")
		} else {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			err = sandboxacceptance.RunMediumTestProgram(ctx, program, []string{"--medium-child"})
			cancel()
		}
	} else {
		err = runAcceptance()
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "BASIC_UNELEVATED status=failed class=%s\n", acceptanceClass(err))
		os.Exit(1)
	}
	if len(os.Args) == 1 {
		fmt.Println("BASIC_UNELEVATED status=passed network=host network_isolation=false offline_hint=informational loopback=reachable desktop_default=true desktop_compatibility=true")
	}
}

type acceptanceFailure string

func (failure acceptanceFailure) Error() string { return string(failure) }

func acceptanceClass(err error) string {
	var failure acceptanceFailure
	if errors.As(err, &failure) {
		return string(failure)
	}
	return "unexpected"
}

type fixture struct {
	base, work, sessions string
	executable           string
	lastWorkspace        string
	lastArtifacts        string
	lastExecutable       string
	sessionPaths         []fixturePathState
}

func runAcceptance() error {
	if windows.GetCurrentProcessToken().IsElevated() {
		return acceptanceFailure("requires-unelevated-medium-context")
	}
	for _, name := range []string{"HARNESS_WINDOWS_SANDBOX_PRIVATE_DESKTOP", "HARNESS_WINDOWS_SANDBOX_OFFLINE_HINTS"} {
		if value, present := os.LookupEnv(name); present && value != "true" {
			return acceptanceFailure("default-config")
		}
	}
	if err := os.MkdirAll(basicTempRoot, 0o700); err != nil {
		return acceptanceFailure("temp-root")
	}
	base, err := newBasicRoot()
	if err != nil {
		return acceptanceFailure("root")
	}
	fixture := fixture{base: base, work: filepath.Join(base, "sandbox"), sessions: filepath.Join(base, "sandbox", "sessions")}
	fixture.executable, err = os.Executable()
	if err != nil {
		return acceptanceFailure("executable")
	}
	if filepath.Ext(fixture.executable) != ".exe" || filepath.Clean(fixture.executable) != fixture.executable {
		return acceptanceFailure("executable")
	}
	if err := os.MkdirAll(fixture.sessions, 0o700); err != nil {
		return acceptanceFailure("root")
	}
	cleanupDone := false
	defer func() {
		if !cleanupDone {
			_ = removeFixture(&fixture)
		}
	}()
	registration, err := sandbox.NewLocalRegistrationForWorkRoot(fixture.work)
	if err != nil {
		return acceptanceFailure("registration")
	}
	registry, err := sandbox.NewRegistry(1, registration)
	if err != nil {
		return acceptanceFailure("registry")
	}
	report, err := registry.Probe(context.Background(), registration.Metadata.ID, registration.Metadata.Version)
	if err != nil || !report.Available {
		return acceptanceFailure("probe")
	}
	if report.Network != sandbox.NetworkHost || report.Actual.NetworkIsolation || len(report.SupportedNetworks) != 1 || report.SupportedNetworks[0] != sandbox.NetworkHost {
		return acceptanceFailure("weak-network-report")
	}
	if err := assertStrictDisabledRejected(registry, registration, &fixture); err != nil {
		return err
	}
	if err := runWriteCase(registry, registration, &fixture); err != nil {
		return err
	}
	if err := runTokenCase(registry, registration, &fixture); err != nil {
		return err
	}
	if err := runOutsideWriteCase(registry, registration, &fixture); err != nil {
		return err
	}
	if err := runCmdCase(registry, registration, &fixture); err != nil {
		return err
	}
	if err := runNetworkCase(registry, registration, &fixture); err != nil {
		return err
	}
	if err := runExplicitDesktopCompatibility(&fixture); err != nil {
		return err
	}
	if err := runOutputCase(registry, registration, &fixture); err != nil {
		return err
	}
	if err := runTimeoutCase(registry, registration, &fixture); err != nil {
		return err
	}
	if err := runCancelDescendantCase(registry, registration, &fixture); err != nil {
		return err
	}
	if err := runNormalExitDescendantCase(registry, registration, &fixture); err != nil {
		return err
	}
	if err := removeFixture(&fixture); err != nil {
		return acceptanceFailure("cleanup")
	}
	cleanupDone = true
	return nil
}

func newBasicRoot() (string, error) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", err
	}
	root := filepath.Join(basicTempRoot, basicRootPrefix+hex.EncodeToString(nonce[:]))
	if err := os.Mkdir(root, 0o700); err != nil {
		return "", err
	}
	return root, nil
}

func assertStrictDisabledRejected(registry *sandbox.Registry, registration sandbox.Registration, fixture *fixture) error {
	spec, cleanup, err := newSpec(fixture, sandbox.NetworkDisabled, true)
	if err != nil {
		return acceptanceFailure("strict-spec")
	}
	defer cleanup()
	if _, err := registry.Start(context.Background(), registration.Metadata.ID, registration.Metadata.Version, spec); !errors.Is(err, sandbox.ErrAssuranceTooWeak) {
		return acceptanceFailure("strict-disabled-admission")
	}
	return nil
}

func runWriteCase(registry *sandbox.Registry, registration sandbox.Registration, fixture *fixture) error {
	result, err := execute(registry, registration, fixture, "write", nil)
	if err != nil || result.ExitCode != 0 || string(result.Stdout.Preview) != "BASIC_TARGET case=write status=ok\n" {
		return acceptanceFailure("workspace-write")
	}
	path := filepath.Join(fixture.lastWorkspace, "basic-write.txt")
	contents, readErr := os.ReadFile(path)
	if readErr != nil || string(contents) != basicFixtureContent {
		return acceptanceFailure("workspace-write-proof")
	}
	return nil
}

func runTokenCase(registry *sandbox.Registry, registration sandbox.Registration, fixture *fixture) error {
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil || user == nil || user.User.Sid == nil {
		return acceptanceFailure("source-token")
	}
	result, err := execute(registry, registration, fixture, "token", []string{user.User.Sid.String()})
	if err != nil || result.ExitCode != 0 || string(result.Stdout.Preview) != "BASIC_TARGET case=token status=restricted same-user non-elevated\n" {
		return acceptanceFailure("restricted-token")
	}
	return nil
}

func runOutsideWriteCase(registry *sandbox.Registry, registration sandbox.Registration, fixture *fixture) error {
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil || user == nil || user.User.Sid == nil {
		return acceptanceFailure("outside-token")
	}
	logonSID, err := currentLogonSID()
	if err != nil {
		return acceptanceFailure("outside-token")
	}
	cases := []struct {
		name, sid, status, content string
	}{
		{name: "current-user", sid: user.User.Sid.String(), status: "denied", content: "unchanged"},
		{name: "everyone", sid: "WD", status: "allowed", content: "xnchanged"},
		{name: "logon", sid: logonSID, status: "allowed", content: "xnchanged"},
	}
	for _, test := range cases {
		outside := filepath.Join(fixture.base, "outside-"+test.name+".txt")
		if err := os.WriteFile(outside, []byte("unchanged"), 0o600); err != nil {
			return acceptanceFailure("outside-prepare")
		}
		verifyFile, err := os.Open(outside)
		if err != nil {
			return acceptanceFailure("outside-prepare")
		}
		if err := setOutsideWriteDACL(outside, user.User.Sid.String(), test.sid); err != nil {
			_ = verifyFile.Close()
			return acceptanceFailure("outside-prepare")
		}
		result, execErr := execute(registry, registration, fixture, "outside-write", []string{outside})
		if execErr != nil || result.ExitCode != 0 || string(result.Stdout.Preview) != "BASIC_TARGET case=outside-write status="+test.status+"\n" {
			_ = verifyFile.Close()
			return acceptanceFailure("outside-write")
		}
		_, _ = verifyFile.Seek(0, io.SeekStart)
		contents, readErr := io.ReadAll(verifyFile)
		_ = verifyFile.Close()
		if readErr != nil || string(contents) != test.content {
			return acceptanceFailure("outside-write-proof")
		}
		if err := removeKnownFile(outside); err != nil {
			return acceptanceFailure("outside-cleanup")
		}
	}
	return nil
}

func currentLogonSID() (string, error) {
	groups, err := windows.GetCurrentProcessToken().GetTokenGroups()
	if err != nil || groups == nil {
		return "", err
	}
	for _, group := range groups.AllGroups() {
		if group.Sid != nil && group.Attributes&windows.SE_GROUP_LOGON_ID != 0 {
			return group.Sid.String(), nil
		}
	}
	return "", errors.New("logon sid unavailable")
}

func setOutsideWriteDACL(path, owner, sid string) error {
	if path == "" || owner == "" || sid == "" {
		return errors.New("outside dacl input")
	}
	// The parent needs DELETE to remove this owned fixture after the target
	// probe. The child requests only FILE_WRITE_DATA|SYNCHRONIZE; the owner ACE
	// satisfies the normal token and the second ACE supplies the tested SID.
	descriptor, err := windows.SecurityDescriptorFromString("D:P(A;;0x00110002;;;" + owner + ")(A;;0x00100002;;;" + sid + ")")
	if err != nil || descriptor == nil {
		return errors.New("outside dacl descriptor")
	}
	dacl, defaulted, err := descriptor.DACL()
	if err != nil || defaulted || dacl == nil {
		return errors.New("outside dacl readback")
	}
	err = windows.SetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION, nil, nil, dacl, nil)
	runtime.KeepAlive(descriptor)
	return err
}

func runCmdCase(registry *sandbox.Registry, registration sandbox.Registration, fixture *fixture) error {
	systemDirectory, err := windows.GetSystemDirectory()
	if err != nil {
		return acceptanceFailure("cmd-path")
	}
	cmdPath := filepath.Join(systemDirectory, "cmd.exe")
	result, err := execute(registry, registration, fixture, cmdPath, []string{"/d", "/c", "exit", "0"})
	if err != nil || result.ExitCode != 0 || result.Stdout.Size != 0 || result.Stderr.Size != 0 {
		return acceptanceFailure("cmd-exec")
	}
	return nil
}

func runNetworkCase(registry *sandbox.Registry, registration sandbox.Registration, fixture *fixture) error {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return acceptanceFailure("network-weak-report")
	}
	defer listener.Close()
	tcpListener, ok := listener.(*net.TCPListener)
	if !ok || tcpListener.SetDeadline(time.Now().Add(6*time.Second)) != nil {
		return acceptanceFailure("network-weak-report")
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return acceptanceFailure("network-weak-report")
	}
	marker := "basic-loopback-" + hex.EncodeToString(nonce[:])
	resultCh := make(chan struct {
		result sandbox.ExecResult
		err    error
	}, 1)
	go func() {
		result, execErr := execute(registry, registration, fixture, "network", []string{listener.Addr().String(), marker})
		resultCh <- struct {
			result sandbox.ExecResult
			err    error
		}{result, execErr}
	}()
	connection, acceptErr := listener.Accept()
	var received []byte
	var readErr error
	if acceptErr == nil {
		if err := connection.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			readErr = err
		} else {
			received, readErr = io.ReadAll(io.LimitReader(connection, int64(len(marker)+1)))
		}
		if err := connection.Close(); readErr == nil {
			readErr = err
		}
	}
	outcome := <-resultCh
	if acceptErr != nil || readErr != nil || string(received) != marker || outcome.err != nil || outcome.result.ExitCode != 0 || string(outcome.result.Stdout.Preview) != "BASIC_TARGET case=network status=weak offline_hint=ok loopback=ok\n" || outcome.result.Stderr.Size != 0 {
		return acceptanceFailure("network-weak-report")
	}
	return nil
}

func runExplicitDesktopCompatibility(fixture *fixture) error {
	previous, hadPrevious := os.LookupEnv("HARNESS_WINDOWS_SANDBOX_PRIVATE_DESKTOP")
	if err := os.Setenv("HARNESS_WINDOWS_SANDBOX_PRIVATE_DESKTOP", "false"); err != nil {
		return acceptanceFailure("desktop-compatibility")
	}
	defer func() {
		if hadPrevious {
			_ = os.Setenv("HARNESS_WINDOWS_SANDBOX_PRIVATE_DESKTOP", previous)
		} else {
			_ = os.Unsetenv("HARNESS_WINDOWS_SANDBOX_PRIVATE_DESKTOP")
		}
	}()
	registration, err := sandbox.NewLocalRegistrationForWorkRoot(fixture.work)
	if err != nil {
		return acceptanceFailure("desktop-compatibility")
	}
	registry, err := sandbox.NewRegistry(1, registration)
	if err != nil {
		return acceptanceFailure("desktop-compatibility")
	}
	if report, err := registry.Probe(context.Background(), registration.Metadata.ID, registration.Metadata.Version); err != nil || !report.Available {
		return acceptanceFailure("desktop-compatibility")
	}
	if err := runCmdCase(registry, registration, fixture); err != nil {
		return acceptanceFailure("desktop-compatibility")
	}
	return nil
}

func runOutputCase(registry *sandbox.Registry, registration sandbox.Registration, fixture *fixture) error {
	result, err := executeWithLimits(registry, registration, fixture, "output", []string{"8192"}, sandbox.Limits{MaxOutputBytes: basicOutputLimit})
	// The Windows pipe reader can receive the whole 8 KiB write at once. The
	// collector rejects that chunk before retaining it, so the retained size can
	// be zero; the contract is the output-limit result plus bounded retention.
	if !errors.Is(err, sandbox.ErrExecOutputLimit) || result.Stdout.Size+result.Stderr.Size > basicOutputLimit || int64(len(result.Stdout.Preview)) > sandbox.DefaultExecPreviewBytes || int64(len(result.Stderr.Preview)) > sandbox.DefaultExecPreviewBytes {
		return acceptanceFailure("output-cap")
	}
	return nil
}

func runTimeoutCase(registry *sandbox.Registry, registration sandbox.Registration, fixture *fixture) error {
	// Account for the current-user setup path while still requiring the Job to
	// terminate a target that outlives the five-second execution budget.
	result, err := executeWithLimits(registry, registration, fixture, "sleep", []string{"10000"}, sandbox.Limits{WallTime: 5 * time.Second})
	if !errors.Is(err, sandbox.ErrExecTimeout) || !result.TimedOut {
		return acceptanceFailure("timeout")
	}
	return nil
}

func runCancelDescendantCase(registry *sandbox.Registry, registration sandbox.Registration, fixture *fixture) error {
	_, err, child := executeAsync(registry, registration, fixture, "spawn", []string{"child.marker", "30000"})
	if !errors.Is(err, context.Canceled) || child.pid == 0 || child.creation == 0 {
		return acceptanceFailure("cancel-start")
	}
	if !waitProcessGone(child, 10*time.Second) {
		return acceptanceFailure("cancel-descendant")
	}
	marker := filepath.Join(fixture.lastArtifacts, "child.marker")
	if _, err := os.Stat(marker); err != nil {
		return acceptanceFailure("cancel-marker")
	}
	return nil
}

func runNormalExitDescendantCase(registry *sandbox.Registry, registration sandbox.Registration, fixture *fixture) error {
	spec, _, err := newSpec(fixture, sandbox.NetworkHost, false)
	if err != nil {
		return acceptanceFailure("normal-exit-start")
	}
	session, err := registry.Start(context.Background(), registration.Metadata.ID, registration.Metadata.Version, spec)
	if err != nil {
		return acceptanceFailure("normal-exit-start")
	}
	fixture.lastWorkspace = spec.Mounts[0].Source
	fixture.lastArtifacts = spec.Mounts[1].Source
	closed := false
	defer func() {
		if !closed {
			_ = session.Close(context.Background())
		}
	}()
	execSession, ok := session.(sandbox.ExecSession)
	if !ok {
		return acceptanceFailure("normal-exit-session")
	}
	result, err := execSession.Exec(context.Background(), sandbox.ExecRequest{
		Args:           append([]string{fixture.lastExecutable, "--target", "spawn-normal-exit"}, "normal-child.marker", "30000", "250"),
		Limits:         spec.Limits,
		Network:        spec.Network,
		ArtifactPolicy: spec.ArtifactPolicy,
		Lease:          spec.Lease,
	})
	if err != nil || result.ExitCode != 0 {
		return acceptanceFailure("normal-exit")
	}
	marker := filepath.Join(fixture.lastArtifacts, "normal-child.marker")
	data, err := os.ReadFile(marker)
	if err != nil {
		return acceptanceFailure("normal-exit-marker")
	}
	child, err := parseChildMarker(string(data))
	if err != nil || !waitProcessGone(child, 10*time.Second) {
		return acceptanceFailure("normal-exit-descendant")
	}
	if err := session.Close(context.Background()); err != nil {
		return acceptanceFailure("normal-exit-session-close")
	}
	closed = true
	if err := removeKnownFile(marker); err != nil {
		return acceptanceFailure("normal-exit-cleanup")
	}
	return nil
}

// fixturePathState is kept private to the command so a fresh session can be
// removed with exact os.Remove calls after the provider closes it.
type fixturePathState struct {
	workspace, artifacts, executable, sessionRoot string
}

func (f *fixture) newSessionPaths() (fixturePathState, func(), error) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return fixturePathState{}, nil, err
	}
	root := filepath.Join(f.sessions, "session-"+hex.EncodeToString(nonce[:]))
	paths := fixturePathState{sessionRoot: root, workspace: filepath.Join(root, "workspace"), artifacts: filepath.Join(root, "artifacts"), executable: filepath.Join(root, "workspace", "basic-target.exe")}
	f.sessionPaths = append(f.sessionPaths, paths)
	for _, path := range []string{root, paths.workspace, paths.artifacts, filepath.Join(root, "home"), filepath.Join(root, "tmp"), filepath.Join(root, "cache")} {
		if err := os.Mkdir(path, 0o700); err != nil {
			return fixturePathState{}, nil, err
		}
	}
	source, err := os.Open(f.executable)
	if err != nil {
		return fixturePathState{}, nil, err
	}
	target, err := os.OpenFile(paths.executable, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o700)
	if err == nil {
		_, err = io.Copy(target, source)
		if closeErr := target.Close(); err == nil {
			err = closeErr
		}
	}
	if closeErr := source.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		_ = removeKnownFile(paths.executable)
		return fixturePathState{}, nil, err
	}
	return paths, func() { _ = removeEmptyFixtureTree(paths) }, nil
}

func newSpec(f *fixture, network sandbox.NetworkPolicy, isolation bool) (sandbox.SessionSpec, func(), error) {
	paths, cleanup, err := f.newSessionPaths()
	if err != nil {
		return sandbox.SessionSpec{}, nil, err
	}
	limits := sandbox.Limits{WallTime: 5 * time.Second, MaxCPUTime: 5 * time.Second, MaxMemoryBytes: 128 << 20, MaxOutputBytes: basicOutputLimit}
	spec := sandbox.SessionSpec{RequestedAssurance: sandbox.Assurance{Level: sandbox.AssuranceProcess, SharedKernel: true, NetworkIsolation: isolation}, Limits: limits, Network: network, ArtifactPolicy: sandbox.ArtifactPolicy{MaxArtifacts: 4, MaxTotalBytes: 1 << 20}, Mounts: []sandbox.Mount{{Source: paths.workspace, Target: "/workspace"}, {Source: paths.artifacts, Target: "/artifacts"}}, Lease: sandbox.LeaseIdentity{RunID: "windows-basic", SegmentID: compactLease(paths.sessionRoot), ModuleID: "sandbox.exec", CompositionRev: "windows-current-user-basic-v1", Token: compactLease(paths.workspace)}}
	if err := spec.Validate(); err != nil {
		cleanup()
		return sandbox.SessionSpec{}, nil, err
	}
	f.lastExecutable = paths.executable
	return spec, func() {}, nil
}

func compactLease(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func execute(registry *sandbox.Registry, registration sandbox.Registration, f *fixture, kind string, args []string) (sandbox.ExecResult, error) {
	return executeWithLimits(registry, registration, f, kind, args, sandbox.Limits{})
}

func executeWithLimits(registry *sandbox.Registry, registration sandbox.Registration, f *fixture, kind string, args []string, override sandbox.Limits) (sandbox.ExecResult, error) {
	spec, _, err := newSpec(f, sandbox.NetworkHost, false)
	if err != nil {
		return sandbox.ExecResult{}, err
	}
	if override.WallTime > 0 {
		spec.Limits.WallTime = override.WallTime
	}
	if override.MaxOutputBytes > 0 {
		spec.Limits.MaxOutputBytes = override.MaxOutputBytes
	}
	executable := f.lastExecutable
	argv := append([]string{executable, "--target", kind}, args...)
	if filepath.IsAbs(kind) {
		executable = kind
		argv = append([]string{kind}, args...)
	}
	session, err := registry.Start(context.Background(), registration.Metadata.ID, registration.Metadata.Version, spec)
	if err != nil {
		return sandbox.ExecResult{}, err
	}
	execSession, ok := session.(sandbox.ExecSession)
	if !ok {
		_ = session.Close(context.Background())
		return sandbox.ExecResult{}, acceptanceFailure("session-contract")
	}
	f.lastWorkspace = spec.Mounts[0].Source
	f.lastArtifacts = spec.Mounts[1].Source
	result, execErr := execSession.Exec(context.Background(), sandbox.ExecRequest{Args: argv, Limits: spec.Limits, Network: spec.Network, ArtifactPolicy: spec.ArtifactPolicy, Lease: spec.Lease})
	closeErr := session.Close(context.Background())
	if closeErr != nil {
		return result, errors.Join(execErr, acceptanceFailure("session-close"))
	}
	return result, execErr
}

func executeAsync(registry *sandbox.Registry, registration sandbox.Registration, f *fixture, kind string, args []string) (sandbox.ExecResult, error, childIdentity) {
	spec, _, err := newSpec(f, sandbox.NetworkHost, false)
	if err != nil {
		return sandbox.ExecResult{}, err, childIdentity{}
	}
	session, err := registry.Start(context.Background(), registration.Metadata.ID, registration.Metadata.Version, spec)
	if err != nil {
		return sandbox.ExecResult{}, err, childIdentity{}
	}
	execSession, ok := session.(sandbox.ExecSession)
	if !ok {
		closeErr := session.Close(context.Background())
		return sandbox.ExecResult{}, errors.Join(acceptanceFailure("session-contract"), closeAcceptanceError(closeErr)), childIdentity{}
	}
	f.lastWorkspace = spec.Mounts[0].Source
	f.lastArtifacts = spec.Mounts[1].Source
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resultCh := make(chan struct {
		result sandbox.ExecResult
		err    error
	}, 1)
	go func() {
		result, execErr := execSession.Exec(ctx, sandbox.ExecRequest{Args: append([]string{f.lastExecutable, "--target", kind}, args...), Limits: spec.Limits, Network: spec.Network, ArtifactPolicy: spec.ArtifactPolicy, Lease: spec.Lease})
		resultCh <- struct {
			result sandbox.ExecResult
			err    error
		}{result, execErr}
	}()
	marker := filepath.Join(f.lastArtifacts, args[0])
	var child childIdentity
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if data, readErr := os.ReadFile(marker); readErr == nil {
			child, _ = parseChildMarker(string(data))
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if child.pid == 0 || child.creation == 0 {
		cancel()
		outcome := <-resultCh
		if closeErr := session.Close(context.Background()); closeErr != nil {
			outcome.err = errors.Join(outcome.err, acceptanceFailure("session-close"))
		}
		return outcome.result, errors.Join(acceptanceFailure("cancel-marker"), outcome.err), childIdentity{}
	}
	cancel()
	outcome := <-resultCh
	if closeErr := session.Close(context.Background()); closeErr != nil {
		outcome.err = errors.Join(outcome.err, acceptanceFailure("session-close"))
	}
	return outcome.result, outcome.err, child
}

func closeAcceptanceError(err error) error {
	if err != nil {
		return acceptanceFailure("session-close")
	}
	return nil
}

func removeEmptyFixtureTree(paths fixturePathState) error {
	for _, path := range []string{paths.executable, filepath.Join(paths.workspace, "basic-write.txt"), filepath.Join(paths.artifacts, "child.marker"), filepath.Join(paths.artifacts, "normal-child.marker")} {
		if err := removeKnownFile(path); err != nil {
			return err
		}
	}
	for _, path := range []string{filepath.Join(paths.sessionRoot, "home"), filepath.Join(paths.sessionRoot, "tmp"), filepath.Join(paths.sessionRoot, "cache"), paths.workspace, paths.artifacts, paths.sessionRoot} {
		if err := removeEmpty(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func removeFixture(f *fixture) error {
	if f == nil {
		return acceptanceFailure("cleanup-fixture")
	}
	for _, paths := range f.sessionPaths {
		if err := removeEmptyFixtureTree(paths); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	for _, name := range []string{"current-user", "everyone", "logon"} {
		if err := removeKnownFile(filepath.Join(f.base, "outside-"+name+".txt")); err != nil {
			return err
		}
	}
	for _, path := range []string{f.sessions, f.work, f.base} {
		if err := removeEmpty(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func removeKnownFile(path string) error {
	err := os.Remove(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func removeEmpty(path string) error {
	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	if len(entries) != 0 {
		return acceptanceFailure("cleanup-nonempty")
	}
	return os.Remove(path)
}

func runTarget(args []string) error {
	if len(args) == 0 {
		return acceptanceFailure("target-args")
	}
	switch args[0] {
	case "token":
		if len(args) != 2 || args[1] == "" {
			return acceptanceFailure("target-args")
		}
		token := windows.GetCurrentProcessToken()
		user, err := token.GetTokenUser()
		if err != nil || user == nil || user.User.Sid == nil || user.User.Sid.String() != args[1] || token.IsElevated() {
			return acceptanceFailure("target-token-identity")
		}
		restricted, err := token.IsRestricted()
		if err != nil || !restricted {
			return acceptanceFailure("target-token-restricted")
		}
		fmt.Println("BASIC_TARGET case=token status=restricted same-user non-elevated")
		return nil
	case "write":
		if len(args) != 1 {
			return acceptanceFailure("target-args")
		}
		workspace := os.Getenv("HARNESS_WORKSPACE")
		if err := os.WriteFile(filepath.Join(workspace, "basic-write.txt"), []byte(basicFixtureContent), 0o600); err != nil {
			return err
		}
		fmt.Println("BASIC_TARGET case=write status=ok")
		return nil
	case "outside-write":
		if len(args) != 2 {
			return acceptanceFailure("target-args")
		}
		name, err := windows.UTF16PtrFromString(args[1])
		if err != nil {
			return err
		}
		handle, err := windows.CreateFile(name, windows.FILE_WRITE_DATA|windows.SYNCHRONIZE, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
		if errors.Is(err, windows.ERROR_ACCESS_DENIED) {
			fmt.Println("BASIC_TARGET case=outside-write status=denied")
			return nil
		}
		if err != nil {
			return err
		}
		file := os.NewFile(uintptr(handle), "basic-outside-write")
		if file == nil {
			_ = windows.CloseHandle(handle)
			return errors.New("outside write handle")
		}
		if _, err := file.WriteAt([]byte("x"), 0); err != nil {
			_ = file.Close()
			return err
		}
		if err := file.Close(); err != nil {
			return err
		}
		fmt.Println("BASIC_TARGET case=outside-write status=allowed")
		return nil
	case "output":
		if len(args) != 2 {
			return acceptanceFailure("target-args")
		}
		count, err := strconv.Atoi(args[1])
		if err != nil || count < 1 || count > 1<<20 {
			return acceptanceFailure("target-args")
		}
		_, err = io.CopyN(os.Stdout, strings.NewReader(strings.Repeat("x", count)), int64(count))
		return err
	case "sleep":
		milliseconds, err := strconv.Atoi(args[1])
		if err != nil || milliseconds < 1 {
			return acceptanceFailure("target-args")
		}
		time.Sleep(time.Duration(milliseconds) * time.Millisecond)
		return nil
	case "network":
		if len(args) != 3 || !validLoopbackTarget(args[1]) || len(args[2]) == 0 || len(args[2]) > 64 || strings.ContainsAny(args[2], "\x00\r\n") {
			return acceptanceFailure("target-args")
		}
		for key, expected := range map[string]string{
			"HTTP_PROXY":         "http://127.0.0.1:9",
			"HTTPS_PROXY":        "http://127.0.0.1:9",
			"ALL_PROXY":          "http://127.0.0.1:9",
			"CARGO_NET_OFFLINE":  "true",
			"npm_config_offline": "true",
		} {
			if os.Getenv(key) != expected {
				return acceptanceFailure("network-env")
			}
		}
		connection, err := net.DialTimeout("tcp", args[1], time.Second)
		if err != nil {
			return err
		}
		if err := connection.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
			_ = connection.Close()
			return err
		}
		written, err := connection.Write([]byte(args[2]))
		if err != nil || written != len(args[2]) {
			_ = connection.Close()
			if err != nil {
				return err
			}
			return io.ErrShortWrite
		}
		if err := connection.Close(); err != nil {
			return err
		}
		fmt.Println("BASIC_TARGET case=network status=weak offline_hint=ok loopback=ok")
		return nil
	case "spawn":
		if len(args) != 3 {
			return acceptanceFailure("target-args")
		}
		return runSpawnTarget(args[1], args[2], args[2], false)
	case "spawn-normal-exit":
		if len(args) != 4 {
			return acceptanceFailure("target-args")
		}
		return runSpawnTarget(args[1], args[2], args[3], true)
	case "wait":
		if len(args) != 3 {
			return acceptanceFailure("target-args")
		}
		milliseconds, err := strconv.Atoi(args[2])
		if err != nil || milliseconds < 1 {
			return acceptanceFailure("target-args")
		}
		time.Sleep(time.Duration(milliseconds) * time.Millisecond)
		return nil
	default:
		return acceptanceFailure("target-args")
	}
}

func runSpawnTarget(markerName, childDuration, parentDuration string, requireLiveChild bool) error {
	childMilliseconds, err := strconv.Atoi(childDuration)
	if err != nil || childMilliseconds < 1 {
		return acceptanceFailure("target-args")
	}
	parentMilliseconds, err := strconv.Atoi(parentDuration)
	if err != nil || parentMilliseconds < 1 || (requireLiveChild && parentMilliseconds >= childMilliseconds) {
		return acceptanceFailure("target-args")
	}
	artifacts := os.Getenv("HARNESS_ARTIFACTS")
	child := exec.Command(os.Args[0], "--target", "wait", markerName, childDuration)
	child.Stdout, child.Stderr = io.Discard, io.Discard
	if err := child.Start(); err != nil {
		return err
	}
	process, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(child.Process.Pid))
	if err != nil {
		return err
	}
	defer windows.CloseHandle(process)
	creation, exit, kernel, user := windows.Filetime{}, windows.Filetime{}, windows.Filetime{}, windows.Filetime{}
	if err := windows.GetProcessTimes(process, &creation, &exit, &kernel, &user); err != nil {
		return err
	}
	marker := fmt.Sprintf("pid=%d;creation=%d", child.Process.Pid, uint64(creation.HighDateTime)<<32|uint64(creation.LowDateTime))
	if err := os.WriteFile(filepath.Join(artifacts, markerName), []byte(marker), 0o600); err != nil {
		return err
	}
	time.Sleep(time.Duration(parentMilliseconds) * time.Millisecond)
	if requireLiveChild {
		var exitCode uint32
		if err := windows.GetExitCodeProcess(process, &exitCode); err != nil || exitCode != 259 {
			return acceptanceFailure("normal-exit-child")
		}
	}
	return nil
}
func validLoopbackTarget(address string) bool {
	host, port, err := net.SplitHostPort(address)
	if err != nil || host != "127.0.0.1" {
		return false
	}
	value, err := strconv.Atoi(port)
	return err == nil && value > 0 && value <= 65535
}

type childIdentity struct {
	pid      uint32
	creation uint64
}

func parseChildMarker(value string) (childIdentity, error) {
	parts := strings.Split(value, ";")
	if len(parts) != 2 || !strings.HasPrefix(parts[0], "pid=") || !strings.HasPrefix(parts[1], "creation=") {
		return childIdentity{}, acceptanceFailure("child-marker")
	}
	pid, err := strconv.ParseUint(strings.TrimPrefix(parts[0], "pid="), 10, 32)
	if err != nil || pid == 0 {
		return childIdentity{}, err
	}
	creation, err := strconv.ParseUint(strings.TrimPrefix(parts[1], "creation="), 10, 64)
	if err != nil || creation == 0 {
		return childIdentity{}, err
	}
	return childIdentity{pid: uint32(pid), creation: creation}, nil
}

func waitProcessGone(identity childIdentity, timeout time.Duration) bool {
	process, err := windows.OpenProcess(windows.SYNCHRONIZE|windows.PROCESS_QUERY_LIMITED_INFORMATION, false, identity.pid)
	if err != nil {
		return errors.Is(err, windows.ERROR_INVALID_PARAMETER)
	}
	defer windows.CloseHandle(process)
	creation, exit, kernel, user := windows.Filetime{}, windows.Filetime{}, windows.Filetime{}, windows.Filetime{}
	if err := windows.GetProcessTimes(process, &creation, &exit, &kernel, &user); err != nil {
		return false
	}
	currentCreation := uint64(creation.HighDateTime)<<32 | uint64(creation.LowDateTime)
	if currentCreation != identity.creation {
		return true
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		state, waitErr := windows.WaitForSingleObject(process, 0)
		if waitErr == nil && state == windows.WAIT_OBJECT_0 {
			return true
		}
		time.Sleep(25 * time.Millisecond)
	}
	return false
}
