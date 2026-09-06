//go:build windows

package sandbox

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

// windowsCurrentUserBackend is the unelevated local Windows implementation.
// It intentionally has no setup installation, account, helper, control pipe,
// firewall, or token/process-object ACL dependency.
type windowsCurrentUserBackend struct {
	workRoot       string
	privateDesktop bool
	offlineHints   bool

	mu     sync.Mutex
	active *windowsCurrentUserSession
}

var _ windowsSessionBackend = (*windowsCurrentUserBackend)(nil)

func cloneWindowsBackendStart(start windowsBackendStart) windowsBackendStart {
	return windowsBackendStart{
		Spec:        cloneSessionSpec(start.Spec),
		Root:        start.Root,
		Workspace:   start.Workspace,
		Artifacts:   start.Artifacts,
		Environment: append([]string(nil), start.Environment...),
	}
}

func newWindowsCurrentUserBackend(workRoot string) (*windowsCurrentUserBackend, error) {
	privateDesktop, err := windowsCurrentUserBooleanEnvironment("HARNESS_WINDOWS_SANDBOX_PRIVATE_DESKTOP", true)
	if err != nil {
		return nil, ErrUnavailable
	}
	offlineHints, err := windowsCurrentUserBooleanEnvironment("HARNESS_WINDOWS_SANDBOX_OFFLINE_HINTS", true)
	if err != nil {
		return nil, ErrUnavailable
	}
	return &windowsCurrentUserBackend{workRoot: workRoot, privateDesktop: privateDesktop, offlineHints: offlineHints}, nil
}

func windowsCurrentUserBooleanEnvironment(name string, defaultValue bool) (bool, error) {
	value, present := os.LookupEnv(name)
	if !present {
		return defaultValue, nil
	}
	switch value {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, ErrUnavailable
	}
}

func (backend *windowsCurrentUserBackend) Probe(ctx context.Context) error {
	if backend == nil || ctx == nil || ctx.Err() != nil || backend.workRoot == "" {
		return ErrUnavailable
	}
	if elevated, err := windowsCurrentUserIsElevated(); !windowsCurrentUserSourceEligible(elevated, err) {
		return ErrUnavailable
	}
	return windowsCurrentUserSupportsJobList(ctx)
}

func (backend *windowsCurrentUserBackend) ProbeActive(ctx context.Context) error {
	if backend == nil || ctx == nil || ctx.Err() != nil {
		return ErrUnavailable
	}
	backend.mu.Lock()
	active := backend.active
	backend.mu.Unlock()
	if active == nil {
		return ErrUnavailable
	}
	return active.probe(ctx)
}

func (backend *windowsCurrentUserBackend) Start(ctx context.Context, start windowsBackendStart) (windowsBackendSession, error) {
	if backend == nil || ctx == nil || ctx.Err() != nil || validateWindowsCurrentUserStart(backend.workRoot, start) != nil {
		return nil, ErrUnavailable
	}
	if elevated, err := windowsCurrentUserIsElevated(); !windowsCurrentUserSourceEligible(elevated, err) {
		return nil, ErrUnavailable
	}
	authority, err := newWindowsSessionAuthority(ctx, backend.workRoot, start.Root, start.Workspace, start.Artifacts)
	if err != nil {
		return nil, ErrUnavailable
	}
	cleanup := func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), windowsBackendTimeout)
		defer cancel()
		_ = authority.Close(closeCtx)
	}
	capability, err := newWindowsCurrentUserCapabilitySID()
	if err != nil {
		cleanup()
		return nil, ErrUnavailable
	}
	owner, err := windowsCurrentUserSID()
	if err != nil {
		cleanup()
		return nil, ErrUnavailable
	}
	// Start owns and has retained the exact session root before this ACL call.
	// The grant is constrained to that newly-created session root and descendants.
	if err := windowsGrantCurrentUserCapabilityRoot(start.Root, owner, capability); err != nil || authority.Verify(ctx) != nil {
		cleanup()
		return nil, ErrUnavailable
	}
	job, err := newWindowsJob(windowsNativeJobAPI{}, windowsJobLimits{MemoryBytes: start.Spec.Limits.MaxMemoryBytes, CPUTime: start.Spec.Limits.MaxCPUTime})
	if err != nil {
		cleanup()
		return nil, ErrUnavailable
	}
	session := &windowsCurrentUserSession{
		backend: backend, start: cloneWindowsBackendStart(start), authority: authority, capability: capability, job: job,
		privateDesktop: backend.privateDesktop, offlineHints: backend.offlineHints,
	}
	backend.mu.Lock()
	if backend.active != nil {
		backend.mu.Unlock()
		closeCtx, cancel := context.WithTimeout(context.Background(), windowsBackendTimeout)
		_ = job.Close(closeCtx)
		cancel()
		cleanup()
		return nil, ErrUnavailable
	}
	backend.active = session
	backend.mu.Unlock()
	return session, nil
}

func validateWindowsCurrentUserStart(workRoot string, start windowsBackendStart) error {
	if workRoot == "" || start.Spec.Validate() != nil || !windowsPathsEqual(start.Root, filepath.Join(workRoot, "sessions", filepath.Base(start.Root))) || !validWindowsSessionName(filepath.Base(start.Root)) || !windowsPathsEqual(start.Workspace, filepath.Join(start.Root, "workspace")) || !windowsPathsEqual(start.Artifacts, filepath.Join(start.Root, "artifacts")) {
		return ErrInvalidSpec
	}
	bindings, err := windowsFixedMountBindings(start.Spec.Mounts, start.Workspace, start.Artifacts)
	if err != nil || !windowsPathsEqual(bindings.workspace, start.Workspace) || !windowsPathsEqual(bindings.artifacts, start.Artifacts) {
		return ErrInvalidSpec
	}
	if _, err := windowsCurrentUserEnvironmentBlock(start.Environment, false); err != nil {
		return ErrInvalidSpec
	}
	return nil
}

func windowsCurrentUserSID() (string, error) {
	var token windows.Token
	if err := windows.OpenProcessToken(windows.CurrentProcess(), windows.TOKEN_QUERY, &token); err != nil {
		return "", ErrUnavailable
	}
	defer token.Close()
	user, err := token.GetTokenUser()
	if err != nil || user == nil || user.User.Sid == nil || user.User.Sid.String() == "" {
		return "", ErrUnavailable
	}
	return user.User.Sid.String(), nil
}

func windowsCurrentUserSupportsJobList(ctx context.Context) error {
	if ctx == nil || ctx.Err() != nil {
		return ErrUnavailable
	}
	job, err := newWindowsJob(windowsNativeJobAPI{}, windowsJobLimits{MemoryBytes: DefaultExecMemoryBytes, CPUTime: DefaultExecWallTime})
	if err != nil {
		return ErrUnavailable
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), windowsBackendTimeout)
		defer cancel()
		_ = job.Close(cleanupCtx)
	}()
	attributes, err := windows.NewProcThreadAttributeList(1)
	if err != nil {
		return ErrUnavailable
	}
	defer attributes.Delete()
	handles := []windows.Handle{job.jobHandle()}
	if handles[0] == 0 || attributes.Update(windowsProcThreadAttributeJobList, unsafe.Pointer(&handles[0]), unsafe.Sizeof(handles[0])) != nil {
		return ErrUnavailable
	}
	return nil
}

type windowsCurrentUserSession struct {
	backend        *windowsCurrentUserBackend
	start          windowsBackendStart
	authority      *windowsSessionAuthority
	capability     string
	job            *windowsJob
	privateDesktop bool
	offlineHints   bool

	mu        sync.Mutex
	runMu     sync.Mutex
	executed  bool
	finished  bool
	finalized bool
	closed    bool
}

var _ windowsBackendSession = (*windowsCurrentUserSession)(nil)

func (session *windowsCurrentUserSession) probe(ctx context.Context) error {
	if session == nil || ctx == nil || ctx.Err() != nil {
		return ErrUnavailable
	}
	session.mu.Lock()
	closed, job := session.closed, session.job
	session.mu.Unlock()
	if closed || job == nil || job.verifyLimits() != nil || session.authority.Verify(ctx) != nil {
		return ErrUnavailable
	}
	return nil
}

func (session *windowsCurrentUserSession) Execute(ctx context.Context, request windowsBackendExec, stdout, stderr io.Writer) (windowsBackendExecResult, error) {
	if session == nil || ctx == nil || ctx.Err() != nil || validateWindowsCurrentUserExecution(session, request) != nil || stdout == nil || stderr == nil {
		return windowsBackendExecResult{}, ErrUnavailable
	}
	session.runMu.Lock()
	defer session.runMu.Unlock()
	session.mu.Lock()
	if session.closed || session.executed || session.finalized {
		session.mu.Unlock()
		return windowsBackendExecResult{}, ErrLeaseTerminated
	}
	session.executed = true
	session.mu.Unlock()
	if session.authority.Verify(ctx) != nil {
		return windowsBackendExecResult{}, ErrUnavailable
	}
	launch, err := session.launch(request)
	if err != nil {
		// CreateProcessAsUser can have succeeded before a parent-side child
		// handle close fails. The process was atomically admitted to this Job,
		// so prove that its complete tree is gone before exposing the launch
		// failure to the provider facade.
		cleanupCtx, cancel := context.WithTimeout(context.Background(), windowsBackendTimeout)
		cleanupErr := session.terminateAndWait(cleanupCtx)
		cancel()
		if cleanupErr != nil {
			return windowsBackendExecResult{}, ErrUnavailable
		}
		return windowsBackendExecResult{}, ErrUnavailable
	}
	defer launch.Close()
	readersDone := launch.copyOutput(stdout, stderr)
	waitErr := windowsCurrentUserWaitProcess(ctx, launch.process)
	timedOut := errors.Is(waitErr, context.DeadlineExceeded)
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), windowsBackendTimeout)
	defer cleanupCancel()
	terminateErr := session.terminateAndWait(cleanupCtx)
	if !windowsCurrentUserDrainOutput(readersDone, launch) {
		return windowsBackendExecResult{}, ErrUnavailable
	}
	if waitErr != nil && !timedOut && !errors.Is(waitErr, context.Canceled) {
		return windowsBackendExecResult{}, ErrUnavailable
	}
	if terminateErr != nil {
		return windowsBackendExecResult{}, ErrUnavailable
	}
	var exitCode uint32
	if err := windows.GetExitCodeProcess(launch.process, &exitCode); err != nil {
		return windowsBackendExecResult{}, ErrUnavailable
	}
	artifacts, err := scanWindowsArtifacts(session.start.Artifacts, request.ArtifactPolicy)
	// The authority proof is a mandatory post-execution cleanup fence. Its
	// bounded cleanup context must not inherit the execution deadline or caller
	// cancellation: either can occur after the Job has been terminated and
	// waited empty but before a result is assembled.
	authorityErr := session.authority.Verify(cleanupCtx)
	if err != nil || authorityErr != nil {
		return windowsBackendExecResult{}, ErrUnavailable
	}
	session.mu.Lock()
	session.finished = true
	session.mu.Unlock()
	result := windowsBackendExecResult{ExitCode: int(exitCode), TimedOut: timedOut, Artifacts: artifacts}
	if timedOut {
		return result, ErrExecTimeout
	}
	if errors.Is(waitErr, context.Canceled) {
		return result, context.Canceled
	}
	if exitCode != 0 {
		return result, ErrExecFailed
	}
	return result, nil
}

func validateWindowsCurrentUserExecution(session *windowsCurrentUserSession, request windowsBackendExec) error {
	if session == nil || ValidateCommand(Command{Args: request.Args}) != nil || request.Limits.Validate() != nil || ValidateLease(request.Lease) != nil || request.ArtifactPolicy.MaxArtifacts <= 0 || request.ArtifactPolicy.MaxTotalBytes <= 0 {
		return ErrInvalidSpec
	}
	if request.Limits != session.start.Spec.Limits || request.ArtifactPolicy != session.start.Spec.ArtifactPolicy || request.Lease != session.start.Spec.Lease {
		return ErrLeaseMismatch
	}
	return nil
}

func (session *windowsCurrentUserSession) launch(request windowsBackendExec) (*windowsCurrentUserLaunch, error) {
	if session == nil || session.job == nil || session.job.verifyLimits() != nil {
		return nil, ErrUnavailable
	}
	application, command, executable, err := windowsCreateProcessArguments(request.Args)
	if err != nil || executable.verify() != nil {
		return nil, ErrUnavailable
	}
	token, err := newWindowsCurrentUserRestrictedToken(session.capability)
	if err != nil {
		return nil, err
	}
	desktop, err := newWindowsCurrentUserDesktop(token.logon, token.capability, session.privateDesktop)
	if err != nil {
		_ = token.Close()
		return nil, err
	}
	defer desktop.Close()
	defer token.Close()
	environment, err := windowsCurrentUserEnvironmentBlock(session.start.Environment, session.offlineHints)
	if err != nil {
		return nil, err
	}
	return newWindowsCurrentUserLaunch(token.token, session.job.jobHandle(), application, command, session.start.Workspace, desktop.name, environment)
}

func (session *windowsCurrentUserSession) terminateAndWait(ctx context.Context) error {
	if session == nil || session.job == nil || ctx == nil {
		return ErrUnavailable
	}
	if err := session.job.Terminate(ctx); err != nil {
		return err
	}
	return session.job.WaitEmpty(ctx)
}

func (session *windowsCurrentUserSession) Terminate(ctx context.Context) error {
	if ctx == nil || ctx.Err() != nil {
		return context.Canceled
	}
	return session.terminateAndWait(ctx)
}

func (session *windowsCurrentUserSession) Finalize(ctx context.Context, outcome PublicationOutcome) error {
	if session == nil || ctx == nil || ctx.Err() != nil || outcome.Validate() != nil {
		return ErrUnavailable
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.closed || !session.finished {
		return ErrUnavailable
	}
	session.finalized = true
	return nil
}

func (session *windowsCurrentUserSession) Abort(ctx context.Context) error {
	if ctx == nil || ctx.Err() != nil {
		return context.Canceled
	}
	return session.terminateAndWait(ctx)
}

func (session *windowsCurrentUserSession) Close(ctx context.Context) error {
	if session == nil || ctx == nil || ctx.Err() != nil {
		return context.Canceled
	}
	session.runMu.Lock()
	defer session.runMu.Unlock()
	session.mu.Lock()
	if session.closed {
		session.mu.Unlock()
		return nil
	}
	job, authority, backend := session.job, session.authority, session.backend
	session.mu.Unlock()
	cleanupCtx, cancel := context.WithTimeout(ctx, windowsBackendTimeout)
	defer cancel()
	var results []error
	if job != nil {
		results = append(results, job.Close(cleanupCtx))
	}
	if authority != nil {
		results = append(results, authority.Close(cleanupCtx))
	}
	if err := errors.Join(results...); err != nil {
		return ErrUnavailable
	}
	session.mu.Lock()
	session.closed = true
	session.mu.Unlock()
	if backend != nil {
		backend.mu.Lock()
		if backend.active == session {
			backend.active = nil
		}
		backend.mu.Unlock()
	}
	return nil
}

func (session *windowsCurrentUserSession) Poison(ctx context.Context) error {
	return session.Close(ctx)
}

type windowsCurrentUserLaunch struct {
	process windows.Handle
	thread  windows.Handle
	stdout  *os.File
	stderr  *os.File
}

func newWindowsCurrentUserLaunch(token windows.Token, job windows.Handle, application, command, directory, desktop string, environment []uint16) (*windowsCurrentUserLaunch, error) {
	if token == 0 || job == 0 || application == "" || command == "" || directory == "" || desktop == "" || len(environment) < 2 {
		return nil, ErrUnavailable
	}
	pipes, err := newWindowsCurrentUserStdioPipes()
	if err != nil {
		return nil, err
	}
	cleanup := true
	defer func() {
		if cleanup {
			pipes.Close()
		}
	}()
	attributes, err := windows.NewProcThreadAttributeList(2)
	if err != nil {
		return nil, ErrUnavailable
	}
	defer attributes.Delete()
	childHandles := []windows.Handle{pipes.childInput, pipes.childStdout, pipes.childStderr}
	if err := attributes.Update(windows.PROC_THREAD_ATTRIBUTE_HANDLE_LIST, unsafe.Pointer(&childHandles[0]), unsafe.Sizeof(childHandles[0])*uintptr(len(childHandles))); err != nil {
		return nil, ErrUnavailable
	}
	jobHandles := []windows.Handle{job}
	if err := attributes.Update(windowsProcThreadAttributeJobList, unsafe.Pointer(&jobHandles[0]), unsafe.Sizeof(jobHandles[0])); err != nil {
		return nil, ErrUnavailable
	}
	app, err := windows.UTF16PtrFromString(application)
	if err != nil {
		return nil, ErrUnavailable
	}
	line, err := windows.UTF16FromString(command)
	if err != nil || len(line) == 0 {
		return nil, ErrUnavailable
	}
	currentDirectory, err := windows.UTF16PtrFromString(directory)
	if err != nil {
		return nil, ErrUnavailable
	}
	desktopName, err := windows.UTF16PtrFromString(desktop)
	if err != nil {
		return nil, ErrUnavailable
	}
	startup := windows.StartupInfoEx{}
	startup.StartupInfo.Cb = uint32(unsafe.Sizeof(startup))
	startup.StartupInfo.Flags = windows.STARTF_USESTDHANDLES
	startup.StartupInfo.StdInput = pipes.childInput
	startup.StartupInfo.StdOutput = pipes.childStdout
	startup.StartupInfo.StdErr = pipes.childStderr
	startup.StartupInfo.Desktop = desktopName
	startup.ProcThreadAttributeList = attributes.List()
	var process windows.ProcessInformation
	if err := windows.CreateProcessAsUser(token, app, &line[0], nil, nil, true, windows.CREATE_UNICODE_ENVIRONMENT|windows.EXTENDED_STARTUPINFO_PRESENT, &environment[0], currentDirectory, &startup.StartupInfo, &process); err != nil {
		return nil, ErrUnavailable
	}
	// Atomic JOB_LIST admission succeeded when CreateProcessAsUser returned.
	// Close every child-side parent handle before returning the parent readers.
	if err := windowsCurrentUserCloseChildPipes(pipes); err != nil {
		_ = windows.TerminateProcess(process.Process, 1)
		_ = windows.CloseHandle(process.Thread)
		_ = windows.CloseHandle(process.Process)
		return nil, ErrUnavailable
	}
	result := &windowsCurrentUserLaunch{process: process.Process, thread: process.Thread, stdout: pipes.parentStdout, stderr: pipes.parentStderr}
	pipes.parentStdout, pipes.parentStderr = nil, nil
	cleanup = false
	return result, nil
}

func (launch *windowsCurrentUserLaunch) Close() error {
	if launch == nil {
		return nil
	}
	var results []error
	if launch.stdout != nil {
		results = append(results, launch.stdout.Close())
		launch.stdout = nil
	}
	if launch.stderr != nil {
		results = append(results, launch.stderr.Close())
		launch.stderr = nil
	}
	if launch.thread != 0 {
		results = append(results, windows.CloseHandle(launch.thread))
		launch.thread = 0
	}
	if launch.process != 0 {
		results = append(results, windows.CloseHandle(launch.process))
		launch.process = 0
	}
	return errors.Join(results...)
}

func (launch *windowsCurrentUserLaunch) copyOutput(stdout, stderr io.Writer) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		var group sync.WaitGroup
		for _, copy := range []struct {
			reader *os.File
			writer io.Writer
		}{{launch.stdout, stdout}, {launch.stderr, stderr}} {
			if copy.reader == nil || copy.writer == nil {
				continue
			}
			group.Add(1)
			go func(reader *os.File, writer io.Writer) {
				defer group.Done()
				_, _ = io.Copy(writer, reader)
			}(copy.reader, copy.writer)
		}
		group.Wait()
		close(done)
	}()
	return done
}

func windowsCurrentUserDrainOutput(done <-chan struct{}, launch *windowsCurrentUserLaunch) bool {
	if done == nil {
		return false
	}
	timer := time.NewTimer(windowsBackendTimeout)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		if launch != nil {
			if launch.stdout != nil {
				_ = launch.stdout.Close()
			}
			if launch.stderr != nil {
				_ = launch.stderr.Close()
			}
		}
		select {
		case <-done:
			return true
		case <-time.After(100 * time.Millisecond):
			return false
		}
	}
}

func windowsCurrentUserWaitProcess(ctx context.Context, process windows.Handle) error {
	if ctx == nil || process == 0 {
		return ErrUnavailable
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		status, err := windows.WaitForSingleObject(process, 25)
		if err != nil {
			return err
		}
		switch status {
		case windows.WAIT_OBJECT_0:
			return nil
		case 258: // WAIT_TIMEOUT
			continue
		default:
			return ErrUnavailable
		}
	}
}

type windowsCurrentUserStdioPipes struct {
	childInput   windows.Handle
	childStdout  windows.Handle
	childStderr  windows.Handle
	parentStdout *os.File
	parentStderr *os.File
}

var windowsCurrentUserCloseChildPipes = func(pipes *windowsCurrentUserStdioPipes) error {
	return pipes.closeChild()
}

func newWindowsCurrentUserStdioPipes() (*windowsCurrentUserStdioPipes, error) {
	attributes := windows.SecurityAttributes{Length: uint32(unsafe.Sizeof(windows.SecurityAttributes{})), InheritHandle: 1}
	var inputRead, inputWrite windows.Handle
	var outputRead, outputWrite windows.Handle
	var errorRead, errorWrite windows.Handle
	if err := windows.CreatePipe(&inputRead, &inputWrite, &attributes, 0); err != nil {
		return nil, err
	}
	cleanup := func() {
		for _, handle := range []windows.Handle{inputRead, inputWrite, outputRead, outputWrite, errorRead, errorWrite} {
			if handle != 0 {
				_ = windows.CloseHandle(handle)
			}
		}
	}
	if err := windows.CreatePipe(&outputRead, &outputWrite, &attributes, 0); err != nil {
		cleanup()
		return nil, err
	}
	if err := windows.CreatePipe(&errorRead, &errorWrite, &attributes, 0); err != nil {
		cleanup()
		return nil, err
	}
	for _, handle := range []windows.Handle{outputRead, errorRead} {
		if err := windows.SetHandleInformation(handle, windows.HANDLE_FLAG_INHERIT, 0); err != nil {
			cleanup()
			return nil, err
		}
	}
	// stdin is intentionally an inherited anonymous pipe whose parent writer
	// is closed before launch, so child reads see EOF and no host input leaks.
	if err := windows.CloseHandle(inputWrite); err != nil {
		cleanup()
		return nil, err
	}
	inputWrite = 0
	stdout := os.NewFile(uintptr(outputRead), "harness-current-user-stdout")
	stderr := os.NewFile(uintptr(errorRead), "harness-current-user-stderr")
	if stdout == nil || stderr == nil {
		if stdout != nil {
			_ = stdout.Close()
		}
		if stderr != nil {
			_ = stderr.Close()
		}
		outputRead, errorRead = 0, 0
		cleanup()
		return nil, ErrUnavailable
	}
	return &windowsCurrentUserStdioPipes{childInput: inputRead, childStdout: outputWrite, childStderr: errorWrite, parentStdout: stdout, parentStderr: stderr}, nil
}

func (pipes *windowsCurrentUserStdioPipes) closeChild() error {
	if pipes == nil {
		return ErrUnavailable
	}
	var results []error
	for _, handle := range []*windows.Handle{&pipes.childInput, &pipes.childStdout, &pipes.childStderr} {
		if *handle == 0 {
			continue
		}
		if err := windows.CloseHandle(*handle); err != nil {
			results = append(results, err)
			continue
		}
		*handle = 0
	}
	return errors.Join(results...)
}

func (pipes *windowsCurrentUserStdioPipes) Close() error {
	if pipes == nil {
		return nil
	}
	var results []error
	results = append(results, pipes.closeChild())
	if pipes.parentStdout != nil {
		results = append(results, pipes.parentStdout.Close())
		pipes.parentStdout = nil
	}
	if pipes.parentStderr != nil {
		results = append(results, pipes.parentStderr.Close())
		pipes.parentStderr = nil
	}
	return errors.Join(results...)
}
