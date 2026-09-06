//go:build linux

package sandbox

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// LocalProvider is a real Linux bwrap+prlimit provider. It never falls back
// to host os/exec and reports unavailable when either dependency is absent.
type LocalProvider struct{}

func (LocalProvider) ID() ProviderID { return "local-ephemeral" }

const (
	localProbeTimeout          = 2 * time.Second
	localProbeUnavailableCause = "sandbox provider unavailable"
)

// localProbeResolver and localProbeRunner are deliberately package-private so
// the live capability check can be tested without changing the Provider
// contract or running a host command in unit tests.
type localProbeResolver interface {
	LookPath(file string) (string, error)
}

type localProbeRunner interface {
	Run(ctx context.Context, name string, args ...string) error
}

type execLocalProbeResolver struct{}

func (execLocalProbeResolver) LookPath(file string) (string, error) {
	return exec.LookPath(file)
}

type execLocalProbeRunner struct{}

func (execLocalProbeRunner) Run(ctx context.Context, name string, args ...string) error {
	command := exec.CommandContext(ctx, name, args...)
	command.Stdout = io.Discard
	command.Stderr = io.Discard
	return command.Run()
}

func (LocalProvider) Probe(ctx context.Context) AssuranceReport {
	return probeLocal(ctx, execLocalProbeResolver{}, execLocalProbeRunner{})
}

func probeLocal(ctx context.Context, resolver localProbeResolver, runner localProbeRunner) AssuranceReport {
	if ctx == nil || ctx.Err() != nil {
		return unavailableLocalProbeReport()
	}
	bwrap, err := resolver.LookPath("bwrap")
	if err != nil {
		return unavailableLocalProbeReport()
	}
	prlimit, err := resolver.LookPath("prlimit")
	if err != nil {
		return unavailableLocalProbeReport()
	}
	probeCtx, cancel := context.WithTimeout(ctx, localProbeTimeout)
	defer cancel()
	if err := runner.Run(probeCtx, prlimit, localProbeArgv(bwrap)...); err != nil || probeCtx.Err() != nil {
		return unavailableLocalProbeReport()
	}
	return AssuranceReport{
		Available:         true,
		Actual:            Assurance{Level: AssuranceProcess, SharedKernel: true, NetworkIsolation: false},
		Network:           NetworkHost,
		SupportedNetworks: []NetworkPolicy{NetworkDisabled, NetworkHost},
		LimitsEnforced:    true,
		MountsEnforced:    true,
	}
}

func unavailableLocalProbeReport() AssuranceReport {
	return AssuranceReport{UnavailableCause: localProbeUnavailableCause}
}

func localProbeArgv(bwrap string) []string {
	argv := []string{
		"--as=67108864", "--cpu=1", "--nofile=32", "--nproc=256", "--", bwrap,
		"--die-with-parent", "--new-session", "--unshare-user", "--unshare-pid", "--unshare-ipc", "--unshare-uts", "--unshare-cgroup", "--unshare-net",
		"--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp", "--chdir", "/tmp", "--clearenv", "--setenv", "PATH", "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
	}
	for _, runtimePath := range []string{"/usr", "/bin", "/sbin", "/lib", "/lib64", "/etc"} {
		if _, err := os.Stat(runtimePath); err == nil {
			argv = append(argv, "--ro-bind", runtimePath, runtimePath)
		}
	}
	return append(argv, "--", "/bin/true")
}

func (provider LocalProvider) Start(ctx context.Context, spec SessionSpec) (Session, error) {
	spec = cloneSessionSpec(spec)
	if ctx == nil {
		return nil, context.Canceled
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	probe := provider.Probe(ctx)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := probe.SatisfiesSpec(spec); err != nil {
		return nil, err
	}
	artifactSource, err := validateLocalMounts(spec.Mounts)
	if err != nil {
		return nil, err
	}
	report := AssuranceReport{
		Available:      true,
		Actual:         Assurance{Level: AssuranceProcess, SharedKernel: true, NetworkIsolation: spec.Network != NetworkHost},
		Network:        spec.Network,
		LimitsEnforced: true,
		MountsEnforced: true,
	}
	return &localSession{spec: spec, report: report, artifactSource: artifactSource}, nil
}

type localSession struct {
	spec           SessionSpec
	report         AssuranceReport
	artifactSource string
	runMu          sync.Mutex
	lifecycle      processLifecycle
}

func (session *localSession) Assurance() AssuranceReport {
	if session == nil {
		return AssuranceReport{}
	}
	if session.lifecycle.isClosed() {
		return AssuranceReport{}
	}
	return cloneAssuranceReport(session.report)
}

func (session *localSession) Limits() Limits {
	if session == nil {
		return Limits{}
	}
	if session.lifecycle.isClosed() {
		return Limits{}
	}
	return session.spec.Limits
}

func (session *localSession) Run(ctx context.Context, command Command) (ArtifactSet, error) {
	if ctx == nil {
		return ArtifactSet{}, context.Canceled
	}
	if err := ctx.Err(); err != nil {
		return ArtifactSet{}, err
	}
	if err := ValidateCommand(command); err != nil {
		return ArtifactSet{}, err
	}
	if session == nil {
		return ArtifactSet{}, ErrProviderFailure
	}
	session.runMu.Lock()
	defer session.runMu.Unlock()
	argv, err := buildLocalArgv(command, session.spec)
	if err != nil {
		return ArtifactSet{}, err
	}
	runCtx, cancel := localRunContext(ctx, session.spec.Limits.WallTime)
	defer cancel()
	cmd := exec.CommandContext(runCtx, argv[0], argv[1:]...)
	var cancellationObserved atomic.Bool
	cmd.Cancel = func() error {
		cancellationObserved.Store(true)
		killProcessGroup(cmd)
		return nil
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	sink := &boundedDiscard{limit: session.spec.Limits.MaxOutputBytes}
	sink.kill = func() { killProcessGroup(cmd) }
	cmd.Stdout, cmd.Stderr = sink, sink
	run, err := session.lifecycle.begin(cancel)
	if err != nil {
		return ArtifactSet{}, err
	}
	if err := runCtx.Err(); err != nil {
		session.lifecycle.finish(run)
		return ArtifactSet{}, err
	}
	if err := cmd.Start(); err != nil {
		closed := session.lifecycle.finish(run)
		if closed {
			return ArtifactSet{}, ErrLeaseTerminated
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ArtifactSet{}, ctxErr
		}
		if runCtx.Err() == context.DeadlineExceeded {
			return ArtifactSet{}, ErrWallTimeLimit
		}
		return ArtifactSet{}, ErrProviderFailure
	}
	if cancellationObserved.Load() || runCtx.Err() != nil {
		killProcessGroup(cmd)
		err = cmd.Wait()
		closed := session.lifecycle.finish(run)
		if closed {
			return ArtifactSet{}, ErrLeaseTerminated
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ArtifactSet{}, ctxErr
		}
		if runCtx.Err() == context.DeadlineExceeded {
			return ArtifactSet{}, ErrWallTimeLimit
		}
		if err != nil {
			return ArtifactSet{}, ErrProviderFailure
		}
		return ArtifactSet{}, ErrProviderFailure
	}
	if !session.lifecycle.publish(run, cmd) {
		killProcessGroup(cmd)
		_ = cmd.Wait()
		session.lifecycle.finish(run)
		return ArtifactSet{}, ErrLeaseTerminated
	}
	err = cmd.Wait()
	closed := session.lifecycle.finish(run)
	if closed {
		return ArtifactSet{}, ErrLeaseTerminated
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ArtifactSet{}, ctxErr
	}
	if runCtx.Err() == context.DeadlineExceeded {
		return ArtifactSet{}, ErrWallTimeLimit
	}
	if err != nil {
		if sink.exceededLimit() {
			return ArtifactSet{}, ErrOutputLimit
		}
		return ArtifactSet{}, ErrProviderFailure
	}
	if sink.exceededLimit() {
		return ArtifactSet{}, ErrOutputLimit
	}
	artifacts, err := scanArtifacts(session.artifactSource, session.spec.ArtifactPolicy)
	if err != nil {
		return ArtifactSet{}, err
	}
	if session.lifecycle.isClosed() {
		return ArtifactSet{}, ErrLeaseTerminated
	}
	return artifacts, nil
}

// Exec is the detailed execution path. It shares the same process lifecycle
// as Run, while retaining only bounded previews and streaming digests.
func (session *localSession) Exec(ctx context.Context, request ExecRequest) (ExecResult, error) {
	if ctx == nil {
		return ExecResult{}, context.Canceled
	}
	if err := ctx.Err(); err != nil {
		return ExecResult{}, err
	}
	if session == nil {
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
	argv, err := buildLocalArgv(Command{Args: request.Args}, session.spec)
	if err != nil {
		return ExecResult{}, err
	}
	runCtx, cancel := localRunContext(ctx, session.spec.Limits.WallTime)
	defer cancel()
	cmd := exec.CommandContext(runCtx, argv[0], argv[1:]...)
	var cancellationObserved atomic.Bool
	cmd.Cancel = func() error {
		cancellationObserved.Store(true)
		killProcessGroup(cmd)
		return nil
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	combined := &combinedCapture{limit: request.Limits.MaxOutputBytes}
	stdout := newBoundedCapture(combined, request.Limits.MaxOutputBytes)
	stderr := newBoundedCapture(combined, request.Limits.MaxOutputBytes)
	cmd.Stdout, cmd.Stderr = stdout, stderr
	combined.kill = func() { killProcessGroup(cmd) }
	run, err := session.lifecycle.begin(cancel)
	if err != nil {
		return ExecResult{}, err
	}
	finish := func() { session.lifecycle.finish(run) }
	if err := runCtx.Err(); err != nil {
		finish()
		return ExecResult{}, err
	}
	if err := cmd.Start(); err != nil {
		finish()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ExecResult{}, ctxErr
		}
		return ExecResult{}, ErrProviderFailure
	}
	if cancellationObserved.Load() || runCtx.Err() != nil {
		killProcessGroup(cmd)
		_ = cmd.Wait()
		finish()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ExecResult{}, ctxErr
		}
		if runCtx.Err() == context.DeadlineExceeded {
			return ExecResult{}, ErrExecTimeout
		}
		return ExecResult{}, ErrProviderFailure
	}
	if !session.lifecycle.publish(run, cmd) {
		killProcessGroup(cmd)
		_ = cmd.Wait()
		finish()
		return ExecResult{}, ErrLeaseTerminated
	}
	waitErr := cmd.Wait()
	closed := session.lifecycle.finish(run)
	result := ExecResult{Stdout: stdout.result(), Stderr: stderr.result(), ExitCode: 0, Artifacts: ArtifactSet{Items: []Artifact{}}}
	if waitErr != nil {
		if exitErr, ok := waitErr.(*exec.ExitError); ok && exitErr.ProcessState != nil {
			result.ExitCode = exitErr.ProcessState.ExitCode()
			if result.ExitCode < 0 {
				result.ExitCode = 1
			}
		}
	}
	if closed {
		return result, ErrLeaseTerminated
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return result, ctxErr
	}
	if runCtx.Err() == context.DeadlineExceeded {
		result.TimedOut = true
		return result, ErrExecTimeout
	}
	if waitErr != nil {
		if errors.Is(waitErr, ErrExecOutputLimit) || stdout.exceededLimit() || stderr.exceededLimit() {
			return result, ErrExecOutputLimit
		}
		return result, ErrExecFailed
	}
	if stdout.exceededLimit() || stderr.exceededLimit() {
		return result, ErrExecOutputLimit
	}
	artifacts, err := scanArtifacts(session.artifactSource, session.spec.ArtifactPolicy)
	if err != nil {
		return result, err
	}
	result.Artifacts = artifacts
	return result, nil
}

func (session *localSession) Close(ctx context.Context) error {
	if ctx == nil {
		return context.Canceled
	}
	if session == nil {
		return ErrLeaseTerminated
	}
	return session.lifecycle.close(ctx, func(process any) {
		if command, ok := process.(*exec.Cmd); ok {
			killProcessGroup(command)
		}
	})
}

func buildLocalArgv(command Command, spec SessionSpec) ([]string, error) {
	bwrap, err := exec.LookPath("bwrap")
	if err != nil {
		return nil, ErrUnavailable
	}
	prlimit, err := exec.LookPath("prlimit")
	if err != nil {
		return nil, ErrUnavailable
	}
	argv := []string{prlimit, "--as=" + formatInt(spec.Limits.MaxMemoryBytes), "--cpu=" + formatCPUSeconds(spec.Limits.MaxCPUTime), "--nofile=256", "--nproc=256", "--", bwrap,
		"--die-with-parent", "--new-session", "--unshare-user", "--unshare-pid", "--unshare-ipc", "--unshare-uts", "--unshare-cgroup",
		"--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp", "--chdir", "/tmp", "--clearenv", "--setenv", "PATH", "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"}
	if spec.Network == NetworkDisabled {
		argv = append(argv, "--unshare-net")
	}
	for _, runtimePath := range []string{"/usr", "/bin", "/sbin", "/lib", "/lib64", "/etc"} {
		if _, err := os.Stat(runtimePath); err == nil {
			argv = append(argv, "--ro-bind", runtimePath, runtimePath)
		}
	}
	for _, mount := range spec.Mounts {
		mode := "--ro-bind"
		if !mount.ReadOnly {
			mode = "--bind"
		}
		source, err := NormalizeHostPath(mount.Source)
		if err != nil {
			return nil, ErrInvalidSpec
		}
		argv = append(argv, mode, source, mount.Target)
	}
	argv = append(argv, "--", command.Args[0])
	argv = append(argv, command.Args[1:]...)
	return argv, nil
}

func validateLocalMounts(mounts []Mount) (string, error) {
	var source string
	for _, mount := range mounts {
		target, err := NormalizeVirtualPath(mount.Target)
		if err != nil || target != mount.Target || !allowedLocalMountTarget(target) {
			return "", ErrInvalidSpec
		}
		if mount.Target != "/artifacts" {
			continue
		}
		if mount.ReadOnly || source != "" {
			return "", ErrInvalidSpec
		}
		clean, err := NormalizeHostPath(mount.Source)
		if err != nil {
			return "", ErrInvalidSpec
		}
		info, err := os.Lstat(clean)
		if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return "", ErrInvalidSpec
		}
		source = clean
	}
	if source == "" {
		return "", ErrInvalidSpec
	}
	return source, nil
}

func allowedLocalMountTarget(target string) bool {
	return target == "/artifacts" || target == "/workspace" || strings.HasPrefix(target, "/workspace/")
}

func scanArtifacts(root string, policy ArtifactPolicy) (ArtifactSet, error) {
	fd, err := syscall.Open(root, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		return ArtifactSet{}, ErrArtifactUnverified
	}
	directory := os.NewFile(uintptr(fd), "sandbox-artifacts")
	defer directory.Close()
	result := ArtifactSet{Items: make([]Artifact, 0, policy.MaxArtifacts)}
	var total int64
	if err := walkArtifactDir(directory, "", policy, &result, &total); err != nil {
		return ArtifactSet{}, ErrArtifactUnverified
	}
	sort.Slice(result.Items, func(i, j int) bool { return result.Items[i].Key < result.Items[j].Key })
	return result, nil
}

func walkArtifactDir(directory *os.File, prefix string, policy ArtifactPolicy, result *ArtifactSet, total *int64) error {
	for {
		entries, err := directory.ReadDir(64)
		if len(entries) == 0 && err == io.EOF {
			return nil
		}
		if err != nil && err != io.EOF {
			return ErrArtifactUnverified
		}
		for _, entry := range entries {
			name := entry.Name()
			if name == "." || name == ".." || strings.Contains(name, "/") || entry.Type()&os.ModeSymlink != 0 {
				return ErrArtifactUnverified
			}
			childPrefix := name
			if prefix != "" {
				childPrefix = prefix + "/" + name
			}
			if entry.IsDir() {
				childFD, openErr := syscall.Openat(int(directory.Fd()), name, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
				if openErr != nil {
					return ErrArtifactUnverified
				}
				child := os.NewFile(uintptr(childFD), "sandbox-artifact-dir")
				childErr := walkArtifactDir(child, childPrefix, policy, result, total)
				closeErr := child.Close()
				if childErr != nil || closeErr != nil {
					return ErrArtifactUnverified
				}
				continue
			}
			if !entry.Type().IsRegular() || !validArtifactKey(childPrefix) || len(result.Items) >= policy.MaxArtifacts {
				return ErrArtifactUnverified
			}
			if err := readArtifactAt(directory, name, childPrefix, policy, result, total); err != nil {
				return err
			}
		}
		if err == io.EOF {
			return nil
		}
	}
}

func readArtifactAt(directory *os.File, name, key string, policy ArtifactPolicy, result *ArtifactSet, total *int64) error {
	fd, err := syscall.Openat(int(directory.Fd()), name, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		return ErrArtifactUnverified
	}
	file := os.NewFile(uintptr(fd), "sandbox-artifact")
	defer file.Close()
	before, err := file.Stat()
	if err != nil || !before.Mode().IsRegular() || !hasSingleArtifactLink(before) || before.Size() < 0 || before.Size() > MaxArtifactBytes || *total > policy.MaxTotalBytes-before.Size() {
		return ErrArtifactUnverified
	}
	localArtifactScanBeforeReadHook()
	hash := sha256.New()
	copied, copyErr := io.CopyN(hash, file, before.Size()+1)
	after, statErr := file.Stat()
	if statErr != nil || !after.Mode().IsRegular() || !hasSingleArtifactLink(after) || copied != before.Size() || (copyErr != nil && copyErr != io.EOF) || !sameArtifactFile(before, after) {
		return ErrArtifactUnverified
	}
	result.Items = append(result.Items, Artifact{Key: key, Digest: hex.EncodeToString(hash.Sum(nil)), Size: before.Size(), Verified: true})
	*total += before.Size()
	return nil
}

func hasSingleArtifactLink(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Nlink == 1
}

// localArtifactScanBeforeReadHook is a package-private test synchronization
// seam. Production retains its no-op default and exposes no hook publicly.
var localArtifactScanBeforeReadHook = func() {}

func sameArtifactFile(before, after os.FileInfo) bool {
	if before.Size() != after.Size() || before.Mode() != after.Mode() {
		return false
	}
	beforeStat, beforeOK := before.Sys().(*syscall.Stat_t)
	afterStat, afterOK := after.Sys().(*syscall.Stat_t)
	return beforeOK && afterOK && beforeStat.Dev == afterStat.Dev && beforeStat.Ino == afterStat.Ino &&
		beforeStat.Mtim == afterStat.Mtim && beforeStat.Ctim == afterStat.Ctim
}

type boundedDiscard struct {
	mu       sync.Mutex
	limit    int64
	total    int64
	exceeded bool
	kill     func()
}

func (sink *boundedDiscard) Write(value []byte) (int, error) {
	sink.mu.Lock()
	if sink.exceeded || sink.total > sink.limit-int64(len(value)) {
		sink.exceeded = true
		kill := sink.kill
		sink.mu.Unlock()
		if kill != nil {
			kill()
		}
		return 0, ErrOutputLimit
	}
	sink.total += int64(len(value))
	sink.mu.Unlock()
	return len(value), nil
}

func (sink *boundedDiscard) exceededLimit() bool {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	return sink.exceeded
}

func killProcessGroup(command *exec.Cmd) {
	if command == nil || command.Process == nil {
		return
	}
	_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
}

func formatInt(value int64) string { return strconv.FormatInt(value, 10) }

func formatCPUSeconds(value time.Duration) string {
	seconds := int64(value / time.Second)
	if value%time.Second != 0 {
		seconds++
	}
	if seconds < 1 {
		seconds = 1
	}
	return strconv.FormatInt(seconds, 10)
}
