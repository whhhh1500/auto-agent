//go:build linux

package sandbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	wslIntegrationEnv = "HARNESS_WSL_INTEGRATION"
	wslExt4RootEnv    = "HARNESS_WSL_EXT4_ROOT"
	wslWindowsRootEnv = "HARNESS_WSL_WINDOWS_ROOT"
	wslPerformanceEnv = "HARNESS_WSL_PERFORMANCE"
	wslEvidenceEnv    = "HARNESS_WSL_EVIDENCE_PATH"
)

type wslRoots struct {
	ext4    string
	windows string
}

type wslSession struct {
	session   Session
	exec      ExecSession
	spec      SessionSpec
	baseRoot  string
	root      string
	workspace string
	artifacts string

	cleanupOnce sync.Once
	cleanupErr  error
}

func requireWSLIntegration(t *testing.T) wslRoots {
	t.Helper()
	if os.Getenv(wslIntegrationEnv) != "1" {
		t.Skip("WSL integration suite is run only by scripts/test-wsl-sandbox.sh")
	}
	if os.Geteuid() == 0 {
		t.Fatal("WSL integration must run as a non-root user")
	}
	roots := wslRoots{ext4: os.Getenv(wslExt4RootEnv), windows: os.Getenv(wslWindowsRootEnv)}
	for label, root := range map[string]string{"ext4": roots.ext4, "windows": roots.windows} {
		info, err := os.Stat(root)
		if err != nil || !info.IsDir() {
			t.Fatalf("%s root unavailable: %v", label, err)
		}
	}
	if report := (LocalProvider{}).Probe(context.Background()); !report.Available {
		t.Fatalf("live local provider probe unavailable: %#v; %s", report, localProbeTestDiagnostic())
	}
	return roots
}

func localProbeTestDiagnostic() string {
	bwrap, bwrapErr := exec.LookPath("bwrap")
	prlimit, prlimitErr := exec.LookPath("prlimit")
	if bwrapErr != nil || prlimitErr != nil {
		return fmt.Sprintf("lookup bwrap=%v prlimit=%v", bwrapErr, prlimitErr)
	}
	ctx, cancel := context.WithTimeout(context.Background(), localProbeTimeout)
	defer cancel()
	command := exec.CommandContext(ctx, prlimit, localProbeArgv(bwrap)...)
	output, err := command.CombinedOutput()
	if len(output) > 512 {
		output = output[:512]
	}
	return fmt.Sprintf("fixed probe err=%v output=%q", err, output)
}

func TestWSLLiveProbe(t *testing.T) {
	_ = requireWSLIntegration(t)
}

func defaultWSLLimits() Limits {
	return Limits{WallTime: 2 * time.Second, MaxCPUTime: 2 * time.Second, MaxMemoryBytes: 256 << 20, MaxOutputBytes: 16 << 10}
}

func startWSLSession(root, label string, limits Limits) (*wslSession, error) {
	directory, err := os.MkdirTemp(root, "sandbox-wsl-"+label+"-")
	if err != nil {
		return nil, err
	}
	cleanup := func(cause error) (*wslSession, error) {
		if cleanupErr := removeExactWSLRoot(root, directory); cleanupErr != nil {
			return nil, fmt.Errorf("%w; session root cleanup: %v", cause, cleanupErr)
		}
		return nil, cause
	}
	workspace := filepath.Join(directory, "workspace")
	artifacts := filepath.Join(directory, "artifacts")
	if err := os.Mkdir(workspace, 0o700); err != nil {
		return cleanup(err)
	}
	if err := os.Mkdir(artifacts, 0o700); err != nil {
		return cleanup(err)
	}
	policy := ArtifactPolicy{MaxArtifacts: 8, MaxTotalBytes: 1 << 20}
	spec := SessionSpec{
		RequestedAssurance: Assurance{Level: AssuranceProcess, SharedKernel: true, NetworkIsolation: true},
		Limits:             limits,
		Network:            NetworkDisabled,
		Mounts: []Mount{
			{Source: workspace, Target: "/workspace", ReadOnly: true},
			{Source: artifacts, Target: "/artifacts"},
		},
		ArtifactPolicy: policy,
		Lease: LeaseIdentity{
			RunID: "wsl-run-" + label, SegmentID: "wsl-segment-" + label,
			ModuleID: "wsl-module", CompositionRev: "wsl-revision", Token: "wsl-token-" + label,
		},
	}
	session, err := (LocalProvider{}).Start(context.Background(), spec)
	if err != nil {
		return cleanup(err)
	}
	execSession, ok := session.(ExecSession)
	if !ok {
		closeErr := session.Close(context.Background())
		_, cleanupErr := cleanup(ErrUnavailable)
		return nil, errors.Join(closeErr, cleanupErr)
	}
	return &wslSession{session: session, exec: execSession, spec: spec, baseRoot: root, root: directory, workspace: workspace, artifacts: artifacts}, nil
}

func (session *wslSession) closeAndRemove() error {
	session.cleanupOnce.Do(func() {
		closeErr := session.session.Close(context.Background())
		removeErr := removeExactWSLRoot(session.baseRoot, session.root)
		session.cleanupErr = errors.Join(closeErr, removeErr)
	})
	return session.cleanupErr
}

// removeExactWSLRoot refuses to recurse outside the unique directory created
// for one session.  A replacement symlink is rejected rather than followed.
func removeExactWSLRoot(baseRoot, sessionRoot string) error {
	cleanBase, err := filepath.Abs(baseRoot)
	if err != nil {
		return err
	}
	cleanRoot, err := filepath.Abs(sessionRoot)
	if err != nil {
		return err
	}
	if filepath.Dir(cleanRoot) != cleanBase || !strings.HasPrefix(filepath.Base(cleanRoot), "sandbox-wsl-") {
		return fmt.Errorf("refusing to remove unexpected session root %q", cleanRoot)
	}
	info, err := os.Lstat(cleanRoot)
	if err != nil {
		return fmt.Errorf("lstat session root: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("session root is not an owned directory: %q", cleanRoot)
	}
	if err := os.RemoveAll(cleanRoot); err != nil {
		return fmt.Errorf("remove session root: %w", err)
	}
	if _, err := os.Lstat(cleanRoot); !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("session root still exists after removal: %w", err)
	}
	return nil
}

func newWSLSession(t *testing.T, root, label string, limits Limits) *wslSession {
	t.Helper()
	session, err := startWSLSession(root, label, limits)
	if err != nil {
		t.Fatalf("start WSL session: %v", err)
	}
	t.Cleanup(func() {
		if err := session.closeAndRemove(); err != nil {
			t.Errorf("close and remove WSL session: %v", err)
		}
	})
	return session
}

func (session *wslSession) execCommand(ctx context.Context, args ...string) (ExecResult, error) {
	return session.exec.Exec(ctx, ExecRequest{Args: args, Limits: session.spec.Limits, Network: session.spec.Network, ArtifactPolicy: session.spec.ArtifactPolicy, Lease: session.spec.Lease})
}

func TestWSLLocalProviderIsolationAndLimits(t *testing.T) {
	roots := requireWSLIntegration(t)
	session := newWSLSession(t, roots.ext4, "isolation", defaultWSLLimits())

	t.Run("namespaces and loopback", func(t *testing.T) {
		result, err := session.execCommand(context.Background(), "/bin/sh", "-c", "for n in user pid ipc uts cgroup net; do readlink /proc/self/ns/$n; done; cat /proc/net/dev")
		if err != nil {
			t.Fatalf("namespace command: %v", err)
		}
		lines := strings.Split(strings.TrimSpace(string(result.Stdout.Preview)), "\n")
		if len(lines) < 6 {
			t.Fatalf("namespace output too short: %q", result.Stdout.Preview)
		}
		for index, namespace := range []string{"user", "pid", "ipc", "uts", "cgroup", "net"} {
			host, readErr := os.Readlink("/proc/self/ns/" + namespace)
			if readErr != nil || lines[index] == host {
				t.Fatalf("%s namespace child=%q host=%q err=%v", namespace, lines[index], host, readErr)
			}
		}
		output := string(result.Stdout.Preview)
		if !strings.Contains(output, "lo:") || strings.Contains(output, "eth0:") || strings.Contains(output, "docker0:") {
			t.Fatalf("network namespace did not expose loopback only: %q", output)
		}
	})

	t.Run("unmounted and readonly paths", func(t *testing.T) {
		secret := filepath.Join(session.root, "unmounted-secret")
		if err := os.WriteFile(secret, []byte("secret"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := session.execCommand(context.Background(), "/usr/bin/test", "!", "-e", secret); err != nil {
			t.Fatalf("unmounted secret was visible: %v", err)
		}
		if _, err := session.execCommand(context.Background(), "/bin/sh", "-c", "! touch /workspace/blocked && ! touch /etc/blocked"); err != nil {
			t.Fatalf("readonly path was writable: %v", err)
		}
	})

	t.Run("argv is literal", func(t *testing.T) {
		literal := "$(touch /workspace/argv-injection)"
		result, err := session.execCommand(context.Background(), "/usr/bin/printf", "%s", literal)
		if err != nil || string(result.Stdout.Preview) != literal {
			t.Fatalf("argv result=%q err=%v", result.Stdout.Preview, err)
		}
		if _, err := os.Stat(filepath.Join(session.workspace, "argv-injection")); !os.IsNotExist(err) {
			t.Fatalf("literal argv was interpreted: %v", err)
		}
	})

	t.Run("rlimits", func(t *testing.T) {
		result, err := session.execCommand(context.Background(), "/bin/bash", "-c", "printf '%s %s %s %s' \"$(ulimit -v)\" \"$(ulimit -t)\" \"$(ulimit -n)\" \"$(ulimit -u)\"")
		if err != nil {
			t.Fatalf("read rlimits: %v", err)
		}
		values := strings.Fields(string(result.Stdout.Preview))
		want := []string{strconv.FormatInt(session.spec.Limits.MaxMemoryBytes/1024, 10), formatCPUSeconds(session.spec.Limits.MaxCPUTime), "256", "256"}
		if !equalStrings(values, want) {
			t.Fatalf("rlimits=%q want=%q", values, want)
		}
	})

	t.Run("combined output", func(t *testing.T) {
		limited := newWSLSession(t, roots.ext4, "output", Limits{WallTime: time.Second, MaxCPUTime: time.Second, MaxMemoryBytes: 256 << 20, MaxOutputBytes: 1024})
		_, err := limited.execCommand(context.Background(), "/bin/sh", "-c", "head -c 700 /dev/zero; head -c 700 /dev/zero >&2")
		if !errors.Is(err, ErrExecOutputLimit) {
			t.Fatalf("combined output error=%v", err)
		}
	})

	t.Run("wall timeout", func(t *testing.T) {
		limited := newWSLSession(t, roots.ext4, "wall", Limits{WallTime: 50 * time.Millisecond, MaxCPUTime: time.Second, MaxMemoryBytes: 256 << 20, MaxOutputBytes: 1024})
		result, err := limited.execCommand(context.Background(), "/bin/sleep", "5")
		if !errors.Is(err, ErrExecTimeout) || !result.TimedOut {
			t.Fatalf("wall timeout result=%#v err=%v", result, err)
		}
	})

	assertWSLBlockedHostListener(t, session, "tcp4", "127.0.0.1:0")
	assertWSLBlockedHostListener(t, session, "tcp6", "[::1]:0")
}

func assertWSLBlockedHostListener(t *testing.T, session *wslSession, network, address string) {
	t.Helper()
	listener, err := net.Listen(network, address)
	if err != nil {
		t.Fatalf("host %s listener unavailable: %v", network, err)
	}
	defer listener.Close()
	host, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.execCommand(context.Background(), "/bin/bash", "-c", "exec 3<>/dev/tcp/"+host+"/"+port); err == nil {
		t.Fatalf("network-disabled sandbox connected to host %s listener", network)
	}
}

func TestWSLLocalProviderArtifacts(t *testing.T) {
	roots := requireWSLIntegration(t)
	t.Run("symlink", func(t *testing.T) {
		session := newWSLSession(t, roots.ext4, "artifact-symlink", defaultWSLLimits())
		if _, err := session.execCommand(context.Background(), "/bin/ln", "-s", "/etc/passwd", "/artifacts/link"); !errors.Is(err, ErrArtifactUnverified) {
			t.Fatalf("symlink artifact error=%v", err)
		}
	})
	t.Run("hard link", func(t *testing.T) {
		session := newWSLSession(t, roots.ext4, "artifact-hardlink", defaultWSLLimits())
		outside := filepath.Join(session.root, "secret")
		if err := os.WriteFile(outside, []byte("secret"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Link(outside, filepath.Join(session.artifacts, "link")); err != nil {
			t.Fatal(err)
		}
		if _, err := session.execCommand(context.Background(), "/bin/true"); !errors.Is(err, ErrArtifactUnverified) {
			t.Fatalf("hard-linked artifact error=%v", err)
		}
	})
	t.Run("mutation race", func(t *testing.T) {
		session := newWSLSession(t, roots.ext4, "artifact-race", defaultWSLLimits())
		previous := localArtifactScanBeforeReadHook
		localArtifactScanBeforeReadHook = func() {
			if err := os.WriteFile(filepath.Join(session.artifacts, "artifact"), []byte("changed after stat"), 0o600); err != nil {
				t.Fatalf("mutate artifact: %v", err)
			}
		}
		t.Cleanup(func() { localArtifactScanBeforeReadHook = previous })
		if _, err := session.execCommand(context.Background(), "/bin/sh", "-c", "printf before > /artifacts/artifact"); !errors.Is(err, ErrArtifactUnverified) {
			t.Fatalf("mutation-race artifact error=%v", err)
		}
	})
}

func TestWSLLocalProviderLifecycleAndConcurrency(t *testing.T) {
	roots := requireWSLIntegration(t)
	t.Run("cancel", func(t *testing.T) {
		session := newWSLSession(t, roots.ext4, "cancel", defaultWSLLimits())
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			_, err := session.execCommand(ctx, "/bin/sleep", "30")
			result <- err
		}()
		time.Sleep(50 * time.Millisecond)
		cancel()
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled execution error=%v", err)
		}
	})

	t.Run("close kills descendants", func(t *testing.T) {
		session := newWSLSession(t, roots.ext4, "close", defaultWSLLimits())
		marker := "harness-wsl-marker-" + strconv.FormatInt(time.Now().UnixNano(), 10)
		finished := make(chan error, 1)
		go func() {
			_, err := session.session.Run(context.Background(), Command{Args: []string{"/bin/bash", "-c", "sleep 30 & printf '%s' \"$!\" > /artifacts/" + marker + "; exec sleep 30"}})
			finished <- err
		}()
		waitForWSLFile(t, filepath.Join(session.artifacts, marker))
		waitForMarkerPIDs(t, marker, true)
		if err := session.closeAndRemove(); err != nil {
			t.Fatalf("close and remove session: %v", err)
		}
		if err := <-finished; !errors.Is(err, ErrLeaseTerminated) {
			t.Fatalf("closed run error=%v", err)
		}
		waitForMarkerPIDs(t, marker, false)
	})

	const workers, commandsPerWorker = 8, 25
	latencies := make(chan time.Duration, workers*commandsPerWorker)
	errorsOut := make(chan error, workers)
	var group sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		group.Add(1)
		go func(worker int) {
			defer group.Done()
			for command := 0; command < commandsPerWorker; command++ {
				label := fmt.Sprintf("worker-%d-command-%d", worker, command)
				started := time.Now()
				session, err := startWSLSession(roots.ext4, label, defaultWSLLimits())
				if err != nil {
					errorsOut <- err
					return
				}
				result, execErr := session.execCommand(context.Background(), "/bin/sh", "-c", "printf '"+label+"' > /artifacts/marker")
				cleanupErr := session.closeAndRemove()
				if execErr != nil || cleanupErr != nil || len(result.Artifacts.Items) != 1 || result.Artifacts.Items[0].Key != "marker" {
					errorsOut <- fmt.Errorf("%s exec=%v cleanup=%v artifacts=%#v", label, execErr, cleanupErr, result.Artifacts)
					return
				}
				latencies <- time.Since(started)
			}
		}(worker)
	}
	group.Wait()
	close(errorsOut)
	close(latencies)
	for err := range errorsOut {
		t.Fatal(err)
	}
	all := make([]time.Duration, 0, workers*commandsPerWorker)
	for latency := range latencies {
		all = append(all, latency)
	}
	if len(all) != workers*commandsPerWorker {
		t.Fatalf("completed %d commands, want %d", len(all), workers*commandsPerWorker)
	}
	if p95 := nearestRank(all, 95); p95 > 500*time.Millisecond {
		t.Fatalf("8-worker p95=%s exceeds 500ms", p95)
	}
}

func waitForWSLFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
}

func waitForMarkerPIDs(t *testing.T, marker string, expected bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		found := markerPIDs(marker)
		if (len(found) > 0) == expected {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("marker %q process presence=%t want=%t", marker, len(markerPIDs(marker)) > 0, expected)
}

func markerPIDs(marker string) []int {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}
	result := make([]int, 0)
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid == os.Getpid() {
			continue
		}
		data, err := os.ReadFile(filepath.Join("/proc", entry.Name(), "cmdline"))
		if err == nil && strings.Contains(string(data), marker) {
			result = append(result, pid)
		}
	}
	return result
}

type wslPerformanceEvidence struct {
	Schema      string                 `json:"schema"`
	Method      string                 `json:"percentile_method"`
	Warmups     int                    `json:"warmups"`
	Samples     int                    `json:"samples"`
	Filesystems []wslFilesystemMetrics `json:"filesystems"`
}

type wslFilesystemMetrics struct {
	Name     string `json:"name"`
	P50Nanos int64  `json:"p50_nanos"`
	P95Nanos int64  `json:"p95_nanos"`
	MaxNanos int64  `json:"max_nanos"`
}

func TestWSLPerformanceEvidence(t *testing.T) {
	if os.Getenv(wslPerformanceEnv) != "1" {
		t.Skip("performance run is separate from correctness")
	}
	roots := requireWSLIntegration(t)
	evidence := wslPerformanceEvidence{Schema: "harness-wsl-sandbox-v1", Method: "nearest-rank", Warmups: 10, Samples: 100}
	for _, filesystem := range []struct {
		name string
		root string
	}{{"ext4", roots.ext4}, {"drvfs", roots.windows}} {
		metrics := measureWSLFilesystem(t, filesystem.name, filesystem.root, evidence.Warmups, evidence.Samples)
		evidence.Filesystems = append(evidence.Filesystems, metrics)
		if time.Duration(metrics.P50Nanos) > 100*time.Millisecond || time.Duration(metrics.P95Nanos) > 300*time.Millisecond || time.Duration(metrics.MaxNanos) > time.Second {
			t.Fatalf("%s benchmark exceeds threshold: %#v", filesystem.name, metrics)
		}
	}
	path := os.Getenv(wslEvidenceEnv)
	if path == "" {
		t.Fatal("performance evidence path is required")
	}
	data, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
}

func measureWSLFilesystem(t *testing.T, name, root string, warmups, samples int) wslFilesystemMetrics {
	t.Helper()
	for iteration := 0; iteration < warmups; iteration++ {
		runWSLShortCommand(t, root, name+"-warmup-"+strconv.Itoa(iteration))
	}
	values := make([]time.Duration, 0, samples)
	for iteration := 0; iteration < samples; iteration++ {
		values = append(values, runWSLShortCommand(t, root, name+"-sample-"+strconv.Itoa(iteration)))
	}
	return wslFilesystemMetrics{Name: name, P50Nanos: int64(nearestRank(values, 50)), P95Nanos: int64(nearestRank(values, 95)), MaxNanos: int64(nearestRank(values, 100))}
}

func runWSLShortCommand(t *testing.T, root, label string) time.Duration {
	t.Helper()
	started := time.Now()
	session, err := startWSLSession(root, label, defaultWSLLimits())
	if err != nil {
		t.Fatalf("start short command session: %v", err)
	}
	result, execErr := session.execCommand(context.Background(), "/bin/sh", "-c", "printf x > /artifacts/result")
	cleanupErr := session.closeAndRemove()
	if execErr != nil || cleanupErr != nil || len(result.Artifacts.Items) != 1 || result.Artifacts.Items[0].Key != "result" {
		t.Fatalf("short command result=%#v exec=%v cleanup=%v", result, execErr, cleanupErr)
	}
	return time.Since(started)
}

func nearestRank(values []time.Duration, percentile int) time.Duration {
	if len(values) == 0 || percentile < 1 || percentile > 100 {
		return 0
	}
	copyOf := append([]time.Duration(nil), values...)
	sort.Slice(copyOf, func(left, right int) bool { return copyOf[left] < copyOf[right] })
	rank := (len(copyOf)*percentile + 99) / 100
	return copyOf[rank-1]
}
