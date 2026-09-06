//go:build linux

package sandbox

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

type fakeLocalProbeResolver struct {
	paths map[string]string
	errs  map[string]error
	calls []string
}

func (resolver *fakeLocalProbeResolver) LookPath(file string) (string, error) {
	resolver.calls = append(resolver.calls, file)
	if err := resolver.errs[file]; err != nil {
		return "", err
	}
	return resolver.paths[file], nil
}

type fakeLocalProbeRunner struct {
	name        string
	args        []string
	hasDeadline bool
	deadline    time.Time
	run         func(context.Context, string, ...string) error
}

func (runner *fakeLocalProbeRunner) Run(ctx context.Context, name string, args ...string) error {
	runner.name = name
	runner.args = append([]string(nil), args...)
	runner.deadline, runner.hasDeadline = ctx.Deadline()
	if runner.run == nil {
		return nil
	}
	return runner.run(ctx, name, args...)
}

func readyLocalProbeResolver() *fakeLocalProbeResolver {
	return &fakeLocalProbeResolver{paths: map[string]string{"bwrap": "/test/bwrap", "prlimit": "/test/prlimit"}}
}

func TestProbeLocalUsesFixedPrlimitBwrapCapabilityCommand(t *testing.T) {
	resolver := readyLocalProbeResolver()
	runner := &fakeLocalProbeRunner{}
	report := probeLocal(context.Background(), resolver, runner)
	if !report.Available || report.Actual != (Assurance{Level: AssuranceProcess, SharedKernel: true}) || report.Network != NetworkHost || !report.LimitsEnforced || !report.MountsEnforced {
		t.Fatalf("report=%#v", report)
	}
	if got, want := resolver.calls, []string{"bwrap", "prlimit"}; !equalStrings(got, want) {
		t.Fatalf("resolver calls=%#v want=%#v", got, want)
	}
	if runner.name != "/test/prlimit" {
		t.Fatalf("runner command=%q", runner.name)
	}
	if !runner.hasDeadline || time.Until(runner.deadline) <= 0 || time.Until(runner.deadline) > localProbeTimeout {
		t.Fatalf("probe deadline hasDeadline=%t deadline=%s", runner.hasDeadline, runner.deadline)
	}
	joined := strings.Join(runner.args, " ")
	for _, required := range []string{
		"--as=", "--cpu=", "--nofile=", "--nproc=", "-- /test/bwrap",
		"--unshare-user", "--unshare-pid", "--unshare-ipc", "--unshare-uts", "--unshare-cgroup", "--unshare-net",
		"--clearenv", "--tmpfs /tmp", "--proc /proc", "--dev /dev", "-- /bin/true",
	} {
		if !strings.Contains(joined, required) {
			t.Fatalf("probe argv missing %q: %#v", required, runner.args)
		}
	}
	for _, forbidden := range []string{"/workspace", "/artifacts", "sh -c"} {
		if strings.Contains(joined, forbidden) {
			t.Fatalf("probe argv contains forbidden %q: %#v", forbidden, runner.args)
		}
	}
}

func TestProbeLocalFailsClosedForLookupAndExecutionFailures(t *testing.T) {
	testCases := []struct {
		name     string
		resolver *fakeLocalProbeResolver
		runner   *fakeLocalProbeRunner
	}{
		{
			name:     "missing bwrap",
			resolver: &fakeLocalProbeResolver{errs: map[string]error{"bwrap": errors.New("private bwrap path")}},
			runner:   &fakeLocalProbeRunner{},
		},
		{
			name:     "missing prlimit",
			resolver: &fakeLocalProbeResolver{paths: map[string]string{"bwrap": "/test/bwrap"}, errs: map[string]error{"prlimit": errors.New("private prlimit path")}},
			runner:   &fakeLocalProbeRunner{},
		},
		{
			name:     "launch failure is sanitized",
			resolver: readyLocalProbeResolver(),
			runner:   &fakeLocalProbeRunner{run: func(context.Context, string, ...string) error { return errors.New("secret launch failure") }},
		},
		{
			name:     "failed exit is sanitized",
			resolver: readyLocalProbeResolver(),
			runner:   &fakeLocalProbeRunner{run: func(context.Context, string, ...string) error { return errors.New("exit status 17: secret output") }},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			report := probeLocal(context.Background(), testCase.resolver, testCase.runner)
			if report.Available || report.UnavailableCause != localProbeUnavailableCause || strings.Contains(report.UnavailableCause, "secret") || strings.Contains(report.UnavailableCause, "private") {
				t.Fatalf("unsafe failed probe report=%#v", report)
			}
		})
	}
}

func TestProbeLocalFailsClosedOnTimeoutAndCancellation(t *testing.T) {
	t.Run("timeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		runner := &fakeLocalProbeRunner{run: func(ctx context.Context, _ string, _ ...string) error {
			<-ctx.Done()
			return ctx.Err()
		}}
		if report := probeLocal(ctx, readyLocalProbeResolver(), runner); report.Available || report.UnavailableCause != localProbeUnavailableCause {
			t.Fatalf("timeout report=%#v", report)
		}
	})
	t.Run("canceled during execution", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		runner := &fakeLocalProbeRunner{run: func(context.Context, string, ...string) error {
			cancel()
			return nil
		}}
		if report := probeLocal(ctx, readyLocalProbeResolver(), runner); report.Available || report.UnavailableCause != localProbeUnavailableCause {
			t.Fatalf("canceled report=%#v", report)
		}
	})
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func TestLocalMountPolicyRejectsReservedAndUnscopedTargets(t *testing.T) {
	root := t.TempDir()
	for _, target := range []string{"/", "/tmp", "/proc", "/dev", "/etc", "/usr", "/workspace/../escape", "/other"} {
		if _, err := validateLocalMounts([]Mount{{Source: root, Target: target, ReadOnly: true}, {Source: root, Target: "/artifacts"}}); !errors.Is(err, ErrInvalidSpec) {
			t.Fatalf("target %q accepted: %v", target, err)
		}
	}
}

func TestLocalArgvIsDirectAndUsesStableWorkingDirectory(t *testing.T) {
	if _, err := exec.LookPath("bwrap"); err != nil {
		t.Skip("bwrap unavailable")
	}
	if _, err := exec.LookPath("prlimit"); err != nil {
		t.Skip("prlimit unavailable")
	}
	root := t.TempDir()
	spec := validSpec()
	spec.Mounts = []Mount{{Source: root, Target: "/artifacts"}}
	argv, err := buildLocalArgv(Command{Args: []string{"/bin/echo", "a value", "--literal"}}, spec)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(argv, " ")
	if strings.Contains(joined, "sh -c") || !strings.Contains(joined, "--chdir /tmp") || !strings.Contains(joined, "--nproc=256") {
		t.Fatalf("unsafe/unstable argv=%q", joined)
	}
	if argv[len(argv)-3] != "/bin/echo" || argv[len(argv)-2] != "a value" || argv[len(argv)-1] != "--literal" {
		t.Fatalf("direct argv was not preserved: %#v", argv)
	}
}

func TestLocalArtifactScannerRejectsSymlink(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(root+"/ok", []byte("ok"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(root+"/ok", root+"/link"); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	_, err := scanArtifacts(root, ArtifactPolicy{MaxArtifacts: 2, MaxTotalBytes: 10})
	if !errors.Is(err, ErrArtifactUnverified) {
		t.Fatalf("symlink artifact accepted: %v", err)
	}
}

func TestLocalArtifactScannerRejectsHardLink(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir() + "/secret"
	if err := os.WriteFile(outside, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(outside, root+"/link"); err != nil {
		t.Skipf("hard link unavailable: %v", err)
	}
	if _, err := scanArtifacts(root, ArtifactPolicy{MaxArtifacts: 1, MaxTotalBytes: 10}); !errors.Is(err, ErrArtifactUnverified) {
		t.Fatalf("hard-linked artifact accepted: %v", err)
	}
}

func TestLocalArtifactScannerRejectsMutationDuringRead(t *testing.T) {
	root := t.TempDir()
	path := root + "/artifact"
	if err := os.WriteFile(path, []byte("before"), 0o600); err != nil {
		t.Fatal(err)
	}
	previous := localArtifactScanBeforeReadHook
	localArtifactScanBeforeReadHook = func() {
		if err := os.WriteFile(path, []byte("after mutation"), 0o600); err != nil {
			t.Fatalf("mutate artifact: %v", err)
		}
	}
	t.Cleanup(func() { localArtifactScanBeforeReadHook = previous })
	if _, err := scanArtifacts(root, ArtifactPolicy{MaxArtifacts: 1, MaxTotalBytes: 32}); !errors.Is(err, ErrArtifactUnverified) {
		t.Fatalf("mutated artifact accepted: %v", err)
	}
}

func TestBoundedDiscardKillsOnCombinedOutputOverflow(t *testing.T) {
	killed := false
	sink := &boundedDiscard{limit: 3, kill: func() { killed = true }}
	if _, err := sink.Write([]byte("1234")); !errors.Is(err, ErrOutputLimit) || !killed || !sink.exceededLimit() {
		t.Fatalf("overflow err=%v killed=%t exceeded=%t", err, killed, sink.exceededLimit())
	}
}

func TestLocalCPUFormattingRoundsUp(t *testing.T) {
	if got := formatCPUSeconds(time.Second + time.Millisecond); got != "2" {
		t.Fatalf("cpu seconds=%s", got)
	}
}

type testArtifactInfo struct {
	stat syscall.Stat_t
}

func (info testArtifactInfo) Name() string      { return "artifact" }
func (info testArtifactInfo) Size() int64       { return info.stat.Size }
func (info testArtifactInfo) Mode() os.FileMode { return 0o600 }
func (info testArtifactInfo) ModTime() time.Time {
	return time.Unix(info.stat.Mtim.Sec, info.stat.Mtim.Nsec)
}
func (info testArtifactInfo) IsDir() bool { return false }
func (info testArtifactInfo) Sys() any    { stat := info.stat; return &stat }

func TestSameArtifactFileRejectsTimestampMutation(t *testing.T) {
	before := testArtifactInfo{stat: syscall.Stat_t{Dev: 1, Ino: 2, Size: 4, Mtim: syscall.Timespec{Sec: 10, Nsec: 1}, Ctim: syscall.Timespec{Sec: 10, Nsec: 1}}}
	after := before
	after.stat.Mtim.Nsec++
	if sameArtifactFile(before, after) {
		t.Fatal("timestamp-mutated artifact was accepted")
	}
	after = before
	if !sameArtifactFile(before, after) {
		t.Fatal("unchanged artifact was rejected")
	}
}
