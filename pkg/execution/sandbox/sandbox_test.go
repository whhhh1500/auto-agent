package sandbox

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNormalizeHostPathRejectsSymlinkComponents(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink creation unavailable: %v", err)
	}
	if _, err := NormalizeHostPath(link); err == nil {
		t.Fatal("symlink mount source was accepted")
	}
	child := filepath.Join(link, "child")
	if _, err := NormalizeHostPath(child); err == nil {
		t.Fatal("symlinked parent mount source was accepted")
	}
}

func TestNormalizeHostPathRejectsRootAndUNC(t *testing.T) {
	if _, err := NormalizeHostPath(string(filepath.Separator)); err == nil {
		t.Fatal("host root mount was accepted")
	}
	for _, value := range []string{`\\server\share`, "//server/share"} {
		if _, err := NormalizeHostPath(value); err == nil {
			t.Fatalf("UNC mount source was accepted: %q", value)
		}
	}
}

func validSpec() SessionSpec {
	return SessionSpec{
		RequestedAssurance: Assurance{Level: AssuranceProcess},
		Limits:             Limits{WallTime: time.Second, MaxOutputBytes: 1024, MaxMemoryBytes: 1024, MaxCPUTime: time.Second},
		Network:            NetworkHost,
		ArtifactPolicy:     ArtifactPolicy{MaxArtifacts: 2, MaxTotalBytes: 2048},
		Lease:              LeaseIdentity{RunID: "run", SegmentID: "segment", ModuleID: "module", CompositionRev: "composition", Token: "token"},
	}
}

func TestAssuranceComparisonFailsClosed(t *testing.T) {
	if (Assurance{Level: AssuranceProcess}).Satisfies(Assurance{Level: AssuranceContainer}) {
		t.Fatal("weaker process assurance satisfied container request")
	}
	if (Assurance{Level: AssuranceProcess}).Satisfies(Assurance{Level: AssuranceProcess, NetworkIsolation: true}) {
		t.Fatal("missing network isolation satisfied isolated request")
	}
	if (Assurance{Level: AssuranceContainer, SharedKernel: true}).Satisfies(Assurance{Level: AssuranceContainer, DedicatedKernel: true}) {
		t.Fatal("shared kernel satisfied dedicated kernel request")
	}
	report := AssuranceReport{Available: true, Actual: Assurance{Level: AssuranceProcess, SharedKernel: true}, Network: NetworkHost, LimitsEnforced: false}
	if !errors.Is(report.SatisfiesSpec(validSpec()), ErrUnavailable) {
		t.Fatal("unenforced limits were accepted")
	}
	for _, actual := range []Assurance{{Level: AssuranceProcess}, {Level: AssuranceVM, SharedKernel: true}} {
		if !errors.Is((AssuranceReport{Available: true, Actual: actual}).Satisfies(Assurance{Level: AssuranceNone}), ErrAssuranceTooWeak) {
			t.Fatalf("invalid actual assurance accepted: %#v", actual)
		}
	}
}

func TestCapabilityReportSupportsHostAndDisabledModes(t *testing.T) {
	report := AssuranceReport{
		Available:         true,
		Actual:            Assurance{Level: AssuranceProcess, SharedKernel: true},
		Network:           NetworkHost,
		SupportedNetworks: []NetworkPolicy{NetworkHost, NetworkDisabled},
		LimitsEnforced:    true,
		MountsEnforced:    true,
	}
	host := validSpec()
	if err := report.SatisfiesSpec(host); err != nil {
		t.Fatalf("host capability rejected: %v", err)
	}
	disabled := validSpec()
	disabled.Network = NetworkDisabled
	disabled.RequestedAssurance.NetworkIsolation = true
	if err := report.SatisfiesSpec(disabled); err != nil {
		t.Fatalf("disabled capability rejected: %v", err)
	}
}

func TestNetworkPolicyAndAssuranceMustMatch(t *testing.T) {
	for _, tc := range []struct {
		policy   NetworkPolicy
		isolated bool
		want     bool
	}{
		{NetworkHost, false, true}, {NetworkDisabled, true, true}, {NetworkIsolated, true, true},
		{NetworkHost, true, false}, {NetworkDisabled, false, false}, {NetworkIsolated, false, false},
	} {
		spec := validSpec()
		spec.Network, spec.RequestedAssurance.NetworkIsolation = tc.policy, tc.isolated
		report := AssuranceReport{Available: true, Actual: Assurance{Level: AssuranceProcess, SharedKernel: true, NetworkIsolation: tc.isolated}, Network: tc.policy, LimitsEnforced: true, MountsEnforced: true}
		if got := report.SatisfiesSpec(spec) == nil; got != tc.want {
			t.Fatalf("policy=%s isolated=%t accepted=%t want=%t", tc.policy, tc.isolated, got, tc.want)
		}
	}
}

func TestSessionSpecRejectsTraversalAndWeakNetwork(t *testing.T) {
	spec := validSpec()
	spec.Mounts = []Mount{{Source: "/workspace/../outside", Target: "/sandbox/input"}}
	if err := spec.Validate(); err == nil {
		t.Fatal("mount traversal accepted")
	}
	if _, err := NormalizeVirtualPath("/sandbox/../root"); err == nil {
		t.Fatal("path traversal accepted")
	}
	if _, err := NormalizeHostPath(`\\server\share\root`); err == nil {
		t.Fatal("UNC host path accepted")
	}
	spec = validSpec()
	spec.Mounts = []Mount{{Source: "/tmp/a", Target: "/input"}, {Source: "/tmp/b", Target: "/input"}}
	if err := spec.Validate(); err == nil {
		t.Fatal("duplicate mount target accepted")
	}
	if err := (Limits{WallTime: MaxWallTime + time.Second, MaxOutputBytes: 1, MaxMemoryBytes: 1, MaxCPUTime: 1}).Validate(); err == nil {
		t.Fatal("wall limit overflow accepted")
	}
	if err := (Limits{WallTime: time.Second, MaxOutputBytes: MaxOutputBytes + 1, MaxMemoryBytes: 1, MaxCPUTime: 1}).Validate(); err == nil {
		t.Fatal("output limit overflow accepted")
	}
	if err := (Limits{WallTime: time.Second, MaxOutputBytes: 1, MaxMemoryBytes: MaxMemoryBytes + 1, MaxCPUTime: time.Second}).Validate(); err == nil {
		t.Fatal("memory limit overflow accepted")
	}
	if err := (Limits{WallTime: time.Second, MaxOutputBytes: 1, MaxMemoryBytes: 1, MaxCPUTime: MaxCPUTime + time.Second}).Validate(); err == nil {
		t.Fatal("CPU limit overflow accepted")
	}
	if err := ValidateProviderID(ProviderID("bad\u200b")); err == nil {
		t.Fatal("unicode control provider id accepted")
	}
}

func TestLeaseFenceAndIdempotentTermination(t *testing.T) {
	lease, err := NewFencedLease(validSpec().Lease)
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Validate(validSpec().Lease); err != nil {
		t.Fatal(err)
	}
	other := validSpec().Lease
	other.SegmentID = "other"
	if !errors.Is(lease.Validate(other), ErrLeaseMismatch) {
		t.Fatal("lease mismatch was not fenced")
	}
	lease.Terminate(false)
	lease.Terminate(true)
	if lease.State() != LeaseTerminated {
		t.Fatalf("termination was not idempotent: %q", lease.State())
	}
	second := *lease
	if second.State() != LeaseTerminated {
		t.Fatal("copied lease did not share state")
	}
	active, err := NewFencedLease(validSpec().Lease)
	if err != nil {
		t.Fatal(err)
	}
	copyOf := *active
	active.Terminate(false)
	if copyOf.State() != LeaseTerminated {
		t.Fatal("copied lease state diverged")
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = copyOf.Validate(validSpec().Lease); copyOf.Terminate(false) }()
	}
	wg.Wait()
}

func TestLocalProviderDoesNotAdvertiseWindowsIsolation(t *testing.T) {
	report := (LocalProvider{}).Probe(context.Background())
	if runtime.GOOS == "windows" && report.Available {
		t.Fatalf("Windows advertised sandbox availability: %#v", report)
	}
	if runtime.GOOS == "windows" && !strings.Contains(report.UnavailableCause, "windows") {
		t.Fatalf("Windows unavailability was not explicit: %#v", report)
	}
}

func TestLocalProviderStartFailsClosed(t *testing.T) {
	provider := LocalProvider{}
	if provider.Probe(context.Background()).Available {
		_, err := provider.Start(context.Background(), validSpec())
		if !errors.Is(err, ErrInvalidSpec) {
			t.Fatalf("available local provider accepted missing artifact mount: %v", err)
		}
		return
	}
	if _, err := provider.Start(context.Background(), validSpec()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("unavailable local provider did not fail closed: %v", err)
	}
}

func TestValidateCommandAndStagedArtifact(t *testing.T) {
	if err := ValidateCommand(Command{Args: []string{"echo", "ok"}}); err != nil {
		t.Fatal(err)
	}
	if err := ValidateCommand(Command{Args: []string{"echo", strings.Repeat("x", MaxArgBytes+1)}}); err == nil {
		t.Fatal("oversized argument accepted")
	}
	digest := strings.Repeat("a", 64)
	set := ArtifactSet{Items: []Artifact{{Key: "result", Digest: digest, Size: 1, Verified: true}}}
	if err := set.Validate(ArtifactPolicy{MaxArtifacts: 1, MaxTotalBytes: 2}); err != nil {
		t.Fatal(err)
	}
	set.Items[0].Verified = false
	if !errors.Is(set.Validate(ArtifactPolicy{MaxArtifacts: 1, MaxTotalBytes: 2}), ErrArtifactUnverified) {
		t.Fatal("unverified artifact accepted")
	}
	set.Items[0].Verified = true
	set.Items = append(set.Items, set.Items[0])
	if !errors.Is(set.Validate(ArtifactPolicy{MaxArtifacts: 2, MaxTotalBytes: 2}), ErrArtifactUnverified) {
		t.Fatal("duplicate artifact key accepted")
	}
	if err := ValidateArtifact(Artifact{Key: "../escape", Digest: digest, Size: 1, Verified: true}); err == nil {
		t.Fatal("artifact traversal key accepted")
	}
}
