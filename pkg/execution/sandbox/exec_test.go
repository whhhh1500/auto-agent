package sandbox

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
	"time"
)

type execTestSession struct {
	report AssuranceReport
	limits Limits
	result ExecResult
	seen   ExecRequest
}

func (s *execTestSession) Assurance() AssuranceReport { return s.report }
func (s *execTestSession) Limits() Limits             { return s.limits }
func (s *execTestSession) Run(context.Context, Command) (ArtifactSet, error) {
	return ArtifactSet{Items: []Artifact{}}, nil
}
func (s *execTestSession) Exec(_ context.Context, request ExecRequest) (ExecResult, error) {
	s.seen = request
	return s.result, nil
}
func (s *execTestSession) Close(context.Context) error { return nil }

func execTestPolicy() (Limits, ArtifactPolicy, LeaseIdentity) {
	return Limits{WallTime: time.Second, MaxCPUTime: time.Second, MaxMemoryBytes: 1 << 20, MaxOutputBytes: 1 << 20}, ArtifactPolicy{MaxArtifacts: 1, MaxTotalBytes: 1 << 20}, LeaseIdentity{RunID: "run", SegmentID: "segment", ModuleID: "module", CompositionRev: "rev", Token: "token"}
}

func validExecResult() ExecResult {
	digest := sha256.Sum256(nil)
	return ExecResult{Stdout: Output{Digest: hex.EncodeToString(digest[:])}, Stderr: Output{Digest: hex.EncodeToString(digest[:])}, Artifacts: ArtifactSet{Items: []Artifact{}}}
}

func TestExecRequestValidationUsesCommandAndStrictBounds(t *testing.T) {
	limits, policy, lease := execTestPolicy()
	base := ExecRequest{Args: []string{"echo", "ok"}, Limits: limits, Network: NetworkDisabled, ArtifactPolicy: policy, Lease: lease}
	for name, mutate := range map[string]func(*ExecRequest){
		"nul argv":  func(r *ExecRequest) { r.Args[1] = "bad\x00arg" },
		"workdir":   func(r *ExecRequest) { r.Workdir = "/workspace" },
		"cpu":       func(r *ExecRequest) { r.Limits.MaxCPUTime = DefaultExecWallTime + time.Nanosecond },
		"artifacts": func(r *ExecRequest) { r.ArtifactPolicy.MaxArtifacts = 0 },
		"bytes":     func(r *ExecRequest) { r.ArtifactPolicy.MaxTotalBytes = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := base
			mutate(&candidate)
			if !errors.Is(candidate.Validate(), ErrInvalidSpec) {
				t.Fatalf("Validate() = %v, want ErrInvalidSpec", candidate.Validate())
			}
		})
	}
}

func TestExecRequestValidationAcceptsKnownNetworkPolicies(t *testing.T) {
	limits, policy, lease := execTestPolicy()
	for _, network := range []NetworkPolicy{NetworkHost, NetworkDisabled, NetworkIsolated} {
		request := ExecRequest{Args: []string{"echo", "ok"}, Limits: limits, Network: network, ArtifactPolicy: policy, Lease: lease}
		if err := request.Validate(); err != nil {
			t.Fatalf("network=%q Validate() = %v", network, err)
		}
	}
}

func TestExecResultValidationRejectsInconsistentOutput(t *testing.T) {
	result := validExecResult()
	result.Stdout = Output{Preview: []byte("x"), Size: 0, Digest: result.Stdout.Digest}
	if !errors.Is(result.Validate(ArtifactPolicy{MaxArtifacts: 1, MaxTotalBytes: 1}), ErrInvalidSpec) {
		t.Fatal("inconsistent output was accepted")
	}
	result = validExecResult()
	result.Stdout.Digest = "not-a-digest"
	if !errors.Is(result.Validate(ArtifactPolicy{MaxArtifacts: 1, MaxTotalBytes: 1}), ErrInvalidSpec) {
		t.Fatal("invalid digest was accepted")
	}
}

func TestGuardedExecRequiresExactSessionPolicy(t *testing.T) {
	limits, policy, lease := execTestPolicy()
	raw := &execTestSession{report: AssuranceReport{Available: true, Actual: Assurance{Level: AssuranceProcess, SharedKernel: true}, Network: NetworkDisabled, SupportedNetworks: []NetworkPolicy{NetworkDisabled}, LimitsEnforced: true, MountsEnforced: true}, limits: limits, result: validExecResult()}
	session, err := newGuardedSession(raw, SessionSpec{RequestedAssurance: Assurance{Level: AssuranceProcess}, Limits: limits, Network: NetworkDisabled, ArtifactPolicy: policy, Lease: lease})
	if err != nil {
		t.Fatal(err)
	}
	request := ExecRequest{Args: []string{"echo"}, Limits: limits, Network: NetworkDisabled, ArtifactPolicy: policy, Lease: lease}
	if _, err := session.Exec(context.Background(), request); err != nil {
		t.Fatalf("Exec() = %v", err)
	}
	request.ArtifactPolicy.MaxTotalBytes++
	if _, err := session.Exec(context.Background(), request); !errors.Is(err, ErrLeaseMismatch) {
		t.Fatalf("widened request error = %v", err)
	}
}
