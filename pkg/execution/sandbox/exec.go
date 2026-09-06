package sandbox

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"
)

const (
	DefaultExecPreviewBytes  int64 = 16 << 10
	DefaultExecCombinedBytes int64 = 1 << 20
	DefaultExecWallTime            = 30 * time.Second
	DefaultExecMemoryBytes   int64 = 512 << 20
	DefaultExecArtifactCount       = 32
	DefaultExecArtifactBytes int64 = 64 << 20
)

var (
	ErrExecUnsupported = errors.New("sandbox exec is unsupported")
	ErrExecFailed      = errors.New("sandbox exec failed")
	ErrExecTimeout     = errors.New("sandbox exec timed out")
	ErrExecOutputLimit = errors.New("sandbox exec output limit exceeded")
)

// ExecRequest is the provider-neutral command contract. Args is always an
// argv vector; shell command strings and caller-selected host mounts are not
// part of this contract.
type ExecRequest struct {
	Args           []string
	Workdir        string
	Limits         Limits
	Network        NetworkPolicy
	ArtifactPolicy ArtifactPolicy
	Lease          LeaseIdentity
}

type Output struct {
	Preview   []byte `json:"preview,omitempty"`
	Size      int64  `json:"size"`
	Digest    string `json:"digest"`
	Truncated bool   `json:"truncated,omitempty"`
}

type ExecResult struct {
	Stdout    Output      `json:"stdout"`
	Stderr    Output      `json:"stderr"`
	ExitCode  int         `json:"exit_code"`
	Signal    string      `json:"signal,omitempty"`
	TimedOut  bool        `json:"timed_out,omitempty"`
	Artifacts ArtifactSet `json:"artifacts"`
}

// ExecSession is additive: providers can support detailed execution without
// changing the existing Session.Run contract.
type ExecSession interface {
	Session
	Exec(context.Context, ExecRequest) (ExecResult, error)
}

func (r ExecRequest) Validate() error {
	if err := ValidateCommand(Command{Args: r.Args}); err != nil {
		return err
	}
	if r.Workdir != "" {
		return ErrInvalidSpec
	}
	if r.Network != NetworkHost && r.Network != NetworkDisabled && r.Network != NetworkIsolated {
		return ErrInvalidSpec
	}
	if err := r.Limits.Validate(); err != nil {
		return err
	}
	if r.Limits.WallTime > DefaultExecWallTime || r.Limits.MaxCPUTime > DefaultExecWallTime || r.Limits.MaxMemoryBytes > DefaultExecMemoryBytes || r.Limits.MaxOutputBytes > DefaultExecCombinedBytes {
		return ErrInvalidSpec
	}
	if r.ArtifactPolicy.MaxArtifacts <= 0 || r.ArtifactPolicy.MaxArtifacts > DefaultExecArtifactCount || r.ArtifactPolicy.MaxTotalBytes <= 0 || r.ArtifactPolicy.MaxTotalBytes > DefaultExecArtifactBytes {
		return ErrInvalidSpec
	}
	return ValidateLease(r.Lease)
}

func (o Output) Validate() error {
	if o.Size < 0 || int64(len(o.Preview)) > DefaultExecPreviewBytes || int64(len(o.Preview)) > o.Size || o.Truncated != (o.Size > int64(len(o.Preview))) {
		return ErrInvalidSpec
	}
	if len(o.Digest) != sha256.Size*2 {
		return ErrInvalidSpec
	}
	if _, err := hex.DecodeString(o.Digest); err != nil {
		return ErrInvalidSpec
	}
	return nil
}

func (r ExecResult) Validate(policy ArtifactPolicy) error {
	if err := r.Stdout.Validate(); err != nil {
		return err
	}
	if err := r.Stderr.Validate(); err != nil {
		return err
	}
	if r.ExitCode < 0 || r.ExitCode > 255 || len(r.Signal) > MaxIdentifierBytes {
		return ErrInvalidSpec
	}
	if r.Signal != "" && !validArtifactKey(r.Signal) {
		return ErrInvalidSpec
	}
	return r.Artifacts.Validate(policy)
}
