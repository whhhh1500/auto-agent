// Package sandbox defines the small, provider-neutral boundary for isolated
// command execution.  Providers must report their real guarantees; this
// package never treats an ordinary process as a sandbox.
package sandbox

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	MaxIdentifierBytes   = 128
	MaxArgs              = 128
	MaxArgBytes          = 16 << 10
	MaxMounts            = 32
	MaxArtifactKeyBytes  = 256
	MaxArtifactBytes     = 64 << 20
	MaxArtifactCount     = 128
	MaxWallTime          = 24 * time.Hour
	MaxCPUTime           = 24 * time.Hour
	MaxOutputBytes       = 64 << 20
	MaxMemoryBytes       = 8 << 30
	localProviderVersion = "1"
)

var (
	ErrUnavailable        = errors.New("sandbox unavailable")
	ErrAssuranceTooWeak   = errors.New("sandbox assurance is weaker than requested")
	ErrInvalidSpec        = errors.New("invalid sandbox session spec")
	ErrLeaseMismatch      = errors.New("sandbox lease mismatch")
	ErrLeaseTerminated    = errors.New("sandbox lease terminated")
	ErrArtifactUnverified = errors.New("sandbox artifact is not verified")
	ErrOutputLimit        = errors.New("sandbox output limit exceeded")
	ErrWallTimeLimit      = errors.New("sandbox wall time limit exceeded")
)

type ProviderID string

func ValidateProviderID(id ProviderID) error {
	value := string(id)
	if value == "" || len(value) > MaxIdentifierBytes || !utf8.ValidString(value) || strings.TrimSpace(value) != value || containsControl(value) {
		return fmt.Errorf("%w: invalid provider id", ErrInvalidSpec)
	}
	return nil
}

type AssuranceLevel uint8

const (
	AssuranceNone AssuranceLevel = iota
	AssuranceProcess
	AssuranceContainer
	AssuranceVM
)

type Assurance struct {
	Level            AssuranceLevel `json:"level"`
	SharedKernel     bool           `json:"shared_kernel"`
	DedicatedKernel  bool           `json:"dedicated_kernel"`
	NetworkIsolation bool           `json:"network_isolation"`
}

type AssuranceReport struct {
	Available         bool              `json:"available"`
	Readiness         ProviderReadiness `json:"readiness,omitempty"`
	Actual            Assurance         `json:"actual"`
	Network           NetworkPolicy     `json:"network"`
	SupportedNetworks []NetworkPolicy   `json:"supported_networks,omitempty"`
	LimitsEnforced    bool              `json:"limits_enforced"`
	MountsEnforced    bool              `json:"mounts_enforced"`
	UnavailableCause  string            `json:"unavailable_cause,omitempty"`
}

// ProviderReadiness is the sanitized setup/readiness state of a provider.
// It is optional so existing providers can continue returning only Available.
type ProviderReadiness string

const (
	ProviderReadinessReady          ProviderReadiness = "ready"
	ProviderReadinessNotInstalled   ProviderReadiness = "not_installed"
	ProviderReadinessRepairRequired ProviderReadiness = "repair_required"
	ProviderReadinessUnavailable    ProviderReadiness = "unavailable"
)

type Limits struct {
	WallTime       time.Duration `json:"wall_time"`
	MaxOutputBytes int64         `json:"max_output_bytes"`
	MaxMemoryBytes int64         `json:"max_memory_bytes"`
	MaxCPUTime     time.Duration `json:"max_cpu_time"`
}

type NetworkPolicy string

const (
	NetworkHost     NetworkPolicy = "host"
	NetworkDisabled NetworkPolicy = "disabled"
	NetworkIsolated NetworkPolicy = "isolated"
)

type Mount struct {
	Source   string `json:"source"`
	Target   string `json:"target"`
	ReadOnly bool   `json:"read_only"`
}

type ArtifactPolicy struct {
	MaxArtifacts  int   `json:"max_artifacts"`
	MaxTotalBytes int64 `json:"max_total_bytes"`
}

type LeaseIdentity struct {
	RunID          string `json:"run_id"`
	SegmentID      string `json:"segment_id"`
	ModuleID       string `json:"module_id"`
	CompositionRev string `json:"composition_revision"`
	Token          string `json:"token"`
}

type LeaseState string

const (
	LeaseActive     LeaseState = "active"
	LeaseTerminated LeaseState = "terminated"
	LeaseUnknown    LeaseState = "unknown"
)

type FencedLease struct {
	state *leaseState
}
type leaseState struct {
	mu       sync.RWMutex
	identity LeaseIdentity
	state    LeaseState
}

func NewFencedLease(identity LeaseIdentity) (*FencedLease, error) {
	if err := ValidateLease(identity); err != nil {
		return nil, err
	}
	return &FencedLease{state: &leaseState{identity: identity, state: LeaseActive}}, nil
}

func (l *FencedLease) Validate(identity LeaseIdentity) error {
	if l == nil {
		return ErrLeaseTerminated
	}
	if err := ValidateLease(identity); err != nil {
		return err
	}
	if l.state == nil {
		return ErrLeaseTerminated
	}
	l.state.mu.RLock()
	defer l.state.mu.RUnlock()
	if l.state.identity != identity {
		return ErrLeaseMismatch
	}
	if l.state.state != LeaseActive {
		return ErrLeaseTerminated
	}
	return nil
}

func (l *FencedLease) Terminate(unknown bool) {
	if l == nil {
		return
	}
	if l.state == nil {
		return
	}
	l.state.mu.Lock()
	defer l.state.mu.Unlock()
	if l.state.state != LeaseActive {
		return
	}
	if unknown {
		l.state.state = LeaseUnknown
	} else {
		l.state.state = LeaseTerminated
	}
}

func (l *FencedLease) Identity() LeaseIdentity {
	if l == nil {
		return LeaseIdentity{}
	}
	if l.state == nil {
		return LeaseIdentity{}
	}
	l.state.mu.RLock()
	defer l.state.mu.RUnlock()
	return l.state.identity
}
func (l *FencedLease) State() LeaseState {
	if l == nil {
		return LeaseUnknown
	}
	if l.state == nil {
		return LeaseUnknown
	}
	l.state.mu.RLock()
	defer l.state.mu.RUnlock()
	return l.state.state
}

type SessionSpec struct {
	RequestedAssurance Assurance
	Limits             Limits
	Network            NetworkPolicy
	Mounts             []Mount
	ArtifactPolicy     ArtifactPolicy
	Lease              LeaseIdentity
}

func cloneSessionSpec(spec SessionSpec) SessionSpec {
	if len(spec.Mounts) > 0 {
		spec.Mounts = append([]Mount(nil), spec.Mounts...)
	}
	return spec
}

type Command struct {
	Args []string
}

type Artifact struct {
	Key      string `json:"key"`
	Digest   string `json:"digest"`
	Size     int64  `json:"size"`
	Verified bool   `json:"verified"`
}

type ArtifactSet struct {
	Items []Artifact `json:"items"`
}

type Provider interface {
	ID() ProviderID
	Probe(context.Context) AssuranceReport
	Start(context.Context, SessionSpec) (Session, error)
}

// RootOwnershipStarter is an optional provider lifecycle API for a provider
// that may adopt the adapter-created session root during Start. The boolean is
// an ownership fact for that *one invocation*: true means the provider has
// durably adopted the root or has uncertain native cleanup and the adapter
// must not recursively delete it, even when Start returns an error.
//
// false means the provider has completed all of its local cleanup and the
// adapter retains ordinary root cleanup ownership. This must not be a static
// marker: a pre-adoption native failure has no durable root owner.
// Implementations must preserve a true-owned root as recoverable evidence and
// must never report false after changing its ACLs or retaining native state.
type RootOwnershipStarter interface {
	StartWithRootOwnership(context.Context, SessionSpec) (Session, bool, error)
}

type Session interface {
	// Assurance and Limits must be concurrency-safe with Run and Close and
	// must report stable capabilities for the lifetime of a started session.
	// Run and Close must honor their contexts. Close MUST be safe while Run is
	// active and SHOULD use its context to interrupt that Run; the registry
	// cannot forcibly stop a provider callback that ignores context.
	Assurance() AssuranceReport
	Limits() Limits
	Run(context.Context, Command) (ArtifactSet, error)
	Close(context.Context) error
}

// PublicationOutcome is the terminal publication result supplied by the
// adapter after verified artifacts have either been published or failed.
type PublicationOutcome string

const (
	PublicationPublished PublicationOutcome = "published"
	PublicationFailed    PublicationOutcome = "failed"
)

func (o PublicationOutcome) Validate() error {
	if o != PublicationPublished && o != PublicationFailed {
		return ErrInvalidSpec
	}
	return nil
}

// PublicationFinalizer is optional. Providers that implement it exclusively
// own terminal session-root cleanup after artifact publication.
type PublicationFinalizer interface {
	Session
	FinalizePublication(context.Context, PublicationOutcome) error
}

func (a Assurance) Satisfies(requested Assurance) bool {
	if a.ValidateActual() != nil || requested.Validate() != nil || a.Level < requested.Level || requested.NetworkIsolation && !a.NetworkIsolation || requested.DedicatedKernel && !a.DedicatedKernel {
		return false
	}
	return true
}

func (a Assurance) Validate() error {
	if a.Level > AssuranceVM || a.SharedKernel && a.DedicatedKernel || a.Level == AssuranceVM && a.SharedKernel {
		return ErrInvalidSpec
	}
	return nil
}

func (a Assurance) ValidateActual() error {
	if err := a.Validate(); err != nil {
		return err
	}
	switch a.Level {
	case AssuranceNone:
		if a.SharedKernel || a.DedicatedKernel || a.NetworkIsolation {
			return ErrInvalidSpec
		}
	case AssuranceProcess, AssuranceContainer:
		if !a.SharedKernel || a.DedicatedKernel {
			return ErrInvalidSpec
		}
	case AssuranceVM:
		if a.SharedKernel || !a.DedicatedKernel {
			return ErrInvalidSpec
		}
	}
	return nil
}

func (r AssuranceReport) Satisfies(requested Assurance) error {
	if !r.Available {
		return ErrUnavailable
	}
	if !r.Actual.Satisfies(requested) {
		return ErrAssuranceTooWeak
	}
	return nil
}

func (r AssuranceReport) SatisfiesSpec(spec SessionSpec) error {
	if !r.Available {
		return ErrUnavailable
	}
	if len(r.SupportedNetworks) == 0 {
		if err := r.Satisfies(spec.RequestedAssurance); err != nil {
			return err
		}
		if spec.Network != r.Network || (r.Network == NetworkDisabled || r.Network == NetworkIsolated) != r.Actual.NetworkIsolation {
			return ErrAssuranceTooWeak
		}
	} else {
		found := false
		for _, network := range r.SupportedNetworks {
			if network == spec.Network {
				found = true
				break
			}
		}
		if !found {
			return ErrAssuranceTooWeak
		}
		// A capability report describes several selectable modes. Project the
		// selected network mode onto the otherwise mode-independent assurance
		// before checking the request; the started session must report the exact
		// mode separately.
		actual := r.Actual
		actual.NetworkIsolation = spec.Network != NetworkHost
		if !actual.Satisfies(spec.RequestedAssurance) {
			return ErrAssuranceTooWeak
		}
	}
	if !r.LimitsEnforced || (len(spec.Mounts) > 0 && !r.MountsEnforced) {
		return ErrUnavailable
	}
	return nil
}

func (s SessionSpec) Validate() error {
	if s.RequestedAssurance.Level > AssuranceVM {
		return ErrInvalidSpec
	}
	if err := s.RequestedAssurance.Validate(); err != nil {
		return err
	}
	if (s.Network == NetworkDisabled || s.Network == NetworkIsolated) != s.RequestedAssurance.NetworkIsolation {
		return fmt.Errorf("%w: network assurance conflicts with policy", ErrInvalidSpec)
	}
	if err := s.Limits.Validate(); err != nil {
		return err
	}
	if s.Network != NetworkHost && s.Network != NetworkDisabled && s.Network != NetworkIsolated {
		return fmt.Errorf("%w: unknown network policy", ErrInvalidSpec)
	}
	if len(s.Mounts) > MaxMounts {
		return fmt.Errorf("%w: too many mounts", ErrInvalidSpec)
	}
	if s.ArtifactPolicy.MaxArtifacts <= 0 || s.ArtifactPolicy.MaxArtifacts > MaxArtifactCount || s.ArtifactPolicy.MaxTotalBytes <= 0 || s.ArtifactPolicy.MaxTotalBytes > MaxArtifactBytes {
		return fmt.Errorf("%w: invalid artifact policy", ErrInvalidSpec)
	}
	targets := make(map[string]struct{}, len(s.Mounts))
	for _, mount := range s.Mounts {
		if _, err := NormalizeHostPath(mount.Source); err != nil {
			return fmt.Errorf("%w: source: %v", ErrInvalidSpec, err)
		}
		target, err := NormalizeVirtualPath(mount.Target)
		if err != nil {
			return fmt.Errorf("%w: target: %v", ErrInvalidSpec, err)
		}
		if _, exists := targets[target]; exists {
			return fmt.Errorf("%w: duplicate mount target", ErrInvalidSpec)
		}
		targets[target] = struct{}{}
	}
	return ValidateLease(s.Lease)
}

func (l Limits) Validate() error {
	if l.WallTime <= 0 || l.WallTime > MaxWallTime || l.MaxOutputBytes <= 0 || l.MaxOutputBytes > MaxOutputBytes || l.MaxMemoryBytes <= 0 || l.MaxMemoryBytes > MaxMemoryBytes || l.MaxCPUTime <= 0 || l.MaxCPUTime > MaxCPUTime {
		return fmt.Errorf("%w: invalid limits", ErrInvalidSpec)
	}
	return nil
}

func ValidateLease(l LeaseIdentity) error {
	for name, value := range map[string]string{"run": l.RunID, "segment": l.SegmentID, "module": l.ModuleID, "composition": l.CompositionRev, "token": l.Token} {
		if value == "" || len(value) > MaxIdentifierBytes || !utf8.ValidString(value) || strings.TrimSpace(value) != value || containsControl(value) {
			return fmt.Errorf("%w: invalid %s lease field", ErrInvalidSpec, name)
		}
		for _, r := range value {
			if unicode.IsSpace(r) {
				return fmt.Errorf("%w: invalid %s lease field", ErrInvalidSpec, name)
			}
		}
	}
	return nil
}

func NormalizeHostPath(value string) (string, error) {
	if value == "" || !filepath.IsAbs(value) || strings.HasPrefix(value, `\\`) || strings.HasPrefix(value, "//") || strings.IndexByte(value, 0) >= 0 || containsControl(value) {
		return "", fmt.Errorf("path must be absolute")
	}
	for _, part := range strings.FieldsFunc(value, func(r rune) bool { return r == '/' || r == '\\' }) {
		if part == ".." {
			return "", fmt.Errorf("path traversal is forbidden")
		}
	}
	clean := filepath.Clean(value)
	if clean == string(filepath.Separator) || (runtime.GOOS == "windows" && filepath.VolumeName(clean) != "" && strings.HasSuffix(clean, `:\`)) {
		return "", fmt.Errorf("root mount is forbidden")
	}
	if err := rejectHostSymlinkComponents(clean); err != nil {
		return "", err
	}
	return clean, nil
}

// rejectHostSymlinkComponents prevents a mount source from changing its
// final host target through an existing symlink. The check walks existing
// components, so a not-yet-created leaf remains valid while a symlinked parent
// is still rejected. Providers that need symlink-aware mounts must resolve and
// authorize the final path in their own stronger adapter.
func rejectHostSymlinkComponents(clean string) error {
	current := clean
	for {
		info, err := os.Lstat(current)
		if err == nil {
			if info.Mode()&os.ModeSymlink != 0 {
				return fmt.Errorf("symlink mount source is forbidden")
			}
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("host path cannot be inspected: %w", err)
		}
		parent := filepath.Dir(current)
		if parent == current {
			break
		}
		current = parent
	}
	return nil
}

func NormalizeVirtualPath(value string) (string, error) {
	if value == "" || strings.HasPrefix(value, "//") || !strings.HasPrefix(value, "/") || strings.Contains(value, `\`) || strings.IndexByte(value, 0) >= 0 || containsControl(value) {
		return "", fmt.Errorf("target must be a POSIX absolute path")
	}
	for _, part := range strings.Split(value, "/") {
		if part == ".." || part == "." {
			return "", fmt.Errorf("path traversal is forbidden")
		}
	}
	clean := path.Clean(value)
	if clean == "/" {
		return "", fmt.Errorf("root mount is forbidden")
	}
	return clean, nil
}

func NormalizePath(value string) (string, error) { return NormalizeHostPath(value) }

func validArtifactKey(value string) bool {
	if strings.HasPrefix(value, "/") || strings.ContainsAny(value, `\\`) || containsControl(value) {
		return false
	}
	for _, part := range strings.Split(value, "/") {
		if part == ".." || part == "." || part == "" {
			return false
		}
	}
	return true
}

func containsControl(value string) bool {
	for _, r := range value {
		if unicode.IsControl(r) || unicode.In(r, unicode.Cf, unicode.Cs, unicode.Co) || r == 0x7f {
			return true
		}
	}
	return false
}

func ValidateCommand(command Command) error {
	if len(command.Args) == 0 || len(command.Args) > MaxArgs {
		return ErrInvalidSpec
	}
	for _, arg := range command.Args {
		if arg == "" || len(arg) > MaxArgBytes || !utf8.ValidString(arg) || strings.IndexByte(arg, 0) >= 0 {
			return ErrInvalidSpec
		}
	}
	return nil
}

func ValidateArtifact(a Artifact) error {
	if a.Key == "" || len(a.Key) > MaxArtifactKeyBytes || a.Size < 0 || a.Size > MaxArtifactBytes || !validArtifactKey(a.Key) || !a.Verified {
		return ErrArtifactUnverified
	}
	if len(a.Digest) != sha256.Size*2 {
		return ErrArtifactUnverified
	}
	if _, err := hex.DecodeString(a.Digest); err != nil {
		return ErrArtifactUnverified
	}
	return nil
}

func (s ArtifactSet) Validate(policy ArtifactPolicy) error {
	if policy.MaxArtifacts <= 0 || policy.MaxArtifacts > MaxArtifactCount || policy.MaxTotalBytes <= 0 || policy.MaxTotalBytes > MaxArtifactBytes || len(s.Items) > policy.MaxArtifacts {
		return ErrArtifactUnverified
	}
	seen := make(map[string]struct{}, len(s.Items))
	var total int64
	for _, item := range s.Items {
		if err := ValidateArtifact(item); err != nil {
			return err
		}
		if _, ok := seen[item.Key]; ok || total > policy.MaxTotalBytes-item.Size {
			return ErrArtifactUnverified
		}
		seen[item.Key] = struct{}{}
		total += item.Size
	}
	return nil
}
