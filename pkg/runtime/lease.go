package runtime

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
)

type LeaseIdentity struct {
	LeaseID             string
	RunID               string
	CompositionRevision string
	ModuleID            ModuleID
	ModuleRevision      Version
	Scope               string
	// HostGeneration fences a public lease to one durable host claim. A legacy
	// owner that returns zero is tolerated by the host for compatibility only;
	// it does not make downstream resources generation-fenced.
	HostGeneration uint64
}

type LeaseRequest struct {
	LeaseID             string
	RunID               string
	CompositionRevision string
	ModuleID            ModuleID
	ModuleRevision      Version
	Scope               string
	HostGeneration      uint64
}

type Lease struct {
	Identity LeaseIdentity
	Token    string
}

type DrainRequest struct {
	CompositionRevision string
	Force               bool
}

type DrainResult struct {
	CompositionRevision string
	Completed           bool
	RemainingLeases     int
}

type FenceRequest struct {
	RequestID           string
	CompositionRevision string
	Reason              string
	HostGeneration      uint64
}

func (lease Lease) Release(ctx context.Context, host *ModuleHost) error {
	if host == nil {
		return ErrLeaseNotFound
	}
	return host.ReleaseLease(ctx, lease)
}

func (host *ModuleHost) AcquireLease(ctx context.Context, request LeaseRequest) (Lease, error) {
	ctx = nonNilContext(ctx)
	// Lifecycle serialization currently covers acquisition as well; splitting
	// the hot path is a follow-up performance task once ownership semantics are
	// durable.
	host.opMu.Lock()
	defer host.opMu.Unlock()
	if err := host.ensureOwnership(ctx); err != nil {
		return Lease{}, err
	}
	if err := validateLeaseRequest(request); err != nil {
		return Lease{}, err
	}
	host.mu.RLock()
	active := host.active
	if active == nil || active.compositionRevision != request.CompositionRevision {
		host.mu.RUnlock()
		return Lease{}, fmt.Errorf("%w: composition %q is not active", ErrLeaseMismatch, request.CompositionRevision)
	}
	if active.state != StateActive {
		host.mu.RUnlock()
		return Lease{}, fmt.Errorf("%w: composition is draining", ErrLeaseMismatch)
	}
	owner, found := active.owners[request.ModuleID]
	manifest, manifestFound := host.manifests[request.ModuleID]
	host.mu.RUnlock()
	if !found || !manifestFound || manifest.Version != request.ModuleRevision {
		return Lease{}, fmt.Errorf("%w: module identity does not match active snapshot", ErrLeaseMismatch)
	}
	leaseID, publicToken, err := newLeaseCredentials()
	if err != nil {
		return Lease{}, err
	}
	request.LeaseID = leaseID
	request.HostGeneration = host.ownershipClaim.Generation
	ownerLease, err := safeOwnerAcquireLease(owner, ctx, request)
	if err != nil {
		return Lease{}, err
	}
	identity := LeaseIdentity{LeaseID: leaseID, RunID: request.RunID, CompositionRevision: request.CompositionRevision, ModuleID: request.ModuleID, ModuleRevision: request.ModuleRevision, Scope: request.Scope, HostGeneration: request.HostGeneration}
	ownerIdentity := ownerLease.Identity
	// Legacy owners predate the host-generation fence and therefore return a
	// zero generation. The public lease still carries the generation; newer
	// owners receive it in LeaseRequest and must echo it exactly.
	if ownerIdentity.HostGeneration == 0 {
		ownerIdentity.HostGeneration = identity.HostGeneration
	}
	if ownerIdentity != identity {
		_ = safeOwnerReleaseLease(owner, ctx, ownerLease)
		return Lease{}, fmt.Errorf("%w: owner returned a different identity", ErrLeaseMismatch)
	}
	host.mu.Lock()
	if host.active != active || active.state != StateActive {
		host.mu.Unlock()
		_ = safeOwnerReleaseLease(owner, ctx, ownerLease)
		return Lease{}, fmt.Errorf("%w: composition changed while acquiring lease", ErrLeaseMismatch)
	}
	publicLease := Lease{Identity: identity, Token: publicToken}
	active.leases[leaseID] = leaseRecord{public: publicLease, owner: ownerLease}
	host.mu.Unlock()
	return publicLease, nil
}

func (host *ModuleHost) ReleaseLease(ctx context.Context, lease Lease) error {
	ctx = nonNilContext(ctx)
	host.opMu.Lock()
	defer host.opMu.Unlock()
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := host.ensureOwnership(ctx); err != nil {
		return err
	}
	host.mu.Lock()
	composition := host.compositions[lease.Identity.CompositionRevision]
	if composition == nil {
		host.mu.Unlock()
		return ErrLeaseNotFound
	}
	stored, found := composition.leases[lease.Identity.LeaseID]
	if !found {
		host.mu.Unlock()
		return ErrLeaseNotFound
	}
	if lease.Token == "" || stored.public.Identity != lease.Identity ||
		subtle.ConstantTimeCompare([]byte(stored.public.Token), []byte(lease.Token)) != 1 {
		host.mu.Unlock()
		return ErrLeaseMismatch
	}
	host.mu.Unlock()
	composition.leaseMu.Lock()
	defer composition.leaseMu.Unlock()
	// Re-check after serializing release operations so two callers cannot
	// deliver the same owner credential twice.
	host.mu.Lock()
	current, stillPresent := composition.leases[lease.Identity.LeaseID]
	if !stillPresent {
		host.mu.Unlock()
		return ErrLeaseNotFound
	}
	if current.public.Identity != lease.Identity || subtle.ConstantTimeCompare([]byte(current.public.Token), []byte(lease.Token)) != 1 {
		host.mu.Unlock()
		return ErrLeaseMismatch
	}
	owner := composition.owners[lease.Identity.ModuleID]
	host.mu.Unlock()
	if owner != nil {
		if err := safeOwnerReleaseLease(owner, ctx, current.owner); err != nil {
			return err
		}
	}
	host.mu.Lock()
	delete(composition.leases, lease.Identity.LeaseID)
	host.mu.Unlock()
	return nil
}

func newLeaseCredentials() (string, string, error) {
	var entropy [32]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return "", "", fmt.Errorf("%w: lease credential entropy: %v", ErrInvalidHost, err)
	}
	return "lease-" + hex.EncodeToString(entropy[:16]), "tok-" + hex.EncodeToString(entropy[16:]), nil
}
