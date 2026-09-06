package runtime

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

// HostOwnershipTTL bounds how long a stopped or crashed process can exclude a
// successor. The runtime has no renewal goroutine: each mutating or
// owner-calling operation renews immediately before its first external call.
// Calls that run beyond this TTL cannot provide external exactly-once effects.
// External isolation ultimately requires each owner to honor HostGeneration;
// accepting a legacy zero generation preserves compatibility only.
const HostOwnershipTTL = 30 * time.Second
const MaxHostOwnershipTTL = 5 * time.Minute

var (
	ErrHostOwnershipConflict = errors.New("host ownership is held")
	ErrHostOwnershipLost     = errors.New("host ownership is lost")
)

// HostOwnershipClaim is an opaque, generation-fenced host lease. Holder is
// never exposed through a public lease token; Generation is propagated to
// owners so they can reject work from a stale host where supported.
type HostOwnershipClaim struct {
	Holder     string
	Generation uint64
	ExpiresAt  time.Time
}

// HostOwnershipStore is separate from CompositionStore so durable state stays
// a small document. Claim atomically acquires an absent or expired lease;
// Renew and Release require the exact holder and generation.
type HostOwnershipStore interface {
	ClaimHostOwnership(context.Context, string, time.Duration) (HostOwnershipClaim, error)
	RenewHostOwnership(context.Context, HostOwnershipClaim, time.Duration) (HostOwnershipClaim, error)
	ValidateHostOwnership(context.Context, HostOwnershipClaim) error
	ReleaseHostOwnership(context.Context, HostOwnershipClaim) error
}

func newHostHolder() (string, error) {
	var entropy [16]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return "", fmt.Errorf("%w: host ownership entropy: %v", ErrInvalidHost, err)
	}
	return "host-" + hex.EncodeToString(entropy[:]), nil
}

func ownershipStore(store CompositionStore) (HostOwnershipStore, error) {
	owners, ok := store.(HostOwnershipStore)
	if !ok || owners == nil {
		return nil, fmt.Errorf("%w: composition store does not implement host ownership", ErrRecoveryBlocked)
	}
	return owners, nil
}

func (host *ModuleHost) claimOwnership(ctx context.Context) error {
	if host.ownershipStore == nil || host.ownershipHolder == "" {
		return fmt.Errorf("%w: host ownership is unavailable", ErrHostOwnershipLost)
	}
	claim, err := safeClaimHostOwnership(host.ownershipStore, ctx, host.ownershipHolder, HostOwnershipTTL)
	if err != nil {
		return err
	}
	host.ownershipClaim = claim
	host.ownershipRenewAfter = time.Now().UTC().Add(HostOwnershipTTL / 3)
	return nil
}

func (host *ModuleHost) ensureOwnership(ctx context.Context) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if host.ownershipStore == nil {
		return nil
	}
	if host.ownershipClaim.Holder == "" || host.ownershipReleased {
		return ErrHostOwnershipLost
	}
	if time.Now().UTC().Before(host.ownershipRenewAfter) {
		// A claim cannot be taken over before expiry. This avoids a database
		// round trip per lease while retaining a conservative TTL/3 renewal.
		return nil
	}
	claim, err := safeRenewHostOwnership(host.ownershipStore, ctx, host.ownershipClaim, HostOwnershipTTL)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrHostOwnershipLost, err)
	}
	host.ownershipClaim = claim
	host.ownershipRenewAfter = time.Now().UTC().Add(HostOwnershipTTL / 3)
	return nil
}

func (host *ModuleHost) forceRenewOwnership(ctx context.Context) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if host.ownershipStore == nil {
		return nil
	}
	if host.ownershipClaim.Holder == "" || host.ownershipReleased {
		return ErrHostOwnershipLost
	}
	claim, err := safeRenewHostOwnership(host.ownershipStore, ctx, host.ownershipClaim, HostOwnershipTTL)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrHostOwnershipLost, err)
	}
	host.ownershipClaim = claim
	host.ownershipRenewAfter = time.Now().UTC().Add(HostOwnershipTTL / 3)
	return nil
}

// ReleaseOwnership relinquishes this host's durable claim. It is idempotent;
// after release every owner-calling operation fails closed on this host.
func (host *ModuleHost) ReleaseOwnership(ctx context.Context) error {
	ctx = nonNilContext(ctx)
	host.opMu.Lock()
	defer host.opMu.Unlock()
	return host.releaseOwnershipLocked(ctx)
}

// Close is an ownership-only close. It never approximates module cleanup.
func (host *ModuleHost) Close(ctx context.Context) error { return host.ReleaseOwnership(ctx) }

func (host *ModuleHost) releaseOwnershipLocked(ctx context.Context) error {
	if host.ownershipStore == nil || host.ownershipClaim.Holder == "" {
		return nil
	}
	claim := host.ownershipClaim
	if err := safeReleaseHostOwnership(host.ownershipStore, ctx, claim); err != nil && !errors.Is(err, ErrHostOwnershipLost) {
		return err
	}
	host.ownershipClaim = HostOwnershipClaim{}
	host.ownershipRenewAfter = time.Time{}
	host.ownershipReleased = true
	return nil
}
