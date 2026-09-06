package runtime

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// MemoryCompositionStore is a concurrency-safe in-memory implementation for
// tests and process-local hosts. It has no background goroutines.
type MemoryCompositionStore struct {
	mu    sync.RWMutex
	state DurableHostState
	found bool
	claim HostOwnershipClaim
}

var _ CompositionStore = (*MemoryCompositionStore)(nil)
var _ HostOwnershipStore = (*MemoryCompositionStore)(nil)

func NewMemoryCompositionStore() *MemoryCompositionStore { return &MemoryCompositionStore{} }

func (store *MemoryCompositionStore) Load(ctx context.Context) (DurableHostState, bool, error) {
	ctx = compositionContext(ctx)
	if err := ctx.Err(); err != nil {
		return DurableHostState{}, false, err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return DurableHostState{}, false, err
	}
	if !store.found {
		return DurableHostState{}, false, nil
	}
	return store.state.Clone(), true, nil
}

func (store *MemoryCompositionStore) CompareAndSwap(ctx context.Context, expectedRevision string, next DurableHostState) error {
	ctx = compositionContext(ctx)
	if err := ctx.Err(); err != nil {
		return err
	}
	canonical, err := canonicalDurableHostState(next)
	if err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if !store.found {
		if expectedRevision != "" {
			return fmt.Errorf("%w: expected %q but state is absent", ErrCompositionConflict, expectedRevision)
		}
	} else {
		if expectedRevision == "" || expectedRevision != store.state.StoreRevision {
			return fmt.Errorf("%w: expected %q, current %q", ErrCompositionConflict, expectedRevision, store.state.StoreRevision)
		}
		if canonical.StoreRevision == store.state.StoreRevision {
			return fmt.Errorf("%w: next revision %q is not a new revision", ErrCompositionConflict, canonical.StoreRevision)
		}
	}
	store.state = canonical
	store.found = true
	return nil
}

func compositionContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func (store *MemoryCompositionStore) ClaimHostOwnership(ctx context.Context, holder string, ttl time.Duration) (HostOwnershipClaim, error) {
	ctx = compositionContext(ctx)
	if err := ctx.Err(); err != nil {
		return HostOwnershipClaim{}, err
	}
	if err := validateHostOwnershipHolder(holder); err != nil || ttl <= 0 || ttl > MaxHostOwnershipTTL {
		return HostOwnershipClaim{}, ErrHostOwnershipLost
	}
	now := time.Now().UTC()
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return HostOwnershipClaim{}, err
	}
	if store.claim.Holder != "" && now.Before(store.claim.ExpiresAt) {
		return HostOwnershipClaim{}, ErrHostOwnershipConflict
	}
	store.claim.Holder = holder
	store.claim.Generation++
	store.claim.ExpiresAt = now.Add(ttl)
	return store.claim, nil
}

func (store *MemoryCompositionStore) RenewHostOwnership(ctx context.Context, claim HostOwnershipClaim, ttl time.Duration) (HostOwnershipClaim, error) {
	ctx = compositionContext(ctx)
	if err := ctx.Err(); err != nil {
		return HostOwnershipClaim{}, err
	}
	if !validHostOwnershipClaim(claim) || ttl <= 0 || ttl > MaxHostOwnershipTTL {
		return HostOwnershipClaim{}, ErrHostOwnershipLost
	}
	now := time.Now().UTC()
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return HostOwnershipClaim{}, err
	}
	if store.claim.Holder != claim.Holder || store.claim.Generation != claim.Generation || !store.claim.ExpiresAt.Equal(claim.ExpiresAt) || !now.Before(store.claim.ExpiresAt) {
		return HostOwnershipClaim{}, ErrHostOwnershipLost
	}
	store.claim.ExpiresAt = now.Add(ttl)
	return store.claim, nil
}

func (store *MemoryCompositionStore) ValidateHostOwnership(ctx context.Context, claim HostOwnershipClaim) error {
	ctx = compositionContext(ctx)
	if err := ctx.Err(); err != nil {
		return err
	}
	now := time.Now().UTC()
	store.mu.RLock()
	defer store.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if !validHostOwnershipClaim(claim) || store.claim.Holder != claim.Holder || store.claim.Generation != claim.Generation || !store.claim.ExpiresAt.Equal(claim.ExpiresAt) || !now.Before(store.claim.ExpiresAt) {
		return ErrHostOwnershipLost
	}
	return nil
}

func (store *MemoryCompositionStore) ReleaseHostOwnership(ctx context.Context, claim HostOwnershipClaim) error {
	ctx = compositionContext(ctx)
	if err := ctx.Err(); err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if !validHostOwnershipClaim(claim) {
		return ErrHostOwnershipLost
	}
	if store.claim.Holder == "" {
		return nil
	}
	if store.claim.Holder != claim.Holder || store.claim.Generation != claim.Generation || !store.claim.ExpiresAt.Equal(claim.ExpiresAt) {
		return ErrHostOwnershipLost
	}
	store.claim.Holder = ""
	store.claim.ExpiresAt = time.Time{}
	return nil
}

func validateHostOwnershipHolder(holder string) error {
	return validateID(holder, "host holder")
}

func validHostOwnershipClaim(claim HostOwnershipClaim) bool {
	return validateHostOwnershipHolder(claim.Holder) == nil && claim.Generation != 0 && !claim.ExpiresAt.IsZero() && claim.ExpiresAt.Location() == time.UTC
}
