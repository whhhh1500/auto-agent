package runtime

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"
)

type ModuleHost struct {
	opMu sync.Mutex
	mu   sync.RWMutex

	modules             map[ModuleID]Module
	manifests           map[ModuleID]ModuleManifest
	apiVersion          Version
	journal             EffectJournal
	inverse             InverseExecutor
	compositionStore    CompositionStore
	ownershipStore      HostOwnershipStore
	ownershipHolder     string
	ownershipClaim      HostOwnershipClaim
	ownershipRenewAfter time.Time
	ownershipReleased   bool
	fenceAuthorizer     FenceAuthorizer
	fenceJournal        FenceJournal
	durableState        DurableHostState
	active              *moduleComposition
	compositions        map[string]*moduleComposition
	states              map[ModuleID]LifecycleState
}

type moduleComposition struct {
	snapshot            Snapshot
	compositionRevision string
	owners              map[ModuleID]ModuleLeaseOwner
	leases              map[string]leaseRecord
	state               LifecycleState
	moduleOrder         []ModuleID
	inherited           []RecordedEffect
	superseded          *moduleComposition
	retainedIDs         map[ModuleID]struct{}
	leaseMu             sync.Mutex
}

// leaseRecord keeps the host-visible credential separate from the opaque
// credential issued by the module owner. The latter must be returned to the
// owner verbatim during release.
type leaseRecord struct {
	public Lease
	owner  Lease
}

func NewModuleHost(apiVersion Version, modules []Module, journal EffectJournal, inverse InverseExecutor) (*ModuleHost, error) {
	return NewModuleHostWithControls(apiVersion, modules, journal, inverse, HostControls{})
}

// NewModuleHostWithControls fixes authorization and audit controls before any
// module lifecycle work begins. The legacy constructor has nil controls and
// therefore fails closed for fencing.
func NewModuleHostWithControls(apiVersion Version, modules []Module, journal EffectJournal, inverse InverseExecutor, controls HostControls) (*ModuleHost, error) {
	return newUnpersistedHostWithControls(apiVersion, modules, journal, inverse, controls)
}

func (host *ModuleHost) Snapshot() (Snapshot, bool) {
	host.mu.RLock()
	defer host.mu.RUnlock()
	if host.active == nil {
		return Snapshot{}, false
	}
	return host.active.snapshot, true
}

func (host *ModuleHost) ActiveCompositionRevision() (string, bool) {
	host.mu.RLock()
	defer host.mu.RUnlock()
	if host.active == nil {
		return "", false
	}
	return host.active.compositionRevision, true
}

func (host *ModuleHost) nextCompositionRevision(snapshotRevision string) (string, error) {
	var entropy [16]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return "", fmt.Errorf("%w: composition revision entropy: %v", ErrInvalidHost, err)
	}
	return "comp-" + hex.EncodeToString(entropy[:]) + "-" + snapshotRevision, nil
}

func compositionRevisionOf(composition *moduleComposition) string {
	if composition == nil {
		return ""
	}
	return composition.compositionRevision
}

func (host *ModuleHost) State(id ModuleID) (LifecycleState, bool) {
	host.mu.RLock()
	defer host.mu.RUnlock()
	state, found := host.states[id]
	return state, found
}

func (host *ModuleHost) rollback(ctx context.Context, revision string, order []ModuleID, owners map[ModuleID]ModuleLeaseOwner) error {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var joined error
	records, err := safeEffectList(host.journal, cleanupCtx, revision)
	if err != nil {
		joined = errors.Join(joined, err)
	}
	if err == nil {
		records, err = orderedEffects(records)
		if err != nil {
			joined = errors.Join(joined, err)
		}
	}
	for index := len(records) - 1; index >= 0; index-- {
		record := records[index]
		if record.State == EffectReverted {
			continue
		}
		if inverseErr := safeInverse(host.inverse, cleanupCtx, record); inverseErr != nil {
			joined = errors.Join(joined, inverseErr)
			continue
		}
		joined = errors.Join(joined, safeMarkReverted(host.journal, cleanupCtx, record.Descriptor.ID))
	}
	for index := len(order) - 1; index >= 0; index-- {
		if owner := owners[order[index]]; owner != nil {
			joined = errors.Join(joined, safeOwnerDeactivate(owner, cleanupCtx))
		}
	}
	return joined
}
