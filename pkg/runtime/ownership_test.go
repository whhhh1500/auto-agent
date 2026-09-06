package runtime

import (
	"context"
	"errors"
	"testing"
	"time"
)

type countingOwnershipStore struct {
	*MemoryCompositionStore
	renewCalls int
	panicRenew bool
}

type blockingFenceOwner struct {
	ModuleLeaseOwner
	entered chan<- struct{}
	release <-chan struct{}
}

type panicManifestModule struct{}

func (panicManifestModule) Manifest() ModuleManifest { panic("manifest") }
func (panicManifestModule) Stage(context.Context, StageContext) (StagedModule, error) {
	return nil, errors.New("unreachable")
}

func (owner blockingFenceOwner) Fence(ctx context.Context, request FenceRequest) error {
	owner.entered <- struct{}{}
	<-owner.release
	return errors.New("fence interrupted after durable block")
}

func (store *countingOwnershipStore) RenewHostOwnership(ctx context.Context, claim HostOwnershipClaim, ttl time.Duration) (HostOwnershipClaim, error) {
	store.renewCalls++
	if store.panicRenew {
		panic("ownership renew")
	}
	return store.MemoryCompositionStore.RenewHostOwnership(ctx, claim, ttl)
}

func TestMemoryHostOwnershipGenerationIsMonotonicAcrossReleaseAndExpiry(t *testing.T) {
	store := NewMemoryCompositionStore()
	first, err := store.ClaimHostOwnership(context.Background(), "host-first", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ReleaseHostOwnership(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if err := store.ReleaseHostOwnership(context.Background(), first); err != nil {
		t.Fatalf("idempotent release error=%v", err)
	}
	second, err := store.ClaimHostOwnership(context.Background(), "host-second", time.Second)
	if err != nil || second.Generation != first.Generation+1 {
		t.Fatalf("released claim=%+v err=%v", second, err)
	}
	store.mu.Lock()
	store.claim.ExpiresAt = time.Now().UTC().Add(-time.Second)
	store.mu.Unlock()
	third, err := store.ClaimHostOwnership(context.Background(), "host-third", time.Second)
	if err != nil || third.Generation != second.Generation+1 {
		t.Fatalf("expired claim=%+v err=%v", third, err)
	}
	if _, err := store.RenewHostOwnership(context.Background(), second, time.Second); !errors.Is(err, ErrHostOwnershipLost) {
		t.Fatalf("stale generation renew error=%v", err)
	}
}

func TestDurableOwnershipRejectsLiveRecoverAndFencesReleasedHost(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryCompositionStore()
	journal := NewMemoryEffectJournal()
	firstModule := newRecoverableModule("ownership-recover")
	first, err := OpenModuleHost(ctx, Version{Major: 1}, []Module{firstModule}, journal, &inverseRecorder{}, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Activate(ctx); err != nil {
		t.Fatal(err)
	}
	revision, _ := first.ActiveCompositionRevision()
	if _, err := RecoverModuleHost(ctx, Version{Major: 1}, []Module{newRecoverableModule("ownership-recover")}, journal, &inverseRecorder{}, store); !errors.Is(err, ErrRecoveryBlocked) {
		t.Fatalf("live host recovery error=%v", err)
	}
	if err := first.Close(ctx); err != nil {
		t.Fatal(err)
	}
	secondModule := newRecoverableModule("ownership-recover")
	second, err := RecoverModuleHost(ctx, Version{Major: 1}, []Module{secondModule}, journal, &inverseRecorder{}, store)
	if err != nil {
		t.Fatal(err)
	}
	if second.ownershipClaim.Generation != 2 {
		t.Fatalf("recovered generation=%d", second.ownershipClaim.Generation)
	}
	if _, err := first.AcquireLease(ctx, LeaseRequest{RunID: "stale", CompositionRevision: revision, ModuleID: firstModule.manifest.ID, ModuleRevision: firstModule.manifest.Version, Scope: "tenant/a"}); !errors.Is(err, ErrHostOwnershipLost) {
		t.Fatalf("released host lease error=%v", err)
	}
	if _, err := second.AcquireLease(ctx, LeaseRequest{RunID: "fresh", CompositionRevision: revision, ModuleID: secondModule.manifest.ID, ModuleRevision: secondModule.manifest.Version, Scope: "tenant/a"}); err != nil {
		t.Fatalf("fresh host lease error=%v", err)
	}
}

func TestOwnershipRenewPanicFailsClosedAndReleasesHostLock(t *testing.T) {
	ctx := context.Background()
	store := &countingOwnershipStore{MemoryCompositionStore: NewMemoryCompositionStore()}
	module := moduleForHost("ownership-panic", nil)
	host, err := OpenModuleHost(ctx, Version{Major: 1}, []Module{module}, NewMemoryEffectJournal(), &inverseRecorder{}, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Activate(ctx); err != nil {
		t.Fatal(err)
	}
	revision, _ := host.ActiveCompositionRevision()
	store.panicRenew = true
	if _, err := host.Drain(ctx, revision); err == nil {
		t.Fatal("ownership panic unexpectedly drained")
	}
	store.panicRenew = false
	if _, err := host.Drain(ctx, revision); err != nil {
		t.Fatalf("host lock remained held after ownership panic: %v", err)
	}
}

func TestOwnershipLeaseFastPathDoesNotRenewPerLease(t *testing.T) {
	ctx := context.Background()
	store := &countingOwnershipStore{MemoryCompositionStore: NewMemoryCompositionStore()}
	module := moduleForHost("ownership-fast-lease", nil)
	host, err := OpenModuleHost(ctx, Version{Major: 1}, []Module{module}, NewMemoryEffectJournal(), &inverseRecorder{}, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Activate(ctx); err != nil {
		t.Fatal(err)
	}
	revision, _ := host.ActiveCompositionRevision()
	before := store.renewCalls
	for index := 0; index < 500; index++ {
		lease, err := host.AcquireLease(ctx, LeaseRequest{RunID: "fast-run", CompositionRevision: revision, ModuleID: module.manifest.ID, ModuleRevision: module.manifest.Version, Scope: "tenant/a"})
		if err != nil {
			t.Fatal(err)
		}
		if err := host.ReleaseLease(ctx, lease); err != nil {
			t.Fatal(err)
		}
	}
	if got := store.renewCalls - before; got != 0 {
		t.Fatalf("lease fast path renewed ownership %d times", got)
	}
}

func TestFenceBlockPreventsConcurrentRecoverFromUsingStaleActiveState(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryCompositionStore()
	journal := newTestFenceJournal()
	initial := newRecoverableModule("ownership-fence-recover")
	host, err := OpenModuleHostWithControls(ctx, Version{Major: 1}, []Module{initial}, NewMemoryEffectJournal(), &inverseRecorder{}, store, HostControls{
		FenceAuthorizer: fenceAuthorizerFunc(allowFence), FenceJournal: journal,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Activate(ctx); err != nil {
		t.Fatal(err)
	}
	revision, _ := host.ActiveCompositionRevision()
	composition, err := host.composition(revision)
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	composition.owners[initial.manifest.ID] = blockingFenceOwner{ModuleLeaseOwner: initial.lastOwner, entered: entered, release: release}
	fenceDone := make(chan error, 1)
	go func() {
		_, err := host.FenceAuthorized(ctx, fenceCommand(revision, "ownership-fence-recover"))
		fenceDone <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("fence did not reach owner after durable block")
	}
	restarted := newRecoverableModule("ownership-fence-recover")
	if _, err := RecoverModuleHost(ctx, Version{Major: 1}, []Module{restarted}, NewMemoryEffectJournal(), &inverseRecorder{}, store); !errors.Is(err, ErrRecoveryBlocked) {
		t.Fatalf("concurrent recover error=%v", err)
	}
	_, recoverCalls, _ := restarted.counts()
	if recoverCalls != 0 {
		t.Fatalf("stale recover constructed owner %d times", recoverCalls)
	}
	close(release)
	if err := <-fenceDone; err == nil {
		t.Fatal("interrupted fence unexpectedly completed")
	}
	state, found, err := store.Load(ctx)
	if err != nil || !found || len(state.Compositions) != 1 || state.Compositions[0].Status != DurableCompositionBlocked {
		t.Fatalf("fence durable state=%#v found=%v err=%v", state.Compositions, found, err)
	}
}

func TestDurableLifecyclePanicsDoNotPublishAndRecordedActivateEffectRollsBack(t *testing.T) {
	for _, test := range []struct {
		name    string
		set     func(*hostModule)
		inverse int
	}{
		{name: "stage", set: func(module *hostModule) { module.stagePanic = true }},
		{name: "health", set: func(module *hostModule) { module.healthPanic = true }},
		{name: "activate", set: func(module *hostModule) { module.effects = 1; module.activatePanic = true }, inverse: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := NewMemoryCompositionStore()
			inverse := &inverseRecorder{}
			module := moduleForHost(ModuleID("ownership-panic-"+test.name), nil)
			test.set(module)
			host, err := OpenModuleHost(context.Background(), Version{Major: 1}, []Module{module}, NewMemoryEffectJournal(), inverse, store)
			if err != nil {
				t.Fatal(err)
			}
			if err := host.Activate(context.Background()); err == nil {
				t.Fatal("panic lifecycle unexpectedly activated")
			}
			if _, found := host.ActiveCompositionRevision(); found {
				t.Fatal("panic lifecycle published active composition")
			}
			inverse.mu.Lock()
			got := len(inverse.ids)
			inverse.mu.Unlock()
			if got != test.inverse {
				t.Fatalf("inverse count=%d want %d", got, test.inverse)
			}
			state, found, err := store.Load(context.Background())
			if err != nil || !found || len(state.Compositions) != 0 {
				t.Fatalf("panic checkpoint=%#v found=%v err=%v", state.Compositions, found, err)
			}
			if err := host.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			if _, err := OpenModuleHost(context.Background(), Version{Major: 1}, []Module{moduleForHost(ModuleID("ownership-panic-"+test.name), nil)}, NewMemoryEffectJournal(), &inverseRecorder{}, store); err != nil {
				t.Fatalf("fresh open after cleaned panic checkpoint: %v", err)
			}
		})
	}
}

func TestManifestPanicFailsClosedWithoutDurableAdvance(t *testing.T) {
	panicModule := panicManifestModule{}
	if _, err := NewModuleHost(Version{Major: 1}, []Module{panicModule}, NewMemoryEffectJournal(), &inverseRecorder{}); !errors.Is(err, ErrInvalidHost) {
		t.Fatalf("new host manifest panic error=%v", err)
	}
	store := NewMemoryCompositionStore()
	if _, err := OpenModuleHost(context.Background(), Version{Major: 1}, []Module{panicModule}, NewMemoryEffectJournal(), &inverseRecorder{}, store); !errors.Is(err, ErrInvalidHost) {
		t.Fatalf("open manifest panic error=%v", err)
	}
	if _, err := RecoverModuleHost(context.Background(), Version{Major: 1}, []Module{panicModule}, NewMemoryEffectJournal(), &inverseRecorder{}, store); !errors.Is(err, ErrInvalidHost) {
		t.Fatalf("recover manifest panic error=%v", err)
	}
	module := moduleForHost("manifest-apply", nil)
	host, err := OpenModuleHost(context.Background(), Version{Major: 1}, []Module{module}, NewMemoryEffectJournal(), &inverseRecorder{}, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := host.ApplyModules(context.Background(), []Module{panicModule}); !errors.Is(err, ErrInvalidHost) {
		t.Fatalf("apply manifest panic error=%v", err)
	}
	state, found, err := store.Load(context.Background())
	if err != nil || !found || len(state.Compositions) != 0 {
		t.Fatalf("manifest panic durable state=%#v found=%v err=%v", state.Compositions, found, err)
	}
}
