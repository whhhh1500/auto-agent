package runtime

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type failingCompositionStore struct {
	mu           sync.Mutex
	state        DurableHostState
	found        bool
	failNext     bool
	conflictNext bool
	failCall     int
	calls        int
	panicNextCAS bool
	ownership    *MemoryCompositionStore
}

type failingInverse struct{ err error }

func (inverse *failingInverse) ExecuteInverse(context.Context, RecordedEffect) error {
	return inverse.err
}

type failingMarkJournal struct {
	inner    *MemoryEffectJournal
	err      error
	listErr  error
	failOnce bool
}

func (journal *failingMarkJournal) Record(ctx context.Context, descriptor EffectDescriptor) (RecordedEffect, error) {
	return journal.inner.Record(ctx, descriptor)
}
func (journal *failingMarkJournal) MarkApplied(ctx context.Context, id EffectID) error {
	return journal.inner.MarkApplied(ctx, id)
}
func (journal *failingMarkJournal) MarkUnknown(ctx context.Context, id EffectID) error {
	return journal.inner.MarkUnknown(ctx, id)
}
func (journal *failingMarkJournal) MarkReverted(ctx context.Context, id EffectID) error {
	if journal.failOnce {
		journal.failOnce = false
		err := journal.err
		journal.err = nil
		return err
	}
	if journal.err != nil {
		return journal.err
	}
	return journal.inner.MarkReverted(ctx, id)
}
func (journal *failingMarkJournal) List(ctx context.Context, revision string) ([]RecordedEffect, error) {
	if journal.listErr != nil {
		return nil, journal.listErr
	}
	return journal.inner.List(ctx, revision)
}

func (store *failingCompositionStore) Load(ctx context.Context) (DurableHostState, bool, error) {
	if err := ctx.Err(); err != nil {
		return DurableHostState{}, false, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if !store.found {
		return DurableHostState{}, false, nil
	}
	return store.state.Clone(), true, nil
}

func (store *failingCompositionStore) CompareAndSwap(ctx context.Context, expected string, next DurableHostState) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.calls++
	if store.panicNextCAS {
		store.panicNextCAS = false
		panic("injected composition CAS panic")
	}
	if store.conflictNext {
		store.conflictNext = false
		return ErrCompositionConflict
	}
	if store.failNext || (store.failCall != 0 && store.calls == store.failCall) {
		store.failNext = false
		store.failCall = 0
		return errors.New("injected composition store failure")
	}
	if store.found {
		if expected == "" || expected != store.state.StoreRevision {
			return ErrCompositionConflict
		}
	} else if expected != "" {
		return ErrCompositionConflict
	}
	store.state = next.Clone()
	store.found = true
	return nil
}

func (store *failingCompositionStore) ownershipPort() *MemoryCompositionStore {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.ownership == nil {
		store.ownership = NewMemoryCompositionStore()
	}
	return store.ownership
}

func (store *failingCompositionStore) ClaimHostOwnership(ctx context.Context, holder string, ttl time.Duration) (HostOwnershipClaim, error) {
	return store.ownershipPort().ClaimHostOwnership(ctx, holder, ttl)
}

func (store *failingCompositionStore) RenewHostOwnership(ctx context.Context, claim HostOwnershipClaim, ttl time.Duration) (HostOwnershipClaim, error) {
	return store.ownershipPort().RenewHostOwnership(ctx, claim, ttl)
}

func (store *failingCompositionStore) ValidateHostOwnership(ctx context.Context, claim HostOwnershipClaim) error {
	return store.ownershipPort().ValidateHostOwnership(ctx, claim)
}

func (store *failingCompositionStore) ReleaseHostOwnership(ctx context.Context, claim HostOwnershipClaim) error {
	return store.ownershipPort().ReleaseHostOwnership(ctx, claim)
}

func (store *failingCompositionStore) setFailNext() {
	store.mu.Lock()
	store.failNext = true
	store.mu.Unlock()
}

func (store *failingCompositionStore) snapshot() DurableHostState {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.state.Clone()
}

func openTestHost(t *testing.T, store CompositionStore, module *hostModule) *ModuleHost {
	t.Helper()
	host, err := OpenModuleHost(context.Background(), Version{Major: 1}, []Module{module}, NewMemoryEffectJournal(), &inverseRecorder{}, store)
	if err != nil {
		t.Fatal(err)
	}
	return host
}

func TestOpenModuleHostBootstrapsWithoutActivating(t *testing.T) {
	store := NewMemoryCompositionStore()
	module := moduleForHost("durable-bootstrap", nil)
	host := openTestHost(t, store, module)
	if _, found := host.ActiveCompositionRevision(); found {
		t.Fatal("opening a new durable host activated a composition")
	}
	state, found, err := store.Load(context.Background())
	if err != nil || !found || len(state.Compositions) != 0 || len(state.Desired) != 1 {
		t.Fatalf("state=%#v found=%v err=%v", state, found, err)
	}
	if got, _ := host.State(module.manifest.ID); got != StateConstructed {
		t.Fatalf("initial state=%s", got)
	}
}

func TestOpenModuleHostRejectsRecoveryAndBindingMismatch(t *testing.T) {
	module := moduleForHost("durable-recovery", nil)
	store := NewMemoryCompositionStore()
	host := openTestHost(t, store, module)
	if err := host.ApplyModules(context.Background(), []Module{module}); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenModuleHost(context.Background(), Version{Major: 1}, []Module{module}, NewMemoryEffectJournal(), &inverseRecorder{}, store); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("recovery error=%v", err)
	}
	other := moduleForHost("different", nil)
	if _, err := OpenModuleHost(context.Background(), Version{Major: 1}, []Module{other}, NewMemoryEffectJournal(), &inverseRecorder{}, store); !errors.Is(err, ErrRecoveryBlocked) {
		t.Fatalf("binding error=%v", err)
	}
}

func TestDurablePreparedFailureDoesNotStageAndIsCleared(t *testing.T) {
	store := NewMemoryCompositionStore()
	var log []string
	module := moduleForHost("durable-prepared-failure", &log)
	host := openTestHost(t, store, module)
	storeState, _, _ := store.Load(context.Background())
	storeState.StoreRevision = "wrong"
	// The store is made to reject the prepared CAS by using a competing valid
	// state. No module callback may run before that CAS succeeds.
	current, _, _ := store.Load(context.Background())
	current.StoreRevision = "store-competitor"
	if err := store.CompareAndSwap(context.Background(), host.durableState.StoreRevision, current); err != nil {
		t.Fatal(err)
	}
	if err := host.ApplyModules(context.Background(), []Module{module}); !errors.Is(err, ErrCompositionConflict) {
		t.Fatalf("apply error=%v", err)
	}
	if len(log) != 0 {
		t.Fatalf("callbacks ran before prepared CAS: %v", log)
	}
}

func TestDurableActivationFailureClearsPreparedAndKeepsOld(t *testing.T) {
	store := NewMemoryCompositionStore()
	module := moduleForHost("durable-activation-failure", nil)
	module.activateErr = errors.New("activate failed")
	host := openTestHost(t, store, module)
	if err := host.ApplyModules(context.Background(), []Module{module}); err == nil {
		t.Fatal("activation unexpectedly succeeded")
	}
	state, _, _ := store.Load(context.Background())
	if len(state.Compositions) != 0 {
		t.Fatalf("prepared checkpoint leaked: %#v", state.Compositions)
	}
	if _, found := host.ActiveCompositionRevision(); found {
		t.Fatal("failed activation published active composition")
	}
}

func TestDurableRollbackFailureRetainsPreparedCheckpoint(t *testing.T) {
	tests := []struct {
		name      string
		makeHost  func(*testing.T, *failingCompositionStore, *hostModule) *ModuleHost
		configure func(*hostModule)
	}{
		{
			name: "inverse",
			makeHost: func(t *testing.T, store *failingCompositionStore, module *hostModule) *ModuleHost {
				host, err := OpenModuleHost(context.Background(), Version{Major: 1}, []Module{module}, NewMemoryEffectJournal(), &failingInverse{err: errors.New("inverse failed")}, store)
				if err != nil {
					t.Fatal(err)
				}
				return host
			},
		},
		{
			name: "mark-reverted",
			makeHost: func(t *testing.T, store *failingCompositionStore, module *hostModule) *ModuleHost {
				journal := &failingMarkJournal{inner: NewMemoryEffectJournal(), err: errors.New("mark reverted failed")}
				host, err := OpenModuleHost(context.Background(), Version{Major: 1}, []Module{module}, journal, &inverseRecorder{}, store)
				if err != nil {
					t.Fatal(err)
				}
				return host
			},
		},
		{
			name: "owner-deactivate",
			makeHost: func(t *testing.T, store *failingCompositionStore, module *hostModule) *ModuleHost {
				host, err := OpenModuleHost(context.Background(), Version{Major: 1}, []Module{module}, NewMemoryEffectJournal(), &inverseRecorder{}, store)
				if err != nil {
					t.Fatal(err)
				}
				return host
			},
			configure: func(module *hostModule) { module.deactivateErr = errors.New("owner deactivate failed") },
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &failingCompositionStore{}
			module := moduleForHost(ModuleID("durable-rollback-"+test.name), nil)
			module.effects = 1
			module.activateErr = errors.New("activation failed after effect")
			if test.configure != nil {
				test.configure(module)
			}
			host := test.makeHost(t, store, module)
			if err := host.ApplyModules(context.Background(), []Module{module}); err == nil {
				t.Fatal("activation unexpectedly succeeded")
			}
			state := store.snapshot()
			if len(state.Compositions) != 1 || state.Compositions[0].Status != DurableCompositionPrepared {
				t.Fatalf("rollback failure lost prepared checkpoint: %#v", state.Compositions)
			}
			if _, found := host.ActiveCompositionRevision(); found {
				t.Fatal("failed candidate was published")
			}
		})
	}
}

func TestDurableActivationFailurePreservesOldLeaseAndCheckpoint(t *testing.T) {
	store := NewMemoryCompositionStore()
	first := moduleForHost("durable-lease-old", nil)
	host := openTestHost(t, store, first)
	if err := host.ApplyModules(context.Background(), []Module{first}); err != nil {
		t.Fatal(err)
	}
	oldRevision, _ := host.ActiveCompositionRevision()
	lease, err := host.AcquireLease(context.Background(), LeaseRequest{RunID: "run-old", CompositionRevision: oldRevision, ModuleID: first.manifest.ID, ModuleRevision: first.manifest.Version, Scope: "tenant/a"})
	if err != nil {
		t.Fatal(err)
	}
	second := moduleForHost("durable-lease-new", nil)
	second.activateErr = errors.New("candidate activation failed")
	if err := host.ApplyModules(context.Background(), []Module{first, second}); err == nil {
		t.Fatal("candidate unexpectedly succeeded")
	}
	if current, _ := host.ActiveCompositionRevision(); current != oldRevision {
		t.Fatalf("old active revision changed to %q", current)
	}
	state, _, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Compositions) != 1 || state.Compositions[0].Status != DurableCompositionActive {
		t.Fatalf("checkpoint after failure=%#v", state.Compositions)
	}
	if err := host.ReleaseLease(context.Background(), lease); err != nil {
		t.Fatal(err)
	}
	if first.lastOwner == nil || first.lastOwner.lastRelease.Token != lease.Identity.LeaseID || lease.Token == lease.Identity.LeaseID {
		t.Fatalf("owner did not receive its opaque token: %#v", first.lastOwner)
	}
	if err := host.ReleaseLease(context.Background(), lease); !errors.Is(err, ErrLeaseNotFound) {
		t.Fatalf("second release error=%v", err)
	}
}

func TestDurableWaitingOnlyUpdatePersistsWithoutChangingActive(t *testing.T) {
	store := &failingCompositionStore{}
	first := moduleForHost("durable-waiting-base", nil)
	host := openTestHost(t, store, first)
	if err := host.ApplyModules(context.Background(), []Module{first}); err != nil {
		t.Fatal(err)
	}
	activeRevision, _ := host.ActiveCompositionRevision()
	waiting := moduleForHost("durable-waiting-child", nil)
	waiting.manifest.Requires = []Dependency{{ModuleID: "missing-provider", Contract: "catalog", Version: VersionRange{Min: Version{Major: 1}}}}
	if err := host.ApplyModules(context.Background(), []Module{first, waiting}); err != nil {
		t.Fatal(err)
	}
	if current, _ := host.ActiveCompositionRevision(); current != activeRevision {
		t.Fatalf("waiting-only update changed active revision: %q -> %q", activeRevision, current)
	}
	if state := store.snapshot(); len(state.Desired) != 2 || len(state.Compositions) != 1 || state.Compositions[0].Status != DurableCompositionActive {
		t.Fatalf("waiting desired state=%#v", state)
	}
	if state, found := host.State(waiting.manifest.ID); !found || state != StateWaitingDependencies {
		t.Fatalf("waiting module state=%s found=%v", state, found)
	}
}

func TestDurableRetainedLineageIsRemovedAfterReferencingActive(t *testing.T) {
	store := NewMemoryCompositionStore()
	first := moduleForHost("durable-lineage-old", nil)
	host := openTestHost(t, store, first)
	if err := host.ApplyModules(context.Background(), []Module{first}); err != nil {
		t.Fatal(err)
	}
	oldRevision, _ := host.ActiveCompositionRevision()
	second := moduleForHost("durable-lineage-new", nil)
	if err := host.ApplyModules(context.Background(), []Module{first, second}); err != nil {
		t.Fatal(err)
	}
	if err := host.Deactivate(context.Background(), oldRevision); err != nil {
		t.Fatal(err)
	}
	state, _, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Compositions) != 2 {
		t.Fatalf("lineage was dropped too early: %#v", state.Compositions)
	}
	for _, composition := range state.Compositions {
		if composition.CompositionRevision == oldRevision && composition.Status != DurableCompositionRetained {
			t.Fatalf("old lineage status=%s", composition.Status)
		}
	}
	newRevision, _ := host.ActiveCompositionRevision()
	if err := host.Deactivate(context.Background(), newRevision); err != nil {
		t.Fatal(err)
	}
	state, _, err = store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Compositions) != 0 {
		t.Fatalf("retained lineage leaked after final deactivation: %#v", state.Compositions)
	}
}

func TestDurableDeactivationRetryRefreshesInheritedEffectState(t *testing.T) {
	store := &failingCompositionStore{}
	first := moduleForHost("durable-inherited-old", nil)
	first.effects = 1
	inverse := &inverseRecorder{}
	host, err := OpenModuleHost(context.Background(), Version{Major: 1}, []Module{first}, NewMemoryEffectJournal(), inverse, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := host.ApplyModules(context.Background(), []Module{first}); err != nil {
		t.Fatal(err)
	}
	oldRevision, _ := host.ActiveCompositionRevision()
	second := moduleForHost("durable-inherited-new", nil)
	if err := host.ApplyModules(context.Background(), []Module{first, second}); err != nil {
		t.Fatal(err)
	}
	if err := host.Deactivate(context.Background(), oldRevision); err != nil {
		t.Fatal(err)
	}
	newRevision, _ := host.ActiveCompositionRevision()
	// durableBlock now persists the fail-closed intent before any inverse.
	// Target the subsequent durable removal CAS, which is the commit point.
	store.mu.Lock()
	store.failCall = store.calls + 2
	store.mu.Unlock()
	if err := host.Deactivate(context.Background(), newRevision); err == nil {
		t.Fatal("first final deactivation unexpectedly succeeded")
	}
	inverse.mu.Lock()
	firstCount := len(inverse.ids)
	inverse.mu.Unlock()
	if firstCount != 1 {
		t.Fatalf("first deactivation inverse count=%d", firstCount)
	}
	if err := host.Deactivate(context.Background(), newRevision); err != nil {
		t.Fatal(err)
	}
	inverse.mu.Lock()
	secondCount := len(inverse.ids)
	inverse.mu.Unlock()
	if secondCount != firstCount {
		t.Fatalf("retry replayed inherited inverse: first=%d second=%d", firstCount, secondCount)
	}
	if state := store.snapshot(); len(state.Compositions) != 0 {
		t.Fatalf("durable compositions remain=%#v", state.Compositions)
	}
}

func TestOpenModuleHostRejectsPersistedAPIMismatchAndBootstrapError(t *testing.T) {
	module := moduleForHost("durable-api-mismatch", nil)
	store := NewMemoryCompositionStore()
	state := DurableHostState{StoreRevision: "store-api", HostAPIVersion: Version{Major: 2}, Desired: []ModuleManifest{module.Manifest()}}
	if err := store.CompareAndSwap(context.Background(), "", state); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenModuleHost(context.Background(), Version{Major: 1}, []Module{module}, NewMemoryEffectJournal(), &inverseRecorder{}, store); !errors.Is(err, ErrRecoveryBlocked) {
		t.Fatalf("API mismatch error=%v", err)
	}
	failing := &failingCompositionStore{failNext: true}
	if _, err := OpenModuleHost(context.Background(), Version{Major: 1}, []Module{module}, NewMemoryEffectJournal(), &inverseRecorder{}, failing); !errors.Is(err, ErrRecoveryBlocked) {
		t.Fatalf("bootstrap failure error=%v", err)
	}
	if failing.found {
		t.Fatal("non-conflict bootstrap failure was retried as a load")
	}
}

func TestDurableBootstrapCASPanicFailsClosed(t *testing.T) {
	module := moduleForHost("durable-bootstrap-panic", nil)
	for _, open := range []func(context.Context, Version, []Module, EffectJournal, InverseExecutor, CompositionStore) (*ModuleHost, error){OpenModuleHost, RecoverModuleHost} {
		store := &failingCompositionStore{panicNextCAS: true}
		if _, err := open(context.Background(), Version{Major: 1}, []Module{module}, NewMemoryEffectJournal(), &inverseRecorder{}, store); !errors.Is(err, ErrRecoveryBlocked) {
			t.Fatalf("bootstrap panic error=%v", err)
		}
		if store.found {
			t.Fatal("bootstrap panic advanced durable state")
		}
	}
}

type cancelAtSecondCheck struct {
	mu     sync.Mutex
	checks int
}

func (ctx *cancelAtSecondCheck) Deadline() (time.Time, bool) { return time.Time{}, false }
func (ctx *cancelAtSecondCheck) Done() <-chan struct{}       { return nil }
func (ctx *cancelAtSecondCheck) Value(key any) any           { return nil }
func (ctx *cancelAtSecondCheck) Err() error {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	ctx.checks++
	if ctx.checks >= 2 {
		return context.Canceled
	}
	return nil
}

func TestDurableCASUsesCallerContext(t *testing.T) {
	store := &failingCompositionStore{}
	module := moduleForHost("durable-context", nil)
	host := openTestHost(t, store, module)
	ctx := &cancelAtSecondCheck{}
	if err := host.ApplyModules(ctx, []Module{module}); !errors.Is(err, context.Canceled) {
		t.Fatalf("context error=%v", err)
	}
	if state := store.snapshot(); len(state.Compositions) != 0 {
		t.Fatalf("canceled CAS changed durable state: %#v", state.Compositions)
	}
}

func TestDeactivateJournalFailureRestoresDrainingState(t *testing.T) {
	module := moduleForHost("deactivate-list-failure", nil)
	host, journal, _ := newHost(t, module)
	if err := host.ApplyModules(context.Background(), []Module{module}); err != nil {
		t.Fatal(err)
	}
	revision, _ := host.ActiveCompositionRevision()
	host.journal = &failingMarkJournal{inner: journal, listErr: errors.New("list failed")}
	if err := host.Deactivate(context.Background(), revision); err == nil {
		t.Fatal("deactivation unexpectedly succeeded")
	}
	host.mu.RLock()
	composition := host.compositions[revision]
	state := composition.state
	host.mu.RUnlock()
	if state != StateDraining {
		t.Fatalf("journal failure left state=%s", state)
	}
}

func TestDurablePromotionFailureRollsBackCandidate(t *testing.T) {
	store := &failingCompositionStore{}
	first := moduleForHost("durable-old", nil)
	host := openTestHost(t, store, first)
	if err := host.ApplyModules(context.Background(), []Module{first}); err != nil {
		t.Fatal(err)
	}
	oldRevision, _ := host.ActiveCompositionRevision()
	second := moduleForHost("durable-new", nil)
	// This failure occurs in the promotion CAS, after candidate activation.
	store.mu.Lock()
	store.failCall = store.calls + 2
	store.mu.Unlock()
	if err := host.ApplyModules(context.Background(), []Module{first, second}); err == nil {
		t.Fatal("promotion unexpectedly succeeded")
	}
	currentRevision, _ := host.ActiveCompositionRevision()
	if currentRevision != oldRevision {
		t.Fatalf("old revision changed: %q -> %q", oldRevision, currentRevision)
	}
	state := store.snapshot()
	if len(state.Compositions) != 1 || state.Compositions[0].Status != DurableCompositionActive {
		t.Fatalf("durable old state=%#v", state.Compositions)
	}
	if second.lastOwner == nil {
		t.Fatal("candidate was not exercised")
	}
}

func TestDurableDeactivateStoreFailureRemainsVisibleAndRetryRemoves(t *testing.T) {
	store := &failingCompositionStore{}
	module := moduleForHost("durable-deactivate", nil)
	host := openTestHost(t, store, module)
	if err := host.ApplyModules(context.Background(), []Module{module}); err != nil {
		t.Fatal(err)
	}
	revision, _ := host.ActiveCompositionRevision()
	storeState := store.snapshot()
	store.setFailNext()
	if err := host.Deactivate(context.Background(), revision); err == nil {
		t.Fatal("deactivation unexpectedly succeeded")
	}
	if _, found := host.ActiveCompositionRevision(); !found {
		t.Fatal("failed durable removal hid active composition")
	}
	if err := host.Deactivate(context.Background(), revision); err != nil {
		t.Fatal(err)
	}
	if _, found := host.ActiveCompositionRevision(); found {
		t.Fatal("successful durable removal left active composition")
	}
	if state := store.snapshot(); len(state.Compositions) != 0 || state.StoreRevision == storeState.StoreRevision {
		t.Fatalf("store after removal=%#v", state)
	}
}
