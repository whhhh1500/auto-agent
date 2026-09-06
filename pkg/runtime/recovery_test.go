package runtime

import (
	"context"
	"errors"
	"sync"
	"testing"
)

type recoverableModule struct {
	manifest               ModuleManifest
	activateCalls          int
	recoverCalls           int
	priorLost              bool
	reconcileErr           error
	deactivateErr          error
	reconcileRecordThenErr bool
	mutateEffects          bool
	lastOwner              *hostOwner
	mu                     sync.Mutex
}

func newRecoverableModule(id ModuleID) *recoverableModule {
	return &recoverableModule{manifest: ModuleManifest{ID: id, Version: Version{Major: 1}, Effects: []EffectKind{"forward", "inverse", "reconcile", "undo-reconcile"}}}
}

func (module *recoverableModule) Manifest() ModuleManifest { return module.manifest.Clone() }
func (module *recoverableModule) Stage(ctx context.Context, stage StageContext) (StagedModule, error) {
	return &recoverableStaged{module: module, stage: stage}, nil
}

type recoverableStaged struct {
	module *recoverableModule
	stage  StageContext
}

type idempotentInverse struct {
	mu          sync.Mutex
	seen        map[EffectID]struct{}
	sideEffects int
}

func (inverse *idempotentInverse) ExecuteInverse(context.Context, RecordedEffect) error {
	return nil
}

func (inverse *idempotentInverse) apply(effect RecordedEffect) {
	inverse.mu.Lock()
	defer inverse.mu.Unlock()
	if inverse.seen == nil {
		inverse.seen = make(map[EffectID]struct{})
	}
	if _, exists := inverse.seen[effect.Descriptor.ID]; exists {
		return
	}
	inverse.seen[effect.Descriptor.ID] = struct{}{}
	inverse.sideEffects++
}

func (inverse *idempotentInverse) sideEffectCount() int {
	inverse.mu.Lock()
	defer inverse.mu.Unlock()
	return inverse.sideEffects
}

type idempotentInverseExecutor struct{ inverse *idempotentInverse }

func (executor idempotentInverseExecutor) ExecuteInverse(ctx context.Context, effect RecordedEffect) error {
	executor.inverse.apply(effect)
	return nil
}

func (staged *recoverableStaged) Health(context.Context) error { return nil }
func (staged *recoverableStaged) Activate(ctx context.Context, transaction ActivationTransaction) (ModuleLeaseOwner, error) {
	staged.module.mu.Lock()
	staged.module.activateCalls++
	staged.module.mu.Unlock()
	owner := &hostOwner{moduleID: staged.module.manifest.ID, version: staged.module.manifest.Version, reconcileErr: staged.module.reconcileErr, deactivateErr: staged.module.deactivateErr, reconcileRecordThenErr: staged.module.reconcileRecordThenErr}
	staged.module.mu.Lock()
	staged.module.lastOwner = owner
	staged.module.mu.Unlock()
	return owner, nil
}
func (staged *recoverableStaged) Recover(ctx context.Context, recovery RecoveryContext) (ModuleLeaseOwner, error) {
	if !recovery.PriorLeasesLost {
		return nil, errors.New("prior leases were not marked lost")
	}
	staged.module.mu.Lock()
	staged.module.recoverCalls++
	staged.module.priorLost = recovery.PriorLeasesLost
	staged.module.mu.Unlock()
	if staged.module.mutateEffects && len(recovery.Effects) != 0 {
		recovery.Effects[0].Descriptor.Forward.Payload = []byte("mutated")
	}
	owner := &hostOwner{moduleID: staged.module.manifest.ID, version: staged.module.manifest.Version, reconcileErr: staged.module.reconcileErr, deactivateErr: staged.module.deactivateErr, reconcileRecordThenErr: staged.module.reconcileRecordThenErr}
	staged.module.mu.Lock()
	staged.module.lastOwner = owner
	staged.module.mu.Unlock()
	return owner, nil
}

func (module *recoverableModule) counts() (int, int, bool) {
	module.mu.Lock()
	defer module.mu.Unlock()
	return module.activateCalls, module.recoverCalls, module.priorLost
}

func TestRecoverModuleHostActiveUsesRecoverNotActivateAndLosesLeases(t *testing.T) {
	store := NewMemoryCompositionStore()
	journal := NewMemoryEffectJournal()
	inverse := &inverseRecorder{}
	initial := newRecoverableModule("recover-active")
	first, err := OpenModuleHost(context.Background(), Version{Major: 1}, []Module{initial}, journal, inverse, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.ApplyModules(context.Background(), []Module{initial}); err != nil {
		t.Fatal(err)
	}
	revision, _ := first.ActiveCompositionRevision()
	oldLease, err := first.AcquireLease(context.Background(), LeaseRequest{RunID: "old-run", CompositionRevision: revision, ModuleID: initial.manifest.ID, ModuleRevision: initial.manifest.Version, Scope: "tenant/a"})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	restarted := newRecoverableModule("recover-active")
	recovered, err := RecoverModuleHost(context.Background(), Version{Major: 1}, []Module{restarted}, journal, inverse, store)
	if err != nil {
		t.Fatal(err)
	}
	activate, recoverCalls, priorLost := restarted.counts()
	if activate != 0 || recoverCalls != 1 || !priorLost {
		t.Fatalf("activate=%d recover=%d priorLost=%v", activate, recoverCalls, priorLost)
	}
	if err := recovered.ReleaseLease(context.Background(), oldLease); !errors.Is(err, ErrLeaseNotFound) {
		t.Fatalf("old lease release error=%v", err)
	}
	newLease, err := recovered.AcquireLease(context.Background(), LeaseRequest{RunID: "new-run", CompositionRevision: revision, ModuleID: restarted.manifest.ID, ModuleRevision: restarted.manifest.Version, Scope: "tenant/a"})
	if err != nil {
		t.Fatal(err)
	}
	if newLease.Token == oldLease.Token {
		t.Fatal("recovery reused old public lease token")
	}
}

func TestInverseRetryFixtureIsIdempotentByEffectID(t *testing.T) {
	inner := NewMemoryEffectJournal()
	journal := &failingMarkJournal{inner: inner, err: errors.New("mark reverted failed once"), failOnce: true}
	inverse := &idempotentInverse{}
	module := moduleForHost("inverse-retry", nil)
	host, err := NewModuleHost(Version{Major: 1}, []Module{module}, journal, idempotentInverseExecutor{inverse: inverse})
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Activate(context.Background()); err != nil {
		t.Fatal(err)
	}
	revision, found := host.ActiveCompositionRevision()
	if !found {
		t.Fatal("active composition missing")
	}
	descriptor := EffectDescriptor{
		ID: "inverse-retry-effect", ModuleID: module.manifest.ID, ModuleRevision: module.manifest.Version,
		CompositionRevision: revision, Phase: EffectPhaseActivate,
		Forward: EffectAction{Kind: "forward", Target: "retry"}, Inverse: EffectAction{Kind: "inverse", Target: "retry"},
	}
	if _, err := journal.Record(context.Background(), descriptor); err != nil {
		t.Fatal(err)
	}
	if err := journal.MarkApplied(context.Background(), descriptor.ID); err != nil {
		t.Fatal(err)
	}
	if err := host.Deactivate(context.Background(), revision); err == nil {
		t.Fatal("first deactivation unexpectedly succeeded")
	}
	if err := host.Deactivate(context.Background(), revision); err != nil {
		t.Fatal(err)
	}
	if got := inverse.sideEffectCount(); got != 1 {
		t.Fatalf("inverse side effect count=%d, want 1", got)
	}
	records, err := inner.List(context.Background(), revision)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].State != EffectReverted {
		t.Fatalf("retry did not finish durable revert: %#v", records)
	}
}

func TestRecoveryContextEffectsAreDefensiveCopies(t *testing.T) {
	store := NewMemoryCompositionStore()
	journal := NewMemoryEffectJournal()
	initial := newRecoverableModule("recover-defensive")
	first, err := OpenModuleHost(context.Background(), Version{Major: 1}, []Module{initial}, journal, &inverseRecorder{}, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.ApplyModules(context.Background(), []Module{initial}); err != nil {
		t.Fatal(err)
	}
	revision, _ := first.ActiveCompositionRevision()
	descriptor := EffectDescriptor{ID: "recover-defensive-effect", ModuleID: initial.manifest.ID, ModuleRevision: initial.manifest.Version, CompositionRevision: revision, Phase: EffectPhaseActivate, Forward: EffectAction{Kind: "forward", Target: "payload", Payload: []byte("original")}, Inverse: EffectAction{Kind: "inverse", Target: "payload"}}
	if _, err := journal.Record(context.Background(), descriptor); err != nil {
		t.Fatal(err)
	}
	if err := journal.MarkApplied(context.Background(), descriptor.ID); err != nil {
		t.Fatal(err)
	}
	restarted := newRecoverableModule("recover-defensive")
	restarted.mutateEffects = true
	if err := first.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := RecoverModuleHost(context.Background(), Version{Major: 1}, []Module{restarted}, journal, &inverseRecorder{}, store); err != nil {
		t.Fatal(err)
	}
	records, err := journal.List(context.Background(), revision)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range records {
		if record.Descriptor.ID == descriptor.ID && string(record.Descriptor.Forward.Payload) != "original" {
			t.Fatalf("recovery mutation leaked into journal: %q", record.Descriptor.Forward.Payload)
		}
	}
}

func TestRecoverOrphanFailurePreventsOwnerCreation(t *testing.T) {
	store := NewMemoryCompositionStore()
	journal := NewMemoryEffectJournal()
	activeModule := newRecoverableModule("recover-orphan-active")
	first, err := OpenModuleHost(context.Background(), Version{Major: 1}, []Module{activeModule}, journal, &inverseRecorder{}, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.ApplyModules(context.Background(), []Module{activeModule}); err != nil {
		t.Fatal(err)
	}
	state := storeState(t, store)
	orphanRevision := "orphan-z"
	orphan := testDurableComposition(t, orphanRevision, DurableCompositionDraining, []ModuleManifest{activeModule.Manifest()}, "")
	state.Compositions = append(state.Compositions, orphan)
	orphanEffect := EffectDescriptor{ID: "orphan-effect", ModuleID: activeModule.manifest.ID, ModuleRevision: activeModule.manifest.Version, CompositionRevision: orphanRevision, Phase: EffectPhaseActivate, Forward: EffectAction{Kind: "forward", Target: "orphan"}, Inverse: EffectAction{Kind: "inverse", Target: "orphan"}}
	if _, err := journal.Record(context.Background(), orphanEffect); err != nil {
		t.Fatal(err)
	}
	if err := journal.MarkApplied(context.Background(), orphanEffect.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.CompareAndSwap(context.Background(), state.StoreRevision, DurableHostState{StoreRevision: "store-orphan", HostAPIVersion: state.HostAPIVersion, Desired: state.Desired, Compositions: state.Compositions}); err != nil {
		t.Fatal(err)
	}
	restarted := newRecoverableModule("recover-orphan-active")
	if _, err := RecoverModuleHost(context.Background(), Version{Major: 1}, []Module{restarted}, journal, &failingInverse{err: errors.New("orphan inverse failed")}, store); !errors.Is(err, ErrRecoveryBlocked) {
		t.Fatalf("orphan cleanup error=%v", err)
	}
	if _, recoverCalls, _ := restarted.counts(); recoverCalls != 0 {
		t.Fatal("active owner was created before orphan cleanup completed")
	}
}

func TestRecoverLineageRetainsMatchingEffectsAndCleansOnFinalDeactivate(t *testing.T) {
	store := NewMemoryCompositionStore()
	journal := NewMemoryEffectJournal()
	inverse := &inverseRecorder{}
	old := newRecoverableModule("recover-lineage")
	first, err := OpenModuleHost(context.Background(), Version{Major: 1}, []Module{old}, journal, inverse, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.ApplyModules(context.Background(), []Module{old}); err != nil {
		t.Fatal(err)
	}
	oldRevision, _ := first.ActiveCompositionRevision()
	oldEffect := EffectDescriptor{ID: "lineage-effect", ModuleID: old.manifest.ID, ModuleRevision: old.manifest.Version, CompositionRevision: oldRevision, Phase: EffectPhaseActivate, Forward: EffectAction{Kind: "forward", Target: "lineage"}, Inverse: EffectAction{Kind: "inverse", Target: "lineage"}}
	if _, err := journal.Record(context.Background(), oldEffect); err != nil {
		t.Fatal(err)
	}
	if err := journal.MarkApplied(context.Background(), oldEffect.ID); err != nil {
		t.Fatal(err)
	}
	newModule := newRecoverableModule("recover-lineage-new")
	if err := first.ApplyModules(context.Background(), []Module{old, newModule}); err != nil {
		t.Fatal(err)
	}
	newRevision, _ := first.ActiveCompositionRevision()
	restartedOld := newRecoverableModule("recover-lineage")
	restartedNew := newRecoverableModule("recover-lineage-new")
	if err := first.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	recovered, err := RecoverModuleHost(context.Background(), Version{Major: 1}, []Module{restartedOld, restartedNew}, journal, inverse, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := recovered.Deactivate(context.Background(), newRevision); err != nil {
		t.Fatal(err)
	}
	state := storeState(t, store)
	if len(state.Compositions) != 0 {
		t.Fatalf("recovered lineage not recursively removed: %#v", state.Compositions)
	}
	inverse.mu.Lock()
	count := 0
	for _, id := range inverse.ids {
		if id == oldEffect.ID {
			count++
		}
	}
	inverse.mu.Unlock()
	if count != 1 {
		t.Fatalf("matching ancestor effect inverse count=%d", count)
	}
}

func TestRecoverLineageRevertsMismatchedAncestorEffects(t *testing.T) {
	store := NewMemoryCompositionStore()
	journal := NewMemoryEffectJournal()
	inverse := &inverseRecorder{}
	old := newRecoverableModule("recover-lineage-mismatch")
	oldRevision := "mismatch-old"
	descriptor := EffectDescriptor{
		ID: "mismatched-lineage-effect", ModuleID: old.manifest.ID, ModuleRevision: old.manifest.Version,
		CompositionRevision: oldRevision, Phase: EffectPhaseActivate,
		Forward: EffectAction{Kind: "forward", Target: "mismatch"}, Inverse: EffectAction{Kind: "inverse", Target: "mismatch"},
	}
	if _, err := journal.Record(context.Background(), descriptor); err != nil {
		t.Fatal(err)
	}
	if err := journal.MarkApplied(context.Background(), descriptor.ID); err != nil {
		t.Fatal(err)
	}
	replacement := newRecoverableModule("recover-lineage-mismatch")
	replacement.manifest.Version = Version{Major: 2}
	activeRevision := "mismatch-active"
	state := DurableHostState{
		StoreRevision:  "store-mismatch",
		HostAPIVersion: Version{Major: 1},
		Desired:        []ModuleManifest{replacement.Manifest()},
		Compositions: []DurableComposition{
			testDurableComposition(t, oldRevision, DurableCompositionRetained, []ModuleManifest{old.Manifest()}, ""),
			testDurableComposition(t, activeRevision, DurableCompositionActive, []ModuleManifest{replacement.Manifest()}, oldRevision),
		},
	}
	if err := store.CompareAndSwap(context.Background(), "", state); err != nil {
		t.Fatal(err)
	}
	records, err := journal.List(context.Background(), oldRevision)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].State != EffectApplied || len(inverse.ids) != 0 {
		t.Fatalf("test setup did not preserve applied ancestor effect: records=%#v inverse=%v", records, inverse.ids)
	}
	restarted := newRecoverableModule("recover-lineage-mismatch")
	restarted.manifest.Version = Version{Major: 2}
	if _, err := RecoverModuleHost(context.Background(), Version{Major: 1}, []Module{restarted}, journal, inverse, store); err != nil {
		t.Fatal(err)
	}
	records, err = journal.List(context.Background(), oldRevision)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].State != EffectReverted {
		t.Fatalf("mismatched ancestor effect was not reverted: %#v", records)
	}
	inverse.mu.Lock()
	count := 0
	for _, id := range inverse.ids {
		if id == descriptor.ID {
			count++
		}
	}
	inverse.mu.Unlock()
	if count != 1 {
		t.Fatalf("mismatched ancestor inverse count=%d", count)
	}
}

func TestOrphanCompositionOrderFollowsSupersedesTopology(t *testing.T) {
	compositions := []DurableComposition{
		{CompositionRevision: "root-z"},
		{CompositionRevision: "child-a", Supersedes: "root-z"},
		{CompositionRevision: "grandchild-m", Supersedes: "child-a"},
		{CompositionRevision: "unrelated-x"},
	}
	ordered := orphanCompositionOrder(compositions)
	position := make(map[string]int, len(ordered))
	for index, revision := range ordered {
		position[revision] = index
	}
	if !(position["grandchild-m"] < position["child-a"] && position["child-a"] < position["root-z"]) {
		t.Fatalf("lineage was not ordered newest to oldest: %v", ordered)
	}
}

func TestRecoveryRejectsStagePhaseEffects(t *testing.T) {
	module := moduleForHost("recovery-stage-phase", nil)
	journal := NewMemoryEffectJournal()
	host, err := NewModuleHost(Version{Major: 1}, []Module{module}, journal, &inverseRecorder{})
	if err != nil {
		t.Fatal(err)
	}
	descriptor := EffectDescriptor{
		ID: "stage-phase-effect", ModuleID: module.manifest.ID, ModuleRevision: module.manifest.Version,
		CompositionRevision: "stage-phase-composition", Phase: EffectPhaseStage,
		Forward: EffectAction{Kind: "forward", Target: "stage"}, Inverse: EffectAction{Kind: "inverse", Target: "stage"},
	}
	if _, err := journal.Record(context.Background(), descriptor); err != nil {
		t.Fatal(err)
	}
	if _, err := host.loadCompositionEffects(context.Background(), descriptor.CompositionRevision, []ModuleManifest{module.Manifest()}, false); !errors.Is(err, ErrInvalidEffect) {
		t.Fatalf("stage effect was accepted during recovery: %v", err)
	}
}

func TestRecoverPreparedRemoveCASConflictPreservesCheckpoint(t *testing.T) {
	store := &failingCompositionStore{}
	journal := NewMemoryEffectJournal()
	module := newRecoverableModule("recover-prepared-cas")
	state := DurableHostState{StoreRevision: "store-prepared-cas", HostAPIVersion: Version{Major: 1}, Desired: []ModuleManifest{module.Manifest()}, Compositions: []DurableComposition{testDurableComposition(t, "prepared-cas", DurableCompositionPrepared, nil, "")}}
	if err := store.CompareAndSwap(context.Background(), "", state); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.conflictNext = true
	store.mu.Unlock()
	if _, err := RecoverModuleHost(context.Background(), Version{Major: 1}, []Module{module}, journal, &inverseRecorder{}, store); !errors.Is(err, ErrRecoveryBlocked) {
		t.Fatalf("prepared CAS conflict error=%v", err)
	}
	if state := store.snapshot(); len(state.Compositions) != 1 || state.Compositions[0].Status != DurableCompositionPrepared {
		t.Fatalf("prepared checkpoint lost after CAS conflict: %#v", state.Compositions)
	}
}

func TestRecoverModuleHostRejectsNonRecoverableActive(t *testing.T) {
	store := NewMemoryCompositionStore()
	module := moduleForHost("recover-nonrecoverable", nil)
	first := openTestHost(t, store, module)
	if err := first.ApplyModules(context.Background(), []Module{module}); err != nil {
		t.Fatal(err)
	}
	if _, err := RecoverModuleHost(context.Background(), Version{Major: 1}, []Module{module}, NewMemoryEffectJournal(), &inverseRecorder{}, store); !errors.Is(err, ErrRecoveryBlocked) {
		t.Fatalf("nonrecoverable error=%v", err)
	}
}

func TestRecoverModuleHostRejectsBlockedCheckpoint(t *testing.T) {
	store := NewMemoryCompositionStore()
	module := newRecoverableModule("recover-blocked")
	state := DurableHostState{StoreRevision: "store-blocked", HostAPIVersion: Version{Major: 1}, Desired: []ModuleManifest{module.Manifest()}, Compositions: []DurableComposition{testDurableComposition(t, "blocked-composition", DurableCompositionBlocked, nil, "")}}
	if err := store.CompareAndSwap(context.Background(), "", state); err != nil {
		t.Fatal(err)
	}
	if _, err := RecoverModuleHost(context.Background(), Version{Major: 1}, []Module{module}, NewMemoryEffectJournal(), &inverseRecorder{}, store); !errors.Is(err, ErrRecoveryBlocked) {
		t.Fatalf("blocked checkpoint error=%v", err)
	}
}

func TestRecoverReconcileFailureDoesNotPublishAndCleansOwners(t *testing.T) {
	store := NewMemoryCompositionStore()
	journal := NewMemoryEffectJournal()
	module := newRecoverableModule("recover-reconcile-fail")
	first, err := OpenModuleHost(context.Background(), Version{Major: 1}, []Module{module}, journal, &inverseRecorder{}, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.ApplyModules(context.Background(), []Module{module}); err != nil {
		t.Fatal(err)
	}
	restarted := newRecoverableModule("recover-reconcile-fail")
	restarted.reconcileErr = errors.New("reconcile failed")
	if err := first.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := RecoverModuleHost(context.Background(), Version{Major: 1}, []Module{restarted}, journal, &inverseRecorder{}, store); !errors.Is(err, ErrRecoveryBlocked) {
		t.Fatalf("reconcile error=%v", err)
	}
	restarted.mu.Lock()
	owner := restarted.lastOwner
	restarted.mu.Unlock()
	if owner == nil {
		t.Fatal("recovery owner was not created")
	}
	owner.mu.Lock()
	deactivated := owner.deactivateCalls
	owner.mu.Unlock()
	if deactivated == 0 {
		t.Fatal("recovery owner was not locally cleaned")
	}
	if state := storeState(t, store); len(state.Compositions) != 1 || state.Compositions[0].Status != DurableCompositionActive {
		t.Fatalf("durable active checkpoint changed: %#v", state.Compositions)
	}
}

func TestRecoverReconcileFailureRevertsNewEffectsAndRetainsActive(t *testing.T) {
	store := NewMemoryCompositionStore()
	journal := NewMemoryEffectJournal()
	inverse := &inverseRecorder{}
	module := newRecoverableModule("recover-reconcile-effect-fail")
	activeRevision := "reconcile-effect-active"
	activeEffect := EffectDescriptor{
		ID: "existing-active-effect", ModuleID: module.manifest.ID, ModuleRevision: module.manifest.Version,
		CompositionRevision: activeRevision, Phase: EffectPhaseActivate,
		Forward: EffectAction{Kind: "forward", Target: "existing"}, Inverse: EffectAction{Kind: "inverse", Target: "existing"},
	}
	if _, err := journal.Record(context.Background(), activeEffect); err != nil {
		t.Fatal(err)
	}
	if err := journal.MarkApplied(context.Background(), activeEffect.ID); err != nil {
		t.Fatal(err)
	}
	state := DurableHostState{
		StoreRevision:  "store-reconcile-effect",
		HostAPIVersion: Version{Major: 1},
		Desired:        []ModuleManifest{module.Manifest()},
		Compositions: []DurableComposition{
			testDurableComposition(t, activeRevision, DurableCompositionActive, []ModuleManifest{module.Manifest()}, ""),
		},
	}
	if err := store.CompareAndSwap(context.Background(), "", state); err != nil {
		t.Fatal(err)
	}
	restarted := newRecoverableModule("recover-reconcile-effect-fail")
	restarted.reconcileErr = errors.New("reconcile failed after recording")
	restarted.reconcileRecordThenErr = true
	restarted.deactivateErr = errors.New("local cleanup failed")
	if _, err := RecoverModuleHost(context.Background(), Version{Major: 1}, []Module{restarted}, journal, inverse, store); !errors.Is(err, ErrRecoveryBlocked) {
		t.Fatalf("reconcile recovery error=%v", err)
	}
	records, err := journal.List(context.Background(), activeRevision)
	if err != nil {
		t.Fatal(err)
	}
	states := make(map[EffectID]EffectState, len(records))
	for _, record := range records {
		states[record.Descriptor.ID] = record.State
	}
	if states[activeEffect.ID] != EffectApplied {
		t.Fatalf("existing active effect was altered: %#v", states)
	}
	if states[EffectID("reconcile-"+string(module.manifest.ID))] != EffectReverted {
		t.Fatalf("new reconcile effect was not reverted: %#v", states)
	}
	if state := storeState(t, store); len(state.Compositions) != 1 || state.Compositions[0].Status != DurableCompositionActive {
		t.Fatalf("active checkpoint changed: %#v", state.Compositions)
	}
	restarted.mu.Lock()
	owner := restarted.lastOwner
	restarted.mu.Unlock()
	if owner == nil {
		t.Fatal("recovery owner was not created")
	}
	owner.mu.Lock()
	deactivateCalls := owner.deactivateCalls
	owner.mu.Unlock()
	if deactivateCalls == 0 {
		t.Fatal("recovery owner cleanup was not attempted")
	}
}

func TestRecoverPreparedCleansAndRecoversOldActive(t *testing.T) {
	store := NewMemoryCompositionStore()
	journal := NewMemoryEffectJournal()
	inverse := &inverseRecorder{}
	old := newRecoverableModule("recover-prepared-old")
	first, err := OpenModuleHost(context.Background(), Version{Major: 1}, []Module{old}, journal, inverse, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.ApplyModules(context.Background(), []Module{old}); err != nil {
		t.Fatal(err)
	}
	state := storeState(t, store)
	active := state.Compositions[0]
	preparedRevision := "prepared-recover"
	prepared := testDurableComposition(t, preparedRevision, DurableCompositionPrepared, active.Manifests, active.CompositionRevision)
	state.Compositions = append(state.Compositions, prepared)
	if err := store.CompareAndSwap(context.Background(), state.StoreRevision, DurableHostState{StoreRevision: "store-prepared", HostAPIVersion: state.HostAPIVersion, Desired: state.Desired, Compositions: state.Compositions}); err != nil {
		t.Fatal(err)
	}
	restarted := newRecoverableModule("recover-prepared-old")
	if err := first.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	second, err := RecoverModuleHost(context.Background(), Version{Major: 1}, []Module{restarted}, journal, inverse, store)
	if err != nil {
		t.Fatal(err)
	}
	if _, found := second.ActiveCompositionRevision(); !found {
		t.Fatal("old active composition was not recovered")
	}
	state = storeState(t, store)
	if len(state.Compositions) != 1 || state.Compositions[0].Status != DurableCompositionActive {
		t.Fatalf("prepared checkpoint remained: %#v", state.Compositions)
	}
}

func TestRecoverPreparedInverseFailurePreservesCheckpoint(t *testing.T) {
	store := NewMemoryCompositionStore()
	journal := NewMemoryEffectJournal()
	module := newRecoverableModule("recover-prepared-fail")
	first, err := OpenModuleHost(context.Background(), Version{Major: 1}, []Module{module}, journal, &inverseRecorder{}, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.ApplyModules(context.Background(), []Module{module}); err != nil {
		t.Fatal(err)
	}
	state := storeState(t, store)
	active := state.Compositions[0]
	preparedRevision := "prepared-fail"
	descriptor := EffectDescriptor{ID: "prepared-effect", ModuleID: module.manifest.ID, ModuleRevision: module.manifest.Version, CompositionRevision: preparedRevision, Phase: EffectPhaseActivate, Forward: EffectAction{Kind: "forward", Target: "prepared"}, Inverse: EffectAction{Kind: "inverse", Target: "prepared"}}
	if _, err := journal.Record(context.Background(), descriptor); err != nil {
		t.Fatal(err)
	}
	if err := journal.MarkApplied(context.Background(), descriptor.ID); err != nil {
		t.Fatal(err)
	}
	prepared := testDurableComposition(t, preparedRevision, DurableCompositionPrepared, active.Manifests, active.CompositionRevision)
	state.Compositions = append(state.Compositions, prepared)
	if err := store.CompareAndSwap(context.Background(), state.StoreRevision, DurableHostState{StoreRevision: "store-prepared-fail", HostAPIVersion: state.HostAPIVersion, Desired: state.Desired, Compositions: state.Compositions}); err != nil {
		t.Fatal(err)
	}
	if _, err := RecoverModuleHost(context.Background(), Version{Major: 1}, []Module{module}, journal, &failingInverse{err: errors.New("inverse failed")}, store); !errors.Is(err, ErrRecoveryBlocked) {
		t.Fatalf("prepared inverse error=%v", err)
	}
	state = storeState(t, store)
	if len(state.Compositions) != 2 || state.Compositions[1].Status != DurableCompositionPrepared {
		t.Fatalf("prepared checkpoint not retained: %#v", state.Compositions)
	}
}

func storeState(t *testing.T, store *MemoryCompositionStore) DurableHostState {
	t.Helper()
	state, found, err := store.Load(context.Background())
	if err != nil || !found {
		t.Fatalf("store state found=%v err=%v", found, err)
	}
	return state
}
