package runtime

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
)

type hostModule struct {
	manifest      ModuleManifest
	stageErr      error
	healthErr     error
	activateErr   error
	deactivateErr error
	reconcileErr  error
	effects       int
	unknown       bool
	stagePanic    bool
	healthPanic   bool
	activatePanic bool
	log           *[]string
	lastOwner     *hostOwner
}

func (module *hostModule) Manifest() ModuleManifest { return module.manifest.Clone() }

func (module *hostModule) Stage(ctx context.Context, stage StageContext) (StagedModule, error) {
	if module.stagePanic {
		panic("stage")
	}
	if module.log != nil {
		*module.log = append(*module.log, "stage:"+string(module.manifest.ID))
	}
	if module.stageErr != nil {
		return nil, module.stageErr
	}
	return &hostStaged{module: module, stage: stage}, nil
}

type hostStaged struct {
	module *hostModule
	stage  StageContext
}

func (staged *hostStaged) Health(ctx context.Context) error {
	if staged.module.healthPanic {
		panic("health")
	}
	if staged.module.log != nil {
		*staged.module.log = append(*staged.module.log, "health:"+string(staged.module.manifest.ID))
	}
	return staged.module.healthErr
}

func (staged *hostStaged) Activate(ctx context.Context, transaction ActivationTransaction) (ModuleLeaseOwner, error) {
	if staged.module.log != nil {
		*staged.module.log = append(*staged.module.log, "activate:"+string(staged.module.manifest.ID))
	}
	for index := 0; index < staged.module.effects; index++ {
		id := EffectID(fmt.Sprintf("%s-effect-%d", staged.module.manifest.ID, index))
		descriptor := EffectDescriptor{
			ID: id, ModuleID: staged.module.manifest.ID, ModuleRevision: staged.module.manifest.Version,
			CompositionRevision: staged.stage.CompositionRevision, Phase: EffectPhaseActivate,
			Forward: EffectAction{Kind: "forward", Target: string(staged.module.manifest.ID)},
			Inverse: EffectAction{Kind: "inverse", Target: string(staged.module.manifest.ID)},
		}
		effectID, err := transaction.Record(ctx, descriptor)
		if err != nil {
			return nil, err
		}
		if staged.module.unknown {
			if err := transaction.MarkUnknown(ctx, effectID); err != nil {
				return nil, err
			}
		} else if err := transaction.MarkApplied(ctx, effectID); err != nil {
			return nil, err
		}
	}
	if staged.module.activatePanic {
		panic("activate")
	}
	owner := &hostOwner{moduleID: staged.module.manifest.ID, version: staged.module.manifest.Version, log: staged.module.log, deactivateErr: staged.module.deactivateErr, reconcileErr: staged.module.reconcileErr}
	staged.module.lastOwner = owner
	return owner, staged.module.activateErr
}

type hostOwner struct {
	moduleID               ModuleID
	version                Version
	log                    *[]string
	drainResult            DrainResult
	fenceErr               error
	deactivateErr          error
	reconcileErr           error
	reconcileRecordThenErr bool
	deactivateCalls        int
	fenceCalls             int
	reconcileCalls         int
	lastRelease            Lease
	mu                     sync.Mutex
}

func (owner *hostOwner) AcquireLease(ctx context.Context, request LeaseRequest) (Lease, error) {
	return Lease{Identity: LeaseIdentity{LeaseID: request.LeaseID, RunID: request.RunID, CompositionRevision: request.CompositionRevision, ModuleID: request.ModuleID, ModuleRevision: request.ModuleRevision, Scope: request.Scope}, Token: request.LeaseID}, nil
}

func (owner *hostOwner) ReleaseLease(ctx context.Context, lease Lease) error {
	owner.mu.Lock()
	owner.lastRelease = lease
	owner.mu.Unlock()
	if owner.log != nil {
		*owner.log = append(*owner.log, "release:"+string(owner.moduleID))
	}
	return nil
}

func (owner *hostOwner) Drain(ctx context.Context, request DrainRequest) (DrainResult, error) {
	if owner.log != nil {
		*owner.log = append(*owner.log, "drain:"+string(owner.moduleID))
	}
	result := owner.drainResult
	if result.CompositionRevision == "" {
		result.CompositionRevision = request.CompositionRevision
		if result.RemainingLeases == 0 {
			result.Completed = true
		}
	}
	return result, nil
}

func (owner *hostOwner) Fence(ctx context.Context, request FenceRequest) error {
	owner.mu.Lock()
	owner.fenceCalls++
	owner.mu.Unlock()
	if owner.log != nil {
		*owner.log = append(*owner.log, "fence:"+string(owner.moduleID))
	}
	return owner.fenceErr
}

func (owner *hostOwner) Deactivate(ctx context.Context) error {
	owner.mu.Lock()
	owner.deactivateCalls++
	owner.mu.Unlock()
	if owner.log != nil {
		*owner.log = append(*owner.log, "deactivate:"+string(owner.moduleID))
	}
	return owner.deactivateErr
}

func (owner *hostOwner) Reconcile(ctx context.Context, desired DesiredModuleState, records []RecordedEffect, transaction ActivationTransaction) error {
	owner.mu.Lock()
	owner.reconcileCalls++
	owner.mu.Unlock()
	if owner.reconcileErr != nil && !owner.reconcileRecordThenErr {
		return owner.reconcileErr
	}
	descriptor := EffectDescriptor{
		ID: EffectID("reconcile-" + string(owner.moduleID)), ModuleID: owner.moduleID, ModuleRevision: owner.version,
		CompositionRevision: desired.CompositionRevision, Phase: EffectPhaseReconcile,
		Forward: EffectAction{Kind: "reconcile", Target: string(owner.moduleID)},
		Inverse: EffectAction{Kind: "undo-reconcile", Target: string(owner.moduleID)},
	}
	id, err := transaction.Record(ctx, descriptor)
	if err != nil {
		return err
	}
	if err := transaction.MarkApplied(ctx, id); err != nil {
		return err
	}
	if owner.reconcileRecordThenErr {
		return owner.reconcileErr
	}
	return nil
}

type inverseRecorder struct {
	mu  sync.Mutex
	ids []EffectID
}

func (recorder *inverseRecorder) ExecuteInverse(ctx context.Context, effect RecordedEffect) error {
	recorder.mu.Lock()
	recorder.ids = append(recorder.ids, effect.Descriptor.ID)
	recorder.mu.Unlock()
	return nil
}

func moduleForHost(id ModuleID, log *[]string) *hostModule {
	return &hostModule{manifest: ModuleManifest{ID: id, Version: Version{Major: 1}, Effects: []EffectKind{"forward", "inverse", "reconcile", "undo-reconcile"}}, log: log}
}

func newHost(t *testing.T, modules ...*hostModule) (*ModuleHost, *MemoryEffectJournal, *inverseRecorder) {
	t.Helper()
	journal := NewMemoryEffectJournal()
	inverse := &inverseRecorder{}
	values := make([]Module, len(modules))
	for index, module := range modules {
		values[index] = module
	}
	host, err := NewModuleHost(Version{Major: 1}, values, journal, inverse)
	if err != nil {
		t.Fatal(err)
	}
	return host, journal, inverse
}

func TestModuleHostRollsBackStageHealthAndActivationFailures(t *testing.T) {
	for _, test := range []struct {
		name       string
		failStage  bool
		failHealth bool
	}{
		{name: "stage", failStage: true},
		{name: "health", failHealth: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var log []string
			provider := moduleForHost("provider", &log)
			consumer := moduleForHost("consumer", &log)
			provider.manifest.Provides = []ProvidedExtension{extension("provider.ext", SemanticCollection, "catalog", 0)}
			consumer.manifest.Requires = []Dependency{{ModuleID: "provider", Contract: "catalog"}}
			if test.failStage {
				consumer.stageErr = errors.New("stage failed")
			} else {
				consumer.healthErr = errors.New("health failed")
			}
			host, _, _ := newHost(t, consumer, provider)
			if err := host.Activate(context.Background()); err == nil {
				t.Fatal("expected candidate failure")
			}
			if _, ok := host.Snapshot(); ok {
				t.Fatal("failed candidate was published")
			}
			if !containsLog(log, "deactivate:provider") {
				t.Fatalf("previously activated owner was not rolled back: %v", log)
			}
		})
	}

	var log []string
	provider := moduleForHost("provider", &log)
	provider.effects = 1
	provider.manifest.Provides = []ProvidedExtension{extension("provider.ext", SemanticCollection, "catalog", 0)}
	consumer := moduleForHost("consumer", &log)
	consumer.manifest.Provides = []ProvidedExtension{extension("consumer.ext", SemanticCollection, "consumer", 0)}
	consumer.manifest.Requires = []Dependency{{ModuleID: "provider", Contract: "catalog"}}
	consumer.effects = 1
	consumer.activateErr = errors.New("activate failed")
	host, _, inverse := newHost(t, provider, consumer)
	if err := host.Activate(context.Background()); err == nil {
		t.Fatal("expected activation failure")
	}
	if _, ok := host.Snapshot(); ok {
		t.Fatal("activation failure published candidate")
	}
	if !reflect.DeepEqual(inverse.ids, []EffectID{"consumer-effect-0", "provider-effect-0"}) {
		t.Fatalf("inverse order=%v", inverse.ids)
	}
	if !containsLog(log, "deactivate:consumer") || !containsLog(log, "deactivate:provider") {
		t.Fatalf("activated owners not rolled back: %v", log)
	}
}

func TestModuleHostLeaseDrainFenceAndRevisionRetention(t *testing.T) {
	var log []string
	module := moduleForHost("worker", &log)
	host, _, _ := newHost(t, module)
	if err := host.Activate(context.Background()); err != nil {
		t.Fatal(err)
	}
	composition, ok := host.ActiveCompositionRevision()
	if !ok {
		t.Fatal("missing composition revision")
	}
	lease, err := host.AcquireLease(context.Background(), LeaseRequest{RunID: "run-1", CompositionRevision: composition, ModuleID: "worker", ModuleRevision: Version{Major: 1}, Scope: "tenant/a"})
	if err != nil {
		t.Fatal(err)
	}
	owner := host.active.owners["worker"].(*hostOwner)
	if lease.Token == lease.Identity.LeaseID {
		t.Fatal("public lease token must not expose lease ID")
	}
	withoutToken := lease
	withoutToken.Token = ""
	if err := host.ReleaseLease(context.Background(), withoutToken); !errors.Is(err, ErrLeaseMismatch) {
		t.Fatalf("blank lease token err=%v", err)
	}
	if _, err := host.AcquireLease(context.Background(), LeaseRequest{RunID: "run-2", CompositionRevision: "comp-wrong", ModuleID: "worker", ModuleRevision: Version{Major: 1}, Scope: "tenant/a"}); !errors.Is(err, ErrLeaseMismatch) {
		t.Fatalf("wrong snapshot lease err=%v", err)
	}
	if result, err := host.Drain(context.Background(), composition); !errors.Is(err, ErrLeasesRemaining) || result.Completed {
		t.Fatalf("drain with lease result=%+v err=%v", result, err)
	}
	if err := lease.Release(context.Background(), host); err != nil {
		t.Fatal(err)
	}
	owner.mu.Lock()
	released := owner.lastRelease
	owner.mu.Unlock()
	if released.Token != lease.Identity.LeaseID || released.Token == lease.Token {
		t.Fatalf("owner did not receive its opaque token: got=%q public=%q", released.Token, lease.Token)
	}
	if result, err := host.Drain(context.Background(), composition); err != nil || !result.Completed {
		t.Fatalf("drain after release result=%+v err=%v", result, err)
	}

	var fenceLog []string
	fenceModule := moduleForHost("fencer", &fenceLog)
	fenceHost, _, _ := newHost(t, fenceModule)
	if err := fenceHost.Activate(context.Background()); err != nil {
		t.Fatal(err)
	}
	fenceRevision, _ := fenceHost.ActiveCompositionRevision()
	fenceOwner := fenceHost.active.owners["fencer"].(*hostOwner)
	fenceOwner.fenceErr = errors.New("fence failed")
	fenceLease, err := fenceHost.AcquireLease(context.Background(), LeaseRequest{RunID: "run-3", CompositionRevision: fenceRevision, ModuleID: "fencer", ModuleRevision: Version{Major: 1}, Scope: "tenant/a"})
	if err != nil {
		t.Fatal(err)
	}
	if result, err := fenceHost.Fence(context.Background(), fenceRevision, "test"); err == nil || result.Completed {
		t.Fatalf("failed fence result=%+v err=%v", result, err)
	}
	if err := fenceLease.Release(context.Background(), fenceHost); err != nil {
		t.Fatal(err)
	}
}

func TestModuleHostDependencyLossAndReconcile(t *testing.T) {
	var log []string
	provider := moduleForHost("provider", &log)
	consumer := moduleForHost("consumer", &log)
	provider.manifest.Provides = []ProvidedExtension{extension("provider.ext", SemanticCollection, "catalog", 0)}
	consumer.manifest.Requires = []Dependency{{ModuleID: "provider", Contract: "catalog"}}
	host, journal, _ := newHost(t, consumer, provider)
	if err := host.Activate(context.Background()); err != nil {
		t.Fatal(err)
	}
	dependencyOldRevision, ok := host.ActiveCompositionRevision()
	if !ok {
		t.Fatal("missing pre-removal composition")
	}
	if err := host.RemoveModule(context.Background(), "provider"); err != nil {
		t.Fatal(err)
	}
	if state, _ := host.State("consumer"); state != StateWaitingDependencies {
		t.Fatalf("consumer state=%s", state)
	}
	snapshot, _ := host.Snapshot()
	if len(snapshot.Extensions()) != 0 {
		t.Fatalf("dependent route remained after dependency loss: %#v", snapshot.Extensions())
	}
	if _, err := host.Drain(context.Background(), dependencyOldRevision); err != nil {
		t.Fatalf("drain old dependency composition: %v", err)
	}
	if state, _ := host.State("consumer"); state != StateWaitingDependencies {
		t.Fatalf("old composition cleanup overwrote dependency state: %s", state)
	}

	var optionalLog []string
	optionalProvider := moduleForHost("optional-provider", &optionalLog)
	optionalConsumer := moduleForHost("optional-consumer", &optionalLog)
	optionalProvider.manifest.Provides = []ProvidedExtension{extension("optional-provider.ext", SemanticCollection, "provider", 0)}
	optionalConsumer.manifest.Provides = []ProvidedExtension{extension("optional-consumer.ext", SemanticCollection, "consumer", 0)}
	optionalConsumer.manifest.Optional = []Dependency{{ModuleID: "optional-provider", Contract: "provider"}}
	optionalHost, _, _ := newHost(t, optionalConsumer, optionalProvider)
	if err := optionalHost.Activate(context.Background()); err != nil {
		t.Fatal(err)
	}
	oldSnapshot, _ := optionalHost.Snapshot()
	oldRevision, _ := optionalHost.ActiveCompositionRevision()
	if err := optionalHost.RemoveModule(context.Background(), "optional-provider"); err != nil {
		t.Fatal(err)
	}
	newSnapshot, _ := optionalHost.Snapshot()
	newRevision, _ := optionalHost.ActiveCompositionRevision()
	if oldRevision == newRevision || oldSnapshot.Revision() == newSnapshot.Revision() || len(oldSnapshot.Extensions()) != 2 || len(newSnapshot.Extensions()) != 1 {
		t.Fatalf("snapshot retention/replacement old=%s/%d new=%s/%d", oldRevision, len(oldSnapshot.Extensions()), newRevision, len(newSnapshot.Extensions()))
	}
	if _, err := optionalHost.Drain(context.Background(), oldRevision); err != nil {
		t.Fatalf("drain old retained-owner composition: %v", err)
	}
	if !containsLog(optionalLog, "drain:optional-consumer") || containsLog(optionalLog, "deactivate:optional-consumer") {
		t.Fatalf("retained owner drain/deactivate behavior log=%v", optionalLog)
	}
	if err := optionalHost.Reconcile(context.Background(), newRevision); err != nil {
		t.Fatal(err)
	}
	records, err := optionalHost.journal.List(context.Background(), newRevision)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Descriptor.Phase != EffectPhaseReconcile {
		t.Fatalf("reconcile records=%+v", records)
	}
	if _, err := journal.List(context.Background(), "missing"); err != nil {
		t.Fatal(err)
	}
}

func TestModuleHostCarriesInheritedEffectsAcrossRemovals(t *testing.T) {
	var log []string
	a := moduleForHost("a", &log)
	b := moduleForHost("b", &log)
	c := moduleForHost("c", &log)
	a.effects, b.effects, c.effects = 1, 1, 1
	host, _, inverse := newHost(t, a, b, c)
	if err := host.Activate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := host.RemoveModule(context.Background(), "b"); err != nil {
		t.Fatal(err)
	}
	if err := host.RemoveModule(context.Background(), "c"); err != nil {
		t.Fatal(err)
	}
	revision, ok := host.ActiveCompositionRevision()
	if !ok {
		t.Fatal("missing final composition")
	}
	if _, err := host.Drain(context.Background(), revision); err != nil {
		t.Fatal(err)
	}
	countA := 0
	for _, id := range inverse.ids {
		if id == "a-effect-0" {
			countA++
		}
	}
	if countA != 1 {
		t.Fatalf("inherited effect inverse count for a=%d ids=%v", countA, inverse.ids)
	}
}

func TestActivationTransactionConcurrentRecordAndMark(t *testing.T) {
	journal := NewMemoryEffectJournal()
	transaction := &activationTransaction{
		journal: journal, moduleID: "concurrent", moduleRevision: Version{Major: 1},
		compositionRevision: "comp-concurrent", phase: EffectPhaseActivate,
		allowedEffects: map[EffectKind]struct{}{"forward": {}, "inverse": {}},
	}
	var group sync.WaitGroup
	for index := 0; index < 32; index++ {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			id := EffectID(fmt.Sprintf("concurrent-%d", index))
			descriptor := EffectDescriptor{
				ID: id, ModuleID: "concurrent", ModuleRevision: Version{Major: 1},
				CompositionRevision: "comp-concurrent", Phase: EffectPhaseActivate,
				Forward: EffectAction{Kind: "forward", Target: "concurrent"},
				Inverse: EffectAction{Kind: "inverse", Target: "concurrent"},
			}
			recorded, err := transaction.Record(context.Background(), descriptor)
			if err != nil {
				t.Errorf("record %d: %v", index, err)
				return
			}
			if err := transaction.MarkApplied(context.Background(), recorded); err != nil {
				t.Errorf("mark %d: %v", index, err)
			}
		}(index)
	}
	group.Wait()
	records, err := journal.List(context.Background(), "comp-concurrent")
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 32 {
		t.Fatalf("record count=%d", len(records))
	}
}

func TestModuleHostApplyModulesInstallsDependenciesAndReplacesAtomically(t *testing.T) {
	consumer := moduleForHost("consumer", nil)
	consumer.manifest.Requires = []Dependency{{ModuleID: "provider", Contract: "catalog", Version: VersionRange{Min: Version{Major: 1}}}}
	provider := moduleForHost("provider", nil)
	provider.manifest.Provides = []ProvidedExtension{extension("provider.catalog", SemanticCollection, "catalog", 0)}
	host, _, _ := newHost(t)
	if err := host.ApplyModules(context.Background(), []Module{consumer}); err != nil {
		t.Fatal(err)
	}
	if state, _ := host.State("consumer"); state != StateWaitingDependencies {
		t.Fatalf("missing provider state=%s", state)
	}
	if err := host.ApplyModules(context.Background(), []Module{consumer, provider}); err != nil {
		t.Fatal(err)
	}
	readyHost, err := NewModuleHost(Version{Major: 1}, []Module{provider}, NewMemoryEffectJournal(), &inverseRecorder{})
	if err != nil {
		t.Fatal(err)
	}
	if state, _ := readyHost.State("provider"); state != StateConstructed {
		t.Fatalf("initial ready module state=%s", state)
	}
	if err := readyHost.ApplyModules(context.Background(), []Module{provider}); err != nil {
		t.Fatal(err)
	}
	if state, _ := readyHost.State("provider"); state != StateActive {
		t.Fatalf("activated ready module state=%s", state)
	}
	if state, _ := host.State("consumer"); state != StateActive {
		t.Fatalf("restored dependency state=%s", state)
	}
	waiting := moduleForHost("waiting", nil)
	waiting.manifest.Requires = []Dependency{{ModuleID: "missing", Contract: "catalog", Version: VersionRange{Min: Version{Major: 1}}}}
	activeBeforeWaiting, _ := host.ActiveCompositionRevision()
	if err := host.ApplyModules(context.Background(), []Module{consumer, provider, waiting}); err != nil {
		t.Fatal(err)
	}
	activeAfterWaiting, _ := host.ActiveCompositionRevision()
	if activeAfterWaiting != activeBeforeWaiting {
		t.Fatal("waiting-only desired update replaced active composition")
	}
	if state, _ := host.State("waiting"); state != StateWaitingDependencies {
		t.Fatalf("waiting-only desired state=%s", state)
	}
	oldSnapshot, _ := host.Snapshot()
	oldComposition, _ := host.ActiveCompositionRevision()
	replacement := moduleForHost("provider", nil)
	replacement.manifest.Provides = provider.manifest.Provides
	replacement.manifest.Version = Version{Major: 2}
	replacement.activateErr = errors.New("replacement failed")
	if err := host.ApplyModules(context.Background(), []Module{consumer, replacement}); err == nil {
		t.Fatal("failed replacement unexpectedly succeeded")
	}
	currentSnapshot, _ := host.Snapshot()
	currentComposition, _ := host.ActiveCompositionRevision()
	if currentSnapshot.Revision() != oldSnapshot.Revision() || currentComposition != oldComposition {
		t.Fatalf("failed replacement changed active composition: snapshot %s/%s composition %s/%s", oldSnapshot.Revision(), currentSnapshot.Revision(), oldComposition, currentComposition)
	}
	if err := host.ApplyModules(context.Background(), []Module{consumer, provider}); err != nil {
		t.Fatal(err)
	}
	noOpComposition, _ := host.ActiveCompositionRevision()
	otherSameProvider := moduleForHost("provider", nil)
	otherSameProvider.manifest.Provides = provider.manifest.Provides
	if err := host.ApplyModules(context.Background(), []Module{consumer, otherSameProvider}); !errors.Is(err, ErrInvalidHost) {
		t.Fatalf("same manifest different instance err=%v", err)
	}
	extra := moduleForHost("extra", nil)
	if err := host.ApplyModules(context.Background(), []Module{consumer, provider, extra}); err != nil {
		t.Fatal(err)
	}
	otherProvider := moduleForHost("provider", nil)
	otherProvider.manifest.Provides = provider.manifest.Provides
	changedExtra := moduleForHost("extra", nil)
	changedExtra.manifest.Version = Version{Major: 2}
	if err := host.ApplyModules(context.Background(), []Module{consumer, otherProvider, changedExtra}); !errors.Is(err, ErrInvalidHost) {
		t.Fatalf("same-version replacement err=%v", err)
	}
	if composition, _ := host.ActiveCompositionRevision(); composition == noOpComposition {
		t.Fatal("adding a module should publish a new composition")
	}
}

func TestModuleHostLegacyFenceFailsClosed(t *testing.T) {
	var log []string
	provider := moduleForHost("fence-provider", &log)
	provider.manifest.Provides = []ProvidedExtension{extension("fence-provider.ext", SemanticCollection, "provider", 0)}
	consumer := moduleForHost("fence-consumer", &log)
	consumer.manifest.Optional = []Dependency{{ModuleID: "fence-provider", Contract: "provider", Version: VersionRange{Min: Version{Major: 1}}}}
	host, _, _ := newHost(t, consumer, provider)
	if err := host.Activate(context.Background()); err != nil {
		t.Fatal(err)
	}
	oldRevision, _ := host.ActiveCompositionRevision()
	if err := host.RemoveModule(context.Background(), "fence-provider"); err != nil {
		t.Fatal(err)
	}
	if result, err := host.Fence(context.Background(), oldRevision, "test"); !errors.Is(err, ErrFenceUnauthorized) || result.Completed {
		t.Fatalf("legacy fence result=%+v err=%v", result, err)
	}
	if containsLog(log, "fence:fence-consumer") || containsLog(log, "deactivate:fence-consumer") {
		t.Fatalf("legacy fence reached an owner: %v", log)
	}
}

func TestFailedReplacementPreservesAndReleasesOldLease(t *testing.T) {
	var oldLog []string
	stable := moduleForHost("stable", &oldLog)
	host, _, _ := newHost(t, stable)
	if err := host.Activate(context.Background()); err != nil {
		t.Fatal(err)
	}
	oldSnapshot, _ := host.Snapshot()
	oldRevision, _ := host.ActiveCompositionRevision()
	oldOwner := stable.lastOwner
	lease, err := host.AcquireLease(context.Background(), LeaseRequest{RunID: "run-old", CompositionRevision: oldRevision, ModuleID: "stable", ModuleRevision: Version{Major: 1}, Scope: "tenant:old/path"})
	if err != nil {
		t.Fatal(err)
	}
	var replacementLog []string
	replacement := moduleForHost("stable", &replacementLog)
	replacement.manifest.Version = Version{Major: 2}
	replacement.activateErr = errors.New("replacement activation failed")
	if err := host.ApplyModules(context.Background(), []Module{replacement}); err == nil {
		t.Fatal("failed replacement unexpectedly succeeded")
	}
	currentSnapshot, _ := host.Snapshot()
	currentRevision, _ := host.ActiveCompositionRevision()
	if currentSnapshot.Revision() != oldSnapshot.Revision() || currentRevision != oldRevision {
		t.Fatalf("failed replacement changed active generation: snapshot=%s/%s revision=%s/%s", oldSnapshot.Revision(), currentSnapshot.Revision(), oldRevision, currentRevision)
	}
	if state, _ := host.State("stable"); state != StateActive {
		t.Fatalf("failed replacement changed module state=%s", state)
	}
	if replacement.lastOwner == nil || !containsLog(replacementLog, "deactivate:stable") {
		t.Fatalf("failed candidate owner was not deactivated: owner=%v log=%v", replacement.lastOwner, replacementLog)
	}
	if err := lease.Release(context.Background(), host); err != nil {
		t.Fatal(err)
	}
	oldOwner.mu.Lock()
	released := oldOwner.lastRelease
	oldOwner.mu.Unlock()
	if released.Token != lease.Identity.LeaseID {
		t.Fatalf("owner did not receive original opaque token: got=%q want=%q", released.Token, lease.Identity.LeaseID)
	}
	if err := lease.Release(context.Background(), host); !errors.Is(err, ErrLeaseNotFound) {
		t.Fatalf("second old lease release err=%v", err)
	}
}

func TestModuleHostUsesExplicitAPIAndActivationIsIdempotent(t *testing.T) {
	module := moduleForHost("api-module", nil)
	module.manifest.CompatibleAPI = VersionRange{Min: Version{Major: 2}}
	journal := NewMemoryEffectJournal()
	if _, err := NewModuleHost(Version{Major: 1}, []Module{module}, journal, &inverseRecorder{}); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("incompatible API err=%v", err)
	}
	module.manifest.CompatibleAPI = VersionRange{Min: Version{Major: 1}}
	host, _, _ := newHost(t, module)
	if err := host.Activate(context.Background()); err != nil {
		t.Fatal(err)
	}
	firstComposition, _ := host.ActiveCompositionRevision()
	if err := host.Activate(context.Background()); err != nil {
		t.Fatal(err)
	}
	secondComposition, _ := host.ActiveCompositionRevision()
	if firstComposition != secondComposition {
		t.Fatalf("idempotent activate changed composition: %q != %q", firstComposition, secondComposition)
	}
	otherHost, _, _ := newHost(t, moduleForHost("api-module", nil))
	if err := otherHost.Activate(context.Background()); err != nil {
		t.Fatal(err)
	}
	otherComposition, _ := otherHost.ActiveCompositionRevision()
	if otherComposition == firstComposition {
		t.Fatalf("host-issued composition revision unexpectedly reused: %q", firstComposition)
	}
}

func TestNewModuleHostAllowsWaitingDependencies(t *testing.T) {
	consumer := moduleForHost("waiting-consumer", nil)
	consumer.manifest.Requires = []Dependency{{ModuleID: "waiting-provider", Contract: "catalog", Version: VersionRange{Min: Version{Major: 1}}}}
	provider := moduleForHost("waiting-provider", nil)
	provider.manifest.Provides = []ProvidedExtension{extension("waiting-provider.catalog", SemanticCollection, "catalog", 0)}
	host, err := NewModuleHost(Version{Major: 1}, []Module{consumer}, NewMemoryEffectJournal(), &inverseRecorder{})
	if err != nil {
		t.Fatal(err)
	}
	if state, _ := host.State("waiting-consumer"); state != StateWaitingDependencies {
		t.Fatalf("initial missing dependency state=%s", state)
	}
	if err := host.ApplyModules(context.Background(), []Module{consumer, provider}); err != nil {
		t.Fatal(err)
	}
	if state, _ := host.State("waiting-consumer"); state != StateActive {
		t.Fatalf("dependency installation state=%s", state)
	}
}

func TestLeaseScopeAcceptsCanonicalOpaqueForm(t *testing.T) {
	if err := validateLeaseRequest(LeaseRequest{RunID: "run", CompositionRevision: "comp", ModuleID: "module", ModuleRevision: Version{Major: 1}, Scope: "tenant:abc/path"}); err != nil {
		t.Fatalf("canonical opaque scope rejected: %v", err)
	}
	if err := validateLeaseRequest(LeaseRequest{RunID: "run", CompositionRevision: "comp", ModuleID: "module", ModuleRevision: Version{Major: 1}, Scope: "tenant\x00abc"}); err == nil {
		t.Fatal("control scope accepted")
	}
}

func TestActivationTransactionRequiresDeclaredForwardEffect(t *testing.T) {
	journal := NewMemoryEffectJournal()
	manifest := ModuleManifest{ID: "module", Version: Version{Major: 1}}
	tx := newActivationTransaction(journal, manifest, "comp", EffectPhaseActivate)
	descriptor := EffectDescriptor{ID: "effect", ModuleID: "module", ModuleRevision: Version{Major: 1}, CompositionRevision: "comp", Phase: EffectPhaseActivate, Forward: EffectAction{Kind: "forward", Target: "module"}, Inverse: EffectAction{Kind: "inverse", Target: "module"}}
	if _, err := tx.Record(context.Background(), descriptor); !errors.Is(err, ErrInvalidEffect) {
		t.Fatalf("undeclared effect err=%v", err)
	}
	manifest.Effects = []EffectKind{"forward"}
	tx = newActivationTransaction(journal, manifest, "comp", EffectPhaseActivate)
	if _, err := tx.Record(context.Background(), descriptor); !errors.Is(err, ErrInvalidEffect) {
		t.Fatalf("undeclared inverse effect err=%v", err)
	}
	manifest.Effects = []EffectKind{"forward", "inverse"}
	tx = newActivationTransaction(journal, manifest, "comp", EffectPhaseActivate)
	if _, err := tx.Record(context.Background(), descriptor); err != nil {
		t.Fatalf("declared forward/inverse effect rejected: %v", err)
	}
}

func TestModuleHostDeactivationOrdersCurrentBeforeInherited(t *testing.T) {
	a := moduleForHost("ordered-a", nil)
	a.effects = 1
	host, _, inverse := newHost(t, a)
	if err := host.Activate(context.Background()); err != nil {
		t.Fatal(err)
	}
	b := moduleForHost("ordered-b", nil)
	b.effects = 1
	if err := host.ApplyModules(context.Background(), []Module{a, b}); err != nil {
		t.Fatal(err)
	}
	revision, _ := host.ActiveCompositionRevision()
	if err := host.Deactivate(context.Background(), revision); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(inverse.ids, []EffectID{"ordered-b-effect-0", "ordered-a-effect-0"}) {
		t.Fatalf("inverse order=%v", inverse.ids)
	}
}

type malformedOrdinalJournal struct{ *MemoryEffectJournal }

func (journal *malformedOrdinalJournal) List(ctx context.Context, revision string) ([]RecordedEffect, error) {
	records, err := journal.MemoryEffectJournal.List(ctx, revision)
	if err == nil && len(records) > 0 {
		records[0].Ordinal = 0
	}
	return records, err
}

func TestModuleHostRejectsMalformedCurrentEffectOrdinal(t *testing.T) {
	journal := &malformedOrdinalJournal{MemoryEffectJournal: NewMemoryEffectJournal()}
	module := moduleForHost("malformed", nil)
	module.effects = 1
	host, err := NewModuleHost(Version{Major: 1}, []Module{module}, journal, &inverseRecorder{})
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Activate(context.Background()); err != nil {
		t.Fatal(err)
	}
	revision, _ := host.ActiveCompositionRevision()
	if err := host.Deactivate(context.Background(), revision); !errors.Is(err, ErrInvalidEffect) {
		t.Fatalf("malformed current ordinal err=%v", err)
	}
}

func TestEffectUnknownIsRevertedIdempotentlyAndTransactionsAreScoped(t *testing.T) {
	journal := NewMemoryEffectJournal()
	descriptor := EffectDescriptor{
		ID: "effect-1", ModuleID: "module", ModuleRevision: Version{Major: 1}, CompositionRevision: "comp-1", Phase: EffectPhaseActivate,
		Forward: EffectAction{Kind: "forward", Target: "module"}, Inverse: EffectAction{Kind: "inverse", Target: "module"},
	}
	if _, err := journal.Record(context.Background(), descriptor); err != nil {
		t.Fatal(err)
	}
	if err := journal.MarkUnknown(context.Background(), descriptor.ID); err != nil {
		t.Fatal(err)
	}
	inverse := &inverseRecorder{}
	records, _ := journal.List(context.Background(), "comp-1")
	if err := inverse.ExecuteInverse(context.Background(), records[0]); err != nil {
		t.Fatal(err)
	}
	if err := journal.MarkReverted(context.Background(), descriptor.ID); err != nil {
		t.Fatal(err)
	}
	if err := journal.MarkReverted(context.Background(), descriptor.ID); err != nil {
		t.Fatalf("revert should be idempotent: %v", err)
	}
	tx := &activationTransaction{journal: journal, moduleID: "module", moduleRevision: Version{Major: 1}, compositionRevision: "comp-2", phase: EffectPhaseActivate}
	if err := tx.MarkApplied(context.Background(), descriptor.ID); !errors.Is(err, ErrInvalidEffect) {
		t.Fatalf("unrecorded effect mutation err=%v", err)
	}
	if _, err := tx.Record(context.Background(), EffectDescriptor{ID: "wrong-phase", ModuleID: "module", ModuleRevision: Version{Major: 1}, CompositionRevision: "comp-2", Phase: EffectPhaseReconcile, Forward: EffectAction{Kind: "forward", Target: "module"}, Inverse: EffectAction{Kind: "inverse", Target: "module"}}); !errors.Is(err, ErrInvalidEffect) {
		t.Fatalf("phase escape err=%v", err)
	}
}

func containsLog(log []string, value string) bool {
	for _, item := range log {
		if item == value {
			return true
		}
	}
	return false
}
