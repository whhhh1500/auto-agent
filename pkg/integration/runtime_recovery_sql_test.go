package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/whhhh1500/auto-agent/pkg/adapter/sql/compositionstore"
	"github.com/whhhh1500/auto-agent/pkg/adapter/sql/effectjournal"
	"github.com/whhhh1500/auto-agent/pkg/adapter/sql/sqlkit"
	"github.com/whhhh1500/auto-agent/pkg/runtime"
	"github.com/whhhh1500/auto-agent/pkg/storage"

	_ "modernc.org/sqlite"
)

func TestSQLRuntimeRecoveryAcrossDatabaseHandleRestart(t *testing.T) {
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "runtime-recovery.db")

	db1 := openRuntimeRecoveryDB(t, databasePath)
	if _, err := storage.OpenSQLSessionStore(ctx, db1, storage.SQLDialectSQLite); err != nil {
		t.Fatal(err)
	}
	composition1 := newRuntimeRecoveryCompositionStore(t, db1)
	journal1 := newRuntimeRecoveryEffectJournal(t, db1)
	inverse := newRuntimeRecoveryInverse()
	initial := newRuntimeRecoveryModule()
	first, err := runtime.OpenModuleHost(ctx, runtime.Version{Major: 1}, []runtime.Module{initial}, journal1, inverse, composition1)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Activate(ctx); err != nil {
		t.Fatal(err)
	}
	compositionRevision, found := first.ActiveCompositionRevision()
	if !found {
		t.Fatal("first host has no active composition")
	}
	firstSnapshot, found := first.Snapshot()
	if !found {
		t.Fatal("first host has no active snapshot")
	}
	oldLease, err := first.AcquireLease(ctx, runtime.LeaseRequest{
		RunID: "old-run", CompositionRevision: compositionRevision,
		ModuleID: initial.manifest.ID, ModuleRevision: initial.manifest.Version, Scope: "tenant/a",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := initial.activateCalls.Load(); got != 1 {
		t.Fatalf("first activation calls=%d, want 1", got)
	}
	if err := first.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := db1.Close(); err != nil {
		t.Fatal(err)
	}

	// A fresh database handle simulates the persistence boundary: neither
	// adapters nor public leases are reused. This is not an OS process test.
	db2 := openRuntimeRecoveryDB(t, databasePath)
	defer db2.Close()
	if _, err := storage.OpenSQLSessionStore(ctx, db2, storage.SQLDialectSQLite); err != nil {
		t.Fatal(err)
	}
	composition2 := newRuntimeRecoveryCompositionStore(t, db2)
	journal2 := newRuntimeRecoveryEffectJournal(t, db2)
	restarted := newRuntimeRecoveryModule()
	recovered, err := runtime.RecoverModuleHost(ctx, runtime.Version{Major: 1}, []runtime.Module{restarted}, journal2, inverse, composition2)
	if err != nil {
		t.Fatal(err)
	}
	if got := restarted.activateCalls.Load(); got != 0 {
		t.Fatalf("recovery called Activate %d times, want 0", got)
	}
	if got := restarted.recoverCalls.Load(); got != 1 {
		t.Fatalf("recovery calls=%d, want 1", got)
	}
	if !restarted.priorLeasesLost.Load() {
		t.Fatal("recovery context did not mark prior leases lost")
	}
	if got := restarted.reconcileCalls.Load(); got != 1 {
		t.Fatalf("recovery reconciliation calls=%d, want 1", got)
	}
	recoveredRevision, found := recovered.ActiveCompositionRevision()
	if !found || recoveredRevision != compositionRevision {
		t.Fatalf("recovered composition=%q found=%v, want %q", recoveredRevision, found, compositionRevision)
	}
	recoveredSnapshot, found := recovered.Snapshot()
	if !found || recoveredSnapshot.Revision() != firstSnapshot.Revision() {
		t.Fatalf("recovered snapshot=%q found=%v, want %q", recoveredSnapshot.Revision(), found, firstSnapshot.Revision())
	}
	if err := recovered.ReleaseLease(ctx, oldLease); !errors.Is(err, runtime.ErrLeaseNotFound) {
		t.Fatalf("old public lease release error=%v, want ErrLeaseNotFound", err)
	}
	newLease, err := recovered.AcquireLease(ctx, runtime.LeaseRequest{
		RunID: "new-run", CompositionRevision: compositionRevision,
		ModuleID: restarted.manifest.ID, ModuleRevision: restarted.manifest.Version, Scope: "tenant/a",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := recovered.ReleaseLease(ctx, newLease); err != nil {
		t.Fatalf("release recovered lease: %v", err)
	}

	// Drain owns the final deactivation when no leases remain.
	drain, err := recovered.Drain(ctx, compositionRevision)
	if err != nil {
		t.Fatal(err)
	}
	if !drain.Completed || drain.RemainingLeases != 0 {
		t.Fatalf("drain=%#v, want completed without leases", drain)
	}
	state, found, err := composition2.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("durable host state disappeared")
	}
	if len(state.Compositions) != 0 {
		t.Fatalf("durable compositions remain after drain: %#v", state.Compositions)
	}
	records, err := journal2.List(ctx, compositionRevision)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("effect record count=%d, want 1", len(records))
	}
	if records[0].State != runtime.EffectReverted {
		t.Fatalf("effect state=%q, want %q", records[0].State, runtime.EffectReverted)
	}
	if calls := inverse.calls(records[0].Descriptor.ID); calls != 1 {
		t.Fatalf("inverse calls for %q=%d, want 1", records[0].Descriptor.ID, calls)
	}
	if executions := inverse.executions(records[0].Descriptor.ID); executions != 1 {
		t.Fatalf("inverse executions for %q=%d, want 1", records[0].Descriptor.ID, executions)
	}
}

func openRuntimeRecoveryDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func newRuntimeRecoveryCompositionStore(t *testing.T, db *sql.DB) *compositionstore.Store {
	t.Helper()
	store, err := compositionstore.New(db, sqlkit.SQLite)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func newRuntimeRecoveryEffectJournal(t *testing.T, db *sql.DB) *effectjournal.Store {
	t.Helper()
	store, err := effectjournal.New(db, sqlkit.SQLite)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

type runtimeRecoveryModule struct {
	manifest        runtime.ModuleManifest
	activateCalls   atomic.Int32
	recoverCalls    atomic.Int32
	reconcileCalls  atomic.Int32
	priorLeasesLost atomic.Bool
}

func newRuntimeRecoveryModule() *runtimeRecoveryModule {
	return &runtimeRecoveryModule{manifest: runtime.ModuleManifest{
		ID: "runtime-recovery", Version: runtime.Version{Major: 1},
		Effects: []runtime.EffectKind{"forward", "inverse"},
	}}
}

func (module *runtimeRecoveryModule) Manifest() runtime.ModuleManifest {
	return module.manifest.Clone()
}

func (module *runtimeRecoveryModule) Stage(_ context.Context, stage runtime.StageContext) (runtime.StagedModule, error) {
	return runtimeRecoveryStaged{module: module, stage: stage}, nil
}

type runtimeRecoveryStaged struct {
	module *runtimeRecoveryModule
	stage  runtime.StageContext
}

func (runtimeRecoveryStaged) Health(context.Context) error { return nil }

func (staged runtimeRecoveryStaged) Activate(ctx context.Context, transaction runtime.ActivationTransaction) (runtime.ModuleLeaseOwner, error) {
	staged.module.activateCalls.Add(1)
	descriptor := runtime.EffectDescriptor{
		ID: "runtime-recovery-effect", ModuleID: staged.module.manifest.ID, ModuleRevision: staged.module.manifest.Version,
		CompositionRevision: staged.stage.CompositionRevision, Phase: runtime.EffectPhaseActivate,
		Forward: runtime.EffectAction{Kind: "forward", Target: "runtime-recovery", Payload: nil},
		Inverse: runtime.EffectAction{Kind: "inverse", Target: "runtime-recovery", Payload: []byte{}},
	}
	effectID, err := transaction.Record(ctx, descriptor)
	if err != nil {
		return nil, err
	}
	if err := transaction.MarkApplied(ctx, effectID); err != nil {
		return nil, err
	}
	return runtimeRecoveryOwner{module: staged.module}, nil
}

func (staged runtimeRecoveryStaged) Recover(_ context.Context, recovery runtime.RecoveryContext) (runtime.ModuleLeaseOwner, error) {
	if !recovery.PriorLeasesLost {
		return nil, errors.New("prior leases were not marked lost")
	}
	if len(recovery.Effects) != 1 || recovery.Effects[0].Descriptor.ID != "runtime-recovery-effect" {
		return nil, errors.New("recovery did not receive the activation effect")
	}
	staged.module.priorLeasesLost.Store(true)
	staged.module.recoverCalls.Add(1)
	return runtimeRecoveryOwner{module: staged.module}, nil
}

type runtimeRecoveryOwner struct{ module *runtimeRecoveryModule }

func (owner runtimeRecoveryOwner) AcquireLease(_ context.Context, request runtime.LeaseRequest) (runtime.Lease, error) {
	return runtime.Lease{Identity: runtime.LeaseIdentity{
		LeaseID: request.LeaseID, RunID: request.RunID, CompositionRevision: request.CompositionRevision,
		ModuleID: request.ModuleID, ModuleRevision: request.ModuleRevision, Scope: request.Scope,
	}, Token: "owner-" + request.LeaseID}, nil
}

func (runtimeRecoveryOwner) ReleaseLease(context.Context, runtime.Lease) error { return nil }

func (runtimeRecoveryOwner) Drain(_ context.Context, request runtime.DrainRequest) (runtime.DrainResult, error) {
	return runtime.DrainResult{CompositionRevision: request.CompositionRevision, Completed: true}, nil
}

func (runtimeRecoveryOwner) Fence(context.Context, runtime.FenceRequest) error { return nil }

func (runtimeRecoveryOwner) Deactivate(context.Context) error { return nil }

func (owner runtimeRecoveryOwner) Reconcile(context.Context, runtime.DesiredModuleState, []runtime.RecordedEffect, runtime.ActivationTransaction) error {
	owner.module.reconcileCalls.Add(1)
	return nil
}

type runtimeRecoveryInverse struct {
	mu              sync.Mutex
	callCounts      map[runtime.EffectID]int
	executionCounts map[runtime.EffectID]int
}

func newRuntimeRecoveryInverse() *runtimeRecoveryInverse {
	return &runtimeRecoveryInverse{
		callCounts: make(map[runtime.EffectID]int), executionCounts: make(map[runtime.EffectID]int),
	}
}

func (inverse *runtimeRecoveryInverse) ExecuteInverse(_ context.Context, effect runtime.RecordedEffect) error {
	inverse.mu.Lock()
	defer inverse.mu.Unlock()
	inverse.callCounts[effect.Descriptor.ID]++
	if inverse.executionCounts[effect.Descriptor.ID] != 0 {
		return nil
	}
	inverse.executionCounts[effect.Descriptor.ID] = 1
	return nil
}

func (inverse *runtimeRecoveryInverse) calls(id runtime.EffectID) int {
	inverse.mu.Lock()
	defer inverse.mu.Unlock()
	return inverse.callCounts[id]
}

func (inverse *runtimeRecoveryInverse) executions(id runtime.EffectID) int {
	inverse.mu.Lock()
	defer inverse.mu.Unlock()
	return inverse.executionCounts[id]
}
