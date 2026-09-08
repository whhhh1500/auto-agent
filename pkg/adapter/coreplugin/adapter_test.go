package coreplugin

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/runtime"
)

type testPlugin struct {
	manifest     core.PluginManifest
	installs     atomic.Int32
	unmounts     atomic.Int32
	panicInstall bool
	install      func(context.Context, *core.PluginMount) error
	cleanupErr   error
	cleanup      func(context.Context) error
}

func (plugin *testPlugin) Manifest() core.PluginManifest { return plugin.manifest }
func (plugin *testPlugin) Install(ctx context.Context, mount *core.PluginMount) error {
	plugin.installs.Add(1)
	mount.OnUnmount(func(context.Context) error { plugin.unmounts.Add(1); return nil })
	if plugin.cleanup != nil {
		mount.OnUnmount(plugin.cleanup)
	} else if plugin.cleanupErr != nil {
		mount.OnUnmount(func(context.Context) error { return plugin.cleanupErr })
	}
	if plugin.panicInstall {
		panic("install")
	}
	if plugin.install != nil {
		return plugin.install(ctx, mount)
	}
	return nil
}

func testManifest(id runtime.ModuleID, extension runtime.ExtensionID) runtime.ModuleManifest {
	return runtime.ModuleManifest{ID: id, Version: runtime.Version{Major: 1}, CompatibleAPI: runtime.VersionRange{Min: runtime.Version{Major: 1}}, Provides: []runtime.ProvidedExtension{{ID: extension, Semantic: runtime.SemanticCollection, Contract: "legacy.contract", Version: runtime.Version{Major: 1}}}}
}
func testScope(t *testing.T) core.ScopePath {
	t.Helper()
	return core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
}
func testHost() core.PluginHost {
	return core.PluginHost{Capabilities: core.NewCapabilityRegistry(), Profiles: core.NewAgentProfileRegistry()}
}

func TestNewRejectsDurableEffectsAndClonesDeclaredManifest(t *testing.T) {
	plugin := &testPlugin{manifest: core.PluginManifest{ID: "legacy.declared", Version: "1.0.0"}}
	withEffect := testManifest("legacy.declared", "legacy.declared.extension")
	withEffect.Effects = []runtime.EffectKind{"legacy.external"}
	if _, err := New(testHost(), testScope(t), plugin, withEffect); !errors.Is(err, runtime.ErrInvalidManifest) {
		t.Fatalf("durable effects error=%v", err)
	}
	declared := testManifest("legacy.declared", "legacy.declared.extension")
	module, err := New(testHost(), testScope(t), plugin, declared)
	if err != nil {
		t.Fatal(err)
	}
	declared.Provides[0].ID = "caller.mutation"
	returned := module.Manifest()
	returned.Provides[0].ID = "returned.mutation"
	if got := module.Manifest().Provides[0].ID; got != "legacy.declared.extension" {
		t.Fatalf("manifest was not cloned: %q", got)
	}
}

func TestLegacyPluginLeaseDrainDeactivateAndUnmount(t *testing.T) {
	plugin := &testPlugin{manifest: core.PluginManifest{ID: "legacy.plugin", Version: "1.0.0"}}
	module, err := New(testHost(), testScope(t), plugin, testManifest("legacy.plugin", "legacy.extension"))
	if err != nil {
		t.Fatal(err)
	}
	host, err := runtime.OpenModuleHost(context.Background(), runtime.Version{Major: 1}, []runtime.Module{module}, runtime.NewMemoryEffectJournal(), inverse{}, runtime.NewMemoryCompositionStore())
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Activate(context.Background()); err != nil {
		t.Fatal(err)
	}
	revision, _ := host.ActiveCompositionRevision()
	lease, err := host.AcquireLease(context.Background(), runtime.LeaseRequest{RunID: "run", CompositionRevision: revision, ModuleID: "legacy.plugin", ModuleRevision: runtime.Version{Major: 1}, Scope: "tenant/a"})
	if err != nil {
		t.Fatal(err)
	}
	if lease.Identity.HostGeneration == 0 {
		t.Fatal("durable host generation was not echoed by the owner")
	}
	if _, err := host.Drain(context.Background(), revision); !errors.Is(err, runtime.ErrLeasesRemaining) {
		t.Fatalf("drain with lease=%v", err)
	}
	if err := host.ReleaseLease(context.Background(), lease); err != nil {
		t.Fatal(err)
	}
	if _, err := host.Drain(context.Background(), revision); err != nil {
		t.Fatal(err)
	}
	if plugin.installs.Load() != 1 || plugin.unmounts.Load() != 1 {
		t.Fatalf("installs=%d unmounts=%d", plugin.installs.Load(), plugin.unmounts.Load())
	}
}

func TestLegacyPluginManifestMismatchAndDuplicatePreflightDoNotLeaveInstall(t *testing.T) {
	cleanupErr := errors.New("mismatch cleanup failed")
	mismatch := &testPlugin{manifest: core.PluginManifest{ID: "other.plugin", Version: "1.0.0"}, cleanupErr: cleanupErr}
	module, err := New(testHost(), testScope(t), mismatch, testManifest("legacy.plugin", "legacy.extension"))
	if err != nil {
		t.Fatal(err)
	}
	host, err := runtime.NewModuleHost(runtime.Version{Major: 1}, []runtime.Module{module}, runtime.NewMemoryEffectJournal(), inverse{})
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Activate(context.Background()); err == nil {
		t.Fatal("mismatch activated")
	} else if !errors.Is(err, cleanupErr) {
		t.Fatalf("mismatch cleanup error was lost: %v", err)
	}
	if mismatch.unmounts.Load() != 1 {
		t.Fatalf("mismatch was not closed: %d", mismatch.unmounts.Load())
	}
	first := &testPlugin{manifest: core.PluginManifest{ID: "legacy.one", Version: "1.0.0"}}
	left, _ := New(testHost(), testScope(t), first, testManifest("legacy.one", "duplicate.extension"))
	right := failingModule{manifest: testManifest("new.module", "duplicate.extension")}
	if _, err := runtime.NewModuleHost(runtime.Version{Major: 1}, []runtime.Module{left, right}, runtime.NewMemoryEffectJournal(), inverse{}); !errors.Is(err, runtime.ErrSemanticConflict) {
		t.Fatalf("duplicate preflight error=%v", err)
	}
	if first.installs.Load() != 0 {
		t.Fatalf("duplicate preflight installed legacy plugin: %d", first.installs.Load())
	}
}

func TestLegacyPluginFenceIsIdempotent(t *testing.T) {
	plugin := &testPlugin{manifest: core.PluginManifest{ID: "legacy.fence", Version: "1.0.0"}}
	module, err := New(testHost(), testScope(t), plugin, testManifest("legacy.fence", "legacy.fence.extension"))
	if err != nil {
		t.Fatal(err)
	}
	host, err := runtime.NewModuleHostWithControls(runtime.Version{Major: 1}, []runtime.Module{module}, runtime.NewMemoryEffectJournal(), inverse{}, runtime.HostControls{
		FenceAuthorizer: fenceAuthorizer{}, FenceJournal: runtime.NewMemoryFenceJournal(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Activate(context.Background()); err != nil {
		t.Fatal(err)
	}
	revision, _ := host.ActiveCompositionRevision()
	command := runtime.FenceCommand{RequestID: "legacy-fence-request", CompositionRevision: revision, ActorID: "admin", Reason: "test"}
	if _, err := host.FenceAuthorized(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	if _, err := host.FenceAuthorized(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	if plugin.unmounts.Load() != 1 {
		t.Fatalf("fence unmounts=%d", plugin.unmounts.Load())
	}
}

func TestLegacyPluginFenceAndDeactivateReplayCloseFailure(t *testing.T) {
	cleanupErr := errors.New("cleanup failed")
	cleanupStarted := make(chan struct{})
	finishCleanup := make(chan struct{})
	plugin := &testPlugin{manifest: core.PluginManifest{ID: "legacy.close", Version: "1.0.0"}, cleanup: func(context.Context) error {
		close(cleanupStarted)
		<-finishCleanup
		return cleanupErr
	}}
	module, err := New(testHost(), testScope(t), plugin, testManifest("legacy.close", "legacy.close.extension"))
	if err != nil {
		t.Fatal(err)
	}
	staged, err := module.Stage(context.Background(), runtime.StageContext{ModuleID: "legacy.close", ModuleRevision: runtime.Version{Major: 1}, CompositionRevision: "composition"})
	if err != nil {
		t.Fatal(err)
	}
	activated, err := staged.Activate(context.Background(), transaction{})
	if err != nil {
		t.Fatal(err)
	}
	owner := activated.(*owner)
	if _, err := owner.Drain(context.Background(), runtime.DrainRequest{CompositionRevision: "other"}); !errors.Is(err, runtime.ErrLeaseMismatch) {
		t.Fatalf("wrong drain composition error=%v", err)
	}
	if err := owner.Fence(context.Background(), runtime.FenceRequest{CompositionRevision: "composition"}); !errors.Is(err, runtime.ErrLeaseMismatch) {
		t.Fatalf("empty fence request id error=%v", err)
	}
	request := runtime.FenceRequest{RequestID: "close-failure", CompositionRevision: "composition", Reason: "test"}
	fenceResult := make(chan error, 1)
	go func() { fenceResult <- owner.Fence(context.Background(), request) }()
	<-cleanupStarted
	if err := owner.Fence(context.Background(), runtime.FenceRequest{RequestID: request.RequestID, CompositionRevision: request.CompositionRevision, Reason: "different"}); !errors.Is(err, runtime.ErrLeaseMismatch) {
		t.Fatalf("different fence command error=%v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	deactivateResult := make(chan error, 1)
	go func() { deactivateResult <- owner.Deactivate(canceled) }()
	cancel()
	if err := <-deactivateResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled deactivate error=%v", err)
	}
	canceledFence, cancelFence := context.WithCancel(context.Background())
	fenceWait := make(chan error, 1)
	go func() { fenceWait <- owner.Fence(canceledFence, request) }()
	cancelFence()
	if err := <-fenceWait; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled fence retry error=%v", err)
	}
	close(finishCleanup)
	if err := <-fenceResult; !errors.Is(err, cleanupErr) {
		t.Fatalf("first fence error=%v", err)
	}
	if err := owner.Fence(context.Background(), request); !errors.Is(err, cleanupErr) {
		t.Fatalf("fence retry error=%v", err)
	}
	if err := owner.Deactivate(context.Background()); !errors.Is(err, cleanupErr) {
		t.Fatalf("deactivate retry error=%v", err)
	}
	if plugin.unmounts.Load() != 1 {
		t.Fatalf("close was repeated: unmounts=%d", plugin.unmounts.Load())
	}
}

func TestLegacyPluginDurableRecoveryFailsClosedWithoutReinstall(t *testing.T) {
	plugin := &testPlugin{manifest: core.PluginManifest{ID: "legacy.recovery", Version: "1.0.0"}}
	module, err := New(testHost(), testScope(t), plugin, testManifest("legacy.recovery", "legacy.recovery.extension"))
	if err != nil {
		t.Fatal(err)
	}
	store := runtime.NewMemoryCompositionStore()
	journal := runtime.NewMemoryEffectJournal()
	first, err := runtime.OpenModuleHost(context.Background(), runtime.Version{Major: 1}, []runtime.Module{module}, journal, inverse{}, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Activate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	restartedPlugin := &testPlugin{manifest: core.PluginManifest{ID: "legacy.recovery", Version: "1.0.0"}}
	restarted, err := New(testHost(), testScope(t), restartedPlugin, testManifest("legacy.recovery", "legacy.recovery.extension"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.RecoverModuleHost(context.Background(), runtime.Version{Major: 1}, []runtime.Module{restarted}, journal, inverse{}, store); !errors.Is(err, runtime.ErrRecoveryBlocked) {
		t.Fatalf("recover error=%v", err)
	}
	if restartedPlugin.installs.Load() != 0 {
		t.Fatalf("recovery reinstalled legacy plugin %d times", restartedPlugin.installs.Load())
	}
}

func TestLegacyPluginRollsBackWhenFollowingModuleFails(t *testing.T) {
	plugin := &testPlugin{manifest: core.PluginManifest{ID: "legacy.rollback", Version: "1.0.0"}}
	legacy, err := New(testHost(), testScope(t), plugin, testManifest("legacy.rollback", "legacy.rollback.extension"))
	if err != nil {
		t.Fatal(err)
	}
	following := failingModule{manifest: testNoProvideManifest("zz.following"), stageErr: errors.New("following stage failed")}
	host, err := runtime.NewModuleHost(runtime.Version{Major: 1}, []runtime.Module{legacy, following}, runtime.NewMemoryEffectJournal(), inverse{})
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Activate(context.Background()); err == nil {
		t.Fatal("activation unexpectedly succeeded")
	}
	if plugin.installs.Load() != 1 || plugin.unmounts.Load() != 1 {
		t.Fatalf("rollback installs=%d unmounts=%d", plugin.installs.Load(), plugin.unmounts.Load())
	}
}

func TestLegacyPluginInstallPanicAndRegistryInstallFailureRollBack(t *testing.T) {
	t.Run("panic", func(t *testing.T) {
		plugin := &testPlugin{manifest: core.PluginManifest{ID: "legacy.panic", Version: "1.0.0"}, panicInstall: true}
		module, err := New(testHost(), testScope(t), plugin, testManifest("legacy.panic", "legacy.panic.extension"))
		if err != nil {
			t.Fatal(err)
		}
		host, err := runtime.NewModuleHost(runtime.Version{Major: 1}, []runtime.Module{module}, runtime.NewMemoryEffectJournal(), inverse{})
		if err != nil {
			t.Fatal(err)
		}
		if err := host.Activate(context.Background()); err == nil {
			t.Fatal("install panic activated")
		}
		if plugin.unmounts.Load() != 1 {
			t.Fatalf("panic rollback unmounts=%d", plugin.unmounts.Load())
		}
	})
	t.Run("registry failure", func(t *testing.T) {
		registryHost := testHost()
		plugin := &testPlugin{manifest: core.PluginManifest{ID: "legacy.registry", Version: "1.0.0"}}
		plugin.install = func(_ context.Context, mount *core.PluginMount) error {
			if err := mount.Capability(staticCapability{manifest: testCapabilityManifest("legacy.registry.tool")}); err != nil {
				return err
			}
			return mount.Capability(nil)
		}
		module, err := New(registryHost, testScope(t), plugin, testManifest("legacy.registry", "legacy.registry.extension"))
		if err != nil {
			t.Fatal(err)
		}
		host, err := runtime.NewModuleHost(runtime.Version{Major: 1}, []runtime.Module{module}, runtime.NewMemoryEffectJournal(), inverse{})
		if err != nil {
			t.Fatal(err)
		}
		if err := host.Activate(context.Background()); err == nil {
			t.Fatal("registry install failure activated")
		}
		entries, err := registryHost.Capabilities.Entries(testScope(t))
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 0 || plugin.unmounts.Load() != 1 {
			t.Fatalf("registry rollback entries=%d unmounts=%d", len(entries), plugin.unmounts.Load())
		}
	})
}

type staticCapability struct{ manifest core.CapabilityManifest }

func (capability staticCapability) Manifest() core.CapabilityManifest { return capability.manifest }
func (staticCapability) Execute(context.Context, core.CapabilityRequest) (core.CapabilityResult, error) {
	return core.CapabilityResult{OK: true}, nil
}

func testCapabilityManifest(id string) core.CapabilityManifest {
	return core.CapabilityManifest{ID: id, Version: "1.0.0", Name: id, Kind: core.KindTool, Contract: "legacy.tool/v1"}
}

func testNoProvideManifest(id runtime.ModuleID) runtime.ModuleManifest {
	return runtime.ModuleManifest{ID: id, Version: runtime.Version{Major: 1}, CompatibleAPI: runtime.VersionRange{Min: runtime.Version{Major: 1}}}
}

type failingModule struct {
	manifest runtime.ModuleManifest
	stageErr error
}

func (module failingModule) Manifest() runtime.ModuleManifest { return module.manifest.Clone() }
func (module failingModule) Stage(context.Context, runtime.StageContext) (runtime.StagedModule, error) {
	if module.stageErr != nil {
		return nil, module.stageErr
	}
	return nil, errors.New("failing module has no staged implementation")
}

type fenceAuthorizer struct{}

func (fenceAuthorizer) AuthorizeFence(context.Context, runtime.FenceCommand) error { return nil }

type inverse struct{}

func (inverse) ExecuteInverse(context.Context, runtime.RecordedEffect) error { return nil }

type transaction struct{}

func (transaction) Record(context.Context, runtime.EffectDescriptor) (runtime.EffectID, error) {
	return "legacy-test", nil
}
func (transaction) MarkApplied(context.Context, runtime.EffectID) error { return nil }
func (transaction) MarkUnknown(context.Context, runtime.EffectID) error { return nil }
