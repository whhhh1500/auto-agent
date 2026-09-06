package runtime

import (
	"context"
	"strings"
	"testing"
)

type panicLifecycleModule struct {
	base *hostModule
	mode string
}

func (module *panicLifecycleModule) Manifest() ModuleManifest { return module.base.Manifest() }

func (module *panicLifecycleModule) Stage(ctx context.Context, stage StageContext) (StagedModule, error) {
	if module.mode == "stage" {
		panic("stage panic")
	}
	return &panicLifecycleStaged{module: module, stage: stage}, nil
}

type panicLifecycleStaged struct {
	module *panicLifecycleModule
	stage  StageContext
}

func (staged *panicLifecycleStaged) Health(ctx context.Context) error {
	if staged.module.mode == "health" {
		panic("health panic")
	}
	return nil
}

func (staged *panicLifecycleStaged) Activate(ctx context.Context, transaction ActivationTransaction) (ModuleLeaseOwner, error) {
	if staged.module.mode == "activate" {
		panic("activate panic")
	}
	return (&hostStaged{module: staged.module.base, stage: staged.stage}).Activate(ctx, transaction)
}

func TestModuleHostConvertsLifecyclePanicsAndReleasesOperationLock(t *testing.T) {
	for _, mode := range []string{"stage", "health", "activate"} {
		t.Run(mode, func(t *testing.T) {
			log := []string{}
			provider := moduleForHost("panic-provider", &log)
			provider.manifest.Provides = []ProvidedExtension{extension("panic-provider.catalog", SemanticCollection, "catalog", 0)}
			badBase := moduleForHost("panic-module", &log)
			badBase.manifest.Requires = []Dependency{{ModuleID: "panic-provider", Contract: "catalog"}}
			bad := &panicLifecycleModule{base: badBase, mode: mode}
			journal := NewMemoryEffectJournal()
			host, err := NewModuleHost(Version{Major: 1}, []Module{bad, provider}, journal, &inverseRecorder{})
			if err != nil {
				t.Fatal(err)
			}

			err = host.Activate(context.Background())
			if err == nil || !strings.Contains(err.Error(), "panic-module") {
				t.Fatalf("panic mode %q error=%v", mode, err)
			}
			if _, published := host.Snapshot(); published {
				t.Fatal("panicking candidate was published")
			}
			if !containsLog(log, "deactivate:panic-provider") {
				t.Fatalf("activated owner was not rolled back after %s panic: %v", mode, log)
			}

			// A panic must not leave ModuleHost.opMu held. Clear the injected
			// fault and prove a subsequent activation can complete.
			bad.mode = ""
			if err := host.Activate(context.Background()); err != nil {
				t.Fatalf("host remained unusable after %s panic: %v", mode, err)
			}
		})
	}
}

func TestSafeLifecycleWrappersAddOperationContext(t *testing.T) {
	module := &panicLifecycleModule{base: moduleForHost("wrapper-module", nil), mode: "stage"}
	_, err := safeModuleStage(module, context.Background(), StageContext{ModuleID: module.Manifest().ID})
	if err == nil || !strings.Contains(err.Error(), "module stage panicked") {
		t.Fatalf("stage panic error=%v", err)
	}

	staged := &panicLifecycleStaged{module: &panicLifecycleModule{base: moduleForHost("wrapper-module", nil), mode: "health"}}
	if err := safeStagedHealth(staged, context.Background()); err == nil || !strings.Contains(err.Error(), "staged module health panicked") {
		t.Fatalf("health panic error=%v", err)
	}

	staged.module.mode = "activate"
	if _, err := safeStagedActivate(staged, context.Background(), nil); err == nil || !strings.Contains(err.Error(), "staged module activate panicked") {
		t.Fatalf("activate panic error=%v", err)
	}
}
