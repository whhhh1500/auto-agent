// Package coreplugin adapts a legacy core.Plugin as a process-local runtime
// Module. It is intentionally non-recoverable: legacy Install has no durable
// effect evidence, so a restart must fail closed rather than replay Install.
// HostGeneration is echoed in leases for host compatibility only; this facade
// has no durable activation baseline and cannot itself fence downstream work.
package coreplugin

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"

	"github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/runtime"
)

type Module struct {
	host     core.PluginHost
	scope    core.ScopePath
	plugin   core.Plugin
	manifest runtime.ModuleManifest
}

var _ runtime.Module = (*Module)(nil)

// New binds an explicitly declared runtime manifest. Effects are prohibited:
// legacy Install may only contribute process-local core registries. Plugins
// whose actual registrations differ from Provides violate this trusted legacy
// contract. Core registries allow layered duplicate contributions at mount
// time, so neither an existing registration nor Install's actual registration
// can be preflighted. core.MountPlugin still rolls back prior contributions
// when Install returns an error.
func New(host core.PluginHost, scope core.ScopePath, plugin core.Plugin, manifest runtime.ModuleManifest) (*Module, error) {
	if plugin == nil || scope.Depth() == 0 {
		return nil, fmt.Errorf("core plugin adapter requires plugin and scope")
	}
	if len(manifest.Effects) != 0 {
		return nil, fmt.Errorf("%w: core plugin adapter cannot declare durable effects", runtime.ErrInvalidManifest)
	}
	if err := runtime.ValidateManifest(manifest); err != nil {
		return nil, err
	}
	return &Module{host: host, scope: scope, plugin: plugin, manifest: manifest.Clone()}, nil
}

func (module *Module) Manifest() runtime.ModuleManifest { return module.manifest.Clone() }

func (module *Module) Stage(_ context.Context, stage runtime.StageContext) (runtime.StagedModule, error) {
	return &staged{module: module, stage: stage}, nil
}

type staged struct {
	module *Module
	stage  runtime.StageContext
}

var _ runtime.StagedModule = (*staged)(nil)

func (stage *staged) Health(context.Context) error {
	if stage == nil || stage.module == nil || stage.module.plugin == nil || stage.module.scope.Depth() == 0 ||
		stage.module.host.Capabilities == nil || stage.module.host.Profiles == nil {
		return errors.New("core plugin adapter binding is incomplete")
	}
	return nil
}

func (stage *staged) Activate(ctx context.Context, transaction runtime.ActivationTransaction) (runtime.ModuleLeaseOwner, error) {
	if transaction == nil {
		return nil, errors.New("core plugin adapter activation transaction is nil")
	}
	mounted, err := stage.module.host.MountPlugin(ctx, stage.module.scope, stage.module.plugin)
	if err != nil {
		return nil, err
	}
	if mounted.Manifest.ID != string(stage.module.manifest.ID) || mounted.Manifest.Version != stage.module.manifest.Version.String() {
		mismatch := fmt.Errorf("core plugin manifest %q@%s differs from declared runtime module %q@%s", mounted.Manifest.ID, mounted.Manifest.Version, stage.module.manifest.ID, stage.module.manifest.Version)
		return nil, errors.Join(mismatch, mounted.Close(ctx))
	}
	return &owner{mounted: mounted, moduleID: stage.module.manifest.ID, moduleRevision: stage.module.manifest.Version, compositionRevision: stage.stage.CompositionRevision}, nil
}

type owner struct {
	mu                  sync.Mutex
	mounted             *core.MountedPlugin
	moduleID            runtime.ModuleID
	moduleRevision      runtime.Version
	compositionRevision string
	draining            bool
	leases              map[string]runtime.Lease
	fences              map[string]*fenceResult
	closeDone           chan struct{}
	closeFinished       bool
	closeErr            error
}

type fenceResult struct {
	done                chan struct{}
	compositionRevision string
	reason              string
	hostGeneration      uint64
	err                 error
}

var _ runtime.ModuleLeaseOwner = (*owner)(nil)

func (owner *owner) AcquireLease(_ context.Context, request runtime.LeaseRequest) (runtime.Lease, error) {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.mounted == nil || owner.draining || request.ModuleID != owner.moduleID || request.ModuleRevision != owner.moduleRevision || request.CompositionRevision != owner.compositionRevision || request.LeaseID == "" {
		return runtime.Lease{}, runtime.ErrLeaseMismatch
	}
	token, err := leaseToken()
	if err != nil {
		return runtime.Lease{}, err
	}
	lease := runtime.Lease{Identity: runtime.LeaseIdentity(request), Token: token}
	if owner.leases == nil {
		owner.leases = make(map[string]runtime.Lease)
	}
	owner.leases[request.LeaseID] = lease
	return lease, nil
}

func (owner *owner) ReleaseLease(_ context.Context, lease runtime.Lease) error {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	stored, ok := owner.leases[lease.Identity.LeaseID]
	if !ok || stored.Identity != lease.Identity || lease.Token == "" || subtle.ConstantTimeCompare([]byte(stored.Token), []byte(lease.Token)) != 1 {
		return runtime.ErrLeaseMismatch
	}
	delete(owner.leases, lease.Identity.LeaseID)
	return nil
}

func (owner *owner) Drain(_ context.Context, request runtime.DrainRequest) (runtime.DrainResult, error) {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if request.CompositionRevision != owner.compositionRevision {
		return runtime.DrainResult{}, fmt.Errorf("%w: drain composition does not match owner", runtime.ErrLeaseMismatch)
	}
	owner.draining = true
	return runtime.DrainResult{CompositionRevision: request.CompositionRevision, Completed: len(owner.leases) == 0, RemainingLeases: len(owner.leases)}, nil
}

func (owner *owner) Fence(ctx context.Context, request runtime.FenceRequest) error {
	if ctx == nil {
		ctx = context.Background()
	}
	owner.mu.Lock()
	if request.RequestID == "" || request.CompositionRevision != owner.compositionRevision {
		owner.mu.Unlock()
		return fmt.Errorf("%w: fence identity does not match owner", runtime.ErrLeaseMismatch)
	}
	if owner.fences == nil {
		owner.fences = make(map[string]*fenceResult)
	}
	if result, found := owner.fences[request.RequestID]; found {
		owner.mu.Unlock()
		if result.compositionRevision != request.CompositionRevision || result.reason != request.Reason || result.hostGeneration != request.HostGeneration {
			return fmt.Errorf("%w: fence request id was reused with different command", runtime.ErrLeaseMismatch)
		}
		select {
		case <-result.done:
			return result.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	result := &fenceResult{done: make(chan struct{}), compositionRevision: request.CompositionRevision, reason: request.Reason, hostGeneration: request.HostGeneration}
	owner.fences[request.RequestID] = result
	owner.draining = true
	owner.leases = make(map[string]runtime.Lease)
	owner.mu.Unlock()
	err := owner.close(ctx)
	owner.mu.Lock()
	result.err = err
	close(result.done)
	owner.mu.Unlock()
	return err
}

func (owner *owner) Deactivate(ctx context.Context) error {
	owner.mu.Lock()
	owner.draining = true
	owner.leases = make(map[string]runtime.Lease)
	owner.mu.Unlock()
	return owner.close(ctx)
}

// close serializes external plugin cleanup without holding owner.mu. It caches
// the mounted plugin's result so retrying Fence or Deactivate cannot turn a
// failed cleanup into a false success.
func (owner *owner) close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	owner.mu.Lock()
	if owner.closeFinished {
		err := owner.closeErr
		owner.mu.Unlock()
		return err
	}
	if done := owner.closeDone; done != nil {
		owner.mu.Unlock()
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
		owner.mu.Lock()
		err := owner.closeErr
		owner.mu.Unlock()
		return err
	}
	done := make(chan struct{})
	owner.closeDone = done
	mounted := owner.mounted
	owner.mu.Unlock()
	var err error
	if mounted != nil {
		err = mounted.Close(ctx)
	}
	owner.mu.Lock()
	owner.closeErr = err
	owner.closeFinished = true
	owner.mounted = nil
	close(done)
	owner.mu.Unlock()
	return err
}

func (owner *owner) Reconcile(_ context.Context, desired runtime.DesiredModuleState, records []runtime.RecordedEffect, _ runtime.ActivationTransaction) error {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.mounted == nil || owner.draining || desired.ModuleID != owner.moduleID || desired.CompositionRevision != owner.compositionRevision || desired.State != runtime.StateActive || len(records) != 0 {
		return runtime.ErrRecoveryBlocked
	}
	return nil
}

func leaseToken() (string, error) {
	var entropy [16]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return "", err
	}
	return "legacy-" + hex.EncodeToString(entropy[:]), nil
}
