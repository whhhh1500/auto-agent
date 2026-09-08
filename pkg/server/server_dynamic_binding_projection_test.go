package server

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/whhhh1500/auto-agent/pkg/app/capabilityruntime"
	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/storage"
)

type projectionGateBindingJournal struct {
	reader         *executionProjectionEpochReader
	recordDelta    int64
	deleteDelta    int64
	recordErr      error
	deleteErr      error
	recordAfterErr error
	deleteAfterErr error
	afterRecord    func()
	entered        chan struct{}
	release        <-chan struct{}
	once           sync.Once
}

func (j *projectionGateBindingJournal) Record(ctx context.Context, _ storage.BindingRecord) error {
	if j.entered != nil {
		j.once.Do(func() { close(j.entered) })
	}
	if j.release != nil {
		select {
		case <-j.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if j.recordErr != nil {
		return j.recordErr
	}
	j.advance(j.recordDelta)
	if j.afterRecord != nil {
		j.afterRecord()
	}
	if j.recordAfterErr != nil {
		return j.recordAfterErr
	}
	return nil
}

func (j *projectionGateBindingJournal) Delete(_ context.Context, _ string) error {
	if j.deleteErr != nil {
		return j.deleteErr
	}
	j.advance(j.deleteDelta)
	if j.deleteAfterErr != nil {
		return j.deleteAfterErr
	}
	return nil
}

func (*projectionGateBindingJournal) List(context.Context) ([]storage.BindingRecord, error) {
	return nil, nil
}

func (j *projectionGateBindingJournal) advance(delta int64) {
	if j.reader == nil {
		return
	}
	j.reader.mu.Lock()
	j.reader.epoch += delta
	j.reader.mu.Unlock()
}

func newProjectionBindingServer(t *testing.T, reader *executionProjectionEpochReader, journal storage.BindingJournal) *Server {
	t.Helper()
	server := &Server{
		runtime:             &core.Runtime{Policy: core.NewPolicyRegistry()},
		journal:             journal,
		executionProjection: newExecutionProjectionCoordinator(reader),
	}
	if err := server.MarkExecutionProjectionAppliedEpoch(context.Background()); err != nil {
		t.Fatalf("mark initial epoch applied: %v", err)
	}
	return server
}

func policyProjectionMount(registry *core.PolicyRegistry, scope core.ScopePath, unmounted *int) bindingProjectionMount {
	return func() (func(), error) {
		unmount, err := registry.Mount(core.PolicyLayer{Scope: scope, DenyPermissions: []core.Permission{core.PermWrite}})
		if err != nil {
			return nil, err
		}
		return func() {
			if unmounted != nil {
				*unmounted++
			}
			unmount()
		}, nil
	}
}

type projectionUnsafeDynamicFactory struct{ creates int }

func (*projectionUnsafeDynamicFactory) ID() string                     { return "unsafe" }
func (*projectionUnsafeDynamicFactory) Version() string                { return "1" }
func (*projectionUnsafeDynamicFactory) ImplementationRevision() string { return "test-unsafe/v1" }
func (f *projectionUnsafeDynamicFactory) New(_ context.Context, req capabilityruntime.Request) (capabilityruntime.Result, error) {
	f.creates++
	return capabilityruntime.Result{Manifest: req.Manifest}, nil
}

func mustProjectionLease(t *testing.T, server *Server) {
	t.Helper()
	lease, err := server.acquireExecutionProjection(context.Background())
	if err != nil {
		t.Fatalf("execution admission: %v", err)
	}
	lease.Release()
}

func assertProjectionStillBlocked(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
		t.Fatalf("%s entered while durable binding publication was gated", what)
	case <-time.After(40 * time.Millisecond):
	}
}

func TestDynamicBindingWriteGateBlocksAdmissionAndWorkerUntilDurableCommit(t *testing.T) {
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	reader := &executionProjectionEpochReader{epoch: 11}
	release := make(chan struct{})
	journal := &projectionGateBindingJournal{reader: reader, recordDelta: 1, entered: make(chan struct{}), release: release}
	server := newProjectionBindingServer(t, reader, journal)
	registry := server.policyRegistry()
	result := make(chan error, 1)
	go func() {
		_, err := server.addBinding(context.Background(), "policy", scope, map[string]any{"scope": scope.String()}, policyProjectionMount(registry, scope, nil))
		result <- err
	}()
	select {
	case <-journal.entered:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for journal Record")
	}

	admissionDone := make(chan struct{})
	go func() {
		lease, err := server.acquireExecutionProjection(context.Background())
		if err == nil {
			lease.Release()
		}
		close(admissionDone)
	}()
	assertProjectionStillBlocked(t, admissionDone, "execution admission")

	queue := &executionProjectionQueue{}
	server.runQueue = queue
	workerDone := make(chan struct{})
	go func() {
		_, _ = server.RunWorkerOnce(context.Background(), "projection-worker")
		close(workerDone)
	}()
	assertProjectionStillBlocked(t, workerDone, "queued worker")
	if claims := queue.count(); claims != 0 {
		t.Fatalf("worker claimed before durable binding commit: %d", claims)
	}

	close(release)
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("add binding: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for binding publication")
	}
	select {
	case <-admissionDone:
	case <-time.After(time.Second):
		t.Fatal("execution admission did not resume")
	}
	select {
	case <-workerDone:
	case <-time.After(time.Second):
		t.Fatal("worker did not resume")
	}
	if claims := queue.count(); claims != 1 {
		t.Fatalf("worker claims=%d, want 1 after durable publication", claims)
	}
	if views, err := registry.List(scope); err != nil || len(views) != 1 {
		t.Fatalf("published policy views=%#v err=%v", views, err)
	}
	mustProjectionLease(t, server)
}

func TestDynamicBindingJournalFailureRollsBackBeforeAdmission(t *testing.T) {
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	reader := &executionProjectionEpochReader{epoch: 4}
	journal := &projectionGateBindingJournal{reader: reader, recordErr: errors.New("journal write failed")}
	server := newProjectionBindingServer(t, reader, journal)
	unmounted := 0
	if _, err := server.addBinding(context.Background(), "policy", scope, map[string]any{"scope": scope.String()}, policyProjectionMount(server.policyRegistry(), scope, &unmounted)); err == nil {
		t.Fatal("journal failure accepted binding")
	}
	if unmounted != 1 {
		t.Fatalf("rolled-back mount unmounted=%d, want 1", unmounted)
	}
	if views, err := server.policyRegistry().List(scope); err != nil || len(views) != 0 {
		t.Fatalf("journal failure published policy views=%#v err=%v", views, err)
	}
	mustProjectionLease(t, server)
}

func TestDynamicBindingRecordResponseUnknownKeepsFaultWithoutBlindRollback(t *testing.T) {
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	reader := &executionProjectionEpochReader{epoch: 8}
	journal := &projectionGateBindingJournal{reader: reader, recordDelta: 1, recordAfterErr: errors.New("response lost")}
	server := newProjectionBindingServer(t, reader, journal)
	unmounted := 0
	if _, err := server.addBinding(context.Background(), "policy", scope, map[string]any{"scope": scope.String()}, policyProjectionMount(server.policyRegistry(), scope, &unmounted)); err == nil {
		t.Fatal("response-lost write was accepted")
	}
	if unmounted != 0 {
		t.Fatalf("response-lost publication unmounted=%d, want 0", unmounted)
	}
	if views, err := server.policyRegistry().List(scope); err != nil || len(views) != 1 {
		t.Fatalf("response-lost publication views=%#v err=%v", views, err)
	}
	if len(server.adminStateFor().handles) != 0 {
		t.Fatal("response-lost publication exposed an admin binding before durable proof")
	}
	if _, err := server.acquireExecutionProjection(context.Background()); !errors.Is(err, errExecutionProjectionUnavailable) {
		t.Fatalf("response-lost admission error=%v", err)
	}
}

func TestDynamicBindingEpochMismatchFaultsAdmissionAfterCommittedPublication(t *testing.T) {
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	reader := &executionProjectionEpochReader{epoch: 20}
	server := newProjectionBindingServer(t, reader, &projectionGateBindingJournal{reader: reader, recordDelta: 2})
	if _, err := server.addBinding(context.Background(), "policy", scope, map[string]any{"scope": scope.String()}, policyProjectionMount(server.policyRegistry(), scope, nil)); err != nil {
		t.Fatalf("committed publication returned error: %v", err)
	}
	if _, err := server.acquireExecutionProjection(context.Background()); !errors.Is(err, errExecutionProjectionUnavailable) {
		t.Fatalf("epoch mismatch admission error=%v", err)
	}
	if len(server.adminStateFor().handles) != 1 {
		t.Fatal("committed binding was not retained for later reconciliation")
	}
}

func TestDynamicBindingEpochReadFailureFaultsAdmissionAfterCommittedPublication(t *testing.T) {
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	reader := &executionProjectionEpochReader{epoch: 30}
	journal := &projectionGateBindingJournal{reader: reader, recordDelta: 1, afterRecord: func() { reader.set(31, errors.New("epoch reader unavailable")) }}
	server := newProjectionBindingServer(t, reader, journal)
	if _, err := server.addBinding(context.Background(), "policy", scope, map[string]any{"scope": scope.String()}, policyProjectionMount(server.policyRegistry(), scope, nil)); err != nil {
		t.Fatalf("committed publication returned error: %v", err)
	}
	if _, err := server.acquireExecutionProjection(context.Background()); !errors.Is(err, errExecutionProjectionUnavailable) {
		t.Fatalf("epoch read failure admission error=%v", err)
	}
}

func TestDynamicBindingUnbindCommitsBeforeUnmountAndAppliesEpoch(t *testing.T) {
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	reader := &executionProjectionEpochReader{epoch: 50}
	server := newProjectionBindingServer(t, reader, &projectionGateBindingJournal{reader: reader, recordDelta: 1, deleteDelta: 1})
	unmounted := 0
	id, err := server.addBinding(context.Background(), "policy", scope, map[string]any{"scope": scope.String()}, policyProjectionMount(server.policyRegistry(), scope, &unmounted))
	if err != nil {
		t.Fatal(err)
	}
	principal := core.Principal{SubjectID: "admin", TenantID: "tenant", Scope: scope}
	found, err := server.unbindBinding(context.Background(), id, principal)
	if err != nil || !found {
		t.Fatalf("unbind found=%t err=%v", found, err)
	}
	if unmounted != 1 {
		t.Fatalf("unbind unmounted=%d, want 1", unmounted)
	}
	if views, err := server.policyRegistry().List(scope); err != nil || len(views) != 0 {
		t.Fatalf("unbind left policy views=%#v err=%v", views, err)
	}
	mustProjectionLease(t, server)
}

func TestDynamicBindingDeleteResponseUnknownKeepsLocalBindingAndFaultsAdmission(t *testing.T) {
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	reader := &executionProjectionEpochReader{epoch: 70}
	journal := &projectionGateBindingJournal{reader: reader, recordDelta: 1, deleteDelta: 1, deleteAfterErr: errors.New("delete response lost")}
	server := newProjectionBindingServer(t, reader, journal)
	unmounted := 0
	id, err := server.addBinding(context.Background(), "policy", scope, map[string]any{"scope": scope.String()}, policyProjectionMount(server.policyRegistry(), scope, &unmounted))
	if err != nil {
		t.Fatal(err)
	}
	principal := core.Principal{SubjectID: "admin", TenantID: "tenant", Scope: scope}
	if found, err := server.unbindBinding(context.Background(), id, principal); !found || err == nil {
		t.Fatalf("response-lost delete found=%t err=%v", found, err)
	}
	if unmounted != 0 {
		t.Fatalf("response-lost delete unmounted=%d, want 0", unmounted)
	}
	if _, ok := server.adminStateFor().get(id); !ok {
		t.Fatal("response-lost delete removed local binding")
	}
	if _, err := server.acquireExecutionProjection(context.Background()); !errors.Is(err, errExecutionProjectionUnavailable) {
		t.Fatalf("response-lost delete admission error=%v", err)
	}
}

func TestDynamicBindingStrictModeRejectsEphemeralPublication(t *testing.T) {
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	reader := &executionProjectionEpochReader{epoch: 60}
	server := newProjectionBindingServer(t, reader, nil)
	mounts := 0
	if _, err := server.addEphemeralBinding("credential", scope, map[string]any{"kind": "static"}, func() (func(), error) {
		mounts++
		return func() {}, nil
	}); !errors.Is(err, errDurableBindingRequired) {
		t.Fatalf("ephemeral strict publication error=%v", err)
	}
	if mounts != 0 {
		t.Fatalf("strict ephemeral publication mounted %d times", mounts)
	}
}

func TestDynamicBindingStrictModeRejectsMissingJournalBeforeMount(t *testing.T) {
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	reader := &executionProjectionEpochReader{epoch: 61}
	server := newProjectionBindingServer(t, reader, nil)
	mounts := 0
	if _, err := server.addBinding(context.Background(), "policy", scope, map[string]any{"scope": scope.String()}, func() (func(), error) {
		mounts++
		return func() {}, nil
	}); !errors.Is(err, errDurableBindingRequired) {
		t.Fatalf("missing strict journal error=%v", err)
	}
	if mounts != 0 {
		t.Fatalf("missing strict journal mounted %d times", mounts)
	}
}

func TestDynamicBindingStrictModeRejectsCustomRuntimeBeforeFactoryCreate(t *testing.T) {
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	reader := &executionProjectionEpochReader{epoch: 62}
	server := newProjectionBindingServer(t, reader, nil)
	server.runtime.Capabilities = core.NewCapabilityRegistry()
	factory := &projectionUnsafeDynamicFactory{}
	runtimes, err := capabilityruntime.New(4, factory)
	if err != nil {
		t.Fatal(err)
	}
	server.capabilityRuntimes = runtimes
	_, _, err = server.prepareDynamicCapability(context.Background(), dynamicCapabilityMount{
		Scope: scope, Runtime: "unsafe", Manifest: core.CapabilityManifest{ID: "example.echo", Version: "1"},
	})
	if !errors.Is(err, errDynamicCapabilityRuntimeNotEpochSafe) {
		t.Fatalf("custom strict runtime error=%v", err)
	}
	if factory.creates != 0 {
		t.Fatalf("custom strict runtime factory created %d provider(s)", factory.creates)
	}
}
