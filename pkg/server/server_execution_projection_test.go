package server

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/whhhh1500/auto-agent/pkg/app/runexecutor"
	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/storage"
)

type executionProjectionEpochReader struct {
	mu    sync.Mutex
	epoch int64
	err   error
}

func (r *executionProjectionEpochReader) AuthorizationEpoch(context.Context) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.epoch, r.err
}

func (r *executionProjectionEpochReader) set(epoch int64, err error) {
	r.mu.Lock()
	r.epoch, r.err = epoch, err
	r.mu.Unlock()
}

type executionProjectionQueue struct {
	storage.RunQueueStore
	mu     sync.Mutex
	claims int
}

func (q *executionProjectionQueue) ClaimRun(context.Context, string, time.Duration) (storage.QueuedRun, bool, error) {
	q.mu.Lock()
	q.claims++
	q.mu.Unlock()
	return storage.QueuedRun{}, false, nil
}

func (q *executionProjectionQueue) count() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.claims
}

type executionProjectionCustomExecutor struct{}

func (executionProjectionCustomExecutor) RunTurn(context.Context, core.Principal, *core.Session, core.TurnInput, func(core.SessionEvent)) (core.TurnResult, error) {
	return core.TurnResult{}, nil
}

func (executionProjectionCustomExecutor) ResumeTurn(context.Context, core.Principal, *core.Session, core.ResumeInput, func(core.SessionEvent)) (core.TurnResult, error) {
	return core.TurnResult{}, nil
}

var _ runexecutor.RunExecutor = executionProjectionCustomExecutor{}

func TestExecutionProjectionConfiguredReaderRequiresExplicitAppliedEpoch(t *testing.T) {
	reader := &executionProjectionEpochReader{epoch: 7}
	server := &Server{executionProjection: newExecutionProjectionCoordinator(reader)}
	if _, err := server.acquireExecutionProjection(context.Background()); !errors.Is(err, errExecutionProjectionUnavailable) {
		t.Fatalf("unapplied admission error=%v", err)
	}
	if err := server.MarkExecutionProjectionAppliedEpoch(context.Background()); err != nil {
		t.Fatalf("mark applied epoch: %v", err)
	}
	lease, err := server.acquireExecutionProjection(context.Background())
	if err != nil {
		t.Fatalf("applied admission: %v", err)
	}
	lease.Release()
}

func TestExecutionProjectionEpochLagBlocksSyncAndWorkerBeforeClaim(t *testing.T) {
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	reader := &executionProjectionEpochReader{epoch: 7}
	server := newProfileAdminTestServer(scope, &profileReplaceJournal{}, core.NewAgentProfileRegistry())
	server.executionProjection = newExecutionProjectionCoordinator(reader)
	if err := server.MarkExecutionProjectionAppliedEpoch(context.Background()); err != nil {
		t.Fatal(err)
	}
	reader.set(8, nil)

	request := httptest.NewRequest(http.MethodPost, "/v1/sessions/missing/runs", bytes.NewBufferString(`{"message":"hello"}`))
	request.SetPathValue("id", "missing")
	response := httptest.NewRecorder()
	server.handleRun(response, request)
	if response.Code != http.StatusServiceUnavailable || response.Body.String() != "{\"error\":\"execution projection is unavailable\"}\n" {
		t.Fatalf("sync gate status=%d body=%s", response.Code, response.Body.String())
	}
	ready := httptest.NewRecorder()
	server.handleReady(ready, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if ready.Code != http.StatusServiceUnavailable {
		t.Fatalf("epoch lag did not mark readiness unavailable: status=%d body=%s", ready.Code, ready.Body.String())
	}

	queue := &executionProjectionQueue{}
	server.runQueue = queue
	claimed, err := server.RunWorkerOnce(context.Background(), "projection-worker")
	if claimed || !errors.Is(err, errExecutionProjectionUnavailable) || queue.count() != 0 {
		t.Fatalf("worker gate claimed=%t err=%v claims=%d", claimed, err, queue.count())
	}

	// Health and control routes do not acquire an execution read lease or turn
	// a projection fault into an authorization failure for repair operations.
	health := httptest.NewRecorder()
	server.handleHealth(health, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if health.Code != http.StatusOK {
		t.Fatalf("health status=%d body=%s", health.Code, health.Body.String())
	}
	if response := invokeAdminProfilePut(t, server, scope, profileAdminLayer(scope, "repair.agent", "repair")); response.Code != http.StatusOK {
		t.Fatalf("admin repair route was blocked status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestExecutionProjectionReconcileUnblocksEpochAdmissionWithoutClearingBindingFault(t *testing.T) {
	reader := &executionProjectionEpochReader{epoch: 3}
	server := &Server{executionProjection: newExecutionProjectionCoordinator(reader)}
	if err := server.MarkExecutionProjectionAppliedEpoch(context.Background()); err != nil {
		t.Fatal(err)
	}
	reader.set(4, nil)
	if _, err := server.acquireExecutionProjection(context.Background()); !errors.Is(err, errExecutionProjectionUnavailable) {
		t.Fatalf("lag admission error=%v", err)
	}
	server.setProfileProjectionError("binding-a", errors.New("live profile differs"))
	if err := server.MarkExecutionProjectionAppliedEpoch(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := server.acquireExecutionProjection(context.Background()); !errors.Is(err, errProfileProjectionUnavailable) {
		t.Fatalf("binding fault was cleared by epoch reconcile: %v", err)
	}
	server.clearProfileProjectionError("binding-a")
	lease, err := server.acquireExecutionProjection(context.Background())
	if err != nil {
		t.Fatalf("reconciled admission: %v", err)
	}
	lease.Release()
}

func TestExecutionProjectionLeaseHoldsUntilRunStartAndSerializesMutation(t *testing.T) {
	reader := &executionProjectionEpochReader{epoch: 9}
	coordinator := newExecutionProjectionCoordinator(reader)
	if err := coordinator.markAppliedEpoch(context.Background()); err != nil {
		t.Fatal(err)
	}
	lease, err := coordinator.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	mutationEntered := make(chan struct{})
	mutationDone := make(chan struct{})
	go func() {
		unlock := coordinator.lockMutation()
		close(mutationEntered)
		unlock()
		close(mutationDone)
	}()
	select {
	case <-mutationEntered:
		t.Fatal("projection mutation entered before execution snapshot reached run start")
	case <-time.After(50 * time.Millisecond):
	}
	lease.releaseOnEvent(core.SessionEvent{Type: core.EvAssistantMessage})
	select {
	case <-mutationEntered:
		t.Fatal("non-start event released execution projection lease")
	case <-time.After(50 * time.Millisecond):
	}
	lease.releaseOnEvent(core.SessionEvent{Type: core.EvRunStart})
	select {
	case <-mutationEntered:
	case <-time.After(time.Second):
		t.Fatal("projection mutation did not run after run start")
	}
	select {
	case <-mutationDone:
	case <-time.After(time.Second):
		t.Fatal("projection mutation did not complete")
	}
}

func TestExecutionProjectionSyncLeaseCoversCompositionUntilRunStart(t *testing.T) {
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	principal := core.Principal{TenantID: "tenant", SubjectID: "subject", Scope: scope}
	profiles := core.NewAgentProfileRegistry()
	name := "projection"
	model := core.ModelSelection{Provider: "mock", Model: "mock"}
	if _, err := profiles.Mount(core.AgentProfileLayer{Scope: scope, ProfileID: "projection.agent", Name: &name, Model: &model}); err != nil {
		t.Fatal(err)
	}
	reader := &executionProjectionEpochReader{epoch: 11}
	mutationEntered := make(chan struct{})
	var resolverObservedMutation bool
	var api *Server
	var err error
	runtime := &core.Runtime{
		Capabilities: core.NewCapabilityRegistry(), Profiles: profiles,
		Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) {
			go func() {
				unlock := api.lockExecutionProjectionMutation()
				close(mutationEntered)
				unlock()
			}()
			select {
			case <-mutationEntered:
				resolverObservedMutation = true
			default:
			}
			return core.MockLlmAdapter{}, nil
		}),
	}
	store := core.NewMemorySessionStore()
	api, err = New(Config{
		Runtime:       runtime,
		Sessions:      store,
		Authenticator: AuthenticatorFunc(func(*http.Request) (core.Principal, error) { return principal, nil }),
	})
	if err != nil {
		t.Fatal(err)
	}
	// This test controls a local fake clock-like epoch reader to exercise the
	// admission lease boundary. Native SQL authority validation is covered by
	// TestNewAuthorizationEpochBindingJournalRequiresSharedNativeSQLAuthority.
	api.executionProjection = newExecutionProjectionCoordinator(reader)
	if err := api.MarkExecutionProjectionAppliedEpoch(context.Background()); err != nil {
		t.Fatal(err)
	}
	sessionScope, err := scope.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "projection-sync"})
	if err != nil {
		t.Fatal(err)
	}
	session, err := core.NewSession(core.SessionOptions{ID: "projection-sync", ProfileID: "projection.agent", Principal: principal, Scope: sessionScope})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Create(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/sessions/projection-sync/runs", bytes.NewBufferString(`{"message":"hello"}`))
	request.SetPathValue("id", session.ID())
	response := httptest.NewRecorder()
	api.handleRun(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("run status=%d body=%s", response.Code, response.Body.String())
	}
	if resolverObservedMutation {
		t.Fatal("projection mutation entered while model composition was still resolving")
	}
	select {
	case <-mutationEntered:
	case <-time.After(time.Second):
		t.Fatal("run/start did not release the execution projection lease")
	}
}

func TestExecutionProjectionQueuedLeaseCoversCompositionUntilRunStart(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	reader := &executionProjectionEpochReader{epoch: 12}
	fixture.server.executionProjection = newExecutionProjectionCoordinator(reader)
	if err := fixture.server.MarkExecutionProjectionAppliedEpoch(context.Background()); err != nil {
		t.Fatal(err)
	}
	mutationEntered := make(chan struct{})
	var resolverObservedMutation bool
	fixture.server.runtime.Models = core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) {
		go func() {
			unlock := fixture.server.lockExecutionProjectionMutation()
			close(mutationEntered)
			unlock()
		}()
		select {
		case <-mutationEntered:
			resolverObservedMutation = true
		default:
		}
		return core.MockLlmAdapter{}, nil
	})
	record := enqueueRunHTTP(t, fixture, "projection lease")
	claimed, err := fixture.server.RunWorkerOnce(context.Background(), "projection-worker")
	if err != nil || !claimed {
		t.Fatalf("worker claimed=%t err=%v", claimed, err)
	}
	if resolverObservedMutation {
		t.Fatal("projection mutation entered while queued runtime composition was resolving")
	}
	select {
	case <-mutationEntered:
	case <-time.After(time.Second):
		t.Fatal("queued run/start did not release the execution projection lease")
	}
	terminal, err := fixture.queue.GetRun(context.Background(), record.RunID)
	if err != nil || terminal.Status != string(core.RunCompleted) {
		t.Fatalf("queued terminal=%#v err=%v", terminal, err)
	}
}

func TestExecutionProjectionEpochReaderRejectsCustomExecutorWithoutStartSignal(t *testing.T) {
	reader := &executionProjectionEpochReader{epoch: 1}
	coordinator := newExecutionProjectionCoordinator(reader)
	if err := coordinator.markAppliedEpoch(context.Background()); err != nil {
		t.Fatal(err)
	}
	lease, err := coordinator.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	if err := requireProjectionAwareExecutor(lease, executionProjectionCustomExecutor{}); !errors.Is(err, errExecutionProjectionUnavailable) {
		t.Fatalf("custom executor error=%v", err)
	}
}

func TestExecutionProjectionReaderNilPreservesLegacyWorkerAdmission(t *testing.T) {
	queue := &executionProjectionQueue{}
	server := &Server{runQueue: queue, executionProjection: newExecutionProjectionCoordinator(nil)}
	claimed, err := server.RunWorkerOnce(context.Background(), "legacy-worker")
	if claimed || err != nil || queue.count() != 1 {
		t.Fatalf("legacy worker claimed=%t err=%v claims=%d", claimed, err, queue.count())
	}
}

func TestExecutionProjectionReadyDoesNotClearIndependentReadinessError(t *testing.T) {
	reader := &executionProjectionEpochReader{epoch: 2}
	server := &Server{executionProjection: newExecutionProjectionCoordinator(reader)}
	server.setReadyError(errors.New("restore failed"))
	if err := server.MarkExecutionProjectionAppliedEpoch(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := server.readinessError(); err == nil || err.Error() != "restore failed" {
		t.Fatalf("readiness error=%v", err)
	}
}
