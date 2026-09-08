package server

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/storage"
)

type fakeRunControlStore struct {
	mu      sync.Mutex
	records map[string]storage.RunRecord
}

func newFakeRunControlStore() *fakeRunControlStore {
	return &fakeRunControlStore{records: map[string]storage.RunRecord{}}
}

func (f *fakeRunControlStore) CreateRun(_ context.Context, record storage.RunRecord) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, exists := f.records[record.RunID]; exists {
		return core.ErrSessionConflict
	}
	now := time.Now().UTC()
	record.Status = storage.RunStatusRunning
	record.CreatedAt, record.UpdatedAt = now, now
	f.records[record.RunID] = record
	return nil
}

func (f *fakeRunControlStore) GetRun(_ context.Context, runID string) (storage.RunRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	record, ok := f.records[runID]
	if !ok {
		return storage.RunRecord{}, storage.ErrRunNotFound
	}
	return record, nil
}

func (f *fakeRunControlStore) FindActiveRun(_ context.Context, sessionID string) (storage.RunRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, record := range f.records {
		if record.SessionID == sessionID && record.Status == storage.RunStatusRunning {
			return record, nil
		}
	}
	return storage.RunRecord{}, storage.ErrRunNotFound
}

func (f *fakeRunControlStore) ListRuns(_ context.Context, sessionID string, _ int) ([]storage.RunRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := []storage.RunRecord{}
	for _, record := range f.records {
		if record.SessionID == sessionID {
			out = append(out, record)
		}
	}
	return out, nil
}

func (f *fakeRunControlStore) RequestRunCancel(_ context.Context, runID string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	record, ok := f.records[runID]
	if !ok || record.Status != storage.RunStatusRunning {
		return false, nil
	}
	record.CancelRequested = true
	f.records[runID] = record
	return true, nil
}

func (f *fakeRunControlStore) RunCancelRequested(_ context.Context, runID string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	record, ok := f.records[runID]
	if !ok {
		return false, storage.ErrRunNotFound
	}
	return record.CancelRequested, nil
}

func (f *fakeRunControlStore) HeartbeatRun(_ context.Context, runID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	record, ok := f.records[runID]
	if !ok || record.Status != storage.RunStatusRunning {
		return storage.ErrRunNotFound
	}
	record.UpdatedAt = time.Now().UTC()
	f.records[runID] = record
	return nil
}

func (f *fakeRunControlStore) FinishRun(_ context.Context, runID string, status core.RunStatus, errorCode string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	record, ok := f.records[runID]
	if !ok {
		return storage.ErrRunNotFound
	}
	record.Status, record.ErrorCode = string(status), errorCode
	record.CompletedAt, record.UpdatedAt = time.Now().UTC(), time.Now().UTC()
	f.records[runID] = record
	return nil
}

func (f *fakeRunControlStore) FailStaleRuns(_ context.Context, olderThan time.Time) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var failed int64
	for id, record := range f.records {
		if record.Status == storage.RunStatusRunning && record.UpdatedAt.Before(olderThan) {
			record.Status, record.ErrorCode = string(core.RunFailed), "worker_lost"
			record.CompletedAt, record.UpdatedAt = time.Now().UTC(), time.Now().UTC()
			f.records[id] = record
			failed++
		}
	}
	return failed, nil
}

func newRunControlServer(t *testing.T, controls storage.RunControlStore) (*Server, *core.Session, core.Principal) {
	t.Helper()
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product, _ := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "product"})
	tenant, _ := product.Child(core.ScopeRef{Kind: core.ScopeTenant, ID: "acme"})
	user, _ := tenant.Child(core.ScopeRef{Kind: core.ScopeUser, ID: "alice"})
	principal := core.Principal{SubjectID: "alice", TenantID: "acme", Scope: user, Grants: core.NewPermissionSet(core.PermRead)}
	profiles := core.NewAgentProfileRegistry()
	name := "Agent"
	model := core.ModelSelection{Provider: "mock", Model: "mock-1"}
	if err := profiles.Bind(core.AgentProfileLayer{Scope: product, ProfileID: "product.agent", Name: &name, Model: &model}); err != nil {
		t.Fatal(err)
	}
	runtime := &core.Runtime{
		Capabilities: core.NewCapabilityRegistry(), Profiles: profiles,
		Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) { return core.MockLlmAdapter{}, nil }),
	}
	sessionScope, _ := user.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "session-control"})
	session, err := core.NewSession(core.SessionOptions{ID: "session-control", ProfileID: "product.agent", Principal: principal, Scope: sessionScope})
	if err != nil {
		t.Fatal(err)
	}
	sessions := core.NewMemorySessionStore()
	if err := sessions.Create(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	server, err := New(Config{
		Runtime: runtime, Sessions: sessions, RunControl: controls, RunCancelPollInterval: time.Millisecond,
		Authenticator: AuthenticatorFunc(func(*http.Request) (core.Principal, error) { return principal, nil }),
	})
	if err != nil {
		t.Fatal(err)
	}
	return server, session, principal
}

func TestRunControlLifecycleAndQueryRoutes(t *testing.T) {
	controls := newFakeRunControlStore()
	server, session, _ := newRunControlServer(t, controls)
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()

	request, _ := http.NewRequest(http.MethodPost, httpServer.URL+"/v1/sessions/"+session.ID()+"/runs", bytes.NewBufferString(`{"message":"hello"}`))
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_, readErr := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if readErr != nil {
		t.Fatal(readErr)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("run status=%d", response.StatusCode)
	}
	runs, err := controls.ListRuns(context.Background(), session.ID(), 10)
	if err != nil || len(runs) != 1 || runs[0].Status != string(core.RunCompleted) || runs[0].CompletedAt.IsZero() {
		t.Fatalf("durable lifecycle is wrong: %#v %v", runs, err)
	}

	getResponse, err := http.Get(httpServer.URL + "/v1/sessions/" + session.ID() + "/runs/" + runs[0].RunID)
	if err != nil {
		t.Fatal(err)
	}
	defer getResponse.Body.Close()
	if getResponse.StatusCode != http.StatusOK {
		t.Fatalf("get run status=%d", getResponse.StatusCode)
	}
	listResponse, err := http.Get(httpServer.URL + "/v1/sessions/" + session.ID() + "/runs")
	if err != nil {
		t.Fatal(err)
	}
	defer listResponse.Body.Close()
	if listResponse.StatusCode != http.StatusOK {
		t.Fatalf("list runs status=%d", listResponse.StatusCode)
	}
}

func TestDurableCancelMonitorCancelsRun(t *testing.T) {
	controls := newFakeRunControlStore()
	server, _, principal := newRunControlServer(t, controls)
	if err := controls.CreateRun(context.Background(), storage.RunRecord{
		RunID: "run-remote-cancel", SessionID: "session-control", TenantID: principal.TenantID, SubjectID: principal.SubjectID,
	}); err != nil {
		t.Fatal(err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	stop := server.monitorRunCancel(runCtx, cancelRun, "run-remote-cancel")
	defer stop()
	if requested, err := controls.RequestRunCancel(context.Background(), "run-remote-cancel"); err != nil || !requested {
		t.Fatalf("request cancel: %t %v", requested, err)
	}
	select {
	case <-runCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("durable cancel flag did not cancel the worker context")
	}
}

func TestCancelRouteWorksWithoutLocalActiveRun(t *testing.T) {
	controls := newFakeRunControlStore()
	server, session, principal := newRunControlServer(t, controls)
	if err := controls.CreateRun(context.Background(), storage.RunRecord{
		RunID: "run-other-instance", SessionID: session.ID(), TenantID: principal.TenantID, SubjectID: principal.SubjectID,
	}); err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()
	response, err := http.Post(httpServer.URL+"/v1/sessions/"+session.ID()+"/cancel", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("cancel status=%d", response.StatusCode)
	}
	requested, err := controls.RunCancelRequested(context.Background(), "run-other-instance")
	if err != nil || !requested {
		t.Fatalf("remote cancel was not persisted: requested=%t err=%v", requested, err)
	}
}

func TestRecoverStaleRunsUsesConfiguredCutoff(t *testing.T) {
	controls := newFakeRunControlStore()
	server, _, principal := newRunControlServer(t, controls)
	server.runStaleAfter = time.Minute
	if err := controls.CreateRun(context.Background(), storage.RunRecord{
		RunID: "run-stale-server", SessionID: "session-control", TenantID: principal.TenantID, SubjectID: principal.SubjectID,
	}); err != nil {
		t.Fatal(err)
	}
	controls.mu.Lock()
	record := controls.records["run-stale-server"]
	record.UpdatedAt = time.Now().UTC().Add(-2 * time.Minute)
	controls.records[record.RunID] = record
	controls.mu.Unlock()
	if err := server.RecoverStaleRuns(context.Background()); err != nil {
		t.Fatal(err)
	}
	recovered, _ := controls.GetRun(context.Background(), record.RunID)
	if recovered.Status != string(core.RunFailed) || recovered.ErrorCode != "worker_lost" {
		t.Fatalf("stale run was not recovered: %#v", recovered)
	}
}

var _ storage.RunControlStore = (*fakeRunControlStore)(nil)
