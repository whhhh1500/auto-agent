package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/whhhh1500/auto-agent/pkg/app/runexecutor"
	core "github.com/whhhh1500/auto-agent/pkg/core"
)

type recordingRunExecutor struct{ runs atomic.Int32 }

func (e *recordingRunExecutor) RunTurn(_ context.Context, _ core.Principal, session *core.Session, input core.TurnInput, emit func(core.SessionEvent)) (core.TurnResult, error) {
	e.runs.Add(1)
	composition := &core.RunCompositionData{Profile: core.AgentProfileSnapshot{ID: "snapshot", ProfileID: session.ProfileID(), Scope: session.Scope()}, Metadata: input.CompositionMetadata}
	start, err := session.Append(input.RunID, core.EvRunStart, core.RunStartData{Composition: composition})
	if err != nil {
		return core.TurnResult{}, err
	}
	if emit != nil {
		emit(start)
	}
	user, err := session.Append(input.RunID, core.EvUserMessage, core.UserMessageData{Text: input.Text})
	if err != nil {
		return core.TurnResult{}, err
	}
	if emit != nil {
		emit(user)
	}
	end, err := session.Append(input.RunID, core.EvRunEnd, core.RunEndData{Status: core.RunCompleted})
	if err != nil {
		return core.TurnResult{}, err
	}
	if emit != nil {
		emit(end)
	}
	return core.TurnResult{RunID: input.RunID, Status: core.RunCompleted}, nil
}

func (e *recordingRunExecutor) ResumeTurn(_ context.Context, _ core.Principal, session *core.Session, input core.ResumeInput, emit func(core.SessionEvent)) (core.TurnResult, error) {
	e.runs.Add(1)
	composition := &core.RunCompositionData{Profile: core.AgentProfileSnapshot{ID: "snapshot", ProfileID: session.ProfileID(), Scope: session.Scope()}, Metadata: input.CompositionMetadata}
	resume, err := session.Append(input.RunID, core.EvRunResume, core.RunResumeData{Composition: composition})
	if err != nil {
		return core.TurnResult{}, err
	}
	if emit != nil {
		emit(resume)
	}
	end, err := session.Append(input.RunID, core.EvRunEnd, core.RunEndData{Status: core.RunCompleted})
	if err != nil {
		return core.TurnResult{}, err
	}
	if emit != nil {
		emit(end)
	}
	return core.TurnResult{RunID: input.RunID, Status: core.RunCompleted}, nil
}

func testServerExecutorRegistry(t *testing.T, executor *recordingRunExecutor) *runexecutor.Registry {
	t.Helper()
	registration := runexecutor.Registration{Metadata: runexecutor.Metadata{ID: "fake", Version: "1", ImplementationRevision: "fake-v1"}, Factory: func(runexecutor.Dependencies) (runexecutor.RunExecutor, error) { return executor, nil }}
	registry, err := runexecutor.NewRegistry(4, registration)
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

func enableFakeExecutorProfile(t *testing.T, fixture *runWorkerFixture) {
	t.Helper()
	segments := fixture.principal.Scope.Segments()
	product := core.MustScopePath(segments[:2]...)
	if err := fixture.server.runtime.Profiles.Bind(core.AgentProfileLayer{Scope: product, ProfileID: fixture.session.ProfileID(), Metadata: map[string]string{runExecutorIDKey: "fake", runExecutorVersionKey: "1"}}); err != nil {
		t.Fatal(err)
	}
}

func TestRunExecutorSyncHTTPUsesExplicitRegistry(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	deferred := &recordingRunExecutor{}
	enableFakeExecutorProfile(t, fixture)
	fixture.server.runExecutors = testServerExecutorRegistry(t, deferred)
	req := httptest.NewRequest(http.MethodPost, "/v1/sessions/"+fixture.session.ID()+"/runs", strings.NewReader(`{"message":"hello"}`))
	req.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	fixture.server.Handler().ServeHTTP(response, req)
	if response.Code != http.StatusOK || deferred.runs.Load() != 1 {
		t.Fatalf("status=%d calls=%d body=%s", response.Code, deferred.runs.Load(), response.Body.String())
	}
	persisted, err := fixture.sessions.Load(context.Background(), fixture.session.ID())
	if err != nil {
		t.Fatal(err)
	}
	events := persisted.Events()
	found := false
	for _, event := range events {
		if event.Type == core.EvRunStart && strings.Contains(string(event.Data), "harness.executor.id") {
			found = true
		}
	}
	if !found {
		t.Fatalf("executor evidence missing from durable start: %#v", events)
	}
}

func TestRunExecutorQueuedWorkerUsesExplicitRegistry(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	deferred := &recordingRunExecutor{}
	enableFakeExecutorProfile(t, fixture)
	fixture.server.runExecutors = testServerExecutorRegistry(t, deferred)
	record := enqueueRunHTTP(t, fixture, "hello")
	claimed, err := fixture.server.RunWorkerOnce(context.Background(), "executor-worker")
	if err != nil || !claimed || deferred.runs.Load() != 1 {
		t.Fatalf("claimed=%t calls=%d err=%v", claimed, deferred.runs.Load(), err)
	}
	terminal, err := fixture.queue.GetRun(context.Background(), record.RunID)
	if err != nil || terminal.Status != string(core.RunCompleted) {
		t.Fatalf("terminal=%#v err=%v", terminal, err)
	}
}

func TestRunExecutorResumeUsesFrozenEvidenceAndRejectsDrift(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	deferred := &recordingRunExecutor{}
	fixture.server.runExecutors = testServerExecutorRegistry(t, deferred)
	runID := "run-frozen"
	metadata := map[string]string{runExecutorIDKey: "fake", runExecutorVersionKey: "1", runExecutorImplementation: "fake-v1"}
	if _, err := fixture.session.Append(runID, core.EvRunStart, core.RunStartData{Composition: &core.RunCompositionData{Profile: core.AgentProfileSnapshot{ID: "snapshot", ProfileID: fixture.session.ProfileID(), Scope: fixture.session.Scope()}, Metadata: metadata}}); err != nil {
		t.Fatal(err)
	}
	// Profile metadata is deliberately changed after start; resume still uses
	// the exact durable start evidence.
	selection, err := fixture.server.executorSelection(context.Background(), fixture.principal, fixture.session, fixture.server.runtime, runID, true)
	if err != nil || selection.id != "fake" || selection.version != "1" || selection.implementation != "fake-v1" {
		t.Fatalf("selection=%#v err=%v", selection, err)
	}
	if _, _, err := fixture.server.resolveRunExecutor(context.Background(), fixture.principal, fixture.session, fixture.server.runtime, nil, runID, true); err != nil {
		t.Fatalf("frozen resolve failed: %v", err)
	}
	badRegistry, err := runexecutor.NewRegistry(4, runexecutor.Registration{Metadata: runexecutor.Metadata{ID: "fake", Version: "1", ImplementationRevision: "fake-v2"}, Factory: func(runexecutor.Dependencies) (runexecutor.RunExecutor, error) { return deferred, nil }})
	if err != nil {
		t.Fatal(err)
	}
	fixture.server.runExecutors = badRegistry
	if _, _, err := fixture.server.resolveRunExecutor(context.Background(), fixture.principal, fixture.session, fixture.server.runtime, nil, runID, true); err == nil {
		t.Fatal("implementation drift was accepted")
	}
}

func TestRunExecutorSelectionRejectsPartialAndUnknownConfiguration(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	segments := fixture.principal.Scope.Segments()
	product := core.MustScopePath(segments[:2]...)
	if err := fixture.server.runtime.Profiles.Bind(core.AgentProfileLayer{Scope: product, ProfileID: fixture.session.ProfileID(), Metadata: map[string]string{runExecutorIDKey: "fake"}}); err != nil {
		t.Fatal(err)
	}
	fixture.server.runExecutors = testServerExecutorRegistry(t, &recordingRunExecutor{})
	if _, _, err := fixture.server.resolveRunExecutor(context.Background(), fixture.principal, fixture.session, fixture.server.runtime, nil, "run_partial", false); err == nil {
		t.Fatal("partial executor metadata accepted")
	}
	if err := fixture.server.runtime.Profiles.Bind(core.AgentProfileLayer{Scope: product, ProfileID: fixture.session.ProfileID(), Metadata: map[string]string{runExecutorIDKey: "unknown", runExecutorVersionKey: "1"}}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := fixture.server.resolveRunExecutor(context.Background(), fixture.principal, fixture.session, fixture.server.runtime, nil, "run_unknown", false); err == nil {
		t.Fatal("unknown executor accepted")
	}
}

func TestRunExecutorResolvePassesCanaryRuntimeInstance(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	enableFakeExecutorProfile(t, fixture)
	var received *core.Runtime
	registration := runexecutor.Registration{Metadata: runexecutor.Metadata{ID: "fake", Version: "1", ImplementationRevision: "fake-v1"}, Factory: func(deps runexecutor.Dependencies) (runexecutor.RunExecutor, error) {
		received = deps.Runtime
		return &recordingRunExecutor{}, nil
	}}
	registry, err := runexecutor.NewRegistry(2, registration)
	if err != nil {
		t.Fatal(err)
	}
	fixture.server.runExecutors = registry
	canaryRuntime := *fixture.server.runtime
	if _, _, err := fixture.server.resolveRunExecutor(context.Background(), fixture.principal, fixture.session, &canaryRuntime, nil, "run-canary", false); err != nil {
		t.Fatal(err)
	}
	if received != &canaryRuntime {
		t.Fatalf("factory received runtime %p, want %p", received, &canaryRuntime)
	}
}

func TestServerNilRegistryUsesDefaultSequentialHTTP(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	req := httptest.NewRequest(http.MethodPost, "/v1/sessions/"+fixture.session.ID()+"/runs", strings.NewReader(`{"message":"hello"}`))
	req.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	fixture.server.Handler().ServeHTTP(response, req)
	if response.Code != http.StatusOK {
		t.Fatalf("default sequential status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestRunExecutorQueuedWorkerResumeUsesFrozenFake(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	executor := &recordingRunExecutor{}
	fixture.server.runExecutors = testServerExecutorRegistry(t, executor)
	record := enqueueRunHTTP(t, fixture, "resume me")
	session, err := fixture.sessions.Load(context.Background(), fixture.session.ID())
	if err != nil {
		t.Fatal(err)
	}
	metadata := map[string]string{runExecutorIDKey: "fake", runExecutorVersionKey: "1", runExecutorImplementation: "fake-v1"}
	composition := &core.RunCompositionData{Profile: core.AgentProfileSnapshot{ID: "snapshot", ProfileID: session.ProfileID(), Scope: session.Scope()}, Metadata: metadata}
	if _, err := session.Append(record.RunID, core.EvRunStart, core.RunStartData{Composition: composition}); err != nil {
		t.Fatal(err)
	}
	approval := core.ApprovalRequestedData{ApprovalID: "apr_0123456789abcdef0123456789abcdef", ToolCall: core.ToolCall{ID: "call_1", Name: "test.tool"}, ResumeCall: core.ToolCall{ID: "resume_1", Name: "test.tool"}}
	if _, err := session.Append(record.RunID, core.EvApprovalRequested, approval); err != nil {
		t.Fatal(err)
	}
	if err := fixture.sessions.Save(context.Background(), session, session.Version()-2); err != nil {
		t.Fatal(err)
	}
	// The profile is changed after the run/start fact; queued resume must not
	// reinterpret that durable run as a different executor.
	segments := fixture.principal.Scope.Segments()
	product := core.MustScopePath(segments[:2]...)
	if err := fixture.server.runtime.Profiles.Bind(core.AgentProfileLayer{Scope: product, ProfileID: session.ProfileID(), Metadata: map[string]string{runExecutorIDKey: "unknown", runExecutorVersionKey: "1"}}); err != nil {
		t.Fatal(err)
	}
	claimed, err := fixture.server.RunWorkerOnce(context.Background(), "resume-executor-worker")
	if err != nil || !claimed || executor.runs.Load() != 1 {
		t.Fatalf("claimed=%t calls=%d err=%v", claimed, executor.runs.Load(), err)
	}
}
