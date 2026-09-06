package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cc-auto-agent/harness-core/pkg/app/runexecutor"
	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
)

type checkpointTestModel struct{ calls *atomic.Int32 }

func (checkpointTestModel) Provider() string { return "checkpoint-test" }

func (m checkpointTestModel) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	if m.calls != nil {
		m.calls.Add(1)
	}
	if options.Messages[len(options.Messages)-1].Role == core.RoleTool {
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: "done"})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
		return nil
	}
	call := core.ToolCall{ID: "call-checkpoint", Name: "checkpoint.write", Args: map[string]any{"value": "one"}}
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &call})
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
	return nil
}

type checkpointTestTool struct{ calls *atomic.Int32 }

func (checkpointTestTool) Manifest() core.CapabilityManifest {
	return core.CapabilityManifest{
		ID: "checkpoint.write", Version: "1.0.0", Name: "Checkpoint write", Kind: core.KindTool,
		Contract: "harness.tool/v1", RequiredPermissions: []core.Permission{core.PermWrite},
		Tool: &core.ToolExposure{Parameters: map[string]any{
			"type": "object", "properties": map[string]any{"value": map[string]any{"type": "string"}},
		}},
	}
}

func (t checkpointTestTool) Execute(context.Context, core.CapabilityRequest) (core.CapabilityResult, error) {
	t.calls.Add(1)
	return core.CapabilityResult{Content: `{"written":true}`, OK: true}, nil
}

type durableToolCallJournal struct {
	db         *sql.DB
	attempts   atomic.Int32
	wasDurable atomic.Bool
}

type countingToolJournal struct{ begins atomic.Int32 }

func (j *countingToolJournal) BeginToolInvocation(context.Context, core.ToolInvocation) (core.ToolInvocationRecord, core.ToolInvocationDecision, error) {
	j.begins.Add(1)
	return core.ToolInvocationRecord{}, core.ToolInvocationExecuteNew, nil
}

func (*countingToolJournal) CompleteToolInvocation(context.Context, core.ToolInvocation, core.CapabilityResult) (core.ToolInvocationRecord, error) {
	return core.ToolInvocationRecord{}, nil
}

func (*countingToolJournal) MarkToolInvocationUncertain(context.Context, core.ToolInvocation, string) error {
	return nil
}

type failingCheckpointStore struct {
	*storage.SQLSessionStore
	err      error
	attempts atomic.Int32
}

func (s *failingCheckpointStore) AppendEvents(ctx context.Context, sessionID string, expectedVersion int64, events []core.SessionEvent) error {
	for _, event := range events {
		if event.Type == core.EvToolCall {
			s.attempts.Add(1)
			return s.err
		}
	}
	return s.SQLSessionStore.AppendEvents(ctx, sessionID, expectedVersion, events)
}

func (s *failingCheckpointStore) AppendEventsFenced(ctx context.Context, fence storage.SessionWriteFence, expectedVersion int64, events []core.SessionEvent) error {
	for _, event := range events {
		if event.Type == core.EvToolCall {
			s.attempts.Add(1)
			return s.err
		}
	}
	return s.SQLSessionStore.AppendEventsFenced(ctx, fence, expectedVersion, events)
}

func (j *durableToolCallJournal) BeginToolInvocation(ctx context.Context, invocation core.ToolInvocation) (core.ToolInvocationRecord, core.ToolInvocationDecision, error) {
	j.attempts.Add(1)
	rows, err := j.db.QueryContext(ctx, "SELECT payload FROM event_chunks WHERE session_id = ? ORDER BY start_seq", invocation.SessionID)
	if err != nil {
		return core.ToolInvocationRecord{}, "", err
	}
	defer rows.Close()
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return core.ToolInvocationRecord{}, "", err
		}
		for _, line := range strings.Split(payload, "\n") {
			var event core.SessionEvent
			if json.Unmarshal([]byte(line), &event) != nil || event.RunID != invocation.RunID || event.Type != core.EvToolCall {
				continue
			}
			var call core.ToolCallData
			if json.Unmarshal(event.Data, &call) == nil && call.CallID == invocation.CallID && call.Name == invocation.CapabilityID {
				j.wasDurable.Store(true)
				return core.ToolInvocationRecord{ToolInvocation: invocation, State: core.ToolInvocationStarted}, core.ToolInvocationExecuteNew, nil
			}
		}
	}
	if err := rows.Err(); err != nil {
		return core.ToolInvocationRecord{}, "", err
	}
	return core.ToolInvocationRecord{}, "", fmt.Errorf("tool call %s was not durable before journal begin", invocation.CallID)
}

func (*durableToolCallJournal) CompleteToolInvocation(_ context.Context, invocation core.ToolInvocation, result core.CapabilityResult) (core.ToolInvocationRecord, error) {
	return core.ToolInvocationRecord{ToolInvocation: invocation, State: core.ToolInvocationCompleted, Result: &result}, nil
}

func (*durableToolCallJournal) MarkToolInvocationUncertain(context.Context, core.ToolInvocation, string) error {
	return nil
}

func enableCheckpointTool(t *testing.T, fixture *runWorkerFixture) (*atomic.Int32, *atomic.Int32, *durableToolCallJournal) {
	t.Helper()
	segments := fixture.principal.Scope.Segments()
	product := core.MustScopePath(segments[:2]...)
	var calls atomic.Int32
	if err := fixture.server.runtime.Capabilities.Register(product, checkpointTestTool{calls: &calls}); err != nil {
		t.Fatal(err)
	}
	if err := fixture.server.runtime.Profiles.Bind(core.AgentProfileLayer{
		Scope: product, ProfileID: "product.agent", AddCapabilities: []string{"checkpoint.write"},
	}); err != nil {
		t.Fatal(err)
	}
	var modelCalls atomic.Int32
	fixture.server.runtime.Models = core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) {
		return checkpointTestModel{calls: &modelCalls}, nil
	})
	journal := &durableToolCallJournal{db: fixture.db}
	fixture.server.runtime.ToolJournal = journal
	return &calls, &modelCalls, journal
}

func enableCheckpointFastRouter(t *testing.T, fixture *runWorkerFixture) (*atomic.Int32, *atomic.Int32, *durableToolCallJournal) {
	t.Helper()
	calls, modelCalls, journal := enableCheckpointTool(t, fixture)
	fixture.server.runtime.FastRouters = core.FastRouterResolverFunc(func(context.Context, *core.AgentProfileSnapshot) (*core.FastRouter, error) {
		router := &core.FastRouter{}
		router.Add(core.FastRule{
			Match: func(text string) bool { return text == "write" }, Capability: "checkpoint.write",
			Args: func(string) map[string]any { return map[string]any{"value": "one"} },
		})
		return router, nil
	})
	return calls, modelCalls, journal
}

func assertCheckpointedToolRan(t *testing.T, calls, modelCalls *atomic.Int32, wantModelCalls int32, journal *durableToolCallJournal) {
	t.Helper()
	if calls.Load() != 1 || modelCalls.Load() != wantModelCalls || journal.attempts.Load() != 1 || !journal.wasDurable.Load() {
		t.Fatalf("tool checkpoint failed: calls=%d model_calls=%d journal_attempts=%d durable=%t", calls.Load(), modelCalls.Load(), journal.attempts.Load(), journal.wasDurable.Load())
	}
}

func TestToolJournalCheckpointFailurePreventsBegin(t *testing.T) {
	checkpointErr := errors.New("checkpoint failed")
	inner := &countingToolJournal{}
	var cancelled atomic.Bool
	failure := &toolCheckpointFailure{}
	journal := &checkpointToolInvocationJournal{
		ToolInvocationJournal: inner,
		checkpoint:            func(context.Context) error { return checkpointErr },
		cancel:                func() { cancelled.Store(true) },
		failure:               failure,
	}
	if _, _, err := journal.BeginToolInvocation(context.Background(), core.ToolInvocation{}); !errors.Is(err, checkpointErr) {
		t.Fatalf("BeginToolInvocation error = %v, want checkpoint failure", err)
	}
	if inner.begins.Load() != 0 {
		t.Fatalf("inner journal began %d times after checkpoint failure", inner.begins.Load())
	}
	if !cancelled.Load() {
		t.Fatal("checkpoint failure did not cancel the run")
	}
	if !errors.Is(failure.Err(), checkpointErr) {
		t.Fatalf("checkpoint failure state = %v, want %v", failure.Err(), checkpointErr)
	}
}

func TestSyncRunCheckpointsToolCallBeforeJournalBegin(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	calls, modelCalls, journal := enableCheckpointTool(t, fixture)
	request := httptest.NewRequest(http.MethodPost, "/v1/sessions/"+fixture.session.ID()+"/runs", strings.NewReader(`{"message":"write"}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	fixture.server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	assertCheckpointedToolRan(t, calls, modelCalls, 2, journal)
	if fixture.server.runtime.ToolJournal != journal {
		t.Fatal("per-run journal wrapper mutated the shared runtime")
	}
}

func TestQueuedRunCheckpointsToolCallBeforeJournalBegin(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	calls, modelCalls, journal := enableCheckpointTool(t, fixture)
	record := enqueueRunHTTP(t, fixture, "write")
	claimed, err := fixture.server.RunWorkerOnce(context.Background(), "checkpoint-worker")
	if err != nil || !claimed {
		t.Fatalf("claimed=%t err=%v", claimed, err)
	}
	terminal, err := fixture.queue.GetRun(context.Background(), record.RunID)
	if err != nil || terminal.Status != string(core.RunCompleted) {
		t.Fatalf("terminal=%#v err=%v", terminal, err)
	}
	assertCheckpointedToolRan(t, calls, modelCalls, 2, journal)
	if fixture.server.runtime.ToolJournal != journal {
		t.Fatal("per-run journal wrapper mutated the shared runtime")
	}
}

func enableCheckpointCustomExecutor(t *testing.T, fixture *runWorkerFixture) {
	t.Helper()
	segments := fixture.principal.Scope.Segments()
	product := core.MustScopePath(segments[:2]...)
	if err := fixture.server.runtime.Profiles.Bind(core.AgentProfileLayer{
		Scope: product, ProfileID: fixture.session.ProfileID(), Metadata: map[string]string{
			runExecutorIDKey: "checkpoint-custom", runExecutorVersionKey: "1",
		},
	}); err != nil {
		t.Fatal(err)
	}
	registration := runexecutor.Registration{
		Metadata: runexecutor.Metadata{ID: "checkpoint-custom", Version: "1", ImplementationRevision: "checkpoint-custom-v1"},
		Factory: func(dependencies runexecutor.Dependencies) (runexecutor.RunExecutor, error) {
			return runexecutor.NewSequential(dependencies.Runtime)
		},
	}
	registry, err := runexecutor.NewRegistry(4, registration)
	if err != nil {
		t.Fatal(err)
	}
	fixture.server.runExecutors = registry
}

func TestSyncCustomExecutorCheckpointsToolCallBeforeJournalBegin(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	calls, modelCalls, journal := enableCheckpointTool(t, fixture)
	enableCheckpointCustomExecutor(t, fixture)
	request := httptest.NewRequest(http.MethodPost, "/v1/sessions/"+fixture.session.ID()+"/runs", strings.NewReader(`{"message":"write"}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	fixture.server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	assertCheckpointedToolRan(t, calls, modelCalls, 2, journal)
}

func TestQueuedCustomExecutorCheckpointsToolCallBeforeJournalBegin(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	calls, modelCalls, journal := enableCheckpointTool(t, fixture)
	enableCheckpointCustomExecutor(t, fixture)
	record := enqueueRunHTTP(t, fixture, "write")
	claimed, err := fixture.server.RunWorkerOnce(context.Background(), "custom-checkpoint-worker")
	if err != nil || !claimed {
		t.Fatalf("claimed=%t err=%v", claimed, err)
	}
	terminal, err := fixture.queue.GetRun(context.Background(), record.RunID)
	if err != nil || terminal.Status != string(core.RunCompleted) {
		t.Fatalf("terminal=%#v err=%v", terminal, err)
	}
	assertCheckpointedToolRan(t, calls, modelCalls, 2, journal)
}

func TestSyncFastRouterCheckpointsToolCallBeforeDurableJournalBegin(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	calls, modelCalls, journal := enableCheckpointFastRouter(t, fixture)
	request := httptest.NewRequest(http.MethodPost, "/v1/sessions/"+fixture.session.ID()+"/runs", strings.NewReader(`{"message":"write"}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	fixture.server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	assertCheckpointedToolRan(t, calls, modelCalls, 0, journal)
	if fixture.server.runtime.ToolJournal != journal {
		t.Fatal("fast sync run polluted the shared runtime journal")
	}
}

func TestQueuedFastRouterCheckpointsToolCallBeforeDurableJournalBegin(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	calls, modelCalls, journal := enableCheckpointFastRouter(t, fixture)
	record := enqueueRunHTTP(t, fixture, "write")
	claimed, err := fixture.server.RunWorkerOnce(context.Background(), "fast-checkpoint-worker")
	if err != nil || !claimed {
		t.Fatalf("claimed=%t err=%v", claimed, err)
	}
	terminal, err := fixture.queue.GetRun(context.Background(), record.RunID)
	if err != nil || terminal.Status != string(core.RunCompleted) {
		t.Fatalf("terminal=%#v err=%v", terminal, err)
	}
	assertCheckpointedToolRan(t, calls, modelCalls, 0, journal)
	if fixture.server.runtime.ToolJournal != journal {
		t.Fatal("fast queued run polluted the shared runtime journal")
	}
}

func TestSyncCheckpointFailureCancelsRunBeforeAnotherModelCall(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	toolCalls, modelCalls, _ := enableCheckpointTool(t, fixture)
	checkpointErr := errors.New("checkpoint unavailable")
	store := &failingCheckpointStore{SQLSessionStore: fixture.sessions, err: checkpointErr}
	inner := &countingToolJournal{}
	fixture.server.sessions = store
	fixture.server.runtime.ToolJournal = inner

	request := httptest.NewRequest(http.MethodPost, "/v1/sessions/"+fixture.session.ID()+"/runs", strings.NewReader(`{"message":"write"}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	fixture.server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	assertCheckpointFailure(t, fixture, store, inner, toolCalls, modelCalls)
	if body := response.Body.String(); strings.Contains(body, "event: run/error") || strings.Contains(body, "event: run/end") || strings.Contains(body, "\"code\":\"tool_cancelled\"") || strings.Contains(body, "\"status\":\"cancelled\"") || !strings.Contains(body, "event: store/error") || !strings.Contains(body, "\"code\":\"store_error\"") || !strings.Contains(body, "\"status\":\"failed\"") || !strings.Contains(body, checkpointErr.Error()) {
		t.Fatalf("sync checkpoint failure did not expose canonical store_error semantics: %s", body)
	}
	runs, err := fixture.queue.ListRuns(context.Background(), fixture.session.ID(), 10)
	if err != nil || len(runs) != 1 || runs[0].Status != string(core.RunFailed) || runs[0].ErrorCode != "store_error" {
		t.Fatalf("sync run control mismatch: runs=%#v err=%v", runs, err)
	}
}

func TestQueuedCheckpointFailureCancelsRunBeforeAnotherModelCall(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	toolCalls, modelCalls, _ := enableCheckpointTool(t, fixture)
	checkpointErr := errors.New("checkpoint unavailable")
	store := &failingCheckpointStore{SQLSessionStore: fixture.sessions, err: checkpointErr}
	inner := &countingToolJournal{}
	fixture.server.sessions = store
	fixture.server.runtime.ToolJournal = inner

	record := enqueueRunHTTP(t, fixture, "write")
	claimed, err := fixture.server.RunWorkerOnce(context.Background(), "checkpoint-failure-worker")
	if !claimed || !errors.Is(err, checkpointErr) {
		t.Fatalf("claimed=%t err=%v", claimed, err)
	}
	assertCheckpointFailure(t, fixture, store, inner, toolCalls, modelCalls)
	terminal, err := fixture.queue.GetRun(context.Background(), record.RunID)
	if err != nil || terminal.Status != string(core.RunFailed) || terminal.ErrorCode != "store_error" {
		t.Fatalf("queued run control mismatch: terminal=%#v err=%v", terminal, err)
	}
}

func assertCheckpointFailure(t *testing.T, fixture *runWorkerFixture, store *failingCheckpointStore, inner *countingToolJournal, toolCalls, modelCalls *atomic.Int32) {
	t.Helper()
	if store.attempts.Load() != 1 || inner.begins.Load() != 0 || toolCalls.Load() != 0 || modelCalls.Load() != 1 {
		t.Fatalf("checkpoint failure crossed side-effect boundary: writes=%d begins=%d tools=%d models=%d", store.attempts.Load(), inner.begins.Load(), toolCalls.Load(), modelCalls.Load())
	}
	persisted, err := fixture.sessions.Load(context.Background(), fixture.session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if persisted.Version() != 0 || len(persisted.Events()) != 0 {
		t.Fatalf("failed checkpoint left partial durable history: version=%d events=%#v", persisted.Version(), persisted.Events())
	}
}
