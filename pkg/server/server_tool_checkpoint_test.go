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
	"sync"
	"sync/atomic"
	"testing"

	"github.com/whhhh1500/auto-agent/pkg/app/runexecutor"
	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/storage"
)

type checkpointTestModel struct{ calls *atomic.Int32 }

type disablingCheckpointModel struct {
	accounts *storage.SQLAccountStore
	inner    checkpointTestModel
}

func (m disablingCheckpointModel) Provider() string { return m.inner.Provider() }

func (m disablingCheckpointModel) Stream(ctx context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	if options.Messages[len(options.Messages)-1].Role != core.RoleTool {
		if err := m.accounts.SetAccountStatus(ctx, "alice", storage.AccountDisabled); err != nil {
			return err
		}
	}
	return m.inner.Stream(ctx, options, emit)
}

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
	db          *sql.DB
	attempts    atomic.Int32
	wasDurable  atomic.Bool
	modelPrefix bool
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

// checkpointReaderToolJournal deliberately exposes the optional read-only
// journal capability so the checkpoint wrappers can be tested without
// coupling this seam to a storage implementation.
type checkpointReaderToolJournal struct {
	countingToolJournal
	reads      atomic.Int32
	ctx        context.Context
	invocation core.ToolInvocation
}

func (j *checkpointReaderToolJournal) GetToolInvocation(ctx context.Context, invocation core.ToolInvocation) (core.ToolInvocationRecord, bool, error) {
	j.reads.Add(1)
	j.ctx, j.invocation = ctx, invocation
	return core.ToolInvocationRecord{ToolInvocation: invocation, State: core.ToolInvocationCompleted}, true, nil
}

func TestRuntimeToolCheckpointPreservesOptionalJournalReader(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	writer := storage.NewWriteBehind(fixture.sessions, fixture.session, fixture.session.Version(), -1)
	invocation, err := core.NewToolInvocation(core.RunInfo{
		RunID: "reader-checkpoint", SessionID: fixture.session.ID(), ProfileID: fixture.session.ProfileID(), Principal: fixture.principal,
	}, core.ToolCall{ID: "reader-call", Name: "checkpoint.write", Args: map[string]any{}}, true)
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		name string
		wrap func(*core.Runtime) (*core.Runtime, error)
	}{
		{
			name: "synchronous checkpoint",
			wrap: func(runtime *core.Runtime) (*core.Runtime, error) {
				wrapped, _ := runtimeWithToolCheckpoint(runtime, writer, func() {})
				return wrapped, nil
			},
		},
		{
			name: "generic queued checkpoint",
			wrap: func(runtime *core.Runtime) (*core.Runtime, error) {
				wrapped, _, err := fixture.server.runtimeWithQueuedToolCheckpoint(runtime, writer, func() {}, storage.SessionWriteFence{}, fixture.session, fixture.principal, false)
				return wrapped, err
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			journal := &checkpointReaderToolJournal{}
			wrapped, err := test.wrap(&core.Runtime{ToolJournal: journal})
			if err != nil {
				t.Fatal(err)
			}
			reader, ok := wrapped.ToolJournal.(core.ToolInvocationReader)
			if !ok {
				t.Fatalf("wrapped journal %T lost ToolInvocationReader", wrapped.ToolJournal)
			}
			record, found, err := reader.GetToolInvocation(context.Background(), invocation)
			if err != nil || !found || record.ToolInvocation != invocation || journal.reads.Load() != 1 || journal.invocation != invocation || journal.ctx == nil {
				t.Fatalf("reader forwarding record=%+v found=%t err=%v reads=%d invocation=%+v ctx=%v", record, found, err, journal.reads.Load(), journal.invocation, journal.ctx)
			}
		})
	}
}

func TestRuntimeToolCheckpointDoesNotInventOptionalJournalReader(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	writer := storage.NewWriteBehind(fixture.sessions, fixture.session, fixture.session.Version(), -1)

	for _, test := range []struct {
		name string
		wrap func(*core.Runtime) (*core.Runtime, error)
	}{
		{
			name: "synchronous checkpoint",
			wrap: func(runtime *core.Runtime) (*core.Runtime, error) {
				wrapped, _ := runtimeWithToolCheckpoint(runtime, writer, func() {})
				return wrapped, nil
			},
		},
		{
			name: "generic queued checkpoint",
			wrap: func(runtime *core.Runtime) (*core.Runtime, error) {
				wrapped, _, err := fixture.server.runtimeWithQueuedToolCheckpoint(runtime, writer, func() {}, storage.SessionWriteFence{}, fixture.session, fixture.principal, false)
				return wrapped, err
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			wrapped, err := test.wrap(&core.Runtime{ToolJournal: &countingToolJournal{}})
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := wrapped.ToolJournal.(core.ToolInvocationReader); ok {
				t.Fatalf("wrapped write-only journal %T unexpectedly implements ToolInvocationReader", wrapped.ToolJournal)
			}
		})
	}
}

type failingCheckpointStore struct {
	*storage.SQLSessionStore
	err           error
	attempts      atomic.Int32
	usageAttempts atomic.Int32
	mu            sync.Mutex
	toolCallBatch []core.SessionEvent
}

func (s *failingCheckpointStore) AppendEvents(ctx context.Context, sessionID string, expectedVersion int64, events []core.SessionEvent) error {
	if err := s.checkpointError(events); err != nil {
		return err
	}
	return s.SQLSessionStore.AppendEvents(ctx, sessionID, expectedVersion, events)
}

func (s *failingCheckpointStore) AppendEventsFenced(ctx context.Context, fence storage.SessionWriteFence, expectedVersion int64, events []core.SessionEvent) error {
	if err := s.checkpointError(events); err != nil {
		return err
	}
	return s.SQLSessionStore.AppendEventsFenced(ctx, fence, expectedVersion, events)
}

func (s *failingCheckpointStore) checkpointError(events []core.SessionEvent) error {
	for _, event := range events {
		if event.Type == core.EvRunUsage {
			s.usageAttempts.Add(1)
		}
		if event.Type == core.EvToolCall {
			s.attempts.Add(1)
			s.mu.Lock()
			s.toolCallBatch = append([]core.SessionEvent(nil), events...)
			s.mu.Unlock()
			return s.err
		}
	}
	return nil
}

func (s *failingCheckpointStore) attemptedToolCallBatch() []core.SessionEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]core.SessionEvent(nil), s.toolCallBatch...)
}

// commitThenErrorCheckpointStore simulates a transport failure after SQLite
// has durably committed the checkpoint. It must not cause the writer to retry
// the same usage identity or cross the tool side-effect boundary.
type commitThenErrorCheckpointStore struct {
	*storage.SQLSessionStore
	err      error
	returned atomic.Bool
	attempts atomic.Int32
}

func (s *commitThenErrorCheckpointStore) AppendEvents(ctx context.Context, sessionID string, expectedVersion int64, events []core.SessionEvent) error {
	if err := s.SQLSessionStore.AppendEvents(ctx, sessionID, expectedVersion, events); err != nil {
		return err
	}
	return s.responseLost(events)
}

func (s *commitThenErrorCheckpointStore) AppendEventsFenced(ctx context.Context, fence storage.SessionWriteFence, expectedVersion int64, events []core.SessionEvent) error {
	if err := s.SQLSessionStore.AppendEventsFenced(ctx, fence, expectedVersion, events); err != nil {
		return err
	}
	return s.responseLost(events)
}

func (s *commitThenErrorCheckpointStore) responseLost(events []core.SessionEvent) error {
	for _, event := range events {
		if event.Type == core.EvToolCall && s.returned.CompareAndSwap(false, true) {
			s.attempts.Add(1)
			return s.err
		}
	}
	return nil
}

func (j *durableToolCallJournal) BeginToolInvocation(ctx context.Context, invocation core.ToolInvocation) (core.ToolInvocationRecord, core.ToolInvocationDecision, error) {
	j.attempts.Add(1)
	rows, err := j.db.QueryContext(ctx, "SELECT payload FROM event_chunks WHERE session_id = ? ORDER BY start_seq", invocation.SessionID)
	if err != nil {
		return core.ToolInvocationRecord{}, "", err
	}
	defer rows.Close()
	var events []core.SessionEvent
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return core.ToolInvocationRecord{}, "", err
		}
		for _, line := range strings.Split(payload, "\n") {
			var event core.SessionEvent
			if json.Unmarshal([]byte(line), &event) != nil || event.RunID != invocation.RunID {
				continue
			}
			events = append(events, event)
		}
	}
	if err := rows.Err(); err != nil {
		return core.ToolInvocationRecord{}, "", err
	}
	if j.modelPrefix {
		if err := validateDurableModelToolPrefix(events, invocation.CallID, invocation.CapabilityID); err != nil {
			return core.ToolInvocationRecord{}, "", err
		}
	} else if err := validateNoDurableModelUsage(events); err != nil {
		return core.ToolInvocationRecord{}, "", err
	}
	j.wasDurable.Store(true)
	return core.ToolInvocationRecord{ToolInvocation: invocation, State: core.ToolInvocationStarted}, core.ToolInvocationExecuteNew, nil
}

func validateDurableModelToolPrefix(events []core.SessionEvent, callID, capabilityID string) error {
	for index, event := range events {
		if event.Type != core.EvToolCall {
			continue
		}
		var call core.ToolCallData
		if json.Unmarshal(event.Data, &call) != nil || call.CallID != callID || call.Name != capabilityID {
			continue
		}
		if index < 2 || events[index-2].Type != core.EvAssistantMessage || events[index-1].Type != core.EvRunUsage {
			return fmt.Errorf("tool call %s lacks durable assistant/usage prefix", callID)
		}
		var assistant core.AssistantMessageData
		var usage core.RunUsageData
		if json.Unmarshal(events[index-2].Data, &assistant) != nil || json.Unmarshal(events[index-1].Data, &usage) != nil || !strings.HasPrefix(usage.InvocationID, "model:") {
			return fmt.Errorf("tool call %s has invalid durable usage", callID)
		}
		matches := assistant.ToolCalls
		if assistant.ToolCall != nil && len(matches) == 0 {
			matches = []core.ToolCall{*assistant.ToolCall}
		}
		matched := false
		for _, candidate := range matches {
			if candidate.ID == call.CallID && candidate.Name == call.Name {
				matched = true
				break
			}
		}
		if !matched {
			return fmt.Errorf("tool call %s lacks matching durable assistant call", callID)
		}
		count := 0
		for _, prior := range events {
			if prior.Type != core.EvRunUsage {
				continue
			}
			var candidate core.RunUsageData
			if json.Unmarshal(prior.Data, &candidate) == nil && candidate.InvocationID == usage.InvocationID {
				count++
			}
		}
		if count != 1 {
			return fmt.Errorf("tool call %s usage identity appears %d times", callID, count)
		}
		return nil
	}
	return fmt.Errorf("tool call %s was not durable before journal begin", callID)
}

func validateNoDurableModelUsage(events []core.SessionEvent) error {
	for _, event := range events {
		if event.Type != core.EvRunUsage {
			continue
		}
		var usage core.RunUsageData
		if json.Unmarshal(event.Data, &usage) == nil && strings.HasPrefix(usage.InvocationID, "model:") {
			return fmt.Errorf("fast-router run persisted model usage %q", usage.InvocationID)
		}
	}
	return nil
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
	journal := &durableToolCallJournal{db: fixture.db, modelPrefix: true}
	fixture.server.runtime.ToolJournal = journal
	return &calls, &modelCalls, journal
}

func enableCheckpointFastRouter(t *testing.T, fixture *runWorkerFixture) (*atomic.Int32, *atomic.Int32, *durableToolCallJournal) {
	t.Helper()
	calls, modelCalls, journal := enableCheckpointTool(t, fixture)
	journal.modelPrefix = false
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

func nativeQueuedWitnessCount(t *testing.T, fixture *runWorkerFixture) int {
	t.Helper()
	var count int
	if err := fixture.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM native_queued_tool_effect_witnesses").Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func nativeStrictQueuedWitnessCount(t *testing.T, db *sql.DB, sessionID string) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM native_queued_tool_effect_witnesses WHERE session_id = ?", sessionID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

type nativeStrictApprovalFixture struct {
	api       *Server
	db        *sql.DB
	session   *core.Session
	runID     string
	toolCalls *atomic.Int32
}

func newNativeStrictApprovalFixture(t *testing.T) *nativeStrictApprovalFixture {
	t.Helper()
	ctx := context.Background()
	db := openNativeStrictTestDB(t)
	if _, err := storage.OpenSQLSessionStore(ctx, db, storage.SQLDialectSQLite); err != nil {
		t.Fatal(err)
	}
	accounts, err := storage.NewSQLAccountStore(db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	if err := accounts.CreateTenant(ctx, "acme", "Acme"); err != nil {
		t.Fatal(err)
	}
	if err := accounts.CreateAccount(ctx, storage.Account{
		AccountID: "alice", Email: "alice@example.test", Role: storage.RoleAccountUser,
		TenantID: "acme", Status: storage.AccountActive,
	}, "native-password"); err != nil {
		t.Fatal(err)
	}
	root := nativeStrictTestRoot()
	model := core.ModelSelection{Provider: "approval-worker", Model: "native-approval"}
	name := "Native approval"
	var toolCalls atomic.Int32
	api, err := NewNativeStrictServer(ctx, NativeStrictServerConfig{
		DB: db, Dialect: storage.SQLDialectSQLite,
		Bootstrap: NativeStrictBootstrap{
			Revision: "native-approval-v1", Root: root.Segments(), DefaultProfileID: "native.approval",
			Profiles: []core.AgentProfileLayer{{
				Scope: root, ProfileID: "native.approval", Name: &name, Model: &model,
				AddCapabilities: []string{"payment.release"},
			}},
			Capabilities: []NativeStrictCapability{{Scope: root, Capability: approvalWorkerTool{calls: &toolCalls}}},
			Model:        NativeStrictModel{Selection: model, Adapter: approvalWorkerModel{}},
		},
		MaxWriteDelay: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = api.Shutdown(context.Background()) })
	account, err := accounts.GetAccount(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	principal, err := storage.PrincipalForAccount(account, root.Segments())
	if err != nil {
		t.Fatal(err)
	}
	scope, err := principal.Scope.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "session-native-approval"})
	if err != nil {
		t.Fatal(err)
	}
	session, err := core.NewSession(core.SessionOptions{ID: "session-native-approval", ProfileID: "native.approval", Principal: principal, Scope: scope})
	if err != nil {
		t.Fatal(err)
	}
	if err := api.sessions.Create(ctx, session); err != nil {
		t.Fatal(err)
	}
	runID := "run-native-approval"
	if err := api.runQueue.EnqueueRun(ctx, storage.QueuedRun{RunRecord: storage.RunRecord{
		RunID: runID, SessionID: session.ID(), TenantID: principal.TenantID, SubjectID: principal.SubjectID,
	}, Message: "release payment", MaxAttempts: 3}); err != nil {
		t.Fatal(err)
	}
	return &nativeStrictApprovalFixture{api: api, db: db, session: session, runID: runID, toolCalls: &toolCalls}
}

func TestNativeQueuedWorkerWritesEffectAdmissionWitness(t *testing.T) {
	fixture := newNativeStrictWitnessFixture(t, false)
	claimed, err := fixture.api.RunWorkerOnce(context.Background(), "worker-native-witness")
	if err != nil || !claimed {
		t.Fatalf("worker claimed=%t err=%v", claimed, err)
	}
	terminal, err := fixture.api.runQueue.GetRun(context.Background(), fixture.runID)
	if err != nil || terminal.Status != string(core.RunCompleted) {
		t.Fatalf("terminal=%+v err=%v", terminal, err)
	}
	if fixture.toolCalls.Load() != 1 || nativeStrictQueuedWitnessCount(t, fixture.db, fixture.session.ID()) != 1 {
		t.Fatalf("tool calls=%d witnesses=%d", fixture.toolCalls.Load(), nativeStrictQueuedWitnessCount(t, fixture.db, fixture.session.ID()))
	}
}

func TestGenericQueuedWorkerDoesNotWriteNativeEffectWitness(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	toolCalls, _, _ := enableCheckpointTool(t, fixture)
	journal, err := storage.NewSQLToolInvocationJournal(fixture.db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	fixture.server.runtime.ToolJournal = journal
	enqueueRunHTTP(t, fixture, "write")
	claimed, err := fixture.server.RunWorkerOnce(context.Background(), "worker-generic-no-witness")
	if err != nil || !claimed || toolCalls.Load() != 1 || nativeQueuedWitnessCount(t, fixture) != 0 {
		t.Fatalf("claimed=%t tool calls=%d witnesses=%d err=%v", claimed, toolCalls.Load(), nativeQueuedWitnessCount(t, fixture), err)
	}
}

func TestNativeQueuedApprovalKeepsOrdinaryJournalPath(t *testing.T) {
	fixture := newNativeStrictApprovalFixture(t)
	claimed, err := fixture.api.RunWorkerOnce(context.Background(), "worker-native-approval-first")
	if err != nil || !claimed {
		t.Fatalf("first claimed=%t err=%v", claimed, err)
	}
	approvals, err := fixture.api.approvals.ListApprovals(context.Background(), storage.ApprovalFilter{TenantID: "acme", Status: core.ApprovalPending})
	if err != nil || len(approvals) != 1 {
		t.Fatalf("approvals=%+v err=%v", approvals, err)
	}
	if _, changed, err := fixture.api.approvals.DecideApproval(context.Background(), approvals[0].ID, core.ApprovalApproved, "admin@acme"); err != nil || !changed {
		t.Fatalf("approval changed=%t err=%v", changed, err)
	}
	claimed, err = fixture.api.RunWorkerOnce(context.Background(), "worker-native-approval-resume")
	if err != nil || !claimed {
		t.Fatalf("resume claimed=%t err=%v", claimed, err)
	}
	terminal, err := fixture.api.runQueue.GetRun(context.Background(), fixture.runID)
	if err != nil || terminal.Status != string(core.RunCompleted) || fixture.toolCalls.Load() != 1 || nativeStrictQueuedWitnessCount(t, fixture.db, fixture.session.ID()) != 0 {
		t.Fatalf("terminal=%+v calls=%d witnesses=%d err=%v", terminal, fixture.toolCalls.Load(), nativeStrictQueuedWitnessCount(t, fixture.db, fixture.session.ID()), err)
	}
}

func TestNativeQueuedWorkerRechecksPrincipalBeforeToolAdmission(t *testing.T) {
	fixture := newNativeStrictWitnessFixture(t, true)
	claimed, err := fixture.api.RunWorkerOnce(context.Background(), "worker-native-principal-recheck")
	if err != nil || !claimed {
		t.Fatalf("claimed=%t err=%v", claimed, err)
	}
	terminal, err := fixture.api.runQueue.GetRun(context.Background(), fixture.runID)
	if err != nil || terminal.Status != string(core.RunCancelled) || fixture.toolCalls.Load() != 0 || nativeStrictQueuedWitnessCount(t, fixture.db, fixture.session.ID()) != 0 {
		t.Fatalf("terminal=%+v tools=%d witnesses=%d err=%v", terminal, fixture.toolCalls.Load(), nativeStrictQueuedWitnessCount(t, fixture.db, fixture.session.ID()), err)
	}
}

func TestNativeQueuedToolAdmissionStopsAtCheckpointFailure(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	checkpointErr := errors.New("native checkpoint failed")
	failing := &failingCheckpointStore{SQLSessionStore: fixture.sessions, err: checkpointErr}
	runID := "run-native-checkpoint-failure"
	call := core.ToolCall{ID: "call-native-checkpoint-failure", Name: "checkpoint.write", Args: map[string]any{"value": "one"}}
	for _, entry := range []struct {
		kind core.SessionEventType
		data any
	}{
		{core.EvRunStart, core.RunStartData{}},
		{core.EvStepStart, core.StepData{Index: 0}},
		{core.EvAssistantMessage, core.AssistantMessageData{ToolCall: &call, ToolCalls: []core.ToolCall{call}}},
		{core.EvRunUsage, core.RunUsageData{InvocationID: "model:1"}},
		{core.EvToolCall, core.ToolCallData{CallID: call.ID, Name: call.Name, Args: call.Args}},
	} {
		if _, err := fixture.session.Append(runID, entry.kind, entry.data); err != nil {
			t.Fatal(err)
		}
	}
	fence := storage.SessionWriteFence{
		SessionID: fixture.session.ID(), RunID: runID, TenantID: fixture.principal.TenantID, SubjectID: fixture.principal.SubjectID,
		WorkerID: "worker-native-checkpoint-failure", QueueGeneration: 1, LeaseHolder: "lease-native-checkpoint-failure",
	}
	writer, err := storage.NewFencedWriteBehind(failing, fence, fixture.session, 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	inner := &countingToolJournal{}
	failure := &toolCheckpointFailure{}
	var cancelled atomic.Bool
	journal := &nativeQueuedToolInvocationJournal{
		ToolInvocationJournal: inner, writer: writer, cancel: func() { cancelled.Store(true) }, failure: failure,
	}
	if _, _, err := journal.BeginToolInvocation(context.Background(), core.ToolInvocation{}); !errors.Is(err, checkpointErr) {
		t.Fatalf("error=%v", err)
	}
	if inner.begins.Load() != 0 || !cancelled.Load() || !errors.Is(failure.Err(), checkpointErr) {
		t.Fatalf("begins=%d cancelled=%t failure=%v", inner.begins.Load(), cancelled.Load(), failure.Err())
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
	if store.attempts.Load() != 1 || store.usageAttempts.Load() != 1 || inner.begins.Load() != 0 || toolCalls.Load() != 0 || modelCalls.Load() != 1 {
		t.Fatalf("checkpoint failure crossed side-effect boundary: writes=%d usage=%d begins=%d tools=%d models=%d", store.attempts.Load(), store.usageAttempts.Load(), inner.begins.Load(), toolCalls.Load(), modelCalls.Load())
	}
	if err := validateDurableModelToolPrefix(store.attemptedToolCallBatch(), "call-checkpoint", "checkpoint.write"); err != nil {
		t.Fatalf("checkpoint writer did not receive one complete assistant/usage/tool-call batch: %v", err)
	}
	persisted, err := fixture.sessions.Load(context.Background(), fixture.session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if persisted.Version() != 0 || len(persisted.Events()) != 0 {
		t.Fatalf("failed checkpoint left partial durable history: version=%d events=%#v", persisted.Version(), persisted.Events())
	}
}

func TestCheckpointCommitResponseLostDoesNotDuplicateDurableUsage(t *testing.T) {
	for _, test := range []struct {
		name string
		run  func(t *testing.T, fixture *runWorkerFixture, checkpointErr error) string
	}{
		{
			name: "sync",
			run: func(t *testing.T, fixture *runWorkerFixture, checkpointErr error) string {
				t.Helper()
				request := httptest.NewRequest(http.MethodPost, "/v1/sessions/"+fixture.session.ID()+"/runs", strings.NewReader(`{"message":"write"}`))
				request.Header.Set("Content-Type", "application/json")
				response := httptest.NewRecorder()
				fixture.server.Handler().ServeHTTP(response, request)
				if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "event: store/error") || !strings.Contains(response.Body.String(), checkpointErr.Error()) {
					t.Fatalf("sync response=%d body=%s", response.Code, response.Body.String())
				}
				runs, err := fixture.queue.ListRuns(context.Background(), fixture.session.ID(), 10)
				if err != nil || len(runs) != 1 || runs[0].Status != string(core.RunFailed) || runs[0].ErrorCode != "store_error" {
					t.Fatalf("sync run control=%#v err=%v", runs, err)
				}
				return runs[0].RunID
			},
		},
		{
			name: "queued",
			run: func(t *testing.T, fixture *runWorkerFixture, checkpointErr error) string {
				t.Helper()
				record := enqueueRunHTTP(t, fixture, "write")
				claimed, err := fixture.server.RunWorkerOnce(context.Background(), "checkpoint-response-lost-worker")
				if !claimed || !errors.Is(err, checkpointErr) {
					t.Fatalf("queued claimed=%t err=%v", claimed, err)
				}
				terminal, err := fixture.queue.GetRun(context.Background(), record.RunID)
				if err != nil || terminal.Status != string(core.RunFailed) || terminal.ErrorCode != "store_error" {
					t.Fatalf("queued run control=%#v err=%v", terminal, err)
				}
				return record.RunID
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newRunWorkerFixture(t)
			toolCalls, modelCalls, _ := enableCheckpointTool(t, fixture)
			checkpointErr := errors.New("checkpoint response lost")
			store := &commitThenErrorCheckpointStore{SQLSessionStore: fixture.sessions, err: checkpointErr}
			inner := &countingToolJournal{}
			fixture.server.sessions = store
			fixture.server.runtime.ToolJournal = inner

			runID := test.run(t, fixture, checkpointErr)
			if store.attempts.Load() != 1 || inner.begins.Load() != 0 || toolCalls.Load() != 0 || modelCalls.Load() != 1 {
				t.Fatalf("commit-response-lost crossed side-effect boundary: writes=%d begins=%d tools=%d models=%d", store.attempts.Load(), inner.begins.Load(), toolCalls.Load(), modelCalls.Load())
			}
			first, err := fixture.sessions.Load(context.Background(), fixture.session.ID())
			if err != nil {
				t.Fatal(err)
			}
			if err := validateDurableModelToolPrefix(eventsForRun(first.Events(), runID), "call-checkpoint", "checkpoint.write"); err != nil {
				t.Fatalf("first durable reload is missing the exact checkpoint prefix: %v", err)
			}
			second, err := fixture.sessions.Load(context.Background(), fixture.session.ID())
			if err != nil {
				t.Fatal(err)
			}
			durableEvents := eventsForRun(second.Events(), runID)
			if err := validateDurableModelToolPrefix(durableEvents, "call-checkpoint", "checkpoint.write"); err != nil {
				t.Fatalf("second durable reload duplicated or lost usage identity: %v", err)
			}
			for _, event := range durableEvents {
				if event.Type == core.EvRunError || event.Type == core.EvRunEnd {
					t.Fatalf("store-error path persisted a second terminal suffix after an unknown checkpoint response: %#v", event)
				}
			}
		})
	}
}

func eventsForRun(events []core.SessionEvent, runID string) []core.SessionEvent {
	filtered := make([]core.SessionEvent, 0, len(events))
	for _, event := range events {
		if event.RunID == runID {
			filtered = append(filtered, event)
		}
	}
	return filtered
}
