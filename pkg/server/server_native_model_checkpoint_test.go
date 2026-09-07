package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
)

type nativeModelAdmissionProbe struct {
	db        *sql.DB
	sessionID string
	calls     int
}

func (m *nativeModelAdmissionProbe) Provider() string { return "checkpoint-test" }

func (m *nativeModelAdmissionProbe) Stream(_ context.Context, _ core.GenerateOptions, emit func(core.StreamChunk)) error {
	m.calls++
	var attempts int
	if err := m.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM native_queued_model_invocations WHERE session_id = ?", m.sessionID).Scan(&attempts); err != nil {
		return err
	}
	var version int64
	if err := m.db.QueryRowContext(context.Background(), "SELECT version FROM sessions WHERE id = ?", m.sessionID).Scan(&version); err != nil {
		return err
	}
	if attempts != 1 || version != 3 {
		return fmt.Errorf("provider observed attempts=%d version=%d", attempts, version)
	}
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: "done"})
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
	return nil
}

type nativeModelOutcomeFenceLoss struct {
	db    *sql.DB
	runID string
	calls int
}

type nativeModelToolLimit struct{ calls int }

func (m *nativeModelToolLimit) Provider() string { return "checkpoint-test" }

func (m *nativeModelToolLimit) Stream(_ context.Context, _ core.GenerateOptions, emit func(core.StreamChunk)) error {
	m.calls++
	first := core.ToolCall{ID: "call-native-tool-limit-1", Name: "checkpoint.write", Args: map[string]any{"value": "one"}}
	second := core.ToolCall{ID: "call-native-tool-limit-2", Name: "checkpoint.write", Args: map[string]any{"value": "two"}}
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &first, ToolCalls: []core.ToolCall{first, second}})
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
	return nil
}

type nativeModelReportedUsageError struct{ calls int }

func (m *nativeModelReportedUsageError) Provider() string { return "checkpoint-test" }

func (m *nativeModelReportedUsageError) Stream(_ context.Context, _ core.GenerateOptions, emit func(core.StreamChunk)) error {
	m.calls++
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: "partial", Usage: &core.TokenUsage{InputTokens: 13, OutputTokens: 2}})
	return errors.New("native provider interrupted after usage")
}

func (m *nativeModelOutcomeFenceLoss) Provider() string { return "checkpoint-test" }

func (m *nativeModelOutcomeFenceLoss) Stream(_ context.Context, _ core.GenerateOptions, emit func(core.StreamChunk)) error {
	m.calls++
	trigger := fmt.Sprintf(`CREATE TRIGGER lose_native_model_outcome_fence AFTER INSERT ON native_queued_model_invocation_outcomes
		BEGIN UPDATE run_queue SET worker_id = 'worker-replaced' WHERE run_id = '%s'; END`, m.runID)
	if _, err := m.db.ExecContext(context.Background(), trigger); err != nil {
		return err
	}
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: "done"})
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
	return nil
}

func TestNativeQueuedModelOutcomeEndRejectsMalformedUsage(t *testing.T) {
	for _, test := range []struct {
		name   string
		events []core.SessionEvent
	}{
		{
			name: "malformed_usage",
			events: []core.SessionEvent{
				{RunID: "run-native-model-scanner", Type: core.EvAssistantMessage},
				{RunID: "run-native-model-scanner", Type: core.EvRunUsage, Data: []byte(`{`)},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if outcomeEnd, err := nativeQueuedModelOutcomeEnd(test.events); err == nil || outcomeEnd != 0 {
				t.Fatalf("outcomeEnd=%d err=%v", outcomeEnd, err)
			}
		})
	}
}

func TestNativeQueuedModelOutcomeEndAllowsReportedUsageWithoutAssistantOutcome(t *testing.T) {
	events := []core.SessionEvent{{RunID: "run-native-model-scanner", Type: core.EvRunUsage, Data: []byte(`{"invocation_id":"model:2"}`)}}
	if outcomeEnd, err := nativeQueuedModelOutcomeEnd(events); err != nil || outcomeEnd != 0 {
		t.Fatalf("outcomeEnd=%d err=%v", outcomeEnd, err)
	}
}

func TestNativeQueuedModelOutcomeEndAcceptsOneModelPairWithOrdinaryAssistantSuffix(t *testing.T) {
	events := []core.SessionEvent{
		{RunID: "run-native-model-scanner", Type: core.EvAssistantMessage},
		{RunID: "run-native-model-scanner", Type: core.EvRunUsage, Data: []byte(`{"invocation_id":"model:2"}`)},
		{RunID: "run-native-model-scanner", Type: core.EvAssistantMessage},
		{RunID: "run-native-model-scanner", Type: core.EvStepEnd},
		{RunID: "run-native-model-scanner", Type: core.EvRunEnd},
	}
	if outcomeEnd, err := nativeQueuedModelOutcomeEnd(events); err != nil || outcomeEnd != 2 {
		t.Fatalf("outcomeEnd=%d err=%v", outcomeEnd, err)
	}
}

func TestNativeStrictQueuedWorkerAtomicallyPersistsToolLimitAssistantSuffix(t *testing.T) {
	fixture := newNativeStrictWitnessFixture(t, false)
	limit := 1
	if err := fixture.api.runtime.Policy.Bind(core.PolicyLayer{Scope: fixture.session.Scope(), MaxToolCalls: &limit}); err != nil {
		t.Fatal(err)
	}
	model := &nativeModelToolLimit{}
	fixture.api.runtime.Models = core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) {
		return model, nil
	})
	claimed, err := fixture.api.RunWorkerOnce(context.Background(), "worker-native-tool-limit")
	if err != nil || !claimed {
		t.Fatalf("claimed=%t err=%v", claimed, err)
	}
	terminal, err := fixture.api.runQueue.GetRun(context.Background(), fixture.runID)
	if err != nil || terminal.Status != string(core.RunLimited) {
		t.Fatalf("terminal=%+v err=%v", terminal, err)
	}
	loaded, err := fixture.api.sessions.Load(context.Background(), fixture.session.ID())
	if err != nil {
		t.Fatal(err)
	}
	limitAssistant := 0
	for _, event := range loaded.Events() {
		if event.Type != core.EvAssistantMessage || !strings.Contains(string(event.Data), "Tool call budget reached; the run stopped.") {
			continue
		}
		limitAssistant++
	}
	if model.calls != 1 || fixture.toolCalls.Load() != 1 || limitAssistant != 1 {
		t.Fatalf("model_calls=%d tool_calls=%d tool_limit_assistants=%d", model.calls, fixture.toolCalls.Load(), limitAssistant)
	}
	var attempts, outcomes int
	if err := fixture.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM native_queued_model_invocations WHERE session_id = ?", fixture.session.ID()).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if err := fixture.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM native_queued_model_invocation_outcomes WHERE session_id = ?", fixture.session.ID()).Scan(&outcomes); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 || outcomes != 1 {
		t.Fatalf("attempts=%d outcomes=%d", attempts, outcomes)
	}
}

func TestNativeStrictQueuedWorkerPersistsReportedUsageAfterProviderFailureWithoutOutcome(t *testing.T) {
	fixture := newNativeStrictWitnessFixture(t, false)
	model := &nativeModelReportedUsageError{}
	fixture.api.runtime.Models = core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) {
		return model, nil
	})
	claimed, err := fixture.api.RunWorkerOnce(context.Background(), "worker-native-model-reported-usage-error")
	if !claimed || err != nil {
		t.Fatalf("claimed=%t err=%v", claimed, err)
	}
	terminal, err := fixture.api.runQueue.GetRun(context.Background(), fixture.runID)
	if err != nil || terminal.Status != string(core.RunFailed) || terminal.ErrorCode != "model_failed" {
		t.Fatalf("terminal=%+v err=%v", terminal, err)
	}
	loaded, err := fixture.api.sessions.Load(context.Background(), fixture.session.ID())
	if err != nil {
		t.Fatal(err)
	}
	usage, failed, ended := 0, false, false
	for _, event := range loaded.Events() {
		switch event.Type {
		case core.EvRunUsage:
			if strings.Contains(string(event.Data), `"invocation_id":"model:`) {
				usage++
			}
		case core.EvRunError:
			failed = strings.Contains(string(event.Data), `"code":"model_failed"`)
		case core.EvRunEnd:
			ended = strings.Contains(string(event.Data), `"status":"failed"`)
		}
	}
	if usage != 1 || !failed || !ended {
		t.Fatalf("reported usage=%d failed=%t ended=%t events=%+v", usage, failed, ended, loaded.Events())
	}
	var attempts, outcomes int
	if err := fixture.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM native_queued_model_invocations WHERE session_id = ?", fixture.session.ID()).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if err := fixture.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM native_queued_model_invocation_outcomes WHERE session_id = ?", fixture.session.ID()).Scan(&outcomes); err != nil {
		t.Fatal(err)
	}
	if model.calls != 1 || attempts != 1 || outcomes != 0 {
		t.Fatalf("model_calls=%d attempts=%d outcomes=%d", model.calls, attempts, outcomes)
	}
	if claimed, err := fixture.api.RunWorkerOnce(context.Background(), "worker-native-model-reported-usage-restart"); err != nil || claimed || model.calls != 1 {
		t.Fatalf("restart claimed=%t err=%v model_calls=%d", claimed, err, model.calls)
	}
}

func TestNativeStrictRejectsSynchronousRunPath(t *testing.T) {
	fixture := newNativeStrictWitnessFixture(t, false)
	token, err := fixture.accounts.CreateToken(context.Background(), "alice", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/sessions/"+fixture.session.ID()+"/runs", strings.NewReader(`{"message":"write"}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+token)
	response := httptest.NewRecorder()
	fixture.api.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusMethodNotAllowed || !strings.Contains(response.Body.String(), "queued run endpoint") {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestNativeStrictQueuedWorkerPersistsModelAttemptsAndOutcomes(t *testing.T) {
	fixture := newNativeStrictWitnessFixture(t, false)
	claimed, err := fixture.api.RunWorkerOnce(context.Background(), "worker-native-model-outcome")
	if err != nil || !claimed {
		t.Fatalf("claimed=%t err=%v", claimed, err)
	}
	terminal, err := fixture.api.runQueue.GetRun(context.Background(), fixture.runID)
	if err != nil {
		t.Fatal(err)
	}
	loaded, loadErr := fixture.api.sessions.Load(context.Background(), fixture.session.ID())
	if terminal.Status != string(core.RunCompleted) {
		if loadErr != nil {
			t.Fatalf("terminal=%+v load=%v", terminal, loadErr)
		}
		t.Fatalf("terminal=%+v events=%+v", terminal, loaded.Events())
	}
	var attempts, outcomes int
	if err := fixture.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM native_queued_model_invocations WHERE session_id = ?", fixture.session.ID()).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if err := fixture.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM native_queued_model_invocation_outcomes WHERE session_id = ?", fixture.session.ID()).Scan(&outcomes); err != nil {
		t.Fatal(err)
	}
	if attempts != 2 || outcomes != 2 || fixture.modelCalls.Load() != 2 {
		t.Fatalf("attempts=%d outcomes=%d model_calls=%d", attempts, outcomes, fixture.modelCalls.Load())
	}
}

func TestNativeQueuedModelAdmissionIsDurableBeforeProvider(t *testing.T) {
	fixture := newNativeStrictWitnessFixture(t, false)
	probe := &nativeModelAdmissionProbe{db: fixture.db, sessionID: fixture.session.ID()}
	fixture.api.runtime.Models = core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) {
		return probe, nil
	})
	claimed, err := fixture.api.RunWorkerOnce(context.Background(), "worker-native-model-admission")
	if err != nil || !claimed {
		t.Fatalf("claimed=%t err=%v", claimed, err)
	}
	terminal, err := fixture.api.runQueue.GetRun(context.Background(), fixture.runID)
	if err != nil || terminal.Status != string(core.RunCompleted) || probe.calls != 1 {
		t.Fatalf("terminal=%+v calls=%d err=%v", terminal, probe.calls, err)
	}
	var outcomes int
	if err := fixture.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM native_queued_model_invocation_outcomes WHERE session_id = ?", fixture.session.ID()).Scan(&outcomes); err != nil || outcomes != 1 {
		t.Fatalf("outcomes=%d err=%v", outcomes, err)
	}
}

func TestNativeQueuedModelOutcomeFenceLossDoesNotFallback(t *testing.T) {
	fixture := newNativeStrictWitnessFixture(t, false)
	model := &nativeModelOutcomeFenceLoss{db: fixture.db, runID: fixture.runID}
	fixture.api.runtime.Models = core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) {
		return model, nil
	})
	claimed, err := fixture.api.RunWorkerOnce(context.Background(), "worker-native-model-outcome-loss")
	if !claimed || !errors.Is(err, storage.ErrSessionWriteFenceLost) {
		t.Fatalf("claimed=%t err=%v", claimed, err)
	}
	if model.calls != 1 {
		t.Fatalf("model calls=%d", model.calls)
	}
	var attempts, outcomes int
	if err := fixture.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM native_queued_model_invocations WHERE session_id = ?", fixture.session.ID()).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if err := fixture.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM native_queued_model_invocation_outcomes WHERE session_id = ?", fixture.session.ID()).Scan(&outcomes); err != nil {
		t.Fatal(err)
	}
	loaded, loadErr := fixture.api.sessions.Load(context.Background(), fixture.session.ID())
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if attempts != 1 || outcomes != 0 || loaded.Version() != 3 {
		t.Fatalf("attempts=%d outcomes=%d version=%d", attempts, outcomes, loaded.Version())
	}
}

func TestNativeQueuedModelDuplicateAdmissionDoesNotCallProvider(t *testing.T) {
	fixture := newNativeStrictWitnessFixture(t, false)
	if _, err := fixture.db.ExecContext(context.Background(), `CREATE TRIGGER duplicate_native_model_attempt BEFORE INSERT ON native_queued_model_invocations
		BEGIN
			INSERT INTO native_queued_model_invocations (
				protocol, tenant_id, subject_id, session_id, run_id, invocation_id, step_index, step_start_seq,
				session_version_at_admission, request_json, request_sha256, authorization_epoch, queue_generation,
				lease_holder_sha256, run_start_seq, profile_snapshot_id, capability_snapshot_id,
				composition_revision, assignment_revision, composition_sha256, bootstrap_revision,
				model_contract_sha256, created_at
			) VALUES (
				NEW.protocol, NEW.tenant_id, NEW.subject_id, NEW.session_id, NEW.run_id, NEW.invocation_id,
				NEW.step_index, NEW.step_start_seq, NEW.session_version_at_admission, NEW.request_json,
				NEW.request_sha256, NEW.authorization_epoch, NEW.queue_generation, NEW.lease_holder_sha256,
				NEW.run_start_seq, NEW.profile_snapshot_id, NEW.capability_snapshot_id,
				NEW.composition_revision, NEW.assignment_revision, NEW.composition_sha256,
				NEW.bootstrap_revision, NEW.model_contract_sha256, NEW.created_at
			);
		END`); err != nil {
		t.Fatal(err)
	}
	claimed, err := fixture.api.RunWorkerOnce(context.Background(), "worker-native-model-duplicate")
	if err != nil || !claimed {
		t.Fatalf("claimed=%t err=%v", claimed, err)
	}
	terminal, err := fixture.api.runQueue.GetRun(context.Background(), fixture.runID)
	if err != nil || terminal.Status != string(core.RunFailed) || terminal.ErrorCode != "model_gate_rejected" {
		t.Fatalf("terminal=%+v err=%v", terminal, err)
	}
	var attempts, outcomes int
	if err := fixture.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM native_queued_model_invocations WHERE session_id = ?", fixture.session.ID()).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if err := fixture.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM native_queued_model_invocation_outcomes WHERE session_id = ?", fixture.session.ID()).Scan(&outcomes); err != nil {
		t.Fatal(err)
	}
	if fixture.modelCalls.Load() != 0 || attempts != 1 || outcomes != 0 {
		t.Fatalf("model_calls=%d attempts=%d outcomes=%d", fixture.modelCalls.Load(), attempts, outcomes)
	}
}

func TestGenericQueuedRunDoesNotWriteNativeModelEvidence(t *testing.T) {
	fixture := newRunWorkerFixture(t)
	_, _, _ = enableCheckpointTool(t, fixture)
	record := enqueueRunHTTP(t, fixture, "write")
	claimed, err := fixture.server.RunWorkerOnce(context.Background(), "worker-generic-no-native-model")
	if err != nil || !claimed {
		t.Fatalf("claimed=%t err=%v", claimed, err)
	}
	terminal, err := fixture.queue.GetRun(context.Background(), record.RunID)
	if err != nil || terminal.Status != string(core.RunCompleted) {
		t.Fatalf("terminal=%+v err=%v", terminal, err)
	}
	var attempts, outcomes int
	if err := fixture.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM native_queued_model_invocations").Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if err := fixture.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM native_queued_model_invocation_outcomes").Scan(&outcomes); err != nil {
		t.Fatal(err)
	}
	if attempts != 0 || outcomes != 0 {
		t.Fatalf("attempts=%d outcomes=%d", attempts, outcomes)
	}
}
