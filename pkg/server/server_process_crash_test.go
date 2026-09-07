package server

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cc-auto-agent/harness-core/internal/testdb"
	"github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/provider/openai"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"go.opentelemetry.io/otel/trace"
)

// The child is killed only after PostgreSQL commits the synthetic effect.
// Neither graceful Shutdown nor reopening a pool substitutes for Process.Kill.
func TestRunWorkerProcessCrashRecovery(t *testing.T) {
	for _, point := range []string{"effect_committed", "journal_completed"} {
		t.Run(point, func(t *testing.T) { runProcessCrashRecovery(t, point, false) })
	}
}

// This uses a real Native Strict worker in a separate OS process. The child is
// killed after the SQL tool journal commits but before Core can append the
// tool/result; the replacement must use the sealed recovery path, not replay
// the non-idempotent tool.
func TestNativeQueuedCompletedToolJournalProcessCrashRecovery(t *testing.T) {
	runNativeQueuedCompletedToolProcessCrashRecovery(t, false)
}

func TestNativeQueuedCompletedToolPostProviderProcessCrashRecovery(t *testing.T) {
	runNativeQueuedCompletedToolProcessCrashRecovery(t, true)
}

func runNativeQueuedCompletedToolProcessCrashRecovery(t *testing.T, postProviderCrash bool) {
	// Keep the child-only bootstrap honest before spending a PostgreSQL process
	// and an OS kill on it. This reuses the established Native Strict fixture
	// manifest rather than maintaining a second hand-written capability schema.
	bootstrap, _ := newNativeRecoveryCrashServer(t, openNativeStrictTestDB(t), storage.SQLDialectSQLite, &nativeRecoveryCrashModel{})
	defer bootstrap.Shutdown(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	open := testdb.Postgres(t)
	db := open()
	if _, err := db.ExecContext(ctx, `CREATE TABLE native_recovery_crash_effects (call_id TEXT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE native_recovery_crash_model_calls (stage TEXT PRIMARY KEY, calls BIGINT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	var schema string
	if err := db.QueryRowContext(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	start := func(phase string) (*exec.Cmd, <-chan error) {
		t.Helper()
		executable, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.CommandContext(ctx, executable, "-test.run=^TestNativeQueuedCompletedToolCrashHelper$", "-test.timeout=100s")
		hideCrashHelperWindow(cmd)
		cmd.Env = append(os.Environ(), "HARNESS_NATIVE_COMPLETED_TOOL_CRASH_HELPER=1", "HARNESS_CRASH_SCHEMA="+schema, "HARNESS_NATIVE_RECOVERY_PHASE="+phase)
		var output bytes.Buffer
		cmd.Stdout, cmd.Stderr = &output, &output
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			err := cmd.Wait()
			if err != nil {
				done <- fmt.Errorf("%w: %s", err, output.String())
				return
			}
			done <- nil
		}()
		t.Cleanup(func() { _ = cmd.Process.Kill() })
		return cmd, done
	}
	first, firstDone := start("crash")
	deadline := time.Now().Add(20 * time.Second)
	for {
		var state string
		err := db.QueryRowContext(ctx, `SELECT state FROM tool_invocations WHERE session_id=$1 AND run_id=$2 AND call_id=$3`, nativeRecoveryCrashSessionID, nativeRecoveryCrashRunID, nativeRecoveryCrashCallID).Scan(&state)
		if err == nil && state == string(core.ToolInvocationCompleted) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for completed native tool journal: %v", err)
		}
		select {
		case err := <-firstDone:
			t.Fatalf("crash child exited before completed journal: %v", err)
		case <-time.After(10 * time.Millisecond):
		}
	}
	if err := first.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := <-firstDone; err == nil || first.ProcessState.Success() {
		t.Fatalf("native crash child did not exit abnormally: %v", err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE run_queue SET lease_expires_at=1 WHERE run_id=$1`, nativeRecoveryCrashRunID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE session_leases SET expires_at=1 WHERE session_id=$1`, nativeRecoveryCrashSessionID); err != nil {
		t.Fatal(err)
	}
	queue, err := storage.NewSQLRunControlStore(db, storage.SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	if requeued, failed, err := queue.RecoverExpiredRunClaims(ctx, time.Now().UTC()); err != nil || requeued != 1 || failed != 0 {
		t.Fatalf("requeue=%d failed=%d err=%v", requeued, failed, err)
	}
	if postProviderCrash {
		middle, middleDone := start("postprovider_crash")
		deadline = time.Now().Add(20 * time.Second)
		for {
			var calls int64
			err := db.QueryRowContext(ctx, "SELECT COALESCE(SUM(calls), 0) FROM native_recovery_crash_model_calls").Scan(&calls)
			if err == nil && calls == 2 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for post-provider model receipt: calls=%d err=%v", calls, err)
			}
			select {
			case err := <-middleDone:
				t.Fatalf("post-provider child exited before adapter receipt: %v", err)
			case <-time.After(10 * time.Millisecond):
			}
		}
		if err := middle.Process.Kill(); err != nil {
			t.Fatal(err)
		}
		if err := <-middleDone; err == nil || middle.ProcessState.Success() {
			t.Fatalf("post-provider child did not exit abnormally: %v", err)
		}
		if _, err := db.ExecContext(ctx, "UPDATE run_queue SET lease_expires_at=1 WHERE run_id=$1", nativeRecoveryCrashRunID); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, "UPDATE session_leases SET expires_at=1 WHERE session_id=$1", nativeRecoveryCrashSessionID); err != nil {
			t.Fatal(err)
		}
		if requeued, failed, err := queue.RecoverExpiredRunClaims(ctx, time.Now().UTC()); err != nil || requeued != 1 || failed != 0 {
			t.Fatalf("post-provider requeue=%d failed=%d err=%v", requeued, failed, err)
		}
	}
	finalPhase := "recover"
	if postProviderCrash {
		finalPhase = "recover_unknown"
	}
	second, secondDone := start(finalPhase)
	if err := <-secondDone; err != nil {
		t.Fatalf("replacement child failed: %v", err)
	}
	if second.ProcessState == nil || !second.ProcessState.Success() {
		t.Fatalf("replacement child did not exit successfully: %#v", second.ProcessState)
	}
	var effects, models, sidecars, attempts, outcomes int64
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM native_recovery_crash_effects`).Scan(&effects); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(SUM(calls), 0) FROM native_recovery_crash_model_calls`).Scan(&models); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM completed_tool_result_recovery_sidecars WHERE session_id=$1 AND run_id=$2`, nativeRecoveryCrashSessionID, nativeRecoveryCrashRunID).Scan(&sidecars); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM native_queued_model_invocations WHERE session_id=$1 AND run_id=$2`, nativeRecoveryCrashSessionID, nativeRecoveryCrashRunID).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM native_queued_model_invocation_outcomes WHERE session_id=$1 AND run_id=$2`, nativeRecoveryCrashSessionID, nativeRecoveryCrashRunID).Scan(&outcomes); err != nil {
		t.Fatal(err)
	}
	store, err := storage.OpenSQLSessionStore(ctx, db, storage.SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	session, err := store.Load(ctx, nativeRecoveryCrashSessionID)
	if err != nil {
		t.Fatal(err)
	}
	results := 0
	for _, event := range session.Events() {
		if event.RunID == nativeRecoveryCrashRunID && event.Type == core.EvToolResult {
			results++
		}
	}
	run, err := queue.GetRun(ctx, nativeRecoveryCrashRunID)
	wantStatus := string(core.RunCompleted)
	if postProviderCrash {
		wantStatus = string(core.RunFailed)
	}
	wantOutcomes := int64(2)
	if postProviderCrash {
		wantOutcomes = 1
	}
	if err != nil || run.Status != wantStatus || effects != 1 || models != 2 || sidecars != 1 || results != 1 || attempts != 2 || outcomes != wantOutcomes {
		t.Fatalf("run=%+v effects=%d models=%d sidecars=%d results=%d attempts=%d outcomes=%d err=%v", run, effects, models, sidecars, results, attempts, outcomes, err)
	}
}

func TestLiveRunWorkerProcessCrashRecovery(t *testing.T) {
	if os.Getenv("HARNESS_ACCEPTANCE_LIVE_SERIAL") != "1" {
		t.Skip("explicit serial live-model acceptance is not enabled")
	}
	for _, point := range []string{"effect_committed", "journal_completed"} {
		if !t.Run(point, func(t *testing.T) { runProcessCrashRecovery(t, point, true) }) {
			return
		}
	}
}

type processCrashEvidence struct {
	PID          int                  `json:"pid"`
	Phase        string               `json:"phase"`
	Blocked      bool                 `json:"blocked_at_fault"`
	ActiveCallID string               `json:"active_call_id,omitempty"`
	ModelCalls   int                  `json:"model_calls"`
	InputTokens  int64                `json:"input_tokens"`
	OutputTokens int64                `json:"output_tokens"`
	ModelMS      []int64              `json:"model_request_ms"`
	Spans        []serialSpanEvidence `json:"ended_spans"`
	ActiveTrace  string               `json:"active_trace_id,omitempty"`
	ActiveSpan   string               `json:"active_span_id,omitempty"`
}

type processCrashJournalEvidence struct {
	Rows      int64  `json:"rows"`
	CallID    string `json:"call_id,omitempty"`
	State     string `json:"state,omitempty"`
	HasResult bool   `json:"has_result"`
}

type processCrashQueueEvidence struct {
	Rows       int64  `json:"rows"`
	WorkerID   string `json:"worker_id,omitempty"`
	Attempt    int    `json:"attempt,omitempty"`
	Generation int64  `json:"generation,omitempty"`
}

type processCrashEventEvidence struct {
	Seq    int64                 `json:"seq"`
	Type   core.SessionEventType `json:"type"`
	CallID string                `json:"call_id,omitempty"`
	Code   string                `json:"code,omitempty"`
	Status core.RunStatus        `json:"status,omitempty"`
}

type processCrashAuditEvidence struct {
	FaultPoint       string                      `json:"fault_point"`
	LiveModel        bool                        `json:"live_model"`
	SessionID        string                      `json:"session_id"`
	RunID            string                      `json:"run_id"`
	BeforeKill       processCrashEvidence        `json:"before_kill"`
	Replacement      processCrashEvidence        `json:"replacement"`
	HardKillExitCode int                         `json:"hard_kill_exit_code"`
	HardKillAbnormal bool                        `json:"hard_kill_abnormal"`
	DurableVersion   int64                       `json:"durable_version_before_kill"`
	DurablePrefix    []processCrashEventEvidence `json:"durable_prefix_before_kill"`
	EffectsBefore    int64                       `json:"effects_before"`
	EffectsAfter     int64                       `json:"effects_after"`
	EffectVersion    int64                       `json:"effect_observed_durable_version"`
	JournalBefore    processCrashJournalEvidence `json:"journal_before"`
	JournalAfter     processCrashJournalEvidence `json:"journal_after"`
	QueueBefore      processCrashQueueEvidence   `json:"queue_before"`
	QueueAfter       processCrashQueueEvidence   `json:"queue_after"`
	RecoveredClaims  int64                       `json:"recovered_claims"`
	RecoveryFailures int64                       `json:"recovery_failures"`
	RunStatus        string                      `json:"run_status"`
	RunErrorCode     string                      `json:"run_error_code"`
	HTTPMatchesSQL   bool                        `json:"http_history_matches_sql"`
	FinalSQLHistory  []processCrashEventEvidence `json:"final_sql_history"`
}

func runProcessCrashRecovery(t *testing.T, point string, live bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	open := testdb.Postgres(t)
	api := newSerialAuditedAPI(t, open(), &processCrashModel{}, &atomic.Int32{})
	configureProcessCrash(t, api, nil)
	if _, err := api.db.ExecContext(ctx, `CREATE TABLE crash_effects (call_id TEXT NOT NULL, durable_version BIGINT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	var schema string
	if err := api.db.QueryRowContext(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	sessionID := serialSession(t, api, "crash.agent")
	var run storage.RunRecord
	if err := acceptanceRequest(&http.Client{Timeout: 20 * time.Second}, http.MethodPost,
		api.http.URL+"/v1/sessions/"+sessionID+"/runs/async", map[string]string{"message": "Call crash.effect exactly once with n=7. Then report its result."}, &run); err != nil {
		t.Fatal(err)
	}
	checkpoints := make(chan processCrashEvidence, 2)
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var evidence processCrashEvidence
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2<<20)).Decode(&evidence); err != nil {
			http.Error(w, "invalid test evidence", http.StatusBadRequest)
			return
		}
		select {
		case checkpoints <- evidence:
		case <-r.Context().Done():
		}
	}))
	defer collector.Close()
	start := func(phase string) (*exec.Cmd, <-chan error) {
		t.Helper()
		executable, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.CommandContext(ctx, executable, "-test.run=^TestRunWorkerCrashHelper$", "-test.timeout=100s")
		hideCrashHelperWindow(cmd)
		cmd.Env = append(os.Environ(), "HARNESS_CRASH_HELPER=1", "HARNESS_CRASH_SCHEMA="+schema,
			"HARNESS_CRASH_COLLECTOR="+collector.URL, "HARNESS_CRASH_PHASE="+phase,
			"HARNESS_CRASH_POINT="+point, fmt.Sprintf("HARNESS_CRASH_LIVE=%t", live))
		// Child logs stay local: no arbitrary upstream errors enter evidence.
		var output bytes.Buffer
		cmd.Stdout, cmd.Stderr = &output, &output
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		t.Cleanup(func() { _ = cmd.Process.Kill() })
		return cmd, done
	}
	first, firstDone := start("crash")
	var before processCrashEvidence
	select {
	case before = <-checkpoints:
	case err := <-firstDone:
		t.Fatalf("crash child exited before committed effect checkpoint: %v", err)
	case <-ctx.Done():
		t.Fatal("timed out waiting for committed effect checkpoint")
	}
	var effectsBefore, effectVersionBefore, effectVersionMax, versionBefore int64
	if err := api.db.QueryRowContext(ctx, `
		SELECT COUNT(*), COALESCE(MIN(durable_version), -1), COALESCE(MAX(durable_version), -1)
		FROM crash_effects`).Scan(&effectsBefore, &effectVersionBefore, &effectVersionMax); err != nil {
		t.Fatal(err)
	}
	if effectVersionBefore != effectVersionMax {
		t.Fatalf("effects observed inconsistent durable versions: min=%d max=%d", effectVersionBefore, effectVersionMax)
	}
	if err := api.db.QueryRowContext(ctx, `SELECT version FROM sessions WHERE id=$1`, sessionID).Scan(&versionBefore); err != nil {
		t.Fatal(err)
	}
	durableBefore := processCrashSQLHistory(t, ctx, api.db, sessionID)
	journalBefore := processCrashJournalFromDB(t, ctx, api.db, run.RunID)
	queueBefore := processCrashQueue(t, ctx, api.db, run.RunID)
	if err := first.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	firstWaitErr := <-firstDone
	hardKillAbnormal := firstWaitErr != nil && !first.ProcessState.Success() && first.ProcessState.ExitCode() != 0
	// Expire the dead owner's two leases without sleeping for the production TTL.
	if _, err := api.db.ExecContext(ctx, `UPDATE run_queue SET lease_expires_at=1 WHERE run_id=$1`, run.RunID); err != nil {
		t.Fatal(err)
	}
	if _, err := api.db.ExecContext(ctx, `UPDATE session_leases SET expires_at=1 WHERE session_id=$1`, sessionID); err != nil {
		t.Fatal(err)
	}
	requeued, failed, err := api.queue.RecoverExpiredRunClaims(ctx, time.Now().UTC())
	if err != nil {
		t.Fatalf("recover claim: %v", err)
	}
	second, secondDone := start("recover")
	var after processCrashEvidence
	select {
	case after = <-checkpoints:
	case err := <-secondDone:
		t.Fatalf("replacement exited before evidence checkpoint: %v", err)
	case <-ctx.Done():
		t.Fatal("replacement timed out")
	}
	secondWaitErr := <-secondDone
	state, err := api.queue.GetRun(ctx, run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	var effectsAfter, effectVersionAfter, effectVersionAfterMax int64
	if err := api.db.QueryRowContext(ctx, `
		SELECT COUNT(*), COALESCE(MIN(durable_version), -1), COALESCE(MAX(durable_version), -1)
		FROM crash_effects`).Scan(&effectsAfter, &effectVersionAfter, &effectVersionAfterMax); err != nil {
		t.Fatal(err)
	}
	if effectVersionAfter != effectVersionAfterMax {
		t.Fatalf("effects observed inconsistent durable versions after recovery: min=%d max=%d", effectVersionAfter, effectVersionAfterMax)
	}
	journalAfter := processCrashJournalFromDB(t, ctx, api.db, run.RunID)
	queueAfter := processCrashQueue(t, ctx, api.db, run.RunID)
	durableAfter := processCrashSQLHistory(t, ctx, api.db, sessionID)
	audit := processCrashAuditEvidence{
		FaultPoint: point, LiveModel: live, SessionID: sessionID, RunID: run.RunID,
		BeforeKill: before, Replacement: after, HardKillExitCode: first.ProcessState.ExitCode(), HardKillAbnormal: hardKillAbnormal,
		DurableVersion: versionBefore, DurablePrefix: processCrashSummarizeEvents(durableBefore),
		EffectsBefore: effectsBefore, EffectsAfter: effectsAfter, EffectVersion: effectVersionBefore,
		JournalBefore: journalBefore, JournalAfter: journalAfter,
		QueueBefore: queueBefore, QueueAfter: queueAfter, RecoveredClaims: requeued, RecoveryFailures: failed,
		RunStatus: state.Status, RunErrorCode: state.ErrorCode, FinalSQLHistory: processCrashSummarizeEvents(durableAfter),
	}
	events := serialAuditHistory(t, api, sessionID)
	t.Logf("hard crash point=%s live=%t durable_version=%d effects=%d->%d model_calls=%d+%d journal_rows=%d status=%s code=%s",
		point, live, versionBefore, effectsBefore, effectsAfter, before.ModelCalls, after.ModelCalls, journalAfter.Rows, state.Status, state.ErrorCode)
	if before.PID != first.Process.Pid || before.Phase != "crash" || !before.Blocked {
		t.Fatalf("checkpoint did not identify the blocked owned child: checkpoint=%+v process_pid=%d", before, first.Process.Pid)
	}
	if !hardKillAbnormal {
		t.Fatalf("owned child was not terminated abnormally: wait_err=%v exit_code=%d", firstWaitErr, first.ProcessState.ExitCode())
	}
	if secondWaitErr != nil {
		t.Fatalf("replacement process failed: %v", secondWaitErr)
	}
	if after.PID != second.Process.Pid || after.PID == before.PID || after.Phase != "recover" || after.Blocked {
		t.Fatalf("replacement was not a distinct completed process: before=%+v after=%+v process_pid=%d", before, after, second.Process.Pid)
	}
	if requeued != 1 || failed != 0 {
		t.Fatalf("claim recovery outcome: requeued=%d failed=%d", requeued, failed)
	}
	if effectsBefore != 1 || effectsAfter != 1 || before.ModelCalls != 1 || after.ModelCalls != 0 || journalAfter.Rows != 1 {
		t.Fatalf("crash replay duplicated work: effects=%d->%d calls=%d+%d journal_rows=%d", effectsBefore, effectsAfter, before.ModelCalls, after.ModelCalls, journalAfter.Rows)
	}
	if effectVersionBefore != versionBefore || effectVersionAfter != effectVersionBefore {
		t.Fatalf("business effect did not observe the complete durable tool-call prefix first: effect_version=%d->%d session_version=%d", effectVersionBefore, effectVersionAfter, versionBefore)
	}
	if state.Status != string(core.RunFailed) || state.ErrorCode != core.CodeRunInterrupted || versionBefore == 0 {
		t.Fatalf("interrupted run lost durable identity: status=%s code=%s version=%d", state.Status, state.ErrorCode, versionBefore)
	}
	assertProcessCrashJournal(t, point, journalBefore, journalAfter)
	assertProcessCrashQueue(t, queueBefore, queueAfter)
	if !reflect.DeepEqual(events, durableAfter) {
		t.Fatal("HTTP history differs from the raw durable SQL event chunks")
	}
	audit.HTTPMatchesSQL = true
	callID := assertProcessCrashDurableHistory(t, run.RunID, versionBefore, durableBefore, events)
	if callID != journalBefore.CallID {
		t.Fatalf("durable tool call and journal identity differ: history=%s journal=%s", callID, journalBefore.CallID)
	}
	assertProcessCrashTraces(t, point, sessionID, run.RunID, callID, before, after)
	processCrashRequireSafeAudit(t, audit)
	// Publish only evidence that survived every semantic and trace assertion.
	serialWriteAudit(t, run.RunID+"_process_crash", audit)
}

func processCrashSQLHistory(t *testing.T, ctx context.Context, db *sql.DB, sessionID string) []core.SessionEvent {
	t.Helper()
	rows, err := db.QueryContext(ctx, `SELECT start_seq, payload FROM event_chunks WHERE session_id=$1 ORDER BY start_seq`, sessionID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	events := []core.SessionEvent{}
	for rows.Next() {
		var start int64
		var payload string
		if err := rows.Scan(&start, &payload); err != nil {
			t.Fatal(err)
		}
		if start != int64(len(events)) {
			t.Fatalf("event chunk starts at seq=%d after %d decoded events", start, len(events))
		}
		chunkEvents := 0
		for _, line := range strings.Split(payload, "\n") {
			if strings.TrimSpace(line) == "" {
				continue
			}
			var event core.SessionEvent
			if err := json.Unmarshal([]byte(line), &event); err != nil {
				t.Fatal(err)
			}
			if event.Seq != start+int64(chunkEvents) {
				t.Fatalf("event chunk start=%d contains out-of-order seq=%d", start, event.Seq)
			}
			events = append(events, event)
			chunkEvents++
		}
		if chunkEvents == 0 {
			t.Fatalf("event chunk at seq=%d is empty", start)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return events
}

func processCrashJournalFromDB(t *testing.T, ctx context.Context, db *sql.DB, runID string) processCrashJournalEvidence {
	t.Helper()
	var evidence processCrashJournalEvidence
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM tool_invocations WHERE run_id=$1`, runID).Scan(&evidence.Rows); err != nil {
		t.Fatal(err)
	}
	if evidence.Rows == 1 {
		var result string
		if err := db.QueryRowContext(ctx, `SELECT call_id, state, result_json FROM tool_invocations WHERE run_id=$1`, runID).Scan(&evidence.CallID, &evidence.State, &result); err != nil {
			t.Fatal(err)
		}
		evidence.HasResult = result != ""
	}
	return evidence
}

func processCrashQueue(t *testing.T, ctx context.Context, db *sql.DB, runID string) processCrashQueueEvidence {
	t.Helper()
	var evidence processCrashQueueEvidence
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_queue WHERE run_id=$1`, runID).Scan(&evidence.Rows); err != nil {
		t.Fatal(err)
	}
	if evidence.Rows == 1 {
		if err := db.QueryRowContext(ctx, `SELECT worker_id, attempt, generation FROM run_queue WHERE run_id=$1`, runID).Scan(&evidence.WorkerID, &evidence.Attempt, &evidence.Generation); err != nil {
			t.Fatal(err)
		}
	}
	return evidence
}

func processCrashSummarizeEvents(events []core.SessionEvent) []processCrashEventEvidence {
	summary := make([]processCrashEventEvidence, 0, len(events))
	for _, event := range events {
		item := processCrashEventEvidence{Seq: event.Seq, Type: event.Type}
		switch event.Type {
		case core.EvToolCall:
			var data core.ToolCallData
			if json.Unmarshal(event.Data, &data) == nil {
				item.CallID = data.CallID
			}
		case core.EvToolResult:
			var data core.ToolResultData
			if json.Unmarshal(event.Data, &data) == nil {
				item.CallID = data.CallID
				item.Code, _ = data.Metadata["code"].(string)
			}
		case core.EvRunError:
			var data core.RuntimeErrorData
			if json.Unmarshal(event.Data, &data) == nil {
				item.Code = data.Code
			}
		case core.EvRunEnd:
			var data core.RunEndData
			if json.Unmarshal(event.Data, &data) == nil {
				item.Status = data.Status
			}
		}
		summary = append(summary, item)
	}
	return summary
}

func processCrashRequireSafeAudit(t *testing.T, evidence processCrashAuditEvidence) {
	t.Helper()
	encoded, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal(err)
	}
	text := string(encoded)
	if strings.Contains(text, "http://") || strings.Contains(text, "https://") ||
		strings.Contains(text, `"message":`) || strings.Contains(text, `"content":`) || strings.Contains(text, `"result_json":`) {
		t.Fatal("process crash evidence contains a URL or raw message/result field")
	}
	for _, name := range []string{"HARNESS_LLM_API_KEY", "HARNESS_LLM_BASE_URL"} {
		if value := os.Getenv(name); value != "" && strings.Contains(text, value) {
			t.Fatalf("process crash evidence contains %s", name)
		}
	}
}

func assertProcessCrashJournal(t *testing.T, point string, before, after processCrashJournalEvidence) {
	t.Helper()
	wantState, wantResult := string(core.ToolInvocationStarted), false
	if point == "journal_completed" {
		wantState, wantResult = string(core.ToolInvocationCompleted), true
	}
	if before.Rows != 1 || before.CallID == "" || before.State != wantState || before.HasResult != wantResult {
		t.Fatalf("journal checkpoint was not committed at %s: %+v", point, before)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("recovery mutated or duplicated the journal: before=%+v after=%+v", before, after)
	}
}

func assertProcessCrashQueue(t *testing.T, before, after processCrashQueueEvidence) {
	t.Helper()
	if before.Rows != 1 || before.WorkerID != "process-crash" || before.Attempt != 1 || before.Generation != 1 {
		t.Fatalf("first worker did not own the expected queue generation: %+v", before)
	}
	if after.Rows != 0 {
		t.Fatalf("replacement did not settle its queue claim: %+v", after)
	}
}

func assertProcessCrashDurableHistory(t *testing.T, runID string, version int64, before, after []core.SessionEvent) string {
	t.Helper()
	wantPrefix := []core.SessionEventType{
		core.EvRunStart, core.EvUserMessage, core.EvStepStart,
		core.EvAssistantMessage, core.EvRunUsage, core.EvToolCall,
	}
	if version != int64(len(wantPrefix)) || len(before) != len(wantPrefix) {
		t.Fatalf("pre-effect durable prefix version=%d events=%d, want %d", version, len(before), len(wantPrefix))
	}
	for i, event := range before {
		if event.Seq != int64(i) || event.RunID != runID || event.Type != wantPrefix[i] {
			t.Fatalf("invalid durable prefix event %d: %+v", i, event)
		}
	}
	var assistant core.AssistantMessageData
	if err := json.Unmarshal(before[3].Data, &assistant); err != nil {
		t.Fatal(err)
	}
	if len(assistant.ToolCalls) != 1 || assistant.ToolCalls[0].Name != "crash.effect" {
		t.Fatalf("durable assistant call differs from fixture: %+v", assistant.ToolCalls)
	}
	var usage core.RunUsageData
	if err := json.Unmarshal(before[4].Data, &usage); err != nil {
		t.Fatal(err)
	}
	if usage.InvocationID != fmt.Sprintf("model:%d", before[2].Seq) {
		t.Fatalf("durable model usage does not bind the active step: usage=%+v step_seq=%d", usage, before[2].Seq)
	}
	var call core.ToolCallData
	if err := json.Unmarshal(before[5].Data, &call); err != nil {
		t.Fatal(err)
	}
	if call.CallID == "" || call.CallID != assistant.ToolCalls[0].ID || call.Name != "crash.effect" || fmt.Sprint(call.Args["n"]) != "7" {
		t.Fatalf("durable tool call identity differs from model output: %+v", call)
	}
	if len(after) != len(before)+4 || !reflect.DeepEqual(after[:len(before)], before) {
		t.Fatalf("recovery did not preserve the exact durable prefix: before=%d after=%d", len(before), len(after))
	}
	wantSuffix := []core.SessionEventType{core.EvToolResult, core.EvStepEnd, core.EvRunError, core.EvRunEnd}
	for i, event := range after[len(before):] {
		if event.Seq != int64(len(before)+i) || event.RunID != runID || event.Type != wantSuffix[i] {
			t.Fatalf("invalid repaired suffix event %d: %+v", i, event)
		}
	}
	var result core.ToolResultData
	if err := json.Unmarshal(after[len(before)].Data, &result); err != nil {
		t.Fatal(err)
	}
	if result.CallID != call.CallID || result.OK || result.Metadata["code"] != core.CodeToolOutcomeUnknown || result.Metadata["repaired"] != true {
		t.Fatalf("interrupted tool result is not a repaired unknown outcome: %+v", result)
	}
	var runError core.RuntimeErrorData
	if err := json.Unmarshal(after[len(before)+2].Data, &runError); err != nil {
		t.Fatal(err)
	}
	var runEnd core.RunEndData
	if err := json.Unmarshal(after[len(before)+3].Data, &runEnd); err != nil {
		t.Fatal(err)
	}
	if runError.Code != core.CodeRunInterrupted || runEnd.Status != core.RunFailed {
		t.Fatalf("repaired run terminal differs: error=%+v end=%+v", runError, runEnd)
	}
	return call.CallID
}

func assertProcessCrashTraces(t *testing.T, point, sessionID, runID, callID string, before, after processCrashEvidence) {
	t.Helper()
	if before.ActiveCallID != callID {
		t.Fatalf("crash checkpoint call identity differs: active=%s durable=%s", before.ActiveCallID, callID)
	}
	processCrashRequireTraceID(t, "active crash trace", before.ActiveTrace)
	processCrashRequireSpanID(t, "active crash span", before.ActiveSpan, false)
	var modelSpans, toolSpans []serialSpanEvidence
	for _, span := range before.Spans {
		processCrashRequireTraceID(t, span.Name+" trace", span.TraceID)
		processCrashRequireSpanID(t, span.Name+" span", span.SpanID, false)
		processCrashRequireSpanID(t, span.Name+" parent", span.ParentID, span.Name == core.SpanQueueClaim)
		if span.SpanID == before.ActiveSpan {
			t.Fatal("active checkpoint span was incorrectly exported as ended")
		}
		switch span.Name {
		case core.SpanModelCall:
			modelSpans = append(modelSpans, span)
		case core.SpanToolCall:
			toolSpans = append(toolSpans, span)
		}
	}
	if len(modelSpans) != 1 || modelSpans[0].TraceID != before.ActiveTrace ||
		modelSpans[0].Attributes["run.id"] != runID || modelSpans[0].Attributes["session.id"] != sessionID ||
		modelSpans[0].Attributes["run.step"] != "0" || modelSpans[0].Attributes["model.outcome"] != "ok" {
		t.Fatalf("ended model span is detached from active run trace: active=%s/%s spans=%+v", before.ActiveTrace, before.ActiveSpan, modelSpans)
	}
	if point == "effect_committed" {
		if len(toolSpans) != 0 || modelSpans[0].ParentID == before.ActiveSpan {
			t.Fatalf("effect checkpoint is not inside the still-active tool span: model=%+v tools=%+v", modelSpans, toolSpans)
		}
	} else if len(toolSpans) != 1 || toolSpans[0].TraceID != before.ActiveTrace || toolSpans[0].ParentID != before.ActiveSpan || modelSpans[0].ParentID != before.ActiveSpan ||
		toolSpans[0].Attributes["run.id"] != runID || toolSpans[0].Attributes["session.id"] != sessionID ||
		toolSpans[0].Attributes["call.id"] != callID || toolSpans[0].Attributes["capability.id"] != "crash.effect" ||
		toolSpans[0].Attributes["tool.outcome"] != "ok" || toolSpans[0].Attributes["tool.idempotent"] != "false" {
		t.Fatalf("journal checkpoint is not inside the run span after the tool span ended: active=%s/%s model=%+v tools=%+v", before.ActiveTrace, before.ActiveSpan, modelSpans, toolSpans)
	}
	assertProcessCrashQueueClaimSpan(t, before.Spans, "process-crash", sessionID, runID)
	if after.ActiveTrace != "" || after.ActiveSpan != "" || after.ActiveCallID != "" {
		t.Fatalf("replacement reported an unexpected active operation after settling the run: %+v", after)
	}
	for _, span := range after.Spans {
		processCrashRequireTraceID(t, span.Name+" trace", span.TraceID)
		processCrashRequireSpanID(t, span.Name+" span", span.SpanID, false)
		processCrashRequireSpanID(t, span.Name+" parent", span.ParentID, span.Name == core.SpanQueueClaim)
		if span.Name == core.SpanModelCall || span.Name == core.SpanToolCall {
			t.Fatalf("replacement emitted a provider span: %+v", span)
		}
	}
	assertProcessCrashQueueClaimSpan(t, after.Spans, "process-recover", sessionID, runID)
}

func processCrashRequireTraceID(t *testing.T, label, value string) {
	t.Helper()
	id, err := trace.TraceIDFromHex(value)
	if err != nil || !id.IsValid() {
		t.Fatalf("%s is invalid: %q", label, value)
	}
}

func processCrashRequireSpanID(t *testing.T, label, value string, allowZero bool) {
	t.Helper()
	if allowZero && value == "0000000000000000" {
		return
	}
	id, err := trace.SpanIDFromHex(value)
	if err != nil || !id.IsValid() {
		t.Fatalf("%s is invalid: %q", label, value)
	}
}

func assertProcessCrashQueueClaimSpan(t *testing.T, spans []serialSpanEvidence, workerID, sessionID, runID string) {
	t.Helper()
	matches := 0
	for _, span := range spans {
		if span.Name != core.SpanQueueClaim {
			continue
		}
		if span.Attributes["worker.id"] == workerID && span.Attributes["claim.outcome"] == "claimed" &&
			span.Attributes["session.id"] == sessionID && span.Attributes["run.id"] == runID &&
			span.ParentID == "0000000000000000" {
			matches++
		}
	}
	if matches != 1 {
		t.Fatalf("queue claim trace matches=%d for worker=%s", matches, workerID)
	}
}

// This is a real OS process entry point, never a normal in-process test fixture.
func TestRunWorkerCrashHelper(t *testing.T) {
	if os.Getenv("HARNESS_NATIVE_COMPLETED_TOOL_CRASH_HELPER") == "1" {
		runNativeQueuedCompletedToolCrashHelper(t)
		return
	}
	if os.Getenv("HARNESS_CRASH_HELPER") != "1" {
		t.Skip("owned process helper only")
	}
	config, err := pgx.ParseConfig(os.Getenv("HARNESS_TEST_PG_DSN"))
	if err != nil {
		t.Fatal("invalid helper PostgreSQL configuration")
	}
	schema := os.Getenv("HARNESS_CRASH_SCHEMA")
	if !strings.HasPrefix(schema, "harness_acceptance_") || strings.ContainsAny(schema, " \";'\\") {
		t.Fatal("invalid owned schema")
	}
	config.RuntimeParams["search_path"] = schema
	db := stdlib.OpenDB(*config)
	phase, point := os.Getenv("HARNESS_CRASH_PHASE"), os.Getenv("HARNESS_CRASH_POINT")
	var inner core.LlmAdapter = &processCrashModel{}
	if os.Getenv("HARNESS_CRASH_LIVE") == "true" {
		inner, err = openai.NewOpenAIAdapterFromEnv()
		if err != nil {
			t.Fatal("live model configuration unavailable")
		}
	}
	timedModel := &processCrashTimedModel{inner: inner}
	model := &serialAcceptanceModel{inner: timedModel, test: t}
	api := newSerialAuditedAPI(t, db, model, &atomic.Int32{})
	report := func(ctx context.Context, block bool, callID string) {
		active := trace.SpanContextFromContext(ctx)
		evidence := processCrashEvidence{PID: os.Getpid(), Phase: phase, Blocked: block, ActiveCallID: callID, ModelCalls: model.calls,
			InputTokens: model.input, OutputTokens: model.output, ModelMS: append([]int64(nil), timedModel.durations...)}
		if active.IsValid() {
			evidence.ActiveTrace, evidence.ActiveSpan = active.TraceID().String(), active.SpanID().String()
		}
		for _, span := range api.exporter.GetSpans() {
			attrs := map[string]string{}
			for _, attr := range span.Attributes {
				key := string(attr.Key)
				switch key {
				case "run.id", "session.id", "worker.id", "claim.outcome", "call.id", "capability.id",
					"run.step", "model.outcome", "tool.outcome", "tool.idempotent":
					attrs[key] = attr.Value.AsString()
				}
			}
			evidence.Spans = append(evidence.Spans, serialSpanEvidence{span.Name, span.SpanContext.TraceID().String(), span.SpanContext.SpanID().String(), span.Parent.SpanID().String(), span.EndTime.Sub(span.StartTime).Milliseconds(), span.Status.Code.String(), attrs})
		}
		if err := acceptanceRequest(&http.Client{Timeout: 10 * time.Second}, http.MethodPost, os.Getenv("HARNESS_CRASH_COLLECTOR"), evidence, nil); err != nil {
			t.Fatal("could not export process checkpoint")
		}
		if block {
			<-ctx.Done() // Parent kills us here; no provider return or cleanup runs.
		}
	}
	configureProcessCrash(t, api, func(ctx context.Context, callID string) {
		if phase == "crash" && point == "effect_committed" {
			report(ctx, true, callID)
		}
	})
	if phase == "crash" && point == "journal_completed" {
		api.server.runtime.ToolJournal = &processCrashJournal{ToolInvocationJournal: api.server.runtime.ToolJournal, checkpoint: report}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if claimed, err := api.server.RunWorkerOnce(ctx, "process-"+phase); err != nil || !claimed {
		t.Fatalf("helper worker claimed=%t error=%v", claimed, err)
	}
	report(ctx, false, "")
}

const (
	nativeRecoveryCrashSessionID = "session-native-recovery-crash"
	nativeRecoveryCrashRunID     = "run-native-recovery-crash"
	nativeRecoveryCrashCallID    = "call-native-recovery-crash"
)

func TestNativeQueuedCompletedToolCrashHelper(t *testing.T) {
	if os.Getenv("HARNESS_NATIVE_COMPLETED_TOOL_CRASH_HELPER") != "1" {
		t.Skip("owned native recovery process helper only")
	}
	runNativeQueuedCompletedToolCrashHelper(t)
}

func runNativeQueuedCompletedToolCrashHelper(t *testing.T) {
	t.Helper()
	config, err := pgx.ParseConfig(os.Getenv("HARNESS_TEST_PG_DSN"))
	if err != nil {
		t.Fatal("invalid native recovery PostgreSQL configuration")
	}
	schema := os.Getenv("HARNESS_CRASH_SCHEMA")
	if !strings.HasPrefix(schema, "harness_acceptance_") || strings.ContainsAny(schema, " \";'\\") {
		t.Fatal("invalid owned schema")
	}
	config.RuntimeParams["search_path"] = schema
	db := stdlib.OpenDB(*config)
	defer db.Close()
	phase := os.Getenv("HARNESS_NATIVE_RECOVERY_PHASE")
	if phase != "crash" && phase != "recover" && phase != "recover_unknown" && phase != "delivery_crash" && phase != "postprovider_crash" {
		t.Fatalf("invalid native recovery phase %q", phase)
	}
	model := &nativeRecoveryCrashModel{db: db, blockAfterProvider: phase == "postprovider_crash"}
	api, accounts := newNativeRecoveryCrashServer(t, db, storage.SQLDialectPostgres, model)
	defer api.Shutdown(context.Background())
	if phase == "crash" {
		if err := accounts.CreateTenant(context.Background(), "acme", "Acme"); err != nil {
			t.Fatal(err)
		}
		if err := accounts.CreateAccount(context.Background(), storage.Account{AccountID: "alice", Email: "alice@example.test", Role: storage.RoleAccountUser, TenantID: "acme", Status: storage.AccountActive}, "native-password"); err != nil {
			t.Fatal(err)
		}
		account, err := accounts.GetAccount(context.Background(), "alice")
		if err != nil {
			t.Fatal(err)
		}
		principal, err := storage.PrincipalForAccount(account, nativeStrictTestRoot().Segments())
		if err != nil {
			t.Fatal(err)
		}
		scope, err := principal.Scope.Child(core.ScopeRef{Kind: core.ScopeSession, ID: nativeRecoveryCrashSessionID})
		if err != nil {
			t.Fatal(err)
		}
		session, err := core.NewSession(core.SessionOptions{ID: nativeRecoveryCrashSessionID, ProfileID: "native.recovery.crash", Principal: principal, Scope: scope})
		if err != nil {
			t.Fatal(err)
		}
		if err := api.sessions.Create(context.Background(), session); err != nil {
			t.Fatal(err)
		}
		if err := api.runQueue.EnqueueRun(context.Background(), storage.QueuedRun{RunRecord: storage.RunRecord{RunID: nativeRecoveryCrashRunID, SessionID: session.ID(), TenantID: principal.TenantID, SubjectID: principal.SubjectID}, Message: "run recovery crash fixture", MaxAttempts: 3}); err != nil {
			t.Fatal(err)
		}
		api.nativeQueuedRecoveryTestHooks = &nativeQueuedRecoveryTestHooks{
			afterToolJournalComplete: func() { select {} },
		}
	} else {
		loaded, err := api.sessions.Load(context.Background(), nativeRecoveryCrashSessionID)
		if err != nil {
			t.Fatal(err)
		}
		principal, err := api.resolveQueuedPrincipal(context.Background(), storage.QueuedRun{RunRecord: storage.RunRecord{TenantID: loaded.Principal().TenantID, SubjectID: loaded.Principal().SubjectID}})
		if err != nil || !reflect.DeepEqual(loaded.Principal(), principal) {
			t.Fatalf("loaded/current native principal equal=%t err=%v loaded=%#v current=%#v", reflect.DeepEqual(loaded.Principal(), principal), err, loaded.Principal(), principal)
		}
		if phase == "delivery_crash" || os.Getenv("HARNESS_NATIVE_RECOVERY_HARDKILL_AFTER_DELIVERY") == "1" {
			api.nativeQueuedRecoveryTestHooks = &nativeQueuedRecoveryTestHooks{
				afterCompletedToolRecovery: func() { select {} },
			}
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	claimed, runErr := api.RunWorkerOnce(ctx, "native-recovery-"+phase)
	if phase == "recover_unknown" && claimed && errors.Is(runErr, storage.ErrCompletedToolResultProofInvalid) {
		return
	}
	if runErr != nil || !claimed {
		t.Fatalf("native recovery helper claimed=%t error=%v", claimed, runErr)
	}
}

func newNativeRecoveryCrashServer(t *testing.T, db *sql.DB, dialect storage.SQLDialect, model core.LlmAdapter) (*Server, *storage.SQLAccountStore) {
	t.Helper()
	accounts, err := storage.NewSQLAccountStore(db, dialect)
	if err != nil {
		t.Fatal(err)
	}
	root := nativeStrictTestRoot()
	selection := core.ModelSelection{Provider: "native-recovery-crash", Model: "fixture"}
	name := "Native recovery crash"
	server, err := NewNativeStrictServer(context.Background(), NativeStrictServerConfig{DB: db, Dialect: dialect, Bootstrap: NativeStrictBootstrap{
		Revision: "native-recovery-crash-v1", Root: root.Segments(), DefaultProfileID: "native.recovery.crash",
		Profiles:     []core.AgentProfileLayer{{Scope: root, ProfileID: "native.recovery.crash", Name: &name, Model: &selection, AddCapabilities: []string{"native.recovery.effect"}}},
		Capabilities: []NativeStrictCapability{{Scope: root, Capability: nativeRecoveryCrashTool{db: db}}},
		Model:        NativeStrictModel{Selection: selection, Adapter: model},
	}, MaxWriteDelay: -1})
	if err != nil {
		t.Fatal(err)
	}
	return server, accounts
}

type nativeRecoveryCrashTool struct{ db *sql.DB }

func (nativeRecoveryCrashTool) Manifest() core.CapabilityManifest {
	manifest := (checkpointTestTool{}).Manifest()
	manifest.ID, manifest.Name = "native.recovery.effect", "Native recovery effect"
	return manifest
}

func (tool nativeRecoveryCrashTool) Execute(ctx context.Context, request core.CapabilityRequest) (core.CapabilityResult, error) {
	if err := core.RequireAcceptedInvocation(request); err != nil {
		return core.CapabilityResult{}, err
	}
	if _, err := tool.db.ExecContext(ctx, `INSERT INTO native_recovery_crash_effects (call_id) VALUES ($1)`, request.CallID); err != nil {
		return core.CapabilityResult{}, err
	}
	return core.CapabilityResult{OK: true, Content: `{"effect":"committed"}`}, nil
}

type nativeRecoveryCrashModel struct {
	db                 *sql.DB
	blockAfterProvider bool
}

func (*nativeRecoveryCrashModel) Provider() string { return "native-recovery-crash" }

func (model *nativeRecoveryCrashModel) Stream(ctx context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	continuation := false
	for _, message := range options.Messages {
		if message.Role == core.RoleTool {
			continuation = true
			break
		}
	}
	stage := "initial"
	if continuation {
		stage = "continuation"
	}
	if _, err := model.db.ExecContext(ctx, `INSERT INTO native_recovery_crash_model_calls (stage, calls) VALUES ($1, 1) ON CONFLICT (stage) DO UPDATE SET calls=native_recovery_crash_model_calls.calls+1`, stage); err != nil {
		return err
	}
	if continuation && model.blockAfterProvider {
		select {}
	}
	if continuation {
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: "recovered"})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
		return nil
	}
	call := core.ToolCall{ID: nativeRecoveryCrashCallID, Name: "native.recovery.effect", Args: map[string]any{"value": "7"}}
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &call, ToolCalls: []core.ToolCall{call}})
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
	return nil
}

func configureProcessCrash(t *testing.T, api *serialAuditedAPI, checkpoint func(context.Context, string)) {
	t.Helper()
	if os.Getenv("HARNESS_LLM_MODEL") == "" {
		t.Setenv("HARNESS_LLM_MODEL", "crash-fixture")
	}
	serialRegister(t, api, processCrashTool{db: api.db, checkpoint: checkpoint})
	serialProfile(t, api, "crash.agent", "Call crash.effect exactly once with n=7. Never simulate its result. After the tool result, answer briefly without another tool call.", "crash.effect")
}

type processCrashTool struct {
	db         *sql.DB
	checkpoint func(context.Context, string)
}

func (processCrashTool) Manifest() core.CapabilityManifest {
	m := (serialFunctionTool{id: "crash.effect"}).Manifest()
	m.Idempotent = false
	return m
}

func (tool processCrashTool) Execute(ctx context.Context, request core.CapabilityRequest) (core.CapabilityResult, error) {
	if err := core.RequireAcceptedInvocation(request); err != nil {
		return core.CapabilityResult{}, err
	}
	result, err := tool.db.ExecContext(ctx, `
		INSERT INTO crash_effects (call_id, durable_version)
		SELECT $1, version FROM sessions WHERE id=$2`, request.CallID, request.Context.Invocation.SessionID)
	if err != nil {
		return core.CapabilityResult{}, err
	}
	if rows, err := result.RowsAffected(); err != nil || rows != 1 {
		return core.CapabilityResult{}, fmt.Errorf("record crash effect durable version: rows=%d error=%v", rows, err)
	}
	if tool.checkpoint != nil {
		tool.checkpoint(ctx, request.CallID)
	}
	return core.CapabilityResult{OK: true, Content: `{"effect":"committed","n":7}`}, nil
}

type processCrashJournal struct {
	core.ToolInvocationJournal
	checkpoint func(context.Context, bool, string)
}

func (j *processCrashJournal) CompleteToolInvocation(ctx context.Context, invocation core.ToolInvocation, result core.CapabilityResult) (core.ToolInvocationRecord, error) {
	record, err := j.ToolInvocationJournal.CompleteToolInvocation(ctx, invocation, result)
	if err == nil {
		j.checkpoint(ctx, true, invocation.CallID)
	}
	return record, err
}

type processCrashModel struct{}

func (*processCrashModel) Provider() string { return "openai-compatible" }
func (*processCrashModel) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	for _, message := range options.Messages {
		if message.Role == "tool" {
			emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: "Effect committed."})
			emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
			return nil
		}
	}
	// Deliberately fresh IDs: only a durable run identity prevents regeneration.
	id, err := core.NewID("call_")
	if err != nil {
		return err
	}
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &core.ToolCall{ID: id, Name: "crash.effect", Args: map[string]any{"n": 7}}})
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
	return nil
}

type processCrashTimedModel struct {
	inner     core.LlmAdapter
	durations []int64
}

func (model *processCrashTimedModel) Provider() string { return model.inner.Provider() }

func (model *processCrashTimedModel) Stream(ctx context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	started := time.Now()
	err := model.inner.Stream(ctx, options, emit)
	model.durations = append(model.durations, time.Since(started).Milliseconds())
	return err
}
