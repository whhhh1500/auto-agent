package server

import (
	"context"
	"database/sql"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/whhhh1500/auto-agent/internal/testdb"
	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/storage"
)

func TestNativeQueuedRetentionWorkerTickerPrunesSafePairsAndPreservesRecoveryProof(t *testing.T) {
	ctx := context.Background()
	fixture, blocked := newNativeQueuedRecoveryBlockedFixture(t)
	successor := newNativeStrictWitnessRecoveryServer(t, fixture.db, fixture.toolCalls, fixture.modelCalls)

	requeueNativeQueuedRecoveryClaim(t, fixture)
	claimed, err := successor.RunWorkerOnce(ctx, "worker-native-retention-safe")
	if err != nil || !claimed {
		t.Fatalf("recovery worker claimed=%t err=%v", claimed, err)
	}
	assertNativeQueuedRecoveryCompleted(t, fixture, 1)
	close(blocked.release)
	if err := <-blocked.done; err == nil {
		t.Fatal("stale pre-recovery worker unexpectedly completed")
	}

	attempts, outcomes := nativeQueuedRetentionPairCounts(t, fixture.db, fixture.session.ID())
	if attempts == 0 || attempts != outcomes {
		t.Fatalf("before ticker attempts=%d outcomes=%d", attempts, outcomes)
	}
	assertNativeQueuedRetentionRecoveryProofCounts(t, fixture.db, fixture.session.ID(), fixture.runID, 1, 1, 1)

	logs := &nativeQueuedRetentionLogHandler{}
	successor.logger = slog.New(logs)
	tick := make(chan struct{}, 1)
	successor.nativeQueuedRetentionEvery = 2 * time.Millisecond
	successor.nativeQueuedRetentionWindow = time.Nanosecond
	successor.nativeQueuedRetentionTestHooks = &nativeQueuedRetentionTestHooks{
		beforePrune: func(context.Context) {
			select {
			case tick <- struct{}{}:
			default:
			}
		},
	}
	serviceCtx, stopService := context.WithCancel(context.Background())
	defer stopService()
	if err := successor.StartRunWorkers(serviceCtx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = successor.Shutdown(context.Background()) }()
	select {
	case <-tick:
	case <-time.After(time.Second):
		t.Fatal("native retention ticker did not fire")
	}
	waitNativeQueuedRetention(t, func() bool {
		attempts, outcomes := nativeQueuedRetentionPairCounts(t, fixture.db, fixture.session.ID())
		return attempts == 0 && outcomes == 0
	}, func() string {
		attempts, outcomes := nativeQueuedRetentionPairCounts(t, fixture.db, fixture.session.ID())
		return logs.String() + " attempts=" + strconv.Itoa(attempts) + " outcomes=" + strconv.Itoa(outcomes)
	})
	assertNativeQueuedRetentionRecoveryProofCounts(t, fixture.db, fixture.session.ID(), fixture.runID, 1, 1, 1)
}

func TestNativeQueuedRetentionWorkerTickerRetainsUnknownAttempt(t *testing.T) {
	fixture := newNativeStrictWitnessFixture(t, false)
	model := &nativeModelReportedUsageError{}
	fixture.api.runtime.Models = core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) {
		return model, nil
	})
	claimed, err := fixture.api.RunWorkerOnce(context.Background(), "worker-native-retention-unknown")
	if err != nil || !claimed {
		t.Fatalf("provider-error worker claimed=%t err=%v", claimed, err)
	}
	attempts, outcomes := nativeQueuedRetentionPairCounts(t, fixture.db, fixture.session.ID())
	if model.calls != 1 || attempts != 1 || outcomes != 0 {
		t.Fatalf("before ticker calls=%d attempts=%d outcomes=%d", model.calls, attempts, outcomes)
	}

	ticks := make(chan struct{}, 2)
	fixture.api.nativeQueuedRetentionEvery = 2 * time.Millisecond
	fixture.api.nativeQueuedRetentionWindow = time.Nanosecond
	fixture.api.nativeQueuedRetentionTestHooks = &nativeQueuedRetentionTestHooks{
		beforePrune: func(context.Context) {
			select {
			case ticks <- struct{}{}:
			default:
			}
		},
	}
	serviceCtx, stopService := context.WithCancel(context.Background())
	defer stopService()
	if err := fixture.api.StartRunWorkers(serviceCtx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = fixture.api.Shutdown(context.Background()) }()
	for count := 0; count < 2; count++ {
		select {
		case <-ticks:
		case <-time.After(time.Second):
			t.Fatalf("native retention tick %d did not fire", count+1)
		}
	}
	// The second callback precedes the second real SQL call; allow that short
	// call to finish before checking the durable replay fence.
	time.Sleep(10 * time.Millisecond)
	attempts, outcomes = nativeQueuedRetentionPairCounts(t, fixture.db, fixture.session.ID())
	if model.calls != 1 || attempts != 1 || outcomes != 0 {
		t.Fatalf("unknown attempt was pruned or replayed: calls=%d attempts=%d outcomes=%d", model.calls, attempts, outcomes)
	}
}

// This exercises the real PostgreSQL worker-owned ticker, rather than only
// calling the storage pruner. The recovery-sidecar preservation case above is
// SQLite because it uses the controlled in-process A window; PostgreSQL's
// storage recovery suite covers that same A/B proof under row locks.
func TestPostgresNativeQueuedRetentionWorkerTickerPrunesSafePair(t *testing.T) {
	ctx := context.Background()
	db := testdb.Postgres(t)()
	api, accounts := newNativeRecoveryCrashServer(t, db, storage.SQLDialectPostgres, nativeQueuedRetentionPostgresModel{})
	t.Cleanup(func() { _ = api.Shutdown(context.Background()) })
	if err := accounts.CreateTenant(ctx, "retention-pg", "Retention PostgreSQL"); err != nil {
		t.Fatal(err)
	}
	if err := accounts.CreateAccount(ctx, storage.Account{
		AccountID: "retention-pg-user", Email: "retention-pg@example.test", Role: storage.RoleAccountUser,
		TenantID: "retention-pg", Status: storage.AccountActive,
	}, "retention-pg-password"); err != nil {
		t.Fatal(err)
	}
	account, err := accounts.GetAccount(ctx, "retention-pg-user")
	if err != nil {
		t.Fatal(err)
	}
	principal, err := storage.PrincipalForAccount(account, nativeStrictTestRoot().Segments())
	if err != nil {
		t.Fatal(err)
	}
	const sessionID = "session-native-retention-pg"
	const runID = "run-native-retention-pg"
	scope, err := principal.Scope.Child(core.ScopeRef{Kind: core.ScopeSession, ID: sessionID})
	if err != nil {
		t.Fatal(err)
	}
	session, err := core.NewSession(core.SessionOptions{ID: sessionID, ProfileID: "native.recovery.crash", Principal: principal, Scope: scope})
	if err != nil {
		t.Fatal(err)
	}
	if err := api.sessions.Create(ctx, session); err != nil {
		t.Fatal(err)
	}
	if err := api.runQueue.EnqueueRun(ctx, storage.QueuedRun{RunRecord: storage.RunRecord{
		RunID: runID, SessionID: sessionID, TenantID: principal.TenantID, SubjectID: principal.SubjectID,
	}, Message: "run native retention fixture", MaxAttempts: 3}); err != nil {
		t.Fatal(err)
	}
	if claimed, err := api.RunWorkerOnce(ctx, "worker-native-retention-pg"); err != nil || !claimed {
		t.Fatalf("worker claimed=%t err=%v", claimed, err)
	}
	if run, err := api.runQueue.GetRun(ctx, runID); err != nil || run.Status != string(core.RunCompleted) {
		t.Fatalf("terminal run=%+v err=%v", run, err)
	}
	attempts, outcomes := nativeQueuedRetentionPostgresPairCounts(t, db, sessionID)
	if attempts == 0 || attempts != outcomes {
		t.Fatalf("before postgres ticker attempts=%d outcomes=%d", attempts, outcomes)
	}
	// This normal completed run has a v44 witness; the A-recovery SQLite case
	// above supplies the completed v43/V44/journal trio.
	assertNativeQueuedRetentionPostgresRecoveryProofCounts(t, db, sessionID, runID, 0, 1, 0)

	tick := make(chan struct{}, 1)
	api.nativeQueuedRetentionEvery = 2 * time.Millisecond
	api.nativeQueuedRetentionWindow = time.Nanosecond
	api.nativeQueuedRetentionTestHooks = &nativeQueuedRetentionTestHooks{beforePrune: func(context.Context) {
		select {
		case tick <- struct{}{}:
		default:
		}
	}}
	serviceCtx, stopService := context.WithCancel(context.Background())
	defer stopService()
	if err := api.StartRunWorkers(serviceCtx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = api.Shutdown(context.Background()) }()
	select {
	case <-tick:
	case <-time.After(time.Second):
		t.Fatal("postgres native retention ticker did not fire")
	}
	waitNativeQueuedRetention(t, func() bool {
		attempts, outcomes := nativeQueuedRetentionPostgresPairCounts(t, db, sessionID)
		return attempts == 0 && outcomes == 0
	}, func() string { return "postgres safe pair remained" })
	assertNativeQueuedRetentionPostgresRecoveryProofCounts(t, db, sessionID, runID, 0, 1, 0)
}

type nativeQueuedRetentionPostgresModel struct{}

func (nativeQueuedRetentionPostgresModel) Provider() string { return "native-recovery-crash" }

func (nativeQueuedRetentionPostgresModel) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	for _, message := range options.Messages {
		if message.Role == core.RoleTool {
			emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: "retention safe"})
			emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
			return nil
		}
	}
	call := core.ToolCall{ID: "call-native-retention-pg", Name: "native.recovery.effect", Args: map[string]any{"value": "retention"}}
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &call, ToolCalls: []core.ToolCall{call}})
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
	return nil
}

func nativeQueuedRetentionPairCounts(t *testing.T, db *sql.DB, sessionID string) (attempts, outcomes int) {
	t.Helper()
	if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM native_queued_model_invocations WHERE session_id = ?`, sessionID).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM native_queued_model_invocation_outcomes WHERE session_id = ?`, sessionID).Scan(&outcomes); err != nil {
		t.Fatal(err)
	}
	return attempts, outcomes
}

func nativeQueuedRetentionPostgresPairCounts(t *testing.T, db *sql.DB, sessionID string) (attempts, outcomes int) {
	t.Helper()
	if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM native_queued_model_invocations WHERE session_id = $1`, sessionID).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM native_queued_model_invocation_outcomes WHERE session_id = $1`, sessionID).Scan(&outcomes); err != nil {
		t.Fatal(err)
	}
	return attempts, outcomes
}

func assertNativeQueuedRetentionRecoveryProofCounts(t *testing.T, db *sql.DB, sessionID, runID string, wantSidecars, wantWitnesses, wantJournal int) {
	t.Helper()
	var sidecars, witnesses, journal int
	if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM completed_tool_result_recovery_sidecars WHERE session_id = ? AND run_id = ?`, sessionID, runID).Scan(&sidecars); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM native_queued_tool_effect_witnesses WHERE session_id = ? AND run_id = ?`, sessionID, runID).Scan(&witnesses); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM tool_invocations WHERE session_id = ? AND run_id = ? AND state = ?`, sessionID, runID, string(core.ToolInvocationCompleted)).Scan(&journal); err != nil {
		t.Fatal(err)
	}
	if sidecars != wantSidecars || witnesses != wantWitnesses || journal != wantJournal {
		t.Fatalf("recovery proofs sidecars=%d witnesses=%d journal=%d; want %d/%d/%d", sidecars, witnesses, journal, wantSidecars, wantWitnesses, wantJournal)
	}
}

func assertNativeQueuedRetentionPostgresRecoveryProofCounts(t *testing.T, db *sql.DB, sessionID, runID string, wantSidecars, wantWitnesses, wantJournal int) {
	t.Helper()
	var sidecars, witnesses, journal int
	if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM completed_tool_result_recovery_sidecars WHERE session_id = $1 AND run_id = $2`, sessionID, runID).Scan(&sidecars); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM native_queued_tool_effect_witnesses WHERE session_id = $1 AND run_id = $2`, sessionID, runID).Scan(&witnesses); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM tool_invocations WHERE session_id = $1 AND run_id = $2 AND state = $3`, sessionID, runID, string(core.ToolInvocationCompleted)).Scan(&journal); err != nil {
		t.Fatal(err)
	}
	if sidecars != wantSidecars || witnesses != wantWitnesses || journal != wantJournal {
		t.Fatalf("postgres recovery proofs sidecars=%d witnesses=%d journal=%d; want %d/%d/%d", sidecars, witnesses, journal, wantSidecars, wantWitnesses, wantJournal)
	}
}

type nativeQueuedRetentionLogHandler struct {
	mu      sync.Mutex
	records []string
}

func (*nativeQueuedRetentionLogHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *nativeQueuedRetentionLogHandler) Handle(_ context.Context, record slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	message := record.Level.String() + " " + record.Message
	record.Attrs(func(attr slog.Attr) bool {
		message += " " + attr.Key + "=" + attr.Value.Resolve().String()
		return true
	})
	h.records = append(h.records, message)
	return nil
}

func (h *nativeQueuedRetentionLogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *nativeQueuedRetentionLogHandler) WithGroup(string) slog.Handler      { return h }

func (h *nativeQueuedRetentionLogHandler) String() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return strings.Join(h.records, "; ")
}

func waitNativeQueuedRetention(t *testing.T, done func() bool, diagnostic func() string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatalf("native queued retention did not reach its durable result: %s", diagnostic())
		}
		time.Sleep(time.Millisecond)
	}
}
