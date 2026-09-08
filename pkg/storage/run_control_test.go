package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

func TestSQLRunControlLifecycle(t *testing.T) {
	sessions := newTestSQLStore(t)
	store, err := NewSQLRunControlStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	record := RunRecord{
		RunID: "run-control-1", SessionID: "session-control",
		TenantID: "acme", SubjectID: "alice", Status: RunStatusRunning,
	}
	if err := store.CreateRun(ctx, record); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateRun(ctx, record); !errors.Is(err, core.ErrSessionConflict) {
		t.Fatalf("duplicate run did not conflict: %v", err)
	}
	if err := store.CreateRun(ctx, RunRecord{RunID: "run-control-2", SessionID: record.SessionID, TenantID: "acme", SubjectID: "alice"}); !errors.Is(err, core.ErrSessionConflict) {
		t.Fatalf("second active run for one session did not conflict: %v", err)
	}
	loaded, err := store.GetRun(ctx, record.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Status != RunStatusRunning || loaded.CancelRequested || loaded.SessionID != record.SessionID {
		t.Fatalf("unexpected run record: %#v", loaded)
	}
	beforeHeartbeat := loaded.UpdatedAt
	time.Sleep(time.Millisecond)
	if err := store.HeartbeatRun(ctx, record.RunID); err != nil {
		t.Fatal(err)
	}
	heartbeated, _ := store.GetRun(ctx, record.RunID)
	if !heartbeated.UpdatedAt.After(beforeHeartbeat) {
		t.Fatalf("heartbeat did not advance updated_at: before=%s after=%s", beforeHeartbeat, heartbeated.UpdatedAt)
	}
	active, err := store.FindActiveRun(ctx, record.SessionID)
	if err != nil || active.RunID != record.RunID {
		t.Fatalf("active run lookup failed: %#v %v", active, err)
	}
	requested, err := store.RequestRunCancel(ctx, record.RunID)
	if err != nil || !requested {
		t.Fatalf("cancel request failed: requested=%t err=%v", requested, err)
	}
	cancelled, err := store.RunCancelRequested(ctx, record.RunID)
	if err != nil || !cancelled {
		t.Fatalf("cancel flag missing: cancelled=%t err=%v", cancelled, err)
	}
	if err := store.FinishRun(ctx, record.RunID, core.RunCancelled, "cancel_requested"); err != nil {
		t.Fatal(err)
	}
	finished, err := store.GetRun(ctx, record.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if finished.Status != string(core.RunCancelled) || finished.ErrorCode != "cancel_requested" || finished.CompletedAt.IsZero() {
		t.Fatalf("terminal run record is wrong: %#v", finished)
	}
	if _, err := store.FindActiveRun(ctx, record.SessionID); !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("terminal run remained active: %v", err)
	}
	if requested, err := store.RequestRunCancel(ctx, record.RunID); err != nil || requested {
		t.Fatalf("terminal run accepted cancellation: requested=%t err=%v", requested, err)
	}
}

func TestSQLRunControlListsNewestFirstAndValidatesStatus(t *testing.T) {
	sessions := newTestSQLStore(t)
	store, _ := NewSQLRunControlStore(sessions.db, SQLDialectSQLite)
	ctx := context.Background()
	if err := store.CreateRun(ctx, RunRecord{RunID: "run-list-1", SessionID: "session-list", TenantID: "acme", SubjectID: "alice", CreatedAt: time.Unix(1, 0)}); err != nil {
		t.Fatal(err)
	}
	if err := store.FinishRun(ctx, "run-list-1", core.RunCompleted, ""); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateRun(ctx, RunRecord{RunID: "run-list-2", SessionID: "session-list", TenantID: "acme", SubjectID: "alice", CreatedAt: time.Unix(2, 0)}); err != nil {
		t.Fatal(err)
	}
	runs, err := store.ListRuns(ctx, "session-list", 10)
	if err != nil || len(runs) != 2 || runs[0].RunID != "run-list-2" {
		t.Fatalf("run list is wrong: %#v %v", runs, err)
	}
	if err := store.FinishRun(ctx, "run-list-1", core.RunStatus("unknown"), ""); err == nil {
		t.Fatal("invalid terminal status was accepted")
	}
}

func TestSQLRunControlFailsStaleRuns(t *testing.T) {
	sessions := newTestSQLStore(t)
	store, _ := NewSQLRunControlStore(sessions.db, SQLDialectSQLite)
	ctx := context.Background()
	if err := store.CreateRun(ctx, RunRecord{RunID: "run-stale", SessionID: "session-stale", TenantID: "acme", SubjectID: "alice"}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessions.db.ExecContext(ctx, "UPDATE run_control SET updated_at = 1 WHERE run_id = ?", "run-stale"); err != nil {
		t.Fatal(err)
	}
	failed, err := store.FailStaleRuns(ctx, time.Now().UTC().Add(-time.Minute))
	if err != nil || failed != 1 {
		t.Fatalf("stale recovery failed: count=%d err=%v", failed, err)
	}
	record, err := store.GetRun(ctx, "run-stale")
	if err != nil || record.Status != string(core.RunFailed) || record.ErrorCode != "worker_lost" {
		t.Fatalf("stale run terminal state is wrong: %#v %v", record, err)
	}
}

func TestSQLRunQueueClaimSerializesOneSession(t *testing.T) {
	sessions := newTestSQLStore(t)
	store, _ := NewSQLRunControlStore(sessions.db, SQLDialectSQLite)
	ctx := context.Background()
	for _, runID := range []string{"run-queue-1", "run-queue-2"} {
		if err := store.EnqueueRun(ctx, QueuedRun{
			RunRecord: RunRecord{RunID: runID, SessionID: "session-queue", TenantID: "acme", SubjectID: "alice"},
			Message:   "hello", MaxAttempts: 2,
		}); err != nil {
			t.Fatal(err)
		}
	}
	first, claimed, err := store.ClaimRun(ctx, "worker-a", time.Minute)
	if err != nil || !claimed || first.RunID != "run-queue-1" || first.Attempt != 1 {
		t.Fatalf("first claim wrong: %#v claimed=%t err=%v", first, claimed, err)
	}
	if _, claimed, err := store.ClaimRun(ctx, "worker-b", time.Minute); err != nil || claimed {
		t.Fatalf("second run in same session was claimed concurrently: claimed=%t err=%v", claimed, err)
	}
	if renewed, err := store.RenewRunClaim(ctx, first.RunID, "wrong-worker", first.Generation, time.Minute); err != nil || renewed {
		t.Fatalf("wrong worker renewed claim: renewed=%t err=%v", renewed, err)
	}
	if renewed, err := store.RenewRunClaim(ctx, first.RunID, "worker-a", first.Generation, time.Minute); err != nil || !renewed {
		t.Fatalf("owner failed to renew claim: renewed=%t err=%v", renewed, err)
	}
	if err := store.FinishRun(ctx, first.RunID, core.RunCompleted, ""); err != nil {
		t.Fatal(err)
	}
	second, claimed, err := store.ClaimRun(ctx, "worker-b", time.Minute)
	if err != nil || !claimed || second.RunID != "run-queue-2" {
		t.Fatalf("second claim wrong: %#v claimed=%t err=%v", second, claimed, err)
	}
}

func TestSQLRunQueueAllowsDifferentSessionsAndCancelsQueued(t *testing.T) {
	sessions := newTestSQLStore(t)
	store, _ := NewSQLRunControlStore(sessions.db, SQLDialectSQLite)
	ctx := context.Background()
	for index, sessionID := range []string{"session-a", "session-b"} {
		if err := store.EnqueueRun(ctx, QueuedRun{
			RunRecord: RunRecord{RunID: fmt.Sprintf("run-parallel-%d", index), SessionID: sessionID, TenantID: "acme", SubjectID: "alice"},
			Message:   "hello",
		}); err != nil {
			t.Fatal(err)
		}
	}
	first, claimed, err := store.ClaimRun(ctx, "worker-a", time.Minute)
	if err != nil || !claimed {
		t.Fatalf("first claim failed: %#v %t %v", first, claimed, err)
	}
	second, claimed, err := store.ClaimRun(ctx, "worker-b", time.Minute)
	if err != nil || !claimed || second.SessionID == first.SessionID {
		t.Fatalf("different session was not claimable: first=%#v second=%#v claimed=%t err=%v", first, second, claimed, err)
	}
	if err := store.EnqueueRun(ctx, QueuedRun{
		RunRecord: RunRecord{RunID: "run-cancel-queued", SessionID: "session-cancel", TenantID: "acme", SubjectID: "alice"},
		Message:   "cancel me",
	}); err != nil {
		t.Fatal(err)
	}
	requested, err := store.RequestRunCancel(ctx, "run-cancel-queued")
	if err != nil || !requested {
		t.Fatalf("queued cancellation failed: requested=%t err=%v", requested, err)
	}
	record, err := store.GetRun(ctx, "run-cancel-queued")
	if err != nil || record.Status != string(core.RunCancelled) || !record.CancelRequested {
		t.Fatalf("queued cancellation state wrong: %#v %v", record, err)
	}
}

func TestSQLRunQueueRecoversExpiredClaimsUntilAttemptsExhausted(t *testing.T) {
	sessions := newTestSQLStore(t)
	store, _ := NewSQLRunControlStore(sessions.db, SQLDialectSQLite)
	ctx := context.Background()
	if err := store.EnqueueRun(ctx, QueuedRun{
		RunRecord: RunRecord{RunID: "run-retry", SessionID: "session-retry", TenantID: "acme", SubjectID: "alice"},
		Message:   "retry", MaxAttempts: 2,
	}); err != nil {
		t.Fatal(err)
	}
	first, claimed, err := store.ClaimRun(ctx, "worker-a", time.Minute)
	if err != nil || !claimed || first.Attempt != 1 {
		t.Fatalf("first claim failed: %#v %t %v", first, claimed, err)
	}
	if _, err := sessions.db.ExecContext(ctx, "UPDATE run_queue SET lease_expires_at = 1 WHERE run_id = ?", first.RunID); err != nil {
		t.Fatal(err)
	}
	requeued, failed, err := store.RecoverExpiredRunClaims(ctx, time.Now().UTC())
	if err != nil || requeued != 1 || failed != 0 {
		t.Fatalf("first recovery wrong: requeued=%d failed=%d err=%v", requeued, failed, err)
	}
	second, claimed, err := store.ClaimRun(ctx, "worker-b", time.Minute)
	if err != nil || !claimed || second.Attempt != 2 {
		t.Fatalf("retry claim wrong: %#v %t %v", second, claimed, err)
	}
	if _, err := sessions.db.ExecContext(ctx, "UPDATE run_queue SET lease_expires_at = 1 WHERE run_id = ?", second.RunID); err != nil {
		t.Fatal(err)
	}
	requeued, failed, err = store.RecoverExpiredRunClaims(ctx, time.Now().UTC())
	if err != nil || requeued != 0 || failed != 1 {
		t.Fatalf("final recovery wrong: requeued=%d failed=%d err=%v", requeued, failed, err)
	}
	terminal, err := store.GetRun(ctx, second.RunID)
	if err != nil || terminal.Status != string(core.RunFailed) || terminal.ErrorCode != "worker_lost" {
		t.Fatalf("exhausted run state wrong: %#v %v", terminal, err)
	}
}

func TestSQLRunQueueFencesExpiredWorkerAndDeletesTerminalPayload(t *testing.T) {
	sessions := newTestSQLStore(t)
	store, _ := NewSQLRunControlStore(sessions.db, SQLDialectSQLite)
	ctx := context.Background()
	if err := store.EnqueueRun(ctx, QueuedRun{
		RunRecord: RunRecord{RunID: "run-fenced", SessionID: "session-fenced", TenantID: "acme", SubjectID: "alice"},
		Message:   "side effect", MaxAttempts: 3,
	}); err != nil {
		t.Fatal(err)
	}
	first, claimed, err := store.ClaimRun(ctx, "worker-old", time.Minute)
	if err != nil || !claimed {
		t.Fatalf("first claim failed: %#v claimed=%t err=%v", first, claimed, err)
	}
	if _, err := sessions.db.ExecContext(ctx, "UPDATE run_queue SET lease_expires_at = 1 WHERE run_id = ?", first.RunID); err != nil {
		t.Fatal(err)
	}
	if requeued, failed, err := store.RecoverExpiredRunClaims(ctx, time.Now().UTC()); err != nil || requeued != 1 || failed != 0 {
		t.Fatalf("claim recovery failed: requeued=%d failed=%d err=%v", requeued, failed, err)
	}
	second, claimed, err := store.ClaimRun(ctx, "worker-new", time.Minute)
	if err != nil || !claimed || second.Attempt != first.Attempt+1 {
		t.Fatalf("second claim failed: %#v claimed=%t err=%v", second, claimed, err)
	}
	if renewed, err := store.RenewRunClaim(ctx, first.RunID, "worker-old", first.Generation, time.Minute); err != nil || renewed {
		t.Fatalf("expired worker renewed a newer claim: renewed=%t err=%v", renewed, err)
	}
	if finished, err := store.FinishRunClaim(ctx, first.RunID, "worker-old", first.Generation, core.RunCompleted, ""); err != nil || finished {
		t.Fatalf("expired worker finished a newer claim: finished=%t err=%v", finished, err)
	}
	if retried, err := store.RetryRunClaim(ctx, first.RunID, "worker-old", first.Generation, time.Now().UTC(), "old_worker"); err != nil || retried {
		t.Fatalf("expired worker requeued a newer claim: retried=%t err=%v", retried, err)
	}
	if finished, err := store.FinishRunClaim(ctx, second.RunID, "worker-new", second.Generation, core.RunCompleted, ""); err != nil || !finished {
		t.Fatalf("current worker failed to finish: finished=%t err=%v", finished, err)
	}
	var queueRows int
	if err := sessions.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM run_queue WHERE run_id = ?", second.RunID).Scan(&queueRows); err != nil {
		t.Fatal(err)
	}
	if queueRows != 0 {
		t.Fatalf("terminal queue payload was retained: rows=%d", queueRows)
	}
}

func TestSQLRunQueueRunningCancelPreservesClaimUntilWorkerFinishes(t *testing.T) {
	sessions := newTestSQLStore(t)
	store, _ := NewSQLRunControlStore(sessions.db, SQLDialectSQLite)
	ctx := context.Background()
	if err := store.EnqueueRun(ctx, QueuedRun{
		RunRecord: RunRecord{RunID: "run-cancel-running", SessionID: "session-cancel-running", TenantID: "acme", SubjectID: "alice"},
		Message:   "cancel while running",
	}); err != nil {
		t.Fatal(err)
	}
	claimedRun, claimed, err := store.ClaimRun(ctx, "worker-cancel", time.Minute)
	if err != nil || !claimed {
		t.Fatalf("claim failed: %#v claimed=%t err=%v", claimedRun, claimed, err)
	}
	if requested, err := store.RequestRunCancel(ctx, claimedRun.RunID); err != nil || !requested {
		t.Fatalf("cancel request failed: requested=%t err=%v", requested, err)
	}
	if renewed, err := store.RenewRunClaim(ctx, claimedRun.RunID, "worker-cancel", claimedRun.Generation, time.Minute); err != nil || !renewed {
		t.Fatalf("cancel request destroyed the live claim: renewed=%t err=%v", renewed, err)
	}
	if finished, err := store.FinishRunClaim(ctx, claimedRun.RunID, "worker-cancel", claimedRun.Generation, core.RunCancelled, "cancel_requested"); err != nil || !finished {
		t.Fatalf("cancelled worker failed to finish: finished=%t err=%v", finished, err)
	}
}

func TestSQLRunQueueIdempotentSubmissionReturnsCanonicalRun(t *testing.T) {
	sessions := newTestSQLStore(t)
	store, _ := NewSQLRunControlStore(sessions.db, SQLDialectSQLite)
	ctx := context.Background()
	message := "perform one durable action"
	digest := RunMessageDigest(message)
	firstRun := QueuedRun{
		RunRecord: RunRecord{RunID: "run-submit-first", SessionID: "session-submit", TenantID: "acme", SubjectID: "alice"},
		Message:   message,
	}
	first, created, err := store.EnqueueRunOnce(ctx, firstRun, "client-request-42", digest)
	if err != nil || !created || first.RunID != firstRun.RunID || first.Status != RunStatusQueued {
		t.Fatalf("first submission wrong: %#v created=%t err=%v", first, created, err)
	}
	replayRun := firstRun
	replayRun.RunID = "run-submit-unused"
	replayed, created, err := store.EnqueueRunOnce(ctx, replayRun, "client-request-42", digest)
	if err != nil || created || replayed.RunID != firstRun.RunID {
		t.Fatalf("submission replay wrong: %#v created=%t err=%v", replayed, created, err)
	}
	for table, want := range map[string]int{"run_control": 1, "run_queue": 1, "run_submissions": 1} {
		var count int
		if err := sessions.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != want {
			t.Fatalf("%s rows=%d, want %d", table, count, want)
		}
	}
	if _, _, err := store.EnqueueRunOnce(ctx, replayRun, "client-request-42", RunMessageDigest("different")); !errors.Is(err, ErrRunSubmissionConflict) {
		t.Fatalf("different request reused key: %v", err)
	}
	otherSession := replayRun
	otherSession.RunID = "run-submit-other-session"
	otherSession.SessionID = "session-submit-other"
	if record, created, err := store.EnqueueRunOnce(ctx, otherSession, "client-request-42", digest); err != nil || !created || record.RunID != otherSession.RunID {
		t.Fatalf("same key could not be scoped to another session: %#v created=%t err=%v", record, created, err)
	}
	var storedHash string
	if err := sessions.db.QueryRowContext(ctx,
		"SELECT key_hash FROM run_submissions WHERE run_id = ?", firstRun.RunID,
	).Scan(&storedHash); err != nil {
		t.Fatal(err)
	}
	if storedHash == "client-request-42" || len(storedHash) != 64 {
		t.Fatalf("raw idempotency key was stored: %q", storedHash)
	}
}

func TestSQLRunQueueIdempotentSubmissionReplaysTerminalState(t *testing.T) {
	sessions := newTestSQLStore(t)
	store, _ := NewSQLRunControlStore(sessions.db, SQLDialectSQLite)
	ctx := context.Background()
	run := QueuedRun{
		RunRecord: RunRecord{RunID: "run-submit-terminal", SessionID: "session-submit-terminal", TenantID: "acme", SubjectID: "alice"},
		Message:   "terminal replay",
	}
	digest := RunMessageDigest(run.Message)
	if _, created, err := store.EnqueueRunOnce(ctx, run, "terminal-key", digest); err != nil || !created {
		t.Fatalf("enqueue failed: created=%t err=%v", created, err)
	}
	claimed, ok, err := store.ClaimRun(ctx, "worker-terminal", time.Minute)
	if err != nil || !ok {
		t.Fatalf("claim failed: %#v ok=%t err=%v", claimed, ok, err)
	}
	if finished, err := store.FinishRunClaim(ctx, claimed.RunID, "worker-terminal", claimed.Generation, core.RunCompleted, ""); err != nil || !finished {
		t.Fatalf("finish failed: finished=%t err=%v", finished, err)
	}
	replay := run
	replay.RunID = "run-submit-terminal-unused"
	record, created, err := store.EnqueueRunOnce(ctx, replay, "terminal-key", digest)
	if err != nil || created || record.RunID != run.RunID || record.Status != string(core.RunCompleted) {
		t.Fatalf("terminal replay wrong: %#v created=%t err=%v", record, created, err)
	}
}

func TestSQLRunQueueRejectsInvalidSubmissionKey(t *testing.T) {
	sessions := newTestSQLStore(t)
	store, _ := NewSQLRunControlStore(sessions.db, SQLDialectSQLite)
	run := QueuedRun{
		RunRecord: RunRecord{RunID: "run-submit-invalid", SessionID: "session-submit-invalid", TenantID: "acme", SubjectID: "alice"},
		Message:   "invalid key",
	}
	if _, _, err := store.EnqueueRunOnce(context.Background(), run, "bad\nkey", RunMessageDigest(run.Message)); !errors.Is(err, ErrInvalidRunSubmission) {
		t.Fatalf("invalid key error=%v", err)
	}
	if _, _, err := store.EnqueueRunOnce(context.Background(), run, "bad\u0085key", RunMessageDigest(run.Message)); !errors.Is(err, ErrInvalidRunSubmission) {
		t.Fatalf("unicode control key error=%v", err)
	}
}

func TestSQLRunQueueSubmissionRollsBackKeyWhenRunCreationFails(t *testing.T) {
	sessions := newTestSQLStore(t)
	store, _ := NewSQLRunControlStore(sessions.db, SQLDialectSQLite)
	ctx := context.Background()
	existing := RunRecord{
		RunID: "run-submit-rollback", SessionID: "session-submit-existing", TenantID: "acme", SubjectID: "alice",
	}
	if err := store.CreateRun(ctx, existing); err != nil {
		t.Fatal(err)
	}
	failed := QueuedRun{
		RunRecord: RunRecord{RunID: existing.RunID, SessionID: "session-submit-failed", TenantID: "acme", SubjectID: "alice"},
		Message:   "must roll back",
	}
	if _, _, err := store.EnqueueRunOnce(ctx, failed, "rollback-key", RunMessageDigest(failed.Message)); err == nil {
		t.Fatal("duplicate run id unexpectedly succeeded")
	}
	var mappings int
	if err := sessions.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM run_submissions WHERE key_hash = ?", hashIdempotencyKey("rollback-key")).Scan(&mappings); err != nil {
		t.Fatal(err)
	}
	if mappings != 0 {
		t.Fatalf("failed transaction retained %d key mappings", mappings)
	}
	retry := failed
	retry.RunID = "run-submit-rollback-retry"
	record, created, err := store.EnqueueRunOnce(ctx, retry, "rollback-key", RunMessageDigest(retry.Message))
	if err != nil || !created || record.RunID != retry.RunID {
		t.Fatalf("rolled-back key could not be reused: %#v created=%t err=%v", record, created, err)
	}
}

func TestRunSubmissionRetentionKeepsActiveFences(t *testing.T) {
	sessions := newTestSQLStore(t)
	store, _ := NewSQLRunControlStore(sessions.db, SQLDialectSQLite)
	ctx := context.Background()
	terminal := QueuedRun{
		RunRecord: RunRecord{RunID: "run-submit-prune-terminal", SessionID: "session-prune-terminal", TenantID: "acme", SubjectID: "alice"},
		Message:   "terminal", AvailableAt: time.Now().UTC().Add(-time.Second),
	}
	active := QueuedRun{
		RunRecord: RunRecord{RunID: "run-submit-prune-active", SessionID: "session-prune-active", TenantID: "acme", SubjectID: "alice"},
		Message:   "active",
	}
	if _, _, err := store.EnqueueRunOnce(ctx, terminal, "prune-terminal", RunMessageDigest(terminal.Message)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.EnqueueRunOnce(ctx, active, "prune-active", RunMessageDigest(active.Message)); err != nil {
		t.Fatal(err)
	}
	claimed, ok, err := store.ClaimRun(ctx, "worker-prune", time.Minute)
	if err != nil || !ok || claimed.RunID != terminal.RunID {
		t.Fatalf("terminal setup claim wrong: %#v ok=%t err=%v", claimed, ok, err)
	}
	if finished, err := store.FinishRunClaim(ctx, claimed.RunID, "worker-prune", claimed.Generation, core.RunCompleted, ""); err != nil || !finished {
		t.Fatalf("terminal setup finish failed: %t %v", finished, err)
	}
	if _, err := sessions.db.ExecContext(ctx, "UPDATE run_submissions SET created_at = 1"); err != nil {
		t.Fatal(err)
	}
	deleted, err := sessions.PruneRunSubmissions(ctx, time.Now().UTC().Add(-time.Minute))
	if err != nil || deleted != 1 {
		t.Fatalf("submission retention wrong: deleted=%d err=%v", deleted, err)
	}
	var activeRows int
	if err := sessions.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM run_submissions WHERE run_id = ?", active.RunID).Scan(&activeRows); err != nil {
		t.Fatal(err)
	}
	if activeRows != 1 {
		t.Fatal("retention deleted an active submission fence")
	}
}

func TestSQLOperationalMetricsSnapshots(t *testing.T) {
	sessions := newTestSQLStore(t)
	queue, _ := NewSQLRunControlStore(sessions.db, SQLDialectSQLite)
	approvals, _ := NewSQLApprovalStore(sessions.db, SQLDialectSQLite)
	ctx := context.Background()
	runID := "run-operational-metrics"
	if err := queue.EnqueueRun(ctx, QueuedRun{
		RunRecord: RunRecord{RunID: runID, SessionID: "session-operational-metrics", TenantID: "acme", SubjectID: "alice"},
		Message:   "observe",
	}); err != nil {
		t.Fatal(err)
	}
	metrics, err := queue.RunQueueMetrics(ctx)
	if err != nil || metrics.Queued != 1 || metrics.Running != 0 || metrics.WaitingApproval != 0 || metrics.OldestQueuedAt.IsZero() {
		t.Fatalf("queued metrics wrong: %#v err=%v", metrics, err)
	}
	claimed, ok, err := queue.ClaimRun(ctx, "metrics-worker", time.Minute)
	if err != nil || !ok {
		t.Fatalf("claim failed: %#v ok=%t err=%v", claimed, ok, err)
	}
	metrics, err = queue.RunQueueMetrics(ctx)
	if err != nil || metrics.Queued != 0 || metrics.Running != 1 || !metrics.OldestQueuedAt.IsZero() {
		t.Fatalf("running metrics wrong: %#v err=%v", metrics, err)
	}
	request := approvalTestRequest(runID, "call-operational-metrics")
	resolution, err := approvals.RequestApproval(ctx, request)
	if err != nil || resolution.Decision != core.ApprovalPending {
		t.Fatalf("approval request failed: %#v err=%v", resolution, err)
	}
	if paused, err := queue.PauseRunClaim(ctx, runID, "metrics-worker", claimed.Generation); err != nil || !paused {
		t.Fatalf("pause failed: paused=%t err=%v", paused, err)
	}
	metrics, err = queue.RunQueueMetrics(ctx)
	if err != nil || metrics.Running != 0 || metrics.WaitingApproval != 1 {
		t.Fatalf("waiting metrics wrong: %#v err=%v", metrics, err)
	}
	approvalMetrics, err := approvals.ApprovalMetrics(ctx)
	if err != nil || approvalMetrics.Pending != 1 || approvalMetrics.OldestRequestedAt.IsZero() {
		t.Fatalf("approval metrics wrong: %#v err=%v", approvalMetrics, err)
	}
}

func TestSQLRunControlRejectsInFlightOverflow(t *testing.T) {
	sessions := newTestSQLStore(t)
	store, err := NewSQLRunControlStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	store.maxInFlight = 2
	ctx := context.Background()
	first := RunRecord{RunID: "run-inflight-1", SessionID: "session-inflight-1", TenantID: "acme", SubjectID: "alice", Status: RunStatusRunning}
	second := RunRecord{RunID: "run-inflight-2", SessionID: "session-inflight-2", TenantID: "acme", SubjectID: "bob", Status: RunStatusRunning}
	third := RunRecord{RunID: "run-inflight-3", SessionID: "session-inflight-3", TenantID: "acme", SubjectID: "cara", Status: RunStatusRunning}
	if err := store.CreateRun(ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateRun(ctx, second); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateRun(ctx, third); err == nil {
		t.Fatal("in-flight run overflow was accepted")
	}
	if err := store.CreateRun(ctx, first); !errors.Is(err, core.ErrSessionConflict) {
		t.Fatalf("existing in-flight run must still conflict: %v", err)
	}
	if err := store.FinishRun(ctx, first.RunID, core.RunCompleted, ""); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateRun(ctx, third); err != nil {
		t.Fatalf("completed run did not free an in-flight slot: %v", err)
	}
}

func TestSQLRunControlCapacityIgnoresTerminalHistory(t *testing.T) {
	sessions := newTestSQLStore(t)
	store, err := NewSQLRunControlStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	store.maxInFlight = 2
	ctx := context.Background()

	// A large terminal history must not consume the in-flight threshold. This
	// verifies the status semantics; it does not assert a bounded table scan.
	tx, err := sessions.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4096; i++ {
		if _, err := tx.ExecContext(ctx, `INSERT INTO run_control
			(run_id, session_id, tenant_id, subject_id, status, cancel_requested, error_code, created_at, updated_at, completed_at)
			VALUES (?, ?, ?, ?, 'completed', 0, '', 1, 1, 1)`,
			fmt.Sprintf("terminal-history-%04d", i), fmt.Sprintf("terminal-session-%04d", i), "acme", "history"); err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	first := RunRecord{RunID: "history-active-1", SessionID: "history-active-1", TenantID: "acme", SubjectID: "alice", Status: RunStatusRunning}
	second := RunRecord{RunID: "history-active-2", SessionID: "history-active-2", TenantID: "acme", SubjectID: "bob", Status: RunStatusRunning}
	third := RunRecord{RunID: "history-active-3", SessionID: "history-active-3", TenantID: "acme", SubjectID: "cara", Status: RunStatusRunning}
	if err := store.CreateRun(ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateRun(ctx, second); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateRun(ctx, third); err == nil {
		t.Fatal("in-flight run overflow was accepted after terminal history")
	}
}

func TestSQLRunQueueRejectsSubmissionOverflow(t *testing.T) {
	sessions := newTestSQLStore(t)
	store, err := NewSQLRunControlStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	store.maxSubmissions = 2
	ctx := context.Background()
	enqueue := func(runID, key string) (RunRecord, bool, error) {
		run := QueuedRun{
			RunRecord: RunRecord{RunID: runID, SessionID: "session-submit-cap", TenantID: "acme", SubjectID: "alice"},
			Message:   "submit " + runID,
		}
		return store.EnqueueRunOnce(ctx, run, key, RunMessageDigest(run.Message))
	}
	first, created, err := enqueue("run-submit-cap-1", "key-1")
	if err != nil || !created {
		t.Fatalf("first submission failed: %#v created=%t err=%v", first, created, err)
	}
	if _, created, err := enqueue("run-submit-cap-2", "key-2"); err != nil || !created {
		t.Fatalf("second submission failed: created=%t err=%v", created, err)
	}
	if _, _, err := enqueue("run-submit-cap-3", "key-3"); err == nil {
		t.Fatal("run submission overflow was accepted")
	} else if !strings.Contains(err.Error(), "run submissions exceed maximum of 2") {
		t.Fatalf("unexpected overflow error: %v", err)
	}
	replay := QueuedRun{
		RunRecord: RunRecord{RunID: "run-submit-unused", SessionID: "session-submit-cap", TenantID: "acme", SubjectID: "alice"},
		Message:   "submit run-submit-cap-1",
	}
	replayed, created, err := store.EnqueueRunOnce(ctx, replay, "key-1", RunMessageDigest(replay.Message))
	if err != nil || created || replayed.RunID != first.RunID {
		t.Fatalf("existing submission was blocked by the cap: %#v created=%t err=%v", replayed, created, err)
	}
}

func TestSQLRunQueueSubmissionCapacityWithHistoricalRows(t *testing.T) {
	sessions := newTestSQLStore(t)
	store, err := NewSQLRunControlStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	store.maxSubmissions = 2
	ctx := context.Background()

	// Submission history intentionally counts toward the global cap. This
	// fixture proves that the cap and overflow behavior remain unchanged with a
	// large history.
	tx, err := sessions.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4096; i++ {
		key := fmt.Sprintf("submission-history-%04d", i)
		if _, err := tx.ExecContext(ctx, `INSERT INTO run_submissions
			(tenant_id, subject_id, session_id, key_hash, request_digest, run_id, created_at)
			VALUES (?, ?, ?, ?, ?, ?, 1)`,
			"acme", "history", "submission-history", hashIdempotencyKey(key), RunMessageDigest(key), key); err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	run := QueuedRun{
		RunRecord: RunRecord{RunID: "submission-history-overflow", SessionID: "submission-history-new", TenantID: "acme", SubjectID: "alice"},
		Message:   "historical submission must remain capped",
	}
	if _, _, err := store.EnqueueRunOnce(ctx, run, "submission-history-new", RunMessageDigest(run.Message)); err == nil {
		t.Fatal("submission overflow was accepted after historical rows")
	} else if !strings.Contains(err.Error(), "run submissions exceed maximum of 2") {
		t.Fatalf("unexpected historical submission overflow error: %v", err)
	}
}
