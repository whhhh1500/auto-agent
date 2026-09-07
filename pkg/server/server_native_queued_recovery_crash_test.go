package server

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/cc-auto-agent/harness-core/internal/testdb"
	"github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
)

// This kills a second Native Strict process only after window A committed the
// canonical tool/result and V3 sidecar. A third process must take window B,
// continue exactly once, and never repeat the business effect.
func TestNativeQueuedCompletedToolDeliveryProcessCrashReadback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	db := testdb.Postgres(t)()
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

	first, firstDone := startNativeRecoveryCrashChild(t, ctx, schema, "crash")
	waitNativeRecoveryCrashJournal(t, ctx, db, firstDone)
	killNativeRecoveryCrashChild(t, first, firstDone, "initial journal")

	queue, err := storage.NewSQLRunControlStore(db, storage.SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	requeueNativeRecoveryCrashClaim(t, ctx, db, queue)
	second, secondDone := startNativeRecoveryCrashChild(t, ctx, schema, "delivery_crash")
	waitNativeRecoveryCrashDelivery(t, ctx, db, secondDone)
	killNativeRecoveryCrashChild(t, second, secondDone, "committed delivery")

	requeueNativeRecoveryCrashClaim(t, ctx, db, queue)
	third, thirdDone := startNativeRecoveryCrashChild(t, ctx, schema, "recover")
	if err := <-thirdDone; err != nil {
		t.Fatalf("B readback child failed: %v", err)
	}
	if third.ProcessState == nil || !third.ProcessState.Success() {
		t.Fatalf("B readback child did not exit successfully: %#v", third.ProcessState)
	}

	var effects, models, sidecars int64
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM native_recovery_crash_effects`).Scan(&effects); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(SUM(calls), 0) FROM native_recovery_crash_model_calls`).Scan(&models); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM completed_tool_result_recovery_sidecars WHERE session_id=$1 AND run_id=$2`, nativeRecoveryCrashSessionID, nativeRecoveryCrashRunID).Scan(&sidecars); err != nil {
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
	if err != nil || run.Status != string(core.RunCompleted) || effects != 1 || models != 2 || sidecars != 1 || results != 1 {
		t.Fatalf("run=%+v effects=%d models=%d sidecars=%d results=%d err=%v", run, effects, models, sidecars, results, err)
	}
}

func startNativeRecoveryCrashChild(t *testing.T, ctx context.Context, schema, phase string) (*exec.Cmd, <-chan error) {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	child := exec.CommandContext(ctx, executable, "-test.run=^TestNativeQueuedCompletedToolCrashHelper$", "-test.timeout=100s")
	hideCrashHelperWindow(child)
	child.Env = append(os.Environ(), "HARNESS_NATIVE_COMPLETED_TOOL_CRASH_HELPER=1", "HARNESS_CRASH_SCHEMA="+schema, "HARNESS_NATIVE_RECOVERY_PHASE="+phase)
	var output bytes.Buffer
	child.Stdout, child.Stderr = &output, &output
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		err := child.Wait()
		if err != nil {
			done <- err
			return
		}
		done <- nil
	}()
	t.Cleanup(func() { _ = child.Process.Kill() })
	return child, done
}

func waitNativeRecoveryCrashJournal(t *testing.T, ctx context.Context, db *sql.DB, done <-chan error) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		var state string
		err := db.QueryRowContext(ctx, `SELECT state FROM tool_invocations WHERE session_id=$1 AND run_id=$2 AND call_id=$3`, nativeRecoveryCrashSessionID, nativeRecoveryCrashRunID, nativeRecoveryCrashCallID).Scan(&state)
		if err == nil && state == string(core.ToolInvocationCompleted) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for completed journal: %v", err)
		}
		select {
		case childErr := <-done:
			t.Fatalf("initial child exited before completed journal: %v", childErr)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func waitNativeRecoveryCrashDelivery(t *testing.T, ctx context.Context, db *sql.DB, done <-chan error) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		var sidecars int
		err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM completed_tool_result_recovery_sidecars WHERE session_id=$1 AND run_id=$2`, nativeRecoveryCrashSessionID, nativeRecoveryCrashRunID).Scan(&sidecars)
		if err == nil && sidecars == 1 {
			events := processCrashSQLHistory(t, ctx, db, nativeRecoveryCrashSessionID)
			if len(events) > 0 && events[len(events)-1].RunID == nativeRecoveryCrashRunID && events[len(events)-1].Type == core.EvToolResult {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for committed tool/result delivery: sidecars=%d err=%v", sidecars, err)
		}
		select {
		case childErr := <-done:
			t.Fatalf("delivery child exited before committed tool/result: %v", childErr)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func killNativeRecoveryCrashChild(t *testing.T, child *exec.Cmd, done <-chan error, stage string) {
	t.Helper()
	if err := child.Process.Kill(); err != nil {
		t.Fatalf("kill %s child: %v", stage, err)
	}
	if err := <-done; err == nil || child.ProcessState == nil || child.ProcessState.Success() {
		t.Fatalf("%s child did not exit abnormally: %v", stage, err)
	}
}

func requeueNativeRecoveryCrashClaim(t *testing.T, ctx context.Context, db *sql.DB, queue *storage.SQLRunControlStore) {
	t.Helper()
	if _, err := db.ExecContext(ctx, `UPDATE run_queue SET lease_expires_at=1 WHERE run_id=$1`, nativeRecoveryCrashRunID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE session_leases SET expires_at=1 WHERE session_id=$1`, nativeRecoveryCrashSessionID); err != nil {
		t.Fatal(err)
	}
	if requeued, failed, err := queue.RecoverExpiredRunClaims(ctx, time.Now().UTC()); err != nil || requeued != 1 || failed != 0 {
		t.Fatalf("requeue=%d failed=%d err=%v", requeued, failed, err)
	}
}
