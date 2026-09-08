package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/storage"
)

func TestNativeQueuedCompletedToolRecoveryDeliversCompletedJournalOnce(t *testing.T) {
	ctx := context.Background()
	fixture, blocked := newNativeQueuedRecoveryBlockedFixture(t)
	successor := newNativeStrictWitnessRecoveryServer(t, fixture.db, fixture.toolCalls, fixture.modelCalls)

	requeueNativeQueuedRecoveryClaim(t, fixture)
	claimed, err := successor.RunWorkerOnce(ctx, "worker-native-completed-tool-a")
	if err != nil || !claimed {
		t.Fatalf("successor claimed=%t err=%v", claimed, err)
	}
	assertNativeQueuedRecoveryCompleted(t, fixture, 1)

	close(blocked.release)
	if err := <-blocked.done; err == nil {
		t.Fatal("stale pre-recovery worker unexpectedly completed")
	}
	claimed, err = successor.RunWorkerOnce(ctx, "worker-native-completed-tool-a-repeat")
	if err != nil || claimed {
		t.Fatalf("repeat claimed=%t err=%v", claimed, err)
	}
	assertNativeQueuedRecoveryCompleted(t, fixture, 1)
}

func TestNativeQueuedCompletedToolRecoveryReadsCommittedSidecarAfterResponseLoss(t *testing.T) {
	ctx := context.Background()
	fixture, blocked := newNativeQueuedRecoveryBlockedFixture(t)
	firstSuccessor := newNativeStrictWitnessRecoveryServer(t, fixture.db, fixture.toolCalls, fixture.modelCalls)
	responseLost := make(chan struct{})
	firstSuccessor.nativeQueuedRecoveryTestHooks = &nativeQueuedRecoveryTestHooks{
		afterCompletedToolRecovery: func() { close(responseLost); panic("native recovery response lost") },
	}

	requeueNativeQueuedRecoveryClaim(t, fixture)
	if value := runNativeQueuedRecoveryWorkerPanic(t, firstSuccessor, "worker-native-completed-tool-b-first"); value != "native recovery response lost" {
		t.Fatalf("response-lost panic=%#v", value)
	}
	<-responseLost
	requeueNativeQueuedRecoveryClaim(t, fixture)
	secondSuccessor := newNativeStrictWitnessRecoveryServer(t, fixture.db, fixture.toolCalls, fixture.modelCalls)
	claimed, err := secondSuccessor.RunWorkerOnce(ctx, "worker-native-completed-tool-b-second")
	if err != nil || !claimed {
		t.Fatalf("readback successor claimed=%t err=%v", claimed, err)
	}
	assertNativeQueuedRecoveryCompleted(t, fixture, 1)

	close(blocked.release)
	if err := <-blocked.done; err == nil {
		t.Fatal("stale pre-recovery worker unexpectedly completed")
	}
}

func TestNativeQueuedCompletedToolRecoveryRejectsMissingWitnessWithoutProviderReplay(t *testing.T) {
	ctx := context.Background()
	fixture, blocked := newNativeQueuedRecoveryBlockedFixture(t)
	if _, err := fixture.db.ExecContext(ctx, `DELETE FROM native_queued_tool_effect_witnesses WHERE session_id = ? AND run_id = ?`, fixture.session.ID(), fixture.runID); err != nil {
		t.Fatal(err)
	}
	successor := newNativeStrictWitnessRecoveryServer(t, fixture.db, fixture.toolCalls, fixture.modelCalls)
	requeueNativeQueuedRecoveryClaim(t, fixture)
	claimed, err := successor.RunWorkerOnce(ctx, "worker-native-completed-tool-missing-witness")
	if err == nil || !claimed {
		t.Fatalf("missing witness claimed=%t err=%v", claimed, err)
	}
	assertNativeQueuedRecoveryRejected(t, fixture, 0)

	close(blocked.release)
	if err := <-blocked.done; err == nil {
		t.Fatal("stale pre-recovery worker unexpectedly completed")
	}
}

func TestNativeQueuedCompletedToolRecoveryRejectsDurablePrincipalMismatchWithoutProviderReplay(t *testing.T) {
	ctx := context.Background()
	fixture, blocked := newNativeQueuedRecoveryBlockedFixture(t)
	var header string
	if err := fixture.db.QueryRowContext(ctx, `SELECT header FROM sessions WHERE id = ?`, fixture.session.ID()).Scan(&header); err != nil {
		t.Fatal(err)
	}
	var options core.SessionOptions
	if err := json.Unmarshal([]byte(header), &options); err != nil {
		t.Fatal(err)
	}
	options.Principal.Attributes["recovery-test-mismatch"] = "present"
	changed, err := json.Marshal(options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.ExecContext(ctx, `UPDATE sessions SET header = ? WHERE id = ?`, string(changed), fixture.session.ID()); err != nil {
		t.Fatal(err)
	}

	successor := newNativeStrictWitnessRecoveryServer(t, fixture.db, fixture.toolCalls, fixture.modelCalls)
	requeueNativeQueuedRecoveryClaim(t, fixture)
	claimed, err := successor.RunWorkerOnce(ctx, "worker-native-completed-tool-principal-mismatch")
	if err == nil || !claimed {
		t.Fatalf("principal mismatch claimed=%t err=%v", claimed, err)
	}
	assertNativeQueuedRecoveryRejected(t, fixture, 0)

	close(blocked.release)
	if err := <-blocked.done; err == nil {
		t.Fatal("stale pre-recovery worker unexpectedly completed")
	}
}

func TestNativeQueuedCompletedToolRecoveryCancellationStopsWithoutContinuationOrSuffix(t *testing.T) {
	ctx := context.Background()
	fixture, blocked := newNativeQueuedRecoveryBlockedFixture(t)
	requested, err := fixture.api.runControl.RequestRunCancel(ctx, fixture.runID)
	if err != nil || !requested {
		t.Fatalf("request cancel=%t err=%v", requested, err)
	}
	if _, err := fixture.db.ExecContext(ctx, `UPDATE run_queue SET lease_expires_at = 1 WHERE run_id = ?`, fixture.runID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.ExecContext(ctx, `UPDATE session_leases SET expires_at = 1 WHERE session_id = ?`, fixture.session.ID()); err != nil {
		t.Fatal(err)
	}
	requeued, failed, err := fixture.api.runQueue.RecoverExpiredRunClaims(ctx, time.Now().UTC())
	if err != nil || requeued != 0 || failed != 1 {
		t.Fatalf("cancel requeue=%d failed=%d err=%v", requeued, failed, err)
	}
	successor := newNativeStrictWitnessRecoveryServer(t, fixture.db, fixture.toolCalls, fixture.modelCalls)
	claimed, err := successor.RunWorkerOnce(ctx, "worker-native-completed-tool-cancelled")
	if err != nil || claimed {
		t.Fatalf("cancelled successor claimed=%t err=%v", claimed, err)
	}
	run, err := fixture.api.runQueue.GetRun(ctx, fixture.runID)
	if err != nil || run.Status != string(core.RunCancelled) || fixture.modelCalls.Load() != 1 || fixture.toolCalls.Load() != 1 {
		t.Fatalf("run=%+v models=%d tools=%d err=%v", run, fixture.modelCalls.Load(), fixture.toolCalls.Load(), err)
	}
	loaded, err := fixture.api.sessions.Load(ctx, fixture.session.ID())
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range loaded.Events() {
		if event.RunID == fixture.runID && (event.Type == core.EvToolResult || event.Type == core.EvRunError || event.Type == core.EvRunEnd) {
			t.Fatalf("cancelled recovery wrote a suffix event %s", event.Type)
		}
	}

	close(blocked.release)
	if err := <-blocked.done; err == nil {
		t.Fatal("cancelled stale worker unexpectedly completed")
	}
}

func TestNativeQueuedCompletedToolRecoveryRejectsRevokedCurrentPrincipalWithoutContinuation(t *testing.T) {
	ctx := context.Background()
	fixture, blocked := newNativeQueuedRecoveryBlockedFixture(t)
	if err := fixture.accounts.SetAccountStatus(ctx, "alice", storage.AccountDisabled); err != nil {
		t.Fatal(err)
	}
	successor := newNativeStrictWitnessRecoveryServer(t, fixture.db, fixture.toolCalls, fixture.modelCalls)
	requeueNativeQueuedRecoveryClaim(t, fixture)
	claimed, err := successor.RunWorkerOnce(ctx, "worker-native-completed-tool-principal-revoked")
	if err == nil || !claimed {
		t.Fatalf("revoked principal claimed=%t err=%v", claimed, err)
	}
	run, err := fixture.api.runQueue.GetRun(ctx, fixture.runID)
	if err != nil || run.Status != string(core.RunFailed) || run.ErrorCode != "principal_resolution_failed" || fixture.modelCalls.Load() != 1 || fixture.toolCalls.Load() != 1 {
		t.Fatalf("run=%+v models=%d tools=%d err=%v", run, fixture.modelCalls.Load(), fixture.toolCalls.Load(), err)
	}
	loaded, err := fixture.api.sessions.Load(ctx, fixture.session.ID())
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range loaded.Events() {
		if event.RunID == fixture.runID && event.Type == core.EvToolResult {
			t.Fatal("revoked principal recovery fabricated a tool/result")
		}
	}

	close(blocked.release)
	if err := <-blocked.done; err == nil {
		t.Fatal("revoked stale worker unexpectedly completed")
	}
}

func TestNativeQueuedCompletedToolRecoveryRejectsUnknownModelAttemptWithoutProviderReplay(t *testing.T) {
	ctx := context.Background()
	fixture, blocked := newNativeQueuedRecoveryBlockedFixture(t)
	firstSuccessor := newNativeStrictWitnessRecoveryServer(t, fixture.db, fixture.toolCalls, fixture.modelCalls)
	modelAttempt := make(chan struct{})
	releaseModelAttempt := make(chan struct{})
	firstSuccessor.nativeQueuedRecoveryTestHooks = &nativeQueuedRecoveryTestHooks{
		afterRecoveredModelAttempt: func() { close(modelAttempt); <-releaseModelAttempt },
	}

	requeueNativeQueuedRecoveryClaim(t, fixture)
	firstDone := make(chan error, 1)
	go func() {
		_, err := firstSuccessor.RunWorkerOnce(ctx, "worker-native-completed-tool-unknown-model")
		firstDone <- err
	}()
	select {
	case <-modelAttempt:
	case <-time.After(10 * time.Second):
		t.Fatal("recovered model attempt was not durably admitted")
	}
	if fixture.modelCalls.Load() != 1 {
		t.Fatalf("provider calls after persisted attempt=%d", fixture.modelCalls.Load())
	}
	requeueNativeQueuedRecoveryClaim(t, fixture)
	secondSuccessor := newNativeStrictWitnessRecoveryServer(t, fixture.db, fixture.toolCalls, fixture.modelCalls)
	claimed, err := secondSuccessor.RunWorkerOnce(ctx, "worker-native-completed-tool-unknown-model-retry")
	if err == nil || !claimed {
		t.Fatalf("unknown v45 claimed=%t err=%v", claimed, err)
	}
	assertNativeQueuedRecoveryRejected(t, fixture, 1)

	close(releaseModelAttempt)
	if err := <-firstDone; err == nil {
		t.Fatal("stale model-attempt worker unexpectedly completed")
	}
	close(blocked.release)
	if err := <-blocked.done; err == nil {
		t.Fatal("stale pre-recovery worker unexpectedly completed")
	}
}

type nativeQueuedRecoveryBlockedFixture struct {
	*nativeStrictWitnessFixture
	done    <-chan error
	release chan struct{}
}

func newNativeQueuedRecoveryBlockedFixture(t *testing.T) (*nativeStrictWitnessFixture, *nativeQueuedRecoveryBlockedFixture) {
	t.Helper()
	fixture := newNativeStrictWitnessFixture(t, false)
	reached := make(chan struct{})
	release := make(chan struct{})
	fixture.api.nativeQueuedRecoveryTestHooks = &nativeQueuedRecoveryTestHooks{
		afterToolJournalComplete: func() { close(reached); <-release },
	}
	done := make(chan error, 1)
	go func() {
		_, err := fixture.api.RunWorkerOnce(context.Background(), "worker-native-completed-tool-original")
		done <- err
	}()
	select {
	case <-reached:
	case <-time.After(10 * time.Second):
		run, runErr := fixture.api.runQueue.GetRun(context.Background(), fixture.runID)
		loaded, loadErr := fixture.api.sessions.Load(context.Background(), fixture.session.ID())
		close(release)
		workerErr := <-done
		t.Fatalf("original worker did not finish its SQL journal: run=%+v run_err=%v version=%d events=%v load_err=%v worker_err=%v", run, runErr, loaded.Version(), nativeQueuedRecoveryEventTypes(loaded.Events()), loadErr, workerErr)
	}
	return fixture, &nativeQueuedRecoveryBlockedFixture{nativeStrictWitnessFixture: fixture, done: done, release: release}
}

func nativeQueuedRecoveryEventTypes(events []core.SessionEvent) []core.SessionEventType {
	types := make([]core.SessionEventType, 0, len(events))
	for _, event := range events {
		types = append(types, event.Type)
	}
	return types
}

func newNativeStrictWitnessRecoveryServer(t *testing.T, db *sql.DB, toolCalls, modelCalls *atomic.Int32) *Server {
	t.Helper()
	root := nativeStrictTestRoot()
	selection := core.ModelSelection{Provider: "checkpoint-test", Model: "native-witness"}
	name := "Native witness"
	api, err := NewNativeStrictServer(context.Background(), NativeStrictServerConfig{
		DB: db, Dialect: storage.SQLDialectSQLite,
		Bootstrap: NativeStrictBootstrap{
			Revision: "native-witness-v1", Root: root.Segments(), DefaultProfileID: "native.witness",
			Profiles: []core.AgentProfileLayer{{
				Scope: root, ProfileID: "native.witness", Name: &name, Model: &selection,
				AddCapabilities: []string{"checkpoint.write"},
			}},
			Capabilities: []NativeStrictCapability{{Scope: root, Capability: checkpointTestTool{calls: toolCalls}}},
			Model:        NativeStrictModel{Selection: selection, Adapter: checkpointTestModel{calls: modelCalls}},
		},
		MaxWriteDelay: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = api.Shutdown(context.Background()) })
	return api
}

func requeueNativeQueuedRecoveryClaim(t *testing.T, fixture *nativeStrictWitnessFixture) {
	t.Helper()
	ctx := context.Background()
	if _, err := fixture.db.ExecContext(ctx, `UPDATE run_queue SET lease_expires_at = 1 WHERE run_id = ?`, fixture.runID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.ExecContext(ctx, `UPDATE session_leases SET expires_at = 1 WHERE session_id = ?`, fixture.session.ID()); err != nil {
		t.Fatal(err)
	}
	requeued, failed, err := fixture.api.runQueue.RecoverExpiredRunClaims(ctx, time.Now().UTC())
	if err != nil || requeued != 1 || failed != 0 {
		t.Fatalf("requeue=%d failed=%d err=%v", requeued, failed, err)
	}
}

func runNativeQueuedRecoveryWorkerPanic(t *testing.T, api *Server, workerID string) (recovered any) {
	t.Helper()
	defer func() { recovered = recover() }()
	_, _ = api.RunWorkerOnce(context.Background(), workerID)
	return nil
}

func assertNativeQueuedRecoveryCompleted(t *testing.T, fixture *nativeStrictWitnessFixture, wantSidecars int) {
	t.Helper()
	ctx := context.Background()
	run, err := fixture.api.runQueue.GetRun(ctx, fixture.runID)
	if err != nil || run.Status != string(core.RunCompleted) || fixture.toolCalls.Load() != 1 || fixture.modelCalls.Load() != 2 {
		t.Fatalf("run=%+v tools=%d models=%d err=%v", run, fixture.toolCalls.Load(), fixture.modelCalls.Load(), err)
	}
	loaded, err := fixture.api.sessions.Load(ctx, fixture.session.ID())
	if err != nil {
		t.Fatal(err)
	}
	results := 0
	for _, event := range loaded.Events() {
		if event.RunID == fixture.runID && event.Type == core.EvToolResult {
			results++
		}
	}
	var sidecars int
	if err := fixture.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM completed_tool_result_recovery_sidecars WHERE session_id = ? AND run_id = ?`, fixture.session.ID(), fixture.runID).Scan(&sidecars); err != nil || results != 1 || sidecars != wantSidecars {
		t.Fatalf("results=%d sidecars=%d err=%v", results, sidecars, err)
	}
}

func assertNativeQueuedRecoveryRejected(t *testing.T, fixture *nativeStrictWitnessFixture, wantResults int) {
	t.Helper()
	ctx := context.Background()
	run, err := fixture.api.runQueue.GetRun(ctx, fixture.runID)
	if err != nil || run.Status != string(core.RunFailed) || run.ErrorCode != "native_completed_tool_recovery_failed" || fixture.toolCalls.Load() != 1 || fixture.modelCalls.Load() != 1 {
		t.Fatalf("run=%+v tools=%d models=%d err=%v", run, fixture.toolCalls.Load(), fixture.modelCalls.Load(), err)
	}
	loaded, err := fixture.api.sessions.Load(ctx, fixture.session.ID())
	if err != nil {
		t.Fatal(err)
	}
	results := 0
	for _, event := range loaded.Events() {
		if event.RunID == fixture.runID && event.Type == core.EvToolResult {
			results++
		}
	}
	if results != wantResults {
		t.Fatalf("tool/results=%d want=%d", results, wantResults)
	}
}
