package storage

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

func approvalTestRequest(runID, callID string) core.ApprovalRequest {
	call := core.ToolCall{ID: callID, Name: "payment.release", Args: map[string]any{"amount": 10}}
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	return core.ApprovalRequest{
		RunID: runID, SessionID: "session-approval-store",
		Principal: core.Principal{TenantID: "acme", SubjectID: "alice", Scope: scope},
		ToolCall:  call,
		Manifest:  core.CapabilityManifest{ID: call.Name, Version: "1.0.0", Name: "Release", Kind: core.KindTool, RequiresApproval: true},
	}
}

func TestSQLApprovalDecisionAtomicallyResumesPausedRun(t *testing.T) {
	sessions := newTestSQLStore(t)
	queue, _ := NewSQLRunControlStore(sessions.db, SQLDialectSQLite)
	approvals, _ := NewSQLApprovalStore(sessions.db, SQLDialectSQLite)
	ctx := context.Background()
	runID := "run-approval-store"
	if err := queue.EnqueueRun(ctx, QueuedRun{
		RunRecord: RunRecord{RunID: runID, SessionID: "session-approval-store", TenantID: "acme", SubjectID: "alice"},
		Message:   "release payment", MaxAttempts: 3,
	}); err != nil {
		t.Fatal(err)
	}
	claimedRun, claimed, err := queue.ClaimRun(ctx, "worker-before-approval", time.Minute)
	if err != nil || !claimed || claimedRun.Attempt != 1 || claimedRun.Generation != 1 {
		t.Fatalf("initial claim wrong: %#v claimed=%t err=%v", claimedRun, claimed, err)
	}
	resolution, err := approvals.RequestApproval(ctx, approvalTestRequest(runID, "call-approval-store"))
	if err != nil || resolution.Decision != core.ApprovalPending {
		t.Fatalf("request approval failed: %#v err=%v", resolution, err)
	}
	paused, err := queue.PauseRunClaim(ctx, runID, "worker-before-approval", claimedRun.Generation)
	if err != nil || !paused {
		t.Fatalf("pause claim failed: paused=%t err=%v", paused, err)
	}
	record, err := queue.GetRun(ctx, runID)
	if err != nil || record.Status != RunStatusWaitingApproval {
		t.Fatalf("run did not enter waiting approval: %#v err=%v", record, err)
	}
	if _, claimed, err := queue.ClaimRun(ctx, "worker-too-early", time.Minute); err != nil || claimed {
		t.Fatalf("waiting run was claimable: claimed=%t err=%v", claimed, err)
	}
	decided, changed, err := approvals.DecideApproval(ctx, resolution.ApprovalID, core.ApprovalApproved, "admin@acme")
	if err != nil || !changed || decided.Status != core.ApprovalApproved {
		t.Fatalf("approval decision failed: %#v changed=%t err=%v", decided, changed, err)
	}
	resumed, claimed, err := queue.ClaimRun(ctx, "worker-after-approval", time.Minute)
	if err != nil || !claimed {
		t.Fatalf("approved run was not claimable: %#v claimed=%t err=%v", resumed, claimed, err)
	}
	if resumed.Attempt != 1 {
		t.Fatalf("approval pause consumed a failure attempt: attempt=%d", resumed.Attempt)
	}
	if resumed.Generation != 2 {
		t.Fatalf("approval resume reused its fence: generation=%d", resumed.Generation)
	}
	resolution, err = approvals.RequestApproval(ctx, approvalTestRequest(runID, "call-approval-store"))
	if err != nil || resolution.Decision != core.ApprovalApproved {
		t.Fatalf("approved decision was not durable: %#v err=%v", resolution, err)
	}
	if _, changed, err := approvals.DecideApproval(ctx, resolution.ApprovalID, core.ApprovalApproved, "admin@acme"); err != nil || changed {
		t.Fatalf("duplicate decision was not idempotent: changed=%t err=%v", changed, err)
	}
}

func TestSQLApprovalExpiryRequeuesRunAsDeniedDecision(t *testing.T) {
	sessions := newTestSQLStore(t)
	queue, _ := NewSQLRunControlStore(sessions.db, SQLDialectSQLite)
	approvals, _ := NewSQLApprovalStore(sessions.db, SQLDialectSQLite)
	approvals.TTL = time.Millisecond
	ctx := context.Background()
	runID := "run-approval-expiry"
	if err := queue.EnqueueRun(ctx, QueuedRun{
		RunRecord: RunRecord{RunID: runID, SessionID: "session-approval-store", TenantID: "acme", SubjectID: "alice"},
		Message:   "expire approval",
	}); err != nil {
		t.Fatal(err)
	}
	claimedRun, claimed, err := queue.ClaimRun(ctx, "worker-expiry", time.Minute)
	if err != nil || !claimed {
		t.Fatal("setup claim failed")
	}
	resolution, err := approvals.RequestApproval(ctx, approvalTestRequest(runID, "call-approval-expiry"))
	if err != nil {
		t.Fatal(err)
	}
	if paused, err := queue.PauseRunClaim(ctx, runID, "worker-expiry", claimedRun.Generation); err != nil || !paused {
		t.Fatalf("pause failed: %t %v", paused, err)
	}
	if _, err := sessions.db.ExecContext(ctx, "UPDATE approval_requests SET expires_at = 1 WHERE id = ?", resolution.ApprovalID); err != nil {
		t.Fatal(err)
	}
	expired, err := approvals.ExpireApprovals(ctx, time.Now().UTC())
	if err != nil || expired != 1 {
		t.Fatalf("expiry failed: expired=%d err=%v", expired, err)
	}
	resolution, err = approvals.RequestApproval(ctx, approvalTestRequest(runID, "call-approval-expiry"))
	if err != nil || resolution.Decision != core.ApprovalExpired {
		t.Fatalf("expired decision missing: %#v err=%v", resolution, err)
	}
	if _, claimed, err := queue.ClaimRun(ctx, "worker-expired-resume", time.Minute); err != nil || !claimed {
		t.Fatalf("expired approval did not resume denial path: claimed=%t err=%v", claimed, err)
	}
}

func TestSQLApprovalCancelRejectsInvalidActor(t *testing.T) {
	sessions := newTestSQLStore(t)
	approvals, _ := NewSQLApprovalStore(sessions.db, SQLDialectSQLite)
	ctx := context.Background()
	request := approvalTestRequest("run-approval-cancel-actor", "call-approval-cancel-actor")
	resolution, err := approvals.RequestApproval(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := approvals.CancelRunApprovals(ctx, request.RunID, "bad\x00actor"); err == nil {
		t.Fatal("cancel accepted a NUL actor")
	} else if !strings.Contains(err.Error(), "approval actor is invalid") {
		t.Fatalf("unexpected cancel error: %v", err)
	}
	record, err := approvals.GetApproval(ctx, resolution.ApprovalID)
	if err != nil || record.Status != core.ApprovalPending {
		t.Fatalf("invalid actor cancelled the approval: %#v err=%v", record, err)
	}
	if _, err := approvals.CancelRunApprovals(ctx, request.RunID, "   "); err == nil {
		t.Fatal("cancel accepted a blank actor")
	}
}

func TestSQLApprovalIdentityConflictAndTenantList(t *testing.T) {
	sessions := newTestSQLStore(t)
	approvals, _ := NewSQLApprovalStore(sessions.db, SQLDialectSQLite)
	ctx := context.Background()
	request := approvalTestRequest("run-approval-conflict", "call-approval-conflict")
	resolution, err := approvals.RequestApproval(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	request.ToolCall.Args["amount"] = 99
	if _, err := approvals.RequestApproval(ctx, request); err == nil {
		t.Fatal("approval call identity accepted different arguments")
	}
	request = approvalTestRequest("run-approval-conflict", "call-approval-conflict")
	request.Manifest.Version = "2.0.0"
	if _, err := approvals.RequestApproval(ctx, request); err == nil {
		t.Fatal("approval decision could be reused after manifest evolution")
	}
	listed, err := approvals.ListApprovals(ctx, ApprovalFilter{TenantID: "acme", Status: core.ApprovalPending})
	if err != nil || len(listed) != 1 || listed[0].ID != resolution.ApprovalID {
		t.Fatalf("tenant approval list wrong: %#v err=%v", listed, err)
	}
	other, err := approvals.ListApprovals(ctx, ApprovalFilter{TenantID: "other"})
	if err != nil || len(other) != 0 {
		t.Fatalf("approval tenant isolation failed: %#v err=%v", other, err)
	}
}

func TestApprovalRetentionDeletesOnlyDecidedRows(t *testing.T) {
	sessions := newTestSQLStore(t)
	approvals, _ := NewSQLApprovalStore(sessions.db, SQLDialectSQLite)
	ctx := context.Background()
	decidedRequest := approvalTestRequest("run-approval-retention-a", "call-approval-retention-a")
	pendingRequest := approvalTestRequest("run-approval-retention-b", "call-approval-retention-b")
	decided, err := approvals.RequestApproval(ctx, decidedRequest)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := approvals.RequestApproval(ctx, pendingRequest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sessions.db.ExecContext(ctx,
		"UPDATE approval_requests SET status = 'denied', decided_at = 1, decided_by = 'tester' WHERE id = ?", decided.ApprovalID,
	); err != nil {
		t.Fatal(err)
	}
	deleted, err := sessions.PruneApprovals(ctx, time.Now().UTC().Add(-time.Minute))
	if err != nil || deleted != 1 {
		t.Fatalf("approval retention wrong: deleted=%d err=%v", deleted, err)
	}
	if _, err := approvals.GetApproval(ctx, decided.ApprovalID); !errors.Is(err, ErrApprovalNotFound) {
		t.Fatalf("old decided approval was retained: %v", err)
	}
	if record, err := approvals.GetApproval(ctx, pending.ApprovalID); err != nil || record.Status != core.ApprovalPending {
		t.Fatalf("pending approval was pruned: %#v err=%v", record, err)
	}
}

func TestSQLApprovalStoreRejectsPendingOverflow(t *testing.T) {
	sessions := newTestSQLStore(t)
	approvals, err := NewSQLApprovalStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	approvals.maxPending = 2
	ctx := context.Background()
	first, err := approvals.RequestApproval(ctx, approvalTestRequest("run-pending-1", "call-pending-1"))
	if err != nil || first.Decision != core.ApprovalPending {
		t.Fatalf("first pending approval failed: %#v err=%v", first, err)
	}
	if _, err := approvals.RequestApproval(ctx, approvalTestRequest("run-pending-2", "call-pending-2")); err != nil {
		t.Fatal(err)
	}
	if _, err := approvals.RequestApproval(ctx, approvalTestRequest("run-pending-3", "call-pending-3")); err == nil {
		t.Fatal("pending approval overflow was accepted")
	} else if !strings.Contains(err.Error(), "pending approvals exceed maximum of 2") {
		t.Fatalf("unexpected overflow error: %v", err)
	}
	replayed, err := approvals.RequestApproval(ctx, approvalTestRequest("run-pending-1", "call-pending-1"))
	if err != nil || replayed.ApprovalID != first.ApprovalID || replayed.Decision != core.ApprovalPending {
		t.Fatalf("existing pending approval was blocked by the cap: %#v err=%v", replayed, err)
	}
}
