package graphreview

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/cc-auto-agent/harness-core/internal/testdb"
	"github.com/cc-auto-agent/harness-core/pkg/core"
	execgraph "github.com/cc-auto-agent/harness-core/pkg/execution/graph"
	contract "github.com/cc-auto-agent/harness-core/pkg/extensions/graph"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
	_ "modernc.org/sqlite"
)

// testReviews represents a separately hosted approval service. Its state stays
// outside the workflow instances being destroyed/reconstructed. SQL approval
// durability is covered by the server integration suite, not this test double.
type testReviews struct {
	request  core.ApprovalRequest
	decision execgraph.ApprovalDecision
}

func (r *testReviews) RequestApproval(_ context.Context, request core.ApprovalRequest) (core.ApprovalResolution, error) {
	r.request = request
	return core.ApprovalResolution{ApprovalID: "review-approval", Decision: core.ApprovalPending}, nil
}
func (r *testReviews) Resolve(_ context.Context, p core.Principal, key contract.CheckpointKey, id string) (execgraph.ApprovalDecision, error) {
	if r.request.Principal.SubjectID != p.SubjectID || r.decision.Key != key || r.decision.ApprovalID != id || r.decision.Validate() != nil {
		return execgraph.ApprovalDecision{}, errors.New("review decision unavailable")
	}
	return r.decision, nil
}
func (r *testReviews) Authorize(_ context.Context, request execgraph.ApprovalAuthorizationRequest) error {
	if request.Decision != r.decision || request.CurrentRevision != r.decision.Revision || request.PendingApprovalID != r.decision.ApprovalID {
		return errors.New("review decision mismatch")
	}
	return nil
}

func TestSQLiteReviewSurvivesReconstruction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "review.db")
	open := func() *sql.DB {
		db, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		return db
	}
	verifyReview(t, open, storage.SQLDialectSQLite)
}

func TestPostgresReviewSurvivesReconstruction(t *testing.T) {
	verifyReview(t, testdb.Postgres(t), storage.SQLDialectPostgres)
}

func verifyReview(t *testing.T, open func() *sql.DB, dialect storage.SQLDialect) {
	t.Helper()
	ctx := context.Background()
	principal := core.Principal{TenantID: "tenant-review", SubjectID: "author", Scope: core.MustScopePath(core.ScopeRef{Kind: core.ScopeTenant, ID: "tenant-review"}, core.ScopeRef{Kind: core.ScopeUser, ID: "author"}), Grants: core.NewPermissionSet(core.PermRead, core.PermWrite)}
	key := contract.CheckpointKey{TenantID: principal.TenantID, SessionID: "session-review", RunID: "run-review"}
	reviews := &testReviews{}
	drafts := 0
	draft := func(context.Context, string) (string, error) { drafts++; return "Reviewed release notes", nil }
	firstDB := open()
	if _, err := storage.OpenSQLSessionStore(ctx, firstDB, dialect); err != nil {
		t.Fatal(err)
	}
	first, err := New(firstDB, dialect, principal, draft, reviews)
	if err != nil {
		t.Fatal(err)
	}
	paused, err := first.Run(ctx, key, "Prepare release notes")
	if !errors.Is(err, execgraph.ErrApprovalPending) || paused.Checkpoint.CurrentNodeID != "review" || drafts != 1 {
		t.Fatalf("pause=%s node=%s drafts=%d err=%v", paused.Checkpoint.Status, paused.Checkpoint.CurrentNodeID, drafts, err)
	}
	attempt := paused.Checkpoint.AttemptID
	if reviews.request.ToolCall.ID != attempt {
		t.Fatal("review request lost stable attempt")
	}
	if err := firstDB.Close(); err != nil {
		t.Fatal(err)
	}
	secondDB := open()
	second, err := New(secondDB, dialect, principal, draft, reviews)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.Resume(ctx, key); err == nil {
		t.Fatal("resume without an authorized decision succeeded")
	}
	reviews.decision = execgraph.ApprovalDecision{TenantID: principal.TenantID, Key: key, ApprovalID: paused.Checkpoint.PendingApprovalID, Decision: execgraph.ApprovalDecisionApproved, Revision: paused.Checkpoint.Revision, SourceSegmentID: paused.Checkpoint.SegmentID, ActorID: "reviewer", AuthorizationBasis: "test-review-service/v1"}
	resumed, err := second.Resume(ctx, key)
	if err != nil || resumed.Checkpoint.Status != contract.CheckpointCompleted || drafts != 1 || string(resumed.Checkpoint.State["result"]) != `"Reviewed release notes"` {
		t.Fatalf("resume=%s drafts=%d err=%v", resumed.Checkpoint.Status, drafts, err)
	}
	history, err := second.store.ListTransitions(ctx, key, 0, 32)
	if err != nil {
		t.Fatal(err)
	}
	starts := map[string]int{}
	for _, transition := range history {
		if transition.OutcomeCode == "node_started" {
			starts[transition.CurrentNodeID]++
			if transition.CurrentNodeID == "review" && transition.AttemptID != attempt {
				t.Fatal("resume changed review attempt")
			}
		}
	}
	if starts["draft"] != 1 || starts["review"] != 2 || starts["finalize"] != 1 {
		t.Fatalf("node starts=%v", starts)
	}
	other := principal
	other.SubjectID = "different-author"
	unauthorized, err := New(secondDB, dialect, other, draft, reviews)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := unauthorized.Run(ctx, key, "replace"); err == nil {
		t.Fatal("different author accessed existing workflow")
	}
	// A panic represents an ambiguous node outcome. Reconstructing the engine
	// must record unknown without re-invoking that draft callback.
	crashKey := key
	crashKey.RunID = "run-interrupted"
	crashCalls := 0
	crashing, err := New(secondDB, dialect, principal, func(context.Context, string) (string, error) { crashCalls++; panic("test node interruption") }, reviews)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := crashing.Run(ctx, crashKey, "interrupted draft"); err == nil {
		t.Fatal("node panic was accepted")
	}
	recovered, err := second.Run(ctx, crashKey, "interrupted draft")
	if !errors.Is(err, execgraph.ErrUnknownOutcome) || recovered.Checkpoint.Status != contract.CheckpointUnknown || crashCalls != 1 || drafts != 1 {
		t.Fatalf("recovery=%s calls=%d drafts=%d err=%v", recovered.Checkpoint.Status, crashCalls, drafts, err)
	}
	t.Logf("three-node SQL graph: completed; drafts=1; stable review attempt; transitions=%d; ambiguous outcome refused", len(history))
}
