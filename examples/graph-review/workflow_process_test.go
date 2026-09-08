package graphreview

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/whhhh1500/auto-agent/pkg/adapter/sql/graphcheckpoint"
	"github.com/whhhh1500/auto-agent/pkg/adapter/sql/sqlkit"
	"github.com/whhhh1500/auto-agent/pkg/core"
	execgraph "github.com/whhhh1500/auto-agent/pkg/execution/graph"
	contract "github.com/whhhh1500/auto-agent/pkg/extensions/graph"
	"github.com/whhhh1500/auto-agent/pkg/storage"
	_ "modernc.org/sqlite"
)

const independentApprovalID = "local-process-review"

var independentPrincipal = core.Principal{
	TenantID:  "tenant-process-review",
	SubjectID: "author-process-review",
	Scope: core.MustScopePath(
		core.ScopeRef{Kind: core.ScopeTenant, ID: "tenant-process-review"},
		core.ScopeRef{Kind: core.ScopeUser, ID: "author-process-review"},
	),
	Grants: core.NewPermissionSet(core.PermRead, core.PermWrite),
}

var independentKey = contract.CheckpointKey{TenantID: "tenant-process-review", SessionID: "session-process-review", RunID: "run-process-review"}

type processReviews struct{ db *sql.DB }

func (r processReviews) RequestApproval(ctx context.Context, request core.ApprovalRequest) (core.ApprovalResolution, error) {
	if request.Principal.SubjectID != independentPrincipal.SubjectID || request.Principal.TenantID != independentPrincipal.TenantID ||
		request.SessionID != independentKey.SessionID || request.RunID != independentKey.RunID {
		return core.ApprovalResolution{}, errors.New("unexpected independent review request")
	}
	_, err := r.db.ExecContext(ctx, `INSERT INTO process_review_decisions
		(approval_id, tenant_id, session_id, run_id, subject_id, decision)
		VALUES (?, ?, ?, ?, ?, 'pending') ON CONFLICT(approval_id) DO NOTHING`,
		independentApprovalID, independentKey.TenantID, independentKey.SessionID, independentKey.RunID, independentPrincipal.SubjectID)
	if err != nil {
		return core.ApprovalResolution{}, err
	}
	return core.ApprovalResolution{ApprovalID: independentApprovalID, Decision: core.ApprovalPending}, nil
}

func (r processReviews) Resolve(ctx context.Context, principal core.Principal, key contract.CheckpointKey, approvalID string) (execgraph.ApprovalDecision, error) {
	var subject, decision, sourceSegment string
	var revision uint64
	err := r.db.QueryRowContext(ctx, `SELECT subject_id, decision, revision, source_segment_id FROM process_review_decisions
		WHERE approval_id=? AND tenant_id=? AND session_id=? AND run_id=?`, approvalID, key.TenantID, key.SessionID, key.RunID).Scan(&subject, &decision, &revision, &sourceSegment)
	if err != nil {
		return execgraph.ApprovalDecision{}, fmt.Errorf("load independent review decision: %w", err)
	}
	if subject != principal.SubjectID || decision != string(execgraph.ApprovalDecisionApproved) || revision == 0 || sourceSegment == "" {
		return execgraph.ApprovalDecision{}, errors.New("independent review decision not authorized")
	}
	return execgraph.ApprovalDecision{TenantID: key.TenantID, Key: key, ApprovalID: approvalID, Decision: execgraph.ApprovalDecisionApproved,
		Revision: revision, SourceSegmentID: sourceSegment, ActorID: "process-reviewer", AuthorizationBasis: "local-sql-review/v1"}, nil
}

func (r processReviews) Authorize(ctx context.Context, request execgraph.ApprovalAuthorizationRequest) error {
	decision, err := r.Resolve(ctx, independentPrincipal, request.Key, request.PendingApprovalID)
	if err != nil {
		return err
	}
	if decision != request.Decision || decision.Revision != request.CurrentRevision || request.SegmentID == "" || request.HostGeneration == 0 {
		return errors.New("independent review authorization evidence mismatch")
	}
	return nil
}

func TestSQLiteReviewSurvivesIndependentProcesses(t *testing.T) {
	dir := t.TempDir()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []string{"start", "approve", "resume"} {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		command := exec.CommandContext(ctx, binary, "-test.run=^TestIndependentReviewProcess$", "--", phase, dir)
		command.Env = append(os.Environ(), "HARNESS_GRAPH_PROCESS_HELPER=1")
		output, err := command.CombinedOutput()
		cancel()
		if err != nil {
			t.Fatalf("%s process: %v\n%s", phase, err, output)
		}
		t.Logf("%s process: %s", phase, strings.TrimSpace(string(output)))
	}
}

func TestIndependentReviewProcess(t *testing.T) {
	if os.Getenv("HARNESS_GRAPH_PROCESS_HELPER") != "1" {
		return
	}
	separator := -1
	for index, argument := range os.Args {
		if argument == "--" {
			separator = index
			break
		}
	}
	if separator < 0 || len(os.Args) != separator+3 {
		t.Fatalf("helper arguments = %q", os.Args)
	}
	runIndependentReviewPhase(t, os.Args[separator+1], os.Args[separator+2])
}

func runIndependentReviewPhase(t *testing.T, phase, dir string) {
	t.Helper()
	ctx := context.Background()
	graphDB, err := sql.Open("sqlite", filepath.Join(dir, "graph.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer graphDB.Close()
	reviewDB, err := sql.Open("sqlite", filepath.Join(dir, "reviews.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer reviewDB.Close()
	if _, err := reviewDB.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS process_review_decisions (
		approval_id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, session_id TEXT NOT NULL, run_id TEXT NOT NULL,
		subject_id TEXT NOT NULL, decision TEXT NOT NULL, revision INTEGER NOT NULL DEFAULT 0, source_segment_id TEXT NOT NULL DEFAULT ''
	); CREATE TABLE IF NOT EXISTS process_draft_calls (id INTEGER PRIMARY KEY CHECK(id=1), calls INTEGER NOT NULL);`); err != nil {
		t.Fatal(err)
	}
	if _, err := storage.OpenSQLSessionStore(ctx, graphDB, storage.SQLDialectSQLite); err != nil {
		t.Fatal(err)
	}
	workflow, err := New(graphDB, storage.SQLDialectSQLite, independentPrincipal, func(ctx context.Context, input string) (string, error) {
		if input != "Prepare release notes" {
			return "", errors.New("unexpected independent graph input")
		}
		if _, err := reviewDB.ExecContext(ctx, `INSERT INTO process_draft_calls(id, calls) VALUES(1, 1) ON CONFLICT(id) DO UPDATE SET calls=calls+1`); err != nil {
			return "", err
		}
		return "Reviewed release notes", nil
	}, processReviews{db: reviewDB})
	if err != nil {
		t.Fatal(err)
	}
	switch phase {
	case "start":
		result, err := workflow.Run(ctx, independentKey, "Prepare release notes")
		if !errors.Is(err, execgraph.ErrApprovalPending) || !result.Suspended || result.Checkpoint.CurrentNodeID != "review" || result.Checkpoint.PendingApprovalID != independentApprovalID || independentDraftCalls(t, ctx, reviewDB) != 1 {
			t.Fatalf("start result=%+v err=%v draft calls=%d", result, err, independentDraftCalls(t, ctx, reviewDB))
		}
		if _, err := reviewDB.ExecContext(ctx, `UPDATE process_review_decisions SET revision=?, source_segment_id=? WHERE approval_id=?`, result.Checkpoint.Revision, result.Checkpoint.SegmentID, independentApprovalID); err != nil {
			t.Fatal(err)
		}
	case "approve":
		result, err := reviewDB.ExecContext(ctx, `UPDATE process_review_decisions SET decision='approved' WHERE approval_id=? AND revision>0 AND source_segment_id<>''`, independentApprovalID)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := result.RowsAffected()
		if err != nil || rows != 1 {
			t.Fatalf("persisted approval update rows=%d err=%v", rows, err)
		}
	case "resume":
		result, err := workflow.Resume(ctx, independentKey)
		if err != nil || result.Checkpoint.Status != contract.CheckpointCompleted || string(result.Checkpoint.State["result"]) != `"Reviewed release notes"` || independentDraftCalls(t, ctx, reviewDB) != 1 {
			t.Fatalf("resume result=%+v err=%v draft calls=%d", result, err, independentDraftCalls(t, ctx, reviewDB))
		}
		store, err := graphcheckpoint.New(graphDB, sqlkit.SQLite)
		if err != nil {
			t.Fatal(err)
		}
		history, err := store.ListTransitions(ctx, independentKey, 0, 32)
		if err != nil {
			t.Fatal(err)
		}
		starts := map[string]int{}
		for _, transition := range history {
			if transition.OutcomeCode == "node_started" {
				starts[transition.CurrentNodeID]++
			}
		}
		if starts["draft"] != 1 || starts["review"] != 2 || starts["finalize"] != 1 {
			t.Fatalf("durable node starts = %v", starts)
		}
	default:
		t.Fatalf("unknown helper phase %q", phase)
	}
}

func independentDraftCalls(t *testing.T, ctx context.Context, db *sql.DB) int {
	t.Helper()
	var calls int
	if err := db.QueryRowContext(ctx, `SELECT calls FROM process_draft_calls WHERE id=1`).Scan(&calls); err != nil {
		t.Fatal(err)
	}
	return calls
}
