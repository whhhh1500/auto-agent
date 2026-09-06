package storage

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cc-auto-agent/harness-core/pkg/control"
	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/evaluation"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

func newPostgresTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("HARNESS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("HARNESS_TEST_PG_DSN is not configured")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	adminConfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	adminDB := stdlib.OpenDB(*adminConfig)
	if err := adminDB.PingContext(ctx); err != nil {
		_ = adminDB.Close()
		t.Fatalf("connect postgres: %v", err)
	}
	schema := fmt.Sprintf("harness_test_%d", time.Now().UnixNano())
	if _, err := adminDB.ExecContext(ctx, "CREATE SCHEMA "+schema); err != nil {
		_ = adminDB.Close()
		t.Fatal(err)
	}
	var db *sql.DB
	t.Cleanup(func() {
		if db != nil {
			if err := db.Close(); err != nil {
				t.Errorf("failed to close test DB connection: %v", err)
			}
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if _, err := adminDB.ExecContext(cleanupCtx, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Errorf("failed to drop postgres test schema %q: %v", schema, err)
		}
		if err := adminDB.Close(); err != nil {
			t.Errorf("failed to close admin DB connection: %v", err)
		}
	})
	testConfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	testConfig.RuntimeParams["search_path"] = schema
	db = stdlib.OpenDB(*testConfig)
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)
	if err := db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestPostgresDurableRuntimeStores(t *testing.T) {
	db := newPostgresTestDB(t)
	ctx := context.Background()
	sessions, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product, _ := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "product"})
	tenant, _ := product.Child(core.ScopeRef{Kind: core.ScopeTenant, ID: "acme"})
	user, _ := tenant.Child(core.ScopeRef{Kind: core.ScopeUser, ID: "alice"})
	principal := core.Principal{
		SubjectID: "alice", TenantID: "acme", Scope: user,
		Grants: core.NewPermissionSet(core.PermRead, core.PermWrite),
	}
	sessionScope, _ := user.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "session-postgres"})
	session, err := core.NewSession(core.SessionOptions{
		ID: "session-postgres", ProfileID: "postgres.agent", Principal: principal, Scope: sessionScope,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append("run-postgres-session", core.EvRunStart, core.RunStartData{}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append("run-postgres-session", core.EvRunEnd, core.RunEndData{Status: core.RunCompleted}); err != nil {
		t.Fatal(err)
	}
	if err := sessions.Create(ctx, session); err != nil {
		t.Fatal(err)
	}
	loaded, err := sessions.Load(ctx, session.ID())
	if err != nil || loaded.Version() != session.Version() {
		t.Fatalf("postgres session round trip failed: version=%d err=%v", loaded.Version(), err)
	}

	queue, err := NewSQLRunControlStore(db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	message := "postgres durable run"
	run, created, err := queue.EnqueueRunOnce(ctx, QueuedRun{
		RunRecord: RunRecord{RunID: "run-postgres-queue", SessionID: session.ID(), TenantID: "acme", SubjectID: "alice"},
		Message:   message, MaxAttempts: 3,
		TraceContext: core.TelemetryTraceContext{
			TraceParent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
			TraceState:  "vendor=value",
		},
	}, "postgres-client-key", RunMessageDigest(message))
	if err != nil || !created || run.Status != RunStatusQueued {
		t.Fatalf("postgres enqueue failed: %#v created=%t err=%v", run, created, err)
	}
	replay, created, err := queue.EnqueueRunOnce(ctx, QueuedRun{
		RunRecord: RunRecord{RunID: "run-postgres-unused", SessionID: session.ID(), TenantID: "acme", SubjectID: "alice"},
		Message:   message,
	}, "postgres-client-key", RunMessageDigest(message))
	if err != nil || created || replay.RunID != run.RunID {
		t.Fatalf("postgres submission replay failed: %#v created=%t err=%v", replay, created, err)
	}
	claimed, ok, err := queue.ClaimRun(ctx, "postgres-worker", time.Minute)
	if err != nil || !ok || claimed.RunID != run.RunID || claimed.Generation != 1 ||
		claimed.TraceContext.TraceParent == "" || claimed.TraceContext.TraceState != "vendor=value" {
		t.Fatalf("postgres SKIP LOCKED claim failed: %#v ok=%t err=%v", claimed, ok, err)
	}
	if renewed, err := queue.RenewRunClaim(ctx, claimed.RunID, "postgres-worker", claimed.Generation, time.Minute); err != nil || !renewed {
		t.Fatalf("postgres claim renewal failed: renewed=%t err=%v", renewed, err)
	}
	if finished, err := queue.FinishRunClaim(ctx, claimed.RunID, "postgres-worker", claimed.Generation, core.RunCompleted, ""); err != nil || !finished {
		t.Fatalf("postgres claim completion failed: finished=%t err=%v", finished, err)
	}
	recoveryRun := QueuedRun{
		RunRecord: RunRecord{RunID: "run-postgres-recovery", SessionID: "session-postgres-recovery", TenantID: "acme", SubjectID: "alice"},
		Message:   "recover postgres", MaxAttempts: 2,
	}
	if err := queue.EnqueueRun(ctx, recoveryRun); err != nil {
		t.Fatal(err)
	}
	expiredClaim, ok, err := queue.ClaimRun(ctx, "postgres-dead-worker", time.Minute)
	if err != nil || !ok || expiredClaim.RunID != recoveryRun.RunID {
		t.Fatalf("postgres recovery claim failed: %#v ok=%t err=%v", expiredClaim, ok, err)
	}
	if _, err := db.ExecContext(ctx, "UPDATE run_queue SET lease_expires_at = 1 WHERE run_id = $1", expiredClaim.RunID); err != nil {
		t.Fatal(err)
	}
	if requeued, failed, err := queue.RecoverExpiredRunClaims(ctx, time.Now().UTC()); err != nil || requeued != 1 || failed != 0 {
		t.Fatalf("postgres SKIP LOCKED recovery failed: requeued=%d failed=%d err=%v", requeued, failed, err)
	}
	if retried, ok, err := queue.ClaimRun(ctx, "postgres-recovery-worker", time.Minute); err != nil || !ok || retried.RunID != recoveryRun.RunID || retried.Generation != 2 {
		t.Fatalf("postgres recovered claim wrong: %#v ok=%t err=%v", retried, ok, err)
	}

	journal, err := NewSQLToolInvocationJournal(db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	invocation, err := core.NewToolInvocation(core.RunInfo{
		RunID: "run-postgres-tool", SessionID: session.ID(), Principal: principal,
	}, core.ToolCall{ID: "call-postgres-tool", Name: "postgres.tool", Args: map[string]any{"value": 1}}, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, decision, err := journal.BeginToolInvocation(ctx, invocation); err != nil || decision != core.ToolInvocationExecuteNew {
		t.Fatalf("postgres tool begin failed: decision=%q err=%v", decision, err)
	}
	if _, err := journal.CompleteToolInvocation(ctx, invocation, core.CapabilityResult{Content: "done", OK: true}); err != nil {
		t.Fatal(err)
	}
	if record, decision, err := journal.BeginToolInvocation(ctx, invocation); err != nil || decision != core.ToolInvocationReplay || record.Result == nil || record.Result.Content != "done" {
		t.Fatalf("postgres tool replay failed: %#v decision=%q err=%v", record, decision, err)
	}

	evaluations, err := NewSQLEvaluationStore(db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	dataset, created, err := evaluations.PutDataset(ctx, sqlEvaluationDataset())
	if err != nil || !created {
		t.Fatalf("postgres evaluation dataset failed: %#v created=%t err=%v", dataset, created, err)
	}
	evaluationRun := sqlEvaluationRun(dataset)
	evaluationRun.ID = "eval_postgres_run"
	evaluationRun.CompositionMetadata = map[string]string{"harness.assignment.id": "postgres-assignment"}
	evaluationRun.AssignmentRevision, err = core.CompositionMetadataRevision(evaluationRun.CompositionMetadata)
	if err != nil {
		t.Fatal(err)
	}
	if err := evaluations.CreateRun(ctx, evaluationRun); err != nil {
		t.Fatal(err)
	}
	caseResult := sqlEvaluationCaseResult()
	caseResult.SessionID = "evalsess_postgres"
	caseResult.AgentRunID = "evalcase_postgres"
	caseResult.Artifacts.CompositionRevision = strings.Repeat("a", 64)
	caseResult.Artifacts.AssignmentRevision = evaluationRun.AssignmentRevision
	if err := evaluations.RecordCaseResult(ctx, evaluationRun.ID, caseResult); err != nil {
		t.Fatal(err)
	}
	evaluationRun.Cases = []evaluation.CaseResult{caseResult}
	evaluationRun.Status, evaluationRun.Score, evaluationRun.Passed, evaluationRun.PassedCases = evaluation.RunCompleted, 1, true, 1
	evaluationRun.CompletedAt = time.Now().UTC()
	if err := evaluations.FinishRun(ctx, evaluationRun); err != nil {
		t.Fatal(err)
	}
	loadedEvaluation, err := evaluations.GetRun(ctx, evaluationRun.ID)
	if err != nil || !loadedEvaluation.Passed || len(loadedEvaluation.Cases) != 1 || loadedEvaluation.Cases[0].Artifacts.ProfileSnapshotID == "" {
		t.Fatalf("postgres evaluation result failed: %#v err=%v", loadedEvaluation, err)
	}
	if loadedEvaluation.AssignmentRevision != evaluationRun.AssignmentRevision ||
		loadedEvaluation.CompositionMetadata["harness.assignment.id"] != "postgres-assignment" ||
		loadedEvaluation.Cases[0].Artifacts.AssignmentRevision != evaluationRun.AssignmentRevision {
		t.Fatalf("postgres composition evidence round trip failed: %#v", loadedEvaluation)
	}
	matched, err := evaluations.QueryRuns(ctx, evaluation.RunQuery{
		DatasetID: dataset.ID, TenantID: "acme", CompositionRevision: strings.Repeat("a", 64),
		AssignmentRevision: evaluationRun.AssignmentRevision, Limit: 10,
	})
	if err != nil || len(matched) != 1 || matched[0].ID != evaluationRun.ID {
		t.Fatalf("postgres revision query failed: %#v err=%v", matched, err)
	}
}

func TestPostgresApprovalPauseResume(t *testing.T) {
	db := newPostgresTestDB(t)
	ctx := context.Background()
	if _, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres); err != nil {
		t.Fatal(err)
	}
	queue, _ := NewSQLRunControlStore(db, SQLDialectPostgres)
	approvals, _ := NewSQLApprovalStore(db, SQLDialectPostgres)
	runID := "run-postgres-approval"
	if err := queue.EnqueueRun(ctx, QueuedRun{
		RunRecord: RunRecord{RunID: runID, SessionID: "session-postgres-approval", TenantID: "acme", SubjectID: "alice"},
		Message:   "approve postgres",
	}); err != nil {
		t.Fatal(err)
	}
	claimed, ok, err := queue.ClaimRun(ctx, "postgres-approval-worker", time.Minute)
	if err != nil || !ok {
		t.Fatalf("claim approval run: %#v ok=%t err=%v", claimed, ok, err)
	}
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	call := core.ToolCall{ID: "call-postgres-approval", Name: "postgres.approval", Args: map[string]any{"x": 1}}
	resolution, err := approvals.RequestApproval(ctx, core.ApprovalRequest{
		RunID: runID, SessionID: claimed.SessionID,
		Principal: core.Principal{TenantID: "acme", SubjectID: "alice", Scope: scope},
		ToolCall:  call,
		Manifest:  core.CapabilityManifest{ID: call.Name, Version: "1.0.0", Name: "Approval", Kind: core.KindTool, RequiresApproval: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if paused, err := queue.PauseRunClaim(ctx, runID, "postgres-approval-worker", claimed.Generation); err != nil || !paused {
		t.Fatalf("postgres pause failed: paused=%t err=%v", paused, err)
	}
	queueMetrics, err := queue.RunQueueMetrics(ctx)
	if err != nil || queueMetrics.WaitingApproval != 1 || queueMetrics.Running != 0 {
		t.Fatalf("postgres queue metrics wrong: %#v err=%v", queueMetrics, err)
	}
	approvalMetrics, err := approvals.ApprovalMetrics(ctx)
	if err != nil || approvalMetrics.Pending != 1 || approvalMetrics.OldestRequestedAt.IsZero() {
		t.Fatalf("postgres approval metrics wrong: %#v err=%v", approvalMetrics, err)
	}
	if _, changed, err := approvals.DecideApproval(ctx, resolution.ApprovalID, core.ApprovalApproved, "postgres-admin"); err != nil || !changed {
		t.Fatalf("postgres approval failed: changed=%t err=%v", changed, err)
	}
	resumed, ok, err := queue.ClaimRun(ctx, "postgres-resume-worker", time.Minute)
	if err != nil || !ok || resumed.Attempt != 1 || resumed.Generation != 2 {
		t.Fatalf("postgres approval resume wrong: %#v ok=%t err=%v", resumed, ok, err)
	}
}

func TestPostgresReleaseAndCanaryLifecycle(t *testing.T) {
	db := newPostgresTestDB(t)
	ctx := context.Background()
	sessions, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product, _ := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "product"})
	principal := core.Principal{TenantID: "acme", SubjectID: "alice", Scope: product, Grants: core.NewPermissionSet(core.PermRead)}
	newProfiles := func() *core.AgentProfileRegistry {
		profiles := core.NewAgentProfileRegistry()
		name := "Stable"
		model := core.ModelSelection{Provider: "postgres-test", Model: "stable"}
		if err := profiles.Bind(core.AgentProfileLayer{
			Scope: product, ProfileID: "postgres.agent", Name: &name, Model: &model,
		}); err != nil {
			t.Fatal(err)
		}
		return profiles
	}

	firstProfiles := newProfiles()
	firstReleases, _ := control.NewReleaseManager(firstProfiles)
	firstReleases.Journal = sessions
	if err := firstReleases.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	releasedName := "Released"
	firstRelease, err := firstReleases.Publish(ctx, product, core.AgentProfileLayer{
		ProfileID: "postgres.agent", Name: &releasedName,
	})
	if err != nil || firstRelease.Version != 1 || firstRelease.Revision == "" {
		t.Fatalf("postgres release publish failed: %#v err=%v", firstRelease, err)
	}

	restoredProfiles := newProfiles()
	restoredReleases, _ := control.NewReleaseManager(restoredProfiles)
	restoredReleases.Journal = sessions
	if err := restoredReleases.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	resolved, err := restoredProfiles.Resolve(principal, product, "postgres.agent")
	if err != nil || resolved.Name != releasedName {
		t.Fatalf("postgres release restore failed: %#v err=%v", resolved, err)
	}
	if rolledBack, err := restoredReleases.Rollback(ctx, "postgres.agent", 0); err != nil || len(rolledBack) != 1 {
		t.Fatalf("postgres release rollback failed: %#v err=%v", rolledBack, err)
	}
	resolved, err = restoredProfiles.Resolve(principal, product, "postgres.agent")
	if err != nil || resolved.Name != "Stable" {
		t.Fatalf("postgres rollback did not restore base: %#v err=%v", resolved, err)
	}

	canaryStore, err := NewSQLCanaryStore(db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	canaries, err := control.NewCanaryManager(restoredReleases, canaryStore)
	if err != nil {
		t.Fatal(err)
	}
	if err := canaries.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	candidateName := "Canary Candidate"
	candidateModel := core.ModelSelection{Provider: "postgres-test", Model: "candidate"}
	layer := core.AgentProfileLayer{
		Scope: product, ProfileID: "postgres.agent", Name: &candidateName, Model: &candidateModel,
	}
	revision, err := core.ProfileLayerRevision(layer)
	if err != nil {
		t.Fatal(err)
	}
	staged, err := canaries.Stage(ctx, control.CanaryRecord{
		ID: "canary-postgres-lifecycle", ProfileID: layer.ProfileID, Scope: product, Layer: &layer,
		Revision: revision, BasisPoints: 10000, CandidateEvaluationRunID: "evaluation-postgres-candidate",
		Gate: evaluation.GateResult{Passed: true},
	})
	if err != nil || staged.Status != control.CanaryActive {
		t.Fatalf("postgres canary stage failed: %#v err=%v", staged, err)
	}
	if paused, err := canaries.Pause(ctx, staged.ID); err != nil || paused.Status != control.CanaryPaused {
		t.Fatalf("postgres canary pause failed: %#v err=%v", paused, err)
	}
	if resumed, err := canaries.Resume(ctx, staged.ID); err != nil || resumed.Status != control.CanaryActive {
		t.Fatalf("postgres canary resume failed: %#v err=%v", resumed, err)
	}
	promoted, err := canaries.Promote(ctx, staged.ID)
	if err != nil || promoted.Status != control.CanaryPromoted || promoted.ReleaseVersion != 2 {
		t.Fatalf("postgres canary promote failed: %#v err=%v", promoted, err)
	}
	replayed, err := canaries.Promote(ctx, staged.ID)
	if err != nil || replayed.ReleaseVersion != promoted.ReleaseVersion {
		t.Fatalf("postgres canary promotion replay failed: %#v err=%v", replayed, err)
	}
	operationRelease, found, err := sessions.FindReleaseByOperation(ctx, staged.ID)
	if err != nil || !found || operationRelease.Version != promoted.ReleaseVersion || operationRelease.OperationID != staged.ID {
		t.Fatalf("postgres release operation lookup failed: %#v found=%t err=%v", operationRelease, found, err)
	}

	finalProfiles := newProfiles()
	finalReleases, _ := control.NewReleaseManager(finalProfiles)
	finalReleases.Journal = sessions
	if err := finalReleases.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	finalCanaries, err := control.NewCanaryManager(finalReleases, canaryStore)
	if err != nil {
		t.Fatal(err)
	}
	if err := finalCanaries.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	resolved, err = finalProfiles.Resolve(principal, product, "postgres.agent")
	if err != nil || resolved.Name != candidateName || resolved.Model.Model != "candidate" {
		t.Fatalf("postgres promoted release restore failed: %#v err=%v", resolved, err)
	}
	if history := finalReleases.History(ctx, "postgres.agent"); len(history) != 2 {
		t.Fatalf("postgres promotion duplicated release history: %#v", history)
	}
}

func TestPostgresEvidenceCursorPagination(t *testing.T) {
	db := newPostgresTestDB(t)
	ctx := context.Background()
	store, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	composition := strings.Repeat("e", 64)
	assignment := strings.Repeat("f", 64)
	now := time.Now().UTC().UnixMilli()
	for index := 0; index < 3; index++ {
		if _, err := db.ExecContext(ctx, `INSERT INTO run_evidence
			(session_id, run_id, segment_seq, kind, tenant_id, subject_id, profile_id,
			 composition_revision, assignment_revision, created_at)
			VALUES ($1, $2, 0, 'run', 'acme', 'alice', 'postgres.cursor', $3, $4, $5)`,
			fmt.Sprintf("session-postgres-cursor-%d", index), fmt.Sprintf("run-postgres-cursor-%d", index),
			composition, assignment, now-int64(index+1)*1000); err != nil {
			t.Fatal(err)
		}
	}
	query := EvidenceQuery{
		CompositionRevision: composition, AssignmentRevision: assignment,
		TenantID: "acme", Limit: 1,
	}
	seen := map[string]bool{}
	for {
		page, err := store.QueryEvidencePage(ctx, query)
		if err != nil || len(page.Records) != 1 {
			t.Fatalf("postgres evidence page wrong: %#v err=%v", page, err)
		}
		if seen[page.Records[0].ID] {
			t.Fatalf("postgres cursor repeated %s", page.Records[0].ID)
		}
		seen[page.Records[0].ID] = true
		if page.NextCursor == "" {
			break
		}
		query.Cursor = page.NextCursor
	}
	if len(seen) != 3 {
		t.Fatalf("postgres cursor returned %d records: %#v", len(seen), seen)
	}
}

func TestPostgresRunEvidenceStatsUseLatestSegmentAndVariant(t *testing.T) {
	db := newPostgresTestDB(t)
	ctx := context.Background()
	store, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().UnixMilli()
	insert := func(sessionID, runID string, seq int64, variant AssignmentVariant, status string, created int64) {
		t.Helper()
		if _, err := db.ExecContext(ctx, `INSERT INTO run_evidence
			(session_id, run_id, segment_seq, kind, tenant_id, subject_id, profile_id,
			 composition_revision, assignment_revision, assignment_variant, status, created_at)
			VALUES ($1, $2, $3, 'run', 'acme', 'alice', 'postgres.stats', '', '', $4, $5, $6)`,
			sessionID, runID, seq, string(variant), status, created); err != nil {
			t.Fatal(err)
		}
	}
	insert("postgres-stats-session-1", "postgres-stats-run-1", 0, AssignmentCandidate, "running", now-5000)
	insert("postgres-stats-session-1", "postgres-stats-run-1", 2, AssignmentLive, "completed", now-4000)
	insert("postgres-stats-session-2", "postgres-stats-run-2", 0, AssignmentCandidate, "failed", now-3000)
	insert("postgres-stats-session-3", "postgres-stats-run-3", 0, AssignmentUnassigned, "running", now-2000)
	stats, err := store.RunEvidenceStats(ctx, EvidenceQuery{TenantID: "acme", ProfileID: "postgres.stats"})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Total != 3 || stats.Terminal != 2 || stats.Completed != 1 || stats.Failed != 1 || stats.CompletionRate != 0.5 {
		t.Fatalf("postgres run stats totals wrong: %#v", stats)
	}
	if stats.ByStatus["completed"] != 1 || stats.ByStatus["failed"] != 1 || stats.ByStatus["running"] != 1 ||
		stats.ByVariant[string(AssignmentCandidate)].Total != 1 || stats.ByVariant[string(AssignmentCandidate)].Failed != 1 ||
		stats.ByVariant[string(AssignmentLive)].Total != 1 || stats.ByVariant[string(AssignmentLive)].Completed != 1 ||
		stats.ByVariant[string(AssignmentUnassigned)].Total != 1 {
		t.Fatalf("postgres run stats distribution wrong: %#v", stats)
	}
	completed, err := store.RunEvidenceStats(ctx, EvidenceQuery{TenantID: "acme", Statuses: []string{"completed"}})
	if err != nil || completed.Total != 1 || completed.ByVariant[string(AssignmentLive)].Total != 1 || completed.CompletionRate != 1 {
		t.Fatalf("postgres filtered run stats wrong: %#v err=%v", completed, err)
	}
}

func TestPostgresSchemaMigrationAndFutureRefusal(t *testing.T) {
	t.Run("v9_to_current", func(t *testing.T) {
		db := newPostgresTestDB(t)
		ctx := context.Background()
		if _, err := db.ExecContext(ctx, sqlSchemaV9); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, sqlInsertMetaRow.bind(SQLDialectPostgres), "9"); err != nil {
			t.Fatal(err)
		}
		if _, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres); err != nil {
			t.Fatal(err)
		}
		var generation string
		if err := db.QueryRowContext(ctx, `SELECT column_name FROM information_schema.columns
			WHERE table_schema = current_schema() AND table_name = 'run_queue' AND column_name = 'generation'`).Scan(&generation); err != nil || generation != "generation" {
			t.Fatalf("generation migration missing: %q err=%v", generation, err)
		}
		for _, column := range []string{"trace_parent", "trace_state"} {
			var found string
			if err := db.QueryRowContext(ctx, `SELECT column_name FROM information_schema.columns
				WHERE table_schema = current_schema() AND table_name = 'run_queue' AND column_name = $1`, column).Scan(&found); err != nil || found != column {
				t.Fatalf("trace column %s missing: found=%q err=%v", column, found, err)
			}
		}
		for _, table := range []string{
			"approval_requests", "run_submissions", "evaluation_datasets",
			"evaluation_runs", "evaluation_case_results", "profile_canaries", "run_evidence",
		} {
			var name string
			if err := db.QueryRowContext(ctx, "SELECT to_regclass($1)", table).Scan(&name); err != nil || !strings.HasSuffix(name, table) {
				t.Fatalf("table %s missing after migration: name=%q err=%v", table, name, err)
			}
		}
		var evidenceStatus string
		if err := db.QueryRowContext(ctx, `SELECT column_name FROM information_schema.columns
			WHERE table_schema = current_schema() AND table_name = 'run_evidence' AND column_name = 'status'`).Scan(&evidenceStatus); err != nil || evidenceStatus != "status" {
			t.Fatalf("postgres v22 run evidence status missing: found=%q err=%v", evidenceStatus, err)
		}
		var evidenceVariant string
		if err := db.QueryRowContext(ctx, `SELECT column_name FROM information_schema.columns
			WHERE table_schema = current_schema() AND table_name = 'run_evidence' AND column_name = 'assignment_variant'`).Scan(&evidenceVariant); err != nil || evidenceVariant != "assignment_variant" {
			t.Fatalf("postgres v23 run evidence assignment variant missing: found=%q err=%v", evidenceVariant, err)
		}
		var evidenceVariantIndex string
		if err := db.QueryRowContext(ctx, `SELECT indexname FROM pg_indexes
			WHERE schemaname = current_schema() AND tablename = 'run_evidence' AND indexname = 'run_evidence_variant_status'`).Scan(&evidenceVariantIndex); err != nil || evidenceVariantIndex != "run_evidence_variant_status" {
			t.Fatalf("postgres v23 run evidence variant/status index missing: found=%q err=%v", evidenceVariantIndex, err)
		}
		for table, columns := range map[string][]string{
			"profile_releases":        {"layer_json", "revision", "operation_id"},
			"profile_canaries":        {"release_version", "base_release_revision"},
			"evaluation_runs":         {"composition_metadata_json"},
			"evaluation_case_results": {"composition_revision", "assignment_revision"},
		} {
			for _, column := range columns {
				var found string
				if err := db.QueryRowContext(ctx, `SELECT column_name FROM information_schema.columns
					WHERE table_schema = current_schema() AND table_name = $1 AND column_name = $2`, table, column).Scan(&found); err != nil || found != column {
					t.Fatalf("column %s.%s missing after migration: found=%q err=%v", table, column, found, err)
				}
			}
		}
		var controlRevision string
		if err := db.QueryRowContext(ctx, "SELECT value FROM store_meta WHERE key='control_revision'").Scan(&controlRevision); err != nil || controlRevision != "0" {
			t.Fatalf("postgres v18 control revision row missing: value=%q err=%v", controlRevision, err)
		}
	})
	t.Run("v22_to_v23_run_evidence_variant_backfill", func(t *testing.T) {
		db := newPostgresTestDB(t)
		ctx := context.Background()
		store, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres)
		if err != nil {
			t.Fatal(err)
		}
		want := seedV22RunEvidenceVariantSessions(t, store)
		if _, err := db.ExecContext(ctx, "DROP INDEX run_evidence_variant_status"); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, "ALTER TABLE run_evidence DROP COLUMN assignment_variant"); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, sqlUpdateMetaRow.bind(SQLDialectPostgres), "22"); err != nil {
			t.Fatal(err)
		}
		reopened, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres)
		if err != nil {
			t.Fatalf("postgres v22 to v23 run evidence migration failed: %v", err)
		}
		var columnCount int
		if err := reopened.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.columns
			WHERE table_schema = current_schema() AND table_name = 'run_evidence' AND column_name = 'assignment_variant'`).Scan(&columnCount); err != nil || columnCount != 1 {
			t.Fatalf("postgres v23 assignment variant column missing after migration: count=%d err=%v", columnCount, err)
		}
		var indexCount int
		if err := reopened.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pg_indexes
			WHERE schemaname = current_schema() AND tablename = 'run_evidence' AND indexname = 'run_evidence_variant_status'`).Scan(&indexCount); err != nil || indexCount != 1 {
			t.Fatalf("postgres v23 variant/status index missing after migration: count=%d err=%v", indexCount, err)
		}
		rows, err := reopened.db.QueryContext(ctx, "SELECT run_id, assignment_variant FROM run_evidence ORDER BY run_id")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		got := map[string]AssignmentVariant{}
		for rows.Next() {
			var runID string
			var variant AssignmentVariant
			if err := rows.Scan(&runID, &variant); err != nil {
				t.Fatal(err)
			}
			got[runID] = variant
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if len(got) != len(want) {
			t.Fatalf("postgres v23 variant backfill returned %d rows, want %d: %#v", len(got), len(want), got)
		}
		for runID, variant := range want {
			if got[runID] != variant {
				t.Fatalf("postgres v23 variant backfill %s=%q, want %q; all=%#v", runID, got[runID], variant, got)
			}
		}
		var version string
		if err := reopened.db.QueryRowContext(ctx, sqlSelectMetaRow.bind(SQLDialectPostgres)).Scan(&version); err != nil || version != strconv.Itoa(SQLSchemaVersion) {
			t.Fatalf("postgres current schema version was not recorded after v23 migration: version=%q err=%v", version, err)
		}
	})
	t.Run("future_version_no_ddl", func(t *testing.T) {
		db := newPostgresTestDB(t)
		ctx := context.Background()
		if _, err := db.ExecContext(ctx, `CREATE TABLE store_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, `INSERT INTO store_meta (key, value) VALUES ('schema_version', '999')`); err != nil {
			t.Fatal(err)
		}
		if _, err := OpenSQLSessionStore(ctx, db, SQLDialectPostgres); err == nil {
			t.Fatal("future postgres schema was accepted")
		}
		var table sql.NullString
		if err := db.QueryRowContext(ctx, "SELECT to_regclass('run_submissions')").Scan(&table); err != nil {
			t.Fatal(err)
		}
		if table.Valid {
			t.Fatal("current postgres DDL ran before future-version refusal")
		}
	})
}
