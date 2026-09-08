package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/whhhh1500/auto-agent/pkg/control"
	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/evaluation"
)

func evidenceSession(t *testing.T, store *SQLSessionStore) (*core.Session, core.RunCompositionEvidence) {
	t.Helper()
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	scope, err := user.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "session-evidence-index"})
	if err != nil {
		t.Fatal(err)
	}
	session, err := core.NewSession(core.SessionOptions{
		ID: "session-evidence-index", ProfileID: "evidence.agent", Principal: principal, Scope: scope,
	})
	if err != nil {
		t.Fatal(err)
	}
	composition := &core.RunCompositionData{
		Profile: core.AgentProfileSnapshot{ProfileID: "evidence.agent", Scope: scope},
		Metadata: map[string]string{
			"harness.assignment.id":        "evidence-assignment",
			"harness.assignment.candidate": "true",
		},
	}
	compositionRevision, err := core.CompositionRevision(composition)
	if err != nil {
		t.Fatal(err)
	}
	assignmentRevision, err := core.CompositionMetadataRevision(composition.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append("run-evidence-index", core.EvRunStart, core.RunStartData{
		CompositionRevision: compositionRevision, AssignmentRevision: assignmentRevision, Composition: composition,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	return session, core.RunCompositionEvidence{CompositionRevision: compositionRevision, AssignmentRevision: assignmentRevision}
}

func TestSQLSessionStoreIndexesAndBackfillsRunEvidence(t *testing.T) {
	ctx := context.Background()
	store := newTestSQLStore(t)
	session, evidence := evidenceSession(t, store)
	query := EvidenceQuery{
		CompositionRevision: evidence.CompositionRevision,
		AssignmentRevision:  evidence.AssignmentRevision,
		TenantID:            session.Principal().TenantID, SubjectID: session.Principal().SubjectID, Limit: 10,
	}
	records, err := store.QueryEvidence(ctx, query)
	if err != nil || len(records) != 1 || records[0].Kind != EvidenceRun || records[0].ID != "run-evidence-index" ||
		records[0].SessionID != session.ID() || records[0].CompositionRevision != evidence.CompositionRevision ||
		records[0].AssignmentRevision != evidence.AssignmentRevision || records[0].Status != "running" {
		t.Fatalf("run evidence index wrong: %#v err=%v", records, err)
	}
	if completed, err := store.QueryEvidence(ctx, EvidenceQuery{Statuses: []string{"completed"}, Limit: 10}); err != nil || len(completed) != 0 {
		t.Fatalf("running evidence leaked through completed filter: %#v err=%v", completed, err)
	}
	window := EvidenceQuery{
		CreatedAfter: records[0].CreatedAt.Add(-time.Second), CreatedBefore: records[0].CreatedAt.Add(time.Second), Limit: 10,
	}
	if ranged, err := store.QueryEvidence(ctx, window); err != nil || len(ranged) != 1 {
		t.Fatalf("evidence time range query wrong: %#v err=%v", ranged, err)
	}
	if _, err := store.db.ExecContext(ctx, "DROP TABLE run_evidence"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, sqlUpdateMetaRow.bind(SQLDialectSQLite), "20"); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenSQLSessionStore(ctx, store.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	records, err = reopened.QueryEvidence(ctx, query)
	if err != nil || len(records) != 1 || records[0].ID != "run-evidence-index" || records[0].Status != "running" {
		t.Fatalf("run evidence backfill wrong: %#v err=%v", records, err)
	}
	if _, err := reopened.db.ExecContext(ctx, "DROP INDEX run_evidence_status"); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.db.ExecContext(ctx, "DROP INDEX run_evidence_variant_status"); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.db.ExecContext(ctx, "ALTER TABLE run_evidence DROP COLUMN status"); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.db.ExecContext(ctx, sqlUpdateMetaRow.bind(SQLDialectSQLite), "21"); err != nil {
		t.Fatal(err)
	}
	reopened, err = OpenSQLSessionStore(ctx, reopened.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	records, err = reopened.QueryEvidence(ctx, query)
	if err != nil || len(records) != 1 || records[0].Status != "running" {
		t.Fatalf("v21 status backfill wrong: %#v err=%v", records, err)
	}
}

func TestEvidenceCursorKeepsSnapshotWhenNewerRecordArrives(t *testing.T) {
	ctx := context.Background()
	store := newTestSQLStore(t)
	now := time.Now().UTC().UnixMilli()
	composition := strings.Repeat("a", 64)
	assignment := strings.Repeat("b", 64)
	for index, created := range []int64{now - 3000, now - 2000, now - 1000} {
		if _, err := store.db.ExecContext(ctx, `INSERT INTO run_evidence
			(session_id, run_id, segment_seq, kind, tenant_id, subject_id, profile_id,
			 composition_revision, assignment_revision, created_at)
			VALUES (?, ?, ?, 'run', 'acme', 'alice', 'cursor.agent', ?, ?, ?)`,
			fmt.Sprintf("session-cursor-%d", index), fmt.Sprintf("run-cursor-%d", index), 0,
			composition, assignment, created); err != nil {
			t.Fatal(err)
		}
	}
	query := EvidenceQuery{CompositionRevision: composition, AssignmentRevision: assignment, TenantID: "acme", Limit: 2}
	first, err := store.QueryEvidencePage(ctx, query)
	if err != nil || len(first.Records) != 2 || first.NextCursor == "" {
		t.Fatalf("first snapshot page wrong: %#v err=%v", first, err)
	}
	if _, err := store.db.ExecContext(ctx, `INSERT INTO run_evidence
		(session_id, run_id, segment_seq, kind, tenant_id, subject_id, profile_id,
		 composition_revision, assignment_revision, created_at)
		VALUES ('session-cursor-new', 'run-cursor-new', 0, 'run', 'acme', 'alice', 'cursor.agent', ?, ?, ?)`,
		composition, assignment, time.Now().UTC().UnixMilli()+1000); err != nil {
		t.Fatal(err)
	}
	query.Cursor = first.NextCursor
	second, err := store.QueryEvidencePage(ctx, query)
	if err != nil || len(second.Records) != 1 || second.NextCursor != "" || second.Records[0].ID != "run-cursor-0" {
		t.Fatalf("second snapshot page drifted: %#v err=%v", second, err)
	}
	for _, record := range second.Records {
		if record.ID == "run-cursor-new" {
			t.Fatal("newer evidence leaked into an existing cursor snapshot")
		}
	}
}

func TestSQLRunEvidenceStatsUseLatestSegmentAndVariant(t *testing.T) {
	ctx := context.Background()
	store := newTestSQLStore(t)
	now := time.Now().UTC().UnixMilli()
	insert := func(sessionID, runID string, seq int64, variant AssignmentVariant, status string, created int64) {
		t.Helper()
		if _, err := store.db.ExecContext(ctx, `INSERT INTO run_evidence
			(session_id, run_id, segment_seq, kind, tenant_id, subject_id, profile_id,
			 composition_revision, assignment_revision, assignment_variant, status, created_at)
			VALUES (?, ?, ?, 'run', 'acme', 'alice', 'stats.agent', '', '', ?, ?, ?)`,
			sessionID, runID, seq, string(variant), status, created); err != nil {
			t.Fatal(err)
		}
	}
	insert("stats-session-1", "stats-run-1", 0, AssignmentCandidate, "running", now-5000)
	insert("stats-session-1", "stats-run-1", 2, AssignmentLive, "completed", now-4000)
	insert("stats-session-2", "stats-run-2", 0, AssignmentCandidate, "failed", now-3000)
	insert("stats-session-3", "stats-run-3", 0, AssignmentUnassigned, "running", now-2000)
	stats, err := store.RunEvidenceStats(ctx, EvidenceQuery{TenantID: "acme", ProfileID: "stats.agent"})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Total != 3 || stats.Terminal != 2 || stats.Completed != 1 || stats.Failed != 1 || stats.CompletionRate != 0.5 {
		t.Fatalf("run stats totals wrong: %#v", stats)
	}
	if stats.ByVariant[string(AssignmentCandidate)].Total != 1 || stats.ByVariant[string(AssignmentCandidate)].Failed != 1 ||
		stats.ByVariant[string(AssignmentLive)].Total != 1 || stats.ByVariant[string(AssignmentLive)].Completed != 1 ||
		stats.ByVariant[string(AssignmentUnassigned)].Total != 1 {
		t.Fatalf("run stats variants wrong: %#v", stats.ByVariant)
	}
	completed, err := store.RunEvidenceStats(ctx, EvidenceQuery{TenantID: "acme", Statuses: []string{"completed"}})
	if err != nil || completed.Total != 1 || completed.ByVariant[string(AssignmentLive)].Total != 1 {
		t.Fatalf("filtered run stats wrong: %#v err=%v", completed, err)
	}
	backtests, err := store.RunEvidenceStats(ctx, EvidenceQuery{TenantID: "acme", Kinds: []EvidenceKind{EvidenceBacktest}})
	if err != nil || backtests.Total != 0 {
		t.Fatalf("backtest-only stats wrong: %#v err=%v", backtests, err)
	}
}

func TestSQLEvidenceQueryCorrelatesRunAndEvaluation(t *testing.T) {
	ctx := context.Background()
	sessions := newTestSQLStore(t)
	session, evidence := evidenceSession(t, sessions)
	evaluations, err := NewSQLEvaluationStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	dataset, _, err := evaluations.PutDataset(ctx, sqlEvaluationDataset())
	if err != nil {
		t.Fatal(err)
	}
	run := sqlEvaluationRun(dataset)
	run.ID = "eval-evidence-index"
	run.TenantID = session.Principal().TenantID
	run.SubjectID = session.Principal().SubjectID
	run.CompositionMetadata = map[string]string{
		"harness.assignment.id":        "evidence-assignment",
		"harness.assignment.candidate": "true",
	}
	if err := evaluations.CreateRun(ctx, run); err != nil {
		t.Fatal(err)
	}
	caseResult := sqlEvaluationCaseResult()
	caseResult.SessionID = "evalsess-evidence-index"
	caseResult.AgentRunID = "evalcase-evidence-index"
	caseResult.Artifacts.CompositionRevision = evidence.CompositionRevision
	caseResult.Artifacts.AssignmentRevision = run.AssignmentRevision
	if err := evaluations.RecordCaseResult(ctx, run.ID, caseResult); err != nil {
		t.Fatal(err)
	}
	segments := session.Scope().Segments()
	product, err := core.NewScopePath(segments[:2]...)
	if err != nil {
		t.Fatal(err)
	}
	profiles := core.NewAgentProfileRegistry()
	stableName := "Evidence Stable"
	stableModel := core.ModelSelection{Provider: "evidence", Model: "stable"}
	if err := profiles.Bind(core.AgentProfileLayer{
		Scope: product, ProfileID: run.ProfileID, Name: &stableName, Model: &stableModel,
	}); err != nil {
		t.Fatal(err)
	}
	releases, err := control.NewReleaseManager(profiles)
	if err != nil {
		t.Fatal(err)
	}
	releases.Journal = sessions
	if err := releases.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	canaryStore, err := NewSQLCanaryStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	canaries, err := control.NewCanaryManager(releases, canaryStore)
	if err != nil {
		t.Fatal(err)
	}
	if err := canaries.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	baseRevision, err := releases.BaselineRevision(run.ProfileID, product)
	if err != nil {
		t.Fatal(err)
	}
	candidateName := "Evidence Candidate"
	candidateModel := core.ModelSelection{Provider: "evidence", Model: "candidate"}
	layer := core.AgentProfileLayer{Scope: product, ProfileID: run.ProfileID, Name: &candidateName, Model: &candidateModel}
	layerRevision, err := core.ProfileLayerRevision(layer)
	if err != nil {
		t.Fatal(err)
	}
	staged, err := canaries.Stage(ctx, control.CanaryRecord{
		ID: "canary-evidence-index", ProfileID: run.ProfileID, Scope: product,
		Layer: &layer, Revision: layerRevision, BaseReleaseRevision: baseRevision,
		BasisPoints: 10000, CandidateEvaluationRunID: run.ID, Gate: evaluation.GateResult{Passed: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	query := EvidenceQuery{
		CompositionRevision: evidence.CompositionRevision,
		AssignmentRevision:  run.AssignmentRevision,
		TenantID:            session.Principal().TenantID, Limit: 20,
	}
	records, err := sessions.QueryEvidence(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	foundRun, foundEvaluation, foundCanary := false, false, false
	for _, record := range records {
		if record.Kind == EvidenceRun && record.ID == "run-evidence-index" {
			foundRun = true
		}
		if record.Kind == EvidenceEvaluation && record.ID == run.ID {
			foundEvaluation = true
		}
		if record.Kind == EvidenceCanary && record.ID == staged.ID {
			foundCanary = true
		}
	}
	if !foundRun || !foundEvaluation || !foundCanary {
		t.Fatalf("cross-object evidence missing: %#v", records)
	}
	if evaluationRecords, err := sessions.QueryEvidence(ctx, EvidenceQuery{
		CompositionRevision: evidence.CompositionRevision, TenantID: query.TenantID,
		Kinds: []EvidenceKind{EvidenceEvaluation}, Statuses: []string{"running"}, Limit: 10,
	}); err != nil || len(evaluationRecords) != 1 || evaluationRecords[0].ID != run.ID {
		t.Fatalf("evaluation status filter wrong: %#v err=%v", evaluationRecords, err)
	}
	if canaryRecords, err := sessions.QueryEvidence(ctx, EvidenceQuery{
		AssignmentRevision: run.AssignmentRevision, TenantID: query.TenantID,
		Kinds: []EvidenceKind{EvidenceCanary}, Statuses: []string{"active"}, Limit: 10,
	}); err != nil || len(canaryRecords) != 1 || canaryRecords[0].ID != staged.ID {
		t.Fatalf("active canary status filter wrong: %#v err=%v", canaryRecords, err)
	}
	canaryOnly, err := sessions.QueryEvidence(ctx, EvidenceQuery{
		CompositionRevision: evidence.CompositionRevision, AssignmentRevision: run.AssignmentRevision,
		TenantID: session.Principal().TenantID, Kinds: []EvidenceKind{EvidenceCanary}, Limit: 20,
	})
	if err != nil || len(canaryOnly) != 1 || canaryOnly[0].Kind != EvidenceCanary || canaryOnly[0].ID != staged.ID {
		t.Fatalf("canary kind filter wrong: %#v err=%v", canaryOnly, err)
	}
	if _, err := sessions.QueryEvidence(ctx, EvidenceQuery{Kinds: []EvidenceKind{"unknown"}, Limit: 10}); err == nil {
		t.Fatal("unknown evidence kind was accepted")
	}
	if _, err := canaries.Promote(ctx, staged.ID); err != nil {
		t.Fatal(err)
	}
	records, err = sessions.QueryEvidence(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	foundRelease := false
	for _, record := range records {
		if record.Kind == EvidenceRelease && record.RelatedCanaryID == staged.ID && record.ArtifactRevision == layerRevision {
			foundRelease = true
		}
	}
	if !foundRelease {
		t.Fatalf("promoted release evidence missing: %#v", records)
	}
	if promotedCanary, err := sessions.QueryEvidence(ctx, EvidenceQuery{
		AssignmentRevision: run.AssignmentRevision, TenantID: query.TenantID,
		Kinds: []EvidenceKind{EvidenceCanary}, Statuses: []string{"promoted"}, Limit: 10,
	}); err != nil || len(promotedCanary) != 1 || promotedCanary[0].ID != staged.ID {
		t.Fatalf("promoted canary status filter wrong: %#v err=%v", promotedCanary, err)
	}
	if activeRelease, err := sessions.QueryEvidence(ctx, EvidenceQuery{
		AssignmentRevision: run.AssignmentRevision, TenantID: query.TenantID,
		Kinds: []EvidenceKind{EvidenceRelease}, Statuses: []string{"active"}, Limit: 10,
	}); err != nil || len(activeRelease) != 1 || activeRelease[0].RelatedCanaryID != staged.ID {
		t.Fatalf("active release status filter wrong: %#v err=%v", activeRelease, err)
	}
	if rolledBack, err := releases.Rollback(ctx, run.ProfileID, 0); err != nil || len(rolledBack) != 1 {
		t.Fatalf("release rollback for status evidence failed: %#v err=%v", rolledBack, err)
	}
	if rolledRelease, err := sessions.QueryEvidence(ctx, EvidenceQuery{
		AssignmentRevision: run.AssignmentRevision, TenantID: query.TenantID,
		Kinds: []EvidenceKind{EvidenceRelease}, Statuses: []string{"rolled_back"}, Limit: 10,
	}); err != nil || len(rolledRelease) != 1 || !rolledRelease[0].RolledBack {
		t.Fatalf("rolled-back release status filter wrong: %#v err=%v", rolledRelease, err)
	}
	pageQuery := query
	pageQuery.Limit = 1
	seen := map[string]bool{}
	for {
		page, err := sessions.QueryEvidencePage(ctx, pageQuery)
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Records) != 1 {
			t.Fatalf("evidence page size=%d records=%#v", len(page.Records), page.Records)
		}
		key := string(page.Records[0].Kind) + "|" + page.Records[0].ID + "|" + page.Records[0].SessionID
		if seen[key] {
			t.Fatalf("evidence pagination repeated %s", key)
		}
		seen[key] = true
		if page.NextCursor == "" {
			break
		}
		pageQuery.Cursor = page.NextCursor
	}
	for _, required := range []string{
		"run|run-evidence-index|session-evidence-index",
		"evaluation|eval-evidence-index|",
		"canary|canary-evidence-index|",
		"release|" + run.ProfileID + ":v1|",
	} {
		if !seen[required] {
			t.Fatalf("evidence pagination omitted %s: %#v", required, seen)
		}
	}
	firstPage, err := sessions.QueryEvidencePage(ctx, EvidenceQuery{
		CompositionRevision: query.CompositionRevision, AssignmentRevision: query.AssignmentRevision,
		TenantID: query.TenantID, Limit: 1,
	})
	if err != nil || firstPage.NextCursor == "" {
		t.Fatalf("first cursor page wrong: %#v err=%v", firstPage, err)
	}
	replacement := "A"
	if strings.HasSuffix(firstPage.NextCursor, replacement) {
		replacement = "B"
	}
	tampered := firstPage.NextCursor[:len(firstPage.NextCursor)-1] + replacement
	if _, err := sessions.QueryEvidencePage(ctx, EvidenceQuery{
		CompositionRevision: query.CompositionRevision, AssignmentRevision: query.AssignmentRevision,
		TenantID: query.TenantID, Limit: 1, Cursor: tampered,
	}); !errors.Is(err, ErrEvidenceCursorInvalid) {
		t.Fatalf("tampered evidence cursor accepted: %v", err)
	}
	if _, err := sessions.QueryEvidencePage(ctx, EvidenceQuery{
		CompositionRevision: strings.Repeat("f", 64), AssignmentRevision: query.AssignmentRevision,
		TenantID: query.TenantID, Limit: 1, Cursor: firstPage.NextCursor,
	}); !errors.Is(err, ErrEvidenceCursorInvalid) {
		t.Fatalf("cross-query evidence cursor accepted: %v", err)
	}
	if _, err := sessions.QueryEvidencePage(ctx, EvidenceQuery{
		CompositionRevision: query.CompositionRevision, AssignmentRevision: query.AssignmentRevision,
		TenantID: query.TenantID, Kinds: []EvidenceKind{EvidenceRun}, Limit: 1, Cursor: firstPage.NextCursor,
	}); !errors.Is(err, ErrEvidenceCursorInvalid) {
		t.Fatalf("cross-kind evidence cursor accepted: %v", err)
	}
	if _, err := sessions.QueryEvidencePage(ctx, EvidenceQuery{
		CompositionRevision: query.CompositionRevision, AssignmentRevision: query.AssignmentRevision,
		TenantID: query.TenantID, Statuses: []string{"running"}, Limit: 1, Cursor: firstPage.NextCursor,
	}); !errors.Is(err, ErrEvidenceCursorInvalid) {
		t.Fatalf("cross-status evidence cursor accepted: %v", err)
	}
	if _, err := sessions.QueryEvidencePage(ctx, EvidenceQuery{
		CompositionRevision: query.CompositionRevision, AssignmentRevision: query.AssignmentRevision,
		TenantID: query.TenantID, CreatedAfter: time.Now().UTC().Add(-time.Hour), Limit: 1, Cursor: firstPage.NextCursor,
	}); !errors.Is(err, ErrEvidenceCursorInvalid) {
		t.Fatalf("cross-time evidence cursor accepted: %v", err)
	}
	if _, err := sessions.QueryEvidence(ctx, EvidenceQuery{Statuses: []string{"bad status"}, Limit: 10}); err == nil {
		t.Fatal("invalid evidence status was accepted")
	}
	encoded, err := json.Marshal(records)
	if err != nil || len(encoded) > 64<<10 {
		t.Fatalf("evidence response is invalid or unbounded: bytes=%d err=%v", len(encoded), err)
	}
	_ = time.Now()
}

var _ EvidenceStore = (*SQLSessionStore)(nil)
var _ EvidencePager = (*SQLSessionStore)(nil)

var _ = evaluation.RunResult{}

func TestEvidenceQueryRejectsNULIdentityFilters(t *testing.T) {
	store := newTestSQLStore(t)
	ctx := context.Background()
	for name, query := range map[string]EvidenceQuery{
		"tenant":  {TenantID: "acme\x00", Limit: 10},
		"subject": {SubjectID: "alice\n", Limit: 10},
		"profile": {ProfileID: "too." + strings.Repeat("x", 128), Limit: 10},
	} {
		t.Run(name, func(t *testing.T) {
			if err := query.Validate(); err == nil {
				t.Fatal("invalid evidence identity filter was accepted")
			}
			if _, err := store.QueryEvidence(ctx, query); err == nil {
				t.Fatal("invalid evidence query reached the store")
			}
		})
	}
}

func TestSQLSessionStoreRejectsRunEvidenceOverflow(t *testing.T) {
	store := newTestSQLStore(t)
	store.maxEvidence = 2
	ctx := context.Background()
	first := createEvidenceSession(t, store, "session-evidence-cap-a", "run-evidence-cap-a")
	_ = createEvidenceSession(t, store, "session-evidence-cap-b", "run-evidence-cap-b")
	third := newEvidenceSession(t, "session-evidence-cap-c", "run-evidence-cap-c")
	if err := store.Create(ctx, third); err == nil || !strings.Contains(err.Error(), "run evidence exceeds") {
		t.Fatalf("sql run evidence overflow was accepted: %v", err)
	}
	header := core.SessionOptions{ProfileID: first.ProfileID(), Principal: first.Principal()}
	if err := insertRunEvidence(ctx, store.db, SQLDialectSQLite, first.ID(), header, first.Events(), 2); err != nil {
		t.Fatalf("replaying an existing run evidence row must not count as overflow: %v", err)
	}
}

func newEvidenceSession(t *testing.T, sessionID, runID string) *core.Session {
	t.Helper()
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	scope, err := user.Child(core.ScopeRef{Kind: core.ScopeSession, ID: sessionID})
	if err != nil {
		t.Fatal(err)
	}
	session, err := core.NewSession(core.SessionOptions{
		ID: sessionID, ProfileID: "evidence.agent", Principal: principal, Scope: scope,
	})
	if err != nil {
		t.Fatal(err)
	}
	composition := &core.RunCompositionData{
		Profile: core.AgentProfileSnapshot{ProfileID: "evidence.agent", Scope: scope},
		Metadata: map[string]string{
			"harness.assignment.id":        "evidence-assignment",
			"harness.assignment.candidate": "true",
		},
	}
	compositionRevision, err := core.CompositionRevision(composition)
	if err != nil {
		t.Fatal(err)
	}
	assignmentRevision, err := core.CompositionMetadataRevision(composition.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append(runID, core.EvRunStart, core.RunStartData{
		CompositionRevision: compositionRevision, AssignmentRevision: assignmentRevision, Composition: composition,
	}); err != nil {
		t.Fatal(err)
	}
	return session
}

func createEvidenceSession(t *testing.T, store *SQLSessionStore, sessionID, runID string) *core.Session {
	t.Helper()
	session := newEvidenceSession(t, sessionID, runID)
	if err := store.Create(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	return session
}
