package storage

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

func fileEvidenceSession(t *testing.T, store *FileSessionStore, backtest bool) (*core.Session, core.RunCompositionEvidence) {
	t.Helper()
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	scope, err := user.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "file-evidence-session"})
	if err != nil {
		t.Fatal(err)
	}
	metadata := map[string]string{}
	if backtest {
		metadata["backtest_of"] = "source-session"
	}
	session, err := core.NewSession(core.SessionOptions{
		ID: "file-evidence-session", ProfileID: "file.evidence", Principal: principal, Scope: scope, Metadata: metadata,
	})
	if err != nil {
		t.Fatal(err)
	}
	composition := &core.RunCompositionData{
		Profile:  core.AgentProfileSnapshot{ProfileID: "file.evidence", Scope: scope},
		Metadata: map[string]string{"assignment": "file-candidate"},
	}
	compositionRevision, err := core.CompositionRevision(composition)
	if err != nil {
		t.Fatal(err)
	}
	assignmentRevision, err := core.CompositionMetadataRevision(composition.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append("run-file-evidence", core.EvRunStart, core.RunStartData{
		CompositionRevision: compositionRevision, AssignmentRevision: assignmentRevision, Composition: composition,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append("run-file-evidence", core.EvApprovalRequested, core.ApprovalRequestedData{
		ApprovalID: "apr_0123456789abcdef0123456789abcdef",
		ToolCall: core.ToolCall{
			ID:   "call-file-evidence",
			Name: "evidence.tool",
		},
		ResumeCall: core.ToolCall{
			ID:   "call-file-evidence",
			Name: "evidence.tool",
		},
		Step: 0,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	return session, core.RunCompositionEvidence{CompositionRevision: compositionRevision, AssignmentRevision: assignmentRevision}
}

func TestFileSessionStoreEvidenceSidecarAndRebuild(t *testing.T) {
	ctx := context.Background()
	store, err := NewFileSessionStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	session, evidence := fileEvidenceSession(t, store, false)
	query := EvidenceQuery{CompositionRevision: evidence.CompositionRevision, AssignmentRevision: evidence.AssignmentRevision, Limit: 10}
	records, err := store.QueryEvidence(ctx, query)
	if err != nil || len(records) != 1 || records[0].Kind != EvidenceRun || records[0].ID != "run-file-evidence" ||
		records[0].SegmentSeq != 0 || records[0].Status != string(core.RunWaitingApproval) || records[0].AssignmentVariant != AssignmentUnassigned {
		t.Fatalf("file evidence query wrong: %#v err=%v", records, err)
	}
	if failed, err := store.QueryEvidence(ctx, EvidenceQuery{Statuses: []string{"failed"}, Limit: 10}); err != nil || len(failed) != 0 {
		t.Fatalf("file waiting approval evidence leaked through failed filter: %#v err=%v", failed, err)
	}
	resumeComposition := &core.RunCompositionData{
		Profile:  core.AgentProfileSnapshot{ProfileID: session.ProfileID(), Scope: session.Scope()},
		Metadata: map[string]string{"assignment": "file-resume", "harness.assignment.variant": "live"},
	}
	resumeRevision, err := core.CompositionRevision(resumeComposition)
	if err != nil {
		t.Fatal(err)
	}
	resumeAssignment, err := core.CompositionMetadataRevision(resumeComposition.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append("run-file-evidence", core.EvRunResume, core.RunResumeData{
		CompositionRevision: resumeRevision, AssignmentRevision: resumeAssignment, Composition: resumeComposition,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append("run-file-evidence", core.EvApprovalResolved, core.ApprovalResolvedData{
		ApprovalID: "apr_0123456789abcdef0123456789abcdef",
		CallID:     "call-file-evidence",
		Decision:   core.ApprovalApproved,
		ResolvedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append("run-file-evidence", core.EvRunEnd, core.RunEndData{Status: core.RunCompleted}); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(ctx, session, 2); err != nil {
		t.Fatal(err)
	}
	if session.Version() != 5 {
		t.Fatalf("file resumed session version=%d, want 5", session.Version())
	}
	loaded, err := store.Load(ctx, session.ID())
	if err != nil || loaded.Version() != 5 {
		t.Fatalf("file persisted resumed session version=%d err=%v, want 5", loaded.Version(), err)
	}
	records, err = store.QueryEvidence(ctx, query)
	if err != nil || len(records) != 1 || records[0].SegmentSeq != 0 || records[0].Status != string(core.RunCompleted) {
		t.Fatalf("file original segment status was not finalized: %#v err=%v", records, err)
	}
	records, err = store.QueryEvidence(ctx, EvidenceQuery{AssignmentRevision: resumeAssignment, Limit: 10})
	if err != nil || len(records) != 1 || records[0].SegmentSeq != 2 || records[0].Status != string(core.RunCompleted) || records[0].AssignmentVariant != AssignmentLive {
		t.Fatalf("file resume evidence missing: %#v err=%v", records, err)
	}
	stats, err := store.RunEvidenceStats(ctx, EvidenceQuery{ProfileID: session.ProfileID()})
	if err != nil || stats.Total != 1 || stats.Completed != 1 || stats.ByVariant[string(AssignmentLive)].Total != 1 {
		t.Fatalf("file run stats wrong: %#v err=%v", stats, err)
	}
	pageQuery := EvidenceQuery{Limit: 1}
	firstPage, err := store.QueryEvidencePage(ctx, pageQuery)
	if err != nil || len(firstPage.Records) != 1 || firstPage.NextCursor == "" {
		t.Fatalf("file first evidence page wrong: %#v err=%v", firstPage, err)
	}
	pageQuery.Cursor = firstPage.NextCursor
	secondPage, err := store.QueryEvidencePage(ctx, pageQuery)
	if err != nil || len(secondPage.Records) != 1 || secondPage.NextCursor != "" ||
		secondPage.Records[0].SegmentSeq == firstPage.Records[0].SegmentSeq ||
		firstPage.Records[0].Status != string(core.RunCompleted) || secondPage.Records[0].Status != string(core.RunCompleted) {
		t.Fatalf("file second evidence page wrong: %#v err=%v", secondPage, err)
	}
	segments := map[int64]bool{firstPage.Records[0].SegmentSeq: true, secondPage.Records[0].SegmentSeq: true}
	if !segments[0] || !segments[2] {
		t.Fatalf("file evidence pages did not contain start and resume segments: %#v", segments)
	}
	path, err := store.evidencePath(session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	records, err = store.QueryEvidence(ctx, query)
	if err != nil || len(records) != 1 || records[0].ID != "run-file-evidence" || records[0].SegmentSeq != 0 || records[0].Status != string(core.RunCompleted) {
		t.Fatalf("file evidence sidecar rebuild failed: %#v err=%v", records, err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("rebuilt file evidence sidecar missing: %v", err)
	}
}

func TestFileSessionStoreMarksBacktestEvidence(t *testing.T) {
	store, err := NewFileSessionStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	_, evidence := fileEvidenceSession(t, store, true)
	records, err := store.QueryEvidence(context.Background(), EvidenceQuery{CompositionRevision: evidence.CompositionRevision, Limit: 10})
	if err != nil || len(records) != 1 || records[0].Kind != EvidenceKind("backtest") {
		t.Fatalf("backtest evidence kind wrong: %#v err=%v", records, err)
	}
	if _, err := json.Marshal(records); err != nil {
		t.Fatal(err)
	}
	if records, err := store.QueryEvidence(context.Background(), EvidenceQuery{
		CompositionRevision: evidence.CompositionRevision, Kinds: []EvidenceKind{EvidenceRun}, Limit: 10,
	}); err != nil || len(records) != 0 {
		t.Fatalf("file backtest leaked through run kind filter: %#v err=%v", records, err)
	}
	if records, err := store.QueryEvidence(context.Background(), EvidenceQuery{
		CompositionRevision: evidence.CompositionRevision, Kinds: []EvidenceKind{EvidenceBacktest}, Limit: 10,
	}); err != nil || len(records) != 1 || records[0].Kind != EvidenceBacktest {
		t.Fatalf("file backtest kind filter wrong: %#v err=%v", records, err)
	}
}

var _ EvidenceStore = (*FileSessionStore)(nil)
var _ EvidencePager = (*FileSessionStore)(nil)
var _ EvidenceStatsStore = (*FileSessionStore)(nil)

func TestFileSessionStoreRejectsInvalidSessionID(t *testing.T) {
	store, err := NewFileSessionStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	for _, id := range []string{"", "bad id", "slash/id"} {
		scope, err := user.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "file-id"})
		if err != nil {
			t.Fatal(err)
		}
		session, err := core.NewSession(core.SessionOptions{
			ID: id, ProfileID: "file.ids", Principal: principal, Scope: scope,
		})
		if err == nil {
			if err := store.Create(ctx, session); err == nil {
				t.Fatalf("invalid session id %q was persisted", id)
			}
		}
		if _, err := store.Load(ctx, id); err == nil {
			t.Fatalf("invalid session id %q was loaded", id)
		}
	}
}
