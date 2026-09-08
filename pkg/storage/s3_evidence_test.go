package storage

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

func s3EvidenceSession(t *testing.T) (*S3SessionStore, *fakeS3, *core.Session, core.RunCompositionEvidence) {
	t.Helper()
	store, server := newFakeS3Store(t)
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	scope, err := user.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "s3-evidence-session"})
	if err != nil {
		t.Fatal(err)
	}
	session, err := core.NewSession(core.SessionOptions{
		ID: "s3-evidence-session", ProfileID: "s3.evidence", Principal: principal, Scope: scope,
	})
	if err != nil {
		t.Fatal(err)
	}
	composition := &core.RunCompositionData{
		Profile:  core.AgentProfileSnapshot{ProfileID: "s3.evidence", Scope: scope},
		Metadata: map[string]string{"assignment": "s3-candidate"},
	}
	compositionRevision, err := core.CompositionRevision(composition)
	if err != nil {
		t.Fatal(err)
	}
	assignmentRevision, err := core.CompositionMetadataRevision(composition.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append("run-s3-evidence", core.EvRunStart, core.RunStartData{
		CompositionRevision: compositionRevision, AssignmentRevision: assignmentRevision, Composition: composition,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append("run-s3-evidence", core.EvApprovalRequested, core.ApprovalRequestedData{
		ApprovalID: "apr_0123456789abcdef0123456789abcdef",
		ToolCall: core.ToolCall{
			ID:   "call-s3-evidence",
			Name: "evidence.tool",
		},
		ResumeCall: core.ToolCall{
			ID:   "call-s3-evidence",
			Name: "evidence.tool",
		},
		Step: 0,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	return store, server, session, core.RunCompositionEvidence{CompositionRevision: compositionRevision, AssignmentRevision: assignmentRevision}
}

func TestS3SessionStoreEvidenceSidecarAndRebuild(t *testing.T) {
	ctx := context.Background()
	store, server, session, evidence := s3EvidenceSession(t)
	loaded, err := store.Load(ctx, session.ID())
	if err != nil || loaded.Version() != 2 {
		t.Fatalf("S3 Create lost initial events: version=%d err=%v", loaded.Version(), err)
	}
	query := EvidenceQuery{CompositionRevision: evidence.CompositionRevision, AssignmentRevision: evidence.AssignmentRevision, Limit: 10}
	records, err := store.QueryEvidence(ctx, query)
	if err != nil || len(records) != 1 || records[0].Kind != EvidenceRun || records[0].SegmentSeq != 0 ||
		records[0].Status != string(core.RunWaitingApproval) || records[0].AssignmentVariant != AssignmentUnassigned {
		t.Fatalf("S3 evidence query wrong: %#v err=%v", records, err)
	}
	if records, err := store.QueryEvidence(ctx, EvidenceQuery{
		CompositionRevision: evidence.CompositionRevision, Kinds: []EvidenceKind{EvidenceEvaluation}, Limit: 10,
	}); err != nil || len(records) != 0 {
		t.Fatalf("S3 fabricated evaluation evidence: %#v err=%v", records, err)
	}
	resumeComposition := &core.RunCompositionData{
		Profile:  core.AgentProfileSnapshot{ProfileID: session.ProfileID(), Scope: session.Scope()},
		Metadata: map[string]string{"assignment": "s3-resume", "harness.assignment.variant": "live"},
	}
	resumeRevision, err := core.CompositionRevision(resumeComposition)
	if err != nil {
		t.Fatal(err)
	}
	resumeAssignment, err := core.CompositionMetadataRevision(resumeComposition.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append("run-s3-evidence", core.EvRunResume, core.RunResumeData{
		CompositionRevision: resumeRevision, AssignmentRevision: resumeAssignment, Composition: resumeComposition,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append("run-s3-evidence", core.EvApprovalResolved, core.ApprovalResolvedData{
		ApprovalID: "apr_0123456789abcdef0123456789abcdef",
		CallID:     "call-s3-evidence",
		Decision:   core.ApprovalApproved,
		ResolvedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append("run-s3-evidence", core.EvRunEnd, core.RunEndData{Status: core.RunCompleted}); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(ctx, session, 2); err != nil {
		t.Fatal(err)
	}
	if session.Version() != 5 {
		t.Fatalf("S3 resumed session version=%d, want 5", session.Version())
	}
	loaded, err = store.Load(ctx, session.ID())
	if err != nil || loaded.Version() != 5 {
		t.Fatalf("S3 persisted resumed session version=%d err=%v, want 5", loaded.Version(), err)
	}
	records, err = store.QueryEvidence(ctx, query)
	if err != nil || len(records) != 1 || records[0].SegmentSeq != 0 || records[0].Status != string(core.RunCompleted) {
		t.Fatalf("S3 original segment status was not finalized: %#v err=%v", records, err)
	}
	records, err = store.QueryEvidence(ctx, EvidenceQuery{AssignmentRevision: resumeAssignment, Limit: 10})
	if err != nil || len(records) != 1 || records[0].SegmentSeq != 2 || records[0].Status != string(core.RunCompleted) || records[0].AssignmentVariant != AssignmentLive {
		t.Fatalf("S3 resume evidence missing: %#v err=%v", records, err)
	}
	stats, err := store.RunEvidenceStats(ctx, EvidenceQuery{ProfileID: session.ProfileID()})
	if err != nil || stats.Total != 1 || stats.Completed != 1 || stats.ByVariant[string(AssignmentLive)].Total != 1 {
		t.Fatalf("S3 run stats wrong: %#v err=%v", stats, err)
	}
	pageQuery := EvidenceQuery{Limit: 1}
	firstPage, err := store.QueryEvidencePage(ctx, pageQuery)
	if err != nil || len(firstPage.Records) != 1 || firstPage.NextCursor == "" {
		t.Fatalf("S3 first evidence page wrong: %#v err=%v", firstPage, err)
	}
	pageQuery.Cursor = firstPage.NextCursor
	secondPage, err := store.QueryEvidencePage(ctx, pageQuery)
	if err != nil || len(secondPage.Records) != 1 || secondPage.NextCursor != "" ||
		secondPage.Records[0].SegmentSeq == firstPage.Records[0].SegmentSeq ||
		firstPage.Records[0].Status != string(core.RunCompleted) || secondPage.Records[0].Status != string(core.RunCompleted) {
		t.Fatalf("S3 second evidence page wrong: %#v err=%v", secondPage, err)
	}
	segments := map[int64]bool{firstPage.Records[0].SegmentSeq: true, secondPage.Records[0].SegmentSeq: true}
	if !segments[0] || !segments[2] {
		t.Fatalf("S3 evidence pages did not contain start and resume segments: %#v", segments)
	}
	server.mu.Lock()
	delete(server.objects, s3EvidenceKey(session.ID()))
	server.mu.Unlock()
	records, err = store.QueryEvidence(ctx, query)
	if err != nil || len(records) != 1 || records[0].ID != "run-s3-evidence" {
		t.Fatalf("S3 evidence rebuild failed: %#v err=%v", records, err)
	}
	server.mu.Lock()
	payload, _ := json.Marshal(persistedRunEvidence{Version: 1, Records: records})
	server.objects[s3EvidenceKey(session.ID())] = fakeObject{body: payload, etag: contentETag(payload)}
	server.mu.Unlock()
	records, err = store.QueryEvidence(ctx, EvidenceQuery{AssignmentRevision: resumeAssignment, Limit: 10})
	if err != nil || len(records) != 1 || records[0].SegmentSeq != 2 || records[0].Status != string(core.RunCompleted) || records[0].AssignmentVariant != AssignmentLive {
		t.Fatalf("S3 stale evidence rebuild failed: %#v err=%v", records, err)
	}
}

func TestS3CreateConflictDoesNotOverwriteExistingEvents(t *testing.T) {
	store, server, session, _ := s3EvidenceSession(t)
	key := chunkKey(session.ID(), 0)
	server.mu.Lock()
	original := append([]byte(nil), server.objects[key].body...)
	server.mu.Unlock()
	if err := store.Create(context.Background(), session); err == nil {
		t.Fatal("duplicate S3 create unexpectedly succeeded")
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	if string(server.objects[key].body) != string(original) {
		t.Fatal("duplicate S3 create overwrote the committed initial chunk")
	}
}

func TestS3EvidenceCreateWorksWithConditionalWritesDisabled(t *testing.T) {
	store, _, session, evidence := s3EvidenceSession(t)
	store.DisableConditionalWrites = true
	if err := store.Create(context.Background(), session); err == nil {
		t.Fatal("duplicate disabled-CAS create unexpectedly succeeded")
	}
	// Use a fresh store/object set so the disabled-CAS path exercises a real
	// initial create rather than the duplicate conflict branch.
	fresh, _, freshSession, freshEvidence := s3EvidenceSession(t)
	fresh.DisableConditionalWrites = true
	loaded, err := fresh.Load(context.Background(), freshSession.ID())
	if err != nil || loaded.Version() != 2 {
		t.Fatalf("disabled-CAS initial evidence create failed: version=%d err=%v", loaded.Version(), err)
	}
	records, err := fresh.QueryEvidence(context.Background(), EvidenceQuery{CompositionRevision: freshEvidence.CompositionRevision, Limit: 10})
	if err != nil || len(records) != 1 || records[0].ID != "run-s3-evidence" {
		t.Fatalf("disabled-CAS evidence query failed: %#v err=%v", records, err)
	}
	_ = evidence
}

var _ EvidenceStore = (*S3SessionStore)(nil)
var _ EvidenceStatsStore = (*S3SessionStore)(nil)
