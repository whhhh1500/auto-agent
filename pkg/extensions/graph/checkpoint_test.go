package graph

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestCheckpointVersionValidatesDeterministicIdentityAndDigest(t *testing.T) {
	checkpoint := checkpointForTest(CheckpointExecuting)
	digest, err := CheckpointDigest(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	version := CheckpointVersion{
		Info: CheckpointVersionInfo{
			ID:             CheckpointVersionID(checkpoint.Key, checkpoint.Revision),
			Origin:         CheckpointVersionOriginCommit,
			Key:            checkpoint.Key,
			Revision:       checkpoint.Revision,
			CheckpointHash: digest,
			CreatedAt:      1,
		},
		Checkpoint: checkpoint,
	}
	validated, err := ValidateCheckpointVersion(version)
	if err != nil {
		t.Fatal(err)
	}
	if validated.Info.ID != CheckpointVersionID(checkpoint.Key, checkpoint.Revision) || validated.Info.CheckpointHash != digest {
		t.Fatalf("version identity = %#v", validated.Info)
	}
	changed := checkpoint
	changed.CurrentNodeID = "other"
	changedDigest, err := CheckpointDigest(changed)
	if err != nil {
		t.Fatal(err)
	}
	if changedDigest == digest || CheckpointVersionID(changed.Key, changed.Revision) != validated.Info.ID {
		t.Fatalf("version ID and checkpoint digest do not have distinct identities")
	}
	version.Checkpoint.State["account"] = json.RawMessage(`"mutated"`)
	if string(validated.Checkpoint.State["account"]) != `"value"` {
		t.Fatalf("version checkpoint was not defensively cloned: %s", validated.Checkpoint.State["account"])
	}

	version = validated
	version.Info.CheckpointHash = "not-a-sha256-digest"
	if _, err := ValidateCheckpointVersion(version); !errors.Is(err, ErrInvalidCheckpoint) {
		t.Fatalf("invalid digest error = %v", err)
	}
	version = validated
	version.Info.ParentID = "unexpected"
	if _, err := ValidateCheckpointVersion(version); !errors.Is(err, ErrInvalidCheckpoint) {
		t.Fatalf("invalid fork parent error = %v", err)
	}
}

func TestCheckpointVersionAcceptsStrictExternalForkParent(t *testing.T) {
	checkpoint := checkpointForTest(CheckpointReady)
	digest, err := CheckpointDigest(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	version := CheckpointVersion{Info: CheckpointVersionInfo{
		ID:             CheckpointVersionID(checkpoint.Key, checkpoint.Revision),
		Origin:         CheckpointVersionOriginFork,
		Key:            checkpoint.Key,
		Revision:       checkpoint.Revision,
		CheckpointHash: digest,
		CreatedAt:      1,
	}, Checkpoint: checkpoint}
	version.Info.ParentID = CheckpointVersionID(CheckpointKey{TenantID: "source-tenant", SessionID: "source-session", RunID: "source-run"}, 9)
	if _, err := ValidateCheckpointVersion(version); err != nil {
		t.Fatalf("external fork parent rejected: %v", err)
	}
	for _, parent := range []string{
		"",
		"too-short",
		"ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789",
		"gggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg",
	} {
		candidate := version
		candidate.Info.ParentID = parent
		if _, err := ValidateCheckpointVersion(candidate); !errors.Is(err, ErrInvalidCheckpoint) {
			t.Fatalf("invalid external parent %q error = %v", parent, err)
		}
	}
	version.Info.ParentID = version.Info.ID
	if _, err := ValidateCheckpointVersion(version); !errors.Is(err, ErrInvalidCheckpoint) {
		t.Fatalf("self-referential external parent error = %v", err)
	}
}

func TestCheckpointVersionAllowsOnlyExpectedNormalParent(t *testing.T) {
	checkpoint := checkpointForTest(CheckpointExecuting)
	checkpoint.Revision = 2
	digest, err := CheckpointDigest(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	version := CheckpointVersion{Info: CheckpointVersionInfo{
		ID:             CheckpointVersionID(checkpoint.Key, checkpoint.Revision),
		ParentID:       CheckpointVersionID(checkpoint.Key, 1),
		Origin:         CheckpointVersionOriginCommit,
		Key:            checkpoint.Key,
		Revision:       checkpoint.Revision,
		CheckpointHash: digest,
		CreatedAt:      1,
	}, Checkpoint: checkpoint}
	if _, err := ValidateCheckpointVersion(version); err != nil {
		t.Fatal(err)
	}
	version.Info.ParentID = CheckpointVersionID(checkpoint.Key, 7)
	if _, err := ValidateCheckpointVersion(version); !errors.Is(err, ErrInvalidCheckpoint) {
		t.Fatalf("wrong parent error = %v", err)
	}
	version.Info.Origin, version.Info.ParentID = CheckpointVersionOriginMigrationFloor, ""
	if _, err := ValidateCheckpointVersion(version); err != nil {
		t.Fatalf("migrated floor rejected: %v", err)
	}
	version.Info.Origin = CheckpointVersionOriginCommit
	if _, err := ValidateCheckpointVersion(version); !errors.Is(err, ErrInvalidCheckpoint) {
		t.Fatalf("commit without preceding parent error = %v", err)
	}
	version.Info.Origin, version.Info.ParentID = CheckpointVersionOriginFork, CheckpointVersionID(checkpoint.Key, 1)
	if _, err := ValidateCheckpointVersion(version); !errors.Is(err, ErrInvalidCheckpoint) {
		t.Fatalf("non-initial fork error = %v", err)
	}
	version.Info.Origin = "unknown"
	if _, err := ValidateCheckpointVersion(version); !errors.Is(err, ErrInvalidCheckpoint) {
		t.Fatalf("unknown origin error = %v", err)
	}
}

func TestCheckpointStatusRequiresStableAttemptEvidence(t *testing.T) {
	base := checkpointForTest(CheckpointExecuting)
	base.Attempt, base.AttemptID = 0, ""
	if _, err := ValidateCheckpoint(base); err == nil {
		t.Fatal("executing checkpoint without attempt was accepted")
	}
	base = checkpointForTest(CheckpointWaitingApproval)
	base.PendingApprovalID = "approval-1"
	base.Attempt, base.AttemptID = 0, ""
	if _, err := ValidateCheckpoint(base); err == nil {
		t.Fatal("waiting checkpoint without attempt was accepted")
	}
	ready := checkpointForTest(CheckpointReady)
	ready.Attempt, ready.AttemptID = 1, "stable-attempt"
	if _, err := ValidateCheckpoint(ready); err != nil {
		t.Fatalf("approved-ready evidence rejected: %v", err)
	}
}

func TestTransitionSourceEvidenceIsBounded(t *testing.T) {
	checkpoint := checkpointForTest(CheckpointExecuting)
	transition := Transition{ID: TransitionID(checkpoint.Key, checkpoint.Revision), Key: checkpoint.Key, Revision: checkpoint.Revision, From: CheckpointReady, To: CheckpointExecuting, SegmentID: checkpoint.SegmentID, CurrentNodeID: checkpoint.CurrentNodeID, AttemptID: checkpoint.AttemptID, SourceNodeID: "start", SourceAttemptID: "stable-attempt"}
	if _, err := ValidateTransition(transition, checkpoint); err != nil {
		t.Fatal(err)
	}
	transition.SourceAttemptID = " bad"
	if _, err := ValidateTransition(transition, checkpoint); err == nil {
		t.Fatal("whitespace source attempt was accepted")
	}
}

func TestCheckpointBoundsAttemptsAndFailureEvidence(t *testing.T) {
	tooManyAttempts := checkpointForTest(CheckpointExecuting)
	tooManyAttempts.Attempt = MaxRetryAttempts + 1
	if _, err := ValidateCheckpoint(tooManyAttempts); err == nil {
		t.Fatal("checkpoint accepted attempts beyond the global retry bound")
	}

	for _, status := range []CheckpointStatus{CheckpointReady, CheckpointExecuting, CheckpointWaitingApproval, CheckpointCompleted} {
		candidate := checkpointForTest(status)
		if status == CheckpointWaitingApproval {
			candidate.PendingApprovalID = "approval-1"
		}
		candidate.FailureCode = "unexpected"
		if _, err := ValidateCheckpoint(candidate); err == nil {
			t.Fatalf("%s checkpoint accepted failure evidence", status)
		}
	}
	for _, status := range []CheckpointStatus{CheckpointFailed, CheckpointCancelled, CheckpointUnknown} {
		candidate := checkpointForTest(status)
		if _, err := ValidateCheckpoint(candidate); err == nil {
			t.Fatalf("%s checkpoint accepted without failure evidence", status)
		}
	}
}

func TestReadyAttemptRequiresApprovedResumeTransition(t *testing.T) {
	next := checkpointForTest(CheckpointReady)
	next.AttemptID = "stable-attempt"
	transition := Transition{ID: TransitionID(next.Key, next.Revision), Key: next.Key, Revision: next.Revision, From: CheckpointExecuting, To: CheckpointReady, SegmentID: next.SegmentID, CurrentNodeID: next.CurrentNodeID, AttemptID: next.AttemptID, OutcomeCode: "retry"}
	if _, err := ValidateTransition(transition, next); err == nil {
		t.Fatal("ready checkpoint retained an attempt without approved-resume evidence")
	}
	transition.From, transition.OutcomeCode = CheckpointWaitingApproval, "approval_approved"
	if _, err := ValidateTransition(transition, next); err != nil {
		t.Fatalf("approved-resume transition rejected: %v", err)
	}
}

func TestApprovalEvidenceIsBoundedAndClonedOnTransition(t *testing.T) {
	previous := checkpointForTest(CheckpointWaitingApproval)
	previous.PendingApprovalID = "approval-1"
	next := previous
	next.Revision = 2
	next.Status = CheckpointReady
	next.PendingApprovalID = ""
	evidence := &ApprovalEvidence{TenantID: previous.Key.TenantID, ApprovalID: previous.PendingApprovalID, Decision: "approved", Revision: previous.Revision, SourceSegmentID: previous.SegmentID, ActorID: "operator-1", AuthorizationBasis: "policy-1"}
	transition := Transition{ID: TransitionID(next.Key, next.Revision), Key: next.Key, Revision: next.Revision, From: previous.Status, To: next.Status, SegmentID: next.SegmentID, SourceSegmentID: previous.SegmentID, CurrentNodeID: next.CurrentNodeID, AttemptID: next.AttemptID, OutcomeCode: "approval_approved", Approval: evidence}
	validated, err := ValidateTransition(transition, next)
	if err != nil {
		t.Fatal(err)
	}
	transition.Approval.ActorID = "mutated"
	if validated.Approval == nil || validated.Approval.ActorID != "operator-1" {
		t.Fatalf("approval evidence was not defensively cloned: %#v", validated.Approval)
	}
	for _, mutate := range []func(*ApprovalEvidence){
		func(value *ApprovalEvidence) { value.TenantID = "other" },
		func(value *ApprovalEvidence) { value.Decision = "maybe" },
		func(value *ApprovalEvidence) { value.SourceSegmentID = "other-segment" },
		func(value *ApprovalEvidence) { value.AuthorizationBasis = "" },
		func(value *ApprovalEvidence) { value.Revision = 0 },
	} {
		candidate := transition
		approval := *evidence
		candidate.Approval = &approval
		mutate(candidate.Approval)
		if _, err := ValidateTransition(candidate, next); err == nil {
			t.Fatal("invalid approval evidence was accepted")
		}
	}
}

func TestResolvedApprovalIsBoundedAndClonedAcrossLiveStatuses(t *testing.T) {
	for _, status := range []CheckpointStatus{CheckpointReady, CheckpointExecuting, CheckpointUnknown, CheckpointFailed} {
		checkpoint := checkpointForTest(status)
		if status == CheckpointFailed || status == CheckpointUnknown {
			checkpoint.FailureCode = "interrupted"
		}
		if status == CheckpointFailed {
			checkpoint.AttemptID = ""
		}
		checkpoint.ResolvedApproval = &ApprovalEvidence{TenantID: "tenant", ApprovalID: "approval-1", Decision: "denied", Revision: 1, SourceSegmentID: "segment", ActorID: "operator-1", AuthorizationBasis: "policy-1"}
		clone, err := ValidateCheckpoint(checkpoint)
		if err != nil {
			t.Fatalf("status=%s: %v", status, err)
		}
		checkpoint.ResolvedApproval.ActorID = "mutated"
		if clone.ResolvedApproval == nil || clone.ResolvedApproval.ActorID != "operator-1" {
			t.Fatalf("status=%s evidence was not cloned: %#v", status, clone.ResolvedApproval)
		}
	}
	waiting := checkpointForTest(CheckpointWaitingApproval)
	waiting.PendingApprovalID = "approval-1"
	waiting.ResolvedApproval = &ApprovalEvidence{TenantID: "tenant", ApprovalID: "approval-1", Decision: "approved", Revision: 1, SourceSegmentID: "segment", ActorID: "operator-1", AuthorizationBasis: "policy-1"}
	if _, err := ValidateCheckpoint(waiting); err == nil {
		t.Fatal("waiting checkpoint accepted resolved approval")
	}
}

func checkpointForTest(status CheckpointStatus) Checkpoint {
	return Checkpoint{Key: CheckpointKey{TenantID: "tenant", SessionID: "session", RunID: "run"}, SegmentID: "segment", GraphID: "graph", DefinitionRevision: "definition", CompositionRevision: "composition", ImplementationRevision: "implementation", CurrentNodeID: "start", Attempt: 1, AttemptID: "stable-attempt", Revision: 1, Status: status, Visits: map[string]int{"start": 1}, State: State{"account": json.RawMessage(`"value"`)}}
}
