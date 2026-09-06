package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

var _ FencedCompletedToolResultRecoveryAppender = (*SQLSessionStore)(nil)

const (
	recoveryMetadataPrefix      = "harness.recovery."
	recoveryProtocolMetadataKey = recoveryMetadataPrefix + "protocol"
	recoveryCallIDMetadataKey   = recoveryMetadataPrefix + "call_id"
	recoveryResultMetadataKey   = recoveryMetadataPrefix + "result_sha256"
	recoveryStepMetadataKey     = recoveryMetadataPrefix + "origin_step_seq"
	recoveryEpochMetadataKey    = recoveryMetadataPrefix + "authorization_epoch"
	recoveryProtocolV1          = "completed_tool_result/v1"
)

type completedToolResultRecoveryMarker struct {
	callID, digest string
	step, epoch    int64
}

// AppendCompletedToolResultRecoveryPrefixFenced atomically records the V2
// recovery prefix. It is intentionally separate from V1: callers must reload
// the authoritative Session after return before starting any continuation.
func (s *SQLSessionStore) AppendCompletedToolResultRecoveryPrefixFenced(ctx context.Context, fence SessionWriteFence, expectedVersion int64, prefix CompletedToolResultRecoveryPrefix) (bool, error) {
	marker, err := validateCompletedToolResultRecoveryPrefix(fence, expectedVersion, prefix)
	if err != nil {
		return false, err
	}
	if s == nil || s.db == nil {
		return false, fmt.Errorf("completed tool result recovery append requires an SQL session store")
	}
	if ctx == nil {
		return false, fmt.Errorf("completed tool result recovery append requires a context")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	// This is deliberately the transaction's first SQL statement. On SQLite it
	// obtains the writer reservation before the Session fence; on PostgreSQL it
	// holds the epoch row before the documented control-plane lock sequence.
	if err := lockAuthorizationEpoch(ctx, tx, s.dialect, prefix.ExpectedAuthorizationEpoch); err != nil {
		return false, err
	}
	header, committed, err := s.acquireCompletedToolResultRecoveryFence(ctx, tx, fence, expectedVersion)
	if err != nil {
		return false, err
	}
	var options core.SessionOptions
	if err := json.Unmarshal([]byte(header), &options); err != nil {
		return false, fmt.Errorf("decode session %s header for completed tool result recovery: %w", fence.SessionID, err)
	}
	if options.Principal.TenantID != fence.TenantID || options.Principal.SubjectID != fence.SubjectID {
		return false, sessionWriteFenceLost("fenced identity does not own the session")
	}
	proof, err := s.lockCompletedToolResultProof(ctx, tx, prefix.Invocation, prefix.ExpectedResultDigest)
	if err != nil {
		return false, err
	}
	committedSession, err := s.restoreFencedSession(ctx, tx, fence.SessionID, options, committed)
	if err != nil {
		return false, err
	}
	if committed == expectedVersion+2 {
		matched, err := completedToolResultRecoveryPrefixApplied(committedSession, expectedVersion, prefix, proof.digest)
		if err != nil {
			return false, err
		}
		if !matched {
			return false, completedToolResultProofInvalid()
		}
		if err := tx.Commit(); err != nil {
			return false, err
		}
		return false, nil
	}
	if committed == expectedVersion+1 {
		return false, completedToolResultProofInvalid()
	}
	if committed != expectedVersion {
		return false, fmt.Errorf("%w: expected %d, found %d", core.ErrSessionConflict, expectedVersion, committed)
	}
	if err := completedToolResultRecoveryInitialState(committedSession, prefix.Invocation, proof.digest, marker.step); err != nil {
		return false, err
	}
	staged, err := committedSession.Clone()
	if err != nil {
		return false, fmt.Errorf("clone completed tool result recovery session: %w", err)
	}
	resume, err := staged.Append(fence.RunID, core.EvRunResume, prefix.Resume)
	if err != nil {
		return false, fmt.Errorf("append completed tool result recovery resume: %w", err)
	}
	result, err := staged.Append(fence.RunID, core.EvToolResult, core.ToolResultData{CallID: prefix.Invocation.CallID, Content: proof.result.Content, OK: proof.result.OK, Metadata: proof.result.Metadata})
	if err != nil {
		return false, fmt.Errorf("append completed tool result recovery result: %w", err)
	}
	events := []core.SessionEvent{resume, result}
	if err := s.insertChunk(ctx, tx, fence.SessionID, expectedVersion, events); err != nil {
		return false, err
	}
	if err := insertRunEvidence(ctx, tx, s.dialect, fence.SessionID, options, events, s.evidenceCap()); err != nil {
		return false, fmt.Errorf("index session %s completed tool result recovery evidence: %w", fence.SessionID, err)
	}
	if err := verifyCompletedToolResultRecoveryPrefixEvidence(ctx, tx, s.dialect, fence.SessionID, options, events); err != nil {
		return false, err
	}
	if err := verifyCompletedToolResultRecoveryPrefixChunk(ctx, tx, s.dialect, fence.SessionID, expectedVersion, events); err != nil {
		return false, err
	}
	if _, err := s.lockCompletedToolResultProof(ctx, tx, prefix.Invocation, prefix.ExpectedResultDigest); err != nil {
		return false, err
	}
	// Recheck after chunk and evidence writes. This catches transaction-local
	// triggers that mutate the epoch even though the initial lock was live.
	if err := lockAuthorizationEpoch(ctx, tx, s.dialect, prefix.ExpectedAuthorizationEpoch); err != nil {
		return false, err
	}
	write, err := tx.ExecContext(ctx, fencedSessionTipUpdate(s.dialect), expectedVersion+2, expectedVersion+2, fence.SessionID, expectedVersion, fence.TenantID, fence.SubjectID, fence.RunID, fence.SessionID, fence.TenantID, fence.SubjectID, fence.WorkerID, fence.QueueGeneration, fence.LeaseHolder)
	if err != nil {
		return false, err
	}
	affected, err := write.RowsAffected()
	if err != nil {
		return false, err
	}
	if affected == 0 {
		return false, sessionWriteFenceLost("claim, cancellation state, or session lease expired before completed tool result recovery commit")
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (s *SQLSessionStore) acquireCompletedToolResultRecoveryFence(ctx context.Context, tx *sql.Tx, fence SessionWriteFence, expectedVersion int64) (string, int64, error) {
	switch s.dialect {
	case SQLDialectSQLite:
		return s.acquireSQLiteCompletedResultFence(ctx, tx, fence, expectedVersion)
	case SQLDialectPostgres:
		return s.lockPostgresSessionWriteFence(ctx, tx, fence)
	default:
		return "", 0, fmt.Errorf("unsupported SQL dialect %q", s.dialect.String())
	}
}

func validateCompletedToolResultRecoveryPrefix(fence SessionWriteFence, expectedVersion int64, prefix CompletedToolResultRecoveryPrefix) (completedToolResultRecoveryMarker, error) {
	if err := validateCompletedToolResultRequest(fence, expectedVersion, prefix.Invocation, prefix.ExpectedResultDigest); err != nil {
		return completedToolResultRecoveryMarker{}, err
	}
	if prefix.ExpectedAuthorizationEpoch < 0 {
		return completedToolResultRecoveryMarker{}, completedToolResultProofInvalid()
	}
	resume := prefix.Resume
	if resume.Composition == nil || resume.ProfileSnapshotID == "" || resume.CapabilitySnapshotID == "" || resume.CompositionRevision == "" || resume.AssignmentRevision == "" || resume.Composition.Profile.ID != resume.ProfileSnapshotID {
		return completedToolResultRecoveryMarker{}, completedToolResultProofInvalid()
	}
	compositionRevision, err := core.CompositionRevision(resume.Composition)
	if err != nil || compositionRevision != resume.CompositionRevision {
		return completedToolResultRecoveryMarker{}, completedToolResultProofInvalid()
	}
	assignmentRevision, err := core.CompositionMetadataRevision(resume.Composition.Metadata)
	if err != nil || assignmentRevision != resume.AssignmentRevision {
		return completedToolResultRecoveryMarker{}, completedToolResultProofInvalid()
	}
	marker, err := completedToolResultRecoveryMarkerFromMetadata(resume.Composition.Metadata)
	if err != nil || marker.callID != prefix.Invocation.CallID || marker.digest != prefix.ExpectedResultDigest || marker.epoch != prefix.ExpectedAuthorizationEpoch {
		return completedToolResultRecoveryMarker{}, completedToolResultProofInvalid()
	}
	return marker, nil
}

func completedToolResultRecoveryMarkerFromMetadata(metadata map[string]string) (completedToolResultRecoveryMarker, error) {
	for key := range metadata {
		if strings.HasPrefix(key, recoveryMetadataPrefix) && key != recoveryProtocolMetadataKey && key != recoveryCallIDMetadataKey && key != recoveryResultMetadataKey && key != recoveryStepMetadataKey && key != recoveryEpochMetadataKey {
			return completedToolResultRecoveryMarker{}, completedToolResultProofInvalid()
		}
	}
	callID, digest := metadata[recoveryCallIDMetadataKey], metadata[recoveryResultMetadataKey]
	step, stepOK := recoveryMarkerSequence(metadata[recoveryStepMetadataKey])
	epoch, epochOK := recoveryMarkerSequence(metadata[recoveryEpochMetadataKey])
	if metadata[recoveryProtocolMetadataKey] != recoveryProtocolV1 || !validRecoveryCallID(callID) || !stepOK || !epochOK || !validCapabilityResultDigest(digest) {
		return completedToolResultRecoveryMarker{}, completedToolResultProofInvalid()
	}
	return completedToolResultRecoveryMarker{callID: callID, digest: digest, step: step, epoch: epoch}, nil
}

func validRecoveryCallID(value string) bool {
	_, err := core.NewToolInvocation(core.RunInfo{RunID: "r", SessionID: "s", Principal: core.Principal{TenantID: "t", SubjectID: "u"}}, core.ToolCall{ID: value, Name: "recovery.tool"}, false)
	return err == nil
}

func recoveryMarkerSequence(value string) (int64, bool) {
	if value == "" || len(value) > 1 && value[0] == '0' {
		return 0, false
	}
	for _, char := range value {
		if char < '0' || char > '9' {
			return 0, false
		}
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	return parsed, err == nil
}

func completedToolResultRecoveryInitialState(session *core.Session, invocation core.ToolInvocation, digest string, stepSeq int64) error {
	_, resultCount, err := completedToolResultState(session, invocation, digest)
	if err != nil || resultCount != 0 {
		if err != nil {
			return err
		}
		return completedToolResultProofInvalid()
	}
	events := session.Events()
	if len(events) == 0 || stepSeq >= int64(len(events)) || events[stepSeq].RunID != invocation.RunID || events[stepSeq].Type != core.EvStepStart {
		return completedToolResultProofInvalid()
	}
	tail := events[len(events)-1]
	if tail.RunID != invocation.RunID || tail.Type != core.EvToolCall {
		return completedToolResultProofInvalid()
	}
	var data core.ToolCallData
	if err := json.Unmarshal(tail.Data, &data); err != nil {
		return fmt.Errorf("decode completed tool result recovery tail: %w", err)
	}
	derived, err := core.NewToolInvocation(core.RunInfo{RunID: invocation.RunID, SessionID: invocation.SessionID, Principal: core.Principal{TenantID: invocation.TenantID, SubjectID: invocation.SubjectID}}, core.ToolCall{ID: data.CallID, Name: data.Name, Args: data.Args}, invocation.Idempotent)
	if err != nil || !sameToolInvocation(derived, invocation) {
		return completedToolResultProofInvalid()
	}
	return nil
}

func completedToolResultRecoveryPrefixApplied(session *core.Session, expectedVersion int64, prefix CompletedToolResultRecoveryPrefix, digest string) (bool, error) {
	if session == nil || session.Version() != expectedVersion+2 {
		return false, nil
	}
	events := session.Events()
	if expectedVersion < 0 || expectedVersion+1 >= int64(len(events)) {
		return false, nil
	}
	resume, result := events[expectedVersion], events[expectedVersion+1]
	if resume.RunID != prefix.Invocation.RunID || resume.Type != core.EvRunResume || result.RunID != prefix.Invocation.RunID || result.Type != core.EvToolResult {
		return false, nil
	}
	want, err := json.Marshal(prefix.Resume)
	if err != nil {
		return false, err
	}
	var actual core.RunResumeData
	if err := json.Unmarshal(resume.Data, &actual); err != nil {
		return false, fmt.Errorf("decode completed tool result recovery resume: %w", err)
	}
	got, err := json.Marshal(actual)
	if err != nil || string(got) != string(want) {
		return false, err
	}
	existing, count, err := completedToolResultState(session, prefix.Invocation, digest)
	if err != nil || count != 1 || existing.Seq != expectedVersion+1 {
		return false, err
	}
	return true, nil
}

func verifyCompletedToolResultRecoveryPrefixChunk(ctx context.Context, tx *sql.Tx, dialect SQLDialect, sessionID string, startSeq int64, events []core.SessionEvent) error {
	var expected strings.Builder
	for _, event := range events {
		encoded, err := json.Marshal(event)
		if err != nil {
			return err
		}
		expected.Write(encoded)
		expected.WriteByte('\n')
	}
	query := sqlQuery{"SELECT payload FROM event_chunks WHERE session_id = ? AND start_seq = ?"}
	var actual string
	if err := tx.QueryRowContext(ctx, query.bind(dialect), sessionID, startSeq).Scan(&actual); err != nil || actual != expected.String() {
		if err != nil {
			return err
		}
		return completedToolResultProofInvalid()
	}
	return nil
}

func verifyCompletedToolResultRecoveryPrefixEvidence(ctx context.Context, tx *sql.Tx, dialect SQLDialect, sessionID string, header core.SessionOptions, events []core.SessionEvent) error {
	records, err := deriveRunEvidence(sessionID, header, events)
	if err != nil {
		return fmt.Errorf("derive completed tool result recovery evidence: %w", err)
	}
	if len(records) != 1 {
		return fmt.Errorf("completed tool result recovery produced %d evidence records, want 1", len(records))
	}
	want := records[0]
	query := sqlQuery{`SELECT kind, tenant_id, subject_id, profile_id,
		composition_revision, assignment_revision, assignment_variant, status, created_at
		FROM run_evidence WHERE session_id = ? AND run_id = ? AND segment_seq = ?`}
	var kind, tenantID, subjectID, profileID, compositionRevision, assignmentRevision, assignmentVariant, status string
	var createdAt int64
	if err := tx.QueryRowContext(ctx, query.bind(dialect), want.SessionID, want.ID, want.SegmentSeq).Scan(
		&kind, &tenantID, &subjectID, &profileID, &compositionRevision, &assignmentRevision, &assignmentVariant, &status, &createdAt,
	); err != nil {
		return fmt.Errorf("read completed tool result recovery evidence: %w", err)
	}
	if kind != string(want.Kind) || tenantID != want.TenantID || subjectID != want.SubjectID || profileID != want.ProfileID ||
		compositionRevision != want.CompositionRevision || assignmentRevision != want.AssignmentRevision || assignmentVariant != string(want.AssignmentVariant) ||
		status != want.Status || createdAt != want.CreatedAt.UnixMilli() {
		return fmt.Errorf("completed tool result recovery evidence does not match written prefix")
	}
	return nil
}
