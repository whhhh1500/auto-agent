package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

var (
	sqlListNativeQueuedModelInvocationsForRun = sqlQuery{`SELECT invocation_id
		FROM native_queued_model_invocations WHERE session_id = ? AND run_id = ?
		ORDER BY step_start_seq ASC, invocation_id ASC`}
)

// RecoverNativeQueuedCompletedToolResultFenced has one sealed native-SQL
// recovery boundary. It either atomically delivers the one completed journal
// result at a canonical tool/call tail (window A), or confirms an exact V3
// sidecar-bound tool/result tail (window B). The historical witness and
// sidecar do not grant current authority: this method locks the supplied
// current epoch and queued Session fence before either result.
//
// recovered=false with a nil error means the locked Session has neither narrow
// candidate tail. A malformed candidate, missing v46 outcome for any same-Run
// v45 attempt, or any proof/fence contradiction fails closed with an error.
func (s *SQLSessionStore) RecoverNativeQueuedCompletedToolResultFenced(ctx context.Context, fence SessionWriteFence, expectedCurrentAuthorizationEpoch, expectedSessionVersion int64) (*core.Session, bool, error) {
	if ctx == nil {
		return nil, false, fmt.Errorf("native queued completed tool recovery requires a context")
	}
	if s == nil || s.db == nil {
		return nil, false, fmt.Errorf("native queued completed tool recovery requires an SQL session store")
	}
	if err := validateSessionWriteFence(fence); err != nil {
		return nil, false, err
	}
	if expectedCurrentAuthorizationEpoch < 0 || expectedSessionVersion < 0 {
		return nil, false, completedToolResultProofInvalid()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := lockAuthorizationEpoch(ctx, tx, s.dialect, expectedCurrentAuthorizationEpoch); err != nil {
		return nil, false, err
	}
	header, committed, err := s.acquireCompletedToolResultRecoverySidecarFence(ctx, tx, fence, expectedSessionVersion)
	if err != nil {
		return nil, false, err
	}
	options, err := completedToolResultRecoverySidecarSessionOptions(header, fence.SessionID)
	if err != nil {
		return nil, false, err
	}
	if options.Principal.TenantID != fence.TenantID || options.Principal.SubjectID != fence.SubjectID {
		return nil, false, sessionWriteFenceLost("fenced identity does not own the session")
	}
	session, err := s.restoreFencedSession(ctx, tx, fence.SessionID, options, committed)
	if err != nil {
		return nil, false, err
	}
	events := session.Events()
	if expectedSessionVersion == 0 || len(events) == 0 {
		if committed != expectedSessionVersion {
			return nil, false, fmt.Errorf("%w: expected %d, found %d", core.ErrSessionConflict, expectedSessionVersion, committed)
		}
		if err := tx.Commit(); err != nil {
			return nil, false, err
		}
		return nil, false, nil
	}
	if err := validateNativeQueuedModelOutcomeHistory(ctx, tx, s.dialect, session, fence.SessionID, fence.RunID); err != nil {
		return nil, false, err
	}
	if events[len(events)-1].RunID != fence.RunID {
		if committed != expectedSessionVersion {
			return nil, false, fmt.Errorf("%w: expected %d, found %d", core.ErrSessionConflict, expectedSessionVersion, committed)
		}
		if err := tx.Commit(); err != nil {
			return nil, false, err
		}
		return nil, false, nil
	}
	tail := events[len(events)-1]
	switch tail.Type {
	case core.EvToolCall:
		if committed != expectedSessionVersion {
			return nil, false, fmt.Errorf("%w: expected %d, found %d", core.ErrSessionConflict, expectedSessionVersion, committed)
		}
		recovered, candidate, err := s.deliverNativeQueuedCompletedToolResult(ctx, tx, session, options, fence, expectedCurrentAuthorizationEpoch, expectedSessionVersion)
		if err != nil {
			return nil, false, err
		}
		if !candidate {
			if err := tx.Commit(); err != nil {
				return nil, false, err
			}
			return nil, false, nil
		}
		if err := tx.Commit(); err != nil {
			return nil, false, err
		}
		return recovered, true, nil
	case core.EvToolResult:
		if committed != expectedSessionVersion && committed != expectedSessionVersion+1 {
			return nil, false, fmt.Errorf("%w: expected %d or %d, found %d", core.ErrSessionConflict, expectedSessionVersion, expectedSessionVersion+1, committed)
		}
		candidate, err := s.readNativeQueuedCompletedToolResult(ctx, tx, session, options, fence)
		if err != nil {
			return nil, false, err
		}
		if !candidate {
			if committed != expectedSessionVersion {
				return nil, false, fmt.Errorf("%w: expected %d, found %d", core.ErrSessionConflict, expectedSessionVersion, committed)
			}
			if err := tx.Commit(); err != nil {
				return nil, false, err
			}
			return nil, false, nil
		}
		if err := tx.Commit(); err != nil {
			return nil, false, err
		}
		return session, true, nil
	default:
		if committed != expectedSessionVersion {
			return nil, false, fmt.Errorf("%w: expected %d, found %d", core.ErrSessionConflict, expectedSessionVersion, committed)
		}
		if err := tx.Commit(); err != nil {
			return nil, false, err
		}
		return nil, false, nil
	}
}

func (s *SQLSessionStore) deliverNativeQueuedCompletedToolResult(ctx context.Context, tx *sql.Tx, session *core.Session, options core.SessionOptions, fence SessionWriteFence, currentEpoch, expectedVersion int64) (*core.Session, bool, error) {
	tail := session.Events()[len(session.Events())-1]
	var call core.ToolCallData
	if err := json.Unmarshal(tail.Data, &call); err != nil {
		return nil, true, completedToolResultProofInvalid()
	}
	witness, found, err := loadNativeQueuedToolEffectWitnessByCall(ctx, tx, s.dialect, fence.SessionID, fence.RunID, call.CallID, true)
	if err != nil {
		return nil, true, err
	}
	if !found {
		return nil, true, completedToolResultProofInvalid()
	}
	invocation, err := core.NewToolInvocation(core.RunInfo{RunID: fence.RunID, SessionID: fence.SessionID, Principal: session.Principal()}, core.ToolCall{ID: call.CallID, Name: call.Name, Args: call.Args}, witness.input.Invocation.Idempotent)
	if err != nil || !sameToolInvocation(invocation, witness.input.Invocation) {
		return nil, true, completedToolResultProofInvalid()
	}
	proof, err := s.completedNativeQueuedToolResultProof(ctx, tx, invocation)
	if err != nil {
		return nil, true, err
	}
	input, err := nativeQueuedRecoverySidecarInput(session, fence, expectedVersion, currentEpoch, witness, proof.digest)
	if err != nil {
		return nil, true, err
	}
	validated, err := validateCompletedToolResultRecoverySidecarInput(fence, expectedVersion, input)
	if err != nil {
		return nil, true, err
	}
	if err := completedToolResultRecoverySidecarInitialState(session, validated, expectedVersion, proof.digest); err != nil {
		return nil, true, err
	}
	event, err := completedToolResultEvent(expectedVersion, invocation, proof.result)
	if err != nil {
		return nil, true, err
	}
	if err := s.insertChunk(ctx, tx, fence.SessionID, expectedVersion, []core.SessionEvent{event}); err != nil {
		return nil, true, err
	}
	sidecar := CompletedToolResultRecoverySidecar{
		Protocol: completedToolResultRecoverySidecarProtocol, Input: validated.input,
		CompositionSHA256: validated.compositionHash, CallEventSeq: expectedVersion - 1,
		ResultEventSeq: expectedVersion, CreatedAt: event.Time,
	}
	if err := insertCompletedToolResultRecoverySidecar(ctx, tx, s.dialect, sidecar, validated.compositionJSON, validated.compositionHash); err != nil {
		return nil, true, err
	}
	stored, found, err := s.loadCompletedToolResultRecoverySidecar(ctx, tx, fence.SessionID, expectedVersion, true)
	if err != nil {
		return nil, true, err
	}
	if !found || !completedToolResultRecoverySidecarMatches(validated, stored, expectedVersion) || stored.CreatedAt.UnixMilli() != sidecar.CreatedAt.UnixMilli() {
		return nil, true, completedToolResultProofInvalid()
	}
	if err := validateCompletedToolResultRecoverySidecarChunk(ctx, tx, fence.SessionID, expectedVersion, event, proof.digest, s.dialect); err != nil {
		return nil, true, err
	}
	full, err := core.RestoreSession(options, append(session.Events(), event))
	if err != nil || validateCompletedToolResultRecoverySidecarDelivered(full, stored, proof.digest) != nil {
		return nil, true, completedToolResultProofInvalid()
	}
	checkedWitness, found, err := loadNativeQueuedToolEffectWitnessByCall(ctx, tx, s.dialect, fence.SessionID, fence.RunID, call.CallID, true)
	if err != nil || !found || !reflect.DeepEqual(checkedWitness, witness) {
		if err != nil {
			return nil, true, err
		}
		return nil, true, completedToolResultProofInvalid()
	}
	if _, err := s.lockCompletedToolResultProof(ctx, tx, invocation, proof.digest); err != nil {
		return nil, true, err
	}
	if err := lockAuthorizationEpoch(ctx, tx, s.dialect, currentEpoch); err != nil {
		return nil, true, err
	}
	write, err := tx.ExecContext(ctx, fencedSessionTipUpdate(s.dialect), expectedVersion+1, expectedVersion+1, fence.SessionID, expectedVersion,
		fence.TenantID, fence.SubjectID, fence.RunID, fence.SessionID, fence.TenantID, fence.SubjectID,
		fence.WorkerID, fence.QueueGeneration, fence.LeaseHolder)
	if err != nil {
		return nil, true, err
	}
	affected, err := write.RowsAffected()
	if err != nil {
		return nil, true, err
	}
	if affected != 1 {
		return nil, true, sessionWriteFenceLost("claim, cancellation state, or session lease expired before native queued completed tool recovery commit")
	}
	return full, true, nil
}

func (s *SQLSessionStore) readNativeQueuedCompletedToolResult(ctx context.Context, tx *sql.Tx, session *core.Session, options core.SessionOptions, fence SessionWriteFence) (bool, error) {
	tail := session.Events()[len(session.Events())-1]
	sidecar, found, err := s.loadCompletedToolResultRecoverySidecar(ctx, tx, fence.SessionID, tail.Seq, false)
	if err != nil || !found {
		return found, err
	}
	if tail.RunID != fence.RunID || !sameToolInvocation(sidecar.Input.Invocation, core.ToolInvocation{TenantID: fence.TenantID, SubjectID: fence.SubjectID, SessionID: fence.SessionID, RunID: fence.RunID, CallID: sidecar.Input.Invocation.CallID, CapabilityID: sidecar.Input.Invocation.CapabilityID, ArgsDigest: sidecar.Input.Invocation.ArgsDigest, Idempotent: sidecar.Input.Invocation.Idempotent}) {
		return true, completedToolResultProofInvalid()
	}
	if sidecar.ResultEventSeq != tail.Seq || sidecar.CallEventSeq+1 != sidecar.ResultEventSeq || sidecar.CallEventSeq < 1 {
		return true, completedToolResultProofInvalid()
	}
	// This preliminary sidecar lookup does not take a row lock. It only obtains
	// the immutable invocation identity required to lock v44 before the journal
	// and final sidecar row, preserving A/B's common lock order.
	witness, found, err := loadNativeQueuedToolEffectWitnessByCall(ctx, tx, s.dialect, fence.SessionID, fence.RunID, sidecar.Input.Invocation.CallID, true)
	if err != nil {
		return true, err
	}
	if !found || !sameToolInvocation(witness.input.Invocation, sidecar.Input.Invocation) {
		return true, completedToolResultProofInvalid()
	}
	before, err := core.RestoreSession(options, session.Events()[:sidecar.ResultEventSeq])
	if err != nil {
		return true, completedToolResultProofInvalid()
	}
	if _, err := validateNativeQueuedRecoveryWitness(before, fence, sidecar.ResultEventSeq, witness); err != nil {
		return true, err
	}
	proof, err := s.completedNativeQueuedToolResultProof(ctx, tx, sidecar.Input.Invocation)
	if err != nil {
		return true, err
	}
	identitySidecar := sidecar
	sidecar, found, err = s.loadCompletedToolResultRecoverySidecar(ctx, tx, fence.SessionID, tail.Seq, true)
	if err != nil || !found {
		if err != nil {
			return true, err
		}
		return true, completedToolResultProofInvalid()
	}
	if !reflect.DeepEqual(sidecar, identitySidecar) {
		return true, completedToolResultProofInvalid()
	}
	if err := validateCompletedToolResultRecoverySidecarDelivered(session, sidecar, proof.digest); err != nil {
		return true, err
	}
	return true, nil
}

func (s *SQLSessionStore) completedNativeQueuedToolResultProof(ctx context.Context, tx *sql.Tx, invocation core.ToolInvocation) (completedToolResultProof, error) {
	record, err := selectNativeQueuedToolInvocation(ctx, tx, s.dialect, invocation)
	if err != nil || record.State != core.ToolInvocationCompleted || record.Result == nil {
		if err != nil {
			return completedToolResultProof{}, err
		}
		return completedToolResultProof{}, completedToolResultProofInvalid()
	}
	digest, err := CanonicalCapabilityResultDigest(*record.Result)
	if err != nil {
		return completedToolResultProof{}, err
	}
	return s.lockCompletedToolResultProof(ctx, tx, invocation, digest)
}

func validateNativeQueuedModelOutcomeHistory(ctx context.Context, tx *sql.Tx, dialect SQLDialect, session *core.Session, sessionID, runID string) error {
	rows, err := tx.QueryContext(ctx, sqlListNativeQueuedModelInvocationsForRun.bind(dialect), sessionID, runID)
	if err != nil {
		return err
	}
	var invocationIDs []string
	for rows.Next() {
		var invocationID string
		if err := rows.Scan(&invocationID); err != nil || invocationID == "" {
			_ = rows.Close()
			return completedToolResultProofInvalid()
		}
		invocationIDs = append(invocationIDs, invocationID)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, invocationID := range invocationIDs {
		attempt, found, err := loadNativeQueuedModelInvocation(ctx, tx, dialect, sessionID, runID, invocationID, false)
		if err != nil || !found {
			return completedToolResultProofInvalid()
		}
		outcome, found, err := loadNativeQueuedModelOutcome(ctx, tx, dialect, sessionID, runID, invocationID, false)
		if err != nil || !found || outcome.attemptRequestSHA256 != attempt.requestSHA256 {
			return completedToolResultProofInvalid()
		}
		if _, err := nativeQueuedModelOutcomePrefixStart(ctx, tx, dialect, sessionID, session, attempt, outcome); err != nil {
			return err
		}
	}
	return nil
}

func nativeQueuedRecoverySidecarInput(session *core.Session, fence SessionWriteFence, expectedVersion, currentEpoch int64, witness nativeQueuedToolEffectWitness, digest string) (CompletedToolResultRecoverySidecarInput, error) {
	start, err := validateNativeQueuedRecoveryWitness(session, fence, expectedVersion, witness)
	if err != nil {
		return CompletedToolResultRecoverySidecarInput{}, err
	}
	return CompletedToolResultRecoverySidecarInput{
		Composition: start.Composition, ProfileSnapshotID: start.ProfileSnapshotID, CapabilitySnapshotID: start.CapabilitySnapshotID,
		CompositionRevision: start.CompositionRevision, AssignmentRevision: start.AssignmentRevision,
		Invocation: witness.input.Invocation, ResultDigest: digest, AuthorizationEpoch: currentEpoch, OriginStepSeq: witness.originStepSeq,
	}, nil
}

func validateNativeQueuedRecoveryWitness(session *core.Session, fence SessionWriteFence, expectedVersion int64, witness nativeQueuedToolEffectWitness) (core.RunStartData, error) {
	if session == nil || session.Version() != expectedVersion || witness.runStartSeq < 0 || witness.runStartSeq >= int64(len(session.Events())) {
		return core.RunStartData{}, completedToolResultProofInvalid()
	}
	startEvent := session.Events()[witness.runStartSeq]
	var start core.RunStartData
	if startEvent.RunID != fence.RunID || startEvent.Type != core.EvRunStart || json.Unmarshal(startEvent.Data, &start) != nil || start.Composition == nil {
		return core.RunStartData{}, completedToolResultProofInvalid()
	}
	if start.ProfileSnapshotID != witness.profileSnapshotID || start.CapabilitySnapshotID != witness.capabilitySnapshotID || start.CompositionRevision != witness.compositionRevision || start.AssignmentRevision != witness.assignmentRevision {
		return core.RunStartData{}, completedToolResultProofInvalid()
	}
	compositionHash, err := canonicalSHA256(start.Composition)
	if err != nil || compositionHash != witness.compositionSHA256 {
		return core.RunStartData{}, completedToolResultProofInvalid()
	}
	var capability *core.SnapshotCapability
	for index := range start.Composition.Capabilities {
		if start.Composition.Capabilities[index].Manifest.ID == witness.input.Invocation.CapabilityID {
			if capability != nil {
				return core.RunStartData{}, completedToolResultProofInvalid()
			}
			capability = &start.Composition.Capabilities[index]
		}
	}
	if capability == nil {
		return core.RunStartData{}, completedToolResultProofInvalid()
	}
	expected := NativeQueuedToolEffectWitnessInput{Invocation: witness.input.Invocation, AuthorizationEpoch: witness.input.AuthorizationEpoch, BootstrapRevision: witness.input.BootstrapRevision, ExpectedCapability: *capability}
	derived, err := deriveNativeQueuedToolEffectWitness(session, fence, expectedVersion, expected)
	if err != nil || !nativeQueuedToolEffectWitnessContractMatches(witness, derived) {
		return core.RunStartData{}, completedToolResultProofInvalid()
	}
	return start, nil
}
