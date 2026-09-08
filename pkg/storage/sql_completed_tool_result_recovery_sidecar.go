package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

const completedToolResultRecoverySidecarProtocol = "completed_tool_result_sidecar/v1"

var (
	sqlInsertCompletedToolResultRecoverySidecar = sqlQuery{`INSERT INTO completed_tool_result_recovery_sidecars
		(protocol, tenant_id, subject_id, session_id, run_id, call_id, capability_id, args_digest, idempotent,
		origin_step_seq, call_event_seq, result_event_seq, result_sha256, authorization_epoch,
		profile_snapshot_id, capability_snapshot_id, composition_revision, assignment_revision,
		composition_json, composition_sha256, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`}
	sqlSelectCompletedToolResultRecoverySidecar = sqlQuery{`SELECT protocol, tenant_id, subject_id, session_id, run_id,
		call_id, capability_id, args_digest, idempotent, origin_step_seq, call_event_seq, result_event_seq,
		result_sha256, authorization_epoch, profile_snapshot_id, capability_snapshot_id, composition_revision,
		assignment_revision, composition_json, composition_sha256, created_at
		FROM completed_tool_result_recovery_sidecars WHERE session_id = ? AND result_event_seq = ?`}
	sqlSelectCompletedToolResultRecoverySidecarForUpdate = sqlQuery{`SELECT protocol, tenant_id, subject_id, session_id, run_id,
		call_id, capability_id, args_digest, idempotent, origin_step_seq, call_event_seq, result_event_seq,
		result_sha256, authorization_epoch, profile_snapshot_id, capability_snapshot_id, composition_revision,
		assignment_revision, composition_json, composition_sha256, created_at
		FROM completed_tool_result_recovery_sidecars WHERE session_id = ? AND result_event_seq = ? FOR UPDATE`}
	sqlSelectEventChunkPayload = sqlQuery{`SELECT payload FROM event_chunks WHERE session_id = ? AND start_seq = ?`}
)

type completedToolResultRecoverySidecarValidated struct {
	input           CompletedToolResultRecoverySidecarInput
	compositionJSON string
	compositionHash string
}

// AppendCompletedToolResultRecoverySidecarFenced atomically delivers exactly
// one canonical tool/result event and its immutable V3 SQL sidecar. It locks
// authorization_epoch before the queued fence and rechecks the exact completed
// journal result and epoch before advancing the fenced Session tip.
//
// This method records historical delivery evidence only. It neither writes a
// run/resume event nor authorizes or starts a continuation. Generic callers
// must not treat a successful append as recovery eligibility.
func (s *SQLSessionStore) AppendCompletedToolResultRecoverySidecarFenced(ctx context.Context, fence SessionWriteFence, expectedVersion int64, input CompletedToolResultRecoverySidecarInput) (bool, error) {
	if ctx == nil {
		return false, fmt.Errorf("completed tool result recovery sidecar append requires a context")
	}
	if s == nil || s.db == nil {
		return false, fmt.Errorf("completed tool result recovery sidecar append requires an SQL session store")
	}
	validated, err := validateCompletedToolResultRecoverySidecarInput(fence, expectedVersion, input)
	if err != nil {
		return false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	// This is deliberately first: SQLite obtains its writer reservation and
	// PostgreSQL holds the epoch row before run_control, queue, lease, Session,
	// journal, event_chunks, and the immutable sidecar row.
	if err := lockAuthorizationEpoch(ctx, tx, s.dialect, validated.input.AuthorizationEpoch); err != nil {
		return false, err
	}
	header, committed, err := s.acquireCompletedToolResultRecoverySidecarFence(ctx, tx, fence, expectedVersion)
	if err != nil {
		return false, err
	}
	options, err := completedToolResultRecoverySidecarSessionOptions(header, fence.SessionID)
	if err != nil {
		return false, err
	}
	if err := validateCompletedToolResultRecoverySidecarSession(options, fence, validated); err != nil {
		return false, err
	}
	proof, err := s.lockCompletedToolResultProof(ctx, tx, validated.input.Invocation, validated.input.ResultDigest)
	if err != nil {
		return false, err
	}
	committedSession, err := s.restoreFencedSession(ctx, tx, fence.SessionID, options, committed)
	if err != nil {
		return false, err
	}
	if committed == expectedVersion+1 {
		sidecar, found, err := s.loadCompletedToolResultRecoverySidecar(ctx, tx, fence.SessionID, expectedVersion, true)
		if err != nil {
			return false, err
		}
		if !found || !completedToolResultRecoverySidecarMatches(validated, sidecar, expectedVersion) ||
			validateCompletedToolResultRecoverySidecarDelivered(committedSession, sidecar, proof.digest) != nil {
			return false, completedToolResultProofInvalid()
		}
		if err := tx.Commit(); err != nil {
			return false, err
		}
		return false, nil
	}
	if committed != expectedVersion {
		return false, fmt.Errorf("%w: expected %d, found %d", core.ErrSessionConflict, expectedVersion, committed)
	}
	if err := completedToolResultRecoverySidecarInitialState(committedSession, validated, expectedVersion, proof.digest); err != nil {
		return false, err
	}
	event, err := completedToolResultEvent(expectedVersion, validated.input.Invocation, proof.result)
	if err != nil {
		return false, err
	}
	if err := s.insertChunk(ctx, tx, fence.SessionID, expectedVersion, []core.SessionEvent{event}); err != nil {
		return false, err
	}
	sidecar := CompletedToolResultRecoverySidecar{
		Protocol:          completedToolResultRecoverySidecarProtocol,
		Input:             validated.input,
		CompositionSHA256: validated.compositionHash,
		CallEventSeq:      expectedVersion - 1, ResultEventSeq: expectedVersion,
		CreatedAt: time.Now().UTC(),
	}
	if err := insertCompletedToolResultRecoverySidecar(ctx, tx, s.dialect, sidecar, validated.compositionJSON, validated.compositionHash); err != nil {
		return false, err
	}
	storedSidecar, found, err := s.loadCompletedToolResultRecoverySidecar(ctx, tx, fence.SessionID, expectedVersion, true)
	if err != nil {
		return false, err
	}
	if !found || !completedToolResultRecoverySidecarMatches(validated, storedSidecar, expectedVersion) {
		return false, completedToolResultProofInvalid()
	}
	if err := validateCompletedToolResultRecoverySidecarChunk(ctx, tx, fence.SessionID, expectedVersion, event, proof.digest, s.dialect); err != nil {
		return false, err
	}
	// The journal and epoch are re-read after both durable writes. This rejects
	// transaction-local trigger edits before the fenced Session tip commits.
	if _, err := s.lockCompletedToolResultProof(ctx, tx, validated.input.Invocation, validated.input.ResultDigest); err != nil {
		return false, err
	}
	if err := lockAuthorizationEpoch(ctx, tx, s.dialect, validated.input.AuthorizationEpoch); err != nil {
		return false, err
	}
	write, err := tx.ExecContext(ctx, fencedSessionTipUpdate(s.dialect),
		expectedVersion+1, expectedVersion+1, fence.SessionID, expectedVersion, fence.TenantID, fence.SubjectID,
		fence.RunID, fence.SessionID, fence.TenantID, fence.SubjectID, fence.WorkerID, fence.QueueGeneration, fence.LeaseHolder,
	)
	if err != nil {
		return false, err
	}
	affected, err := write.RowsAffected()
	if err != nil {
		return false, err
	}
	if affected == 0 {
		return false, sessionWriteFenceLost("claim, cancellation state, or session lease expired before completed tool result sidecar commit")
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// LoadCompletedToolResultRecoverySidecarFenced returns an authoritative
// Session plus the exact immutable V3 sidecar only while the supplied current
// SQL epoch and queued Session fence are live. AuthorizationEpoch in the
// returned sidecar is historical delivery evidence and is intentionally not
// compared to expectedCurrentAuthorizationEpoch.
//
// This is a strict readback, not a continuation grant. It does not consume or
// acknowledge the sidecar, compose a Runtime, or authorize any future model
// or capability effect. found=false means only that the current Session tail
// has no V3 sidecar; a tool/result tail without one is proof-invalid.
func (s *SQLSessionStore) LoadCompletedToolResultRecoverySidecarFenced(ctx context.Context, fence SessionWriteFence, expectedCurrentAuthorizationEpoch, expectedSessionVersion int64, invocation core.ToolInvocation) (*core.Session, CompletedToolResultRecoverySidecar, bool, error) {
	if ctx == nil {
		return nil, CompletedToolResultRecoverySidecar{}, false, fmt.Errorf("completed tool result recovery sidecar read requires a context")
	}
	if s == nil || s.db == nil {
		return nil, CompletedToolResultRecoverySidecar{}, false, fmt.Errorf("completed tool result recovery sidecar read requires an SQL session store")
	}
	if err := validateCompletedToolResultRecoverySidecarReadRequest(fence, expectedCurrentAuthorizationEpoch, expectedSessionVersion, invocation); err != nil {
		return nil, CompletedToolResultRecoverySidecar{}, false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, CompletedToolResultRecoverySidecar{}, false, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := lockAuthorizationEpoch(ctx, tx, s.dialect, expectedCurrentAuthorizationEpoch); err != nil {
		return nil, CompletedToolResultRecoverySidecar{}, false, err
	}
	header, committed, err := s.acquireCompletedToolResultRecoverySidecarFence(ctx, tx, fence, expectedSessionVersion)
	if err != nil {
		return nil, CompletedToolResultRecoverySidecar{}, false, err
	}
	if committed != expectedSessionVersion {
		return nil, CompletedToolResultRecoverySidecar{}, false, fmt.Errorf("%w: expected %d, found %d", core.ErrSessionConflict, expectedSessionVersion, committed)
	}
	options, err := completedToolResultRecoverySidecarSessionOptions(header, fence.SessionID)
	if err != nil {
		return nil, CompletedToolResultRecoverySidecar{}, false, err
	}
	if options.Principal.TenantID != fence.TenantID || options.Principal.SubjectID != fence.SubjectID {
		return nil, CompletedToolResultRecoverySidecar{}, false, sessionWriteFenceLost("fenced identity does not own the session")
	}
	session, err := s.restoreFencedSession(ctx, tx, fence.SessionID, options, committed)
	if err != nil {
		return nil, CompletedToolResultRecoverySidecar{}, false, err
	}
	events := session.Events()
	if len(events) == 0 || events[len(events)-1].Type != core.EvToolResult {
		if err := tx.Commit(); err != nil {
			return nil, CompletedToolResultRecoverySidecar{}, false, err
		}
		return session, CompletedToolResultRecoverySidecar{}, false, nil
	}
	// The Session row is already locked, so an in-contract writer cannot change
	// this current tail before the later sidecar row lock. Locate the immutable
	// row without a row lock first solely to obtain its exact journal identity;
	// then take tool_invocations before event_chunks/sidecar, matching V3's
	// documented lock order.
	sidecar, found, err := s.loadCompletedToolResultRecoverySidecar(ctx, tx, fence.SessionID, expectedSessionVersion-1, false)
	if err != nil {
		return nil, CompletedToolResultRecoverySidecar{}, false, err
	}
	if !found || !sameToolInvocation(sidecar.Input.Invocation, invocation) {
		return nil, CompletedToolResultRecoverySidecar{}, false, completedToolResultProofInvalid()
	}
	if err := validateCompletedToolResultRecoverySidecarSession(options, fence, completedToolResultRecoverySidecarValidated{input: sidecar.Input}); err != nil {
		return nil, CompletedToolResultRecoverySidecar{}, false, completedToolResultProofInvalid()
	}
	proof, err := s.lockCompletedToolResultProof(ctx, tx, invocation, sidecar.Input.ResultDigest)
	if err != nil {
		return nil, CompletedToolResultRecoverySidecar{}, false, err
	}
	sidecar, found, err = s.loadCompletedToolResultRecoverySidecar(ctx, tx, fence.SessionID, expectedSessionVersion-1, true)
	if err != nil {
		return nil, CompletedToolResultRecoverySidecar{}, false, err
	}
	if !found || !sameToolInvocation(sidecar.Input.Invocation, invocation) {
		return nil, CompletedToolResultRecoverySidecar{}, false, completedToolResultProofInvalid()
	}
	if err := validateCompletedToolResultRecoverySidecarDelivered(session, sidecar, proof.digest); err != nil {
		return nil, CompletedToolResultRecoverySidecar{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return nil, CompletedToolResultRecoverySidecar{}, false, err
	}
	return session, sidecar, true, nil
}

func (s *SQLSessionStore) acquireCompletedToolResultRecoverySidecarFence(ctx context.Context, tx *sql.Tx, fence SessionWriteFence, expectedVersion int64) (string, int64, error) {
	switch s.dialect {
	case SQLDialectSQLite:
		return s.acquireSQLiteCompletedResultFence(ctx, tx, fence, expectedVersion)
	case SQLDialectPostgres:
		return s.lockPostgresSessionWriteFence(ctx, tx, fence)
	default:
		return "", 0, fmt.Errorf("unsupported SQL dialect %q", s.dialect.String())
	}
}

func validateCompletedToolResultRecoverySidecarInput(fence SessionWriteFence, expectedVersion int64, input CompletedToolResultRecoverySidecarInput) (completedToolResultRecoverySidecarValidated, error) {
	if err := validateCompletedToolResultRequest(fence, expectedVersion, input.Invocation, input.ResultDigest); err != nil {
		return completedToolResultRecoverySidecarValidated{}, err
	}
	if input.AuthorizationEpoch < 0 || input.OriginStepSeq < 0 || input.Composition == nil || input.ProfileSnapshotID == "" || input.CapabilitySnapshotID == "" || input.CompositionRevision == "" || input.AssignmentRevision == "" {
		return completedToolResultRecoverySidecarValidated{}, completedToolResultProofInvalid()
	}
	if input.Composition.Profile.ID != input.ProfileSnapshotID || input.Composition.Profile.ProfileID == "" || input.Composition.Profile.Scope.Depth() == 0 {
		return completedToolResultRecoverySidecarValidated{}, completedToolResultProofInvalid()
	}
	for key := range input.Composition.Metadata {
		if strings.HasPrefix(key, recoveryMetadataPrefix) {
			return completedToolResultRecoverySidecarValidated{}, completedToolResultProofInvalid()
		}
	}
	compositionRevision, err := core.CompositionRevision(input.Composition)
	if err != nil || compositionRevision != input.CompositionRevision {
		return completedToolResultRecoverySidecarValidated{}, completedToolResultProofInvalid()
	}
	assignmentRevision, err := core.CompositionMetadataRevision(input.Composition.Metadata)
	if err != nil || assignmentRevision == "" || assignmentRevision != input.AssignmentRevision {
		return completedToolResultRecoverySidecarValidated{}, completedToolResultProofInvalid()
	}
	encoded, err := json.Marshal(input.Composition)
	if err != nil || len(encoded) == 0 || len(encoded) > 1048576 {
		return completedToolResultRecoverySidecarValidated{}, completedToolResultProofInvalid()
	}
	sum := sha256.Sum256(encoded)
	copyOf := input
	copyOf.Composition = nil
	if err := json.Unmarshal(encoded, &copyOf.Composition); err != nil {
		return completedToolResultRecoverySidecarValidated{}, completedToolResultProofInvalid()
	}
	return completedToolResultRecoverySidecarValidated{
		input: copyOf, compositionJSON: string(encoded), compositionHash: hex.EncodeToString(sum[:]),
	}, nil
}

func validateCompletedToolResultRecoverySidecarReadRequest(fence SessionWriteFence, expectedCurrentAuthorizationEpoch, expectedSessionVersion int64, invocation core.ToolInvocation) error {
	if err := validateSessionWriteFence(fence); err != nil {
		return err
	}
	if expectedCurrentAuthorizationEpoch < 0 || expectedSessionVersion < 1 {
		return completedToolResultProofInvalid()
	}
	if err := core.ValidateToolInvocation(invocation); err != nil {
		return err
	}
	if invocation.SessionID != fence.SessionID || invocation.RunID != fence.RunID || invocation.TenantID != fence.TenantID || invocation.SubjectID != fence.SubjectID {
		return completedToolResultProofInvalid()
	}
	return nil
}

func completedToolResultRecoverySidecarSessionOptions(header, sessionID string) (core.SessionOptions, error) {
	var options core.SessionOptions
	if err := json.Unmarshal([]byte(header), &options); err != nil {
		return core.SessionOptions{}, fmt.Errorf("decode session %s header for completed tool result sidecar: %w", sessionID, err)
	}
	return options, nil
}

func validateCompletedToolResultRecoverySidecarSession(options core.SessionOptions, fence SessionWriteFence, validated completedToolResultRecoverySidecarValidated) error {
	if options.Principal.TenantID != fence.TenantID || options.Principal.SubjectID != fence.SubjectID {
		return sessionWriteFenceLost("fenced identity does not own the session")
	}
	composition := validated.input.Composition
	if composition == nil || composition.Profile.ProfileID != options.ProfileID || !composition.Profile.Scope.Equal(options.Scope) {
		return completedToolResultProofInvalid()
	}
	return nil
}

func completedToolResultRecoverySidecarInitialState(session *core.Session, validated completedToolResultRecoverySidecarValidated, expectedVersion int64, digest string) error {
	if session == nil || session.Version() != expectedVersion || expectedVersion < 1 {
		return completedToolResultProofInvalid()
	}
	_, resultCount, err := completedToolResultState(session, validated.input.Invocation, digest)
	if err != nil || resultCount != 0 {
		if err != nil {
			return err
		}
		return completedToolResultProofInvalid()
	}
	events := session.Events()
	tail := events[len(events)-1]
	if tail.Seq != expectedVersion-1 || tail.RunID != validated.input.Invocation.RunID || tail.Type != core.EvToolCall {
		return completedToolResultProofInvalid()
	}
	if err := validateCompletedToolResultRecoverySidecarOrigin(events, validated.input.Invocation.RunID, validated.input.Invocation.CallID, validated.input.OriginStepSeq, tail.Seq); err != nil {
		return err
	}
	var data core.ToolCallData
	if err := json.Unmarshal(tail.Data, &data); err != nil {
		return fmt.Errorf("decode completed tool result sidecar tail: %w", err)
	}
	derived, err := core.NewToolInvocation(core.RunInfo{RunID: validated.input.Invocation.RunID, SessionID: validated.input.Invocation.SessionID, Principal: core.Principal{TenantID: validated.input.Invocation.TenantID, SubjectID: validated.input.Invocation.SubjectID}}, core.ToolCall{ID: data.CallID, Name: data.Name, Args: data.Args}, validated.input.Invocation.Idempotent)
	if err != nil || !sameToolInvocation(derived, validated.input.Invocation) {
		return completedToolResultProofInvalid()
	}
	return nil
}

func validateCompletedToolResultRecoverySidecarOrigin(events []core.SessionEvent, runID, targetCallID string, originStepSeq, tailSeq int64) error {
	if originStepSeq < 0 || tailSeq <= originStepSeq || tailSeq >= int64(len(events)) {
		return completedToolResultProofInvalid()
	}
	origin := events[originStepSeq]
	if origin.Seq != originStepSeq || origin.RunID != runID || origin.Type != core.EvStepStart {
		return completedToolResultProofInvalid()
	}
	var assistant core.AssistantMessageData
	var usage core.RunUsageData
	state, pending, targetSeen := 1, false, false
	var assistantCalls []core.ToolCall
	var pendingCallID string
	callIndex := 0
	for _, event := range events[originStepSeq+1 : tailSeq+1] {
		if event.RunID != runID {
			return completedToolResultProofInvalid()
		}
		switch event.Type {
		case core.EvAssistantChunk, core.EvContextSummary:
			if state != 1 {
				return completedToolResultProofInvalid()
			}
		case core.EvRunUsage:
			if json.Unmarshal(event.Data, &usage) != nil {
				return completedToolResultProofInvalid()
			}
			if state == 1 && strings.HasPrefix(usage.InvocationID, "summary:") {
				continue
			}
			if state != 2 || usage.InvocationID != fmt.Sprintf("model:%d", originStepSeq) {
				return completedToolResultProofInvalid()
			}
			state = 3
		case core.EvAssistantMessage:
			if state != 1 || json.Unmarshal(event.Data, &assistant) != nil {
				return completedToolResultProofInvalid()
			}
			assistantCalls = append([]core.ToolCall(nil), assistant.ToolCalls...)
			if assistant.ToolCall != nil {
				if len(assistantCalls) == 0 {
					assistantCalls = []core.ToolCall{*assistant.ToolCall}
				} else if !reflect.DeepEqual(assistantCalls[0], *assistant.ToolCall) {
					return completedToolResultProofInvalid()
				}
			}
			if len(assistantCalls) == 0 {
				return completedToolResultProofInvalid()
			}
			state = 2
		case core.EvToolCall:
			if state != 3 || pending || callIndex >= len(assistantCalls) {
				return completedToolResultProofInvalid()
			}
			var call core.ToolCallData
			if json.Unmarshal(event.Data, &call) != nil || call.CallID != assistantCalls[callIndex].ID || call.Name != assistantCalls[callIndex].Name || !reflect.DeepEqual(call.Args, assistantCalls[callIndex].Args) {
				return completedToolResultProofInvalid()
			}
			pending, pendingCallID = true, call.CallID
			if event.Seq == tailSeq && call.CallID == targetCallID {
				targetSeen = true
			}
			callIndex++
		case core.EvToolResult:
			if state != 3 || !pending {
				return completedToolResultProofInvalid()
			}
			var result core.ToolResultData
			if json.Unmarshal(event.Data, &result) != nil || result.CallID != pendingCallID {
				return completedToolResultProofInvalid()
			}
			pending = false
		case core.EvStepStart, core.EvStepEnd:
			return completedToolResultProofInvalid()
		default:
			return completedToolResultProofInvalid()
		}
	}
	if state != 3 || !targetSeen || !pending {
		return completedToolResultProofInvalid()
	}
	return nil
}

func validateCompletedToolResultRecoverySidecarChunk(ctx context.Context, tx *sql.Tx, sessionID string, resultSeq int64, expected core.SessionEvent, digest string, dialect SQLDialect) error {
	var payload string
	if err := tx.QueryRowContext(ctx, sqlSelectEventChunkPayload.bind(dialect), sessionID, resultSeq).Scan(&payload); err != nil {
		return err
	}
	var actual core.SessionEvent
	if err := json.Unmarshal([]byte(strings.TrimSuffix(payload, "\n")), &actual); err != nil || actual.Seq != expected.Seq || actual.RunID != expected.RunID || actual.Type != expected.Type || string(actual.Data) != string(expected.Data) {
		return completedToolResultProofInvalid()
	}
	var data core.ToolResultData
	if err := json.Unmarshal(actual.Data, &data); err != nil {
		return completedToolResultProofInvalid()
	}
	actualDigest, err := CanonicalCapabilityResultDigest(core.CapabilityResult{Content: data.Content, OK: data.OK, Metadata: data.Metadata})
	if err != nil || actualDigest != digest {
		return completedToolResultProofInvalid()
	}
	return nil
}

func insertCompletedToolResultRecoverySidecar(ctx context.Context, tx *sql.Tx, dialect SQLDialect, sidecar CompletedToolResultRecoverySidecar, compositionJSON, compositionHash string) error {
	_, err := tx.ExecContext(ctx, sqlInsertCompletedToolResultRecoverySidecar.bind(dialect),
		sidecar.Protocol, sidecar.Input.Invocation.TenantID, sidecar.Input.Invocation.SubjectID, sidecar.Input.Invocation.SessionID,
		sidecar.Input.Invocation.RunID, sidecar.Input.Invocation.CallID, sidecar.Input.Invocation.CapabilityID, sidecar.Input.Invocation.ArgsDigest,
		boolInt(sidecar.Input.Invocation.Idempotent), sidecar.Input.OriginStepSeq, sidecar.CallEventSeq, sidecar.ResultEventSeq,
		sidecar.Input.ResultDigest, sidecar.Input.AuthorizationEpoch, sidecar.Input.ProfileSnapshotID, sidecar.Input.CapabilitySnapshotID,
		sidecar.Input.CompositionRevision, sidecar.Input.AssignmentRevision, compositionJSON, compositionHash, sidecar.CreatedAt.UnixMilli(),
	)
	return err
}

func (s *SQLSessionStore) loadCompletedToolResultRecoverySidecar(ctx context.Context, tx *sql.Tx, sessionID string, resultEventSeq int64, lock bool) (CompletedToolResultRecoverySidecar, bool, error) {
	query := sqlSelectCompletedToolResultRecoverySidecar
	if lock && s.dialect == SQLDialectPostgres {
		query = sqlSelectCompletedToolResultRecoverySidecarForUpdate
	}
	sidecar, compositionJSON, compositionHash, err := scanCompletedToolResultRecoverySidecar(tx.QueryRowContext(ctx, query.bind(s.dialect), sessionID, resultEventSeq))
	if errors.Is(err, sql.ErrNoRows) {
		return CompletedToolResultRecoverySidecar{}, false, nil
	}
	if err != nil {
		return CompletedToolResultRecoverySidecar{}, false, err
	}
	validated, err := validateCompletedToolResultRecoverySidecarInput(
		SessionWriteFence{SessionID: sidecar.Input.Invocation.SessionID, RunID: sidecar.Input.Invocation.RunID, TenantID: sidecar.Input.Invocation.TenantID, SubjectID: sidecar.Input.Invocation.SubjectID, WorkerID: "sidecar-read", QueueGeneration: 1, LeaseHolder: "sidecar-read"},
		sidecar.CallEventSeq+1, sidecar.Input,
	)
	if err != nil || compositionJSON != validated.compositionJSON || compositionHash != validated.compositionHash || sidecar.Protocol != completedToolResultRecoverySidecarProtocol || sidecar.ResultEventSeq != sidecar.CallEventSeq+1 || sidecar.CreatedAt.IsZero() {
		return CompletedToolResultRecoverySidecar{}, false, completedToolResultProofInvalid()
	}
	sidecar.CompositionSHA256 = compositionHash
	return sidecar, true, nil
}

func scanCompletedToolResultRecoverySidecar(row *sql.Row) (CompletedToolResultRecoverySidecar, string, string, error) {
	var sidecar CompletedToolResultRecoverySidecar
	var compositionJSON, compositionHash string
	var idempotent int
	var createdAt int64
	err := row.Scan(
		&sidecar.Protocol, &sidecar.Input.Invocation.TenantID, &sidecar.Input.Invocation.SubjectID, &sidecar.Input.Invocation.SessionID,
		&sidecar.Input.Invocation.RunID, &sidecar.Input.Invocation.CallID, &sidecar.Input.Invocation.CapabilityID, &sidecar.Input.Invocation.ArgsDigest,
		&idempotent, &sidecar.Input.OriginStepSeq, &sidecar.CallEventSeq, &sidecar.ResultEventSeq, &sidecar.Input.ResultDigest,
		&sidecar.Input.AuthorizationEpoch, &sidecar.Input.ProfileSnapshotID, &sidecar.Input.CapabilitySnapshotID,
		&sidecar.Input.CompositionRevision, &sidecar.Input.AssignmentRevision, &compositionJSON, &compositionHash, &createdAt,
	)
	if err != nil {
		return CompletedToolResultRecoverySidecar{}, "", "", err
	}
	if idempotent != 0 && idempotent != 1 || createdAt <= 0 {
		return CompletedToolResultRecoverySidecar{}, "", "", completedToolResultProofInvalid()
	}
	sidecar.Input.Invocation.Idempotent = idempotent == 1
	if err := json.Unmarshal([]byte(compositionJSON), &sidecar.Input.Composition); err != nil {
		return CompletedToolResultRecoverySidecar{}, "", "", completedToolResultProofInvalid()
	}
	sidecar.CreatedAt = time.UnixMilli(createdAt).UTC()
	return sidecar, compositionJSON, compositionHash, nil
}

func completedToolResultRecoverySidecarMatches(validated completedToolResultRecoverySidecarValidated, sidecar CompletedToolResultRecoverySidecar, expectedVersion int64) bool {
	return sidecar.Protocol == completedToolResultRecoverySidecarProtocol &&
		sidecar.CallEventSeq == expectedVersion-1 && sidecar.ResultEventSeq == expectedVersion &&
		sidecar.CompositionSHA256 == validated.compositionHash &&
		sameToolInvocation(sidecar.Input.Invocation, validated.input.Invocation) &&
		sidecar.Input.ResultDigest == validated.input.ResultDigest && sidecar.Input.AuthorizationEpoch == validated.input.AuthorizationEpoch &&
		sidecar.Input.OriginStepSeq == validated.input.OriginStepSeq && sidecar.Input.ProfileSnapshotID == validated.input.ProfileSnapshotID &&
		sidecar.Input.CapabilitySnapshotID == validated.input.CapabilitySnapshotID &&
		sidecar.Input.CompositionRevision == validated.input.CompositionRevision && sidecar.Input.AssignmentRevision == validated.input.AssignmentRevision
}

func validateCompletedToolResultRecoverySidecarDelivered(session *core.Session, sidecar CompletedToolResultRecoverySidecar, digest string) error {
	if session == nil || session.Version() != sidecar.ResultEventSeq+1 || sidecar.CallEventSeq+1 != sidecar.ResultEventSeq || sidecar.CallEventSeq < 0 {
		return completedToolResultProofInvalid()
	}
	events := session.Events()
	if int64(len(events)) != session.Version() || sidecar.ResultEventSeq >= int64(len(events)) {
		return completedToolResultProofInvalid()
	}
	call, result := events[sidecar.CallEventSeq], events[sidecar.ResultEventSeq]
	if call.RunID != sidecar.Input.Invocation.RunID || call.Type != core.EvToolCall || result.RunID != sidecar.Input.Invocation.RunID || result.Type != core.EvToolResult {
		return completedToolResultProofInvalid()
	}
	if err := validateCompletedToolResultRecoverySidecarOrigin(events, sidecar.Input.Invocation.RunID, sidecar.Input.Invocation.CallID, sidecar.Input.OriginStepSeq, sidecar.CallEventSeq); err != nil {
		return err
	}
	var callData core.ToolCallData
	if err := json.Unmarshal(call.Data, &callData); err != nil {
		return completedToolResultProofInvalid()
	}
	derived, err := core.NewToolInvocation(core.RunInfo{RunID: sidecar.Input.Invocation.RunID, SessionID: sidecar.Input.Invocation.SessionID, Principal: core.Principal{TenantID: sidecar.Input.Invocation.TenantID, SubjectID: sidecar.Input.Invocation.SubjectID}}, core.ToolCall{ID: callData.CallID, Name: callData.Name, Args: callData.Args}, sidecar.Input.Invocation.Idempotent)
	if err != nil || !sameToolInvocation(derived, sidecar.Input.Invocation) {
		return completedToolResultProofInvalid()
	}
	var resultData core.ToolResultData
	if err := json.Unmarshal(result.Data, &resultData); err != nil || resultData.CallID != sidecar.Input.Invocation.CallID {
		return completedToolResultProofInvalid()
	}
	resultDigest, err := CanonicalCapabilityResultDigest(core.CapabilityResult{Content: resultData.Content, OK: resultData.OK, Metadata: resultData.Metadata})
	if err != nil || resultDigest != digest || digest != sidecar.Input.ResultDigest {
		return completedToolResultProofInvalid()
	}
	_, count, err := completedToolResultState(session, sidecar.Input.Invocation, digest)
	if err != nil || count != 1 {
		if err != nil {
			return err
		}
		return completedToolResultProofInvalid()
	}
	return nil
}
