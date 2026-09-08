package storage

import (
	"bytes"
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

const nativeQueuedModelInvocationProtocol = "native_queued_model_call/v1"

// NativeQueuedModelInvocationInput describes the exact main-model request a
// native queued worker wants to admit. This first storage slice proves only
// one pre-provider attempt; it has no prompt digest, canonical outcome,
// provider replay, current-account proof, or recovery authority.
type NativeQueuedModelInvocationInput struct {
	Request            core.ModelCallRequest
	AuthorizationEpoch int64
	BootstrapRevision  string
}

var (
	sqlInsertNativeQueuedModelInvocation = sqlQuery{`INSERT INTO native_queued_model_invocations
		(protocol, tenant_id, subject_id, session_id, run_id, invocation_id, step_index, step_start_seq,
		session_version_at_admission, request_json, request_sha256, authorization_epoch, queue_generation,
		lease_holder_sha256, run_start_seq, profile_snapshot_id, capability_snapshot_id,
		composition_revision, assignment_revision, composition_sha256, bootstrap_revision,
		model_contract_sha256, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (session_id, run_id, invocation_id) DO NOTHING`}
	sqlSelectNativeQueuedModelInvocation = sqlQuery{`SELECT protocol, tenant_id, subject_id, session_id,
		run_id, invocation_id, step_index, step_start_seq, session_version_at_admission, request_json,
		request_sha256, authorization_epoch, queue_generation, lease_holder_sha256, run_start_seq,
		profile_snapshot_id, capability_snapshot_id, composition_revision, assignment_revision,
		composition_sha256, bootstrap_revision, model_contract_sha256, created_at FROM native_queued_model_invocations
		WHERE session_id = ? AND run_id = ? AND invocation_id = ?`}
	sqlSelectNativeQueuedModelInvocationForUpdate = sqlQuery{`SELECT protocol, tenant_id, subject_id, session_id,
		run_id, invocation_id, step_index, step_start_seq, session_version_at_admission, request_json,
		request_sha256, authorization_epoch, queue_generation, lease_holder_sha256, run_start_seq,
		profile_snapshot_id, capability_snapshot_id, composition_revision, assignment_revision,
		composition_sha256, bootstrap_revision, model_contract_sha256, created_at FROM native_queued_model_invocations
		WHERE session_id = ? AND run_id = ? AND invocation_id = ? FOR UPDATE`}
)

type nativeQueuedModelInvocation struct {
	input                                                NativeQueuedModelInvocationInput
	invocationID, requestJSON, requestSHA256             string
	stepStartSeq, sessionVersionAtAdmission, runStartSeq int64
	queueGeneration                                      int64
	leaseHolderSHA256                                    string
	profileSnapshotID, capabilitySnapshotID              string
	compositionRevision, assignmentRevision              string
	compositionSHA256, modelContractSHA256               string
	createdAt                                            time.Time
}

// BeginNativeQueuedModelInvocationFenced atomically admits at most one
// external provider call for one durable model step. admitted=true is the only
// result that permits calling the provider. admitted=false means an exact
// attempt already exists and its outcome is unknown; callers must not issue
// the model request again. Admitted attempts are permanent replay fences in
// this slice: no age-based GC is safe before canonical outcome delivery exists.
func (s *SQLSessionStore) BeginNativeQueuedModelInvocationFenced(ctx context.Context, fence SessionWriteFence, expectedVersion int64, input NativeQueuedModelInvocationInput) (bool, error) {
	if ctx == nil {
		return false, fmt.Errorf("native queued model invocation requires a context")
	}
	if s == nil || s.db == nil {
		return false, fmt.Errorf("native queued model invocation requires an SQL session store")
	}
	if err := validateNativeQueuedModelInvocationInput(fence, expectedVersion, input); err != nil {
		return false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := lockAuthorizationEpoch(ctx, tx, s.dialect, input.AuthorizationEpoch); err != nil {
		return false, err
	}
	header, committed, err := s.acquireCompletedToolResultRecoverySidecarFence(ctx, tx, fence, expectedVersion)
	if err != nil {
		return false, err
	}
	if committed != expectedVersion {
		return false, fmt.Errorf("%w: expected %d, found %d", core.ErrSessionConflict, expectedVersion, committed)
	}
	options, err := completedToolResultRecoverySidecarSessionOptions(header, fence.SessionID)
	if err != nil {
		return false, err
	}
	if options.Principal.TenantID != fence.TenantID || options.Principal.SubjectID != fence.SubjectID {
		return false, sessionWriteFenceLost("fenced identity does not own the session")
	}
	session, err := s.restoreFencedSession(ctx, tx, fence.SessionID, options, committed)
	if err != nil {
		return false, err
	}
	expected, err := deriveNativeQueuedModelInvocation(session, fence, expectedVersion, input)
	if err != nil {
		return false, err
	}
	now := time.Now().UTC().UnixMilli()
	result, err := tx.ExecContext(ctx, sqlInsertNativeQueuedModelInvocation.bind(s.dialect), nativeQueuedModelInvocationProtocol,
		input.Request.Principal.TenantID, input.Request.Principal.SubjectID, input.Request.SessionID, input.Request.RunID,
		expected.invocationID, input.Request.Step, expected.stepStartSeq, expected.sessionVersionAtAdmission,
		expected.requestJSON, expected.requestSHA256, input.AuthorizationEpoch, fence.QueueGeneration, expected.leaseHolderSHA256,
		expected.runStartSeq, expected.profileSnapshotID, expected.capabilitySnapshotID, expected.compositionRevision,
		expected.assignmentRevision, expected.compositionSHA256, input.BootstrapRevision, expected.modelContractSHA256, now)
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	stored, found, err := loadNativeQueuedModelInvocation(ctx, tx, s.dialect, input.Request.SessionID, input.Request.RunID, expected.invocationID, true)
	if err != nil {
		return false, err
	}
	if !found || rows == 1 && !nativeQueuedModelInvocationMatches(stored, expected) || rows == 0 && !nativeQueuedModelInvocationAttemptMatches(stored, expected) || rows == 1 && stored.createdAt.UnixMilli() != now {
		return false, completedToolResultProofInvalid()
	}
	if err := lockAuthorizationEpoch(ctx, tx, s.dialect, input.AuthorizationEpoch); err != nil {
		return false, err
	}
	write, err := tx.ExecContext(ctx, nativeQueuedToolEffectFenceUpdate(s.dialect), fence.SessionID, expectedVersion, fence.TenantID, fence.SubjectID,
		fence.RunID, fence.SessionID, fence.TenantID, fence.SubjectID, fence.WorkerID, fence.QueueGeneration, fence.LeaseHolder)
	if err != nil {
		return false, err
	}
	affected, err := write.RowsAffected()
	if err != nil {
		return false, err
	}
	if affected != 1 {
		return false, sessionWriteFenceLost("claim, cancellation state, or session lease expired before native queued model admission")
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return rows == 1, nil
}

func validateNativeQueuedModelInvocationInput(fence SessionWriteFence, expectedVersion int64, input NativeQueuedModelInvocationInput) error {
	if err := validateSessionWriteFence(fence); err != nil {
		return err
	}
	if expectedVersion < 1 || input.AuthorizationEpoch < 0 || strings.TrimSpace(input.BootstrapRevision) == "" || len(input.BootstrapRevision) > 512 || strings.ContainsAny(input.BootstrapRevision, "\r\n\x00") {
		return completedToolResultProofInvalid()
	}
	if err := input.Request.Validate(); err != nil {
		return err
	}
	if input.Request.SessionID != fence.SessionID || input.Request.RunID != fence.RunID || input.Request.Principal.TenantID != fence.TenantID || input.Request.Principal.SubjectID != fence.SubjectID {
		return completedToolResultProofInvalid()
	}
	return nil
}

func deriveNativeQueuedModelInvocation(session *core.Session, fence SessionWriteFence, expectedVersion int64, input NativeQueuedModelInvocationInput) (nativeQueuedModelInvocation, error) {
	if session == nil || session.Version() != expectedVersion || !nativeQueuedModelPrincipalEqual(session.Principal(), input.Request.Principal) || !session.Scope().Equal(input.Request.Scope) {
		return nativeQueuedModelInvocation{}, completedToolResultProofInvalid()
	}
	status, exists := session.RunStatus(input.Request.RunID)
	if !exists || status != "" {
		return nativeQueuedModelInvocation{}, completedToolResultProofInvalid()
	}
	events := session.Events()
	const (
		awaitStepStart = iota
		awaitAssistant
		awaitModelUsage
		awaitToolOrStepEnd
		awaitToolResult
		awaitApprovalResume
		awaitApprovalResult
		awaitApprovalResolution
	)
	runStartSeq, stepStartSeq := int64(-1), int64(-1)
	stepIndex, nextStepIndex := -1, 0
	state := awaitStepStart
	initialUserMessage := false
	modelChunked := false
	var toolCalls []core.ToolCall
	toolCallIndex := 0
	pendingToolCallID := ""
	var start, composition nativeQueuedModelCompositionEvidence
	var approval nativeQueuedModelApproval
	for index, event := range events {
		if event.RunID != input.Request.RunID {
			if runStartSeq >= 0 {
				return nativeQueuedModelInvocation{}, completedToolResultProofInvalid()
			}
			continue
		}
		if runStartSeq < 0 {
			if event.Type != core.EvRunStart {
				return nativeQueuedModelInvocation{}, completedToolResultProofInvalid()
			}
			var err error
			start, err = nativeQueuedModelCompositionEvidenceFromEvent(event)
			if err != nil || !start.matchesSession(session) {
				return nativeQueuedModelInvocation{}, completedToolResultProofInvalid()
			}
			composition = start
			runStartSeq = int64(index)
			continue
		}
		switch event.Type {
		case core.EvRunStart:
			return nativeQueuedModelInvocation{}, completedToolResultProofInvalid()
		case core.EvRunResume:
			if state != awaitApprovalResume {
				return nativeQueuedModelInvocation{}, completedToolResultProofInvalid()
			}
			resume, err := nativeQueuedModelCompositionEvidenceFromEvent(event)
			if err != nil || !resume.matchesSession(session) || resume.composition.Model != start.composition.Model || resume.composition.ResolvedProvider != start.composition.ResolvedProvider || resume.composition.ModelRevision != start.composition.ModelRevision {
				return nativeQueuedModelInvocation{}, completedToolResultProofInvalid()
			}
			composition = resume
			state = awaitApprovalResult
		case core.EvApprovalRequested:
			var requested core.ApprovalRequestedData
			if state != awaitToolResult || json.Unmarshal(event.Data, &requested) != nil || requested.Step != stepIndex || requested.Fast || toolCallIndex >= len(toolCalls) || !nativeQueuedModelToolCallEqual(requested.ToolCall, toolCalls[toolCallIndex]) || !nativeQueuedModelToolCallEqual(requested.ResumeCall, toolCalls[toolCallIndex]) || !nativeQueuedModelToolCallSliceEqual(requested.RemainingCalls, toolCalls[toolCallIndex+1:]) {
				return nativeQueuedModelInvocation{}, completedToolResultProofInvalid()
			}
			approval = nativeQueuedModelApproval{approvalID: requested.ApprovalID, call: requested.ToolCall}
			state = awaitApprovalResume
		case core.EvApprovalResolved:
			var resolved core.ApprovalResolvedData
			if state != awaitApprovalResolution || json.Unmarshal(event.Data, &resolved) != nil || resolved.ApprovalID != approval.approvalID || resolved.CallID != approval.call.ID || resolved.Decision != core.ApprovalApproved {
				return nativeQueuedModelInvocation{}, completedToolResultProofInvalid()
			}
			approval = nativeQueuedModelApproval{}
			toolCallIndex++
			pendingToolCallID = ""
			state = awaitToolOrStepEnd
		case core.EvUserMessage:
			if state != awaitStepStart || nextStepIndex != 0 || initialUserMessage {
				return nativeQueuedModelInvocation{}, completedToolResultProofInvalid()
			}
			initialUserMessage = true
		case core.EvStepStart:
			var data core.StepData
			if state != awaitStepStart || stepStartSeq >= 0 || json.Unmarshal(event.Data, &data) != nil || data.Index != nextStepIndex {
				return nativeQueuedModelInvocation{}, completedToolResultProofInvalid()
			}
			stepStartSeq, stepIndex = int64(index), data.Index
			state, modelChunked = awaitAssistant, false
			toolCalls, toolCallIndex, pendingToolCallID = nil, 0, ""
		case core.EvAssistantChunk:
			if state != awaitAssistant {
				return nativeQueuedModelInvocation{}, completedToolResultProofInvalid()
			}
			modelChunked = true
		case core.EvAssistantMessage:
			var message core.AssistantMessageData
			if state != awaitAssistant || json.Unmarshal(event.Data, &message) != nil {
				return nativeQueuedModelInvocation{}, completedToolResultProofInvalid()
			}
			calls := append([]core.ToolCall(nil), message.ToolCalls...)
			if message.ToolCall != nil {
				if len(calls) == 0 {
					calls = []core.ToolCall{*message.ToolCall}
				} else if !reflect.DeepEqual(calls[0], *message.ToolCall) {
					return nativeQueuedModelInvocation{}, completedToolResultProofInvalid()
				}
			}
			toolCalls = calls
			state = awaitModelUsage
		case core.EvRunUsage:
			var usage core.RunUsageData
			if json.Unmarshal(event.Data, &usage) != nil {
				return nativeQueuedModelInvocation{}, completedToolResultProofInvalid()
			}
			if state == awaitAssistant && strings.HasPrefix(usage.InvocationID, "summary:") {
				continue
			}
			if state != awaitModelUsage || usage.InvocationID != fmt.Sprintf("model:%d", stepStartSeq) {
				return nativeQueuedModelInvocation{}, completedToolResultProofInvalid()
			}
			state = awaitToolOrStepEnd
		case core.EvContextSummary:
			if state != awaitAssistant {
				return nativeQueuedModelInvocation{}, completedToolResultProofInvalid()
			}
		case core.EvToolCall:
			var call core.ToolCallData
			if state != awaitToolOrStepEnd || toolCallIndex >= len(toolCalls) || json.Unmarshal(event.Data, &call) != nil || call.CallID != toolCalls[toolCallIndex].ID || call.Name != toolCalls[toolCallIndex].Name || !reflect.DeepEqual(call.Args, toolCalls[toolCallIndex].Args) {
				return nativeQueuedModelInvocation{}, completedToolResultProofInvalid()
			}
			pendingToolCallID = call.CallID
			state = awaitToolResult
		case core.EvToolResult:
			var result core.ToolResultData
			if (state != awaitToolResult && state != awaitApprovalResult) || json.Unmarshal(event.Data, &result) != nil || result.CallID != pendingToolCallID {
				return nativeQueuedModelInvocation{}, completedToolResultProofInvalid()
			}
			if state == awaitApprovalResult {
				state = awaitApprovalResolution
				continue
			}
			toolCallIndex++
			pendingToolCallID = ""
			state = awaitToolOrStepEnd
		case core.EvStepEnd:
			var data core.StepData
			if state != awaitToolOrStepEnd || toolCallIndex != len(toolCalls) || json.Unmarshal(event.Data, &data) != nil || data.Index != stepIndex {
				return nativeQueuedModelInvocation{}, completedToolResultProofInvalid()
			}
			stepStartSeq, stepIndex = -1, -1
			nextStepIndex++
			state = awaitStepStart
		default:
			return nativeQueuedModelInvocation{}, completedToolResultProofInvalid()
		}
	}
	if runStartSeq < 0 || stepStartSeq < 0 || state != awaitAssistant || modelChunked || stepIndex != input.Request.Step || !composition.matchesSession(session) || input.Request.Provider != composition.composition.Model.Provider || input.Request.Model != composition.composition.Model.Model {
		return nativeQueuedModelInvocation{}, completedToolResultProofInvalid()
	}
	requestJSON, err := json.Marshal(input.Request)
	if err != nil || len(requestJSON) == 0 || len(requestJSON) > 1048576 {
		return nativeQueuedModelInvocation{}, completedToolResultProofInvalid()
	}
	requestSum := sha256.Sum256(requestJSON)
	compositionSHA256, err := canonicalSHA256(composition.composition)
	if err != nil {
		return nativeQueuedModelInvocation{}, completedToolResultProofInvalid()
	}
	modelContractSHA256, err := canonicalSHA256(struct {
		BootstrapRevision, ProfileSnapshotID, CapabilitySnapshotID, CompositionRevision, AssignmentRevision string
		Model                                                                                               core.ModelSelection
		ResolvedProvider, ModelRevision                                                                     string
	}{input.BootstrapRevision, composition.profileSnapshotID, composition.capabilitySnapshotID, composition.compositionRevision, composition.assignmentRevision, composition.composition.Model, composition.composition.ResolvedProvider, composition.composition.ModelRevision})
	if err != nil {
		return nativeQueuedModelInvocation{}, completedToolResultProofInvalid()
	}
	leaseHash := sha256.Sum256([]byte(fence.LeaseHolder))
	return nativeQueuedModelInvocation{input: input, invocationID: fmt.Sprintf("model:%d", stepStartSeq), stepStartSeq: stepStartSeq,
		sessionVersionAtAdmission: expectedVersion, requestJSON: string(requestJSON), requestSHA256: hex.EncodeToString(requestSum[:]),
		queueGeneration: fence.QueueGeneration, leaseHolderSHA256: hex.EncodeToString(leaseHash[:]), runStartSeq: runStartSeq,
		profileSnapshotID: composition.profileSnapshotID, capabilitySnapshotID: composition.capabilitySnapshotID, compositionRevision: composition.compositionRevision,
		assignmentRevision: composition.assignmentRevision, compositionSHA256: compositionSHA256, modelContractSHA256: modelContractSHA256,
	}, nil
}

func nativeQueuedModelPrincipalEqual(left, right core.Principal) bool {
	encodedLeft, err := json.Marshal(left)
	if err != nil {
		return false
	}
	encodedRight, err := json.Marshal(right)
	return err == nil && bytes.Equal(encodedLeft, encodedRight)
}

type nativeQueuedModelCompositionEvidence struct {
	profileSnapshotID, capabilitySnapshotID, compositionRevision, assignmentRevision string
	composition                                                                      *core.RunCompositionData
}

type nativeQueuedModelApproval struct {
	approvalID string
	call       core.ToolCall
}

func nativeQueuedModelCompositionEvidenceFromEvent(event core.SessionEvent) (nativeQueuedModelCompositionEvidence, error) {
	var profileSnapshotID, capabilitySnapshotID, compositionRevision, assignmentRevision string
	var composition *core.RunCompositionData
	switch event.Type {
	case core.EvRunStart:
		var data core.RunStartData
		if err := json.Unmarshal(event.Data, &data); err != nil {
			return nativeQueuedModelCompositionEvidence{}, err
		}
		profileSnapshotID, capabilitySnapshotID, compositionRevision, assignmentRevision, composition = data.ProfileSnapshotID, data.CapabilitySnapshotID, data.CompositionRevision, data.AssignmentRevision, data.Composition
	case core.EvRunResume:
		var data core.RunResumeData
		if err := json.Unmarshal(event.Data, &data); err != nil {
			return nativeQueuedModelCompositionEvidence{}, err
		}
		profileSnapshotID, capabilitySnapshotID, compositionRevision, assignmentRevision, composition = data.ProfileSnapshotID, data.CapabilitySnapshotID, data.CompositionRevision, data.AssignmentRevision, data.Composition
	default:
		return nativeQueuedModelCompositionEvidence{}, completedToolResultProofInvalid()
	}
	if composition == nil || profileSnapshotID == "" || capabilitySnapshotID == "" || compositionRevision == "" || composition.Profile.ID != profileSnapshotID {
		return nativeQueuedModelCompositionEvidence{}, completedToolResultProofInvalid()
	}
	calculatedComposition, err := core.CompositionRevision(composition)
	if err != nil || calculatedComposition != compositionRevision {
		return nativeQueuedModelCompositionEvidence{}, completedToolResultProofInvalid()
	}
	calculatedAssignment, err := core.CompositionMetadataRevision(composition.Metadata)
	if err != nil || calculatedAssignment != assignmentRevision {
		return nativeQueuedModelCompositionEvidence{}, completedToolResultProofInvalid()
	}
	return nativeQueuedModelCompositionEvidence{profileSnapshotID: profileSnapshotID, capabilitySnapshotID: capabilitySnapshotID, compositionRevision: compositionRevision, assignmentRevision: assignmentRevision, composition: composition}, nil
}

func (e nativeQueuedModelCompositionEvidence) matchesSession(session *core.Session) bool {
	return session != nil && e.composition != nil && e.profileSnapshotID != "" && e.capabilitySnapshotID != "" && e.compositionRevision != "" && e.composition.Profile.ID == e.profileSnapshotID && e.composition.Profile.ProfileID == session.ProfileID() && e.composition.Profile.Scope.Equal(session.Scope())
}

func nativeQueuedModelToolCallEqual(left, right core.ToolCall) bool {
	return left.ID == right.ID && left.Name == right.Name && reflect.DeepEqual(left.Args, right.Args)
}

func nativeQueuedModelToolCallSliceEqual(left, right []core.ToolCall) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if !nativeQueuedModelToolCallEqual(left[index], right[index]) {
			return false
		}
	}
	return true
}

func loadNativeQueuedModelInvocation(ctx context.Context, tx *sql.Tx, dialect SQLDialect, sessionID, runID, invocationID string, lock bool) (nativeQueuedModelInvocation, bool, error) {
	query := sqlSelectNativeQueuedModelInvocation
	if lock && dialect == SQLDialectPostgres {
		query = sqlSelectNativeQueuedModelInvocationForUpdate
	}
	var record nativeQueuedModelInvocation
	var protocol string
	var tenantID, subjectID, storedSessionID, storedRunID string
	var stepIndex int
	var requestJSON string
	var createdAt int64
	err := tx.QueryRowContext(ctx, query.bind(dialect), sessionID, runID, invocationID).Scan(&protocol,
		&tenantID, &subjectID, &storedSessionID, &storedRunID, &record.invocationID, &stepIndex, &record.stepStartSeq,
		&record.sessionVersionAtAdmission, &requestJSON, &record.requestSHA256, &record.input.AuthorizationEpoch,
		&record.queueGeneration, &record.leaseHolderSHA256, &record.runStartSeq, &record.profileSnapshotID,
		&record.capabilitySnapshotID, &record.compositionRevision, &record.assignmentRevision, &record.compositionSHA256,
		&record.input.BootstrapRevision, &record.modelContractSHA256, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nativeQueuedModelInvocation{}, false, nil
	}
	if err != nil {
		return nativeQueuedModelInvocation{}, false, err
	}
	if json.Unmarshal([]byte(requestJSON), &record.input.Request) != nil {
		return nativeQueuedModelInvocation{}, false, completedToolResultProofInvalid()
	}
	if record.input.Request.Validate() != nil {
		return nativeQueuedModelInvocation{}, false, completedToolResultProofInvalid()
	}
	record.requestJSON = requestJSON
	record.createdAt = time.UnixMilli(createdAt).UTC()
	requestSum := sha256.Sum256([]byte(requestJSON))
	if protocol != nativeQueuedModelInvocationProtocol || createdAt <= 0 || len(requestJSON) == 0 || len(requestJSON) > 1048576 || hex.EncodeToString(requestSum[:]) != record.requestSHA256 || record.invocationID != fmt.Sprintf("model:%d", record.stepStartSeq) || record.stepStartSeq < 0 || record.sessionVersionAtAdmission <= record.stepStartSeq || record.runStartSeq < 0 || record.runStartSeq > record.stepStartSeq || record.input.AuthorizationEpoch < 0 || record.queueGeneration < 1 ||
		!validCapabilityResultDigest(record.leaseHolderSHA256) || !validCapabilityResultDigest(record.compositionRevision) || record.assignmentRevision != "" && !validCapabilityResultDigest(record.assignmentRevision) || !validCapabilityResultDigest(record.compositionSHA256) || !validCapabilityResultDigest(record.modelContractSHA256) ||
		strings.TrimSpace(record.profileSnapshotID) == "" || len(record.profileSnapshotID) > 512 || strings.TrimSpace(record.capabilitySnapshotID) == "" || len(record.capabilitySnapshotID) > 512 || strings.TrimSpace(record.input.BootstrapRevision) == "" || len(record.input.BootstrapRevision) > 512 || strings.ContainsAny(record.input.BootstrapRevision, "\r\n\x00") ||
		record.input.Request.Principal.TenantID != tenantID || record.input.Request.Principal.SubjectID != subjectID || record.input.Request.SessionID != storedSessionID || record.input.Request.RunID != storedRunID || record.input.Request.Step != stepIndex {
		return nativeQueuedModelInvocation{}, false, completedToolResultProofInvalid()
	}
	return record, true, nil
}

func nativeQueuedModelInvocationMatches(stored, expected nativeQueuedModelInvocation) bool {
	return stored.invocationID == expected.invocationID && stored.requestJSON == expected.requestJSON && stored.requestSHA256 == expected.requestSHA256 &&
		stored.input.AuthorizationEpoch == expected.input.AuthorizationEpoch && stored.input.BootstrapRevision == expected.input.BootstrapRevision &&
		stored.stepStartSeq == expected.stepStartSeq && stored.sessionVersionAtAdmission == expected.sessionVersionAtAdmission &&
		stored.queueGeneration == expected.queueGeneration && stored.leaseHolderSHA256 == expected.leaseHolderSHA256 && stored.runStartSeq == expected.runStartSeq &&
		stored.profileSnapshotID == expected.profileSnapshotID && stored.capabilitySnapshotID == expected.capabilitySnapshotID &&
		stored.compositionRevision == expected.compositionRevision && stored.assignmentRevision == expected.assignmentRevision &&
		stored.compositionSHA256 == expected.compositionSHA256 && stored.modelContractSHA256 == expected.modelContractSHA256
}

func nativeQueuedModelInvocationAttemptMatches(stored, expected nativeQueuedModelInvocation) bool {
	return stored.invocationID == expected.invocationID && stored.requestJSON == expected.requestJSON && stored.requestSHA256 == expected.requestSHA256 &&
		stored.input.BootstrapRevision == expected.input.BootstrapRevision && stored.stepStartSeq == expected.stepStartSeq &&
		stored.sessionVersionAtAdmission == expected.sessionVersionAtAdmission && stored.runStartSeq == expected.runStartSeq &&
		stored.profileSnapshotID == expected.profileSnapshotID && stored.capabilitySnapshotID == expected.capabilitySnapshotID &&
		stored.compositionRevision == expected.compositionRevision && stored.assignmentRevision == expected.assignmentRevision &&
		stored.compositionSHA256 == expected.compositionSHA256 && stored.modelContractSHA256 == expected.modelContractSHA256
}
