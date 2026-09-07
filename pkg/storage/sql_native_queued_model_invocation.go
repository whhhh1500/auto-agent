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

	core "github.com/cc-auto-agent/harness-core/pkg/core"
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
	if session == nil || session.Version() != expectedVersion || !reflect.DeepEqual(session.Principal(), input.Request.Principal) || !session.Scope().Equal(input.Request.Scope) {
		return nativeQueuedModelInvocation{}, completedToolResultProofInvalid()
	}
	status, exists := session.RunStatus(input.Request.RunID)
	if !exists || status != "" {
		return nativeQueuedModelInvocation{}, completedToolResultProofInvalid()
	}
	events := session.Events()
	var start core.RunStartData
	runStartSeq, stepStartSeq := int64(-1), int64(-1)
	stepIndex := -1
	for index, event := range events {
		if event.RunID != input.Request.RunID {
			if runStartSeq >= 0 {
				return nativeQueuedModelInvocation{}, completedToolResultProofInvalid()
			}
			continue
		}
		switch event.Type {
		case core.EvRunStart:
			if runStartSeq >= 0 || json.Unmarshal(event.Data, &start) != nil {
				return nativeQueuedModelInvocation{}, completedToolResultProofInvalid()
			}
			runStartSeq = int64(index)
		case core.EvRunResume, core.EvApprovalRequested, core.EvApprovalResolved:
			return nativeQueuedModelInvocation{}, completedToolResultProofInvalid()
		case core.EvAssistantMessage, core.EvToolCall, core.EvToolResult, core.EvStepError, core.EvRunError, core.EvRunEnd, core.EvUserMessage, core.EvAssistantChunk:
			if stepStartSeq >= 0 {
				return nativeQueuedModelInvocation{}, completedToolResultProofInvalid()
			}
		case core.EvStepStart:
			var data core.StepData
			if stepStartSeq >= 0 || json.Unmarshal(event.Data, &data) != nil {
				return nativeQueuedModelInvocation{}, completedToolResultProofInvalid()
			}
			stepStartSeq, stepIndex = int64(index), data.Index
		case core.EvStepEnd:
			stepStartSeq, stepIndex = -1, -1
		case core.EvRunUsage:
			var usage core.RunUsageData
			if json.Unmarshal(event.Data, &usage) != nil || stepStartSeq < 0 || len(usage.InvocationID) <= len("summary:") || !strings.HasPrefix(usage.InvocationID, "summary:") {
				return nativeQueuedModelInvocation{}, completedToolResultProofInvalid()
			}
		case core.EvContextSummary:
			if stepStartSeq < 0 {
				return nativeQueuedModelInvocation{}, completedToolResultProofInvalid()
			}
		}
	}
	if runStartSeq < 0 || stepStartSeq < 0 || stepIndex != input.Request.Step || start.Composition == nil || start.ProfileSnapshotID == "" || start.CapabilitySnapshotID == "" || start.CompositionRevision == "" || start.Composition.Profile.ID != start.ProfileSnapshotID || start.Composition.Profile.ProfileID != session.ProfileID() || !start.Composition.Profile.Scope.Equal(session.Scope()) || input.Request.Provider != start.Composition.ResolvedProvider || input.Request.Model != start.Composition.Model.Model {
		return nativeQueuedModelInvocation{}, completedToolResultProofInvalid()
	}
	compositionRevision, err := core.CompositionRevision(start.Composition)
	if err != nil || compositionRevision != start.CompositionRevision {
		return nativeQueuedModelInvocation{}, completedToolResultProofInvalid()
	}
	assignmentRevision, err := core.CompositionMetadataRevision(start.Composition.Metadata)
	if err != nil || assignmentRevision != start.AssignmentRevision {
		return nativeQueuedModelInvocation{}, completedToolResultProofInvalid()
	}
	requestJSON, err := json.Marshal(input.Request)
	if err != nil || len(requestJSON) == 0 || len(requestJSON) > 1048576 {
		return nativeQueuedModelInvocation{}, completedToolResultProofInvalid()
	}
	requestSum := sha256.Sum256(requestJSON)
	compositionSHA256, err := canonicalSHA256(start.Composition)
	if err != nil {
		return nativeQueuedModelInvocation{}, completedToolResultProofInvalid()
	}
	modelContractSHA256, err := canonicalSHA256(struct {
		BootstrapRevision, ProfileSnapshotID, CapabilitySnapshotID, CompositionRevision, AssignmentRevision string
		Model                                                                                               core.ModelSelection
		ResolvedProvider, ModelRevision                                                                     string
	}{input.BootstrapRevision, start.ProfileSnapshotID, start.CapabilitySnapshotID, compositionRevision, assignmentRevision, start.Composition.Model, start.Composition.ResolvedProvider, start.Composition.ModelRevision})
	if err != nil {
		return nativeQueuedModelInvocation{}, completedToolResultProofInvalid()
	}
	leaseHash := sha256.Sum256([]byte(fence.LeaseHolder))
	return nativeQueuedModelInvocation{input: input, invocationID: fmt.Sprintf("model:%d", stepStartSeq), stepStartSeq: stepStartSeq,
		sessionVersionAtAdmission: expectedVersion, requestJSON: string(requestJSON), requestSHA256: hex.EncodeToString(requestSum[:]),
		queueGeneration: fence.QueueGeneration, leaseHolderSHA256: hex.EncodeToString(leaseHash[:]), runStartSeq: runStartSeq,
		profileSnapshotID: start.ProfileSnapshotID, capabilitySnapshotID: start.CapabilitySnapshotID, compositionRevision: compositionRevision,
		assignmentRevision: assignmentRevision, compositionSHA256: compositionSHA256, modelContractSHA256: modelContractSHA256,
	}, nil
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
	record.requestJSON = requestJSON
	record.createdAt = time.UnixMilli(createdAt).UTC()
	if protocol != nativeQueuedModelInvocationProtocol || createdAt <= 0 || record.invocationID != fmt.Sprintf("model:%d", record.stepStartSeq) || record.sessionVersionAtAdmission <= record.stepStartSeq ||
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
