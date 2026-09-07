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

const nativeQueuedToolEffectWitnessProtocol = "native_queued_tool_effect/v1"

var (
	sqlInsertToolInvocationIfAbsent = sqlQuery{`INSERT INTO tool_invocations
		(tenant_id, subject_id, session_id, run_id, call_id, capability_id, args_digest,
		idempotent, state, result_json, error_code, started_at, updated_at, completed_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'started', '', '', ?, ?, 0)
		ON CONFLICT (session_id, run_id, call_id) DO NOTHING`}
	sqlSelectToolInvocationForUpdate = sqlQuery{`SELECT tenant_id, subject_id, session_id, run_id, call_id,
		capability_id, args_digest, idempotent, state, result_json, error_code,
		started_at, updated_at, completed_at FROM tool_invocations
		WHERE session_id = ? AND run_id = ? AND call_id = ? FOR UPDATE`}
	sqlInsertNativeQueuedToolEffectWitness = sqlQuery{`INSERT INTO native_queued_tool_effect_witnesses
		(protocol, tenant_id, subject_id, session_id, run_id, call_id, capability_id, args_digest, idempotent,
		authorization_epoch, queue_generation, lease_holder_sha256, run_start_seq, origin_step_seq,
		tool_call_seq, session_version_after_call, profile_snapshot_id, capability_snapshot_id,
		composition_revision, assignment_revision, composition_sha256, bootstrap_revision,
		capability_manifest_sha256, provider_revision, capability_contract_sha256, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (session_id, run_id, call_id, queue_generation) DO NOTHING`}
	sqlSelectNativeQueuedToolEffectWitness = sqlQuery{`SELECT protocol, tenant_id, subject_id, session_id,
		run_id, call_id, capability_id, args_digest, idempotent, authorization_epoch, queue_generation,
		lease_holder_sha256, run_start_seq, origin_step_seq, tool_call_seq, session_version_after_call,
		profile_snapshot_id, capability_snapshot_id, composition_revision, assignment_revision,
		composition_sha256, bootstrap_revision, capability_manifest_sha256, provider_revision,
		capability_contract_sha256, created_at FROM native_queued_tool_effect_witnesses
		WHERE session_id = ? AND run_id = ? AND call_id = ? AND queue_generation = ?`}
	sqlSelectNativeQueuedToolEffectWitnessForUpdate = sqlQuery{`SELECT protocol, tenant_id, subject_id, session_id,
		run_id, call_id, capability_id, args_digest, idempotent, authorization_epoch, queue_generation,
		lease_holder_sha256, run_start_seq, origin_step_seq, tool_call_seq, session_version_after_call,
		profile_snapshot_id, capability_snapshot_id, composition_revision, assignment_revision,
		composition_sha256, bootstrap_revision, capability_manifest_sha256, provider_revision,
		capability_contract_sha256, created_at FROM native_queued_tool_effect_witnesses
		WHERE session_id = ? AND run_id = ? AND call_id = ? AND queue_generation = ? FOR UPDATE`}
	sqlSelectAnyNativeQueuedToolEffectWitness = sqlQuery{`SELECT protocol, tenant_id, subject_id, session_id,
		run_id, call_id, capability_id, args_digest, idempotent, authorization_epoch, queue_generation,
		lease_holder_sha256, run_start_seq, origin_step_seq, tool_call_seq, session_version_after_call,
		profile_snapshot_id, capability_snapshot_id, composition_revision, assignment_revision,
		composition_sha256, bootstrap_revision, capability_manifest_sha256, provider_revision,
		capability_contract_sha256, created_at FROM native_queued_tool_effect_witnesses
		WHERE session_id = ? AND run_id = ? AND call_id = ? ORDER BY queue_generation DESC LIMIT 1`}
	sqlSelectAnyNativeQueuedToolEffectWitnessForUpdate = sqlQuery{`SELECT protocol, tenant_id, subject_id, session_id,
		run_id, call_id, capability_id, args_digest, idempotent, authorization_epoch, queue_generation,
		lease_holder_sha256, run_start_seq, origin_step_seq, tool_call_seq, session_version_after_call,
		profile_snapshot_id, capability_snapshot_id, composition_revision, assignment_revision,
		composition_sha256, bootstrap_revision, capability_manifest_sha256, provider_revision,
		capability_contract_sha256, created_at FROM native_queued_tool_effect_witnesses
		WHERE session_id = ? AND run_id = ? AND call_id = ? ORDER BY queue_generation DESC LIMIT 1 FOR UPDATE`}
)

type nativeQueuedToolEffectWitness struct {
	input                                       NativeQueuedToolEffectWitnessInput
	queueGeneration                             int64
	leaseHolderSHA256                           string
	runStartSeq, originStepSeq, toolCallSeq     int64
	sessionVersionAfterCall                     int64
	profileSnapshotID, capabilitySnapshotID     string
	compositionRevision, assignmentRevision     string
	compositionSHA256, capabilityManifestSHA256 string
	capabilityContractSHA256                    string
	createdAt                                   time.Time
}

// BeginNativeQueuedToolEffectFenced atomically creates or confirms one tool
// journal Begin record and its immutable native queued effect-admission
// witness. The witness proves only the SQL epoch, queued ownership, durable
// composition, and tool-call tail observed before the provider may run. It is
// not proof that the provider ran, current account authority, recovery
// eligibility, or a continuation grant.
func (s *SQLSessionStore) BeginNativeQueuedToolEffectFenced(ctx context.Context, fence SessionWriteFence, expectedVersion int64, input NativeQueuedToolEffectWitnessInput) (core.ToolInvocationRecord, core.ToolInvocationDecision, error) {
	if ctx == nil {
		return core.ToolInvocationRecord{}, "", fmt.Errorf("native queued tool effect witness requires a context")
	}
	if s == nil || s.db == nil {
		return core.ToolInvocationRecord{}, "", fmt.Errorf("native queued tool effect witness requires an SQL session store")
	}
	if err := validateNativeQueuedToolEffectWitnessInput(fence, expectedVersion, input); err != nil {
		return core.ToolInvocationRecord{}, "", err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return core.ToolInvocationRecord{}, "", err
	}
	defer func() { _ = tx.Rollback() }()
	if err := lockAuthorizationEpoch(ctx, tx, s.dialect, input.AuthorizationEpoch); err != nil {
		return core.ToolInvocationRecord{}, "", err
	}
	header, committed, err := s.acquireCompletedToolResultRecoverySidecarFence(ctx, tx, fence, expectedVersion)
	if err != nil {
		return core.ToolInvocationRecord{}, "", err
	}
	if committed != expectedVersion {
		return core.ToolInvocationRecord{}, "", fmt.Errorf("%w: expected %d, found %d", core.ErrSessionConflict, expectedVersion, committed)
	}
	options, err := completedToolResultRecoverySidecarSessionOptions(header, fence.SessionID)
	if err != nil {
		return core.ToolInvocationRecord{}, "", err
	}
	if options.Principal.TenantID != fence.TenantID || options.Principal.SubjectID != fence.SubjectID {
		return core.ToolInvocationRecord{}, "", sessionWriteFenceLost("fenced identity does not own the session")
	}
	session, err := s.restoreFencedSession(ctx, tx, fence.SessionID, options, committed)
	if err != nil {
		return core.ToolInvocationRecord{}, "", err
	}
	witness, err := deriveNativeQueuedToolEffectWitness(session, fence, expectedVersion, input)
	if err != nil {
		return core.ToolInvocationRecord{}, "", err
	}
	inserted, record, err := beginNativeQueuedToolInvocation(ctx, tx, s.dialect, input.Invocation)
	if err != nil {
		return core.ToolInvocationRecord{}, "", err
	}
	decision := core.ToolInvocationExecuteNew
	var stored nativeQueuedToolEffectWitness
	var found bool
	if !inserted {
		decisionRecord, existingDecision, err := existingToolInvocationDecision(record, input.Invocation)
		if err != nil {
			return core.ToolInvocationRecord{}, "", err
		}
		record, decision = decisionRecord, existingDecision
		existingWitness, found, err := loadAnyNativeQueuedToolEffectWitness(ctx, tx, s.dialect, input.Invocation)
		if err != nil {
			return core.ToolInvocationRecord{}, "", err
		}
		if !found || !nativeQueuedToolEffectWitnessContractMatches(existingWitness, witness) {
			return core.ToolInvocationRecord{}, "", completedToolResultProofInvalid()
		}
		stored = existingWitness
	}
	if decision == core.ToolInvocationExecuteNew || decision == core.ToolInvocationExecuteRetry {
		witnessInserted, err := insertNativeQueuedToolEffectWitness(ctx, tx, s.dialect, witness)
		if err != nil {
			return core.ToolInvocationRecord{}, "", err
		}
		if inserted && !witnessInserted {
			return core.ToolInvocationRecord{}, "", completedToolResultProofInvalid()
		}
		stored, found, err = loadNativeQueuedToolEffectWitness(ctx, tx, s.dialect, input.Invocation, fence.QueueGeneration, true)
		if err != nil {
			return core.ToolInvocationRecord{}, "", err
		}
		if !found || !nativeQueuedToolEffectWitnessMatches(stored, witness) {
			return core.ToolInvocationRecord{}, "", completedToolResultProofInvalid()
		}
		if inserted && stored.createdAt.UnixMilli() != witness.createdAt.UnixMilli() {
			return core.ToolInvocationRecord{}, "", completedToolResultProofInvalid()
		}
	}
	checked, err := selectNativeQueuedToolInvocation(ctx, tx, s.dialect, input.Invocation)
	if err != nil || !reflect.DeepEqual(checked, record) {
		if err != nil {
			return core.ToolInvocationRecord{}, "", err
		}
		return core.ToolInvocationRecord{}, "", completedToolResultProofInvalid()
	}
	if checkedWitness, found, err := loadNativeQueuedToolEffectWitness(ctx, tx, s.dialect, input.Invocation, stored.queueGeneration, true); err != nil || !found || !reflect.DeepEqual(checkedWitness, stored) {
		if err != nil {
			return core.ToolInvocationRecord{}, "", err
		}
		return core.ToolInvocationRecord{}, "", completedToolResultProofInvalid()
	}
	if err := lockAuthorizationEpoch(ctx, tx, s.dialect, input.AuthorizationEpoch); err != nil {
		return core.ToolInvocationRecord{}, "", err
	}
	write, err := tx.ExecContext(ctx, nativeQueuedToolEffectFenceUpdate(s.dialect), fence.SessionID, expectedVersion, fence.TenantID, fence.SubjectID,
		fence.RunID, fence.SessionID, fence.TenantID, fence.SubjectID, fence.WorkerID, fence.QueueGeneration, fence.LeaseHolder)
	if err != nil {
		return core.ToolInvocationRecord{}, "", err
	}
	affected, err := write.RowsAffected()
	if err != nil {
		return core.ToolInvocationRecord{}, "", err
	}
	if affected != 1 {
		return core.ToolInvocationRecord{}, "", sessionWriteFenceLost("claim, cancellation state, or session lease expired before native queued tool effect admission")
	}
	if err := tx.Commit(); err != nil {
		return core.ToolInvocationRecord{}, "", err
	}
	return core.CloneToolInvocationRecord(record), decision, nil
}

func validateNativeQueuedToolEffectWitnessInput(fence SessionWriteFence, expectedVersion int64, input NativeQueuedToolEffectWitnessInput) error {
	if err := validateSessionWriteFence(fence); err != nil {
		return err
	}
	if expectedVersion < 1 || input.AuthorizationEpoch < 0 || strings.TrimSpace(input.BootstrapRevision) == "" || len(input.BootstrapRevision) > 512 || strings.ContainsAny(input.BootstrapRevision, "\r\n\x00") {
		return completedToolResultProofInvalid()
	}
	if err := core.ValidateToolInvocation(input.Invocation); err != nil {
		return err
	}
	if input.Invocation.SessionID != fence.SessionID || input.Invocation.RunID != fence.RunID || input.Invocation.TenantID != fence.TenantID || input.Invocation.SubjectID != fence.SubjectID || input.ExpectedCapability.Manifest.ID != input.Invocation.CapabilityID || input.ExpectedCapability.Manifest.Idempotent != input.Invocation.Idempotent || input.ExpectedCapability.Source.Depth() == 0 || input.ExpectedCapability.Manifest.RequiresApproval || len(input.ExpectedCapability.ProviderRevision) > 512 {
		return completedToolResultProofInvalid()
	}
	if _, err := json.Marshal(input.ExpectedCapability); err != nil {
		return completedToolResultProofInvalid()
	}
	return nil
}

func deriveNativeQueuedToolEffectWitness(session *core.Session, fence SessionWriteFence, expectedVersion int64, input NativeQueuedToolEffectWitnessInput) (nativeQueuedToolEffectWitness, error) {
	if session == nil || session.Version() != expectedVersion {
		return nativeQueuedToolEffectWitness{}, completedToolResultProofInvalid()
	}
	status, exists := session.RunStatus(input.Invocation.RunID)
	if !exists || status != "" {
		return nativeQueuedToolEffectWitness{}, completedToolResultProofInvalid()
	}
	events := session.Events()
	if len(events) == 0 || events[len(events)-1].RunID != input.Invocation.RunID || events[len(events)-1].Type != core.EvToolCall {
		return nativeQueuedToolEffectWitness{}, completedToolResultProofInvalid()
	}
	var start core.RunStartData
	runStartSeq, originStepSeq := int64(-1), int64(-1)
	for index, event := range events {
		if event.RunID != input.Invocation.RunID {
			if runStartSeq >= 0 {
				return nativeQueuedToolEffectWitness{}, completedToolResultProofInvalid()
			}
			continue
		}
		switch event.Type {
		case core.EvRunStart:
			if runStartSeq >= 0 || json.Unmarshal(event.Data, &start) != nil {
				return nativeQueuedToolEffectWitness{}, completedToolResultProofInvalid()
			}
			runStartSeq = int64(index)
		case core.EvRunResume, core.EvApprovalRequested, core.EvApprovalResolved:
			return nativeQueuedToolEffectWitness{}, completedToolResultProofInvalid()
		case core.EvStepStart:
			originStepSeq = int64(index)
		case core.EvStepEnd:
			originStepSeq = -1
		}
	}
	toolCallSeq := int64(len(events) - 1)
	if runStartSeq < 0 || originStepSeq < 0 || start.Composition == nil || start.ProfileSnapshotID == "" || start.CapabilitySnapshotID == "" || start.CompositionRevision == "" || start.Composition.Profile.ID != start.ProfileSnapshotID || start.Composition.Profile.ProfileID != session.ProfileID() || !start.Composition.Profile.Scope.Equal(session.Scope()) {
		return nativeQueuedToolEffectWitness{}, completedToolResultProofInvalid()
	}
	if err := validateCompletedToolResultRecoverySidecarOrigin(events, input.Invocation.RunID, input.Invocation.CallID, originStepSeq, toolCallSeq); err != nil {
		return nativeQueuedToolEffectWitness{}, err
	}
	var tail core.ToolCallData
	if json.Unmarshal(events[toolCallSeq].Data, &tail) != nil {
		return nativeQueuedToolEffectWitness{}, completedToolResultProofInvalid()
	}
	derived, err := core.NewToolInvocation(core.RunInfo{RunID: input.Invocation.RunID, SessionID: input.Invocation.SessionID, Principal: session.Principal()}, core.ToolCall{ID: tail.CallID, Name: tail.Name, Args: tail.Args}, input.Invocation.Idempotent)
	if err != nil || !sameToolInvocation(derived, input.Invocation) {
		return nativeQueuedToolEffectWitness{}, completedToolResultProofInvalid()
	}
	var durableCapability *core.SnapshotCapability
	for index := range start.Composition.Capabilities {
		candidate := &start.Composition.Capabilities[index]
		if candidate.Manifest.ID == input.Invocation.CapabilityID {
			if durableCapability != nil {
				return nativeQueuedToolEffectWitness{}, completedToolResultProofInvalid()
			}
			durableCapability = candidate
		}
	}
	if durableCapability == nil {
		return nativeQueuedToolEffectWitness{}, completedToolResultProofInvalid()
	}
	durable := normalizeNativeQueuedWitnessCapability(*durableCapability)
	expected := normalizeNativeQueuedWitnessCapability(input.ExpectedCapability)
	if !reflect.DeepEqual(durable, expected) || durable.Manifest.RequiresApproval || !start.Composition.EffectivePermissions.Allows(durable.Manifest.RequiredPermissions) || !containsString(start.Composition.Profile.Capabilities, input.Invocation.CapabilityID) {
		return nativeQueuedToolEffectWitness{}, completedToolResultProofInvalid()
	}
	input.ExpectedCapability = expected
	compositionRevision, err := core.CompositionRevision(start.Composition)
	if err != nil || compositionRevision != start.CompositionRevision {
		return nativeQueuedToolEffectWitness{}, completedToolResultProofInvalid()
	}
	assignmentRevision, err := core.CompositionMetadataRevision(start.Composition.Metadata)
	if err != nil || assignmentRevision != start.AssignmentRevision {
		return nativeQueuedToolEffectWitness{}, completedToolResultProofInvalid()
	}
	compositionHash, err := canonicalSHA256(start.Composition)
	if err != nil {
		return nativeQueuedToolEffectWitness{}, completedToolResultProofInvalid()
	}
	manifestHash, err := canonicalSHA256(durable.Manifest)
	if err != nil {
		return nativeQueuedToolEffectWitness{}, completedToolResultProofInvalid()
	}
	contractHash, err := canonicalSHA256(struct {
		BootstrapRevision, ProfileSnapshotID, CapabilitySnapshotID, CompositionRevision, AssignmentRevision string
		Capability                                                                                          core.SnapshotCapability
	}{input.BootstrapRevision, start.ProfileSnapshotID, start.CapabilitySnapshotID, compositionRevision, assignmentRevision, durable})
	if err != nil {
		return nativeQueuedToolEffectWitness{}, completedToolResultProofInvalid()
	}
	leaseHash := sha256.Sum256([]byte(fence.LeaseHolder))
	return nativeQueuedToolEffectWitness{
		input: input, queueGeneration: fence.QueueGeneration, leaseHolderSHA256: hex.EncodeToString(leaseHash[:]),
		runStartSeq: runStartSeq, originStepSeq: originStepSeq, toolCallSeq: toolCallSeq, sessionVersionAfterCall: expectedVersion,
		profileSnapshotID: start.ProfileSnapshotID, capabilitySnapshotID: start.CapabilitySnapshotID,
		compositionRevision: compositionRevision, assignmentRevision: assignmentRevision, compositionSHA256: compositionHash,
		capabilityManifestSHA256: manifestHash, capabilityContractSHA256: contractHash, createdAt: time.Now().UTC(),
	}, nil
}

func normalizeNativeQueuedWitnessCapability(capability core.SnapshotCapability) core.SnapshotCapability {
	manifest := capability.Manifest
	if len(manifest.RequiredPermissions) == 0 {
		manifest.RequiredPermissions = nil
	}
	if len(manifest.RequiredCredentials) == 0 {
		manifest.RequiredCredentials = nil
	}
	if len(manifest.InputSchema) == 0 {
		manifest.InputSchema = nil
	}
	if len(manifest.OutputSchema) == 0 {
		manifest.OutputSchema = nil
	}
	if len(manifest.Metadata) == 0 {
		manifest.Metadata = nil
	}
	if manifest.Tool != nil && len(manifest.Tool.Parameters) == 0 {
		tool := *manifest.Tool
		tool.Parameters = nil
		manifest.Tool = &tool
	}
	if manifest.Execution != nil && len(manifest.Execution.Headers) == 0 {
		execution := *manifest.Execution
		execution.Headers = nil
		manifest.Execution = &execution
	}
	capability.Manifest = manifest
	return capability
}

func beginNativeQueuedToolInvocation(ctx context.Context, tx *sql.Tx, dialect SQLDialect, invocation core.ToolInvocation) (bool, core.ToolInvocationRecord, error) {
	var count int
	if err := tx.QueryRowContext(ctx, sqlCountToolInvocations.bind(dialect)).Scan(&count); err != nil {
		return false, core.ToolInvocationRecord{}, err
	}
	if count >= MaxToolInvocations {
		if record, err := selectNativeQueuedToolInvocation(ctx, tx, dialect, invocation); err == nil {
			return false, record, nil
		} else if !errors.Is(err, sql.ErrNoRows) {
			return false, core.ToolInvocationRecord{}, err
		}
		return false, core.ToolInvocationRecord{}, fmt.Errorf("tool invocations exceed maximum of %d", MaxToolInvocations)
	}
	now := time.Now().UTC()
	result, err := tx.ExecContext(ctx, sqlInsertToolInvocationIfAbsent.bind(dialect), invocation.TenantID, invocation.SubjectID, invocation.SessionID, invocation.RunID, invocation.CallID, invocation.CapabilityID, invocation.ArgsDigest, boolInt(invocation.Idempotent), now.UnixMilli(), now.UnixMilli())
	if err != nil {
		return false, core.ToolInvocationRecord{}, err
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return false, core.ToolInvocationRecord{}, err
	}
	record, err := selectNativeQueuedToolInvocation(ctx, tx, dialect, invocation)
	return inserted == 1, record, err
}

func selectNativeQueuedToolInvocation(ctx context.Context, tx *sql.Tx, dialect SQLDialect, invocation core.ToolInvocation) (core.ToolInvocationRecord, error) {
	query := sqlSelectToolInvocation
	if dialect == SQLDialectPostgres {
		query = sqlSelectToolInvocationForUpdate
	}
	record, err := scanToolInvocation(tx.QueryRowContext(ctx, query.bind(dialect), invocation.SessionID, invocation.RunID, invocation.CallID))
	if err != nil {
		return core.ToolInvocationRecord{}, err
	}
	if !sameToolInvocation(record.ToolInvocation, invocation) {
		return core.ToolInvocationRecord{}, completedToolResultProofInvalid()
	}
	return record, nil
}

func insertNativeQueuedToolEffectWitness(ctx context.Context, tx *sql.Tx, dialect SQLDialect, witness nativeQueuedToolEffectWitness) (bool, error) {
	result, err := tx.ExecContext(ctx, sqlInsertNativeQueuedToolEffectWitness.bind(dialect), nativeQueuedToolEffectWitnessProtocol,
		witness.input.Invocation.TenantID, witness.input.Invocation.SubjectID, witness.input.Invocation.SessionID, witness.input.Invocation.RunID,
		witness.input.Invocation.CallID, witness.input.Invocation.CapabilityID, witness.input.Invocation.ArgsDigest, boolInt(witness.input.Invocation.Idempotent),
		witness.input.AuthorizationEpoch, witness.queueGeneration, witness.leaseHolderSHA256, witness.runStartSeq, witness.originStepSeq,
		witness.toolCallSeq, witness.sessionVersionAfterCall, witness.profileSnapshotID, witness.capabilitySnapshotID, witness.compositionRevision,
		witness.assignmentRevision, witness.compositionSHA256, witness.input.BootstrapRevision, witness.capabilityManifestSHA256,
		witness.input.ExpectedCapability.ProviderRevision, witness.capabilityContractSHA256, witness.createdAt.UnixMilli())
	if err != nil {
		return false, err
	}
	inserted, err := result.RowsAffected()
	return inserted == 1, err
}

func loadNativeQueuedToolEffectWitness(ctx context.Context, tx *sql.Tx, dialect SQLDialect, invocation core.ToolInvocation, generation int64, lock bool) (nativeQueuedToolEffectWitness, bool, error) {
	query := sqlSelectNativeQueuedToolEffectWitness
	if lock && dialect == SQLDialectPostgres {
		query = sqlSelectNativeQueuedToolEffectWitnessForUpdate
	}
	var witness nativeQueuedToolEffectWitness
	var protocol string
	var idempotent int
	var createdAt int64
	err := tx.QueryRowContext(ctx, query.bind(dialect), invocation.SessionID, invocation.RunID, invocation.CallID, generation).Scan(&protocol,
		&witness.input.Invocation.TenantID, &witness.input.Invocation.SubjectID, &witness.input.Invocation.SessionID, &witness.input.Invocation.RunID,
		&witness.input.Invocation.CallID, &witness.input.Invocation.CapabilityID, &witness.input.Invocation.ArgsDigest, &idempotent,
		&witness.input.AuthorizationEpoch, &witness.queueGeneration, &witness.leaseHolderSHA256, &witness.runStartSeq, &witness.originStepSeq,
		&witness.toolCallSeq, &witness.sessionVersionAfterCall, &witness.profileSnapshotID, &witness.capabilitySnapshotID,
		&witness.compositionRevision, &witness.assignmentRevision, &witness.compositionSHA256, &witness.input.BootstrapRevision,
		&witness.capabilityManifestSHA256, &witness.input.ExpectedCapability.ProviderRevision, &witness.capabilityContractSHA256, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nativeQueuedToolEffectWitness{}, false, nil
	}
	if err != nil {
		return nativeQueuedToolEffectWitness{}, false, err
	}
	witness.input.Invocation.Idempotent = idempotent == 1
	witness.createdAt = time.UnixMilli(createdAt).UTC()
	if protocol != nativeQueuedToolEffectWitnessProtocol || idempotent < 0 || idempotent > 1 || createdAt <= 0 {
		return nativeQueuedToolEffectWitness{}, false, completedToolResultProofInvalid()
	}
	return witness, true, nil
}

func loadAnyNativeQueuedToolEffectWitness(ctx context.Context, tx *sql.Tx, dialect SQLDialect, invocation core.ToolInvocation) (nativeQueuedToolEffectWitness, bool, error) {
	witness, found, err := loadNativeQueuedToolEffectWitnessByCall(ctx, tx, dialect, invocation.SessionID, invocation.RunID, invocation.CallID, false)
	if err != nil || !found {
		return witness, found, err
	}
	if !sameToolInvocation(witness.input.Invocation, invocation) {
		return nativeQueuedToolEffectWitness{}, false, completedToolResultProofInvalid()
	}
	return witness, true, nil
}

func loadNativeQueuedToolEffectWitnessByCall(ctx context.Context, tx *sql.Tx, dialect SQLDialect, sessionID, runID, callID string, lock bool) (nativeQueuedToolEffectWitness, bool, error) {
	query := sqlSelectAnyNativeQueuedToolEffectWitness
	if lock && dialect == SQLDialectPostgres {
		query = sqlSelectAnyNativeQueuedToolEffectWitnessForUpdate
	}
	var witness nativeQueuedToolEffectWitness
	var protocol string
	var idempotent int
	var createdAt int64
	err := tx.QueryRowContext(ctx, query.bind(dialect), sessionID, runID, callID).Scan(&protocol,
		&witness.input.Invocation.TenantID, &witness.input.Invocation.SubjectID, &witness.input.Invocation.SessionID, &witness.input.Invocation.RunID,
		&witness.input.Invocation.CallID, &witness.input.Invocation.CapabilityID, &witness.input.Invocation.ArgsDigest, &idempotent,
		&witness.input.AuthorizationEpoch, &witness.queueGeneration, &witness.leaseHolderSHA256, &witness.runStartSeq, &witness.originStepSeq,
		&witness.toolCallSeq, &witness.sessionVersionAfterCall, &witness.profileSnapshotID, &witness.capabilitySnapshotID,
		&witness.compositionRevision, &witness.assignmentRevision, &witness.compositionSHA256, &witness.input.BootstrapRevision,
		&witness.capabilityManifestSHA256, &witness.input.ExpectedCapability.ProviderRevision, &witness.capabilityContractSHA256, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nativeQueuedToolEffectWitness{}, false, nil
	}
	if err != nil {
		return nativeQueuedToolEffectWitness{}, false, err
	}
	witness.input.Invocation.Idempotent = idempotent == 1
	witness.createdAt = time.UnixMilli(createdAt).UTC()
	if protocol != nativeQueuedToolEffectWitnessProtocol || idempotent < 0 || idempotent > 1 || witness.input.Invocation.SessionID != sessionID || witness.input.Invocation.RunID != runID || witness.input.Invocation.CallID != callID || createdAt <= 0 {
		return nativeQueuedToolEffectWitness{}, false, completedToolResultProofInvalid()
	}
	return witness, true, nil
}

func nativeQueuedToolEffectWitnessMatches(stored, expected nativeQueuedToolEffectWitness) bool {
	return sameToolInvocation(stored.input.Invocation, expected.input.Invocation) &&
		stored.input.AuthorizationEpoch == expected.input.AuthorizationEpoch && stored.input.BootstrapRevision == expected.input.BootstrapRevision &&
		stored.input.ExpectedCapability.ProviderRevision == expected.input.ExpectedCapability.ProviderRevision &&
		stored.queueGeneration == expected.queueGeneration && stored.leaseHolderSHA256 == expected.leaseHolderSHA256 &&
		stored.runStartSeq == expected.runStartSeq && stored.originStepSeq == expected.originStepSeq && stored.toolCallSeq == expected.toolCallSeq &&
		stored.sessionVersionAfterCall == expected.sessionVersionAfterCall && stored.profileSnapshotID == expected.profileSnapshotID &&
		stored.capabilitySnapshotID == expected.capabilitySnapshotID && stored.compositionRevision == expected.compositionRevision &&
		stored.assignmentRevision == expected.assignmentRevision && stored.compositionSHA256 == expected.compositionSHA256 &&
		stored.capabilityManifestSHA256 == expected.capabilityManifestSHA256 && stored.capabilityContractSHA256 == expected.capabilityContractSHA256
}

func nativeQueuedToolEffectWitnessContractMatches(stored, expected nativeQueuedToolEffectWitness) bool {
	return sameToolInvocation(stored.input.Invocation, expected.input.Invocation) && stored.input.BootstrapRevision == expected.input.BootstrapRevision &&
		stored.runStartSeq == expected.runStartSeq && stored.originStepSeq == expected.originStepSeq && stored.toolCallSeq == expected.toolCallSeq &&
		stored.sessionVersionAfterCall == expected.sessionVersionAfterCall &&
		stored.profileSnapshotID == expected.profileSnapshotID && stored.capabilitySnapshotID == expected.capabilitySnapshotID &&
		stored.compositionRevision == expected.compositionRevision && stored.assignmentRevision == expected.assignmentRevision &&
		stored.compositionSHA256 == expected.compositionSHA256 && stored.capabilityManifestSHA256 == expected.capabilityManifestSHA256 &&
		stored.input.ExpectedCapability.ProviderRevision == expected.input.ExpectedCapability.ProviderRevision &&
		stored.capabilityContractSHA256 == expected.capabilityContractSHA256
}

func canonicalSHA256(value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func nativeQueuedToolEffectFenceUpdate(dialect SQLDialect) string {
	now := "CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)"
	if dialect == SQLDialectPostgres {
		now = "CAST(EXTRACT(EPOCH FROM clock_timestamp()) * 1000 AS BIGINT)"
	}
	query := fmt.Sprintf(`UPDATE sessions SET updated_at = updated_at
		WHERE id = ? AND version = ? AND tenant_id = ? AND user_id = ? AND EXISTS (
			SELECT 1 FROM run_control rc JOIN run_queue q ON q.run_id = rc.run_id
			JOIN session_leases sl ON sl.session_id = rc.session_id
			WHERE rc.run_id = ? AND rc.session_id = ? AND rc.tenant_id = ? AND rc.subject_id = ?
			AND rc.status = 'running' AND rc.cancel_requested = 0
			AND q.worker_id = ? AND q.generation = ? AND q.lease_expires_at > %s
			AND sl.holder = ? AND sl.expires_at > %s)`, now, now)
	return (sqlQuery{query}).bind(dialect)
}
