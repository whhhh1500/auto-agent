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

const nativeQueuedModelOutcomeProtocol = "native_queued_model_outcome/v1"

var (
	sqlInsertNativeQueuedModelOutcome = sqlQuery{`INSERT INTO native_queued_model_invocation_outcomes
		(protocol, session_id, run_id, invocation_id, attempt_request_sha256, assistant_event_seq,
		usage_event_seq, session_version_after_outcome, outcome_sha256, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (session_id, run_id, invocation_id) DO NOTHING`}
	sqlSelectNativeQueuedModelOutcome = sqlQuery{`SELECT protocol, session_id, run_id, invocation_id,
		attempt_request_sha256, assistant_event_seq, usage_event_seq, session_version_after_outcome,
		outcome_sha256, created_at FROM native_queued_model_invocation_outcomes
		WHERE session_id = ? AND run_id = ? AND invocation_id = ?`}
	sqlSelectNativeQueuedModelOutcomeForUpdate = sqlQuery{`SELECT protocol, session_id, run_id, invocation_id,
		attempt_request_sha256, assistant_event_seq, usage_event_seq, session_version_after_outcome,
		outcome_sha256, created_at FROM native_queued_model_invocation_outcomes
		WHERE session_id = ? AND run_id = ? AND invocation_id = ? FOR UPDATE`}
)

type nativeQueuedModelOutcome struct {
	sessionID, runID, invocationID, attemptRequestSHA256  string
	assistantEventSeq, usageEventSeq, versionAfterOutcome int64
	outcomeSHA256                                         string
	createdAt                                             time.Time
}

// AppendNativeQueuedModelOutcomeFenced atomically appends one Core-validated
// main-model outcome suffix and its immutable v46 evidence. The final two
// events must be assistant/message then run/usage(model:<step-start-seq>).
// This method does not prove provider execution or transport receipt,
// authorize continuation, or permit provider replay.
func (s *SQLSessionStore) AppendNativeQueuedModelOutcomeFenced(ctx context.Context, fence SessionWriteFence, expectedVersion int64, events []core.SessionEvent) (bool, error) {
	if ctx == nil {
		return false, fmt.Errorf("native queued model outcome requires a context")
	}
	if s == nil || s.db == nil {
		return false, fmt.Errorf("native queued model outcome requires an SQL session store")
	}
	if err := validateSessionWriteFence(fence); err != nil {
		return false, err
	}
	if expectedVersion < 1 || len(events) < 2 {
		return false, completedToolResultProofInvalid()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	header, committed, err := s.acquireCompletedToolResultRecoverySidecarFence(ctx, tx, fence, expectedVersion)
	if err != nil {
		return false, err
	}
	options, err := completedToolResultRecoverySidecarSessionOptions(header, fence.SessionID)
	if err != nil {
		return false, err
	}
	if options.Principal.TenantID != fence.TenantID || options.Principal.SubjectID != fence.SubjectID {
		return false, sessionWriteFenceLost("fenced identity does not own the session")
	}
	targetVersion := expectedVersion + int64(len(events))
	if committed != expectedVersion && committed != targetVersion {
		return false, fmt.Errorf("%w: expected %d or %d, found %d", core.ErrSessionConflict, expectedVersion, targetVersion, committed)
	}
	session, err := s.restoreFencedSession(ctx, tx, fence.SessionID, options, committed)
	if err != nil {
		return false, err
	}
	outcome, err := deriveNativeQueuedModelOutcome(session, options, expectedVersion, events)
	if err != nil {
		return false, err
	}
	attempt, found, err := loadNativeQueuedModelInvocation(ctx, tx, s.dialect, fence.SessionID, fence.RunID, outcome.invocationID, true)
	if err != nil {
		return false, err
	}
	if !found {
		return false, completedToolResultProofInvalid()
	}
	outcome.attemptRequestSHA256 = attempt.requestSHA256
	if committed == targetVersion {
		stored, found, err := loadNativeQueuedModelOutcome(ctx, tx, s.dialect, fence.SessionID, fence.RunID, outcome.invocationID, true)
		if err != nil {
			return false, err
		}
		if !found || !nativeQueuedModelOutcomeMatches(stored, outcome) || validateNativeQueuedModelOutcomeCommitted(session, expectedVersion, events, outcome) != nil {
			return false, completedToolResultProofInvalid()
		}
		if err := tx.Commit(); err != nil {
			return false, err
		}
		return false, nil
	}
	if attempt.queueGeneration != fence.QueueGeneration || attempt.leaseHolderSHA256 != sha256String(fence.LeaseHolder) {
		return false, sessionWriteFenceLost("model attempt was admitted by another queued owner")
	}
	if err := validateNativeQueuedModelOutcomeBase(session, attempt, expectedVersion); err != nil {
		return false, err
	}
	if err := s.insertChunk(ctx, tx, fence.SessionID, expectedVersion, events); err != nil {
		return false, err
	}
	result, err := tx.ExecContext(ctx, sqlInsertNativeQueuedModelOutcome.bind(s.dialect), nativeQueuedModelOutcomeProtocol,
		outcome.sessionID, outcome.runID, outcome.invocationID, outcome.attemptRequestSHA256,
		outcome.assistantEventSeq, outcome.usageEventSeq, outcome.versionAfterOutcome, outcome.outcomeSHA256, outcome.createdAt.UnixMilli())
	if err != nil {
		return false, err
	}
	inserted, err := result.RowsAffected()
	if err != nil || inserted != 1 {
		if err != nil {
			return false, err
		}
		return false, completedToolResultProofInvalid()
	}
	stored, found, err := loadNativeQueuedModelOutcome(ctx, tx, s.dialect, fence.SessionID, fence.RunID, outcome.invocationID, true)
	if err != nil {
		return false, err
	}
	if !found || !nativeQueuedModelOutcomeMatches(stored, outcome) || stored.createdAt.UnixMilli() != outcome.createdAt.UnixMilli() {
		return false, completedToolResultProofInvalid()
	}
	if err := verifyCompletedToolResultRecoveryPrefixChunk(ctx, tx, s.dialect, fence.SessionID, expectedVersion, events); err != nil {
		return false, err
	}
	if checkedAttempt, found, err := loadNativeQueuedModelInvocation(ctx, tx, s.dialect, fence.SessionID, fence.RunID, outcome.invocationID, true); err != nil || !found || !reflect.DeepEqual(checkedAttempt, attempt) {
		if err != nil {
			return false, err
		}
		return false, completedToolResultProofInvalid()
	}
	write, err := tx.ExecContext(ctx, fencedSessionTipUpdate(s.dialect), targetVersion, targetVersion, fence.SessionID, expectedVersion,
		fence.TenantID, fence.SubjectID, fence.RunID, fence.SessionID, fence.TenantID, fence.SubjectID,
		fence.WorkerID, fence.QueueGeneration, fence.LeaseHolder)
	if err != nil {
		return false, err
	}
	affected, err := write.RowsAffected()
	if err != nil {
		return false, err
	}
	if affected != 1 {
		return false, sessionWriteFenceLost("claim, cancellation state, or session lease expired before model outcome commit")
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func deriveNativeQueuedModelOutcome(session *core.Session, options core.SessionOptions, expectedVersion int64, events []core.SessionEvent) (nativeQueuedModelOutcome, error) {
	assistant, usage := events[len(events)-2], events[len(events)-1]
	if assistant.Type != core.EvAssistantMessage || usage.Type != core.EvRunUsage || assistant.RunID == "" || assistant.RunID != usage.RunID || assistant.RunID != events[0].RunID {
		return nativeQueuedModelOutcome{}, completedToolResultProofInvalid()
	}
	for index, event := range events {
		if event.Seq != expectedVersion+int64(index) || event.RunID != assistant.RunID {
			return nativeQueuedModelOutcome{}, completedToolResultProofInvalid()
		}
		if index < len(events)-2 && event.Type != core.EvAssistantChunk {
			return nativeQueuedModelOutcome{}, completedToolResultProofInvalid()
		}
	}
	var usageData core.RunUsageData
	if json.Unmarshal(usage.Data, &usageData) != nil || !strings.HasPrefix(usageData.InvocationID, "model:") {
		return nativeQueuedModelOutcome{}, completedToolResultProofInvalid()
	}
	baseEvents := session.Events()
	if int64(len(baseEvents)) < expectedVersion {
		return nativeQueuedModelOutcome{}, completedToolResultProofInvalid()
	}
	base, err := core.RestoreSession(options, baseEvents[:expectedVersion])
	if err != nil {
		return nativeQueuedModelOutcome{}, completedToolResultProofInvalid()
	}
	full := append(base.Events(), events...)
	if _, err := core.RestoreSession(options, full); err != nil {
		return nativeQueuedModelOutcome{}, completedToolResultProofInvalid()
	}
	encoded, err := json.Marshal(events)
	if err != nil {
		return nativeQueuedModelOutcome{}, completedToolResultProofInvalid()
	}
	sum := sha256.Sum256(encoded)
	return nativeQueuedModelOutcome{sessionID: base.ID(), runID: assistant.RunID, invocationID: usageData.InvocationID,
		assistantEventSeq: assistant.Seq, usageEventSeq: usage.Seq, versionAfterOutcome: usage.Seq + 1,
		outcomeSHA256: hex.EncodeToString(sum[:]), createdAt: time.Now().UTC()}, nil
}

func validateNativeQueuedModelOutcomeBase(session *core.Session, attempt nativeQueuedModelInvocation, expectedVersion int64) error {
	if session == nil || session.Version() != expectedVersion || attempt.sessionVersionAtAdmission > expectedVersion || attempt.invocationID != fmt.Sprintf("model:%d", attempt.stepStartSeq) {
		return completedToolResultProofInvalid()
	}
	for _, event := range session.Events()[attempt.sessionVersionAtAdmission:expectedVersion] {
		if event.RunID != attempt.input.Request.RunID || event.Type != core.EvAssistantChunk {
			return completedToolResultProofInvalid()
		}
	}
	return nil
}

func validateNativeQueuedModelOutcomeCommitted(session *core.Session, expectedVersion int64, events []core.SessionEvent, outcome nativeQueuedModelOutcome) error {
	if session == nil || session.Version() != outcome.versionAfterOutcome || int64(len(session.Events())) != session.Version() {
		return completedToolResultProofInvalid()
	}
	actual := session.Events()[expectedVersion:]
	if len(actual) != len(events) {
		return completedToolResultProofInvalid()
	}
	encoded, err := json.Marshal(actual)
	if err != nil {
		return completedToolResultProofInvalid()
	}
	sum := sha256.Sum256(encoded)
	if hex.EncodeToString(sum[:]) != outcome.outcomeSHA256 {
		return completedToolResultProofInvalid()
	}
	return nil
}

func validateNativeQueuedModelOutcomeForPrune(session *core.Session, attempt nativeQueuedModelInvocation, outcome nativeQueuedModelOutcome, outcomeStartSeq int64) error {
	if session == nil || attempt.sessionVersionAtAdmission < 0 || outcomeStartSeq < attempt.sessionVersionAtAdmission || outcomeStartSeq > outcome.assistantEventSeq || outcome.assistantEventSeq < attempt.sessionVersionAtAdmission || outcome.versionAfterOutcome > session.Version() ||
		attempt.input.Request.SessionID != outcome.sessionID || attempt.input.Request.RunID != outcome.runID || attempt.invocationID != outcome.invocationID {
		return completedToolResultProofInvalid()
	}
	events := session.Events()
	eventCount := int64(len(events))
	if outcome.assistantEventSeq >= eventCount || outcome.usageEventSeq < 0 || outcome.usageEventSeq >= eventCount || outcome.versionAfterOutcome <= 0 || outcome.versionAfterOutcome > eventCount {
		return completedToolResultProofInvalid()
	}
	assistant := events[outcome.assistantEventSeq]
	usage := events[outcome.usageEventSeq]
	if assistant.Type != core.EvAssistantMessage || usage.Type != core.EvRunUsage || assistant.RunID != outcome.runID || usage.RunID != outcome.runID {
		return completedToolResultProofInvalid()
	}
	var usageData core.RunUsageData
	if json.Unmarshal(usage.Data, &usageData) != nil || usageData.InvocationID != outcome.invocationID {
		return completedToolResultProofInvalid()
	}
	for _, event := range events[outcomeStartSeq:outcome.assistantEventSeq] {
		if event.Type != core.EvAssistantChunk || event.RunID != outcome.runID {
			return completedToolResultProofInvalid()
		}
	}
	encoded, err := json.Marshal(events[outcomeStartSeq:outcome.versionAfterOutcome])
	if err != nil {
		return completedToolResultProofInvalid()
	}
	sum := sha256.Sum256(encoded)
	if hex.EncodeToString(sum[:]) != outcome.outcomeSHA256 {
		return completedToolResultProofInvalid()
	}
	return nil
}

func loadNativeQueuedModelOutcome(ctx context.Context, tx *sql.Tx, dialect SQLDialect, sessionID, runID, invocationID string, lock bool) (nativeQueuedModelOutcome, bool, error) {
	query := sqlSelectNativeQueuedModelOutcome
	if lock && dialect == SQLDialectPostgres {
		query = sqlSelectNativeQueuedModelOutcomeForUpdate
	}
	var outcome nativeQueuedModelOutcome
	var protocol string
	var createdAt int64
	err := tx.QueryRowContext(ctx, query.bind(dialect), sessionID, runID, invocationID).Scan(&protocol, &outcome.sessionID,
		&outcome.runID, &outcome.invocationID, &outcome.attemptRequestSHA256, &outcome.assistantEventSeq,
		&outcome.usageEventSeq, &outcome.versionAfterOutcome, &outcome.outcomeSHA256, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nativeQueuedModelOutcome{}, false, nil
	}
	if err != nil {
		return nativeQueuedModelOutcome{}, false, err
	}
	outcome.createdAt = time.UnixMilli(createdAt).UTC()
	if protocol != nativeQueuedModelOutcomeProtocol || outcome.sessionID != sessionID || outcome.runID != runID || outcome.invocationID != invocationID || createdAt <= 0 || outcome.assistantEventSeq < 0 || outcome.usageEventSeq <= 0 || outcome.versionAfterOutcome <= 0 || outcome.assistantEventSeq != outcome.usageEventSeq-1 || outcome.usageEventSeq != outcome.versionAfterOutcome-1 || !validCapabilityResultDigest(outcome.attemptRequestSHA256) || !validCapabilityResultDigest(outcome.outcomeSHA256) {
		return nativeQueuedModelOutcome{}, false, completedToolResultProofInvalid()
	}
	return outcome, true, nil
}

func nativeQueuedModelOutcomeMatches(stored, expected nativeQueuedModelOutcome) bool {
	return stored.sessionID == expected.sessionID && stored.runID == expected.runID && stored.invocationID == expected.invocationID &&
		stored.attemptRequestSHA256 == expected.attemptRequestSHA256 && stored.assistantEventSeq == expected.assistantEventSeq &&
		stored.usageEventSeq == expected.usageEventSeq && stored.versionAfterOutcome == expected.versionAfterOutcome && stored.outcomeSHA256 == expected.outcomeSHA256
}

func sha256String(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
