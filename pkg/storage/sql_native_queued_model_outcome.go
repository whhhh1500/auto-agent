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
	sqlListNativeQueuedModelOutcomePrefixStarts = sqlQuery{`SELECT start_seq FROM event_chunks
		WHERE session_id = ? AND start_seq >= ? AND start_seq <= ? ORDER BY start_seq`}
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
	return s.appendNativeQueuedModelOutcomeBatchFenced(ctx, fence, expectedVersion, len(events), events)
}

// AppendNativeQueuedModelOutcomeBatchFenced atomically appends one native
// queued model outcome prefix and its same-Run Session suffix. outcomeEnd is
// the number of events in the outcome prefix; that prefix contains exactly one
// assistant/message plus model usage pair and retains the v46 hash semantics.
// The complete batch is validated and committed under one fenced transaction,
// so a suffix failure cannot leave a durable outcome ahead of WriteBehind.
//
// appended is true only for a first full-batch commit. A false nil result is
// the response-lost convergence case, which requires the whole supplied batch
// and its v46 row to match exactly. A later successor never converges here.
func (s *SQLSessionStore) AppendNativeQueuedModelOutcomeBatchFenced(ctx context.Context, fence SessionWriteFence, expectedVersion int64, outcomeEnd int, events []core.SessionEvent) (bool, error) {
	return s.appendNativeQueuedModelOutcomeBatchFenced(ctx, fence, expectedVersion, outcomeEnd, events)
}

func (s *SQLSessionStore) appendNativeQueuedModelOutcomeBatchFenced(ctx context.Context, fence SessionWriteFence, expectedVersion int64, outcomeEnd int, events []core.SessionEvent) (bool, error) {
	if ctx == nil {
		return false, fmt.Errorf("native queued model outcome requires a context")
	}
	if s == nil || s.db == nil {
		return false, fmt.Errorf("native queued model outcome requires an SQL session store")
	}
	if err := validateSessionWriteFence(fence); err != nil {
		return false, err
	}
	if expectedVersion < 1 || outcomeEnd < 2 || outcomeEnd > len(events) {
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
	if targetVersion < expectedVersion {
		return false, completedToolResultProofInvalid()
	}
	if committed != expectedVersion && committed != targetVersion {
		return false, fmt.Errorf("%w: expected %d or %d, found %d", core.ErrSessionConflict, expectedVersion, targetVersion, committed)
	}
	session, err := s.restoreFencedSession(ctx, tx, fence.SessionID, options, committed)
	if err != nil {
		return false, err
	}
	outcome, err := deriveNativeQueuedModelOutcomeBatch(session, options, expectedVersion, outcomeEnd, events)
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
	if err := validateNativeQueuedModelOutcomeBase(session, options, fence, attempt, expectedVersion); err != nil {
		return false, err
	}
	if committed == targetVersion {
		stored, found, err := loadNativeQueuedModelOutcome(ctx, tx, s.dialect, fence.SessionID, fence.RunID, outcome.invocationID, true)
		if err != nil {
			return false, err
		}
		if !found || !nativeQueuedModelOutcomeMatches(stored, outcome) || validateNativeQueuedModelOutcomeBatchCommitted(session, options, expectedVersion, events, outcome) != nil {
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

func deriveNativeQueuedModelOutcomeBatch(session *core.Session, options core.SessionOptions, expectedVersion int64, outcomeEnd int, events []core.SessionEvent) (nativeQueuedModelOutcome, error) {
	if outcomeEnd < 2 || outcomeEnd > len(events) {
		return nativeQueuedModelOutcome{}, completedToolResultProofInvalid()
	}
	modelOutcomes := 0
	for index, event := range events {
		if event.Type != core.EvRunUsage {
			continue
		}
		var usage core.RunUsageData
		if json.Unmarshal(event.Data, &usage) != nil || !strings.HasPrefix(usage.InvocationID, "model:") {
			continue
		}
		modelOutcomes++
		if index == 0 || events[index-1].Type != core.EvAssistantMessage || index+1 != outcomeEnd {
			return nativeQueuedModelOutcome{}, completedToolResultProofInvalid()
		}
	}
	if modelOutcomes != 1 {
		return nativeQueuedModelOutcome{}, completedToolResultProofInvalid()
	}
	outcome, err := deriveNativeQueuedModelOutcome(session, options, expectedVersion, events[:outcomeEnd])
	if err != nil {
		return nativeQueuedModelOutcome{}, err
	}
	for index, event := range events {
		if event.Seq != expectedVersion+int64(index) || event.RunID != outcome.runID {
			return nativeQueuedModelOutcome{}, completedToolResultProofInvalid()
		}
	}
	baseEvents := session.Events()
	if int64(len(baseEvents)) < expectedVersion {
		return nativeQueuedModelOutcome{}, completedToolResultProofInvalid()
	}
	base, err := core.RestoreSession(options, baseEvents[:expectedVersion])
	if err != nil {
		return nativeQueuedModelOutcome{}, completedToolResultProofInvalid()
	}
	if _, err := core.RestoreSession(options, append(base.Events(), events...)); err != nil {
		return nativeQueuedModelOutcome{}, completedToolResultProofInvalid()
	}
	return outcome, nil
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

func validateNativeQueuedModelOutcomeBase(session *core.Session, options core.SessionOptions, fence SessionWriteFence, attempt nativeQueuedModelInvocation, expectedVersion int64) error {
	if session == nil || session.Version() < expectedVersion || attempt.sessionVersionAtAdmission > expectedVersion || attempt.invocationID != fmt.Sprintf("model:%d", attempt.stepStartSeq) {
		return completedToolResultProofInvalid()
	}
	events := session.Events()
	if int64(len(events)) < expectedVersion || attempt.sessionVersionAtAdmission < 1 || int64(len(events)) < attempt.sessionVersionAtAdmission {
		return completedToolResultProofInvalid()
	}
	base, err := core.RestoreSession(options, events[:attempt.sessionVersionAtAdmission])
	if err != nil {
		return completedToolResultProofInvalid()
	}
	derived, err := deriveNativeQueuedModelInvocation(base, fence, attempt.sessionVersionAtAdmission, attempt.input)
	if err != nil || !nativeQueuedModelInvocationAttemptMatches(attempt, derived) {
		return completedToolResultProofInvalid()
	}
	for _, event := range events[attempt.sessionVersionAtAdmission:expectedVersion] {
		if event.RunID != attempt.input.Request.RunID || event.Type != core.EvAssistantChunk {
			return completedToolResultProofInvalid()
		}
	}
	return nil
}

func validateNativeQueuedModelOutcomeBatchCommitted(session *core.Session, options core.SessionOptions, expectedVersion int64, events []core.SessionEvent, outcome nativeQueuedModelOutcome) error {
	if session == nil || session.Version() != expectedVersion+int64(len(events)) || int64(len(session.Events())) != session.Version() || outcome.versionAfterOutcome > session.Version() {
		return completedToolResultProofInvalid()
	}
	actual := session.Events()[expectedVersion:]
	expected, err := json.Marshal(events)
	if err != nil {
		return completedToolResultProofInvalid()
	}
	stored, err := json.Marshal(actual)
	if err != nil || !reflect.DeepEqual(expected, stored) {
		return completedToolResultProofInvalid()
	}
	if _, err := deriveNativeQueuedModelOutcomeBatch(session, options, expectedVersion, int(outcome.versionAfterOutcome-expectedVersion), events); err != nil {
		return err
	}
	return nil
}

// nativeQueuedModelOutcomePrefixStart derives v46's logical hash boundary.
// That boundary is the batch's expected version, which can be after v45
// admission when already-persisted assistant chunks precede the eventual
// assistant/message. Event-chunk storage boundaries are unrelated: a
// WriteBehind batch may also coalesce earlier non-outcome events into the
// same physical chunk. The v46 digest identifies exactly one permitted
// suffix of the consecutive assistant-chunk region. A candidate is either
// v45's admission version (a coalesced write may begin its physical chunk
// earlier) or a later event_chunks boundary, because every append creates one
// chunk at its expected version. This excludes a forged hash which starts in
// the middle of a previously persisted assistant-chunk batch.
func nativeQueuedModelOutcomePrefixStart(ctx context.Context, tx *sql.Tx, dialect SQLDialect, sessionID string, session *core.Session, attempt nativeQueuedModelInvocation, outcome nativeQueuedModelOutcome) (int64, error) {
	if session == nil || attempt.sessionVersionAtAdmission < 1 || outcome.assistantEventSeq < attempt.sessionVersionAtAdmission || outcome.versionAfterOutcome > session.Version() {
		return 0, completedToolResultProofInvalid()
	}
	eventCount := int64(len(session.Events()))
	if outcome.assistantEventSeq < 0 || outcome.assistantEventSeq >= eventCount || outcome.usageEventSeq != outcome.assistantEventSeq+1 || outcome.usageEventSeq >= eventCount || outcome.versionAfterOutcome != outcome.usageEventSeq+1 || outcome.versionAfterOutcome <= 0 || outcome.versionAfterOutcome > eventCount {
		return 0, completedToolResultProofInvalid()
	}
	rows, err := tx.QueryContext(ctx, sqlListNativeQueuedModelOutcomePrefixStarts.bind(dialect), sessionID, attempt.sessionVersionAtAdmission, outcome.assistantEventSeq)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	candidates := map[int64]struct{}{attempt.sessionVersionAtAdmission: {}}
	for rows.Next() {
		var start int64
		if err := rows.Scan(&start); err != nil {
			return 0, err
		}
		candidates[start] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	events := session.Events()
	first := outcome.assistantEventSeq
	for first > attempt.sessionVersionAtAdmission {
		event := events[first-1]
		if event.Type != core.EvAssistantChunk || event.RunID != outcome.runID {
			break
		}
		first--
	}
	matched := int64(-1)
	for candidate := first; candidate <= outcome.assistantEventSeq; candidate++ {
		if _, allowed := candidates[candidate]; !allowed {
			continue
		}
		if err := validateNativeQueuedModelOutcomeForPrune(session, attempt, outcome, candidate); err != nil {
			continue
		}
		if matched >= 0 {
			return 0, completedToolResultProofInvalid()
		}
		matched = candidate
	}
	if matched < 0 {
		return 0, completedToolResultProofInvalid()
	}
	return matched, nil
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
