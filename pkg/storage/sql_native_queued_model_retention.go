package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"time"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

const maxNativeQueuedModelPruneBatch = 8192

var (
	sqlListPrunableNativeQueuedModels = sqlQuery{`SELECT session_id, run_id, invocation_id
		FROM native_queued_model_invocation_outcomes WHERE created_at < ?
		ORDER BY session_id, run_id, invocation_id LIMIT 8192`}
	sqlLockNativeQueuedModelRun                 = sqlQuery{`SELECT status, session_id, tenant_id, subject_id FROM run_control WHERE run_id = ?`}
	sqlLockNativeQueuedModelRunPostgres         = sqlQuery{`SELECT status, session_id, tenant_id, subject_id FROM run_control WHERE run_id = ? FOR UPDATE`}
	sqlLockNativeQueuedModelSession             = sqlQuery{`SELECT version, header FROM sessions WHERE id = ?`}
	sqlLockNativeQueuedModelSessionPostgres     = sqlQuery{`SELECT version, header FROM sessions WHERE id = ? FOR UPDATE`}
	sqlSelectNativeQueuedModelOutcomeChunkStart = sqlQuery{`SELECT start_seq FROM event_chunks
		WHERE session_id = ? AND start_seq <= ? ORDER BY start_seq DESC LIMIT 1`}
	sqlDeleteNativeQueuedModelOutcome = sqlQuery{`DELETE FROM native_queued_model_invocation_outcomes
		WHERE session_id = ? AND run_id = ? AND invocation_id = ?`}
	sqlDeleteNativeQueuedModelAttempt = sqlQuery{`DELETE FROM native_queued_model_invocations
		WHERE session_id = ? AND run_id = ? AND invocation_id = ?`}
)

type nativeQueuedModelPruneCandidate struct{ sessionID, runID, invocationID string }

// PruneNativeQueuedModelInvocations removes old v45 attempt/v46 outcome pairs
// only after the canonical outcome is superseded by a later durable Session
// suffix or the run is terminal. Attempts without outcomes are permanent
// replay fences and are never removed by age. The return count is the number
// of atomically removed pairs; one call processes at most 8192 candidates.
func (s *SQLSessionStore) PruneNativeQueuedModelInvocations(ctx context.Context, olderThan time.Time) (int64, error) {
	if s == nil || s.db == nil {
		return 0, fmt.Errorf("native queued model pruning requires an SQL session store")
	}
	rows, err := s.db.QueryContext(ctx, sqlListPrunableNativeQueuedModels.bind(s.dialect), olderThan.UTC().UnixMilli())
	if err != nil {
		return 0, err
	}
	var candidates []nativeQueuedModelPruneCandidate
	for rows.Next() {
		var candidate nativeQueuedModelPruneCandidate
		if err := rows.Scan(&candidate.sessionID, &candidate.runID, &candidate.invocationID); err != nil {
			_ = rows.Close()
			return 0, err
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, err
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	if len(candidates) > maxNativeQueuedModelPruneBatch {
		return 0, fmt.Errorf("native queued model prune batch exceeds %d", maxNativeQueuedModelPruneBatch)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()
	var deleted int64
	for _, candidate := range candidates {
		removed, err := s.pruneNativeQueuedModelCandidate(ctx, tx, candidate, olderThan.UTC().UnixMilli())
		if err != nil {
			return 0, err
		}
		if removed {
			deleted++
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return deleted, nil
}

func (s *SQLSessionStore) pruneNativeQueuedModelCandidate(ctx context.Context, tx *sql.Tx, candidate nativeQueuedModelPruneCandidate, cutoff int64) (bool, error) {
	runQuery, sessionQuery := sqlLockNativeQueuedModelRun, sqlLockNativeQueuedModelSession
	if s.dialect == SQLDialectPostgres {
		runQuery, sessionQuery = sqlLockNativeQueuedModelRunPostgres, sqlLockNativeQueuedModelSessionPostgres
	}
	var status, runSessionID, tenantID, subjectID string
	if err := tx.QueryRowContext(ctx, runQuery.bind(s.dialect), candidate.runID).Scan(&status, &runSessionID, &tenantID, &subjectID); errors.Is(err, sql.ErrNoRows) {
		return false, completedToolResultProofInvalid()
	} else if err != nil {
		return false, err
	}
	if runSessionID != candidate.sessionID {
		return false, completedToolResultProofInvalid()
	}
	active, err := nativeQueuedModelRunActive(status)
	if err != nil {
		return false, err
	}
	var version int64
	var header string
	if err := tx.QueryRowContext(ctx, sessionQuery.bind(s.dialect), candidate.sessionID).Scan(&version, &header); errors.Is(err, sql.ErrNoRows) {
		return false, completedToolResultProofInvalid()
	} else if err != nil {
		return false, err
	}
	options, err := completedToolResultRecoverySidecarSessionOptions(header, candidate.sessionID)
	if err != nil {
		return false, completedToolResultProofInvalid()
	}
	if options.Principal.TenantID != tenantID || options.Principal.SubjectID != subjectID {
		return false, completedToolResultProofInvalid()
	}
	attempt, found, err := loadNativeQueuedModelInvocation(ctx, tx, s.dialect, candidate.sessionID, candidate.runID, candidate.invocationID, true)
	if err != nil {
		return false, err
	}
	if !found {
		_, outcomeFound, err := loadNativeQueuedModelOutcome(ctx, tx, s.dialect, candidate.sessionID, candidate.runID, candidate.invocationID, true)
		if err != nil {
			return false, err
		}
		if outcomeFound {
			return false, completedToolResultProofInvalid()
		}
		return false, nil
	}
	outcome, found, err := loadNativeQueuedModelOutcome(ctx, tx, s.dialect, candidate.sessionID, candidate.runID, candidate.invocationID, true)
	if err != nil {
		return false, err
	}
	if !found {
		return false, completedToolResultProofInvalid()
	}
	if attempt.requestSHA256 != outcome.attemptRequestSHA256 {
		return false, completedToolResultProofInvalid()
	}
	if !reflect.DeepEqual(attempt.input.Request.Principal, options.Principal) || attempt.input.Request.SessionID != candidate.sessionID || attempt.input.Request.RunID != candidate.runID ||
		outcome.sessionID != candidate.sessionID || outcome.runID != candidate.runID || outcome.invocationID != candidate.invocationID {
		return false, completedToolResultProofInvalid()
	}
	session, err := s.restoreFencedSession(ctx, tx, candidate.sessionID, options, version)
	if err != nil {
		return false, err
	}
	var outcomeStartSeq int64
	if err := tx.QueryRowContext(ctx, sqlSelectNativeQueuedModelOutcomeChunkStart.bind(s.dialect), candidate.sessionID, outcome.assistantEventSeq).Scan(&outcomeStartSeq); errors.Is(err, sql.ErrNoRows) {
		return false, completedToolResultProofInvalid()
	} else if err != nil {
		return false, err
	}
	if outcomeStartSeq < 0 || outcomeStartSeq > outcome.assistantEventSeq || outcome.versionAfterOutcome > int64(len(session.Events())) {
		return false, completedToolResultProofInvalid()
	}
	outcomeEvents := session.Events()[outcomeStartSeq:outcome.versionAfterOutcome]
	if err := verifyCompletedToolResultRecoveryPrefixChunk(ctx, tx, s.dialect, candidate.sessionID, outcomeStartSeq, outcomeEvents); err != nil {
		return false, err
	}
	if err := validateNativeQueuedModelOutcomeForPrune(session, attempt, outcome, outcomeStartSeq); err != nil {
		return false, err
	}
	if outcome.createdAt.UnixMilli() >= cutoff || active && version <= outcome.versionAfterOutcome {
		return false, nil
	}
	result, err := tx.ExecContext(ctx, sqlDeleteNativeQueuedModelOutcome.bind(s.dialect), candidate.sessionID, candidate.runID, candidate.invocationID)
	if err != nil {
		return false, err
	}
	count, err := result.RowsAffected()
	if err != nil || count != 1 {
		if err != nil {
			return false, err
		}
		return false, completedToolResultProofInvalid()
	}
	result, err = tx.ExecContext(ctx, sqlDeleteNativeQueuedModelAttempt.bind(s.dialect), candidate.sessionID, candidate.runID, candidate.invocationID)
	if err != nil {
		return false, err
	}
	count, err = result.RowsAffected()
	if err != nil || count != 1 {
		if err != nil {
			return false, err
		}
		return false, completedToolResultProofInvalid()
	}
	return true, nil
}

func nativeQueuedModelRunActive(status string) (bool, error) {
	switch status {
	case RunStatusQueued, RunStatusRunning, RunStatusWaitingApproval:
		return true, nil
	case string(core.RunCompleted), string(core.RunLimited), string(core.RunFailed), string(core.RunCancelled):
		return false, nil
	default:
		return false, completedToolResultProofInvalid()
	}
}
