package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

var _ FencedCompletedToolResultAppender = (*SQLSessionStore)(nil)

var (
	sqlFenceSQLiteAcquireCompletedResult = sqlQuery{`UPDATE sessions SET updated_at = updated_at
		WHERE id = ? AND tenant_id = ? AND user_id = ? AND EXISTS (
			SELECT 1 FROM run_control rc
			JOIN run_queue q ON q.run_id = rc.run_id
			JOIN session_leases sl ON sl.session_id = rc.session_id
			WHERE rc.run_id = ? AND rc.session_id = ?
			AND rc.tenant_id = ? AND rc.subject_id = ?
			AND rc.status = 'running' AND rc.cancel_requested = 0
			AND q.worker_id = ? AND q.generation = ?
			AND q.lease_expires_at > CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)
			AND sl.holder = ?
			AND sl.expires_at > CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)
		)`}
	sqlFenceSelectToolInvocation = sqlQuery{`SELECT tenant_id, subject_id, session_id, run_id, call_id,
		capability_id, args_digest, idempotent, state, result_json, error_code,
		started_at, updated_at, completed_at
		FROM tool_invocations
		WHERE tenant_id = ? AND subject_id = ? AND session_id = ? AND run_id = ? AND call_id = ?
		AND capability_id = ? AND args_digest = ? AND idempotent = ?`}
	sqlFencePostgresLockToolInvocation = sqlQuery{`SELECT tenant_id, subject_id, session_id, run_id, call_id,
		capability_id, args_digest, idempotent, state, result_json, error_code,
		started_at, updated_at, completed_at
		FROM tool_invocations
		WHERE tenant_id = ? AND subject_id = ? AND session_id = ? AND run_id = ? AND call_id = ?
		AND capability_id = ? AND args_digest = ? AND idempotent = ? FOR UPDATE`}
)

// AppendCompletedToolResultFenced implements FencedCompletedToolResultAppender
// for SQL stores whose Session, queue, lease, and tool journal rows share one
// database transaction. It deliberately creates only a canonical tool/result;
// a higher-level coordinator owns recovery-tail classification and any future
// run/resume marker contract.
func (s *SQLSessionStore) AppendCompletedToolResultFenced(ctx context.Context, fence SessionWriteFence, expectedVersion int64, invocation core.ToolInvocation, expectedResultDigest string) (core.SessionEvent, bool, error) {
	if ctx == nil {
		return core.SessionEvent{}, false, fmt.Errorf("completed tool result append requires a context")
	}
	if s == nil || s.db == nil {
		return core.SessionEvent{}, false, fmt.Errorf("completed tool result append requires an SQL session store")
	}
	if err := validateCompletedToolResultRequest(fence, expectedVersion, invocation, expectedResultDigest); err != nil {
		return core.SessionEvent{}, false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return core.SessionEvent{}, false, err
	}
	defer func() { _ = tx.Rollback() }()

	var header string
	var committed int64
	switch s.dialect {
	case SQLDialectSQLite:
		header, committed, err = s.acquireSQLiteCompletedResultFence(ctx, tx, fence, expectedVersion)
	case SQLDialectPostgres:
		header, committed, err = s.lockPostgresSessionWriteFence(ctx, tx, fence)
	default:
		err = fmt.Errorf("unsupported SQL dialect %q", s.dialect.String())
	}
	if err != nil {
		return core.SessionEvent{}, false, err
	}
	var options core.SessionOptions
	if err := json.Unmarshal([]byte(header), &options); err != nil {
		return core.SessionEvent{}, false, fmt.Errorf("decode session %s header for completed tool result: %w", fence.SessionID, err)
	}
	if options.Principal.TenantID != fence.TenantID || options.Principal.SubjectID != fence.SubjectID {
		return core.SessionEvent{}, false, sessionWriteFenceLost("fenced identity does not own the session")
	}

	proof, err := s.lockCompletedToolResultProof(ctx, tx, invocation, expectedResultDigest)
	if err != nil {
		return core.SessionEvent{}, false, err
	}

	committedSession, err := s.restoreFencedSession(ctx, tx, fence.SessionID, options, committed)
	if err != nil {
		return core.SessionEvent{}, false, err
	}
	existing, resultCount, err := completedToolResultState(committedSession, invocation, proof.digest)
	if err != nil {
		return core.SessionEvent{}, false, err
	}
	if resultCount > 1 || (resultCount == 1 && existing.Seq != expectedVersion) {
		return core.SessionEvent{}, false, completedToolResultProofInvalid()
	}
	if resultCount == 1 {
		if committed != expectedVersion+1 {
			return core.SessionEvent{}, false, fmt.Errorf("%w: expected %d, found %d", core.ErrSessionConflict, expectedVersion+1, committed)
		}
		if err := tx.Commit(); err != nil {
			return core.SessionEvent{}, false, err
		}
		return existing, false, nil
	}
	if committed != expectedVersion {
		return core.SessionEvent{}, false, fmt.Errorf("%w: expected %d, found %d", core.ErrSessionConflict, expectedVersion, committed)
	}

	event, err := completedToolResultEvent(expectedVersion, invocation, proof.result)
	if err != nil {
		return core.SessionEvent{}, false, err
	}
	if err := s.insertChunk(ctx, tx, fence.SessionID, expectedVersion, []core.SessionEvent{event}); err != nil {
		return core.SessionEvent{}, false, err
	}
	if err := insertRunEvidence(ctx, tx, s.dialect, fence.SessionID, options, []core.SessionEvent{event}, s.evidenceCap()); err != nil {
		return core.SessionEvent{}, false, fmt.Errorf("index session %s completed tool result evidence: %w", fence.SessionID, err)
	}
	// Re-read the exact journal proof after every staged Session side effect.
	// PostgreSQL already holds the row lock, but this is still required to make
	// the commit condition explicit and to reject in-transaction trigger edits.
	// SQLite runs under the writer reservation acquired above and receives the
	// same logical recheck before the final fenced Session tip update.
	if _, err := s.lockCompletedToolResultProof(ctx, tx, invocation, expectedResultDigest); err != nil {
		return core.SessionEvent{}, false, err
	}
	write, err := tx.ExecContext(ctx, fencedSessionTipUpdate(s.dialect),
		expectedVersion+1, expectedVersion+1, fence.SessionID, expectedVersion, fence.TenantID, fence.SubjectID,
		fence.RunID, fence.SessionID, fence.TenantID, fence.SubjectID, fence.WorkerID, fence.QueueGeneration, fence.LeaseHolder,
	)
	if err != nil {
		return core.SessionEvent{}, false, err
	}
	affected, err := write.RowsAffected()
	if err != nil {
		return core.SessionEvent{}, false, err
	}
	if affected == 0 {
		return core.SessionEvent{}, false, sessionWriteFenceLost("claim, cancellation state, or session lease expired before completed tool result commit")
	}
	if err := tx.Commit(); err != nil {
		return core.SessionEvent{}, false, err
	}
	return event, true, nil
}

func validateCompletedToolResultRequest(fence SessionWriteFence, expectedVersion int64, invocation core.ToolInvocation, expectedResultDigest string) error {
	if err := validateSessionWriteFence(fence); err != nil {
		return err
	}
	if expectedVersion < 0 {
		return fmt.Errorf("completed tool result expected version must not be negative")
	}
	if err := core.ValidateToolInvocation(invocation); err != nil {
		return err
	}
	if invocation.SessionID != fence.SessionID || invocation.RunID != fence.RunID || invocation.TenantID != fence.TenantID || invocation.SubjectID != fence.SubjectID {
		return completedToolResultProofInvalid()
	}
	if !validCapabilityResultDigest(expectedResultDigest) {
		return completedToolResultProofInvalid()
	}
	return nil
}

func (s *SQLSessionStore) acquireSQLiteCompletedResultFence(ctx context.Context, tx *sql.Tx, fence SessionWriteFence, expectedVersion int64) (string, int64, error) {
	write, err := tx.ExecContext(ctx, sqlFenceSQLiteAcquireCompletedResult.bind(s.dialect),
		fence.SessionID, fence.TenantID, fence.SubjectID, fence.RunID, fence.SessionID,
		fence.TenantID, fence.SubjectID, fence.WorkerID, fence.QueueGeneration, fence.LeaseHolder,
	)
	if err != nil {
		return "", 0, err
	}
	affected, err := write.RowsAffected()
	if err != nil {
		return "", 0, err
	}
	if affected == 0 {
		return "", 0, s.classifySessionFenceFailure(ctx, tx, fence, expectedVersion)
	}
	var committed int64
	var header string
	if err := tx.QueryRowContext(ctx, sqlSelectSession.bind(s.dialect), fence.SessionID).Scan(&committed, &header); err != nil {
		return "", 0, err
	}
	return header, committed, nil
}

type completedToolResultProof struct {
	result core.CapabilityResult
	digest string
}

// lockCompletedToolResultProof reads the exact journal row under the current
// transaction and verifies that its completed canonical result still matches
// caller preflight. SQL errors and malformed durable rows remain distinct from
// an absent, non-completed, or identity-mismatched proof.
func (s *SQLSessionStore) lockCompletedToolResultProof(ctx context.Context, tx *sql.Tx, invocation core.ToolInvocation, expectedResultDigest string) (completedToolResultProof, error) {
	// PostgreSQL reaches this row only after lockPostgresSessionWriteFence has
	// locked run_control, run_queue, session_leases, and sessions. Begin and
	// Complete touch only tool_invocations, so they cannot form a reverse
	// control-plane lock cycle with this final journal-row lock.
	query := sqlFenceSelectToolInvocation
	if s.dialect == SQLDialectPostgres {
		query = sqlFencePostgresLockToolInvocation
	}
	record, err := scanToolInvocation(tx.QueryRowContext(ctx, query.bind(s.dialect),
		invocation.TenantID, invocation.SubjectID, invocation.SessionID, invocation.RunID, invocation.CallID,
		invocation.CapabilityID, invocation.ArgsDigest, boolInt(invocation.Idempotent),
	))
	if errors.Is(err, errToolInvocationNotFound) {
		return completedToolResultProof{}, completedToolResultProofInvalid()
	}
	if err != nil {
		return completedToolResultProof{}, err
	}
	if !sameToolInvocation(record.ToolInvocation, invocation) {
		return completedToolResultProof{}, completedToolResultProofInvalid()
	}
	if record.State != core.ToolInvocationCompleted || record.Result == nil {
		return completedToolResultProof{}, completedToolResultProofInvalid()
	}
	result := *record.Result
	digest, err := CanonicalCapabilityResultDigest(result)
	if err != nil {
		return completedToolResultProof{}, fmt.Errorf("canonical completed tool result digest: %w", err)
	}
	if digest != expectedResultDigest {
		return completedToolResultProof{}, completedToolResultProofInvalid()
	}
	return completedToolResultProof{result: result, digest: digest}, nil
}

func completedToolResultState(session *core.Session, invocation core.ToolInvocation, expectedResultDigest string) (core.SessionEvent, int, error) {
	if session == nil {
		return core.SessionEvent{}, 0, fmt.Errorf("restored completed tool result session is nil")
	}
	callCount := 0
	resultCount := 0
	var existing core.SessionEvent
	for _, event := range session.Events() {
		if event.RunID != invocation.RunID {
			continue
		}
		switch event.Type {
		case core.EvToolCall:
			var data core.ToolCallData
			if err := json.Unmarshal(event.Data, &data); err != nil {
				return core.SessionEvent{}, 0, fmt.Errorf("decode durable tool call: %w", err)
			}
			if data.CallID != invocation.CallID {
				continue
			}
			derived, err := core.NewToolInvocation(core.RunInfo{
				RunID: invocation.RunID, SessionID: invocation.SessionID,
				Principal: core.Principal{TenantID: invocation.TenantID, SubjectID: invocation.SubjectID},
			}, core.ToolCall{ID: data.CallID, Name: data.Name, Args: data.Args}, invocation.Idempotent)
			if err != nil {
				return core.SessionEvent{}, 0, fmt.Errorf("validate durable tool call: %w", err)
			}
			if !sameToolInvocation(derived, invocation) {
				return core.SessionEvent{}, 0, completedToolResultProofInvalid()
			}
			callCount++
		case core.EvToolResult:
			var data core.ToolResultData
			if err := json.Unmarshal(event.Data, &data); err != nil {
				return core.SessionEvent{}, 0, fmt.Errorf("decode durable tool result: %w", err)
			}
			if data.CallID != invocation.CallID {
				continue
			}
			digest, err := CanonicalCapabilityResultDigest(core.CapabilityResult{Content: data.Content, OK: data.OK, Metadata: data.Metadata})
			if err != nil {
				return core.SessionEvent{}, 0, fmt.Errorf("validate durable tool result: %w", err)
			}
			if digest != expectedResultDigest {
				return core.SessionEvent{}, 0, completedToolResultProofInvalid()
			}
			resultCount++
			existing = event
		}
	}
	if callCount != 1 {
		return core.SessionEvent{}, 0, completedToolResultProofInvalid()
	}
	return existing, resultCount, nil
}

func completedToolResultEvent(seq int64, invocation core.ToolInvocation, result core.CapabilityResult) (core.SessionEvent, error) {
	data, err := json.Marshal(core.ToolResultData{CallID: invocation.CallID, Content: result.Content, OK: result.OK, Metadata: result.Metadata})
	if err != nil {
		return core.SessionEvent{}, fmt.Errorf("encode completed tool result event: %w", err)
	}
	return core.SessionEvent{Seq: seq, Time: time.Now().UTC(), RunID: invocation.RunID, Type: core.EvToolResult, Data: data}, nil
}
