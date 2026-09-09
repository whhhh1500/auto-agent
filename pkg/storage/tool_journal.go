package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

// SQLToolInvocationJournal durably fences tool side effects on the shared SQL
// schema. The primary key is the logical run/call identity; capability and
// argument fingerprints turn unsafe identity reuse into an explicit conflict.
const MaxToolInvocations = 8192

var errToolInvocationNotFound = errors.New("tool invocation not found")

type SQLToolInvocationJournal struct {
	db             *sql.DB
	dialect        SQLDialect
	maxInvocations int
}

func (s *SQLToolInvocationJournal) atomicSessionFenceDomain() atomicSessionFenceDomain {
	domain, _ := newAtomicSessionFenceDomain(s.db, s.dialect)
	return domain
}

var (
	sqlInsertToolInvocation = sqlQuery{`INSERT INTO tool_invocations
		(tenant_id, subject_id, session_id, run_id, call_id, capability_id, args_digest,
		 idempotent, state, result_json, error_code, started_at, updated_at, completed_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'started', '', '', ?, ?, 0)`}
	sqlSelectToolInvocation = sqlQuery{`SELECT tenant_id, subject_id, session_id, run_id, call_id,
		capability_id, args_digest, idempotent, state, result_json, error_code,
		started_at, updated_at, completed_at
		FROM tool_invocations WHERE session_id = ? AND run_id = ? AND call_id = ?`}
	sqlSelectToolInvocationForReader = sqlQuery{`SELECT tenant_id, subject_id, session_id, run_id, call_id,
		capability_id, args_digest, idempotent, state, result_json, error_code,
		started_at, updated_at, completed_at
		FROM tool_invocations
		WHERE tenant_id = ? AND subject_id = ? AND session_id = ? AND run_id = ? AND call_id = ?
		AND capability_id = ? AND args_digest = ? AND idempotent = ?`}
	sqlListRunToolInvocations = sqlQuery{`SELECT tenant_id, subject_id, session_id, run_id, call_id,
		capability_id, args_digest, idempotent, state, result_json, error_code,
		started_at, updated_at, completed_at
		FROM tool_invocations
		WHERE tenant_id = ? AND subject_id = ? AND session_id = ? AND run_id = ?
		ORDER BY call_id LIMIT ?`}
	sqlCompleteToolInvocation = sqlQuery{`UPDATE tool_invocations
		SET state = 'completed', result_json = ?, error_code = '', updated_at = ?, completed_at = ?
		WHERE session_id = ? AND run_id = ? AND call_id = ?
		AND tenant_id = ? AND subject_id = ? AND capability_id = ? AND args_digest = ? AND idempotent = ?
		AND state IN ('started', 'uncertain')`}
	sqlUncertainToolInvocation = sqlQuery{`UPDATE tool_invocations
		SET state = 'uncertain', error_code = ?, updated_at = ?
		WHERE session_id = ? AND run_id = ? AND call_id = ?
		AND tenant_id = ? AND subject_id = ? AND capability_id = ? AND args_digest = ? AND idempotent = ?
		AND state <> 'completed'`}
	sqlCountToolInvocations = sqlQuery{`SELECT COUNT(*) FROM tool_invocations`}
)

func NewSQLToolInvocationJournal(db *sql.DB, dialect SQLDialect) (*SQLToolInvocationJournal, error) {
	if db == nil {
		return nil, fmt.Errorf("tool invocation journal requires a database handle")
	}
	if err := validateSQLDialect(dialect); err != nil {
		return nil, err
	}
	return &SQLToolInvocationJournal{db: db, dialect: dialect}, nil
}

func (s *SQLToolInvocationJournal) invocationCap() int {
	if s != nil && s.maxInvocations > 0 {
		return s.maxInvocations
	}
	return MaxToolInvocations
}

func (s *SQLToolInvocationJournal) BeginToolInvocation(ctx context.Context, invocation core.ToolInvocation) (core.ToolInvocationRecord, core.ToolInvocationDecision, error) {
	if err := core.ValidateToolInvocation(invocation); err != nil {
		return core.ToolInvocationRecord{}, "", err
	}
	var stored int
	if err := s.db.QueryRowContext(ctx, sqlCountToolInvocations.bind(s.dialect)).Scan(&stored); err != nil {
		return core.ToolInvocationRecord{}, "", err
	}
	if stored >= s.invocationCap() {
		record, getErr := s.getToolInvocation(ctx, invocation.SessionID, invocation.RunID, invocation.CallID)
		if getErr == nil {
			return existingToolInvocationDecision(record, invocation)
		}
		if !errors.Is(getErr, errToolInvocationNotFound) {
			return core.ToolInvocationRecord{}, "", getErr
		}
		return core.ToolInvocationRecord{}, "", fmt.Errorf("tool invocations exceed maximum of %d", s.invocationCap())
	}
	now := time.Now().UTC()
	_, err := s.db.ExecContext(ctx, sqlInsertToolInvocation.bind(s.dialect),
		invocation.TenantID, invocation.SubjectID, invocation.SessionID,
		invocation.RunID, invocation.CallID, invocation.CapabilityID, invocation.ArgsDigest,
		boolInt(invocation.Idempotent), now.UnixMilli(), now.UnixMilli(),
	)
	if err == nil {
		return core.ToolInvocationRecord{
			ToolInvocation: invocation, State: core.ToolInvocationStarted,
			StartedAt: now, UpdatedAt: now,
		}, core.ToolInvocationExecuteNew, nil
	}
	if !isDuplicateConstraint(err) {
		return core.ToolInvocationRecord{}, "", err
	}
	record, err := s.getToolInvocation(ctx, invocation.SessionID, invocation.RunID, invocation.CallID)
	if err != nil {
		return core.ToolInvocationRecord{}, "", err
	}
	return existingToolInvocationDecision(record, invocation)
}

func existingToolInvocationDecision(record core.ToolInvocationRecord, invocation core.ToolInvocation) (core.ToolInvocationRecord, core.ToolInvocationDecision, error) {
	if !sameToolInvocation(record.ToolInvocation, invocation) {
		return record, core.ToolInvocationConflict, nil
	}
	switch record.State {
	case core.ToolInvocationCompleted:
		if record.Result == nil {
			return core.ToolInvocationRecord{}, "", fmt.Errorf("completed tool invocation has no result")
		}
		return record, core.ToolInvocationReplay, nil
	case core.ToolInvocationStarted, core.ToolInvocationUncertain:
		if invocation.Idempotent {
			return record, core.ToolInvocationExecuteRetry, nil
		}
		return record, core.ToolInvocationUnknown, nil
	default:
		return core.ToolInvocationRecord{}, "", fmt.Errorf("unknown tool invocation state %q", record.State)
	}
}

func (s *SQLToolInvocationJournal) CompleteToolInvocation(ctx context.Context, invocation core.ToolInvocation, result core.CapabilityResult) (core.ToolInvocationRecord, error) {
	if err := core.ValidateToolInvocation(invocation); err != nil {
		return core.ToolInvocationRecord{}, err
	}
	if err := core.ValidateCapabilityResult(result); err != nil {
		return core.ToolInvocationRecord{}, err
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return core.ToolInvocationRecord{}, fmt.Errorf("encode tool invocation result: %w", err)
	}
	now := time.Now().UTC().UnixMilli()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return core.ToolInvocationRecord{}, err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, sqlCompleteToolInvocation.bind(s.dialect),
		string(encoded), now, now,
		invocation.SessionID, invocation.RunID, invocation.CallID,
		invocation.TenantID, invocation.SubjectID, invocation.CapabilityID,
		invocation.ArgsDigest, boolInt(invocation.Idempotent),
	); err != nil {
		return core.ToolInvocationRecord{}, err
	}
	record, err := scanToolInvocation(tx.QueryRowContext(ctx, sqlSelectToolInvocation.bind(s.dialect),
		invocation.SessionID, invocation.RunID, invocation.CallID,
	))
	if err != nil {
		return core.ToolInvocationRecord{}, err
	}
	if !sameToolInvocation(record.ToolInvocation, invocation) {
		return core.ToolInvocationRecord{}, fmt.Errorf("tool invocation identity conflicts with the durable journal")
	}
	if record.State != core.ToolInvocationCompleted || record.Result == nil {
		return core.ToolInvocationRecord{}, fmt.Errorf("tool invocation did not reach completed state")
	}
	if err := tx.Commit(); err != nil {
		return core.ToolInvocationRecord{}, err
	}
	return core.CloneToolInvocationRecord(record), nil
}

func (s *SQLToolInvocationJournal) MarkToolInvocationUncertain(ctx context.Context, invocation core.ToolInvocation, errorCode string) error {
	if err := core.ValidateToolInvocation(invocation); err != nil {
		return err
	}
	if len(errorCode) > 128 {
		errorCode = errorCode[:128]
	}
	now := time.Now().UTC().UnixMilli()
	result, err := s.db.ExecContext(ctx, sqlUncertainToolInvocation.bind(s.dialect),
		errorCode, now,
		invocation.SessionID, invocation.RunID, invocation.CallID,
		invocation.TenantID, invocation.SubjectID, invocation.CapabilityID,
		invocation.ArgsDigest, boolInt(invocation.Idempotent),
	)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected > 0 {
		return nil
	}
	record, err := s.getToolInvocation(ctx, invocation.SessionID, invocation.RunID, invocation.CallID)
	if err != nil {
		return err
	}
	if !sameToolInvocation(record.ToolInvocation, invocation) {
		return fmt.Errorf("tool invocation identity conflicts with the durable journal")
	}
	if record.State == core.ToolInvocationCompleted {
		return nil
	}
	return fmt.Errorf("tool invocation could not be marked uncertain")
}

// GetToolInvocation reads an exact durable journal identity without invoking
// Begin or creating a started row. The SQL predicate includes every immutable
// identity field. Missing rows, cross-principal requests, and same-key identity
// conflicts are deliberately indistinguishable: all return found=false and no
// record. This prevents a recovery caller from using the reader to discover a
// different capability, argument digest, state, or completed result.
func (s *SQLToolInvocationJournal) GetToolInvocation(ctx context.Context, invocation core.ToolInvocation) (core.ToolInvocationRecord, bool, error) {
	if err := core.ValidateToolInvocation(invocation); err != nil {
		return core.ToolInvocationRecord{}, false, err
	}
	record, err := scanToolInvocation(s.db.QueryRowContext(ctx, sqlSelectToolInvocationForReader.bind(s.dialect),
		invocation.TenantID, invocation.SubjectID, invocation.SessionID, invocation.RunID, invocation.CallID,
		invocation.CapabilityID, invocation.ArgsDigest, boolInt(invocation.Idempotent),
	))
	if err != nil {
		if errors.Is(err, errToolInvocationNotFound) {
			return core.ToolInvocationRecord{}, false, nil
		}
		return core.ToolInvocationRecord{}, false, err
	}
	return core.CloneToolInvocationRecord(record), true, nil
}

// ListRunToolInvocations returns one complete, exact-principal run scope for
// evidence reconstruction. It is an optional structural companion consumed by
// evaluation; it never creates or changes journal records.
func (s *SQLToolInvocationJournal) ListRunToolInvocations(ctx context.Context, principal core.Principal, sessionID, runID string) ([]core.ToolInvocationRecord, error) {
	if s == nil || s.db == nil || !validExternalEffectPrincipalPart(principal.TenantID) || !validExternalEffectPrincipalPart(principal.SubjectID) {
		return nil, fmt.Errorf("tool invocation run scope is invalid")
	}
	if err := core.ValidateSessionID(sessionID); err != nil {
		return nil, err
	}
	if err := core.ValidateRunID(runID); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, sqlListRunToolInvocations.bind(s.dialect), principal.TenantID, principal.SubjectID, sessionID, runID, MaxToolInvocations+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	records := make([]core.ToolInvocationRecord, 0)
	for rows.Next() {
		record, err := scanToolInvocation(rows)
		if err != nil {
			return nil, err
		}
		if len(records) == MaxToolInvocations {
			return nil, fmt.Errorf("tool invocation run scope exceeds maximum of %d", MaxToolInvocations)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

func (s *SQLToolInvocationJournal) getToolInvocation(ctx context.Context, sessionID, runID, callID string) (core.ToolInvocationRecord, error) {
	return scanToolInvocation(s.db.QueryRowContext(ctx, sqlSelectToolInvocation.bind(s.dialect), sessionID, runID, callID))
}

func scanToolInvocation(scanner runScanner) (core.ToolInvocationRecord, error) {
	var record core.ToolInvocationRecord
	var idempotent int
	var state string
	var resultJSON string
	var startedMillis, updatedMillis, completedMillis int64
	err := scanner.Scan(
		&record.TenantID, &record.SubjectID, &record.SessionID, &record.RunID, &record.CallID,
		&record.CapabilityID, &record.ArgsDigest, &idempotent, &state, &resultJSON,
		&record.ErrorCode, &startedMillis, &updatedMillis, &completedMillis,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return core.ToolInvocationRecord{}, errToolInvocationNotFound
	}
	if err != nil {
		return core.ToolInvocationRecord{}, err
	}
	if idempotent != 0 && idempotent != 1 {
		return core.ToolInvocationRecord{}, fmt.Errorf("invalid tool invocation idempotency value %d", idempotent)
	}
	record.Idempotent = idempotent != 0
	record.State = core.ToolInvocationState(state)
	if err := core.ValidateToolInvocation(record.ToolInvocation); err != nil {
		return core.ToolInvocationRecord{}, fmt.Errorf("validate tool invocation record: %w", err)
	}
	if startedMillis <= 0 || updatedMillis <= 0 {
		return core.ToolInvocationRecord{}, fmt.Errorf("tool invocation has invalid timestamps")
	}
	switch record.State {
	case core.ToolInvocationStarted, core.ToolInvocationUncertain:
		if resultJSON != "" || completedMillis != 0 {
			return core.ToolInvocationRecord{}, fmt.Errorf("non-completed tool invocation has a durable result")
		}
	case core.ToolInvocationCompleted:
		if resultJSON == "" || completedMillis <= 0 {
			return core.ToolInvocationRecord{}, fmt.Errorf("completed tool invocation has no durable result")
		}
	default:
		return core.ToolInvocationRecord{}, fmt.Errorf("unknown tool invocation state %q", record.State)
	}
	record.StartedAt = time.UnixMilli(startedMillis).UTC()
	record.UpdatedAt = time.UnixMilli(updatedMillis).UTC()
	if completedMillis > 0 {
		record.CompletedAt = time.UnixMilli(completedMillis).UTC()
	}
	if resultJSON != "" {
		if strings.TrimSpace(resultJSON) == "null" {
			return core.ToolInvocationRecord{}, fmt.Errorf("decode tool invocation result: null is not a capability result")
		}
		var result core.CapabilityResult
		if err := json.Unmarshal([]byte(resultJSON), &result); err != nil {
			return core.ToolInvocationRecord{}, fmt.Errorf("decode tool invocation result: %w", err)
		}
		if err := core.ValidateCapabilityResult(result); err != nil {
			return core.ToolInvocationRecord{}, fmt.Errorf("validate tool invocation result: %w", err)
		}
		record.Result = &result
	}
	return core.CloneToolInvocationRecord(record), nil
}

func sameToolInvocation(left, right core.ToolInvocation) bool {
	return left.TenantID == right.TenantID && left.SubjectID == right.SubjectID &&
		left.SessionID == right.SessionID && left.RunID == right.RunID && left.CallID == right.CallID &&
		left.CapabilityID == right.CapabilityID && left.ArgsDigest == right.ArgsDigest &&
		left.Idempotent == right.Idempotent
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

var _ core.ToolInvocationJournal = (*SQLToolInvocationJournal)(nil)
var _ core.ToolInvocationReader = (*SQLToolInvocationJournal)(nil)
