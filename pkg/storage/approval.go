package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

const (
	DefaultApprovalTTL      = 24 * time.Hour
	HardApprovalTTL         = 30 * 24 * time.Hour
	MaxApprovalRequestBytes = 2 << 20
	MaxPendingApprovals     = 1024
)

var ErrApprovalNotFound = errors.New("approval not found")

type ApprovalRecord struct {
	ID             string                `json:"id"`
	TenantID       string                `json:"tenant_id"`
	SubjectID      string                `json:"subject_id"`
	SessionID      string                `json:"session_id"`
	RunID          string                `json:"run_id"`
	CallID         string                `json:"call_id"`
	CapabilityID   string                `json:"capability_id"`
	ArgsDigest     string                `json:"args_digest"`
	ManifestDigest string                `json:"manifest_digest"`
	Request        core.ApprovalRequest  `json:"request"`
	Status         core.ApprovalDecision `json:"status"`
	RequestedAt    time.Time             `json:"requested_at"`
	ExpiresAt      time.Time             `json:"expires_at"`
	DecidedAt      time.Time             `json:"decided_at,omitempty"`
	DecidedBy      string                `json:"decided_by,omitempty"`
}

type ApprovalFilter struct {
	TenantID string
	Status   core.ApprovalDecision
	Limit    int
}

type ApprovalMetrics struct {
	Pending           int64
	OldestRequestedAt time.Time
}

type ApprovalMetricsStore interface {
	ApprovalMetrics(ctx context.Context) (ApprovalMetrics, error)
}

type ApprovalStore interface {
	core.Approver
	core.DurableApprover
	GetApproval(ctx context.Context, id string) (ApprovalRecord, error)
	ListApprovals(ctx context.Context, filter ApprovalFilter) ([]ApprovalRecord, error)
	DecideApproval(ctx context.Context, id string, decision core.ApprovalDecision, actor string) (ApprovalRecord, bool, error)
	ExpireApprovals(ctx context.Context, now time.Time) (int64, error)
	CancelRunApprovals(ctx context.Context, runID, actor string) (int64, error)
}

type SQLApprovalStore struct {
	db         *sql.DB
	dialect    SQLDialect
	TTL        time.Duration
	maxPending int
}

var (
	sqlInsertApproval = sqlQuery{`INSERT INTO approval_requests
		(id, tenant_id, subject_id, session_id, run_id, call_id, capability_id, args_digest, manifest_digest,
		 request_json, status, requested_at, expires_at, decided_at, decided_by)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, 0, '')`}
	sqlGetApproval = sqlQuery{`SELECT id, tenant_id, subject_id, session_id, run_id, call_id,
		capability_id, args_digest, manifest_digest, request_json, status, requested_at, expires_at, decided_at, decided_by
		FROM approval_requests WHERE id = ?`}
	sqlGetApprovalByCall = sqlQuery{`SELECT id, tenant_id, subject_id, session_id, run_id, call_id,
		capability_id, args_digest, manifest_digest, request_json, status, requested_at, expires_at, decided_at, decided_by
		FROM approval_requests WHERE session_id = ? AND run_id = ? AND call_id = ?`}
	sqlListApprovals = sqlQuery{`SELECT id, tenant_id, subject_id, session_id, run_id, call_id,
		capability_id, args_digest, manifest_digest, request_json, status, requested_at, expires_at, decided_at, decided_by
		FROM approval_requests`}
	sqlDecideApproval = sqlQuery{`UPDATE approval_requests SET status = ?, decided_at = ?, decided_by = ?
		WHERE id = ? AND status = 'pending'`}
	sqlExpiredApprovals      = sqlQuery{`SELECT id FROM approval_requests WHERE status = 'pending' AND expires_at <= ? ORDER BY expires_at`}
	sqlPendingRunApprovals   = sqlQuery{`SELECT id FROM approval_requests WHERE run_id = ? AND status = 'pending'`}
	sqlCountPendingApprovals = sqlQuery{`SELECT COUNT(*) FROM approval_requests WHERE status = 'pending'`}
	sqlResumeApprovedRun     = sqlQuery{`UPDATE run_control SET status = 'queued', error_code = '', updated_at = ?
		WHERE run_id = ? AND session_id = ? AND tenant_id = ? AND subject_id = ?
			AND status = 'waiting_approval' AND cancel_requested = 0`}
	sqlWakeRunQueue = sqlQuery{`UPDATE run_queue SET available_at = ?, worker_id = '', lease_expires_at = 0
		WHERE run_id = ? AND EXISTS (
			SELECT 1 FROM run_control
			WHERE run_control.run_id = run_queue.run_id AND session_id = ? AND tenant_id = ? AND subject_id = ?
				AND status = 'queued' AND cancel_requested = 0
		)`}
	sqlApprovalMetrics = sqlQuery{`SELECT COUNT(*), MIN(requested_at)
		FROM approval_requests WHERE status = 'pending'`}
)

func NewSQLApprovalStore(db *sql.DB, dialect SQLDialect) (*SQLApprovalStore, error) {
	if db == nil {
		return nil, fmt.Errorf("approval store requires a database handle")
	}
	if err := validateSQLDialect(dialect); err != nil {
		return nil, err
	}
	return &SQLApprovalStore{db: db, dialect: dialect, TTL: DefaultApprovalTTL}, nil
}

func (s *SQLApprovalStore) ApprovalMetrics(ctx context.Context) (ApprovalMetrics, error) {
	var metrics ApprovalMetrics
	var oldest sql.NullInt64
	if err := s.db.QueryRowContext(ctx, sqlApprovalMetrics.bind(s.dialect)).Scan(&metrics.Pending, &oldest); err != nil {
		return ApprovalMetrics{}, err
	}
	if oldest.Valid && oldest.Int64 > 0 {
		metrics.OldestRequestedAt = time.UnixMilli(oldest.Int64).UTC()
	}
	return metrics, nil
}

func (s *SQLApprovalStore) Approve(ctx context.Context, request core.ApprovalRequest) (core.ApprovalDecision, error) {
	resolution, err := s.RequestApproval(ctx, request)
	return resolution.Decision, err
}

func (s *SQLApprovalStore) RequestApproval(ctx context.Context, request core.ApprovalRequest) (core.ApprovalResolution, error) {
	invocation, err := approvalInvocation(request)
	if err != nil {
		return core.ApprovalResolution{}, err
	}
	manifestDigest, err := approvalManifestDigest(request.Manifest)
	if err != nil {
		return core.ApprovalResolution{}, err
	}
	id := approvalID(invocation, manifestDigest)
	// Arbitrary identity attributes are not needed to decide the request and
	// may contain product-specific sensitive data. Persist only stable identity,
	// ownership scope, and grants.
	request.Principal.Attributes = nil
	encoded, err := json.Marshal(request)
	if err != nil {
		return core.ApprovalResolution{}, fmt.Errorf("encode approval request: %w", err)
	}
	if len(encoded) > MaxApprovalRequestBytes {
		return core.ApprovalResolution{}, fmt.Errorf("approval request exceeds %d bytes", MaxApprovalRequestBytes)
	}
	ttl := s.TTL
	if ttl <= 0 {
		ttl = DefaultApprovalTTL
	}
	if ttl > HardApprovalTTL {
		return core.ApprovalResolution{}, fmt.Errorf("approval ttl exceeds %s", HardApprovalTTL)
	}
	cap := s.maxPending
	if cap <= 0 {
		cap = MaxPendingApprovals
	}
	var pending int
	if err := s.db.QueryRowContext(ctx, sqlCountPendingApprovals.bind(s.dialect)).Scan(&pending); err != nil {
		return core.ApprovalResolution{}, err
	}
	if pending >= cap {
		existing, existingErr := scanApproval(s.db.QueryRowContext(ctx, sqlGetApprovalByCall.bind(s.dialect),
			invocation.SessionID, invocation.RunID, invocation.CallID,
		))
		if existingErr == nil {
			if !approvalMatches(existing, invocation, manifestDigest) {
				return core.ApprovalResolution{}, fmt.Errorf("approval call identity conflicts with the durable request")
			}
			return approvalResolution(existing), nil
		}
		if !errors.Is(existingErr, ErrApprovalNotFound) {
			return core.ApprovalResolution{}, existingErr
		}
		return core.ApprovalResolution{}, fmt.Errorf("pending approvals exceed maximum of %d", cap)
	}
	now := time.Now().UTC()
	_, err = s.db.ExecContext(ctx, sqlInsertApproval.bind(s.dialect),
		id, invocation.TenantID, invocation.SubjectID, invocation.SessionID, invocation.RunID,
		invocation.CallID, invocation.CapabilityID, invocation.ArgsDigest, manifestDigest, string(encoded),
		now.UnixMilli(), now.Add(ttl).UnixMilli(),
	)
	if err != nil {
		if !isDuplicateConstraint(err) {
			return core.ApprovalResolution{}, err
		}
		existing, existingErr := scanApproval(s.db.QueryRowContext(ctx, sqlGetApprovalByCall.bind(s.dialect),
			invocation.SessionID, invocation.RunID, invocation.CallID,
		))
		if existingErr != nil {
			return core.ApprovalResolution{}, existingErr
		}
		if !approvalMatches(existing, invocation, manifestDigest) {
			return core.ApprovalResolution{}, fmt.Errorf("approval call identity conflicts with the durable request")
		}
		return approvalResolution(existing), nil
	}
	record, err := s.GetApproval(ctx, id)
	if err != nil {
		return core.ApprovalResolution{}, err
	}
	if !approvalMatches(record, invocation, manifestDigest) {
		return core.ApprovalResolution{}, fmt.Errorf("approval identity conflicts with the durable request")
	}
	return approvalResolution(record), nil
}

func (s *SQLApprovalStore) GetApproval(ctx context.Context, id string) (ApprovalRecord, error) {
	if err := core.ValidateApprovalID(id); err != nil {
		return ApprovalRecord{}, err
	}
	return scanApproval(s.db.QueryRowContext(ctx, sqlGetApproval.bind(s.dialect), id))
}

func (s *SQLApprovalStore) ListApprovals(ctx context.Context, filter ApprovalFilter) ([]ApprovalRecord, error) {
	if err := validateSQLTextFilter("approval tenant_id", filter.TenantID); err != nil {
		return nil, err
	}
	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	query := sqlListApprovals.text
	args := []any{}
	clauses := []string{}
	if filter.TenantID != "" {
		clauses = append(clauses, "tenant_id = ?")
		args = append(args, filter.TenantID)
	}
	if filter.Status != "" {
		if !validApprovalStatus(filter.Status) {
			return nil, fmt.Errorf("invalid approval status %q", filter.Status)
		}
		clauses = append(clauses, "status = ?")
		args = append(args, string(filter.Status))
	}
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " ORDER BY requested_at DESC LIMIT ?"
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, (sqlQuery{query}).bind(s.dialect), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []ApprovalRecord{}
	for rows.Next() {
		record, err := scanApproval(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	return out, rows.Err()
}

func (s *SQLApprovalStore) DecideApproval(ctx context.Context, id string, decision core.ApprovalDecision, actor string) (ApprovalRecord, bool, error) {
	if err := core.ValidateApprovalID(id); err != nil {
		return ApprovalRecord{}, false, err
	}
	if decision != core.ApprovalApproved && decision != core.ApprovalDenied {
		return ApprovalRecord{}, false, fmt.Errorf("approval decision must be approved or denied")
	}
	if err := validateApprovalActor(actor); err != nil {
		return ApprovalRecord{}, false, err
	}
	return s.decideAndResume(ctx, id, decision, actor, time.Now().UTC())
}

func (s *SQLApprovalStore) ExpireApprovals(ctx context.Context, now time.Time) (int64, error) {
	if now.IsZero() {
		return 0, fmt.Errorf("approval expiry time is zero")
	}
	rows, err := s.db.QueryContext(ctx, sqlExpiredApprovals.bind(s.dialect), now.UTC().UnixMilli())
	if err != nil {
		return 0, err
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, err
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	var expired int64
	for _, id := range ids {
		_, changed, err := s.decideAndResume(ctx, id, core.ApprovalExpired, "system:expiry", now.UTC())
		if err != nil {
			return expired, err
		}
		if changed {
			expired++
		}
	}
	return expired, nil
}

func (s *SQLApprovalStore) CancelRunApprovals(ctx context.Context, runID, actor string) (int64, error) {
	if err := core.ValidateRunID(runID); err != nil {
		return 0, err
	}
	if err := validateApprovalActor(actor); err != nil {
		return 0, err
	}
	rows, err := s.db.QueryContext(ctx, sqlPendingRunApprovals.bind(s.dialect), runID)
	if err != nil {
		return 0, err
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, err
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	now := time.Now().UTC().UnixMilli()
	var count int64
	for _, id := range ids {
		result, err := s.db.ExecContext(ctx, sqlDecideApproval.bind(s.dialect), string(core.ApprovalDenied), now, actor, id)
		if err != nil {
			return count, err
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return count, err
		}
		count += affected
	}
	return count, nil
}

func (s *SQLApprovalStore) decideAndResume(ctx context.Context, id string, decision core.ApprovalDecision, actor string, now time.Time) (ApprovalRecord, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ApprovalRecord{}, false, err
	}
	defer func() { _ = tx.Rollback() }()
	record, err := scanApproval(tx.QueryRowContext(ctx, sqlGetApproval.bind(s.dialect), id))
	if err != nil {
		return ApprovalRecord{}, false, err
	}
	if record.Status != core.ApprovalPending {
		return record, false, nil
	}
	result, err := tx.ExecContext(ctx, sqlDecideApproval.bind(s.dialect), string(decision), now.UnixMilli(), actor, id)
	if err != nil {
		return ApprovalRecord{}, false, err
	}
	affected, err := result.RowsAffected()
	if err != nil || affected == 0 {
		return ApprovalRecord{}, false, err
	}
	resumeSessionID, resumeRunID, err := delegatedApprovalResumeTarget(ctx, tx, s.dialect, record)
	if err != nil {
		return ApprovalRecord{}, false, err
	}
	resumed, err := tx.ExecContext(ctx, sqlResumeApprovedRun.bind(s.dialect), now.UnixMilli(), resumeRunID, resumeSessionID, record.TenantID, record.SubjectID)
	if err != nil {
		return ApprovalRecord{}, false, err
	}
	resumedRows, err := resumed.RowsAffected()
	if err != nil {
		return ApprovalRecord{}, false, err
	}
	if resumedRows == 0 {
		return ApprovalRecord{}, false, fmt.Errorf("approval run is not waiting for a decision")
	}
	queueResult, err := tx.ExecContext(ctx, sqlWakeRunQueue.bind(s.dialect), now.UnixMilli(), resumeRunID, resumeSessionID, record.TenantID, record.SubjectID)
	if err != nil {
		return ApprovalRecord{}, false, err
	}
	queueRows, err := queueResult.RowsAffected()
	if err != nil {
		return ApprovalRecord{}, false, err
	}
	if queueRows == 0 {
		return ApprovalRecord{}, false, fmt.Errorf("approval run has no durable continuation")
	}
	if err := tx.Commit(); err != nil {
		return ApprovalRecord{}, false, err
	}
	record.Status, record.DecidedAt, record.DecidedBy = decision, now, actor
	return record, true, nil
}

// delegatedApprovalResumeTarget maps a child approval to its one queued
// parent. A nested link cannot be resumed from this transaction because only
// the server-owned parent has a durable queue continuation.
func delegatedApprovalResumeTarget(ctx context.Context, tx *sql.Tx, dialect SQLDialect, record ApprovalRecord) (string, string, error) {
	var sessionID, tenantID, subjectID string
	directQuery := `SELECT session_id, tenant_id, subject_id FROM run_control WHERE run_id = ?`
	err := tx.QueryRowContext(ctx, (sqlQuery{directQuery}).bind(dialect), record.RunID).Scan(&sessionID, &tenantID, &subjectID)
	if err == nil {
		if sessionID != record.SessionID || tenantID != record.TenantID || subjectID != record.SubjectID {
			return "", "", fmt.Errorf("approval run identity does not match its durable control record")
		}
		return record.SessionID, record.RunID, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", "", err
	}
	query := `SELECT ` + delegationLinkColumns + ` FROM delegation_links WHERE child_session_id = ?`
	link, err := scanDelegationLink(tx.QueryRowContext(ctx, (sqlQuery{query}).bind(dialect), record.SessionID))
	if errors.Is(err, sql.ErrNoRows) {
		return record.SessionID, record.RunID, nil
	}
	if err != nil {
		return "", "", err
	}
	if link.ChildSessionID != record.SessionID || link.ChildRunID != record.RunID ||
		link.TenantID != record.TenantID || link.SubjectID != record.SubjectID {
		return "", "", fmt.Errorf("delegated approval identity does not match its durable link")
	}
	if link.Depth != 1 {
		return "", "", fmt.Errorf("delegated approval parent depth is not directly queue-resumable")
	}
	if err := core.ValidateSessionID(link.ParentSessionID); err != nil {
		return "", "", fmt.Errorf("delegated approval parent session is invalid: %w", err)
	}
	if err := core.ValidateRunID(link.ParentRunID); err != nil {
		return "", "", fmt.Errorf("delegated approval parent run is invalid: %w", err)
	}
	return link.ParentSessionID, link.ParentRunID, nil
}

func approvalInvocation(request core.ApprovalRequest) (core.ToolInvocation, error) {
	if request.Manifest.ID != request.ToolCall.Name {
		return core.ToolInvocation{}, fmt.Errorf("approval manifest %q does not match tool %q", request.Manifest.ID, request.ToolCall.Name)
	}
	return core.NewToolInvocation(core.RunInfo{
		RunID: request.RunID, SessionID: request.SessionID, Principal: request.Principal,
	}, request.ToolCall, request.Manifest.Idempotent)
}

func approvalID(invocation core.ToolInvocation, manifestDigest string) string {
	value := strings.Join([]string{
		invocation.TenantID, invocation.SubjectID, invocation.SessionID, invocation.RunID,
		invocation.CallID, invocation.CapabilityID, invocation.ArgsDigest, manifestDigest,
	}, "\x00")
	sum := sha256.Sum256([]byte(value))
	return "apr_" + hex.EncodeToString(sum[:])
}

func approvalMatches(record ApprovalRecord, invocation core.ToolInvocation, manifestDigest string) bool {
	return record.TenantID == invocation.TenantID && record.SubjectID == invocation.SubjectID &&
		record.SessionID == invocation.SessionID && record.RunID == invocation.RunID &&
		record.CallID == invocation.CallID && record.CapabilityID == invocation.CapabilityID &&
		record.ArgsDigest == invocation.ArgsDigest && record.ManifestDigest == manifestDigest
}

func approvalResolution(record ApprovalRecord) core.ApprovalResolution {
	return core.ApprovalResolution{
		ApprovalID: record.ID, Decision: record.Status, RequestedAt: record.RequestedAt, ExpiresAt: record.ExpiresAt,
		DecidedAt: record.DecidedAt, DecidedBy: record.DecidedBy,
	}
}

func approvalManifestDigest(manifest core.CapabilityManifest) (string, error) {
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return "", fmt.Errorf("encode approval manifest: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

func validateApprovalActor(actor string) error {
	if strings.TrimSpace(actor) == "" || len(actor) > 512 || strings.ContainsAny(actor, "\r\n\x00") {
		return fmt.Errorf("approval actor is invalid")
	}
	return nil
}

func validApprovalStatus(status core.ApprovalDecision) bool {
	switch status {
	case core.ApprovalPending, core.ApprovalApproved, core.ApprovalDenied, core.ApprovalExpired:
		return true
	default:
		return false
	}
}

func scanApproval(scanner runScanner) (ApprovalRecord, error) {
	var record ApprovalRecord
	var requestJSON, status string
	var requestedMillis, expiresMillis, decidedMillis int64
	err := scanner.Scan(
		&record.ID, &record.TenantID, &record.SubjectID, &record.SessionID, &record.RunID,
		&record.CallID, &record.CapabilityID, &record.ArgsDigest, &record.ManifestDigest, &requestJSON, &status,
		&requestedMillis, &expiresMillis, &decidedMillis, &record.DecidedBy,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return ApprovalRecord{}, ErrApprovalNotFound
	}
	if err != nil {
		return ApprovalRecord{}, err
	}
	record.Status = core.ApprovalDecision(status)
	if !validApprovalStatus(record.Status) {
		return ApprovalRecord{}, fmt.Errorf("approval %s has invalid status %q", record.ID, status)
	}
	if err := json.Unmarshal([]byte(requestJSON), &record.Request); err != nil {
		return ApprovalRecord{}, fmt.Errorf("decode approval request: %w", err)
	}
	record.RequestedAt = time.UnixMilli(requestedMillis).UTC()
	record.ExpiresAt = time.UnixMilli(expiresMillis).UTC()
	if decidedMillis > 0 {
		record.DecidedAt = time.UnixMilli(decidedMillis).UTC()
	}
	return record, nil
}

var _ ApprovalStore = (*SQLApprovalStore)(nil)
