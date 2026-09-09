package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

const (
	// RouteEvidenceReceiptProtocol identifies the content-free receipt format.
	// Its payload is a projection; Session events and the Tool Journal remain
	// the canonical source of execution evidence.
	RouteEvidenceReceiptProtocol = "route_evidence_receipt/v1"

	MaxRouteEvidencePayloadBytes     = 32 << 10
	maxRouteEvidenceErrorClass       = 64
	routeEvidenceSQLRetries          = 8
	routeEvidenceIdentityVerified    = "verified"
	routeEvidenceIdentityUnavailable = "unavailable"
	routeEvidenceV2Implementation    = "programmatic-route-projection/v2-probe-once-host-projected-ptc-choice-capacity-admission"
)

var routeEvidenceV1Fields = map[string]struct{}{
	"run.id": {}, "session.id": {}, "run.status": {},
	"programmatic.route.identity.status": {}, "programmatic.route.version": {}, "programmatic.route.mode": {}, "programmatic.route.implementation": {},
	"programmatic.route.selection": {}, "programmatic.route.selection.status": {},
	"programmatic.route.coverage.status": {}, "programmatic.route.coverage.candidates": {}, "programmatic.route.coverage.selected": {}, "programmatic.route.coverage.executed": {}, "programmatic.route.coverage.duplicate": {}, "programmatic.route.coverage.exact_once": {},
	"programmatic.route.effects.journal.status": {}, "programmatic.route.effects.journal.completed": {}, "programmatic.route.effects.external.status": {},
	"programmatic.route.accounting.status": {}, "programmatic.route.accounting.model_calls": {},
	"programmatic.route.context.archive.status": {}, "programmatic.route.context.assembly.status": {}, "programmatic.route.context.summary_events": {},
}

// RouteEvidenceReceiptInput is the storage-owned boundary for a route evidence
// projection. Payload must be the exact bounded v1 fact object and payload
// SHA-256 must match its canonical bytes. Storage enforces that content-free
// boundary without importing route-internal types.
type RouteEvidenceReceiptInput struct {
	Protocol             string
	SessionID            string
	RunID                string
	TerminalEventSeq     int64
	SourceSessionVersion int64
	TerminalStatus       string
	Payload              json.RawMessage
	PayloadSHA256        string
	CreatedAt            time.Time
}

// RouteEvidenceReceipt is an immutable audit projection. Delivery state is
// deliberately absent and is represented by RouteEvidenceOutboxClaim instead.
type RouteEvidenceReceipt struct {
	ReceiptID            string          `json:"receipt_id"`
	Protocol             string          `json:"protocol"`
	SessionID            string          `json:"session_id"`
	RunID                string          `json:"run_id"`
	TerminalEventSeq     int64           `json:"terminal_event_seq"`
	SourceSessionVersion int64           `json:"source_session_version"`
	TerminalStatus       string          `json:"terminal_status"`
	Payload              json.RawMessage `json:"payload"`
	PayloadSHA256        string          `json:"payload_sha256"`
	CreatedAt            time.Time       `json:"created_at"`
}

// RouteEvidenceOutboxClaim is a fenced, temporary delivery lease. A successful
// remote delivery made after its lease expires can be delivered again; clients
// must deduplicate by Receipt.ReceiptID.
type RouteEvidenceOutboxClaim struct {
	Receipt         RouteEvidenceReceipt
	WorkerID        string    `json:"worker_id"`
	LeaseGeneration int64     `json:"lease_generation"`
	LeaseExpiresAt  time.Time `json:"lease_expires_at"`
	Attempts        int64     `json:"attempts"`
}

// RouteEvidenceOutboxStore is intentionally small. It lets a server-side
// dispatcher materialize immutable receipts and process their durable delivery
// queue without exposing SQL or route-internal evidence types.
type RouteEvidenceOutboxStore interface {
	CreateOrLoadRouteEvidenceReceipt(context.Context, RouteEvidenceReceiptInput) (RouteEvidenceReceipt, bool, error)
	ClaimRouteEvidenceReceipt(context.Context, string, time.Duration) (RouteEvidenceOutboxClaim, bool, error)
	AckRouteEvidenceReceipt(context.Context, string, string, int64) (bool, error)
	RetryRouteEvidenceReceipt(context.Context, string, string, int64, time.Time, string) (bool, error)
}

var _ RouteEvidenceOutboxStore = (*SQLSessionStore)(nil)

const MaxRouteEvidenceTerminalCandidates = 256

// RouteEvidenceTerminalCandidate is a discovery record only. A reconciler
// must re-read and validate the canonical Session and Tool Journal before it
// materializes a receipt.
type RouteEvidenceTerminalCandidate struct {
	RunID     string `json:"run_id"`
	SessionID string `json:"session_id"`
	TenantID  string `json:"tenant_id"`
	SubjectID string `json:"subject_id"`
	Status    string `json:"status"`
}

// RouteEvidenceTerminalCursor advances a stable completed_at/run_id keyset
// page. It is not execution evidence and is never persisted in a receipt.
type RouteEvidenceTerminalCursor struct {
	CompletedAt time.Time
	RunID       string
}

// RouteEvidenceTerminalCandidateFinder is optional discovery support for a
// server reconciler. It intentionally stays outside RouteEvidenceOutboxStore.
type RouteEvidenceTerminalCandidateFinder interface {
	ListTerminalRouteEvidenceCandidates(context.Context, RouteEvidenceTerminalCursor, int) ([]RouteEvidenceTerminalCandidate, RouteEvidenceTerminalCursor, error)
}

var _ RouteEvidenceTerminalCandidateFinder = (*SQLSessionStore)(nil)

var (
	sqlSelectRouteEvidenceReceipt = sqlQuery{`SELECT receipt_id, protocol, session_id, run_id, terminal_event_seq, source_session_version,
		terminal_status, payload_json, payload_sha256, created_at
		FROM route_evidence_receipts WHERE session_id = ? AND run_id = ? AND terminal_event_seq = ?`}
	sqlSelectRouteEvidenceReceiptForUpdate = sqlQuery{`SELECT receipt_id, protocol, session_id, run_id, terminal_event_seq, source_session_version,
		terminal_status, payload_json, payload_sha256, created_at
		FROM route_evidence_receipts WHERE session_id = ? AND run_id = ? AND terminal_event_seq = ? FOR UPDATE`}
	sqlInsertRouteEvidenceReceipt = sqlQuery{`INSERT INTO route_evidence_receipts
		(receipt_id, protocol, session_id, run_id, terminal_event_seq, source_session_version, terminal_status, payload_json, payload_sha256, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`}
	sqlInsertRouteEvidenceOutbox = sqlQuery{`INSERT INTO route_evidence_outbox
		(receipt_id, state, available_at, lease_owner, lease_generation, lease_expires_at, attempts, last_error_class, delivered_at)
		VALUES (?, 'pending', ?, '', 0, 0, 0, '', 0)`}
	sqlSelectRouteEvidenceOutboxForReceipt = sqlQuery{`SELECT state FROM route_evidence_outbox WHERE receipt_id = ?`}
	sqlClaimRouteEvidenceCandidate         = sqlQuery{`SELECT r.receipt_id, r.protocol, r.session_id, r.run_id, r.terminal_event_seq, r.source_session_version,
		r.terminal_status, r.payload_json, r.payload_sha256, r.created_at,
		o.lease_generation, o.attempts
		FROM route_evidence_outbox o JOIN route_evidence_receipts r ON r.receipt_id = o.receipt_id
		WHERE (o.state = 'pending' AND o.available_at <= ?)
			OR (o.state = 'leased' AND o.lease_expires_at <= ?)
		ORDER BY CASE WHEN o.state = 'pending' THEN o.available_at ELSE o.lease_expires_at END, o.receipt_id
		LIMIT 1`}
	sqlClaimRouteEvidenceCandidatePostgres = sqlQuery{`SELECT r.receipt_id, r.protocol, r.session_id, r.run_id, r.terminal_event_seq, r.source_session_version,
		r.terminal_status, r.payload_json, r.payload_sha256, r.created_at,
		o.lease_generation, o.attempts
		FROM route_evidence_outbox o JOIN route_evidence_receipts r ON r.receipt_id = o.receipt_id
		WHERE (o.state = 'pending' AND o.available_at <= ?)
			OR (o.state = 'leased' AND o.lease_expires_at <= ?)
		ORDER BY CASE WHEN o.state = 'pending' THEN o.available_at ELSE o.lease_expires_at END, o.receipt_id
		LIMIT 1 FOR UPDATE OF o SKIP LOCKED`}
	sqlClaimRouteEvidenceOutbox = sqlQuery{`UPDATE route_evidence_outbox
		SET state = 'leased', lease_owner = ?, lease_generation = lease_generation + 1,
			lease_expires_at = ?, attempts = attempts + 1, last_error_class = ''
		WHERE receipt_id = ? AND lease_generation = ?
			AND ((state = 'pending' AND available_at <= ?) OR (state = 'leased' AND lease_expires_at <= ?))`}
	sqlAckRouteEvidenceOutbox = sqlQuery{`UPDATE route_evidence_outbox
		SET state = 'delivered', lease_owner = '', lease_expires_at = 0, delivered_at = ?
		WHERE receipt_id = ? AND state = 'leased' AND lease_owner = ? AND lease_generation = ? AND lease_expires_at > ?`}
	sqlRetryRouteEvidenceOutbox = sqlQuery{`UPDATE route_evidence_outbox
		SET state = 'pending', available_at = ?, lease_owner = '', lease_expires_at = 0, last_error_class = ?
		WHERE receipt_id = ? AND state = 'leased' AND lease_owner = ? AND lease_generation = ? AND lease_expires_at > ?`}
	sqlListTerminalRouteEvidenceCandidates = sqlQuery{`SELECT rc.run_id, rc.session_id, rc.tenant_id, rc.subject_id, rc.status, rc.completed_at
		FROM run_control rc
		WHERE rc.status IN ('completed', 'limited', 'failed', 'cancelled') AND rc.completed_at > 0
			AND NOT EXISTS (SELECT 1 FROM route_evidence_receipts receipt
				WHERE receipt.session_id = rc.session_id AND receipt.run_id = rc.run_id)
			AND (rc.completed_at > ? OR (rc.completed_at = ? AND rc.run_id > ?))
		ORDER BY rc.completed_at, rc.run_id LIMIT ?`}
)

// RouteEvidenceReceiptID returns the deterministic id used by receivers to
// deduplicate at-least-once delivery. It includes the exact source Session
// cursor. Any source or payload disagreement is
// rejected by CreateOrLoadRouteEvidenceReceipt instead of making a new receipt.
func RouteEvidenceReceiptID(input RouteEvidenceReceiptInput) string {
	sum := sha256.Sum256([]byte(input.Protocol + "\x00" + input.SessionID + "\x00" + input.RunID + "\x00" +
		fmt.Sprintf("%d", input.TerminalEventSeq) + "\x00" + fmt.Sprintf("%d", input.SourceSessionVersion) + "\x00" + input.TerminalStatus + "\x00" + input.PayloadSHA256))
	return "re_" + hex.EncodeToString(sum[:])
}

// CreateOrLoadRouteEvidenceReceipt atomically creates an immutable receipt and
// its pending delivery record. Existing source identities must match exactly;
// a changed payload or digest fails closed rather than overwriting audit data.
func (s *SQLSessionStore) CreateOrLoadRouteEvidenceReceipt(ctx context.Context, input RouteEvidenceReceiptInput) (RouteEvidenceReceipt, bool, error) {
	if s == nil || s.db == nil {
		return RouteEvidenceReceipt{}, false, fmt.Errorf("route evidence outbox requires an SQL session store")
	}
	if err := validateRouteEvidenceReceiptInput(&input); err != nil {
		return RouteEvidenceReceipt{}, false, err
	}
	if input.CreatedAt.IsZero() {
		input.CreatedAt = time.Now().UTC()
	}
	receipt := receiptFromRouteEvidenceInput(input)
	var lastRetryErr error
	for attempt := 0; attempt < routeEvidenceSQLRetries; attempt++ {
		stored, created, retry, err := s.createOrLoadRouteEvidenceReceipt(ctx, receipt)
		if retry {
			if err != nil {
				lastRetryErr = err
			}
			if err := waitRouteEvidenceRetry(ctx, attempt); err != nil {
				return RouteEvidenceReceipt{}, false, err
			}
			continue
		}
		if err != nil {
			return RouteEvidenceReceipt{}, false, err
		}
		return stored, created, nil
	}
	if lastRetryErr != nil {
		return RouteEvidenceReceipt{}, false, fmt.Errorf("route evidence receipt creation conflicted repeatedly: %w", lastRetryErr)
	}
	return RouteEvidenceReceipt{}, false, fmt.Errorf("route evidence receipt creation conflicted repeatedly")
}

func (s *SQLSessionStore) createOrLoadRouteEvidenceReceipt(ctx context.Context, receipt RouteEvidenceReceipt) (RouteEvidenceReceipt, bool, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return RouteEvidenceReceipt{}, false, false, err
	}
	defer func() { _ = tx.Rollback() }()
	query := sqlSelectRouteEvidenceReceipt
	if s.dialect == SQLDialectPostgres {
		query = sqlSelectRouteEvidenceReceiptForUpdate
	}
	stored, err := scanRouteEvidenceReceipt(tx.QueryRowContext(ctx, query.bind(s.dialect), receipt.SessionID, receipt.RunID, receipt.TerminalEventSeq))
	if err == nil {
		if !routeEvidenceReceiptMatches(stored, receipt) {
			return RouteEvidenceReceipt{}, false, false, fmt.Errorf("route evidence receipt source conflicts with immutable receipt")
		}
		var state string
		if err := tx.QueryRowContext(ctx, sqlSelectRouteEvidenceOutboxForReceipt.bind(s.dialect), stored.ReceiptID).Scan(&state); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return RouteEvidenceReceipt{}, false, false, fmt.Errorf("route evidence receipt is missing its outbox record")
			}
			return RouteEvidenceReceipt{}, false, false, err
		}
		if err := tx.Commit(); err != nil {
			return RouteEvidenceReceipt{}, false, isSQLiteMigrationBusy(err), err
		}
		return stored, false, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return RouteEvidenceReceipt{}, false, false, err
	}
	if _, err := tx.ExecContext(ctx, sqlInsertRouteEvidenceReceipt.bind(s.dialect), receipt.ReceiptID, receipt.Protocol,
		receipt.SessionID, receipt.RunID, receipt.TerminalEventSeq, receipt.SourceSessionVersion, receipt.TerminalStatus, string(receipt.Payload), receipt.PayloadSHA256, receipt.CreatedAt.UnixMilli()); err != nil {
		return RouteEvidenceReceipt{}, false, isSQLiteMigrationBusy(err) || isDuplicateConstraint(err), err
	}
	if _, err := tx.ExecContext(ctx, sqlInsertRouteEvidenceOutbox.bind(s.dialect), receipt.ReceiptID, receipt.CreatedAt.UnixMilli()); err != nil {
		return RouteEvidenceReceipt{}, false, isSQLiteMigrationBusy(err) || isDuplicateConstraint(err), err
	}
	if err := tx.Commit(); err != nil {
		return RouteEvidenceReceipt{}, false, isSQLiteMigrationBusy(err) || isDuplicateConstraint(err), err
	}
	return receipt, true, false, nil
}

// ClaimRouteEvidenceReceipt claims one pending or expired delivery. Lease
// generation is incremented with each claim and fences acknowledgement/retry.
func (s *SQLSessionStore) ClaimRouteEvidenceReceipt(ctx context.Context, workerID string, leaseTTL time.Duration) (RouteEvidenceOutboxClaim, bool, error) {
	if s == nil || s.db == nil {
		return RouteEvidenceOutboxClaim{}, false, fmt.Errorf("route evidence outbox requires an SQL session store")
	}
	if err := validateWorkerID(workerID); err != nil {
		return RouteEvidenceOutboxClaim{}, false, err
	}
	if leaseTTL <= 0 {
		return RouteEvidenceOutboxClaim{}, false, fmt.Errorf("route evidence lease ttl must be positive")
	}
	var lastRetryErr error
	for attempt := 0; attempt < routeEvidenceSQLRetries; attempt++ {
		claim, found, retry, err := s.claimRouteEvidenceReceipt(ctx, workerID, leaseTTL)
		if retry {
			if err != nil {
				lastRetryErr = err
			}
			if err := waitRouteEvidenceRetry(ctx, attempt); err != nil {
				return RouteEvidenceOutboxClaim{}, false, err
			}
			continue
		}
		if err != nil {
			return RouteEvidenceOutboxClaim{}, false, err
		}
		return claim, found, nil
	}
	if lastRetryErr != nil {
		return RouteEvidenceOutboxClaim{}, false, fmt.Errorf("route evidence claim conflicted repeatedly: %w", lastRetryErr)
	}
	return RouteEvidenceOutboxClaim{}, false, nil
}

func (s *SQLSessionStore) claimRouteEvidenceReceipt(ctx context.Context, workerID string, leaseTTL time.Duration) (RouteEvidenceOutboxClaim, bool, bool, error) {
	now := time.Now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return RouteEvidenceOutboxClaim{}, false, false, err
	}
	defer func() { _ = tx.Rollback() }()
	query := sqlClaimRouteEvidenceCandidate
	if s.dialect == SQLDialectPostgres {
		query = sqlClaimRouteEvidenceCandidatePostgres
	}
	var receipt RouteEvidenceReceipt
	var payload string
	var generation, attempts, createdAt int64
	err = tx.QueryRowContext(ctx, query.bind(s.dialect), now.UnixMilli(), now.UnixMilli()).Scan(
		&receipt.ReceiptID, &receipt.Protocol, &receipt.SessionID, &receipt.RunID, &receipt.TerminalEventSeq,
		&receipt.SourceSessionVersion, &receipt.TerminalStatus, &payload, &receipt.PayloadSHA256, &createdAt, &generation, &attempts,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return RouteEvidenceOutboxClaim{}, false, false, nil
	}
	if err != nil {
		return RouteEvidenceOutboxClaim{}, false, false, err
	}
	receipt.Payload = json.RawMessage([]byte(payload))
	receipt.CreatedAt = time.UnixMilli(createdAt).UTC()
	leaseExpiresAt := now.Add(leaseTTL)
	result, err := tx.ExecContext(ctx, sqlClaimRouteEvidenceOutbox.bind(s.dialect), workerID, leaseExpiresAt.UnixMilli(), receipt.ReceiptID,
		generation, now.UnixMilli(), now.UnixMilli())
	if err != nil {
		return RouteEvidenceOutboxClaim{}, false, isSQLiteMigrationBusy(err), err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return RouteEvidenceOutboxClaim{}, false, false, err
	}
	if affected != 1 {
		return RouteEvidenceOutboxClaim{}, false, true, nil
	}
	if err := tx.Commit(); err != nil {
		return RouteEvidenceOutboxClaim{}, false, isSQLiteMigrationBusy(err), err
	}
	return RouteEvidenceOutboxClaim{Receipt: receipt, WorkerID: workerID, LeaseGeneration: generation + 1,
		LeaseExpiresAt: leaseExpiresAt, Attempts: attempts + 1}, true, false, nil
}

// AckRouteEvidenceReceipt marks a claimed receipt delivered only while the
// exact claim is still live. A false nil result means a newer owner won.
func (s *SQLSessionStore) AckRouteEvidenceReceipt(ctx context.Context, receiptID, workerID string, leaseGeneration int64) (bool, error) {
	if err := validateRouteEvidenceClaim(receiptID, workerID, leaseGeneration); err != nil {
		return false, err
	}
	now := time.Now().UTC()
	result, err := s.db.ExecContext(ctx, sqlAckRouteEvidenceOutbox.bind(s.dialect), now.UnixMilli(), receiptID, workerID, leaseGeneration, now.UnixMilli())
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	return affected == 1, err
}

// RetryRouteEvidenceReceipt releases one live claim with a bounded error class.
// It never records raw exporter errors, which can contain request data.
func (s *SQLSessionStore) RetryRouteEvidenceReceipt(ctx context.Context, receiptID, workerID string, leaseGeneration int64, availableAt time.Time, errorClass string) (bool, error) {
	if err := validateRouteEvidenceClaim(receiptID, workerID, leaseGeneration); err != nil {
		return false, err
	}
	if availableAt.IsZero() {
		return false, fmt.Errorf("route evidence retry availability time is zero")
	}
	if err := validateRouteEvidenceErrorClass(errorClass); err != nil {
		return false, err
	}
	now := time.Now().UTC()
	result, err := s.db.ExecContext(ctx, sqlRetryRouteEvidenceOutbox.bind(s.dialect), availableAt.UTC().UnixMilli(), errorClass,
		receiptID, workerID, leaseGeneration, now.UnixMilli())
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	return affected == 1, err
}

// ListTerminalRouteEvidenceCandidates returns a bounded keyset page of
// terminal RunControl rows with no receipt. It is candidate discovery only;
// missing, stale, or conflicting canonical evidence must never be inferred
// from this sidecar query.
func (s *SQLSessionStore) ListTerminalRouteEvidenceCandidates(ctx context.Context, cursor RouteEvidenceTerminalCursor, limit int) ([]RouteEvidenceTerminalCandidate, RouteEvidenceTerminalCursor, error) {
	if s == nil || s.db == nil {
		return nil, RouteEvidenceTerminalCursor{}, fmt.Errorf("route evidence candidates require an SQL session store")
	}
	if cursor.CompletedAt.IsZero() && cursor.RunID != "" {
		return nil, RouteEvidenceTerminalCursor{}, fmt.Errorf("route evidence candidate cursor is invalid")
	}
	if cursor.RunID != "" {
		if err := core.ValidateRunID(cursor.RunID); err != nil {
			return nil, RouteEvidenceTerminalCursor{}, err
		}
	}
	if limit < 1 || limit > MaxRouteEvidenceTerminalCandidates {
		return nil, RouteEvidenceTerminalCursor{}, fmt.Errorf("route evidence candidate limit must be between 1 and %d", MaxRouteEvidenceTerminalCandidates)
	}
	after := cursor.CompletedAt.UTC().UnixMilli()
	rows, err := s.db.QueryContext(ctx, sqlListTerminalRouteEvidenceCandidates.bind(s.dialect), after, after, cursor.RunID, limit)
	if err != nil {
		return nil, RouteEvidenceTerminalCursor{}, err
	}
	defer rows.Close()
	candidates := make([]RouteEvidenceTerminalCandidate, 0, limit)
	next := cursor
	for rows.Next() {
		var candidate RouteEvidenceTerminalCandidate
		var completedAt int64
		if err := rows.Scan(&candidate.RunID, &candidate.SessionID, &candidate.TenantID, &candidate.SubjectID, &candidate.Status, &completedAt); err != nil {
			return nil, RouteEvidenceTerminalCursor{}, err
		}
		candidates = append(candidates, candidate)
		next = RouteEvidenceTerminalCursor{CompletedAt: time.UnixMilli(completedAt).UTC(), RunID: candidate.RunID}
	}
	if err := rows.Err(); err != nil {
		return nil, RouteEvidenceTerminalCursor{}, err
	}
	return candidates, next, nil
}

func validateRouteEvidenceReceiptInput(input *RouteEvidenceReceiptInput) error {
	if input == nil || input.Protocol != RouteEvidenceReceiptProtocol {
		return fmt.Errorf("route evidence receipt protocol is invalid")
	}
	if err := core.ValidateSessionID(input.SessionID); err != nil {
		return err
	}
	if err := core.ValidateRunID(input.RunID); err != nil {
		return err
	}
	if input.TerminalEventSeq < 0 || input.SourceSessionVersion <= input.TerminalEventSeq || !validRouteEvidenceTerminalStatus(input.TerminalStatus) {
		return fmt.Errorf("route evidence receipt terminal identity is invalid")
	}
	if len(input.Payload) < 2 || len(input.Payload) > MaxRouteEvidencePayloadBytes || !json.Valid(input.Payload) {
		return fmt.Errorf("route evidence receipt payload is invalid")
	}
	canonicalPayload, err := canonicalRouteEvidenceV1Payload(input.Payload, input.SessionID, input.RunID, input.TerminalStatus)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(canonicalPayload)
	canonicalDigest := hex.EncodeToString(sum[:])
	if !validRouteEvidenceSHA256(input.PayloadSHA256) || input.PayloadSHA256 != canonicalDigest {
		return fmt.Errorf("route evidence receipt payload digest does not match payload")
	}
	input.Payload = canonicalPayload
	input.PayloadSHA256 = canonicalDigest
	return nil
}

// canonicalRouteEvidenceV1Payload is the content boundary for the durable
// receipt. It accepts only the exact fixed v1 fact set and rewrites it to
// canonical JSON before hashing or storage. Session and Tool Journal evidence
// remain canonical; this projection must never become a content carrier.
func canonicalRouteEvidenceV1Payload(payload []byte, sessionID, runID, terminalStatus string) (json.RawMessage, error) {
	var values map[string]string
	if err := json.Unmarshal(payload, &values); err != nil || values == nil {
		return nil, fmt.Errorf("route evidence receipt payload must be a string-valued JSON object")
	}
	for key, value := range values {
		if _, ok := routeEvidenceV1Fields[key]; !ok || len(value) > 256 || strings.ContainsAny(value, "\x00\r\n") {
			return nil, fmt.Errorf("route evidence receipt payload contains an invalid fact")
		}
	}
	if values["session.id"] != sessionID || values["run.id"] != runID || values["run.status"] != terminalStatus {
		return nil, fmt.Errorf("route evidence receipt payload terminal facts do not match identity")
	}
	identity := values["programmatic.route.identity.status"]
	switch identity {
	case routeEvidenceIdentityUnavailable:
		if len(values) != 4 {
			return nil, fmt.Errorf("unavailable route evidence receipt must contain only identity facts")
		}
	case routeEvidenceIdentityVerified:
		if len(values) != len(routeEvidenceV1Fields) {
			return nil, fmt.Errorf("verified route evidence receipt is missing fixed facts")
		}
		if err := validateVerifiedRouteEvidenceV1(values); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("route evidence receipt identity status is invalid")
	}
	canonical, err := json.Marshal(values)
	if err != nil {
		return nil, fmt.Errorf("canonicalize route evidence receipt payload: %w", err)
	}
	return json.RawMessage(canonical), nil
}

func validateVerifiedRouteEvidenceV1(values map[string]string) error {
	if values["programmatic.route.version"] != "2" || values["programmatic.route.mode"] != "auto_probe_once" || values["programmatic.route.implementation"] != routeEvidenceV2Implementation ||
		!oneOfRouteEvidence(values["programmatic.route.selection"], "unavailable", "direct", "ptc") ||
		!oneOfRouteEvidence(values["programmatic.route.selection.status"], "verified", "unavailable") ||
		!oneOfRouteEvidence(values["programmatic.route.coverage.status"], "complete", "incomplete", "unavailable") ||
		!oneOfRouteEvidence(values["programmatic.route.effects.journal.status"], "verified", "unavailable") ||
		values["programmatic.route.effects.external.status"] != "unavailable" ||
		!oneOfRouteEvidence(values["programmatic.route.accounting.status"], "verified", "unavailable") ||
		!oneOfRouteEvidence(values["programmatic.route.context.archive.status"], "verified", "unavailable") ||
		values["programmatic.route.context.assembly.status"] != "unavailable" ||
		!oneOfRouteEvidence(values["programmatic.route.coverage.duplicate"], "true", "false") ||
		!oneOfRouteEvidence(values["programmatic.route.coverage.exact_once"], "true", "false") {
		return fmt.Errorf("verified route evidence receipt contains invalid fixed facts")
	}
	for _, key := range []string{"programmatic.route.coverage.candidates", "programmatic.route.coverage.selected", "programmatic.route.coverage.executed", "programmatic.route.effects.journal.completed", "programmatic.route.accounting.model_calls", "programmatic.route.context.summary_events"} {
		if value, err := strconv.ParseInt(values[key], 10, 64); err != nil || value < 0 {
			return fmt.Errorf("verified route evidence receipt contains an invalid count")
		}
	}
	return nil
}

func oneOfRouteEvidence(value string, allowed ...string) bool {
	for _, candidate := range allowed {
		if value == candidate {
			return true
		}
	}
	return false
}

func receiptFromRouteEvidenceInput(input RouteEvidenceReceiptInput) RouteEvidenceReceipt {
	return RouteEvidenceReceipt{ReceiptID: RouteEvidenceReceiptID(input), Protocol: input.Protocol, SessionID: input.SessionID,
		RunID: input.RunID, TerminalEventSeq: input.TerminalEventSeq, SourceSessionVersion: input.SourceSessionVersion, TerminalStatus: input.TerminalStatus,
		Payload: append(json.RawMessage(nil), input.Payload...), PayloadSHA256: input.PayloadSHA256, CreatedAt: input.CreatedAt.UTC()}
}

func scanRouteEvidenceReceipt(row interface{ Scan(...any) error }) (RouteEvidenceReceipt, error) {
	var receipt RouteEvidenceReceipt
	var createdAt int64
	var payload string
	if err := row.Scan(&receipt.ReceiptID, &receipt.Protocol, &receipt.SessionID, &receipt.RunID, &receipt.TerminalEventSeq, &receipt.SourceSessionVersion,
		&receipt.TerminalStatus, &payload, &receipt.PayloadSHA256, &createdAt); err != nil {
		return RouteEvidenceReceipt{}, err
	}
	receipt.Payload = json.RawMessage([]byte(payload))
	receipt.CreatedAt = time.UnixMilli(createdAt).UTC()
	return receipt, nil
}

func routeEvidenceReceiptMatches(left, right RouteEvidenceReceipt) bool {
	return left.ReceiptID == right.ReceiptID && left.Protocol == right.Protocol && left.SessionID == right.SessionID &&
		left.RunID == right.RunID && left.TerminalEventSeq == right.TerminalEventSeq && left.SourceSessionVersion == right.SourceSessionVersion && left.TerminalStatus == right.TerminalStatus &&
		left.PayloadSHA256 == right.PayloadSHA256 && string(left.Payload) == string(right.Payload)
}

func validateRouteEvidenceClaim(receiptID, workerID string, leaseGeneration int64) error {
	if !validRouteEvidenceReceiptID(receiptID) {
		return fmt.Errorf("route evidence receipt id is invalid")
	}
	if err := validateWorkerID(workerID); err != nil {
		return err
	}
	if leaseGeneration < 1 {
		return fmt.Errorf("route evidence lease generation must be positive")
	}
	return nil
}

func validateRouteEvidenceErrorClass(value string) error {
	if value == "" || len(value) > maxRouteEvidenceErrorClass {
		return fmt.Errorf("route evidence error class is invalid")
	}
	for _, r := range value {
		if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '.' || r == '_' || r == '-') {
			return fmt.Errorf("route evidence error class is invalid")
		}
	}
	return nil
}

func validRouteEvidenceTerminalStatus(value string) bool {
	switch value {
	case "completed", "limited", "failed", "cancelled":
		return true
	default:
		return false
	}
}

func validRouteEvidenceSHA256(value string) bool {
	if len(value) != 64 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func validRouteEvidenceReceiptID(value string) bool {
	return len(value) == 67 && strings.HasPrefix(value, "re_") && validRouteEvidenceSHA256(value[3:])
}

func waitRouteEvidenceRetry(ctx context.Context, attempt int) error {
	delay := time.Duration(attempt+1) * time.Millisecond
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
