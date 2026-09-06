package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"strings"
	"time"
)

// SessionSummary is one row of the session catalog.
type SessionSummary struct {
	ID         string    `json:"id"`
	TenantID   string    `json:"tenant_id"`
	UserID     string    `json:"user_id"`
	ProfileID  string    `json:"profile_id"`
	EventCount int64     `json:"event_count"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// SessionLister is implemented by stores that can list the session catalog.
// Callers type-assert: the core core.SessionStore interface stays minimal.
type SessionLister interface {
	ListSessions(ctx context.Context, tenantID, userID string, limit int) ([]SessionSummary, error)
}

// SQLSessionStore persists sessions in any database/sql backend using the
// same commit model as the S3 store, with one structural upgrade: Save is a
// single transaction (read version -> compare -> append chunk -> advance),
// so a save is atomic and orphan chunks cannot exist.
//
// The kernel imports no SQL driver. The deployer registers one:
//
//	import _ "modernc.org/sqlite"              // demo/self-host (pure Go)
//	import _ "github.com/jackc/pgx/v5/stdlib"  // production
//
// Event payloads are stored as JSONL TEXT in chunks keyed by their start seq
// — byte-compatible with the FileSessionStore and S3SessionStore formats, so
// sessions migrate between backends by copying data.
const (
	MaxStoredSessions        = 4096
	MaxEventChunksPerSession = 4096
	MaxEventChunkBytes       = 16 << 20
)

type SQLSessionStore struct {
	db                *sql.DB
	dialect           SQLDialect
	maxReleaseHistory int
	maxSessions       int
	maxEventChunks    int
	maxEvidence       int
}

var (
	sqlInsertSession      = sqlQuery{"INSERT INTO sessions (id, version, tenant_id, user_id, profile_id, status, event_count, header, updated_at) VALUES (?, ?, ?, ?, ?, 'active', ?, ?, ?)"}
	sqlSelectSession      = sqlQuery{"SELECT version, header FROM sessions WHERE id = ?"}
	sqlCountSessions      = sqlQuery{"SELECT COUNT(*) FROM sessions"}
	sqlSelectCatalog      = sqlQuery{"SELECT id, tenant_id, user_id, profile_id, event_count, updated_at FROM sessions WHERE tenant_id = ? AND user_id = ? ORDER BY updated_at DESC LIMIT ?"}
	sqlInsertChunk        = sqlQuery{"INSERT INTO event_chunks (session_id, start_seq, payload) VALUES (?, ?, ?)"}
	sqlCountSessionChunks = sqlQuery{"SELECT COUNT(*) FROM event_chunks WHERE session_id = ?"}
	sqlSelectChunks       = sqlQuery{"SELECT start_seq, payload FROM event_chunks WHERE session_id = ? ORDER BY start_seq"}
	sqlUpdateSessionTip   = sqlQuery{"UPDATE sessions SET version = ?, event_count = ?, updated_at = ? WHERE id = ? AND version = ?"}
)

type sqlSessionRow struct {
	Version int64
	Header  core.SessionOptions
}

func (s *SQLSessionStore) sessionCap() int {
	if s != nil && s.maxSessions > 0 {
		return s.maxSessions
	}
	return MaxStoredSessions
}

func (s *SQLSessionStore) evidenceCap() int {
	if s != nil && s.maxEvidence > 0 {
		return s.maxEvidence
	}
	return MaxEvidenceRecords
}

func (s *SQLSessionStore) Create(ctx context.Context, session *core.Session) error {
	if err := core.ValidateSessionID(session.ID()); err != nil {
		return err
	}
	header := core.SessionOptions{
		ID: session.ID(), ProfileID: session.ProfileID(), Principal: session.Principal(),
		Scope: session.Scope(), Metadata: session.Metadata(),
	}
	encoded, err := json.Marshal(header)
	if err != nil {
		return err
	}
	events := session.Events()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var stored int
	if err := tx.QueryRowContext(ctx, sqlCountSessions.bind(s.dialect)).Scan(&stored); err != nil {
		return err
	}
	if stored >= s.sessionCap() {
		var existing int64
		var headerJSON string
		getErr := tx.QueryRowContext(ctx, sqlSelectSession.bind(s.dialect), session.ID()).Scan(&existing, &headerJSON)
		if getErr == nil {
			return fmt.Errorf("%w: %s", core.ErrSessionConflict, session.ID())
		}
		if !errors.Is(getErr, sql.ErrNoRows) {
			return getErr
		}
		return fmt.Errorf("stored sessions exceed maximum of %d", s.sessionCap())
	}
	if _, err := tx.ExecContext(ctx, sqlInsertSession.bind(s.dialect),
		session.ID(), int64(len(events)), header.Principal.TenantID, header.Principal.SubjectID,
		header.ProfileID, int64(len(events)), string(encoded), time.Now().UTC().UnixMilli(),
	); err != nil {
		return s.translateDuplicate(session.ID(), err)
	}
	if len(events) > 0 {
		if err := s.insertChunk(ctx, tx, session.ID(), 0, events); err != nil {
			return err
		}
		if err := insertRunEvidence(ctx, tx, s.dialect, session.ID(), header, events, s.evidenceCap()); err != nil {
			return fmt.Errorf("index session %s run evidence: %w", session.ID(), err)
		}
	}
	if err := tx.Commit(); err != nil {
		return s.translateDuplicate(session.ID(), err)
	}
	return nil
}

// isDuplicateConstraint reports whether a driver error is a unique-key
// violation, using the portable subset of surfaced messages: SQLite via
// modernc reports SQLITE_CONSTRAINT codes, PostgreSQL reports SQLSTATE 23505.
func isDuplicateConstraint(err error) bool {
	if err == nil {
		return false
	}
	text := err.Error()
	return strings.Contains(text, "SQLITE_CONSTRAINT") || strings.Contains(text, "23505") ||
		strings.Contains(text, "UNIQUE constraint") || strings.Contains(text, "duplicate key")
}

// duplicateAsConflict maps backend duplicate-key errors onto
// core.ErrSessionConflict for any named resource.
func duplicateAsConflict(id string, err error) error {
	if err == nil {
		return nil
	}
	if isDuplicateConstraint(err) {
		return fmt.Errorf("%w: %s", core.ErrSessionConflict, id)
	}
	return err
}

// translateDuplicate maps backend duplicate-key errors onto core.ErrSessionConflict.
func (s *SQLSessionStore) translateDuplicate(id string, err error) error {
	return duplicateAsConflict(id, err)
}

func (s *SQLSessionStore) eventChunkCap() int {
	if s != nil && s.maxEventChunks > 0 {
		return s.maxEventChunks
	}
	return MaxEventChunksPerSession
}

func (s *SQLSessionStore) insertChunk(ctx context.Context, tx *sql.Tx, id string, startSeq int64, events []core.SessionEvent) error {
	var payload strings.Builder
	for _, event := range events {
		encoded, err := json.Marshal(event)
		if err != nil {
			return err
		}
		payload.Write(encoded)
		payload.WriteByte('\n')
		if payload.Len() > MaxEventChunkBytes {
			return fmt.Errorf("event chunk exceeds %d bytes", MaxEventChunkBytes)
		}
	}
	var stored int
	if err := tx.QueryRowContext(ctx, sqlCountSessionChunks.bind(s.dialect), id).Scan(&stored); err != nil {
		return err
	}
	if stored >= s.eventChunkCap() {
		return fmt.Errorf("session event chunks exceed maximum of %d", s.eventChunkCap())
	}
	_, err := tx.ExecContext(ctx, sqlInsertChunk.bind(s.dialect), id, startSeq, payload.String())
	return err
}

func (s *SQLSessionStore) Load(ctx context.Context, id string) (*core.Session, error) {
	if err := core.ValidateSessionID(id); err != nil {
		return nil, err
	}
	row, err := s.loadSessionRow(ctx, id)
	if err != nil {
		return nil, err
	}
	session, committed, err := s.restore(ctx, id, row)
	if err != nil {
		return nil, err
	}
	synthetic := core.RepairInterrupted(session.Events())
	if len(synthetic) > 0 {
		if err := core.AppendRepair(session, synthetic, nil); err != nil {
			return nil, err
		}
		if err := s.Save(ctx, session, committed); err != nil {
			return nil, fmt.Errorf("persist repair for session %s: %w", id, err)
		}
	}
	return session, nil
}

func (s *SQLSessionStore) loadSessionRow(ctx context.Context, id string) (sqlSessionRow, error) {
	var row sqlSessionRow
	var header string
	err := s.db.QueryRowContext(ctx, sqlSelectSession.bind(s.dialect), id).Scan(&row.Version, &header)
	if errors.Is(err, sql.ErrNoRows) {
		return sqlSessionRow{}, fmt.Errorf("%w: %s", core.ErrSessionNotFound, id)
	}
	if err != nil {
		return sqlSessionRow{}, err
	}
	if err := json.Unmarshal([]byte(header), &row.Header); err != nil {
		return sqlSessionRow{}, fmt.Errorf("decode session %s header: %w", id, err)
	}
	return row, nil
}

func (s *SQLSessionStore) restore(ctx context.Context, id string, row sqlSessionRow) (*core.Session, int64, error) {
	rows, err := s.db.QueryContext(ctx, sqlSelectChunks.bind(s.dialect), id)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var events []core.SessionEvent
	nextSeq := int64(0)
	for rows.Next() {
		var startSeq int64
		var payload string
		if err := rows.Scan(&startSeq, &payload); err != nil {
			return nil, 0, err
		}
		if startSeq != nextSeq {
			return nil, 0, fmt.Errorf("session %s chunk sequence discontinuous at %d", id, startSeq)
		}
		decoded := 0
		for _, line := range strings.Split(payload, "\n") {
			if strings.TrimSpace(line) == "" {
				continue
			}
			if len(line) > core.MaxSessionEventDataBytes+(1<<20) {
				return nil, 0, fmt.Errorf("decode session %s event: encoded event exceeds size limit", id)
			}
			var event core.SessionEvent
			if err := json.Unmarshal([]byte(line), &event); err != nil {
				return nil, 0, fmt.Errorf("decode session %s event: %w", id, err)
			}
			events = append(events, event)
			decoded++
		}
		nextSeq += int64(decoded)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	// Defensive: trust only the committed version prefix.
	if int64(len(events)) > row.Version {
		events = events[:row.Version]
	}
	session, err := core.RestoreSession(row.Header, events)
	if err != nil {
		return nil, 0, err
	}
	return session, row.Version, nil
}

func (s *SQLSessionStore) Save(ctx context.Context, session *core.Session, expectedVersion int64) error {
	if session.Version() < expectedVersion {
		return fmt.Errorf("session %s history shrank from %d to %d events", session.ID(), expectedVersion, session.Version())
	}
	return s.AppendEvents(ctx, session.ID(), expectedVersion, session.EventsFrom(expectedVersion))
}

func (s *SQLSessionStore) AppendEvents(ctx context.Context, sessionID string, expectedVersion int64, events []core.SessionEvent) error {
	if err := core.ValidateSessionID(sessionID); err != nil {
		return err
	}
	if err := validateAppendEvents(expectedVersion, events); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	var committed int64
	var header string
	err = tx.QueryRowContext(ctx, sqlSelectSession.bind(s.dialect), sessionID).Scan(&committed, &header)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: %s", core.ErrSessionNotFound, sessionID)
	}
	if err != nil {
		return err
	}
	if committed != expectedVersion {
		return fmt.Errorf("%w: expected %d, found %d", core.ErrSessionConflict, expectedVersion, committed)
	}
	if len(events) > 0 {
		if err := s.insertChunk(ctx, tx, sessionID, expectedVersion, events); err != nil {
			return err
		}
		var options core.SessionOptions
		if err := json.Unmarshal([]byte(header), &options); err != nil {
			return fmt.Errorf("decode session %s header for evidence: %w", sessionID, err)
		}
		if err := insertRunEvidence(ctx, tx, s.dialect, sessionID, options, events, s.evidenceCap()); err != nil {
			return fmt.Errorf("index session %s run evidence: %w", sessionID, err)
		}
	}
	targetVersion := expectedVersion + int64(len(events))
	result, err := tx.ExecContext(ctx, sqlUpdateSessionTip.bind(s.dialect),
		targetVersion, targetVersion, time.Now().UTC().UnixMilli(),
		sessionID, committed,
	)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return fmt.Errorf("%w: expected %d, found a newer tip", core.ErrSessionConflict, expectedVersion)
	}
	return tx.Commit()
}

// ListSessions returns the caller's session catalog, newest first. It
// implements SessionLister; the sessions table IS the metadata sidecar.
func (s *SQLSessionStore) ListSessions(ctx context.Context, tenantID, userID string, limit int) ([]SessionSummary, error) {
	if err := validateSQLTextFilter("session tenant_id", tenantID); err != nil {
		return nil, err
	}
	if err := validateSQLTextFilter("session user_id", userID); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, sqlSelectCatalog.bind(s.dialect), tenantID, userID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []SessionSummary{}
	for rows.Next() {
		var summary SessionSummary
		var updatedMillis int64
		if err := rows.Scan(&summary.ID, &summary.TenantID, &summary.UserID, &summary.ProfileID, &summary.EventCount, &updatedMillis); err != nil {
			return nil, err
		}
		summary.UpdatedAt = time.UnixMilli(updatedMillis).UTC()
		out = append(out, summary)
	}
	return out, rows.Err()
}
