package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	core "github.com/whhhh1500/auto-agent/pkg/core"
	"time"
)

// AuditEvent is one durable administrative action. Details must never carry
// secret values (credential values, passwords) — writers are responsible for
// passing only structural metadata.
type AuditEvent struct {
	ID       string         `json:"id"`
	Time     time.Time      `json:"time"`
	Actor    string         `json:"actor"`
	Role     string         `json:"role,omitempty"`
	TenantID string         `json:"tenant,omitempty"`
	Action   string         `json:"action"`
	Target   string         `json:"target,omitempty"`
	Detail   map[string]any `json:"detail,omitempty"`
	RemoteIP string         `json:"remote_ip,omitempty"`
}

// AuditFilter narrows an audit query.
type AuditFilter struct {
	Actor    string
	Action   string
	TenantID string
	Limit    int
	Offset   int
}

// RetentionPruner is implemented by stores with bounded tables that need
// periodic cleanup (audit events, obs hits, expired leases).
type RetentionPruner interface {
	PruneAudit(ctx context.Context, olderThan time.Time) (int64, error)
	PruneHits(ctx context.Context, olderThan time.Time) (int64, error)
	PruneExpiredLeases(ctx context.Context) (int64, error)
	PruneToolInvocations(ctx context.Context, olderThan time.Time) (int64, error)
	PruneApprovals(ctx context.Context, olderThan time.Time) (int64, error)
	PruneRunSubmissions(ctx context.Context, olderThan time.Time) (int64, error)
}

// AuditStore durably records administrative actions. Best-effort by design:
// callers log the event even when the store errors, so the file log always
// carries the trail.
type AuditStore interface {
	RecordAudit(ctx context.Context, event AuditEvent) error
	ListAudit(ctx context.Context, filter AuditFilter) ([]AuditEvent, int, error)
}

const (
	MaxAuditEvents      = 8192
	MaxAuditDetailBytes = 16 << 10
)

var (
	sqlInsertAudit = sqlQuery{"INSERT INTO audit_events (id, time, actor, role, tenant, action, target, detail, remote_ip) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"}
	sqlCountAudit  = sqlQuery{"SELECT COUNT(*) FROM audit_events"}
)

// SQLAuditStore implements AuditStore on the shared schema.
type SQLAuditStore struct {
	db        *sql.DB
	dialect   SQLDialect
	maxEvents int
}

func (s *SQLAuditStore) eventCap() int {
	if s != nil && s.maxEvents > 0 {
		return s.maxEvents
	}
	return MaxAuditEvents
}

func NewSQLAuditStore(db *sql.DB, dialect SQLDialect) (*SQLAuditStore, error) {
	if db == nil {
		return nil, fmt.Errorf("sql audit store requires a database handle")
	}
	if err := validateSQLDialect(dialect); err != nil {
		return nil, err
	}
	return &SQLAuditStore{db: db, dialect: dialect}, nil
}

func (s *SQLAuditStore) RecordAudit(ctx context.Context, event AuditEvent) error {
	for name, value := range map[string]string{
		"audit id": event.ID, "audit actor": event.Actor, "audit role": event.Role,
		"audit tenant_id": event.TenantID, "audit action": event.Action,
		"audit target": event.Target, "audit remote_ip": event.RemoteIP,
	} {
		if err := validateSQLTextFilter(name, value); err != nil {
			return err
		}
	}
	if event.ID == "" {
		id, err := core.NewID("aud_")
		if err != nil {
			return err
		}
		event.ID = id
	}
	if event.Time.IsZero() {
		event.Time = time.Now().UTC()
	}
	detail := ""
	if len(event.Detail) > 0 {
		encoded, err := json.Marshal(event.Detail)
		if err != nil {
			return err
		}
		if len(encoded) > MaxAuditDetailBytes {
			return fmt.Errorf("audit detail exceeds %d bytes", MaxAuditDetailBytes)
		}
		detail = string(encoded)
	}
	var stored int
	if err := s.db.QueryRowContext(ctx, sqlCountAudit.bind(s.dialect)).Scan(&stored); err != nil {
		return err
	}
	if stored >= s.eventCap() {
		return fmt.Errorf("audit events exceed maximum of %d", s.eventCap())
	}
	_, err := s.db.ExecContext(ctx, sqlInsertAudit.bind(s.dialect),
		event.ID, event.Time.UnixMilli(), event.Actor, event.Role, event.TenantID,
		event.Action, event.Target, detail, event.RemoteIP,
	)
	return err
}

func (s *SQLAuditStore) ListAudit(ctx context.Context, filter AuditFilter) ([]AuditEvent, int, error) {
	for name, value := range map[string]string{
		"audit actor": filter.Actor, "audit action": filter.Action, "audit tenant_id": filter.TenantID,
	} {
		if err := validateSQLTextFilter(name, value); err != nil {
			return nil, 0, err
		}
	}
	where := []string{}
	args := []any{}
	if filter.Actor != "" {
		where = append(where, "actor = ?")
		args = append(args, filter.Actor)
	}
	if filter.Action != "" {
		where = append(where, "action = ?")
		args = append(args, filter.Action)
	}
	if filter.TenantID != "" {
		where = append(where, "tenant = ?")
		args = append(args, filter.TenantID)
	}
	clause := " WHERE 1=1"
	for _, condition := range where {
		clause += " AND " + condition
	}

	countQuery := sqlQuery{"SELECT COUNT(*) FROM audit_events" + clause}
	var total int
	if err := s.db.QueryRowContext(ctx, countQuery.bind(s.dialect), args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	limit := filter.Limit
	if limit <= 0 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}
	offset := filter.Offset
	if offset < 0 {
		offset = 0
	}
	pageQuery := sqlQuery{"SELECT id, time, actor, role, tenant, action, target, detail, remote_ip FROM audit_events" +
		clause + " ORDER BY time DESC LIMIT ? OFFSET ?"}
	pageArgs := append(append([]any(nil), args...), limit, offset)
	rows, err := s.db.QueryContext(ctx, pageQuery.bind(s.dialect), pageArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	out := []AuditEvent{}
	for rows.Next() {
		var event AuditEvent
		var detail string
		var timeMillis int64
		if err := rows.Scan(&event.ID, &timeMillis, &event.Actor, &event.Role, &event.TenantID, &event.Action, &event.Target, &detail, &event.RemoteIP); err != nil {
			return nil, 0, err
		}
		event.Time = time.UnixMilli(timeMillis).UTC()
		if detail != "" {
			_ = json.Unmarshal([]byte(detail), &event.Detail)
		}
		out = append(out, event)
	}
	return out, total, rows.Err()
}
