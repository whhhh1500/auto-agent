package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/whhhh1500/auto-agent/pkg/extensions/subagent"
)

// SQLDelegationLinkStore is the durable metadata-only delegation catalog.
// It deliberately has no columns for prompts, arguments, results, or events.
type SQLDelegationLinkStore struct {
	db      *sql.DB
	dialect SQLDialect
}

func NewSQLDelegationLinkStore(db *sql.DB, dialect SQLDialect) (*SQLDelegationLinkStore, error) {
	if db == nil {
		return nil, fmt.Errorf("delegation link store requires a database handle")
	}
	if err := validateSQLDialect(dialect); err != nil {
		return nil, err
	}
	return &SQLDelegationLinkStore{db: db, dialect: dialect}, nil
}

const delegationLinkColumns = `parent_session_id, parent_run_id, parent_call_id,
child_session_id, child_run_id, tenant_id, subject_id, depth, created_at, updated_at`

func scanDelegationLink(row interface{ Scan(...any) error }) (subagent.Link, error) {
	var link subagent.Link
	var created, updated int64
	if err := row.Scan(&link.ParentSessionID, &link.ParentRunID, &link.ParentCallID,
		&link.ChildSessionID, &link.ChildRunID, &link.TenantID, &link.SubjectID,
		&link.Depth, &created, &updated); err != nil {
		return subagent.Link{}, err
	}
	link.CreatedAt = time.UnixMilli(created).UTC()
	link.UpdatedAt = time.UnixMilli(updated).UTC()
	if err := subagent.ValidateLink(link); err != nil {
		return subagent.Link{}, fmt.Errorf("stored delegation link is invalid: %w", err)
	}
	return link, nil
}

func (s *SQLDelegationLinkStore) Get(ctx context.Context, key string) (subagent.Link, bool, error) {
	if s == nil || s.db == nil {
		return subagent.Link{}, false, fmt.Errorf("delegation link store is nil")
	}
	parts := strings.Split(key, "\x00")
	if len(parts) != 3 {
		return subagent.Link{}, false, fmt.Errorf("delegation link key is invalid")
	}
	for _, part := range parts {
		if err := validateDelegationFilterID(part); err != nil {
			return subagent.Link{}, false, err
		}
	}
	query := `SELECT ` + delegationLinkColumns + ` FROM delegation_links
		WHERE parent_session_id = ? AND parent_run_id = ? AND parent_call_id = ?`
	link, err := scanDelegationLink(s.db.QueryRowContext(ctx, (sqlQuery{query}).bind(s.dialect), parts[0], parts[1], parts[2]))
	if errors.Is(err, sql.ErrNoRows) {
		return subagent.Link{}, false, nil
	}
	if err != nil {
		return subagent.Link{}, false, err
	}
	return link, true, nil
}

func (s *SQLDelegationLinkStore) GetByChild(ctx context.Context, child string) (subagent.Link, bool, error) {
	if s == nil || s.db == nil {
		return subagent.Link{}, false, fmt.Errorf("delegation link store is nil")
	}
	if err := validateDelegationFilterID(child); err != nil {
		return subagent.Link{}, false, err
	}
	query := `SELECT ` + delegationLinkColumns + ` FROM delegation_links WHERE child_session_id = ?`
	link, err := scanDelegationLink(s.db.QueryRowContext(ctx, (sqlQuery{query}).bind(s.dialect), child))
	if errors.Is(err, sql.ErrNoRows) {
		return subagent.Link{}, false, nil
	}
	if err != nil {
		return subagent.Link{}, false, err
	}
	return link, true, nil
}

func (s *SQLDelegationLinkStore) PutIfAbsent(ctx context.Context, link subagent.Link) (subagent.Link, bool, error) {
	if s == nil || s.db == nil {
		return subagent.Link{}, false, fmt.Errorf("delegation link store is nil")
	}
	if err := subagent.ValidateLink(link); err != nil {
		return subagent.Link{}, false, err
	}
	if link.CreatedAt.IsZero() {
		link.CreatedAt = time.Now().UTC()
	}
	if link.UpdatedAt.IsZero() {
		link.UpdatedAt = link.CreatedAt
	}
	query := `INSERT INTO delegation_links (` + delegationLinkColumns + `)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	var err error
	for attempt := 0; attempt < 50; attempt++ {
		_, err = s.db.ExecContext(ctx, (sqlQuery{query}).bind(s.dialect),
			link.ParentSessionID, link.ParentRunID, link.ParentCallID,
			link.ChildSessionID, link.ChildRunID, link.TenantID, link.SubjectID,
			link.Depth, link.CreatedAt.UnixMilli(), link.UpdatedAt.UnixMilli())
		if !isSQLiteBusy(err) || s.dialect != SQLDialectSQLite {
			break
		}
		select {
		case <-ctx.Done():
			return subagent.Link{}, false, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	if err == nil {
		return link, true, nil
	}
	if !isDuplicateConstraint(err) {
		return subagent.Link{}, false, err
	}
	existing, found, getErr := s.Get(ctx, link.Key())
	if getErr != nil {
		return subagent.Link{}, false, getErr
	}
	if found {
		return existing, false, nil
	}
	// A different parent attempting to reuse a child is a hard conflict, not
	// an idempotent replay. Returning a generic error avoids leaking the other
	// tenant's link metadata to an unauthorized caller.
	return subagent.Link{}, false, fmt.Errorf("delegation link conflicts with an existing child session")
}

func isSQLiteBusy(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "database is locked") || strings.Contains(message, "sqlite_busy")
}

func validateDelegationFilterID(value string) error {
	return subagent.ValidateLinkFilter(subagent.DelegationLinkFilter{ParentSessionID: value})
}

func (s *SQLDelegationLinkStore) List(ctx context.Context, filter subagent.DelegationLinkFilter) ([]subagent.Link, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("delegation link store is nil")
	}
	if err := subagent.ValidateLinkFilter(filter); err != nil {
		return nil, err
	}
	query := `SELECT ` + delegationLinkColumns + ` FROM delegation_links`
	clauses := make([]string, 0, 4)
	args := make([]any, 0, 4)
	if filter.ParentSessionID != "" {
		clauses = append(clauses, "parent_session_id = ?")
		args = append(args, filter.ParentSessionID)
	}
	if filter.ParentRunID != "" {
		clauses = append(clauses, "parent_run_id = ?")
		args = append(args, filter.ParentRunID)
	}
	if filter.ChildSessionID != "" {
		clauses = append(clauses, "child_session_id = ?")
		args = append(args, filter.ChildSessionID)
	}
	if filter.TenantID != "" {
		clauses = append(clauses, "tenant_id = ?")
		args = append(args, filter.TenantID)
	}
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " ORDER BY created_at DESC, parent_session_id DESC, parent_run_id DESC, parent_call_id DESC"
	limit := filter.Limit
	if limit == 0 {
		limit = 100
	}
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}
	if filter.Offset > 0 {
		query += " OFFSET ?"
		args = append(args, filter.Offset)
	}
	rows, err := s.db.QueryContext(ctx, (sqlQuery{query}).bind(s.dialect), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	links := make([]subagent.Link, 0)
	for rows.Next() {
		link, scanErr := scanDelegationLink(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		links = append(links, link)
	}
	return links, rows.Err()
}
