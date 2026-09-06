package storage

import (
	"context"
	"strings"
	"time"
)

const sessionLikeEscape = "!"

// SessionFilter narrows and orders a session catalog query.
type SessionFilter struct {
	TenantID  string
	UserID    string
	ProfileID string
	Status    string
	IDPrefix  string
	// Sort orders by updated_at: "desc" (default) or "asc".
	Sort string
	// Limit defaults to 50, capped at 500. Offset paginates.
	Limit  int
	Offset int
}

// SessionQuerier is implemented by stores that can serve filtered, sorted,
// paginated session catalogs. It returns the matching page plus the total
// count for pagination UI.
type SessionQuerier interface {
	QuerySessions(ctx context.Context, filter SessionFilter) ([]SessionSummary, int, error)
}

func (s *SQLSessionStore) QuerySessions(ctx context.Context, filter SessionFilter) ([]SessionSummary, int, error) {
	if err := validateSQLTextFilter("session filter tenant_id", filter.TenantID); err != nil {
		return nil, 0, err
	}
	if err := validateSQLTextFilter("session filter user_id", filter.UserID); err != nil {
		return nil, 0, err
	}
	if err := validateSQLTextFilter("session filter profile_id", filter.ProfileID); err != nil {
		return nil, 0, err
	}
	if err := validateSQLTextFilter("session filter status", filter.Status); err != nil {
		return nil, 0, err
	}

	where := []string{"tenant_id = ?", "user_id = ?"}
	args := []any{filter.TenantID, filter.UserID}
	if filter.ProfileID != "" {
		where = append(where, "profile_id = ?")
		args = append(args, filter.ProfileID)
	}
	if filter.Status != "" {
		where = append(where, "status = ?")
		args = append(args, filter.Status)
	}
	prefix := strings.TrimSpace(filter.IDPrefix)
	if prefix != "" {
		if err := validateSQLTextFilter("session filter prefix", prefix); err != nil {
			return nil, 0, err
		}
		where = append(where, "id LIKE ? ESCAPE '"+sessionLikeEscape+"'")
		args = append(args, sanitizeLikePrefix(prefix)+"%")
	}
	whereClause := strings.Join(where, " AND ")

	order := "DESC"
	if strings.EqualFold(filter.Sort, "asc") {
		order = "ASC"
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

	countQuery := sqlQuery{"SELECT COUNT(*) FROM sessions WHERE " + whereClause}
	var total int
	if err := s.db.QueryRowContext(ctx, countQuery.bind(s.dialect), args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	pageQuery := sqlQuery{"SELECT id, tenant_id, user_id, profile_id, event_count, updated_at FROM sessions WHERE " +
		whereClause + " ORDER BY updated_at " + order + " LIMIT ? OFFSET ?"}
	pageArgs := append(append([]any(nil), args...), limit, offset)
	rows, err := s.db.QueryContext(ctx, pageQuery.bind(s.dialect), pageArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	out := []SessionSummary{}
	for rows.Next() {
		var summary SessionSummary
		var updatedMillis int64
		if err := rows.Scan(&summary.ID, &summary.TenantID, &summary.UserID, &summary.ProfileID, &summary.EventCount, &updatedMillis); err != nil {
			return nil, 0, err
		}
		summary.UpdatedAt = time.UnixMilli(updatedMillis).UTC()
		out = append(out, summary)
	}
	return out, total, rows.Err()
}

// sanitizeLikePrefix escapes LIKE wildcards and the escape character itself
// so a user-provided prefix cannot broaden the catalog match.
func sanitizeLikePrefix(value string) string {
	replacer := strings.NewReplacer(
		sessionLikeEscape, sessionLikeEscape+sessionLikeEscape,
		"%", sessionLikeEscape+"%",
		"_", sessionLikeEscape+"_",
	)
	return replacer.Replace(value)
}
