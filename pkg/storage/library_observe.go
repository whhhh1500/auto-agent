package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/execution"
)

var (
	sqlInsertLibraryObservation = sqlQuery{`INSERT INTO tool_library_observations
		(id, time, action, query, tool_id, library, name, hit_ids, hits, explore, call_id, scope, tenant_id)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`}
	sqlCountLibraryObservations = sqlQuery{`SELECT COUNT(*) FROM tool_library_observations`}
	sqlPruneLibraryObservations = sqlQuery{`DELETE FROM tool_library_observations WHERE id IN (
		SELECT id FROM tool_library_observations ORDER BY time ASC, id ASC LIMIT ?)`}
	sqlListLibraryObservations = sqlQuery{`SELECT id, time, action, query, tool_id, library, name, hit_ids, hits, explore, call_id, scope, tenant_id
		FROM tool_library_observations ORDER BY time DESC, id DESC LIMIT ?`}
	sqlListLibraryObservationsTenant = sqlQuery{`SELECT id, time, action, query, tool_id, library, name, hit_ids, hits, explore, call_id, scope, tenant_id
		FROM tool_library_observations WHERE tenant_id = ? ORDER BY time DESC, id DESC LIMIT ?`}
)

// SQLLibraryObserver persists tool-library disclosure traces for later ranking
// work. Inserts are best-effort from the caller; at cap the oldest rows drop.
type SQLLibraryObserver struct {
	db      *sql.DB
	dialect SQLDialect
	max     int
}

func NewSQLLibraryObserver(db *sql.DB, dialect SQLDialect) (*SQLLibraryObserver, error) {
	if db == nil {
		return nil, fmt.Errorf("sql library observer requires a database handle")
	}
	if err := validateSQLDialect(dialect); err != nil {
		return nil, err
	}
	return &SQLLibraryObserver{db: db, dialect: dialect}, nil
}

func (s *SQLSessionStore) LibraryObserver() *SQLLibraryObserver {
	if s == nil {
		return nil
	}
	return &SQLLibraryObserver{db: s.db, dialect: s.dialect}
}

func (o *SQLLibraryObserver) cap() int {
	if o != nil && o.max > 0 {
		return o.max
	}
	return execution.MaxLibraryObservations
}

func (o *SQLLibraryObserver) Observe(ctx context.Context, observation execution.ToolLibraryObservation) error {
	if o == nil || o.db == nil {
		return nil
	}
	if observation.Time.IsZero() {
		observation.Time = time.Now().UTC()
	}
	if observation.Query != "" && len([]rune(observation.Query)) > 240 {
		observation.Query = string([]rune(observation.Query)[:240])
	}
	if len(observation.HitIDs) > 32 {
		observation.HitIDs = append([]string(nil), observation.HitIDs[:32]...)
	}
	id, err := core.NewID("obs_")
	if err != nil {
		return err
	}
	encodedHits, err := json.Marshal(observation.HitIDs)
	if err != nil {
		return err
	}
	explore := 0
	if observation.Explore {
		explore = 1
	}
	var stored int
	if err := o.db.QueryRowContext(ctx, sqlCountLibraryObservations.bind(o.dialect)).Scan(&stored); err != nil {
		return err
	}
	if stored >= o.cap() {
		extra := stored - o.cap() + 1
		if extra < 1 {
			extra = 1
		}
		if _, err := o.db.ExecContext(ctx, sqlPruneLibraryObservations.bind(o.dialect), extra); err != nil {
			return err
		}
	}
	_, err = o.db.ExecContext(ctx, sqlInsertLibraryObservation.bind(o.dialect),
		id, observation.Time.UTC().UnixMilli(), observation.Action, observation.Query,
		observation.ToolID, observation.Library, observation.Name, string(encodedHits),
		observation.Hits, explore, observation.CallID, observation.Scope, observation.TenantID,
	)
	return err
}

func (o *SQLLibraryObserver) List(ctx context.Context, limit int) ([]execution.ToolLibraryObservation, error) {
	return o.ListForTenant(ctx, "", limit)
}

func (o *SQLLibraryObserver) ListForTenant(ctx context.Context, tenantID string, limit int) ([]execution.ToolLibraryObservation, error) {
	if o == nil || o.db == nil {
		return nil, fmt.Errorf("sql library observer is not configured")
	}
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	var rows *sql.Rows
	var err error
	if tenantID != "" {
		rows, err = o.db.QueryContext(ctx, sqlListLibraryObservationsTenant.bind(o.dialect), tenantID, limit)
	} else {
		rows, err = o.db.QueryContext(ctx, sqlListLibraryObservations.bind(o.dialect), limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []execution.ToolLibraryObservation{}
	for rows.Next() {
		var rec execution.ToolLibraryObservation
		var millis int64
		var hitJSON string
		var explore int
		var id string
		if err := rows.Scan(&id, &millis, &rec.Action, &rec.Query, &rec.ToolID, &rec.Library, &rec.Name, &hitJSON, &rec.Hits, &explore, &rec.CallID, &rec.Scope, &rec.TenantID); err != nil {
			return nil, err
		}
		rec.Time = time.UnixMilli(millis).UTC()
		rec.Explore = explore != 0
		if hitJSON != "" {
			_ = json.Unmarshal([]byte(hitJSON), &rec.HitIDs)
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

var _ execution.ToolLibraryObserver = (*SQLLibraryObserver)(nil)
