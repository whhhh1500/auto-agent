// Package graphsegment provides the SQL-backed segment authority for Graph.
package graphsegment

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"sync"
	"time"

	rungraph "github.com/cc-auto-agent/harness-core/pkg/adapter/runexecutor/graph"
	sqlkit "github.com/cc-auto-agent/harness-core/pkg/adapter/sql/sqlkit"
	execgraph "github.com/cc-auto-agent/harness-core/pkg/execution/graph"
	graph "github.com/cc-auto-agent/harness-core/pkg/extensions/graph"
)

var (
	ErrLeaseBusy        = errors.New("graph segment lease busy")
	ErrLeaseInvalid     = errors.New("graph segment lease invalid")
	ErrLeaseUnavailable = errors.New("graph segment lease unavailable")
)

const DefaultTTL = 10 * time.Minute

type Clock func() time.Time

type Authority struct {
	db      *sql.DB
	dialect sqlkit.Dialect
	ttl     time.Duration
	now     Clock
}

func New(db *sql.DB, dialect sqlkit.Dialect) (*Authority, error) {
	return NewWithOptions(db, dialect, DefaultTTL, time.Now)
}
func NewWithOptions(db *sql.DB, dialect sqlkit.Dialect, ttl time.Duration, now Clock) (*Authority, error) {
	if db == nil || !dialect.Valid() || ttl <= 5*time.Minute || now == nil {
		return nil, ErrLeaseInvalid
	}
	return &Authority{db: db, dialect: dialect, ttl: ttl, now: now}, nil
}

var _ rungraph.SegmentAuthority = (*Authority)(nil)

func (a *Authority) Acquire(ctx context.Context, key graph.CheckpointKey) (grant rungraph.SegmentGrant, err error) {
	defer func() {
		if recover() != nil {
			grant, err = rungraph.SegmentGrant{}, ErrLeaseUnavailable
		}
	}()
	if err := ctxErr(ctx); err != nil {
		return rungraph.SegmentGrant{}, err
	}
	if err := graph.ValidateCheckpointKey(key); err != nil {
		return rungraph.SegmentGrant{}, ErrLeaseInvalid
	}
	if a == nil || a.db == nil {
		return rungraph.SegmentGrant{}, ErrLeaseUnavailable
	}
	segment, holder, err := randomID(), randomID(), error(nil)
	if segment == "" || holder == "" {
		return rungraph.SegmentGrant{}, ErrLeaseUnavailable
	}
	nowTime, ok := safeNow(a.now)
	if !ok {
		return rungraph.SegmentGrant{}, ErrLeaseUnavailable
	}
	now := nowTime.UnixNano()
	expires := nowTime.Add(a.ttl).UnixNano()
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return rungraph.SegmentGrant{}, stableDBError(err)
	}
	defer tx.Rollback()
	q := a.bind(`UPDATE graph_segment_leases SET segment_id=?, holder_id=?, host_generation=host_generation+1, expires_at=?, released=0, updated_at=? WHERE tenant_id=? AND session_id=? AND run_id=? AND (released=1 OR expires_at<=?)`)
	res, err := tx.ExecContext(ctx, q, segment, holder, expires, now, key.TenantID, key.SessionID, key.RunID, now)
	if err != nil {
		return rungraph.SegmentGrant{}, stableDBError(err)
	}
	rows, rowsErr := res.RowsAffected()
	if rowsErr != nil {
		return rungraph.SegmentGrant{}, ErrLeaseUnavailable
	}
	var generation int64
	if rows == 0 {
		q = a.bind(`INSERT INTO graph_segment_leases (tenant_id,session_id,run_id,segment_id,holder_id,host_generation,expires_at,released,created_at,updated_at) VALUES (?,?,?,?,?,1,?,0,?,?) ON CONFLICT (tenant_id,session_id,run_id) DO NOTHING`)
		inserted, insertErr := tx.ExecContext(ctx, q, key.TenantID, key.SessionID, key.RunID, segment, holder, expires, now, now)
		if insertErr != nil {
			return rungraph.SegmentGrant{}, ErrLeaseUnavailable
		}
		insertRows, insertRowsErr := inserted.RowsAffected()
		if insertRowsErr != nil {
			return rungraph.SegmentGrant{}, ErrLeaseUnavailable
		}
		if insertRows == 0 {
			return rungraph.SegmentGrant{}, ErrLeaseBusy
		}
		generation = 1
	} else if err = tx.QueryRowContext(ctx, a.bind(`SELECT host_generation FROM graph_segment_leases WHERE tenant_id=? AND session_id=? AND run_id=?`), key.TenantID, key.SessionID, key.RunID).Scan(&generation); err != nil {
		return rungraph.SegmentGrant{}, ErrLeaseUnavailable
	}
	if err = tx.Commit(); err != nil {
		return rungraph.SegmentGrant{}, ErrLeaseUnavailable
	}
	return rungraph.SegmentGrant{SegmentID: segment, HostGeneration: uint64(generation), Lease: &lease{a: a, key: key, segment: segment, holder: holder, generation: uint64(generation)}}, nil
}

type lease struct {
	a               *Authority
	key             graph.CheckpointKey
	segment, holder string
	generation      uint64
	mu              sync.Mutex
	released        bool
}

var _ execgraph.SegmentLease = (*lease)(nil)

func (l *lease) Verify(ctx context.Context, req execgraph.LeaseRequest) error {
	if l == nil || l.a == nil {
		return ErrLeaseInvalid
	}
	return l.check(ctx, req)
}
func (l *lease) Release(ctx context.Context, req execgraph.LeaseRequest) error {
	if l == nil || l.a == nil {
		return ErrLeaseInvalid
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released {
		return nil
	}
	if err := l.check(ctx, req); err != nil {
		return err
	}
	now, ok := safeNow(l.a.now)
	if !ok {
		return ErrLeaseUnavailable
	}
	res, err := l.a.db.ExecContext(ctx, l.a.bind(`UPDATE graph_segment_leases SET released=1, updated_at=? WHERE tenant_id=? AND session_id=? AND run_id=? AND segment_id=? AND holder_id=? AND host_generation=? AND released=0 AND expires_at>?`), now.UnixNano(), l.key.TenantID, l.key.SessionID, l.key.RunID, l.segment, l.holder, l.generation, now.UnixNano())
	if err != nil {
		return stableDBError(err)
	}
	n, rowsErr := res.RowsAffected()
	if rowsErr != nil {
		return ErrLeaseUnavailable
	}
	if n == 0 {
		return ErrLeaseUnavailable
	}
	l.released = true
	return nil
}
func (l *lease) check(ctx context.Context, req execgraph.LeaseRequest) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	if req.Key != l.key || req.SegmentID != l.segment || req.HostGeneration != l.generation {
		return ErrLeaseInvalid
	}
	var holder string
	var expires int64
	var released int
	err := l.a.db.QueryRowContext(ctx, l.a.bind(`SELECT holder_id,expires_at,released FROM graph_segment_leases WHERE tenant_id=? AND session_id=? AND run_id=?`), l.key.TenantID, l.key.SessionID, l.key.RunID).Scan(&holder, &expires, &released)
	if err != nil {
		return stableDBError(err)
	}
	now, ok := safeNow(l.a.now)
	if !ok {
		return ErrLeaseUnavailable
	}
	if holder != l.holder || released != 0 || expires <= now.UnixNano() {
		return ErrLeaseUnavailable
	}
	return nil
}
func stableDBError(err error) error {
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return ErrLeaseUnavailable
}
func safeNow(now Clock) (value time.Time, ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	return now(), true
}
func (a *Authority) bind(q string) string { out, _ := sqlkit.Bind(q, a.dialect); return out }
func ctxErr(ctx context.Context) error {
	if ctx == nil {
		return context.Canceled
	}
	return ctx.Err()
}
func randomID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return ""
	}
	return hex.EncodeToString(b)
}
