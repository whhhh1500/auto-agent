// Package compositionstore provides the SQL adapter for runtime.CompositionStore.
// It persists only the validated runtime composition document; the document
// model contains no credential fields and unknown JSON fields are refused.
package compositionstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/whhhh1500/auto-agent/pkg/adapter/sql/sqlkit"
	"github.com/whhhh1500/auto-agent/pkg/runtime"
)

const (
	hostStateKey          = "module_host"
	sqliteBusyMaxAttempts = 5
)

// MaxStateJSONBytes bounds the persisted host-state document to 16 MiB. The
// limit applies before JSON decoding so corrupted rows cannot cause unbounded
// allocation during startup recovery.
const MaxStateJSONBytes = 16 << 20

var (
	selectState = `SELECT revision, state_json FROM runtime_host_state
		WHERE host_key = 'module_host'`
	insertState = `INSERT INTO runtime_host_state
		(host_key, revision, state_json, updated_at) VALUES ('module_host', ?, ?, ?)
		ON CONFLICT (host_key) DO NOTHING`
	updateState = `UPDATE runtime_host_state
		SET revision = ?, state_json = ?, updated_at = ?
		WHERE host_key = 'module_host' AND revision = ?`
	insertOwnership = `INSERT INTO runtime_host_ownership (host_key, holder, generation, expires_at)
		VALUES ('module_host', ?, 1, ?) ON CONFLICT (host_key) DO NOTHING`
	claimExpiredOwnership = `UPDATE runtime_host_ownership
		SET holder = ?, generation = generation + 1, expires_at = ?
		WHERE host_key = 'module_host' AND (holder = '' OR expires_at <= ?)`
	selectOwnership = `SELECT holder, generation, expires_at FROM runtime_host_ownership
		WHERE host_key = 'module_host'`
	renewOwnership = `UPDATE runtime_host_ownership SET expires_at = ?
		WHERE host_key = 'module_host' AND holder = ? AND generation = ? AND expires_at = ? AND expires_at > ?`
	releaseOwnership = `UPDATE runtime_host_ownership SET holder = '', expires_at = 0
		WHERE host_key = 'module_host' AND holder = ? AND generation = ? AND expires_at = ?`
)

// Store persists the one durable runtime host-state document through strict
// compare-and-swap. It does not initialize or migrate the surrounding schema.
type Store struct {
	db      *sql.DB
	dialect sqlkit.Dialect
}

var _ runtime.CompositionStore = (*Store)(nil)
var _ runtime.HostOwnershipStore = (*Store)(nil)

// New constructs a composition store over an already-initialized SQL schema.
func New(db *sql.DB, dialect sqlkit.Dialect) (*Store, error) {
	if db == nil {
		return nil, fmt.Errorf("composition store requires a database handle")
	}
	if !dialect.Valid() {
		return nil, fmt.Errorf("unsupported SQL dialect %q", dialect.String())
	}
	return &Store{db: db, dialect: dialect}, nil
}

// Load returns the validated persisted document. Corrupt, unknown-field, or
// revision-mismatched rows fail closed rather than being treated as absent.
func (store *Store) Load(ctx context.Context) (runtime.DurableHostState, bool, error) {
	ctx = nonNilContext(ctx)
	var revision, raw string
	if err := store.db.QueryRowContext(ctx, store.bind(selectState)).Scan(&revision, &raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return runtime.DurableHostState{}, false, nil
		}
		return runtime.DurableHostState{}, false, err
	}
	state, err := decodeState(raw)
	if err != nil {
		return runtime.DurableHostState{}, false, err
	}
	if revision != state.StoreRevision {
		return runtime.DurableHostState{}, false, fmt.Errorf("%w: stored revision column does not match document", runtime.ErrInvalidComposition)
	}
	return state.Clone(), true, nil
}

// CompareAndSwap creates the state when expectedRevision is empty, or replaces
// it only when the independent revision column still matches expectedRevision.
func (store *Store) CompareAndSwap(ctx context.Context, expectedRevision string, next runtime.DurableHostState) error {
	ctx = nonNilContext(ctx)
	if err := runtime.ValidateDurableHostState(next); err != nil {
		return err
	}
	if expectedRevision != "" && next.StoreRevision == expectedRevision {
		return fmt.Errorf("%w: next revision %q is not a new revision", runtime.ErrCompositionConflict, next.StoreRevision)
	}
	raw, err := json.Marshal(next)
	if err != nil {
		return fmt.Errorf("%w: marshal composition state: %v", runtime.ErrInvalidComposition, err)
	}
	if len(raw) > MaxStateJSONBytes {
		return fmt.Errorf("%w: composition state exceeds %d bytes", runtime.ErrInvalidComposition, MaxStateJSONBytes)
	}
	now := time.Now().UTC().UnixMilli()
	if expectedRevision == "" {
		result, err := store.execWrite(ctx, insertState, next.StoreRevision, string(raw), now)
		if err != nil {
			return err
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if changed != 1 {
			return fmt.Errorf("%w: composition state already exists", runtime.ErrCompositionConflict)
		}
		return nil
	}
	result, err := store.execWrite(ctx, updateState, next.StoreRevision, string(raw), now, expectedRevision)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return fmt.Errorf("%w: expected revision %q", runtime.ErrCompositionConflict, expectedRevision)
	}
	return nil
}

func decodeState(raw string) (runtime.DurableHostState, error) {
	if len(raw) == 0 || len(raw) > MaxStateJSONBytes {
		return runtime.DurableHostState{}, fmt.Errorf("%w: persisted state JSON length is invalid", runtime.ErrInvalidComposition)
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	var state runtime.DurableHostState
	if err := decoder.Decode(&state); err != nil {
		return runtime.DurableHostState{}, fmt.Errorf("%w: decode persisted state: %v", runtime.ErrInvalidComposition, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return runtime.DurableHostState{}, fmt.Errorf("%w: persisted state must contain one JSON value", runtime.ErrInvalidComposition)
	}
	if err := runtime.ValidateDurableHostState(state); err != nil {
		return runtime.DurableHostState{}, fmt.Errorf("%w: persisted state: %v", runtime.ErrInvalidComposition, err)
	}
	return state, nil
}

func (store *Store) execWrite(ctx context.Context, query string, args ...any) (sql.Result, error) {
	for attempt := 0; ; attempt++ {
		result, err := store.db.ExecContext(ctx, store.bind(query), args...)
		if err == nil || store.dialect != sqlkit.SQLite || !isSQLiteBusy(err) || attempt == sqliteBusyMaxAttempts-1 {
			return result, err
		}
		wait := time.Duration(1<<attempt) * 5 * time.Millisecond
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func (store *Store) bind(query string) string {
	bound, err := sqlkit.Bind(query, store.dialect)
	if err != nil {
		panic("composition store SQL binding invariant violated: " + err.Error())
	}
	return bound
}

func nonNilContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func isSQLiteBusy(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "database is locked") || strings.Contains(message, "database is busy")
}

func (store *Store) ClaimHostOwnership(ctx context.Context, holder string, ttl time.Duration) (runtime.HostOwnershipClaim, error) {
	ctx = nonNilContext(ctx)
	if !validOwnershipInput(holder, ttl) {
		return runtime.HostOwnershipClaim{}, runtime.ErrHostOwnershipLost
	}
	now, err := store.ownershipNow(ctx)
	if err != nil {
		return runtime.HostOwnershipClaim{}, err
	}
	expires := now.Add(ttl).UnixMilli()
	result, err := store.execWrite(ctx, insertOwnership, holder, expires)
	if err != nil {
		return runtime.HostOwnershipClaim{}, err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return runtime.HostOwnershipClaim{}, err
	}
	if changed == 0 {
		result, err = store.execWrite(ctx, claimExpiredOwnership, holder, expires, now.UnixMilli())
		if err != nil {
			return runtime.HostOwnershipClaim{}, err
		}
		changed, err = result.RowsAffected()
		if err != nil {
			return runtime.HostOwnershipClaim{}, err
		}
		if changed == 0 {
			return runtime.HostOwnershipClaim{}, runtime.ErrHostOwnershipConflict
		}
	}
	return store.loadOwnership(ctx, holder)
}

func (store *Store) RenewHostOwnership(ctx context.Context, claim runtime.HostOwnershipClaim, ttl time.Duration) (runtime.HostOwnershipClaim, error) {
	ctx = nonNilContext(ctx)
	if !validOwnershipClaim(claim) || ttl <= 0 || ttl > runtime.MaxHostOwnershipTTL {
		return runtime.HostOwnershipClaim{}, runtime.ErrHostOwnershipLost
	}
	now, err := store.ownershipNow(ctx)
	if err != nil {
		return runtime.HostOwnershipClaim{}, err
	}
	expires := now.Add(ttl).UnixMilli()
	result, err := store.execWrite(ctx, renewOwnership, expires, claim.Holder, claim.Generation, claim.ExpiresAt.UnixMilli(), now.UnixMilli())
	if err != nil {
		return runtime.HostOwnershipClaim{}, err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return runtime.HostOwnershipClaim{}, err
	}
	if changed != 1 {
		return runtime.HostOwnershipClaim{}, runtime.ErrHostOwnershipLost
	}
	return runtime.HostOwnershipClaim{Holder: claim.Holder, Generation: claim.Generation, ExpiresAt: time.UnixMilli(expires).UTC()}, nil
}

func (store *Store) ValidateHostOwnership(ctx context.Context, claim runtime.HostOwnershipClaim) error {
	ctx = nonNilContext(ctx)
	if !validOwnershipClaim(claim) {
		return runtime.ErrHostOwnershipLost
	}
	now, err := store.ownershipNow(ctx)
	if err != nil {
		return err
	}
	current, err := store.loadOwnership(ctx, claim.Holder)
	if err != nil || current.Holder != claim.Holder || current.Generation != claim.Generation || !current.ExpiresAt.Equal(claim.ExpiresAt) || !now.Before(current.ExpiresAt) {
		if err != nil {
			return err
		}
		return runtime.ErrHostOwnershipLost
	}
	return nil
}

func (store *Store) ReleaseHostOwnership(ctx context.Context, claim runtime.HostOwnershipClaim) error {
	ctx = nonNilContext(ctx)
	if !validOwnershipClaim(claim) {
		return runtime.ErrHostOwnershipLost
	}
	result, err := store.execWrite(ctx, releaseOwnership, claim.Holder, claim.Generation, claim.ExpiresAt.UnixMilli())
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed == 1 {
		return nil
	}
	var holder string
	var generation uint64
	if err := store.db.QueryRowContext(ctx, store.bind(selectOwnership)).Scan(&holder, &generation, new(int64)); err != nil {
		return runtime.ErrHostOwnershipLost
	}
	if holder == "" && generation == claim.Generation {
		return nil
	}
	return runtime.ErrHostOwnershipLost
}

func (store *Store) loadOwnership(ctx context.Context, expectedHolder string) (runtime.HostOwnershipClaim, error) {
	var holder string
	var generation uint64
	var expires int64
	if err := store.db.QueryRowContext(ctx, store.bind(selectOwnership)).Scan(&holder, &generation, &expires); err != nil {
		return runtime.HostOwnershipClaim{}, err
	}
	if holder != expectedHolder || generation == 0 || expires <= 0 {
		return runtime.HostOwnershipClaim{}, runtime.ErrHostOwnershipLost
	}
	return runtime.HostOwnershipClaim{Holder: holder, Generation: generation, ExpiresAt: time.UnixMilli(expires).UTC()}, nil
}

func (store *Store) ownershipNow(ctx context.Context) (time.Time, error) {
	query := "SELECT CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)"
	if store.dialect == sqlkit.Postgres {
		query = "SELECT CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)"
	}
	var millis int64
	if err := store.db.QueryRowContext(ctx, query).Scan(&millis); err != nil {
		return time.Time{}, err
	}
	return time.UnixMilli(millis).UTC(), nil
}

func validOwnershipInput(holder string, ttl time.Duration) bool {
	return len(holder) >= 1 && len(holder) <= 128 && ttl > 0 && ttl <= runtime.MaxHostOwnershipTTL
}

func validOwnershipClaim(claim runtime.HostOwnershipClaim) bool {
	return len(claim.Holder) >= 1 && len(claim.Holder) <= 128 && claim.Generation != 0 && !claim.ExpiresAt.IsZero() && claim.ExpiresAt.Location() == time.UTC
}
