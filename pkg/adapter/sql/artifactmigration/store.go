// Package artifactmigration persists the bounded resource migration contract.
// It owns no object-store client and never stores object bodies or credentials.
package artifactmigration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/whhhh1500/auto-agent/pkg/adapter/sql/sqlkit"
	app "github.com/whhhh1500/auto-agent/pkg/app/artifactmigration"
)

const sqliteBusyMaxAttempts = 5

type Store struct {
	db      *sql.DB
	dialect sqlkit.Dialect
}

var _ app.Repository = (*Store)(nil)

func New(db *sql.DB, dialect sqlkit.Dialect) (*Store, error) {
	if db == nil {
		return nil, fmt.Errorf("artifact migration store requires a database handle")
	}
	if !dialect.Valid() {
		return nil, fmt.Errorf("unsupported SQL dialect %q", dialect.String())
	}
	return &Store{db: db, dialect: dialect}, nil
}

func (store *Store) Load(ctx context.Context) (app.Migration, bool, error) {
	ctx = nonNilContext(ctx)
	migration, err := store.load(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return app.Migration{}, false, nil
	}
	if err != nil {
		return app.Migration{}, false, err
	}
	return migration, true, nil
}

// CompareAndSwap creates the first slot record at revision zero, or updates a
// controller-owned record only when the observed store revision still matches.
// Replacing a desired revision requires a strictly newer generation, which
// fences all prior lease handles without depending on process-local state.
func (store *Store) CompareAndSwap(ctx context.Context, expected uint64, next app.Migration) (app.Migration, error) {
	ctx = nonNilContext(ctx)
	if next.State.Terminal() {
		next.LeaseOwner, next.LeaseExpiresAt = "", time.Time{}
	}
	if err := app.ValidateMigration(next); err != nil {
		return app.Migration{}, err
	}
	if expected == 0 {
		if next.StoreRevision != 0 {
			return app.Migration{}, app.ErrMigrationConflict
		}
		now, err := store.now(ctx)
		if err != nil {
			return app.Migration{}, err
		}
		next.StoreRevision = 1
		next.CreatedAt = now
		next.UpdatedAt = now
		if err := store.insert(ctx, next); err != nil {
			return app.Migration{}, err
		}
		return next, nil
	}
	current, found, err := store.Load(ctx)
	if err != nil {
		return app.Migration{}, err
	}
	if !found || current.StoreRevision != expected || next.StoreRevision != expected {
		return app.Migration{}, app.ErrMigrationConflict
	}
	if err := validateControllerUpdate(current, next); err != nil {
		return app.Migration{}, err
	}
	now, err := store.now(ctx)
	if err != nil {
		return app.Migration{}, err
	}
	next.StoreRevision = expected + 1
	next.CreatedAt = current.CreatedAt
	next.UpdatedAt = now
	// A terminal transition is the retention boundary for this generation. A
	// replacement also retires a terminal prior generation. Keep the row write
	// and metadata-only journal deletion in one transaction so foreground
	// appenders cannot leave an unbounded orphan journal after a terminal CAS.
	if current.State.Terminal() || next.State.Terminal() || current.ID != next.ID {
		tx, err := store.db.BeginTx(ctx, nil)
		if err != nil {
			return app.Migration{}, err
		}
		defer tx.Rollback()
		result, err := tx.ExecContext(ctx, store.bind(migrationUpdate+` WHERE resource_key = 'resources' AND store_revision = ?`), append(migrationArgsWithoutKey(next), expected)...)
		if err != nil {
			return app.Migration{}, err
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return app.Migration{}, err
		}
		if changed != 1 {
			return app.Migration{}, app.ErrMigrationConflict
		}
		if _, err := tx.ExecContext(ctx, store.bind(`DELETE FROM artifact_migration_mutations WHERE migration_id = ?`), current.ID); err != nil {
			return app.Migration{}, err
		}
		if err := tx.Commit(); err != nil {
			return app.Migration{}, err
		}
		return next, nil
	}
	if err := store.updateCAS(ctx, expected, next); err != nil {
		return app.Migration{}, err
	}
	return next, nil
}

// UpdateLeaseHeld persists worker progress only when its exact lease handle is
// still current and unexpired. It is the required write path for a copier.
func (store *Store) UpdateLeaseHeld(ctx context.Context, lease app.Lease, expected uint64, next app.Migration) (app.Migration, error) {
	ctx = nonNilContext(ctx)
	if err := app.ValidateLease(lease); err != nil {
		return app.Migration{}, app.ErrLeaseLost
	}
	if next.State.Terminal() {
		next.LeaseOwner, next.LeaseExpiresAt = "", time.Time{}
	}
	if err := app.ValidateMigration(next); err != nil {
		return app.Migration{}, err
	}
	if expected == 0 || next.ID != lease.MigrationID || next.Generation != lease.Generation || next.StoreRevision != expected {
		return app.Migration{}, app.ErrLeaseLost
	}
	if !next.State.Terminal() && (next.LeaseOwner != lease.Owner || !next.LeaseExpiresAt.Equal(lease.ExpiresAt)) {
		return app.Migration{}, app.ErrLeaseLost
	}
	now, err := store.now(ctx)
	if err != nil {
		return app.Migration{}, err
	}
	next.StoreRevision++
	next.UpdatedAt = now
	if next.State.Terminal() {
		tx, err := store.db.BeginTx(ctx, nil)
		if err != nil {
			return app.Migration{}, err
		}
		defer tx.Rollback()
		args := append(workerMigrationArgs(next), lease.MigrationID, lease.Generation, expected, lease.Owner, lease.ExpiresAt.UnixMilli(), now.UnixMilli())
		result, err := tx.ExecContext(ctx, store.bind(workerMigrationUpdate+` WHERE resource_key = 'resources' AND migration_id = ? AND generation = ? AND store_revision = ?
			AND lease_owner = ? AND lease_expires_at = ? AND lease_expires_at > ?`), args...)
		if err != nil {
			return app.Migration{}, err
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return app.Migration{}, err
		}
		if changed != 1 {
			return app.Migration{}, app.ErrLeaseLost
		}
		if _, err := tx.ExecContext(ctx, store.bind(`DELETE FROM artifact_migration_mutations WHERE migration_id = ?`), next.ID); err != nil {
			return app.Migration{}, err
		}
		if err := tx.Commit(); err != nil {
			return app.Migration{}, err
		}
		return store.load(ctx)
	}
	if err := store.updateLeaseHeld(ctx, lease, expected, now, next); err != nil {
		return app.Migration{}, err
	}
	return store.load(ctx)
}

func (store *Store) ClaimLease(ctx context.Context, owner string, ttl time.Duration) (app.Lease, error) {
	ctx = nonNilContext(ctx)
	if len(owner) == 0 || len(owner) > app.MaxLeaseOwnerBytes || !validTTL(ttl) {
		return app.Lease{}, app.ErrLeaseConflict
	}
	now, err := store.now(ctx)
	if err != nil {
		return app.Lease{}, err
	}
	expires := now.Add(ttl)
	result, err := store.exec(ctx, `UPDATE artifact_migrations
		SET lease_owner = ?, lease_expires_at = ?, updated_at = ?
		WHERE resource_key = 'resources' AND state IN ('syncing', 'catching_up', 'verifying')
		AND (lease_owner = '' OR lease_expires_at <= ?)`, owner, expires.UnixMilli(), now.UnixMilli(), now.UnixMilli())
	if err != nil {
		return app.Lease{}, err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return app.Lease{}, err
	}
	if changed != 1 {
		return app.Lease{}, app.ErrLeaseConflict
	}
	migration, err := store.load(ctx)
	if err != nil {
		return app.Lease{}, err
	}
	if migration.LeaseOwner != owner || !migration.LeaseExpiresAt.Equal(expires) {
		return app.Lease{}, app.ErrLeaseLost
	}
	return leaseFor(migration), nil
}

func (store *Store) RenewLease(ctx context.Context, lease app.Lease, ttl time.Duration) (app.Lease, error) {
	ctx = nonNilContext(ctx)
	if err := app.ValidateLease(lease); err != nil || !validTTL(ttl) {
		return app.Lease{}, app.ErrLeaseLost
	}
	now, err := store.now(ctx)
	if err != nil {
		return app.Lease{}, err
	}
	expires := now.Add(ttl)
	result, err := store.exec(ctx, `UPDATE artifact_migrations
		SET lease_expires_at = ?, updated_at = ?
		WHERE resource_key = 'resources' AND migration_id = ? AND generation = ?
		AND lease_owner = ? AND lease_expires_at = ? AND lease_expires_at > ?
		AND state IN ('syncing', 'catching_up', 'verifying')`, expires.UnixMilli(), now.UnixMilli(), lease.MigrationID, lease.Generation, lease.Owner, lease.ExpiresAt.UnixMilli(), now.UnixMilli())
	if err != nil {
		return app.Lease{}, err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return app.Lease{}, err
	}
	if changed != 1 {
		return app.Lease{}, app.ErrLeaseLost
	}
	return app.Lease{MigrationID: lease.MigrationID, Generation: lease.Generation, Owner: lease.Owner, ExpiresAt: expires}, nil
}

func (store *Store) ReleaseLease(ctx context.Context, lease app.Lease) error {
	ctx = nonNilContext(ctx)
	if err := app.ValidateLease(lease); err != nil {
		return app.ErrLeaseLost
	}
	result, err := store.exec(ctx, `UPDATE artifact_migrations SET lease_owner = '', lease_expires_at = 0,
	updated_at = ?
		WHERE resource_key = 'resources' AND migration_id = ? AND generation = ?
		AND lease_owner = ? AND lease_expires_at = ?`, time.Now().UTC().UnixMilli(), lease.MigrationID, lease.Generation, lease.Owner, lease.ExpiresAt.UnixMilli())
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return app.ErrLeaseLost
	}
	return nil
}

func (store *Store) ValidateLease(ctx context.Context, lease app.Lease) error {
	ctx = nonNilContext(ctx)
	if err := app.ValidateLease(lease); err != nil {
		return app.ErrLeaseLost
	}
	now, err := store.now(ctx)
	if err != nil {
		return err
	}
	var count int
	if err := store.db.QueryRowContext(ctx, store.bind(`SELECT COUNT(*) FROM artifact_migrations
		WHERE resource_key = 'resources' AND migration_id = ? AND generation = ?
		AND lease_owner = ? AND lease_expires_at = ? AND lease_expires_at > ?`), lease.MigrationID, lease.Generation, lease.Owner, lease.ExpiresAt.UnixMilli(), now.UnixMilli()).Scan(&count); err != nil {
		return err
	}
	if count != 1 {
		return app.ErrLeaseLost
	}
	return nil
}

func (store *Store) AppendMutation(ctx context.Context, command app.MutationAppend) (app.Mutation, error) {
	ctx = nonNilContext(ctx)
	if err := app.ValidateMutationAppend(command); err != nil {
		return app.Mutation{}, err
	}
	now, err := store.now(ctx)
	if err != nil {
		return app.Mutation{}, err
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return app.Mutation{}, err
	}
	defer tx.Rollback()
	var sequence uint64
	if err := tx.QueryRowContext(ctx, store.bind(`UPDATE artifact_migrations
		SET mutation_high_water = mutation_high_water + 1, updated_at = ?
		WHERE resource_key = 'resources' AND migration_id = ? AND generation = ? AND desired_revision = ?
		AND state IN ('syncing', 'catching_up', 'verifying') RETURNING mutation_high_water`), now.UnixMilli(), command.Token.MigrationID, command.Token.Generation, command.Token.DesiredRevision).Scan(&sequence); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return app.Mutation{}, app.ErrMutationConflict
		}
		return app.Mutation{}, err
	}
	mutation := app.Mutation{MigrationID: command.Token.MigrationID, Sequence: sequence, Operation: command.Operation, ObjectKey: command.ObjectKey, ETag: command.ETag, Digest: command.Digest, State: app.MutationPending, CreatedAt: now, UpdatedAt: now}
	if _, err := tx.ExecContext(ctx, store.bind(`INSERT INTO artifact_migration_mutations
		(migration_id, sequence, operation, object_key, etag, digest, state, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?)`), mutation.MigrationID, mutation.Sequence, mutation.Operation, mutation.ObjectKey, mutation.ETag, mutation.Digest, now.UnixMilli(), now.UnixMilli()); err != nil {
		return app.Mutation{}, err
	}
	if err := tx.Commit(); err != nil {
		return app.Mutation{}, err
	}
	return mutation, nil
}

// FailMutation terminally fences an active migration when its authoritative
// local mutation cannot be journaled. The detail is a fixed safe code, never
// a driver error, credential, endpoint, or object body.
func (store *Store) FailMutation(ctx context.Context, token app.MutationToken, code string) error {
	ctx = nonNilContext(ctx)
	if err := app.ValidateMutationToken(token); err != nil || len(code) == 0 || len(code) > app.MaxErrorCodeBytes {
		return app.ErrInvalidMigration
	}
	now, err := store.now(ctx)
	if err != nil {
		return err
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, store.bind(`UPDATE artifact_migrations
		SET state = 'apply_failed', error_code = ?, error_detail = 'mutation journal append failed',
			lease_owner = '', lease_expires_at = 0, store_revision = store_revision + 1, updated_at = ?
		WHERE resource_key = 'resources' AND migration_id = ? AND generation = ? AND desired_revision = ?
			AND state IN ('syncing', 'catching_up', 'verifying')`), code, now.UnixMilli(), token.MigrationID, token.Generation, token.DesiredRevision)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return app.ErrMutationConflict
	}
	if _, err := tx.ExecContext(ctx, store.bind(`DELETE FROM artifact_migration_mutations WHERE migration_id = ?`), token.MigrationID); err != nil {
		return err
	}
	return tx.Commit()
}

func (store *Store) MarkMutation(ctx context.Context, lease app.Lease, sequence uint64, state app.MutationState) error {
	ctx = nonNilContext(ctx)
	if err := app.ValidateLease(lease); err != nil || sequence == 0 || !state.Valid() {
		return app.ErrLeaseLost
	}
	now, err := store.now(ctx)
	if err != nil {
		return err
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, store.bind(`UPDATE artifact_migrations SET updated_at = ?
		WHERE resource_key = 'resources' AND migration_id = ? AND generation = ?
		AND lease_owner = ? AND lease_expires_at = ? AND lease_expires_at > ?
		AND state IN ('syncing', 'catching_up', 'verifying')`), now.UnixMilli(), lease.MigrationID, lease.Generation, lease.Owner, lease.ExpiresAt.UnixMilli(), now.UnixMilli())
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return app.ErrLeaseLost
	}
	result, err = tx.ExecContext(ctx, store.bind(`UPDATE artifact_migration_mutations SET state = ?, updated_at = ?
		WHERE migration_id = ? AND sequence = ?`), state, now.UnixMilli(), lease.MigrationID, sequence)
	if err != nil {
		return err
	}
	changed, err = result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return app.ErrMutationConflict
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

// MarkMutationsThrough acknowledges a bounded replay page in one transaction.
// The migration lease is checked before journal state changes, so a stale
// worker cannot acknowledge another generation's mutations.
func (store *Store) MarkMutationsThrough(ctx context.Context, lease app.Lease, through uint64) error {
	ctx = nonNilContext(ctx)
	if err := app.ValidateLease(lease); err != nil || through == 0 {
		return app.ErrLeaseLost
	}
	now, err := store.now(ctx)
	if err != nil {
		return err
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, store.bind(`UPDATE artifact_migrations SET updated_at = ?
		WHERE resource_key = 'resources' AND migration_id = ? AND generation = ?
		AND lease_owner = ? AND lease_expires_at = ? AND lease_expires_at > ?
		AND state IN ('syncing', 'catching_up', 'verifying')`), now.UnixMilli(), lease.MigrationID, lease.Generation, lease.Owner, lease.ExpiresAt.UnixMilli(), now.UnixMilli())
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return app.ErrLeaseLost
	}
	if _, err := tx.ExecContext(ctx, store.bind(`UPDATE artifact_migration_mutations SET state = 'applied', updated_at = ?
		WHERE migration_id = ? AND sequence <= ? AND state = 'pending'`), now.UnixMilli(), lease.MigrationID, through); err != nil {
		return err
	}
	return tx.Commit()
}

func (store *Store) ListMutations(ctx context.Context, migrationID string, after uint64, limit int) ([]app.Mutation, error) {
	ctx = nonNilContext(ctx)
	if len(migrationID) == 0 || len(migrationID) > app.MaxMigrationIDBytes || limit < 1 || limit > app.MaxMutationListLimit {
		return nil, app.ErrInvalidMigration
	}
	rows, err := store.db.QueryContext(ctx, store.bind(`SELECT sequence, operation, object_key, etag, digest, state, created_at, updated_at
		FROM artifact_migration_mutations WHERE migration_id = ? AND sequence > ? ORDER BY sequence ASC LIMIT ?`), migrationID, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	mutations := make([]app.Mutation, 0, limit)
	for rows.Next() {
		mutation, err := scanMutation(rows, migrationID)
		if err != nil {
			return nil, err
		}
		mutations = append(mutations, mutation)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return mutations, nil
}

func (store *Store) insert(ctx context.Context, migration app.Migration) error {
	result, err := store.exec(ctx, migrationInsert, migrationArgs(migration)...)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return app.ErrMigrationConflict
	}
	return nil
}

func (store *Store) updateCAS(ctx context.Context, expected uint64, migration app.Migration) error {
	args := append(migrationArgsWithoutKey(migration), expected)
	result, err := store.exec(ctx, migrationUpdate+` WHERE resource_key = 'resources' AND store_revision = ?`, args...)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return app.ErrMigrationConflict
	}
	return nil
}

func (store *Store) updateLeaseHeld(ctx context.Context, lease app.Lease, expected uint64, now time.Time, migration app.Migration) error {
	args := append(workerMigrationArgs(migration), lease.MigrationID, lease.Generation, expected, lease.Owner, lease.ExpiresAt.UnixMilli(), now.UnixMilli())
	result, err := store.exec(ctx, workerMigrationUpdate+` WHERE resource_key = 'resources' AND migration_id = ? AND generation = ? AND store_revision = ?
		AND lease_owner = ? AND lease_expires_at = ? AND lease_expires_at > ?`, args...)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return app.ErrLeaseLost
	}
	return nil
}

const migrationInsert = `INSERT INTO artifact_migrations
	(resource_key, migration_id, desired_revision, source_backend, source_identity, target_backend, target_identity, state, generation, store_revision,
	listing_cursor, mutation_high_water, copied_objects, copied_bytes, verified_objects, verified_bytes, error_code, error_detail, lease_owner, lease_expires_at,
	created_at, updated_at, verified_at, activated_at)
	VALUES ('resources', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

const migrationUpdate = `UPDATE artifact_migrations SET migration_id = ?, desired_revision = ?, source_backend = ?, source_identity = ?, target_backend = ?, target_identity = ?,
	state = ?, generation = ?, store_revision = ?, listing_cursor = ?, mutation_high_water = ?, copied_objects = ?, copied_bytes = ?, verified_objects = ?, verified_bytes = ?,
	error_code = ?, error_detail = ?, lease_owner = ?, lease_expires_at = ?, created_at = ?, updated_at = ?, verified_at = ?, activated_at = ?`

// Worker progress never writes mutation_high_water. Foreground object writes
// allocate journal sequences independently, so a slow copy progress update
// cannot erase a newer mutation sequence or contend on the control revision.
const workerMigrationUpdate = `UPDATE artifact_migrations SET migration_id = ?, desired_revision = ?, source_backend = ?, source_identity = ?, target_backend = ?, target_identity = ?,
	state = ?, generation = ?, store_revision = ?, listing_cursor = ?, copied_objects = ?, copied_bytes = ?, verified_objects = ?, verified_bytes = ?,
	error_code = ?, error_detail = ?, lease_owner = ?, lease_expires_at = ?, created_at = ?, updated_at = ?, verified_at = ?, activated_at = ?`

func migrationArgs(migration app.Migration) []any {
	return []any{migration.ID, migration.DesiredRevision, migration.Source.Backend, migration.Source.Identity, migration.Target.Backend, migration.Target.Identity,
		migration.State, migration.Generation, migration.StoreRevision, migration.ListingCursor, migration.MutationHighWater, migration.CopiedObjects, migration.CopiedBytes,
		migration.VerifiedObjects, migration.VerifiedBytes, migration.ErrorCode, migration.ErrorDetail, migration.LeaseOwner, millis(migration.LeaseExpiresAt),
		millis(migration.CreatedAt), millis(migration.UpdatedAt), millis(migration.VerifiedAt), millis(migration.ActivatedAt)}
}

func migrationArgsWithoutKey(migration app.Migration) []any { return migrationArgs(migration) }

func workerMigrationArgs(migration app.Migration) []any {
	return []any{migration.ID, migration.DesiredRevision, migration.Source.Backend, migration.Source.Identity, migration.Target.Backend, migration.Target.Identity,
		migration.State, migration.Generation, migration.StoreRevision, migration.ListingCursor, migration.CopiedObjects, migration.CopiedBytes,
		migration.VerifiedObjects, migration.VerifiedBytes, migration.ErrorCode, migration.ErrorDetail, migration.LeaseOwner, millis(migration.LeaseExpiresAt),
		millis(migration.CreatedAt), millis(migration.UpdatedAt), millis(migration.VerifiedAt), millis(migration.ActivatedAt)}
}

func (store *Store) load(ctx context.Context) (app.Migration, error) {
	row := store.db.QueryRowContext(ctx, store.bind(`SELECT migration_id, desired_revision, source_backend, source_identity, target_backend, target_identity, state,
		generation, store_revision, listing_cursor, mutation_high_water, copied_objects, copied_bytes, verified_objects, verified_bytes, error_code, error_detail,
		lease_owner, lease_expires_at, created_at, updated_at, verified_at, activated_at FROM artifact_migrations WHERE resource_key = 'resources'`))
	return scanMigration(row)
}

type scanner interface{ Scan(...any) error }

func scanMigration(row scanner) (app.Migration, error) {
	var migration app.Migration
	var source, target, state string
	var expires, created, updated, verified, activated int64
	if err := row.Scan(&migration.ID, &migration.DesiredRevision, &source, &migration.Source.Identity, &target, &migration.Target.Identity, &state,
		&migration.Generation, &migration.StoreRevision, &migration.ListingCursor, &migration.MutationHighWater, &migration.CopiedObjects, &migration.CopiedBytes,
		&migration.VerifiedObjects, &migration.VerifiedBytes, &migration.ErrorCode, &migration.ErrorDetail, &migration.LeaseOwner, &expires, &created, &updated, &verified, &activated); err != nil {
		return app.Migration{}, err
	}
	migration.Source.Backend, migration.Target.Backend, migration.State = app.Backend(source), app.Backend(target), app.State(state)
	migration.LeaseExpiresAt, migration.CreatedAt, migration.UpdatedAt = fromMillis(expires), fromMillis(created), fromMillis(updated)
	migration.VerifiedAt, migration.ActivatedAt = fromMillis(verified), fromMillis(activated)
	if err := app.ValidateMigration(migration); err != nil {
		return app.Migration{}, fmt.Errorf("%w: persisted migration: %v", app.ErrInvalidMigration, err)
	}
	return migration, nil
}

func scanMutation(row scanner, migrationID string) (app.Mutation, error) {
	var mutation app.Mutation
	var operation, state string
	var created, updated int64
	mutation.MigrationID = migrationID
	if err := row.Scan(&mutation.Sequence, &operation, &mutation.ObjectKey, &mutation.ETag, &mutation.Digest, &state, &created, &updated); err != nil {
		return app.Mutation{}, err
	}
	mutation.Operation, mutation.State = app.MutationOperation(operation), app.MutationState(state)
	mutation.CreatedAt, mutation.UpdatedAt = fromMillis(created), fromMillis(updated)
	if mutation.Sequence == 0 || !mutation.Operation.Valid() || !mutation.State.Valid() || mutation.ObjectKey == "" || mutation.CreatedAt.IsZero() || mutation.UpdatedAt.IsZero() {
		return app.Mutation{}, fmt.Errorf("%w: persisted mutation", app.ErrInvalidMigration)
	}
	return mutation, nil
}

func validateControllerUpdate(current, next app.Migration) error {
	changedIdentity := current.ID != next.ID || current.DesiredRevision != next.DesiredRevision || current.Source != next.Source || current.Target != next.Target
	if changedIdentity {
		if !current.State.Terminal() || next.Generation <= current.Generation || next.State != app.StateSyncing || next.LeaseOwner != "" || !next.LeaseExpiresAt.IsZero() {
			return app.ErrMigrationConflict
		}
		return nil
	}
	if next.Generation != current.Generation {
		return app.ErrMigrationConflict
	}
	return nil
}

func leaseFor(migration app.Migration) app.Lease {
	return app.Lease{MigrationID: migration.ID, Generation: migration.Generation, Owner: migration.LeaseOwner, ExpiresAt: migration.LeaseExpiresAt}
}

func (store *Store) now(ctx context.Context) (time.Time, error) {
	query := "SELECT CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)"
	if store.dialect == sqlkit.Postgres {
		query = "SELECT CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)"
	}
	var value int64
	if err := store.db.QueryRowContext(ctx, query).Scan(&value); err != nil {
		return time.Time{}, err
	}
	return time.UnixMilli(value).UTC(), nil
}

func (store *Store) exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	for attempt := 0; ; attempt++ {
		result, err := store.db.ExecContext(ctx, store.bind(query), args...)
		if err == nil || store.dialect != sqlkit.SQLite || !isSQLiteBusy(err) || attempt == sqliteBusyMaxAttempts-1 {
			return result, err
		}
		timer := time.NewTimer(time.Duration(1<<attempt) * 5 * time.Millisecond)
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
		panic("artifact migration SQL binding invariant violated: " + err.Error())
	}
	return bound
}

func nonNilContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func validTTL(ttl time.Duration) bool { return ttl > 0 && ttl <= app.MaxLeaseTTL }
func millis(value time.Time) int64 {
	if value.IsZero() {
		return 0
	}
	return value.UnixMilli()
}
func fromMillis(value int64) time.Time {
	if value == 0 {
		return time.Time{}
	}
	return time.UnixMilli(value).UTC()
}
func isSQLiteBusy(err error) bool {
	message := strings.ToLower(fmt.Sprint(err))
	return strings.Contains(message, "database is locked") || strings.Contains(message, "database is busy")
}
