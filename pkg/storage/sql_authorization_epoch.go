package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
)

const authorizationEpochMetaKey = "authorization_epoch"

const maxAuthorizationEpoch int64 = 1<<63 - 1

var (
	sqlSelectAuthorizationEpoch = sqlQuery{"SELECT value FROM store_meta WHERE key = '" + authorizationEpochMetaKey + "'"}
	// The canonical-value predicate prevents SQLite's permissive integer casts
	// from silently repairing a malformed value. The upper bound makes epoch
	// exhaustion a failed control-plane mutation rather than a saturated value.
	sqlBumpAuthorizationEpoch = sqlQuery{"UPDATE store_meta SET value = CAST(CAST(value AS BIGINT) + 1 AS TEXT) WHERE key = '" + authorizationEpochMetaKey + "' AND value = CAST(CAST(value AS BIGINT) AS TEXT) AND CAST(value AS BIGINT) >= 0 AND CAST(value AS BIGINT) < " + strconv.FormatInt(maxAuthorizationEpoch, 10)}
	// This no-op UPDATE both compares the expected epoch and retains the SQL
	// row lock until the surrounding transaction ends. It is deliberately used
	// instead of a read-then-compare sequence: SQLite obtains its writer
	// reservation and PostgreSQL obtains a row lock in the same statement.
	sqlLockAuthorizationEpoch = sqlQuery{"UPDATE store_meta SET value = value WHERE key = '" + authorizationEpochMetaKey + "' AND value = ?"}
)

var _ AuthorizationEpochReader = (*SQLSessionStore)(nil)
var _ AuthorizationEpochReader = (*SQLRunControlStore)(nil)
var _ AuthorizationEpochReader = (*SQLBindingJournal)(nil)

// AuthorizationEpoch reads the shared SQL authorization epoch.
func (s *SQLSessionStore) AuthorizationEpoch(ctx context.Context) (int64, error) {
	if s == nil || s.db == nil {
		return 0, fmt.Errorf("authorization epoch requires an SQL session store")
	}
	return readAuthorizationEpoch(ctx, s.db, s.dialect)
}

// AuthorizationEpoch reads the shared SQL authorization epoch.
func (s *SQLRunControlStore) AuthorizationEpoch(ctx context.Context) (int64, error) {
	if s == nil || s.db == nil {
		return 0, fmt.Errorf("authorization epoch requires an SQL run-control store")
	}
	return readAuthorizationEpoch(ctx, s.db, s.dialect)
}

// AuthorizationEpoch reads the shared SQL authorization epoch.
func (s *SQLBindingJournal) AuthorizationEpoch(ctx context.Context) (int64, error) {
	if s == nil || s.db == nil {
		return 0, fmt.Errorf("authorization epoch requires an SQL binding journal")
	}
	return readAuthorizationEpoch(ctx, s.db, s.dialect)
}

func readAuthorizationEpoch(ctx context.Context, db *sql.DB, dialect SQLDialect) (int64, error) {
	return readAuthorizationEpochRow(ctx, db, dialect)
}

type authorizationEpochRowReader interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func readAuthorizationEpochRow(ctx context.Context, reader authorizationEpochRowReader, dialect SQLDialect) (int64, error) {
	var raw string
	if err := reader.QueryRowContext(ctx, sqlSelectAuthorizationEpoch.bind(dialect)).Scan(&raw); err != nil {
		if err == sql.ErrNoRows {
			return 0, fmt.Errorf("authorization epoch row is missing")
		}
		return 0, err
	}
	epoch, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || epoch < 0 || strconv.FormatInt(epoch, 10) != raw {
		return 0, fmt.Errorf("authorization epoch %q is invalid", raw)
	}
	return epoch, nil
}

func bumpAuthorizationEpoch(ctx context.Context, tx *sql.Tx, dialect SQLDialect) error {
	result, err := tx.ExecContext(ctx, sqlBumpAuthorizationEpoch.bind(dialect))
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return fmt.Errorf("authorization epoch is missing, invalid, or exhausted")
	}
	return nil
}

// lockAuthorizationEpoch compares expected against the durable epoch and
// holds the matching row lock until tx ends. It must be called from the same
// transaction that persists the state authorized by expected. Recovery-prefix
// transactions take this first, before their queued Session fence:
// authorization_epoch -> run_control -> run_queue -> session_leases ->
// sessions -> tool_invocations.
func lockAuthorizationEpoch(ctx context.Context, tx *sql.Tx, dialect SQLDialect, expected int64) error {
	if expected < 0 {
		return fmt.Errorf("authorization epoch must not be negative")
	}
	result, err := tx.ExecContext(ctx, sqlLockAuthorizationEpoch.bind(dialect), strconv.FormatInt(expected, 10))
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		actual, readErr := readAuthorizationEpochRow(ctx, tx, dialect)
		if readErr != nil {
			return readErr
		}
		if actual != expected {
			return ErrAuthorizationEpochChanged
		}
		return fmt.Errorf("authorization epoch lock affected no rows")
	}
	if expected == maxAuthorizationEpoch {
		return fmt.Errorf("authorization epoch is exhausted")
	}
	return nil
}
