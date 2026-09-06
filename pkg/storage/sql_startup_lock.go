package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// postgresStartupLockKey serializes the brief database bootstrap window across
// application processes. It is deliberately a fixed, process-independent key:
// callers hold it only while inspecting the pre-migration state, applying the
// schema, and creating the one-time initial administrator.
const postgresStartupLockKey int64 = 0x4843524553544150 // "HCRESTAP"

// PostgresStartupLock owns the dedicated connection on which the PostgreSQL
// advisory lock is held. Advisory locks are connection-scoped, so keeping the
// connection private prevents a pooled connection from releasing the lock
// before startup work is finished.
type PostgresStartupLock struct {
	conn *sql.Conn
}

// AcquirePostgresStartupLock serializes PostgreSQL schema/bootstrap startup.
// The returned lock must be released once the initial-admin decision has been
// durably committed. It intentionally does not affect SQLite callers.
func AcquirePostgresStartupLock(ctx context.Context, db *sql.DB) (*PostgresStartupLock, error) {
	if db == nil {
		return nil, fmt.Errorf("postgres startup lock requires a database handle")
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("reserve postgres startup lock connection: %w", err)
	}
	if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", postgresStartupLockKey); err != nil {
		closeErr := conn.Close()
		if closeErr != nil {
			return nil, errors.Join(
				fmt.Errorf("acquire postgres startup lock: %w", err),
				fmt.Errorf("close postgres startup lock connection after acquire failure: %w", closeErr),
			)
		}
		return nil, fmt.Errorf("acquire postgres startup lock: %w", err)
	}
	return &PostgresStartupLock{conn: conn}, nil
}

// Release unlocks and closes the dedicated PostgreSQL connection. It always
// attempts Close even if unlock fails, so a failed unlock cannot strand the
// session lock. A caller's main startup error should be retained separately.
func (l *PostgresStartupLock) Release(ctx context.Context) error {
	if l == nil || l.conn == nil {
		return nil
	}
	conn := l.conn
	l.conn = nil
	var unlocked bool
	unlockErr := conn.QueryRowContext(ctx, "SELECT pg_advisory_unlock($1)", postgresStartupLockKey).Scan(&unlocked)
	if unlockErr == nil && !unlocked {
		unlockErr = fmt.Errorf("postgres startup advisory lock was not held")
	}
	closeErr := conn.Close()
	switch {
	case unlockErr != nil && closeErr != nil:
		return errors.Join(
			fmt.Errorf("release postgres startup lock: %w", unlockErr),
			fmt.Errorf("close postgres startup lock connection: %w", closeErr),
		)
	case unlockErr != nil:
		return fmt.Errorf("release postgres startup lock: %w", unlockErr)
	case closeErr != nil:
		return fmt.Errorf("close postgres startup lock connection: %w", closeErr)
	default:
		return nil
	}
}
