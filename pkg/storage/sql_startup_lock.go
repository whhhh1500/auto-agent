package storage

import (
	"context"
	"database/sql"
	"database/sql/driver"
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
	db   *sql.DB
	conn *sql.Conn
}

func (l *PostgresStartupLock) connection() (*sql.Conn, error) {
	if l == nil || l.db == nil || l.conn == nil {
		return nil, fmt.Errorf("postgres startup lock is not held")
	}
	return l.conn, nil
}

func (l *PostgresStartupLock) discardConnection() error {
	if l == nil || l.conn == nil {
		return nil
	}
	conn := l.conn
	l.conn = nil
	return discardSQLConn(conn)
}

type postgresSessionAdvisoryLock struct {
	conn  *sql.Conn
	key   int64
	label string
}

func acquirePostgresSessionAdvisoryLock(ctx context.Context, db *sql.DB, key int64, label string) (*postgresSessionAdvisoryLock, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("reserve %s connection: %w", label, err)
	}
	if err := acquirePostgresAdvisoryLock(ctx, conn, key); err != nil {
		discardErr := discardSQLConn(conn)
		return nil, errors.Join(
			fmt.Errorf("acquire %s: %w", label, err),
			wrapSQLConnDiscardError(label, discardErr),
		)
	}
	return &postgresSessionAdvisoryLock{conn: conn, key: key, label: label}, nil
}

func acquirePostgresAdvisoryLock(ctx context.Context, conn *sql.Conn, key int64) error {
	_, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", key)
	return err
}

func releasePostgresAdvisoryLock(ctx context.Context, conn *sql.Conn, key int64) error {
	var unlocked bool
	if err := conn.QueryRowContext(ctx, "SELECT pg_advisory_unlock($1)", key).Scan(&unlocked); err != nil {
		return err
	}
	if !unlocked {
		return fmt.Errorf("advisory lock was not held")
	}
	return nil
}

func (l *postgresSessionAdvisoryLock) release(ctx context.Context) error {
	if l == nil || l.conn == nil {
		return nil
	}
	conn := l.conn
	l.conn = nil
	unlockErr := releasePostgresAdvisoryLock(ctx, conn, l.key)
	if unlockErr != nil {
		discardErr := discardSQLConn(conn)
		return errors.Join(
			fmt.Errorf("release %s: %w", l.label, unlockErr),
			wrapSQLConnDiscardError(l.label, discardErr),
		)
	}
	if err := conn.Close(); err != nil {
		return fmt.Errorf("close %s connection: %w", l.label, err)
	}
	return nil
}

// discardSQLConn marks a database/sql connection bad before closing it. This
// is required when a canceled or failed advisory-lock query leaves the server
// session's lock state unknown: ordinary Conn.Close would return that session
// to the pool, where a hidden session lock could survive indefinitely.
func discardSQLConn(conn *sql.Conn) error {
	if conn == nil {
		return nil
	}
	rawErr := conn.Raw(func(any) error { return driver.ErrBadConn })
	if errors.Is(rawErr, driver.ErrBadConn) || errors.Is(rawErr, sql.ErrConnDone) {
		rawErr = nil
	}
	closeErr := conn.Close()
	if errors.Is(closeErr, sql.ErrConnDone) {
		closeErr = nil
	}
	return errors.Join(rawErr, closeErr)
}

func wrapSQLConnDiscardError(label string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("discard %s connection: %w", label, err)
}

// AcquirePostgresStartupLock serializes PostgreSQL schema/bootstrap startup.
// The returned lock must be released once the initial-admin decision has been
// durably committed. PostgreSQL deployments must connect directly or through
// a session-pooling proxy; transaction pooling cannot preserve these
// session-level advisory locks. This function intentionally does not affect
// SQLite callers.
func AcquirePostgresStartupLock(ctx context.Context, db *sql.DB) (*PostgresStartupLock, error) {
	if db == nil {
		return nil, fmt.Errorf("postgres startup lock requires a database handle")
	}
	lock, err := acquirePostgresSessionAdvisoryLock(ctx, db, postgresStartupLockKey, "postgres startup lock")
	if err != nil {
		return nil, err
	}
	return &PostgresStartupLock{db: db, conn: lock.conn}, nil
}

// AccountsTableExists inspects the pre-migration accounts state on the same
// PostgreSQL session that owns the startup lock. Callers must use this method,
// rather than the package function, when a one-connection pool is possible.
func (l *PostgresStartupLock) AccountsTableExists(ctx context.Context) (bool, error) {
	conn, err := l.connection()
	if err != nil {
		return false, err
	}
	return sqlTableExists(ctx, conn, SQLDialectPostgres, "accounts")
}

// BootstrapInitialAdmin completes the one-time administrator decision on the
// same PostgreSQL session that owns the startup lock. It avoids reserving a
// second pool connection while preserving the lock across the whole decision.
func (l *PostgresStartupLock) BootstrapInitialAdmin(ctx context.Context, accountsTableExisted bool) (InitialAdminCredentials, error) {
	conn, err := l.connection()
	if err != nil {
		return InitialAdminCredentials{}, err
	}
	return bootstrapInitialAdmin(ctx, conn, SQLDialectPostgres, accountsTableExisted)
}

// Release unlocks and returns the dedicated PostgreSQL connection. If unlock
// fails or its outcome is unknown, the connection is marked bad and discarded
// instead, so a hidden session lock cannot return to the pool. A caller's main
// startup error should be retained separately.
func (l *PostgresStartupLock) Release(ctx context.Context) error {
	if l == nil || l.conn == nil {
		return nil
	}
	conn := l.conn
	l.conn = nil
	unlockErr := releasePostgresAdvisoryLock(ctx, conn, postgresStartupLockKey)
	if unlockErr != nil {
		discardErr := discardSQLConn(conn)
		return errors.Join(
			fmt.Errorf("release postgres startup lock: %w", unlockErr),
			wrapSQLConnDiscardError("postgres startup lock", discardErr),
		)
	}
	if err := conn.Close(); err != nil {
		return fmt.Errorf("close postgres startup lock connection: %w", err)
	}
	return nil
}
