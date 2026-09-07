package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// ErrNativeStrictDynamicControl indicates that durable dynamic binding,
// release, or canary state exists in an authority that must remain static for
// native strict Phase 1. Infrastructure, transaction, and context failures do
// not wrap this sentinel.
var ErrNativeStrictDynamicControl = errors.New("native strict durable dynamic control is present")

// VerifyNativeStrictStaticControl verifies that durable state contains no
// dynamic binding, release, or canary artifact. Native strict bootstrap owns
// its projection in process and deliberately does not replay those control
// planes. The helper is read-only and assumes OpenSQLSessionStore has already
// completed schema migration. Its consistent read snapshot is a deployment
// preflight and lag detector, not a transaction-level execution grant. It
// wraps ErrNativeStrictDynamicControl only when an artifact is actually found.
func VerifyNativeStrictStaticControl(ctx context.Context, db *sql.DB, dialect SQLDialect) error {
	return verifyNativeStrictStaticControl(ctx, db, dialect, nil)
}

func verifyNativeStrictStaticControl(ctx context.Context, db *sql.DB, dialect SQLDialect, afterQuery func(int) error) error {
	if db == nil {
		return fmt.Errorf("native strict control preflight requires a database handle")
	}
	if err := validateSQLDialect(dialect); err != nil {
		return err
	}
	options := &sql.TxOptions{ReadOnly: true}
	if dialect == SQLDialectPostgres {
		options.Isolation = sql.LevelRepeatableRead
	}
	tx, err := db.BeginTx(ctx, options)
	if err != nil {
		return fmt.Errorf("begin native strict control preflight: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	for index, control := range []struct {
		name  string
		query sqlQuery
	}{
		{name: "binding", query: sqlQuery{"SELECT 1 FROM admin_bindings LIMIT 1"}},
		{name: "release", query: sqlQuery{"SELECT 1 FROM profile_releases LIMIT 1"}},
		{name: "canary", query: sqlQuery{"SELECT 1 FROM profile_canaries LIMIT 1"}},
	} {
		var exists int
		err := tx.QueryRowContext(ctx, control.query.bind(dialect)).Scan(&exists)
		switch {
		case err == nil:
			return fmt.Errorf("%w: durable %s artifact exists", ErrNativeStrictDynamicControl, control.name)
		case !errors.Is(err, sql.ErrNoRows):
			return fmt.Errorf("inspect native strict %s control: %w", control.name, err)
		}
		if afterQuery != nil {
			if err := afterQuery(index); err != nil {
				return err
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("complete native strict control preflight: %w", err)
	}
	return nil
}
