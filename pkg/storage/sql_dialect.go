package storage

import (
	"fmt"
	"github.com/cc-auto-agent/harness-core/pkg/adapter/sql/sqlkit"
)

// SQLDialect is retained as an alias for sqlkit.Dialect so existing storage
// integrations continue to compile while SQL dialect mechanics remain in the
// driver-neutral sqlkit adapter.
type SQLDialect = sqlkit.Dialect

const (
	// SQLDialectSQLite is the source-compatible SQLite dialect alias.
	SQLDialectSQLite = sqlkit.SQLite
	// SQLDialectPostgres is the source-compatible PostgreSQL dialect alias.
	SQLDialectPostgres = sqlkit.Postgres
)

func validateSQLDialect(dialect SQLDialect) error {
	if !dialect.Valid() {
		return fmt.Errorf("unsupported SQL dialect %q", dialect.String())
	}
	return nil
}

// sqlQuery is one static query with ? placeholders, rebound per dialect.
type sqlQuery struct{ text string }

// bind relies on exported SQL entry points validating dialect before any store
// is constructed. A panic here therefore represents an internal programming
// error, never an unsupported external configuration value.
func (q sqlQuery) bind(dialect SQLDialect) string {
	bound, err := sqlkit.Bind(q.text, dialect)
	if err != nil {
		panic("storage sql query binding invariant violated: " + err.Error())
	}
	return bound
}
