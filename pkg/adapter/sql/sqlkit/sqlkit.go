// Package sqlkit provides small SQL-dialect helpers that do not depend on a
// particular database driver or storage implementation.
package sqlkit

import (
	"fmt"
	"strconv"
	"strings"
)

// Dialect identifies the placeholder convention used by a SQL engine.
type Dialect int

const (
	// SQLite uses question-mark placeholders.
	SQLite Dialect = iota
	// Postgres uses ordinal dollar placeholders.
	Postgres
)

// Valid reports whether dialect is supported by this adapter.
func (dialect Dialect) Valid() bool {
	return dialect == SQLite || dialect == Postgres
}

// String returns the stable dialect name, or an explicit stable representation
// for an unsupported value.
func (dialect Dialect) String() string {
	switch dialect {
	case SQLite:
		return "sqlite"
	case Postgres:
		return "postgres"
	default:
		return "unknown(" + strconv.Itoa(int(dialect)) + ")"
	}
}

// Bind converts question-mark placeholders in query to the requested dialect.
// It preserves question marks inside SQL single-quoted string literals. Target
// queries must use ordinary SQL string quoting; this helper does not parse
// comments, dollar-quoted PostgreSQL strings, or SQL syntax beyond literals.
func Bind(query string, dialect Dialect) (string, error) {
	if !dialect.Valid() {
		return "", fmt.Errorf("unsupported SQL dialect %q", dialect.String())
	}
	if dialect == SQLite {
		return query, nil
	}

	var bound strings.Builder
	bound.Grow(len(query))
	quoted := false
	parameter := 0
	for index := 0; index < len(query); index++ {
		character := query[index]
		if character == '\'' {
			bound.WriteByte(character)
			if quoted && index+1 < len(query) && query[index+1] == '\'' {
				bound.WriteByte(query[index+1])
				index++
				continue
			}
			quoted = !quoted
			continue
		}
		if character == '?' && !quoted {
			parameter++
			bound.WriteByte('$')
			bound.WriteString(strconv.Itoa(parameter))
			continue
		}
		bound.WriteByte(character)
	}
	return bound.String(), nil
}
