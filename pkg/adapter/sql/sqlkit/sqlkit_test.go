package sqlkit

import "testing"

func TestBind(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		dialect Dialect
		want    string
	}{
		{
			name:    "sqlite keeps question-mark placeholders",
			query:   "SELECT * FROM things WHERE left_value = ? AND right_value = ?",
			dialect: SQLite,
			want:    "SELECT * FROM things WHERE left_value = ? AND right_value = ?",
		},
		{
			name:    "postgres numbers placeholders",
			query:   "SELECT * FROM things WHERE left_value = ? AND right_value = ?",
			dialect: Postgres,
			want:    "SELECT * FROM things WHERE left_value = $1 AND right_value = $2",
		},
		{
			name:    "single quoted question marks stay literal",
			query:   "SELECT '?' AS literal, value FROM things WHERE value = ? AND escaped = 'it''s ?'",
			dialect: Postgres,
			want:    "SELECT '?' AS literal, value FROM things WHERE value = $1 AND escaped = 'it''s ?'",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := Bind(test.query, test.dialect)
			if err != nil {
				t.Fatalf("Bind(%q, %v): %v", test.query, test.dialect, err)
			}
			if got != test.want {
				t.Fatalf("Bind(%q, %v) = %q; want %q", test.query, test.dialect, got, test.want)
			}
		})
	}
}

func TestBindHasNoCrossCallState(t *testing.T) {
	if got, err := Bind("WHERE first = ? AND second = ?", Postgres); err != nil {
		t.Fatalf("first Bind(): %v", err)
	} else if want := "WHERE first = $1 AND second = $2"; got != want {
		t.Fatalf("first Bind() = %q; want %q", got, want)
	}
	if got, err := Bind("WHERE only = ?", Postgres); err != nil {
		t.Fatalf("second Bind(): %v", err)
	} else if want := "WHERE only = $1"; got != want {
		t.Fatalf("second Bind() reused state: %q; want %q", got, want)
	}
}

func TestDialectValidAndString(t *testing.T) {
	tests := []struct {
		dialect Dialect
		valid   bool
		want    string
	}{
		{dialect: SQLite, valid: true, want: "sqlite"},
		{dialect: Postgres, valid: true, want: "postgres"},
		{dialect: Dialect(99), valid: false, want: "unknown(99)"},
	}
	for _, test := range tests {
		if got := test.dialect.Valid(); got != test.valid {
			t.Errorf("Dialect(%d).Valid() = %t; want %t", test.dialect, got, test.valid)
		}
		if got := test.dialect.String(); got != test.want {
			t.Errorf("Dialect(%d).String() = %q; want %q", test.dialect, got, test.want)
		}
	}
}

func TestBindRejectsUnknownDialect(t *testing.T) {
	if _, err := Bind("SELECT ?", Dialect(99)); err == nil || err.Error() != `unsupported SQL dialect "unknown(99)"` {
		t.Fatalf("Bind() unknown dialect error = %v; want unsupported dialect error", err)
	}
}
