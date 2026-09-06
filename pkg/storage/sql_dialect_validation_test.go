package storage

import (
	"context"
	"testing"
)

func TestExportedSQLEntryPointsRejectUnknownDialect(t *testing.T) {
	db := newTestSQLStore(t).db
	ctx := context.Background()
	invalid := SQLDialect(99)
	const wantError = `unsupported SQL dialect "unknown(99)"`

	tests := []struct {
		name string
		call func() error
	}{
		{"OpenSQLSessionStore", func() error { _, err := OpenSQLSessionStore(ctx, db, invalid); return err }},
		{"AccountsTableExists", func() error { _, err := AccountsTableExists(ctx, db, invalid); return err }},
		{"EnsureMasterKey", func() error { _, err := EnsureMasterKey(ctx, db, invalid); return err }},
		{"NewSQLAccountStore", func() error { _, err := NewSQLAccountStore(db, invalid); return err }},
		{"NewSQLApprovalStore", func() error { _, err := NewSQLApprovalStore(db, invalid); return err }},
		{"NewSQLAuditStore", func() error { _, err := NewSQLAuditStore(db, invalid); return err }},
		{"NewSQLBindingJournal", func() error { _, err := NewSQLBindingJournal(db, invalid); return err }},
		{"NewSQLCanaryStore", func() error { _, err := NewSQLCanaryStore(db, invalid); return err }},
		{"NewSQLDelegationLinkStore", func() error { _, err := NewSQLDelegationLinkStore(db, invalid); return err }},
		{"NewSQLEvaluationStore", func() error { _, err := NewSQLEvaluationStore(db, invalid); return err }},
		{"NewSQLLibraryObserver", func() error { _, err := NewSQLLibraryObserver(db, invalid); return err }},
		{"NewSQLMemoryStore", func() error { _, err := NewSQLMemoryStore(db, invalid); return err }},
		{"NewSQLObsStore", func() error { _, err := NewSQLObsStore(db, invalid); return err }},
		{"NewSQLRagIndex", func() error { _, err := NewSQLRagIndex(db, invalid); return err }},
		{"NewSQLRunControlStore", func() error { _, err := NewSQLRunControlStore(db, invalid); return err }},
		{"NewSQLRunnerStore", func() error { _, err := NewSQLRunnerStore(db, invalid); return err }},
		{"NewSQLRunStatsStore", func() error { _, err := NewSQLRunStatsStore(db, invalid); return err }},
		{"NewSQLToolInvocationJournal", func() error { _, err := NewSQLToolInvocationJournal(db, invalid); return err }},
	}

	if got, want := len(tests), 18; got != want {
		t.Fatalf("unknown dialect entry-point test count = %d; want %d", got, want)
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			defer func() {
				if recovered := recover(); recovered != nil {
					t.Errorf("unknown dialect panicked: %v", recovered)
				}
			}()
			if err := test.call(); err == nil || err.Error() != wantError {
				t.Errorf("unknown dialect error = %v; want %q", err, wantError)
			}
		})
	}
}
