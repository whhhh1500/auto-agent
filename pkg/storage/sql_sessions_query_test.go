package storage

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	. "github.com/whhhh1500/auto-agent/pkg/core"
)

func TestSanitizeLikePrefixEscapesWildcardsAndEscape(t *testing.T) {
	if got := sanitizeLikePrefix("sess_"); got != "sess!_" {
		t.Fatalf("underscore prefix = %q", got)
	}
	if got := sanitizeLikePrefix("100%"); got != "100!%" {
		t.Fatalf("percent prefix = %q", got)
	}
	if got := sanitizeLikePrefix("a!b"); got != "a!!b" {
		t.Fatalf("escape prefix = %q", got)
	}
}

func TestSQLSessionQueryPrefixIsLiteralAndRejectsNUL(t *testing.T) {
	store := newTestSQLStore(t)
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	ctx := context.Background()
	for _, id := range []string{"sess_one", "sessXone", "sess_two"} {
		scope, err := user.Child(ScopeRef{Kind: ScopeSession, ID: id})
		if err != nil {
			t.Fatal(err)
		}
		session, err := NewSession(SessionOptions{ID: id, ProfileID: "test.agent", Principal: principal, Scope: scope})
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Create(ctx, session); err != nil {
			t.Fatal(err)
		}
	}

	underscore, total, err := store.QuerySessions(ctx, SessionFilter{TenantID: "tenant-a", UserID: "user-a", IDPrefix: "sess_"})
	if err != nil || total != 2 {
		t.Fatalf("literal underscore prefix total=%d err=%v rows=%#v", total, err, underscore)
	}
	got := map[string]bool{}
	for _, row := range underscore {
		got[row.ID] = true
	}
	if !got["sess_one"] || !got["sess_two"] || got["sessXone"] {
		t.Fatalf("underscore prefix matched the wrong sessions: %#v", underscore)
	}

	percent, total, err := store.QuerySessions(ctx, SessionFilter{TenantID: "tenant-a", UserID: "user-a", IDPrefix: "%"})
	if err != nil || total != 0 || len(percent) != 0 {
		t.Fatalf("percent prefix must not wildcard: total=%d rows=%#v err=%v", total, percent, err)
	}

	all, total, err := store.QuerySessions(ctx, SessionFilter{TenantID: "tenant-a", UserID: "user-a", IDPrefix: "  "})
	if err != nil || total != 3 {
		t.Fatalf("whitespace prefix should be ignored: total=%d err=%v", total, err)
	}
	_ = all

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	bare := &SQLSessionStore{db: db, dialect: SQLDialectSQLite}
	_, _, err = bare.QuerySessions(ctx, SessionFilter{TenantID: "tenant-a", UserID: "user-a", IDPrefix: "pre\x00fix"})
	if err == nil || strings.Contains(err.Error(), "no such table") {
		t.Fatalf("NUL prefix reached SQL: %v", err)
	}
}

func TestSQLListSessionsRejectsNULIdentity(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	bare := &SQLSessionStore{db: db, dialect: SQLDialectSQLite}
	ctx := context.Background()
	_, err = bare.ListSessions(ctx, "tenant\x00a", "user-a", 10)
	if err == nil || strings.Contains(err.Error(), "no such table") {
		t.Fatalf("NUL tenant reached SQL: %v", err)
	}
}
