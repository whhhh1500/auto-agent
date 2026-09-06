package storage

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

func TestSQLAuditAndApprovalRejectNULFiltersBeforeSQL(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	audits, err := NewSQLAuditStore(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	approvals, err := NewSQLApprovalStore(db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	assertBeforeSQL := func(name string, err error) {
		t.Helper()
		if err == nil {
			t.Fatalf("%s was accepted", name)
		}
		if strings.Contains(err.Error(), "no such table") {
			t.Fatalf("%s reached SQL before validation: %v", name, err)
		}
	}
	assertBeforeSQL("record actor NUL", audits.RecordAudit(ctx, AuditEvent{Actor: "alice\x00", Action: "bind"}))
	assertBeforeSQL("list tenant NUL", func() error {
		_, _, err := audits.ListAudit(ctx, AuditFilter{TenantID: "acme\x00"})
		return err
	}())
	assertBeforeSQL("list action control", func() error {
		_, _, err := audits.ListAudit(ctx, AuditFilter{Action: "bind\n"})
		return err
	}())
	assertBeforeSQL("approval tenant NUL", func() error {
		_, err := approvals.ListApprovals(ctx, ApprovalFilter{TenantID: "acme\x00"})
		return err
	}())
}

func TestSQLAuditRoundTripStillWorks(t *testing.T) {
	sessions := newTestSQLStore(t)
	audits, err := NewSQLAuditStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := audits.RecordAudit(ctx, AuditEvent{Actor: "alice", Role: "admin", TenantID: "acme", Action: "bind", Target: "cap.tool"}); err != nil {
		t.Fatal(err)
	}
	events, total, err := audits.ListAudit(ctx, AuditFilter{TenantID: "acme", Limit: 10})
	if err != nil || total != 1 || len(events) != 1 || events[0].Actor != "alice" || events[0].Action != "bind" {
		t.Fatalf("audit round trip = %#v total=%d err=%v", events, total, err)
	}
}

func TestSQLAuditStoreRejectsOverflowAndOversizedDetail(t *testing.T) {
	sessions := newTestSQLStore(t)
	audits, err := NewSQLAuditStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	audits.maxEvents = 2
	ctx := context.Background()
	if err := audits.RecordAudit(ctx, AuditEvent{Actor: "alice", Action: "bind"}); err != nil {
		t.Fatal(err)
	}
	if err := audits.RecordAudit(ctx, AuditEvent{Actor: "bob", Action: "login"}); err != nil {
		t.Fatal(err)
	}
	if err := audits.RecordAudit(ctx, AuditEvent{Actor: "cara", Action: "logout"}); err == nil {
		t.Fatal("audit overflow was accepted")
	} else if !strings.Contains(err.Error(), "audit events exceed maximum of 2") {
		t.Fatalf("unexpected overflow error: %v", err)
	}
	if err := audits.RecordAudit(ctx, AuditEvent{
		Actor: "alice", Action: "bind",
		Detail: map[string]any{"blob": strings.Repeat("x", MaxAuditDetailBytes)},
	}); err == nil {
		t.Fatal("oversized audit detail was accepted")
	}
	if _, err := sessions.db.ExecContext(ctx, "DELETE FROM audit_events"); err != nil {
		t.Fatal(err)
	}
	if err := audits.RecordAudit(ctx, AuditEvent{Actor: "dana", Action: "bind"}); err != nil {
		t.Fatalf("cleared audit table did not free a slot: %v", err)
	}
}
