package notificationtarget

import (
	"context"
	"errors"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/whhhh1500/auto-agent/pkg/adapter/sql/sqlkit"
	appnotification "github.com/whhhh1500/auto-agent/pkg/app/notification"
	"github.com/whhhh1500/auto-agent/pkg/storage"
)

func TestPostgresStoreTenantCASAndOpaqueConfig(t *testing.T) {
	dsn := os.Getenv("HARNESS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("HARNESS_TEST_PG_DSN is not configured")
	}
	base, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	admin := stdlib.OpenDB(*base)
	t.Cleanup(func() { _ = admin.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	schema := "harness_test_notification_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	if _, err := admin.ExecContext(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.ExecContext(context.Background(), "DROP SCHEMA "+schema+" CASCADE") })
	db := stdlib.OpenDB(*base)
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(ctx, "SET search_path TO "+schema); err != nil {
		t.Fatal(err)
	}
	if _, err := storage.OpenSQLSessionStore(ctx, db, storage.SQLDialectPostgres); err != nil {
		t.Fatal(err)
	}
	channel := appnotification.ChannelRef{ID: "webhook", Version: "1"}
	changedChannel := appnotification.ChannelRef{ID: "alternate", Version: "2"}
	store, err := New(Options{DB: db, Dialect: sqlkit.Postgres, Cipher: testCipher{}, Channels: []appnotification.ChannelRef{channel, changedChannel}})
	if err != nil {
		t.Fatal(err)
	}
	target, _ := appnotification.NewTargetRef("ops")
	descriptor := appnotification.TargetDescriptor{Target: target, Channel: channel, Label: "ops"}
	originalConfig := appnotification.TargetConfiguration{Payload: []byte("opaque")}
	if revision, err := store.Create(ctx, "tenant-a", descriptor, originalConfig, true); err != nil || revision != "1" {
		t.Fatalf("create revision=%q err=%v", revision, err)
	}
	if _, err := store.Update(ctx, "tenant-a", descriptor, appnotification.TargetConfiguration{Payload: []byte("opaque-2")}, true, "0"); !errors.Is(err, appnotification.ErrInvalidTargetRevision) {
		t.Fatalf("invalid revision error=%v", err)
	}
	if records, err := store.ListRecords(ctx, "tenant-a"); err != nil || len(records) != 1 || records[0].Revision != "1" {
		t.Fatalf("records=%#v err=%v", records, err)
	}
	if revision, err := store.Update(ctx, "tenant-a", descriptor, appnotification.TargetConfiguration{}, true, "1"); err != nil || revision != "2" {
		t.Fatalf("same-channel preserve revision=%q err=%v", revision, err)
	}
	preserved, err := store.ResolveConfig(ctx, "tenant-a", target, channel)
	if err != nil || string(preserved.Payload) != string(originalConfig.Payload) {
		t.Fatalf("same-channel preserved payload=%q err=%v", preserved.Payload, err)
	}
	var beforeRevision, beforeCiphertext string
	if err := db.QueryRowContext(ctx, `SELECT revision, config_ciphertext FROM notification_targets WHERE tenant_id = $1 AND target_ref = $2`, "tenant-a", "ops").Scan(&beforeRevision, &beforeCiphertext); err != nil {
		t.Fatal(err)
	}
	changed := descriptor
	changed.Channel = changedChannel
	if _, err := store.Update(ctx, "tenant-a", changed, appnotification.TargetConfiguration{}, true, "2"); !errors.Is(err, appnotification.ErrTargetChannelChangeRequiresConfiguration) {
		t.Fatalf("preserved channel switch error=%v", err)
	}
	var afterRevision, afterCiphertext string
	if err := db.QueryRowContext(ctx, `SELECT revision, config_ciphertext FROM notification_targets WHERE tenant_id = $1 AND target_ref = $2`, "tenant-a", "ops").Scan(&afterRevision, &afterCiphertext); err != nil {
		t.Fatal(err)
	}
	if afterRevision != beforeRevision || afterCiphertext != beforeCiphertext {
		t.Fatalf("rejected channel switch mutated row revision=%s/%s ciphertextChanged=%t", beforeRevision, afterRevision, beforeCiphertext != afterCiphertext)
	}
	newConfig := appnotification.TargetConfiguration{Payload: []byte("opaque-alternate")}
	if revision, err := store.Update(ctx, "tenant-a", changed, newConfig, true, "2"); err != nil || revision != "3" {
		t.Fatalf("explicit channel switch revision=%q err=%v", revision, err)
	}
	switched, err := store.ResolveConfig(ctx, "tenant-a", target, changedChannel)
	if err != nil || string(switched.Payload) != string(newConfig.Payload) {
		t.Fatalf("explicit channel switch payload=%q err=%v", switched.Payload, err)
	}
}
