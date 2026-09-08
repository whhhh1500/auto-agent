package notificationextension_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/nikoksr/notify"
	"github.com/whhhh1500/auto-agent/pkg/adapter/notification/notifybridge"
	notificationruntime "github.com/whhhh1500/auto-agent/pkg/adapter/notification/runtime"
	notificationsql "github.com/whhhh1500/auto-agent/pkg/adapter/sql/notificationtarget"
	"github.com/whhhh1500/auto-agent/pkg/adapter/sql/sqlkit"
	"github.com/whhhh1500/auto-agent/pkg/app/notification"
	"github.com/whhhh1500/auto-agent/pkg/storage"
	_ "modernc.org/sqlite"
)

// This service captures messages locally; the example never contacts a platform.
type captureService struct {
	config string
	sent   *[]string
}

func (service captureService) Send(ctx context.Context, subject, message string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	*service.sent = append(*service.sent, service.config+":"+subject+":"+message)
	return nil
}

func TestCustomPlatformsWithLiveSQLTargetConfiguration(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	if _, err = storage.OpenSQLSessionStore(ctx, db, storage.SQLDialectSQLite); err != nil {
		t.Fatal(err)
	}
	key := make([]byte, 32)
	if _, err = rand.Read(key); err != nil {
		t.Fatal(err)
	}
	cipher, err := storage.NewCipher(hex.EncodeToString(key))
	if err != nil {
		t.Fatal(err)
	}
	clear(key)

	var sent []string
	refs := []notification.ChannelRef{{ID: "custom-alpha", Version: "1"}, {ID: "custom-beta", Version: "7"}}
	var registrations []notificationruntime.Registration
	for _, ref := range refs {
		registrations = append(registrations, notificationruntime.Registration{
			Ref: ref,
			Build: func(targets *notification.Service) (notification.Channel, error) {
				return notifybridge.New(ref, targets, func(_ context.Context, config []byte) (notify.Notifier, error) {
					return captureService{config: string(config), sent: &sent}, nil
				})
			},
		})
	}
	assembly, err := notificationruntime.New(registrations)
	if err != nil {
		t.Fatal(err)
	}
	repository, err := notificationsql.New(notificationsql.Options{DB: db, Dialect: sqlkit.SQLite, Cipher: cipher, Channels: assembly.Refs()})
	if err != nil {
		t.Fatal(err)
	}
	targets, err := notification.NewService(repository, assembly.Refs(), assembly.Validators()...)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := assembly.BuildRegistry(targets)
	if err != nil {
		t.Fatal(err)
	}
	target, err := notification.NewTargetRef("ops")
	if err != nil {
		t.Fatal(err)
	}
	descriptor := notification.TargetDescriptor{Target: target, Channel: refs[0], Formats: []string{"text"}}
	config := func(value string) notification.TargetConfiguration {
		return notification.TargetConfiguration{Payload: []byte(value)}
	}
	revision, err := targets.Create(ctx, "tenant-a", descriptor, config("alpha-private"), true)
	if err != nil {
		t.Fatal(err)
	}
	beta := descriptor.Clone()
	beta.Channel = refs[1]
	if _, err = targets.Create(ctx, "tenant-b", beta, config("beta-private"), true); err != nil {
		t.Fatal(err)
	}
	var ciphertext string
	if err = db.QueryRow("SELECT config_ciphertext FROM notification_targets WHERE tenant_id = ?", "tenant-a").Scan(&ciphertext); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(ciphertext, "alpha-private") || !strings.HasPrefix(ciphertext, storage.SecretPrefix) {
		t.Fatal("configuration was not encrypted")
	}
	delivery := notification.Delivery{TenantID: "tenant-a", SessionID: "session", RunID: "run", CallID: "call", IdempotencyKey: "call", Target: target, Text: "hello", Format: "text"}
	if _, err = registry.Deliver(ctx, refs[0], delivery); err != nil {
		t.Fatal(err)
	}
	if _, err = registry.Deliver(ctx, refs[1], delivery); err == nil {
		t.Fatal("wrong channel delivered")
	}
	delivery.TenantID = "tenant-b"
	if _, err = registry.Deliver(ctx, refs[1], delivery); err != nil {
		t.Fatal(err)
	}
	delivery.TenantID = "tenant-a"
	revision, err = targets.Update(ctx, "tenant-a", descriptor, config("rotated-private"), true, revision)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = registry.Deliver(ctx, refs[0], delivery); err != nil {
		t.Fatal(err)
	}
	revision, err = targets.Update(ctx, "tenant-a", descriptor, notification.TargetConfiguration{}, false, revision)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = registry.Deliver(ctx, refs[0], delivery); err == nil {
		t.Fatal("disabled target delivered")
	}
	visible, err := targets.List(ctx, "tenant-a")
	if err != nil || len(visible) != 0 {
		t.Fatalf("disabled target visible: %v", err)
	}
	if err = targets.Delete(ctx, "tenant-a", target, revision); err != nil {
		t.Fatal(err)
	}
	if _, err = registry.Deliver(ctx, refs[0], delivery); err == nil {
		t.Fatal("deleted target delivered")
	}
	want := []string{"alpha-private:auto-agent:hello", "beta-private:auto-agent:hello", "rotated-private:auto-agent:hello"}
	if len(sent) != len(want) {
		t.Fatalf("send count %d, want %d", len(sent), len(want))
	}
	for i := range want {
		if sent[i] != want[i] {
			t.Fatalf("delivery %d mixed target configuration", i)
		}
	}
}
