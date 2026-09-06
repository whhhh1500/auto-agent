package notificationtarget

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/cc-auto-agent/harness-core/pkg/adapter/sql/sqlkit"
	appnotification "github.com/cc-auto-agent/harness-core/pkg/app/notification"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
)

type testCipher struct{}

func (testCipher) Encrypt(value string) (string, error) {
	return "sealed:" + base64.RawStdEncoding.EncodeToString([]byte(value)), nil
}
func (testCipher) Decrypt(value string) (string, error) {
	if !strings.HasPrefix(value, "sealed:") {
		return "", errors.New("bad ciphertext")
	}
	decoded, err := base64.RawStdEncoding.DecodeString(strings.TrimPrefix(value, "sealed:"))
	return string(decoded), err
}

func openNotificationSQLite(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "notification.db")+"?_pragma=busy_timeout%285000%29&_pragma=journal_mode%28WAL%29")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := storage.OpenSQLSessionStore(context.Background(), db, storage.SQLDialectSQLite); err != nil {
		db.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func notificationStoreFixture(t *testing.T, cipher Cipher) (*Store, appnotification.TargetDescriptor) {
	t.Helper()
	db := openNotificationSQLite(t)
	channel := appnotification.ChannelRef{ID: "webhook", Version: "1"}
	store, err := New(Options{DB: db, Dialect: sqlkit.SQLite, Cipher: cipher, Channels: []appnotification.ChannelRef{channel}})
	if err != nil {
		t.Fatal(err)
	}
	target, err := appnotification.NewTargetRef("ops")
	if err != nil {
		t.Fatal(err)
	}
	return store, appnotification.TargetDescriptor{Target: target, Channel: channel, Label: "Operations", Formats: []string{"text", "markdown"}}
}

func TestStoreTenantScopeRevisionAndDisabledSemantics(t *testing.T) {
	store, descriptor := notificationStoreFixture(t, testCipher{})
	ctx := context.Background()
	config := appnotification.TargetConfiguration{Payload: []byte(`{"url":"https://example.test/hook","secret":"super-secret"}`)}
	revision, err := store.Create(ctx, "tenant-a", descriptor, config, true)
	if err != nil || revision != "1" {
		t.Fatalf("create revision=%q err=%v", revision, err)
	}
	if _, err := store.Create(ctx, "tenant-a", descriptor, config, true); !errors.Is(err, appnotification.ErrTargetRevisionConflict) {
		t.Fatalf("duplicate create error=%v", err)
	}
	if got, err := store.List(ctx, "tenant-b"); err != nil || len(got) != 0 {
		t.Fatalf("cross-tenant list=%#v err=%v", got, err)
	}
	if _, err := store.ResolveConfig(ctx, "tenant-b", descriptor.Target, descriptor.Channel); !errors.Is(err, appnotification.ErrTargetNotFound) {
		t.Fatalf("cross-tenant resolve error=%v", err)
	}
	resolved, err := store.ResolveConfig(ctx, "tenant-a", descriptor.Target, descriptor.Channel)
	if err != nil || string(resolved.Payload) != string(config.Payload) {
		t.Fatalf("resolve=%q err=%v", resolved.Payload, err)
	}
	resolved.Payload[0] = 'X'
	resolvedAgain, err := store.ResolveConfig(ctx, "tenant-a", descriptor.Target, descriptor.Channel)
	if err != nil || string(resolvedAgain.Payload) != string(config.Payload) {
		t.Fatalf("resolve ownership=%q err=%v", resolvedAgain.Payload, err)
	}
	if got, err := store.Update(ctx, "tenant-a", descriptor, config, false, "1"); err != nil || got != "2" {
		t.Fatalf("disable revision=%q err=%v", got, err)
	}
	if _, err := store.ResolveConfig(ctx, "tenant-a", descriptor.Target, descriptor.Channel); !errors.Is(err, appnotification.ErrTargetDisabled) {
		t.Fatalf("disabled resolve error=%v", err)
	}
	if got, err := store.List(ctx, "tenant-a"); err != nil || len(got) != 0 {
		t.Fatalf("disabled list=%#v err=%v", got, err)
	}
	if _, err := store.Update(ctx, "tenant-a", descriptor, config, true, "1"); !errors.Is(err, appnotification.ErrTargetRevisionConflict) {
		t.Fatalf("stale update error=%v", err)
	}
	if got, err := store.Update(ctx, "tenant-a", descriptor, appnotification.TargetConfiguration{}, true, "2"); err != nil || got != "3" {
		t.Fatalf("metadata-only update revision=%q err=%v", got, err)
	}
	preserved, err := store.ResolveConfig(ctx, "tenant-a", descriptor.Target, descriptor.Channel)
	if err != nil || string(preserved.Payload) != string(config.Payload) {
		t.Fatalf("metadata-only update replaced config=%q err=%v", preserved.Payload, err)
	}
	if err := store.Delete(ctx, "tenant-a", descriptor.Target, "2"); !errors.Is(err, appnotification.ErrTargetRevisionConflict) {
		t.Fatalf("delete stale revision error=%v", err)
	}
	if err := store.Delete(ctx, "tenant-a", descriptor.Target, "3"); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, "tenant-a", descriptor.Target, "2"); !errors.Is(err, appnotification.ErrTargetNotFound) {
		t.Fatalf("deleted target error=%v", err)
	}
}

func TestStorePreservedConfigurationRequiresExactChannel(t *testing.T) {
	db := openNotificationSQLite(t)
	firstChannel := appnotification.ChannelRef{ID: "webhook", Version: "1"}
	secondChannel := appnotification.ChannelRef{ID: "webhook", Version: "2"}
	store, err := New(Options{DB: db, Dialect: sqlkit.SQLite, Cipher: testCipher{}, Channels: []appnotification.ChannelRef{firstChannel, secondChannel}})
	if err != nil {
		t.Fatal(err)
	}
	target, err := appnotification.NewTargetRef("ops")
	if err != nil {
		t.Fatal(err)
	}
	descriptor := appnotification.TargetDescriptor{Target: target, Channel: firstChannel, Label: "Ops"}
	config := appnotification.TargetConfiguration{Payload: []byte(`{"url":"https://example.test/hook","secret":"keep-me"}`)}
	if _, err := store.Create(context.Background(), "tenant-a", descriptor, config, true); err != nil {
		t.Fatal(err)
	}
	var beforeCiphertext string
	if err := db.QueryRow(`SELECT config_ciphertext FROM notification_targets WHERE tenant_id = 'tenant-a' AND target_ref = 'ops'`).Scan(&beforeCiphertext); err != nil {
		t.Fatal(err)
	}
	changed := descriptor
	changed.Channel = secondChannel
	if _, err := store.Update(context.Background(), "tenant-a", changed, appnotification.TargetConfiguration{}, true, "1"); !errors.Is(err, appnotification.ErrTargetChannelChangeRequiresConfiguration) {
		t.Fatalf("preserved channel change error=%v", err)
	}
	var revision, afterCiphertext string
	if err := db.QueryRow(`SELECT revision, config_ciphertext FROM notification_targets WHERE tenant_id = 'tenant-a' AND target_ref = 'ops'`).Scan(&revision, &afterCiphertext); err != nil {
		t.Fatal(err)
	}
	if revision != "1" || afterCiphertext != beforeCiphertext {
		t.Fatalf("rejected update mutated row revision=%s ciphertextChanged=%t", revision, afterCiphertext != beforeCiphertext)
	}
	newConfig := appnotification.TargetConfiguration{Payload: []byte(`{"url":"https://example.test/hook-v2","secret":"new-secret"}`)}
	if _, err := store.Update(context.Background(), "tenant-a", changed, newConfig, true, "1"); err != nil {
		t.Fatal(err)
	}
	resolved, err := store.ResolveConfig(context.Background(), "tenant-a", target, secondChannel)
	if err != nil || string(resolved.Payload) != string(newConfig.Payload) {
		t.Fatalf("explicit channel replacement payload=%q err=%v", resolved.Payload, err)
	}
}

func TestStoreCipherBoundaryAndNoPlaintextDescriptor(t *testing.T) {
	store, descriptor := notificationStoreFixture(t, testCipher{})
	secretURL := `https://example.test/private-hook`
	secret := `very-private-secret`
	config := appnotification.TargetConfiguration{Payload: []byte(fmt.Sprintf(`{"url":%q,"secret":%q}`, secretURL, secret))}
	if _, err := store.Create(context.Background(), "tenant-a", descriptor, config, true); err != nil {
		t.Fatal(err)
	}
	var ciphertext string
	if err := store.db.QueryRow(`SELECT config_ciphertext FROM notification_targets WHERE tenant_id = 'tenant-a' AND target_ref = 'ops'`).Scan(&ciphertext); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(ciphertext, secretURL) || strings.Contains(ciphertext, secret) {
		t.Fatalf("plaintext private config persisted: %q", ciphertext)
	}
	listed, err := store.List(context.Background(), "tenant-a")
	if err != nil || len(listed) != 1 {
		t.Fatalf("list=%#v err=%v", listed, err)
	}
	if strings.Contains(fmt.Sprintf("%#v", listed[0]), secretURL) || strings.Contains(fmt.Sprintf("%#v", listed[0]), secret) {
		t.Fatal("private config leaked through descriptor")
	}
}

func TestStoreWithoutCipherRejectsPrivateConfiguration(t *testing.T) {
	store, descriptor := notificationStoreFixture(t, nil)
	private := appnotification.TargetConfiguration{Payload: []byte("private")}
	if _, err := store.Create(context.Background(), "tenant-a", descriptor, private, true); !errors.Is(err, ErrCipherRequired) {
		t.Fatalf("no-cipher create error=%v", err)
	}
	if _, err := store.Create(context.Background(), "tenant-a", descriptor, appnotification.TargetConfiguration{}, true); err != nil {
		t.Fatalf("empty config create error=%v", err)
	}
	if _, err := store.ResolveConfig(context.Background(), "tenant-a", descriptor.Target, descriptor.Channel); err != nil {
		t.Fatalf("empty config resolve error=%v", err)
	}
}

func TestStorePostgresBindingAndInputBounds(t *testing.T) {
	db := openNotificationSQLite(t)
	channel := appnotification.ChannelRef{ID: "webhook", Version: "1"}
	store, err := New(Options{DB: db, Dialect: sqlkit.Postgres, Cipher: testCipher{}, Channels: []appnotification.ChannelRef{channel}})
	if err != nil {
		t.Fatal(err)
	}
	bound := store.bind("SELECT * FROM notification_targets WHERE tenant_id = ? AND target_ref = ?")
	if bound != "SELECT * FROM notification_targets WHERE tenant_id = $1 AND target_ref = $2" {
		t.Fatalf("bound query=%q", bound)
	}
	target, _ := appnotification.NewTargetRef("ops")
	descriptor := appnotification.TargetDescriptor{Target: target, Channel: channel}
	if _, err := store.Create(context.Background(), "tenant-a", descriptor, appnotification.TargetConfiguration{Payload: make([]byte, appnotification.MaxTargetConfigurationBytes+1)}, true); !errors.Is(err, appnotification.ErrInvalidTargetConfiguration) {
		t.Fatalf("large config error=%v", err)
	}
}

func TestDecodeFormatsRejectsNullWrongTypeAndTrailingData(t *testing.T) {
	for _, raw := range []string{"null", "{}", `"text"`, `["text"] trailing`} {
		var formats []string
		if err := decodeFormats(raw, &formats); err == nil {
			t.Errorf("decodeFormats(%q) accepted malformed value", raw)
		}
	}
}

func TestStoreTenantCapacityDoesNotOvershootUnderConcurrentCreate(t *testing.T) {
	store, descriptor := notificationStoreFixture(t, nil)
	ctx := context.Background()
	for index := 0; index < appnotification.MaxTargets-1; index++ {
		target, err := appnotification.NewTargetRef("target-" + strconv.Itoa(index))
		if err != nil {
			t.Fatal(err)
		}
		descriptor.Target = target
		if _, err := store.Create(ctx, "tenant-a", descriptor, appnotification.TargetConfiguration{}, true); err != nil {
			t.Fatalf("prefill %d: %v", index, err)
		}
	}
	var wait sync.WaitGroup
	for index := 0; index < 4; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			localDescriptor := descriptor.Clone()
			target, err := appnotification.NewTargetRef("racing-" + strconv.Itoa(index))
			if err != nil {
				return
			}
			localDescriptor.Target = target
			_, _ = store.Create(ctx, "tenant-a", localDescriptor, appnotification.TargetConfiguration{}, true)
		}(index)
	}
	wait.Wait()
	var count int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM notification_targets WHERE tenant_id = 'tenant-a'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count > appnotification.MaxTargets {
		t.Fatalf("tenant capacity overshot: %d", count)
	}
}
