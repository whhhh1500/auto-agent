package server_test

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	webhooktargets "github.com/whhhh1500/auto-agent/pkg/adapter/notification/webhook/targetresolver"
	notificationsql "github.com/whhhh1500/auto-agent/pkg/adapter/sql/notificationtarget"
	"github.com/whhhh1500/auto-agent/pkg/adapter/sql/sqlkit"
	appnotification "github.com/whhhh1500/auto-agent/pkg/app/notification"
	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/server"
	"github.com/whhhh1500/auto-agent/pkg/storage"
	_ "modernc.org/sqlite"
)

type notificationTestCipher struct{}

func (notificationTestCipher) Encrypt(value string) (string, error) {
	return base64.RawStdEncoding.EncodeToString([]byte(value)), nil
}
func (notificationTestCipher) Decrypt(value string) (string, error) {
	decoded, err := base64.RawStdEncoding.DecodeString(value)
	return string(decoded), err
}

type notificationTargetRepository struct {
	mu      sync.Mutex
	records map[string]appnotification.TargetRecord
}

func (repository *notificationTargetRepository) List(context.Context, string) ([]appnotification.TargetDescriptor, error) {
	return nil, errors.New("legacy list must not be called")
}
func (repository *notificationTargetRepository) ListRecords(_ context.Context, tenant string) ([]appnotification.TargetRecord, error) {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	result := make([]appnotification.TargetRecord, 0)
	for key, record := range repository.records {
		if strings.HasPrefix(key, tenant+"\x00") {
			result = append(result, record.Clone())
		}
	}
	return result, nil
}
func (repository *notificationTargetRepository) Create(_ context.Context, tenant string, descriptor appnotification.TargetDescriptor, _ appnotification.TargetConfiguration, enabled bool) (string, error) {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	key := tenant + "\x00" + descriptor.Target.String()
	if _, exists := repository.records[key]; exists {
		return "", appnotification.ErrTargetRevisionConflict
	}
	repository.records[key] = appnotification.TargetRecord{Descriptor: descriptor.Clone(), Enabled: enabled, Revision: "1"}
	return "1", nil
}
func (repository *notificationTargetRepository) Update(_ context.Context, tenant string, descriptor appnotification.TargetDescriptor, _ appnotification.TargetConfiguration, enabled bool, expected string) (string, error) {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	key := tenant + "\x00" + descriptor.Target.String()
	record, exists := repository.records[key]
	if !exists {
		return "", appnotification.ErrTargetNotFound
	}
	if record.Revision != expected {
		return "", appnotification.ErrTargetRevisionConflict
	}
	record.Descriptor, record.Enabled, record.Revision = descriptor.Clone(), enabled, "2"
	repository.records[key] = record
	return "2", nil
}
func (repository *notificationTargetRepository) Delete(_ context.Context, tenant string, target appnotification.TargetRef, expected string) error {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	key := tenant + "\x00" + target.String()
	record, exists := repository.records[key]
	if !exists {
		return appnotification.ErrTargetNotFound
	}
	if record.Revision != expected {
		return appnotification.ErrTargetRevisionConflict
	}
	delete(repository.records, key)
	return nil
}
func (repository *notificationTargetRepository) ResolveConfig(context.Context, string, appnotification.TargetRef, appnotification.ChannelRef) (appnotification.TargetConfiguration, error) {
	return appnotification.TargetConfiguration{}, errors.New("config resolution must not be called by management routes")
}

func TestNotificationTargetAdminCRUDIsTenantScopedAndSecretFree(t *testing.T) {
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	channel := appnotification.ChannelRef{ID: "webhook", Version: "1"}
	repository := &notificationTargetRepository{records: make(map[string]appnotification.TargetRecord)}
	targets, err := appnotification.NewService(repository, []appnotification.ChannelRef{channel})
	if err != nil {
		t.Fatal(err)
	}
	principal := core.Principal{SubjectID: "tenant-admin", TenantID: "tenant-a", Scope: global, Attributes: map[string]string{"role": storage.RoleAccountTenantAdmin}}
	api, err := server.New(server.Config{
		Runtime:  &core.Runtime{Capabilities: core.NewCapabilityRegistry(), Profiles: core.NewAgentProfileRegistry()},
		Sessions: core.NewMemorySessionStore(), NotificationTargets: targets,
		Authenticator: server.AuthenticatorFunc(func(*http.Request) (core.Principal, error) { return principal, nil }),
	})
	if err != nil {
		t.Fatal(err)
	}
	handler := api.Handler()
	body := `{"target_ref":"ops","channel_id":"webhook","channel_version":"1","label":"Ops","formats":["text","markdown"],"config":{"url":"https://example.test/hook","secret":"fake-secret"},"enabled":true}`
	request := httptest.NewRequest(http.MethodPost, "/v1/admin/notification-targets", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusCreated || strings.Contains(response.Body.String(), "fake-secret") || strings.Contains(response.Body.String(), "https://example.test") {
		t.Fatalf("create code=%d body=%s", response.Code, response.Body.String())
	}
	request = httptest.NewRequest(http.MethodGet, "/v1/admin/notification-targets", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"revision":"1"`) || strings.Contains(response.Body.String(), "fake-secret") {
		t.Fatalf("list code=%d body=%s", response.Code, response.Body.String())
	}
	unknownBody := strings.TrimSuffix(body, "}") + `,"unknown":true}`
	request = httptest.NewRequest(http.MethodPost, "/v1/admin/notification-targets", strings.NewReader(unknownBody))
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("unknown field code=%d body=%s", response.Code, response.Body.String())
	}
	request = httptest.NewRequest(http.MethodGet, "/v1/admin/notification-targets?tenant_id=tenant-b", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("cross-tenant code=%d body=%s", response.Code, response.Body.String())
	}
	principal.Attributes["role"] = storage.RoleAccountUser
	request = httptest.NewRequest(http.MethodGet, "/v1/admin/notification-targets", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("ordinary user code=%d body=%s", response.Code, response.Body.String())
	}
}

func TestNotificationTargetAdminSQLiteEndToEnd(t *testing.T) {
	db, err := sql.Open("sqlite", "file:"+t.TempDir()+"/notification.db?_pragma=busy_timeout%285000%29&_pragma=journal_mode%28WAL%29")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := storage.OpenSQLSessionStore(context.Background(), db, storage.SQLDialectSQLite); err != nil {
		t.Fatal(err)
	}
	channel := appnotification.ChannelRef{ID: "webhook", Version: "1"}
	repository, err := notificationsql.New(notificationsql.Options{DB: db, Dialect: sqlkit.SQLite, Cipher: notificationTestCipher{}, Channels: []appnotification.ChannelRef{channel}})
	if err != nil {
		t.Fatal(err)
	}
	targets, err := appnotification.NewService(repository, []appnotification.ChannelRef{channel}, webhooktargets.NewConfigurationValidator())
	if err != nil {
		t.Fatal(err)
	}
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	principal := core.Principal{SubjectID: "tenant-admin", TenantID: "tenant-a", Scope: global, Attributes: map[string]string{"role": storage.RoleAccountTenantAdmin}}
	api, err := server.New(server.Config{Runtime: &core.Runtime{Capabilities: core.NewCapabilityRegistry(), Profiles: core.NewAgentProfileRegistry()}, Sessions: core.NewMemorySessionStore(), NotificationTargets: targets, Authenticator: server.AuthenticatorFunc(func(*http.Request) (core.Principal, error) { return principal, nil })})
	if err != nil {
		t.Fatal(err)
	}
	handler := api.Handler()
	body := `{"target_ref":"ops","channel_id":"webhook","channel_version":"1","label":"Ops","formats":["text"],"config":{"url":"https://example.test/hook","secret":"sqlite-secret"},"enabled":true}`
	request := httptest.NewRequest(http.MethodPost, "/v1/admin/notification-targets", strings.NewReader(body))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("create code=%d body=%s", response.Code, response.Body.String())
	}
	request = httptest.NewRequest(http.MethodGet, "/v1/admin/notification-targets", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"revision":"1"`) || strings.Contains(response.Body.String(), "sqlite-secret") || strings.Contains(response.Body.String(), "example.test") {
		t.Fatalf("sqlite list code=%d body=%s", response.Code, response.Body.String())
	}
	update := `{"target_ref":"ops","channel_id":"webhook","channel_version":"1","label":"Ops 2","formats":["text","markdown"],"config":{"url":"https://example.test/hook2","secret":"sqlite-secret-2"},"enabled":true,"expected_revision":"1"}`
	request = httptest.NewRequest(http.MethodPut, "/v1/admin/notification-targets", strings.NewReader(update))
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"revision":"2"`) || strings.Contains(response.Body.String(), "sqlite-secret-2") {
		t.Fatalf("sqlite update code=%d body=%s", response.Code, response.Body.String())
	}
	updateWithoutConfig := `{"target_ref":"ops","channel_id":"webhook","channel_version":"1","label":"Ops 3","formats":["text"],"enabled":true,"expected_revision":"2"}`
	request = httptest.NewRequest(http.MethodPut, "/v1/admin/notification-targets", strings.NewReader(updateWithoutConfig))
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"revision":"3"`) {
		t.Fatalf("sqlite metadata-only update code=%d body=%s", response.Code, response.Body.String())
	}
	resolver, err := webhooktargets.New(targets)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := resolver.Resolve(context.Background(), "tenant-a", descriptorTarget("ops"))
	if err != nil || string(resolved.Secret) != "sqlite-secret-2" || resolved.URL != "https://example.test/hook2" {
		t.Fatalf("preserved config url=%q secret=%q err=%v", resolved.URL, resolved.Secret, err)
	}
	for index := range resolved.Secret {
		resolved.Secret[index] = 0
	}
}

func descriptorTarget(value string) appnotification.TargetRef {
	target, _ := appnotification.NewTargetRef(value)
	return target
}
