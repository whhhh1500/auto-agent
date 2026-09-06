package server_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	_ "modernc.org/sqlite"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/server"
)

// minimalS3 is a tiny fake S3 server: bucket creation, object put/get/delete
// and prefix listing — enough for the storage settings API tests.
type minimalS3 struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func newMinimalS3() *minimalS3 { return &minimalS3{objects: map[string][]byte{}} }

func (m *minimalS3) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/b1/")
		m.mu.Lock()
		defer m.mu.Unlock()
		switch r.Method {
		case http.MethodPut:
			if key == "" { // create bucket
				w.WriteHeader(http.StatusOK)
				return
			}
			data := make([]byte, r.ContentLength)
			_, _ = io.ReadFull(r.Body, data)
			m.objects[key] = data
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if r.URL.Query().Get("list-type") != "" {
				prefix := r.URL.Query().Get("prefix")
				body := &strings.Builder{}
				body.WriteString("<ListBucketResult>")
				for name := range m.objects {
					if strings.HasPrefix(name, prefix) {
						body.WriteString("<Contents><Key>" + name + "</Key></Contents>")
					}
				}
				body.WriteString("<IsTruncated>false</IsTruncated></ListBucketResult>")
				_, _ = w.Write([]byte(body.String()))
				return
			}
			data, ok := m.objects[key]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_, _ = w.Write(data)
		case http.MethodDelete:
			delete(m.objects, key)
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
}

func TestStorageSettingsHotSwapAndSecretMerge(t *testing.T) {
	// Fixture: embedded resources wrapped in a DynamicObjectStore.
	db, err := sql.Open("sqlite", t.TempDir()+"/storage.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err = storage.OpenSQLSessionStore(ctx, db, storage.SQLDialectSQLite); err != nil {
		t.Fatal(err)
	}
	accounts, err := storage.NewSQLAccountStore(db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	profiles := core.NewAgentProfileRegistry()
	embedded, err := storage.NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	resources := storage.NewDynamicObjectStore(embedded, "embedded")
	runtime := &core.Runtime{
		Capabilities: core.NewCapabilityRegistry(), Profiles: profiles,
		Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) {
			return core.MockLlmAdapter{}, nil
		}),
	}
	api, err := server.New(server.Config{
		Runtime: runtime, Sessions: core.NewMemorySessionStore(),
		Authenticator: server.AccountAuthenticator{Store: accounts},
		Accounts:      accounts, Resources: resources, EmbeddedResources: embedded,
	})
	if err != nil {
		t.Fatal(err)
	}
	httpServer := newTestHTTPServer(t, api.Handler())

	// First boot: bootstrap admin, capture credentials for login.
	adminEmail, adminPassword, created, err := storage.BootstrapAdmin(ctx, accounts, func(string, ...any) {})
	if err != nil || !created {
		t.Fatalf("bootstrap: created=%v err=%v", created, err)
	}
	adminToken := loginWith(t, httpServer.URL, adminEmail, adminPassword)
	authed := map[string]string{
		"Content-Type":  "application/json",
		"Authorization": "Bearer " + adminToken,
	}

	// Default backend is the embedded filesystem.
	active0 := readBody(t, doJSONWithHeaders(t, http.MethodGet, httpServer.URL+"/v1/admin/storage/resources", "alice", authed, ""))
	if !strings.Contains(active0, `"active":"embedded"`) {
		t.Fatalf("default backend should be embedded: %s", active0)
	}

	// Save an S3 config pointing at the minimal fake: hot swap to "s3".
	s3Fake := httptest.NewServer(newMinimalS3().handler())
	defer s3Fake.Close()
	saved := doJSONWithHeaders(t, http.MethodPut, httpServer.URL+"/v1/admin/storage/resources", "alice", authed, `{
		"type": "s3", "endpoint": "`+s3Fake.URL+`", "bucket": "b1",
		"access_key": "ak", "secret_key": "pre-middle-unique-tail", "path_style": true
	}`)
	if saved.StatusCode != http.StatusOK {
		t.Fatalf("save status=%d body=%s", saved.StatusCode, readBody(t, saved))
	}
	active := readBody(t, doJSONWithHeaders(t, http.MethodGet, httpServer.URL+"/v1/admin/storage/resources", "alice", authed, ""))
	if !strings.Contains(active, `"active":"s3"`) {
		t.Fatalf("active backend should be s3: %s", active)
	}

	// GET includes only a fixed preview, never the persisted secret or length.
	cfg := readBody(t, doJSONWithHeaders(t, http.MethodGet, httpServer.URL+"/v1/admin/storage/resources", "alice", authed, ""))
	if strings.Contains(cfg, "pre-middle-unique-tail") || strings.Contains(cfg, "middle") || strings.Contains(cfg, "unique") {
		t.Fatal("storage secret leaked through GET")
	}
	if strings.Contains(cfg, `"secret_key"`) || !strings.Contains(cfg, `"has_secret":true`) || !strings.Contains(cfg, `"secret_preview":"pre••••••••tail"`) {
		t.Fatalf("storage GET must expose only fixed preview state: %s", cfg)
	}

	// Re-save without secret_key: presence semantics preserve the stored secret.
	resaved := doJSONWithHeaders(t, http.MethodPut, httpServer.URL+"/v1/admin/storage/resources", "alice", authed, `{
		"type": "s3", "endpoint": "`+s3Fake.URL+`", "bucket": "b1",
		"access_key": "ak", "path_style": true
	}`)
	if resaved.StatusCode != http.StatusOK {
		t.Fatalf("omitted-secret re-save status=%d body=%s", resaved.StatusCode, readBody(t, resaved))
	}
	// Older Console clients sent the former fixed mask to request a preserve.
	// The compatibility mapper accepts it, but GET must never emit that mask.
	legacy := doJSONWithHeaders(t, http.MethodPut, httpServer.URL+"/v1/admin/storage/resources", "alice", authed, `{
		"type": "s3", "endpoint": "`+s3Fake.URL+`", "bucket": "b1",
		"access_key": "ak", "secret_key": "••••", "path_style": true
	}`)
	if legacy.StatusCode != http.StatusOK {
		t.Fatalf("legacy-mask re-save status=%d body=%s", legacy.StatusCode, readBody(t, legacy))
	}

	for name, body := range map[string]string{
		"empty": `{
			"type":"s3","endpoint":"` + s3Fake.URL + `","bucket":"b1","access_key":"ak","secret_key":""
		}`,
		"conflict": `{
			"type":"s3","endpoint":"` + s3Fake.URL + `","bucket":"b1","access_key":"ak","secret_key":"replacement-secret","clear_secret":true
		}`,
		"clear-s3": `{
			"type":"s3","endpoint":"` + s3Fake.URL + `","bucket":"b1","access_key":"ak","clear_secret":true
		}`,
	} {
		t.Run(name, func(t *testing.T) {
			response := doJSONWithHeaders(t, http.MethodPut, httpServer.URL+"/v1/admin/storage/resources", "alice", authed, body)
			if response.StatusCode != http.StatusBadRequest {
				t.Fatalf("%s status=%d body=%s", name, response.StatusCode, readBody(t, response))
			}
		})
	}

	// Explicitly clear while moving back to embedded. A following S3 write
	// without secret_key is a first configuration and must fail.
	if back := doJSONWithHeaders(t, http.MethodPut, httpServer.URL+"/v1/admin/storage/resources", "alice", authed, `{"type":"embedded","clear_secret":true}`); back.StatusCode != http.StatusOK {
		t.Fatalf("clear and swap back status=%d", back.StatusCode)
	}
	if raw, found, err := accounts.GetSetting(ctx, "storage.resources"); err != nil || !found || strings.Contains(raw, "pre-middle-unique-tail") {
		t.Fatalf("clear persisted config found=%v err=%v", found, err)
	}
	if active := readBody(t, doJSONWithHeaders(t, http.MethodGet, httpServer.URL+"/v1/admin/storage/resources", "alice", authed, "")); !strings.Contains(active, `"active":"embedded"`) {
		t.Fatalf("active backend should be embedded: %s", active)
	}
	missing := doJSONWithHeaders(t, http.MethodPut, httpServer.URL+"/v1/admin/storage/resources", "alice", authed, `{
		"type":"s3","endpoint":"`+s3Fake.URL+`","bucket":"b1","access_key":"ak"
	}`)
	if missing.StatusCode != http.StatusBadRequest {
		t.Fatalf("first s3 config without secret status=%d body=%s", missing.StatusCode, readBody(t, missing))
	}

	// Short secrets expose no characters at all and use the same fixed mask.
	short := doJSONWithHeaders(t, http.MethodPut, httpServer.URL+"/v1/admin/storage/resources", "alice", authed, `{
		"type":"s3","endpoint":"`+s3Fake.URL+`","bucket":"b1","access_key":"ak","secret_key":"short","path_style":true
	}`)
	if short.StatusCode != http.StatusOK {
		t.Fatalf("short-secret save status=%d body=%s", short.StatusCode, readBody(t, short))
	}
	shortView := readBody(t, doJSONWithHeaders(t, http.MethodGet, httpServer.URL+"/v1/admin/storage/resources", "alice", authed, ""))
	if strings.Contains(shortView, "short") || !strings.Contains(shortView, `"secret_preview":"••••••••"`) || strings.Contains(shortView, `"secret_key"`) {
		t.Fatalf("short secret GET must be fully hidden behind fixed mask: %s", shortView)
	}
}

// loginWith posts credentials and returns the bearer token.
func loginWith(t *testing.T, baseURL, email, password string) string {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, baseURL+"/v1/auth/login",
		strings.NewReader(`{"email":"`+email+`","password":"`+password+`"}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var data struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(response.Body).Decode(&data); err != nil || data.Token == "" {
		t.Fatalf("login failed: status=%d", response.StatusCode)
	}
	return data.Token
}
