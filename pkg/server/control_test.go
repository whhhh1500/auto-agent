package server_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"github.com/whhhh1500/auto-agent/pkg/storage"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	_ "modernc.org/sqlite"

	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/server"
)

// controlFixture is a full first-boot deployment: embedded SQL store, built-in
// accounts, embedded resource filesystem, mock models.
type controlFixture struct {
	t            *testing.T
	httpServer   *httptest.Server
	adminEmail   string
	adminPass    string
	db           *sql.DB
	profiles     *core.AgentProfileRegistry
	capabilities *core.CapabilityRegistry
	journal      *storage.SQLBindingJournal
}

func newControlFixture(t *testing.T) *controlFixture {
	t.Helper()
	f := &controlFixture{t: t}
	db, err := sql.Open("sqlite", t.TempDir()+"/control.db")
	if err != nil {
		t.Fatal(err)
	}
	f.db = db
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	sqlStore, err := storage.OpenSQLSessionStore(ctx, db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	accounts, err := storage.NewSQLAccountStore(db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	resourceDir := t.TempDir()
	embedded, err := storage.NewFileObjectStore(resourceDir)
	if err != nil {
		t.Fatal(err)
	}
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	profiles := core.NewAgentProfileRegistry()
	name := "Test Agent"
	model := core.ModelSelection{Provider: "mock", Model: "mock-1"}
	if err := profiles.Bind(core.AgentProfileLayer{
		Scope: global, ProfileID: "test.agent", Name: &name, Model: &model,
	}); err != nil {
		t.Fatal(err)
	}
	f.profiles = profiles
	capabilities := core.NewCapabilityRegistry()
	f.capabilities = capabilities
	runtime := &core.Runtime{
		Capabilities: capabilities, Profiles: profiles,
		Policy: core.NewPolicyRegistry(),
		Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) {
			return core.MockLlmAdapter{}, nil
		}),
	}
	auditStore, err := storage.NewSQLAuditStore(db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	obsStore, err := storage.NewSQLObsStore(db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := storage.NewSQLBindingJournal(db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	f.journal = journal
	api, err := server.New(server.Config{
		Runtime: runtime, Sessions: sqlStore,
		Authenticator: server.AccountAuthenticator{Store: accounts},
		Accounts:      accounts, Resources: embedded, Audit: auditStore, Obs: obsStore,
		BindingJournal: journal,
	})
	if err != nil {
		t.Fatal(err)
	}
	f.httpServer = newTestHTTPServer(t, api.Handler())

	// First boot: the platform auto-creates exactly one admin account.
	email, password, created, err := storage.BootstrapAdmin(ctx, accounts, func(string, ...any) {})
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("fresh deployment must bootstrap exactly one admin")
	}
	f.adminEmail, f.adminPass = email, password
	return f
}

// login exchanges credentials for a bearer token and asserts the admin role.
func (f *controlFixture) login(email, password string) string {
	f.t.Helper()
	token, role := f.loginAny(email, password)
	if role != storage.RoleAccountAdmin {
		f.t.Fatalf("expected admin role, got %q", role)
	}
	return token
}

// loginAny logs in without role assertions.
func (f *controlFixture) loginAny(email, password string) (string, string) {
	f.t.Helper()
	response := f.do("POST", "/v1/auth/login", map[string]any{"email": email, "password": password}, "")
	if response.StatusCode != http.StatusOK {
		f.t.Fatalf("login status=%d body=%s", response.StatusCode, readBody(f.t, response))
	}
	var data struct {
		Token string `json:"token"`
		Role  string `json:"role"`
	}
	decodeBody(f.t, response, &data)
	if data.Token == "" {
		f.t.Fatalf("empty token: %#v", data)
	}
	return data.Token, data.Role
}

// do performs an authenticated request with the bearer token.
func (f *controlFixture) do(method, path string, body any, token string) *http.Response {
	f.t.Helper()
	var reader io.Reader
	switch b := body.(type) {
	case nil:
	case io.Reader:
		reader = b
	default:
		reader = strings.NewReader(mustJSONString(f.t, b))
	}
	request, err := http.NewRequest(method, f.httpServer.URL+path, reader)
	if err != nil {
		f.t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		f.t.Fatal(err)
	}
	return response
}

func mustJSONString(t *testing.T, value any) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

func mustAccountStore(t *testing.T, db *sql.DB) storage.AccountStore {
	t.Helper()
	store, err := storage.NewSQLAccountStore(db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func TestFirstBootBootstrapAdminAndLogin(t *testing.T) {
	f := newControlFixture(t)
	if !regexp.MustCompile(`^admin_\d{5}$`).MatchString(f.adminEmail) {
		t.Fatalf("bootstrap account wrong format: %s", f.adminEmail)
	}
	if len(f.adminPass) < 12 {
		t.Fatalf("bootstrap password too weak: %d chars", len(f.adminPass))
	}

	// Second bootstrap is a no-op.
	_, _, created, err := storage.BootstrapAdmin(context.Background(), mustAccountStore(t, f.db), func(string, ...any) {})
	if err != nil || created {
		t.Fatalf("second bootstrap must be a no-op: created=%v err=%v", created, err)
	}

	// Wrong password is rejected without distinguishing users.
	response := f.do("POST", "/v1/auth/login", map[string]any{"email": f.adminEmail, "password": "wrong-password"}, "")
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong password must 401, got %d", response.StatusCode)
	}

	// Correct credentials yield a working bearer token.
	token := f.login(f.adminEmail, f.adminPass)
	overview := f.do("GET", "/v1/admin/overview", nil, token)
	if overview.StatusCode != http.StatusOK {
		t.Fatalf("authenticated overview status=%d", overview.StatusCode)
	}
	anonymous := f.do("GET", "/v1/admin/overview", nil, "")
	if anonymous.StatusCode != http.StatusUnauthorized {
		t.Fatalf("anonymous overview must 401, got %d", anonymous.StatusCode)
	}
}

func TestPendingAccountMustActivateBeforeAdminAccess(t *testing.T) {
	f := newControlFixture(t)
	accounts, err := storage.NewSQLAccountStore(f.db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	const accountID = "pending_admin"
	const initialPassword = "one-time-password-123"
	if err := accounts.CreateAccount(context.Background(), storage.Account{
		AccountID: accountID, Role: storage.RoleAccountAdmin, TenantID: "system",
		Status: storage.AccountPendingActivation, MustChangePassword: true,
	}, initialPassword); err != nil {
		t.Fatal(err)
	}
	adminToken := f.login(f.adminEmail, f.adminPass)
	bypass := f.do("POST", "/v1/admin/accounts/"+accountID+"/status", map[string]any{"status": "active"}, adminToken)
	if bypass.StatusCode != http.StatusBadRequest {
		t.Fatalf("pending status bypass=%d body=%s", bypass.StatusCode, readBody(t, bypass))
	}
	login := f.do("POST", "/v1/auth/login", map[string]any{"account": accountID, "password": initialPassword}, "")
	if login.StatusCode != http.StatusOK {
		t.Fatalf("pending login status=%d body=%s", login.StatusCode, readBody(t, login))
	}
	var loginData struct {
		Token              string `json:"token"`
		Account            string `json:"account"`
		MustChangePassword bool   `json:"must_change_password"`
	}
	decodeBody(t, login, &loginData)
	if loginData.Account != accountID || loginData.Token == "" || !loginData.MustChangePassword {
		t.Fatalf("pending login = %#v", loginData)
	}
	denied := f.do("GET", "/v1/admin/overview", nil, loginData.Token)
	if denied.StatusCode != http.StatusForbidden {
		t.Fatalf("pending overview status=%d body=%s", denied.StatusCode, readBody(t, denied))
	}
	same := f.do("POST", "/v1/auth/activate", map[string]any{
		"password": initialPassword, "confirm": initialPassword,
	}, loginData.Token)
	if same.StatusCode != http.StatusBadRequest {
		t.Fatalf("same-password activation status=%d body=%s", same.StatusCode, readBody(t, same))
	}
	const newPassword = "new-secure-password-456"
	activated := f.do("POST", "/v1/auth/activate", map[string]any{
		"password": newPassword, "confirm": newPassword,
	}, loginData.Token)
	if activated.StatusCode != http.StatusOK {
		t.Fatalf("activation status=%d body=%s", activated.StatusCode, readBody(t, activated))
	}
	var activationData struct {
		Token              string `json:"token"`
		MustChangePassword bool   `json:"must_change_password"`
	}
	decodeBody(t, activated, &activationData)
	if activationData.Token == "" || activationData.MustChangePassword {
		t.Fatalf("activation response = %#v", activationData)
	}
	if response := f.do("GET", "/v1/admin/overview", nil, loginData.Token); response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("initial token survived activation: status=%d", response.StatusCode)
	}
	if response := f.do("GET", "/v1/admin/overview", nil, activationData.Token); response.StatusCode != http.StatusOK {
		t.Fatalf("active token overview status=%d body=%s", response.StatusCode, readBody(t, response))
	}
}

func TestV1RequestBodiesAcceptTopLevelUnknownFieldsButRejectInvalidJSON(t *testing.T) {
	f := newControlFixture(t)
	login := f.do("POST", "/v1/auth/login", strings.NewReader(`{"email":"`+f.adminEmail+`","password":"`+f.adminPass+`","future_client_field":true}`), "")
	if login.StatusCode != http.StatusOK {
		t.Fatalf("compatible login status=%d body=%s", login.StatusCode, readBody(t, login))
	}
	invalidType := f.do("POST", "/v1/auth/login", strings.NewReader(`{"email":false,"password":"x"}`), "")
	if invalidType.StatusCode != http.StatusBadRequest {
		t.Fatalf("type mismatch status=%d body=%s", invalidType.StatusCode, readBody(t, invalidType))
	}
	missingPassword := f.do("POST", "/v1/auth/login", strings.NewReader(`{"email":"`+f.adminEmail+`"}`), "")
	if missingPassword.StatusCode != http.StatusBadRequest {
		t.Fatalf("missing password status=%d body=%s", missingPassword.StatusCode, readBody(t, missingPassword))
	}
	trailing := f.do("POST", "/v1/auth/login", strings.NewReader(`{"email":"`+f.adminEmail+`","password":"`+f.adminPass+`}{}`), "")
	if trailing.StatusCode != http.StatusBadRequest {
		t.Fatalf("trailing value status=%d body=%s", trailing.StatusCode, readBody(t, trailing))
	}

	adminToken := f.login(f.adminEmail, f.adminPass)
	created := f.do("POST", "/v1/admin/accounts", strings.NewReader(`{"account":"compatible_account","password":"compatible-password-123","role":"user","tenant_id":"default","future_client_field":true}`), adminToken)
	if created.StatusCode != http.StatusCreated {
		t.Fatalf("compatible admin request status=%d body=%s", created.StatusCode, readBody(t, created))
	}
	wrongType := f.do("POST", "/v1/admin/accounts", strings.NewReader(`{"account":"wrong_type_account","password":false,"role":"user","tenant_id":"default"}`), adminToken)
	if wrongType.StatusCode != http.StatusBadRequest {
		t.Fatalf("admin type mismatch status=%d body=%s", wrongType.StatusCode, readBody(t, wrongType))
	}
	adminTrailing := f.do("POST", "/v1/admin/accounts", strings.NewReader(`{"account":"trailing_account","password":"compatible-password-123","role":"user","tenant_id":"default"}{}`), adminToken)
	if adminTrailing.StatusCode != http.StatusBadRequest {
		t.Fatalf("admin trailing value status=%d body=%s", adminTrailing.StatusCode, readBody(t, adminTrailing))
	}
	listed := f.do("GET", "/v1/admin/accounts", nil, adminToken)
	listedBody := readBody(t, listed)
	if listed.StatusCode != http.StatusOK || !strings.Contains(listedBody, `"accounts"`) || !strings.Contains(listedBody, `"role":"user"`) {
		t.Fatalf("account list status=%d body=%s", listed.StatusCode, listedBody)
	}
	if strings.Contains(listedBody, "password_hash") || strings.Contains(listedBody, "passwordHash") {
		t.Fatalf("account list exposed credential material: %s", listedBody)
	}
}

func TestOrdinaryUserCannotListAccounts(t *testing.T) {
	f := newControlFixture(t)
	accounts, err := storage.NewSQLAccountStore(f.db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	if err := accounts.CreateAccount(context.Background(), storage.Account{
		AccountID: "ordinary_user", Role: storage.RoleAccountUser,
		TenantID: "default", Status: storage.AccountActive,
	}, "ordinary-password-123"); err != nil {
		t.Fatal(err)
	}
	token, role := f.loginAny("ordinary_user", "ordinary-password-123")
	if role != storage.RoleAccountUser {
		t.Fatalf("role=%q", role)
	}
	response := f.do("GET", "/v1/admin/accounts", nil, token)
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("ordinary account list status=%d body=%s", response.StatusCode, readBody(t, response))
	}
}

func TestAccountAdministrationHTTPCompatibilityThroughApplicationService(t *testing.T) {
	f := newControlFixture(t)
	adminToken := f.login(f.adminEmail, f.adminPass)

	createdTenant := f.do("POST", "/v1/admin/tenants", map[string]any{"id": "acme", "name": "Acme"}, adminToken)
	if createdTenant.StatusCode != http.StatusCreated {
		t.Fatalf("tenant create status=%d body=%s", createdTenant.StatusCode, readBody(t, createdTenant))
	}
	var tenantPayload struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	decodeBody(t, createdTenant, &tenantPayload)
	if tenantPayload.ID != "acme" || tenantPayload.Name != "Acme" {
		t.Fatalf("tenant create payload=%#v", tenantPayload)
	}

	createdAccount := f.do("POST", "/v1/admin/accounts", map[string]any{
		"account": "acme-admin", "email": "acme-admin@example.test", "password": "tenant-admin-password",
		"role": "tenant_admin", "tenant_id": "acme", "future_client_field": true,
	}, adminToken)
	if createdAccount.StatusCode != http.StatusCreated {
		t.Fatalf("account create status=%d body=%s", createdAccount.StatusCode, readBody(t, createdAccount))
	}
	var accountPayload struct {
		Account string `json:"account"`
		Email   string `json:"email"`
		Role    string `json:"role"`
		Tenant  string `json:"tenant"`
	}
	decodeBody(t, createdAccount, &accountPayload)
	if accountPayload != (struct {
		Account string `json:"account"`
		Email   string `json:"email"`
		Role    string `json:"role"`
		Tenant  string `json:"tenant"`
	}{Account: "acme-admin", Email: "acme-admin@example.test", Role: "tenant_admin", Tenant: "acme"}) {
		t.Fatalf("account create payload=%#v", accountPayload)
	}

	tenantToken, role := f.loginAny("acme-admin", "tenant-admin-password")
	if role != storage.RoleAccountTenantAdmin {
		t.Fatalf("tenant role=%q", role)
	}
	listed := f.do("GET", "/v1/admin/accounts", nil, tenantToken)
	if listed.StatusCode != http.StatusOK {
		t.Fatalf("tenant list status=%d body=%s", listed.StatusCode, readBody(t, listed))
	}
	var accountList struct {
		Accounts []struct {
			Account string `json:"account"`
			Tenant  string `json:"tenant_id"`
			Role    string `json:"role"`
		} `json:"accounts"`
	}
	decodeBody(t, listed, &accountList)
	if len(accountList.Accounts) != 1 || accountList.Accounts[0].Account != "acme-admin" || accountList.Accounts[0].Tenant != "acme" || accountList.Accounts[0].Role != "tenant_admin" {
		t.Fatalf("tenant filtered list=%#v", accountList.Accounts)
	}

	selfPassword := f.do("POST", "/v1/admin/accounts/acme-admin/password", map[string]any{"password": "tenant-admin-password-2"}, tenantToken)
	if selfPassword.StatusCode != http.StatusOK {
		t.Fatalf("self password status=%d body=%s", selfPassword.StatusCode, readBody(t, selfPassword))
	}
	rotatedTenantToken, rotatedRole := f.loginAny("acme-admin", "tenant-admin-password-2")
	if rotatedRole != storage.RoleAccountTenantAdmin {
		t.Fatalf("rotated tenant role=%q", rotatedRole)
	}
	otherPassword := f.do("POST", "/v1/admin/accounts/"+f.adminEmail+"/password", map[string]any{"password": "tenant-admin-password-2"}, rotatedTenantToken)
	if otherPassword.StatusCode != http.StatusForbidden {
		t.Fatalf("tenant cross-account password status=%d body=%s", otherPassword.StatusCode, readBody(t, otherPassword))
	}
	badStatus := f.do("POST", "/v1/admin/accounts/acme-admin/status", map[string]any{"status": "pending_activation"}, adminToken)
	if badStatus.StatusCode != http.StatusBadRequest {
		t.Fatalf("pending status compatibility status=%d body=%s", badStatus.StatusCode, readBody(t, badStatus))
	}
}

func TestResourceManagementLifecycle(t *testing.T) {
	f := newControlFixture(t)
	token := f.login(f.adminEmail, f.adminPass)

	// Upload → get → list → delete.
	put := f.do("PUT", "/v1/resources/docs/readme.md", strings.NewReader("hello resource"), token)
	if put.StatusCode != http.StatusCreated {
		t.Fatalf("put status=%d body=%s", put.StatusCode, readBody(f.t, put))
	}
	get := f.do("GET", "/v1/resources/docs/readme.md", nil, token)
	body := readBody(f.t, get)
	if get.StatusCode != http.StatusOK || body != "hello resource" {
		t.Fatalf("get wrong: status=%d body=%s", get.StatusCode, body)
	}
	oversized := f.do("PUT", "/v1/resources/docs/oversized.bin", strings.NewReader(strings.Repeat("x", storage.MaxObjectBytes+1)), token)
	if oversized.StatusCode != http.StatusCreated {
		t.Fatalf("streaming resource above byte fallback limit must succeed, got %d body=%s", oversized.StatusCode, readBody(f.t, oversized))
	}
	listed := f.do("GET", "/v1/resources?prefix=docs/", nil, token)
	listBody := readBody(f.t, listed)
	if !strings.Contains(listBody, `"key":"docs/readme.md"`) || !strings.Contains(listBody, `"size":14`) {
		t.Fatalf("listing wrong: %s", listBody)
	}
	if deleted := f.do("DELETE", "/v1/resources/docs/readme.md", nil, token); deleted.StatusCode != http.StatusOK {
		t.Fatalf("delete status=%d", deleted.StatusCode)
	}
	if gone := f.do("GET", "/v1/resources/docs/readme.md", nil, token); gone.StatusCode != http.StatusNotFound {
		t.Fatalf("deleted resource must 404, got %d", gone.StatusCode)
	}

	// Tenant isolation: another tenant's admin cannot see this key.
	f.t.Helper()
	if err := mustAccountStore(t, f.db).CreateTenant(context.Background(), "acme", "Acme"); err != nil {
		t.Fatal(err)
	}
	if err := mustAccountStore(t, f.db).CreateAccount(context.Background(), storage.Account{
		Email: "bob@acme.com", Role: storage.RoleAccountTenantAdmin, TenantID: "acme", Status: storage.AccountActive,
	}, "bob-password-1"); err != nil {
		t.Fatal(err)
	}
	bobToken, _ := f.loginAny("bob@acme.com", "bob-password-1")
	bobListed := readBody(f.t, f.do("GET", "/v1/resources", nil, bobToken))
	if strings.Contains(bobListed, "docs/readme.md") {
		t.Fatalf("cross-tenant resource leak: %s", bobListed)
	}
}
