package accountadmin

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	appidentity "github.com/cc-auto-agent/harness-core/pkg/app/identity"
)

func TestCreateAccountRequestValidatesTypedRole(t *testing.T) {
	var request CreateAccountRequest
	if err := json.Unmarshal([]byte(`{"account":"alice","password":"password-123","role":"user","tenant_id":"acme"}`), &request); err != nil {
		t.Fatal(err)
	}
	if err := request.Validate(); err != nil {
		t.Fatalf("valid account request rejected: %v", err)
	}
	if request.Role != appidentity.RoleUser {
		t.Fatalf("role=%q", request.Role)
	}

	request.Role = appidentity.Role("operator")
	if err := request.Validate(); err == nil {
		t.Fatal("invalid role accepted")
	}
}

func TestSetAccountStatusRequestValidatesLifecycleStatus(t *testing.T) {
	for _, status := range []appidentity.Status{appidentity.StatusActive, appidentity.StatusDisabled} {
		request := SetAccountStatusRequest{Status: status}
		if err := request.Validate(); err != nil {
			t.Fatalf("status %q rejected: %v", status, err)
		}
	}
	for _, status := range []appidentity.Status{appidentity.StatusPendingActivation, "unknown"} {
		request := SetAccountStatusRequest{Status: status}
		err := request.Validate()
		if err == nil {
			t.Fatalf("status %q accepted", status)
		}
		if err.Error() != "status must be active or disabled" {
			t.Fatalf("status %q error=%q", status, err)
		}
	}
}

func TestAccountViewsDoNotExposeCredentialsAndPreserveWireShape(t *testing.T) {
	view := AccountView{
		Account: "alice", Email: "alice@example.com", Role: appidentity.RoleUser,
		TenantID: "acme", Status: appidentity.StatusActive,
		MustChangePassword: false, CreatedAt: time.Unix(10, 0).UTC(),
	}
	payload, err := json.Marshal(AccountListResponse{Accounts: []AccountView{view}})
	if err != nil {
		t.Fatal(err)
	}
	encoded := string(payload)
	for _, forbidden := range []string{"password_hash", "credential"} {
		if strings.Contains(encoded, forbidden) {
			t.Fatalf("account response contains credential field %q: %s", forbidden, encoded)
		}
	}
	if !strings.Contains(encoded, `"accounts":[{"account":"alice"`) || !strings.Contains(encoded, `"role":"user"`) {
		t.Fatalf("unexpected account response wire shape: %s", encoded)
	}
}

func TestTenantResponsesPreserveWireShape(t *testing.T) {
	payload, err := json.Marshal(TenantListResponse{Tenants: []TenantView{{ID: "acme", Name: "Acme", CreatedAt: time.Unix(10, 0).UTC()}}})
	if err != nil {
		t.Fatal(err)
	}
	if got := string(payload); !strings.Contains(got, `"tenants":[{"id":"acme","name":"Acme"`) {
		t.Fatalf("unexpected tenant response wire shape: %s", got)
	}
}
