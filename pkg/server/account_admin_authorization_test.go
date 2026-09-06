package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

func TestAccountAdminRouteAuthorizationPrecedesAvailabilityAndDecoding(t *testing.T) {
	ordinary := core.Principal{
		SubjectID: "ordinary", TenantID: "acme", Attributes: map[string]string{"role": "user"},
	}
	server := &Server{
		authenticator: AuthenticatorFunc(func(*http.Request) (core.Principal, error) { return ordinary, nil }),
		maxBody:       1 << 20,
	}

	// The feature is not configured and the JSON is malformed. Authorization
	// remains the first observable decision, matching the legacy handlers.
	request := httptest.NewRequest(http.MethodPost, "/v1/admin/accounts", strings.NewReader("{"))
	response := httptest.NewRecorder()
	server.handleCreateAccount(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("unauthorized malformed create status=%d body=%s", response.Code, response.Body.String())
	}
	request = httptest.NewRequest(http.MethodGet, "/v1/admin/accounts", nil)
	response = httptest.NewRecorder()
	server.handleListAccounts(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("unauthorized list against disabled feature status=%d body=%s", response.Code, response.Body.String())
	}

	// An ordinary user changing their own password must pass the coarse route
	// gate. With no service configured it reaches the expected 501 instead of
	// an accidental 403.
	request = httptest.NewRequest(http.MethodPost, "/v1/admin/accounts/ordinary/password", strings.NewReader(`{"password":"password-123"}`))
	request.SetPathValue("account", "ordinary")
	response = httptest.NewRecorder()
	server.handleSetPassword(response, request)
	if response.Code != http.StatusNotImplemented {
		t.Fatalf("self password against disabled feature status=%d body=%s", response.Code, response.Body.String())
	}
}
