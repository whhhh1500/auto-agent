package server_test

import (
	"net/http"
	"net/url"
	"testing"
)

func TestAccountTokenLifecycleRevokesLogoutAndPasswordRotationTokens(t *testing.T) {
	f := newControlFixture(t)

	logoutToken := f.login(f.adminEmail, f.adminPass)
	logout := f.do(http.MethodPost, "/v1/auth/logout", nil, logoutToken)
	if logout.StatusCode != http.StatusOK {
		t.Fatalf("logout status=%d body=%s", logout.StatusCode, readBody(t, logout))
	}
	_ = logout.Body.Close()

	loggedOut := f.do(http.MethodGet, "/v1/admin/overview", nil, logoutToken)
	if loggedOut.StatusCode != http.StatusUnauthorized {
		t.Fatalf("logged-out token overview status=%d body=%s", loggedOut.StatusCode, readBody(t, loggedOut))
	}
	_ = loggedOut.Body.Close()

	rotationToken := f.login(f.adminEmail, f.adminPass)
	otherLiveToken := f.login(f.adminEmail, f.adminPass)
	newPassword := f.adminPass + "-rotated"
	rotation := f.do(http.MethodPost, "/v1/admin/accounts/"+url.PathEscape(f.adminEmail)+"/password", map[string]string{"password": newPassword}, rotationToken)
	if rotation.StatusCode != http.StatusOK {
		t.Fatalf("password rotation status=%d body=%s", rotation.StatusCode, readBody(t, rotation))
	}
	_ = rotation.Body.Close()

	for name, token := range map[string]string{"rotation": rotationToken, "other": otherLiveToken} {
		response := f.do(http.MethodGet, "/v1/admin/overview", nil, token)
		if response.StatusCode != http.StatusUnauthorized {
			t.Fatalf("%s pre-rotation token overview status=%d body=%s", name, response.StatusCode, readBody(t, response))
		}
		_ = response.Body.Close()
	}

	oldPassword := f.do(http.MethodPost, "/v1/auth/login", map[string]string{"email": f.adminEmail, "password": f.adminPass}, "")
	if oldPassword.StatusCode != http.StatusUnauthorized {
		t.Fatalf("old password login status=%d body=%s", oldPassword.StatusCode, readBody(t, oldPassword))
	}
	_ = oldPassword.Body.Close()

	newToken := f.login(f.adminEmail, newPassword)
	overview := f.do(http.MethodGet, "/v1/admin/overview", nil, newToken)
	if overview.StatusCode != http.StatusOK {
		t.Fatalf("new password token overview status=%d body=%s", overview.StatusCode, readBody(t, overview))
	}
	_ = overview.Body.Close()
}
