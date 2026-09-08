package server

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/storage"
)

func TestRunnerRoutesRejectOrdinaryUsersWithoutDedicatedAuthenticator(t *testing.T) {
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	user, _ := global.Child(core.ScopeRef{Kind: core.ScopeUser, ID: "alice"})
	server := &Server{authenticator: AuthenticatorFunc(func(*http.Request) (core.Principal, error) {
		return core.Principal{SubjectID: "alice", Scope: user, Attributes: map[string]string{"role": storage.RoleAccountUser}}, nil
	})}
	response := httptest.NewRecorder()
	if server.authenticateRunner(response, httptest.NewRequest(http.MethodPost, "/v1/runners/claim", nil)) {
		t.Fatal("ordinary user authenticated as a private runner")
	}
	if response.Code != http.StatusForbidden {
		t.Fatalf("unexpected status: %d", response.Code)
	}
}

func TestDedicatedRunnerAuthenticatorIsUsed(t *testing.T) {
	called := false
	server := &Server{runnerAuth: AuthenticatorFunc(func(*http.Request) (core.Principal, error) {
		called = true
		return core.Principal{SubjectID: "runner-worker"}, nil
	})}
	response := httptest.NewRecorder()
	if !server.authenticateRunner(response, httptest.NewRequest(http.MethodPost, "/v1/runners/claim", nil)) || !called {
		t.Fatal("dedicated runner authenticator was not used")
	}

	server.runnerAuth = AuthenticatorFunc(func(*http.Request) (core.Principal, error) {
		return core.Principal{}, errors.New("bad runner token")
	})
	response = httptest.NewRecorder()
	if server.authenticateRunner(response, httptest.NewRequest(http.MethodPost, "/v1/runners/claim", nil)) {
		t.Fatal("invalid runner credential was accepted")
	}
}
