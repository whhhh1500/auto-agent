package starter

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/execution"
)

func TestStarterHTTPFlow(t *testing.T) {
	called := false
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		if r.Method != http.MethodGet {
			t.Fatalf("method=%s", r.Method)
		}
		_, _ = w.Write([]byte("starter-ok"))
	}))
	defer remote.Close()
	api, err := NewServer(Config{
		LLM: core.MockLlmAdapter{}, Model: "mock-model", HTTPURL: remote.URL,
		HTTPExecutor: execution.HTTPExecutor{Client: remote.Client(), AllowPrivateNetwork: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(api.Handler())
	defer httpServer.Close()
	request, err := http.NewRequest(http.MethodPost, httpServer.URL+"/v1/sessions", bytes.NewBufferString(`{"profile_id":"starter.assistant"}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Harness-Tenant", "acme")
	request.Header.Set("X-Harness-Subject", "alice")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("session status=%d", response.StatusCode)
	}
	var session struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(response.Body).Decode(&session); err != nil {
		t.Fatal(err)
	}
	run, err := http.NewRequest(http.MethodPost, httpServer.URL+"/v1/sessions/"+session.ID+"/runs", bytes.NewBufferString(`{"message":"read the example"}`))
	if err != nil {
		t.Fatal(err)
	}
	run.Header = request.Header.Clone()
	response, err = http.DefaultClient.Do(run)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || !called {
		t.Fatalf("run status=%d called=%t body=%s", response.StatusCode, called, body)
	}
}
