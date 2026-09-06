package server

import (
	"context"
	"net/http"
	"testing"

	"github.com/cc-auto-agent/harness-core/pkg/extensions/runner"
)

func TestRunnerHTTPNamedDTOsAcceptForwardCompatibleFields(t *testing.T) {
	api, _, store := newRunnerProtocolServer(t)
	task, _, err := store.CreateTask(context.Background(), runner.Task{
		Capability: "runner.render", Args: map[string]any{}, MaxAttempts: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	handler := api.Handler()
	claim := runnerRequest(t, handler, http.MethodPost, "/v1/runners/claim", "worker-dto", map[string]any{"future_field": true})
	if claim.Code != http.StatusOK {
		t.Fatalf("claim status=%d body=%s", claim.Code, claim.Body.String())
	}

	renew := runnerRequest(t, handler, http.MethodPost, "/v1/runners/tasks/"+task.ID+"/renew", "worker-dto", map[string]any{
		"generation": 1, "future_field": "ignored",
	})
	if renew.Code != http.StatusOK {
		t.Fatalf("renew status=%d body=%s", renew.Code, renew.Body.String())
	}
	complete := runnerRequest(t, handler, http.MethodPost, "/v1/runners/tasks/"+task.ID+"/complete", "worker-dto", map[string]any{
		"generation": 1, "content": "done", "ok": true, "future_field": []string{"ignored"},
	})
	if complete.Code != http.StatusOK {
		t.Fatalf("complete status=%d body=%s", complete.Code, complete.Body.String())
	}
}

func TestRunnerHTTPNamedDTOsRejectInvalidGeneration(t *testing.T) {
	api, _, store := newRunnerProtocolServer(t)
	task, _, err := store.CreateTask(context.Background(), runner.Task{
		Capability: "runner.render", Args: map[string]any{}, MaxAttempts: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	response := runnerRequest(t, api.Handler(), http.MethodPost, "/v1/runners/tasks/"+task.ID+"/renew", "worker-dto", map[string]any{"generation": 0})
	if response.Code != http.StatusBadRequest {
		t.Fatalf("renew status=%d body=%s", response.Code, response.Body.String())
	}
}
