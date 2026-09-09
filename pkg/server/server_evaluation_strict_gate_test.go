package server_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/whhhh1500/auto-agent/pkg/control"
	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/evaluation"
	"github.com/whhhh1500/auto-agent/pkg/server"
	"github.com/whhhh1500/auto-agent/pkg/storage"
)

type strictGateModel struct{ text string }

func (strictGateModel) Provider() string { return "strict-gate" }
func (m strictGateModel) Stream(_ context.Context, _ core.GenerateOptions, emit func(core.StreamChunk)) error {
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: m.text})
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
	return nil
}

type strictGateStore struct {
	evaluation.Store
	baselineID string
	baseline   evaluation.RunResult
}

func (s strictGateStore) GetRun(ctx context.Context, id string) (evaluation.RunResult, error) {
	if id == s.baselineID {
		return s.baseline, nil
	}
	return s.Store.GetRun(ctx, id)
}

func newStrictGateServer(t *testing.T, dataset evaluation.Dataset, store evaluation.Store) (*httptest.Server, *core.AgentProfileRegistry, core.Principal) {
	t.Helper()
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product, _ := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "product"})
	tenant, _ := product.Child(core.ScopeRef{Kind: core.ScopeTenant, ID: "acme"})
	principal := core.Principal{
		SubjectID: "alice", TenantID: "acme", Scope: tenant,
		Grants:     core.NewPermissionSet(core.PermRead, core.PermWrite),
		Attributes: map[string]string{"role": storage.RoleAccountTenantAdmin},
	}
	profiles := core.NewAgentProfileRegistry()
	name := "Strict Gate Agent"
	stable := core.ModelSelection{Provider: "strict-gate", Model: "stable"}
	if err := profiles.Bind(core.AgentProfileLayer{Scope: product, ProfileID: "strict.agent", Name: &name, Model: &stable}); err != nil {
		t.Fatal(err)
	}
	runtime := &core.Runtime{
		Capabilities: core.NewCapabilityRegistry(), Profiles: profiles,
		Models: core.ModelResolverFunc(func(_ context.Context, selection core.ModelSelection) (core.LlmAdapter, error) {
			return strictGateModel{text: selection.Model}, nil
		}),
	}
	if _, _, err := store.PutDataset(context.Background(), dataset); err != nil {
		t.Fatal(err)
	}
	releases, err := control.NewReleaseManager(profiles)
	if err != nil {
		t.Fatal(err)
	}
	api, err := server.New(server.Config{
		Runtime: runtime, Sessions: core.NewMemorySessionStore(), Releases: releases, Evaluations: store,
		Authenticator: server.AuthenticatorFunc(func(*http.Request) (core.Principal, error) { return principal, nil }),
		RunPrincipalResolver: server.RunPrincipalResolverFunc(func(_ context.Context, tenantID, subjectID string) (core.Principal, error) {
			if tenantID == principal.TenantID && subjectID == principal.SubjectID {
				return principal, nil
			}
			return core.Principal{}, fmt.Errorf("unknown evaluation principal")
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	return newTestHTTPServer(t, api.Handler()), profiles, principal
}

func strictGateDataset(failingCase bool) evaluation.Dataset {
	secondExpected := "candidate"
	if failingCase {
		secondExpected = "missing"
	}
	return evaluation.Dataset{
		ID: "strict.gate", Version: 1, Name: "Strict Gate", ProfileID: "strict.agent", PassThreshold: 0.5,
		Cases: []evaluation.Case{
			{ID: "case-one", Input: "first", Assertions: []evaluation.Assertion{{ID: "answer", Kind: evaluation.AssertAnswerContains, Expected: "candidate"}}},
			{ID: "case-two", Input: "second", Assertions: []evaluation.Assertion{{ID: "answer", Kind: evaluation.AssertAnswerContains, Expected: secondExpected}}},
		},
	}
}

func strictGatePublish(t *testing.T, httpServer *httptest.Server, gate string) *http.Response {
	t.Helper()
	return doJSON(t, http.MethodPost, httpServer.URL+"/v1/profiles/strict.agent/publish", "alice", `{
		"scope":[{"kind":"global","id":"global"},{"kind":"product","id":"product"},{"kind":"tenant","id":"acme"}],
		"layer":{"profile_id":"strict.agent","model":{"provider":"strict-gate","model":"candidate"}},
		"evaluation_gate":`+gate+`
	}`)
}

func TestReleaseStrictGatePreservesAverageCompatibilityAndRequiresEveryCase(t *testing.T) {
	t.Run("default retains average threshold semantics", func(t *testing.T) {
		httpServer, profiles, principal := newStrictGateServer(t, strictGateDataset(true), evaluation.NewMemoryStore())
		response := strictGatePublish(t, httpServer, `{"dataset_id":"strict.gate","dataset_version":1,"subject_id":"alice"}`)
		if response.StatusCode != http.StatusCreated {
			t.Fatalf("average-compatible release status=%d body=%s", response.StatusCode, readBody(t, response))
		}
		live, err := profiles.Resolve(principal, principal.Scope, "strict.agent")
		if err != nil || live.Model.Model != "candidate" {
			t.Fatalf("average-compatible release was not published: %#v err=%v", live, err)
		}
	})

	t.Run("strict rejects one failed case without publishing", func(t *testing.T) {
		httpServer, profiles, principal := newStrictGateServer(t, strictGateDataset(true), evaluation.NewMemoryStore())
		response := strictGatePublish(t, httpServer, `{"dataset_id":"strict.gate","dataset_version":1,"subject_id":"alice","require_all_cases":true}`)
		body := readBody(t, response)
		if response.StatusCode != http.StatusConflict || !strings.Contains(body, `"passed":false`) {
			t.Fatalf("strict failure status=%d body=%s", response.StatusCode, body)
		}
		live, err := profiles.Resolve(principal, principal.Scope, "strict.agent")
		if err != nil || live.Model.Model != "stable" {
			t.Fatalf("strict rejection polluted live profile: %#v err=%v", live, err)
		}
	})

	t.Run("strict publishes when every weighted case passes", func(t *testing.T) {
		httpServer, profiles, principal := newStrictGateServer(t, strictGateDataset(false), evaluation.NewMemoryStore())
		response := strictGatePublish(t, httpServer, `{"dataset_id":"strict.gate","dataset_version":1,"subject_id":"alice","require_all_cases":true}`)
		if response.StatusCode != http.StatusCreated {
			t.Fatalf("strict passing release status=%d body=%s", response.StatusCode, readBody(t, response))
		}
		live, err := profiles.Resolve(principal, principal.Scope, "strict.agent")
		if err != nil || live.Model.Model != "candidate" {
			t.Fatalf("strict passing release was not published: %#v err=%v", live, err)
		}
	})
}

func TestReleaseStrictRegressionRejectsBaselineMissingFrozenCase(t *testing.T) {
	backing := evaluation.NewMemoryStore()
	dataset := strictGateDataset(false)
	if err := evaluation.ValidateDataset(&dataset); err != nil {
		t.Fatal(err)
	}
	store := strictGateStore{
		Store: backing, baselineID: "eval-strict-missing-baseline",
		baseline: evaluation.RunResult{
			ID: "eval-strict-missing-baseline", DatasetID: dataset.ID, DatasetVersion: dataset.Version, DatasetRevision: dataset.Revision,
			TenantID: "acme", SubjectID: "alice", ProfileID: "strict.agent", Status: evaluation.RunCompleted,
			Score: 1, Passed: true, TotalCases: 2, PassedCases: 1, Cases: []evaluation.CaseResult{{CaseID: "case-one", Passed: true}},
		},
	}
	httpServer, profiles, principal := newStrictGateServer(t, dataset, store)
	response := strictGatePublish(t, httpServer, `{"dataset_id":"strict.gate","dataset_version":1,"subject_id":"alice","baseline_run_id":"eval-strict-missing-baseline","max_regression":0,"require_all_cases":true}`)
	body := readBody(t, response)
	if response.StatusCode != http.StatusBadRequest || !strings.Contains(body, "baseline cases do not match frozen dataset") {
		t.Fatalf("missing baseline case status=%d body=%s", response.StatusCode, body)
	}
	live, err := profiles.Resolve(principal, principal.Scope, "strict.agent")
	if err != nil || live.Model.Model != "stable" {
		t.Fatalf("baseline rejection polluted live profile: %#v err=%v", live, err)
	}
}
