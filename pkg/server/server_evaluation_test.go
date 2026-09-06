package server

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/evaluation"
	"github.com/cc-auto-agent/harness-core/pkg/storage"

	_ "modernc.org/sqlite"
)

type evaluationServerModel struct{ calls *atomic.Int32 }

func (evaluationServerModel) Provider() string         { return "evaluation-server" }
func (evaluationServerModel) ArtifactRevision() string { return "evaluation-server/v1" }
func (m evaluationServerModel) Stream(_ context.Context, _ core.GenerateOptions, emit func(core.StreamChunk)) error {
	if m.calls != nil {
		m.calls.Add(1)
	}
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: "safe evaluation answer"})
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
	return nil
}

type evaluationServerFixture struct {
	server        *Server
	evaluations   *storage.SQLEvaluationStore
	target        core.Principal
	tenantAdmin   core.Principal
	platformAdmin core.Principal
	otherAdmin    core.Principal
	modelCalls    atomic.Int32
}

func newEvaluationServerFixture(t *testing.T) *evaluationServerFixture {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "evaluation.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	sessions, err := storage.OpenSQLSessionStore(context.Background(), db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	evaluations, err := storage.NewSQLEvaluationStore(db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product, _ := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "product"})
	tenant, _ := product.Child(core.ScopeRef{Kind: core.ScopeTenant, ID: "acme"})
	user, _ := tenant.Child(core.ScopeRef{Kind: core.ScopeUser, ID: "alice@acme.test"})
	otherTenant, _ := product.Child(core.ScopeRef{Kind: core.ScopeTenant, ID: "other"})
	target := core.Principal{
		SubjectID: "alice@acme.test", TenantID: "acme", Scope: user,
		Grants:     core.NewPermissionSet(core.PermRead, core.PermWrite),
		Attributes: map[string]string{"role": storage.RoleAccountUser},
	}
	tenantAdmin := core.Principal{
		SubjectID: "admin@acme.test", TenantID: "acme", Scope: tenant,
		Grants:     core.NewPermissionSet(core.PermRead, core.PermWrite),
		Attributes: map[string]string{"role": storage.RoleAccountTenantAdmin},
	}
	platformAdmin := core.Principal{
		SubjectID: "platform@test", TenantID: "system", Scope: global,
		Grants:     core.NewPermissionSet(core.PermRead, core.PermWrite, core.PermSend),
		Attributes: map[string]string{"role": storage.RoleAccountAdmin},
	}
	otherAdmin := core.Principal{
		SubjectID: "admin@other.test", TenantID: "other", Scope: otherTenant,
		Grants:     core.NewPermissionSet(core.PermRead, core.PermWrite),
		Attributes: map[string]string{"role": storage.RoleAccountTenantAdmin},
	}
	profiles := core.NewAgentProfileRegistry()
	name := "Evaluation Server Agent"
	model := core.ModelSelection{Provider: "evaluation-server", Model: "evaluation-server"}
	if err := profiles.Bind(core.AgentProfileLayer{
		Scope: product, ProfileID: "evaluation.agent", Name: &name, Model: &model,
	}); err != nil {
		t.Fatal(err)
	}
	fixture := &evaluationServerFixture{
		evaluations: evaluations, target: target, tenantAdmin: tenantAdmin,
		platformAdmin: platformAdmin, otherAdmin: otherAdmin,
	}
	runtime := &core.Runtime{
		Capabilities: core.NewCapabilityRegistry(), Profiles: profiles,
		Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) {
			return evaluationServerModel{calls: &fixture.modelCalls}, nil
		}),
	}
	server, err := New(Config{
		Runtime: runtime, Sessions: sessions, Evaluations: evaluations,
		Authenticator: AuthenticatorFunc(func(r *http.Request) (core.Principal, error) {
			switch r.Header.Get("X-Test-Role") {
			case "platform":
				return platformAdmin, nil
			case "other":
				return otherAdmin, nil
			case "user":
				return target, nil
			default:
				return tenantAdmin, nil
			}
		}),
		RunPrincipalResolver: RunPrincipalResolverFunc(func(_ context.Context, tenantID, subjectID string) (core.Principal, error) {
			if tenantID == target.TenantID && subjectID == target.SubjectID {
				return target, nil
			}
			return core.Principal{}, context.Canceled
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	fixture.server = server
	return fixture
}

type failEvaluationCaseOnce struct {
	evaluation.Store
	fail atomic.Bool
}

func (s *failEvaluationCaseOnce) RecordCaseResult(ctx context.Context, runID string, result evaluation.CaseResult) error {
	if s.fail.CompareAndSwap(true, false) {
		return context.DeadlineExceeded
	}
	return s.Store.RecordCaseResult(ctx, runID, result)
}

func (f *evaluationServerFixture) request(t *testing.T, method, path, role string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var payload []byte
	var err error
	if body != nil {
		payload, err = json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
	}
	request := httptest.NewRequest(method, path, bytes.NewReader(payload))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Test-Role", role)
	response := httptest.NewRecorder()
	f.server.Handler().ServeHTTP(response, request)
	return response
}

func evaluationAPIDataset() evaluation.Dataset {
	return evaluation.Dataset{
		ID: "evaluation.api", Version: 1, Name: "API Dataset", ProfileID: "evaluation.agent",
		Cases: []evaluation.Case{{
			ID: "case-api", Input: "answer safely",
			Assertions: []evaluation.Assertion{
				{ID: "status", Kind: evaluation.AssertRunStatus, ExpectedStatus: core.RunCompleted},
				{ID: "answer", Kind: evaluation.AssertAnswerContains, Expected: "safe evaluation"},
			},
		}},
	}
}

func TestEvaluationAdminAPIAndTenantIsolation(t *testing.T) {
	fixture := newEvaluationServerFixture(t)
	dataset := evaluationAPIDataset()
	forbidden := fixture.request(t, http.MethodPost, "/v1/admin/evaluations/datasets", "tenant", dataset)
	if forbidden.Code != http.StatusForbidden {
		t.Fatalf("tenant dataset create status=%d body=%s", forbidden.Code, forbidden.Body.String())
	}
	created := fixture.request(t, http.MethodPost, "/v1/admin/evaluations/datasets", "platform", dataset)
	if created.Code != http.StatusCreated {
		t.Fatalf("dataset create status=%d body=%s", created.Code, created.Body.String())
	}
	replayed := fixture.request(t, http.MethodPost, "/v1/admin/evaluations/datasets", "platform", dataset)
	if replayed.Code != http.StatusOK {
		t.Fatalf("dataset replay status=%d body=%s", replayed.Code, replayed.Body.String())
	}
	runResponse := fixture.request(t, http.MethodPost, "/v1/admin/evaluations/runs", "tenant", map[string]any{
		"dataset_id": dataset.ID, "dataset_version": dataset.Version,
		"subject_id": fixture.target.SubjectID,
		"composition_metadata": map[string]string{
			"harness.assignment.id":        "evaluation-api-assignment",
			"harness.assignment.candidate": "true",
		},
	})
	if runResponse.Code != http.StatusOK {
		t.Fatalf("evaluation run status=%d body=%s", runResponse.Code, runResponse.Body.String())
	}
	var run evaluation.RunResult
	if err := json.Unmarshal(runResponse.Body.Bytes(), &run); err != nil {
		t.Fatal(err)
	}
	if !run.Passed || run.Score != 1 || run.TenantID != "acme" || len(run.Cases) != 1 ||
		run.CompositionMetadata["harness.assignment.id"] != "evaluation-api-assignment" ||
		run.Cases[0].Artifacts.CompositionRevision == "" || run.Cases[0].Artifacts.AssignmentRevision == "" {
		t.Fatalf("evaluation run wrong: %#v", run)
	}
	queryPath := "/v1/admin/evaluations/runs?assignment_revision=" + run.Cases[0].Artifacts.AssignmentRevision
	filtered := fixture.request(t, http.MethodGet, queryPath, "tenant", nil)
	if filtered.Code != http.StatusOK {
		t.Fatalf("revision query status=%d body=%s", filtered.Code, filtered.Body.String())
	}
	var filteredResult struct {
		Runs []evaluation.RunResult `json:"runs"`
	}
	if err := json.Unmarshal(filtered.Body.Bytes(), &filteredResult); err != nil {
		t.Fatal(err)
	}
	if len(filteredResult.Runs) != 1 || filteredResult.Runs[0].ID != run.ID {
		t.Fatalf("assignment revision query wrong: %#v", filteredResult)
	}
	invalidQuery := fixture.request(t, http.MethodGet, "/v1/admin/evaluations/runs?composition_revision=invalid", "tenant", nil)
	if invalidQuery.Code != http.StatusBadRequest {
		t.Fatalf("invalid revision query status=%d body=%s", invalidQuery.Code, invalidQuery.Body.String())
	}
	get := fixture.request(t, http.MethodGet, "/v1/admin/evaluations/runs/"+run.ID, "tenant", nil)
	if get.Code != http.StatusOK {
		t.Fatalf("evaluation get status=%d body=%s", get.Code, get.Body.String())
	}
	other := fixture.request(t, http.MethodGet, "/v1/admin/evaluations/runs/"+run.ID, "other", nil)
	if other.Code != http.StatusNotFound {
		t.Fatalf("cross-tenant evaluation status=%d body=%s", other.Code, other.Body.String())
	}
	unsafe := fixture.request(t, http.MethodPost, "/v1/admin/evaluations/runs", "tenant", map[string]any{
		"dataset_id": dataset.ID, "dataset_version": dataset.Version,
		"subject_id": fixture.target.SubjectID, "allow_capabilities": []string{"danger.execute"},
	})
	if unsafe.Code != http.StatusForbidden {
		t.Fatalf("tenant unsafe allow status=%d body=%s", unsafe.Code, unsafe.Body.String())
	}
}

func TestEvaluationGateAPI(t *testing.T) {
	fixture := newEvaluationServerFixture(t)
	dataset := evaluationAPIDataset()
	if response := fixture.request(t, http.MethodPost, "/v1/admin/evaluations/datasets", "platform", dataset); response.Code != http.StatusCreated {
		t.Fatalf("dataset create status=%d body=%s", response.Code, response.Body.String())
	}
	runEval := func(baseline string) evaluation.RunResult {
		body := map[string]any{
			"dataset_id": dataset.ID, "dataset_version": dataset.Version,
			"tenant_id": "acme", "subject_id": fixture.target.SubjectID,
		}
		if baseline != "" {
			body["baseline_run_id"] = baseline
		}
		response := fixture.request(t, http.MethodPost, "/v1/admin/evaluations/runs", "platform", body)
		if response.Code != http.StatusOK {
			t.Fatalf("evaluation run status=%d body=%s", response.Code, response.Body.String())
		}
		var run evaluation.RunResult
		if err := json.Unmarshal(response.Body.Bytes(), &run); err != nil {
			t.Fatal(err)
		}
		return run
	}
	baseline := runEval("")
	current := runEval(baseline.ID)
	minimum, tolerance := 0.9, 0.0
	gate := fixture.request(t, http.MethodPost, "/v1/admin/evaluations/runs/"+current.ID+"/gate", "tenant", map[string]any{
		"require_passed": true, "min_score": minimum, "max_regression": tolerance,
	})
	if gate.Code != http.StatusOK {
		t.Fatalf("gate status=%d body=%s", gate.Code, gate.Body.String())
	}
	var result evaluation.GateResult
	if err := json.Unmarshal(gate.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if !result.Passed || result.Comparison == nil || result.Comparison.Regressed {
		t.Fatalf("gate result wrong: %#v", result)
	}
}

func TestEvaluationResumeAPIRecoversMissingCaseResultWithoutModelReplay(t *testing.T) {
	fixture := newEvaluationServerFixture(t)
	dataset := evaluationAPIDataset()
	if response := fixture.request(t, http.MethodPost, "/v1/admin/evaluations/datasets", "platform", dataset); response.Code != http.StatusCreated {
		t.Fatalf("dataset create status=%d body=%s", response.Code, response.Body.String())
	}
	flaky := &failEvaluationCaseOnce{Store: fixture.evaluations}
	flaky.fail.Store(true)
	fixture.server.evaluations = flaky
	fixture.server.evaluationRunner.Store = flaky
	first := fixture.request(t, http.MethodPost, "/v1/admin/evaluations/runs", "tenant", map[string]any{
		"dataset_id": dataset.ID, "dataset_version": dataset.Version,
		"subject_id": fixture.target.SubjectID,
	})
	if first.Code != http.StatusInternalServerError {
		t.Fatalf("interrupted evaluation status=%d body=%s", first.Code, first.Body.String())
	}
	var interrupted struct {
		Run evaluation.RunResult `json:"run"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &interrupted); err != nil {
		t.Fatal(err)
	}
	if interrupted.Run.ID == "" || interrupted.Run.Status != evaluation.RunRunning || fixture.modelCalls.Load() != 1 {
		t.Fatalf("interrupted run wrong: %#v model_calls=%d", interrupted.Run, fixture.modelCalls.Load())
	}
	other := fixture.request(t, http.MethodPost, "/v1/admin/evaluations/runs/"+interrupted.Run.ID+"/resume", "other", nil)
	if other.Code != http.StatusNotFound {
		t.Fatalf("cross-tenant resume status=%d body=%s", other.Code, other.Body.String())
	}
	resumedResponse := fixture.request(t, http.MethodPost, "/v1/admin/evaluations/runs/"+interrupted.Run.ID+"/resume", "tenant", nil)
	if resumedResponse.Code != http.StatusOK {
		t.Fatalf("resume status=%d body=%s", resumedResponse.Code, resumedResponse.Body.String())
	}
	var resumed evaluation.RunResult
	if err := json.Unmarshal(resumedResponse.Body.Bytes(), &resumed); err != nil {
		t.Fatal(err)
	}
	if resumed.Status != evaluation.RunCompleted || !resumed.Passed || len(resumed.Cases) != 1 || fixture.modelCalls.Load() != 1 {
		t.Fatalf("resumed run wrong: %#v model_calls=%d", resumed, fixture.modelCalls.Load())
	}
}
