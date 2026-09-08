package server_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"github.com/whhhh1500/auto-agent/pkg/control"
	"github.com/whhhh1500/auto-agent/pkg/evaluation"
	"github.com/whhhh1500/auto-agent/pkg/storage"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	_ "modernc.org/sqlite"

	cryptoexample "github.com/whhhh1500/auto-agent/examples/crypto"
	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/server"
)

type releaseAuditStore struct {
	mu     sync.Mutex
	events []storage.AuditEvent
}

func (s *releaseAuditStore) RecordAudit(_ context.Context, event storage.AuditEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
	return nil
}

func (s *releaseAuditStore) ListAudit(context.Context, storage.AuditFilter) ([]storage.AuditEvent, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]storage.AuditEvent(nil), s.events...)
	return out, len(out), nil
}

func (s *releaseAuditStore) last(t *testing.T) storage.AuditEvent {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.events) == 0 {
		t.Fatal("expected an audit event")
	}
	return s.events[len(s.events)-1]
}

type releaseGateModel struct{ text string }

func (releaseGateModel) Provider() string { return "release-gate" }
func (m releaseGateModel) Stream(_ context.Context, _ core.GenerateOptions, emit func(core.StreamChunk)) error {
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: m.text})
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
	return nil
}

type releaseCompatibilityTool struct{}

func (releaseCompatibilityTool) Manifest() core.CapabilityManifest {
	return core.CapabilityManifest{
		ID: "release.lookup", Version: "1.0.0", Name: "Release Lookup", Kind: core.KindTool,
		Contract: "harness.tool/v1", RequiredPermissions: []core.Permission{core.PermSend}, Idempotent: true,
		Tool: &core.ToolExposure{Parameters: map[string]any{
			"type": "object", "properties": map[string]any{
				"query": map[string]any{"type": "string"},
			}, "required": []any{"query"}, "additionalProperties": false,
		}},
	}
}

func (releaseCompatibilityTool) Execute(context.Context, core.CapabilityRequest) (core.CapabilityResult, error) {
	return core.CapabilityResult{Content: "lookup", OK: true}, nil
}

func newGatedReleaseServer(t *testing.T) (*httptest.Server, *core.AgentProfileRegistry, *evaluation.MemoryStore, core.Principal, *releaseAuditStore) {
	t.Helper()
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product, _ := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "product"})
	tenant, _ := product.Child(core.ScopeRef{Kind: core.ScopeTenant, ID: "acme"})
	principal := core.Principal{
		SubjectID: "alice", TenantID: "acme", Scope: tenant,
		Grants:     core.NewPermissionSet(core.PermRead, core.PermWrite, core.PermSend),
		Attributes: map[string]string{"role": storage.RoleAccountTenantAdmin},
	}
	platform := core.Principal{
		SubjectID: "platform", TenantID: "system", Scope: global,
		Grants:     core.NewPermissionSet(core.PermRead, core.PermWrite, core.PermSend),
		Attributes: map[string]string{"role": storage.RoleAccountAdmin},
	}
	profiles := core.NewAgentProfileRegistry()
	policies := core.NewPolicyRegistry()
	if err := policies.Bind(core.PolicyLayer{Scope: tenant, DenyPermissions: []core.Permission{core.PermSend}}); err != nil {
		t.Fatal(err)
	}
	capabilities := core.NewCapabilityRegistry()
	if err := capabilities.Register(product, releaseCompatibilityTool{}); err != nil {
		t.Fatal(err)
	}
	name := "Release Agent"
	stableModel := core.ModelSelection{Provider: "release-gate", Model: "stable"}
	if err := profiles.Bind(core.AgentProfileLayer{
		Scope: product, ProfileID: "release.agent", Name: &name, Model: &stableModel,
		AddCapabilities: []string{"release.lookup"},
	}); err != nil {
		t.Fatal(err)
	}
	runtime := &core.Runtime{
		Capabilities: capabilities, Profiles: profiles, Policy: policies,
		Models: core.ModelResolverFunc(func(_ context.Context, selection core.ModelSelection) (core.LlmAdapter, error) {
			return releaseGateModel{text: selection.Model}, nil
		}),
	}
	releases, _ := control.NewReleaseManager(profiles)
	evaluations := evaluation.NewMemoryStore()
	audit := &releaseAuditStore{}
	if _, _, err := evaluations.PutDataset(context.Background(), evaluation.Dataset{
		ID: "release.gate", Version: 1, Name: "Release Gate", ProfileID: "release.agent",
		Cases: []evaluation.Case{{
			ID: "case-release", Input: "evaluate release",
			Assertions: []evaluation.Assertion{
				{ID: "status", Kind: evaluation.AssertRunStatus, ExpectedStatus: core.RunCompleted},
				{ID: "answer", Kind: evaluation.AssertAnswerContains, Expected: "candidate"},
			},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	api, err := server.New(server.Config{
		Runtime: runtime, Sessions: core.NewMemorySessionStore(), Releases: releases, Evaluations: evaluations, Audit: audit,
		Authenticator: server.AuthenticatorFunc(func(r *http.Request) (core.Principal, error) {
			if r.Header.Get("X-Harness-Subject") == "platform" {
				return platform, nil
			}
			return principal, nil
		}),
		RunPrincipalResolver: server.RunPrincipalResolverFunc(func(_ context.Context, tenantID, subjectID string) (core.Principal, error) {
			if tenantID == principal.TenantID && subjectID == principal.SubjectID {
				return principal, nil
			}
			return core.Principal{}, context.Canceled
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	return newTestHTTPServer(t, api.Handler()), profiles, evaluations, principal, audit
}

func newGatedCanaryServer(t *testing.T) (*httptest.Server, *core.AgentProfileRegistry, evaluation.Store, *releaseAuditStore) {
	t.Helper()
	db, err := sql.Open("sqlite", t.TempDir()+"/canary.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	sessions, err := storage.OpenSQLSessionStore(context.Background(), db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product, _ := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "product"})
	tenant, _ := product.Child(core.ScopeRef{Kind: core.ScopeTenant, ID: "acme"})
	principal := core.Principal{
		SubjectID: "alice", TenantID: "acme", Scope: tenant,
		Grants:     core.NewPermissionSet(core.PermRead, core.PermWrite, core.PermSend),
		Attributes: map[string]string{"role": storage.RoleAccountTenantAdmin},
	}
	platform := core.Principal{
		SubjectID: "platform", TenantID: "system", Scope: global,
		Grants:     core.NewPermissionSet(core.PermRead, core.PermWrite, core.PermSend),
		Attributes: map[string]string{"role": storage.RoleAccountAdmin},
	}
	profiles := core.NewAgentProfileRegistry()
	policies := core.NewPolicyRegistry()
	if err := policies.Bind(core.PolicyLayer{Scope: tenant, DenyPermissions: []core.Permission{core.PermSend}}); err != nil {
		t.Fatal(err)
	}
	capabilities := core.NewCapabilityRegistry()
	if err := capabilities.Register(product, releaseCompatibilityTool{}); err != nil {
		t.Fatal(err)
	}
	name := "Canary Agent"
	stableModel := core.ModelSelection{Provider: "release-gate", Model: "stable"}
	if err := profiles.Bind(core.AgentProfileLayer{
		Scope: product, ProfileID: "release.agent", Name: &name, Model: &stableModel,
		AddCapabilities: []string{"release.lookup"},
	}); err != nil {
		t.Fatal(err)
	}
	runtime := &core.Runtime{
		Capabilities: capabilities, Profiles: profiles, Policy: policies,
		Models: core.ModelResolverFunc(func(_ context.Context, selection core.ModelSelection) (core.LlmAdapter, error) {
			return releaseGateModel{text: selection.Model}, nil
		}),
	}
	releases, _ := control.NewReleaseManager(profiles)
	releases.Journal = sessions
	if err := releases.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	canaryStore, err := storage.NewSQLCanaryStore(db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	canaries, err := control.NewCanaryManager(releases, canaryStore)
	if err != nil {
		t.Fatal(err)
	}
	if err := canaries.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	evaluations, err := storage.NewSQLEvaluationStore(db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	audit := &releaseAuditStore{}
	if _, _, err := evaluations.PutDataset(context.Background(), evaluation.Dataset{
		ID: "release.gate", Version: 1, Name: "Release Gate", ProfileID: "release.agent",
		Cases: []evaluation.Case{{
			ID: "case-release", Input: "evaluate release",
			Assertions: []evaluation.Assertion{
				{ID: "status", Kind: evaluation.AssertRunStatus, ExpectedStatus: core.RunCompleted},
				{ID: "answer", Kind: evaluation.AssertAnswerContains, Expected: "candidate"},
			},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	api, err := server.New(server.Config{
		Runtime: runtime, Sessions: sessions, Releases: releases, Canaries: canaries, Evaluations: evaluations, Audit: audit,
		Authenticator: server.AuthenticatorFunc(func(r *http.Request) (core.Principal, error) {
			if r.Header.Get("X-Harness-Subject") == "platform" {
				return platform, nil
			}
			return principal, nil
		}),
		RunPrincipalResolver: server.RunPrincipalResolverFunc(func(_ context.Context, tenantID, subjectID string) (core.Principal, error) {
			if tenantID == principal.TenantID && subjectID == principal.SubjectID {
				return principal, nil
			}
			return core.Principal{}, context.Canceled
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	return newTestHTTPServer(t, api.Handler()), profiles, evaluations, audit
}

// openListerDB provides the SQLite handle for the listing test.
func openListerDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", t.TempDir()+"/sessions.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func newReleaseServer(t *testing.T) (*httptest.Server, *core.AgentProfileRegistry) {
	t.Helper()
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product, _ := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "crypto"})
	capabilities := core.NewCapabilityRegistry()
	profiles := core.NewAgentProfileRegistry()
	if err := cryptoexample.RegisterProductCapabilities(capabilities, product); err != nil {
		t.Fatal(err)
	}
	if err := cryptoexample.RegisterProfiles(profiles, product); err != nil {
		t.Fatal(err)
	}
	releases, err := control.NewReleaseManager(profiles)
	if err != nil {
		t.Fatal(err)
	}
	runtime := &core.Runtime{
		Capabilities: capabilities, Profiles: profiles,
		Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) {
			return core.MockLlmAdapter{}, nil
		}),
	}
	api, err := server.New(server.Config{
		Runtime: runtime, Sessions: core.NewMemorySessionStore(),
		Authenticator: tenantOperatorAuthenticator(product.Segments()),
		Releases:      releases,
	})
	if err != nil {
		t.Fatal(err)
	}
	return newTestHTTPServer(t, api.Handler()), profiles
}

func TestPublishRollbackRoutesOwnScopeCheck(t *testing.T) {
	httpServer, _ := newReleaseServer(t)
	base := httpServer.URL

	// Publish a tenant-scoped persona layer for an existing profile.
	publish := doJSON(t, http.MethodPost, base+"/v1/profiles/crypto.agent.analyst/publish", "alice", `{
		"scope": [
			{"kind":"global","id":"global"},{"kind":"product","id":"crypto"},
			{"kind":"tenant","id":"acme"}
		],
		"layer": {
			"profile_id": "crypto.agent.analyst",
			"put_fragments": [
				{"id": "tenant.brand", "section": "style", "content": "Brand voice: concise Chinese."}
			]
		}
	}`)
	if publish.StatusCode != http.StatusCreated {
		t.Fatalf("publish status=%d body=%s", publish.StatusCode, readBody(t, publish))
	}

	// Cross-tenant publish is forbidden: bob's principal sits in another
	// tenant, so the acme scope is not on his chain.
	forbidden := doJSONWithHeaders(t, http.MethodPost, base+"/v1/profiles/crypto.agent.analyst/publish", "bob",
		map[string]string{"Content-Type": "application/json", "X-Harness-Tenant": "otherco"}, `{
		"scope": [
			{"kind":"global","id":"global"},{"kind":"product","id":"crypto"},
			{"kind":"tenant","id":"acme"}
		],
		"layer": {"profile_id": "crypto.agent.analyst"}
	}`)
	if forbidden.StatusCode != http.StatusForbidden {
		t.Fatalf("cross-scope publish status=%d body=%s", forbidden.StatusCode, readBody(t, forbidden))
	}

	// History shows one live release.
	history := doJSON(t, http.MethodGet, base+"/v1/profiles/crypto.agent.analyst/releases", "alice", "")
	historyBody := readBody(t, history)
	if !strings.Contains(historyBody, `"version":1`) || strings.Contains(historyBody, `"rolled_back":true`) {
		t.Fatalf("unexpected history: %s", historyBody)
	}

	// Rollback to zero unmounts the release.
	rollback := doJSON(t, http.MethodPost, base+"/v1/profiles/crypto.agent.analyst/rollback", "alice", `{"to_version": 0}`)
	if rollback.StatusCode != http.StatusOK || !strings.Contains(readBody(t, rollback), `"version":1`) {
		t.Fatalf("rollback failed: status=%d body=%s", rollback.StatusCode, readBody(t, rollback))
	}
	historyBody = readBody(t, doJSON(t, http.MethodGet, base+"/v1/profiles/crypto.agent.analyst/releases", "alice", ""))
	if !strings.Contains(historyBody, `"rolled_back":true`) {
		t.Fatalf("history does not record the rollback: %s", historyBody)
	}
}

func TestPublishRejectsMismatchedProfileID(t *testing.T) {
	httpServer, _ := newReleaseServer(t)
	response := doJSON(t, http.MethodPost, httpServer.URL+"/v1/profiles/crypto.agent.analyst/publish", "alice", `{
		"scope":[{"kind":"global","id":"global"},{"kind":"product","id":"crypto"},{"kind":"tenant","id":"acme"}],
		"layer":{"profile_id":"other.agent"}
	}`)
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("mismatched profile id status=%d body=%s", response.StatusCode, readBody(t, response))
	}
}

func TestReleaseEvaluationGateFailureDoesNotPolluteLiveState(t *testing.T) {
	httpServer, profiles, evaluations, principal, _ := newGatedReleaseServer(t)
	response := doJSON(t, http.MethodPost, httpServer.URL+"/v1/profiles/release.agent/publish", "alice", `{
		"scope":[{"kind":"global","id":"global"},{"kind":"product","id":"product"},{"kind":"tenant","id":"acme"}],
		"layer":{"profile_id":"release.agent","model":{"provider":"release-gate","model":"bad"}},
		"evaluation_gate":{"dataset_id":"release.gate","dataset_version":1,"subject_id":"alice"}
	}`)
	body := readBody(t, response)
	if response.StatusCode != http.StatusConflict || !strings.Contains(body, `"passed":false`) {
		t.Fatalf("failed gate status=%d body=%s", response.StatusCode, body)
	}
	live, err := profiles.Resolve(principal, principal.Scope, "release.agent")
	if err != nil || live.Model.Model != "stable" {
		t.Fatalf("failed candidate changed live profile: %#v err=%v", live, err)
	}
	history := readBody(t, doJSON(t, http.MethodGet, httpServer.URL+"/v1/profiles/release.agent/releases", "alice", ""))
	if strings.Contains(history, `"version":1`) {
		t.Fatalf("failed gate created release history: %s", history)
	}
	runs, err := evaluations.ListRuns(context.Background(), "release.gate", "acme", 10)
	if err != nil || len(runs) != 1 || runs[0].Status != evaluation.RunCompleted || runs[0].Passed {
		t.Fatalf("candidate evidence missing: %#v err=%v", runs, err)
	}
}

func TestReleaseEvaluationGatePublishesOnlyPassingCandidate(t *testing.T) {
	httpServer, profiles, _, principal, _ := newGatedReleaseServer(t)
	response := doJSON(t, http.MethodPost, httpServer.URL+"/v1/profiles/release.agent/publish", "alice", `{
		"scope":[{"kind":"global","id":"global"},{"kind":"product","id":"product"},{"kind":"tenant","id":"acme"}],
		"layer":{"profile_id":"release.agent","model":{"provider":"release-gate","model":"candidate"}},
		"evaluation_gate":{"dataset_id":"release.gate","dataset_version":1,"subject_id":"alice"}
	}`)
	body := readBody(t, response)
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("passing gate status=%d body=%s", response.StatusCode, body)
	}
	var result struct {
		Release struct {
			Version  int    `json:"version"`
			Revision string `json:"revision"`
		} `json:"release"`
		Evaluation struct {
			CandidateRevision string                `json:"candidate_revision"`
			CandidateRun      evaluation.RunResult  `json:"candidate_run"`
			Gate              evaluation.GateResult `json:"gate"`
		} `json:"evaluation"`
	}
	if err := json.Unmarshal([]byte(body), &result); err != nil {
		t.Fatal(err)
	}
	if result.Release.Version != 1 || result.Release.Revision == "" ||
		result.Release.Revision != result.Evaluation.CandidateRevision ||
		!result.Evaluation.Gate.Passed || !result.Evaluation.CandidateRun.Passed ||
		result.Evaluation.CandidateRun.CompositionMetadata["release.candidate_revision"] != result.Evaluation.CandidateRevision ||
		len(result.Evaluation.CandidateRun.Cases) != 1 ||
		result.Evaluation.CandidateRun.Cases[0].Artifacts.CompositionRevision == "" ||
		result.Evaluation.CandidateRun.Cases[0].Artifacts.AssignmentRevision == "" {
		t.Fatalf("gated publish response wrong: %#v", result)
	}
	live, err := profiles.Resolve(principal, principal.Scope, "release.agent")
	if err != nil || live.Model.Model != "candidate" {
		t.Fatalf("passing candidate was not published: %#v err=%v", live, err)
	}
}

func TestReleaseCompatibilityGateBlocksPassingCandidateThatRemovesCapability(t *testing.T) {
	httpServer, profiles, evaluations, principal, audit := newGatedReleaseServer(t)
	if !principal.Grants.Allows([]core.Permission{core.PermSend}) {
		t.Fatal("fixture must allow registry/profile composition before Policy removes release.lookup")
	}
	response := doJSON(t, http.MethodPost, httpServer.URL+"/v1/profiles/release.agent/publish", "alice", `{
		"scope":[{"kind":"global","id":"global"},{"kind":"product","id":"product"},{"kind":"tenant","id":"acme"}],
		"layer":{"profile_id":"release.agent","model":{"provider":"release-gate","model":"candidate"},"remove_capabilities":["release.lookup"]},
		"evaluation_gate":{"dataset_id":"release.gate","dataset_version":1,"subject_id":"alice"}
	}`)
	body := readBody(t, response)
	if response.StatusCode != http.StatusConflict || !strings.Contains(body, `"compatible":false`) ||
		!strings.Contains(body, `"code":"capability_removed"`) {
		t.Fatalf("compatibility gate status=%d body=%s", response.StatusCode, body)
	}
	live, err := profiles.Resolve(principal, principal.Scope, "release.agent")
	if err != nil || len(live.Capabilities) != 1 || live.Capabilities[0] != "release.lookup" || live.Model.Model != "stable" {
		t.Fatalf("incompatible candidate polluted live profile: %#v err=%v", live, err)
	}
	runs, err := evaluations.ListRuns(context.Background(), "release.gate", "acme", 10)
	if err != nil || len(runs) != 1 || !runs[0].Passed {
		t.Fatalf("numeric evaluation evidence should still pass: %#v err=%v", runs, err)
	}
	for _, key := range []string{
		"release.baseline_capability_snapshot", "release.candidate_capability_snapshot",
		"release.capability_compatibility_revision",
	} {
		if runs[0].Metadata[key] == "" {
			t.Fatalf("evaluation metadata lost compatibility evidence %s: %#v", key, runs[0].Metadata)
		}
	}
	auditEvent := audit.last(t)
	if auditEvent.Action != "profile.publish.rejected" {
		t.Fatalf("compatibility rejection audit action=%q", auditEvent.Action)
	}
	gate, ok := auditEvent.Detail["gate"].(evaluation.GateResult)
	if !ok || gate.CapabilityCompatibility == nil || gate.CapabilityCompatibility.Compatible {
		t.Fatalf("compatibility rejection audit lost gate evidence: %#v", auditEvent.Detail)
	}
	history := readBody(t, doJSON(t, http.MethodGet, httpServer.URL+"/v1/profiles/release.agent/releases", "alice", ""))
	if strings.Contains(history, `"version":1`) {
		t.Fatalf("compatibility failure created release history: %s", history)
	}
}

func TestPlatformAdminCanExplicitlyAllowBreakingCapabilityChange(t *testing.T) {
	httpServer, profiles, _, _, audit := newGatedReleaseServer(t)
	response := doJSON(t, http.MethodPost, httpServer.URL+"/v1/profiles/release.agent/publish", "platform", `{
		"scope":[{"kind":"global","id":"global"},{"kind":"product","id":"product"},{"kind":"tenant","id":"acme"}],
		"layer":{"profile_id":"release.agent","model":{"provider":"release-gate","model":"candidate"},"remove_capabilities":["release.lookup"]},
		"evaluation_gate":{"dataset_id":"release.gate","dataset_version":1,"tenant_id":"acme","subject_id":"alice","allow_breaking_capabilities":["release.lookup"]}
	}`)
	body := readBody(t, response)
	if response.StatusCode != http.StatusCreated || !strings.Contains(body, `"compatible":true`) ||
		!strings.Contains(body, `"allowed":true`) {
		t.Fatalf("explicit compatibility override status=%d body=%s", response.StatusCode, body)
	}
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product, _ := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "product"})
	tenant, _ := product.Child(core.ScopeRef{Kind: core.ScopeTenant, ID: "acme"})
	target := core.Principal{SubjectID: "alice", TenantID: "acme", Scope: tenant, Grants: core.NewPermissionSet(core.PermRead)}
	live, err := profiles.Resolve(target, tenant, "release.agent")
	if err != nil || len(live.Capabilities) != 0 || live.Model.Model != "candidate" {
		t.Fatalf("explicitly allowed candidate was not published: %#v err=%v", live, err)
	}
	auditEvent := audit.last(t)
	compatibility, ok := auditEvent.Detail["capability_compatibility"].(*evaluation.CapabilityCompatibilityResult)
	if auditEvent.Action != "profile.publish" || !ok || compatibility == nil || !compatibility.Compatible ||
		len(compatibility.Issues) != 1 || !compatibility.Issues[0].Allowed {
		t.Fatalf("successful override audit lost compatibility evidence: %#v", auditEvent)
	}
}

func TestTenantAdminCannotOverrideCapabilityCompatibility(t *testing.T) {
	httpServer, _, _, _, _ := newGatedReleaseServer(t)
	response := doJSON(t, http.MethodPost, httpServer.URL+"/v1/profiles/release.agent/publish", "alice", `{
		"scope":[{"kind":"global","id":"global"},{"kind":"product","id":"product"},{"kind":"tenant","id":"acme"}],
		"layer":{"profile_id":"release.agent","model":{"provider":"release-gate","model":"candidate"},"remove_capabilities":["release.lookup"]},
		"evaluation_gate":{"dataset_id":"release.gate","dataset_version":1,"subject_id":"alice","allow_breaking_capabilities":["release.lookup"]}
	}`)
	body := readBody(t, response)
	if response.StatusCode != http.StatusBadRequest || !strings.Contains(body, "only platform admins") {
		t.Fatalf("tenant compatibility override status=%d body=%s", response.StatusCode, body)
	}
}

func TestCanaryCompatibilityGateBlocksPassingCandidate(t *testing.T) {
	httpServer, profiles, evaluations, audit := newGatedCanaryServer(t)
	response := doJSON(t, http.MethodPost, httpServer.URL+"/v1/profiles/release.agent/canaries", "alice", `{
		"scope":[{"kind":"global","id":"global"},{"kind":"product","id":"product"},{"kind":"tenant","id":"acme"}],
		"layer":{"profile_id":"release.agent","model":{"provider":"release-gate","model":"candidate"},"remove_capabilities":["release.lookup"]},
		"basis_points":1000,
		"evaluation_gate":{"dataset_id":"release.gate","dataset_version":1,"subject_id":"alice"}
	}`)
	body := readBody(t, response)
	if response.StatusCode != http.StatusConflict || !strings.Contains(body, `"compatible":false`) ||
		!strings.Contains(body, `"capability_removed"`) {
		t.Fatalf("canary compatibility gate status=%d body=%s", response.StatusCode, body)
	}
	listed := readBody(t, doJSON(t, http.MethodGet, httpServer.URL+"/v1/profiles/release.agent/canaries", "alice", ""))
	if !strings.Contains(listed, `"canaries":[]`) {
		t.Fatalf("incompatible canary was staged: %s", listed)
	}
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product, _ := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "product"})
	tenant, _ := product.Child(core.ScopeRef{Kind: core.ScopeTenant, ID: "acme"})
	principal := core.Principal{SubjectID: "alice", TenantID: "acme", Scope: tenant, Grants: core.NewPermissionSet(core.PermRead)}
	live, err := profiles.Resolve(principal, tenant, "release.agent")
	if err != nil || len(live.Capabilities) != 1 || live.Capabilities[0] != "release.lookup" {
		t.Fatalf("incompatible canary changed live profile: %#v err=%v", live, err)
	}
	runs, err := evaluations.ListRuns(context.Background(), "release.gate", "acme", 10)
	if err != nil || len(runs) != 1 || !runs[0].Passed {
		t.Fatalf("canary evaluation evidence missing: %#v err=%v", runs, err)
	}
	auditEvent := audit.last(t)
	if auditEvent.Action != "profile.canary.rejected" {
		t.Fatalf("canary compatibility rejection audit action=%q detail=%#v", auditEvent.Action, auditEvent.Detail)
	}
}

func TestCanaryStageRoutesPauseResumeAndPromote(t *testing.T) {
	httpServer, profiles, _, _ := newGatedCanaryServer(t)
	stage := doJSON(t, http.MethodPost, httpServer.URL+"/v1/profiles/release.agent/canaries", "alice", `{
		"scope":[{"kind":"global","id":"global"},{"kind":"product","id":"product"},{"kind":"tenant","id":"acme"}],
		"layer":{"profile_id":"release.agent","model":{"provider":"release-gate","model":"candidate"}},
		"basis_points":10000,
		"evaluation_gate":{"dataset_id":"release.gate","dataset_version":1,"subject_id":"alice"}
	}`)
	stageBody := readBody(t, stage)
	if stage.StatusCode != http.StatusCreated {
		t.Fatalf("stage status=%d body=%s", stage.StatusCode, stageBody)
	}
	var staged struct {
		Canary control.CanaryRecord `json:"canary"`
	}
	if err := json.Unmarshal([]byte(stageBody), &staged); err != nil || staged.Canary.ID == "" || staged.Canary.Status != control.CanaryActive {
		t.Fatalf("stage response wrong: %#v err=%v", staged, err)
	}
	principal := core.Principal{SubjectID: "alice", TenantID: "acme", Scope: staged.Canary.Scope, Grants: core.NewPermissionSet(core.PermRead)}
	live, err := profiles.Resolve(principal, principal.Scope, "release.agent")
	if err != nil || live.Model.Model != "stable" {
		t.Fatalf("staged canary polluted live profile: %#v err=%v", live, err)
	}
	blockedPublish := doJSON(t, http.MethodPost, httpServer.URL+"/v1/profiles/release.agent/publish", "alice", `{
		"scope":[{"kind":"global","id":"global"},{"kind":"product","id":"product"},{"kind":"tenant","id":"acme"}],
		"layer":{"profile_id":"release.agent","model":{"provider":"release-gate","model":"unexpected"}}
	}`)
	blockedBody := readBody(t, blockedPublish)
	if blockedPublish.StatusCode != http.StatusConflict || !strings.Contains(blockedBody, "reserved") {
		t.Fatalf("open canary did not block overlapping publish: status=%d body=%s", blockedPublish.StatusCode, blockedBody)
	}

	run := func() (*http.Response, string) {
		created := doJSON(t, http.MethodPost, httpServer.URL+"/v1/sessions", "alice", `{"profile_id":"release.agent"}`)
		body := readBody(t, created)
		if created.StatusCode != http.StatusCreated {
			t.Fatalf("create session status=%d body=%s", created.StatusCode, body)
		}
		var session struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal([]byte(body), &session); err != nil || session.ID == "" {
			t.Fatalf("decode session: %#v err=%v", session, err)
		}
		response := doJSON(t, http.MethodPost, httpServer.URL+"/v1/sessions/"+session.ID+"/runs", "alice", `{"message":"route me"}`)
		return response, readBody(t, response)
	}

	candidateRun, candidateBody := run()
	if candidateRun.Header.Get("X-Harness-Canary") != staged.Canary.ID || !strings.Contains(candidateBody, "candidate") {
		t.Fatalf("candidate route wrong: header=%q body=%s", candidateRun.Header.Get("X-Harness-Canary"), candidateBody)
	}
	pause := doJSON(t, http.MethodPost, httpServer.URL+"/v1/profiles/release.agent/canaries/"+staged.Canary.ID+"/pause", "alice", `{}`)
	if pause.StatusCode != http.StatusOK {
		t.Fatalf("pause status=%d body=%s", pause.StatusCode, readBody(t, pause))
	}
	stableRun, stableBody := run()
	if stableRun.Header.Get("X-Harness-Canary") != "" || !strings.Contains(stableBody, "stable") {
		t.Fatalf("paused route wrong: header=%q body=%s", stableRun.Header.Get("X-Harness-Canary"), stableBody)
	}
	resume := doJSON(t, http.MethodPost, httpServer.URL+"/v1/profiles/release.agent/canaries/"+staged.Canary.ID+"/resume", "alice", `{}`)
	if resume.StatusCode != http.StatusOK {
		t.Fatalf("resume status=%d body=%s", resume.StatusCode, readBody(t, resume))
	}
	resumedRun, resumedBody := run()
	if resumedRun.Header.Get("X-Harness-Canary") != staged.Canary.ID || !strings.Contains(resumedBody, "candidate") {
		t.Fatalf("resumed route wrong: header=%q body=%s", resumedRun.Header.Get("X-Harness-Canary"), resumedBody)
	}
	promote := doJSON(t, http.MethodPost, httpServer.URL+"/v1/profiles/release.agent/canaries/"+staged.Canary.ID+"/promote", "alice", `{}`)
	promoteBody := readBody(t, promote)
	if promote.StatusCode != http.StatusOK || !strings.Contains(promoteBody, `"status":"promoted"`) || !strings.Contains(promoteBody, `"release_version":1`) {
		t.Fatalf("promote status=%d body=%s", promote.StatusCode, promoteBody)
	}
	promotedRun, promotedBody := run()
	if promotedRun.Header.Get("X-Harness-Canary") != "" || !strings.Contains(promotedBody, "candidate") {
		t.Fatalf("promoted live route wrong: header=%q body=%s", promotedRun.Header.Get("X-Harness-Canary"), promotedBody)
	}
	history := readBody(t, doJSON(t, http.MethodGet, httpServer.URL+"/v1/profiles/release.agent/releases", "alice", ""))
	if strings.Count(history, `"version":1`) != 1 || !strings.Contains(history, `"operation_id":"`+staged.Canary.ID+`"`) {
		t.Fatalf("promotion release history wrong: %s", history)
	}
}

func TestCanaryFailedGateDoesNotStage(t *testing.T) {
	httpServer, profiles, evaluations, _ := newGatedCanaryServer(t)
	response := doJSON(t, http.MethodPost, httpServer.URL+"/v1/profiles/release.agent/canaries", "alice", `{
		"scope":[{"kind":"global","id":"global"},{"kind":"product","id":"product"},{"kind":"tenant","id":"acme"}],
		"layer":{"profile_id":"release.agent","model":{"provider":"release-gate","model":"rejected"}},
		"basis_points":1000,
		"evaluation_gate":{"dataset_id":"release.gate","dataset_version":1,"subject_id":"alice"}
	}`)
	body := readBody(t, response)
	if response.StatusCode != http.StatusConflict || !strings.Contains(body, `"passed":false`) {
		t.Fatalf("failed canary gate status=%d body=%s", response.StatusCode, body)
	}
	listed := readBody(t, doJSON(t, http.MethodGet, httpServer.URL+"/v1/profiles/release.agent/canaries", "alice", ""))
	if !strings.Contains(listed, `"canaries":[]`) {
		t.Fatalf("failed gate staged canary: %s", listed)
	}
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product, _ := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "product"})
	tenant, _ := product.Child(core.ScopeRef{Kind: core.ScopeTenant, ID: "acme"})
	principal := core.Principal{SubjectID: "alice", TenantID: "acme", Scope: tenant, Grants: core.NewPermissionSet(core.PermRead)}
	live, err := profiles.Resolve(principal, tenant, "release.agent")
	if err != nil || live.Model.Model != "stable" {
		t.Fatalf("failed canary changed live: %#v err=%v", live, err)
	}
	runs, err := evaluations.ListRuns(context.Background(), "release.gate", "acme", 10)
	if err != nil || len(runs) != 1 || runs[0].Passed {
		t.Fatalf("failed canary evidence missing: %#v err=%v", runs, err)
	}
}

func TestCanaryAndReleaseEvidenceDetailPaths(t *testing.T) {
	httpServer, _, _, _ := newGatedCanaryServer(t)
	stage := doJSON(t, http.MethodPost, httpServer.URL+"/v1/profiles/release.agent/canaries", "alice", `{
		"scope":[{"kind":"global","id":"global"},{"kind":"product","id":"product"},{"kind":"tenant","id":"acme"}],
		"layer":{"profile_id":"release.agent","model":{"provider":"release-gate","model":"candidate"}},
		"basis_points":10000,
		"evaluation_gate":{"dataset_id":"release.gate","dataset_version":1,"subject_id":"alice"}
	}`)
	stageBody := readBody(t, stage)
	if stage.StatusCode != http.StatusCreated {
		t.Fatalf("stage status=%d body=%s", stage.StatusCode, stageBody)
	}
	var staged struct {
		Canary struct {
			ID string `json:"id"`
		} `json:"canary"`
		Evaluation struct {
			CandidateRun evaluation.RunResult `json:"candidate_run"`
		} `json:"evaluation"`
	}
	if err := json.Unmarshal([]byte(stageBody), &staged); err != nil || staged.Canary.ID == "" {
		t.Fatalf("stage response decode failed: %#v err=%v", staged, err)
	}
	canaryEvidenceQuery := "/v1/admin/evidence?assignment_revision=" + staged.Evaluation.CandidateRun.Cases[0].Artifacts.AssignmentRevision + "&limit=50"
	evidenceBody := readBody(t, doJSON(t, http.MethodGet, httpServer.URL+canaryEvidenceQuery, "alice", ""))
	var evidencePage struct {
		Evidence []struct {
			Kind       string `json:"kind"`
			ID         string `json:"id"`
			DetailPath string `json:"detail_path"`
		} `json:"evidence"`
	}
	if err := json.Unmarshal([]byte(evidenceBody), &evidencePage); err != nil {
		t.Fatal(err)
	}
	var canaryPath string
	for _, item := range evidencePage.Evidence {
		if item.Kind == "canary" && item.ID == staged.Canary.ID {
			canaryPath = item.DetailPath
			break
		}
	}
	if canaryPath == "" {
		t.Fatalf("canary evidence detail path missing: %s", evidenceBody)
	}
	canaryDetail := readBody(t, doJSON(t, http.MethodGet, httpServer.URL+canaryPath, "alice", ""))
	if !strings.Contains(canaryDetail, `"id":"`+staged.Canary.ID+`"`) {
		t.Fatalf("canary detail path returned wrong object: %s", canaryDetail)
	}

	promote := doJSON(t, http.MethodPost, httpServer.URL+"/v1/profiles/release.agent/canaries/"+staged.Canary.ID+"/promote", "alice", `{}`)
	promoteBody := readBody(t, promote)
	if promote.StatusCode != http.StatusOK {
		t.Fatalf("promote status=%d body=%s", promote.StatusCode, promoteBody)
	}
	var promoted struct {
		ReleaseVersion int `json:"release_version"`
	}
	if err := json.Unmarshal([]byte(promoteBody), &promoted); err != nil || promoted.ReleaseVersion != 1 {
		t.Fatalf("promotion response wrong: %#v err=%v", promoted, err)
	}
	evidenceBody = readBody(t, doJSON(t, http.MethodGet, httpServer.URL+canaryEvidenceQuery, "alice", ""))
	if err := json.Unmarshal([]byte(evidenceBody), &evidencePage); err != nil {
		t.Fatal(err)
	}
	var releasePath string
	for _, item := range evidencePage.Evidence {
		if item.Kind == "release" && item.DetailPath != "" {
			releasePath = item.DetailPath
			break
		}
	}
	if releasePath == "" {
		t.Fatalf("release evidence detail path missing: %s", evidenceBody)
	}
	releaseDetail := readBody(t, doJSON(t, http.MethodGet, httpServer.URL+releasePath, "alice", ""))
	if !strings.Contains(releaseDetail, `"version":1`) || !strings.Contains(releaseDetail, `"profile_id":"release.agent"`) {
		t.Fatalf("release detail path returned wrong object: %s", releaseDetail)
	}
}

func TestReleaseDetailPathRespectsScopeVisibility(t *testing.T) {
	httpServer, _ := newReleaseServer(t)
	publish := doJSON(t, http.MethodPost, httpServer.URL+"/v1/profiles/crypto.agent.analyst/publish", "alice", `{
		"scope":[{"kind":"global","id":"global"},{"kind":"product","id":"crypto"},{"kind":"tenant","id":"acme"}],
		"layer":{"profile_id":"crypto.agent.analyst"}
	}`)
	if publish.StatusCode != http.StatusCreated {
		t.Fatalf("publish status=%d body=%s", publish.StatusCode, readBody(t, publish))
	}
	visible := doJSON(t, http.MethodGet, httpServer.URL+"/v1/profiles/crypto.agent.analyst/releases/1", "alice", "")
	if visible.StatusCode != http.StatusOK {
		t.Fatalf("visible release detail status=%d body=%s", visible.StatusCode, readBody(t, visible))
	}
	hidden := doJSONWithHeaders(t, http.MethodGet, httpServer.URL+"/v1/profiles/crypto.agent.analyst/releases/1", "bob",
		map[string]string{"Content-Type": "application/json", "X-Harness-Tenant": "otherco"}, "")
	if hidden.StatusCode != http.StatusNotFound {
		t.Fatalf("cross-tenant release detail status=%d body=%s", hidden.StatusCode, readBody(t, hidden))
	}
}

func TestSessionsListRouteUsesLister(t *testing.T) {
	// MemorySessionStore does not implement SessionLister: route says so.
	httpServer, _ := newReleaseServer(t)
	response := doJSON(t, http.MethodGet, httpServer.URL+"/v1/sessions", "alice", "")
	if response.StatusCode != http.StatusNotImplemented {
		t.Fatalf("expected 501 without a lister, got %d", response.StatusCode)
	}

	// A SQL store implements SessionLister: create two sessions and list them.
	sqlDB := openListerDB(t)
	sqlStore, err := storage.OpenSQLSessionStore(context.Background(), sqlDB, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product, _ := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "crypto"})
	capabilities := core.NewCapabilityRegistry()
	profiles := core.NewAgentProfileRegistry()
	if err := cryptoexample.RegisterProductCapabilities(capabilities, product); err != nil {
		t.Fatal(err)
	}
	if err := cryptoexample.RegisterProfiles(profiles, product); err != nil {
		t.Fatal(err)
	}
	runtime := &core.Runtime{
		Capabilities: capabilities, Profiles: profiles,
		Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) {
			return core.MockLlmAdapter{}, nil
		}),
	}
	api, err := server.New(server.Config{
		Runtime: runtime, Sessions: sqlStore,
		Authenticator: server.HeaderAuthenticator{
			Root:          product.Segments(),
			DefaultGrants: core.NewPermissionSet(core.PermRead),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	listServer := newTestHTTPServer(t, api.Handler())

	for range []int{1, 2} {
		created := doJSON(t, http.MethodPost, listServer.URL+"/v1/sessions", "alice", `{"profile_id":"crypto.agent.analyst"}`)
		if created.StatusCode != http.StatusCreated {
			t.Fatalf("create failed: %d", created.StatusCode)
		}
	}
	listed := doJSON(t, http.MethodGet, listServer.URL+"/v1/sessions", "alice", "")
	body := readBody(t, listed)
	if listed.StatusCode != http.StatusOK || strings.Count(body, "sess_") != 2 {
		t.Fatalf("listing wrong: status=%d body=%s", listed.StatusCode, body)
	}
	// bob sees nothing of alice's sessions.
	other := doJSON(t, http.MethodGet, listServer.URL+"/v1/sessions", "bob", "")
	if other.StatusCode != http.StatusOK || strings.Contains(readBody(t, other), "sess_") {
		t.Fatalf("cross-user isolation broken: %s", readBody(t, other))
	}
}
