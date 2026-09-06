package server

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
)

type evidenceQueryOnlyStore struct{}

func (evidenceQueryOnlyStore) QueryEvidence(context.Context, storage.EvidenceQuery) ([]storage.EvidenceRecord, error) {
	return nil, nil
}

func storeEvidenceStatsRun(t *testing.T, store *storage.SQLSessionStore, principal core.Principal, sessionID, runID string, variants []storage.AssignmentVariant, terminal core.RunStatus) {
	t.Helper()
	if len(variants) == 0 {
		t.Fatal("evidence stats run requires at least one segment")
	}
	scope, err := principal.Scope.Child(core.ScopeRef{Kind: core.ScopeSession, ID: sessionID})
	if err != nil {
		t.Fatal(err)
	}
	session, err := core.NewSession(core.SessionOptions{
		ID: sessionID, ProfileID: "evidence.stats", Principal: principal, Scope: scope,
	})
	if err != nil {
		t.Fatal(err)
	}
	for index, variant := range variants {
		metadata := map[string]string{}
		if variant != storage.AssignmentUnassigned {
			metadata["harness.assignment.variant"] = string(variant)
		}
		composition := &core.RunCompositionData{
			Profile: core.AgentProfileSnapshot{ProfileID: "evidence.stats", Scope: scope}, Metadata: metadata,
		}
		if index == 0 {
			if _, err := session.Append(runID, core.EvRunStart, core.RunStartData{Composition: composition}); err != nil {
				t.Fatal(err)
			}
		} else if _, err := session.Append(runID, core.EvRunResume, core.RunResumeData{Composition: composition}); err != nil {
			t.Fatal(err)
		}
	}
	if terminal != "" {
		if _, err := session.Append(runID, core.EvRunEnd, core.RunEndData{Status: terminal}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Create(context.Background(), session); err != nil {
		t.Fatal(err)
	}
}

func TestEvidenceStatsAPIQueryScopeAndAggregation(t *testing.T) {
	fixture := newEvaluationServerFixture(t)
	store, ok := fixture.server.evidence.(*storage.SQLSessionStore)
	if !ok {
		t.Fatalf("evidence store type = %T, want *storage.SQLSessionStore", fixture.server.evidence)
	}
	storeEvidenceStatsRun(t, store, fixture.target, "stats-latest-session", "stats-latest-run",
		[]storage.AssignmentVariant{storage.AssignmentCandidate, storage.AssignmentLive}, core.RunCompleted)
	storeEvidenceStatsRun(t, store, fixture.target, "stats-failed-session", "stats-failed-run",
		[]storage.AssignmentVariant{storage.AssignmentCandidate}, core.RunFailed)
	storeEvidenceStatsRun(t, store, fixture.target, "stats-running-session", "stats-running-run",
		[]storage.AssignmentVariant{storage.AssignmentUnassigned}, "")

	other := fixture.otherAdmin
	other.SubjectID = "bob@other.test"
	otherScope, err := fixture.otherAdmin.Scope.Child(core.ScopeRef{Kind: core.ScopeUser, ID: other.SubjectID})
	if err != nil {
		t.Fatal(err)
	}
	other.Scope = otherScope
	storeEvidenceStatsRun(t, store, other, "stats-other-session", "stats-other-run",
		[]storage.AssignmentVariant{storage.AssignmentLive}, core.RunCompleted)

	response := fixture.request(t, http.MethodGet, "/v1/admin/evidence/stats?profile_id=evidence.stats", "tenant", nil)
	if response.Code != http.StatusOK {
		t.Fatalf("tenant stats status=%d body=%s", response.Code, response.Body.String())
	}
	var result struct {
		Runs storage.RunEvidenceStats `json:"runs"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	stats := result.Runs
	if stats.Total != 3 || stats.Terminal != 2 || stats.Completed != 1 || stats.Failed != 1 || stats.CompletionRate != 0.5 {
		t.Fatalf("tenant stats totals wrong: %#v", stats)
	}
	if stats.ByStatus["completed"] != 1 || stats.ByStatus["failed"] != 1 || stats.ByStatus["running"] != 1 {
		t.Fatalf("tenant stats by status wrong: %#v", stats.ByStatus)
	}
	if candidate := stats.ByVariant[string(storage.AssignmentCandidate)]; candidate.Total != 1 || candidate.Failed != 1 {
		t.Fatalf("candidate stats wrong: %#v", candidate)
	}
	if live := stats.ByVariant[string(storage.AssignmentLive)]; live.Total != 1 || live.Completed != 1 {
		t.Fatalf("latest live segment stats wrong: %#v", live)
	}
	if unassigned := stats.ByVariant[string(storage.AssignmentUnassigned)]; unassigned.Total != 1 || unassigned.Terminal != 0 {
		t.Fatalf("unassigned stats wrong: %#v", unassigned)
	}

	completed := fixture.request(t, http.MethodGet, "/v1/admin/evidence/stats?profile_id=evidence.stats&status=completed", "tenant", nil)
	if completed.Code != http.StatusOK {
		t.Fatalf("completed stats status=%d body=%s", completed.Code, completed.Body.String())
	}
	result = struct {
		Runs storage.RunEvidenceStats `json:"runs"`
	}{}
	if err := json.Unmarshal(completed.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Runs.Total != 1 || result.Runs.ByVariant[string(storage.AssignmentLive)].Total != 1 {
		t.Fatalf("status-filtered stats wrong: %#v", result.Runs)
	}

	otherStats := fixture.request(t, http.MethodGet, "/v1/admin/evidence/stats?profile_id=evidence.stats&tenant=other", "platform", nil)
	if otherStats.Code != http.StatusOK {
		t.Fatalf("platform tenant-filtered stats status=%d body=%s", otherStats.Code, otherStats.Body.String())
	}
	result = struct {
		Runs storage.RunEvidenceStats `json:"runs"`
	}{}
	if err := json.Unmarshal(otherStats.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Runs.Total != 1 || result.Runs.Completed != 1 || result.Runs.ByVariant[string(storage.AssignmentLive)].Total != 1 {
		t.Fatalf("platform tenant-filtered stats wrong: %#v", result.Runs)
	}

	list := fixture.request(t, http.MethodGet, "/v1/admin/evidence?profile_id=evidence.stats", "tenant", nil)
	if list.Code != http.StatusOK {
		t.Fatalf("tenant evidence list status=%d body=%s", list.Code, list.Body.String())
	}
	var page struct {
		Evidence []storage.EvidenceRecord `json:"evidence"`
	}
	if err := json.Unmarshal(list.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Evidence) == 0 {
		t.Fatal("tenant evidence list was empty")
	}
	for _, record := range page.Evidence {
		if record.TenantID != "acme" {
			t.Fatalf("tenant evidence list leaked record: %#v", record)
		}
	}

	for _, path := range []string{"/v1/admin/evidence", "/v1/admin/evidence/stats"} {
		for _, query := range []string{"?cursor=opaque", "?created_after=not-a-time", "?kind=not-a-kind"} {
			invalid := fixture.request(t, http.MethodGet, path+query, "tenant", nil)
			if invalid.Code != http.StatusBadRequest {
				t.Fatalf("invalid evidence query %s%s status=%d body=%s", path, query, invalid.Code, invalid.Body.String())
			}
		}
		crossTenant := fixture.request(t, http.MethodGet, path+"?tenant=other", "tenant", nil)
		if crossTenant.Code != http.StatusForbidden {
			t.Fatalf("cross-tenant evidence query %s status=%d body=%s", path, crossTenant.Code, crossTenant.Body.String())
		}
	}

	forbidden := fixture.request(t, http.MethodGet, "/v1/admin/evidence/stats", "user", nil)
	if forbidden.Code != http.StatusForbidden {
		t.Fatalf("non-operator stats status=%d body=%s", forbidden.Code, forbidden.Body.String())
	}

	fixture.server.evidence = evidenceQueryOnlyStore{}
	unsupportedInvalid := fixture.request(t, http.MethodGet, "/v1/admin/evidence/stats?created_before=not-a-time", "tenant", nil)
	if unsupportedInvalid.Code != http.StatusBadRequest {
		t.Fatalf("unsupported store must still reject malformed stats query: status=%d body=%s", unsupportedInvalid.Code, unsupportedInvalid.Body.String())
	}
	unsupported := fixture.request(t, http.MethodGet, "/v1/admin/evidence/stats", "tenant", nil)
	if unsupported.Code != http.StatusNotImplemented {
		t.Fatalf("unsupported stats status=%d body=%s", unsupported.Code, unsupported.Body.String())
	}
}
