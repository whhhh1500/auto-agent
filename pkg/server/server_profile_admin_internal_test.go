package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/storage"
)

type profileRestoreJournal struct{ records []storage.BindingRecord }

func (j *profileRestoreJournal) Record(_ context.Context, record storage.BindingRecord) error {
	j.records = append(j.records, record)
	return nil
}
func (j *profileRestoreJournal) Delete(_ context.Context, id string) error {
	for i, record := range j.records {
		if record.ID == id {
			j.records = append(j.records[:i], j.records[i+1:]...)
			return nil
		}
	}
	return nil
}
func (j *profileRestoreJournal) List(context.Context) ([]storage.BindingRecord, error) {
	return append([]storage.BindingRecord(nil), j.records...), nil
}

func TestRestoreBindingsRestoresProfileLayer(t *testing.T) {
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	name := "Restored profile"
	model := core.ModelSelection{Provider: "mock", Model: "mock"}
	layer := core.AgentProfileLayer{
		Scope: scope, ProfileID: "general", Name: &name, Model: &model,
		Metadata: map[string]string{
			"harness.executor.id":      "graph-core-turn",
			"harness.executor.version": "1",
		},
	}
	payload, err := json.Marshal(profileBindingPayload{Scope: scope.String(), Layer: layer})
	if err != nil {
		t.Fatal(err)
	}
	journal := &profileRestoreJournal{records: []storage.BindingRecord{{
		ID: profileBindingID(layer.ProfileID, scope), Kind: profileBindingKind, Payload: payload,
	}}}
	profiles := core.NewAgentProfileRegistry()
	server := &Server{
		runtime: &core.Runtime{Profiles: profiles}, journal: journal,
	}
	if err := server.RestoreBindings(context.Background()); err != nil {
		t.Fatal(err)
	}
	principal := core.Principal{Scope: scope}
	effective, err := profiles.Resolve(principal, scope, layer.ProfileID)
	if err != nil {
		t.Fatal(err)
	}
	if effective.Name != name {
		t.Fatalf("restored profile name=%q want %q", effective.Name, name)
	}
	if effective.Metadata["harness.executor.id"] != "graph-core-turn" || effective.Metadata["harness.executor.version"] != "1" {
		t.Fatalf("restored graph metadata=%#v", effective.Metadata)
	}
	if binding, ok := server.adminStateFor().get(journal.records[0].ID); !ok || binding.Kind != profileBindingKind {
		t.Fatalf("restored profile missing admin state: %#v", binding)
	}
}

func TestAdminProfileJournalFailureRollsBackMount(t *testing.T) {
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	model := core.ModelSelection{Provider: "mock", Model: "mock"}
	profiles := core.NewAgentProfileRegistry()
	server := &Server{
		runtime: &core.Runtime{Profiles: profiles}, journal: &failingBindingJournal{},
		maxBody: 1 << 20,
		authenticator: AuthenticatorFunc(func(*http.Request) (core.Principal, error) {
			return core.Principal{SubjectID: "admin", Scope: scope, Attributes: map[string]string{"role": storage.RoleAccountAdmin}}, nil
		}),
		sessions: core.NewMemorySessionStore(),
	}
	body, err := json.Marshal(adminProfileWriteRequest{Scope: scope, Layer: core.AgentProfileLayer{
		ProfileID: "rollback.agent", Model: &model,
	}})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPut, "/v1/admin/profiles/rollback.agent", bytes.NewReader(body))
	request.SetPathValue("id", "rollback.agent")
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	server.handleAdminProfilePut(response, request)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("journal failure status=%d body=%s", response.Code, response.Body.String())
	}
	principal := core.Principal{Scope: scope}
	if _, err := profiles.Resolve(principal, scope, "rollback.agent"); err == nil {
		t.Fatal("journal failure left profile mounted")
	}
}
