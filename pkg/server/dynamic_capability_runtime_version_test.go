package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	capabilityruntime "github.com/cc-auto-agent/harness-core/pkg/app/capabilityruntime"
	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
)

type versionedCapabilityFactory struct {
	seen     capabilityruntime.Request
	revision string
}

func (*versionedCapabilityFactory) ID() string      { return "custom" }
func (*versionedCapabilityFactory) Version() string { return "2" }
func (f *versionedCapabilityFactory) ImplementationRevision() string {
	if f.revision != "" {
		return f.revision
	}
	return "custom/v2"
}
func (f *versionedCapabilityFactory) New(_ context.Context, req capabilityruntime.Request) (capabilityruntime.Result, error) {
	f.seen = req
	return capabilityruntime.Result{Provider: versionedCapabilityProvider{}, Manifest: req.Manifest}, nil
}

type versionedCapabilityProvider struct{}

func (versionedCapabilityProvider) Execute(context.Context, core.CapabilityRequest) (core.CapabilityResult, error) {
	return core.CapabilityResult{Content: "ok", OK: true}, nil
}

func TestDynamicCapabilityCustomV2JournalRestoreAndStandardFields(t *testing.T) {
	factory := &versionedCapabilityFactory{}
	api, global, journal, _ := newDynamicBindingServerWithFactories(t, []capabilityruntime.Factory{factory})
	response := dynamicBindingRequest(t, api.Handler(), dynamicCapabilityPayload(global, "custom.echo", map[string]any{
		"runtime": "custom@2", "entrypoint": "local-entry", "workdir": "workspace", "writes": true,
		"headers": map[string]string{"X-Ref": "$credential:token"},
	}))
	if response.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if len(journal.records) != 1 {
		t.Fatalf("records=%d", len(journal.records))
	}
	payload := string(journal.records[0].Payload)
	if strings.Contains(payload, "secret") || !strings.Contains(payload, `"runtime":"custom@2"`) || !strings.Contains(payload, `"workdir":"workspace"`) {
		t.Fatalf("bad journal payload: %s", payload)
	}
	second, err := New(Config{Runtime: &core.Runtime{Capabilities: core.NewCapabilityRegistry()}, Sessions: core.NewMemorySessionStore(), Authenticator: api.authenticator, BindingJournal: journal, CapabilityRuntimeFactories: []capabilityruntime.Factory{&versionedCapabilityFactory{}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := second.RestoreBindings(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot := resolvedDynamicSnapshot(t, second, global)
	if _, ok := snapshot.ManifestFor("custom.echo"); !ok {
		t.Fatal("custom capability not restored")
	}
	result, err := snapshot.Execute(context.Background(), core.ToolCall{ID: "call-1", Name: "custom.echo", Args: map[string]any{}})
	if err != nil || !result.OK || result.Content != "ok" {
		t.Fatalf("restored provider result=%#v err=%v", result, err)
	}
}

func TestDynamicCapabilityCustomHeadersRejectSecretAndAllowCredentialRef(t *testing.T) {
	factory := &versionedCapabilityFactory{}
	api, global, journal, _ := newDynamicBindingServerWithFactories(t, []capabilityruntime.Factory{factory})
	bad := dynamicBindingRequest(t, api.Handler(), dynamicCapabilityPayload(global, "custom.secret", map[string]any{
		"runtime": "custom@2", "entrypoint": "local", "headers": map[string]string{"Authorization": "Bearer actual-secret-value"},
	}))
	if bad.Code != http.StatusBadRequest || len(journal.records) != 0 || strings.Contains(bad.Body.String(), "actual-secret-value") {
		t.Fatalf("secret header accepted/leaked: status=%d body=%s records=%d", bad.Code, bad.Body.String(), len(journal.records))
	}
	good := dynamicBindingRequest(t, api.Handler(), dynamicCapabilityPayload(global, "custom.credential", map[string]any{
		"runtime": "custom@2", "entrypoint": "local", "headers": map[string]string{"Authorization": "$credential:api-token", "Accept": "application/json"},
	}))
	if good.Code != http.StatusCreated || len(journal.records) != 1 {
		t.Fatalf("credential reference rejected: status=%d records=%d", good.Code, len(journal.records))
	}
}

func TestDynamicCapabilityCustomRevisionMismatchRestoreFailsClosed(t *testing.T) {
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	payload, _ := json.Marshal(dynamicCapabilityJournalPayload{Capability: "custom.echo", Scope: global.String(), Runtime: "custom@2", RuntimeImplementationRevision: "custom/v2", Manifest: core.CapabilityManifest{ID: "custom.echo", Version: "1.0.0", Kind: "connector"}})
	journal := &dynamicBindingJournal{records: []storage.BindingRecord{{ID: "adm_rev", Kind: "capability", Payload: payload}}}
	api, _, _, _ := newDynamicBindingServerWithFactories(t, []capabilityruntime.Factory{&versionedCapabilityFactory{revision: "custom/v2-mutated"}})
	api.journal = journal
	if err := api.RestoreBindings(context.Background()); err == nil {
		t.Fatal("revision mismatch restored")
	}
	if _, ok := resolvedDynamicSnapshot(t, api, global).ManifestFor("custom.echo"); ok {
		t.Fatal("mismatched capability mounted")
	}
	response := httptest.NewRecorder()
	api.handleReady(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if response.Code == http.StatusOK {
		t.Fatal("ready despite revision mismatch")
	}
}

func TestDynamicCapabilityExternalMissingRevisionRestoreFailsClosed(t *testing.T) {
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	payload, _ := json.Marshal(dynamicCapabilityJournalPayload{Capability: "custom.echo", Scope: global.String(), Runtime: "custom@2", Manifest: core.CapabilityManifest{ID: "custom.echo", Version: "1.0.0", Kind: "connector"}})
	journal := &dynamicBindingJournal{records: []storage.BindingRecord{{ID: "adm_missing_rev", Kind: "capability", Payload: payload}}}
	api, _, _, _ := newDynamicBindingServerWithFactories(t, []capabilityruntime.Factory{&versionedCapabilityFactory{}})
	api.journal = journal
	if err := api.RestoreBindings(context.Background()); err == nil {
		t.Fatal("missing external revision restored")
	}
	if _, ok := resolvedDynamicSnapshot(t, api, global).ManifestFor("custom.echo"); ok {
		t.Fatal("missing-revision capability mounted")
	}
}

func newDynamicBindingServerWithFactories(t *testing.T, factories []capabilityruntime.Factory) (*Server, core.ScopePath, *dynamicBindingJournal, *dynamicBindingAudit) {
	api, global, journal, audit := newDynamicBindingServer(t, nil)
	// Rebuild through the public constructor so external factories follow the same immutable path.
	api, err := New(Config{Runtime: api.runtime, Sessions: api.sessions, Authenticator: api.authenticator, BindingJournal: journal, Audit: audit, CapabilityRuntimeFactories: factories})
	if err != nil {
		t.Fatal(err)
	}
	return api, global, journal, audit
}
