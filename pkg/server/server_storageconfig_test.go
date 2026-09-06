package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	appidentity "github.com/cc-auto-agent/harness-core/pkg/app/identity"
	appstorageconfig "github.com/cc-auto-agent/harness-core/pkg/app/storageconfig"
	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
)

func TestStorageConfigUseCasesDriveTypedHTTPWithoutSecretLeakage(t *testing.T) {
	getResult := appstorageconfig.GetResult{
		Found: true,
		Config: appstorageconfig.ConfigView{
			Backend: appstorageconfig.BackendFile, Path: "data/sessions", PathStyle: true,
			DisableConditionalWrites: true, Source: appstorageconfig.SourceDB,
			Status: appstorageconfig.StatusActive, DesiredRevision: "rev_desired",
			HasSecret: true, SecretPreview: "pre••••••••tail",
		},
		Evidence: appstorageconfig.ResolutionEvidence{
			Kind: appstorageconfig.KindSessions, Source: appstorageconfig.SourceDB,
			Status: appstorageconfig.StatusRestartPending, Found: true,
			DesiredType: appstorageconfig.BackendFile, ActiveType: appstorageconfig.BackendEmbedded,
			DesiredRevision: "rev_desired", ActiveRevision: "rev_active", RestartPending: true,
		},
		Active: appstorageconfig.ActiveState{Backend: appstorageconfig.BackendEmbedded, Revision: "rev_active"},
	}
	putResult := appstorageconfig.PutResult{
		Config:   getResult.Config,
		Evidence: getResult.Evidence,
	}
	useCases := &storageConfigUseCasesStub{getResult: getResult, putResult: putResult}
	audit := &storageConfigAuditStore{}
	api := newSettingsTestServer(t, core.Principal{
		SubjectID: "platform", TenantID: "global", Attributes: map[string]string{"role": storage.RoleAccountAdmin},
	}, Config{StorageConfigUseCases: useCases, Audit: audit})

	get := httptest.NewRecorder()
	api.Handler().ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/v1/admin/storage/sessions", nil))
	getBody := get.Body.String()
	if get.Code != http.StatusOK {
		t.Fatalf("typed GET status=%d body=%s", get.Code, getBody)
	}
	if useCases.getCommand.Actor != (appidentity.AdminActor{AccountID: "platform", TenantID: "global", Role: appidentity.RoleAdmin}) || useCases.getCommand.Kind != appstorageconfig.KindSessions {
		t.Fatalf("typed GET command=%#v", useCases.getCommand)
	}
	for _, want := range []string{
		`"path":"data/sessions"`, `"config_source":"db"`, `"config_status":"active"`,
		`"disable_conditional_writes":true`, `"desired_type":"file"`, `"active_type":"embedded"`,
		`"desired_revision":"rev_desired"`, `"active_revision":"rev_active"`, `"restart_pending":true`,
		`"secret_preview":"pre••••••••tail"`,
	} {
		if !strings.Contains(getBody, want) {
			t.Fatalf("typed GET missing %q: %s", want, getBody)
		}
	}
	for _, forbidden := range []string{"secret_key", "pre-middle-unique-tail", "middle", `"disable_conditional_writes":false`} {
		if strings.Contains(getBody, forbidden) {
			t.Fatalf("typed GET exposed or misrepresented %q: %s", forbidden, getBody)
		}
	}
	if !strings.Contains(getBody, `"active":"applies on restart"`) {
		t.Fatalf("typed GET did not preserve legacy sessions active label: %s", getBody)
	}

	put := httptest.NewRecorder()
	api.Handler().ServeHTTP(put, httptest.NewRequest(http.MethodPut, "/v1/admin/storage/sessions", strings.NewReader(`{
		"type":"file","path":"data/sessions","path_style":true,"config_status":"active",
		"disable_conditional_writes":true,"expected_revision":"rev_previous","secret_key":"pre-middle-unique-tail"
	}`)))
	putBody := put.Body.String()
	if put.Code != http.StatusOK {
		t.Fatalf("typed PUT status=%d body=%s", put.Code, putBody)
	}
	command := useCases.putCommand
	if command.Actor != (appidentity.AdminActor{AccountID: "platform", TenantID: "global", Role: appidentity.RoleAdmin}) ||
		command.Kind != appstorageconfig.KindSessions || command.Backend != appstorageconfig.BackendFile ||
		command.Path != "data/sessions" || !command.PathStyle || !command.DisableConditionalWrites ||
		!command.DisableConditionalWritesPresent || command.Status != appstorageconfig.StatusActive ||
		command.ExpectedRevision != "rev_previous" || command.SecretKey == nil || *command.SecretKey != "pre-middle-unique-tail" {
		t.Fatalf("typed PUT command=%#v", command)
	}
	if strings.Contains(putBody, "pre-middle-unique-tail") || strings.Contains(putBody, "middle") || strings.Contains(putBody, `"secret_key"`) {
		t.Fatalf("typed PUT leaked secret: %s", putBody)
	}
	if !strings.Contains(putBody, `"status":"saved"`) || !strings.Contains(putBody, `"applied":"restart to apply"`) {
		t.Fatalf("typed PUT response=%s", putBody)
	}
	// expected_revision is input-only and must not be echoed as if it were the
	// newly persisted desired revision.
	if strings.Contains(putBody, `"expected_revision"`) {
		t.Fatalf("typed PUT echoed expected revision: %s", putBody)
	}
	event := audit.last(t)
	if event.Action != "storage.config" || event.Target != storageSessionsKey {
		t.Fatalf("typed PUT audit=%#v", event)
	}
	detail, err := json.Marshal(event.Detail)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(detail), "pre-middle-unique-tail") || strings.Contains(string(detail), "middle") || strings.Contains(string(detail), "pre••••••••tail") {
		t.Fatalf("typed PUT audit exposed secret material: %s", detail)
	}
}

func TestStorageConfigUseCaseErrorsAndAuthorizationMapStably(t *testing.T) {
	admin := core.Principal{SubjectID: "platform", Attributes: map[string]string{"role": storage.RoleAccountAdmin}}
	for name, test := range map[string]struct {
		err  error
		want int
	}{
		"forbidden":             {err: appstorageconfig.ErrForbidden, want: http.StatusForbidden},
		"invalid input":         {err: appstorageconfig.ErrInvalidInput, want: http.StatusBadRequest},
		"invalid configuration": {err: appstorageconfig.ErrInvalidConfiguration, want: http.StatusBadRequest},
		"inactive":              {err: appstorageconfig.ErrInactiveConfiguration, want: http.StatusBadRequest},
		"conflict":              {err: appstorageconfig.ErrConflict, want: http.StatusConflict},
		"internal":              {err: errors.New("storage runtime failed"), want: http.StatusInternalServerError},
	} {
		t.Run(name, func(t *testing.T) {
			for operation, useCases := range map[string]*storageConfigUseCasesStub{
				"get": {getErr: test.err},
				"put": {putErr: test.err},
			} {
				t.Run(operation, func(t *testing.T) {
					api := newSettingsTestServer(t, admin, Config{StorageConfigUseCases: useCases})
					response := httptest.NewRecorder()
					request := httptest.NewRequest(http.MethodGet, "/v1/admin/storage/resources", nil)
					if operation == "put" {
						request = httptest.NewRequest(http.MethodPut, "/v1/admin/storage/resources", strings.NewReader(`{"type":"embedded"}`))
					}
					api.Handler().ServeHTTP(response, request)
					if response.Code != test.want {
						t.Fatalf("%s %s status=%d body=%s", name, operation, response.Code, response.Body.String())
					}
				})
			}
		})
	}

	ordinary := core.Principal{SubjectID: "ordinary", Attributes: map[string]string{"role": storage.RoleAccountUser}}
	api := newSettingsTestServer(t, ordinary, Config{StorageConfigUseCases: &storageConfigUseCasesStub{putErr: errors.New("must not be called")}})
	response := httptest.NewRecorder()
	api.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodPut, "/v1/admin/storage/resources", strings.NewReader("{")))
	if response.Code != http.StatusForbidden {
		t.Fatalf("unauthorized malformed typed PUT status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestStorageConfigApplyFailureAfterPersistIsAuditedWithoutSuccessResponse(t *testing.T) {
	audit := &storageConfigAuditStore{}
	useCases := &storageConfigUseCasesStub{
		putResult: appstorageconfig.PutResult{
			Config: appstorageconfig.ConfigView{
				Backend: appstorageconfig.BackendS3, DesiredRevision: "rev_committed", Status: appstorageconfig.StatusActive,
				HasSecret: true, SecretPreview: "pre••••••••tail",
			},
			Evidence: appstorageconfig.ResolutionEvidence{
				Kind: appstorageconfig.KindResources, Source: appstorageconfig.SourceDB,
				Status: appstorageconfig.StatusApplyFailed, Found: true,
				DesiredType: appstorageconfig.BackendS3, DesiredRevision: "rev_committed",
				ErrorCode: appstorageconfig.ErrorCodeApplyFailed,
			},
		},
		putErr: errors.Join(appstorageconfig.ErrApplyFailed, errors.New("backend unavailable")),
	}
	api := newSettingsTestServer(t, core.Principal{
		SubjectID: "platform", Attributes: map[string]string{"role": storage.RoleAccountAdmin},
	}, Config{StorageConfigUseCases: useCases, Audit: audit})
	response := httptest.NewRecorder()
	api.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodPut, "/v1/admin/storage/resources", strings.NewReader(`{"type":"s3","secret_key":"pre-middle-unique-tail"}`)))
	if response.Code != http.StatusInternalServerError || strings.Contains(response.Body.String(), "active now") || strings.Contains(response.Body.String(), "pre-middle-unique-tail") {
		t.Fatalf("apply-failed response status=%d body=%s", response.Code, response.Body.String())
	}
	event := audit.last(t)
	if event.Action != "storage.config" || event.Target != storageResourcesKey || event.Detail["type"] != "s3" ||
		event.Detail["revision"] != "rev_committed" || event.Detail["status"] != "apply_failed" ||
		event.Detail["error_code"] != "storage_config_apply_failed" {
		t.Fatalf("apply-failed audit=%#v", event)
	}
	detail, err := json.Marshal(event.Detail)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"pre-middle-unique-tail", "middle", "pre••••••••tail", "secret_key"} {
		if strings.Contains(string(detail), forbidden) {
			t.Fatalf("apply-failed audit exposed %q: %s", forbidden, detail)
		}
	}
}

func TestStorageConfigResourceResponseOmitsSessionOnlyConditionalWriteFlag(t *testing.T) {
	flag := true
	configuration := storageConfigHTTPConfiguration(appstorageconfig.KindResources, appstorageconfig.ConfigView{
		Backend: appstorageconfig.BackendEmbedded, DisableConditionalWrites: flag,
	})
	encoded, err := json.Marshal(configuration)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "disable_conditional_writes") {
		t.Fatalf("resources response exposed session-only conditional-write field: %s", encoded)
	}
}

type storageConfigUseCasesStub struct {
	getResult  appstorageconfig.GetResult
	getErr     error
	putResult  appstorageconfig.PutResult
	putErr     error
	getCommand appstorageconfig.GetCommand
	putCommand appstorageconfig.PutCommand
}

func (stub *storageConfigUseCasesStub) Get(_ context.Context, command appstorageconfig.GetCommand) (appstorageconfig.GetResult, error) {
	stub.getCommand = command
	return stub.getResult, stub.getErr
}

func (stub *storageConfigUseCasesStub) Put(_ context.Context, command appstorageconfig.PutCommand) (appstorageconfig.PutResult, error) {
	stub.putCommand = command
	return stub.putResult, stub.putErr
}

type storageConfigAuditStore struct{ events []storage.AuditEvent }

func (store *storageConfigAuditStore) RecordAudit(_ context.Context, event storage.AuditEvent) error {
	store.events = append(store.events, event)
	return nil
}

func (store *storageConfigAuditStore) ListAudit(context.Context, storage.AuditFilter) ([]storage.AuditEvent, int, error) {
	return append([]storage.AuditEvent(nil), store.events...), len(store.events), nil
}

func (store *storageConfigAuditStore) last(t *testing.T) storage.AuditEvent {
	t.Helper()
	if len(store.events) == 0 {
		t.Fatal("expected storage configuration audit event")
	}
	return store.events[len(store.events)-1]
}
