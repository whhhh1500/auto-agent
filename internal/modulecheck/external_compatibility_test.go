// Package modulecheck_test is intentionally outside every public package.
// These compile assertions model a small third-party integration: they import
// public contracts and constructors without becoming a production dependency.
package modulecheck_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/cc-auto-agent/harness-core/pkg/adapter/httpapi/jsonbody"
	modelsettingshttp "github.com/cc-auto-agent/harness-core/pkg/adapter/httpapi/modelsettings"
	settingshttp "github.com/cc-auto-agent/harness-core/pkg/adapter/httpapi/settings"
	storagehttp "github.com/cc-auto-agent/harness-core/pkg/adapter/httpapi/storage"
	"github.com/cc-auto-agent/harness-core/pkg/adapter/modelexecution/corebridge"
	modelexecutionopenai "github.com/cc-auto-agent/harness-core/pkg/adapter/modelexecution/openai"
	modelprotocol "github.com/cc-auto-agent/harness-core/pkg/adapter/modelprotocol"
	modelprovider "github.com/cc-auto-agent/harness-core/pkg/adapter/modelprovider"
	modelsettingsadapter "github.com/cc-auto-agent/harness-core/pkg/adapter/modelsettings"
	sqlartifactmigration "github.com/cc-auto-agent/harness-core/pkg/adapter/sql/artifactmigration"
	fencejournal "github.com/cc-auto-agent/harness-core/pkg/adapter/sql/fencejournal"
	sqlsettings "github.com/cc-auto-agent/harness-core/pkg/adapter/sql/settings"
	"github.com/cc-auto-agent/harness-core/pkg/adapter/sql/sqlkit"
	appartifactmigration "github.com/cc-auto-agent/harness-core/pkg/app/artifactmigration"
	"github.com/cc-auto-agent/harness-core/pkg/app/identity"
	"github.com/cc-auto-agent/harness-core/pkg/app/modelcontrol"
	"github.com/cc-auto-agent/harness-core/pkg/app/modelexecution"
	appmodelsettings "github.com/cc-auto-agent/harness-core/pkg/app/modelsettings"
	"github.com/cc-auto-agent/harness-core/pkg/app/runexecutor"
	"github.com/cc-auto-agent/harness-core/pkg/app/secretview"
	appsettings "github.com/cc-auto-agent/harness-core/pkg/app/settings"
	"github.com/cc-auto-agent/harness-core/pkg/control"
	"github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/evaluation"
	"github.com/cc-auto-agent/harness-core/pkg/execution"
	"github.com/cc-auto-agent/harness-core/pkg/extensions/graph"
	"github.com/cc-auto-agent/harness-core/pkg/runtime"
	"github.com/cc-auto-agent/harness-core/pkg/server"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
)

// Keep the fixture as a compile contract. The test body deliberately does not
// start a server or open a database, so it is safe in every CI environment.
func TestExternalCompatibilityFixture(t *testing.T) {
	var (
		_ core.LlmAdapter                              = fixtureLLM{}
		_ core.SessionStore                            = fixtureSessions{}
		_ identity.AdminUseCases                       = (*identity.AdminService)(nil)
		_ appsettings.Repository                       = fixtureSettingsRepository{}
		_ appsettings.UseCases                         = (*appsettings.Service)(nil)
		_ appsettings.Repository                       = (*sqlsettings.Store)(nil)
		_ appmodelsettings.Repository                  = fixtureModelSettingsRepository{}
		_ appmodelsettings.UseCases                    = (*appmodelsettings.Service)(nil)
		_ appmodelsettings.Repository                  = (*modelsettingsadapter.Store)(nil)
		_ storage.SQLDialect                           = sqlkit.SQLite
		_ sqlkit.Dialect                               = storage.SQLDialectPostgres
		_ func(string, sqlkit.Dialect) (string, error) = sqlkit.Bind
		_ storage.AccountStore                         = (*storage.SQLAccountStore)(nil)
		_ storage.ObjectStore                          = storage.NewMemoryObjectStore()
		_ server.Authenticator                         = server.AuthenticatorFunc(fixtureAuthenticate)
		_ control.CanaryStore                          = fixtureCanaries{}
		_ evaluation.Store                             = fixtureEvaluations{}
		_ core.Executor                                = execution.HTTPExecutor{}
		_ execution.ToolLibraryObserver                = execution.NewMemoryLibraryObserver()
		_ modelprovider.Registration
		_ modelprotocol.Registration
		_ runtime.FenceJournal            = (*fencejournal.Store)(nil)
		_ appartifactmigration.Repository = (*sqlartifactmigration.Store)(nil)
	)

	// Referencing constructors and stable helpers catches accidental removal or
	// signature changes while keeping this fixture independent of implementation
	// state and external services.
	_ = core.NewAgent
	_ = core.NewAgentProfileRegistry
	_ = core.NewCapabilityRegistry
	_ = core.NewSession
	_ = core.NewScopePath
	_ = identity.NewAdminService
	_ = appsettings.NewService
	_ = appmodelsettings.NewService
	_ = appmodelsettings.NormalizeAllowedModels
	_ = appmodelsettings.NormalizeStoredConfiguration
	_ = appmodelsettings.ConfigSourceDB
	_ = secretview.Preview
	_ = modelsettingshttp.PutRequest{}
	_ = modelsettingshttp.GetResponse{}
	_ = modelsettingshttp.PutResponse{}
	_ = modelsettingshttp.OptionalBool{}
	_ = modelsettingshttp.OptionalInt{}
	_ = modelsettingshttp.LegacyPutRequest
	_ = modelsettingsadapter.New
	_ = settingshttp.PutRequest{}
	_ = settingshttp.GetResponse{}
	_ = settingshttp.PutResponse{}
	_ = storagehttp.Configuration{}
	_ = storagehttp.GetResponse{}
	_ = storagehttp.OptionalString{}
	_ = storagehttp.PutRequest{}
	_ = storagehttp.PutResponse{}
	_ = storagehttp.TestRequest{}
	_ = storagehttp.TestResponse{}
	_ = sqlsettings.Options{}
	_ = sqlsettings.New
	_ = jsonbody.Decode
	_ = jsonbody.DecodeOptional
	_ = sqlkit.Bind
	_ = storage.NewSQLAccountStore
	_ = storage.OpenSQLSessionStore
	_ = storage.NewMemoryObjectStore
	_ = server.New
	_ = control.NewReleaseManager
	_ = control.NewCanaryManager
	_ = evaluation.NewMemoryStore
	_ = evaluation.NewRegistry
	_ = execution.ValidatePublicHTTPURL
	_ = modelprovider.Registration.Extension
	_ = modelprotocol.Registration.Extension
	_ = fencejournal.New
	_ = sqlartifactmigration.New
	_ = appartifactmigration.ValidateMigration
	_ = (appartifactmigration.Migration{}).MutationToken
	_ = runexecutor.NewRegistry
	_ = runexecutor.NewDefaultRegistry
	_ = runexecutor.NewSequential
	_ = runexecutor.SequentialRegistration
	_ = runexecutor.Registration{}
	_ = runexecutor.Metadata{}
	_ = runexecutor.SequentialID
	_ = modelcontrol.NewRegistry
	_ = (&modelcontrol.Registry{}).Catalogs
	_ = (&modelcontrol.Registry{}).Compatibilities
	_ = (&modelcontrol.Registry{}).Protocols
	_ = (&modelcontrol.Registry{}).Providers
	_ = (&modelcontrol.Registry{}).Resolve
	_ = (&modelcontrol.Registry{}).SnapshotRevision
	_ = modelcontrol.CatalogModel{}
	_ = modelcontrol.CredentialRef{}
	_ = modelcontrol.EndpointRef{}
	_ = modelcontrol.ImplementationBinding{}
	_ = modelcontrol.ModelCapabilities{}
	_ = modelcontrol.ProviderPlan{}
	_ = modelcontrol.ProviderSpec{}
	_ = modelcontrol.ProtocolSpec{}
	_ = modelcontrol.Compatibility{}
	_ = modelcontrol.ModalityText
	_ = modelexecution.NewRegistry
	_ = modelexecution.NewStreamValidator
	_ = modelexecution.NewCredentialMaterial
	_ = (&modelexecution.Registry{}).Execute
	_ = modelexecution.Request{}.Clone
	_ = (&modelexecution.CredentialMaterial{}).Bytes
	_ = modelexecutionopenai.NewHTTPProvider
	_ = modelexecutionopenai.NewStaticCredentialResolver
	_ = modelexecutionopenai.ChatCompletionsProtocol{}
	_ = corebridge.Adapter{}
	_ = graph.NewNodeKindRegistry
	_ = graph.NewPredicateRegistry
	_ = graph.NewReducerRegistry
	_ = graph.ValidateDefinition
	_ = graph.ApplyStatePatch
	_ = graph.Definition{}
	_ = graph.ValidatedDefinition{}
	_ = graph.StatePatch{}
	fixtureNodeKinds, err := graph.NewNodeKindRegistry([]graph.NodeKindMetadata{{ID: "fixture", Version: "1"}})
	if err != nil {
		t.Fatal(err)
	}
	fixtureReducers, err := graph.NewReducerRegistry(nil)
	if err != nil {
		t.Fatal(err)
	}
	fixturePredicates, err := graph.NewPredicateRegistry(nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.ValidateDefinition(graph.Definition{
		ID: "fixture", Version: "1", EntryNode: "end",
		State: graph.StateSchema{Reducer: graph.ReducerTopLevelJSONPatch, ReducerVersion: "1"}, Limits: graph.Limits{MaxSteps: 1},
		Nodes: []graph.NodeSpec{{
			ID: "end", Kind: "fixture", KindVersion: "1", Terminal: true, Timeout: time.Second,
			ContextView:   graph.ContextView{Layers: []graph.LayerKind{graph.LayerCurrentInput}},
			ContextBudget: graph.ContextBudget{TotalTokens: 1, LayerBudgets: map[graph.LayerKind]int64{graph.LayerCurrentInput: 1}},
		}},
	}, fixtureNodeKinds, fixtureReducers, fixturePredicates); err != nil {
		t.Fatalf("external Graph-G0 fixture: %v", err)
	}

	// Keep the compiler from treating the assertions above as an accidental
	// unused-only fixture if the compiler changes its handling of blank ids.
	t.Log("public compatibility contracts compile")
}

func TestModelRegistrationDomainsRemainIndependent(t *testing.T) {
	provider, err := (modelprovider.Registration{ID: "shared-provider", Version: runtime.Version{Major: 1}, Priority: 1}).Extension()
	if err != nil {
		t.Fatal(err)
	}
	protocol, err := (modelprotocol.Registration{ID: "shared-protocol", Version: runtime.Version{Major: 1}, Priority: 1}).Extension()
	if err != nil {
		t.Fatal(err)
	}
	if provider.ID == protocol.ID || provider.Contract == protocol.Contract {
		t.Fatalf("provider/protocol metadata domains collided: provider=%#v protocol=%#v", provider, protocol)
	}
	if _, err := runtime.BuildSnapshot([]runtime.ModuleManifest{
		{ID: "provider-owner", Version: runtime.Version{Major: 1}, Provides: []runtime.ProvidedExtension{provider}},
		{ID: "protocol-owner", Version: runtime.Version{Major: 1}, Provides: []runtime.ProvidedExtension{protocol}},
	}); err != nil {
		t.Fatalf("independent provider/protocol metadata did not compose: %v", err)
	}
}

type fixtureLLM struct{}

type fixtureSettingsRepository struct{}

func (fixtureSettingsRepository) GetSetting(context.Context, string) (string, bool, error) {
	return "", false, nil
}

func (fixtureSettingsRepository) SetSetting(context.Context, string, string) error { return nil }

type fixtureModelSettingsRepository struct{}

func (fixtureModelSettingsRepository) Load(context.Context) (appmodelsettings.StoredConfiguration, bool, error) {
	return appmodelsettings.StoredConfiguration{}, false, nil
}

func (fixtureModelSettingsRepository) Save(context.Context, appmodelsettings.StoredConfiguration) error {
	return nil
}

func (fixtureLLM) Provider() string { return "fixture" }
func (fixtureLLM) Stream(context.Context, core.GenerateOptions, func(core.StreamChunk)) error {
	return nil
}

type fixtureSessions struct{}

func (fixtureSessions) Create(context.Context, *core.Session) error { return nil }
func (fixtureSessions) Load(context.Context, string) (*core.Session, error) {
	return nil, nil
}
func (fixtureSessions) Save(context.Context, *core.Session, int64) error { return nil }

func fixtureAuthenticate(*http.Request) (core.Principal, error) {
	return core.Principal{}, nil
}

type fixtureCanaries struct{}

func (fixtureCanaries) CreateCanary(context.Context, control.CanaryRecord) error { return nil }
func (fixtureCanaries) UpdateCanary(context.Context, control.CanaryRecord, ...control.CanaryStatus) (bool, error) {
	return false, nil
}
func (fixtureCanaries) GetCanary(context.Context, string) (control.CanaryRecord, error) {
	return control.CanaryRecord{}, nil
}
func (fixtureCanaries) ListOpenCanaries(context.Context) ([]control.CanaryRecord, error) {
	return nil, nil
}
func (fixtureCanaries) ListCanaries(context.Context, string, int) ([]control.CanaryRecord, error) {
	return nil, nil
}
func (fixtureCanaries) ControlRevision(context.Context) (int64, error) { return 0, nil }

type fixtureEvaluations struct{}

func (fixtureEvaluations) PutDataset(context.Context, evaluation.Dataset) (evaluation.Dataset, bool, error) {
	return evaluation.Dataset{}, false, nil
}
func (fixtureEvaluations) GetDataset(context.Context, string, int) (evaluation.Dataset, error) {
	return evaluation.Dataset{}, nil
}
func (fixtureEvaluations) ListDatasets(context.Context, string, int) ([]evaluation.DatasetSummary, error) {
	return nil, nil
}
func (fixtureEvaluations) CreateRun(context.Context, evaluation.RunResult) error { return nil }
func (fixtureEvaluations) RecordRunError(context.Context, string, string) error  { return nil }
func (fixtureEvaluations) RecordCaseResult(context.Context, string, evaluation.CaseResult) error {
	return nil
}
func (fixtureEvaluations) FinishRun(context.Context, evaluation.RunResult) error { return nil }
func (fixtureEvaluations) GetRun(context.Context, string) (evaluation.RunResult, error) {
	return evaluation.RunResult{}, nil
}
func (fixtureEvaluations) ListRuns(context.Context, string, string, int) ([]evaluation.RunResult, error) {
	return nil, nil
}
