package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	appidentity "github.com/whhhh1500/auto-agent/pkg/app/identity"
	appmodelsettings "github.com/whhhh1500/auto-agent/pkg/app/modelsettings"
	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/extensions/memory"
	"github.com/whhhh1500/auto-agent/pkg/extensions/rag"
)

func TestGeneralProfileControllerPrefersPersistedModelAndHasNoTools(t *testing.T) {
	t.Setenv("HARNESS_LLM_MODEL", "environment-model")
	registry, global, product, principal := newGeneralProfileTestRegistry(t)
	repository := &generalModelSettingsRepositoryStub{found: true, configuration: appmodelsettings.StoredConfiguration{
		Model: "database-model", APIKey: "database-key",
	}}
	controller, err := newGeneralProfileController(registry, global, generalProfileModelSource(repository))
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	if err := controller.Mount(context.Background()); err != nil {
		t.Fatal(err)
	}

	profile, err := registry.Resolve(principal, product, generalProfileID)
	if err != nil {
		t.Fatal(err)
	}
	if profile.Name != generalProfileName || profile.Model.Provider != legacyOpenAIProvider || profile.Model.Model != "database-model" {
		t.Fatalf("general profile identity/model=%q/%#v", profile.Name, profile.Model)
	}
	if len(profile.Capabilities) != 0 || profile.MaxSteps != core.DefaultMaxSteps || profile.MaxToolCalls != core.DefaultMaxToolCalls {
		t.Fatalf("general profile capabilities=%q limits=%d/%d", profile.Capabilities, profile.MaxSteps, profile.MaxToolCalls)
	}
	if !strings.Contains(profile.SystemPrompt(), generalProfilePrompt) {
		t.Fatalf("general profile prompt=%q", profile.SystemPrompt())
	}
}

func TestGeneralCapabilityIDsAreSortedUniqueAndDescribeExecutionChoice(t *testing.T) {
	ids := generalCapabilityIDs()
	if !sort.StringsAreSorted(ids) {
		t.Fatalf("capability ids are not sorted: %q", ids)
	}
	seen := map[string]bool{}
	for _, id := range ids {
		if seen[id] {
			t.Fatalf("duplicate capability id %q", id)
		}
		seen[id] = true
	}
	for _, id := range []string{"program.catalog", "program.execute", "sandbox.exec"} {
		if !seen[id] {
			t.Fatalf("%s missing from general capability source", id)
		}
	}
	if seen["codeptc"] {
		t.Fatal("reserved CodePTC must not be selected by the general profile")
	}
	for _, phrase := range []string{"ordinary tool directly", "program.execute", "deterministic, bounded", "program.catalog", "not exposed", "argv only", "network", "host fallback"} {
		if !strings.Contains(generalProfilePrompt, phrase) {
			t.Fatalf("general execution policy is incomplete (%q): %q", phrase, generalProfilePrompt)
		}
	}
	const programBindingInstruction = "When choosing program.execute, first obtain program bindings from program.catalog and follow the program tool description."
	if !strings.Contains(generalProfilePrompt, programBindingInstruction) {
		t.Fatalf("general execution policy must condition catalog bindings on program execution: %q", generalProfilePrompt)
	}
	if strings.Contains(generalProfilePrompt, " First obtain program bindings from program.catalog") {
		t.Fatalf("general execution policy must not require catalog before direct tool selection: %q", generalProfilePrompt)
	}
}

func TestGeneralProfileControllerKeepsExactImmutableCapabilitySetAcrossRefresh(t *testing.T) {
	registry, global, product, principal := newGeneralProfileTestRegistry(t)
	ids := []string{"rag.search", "memory.remember", "memory.recall", "memory.forget", "notify.send", "notify.targets", "notify.channels"}
	expected := append([]string(nil), ids...)
	sort.Strings(expected)
	controller, err := newGeneralProfileController(registry, global, generalProfileModelSource(&generalModelSettingsRepositoryStub{found: true, configuration: appmodelsettings.StoredConfiguration{Model: "database-model", APIKey: "test-key"}}), ids)
	if err != nil {
		t.Fatal(err)
	}
	ids[0] = "mutated.invalid"
	defer controller.Close()
	if err := controller.Mount(context.Background()); err != nil {
		t.Fatal(err)
	}
	profile, err := registry.Resolve(principal, product, generalProfileID)
	if err != nil || strings.Join(profile.Capabilities, ",") != strings.Join(expected, ",") {
		t.Fatalf("capabilities=%q err=%v", profile.Capabilities, err)
	}
	if !strings.Contains(profile.Description, "recall") || !strings.Contains(profile.SystemPrompt(), "notification") {
		t.Fatalf("general metadata/prompt=%q/%q", profile.Description, profile.SystemPrompt())
	}
	if err := controller.AfterSave(context.Background()); err != nil {
		t.Fatal(err)
	}
	profile, err = registry.Resolve(principal, product, generalProfileID)
	if err != nil || strings.Join(profile.Capabilities, ",") != strings.Join(expected, ",") {
		t.Fatalf("refresh capabilities=%q err=%v", profile.Capabilities, err)
	}
}

func TestGeneralRuntimeUsesGuardedMemoryAndRAGAndFiltersPermissions(t *testing.T) {
	registry, global, product, principal := newGeneralProfileTestRegistry(t)
	principal.TenantID, principal.SubjectID = "tenant", "user"
	principal.Grants = core.NewPermissionSet(core.PermRead, core.PermWrite)
	tenantScope, err := product.Child(core.ScopeRef{Kind: core.ScopeTenant, ID: "tenant"})
	if err != nil {
		t.Fatal(err)
	}
	principal.Scope = tenantScope
	memoryStore := memory.NewSliceStore()
	ragIndex := rag.NewKeywordIndex()
	if err := ragIndex.Ingest(context.Background(), principal.Scope, rag.Document{ID: "doc", Source: "test", Content: "knowledge base answer"}); err != nil {
		t.Fatal(err)
	}
	capabilities := core.NewCapabilityRegistry()
	memoryCapabilities, err := memory.NewStandardCapabilities(memoryStore)
	if err != nil {
		t.Fatal(err)
	}
	for _, capability := range memoryCapabilities {
		if err := capabilities.Register(global, capability); err != nil {
			t.Fatal(err)
		}
	}
	search, err := rag.NewStandardSearchCapability("rag.search", ragIndex)
	if err != nil {
		t.Fatal(err)
	}
	if err := capabilities.Register(global, search); err != nil {
		t.Fatal(err)
	}
	ids := []string{memory.RecallCapabilityID, memory.RememberCapabilityID, memory.ForgetCapabilityID, "rag.search"}
	controller, err := newGeneralProfileController(registry, global, generalProfileModelSource(&generalModelSettingsRepositoryStub{found: true, configuration: appmodelsettings.StoredConfiguration{Model: "test-model", APIKey: "test-key"}}), ids)
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	if err := controller.Mount(context.Background()); err != nil {
		t.Fatal(err)
	}
	sessionScope, err := tenantScope.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "general-tools"})
	if err != nil {
		t.Fatal(err)
	}
	session, err := core.NewSession(core.SessionOptions{ID: "general-tools", ProfileID: generalProfileID, Principal: principal, Scope: sessionScope})
	if err != nil {
		t.Fatal(err)
	}
	model := &generalToolSequenceModel{}
	runtime := &core.Runtime{Capabilities: capabilities, Profiles: registry, Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) { return model, nil }), ToolJournal: immediateToolJournal{}}
	result, err := runtime.RunTurn(context.Background(), principal, session, core.TurnInput{RunID: "general-tools-run", Text: "remember then search"}, nil)
	if err != nil || result.Status != core.RunCompleted || model.steps != 3 {
		t.Fatalf("general runtime result=%#v steps=%d err=%v", result, model.steps, err)
	}
	entries, err := memoryStore.Recall(context.Background(), principal.Scope, "Paris", nil, 10)
	if err != nil || len(entries) != 1 {
		t.Fatalf("general memory store=%#v err=%v", entries, err)
	}
	regular := principal
	regular.Grants = core.NewPermissionSet(core.PermRead)
	readScope, err := tenantScope.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "general-read"})
	if err != nil {
		t.Fatal(err)
	}
	readSession, err := core.NewSession(core.SessionOptions{ID: "general-read", ProfileID: generalProfileID, Principal: regular, Scope: readScope})
	if err != nil {
		t.Fatal(err)
	}
	readModel := &generalToolInventoryModel{}
	readRuntime := &core.Runtime{Capabilities: capabilities, Profiles: registry, Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) { return readModel, nil }), ToolJournal: immediateToolJournal{}}
	if _, err := readRuntime.RunTurn(context.Background(), regular, readSession, core.TurnInput{RunID: "general-read-run", Text: "read only"}, nil); err != nil || strings.Join(readModel.tools, ",") != "memory.recall,rag.search" {
		t.Fatalf("read-only capabilities=%q err=%v", readModel.tools, err)
	}
	adminScope, err := global.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "general-admin"})
	if err != nil {
		t.Fatal(err)
	}
	admin := principal
	admin.Scope = global
	admin.Grants = core.NewPermissionSet(core.PermRead, core.PermWrite, core.PermSend)
	adminProfile, err := registry.Resolve(admin, adminScope, generalProfileID)
	if err != nil {
		t.Fatal(err)
	}
	adminSnapshot, err := (core.CapabilityResolver{Registry: capabilities}).Resolve(admin, adminScope)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := adminProfile.FilterCapabilities(adminSnapshot); err != nil {
		t.Fatalf("global admin profile/capability mismatch: %v", err)
	}
}

func TestGeneralProfileControllerUsesEnvironmentOnlyWhenDatabaseIsAbsent(t *testing.T) {
	t.Setenv("HARNESS_LLM_MODEL", "environment-model")
	registry, global, product, principal := newGeneralProfileTestRegistry(t)
	controller, err := newGeneralProfileController(registry, global, generalProfileModelSource(&generalModelSettingsRepositoryStub{}))
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	if err := controller.Mount(context.Background()); err != nil {
		t.Fatal(err)
	}
	profile, err := registry.Resolve(principal, product, generalProfileID)
	if err != nil {
		t.Fatal(err)
	}
	if profile.Model.Model != "environment-model" {
		t.Fatalf("environment fallback model=%q", profile.Model.Model)
	}
}

func TestGeneralProfileControllerKeepsUnconfiguredProfileVisible(t *testing.T) {
	t.Setenv("HARNESS_LLM_MODEL", "")
	registry, global, product, principal := newGeneralProfileTestRegistry(t)
	controller, err := newGeneralProfileController(registry, global, generalProfileModelSource(&generalModelSettingsRepositoryStub{}))
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	if err := controller.Mount(context.Background()); err != nil {
		t.Fatal(err)
	}
	profiles, err := registry.ListProfiles(product)
	if err != nil || len(profiles) != 1 || profiles[0] != generalProfileID {
		t.Fatalf("visible profiles=%q err=%v", profiles, err)
	}
	if _, err := registry.Resolve(principal, product, generalProfileID); err == nil || !strings.Contains(err.Error(), "model name is empty") {
		t.Fatalf("unconfigured general profile error=%v; want explicit model configuration failure", err)
	}
}

func TestGeneralProfileControllerFailsClosedWhenModelSettingsCannotBeRead(t *testing.T) {
	registry, global, product, _ := newGeneralProfileTestRegistry(t)
	repository := &generalModelSettingsRepositoryStub{err: fmt.Errorf("database is unavailable")}
	controller, err := newGeneralProfileController(registry, global, generalProfileModelSource(repository))
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	if err := controller.Mount(context.Background()); err == nil || !strings.Contains(err.Error(), "database is unavailable") {
		t.Fatalf("mount error=%v; want model-settings read failure", err)
	}
	profiles, err := registry.ListProfiles(product)
	if err != nil || len(profiles) != 0 {
		t.Fatalf("failed startup mounted profiles=%q err=%v", profiles, err)
	}
}

func TestGeneralProfileControllerFailsClosedForPresentInactiveOrInvalidConfiguration(t *testing.T) {
	for name, configuration := range map[string]appmodelsettings.StoredConfiguration{
		"inactive": {APIKey: "", Model: "database-model"},
		"invalid":  {APIKey: "database-key", Model: "invalid\nmodel"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("HARNESS_LLM_MODEL", "environment-model")
			registry, global, product, _ := newGeneralProfileTestRegistry(t)
			controller, err := newGeneralProfileController(registry, global, generalProfileModelSource(&generalModelSettingsRepositoryStub{
				found: true, configuration: configuration,
			}))
			if err != nil {
				t.Fatal(err)
			}
			defer controller.Close()
			if err := controller.Mount(context.Background()); err == nil {
				t.Fatal("present invalid configuration mounted from environment fallback")
			}
			profiles, err := registry.ListProfiles(product)
			if err != nil || len(profiles) != 0 {
				t.Fatalf("invalid database configuration mounted profiles=%q err=%v", profiles, err)
			}
		})
	}
}

func TestGeneralProfileObserverRefreshesWithoutAccumulatingMounts(t *testing.T) {
	registry, global, product, principal := newGeneralProfileTestRegistry(t)
	repository := &generalModelSettingsRepositoryStub{found: true, configuration: appmodelsettings.StoredConfiguration{Model: "old-model", APIKey: "old-key"}}
	controller, err := newGeneralProfileController(registry, global, generalProfileModelSource(repository))
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.Mount(context.Background()); err != nil {
		t.Fatal(err)
	}
	service, err := appmodelsettings.NewService(repository, controller)
	if err != nil {
		t.Fatal(err)
	}
	actor := appidentity.AdminActor{Role: appidentity.RoleAdmin}
	if err := service.Put(context.Background(), appmodelsettings.PutCommand{Actor: actor, Model: "new-model", APIKey: generalStringPtr("new-key")}); err != nil {
		t.Fatal(err)
	}
	profile, err := registry.Resolve(principal, product, generalProfileID)
	if err != nil || profile.Model.Model != "new-model" {
		t.Fatalf("refreshed general model=%q err=%v", profile.Model.Model, err)
	}
	controller.Close()
	profiles, err := registry.ListProfiles(product)
	if err != nil || len(profiles) != 0 {
		t.Fatalf("general layers accumulated after refresh: profiles=%q err=%v", profiles, err)
	}
}

func TestGeneralProfileControllerResolvesForPlatformAdminSession(t *testing.T) {
	registry, global, _, platformAdmin := newGeneralProfileTestRegistry(t)
	session, err := global.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "platform-session"})
	if err != nil {
		t.Fatal(err)
	}
	controller, err := newGeneralProfileController(registry, global, generalProfileModelSource(&generalModelSettingsRepositoryStub{
		found: true, configuration: appmodelsettings.StoredConfiguration{Model: "database-model", APIKey: "test-key"},
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	if err := controller.Mount(context.Background()); err != nil {
		t.Fatal(err)
	}
	profile, err := registry.Resolve(platformAdmin, session, generalProfileID)
	if err != nil {
		t.Fatalf("resolve general profile for platform-admin session: %v", err)
	}
	if profile.Model.Model != "database-model" {
		t.Fatalf("platform-admin general model=%q", profile.Model.Model)
	}
}

func TestGeneralProfileRunUsesPersistedAnthropicProvider(t *testing.T) {
	var calls int
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != http.MethodPost || r.URL.Path != "/v1/messages" || r.Header.Get("x-api-key") != "test-key" {
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		_, _ = w.Write([]byte(`{"content":[{"type":"text","text":"ok"}],"usage":{"input_tokens":1,"output_tokens":1},"stop_reason":"end_turn"}`))
	}))
	defer endpoint.Close()
	registry, global, product, principal := newGeneralProfileTestRegistry(t)
	principal.TenantID = "test"
	repository := &generalModelSettingsRepositoryStub{found: true, configuration: appmodelsettings.StoredConfiguration{
		BaseURL: endpoint.URL, APIKey: "test-key", Model: "anthropic-test", Provider: appmodelsettings.ProviderAnthropic, Protocol: appmodelsettings.ProtocolAnthropicMessages,
	}}
	controller, err := newGeneralProfileController(registry, global, generalProfileModelSource(repository))
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	if err := controller.Mount(context.Background()); err != nil {
		t.Fatal(err)
	}
	sessionScope, err := product.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "anthropic-general"})
	if err != nil {
		t.Fatal(err)
	}
	session, err := core.NewSession(core.SessionOptions{ID: "anthropic-general", ProfileID: generalProfileID, Principal: principal, Scope: sessionScope})
	if err != nil {
		t.Fatal(err)
	}
	runtime := &core.Runtime{Capabilities: core.NewCapabilityRegistry(), Profiles: registry, Models: newLegacyModelResolver(repository), StreamChunks: true}
	if _, err := runtime.RunTurn(context.Background(), principal, session, core.TurnInput{RunID: "run-anthropic-general", Text: "hello"}, nil); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("anthropic calls=%d", calls)
	}
	profile, err := registry.Resolve(principal, product, generalProfileID)
	if err != nil || profile.Model.Provider != string(appmodelsettings.ProviderAnthropic) {
		t.Fatalf("general profile=%#v err=%v", profile, err)
	}
}

func newGeneralProfileTestRegistry(t *testing.T) (*core.AgentProfileRegistry, core.ScopePath, core.ScopePath, core.Principal) {
	t.Helper()
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	deployment, err := global.Child(core.ScopeRef{Kind: core.ScopeDeployment, ID: "test"})
	if err != nil {
		t.Fatal(err)
	}
	product, err := deployment.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "test"})
	if err != nil {
		t.Fatal(err)
	}
	return core.NewAgentProfileRegistry(), global, product, core.Principal{SubjectID: "platform-admin", Scope: global}
}

type generalModelSettingsRepositoryStub struct {
	configuration appmodelsettings.StoredConfiguration
	found         bool
	err           error
}

func (repository *generalModelSettingsRepositoryStub) Load(context.Context) (appmodelsettings.StoredConfiguration, bool, error) {
	return repository.configuration, repository.found, repository.err
}

func (repository *generalModelSettingsRepositoryStub) Save(_ context.Context, configuration appmodelsettings.StoredConfiguration) error {
	if repository.err != nil {
		return repository.err
	}
	repository.configuration = configuration
	repository.found = true
	return nil
}

func generalStringPtr(value string) *string { return &value }

type generalToolSequenceModel struct{ steps int }

func (*generalToolSequenceModel) Provider() string { return "general-tools-test" }
func (m *generalToolSequenceModel) Stream(_ context.Context, _ core.GenerateOptions, emit func(core.StreamChunk)) error {
	calls := []core.ToolCall{
		{ID: "remember", Name: memory.RememberCapabilityID, Args: map[string]any{"key": "city", "content": "Paris"}},
		{ID: "recall", Name: memory.RecallCapabilityID, Args: map[string]any{"query": "Paris"}},
		{ID: "search", Name: "rag.search", Args: map[string]any{"query": "knowledge"}},
	}
	if m.steps >= len(calls) {
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: "done"})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
		return nil
	}
	call := calls[m.steps]
	m.steps++
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &call, ToolCalls: []core.ToolCall{call}})
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
	return nil
}

type immediateToolJournal struct{}

func (immediateToolJournal) BeginToolInvocation(_ context.Context, invocation core.ToolInvocation) (core.ToolInvocationRecord, core.ToolInvocationDecision, error) {
	now := time.Now().UTC()
	return core.ToolInvocationRecord{ToolInvocation: invocation, State: core.ToolInvocationStarted, StartedAt: now, UpdatedAt: now}, core.ToolInvocationExecuteNew, nil
}
func (immediateToolJournal) CompleteToolInvocation(_ context.Context, invocation core.ToolInvocation, result core.CapabilityResult) (core.ToolInvocationRecord, error) {
	now := time.Now().UTC()
	return core.ToolInvocationRecord{ToolInvocation: invocation, State: core.ToolInvocationCompleted, Result: &result, StartedAt: now, UpdatedAt: now, CompletedAt: now}, nil
}
func (immediateToolJournal) MarkToolInvocationUncertain(context.Context, core.ToolInvocation, string) error {
	return nil
}

type generalToolInventoryModel struct{ tools []string }

func (*generalToolInventoryModel) Provider() string { return "general-tools-inventory" }
func (m *generalToolInventoryModel) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	m.tools = make([]string, len(options.Tools))
	for index, tool := range options.Tools {
		m.tools[index] = tool.Name
	}
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: "done"})
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
	return nil
}
