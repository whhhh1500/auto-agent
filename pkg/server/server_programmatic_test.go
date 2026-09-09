package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/whhhh1500/auto-agent/internal/executionroute"
	"github.com/whhhh1500/auto-agent/internal/testdb"
	programtools "github.com/whhhh1500/auto-agent/pkg/adapter/programmatic/toolcapability"
	"github.com/whhhh1500/auto-agent/pkg/app/contextassembly"
	programaccess "github.com/whhhh1500/auto-agent/pkg/app/programmatic"
	"github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/storage"

	_ "modernc.org/sqlite"
)

const (
	programmaticFirstEffectID  = "programmatic.effect_first"
	programmaticSecondEffectID = "programmatic.effect_approved"
)

func TestNativeQueuedProgramApprovalResumesReplacementWithoutReplayingChild(t *testing.T) {
	forProgrammaticServerDatabase(t, func(t *testing.T, database programmaticServerDatabase) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		firstEffects, secondEffects := &atomic.Int32{}, &atomic.Int32{}
		modelAudit := &programmaticModelAudit{}
		first := newProgrammaticServerAPI(t, database.open(t), database.dialect, firstEffects, secondEffects, modelAudit)
		client := &http.Client{Timeout: 10 * time.Second}
		sessionID, run := programmaticSubmit(t, client, first.http.URL)

		if claimed, err := first.server.RunWorkerOnce(ctx, "programmatic-first"); err != nil || !claimed {
			t.Fatalf("initial worker claimed=%t err=%v", claimed, err)
		}
		paused, err := first.queue.GetRun(ctx, run.RunID)
		if err != nil || paused.Status != storage.RunStatusWaitingApproval || firstEffects.Load() != 1 || secondEffects.Load() != 0 {
			t.Fatalf("paused=%#v first=%d second=%d err=%v", paused, firstEffects.Load(), secondEffects.Load(), err)
		}
		pending := programmaticPendingApproval(t, ctx, first.approvals)
		assertProgrammaticHistory(t, first.sessions, ctx, sessionID, run.RunID, false)
		first.close(t)

		// A replacement server reconstructs every SQL-backed collaborator. The
		// HTTP decision calls SQLApprovalStore.DecideApproval, which atomically
		// wakes the server-owned run_control record for the new worker.
		second := newProgrammaticServerAPI(t, database.open(t), database.dialect, firstEffects, secondEffects, modelAudit)
		if err := acceptanceRequest(client, http.MethodPost, second.http.URL+"/v1/admin/approvals/"+pending.ID+"/decision", map[string]any{"decision": "approved"}, nil); err != nil {
			t.Fatal(err)
		}
		if claimed, err := second.server.RunWorkerOnce(ctx, "programmatic-replacement"); err != nil || !claimed {
			t.Fatalf("replacement worker claimed=%t err=%v", claimed, err)
		}
		terminal, err := second.queue.GetRun(ctx, run.RunID)
		if err != nil || terminal.Status != string(core.RunCompleted) || firstEffects.Load() != 1 || secondEffects.Load() != 1 {
			t.Fatalf("terminal=%#v first=%d second=%d err=%v", terminal, firstEffects.Load(), secondEffects.Load(), err)
		}
		assertProgrammaticHistory(t, second.sessions, ctx, sessionID, run.RunID, true)
		assertProgrammaticFinalContext(t, modelAudit)
		if calls := modelAudit.modelCalls.Load(); calls != 3 {
			t.Fatalf("route projection added or skipped a model round: calls=%d, want catalog+execute+final", calls)
		}
		if err := acceptanceRequest(client, http.MethodPost, second.http.URL+"/v1/admin/approvals/"+pending.ID+"/decision", map[string]any{"decision": "approved"}, nil); err != nil {
			t.Fatal(err)
		}
		if claimed, err := second.server.RunWorkerOnce(ctx, "programmatic-duplicate"); err != nil || claimed || firstEffects.Load() != 1 || secondEffects.Load() != 1 {
			t.Fatalf("duplicate claimed=%t first=%d second=%d err=%v", claimed, firstEffects.Load(), secondEffects.Load(), err)
		}
	})
}

func TestNativeQueuedProgramUnknownChildFailsClosedAfterReplacement(t *testing.T) {
	forProgrammaticServerDatabase(t, func(t *testing.T, database programmaticServerDatabase) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		firstEffects, secondEffects := &atomic.Int32{}, &atomic.Int32{}
		first := newProgrammaticServerAPI(t, database.open(t), database.dialect, firstEffects, secondEffects, &programmaticModelAudit{})
		client := &http.Client{Timeout: 10 * time.Second}
		sessionID, run := programmaticSubmit(t, client, first.http.URL)
		if claimed, err := first.server.RunWorkerOnce(ctx, "programmatic-unknown-first"); err != nil || !claimed {
			t.Fatalf("initial worker claimed=%t err=%v", claimed, err)
		}
		pending := programmaticPendingApproval(t, ctx, first.approvals)
		session, err := first.sessions.Load(ctx, sessionID)
		if err != nil {
			t.Fatal(err)
		}
		call := programmaticHistoryCall(t, session, run.RunID, programmaticSecondEffectID)
		invocation, err := core.NewToolInvocation(core.RunInfo{RunID: run.RunID, SessionID: sessionID, Principal: first.principal}, call, false)
		if err != nil {
			t.Fatal(err)
		}
		if _, decision, err := first.journal.BeginToolInvocation(ctx, invocation); err != nil || decision != core.ToolInvocationExecuteNew {
			t.Fatalf("begin unknown invocation decision=%q err=%v", decision, err)
		}
		if err := first.journal.MarkToolInvocationUncertain(ctx, invocation, "test_unknown_effect"); err != nil {
			t.Fatal(err)
		}
		first.close(t)

		second := newProgrammaticServerAPI(t, database.open(t), database.dialect, firstEffects, secondEffects, &programmaticModelAudit{})
		if err := acceptanceRequest(client, http.MethodPost, second.http.URL+"/v1/admin/approvals/"+pending.ID+"/decision", map[string]any{"decision": "approved"}, nil); err != nil {
			t.Fatal(err)
		}
		claimed, runErr := second.server.RunWorkerOnce(ctx, "programmatic-unknown-replacement")
		if !claimed || secondEffects.Load() != 0 || firstEffects.Load() != 1 {
			t.Fatalf("unknown recovery claimed=%t err=%v first=%d second=%d", claimed, runErr, firstEffects.Load(), secondEffects.Load())
		}
		if record, found, err := second.journal.GetToolInvocation(ctx, invocation); err != nil || !found || record.State != core.ToolInvocationUncertain {
			t.Fatalf("unknown journal record=%#v found=%t err=%v", record, found, err)
		}
	})
}

type programmaticServerDatabase struct {
	dialect storage.SQLDialect
	open    func(*testing.T) *sql.DB
}

func forProgrammaticServerDatabase(t *testing.T, run func(*testing.T, programmaticServerDatabase)) {
	t.Helper()
	t.Run("sqlite", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "programmatic-server.db")
		run(t, programmaticServerDatabase{dialect: storage.SQLDialectSQLite, open: func(t *testing.T) *sql.DB {
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			return db
		}})
	})
	t.Run("postgres", func(t *testing.T) {
		open := testdb.Postgres(t)
		run(t, programmaticServerDatabase{dialect: storage.SQLDialectPostgres, open: func(*testing.T) *sql.DB { return open() }})
	})
}

type programmaticServerAPI struct {
	server    *Server
	http      *httptest.Server
	db        *sql.DB
	sessions  *storage.SQLSessionStore
	queue     *storage.SQLRunControlStore
	approvals *storage.SQLApprovalStore
	journal   *storage.SQLToolInvocationJournal
	principal core.Principal
}

func newProgrammaticServerAPI(t *testing.T, db *sql.DB, dialect storage.SQLDialect, firstEffects, secondEffects *atomic.Int32, modelAudit *programmaticModelAudit) *programmaticServerAPI {
	t.Helper()
	ctx := context.Background()
	sessions, err := storage.OpenSQLSessionStore(ctx, db, dialect)
	if err != nil {
		t.Fatal(err)
	}
	queue, err := storage.NewSQLRunControlStore(db, dialect)
	if err != nil {
		t.Fatal(err)
	}
	approvals, err := storage.NewSQLApprovalStore(db, dialect)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := storage.NewSQLToolInvocationJournal(db, dialect)
	if err != nil {
		t.Fatal(err)
	}
	product := core.MustScopePath(core.ScopeRef{Kind: core.ScopeProduct, ID: "programmatic-acceptance"})
	tenant, _ := product.Child(core.ScopeRef{Kind: core.ScopeTenant, ID: "programmatic-tenant"})
	user, _ := tenant.Child(core.ScopeRef{Kind: core.ScopeUser, ID: "programmatic-user"})
	principal := core.Principal{TenantID: "programmatic-tenant", SubjectID: "programmatic-user", Scope: user, Grants: core.NewPermissionSet(core.PermRead, core.PermWrite), Attributes: map[string]string{"role": storage.RoleAccountTenantAdmin}}
	registry := core.NewCapabilityRegistry()
	catalog, err := programtools.NewCatalogCapability(programtools.CatalogID)
	if err != nil {
		t.Fatal(err)
	}
	execute, err := programtools.NewExecuteCapability(programtools.ExecuteID)
	if err != nil {
		t.Fatal(err)
	}
	for _, capability := range []core.Capability{catalog, execute, programmaticEffect{id: programmaticFirstEffectID, revision: "first/v1", calls: firstEffects}, programmaticEffect{id: programmaticSecondEffectID, revision: "second/v1", approval: true, calls: secondEffects}} {
		if err := registry.Register(product, capability); err != nil {
			t.Fatal(err)
		}
	}
	profiles := core.NewAgentProfileRegistry()
	name := "Programmatic queued acceptance"
	model := core.ModelSelection{Provider: "programmatic-queued-fixture", Model: "programmatic-queued-fixture"}
	steps, tools := 8, 8
	if err := profiles.Bind(core.AgentProfileLayer{Scope: product, ProfileID: "programmatic.queued", Name: &name, Model: &model, MaxSteps: &steps, MaxToolCalls: &tools,
		AddCapabilities: []string{programtools.CatalogID, programtools.ExecuteID, programmaticFirstEffectID, programmaticSecondEffectID},
		Metadata: map[string]string{
			executionroute.RouteVersionKey: executionroute.RouteVersion,
			executionroute.RouteModeKey:    string(programaccess.RoutePTCOnly),
		}}); err != nil {
		t.Fatal(err)
	}
	assembler, err := contextassembly.NewAssembler(contextassembly.Config{})
	if err != nil {
		t.Fatal(err)
	}
	runtime := &core.Runtime{Capabilities: registry, Profiles: profiles, Approver: approvals, ToolJournal: journal, ContextAssembler: assembler.AssembleModelContext,
		Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) {
			return &programmaticQueuedModel{audit: modelAudit}, nil
		})}
	server, err := New(Config{Runtime: runtime, Sessions: sessions, Authenticator: AuthenticatorFunc(func(*http.Request) (core.Principal, error) { return principal, nil }), DefaultProfileID: "programmatic.queued",
		Leaser: sessions, LeaseTTL: time.Second, RunControl: queue, RunQueue: queue, Approvals: approvals,
		RunPrincipalResolver: RunPrincipalResolverFunc(func(context.Context, string, string) (core.Principal, error) { return principal, nil }),
		RunWorkerConcurrency: 1, RunWorkerPollInterval: 5 * time.Millisecond, RunWorkerClaimTTL: time.Second, RunWorkerMaxAttempts: 2, MaxWriteDelay: -1})
	if err != nil {
		t.Fatal(err)
	}
	api := &programmaticServerAPI{server: server, http: httptest.NewServer(server.Handler()), db: db, sessions: sessions, queue: queue, approvals: approvals, journal: journal, principal: principal}
	t.Cleanup(func() { api.close(t) })
	return api
}

func (api *programmaticServerAPI) close(t *testing.T) {
	t.Helper()
	if api.http != nil {
		api.http.Close()
		api.http = nil
	}
	if api.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := api.server.Shutdown(ctx); err != nil {
			t.Errorf("shutdown programmatic server: %v", err)
		}
		api.server = nil
	}
	if api.db != nil {
		_ = api.db.Close()
		api.db = nil
	}
}

type programmaticEffect struct {
	id       string
	revision string
	approval bool
	calls    *atomic.Int32
}

func (effect programmaticEffect) Manifest() core.CapabilityManifest {
	schema := map[string]any{"type": "object", "additionalProperties": false}
	return core.CapabilityManifest{ID: effect.id, Version: "1", Name: effect.id, Description: effect.id, Kind: core.KindTool, Contract: "test/programmatic-effect/v1",
		RequiresApproval: effect.approval, Metadata: map[string]string{programaccess.ExposureKey: programaccess.ExposureVersion}, Tool: &core.ToolExposure{Parameters: schema}}
}
func (effect programmaticEffect) ArtifactRevision() string { return effect.revision }
func (effect programmaticEffect) Execute(_ context.Context, request core.CapabilityRequest) (core.CapabilityResult, error) {
	if err := core.RequireAcceptedInvocation(request); err != nil {
		return core.CapabilityResult{}, err
	}
	effect.calls.Add(1)
	return core.CapabilityResult{Content: fmt.Sprintf(`{"effect":%q}`, effect.id), OK: true}, nil
}

type programmaticModelAudit struct {
	mu         sync.Mutex
	finals     [][]core.ChatMessage
	modelCalls atomic.Int32
}

func (audit *programmaticModelAudit) recordFinal(messages []core.ChatMessage) {
	audit.mu.Lock()
	defer audit.mu.Unlock()
	copyOf := make([]core.ChatMessage, len(messages))
	copy(copyOf, messages)
	audit.finals = append(audit.finals, copyOf)
}

func (audit *programmaticModelAudit) lastFinal() []core.ChatMessage {
	audit.mu.Lock()
	defer audit.mu.Unlock()
	if len(audit.finals) == 0 {
		return nil
	}
	copyOf := make([]core.ChatMessage, len(audit.finals[len(audit.finals)-1]))
	copy(copyOf, audit.finals[len(audit.finals)-1])
	return copyOf
}

type programmaticQueuedModel struct{ audit *programmaticModelAudit }

func (*programmaticQueuedModel) Provider() string { return "programmatic-queued-fixture" }
func (model *programmaticQueuedModel) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	model.audit.modelCalls.Add(1)
	for index := len(options.Messages) - 1; index >= 0; index-- {
		message := options.Messages[index]
		if message.Role != core.RoleTool {
			continue
		}
		switch message.ToolCallID {
		case "programmatic-catalog":
			bindings, ok := programmaticCatalogBindings(message.Content)
			if !ok {
				return fmt.Errorf("catalog did not return both effect bindings")
			}
			call := core.ToolCall{ID: "programmatic-root", Name: programtools.ExecuteID, Args: map[string]any{"source": programmaticSource(), "bindings": bindings}}
			emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &call})
			emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
			return nil
		case "programmatic-root":
			model.audit.recordFinal(options.Messages)
			emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: "program complete"})
			emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
			return nil
		}
	}
	call := core.ToolCall{ID: "programmatic-catalog", Name: programtools.CatalogID, Args: map[string]any{}}
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &call})
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
	return nil
}

func programmaticCatalogBindings(content string) (map[string]any, bool) {
	var catalog struct {
		Tools []struct {
			Schema struct {
				Name string `json:"name"`
			} `json:"schema"`
			BindingDigest string `json:"binding_digest"`
		} `json:"tools"`
	}
	if json.Unmarshal([]byte(content), &catalog) != nil {
		return nil, false
	}
	bindings := map[string]any{}
	for _, tool := range catalog.Tools {
		if tool.Schema.Name == programmaticFirstEffectID || tool.Schema.Name == programmaticSecondEffectID {
			bindings[tool.Schema.Name] = tool.BindingDigest
		}
	}
	return bindings, len(bindings) == 2
}

func programmaticSource() string {
	return `{"version":"ptc-ir/v1","body":[` +
		`{"op":"call","assign":"first","tool":"programmatic.effect_first","args":{"op":"map","entries":{}}},` +
		`{"op":"call","assign":"second","tool":"programmatic.effect_approved","args":{"op":"map","entries":{}}},` +
		`{"op":"return","value":{"op":"literal","value":"done"}}]}`
}

func programmaticSubmit(t *testing.T, client *http.Client, base string) (string, storage.RunRecord) {
	t.Helper()
	var session struct {
		ID string `json:"id"`
	}
	if err := acceptanceRequest(client, http.MethodPost, base+"/v1/sessions", map[string]any{"profile_id": "programmatic.queued"}, &session); err != nil {
		t.Fatal(err)
	}
	var run storage.RunRecord
	if err := acceptanceRequest(client, http.MethodPost, base+"/v1/sessions/"+session.ID+"/runs/async", map[string]any{"message": "execute the bounded program"}, &run); err != nil {
		t.Fatal(err)
	}
	return session.ID, run
}

func programmaticPendingApproval(t *testing.T, ctx context.Context, approvals *storage.SQLApprovalStore) storage.ApprovalRecord {
	t.Helper()
	pending, err := approvals.ListApprovals(ctx, storage.ApprovalFilter{TenantID: "programmatic-tenant", Status: core.ApprovalPending})
	if err != nil || len(pending) != 1 || pending[0].CapabilityID != programmaticSecondEffectID {
		t.Fatalf("pending=%#v err=%v", pending, err)
	}
	return pending[0]
}

func programmaticHistoryCall(t *testing.T, session *core.Session, runID, name string) core.ToolCall {
	t.Helper()
	for _, event := range session.Events() {
		if event.RunID != runID || event.Type != core.EvToolCall {
			continue
		}
		var data core.ToolCallData
		if json.Unmarshal(event.Data, &data) == nil && data.Name == name {
			return core.ToolCall{ID: data.CallID, Name: data.Name, Args: data.Args}
		}
	}
	t.Fatalf("tool call %q not found", name)
	return core.ToolCall{}
}

func assertProgrammaticHistory(t *testing.T, sessions *storage.SQLSessionStore, ctx context.Context, sessionID, runID string, completed bool) {
	t.Helper()
	session, err := sessions.Load(ctx, sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if status, found := session.RunStatus(runID); !found || (completed && status != core.RunCompleted) || (!completed && status != core.RunWaitingApproval) {
		t.Fatalf("run status=%q found=%t completed=%t", status, found, completed)
	}
	calls, results := map[string]int{}, map[string]int{}
	starts, resumes, ends := 0, 0, 0
	var start core.RunStartData
	for _, event := range session.Events() {
		if event.RunID != runID {
			continue
		}
		switch event.Type {
		case core.EvRunStart:
			starts++
			if err := json.Unmarshal(event.Data, &start); err != nil {
				t.Fatal(err)
			}
		case core.EvRunResume:
			resumes++
		case core.EvRunEnd:
			ends++
		case core.EvToolCall:
			var data core.ToolCallData
			if json.Unmarshal(event.Data, &data) == nil {
				calls[data.Name]++
			}
		case core.EvToolResult:
			var data core.ToolResultData
			if json.Unmarshal(event.Data, &data) == nil {
				results[data.CallID]++
			}
		}
	}
	if starts != 1 || calls[programtools.CatalogID] != 1 || calls[programtools.ExecuteID] != 1 || calls[programmaticFirstEffectID] != 1 || calls[programmaticSecondEffectID] != 1 {
		t.Fatalf("guarded call history starts=%d calls=%#v", starts, calls)
	}
	if start.Composition == nil || start.Composition.Metadata[executionroute.RouteVersionKey] != executionroute.RouteVersion ||
		start.Composition.Metadata[executionroute.RouteModeKey] != string(programaccess.RoutePTCOnly) ||
		start.Composition.Metadata[executionroute.RouteCatalogToolIDKey] != programtools.CatalogID ||
		start.Composition.Metadata[executionroute.RouteExecuteToolIDKey] != programtools.ExecuteID ||
		start.Composition.Metadata[executionroute.RouteImplementationKey] != executionroute.RouteImplementationRevision {
		t.Fatalf("programmatic route evidence is incomplete: %#v", start)
	}
	if !completed {
		if resumes != 0 || ends != 0 || len(results) != 2 {
			t.Fatalf("paused history resumes=%d ends=%d results=%#v", resumes, ends, results)
		}
		return
	}
	if resumes != 1 || ends != 1 || len(results) != 4 {
		t.Fatalf("completed history resumes=%d ends=%d results=%#v", resumes, ends, results)
	}
}

func assertProgrammaticFinalContext(t *testing.T, audit *programmaticModelAudit) {
	t.Helper()
	messages := audit.lastFinal()
	if len(messages) == 0 {
		t.Fatal("replacement model did not receive the completed program result")
	}
	pairs := map[string]bool{}
	rootResult := false
	for _, message := range messages {
		if message.Role == core.RoleAssistant {
			for _, call := range append([]core.ToolCall{valueOrEmpty(message.ToolCall)}, message.ToolCalls...) {
				if call.ID != "" {
					pairs[call.ID] = true
				}
			}
			continue
		}
		if message.Role != core.RoleTool {
			continue
		}
		if strings.Contains(message.ToolCallID, "/") {
			t.Fatalf("nested program child leaked into final model context: %#v", message)
		}
		if !pairs[message.ToolCallID] {
			t.Fatalf("unpaired tool result reached final model context: %#v", message)
		}
		if message.ToolCallID == "programmatic-root" {
			rootResult = true
		}
	}
	if !rootResult {
		t.Fatalf("final model context omitted paired outer program result: %#v", messages)
	}
}

func valueOrEmpty(call *core.ToolCall) core.ToolCall {
	if call == nil {
		return core.ToolCall{}
	}
	return *call
}
