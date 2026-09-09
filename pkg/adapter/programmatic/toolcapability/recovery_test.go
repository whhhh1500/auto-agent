package toolcapability

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/whhhh1500/auto-agent/internal/testdb"
	access "github.com/whhhh1500/auto-agent/pkg/app/programmatic"
	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/storage"

	_ "modernc.org/sqlite"
)

func TestProgramResumeAfterSQLRestoreDoesNotReplayCompletedChild(t *testing.T) {
	forProgramRecoveryDatabase(t, func(t *testing.T, database programRecoveryDatabase) {
		fixture := newProgramRecoveryFixture(t, database, []string{"effect.first", "effect.approved"}, 8)
		paused := fixture.pause(t)
		fixture.persist(t, paused)
		restored := fixture.restore(t)
		approval := pendingApproval(t, restored, fixture.runID)
		fixture.approve(t, approval)

		// Construct a new Runtime after loading the durable SQL session. The first
		// child has a ToolResult in the projected history, so nestedToolInvoker must
		// return it rather than replaying its non-idempotent provider.
		runtime := fixture.runtime(&programRecoveryModel{})
		result, err := runtime.ResumeTurn(context.Background(), fixture.principal, restored, core.ResumeInput{RunID: fixture.runID}, nil)
		if err != nil || result.Status != core.RunCompleted || fixture.effect("effect.first").calls.Load() != 1 || fixture.effect("effect.approved").calls.Load() != 1 {
			t.Fatalf("restored resume=%#v err=%v first=%d approved=%d", result, err, fixture.effect("effect.first").calls.Load(), fixture.effect("effect.approved").calls.Load())
		}
		if status, _ := restored.RunStatus(fixture.runID); status != core.RunCompleted {
			t.Fatalf("restored run status=%q", status)
		}
	})
}

func TestProgramResumeRejectsChangedOrRevokedBindingBeforePendingChildEffect(t *testing.T) {
	forProgramRecoveryDatabase(t, func(t *testing.T, database programRecoveryDatabase) {
		for _, test := range []struct {
			name    string
			mutate  func(*programRecoveryFixture) error
			wantErr bool
		}{
			{
				name: "changed binding digest",
				mutate: func(f *programRecoveryFixture) error {
					replacement := newProgramEffect("effect.approved", "effect.approved/v2", true)
					return f.registry.Bind(core.CapabilityBinding{Scope: f.product, Mode: core.BindingReplace, Manifest: replacement.Manifest(), Provider: replacement})
				},
			},
			{
				name: "revoked capability",
				mutate: func(f *programRecoveryFixture) error {
					return f.registry.Bind(core.CapabilityBinding{Scope: f.product, Mode: core.BindingDisable, Manifest: core.CapabilityManifest{ID: "effect.approved", Version: "1"}})
				},
				wantErr: true,
			},
		} {
			t.Run(test.name, func(t *testing.T) {
				fixture := newProgramRecoveryFixture(t, database, []string{"effect.first", "effect.approved"}, 8)
				paused := fixture.pause(t)
				fixture.persist(t, paused)
				restored := fixture.restore(t)
				approval := pendingApproval(t, restored, fixture.runID)
				fixture.approve(t, approval)
				if err := test.mutate(fixture); err != nil {
					t.Fatal(err)
				}

				result, err := fixture.runtime(&programRecoveryModel{}).ResumeTurn(context.Background(), fixture.principal, restored, core.ResumeInput{RunID: fixture.runID}, nil)
				if (err != nil) != test.wantErr || fixture.effect("effect.first").calls.Load() != 1 || fixture.effect("effect.approved").calls.Load() != 0 {
					t.Fatalf("resume=%#v err=%v first=%d approved=%d", result, err, fixture.effect("effect.first").calls.Load(), fixture.effect("effect.approved").calls.Load())
				}
				if test.wantErr {
					if status, _ := restored.RunStatus(fixture.runID); status != core.RunFailed {
						t.Fatalf("revoked resume status=%q", status)
					}
					return
				}
				if !hasToolResultCode(t, restored, fixture.runID, "program-root", "program_bindings_mismatch") {
					t.Fatalf("changed binding did not return a program binding denial: %#v", restored.Events())
				}
			})
		}
	})
}

func TestProgramResumeFailsClosedForUnknownPendingChild(t *testing.T) {
	forProgramRecoveryDatabase(t, func(t *testing.T, database programRecoveryDatabase) {
		fixture := newProgramRecoveryFixture(t, database, []string{"effect.first", "effect.approved", "effect.after"}, 8)
		paused := fixture.pause(t)
		fixture.persist(t, paused)
		restored := fixture.restore(t)
		approval := pendingApproval(t, restored, fixture.runID)
		child := findToolCall(t, restored, fixture.runID, "effect.approved")
		invocation, err := core.NewToolInvocation(core.RunInfo{RunID: fixture.runID, SessionID: restored.ID(), Principal: fixture.principal}, child, false)
		if err != nil {
			t.Fatal(err)
		}
		if _, decision, err := fixture.journal.BeginToolInvocation(context.Background(), invocation); err != nil || decision != core.ToolInvocationExecuteNew {
			t.Fatalf("begin uncertain child decision=%q err=%v", decision, err)
		}
		if err := fixture.journal.MarkToolInvocationUncertain(context.Background(), invocation, "simulated_crash_after_effect"); err != nil {
			t.Fatal(err)
		}
		fixture.approve(t, approval)

		result, err := fixture.runtime(&programRecoveryModel{}).ResumeTurn(context.Background(), fixture.principal, restored, core.ResumeInput{RunID: fixture.runID}, nil)
		if err != nil || result.Status != core.RunCompleted || fixture.effect("effect.first").calls.Load() != 1 || fixture.effect("effect.approved").calls.Load() != 0 || fixture.effect("effect.after").calls.Load() != 0 {
			t.Fatalf("unknown-child resume=%#v err=%v first=%d approved=%d after=%d", result, err, fixture.effect("effect.first").calls.Load(), fixture.effect("effect.approved").calls.Load(), fixture.effect("effect.after").calls.Load())
		}
		if root := latestToolResult(t, restored, fixture.runID, "program-root"); root.OK {
			t.Fatalf("unknown child unexpectedly produced a successful program result: %#v", root)
		}
		if record, found, err := fixture.journal.GetToolInvocation(context.Background(), invocation); err != nil || !found || record.State != core.ToolInvocationUncertain {
			t.Fatalf("unknown child journal record=%#v found=%t err=%v", record, found, err)
		}
	})
}

func TestProgramNestedCallsConsumeOuterToolBudget(t *testing.T) {
	forProgramRecoveryDatabase(t, func(t *testing.T, database programRecoveryDatabase) {
		fixture := newProgramRecoveryFixture(t, database, []string{"effect.first", "effect.approved"}, 2)
		result, err := fixture.runtime(&programRecoveryModel{root: fixture.rootCall()}).RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{RunID: fixture.runID, Text: "run bounded program", MaxToolCallsOverride: 2}, nil)
		if err != nil || result.Status != core.RunCompleted || fixture.effect("effect.first").calls.Load() != 1 || fixture.effect("effect.approved").calls.Load() != 0 {
			t.Fatalf("budget result=%#v err=%v first=%d second=%d", result, err, fixture.effect("effect.first").calls.Load(), fixture.effect("effect.approved").calls.Load())
		}
		denied := findToolCall(t, fixture.session, fixture.runID, "effect.approved")
		if !hasToolResultCode(t, fixture.session, fixture.runID, denied.ID, core.CodeBudgetExceeded) {
			t.Fatalf("nested budget denial missing from child tool result: %#v", latestToolResult(t, fixture.session, fixture.runID, denied.ID))
		}
	})
}

type programRecoveryFixture struct {
	db        *sql.DB
	store     *storage.SQLSessionStore
	journal   *storage.SQLToolInvocationJournal
	approvals *storage.SQLApprovalStore
	approver  *programRecoveryApprover
	registry  *core.CapabilityRegistry
	profiles  *core.AgentProfileRegistry
	principal core.Principal
	product   core.ScopePath
	session   *core.Session
	runID     string
	program   string
	bindings  map[string]any
	effects   map[string]*programEffect
}

var programRecoveryFixtureSequence atomic.Int64

type programRecoveryDatabase struct {
	dialect storage.SQLDialect
	open    func(*testing.T) *sql.DB
}

func forProgramRecoveryDatabase(t *testing.T, test func(*testing.T, programRecoveryDatabase)) {
	t.Helper()
	t.Run("sqlite", func(t *testing.T) {
		test(t, programRecoveryDatabase{dialect: storage.SQLDialectSQLite, open: func(t *testing.T) *sql.DB {
			db, err := sql.Open("sqlite", ":memory:")
			if err != nil {
				t.Fatal(err)
			}
			db.SetMaxOpenConns(1)
			return db
		}})
	})
	t.Run("postgres", func(t *testing.T) {
		open := testdb.Postgres(t)
		test(t, programRecoveryDatabase{dialect: storage.SQLDialectPostgres, open: func(*testing.T) *sql.DB { return open() }})
	})
}

func newProgramRecoveryFixture(t *testing.T, database programRecoveryDatabase, tools []string, maxCalls int) *programRecoveryFixture {
	t.Helper()
	ctx := context.Background()
	sequence := programRecoveryFixtureSequence.Add(1)
	sessionID := fmt.Sprintf("program-recovery-session-%d", sequence)
	runID := fmt.Sprintf("program-recovery-run-%d", sequence)
	db := database.open(t)
	t.Cleanup(func() { _ = db.Close() })
	store, err := storage.OpenSQLSessionStore(ctx, db, database.dialect)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := storage.NewSQLToolInvocationJournal(db, database.dialect)
	if err != nil {
		t.Fatal(err)
	}
	approvals, err := storage.NewSQLApprovalStore(db, database.dialect)
	if err != nil {
		t.Fatal(err)
	}
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product, err := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "program-recovery"})
	if err != nil {
		t.Fatal(err)
	}
	sessionScope, err := product.Child(core.ScopeRef{Kind: core.ScopeSession, ID: sessionID})
	if err != nil {
		t.Fatal(err)
	}
	principal := core.Principal{SubjectID: "subject", TenantID: "tenant", Scope: product, Grants: core.NewPermissionSet(core.PermRead, core.PermWrite, core.PermSend)}
	session, err := core.NewSession(core.SessionOptions{ID: sessionID, ProfileID: "program-recovery", Principal: principal, Scope: sessionScope})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Create(ctx, session); err != nil {
		t.Fatal(err)
	}
	registry := core.NewCapabilityRegistry()
	catalog, err := NewCatalogCapability(CatalogID)
	if err != nil {
		t.Fatal(err)
	}
	execute, err := NewExecuteCapability(ExecuteID)
	if err != nil {
		t.Fatal(err)
	}
	for _, capability := range []core.Capability{catalog, execute} {
		if err := registry.Register(product, capability); err != nil {
			t.Fatal(err)
		}
	}
	effects := make(map[string]*programEffect, len(tools))
	for index, id := range tools {
		effect := newProgramEffect(id, id+"/v1", index == 1)
		effects[id] = effect
		if err := registry.Register(product, effect); err != nil {
			t.Fatal(err)
		}
	}
	profiles := core.NewAgentProfileRegistry()
	ids := append([]string{CatalogID, ExecuteID}, tools...)
	model := core.ModelSelection{Provider: "program-recovery", Model: "program-recovery"}
	limit := maxCalls
	if err := profiles.Bind(core.AgentProfileLayer{Scope: product, ProfileID: "program-recovery", Model: &model, AddCapabilities: ids, MaxToolCalls: &limit}); err != nil {
		t.Fatal(err)
	}
	fixture := &programRecoveryFixture{db: db, store: store, journal: journal, approvals: approvals, approver: &programRecoveryApprover{store: approvals}, registry: registry, profiles: profiles, principal: principal, product: product, session: session, runID: runID, effects: effects}
	fixture.program, fixture.bindings = fixture.programArguments(t, tools)
	return fixture
}

func (f *programRecoveryFixture) runtime(model core.LlmAdapter) *core.Runtime {
	return &core.Runtime{Capabilities: f.registry, Profiles: f.profiles, Approver: f.approver, ToolJournal: f.journal, Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) { return model, nil })}
}

func (f *programRecoveryFixture) rootCall() core.ToolCall {
	return core.ToolCall{ID: "program-root", Name: ExecuteID, Args: map[string]any{"source": f.program, "bindings": f.bindings}}
}

func (f *programRecoveryFixture) pause(t *testing.T) *core.Session {
	t.Helper()
	result, err := f.runtime(&programRecoveryModel{root: f.rootCall()}).RunTurn(context.Background(), f.principal, f.session, core.TurnInput{RunID: f.runID, Text: "run program"}, nil)
	if err != nil || result.Status != core.RunWaitingApproval || f.effect("effect.first").calls.Load() != 1 || f.effect("effect.approved").calls.Load() != 0 {
		t.Fatalf("pause result=%#v err=%v first=%d approved=%d events=%#v", result, err, f.effect("effect.first").calls.Load(), f.effect("effect.approved").calls.Load(), f.session.Events())
	}
	return f.session
}

func (f *programRecoveryFixture) persist(t *testing.T, session *core.Session) {
	t.Helper()
	if err := f.store.Save(context.Background(), session, 0); err != nil {
		t.Fatal(err)
	}
}

func (f *programRecoveryFixture) restore(t *testing.T) *core.Session {
	t.Helper()
	session, err := f.store.Load(context.Background(), f.session.ID())
	if err != nil {
		t.Fatal(err)
	}
	return session
}

func (f *programRecoveryFixture) effect(id string) *programEffect { return f.effects[id] }

func (f *programRecoveryFixture) approve(t *testing.T, pending core.ApprovalRequestedData) {
	t.Helper()
	record, err := f.approvals.GetApproval(context.Background(), pending.ApprovalID)
	if err != nil || record.Status != core.ApprovalPending {
		t.Fatalf("persisted approval record=%#v err=%v", record, err)
	}
	f.approver.approved.Store(true)
}

func (f *programRecoveryFixture) programArguments(t *testing.T, tools []string) (string, map[string]any) {
	t.Helper()
	snapshot, err := (core.CapabilityResolver{Registry: f.registry}).Resolve(f.principal, f.session.Scope())
	if err != nil {
		t.Fatal(err)
	}
	profile, err := f.profiles.Resolve(f.principal, f.session.Scope(), f.session.ProfileID())
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err = profile.FilterCapabilities(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := access.Project(snapshot.Capabilities())
	if err != nil {
		t.Fatal(err)
	}
	descriptors := catalog.Descriptors()
	digests := make(map[string]any, len(tools))
	for _, id := range tools {
		found := false
		for _, descriptor := range descriptors {
			if descriptor.Schema.Name == id {
				digests[id] = descriptor.BindingDigest
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("program catalog omitted %q: %#v", id, descriptors)
		}
	}
	body := make([]any, 0, len(tools)+1)
	for index, id := range tools {
		body = append(body, map[string]any{"op": "call", "assign": fmt.Sprintf("result%d", index), "tool": id, "args": map[string]any{"op": "map", "entries": map[string]any{}}})
	}
	body = append(body, map[string]any{"op": "return", "value": map[string]any{"op": "literal", "value": "done"}})
	encoded, err := json.Marshal(map[string]any{"version": "ptc-ir/v1", "body": body})
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded), digests
}

type programEffect struct {
	manifest core.CapabilityManifest
	revision string
	calls    atomic.Int32
}

func newProgramEffect(id, revision string, approval bool) *programEffect {
	return &programEffect{manifest: core.CapabilityManifest{
		ID: id, Version: "1", Name: id, Description: id, Kind: core.KindTool, Contract: "test/program-effect/v1",
		Metadata:         map[string]string{access.ExposureKey: access.ExposureVersion},
		RequiresApproval: approval,
		Tool:             &core.ToolExposure{Description: id, Parameters: map[string]any{"type": "object", "additionalProperties": false}},
	}, revision: revision}
}

func (e *programEffect) Manifest() core.CapabilityManifest { return e.manifest }
func (e *programEffect) ArtifactRevision() string          { return e.revision }
func (e *programEffect) Execute(context.Context, core.CapabilityRequest) (core.CapabilityResult, error) {
	e.calls.Add(1)
	return core.CapabilityResult{Content: `{"ok":true}`, OK: true}, nil
}

type programRecoveryModel struct {
	root  core.ToolCall
	calls atomic.Int32
}

func (*programRecoveryModel) Provider() string { return "program-recovery" }
func (m *programRecoveryModel) Stream(_ context.Context, _ core.GenerateOptions, emit func(core.StreamChunk)) error {
	if m.calls.Add(1) == 1 && m.root.Name != "" {
		call := m.root
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &call, ToolCalls: []core.ToolCall{call}})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
		return nil
	}
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: "complete"})
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
	return nil
}

// programRecoveryApprover persists the requested approval through the real SQL
// store. SQLApprovalStore.DecideApproval also moves server-owned run_control
// rows, which direct Runtime.RunTurn deliberately does not create; the test
// supplies the later decision through this durable-approver seam instead.
type programRecoveryApprover struct {
	store    *storage.SQLApprovalStore
	approved atomic.Bool
}

func (a *programRecoveryApprover) Approve(ctx context.Context, request core.ApprovalRequest) (core.ApprovalDecision, error) {
	resolution, err := a.RequestApproval(ctx, request)
	return resolution.Decision, err
}

func (a *programRecoveryApprover) RequestApproval(ctx context.Context, request core.ApprovalRequest) (core.ApprovalResolution, error) {
	resolution, err := a.store.RequestApproval(ctx, request)
	if err == nil && a.approved.Load() {
		resolution.Decision = core.ApprovalApproved
	}
	return resolution, err
}

func pendingApproval(t *testing.T, session *core.Session, runID string) core.ApprovalRequestedData {
	t.Helper()
	pending, ok, err := session.PendingApproval(runID)
	if err != nil || !ok {
		t.Fatalf("pending approval=%#v ok=%t err=%v", pending, ok, err)
	}
	return pending
}

func findToolCall(t *testing.T, session *core.Session, runID, name string) core.ToolCall {
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

func latestToolResult(t *testing.T, session *core.Session, runID, callID string) core.ToolResultData {
	t.Helper()
	var found *core.ToolResultData
	for _, event := range session.Events() {
		if event.RunID != runID || event.Type != core.EvToolResult {
			continue
		}
		var data core.ToolResultData
		if json.Unmarshal(event.Data, &data) == nil && data.CallID == callID {
			copyOf := data
			found = &copyOf
		}
	}
	if found == nil {
		t.Fatalf("tool result %q not found", callID)
	}
	return *found
}

func hasToolResultCode(t *testing.T, session *core.Session, runID, callID, code string) bool {
	t.Helper()
	result := latestToolResult(t, session, runID, callID)
	got, _ := result.Metadata["code"].(string)
	return got == code
}
