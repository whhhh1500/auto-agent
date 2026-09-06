package main

import (
	"context"
	"database/sql"
	"sync/atomic"
	"testing"

	graphcheckpoint "github.com/cc-auto-agent/harness-core/pkg/adapter/sql/graphcheckpoint"
	"github.com/cc-auto-agent/harness-core/pkg/adapter/sql/sqlkit"
	"github.com/cc-auto-agent/harness-core/pkg/app/runexecutor"
	"github.com/cc-auto-agent/harness-core/pkg/core"
	graphcontract "github.com/cc-auto-agent/harness-core/pkg/extensions/graph"
	"github.com/cc-auto-agent/harness-core/pkg/storage"

	_ "modernc.org/sqlite"
)

func TestRunExecutorCompositionRegistersSequentialAndGraph(t *testing.T) {
	db, err := sql.Open("sqlite", "file:"+t.TempDir()+"/composition.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := storage.OpenSQLSessionStore(context.Background(), db, storage.SQLDialectSQLite); err != nil {
		t.Fatal(err)
	}
	approvals, err := storage.NewSQLApprovalStore(db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := newRunExecutorRegistry(db, storage.SQLDialectSQLite, approvals, nil)
	if err != nil {
		t.Fatal(err)
	}
	items := registry.List()
	if len(items) != 2 || items[0].ID != "graph-core-turn" || items[1].ID != runexecutor.SequentialID {
		t.Fatalf("registry=%#v", items)
	}
	if _, meta, err := registry.ResolveOrDefault("", "", runexecutor.Dependencies{Runtime: testCompositionRuntime(t)}); err != nil || meta.ID != runexecutor.SequentialID {
		t.Fatalf("default=%#v err=%v", meta, err)
	}
	if _, _, err := registry.Resolve("unknown", "1", runexecutor.Dependencies{Runtime: testCompositionRuntime(t)}); err == nil {
		t.Fatal("unknown executor accepted")
	}
	product := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"}, core.ScopeRef{Kind: core.ScopeProduct, ID: "product"})
	principal := core.Principal{TenantID: "tenant", SubjectID: "subject", Scope: product}
	scope, err := product.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "composition-session"})
	if err != nil {
		t.Fatal(err)
	}
	session, err := core.NewSession(core.SessionOptions{ID: "composition-session", ProfileID: "general", Principal: principal, Scope: scope})
	if err != nil {
		t.Fatal(err)
	}
	profileRuntime := testCompositionRuntime(t)
	profile, err := profileRuntime.Profiles.Resolve(principal, scope, "general")
	if err != nil {
		t.Fatal(err)
	}
	if profile.Metadata["harness.executor.id"] != "graph-core-turn" {
		t.Fatalf("profile=%v", profile.Metadata)
	}
	executor, meta, err := registry.Resolve("graph-core-turn", "1", runexecutor.Dependencies{Runtime: profileRuntime})
	if err != nil || meta.ImplementationRevision == "" {
		t.Fatalf("resolve meta=%#v err=%v", meta, err)
	}
	result, err := executor.RunTurn(context.Background(), principal, session, core.TurnInput{RunID: "composition-run", Text: "hello"}, nil)
	if err != nil || result.Status != core.RunCompleted {
		t.Fatalf("result=%#v err=%v", result, err)
	}
}

func TestRunExecutorCompositionApprovalSurvivesSQLiteRebuild(t *testing.T) {
	ctx := context.Background()
	dbPath := t.TempDir() + "/restart.db"
	open := func() (*sql.DB, *storage.SQLSessionStore, *storage.SQLApprovalStore, *runexecutor.Registry) {
		db, err := sql.Open("sqlite", "file:"+dbPath)
		if err != nil {
			t.Fatal(err)
		}
		sessions, err := storage.OpenSQLSessionStore(ctx, db, storage.SQLDialectSQLite)
		if err != nil {
			db.Close()
			t.Fatal(err)
		}
		approvals, err := storage.NewSQLApprovalStore(db, storage.SQLDialectSQLite)
		if err != nil {
			db.Close()
			t.Fatal(err)
		}
		registry, err := newRunExecutorRegistry(db, storage.SQLDialectSQLite, approvals, nil)
		if err != nil {
			db.Close()
			t.Fatal(err)
		}
		return db, sessions, approvals, registry
	}
	db, sessions, approvals, registry := open()
	model := &compositionApprovalModel{}
	runtimeBundle := testApprovalCompositionRuntime(t, approvals, model)
	runtime := runtimeBundle.Runtime
	principal := runtimeBundle.principal
	scope, err := principal.Scope.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "composition-approval-session"})
	if err != nil {
		t.Fatal(err)
	}
	session, err := core.NewSession(core.SessionOptions{ID: "composition-approval-session", ProfileID: "general", Principal: principal, Scope: scope})
	if err != nil {
		t.Fatal(err)
	}
	if err := sessions.Create(ctx, session); err != nil {
		t.Fatal(err)
	}
	runs, err := storage.NewSQLRunControlStore(db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	if err := runs.CreateRun(ctx, storage.RunRecord{RunID: "composition-approval-run", SessionID: session.ID(), TenantID: principal.TenantID, SubjectID: principal.SubjectID}); err != nil {
		t.Fatal(err)
	}
	executor, meta, err := registry.Resolve("graph-core-turn", "1", runexecutor.Dependencies{Runtime: runtime})
	if err != nil || meta.ImplementationRevision != "graph-core-turn-v1" {
		t.Fatalf("resolve meta=%#v err=%v", meta, err)
	}
	first, err := executor.RunTurn(ctx, principal, session, core.TurnInput{RunID: "composition-approval-run", Text: "approve"}, nil)
	if err != nil || first.Status != core.RunWaitingApproval {
		t.Fatalf("first=%#v err=%v", first, err)
	}
	if err := sessions.Save(ctx, session, 0); err != nil {
		t.Fatal(err)
	}
	pending, ok, err := session.PendingApproval("composition-approval-run")
	if err != nil || !ok {
		t.Fatalf("pending=%#v ok=%v err=%v", pending, ok, err)
	}
	if paused, err := runs.PauseRun(ctx, "composition-approval-run", "approval pending", 3, core.TelemetryTraceContext{}); err != nil || !paused {
		t.Fatalf("pause run paused=%v err=%v", paused, err)
	}
	cpStore, err := graphcheckpoint.New(db, sqlkit.SQLite)
	if err != nil {
		t.Fatal(err)
	}
	key := graphcontract.CheckpointKey{TenantID: principal.TenantID, SessionID: session.ID(), RunID: "composition-approval-run"}
	cp1, err := cpStore.Load(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if cp1.Status != graphcontract.CheckpointWaitingApproval || cp1.SegmentID == "" || cp1.HostGeneration == 0 {
		t.Fatalf("checkpoint=%#v", cp1)
	}
	if _, changed, err := approvals.DecideApproval(ctx, pending.ApprovalID, core.ApprovalApproved, "operator"); err != nil || !changed {
		t.Fatalf("decide changed=%v err=%v", changed, err)
	}
	db.Close()

	db, sessions, approvals, registry = open()
	defer db.Close()
	restored, err := sessions.Load(ctx, session.ID())
	if err != nil {
		t.Fatal(err)
	}
	runtimeBundle = testApprovalCompositionRuntime(t, approvals, model)
	runtime = runtimeBundle.Runtime
	executor, _, err = registry.Resolve("graph-core-turn", "1", runexecutor.Dependencies{Runtime: runtime})
	if err != nil {
		t.Fatal(err)
	}
	second, err := executor.ResumeTurn(ctx, principal, restored, core.ResumeInput{RunID: "composition-approval-run"}, nil)
	if err != nil || second.Status != core.RunCompleted {
		t.Fatalf("resume=%#v err=%v", second, err)
	}
	if model.calls.Load() != 2 {
		t.Fatalf("model calls=%d, want initial+resume only", model.calls.Load())
	}
	if model.capabilityCalls.Load() != 1 {
		t.Fatalf("capability calls=%d, want 1", model.capabilityCalls.Load())
	}
	cp2, err := graphcheckpoint.New(db, sqlkit.SQLite)
	if err != nil {
		t.Fatal(err)
	}
	final, err := cp2.Load(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if final.Status != graphcontract.CheckpointCompleted || final.Revision <= cp1.Revision || final.SegmentID == cp1.SegmentID || final.HostGeneration <= cp1.HostGeneration {
		t.Fatalf("checkpoint progression before=%#v after=%#v", cp1, final)
	}
}

type compositionApprovalModel struct {
	calls, capabilityCalls atomic.Int32
}

func (m *compositionApprovalModel) Provider() string         { return "composition-approval" }
func (m *compositionApprovalModel) ArtifactRevision() string { return "composition-approval/v1" }
func (m *compositionApprovalModel) Stream(ctx context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	m.calls.Add(1)
	if len(options.Messages) > 0 && options.Messages[len(options.Messages)-1].Role == core.RoleTool {
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: "approved"})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
		return nil
	}
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &core.ToolCall{ID: "call-composition", Name: "composition.approve", Args: map[string]any{"ok": true}}})
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
	return nil
}

type compositionApprovalCapability struct {
	calls *atomic.Int32
}

func (c compositionApprovalCapability) Manifest() core.CapabilityManifest {
	return core.CapabilityManifest{ID: "composition.approve", Version: "1", Name: "composition.approve", Kind: core.KindTool, Contract: "harness.tool/v1", Tool: &core.ToolExposure{}, RequiresApproval: true}
}
func (c compositionApprovalCapability) Execute(context.Context, core.CapabilityRequest) (core.CapabilityResult, error) {
	c.calls.Add(1)
	return core.CapabilityResult{Content: `{"ok":true}`, OK: true}, nil
}

type compositionTestRuntime struct {
	*core.Runtime
	principal core.Principal
}

func testApprovalCompositionRuntime(t *testing.T, approvals core.Approver, model *compositionApprovalModel) *compositionTestRuntime {
	t.Helper()
	product := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"}, core.ScopeRef{Kind: core.ScopeProduct, ID: "product"})
	user := product
	user, _ = user.Child(core.ScopeRef{Kind: core.ScopeUser, ID: "composition-user"})
	principal := core.Principal{TenantID: "composition-tenant", SubjectID: "composition-user", Scope: user}
	calls := &model.capabilityCalls
	capabilities := core.NewCapabilityRegistry()
	if err := capabilities.Register(product, compositionApprovalCapability{calls: calls}); err != nil {
		t.Fatal(err)
	}
	profiles := core.NewAgentProfileRegistry()
	selection := core.ModelSelection{Provider: "composition-approval", Model: "composition-approval"}
	if err := profiles.Bind(core.AgentProfileLayer{Scope: product, ProfileID: "general", Model: &selection, AddCapabilities: []string{"composition.approve"}, Metadata: map[string]string{"harness.executor.id": "graph-core-turn", "harness.executor.version": "1"}}); err != nil {
		t.Fatal(err)
	}
	return &compositionTestRuntime{Runtime: &core.Runtime{Capabilities: capabilities, Profiles: profiles, Approver: approvals, Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) { return model, nil })}, principal: principal}
}

func testCompositionRuntime(t *testing.T) *core.Runtime {
	t.Helper()
	profiles := core.NewAgentProfileRegistry()
	product := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"}, core.ScopeRef{Kind: core.ScopeProduct, ID: "product"})
	sel := core.ModelSelection{Provider: "mock", Model: "mock"}
	if err := profiles.Bind(core.AgentProfileLayer{Scope: product, ProfileID: "general", Model: &sel, Metadata: map[string]string{"harness.executor.id": "graph-core-turn", "harness.executor.version": "1"}}); err != nil {
		t.Fatal(err)
	}
	return &core.Runtime{Profiles: profiles, Capabilities: core.NewCapabilityRegistry(), Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) { return core.MockLlmAdapter{}, nil })}
}
