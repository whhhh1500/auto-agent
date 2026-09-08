package server

import (
	"context"
	"database/sql"
	"sync/atomic"
	"testing"

	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/storage"
)

type nativeStrictWitnessFixture struct {
	api        *Server
	db         *sql.DB
	accounts   *storage.SQLAccountStore
	session    *core.Session
	runID      string
	toolCalls  *atomic.Int32
	modelCalls *atomic.Int32
}

func newNativeStrictWitnessFixture(t *testing.T, disableAccount bool) *nativeStrictWitnessFixture {
	t.Helper()
	ctx := context.Background()
	db := openNativeStrictTestDB(t)
	if _, err := storage.OpenSQLSessionStore(ctx, db, storage.SQLDialectSQLite); err != nil {
		t.Fatal(err)
	}
	accounts, err := storage.NewSQLAccountStore(db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	if err := accounts.CreateTenant(ctx, "acme", "Acme"); err != nil {
		t.Fatal(err)
	}
	if err := accounts.CreateAccount(ctx, storage.Account{
		AccountID: "alice", Email: "alice@example.test", Role: storage.RoleAccountUser,
		TenantID: "acme", Status: storage.AccountActive,
	}, "native-password"); err != nil {
		t.Fatal(err)
	}
	root := nativeStrictTestRoot()
	model := core.ModelSelection{Provider: "checkpoint-test", Model: "native-witness"}
	name := "Native witness"
	var toolCalls, modelCalls atomic.Int32
	var adapter core.LlmAdapter = checkpointTestModel{calls: &modelCalls}
	if disableAccount {
		adapter = disablingCheckpointModel{accounts: accounts, inner: checkpointTestModel{calls: &modelCalls}}
	}
	api, err := NewNativeStrictServer(ctx, NativeStrictServerConfig{
		DB: db, Dialect: storage.SQLDialectSQLite,
		Bootstrap: NativeStrictBootstrap{
			Revision: "native-witness-v1", Root: root.Segments(), DefaultProfileID: "native.witness",
			Profiles: []core.AgentProfileLayer{{
				Scope: root, ProfileID: "native.witness", Name: &name, Model: &model,
				AddCapabilities: []string{"checkpoint.write"},
			}},
			Capabilities: []NativeStrictCapability{{Scope: root, Capability: checkpointTestTool{calls: &toolCalls}}},
			Model:        NativeStrictModel{Selection: model, Adapter: adapter},
		},
		MaxWriteDelay: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = api.Shutdown(context.Background()) })
	account, err := accounts.GetAccount(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	principal, err := storage.PrincipalForAccount(account, root.Segments())
	if err != nil {
		t.Fatal(err)
	}
	scope, err := principal.Scope.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "session-native-witness"})
	if err != nil {
		t.Fatal(err)
	}
	session, err := core.NewSession(core.SessionOptions{ID: "session-native-witness", ProfileID: "native.witness", Principal: principal, Scope: scope})
	if err != nil {
		t.Fatal(err)
	}
	if err := api.sessions.Create(ctx, session); err != nil {
		t.Fatal(err)
	}
	runID := "run-native-witness"
	if err := api.runQueue.EnqueueRun(ctx, storage.QueuedRun{RunRecord: storage.RunRecord{
		RunID: runID, SessionID: session.ID(), TenantID: principal.TenantID, SubjectID: principal.SubjectID,
	}, Message: "write", MaxAttempts: 3}); err != nil {
		t.Fatal(err)
	}
	return &nativeStrictWitnessFixture{api: api, db: db, accounts: accounts, session: session, runID: runID, toolCalls: &toolCalls, modelCalls: &modelCalls}
}

func TestNativeStrictConstructorQueuedWorkerWritesEffectWitness(t *testing.T) {
	ctx := context.Background()
	fixture := newNativeStrictWitnessFixture(t, false)
	claimed, err := fixture.api.RunWorkerOnce(ctx, "worker-native-constructor")
	if err != nil || !claimed {
		t.Fatalf("claimed=%t err=%v", claimed, err)
	}
	terminal, err := fixture.api.runQueue.GetRun(ctx, fixture.runID)
	if err != nil || terminal.Status != string(core.RunCompleted) || fixture.toolCalls.Load() != 1 || fixture.modelCalls.Load() != 2 {
		t.Fatalf("terminal=%+v tools=%d models=%d err=%v", terminal, fixture.toolCalls.Load(), fixture.modelCalls.Load(), err)
	}
	var witnesses int
	if err := fixture.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM native_queued_tool_effect_witnesses WHERE session_id = ?", fixture.session.ID()).Scan(&witnesses); err != nil || witnesses != 1 {
		t.Fatalf("witnesses=%d err=%v", witnesses, err)
	}
}

func TestNativeStrictConstructorRechecksAccountBeforeToolAdmission(t *testing.T) {
	ctx := context.Background()
	fixture := newNativeStrictWitnessFixture(t, true)
	claimed, err := fixture.api.RunWorkerOnce(ctx, "worker-native-constructor-disabled")
	if err != nil || !claimed {
		t.Fatalf("claimed=%t err=%v", claimed, err)
	}
	terminal, err := fixture.api.runQueue.GetRun(ctx, fixture.runID)
	if err != nil || terminal.Status != string(core.RunCancelled) || fixture.toolCalls.Load() != 0 || fixture.modelCalls.Load() != 1 {
		t.Fatalf("terminal=%+v tools=%d models=%d err=%v", terminal, fixture.toolCalls.Load(), fixture.modelCalls.Load(), err)
	}
	var witnesses int
	if err := fixture.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM native_queued_tool_effect_witnesses WHERE session_id = ?", fixture.session.ID()).Scan(&witnesses); err != nil || witnesses != 0 {
		t.Fatalf("witnesses=%d err=%v", witnesses, err)
	}
}
