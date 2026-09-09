package server

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	effectadapter "github.com/whhhh1500/auto-agent/pkg/adapter/effectreceipt"
	appreceipt "github.com/whhhh1500/auto-agent/pkg/app/effectreceipt"
	"github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/storage"
)

const (
	nativeQueuedEffectCapabilityID = "effect.dispatch"
	nativeQueuedEffectCallID       = "call-effect-dispatch"
	nativeQueuedEffectSessionID    = "session-native-effect"
	nativeQueuedEffectRunID        = "run-native-effect"
)

var (
	nativeQueuedEffectTarget  = []byte("native-queued-effect-target")
	nativeQueuedEffectPayload = []byte(`{"value":"one"}`)
)

type nativeQueuedEffectTestDriver struct {
	dispatches atomic.Int32
	readBacks  atomic.Int32
}

func (*nativeQueuedEffectTestDriver) Ref() appreceipt.DriverRef {
	return appreceipt.DriverRef{ID: "native-queued-effect-test", Version: "v1"}
}

func (d *nativeQueuedEffectTestDriver) Dispatch(_ context.Context, request appreceipt.DispatchRequest) (appreceipt.Submission, error) {
	d.dispatches.Add(1)
	return appreceipt.Submission{
		OperationKey:  request.Intent.OperationKey,
		IntentDigest:  request.Intent.IntentDigest,
		ReceiptDigest: appreceipt.SHA256Digest([]byte("native-queued-effect-receipt/" + request.Intent.OperationKey)),
	}, nil
}

func (d *nativeQueuedEffectTestDriver) ReadBack(_ context.Context, intent appreceipt.Intent) (appreceipt.Observation, error) {
	d.readBacks.Add(1)
	return appreceipt.Observation{
		State:          appreceipt.ObservationConfirmed,
		OperationKey:   intent.OperationKey,
		IntentDigest:   intent.IntentDigest,
		EvidenceDigest: appreceipt.SHA256Digest([]byte("native-queued-effect-evidence/" + intent.OperationKey)),
	}, nil
}

type nativeQueuedEffectTestModel struct{ calls *atomic.Int32 }

func (nativeQueuedEffectTestModel) Provider() string { return "native-queued-effect-test" }

func (m nativeQueuedEffectTestModel) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	if m.calls != nil {
		m.calls.Add(1)
	}
	if options.Messages[len(options.Messages)-1].Role == core.RoleTool {
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: "done"})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
		return nil
	}
	call := core.ToolCall{
		ID: nativeQueuedEffectCallID, Name: nativeQueuedEffectCapabilityID, Args: map[string]any{"value": "one"},
	}
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &call})
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
	return nil
}

type nativeQueuedEffectBridgeFixture struct {
	api      *Server
	db       *sql.DB
	accounts *storage.SQLAccountStore
	session  *core.Session
	runID    string
	driver   *nativeQueuedEffectTestDriver
	receipts *storage.SQLExternalEffectReceiptStore
}

type nativeQueuedEffectFixtureMutation func(context.Context, *nativeQueuedEffectBridgeFixture) error

func newNativeQueuedEffectBridgeFixture(t *testing.T, store appreceipt.Store, mutate nativeQueuedEffectFixtureMutation) *nativeQueuedEffectBridgeFixture {
	t.Helper()
	ctx := context.Background()
	db := openNativeStrictTestDB(t)
	if _, err := storage.OpenSQLSessionStore(ctx, db, storage.SQLDialectSQLite); err != nil {
		t.Fatal(err)
	}
	receipts, err := storage.NewSQLExternalEffectReceiptStore(db, storage.SQLDialectSQLite)
	if err != nil {
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

	fixture := &nativeQueuedEffectBridgeFixture{db: db, accounts: accounts, driver: &nativeQueuedEffectTestDriver{}, receipts: receipts}
	if store == nil {
		store = receipts
	}
	if responseLost, ok := store.(responseLostNativeQueuedEffectStore); ok && responseLost.Store == nil {
		responseLost.Store = receipts
		store = responseLost
	}
	api := newNativeQueuedEffectBridgeServer(t, db, store, fixture.driver, effectadapter.BinderFunc(func(ctx context.Context, _ core.CapabilityRequest) (effectadapter.Binding, error) {
		if mutate != nil {
			if err := mutate(ctx, fixture); err != nil {
				return effectadapter.Binding{}, err
			}
		}
		return effectadapter.Binding{Target: append([]byte(nil), nativeQueuedEffectTarget...), Payload: append([]byte(nil), nativeQueuedEffectPayload...)}, nil
	}))
	fixture.api = api

	account, err := accounts.GetAccount(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	root := nativeStrictTestRoot()
	principal, err := storage.PrincipalForAccount(account, root.Segments())
	if err != nil {
		t.Fatal(err)
	}
	scope, err := principal.Scope.Child(core.ScopeRef{Kind: core.ScopeSession, ID: nativeQueuedEffectSessionID})
	if err != nil {
		t.Fatal(err)
	}
	session, err := core.NewSession(core.SessionOptions{
		ID: nativeQueuedEffectSessionID, ProfileID: "native.effect", Principal: principal, Scope: scope,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := api.sessions.Create(ctx, session); err != nil {
		t.Fatal(err)
	}
	if err := api.runQueue.EnqueueRun(ctx, storage.QueuedRun{RunRecord: storage.RunRecord{
		RunID: nativeQueuedEffectRunID, SessionID: session.ID(), TenantID: principal.TenantID, SubjectID: principal.SubjectID,
	}, Message: "dispatch", MaxAttempts: 3}); err != nil {
		t.Fatal(err)
	}
	fixture.session, fixture.runID = session, nativeQueuedEffectRunID
	return fixture
}

func newNativeQueuedEffectBridgeServer(t *testing.T, db *sql.DB, store appreceipt.Store, driver *nativeQueuedEffectTestDriver, binder effectadapter.Binder) *Server {
	t.Helper()
	root := nativeStrictTestRoot()
	selection := core.ModelSelection{Provider: "native-queued-effect-test", Model: "native-effect"}
	name := "Native queued effect"
	manifest := core.CapabilityManifest{
		ID: nativeQueuedEffectCapabilityID, Version: "1.0.0", Name: "Native queued effect", Kind: core.KindTool,
		Contract: "harness.tool/v1", RequiredPermissions: []core.Permission{core.PermWrite}, Idempotent: true,
		Tool: &core.ToolExposure{Parameters: map[string]any{
			"type": "object", "properties": map[string]any{"value": map[string]any{"type": "string"}},
		}},
	}
	bridge, err := effectadapter.NewBridge(manifest, store, driver, binder)
	if err != nil {
		t.Fatal(err)
	}
	registry := appreceipt.NewDriverRegistry()
	if err := registry.Register(driver); err != nil {
		t.Fatal(err)
	}
	api, err := NewNativeStrictServer(context.Background(), NativeStrictServerConfig{
		DB: db, Dialect: storage.SQLDialectSQLite, EffectReceiptDrivers: registry,
		Bootstrap: NativeStrictBootstrap{
			Revision: "native-queued-effect-v1", Root: root.Segments(), DefaultProfileID: "native.effect",
			Profiles: []core.AgentProfileLayer{{
				Scope: root, ProfileID: "native.effect", Name: &name, Model: &selection,
				AddCapabilities: []string{nativeQueuedEffectCapabilityID},
			}},
			Capabilities: []NativeStrictCapability{{Scope: root, Capability: bridge}},
			Model:        NativeStrictModel{Selection: selection, Adapter: nativeQueuedEffectTestModel{}},
		},
		MaxWriteDelay: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = api.Shutdown(context.Background()) })
	return api
}

func nativeQueuedEffectIntent(t *testing.T, fixture *nativeQueuedEffectBridgeFixture) appreceipt.Intent {
	t.Helper()
	invocation, err := core.NewToolInvocation(core.RunInfo{
		RunID: fixture.runID, SessionID: fixture.session.ID(), Principal: fixture.session.Principal(),
	}, core.ToolCall{ID: nativeQueuedEffectCallID, Name: nativeQueuedEffectCapabilityID, Args: map[string]any{"value": "one"}}, true)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := appreceipt.NewIntent(invocation, fixture.driver.Ref(), nativeQueuedEffectTarget, nativeQueuedEffectPayload)
	if err != nil {
		t.Fatal(err)
	}
	return intent
}

func TestNativeStrictQueuedEffectDispatchConfirmsV50Receipt(t *testing.T) {
	ctx := context.Background()
	fixture := newNativeQueuedEffectBridgeFixture(t, nil, nil)
	claimed, err := fixture.api.RunWorkerOnce(ctx, "worker-native-effect-success")
	if err != nil || !claimed {
		t.Fatalf("claimed=%t err=%v", claimed, err)
	}
	run, err := fixture.api.runQueue.GetRun(ctx, fixture.runID)
	if err != nil || run.Status != string(core.RunCompleted) {
		t.Fatalf("run=%+v err=%v", run, err)
	}
	record, found, err := fixture.receipts.Get(ctx, nativeQueuedEffectIntent(t, fixture))
	if err != nil || !found || record.State != appreceipt.StateConfirmed || record.DispatchAttempts != 1 || record.EvidenceDigest == "" {
		t.Fatalf("receipt=%+v found=%t err=%v", record, found, err)
	}
	if fixture.driver.dispatches.Load() != 1 || fixture.driver.readBacks.Load() != 1 {
		t.Fatalf("provider dispatches=%d readbacks=%d", fixture.driver.dispatches.Load(), fixture.driver.readBacks.Load())
	}
	var schemaVersion string
	if err := fixture.db.QueryRowContext(ctx, "SELECT value FROM store_meta WHERE key = 'schema_version'").Scan(&schemaVersion); err != nil || schemaVersion != "50" {
		t.Fatalf("schema version=%q err=%v", schemaVersion, err)
	}
}

func TestNativeStrictQueuedEffectDispatchFailsClosedAfterFenceChange(t *testing.T) {
	tests := []struct {
		name   string
		mutate nativeQueuedEffectFixtureMutation
	}{
		{
			name: "authorization epoch",
			mutate: func(ctx context.Context, fixture *nativeQueuedEffectBridgeFixture) error {
				return fixture.accounts.CreateAccount(ctx, storage.Account{
					AccountID: "bob", Email: "bob@example.test", Role: storage.RoleAccountUser,
					TenantID: "acme", Status: storage.AccountActive,
				}, "native-password")
			},
		},
		{
			name: "claim lease",
			mutate: func(ctx context.Context, fixture *nativeQueuedEffectBridgeFixture) error {
				_, err := fixture.db.ExecContext(ctx, "UPDATE run_queue SET worker_id = ? WHERE run_id = ?", "worker-replaced", fixture.runID)
				return err
			},
		},
		{
			name: "session version",
			mutate: func(ctx context.Context, fixture *nativeQueuedEffectBridgeFixture) error {
				_, err := fixture.db.ExecContext(ctx, "UPDATE sessions SET version = version + 1 WHERE id = ?", fixture.session.ID())
				return err
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newNativeQueuedEffectBridgeFixture(t, nil, test.mutate)
			claimed, _ := fixture.api.RunWorkerOnce(context.Background(), "worker-native-effect-fence")
			if !claimed {
				t.Fatal("worker did not claim the queued run")
			}
			if fixture.driver.dispatches.Load() != 0 || fixture.driver.readBacks.Load() != 0 {
				t.Fatalf("provider dispatches=%d readbacks=%d after failed admission", fixture.driver.dispatches.Load(), fixture.driver.readBacks.Load())
			}
			record, found, err := fixture.receipts.Get(context.Background(), nativeQueuedEffectIntent(t, fixture))
			if err != nil || !found || record.State != appreceipt.StatePrepared || record.DispatchAttempts != 0 {
				t.Fatalf("failed admission receipt=%+v found=%t err=%v", record, found, err)
			}
		})
	}
}

type panicCheckpointSessionStore struct{}

func (panicCheckpointSessionStore) Create(context.Context, *core.Session) error { return nil }
func (panicCheckpointSessionStore) Load(context.Context, string) (*core.Session, error) {
	return nil, core.ErrSessionNotFound
}
func (panicCheckpointSessionStore) Save(context.Context, *core.Session, int64) error {
	panic("checkpoint implementation detail")
}

func TestNativeQueuedEffectDispatchAdmissionPanicCancelsRun(t *testing.T) {
	fixture := newNativeQueuedEffectBridgeFixture(t, nil, nil)
	if _, err := fixture.session.Append(fixture.runID, core.EvRunStart, core.RunStartData{}); err != nil {
		t.Fatal(err)
	}
	writer := storage.NewWriteBehind(panicCheckpointSessionStore{}, fixture.session, 0, -1)
	writer.MarkDirty()
	runCtx, cancel := context.WithCancel(context.Background())
	failure := &toolCheckpointFailure{}
	admitter := &nativeQueuedEffectDispatchAdmitter{
		server: fixture.api, store: fixture.api.sessions.(*storage.SQLSessionStore), writer: writer,
		cancel: cancel, failure: failure,
		fence: storage.SessionWriteFence{
			SessionID: fixture.session.ID(), RunID: fixture.runID,
			TenantID: fixture.session.Principal().TenantID, SubjectID: fixture.session.Principal().SubjectID,
			WorkerID: "panic-worker", QueueGeneration: 1, LeaseHolder: "panic-lease",
		},
		session: fixture.session, principal: fixture.session.Principal(),
	}
	_, begun, err := admitter.BeginDispatch(runCtx, nativeQueuedEffectIntent(t, fixture))
	if begun || !errors.Is(err, appreceipt.ErrDispatchAdmissionPanic) || !failure.Failed() || runCtx.Err() == nil {
		t.Fatalf("panic admission begun=%t err=%v failure=%v context=%v", begun, err, failure.Err(), runCtx.Err())
	}
	if fixture.driver.dispatches.Load() != 0 || fixture.driver.readBacks.Load() != 0 {
		t.Fatalf("panic admission crossed provider dispatches=%d readbacks=%d", fixture.driver.dispatches.Load(), fixture.driver.readBacks.Load())
	}
}

func TestNativeStrictPublicSynchronousRunRouteRejectsEffectDispatch(t *testing.T) {
	ctx := context.Background()
	fixture := newNativeQueuedEffectBridgeFixture(t, nil, nil)
	request := httptest.NewRequest(http.MethodPost, "/v1/sessions/"+fixture.session.ID()+"/runs", nil)
	response := httptest.NewRecorder()
	fixture.api.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("native synchronous route status=%d body=%s", response.Code, response.Body.String())
	}
	if fixture.driver.dispatches.Load() != 0 || fixture.driver.readBacks.Load() != 0 {
		t.Fatalf("public synchronous route provider dispatches=%d readbacks=%d", fixture.driver.dispatches.Load(), fixture.driver.readBacks.Load())
	}
	var receipts int
	if err := fixture.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM external_effect_receipts WHERE session_id = ?", fixture.session.ID()).Scan(&receipts); err != nil || receipts != 0 {
		t.Fatalf("public synchronous route receipts=%d err=%v", receipts, err)
	}
}

type responseLostNativeQueuedEffectStore struct{ appreceipt.Store }

func (responseLostNativeQueuedEffectStore) MarkAccepted(context.Context, appreceipt.Intent, appreceipt.Submission) (appreceipt.Record, error) {
	return appreceipt.Record{}, appreceipt.ErrStore
}

func TestNativeStrictQueuedEffectDispatchingReplayReadsBackWithoutRedispatch(t *testing.T) {
	ctx := context.Background()
	fixture := newNativeQueuedEffectBridgeFixture(t, responseLostNativeQueuedEffectStore{}, nil)
	fixture.api.nativeQueuedRecoveryTestHooks = &nativeQueuedRecoveryTestHooks{}
	reached := make(chan struct{})
	release := make(chan struct{})
	fixture.api.nativeQueuedRecoveryTestHooks.afterToolJournalComplete = func() {
		close(reached)
		<-release
	}
	done := make(chan error, 1)
	go func() {
		_, err := fixture.api.RunWorkerOnce(ctx, "worker-native-effect-response-lost")
		done <- err
	}()
	select {
	case <-reached:
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatalf("original worker did not reach completed journal: %v", <-done)
	}
	intent := nativeQueuedEffectIntent(t, fixture)
	record, found, err := fixture.receipts.Get(ctx, intent)
	if err != nil || !found || record.State != appreceipt.StateDispatching || record.DispatchAttempts != 1 || fixture.driver.dispatches.Load() != 1 || fixture.driver.readBacks.Load() != 0 {
		t.Fatalf("response-lost receipt=%+v found=%t err=%v dispatches=%d readbacks=%d", record, found, err, fixture.driver.dispatches.Load(), fixture.driver.readBacks.Load())
	}

	requeueNativeQueuedEffectClaim(t, fixture)
	successor := newNativeQueuedEffectBridgeServer(t, fixture.db, fixture.receipts, fixture.driver, effectadapter.BinderFunc(func(context.Context, core.CapabilityRequest) (effectadapter.Binding, error) {
		return effectadapter.Binding{Target: append([]byte(nil), nativeQueuedEffectTarget...), Payload: append([]byte(nil), nativeQueuedEffectPayload...)}, nil
	}))
	successor.runEffectReceiptRecoveryOnce(ctx)
	record, found, err = fixture.receipts.Get(ctx, intent)
	if err != nil || !found || record.State != appreceipt.StateConfirmed || record.DispatchAttempts != 1 || fixture.driver.dispatches.Load() != 1 || fixture.driver.readBacks.Load() != 1 {
		t.Fatalf("replayed receipt=%+v found=%t err=%v dispatches=%d readbacks=%d", record, found, err, fixture.driver.dispatches.Load(), fixture.driver.readBacks.Load())
	}
	claimed, err := successor.RunWorkerOnce(ctx, "worker-native-effect-successor")
	if err != nil || !claimed || fixture.driver.dispatches.Load() != 1 {
		t.Fatalf("successor claimed=%t err=%v dispatches=%d", claimed, err, fixture.driver.dispatches.Load())
	}
	close(release)
	if err := <-done; err == nil {
		t.Fatal("stale response-lost worker unexpectedly completed")
	}
	run, err := successor.runQueue.GetRun(ctx, fixture.runID)
	if err != nil || run.Status != string(core.RunCompleted) {
		t.Fatalf("successor run=%+v err=%v", run, err)
	}
}

func requeueNativeQueuedEffectClaim(t *testing.T, fixture *nativeQueuedEffectBridgeFixture) {
	t.Helper()
	ctx := context.Background()
	if _, err := fixture.db.ExecContext(ctx, "UPDATE run_queue SET lease_expires_at = 1 WHERE run_id = ?", fixture.runID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.ExecContext(ctx, "UPDATE session_leases SET expires_at = 1 WHERE session_id = ?", fixture.session.ID()); err != nil {
		t.Fatal(err)
	}
	requeued, failed, err := fixture.api.runQueue.RecoverExpiredRunClaims(ctx, time.Now().UTC())
	if err != nil || requeued != 1 || failed != 0 {
		t.Fatalf("requeue=%d failed=%d err=%v", requeued, failed, err)
	}
}
