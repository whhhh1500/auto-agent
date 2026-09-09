package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/whhhh1500/auto-agent/pkg/app/effectreceipt"
	"github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/storage"
	_ "modernc.org/sqlite"
)

type effectRecoveryTestDriver struct{ readBacks, dispatches atomic.Int32 }

func (*effectRecoveryTestDriver) Ref() effectreceipt.DriverRef {
	return effectreceipt.DriverRef{ID: "recovery-test", Version: "v1"}
}
func (d *effectRecoveryTestDriver) Dispatch(context.Context, effectreceipt.DispatchRequest) (effectreceipt.Submission, error) {
	d.dispatches.Add(1)
	return effectreceipt.Submission{}, errors.New("dispatch must not run during recovery")
}
func (d *effectRecoveryTestDriver) ReadBack(_ context.Context, intent effectreceipt.Intent) (effectreceipt.Observation, error) {
	d.readBacks.Add(1)
	return effectreceipt.Observation{State: effectreceipt.ObservationConfirmed, OperationKey: intent.OperationKey, IntentDigest: intent.IntentDigest, EvidenceDigest: effectreceipt.SHA256Digest([]byte("read-back"))}, nil
}

type effectRecoveryAuthorizerFunc func(context.Context, effectreceipt.RecoveryRecord) (bool, error)

func (f effectRecoveryAuthorizerFunc) AuthorizeEffectRecovery(ctx context.Context, record effectreceipt.RecoveryRecord) (bool, error) {
	return f(ctx, record)
}

func newEffectRecoveryCoordinator(t *testing.T, authorize effectRecoveryAuthorizerFunc) (*effectreceipt.RecoveryCoordinator, *storage.SQLExternalEffectReceiptStore, *effectRecoveryTestDriver, effectreceipt.Intent) {
	t.Helper()
	db, err := sql.Open("sqlite", t.TempDir()+"/effects.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := storage.OpenSQLSessionStore(context.Background(), db, storage.SQLDialectSQLite); err != nil {
		t.Fatal(err)
	}
	store, err := storage.NewSQLExternalEffectReceiptStore(db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	driver := &effectRecoveryTestDriver{}
	registry := effectreceipt.NewDriverRegistry()
	if err := registry.Register(driver); err != nil {
		t.Fatal(err)
	}
	coordinator, err := effectreceipt.NewRecoveryCoordinator(store, store, registry, authorize)
	if err != nil {
		t.Fatal(err)
	}
	invocation := core.ToolInvocation{TenantID: "tenant", SubjectID: "subject", SessionID: "effect-recovery-session", RunID: "effect-recovery-run", CallID: "effect-recovery-call", CapabilityID: "effect.recover", ArgsDigest: effectreceipt.SHA256Digest([]byte("args")), Idempotent: true}
	intent, err := effectreceipt.NewIntent(invocation, driver.Ref(), []byte("target"), []byte("payload"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Ensure(context.Background(), intent); err != nil {
		t.Fatal(err)
	}
	if _, begun, err := store.BeginDispatch(context.Background(), intent); err != nil || !begun {
		t.Fatalf("begin=%t err=%v", begun, err)
	}
	if _, err := store.MarkAccepted(context.Background(), intent, effectreceipt.Submission{OperationKey: intent.OperationKey, IntentDigest: intent.IntentDigest, ReceiptDigest: effectreceipt.SHA256Digest([]byte("accepted"))}); err != nil {
		t.Fatal(err)
	}
	return coordinator, store, driver, intent
}

func TestEffectReceiptRecoveryWorkerOnlyReadsBack(t *testing.T) {
	coordinator, store, driver, intent := newEffectRecoveryCoordinator(t, func(context.Context, effectreceipt.RecoveryRecord) (bool, error) { return true, nil })
	server := &Server{effectReceiptRecovery: coordinator, effectRecoveryCursors: map[effectreceipt.DriverRef]effectreceipt.RecoveryCursor{}}
	server.runEffectReceiptRecoveryOnce(context.Background())
	record, found, err := store.Get(context.Background(), intent)
	if err != nil || !found || record.State != effectreceipt.StateConfirmed || driver.readBacks.Load() != 1 || driver.dispatches.Load() != 0 {
		t.Fatalf("recovery record=%+v found=%t err=%v reads=%d dispatches=%d", record, found, err, driver.readBacks.Load(), driver.dispatches.Load())
	}
}

func TestEffectReceiptRecoveryKeepsCursorOnAuthorizationError(t *testing.T) {
	coordinator, _, driver, _ := newEffectRecoveryCoordinator(t, func(context.Context, effectreceipt.RecoveryRecord) (bool, error) {
		return false, errors.New("transient")
	})
	cursor := effectreceipt.RecoveryCursor{UpdatedAt: time.Unix(1700000000, 0).UTC(), TenantID: "tenant", SubjectID: "subject", SessionID: "effect-recovery-session", RunID: "effect-recovery-run", CallID: "before"}
	server := &Server{effectReceiptRecovery: coordinator, effectRecoveryCursors: map[effectreceipt.DriverRef]effectreceipt.RecoveryCursor{driver.Ref(): cursor}}
	server.runEffectReceiptRecoveryOnce(context.Background())
	if got := server.effectRecoveryCursors[driver.Ref()]; got != cursor || driver.readBacks.Load() != 0 {
		t.Fatalf("cursor=%+v reads=%d", got, driver.readBacks.Load())
	}
}

func TestEffectReceiptRecoveryRetrySweepRevisitsDeniedRowsDespiteContinuousNewRecords(t *testing.T) {
	var authorized atomic.Bool
	coordinator, store, driver, old := newEffectRecoveryCoordinator(t, func(context.Context, effectreceipt.RecoveryRecord) (bool, error) {
		return authorized.Load(), nil
	})
	server := &Server{
		effectReceiptRecovery:     coordinator,
		effectRecoveryCursors:     map[effectreceipt.DriverRef]effectreceipt.RecoveryCursor{},
		effectRecoveryRetrySweeps: map[effectreceipt.DriverRef]effectRecoveryRetrySweep{},
	}

	// The old row is denied while every tick appends a newer unresolved row.
	// The main cursor consequently never receives an empty page that could
	// restart it and revisit the old row by itself.
	server.runEffectReceiptRecoveryOnce(context.Background())
	for index := 0; index < 3; index++ {
		seedAcceptedEffectRecoveryRecord(t, store, driver.Ref(), index)
		server.runEffectReceiptRecoveryOnce(context.Background())
	}
	if cursor := server.effectRecoveryCursors[driver.Ref()]; cursor.UpdatedAt.IsZero() {
		t.Fatal("main recovery cursor reset despite continuously appended unresolved records")
	}
	mainCursor := server.effectRecoveryCursors[driver.Ref()]
	sweep := server.effectRecoveryRetrySweeps[driver.Ref()]
	if !sweep.active || !effectRecoveryCursorAtOrAfter(sweep.watermark, mainCursor) {
		t.Fatalf("later denied records were not included in active retry sweep: sweep=%+v main=%+v", sweep, mainCursor)
	}
	if record, found, err := store.Get(context.Background(), old); err != nil || !found || record.State == effectreceipt.StateConfirmed {
		t.Fatalf("old denied record=%+v found=%t err=%v", record, found, err)
	}

	authorized.Store(true)
	var record effectreceipt.Record
	var found bool
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		server.runEffectReceiptRecoveryOnce(context.Background())
		record, found, err = store.Get(context.Background(), old)
		if err == nil && found && record.State == effectreceipt.StateConfirmed {
			break
		}
	}
	if err != nil || !found || record.State != effectreceipt.StateConfirmed {
		t.Fatalf("old record was not revisited after authority returned: %+v found=%t err=%v", record, found, err)
	}
	if driver.readBacks.Load() == 0 || driver.dispatches.Load() != 0 {
		t.Fatalf("reads=%d dispatches=%d", driver.readBacks.Load(), driver.dispatches.Load())
	}
}

func seedAcceptedEffectRecoveryRecord(t *testing.T, store *storage.SQLExternalEffectReceiptStore, driver effectreceipt.DriverRef, index int) {
	t.Helper()
	invocation := core.ToolInvocation{TenantID: "tenant", SubjectID: "subject", SessionID: "effect-recovery-session", RunID: "effect-recovery-run", CallID: fmt.Sprintf("effect-recovery-new-%02d", index), CapabilityID: "effect.recover", ArgsDigest: effectreceipt.SHA256Digest([]byte("args")), Idempotent: true}
	intent, err := effectreceipt.NewIntent(invocation, driver, []byte("target"), []byte("payload"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Ensure(context.Background(), intent); err != nil {
		t.Fatal(err)
	}
	if _, begun, err := store.BeginDispatch(context.Background(), intent); err != nil || !begun {
		t.Fatalf("begin=%t err=%v", begun, err)
	}
	if _, err := store.MarkAccepted(context.Background(), intent, effectreceipt.Submission{OperationKey: intent.OperationKey, IntentDigest: intent.IntentDigest, ReceiptDigest: effectreceipt.SHA256Digest([]byte("accepted"))}); err != nil {
		t.Fatal(err)
	}
}

func TestEffectReceiptRecoveryWorkerStopsWithLifecycleContext(t *testing.T) {
	coordinator, _, _, _ := newEffectRecoveryCoordinator(t, func(context.Context, effectreceipt.RecoveryRecord) (bool, error) { return true, nil })
	server, err := New(Config{Runtime: &core.Runtime{}, Sessions: core.NewMemorySessionStore(), Authenticator: AuthenticatorFunc(func(*http.Request) (core.Principal, error) { return core.Principal{}, nil }), EffectReceiptRecovery: coordinator})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := server.StartRunWorkers(ctx); err != nil {
		t.Fatal(err)
	}
	cancel()
	shutdownCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := server.Shutdown(shutdownCtx); err != nil {
		t.Fatal(err)
	}
}

func TestNativeStrictAutomaticallyWiresEffectReceiptRecovery(t *testing.T) {
	registry := effectreceipt.NewDriverRegistry()
	driver := &effectRecoveryTestDriver{}
	if err := registry.Register(driver); err != nil {
		t.Fatal(err)
	}
	root := nativeStrictTestRoot()
	api, err := NewNativeStrictServer(context.Background(), NativeStrictServerConfig{
		DB: openNativeStrictTestDB(t), Dialect: storage.SQLDialectSQLite, Bootstrap: nativeStrictTestBootstrap(root), EffectReceiptDrivers: registry,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = api.Shutdown(context.Background()) })
	if api.effectReceiptRecovery == nil || len(api.effectReceiptRecovery.Drivers()) != 1 || api.effectReceiptRecovery.Drivers()[0] != driver.Ref() {
		t.Fatalf("native strict effect recovery=%v drivers=%v", api.effectReceiptRecovery, api.effectReceiptRecovery.Drivers())
	}
}

func TestNativeQueuedPrincipalComparisonAcceptsSQLNilEmptyNormalization(t *testing.T) {
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	left := core.Principal{TenantID: "tenant", SubjectID: "subject", Scope: scope}
	right := core.Principal{
		TenantID: "tenant", SubjectID: "subject", Scope: scope,
		Grants: core.NewPermissionSet(), Attributes: map[string]string{},
	}
	if !sameNativeQueuedPrincipal(left, right) {
		t.Fatal("nil and empty authority maps must compare equally")
	}
	right.Grants[core.PermSend] = true
	if sameNativeQueuedPrincipal(left, right) {
		t.Fatal("a changed grant must revoke the in-flight authority")
	}
}

func TestNativeStrictEffectRecoveryAuthorizerUsesCanonicalSessionScope(t *testing.T) {
	root := nativeStrictTestRoot()
	api := newNativeStrictTestServer(t, openNativeStrictTestDB(t), nativeStrictTestBootstrap(root))
	accounts, err := storage.NewSQLAccountStore(api.nativeStrict.db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	if err := accounts.CreateTenant(context.Background(), "effect-tenant", "Effect tenant"); err != nil {
		t.Fatal(err)
	}
	if err := accounts.CreateAccount(context.Background(), storage.Account{AccountID: "effect-user", Email: "effect@example.test", Role: storage.RoleAccountUser, TenantID: "effect-tenant", Status: storage.AccountActive}, "password"); err != nil {
		t.Fatal(err)
	}
	resolver := api.runPrincipal.(*storage.SQLQueuedPrincipalResolver)
	principal, err := resolver.ResolveRunPrincipal(context.Background(), "effect-tenant", "effect-user")
	if err != nil {
		t.Fatal(err)
	}
	sessions := api.sessions.(*storage.SQLSessionStore)
	makeSession := func(id, run string) *core.Session {
		scope, err := principal.Scope.Child(core.ScopeRef{Kind: core.ScopeSession, ID: id})
		if err != nil {
			t.Fatal(err)
		}
		session, err := core.NewSession(core.SessionOptions{ID: id, ProfileID: "native.agent", Scope: scope, Principal: principal})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := session.Append(run, core.EvRunStart, core.RunStartData{}); err != nil {
			t.Fatal(err)
		}
		if _, err := session.Append(run, core.EvRunEnd, core.RunEndData{Status: core.RunCompleted}); err != nil {
			t.Fatal(err)
		}
		if err := sessions.Create(context.Background(), session); err != nil {
			t.Fatal(err)
		}
		return session
	}
	session := makeSession("effect-scope-session", "effect-scope-run")
	other := makeSession("effect-other-session", "effect-other-run")
	authorizer := nativeStrictEffectRecoveryAuthorizer{runtime: api.runtime, resolver: resolver, sessions: sessions}
	recordFor := func(sessionID, runID, capability string) effectreceipt.RecoveryRecord {
		return effectreceipt.RecoveryRecord{Record: effectreceipt.Record{Intent: effectreceipt.Intent{Invocation: core.ToolInvocation{TenantID: principal.TenantID, SubjectID: principal.SubjectID, SessionID: sessionID, RunID: runID, CallID: "effect-call", CapabilityID: capability, Idempotent: false}}}}
	}
	// A session-scope provide is visible only when recovery resolves the exact
	// canonical session scope, not the principal's broader user scope.
	provided := &nativeStrictTestCapability{manifest: core.CapabilityManifest{ID: "effect.session.only", Version: "v1", Kind: core.KindTool, Tool: &core.ToolExposure{Parameters: map[string]any{"type": "object"}}}}
	if _, err := api.runtime.Capabilities.Mount(core.CapabilityBinding{Scope: session.Scope(), Mode: core.BindingProvide, Manifest: provided.Manifest(), Provider: provided}); err != nil {
		t.Fatal(err)
	}
	if err := api.runtime.Profiles.Bind(core.AgentProfileLayer{Scope: session.Scope(), ProfileID: "native.agent", AddCapabilities: []string{"effect.session.only"}}); err != nil {
		t.Fatal(err)
	}
	if allowed, err := authorizer.AuthorizeEffectRecovery(context.Background(), recordFor(session.ID(), "effect-scope-run", "effect.session.only")); err != nil || !allowed {
		t.Fatalf("session provide allowed=%t err=%v", allowed, err)
	}
	if err := api.runtime.Profiles.Bind(core.AgentProfileLayer{Scope: session.Scope(), ProfileID: "native.agent", RemoveCapabilities: []string{"effect.session.only"}}); err != nil {
		t.Fatal(err)
	}
	if allowed, err := authorizer.AuthorizeEffectRecovery(context.Background(), recordFor(session.ID(), "effect-scope-run", "effect.session.only")); err != nil || allowed {
		t.Fatalf("profile-removed capability allowed=%t err=%v", allowed, err)
	}
	if allowed, err := authorizer.AuthorizeEffectRecovery(context.Background(), recordFor(other.ID(), "effect-other-run", "effect.session.only")); err != nil || allowed {
		t.Fatalf("cross-session provide allowed=%t err=%v", allowed, err)
	}
	if _, err := api.runtime.Capabilities.Mount(core.CapabilityBinding{Scope: session.Scope(), Mode: core.BindingDisable, Manifest: core.CapabilityManifest{ID: "native.echo", Version: "v1"}}); err != nil {
		t.Fatal(err)
	}
	if allowed, err := authorizer.AuthorizeEffectRecovery(context.Background(), recordFor(session.ID(), "effect-scope-run", "native.echo")); err != nil || allowed {
		t.Fatalf("session disable allowed=%t err=%v", allowed, err)
	}
	if allowed, err := authorizer.AuthorizeEffectRecovery(context.Background(), recordFor(other.ID(), "missing-run", "native.echo")); err != nil || allowed {
		t.Fatalf("missing run allowed=%t err=%v", allowed, err)
	}
}
