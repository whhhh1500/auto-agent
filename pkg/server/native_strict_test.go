package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/whhhh1500/auto-agent/pkg/app/runliveness"
	"github.com/whhhh1500/auto-agent/pkg/control"
	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/evaluation"
	"github.com/whhhh1500/auto-agent/pkg/storage"

	_ "modernc.org/sqlite"
)

type nativeStrictTestCapability struct {
	manifest core.CapabilityManifest
}

func (c *nativeStrictTestCapability) Manifest() core.CapabilityManifest { return c.manifest }

func (*nativeStrictTestCapability) Execute(context.Context, core.CapabilityRequest) (core.CapabilityResult, error) {
	return core.CapabilityResult{Content: "native strict test"}, nil
}

func nativeStrictTestRoot() core.ScopePath {
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product, err := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "native-test"})
	if err != nil {
		panic(err)
	}
	return product
}

func nativeStrictTestBootstrap(root core.ScopePath) NativeStrictBootstrap {
	name := "Native strict test"
	model := core.ModelSelection{Provider: "mock", Model: "native-static"}
	maxSteps := 4
	capability := &nativeStrictTestCapability{manifest: core.CapabilityManifest{
		ID: "native.echo", Version: "v1", Name: "Native Echo", Kind: core.KindTool,
		Tool: &core.ToolExposure{Parameters: map[string]any{"type": "object"}},
	}}
	return NativeStrictBootstrap{
		Revision: "native-test-v1", Root: root.Segments(), DefaultProfileID: "native.agent",
		Profiles: []core.AgentProfileLayer{{
			Scope: root, ProfileID: "native.agent", Name: &name, Model: &model,
			AddCapabilities: []string{"native.echo"}, Metadata: map[string]string{"bootstrap": "true"},
		}},
		Policies:     []core.PolicyLayer{{Scope: root, MaxSteps: &maxSteps}},
		Capabilities: []NativeStrictCapability{{Scope: root, Capability: capability}},
		Model:        NativeStrictModel{Selection: model, Adapter: core.MockLlmAdapter{}},
	}
}

func openNativeStrictTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "native-strict.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func newNativeStrictTestServer(t *testing.T, db *sql.DB, bootstrap NativeStrictBootstrap) *Server {
	t.Helper()
	api, err := NewNativeStrictServer(context.Background(), NativeStrictServerConfig{
		DB: db, Dialect: storage.SQLDialectSQLite, Bootstrap: bootstrap,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = api.Shutdown(context.Background()) })
	return api
}

func TestNewNativeStrictServerOwnsSQLRuntimeAndInitializesEpoch(t *testing.T) {
	root := nativeStrictTestRoot()
	api := newNativeStrictTestServer(t, openNativeStrictTestDB(t), nativeStrictTestBootstrap(root))

	if api.nativeStrict == nil || api.nativeStrict.phase != nativeStrictPhaseStaticBootstrap {
		t.Fatalf("native strict ownership=%#v", api.nativeStrict)
	}
	store, enabled, err := api.nativeQueuedCompletedToolRecoveryStore()
	if err != nil || !enabled || store == nil {
		t.Fatalf("native static recovery authority: store=%T enabled=%t err=%v", store, enabled, err)
	}
	if _, enabled, err := (&Server{}).nativeQueuedCompletedToolRecoveryStore(); err != nil || enabled {
		t.Fatalf("generic server recovery authority: enabled=%t err=%v", enabled, err)
	}
	if _, ok := api.sessions.(*storage.SQLSessionStore); !ok {
		t.Fatalf("sessions=%T, want SQL session store", api.sessions)
	}
	if _, ok := api.runControl.(*storage.SQLRunControlStore); !ok {
		t.Fatalf("run control=%T, want SQL run control", api.runControl)
	}
	if _, ok := api.runPrincipal.(*storage.SQLQueuedPrincipalResolver); !ok {
		t.Fatalf("run principal=%T, want native SQL resolver", api.runPrincipal)
	}
	if _, ok := api.leaser.(*storage.SQLSessionStore); !ok {
		t.Fatalf("leaser=%T, want SQL session store", api.leaser)
	}
	auth, ok := api.authenticator.(AccountAuthenticator)
	if !ok || auth.DevHeaderFallback {
		t.Fatalf("authenticator=%#v, want account auth without development fallback", api.authenticator)
	}
	if api.runtime.Credentials != nil || api.runtime.FastRouters != nil || api.runtime.Hooks != nil ||
		api.runtime.RateLimiter != nil || api.runtime.ModelCallGate != nil {
		t.Fatal("native strict runtime retained a prohibited dynamic runtime seam")
	}
	if api.capabilityRuntimes != nil {
		t.Fatal("native strict constructed a dynamic capability runtime registry")
	}
	if _, ok := api.runtime.ToolJournal.(*storage.SQLToolInvocationJournal); !ok {
		t.Fatalf("tool journal=%T, want SQL tool journal", api.runtime.ToolJournal)
	}
	if _, ok := api.runtime.Approver.(*storage.SQLApprovalStore); !ok {
		t.Fatalf("approver=%T, want SQL approval store", api.runtime.Approver)
	}
	if err := storage.ValidateAuthorizationEpochSQLPrincipalAuthority(
		api.runPrincipal, api.sessions, api.runQueue, api.leaser, api.runtime.ToolJournal,
	); err != nil {
		t.Fatalf("native SQL authority: %v", err)
	}
	lease, err := api.acquireExecutionProjection(context.Background())
	if err != nil {
		t.Fatalf("constructor did not initialize epoch admission: %v", err)
	}
	lease.Release()
	ready := httptest.NewRecorder()
	api.Handler().ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if ready.Code != http.StatusOK {
		t.Fatalf("ready status=%d body=%s", ready.Code, ready.Body.String())
	}
	if err := api.MarkExecutionProjectionAppliedEpoch(context.Background()); err == nil {
		t.Fatal("public epoch marker accepted native strict server")
	}

	selection := nativeStrictTestBootstrap(root).Model.Selection
	if _, err := api.runtime.Models.ResolveModel(context.Background(), selection); err != nil {
		t.Fatalf("fixed model selection rejected: %v", err)
	}
	if _, err := api.runtime.Models.ResolveModel(context.Background(), core.ModelSelection{Provider: "mock", Model: "other"}); err == nil {
		t.Fatal("undeclared model selection was accepted")
	}

	accounts, err := storage.NewSQLAccountStore(api.nativeStrict.db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	if err := accounts.CreateTenant(context.Background(), "native-tenant", "Native tenant"); err != nil {
		t.Fatal(err)
	}
	if err := accounts.CreateAccount(context.Background(), storage.Account{
		AccountID: "native-user", Email: "native-user@example.test", Role: storage.RoleAccountUser,
		TenantID: "native-tenant", Status: storage.AccountActive,
	}, "native-password"); err != nil {
		t.Fatal(err)
	}
	lease, err = api.acquireExecutionProjection(context.Background())
	if err != nil {
		t.Fatalf("account authority lag did not auto-reconcile on admission: %v", err)
	}
	lease.Release()
}

func TestNativeStrictSharedSQLiteAccountEpochAutoReconcilesButDynamicControlBlocks(t *testing.T) {
	root := nativeStrictTestRoot()
	db := openNativeStrictTestDB(t)
	serverA := newNativeStrictTestServer(t, db, nativeStrictTestBootstrap(root))
	serverB := newNativeStrictTestServer(t, db, nativeStrictTestBootstrap(root))
	accounts, err := storage.NewSQLAccountStore(serverA.nativeStrict.db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	if err := accounts.CreateTenant(context.Background(), "shared-tenant", "Shared tenant"); err != nil {
		t.Fatal(err)
	}
	if err := accounts.CreateAccount(context.Background(), storage.Account{
		AccountID: "shared-user", Role: storage.RoleAccountUser, TenantID: "shared-tenant", Status: storage.AccountActive,
	}, "shared-password"); err != nil {
		t.Fatal(err)
	}
	lease, err := serverB.acquireExecutionProjection(context.Background())
	if err != nil {
		t.Fatalf("second native server did not reconcile pure account epoch lag: %v", err)
	}
	lease.Release()

	journal, err := storage.NewSQLBindingJournal(db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.Record(context.Background(), storage.BindingRecord{ID: "foreign-dynamic-control", Kind: "policy"}); err != nil {
		t.Fatal(err)
	}
	if _, err := serverB.acquireExecutionProjection(context.Background()); err == nil ||
		!errors.Is(err, storage.ErrNativeStrictDynamicControl) || !strings.Contains(err.Error(), "durable binding") {
		t.Fatalf("dynamic control did not keep native admission blocked: %v", err)
	}
	if serverB.executionProjectionCoordinator().fault(nativeStrictControlSource) == nil {
		t.Fatal("dynamic control fault was not retained")
	}
	if _, err := serverB.acquireExecutionProjection(context.Background()); err == nil ||
		!errors.Is(err, storage.ErrNativeStrictDynamicControl) || !strings.Contains(err.Error(), "durable binding") {
		t.Fatalf("retained dynamic control fault allowed an execution retry: %v", err)
	}
}

func TestNativeStrictTransientPreflightFailureRetriesOnNextAdmission(t *testing.T) {
	db := openNativeStrictTestDB(t)
	api := newNativeStrictTestServer(t, db, nativeStrictTestBootstrap(nativeStrictTestRoot()))
	accounts, err := storage.NewSQLAccountStore(db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	if err := accounts.CreateTenant(context.Background(), "transient-tenant", "Transient tenant"); err != nil {
		t.Fatal(err)
	}
	if err := accounts.CreateAccount(context.Background(), storage.Account{
		AccountID: "transient-user", Role: storage.RoleAccountUser, TenantID: "transient-tenant", Status: storage.AccountActive,
	}, "transient-password"); err != nil {
		t.Fatal(err)
	}
	transient := errors.New("temporary preflight database failure")
	if _, err := api.acquireExecutionProjectionWithNativePreflight(context.Background(), func(context.Context) error {
		return transient
	}); !errors.Is(err, transient) {
		t.Fatalf("transient admission error=%v", err)
	}
	if fault := api.executionProjectionCoordinator().fault(nativeStrictControlSource); fault != nil {
		t.Fatalf("transient preflight became sticky artifact fault: %v", fault)
	}
	lease, err := api.acquireExecutionProjection(context.Background())
	if err != nil {
		t.Fatalf("next admission did not retry transient preflight: %v", err)
	}
	lease.Release()
}

func TestNativeStrictCandidateIsShutdownOnFinalFailureAndRetry(t *testing.T) {
	root := nativeStrictTestRoot()
	t.Run("final error", func(t *testing.T) {
		db := openNativeStrictTestDB(t)
		var candidate *Server
		_, err := newNativeStrictServer(context.Background(), NativeStrictServerConfig{
			DB: db, Dialect: storage.SQLDialectSQLite, Bootstrap: nativeStrictTestBootstrap(root),
		}, func(server *Server) error {
			candidate = server
			journal, err := storage.NewSQLBindingJournal(db, storage.SQLDialectSQLite)
			if err != nil {
				return err
			}
			return journal.Record(context.Background(), storage.BindingRecord{ID: "final-preflight-binding", Kind: "policy"})
		})
		if err == nil || !strings.Contains(err.Error(), "durable binding") {
			t.Fatalf("constructor final failure=%v", err)
		}
		assertNativeStrictCandidateClosed(t, candidate)
	})

	t.Run("epoch retry", func(t *testing.T) {
		db := openNativeStrictTestDB(t)
		accounts, err := storage.NewSQLAccountStore(db, storage.SQLDialectSQLite)
		if err != nil {
			t.Fatal(err)
		}
		var rejected *Server
		calls := 0
		api, err := newNativeStrictServer(context.Background(), NativeStrictServerConfig{
			DB: db, Dialect: storage.SQLDialectSQLite, Bootstrap: nativeStrictTestBootstrap(root),
		}, func(server *Server) error {
			calls++
			if calls != 1 {
				return nil
			}
			rejected = server
			if err := accounts.CreateTenant(context.Background(), "retry-tenant", "Retry tenant"); err != nil {
				return err
			}
			return accounts.CreateAccount(context.Background(), storage.Account{
				AccountID: "retry-user", Role: storage.RoleAccountUser, TenantID: "retry-tenant", Status: storage.AccountActive,
			}, "retry-password")
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = api.Shutdown(context.Background()) })
		if calls != 2 {
			t.Fatalf("candidate builds=%d, want 2", calls)
		}
		assertNativeStrictCandidateClosed(t, rejected)
	})
}

func assertNativeStrictCandidateClosed(t *testing.T, candidate *Server) {
	t.Helper()
	if candidate == nil || candidate.liveness == nil {
		t.Fatal("constructor did not expose a built candidate to the test hook")
	}
	_, err := candidate.liveness.Register(context.Background(), "after-rejection", time.Second, func(context.Context) error {
		return nil
	}, nil)
	if !errors.Is(err, runliveness.ErrClosed) {
		t.Fatalf("rejected candidate liveness register error=%v, want closed", err)
	}
}

func TestNativeStrictInternalDynamicControlPathsFailClosed(t *testing.T) {
	api := newNativeStrictTestServer(t, openNativeStrictTestDB(t), nativeStrictTestBootstrap(nativeStrictTestRoot()))
	if api.capabilityRuntimes != nil {
		t.Fatal("native strict has a dynamic capability runtime registry")
	}
	if err := api.RestoreBindings(context.Background()); !errors.Is(err, errNativeStrictDynamicControlUnavailable) {
		t.Fatalf("native binding restore error=%v", err)
	}
	mounted := false
	if _, err := api.addBinding(context.Background(), "policy", nativeStrictTestRoot(), nil, func() (func(), error) {
		mounted = true
		return func() {}, nil
	}); !errors.Is(err, errNativeStrictDynamicControlUnavailable) {
		t.Fatalf("native binding publication error=%v", err)
	}
	if mounted {
		t.Fatal("native binding publication invoked mount callback")
	}
	if _, _, err := api.prepareDynamicCapability(context.Background(), dynamicCapabilityMount{}); !errors.Is(err, errNativeStrictDynamicControlUnavailable) {
		t.Fatalf("native dynamic capability preparation error=%v", err)
	}
	releases, err := control.NewReleaseManager(api.runtime.Profiles)
	if err != nil {
		t.Fatal(err)
	}
	api.Releases = releases
	if err := api.refreshControlPlane(context.Background()); !errors.Is(err, errNativeStrictDynamicControlUnavailable) {
		t.Fatalf("native release refresh error=%v", err)
	}
}

func TestNativeStrictActivationPreservesCommittedTokenWhenProjectionReconcileFails(t *testing.T) {
	db := openNativeStrictTestDB(t)
	api := newNativeStrictTestServer(t, db, nativeStrictTestBootstrap(nativeStrictTestRoot()))
	accounts, err := storage.NewSQLAccountStore(db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	if err := accounts.CreateTenant(context.Background(), "activation-tenant", "Activation tenant"); err != nil {
		t.Fatal(err)
	}
	if err := accounts.CreateAccount(context.Background(), storage.Account{
		AccountID: "activation-user", Role: storage.RoleAccountUser, TenantID: "activation-tenant",
		Status: storage.AccountPendingActivation, MustChangePassword: true,
	}, "initial-password-123"); err != nil {
		t.Fatal(err)
	}
	restrictedToken, err := accounts.CreateToken(context.Background(), "activation-user", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := storage.NewSQLBindingJournal(db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.Record(context.Background(), storage.BindingRecord{ID: "activation-foreign-control", Kind: "policy"}); err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/auth/activate", strings.NewReader(
		`{"password":"replacement-password-456","confirm":"replacement-password-456"}`,
	))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+restrictedToken)
	response := httptest.NewRecorder()
	api.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("activation status=%d body=%s", response.Code, response.Body.String())
	}
	if response.Header().Get(nativeStrictExecutionProjectionHeader) != "unavailable" {
		t.Fatalf("activation projection header=%q", response.Header().Get(nativeStrictExecutionProjectionHeader))
	}
	var payload struct {
		Token   string `json:"token"`
		Account string `json:"account"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Token == "" || payload.Account != "activation-user" {
		t.Fatalf("activation response lost committed result: %#v", payload)
	}
	account, err := accounts.ResolveToken(context.Background(), payload.Token)
	if err != nil || account.Status != storage.AccountActive {
		t.Fatalf("activation token is not committed: account=%#v err=%v", account, err)
	}
}

func TestNativeStrictBootstrapIsDetachedFromCallerValues(t *testing.T) {
	root := nativeStrictTestRoot()
	bootstrap := nativeStrictTestBootstrap(root)
	capability := bootstrap.Capabilities[0].Capability.(*nativeStrictTestCapability)
	api := newNativeStrictTestServer(t, openNativeStrictTestDB(t), bootstrap)

	*bootstrap.Profiles[0].Name = "mutated profile"
	bootstrap.Profiles[0].AddCapabilities[0] = "mutated.capability"
	bootstrap.Profiles[0].Metadata["bootstrap"] = "mutated"
	*bootstrap.Policies[0].MaxSteps = 99
	bootstrap.Root[1].ID = "mutated-root"
	bootstrap.Capabilities[0].Scope = core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "other"})
	capability.manifest.Name = "mutated capability"
	capability.manifest.Tool.Parameters["mutated"] = true

	profile, err := api.runtime.Profiles.Resolve(core.Principal{Scope: root}, root, "native.agent")
	if err != nil {
		t.Fatal(err)
	}
	if profile.Name != "Native strict test" || profile.Metadata["bootstrap"] != "true" ||
		len(profile.Capabilities) != 1 || profile.Capabilities[0] != "native.echo" {
		t.Fatalf("caller mutation leaked into profile: %#v", profile)
	}
	policies, err := api.runtime.Policy.List(root)
	if err != nil || len(policies) != 1 || policies[0].MaxSteps == nil || *policies[0].MaxSteps != 4 {
		t.Fatalf("caller mutation leaked into policy: %#v err=%v", policies, err)
	}
	entries, err := api.runtime.Capabilities.Entries(root)
	if err != nil || len(entries) != 1 || entries[0].Manifest.Name != "Native Echo" ||
		entries[0].Manifest.Tool.Parameters["mutated"] != nil {
		t.Fatalf("caller mutation leaked into capability descriptor: %#v err=%v", entries, err)
	}
}

func TestNativeStrictValidatesEveryScopedProfileModel(t *testing.T) {
	root := nativeStrictTestRoot()
	t.Run("inherited fixed model", func(t *testing.T) {
		bootstrap := nativeStrictTestBootstrap(root)
		name := "Inherited profile"
		bootstrap.Profiles = append(bootstrap.Profiles, core.AgentProfileLayer{
			Scope: root, ProfileID: "native.inherited", Extends: bootstrap.DefaultProfileID, Name: &name,
		})
		api := newNativeStrictTestServer(t, openNativeStrictTestDB(t), bootstrap)
		profile, err := api.runtime.Profiles.Resolve(core.Principal{Scope: root}, root, "native.inherited")
		if err != nil || profile.Model != bootstrap.Model.Selection {
			t.Fatalf("inherited fixed model profile=%#v err=%v", profile, err)
		}
	})

	for _, test := range []struct {
		name  string
		layer core.AgentProfileLayer
		want  string
	}{
		{
			name:  "missing model",
			layer: core.AgentProfileLayer{Scope: root, ProfileID: "native.no-model"},
			want:  "model provider is empty",
		},
		{
			name:  "missing parent",
			layer: core.AgentProfileLayer{Scope: root, ProfileID: "native.missing-parent", Extends: "native.unknown-parent"},
			want:  "native.unknown-parent",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			bootstrap := nativeStrictTestBootstrap(root)
			bootstrap.Profiles = append(bootstrap.Profiles, test.layer)
			_, err := NewNativeStrictServer(context.Background(), NativeStrictServerConfig{
				DB: openNativeStrictTestDB(t), Dialect: storage.SQLDialectSQLite, Bootstrap: bootstrap,
			})
			if err == nil || !strings.Contains(err.Error(), test.layer.ProfileID) || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("invalid scoped profile error=%v", err)
			}
		})
	}
}

func TestNativeStrictRejectsDynamicDurableControl(t *testing.T) {
	root := nativeStrictTestRoot()
	for _, test := range []struct {
		name string
		seed func(t *testing.T, db *sql.DB, root core.ScopePath, bootstrap NativeStrictBootstrap)
		want string
	}{
		{
			name: "binding",
			seed: func(t *testing.T, db *sql.DB, _ core.ScopePath, _ NativeStrictBootstrap) {
				journal, err := storage.NewSQLBindingJournal(db, storage.SQLDialectSQLite)
				if err != nil {
					t.Fatal(err)
				}
				if err := journal.Record(context.Background(), storage.BindingRecord{ID: "native-binding", Kind: "policy"}); err != nil {
					t.Fatal(err)
				}
			},
			want: "binding",
		},
		{
			name: "release",
			seed: func(t *testing.T, db *sql.DB, root core.ScopePath, bootstrap NativeStrictBootstrap) {
				store, err := storage.OpenSQLSessionStore(context.Background(), db, storage.SQLDialectSQLite)
				if err != nil {
					t.Fatal(err)
				}
				layer := core.CloneAgentProfileLayer(bootstrap.Profiles[0])
				revision, err := core.ProfileLayerRevision(layer)
				if err != nil {
					t.Fatal(err)
				}
				if err := store.RecordRelease(context.Background(), control.ReleaseInfo{
					ProfileID: layer.ProfileID, Version: 1, Scope: root, Layer: &layer, Revision: revision, CreatedAt: time.Now().UTC(),
				}); err != nil {
					t.Fatal(err)
				}
			},
			want: "release",
		},
		{
			name: "canary",
			seed: func(t *testing.T, db *sql.DB, root core.ScopePath, bootstrap NativeStrictBootstrap) {
				store, err := storage.NewSQLCanaryStore(db, storage.SQLDialectSQLite)
				if err != nil {
					t.Fatal(err)
				}
				layer := core.CloneAgentProfileLayer(bootstrap.Profiles[0])
				revision, err := core.ProfileLayerRevision(layer)
				if err != nil {
					t.Fatal(err)
				}
				now := time.Now().UTC()
				if err := store.CreateCanary(context.Background(), control.CanaryRecord{
					ID: "native-canary", ProfileID: layer.ProfileID, Scope: root, Layer: &layer, Revision: revision,
					BaseReleaseRevision: strings.Repeat("0", 64), Status: control.CanaryActive, BasisPoints: 1,
					CandidateEvaluationRunID: "native-canary-evaluation", Gate: evaluation.GateResult{Passed: true},
					CreatedAt: now, UpdatedAt: now,
				}); err != nil {
					t.Fatal(err)
				}
			},
			want: "canary",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := openNativeStrictTestDB(t)
			bootstrap := nativeStrictTestBootstrap(root)
			if _, err := storage.OpenSQLSessionStore(context.Background(), db, storage.SQLDialectSQLite); err != nil {
				t.Fatal(err)
			}
			test.seed(t, db, root, bootstrap)
			_, err := NewNativeStrictServer(context.Background(), NativeStrictServerConfig{
				DB: db, Dialect: storage.SQLDialectSQLite, Bootstrap: bootstrap,
			})
			if err == nil || !errors.Is(err, storage.ErrNativeStrictDynamicControl) || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("dynamic %s artifact error=%v", test.name, err)
			}
		})
	}
}

func TestNativeStrictDynamicRoutesAreExplicitlyUnavailable(t *testing.T) {
	api := newNativeStrictTestServer(t, openNativeStrictTestDB(t), nativeStrictTestBootstrap(nativeStrictTestRoot()))
	for _, target := range []string{
		"/v1/admin/policies",
		"/v1/admin/credentials",
		"/v1/admin/capabilities/bind",
		"/v1/admin/capabilities/native.echo/disable",
		"/v1/profiles/native.agent/publish",
		"/v1/profiles/native.agent/rollback",
		"/v1/profiles/native.agent/canaries",
		"/v1/admin/storage/test",
	} {
		response := httptest.NewRecorder()
		api.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodPost, target, nil))
		if response.Code != http.StatusNotImplemented {
			t.Fatalf("%s status=%d body=%s", target, response.Code, response.Body.String())
		}
	}
	for _, target := range []string{
		"/v1/admin/profiles/native.agent",
		"/v1/admin/model-settings/llm",
		"/v1/resources/config",
		"/v1/admin/storage/resources",
		"/v1/admin/storage/sessions",
	} {
		response := httptest.NewRecorder()
		api.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodPut, target, nil))
		if response.Code != http.StatusNotImplemented {
			t.Fatalf("PUT %s status=%d body=%s", target, response.Code, response.Body.String())
		}
	}
}

func TestNativeStrictValidationRestartAndGenericSeparation(t *testing.T) {
	if _, err := NewNativeStrictServer(context.Background(), NativeStrictServerConfig{}); err == nil {
		t.Fatal("incomplete native strict config was accepted")
	}
	root := nativeStrictTestRoot()
	db := openNativeStrictTestDB(t)
	bootstrap := nativeStrictTestBootstrap(root)
	first := newNativeStrictTestServer(t, db, bootstrap)
	second := newNativeStrictTestServer(t, db, bootstrap)
	if first == second || first.nativeStrict == second.nativeStrict {
		t.Fatal("native strict restart reused server ownership state")
	}

	generic, err := New(Config{
		Runtime: &core.Runtime{}, Sessions: core.NewMemorySessionStore(),
		Authenticator: AuthenticatorFunc(func(*http.Request) (core.Principal, error) { return core.Principal{}, nil }),
	})
	if err != nil {
		t.Fatal(err)
	}
	if generic.nativeStrict != nil {
		t.Fatal("generic New acquired native strict ownership")
	}
	if generic.capabilityRuntimes == nil {
		t.Fatal("generic New no longer constructs its capability runtime registry")
	}
	if err := generic.MarkExecutionProjectionAppliedEpoch(context.Background()); err != nil {
		t.Fatalf("generic epoch marker behavior changed: %v", err)
	}

	bad := nativeStrictTestBootstrap(root)
	bad.Profiles[0].Model = &core.ModelSelection{Provider: "mock", Model: "wrong"}
	if _, err := NewNativeStrictServer(context.Background(), NativeStrictServerConfig{
		DB: db, Dialect: storage.SQLDialectSQLite, Bootstrap: bad,
	}); err == nil || !strings.Contains(err.Error(), "different model") {
		t.Fatalf("mismatched static model error=%v", err)
	}
}
