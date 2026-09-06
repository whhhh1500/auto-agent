package server

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/cc-auto-agent/harness-core/pkg/buildinfo"
	"github.com/cc-auto-agent/harness-core/pkg/extensions/runner"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

type configJournalStub struct{}

type configRunQueueStub struct{ storage.RunQueueStore }

func (*configJournalStub) Record(context.Context, storage.BindingRecord) error { return nil }
func (*configJournalStub) Delete(context.Context, string) error                { return nil }
func (*configJournalStub) List(context.Context) ([]storage.BindingRecord, error) {
	return nil, nil
}

type failingBindingJournal struct{}

func (*failingBindingJournal) Record(context.Context, storage.BindingRecord) error {
	return errors.New("record failed")
}
func (*failingBindingJournal) Delete(context.Context, string) error {
	return errors.New("delete failed")
}
func (*failingBindingJournal) List(context.Context) ([]storage.BindingRecord, error) { return nil, nil }

type configRetentionStub struct{}

func (*configRetentionStub) PruneAudit(context.Context, time.Time) (int64, error) { return 0, nil }
func (*configRetentionStub) PruneHits(context.Context, time.Time) (int64, error)  { return 0, nil }
func (*configRetentionStub) PruneExpiredLeases(context.Context) (int64, error)    { return 0, nil }
func (*configRetentionStub) PruneToolInvocations(context.Context, time.Time) (int64, error) {
	return 0, nil
}
func (*configRetentionStub) PruneApprovals(context.Context, time.Time) (int64, error) { return 0, nil }
func (*configRetentionStub) PruneRunSubmissions(context.Context, time.Time) (int64, error) {
	return 0, nil
}

type configRunStatsStub struct{}

func (*configRunStatsStub) RecordRunStat(context.Context, storage.RunStat) error { return nil }
func (*configRunStatsStub) Metrics(context.Context, string) (storage.RunMetrics, error) {
	return storage.RunMetrics{}, nil
}

func TestBindingJournalFailureRollsBackMount(t *testing.T) {
	server := &Server{journal: &failingBindingJournal{}}
	unmounted := false
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	if _, err := server.addBinding(context.Background(), "policy", scope, map[string]any{"scope": "global:global"}, func() { unmounted = true }); err == nil {
		t.Fatal("journal failure must fail binding")
	}
	if !unmounted {
		t.Fatal("journal failure did not roll back mounted contribution")
	}
	if len(server.adminStateFor().handles) != 0 {
		t.Fatal("journal failure left admin state behind")
	}
}

func TestBindingDeleteFailureKeepsMount(t *testing.T) {
	server := &Server{journal: &failingBindingJournal{}}
	unmounted := false
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	id, err := server.addEphemeralBinding("policy", scope, map[string]any{}, func() { unmounted = true })
	if err != nil {
		t.Fatal(err)
	}
	principal := core.Principal{SubjectID: "admin", TenantID: "system", Scope: scope}
	found, err := server.unbindBinding(context.Background(), id, principal)
	if !found || err == nil {
		t.Fatalf("expected journal delete failure: found=%t err=%v", found, err)
	}
	if unmounted {
		t.Fatal("delete failure unmounted live contribution")
	}
	if _, ok := server.adminStateFor().handles[id]; !ok {
		t.Fatal("delete failure removed admin state")
	}
}

func TestReadyReportsRestoreFailure(t *testing.T) {
	server := &Server{}
	server.setReadyError(errors.New("restore failed"))
	response := httptest.NewRecorder()
	server.handleReady(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("unexpected readiness status: %d", response.Code)
	}
}

func TestHealthReportsBuildInfo(t *testing.T) {
	response := httptest.NewRecorder()
	(&Server{}).handleHealth(response, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	var payload map[string]string
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if response.Code != http.StatusOK || payload["status"] != "ok" ||
		payload["version"] != buildinfo.Version || payload["commit"] != buildinfo.Commit {
		t.Fatalf("health payload=%v status=%d", payload, response.Code)
	}
}

func TestNewRejectsUnsafeRunStaleThreshold(t *testing.T) {
	_, err := New(Config{
		Runtime: &core.Runtime{}, Sessions: core.NewMemorySessionStore(), RunControl: newFakeRunControlStore(),
		RunCancelPollInterval: time.Second, RunStaleAfter: 2 * time.Second,
		Authenticator: AuthenticatorFunc(func(*http.Request) (core.Principal, error) { return core.Principal{}, nil }),
	})
	if err == nil {
		t.Fatal("unsafe stale threshold was accepted")
	}
}

func TestNewRejectsDurableRunQueueWithoutSessionLeaser(t *testing.T) {
	_, err := New(Config{
		Runtime: &core.Runtime{}, Sessions: core.NewMemorySessionStore(), RunQueue: configRunQueueStub{},
		Authenticator: AuthenticatorFunc(func(*http.Request) (core.Principal, error) { return core.Principal{}, nil }),
	})
	if err == nil || !strings.Contains(err.Error(), "requires a session leaser") {
		t.Fatalf("durable queue without session leaser error=%v", err)
	}
}

func TestNewRejectsRunnerHubWithoutStore(t *testing.T) {
	_, err := New(Config{
		Runtime: &core.Runtime{}, Sessions: core.NewMemorySessionStore(), Runners: &runner.Hub{},
		Authenticator: AuthenticatorFunc(func(*http.Request) (core.Principal, error) { return core.Principal{}, nil }),
	})
	if err == nil {
		t.Fatal("runner hub without a store was accepted")
	}
}

func TestNewRunnerRecoveryInterval(t *testing.T) {
	tests := []struct {
		name     string
		runners  bool
		leaseTTL time.Duration
		interval time.Duration
		want     time.Duration
		wantErr  bool
	}{
		{name: "no runner", want: 0},
		{name: "default minimum", runners: true, leaseTTL: 1500 * time.Millisecond, want: time.Second},
		{name: "default half ttl", runners: true, leaseTTL: 8 * time.Second, want: 4 * time.Second},
		{name: "default maximum", runners: true, leaseTTL: 2 * time.Minute, want: 30 * time.Second},
		{name: "explicit", runners: true, leaseTTL: 8 * time.Second, interval: 3 * time.Second, want: 3 * time.Second},
		{name: "default exceeds short ttl", runners: true, leaseTTL: 500 * time.Millisecond, wantErr: true},
		{name: "negative", runners: true, leaseTTL: 8 * time.Second, interval: -time.Second, wantErr: true},
		{name: "exceeds ttl", runners: true, leaseTTL: 8 * time.Second, interval: 9 * time.Second, wantErr: true},
		{name: "without hub ignores interval", interval: time.Second, want: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := Config{
				Runtime: &core.Runtime{}, Sessions: core.NewMemorySessionStore(),
				Authenticator:          AuthenticatorFunc(func(*http.Request) (core.Principal, error) { return core.Principal{}, nil }),
				RunnerRecoveryInterval: test.interval,
			}
			if test.runners {
				hub := runner.NewHub()
				hub.LeaseTTL = test.leaseTTL
				config.Runners = hub
			}
			server, err := New(config)
			if test.wantErr {
				if err == nil {
					t.Fatal("New accepted an invalid runner recovery interval")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if server.runnerRecoveryInterval != test.want {
				t.Fatalf("runner recovery interval = %s, want %s", server.runnerRecoveryInterval, test.want)
			}
		})
	}
}

func TestNewWiresOptionalInfrastructure(t *testing.T) {
	journal := &configJournalStub{}
	retention := &configRetentionStub{}
	runStats := &configRunStatsStub{}
	server, err := New(Config{
		Runtime:  &core.Runtime{},
		Sessions: core.NewMemorySessionStore(),
		Authenticator: AuthenticatorFunc(func(*http.Request) (core.Principal, error) {
			return core.Principal{}, nil
		}),
		BindingJournal: journal,
		Retention:      retention,
		RunStats:       runStats,
	})
	if err != nil {
		t.Fatal(err)
	}
	if server.journal != journal {
		t.Fatal("binding journal was not wired into server")
	}
	if server.retention != retention {
		t.Fatal("retention pruner was not wired into server")
	}
	if server.runStats != runStats {
		t.Fatal("run stats store was not wired into server")
	}
}
func TestBearerToken(t *testing.T) {
	token, ok := bearerToken("Bearer abc")
	if !ok || token != "abc" {
		t.Fatalf("bearer token=%q ok=%t", token, ok)
	}
	if _, ok := bearerToken(""); ok {
		t.Fatal("empty header produced a bearer token")
	}
	if _, ok := bearerToken("Bearer "); ok {
		t.Fatal("empty bearer token was accepted")
	}
	if _, ok := bearerToken("Basic abc"); ok {
		t.Fatal("non-bearer header was treated as a token")
	}
	if _, ok := bearerToken("bearer abc"); ok {
		t.Fatal("lowercase bearer prefix was accepted")
	}
}

func TestAdminStateRejectsBindingOverflow(t *testing.T) {
	server := &Server{}
	state := server.adminStateFor()
	state.maxBindings = 2
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	unmounted := 0
	unmount := func() { unmounted++ }
	if _, err := server.addEphemeralBinding("policy", scope, map[string]any{"n": 1}, unmount); err != nil {
		t.Fatal(err)
	}
	if _, err := server.addEphemeralBinding("policy", scope, map[string]any{"n": 2}, unmount); err != nil {
		t.Fatal(err)
	}
	if _, err := server.addEphemeralBinding("policy", scope, map[string]any{"n": 3}, unmount); err == nil {
		t.Fatal("in-memory admin binding overflow was accepted")
	}
	if unmounted != 1 {
		t.Fatalf("overflow did not roll back the extra mount: unmounted=%d", unmounted)
	}
	if len(state.handles) != 2 {
		t.Fatalf("handles=%d, want 2", len(state.handles))
	}
	if _, err := state.add("policy", "adm_1", map[string]any{}, scope, unmount); err == nil {
		t.Fatal("duplicate binding id was accepted")
	}
}

func TestRegisterRunRejectsOverflow(t *testing.T) {
	server := &Server{runs: map[string]*activeRun{}, maxActiveRuns: 2}
	if err := server.registerRun("session-1", "run-1", func() {}); err != nil {
		t.Fatal(err)
	}
	if err := server.registerRun("session-2", "run-2", func() {}); err != nil {
		t.Fatal(err)
	}
	if err := server.registerRun("session-3", "run-3", func() {}); err == nil {
		t.Fatal("active run overflow was accepted")
	}
	if err := server.registerRun("session-1", "run-1b", func() {}); err != nil {
		t.Fatalf("replacing an active session run was blocked by the cap: %v", err)
	}
	server.clearRun("session-1", "run-1b")
	if err := server.registerRun("session-3", "run-3", func() {}); err != nil {
		t.Fatalf("cleared run did not free a slot: %v", err)
	}
}
