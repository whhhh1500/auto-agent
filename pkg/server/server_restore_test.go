package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	capabilityruntime "github.com/whhhh1500/auto-agent/pkg/app/capabilityruntime"
	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/extensions/runner"
	"github.com/whhhh1500/auto-agent/pkg/storage"
)

type restoreJournal struct{ records []storage.BindingRecord }

func (*restoreJournal) Record(context.Context, storage.BindingRecord) error { return nil }
func (*restoreJournal) Delete(context.Context, string) error                { return nil }
func (j *restoreJournal) List(context.Context) ([]storage.BindingRecord, error) {
	return j.records, nil
}

type retentionPrunerRecorder struct {
	audit, hits, tools, approvals, submissions []time.Time
	leaseCalls                                 int
}

func (r *retentionPrunerRecorder) PruneAudit(_ context.Context, cutoff time.Time) (int64, error) {
	r.audit = append(r.audit, cutoff)
	return 0, nil
}

func (r *retentionPrunerRecorder) PruneHits(_ context.Context, cutoff time.Time) (int64, error) {
	r.hits = append(r.hits, cutoff)
	return 0, nil
}

func (r *retentionPrunerRecorder) PruneExpiredLeases(context.Context) (int64, error) {
	r.leaseCalls++
	return 0, nil
}

func (r *retentionPrunerRecorder) PruneToolInvocations(_ context.Context, cutoff time.Time) (int64, error) {
	r.tools = append(r.tools, cutoff)
	return 0, nil
}

func (r *retentionPrunerRecorder) PruneApprovals(_ context.Context, cutoff time.Time) (int64, error) {
	r.approvals = append(r.approvals, cutoff)
	return 0, nil
}

func (r *retentionPrunerRecorder) PruneRunSubmissions(_ context.Context, cutoff time.Time) (int64, error) {
	r.submissions = append(r.submissions, cutoff)
	return 0, nil
}

type retentionRunnerStore struct {
	runner.Store
	cutoffs []time.Time
	deleted int64
	err     error
}

func (s *retentionRunnerStore) PruneTasks(_ context.Context, cutoff time.Time) (int64, error) {
	s.cutoffs = append(s.cutoffs, cutoff)
	return s.deleted, s.err
}

type queueOnlyRunnerStore struct{ runner.Store }

type retentionLogRecord struct {
	level   slog.Level
	message string
	attrs   map[string]any
}

type retentionLogHandler struct{ records []retentionLogRecord }

func (*retentionLogHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *retentionLogHandler) Handle(_ context.Context, record slog.Record) error {
	captured := retentionLogRecord{level: record.Level, message: record.Message, attrs: map[string]any{}}
	record.Attrs(func(attr slog.Attr) bool {
		captured.attrs[attr.Key] = attr.Value.Resolve().Any()
		return true
	})
	h.records = append(h.records, captured)
	return nil
}

func (h *retentionLogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *retentionLogHandler) WithGroup(string) slog.Handler      { return h }

func TestRestoreBindingsRollsBackPartialRestore(t *testing.T) {
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	policyPayload, _ := json.Marshal(map[string]any{"scope": global.String(), "deny": []string{"data.write"}})
	server := &Server{
		runtime: &core.Runtime{
			Capabilities: core.NewCapabilityRegistry(), Policy: core.NewPolicyRegistry(), Credentials: core.NewCredentialRegistry(),
		},
		journal: &restoreJournal{records: []storage.BindingRecord{
			{ID: "adm_1", Kind: "policy", Payload: policyPayload},
			{ID: "adm_2", Kind: "unknown", Payload: policyPayload},
		}},
	}
	if err := server.RestoreBindings(context.Background()); err == nil {
		t.Fatal("partial restore unexpectedly succeeded")
	}
	layers, err := server.runtime.Policy.List(global)
	if err != nil {
		t.Fatal(err)
	}
	if len(layers) != 0 || len(server.adminStateFor().handles) != 0 {
		t.Fatalf("partial restore left mounted state: layers=%#v handles=%#v", layers, server.adminStateFor().handles)
	}
}

func TestRestoreBindingsRunnerRuntimeAcrossServerRestart(t *testing.T) {
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	payload, err := json.Marshal(dynamicCapabilityJournalPayload{
		Capability: "runner.render", Scope: global.String(), Runtime: "runner",
		Manifest: core.CapabilityManifest{ID: "runner.render", Version: "1.0.0", Kind: "connector"},
	})
	if err != nil {
		t.Fatal(err)
	}
	restoredRegistry := core.NewCapabilityRegistry()
	server := &Server{
		runtime: &core.Runtime{Capabilities: restoredRegistry}, runners: runner.NewHub(),
		journal: &restoreJournal{records: []storage.BindingRecord{{ID: "adm_1", Kind: "capability", Payload: payload}}},
	}
	server.capabilityRuntimes, err = capabilityruntime.New(32, server.builtinCapabilityRuntimeFactories()...)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.RestoreBindings(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot, err := (core.CapabilityResolver{Registry: restoredRegistry}).Resolve(core.Principal{
		SubjectID: "operator", Scope: global, Grants: core.NewPermissionSet(),
	}, global)
	if err != nil {
		t.Fatal(err)
	}
	manifest, ok := snapshot.ManifestFor("runner.render")
	if !ok || !snapshot.Authorized("runner.render") || manifest.Execution == nil || manifest.Execution.Runtime != "runner" {
		t.Fatalf("restored runner capability found=%t authorized=%t manifest=%#v", ok, snapshot.Authorized("runner.render"), manifest)
	}
}

func TestRestoreBindingsRunnerUnavailableRollsBackAndFailsReadiness(t *testing.T) {
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	httpPayload, err := json.Marshal(dynamicCapabilityJournalPayload{
		Capability: "remote.ping", Scope: global.String(), Runtime: "http",
		Manifest:   core.CapabilityManifest{ID: "remote.ping", Version: "1.0.0", Kind: "connector"},
		Entrypoint: "https://93.184.216.34/ping", Method: "POST",
	})
	if err != nil {
		t.Fatal(err)
	}
	runnerPayload, err := json.Marshal(dynamicCapabilityJournalPayload{
		Capability: "runner.render", Scope: global.String(), Runtime: "runner",
		Manifest: core.CapabilityManifest{ID: "runner.render", Version: "1.0.0", Kind: "connector"},
	})
	if err != nil {
		t.Fatal(err)
	}
	registry := core.NewCapabilityRegistry()
	server := &Server{
		runtime: &core.Runtime{Capabilities: registry},
		journal: &restoreJournal{records: []storage.BindingRecord{
			{ID: "adm_1", Kind: "capability", Payload: httpPayload},
			{ID: "adm_2", Kind: "capability", Payload: runnerPayload},
		}},
	}
	if err := server.RestoreBindings(context.Background()); err == nil {
		t.Fatal("runner restore without a hub unexpectedly succeeded")
	}
	entries, err := registry.Entries(global)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 || len(server.adminStateFor().handles) != 0 {
		t.Fatalf("failed restore left bindings: entries=%#v handles=%#v", entries, server.adminStateFor().handles)
	}
	response := httptest.NewRecorder()
	server.handleReady(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("readiness status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestRestoreBindingsLegacyHTTPPayloadDefaultsRuntime(t *testing.T) {
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	legacyPayload, err := json.Marshal(map[string]any{
		"capability": "remote.ping", "scope": global.String(), "replace": false,
		"manifest":   map[string]any{"id": "remote.ping", "version": "1.0.0", "kind": "connector"},
		"entrypoint": "https://93.184.216.34/ping", "method": "POST",
	})
	if err != nil {
		t.Fatal(err)
	}
	registry := core.NewCapabilityRegistry()
	server := &Server{
		runtime: &core.Runtime{Capabilities: registry},
		journal: &restoreJournal{records: []storage.BindingRecord{{ID: "adm_legacy", Kind: "capability", Payload: legacyPayload}}},
	}
	server.capabilityRuntimes, err = capabilityruntime.New(32, server.builtinCapabilityRuntimeFactories()...)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.RestoreBindings(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot, err := (core.CapabilityResolver{Registry: registry}).Resolve(core.Principal{
		SubjectID: "operator", Scope: global, Grants: core.NewPermissionSet(),
	}, global)
	if err != nil {
		t.Fatal(err)
	}
	manifest, ok := snapshot.ManifestFor("remote.ping")
	if !ok || !snapshot.Authorized("remote.ping") || manifest.Execution == nil || manifest.Execution.Runtime != "http" || manifest.Execution.Entrypoint != "https://93.184.216.34/ping" {
		t.Fatalf("legacy HTTP restore found=%t authorized=%t manifest=%#v", ok, snapshot.Authorized("remote.ping"), manifest)
	}
}

func TestPruneRetentionOnceRunnerOnlyAndLoopGate(t *testing.T) {
	store := &retentionRunnerStore{Store: runner.NewMemoryStore(), deleted: 2}
	logs := &retentionLogHandler{}
	server := &Server{runners: runner.NewHubWithStore(store), logger: slog.New(logs)}
	if !server.retentionLoopEnabled() {
		t.Fatal("runner-only retention store did not enable the retention loop")
	}

	cutoff := time.Date(2026, time.September, 1, 2, 3, 4, 0, time.UTC)
	server.pruneRetentionOnce(context.Background(), cutoff)
	if len(store.cutoffs) != 1 || !store.cutoffs[0].Equal(cutoff) {
		t.Fatalf("runner retention cutoffs = %#v, want [%s]", store.cutoffs, cutoff)
	}
	if len(logs.records) != 1 {
		t.Fatalf("runner retention logs = %#v, want one success log", logs.records)
	}
	record := logs.records[0]
	if record.level != slog.LevelInfo || record.message != "retention prune" || record.attrs["table"] != "runner_tasks" || record.attrs["deleted"] != int64(2) {
		t.Fatalf("runner retention success log = %#v", record)
	}
}

func TestPruneRetentionOnceUsesOneCutoffForSQLAndRunner(t *testing.T) {
	bounded := &retentionPrunerRecorder{}
	runnerStore := &retentionRunnerStore{Store: runner.NewMemoryStore()}
	server := &Server{retention: bounded, runners: runner.NewHubWithStore(runnerStore)}
	if !server.retentionLoopEnabled() {
		t.Fatal("combined retention surfaces did not enable the retention loop")
	}

	cutoff := time.Date(2026, time.September, 1, 5, 6, 7, 890000000, time.UTC)
	server.pruneRetentionOnce(context.Background(), cutoff)
	for name, cutoffs := range map[string][]time.Time{
		"audit_events":      bounded.audit,
		"obs_hits":          bounded.hits,
		"tool_invocations":  bounded.tools,
		"approval_requests": bounded.approvals,
		"run_submissions":   bounded.submissions,
		"runner_tasks":      runnerStore.cutoffs,
	} {
		if len(cutoffs) != 1 || !cutoffs[0].Equal(cutoff) {
			t.Errorf("%s cutoffs = %#v, want [%s]", name, cutoffs, cutoff)
		}
	}
	if bounded.leaseCalls != 1 {
		t.Fatalf("session lease prune calls = %d, want 1", bounded.leaseCalls)
	}
}

func TestPruneRetentionOnceSilentlySkipsUnsupportedRunner(t *testing.T) {
	bounded := &retentionPrunerRecorder{}
	logs := &retentionLogHandler{}
	hub := runner.NewHubWithStore(queueOnlyRunnerStore{Store: runner.NewMemoryStore()})
	server := &Server{retention: bounded, runners: hub, logger: slog.New(logs)}
	if !server.retentionLoopEnabled() {
		t.Fatal("queue-only runner disabled the existing SQL retention loop")
	}
	server.pruneRetentionOnce(context.Background(), time.Date(2026, time.September, 1, 8, 9, 10, 0, time.UTC))
	if bounded.leaseCalls != 1 || len(bounded.audit) != 1 || len(bounded.submissions) != 1 {
		t.Fatalf("existing retention was not called: %#v", bounded)
	}
	if len(logs.records) != 0 {
		t.Fatalf("unsupported runner retention emitted logs: %#v", logs.records)
	}

	runnerOnly := &Server{runners: hub, logger: slog.New(logs)}
	if runnerOnly.retentionLoopEnabled() {
		t.Fatal("queue-only runner unexpectedly enabled the retention loop")
	}
	runnerOnly.pruneRetentionOnce(context.Background(), time.Date(2026, time.September, 1, 11, 12, 13, 0, time.UTC))
	if len(logs.records) != 0 {
		t.Fatalf("direct unsupported runner prune emitted logs: %#v", logs.records)
	}
}

func TestPruneRetentionOnceLogsRunnerErrors(t *testing.T) {
	pruneErr := errors.New("runner retention failed")
	store := &retentionRunnerStore{Store: runner.NewMemoryStore(), err: pruneErr}
	logs := &retentionLogHandler{}
	server := &Server{runners: runner.NewHubWithStore(store), logger: slog.New(logs)}
	server.pruneRetentionOnce(context.Background(), time.Date(2026, time.September, 1, 14, 15, 16, 0, time.UTC))

	if len(logs.records) != 1 {
		t.Fatalf("runner retention error logs = %#v, want one", logs.records)
	}
	record := logs.records[0]
	if record.level != slog.LevelError || record.message != "retention prune failed" || record.attrs["table"] != "runner_tasks" || record.attrs["error"] != pruneErr.Error() {
		t.Fatalf("runner retention error log = %#v", record)
	}
}
