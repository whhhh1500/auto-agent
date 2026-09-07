package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
)

type projectionGateQueue struct {
	storage.RunQueueStore
	claims atomic.Int32
}

func (q *projectionGateQueue) ClaimRun(context.Context, string, time.Duration) (storage.QueuedRun, bool, error) {
	q.claims.Add(1)
	return storage.QueuedRun{}, false, nil
}

func TestProfileProjectionFailureBlocksRunsAndWorkerClaims(t *testing.T) {
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	server := newProfileAdminTestServer(scope, &profileReplaceJournal{}, core.NewAgentProfileRegistry())
	server.setProfileProjectionError("broken", errors.New("projection mismatch"))
	request := httptest.NewRequest(http.MethodPost, "/v1/sessions/missing/runs", bytes.NewBufferString(`{"message":"hello"}`))
	request.SetPathValue("id", "missing")
	response := httptest.NewRecorder()
	server.handleRun(response, request)
	if response.Code != http.StatusServiceUnavailable || response.Body.String() != "{\"error\":\"profile projection is unavailable\"}\n" {
		t.Fatalf("run gate status=%d body=%s", response.Code, response.Body.String())
	}
	queue := &projectionGateQueue{}
	server.runQueue = queue
	claimed, err := server.RunWorkerOnce(context.Background(), "projection-worker")
	if claimed || !errors.Is(err, errProfileProjectionUnavailable) || queue.claims.Load() != 0 {
		t.Fatalf("worker gate claimed=%t err=%v claims=%d", claimed, err, queue.claims.Load())
	}
	// Control-plane routes remain available so an operator can repair the projection.
	if response := invokeAdminProfilePut(t, server, scope, profileAdminLayer(scope, "repair.agent", "repair")); response.Code != http.StatusOK {
		t.Fatalf("admin repair route was blocked status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestProfileProjectionFailuresRequireMatchingRepairs(t *testing.T) {
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	server := newProfileAdminTestServer(scope, &profileReplaceJournal{}, core.NewAgentProfileRegistry())
	server.setProfileProjectionError("first", errors.New("first projection mismatch"))
	server.setProfileProjectionError("second", errors.New("second projection mismatch"))
	server.clearProfileProjectionError("second")
	if err := server.profileProjectionError(); err == nil {
		t.Fatal("repairing one projection failure cleared another binding failure")
	}
	server.clearProfileProjectionError("first")
	if err := server.profileProjectionError(); err != nil {
		t.Fatalf("all matching repairs should clear projection failures: %v", err)
	}
}

func TestRestoreBindingsDoesNotClearUnreconciledProfileProjectionFailure(t *testing.T) {
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	server := newProfileAdminTestServer(scope, &profileReplaceJournal{}, core.NewAgentProfileRegistry())
	server.setProfileProjectionError("broken", errors.New("durable and live profile differ"))
	if err := server.RestoreBindings(context.Background()); err != nil {
		t.Fatalf("empty restore: %v", err)
	}
	if err := server.profileProjectionError(); err == nil {
		t.Fatal("restore cleared an unreconciled profile projection failure")
	}
}

func TestAdminProfilePutSamePayloadReconcilesDurableLiveMismatch(t *testing.T) {
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	profiles := core.NewAgentProfileRegistry()
	journal := &profileReplaceJournal{}
	server := newProfileAdminTestServer(scope, journal, profiles)
	old := profileAdminLayer(scope, "reconcile.agent", "old")
	if response := invokeAdminProfilePut(t, server, scope, old); response.Code != http.StatusOK {
		t.Fatalf("initial put status=%d body=%s", response.Code, response.Body.String())
	}
	newLayer := profileAdminLayer(scope, old.ProfileID, "new")
	payload, err := json.Marshal(profileBindingPayload{Scope: scope.String(), Layer: newLayer})
	if err != nil {
		t.Fatal(err)
	}
	id := profileBindingID(old.ProfileID, scope)
	if err := journal.Replace(context.Background(), id, storage.BindingRecord{ID: id, Kind: profileBindingKind, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	server.setProfileProjectionError(id, errors.New("durable profile committed before live publication"))
	if got := resolvedProfileName(t, profiles, scope, old.ProfileID); got != "old" {
		t.Fatalf("test setup live profile=%q", got)
	}
	if response := invokeAdminProfilePut(t, server, scope, newLayer); response.Code != http.StatusOK {
		t.Fatalf("same payload reconcile status=%d body=%s", response.Code, response.Body.String())
	}
	if journal.replaceCalls != 1 {
		t.Fatalf("same payload reconcile rewrote durable binding: replaceCalls=%d", journal.replaceCalls)
	}
	if got := resolvedProfileName(t, profiles, scope, old.ProfileID); got != "new" {
		t.Fatalf("same payload did not reconcile live projection=%q", got)
	}
	ready := httptest.NewRecorder()
	server.handleReady(ready, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if ready.Code != http.StatusOK {
		t.Fatalf("successful reconcile did not clear projection readiness status=%d body=%s", ready.Code, ready.Body.String())
	}
}

func TestAdminProfilePutProjectionFailureGatesAndSamePayloadRepairUnblocks(t *testing.T) {
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	profiles := core.NewAgentProfileRegistry()
	journal := &profileReplaceJournal{}
	server := newProfileAdminTestServer(scope, journal, profiles)
	old := profileAdminLayer(scope, "recover.agent", "old")
	if response := invokeAdminProfilePut(t, server, scope, old); response.Code != http.StatusOK {
		t.Fatalf("initial put status=%d body=%s", response.Code, response.Body.String())
	}
	var unmountExtra func()
	journal.afterReplace = func() {
		var err error
		unmountExtra, err = profiles.Mount(old)
		if err != nil {
			panic(err)
		}
	}
	updated := profileAdminLayer(scope, old.ProfileID, "new")
	if response := invokeAdminProfilePut(t, server, scope, updated); response.Code != http.StatusServiceUnavailable {
		t.Fatalf("projection failure status=%d body=%s", response.Code, response.Body.String())
	}
	queue := &projectionGateQueue{}
	server.runQueue = queue
	if claimed, err := server.RunWorkerOnce(context.Background(), "projection-worker"); claimed || !errors.Is(err, errProfileProjectionUnavailable) || queue.claims.Load() != 0 {
		t.Fatalf("blocked worker claimed=%t err=%v claims=%d", claimed, err, queue.claims.Load())
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/sessions/missing/runs", bytes.NewBufferString(`{"message":"hello"}`))
	request.SetPathValue("id", "missing")
	response := httptest.NewRecorder()
	server.handleRun(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("blocked sync run status=%d body=%s", response.Code, response.Body.String())
	}
	unmountExtra()
	journal.afterReplace = nil
	if response := invokeAdminProfilePut(t, server, scope, updated); response.Code != http.StatusOK {
		t.Fatalf("same-payload repair status=%d body=%s", response.Code, response.Body.String())
	}
	claimed, err := server.RunWorkerOnce(context.Background(), "projection-worker")
	if claimed || err != nil || queue.claims.Load() != 1 {
		t.Fatalf("repaired worker claimed=%t err=%v claims=%d", claimed, err, queue.claims.Load())
	}
}

func TestAdminProfilePutMissingLiveSlotRequiresCleanRebuild(t *testing.T) {
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	profiles := core.NewAgentProfileRegistry()
	journal := &profileReplaceJournal{}
	server := newProfileAdminTestServer(scope, journal, profiles)
	old := profileAdminLayer(scope, "rebuild.agent", "old")
	if response := invokeAdminProfilePut(t, server, scope, old); response.Code != http.StatusOK {
		t.Fatalf("initial put status=%d body=%s", response.Code, response.Body.String())
	}
	id := profileBindingID(old.ProfileID, scope)
	binding, ok := server.adminStateFor().get(id)
	if !ok || binding.unmount == nil {
		t.Fatalf("missing initial binding: %#v", binding)
	}
	journal.afterReplace = binding.unmount
	updated := profileAdminLayer(scope, old.ProfileID, "new")
	if response := invokeAdminProfilePut(t, server, scope, updated); response.Code != http.StatusServiceUnavailable {
		t.Fatalf("missing live slot status=%d body=%s", response.Code, response.Body.String())
	}
	journal.afterReplace = nil
	if response := invokeAdminProfilePut(t, server, scope, updated); response.Code != http.StatusServiceUnavailable {
		t.Fatalf("same-payload must not remount at a new order status=%d body=%s", response.Code, response.Body.String())
	}
	if err := server.profileProjectionError(); err == nil {
		t.Fatal("missing live slot did not retain a projection fault")
	}
	server.readyMu.RLock()
	globalReadyErr := server.readyErr
	server.readyMu.RUnlock()
	if globalReadyErr != nil {
		t.Fatalf("binding projection fault polluted global readiness: %v", globalReadyErr)
	}
	restartedProfiles := core.NewAgentProfileRegistry()
	restarted := newProfileAdminTestServer(scope, journal, restartedProfiles)
	if err := restarted.RestoreBindings(context.Background()); err != nil {
		t.Fatalf("clean rebuild: %v", err)
	}
	if got := resolvedProfileName(t, restartedProfiles, scope, old.ProfileID); got != "new" {
		t.Fatalf("clean rebuild profile=%q", got)
	}
}
