package server

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
)

type profileReplaceJournal struct {
	mu             sync.Mutex
	records        []storage.BindingRecord
	listErr        error
	recordErr      error
	replaceErr     error
	recordCalls    int
	replaceCalls   int
	replaceEntered chan struct{}
	replaceRelease <-chan struct{}
	afterReplace   func()
	replaceOnce    sync.Once
}

type gatedSQLProfileReplaceJournal struct {
	*storage.SQLBindingJournal
	entered chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (j *gatedSQLProfileReplaceJournal) Replace(ctx context.Context, oldID string, record storage.BindingRecord) error {
	if err := j.SQLBindingJournal.Replace(ctx, oldID, record); err != nil {
		return err
	}
	j.once.Do(func() { close(j.entered) })
	select {
	case <-j.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (j *profileReplaceJournal) Record(_ context.Context, record storage.BindingRecord) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.recordCalls++
	if j.recordErr != nil {
		return j.recordErr
	}
	for _, existing := range j.records {
		if existing.ID == record.ID {
			return fmt.Errorf("binding %s already exists", record.ID)
		}
	}
	j.records = append(j.records, cloneProfileBindingRecord(record))
	return nil
}

func (j *profileReplaceJournal) Replace(ctx context.Context, oldID string, record storage.BindingRecord) error {
	j.mu.Lock()
	j.replaceCalls++
	if j.replaceErr != nil {
		j.mu.Unlock()
		return j.replaceErr
	}
	index := -1
	for i, existing := range j.records {
		if existing.ID == oldID {
			index = i
		}
		if existing.ID == record.ID && existing.ID != oldID {
			j.mu.Unlock()
			return fmt.Errorf("binding %s already exists", record.ID)
		}
	}
	if index < 0 {
		j.mu.Unlock()
		return fmt.Errorf("binding %s not found", oldID)
	}
	j.records[index] = cloneProfileBindingRecord(record)
	afterReplace := j.afterReplace
	entered, release := j.replaceEntered, j.replaceRelease
	j.mu.Unlock()
	if afterReplace != nil {
		afterReplace()
	}
	if entered != nil {
		j.replaceOnce.Do(func() { close(entered) })
	}
	if release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (j *profileReplaceJournal) Delete(_ context.Context, id string) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	for i, record := range j.records {
		if record.ID == id {
			j.records = append(j.records[:i], j.records[i+1:]...)
			return nil
		}
	}
	return fmt.Errorf("binding %s not found", id)
}

func (j *profileReplaceJournal) List(context.Context) ([]storage.BindingRecord, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.listErr != nil {
		return nil, j.listErr
	}
	out := make([]storage.BindingRecord, len(j.records))
	for i, record := range j.records {
		out[i] = cloneProfileBindingRecord(record)
	}
	return out, nil
}

func TestAdminProfilePutRedactsJournalListError(t *testing.T) {
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	journal := &profileReplaceJournal{listErr: fmt.Errorf("list failed for secret-profile-payload")}
	server := newProfileAdminTestServer(scope, journal, core.NewAgentProfileRegistry())
	response := invokeAdminProfilePut(t, server, scope, profileAdminLayer(scope, "list-error.agent", "next"))
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("list failure status=%d body=%s", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), "secret-profile-payload") || !strings.Contains(response.Body.String(), "durable profile update failed") {
		t.Fatalf("list error leaked or was not redacted: %s", response.Body.String())
	}
}

func cloneProfileBindingRecord(record storage.BindingRecord) storage.BindingRecord {
	record.Summary = append(json.RawMessage(nil), record.Summary...)
	record.Payload = append(json.RawMessage(nil), record.Payload...)
	return record
}

func newProfileAdminTestServer(scope core.ScopePath, journal storage.BindingJournal, profiles *core.AgentProfileRegistry) *Server {
	return &Server{
		runtime: &core.Runtime{Profiles: profiles}, journal: journal, maxBody: 1 << 20,
		authenticator: AuthenticatorFunc(func(*http.Request) (core.Principal, error) {
			return core.Principal{SubjectID: "admin", Scope: scope, Attributes: map[string]string{"role": storage.RoleAccountAdmin}}, nil
		}),
		sessions: core.NewMemorySessionStore(),
	}
}

func profileAdminLayer(scope core.ScopePath, profileID, name string) core.AgentProfileLayer {
	model := core.ModelSelection{Provider: "mock", Model: "mock"}
	return core.AgentProfileLayer{Scope: scope, ProfileID: profileID, Name: &name, Model: &model}
}

func invokeAdminProfilePut(t *testing.T, server *Server, scope core.ScopePath, layer core.AgentProfileLayer) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(adminProfileWriteRequest{Scope: scope, Layer: layer})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPut, "/v1/admin/profiles/"+layer.ProfileID, bytes.NewReader(body))
	request.SetPathValue("id", layer.ProfileID)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	server.handleAdminProfilePut(response, request)
	return response
}

func resolvedProfileName(t *testing.T, profiles *core.AgentProfileRegistry, scope core.ScopePath, profileID string) string {
	t.Helper()
	profile, err := profiles.Resolve(core.Principal{Scope: scope}, scope, profileID)
	if err != nil {
		t.Fatal(err)
	}
	return profile.Name
}

func TestAdminProfilePutRequiresAtomicReplacement(t *testing.T) {
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	profiles := core.NewAgentProfileRegistry()
	journal := &profileRestoreJournal{}
	server := newProfileAdminTestServer(scope, journal, profiles)
	old := profileAdminLayer(scope, "replace.agent", "old")
	payload, err := json.Marshal(profileBindingPayload{Scope: scope.String(), Layer: old})
	if err != nil {
		t.Fatal(err)
	}
	id := profileBindingID(old.ProfileID, scope)
	journal.records = []storage.BindingRecord{{ID: id, Kind: profileBindingKind, Payload: payload}}
	unmount, err := profiles.Mount(old)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.adminStateFor().add(profileBindingKind, id, profileBindingPayload{Scope: scope.String(), Layer: old}, scope, unmount); err != nil {
		t.Fatal(err)
	}
	response := invokeAdminProfilePut(t, server, scope, profileAdminLayer(scope, old.ProfileID, "new"))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("replacement without optional journal status=%d body=%s", response.Code, response.Body.String())
	}
	if got := resolvedProfileName(t, profiles, scope, old.ProfileID); got != "old" {
		t.Fatalf("unsupported replacement changed live profile to %q", got)
	}
	if len(journal.records) != 1 || !profilePayloadEqual(journal.records[0].Payload, profileBindingPayload{Scope: scope.String(), Layer: old}) {
		t.Fatalf("unsupported replacement changed durable record: %#v", journal.records)
	}
}

func TestAdminProfilePutRejectsInvalidCandidateBeforeDurableWrite(t *testing.T) {
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	profiles := core.NewAgentProfileRegistry()
	journal := &profileReplaceJournal{}
	server := newProfileAdminTestServer(scope, journal, profiles)
	layer := profileAdminLayer(scope, "invalid.agent", "invalid")
	layer.PutFragments = []core.PromptFragment{{Section: "instructions", Content: "valid content"}}
	response := invokeAdminProfilePut(t, server, scope, layer)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("invalid candidate status=%d body=%s", response.Code, response.Body.String())
	}
	if journal.recordCalls != 0 || journal.replaceCalls != 0 {
		t.Fatalf("invalid candidate touched journal: record=%d replace=%d", journal.recordCalls, journal.replaceCalls)
	}
	if _, err := profiles.Resolve(core.Principal{Scope: scope}, scope, layer.ProfileID); err == nil {
		t.Fatal("invalid candidate was published live")
	}
}

func TestAdminProfilePutPersistsBeforeLivePublication(t *testing.T) {
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	profiles := core.NewAgentProfileRegistry()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "profile-replace.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := storage.OpenSQLSessionStore(context.Background(), db, storage.SQLDialectSQLite); err != nil {
		t.Fatal(err)
	}
	baseJournal, err := storage.NewSQLBindingJournal(db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	journal := &gatedSQLProfileReplaceJournal{SQLBindingJournal: baseJournal, entered: make(chan struct{}), release: release}
	server := newProfileAdminTestServer(scope, journal, profiles)
	old := profileAdminLayer(scope, "ordered.agent", "old")
	if response := invokeAdminProfilePut(t, server, scope, old); response.Code != http.StatusOK {
		t.Fatalf("initial put status=%d body=%s", response.Code, response.Body.String())
	}
	result := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		result <- invokeAdminProfilePut(t, server, scope, profileAdminLayer(scope, old.ProfileID, "new"))
	}()
	select {
	case <-journal.entered:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for replacement durable gate")
	}
	if got := resolvedProfileName(t, profiles, scope, old.ProfileID); got != "old" {
		t.Fatalf("live profile changed before durable replacement returned: %q", got)
	}
	records, err := journal.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || !profilePayloadEqual(records[0].Payload, profileBindingPayload{Scope: scope.String(), Layer: profileAdminLayer(scope, old.ProfileID, "new")}) {
		t.Fatalf("durable replacement missing at gate: %#v", records)
	}
	close(release)
	response := <-result
	if response.Code != http.StatusOK {
		t.Fatalf("replacement status=%d body=%s", response.Code, response.Body.String())
	}
	if got := resolvedProfileName(t, profiles, scope, old.ProfileID); got != "new" {
		t.Fatalf("live profile after replacement=%q", got)
	}
}

func TestAdminProfilePutReplacementFailureAndRestartRestore(t *testing.T) {
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	profiles := core.NewAgentProfileRegistry()
	journal := &profileReplaceJournal{}
	server := newProfileAdminTestServer(scope, journal, profiles)
	old := profileAdminLayer(scope, "durable.agent", "old")
	if response := invokeAdminProfilePut(t, server, scope, old); response.Code != http.StatusOK {
		t.Fatalf("initial put status=%d body=%s", response.Code, response.Body.String())
	}
	journal.replaceErr = fmt.Errorf("replace failed for payload secret-profile-value")
	if response := invokeAdminProfilePut(t, server, scope, profileAdminLayer(scope, old.ProfileID, "rejected")); response.Code != http.StatusInternalServerError {
		t.Fatalf("replace failure status=%d body=%s", response.Code, response.Body.String())
	} else if strings.Contains(response.Body.String(), "secret-profile-value") || !strings.Contains(response.Body.String(), "durable profile update failed") {
		t.Fatalf("journal error leaked or was not redacted: %s", response.Body.String())
	}
	if got := resolvedProfileName(t, profiles, scope, old.ProfileID); got != "old" {
		t.Fatalf("failed replacement changed live profile=%q", got)
	}
	journal.replaceErr = nil
	if response := invokeAdminProfilePut(t, server, scope, profileAdminLayer(scope, old.ProfileID, "new")); response.Code != http.StatusOK {
		t.Fatalf("replacement status=%d body=%s", response.Code, response.Body.String())
	}
	if journal.recordCalls != 1 || journal.replaceCalls != 2 {
		t.Fatalf("journal calls record=%d replace=%d", journal.recordCalls, journal.replaceCalls)
	}
	if response := invokeAdminProfilePut(t, server, scope, profileAdminLayer(scope, old.ProfileID, "new")); response.Code != http.StatusOK {
		t.Fatalf("same payload status=%d body=%s", response.Code, response.Body.String())
	}
	if journal.replaceCalls != 2 {
		t.Fatalf("same payload invoked replace: calls=%d", journal.replaceCalls)
	}
	restartedProfiles := core.NewAgentProfileRegistry()
	restarted := newProfileAdminTestServer(scope, journal, restartedProfiles)
	if err := restarted.RestoreBindings(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := resolvedProfileName(t, restartedProfiles, scope, old.ProfileID); got != "new" {
		t.Fatalf("restart restored profile=%q", got)
	}
}

func TestAdminProfilePutReplacementWorksAtRegistryCapacity(t *testing.T) {
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	profiles := core.NewAgentProfileRegistry()
	journal := &profileReplaceJournal{}
	server := newProfileAdminTestServer(scope, journal, profiles)
	old := profileAdminLayer(scope, "projection.agent", "old")
	if response := invokeAdminProfilePut(t, server, scope, old); response.Code != http.StatusOK {
		t.Fatalf("initial put status=%d body=%s", response.Code, response.Body.String())
	}
	journal.afterReplace = func() {
		for i := 0; i < core.MaxRegistryBindings-1; i++ {
			if _, err := profiles.Mount(core.AgentProfileLayer{Scope: scope, ProfileID: fmt.Sprintf("fill-%d", i)}); err != nil {
				panic(err)
			}
		}
	}
	response := invokeAdminProfilePut(t, server, scope, profileAdminLayer(scope, old.ProfileID, "new"))
	if response.Code != http.StatusOK {
		t.Fatalf("replacement at registry capacity status=%d body=%s", response.Code, response.Body.String())
	}
	if got := resolvedProfileName(t, profiles, scope, old.ProfileID); got != "new" {
		t.Fatalf("replacement at registry capacity live profile=%q", got)
	}
	records, err := journal.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || !profilePayloadEqual(records[0].Payload, profileBindingPayload{Scope: scope.String(), Layer: profileAdminLayer(scope, old.ProfileID, "new")}) {
		t.Fatalf("durable replacement missing at registry capacity: %#v", records)
	}
	ready := httptest.NewRecorder()
	server.handleReady(ready, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if ready.Code != http.StatusOK {
		t.Fatalf("replacement at registry capacity readiness=%d body=%s", ready.Code, ready.Body.String())
	}
}

func TestAdminProfilePutSerializesWithProfileBindingUnbind(t *testing.T) {
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	profiles := core.NewAgentProfileRegistry()
	release := make(chan struct{})
	journal := &profileReplaceJournal{replaceEntered: make(chan struct{}), replaceRelease: release}
	server := newProfileAdminTestServer(scope, journal, profiles)
	old := profileAdminLayer(scope, "serialized.agent", "old")
	if response := invokeAdminProfilePut(t, server, scope, old); response.Code != http.StatusOK {
		t.Fatalf("initial put status=%d body=%s", response.Code, response.Body.String())
	}

	putDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		putDone <- invokeAdminProfilePut(t, server, scope, profileAdminLayer(scope, old.ProfileID, "new"))
	}()
	select {
	case <-journal.replaceEntered:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for profile replacement")
	}
	principal := core.Principal{TenantID: "tenant", SubjectID: "admin", Scope: scope}
	type unbindResult struct {
		found bool
		err   error
	}
	unbound := make(chan unbindResult, 1)
	go func() {
		found, err := server.unbindBinding(context.Background(), profileBindingID(old.ProfileID, scope), principal)
		unbound <- unbindResult{found: found, err: err}
	}()
	select {
	case result := <-unbound:
		t.Fatalf("profile unbind escaped the profile mutation order: found=%t err=%v", result.found, result.err)
	case <-time.After(40 * time.Millisecond):
	}
	close(release)
	select {
	case response := <-putDone:
		if response.Code != http.StatusOK {
			t.Fatalf("replacement status=%d body=%s", response.Code, response.Body.String())
		}
	case <-time.After(time.Second):
		t.Fatal("profile replacement did not complete")
	}
	select {
	case result := <-unbound:
		if !result.found || result.err != nil {
			t.Fatalf("profile unbind found=%t err=%v", result.found, result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("profile unbind did not complete")
	}
	if records, err := journal.List(context.Background()); err != nil || len(records) != 0 {
		t.Fatalf("durable profile records=%#v err=%v", records, err)
	}
	if _, err := profiles.Resolve(principal, scope, old.ProfileID); err == nil {
		t.Fatal("profile binding remained mounted after serialized unbind")
	}
}

func TestAdminProfilePutRejectsInvalidResolvedCandidateBeforeDurableWrite(t *testing.T) {
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	profiles := core.NewAgentProfileRegistry()
	journal := &profileReplaceJournal{}
	server := newProfileAdminTestServer(scope, journal, profiles)
	layer := profileAdminLayer(scope, "broken.agent", "broken")
	layer.Extends = "missing.base"
	response := invokeAdminProfilePut(t, server, scope, layer)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("invalid candidate status=%d body=%s", response.Code, response.Body.String())
	}
	records, err := journal.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("invalid candidate created durable record: %#v", records)
	}
	ready := httptest.NewRecorder()
	server.handleReady(ready, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if ready.Code != http.StatusOK {
		t.Fatalf("invalid candidate changed readiness=%d body=%s", ready.Code, ready.Body.String())
	}
}

func TestAdminProfilePutConcurrentReplacementsKeepOneDurableLayer(t *testing.T) {
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	profiles := core.NewAgentProfileRegistry()
	journal := &profileReplaceJournal{}
	server := newProfileAdminTestServer(scope, journal, profiles)
	old := profileAdminLayer(scope, "concurrent.agent", "old")
	if response := invokeAdminProfilePut(t, server, scope, old); response.Code != http.StatusOK {
		t.Fatalf("initial put status=%d body=%s", response.Code, response.Body.String())
	}
	responses := make(chan *httptest.ResponseRecorder, 2)
	for _, name := range []string{"one", "two"} {
		name := name
		go func() {
			responses <- invokeAdminProfilePut(t, server, scope, profileAdminLayer(scope, old.ProfileID, name))
		}()
	}
	for range 2 {
		if response := <-responses; response.Code != http.StatusOK {
			t.Fatalf("concurrent replacement status=%d body=%s", response.Code, response.Body.String())
		}
	}
	records, err := journal.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("concurrent replacement records=%#v", records)
	}
	var payload profileBindingPayload
	if err := json.Unmarshal(records[0].Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Layer.Name == nil {
		t.Fatalf("final profile payload=%#v", payload)
	}
	if got := resolvedProfileName(t, profiles, scope, old.ProfileID); got != *payload.Layer.Name {
		t.Fatalf("live=%q durable=%q", got, *payload.Layer.Name)
	}
}

func TestAdminProfilePutReplacementPreservesLaterLayerPrecedence(t *testing.T) {
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	profiles := core.NewAgentProfileRegistry()
	journal := &profileReplaceJournal{}
	server := newProfileAdminTestServer(scope, journal, profiles)
	old := profileAdminLayer(scope, "precedence.agent", "old")
	if response := invokeAdminProfilePut(t, server, scope, old); response.Code != http.StatusOK {
		t.Fatalf("initial put status=%d body=%s", response.Code, response.Body.String())
	}
	if _, err := profiles.Mount(profileAdminLayer(scope, old.ProfileID, "later")); err != nil {
		t.Fatal(err)
	}
	if response := invokeAdminProfilePut(t, server, scope, profileAdminLayer(scope, old.ProfileID, "new")); response.Code != http.StatusOK {
		t.Fatalf("replacement status=%d body=%s", response.Code, response.Body.String())
	}
	if got := resolvedProfileName(t, profiles, scope, old.ProfileID); got != "later" {
		t.Fatalf("replacement changed later-layer precedence to %q", got)
	}
}

func TestAdminProfilePutRejectsDuplicateDurableProfileBindings(t *testing.T) {
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	journal := &profileReplaceJournal{}
	server := newProfileAdminTestServer(scope, journal, core.NewAgentProfileRegistry())
	layer := profileAdminLayer(scope, "duplicate.agent", "first")
	if response := invokeAdminProfilePut(t, server, scope, layer); response.Code != http.StatusOK {
		t.Fatalf("initial put status=%d body=%s", response.Code, response.Body.String())
	}
	duplicate := profileAdminLayer(scope, layer.ProfileID, "second")
	payload, err := json.Marshal(profileBindingPayload{Scope: scope.String(), Layer: duplicate})
	if err != nil {
		t.Fatal(err)
	}
	journal.records = append(journal.records, storage.BindingRecord{ID: "legacy-duplicate", Kind: profileBindingKind, Payload: payload})
	if response := invokeAdminProfilePut(t, server, scope, profileAdminLayer(scope, layer.ProfileID, "third")); response.Code != http.StatusInternalServerError {
		t.Fatalf("duplicate durable bindings status=%d body=%s", response.Code, response.Body.String())
	}
	if got := resolvedProfileName(t, server.runtime.Profiles, scope, layer.ProfileID); got != "first" {
		t.Fatalf("duplicate durable binding changed live projection=%q", got)
	}
}
