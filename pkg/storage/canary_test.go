package storage

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/cc-auto-agent/harness-core/pkg/control"
	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/evaluation"
)

type countingReleaseJournal struct {
	*SQLSessionStore
	listCalls int
}

func (j *countingReleaseJournal) ListReleaseProfiles(ctx context.Context) ([]string, error) {
	j.listCalls++
	return j.SQLSessionStore.ListReleaseProfiles(ctx)
}

func sqlCanaryTestRecord(t *testing.T, id string, scope core.ScopePath) control.CanaryRecord {
	t.Helper()
	name := "SQL Candidate"
	layer := core.AgentProfileLayer{Scope: scope, ProfileID: "storage.agent", Name: &name}
	revision, err := core.ProfileLayerRevision(layer)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	return control.CanaryRecord{
		ID: id, ProfileID: layer.ProfileID, Scope: scope, Layer: &layer, Revision: revision,
		BaseReleaseRevision: strings.Repeat("0", 64),
		Status:              control.CanaryActive, BasisPoints: 2500,
		CandidateEvaluationRunID: "evaluation-storage-candidate",
		Gate: evaluation.GateResult{
			Passed: true, Reasons: []string{"passed"},
			CapabilityCompatibility: &evaluation.CapabilityCompatibilityResult{
				Compatible: true, BaselineSnapshotID: "baseline-capabilities",
				CandidateSnapshotID: "candidate-capabilities", Added: []string{"storage.safe"},
			},
		},
		CreatedAt: now, UpdatedAt: now,
	}
}

func TestSQLCanaryStoreRoundTripAndExpectedStatusCAS(t *testing.T) {
	ctx := context.Background()
	sessions := newTestSQLStore(t)
	store, err := NewSQLCanaryStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	_, product, _, _ := testScopes()
	record := sqlCanaryTestRecord(t, "canary-sql-roundtrip", product)
	beforeRevision, err := store.ControlRevision(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateCanary(ctx, record); err != nil {
		t.Fatal(err)
	}
	afterCreateRevision, err := store.ControlRevision(ctx)
	if err != nil || afterCreateRevision != beforeRevision+1 {
		t.Fatalf("canary create revision=%d, want %d err=%v", afterCreateRevision, beforeRevision+1, err)
	}

	loaded, err := store.GetCanary(ctx, record.ID)
	if err != nil || loaded.Revision != record.Revision || loaded.BaseReleaseRevision != record.BaseReleaseRevision ||
		loaded.Gate.CapabilityCompatibility == nil || !loaded.Gate.CapabilityCompatibility.Compatible ||
		loaded.Gate.CapabilityCompatibility.CandidateSnapshotID != "candidate-capabilities" ||
		loaded.Layer == nil || loaded.Layer.Name == nil || *loaded.Layer.Name != "SQL Candidate" {
		t.Fatalf("canary round trip failed: %#v err=%v", loaded, err)
	}
	loaded.Status = control.CanaryPaused
	loaded.BasisPoints = 5000
	loaded.UpdatedAt = loaded.UpdatedAt.Add(time.Second)
	changed, err := store.UpdateCanary(ctx, loaded, control.CanaryActive)
	if err != nil || !changed {
		t.Fatalf("expected-status update failed: changed=%t err=%v", changed, err)
	}
	loaded.Status = control.CanaryRolledBack
	loaded.UpdatedAt = loaded.UpdatedAt.Add(time.Second)
	changed, err = store.UpdateCanary(ctx, loaded, control.CanaryActive)
	if err != nil || changed {
		t.Fatalf("stale expected status changed row: changed=%t err=%v", changed, err)
	}
	afterStaleRevision, err := store.ControlRevision(ctx)
	if err != nil || afterStaleRevision != afterCreateRevision+1 {
		t.Fatalf("stale CAS changed control revision: revision=%d want=%d err=%v", afterStaleRevision, afterCreateRevision+1, err)
	}
	rows, err := store.ListCanaries(ctx, record.ProfileID, 10)
	if err != nil || len(rows) != 1 || rows[0].Status != control.CanaryPaused || rows[0].BasisPoints != 5000 {
		t.Fatalf("canary list lost persisted state: %#v err=%v", rows, err)
	}
	open, err := store.ListOpenCanaries(ctx)
	if err != nil || len(open) != 1 || open[0].Status != control.CanaryPaused {
		t.Fatalf("open canary query wrong: %#v err=%v", open, err)
	}
}

func TestSQLCanaryStoreEnforcesOneActiveCanaryPerProfileScope(t *testing.T) {
	ctx := context.Background()
	sessions := newTestSQLStore(t)
	store, err := NewSQLCanaryStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	_, product, _, _ := testScopes()
	first := sqlCanaryTestRecord(t, "canary-sql-first", product)
	if err := store.CreateCanary(ctx, first); err != nil {
		t.Fatal(err)
	}
	second := sqlCanaryTestRecord(t, "canary-sql-second", product)
	if err := store.CreateCanary(ctx, second); err == nil {
		t.Fatal("database accepted two active canaries for the same profile and scope")
	}
}

func TestSQLCanaryPromotionRecoversAfterReleaseWasRecorded(t *testing.T) {
	ctx := context.Background()
	sessions := newTestSQLStore(t)
	canaryStore, err := NewSQLCanaryStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	_, product, _, _ := testScopes()
	stableName := "Stable"
	stableModel := core.ModelSelection{Provider: "test", Model: "stable"}
	newProfiles := func() *core.AgentProfileRegistry {
		profiles := core.NewAgentProfileRegistry()
		if err := profiles.Bind(core.AgentProfileLayer{
			Scope: product, ProfileID: "storage.agent", Name: &stableName, Model: &stableModel,
		}); err != nil {
			t.Fatal(err)
		}
		return profiles
	}

	firstReleases, _ := control.NewReleaseManager(newProfiles())
	firstReleases.Journal = sessions
	if err := firstReleases.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	firstCanaries, err := control.NewCanaryManager(firstReleases, canaryStore)
	if err != nil {
		t.Fatal(err)
	}
	if err := firstCanaries.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	record := sqlCanaryTestRecord(t, "canary-recover-promotion", product)
	record.BasisPoints = 10000
	record.BaseReleaseRevision = ""
	if _, err := firstCanaries.Stage(ctx, record); err != nil {
		t.Fatal(err)
	}

	// Simulate a process stopping after the release journal commit but before
	// the canary's final promoted state was persisted.
	promoting, err := canaryStore.GetCanary(ctx, record.ID)
	if err != nil {
		t.Fatal(err)
	}
	promoting.Status = control.CanaryPromoting
	promoting.UpdatedAt = promoting.UpdatedAt.Add(time.Second)
	if changed, err := canaryStore.UpdateCanary(ctx, promoting, control.CanaryActive); err != nil || !changed {
		t.Fatalf("mark promoting: changed=%t err=%v", changed, err)
	}
	if _, err := firstReleases.PublishWithOperationAtBaseline(
		ctx, record.Scope, *record.Layer, record.ID, promoting.BaseReleaseRevision,
	); err != nil {
		t.Fatal(err)
	}

	restoredProfiles := newProfiles()
	restoredReleases, _ := control.NewReleaseManager(restoredProfiles)
	restoredReleases.Journal = sessions
	if err := restoredReleases.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	restoredCanaries, err := control.NewCanaryManager(restoredReleases, canaryStore)
	if err != nil {
		t.Fatal(err)
	}
	if err := restoredCanaries.Restore(ctx); err != nil {
		t.Fatal(err)
	}

	recovered, err := canaryStore.GetCanary(ctx, record.ID)
	if err != nil || recovered.Status != control.CanaryPromoted || recovered.ReleaseVersion != 1 {
		t.Fatalf("promotion was not recovered: %#v err=%v", recovered, err)
	}
	history := restoredReleases.History(ctx, record.ProfileID)
	if len(history) != 1 || history[0].OperationID != record.ID || history[0].Version != 1 {
		t.Fatalf("recovery duplicated release: %#v", history)
	}
	principal := core.Principal{TenantID: "acme", SubjectID: "alice", Scope: product, Grants: core.NewPermissionSet(core.PermRead)}
	resolved, err := restoredProfiles.Resolve(principal, product, record.ProfileID)
	if err != nil || resolved.Name != "SQL Candidate" {
		t.Fatalf("promoted release was not restored live: %#v err=%v", resolved, err)
	}
}

func TestReleaseAndCanaryRefreshAcrossManagers(t *testing.T) {
	ctx := context.Background()
	sessions := newTestSQLStore(t)
	canaryStore, err := NewSQLCanaryStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	_, product, _, _ := testScopes()
	principal := core.Principal{
		TenantID: "acme", SubjectID: "alice", Scope: product,
		Grants: core.NewPermissionSet(core.PermRead),
	}
	newProfiles := func() *core.AgentProfileRegistry {
		profiles := core.NewAgentProfileRegistry()
		name := "Stable"
		model := core.ModelSelection{Provider: "test", Model: "stable"}
		if err := profiles.Bind(core.AgentProfileLayer{
			Scope: product, ProfileID: "storage.agent", Name: &name, Model: &model,
		}); err != nil {
			t.Fatal(err)
		}
		return profiles
	}
	newManagers := func() (*core.AgentProfileRegistry, *control.ReleaseManager, *control.CanaryManager) {
		profiles := newProfiles()
		releases, err := control.NewReleaseManager(profiles)
		if err != nil {
			t.Fatal(err)
		}
		releases.Journal = sessions
		if err := releases.Restore(ctx); err != nil {
			t.Fatal(err)
		}
		canaries, err := control.NewCanaryManager(releases, canaryStore)
		if err != nil {
			t.Fatal(err)
		}
		if err := canaries.Restore(ctx); err != nil {
			t.Fatal(err)
		}
		return profiles, releases, canaries
	}
	resolveName := func(profiles *core.AgentProfileRegistry) string {
		resolved, err := profiles.Resolve(principal, product, "storage.agent")
		if err != nil {
			t.Fatal(err)
		}
		return resolved.Name
	}

	_, releasesA, canariesA := newManagers()
	profilesB, releasesB, canariesB := newManagers()
	record := sqlCanaryTestRecord(t, "canary-cross-manager-refresh", product)
	record.BaseReleaseRevision = ""
	record.BasisPoints = 10000
	staged, err := canariesA.Stage(ctx, record)
	if err != nil {
		t.Fatal(err)
	}
	if _, selected := canariesB.Select(principal, record.ProfileID); selected != nil {
		t.Fatal("second manager observed a canary before refresh")
	}
	if err := canariesB.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	profiles, selected := canariesB.Select(principal, record.ProfileID)
	if selected == nil || selected.ID != staged.ID || resolveName(profiles) != "SQL Candidate" {
		t.Fatalf("second manager did not refresh staged candidate: %#v", selected)
	}

	if _, err := canariesA.Pause(ctx, staged.ID); err != nil {
		t.Fatal(err)
	}
	if err := canariesB.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	profiles, selected = canariesB.Select(principal, record.ProfileID)
	if selected != nil || resolveName(profiles) != "Stable" {
		t.Fatal("second manager did not refresh paused state")
	}
	if _, err := canariesA.Resume(ctx, staged.ID); err != nil {
		t.Fatal(err)
	}
	if err := canariesB.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if _, selected = canariesB.Select(principal, record.ProfileID); selected == nil {
		t.Fatal("second manager did not refresh resumed state")
	}

	promoted, err := canariesA.Promote(ctx, staged.ID)
	if err != nil || promoted.ReleaseVersion != 1 {
		t.Fatalf("first manager promotion failed: %#v err=%v", promoted, err)
	}
	if err := canariesB.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	profiles, selected = canariesB.Select(principal, record.ProfileID)
	if selected != nil || profiles != releasesB.Profiles || resolveName(profilesB) != "SQL Candidate" {
		t.Fatalf("second manager did not sync promoted live release: selected=%#v name=%q", selected, resolveName(profilesB))
	}

	if rolledBack, err := releasesA.Rollback(ctx, record.ProfileID, 0); err != nil || len(rolledBack) != 1 {
		t.Fatalf("first manager rollback failed: %#v err=%v", rolledBack, err)
	}
	if err := canariesB.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if name := resolveName(profilesB); name != "Stable" {
		t.Fatalf("second manager did not sync external rollback: %q", name)
	}
}

func TestReleaseSyncUsesControlRevisionFastPath(t *testing.T) {
	ctx := context.Background()
	sessions := newTestSQLStore(t)
	journal := &countingReleaseJournal{SQLSessionStore: sessions}
	_, product, _, _ := testScopes()
	newProfiles := func() *core.AgentProfileRegistry {
		profiles := core.NewAgentProfileRegistry()
		name := "Stable"
		model := core.ModelSelection{Provider: "test", Model: "stable"}
		if err := profiles.Bind(core.AgentProfileLayer{
			Scope: product, ProfileID: "storage.agent", Name: &name, Model: &model,
		}); err != nil {
			t.Fatal(err)
		}
		return profiles
	}
	readerProfiles := newProfiles()
	reader, _ := control.NewReleaseManager(readerProfiles)
	reader.Journal = journal
	if err := reader.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	journal.listCalls = 0
	if err := reader.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if journal.listCalls != 1 {
		t.Fatalf("first release sync scans=%d, want 1", journal.listCalls)
	}
	if err := reader.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if journal.listCalls != 1 {
		t.Fatalf("unchanged control revision rescanned release history: %d", journal.listCalls)
	}

	writer, _ := control.NewReleaseManager(newProfiles())
	writer.Journal = sessions
	if err := writer.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	name := "Peer Release"
	if _, err := writer.Publish(ctx, product, core.AgentProfileLayer{
		ProfileID: "storage.agent", Name: &name,
	}); err != nil {
		t.Fatal(err)
	}
	if err := reader.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if journal.listCalls != 2 {
		t.Fatalf("changed control revision did not rescan release history: %d", journal.listCalls)
	}
	principal := core.Principal{TenantID: "acme", SubjectID: "alice", Scope: product, Grants: core.NewPermissionSet(core.PermRead)}
	resolved, err := readerProfiles.Resolve(principal, product, "storage.agent")
	if err != nil || resolved.Name != name {
		t.Fatalf("revision-triggered sync did not apply release: %#v err=%v", resolved, err)
	}
}

func TestSQLCanaryStoreRejectsInvalidProfileFilter(t *testing.T) {
	sessions := newTestSQLStore(t)
	store, err := NewSQLCanaryStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := store.ListCanaries(ctx, "bad id", 10); err == nil {
		t.Fatal("invalid canary profile filter was accepted")
	}
}

func TestSQLCanaryStoreRejectsOpenOverflow(t *testing.T) {
	ctx := context.Background()
	sessions := newTestSQLStore(t)
	store, err := NewSQLCanaryStore(sessions.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	store.maxOpen = 2
	_, product, tenant, user := testScopes()
	if err := store.CreateCanary(ctx, sqlCanaryTestRecord(t, "canary-open-1", product)); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateCanary(ctx, sqlCanaryTestRecord(t, "canary-open-2", tenant)); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateCanary(ctx, sqlCanaryTestRecord(t, "canary-open-3", user)); err == nil {
		t.Fatal("open canary overflow was accepted")
	}
	loaded, err := store.GetCanary(ctx, "canary-open-1")
	if err != nil {
		t.Fatal(err)
	}
	loaded.Status = control.CanaryRolledBack
	loaded.UpdatedAt = loaded.UpdatedAt.Add(time.Second)
	if _, err := store.UpdateCanary(ctx, loaded, control.CanaryActive); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateCanary(ctx, sqlCanaryTestRecord(t, "canary-open-3", user)); err != nil {
		t.Fatalf("promoted canary did not free an open slot: %v", err)
	}
}
