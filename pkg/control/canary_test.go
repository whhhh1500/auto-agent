package control

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"

	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/evaluation"
)

type memoryCanaryStore struct {
	records       map[string]CanaryRecord
	revision      int64
	listOpenCalls int
}

func newMemoryCanaryStore() *memoryCanaryStore {
	return &memoryCanaryStore{records: map[string]CanaryRecord{}}
}

func (s *memoryCanaryStore) CreateCanary(_ context.Context, record CanaryRecord) error {
	if _, exists := s.records[record.ID]; exists {
		return fmt.Errorf("canary %s already exists", record.ID)
	}
	for _, existing := range s.records {
		if existing.ProfileID == record.ProfileID && existing.Scope.Equal(record.Scope) &&
			(existing.Status == CanaryActive || existing.Status == CanaryPaused) {
			return fmt.Errorf("active canary already exists")
		}
	}
	s.records[record.ID] = cloneCanaryRecord(record)
	s.revision++
	return nil
}

func (s *memoryCanaryStore) UpdateCanary(_ context.Context, record CanaryRecord, expected ...CanaryStatus) (bool, error) {
	current, exists := s.records[record.ID]
	if !exists {
		return false, nil
	}
	allowed := false
	for _, status := range expected {
		if current.Status == status {
			allowed = true
			break
		}
	}
	if !allowed {
		return false, nil
	}
	s.records[record.ID] = cloneCanaryRecord(record)
	s.revision++
	return true, nil
}

func (s *memoryCanaryStore) ControlRevision(context.Context) (int64, error) {
	return s.revision, nil
}

func (s *memoryCanaryStore) GetCanary(_ context.Context, id string) (CanaryRecord, error) {
	record, exists := s.records[id]
	if !exists {
		return CanaryRecord{}, fmt.Errorf("canary not found")
	}
	return cloneCanaryRecord(record), nil
}

func (s *memoryCanaryStore) ListCanaries(_ context.Context, profileID string, limit int) ([]CanaryRecord, error) {
	out := []CanaryRecord{}
	for _, record := range s.records {
		if profileID == "" || record.ProfileID == profileID {
			out = append(out, cloneCanaryRecord(record))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *memoryCanaryStore) ListOpenCanaries(context.Context) ([]CanaryRecord, error) {
	s.listOpenCalls++
	out := []CanaryRecord{}
	for _, record := range s.records {
		if record.Status == CanaryActive || record.Status == CanaryPaused || record.Status == CanaryPromoting {
			out = append(out, cloneCanaryRecord(record))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}

func TestCanaryRefreshUsesRevisionFastPath(t *testing.T) {
	ctx := context.Background()
	store := newMemoryCanaryStore()
	manager, scope, _ := newCanaryTestManager(t, store)
	record := canaryTestRecord(t, "canary-refresh-revision", scope, "Candidate", 10000)
	if _, err := manager.Stage(ctx, record); err != nil {
		t.Fatal(err)
	}
	store.listOpenCalls = 0
	if err := manager.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if store.listOpenCalls != 1 {
		t.Fatalf("first refresh scans=%d, want 1", store.listOpenCalls)
	}
	if err := manager.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if store.listOpenCalls != 1 {
		t.Fatalf("unchanged revision triggered another full scan: %d", store.listOpenCalls)
	}
	if _, err := manager.Pause(ctx, record.ID); err != nil {
		t.Fatal(err)
	}
	if err := manager.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if store.listOpenCalls != 2 {
		t.Fatalf("changed revision did not trigger a new scan: %d", store.listOpenCalls)
	}
}

func newCanaryTestManager(t *testing.T, store CanaryStore) (*CanaryManager, core.ScopePath, core.Principal) {
	t.Helper()
	scope := releaseScope(t)
	profiles := core.NewAgentProfileRegistry()
	stable := "Stable"
	model := core.ModelSelection{Provider: "test", Model: "stable"}
	if err := profiles.Bind(core.AgentProfileLayer{
		Scope: scope, ProfileID: "product.agent", Name: &stable, Model: &model,
	}); err != nil {
		t.Fatal(err)
	}
	releases, err := NewReleaseManager(profiles)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewCanaryManager(releases, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	principal := core.Principal{
		TenantID: "acme", SubjectID: "alice", Scope: scope,
		Grants: core.NewPermissionSet(core.PermRead),
	}
	return manager, scope, principal
}

func canaryTestRecord(t *testing.T, id string, scope core.ScopePath, name string, basisPoints int) CanaryRecord {
	t.Helper()
	layer := core.AgentProfileLayer{Scope: scope, ProfileID: "product.agent", Name: &name}
	revision, err := core.ProfileLayerRevision(layer)
	if err != nil {
		t.Fatal(err)
	}
	return CanaryRecord{
		ID: id, ProfileID: layer.ProfileID, Scope: scope, Layer: &layer, Revision: revision,
		BasisPoints: basisPoints, CandidateEvaluationRunID: "evaluation-candidate-1",
		Gate: evaluation.GateResult{Passed: true},
	}
}

const (
	canaryCoverageMarkerA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	canaryCoverageMarkerB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

func satisfiedCanaryCoverageGate(marker string) evaluation.GateResult {
	return evaluation.GateResult{
		Passed:                   true,
		RequiredCoverageRevision: marker,
		Coverage: &evaluation.CoverageGateResult{
			Status: evaluation.CoverageSatisfied, RequiredCases: 1, SatisfiedCases: 1,
		},
	}
}

func resolvedCanaryName(t *testing.T, profiles *core.AgentProfileRegistry, principal core.Principal) string {
	t.Helper()
	profile, err := profiles.Resolve(principal, principal.Scope, "product.agent")
	if err != nil {
		t.Fatal(err)
	}
	return profile.Name
}

func TestCanaryStagePauseResumeRollback(t *testing.T) {
	ctx := context.Background()
	store := newMemoryCanaryStore()
	manager, scope, principal := newCanaryTestManager(t, store)
	record := canaryTestRecord(t, "canary-lifecycle", scope, "Candidate", 10000)

	staged, err := manager.Stage(ctx, record)
	if err != nil || staged.Status != CanaryActive {
		t.Fatalf("stage failed: %#v err=%v", staged, err)
	}
	profiles, selected := manager.Select(principal, record.ProfileID)
	if selected == nil || selected.ID != record.ID || resolvedCanaryName(t, profiles, principal) != "Candidate" {
		t.Fatalf("active canary was not selected: %#v", selected)
	}

	if _, err := manager.Pause(ctx, record.ID); err != nil {
		t.Fatal(err)
	}
	profiles, selected = manager.Select(principal, record.ProfileID)
	if selected != nil || resolvedCanaryName(t, profiles, principal) != "Stable" {
		t.Fatal("paused canary must route entirely to live")
	}

	if _, err := manager.Resume(ctx, record.ID); err != nil {
		t.Fatal(err)
	}
	profiles, selected = manager.Select(principal, record.ProfileID)
	if selected == nil || resolvedCanaryName(t, profiles, principal) != "Candidate" {
		t.Fatal("resumed canary did not restore candidate routing")
	}

	rolledBack, err := manager.Rollback(ctx, record.ID)
	if err != nil || rolledBack.Status != CanaryRolledBack {
		t.Fatalf("rollback failed: %#v err=%v", rolledBack, err)
	}
	profiles, selected = manager.Select(principal, record.ProfileID)
	if selected != nil || resolvedCanaryName(t, profiles, principal) != "Stable" {
		t.Fatal("rolled-back canary must route entirely to live")
	}
}

func TestCanaryRejectsFailedGateConflictAndInvalidPercentage(t *testing.T) {
	ctx := context.Background()
	manager, scope, _ := newCanaryTestManager(t, newMemoryCanaryStore())
	failed := canaryTestRecord(t, "canary-failed-gate", scope, "Rejected", 1000)
	failed.Gate.Passed = false
	if _, err := manager.Stage(ctx, failed); err == nil {
		t.Fatal("failed evaluation gate staged a canary")
	}

	first := canaryTestRecord(t, "canary-conflict-1", scope, "First", 1000)
	if _, err := manager.Stage(ctx, first); err != nil {
		t.Fatal(err)
	}
	second := canaryTestRecord(t, "canary-conflict-2", scope, "Second", 1000)
	if _, err := manager.Stage(ctx, second); err == nil {
		t.Fatal("same profile and scope accepted two active canaries")
	}
	for _, invalid := range []int{0, 10001} {
		if _, err := manager.SetBasisPoints(ctx, first.ID, invalid); err == nil {
			t.Fatalf("accepted invalid basis points %d", invalid)
		}
	}
	for _, valid := range []int{1, 10000} {
		updated, err := manager.SetBasisPoints(ctx, first.ID, valid)
		if err != nil || updated.BasisPoints != valid {
			t.Fatalf("set basis points %d failed: %#v err=%v", valid, updated, err)
		}
	}
}

func TestCanaryRequiredEfficiencyGateSurvivesCloneAndFailsClosedWhenMissing(t *testing.T) {
	ctx := context.Background()
	manager, scope, _ := newCanaryTestManager(t, newMemoryCanaryStore())
	policy := evaluation.DefaultEfficiencyGatePolicy()

	missing := canaryTestRecord(t, "canary-efficiency-missing", scope, "Missing", 1000)
	missing.Gate.RequiredEfficiencyContract = policy.ContractID
	if _, err := manager.Stage(ctx, missing); err == nil {
		t.Fatal("required efficiency gate without a verdict staged a canary")
	}

	record := canaryTestRecord(t, "canary-efficiency-clone", scope, "Candidate", 1000)
	record.Gate.RequiredEfficiencyContract = policy.ContractID
	record.Gate.Efficiency = &evaluation.EfficiencyGateResult{
		ContractID: policy.ContractID, Verdict: evaluation.EfficiencyGatePassed,
		ReasonCodes: []evaluation.EfficiencyGateReasonCode{evaluation.EfficiencyReasonImprovementInsufficient},
	}
	staged, err := manager.Stage(ctx, record)
	if err != nil {
		t.Fatal(err)
	}
	clone := cloneCanaryRecord(staged)
	clone.Gate.Efficiency.ReasonCodes[0] = evaluation.EfficiencyReasonRunInvalid
	if staged.Gate.Efficiency == nil || staged.Gate.Efficiency.ReasonCodes[0] != evaluation.EfficiencyReasonImprovementInsufficient {
		t.Fatalf("canary clone lost or aliased efficiency verdict: staged=%#v clone=%#v", staged.Gate.Efficiency, clone.Gate.Efficiency)
	}
	clone.Gate.Efficiency = nil
	if err := ValidateCanaryRecord(clone); err == nil {
		t.Fatal("canary validation accepted a record whose required efficiency verdict was lost")
	}
}

func TestCanaryRequiredCoverageGateStagesClonesAndFailsClosedWhenMissing(t *testing.T) {
	ctx := context.Background()
	manager, scope, _ := newCanaryTestManager(t, newMemoryCanaryStore())

	missing := canaryTestRecord(t, "canary-coverage-missing", scope, "Missing", 1000)
	missing.Gate.Passed = true
	missing.Gate.RequiredCoverageRevision = canaryCoverageMarkerA
	if _, err := manager.Stage(ctx, missing); err == nil {
		t.Fatal("required coverage marker without a gate result staged a canary")
	}

	record := canaryTestRecord(t, "canary-coverage-clone", scope, "Candidate", 1000)
	record.Gate = satisfiedCanaryCoverageGate(canaryCoverageMarkerA)
	staged, err := manager.Stage(ctx, record)
	if err != nil {
		t.Fatal(err)
	}
	clone := cloneCanaryRecord(staged)
	if clone.Gate.Coverage == staged.Gate.Coverage {
		t.Fatal("canary clone aliased coverage result")
	}
	clone.Gate.Coverage = nil
	if err := ValidateCanaryRecord(clone); err == nil {
		t.Fatal("canary validation accepted a record whose required coverage result was lost")
	}

	withReasons := CanaryRecord{Gate: evaluation.GateResult{Coverage: &evaluation.CoverageGateResult{
		Status: evaluation.CoverageUnavailable, RequiredCases: 1, UnavailableCases: 1,
		ReasonCodes: []evaluation.CoverageReasonCode{evaluation.CoverageReasonEffectUnavailable},
	}}}
	deepClone := cloneCanaryRecord(withReasons)
	deepClone.Gate.Coverage.ReasonCodes[0] = evaluation.CoverageReasonRouteUnavailable
	if withReasons.Gate.Coverage.ReasonCodes[0] != evaluation.CoverageReasonEffectUnavailable {
		t.Fatal("canary clone aliased coverage reason codes")
	}
}

func TestCanaryCoverageArtifactDriftFailsRefresh(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name   string
		mutate func(*CanaryRecord)
	}{
		{
			name: "marker",
			mutate: func(record *CanaryRecord) {
				record.Gate.RequiredCoverageRevision = canaryCoverageMarkerB
			},
		},
		{
			name: "coverage_result",
			mutate: func(record *CanaryRecord) {
				record.Gate.Coverage = &evaluation.CoverageGateResult{
					Status: evaluation.CoverageSatisfied, RequiredCases: 2, SatisfiedCases: 2,
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newMemoryCanaryStore()
			manager, scope, _ := newCanaryTestManager(t, store)
			record := canaryTestRecord(t, "canary-coverage-drift-"+test.name, scope, "Candidate", 1000)
			record.Gate = satisfiedCanaryCoverageGate(canaryCoverageMarkerA)
			if _, err := manager.Stage(ctx, record); err != nil {
				t.Fatal(err)
			}

			durable := cloneCanaryRecord(store.records[record.ID])
			test.mutate(&durable)
			store.records[record.ID] = durable
			store.revision++
			if err := manager.Refresh(ctx); err == nil {
				t.Fatal("refresh accepted coverage artifact drift")
			}
		})
	}
}

func TestCanaryCoverageGateRestoreFailsClosedAndLegacyRemainsCompatible(t *testing.T) {
	ctx := context.Background()

	t.Run("required coverage missing after persistence", func(t *testing.T) {
		store := newMemoryCanaryStore()
		first, scope, _ := newCanaryTestManager(t, store)
		record := canaryTestRecord(t, "canary-coverage-restore-invalid", scope, "Candidate", 1000)
		record.Gate = satisfiedCanaryCoverageGate(canaryCoverageMarkerA)
		if _, err := first.Stage(ctx, record); err != nil {
			t.Fatal(err)
		}
		durable := cloneCanaryRecord(store.records[record.ID])
		durable.Gate.Coverage = nil
		store.records[record.ID] = durable
		store.revision++

		restored, err := NewCanaryManager(first.Releases, store)
		if err != nil {
			t.Fatal(err)
		}
		if err := restored.Restore(ctx); err == nil {
			t.Fatal("restore accepted a required coverage marker without a satisfied result")
		}
	})

	t.Run("legacy gate", func(t *testing.T) {
		store := newMemoryCanaryStore()
		first, scope, principal := newCanaryTestManager(t, store)
		record := canaryTestRecord(t, "canary-coverage-legacy", scope, "Legacy", 10000)
		if _, err := first.Stage(ctx, record); err != nil {
			t.Fatal(err)
		}
		restored, err := NewCanaryManager(first.Releases, store)
		if err != nil {
			t.Fatal(err)
		}
		if err := restored.Restore(ctx); err != nil {
			t.Fatalf("restore rejected legacy gate JSON: %v", err)
		}
		profiles, selected := restored.Select(principal, record.ProfileID)
		if selected == nil || selected.ID != record.ID || resolvedCanaryName(t, profiles, principal) != "Legacy" {
			t.Fatalf("legacy canary did not restore: %#v", selected)
		}
	})
}

func TestCanarySubjectBucketingIsStable(t *testing.T) {
	ctx := context.Background()
	manager, scope, principal := newCanaryTestManager(t, newMemoryCanaryStore())
	record := canaryTestRecord(t, "canary-stable-bucket", scope, "Candidate", 5000)
	if _, err := manager.Stage(ctx, record); err != nil {
		t.Fatal(err)
	}

	firstBucket := canaryBucket(record.ID, principal)
	_, firstSelection := manager.Select(principal, record.ProfileID)
	for index := 0; index < 20; index++ {
		if bucket := canaryBucket(record.ID, principal); bucket != firstBucket {
			t.Fatalf("bucket changed from %d to %d", firstBucket, bucket)
		}
		_, selected := manager.Select(principal, record.ProfileID)
		if (selected != nil) != (firstSelection != nil) {
			t.Fatal("same subject changed canary assignment")
		}
	}

	foundCandidate, foundLive := false, false
	for index := 0; index < 1000 && (!foundCandidate || !foundLive); index++ {
		candidatePrincipal := principal
		candidatePrincipal.SubjectID = fmt.Sprintf("subject-%d", index)
		_, selected := manager.Select(candidatePrincipal, record.ProfileID)
		foundCandidate = foundCandidate || selected != nil
		foundLive = foundLive || selected == nil
	}
	if !foundCandidate || !foundLive {
		t.Fatalf("50%% canary did not produce both stable partitions: candidate=%t live=%t", foundCandidate, foundLive)
	}
}

func TestCanaryRestoreRebuildsCandidateRegistry(t *testing.T) {
	ctx := context.Background()
	store := newMemoryCanaryStore()
	first, scope, principal := newCanaryTestManager(t, store)
	record := canaryTestRecord(t, "canary-restore", scope, "Restored Candidate", 10000)
	if _, err := first.Stage(ctx, record); err != nil {
		t.Fatal(err)
	}

	restored, _, _ := newCanaryTestManager(t, store)
	profiles, selected := restored.Select(principal, record.ProfileID)
	if selected == nil || selected.ID != record.ID || resolvedCanaryName(t, profiles, principal) != "Restored Candidate" {
		t.Fatalf("restore did not rebuild candidate registry: %#v", selected)
	}
}

func TestCanaryPromotionPublishesExactlyOnce(t *testing.T) {
	ctx := context.Background()
	manager, scope, principal := newCanaryTestManager(t, newMemoryCanaryStore())
	record := canaryTestRecord(t, "canary-promote-once", scope, "Promoted Candidate", 10000)
	if _, err := manager.Stage(ctx, record); err != nil {
		t.Fatal(err)
	}
	promoted, err := manager.Promote(ctx, record.ID)
	if err != nil || promoted.Status != CanaryPromoted || promoted.ReleaseVersion != 1 {
		t.Fatalf("promotion failed: %#v err=%v", promoted, err)
	}
	replayed, err := manager.Promote(ctx, record.ID)
	if err != nil || replayed.ReleaseVersion != promoted.ReleaseVersion {
		t.Fatalf("promotion replay diverged: %#v err=%v", replayed, err)
	}
	if history := manager.Releases.History(ctx, record.ProfileID); len(history) != 1 || history[0].OperationID != record.ID {
		t.Fatalf("promotion created wrong release history: %#v", history)
	}
	afterName := "After Promotion"
	if release, err := manager.Releases.Publish(ctx, scope, core.AgentProfileLayer{
		ProfileID: record.ProfileID, Name: &afterName,
	}); err != nil || release.Version != 2 {
		t.Fatalf("promotion did not release the profile reservation: %#v err=%v", release, err)
	}
	profiles, selected := manager.Select(principal, record.ProfileID)
	if selected != nil || resolvedCanaryName(t, profiles, principal) != afterName {
		t.Fatal("promoted artifact was not installed as the live profile")
	}
}

func TestOpenCanaryReservesReleaseAndPromotionRejectsDrift(t *testing.T) {
	ctx := context.Background()
	manager, scope, _ := newCanaryTestManager(t, newMemoryCanaryStore())
	record := canaryTestRecord(t, "canary-baseline-drift", scope, "Candidate", 10000)
	staged, err := manager.Stage(ctx, record)
	if err != nil {
		t.Fatal(err)
	}
	otherName := "Unexpected Live Change"
	if _, err := manager.Releases.Publish(ctx, scope, core.AgentProfileLayer{
		ProfileID: record.ProfileID, Name: &otherName,
	}); !errors.Is(err, ErrReleaseReserved) {
		t.Fatalf("open canary did not reserve overlapping releases: %v", err)
	}
	if _, err := manager.Releases.PublishWithOperation(ctx, scope, core.AgentProfileLayer{
		ProfileID: record.ProfileID, Name: &otherName,
	}, staged.ID); !errors.Is(err, ErrReleaseReserved) {
		t.Fatalf("public operation publish bypassed the canary reservation: %v", err)
	}
	if _, err := manager.Releases.Rollback(ctx, record.ProfileID, 0); !errors.Is(err, ErrReleaseReserved) {
		t.Fatalf("open canary did not reserve release rollback: %v", err)
	}

	// Simulate an out-of-band manager lifetime that missed the in-process
	// reservation but changed the durable Release baseline.
	manager.Releases.releaseReservation(staged.ProfileID, staged.ID)
	if _, err := manager.Releases.Publish(ctx, scope, core.AgentProfileLayer{
		ProfileID: record.ProfileID, Name: &otherName,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Promote(ctx, staged.ID); !errors.Is(err, ErrReleaseBaselineDrift) {
		t.Fatalf("promotion accepted stale evaluation baseline: %v", err)
	}
	current, err := manager.Get(ctx, staged.ID)
	if err != nil || current.Status != CanaryActive {
		t.Fatalf("baseline drift moved canary into an intermediate state: %#v err=%v", current, err)
	}
}

func TestCanarySelectsMostSpecificVisibleScope(t *testing.T) {
	ctx := context.Background()
	store := newMemoryCanaryStore()
	manager, product, _ := newCanaryTestManager(t, store)
	tenant, err := product.Child(core.ScopeRef{Kind: core.ScopeTenant, ID: "acme"})
	if err != nil {
		t.Fatal(err)
	}
	principal := core.Principal{TenantID: "acme", SubjectID: "alice", Scope: tenant, Grants: core.NewPermissionSet(core.PermRead)}
	if _, err := manager.Stage(ctx, canaryTestRecord(t, "canary-product", product, "Product Candidate", 10000)); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Stage(ctx, canaryTestRecord(t, "canary-tenant", tenant, "Tenant Candidate", 10000)); err != nil {
		t.Fatal(err)
	}
	profiles, selected := manager.Select(principal, "product.agent")
	if selected == nil || selected.ID != "canary-tenant" || resolvedCanaryName(t, profiles, principal) != "Tenant Candidate" {
		t.Fatalf("most specific canary was not selected: %#v", selected)
	}
}

func TestPausedSpecificCanaryShadowsActiveAncestor(t *testing.T) {
	ctx := context.Background()
	store := newMemoryCanaryStore()
	manager, product, _ := newCanaryTestManager(t, store)
	tenant, err := product.Child(core.ScopeRef{Kind: core.ScopeTenant, ID: "acme"})
	if err != nil {
		t.Fatal(err)
	}
	principal := core.Principal{
		TenantID: "acme", SubjectID: "alice", Scope: tenant,
		Grants: core.NewPermissionSet(core.PermRead),
	}
	ancestor := canaryTestRecord(t, "canary-active-ancestor", product, "Ancestor Candidate", 10000)
	if _, err := manager.Stage(ctx, ancestor); err != nil {
		t.Fatal(err)
	}
	specific := canaryTestRecord(t, "canary-paused-specific", tenant, "Specific Candidate", 10000)
	if _, err := manager.Stage(ctx, specific); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Pause(ctx, specific.ID); err != nil {
		t.Fatal(err)
	}
	profiles, assignment := manager.Assign(principal, "product.agent")
	if assignment == nil || assignment.ID != specific.ID || assignment.Candidate || assignment.Status != CanaryPaused {
		t.Fatalf("specific paused canary did not shadow active ancestor: %#v", assignment)
	}
	if name := resolvedCanaryName(t, profiles, principal); name != "Stable" {
		t.Fatalf("paused specific canary routed to %q, want Stable", name)
	}
	if _, selected := manager.Select(principal, "product.agent"); selected != nil {
		t.Fatalf("legacy Select exposed ancestor candidate through paused specific canary: %#v", selected)
	}
}

var _ CanaryStore = (*memoryCanaryStore)(nil)
