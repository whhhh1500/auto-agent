package control

import (
	"context"
	"errors"
	"testing"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

type failingJournal struct {
	recordErr   error
	rollbackErr error
	revision    int64
}

func (j *failingJournal) RecordRelease(context.Context, ReleaseInfo) error {
	if j.recordErr == nil {
		j.revision++
	}
	return j.recordErr
}
func (j *failingJournal) MarkRolledBack(context.Context, string, []int) error {
	if j.rollbackErr == nil {
		j.revision++
	}
	return j.rollbackErr
}
func (j *failingJournal) LoadReleases(context.Context, string) ([]ReleaseInfo, error) {
	return nil, errors.New("unavailable")
}
func (j *failingJournal) ListReleaseProfiles(context.Context) ([]string, error) { return nil, nil }
func (j *failingJournal) FindReleaseByOperation(context.Context, string) (ReleaseInfo, bool, error) {
	return ReleaseInfo{}, false, nil
}
func (j *failingJournal) ControlRevision(context.Context) (int64, error) { return j.revision, nil }

func releaseScope(t *testing.T) core.ScopePath {
	t.Helper()
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product, err := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "product"})
	if err != nil {
		t.Fatal(err)
	}
	return product
}

func TestPublishJournalFailureRollsBackMountedProfile(t *testing.T) {
	profiles := core.NewAgentProfileRegistry()
	manager, err := NewReleaseManager(profiles)
	if err != nil {
		t.Fatal(err)
	}
	manager.Journal = &failingJournal{recordErr: errors.New("write failed")}
	if err := manager.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	name := "Agent"
	if _, err := manager.Publish(context.Background(), releaseScope(t), core.AgentProfileLayer{ProfileID: "product.agent", Name: &name}); err == nil {
		t.Fatal("journal failure must fail publish")
	}
	if profilesList, _ := profiles.ListProfiles(releaseScope(t)); len(profilesList) != 0 {
		t.Fatalf("failed publish left a mounted profile: %#v", profilesList)
	}
}

func TestRollbackJournalFailureLeavesProfileMounted(t *testing.T) {
	profiles := core.NewAgentProfileRegistry()
	manager, _ := NewReleaseManager(profiles)
	journal := &failingJournal{}
	manager.Journal = journal
	if err := manager.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	name := "Agent"
	if _, err := manager.Publish(context.Background(), releaseScope(t), core.AgentProfileLayer{ProfileID: "product.agent", Name: &name}); err != nil {
		t.Fatal(err)
	}
	journal.rollbackErr = errors.New("write failed")
	if _, err := manager.Rollback(context.Background(), "product.agent", 0); err == nil {
		t.Fatal("journal failure must fail rollback")
	}
	if profilesList, _ := profiles.ListProfiles(releaseScope(t)); len(profilesList) != 1 {
		t.Fatalf("failed rollback changed mounted state: %#v", profilesList)
	}
}

func TestPrepareCandidateDoesNotPolluteLiveRegistry(t *testing.T) {
	profiles := core.NewAgentProfileRegistry()
	manager, _ := NewReleaseManager(profiles)
	scope := releaseScope(t)
	baseName := "Stable"
	model := core.ModelSelection{Provider: "mock", Model: "stable"}
	if err := profiles.Bind(core.AgentProfileLayer{
		Scope: scope, ProfileID: "product.agent", Name: &baseName, Model: &model,
	}); err != nil {
		t.Fatal(err)
	}
	candidateName := "Candidate"
	candidateProfiles, revision, err := manager.PrepareCandidate(scope, core.AgentProfileLayer{
		ProfileID: "product.agent", Name: &candidateName,
	})
	if err != nil || revision == "" {
		t.Fatalf("prepare candidate failed: revision=%q err=%v", revision, err)
	}
	principal := core.Principal{SubjectID: "u", TenantID: "t", Scope: scope, Grants: core.NewPermissionSet(core.PermRead)}
	candidate, err := candidateProfiles.Resolve(principal, scope, "product.agent")
	if err != nil || candidate.Name != candidateName {
		t.Fatalf("candidate snapshot wrong: %#v err=%v", candidate, err)
	}
	live, err := profiles.Resolve(principal, scope, "product.agent")
	if err != nil || live.Name != baseName {
		t.Fatalf("candidate polluted live registry: %#v err=%v", live, err)
	}
	if len(manager.History(context.Background(), "product.agent")) != 0 {
		t.Fatal("candidate preparation changed release history")
	}
}

func TestPublishWithOperationIsIdempotent(t *testing.T) {
	profiles := core.NewAgentProfileRegistry()
	manager, _ := NewReleaseManager(profiles)
	scope := releaseScope(t)
	name := "Candidate"
	layer := core.AgentProfileLayer{ProfileID: "product.agent", Name: &name}
	first, err := manager.PublishWithOperation(context.Background(), scope, layer, "canary-operation-1")
	if err != nil {
		t.Fatal(err)
	}
	second, err := manager.PublishWithOperation(context.Background(), scope, layer, "canary-operation-1")
	if err != nil {
		t.Fatal(err)
	}
	if first.Version != 1 || second.Version != first.Version || second.OperationID != first.OperationID {
		t.Fatalf("idempotent publish diverged: first=%#v second=%#v", first, second)
	}
	if history := manager.History(context.Background(), layer.ProfileID); len(history) != 1 {
		t.Fatalf("idempotent publish created %d releases", len(history))
	}
	otherName := "Different"
	if _, err := manager.PublishWithOperation(context.Background(), scope, core.AgentProfileLayer{
		ProfileID: layer.ProfileID, Name: &otherName,
	}, "canary-operation-1"); err == nil {
		t.Fatal("operation id was reused for a different artifact")
	}
}

func TestPublishAtBaselineRejectsOverlappingDriftButIgnoresSiblingScope(t *testing.T) {
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	left, _ := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "left"})
	right, _ := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "right"})
	profiles := core.NewAgentProfileRegistry()
	manager, _ := NewReleaseManager(profiles)
	name := "Agent"
	model := core.ModelSelection{Provider: "test", Model: "stable"}
	if err := profiles.Bind(core.AgentProfileLayer{Scope: left, ProfileID: "product.agent", Name: &name, Model: &model}); err != nil {
		t.Fatal(err)
	}
	_, _, baseline, err := manager.PrepareCandidateAtCurrentBaseline(left, core.AgentProfileLayer{
		ProfileID: "product.agent", Name: &name,
	})
	if err != nil {
		t.Fatal(err)
	}
	rightName := "Right"
	if _, err := manager.Publish(context.Background(), right, core.AgentProfileLayer{
		ProfileID: "product.agent", Name: &rightName,
	}); err != nil {
		t.Fatalf("sibling release should remain independent: %v", err)
	}
	if _, err := manager.PublishAtBaseline(context.Background(), left, core.AgentProfileLayer{
		ProfileID: "product.agent", Name: &name,
	}, baseline); err != nil {
		t.Fatalf("sibling release changed the baseline: %v", err)
	}

	_, _, staleBaseline, err := manager.PrepareCandidateAtCurrentBaseline(left, core.AgentProfileLayer{
		ProfileID: "product.agent", Name: &name,
	})
	if err != nil {
		t.Fatal(err)
	}
	leftName := "Drift"
	if _, err := manager.Publish(context.Background(), left, core.AgentProfileLayer{
		ProfileID: "product.agent", Name: &leftName,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.PublishAtBaseline(context.Background(), left, core.AgentProfileLayer{
		ProfileID: "product.agent", Name: &name,
	}, staleBaseline); !errors.Is(err, ErrReleaseBaselineDrift) {
		t.Fatalf("overlapping drift was not rejected: %v", err)
	}
}

func TestPublishAtBaselineRequiresDigest(t *testing.T) {
	profiles := core.NewAgentProfileRegistry()
	manager, _ := NewReleaseManager(profiles)
	name := "Agent"
	if _, err := manager.PublishAtBaseline(context.Background(), releaseScope(t), core.AgentProfileLayer{
		ProfileID: "product.agent", Name: &name,
	}, ""); err == nil {
		t.Fatal("empty baseline silently degraded to an ungated publish")
	}
	if _, err := manager.PublishWithOperationAtBaseline(context.Background(), releaseScope(t), core.AgentProfileLayer{
		ProfileID: "product.agent", Name: &name,
	}, "operation-baseline-digest", "not-a-digest"); err == nil {
		t.Fatal("invalid operation baseline was accepted")
	}
}
