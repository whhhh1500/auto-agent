package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fenceAuthorizerFunc func(context.Context, FenceCommand) error

func (fn fenceAuthorizerFunc) AuthorizeFence(ctx context.Context, command FenceCommand) error {
	return fn(ctx, command)
}

type testFenceJournal struct {
	journal          *MemoryFenceJournal
	beginErr         error
	completeErr      error
	completeAfterErr error
	cancelAfterBegin func()
	beginCalls       atomic.Int32
	unknowns         atomic.Int32
}

type panicFenceOwner struct{ ModuleLeaseOwner }

func (panicFenceOwner) Fence(context.Context, FenceRequest) error { panic("owner fence panic") }

func newTestFenceJournal() *testFenceJournal {
	return &testFenceJournal{journal: NewMemoryFenceJournal()}
}

func (journal *testFenceJournal) Begin(ctx context.Context, command FenceCommand) (FenceRecord, error) {
	journal.beginCalls.Add(1)
	if journal.beginErr != nil {
		return FenceRecord{}, journal.beginErr
	}
	record, err := journal.journal.Begin(ctx, command)
	if err == nil && journal.cancelAfterBegin != nil {
		journal.cancelAfterBegin()
	}
	return record, err
}

func (journal *testFenceJournal) Complete(ctx context.Context, command FenceCommand, result FenceResult) error {
	if journal.completeErr != nil {
		return journal.completeErr
	}
	if err := journal.journal.Complete(ctx, command, result); err != nil {
		return err
	}
	return journal.completeAfterErr
}

func (journal *testFenceJournal) MarkUnknown(ctx context.Context, command FenceCommand) error {
	journal.unknowns.Add(1)
	return journal.journal.MarkUnknown(ctx, command)
}

func newAuthorizedFenceHost(t *testing.T, authorizer FenceAuthorizer, journal FenceJournal) (*ModuleHost, *hostModule, string) {
	t.Helper()
	module := moduleForHost("fence-runtime", nil)
	host, err := NewModuleHostWithControls(Version{Major: 1}, []Module{module}, NewMemoryEffectJournal(), &inverseRecorder{}, HostControls{
		FenceAuthorizer: authorizer, FenceJournal: journal,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Activate(context.Background()); err != nil {
		t.Fatal(err)
	}
	revision, found := host.ActiveCompositionRevision()
	if !found {
		t.Fatal("missing active composition")
	}
	return host, module, revision
}

func allowFence(context.Context, FenceCommand) error { return nil }

func fenceCommand(revision, requestID string) FenceCommand {
	return FenceCommand{RequestID: requestID, CompositionRevision: revision, ActorID: "platform-admin", Reason: "security incident"}
}

func TestFenceAuthorizedDeniesBeforeJournalOrOwner(t *testing.T) {
	module := moduleForHost("fence-runtime", nil)
	host, err := NewModuleHost(Version{Major: 1}, []Module{module}, NewMemoryEffectJournal(), &inverseRecorder{})
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Activate(context.Background()); err != nil {
		t.Fatal(err)
	}
	revision, found := host.ActiveCompositionRevision()
	if !found {
		t.Fatal("missing active composition")
	}
	if _, err := host.FenceAuthorized(context.Background(), fenceCommand(revision, "fence-nil-authorizer")); !errors.Is(err, ErrFenceUnauthorized) {
		t.Fatalf("nil authorizer error=%v", err)
	}
	if module.lastOwner == nil {
		t.Fatal("nil authorizer did not construct owner")
	}
	module.lastOwner.mu.Lock()
	fenceCalls := module.lastOwner.fenceCalls
	module.lastOwner.mu.Unlock()
	if fenceCalls != 0 {
		t.Fatal("nil authorizer reached owner")
	}

	for _, authorizer := range []FenceAuthorizer{
		fenceAuthorizerFunc(func(context.Context, FenceCommand) error { return errors.New("deny") }),
		fenceAuthorizerFunc(func(context.Context, FenceCommand) error { panic("deny") }),
	} {
		journal := newTestFenceJournal()
		host, _, revision = newAuthorizedFenceHost(t, authorizer, journal)
		if _, err := host.FenceAuthorized(context.Background(), fenceCommand(revision, "fence-authorizer-failure")); !errors.Is(err, ErrFenceUnauthorized) {
			t.Fatalf("authorizer failure error=%v", err)
		}
		if journal.beginCalls.Load() != 0 {
			t.Fatal("failed authorizer reached journal")
		}
	}

	journal := newTestFenceJournal()
	journal.beginErr = errors.New("journal unavailable")
	host, module, revision = newAuthorizedFenceHost(t, fenceAuthorizerFunc(allowFence), journal)
	if _, err := host.FenceAuthorized(context.Background(), fenceCommand(revision, "fence-begin-unavailable")); err == nil {
		t.Fatal("unavailable begin unexpectedly fenced")
	}
	if journal.beginCalls.Load() != 1 || module.lastOwner == nil {
		t.Fatal("begin evidence was not checked")
	}
}

func TestFenceAuthorizedExecuteReplayAndConcurrentRequest(t *testing.T) {
	journal := newTestFenceJournal()
	host, module, revision := newAuthorizedFenceHost(t, fenceAuthorizerFunc(allowFence), journal)
	command := fenceCommand(revision, "fence-replay")
	type outcome struct {
		result DrainResult
		err    error
	}
	resultCh := make(chan outcome, 1)
	go func() {
		result, err := host.FenceAuthorized(context.Background(), command)
		resultCh <- outcome{result: result, err: err}
	}()
	select {
	case outcome := <-resultCh:
		if outcome.err != nil || !outcome.result.Completed {
			t.Fatalf("fence result=%+v err=%v", outcome.result, outcome.err)
		}
	case <-time.After(time.Second):
		t.Fatal("authorized fence did not complete; possible lease mutex deadlock")
	}
	if module.lastOwner == nil {
		t.Fatal("owner was not constructed")
	}
	module.lastOwner.mu.Lock()
	fenceCalls := module.lastOwner.fenceCalls
	module.lastOwner.mu.Unlock()
	if fenceCalls != 1 {
		t.Fatalf("owner fence calls=%d, want 1", fenceCalls)
	}
	if replay, err := host.FenceAuthorized(context.Background(), command); err != nil || !replay.Completed {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	if journal.beginCalls.Load() != 2 {
		t.Fatalf("begin calls=%d, want execute plus replay", journal.beginCalls.Load())
	}

	// A separate host makes two simultaneous requests with the same ID. Host
	// serialization yields one execute and one replay, not two owner fences.
	journal = newTestFenceJournal()
	host, module, revision = newAuthorizedFenceHost(t, fenceAuthorizerFunc(allowFence), journal)
	command = fenceCommand(revision, "fence-concurrent")
	var group sync.WaitGroup
	results := make(chan outcome, 2)
	for range 2 {
		group.Add(1)
		go func() {
			defer group.Done()
			result, err := host.FenceAuthorized(context.Background(), command)
			results <- outcome{result: result, err: err}
		}()
	}
	group.Wait()
	close(results)
	for outcome := range results {
		if outcome.err != nil || !outcome.result.Completed {
			t.Fatalf("concurrent fence result=%+v err=%v", outcome.result, outcome.err)
		}
	}
	if journal.beginCalls.Load() != 2 {
		t.Fatalf("concurrent begin calls=%d, want 2", journal.beginCalls.Load())
	}
	module.lastOwner.mu.Lock()
	fenceCalls = module.lastOwner.fenceCalls
	module.lastOwner.mu.Unlock()
	if fenceCalls != 1 {
		t.Fatalf("concurrent owner fence calls=%d, want 1", fenceCalls)
	}
}

func TestFenceAuthorizedConflictUnknownAndFailuresFailClosed(t *testing.T) {
	journal := newTestFenceJournal()
	host, module, revision := newAuthorizedFenceHost(t, fenceAuthorizerFunc(allowFence), journal)
	owner := module.lastOwner
	lease, err := host.AcquireLease(context.Background(), LeaseRequest{RunID: "fence-run", CompositionRevision: revision, ModuleID: module.manifest.ID, ModuleRevision: module.manifest.Version, Scope: "tenant/a"})
	if err != nil {
		t.Fatal(err)
	}
	owner.fenceErr = errors.New("owner fence failed")
	command := fenceCommand(revision, "fence-owner-failure")
	if result, err := host.FenceAuthorized(context.Background(), command); err == nil || result.Completed {
		t.Fatalf("owner failure result=%+v err=%v", result, err)
	}
	if state, _ := host.State(module.manifest.ID); state != StateDraining {
		t.Fatalf("owner failure state=%s, want draining", state)
	}
	if _, err := host.AcquireLease(context.Background(), LeaseRequest{RunID: "fence-new", CompositionRevision: revision, ModuleID: module.manifest.ID, ModuleRevision: module.manifest.Version, Scope: "tenant/a"}); !errors.Is(err, ErrLeaseMismatch) {
		t.Fatalf("fence failure admitted a new lease: %v", err)
	}
	if err := host.ReleaseLease(context.Background(), lease); err != nil {
		t.Fatalf("fence failure lost existing lease: %v", err)
	}
	if journal.unknowns.Load() != 1 {
		t.Fatalf("owner failure unknown writes=%d", journal.unknowns.Load())
	}
	if _, err := host.FenceAuthorized(context.Background(), command); !errors.Is(err, ErrFenceUnknown) {
		t.Fatalf("unknown replay error=%v", err)
	}
	if _, err := host.FenceAuthorized(context.Background(), FenceCommand{RequestID: command.RequestID, CompositionRevision: revision, ActorID: "platform-admin", Reason: "different"}); !errors.Is(err, ErrFenceConflict) {
		t.Fatalf("conflicting request error=%v", err)
	}

	journal = newTestFenceJournal()
	journal.completeErr = errors.New("complete failed")
	host, _, revision = newAuthorizedFenceHost(t, fenceAuthorizerFunc(allowFence), journal)
	command = fenceCommand(revision, "fence-complete-failure")
	if _, err := host.FenceAuthorized(context.Background(), command); err == nil {
		t.Fatal("complete failure unexpectedly returned success")
	}
	if journal.unknowns.Load() != 1 {
		t.Fatalf("complete failure unknown writes=%d", journal.unknowns.Load())
	}
	if _, err := host.FenceAuthorized(context.Background(), command); !errors.Is(err, ErrFenceUnknown) {
		t.Fatalf("complete failure replay error=%v", err)
	}

	journal = newTestFenceJournal()
	host, module, revision = newAuthorizedFenceHost(t, fenceAuthorizerFunc(allowFence), journal)
	module.lastOwner.deactivateErr = errors.New("owner deactivate failed")
	command = fenceCommand(revision, "fence-deactivate-failure")
	if _, err := host.FenceAuthorized(context.Background(), command); err == nil {
		t.Fatal("deactivate failure unexpectedly returned success")
	}
	if state, _ := host.State(module.manifest.ID); state != StateDraining {
		t.Fatalf("deactivate failure state=%s, want draining", state)
	}
	if journal.unknowns.Load() != 1 {
		t.Fatalf("deactivate failure unknown writes=%d", journal.unknowns.Load())
	}
}

func TestFenceCompleteTransportErrorCannotDowngradeCommittedReplay(t *testing.T) {
	journal := newTestFenceJournal()
	journal.completeAfterErr = errors.New("transport lost after committed complete")
	host, _, revision := newAuthorizedFenceHost(t, fenceAuthorizerFunc(allowFence), journal)
	command := fenceCommand(revision, "fence-complete-transport")
	if _, err := host.FenceAuthorized(context.Background(), command); err == nil {
		t.Fatal("transport error unexpectedly reported completion")
	}
	if journal.unknowns.Load() != 1 {
		t.Fatalf("unknown audit attempts=%d", journal.unknowns.Load())
	}
	replay, err := journal.journal.Begin(context.Background(), command)
	if err != nil || replay.Decision != FenceDecisionReplay || !replay.Result.Completed {
		t.Fatalf("committed replay=%#v err=%v", replay, err)
	}
}

func TestFenceCommandAndMemoryJournalValidation(t *testing.T) {
	for _, command := range []FenceCommand{
		{RequestID: "", CompositionRevision: "comp", ActorID: "actor", Reason: "reason"},
		{RequestID: "request", CompositionRevision: "comp", ActorID: "actor", Reason: "\n"},
		{RequestID: "request", CompositionRevision: "comp", ActorID: "actor", Reason: string(make([]byte, MaxFenceReasonBytes+1))},
	} {
		if err := command.Validate(); !errors.Is(err, ErrInvalidFenceCommand) {
			t.Fatalf("command=%#v error=%v", command, err)
		}
	}
	journal := NewMemoryFenceJournal()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := journal.Begin(ctx, FenceCommand{RequestID: "request", CompositionRevision: "comp", ActorID: "actor", Reason: "reason"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled begin error=%v", err)
	}
}

func TestHostControlsRequireAuthorizerAndJournalTogether(t *testing.T) {
	module := moduleForHost("fence-controls", nil)
	for _, controls := range []HostControls{
		{FenceAuthorizer: fenceAuthorizerFunc(allowFence)},
		{FenceJournal: NewMemoryFenceJournal()},
	} {
		if _, err := NewModuleHostWithControls(Version{Major: 1}, []Module{module}, NewMemoryEffectJournal(), &inverseRecorder{}, controls); !errors.Is(err, ErrInvalidHost) {
			t.Fatalf("partial controls error=%v", err)
		}
	}
	if _, err := NewModuleHost(Version{Major: 1}, []Module{module}, NewMemoryEffectJournal(), &inverseRecorder{}); err != nil {
		t.Fatalf("legacy no-controls constructor error=%v", err)
	}
}

func TestFenceUnknownAuditUsesCleanupContextAfterCallerCancellation(t *testing.T) {
	journal := newTestFenceJournal()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	journal.cancelAfterBegin = cancel
	host, _, revision := newAuthorizedFenceHost(t, fenceAuthorizerFunc(allowFence), journal)
	if _, err := host.FenceAuthorized(ctx, fenceCommand(revision, "fence-cancelled-complete")); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled complete error=%v", err)
	}
	if journal.unknowns.Load() != 1 {
		t.Fatalf("unknown was not attempted after caller cancellation: %d", journal.unknowns.Load())
	}
	if _, err := journal.journal.Begin(context.Background(), fenceCommand(revision, "fence-cancelled-complete")); !errors.Is(err, ErrFenceUnknown) {
		t.Fatalf("cleanup audit record error=%v", err)
	}
}

func TestMemoryFenceJournalConcurrentAndCapacityFailClosed(t *testing.T) {
	journal := NewMemoryFenceJournal()
	command := FenceCommand{RequestID: "fence-memory-concurrent", CompositionRevision: "comp-memory", ActorID: "actor-memory", Reason: "test"}
	decisions := make(chan FenceDecision, 2)
	var group sync.WaitGroup
	for range 2 {
		group.Add(1)
		go func() {
			defer group.Done()
			record, err := journal.Begin(context.Background(), command)
			if errors.Is(err, ErrFenceUnknown) {
				decisions <- FenceDecisionUnknown
				return
			}
			if err != nil {
				t.Errorf("concurrent begin: %v", err)
				return
			}
			decisions <- record.Decision
		}()
	}
	group.Wait()
	close(decisions)
	execute, unknown := 0, 0
	for decision := range decisions {
		switch decision {
		case FenceDecisionExecute:
			execute++
		case FenceDecisionUnknown:
			unknown++
		default:
			t.Fatalf("unexpected concurrent decision %q", decision)
		}
	}
	if execute != 1 || unknown != 1 {
		t.Fatalf("concurrent decisions execute=%d unknown=%d", execute, unknown)
	}
	if err := journal.Complete(context.Background(), command, FenceResult{CompositionRevision: command.CompositionRevision, Completed: true}); err != nil {
		t.Fatal(err)
	}
	if record, err := journal.Begin(context.Background(), command); err != nil || record.Decision != FenceDecisionReplay {
		t.Fatalf("completed replay=%#v err=%v", record, err)
	}
	for index := 1; index < MaxMemoryFenceRecords; index++ {
		candidate := FenceCommand{RequestID: fmt.Sprintf("fence-memory-%d", index), CompositionRevision: "comp-memory", ActorID: "actor-memory", Reason: "test"}
		if _, err := journal.Begin(context.Background(), candidate); err != nil {
			t.Fatalf("fill journal at %d: %v", index, err)
		}
	}
	if _, err := journal.Begin(context.Background(), FenceCommand{RequestID: "fence-memory-overflow", CompositionRevision: "comp-memory", ActorID: "actor-memory", Reason: "test"}); !errors.Is(err, ErrFenceJournalFull) {
		t.Fatalf("overflow error=%v", err)
	}
	if record, err := journal.Begin(context.Background(), command); err != nil || record.Decision != FenceDecisionReplay {
		t.Fatalf("full journal did not retain replay evidence: %#v err=%v", record, err)
	}
}

func TestMemoryFenceJournalReplayCannotBeDowngradedToUnknown(t *testing.T) {
	journal := NewMemoryFenceJournal()
	command := FenceCommand{RequestID: "fence-memory-replay", CompositionRevision: "comp-memory", ActorID: "actor-memory", Reason: "test"}
	if _, err := journal.Begin(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	result := FenceResult{CompositionRevision: command.CompositionRevision, Completed: true}
	if err := journal.Complete(context.Background(), command, result); err != nil {
		t.Fatal(err)
	}
	if err := journal.MarkUnknown(context.Background(), command); !errors.Is(err, ErrFenceConflict) {
		t.Fatalf("replay downgrade error=%v", err)
	}
	replay, err := journal.Begin(context.Background(), command)
	if err != nil || replay.Decision != FenceDecisionReplay || replay.Result != result {
		t.Fatalf("replay after rejected downgrade=%#v err=%v", replay, err)
	}
}

func TestFenceAuthorizedDurableBlockFailureHasNoOwnerOrInverseEffect(t *testing.T) {
	ctx := context.Background()
	store := &failingCompositionStore{}
	journal := newTestFenceJournal()
	module := moduleForHost("fence-durable-block", nil)
	module.effects = 1
	inverse := &inverseRecorder{}
	host, err := OpenModuleHostWithControls(ctx, Version{Major: 1}, []Module{module}, NewMemoryEffectJournal(), inverse, store, HostControls{
		FenceAuthorizer: fenceAuthorizerFunc(allowFence), FenceJournal: journal,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Activate(ctx); err != nil {
		t.Fatal(err)
	}
	revision, found := host.ActiveCompositionRevision()
	if !found {
		t.Fatal("missing active composition")
	}
	store.setFailNext()
	if _, err := host.FenceAuthorized(ctx, fenceCommand(revision, "fence-durable-block-failure")); err == nil {
		t.Fatal("durable block failure unexpectedly fenced")
	}
	inverse.mu.Lock()
	inverseCount := len(inverse.ids)
	inverse.mu.Unlock()
	if inverseCount != 0 {
		t.Fatalf("durable block failure ran inverse %d times", inverseCount)
	}
	module.lastOwner.mu.Lock()
	fenceCalls := module.lastOwner.fenceCalls
	module.lastOwner.mu.Unlock()
	if fenceCalls != 0 {
		t.Fatalf("durable block failure reached owner %d times", fenceCalls)
	}
	if journal.unknowns.Load() != 1 {
		t.Fatalf("durable block failure unknown writes=%d", journal.unknowns.Load())
	}
}

func TestFenceAuthorizedOwnerFailurePersistsBlockAndStopsFreshRecovery(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryCompositionStore()
	journal := newTestFenceJournal()
	module := newRecoverableModule("fence-persisted-block")
	host, err := OpenModuleHostWithControls(ctx, Version{Major: 1}, []Module{module}, NewMemoryEffectJournal(), &inverseRecorder{}, store, HostControls{
		FenceAuthorizer: fenceAuthorizerFunc(allowFence), FenceJournal: journal,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Activate(ctx); err != nil {
		t.Fatal(err)
	}
	revision, found := host.ActiveCompositionRevision()
	if !found {
		t.Fatal("missing active composition")
	}
	module.mu.Lock()
	module.lastOwner.fenceErr = errors.New("owner fence failure")
	module.mu.Unlock()
	if _, err := host.FenceAuthorized(ctx, fenceCommand(revision, "fence-owner-persisted-block")); err == nil {
		t.Fatal("owner failure unexpectedly completed fence")
	}
	state, found, err := store.Load(ctx)
	if err != nil || !found {
		t.Fatalf("durable state found=%v err=%v", found, err)
	}
	if len(state.Compositions) != 1 || state.Compositions[0].Status != DurableCompositionBlocked {
		t.Fatalf("owner failure durable state=%#v", state.Compositions)
	}
	if _, err := RecoverModuleHostWithControls(ctx, Version{Major: 1}, []Module{module}, NewMemoryEffectJournal(), &inverseRecorder{}, store, HostControls{
		FenceAuthorizer: fenceAuthorizerFunc(allowFence), FenceJournal: journal,
	}); !errors.Is(err, ErrRecoveryBlocked) {
		t.Fatalf("blocked recovery error=%v", err)
	}
	_, recoverCalls, _ := module.counts()
	if recoverCalls != 0 {
		t.Fatalf("blocked recovery called Recover %d times", recoverCalls)
	}
}

func TestFenceAuthorizedDurableOwnerPanicMarksUnknownAndReleasesHostLock(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryCompositionStore()
	journal := newTestFenceJournal()
	module := moduleForHost("fence-owner-panic", nil)
	host, err := OpenModuleHostWithControls(ctx, Version{Major: 1}, []Module{module}, NewMemoryEffectJournal(), &inverseRecorder{}, store, HostControls{
		FenceAuthorizer: fenceAuthorizerFunc(allowFence), FenceJournal: journal,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Activate(ctx); err != nil {
		t.Fatal(err)
	}
	revision, found := host.ActiveCompositionRevision()
	if !found {
		t.Fatal("missing active composition")
	}
	composition, err := host.composition(revision)
	if err != nil {
		t.Fatal(err)
	}
	composition.owners[module.manifest.ID] = panicFenceOwner{ModuleLeaseOwner: module.lastOwner}
	command := fenceCommand(revision, "fence-owner-panic")
	if _, err := host.FenceAuthorized(ctx, command); err == nil {
		t.Fatal("owner panic unexpectedly completed fence")
	}
	if _, err := host.FenceAuthorized(ctx, command); !errors.Is(err, ErrFenceUnknown) {
		t.Fatalf("owner panic retry error=%v", err)
	}
	state, found, err := store.Load(ctx)
	if err != nil || !found || len(state.Compositions) != 1 || state.Compositions[0].Status != DurableCompositionBlocked {
		t.Fatalf("owner panic durable state=%#v found=%v err=%v", state.Compositions, found, err)
	}
	if _, err := RecoverModuleHost(ctx, Version{Major: 1}, []Module{module}, NewMemoryEffectJournal(), &inverseRecorder{}, store); !errors.Is(err, ErrRecoveryBlocked) {
		t.Fatalf("owner panic recovery error=%v", err)
	}
}

func TestFenceAuthorizedDurableDeactivationFailureRemainsDraining(t *testing.T) {
	ctx := context.Background()
	store := &failingFenceCompositionStore{store: NewMemoryCompositionStore()}
	journal := newTestFenceJournal()
	module := moduleForHost("fence-durable", nil)
	host, err := OpenModuleHostWithControls(ctx, Version{Major: 1}, []Module{module}, NewMemoryEffectJournal(), &inverseRecorder{}, store, HostControls{
		FenceAuthorizer: fenceAuthorizerFunc(allowFence), FenceJournal: journal,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Activate(ctx); err != nil {
		t.Fatal(err)
	}
	revision, found := host.ActiveCompositionRevision()
	if !found {
		t.Fatal("missing active composition")
	}
	store.failRemove.Store(true)
	if _, err := host.FenceAuthorized(ctx, fenceCommand(revision, "fence-durable-remove")); err == nil {
		t.Fatal("durable removal failure unexpectedly completed")
	}
	if state, _ := host.State(module.manifest.ID); state != StateDraining {
		t.Fatalf("durable removal failure state=%s, want draining", state)
	}
	if journal.unknowns.Load() != 1 {
		t.Fatalf("durable removal unknown writes=%d", journal.unknowns.Load())
	}
}

type failingFenceCompositionStore struct {
	store      *MemoryCompositionStore
	failRemove atomic.Bool
}

func (store *failingFenceCompositionStore) Load(ctx context.Context) (DurableHostState, bool, error) {
	return store.store.Load(ctx)
}

func (store *failingFenceCompositionStore) CompareAndSwap(ctx context.Context, expected string, next DurableHostState) error {
	if store.failRemove.Load() && len(next.Compositions) == 0 {
		return errors.New("durable composition removal failed")
	}
	return store.store.CompareAndSwap(ctx, expected, next)
}

func (store *failingFenceCompositionStore) ClaimHostOwnership(ctx context.Context, holder string, ttl time.Duration) (HostOwnershipClaim, error) {
	return store.store.ClaimHostOwnership(ctx, holder, ttl)
}

func (store *failingFenceCompositionStore) RenewHostOwnership(ctx context.Context, claim HostOwnershipClaim, ttl time.Duration) (HostOwnershipClaim, error) {
	return store.store.RenewHostOwnership(ctx, claim, ttl)
}

func (store *failingFenceCompositionStore) ValidateHostOwnership(ctx context.Context, claim HostOwnershipClaim) error {
	return store.store.ValidateHostOwnership(ctx, claim)
}

func (store *failingFenceCompositionStore) ReleaseHostOwnership(ctx context.Context, claim HostOwnershipClaim) error {
	return store.store.ReleaseHostOwnership(ctx, claim)
}
