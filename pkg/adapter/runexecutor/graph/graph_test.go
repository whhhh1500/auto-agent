package graph

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	memory "github.com/cc-auto-agent/harness-core/pkg/adapter/memory/graphcheckpoint"
	runexecutor "github.com/cc-auto-agent/harness-core/pkg/app/runexecutor"
	core "github.com/cc-auto-agent/harness-core/pkg/core"
	execgraph "github.com/cc-auto-agent/harness-core/pkg/execution/graph"
	contract "github.com/cc-auto-agent/harness-core/pkg/extensions/graph"
)

type testAuthority struct{}

func (testAuthority) Acquire(context.Context, contract.CheckpointKey) (SegmentGrant, error) {
	return SegmentGrant{SegmentID: "seg-test", HostGeneration: 1, Lease: testLease{}}, nil
}

type testLease struct{}

func (testLease) Verify(context.Context, execgraph.LeaseRequest) error  { return nil }
func (testLease) Release(context.Context, execgraph.LeaseRequest) error { return nil }

type countedLease struct {
	verifies, releases atomic.Int32
	releaseErr         error
	releasePanic       bool
}

func (l *countedLease) Verify(context.Context, execgraph.LeaseRequest) error {
	l.verifies.Add(1)
	return nil
}
func (l *countedLease) Release(context.Context, execgraph.LeaseRequest) error {
	l.releases.Add(1)
	if l.releasePanic {
		panic("secret release panic")
	}
	if l.releaseErr != nil {
		return l.releaseErr
	}
	return nil
}

type countedAuthority struct{ lease *countedLease }

func (a countedAuthority) Acquire(context.Context, contract.CheckpointKey) (SegmentGrant, error) {
	return SegmentGrant{SegmentID: "seg-e2e", HostGeneration: 1, Lease: a.lease}, nil
}

type incrementingAuthority struct {
	lease      *countedLease
	generation atomic.Uint64
}

func (a *incrementingAuthority) Acquire(context.Context, contract.CheckpointKey) (SegmentGrant, error) {
	return SegmentGrant{SegmentID: "seg-e2e", HostGeneration: a.generation.Add(1), Lease: a.lease}, nil
}

type countingModel struct{ calls atomic.Int32 }

func (m *countingModel) Provider() string         { return "mock" }
func (m *countingModel) ArtifactRevision() string { return "mock/counting" }
func (m *countingModel) Stream(ctx context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	m.calls.Add(1)
	return core.MockLlmAdapter{}.Stream(ctx, options, emit)
}

type approvalCapability struct {
	manifest core.CapabilityManifest
	calls    *atomic.Int32
}

func (c approvalCapability) Manifest() core.CapabilityManifest { return c.manifest }
func (c approvalCapability) Execute(context.Context, core.CapabilityRequest) (core.CapabilityResult, error) {
	c.calls.Add(1)
	return core.CapabilityResult{Content: `{"ok":true}`, OK: true}, nil
}

type approvalModel struct{ call core.ToolCall }

func (m approvalModel) Provider() string { return "approval-test" }
func (m approvalModel) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	if len(options.Messages) > 0 && options.Messages[len(options.Messages)-1].Role == core.RoleTool {
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: "approved"})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
		return nil
	}
	call := m.call
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &call})
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
	return nil
}

type sharedApprovalState struct{ decision atomic.Int32 }
type durableApprover struct{ state *sharedApprovalState }

func (a *durableApprover) RequestApproval(context.Context, core.ApprovalRequest) (core.ApprovalResolution, error) {
	d := core.ApprovalPending
	if a.state.decision.Load() == 1 {
		d = core.ApprovalApproved
	}
	if a.state.decision.Load() == 2 {
		d = core.ApprovalDenied
	}
	return core.ApprovalResolution{ApprovalID: "apr_0123456789abcdef0123456789abcdef", Decision: d, ExpiresAt: time.Now().Add(time.Hour)}, nil
}
func (a *durableApprover) Approve(ctx context.Context, req core.ApprovalRequest) (core.ApprovalDecision, error) {
	r, err := a.RequestApproval(ctx, req)
	return r.Decision, err
}

type graphDecisionResolver struct{ state *sharedApprovalState }

func (r graphDecisionResolver) Resolve(_ context.Context, principal core.Principal, key contract.CheckpointKey, id string) (execgraph.ApprovalDecision, error) {
	decision := execgraph.ApprovalDecisionDenied
	if r.state.decision.Load() == 1 {
		decision = execgraph.ApprovalDecisionApproved
	}
	return execgraph.ApprovalDecision{TenantID: principal.TenantID, Key: key, ApprovalID: id, Decision: decision, Revision: 3, SourceSegmentID: "seg-e2e", ActorID: "operator", AuthorizationBasis: "test"}, nil
}

type graphApprovalAuthorizer struct{}

func (graphApprovalAuthorizer) Authorize(context.Context, execgraph.ApprovalAuthorizationRequest) error {
	return nil
}

func approvalE2E(t *testing.T, decision execgraph.ApprovalDecisionCode) (runexecutor.RunExecutor, core.Principal, *core.Session, *durableApprover, *atomic.Int32) {
	return approvalE2EWithAuthorizer(t, decision, graphApprovalAuthorizer{})
}
func approvalE2EWithAuthorizer(t *testing.T, decision execgraph.ApprovalDecisionCode, authorizer execgraph.ApprovalAuthorizer) (runexecutor.RunExecutor, core.Principal, *core.Session, *durableApprover, *atomic.Int32) {
	executor, principal, session, approver, calls, _ := approvalE2EWithAuthorizerAndLease(t, decision, authorizer)
	return executor, principal, session, approver, calls
}
func approvalE2EWithAuthorizerAndLease(t *testing.T, decision execgraph.ApprovalDecisionCode, authorizer execgraph.ApprovalAuthorizer) (runexecutor.RunExecutor, core.Principal, *core.Session, *durableApprover, *atomic.Int32, *countedLease) {
	t.Helper()
	product := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"}, core.ScopeRef{Kind: core.ScopeProduct, ID: "product"})
	userScope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"}, core.ScopeRef{Kind: core.ScopeProduct, ID: "product"}, core.ScopeRef{Kind: core.ScopeUser, ID: "user"})
	principal := core.Principal{SubjectID: "user", TenantID: "tenant", Scope: userScope}
	call := core.ToolCall{ID: "call-approval", Name: "test.approve", Args: map[string]any{"ok": true}}
	manifest := core.CapabilityManifest{ID: call.Name, Version: "1", Name: call.Name, Kind: core.KindTool, Contract: "harness.tool/v1", Tool: &core.ToolExposure{}, RequiresApproval: true}
	calls := &atomic.Int32{}
	capabilities := core.NewCapabilityRegistry()
	if err := capabilities.Register(product, approvalCapability{manifest: manifest, calls: calls}); err != nil {
		t.Fatal(err)
	}
	profiles := core.NewAgentProfileRegistry()
	selection := core.ModelSelection{Provider: "approval-test", Model: "approval-test"}
	if err := profiles.Bind(core.AgentProfileLayer{Scope: product, ProfileID: "general", Model: &selection, AddCapabilities: []string{call.Name}}); err != nil {
		t.Fatal(err)
	}
	state := &sharedApprovalState{}
	approver := &durableApprover{state: state}
	runtime := &core.Runtime{Capabilities: capabilities, Profiles: profiles, Approver: approver, Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) {
		return approvalModel{call: call}, nil
	})}
	scope, err := userScope.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "session-approval-e2e"})
	if err != nil {
		t.Fatal(err)
	}
	session, err := core.NewSession(core.SessionOptions{ID: "session-approval-e2e", ProfileID: "general", Principal: principal, Scope: scope})
	if err != nil {
		t.Fatal(err)
	}
	definition, err := NewDefaultDefinition()
	if err != nil {
		t.Fatal(err)
	}
	lease := &countedLease{}
	registration, err := Registration(Options{Definition: definition, Store: memory.NewDefault(), Authority: &incrementingAuthority{lease: lease}, Planner: ReferencePlanner{}, Approval: authorizer, Decisions: graphDecisionResolver{state: state}})
	if err != nil {
		t.Fatal(err)
	}
	executor, err := registration.Factory(runexecutor.Dependencies{Runtime: runtime})
	if err != nil {
		t.Fatal(err)
	}
	return executor, principal, session, approver, calls, lease
}

func TestApprovalSuspendApprovedResume(t *testing.T) {
	executor, principal, session, approver, calls := approvalE2E(t, execgraph.ApprovalDecisionApproved)
	result, err := executor.RunTurn(context.Background(), principal, session, core.TurnInput{RunID: "run-approval-e2e", Text: "approve"}, nil)
	if err != nil || result.Status != core.RunWaitingApproval {
		t.Fatalf("suspend result=%#v err=%v", result, err)
	}
	approver.state.decision.Store(1)
	result, err = executor.ResumeTurn(context.Background(), principal, session, core.ResumeInput{RunID: "run-approval-e2e"}, nil)
	if err != nil || result.Status != core.RunCompleted {
		t.Fatalf("resume result=%#v err=%v", result, err)
	}
	if calls.Load() == 0 {
		t.Fatal("approval capability was not called")
	}
}

func TestApprovalDeniedDoesNotInvokeResumeNode(t *testing.T) {
	executor, principal, session, approver, calls := approvalE2E(t, execgraph.ApprovalDecisionDenied)
	result, err := executor.RunTurn(context.Background(), principal, session, core.TurnInput{RunID: "run-approval-e2e", Text: "deny"}, nil)
	if err != nil || result.Status != core.RunWaitingApproval {
		t.Fatalf("suspend result=%#v err=%v", result, err)
	}
	approver.state.decision.Store(2)
	result, err = executor.ResumeTurn(context.Background(), principal, session, core.ResumeInput{RunID: "run-approval-e2e"}, nil)
	if err == nil || result.Status == core.RunCompleted {
		t.Fatalf("denied result=%#v err=%v", result, err)
	}
	if calls.Load() != 0 {
		t.Fatalf("denied approval invoked capability=%d", calls.Load())
	}
}

func e2eExecutor(t *testing.T) (runexecutor.RunExecutor, core.Principal, *core.Session, *countingModel) {
	return e2eExecutorWithAuthority(t, fixedAuthority{lease: &countedLease{}, segment: "seg", generation: 1})
}
func e2eExecutorWithAuthority(t *testing.T, authority SegmentAuthority) (runexecutor.RunExecutor, core.Principal, *core.Session, *countingModel) {
	return e2eExecutorWithAuthorityAndStore(t, authority, memory.NewDefault())
}
func e2eExecutorWithAuthorityAndStore(t *testing.T, authority SegmentAuthority, store contract.Store) (runexecutor.RunExecutor, core.Principal, *core.Session, *countingModel) {
	t.Helper()
	product := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"}, core.ScopeRef{Kind: core.ScopeProduct, ID: "product"})
	userScope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"}, core.ScopeRef{Kind: core.ScopeProduct, ID: "product"}, core.ScopeRef{Kind: core.ScopeUser, ID: "user"})
	principal := core.Principal{SubjectID: "user", TenantID: "tenant", Scope: userScope}
	profiles := core.NewAgentProfileRegistry()
	selection := core.ModelSelection{Provider: "mock", Model: "mock"}
	if err := profiles.Bind(core.AgentProfileLayer{Scope: product, ProfileID: "general", Model: &selection}); err != nil {
		t.Fatal(err)
	}
	model := &countingModel{}
	runtime := &core.Runtime{Capabilities: core.NewCapabilityRegistry(), Profiles: profiles, Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) { return model, nil })}
	scope, err := userScope.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "session-concurrent"})
	if err != nil {
		t.Fatal(err)
	}
	session, err := core.NewSession(core.SessionOptions{ID: "session-concurrent", ProfileID: "general", Principal: principal, Scope: scope})
	if err != nil {
		t.Fatal(err)
	}
	definition, err := NewDefaultDefinition()
	if err != nil {
		t.Fatal(err)
	}
	registration, err := Registration(Options{Definition: definition, Store: store, Authority: authority, Planner: ReferencePlanner{}})
	if err != nil {
		t.Fatal(err)
	}
	executor, err := registration.Factory(runexecutor.Dependencies{Runtime: runtime})
	if err != nil {
		t.Fatal(err)
	}
	return executor, principal, session, model
}

type testStore struct{}

type createErrorStore struct{ inner *memory.Store }

func (s createErrorStore) Load(ctx context.Context, key contract.CheckpointKey) (contract.Checkpoint, error) {
	return s.inner.Load(ctx, key)
}
func (s createErrorStore) Create(context.Context, contract.Checkpoint, contract.Transition) (contract.Checkpoint, contract.CommitDisposition, error) {
	return contract.Checkpoint{}, contract.CommitUnknown, errors.New("secret create failure")
}
func (s createErrorStore) CompareAndSwap(ctx context.Context, key contract.CheckpointKey, rev uint64, next contract.Checkpoint, tr contract.Transition) (contract.Checkpoint, contract.CommitDisposition, error) {
	return s.inner.CompareAndSwap(ctx, key, rev, next, tr)
}
func (s createErrorStore) ListTransitions(ctx context.Context, key contract.CheckpointKey, from uint64, limit int) ([]contract.Transition, error) {
	return s.inner.ListTransitions(ctx, key, from, limit)
}

type executingFaultStore struct {
	inner    *memory.Store
	injected atomic.Bool
}

func (s *executingFaultStore) Load(ctx context.Context, key contract.CheckpointKey) (contract.Checkpoint, error) {
	return s.inner.Load(ctx, key)
}
func (s *executingFaultStore) Create(ctx context.Context, c contract.Checkpoint, tr contract.Transition) (contract.Checkpoint, contract.CommitDisposition, error) {
	return s.inner.Create(ctx, c, tr)
}
func (s *executingFaultStore) CompareAndSwap(ctx context.Context, key contract.CheckpointKey, rev uint64, next contract.Checkpoint, tr contract.Transition) (contract.Checkpoint, contract.CommitDisposition, error) {
	out, d, err := s.inner.CompareAndSwap(ctx, key, rev, next, tr)
	if err == nil && next.Status == contract.CheckpointExecuting && !s.injected.Swap(true) {
		return out, d, errors.New("injected executing CAS failure")
	}
	return out, d, err
}
func (s *executingFaultStore) ListTransitions(ctx context.Context, key contract.CheckpointKey, after uint64, limit int) ([]contract.Transition, error) {
	return s.inner.ListTransitions(ctx, key, after, limit)
}

type fixedAuthority struct {
	lease      execgraph.SegmentLease
	segment    string
	generation uint64
}

func (a fixedAuthority) Acquire(context.Context, contract.CheckpointKey) (SegmentGrant, error) {
	return SegmentGrant{SegmentID: a.segment, HostGeneration: a.generation, Lease: a.lease}, nil
}

type faultAuthority struct {
	lease    *countedLease
	mode     string
	acquires *atomic.Int32
}

func (a faultAuthority) Acquire(context.Context, contract.CheckpointKey) (SegmentGrant, error) {
	if a.acquires != nil {
		a.acquires.Add(1)
	}
	switch a.mode {
	case "error":
		return SegmentGrant{}, errors.New("secret")
	case "panic":
		panic("secret")
	case "segment":
		return SegmentGrant{HostGeneration: 1, Lease: a.lease}, nil
	case "generation":
		return SegmentGrant{SegmentID: "seg", Lease: a.lease}, nil
	case "lease":
		return SegmentGrant{SegmentID: "seg", HostGeneration: 1}, nil
	}
	return SegmentGrant{SegmentID: "seg", HostGeneration: 1, Lease: a.lease}, nil
}

type recordingAuthority struct {
	inner    SegmentAuthority
	acquires *atomic.Int32
}

func (a recordingAuthority) Acquire(ctx context.Context, key contract.CheckpointKey) (SegmentGrant, error) {
	a.acquires.Add(1)
	return a.inner.Acquire(ctx, key)
}

type verifyFaultLease struct {
	countedLease
	mode string
}

func (l *verifyFaultLease) Verify(context.Context, execgraph.LeaseRequest) error {
	l.verifies.Add(1)
	if l.mode == "panic" {
		panic("secret")
	}
	return errors.New("secret")
}

func (testStore) Load(context.Context, contract.CheckpointKey) (contract.Checkpoint, error) {
	return contract.Checkpoint{}, contract.ErrCheckpointNotFound
}
func (testStore) Create(context.Context, contract.Checkpoint, contract.Transition) (contract.Checkpoint, contract.CommitDisposition, error) {
	return contract.Checkpoint{}, contract.CommitUnknown, errors.New("not used")
}
func (testStore) CompareAndSwap(context.Context, contract.CheckpointKey, uint64, contract.Checkpoint, contract.Transition) (contract.Checkpoint, contract.CommitDisposition, error) {
	return contract.Checkpoint{}, contract.CommitUnknown, errors.New("not used")
}
func (testStore) ListTransitions(context.Context, contract.CheckpointKey, uint64, int) ([]contract.Transition, error) {
	return nil, errors.New("not used")
}

func TestDefaultDefinitionAndReferencePlannerAreBounded(t *testing.T) {
	definition, err := NewDefaultDefinition()
	if err != nil {
		t.Fatal(err)
	}
	if definition.Revision() == "" || definition.Definition().ID != ID {
		t.Fatalf("definition=%#v", definition.Definition())
	}
	view := contract.ContextView{StateFields: []string{statusField}, Layers: []contract.LayerKind{contract.LayerCurrentInput, contract.LayerRequiredState}}
	budget := contract.ContextBudget{TotalTokens: 32, LayerBudgets: map[contract.LayerKind]int64{contract.LayerCurrentInput: 16, contract.LayerRequiredState: 16}}
	plan, err := (ReferencePlanner{}).Plan(context.Background(), execgraph.ContextRequest{Key: contract.CheckpointKey{TenantID: "tenant", SessionID: "session", RunID: "run"}, DefinitionRevision: definition.Revision(), View: view, Budget: budget})
	if err != nil {
		t.Fatal(err)
	}
	if err := contract.ValidateContextPlan(plan); err != nil {
		t.Fatalf("reference plan invalid: %v", err)
	}
}

func TestRegistrationIsFixedAndRequiresLeaseDependencies(t *testing.T) {
	definition, err := NewDefaultDefinition()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Registration(Options{Definition: definition, Store: testStore{}, Authority: testAuthority{}}); !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("missing planner err=%v", err)
	}
	registration, err := Registration(Options{Definition: definition, Store: testStore{}, Authority: testAuthority{}, Planner: ReferencePlanner{}})
	if err != nil {
		t.Fatal(err)
	}
	if registration.Metadata.ID != ID || registration.Metadata.Version != Version || registration.Metadata.ImplementationRevision != ImplementationRevision {
		t.Fatalf("metadata=%#v", registration.Metadata)
	}
	registry, err := runexecutor.NewRegistry(2, registration)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := registry.Resolve(ID, Version, runexecutor.Dependencies{}); !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("nil runtime err=%v", err)
	}
}

func TestCompleteLargeAnswerNotPersisted(t *testing.T) {
	definition, err := NewDefaultDefinition()
	if err != nil {
		t.Fatal(err)
	}
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"}, core.ScopeRef{Kind: core.ScopeProduct, ID: "product"})
	userScope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"}, core.ScopeRef{Kind: core.ScopeProduct, ID: "product"}, core.ScopeRef{Kind: core.ScopeUser, ID: "user"})
	_ = global
	principal := core.Principal{SubjectID: "user", TenantID: "tenant", Scope: userScope}
	profiles := core.NewAgentProfileRegistry()
	model := core.ModelSelection{Provider: "mock", Model: "mock"}
	if err := profiles.Bind(core.AgentProfileLayer{Scope: product, ProfileID: "general", Model: &model}); err != nil {
		t.Fatal(err)
	}
	runtime := &core.Runtime{Capabilities: core.NewCapabilityRegistry(), Profiles: profiles, Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) { return core.MockLlmAdapter{}, nil })}
	scope, err := userScope.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "session-e2e"})
	if err != nil {
		t.Fatal(err)
	}
	session, err := core.NewSession(core.SessionOptions{ID: "session-e2e", ProfileID: "general", Principal: principal, Scope: scope})
	if err != nil {
		t.Fatal(err)
	}
	lease := &countedLease{}
	registration, err := Registration(Options{Definition: definition, Store: memory.NewDefault(), Authority: countedAuthority{lease: lease}, Planner: ReferencePlanner{}})
	if err != nil {
		t.Fatal(err)
	}
	executor, err := registration.Factory(runexecutor.Dependencies{Runtime: runtime})
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.RunTurn(context.Background(), principal, session, core.TurnInput{RunID: "run-e2e", Text: "hello"}, nil)
	if err != nil || result.Status != core.RunCompleted {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	if result.Answer == "" {
		t.Fatal("completed answer was lost for current call")
	}
	if lease.releases.Load() == 0 {
		t.Fatal("lease was not released after completion")
	}
}

func TestConcurrentSameRunAtMostOneCoreTurn(t *testing.T) {
	executor, principal, session, model := e2eExecutor(t)
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			_, err := executor.RunTurn(context.Background(), principal, session, core.TurnInput{RunID: "run-concurrent", Text: "hello"}, nil)
			results <- err
		}()
	}
	for i := 0; i < 2; i++ {
		<-results
	}
	if got := model.calls.Load(); got > 1 {
		t.Fatalf("core model calls=%d, want at most one", got)
	}
}

func TestDecisionResolverErrorAndPanicSanitized(t *testing.T) {
	key := contract.CheckpointKey{TenantID: "tenant", SessionID: "session", RunID: "run"}
	resolver := resolverFunc(func(context.Context, core.Principal, contract.CheckpointKey, string) (execgraph.ApprovalDecision, error) {
		return execgraph.ApprovalDecision{}, errors.New("secret resolver detail")
	})
	if _, err := safeResolve(context.Background(), resolver, core.Principal{}, key, "approval"); !errors.Is(err, ErrApprovalResolverUnavailable) {
		t.Fatalf("err=%v", err)
	}
	panicResolver := resolverFunc(func(context.Context, core.Principal, contract.CheckpointKey, string) (execgraph.ApprovalDecision, error) {
		panic("secret")
	})
	if _, err := safeResolve(context.Background(), panicResolver, core.Principal{}, key, "approval"); !errors.Is(err, ErrCallbackPanic) {
		t.Fatalf("panic err=%v", err)
	}
}

func TestLeaseRejectAndReleaseCounts(t *testing.T) {
	for _, mode := range []string{"error", "panic", "segment", "generation", "lease"} {
		t.Run(mode, func(t *testing.T) {
			lease := &countedLease{}
			acquires := &atomic.Int32{}
			executor, principal, session, model := e2eExecutorWithAuthority(t, faultAuthority{lease: lease, mode: mode, acquires: acquires})
			_, err := executor.RunTurn(context.Background(), principal, session, core.TurnInput{RunID: "run-lease", Text: "hello"}, nil)
			if err == nil || !errors.Is(err, ErrSegmentUnavailable) || strings.Contains(err.Error(), "secret") || acquires.Load() != 1 || model.calls.Load() != 0 || lease.releases.Load() != 0 {
				t.Fatalf("err=%v acquire=%d model=%d release=%d", err, acquires.Load(), model.calls.Load(), lease.releases.Load())
			}
		})
	}
	for _, mode := range []string{"error", "panic"} {
		t.Run("verify_"+mode, func(t *testing.T) {
			lease := &verifyFaultLease{mode: mode}
			acquires := &atomic.Int32{}
			executor, principal, session, model := e2eExecutorWithAuthority(t, recordingAuthority{inner: fixedAuthority{lease: lease, segment: "seg", generation: 1}, acquires: acquires})
			_, err := executor.RunTurn(context.Background(), principal, session, core.TurnInput{RunID: "run-verify", Text: "hello"}, nil)
			if err == nil || !errors.Is(err, ErrSegmentUnavailable) || strings.Contains(err.Error(), "secret") || acquires.Load() != 1 || lease.verifies.Load() != 1 || model.calls.Load() != 0 || lease.releases.Load() != 1 {
				t.Fatalf("err=%v acquire=%d verify=%d model=%d release=%d", err, acquires.Load(), lease.verifies.Load(), model.calls.Load(), lease.releases.Load())
			}
		})
	}
}

func TestLeaseTerminalReleaseOutcomes(t *testing.T) {
	cases := []struct {
		name  string
		panic bool
	}{{"release_error", false}, {"release_panic", true}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lease := &countedLease{releasePanic: tc.panic}
			if !tc.panic {
				lease.releaseErr = errors.New("secret release error")
			}
			executor, principal, session, model := e2eExecutorWithAuthority(t, fixedAuthority{lease: lease, segment: "seg", generation: 1})
			result, err := executor.RunTurn(context.Background(), principal, session, core.TurnInput{RunID: "run-release", Text: "hello"}, nil)
			// The graph verifies the active lease at each checkpoint mutation
			// boundary and around node execution. The important ownership
			// invariant here is one physical release despite the terminal
			// release error/panic.
			if err == nil || !errors.Is(err, ErrSegmentUnavailable) || strings.Contains(err.Error(), "secret") || result.Status != core.RunCompleted || model.calls.Load() != 1 || lease.verifies.Load() != 7 || lease.releases.Load() != 1 {
				t.Fatalf("result=%#v err=%v verify=%d release=%d model=%d", result, err, lease.verifies.Load(), lease.releases.Load(), model.calls.Load())
			}
		})
	}
}

func TestWaitingApprovalReleaseOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name  string
		panic bool
	}{{"release_error", false}, {"release_panic", true}} {
		t.Run(tc.name, func(t *testing.T) {
			executor, principal, session, _, calls, lease := approvalE2EWithAuthorizerAndLease(t, execgraph.ApprovalDecisionApproved, graphApprovalAuthorizer{})
			lease.releasePanic = tc.panic
			if !tc.panic {
				lease.releaseErr = errors.New("secret waiting release failure")
			}
			result, err := executor.RunTurn(context.Background(), principal, session, core.TurnInput{RunID: "run-wait-release", Text: "approve"}, nil)
			if result.Status != core.RunWaitingApproval || err == nil || !errors.Is(err, ErrSegmentUnavailable) || strings.Contains(err.Error(), "secret") || lease.releases.Load() != 1 || lease.verifies.Load() != 7 || calls.Load() != 0 {
				t.Fatalf("result=%#v err=%v verify=%d release=%d capability=%d", result, err, lease.verifies.Load(), lease.releases.Load(), calls.Load())
			}
		})
	}
}

func TestWaitingApprovalReleaseSuccessIsEngineOwned(t *testing.T) {
	executor, principal, session, _, calls, lease := approvalE2EWithAuthorizerAndLease(t, execgraph.ApprovalDecisionApproved, graphApprovalAuthorizer{})
	result, err := executor.RunTurn(context.Background(), principal, session, core.TurnInput{RunID: "run-wait-success", Text: "approve"}, nil)
	if err != nil || result.Status != core.RunWaitingApproval || lease.releases.Load() != 1 || lease.verifies.Load() != 7 || calls.Load() != 0 {
		t.Fatalf("result=%#v err=%v verify=%d release=%d capability=%d", result, err, lease.verifies.Load(), lease.releases.Load(), calls.Load())
	}
}

func TestStoreCreateErrorReleasesLease(t *testing.T) {
	lease := &countedLease{}
	store := createErrorStore{inner: memory.NewDefault()}
	executor, principal, session, model := e2eExecutorWithAuthorityAndStore(t, fixedAuthority{lease: lease, segment: "seg", generation: 1}, store)
	result, err := executor.RunTurn(context.Background(), principal, session, core.TurnInput{RunID: "run-create-error", Text: "hello"}, nil)
	if err == nil || !errors.Is(err, execgraph.ErrExecutionFailed) || strings.Contains(err.Error(), "secret") || result.Status == core.RunCompleted || model.calls.Load() != 0 || lease.releases.Load() != 1 {
		t.Fatalf("result=%#v err=%v model=%d release=%d", result, err, model.calls.Load(), lease.releases.Load())
	}
}

func TestExecutingRecoveryUnknownNoReplay(t *testing.T) {
	product := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"}, core.ScopeRef{Kind: core.ScopeProduct, ID: "product"})
	userScope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"}, core.ScopeRef{Kind: core.ScopeProduct, ID: "product"}, core.ScopeRef{Kind: core.ScopeUser, ID: "user"})
	principal := core.Principal{SubjectID: "user", TenantID: "tenant", Scope: userScope}
	profiles := core.NewAgentProfileRegistry()
	selection := core.ModelSelection{Provider: "mock", Model: "mock"}
	if err := profiles.Bind(core.AgentProfileLayer{Scope: product, ProfileID: "general", Model: &selection}); err != nil {
		t.Fatal(err)
	}
	model := &countingModel{}
	runtime := &core.Runtime{Capabilities: core.NewCapabilityRegistry(), Profiles: profiles, Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) { return model, nil })}
	scope, _ := userScope.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "session-recovery"})
	session, err := core.NewSession(core.SessionOptions{ID: "session-recovery", ProfileID: "general", Principal: principal, Scope: scope})
	if err != nil {
		t.Fatal(err)
	}
	store := &executingFaultStore{inner: memory.NewDefault()}
	lease := &countedLease{}
	definition, _ := NewDefaultDefinition()
	registration, err := Registration(Options{Definition: definition, Store: store, Authority: fixedAuthority{lease: lease, segment: "seg-1", generation: 1}, Planner: ReferencePlanner{}})
	if err != nil {
		t.Fatal(err)
	}
	executor, _ := registration.Factory(runexecutor.Dependencies{Runtime: runtime})
	_, _ = executor.RunTurn(context.Background(), principal, session, core.TurnInput{RunID: "run-recovery", Text: "hello"}, nil)
	key := contract.CheckpointKey{TenantID: principal.TenantID, SessionID: session.ID(), RunID: "run-recovery"}
	checkpoint, err := store.Load(context.Background(), key)
	if err != nil || checkpoint.Status != contract.CheckpointExecuting {
		t.Fatalf("checkpoint=%#v err=%v", checkpoint, err)
	}
	registration, err = Registration(Options{Definition: definition, Store: store, Authority: fixedAuthority{lease: lease, segment: "seg-2", generation: 2}, Planner: ReferencePlanner{}})
	if err != nil {
		t.Fatal(err)
	}
	executor, _ = registration.Factory(runexecutor.Dependencies{Runtime: runtime})
	result, _ := executor.RunTurn(context.Background(), principal, session, core.TurnInput{RunID: "run-recovery", Text: "hello"}, nil)
	if result.Status != core.RunFailed {
		t.Fatalf("recovery result=%#v", result)
	}
	checkpoint, _ = store.Load(context.Background(), key)
	if checkpoint.Status != contract.CheckpointUnknown || model.calls.Load() != 0 {
		t.Fatalf("checkpoint=%#v model_calls=%d", checkpoint, model.calls.Load())
	}
}

type authorizerFunc func(context.Context, execgraph.ApprovalAuthorizationRequest) error

func (f authorizerFunc) Authorize(ctx context.Context, request execgraph.ApprovalAuthorizationRequest) error {
	return f(ctx, request)
}

func TestApprovalAuthorizerErrorAndPanic(t *testing.T) {
	for name, authorizer := range map[string]execgraph.ApprovalAuthorizer{
		"error": authorizerFunc(func(context.Context, execgraph.ApprovalAuthorizationRequest) error { return errors.New("secret") }),
		"panic": authorizerFunc(func(context.Context, execgraph.ApprovalAuthorizationRequest) error { panic("secret") }),
	} {
		t.Run(name, func(t *testing.T) {
			executor, principal, session, approver, calls := approvalE2EWithAuthorizer(t, execgraph.ApprovalDecisionApproved, authorizer)
			result, err := executor.RunTurn(context.Background(), principal, session, core.TurnInput{RunID: "run-auth", Text: "approve"}, nil)
			if err != nil || result.Status != core.RunWaitingApproval {
				t.Fatalf("suspend=%#v err=%v", result, err)
			}
			approver.state.decision.Store(1)
			result, err = executor.ResumeTurn(context.Background(), principal, session, core.ResumeInput{RunID: "run-auth"}, nil)
			if err == nil || result.Status == core.RunCompleted || calls.Load() != 0 {
				t.Fatalf("resume=%#v err=%v calls=%d", result, err, calls.Load())
			}
		})
	}
}

type resolverFunc func(context.Context, core.Principal, contract.CheckpointKey, string) (execgraph.ApprovalDecision, error)

func (f resolverFunc) Resolve(ctx context.Context, principal core.Principal, key contract.CheckpointKey, id string) (execgraph.ApprovalDecision, error) {
	return f(ctx, principal, key, id)
}
