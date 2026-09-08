package graph

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	memory "github.com/whhhh1500/auto-agent/pkg/adapter/memory/graphcheckpoint"
	contract "github.com/whhhh1500/auto-agent/pkg/extensions/graph"
)

func TestExecutorThreeNodeHappyPath(t *testing.T) {
	t.Parallel()
	calls := make([]string, 0, 3)
	var mu sync.Mutex
	executor, store := testExecutor(t, threeNodeDefinition(t), funcNode(func(_ context.Context, request NodeRequest) (NodeResult, error) {
		mu.Lock()
		calls = append(calls, request.Spec.ID)
		mu.Unlock()
		if request.Spec.ID == "middle" {
			return NodeResult{Patch: contract.StatePatch{Set: map[string]json.RawMessage{"account": json.RawMessage(`"updated"`)}}}, nil
		}
		return NodeResult{}, nil
	}))
	result, err := executor.Run(context.Background(), StartRequest{Key: testKey(), SegmentID: "segment-1", HostGeneration: 1, InitialState: initialState()})
	if err != nil {
		t.Fatal(err)
	}
	if result.Checkpoint.Status != contract.CheckpointCompleted || string(result.Checkpoint.State["account"]) != `"updated"` {
		t.Fatalf("result = %#v", result.Checkpoint)
	}
	if got := len(calls); got != 3 {
		t.Fatalf("node calls = %v", calls)
	}
	transitions, err := store.ListTransitions(context.Background(), testKey(), 0, 16)
	if err != nil || len(transitions) != 7 {
		t.Fatalf("transitions = %d, %v", len(transitions), err)
	}
	edges := 0
	for _, transition := range transitions {
		if transition.Edge != nil {
			edges++
			if transition.SourceNodeID == "" || transition.SourceAttemptID == "" {
				t.Fatalf("edge transition lost causal attempt: %#v", transition)
			}
		}
		if transition.OutcomeCode == "node_started" && transition.Edge != nil {
			t.Fatalf("node start carried stale edge evidence: %#v", transition)
		}
	}
	if edges != 2 {
		t.Fatalf("edge evidence count = %d", edges)
	}
}

func TestExecutorProductionUsesOutcomeConstants(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	source, err := os.ReadFile(strings.Replace(file, "executor_test.go", "executor.go", 1))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(source), "\n") {
		if strings.Contains(line, "OutcomeCode =") || strings.Contains(line, "Event") && strings.Contains(line, "= \"") {
			continue
		}
		for _, raw := range []string{"created", "node_started", "approval_pending", "retry", "error_edge", "edge_selected", "completed", "cancelled", "recovery_unknown", "interrupted_unknown", "node_timeout", "node_failed", "preflight_failed", "binding_missing", "lease_unavailable", "sandbox_unavailable", "sandbox_denied", "context_unavailable", "invalid_approval", "reducer_failed", "edge_selection_failed", "step_limit", "visit_limit", "unknown_node", "approval_approved", "approval_denied", "unknown"} {
			if strings.Contains(line, "\""+raw+"\"") {
				t.Errorf("production outcome literal %q remains: %s", raw, strings.TrimSpace(line))
			}
		}
	}
}

func TestExecutorConcurrentReadyCommitInvokesNodeOnce(t *testing.T) {
	t.Parallel()
	definition := oneNodeDefinition(t, contract.RetryPolicy{MaxAttempts: 1})
	arrived := make(chan struct{}, 2)
	releaseCAS := make(chan struct{})
	started := make(chan struct{})
	allowNode := make(chan struct{})
	var nodeCalls atomic.Int32
	var once sync.Once
	observer := &recordingObserver{}
	store := &casBarrierStore{Store: memory.NewDefault(), arrived: arrived, release: releaseCAS}
	node := funcNode(func(_ context.Context, _ NodeRequest) (NodeResult, error) {
		nodeCalls.Add(1)
		once.Do(func() { close(started) })
		<-allowNode
		return NodeResult{}, nil
	})
	bindings, err := NewBindings(definition, "composition-1", []NodeBinding{{Kind: "prompt", Version: "1", Revision: "node-v1", Node: node}}, BuiltinReducerBinding("builtin-v1"), nil)
	if err != nil {
		t.Fatal(err)
	}
	executor1, err := NewExecutorWithOptions(definition, bindings, store, Options{Lease: testLease{}, Planner: testPlanner{}, Observer: observer})
	if err != nil {
		t.Fatal(err)
	}
	executor2, err := NewExecutorWithOptions(definition, bindings, store, Options{Lease: testLease{}, Planner: testPlanner{}, Observer: observer})
	if err != nil {
		t.Fatal(err)
	}
	request := StartRequest{Key: testKey(), SegmentID: "segment-1", HostGeneration: 1, InitialState: initialState()}
	results := make(chan struct {
		result Result
		err    error
	}, 2)
	for _, executor := range []*Executor{executor1, executor2} {
		go func() {
			result, runErr := executor.Run(context.Background(), request)
			results <- struct {
				result Result
				err    error
			}{result: result, err: runErr}
		}()
	}
	for range 2 {
		select {
		case <-arrived:
		case <-time.After(time.Second):
			t.Fatal("concurrent callers did not reach the same node-start CAS")
		}
	}
	close(releaseCAS)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("winning caller did not enter node")
	}
	if got := nodeCalls.Load(); got != 1 {
		t.Fatalf("node calls while winner is blocked = %d", got)
	}
	select {
	case outcome := <-results:
		if !errors.Is(outcome.err, ErrExecutionInProgress) {
			t.Fatalf("loser result while winner is blocked=%#v err=%v", outcome.result, outcome.err)
		}
	case <-time.After(time.Second):
		t.Fatal("loser did not report the replayed executing fact")
	}
	close(allowNode)
	select {
	case outcome := <-results:
		if outcome.err != nil || outcome.result.Checkpoint.Status != contract.CheckpointCompleted {
			t.Fatalf("winner result=%#v err=%v", outcome.result, outcome.err)
		}
	case <-time.After(time.Second):
		t.Fatal("winner executor did not return")
	}
	if nodeCalls.Load() != 1 {
		t.Fatalf("node_calls=%d", nodeCalls.Load())
	}
	observer.mu.Lock()
	defer observer.mu.Unlock()
	startedEvents := 0
	nodeStartEvents := 0
	for _, event := range observer.events {
		if event.Kind == EventStarted {
			startedEvents++
		}
		if event.Kind == EventNodeStart {
			nodeStartEvents++
		}
	}
	if startedEvents != 1 || nodeStartEvents != 1 {
		t.Fatalf("duplicate observer events: started=%d node_start=%d events=%v", startedEvents, nodeStartEvents, observer.events)
	}
}

func TestExecutorRetriesOnlyTypedRetryableFailure(t *testing.T) {
	t.Parallel()
	definition := oneNodeDefinition(t, contract.RetryPolicy{MaxAttempts: 2})
	attempts := 0
	executor, _ := testExecutor(t, definition, funcNode(func(_ context.Context, _ NodeRequest) (NodeResult, error) {
		attempts++
		if attempts == 1 {
			return NodeResult{}, retryError{}
		}
		return NodeResult{}, nil
	}))
	result, err := executor.Run(context.Background(), StartRequest{Key: testKey(), SegmentID: "segment-1", HostGeneration: 1, InitialState: initialState()})
	if err != nil || result.Checkpoint.Status != contract.CheckpointCompleted || attempts != 2 {
		t.Fatalf("result=%#v attempts=%d err=%v", result.Checkpoint, attempts, err)
	}
}

func TestExecutorApprovalSuspendsAndResumesStableAttempt(t *testing.T) {
	t.Parallel()
	definition := oneNodeDefinition(t, contract.RetryPolicy{MaxAttempts: 1})
	var attempts []string
	executor, _ := testExecutor(t, definition, funcNode(func(_ context.Context, request NodeRequest) (NodeResult, error) {
		attempts = append(attempts, request.AttemptID)
		if len(attempts) == 1 {
			return NodeResult{}, &ApprovalPendingError{ApprovalID: "approval-1"}
		}
		return NodeResult{}, nil
	}))
	executor.approval = testApprovalAuthorizer{}
	request := StartRequest{Key: testKey(), SegmentID: "segment-1", HostGeneration: 1, InitialState: initialState()}
	first, err := executor.Run(context.Background(), request)
	if !errors.Is(err, ErrApprovalPending) || !first.Suspended || first.Checkpoint.Status != contract.CheckpointWaitingApproval {
		t.Fatalf("first=%#v err=%v", first, err)
	}
	resumed, err := executor.Resume(context.Background(), ResumeRequest{Key: testKey(), SegmentID: "segment-2", HostGeneration: 2, ApprovalID: "approval-1", Approved: true, Decision: approvalDecision(testKey(), "approval-1", contract.CheckpointWaitingApproval, 3, "segment-1", true)})
	if err != nil || resumed.Checkpoint.Status != contract.CheckpointCompleted {
		t.Fatalf("resumed=%#v err=%v", resumed, err)
	}
	if len(attempts) != 2 || attempts[0] != attempts[1] {
		t.Fatalf("approval did not retain stable attempt: %v", attempts)
	}
}

func TestExecutorDeniedApprovalCanResumeWithDurableDecision(t *testing.T) {
	t.Parallel()
	definition := baseDefinition([]contract.NodeSpec{testNode("end", true, contract.RetryPolicy{MaxAttempts: 2})}, nil)
	definition.Nodes[0].ApprovalDeniedPolicy = contract.ApprovalDeniedResumeWithDecision
	validated := validateDefinition(t, definition)
	var seen *contract.ApprovalEvidence
	calls := 0
	executor, _ := testExecutor(t, validated, funcNode(func(_ context.Context, request NodeRequest) (NodeResult, error) {
		calls++
		if calls == 1 {
			return NodeResult{}, &ApprovalPendingError{ApprovalID: "approval-1"}
		}
		if calls == 2 {
			if request.ResolvedApproval == nil || request.ResolvedApproval.Decision != "denied" {
				t.Fatalf("retry lost resolved approval: %#v", request.ResolvedApproval)
			}
			return NodeResult{}, retryError{}
		}
		seen = request.ResolvedApproval
		return NodeResult{}, nil
	}))
	executor.approval = testApprovalAuthorizer{}
	first, err := executor.Run(context.Background(), StartRequest{Key: testKey(), SegmentID: "segment-1", HostGeneration: 1, InitialState: initialState()})
	if !errors.Is(err, ErrApprovalPending) || first.Checkpoint.Status != contract.CheckpointWaitingApproval {
		t.Fatalf("first=%#v err=%v", first, err)
	}
	decision := approvalDecision(testKey(), "approval-1", contract.CheckpointWaitingApproval, first.Checkpoint.Revision, "segment-1", false)
	resumed, err := executor.Resume(context.Background(), ResumeRequest{Key: testKey(), SegmentID: "segment-2", HostGeneration: 2, ApprovalID: "approval-1", Decision: decision})
	if err != nil || resumed.Checkpoint.Status != contract.CheckpointCompleted || calls != 3 {
		t.Fatalf("resumed=%#v calls=%d err=%v", resumed, calls, err)
	}
	if seen == nil || seen.Decision != "denied" || seen.ApprovalID != "approval-1" {
		t.Fatalf("resolved approval not delivered: %#v", seen)
	}
}

func TestExecutorExecutingCheckpointBecomesUnknown(t *testing.T) {
	t.Parallel()
	definition := oneNodeDefinition(t, contract.RetryPolicy{MaxAttempts: 1})
	executor, store := testExecutor(t, definition, funcNode(func(context.Context, NodeRequest) (NodeResult, error) { return NodeResult{}, nil }))
	recoveryLease := &recordingLease{}
	executor.lease = recoveryLease
	def := definition.Definition()
	checkpoint := contract.Checkpoint{Key: testKey(), SegmentID: "segment-1", GraphID: def.ID, DefinitionRevision: definition.Revision(), CompositionRevision: "composition-1", ImplementationRevision: executor.bindings.ImplementationRevision(), HostGeneration: 1, CurrentNodeID: "end", Attempt: 1, AttemptID: "end:1:2", Steps: 1, Visits: map[string]int{"end": 1}, Revision: 1, State: initialState(), Status: contract.CheckpointExecuting}
	transition := contract.Transition{ID: contract.TransitionID(checkpoint.Key, 1), Key: checkpoint.Key, Revision: 1, To: contract.CheckpointReady, SegmentID: checkpoint.SegmentID, CurrentNodeID: checkpoint.CurrentNodeID, OutcomeCode: "created"}
	// The store correctly rejects a malformed initial transition; create a ready
	// fact first and then atomically move it to executing to model a crash.
	checkpoint.Status, checkpoint.Attempt, checkpoint.AttemptID, checkpoint.Steps, checkpoint.Visits = contract.CheckpointReady, 0, "", 0, map[string]int{}
	transition.To = contract.CheckpointReady
	if _, _, err := store.Create(context.Background(), checkpoint, transition); err != nil {
		t.Fatal(err)
	}
	executing := checkpoint
	executing.Revision, executing.Status, executing.Attempt, executing.AttemptID, executing.Steps, executing.Visits = 2, contract.CheckpointExecuting, 1, "end:1:2", 1, map[string]int{"end": 1}
	start := contract.Transition{ID: contract.TransitionID(executing.Key, 2), Key: executing.Key, Revision: 2, From: contract.CheckpointReady, To: contract.CheckpointExecuting, SegmentID: executing.SegmentID, CurrentNodeID: executing.CurrentNodeID, AttemptID: executing.AttemptID, OutcomeCode: "node_started"}
	start.SourceSegmentID, start.SourceHostGeneration = checkpoint.SegmentID, checkpoint.HostGeneration
	start.SourceNodeID, start.SourceAttemptID = checkpoint.CurrentNodeID, checkpoint.AttemptID
	if _, _, err := store.CompareAndSwap(context.Background(), executing.Key, 1, executing, start); err != nil {
		t.Fatal(err)
	}
	if _, err := executor.Run(context.Background(), StartRequest{Key: testKey(), SegmentID: "segment-1", HostGeneration: 1}); !errors.Is(err, ErrSegmentMismatch) {
		t.Fatalf("old recovery owner error=%v", err)
	}
	result, err := executor.Run(context.Background(), StartRequest{Key: testKey(), SegmentID: "segment-2", HostGeneration: 2})
	if !errors.Is(err, ErrUnknownOutcome) || result.Checkpoint.Status != contract.CheckpointUnknown || recoveryLease.releases != 1 {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	transitions, err := store.ListTransitions(context.Background(), testKey(), 0, 8)
	if err != nil || len(transitions) != 3 || transitions[2].SourceSegmentID != "segment-1" || transitions[2].SourceHostGeneration != 1 {
		t.Fatalf("recovery handoff evidence=%#v err=%v", transitions, err)
	}
}

func TestExecutorUnknownRecoveryReturnsReleaseErrorWithCheckpoint(t *testing.T) {
	t.Parallel()
	definition := oneNodeDefinition(t, contract.RetryPolicy{MaxAttempts: 1})
	executor, store := testExecutor(t, definition, funcNode(func(context.Context, NodeRequest) (NodeResult, error) {
		t.Fatal("recovery must not invoke the interrupted node")
		return NodeResult{}, nil
	}))
	releaseErr := errors.New("lease release failed")
	lease := &recordingLease{releaseErr: releaseErr}
	executor.lease = lease
	seedExecutingCheckpoint(t, executor, store)
	result, err := executor.Run(context.Background(), StartRequest{Key: testKey(), SegmentID: "segment-2", HostGeneration: 2})
	if !errors.Is(err, ErrUnknownOutcome) || !errors.Is(err, ErrLeaseUnavailable) || !errors.Is(err, releaseErr) || result.Checkpoint.Status != contract.CheckpointUnknown || lease.releases != 1 {
		t.Fatalf("release failure result=%#v err=%v releases=%d", result, err, lease.releases)
	}
}

func TestExecutorFailsClosedOnBindingRevisionDrift(t *testing.T) {
	t.Parallel()
	definition := oneNodeDefinition(t, contract.RetryPolicy{MaxAttempts: 1})
	first, store := testExecutor(t, definition, funcNode(func(context.Context, NodeRequest) (NodeResult, error) {
		return NodeResult{}, &ApprovalPendingError{ApprovalID: "approval-1"}
	}))
	_, _ = first.Run(context.Background(), StartRequest{Key: testKey(), SegmentID: "segment-1", HostGeneration: 1, InitialState: initialState()})
	drift, err := NewBindings(definition, "composition-1", []NodeBinding{{Kind: "prompt", Version: "1", Revision: "node-v2", Node: funcNode(func(context.Context, NodeRequest) (NodeResult, error) { return NodeResult{}, nil })}}, BuiltinReducerBinding("builtin-v1"), nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewExecutorWithOptions(definition, drift, store, testOptions())
	if err != nil {
		t.Fatal(err)
	}
	_, err = second.Run(context.Background(), StartRequest{Key: testKey(), SegmentID: "segment-1", HostGeneration: 1})
	if !errors.Is(err, ErrBindingMismatch) {
		t.Fatalf("binding drift error=%v", err)
	}
}

func TestExecutorRejectsCheckpointContentOutsideFrozenDefinition(t *testing.T) {
	t.Parallel()
	definition := oneNodeDefinition(t, contract.RetryPolicy{MaxAttempts: 1})
	calls := 0
	executor, store := testExecutor(t, definition, funcNode(func(context.Context, NodeRequest) (NodeResult, error) {
		calls++
		return NodeResult{}, nil
	}))
	def := definition.Definition()
	checkpoint := contract.Checkpoint{
		Key:                    testKey(),
		SegmentID:              "segment-1",
		GraphID:                def.ID,
		DefinitionRevision:     definition.Revision(),
		CompositionRevision:    executor.bindings.CompositionRevision(),
		ImplementationRevision: executor.bindings.ImplementationRevision(),
		HostGeneration:         1,
		CurrentNodeID:          def.EntryNode,
		Revision:               1,
		State:                  contract.State{"account": json.RawMessage(`1`)},
		Visits:                 map[string]int{},
		Status:                 contract.CheckpointReady,
	}
	transition := contract.Transition{ID: contract.TransitionID(checkpoint.Key, 1), Key: checkpoint.Key, Revision: 1, To: contract.CheckpointReady, SegmentID: checkpoint.SegmentID, CurrentNodeID: checkpoint.CurrentNodeID, OutcomeCode: "created"}
	if _, _, err := store.Create(context.Background(), checkpoint, transition); err != nil {
		t.Fatal(err)
	}
	result, err := executor.Run(context.Background(), StartRequest{Key: testKey(), SegmentID: "segment-1", HostGeneration: 1})
	if !errors.Is(err, ErrBindingMismatch) || result.Checkpoint.Status != contract.CheckpointReady || calls != 0 {
		t.Fatalf("tampered checkpoint was not rejected before node execution: result=%#v err=%v calls=%d", result, err, calls)
	}
}

func TestExecutorRejectsCheckpointTopologyOutsideFrozenDefinition(t *testing.T) {
	t.Parallel()
	for name, mutate := range map[string]func(*contract.Checkpoint){
		"unknown current node": func(checkpoint *contract.Checkpoint) { checkpoint.CurrentNodeID = "not-in-definition" },
		"unknown visit node":   func(checkpoint *contract.Checkpoint) { checkpoint.Visits = map[string]int{"not-in-definition": 1} },
		"last edge target": func(checkpoint *contract.Checkpoint) {
			checkpoint.LastEdge = &contract.EdgeEvidence{From: "end", To: "start", Kind: contract.EdgeDefault}
		},
	} {
		t.Run(name, func(t *testing.T) {
			definition := oneNodeDefinition(t, contract.RetryPolicy{MaxAttempts: 1})
			calls := 0
			executor, store := testExecutor(t, definition, funcNode(func(context.Context, NodeRequest) (NodeResult, error) {
				calls++
				return NodeResult{}, nil
			}))
			def := definition.Definition()
			checkpoint := contract.Checkpoint{
				Key:                    testKey(),
				SegmentID:              "segment-1",
				GraphID:                def.ID,
				DefinitionRevision:     definition.Revision(),
				CompositionRevision:    executor.bindings.CompositionRevision(),
				ImplementationRevision: executor.bindings.ImplementationRevision(),
				HostGeneration:         1,
				CurrentNodeID:          def.EntryNode,
				Revision:               1,
				State:                  initialState(),
				Visits:                 map[string]int{},
				Status:                 contract.CheckpointReady,
			}
			mutate(&checkpoint)
			transition := contract.Transition{ID: contract.TransitionID(checkpoint.Key, 1), Key: checkpoint.Key, Revision: 1, To: contract.CheckpointReady, SegmentID: checkpoint.SegmentID, CurrentNodeID: checkpoint.CurrentNodeID, OutcomeCode: "created"}
			if _, _, err := store.Create(context.Background(), checkpoint, transition); err != nil {
				t.Fatal(err)
			}
			result, err := executor.Run(context.Background(), StartRequest{Key: testKey(), SegmentID: "segment-1", HostGeneration: 1})
			if !errors.Is(err, ErrBindingMismatch) || result.Checkpoint.Status != contract.CheckpointReady || calls != 0 {
				t.Fatalf("topology tampering was not rejected: result=%#v err=%v calls=%d", result, err, calls)
			}
		})
	}
}

func TestExecutorTimeoutAndCancellationCommitTerminalFacts(t *testing.T) {
	t.Parallel()
	timeoutDefinition := oneNodeDefinition(t, contract.RetryPolicy{MaxAttempts: 1})
	timeoutDef := timeoutDefinition.Definition()
	timeoutDef.Revision = ""
	timeoutDef.Nodes[0].Timeout = time.Millisecond
	timeoutDefinition = validateDefinition(t, timeoutDef)
	timedOut, _ := testExecutor(t, timeoutDefinition, funcNode(func(ctx context.Context, _ NodeRequest) (NodeResult, error) {
		<-ctx.Done()
		return NodeResult{}, ctx.Err()
	}))
	result, err := timedOut.Run(context.Background(), StartRequest{Key: testKey(), SegmentID: "segment-1", HostGeneration: 1, InitialState: initialState()})
	if !errors.Is(err, ErrExecutionFailed) || result.Checkpoint.FailureCode != "node_timeout" {
		t.Fatalf("timeout result=%#v err=%v", result, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancelled, _ := testExecutor(t, oneNodeDefinition(t, contract.RetryPolicy{MaxAttempts: 1}), funcNode(func(_ context.Context, _ NodeRequest) (NodeResult, error) {
		cancel()
		return NodeResult{}, nil
	}))
	result, err = cancelled.Run(ctx, StartRequest{Key: testKey(), SegmentID: "segment-1", HostGeneration: 1, InitialState: initialState()})
	if !errors.Is(err, context.Canceled) || result.Checkpoint.Status != contract.CheckpointCancelled {
		t.Fatalf("cancel result=%#v err=%v", result, err)
	}
}

func TestExecutorCustomReducerIsBoundAndSchemaValidated(t *testing.T) {
	t.Parallel()
	definition := baseDefinition([]contract.NodeSpec{testNode("end", true, contract.RetryPolicy{MaxAttempts: 1})}, nil)
	definition.State.Reducer, definition.State.ReducerVersion = "replace-account", "1"
	kinds, _ := contract.NewNodeKindRegistry([]contract.NodeKindMetadata{{ID: "prompt", Version: "1"}})
	reducers, _ := contract.NewReducerRegistry([]contract.ReducerMetadata{{ID: "replace-account", Version: "1"}})
	predicates, _ := contract.NewPredicateRegistry(nil)
	validated, err := contract.ValidateDefinition(definition, kinds, reducers, predicates)
	if err != nil {
		t.Fatal(err)
	}
	bindings, err := NewBindings(validated, "composition-1", []NodeBinding{{Kind: "prompt", Version: "1", Revision: "node-v1", Node: funcNode(func(context.Context, NodeRequest) (NodeResult, error) { return NodeResult{}, nil })}}, ReducerBinding{ID: "replace-account", Version: "1", Revision: "reducer-v1", Reducer: funcReducer(func(contract.State, contract.StatePatch) (contract.State, error) {
		return contract.State{"account": json.RawMessage(`"reduced"`)}, nil
	})}, nil)
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewExecutorWithOptions(validated, bindings, memory.NewDefault(), testOptions())
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.Run(context.Background(), StartRequest{Key: testKey(), SegmentID: "segment-1", HostGeneration: 1, InitialState: initialState()})
	if err != nil || string(result.Checkpoint.State["account"]) != `"reduced"` {
		t.Fatalf("custom reducer result=%#v err=%v", result, err)
	}
}

func TestExecutorConditionalEdgeUsesExactBoundPredicate(t *testing.T) {
	t.Parallel()
	definition := baseDefinition([]contract.NodeSpec{testNode("start", false, contract.RetryPolicy{MaxAttempts: 1}), testNode("yes", true, contract.RetryPolicy{MaxAttempts: 1}), testNode("no", true, contract.RetryPolicy{MaxAttempts: 1})}, []contract.EdgeSpec{{From: "start", To: "yes", Kind: contract.EdgeConditional, Predicate: "choose", PredicateVersion: "1"}, {From: "start", To: "no", Kind: contract.EdgeDefault}})
	kinds, _ := contract.NewNodeKindRegistry([]contract.NodeKindMetadata{{ID: "prompt", Version: "1"}})
	reducers, _ := contract.NewReducerRegistry(nil)
	predicates, _ := contract.NewPredicateRegistry([]contract.PredicateMetadata{{ID: "choose", Version: "1"}})
	validated, err := contract.ValidateDefinition(definition, kinds, reducers, predicates)
	if err != nil {
		t.Fatal(err)
	}
	bindings, err := NewBindings(validated, "composition-1", []NodeBinding{{Kind: "prompt", Version: "1", Revision: "node-v1", Node: funcNode(func(context.Context, NodeRequest) (NodeResult, error) { return NodeResult{}, nil })}}, BuiltinReducerBinding("builtin-v1"), []PredicateBinding{{ID: "choose", Version: "1", Revision: "predicate-v1", Predicate: funcPredicate(func(context.Context, contract.State) (bool, error) { return true, nil })}})
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewExecutorWithOptions(validated, bindings, memory.NewDefault(), testOptions())
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.Run(context.Background(), StartRequest{Key: testKey(), SegmentID: "segment-1", HostGeneration: 1, InitialState: initialState()})
	if err != nil || result.Checkpoint.CurrentNodeID != "yes" || result.Checkpoint.Status != contract.CheckpointCompleted {
		t.Fatalf("conditional result=%#v err=%v", result, err)
	}
}

func TestExecutorLeasePlannerAndSandboxFailClosed(t *testing.T) {
	t.Parallel()
	definition := oneNodeDefinition(t, contract.RetryPolicy{MaxAttempts: 1})
	calls := 0
	node := funcNode(func(_ context.Context, request NodeRequest) (NodeResult, error) {
		calls++
		if request.Context.PlanRevision == "" || string(request.State["account"]) != `"initial"` {
			t.Fatal("node did not receive bounded projected context")
		}
		return NodeResult{}, nil
	})
	bindings, err := NewBindings(definition, "composition-1", []NodeBinding{{Kind: "prompt", Version: "1", Revision: "node-v1", Node: node}}, BuiltinReducerBinding("builtin-v1"), nil)
	if err != nil {
		t.Fatal(err)
	}
	withoutLease, err := NewExecutor(definition, bindings, memory.NewDefault(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := withoutLease.Run(context.Background(), StartRequest{Key: testKey(), SegmentID: "segment-1", HostGeneration: 1, InitialState: initialState()}); !errors.Is(err, ErrLeaseUnavailable) || calls != 0 {
		t.Fatalf("lease fail closed err=%v calls=%d", err, calls)
	}
	deniedLease, err := NewExecutorWithOptions(definition, bindings, memory.NewDefault(), Options{Lease: rejectLease{}, Planner: testPlanner{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := deniedLease.Run(context.Background(), StartRequest{Key: testKey(), SegmentID: "segment-1", HostGeneration: 1, InitialState: initialState()}); !errors.Is(err, ErrLeaseUnavailable) || calls != 0 {
		t.Fatalf("stale/duplicate owner lease err=%v calls=%d", err, calls)
	}
	withoutPlanner, err := NewExecutorWithOptions(definition, bindings, memory.NewDefault(), Options{Lease: testLease{}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := withoutPlanner.Run(context.Background(), StartRequest{Key: testKey(), SegmentID: "segment-1", HostGeneration: 1, InitialState: initialState()})
	if !errors.Is(err, ErrExecutionFailed) || result.Checkpoint.FailureCode != "context_unavailable" || calls != 0 {
		t.Fatalf("planner fail closed result=%#v err=%v calls=%d", result, err, calls)
	}

	definitionValue := definition.Definition()
	definitionValue.Revision = ""
	definitionValue.Nodes[0].UsesSandbox = true
	sandboxDefinition := validateDefinition(t, definitionValue)
	sandboxBindings, err := NewBindings(sandboxDefinition, "composition-1", []NodeBinding{{Kind: "prompt", Version: "1", Revision: "node-v1", Node: node}}, BuiltinReducerBinding("builtin-v1"), nil)
	if err != nil {
		t.Fatal(err)
	}
	sandboxExecutor, err := NewExecutorWithOptions(sandboxDefinition, sandboxBindings, memory.NewDefault(), testOptions())
	if err != nil {
		t.Fatal(err)
	}
	result, err = sandboxExecutor.Run(context.Background(), StartRequest{Key: testKey(), SegmentID: "segment-1", HostGeneration: 1, InitialState: initialState()})
	if !errors.Is(err, ErrExecutionFailed) || result.Checkpoint.FailureCode != "sandbox_unavailable" || calls != 0 {
		t.Fatalf("sandbox fail closed result=%#v err=%v calls=%d", result, err, calls)
	}
}

func TestExecutorReadyPreflightFailureUsesLegalTwoStepTransition(t *testing.T) {
	t.Parallel()
	definition := oneNodeDefinition(t, contract.RetryPolicy{MaxAttempts: 1})
	executor, store := testExecutor(t, definition, funcNode(func(context.Context, NodeRequest) (NodeResult, error) {
		t.Fatal("node must not run after step preflight failure")
		return NodeResult{}, nil
	}))
	def := definition.Definition()
	checkpoint := contract.Checkpoint{Key: testKey(), SegmentID: "segment-1", GraphID: def.ID, DefinitionRevision: definition.Revision(), CompositionRevision: executor.bindings.CompositionRevision(), ImplementationRevision: executor.bindings.ImplementationRevision(), HostGeneration: 1, CurrentNodeID: def.EntryNode, Steps: def.Limits.MaxSteps, Revision: 1, State: initialState(), Visits: map[string]int{}, Status: contract.CheckpointReady}
	created := contract.Transition{ID: contract.TransitionID(checkpoint.Key, checkpoint.Revision), Key: checkpoint.Key, Revision: checkpoint.Revision, To: contract.CheckpointReady, SegmentID: checkpoint.SegmentID, CurrentNodeID: checkpoint.CurrentNodeID, OutcomeCode: "created"}
	if _, _, err := store.Create(context.Background(), checkpoint, created); err != nil {
		t.Fatal(err)
	}
	result, err := executor.Run(context.Background(), StartRequest{Key: testKey(), SegmentID: "segment-1", HostGeneration: 1})
	if !errors.Is(err, ErrExecutionFailed) || result.Checkpoint.Status != contract.CheckpointFailed || result.Checkpoint.FailureCode != "step_limit" {
		t.Fatalf("preflight result=%#v err=%v", result, err)
	}
	transitions, err := store.ListTransitions(context.Background(), testKey(), 0, 8)
	if err != nil || len(transitions) != 3 || transitions[1].From != contract.CheckpointReady || transitions[1].To != contract.CheckpointExecuting || transitions[2].From != contract.CheckpointExecuting || transitions[2].To != contract.CheckpointFailed {
		t.Fatalf("preflight transitions=%#v err=%v", transitions, err)
	}
}

func TestExecutorVerifiesExistingReadyLeaseBeforeCAS(t *testing.T) {
	t.Parallel()
	definition := oneNodeDefinition(t, contract.RetryPolicy{MaxAttempts: 1})
	store := memory.NewDefault()
	bindings, err := NewBindings(definition, "composition-1", []NodeBinding{{Kind: "prompt", Version: "1", Revision: "node-v1", Node: funcNode(func(context.Context, NodeRequest) (NodeResult, error) {
		t.Fatal("node should not run")
		return NodeResult{}, nil
	})}}, BuiltinReducerBinding("builtin-v1"), nil)
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewExecutorWithOptions(definition, bindings, store, Options{Lease: rejectLease{}, Planner: testPlanner{}})
	if err != nil {
		t.Fatal(err)
	}
	def := definition.Definition()
	checkpoint := contract.Checkpoint{Key: testKey(), SegmentID: "segment-1", GraphID: def.ID, DefinitionRevision: definition.Revision(), CompositionRevision: "composition-1", ImplementationRevision: bindings.ImplementationRevision(), HostGeneration: 1, CurrentNodeID: def.EntryNode, Revision: 1, State: initialState(), Visits: map[string]int{}, Status: contract.CheckpointReady}
	created := contract.Transition{ID: contract.TransitionID(checkpoint.Key, checkpoint.Revision), Key: checkpoint.Key, Revision: checkpoint.Revision, To: contract.CheckpointReady, SegmentID: checkpoint.SegmentID, CurrentNodeID: checkpoint.CurrentNodeID, OutcomeCode: "created"}
	if _, _, err := store.Create(context.Background(), checkpoint, created); err != nil {
		t.Fatal(err)
	}
	if _, err := executor.Run(context.Background(), StartRequest{Key: testKey(), SegmentID: "segment-1", HostGeneration: 1}); !errors.Is(err, ErrLeaseUnavailable) {
		t.Fatalf("ready lease error=%v", err)
	}
	transitions, err := store.ListTransitions(context.Background(), testKey(), 0, 8)
	if err != nil || len(transitions) != 1 {
		t.Fatalf("stale ready caller mutated checkpoint transitions=%#v err=%v", transitions, err)
	}
}

func TestExecutorLeaseLossAfterNodeReturnsKeepsExecutingCheckpoint(t *testing.T) {
	t.Parallel()
	definition := oneNodeDefinition(t, contract.RetryPolicy{MaxAttempts: 1})
	executor, store := testExecutor(t, definition, funcNode(func(context.Context, NodeRequest) (NodeResult, error) {
		return NodeResult{}, nil
	}))
	// The first four checks authorize Run/Create, node_started, and node
	// execution. Revoke on the fifth check, immediately after node return.
	executor.lease = &sequencedLease{rejectAt: 5}
	result, err := executor.Run(context.Background(), StartRequest{Key: testKey(), SegmentID: "segment-1", HostGeneration: 1, InitialState: initialState()})
	if !errors.Is(err, ErrLeaseUnavailable) || result.Checkpoint.Status != contract.CheckpointExecuting || result.Checkpoint.Revision != 2 {
		t.Fatalf("post-node lease loss result=%#v err=%v", result, err)
	}
	checkpoint, err := store.Load(context.Background(), testKey())
	if err != nil || checkpoint.Status != contract.CheckpointExecuting || checkpoint.Revision != 2 || checkpoint.FailureCode != "" {
		t.Fatalf("post-node lease loss mutated durable checkpoint=%#v err=%v", checkpoint, err)
	}
	transitions, err := store.ListTransitions(context.Background(), testKey(), 0, 8)
	if err != nil || len(transitions) != 2 || transitions[1].OutcomeCode != string(OutcomeNodeStarted) {
		t.Fatalf("post-node lease loss wrote synthetic transition=%#v err=%v", transitions, err)
	}
}

func TestExecutorLeaseLossImmediatelyBeforeCommitKeepsExecutingCheckpoint(t *testing.T) {
	t.Parallel()
	definition := oneNodeDefinition(t, contract.RetryPolicy{MaxAttempts: 1})
	executor, store := testExecutor(t, definition, funcNode(func(context.Context, NodeRequest) (NodeResult, error) {
		return NodeResult{}, nil
	}))
	// The fifth check is the post-node fence and succeeds. The sixth is the
	// commitWithEvidence fence immediately before the completed CAS.
	executor.lease = &sequencedLease{rejectAt: 6}
	result, err := executor.Run(context.Background(), StartRequest{Key: testKey(), SegmentID: "segment-1", HostGeneration: 1, InitialState: initialState()})
	if !errors.Is(err, ErrLeaseUnavailable) || result.Checkpoint.Status != contract.CheckpointExecuting || result.Checkpoint.Revision != 2 {
		t.Fatalf("pre-commit lease loss result=%#v err=%v", result, err)
	}
	checkpoint, err := store.Load(context.Background(), testKey())
	if err != nil || checkpoint.Status != contract.CheckpointExecuting || checkpoint.Revision != 2 || checkpoint.FailureCode != "" {
		t.Fatalf("pre-commit lease loss mutated durable checkpoint=%#v err=%v", checkpoint, err)
	}
	transitions, err := store.ListTransitions(context.Background(), testKey(), 0, 8)
	if err != nil || len(transitions) != 2 || transitions[1].OutcomeCode != string(OutcomeNodeStarted) {
		t.Fatalf("pre-commit lease loss wrote completed transition=%#v err=%v", transitions, err)
	}
}

func TestExecutorCancellationUsesDurableLeaseFence(t *testing.T) {
	t.Parallel()
	definition := oneNodeDefinition(t, contract.RetryPolicy{MaxAttempts: 1})
	ctx, cancel := context.WithCancel(context.Background())
	executor, _ := testExecutor(t, definition, funcNode(func(context.Context, NodeRequest) (NodeResult, error) {
		cancel()
		return NodeResult{}, nil
	}))
	// A lease implementation may reject a canceled caller context. Cancellation
	// must still use e.cancel's detached durable context for its final fence.
	executor.lease = contextSensitiveLease{}
	result, err := executor.Run(ctx, StartRequest{Key: testKey(), SegmentID: "segment-1", HostGeneration: 1, InitialState: initialState()})
	if !errors.Is(err, context.Canceled) || result.Checkpoint.Status != contract.CheckpointCancelled {
		t.Fatalf("cancellation result=%#v err=%v", result, err)
	}
}

func TestExecutorApprovalReleasesSegmentLease(t *testing.T) {
	t.Parallel()
	definition := oneNodeDefinition(t, contract.RetryPolicy{MaxAttempts: 1})
	lease := &recordingLease{}
	bindings, err := NewBindings(definition, "composition-1", []NodeBinding{{Kind: "prompt", Version: "1", Revision: "node-v1", Node: funcNode(func(context.Context, NodeRequest) (NodeResult, error) {
		return NodeResult{}, &ApprovalPendingError{ApprovalID: "approval-1"}
	})}}, BuiltinReducerBinding("builtin-v1"), nil)
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewExecutorWithOptions(definition, bindings, memory.NewDefault(), Options{Lease: lease, Planner: testPlanner{}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.Run(context.Background(), StartRequest{Key: testKey(), SegmentID: "segment-1", HostGeneration: 1, InitialState: initialState()})
	if !errors.Is(err, ErrApprovalPending) || !result.Suspended || lease.releases != 1 {
		t.Fatalf("approval result=%#v err=%v releases=%d", result, err, lease.releases)
	}
}

func TestExecutorInvalidApprovalIdentityBecomesTerminalFailure(t *testing.T) {
	t.Parallel()
	definition := oneNodeDefinition(t, contract.RetryPolicy{MaxAttempts: 1})
	executor, _ := testExecutor(t, definition, funcNode(func(context.Context, NodeRequest) (NodeResult, error) {
		return NodeResult{}, &ApprovalPendingError{ApprovalID: "bad\x01approval"}
	}))
	result, err := executor.Run(context.Background(), StartRequest{Key: testKey(), SegmentID: "segment-1", HostGeneration: 1, InitialState: initialState()})
	if !errors.Is(err, ErrExecutionFailed) || result.Checkpoint.Status != contract.CheckpointFailed || result.Checkpoint.FailureCode != "invalid_approval" {
		t.Fatalf("invalid approval did not become terminal failure: result=%#v err=%v", result, err)
	}
}

func TestDeniedApprovalReleasesNewLeaseAndEmitsResolution(t *testing.T) {
	t.Parallel()
	definition := oneNodeDefinition(t, contract.RetryPolicy{MaxAttempts: 1})
	lease, observer := &recordingLease{}, &recordingObserver{}
	bindings, err := NewBindings(definition, "composition-1", []NodeBinding{{Kind: "prompt", Version: "1", Revision: "node-v1", Node: funcNode(func(context.Context, NodeRequest) (NodeResult, error) {
		return NodeResult{}, &ApprovalPendingError{ApprovalID: "approval-1"}
	})}}, BuiltinReducerBinding("builtin-v1"), nil)
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewExecutorWithOptions(definition, bindings, memory.NewDefault(), Options{Lease: lease, Planner: testPlanner{}, Observer: observer, Approval: testApprovalAuthorizer{}})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = executor.Run(context.Background(), StartRequest{Key: testKey(), SegmentID: "segment-1", HostGeneration: 1, InitialState: initialState()})
	result, err := executor.Resume(context.Background(), ResumeRequest{Key: testKey(), SegmentID: "segment-2", HostGeneration: 2, ApprovalID: "approval-1", Approved: false, Decision: approvalDecision(testKey(), "approval-1", contract.CheckpointWaitingApproval, 3, "segment-1", false)})
	if !errors.Is(err, ErrExecutionFailed) || result.Checkpoint.Status != contract.CheckpointFailed || lease.releases != 2 || !observer.has(EventApprovalResolved) || !observer.has(EventFailed) {
		t.Fatalf("denied result=%#v err=%v releases=%d events=%v", result, err, lease.releases, observer.events)
	}
	transitions, err := executor.store.ListTransitions(context.Background(), testKey(), 0, 8)
	if err != nil || transitions[len(transitions)-1].Approval == nil || transitions[len(transitions)-1].Approval.ActorID != "operator-1" || transitions[len(transitions)-1].Approval.AuthorizationBasis != "test-policy" {
		t.Fatalf("denial evidence was not persisted: transitions=%#v err=%v", transitions, err)
	}
}

func TestExecutorResumeRequiresAuthorizedDecisionEvidence(t *testing.T) {
	t.Parallel()
	definition := oneNodeDefinition(t, contract.RetryPolicy{MaxAttempts: 1})
	executor, _ := testExecutor(t, definition, funcNode(func(context.Context, NodeRequest) (NodeResult, error) {
		return NodeResult{}, &ApprovalPendingError{ApprovalID: "approval-1"}
	}))
	_, err := executor.Run(context.Background(), StartRequest{Key: testKey(), SegmentID: "segment-1", HostGeneration: 1, InitialState: initialState()})
	if !errors.Is(err, ErrApprovalPending) {
		t.Fatal(err)
	}
	bare := ResumeRequest{Key: testKey(), SegmentID: "segment-2", HostGeneration: 2, ApprovalID: "approval-1", Approved: true}
	result, err := executor.Resume(context.Background(), bare)
	if !errors.Is(err, ErrApprovalUnavailable) || result.Checkpoint.Revision != 3 || result.Checkpoint.Status != contract.CheckpointWaitingApproval {
		t.Fatalf("bare approval was accepted: result=%#v err=%v", result, err)
	}
	decision := approvalDecision(testKey(), "approval-1", contract.CheckpointWaitingApproval, 3, "segment-1", true)
	executor.approval = testApprovalAuthorizer{}
	decision.Revision = 2
	result, err = executor.Resume(context.Background(), ResumeRequest{Key: testKey(), SegmentID: "segment-2", HostGeneration: 2, ApprovalID: "approval-1", Decision: decision})
	if !errors.Is(err, ErrInvalidApproval) || result.Checkpoint.Revision != 3 {
		t.Fatalf("stale approval mutated checkpoint: result=%#v err=%v", result, err)
	}
}

func TestApprovalDecisionRejectsFormatControlCharacters(t *testing.T) {
	key := testKey()
	base := approvalDecision(key, "approval-1", contract.CheckpointWaitingApproval, 3, "segment-1", true)
	for name, mutate := range map[string]func(*ApprovalDecision){
		"zero-width-space": func(value *ApprovalDecision) { value.ActorID = "operator\u200b" },
		"invalid-utf8": func(value *ApprovalDecision) {
			value.AuthorizationBasis = string([]byte{'p', 'o', 'l', 'i', 'c', 'y', 0xed, 0xa0, 0x80})
		},
		"private-use": func(value *ApprovalDecision) { value.AuthorizationBasis = "policy\ue000" },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := base
			mutate(&candidate)
			if err := candidate.Validate(); !errors.Is(err, ErrInvalidApproval) {
				t.Fatalf("format control was accepted: %v", err)
			}
		})
	}
}

func TestExecutorApprovalAuthorizerPanicDoesNotMutate(t *testing.T) {
	t.Parallel()
	definition := oneNodeDefinition(t, contract.RetryPolicy{MaxAttempts: 1})
	executor, _ := testExecutor(t, definition, funcNode(func(context.Context, NodeRequest) (NodeResult, error) {
		return NodeResult{}, &ApprovalPendingError{ApprovalID: "approval-1"}
	}))
	_, _ = executor.Run(context.Background(), StartRequest{Key: testKey(), SegmentID: "segment-1", HostGeneration: 1, InitialState: initialState()})
	executor.approval = panicApprovalAuthorizer{}
	decision := approvalDecision(testKey(), "approval-1", contract.CheckpointWaitingApproval, 3, "segment-1", true)
	result, err := executor.Resume(context.Background(), ResumeRequest{Key: testKey(), SegmentID: "segment-2", HostGeneration: 2, ApprovalID: "approval-1", Decision: decision})
	if !errors.Is(err, ErrApprovalPanic) || result.Checkpoint.Revision != 3 || result.Checkpoint.Status != contract.CheckpointWaitingApproval {
		t.Fatalf("authorizer panic mutated checkpoint: result=%#v err=%v", result, err)
	}
}

func TestExecutorNodePanicRequiresUnknownRecoveryAndNeverRetries(t *testing.T) {
	t.Parallel()
	definition := oneNodeDefinition(t, contract.RetryPolicy{MaxAttempts: 3})
	var calls atomic.Int32
	executor, store := testExecutor(t, definition, funcNode(func(context.Context, NodeRequest) (NodeResult, error) {
		calls.Add(1)
		panic("node secret must not escape")
	}))
	first, err := executor.Run(context.Background(), StartRequest{Key: testKey(), SegmentID: "segment-1", HostGeneration: 1, InitialState: initialState()})
	if !errors.Is(err, ErrNodePanic) || first.Checkpoint.Status != contract.CheckpointExecuting || calls.Load() != 1 {
		t.Fatalf("node panic was not preserved as unknown outcome: result=%#v err=%v calls=%d", first, err, calls.Load())
	}
	if _, err := executor.Run(context.Background(), StartRequest{Key: testKey(), SegmentID: "segment-1", HostGeneration: 1}); !errors.Is(err, ErrSegmentMismatch) {
		t.Fatalf("same owner was allowed to retry panicked node: %v", err)
	}
	recovered, err := executor.Run(context.Background(), StartRequest{Key: testKey(), SegmentID: "segment-2", HostGeneration: 2})
	if !errors.Is(err, ErrUnknownOutcome) || recovered.Checkpoint.Status != contract.CheckpointUnknown || calls.Load() != 1 {
		t.Fatalf("recovery did not close node panic as unknown: result=%#v err=%v calls=%d", recovered, err, calls.Load())
	}
	transitions, err := store.ListTransitions(context.Background(), testKey(), 0, 8)
	if err != nil || len(transitions) != 3 || transitions[2].OutcomeCode != "recovery_unknown" {
		t.Fatalf("unexpected node panic transition history: %#v err=%v", transitions, err)
	}
}

func TestGraphPluginPanicsAreContained(t *testing.T) {
	t.Parallel()
	if _, err := safeNodeExecute(panicNode{}, context.Background(), NodeRequest{}); !errors.Is(err, ErrNodePanic) {
		t.Fatalf("node panic=%v", err)
	}
	if _, err := safeReduce(panicReducer{}, nil, contract.StatePatch{}); !errors.Is(err, ErrReducerPanic) {
		t.Fatalf("reducer panic=%v", err)
	}
	if _, err := safeEvaluate(panicPredicate{}, context.Background(), nil); !errors.Is(err, ErrPredicatePanic) {
		t.Fatalf("predicate panic=%v", err)
	}
	if _, err := safePlan(panicPlanner{}, context.Background(), ContextRequest{}); !errors.Is(err, ErrPlannerPanic) {
		t.Fatalf("planner panic=%v", err)
	}
	if err := safeSandboxAuthorize(panicSandbox{}, context.Background(), SandboxRequest{}); !errors.Is(err, ErrSandboxPanic) {
		t.Fatalf("sandbox panic=%v", err)
	}
	if err := safeLeaseVerify(panicLease{}, context.Background(), LeaseRequest{}); !errors.Is(err, ErrLeasePanic) {
		t.Fatalf("lease verify panic=%v", err)
	}
	if err := safeLeaseRelease(panicLease{}, context.Background(), LeaseRequest{}); !errors.Is(err, ErrLeasePanic) {
		t.Fatalf("lease release panic=%v", err)
	}
	if err := safeObserve(panicObserver{}, context.Background(), Event{}); !errors.Is(err, ErrObserverPanic) {
		t.Fatalf("observer panic=%v", err)
	}
}

func TestObserverReceivesShortBoundedDeadline(t *testing.T) {
	t.Parallel()
	observer := &deadlineObserver{}
	executor := &Executor{observer: observer}
	executor.emit(context.Background(), Event{Kind: EventStarted})
	if !observer.seen || observer.remaining <= 0 || observer.remaining > observerTimeout {
		t.Fatalf("observer deadline seen=%v remaining=%s", observer.seen, observer.remaining)
	}
}

func TestStableAttemptIDIsBoundedForLargestNodeID(t *testing.T) {
	key := testKey()
	largeNode := strings.Repeat("n", contract.MaxNodeIDBytes)
	if id := stableAttemptID(key, largeNode, 1, 2); len(id) > contract.MaxCheckpointIDBytes || id == stableAttemptID(key, largeNode, 2, 2) {
		t.Fatalf("stable attempt id is not bounded/distinct: %q", id)
	}
}

type funcNode func(context.Context, NodeRequest) (NodeResult, error)

func (f funcNode) Execute(ctx context.Context, request NodeRequest) (NodeResult, error) {
	return f(ctx, request)
}

type retryError struct{}

func (retryError) Error() string   { return "retry" }
func (retryError) Retryable() bool { return true }

type funcReducer func(contract.State, contract.StatePatch) (contract.State, error)

func (f funcReducer) Reduce(state contract.State, patch contract.StatePatch) (contract.State, error) {
	return f(state, patch)
}

type funcPredicate func(context.Context, contract.State) (bool, error)

func (f funcPredicate) Evaluate(ctx context.Context, state contract.State) (bool, error) {
	return f(ctx, state)
}

type casBarrierStore struct {
	*memory.Store
	arrived chan<- struct{}
	release <-chan struct{}
	count   atomic.Int32
}

func (s *casBarrierStore) CompareAndSwap(ctx context.Context, key contract.CheckpointKey, expectedRevision uint64, next contract.Checkpoint, transition contract.Transition) (contract.Checkpoint, contract.CommitDisposition, error) {
	if transition.OutcomeCode == "node_started" && s.count.Add(1) <= 2 {
		s.arrived <- struct{}{}
		<-s.release
	}
	return s.Store.CompareAndSwap(ctx, key, expectedRevision, next, transition)
}

func testExecutor(t *testing.T, definition *contract.ValidatedDefinition, node Node) (*Executor, *memory.Store) {
	t.Helper()
	store := memory.NewDefault()
	bindings, err := NewBindings(definition, "composition-1", []NodeBinding{{Kind: "prompt", Version: "1", Revision: "node-v1", Node: node}}, BuiltinReducerBinding("builtin-v1"), nil)
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewExecutorWithOptions(definition, bindings, store, testOptions())
	if err != nil {
		t.Fatal(err)
	}
	return executor, store
}

func threeNodeDefinition(t *testing.T) *contract.ValidatedDefinition {
	t.Helper()
	definition := baseDefinition([]contract.NodeSpec{testNode("start", false, contract.RetryPolicy{MaxAttempts: 1}), testNode("middle", false, contract.RetryPolicy{MaxAttempts: 1}), testNode("end", true, contract.RetryPolicy{MaxAttempts: 1})}, []contract.EdgeSpec{{From: "start", To: "middle", Kind: contract.EdgeDefault}, {From: "middle", To: "end", Kind: contract.EdgeDefault}})
	return validateDefinition(t, definition)
}
func oneNodeDefinition(t *testing.T, retry contract.RetryPolicy) *contract.ValidatedDefinition {
	t.Helper()
	return validateDefinition(t, baseDefinition([]contract.NodeSpec{testNode("end", true, retry)}, nil))
}
func baseDefinition(nodes []contract.NodeSpec, edges []contract.EdgeSpec) contract.Definition {
	return contract.Definition{ID: "graph-1", Version: "1", EntryNode: nodes[0].ID, Nodes: nodes, Edges: edges, State: contract.StateSchema{Reducer: contract.ReducerTopLevelJSONPatch, ReducerVersion: "1", Fields: []contract.StateField{{Name: "account", Type: contract.StateString, Required: true, Sensitive: true, MaxBytes: 128}}}, Limits: contract.Limits{MaxSteps: 16}, Redaction: contract.RedactionSchema{StateFields: []string{"account"}}}
}
func testNode(id string, terminal bool, retry contract.RetryPolicy) contract.NodeSpec {
	return contract.NodeSpec{ID: id, Kind: "prompt", KindVersion: "1", ContextView: contract.ContextView{StateFields: []string{"account"}, Layers: []contract.LayerKind{contract.LayerCurrentInput, contract.LayerRequiredState}}, ContextBudget: contract.ContextBudget{TotalTokens: 16, LayerBudgets: map[contract.LayerKind]int64{contract.LayerCurrentInput: 8, contract.LayerRequiredState: 8}}, Retry: retry, Timeout: time.Second, Terminal: terminal}
}
func validateDefinition(t *testing.T, definition contract.Definition) *contract.ValidatedDefinition {
	t.Helper()
	kinds, err := contract.NewNodeKindRegistry([]contract.NodeKindMetadata{{ID: "prompt", Version: "1"}})
	if err != nil {
		t.Fatal(err)
	}
	reducers, err := contract.NewReducerRegistry(nil)
	if err != nil {
		t.Fatal(err)
	}
	predicates, err := contract.NewPredicateRegistry(nil)
	if err != nil {
		t.Fatal(err)
	}
	validated, err := contract.ValidateDefinition(definition, kinds, reducers, predicates)
	if err != nil {
		t.Fatal(err)
	}
	return validated
}
func initialState() contract.State { return contract.State{"account": json.RawMessage(`"initial"`)} }
func testKey() contract.CheckpointKey {
	return contract.CheckpointKey{TenantID: "tenant", SessionID: "session", RunID: "run"}
}

type testLease struct{}

func (testLease) Verify(context.Context, LeaseRequest) error  { return nil }
func (testLease) Release(context.Context, LeaseRequest) error { return nil }

type contextSensitiveLease struct{}

func (contextSensitiveLease) Verify(ctx context.Context, _ LeaseRequest) error { return ctx.Err() }
func (contextSensitiveLease) Release(context.Context, LeaseRequest) error      { return nil }

type recordingLease struct {
	mu         sync.Mutex
	releases   int
	releaseErr error
}

type sequencedLease struct {
	mu       sync.Mutex
	verifies int
	rejectAt int
}

func (l *sequencedLease) Verify(context.Context, LeaseRequest) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.verifies++
	if l.rejectAt > 0 && l.verifies >= l.rejectAt {
		return errors.New("lease revoked")
	}
	return nil
}

func (*sequencedLease) Release(context.Context, LeaseRequest) error { return nil }

func (*recordingLease) Verify(context.Context, LeaseRequest) error { return nil }
func (l *recordingLease) Release(context.Context, LeaseRequest) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.releases++
	return l.releaseErr
}

type recordingObserver struct {
	mu     sync.Mutex
	events []Event
}

func (o *recordingObserver) Observe(_ context.Context, event Event) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.events = append(o.events, event)
	return nil
}
func (o *recordingObserver) has(kind EventKind) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, event := range o.events {
		if event.Kind == kind {
			return true
		}
	}
	return false
}

type deadlineObserver struct {
	seen      bool
	remaining time.Duration
}

func (o *deadlineObserver) Observe(ctx context.Context, _ Event) error {
	deadline, ok := ctx.Deadline()
	if !ok {
		return errors.New("missing observer deadline")
	}
	o.seen, o.remaining = true, time.Until(deadline)
	return nil
}

type rejectLease struct{}

func (rejectLease) Verify(context.Context, LeaseRequest) error {
	return errors.New("not current owner")
}
func (rejectLease) Release(context.Context, LeaseRequest) error { return nil }

type testApprovalAuthorizer struct{}

func (testApprovalAuthorizer) Authorize(_ context.Context, request ApprovalAuthorizationRequest) error {
	if request.Key != request.Decision.Key || request.PendingApprovalID != request.Decision.ApprovalID || request.CurrentRevision != request.Decision.Revision || request.Decision.SourceSegmentID == request.SegmentID {
		return errors.New("invalid approval authorization request")
	}
	return nil
}

type panicNode struct{}

func (panicNode) Execute(context.Context, NodeRequest) (NodeResult, error) { panic("node") }

type panicReducer struct{}

func (panicReducer) Reduce(contract.State, contract.StatePatch) (contract.State, error) {
	panic("reducer")
}

type panicPredicate struct{}

func (panicPredicate) Evaluate(context.Context, contract.State) (bool, error) { panic("predicate") }

type panicPlanner struct{}

func (panicPlanner) Plan(context.Context, ContextRequest) (contract.ContextPlan, error) {
	panic("planner")
}

type panicSandbox struct{}

func (panicSandbox) Authorize(context.Context, SandboxRequest) error { panic("sandbox") }

type panicLease struct{}

func (panicLease) Verify(context.Context, LeaseRequest) error  { panic("lease verify") }
func (panicLease) Release(context.Context, LeaseRequest) error { panic("lease release") }

type panicObserver struct{}

func (panicObserver) Observe(context.Context, Event) error { panic("observer") }

type panicApprovalAuthorizer struct{}

func (panicApprovalAuthorizer) Authorize(context.Context, ApprovalAuthorizationRequest) error {
	panic("approval authorizer")
}

func approvalDecision(key contract.CheckpointKey, approvalID string, _ contract.CheckpointStatus, revision uint64, sourceSegment string, approved bool) ApprovalDecision {
	decision := ApprovalDecisionDenied
	if approved {
		decision = ApprovalDecisionApproved
	}
	return ApprovalDecision{TenantID: key.TenantID, Key: key, ApprovalID: approvalID, Decision: decision, Revision: revision, SourceSegmentID: sourceSegment, ActorID: "operator-1", AuthorizationBasis: "test-policy"}
}

type testPlanner struct{}

func (testPlanner) Plan(_ context.Context, request ContextRequest) (contract.ContextPlan, error) {
	sources := []contract.ContextSource{{Layer: contract.LayerCurrentInput, SourceID: "input", Revision: "1", EstimatedTokens: 1, Required: true}}
	for _, layer := range request.View.Layers {
		if layer == contract.LayerRequiredState {
			sources = append(sources, contract.ContextSource{Layer: layer, SourceID: "state", Revision: "1", EstimatedTokens: 1, Required: true})
		}
	}
	return contract.ContextPlan{DefinitionRevision: request.DefinitionRevision, PlanRevision: "plan-1", View: request.View.Clone(), Budget: request.Budget.Clone(), Sources: sources}, nil
}

func testOptions() Options { return Options{Lease: testLease{}, Planner: testPlanner{}} }

func seedExecutingCheckpoint(t *testing.T, executor *Executor, store *memory.Store) {
	t.Helper()
	def := executor.definition.Definition()
	checkpoint := contract.Checkpoint{
		Key: testKey(), SegmentID: "segment-1", GraphID: def.ID,
		DefinitionRevision: executor.definition.Revision(), CompositionRevision: executor.bindings.CompositionRevision(),
		ImplementationRevision: executor.bindings.ImplementationRevision(), HostGeneration: 1,
		CurrentNodeID: def.EntryNode, Revision: 1, State: initialState(), Visits: map[string]int{}, Status: contract.CheckpointReady,
	}
	created := contract.Transition{ID: contract.TransitionID(checkpoint.Key, 1), Key: checkpoint.Key, Revision: 1, To: contract.CheckpointReady, SegmentID: checkpoint.SegmentID, CurrentNodeID: checkpoint.CurrentNodeID, OutcomeCode: "created"}
	if _, _, err := store.Create(context.Background(), checkpoint, created); err != nil {
		t.Fatal(err)
	}
	executing := checkpoint
	executing.Revision, executing.Status, executing.Attempt, executing.AttemptID, executing.Steps, executing.Visits = 2, contract.CheckpointExecuting, 1, "end:1:2", 1, map[string]int{def.EntryNode: 1}
	start := contract.Transition{ID: contract.TransitionID(executing.Key, 2), Key: executing.Key, Revision: 2, From: contract.CheckpointReady, To: contract.CheckpointExecuting, SegmentID: executing.SegmentID, CurrentNodeID: executing.CurrentNodeID, AttemptID: executing.AttemptID, OutcomeCode: "node_started", SourceSegmentID: checkpoint.SegmentID, SourceHostGeneration: checkpoint.HostGeneration, SourceNodeID: checkpoint.CurrentNodeID, SourceAttemptID: checkpoint.AttemptID}
	if _, _, err := store.CompareAndSwap(context.Background(), executing.Key, 1, executing, start); err != nil {
		t.Fatal(err)
	}
}
