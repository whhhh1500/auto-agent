// Package graph adapts the bounded execution/graph engine to the application
// run-executor seam. It is intentionally dependency-injected: no globals,
// registries, goroutines, or persistence implementations are owned here.
package graph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	runexecutor "github.com/cc-auto-agent/harness-core/pkg/app/runexecutor"
	core "github.com/cc-auto-agent/harness-core/pkg/core"
	execgraph "github.com/cc-auto-agent/harness-core/pkg/execution/graph"
	contract "github.com/cc-auto-agent/harness-core/pkg/extensions/graph"
)

const (
	ID                     = "graph-core-turn"
	Version                = "1"
	ImplementationRevision = "graph-core-turn-v1"
	statusField            = "status"
)

var (
	ErrInvalidOptions              = errors.New("invalid graph run executor options")
	ErrSegmentUnavailable          = errors.New("graph run segment authority unavailable")
	ErrApprovalResolverUnavailable = errors.New("graph approval decision resolver unavailable")
	ErrCallbackPanic               = errors.New("graph adapter callback panicked")
)

// SegmentGrant proves both the lease identity and host generation used by the
// graph engine. A checkpoint revision is never accepted as a lease proof.
type SegmentGrant struct {
	SegmentID      string
	HostGeneration uint64
	Lease          execgraph.SegmentLease
}

type SegmentAuthority interface {
	Acquire(context.Context, contract.CheckpointKey) (SegmentGrant, error)
}

type ApprovalDecisionResolver interface {
	Resolve(context.Context, core.Principal, contract.CheckpointKey, string) (execgraph.ApprovalDecision, error)
}

// Options are immutable after NewRegistration. All graph/runtime dependencies
// are supplied by the embedding composition root.
type Options struct {
	Definition *contract.ValidatedDefinition
	Store      contract.Store
	Authority  SegmentAuthority
	Planner    execgraph.ContextPlanner
	Observer   execgraph.Observer
	Sandbox    execgraph.SandboxAuthorizer
	Approval   execgraph.ApprovalAuthorizer
	Decisions  ApprovalDecisionResolver
}

// NewDefaultDefinition returns the bounded built-in one-node graph. This is
// the only definition accepted by this registration; arbitrary Graph
// definitions require a separate adapter registration.
func NewDefaultDefinition() (*contract.ValidatedDefinition, error) {
	definition := contract.Definition{
		ID: ID, Version: Version, EntryNode: "turn",
		Nodes: []contract.NodeSpec{{ID: "turn", Kind: "core-turn", KindVersion: "1", Terminal: true, Timeout: 5 * time.Minute,
			ContextView:   contract.ContextView{StateFields: []string{statusField}, Layers: []contract.LayerKind{contract.LayerCurrentInput, contract.LayerRequiredState}},
			ContextBudget: contract.ContextBudget{TotalTokens: 32, LayerBudgets: map[contract.LayerKind]int64{contract.LayerCurrentInput: 16, contract.LayerRequiredState: 16}},
		}},
		State:  contract.StateSchema{Reducer: contract.ReducerTopLevelJSONPatch, ReducerVersion: "1", Fields: []contract.StateField{{Name: statusField, Type: contract.StateString, Required: true, MaxBytes: 64}}},
		Limits: contract.Limits{MaxSteps: 4, MaxVisitsPerNode: 1},
	}
	kinds, err := contract.NewNodeKindRegistry([]contract.NodeKindMetadata{{ID: "core-turn", Version: "1"}})
	if err != nil {
		return nil, err
	}
	reducers, err := contract.NewReducerRegistry(nil)
	if err != nil {
		return nil, err
	}
	predicates, err := contract.NewPredicateRegistry(nil)
	if err != nil {
		return nil, err
	}
	return contract.ValidateDefinition(definition, kinds, reducers, predicates)
}

// ReferencePlanner emits only bounded source identities and estimates. It is
// suitable for the built-in node when no domain planner is needed.
type ReferencePlanner struct{}

func (ReferencePlanner) Plan(ctx context.Context, request execgraph.ContextRequest) (contract.ContextPlan, error) {
	if ctx == nil {
		return contract.ContextPlan{}, ErrInvalidOptions
	}
	if err := ctx.Err(); err != nil {
		return contract.ContextPlan{}, err
	}
	if err := contract.ValidateContextBudget(request.Budget); err != nil {
		return contract.ContextPlan{}, err
	}
	sources := []contract.ContextSource{{Layer: contract.LayerCurrentInput, SourceID: "current-input", Revision: "1", EstimatedTokens: 1, Required: true}}
	for _, layer := range request.View.Layers {
		if layer == contract.LayerRequiredState {
			sources = append(sources, contract.ContextSource{Layer: layer, SourceID: "state", Revision: "1", EstimatedTokens: 1, Required: true})
		}
	}
	for _, source := range sources {
		if request.Budget.LayerBudgets[source.Layer] < source.EstimatedTokens {
			return contract.ContextPlan{}, ErrInvalidOptions
		}
	}
	plan := contract.ContextPlan{DefinitionRevision: request.DefinitionRevision, PlanRevision: "reference-v1", View: request.View.Clone(), Budget: request.Budget.Clone(), Sources: sources}
	if err := contract.ValidateContextPlan(plan); err != nil {
		return contract.ContextPlan{}, err
	}
	return plan, nil
}

// Registration creates the fixed, auditable graph executor registration.
func Registration(options Options) (runexecutor.Registration, error) {
	if options.Definition == nil || options.Store == nil || options.Authority == nil || options.Planner == nil {
		return runexecutor.Registration{}, fmt.Errorf("%w: definition/store/authority/planner required", ErrInvalidOptions)
	}
	defaultDefinition, err := NewDefaultDefinition()
	if err != nil || options.Definition.Revision() != defaultDefinition.Revision() {
		return runexecutor.Registration{}, fmt.Errorf("%w: only the built-in core-turn definition is supported", ErrInvalidOptions)
	}
	definition := options.Definition.Clone()
	return runexecutor.Registration{
		Metadata: runexecutor.Metadata{ID: ID, Version: Version, ImplementationRevision: ImplementationRevision},
		Factory: func(deps runexecutor.Dependencies) (runexecutor.RunExecutor, error) {
			if deps.Runtime == nil {
				return nil, fmt.Errorf("%w: runtime required", ErrInvalidOptions)
			}
			return &executor{runtime: deps.Runtime, options: options, definition: definition}, nil
		},
	}, nil
}

type executor struct {
	runtime    *core.Runtime
	options    Options
	definition *contract.ValidatedDefinition
}
type onceLease struct {
	inner execgraph.SegmentLease
	once  sync.Once
	err   error
}

func (l *onceLease) Verify(ctx context.Context, r execgraph.LeaseRequest) error {
	return l.inner.Verify(ctx, r)
}
func (l *onceLease) Release(ctx context.Context, r execgraph.LeaseRequest) error {
	l.once.Do(func() {
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		l.err = safeRelease(releaseCtx, l.inner, r)
	})
	return l.err
}

func (e *executor) RunTurn(ctx context.Context, principal core.Principal, session *core.Session, input core.TurnInput, emit func(core.SessionEvent)) (core.TurnResult, error) {
	if session == nil || input.RunID == "" {
		return core.TurnResult{}, fmt.Errorf("%w: session/run", ErrInvalidOptions)
	}
	key := contract.CheckpointKey{TenantID: principal.TenantID, SessionID: session.ID(), RunID: input.RunID}
	grant, err := safeAcquire(ctx, e.options.Authority, key)
	if err != nil {
		return core.TurnResult{}, fmt.Errorf("%w: %v", ErrSegmentUnavailable, err)
	}
	if grant.SegmentID == "" || grant.HostGeneration == 0 || grant.Lease == nil {
		return core.TurnResult{}, ErrSegmentUnavailable
	}
	lease := &onceLease{inner: grant.Lease}
	node := &turnNode{runtime: e.runtime, principal: principal, session: session, runID: input.RunID, text: input.Text, metadata: input.CompositionMetadata, emit: emit, resume: false, lease: lease}
	engine, err := e.engine(node)
	if err != nil {
		_ = lease.Release(ctx, execgraph.LeaseRequest{Key: key, SegmentID: grant.SegmentID, HostGeneration: grant.HostGeneration})
		return core.TurnResult{}, err
	}
	result, runErr := engine.Run(ctx, execgraph.StartRequest{Key: key, SegmentID: grant.SegmentID, HostGeneration: grant.HostGeneration, InitialState: initialState(input.Text)})
	if result.Checkpoint.Status != contract.CheckpointWaitingApproval {
		if releaseErr := lease.Release(ctx, execgraph.LeaseRequest{Key: key, SegmentID: grant.SegmentID, HostGeneration: grant.HostGeneration}); releaseErr != nil && runErr == nil {
			runErr = ErrSegmentUnavailable
		}
	}
	return mapResult(input.RunID, node.answer, result, runErr)
}

func (e *executor) ResumeTurn(ctx context.Context, principal core.Principal, session *core.Session, input core.ResumeInput, emit func(core.SessionEvent)) (core.TurnResult, error) {
	if session == nil || input.RunID == "" || e.options.Decisions == nil {
		return core.TurnResult{}, ErrApprovalResolverUnavailable
	}
	key := contract.CheckpointKey{TenantID: principal.TenantID, SessionID: session.ID(), RunID: input.RunID}
	pending, ok, err := session.PendingApproval(input.RunID)
	if err != nil {
		return core.TurnResult{}, err
	}
	if !ok {
		return core.TurnResult{}, fmt.Errorf("%w: no pending approval", ErrApprovalResolverUnavailable)
	}
	decision, err := safeResolve(ctx, e.options.Decisions, principal, key, pending.ApprovalID)
	if err != nil {
		return core.TurnResult{}, err
	}
	if err := decision.Validate(); err != nil || decision.Key != key || decision.ApprovalID != pending.ApprovalID {
		return core.TurnResult{}, ErrApprovalResolverUnavailable
	}
	grant, err := safeAcquire(ctx, e.options.Authority, key)
	if err != nil || grant.SegmentID == "" || grant.HostGeneration == 0 || grant.Lease == nil {
		return core.TurnResult{}, ErrSegmentUnavailable
	}
	lease := &onceLease{inner: grant.Lease}
	node := &turnNode{runtime: e.runtime, principal: principal, session: session, runID: input.RunID, metadata: input.CompositionMetadata, emit: emit, resume: true, lease: lease}
	engine, err := e.engine(node)
	if err != nil {
		_ = lease.Release(ctx, execgraph.LeaseRequest{Key: key, SegmentID: grant.SegmentID, HostGeneration: grant.HostGeneration})
		return core.TurnResult{}, err
	}
	result, runErr := engine.Resume(ctx, execgraph.ResumeRequest{Key: key, SegmentID: grant.SegmentID, HostGeneration: grant.HostGeneration, ApprovalID: pending.ApprovalID, Decision: decision})
	if result.Checkpoint.Status != contract.CheckpointWaitingApproval {
		if releaseErr := lease.Release(ctx, execgraph.LeaseRequest{Key: key, SegmentID: grant.SegmentID, HostGeneration: grant.HostGeneration}); releaseErr != nil && runErr == nil {
			runErr = ErrSegmentUnavailable
		}
	}
	return mapResult(input.RunID, node.answer, result, runErr)
}

func (e *executor) engine(node execgraph.Node) (*execgraph.Executor, error) {
	// The definition is supplied by the caller, but the built-in binding is
	// deliberately replaced per invocation so the node captures this runtime.
	bindings, err := execgraph.NewBindings(e.definition, "graph-core-turn-composition", []execgraph.NodeBinding{{Kind: "core-turn", Version: "1", Revision: ImplementationRevision, Node: node}}, execgraph.BuiltinReducerBinding("builtin-v1"), nil)
	if err != nil {
		return nil, err
	}
	return execgraph.NewExecutorWithOptions(e.definition, bindings, e.options.Store, execgraph.Options{Observer: e.options.Observer, Lease: node.(*turnNode).lease, Planner: e.options.Planner, Sandbox: e.options.Sandbox, Approval: e.options.Approval})
}

func initialState(_ string) contract.State {
	return contract.State{statusField: json.RawMessage(`"pending"`)}
}

func mapResult(runID, answer string, result execgraph.Result, runErr error) (core.TurnResult, error) {
	if result.Checkpoint.Status == contract.CheckpointWaitingApproval {
		if errors.Is(runErr, execgraph.ErrLeaseUnavailable) || errors.Is(runErr, execgraph.ErrLeasePanic) {
			return core.TurnResult{RunID: runID, Status: core.RunWaitingApproval}, ErrSegmentUnavailable
		}
		if runErr != nil && !errors.Is(runErr, execgraph.ErrApprovalPending) {
			return core.TurnResult{RunID: runID, Status: core.RunWaitingApproval}, sanitizeRunError(runErr)
		}
		return core.TurnResult{RunID: runID, Status: core.RunWaitingApproval}, nil
	}
	if errors.Is(runErr, execgraph.ErrLeaseUnavailable) || errors.Is(runErr, execgraph.ErrLeasePanic) {
		return core.TurnResult{RunID: runID, Status: core.RunFailed}, ErrSegmentUnavailable
	}
	if result.Checkpoint.Status == contract.CheckpointCompleted {
		status, ok := checkpointStatus(result.Checkpoint.State)
		if !ok || status != core.RunCompleted {
			return core.TurnResult{RunID: runID, Status: core.RunFailed}, ErrInvalidOptions
		}
		return core.TurnResult{RunID: runID, Status: core.RunCompleted, Answer: answer}, runErr
	}
	status := core.RunFailed
	if result.Checkpoint.Status == contract.CheckpointCancelled {
		status = core.RunCancelled
	}
	return core.TurnResult{RunID: runID, Status: status}, sanitizeRunError(runErr)
}

// Engine/storage errors are deliberately reduced to stable adapter classes;
// underlying messages may contain credentials or host-specific details.
func sanitizeRunError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, execgraph.ErrLeaseUnavailable) || errors.Is(err, execgraph.ErrLeasePanic) {
		return ErrSegmentUnavailable
	}
	if errors.Is(err, execgraph.ErrExecutionFailed) {
		return execgraph.ErrExecutionFailed
	}
	return execgraph.ErrExecutionFailed
}

func checkpointStatus(state contract.State) (core.RunStatus, bool) {
	raw, ok := state[statusField]
	if !ok {
		return "", false
	}
	var value string
	if json.Unmarshal(raw, &value) != nil || strings.TrimSpace(value) != value {
		return "", false
	}
	switch core.RunStatus(value) {
	case core.RunCompleted, core.RunFailed, core.RunCancelled, core.RunWaitingApproval:
		return core.RunStatus(value), true
	default:
		return "", false
	}
}

func safeAcquire(ctx context.Context, authority SegmentAuthority, key contract.CheckpointKey) (grant SegmentGrant, err error) {
	defer func() {
		if recover() != nil {
			grant = SegmentGrant{}
			err = ErrCallbackPanic
		}
	}()
	grant, err = authority.Acquire(ctx, key)
	if err != nil {
		return SegmentGrant{}, ErrSegmentUnavailable
	}
	return grant, nil
}

func safeResolve(ctx context.Context, resolver ApprovalDecisionResolver, principal core.Principal, key contract.CheckpointKey, id string) (decision execgraph.ApprovalDecision, err error) {
	defer func() {
		if recover() != nil {
			decision = execgraph.ApprovalDecision{}
			err = ErrCallbackPanic
		}
	}()
	decision, err = resolver.Resolve(ctx, principal, key, id)
	if err != nil {
		return execgraph.ApprovalDecision{}, ErrApprovalResolverUnavailable
	}
	return decision, nil
}

func safeRelease(ctx context.Context, lease execgraph.SegmentLease, request execgraph.LeaseRequest) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrCallbackPanic
		}
	}()
	return lease.Release(ctx, request)
}

type turnNode struct {
	runtime     *core.Runtime
	principal   core.Principal
	session     *core.Session
	runID, text string
	metadata    map[string]string
	answer      string
	emit        func(core.SessionEvent)
	resume      bool
	lease       execgraph.SegmentLease
}

func (n *turnNode) Execute(ctx context.Context, request execgraph.NodeRequest) (execgraph.NodeResult, error) {
	if request.ResolvedApproval != nil && request.ResolvedApproval.Decision != string(execgraph.ApprovalDecisionApproved) && request.ResolvedApproval.Decision != string(execgraph.ApprovalDecisionDenied) {
		return execgraph.NodeResult{}, execgraph.ErrInvalidApproval
	}
	var result core.TurnResult
	var err error
	if n.resume {
		result, err = n.runtime.ResumeTurn(ctx, n.principal, n.session, core.ResumeInput{RunID: n.runID, CompositionMetadata: cloneMetadata(n.metadata)}, n.emit)
	} else {
		result, err = n.runtime.RunTurn(ctx, n.principal, n.session, core.TurnInput{RunID: n.runID, Text: n.text, CompositionMetadata: cloneMetadata(n.metadata)}, n.emit)
	}
	if err != nil {
		return execgraph.NodeResult{}, err
	}
	if result.Status == core.RunWaitingApproval {
		pending, ok, pendingErr := n.session.PendingApproval(n.runID)
		if pendingErr != nil {
			return execgraph.NodeResult{}, pendingErr
		}
		if !ok {
			return execgraph.NodeResult{}, fmt.Errorf("%w: missing core approval", execgraph.ErrApprovalPending)
		}
		return execgraph.NodeResult{}, &execgraph.ApprovalPendingError{ApprovalID: pending.ApprovalID}
	}
	if result.Status != core.RunCompleted {
		return execgraph.NodeResult{}, fmt.Errorf("core turn did not complete: %s", result.Status)
	}
	n.answer = result.Answer
	status, _ := json.Marshal(string(result.Status))
	return execgraph.NodeResult{Patch: contract.StatePatch{Set: map[string]json.RawMessage{statusField: status}}}, nil
}

func cloneMetadata(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	out := make(map[string]string, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}
