package graph

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	contract "github.com/cc-auto-agent/harness-core/pkg/extensions/graph"
)

var (
	ErrExecutionFailed     = errors.New("graph execution failed")
	ErrUnknownOutcome      = errors.New("graph execution outcome is unknown")
	ErrApprovalPending     = errors.New("graph execution is waiting for approval")
	ErrSegmentMismatch     = errors.New("graph execution segment mismatch")
	ErrEdgeAmbiguous       = errors.New("graph conditional edge selection is ambiguous")
	ErrExecutionInProgress = errors.New("graph execution is already in progress")
	ErrExecutionReplay     = errors.New("graph execution commit was replayed")
	ErrNodePanic           = errors.New("graph node panicked")
	ErrReducerPanic        = errors.New("graph reducer panicked")
	ErrPredicatePanic      = errors.New("graph predicate panicked")
	ErrPlannerPanic        = errors.New("graph context planner panicked")
	ErrSandboxPanic        = errors.New("graph sandbox authorizer panicked")
	ErrLeasePanic          = errors.New("graph segment lease panicked")
	ErrObserverPanic       = errors.New("graph observer panicked")
)

const (
	durableOperationTimeout = 5 * time.Second
	observerTimeout         = 100 * time.Millisecond
)

type EventKind string

// OutcomeCode is the finite, low-cardinality vocabulary emitted by the graph
// engine. Dynamic error text must never be used as an outcome.
type OutcomeCode string

const (
	OutcomeCreated             OutcomeCode = "created"
	OutcomeNodeStarted         OutcomeCode = "node_started"
	OutcomeApprovalPending     OutcomeCode = "approval_pending"
	OutcomeRetry               OutcomeCode = "retry"
	OutcomeErrorEdge           OutcomeCode = "error_edge"
	OutcomeEdgeSelected        OutcomeCode = "edge_selected"
	OutcomeCompleted           OutcomeCode = "completed"
	OutcomeCancelled           OutcomeCode = "cancelled"
	OutcomeRecoveryUnknown     OutcomeCode = "recovery_unknown"
	OutcomeInterruptedUnknown  OutcomeCode = "interrupted_unknown"
	OutcomeNodeTimeout         OutcomeCode = "node_timeout"
	OutcomeNodeFailed          OutcomeCode = "node_failed"
	OutcomeBindingMissing      OutcomeCode = "binding_missing"
	OutcomeLeaseUnavailable    OutcomeCode = "lease_unavailable"
	OutcomeSandboxUnavailable  OutcomeCode = "sandbox_unavailable"
	OutcomeSandboxDenied       OutcomeCode = "sandbox_denied"
	OutcomeContextUnavailable  OutcomeCode = "context_unavailable"
	OutcomeInvalidApproval     OutcomeCode = "invalid_approval"
	OutcomeReducerFailed       OutcomeCode = "reducer_failed"
	OutcomeEdgeSelectionFailed OutcomeCode = "edge_selection_failed"
	OutcomeStepLimit           OutcomeCode = "step_limit"
	OutcomeVisitLimit          OutcomeCode = "visit_limit"
	OutcomeUnknownNode         OutcomeCode = "unknown_node"
	OutcomeApprovalApproved    OutcomeCode = "approval_approved"
	OutcomeApprovalDenied      OutcomeCode = "approval_denied"
	OutcomePreflightFailed     OutcomeCode = "preflight_failed"
	OutcomeUnknown             OutcomeCode = "unknown"
)

func ValidOutcomeCode(value OutcomeCode) bool {
	switch value {
	case OutcomeCreated, OutcomeNodeStarted, OutcomeApprovalPending, OutcomeRetry, OutcomeErrorEdge, OutcomeEdgeSelected, OutcomeCompleted, OutcomeCancelled, OutcomeRecoveryUnknown, OutcomeInterruptedUnknown, OutcomeNodeTimeout, OutcomeNodeFailed, OutcomePreflightFailed, OutcomeBindingMissing, OutcomeLeaseUnavailable, OutcomeSandboxUnavailable, OutcomeSandboxDenied, OutcomeContextUnavailable, OutcomeInvalidApproval, OutcomeReducerFailed, OutcomeEdgeSelectionFailed, OutcomeStepLimit, OutcomeVisitLimit, OutcomeUnknownNode, OutcomeApprovalApproved, OutcomeApprovalDenied, OutcomeUnknown:
		return true
	default:
		return false
	}
}

func NormalizeOutcomeCode(value OutcomeCode) OutcomeCode {
	if ValidOutcomeCode(value) {
		return value
	}
	return OutcomeUnknown
}

const (
	EventStarted          EventKind = "started"
	EventNodeStart        EventKind = "node_start"
	EventNodeEnd          EventKind = "node_end"
	EventRetry            EventKind = "retry"
	EventSuspended        EventKind = "suspended"
	EventCompleted        EventKind = "completed"
	EventFailed           EventKind = "failed"
	EventCancelled        EventKind = "cancelled"
	EventUnknown          EventKind = "unknown"
	EventResumed          EventKind = "resumed"
	EventApprovalResolved EventKind = "approval_resolved"
	EventTimeout          EventKind = "timeout"
	EventConflict         EventKind = "conflict"
	EventEdgeAmbiguous    EventKind = "edge_ambiguous"
	EventNodeError        EventKind = "node_error"
)

// Event carries only bounded metadata. Raw state, configuration, prompts,
// credentials, approval payloads, and tool arguments are intentionally absent.
type Event struct {
	Kind                   EventKind
	GraphID                string
	DefinitionRevision     string
	NodeID                 string
	NodeKind               string
	Status                 contract.CheckpointStatus
	CheckpointRevision     uint64
	Step                   int
	Attempt                int
	OutcomeCode            OutcomeCode
	EdgeKind               contract.EdgeKind
	TenantID               string
	SessionID              string
	RunID                  string
	SegmentID              string
	AttemptID              string
	Duration               time.Duration
	ContextPlanRevision    string
	ContextEstimatedTokens int64
}

// Observer is called after a committed fact using a short bounded context.
// Implementations must be non-blocking with respect to that deadline; observer
// failure can never reverse an execution commit.
type Observer interface {
	Observe(context.Context, Event) error
}

type StartRequest struct {
	Key            contract.CheckpointKey
	SegmentID      string
	HostGeneration uint64
	InitialState   contract.State
}

type ResumeRequest struct {
	Key            contract.CheckpointKey
	SegmentID      string
	HostGeneration uint64
	ApprovalID     string
	// Approved is retained for source compatibility but is ignored unless a
	// validated Decision is present. A bare boolean can never authorize a
	// resume.
	Approved bool
	Decision ApprovalDecision
}

type Result struct {
	Checkpoint contract.Checkpoint
	Suspended  bool
}

// Executor is safe for concurrent use when the Store is safe for concurrent
// use. It starts no goroutines and holds no transaction while node code runs.
type Executor struct {
	definition *contract.ValidatedDefinition
	bindings   *Bindings
	store      contract.Store
	observer   Observer
	lease      SegmentLease
	planner    ContextPlanner
	sandbox    SandboxAuthorizer
	approval   ApprovalAuthorizer
}

// NewExecutor is an inspection-only compatibility constructor. The resulting
// executor has no SegmentLease or ContextPlanner, so Run fails closed before
// creating or mutating a checkpoint. Executable use must call
// NewExecutorWithOptions.
func NewExecutor(definition *contract.ValidatedDefinition, bindings *Bindings, store contract.Store, observer Observer) (*Executor, error) {
	return NewExecutorWithOptions(definition, bindings, store, Options{Observer: observer})
}

// NewExecutorWithOptions configures the explicit external authority seams.
// A nil Lease or Planner is accepted at construction only so configuration can
// be inspected; Run fails closed before any node executes.
func NewExecutorWithOptions(definition *contract.ValidatedDefinition, bindings *Bindings, store contract.Store, options Options) (*Executor, error) {
	if definition == nil || definition.Revision() == "" || bindings == nil || store == nil {
		return nil, fmt.Errorf("%w: executor dependencies are incomplete", ErrBindingMismatch)
	}
	if bindings.DefinitionRevision() != definition.Revision() {
		return nil, fmt.Errorf("%w: definition revision differs", ErrBindingMismatch)
	}
	return &Executor{definition: definition.Clone(), bindings: bindings, store: store, observer: options.Observer, lease: options.Lease, planner: options.Planner, sandbox: options.Sandbox, approval: options.Approval}, nil
}

// Run creates a ready checkpoint if absent, or continues the matching ready
// segment. It never replays a checkpoint found executing after a crash.
func (e *Executor) Run(ctx context.Context, request StartRequest) (Result, error) {
	if ctx == nil {
		return Result{}, errors.New("nil context")
	}
	checkpoint, err := e.store.Load(ctx, request.Key)
	if errors.Is(err, contract.ErrCheckpointNotFound) {
		if err := e.verifyLease(ctx, LeaseRequest{Key: request.Key, SegmentID: request.SegmentID, HostGeneration: request.HostGeneration}); err != nil {
			return Result{}, err
		}
		checkpoint, _, err = e.create(ctx, request)
	}
	if err != nil {
		return Result{}, err
	}
	if err := e.verifyFrozenCheckpoint(checkpoint); err != nil {
		return Result{Checkpoint: checkpoint}, err
	}
	if checkpoint.Status == contract.CheckpointExecuting {
		return e.recoverUnknown(ctx, checkpoint, request)
	}
	if err := e.verifyCheckpoint(checkpoint, request.SegmentID, request.HostGeneration); err != nil {
		return Result{Checkpoint: checkpoint}, err
	}
	if checkpoint.Status == contract.CheckpointReady {
		if err := e.verifyLease(ctx, LeaseRequest{Key: request.Key, SegmentID: request.SegmentID, HostGeneration: request.HostGeneration}); err != nil {
			return Result{Checkpoint: checkpoint}, err
		}
	}
	return e.drive(ctx, checkpoint)
}

func (e *Executor) Resume(ctx context.Context, request ResumeRequest) (Result, error) {
	if ctx == nil {
		return Result{}, errors.New("nil context")
	}
	checkpoint, err := e.store.Load(ctx, request.Key)
	if err != nil {
		return Result{}, err
	}
	if err := e.verifyFrozenCheckpoint(checkpoint); err != nil {
		return Result{Checkpoint: checkpoint}, err
	}
	if checkpoint.Status != contract.CheckpointWaitingApproval {
		return Result{Checkpoint: checkpoint}, fmt.Errorf("%w: checkpoint is not waiting approval", ErrSegmentMismatch)
	}
	if checkpoint.PendingApprovalID != request.ApprovalID || request.ApprovalID == "" || request.SegmentID == "" || request.HostGeneration <= checkpoint.HostGeneration {
		return Result{Checkpoint: checkpoint}, fmt.Errorf("%w: stale approval resume", ErrSegmentMismatch)
	}
	if err := e.authorizeApproval(ctx, checkpoint, request); err != nil {
		return Result{Checkpoint: checkpoint}, err
	}
	lease := LeaseRequest{Key: request.Key, SegmentID: request.SegmentID, HostGeneration: request.HostGeneration}
	if err := e.verifyLease(ctx, lease); err != nil {
		return Result{Checkpoint: checkpoint}, err
	}
	next := contract.CloneCheckpoint(checkpoint)
	next.SegmentID = request.SegmentID
	next.HostGeneration = request.HostGeneration
	next.PendingApprovalID = ""
	if request.Decision.Decision == ApprovalDecisionApproved {
		next.Status = contract.CheckpointReady
		next.FailureCode = ""
		next.ResolvedApproval = &contract.ApprovalEvidence{TenantID: request.Decision.TenantID, ApprovalID: request.Decision.ApprovalID, Decision: string(request.Decision.Decision), Revision: request.Decision.Revision, SourceSegmentID: request.Decision.SourceSegmentID, ActorID: request.Decision.ActorID, AuthorizationBasis: request.Decision.AuthorizationBasis}
		var disposition contract.CommitDisposition
		next, disposition, err = e.commitWithApproval(ctx, checkpoint, next, OutcomeApprovalApproved, request.Decision, lease)
		if err != nil {
			return Result{Checkpoint: checkpoint}, err
		}
		if disposition == contract.CommitApplied {
			e.emit(ctx, eventFor(EventApprovalResolved, e.definition.Definition(), next, OutcomeApprovalApproved))
			e.emit(ctx, eventFor(EventResumed, e.definition.Definition(), next, OutcomeApprovalApproved))
		}
		return e.drive(ctx, next)
	}
	spec, _ := findNode(e.definition.Definition(), checkpoint.CurrentNodeID)
	next.ResolvedApproval = &contract.ApprovalEvidence{TenantID: request.Decision.TenantID, ApprovalID: request.Decision.ApprovalID, Decision: string(request.Decision.Decision), Revision: request.Decision.Revision, SourceSegmentID: request.Decision.SourceSegmentID, ActorID: request.Decision.ActorID, AuthorizationBasis: request.Decision.AuthorizationBasis}
	if spec.ApprovalDeniedPolicy == contract.ApprovalDeniedResumeWithDecision {
		next.Status = contract.CheckpointReady
		next.FailureCode = ""
		var disposition contract.CommitDisposition
		next, disposition, err = e.commitWithApproval(ctx, checkpoint, next, OutcomeApprovalDenied, request.Decision, lease)
		if err != nil {
			return Result{Checkpoint: checkpoint}, err
		}
		if disposition == contract.CommitApplied {
			e.emit(ctx, eventFor(EventApprovalResolved, e.definition.Definition(), next, OutcomeApprovalDenied))
			e.emit(ctx, eventFor(EventResumed, e.definition.Definition(), next, OutcomeApprovalDenied))
		}
		return e.drive(ctx, next)
	}
	next.Status = contract.CheckpointFailed
	next.FailureCode, next.AttemptID = string(OutcomeApprovalDenied), ""
	next.PendingApprovalID = ""
	var disposition contract.CommitDisposition
	next, disposition, err = e.commitWithApproval(ctx, checkpoint, next, OutcomeApprovalDenied, request.Decision, lease)
	if err != nil {
		return Result{Checkpoint: checkpoint}, err
	}
	if disposition == contract.CommitApplied {
		e.emit(ctx, eventFor(EventApprovalResolved, e.definition.Definition(), next, OutcomeApprovalDenied))
		e.emit(ctx, eventFor(EventFailed, e.definition.Definition(), next, OutcomeApprovalDenied))
	}
	releaseErr := e.releaseLease(ctx, lease)
	return Result{Checkpoint: next}, errors.Join(fmt.Errorf("%w: approval denied", ErrExecutionFailed), releaseErr)
}

// recoverUnknown is the sole abandoned-executing handoff. It requires a new
// segment and strictly newer generation, validates that new authority, then
// commits only unknown; it never invokes the interrupted node.
func (e *Executor) recoverUnknown(ctx context.Context, checkpoint contract.Checkpoint, request StartRequest) (Result, error) {
	if request.SegmentID == "" || request.SegmentID == checkpoint.SegmentID || request.HostGeneration <= checkpoint.HostGeneration {
		return Result{Checkpoint: checkpoint}, fmt.Errorf("%w: recovery requires a new segment and newer generation", ErrSegmentMismatch)
	}
	lease := LeaseRequest{Key: request.Key, SegmentID: request.SegmentID, HostGeneration: request.HostGeneration}
	if err := e.verifyLease(ctx, lease); err != nil {
		return Result{Checkpoint: checkpoint}, err
	}
	next := contract.CloneCheckpoint(checkpoint)
	next.SegmentID, next.HostGeneration = request.SegmentID, request.HostGeneration
	next.Status, next.FailureCode = contract.CheckpointUnknown, string(OutcomeInterruptedUnknown)
	durable, cancel := durableContext()
	defer cancel()
	next, disposition, err := e.commit(durable, checkpoint, next, OutcomeRecoveryUnknown, lease)
	if err != nil {
		return Result{Checkpoint: checkpoint}, err
	}
	if disposition == contract.CommitApplied {
		e.emit(ctx, eventFor(EventUnknown, e.definition.Definition(), next, OutcomeInterruptedUnknown))
	}
	unknownErr := fmt.Errorf("%w: stable attempt %s", ErrUnknownOutcome, next.AttemptID)
	return Result{Checkpoint: next}, errors.Join(unknownErr, e.releaseLease(ctx, lease))
}

func (e *Executor) create(ctx context.Context, request StartRequest) (contract.Checkpoint, contract.CommitDisposition, error) {
	def := e.definition.Definition()
	state, err := contract.ValidateState(def.State, request.InitialState)
	if err != nil {
		return contract.Checkpoint{}, contract.CommitUnknown, err
	}
	checkpoint := contract.Checkpoint{
		Key: request.Key, SegmentID: request.SegmentID, GraphID: def.ID,
		DefinitionRevision: e.definition.Revision(), CompositionRevision: e.bindings.CompositionRevision(),
		ImplementationRevision: e.bindings.ImplementationRevision(), HostGeneration: request.HostGeneration,
		CurrentNodeID: def.EntryNode, Revision: 1, State: state, Visits: map[string]int{}, Status: contract.CheckpointReady,
	}
	transition := transitionFor(contract.Checkpoint{}, checkpoint, OutcomeCreated)
	// Run verifies the creator before entering this helper. Re-verify directly
	// before Create as well so the initial head cannot be written after that
	// authority has been revoked.
	if err := e.verifyLease(ctx, LeaseRequest{Key: request.Key, SegmentID: request.SegmentID, HostGeneration: request.HostGeneration}); err != nil {
		return contract.Checkpoint{}, contract.CommitUnknown, err
	}
	created, disposition, err := e.store.Create(ctx, checkpoint, transition)
	if err == nil && !disposition.Valid() {
		return contract.Checkpoint{}, contract.CommitUnknown, fmt.Errorf("%w: store returned invalid commit disposition", ErrExecutionFailed)
	}
	if err == nil && disposition == contract.CommitApplied {
		spec, _ := findNode(def, created.CurrentNodeID)
		e.emit(ctx, eventForAttempt(EventStarted, def, created, spec, "", 0, contract.ContextPlan{}, OutcomeCreated))
	}
	return created, disposition, err
}

func (e *Executor) drive(ctx context.Context, checkpoint contract.Checkpoint) (Result, error) {
	def := e.definition.Definition()
	for {
		if ctx.Err() != nil {
			return e.cancel(checkpoint)
		}
		switch checkpoint.Status {
		case contract.CheckpointCompleted:
			return Result{Checkpoint: checkpoint}, nil
		case contract.CheckpointWaitingApproval:
			return Result{Checkpoint: checkpoint, Suspended: true}, fmt.Errorf("%w: %s", ErrApprovalPending, checkpoint.PendingApprovalID)
		case contract.CheckpointFailed, contract.CheckpointCancelled, contract.CheckpointUnknown:
			return Result{Checkpoint: checkpoint}, terminalError(checkpoint)
		case contract.CheckpointExecuting:
			return Result{Checkpoint: checkpoint}, ErrUnknownOutcome
		case contract.CheckpointReady:
			// continue below
		default:
			return Result{Checkpoint: checkpoint}, fmt.Errorf("%w: invalid checkpoint status", ErrExecutionFailed)
		}

		spec, ok := findNode(def, checkpoint.CurrentNodeID)
		if !ok {
			return e.fail(checkpoint, OutcomeUnknownNode)
		}
		// An approved resume retains its stable attempt and is not a new live
		// entry. Limits apply before a new attempt is created.
		if checkpoint.AttemptID == "" && checkpoint.Steps >= def.Limits.MaxSteps {
			return e.fail(checkpoint, OutcomeStepLimit)
		}
		if checkpoint.AttemptID == "" && def.Limits.MaxVisitsPerNode > 0 && checkpoint.Visits[spec.ID] >= def.Limits.MaxVisitsPerNode {
			return e.fail(checkpoint, OutcomeVisitLimit)
		}
		executing := contract.CloneCheckpoint(checkpoint)
		lease := LeaseRequest{Key: executing.Key, SegmentID: executing.SegmentID, HostGeneration: executing.HostGeneration}
		executing.Status = contract.CheckpointExecuting
		if executing.AttemptID == "" {
			executing.Attempt++
			executing.AttemptID = stableAttemptID(executing.Key, spec.ID, executing.Attempt, executing.Revision+1)
			executing.Steps++
			executing.Visits[spec.ID]++
		}
		var err error
		var disposition contract.CommitDisposition
		executing, disposition, err = e.commit(ctx, checkpoint, executing, OutcomeNodeStarted, lease)
		if err != nil {
			return Result{Checkpoint: checkpoint}, err
		}
		if disposition == contract.CommitReplayed {
			return Result{Checkpoint: executing}, ErrExecutionInProgress
		}
		e.emit(ctx, eventFor(EventNodeStart, def, executing, ""))

		binding, bound := e.bindings.node(spec.Kind, spec.KindVersion)
		if !bound {
			return e.fail(executing, OutcomeBindingMissing)
		}
		if err := e.verifyLease(ctx, lease); err != nil {
			return Result{Checkpoint: executing}, err
		}
		if spec.UsesSandbox {
			if e.sandbox == nil {
				return e.fail(executing, OutcomeSandboxUnavailable)
			}
			if err := safeSandboxAuthorize(e.sandbox, ctx, SandboxRequest{Lease: lease, NodeID: spec.ID, Attempt: executing.AttemptID}); err != nil {
				return e.fail(executing, OutcomeSandboxDenied)
			}
		}
		plan, err := e.plan(ctx, executing, spec)
		if err != nil {
			if ctx.Err() != nil {
				return e.cancel(executing)
			}
			return e.fail(executing, OutcomeContextUnavailable)
		}
		attemptCtx, cancel := context.WithTimeout(ctx, spec.Timeout)
		started := time.Now()
		result, executeErr := safeNodeExecute(binding.Node, attemptCtx, NodeRequest{Spec: cloneNodeSpec(spec), State: projectState(executing.State, spec.ContextView.StateFields), Context: plan.Clone(), ResolvedApproval: cloneApprovalEvidence(executing.ResolvedApproval), AttemptID: executing.AttemptID, SegmentID: executing.SegmentID})
		duration := time.Since(started)
		timedOut := errors.Is(attemptCtx.Err(), context.DeadlineExceeded)
		cancel()
		if ctx.Err() != nil {
			return e.cancel(executing)
		}
		// The node may have run side effects while the lease was revoked. Do
		// not interpret its result or turn the stale worker into a synthetic
		// failure; the executing checkpoint is the last authorized fact.
		if err := e.verifyLease(ctx, lease); err != nil {
			return Result{Checkpoint: executing}, err
		}
		if timedOut {
			executeErr = context.DeadlineExceeded
			e.emit(ctx, eventForAttempt(EventTimeout, def, executing, spec, executing.AttemptID, duration, plan, OutcomeNodeTimeout))
		}
		if pending, ok := asApprovalPending(executeErr); ok {
			if !validBindingID(pending.ApprovalID) {
				return e.fail(executing, OutcomeInvalidApproval)
			}
			next := contract.CloneCheckpoint(executing)
			next.Status, next.PendingApprovalID = contract.CheckpointWaitingApproval, pending.ApprovalID
			next.ResolvedApproval = nil
			next, disposition, err = e.commit(ctx, executing, next, OutcomeApprovalPending, lease)
			if err != nil {
				return Result{Checkpoint: executing}, err
			}
			if disposition == contract.CommitApplied {
				e.emit(ctx, eventForAttempt(EventSuspended, def, next, spec, executing.AttemptID, duration, plan, OutcomeApprovalPending))
			}
			return Result{Checkpoint: next, Suspended: true}, errors.Join(fmt.Errorf("%w: %s", ErrApprovalPending, pending.ApprovalID), e.releaseLease(ctx, lease))
		}
		if executeErr != nil {
			if errors.Is(executeErr, ErrNodePanic) {
				// The node may have performed an external side effect before
				// panicking. Keep the durable executing fact; only a newer
				// segment/generation may take it through recoverUnknown.
				return Result{Checkpoint: executing}, executeErr
			}
			if retryable(executeErr) && executing.Attempt < attempts(spec) {
				next := contract.CloneCheckpoint(executing)
				next.Status, next.AttemptID = contract.CheckpointReady, ""
				next, disposition, err = e.commit(ctx, executing, next, OutcomeRetry, lease)
				if err != nil {
					return Result{Checkpoint: executing}, err
				}
				if disposition == contract.CommitReplayed {
					return Result{Checkpoint: next}, ErrExecutionReplay
				}
				e.emit(ctx, eventForAttempt(EventRetry, def, next, spec, executing.AttemptID, duration, plan, failureCode(executeErr)))
				if spec.Retry.Backoff > 0 {
					timer := time.NewTimer(spec.Retry.Backoff)
					select {
					case <-ctx.Done():
						timer.Stop()
						return e.cancel(next)
					case <-timer.C:
					}
				}
				checkpoint = next
				continue
			}
			if edge, exists := errorEdge(def, spec.ID); exists {
				next := moveTo(executing, edge)
				next, disposition, err = e.commit(ctx, executing, next, OutcomeErrorEdge, lease)
				if err != nil {
					return Result{Checkpoint: executing}, err
				}
				if disposition == contract.CommitReplayed {
					return Result{Checkpoint: next}, ErrExecutionReplay
				}
				e.emit(ctx, edgeEvent(def, next, spec, executing.AttemptID, duration, plan, edge))
				checkpoint = next
				continue
			}
			return e.fail(executing, failureCode(executeErr))
		}

		state, reduceErr := e.reduce(executing.State, result.Patch)
		if ctx.Err() != nil {
			return e.cancel(executing)
		}
		if reduceErr != nil {
			return e.fail(executing, OutcomeReducerFailed)
		}
		if spec.Terminal {
			next := contract.CloneCheckpoint(executing)
			next.Status, next.State, next.AttemptID, next.ResolvedApproval = contract.CheckpointCompleted, state, "", nil
			next, disposition, err = e.commit(ctx, executing, next, OutcomeCompleted, lease)
			if err != nil {
				return Result{Checkpoint: executing}, err
			}
			if disposition == contract.CommitApplied {
				e.emit(ctx, eventForAttempt(EventCompleted, def, next, spec, executing.AttemptID, duration, plan, OutcomeCompleted))
			}
			return Result{Checkpoint: next}, nil
		}
		edge, selectErr := e.selectEdge(ctx, def, spec.ID, state)
		if ctx.Err() != nil {
			return e.cancel(executing)
		}
		if selectErr != nil {
			kind := EventNodeError
			if errors.Is(selectErr, ErrEdgeAmbiguous) {
				kind = EventEdgeAmbiguous
			}
			e.emit(ctx, eventForAttempt(kind, def, executing, spec, executing.AttemptID, duration, plan, OutcomeEdgeSelectionFailed))
			return e.fail(executing, OutcomeEdgeSelectionFailed)
		}
		next := moveTo(executing, edge)
		next.State = state
		next, disposition, err = e.commit(ctx, executing, next, OutcomeEdgeSelected, lease)
		if err != nil {
			return Result{Checkpoint: executing}, err
		}
		if disposition == contract.CommitReplayed {
			return Result{Checkpoint: next}, ErrExecutionReplay
		}
		e.emit(ctx, edgeEvent(def, next, spec, executing.AttemptID, duration, plan, edge))
		checkpoint = next
	}
}

func (e *Executor) reduce(state contract.State, patch contract.StatePatch) (contract.State, error) {
	def := e.definition.Definition()
	if e.bindings.reducer.ID == contract.ReducerTopLevelJSONPatch {
		return contract.ApplyStatePatch(def.State, state, patch)
	}
	next, err := safeReduce(e.bindings.reducer.Reducer, cloneState(state), clonePatch(patch))
	if err != nil {
		return nil, err
	}
	return contract.ValidateState(def.State, next)
}

func (e *Executor) selectEdge(ctx context.Context, def contract.Definition, from string, state contract.State) (contract.EdgeSpec, error) {
	var fallback *contract.EdgeSpec
	var selected *contract.EdgeSpec
	for _, edge := range def.Edges {
		if edge.From != from || edge.Kind == contract.EdgeError {
			continue
		}
		if edge.Kind == contract.EdgeDefault {
			copied := edge
			fallback = &copied
			continue
		}
		binding, ok := e.bindings.predicate(edge.Predicate, edge.PredicateVersion)
		if !ok {
			return contract.EdgeSpec{}, ErrBindingMismatch
		}
		matched, err := safeEvaluate(binding.Predicate, ctx, cloneState(state))
		if err != nil {
			return contract.EdgeSpec{}, err
		}
		if matched {
			if selected != nil {
				return contract.EdgeSpec{}, ErrEdgeAmbiguous
			}
			copied := edge
			selected = &copied
		}
	}
	if selected != nil {
		return *selected, nil
	}
	if fallback != nil {
		return *fallback, nil
	}
	return contract.EdgeSpec{}, errors.New("no edge selected")
}

func (e *Executor) fail(checkpoint contract.Checkpoint, code OutcomeCode) (Result, error) {
	source := checkpoint
	if checkpoint.Status == contract.CheckpointReady {
		// The public state machine intentionally forbids ready -> failed. Enter
		// a synthetic executing fact first; no node code is run on this path.
		executing := contract.CloneCheckpoint(checkpoint)
		executing.Status = contract.CheckpointExecuting
		executing.Attempt++
		executing.AttemptID = stableAttemptID(executing.Key, executing.CurrentNodeID, executing.Attempt, executing.Revision+1)
		ctx, cancel := durableContext()
		defer cancel()
		committed, disposition, err := e.commit(ctx, checkpoint, executing, OutcomePreflightFailed, leaseForCheckpoint(checkpoint))
		if err != nil {
			return Result{Checkpoint: checkpoint}, err
		}
		if disposition == contract.CommitReplayed {
			return Result{Checkpoint: committed}, ErrExecutionInProgress
		}
		checkpoint = committed
		source = committed
	}
	next := contract.CloneCheckpoint(checkpoint)
	next.Status, next.FailureCode, next.AttemptID, next.ResolvedApproval = contract.CheckpointFailed, string(code), "", nil
	ctx, cancel := durableContext()
	defer cancel()
	committed, disposition, err := e.commit(ctx, checkpoint, next, code, leaseForCheckpoint(checkpoint))
	if err != nil {
		return Result{Checkpoint: checkpoint}, err
	}
	if disposition == contract.CommitApplied {
		spec, _ := findNode(e.definition.Definition(), source.CurrentNodeID)
		e.emit(context.Background(), eventForAttempt(EventFailed, e.definition.Definition(), committed, spec, source.AttemptID, 0, contract.ContextPlan{}, code))
	}
	return Result{Checkpoint: committed}, fmt.Errorf("%w: %s", ErrExecutionFailed, code)
}

func (e *Executor) cancel(checkpoint contract.Checkpoint) (Result, error) {
	if checkpoint.Status != contract.CheckpointReady && checkpoint.Status != contract.CheckpointExecuting {
		return Result{Checkpoint: checkpoint}, terminalError(checkpoint)
	}
	source := checkpoint
	next := contract.CloneCheckpoint(checkpoint)
	next.Status, next.FailureCode, next.AttemptID, next.ResolvedApproval = contract.CheckpointCancelled, string(OutcomeCancelled), "", nil
	ctx, cancel := context.WithTimeout(context.Background(), durableOperationTimeout)
	defer cancel()
	committed, disposition, err := e.commit(ctx, checkpoint, next, OutcomeCancelled, leaseForCheckpoint(checkpoint))
	if err != nil {
		return Result{Checkpoint: checkpoint}, err
	}
	if disposition == contract.CommitApplied {
		spec, _ := findNode(e.definition.Definition(), source.CurrentNodeID)
		e.emit(context.Background(), eventForAttempt(EventCancelled, e.definition.Definition(), committed, spec, source.AttemptID, 0, contract.ContextPlan{}, OutcomeCancelled))
	}
	return Result{Checkpoint: committed}, context.Canceled
}

func (e *Executor) commit(ctx context.Context, previous, next contract.Checkpoint, outcome OutcomeCode, lease LeaseRequest) (contract.Checkpoint, contract.CommitDisposition, error) {
	return e.commitWithEvidence(ctx, previous, next, outcome, nil, lease)
}

func (e *Executor) commitWithApproval(ctx context.Context, previous, next contract.Checkpoint, outcome OutcomeCode, decision ApprovalDecision, lease LeaseRequest) (contract.Checkpoint, contract.CommitDisposition, error) {
	evidence := &contract.ApprovalEvidence{
		TenantID: decision.TenantID, ApprovalID: decision.ApprovalID, Decision: string(decision.Decision),
		Revision: decision.Revision, SourceSegmentID: decision.SourceSegmentID,
		ActorID: decision.ActorID, AuthorizationBasis: decision.AuthorizationBasis,
	}
	return e.commitWithEvidence(ctx, previous, next, outcome, evidence, lease)
}

func (e *Executor) commitWithEvidence(ctx context.Context, previous, next contract.Checkpoint, outcome OutcomeCode, approval *contract.ApprovalEvidence, lease LeaseRequest) (contract.Checkpoint, contract.CommitDisposition, error) {
	next.Revision = previous.Revision + 1
	transition := transitionFor(previous, next, outcome)
	transition.Approval = cloneApprovalEvidence(approval)
	// Every checkpoint mutation is authorized by the active segment immediately
	// before the CAS. Keeping the lease request mandatory prevents a new caller
	// from accidentally reintroducing an unfenced mutation path.
	if err := e.verifyLease(ctx, lease); err != nil {
		return contract.Checkpoint{}, contract.CommitUnknown, err
	}
	committed, disposition, err := e.store.CompareAndSwap(ctx, previous.Key, previous.Revision, next, transition)
	if err != nil {
		return contract.Checkpoint{}, contract.CommitUnknown, err
	}
	if !disposition.Valid() {
		return contract.Checkpoint{}, contract.CommitUnknown, fmt.Errorf("%w: store returned invalid commit disposition", ErrExecutionFailed)
	}
	return committed, disposition, nil
}

func leaseForCheckpoint(checkpoint contract.Checkpoint) LeaseRequest {
	return LeaseRequest{Key: checkpoint.Key, SegmentID: checkpoint.SegmentID, HostGeneration: checkpoint.HostGeneration}
}

func (e *Executor) verifyCheckpoint(checkpoint contract.Checkpoint, segment string, generation uint64) error {
	if err := e.verifyFrozenCheckpoint(checkpoint); err != nil {
		return err
	}
	if checkpoint.Status != contract.CheckpointWaitingApproval && (checkpoint.SegmentID != segment || checkpoint.HostGeneration != generation) {
		return ErrSegmentMismatch
	}
	return nil
}

func (e *Executor) verifyFrozenCheckpoint(checkpoint contract.Checkpoint) error {
	def := e.definition.Definition()
	if checkpoint.GraphID != def.ID || checkpoint.DefinitionRevision != e.definition.Revision() || checkpoint.CompositionRevision != e.bindings.CompositionRevision() || checkpoint.ImplementationRevision != e.bindings.ImplementationRevision() {
		return ErrBindingMismatch
	}
	canonicalState, err := contract.ValidateState(def.State, checkpoint.State)
	if err != nil || !reflect.DeepEqual(canonicalState, checkpoint.State) {
		return fmt.Errorf("%w: checkpoint state does not match frozen definition", ErrBindingMismatch)
	}
	if _, exists := findNode(def, checkpoint.CurrentNodeID); !exists {
		return fmt.Errorf("%w: checkpoint current node is not in frozen definition", ErrBindingMismatch)
	}
	for nodeID, visits := range checkpoint.Visits {
		if _, exists := findNode(def, nodeID); !exists {
			return fmt.Errorf("%w: checkpoint visit node is not in frozen definition", ErrBindingMismatch)
		}
		if def.Limits.MaxVisitsPerNode > 0 && visits > def.Limits.MaxVisitsPerNode {
			return fmt.Errorf("%w: checkpoint visit count exceeds frozen definition", ErrBindingMismatch)
		}
	}
	if checkpoint.LastEdge != nil {
		if checkpoint.LastEdge.To != checkpoint.CurrentNodeID {
			return fmt.Errorf("%w: checkpoint last edge does not reach current node", ErrBindingMismatch)
		}
		matched := false
		for _, edge := range def.Edges {
			if edge.From == checkpoint.LastEdge.From && edge.To == checkpoint.LastEdge.To && edge.Kind == checkpoint.LastEdge.Kind && edge.Predicate == checkpoint.LastEdge.Predicate {
				matched = true
				break
			}
		}
		if !matched {
			return fmt.Errorf("%w: checkpoint last edge is not in frozen definition", ErrBindingMismatch)
		}
	}
	return nil
}

func transitionFor(previous, next contract.Checkpoint, outcome OutcomeCode) contract.Transition {
	transition := contract.Transition{ID: contract.TransitionID(next.Key, next.Revision), Key: next.Key, Revision: next.Revision, From: previous.Status, To: next.Status, SegmentID: next.SegmentID, CurrentNodeID: next.CurrentNodeID, AttemptID: next.AttemptID, OutcomeCode: string(outcome)}
	if previous.CurrentNodeID != "" {
		transition.SourceSegmentID, transition.SourceHostGeneration = previous.SegmentID, previous.HostGeneration
		transition.SourceNodeID, transition.SourceAttemptID = previous.CurrentNodeID, previous.AttemptID
	}
	if outcome == OutcomeEdgeSelected || outcome == OutcomeErrorEdge {
		transition.Edge = cloneEdge(next.LastEdge)
	}
	return transition
}
func moveTo(checkpoint contract.Checkpoint, edge contract.EdgeSpec) contract.Checkpoint {
	next := contract.CloneCheckpoint(checkpoint)
	next.Status, next.CurrentNodeID, next.Attempt, next.AttemptID, next.FailureCode, next.ResolvedApproval = contract.CheckpointReady, edge.To, 0, "", "", nil
	next.LastEdge = &contract.EdgeEvidence{From: edge.From, To: edge.To, Kind: edge.Kind, Predicate: edge.Predicate}
	return next
}
func errorEdge(def contract.Definition, from string) (contract.EdgeSpec, bool) {
	for _, edge := range def.Edges {
		if edge.From == from && edge.Kind == contract.EdgeError {
			return edge, true
		}
	}
	return contract.EdgeSpec{}, false
}
func findNode(def contract.Definition, id string) (contract.NodeSpec, bool) {
	for _, node := range def.Nodes {
		if node.ID == id {
			return node, true
		}
	}
	return contract.NodeSpec{}, false
}
func attempts(spec contract.NodeSpec) int {
	if spec.Retry.MaxAttempts == 0 {
		return 1
	}
	return spec.Retry.MaxAttempts
}
func retryable(err error) bool {
	var retry RetryableError
	return errors.As(err, &retry) && retry.Retryable()
}
func asApprovalPending(err error) (*ApprovalPendingError, bool) {
	var pending *ApprovalPendingError
	return pending, errors.As(err, &pending)
}
func failureCode(err error) OutcomeCode {
	if errors.Is(err, context.DeadlineExceeded) {
		return OutcomeNodeTimeout
	}
	return OutcomeNodeFailed
}
func terminalError(checkpoint contract.Checkpoint) error {
	switch checkpoint.Status {
	case contract.CheckpointUnknown:
		return ErrUnknownOutcome
	case contract.CheckpointFailed:
		return fmt.Errorf("%w: %s", ErrExecutionFailed, checkpoint.FailureCode)
	case contract.CheckpointCancelled:
		return context.Canceled
	default:
		return nil
	}
}
func (e *Executor) emit(ctx context.Context, event Event) {
	if e.observer == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), observerTimeout)
	defer cancel()
	_ = safeObserve(e.observer, bounded, event)
}
func eventFor(kind EventKind, def contract.Definition, checkpoint contract.Checkpoint, code OutcomeCode) Event {
	spec, _ := findNode(def, checkpoint.CurrentNodeID)
	return eventForAttempt(kind, def, checkpoint, spec, checkpoint.AttemptID, 0, contract.ContextPlan{}, code)
}
func eventForAttempt(kind EventKind, def contract.Definition, checkpoint contract.Checkpoint, spec contract.NodeSpec, attempt string, duration time.Duration, plan contract.ContextPlan, code OutcomeCode) Event {
	return Event{Kind: kind, GraphID: def.ID, DefinitionRevision: checkpoint.DefinitionRevision, NodeID: spec.ID, NodeKind: spec.Kind, Status: checkpoint.Status, CheckpointRevision: checkpoint.Revision, Step: checkpoint.Steps, Attempt: checkpoint.Attempt, AttemptID: attempt, TenantID: checkpoint.Key.TenantID, SessionID: checkpoint.Key.SessionID, RunID: checkpoint.Key.RunID, SegmentID: checkpoint.SegmentID, Duration: duration, ContextPlanRevision: plan.PlanRevision, ContextEstimatedTokens: contextEstimate(plan), OutcomeCode: code}
}
func edgeEvent(def contract.Definition, checkpoint contract.Checkpoint, spec contract.NodeSpec, attempt string, duration time.Duration, plan contract.ContextPlan, edge contract.EdgeSpec) Event {
	event := eventForAttempt(EventNodeEnd, def, checkpoint, spec, attempt, duration, plan, OutcomeEdgeSelected)
	event.EdgeKind = edge.Kind
	return event
}
func stableAttemptID(key contract.CheckpointKey, nodeID string, attempt int, revision uint64) string {
	sum := sha256.Sum256([]byte(key.String() + "\x00" + nodeID + "\x00" + fmt.Sprintf("%d\x00%d", attempt, revision)))
	return "a-" + hex.EncodeToString(sum[:])
}
func durableContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), durableOperationTimeout)
}
func (e *Executor) verifyLease(ctx context.Context, request LeaseRequest) error {
	if e.lease == nil {
		return ErrLeaseUnavailable
	}
	if err := safeLeaseVerify(e.lease, ctx, request); err != nil {
		return fmt.Errorf("%w: %w", ErrLeaseUnavailable, err)
	}
	return nil
}

func safeLeaseVerify(lease SegmentLease, ctx context.Context, request LeaseRequest) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrLeasePanic
		}
	}()
	return lease.Verify(ctx, request)
}
func (e *Executor) releaseLease(ctx context.Context, request LeaseRequest) error {
	if e.lease == nil {
		return ErrLeaseUnavailable
	}
	bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), durableOperationTimeout)
	defer cancel()
	if err := safeLeaseRelease(e.lease, bounded, request); err != nil {
		return fmt.Errorf("%w: %w", ErrLeaseUnavailable, err)
	}
	return nil
}

func safeLeaseRelease(lease SegmentLease, ctx context.Context, request LeaseRequest) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrLeasePanic
		}
	}()
	return lease.Release(ctx, request)
}
func (e *Executor) plan(ctx context.Context, checkpoint contract.Checkpoint, spec contract.NodeSpec) (contract.ContextPlan, error) {
	if e.planner == nil {
		return contract.ContextPlan{}, ErrContextUnavailable
	}
	state := projectState(checkpoint.State, spec.ContextView.StateFields)
	plan, err := safePlan(e.planner, ctx, ContextRequest{Key: checkpoint.Key, DefinitionRevision: checkpoint.DefinitionRevision, NodeID: spec.ID, View: spec.ContextView.Clone(), Budget: spec.ContextBudget.Clone(), State: state, PendingApprovalID: checkpoint.PendingApprovalID, ResolvedApproval: cloneApprovalEvidence(checkpoint.ResolvedApproval)})
	if err != nil {
		return contract.ContextPlan{}, err
	}
	if err := contract.ValidateContextPlan(plan); err != nil || plan.DefinitionRevision != checkpoint.DefinitionRevision || !reflect.DeepEqual(plan.View, spec.ContextView) || !reflect.DeepEqual(plan.Budget, spec.ContextBudget) {
		return contract.ContextPlan{}, ErrContextUnavailable
	}
	return plan.Clone(), nil
}

func safePlan(planner ContextPlanner, ctx context.Context, request ContextRequest) (plan contract.ContextPlan, err error) {
	defer func() {
		if recover() != nil {
			plan = contract.ContextPlan{}
			err = ErrPlannerPanic
		}
	}()
	return planner.Plan(ctx, request)
}

func safeSandboxAuthorize(authorizer SandboxAuthorizer, ctx context.Context, request SandboxRequest) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrSandboxPanic
		}
	}()
	return authorizer.Authorize(ctx, request)
}

func safeNodeExecute(node Node, ctx context.Context, request NodeRequest) (result NodeResult, err error) {
	defer func() {
		if recover() != nil {
			result = NodeResult{}
			err = ErrNodePanic
		}
	}()
	return node.Execute(ctx, request)
}

func safeReduce(reducer Reducer, state contract.State, patch contract.StatePatch) (next contract.State, err error) {
	defer func() {
		if recover() != nil {
			next = nil
			err = ErrReducerPanic
		}
	}()
	return reducer.Reduce(state, patch)
}

func safeEvaluate(predicate Predicate, ctx context.Context, state contract.State) (matched bool, err error) {
	defer func() {
		if recover() != nil {
			matched = false
			err = ErrPredicatePanic
		}
	}()
	return predicate.Evaluate(ctx, state)
}

func safeObserve(observer Observer, ctx context.Context, event Event) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrObserverPanic
		}
	}()
	return observer.Observe(ctx, event)
}

func cloneApprovalEvidence(evidence *contract.ApprovalEvidence) *contract.ApprovalEvidence {
	if evidence == nil {
		return nil
	}
	copyOf := *evidence
	return &copyOf
}

func (e *Executor) authorizeApproval(ctx context.Context, checkpoint contract.Checkpoint, request ResumeRequest) error {
	if e.approval == nil {
		return ErrApprovalUnavailable
	}
	if err := request.Decision.Validate(); err != nil {
		return err
	}
	if request.Decision.Key != checkpoint.Key || request.Decision.TenantID != checkpoint.Key.TenantID ||
		request.Decision.ApprovalID != checkpoint.PendingApprovalID || request.Decision.ApprovalID != request.ApprovalID ||
		request.Decision.Revision != checkpoint.Revision || request.Decision.SourceSegmentID != checkpoint.SegmentID {
		return fmt.Errorf("%w: decision does not match suspended checkpoint", ErrInvalidApproval)
	}
	authorization := ApprovalAuthorizationRequest{
		Key: checkpoint.Key, PendingApprovalID: checkpoint.PendingApprovalID, CurrentRevision: checkpoint.Revision,
		SegmentID: request.SegmentID, HostGeneration: request.HostGeneration, Decision: request.Decision,
	}
	if err := safeApprovalAuthorize(e.approval, ctx, authorization); err != nil {
		if errors.Is(err, ErrApprovalPanic) {
			return err
		}
		return fmt.Errorf("%w: %w", ErrApprovalAuthorizationFail, err)
	}
	return nil
}

func safeApprovalAuthorize(authorizer ApprovalAuthorizer, ctx context.Context, request ApprovalAuthorizationRequest) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrApprovalPanic
		}
	}()
	return authorizer.Authorize(ctx, request)
}
func projectState(input contract.State, fields []string) contract.State {
	output := make(contract.State, len(fields))
	for _, field := range fields {
		if raw, ok := input[field]; ok {
			output[field] = append([]byte(nil), raw...)
		}
	}
	return output
}
func contextEstimate(plan contract.ContextPlan) int64 {
	var total int64
	for _, source := range plan.Sources {
		total += source.EstimatedTokens
	}
	return total
}
func cloneState(input contract.State) contract.State {
	output := make(contract.State, len(input))
	for key, raw := range input {
		output[key] = append([]byte(nil), raw...)
	}
	return output
}
func clonePatch(input contract.StatePatch) contract.StatePatch {
	output := contract.StatePatch{Set: make(map[string]json.RawMessage, len(input.Set)), Delete: append([]string(nil), input.Delete...)}
	for key, raw := range input.Set {
		output.Set[key] = append([]byte(nil), raw...)
	}
	return output
}
func cloneNodeSpec(input contract.NodeSpec) contract.NodeSpec {
	output := input
	output.Config = append([]byte(nil), input.Config...)
	output.ContextView = input.ContextView.Clone()
	output.ContextBudget = input.ContextBudget.Clone()
	return output
}
func cloneEdge(input *contract.EdgeEvidence) *contract.EdgeEvidence {
	if input == nil {
		return nil
	}
	output := *input
	return &output
}
