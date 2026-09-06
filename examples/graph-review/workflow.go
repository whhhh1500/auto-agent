// Package graphreview demonstrates a SQL-backed draft/review/finalize graph.
// It is an example assembly, separate from the server's graph-core-turn adapter.
package graphreview

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	rungraph "github.com/cc-auto-agent/harness-core/pkg/adapter/runexecutor/graph"
	"github.com/cc-auto-agent/harness-core/pkg/adapter/sql/graphcheckpoint"
	"github.com/cc-auto-agent/harness-core/pkg/adapter/sql/graphsegment"
	"github.com/cc-auto-agent/harness-core/pkg/adapter/sql/sqlkit"
	"github.com/cc-auto-agent/harness-core/pkg/core"
	execgraph "github.com/cc-auto-agent/harness-core/pkg/execution/graph"
	contract "github.com/cc-auto-agent/harness-core/pkg/extensions/graph"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
)

// DraftFunc is supplied by the product. The example never selects a model or
// performs external publication. Finalize produces a durable state value only.
type DraftFunc func(context.Context, string) (string, error)

// Reviews is an independently hosted approval authority. The application must
// bind its decisions to owner, run, attempt and draft; there is no allow-all
// default. A server embedding may compose its SQL approval/Graph adapters here.
type Reviews interface {
	core.DurableApprover
	rungraph.ApprovalDecisionResolver
	execgraph.ApprovalAuthorizer
}

type Workflow struct {
	store     *graphcheckpoint.Store
	authority *graphsegment.Authority
	reviews   Reviews
	principal core.Principal
	draft     DraftFunc
}

func New(db *sql.DB, dialect storage.SQLDialect, principal core.Principal, draft DraftFunc, reviews Reviews) (*Workflow, error) {
	if draft == nil || reviews == nil || principal.SubjectID == "" || principal.TenantID == "" || principal.Scope.Depth() == 0 {
		return nil, errors.New("draft function, review authority and valid principal required")
	}
	var d sqlkit.Dialect
	switch dialect {
	case storage.SQLDialectSQLite:
		d = sqlkit.SQLite
	case storage.SQLDialectPostgres:
		d = sqlkit.Postgres
	default:
		return nil, errors.New("unsupported SQL dialect")
	}
	store, err := graphcheckpoint.New(db, d)
	if err != nil {
		return nil, err
	}
	authority, err := graphsegment.New(db, d)
	if err != nil {
		return nil, err
	}
	return &Workflow{store: store, authority: authority, reviews: reviews, principal: principal, draft: draft}, nil
}

func Definition() (*contract.ValidatedDefinition, error) {
	view := contract.ContextView{StateFields: []string{"owner", "request", "draft", "result"}, Layers: []contract.LayerKind{contract.LayerCurrentInput, contract.LayerRequiredState}}
	budget := contract.ContextBudget{TotalTokens: 8192, LayerBudgets: map[contract.LayerKind]int64{contract.LayerCurrentInput: 4096, contract.LayerRequiredState: 4096}}
	var nodes []contract.NodeSpec
	for _, id := range []string{"draft", "review", "finalize"} {
		nodes = append(nodes, contract.NodeSpec{ID: id, Kind: "document-review", KindVersion: "1", ContextView: view, ContextBudget: budget, Timeout: time.Minute, Retry: contract.RetryPolicy{MaxAttempts: 1}, Terminal: id == "finalize"})
	}
	definition := contract.Definition{ID: "example-document-review", Version: "1", EntryNode: "draft", Nodes: nodes,
		Edges: []contract.EdgeSpec{{From: "draft", To: "review", Kind: contract.EdgeDefault}, {From: "review", To: "finalize", Kind: contract.EdgeDefault}},
		State: contract.StateSchema{Reducer: contract.ReducerTopLevelJSONPatch, ReducerVersion: "1", Fields: []contract.StateField{
			{Name: "owner", Type: contract.StateString, Required: true, MaxBytes: 8192},
			{Name: "request", Type: contract.StateString, Required: true, MaxBytes: 8192},
			{Name: "draft", Type: contract.StateString, Required: true, MaxBytes: 8192},
			{Name: "result", Type: contract.StateString, Required: true, MaxBytes: 8192},
		}}, Limits: contract.Limits{MaxSteps: 8, MaxVisitsPerNode: 2}}
	kinds, err := contract.NewNodeKindRegistry([]contract.NodeKindMetadata{{ID: "document-review", Version: "1"}})
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

func (w *Workflow) Run(ctx context.Context, key contract.CheckpointKey, input string) (execgraph.Result, error) {
	return w.execute(ctx, key, input, false)
}

// Resume reads an already committed decision from the external review authority.
func (w *Workflow) Resume(ctx context.Context, key contract.CheckpointKey) (execgraph.Result, error) {
	return w.execute(ctx, key, "", true)
}

func (w *Workflow) execute(ctx context.Context, key contract.CheckpointKey, input string, resume bool) (result execgraph.Result, resultErr error) {
	if key.TenantID != w.principal.TenantID {
		return execgraph.Result{}, errors.New("tenant mismatch")
	}
	owner, _ := json.Marshal(w.principal.SubjectID + ":" + w.principal.Scope.String())
	checkpoint, loadErr := w.store.Load(ctx, key)
	if loadErr != nil && !errors.Is(loadErr, contract.ErrCheckpointNotFound) {
		return execgraph.Result{}, loadErr
	}
	if loadErr == nil && string(checkpoint.State["owner"]) != string(owner) {
		return execgraph.Result{}, errors.New("graph owner mismatch")
	}
	var decision execgraph.ApprovalDecision
	if resume {
		if loadErr != nil {
			return execgraph.Result{}, loadErr
		}
		var err error
		decision, err = w.reviews.Resolve(ctx, w.principal, key, checkpoint.PendingApprovalID)
		if err != nil {
			return execgraph.Result{}, err
		}
	}
	grant, err := w.authority.Acquire(ctx, key)
	if err != nil {
		return execgraph.Result{}, err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resultErr = errors.Join(resultErr, grant.Lease.Release(cleanup, execgraph.LeaseRequest{Key: key, SegmentID: grant.SegmentID, HostGeneration: grant.HostGeneration}))
	}()
	definition, err := Definition()
	if err != nil {
		return execgraph.Result{}, err
	}
	bindings, err := execgraph.NewBindings(definition, "example-review-v1", []execgraph.NodeBinding{{Kind: "document-review", Version: "1", Revision: "example-review-v1", Node: &reviewNode{workflow: w, key: key}}}, execgraph.BuiltinReducerBinding("builtin-v1"), nil)
	if err != nil {
		return execgraph.Result{}, err
	}
	engine, err := execgraph.NewExecutorWithOptions(definition, bindings, w.store, execgraph.Options{Lease: grant.Lease, Planner: rungraph.ReferencePlanner{}, Approval: w.reviews})
	if err != nil {
		return execgraph.Result{}, err
	}
	if resume {
		return engine.Resume(ctx, execgraph.ResumeRequest{Key: key, SegmentID: grant.SegmentID, HostGeneration: grant.HostGeneration, ApprovalID: decision.ApprovalID, Decision: decision})
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		return execgraph.Result{}, err
	}
	return engine.Run(ctx, execgraph.StartRequest{Key: key, SegmentID: grant.SegmentID, HostGeneration: grant.HostGeneration,
		InitialState: contract.State{"owner": owner, "request": encoded, "draft": json.RawMessage(`""`), "result": json.RawMessage(`""`)}})
}

type reviewNode struct {
	workflow *Workflow
	key      contract.CheckpointKey
}

func (node *reviewNode) Execute(ctx context.Context, request execgraph.NodeRequest) (execgraph.NodeResult, error) {
	w := node.workflow
	var draft, input string
	if err := json.Unmarshal(request.State["request"], &input); err != nil {
		return execgraph.NodeResult{}, err
	}
	if err := json.Unmarshal(request.State["draft"], &draft); err != nil {
		return execgraph.NodeResult{}, err
	}
	switch request.Spec.ID {
	case "draft":
		text, err := w.draft(ctx, input)
		if err != nil {
			return execgraph.NodeResult{}, err
		}
		return patch("draft", text), nil
	case "review":
		if request.ResolvedApproval != nil && request.ResolvedApproval.Decision == string(core.ApprovalApproved) {
			return execgraph.NodeResult{}, nil
		}
		resolution, err := w.reviews.RequestApproval(ctx, core.ApprovalRequest{SessionID: node.key.SessionID, RunID: node.key.RunID, Principal: w.principal,
			ToolCall: core.ToolCall{ID: request.AttemptID, Name: "example.document.review", Args: map[string]any{"draft": draft}},
			Manifest: core.CapabilityManifest{ID: "example.document.review", Version: "1", Name: "Review document", Kind: core.KindTool, Contract: "harness.tool/v1", Idempotent: true, RequiresApproval: true, Tool: &core.ToolExposure{}}})
		if err != nil {
			return execgraph.NodeResult{}, err
		}
		return execgraph.NodeResult{}, &execgraph.ApprovalPendingError{ApprovalID: resolution.ApprovalID}
	case "finalize":
		return patch("result", draft), nil
	default:
		return execgraph.NodeResult{}, errors.New("unknown example node")
	}
}

func patch(field, value string) execgraph.NodeResult {
	encoded, _ := json.Marshal(value)
	return execgraph.NodeResult{Patch: contract.StatePatch{Set: map[string]json.RawMessage{field: encoded}}}
}
