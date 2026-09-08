// Package graph executes one bounded, sequential Graph definition against a
// Graph checkpoint Store. It intentionally depends only on the public Graph
// contract: provider, core, server, and storage SDK types do not cross this
// boundary.
package graph

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	contract "github.com/whhhh1500/auto-agent/pkg/extensions/graph"
)

var ErrBindingMismatch = errors.New("graph implementation binding mismatch")

// Node receives defensive copies of its declared spec and state. Implementors
// must treat the stable AttemptID as an idempotency key for external work.
type Node interface {
	Execute(context.Context, NodeRequest) (NodeResult, error)
}

type NodeRequest struct {
	Spec             contract.NodeSpec
	State            contract.State
	Context          contract.ContextPlan
	ResolvedApproval *contract.ApprovalEvidence
	AttemptID        string
	SegmentID        string
}

type NodeResult struct {
	Patch contract.StatePatch
}

type Predicate interface {
	Evaluate(context.Context, contract.State) (bool, error)
}

type Reducer interface {
	Reduce(contract.State, contract.StatePatch) (contract.State, error)
}

// RetryableError opts a node failure into the definition's bounded retry
// policy. Unknown errors are never retried.
type RetryableError interface {
	error
	Retryable() bool
}

// ApprovalPendingError suspends the current stable node attempt. The approval
// payload stays outside this package; only a bounded opaque ID is persisted.
type ApprovalPendingError struct{ ApprovalID string }

func (e *ApprovalPendingError) Error() string { return "graph approval pending" }

type NodeBinding struct {
	Kind     string
	Version  string
	Revision string
	Node     Node
}

type ReducerBinding struct {
	ID       string
	Version  string
	Revision string
	Reducer  Reducer
}

type PredicateBinding struct {
	ID        string
	Version   string
	Revision  string
	Predicate Predicate
}

// Bindings is immutable after construction. The implementation objects are
// supplied by the embedding application and must themselves be immutable for a
// binding revision to be meaningful; this package copies all binding metadata
// and never exposes its maps.
type Bindings struct {
	definitionRevision     string
	compositionRevision    string
	implementationRevision string
	nodes                  map[string]NodeBinding
	reducer                ReducerBinding
	predicates             map[string]PredicateBinding
}

func BuiltinReducerBinding(revision string) ReducerBinding {
	return ReducerBinding{ID: contract.ReducerTopLevelJSONPatch, Version: "1", Revision: revision}
}

func NewBindings(definition *contract.ValidatedDefinition, compositionRevision string, nodes []NodeBinding, reducer ReducerBinding, predicates []PredicateBinding) (*Bindings, error) {
	if definition == nil || definition.Revision() == "" || !validBindingID(compositionRevision) {
		return nil, fmt.Errorf("%w: definition or composition revision is invalid", ErrBindingMismatch)
	}
	if len(nodes) == 0 || len(nodes) > contract.MaxRegistryEntries || len(predicates) > contract.MaxRegistryEntries {
		return nil, fmt.Errorf("%w: binding count is invalid", ErrBindingMismatch)
	}
	bound := &Bindings{
		definitionRevision:  definition.Revision(),
		compositionRevision: compositionRevision,
		nodes:               make(map[string]NodeBinding, len(nodes)),
		predicates:          make(map[string]PredicateBinding, len(predicates)),
	}
	for _, node := range nodes {
		if !validBindingID(node.Kind) || !validBindingID(node.Version) || !validBindingID(node.Revision) || node.Node == nil {
			return nil, fmt.Errorf("%w: invalid node binding", ErrBindingMismatch)
		}
		key := bindingKey(node.Kind, node.Version)
		if _, exists := bound.nodes[key]; exists {
			return nil, fmt.Errorf("%w: duplicate node binding %s", ErrBindingMismatch, key)
		}
		bound.nodes[key] = node
	}
	if !validBindingID(reducer.ID) || !validBindingID(reducer.Version) || !validBindingID(reducer.Revision) {
		return nil, fmt.Errorf("%w: invalid reducer binding", ErrBindingMismatch)
	}
	if reducer.ID != contract.ReducerTopLevelJSONPatch && reducer.Reducer == nil {
		return nil, fmt.Errorf("%w: custom reducer has no implementation", ErrBindingMismatch)
	}
	bound.reducer = reducer
	for _, predicate := range predicates {
		if !validBindingID(predicate.ID) || !validBindingID(predicate.Version) || !validBindingID(predicate.Revision) || predicate.Predicate == nil {
			return nil, fmt.Errorf("%w: invalid predicate binding", ErrBindingMismatch)
		}
		key := bindingKey(predicate.ID, predicate.Version)
		if _, exists := bound.predicates[key]; exists {
			return nil, fmt.Errorf("%w: duplicate predicate binding %s", ErrBindingMismatch, key)
		}
		bound.predicates[key] = predicate
	}

	def := definition.Definition()
	if reducer.ID != def.State.Reducer || reducer.Version != def.State.ReducerVersion {
		return nil, fmt.Errorf("%w: reducer contract differs from definition", ErrBindingMismatch)
	}
	for _, spec := range def.Nodes {
		if _, exists := bound.nodes[bindingKey(spec.Kind, spec.KindVersion)]; !exists {
			return nil, fmt.Errorf("%w: node kind %s@%s is unbound", ErrBindingMismatch, spec.Kind, spec.KindVersion)
		}
	}
	for _, edge := range def.Edges {
		if edge.Kind == contract.EdgeConditional {
			if _, exists := bound.predicates[bindingKey(edge.Predicate, edge.PredicateVersion)]; !exists {
				return nil, fmt.Errorf("%w: predicate %s@%s is unbound", ErrBindingMismatch, edge.Predicate, edge.PredicateVersion)
			}
		}
	}
	revision, err := implementationSetRevision(bound)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBindingMismatch, err)
	}
	bound.implementationRevision = revision
	return bound, nil
}

func (b *Bindings) DefinitionRevision() string {
	if b == nil {
		return ""
	}
	return b.definitionRevision
}
func (b *Bindings) CompositionRevision() string {
	if b == nil {
		return ""
	}
	return b.compositionRevision
}
func (b *Bindings) ImplementationRevision() string {
	if b == nil {
		return ""
	}
	return b.implementationRevision
}

func (b *Bindings) node(kind, version string) (NodeBinding, bool) {
	if b == nil {
		return NodeBinding{}, false
	}
	v, ok := b.nodes[bindingKey(kind, version)]
	return v, ok
}
func (b *Bindings) predicate(id, version string) (PredicateBinding, bool) {
	if b == nil {
		return PredicateBinding{}, false
	}
	v, ok := b.predicates[bindingKey(id, version)]
	return v, ok
}

func implementationSetRevision(bindings *Bindings) (string, error) {
	type metadata struct{ Type, ID, Version, Revision string }
	items := make([]metadata, 0, len(bindings.nodes)+len(bindings.predicates)+1)
	for _, node := range bindings.nodes {
		items = append(items, metadata{"node", node.Kind, node.Version, node.Revision})
	}
	items = append(items, metadata{"reducer", bindings.reducer.ID, bindings.reducer.Version, bindings.reducer.Revision})
	for _, predicate := range bindings.predicates {
		items = append(items, metadata{"predicate", predicate.ID, predicate.Version, predicate.Revision})
	}
	sort.Slice(items, func(i, j int) bool {
		left, right := items[i], items[j]
		return left.Type+"\x00"+left.ID+"\x00"+left.Version < right.Type+"\x00"+right.ID+"\x00"+right.Version
	})
	encoded, err := json.Marshal(items)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

func bindingKey(id, version string) string { return id + "\x00" + version }
func validBindingID(value string) bool {
	if value == "" || len(value) > contract.MaxCheckpointIDBytes || !utf8.ValidString(value) || strings.TrimSpace(value) != value {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) || unicode.IsSpace(r) {
			return false
		}
	}
	return true
}
