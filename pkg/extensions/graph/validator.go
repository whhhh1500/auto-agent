package graph

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

var ErrInvalidDefinition = errors.New("invalid graph definition")

// ValidatedDefinition is immutable by construction. Its fields are private:
// callers obtain a usable value only from ValidateDefinition, while a zero
// value safely exposes no definition or revision.
type ValidatedDefinition struct {
	definition Definition
	revision   string
}

func (d *ValidatedDefinition) Clone() *ValidatedDefinition {
	if d == nil {
		return nil
	}
	return &ValidatedDefinition{definition: cloneDefinition(d.definition), revision: d.revision}
}

func (d *ValidatedDefinition) Definition() Definition {
	if d == nil {
		return Definition{}
	}
	return cloneDefinition(d.definition)
}

func (d *ValidatedDefinition) Revision() string {
	if d == nil {
		return ""
	}
	return d.revision
}

// ValidateDefinition canonicalizes and freezes a Graph-G0 Definition. It has
// no execution side effects and never resolves an implementation.
func ValidateDefinition(def Definition, nodeKinds NodeKindRegistry, reducers ReducerRegistry, predicates PredicateRegistry) (*ValidatedDefinition, error) {
	if err := preflightDefinition(def); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidDefinition, err)
	}
	// A value copy alone still aliases slices, maps, and RawMessage backing
	// arrays. Clone before canonical sorting so validation never mutates caller
	// owned input.
	def = cloneDefinition(def)
	if err := validateDefinition(&def, nodeKinds, reducers, predicates); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidDefinition, err)
	}
	canonicalizeDefinition(&def)
	revision, err := definitionRevision(def)
	if err != nil {
		return nil, fmt.Errorf("%w: revision: %v", ErrInvalidDefinition, err)
	}
	if def.Revision != "" && def.Revision != revision {
		return nil, fmt.Errorf("%w: supplied revision does not match canonical definition", ErrInvalidDefinition)
	}
	def.Revision = revision
	return &ValidatedDefinition{definition: cloneDefinition(def), revision: revision}, nil
}

// preflightDefinition performs only count, length, and rough aggregate checks
// before cloneDefinition allocates maps or copies raw JSON. The exact encoded
// definition limit remains enforced after canonicalization.
func preflightDefinition(def Definition) error {
	if len(def.Nodes) == 0 || len(def.Nodes) > MaxNodes {
		return fmt.Errorf("node count must be between 1 and %d", MaxNodes)
	}
	if len(def.Edges) > MaxEdges {
		return fmt.Errorf("edge count exceeds %d", MaxEdges)
	}
	if len(def.State.Fields) > MaxStateFields || len(def.Redaction.StateFields) > MaxStateFields {
		return fmt.Errorf("state or redaction field count exceeds %d", MaxStateFields)
	}
	size := int64(0)
	add := func(amount int) bool { return addBoundedBytes(&size, amount, MaxDefinitionBytes) }
	if !add(len(def.ID)) || !add(len(def.Version)) || !add(len(def.Revision)) || !add(len(def.EntryNode)) || !add(len(def.State.Reducer)) || !add(len(def.State.ReducerVersion)) {
		return fmt.Errorf("rough definition size exceeds %d bytes", MaxDefinitionBytes)
	}
	for _, node := range def.Nodes {
		if len(node.Config) > MaxNodeConfigBytes {
			return fmt.Errorf("node config exceeds %d bytes", MaxNodeConfigBytes)
		}
		if len(node.ContextView.StateFields) > MaxContextStateFields || len(node.ContextView.Layers) > MaxContextViewLayers || len(node.ContextBudget.LayerBudgets) > MaxContextViewLayers {
			return fmt.Errorf("node context selection exceeds its bounded contract")
		}
		if !add(len(node.ID)) || !add(len(node.Kind)) || !add(len(node.KindVersion)) || !add(len(node.Config)) || !add(64) {
			return fmt.Errorf("rough definition size exceeds %d bytes", MaxDefinitionBytes)
		}
		for _, field := range node.ContextView.StateFields {
			if !add(len(field)) {
				return fmt.Errorf("rough definition size exceeds %d bytes", MaxDefinitionBytes)
			}
		}
		for _, layer := range node.ContextView.Layers {
			if !add(len(layer)) {
				return fmt.Errorf("rough definition size exceeds %d bytes", MaxDefinitionBytes)
			}
		}
		for layer := range node.ContextBudget.LayerBudgets {
			if !add(len(layer)) || !add(24) {
				return fmt.Errorf("rough definition size exceeds %d bytes", MaxDefinitionBytes)
			}
		}
	}
	for _, edge := range def.Edges {
		if !add(len(edge.From)) || !add(len(edge.To)) || !add(len(edge.Kind)) || !add(len(edge.Predicate)) || !add(len(edge.PredicateVersion)) || !add(16) {
			return fmt.Errorf("rough definition size exceeds %d bytes", MaxDefinitionBytes)
		}
	}
	for _, field := range def.State.Fields {
		if !add(len(field.Name)) || !add(len(field.Type)) || !add(24) {
			return fmt.Errorf("rough definition size exceeds %d bytes", MaxDefinitionBytes)
		}
	}
	for _, field := range def.Redaction.StateFields {
		if !add(len(field)) {
			return fmt.Errorf("rough definition size exceeds %d bytes", MaxDefinitionBytes)
		}
	}
	return nil
}

func validateDefinition(def *Definition, nodeKinds NodeKindRegistry, reducers ReducerRegistry, predicates PredicateRegistry) error {
	if err := validateIdentifier(def.ID, MaxGraphIDBytes, "graph id"); err != nil {
		return err
	}
	if err := validateIdentifier(def.Version, MaxGraphVersionBytes, "graph version"); err != nil {
		return err
	}
	if def.Revision != "" && (len(def.Revision) != sha256.Size*2 || !isLowerHex(def.Revision)) {
		return fmt.Errorf("graph revision is not a SHA-256 hex string")
	}
	if err := validateIdentifier(def.EntryNode, MaxNodeIDBytes, "entry node"); err != nil {
		return err
	}
	if len(def.Nodes) == 0 || len(def.Nodes) > MaxNodes {
		return fmt.Errorf("node count must be between 1 and %d", MaxNodes)
	}
	if len(def.Edges) > MaxEdges {
		return fmt.Errorf("edge count exceeds %d", MaxEdges)
	}
	if err := validateLimits(def.Limits); err != nil {
		return err
	}
	fields, err := validateStateSchema(def.State, reducers)
	if err != nil {
		return err
	}
	if err := validateRedaction(def.Redaction, fields); err != nil {
		return err
	}

	nodes := make(map[string]NodeSpec, len(def.Nodes))
	for index, node := range def.Nodes {
		if err := validateNode(node, fields, nodeKinds); err != nil {
			return fmt.Errorf("node %d: %w", index, err)
		}
		if _, exists := nodes[node.ID]; exists {
			return fmt.Errorf("node %q is duplicate", node.ID)
		}
		nodes[node.ID] = node
	}
	if _, exists := nodes[def.EntryNode]; !exists {
		return fmt.Errorf("entry node %q does not exist", def.EntryNode)
	}

	successors := make(map[string][]string, len(nodes))
	allSuccessors := make(map[string][]string, len(nodes))
	edgeKinds := make(map[string]map[EdgeKind]struct{}, len(nodes))
	predicatesByFrom := make(map[string]map[string]struct{}, len(nodes))
	for index, edge := range def.Edges {
		if err := validateEdge(edge, nodes, predicates); err != nil {
			return fmt.Errorf("edge %d: %w", index, err)
		}
		if edgeKinds[edge.From] == nil {
			edgeKinds[edge.From] = make(map[EdgeKind]struct{})
		}
		if edge.Kind == EdgeDefault || edge.Kind == EdgeError {
			if _, duplicate := edgeKinds[edge.From][edge.Kind]; duplicate {
				return fmt.Errorf("node %q has more than one %s edge", edge.From, edge.Kind)
			}
			edgeKinds[edge.From][edge.Kind] = struct{}{}
		}
		if edge.Kind == EdgeConditional {
			if predicatesByFrom[edge.From] == nil {
				predicatesByFrom[edge.From] = make(map[string]struct{})
			}
			if _, duplicate := predicatesByFrom[edge.From][edge.Predicate]; duplicate {
				return fmt.Errorf("node %q repeats conditional predicate %q", edge.From, edge.Predicate)
			}
			predicatesByFrom[edge.From][edge.Predicate] = struct{}{}
		}
		allSuccessors[edge.From] = append(allSuccessors[edge.From], edge.To)
		if edge.Kind != EdgeError {
			successors[edge.From] = append(successors[edge.From], edge.To)
		}
	}
	for id, node := range nodes {
		outgoing := len(allSuccessors[id])
		if node.Terminal && outgoing != 0 {
			return fmt.Errorf("terminal node %q has outgoing edges", id)
		}
		if !node.Terminal && len(successors[id]) == 0 {
			return fmt.Errorf("non-terminal node %q has no success edge", id)
		}
	}
	if err := validateReachability(def.EntryNode, nodes, allSuccessors); err != nil {
		return err
	}
	if err := validateTerminalPaths(nodes, successors); err != nil {
		return err
	}
	if graphHasCycle(nodes, allSuccessors) && def.Limits.MaxVisitsPerNode <= 0 {
		return fmt.Errorf("cyclic graph requires a positive max_visits_per_node")
	}
	return nil
}

func validateNode(node NodeSpec, fields map[string]StateField, kinds NodeKindRegistry) error {
	if err := validateIdentifier(node.ID, MaxNodeIDBytes, "node id"); err != nil {
		return err
	}
	if err := validateIdentifier(node.Kind, MaxNodeKindBytes, "node kind"); err != nil {
		return err
	}
	metadata, exists := kinds.Resolve(node.Kind)
	if !exists {
		return fmt.Errorf("node kind %q is not registered", node.Kind)
	}
	if err := validateIdentifier(node.KindVersion, MaxGraphVersionBytes, "node kind version"); err != nil {
		return err
	}
	if metadata.Version != node.KindVersion {
		return fmt.Errorf("node kind %q version %q does not match registered version %q", node.Kind, node.KindVersion, metadata.Version)
	}
	if len(node.Config) > MaxNodeConfigBytes {
		return fmt.Errorf("node config exceeds %d bytes", MaxNodeConfigBytes)
	}
	if len(node.Config) > 0 {
		if _, err := canonicalJSON(node.Config); err != nil {
			return fmt.Errorf("node config: %w", err)
		}
	}
	if err := validateContext(node.ContextView, node.ContextBudget, fields); err != nil {
		return err
	}
	if node.Retry.MaxAttempts < 0 || node.Retry.MaxAttempts > MaxRetryAttempts || node.Retry.Backoff < 0 || node.Retry.Backoff > MaxNodeTimeout {
		return fmt.Errorf("retry policy is out of bounds")
	}
	if node.Timeout <= 0 || node.Timeout > MaxNodeTimeout {
		return fmt.Errorf("timeout must be positive and at most %s", MaxNodeTimeout)
	}
	if node.ApprovalDeniedPolicy != "" && node.ApprovalDeniedPolicy != ApprovalDeniedFail && node.ApprovalDeniedPolicy != ApprovalDeniedResumeWithDecision {
		return fmt.Errorf("invalid approval denied policy")
	}
	return nil
}

func validateEdge(edge EdgeSpec, nodes map[string]NodeSpec, predicates PredicateRegistry) error {
	if err := validateIdentifier(edge.From, MaxNodeIDBytes, "edge from"); err != nil {
		return err
	}
	if err := validateIdentifier(edge.To, MaxNodeIDBytes, "edge to"); err != nil {
		return err
	}
	if _, exists := nodes[edge.From]; !exists {
		return fmt.Errorf("source node %q does not exist", edge.From)
	}
	if _, exists := nodes[edge.To]; !exists {
		return fmt.Errorf("target node %q does not exist", edge.To)
	}
	switch edge.Kind {
	case EdgeDefault, EdgeError:
		if edge.Predicate != "" || edge.PredicateVersion != "" {
			return fmt.Errorf("%s edge cannot name a predicate or predicate version", edge.Kind)
		}
	case EdgeConditional:
		if err := validateIdentifier(edge.Predicate, MaxNodeKindBytes, "edge predicate"); err != nil {
			return err
		}
		if err := validateIdentifier(edge.PredicateVersion, MaxGraphVersionBytes, "edge predicate version"); err != nil {
			return err
		}
		metadata, exists := predicates.Resolve(edge.Predicate)
		if !exists {
			return fmt.Errorf("predicate %q is not registered", edge.Predicate)
		}
		if metadata.Version != edge.PredicateVersion {
			return fmt.Errorf("predicate %q version %q does not match registered version %q", edge.Predicate, edge.PredicateVersion, metadata.Version)
		}
	default:
		return fmt.Errorf("edge kind %q is invalid", edge.Kind)
	}
	return nil
}

func validateStateSchema(schema StateSchema, reducers ReducerRegistry) (map[string]StateField, error) {
	if err := validateIdentifier(schema.Reducer, MaxNodeKindBytes, "state reducer"); err != nil {
		return nil, err
	}
	if err := validateIdentifier(schema.ReducerVersion, MaxGraphVersionBytes, "state reducer version"); err != nil {
		return nil, err
	}
	metadata, exists := reducers.Resolve(schema.Reducer)
	if !exists {
		return nil, fmt.Errorf("state reducer %q is not registered", schema.Reducer)
	}
	if metadata.Version != schema.ReducerVersion {
		return nil, fmt.Errorf("state reducer %q version %q does not match registered version %q", schema.Reducer, schema.ReducerVersion, metadata.Version)
	}
	if len(schema.Fields) > MaxStateFields {
		return nil, fmt.Errorf("state field count exceeds %d", MaxStateFields)
	}
	fields := make(map[string]StateField, len(schema.Fields))
	for _, field := range schema.Fields {
		if err := validateIdentifier(field.Name, MaxNodeIDBytes, "state field"); err != nil {
			return nil, err
		}
		if _, exists := fields[field.Name]; exists {
			return nil, fmt.Errorf("state field %q is duplicate", field.Name)
		}
		switch field.Type {
		case StateString, StateNumber, StateBoolean, StateObject, StateArray:
		default:
			return nil, fmt.Errorf("state field %q has invalid type %q", field.Name, field.Type)
		}
		if field.MaxBytes <= 0 || field.MaxBytes > MaxStateFieldBytes {
			return nil, fmt.Errorf("state field %q max_bytes is out of bounds", field.Name)
		}
		fields[field.Name] = field
	}
	return fields, nil
}

func validateRedaction(redaction RedactionSchema, fields map[string]StateField) error {
	if len(redaction.StateFields) > MaxStateFields {
		return fmt.Errorf("redaction field count exceeds %d", MaxStateFields)
	}
	seen := make(map[string]struct{}, len(redaction.StateFields))
	for _, field := range redaction.StateFields {
		if err := validateIdentifier(field, MaxNodeIDBytes, "redacted state field"); err != nil {
			return err
		}
		if _, exists := fields[field]; !exists {
			return fmt.Errorf("redacted state field %q is not declared", field)
		}
		if _, duplicate := seen[field]; duplicate {
			return fmt.Errorf("redacted state field %q is duplicate", field)
		}
		seen[field] = struct{}{}
	}
	for name, field := range fields {
		if field.Sensitive {
			if _, listed := seen[name]; !listed {
				return fmt.Errorf("sensitive state field %q must be redacted", name)
			}
		}
	}
	return nil
}

func validateContext(view ContextView, budget ContextBudget, fields map[string]StateField) error {
	if err := ValidateContextView(view); err != nil {
		return fmt.Errorf("context view: %w", err)
	}
	if err := ValidateContextBudget(budget); err != nil {
		return fmt.Errorf("context budget: %w", err)
	}
	currentInput := false
	for _, layer := range view.Layers {
		if layer == LayerCurrentInput {
			currentInput = true
			break
		}
	}
	if !currentInput || budget.LayerBudgets[LayerCurrentInput] <= 0 {
		return fmt.Errorf("context view requires current_input with a positive budget")
	}
	selected := make(map[string]struct{}, len(view.StateFields))
	for _, name := range view.StateFields {
		if err := validateIdentifier(name, MaxNodeIDBytes, "context required state field"); err != nil {
			return err
		}
		field, exists := fields[name]
		if !exists || !field.Required {
			if !exists {
				return fmt.Errorf("context state field %q is not declared", name)
			}
		}
		selected[name] = struct{}{}
	}
	for name, field := range fields {
		if field.Required {
			if _, exists := selected[name]; !exists {
				return fmt.Errorf("context view must include required state field %q", name)
			}
		}
	}
	return nil
}

func validateLimits(limits Limits) error {
	if limits.MaxSteps <= 0 || limits.MaxSteps > MaxSteps {
		return fmt.Errorf("max_steps must be between 1 and %d", MaxSteps)
	}
	if limits.MaxVisitsPerNode < 0 || limits.MaxVisitsPerNode > MaxVisitsPerNode {
		return fmt.Errorf("max_visits_per_node is out of bounds")
	}
	return nil
}

func validateIdentifier(value string, maximum int, name string) error {
	if value == "" || len(value) > maximum || !utf8.ValidString(value) || strings.TrimSpace(value) != value {
		return fmt.Errorf("%s is invalid", name)
	}
	for _, r := range value {
		if unicode.IsControl(r) || unicode.IsSpace(r) {
			return fmt.Errorf("%s contains a control or whitespace character", name)
		}
	}
	return nil
}

func validateReachability(entry string, nodes map[string]NodeSpec, edges map[string][]string) error {
	seen := map[string]bool{entry: true}
	queue := []string{entry}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		for _, next := range edges[current] {
			if !seen[next] {
				seen[next] = true
				queue = append(queue, next)
			}
		}
	}
	for id := range nodes {
		if !seen[id] {
			return fmt.Errorf("node %q is unreachable from entry", id)
		}
	}
	return nil
}

func validateTerminalPaths(nodes map[string]NodeSpec, edges map[string][]string) error {
	terminal := make(map[string]bool, len(nodes))
	reverse := make(map[string][]string, len(nodes))
	for id, node := range nodes {
		if node.Terminal {
			terminal[id] = true
		}
	}
	if len(terminal) == 0 {
		return fmt.Errorf("graph has no terminal node")
	}
	for from, targets := range edges {
		for _, to := range targets {
			reverse[to] = append(reverse[to], from)
		}
	}
	canReach := make(map[string]bool, len(nodes))
	queue := make([]string, 0, len(terminal))
	for id := range terminal {
		canReach[id] = true
		queue = append(queue, id)
	}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		for _, previous := range reverse[current] {
			if !canReach[previous] {
				canReach[previous] = true
				queue = append(queue, previous)
			}
		}
	}
	for id, node := range nodes {
		if !node.Terminal && !canReach[id] {
			return fmt.Errorf("non-terminal node %q has no success path to a terminal", id)
		}
	}
	return nil
}

func graphHasCycle(nodes map[string]NodeSpec, edges map[string][]string) bool {
	color := make(map[string]uint8, len(nodes))
	var visit func(string) bool
	visit = func(id string) bool {
		color[id] = 1
		for _, next := range edges[id] {
			if color[next] == 1 || (color[next] == 0 && visit(next)) {
				return true
			}
		}
		color[id] = 2
		return false
	}
	for id := range nodes {
		if color[id] == 0 && visit(id) {
			return true
		}
	}
	return false
}

func canonicalizeDefinition(def *Definition) {
	for index := range def.Nodes {
		if def.Nodes[index].ApprovalDeniedPolicy == ApprovalDeniedFail {
			def.Nodes[index].ApprovalDeniedPolicy = ""
		}
		if len(def.Nodes[index].Config) > 0 {
			def.Nodes[index].Config, _ = canonicalJSON(def.Nodes[index].Config)
		}
		sort.Strings(def.Nodes[index].ContextView.StateFields)
		sort.Slice(def.Nodes[index].ContextView.Layers, func(i, j int) bool {
			return def.Nodes[index].ContextView.Layers[i] < def.Nodes[index].ContextView.Layers[j]
		})
	}
	sort.Slice(def.Nodes, func(i, j int) bool { return def.Nodes[i].ID < def.Nodes[j].ID })
	sort.Slice(def.Edges, func(i, j int) bool {
		left, right := def.Edges[i], def.Edges[j]
		return left.From+"\x00"+string(left.Kind)+"\x00"+left.Predicate+"\x00"+left.PredicateVersion+"\x00"+left.To < right.From+"\x00"+string(right.Kind)+"\x00"+right.Predicate+"\x00"+right.PredicateVersion+"\x00"+right.To
	})
	sort.Slice(def.State.Fields, func(i, j int) bool { return def.State.Fields[i].Name < def.State.Fields[j].Name })
	sort.Strings(def.Redaction.StateFields)
}

func definitionRevision(def Definition) (string, error) {
	def.Revision = ""
	encoded, err := json.Marshal(def)
	if err != nil || len(encoded) > MaxDefinitionBytes {
		if err != nil {
			return "", err
		}
		return "", fmt.Errorf("canonical definition exceeds %d bytes", MaxDefinitionBytes)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func canonicalJSON(raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) == 0 || !utf8.Valid(raw) || !json.Valid(raw) {
		return nil, errors.New("invalid JSON")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	if decoder.More() {
		return nil, errors.New("trailing JSON")
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(encoded), nil
}

func cloneDefinition(def Definition) Definition {
	copyOf := def
	copyOf.Nodes = append([]NodeSpec(nil), def.Nodes...)
	for index := range copyOf.Nodes {
		copyOf.Nodes[index].Config = append(json.RawMessage(nil), def.Nodes[index].Config...)
		copyOf.Nodes[index].ContextView = def.Nodes[index].ContextView.Clone()
		copyOf.Nodes[index].ContextBudget = def.Nodes[index].ContextBudget.Clone()
	}
	copyOf.Edges = append([]EdgeSpec(nil), def.Edges...)
	copyOf.State.Fields = append([]StateField(nil), def.State.Fields...)
	copyOf.Redaction.StateFields = append([]string(nil), def.Redaction.StateFields...)
	return copyOf
}

func isLowerHex(value string) bool {
	for _, c := range value {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
