package graph

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestValidateDefinitionCanonicalRevisionAndClone(t *testing.T) {
	first, err := ValidateDefinition(validDefinition(), testNodeKinds(t), testReducers(t), testPredicates(t))
	if err != nil {
		t.Fatal(err)
	}
	secondDef := validDefinition()
	secondDef.Nodes[0], secondDef.Nodes[1] = secondDef.Nodes[1], secondDef.Nodes[0]
	secondDef.Nodes[1].Config = json.RawMessage(` { "z" : [ 1, 2 ], "a" : true } `)
	second, err := ValidateDefinition(secondDef, testNodeKinds(t), testReducers(t), testPredicates(t))
	if err != nil {
		t.Fatal(err)
	}
	if first.Revision() != second.Revision() {
		t.Fatalf("order or JSON whitespace changed revision: %s != %s", first.Revision(), second.Revision())
	}
	definition := first.Definition()
	definition.Nodes[0].ID = "changed"
	definition.Nodes[0].ContextView.StateFields[0] = "changed"
	if got := first.Definition(); got.Nodes[0].ID == "changed" || got.Nodes[0].ContextView.StateFields[0] == "changed" {
		t.Fatal("Definition did not return a deep copy")
	}
	clone := first.Clone()
	if clone == first || clone.Revision() != first.Revision() || !reflect.DeepEqual(clone.Definition(), first.Definition()) {
		t.Fatal("Clone did not preserve an independent immutable definition")
	}
}

func TestValidateDefinitionRejectsRevisionMismatch(t *testing.T) {
	definition := validDefinition()
	definition.Revision = strings.Repeat("0", 64)
	_, err := ValidateDefinition(definition, testNodeKinds(t), testReducers(t), testPredicates(t))
	if !errors.Is(err, ErrInvalidDefinition) || !strings.Contains(err.Error(), "revision") {
		t.Fatalf("err=%v, want revision mismatch", err)
	}
}

func TestValidateDefinitionPreflightRejectsOversizeBeforeCanonicalization(t *testing.T) {
	definition := validDefinition()
	definition.Nodes[0].Config = make(json.RawMessage, MaxNodeConfigBytes+1)
	if _, err := ValidateDefinition(definition, testNodeKinds(t), testReducers(t), testPredicates(t)); !strings.Contains(err.Error(), "config exceeds") {
		t.Fatalf("err=%v, want preflight config bound", err)
	}
	definition = validDefinition()
	definition.Nodes = make([]NodeSpec, MaxNodes+1)
	if _, err := ValidateDefinition(definition, testNodeKinds(t), testReducers(t), testPredicates(t)); !strings.Contains(err.Error(), "node count") {
		t.Fatalf("err=%v, want preflight count bound", err)
	}
}

func TestValidatedDefinitionZeroValueIsSafe(t *testing.T) {
	var value ValidatedDefinition
	if value.Revision() != "" || !reflect.DeepEqual(value.Definition(), Definition{}) {
		t.Fatalf("zero validated definition leaked state: %#v", value.Definition())
	}
}

func TestDefinitionRevisionBindsRegistryVersions(t *testing.T) {
	baseline, err := ValidateDefinition(validDefinition(), testNodeKinds(t), testReducers(t), testPredicates(t))
	if err != nil {
		t.Fatal(err)
	}
	upgraded := validDefinition()
	upgraded.Nodes[0].KindVersion = "2"
	if _, err := ValidateDefinition(upgraded, testNodeKinds(t), testReducers(t), testPredicates(t)); err == nil {
		t.Fatal("node version drift accepted against old registry")
	}
	kinds, err := NewNodeKindRegistry([]NodeKindMetadata{{ID: "prompt", Version: "2"}})
	if err != nil {
		t.Fatal(err)
	}
	// Every node names its exact implementation contract version.
	upgraded.Nodes[1].KindVersion = "2"
	validated, err := ValidateDefinition(upgraded, kinds, testReducers(t), testPredicates(t))
	if err != nil {
		t.Fatal(err)
	}
	if baseline.Revision() == validated.Revision() {
		t.Fatal("kind contract version did not change definition revision")
	}

	reducerDefinition := validDefinition()
	reducerDefinition.State.Reducer, reducerDefinition.State.ReducerVersion = "custom", "1"
	firstReducers, err := NewReducerRegistry([]ReducerMetadata{{ID: "custom", Version: "1"}})
	if err != nil {
		t.Fatal(err)
	}
	firstReducer, err := ValidateDefinition(reducerDefinition, testNodeKinds(t), firstReducers, testPredicates(t))
	if err != nil {
		t.Fatal(err)
	}
	reducerDefinition.State.ReducerVersion = "2"
	if _, err := ValidateDefinition(reducerDefinition, testNodeKinds(t), firstReducers, testPredicates(t)); err == nil {
		t.Fatal("reducer version drift accepted against old registry")
	}
	secondReducers, err := NewReducerRegistry([]ReducerMetadata{{ID: "custom", Version: "2"}})
	if err != nil {
		t.Fatal(err)
	}
	secondReducer, err := ValidateDefinition(reducerDefinition, testNodeKinds(t), secondReducers, testPredicates(t))
	if err != nil || firstReducer.Revision() == secondReducer.Revision() {
		t.Fatalf("reducer version binding err=%v revisions=%q/%q", err, firstReducer.Revision(), secondReducer.Revision())
	}

	predicateDefinition := validDefinition()
	predicateDefinition.Edges = append(predicateDefinition.Edges, EdgeSpec{From: "start", To: "end", Kind: EdgeConditional, Predicate: "again", PredicateVersion: "1"})
	firstPredicate, err := ValidateDefinition(predicateDefinition, testNodeKinds(t), testReducers(t), testPredicates(t))
	if err != nil {
		t.Fatal(err)
	}
	predicateDefinition.Edges[1].PredicateVersion = "2"
	if _, err := ValidateDefinition(predicateDefinition, testNodeKinds(t), testReducers(t), testPredicates(t)); err == nil {
		t.Fatal("predicate version drift accepted against old registry")
	}
	secondPredicates, err := NewPredicateRegistry([]PredicateMetadata{{ID: "again", Version: "2"}})
	if err != nil {
		t.Fatal(err)
	}
	secondPredicate, err := ValidateDefinition(predicateDefinition, testNodeKinds(t), testReducers(t), secondPredicates)
	if err != nil || firstPredicate.Revision() == secondPredicate.Revision() {
		t.Fatalf("predicate version binding err=%v revisions=%q/%q", err, firstPredicate.Revision(), secondPredicate.Revision())
	}
}

func TestValidateDefinitionRejectsGraphSafetyViolations(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Definition)
		want string
	}{
		{"unknown kind", func(d *Definition) { d.Nodes[0].Kind = "unknown" }, "not registered"},
		{"kind version mismatch", func(d *Definition) { d.Nodes[0].KindVersion = "2" }, "version"},
		{"unknown reducer", func(d *Definition) { d.State.Reducer = "unknown" }, "not registered"},
		{"reducer version mismatch", func(d *Definition) { d.State.ReducerVersion = "2" }, "version"},
		{"unknown predicate", func(d *Definition) {
			d.Edges[0].Kind, d.Edges[0].Predicate, d.Edges[0].PredicateVersion = EdgeConditional, "unknown", "1"
		}, "not registered"},
		{"predicate version mismatch", func(d *Definition) {
			d.Edges[0].Kind, d.Edges[0].Predicate, d.Edges[0].PredicateVersion = EdgeConditional, "again", "2"
		}, "version"},
		{"duplicate node", func(d *Definition) { d.Nodes = append(d.Nodes, d.Nodes[0]) }, "duplicate"},
		{"duplicate state", func(d *Definition) { d.State.Fields = append(d.State.Fields, d.State.Fields[0]) }, "duplicate"},
		{"missing entry", func(d *Definition) { d.EntryNode = "nope" }, "does not exist"},
		{"unreachable", func(d *Definition) { d.Nodes = append(d.Nodes, terminalNode("orphan")) }, "unreachable"},
		{"terminal outgoing", func(d *Definition) { d.Edges = append(d.Edges, EdgeSpec{From: "end", To: "start", Kind: EdgeDefault}) }, "terminal"},
		{"no success path", func(d *Definition) { d.Edges[0].Kind = EdgeError }, "no success edge"},
		{"duplicate default", func(d *Definition) { d.Edges = append(d.Edges, EdgeSpec{From: "start", To: "end", Kind: EdgeDefault}) }, "more than one"},
		{"bad context field", func(d *Definition) { d.Nodes[0].ContextView.StateFields = []string{"missing"} }, "not declared"},
		{"required context omitted", func(d *Definition) {
			d.Nodes[0].ContextView.StateFields = nil
			d.Nodes[0].ContextView.Layers = []LayerKind{LayerCurrentInput}
		}, "must include required"},
		{"context budget invalid", func(d *Definition) { d.Nodes[0].ContextBudget.OutputReserve = 65 }, "context budget"},
		{"missing current input", func(d *Definition) { d.Nodes[0].ContextView.Layers = []LayerKind{LayerRequiredState} }, "current_input"},
		{"missing current input budget", func(d *Definition) { d.Nodes[0].ContextBudget.LayerBudgets[LayerCurrentInput] = 0 }, "current_input"},
		{"sensitive redaction", func(d *Definition) { d.Redaction.StateFields = nil }, "sensitive"},
		{"invalid config", func(d *Definition) { d.Nodes[0].Config = json.RawMessage(`{`) }, "node config"},
		{"timeout", func(d *Definition) { d.Nodes[0].Timeout = 0 }, "timeout"},
		{"retry", func(d *Definition) { d.Nodes[0].Retry.MaxAttempts = MaxRetryAttempts + 1 }, "retry"},
		{"control", func(d *Definition) { d.ID = "bad\u0085id" }, "control"},
		{"unicode whitespace", func(d *Definition) { d.ID = "bad\u00a0id" }, "whitespace"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			definition := validDefinition()
			test.edit(&definition)
			_, err := ValidateDefinition(definition, testNodeKinds(t), testReducers(t), testPredicates(t))
			if !errors.Is(err, ErrInvalidDefinition) || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("err=%v, want %q", err, test.want)
			}
		})
	}
}

func TestValidateDefinitionCycleRequiresVisitCapAndTerminalPath(t *testing.T) {
	definition := validDefinition()
	definition.Edges = []EdgeSpec{
		{From: "start", To: "loop", Kind: EdgeDefault},
		{From: "loop", To: "start", Kind: EdgeConditional, Predicate: "again", PredicateVersion: "1"},
		{From: "loop", To: "end", Kind: EdgeDefault},
	}
	definition.Nodes = []NodeSpec{definition.Nodes[0], node("loop", false), definition.Nodes[1]}
	definition.Limits.MaxVisitsPerNode = 0
	if _, err := ValidateDefinition(definition, testNodeKinds(t), testReducers(t), testPredicates(t)); !strings.Contains(err.Error(), "max_visits") {
		t.Fatalf("err=%v, want cyclic limit failure", err)
	}
	definition.Limits.MaxVisitsPerNode = 2
	if _, err := ValidateDefinition(definition, testNodeKinds(t), testReducers(t), testPredicates(t)); err != nil {
		t.Fatalf("bounded cycle rejected: %v", err)
	}
	definition.Edges = definition.Edges[:2]
	if _, err := ValidateDefinition(definition, testNodeKinds(t), testReducers(t), testPredicates(t)); !strings.Contains(err.Error(), "unreachable") {
		t.Fatalf("err=%v, want isolated terminal failure", err)
	}
}

func TestValidateDefinitionErrorEdgeCycleRequiresVisitCap(t *testing.T) {
	definition := validDefinition()
	definition.Nodes = []NodeSpec{definition.Nodes[0], node("recover", false), definition.Nodes[1]}
	definition.Edges = []EdgeSpec{
		{From: "start", To: "end", Kind: EdgeDefault},
		{From: "start", To: "recover", Kind: EdgeError},
		{From: "recover", To: "end", Kind: EdgeDefault},
		{From: "recover", To: "start", Kind: EdgeError},
	}
	if _, err := ValidateDefinition(definition, testNodeKinds(t), testReducers(t), testPredicates(t)); !strings.Contains(err.Error(), "max_visits") {
		t.Fatalf("error-edge cycle escaped visit bound: %v", err)
	}
	definition.Limits.MaxVisitsPerNode = 2
	if _, err := ValidateDefinition(definition, testNodeKinds(t), testReducers(t), testPredicates(t)); err != nil {
		t.Fatalf("bounded error-edge cycle rejected: %v", err)
	}
}

func validDefinition() Definition {
	return Definition{
		ID: "general", Version: "1", EntryNode: "start",
		State:     StateSchema{Reducer: ReducerTopLevelJSONPatch, ReducerVersion: "1", Fields: []StateField{{Name: "account", Type: StateString, Required: true, Sensitive: true, MaxBytes: 128}}},
		Limits:    Limits{MaxSteps: 8},
		Redaction: RedactionSchema{StateFields: []string{"account"}},
		Nodes:     []NodeSpec{node("start", false), terminalNode("end")},
		Edges:     []EdgeSpec{{From: "start", To: "end", Kind: EdgeDefault}},
	}
}

func TestApprovalDeniedFailCanonicalizesLikeLegacyDefault(t *testing.T) {
	legacy := validDefinition()
	explicit := validDefinition()
	explicit.Nodes[0].ApprovalDeniedPolicy = ApprovalDeniedFail
	left, err := ValidateDefinition(legacy, testNodeKinds(t), testReducers(t), testPredicates(t))
	if err != nil {
		t.Fatal(err)
	}
	right, err := ValidateDefinition(explicit, testNodeKinds(t), testReducers(t), testPredicates(t))
	if err != nil {
		t.Fatal(err)
	}
	if left.Revision() != right.Revision() {
		t.Fatalf("legacy revision=%s explicit failed revision=%s", left.Revision(), right.Revision())
	}
}

func node(id string, terminal bool) NodeSpec {
	return NodeSpec{
		ID: id, Kind: "prompt", KindVersion: "1", Config: json.RawMessage(`{"a":true,"z":[1,2]}`),
		ContextView:   ContextView{StateFields: []string{"account"}, Layers: []LayerKind{LayerCurrentInput, LayerRequiredState}},
		ContextBudget: ContextBudget{TotalTokens: 64, LayerBudgets: map[LayerKind]int64{LayerCurrentInput: 32, LayerRequiredState: 32}},
		Retry:         RetryPolicy{MaxAttempts: 1, Backoff: time.Millisecond}, Timeout: time.Second, Terminal: terminal,
	}
}

func terminalNode(id string) NodeSpec { return node(id, true) }

func testNodeKinds(t *testing.T) NodeKindRegistry {
	t.Helper()
	registry, err := NewNodeKindRegistry([]NodeKindMetadata{{ID: "prompt", Version: "1"}})
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

func testReducers(t *testing.T) ReducerRegistry {
	t.Helper()
	registry, err := NewReducerRegistry(nil)
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

func testPredicates(t *testing.T) PredicateRegistry {
	t.Helper()
	registry, err := NewPredicateRegistry([]PredicateMetadata{{ID: "again", Version: "1"}})
	if err != nil {
		t.Fatal(err)
	}
	return registry
}
