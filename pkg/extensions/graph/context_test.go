package graph

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestValidateContextViewIsExplicit(t *testing.T) {
	valid := ContextView{StateFields: []string{"ticket.id", "ticket.status"}, Layers: []LayerKind{LayerCurrentInput, LayerRequiredState, LayerRecentHistory}}
	if err := ValidateContextView(valid); err != nil {
		t.Fatalf("valid view: %v", err)
	}
	for name, view := range map[string]ContextView{
		"wildcard state":      {StateFields: []string{"*"}, Layers: []LayerKind{LayerRequiredState}},
		"global state":        {StateFields: []string{"global"}, Layers: []LayerKind{LayerRequiredState}},
		"wildcard layer":      {Layers: []LayerKind{"all"}},
		"duplicate layer":     {Layers: []LayerKind{LayerSummary, LayerSummary}},
		"state without layer": {StateFields: []string{"id"}, Layers: []LayerKind{LayerCurrentInput}},
	} {
		if err := ValidateContextView(view); err == nil {
			t.Errorf("%s: expected rejection", name)
		}
	}
}

func TestValidateContextBudgetArithmeticAndBounds(t *testing.T) {
	valid := ContextBudget{
		TotalTokens:        MaxContextTotalTokens,
		OutputReserve:      100,
		StablePrefixTokens: 100,
		LayerBudgets:       map[LayerKind]int64{LayerCurrentInput: MaxContextTotalTokens - 100},
	}
	if err := ValidateContextBudget(valid); err != nil {
		t.Fatalf("valid budget: %v", err)
	}
	for name, budget := range map[string]ContextBudget{
		"negative total":           {TotalTokens: -1},
		"total cap":                {TotalTokens: MaxContextTotalTokens + 1},
		"output exceeds":           {TotalTokens: 2, OutputReserve: 3},
		"stable exceeds input":     {TotalTokens: 10, OutputReserve: 8, StablePrefixTokens: 3},
		"layer exceeds input":      {TotalTokens: 10, OutputReserve: 2, LayerBudgets: map[LayerKind]int64{LayerSummary: 9}},
		"negative layer":           {TotalTokens: 10, LayerBudgets: map[LayerKind]int64{LayerSummary: -1}},
		"unknown layer":            {TotalTokens: 10, LayerBudgets: map[LayerKind]int64{"unknown": 1}},
		"large arithmetic operand": {TotalTokens: MaxContextTotalTokens, LayerBudgets: map[LayerKind]int64{LayerSummary: maxInt64ForTest()}},
	} {
		if err := ValidateContextBudget(budget); err == nil {
			t.Errorf("%s: expected rejection", name)
		}
	}
}

func TestValidateContextPlanSourceIsolationAndSummaryLineage(t *testing.T) {
	plan := baseContextPlan()
	if err := ValidateContextPlan(plan); err != nil {
		t.Fatalf("valid plan: %v", err)
	}
	for name, mutate := range map[string]func(*ContextPlan){
		"duplicate source": func(p *ContextPlan) { p.Sources = append(p.Sources, p.Sources[0].Clone()) },
		"cross layer identity": func(p *ContextPlan) {
			p.Sources = append(p.Sources, ContextSource{Layer: LayerArtifact, SourceID: "input", Revision: "r2", EstimatedTokens: 1})
		},
		"duplicate ID different revision": func(p *ContextPlan) {
			p.Sources = append(p.Sources, ContextSource{Layer: LayerCurrentInput, SourceID: "input", Revision: "r2", EstimatedTokens: 1, Required: true})
		},
		"recursive summary": func(p *ContextPlan) {
			p.Sources[1].SummaryOf[0] = ContextSourceRef{Layer: LayerSummary, SourceID: "old-digest", Revision: "s1"}
		},
		"artifact masquerade": func(p *ContextPlan) { p.Sources[0].SourceID = "retrieval/current" },
		"unselected source": func(p *ContextPlan) {
			p.Sources = append(p.Sources, ContextSource{Layer: LayerRetrieval, SourceID: "r", Revision: "1"})
		},
	} {
		candidate := plan.Clone()
		mutate(&candidate)
		if err := ValidateContextPlan(candidate); err == nil {
			t.Errorf("%s: expected rejection", name)
		}
	}
}

func TestContextPlanRequiredLayersNeedPositiveBudget(t *testing.T) {
	plan := baseContextPlan()
	plan.Budget.LayerBudgets[LayerCurrentInput] = 0
	if err := ValidateContextPlan(plan); err == nil {
		t.Fatal("expected current input zero budget rejection")
	}
	plan = baseContextPlan()
	plan.Budget.LayerBudgets[LayerRequiredState] = 0
	if err := ValidateContextPlan(plan); err == nil {
		t.Fatal("expected required state zero budget rejection")
	}
}

func TestContextPlanSummaryRequiresReason(t *testing.T) {
	plan := baseContextPlan()
	plan.CompactionReason = ""
	if err := ValidateContextPlan(plan); err == nil {
		t.Fatal("expected summary without compaction reason rejection")
	}
	plan.CompactionReason = "   "
	if err := ValidateContextPlan(plan); err == nil {
		t.Fatal("expected whitespace-only compaction reason rejection")
	}
}

func TestContextNamesRejectUnicodeWhitespace(t *testing.T) {
	for _, name := range []string{" leading", "trailing ", "with\u00a0space", "with\u2003space"} {
		if err := ValidateContextView(ContextView{StateFields: []string{name}}); err == nil {
			t.Errorf("%q: expected whitespace rejection", name)
		}
	}
}

func TestContextPlanCloneIsDeep(t *testing.T) {
	original := baseContextPlan()
	clone := original.Clone()
	clone.View.StateFields[0] = "changed"
	clone.View.Layers[0] = LayerArtifact
	clone.Budget.LayerBudgets[LayerCurrentInput] = 1
	clone.Sources[0].SummaryOf = []ContextSourceRef{{Layer: LayerArtifact, SourceID: "new", Revision: "new"}}
	clone.Sources[0].SourceID = "changed"
	if original.View.StateFields[0] == "changed" || original.View.Layers[0] == LayerArtifact || original.Budget.LayerBudgets[LayerCurrentInput] == 1 || original.Sources[0].SourceID == "changed" || len(original.Sources[0].SummaryOf) != 0 {
		t.Fatal("clone shares mutable state")
	}
}

func TestContextValidationRejectsUTF8ControlsAndOversize(t *testing.T) {
	badUTF8 := string([]byte{0xff, 0xfe})
	if err := ValidateContextView(ContextView{StateFields: []string{strings.Repeat("x", MaxContextIDBytes+1)}}); err == nil {
		t.Fatal("expected oversized single state field rejection")
	}
	if err := ValidateContextView(ContextView{StateFields: []string{badUTF8}}); err == nil {
		t.Fatal("expected invalid UTF-8 rejection")
	}
	if err := ValidateContextView(ContextView{StateFields: []string{"line\nfeed"}}); err == nil {
		t.Fatal("expected control rejection")
	}
	if err := ValidateContextView(ContextView{StateFields: func() []string {
		fields := make([]string, MaxContextStateFields)
		for i := range fields {
			fields[i] = strings.Repeat("x", MaxContextIDBytes)
		}
		return fields
	}(), Layers: []LayerKind{LayerRequiredState}}); err == nil {
		t.Fatal("expected encoded view size rejection")
	}
	if err := ValidateContextPlan(ContextPlan{DefinitionRevision: strings.Repeat("x", MaxContextRevisionBytes+1), PlanRevision: "p"}); err == nil {
		t.Fatal("expected revision size rejection")
	}
	if err := ValidateContextPlan(ContextPlan{DefinitionRevision: "d", PlanRevision: "p", View: ContextView{Layers: []LayerKind{LayerCurrentInput}}, Budget: ContextBudget{TotalTokens: 10, LayerBudgets: map[LayerKind]int64{LayerCurrentInput: 10}}, Sources: []ContextSource{{Layer: LayerCurrentInput, SourceID: "i", Revision: "r", Required: true, EstimatedTokens: 1}}, CompactionReason: string([]byte{0xc3, 0x28})}); err == nil {
		t.Fatal("expected compaction reason UTF-8 rejection")
	}
	tooManyBudgets := ContextBudget{TotalTokens: MaxContextTotalTokens, LayerBudgets: make(map[LayerKind]int64, MaxContextViewLayers+1)}
	for i := 0; i < MaxContextViewLayers+1; i++ {
		tooManyBudgets.LayerBudgets[LayerKind(fmt.Sprintf("layer-%d", i))] = 0
	}
	if err := ValidateContextBudget(tooManyBudgets); err == nil {
		t.Fatal("expected too many layer budget keys rejection")
	}
	oversized := ContextPlan{
		DefinitionRevision: "d", PlanRevision: "p",
		View:   ContextView{Layers: []LayerKind{LayerRecentHistory}},
		Budget: ContextBudget{TotalTokens: MaxContextTotalTokens, LayerBudgets: map[LayerKind]int64{LayerRecentHistory: MaxContextTotalTokens}},
	}
	for i := 0; i < MaxContextSources; i++ {
		oversized.Sources = append(oversized.Sources, ContextSource{Layer: LayerRecentHistory, SourceID: strings.Repeat("x", MaxContextIDBytes-4) + fmt.Sprintf("%04d", i), Revision: strings.Repeat("r", MaxContextRevisionBytes), EstimatedTokens: 0})
	}
	if err := ValidateContextPlan(oversized); err == nil {
		t.Fatal("expected encoded plan size rejection")
	}
}

func TestContextValidationErrorsAreClassified(t *testing.T) {
	if err := ValidateContextView(ContextView{Layers: []LayerKind{"bad"}}); !errors.Is(err, ErrInvalidContextView) {
		t.Fatalf("view error classification: %v", err)
	}
	if err := ValidateContextBudget(ContextBudget{TotalTokens: -1}); !errors.Is(err, ErrInvalidContextBudget) {
		t.Fatalf("budget error classification: %v", err)
	}
	if err := ValidateContextPlan(ContextPlan{}); !errors.Is(err, ErrInvalidContextPlan) {
		t.Fatalf("plan error classification: %v", err)
	}
}

func baseContextPlan() ContextPlan {
	return ContextPlan{
		DefinitionRevision: "graph-r1",
		PlanRevision:       "plan-r1",
		View:               ContextView{StateFields: []string{"ticket.id"}, Layers: []LayerKind{LayerCurrentInput, LayerRequiredState, LayerSummary}},
		Budget:             ContextBudget{TotalTokens: 100, OutputReserve: 10, StablePrefixTokens: 20, LayerBudgets: map[LayerKind]int64{LayerCurrentInput: 30, LayerRequiredState: 20, LayerSummary: 30}},
		Sources: []ContextSource{
			{Layer: LayerCurrentInput, SourceID: "input", Revision: "r1", EstimatedTokens: 10, Required: true},
			{Layer: LayerSummary, SourceID: "digest", Revision: "s1", EstimatedTokens: 10, SummaryOf: []ContextSourceRef{{Layer: LayerCurrentInput, SourceID: "input", Revision: "r1"}, {Layer: LayerRecentHistory, SourceID: "archived-history", Revision: "r7"}}},
			{Layer: LayerRequiredState, SourceID: "state", Revision: "r1", EstimatedTokens: 10, Required: true},
		},
		CompactionReason: "rolling history compaction",
	}
}

// Keep the overflow test independent of architecture width while still
// constructing a value that cannot be accepted as a bounded token budget.
func maxInt64ForTest() int64 { return int64(^uint64(0) >> 1) }
