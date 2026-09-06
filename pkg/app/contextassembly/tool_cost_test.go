package contextassembly

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/cc-auto-agent/harness-core/pkg/core"
)

func budgetTool() core.ToolSchema {
	return core.ToolSchema{Name: "documents.search", Description: "Retrieve a document", Parameters: map[string]any{"type": "object", "properties": map[string]any{"query": map[string]any{"type": "string"}}}}
}

func TestAssemblerReservesFinalToolsBeforeHistorySelection(t *testing.T) {
	a, _ := NewAssembler(Config{SafetyMarginTokens: 8})
	tools := []core.ToolSchema{budgetTool()}
	toolCost, err := (ConservativeEstimator{}).EstimateTools(tools)
	if err != nil {
		t.Fatal(err)
	}
	request := core.ModelContext{ContextWindowTokens: int(toolCost.Tokens) + 100, MaxOutputTokens: 16, Messages: []core.ChatMessage{
		{Role: core.RoleUser, Content: strings.Repeat("old ", 50), SourceSeq: 1},
		{Role: core.RoleAssistant, Content: "old response", SourceSeq: 2},
		{Role: core.RoleUser, Content: "current question", SourceSeq: 3},
	}}
	without, err := a.AssembleModelContext(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	request.Tools = tools
	with, err := a.AssembleModelContext(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(without.Messages) != 3 || len(with.Messages) >= len(without.Messages) || with.Messages[len(with.Messages)-1].Content != "current question" || with.DroppedGroups == 0 {
		t.Fatalf("tools did not reserve space before history selection: before=%#v after=%#v", without, with)
	}
	if with.InputTokens < toolCost.Tokens || with.InputBytes < toolCost.Bytes || with.InputTokens > int64(request.ContextWindowTokens-request.MaxOutputTokens-8) {
		t.Fatal("tool cost missing from input evidence")
	}
	request.ContextWindowTokens = int(toolCost.Tokens) + 16 + 8
	if _, err := a.AssembleModelContext(context.Background(), request); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("mandatory current message must not be silently dropped: %v", err)
	}
}

type customContextEstimator struct {
	ConservativeEstimator
	tools func([]core.ToolSchema) (Cost, error)
}

func (e customContextEstimator) EstimateTools(tools []core.ToolSchema) (Cost, error) {
	return e.tools(tools)
}

func TestAssemblerUsesCustomToolEstimatorAndRejectsBrokenEstimates(t *testing.T) {
	request := core.ModelContext{ContextWindowTokens: 80, MaxOutputTokens: 10, Messages: []core.ChatMessage{{Role: core.RoleUser, Content: "current"}}, Tools: []core.ToolSchema{budgetTool()}}
	called := 0
	a, err := NewAssembler(Config{SafetyMarginTokens: 4, Estimator: customContextEstimator{tools: func(tools []core.ToolSchema) (Cost, error) {
		called++
		if len(tools) != 1 || tools[0].Name != request.Tools[0].Name {
			t.Fatal("estimator received a different tool set")
		}
		return Cost{Bytes: 100, Tokens: 5}, nil
	}}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := a.AssembleModelContext(context.Background(), request)
	if err != nil || called != 1 || result.InputTokens != 16 {
		t.Fatalf("custom estimator not used: calls=%d result=%#v error=%v", called, result, err)
	}
	for _, bad := range []Cost{{}, {Bytes: -1, Tokens: 1}, {Bytes: 1, Tokens: -1}, {Bytes: MaxBytes + 1, Tokens: 1}, {Bytes: 1, Tokens: MaxTokens + 1}} {
		a, _ := NewAssembler(Config{SafetyMarginTokens: 4, Estimator: customContextEstimator{tools: func([]core.ToolSchema) (Cost, error) { return bad, nil }}})
		if _, err := a.AssembleModelContext(context.Background(), request); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid estimate accepted: %#v error=%v", bad, err)
		}
	}
	a, _ = NewAssembler(Config{SafetyMarginTokens: 4, Estimator: customContextEstimator{tools: func([]core.ToolSchema) (Cost, error) { panic("plugin failure") }}})
	if _, err := a.AssembleModelContext(context.Background(), request); !errors.Is(err, ErrInvalid) {
		t.Fatalf("plugin panic not isolated: %v", err)
	}
}

func TestConservativeToolEstimatorRejectsInvalidSchemas(t *testing.T) {
	cyclic := map[string]any{}
	cyclic["properties"] = cyclic
	for _, tools := range [][]core.ToolSchema{
		{budgetTool(), budgetTool()},
		{{Name: " bad-name"}},
		{{Name: "tool", Description: "bad\x00description"}},
		{{Name: "tool", Parameters: map[string]any{"type": "nonexistent"}}},
		{{Name: "tool", Parameters: map[string]any{"constant": math.NaN()}}},
		{{Name: "tool", Parameters: cyclic}},
	} {
		if _, err := (ConservativeEstimator{}).EstimateTools(tools); err == nil {
			t.Fatal("invalid tools accepted")
		}
	}
}

func TestRequiredToolsObeyLayerBudget(t *testing.T) {
	r := Request{Budget: Budget{TotalBytes: 1000, TotalTokens: 1000, LayerTokens: map[Layer]int64{LayerTools: 0}}, Estimator: ConservativeEstimator{}, RequiredTools: Cost{Bytes: 100, Tokens: 10}}
	if _, err := Assemble(context.Background(), r); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("tools bypassed layer budget: %v", err)
	}
}

type overriddenMessageEstimator struct {
	ConservativeEstimator
	calls *int
}

func (e overriddenMessageEstimator) Estimate(core.ChatMessage) (Cost, error) {
	*e.calls++
	return Cost{Bytes: 12, Tokens: 3}, nil
}

func TestBuiltinFastPathDoesNotBypassEmbeddedEstimatorOverride(t *testing.T) {
	calls := 0
	a, _ := NewAssembler(Config{SafetyMarginTokens: 4, Estimator: overriddenMessageEstimator{calls: &calls}})
	result, err := a.AssembleModelContext(context.Background(), core.ModelContext{ContextWindowTokens: 100, MaxOutputTokens: 10, Messages: []core.ChatMessage{{Role: core.RoleUser, Content: "current"}}})
	if err != nil || calls != 1 || result.InputTokens != 3 || result.InputBytes != 12 {
		t.Fatalf("override bypassed: calls=%d result=%#v error=%v", calls, result, err)
	}
}

func TestBuiltinValidatedAndPublicEstimatesAgree(t *testing.T) {
	messages := []core.ChatMessage{
		{Role: core.RoleUser, Content: "English 中文 \"quote\"\n<escaped>"},
		{Role: core.RoleAssistant, ToolCall: &core.ToolCall{ID: "a", Name: "search", Args: map[string]any{"nested": []any{"中", float64(1), true}}, Continuation: "state"}},
		{Role: core.RoleAssistant, ToolCalls: []core.ToolCall{{ID: "a", Name: "first"}, {ID: "b", Name: "second", Args: map[string]any{"x": "y"}}}},
		{Role: core.RoleTool, ToolCallID: "a", Content: "result"},
	}
	for _, estimator := range []BudgetEstimator{ByteEstimator{}, ConservativeEstimator{}} {
		for _, message := range messages {
			if err := validateMessage(message); err != nil {
				t.Fatal(err)
			}
			public, err := estimator.Estimate(message)
			if err != nil {
				t.Fatal(err)
			}
			fast, err := safeEstimate(estimator, message)
			if err != nil || fast != public {
				t.Fatalf("fast path changed cost: public=%v fast=%v error=%v", public, fast, err)
			}
		}
	}
}
