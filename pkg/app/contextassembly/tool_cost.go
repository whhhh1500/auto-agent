package contextassembly

import (
	"encoding/json"
	"fmt"

	"github.com/cc-auto-agent/harness-core/pkg/core"
)

// EstimateTools includes every exposed name, description and parameter schema.
// The neutral JSON cost reserves 64 bytes/tokens per tool plus 32 for framing.
// Protocols with different framing/tokenization can replace ContextEstimator;
// this portable default is an estimate, not a provider billing tokenizer.
func (ConservativeEstimator) EstimateTools(tools []core.ToolSchema) (Cost, error) {
	if len(tools) == 0 {
		return Cost{}, nil
	}
	if len(tools) > MaxItems {
		return Cost{}, ErrInvalid
	}
	seen := make(map[string]bool, len(tools))
	total := int64(32)
	for _, tool := range tools {
		if validateIdentifier(tool.Name, MaxMetadataBytes, false) != nil || validatePromptText(tool.Description) != nil || seen[tool.Name] {
			return Cost{}, ErrInvalid
		}
		seen[tool.Name] = true
		// Marshal before recursive schema validation: cycles/unsupported values
		// must not reach the schema walk. Nothing is logged on encoding failure.
		encoded, err := json.Marshal(tool)
		if err != nil || len(encoded) > MaxBytes || core.ValidateSchema(tool.Parameters) != nil {
			return Cost{}, ErrInvalid
		}
		total += int64(len(encoded)) + 64
		if total > MaxBytes {
			return Cost{}, ErrBudgetExceeded
		}
	}
	return Cost{Bytes: total, Tokens: total}, nil
}

func safeEstimateTools(estimator ContextEstimator, tools []core.ToolSchema) (cost Cost, err error) {
	defer func() {
		if recover() != nil {
			cost, err = Cost{}, fmt.Errorf("%w: tool estimator panic", ErrInvalid)
		}
	}()
	if len(tools) == 0 {
		return Cost{}, nil
	}
	cost, err = estimator.EstimateTools(tools)
	if err != nil {
		return Cost{}, err
	}
	if !validCost(cost) || cost.Bytes == 0 || cost.Tokens == 0 {
		return Cost{}, ErrInvalid
	}
	return cost, nil
}
