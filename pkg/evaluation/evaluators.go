package evaluation

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

type Observation struct {
	Status    core.RunStatus
	Answer    string
	Error     string
	Events    []core.SessionEvent
	ToolCalls []string
}

type Evaluator interface {
	Kind() AssertionKind
	Evaluate(ctx context.Context, assertion Assertion, observation Observation) (AssertionResult, error)
}

type EvaluatorFunc struct {
	AssertionKind AssertionKind
	EvaluateFunc  func(context.Context, Assertion, Observation) (AssertionResult, error)
}

func (e EvaluatorFunc) Kind() AssertionKind { return e.AssertionKind }
func (e EvaluatorFunc) Evaluate(ctx context.Context, assertion Assertion, observation Observation) (AssertionResult, error) {
	return e.EvaluateFunc(ctx, assertion, observation)
}

type Registry struct{ evaluators map[AssertionKind]Evaluator }

func NewRegistry(custom ...Evaluator) (*Registry, error) {
	registry := &Registry{evaluators: map[AssertionKind]Evaluator{}}
	for _, evaluator := range builtinEvaluators() {
		registry.evaluators[evaluator.Kind()] = evaluator
	}
	for _, evaluator := range custom {
		if evaluator == nil || evaluator.Kind() == "" {
			return nil, fmt.Errorf("evaluation evaluator is nil or has no kind")
		}
		registry.evaluators[evaluator.Kind()] = evaluator
	}
	return registry, nil
}

func (r *Registry) Evaluate(ctx context.Context, assertion Assertion, observation Observation) (result AssertionResult, err error) {
	if r == nil {
		return AssertionResult{}, fmt.Errorf("evaluation registry is nil")
	}
	evaluator := r.evaluators[assertion.Kind]
	if evaluator == nil {
		return AssertionResult{}, fmt.Errorf("no evaluator for assertion kind %q", assertion.Kind)
	}
	defer func() {
		if recover() != nil {
			result = AssertionResult{}
			err = fmt.Errorf("evaluator %q panicked", assertion.Kind)
		}
	}()
	result, err = evaluator.Evaluate(ctx, assertion, observation)
	if err != nil {
		return AssertionResult{}, err
	}
	result.AssertionID = assertion.ID
	result.Kind = assertion.Kind
	if result.Score < 0 || result.Score > 1 {
		return AssertionResult{}, fmt.Errorf("evaluator %q returned score outside 0..1", assertion.Kind)
	}
	return result, nil
}

func builtinEvaluators() []Evaluator {
	return []Evaluator{
		EvaluatorFunc{AssertRunStatus, evaluateRunStatus},
		EvaluatorFunc{AssertAnswerExact, evaluateAnswerExact},
		EvaluatorFunc{AssertAnswerContains, evaluateAnswerContains},
		EvaluatorFunc{AssertAnswerJSON, evaluateAnswerJSON},
		EvaluatorFunc{AssertToolCalled, evaluateToolCalled},
		EvaluatorFunc{AssertToolNotCalled, evaluateToolNotCalled},
	}
}

func evaluateRunStatus(_ context.Context, assertion Assertion, observation Observation) (AssertionResult, error) {
	passed := observation.Status == assertion.ExpectedStatus
	return binaryResult(passed, fmt.Sprintf("status=%s expected=%s", observation.Status, assertion.ExpectedStatus)), nil
}

func evaluateAnswerExact(_ context.Context, assertion Assertion, observation Observation) (AssertionResult, error) {
	actual, expected := observation.Answer, assertion.Expected
	if !assertion.CaseSensitive {
		actual, expected = strings.ToLower(actual), strings.ToLower(expected)
	}
	return binaryResult(actual == expected, "answer exact match"), nil
}

func evaluateAnswerContains(_ context.Context, assertion Assertion, observation Observation) (AssertionResult, error) {
	actual, expected := observation.Answer, assertion.Expected
	if !assertion.CaseSensitive {
		actual, expected = strings.ToLower(actual), strings.ToLower(expected)
	}
	return binaryResult(strings.Contains(actual, expected), "answer contains expected text"), nil
}

func evaluateAnswerJSON(_ context.Context, assertion Assertion, observation Observation) (AssertionResult, error) {
	var value any
	if err := json.Unmarshal([]byte(observation.Answer), &value); err != nil {
		return binaryResult(false, "answer is not valid JSON"), nil
	}
	if err := core.ValidateJSONValue(assertion.Schema, value); err != nil {
		return binaryResult(false, boundedEvaluationMessage(err.Error())), nil
	}
	return binaryResult(true, "answer matches JSON schema"), nil
}

func evaluateToolCalled(_ context.Context, assertion Assertion, observation Observation) (AssertionResult, error) {
	return binaryResult(containsString(observation.ToolCalls, assertion.CapabilityID),
		"required capability call"), nil
}

func evaluateToolNotCalled(_ context.Context, assertion Assertion, observation Observation) (AssertionResult, error) {
	return binaryResult(!containsString(observation.ToolCalls, assertion.CapabilityID),
		"forbidden capability call"), nil
}

func binaryResult(passed bool, message string) AssertionResult {
	score := 0.0
	if passed {
		score = 1
	}
	return AssertionResult{Score: score, Passed: passed, Message: boundedEvaluationMessage(message)}
}

func containsString(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func boundedEvaluationMessage(value string) string {
	if len(value) > 4096 {
		return value[:4096]
	}
	return value
}
