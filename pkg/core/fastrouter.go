package core

import "context"

// FastRule is one deterministic, zero-model route.
type FastRule struct {
	Match        func(text string) bool
	Capability   string
	Args         func(text string) map[string]any
	Answer       func(result CapabilityResult) string
	BlockMessage string
}

// FastDispatch contains the auditable result of a matched route.
type FastDispatch struct {
	Matched bool
	Blocked bool
	Call    *ToolCall
	Result  *CapabilityResult
	Answer  string
}

// FastRouter holds ordered deterministic routes evaluated before the LLM.
type FastRouter struct {
	rules []FastRule
}

func (r *FastRouter) Add(rule FastRule) {
	r.rules = append(r.rules, rule)
}

// Dispatch either executes or blocks a matched route; only no-match falls through.
func (r *FastRouter) Dispatch(ctx context.Context, text string, tools ToolRuntime) (FastDispatch, error) {
	for _, rule := range r.rules {
		if rule.Match == nil || !rule.Match(text) {
			continue
		}
		if err := validateCapabilityID(rule.Capability); err != nil || !tools.Authorized(rule.Capability) {
			message := rule.BlockMessage
			if message == "" {
				message = "This capability is not available for the current principal."
			}
			return FastDispatch{Matched: true, Blocked: true, Answer: message}, nil
		}
		args := map[string]any{}
		if rule.Args != nil {
			cloned, err := cloneCallArgs(rule.Args(text))
			if err != nil {
				return FastDispatch{Matched: true}, err
			}
			args = cloned
		}
		callID, err := NewID("call_")
		if err != nil {
			return FastDispatch{}, err
		}
		call := ToolCall{ID: callID, Name: rule.Capability, Args: args}
		result, err := tools.Execute(ctx, call)
		if err != nil {
			return FastDispatch{Matched: true, Call: &call}, err
		}
		answer := result.Content
		if rule.Answer != nil {
			answer = rule.Answer(result)
		}
		return FastDispatch{Matched: true, Call: &call, Result: &result, Answer: answer}, nil
	}
	return FastDispatch{}, nil
}
