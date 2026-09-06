package core

import "context"

// Session returns the caller-owned session bound to the agent.
func (a *Agent) Session() *Session { return a.opts.Session }

// Run creates a run ID and executes one turn.
func (a *Agent) Run(ctx context.Context, userText string) error {
	runID, err := NewID("run_")
	if err != nil {
		return err
	}
	_, err = a.RunTurn(ctx, TurnInput{RunID: runID, Text: userText})
	return err
}

func firstCall(calls []ToolCall) *ToolCall {
	if len(calls) == 0 {
		return nil
	}
	return &calls[0]
}
