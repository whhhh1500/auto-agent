package contextassembly

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/cc-auto-agent/harness-core/pkg/core"
)

// SummaryRequest binds an optional metered policy to its enclosing run.
// RollingSummarizer supplies run identity; the policy selects provider/model.
type SummaryRequest struct {
	Identity core.ModelCallRequest
	Messages []core.ChatMessage
}

// SummaryResult keeps valid reported usage even when the policy returns an
// error. Nil Usage means unreported, not that the failed request was free.
type SummaryResult struct {
	Text  string
	Usage *core.TokenUsage
}

// MeteredContextSummarizer is optional alongside ContextSummarizer. Implement
// both to expose metered model or remote summarization without mutable counters.
type MeteredContextSummarizer interface {
	SummarizeWithUsage(context.Context, SummaryRequest) (SummaryResult, error)
}

func summarizeAndRecordUsage(ctx context.Context, policy ContextSummarizer, session *core.Session, runID string, emit func(core.SessionEvent), messages []core.ChatMessage, start, end int64) (string, error) {
	metered, ok := policy.(MeteredContextSummarizer)
	if !ok {
		return policy.Summarize(ctx, messages)
	}
	identity := core.ModelCallRequest{Principal: session.Principal(), Scope: session.Scope(), SessionID: session.ID(), RunID: runID}
	// Agent appends step/start immediately before summarization. Read only the
	// last event, avoiding a clone of the complete durable history for identity.
	if version := session.Version(); version > 0 {
		last := session.EventsFrom(version - 1)
		if len(last) == 1 && last[0].RunID == runID && last[0].Type == core.EvStepStart {
			var step core.StepData
			if err := json.Unmarshal(last[0].Data, &step); err != nil {
				return "", err
			}
			identity.Step = step.Index
		}
	}
	identity.Deadline, _ = ctx.Deadline()
	requestVersion := session.Version()
	result, err := metered.SummarizeWithUsage(ctx, SummaryRequest{Identity: identity, Messages: messages})
	if usage := result.Usage; usage != nil || err == nil {
		if usage == nil {
			usage = &core.TokenUsage{}
		}
		if usage.InputTokens < 0 || usage.OutputTokens < 0 || usage.InputTokens > core.MaxReportedTokensPerCall || usage.OutputTokens > core.MaxReportedTokensPerCall {
			return "", fmt.Errorf("summarizer reported invalid token usage")
		}
		event, appendErr := session.Append(runID, core.EvRunUsage, core.RunUsageData{InputTokens: usage.InputTokens, OutputTokens: usage.OutputTokens, InvocationID: fmt.Sprintf("summary:%d:%d:%d", start, end, requestVersion)})
		if appendErr != nil {
			return "", appendErr
		}
		if emit != nil {
			emit(event)
		}
	}
	return result.Text, err
}
