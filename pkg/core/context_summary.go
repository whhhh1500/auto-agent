package core

import "context"

// RunSummarizer archives an over-budget history prefix into a durable
// context/summary event before the model request is assembled. The concrete
// policy and provider mechanics live outside the core message kernel.
type RunSummarizer interface {
	EnsureSummarized(
		ctx context.Context,
		session *Session,
		runID string,
		emit func(SessionEvent),
		messages []ChatMessage,
	) ([]ChatMessage, error)
}
