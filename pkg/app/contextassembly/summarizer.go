package contextassembly

import (
	"context"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/cc-auto-agent/harness-core/pkg/core"
)

const (
	maxContextSummaryBytes      = 12 << 10
	defaultSummaryInputMessages = 256
	defaultSummaryInputBytes    = 256 << 10
	maxSummaryInputMessages     = 4096
	maxSummaryInputBytes        = 1 << 20
)

// ContextSummarizer produces a summary text for one projected message range.
type ContextSummarizer interface {
	Summarize(context.Context, []core.ChatMessage) (string, error)
}

// ContextSummarizerFunc adapts a function to ContextSummarizer.
type ContextSummarizerFunc func(context.Context, []core.ChatMessage) (string, error)

func (f ContextSummarizerFunc) Summarize(ctx context.Context, messages []core.ChatMessage) (string, error) {
	return f(ctx, messages)
}

// RollingSummarizer archives a bounded history prefix into one durable event.
type RollingSummarizer struct {
	MaxMessages int
	// KeepTail is an upper bound; a safe event range may retain fewer messages.
	KeepTail   int
	Summarizer ContextSummarizer
}

func (r *RollingSummarizer) EnsureSummarized(ctx context.Context, session *core.Session, runID string, emit func(core.SessionEvent), messages []core.ChatMessage) ([]core.ChatMessage, error) {
	if ctx == nil {
		return nil, fmt.Errorf("rolling summarizer context is nil")
	}
	maxMessages, keepTail, err := r.config()
	if err != nil {
		return nil, err
	}
	if len(messages) <= maxMessages {
		return messages, nil
	}
	if session == nil {
		return nil, fmt.Errorf("rolling summarizer session is nil")
	}
	boundary, start, end := summarizeArchiveRange(messages, keepTail)
	if boundary <= 0 {
		return nil, fmt.Errorf("rolling summarizer cannot fit history within MaxMessages")
	}
	if r.Summarizer == nil {
		return nil, fmt.Errorf("rolling summarizer has no ContextSummarizer")
	}
	archived := messages[:boundary]
	if summariesOnly(archived) {
		return nil, fmt.Errorf("rolling summarizer cannot make summary progress")
	}
	summary, err := summarizeAndRecordUsage(ctx, r.Summarizer, session, runID, emit, archived, start, end)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(summary) == "" || !utf8.ValidString(summary) || len(summary) > maxContextSummaryBytes {
		return nil, fmt.Errorf("rolling summarizer returned an invalid or oversized summary")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	event, err := session.Append(runID, core.EvContextSummary, core.ContextSummaryData{
		Op: "replace", Start: start, End: end, Summary: summary,
	})
	if err != nil {
		return nil, err
	}
	if emit != nil {
		emit(event)
	}
	result, err := session.DeriveMessages()
	if err != nil {
		return nil, err
	}
	if len(result) > maxMessages {
		return nil, fmt.Errorf("rolling summarizer result exceeds MaxMessages")
	}
	return result, nil
}

func (r *RollingSummarizer) config() (int, int, error) {
	if r == nil || r.MaxMessages <= 1 || r.MaxMessages > core.MaxSessionEvents {
		return 0, 0, fmt.Errorf("rolling summarizer MaxMessages must be between 2 and %d", core.MaxSessionEvents)
	}
	if r.KeepTail < 0 || r.KeepTail >= r.MaxMessages {
		return 0, 0, fmt.Errorf("rolling summarizer KeepTail must be between 1 and MaxMessages-1")
	}
	keepTail := r.KeepTail
	if keepTail == 0 {
		keepTail = r.MaxMessages / 2
	}
	if keepTail <= 0 || keepTail >= r.MaxMessages {
		return 0, 0, fmt.Errorf("rolling summarizer KeepTail is outside the message budget")
	}
	return r.MaxMessages, keepTail, nil
}

func summariesOnly(messages []core.ChatMessage) bool {
	if len(messages) == 0 {
		return false
	}
	for _, message := range messages {
		if message.Provenance == nil || message.Provenance.Kind != "summary" {
			return false
		}
	}
	return true
}

// Summary messages are positioned at their original range, but SourceSeq is
// the later summary event. Close the archive over both before persisting: every
// replaced message must be summarized, and no retained message may be shadowed.
// A suffix minimum makes this linear even with multiple disjoint summaries.
func summarizeArchiveRange(messages []core.ChatMessage, keepTail int) (int, int64, int64) {
	suffixStart := make([]int64, len(messages))
	for i := len(messages) - 1; i >= 0; i-- {
		start, _ := summaryMessageRange(messages[i])
		suffixStart[i] = start
		if i+1 < len(messages) {
			suffixStart[i] = min(start, suffixStart[i+1])
		}
	}
	var start, end int64
	for i := 0; i+1 < len(messages); i++ {
		low, high := summaryMessageRange(messages[i])
		if i == 0 {
			start, end = low, high
		} else {
			start, end = min(start, low), max(end, high)
		}
		next := messages[i+1]
		if len(messages)-i-1 <= keepTail && next.Role == core.RoleUser &&
			(next.Provenance == nil || next.Provenance.Kind != "summary") && end < suffixStart[i+1] {
			return i + 1, start, end
		}
	}
	return 0, 0, 0
}

func summaryMessageRange(message core.ChatMessage) (int64, int64) {
	start, end := message.SourceSeq, message.SourceSeq
	if p := message.Provenance; p != nil && p.Kind == "summary" {
		start, end = min(start, p.SourceStart), max(end, p.SourceEnd)
	}
	return start, end
}
