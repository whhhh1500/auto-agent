package contextassembly

import (
	"context"
	"errors"
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
	summary, err := r.Summarizer.Summarize(ctx, archived)
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

// DefaultSummarizerPrompt is used when no custom system prompt is supplied.
const DefaultSummarizerPrompt = "Summarize the preceding agent conversation for archival. " +
	"Preserve task intent, confirmed facts, tool results that still matter, open questions, " +
	"and any constraints. Be terse and factual; do not invent details."

// LlmSummarizer executes bounded summary-only model calls. It is an app-level
// policy, while core retains only the model stream and proof contracts.
type LlmSummarizer struct {
	Adapter          core.LlmAdapter
	System           string
	ModelCallGate    core.ModelCallGate
	RequestIdentity  core.ModelCallRequest
	MaxInputMessages int
	MaxInputBytes    int
	MaxOutputBytes   int
}

func (s LlmSummarizer) Summarize(ctx context.Context, messages []core.ChatMessage) (string, error) {
	if s.Adapter == nil {
		return "", fmt.Errorf("summarizer LLM adapter is nil")
	}
	if ctx == nil {
		return "", fmt.Errorf("summarizer context is nil")
	}
	maxMessages, maxInputBytes, maxOutputBytes, err := s.limits()
	if err != nil {
		return "", err
	}
	acceptedCall := core.AcceptedModelCall{}
	if s.ModelCallGate != nil {
		acceptedCall, err = core.PrepareAcceptedModelCall(ctx, s.ModelCallGate, s.RequestIdentity)
		if err != nil {
			return "", err
		}
	}
	system := s.System
	if system == "" {
		system = DefaultSummarizerPrompt
	}
	if len(system) > maxInputBytes || !utf8.ValidString(system) {
		return "", fmt.Errorf("summarizer system exceeds input budget")
	}
	transcript, err := boundedSummaryTranscript(ctx, messages, maxMessages, maxInputBytes-len(system))
	if err != nil {
		return "", err
	}
	stream, err := streamSummary(ctx, s.Adapter, core.GenerateOptions{
		Provider: s.RequestIdentity.Provider, Model: s.RequestIdentity.Model, System: system,
		Messages:  []core.ChatMessage{{Role: core.RoleUser, Content: transcript}},
		ModelCall: s.RequestIdentity, AcceptedCall: acceptedCall,
	}, maxOutputBytes)
	if err != nil {
		if errors.Is(err, core.ErrAcceptedModelCallRequired) {
			return "", err
		}
		return "", fmt.Errorf("summarizer model call failed")
	}
	if stream.toolCalls {
		return "", fmt.Errorf("summarizer model returned tool calls")
	}
	summary := strings.TrimSpace(stream.text)
	if summary == "" || len(summary) > maxOutputBytes || !utf8.ValidString(summary) {
		return "", fmt.Errorf("summarizer model returned an invalid or oversized summary")
	}
	return summary, nil
}

func (s LlmSummarizer) limits() (int, int, int, error) {
	messages, inputBytes, outputBytes := s.MaxInputMessages, s.MaxInputBytes, s.MaxOutputBytes
	if messages == 0 {
		messages = defaultSummaryInputMessages
	}
	if inputBytes == 0 {
		inputBytes = defaultSummaryInputBytes
	}
	if outputBytes == 0 {
		outputBytes = maxContextSummaryBytes
	}
	if messages <= 0 || messages > maxSummaryInputMessages || inputBytes <= 0 || inputBytes > maxSummaryInputBytes || outputBytes <= 0 || outputBytes > maxContextSummaryBytes {
		return 0, 0, 0, fmt.Errorf("summarizer limits are outside the bounded range")
	}
	return messages, inputBytes, outputBytes, nil
}

func boundedSummaryTranscript(ctx context.Context, messages []core.ChatMessage, maxMessages, maxBytes int) (string, error) {
	if maxBytes < 128 {
		return "", fmt.Errorf("summarizer input budget is too small")
	}
	const omissionReserve = 64
	count := len(messages)
	if count > maxMessages {
		count = maxMessages
	}
	var transcript strings.Builder
	initial := maxBytes - omissionReserve
	if initial > 4096 {
		initial = 4096
	}
	transcript.Grow(initial)
	omittedMessages := len(messages) - count
	for i := 0; i < count; i++ {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		message := messages[i]
		if !utf8.ValidString(message.Content) {
			return "", fmt.Errorf("summarizer input contains invalid UTF-8")
		}
		linePrefix := string(message.Role) + ": "
		remaining := maxBytes - omissionReserve - transcript.Len()
		if remaining <= len(linePrefix)+1 {
			omittedMessages += count - i
			break
		}
		if len(message.Content)+len(linePrefix)+1 <= remaining {
			transcript.WriteString(linePrefix)
			transcript.WriteString(message.Content)
			transcript.WriteByte('\n')
			continue
		}
		marker := fmt.Sprintf("%s[omitted bytes=%d]", linePrefix, len(message.Content))
		if len(marker)+1 <= remaining {
			transcript.WriteString(marker)
			transcript.WriteByte('\n')
			continue
		}
		omittedMessages += count - i
		break
	}
	if omittedMessages > 0 {
		marker := fmt.Sprintf("[omitted messages=%d]", omittedMessages)
		if transcript.Len()+len(marker) <= maxBytes {
			transcript.WriteString(marker)
		}
	}
	if transcript.Len() == 0 {
		return "", fmt.Errorf("summarizer input is empty after bounding")
	}
	return transcript.String(), nil
}

type summaryStreamResult struct {
	text      string
	toolCalls bool
}

func streamSummary(ctx context.Context, adapter core.LlmAdapter, options core.GenerateOptions, maxBytes int) (result summaryStreamResult, err error) {
	if adapter == nil {
		return result, fmt.Errorf("summarizer LLM adapter is nil")
	}
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var text strings.Builder
	err = core.ConsumeModelStream(streamCtx, adapter, options, func(chunk core.StreamChunk) error {
		if chunk.Kind == core.StreamKindAssistant {
			if chunk.ToolCall != nil || len(chunk.ToolCalls) > 0 {
				result.toolCalls = true
				cancel()
				return nil
			}
			if len(chunk.Text)+text.Len() > maxBytes {
				cancel()
				return nil
			}
			text.WriteString(chunk.Text)
		}
		return nil
	})
	if err != nil {
		return result, err
	}
	if ctx.Err() != nil {
		return result, ctx.Err()
	}
	result.text = text.String()
	return result, nil
}
