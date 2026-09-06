package contextassembly

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/cc-auto-agent/harness-core/pkg/core"
)

const (
	// DefaultExtractiveSummaryMaxBytes is the hard durable output budget used
	// by the default server composition. It deliberately leaves ample room for
	// the enclosing context/summary event and later model-context assembly.
	DefaultExtractiveSummaryMaxBytes        = 12 << 10
	DefaultExtractiveSummaryMaxMessageBytes = 1024
	DefaultExtractiveSummaryMaxToolBytes    = 2048
	maxExtractiveSourceMessages             = 4096
	maxExtractiveSummaryItems               = 4
	minimumExtractiveSummaryBytes           = 512
)

// ExtractiveSummarizerConfig controls the fixed local extraction budget. Zero
// values select the bounded defaults; values cannot enlarge the durable
// summary past DefaultExtractiveSummaryMaxBytes.
type ExtractiveSummarizerConfig struct {
	MaxBytes        int
	MaxMessageBytes int
	MaxToolBytes    int
}

// ExtractiveSummarizer is a deterministic ContextSummarizer that does not
// call a model. It records only source material that is already present in the
// projected history, labels omitted material, and never invents conclusions.
//
// It is intentionally a small default. Deployments that need semantic
// compression can replace it through contextassembly.ContextSummarizer.
type ExtractiveSummarizer struct {
	maxBytes, maxMessageBytes, maxToolBytes int
}

// NewExtractiveSummarizer creates a bounded deterministic summarizer.
func NewExtractiveSummarizer(config ExtractiveSummarizerConfig) (*ExtractiveSummarizer, error) {
	if config.MaxBytes == 0 {
		config.MaxBytes = DefaultExtractiveSummaryMaxBytes
	}
	if config.MaxMessageBytes == 0 {
		config.MaxMessageBytes = DefaultExtractiveSummaryMaxMessageBytes
	}
	if config.MaxToolBytes == 0 {
		config.MaxToolBytes = DefaultExtractiveSummaryMaxToolBytes
	}
	if config.MaxBytes < minimumExtractiveSummaryBytes || config.MaxBytes > DefaultExtractiveSummaryMaxBytes ||
		config.MaxMessageBytes <= 0 || config.MaxMessageBytes > config.MaxBytes ||
		config.MaxToolBytes <= 0 || config.MaxToolBytes > config.MaxBytes {
		return nil, fmt.Errorf("%w: extractive summarizer configuration", ErrInvalid)
	}
	return &ExtractiveSummarizer{maxBytes: config.MaxBytes, maxMessageBytes: config.MaxMessageBytes, maxToolBytes: config.MaxToolBytes}, nil
}

type extractiveTool struct {
	id, name string
	result   *core.ChatMessage
}

// Summarize emits existing durable summaries, the latest user goals, and tool
// call/result evidence. It walks the source once and only writes bounded
// fragments, so a giant transcript or tool result is never concatenated.
func (s *ExtractiveSummarizer) Summarize(ctx context.Context, messages []core.ChatMessage) (string, error) {
	if s == nil || ctx == nil || len(messages) == 0 || len(messages) > maxExtractiveSourceMessages {
		return "", ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}

	summaries := make([]core.ChatMessage, 0, maxExtractiveSummaryItems)
	users := make([]core.ChatMessage, 0, maxExtractiveSummaryItems)
	tools := make([]extractiveTool, 0)
	byID := make(map[string]int)
	unpairedResults := make([]core.ChatMessage, 0)
	for i := range messages {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		message := messages[i]
		if message.Provenance != nil && message.Provenance.Kind == "summary" {
			summaries = appendLatest(summaries, message, maxExtractiveSummaryItems)
			continue
		}
		if message.Role == core.RoleUser {
			users = appendLatest(users, message, maxExtractiveSummaryItems)
		}
		if message.Role == core.RoleAssistant {
			calls := message.ToolCalls
			if message.ToolCall != nil {
				calls = append([]core.ToolCall{*message.ToolCall}, calls...)
			}
			for _, call := range calls {
				if call.ID == "" || call.Name == "" {
					continue
				}
				if _, exists := byID[call.ID]; exists {
					continue
				}
				byID[call.ID] = len(tools)
				tools = append(tools, extractiveTool{id: call.ID, name: call.Name})
			}
		}
		if message.Role == core.RoleTool {
			if index, exists := byID[message.ToolCallID]; exists && tools[index].result == nil {
				copyOf := message
				tools[index].result = &copyOf
				delete(byID, message.ToolCallID) // IDs can be reused after a completed call, across turns.
			} else {
				unpairedResults = append(unpairedResults, message)
			}
		}
	}

	// Reserve a small suffix for the explicit omission declaration before any
	// source content is copied. This makes the output cap exact even when every
	// selected record would otherwise fill the builder.
	reserve := 192
	builder := boundedSummaryBuilder{limit: s.maxBytes - reserve, hardLimit: s.maxBytes}
	builder.record(fmt.Sprintf("[extractive summary source_messages=%d]", len(messages)))
	for _, message := range summaries {
		// A prior durable summary has a different budget from a single raw
		// message. Preserve it whole when it fits, otherwise label its omission.
		remaining := builder.limit - builder.builder.Len() - len("prior_summary: ") - 1
		builder.record("prior_summary: " + s.messageValue(message.Content, remaining))
	}
	for _, message := range users {
		builder.record("recent_user_goal_or_constraint: " + s.messageValue(message.Content, s.maxMessageBytes))
	}
	for _, tool := range tools {
		call := "tool_call id=" + s.identifier(tool.id) + " name=" + s.identifier(tool.name)
		if tool.result == nil {
			builder.record(call + " status=unresolved")
			continue
		}
		// Keep the call and its result as one record: a bounded summary either
		// retains the causal pair or labels the whole pair omitted, never leaves
		// a dangling call that appears unresolved when it was not.
		builder.record(call + "\ntool_result id=" + s.identifier(tool.result.ToolCallID) + " result=" + s.messageValue(tool.result.Content, s.maxToolBytes))
	}
	for _, result := range unpairedResults {
		builder.record("tool_result id=" + s.identifier(result.ToolCallID) + " status=unpaired result=" + s.messageValue(result.Content, s.maxToolBytes))
	}
	if builder.omitted > 0 {
		builder.forceRecord(fmt.Sprintf("[extractive omitted_records=%d]", builder.omitted))
	}
	if builder.builder.Len() == 0 {
		return "", ErrInvalid
	}
	return builder.builder.String(), nil
}

func appendLatest(messages []core.ChatMessage, message core.ChatMessage, limit int) []core.ChatMessage {
	if len(messages) < limit {
		return append(messages, message)
	}
	copy(messages, messages[1:])
	messages[len(messages)-1] = message
	return messages
}

func (s *ExtractiveSummarizer) messageValue(value string, limit int) string {
	if utf8.ValidString(value) && len(value) <= limit {
		return value
	}
	sum := sha256Prefix(value)
	return fmt.Sprintf("[omitted bytes=%d sha256=%s]", len(value), hex.EncodeToString(sum))
}

func sha256Prefix(value string) []byte {
	hash := sha256.New()
	// Copy through a small fixed buffer rather than converting the whole string
	// to []byte. A 1 MiB tool result therefore never causes a second 1 MiB
	// allocation merely to compute its evidence fingerprint.
	var buffer [4096]byte
	for len(value) > 0 {
		n := len(value)
		if n > len(buffer) {
			n = len(buffer)
		}
		copy(buffer[:n], value[:n])
		_, _ = hash.Write(buffer[:n])
		value = value[n:]
	}
	return hash.Sum(nil)[:8]
}

func (s *ExtractiveSummarizer) identifier(value string) string {
	return s.messageValue(value, s.maxMessageBytes)
}

type boundedSummaryBuilder struct {
	builder          strings.Builder
	limit, hardLimit int
	omitted          int
}

func (b *boundedSummaryBuilder) record(value string) {
	if value == "" || b.builder.Len()+len(value)+1 > b.limit {
		b.omitted++
		return
	}
	if b.builder.Len() > 0 {
		b.builder.WriteByte('\n')
	}
	b.builder.WriteString(value)
}

func (b *boundedSummaryBuilder) forceRecord(value string) {
	extra := len(value)
	if b.builder.Len() > 0 {
		extra++
	}
	if b.builder.Len()+extra > b.hardLimit {
		return
	}
	if b.builder.Len() > 0 {
		b.builder.WriteByte('\n')
	}
	b.builder.WriteString(value)
}
