package core

import (
	"fmt"
	"unicode/utf8"
)

// ContextCompactor projects the logged message history down to what one model
// request should contain. Compaction is strictly subtractive: every output is
// a prefix-consistent subset or redaction of the log, so "model-visible means
// logged" keeps holding without extra bookkeeping events.
//
// The loop calls Compact immediately before each model request.
type ContextCompactor interface {
	Compact(messages []ChatMessage) []ChatMessage
}

// ContextCompactorFunc adapts a function to ContextCompactor.
type ContextCompactorFunc func([]ChatMessage) []ChatMessage

func (f ContextCompactorFunc) Compact(messages []ChatMessage) []ChatMessage {
	return f(messages)
}

// RecentTurnsCompactor is the default mechanical compactor. It needs no model
// call and no rewriting:
//
//   - Tool results in turns older than the most recent user message are
//     truncated to MaxToolResultChars with a stable marker, so pairing between
//     tool calls and results stays intact while stale payloads stop consuming
//     the window.
//   - Whole turns (a user message through everything before the next user
//     message) are dropped from the front until at most MaxMessages remain.
//     Dropping at turn boundaries keeps assistant tool_call messages and their
//     tool results together, which provider APIs require.
type RecentTurnsCompactor struct {
	MaxMessages        int
	MaxToolResultChars int
}

// ElidedToolResultMarker prefixes truncated tool results.
const ElidedToolResultMarker = "[earlier tool result elided"

func (c RecentTurnsCompactor) Compact(messages []ChatMessage) []ChatMessage {
	out := messages
	if c.MaxToolResultChars > 0 {
		out = c.pruneOldToolResults(out)
	}
	if c.MaxMessages > 0 {
		out = c.windowTurns(out)
	}
	return out
}

// pruneOldToolResults redacts tool results that precede the final user message.
func (c RecentTurnsCompactor) pruneOldToolResults(messages []ChatMessage) []ChatMessage {
	lastUser := -1
	for i, message := range messages {
		if message.Role == RoleUser {
			lastUser = i
		}
	}
	if lastUser <= 0 {
		return messages
	}
	firstPrunable := -1
	for i := 0; i < lastUser; i++ {
		message := messages[i]
		if message.Role == RoleTool {
			if _, ok := c.elidedToolResult(message.Content); ok {
				firstPrunable = i
				break
			}
		}
	}
	if firstPrunable < 0 {
		return messages
	}

	out := append([]ChatMessage(nil), messages...)
	for i := firstPrunable; i < lastUser; i++ {
		message := out[i]
		if message.Role != RoleTool {
			continue
		}
		if content, ok := c.elidedToolResult(message.Content); ok {
			out[i].Content = content
		}
	}
	return out
}

// elidedToolResult applies the tool-result portion of RecentTurnsCompactor's
// public semantics. It is shared by the event-level fast path so it cannot
// drift from Compact's Unicode-aware truncation behavior.
func (c RecentTurnsCompactor) elidedToolResult(content string) (string, bool) {
	if c.MaxToolResultChars <= 0 || len(content) <= c.MaxToolResultChars {
		return "", false
	}
	chars := utf8.RuneCountInString(content)
	if chars <= c.MaxToolResultChars {
		return "", false
	}
	return fmt.Sprintf("%s: %d chars]", ElidedToolResultMarker, chars), true
}

func asRecentTurnsCompactor(compactor ContextCompactor) (RecentTurnsCompactor, bool) {
	switch value := compactor.(type) {
	case RecentTurnsCompactor:
		return value, true
	case *RecentTurnsCompactor:
		if value != nil {
			return *value, true
		}
	}
	return RecentTurnsCompactor{}, false
}

// windowTurns keeps the most recent turns within MaxMessages. Turn boundaries
// are user messages; leading non-user messages belong to the first turn. When
// even one turn exceeds the window, the latest turn is kept whole: truncating
// mid-turn would break tool_call/tool_result pairing.
func (c RecentTurnsCompactor) windowTurns(messages []ChatMessage) []ChatMessage {
	if len(messages) <= c.MaxMessages {
		return messages
	}
	firstFitting := 0
	lastBoundary := 0
	for i, message := range messages {
		if message.Role == RoleUser && i > 0 {
			lastBoundary = i
			if firstFitting == 0 && len(messages)-i <= c.MaxMessages {
				// The earliest fitting boundary keeps the most context.
				firstFitting = i
			}
		}
	}
	keep := firstFitting
	if keep == 0 {
		keep = lastBoundary
	}
	if keep == 0 {
		return messages
	}
	return append([]ChatMessage(nil), messages[keep:]...)
}
