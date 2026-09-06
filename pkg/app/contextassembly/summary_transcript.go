package contextassembly

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"unicode/utf8"

	"github.com/cc-auto-agent/harness-core/pkg/core"
)

type summaryToolCall struct {
	ID          string         `json:"id"`
	Name        string         `json:"name"`
	Args        map[string]any `json:"args,omitempty"`
	ArgsOmitted bool           `json:"args_omitted,omitempty"`
}

type summaryRecord struct {
	Role       core.ChatRole           `json:"role"`
	Seq        int64                   `json:"seq"`
	Content    string                  `json:"content,omitempty"`
	CallID     string                  `json:"tool_call_id,omitempty"`
	Calls      []summaryToolCall       `json:"tool_calls,omitempty"`
	Provenance *core.ContextProvenance `json:"provenance,omitempty"`
}

func boundedSummaryTranscript(ctx context.Context, messages []core.ChatMessage, maxMessages, maxBytes int) (string, error) {
	if maxBytes < 128 {
		return "", fmt.Errorf("summarizer input budget is too small")
	}
	const reserve = 64
	count := min(len(messages), maxMessages)
	var transcript strings.Builder
	transcript.Grow(min(maxBytes-reserve, 4096))
	omitted := len(messages) - count
	for i := 0; i < count; i++ {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		remaining := maxBytes - reserve - transcript.Len()
		if remaining < 64 {
			omitted += count - i
			break
		}
		record, err := boundedSummaryRecord(messages[i], remaining)
		if err != nil {
			return "", err
		}
		encoded, err := json.Marshal(record)
		if err != nil {
			return "", fmt.Errorf("summarizer input contains invalid JSON")
		}
		if len(encoded)+1 > remaining {
			omitted++
			continue
		}
		transcript.Write(encoded)
		transcript.WriteByte('\n')
	}
	if omitted > 0 {
		fmt.Fprintf(&transcript, "{\"omitted_messages\":%d}\n", omitted)
	}
	if transcript.Len() == 0 {
		return "", fmt.Errorf("summarizer input is empty after bounding")
	}
	return transcript.String(), nil
}

func boundedSummaryRecord(message core.ChatMessage, maxBytes int) (summaryRecord, error) {
	r := summaryRecord{Role: message.Role, Seq: message.SourceSeq, Content: message.Content, CallID: message.ToolCallID, Provenance: message.Provenance}
	if (message.Role != core.RoleUser && message.Role != core.RoleAssistant && message.Role != core.RoleTool) || !utf8.ValidString(message.Content) || len(message.ToolCallID) > 256 {
		return r, ErrInvalid
	}
	// JSON escaping can expand one source byte to six bytes. Bound source
	// material before Marshal, including arbitrary tool argument graphs.
	budget := maxBytes / 6
	if len(r.Content) > budget {
		r.Content = fmt.Sprintf("[omitted bytes=%d]", len(r.Content))
	}
	budget -= len(r.Content) + len(r.CallID)
	calls := message.ToolCalls
	if message.ToolCall != nil {
		if len(calls) == 0 {
			calls = []core.ToolCall{*message.ToolCall}
		} else if !reflect.DeepEqual(*message.ToolCall, calls[0]) {
			return r, ErrInvalid
		}
	}
	if len(calls) > 128 {
		return r, ErrInvalid
	}
	for _, call := range calls {
		if call.ID == "" || call.Name == "" || len(call.ID) > 256 || len(call.Name) > 256 || !utf8.ValidString(call.ID+call.Name) {
			return r, ErrInvalid
		}
		budget -= len(call.ID) + len(call.Name)
		entry := summaryToolCall{ID: call.ID, Name: call.Name}
		argumentBudget, nodes := max(0, budget), 2048
		if summaryJSONFits(call.Args, &argumentBudget, &nodes, 0) {
			entry.Args = call.Args
			budget = argumentBudget
		} else {
			entry.ArgsOmitted = true
		}
		r.Calls = append(r.Calls, entry)
	}
	if p := r.Provenance; p != nil && (len(p.Kind)+len(p.Revision) > 512 || !utf8.ValidString(p.Kind+p.Revision)) {
		return r, ErrInvalid
	}
	return r, nil
}

// Only bounded JSON-native values reach Marshal. Unknown/custom marshalers,
// cycles, deep or oversized graphs are explicitly omitted without invoking
// custom code or allocating a complete oversized JSON buffer.
func summaryJSONFits(value any, remaining, nodes *int, depth int) bool {
	*nodes--
	*remaining -= 2
	if depth > 32 || *nodes < 0 || *remaining < 0 {
		return false
	}
	switch v := value.(type) {
	case nil, bool:
		*remaining -= 5
	case string:
		if len(v) > *remaining || !utf8.ValidString(v) {
			return false
		}
		*remaining -= len(v)
	case json.Number:
		*remaining -= len(v)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		*remaining -= 32
	case []any:
		if len(v) > *nodes {
			return false
		}
		for _, item := range v {
			if !summaryJSONFits(item, remaining, nodes, depth+1) {
				return false
			}
		}
	case map[string]any:
		if len(v) > *nodes {
			return false
		}
		for key, item := range v {
			if !summaryJSONFits(key, remaining, nodes, depth+1) || !summaryJSONFits(item, remaining, nodes, depth+1) {
				return false
			}
		}
	default:
		return false
	}
	return *remaining >= 0
}
