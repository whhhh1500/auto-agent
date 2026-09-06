package contextassembly

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"unicode/utf8"

	"github.com/cc-auto-agent/harness-core/pkg/core"
)

const (
	DefaultSafetyMarginTokens = 1024
	DefaultMaxToolResultBytes = 4096

	priorityOlderExact = 100
	prioritySummary    = 200
	priorityCurrent    = 300
)

// Config has fixed local safeguards; policy persistence is deliberately left
// to a later control-plane slice. Zero values select conservative defaults.
type Config struct{ SafetyMarginTokens, MaxToolResultBytes int }

// Assembler makes an ephemeral bounded model context. It does not summarize,
// write artifacts, start goroutines, or mutate the durable event projection.
type Assembler struct{ safetyMargin, maxToolResultBytes int }

func NewAssembler(config Config) (*Assembler, error) {
	if config.SafetyMarginTokens == 0 {
		config.SafetyMarginTokens = DefaultSafetyMarginTokens
	}
	if config.MaxToolResultBytes == 0 {
		config.MaxToolResultBytes = DefaultMaxToolResultBytes
	}
	if config.SafetyMarginTokens <= 0 || config.MaxToolResultBytes <= 0 || config.MaxToolResultBytes > MaxBytes {
		return nil, fmt.Errorf("%w: assembler configuration", ErrInvalid)
	}
	return &Assembler{safetyMargin: config.SafetyMarginTokens, maxToolResultBytes: config.MaxToolResultBytes}, nil
}

func (a *Assembler) AssembleModelContext(ctx context.Context, request core.ModelContext) (core.ModelContext, error) {
	if a == nil || ctx == nil {
		return core.ModelContext{}, ErrInvalid
	}
	if request.ContextWindowTokens <= 0 || request.MaxOutputTokens <= 0 || request.ContextWindowTokens <= request.MaxOutputTokens+a.safetyMargin {
		return core.ModelContext{}, ErrBudgetExceeded
	}
	inputTokens := request.ContextWindowTokens - request.MaxOutputTokens - a.safetyMargin
	items, err := a.items(ctx, request.Messages)
	if err != nil {
		return core.ModelContext{}, err
	}
	result, err := Assemble(ctx, Request{Budget: Budget{TotalBytes: MaxBytes, TotalTokens: int64(inputTokens)}, Estimator: utf8ByteUpperBoundEstimator{}, RequiredSystem: request.System, Items: items, LightweightEvidence: true})
	if err != nil {
		return core.ModelContext{}, err
	}
	return core.ModelContext{System: result.System, Messages: result.Messages, ContextWindowTokens: request.ContextWindowTokens, MaxOutputTokens: request.MaxOutputTokens, InputBytes: result.Evidence.Used.Bytes, InputTokens: result.Evidence.Used.Tokens, DroppedGroups: droppedGroups(result.Evidence)}, nil
}

func (a *Assembler) items(ctx context.Context, messages []core.ChatMessage) ([]Item, error) {
	latestUser := -1
	for i := range messages {
		if messages[i].Role == core.RoleUser && (messages[i].Provenance == nil || messages[i].Provenance.Kind != "summary") {
			latestUser = i
		}
	}
	groups := make(map[string]string)
	items := make([]Item, 0, len(messages))
	for i, message := range messages {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		layer, priority := LayerRecent, priorityOlderExact
		if message.Provenance != nil && message.Provenance.Kind == "summary" {
			layer, priority = LayerSummary, prioritySummary
		}
		required := false
		if latestUser >= 0 && i >= latestUser {
			priority, required = priorityCurrent, true
		}
		groupID := ""
		calls := message.ToolCalls
		if len(calls) == 0 && message.ToolCall != nil {
			calls = []core.ToolCall{*message.ToolCall}
		}
		if message.Role == core.RoleAssistant && len(calls) > 0 {
			groupID = "tool-group-" + sourceID(message, i)
			for _, call := range calls {
				groups[call.ID] = groupID
			}
		} else if message.Role == core.RoleTool {
			layer = LayerToolResults
			groupID = groups[message.ToolCallID]
			if i < latestUser && len(message.Content) > a.maxToolResultBytes {
				message.Content = toolMarker(message.Content)
			}
		}
		revision := strconv.FormatInt(message.SourceSeq, 10)
		if message.Provenance != nil && message.Provenance.Revision != "" {
			revision = message.Provenance.Revision
		}
		items = append(items, Item{Message: message, Layer: layer, SourceID: sourceID(message, i), Revision: revision, GroupID: groupID, Priority: priority, Required: required})
	}
	return items, nil
}

func sourceID(message core.ChatMessage, index int) string {
	if message.SourceSeq > 0 {
		return "seq-" + strconv.FormatInt(message.SourceSeq, 10)
	}
	return "message-" + strconv.Itoa(index)
}
func toolMarker(content string) string {
	sum := sha256.Sum256([]byte(content))
	return "[earlier tool result elided bytes=" + strconv.Itoa(len(content)) + " sha256=" + hex.EncodeToString(sum[:8]) + "]"
}
func droppedGroups(e Evidence) int {
	if e.DroppedGroups > 0 {
		return e.DroppedGroups
	}
	seen := make(map[string]struct{}, e.Dropped)
	for _, item := range e.DroppedItems {
		key := item.GroupID
		if key == "" {
			key = item.SourceID
		}
		seen[key] = struct{}{}
	}
	return len(seen)
}

// utf8ByteUpperBoundEstimator budgets every UTF-8 byte as one token plus a
// small structural allowance. This intentionally underuses modern BPE context
// windows, but remains safe when no provider-specific TokenEstimator is bound:
// a byte-oriented tokenizer cannot require more than one input token per byte.
// A future protocol adapter may inject an exact TokenEstimator to recover that
// capacity; this default must not pretend an ASCII heuristic is a hard limit.
type utf8ByteUpperBoundEstimator struct{}

func (utf8ByteUpperBoundEstimator) Estimate(message core.ChatMessage) (Cost, error) {
	bytes, err := (ByteEstimator{}).Estimate(message)
	if err != nil {
		return Cost{}, err
	}
	tokens, err := utf8ByteUpperBoundTokens(message.Content)
	if err != nil {
		return Cost{}, err
	}
	if message.ToolCall != nil {
		n, err := toolCallTokens(*message.ToolCall)
		if err != nil {
			return Cost{}, err
		}
		tokens += n
	}
	for _, toolCall := range message.ToolCalls {
		n, err := toolCallTokens(toolCall)
		if err != nil {
			return Cost{}, err
		}
		tokens += n
	}
	if message.ToolCallID != "" {
		n, err := utf8ByteUpperBoundTokens(message.ToolCallID)
		if err != nil {
			return Cost{}, err
		}
		tokens += n + 2
	}
	return Cost{Bytes: bytes.Bytes, Tokens: tokens + 4}, nil
}

func toolCallTokens(call core.ToolCall) (int64, error) {
	id, err := utf8ByteUpperBoundTokens(call.ID)
	if err != nil {
		return 0, err
	}
	name, err := utf8ByteUpperBoundTokens(call.Name)
	if err != nil {
		return 0, err
	}
	args := int64(0)
	if call.Args != nil {
		encoded, err := json.Marshal(call.Args)
		if err != nil {
			return 0, ErrInvalid
		}
		args, err = utf8ByteUpperBoundTokens(string(encoded))
		if err != nil {
			return 0, err
		}
	}
	return id + name + args + 8, nil
}
func (utf8ByteUpperBoundEstimator) EstimateFragment(layer Layer, text string) (Cost, error) {
	bytes, err := (ByteEstimator{}).EstimateFragment(layer, text)
	if err != nil {
		return Cost{}, err
	}
	tokens, err := utf8ByteUpperBoundTokens(text)
	if err != nil {
		return Cost{}, err
	}
	return Cost{Bytes: bytes.Bytes, Tokens: tokens + 4}, nil
}
func utf8ByteUpperBoundTokens(text string) (int64, error) {
	if !utf8.ValidString(text) {
		return 0, ErrInvalid
	}
	for _, r := range text {
		if r == 0 {
			return 0, ErrInvalid
		}
	}
	return int64(len(text)), nil
}
