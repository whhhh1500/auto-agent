package core

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

const (
	StreamKindAssistant = "assistant"
	StreamKindFinish    = "finish"
	FinishStop          = "stop"
	FinishToolCalls     = "tool-calls"
)

const (
	maxModelTextBytes              = 8 << 20
	maxModelToolCalls              = 128
	MaxReportedTokensPerCall int64 = 1_000_000_000_000
	MaxReportedTokensPerRun  int64 = 10_000_000_000_000
)

type GenerateOptions struct {
	Provider     string
	Model        string
	System       string
	Messages     []ChatMessage
	Tools        []ToolSchema
	ModelCall    ModelCallRequest  `json:"-"`
	AcceptedCall AcceptedModelCall `json:"-"`
}

// TokenUsage is the provider-neutral token metering for one model call.
type TokenUsage struct {
	InputTokens  int64 `json:"input_tokens"`
	OutputTokens int64 `json:"output_tokens"`
}

// StreamChunk is the model-facing stream vocabulary. Kind "assistant" carries
// text and optional tool calls; Kind "finish" signals the step ended.
type StreamChunk struct {
	Kind       string // StreamKindAssistant | StreamKindFinish
	Text       string
	ToolCall   *ToolCall   // first tool call, kept for simple consumers
	ToolCalls  []ToolCall  // every tool call in this step, in model order
	Usage      *TokenUsage // reported at most once per model call
	FinishKind string      // FinishStop | FinishToolCalls
}

// LlmAdapter is the provider seam. Every provider implements Stream(); the loop
// and session log share the same vocabulary. Swapping the provider is a config
// choice — the loop never depends on a concrete provider.
type LlmAdapter interface {
	Provider() string
	Stream(ctx context.Context, opts GenerateOptions, emit func(StreamChunk)) error
}

type modelStreamResult struct {
	Text       string
	ToolCalls  []ToolCall
	Usage      TokenUsage
	FinishKind string
}

// consumeModelStream is the single protocol boundary for every model adapter.
// It validates ordering and terminal state, isolates adapter/callback panics,
// and returns one normalized provider-neutral result.
func consumeModelStream(
	ctx context.Context,
	adapter LlmAdapter,
	opts GenerateOptions,
	onAssistant func(StreamChunk) error,
) (result modelStreamResult, err error) {
	if adapter == nil {
		return result, fmt.Errorf("model adapter is nil")
	}
	var text strings.Builder
	calls := []ToolCall{}
	finished := false
	usageSeen := false
	var protocolErr error
	streamClosed := false
	streamCtx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()
	var emitMu sync.Mutex

	emit := func(chunk StreamChunk) {
		emitMu.Lock()
		defer emitMu.Unlock()
		if streamClosed {
			return
		}
		if protocolErr != nil {
			return
		}
		if finished {
			protocolErr = fmt.Errorf("model stream emitted %q after finish", chunk.Kind)
			return
		}
		if chunk.Usage != nil {
			if usageSeen {
				protocolErr = fmt.Errorf("model stream reported usage more than once")
				return
			}
			if chunk.Usage.InputTokens < 0 || chunk.Usage.OutputTokens < 0 {
				protocolErr = fmt.Errorf("model stream reported negative token usage")
				return
			}
			if chunk.Usage.InputTokens > MaxReportedTokensPerCall || chunk.Usage.OutputTokens > MaxReportedTokensPerCall {
				protocolErr = fmt.Errorf("model stream token usage exceeds the per-call limit")
				cancelStream()
				return
			}
			usageSeen = true
			result.Usage = *chunk.Usage
		}

		switch chunk.Kind {
		case StreamKindAssistant:
			if chunk.FinishKind != "" {
				protocolErr = fmt.Errorf("assistant chunk carries finish kind %q", chunk.FinishKind)
				return
			}
			if text.Len()+len(chunk.Text) > maxModelTextBytes {
				protocolErr = fmt.Errorf("model stream exceeded the %d-byte text limit", maxModelTextBytes)
				cancelStream()
				return
			}
			text.WriteString(chunk.Text)
			appendStreamCalls := func(items []ToolCall) {
				for _, call := range items {
					if !containsCall(calls, call) {
						if len(calls) >= maxModelToolCalls {
							protocolErr = fmt.Errorf("model stream exceeded the %d tool-call limit", maxModelToolCalls)
							cancelStream()
							return
						}
						calls = append(calls, call)
					}
				}
			}
			if chunk.ToolCall != nil {
				appendStreamCalls([]ToolCall{*chunk.ToolCall})
			}
			appendStreamCalls(chunk.ToolCalls)
			if onAssistant != nil {
				if callbackErr := safeAssistantCallback(onAssistant, chunk); callbackErr != nil {
					protocolErr = callbackErr
					cancelStream()
				}
			}
		case StreamKindFinish:
			if chunk.Text != "" || chunk.ToolCall != nil || len(chunk.ToolCalls) > 0 {
				protocolErr = fmt.Errorf("finish chunk carries assistant content")
				cancelStream()
				return
			}
			switch chunk.FinishKind {
			case FinishStop, FinishToolCalls:
				finished = true
				result.FinishKind = chunk.FinishKind
			default:
				protocolErr = fmt.Errorf("model stream has invalid finish kind %q", chunk.FinishKind)
				cancelStream()
			}
		default:
			protocolErr = fmt.Errorf("model stream has unknown chunk kind %q", chunk.Kind)
			cancelStream()
		}
	}

	streamErr := safeModelStream(streamCtx, adapter, opts, emit)
	emitMu.Lock()
	streamClosed = true
	emitMu.Unlock()
	if streamErr != nil {
		return result, streamErr
	}
	if protocolErr != nil {
		return result, protocolErr
	}
	if ctx.Err() != nil {
		return result, ctx.Err()
	}
	if !finished {
		return result, fmt.Errorf("model stream ended without a finish chunk")
	}
	seenCallIDs := map[string]bool{}
	for _, call := range calls {
		if err := validateToolCall(call); err != nil {
			return result, err
		}
		if seenCallIDs[call.ID] {
			return result, fmt.Errorf("model stream returned duplicate tool call id %q", call.ID)
		}
		seenCallIDs[call.ID] = true
	}
	if result.FinishKind == FinishToolCalls && len(calls) == 0 {
		return result, fmt.Errorf("model stream finished with tool-calls but returned no calls")
	}
	if result.FinishKind == FinishStop && len(calls) > 0 {
		return result, fmt.Errorf("model stream finished with stop while returning tool calls")
	}
	if text.Len() == 0 && len(calls) == 0 {
		return result, fmt.Errorf("model stream returned neither text nor tool calls")
	}
	result.Text = text.String()
	result.ToolCalls = calls
	return result, nil
}

// ConsumeModelStream runs the strict provider-neutral stream protocol and
// forwards validated chunks to the caller. It intentionally exposes no
// aggregate result, allowing app-level policies to impose smaller budgets
// without first buffering provider output.
func ConsumeModelStream(ctx context.Context, adapter LlmAdapter, opts GenerateOptions, onAssistant func(StreamChunk) error) error {
	_, err := consumeModelStream(ctx, adapter, opts, onAssistant)
	return err
}

func validateToolCall(call ToolCall) error {
	if err := validateToolCallID(call.ID); err != nil {
		return err
	}
	if err := validateCapabilityID(call.Name); err != nil {
		return fmt.Errorf("model stream returned invalid tool name: %w", err)
	}
	return nil
}

func validateToolCallID(id string) error {
	if strings.TrimSpace(id) == "" || len(id) > 256 || strings.ContainsAny(id, "\r\n\x00") {
		return fmt.Errorf("invalid tool call id")
	}
	return nil
}

func safeModelStream(ctx context.Context, adapter LlmAdapter, opts GenerateOptions, emit func(StreamChunk)) (err error) {
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("model adapter panicked")
		}
	}()
	return adapter.Stream(ctx, opts, emit)
}

func safeAssistantCallback(callback func(StreamChunk) error, chunk StreamChunk) (err error) {
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("model stream consumer panicked")
		}
	}()
	return callback(chunk)
}

// MockLlmAdapter runs with no API key and exercises the provider-neutral loop.
type MockLlmAdapter struct{}

func (MockLlmAdapter) Provider() string         { return "mock" }
func (MockLlmAdapter) ArtifactRevision() string { return "mock/v1" }

func (MockLlmAdapter) Stream(_ context.Context, opts GenerateOptions, emit func(StreamChunk)) error {
	if len(opts.Messages) == 0 {
		return fmt.Errorf("mock: no messages")
	}
	last := opts.Messages[len(opts.Messages)-1]

	// A tool result just came back: expose it without assuming a product schema.
	if last.Role == RoleTool {
		emit(StreamChunk{Kind: StreamKindAssistant, Text: "Capability result: " + last.Content})
		emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishStop})
		return nil
	}

	// No tooling available this turn → answer directly (fail-closed).
	if len(opts.Tools) == 0 {
		emit(StreamChunk{Kind: StreamKindAssistant, Text: "No model-facing capabilities are available for this run."})
		emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishStop})
		return nil
	}

	// Use only a schema whose required arguments the mock can supply. The mock
	// is intentionally not a product argument generator, so it skips unknown
	// required schemas instead of emitting an invalid empty call.
	for _, tool := range opts.Tools {
		args, ok := mockToolArgs(tool)
		if !ok {
			continue
		}
		emit(StreamChunk{Kind: StreamKindAssistant, ToolCall: &ToolCall{ID: "call-1", Name: tool.Name, Args: args}})
		emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishToolCalls})
		return nil
	}

	emit(StreamChunk{Kind: StreamKindAssistant, Text: "No model-facing capabilities have mock-safe arguments for this run."})
	emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishStop})
	return nil
}

func mockToolArgs(tool ToolSchema) (map[string]any, bool) {
	if tool.Name == disclosedLibraryID {
		return map[string]any{"action": "list"}, true
	}
	if tool.Parameters == nil {
		return map[string]any{}, true
	}
	required, exists := tool.Parameters["required"]
	if !exists {
		return map[string]any{}, true
	}
	switch values := required.(type) {
	case []any:
		return map[string]any{}, len(values) == 0
	case []string:
		return map[string]any{}, len(values) == 0
	default:
		return nil, false
	}
}
