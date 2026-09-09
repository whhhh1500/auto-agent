package openai

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/whhhh1500/auto-agent/pkg/app/modelexecution"
)

// ChatCompletionsProtocol implements the OpenAI-compatible /chat/completions
// wire contract. It is reusable with every Provider that can send its bounded
// protocol-neutral outbound request.
type ChatCompletionsProtocol struct{ MaxTokens int }

func (p ChatCompletionsProtocol) Execute(ctx context.Context, request modelexecution.Request, provider modelexecution.Provider, emit modelexecution.Emit) error {
	if provider == nil || ctx == nil || p.MaxTokens < 0 {
		return fmt.Errorf("openai chat execution is invalid")
	}
	aliases, err := newToolNameAliases(request)
	if err != nil {
		return err
	}
	body, err := marshalChatRequestWithAliases(request, p.MaxTokens, aliases)
	if err != nil {
		return err
	}
	response, err := provider.Send(ctx, request.Plan, modelexecution.OutboundRequest{Method: "POST", Path: "/chat/completions", Headers: map[string]string{"Content-Type": "application/json", "Accept": "text/event-stream, application/json"}, Body: body, MaxResponseBytes: modelexecution.DefaultMaxResponseBytes})
	if err != nil {
		return err
	}
	defer response.Body.Close()
	return parseChatResponseWithAliases(response.Body, aliases, emit)
}

func marshalChatRequest(request modelexecution.Request, maxTokens int) ([]byte, error) {
	aliases, err := newToolNameAliases(request)
	if err != nil {
		return nil, err
	}
	return marshalChatRequestWithAliases(request, maxTokens, aliases)
}

func marshalChatRequestWithAliases(request modelexecution.Request, maxTokens int, aliases *toolNameAliases) ([]byte, error) {
	messages := make([]map[string]any, 0, len(request.Messages)+1)
	if request.System != "" {
		messages = append(messages, map[string]any{"role": "system", "content": request.System})
	}
	for _, message := range request.Messages {
		item := map[string]any{"role": message.Role, "content": message.Content}
		if message.ToolCallID != "" {
			item["tool_call_id"] = message.ToolCallID
		}
		if len(message.ToolCalls) > 0 {
			calls := make([]map[string]any, 0, len(message.ToolCalls))
			for _, call := range message.ToolCalls {
				name, err := aliases.wire(call.Name)
				if err != nil {
					return nil, err
				}
				item := map[string]any{"id": call.ID, "type": "function", "function": map[string]any{"name": name, "arguments": string(call.Arguments)}}
				if call.Continuation != "" {
					extra, err := decodeChatContinuation(call.Continuation)
					if err != nil {
						return nil, err
					}
					item["extra_content"] = extra
				}
				calls = append(calls, item)
			}
			item["tool_calls"] = calls
		}
		messages = append(messages, item)
	}
	payload := map[string]any{"model": request.Plan.Catalog.WireModel, "stream": true, "messages": messages, "stream_options": map[string]any{"include_usage": true}}
	if maxTokens > 0 {
		payload["max_tokens"] = maxTokens
	}
	if len(request.Tools) > 0 {
		tools := make([]map[string]any, 0, len(request.Tools))
		for _, tool := range request.Tools {
			var parameters any
			if len(tool.Parameters) == 0 {
				parameters = map[string]any{"type": "object", "properties": map[string]any{}}
			} else if err := json.Unmarshal(tool.Parameters, &parameters); err != nil {
				return nil, fmt.Errorf("openai tool parameters: %w", err)
			}
			name, err := aliases.wire(tool.Name)
			if err != nil {
				return nil, err
			}
			tools = append(tools, map[string]any{"type": "function", "function": map[string]any{"name": name, "description": aliases.description(tool.Name, tool.Description), "parameters": parameters}})
		}
		payload["tools"] = tools
	}
	return json.Marshal(payload)
}

func parseChatResponse(body io.Reader, emit modelexecution.Emit) error {
	return parseChatResponseWithAliases(body, nil, emit)
}

func parseChatResponseWithAliases(body io.Reader, aliases *toolNameAliases, emit modelexecution.Emit) error {
	reader := bufio.NewReader(&limitReader{reader: body, remaining: modelexecution.DefaultMaxResponseBytes})
	first, err := firstNonSpace(reader)
	if err != nil {
		return err
	}
	if first == 'd' {
		return parseChatSSEWithAliases(reader, aliases, emit)
	}
	raw, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	return parseChatSingleWithAliases(raw, aliases, emit)
}

type limitReader struct {
	reader    io.Reader
	remaining int64
}

func (r *limitReader) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		var probe [1]byte
		n, err := r.reader.Read(probe[:])
		if n > 0 {
			return 0, fmt.Errorf("model response exceeded limit")
		}
		return 0, err
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.reader.Read(p)
	r.remaining -= int64(n)
	return n, err
}
func firstNonSpace(r *bufio.Reader) (byte, error) {
	for {
		value, err := r.Peek(1)
		if err != nil {
			return 0, err
		}
		if strings.ContainsRune(" \t\r\n", rune(value[0])) {
			_, _ = r.Discard(1)
			continue
		}
		return value[0], nil
	}
}

func parseChatSSE(reader io.Reader, emit modelexecution.Emit) error {
	return parseChatSSEWithAliases(reader, nil, emit)
}

func parseChatSSEWithAliases(reader io.Reader, aliases *toolNameAliases, emit modelexecution.Emit) error {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, 1<<20), 1<<20)
	var pendingFinish *modelexecution.FinishReason
	hasTools := false
	done := false
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "[DONE]" {
			done = true
			break
		}
		var chunk struct {
			Choices []struct {
				Delta struct {
					Content   *string `json:"content"`
					ToolCalls []struct {
						Index        *int            `json:"index"`
						ID           string          `json:"id"`
						ExtraContent json.RawMessage `json:"extra_content"`
						Function     struct {
							Name      string `json:"name"`
							Arguments string `json:"arguments"`
						} `json:"function"`
					} `json:"tool_calls"`
				} `json:"delta"`
				FinishReason *string `json:"finish_reason"`
			} `json:"choices"`
			Usage *struct {
				PromptTokens     int64 `json:"prompt_tokens"`
				CompletionTokens int64 `json:"completion_tokens"`
			} `json:"usage"`
		}
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			return fmt.Errorf("openai SSE event: %w", err)
		}
		for _, choice := range chunk.Choices {
			if choice.Delta.Content != nil && *choice.Delta.Content != "" {
				if err := emit(modelexecution.Event{Kind: modelexecution.EventTextDelta, Text: *choice.Delta.Content}); err != nil {
					return err
				}
			}
			for _, call := range choice.Delta.ToolCalls {
				hasTools = true
				index := 0
				if call.Index != nil {
					index = *call.Index
				}
				fragment := []byte(call.Function.Arguments)
				continuation, err := encodeChatContinuation(call.ExtraContent)
				if err != nil {
					return err
				}
				name, err := responseToolName(aliases, call.Function.Name)
				if err != nil {
					return err
				}
				if err := emit(modelexecution.Event{Kind: modelexecution.EventToolCallDelta, ToolCall: &modelexecution.ToolCallDelta{Index: index, ID: call.ID, Name: name, ArgumentsFragment: fragment, Continuation: continuation}}); err != nil {
					return err
				}
			}
			if choice.FinishReason != nil {
				reason, err := finish(*choice.FinishReason)
				if err != nil {
					return err
				}
				pendingFinish = &reason
			}
		}
		if chunk.Usage != nil {
			if err := emit(modelexecution.Event{Kind: modelexecution.EventUsage, Usage: &modelexecution.Usage{InputTokens: chunk.Usage.PromptTokens, OutputTokens: chunk.Usage.CompletionTokens}}); err != nil {
				return err
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if !done {
		return fmt.Errorf("openai SSE stream ended without [DONE]")
	}
	if pendingFinish == nil {
		inferred := modelexecution.FinishStop
		if hasTools {
			inferred = modelexecution.FinishToolCalls
		}
		pendingFinish = &inferred
	}
	return emit(modelexecution.Event{Kind: modelexecution.EventFinish, Finish: *pendingFinish})
}

func parseChatSingle(raw []byte, emit modelexecution.Emit) error {
	return parseChatSingleWithAliases(raw, nil, emit)
}

func parseChatSingleWithAliases(raw []byte, aliases *toolNameAliases, emit modelexecution.Emit) error {
	var response struct {
		Choices []struct {
			Message struct {
				Content   string `json:"content"`
				ToolCalls []struct {
					ID           string          `json:"id"`
					ExtraContent json.RawMessage `json:"extra_content"`
					Function     struct {
						Name      string `json:"name"`
						Arguments string `json:"arguments"`
					} `json:"function"`
				} `json:"tool_calls"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
		Usage *struct {
			PromptTokens     int64 `json:"prompt_tokens"`
			CompletionTokens int64 `json:"completion_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return err
	}
	if len(response.Choices) == 0 {
		return fmt.Errorf("openai response has no choices")
	}
	choice := response.Choices[0]
	if choice.Message.Content != "" {
		if err := emit(modelexecution.Event{Kind: modelexecution.EventTextDelta, Text: choice.Message.Content}); err != nil {
			return err
		}
	}
	for index, call := range choice.Message.ToolCalls {
		continuation, err := encodeChatContinuation(call.ExtraContent)
		if err != nil {
			return err
		}
		name, err := responseToolName(aliases, call.Function.Name)
		if err != nil {
			return err
		}
		if err := emit(modelexecution.Event{Kind: modelexecution.EventToolCallDelta, ToolCall: &modelexecution.ToolCallDelta{Index: index, ID: call.ID, Name: name, ArgumentsFragment: []byte(call.Function.Arguments), Continuation: continuation}}); err != nil {
			return err
		}
	}
	if response.Usage != nil {
		if err := emit(modelexecution.Event{Kind: modelexecution.EventUsage, Usage: &modelexecution.Usage{InputTokens: response.Usage.PromptTokens, OutputTokens: response.Usage.CompletionTokens}}); err != nil {
			return err
		}
	}
	reason, err := finishOrInfer(choice.FinishReason, len(choice.Message.ToolCalls) > 0)
	if err != nil {
		return err
	}
	return emit(modelexecution.Event{Kind: modelexecution.EventFinish, Finish: reason})
}

func responseToolName(aliases *toolNameAliases, wire string) (string, error) {
	// Chat-completions commonly omits function.name after the first streamed
	// arguments fragment. The normalized stream state binds that empty fragment
	// to the already-established call index; only an asserted name is decoded.
	if wire == "" {
		return "", nil
	}
	return aliases.internal(wire)
}
func finish(value string) (modelexecution.FinishReason, error) {
	switch value {
	case "stop", "length":
		return modelexecution.FinishStop, nil
	case "tool_calls", "function_call":
		return modelexecution.FinishToolCalls, nil
	default:
		return "", fmt.Errorf("openai finish reason is unsupported")
	}
}
func finishOrInfer(value string, hasTools bool) (modelexecution.FinishReason, error) {
	if value == "" {
		if hasTools {
			return modelexecution.FinishToolCalls, nil
		}
		return modelexecution.FinishStop, nil
	}
	return finish(value)
}
