package anthropic

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/cc-auto-agent/harness-core/pkg/app/modelcontrol"
	"github.com/cc-auto-agent/harness-core/pkg/app/modelexecution"
)

// MessagesProtocol maps only text and local function tools. It deliberately
// fails closed for server/built-in tools and any event the normalized contract
// cannot preserve. Thinking, signatures, and redacted thinking are recognized
// but never emitted as assistant text.
type MessagesProtocol struct{ MaxTokens int }

func (p MessagesProtocol) Execute(ctx context.Context, request modelexecution.Request, provider modelexecution.Provider, emit modelexecution.Emit) error {
	if ctx == nil || provider == nil || p.MaxTokens <= 0 {
		return fmt.Errorf("anthropic messages execution is invalid")
	}
	body, err := marshalMessagesRequest(request, p.MaxTokens)
	if err != nil {
		return err
	}
	response, err := provider.Send(ctx, request.Plan, modelexecution.OutboundRequest{Method: "POST", Path: "/v1/messages", Body: body, MaxResponseBytes: modelexecution.DefaultMaxResponseBytes})
	if err != nil {
		return err
	}
	defer response.Body.Close()
	return parseMessagesResponse(response.Body, emit)
}

func NewMessagesProtocolRegistration(binding modelcontrol.ImplementationBinding, maxTokens int) (modelexecution.ProtocolRegistration, error) {
	if binding.Ref.ID == "" || binding.Ref.Version == "" || binding.ImplementationRevision == "" || maxTokens <= 0 {
		return modelexecution.ProtocolRegistration{}, fmt.Errorf("anthropic messages protocol registration is invalid")
	}
	protocol := MessagesProtocol{MaxTokens: maxTokens}
	return modelexecution.ProtocolRegistration{Binding: binding, Factory: func() (modelexecution.Protocol, error) { return protocol, nil }}, nil
}

type wireMessage struct {
	Role    string `json:"role"`
	Content []any  `json:"content"`
}

func marshalMessagesRequest(request modelexecution.Request, maxTokens int) ([]byte, error) {
	messages := make([]wireMessage, 0, len(request.Messages))
	for _, message := range request.Messages {
		blocks, role, err := messageBlocks(message)
		if err != nil {
			return nil, err
		}
		if len(messages) > 0 && messages[len(messages)-1].Role == role {
			messages[len(messages)-1].Content = append(messages[len(messages)-1].Content, blocks...)
		} else {
			messages = append(messages, wireMessage{Role: role, Content: blocks})
		}
	}
	payload := map[string]any{"model": request.Plan.Catalog.WireModel, "max_tokens": maxTokens, "stream": true, "messages": messages}
	if request.System != "" {
		payload["system"] = request.System
	}
	if len(request.Tools) > 0 {
		tools := make([]map[string]any, 0, len(request.Tools))
		for _, tool := range request.Tools {
			var schema any
			if len(tool.Parameters) == 0 {
				schema = map[string]any{"type": "object", "properties": map[string]any{}}
			} else if err := json.Unmarshal(tool.Parameters, &schema); err != nil {
				return nil, fmt.Errorf("anthropic tool schema: %w", err)
			}
			tools = append(tools, map[string]any{"name": tool.Name, "description": tool.Description, "input_schema": schema})
		}
		payload["tools"] = tools
	}
	return json.Marshal(payload)
}

func messageBlocks(message modelexecution.Message) ([]any, string, error) {
	switch message.Role {
	case "user":
		return []any{map[string]any{"type": "text", "text": message.Content}}, "user", nil
	case "assistant":
		blocks := make([]any, 0, len(message.ToolCalls)+1)
		if message.Content != "" {
			blocks = append(blocks, map[string]any{"type": "text", "text": message.Content})
		}
		for _, call := range message.ToolCalls {
			if call.Continuation != "" {
				return nil, "", fmt.Errorf("anthropic cannot replay this tool continuation")
			}
			var input any
			if err := json.Unmarshal(call.Arguments, &input); err != nil {
				return nil, "", fmt.Errorf("anthropic tool arguments: %w", err)
			}
			blocks = append(blocks, map[string]any{"type": "tool_use", "id": call.ID, "name": call.Name, "input": input})
		}
		if len(blocks) == 0 {
			blocks = append(blocks, map[string]any{"type": "text", "text": ""})
		}
		return blocks, "assistant", nil
	case "tool":
		return []any{map[string]any{"type": "tool_result", "tool_use_id": message.ToolCallID, "content": message.Content}}, "user", nil
	default:
		return nil, "", fmt.Errorf("anthropic message role is unsupported")
	}
}

type messageUsage struct {
	InputTokens  int64 `json:"input_tokens"`
	OutputTokens int64 `json:"output_tokens"`
}
type messageBlock struct {
	Type      string          `json:"type"`
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Text      string          `json:"text"`
	Thinking  string          `json:"thinking"`
	Signature string          `json:"signature"`
	Data      json.RawMessage `json:"data"`
	Input     json.RawMessage `json:"input"`
}
type finalMessage struct {
	Content    []messageBlock `json:"content"`
	Usage      messageUsage   `json:"usage"`
	StopReason string         `json:"stop_reason"`
}
type contentDelta struct {
	Type        string `json:"type"`
	Text        string `json:"text"`
	PartialJSON string `json:"partial_json"`
	Thinking    string `json:"thinking"`
	Signature   string `json:"signature"`
	StopReason  string `json:"stop_reason"`
}
type streamEvent struct {
	Type    string `json:"type"`
	Index   *int   `json:"index"`
	Message *struct {
		Usage messageUsage `json:"usage"`
	} `json:"message"`
	ContentBlock *messageBlock `json:"content_block"`
	Delta        *contentDelta `json:"delta"`
	Usage        *messageUsage `json:"usage"`
}

type blockState struct {
	kind, id, name, args string
	stopped              bool
}
type messagesStreamState struct {
	started, deltaSeen, stopped bool
	inputTokens, outputTokens   int64
	stopReasonValue             string
	events, textBytes, ignored  int
	blocks                      map[int]*blockState
}

func parseMessagesResponse(body io.Reader, emit modelexecution.Emit) error {
	reader := bufio.NewReader(body)
	first, err := firstNonSpace(reader)
	if err != nil {
		return err
	}
	if first == 'd' || first == 'e' {
		return parseMessagesSSE(reader, emit)
	}
	raw, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	return parseMessagesSingle(raw, emit)
}

func firstNonSpace(reader *bufio.Reader) (byte, error) {
	for {
		value, err := reader.Peek(1)
		if err != nil {
			return 0, err
		}
		if strings.ContainsRune(" \t\r\n", rune(value[0])) {
			_, _ = reader.Discard(1)
			continue
		}
		return value[0], nil
	}
}

func parseMessagesSSE(reader io.Reader, emit modelexecution.Emit) error {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, 1<<20), int(modelexecution.DefaultMaxResponseBytes))
	state := messagesStreamState{blocks: make(map[int]*blockState)}
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "" {
			continue
		}
		var event streamEvent
		if err := json.Unmarshal([]byte(data), &event); err != nil {
			return fmt.Errorf("anthropic SSE event: %w", err)
		}
		if err := state.accept(event, emit); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if !state.stopped {
		return fmt.Errorf("anthropic stream ended before message_stop")
	}
	return nil
}

func (s *messagesStreamState) accept(event streamEvent, emit modelexecution.Emit) error {
	s.events++
	if event.Type == "" || s.events > modelexecution.DefaultMaxEvents || s.stopped {
		return fmt.Errorf("anthropic stream event is invalid")
	}
	if !s.started {
		if event.Type != "message_start" || event.Message == nil || event.Message.Usage.InputTokens < 0 {
			return fmt.Errorf("anthropic stream did not start with usage")
		}
		s.started, s.inputTokens = true, event.Message.Usage.InputTokens
		return nil
	}
	switch event.Type {
	case "ping":
		return nil
	case "content_block_start":
		return s.start(event, emit)
	case "content_block_delta":
		return s.delta(event, emit)
	case "content_block_stop":
		return s.stopBlock(event)
	case "message_delta":
		return s.messageDelta(event)
	case "message_stop":
		return s.messageStop(emit)
	case "error":
		return fmt.Errorf("anthropic stream error")
	default:
		return fmt.Errorf("anthropic stream event is unsupported")
	}
}

func (s *messagesStreamState) start(event streamEvent, emit modelexecution.Emit) error {
	if event.Index == nil || event.ContentBlock == nil || *event.Index < 0 || *event.Index != len(s.blocks) || len(s.blocks) >= modelexecution.DefaultMaxEvents {
		return fmt.Errorf("anthropic content block start is invalid")
	}
	block := event.ContentBlock
	state := &blockState{kind: block.Type}
	switch block.Type {
	case "text":
		if err := s.emitText(block.Text, emit); err != nil {
			return err
		}
	case "thinking", "redacted_thinking":
		if err := s.ignoreParts(block.Thinking, block.Signature, string(block.Data)); err != nil {
			return err
		}
	case "tool_use":
		if *event.Index >= modelexecution.DefaultMaxToolCalls || block.ID == "" || block.Name == "" || len(block.Input) > 0 && string(block.Input) != "{}" {
			return fmt.Errorf("anthropic tool block identity is invalid")
		}
		state.id, state.name = block.ID, block.Name
		if err := emit(modelexecution.Event{Kind: modelexecution.EventToolCallDelta, ToolCall: &modelexecution.ToolCallDelta{Index: *event.Index, ID: block.ID, Name: block.Name}}); err != nil {
			return err
		}
	default:
		return fmt.Errorf("anthropic content block is unsupported")
	}
	s.blocks[*event.Index] = state
	return nil
}

func (s *messagesStreamState) delta(event streamEvent, emit modelexecution.Emit) error {
	if event.Index == nil || event.Delta == nil || *event.Index < 0 {
		return fmt.Errorf("anthropic content delta is invalid")
	}
	block := s.blocks[*event.Index]
	if block == nil || block.stopped {
		return fmt.Errorf("anthropic content delta lifecycle is invalid")
	}
	switch event.Delta.Type {
	case "text_delta":
		if block.kind != "text" {
			return fmt.Errorf("anthropic text delta identity is invalid")
		}
		return s.emitText(event.Delta.Text, emit)
	case "input_json_delta":
		if block.kind != "tool_use" || len(block.args)+len(event.Delta.PartialJSON) > modelexecution.DefaultMaxToolArgBytes {
			return fmt.Errorf("anthropic tool delta is invalid")
		}
		block.args += event.Delta.PartialJSON
		if event.Delta.PartialJSON == "" {
			return nil
		}
		return emit(modelexecution.Event{Kind: modelexecution.EventToolCallDelta, ToolCall: &modelexecution.ToolCallDelta{Index: *event.Index, ID: block.id, Name: block.name, ArgumentsFragment: []byte(event.Delta.PartialJSON)}})
	case "thinking_delta", "signature_delta":
		if block.kind != "thinking" && block.kind != "redacted_thinking" {
			return fmt.Errorf("anthropic private delta identity is invalid")
		}
		return s.ignoreParts(event.Delta.Thinking, event.Delta.Signature)
	default:
		return fmt.Errorf("anthropic content delta is unsupported")
	}
}

func (s *messagesStreamState) emitText(value string, emit modelexecution.Emit) error {
	if value == "" {
		return nil
	}
	if s.textBytes > modelexecution.DefaultMaxTextBytes-len(value) {
		return fmt.Errorf("anthropic text bound")
	}
	s.textBytes += len(value)
	return emit(modelexecution.Event{Kind: modelexecution.EventTextDelta, Text: value})
}
func (s *messagesStreamState) ignore(value string) error {
	if len(value) > int(modelexecution.DefaultMaxResponseBytes) || s.ignored > int(modelexecution.DefaultMaxResponseBytes)-len(value) {
		return fmt.Errorf("anthropic private content bound")
	}
	s.ignored += len(value)
	return nil
}
func (s *messagesStreamState) ignoreParts(values ...string) error {
	for _, value := range values {
		if err := s.ignore(value); err != nil {
			return err
		}
	}
	return nil
}
func (s *messagesStreamState) stopBlock(event streamEvent) error {
	if event.Index == nil || *event.Index < 0 || s.blocks[*event.Index] == nil || s.blocks[*event.Index].stopped {
		return fmt.Errorf("anthropic content block stop is invalid")
	}
	block := s.blocks[*event.Index]
	if block.kind == "tool_use" && (!json.Valid([]byte(block.args)) || !jsonObject(block.args)) {
		return fmt.Errorf("anthropic tool arguments are incomplete")
	}
	block.stopped = true
	return nil
}
func (s *messagesStreamState) messageDelta(event streamEvent) error {
	if s.deltaSeen || event.Delta == nil || event.Usage == nil || event.Usage.OutputTokens < 0 || !s.allStopped() {
		return fmt.Errorf("anthropic message delta is invalid")
	}
	s.deltaSeen, s.outputTokens, s.stopReasonValue = true, event.Usage.OutputTokens, event.Delta.StopReason
	return nil
}
func (s *messagesStreamState) messageStop(emit modelexecution.Emit) error {
	if !s.deltaSeen || !s.allStopped() {
		return fmt.Errorf("anthropic message stop is invalid")
	}
	reason, err := anthropicFinish(s.stopReason())
	if err != nil {
		return err
	}
	if reason == modelexecution.FinishToolCalls && !s.hasTools() {
		return fmt.Errorf("anthropic tool finish has no tools")
	}
	if err := emit(modelexecution.Event{Kind: modelexecution.EventUsage, Usage: &modelexecution.Usage{InputTokens: s.inputTokens, OutputTokens: s.outputTokens}}); err != nil {
		return err
	}
	s.stopped = true
	return emit(modelexecution.Event{Kind: modelexecution.EventFinish, Finish: reason})
}
func (s *messagesStreamState) stopReason() string { return s.stopReasonValue }
func (s *messagesStreamState) allStopped() bool {
	for _, block := range s.blocks {
		if !block.stopped {
			return false
		}
	}
	return true
}
func (s *messagesStreamState) hasTools() bool {
	for _, block := range s.blocks {
		if block.kind == "tool_use" {
			return true
		}
	}
	return false
}
func anthropicFinish(value string) (modelexecution.FinishReason, error) {
	switch value {
	case "end_turn", "stop_sequence", "refusal":
		return modelexecution.FinishStop, nil
	case "tool_use":
		return modelexecution.FinishToolCalls, nil
	default:
		return "", fmt.Errorf("anthropic stop reason is unsupported")
	}
}
func parseMessagesSingle(raw []byte, emit modelexecution.Emit) error {
	var message finalMessage
	if err := json.Unmarshal(raw, &message); err != nil || message.Usage.InputTokens < 0 || message.Usage.OutputTokens < 0 {
		return fmt.Errorf("anthropic single response is invalid")
	}
	state := messagesStreamState{started: true, inputTokens: message.Usage.InputTokens, outputTokens: message.Usage.OutputTokens, blocks: make(map[int]*blockState)}
	for index, block := range message.Content {
		blockForStart := block
		if blockForStart.Type == "text" {
			blockForStart.Text = ""
		}
		if blockForStart.Type == "tool_use" {
			// The streaming protocol starts tool_use with an empty input and
			// supplies the JSON through deltas; normalize a complete response
			// through that same path without treating its final input as a start.
			blockForStart.Input = []byte("{}")
		}
		if err := state.start(streamEvent{Index: &index, ContentBlock: &blockForStart}, emit); err != nil {
			return err
		}
		switch block.Type {
		case "text":
			if err := state.emitText(block.Text, emit); err != nil {
				return err
			}
		case "tool_use":
			if !json.Valid(block.Input) {
				return fmt.Errorf("anthropic single tool input is invalid")
			}
			if err := state.delta(streamEvent{Index: &index, Delta: &contentDelta{Type: "input_json_delta", PartialJSON: string(block.Input)}}, emit); err != nil {
				return err
			}
		}
		if err := state.stopBlock(streamEvent{Index: &index}); err != nil {
			return err
		}
	}
	state.deltaSeen = true
	reason, err := anthropicFinish(message.StopReason)
	if err != nil {
		return err
	}
	if reason == modelexecution.FinishToolCalls && !state.hasTools() {
		return fmt.Errorf("anthropic tool finish has no tools")
	}
	if err := emit(modelexecution.Event{Kind: modelexecution.EventUsage, Usage: &modelexecution.Usage{InputTokens: state.inputTokens, OutputTokens: state.outputTokens}}); err != nil {
		return err
	}
	return emit(modelexecution.Event{Kind: modelexecution.EventFinish, Finish: reason})
}

func jsonObject(value string) bool {
	var object map[string]json.RawMessage
	return json.Unmarshal([]byte(value), &object) == nil && object != nil
}
