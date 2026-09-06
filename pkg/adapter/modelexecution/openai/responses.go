package openai

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/cc-auto-agent/harness-core/pkg/app/modelexecution"
)

// ResponsesProtocol implements the OpenAI /responses wire protocol. It only
// maps text, refusal, and ordinary function-call output. Built-in tools and
// other output types fail closed because the normalized execution contract
// cannot represent them without losing semantics.
//
// Stream handling table:
//   - emitted: output_text.delta, refusal.delta, function_call_arguments.delta,
//     response.completed (usage and finish)
//   - verified/no emit: created, in_progress, output_item/content_part added or
//     done, output_text/refusal/function-call-arguments done
//   - ignored/no assistant disclosure: reasoning item and reasoning lifecycle
//     and summary events; their SSE events still count toward stream bounds
//   - rejected: failed, incomplete, error, unknown events, built-in-tool items,
//     and identity/order/final-snapshot conflicts.
type ResponsesProtocol struct{ MaxOutputTokens int }

func (p ResponsesProtocol) Execute(ctx context.Context, request modelexecution.Request, provider modelexecution.Provider, emit modelexecution.Emit) error {
	if provider == nil || ctx == nil || p.MaxOutputTokens < 0 {
		return fmt.Errorf("openai responses execution is invalid")
	}
	body, err := marshalResponsesRequest(request, p.MaxOutputTokens)
	if err != nil {
		return err
	}
	response, err := provider.Send(ctx, request.Plan, modelexecution.OutboundRequest{Method: "POST", Path: "/responses", Headers: map[string]string{"Content-Type": "application/json", "Accept": "text/event-stream, application/json"}, Body: body, MaxResponseBytes: modelexecution.DefaultMaxResponseBytes})
	if err != nil {
		return err
	}
	defer response.Body.Close()
	return parseResponsesResponse(response.Body, emit)
}

func parseResponsesResponse(body io.Reader, emit modelexecution.Emit) error {
	reader := bufio.NewReader(&limitReader{reader: body, remaining: modelexecution.DefaultMaxResponseBytes})
	first, err := firstNonSpace(reader)
	if err != nil {
		return err
	}
	if first == 'd' {
		return parseResponsesSSE(reader, emit)
	}
	raw, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	return parseResponsesSingle(raw, emit)
}

type responsesUsage struct {
	InputTokens  int64 `json:"input_tokens"`
	OutputTokens int64 `json:"output_tokens"`
}

type responsesContent struct {
	Type    string `json:"type"`
	Text    string `json:"text"`
	Refusal string `json:"refusal"`
}

type responsesItem struct {
	ID        string             `json:"id"`
	Type      string             `json:"type"`
	CallID    string             `json:"call_id"`
	Name      string             `json:"name"`
	Arguments string             `json:"arguments"`
	Content   []responsesContent `json:"content"`
}

type responsesFinal struct {
	ID     string          `json:"id"`
	Status string          `json:"status"`
	Output []responsesItem `json:"output"`
	Usage  *responsesUsage `json:"usage"`
}

type responsesSSEEvent struct {
	Type         string            `json:"type"`
	Sequence     *int64            `json:"sequence_number"`
	ItemID       string            `json:"item_id"`
	OutputIndex  *int              `json:"output_index"`
	ContentIndex *int              `json:"content_index"`
	Delta        string            `json:"delta"`
	Text         string            `json:"text"`
	Refusal      string            `json:"refusal"`
	Arguments    string            `json:"arguments"`
	Name         string            `json:"name"`
	Item         *responsesItem    `json:"item"`
	Part         *responsesContent `json:"part"`
	Response     *responsesFinal   `json:"response"`
}

type responseOutputState struct {
	id, kind, callID, name string
	arguments              string
	emitted                bool
	done                   bool
}
type responseContentState struct {
	kind, text string
	done       bool
}
type responsesStreamState struct {
	created, completed bool
	hasSequence        bool
	lastSequence       int64
	responseID         string
	events             int
	outputs            map[int]*responseOutputState
	contents           map[string]*responseContentState
}

func parseResponsesSSE(reader io.Reader, emit modelexecution.Emit) error {
	scanner := bufio.NewScanner(reader)
	// A valid normalized text/tool fragment may be as large as its contract
	// bound; the enclosing limitReader still caps the entire response at 32MiB.
	scanner.Buffer(make([]byte, 0, 1<<20), int(modelexecution.DefaultMaxResponseBytes))
	state := responsesStreamState{outputs: make(map[int]*responseOutputState), contents: make(map[string]*responseContentState)}
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "[DONE]" {
			break
		}
		if len(data) == 0 {
			continue
		}
		var event responsesSSEEvent
		if err := json.Unmarshal([]byte(data), &event); err != nil {
			return fmt.Errorf("openai responses SSE event: %w", err)
		}
		if err := state.accept(event, emit); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if !state.completed {
		return fmt.Errorf("openai responses stream ended before completion")
	}
	return nil
}

func (s *responsesStreamState) accept(event responsesSSEEvent, emit modelexecution.Emit) error {
	if event.Type == "" || event.Sequence == nil || *event.Sequence < 0 || (s.hasSequence && *event.Sequence <= s.lastSequence) {
		return fmt.Errorf("openai responses sequence is invalid")
	}
	s.hasSequence = true
	s.lastSequence = *event.Sequence
	s.events++
	if s.events > modelexecution.DefaultMaxEvents || s.completed {
		return fmt.Errorf("openai responses stream bounds or terminal state")
	}
	if !s.created {
		if event.Type != "response.created" || event.Response == nil || !validResponseID(event.Response.ID) || (event.Response.Status != "queued" && event.Response.Status != "in_progress") {
			return fmt.Errorf("openai responses stream did not start with created")
		}
		s.created, s.responseID = true, event.Response.ID
		return nil
	}
	switch event.Type {
	case "response.queued":
		return s.lifecycle(event.Response, "queued")
	case "response.in_progress":
		return s.lifecycle(event.Response, "in_progress")
	case "response.output_item.added":
		return s.addOutput(event)
	case "response.content_part.added":
		return s.addContent(event)
	case "response.output_text.delta":
		return s.emitContent(event, "output_text", event.Delta, emit)
	case "response.refusal.delta":
		return s.emitContent(event, "refusal", event.Delta, emit)
	case "response.output_text.done":
		return s.finishContent(event, "output_text", event.Text, emit)
	case "response.refusal.done":
		return s.finishContent(event, "refusal", event.Refusal, emit)
	case "response.function_call_arguments.delta":
		return s.emitArguments(event, event.Delta, emit)
	case "response.function_call_arguments.done":
		output, err := s.output(event, "function_call")
		if err != nil || output.done {
			return fmt.Errorf("openai responses function completion is out of order")
		}
		return s.finishArguments(event, emit)
	case "response.content_part.done":
		return s.finishContentPart(event, emit)
	case "response.output_item.done":
		return s.finishOutput(event, emit)
	case "response.reasoning_text.delta", "response.reasoning_text.done", "response.reasoning_summary_part.added", "response.reasoning_summary_part.done", "response.reasoning_summary_text.delta", "response.reasoning_summary_text.done":
		return nil
	case "response.completed":
		if event.Response == nil || event.Response.Status != "completed" || event.Response.ID != s.responseID {
			return fmt.Errorf("openai responses completion is invalid")
		}
		if err := s.reconcileFinal(*event.Response, emit); err != nil {
			return err
		}
		if event.Response.Usage != nil {
			if err := emit(modelexecution.Event{Kind: modelexecution.EventUsage, Usage: &modelexecution.Usage{InputTokens: event.Response.Usage.InputTokens, OutputTokens: event.Response.Usage.OutputTokens}}); err != nil {
				return err
			}
		}
		s.completed = true
		return emit(modelexecution.Event{Kind: modelexecution.EventFinish, Finish: s.finishReason()})
	case "response.failed", "response.incomplete", "error":
		return fmt.Errorf("openai responses terminal failure")
	default:
		return fmt.Errorf("openai responses event is unsupported")
	}
}

func (s *responsesStreamState) addOutput(event responsesSSEEvent) error {
	if event.OutputIndex == nil || event.Item == nil || *event.OutputIndex < 0 || *event.OutputIndex >= modelexecution.DefaultMaxEvents || event.Item.ID == "" {
		return fmt.Errorf("openai responses output item is invalid")
	}
	if _, exists := s.outputs[*event.OutputIndex]; exists {
		return fmt.Errorf("openai responses duplicate output item")
	}
	item := event.Item
	state := &responseOutputState{id: item.ID, kind: item.Type}
	switch item.Type {
	case "message", "reasoning":
	case "function_call":
		if item.CallID == "" || item.Name == "" {
			return fmt.Errorf("openai responses function identity is invalid")
		}
		state.callID, state.name = item.CallID, item.Name
	default:
		return fmt.Errorf("openai responses built-in output is unsupported")
	}
	s.outputs[*event.OutputIndex] = state
	return nil
}

func (s *responsesStreamState) lifecycle(response *responsesFinal, status string) error {
	if response == nil || response.ID != s.responseID || response.Status != status {
		return fmt.Errorf("openai responses lifecycle identity drift")
	}
	return nil
}

func (s *responsesStreamState) addContent(event responsesSSEEvent) error {
	output, err := s.output(event, "message")
	if err != nil || event.ContentIndex == nil || event.Part == nil || *event.ContentIndex < 0 || *event.ContentIndex >= modelexecution.DefaultMaxEvents {
		return fmt.Errorf("openai responses content part is invalid")
	}
	key := contentKey(*event.OutputIndex, *event.ContentIndex, event.ItemID)
	if event.ItemID != output.id || s.contents[key] != nil {
		return fmt.Errorf("openai responses content identity drift")
	}
	switch event.Part.Type {
	case "output_text", "refusal", "reasoning_text":
		s.contents[key] = &responseContentState{kind: event.Part.Type}
		return nil
	default:
		return fmt.Errorf("openai responses content is unsupported")
	}
}

func (s *responsesStreamState) emitContent(event responsesSSEEvent, kind, delta string, emit modelexecution.Emit) error {
	content, err := s.content(event, kind)
	output, outputErr := s.output(event, "message")
	if err != nil || outputErr != nil || output.done || content.done {
		return fmt.Errorf("openai responses content delta is invalid")
	}
	if int64(len(content.text))+int64(len(delta)) > modelexecution.DefaultMaxTextBytes {
		return fmt.Errorf("openai responses text bound")
	}
	content.text += delta
	if delta == "" {
		return nil
	}
	return emit(modelexecution.Event{Kind: modelexecution.EventTextDelta, Text: delta})
}

func (s *responsesStreamState) finishContent(event responsesSSEEvent, kind, final string, emit modelexecution.Emit) error {
	content, err := s.content(event, kind)
	output, outputErr := s.output(event, "message")
	if err != nil || outputErr != nil || output.done || content.done {
		return fmt.Errorf("openai responses content completion is invalid")
	}
	if err := s.emitSuffix(content, final, emit); err != nil {
		return err
	}
	content.done = true
	return nil
}

func (s *responsesStreamState) finishContentPart(event responsesSSEEvent, emit modelexecution.Emit) error {
	if event.Part == nil {
		return fmt.Errorf("openai responses content completion is missing")
	}
	value := event.Part.Text
	if event.Part.Type == "refusal" {
		value = event.Part.Refusal
	}
	if event.Part.Type != "output_text" && event.Part.Type != "refusal" {
		return nil
	}
	return s.finishContent(event, event.Part.Type, value, emit)
}

func (s *responsesStreamState) emitSuffix(content *responseContentState, final string, emit modelexecution.Emit) error {
	if !strings.HasPrefix(final, content.text) || int64(len(final)) > modelexecution.DefaultMaxTextBytes {
		return fmt.Errorf("openai responses final content conflicts")
	}
	suffix := final[len(content.text):]
	content.text = final
	if suffix == "" {
		return nil
	}
	return emit(modelexecution.Event{Kind: modelexecution.EventTextDelta, Text: suffix})
}

func (s *responsesStreamState) emitArguments(event responsesSSEEvent, delta string, emit modelexecution.Emit) error {
	output, err := s.output(event, "function_call")
	if err != nil || event.ItemID != output.id || output.done || *event.OutputIndex >= modelexecution.DefaultMaxToolCalls {
		return fmt.Errorf("openai responses function delta is invalid")
	}
	if len(output.arguments)+len(delta) > modelexecution.DefaultMaxToolArgBytes {
		return fmt.Errorf("openai responses function argument bound")
	}
	output.arguments += delta
	if delta == "" {
		return nil
	}
	output.emitted = true
	return emit(modelexecution.Event{Kind: modelexecution.EventToolCallDelta, ToolCall: &modelexecution.ToolCallDelta{Index: *event.OutputIndex, ID: output.callID, Name: output.name, ArgumentsFragment: []byte(delta)}})
}

func (s *responsesStreamState) finishArguments(event responsesSSEEvent, emit modelexecution.Emit) error {
	output, err := s.output(event, "function_call")
	if err != nil || event.ItemID != output.id || *event.OutputIndex >= modelexecution.DefaultMaxToolCalls || (event.Name != "" && event.Name != output.name) || !strings.HasPrefix(event.Arguments, output.arguments) || len(event.Arguments) > modelexecution.DefaultMaxToolArgBytes {
		return fmt.Errorf("openai responses function completion is invalid")
	}
	suffix := event.Arguments[len(output.arguments):]
	output.arguments = event.Arguments
	if suffix == "" && output.emitted {
		return nil
	}
	output.emitted = true
	return emit(modelexecution.Event{Kind: modelexecution.EventToolCallDelta, ToolCall: &modelexecution.ToolCallDelta{Index: *event.OutputIndex, ID: output.callID, Name: output.name, ArgumentsFragment: []byte(suffix)}})
}

func (s *responsesStreamState) finishOutput(event responsesSSEEvent, emit modelexecution.Emit) error {
	if event.Item == nil || event.OutputIndex == nil {
		return fmt.Errorf("openai responses output completion is missing")
	}
	output, err := s.output(event, event.Item.Type)
	if err != nil || output.id != event.Item.ID || output.done {
		return fmt.Errorf("openai responses output completion is invalid")
	}
	if err := s.reconcileItem(*event.OutputIndex, *event.Item, emit); err != nil {
		return err
	}
	output.done = true
	return nil
}

func (s *responsesStreamState) reconcileFinal(final responsesFinal, emit modelexecution.Emit) error {
	if len(final.Output) > modelexecution.DefaultMaxEvents || len(final.Output) != len(s.outputs) {
		return fmt.Errorf("openai responses output bound")
	}
	for index, item := range final.Output {
		if err := s.reconcileItem(index, item, emit); err != nil {
			return err
		}
		if item.Type == "message" && s.contentCount(index) != len(item.Content) {
			return fmt.Errorf("openai responses final content count conflicts")
		}
	}
	return nil
}

func (s *responsesStreamState) contentCount(outputIndex int) int {
	prefix := fmt.Sprintf("%d/", outputIndex)
	count := 0
	for key := range s.contents {
		if strings.HasPrefix(key, prefix) {
			count++
		}
	}
	return count
}

func (s *responsesStreamState) reconcileItem(index int, item responsesItem, emit modelexecution.Emit) error {
	output := s.outputs[index]
	if output == nil || output.id != item.ID || output.kind != item.Type {
		return fmt.Errorf("openai responses final output identity drift")
	}
	switch item.Type {
	case "reasoning":
		return nil
	case "function_call":
		if item.CallID != output.callID || item.Name != output.name {
			return fmt.Errorf("openai responses final function identity drift")
		}
		return s.finishArguments(responsesSSEEvent{ItemID: item.ID, OutputIndex: &index, Arguments: item.Arguments, Name: item.Name}, emit)
	case "message":
		for contentIndex, content := range item.Content {
			if content.Type != "output_text" && content.Type != "refusal" {
				return fmt.Errorf("openai responses final message content is unsupported")
			}
			key := contentKey(index, contentIndex, item.ID)
			state := s.contents[key]
			if state == nil || state.kind != content.Type {
				return fmt.Errorf("openai responses final content identity drift")
			}
			value := content.Text
			if content.Type == "refusal" {
				value = content.Refusal
			}
			if err := s.emitSuffix(state, value, emit); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("openai responses final output is unsupported")
	}
}

func (s *responsesStreamState) output(event responsesSSEEvent, kind string) (*responseOutputState, error) {
	if event.OutputIndex == nil || *event.OutputIndex < 0 {
		return nil, fmt.Errorf("openai responses output index is invalid")
	}
	output := s.outputs[*event.OutputIndex]
	if output == nil || output.kind != kind {
		return nil, fmt.Errorf("openai responses output identity is invalid")
	}
	return output, nil
}
func (s *responsesStreamState) content(event responsesSSEEvent, kind string) (*responseContentState, error) {
	output, err := s.output(event, "message")
	if err != nil || event.ContentIndex == nil || event.ItemID != output.id || *event.ContentIndex < 0 {
		return nil, fmt.Errorf("openai responses content identity is invalid")
	}
	content := s.contents[contentKey(*event.OutputIndex, *event.ContentIndex, event.ItemID)]
	if content == nil || content.kind != kind {
		return nil, fmt.Errorf("openai responses content identity is invalid")
	}
	return content, nil
}
func (s *responsesStreamState) finishReason() modelexecution.FinishReason {
	for _, output := range s.outputs {
		if output.kind == "function_call" {
			return modelexecution.FinishToolCalls
		}
	}
	return modelexecution.FinishStop
}
func contentKey(output, content int, itemID string) string {
	return fmt.Sprintf("%d/%d/%s", output, content, itemID)
}

func parseResponsesSingle(raw []byte, emit modelexecution.Emit) error {
	var response responsesFinal
	if err := json.Unmarshal(raw, &response); err != nil || response.Status != "completed" || !validResponseID(response.ID) {
		return fmt.Errorf("openai responses single response is invalid")
	}
	state := responsesStreamState{created: true, outputs: make(map[int]*responseOutputState), contents: make(map[string]*responseContentState)}
	for index, item := range response.Output {
		if err := state.addOutput(responsesSSEEvent{OutputIndex: &index, Item: &item}); err != nil {
			return err
		}
		if item.Type == "message" {
			for contentIndex, content := range item.Content {
				if err := state.addContent(responsesSSEEvent{OutputIndex: &index, ContentIndex: &contentIndex, ItemID: item.ID, Part: &content}); err != nil {
					return err
				}
			}
		}
	}
	if err := state.reconcileFinal(response, emit); err != nil {
		return err
	}
	if response.Usage != nil {
		if err := emit(modelexecution.Event{Kind: modelexecution.EventUsage, Usage: &modelexecution.Usage{InputTokens: response.Usage.InputTokens, OutputTokens: response.Usage.OutputTokens}}); err != nil {
			return err
		}
	}
	return emit(modelexecution.Event{Kind: modelexecution.EventFinish, Finish: state.finishReason()})
}

func validResponseID(value string) bool {
	if value == "" || value != strings.TrimSpace(value) || len(value) > 256 || !utf8.ValidString(value) {
		return false
	}
	for _, runeValue := range value {
		if unicode.IsControl(runeValue) || unicode.Is(unicode.Cf, runeValue) || unicode.Is(unicode.Cs, runeValue) || unicode.Is(unicode.Co, runeValue) {
			return false
		}
	}
	return true
}
