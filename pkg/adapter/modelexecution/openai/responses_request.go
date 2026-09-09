package openai

import (
	"encoding/json"
	"fmt"

	"github.com/whhhh1500/auto-agent/pkg/app/modelcontrol"
	"github.com/whhhh1500/auto-agent/pkg/app/modelexecution"
)

// NewResponsesProtocolRegistration returns an explicit exact protocol binding.
// Callers must register it with a modelcontrol plan that carries this same
// reference and implementation revision; it performs no global registration.
func NewResponsesProtocolRegistration(binding modelcontrol.ImplementationBinding, maxOutputTokens int) (modelexecution.ProtocolRegistration, error) {
	if binding.Ref.ID == "" || binding.Ref.Version == "" || binding.ImplementationRevision == "" || maxOutputTokens < 0 {
		return modelexecution.ProtocolRegistration{}, fmt.Errorf("openai responses protocol registration is invalid")
	}
	protocol := ResponsesProtocol{MaxOutputTokens: maxOutputTokens}
	return modelexecution.ProtocolRegistration{Binding: binding, Factory: func() (modelexecution.Protocol, error) { return protocol, nil }}, nil
}

func marshalResponsesRequest(request modelexecution.Request, maxOutputTokens int) ([]byte, error) {
	aliases, err := newToolNameAliases(request)
	if err != nil {
		return nil, err
	}
	return marshalResponsesRequestWithAliases(request, maxOutputTokens, aliases)
}

func marshalResponsesRequestWithAliases(request modelexecution.Request, maxOutputTokens int, aliases *toolNameAliases) ([]byte, error) {
	input := make([]any, 0, len(request.Messages))
	for _, message := range request.Messages {
		switch message.Role {
		case "user", "assistant":
			if len(message.ToolCalls) == 0 {
				input = append(input, map[string]any{"role": message.Role, "content": message.Content})
				continue
			}
			if message.Content != "" {
				input = append(input, map[string]any{"role": message.Role, "content": message.Content})
			}
			for _, call := range message.ToolCalls {
				name, err := aliases.wire(call.Name)
				if err != nil {
					return nil, err
				}
				input = append(input, map[string]any{"type": "function_call", "call_id": call.ID, "name": name, "arguments": string(call.Arguments)})
			}
		case "tool":
			input = append(input, map[string]any{"type": "function_call_output", "call_id": message.ToolCallID, "output": message.Content})
		default:
			return nil, fmt.Errorf("openai responses message role is unsupported")
		}
	}
	// We send complete local history and never resume with previous_response_id,
	// so retaining upstream response state is neither needed nor desirable.
	payload := map[string]any{"model": request.Plan.Catalog.WireModel, "stream": true, "store": false, "input": input}
	if request.System != "" {
		payload["instructions"] = request.System
	}
	if maxOutputTokens > 0 {
		payload["max_output_tokens"] = maxOutputTokens
	}
	if len(request.Tools) > 0 {
		payload["parallel_tool_calls"] = true
		tools := make([]map[string]any, 0, len(request.Tools))
		for _, tool := range request.Tools {
			var parameters any
			if len(tool.Parameters) == 0 {
				parameters = map[string]any{"type": "object", "properties": map[string]any{}}
			} else if err := json.Unmarshal(tool.Parameters, &parameters); err != nil {
				return nil, fmt.Errorf("openai responses tool parameters: %w", err)
			}
			name, err := aliases.wire(tool.Name)
			if err != nil {
				return nil, err
			}
			tools = append(tools, map[string]any{"type": "function", "name": name, "description": aliases.description(tool.Name, tool.Description), "parameters": parameters})
		}
		payload["tools"] = tools
	}
	return json.Marshal(payload)
}
