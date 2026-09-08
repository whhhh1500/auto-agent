package corebridge

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/cc-auto-agent/harness-core/pkg/adapter/modelexecution/anthropic"
	"github.com/cc-auto-agent/harness-core/pkg/adapter/modelexecution/openai"
	"github.com/cc-auto-agent/harness-core/pkg/app/modelcontrol"
	"github.com/cc-auto-agent/harness-core/pkg/app/modelexecution"
	"github.com/cc-auto-agent/harness-core/pkg/core"
)

func TestNormalizedProtocolContract(t *testing.T) {
	adapters := []struct {
		name     string
		protocol modelexecution.Protocol
		ref      modelcontrol.Ref
		path     string
		text     string
		tool     string
		textSSE  string
		toolSSE  string
	}{
		{
			name:     "openai-chat",
			protocol: openai.ChatCompletionsProtocol{MaxTokens: 16},
			ref:      modelcontrol.Ref{ID: "openai-chat", Version: "1"},
			path:     "/chat/completions",
			text:     `{"choices":[{"message":{"content":"hello"},"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":3}}`,
			tool:     `{"choices":[{"message":{"tool_calls":[{"id":"call-1","function":{"name":"weather","arguments":"{\"city\":\"Paris\"}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":2,"completion_tokens":3}}`,
			textSSE: openAIChatContractSSE(
				`{"choices":[{"delta":{"content":"hel"}}]}`,
				`{"choices":[{"delta":{"content":"lo"},"finish_reason":"stop"}]}`,
				`{"choices":[],"usage":{"prompt_tokens":2,"completion_tokens":3}}`,
			),
			toolSSE: openAIChatContractSSE(
				`{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call-1","function":{"name":"weather","arguments":"{\"city\":"}}]}}]}`,
				`{"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"Paris\"}"}}]},"finish_reason":"tool_calls"}]}`,
				`{"choices":[],"usage":{"prompt_tokens":2,"completion_tokens":3}}`,
			),
		},
		{
			name:     "openai-responses",
			protocol: openai.ResponsesProtocol{MaxOutputTokens: 16},
			ref:      modelcontrol.Ref{ID: "openai-responses", Version: "1"},
			path:     "/responses",
			text:     `{"id":"res-1","status":"completed","output":[{"id":"msg-1","type":"message","content":[{"type":"output_text","text":"hello"}]}],"usage":{"input_tokens":2,"output_tokens":3}}`,
			tool:     `{"id":"res-1","status":"completed","output":[{"id":"item-1","type":"function_call","call_id":"call-1","name":"weather","arguments":"{\"city\":\"Paris\"}"}],"usage":{"input_tokens":2,"output_tokens":3}}`,
			textSSE: normalizedContractSSE(
				`{"type":"response.created","sequence_number":1,"response":{"id":"res-1","status":"in_progress"}}`,
				`{"type":"response.output_item.added","sequence_number":2,"output_index":0,"item":{"id":"msg-1","type":"message"}}`,
				`{"type":"response.content_part.added","sequence_number":3,"output_index":0,"content_index":0,"item_id":"msg-1","part":{"type":"output_text"}}`,
				`{"type":"response.output_text.delta","sequence_number":4,"output_index":0,"content_index":0,"item_id":"msg-1","delta":"hel"}`,
				`{"type":"response.output_text.done","sequence_number":5,"output_index":0,"content_index":0,"item_id":"msg-1","text":"hello"}`,
				`{"type":"response.output_item.done","sequence_number":6,"output_index":0,"item":{"id":"msg-1","type":"message","content":[{"type":"output_text","text":"hello"}]}}`,
				`{"type":"response.completed","sequence_number":7,"response":{"id":"res-1","status":"completed","output":[{"id":"msg-1","type":"message","content":[{"type":"output_text","text":"hello"}]}],"usage":{"input_tokens":2,"output_tokens":3}}}`,
			),
			toolSSE: normalizedContractSSE(
				`{"type":"response.created","sequence_number":1,"response":{"id":"res-1","status":"in_progress"}}`,
				`{"type":"response.output_item.added","sequence_number":2,"output_index":0,"item":{"id":"item-1","type":"function_call","call_id":"call-1","name":"weather"}}`,
				`{"type":"response.function_call_arguments.delta","sequence_number":3,"output_index":0,"item_id":"item-1","delta":"{\"city\":"}`,
				`{"type":"response.function_call_arguments.done","sequence_number":4,"output_index":0,"item_id":"item-1","name":"weather","arguments":"{\"city\":\"Paris\"}"}`,
				`{"type":"response.output_item.done","sequence_number":5,"output_index":0,"item":{"id":"item-1","type":"function_call","call_id":"call-1","name":"weather","arguments":"{\"city\":\"Paris\"}"}}`,
				`{"type":"response.completed","sequence_number":6,"response":{"id":"res-1","status":"completed","output":[{"id":"item-1","type":"function_call","call_id":"call-1","name":"weather","arguments":"{\"city\":\"Paris\"}"}],"usage":{"input_tokens":2,"output_tokens":3}}}`,
			),
		},
		{
			name:     "anthropic-messages",
			protocol: anthropic.MessagesProtocol{MaxTokens: 16},
			ref:      modelcontrol.Ref{ID: "anthropic-messages", Version: "1"},
			path:     "/v1/messages",
			text:     `{"content":[{"type":"text","text":"hello"}],"usage":{"input_tokens":2,"output_tokens":3},"stop_reason":"end_turn"}`,
			tool:     `{"content":[{"type":"tool_use","id":"call-1","name":"weather","input":{"city":"Paris"}}],"usage":{"input_tokens":2,"output_tokens":3},"stop_reason":"tool_use"}`,
			textSSE: normalizedContractSSE(
				`{"type":"message_start","message":{"usage":{"input_tokens":2}}}`,
				`{"type":"content_block_start","index":0,"content_block":{"type":"text"}}`,
				`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hel"}}`,
				`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"lo"}}`,
				`{"type":"content_block_stop","index":0}`,
				`{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":3}}`,
				`{"type":"message_stop"}`,
			),
			toolSSE: normalizedContractSSE(
				`{"type":"message_start","message":{"usage":{"input_tokens":2}}}`,
				`{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"call-1","name":"weather","input":{}}}`,
				`{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"city\":"}}`,
				`{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"\"Paris\"}"}}`,
				`{"type":"content_block_stop","index":0}`,
				`{"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":3}}`,
				`{"type":"message_stop"}`,
			),
		},
	}

	for _, adapter := range adapters {
		for _, trajectory := range []struct {
			name       string
			response   string
			finishKind string
			wantTool   bool
		}{
			{name: "text-stop/single", response: adapter.text, finishKind: core.FinishStop},
			{name: "tool-calls/single", response: adapter.tool, finishKind: core.FinishToolCalls, wantTool: true},
			{name: "text-stop/sse", response: adapter.textSSE, finishKind: core.FinishStop},
			{name: "tool-calls/sse", response: adapter.toolSSE, finishKind: core.FinishToolCalls, wantTool: true},
		} {
			t.Run(adapter.name+"/"+trajectory.name, func(t *testing.T) {
				plan := normalizedContractPlan(adapter.ref)
				provider := &normalizedContractProvider{response: trajectory.response, wantPath: adapter.path}
				registry, err := modelexecution.NewRegistry(
					plan.SnapshotRevision,
					[]modelcontrol.ProviderPlan{plan},
					[]modelexecution.ProviderRegistration{{Binding: plan.Provider, Factory: func() (modelexecution.Provider, error) { return provider, nil }}},
					[]modelexecution.ProtocolRegistration{{Binding: plan.Protocol, Factory: func() (modelexecution.Protocol, error) { return adapter.protocol, nil }}},
				)
				if err != nil {
					t.Fatal(err)
				}
				bridge := Adapter{Registry: registry, Plan: plan}
				var chunks []core.StreamChunk
				options := core.GenerateOptions{Messages: []core.ChatMessage{{Role: core.RoleUser, Content: "hi"}}}
				if trajectory.wantTool {
					options.Tools = []core.ToolSchema{{Name: "weather", Parameters: map[string]any{"type": "object"}}}
				}
				if err := bridge.Stream(context.Background(), options, func(chunk core.StreamChunk) { chunks = append(chunks, chunk) }); err != nil {
					t.Fatal(err)
				}
				if provider.calls != 1 {
					t.Fatalf("provider calls=%d, want 1", provider.calls)
				}
				assertNormalizedContractChunks(t, chunks, trajectory.finishKind, trajectory.wantTool)
			})
		}
	}
}

func normalizedContractPlan(protocol modelcontrol.Ref) modelcontrol.ProviderPlan {
	provider := modelcontrol.Ref{ID: "scripted-provider", Version: "1"}
	return modelcontrol.ProviderPlan{
		Catalog: modelcontrol.CatalogModel{WireModel: "scripted", Provider: provider, Protocol: protocol},
		Provider: modelcontrol.ImplementationBinding{
			Ref:                    provider,
			ImplementationRevision: "provider-test",
		},
		Endpoint: modelcontrol.EndpointRef{ID: "scripted-endpoint", Revision: "1"},
		Protocol: modelcontrol.ImplementationBinding{
			Ref:                    protocol,
			ImplementationRevision: "protocol-test",
		},
		SnapshotRevision: "contract-test",
	}
}

func assertNormalizedContractChunks(t *testing.T, chunks []core.StreamChunk, finishKind string, wantTool bool) {
	t.Helper()
	if len(chunks) < 2 {
		t.Fatalf("chunks=%#v, want assistant and finish", chunks)
	}
	finish := chunks[len(chunks)-1]
	if finish.Kind != core.StreamKindFinish || finish.FinishKind != finishKind || finish.Usage == nil || finish.Usage.InputTokens != 2 || finish.Usage.OutputTokens != 3 {
		t.Fatalf("chunks=%#v", chunks)
	}
	for _, chunk := range chunks[:len(chunks)-1] {
		if chunk.Kind != core.StreamKindAssistant {
			t.Fatalf("non-assistant before finish: %#v", chunks)
		}
	}
	if !wantTool {
		var text strings.Builder
		for _, assistant := range chunks[:len(chunks)-1] {
			if assistant.ToolCall != nil || len(assistant.ToolCalls) != 0 {
				t.Fatalf("text chunks carry a tool: %#v", chunks)
			}
			text.WriteString(assistant.Text)
		}
		if text.String() != "hello" {
			t.Fatalf("text chunks=%#v", chunks)
		}
		return
	}
	if len(chunks) != 2 {
		t.Fatalf("tool chunks=%#v, want exactly one canonical call and finish", chunks)
	}
	assistant := chunks[0]
	wantCall := core.ToolCall{ID: "call-1", Name: "weather", Args: map[string]any{"city": "Paris"}}
	if assistant.Text != "" || assistant.ToolCall == nil || !reflect.DeepEqual(*assistant.ToolCall, wantCall) || !reflect.DeepEqual(assistant.ToolCalls, []core.ToolCall{wantCall}) {
		t.Fatalf("tool chunks=%#v", chunks)
	}
}

func openAIChatContractSSE(events ...string) string {
	return normalizedContractSSE(events...) + "data: [DONE]\n\n"
}

func normalizedContractSSE(events ...string) string {
	var stream strings.Builder
	for _, event := range events {
		stream.WriteString("data: ")
		stream.WriteString(event)
		stream.WriteString("\n\n")
	}
	return stream.String()
}

type normalizedContractProvider struct {
	response string
	wantPath string
	calls    int
}

func (p *normalizedContractProvider) Send(_ context.Context, _ modelcontrol.ProviderPlan, request modelexecution.OutboundRequest) (modelexecution.InboundResponse, error) {
	p.calls++
	if request.Method != "POST" || request.Path != p.wantPath {
		return modelexecution.InboundResponse{}, fmt.Errorf("outbound request method=%q path=%q, want POST %q", request.Method, request.Path, p.wantPath)
	}
	return modelexecution.InboundResponse{Status: 200, Body: io.NopCloser(strings.NewReader(p.response))}, nil
}
