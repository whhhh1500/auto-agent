package openai

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/whhhh1500/auto-agent/pkg/app/modelcontrol"
	"github.com/whhhh1500/auto-agent/pkg/app/modelexecution"
)

func TestToolNameAliasesKeepLegalNamesAndRejectCollisions(t *testing.T) {
	internal := "program.catalog"
	alias := encodedToolName(internal)
	if len(alias) > 64 || !isOpenAIWireToolName(alias) {
		t.Fatalf("alias %q is not a legal OpenAI tool name", alias)
	}
	request := modelexecution.Request{Tools: []modelexecution.Tool{{Name: "weather_v1"}, {Name: internal}}}
	mapping, err := newToolNameAliases(request)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := mapping.wire("weather_v1"); err != nil || got != "weather_v1" {
		t.Fatalf("legal name=%q err=%v", got, err)
	}
	if got, err := mapping.wire(internal); err != nil || got != alias {
		t.Fatalf("alias=%q err=%v want=%q", got, err, alias)
	}
	if _, err := newToolNameAliases(modelexecution.Request{Tools: []modelexecution.Tool{{Name: internal}, {Name: alias}}}); err == nil {
		t.Fatal("generated alias collision was accepted")
	}
}

func TestToolNameAliasesMapHistoryOutboundOnly(t *testing.T) {
	current := "program.catalog"
	historical := "workflow.run"
	request := aliasRequest(current, historical)
	mapping, err := newToolNameAliases(request)
	if err != nil {
		t.Fatal(err)
	}
	historyWire, err := mapping.wire(historical)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mapping.internal(historyWire); err == nil {
		t.Fatal("history-only function name was accepted as a current tool")
	}

	responsesBody, err := marshalResponsesRequest(request, 0)
	if err != nil {
		t.Fatal(err)
	}
	assertRequestMapsToolAndHistory(t, responsesBody, false, current, historical, mapping)
	chatBody, err := marshalChatRequest(request, 0)
	if err != nil {
		t.Fatal(err)
	}
	assertRequestMapsToolAndHistory(t, chatBody, true, current, historical, mapping)
}

func TestResponsesAndChatRestoreOnlyAdvertisedToolNames(t *testing.T) {
	internal := "program.catalog"
	request := aliasRequest(internal, "workflow.run")
	mapping, err := newToolNameAliases(request)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := mapping.wire(internal)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name     string
		protocol modelexecution.Protocol
		response string
	}{
		{
			name:     "responses",
			protocol: ResponsesProtocol{},
			response: `{"id":"res-1","status":"completed","output":[{"id":"item-1","type":"function_call","call_id":"call-1","name":"` + wire + `","arguments":"{}"}]}`,
		},
		{
			name:     "chat",
			protocol: ChatCompletionsProtocol{},
			response: `{"choices":[{"message":{"tool_calls":[{"id":"call-1","type":"function","function":{"name":"` + wire + `","arguments":"{}"}}]},"finish_reason":"tool_calls"}]}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			provider := recordingProvider{body: io.NopCloser(strings.NewReader(tc.response))}
			var events []modelexecution.Event
			if err := tc.protocol.Execute(context.Background(), request, &provider, func(event modelexecution.Event) error {
				events = append(events, event)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if len(events) < 1 || events[0].ToolCall == nil || events[0].ToolCall.Name != internal {
				t.Fatalf("events=%#v", events)
			}
			if !strings.Contains(string(provider.request.Body), `"name":"`+wire+`"`) {
				t.Fatalf("outbound body did not contain wire alias: %s", provider.request.Body)
			}
		})
	}
	if err := parseResponsesResponseWithAliases(strings.NewReader(`{"id":"res-1","status":"completed","output":[{"id":"item-1","type":"function_call","call_id":"call-1","name":"unknown_wire_name","arguments":"{}"}]}`), mapping, func(modelexecution.Event) error { return nil }); err == nil {
		t.Fatal("unknown response alias was accepted")
	}
}

func TestChatAliasesAllowArgumentsOnlyContinuation(t *testing.T) {
	internal := "program.catalog"
	request := aliasRequest(internal, "workflow.run")
	mapping, err := newToolNameAliases(request)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := mapping.wire(internal)
	if err != nil {
		t.Fatal(err)
	}
	raw := sse(
		`{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call-1","function":{"name":"`+wire+`","arguments":"{"}}]}}]}`,
		`{"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"}"}}]},"finish_reason":"tool_calls"}]}`,
	)
	var events []modelexecution.Event
	if err := parseChatResponseWithAliases(strings.NewReader(raw), mapping, func(event modelexecution.Event) error {
		events = append(events, event)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 || events[0].ToolCall == nil || events[0].ToolCall.Name != internal || events[1].ToolCall == nil || events[1].ToolCall.Name != "" || events[2].Finish != modelexecution.FinishToolCalls {
		t.Fatalf("events=%#v", events)
	}
}

func aliasRequest(current, historical string) modelexecution.Request {
	return modelexecution.Request{
		Plan:     modelcontrol.ProviderPlan{Catalog: modelcontrol.CatalogModel{WireModel: "wire"}},
		Messages: []modelexecution.Message{{Role: "assistant", ToolCalls: []modelexecution.ToolCall{{ID: "old-call", Name: historical, Arguments: []byte(`{}`)}}}},
		Tools:    []modelexecution.Tool{{Name: current, Description: "catalog", Parameters: []byte(`{"type":"object"}`)}},
	}
}

func assertRequestMapsToolAndHistory(t *testing.T, raw []byte, chat bool, current, historical string, mapping *toolNameAliases) {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	currentWire, err := mapping.wire(current)
	if err != nil {
		t.Fatal(err)
	}
	historyWire, err := mapping.wire(historical)
	if err != nil {
		t.Fatal(err)
	}
	if chat {
		tools := payload["tools"].([]any)
		function := tools[0].(map[string]any)["function"].(map[string]any)
		if function["name"] != currentWire || !strings.Contains(function["description"].(string), current) {
			t.Fatalf("chat tool=%#v", function)
		}
		messages := payload["messages"].([]any)
		calls := messages[0].(map[string]any)["tool_calls"].([]any)
		if calls[0].(map[string]any)["function"].(map[string]any)["name"] != historyWire {
			t.Fatalf("chat history=%#v", calls)
		}
		return
	}
	tools := payload["tools"].([]any)
	if tools[0].(map[string]any)["name"] != currentWire || !strings.Contains(tools[0].(map[string]any)["description"].(string), current) {
		t.Fatalf("responses tool=%#v", tools)
	}
	input := payload["input"].([]any)
	if input[0].(map[string]any)["name"] != historyWire {
		t.Fatalf("responses history=%#v", input)
	}
}
