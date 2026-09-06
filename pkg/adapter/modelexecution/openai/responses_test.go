package openai

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/cc-auto-agent/harness-core/pkg/app/modelcontrol"
	"github.com/cc-auto-agent/harness-core/pkg/app/modelexecution"
)

func TestResponsesSSETextSuffixUsageAndFinish(t *testing.T) {
	stream := sse(
		`{"type":"response.created","sequence_number":1,"response":{"id":"res-1","status":"in_progress"}}`,
		`{"type":"response.output_item.added","sequence_number":2,"output_index":0,"item":{"id":"msg-1","type":"message"}}`,
		`{"type":"response.content_part.added","sequence_number":3,"output_index":0,"content_index":0,"item_id":"msg-1","part":{"type":"output_text"}}`,
		`{"type":"response.output_text.delta","sequence_number":4,"output_index":0,"content_index":0,"item_id":"msg-1","delta":"hel"}`,
		`{"type":"response.output_text.done","sequence_number":5,"output_index":0,"content_index":0,"item_id":"msg-1","text":"hello"}`,
		`{"type":"response.output_item.done","sequence_number":6,"output_index":0,"item":{"id":"msg-1","type":"message","content":[{"type":"output_text","text":"hello"}]}}`,
		`{"type":"response.completed","sequence_number":7,"response":{"id":"res-1","status":"completed","output":[{"id":"msg-1","type":"message","content":[{"type":"output_text","text":"hello"}]}],"usage":{"input_tokens":2,"output_tokens":3}}}`,
	)
	events := collectResponses(t, stream)
	if len(events) != 4 || events[0].Text != "hel" || events[1].Text != "lo" || events[2].Kind != modelexecution.EventUsage || events[2].Usage.InputTokens != 2 || events[3].Finish != modelexecution.FinishStop {
		t.Fatalf("events=%#v", events)
	}
}

func TestResponsesAcceptsZeroInitialSequenceAndQueuedLifecycle(t *testing.T) {
	stream := sse(
		`{"type":"response.created","sequence_number":0,"response":{"id":"res-1","status":"queued"}}`,
		`{"type":"response.queued","sequence_number":1,"response":{"id":"res-1","status":"queued"}}`,
		`{"type":"response.completed","sequence_number":2,"response":{"id":"res-1","status":"completed","output":[]}}`,
	)
	events := collectResponses(t, stream)
	if len(events) != 1 || events[0].Finish != modelexecution.FinishStop {
		t.Fatalf("events=%#v", events)
	}
}

func TestResponsesSSEFunctionArgumentsArePrefixSafe(t *testing.T) {
	stream := sse(
		`{"type":"response.created","sequence_number":1,"response":{"id":"res-1","status":"in_progress"}}`,
		`{"type":"response.output_item.added","sequence_number":2,"output_index":0,"item":{"id":"item-1","type":"function_call","call_id":"call-1","name":"weather"}}`,
		`{"type":"response.function_call_arguments.delta","sequence_number":3,"output_index":0,"item_id":"item-1","delta":"{\"city\":"}`,
		`{"type":"response.function_call_arguments.done","sequence_number":4,"output_index":0,"item_id":"item-1","name":"weather","arguments":"{\"city\":\"Paris\"}"}`,
		`{"type":"response.output_item.done","sequence_number":5,"output_index":0,"item":{"id":"item-1","type":"function_call","call_id":"call-1","name":"weather","arguments":"{\"city\":\"Paris\"}"}}`,
		`{"type":"response.completed","sequence_number":6,"response":{"id":"res-1","status":"completed","output":[{"id":"item-1","type":"function_call","call_id":"call-1","name":"weather","arguments":"{\"city\":\"Paris\"}"}]}}`,
	)
	events := collectResponses(t, stream)
	if len(events) != 3 || events[0].ToolCall.ID != "call-1" || string(events[0].ToolCall.ArgumentsFragment) != `{"city":` || string(events[1].ToolCall.ArgumentsFragment) != `"Paris"}` || events[2].Finish != modelexecution.FinishToolCalls {
		t.Fatalf("events=%#v", events)
	}
}

func TestResponsesInterleavedFunctionsKeepFullIdentity(t *testing.T) {
	stream := sse(
		`{"type":"response.created","sequence_number":1,"response":{"id":"res-1","status":"in_progress"}}`,
		`{"type":"response.output_item.added","sequence_number":2,"output_index":0,"item":{"id":"item-a","type":"function_call","call_id":"call-a","name":"alpha"}}`,
		`{"type":"response.output_item.added","sequence_number":3,"output_index":1,"item":{"id":"item-b","type":"function_call","call_id":"call-b","name":"beta"}}`,
		`{"type":"response.function_call_arguments.delta","sequence_number":4,"output_index":1,"item_id":"item-b","delta":"{\"b\":1}"}`,
		`{"type":"response.function_call_arguments.delta","sequence_number":5,"output_index":0,"item_id":"item-a","delta":"{\"a\":1}"}`,
		`{"type":"response.completed","sequence_number":6,"response":{"id":"res-1","status":"completed","output":[{"id":"item-a","type":"function_call","call_id":"call-a","name":"alpha","arguments":"{\"a\":1}"},{"id":"item-b","type":"function_call","call_id":"call-b","name":"beta","arguments":"{\"b\":1}"}]}}`,
	)
	events := collectResponses(t, stream)
	if len(events) != 3 || events[0].ToolCall.Index != 1 || events[0].ToolCall.ID != "call-b" || events[1].ToolCall.Index != 0 || events[1].ToolCall.Name != "alpha" || events[2].Finish != modelexecution.FinishToolCalls {
		t.Fatalf("events=%#v", events)
	}
}

func TestResponsesMapsRefusalToVisibleText(t *testing.T) {
	stream := sse(
		`{"type":"response.created","sequence_number":1,"response":{"id":"res-1","status":"in_progress"}}`,
		`{"type":"response.output_item.added","sequence_number":2,"output_index":0,"item":{"id":"msg-1","type":"message"}}`,
		`{"type":"response.content_part.added","sequence_number":3,"output_index":0,"content_index":0,"item_id":"msg-1","part":{"type":"refusal"}}`,
		`{"type":"response.refusal.delta","sequence_number":4,"output_index":0,"content_index":0,"item_id":"msg-1","delta":"cannot"}`,
		`{"type":"response.refusal.done","sequence_number":5,"output_index":0,"content_index":0,"item_id":"msg-1","refusal":"cannot help"}`,
		`{"type":"response.completed","sequence_number":6,"response":{"id":"res-1","status":"completed","output":[{"id":"msg-1","type":"message","content":[{"type":"refusal","refusal":"cannot help"}]}]}}`,
	)
	events := collectResponses(t, stream)
	if len(events) != 3 || events[0].Text != "cannot" || events[1].Text != " help" || events[2].Finish != modelexecution.FinishStop {
		t.Fatalf("events=%#v", events)
	}
}

func TestResponsesKnownReasoningIsIgnoredButBuiltinsFailClosed(t *testing.T) {
	reasoning := sse(
		`{"type":"response.created","sequence_number":1,"response":{"id":"res-1","status":"in_progress"}}`,
		`{"type":"response.output_item.added","sequence_number":2,"output_index":0,"item":{"id":"rs-1","type":"reasoning"}}`,
		`{"type":"response.reasoning_text.delta","sequence_number":3,"output_index":0,"content_index":0,"item_id":"rs-1","delta":"private"}`,
		`{"type":"response.completed","sequence_number":4,"response":{"id":"res-1","status":"completed","output":[{"id":"rs-1","type":"reasoning"}]}}`,
	)
	events := collectResponses(t, reasoning)
	if len(events) != 1 || events[0].Finish != modelexecution.FinishStop {
		t.Fatalf("reasoning events=%#v", events)
	}
	builtin := sse(`{"type":"response.created","sequence_number":1,"response":{"id":"res-1","status":"in_progress"}}`, `{"type":"response.output_item.added","sequence_number":2,"output_index":0,"item":{"id":"web-1","type":"web_search_call"}}`)
	if err := parseResponsesResponse(strings.NewReader(builtin), func(modelexecution.Event) error { return nil }); err == nil {
		t.Fatal("built-in tool was accepted")
	}
}

func TestResponsesRejectsSequenceIdentityAndFinalPrefixConflicts(t *testing.T) {
	cases := []string{
		sse(`{"type":"response.created","sequence_number":1}`, `{"type":"response.in_progress","sequence_number":1}`),
		sse(`{"type":"response.created","sequence_number":2}`, `{"type":"response.in_progress","sequence_number":1}`),
		sse(`{"type":"response.created"}`),
		sse(`{"type":"response.created","sequence_number":1}`, `{"type":"response.output_item.added","sequence_number":2,"output_index":0,"item":{"id":"item-1","type":"function_call","call_id":"call-1","name":"weather"}}`, `{"type":"response.function_call_arguments.delta","sequence_number":3,"output_index":0,"item_id":"item-1","delta":"{\"a\":"}`, `{"type":"response.function_call_arguments.done","sequence_number":4,"output_index":0,"item_id":"item-1","arguments":"{\"b\":1}"}`),
		sse(`{"type":"response.created","sequence_number":1}`, `{"type":"response.output_item.added","sequence_number":2,"output_index":0,"item":{"id":"item-1","type":"function_call","call_id":"call-1","name":"weather"}}`, `{"type":"response.function_call_arguments.delta","sequence_number":3,"output_index":0,"item_id":"item-1","delta":"{}"}`, `{"type":"response.completed","sequence_number":4,"response":{"status":"completed","output":[{"id":"item-1","type":"function_call","call_id":"call-other","name":"other","arguments":"{}"}]}}`),
		sse(`{"type":"response.created","sequence_number":1}`, `{"type":"response.output_item.added","sequence_number":2,"output_index":0,"item":{"id":"msg-1","type":"message"}}`, `{"type":"response.content_part.added","sequence_number":3,"output_index":0,"content_index":0,"item_id":"msg-1","part":{"type":"output_text"}}`, `{"type":"response.output_text.delta","sequence_number":4,"output_index":0,"content_index":0,"item_id":"msg-1","delta":"ok"}`, `{"type":"response.completed","sequence_number":5,"response":{"status":"completed","output":[{"id":"msg-1","type":"message","content":[{"type":"output_text","text":"no"}]}]}}`),
		sse(`{"type":"response.created","sequence_number":1}`, `{"type":"response.unknown","sequence_number":2}`),
	}
	for index, stream := range cases {
		if err := parseResponsesResponse(strings.NewReader(stream), func(modelexecution.Event) error { return nil }); err == nil {
			t.Fatalf("case %d accepted", index)
		}
	}
}

func TestResponsesLifecycleRequiresOneBoundedResponseID(t *testing.T) {
	streams := []string{
		sse(`{"type":"response.created","sequence_number":1,"response":{"status":"in_progress"}}`),
		sse(`{"type":"response.created","sequence_number":1,"response":{"id":"res-1","status":"in_progress"}}`, `{"type":"response.queued","sequence_number":2,"response":{"id":"res-other","status":"queued"}}`),
		sse(`{"type":"response.created","sequence_number":1,"response":{"id":"res-1","status":"in_progress"}}`, `{"type":"response.completed","sequence_number":2,"response":{"id":"res-other","status":"completed","output":[]}}`),
	}
	for index, stream := range streams {
		if err := parseResponsesResponse(strings.NewReader(stream), func(modelexecution.Event) error { return nil }); err == nil {
			t.Fatalf("stream %d accepted", index)
		}
	}
	if err := parseResponsesResponse(strings.NewReader(`{"status":"completed","output":[]}`), func(modelexecution.Event) error { return nil }); err == nil {
		t.Fatal("single response without ID accepted")
	}
}

func TestResponsesSingleAndRequestMapping(t *testing.T) {
	request := modelexecution.Request{Plan: modelcontrol.ProviderPlan{Catalog: modelcontrol.CatalogModel{WireModel: "wire"}}, System: "system", Messages: []modelexecution.Message{{Role: "user", Content: "hi"}, {Role: "tool", ToolCallID: "call-1", Content: "sunny"}}, Tools: []modelexecution.Tool{{Name: "weather", Description: "weather", Parameters: []byte(`{"type":"object"}`)}}}
	body, err := marshalResponsesRequest(request, 12)
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]any
	if err := json.Unmarshal(body, &wire); err != nil || wire["instructions"] != "system" || wire["max_output_tokens"].(float64) != 12 || wire["stream"] != true || wire["store"] != false {
		t.Fatalf("body=%s err=%v", body, err)
	}
	single := `{"id":"res-1","status":"completed","output":[{"id":"msg-1","type":"message","content":[{"type":"output_text","text":"hello"}]},{"id":"item-1","type":"function_call","call_id":"call-1","name":"weather","arguments":"{}"}],"usage":{"input_tokens":1,"output_tokens":2}}`
	events := collectResponses(t, single)
	if len(events) != 4 || events[0].Text != "hello" || events[1].ToolCall.ID != "call-1" || events[2].Kind != modelexecution.EventUsage || events[3].Finish != modelexecution.FinishToolCalls {
		t.Fatalf("single events=%#v", events)
	}
}

func TestResponsesProtocolUsesSharedProviderAndExactRegistration(t *testing.T) {
	provider := recordingProvider{body: io.NopCloser(strings.NewReader(`{"id":"res-1","status":"completed","output":[{"id":"msg-1","type":"message","content":[{"type":"output_text","text":"ok"}]}]}`))}
	protocol := ResponsesProtocol{}
	request := modelexecution.Request{Plan: modelcontrol.ProviderPlan{Catalog: modelcontrol.CatalogModel{WireModel: "wire"}}, Messages: []modelexecution.Message{{Role: "user", Content: "hi"}}}
	if err := protocol.Execute(context.Background(), request, &provider, func(modelexecution.Event) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if provider.path != "/responses" || !strings.Contains(string(provider.request.Body), `"stream":true`) {
		t.Fatalf("outbound=%#v", provider.request)
	}
	binding := modelcontrol.ImplementationBinding{Ref: modelcontrol.Ref{ID: "responses", Version: "1"}, ImplementationRevision: "impl-1"}
	registration, err := NewResponsesProtocolRegistration(binding, 1)
	if err != nil || registration.Binding != binding || registration.Factory == nil {
		t.Fatalf("registration=%#v err=%v", registration, err)
	}
}

func collectResponses(t *testing.T, raw string) []modelexecution.Event {
	t.Helper()
	var events []modelexecution.Event
	if err := parseResponsesResponse(strings.NewReader(raw), func(event modelexecution.Event) error {
		events = append(events, event)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return events
}
func sse(events ...string) string {
	var out strings.Builder
	for _, event := range events {
		out.WriteString("data: ")
		out.WriteString(event)
		out.WriteString("\n\n")
	}
	out.WriteString("data: [DONE]\n\n")
	return out.String()
}
