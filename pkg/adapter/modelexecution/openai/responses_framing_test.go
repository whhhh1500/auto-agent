package openai

import (
	"strings"
	"testing"

	"github.com/whhhh1500/auto-agent/pkg/app/modelexecution"
)

func TestResponsesRecognizesStandardSSEFields(t *testing.T) {
	for _, prefix := range []string{": initial comment", "event: response.created", "id: response-1", "retry: 1000"} {
		t.Run(prefix, func(t *testing.T) {
			raw := strings.Join([]string{
				prefix,
				`data: {"type":"response.created","sequence_number":1,"response":{"id":"res-1","status":"in_progress"}}`,
				"",
				"event: response.completed",
				`data: {"type":"response.completed","sequence_number":2,"response":{"id":"res-1","status":"completed","output":[]}}`,
				"",
				"data: [DONE]",
				"",
			}, "\n")

			var events []modelexecution.Event
			if err := parseResponsesResponse(strings.NewReader(raw), func(event modelexecution.Event) error {
				events = append(events, event)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if len(events) != 1 || events[0].Finish != modelexecution.FinishStop {
				t.Fatalf("events=%#v", events)
			}
		})
	}
}

func TestResponsesDoesNotTreatUnknownFieldAsSSE(t *testing.T) {
	raw := strings.Join([]string{
		"unknown: field",
		`data: {"type":"response.created","sequence_number":1,"response":{"id":"res-1","status":"in_progress"}}`,
		`data: {"type":"response.completed","sequence_number":2,"response":{"id":"res-1","status":"completed","output":[]}}`,
	}, "\n")
	if err := parseResponsesResponse(strings.NewReader(raw), func(modelexecution.Event) error { return nil }); err == nil {
		t.Fatal("unknown field framing was accepted as SSE")
	}
}

func TestResponsesTextAndContentPartCompletionsConfirmSameContent(t *testing.T) {
	stream := sse(
		`{"type":"response.created","sequence_number":1,"response":{"id":"res-1","status":"in_progress"}}`,
		`{"type":"response.in_progress","sequence_number":2,"response":{"id":"res-1","status":"in_progress"}}`,
		`{"type":"response.output_item.added","sequence_number":3,"output_index":0,"item":{"id":"msg-1","type":"message"}}`,
		`{"type":"response.content_part.added","sequence_number":4,"output_index":0,"content_index":0,"item_id":"msg-1","part":{"type":"output_text"}}`,
		`{"type":"response.output_text.delta","sequence_number":5,"output_index":0,"content_index":0,"item_id":"msg-1","delta":"hel"}`,
		`{"type":"response.output_text.done","sequence_number":6,"output_index":0,"content_index":0,"item_id":"msg-1","text":"hello"}`,
		`{"type":"response.content_part.done","sequence_number":7,"output_index":0,"content_index":0,"item_id":"msg-1","part":{"type":"output_text","text":"hello"}}`,
		`{"type":"response.output_item.done","sequence_number":8,"output_index":0,"item":{"id":"msg-1","type":"message","content":[{"type":"output_text","text":"hello"}]}}`,
		`{"type":"response.completed","sequence_number":9,"response":{"id":"res-1","status":"completed","output":[{"id":"msg-1","type":"message","content":[{"type":"output_text","text":"hello"}]}]}}`,
	)
	events := collectResponses(t, stream)
	if len(events) != 3 || events[0].Text != "hel" || events[1].Text != "lo" || events[2].Finish != modelexecution.FinishStop {
		t.Fatalf("events=%#v", events)
	}
}

func TestResponsesRejectsRepeatedOrConflictingTextCompletion(t *testing.T) {
	base := []string{
		`{"type":"response.created","sequence_number":1,"response":{"id":"res-1","status":"in_progress"}}`,
		`{"type":"response.output_item.added","sequence_number":2,"output_index":0,"item":{"id":"msg-1","type":"message"}}`,
		`{"type":"response.content_part.added","sequence_number":3,"output_index":0,"content_index":0,"item_id":"msg-1","part":{"type":"output_text"}}`,
		`{"type":"response.output_text.done","sequence_number":4,"output_index":0,"content_index":0,"item_id":"msg-1","text":"hello"}`,
	}
	partDone := `{"type":"response.content_part.done","sequence_number":5,"output_index":0,"content_index":0,"item_id":"msg-1","part":{"type":"output_text","text":"hello"}}`
	for name, events := range map[string][]string{
		"duplicate text done":                append(append([]string{}, base...), `{"type":"response.output_text.done","sequence_number":5,"output_index":0,"content_index":0,"item_id":"msg-1","text":"hello"}`),
		"duplicate part done":                append(append([]string{}, base...), partDone, `{"type":"response.content_part.done","sequence_number":6,"output_index":0,"content_index":0,"item_id":"msg-1","part":{"type":"output_text","text":"hello"}}`),
		"part conflicts with text done":      append(append([]string{}, base...), `{"type":"response.content_part.done","sequence_number":5,"output_index":0,"content_index":0,"item_id":"msg-1","part":{"type":"output_text","text":"other"}}`),
		"output item extends confirmed text": append(append([]string{}, base...), `{"type":"response.output_item.done","sequence_number":5,"output_index":0,"item":{"id":"msg-1","type":"message","content":[{"type":"output_text","text":"hello!"}]}}`),
	} {
		t.Run(name, func(t *testing.T) {
			if err := parseResponsesResponse(strings.NewReader(sse(events...)), func(modelexecution.Event) error { return nil }); err == nil {
				t.Fatal("completion inconsistency was accepted")
			}
		})
	}
}

func TestResponsesFinalCanCompleteUnfinishedText(t *testing.T) {
	stream := sse(
		`{"type":"response.created","sequence_number":1,"response":{"id":"res-1","status":"in_progress"}}`,
		`{"type":"response.output_item.added","sequence_number":2,"output_index":0,"item":{"id":"msg-1","type":"message"}}`,
		`{"type":"response.content_part.added","sequence_number":3,"output_index":0,"content_index":0,"item_id":"msg-1","part":{"type":"output_text"}}`,
		`{"type":"response.output_text.delta","sequence_number":4,"output_index":0,"content_index":0,"item_id":"msg-1","delta":"hel"}`,
		`{"type":"response.completed","sequence_number":5,"response":{"id":"res-1","status":"completed","output":[{"id":"msg-1","type":"message","content":[{"type":"output_text","text":"hello"}]}]}}`,
	)
	events := collectResponses(t, stream)
	if len(events) != 3 || events[0].Text != "hel" || events[1].Text != "lo" || events[2].Finish != modelexecution.FinishStop {
		t.Fatalf("events=%#v", events)
	}
}
