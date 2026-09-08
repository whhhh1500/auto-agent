package openai

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/whhhh1500/auto-agent/pkg/app/modelexecution"
)

func TestChatContinuationSurvivesSingleAndSignatureOnlySSE(t *testing.T) {
	for name, response := range map[string]string{
		"json": `{"choices":[{"message":{"tool_calls":[{"id":"call-a","function":{"name":"lookup","arguments":"{}"},"extra_content":{"google":{"thought_signature":"opaque-signed-value"}}}]},"finish_reason":"tool_calls"}]}`,
		"sse":  "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-a\",\"function\":{\"name\":\"lookup\",\"arguments\":\"{}\"}}]}}]}\n\ndata: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"extra_content\":{\"google\":{\"thought_signature\":\"opaque-signed-value\"}}}]},\"finish_reason\":\"tool_calls\"}]}\n\ndata: [DONE]\n",
	} {
		t.Run(name, func(t *testing.T) {
			var continuation string
			validator := modelexecution.NewStreamValidator()
			err := parseChatResponse(strings.NewReader(response), func(event modelexecution.Event) error {
				checked, err := validator.Accept(event)
				if err == nil && checked.ToolCall != nil && checked.ToolCall.Continuation != "" {
					continuation = checked.ToolCall.Continuation
				}
				return err
			})
			if err != nil || continuation == "" {
				t.Fatalf("continuation missing: %v", err)
			}
			request := modelexecution.Request{Messages: []modelexecution.Message{{Role: "assistant", ToolCalls: []modelexecution.ToolCall{{ID: "call-a", Name: "lookup", Arguments: []byte("{}"), Continuation: continuation}}}}}
			wire, err := marshalChatRequest(request, 100)
			if err != nil {
				t.Fatal(err)
			}
			var decoded struct {
				Messages []struct {
					ToolCalls []struct {
						ExtraContent struct {
							Google struct {
								Signature string `json:"thought_signature"`
							} `json:"google"`
						} `json:"extra_content"`
					} `json:"tool_calls"`
				} `json:"messages"`
			}
			if json.Unmarshal(wire, &decoded) != nil || decoded.Messages[0].ToolCalls[0].ExtraContent.Google.Signature != "opaque-signed-value" {
				t.Fatal("signature did not return on the original tool call")
			}
		})
	}
}

func TestChatContinuationRejectsMalformedOversizedAndForeignProtocol(t *testing.T) {
	for _, value := range []string{`{"protocol":"another-protocol","extra_content":{}}`, `{"protocol":"openai.chat/v1","extra_content":[]}`, strings.Repeat("x", modelexecution.DefaultMaxContinuationBytes+1)} {
		if _, err := decodeChatContinuation(value); err == nil {
			t.Fatal("invalid continuation accepted")
		}
	}
	if _, err := encodeChatContinuation(json.RawMessage(`[]`)); err == nil {
		t.Fatal("non-object extra content accepted")
	}
}
