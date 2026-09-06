package openai

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	. "github.com/cc-auto-agent/harness-core/pkg/core"
)

func TestParseSSEAccumulatesParallelToolCallsByIndex(t *testing.T) {
	stream := strings.Join([]string{
		`data: {"choices":[{"delta":{"content":"Checking"}}]}`, ``,
		`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call-a","function":{"name":"market.quote","arguments":"{\"symbol\":\"BTC\"}"}}]}}]}`,
		`data: {"choices":[{"delta":{"tool_calls":[{"index":1,"id":"call-b","function":{"name":"market.candles","arguments":"{\"symbol\":"}}]}}]}`,
		`data: {"choices":[{"delta":{"tool_calls":[{"index":1,"function":{"arguments":"\"ETH\"}"}}]}}]}`, ``,
		`data: {"choices":[],"usage":{"prompt_tokens":10,"completion_tokens":5}}`, ``, `data: [DONE]`, ``,
	}, "\n")
	text, calls, usage, err := parseSSE([]byte(stream), func(StreamChunk) {})
	if err != nil {
		t.Fatal(err)
	}
	if text != "Checking" || len(calls) != 2 || calls[1].Args["symbol"] != "ETH" {
		t.Fatalf("unexpected stream parse: %q %#v", text, calls)
	}
	if usage == nil || usage.InputTokens != 10 || usage.OutputTokens != 5 {
		t.Fatalf("usage not parsed: %#v", usage)
	}
}

func TestParseOpenAIResponseDoesNotDuplicateStreamedText(t *testing.T) {
	stream := strings.Join([]string{
		`data: {"choices":[{"delta":{"content":"hel"}}]}`,
		`data: {"choices":[{"delta":{"content":"lo"}}]}`,
		`data: [DONE]`,
	}, "\n")
	var chunks []StreamChunk
	if err := parseOpenAIResponse(bytes.NewBufferString(stream), func(chunk StreamChunk) { chunks = append(chunks, chunk) }); err != nil {
		t.Fatal(err)
	}
	var text strings.Builder
	for _, chunk := range chunks {
		if chunk.Kind == "assistant" {
			text.WriteString(chunk.Text)
		}
	}
	if text.String() != "hello" {
		t.Fatalf("streamed text was duplicated or lost: %q chunks=%#v", text.String(), chunks)
	}
}

func TestParseOpenAIResponseReportsUsageOnce(t *testing.T) {
	stream := strings.Join([]string{
		`data: {"choices":[{"delta":{"content":"ok"}}]}`,
		`data: {"choices":[],"usage":{"prompt_tokens":7,"completion_tokens":3}}`,
		`data: [DONE]`,
	}, "\n")
	inputTokens, outputTokens := int64(0), int64(0)
	if err := parseOpenAIResponse(bytes.NewBufferString(stream), func(chunk StreamChunk) {
		if chunk.Usage != nil {
			inputTokens += chunk.Usage.InputTokens
			outputTokens += chunk.Usage.OutputTokens
		}
	}); err != nil {
		t.Fatal(err)
	}
	if inputTokens != 7 || outputTokens != 3 {
		t.Fatalf("usage was duplicated or lost: in=%d out=%d", inputTokens, outputTokens)
	}
}

func TestRetryAdapterRejectsInvalidConfiguration(t *testing.T) {
	if err := (&RetryLlmAdapter{}).Stream(context.Background(), GenerateOptions{}, func(StreamChunk) {}); err == nil {
		t.Fatal("nil next adapter must be rejected")
	}
	adapter := &RetryLlmAdapter{Next: MockLlmAdapter{}, MaxRetries: -1}
	if err := adapter.Stream(context.Background(), GenerateOptions{}, func(StreamChunk) {}); err == nil {
		t.Fatal("negative max retries must be rejected")
	}
	adapter = &RetryLlmAdapter{Next: MockLlmAdapter{}, MaxRetries: HardMaxRetries + 1}
	if err := adapter.Stream(context.Background(), GenerateOptions{}, func(StreamChunk) {}); err == nil {
		t.Fatal("excessive max retries must be rejected")
	}
}

func TestOpenAIAdapterRejectsInvalidConfiguration(t *testing.T) {
	for _, config := range []OpenAIAdapterConfig{
		{BaseURL: "https://api.example.com", Model: "m"},
		{BaseURL: "not-a-url", APIKey: "key", Model: "m"},
		{BaseURL: "https://user:pass@example.com", APIKey: "key", Model: "m"},
		{BaseURL: "https://api.example.com", APIKey: "key"},
		{BaseURL: "https://api.example.com", APIKey: "key", Model: "m", MaxTokens: -1},
		{BaseURL: "https://api.example.com", APIKey: "key", Model: "m", AllowedModels: []string{"other"}},
	} {
		adapter := NewOpenAICompatibleAdapter(config)
		if err := adapter.Stream(context.Background(), GenerateOptions{}, func(StreamChunk) {}); err == nil {
			t.Fatalf("invalid adapter configuration was accepted: %#v", config)
		}
	}
}

func TestParseSingleWithMultipleToolCallsAndUsage(t *testing.T) {
	body := []byte(`{"choices":[{"message":{"content":"hi","tool_calls":[{"id":"call-1","function":{"name":"a","arguments":"{\"x\":1}"}},{"id":"call-2","function":{"name":"b","arguments":"{}"}}]}}],"usage":{"prompt_tokens":7,"completion_tokens":3}}`)
	text, calls, usage, err := parseSingle(body)
	if err != nil || text != "hi" || len(calls) != 2 || usage == nil || usage.OutputTokens != 3 {
		t.Fatalf("unexpected parse: %q %#v %#v %v", text, calls, usage, err)
	}
}

func TestOpenAIAdapterRejectsControlCharacters(t *testing.T) {
	adapter := NewOpenAICompatibleAdapter(OpenAIAdapterConfig{
		BaseURL: "https://api.example.com", APIKey: "key\r\nX-Injected: 1", Model: "m",
	})
	if err := adapter.Stream(context.Background(), GenerateOptions{}, func(StreamChunk) {}); err == nil {
		t.Fatal("API key with a newline was accepted")
	}
	adapter = NewOpenAICompatibleAdapter(OpenAIAdapterConfig{
		BaseURL: "https://api.example.com", APIKey: "key", Model: "m\x00odel",
	})
	if err := adapter.Stream(context.Background(), GenerateOptions{}, func(StreamChunk) {}); err == nil {
		t.Fatal("model with NUL was accepted")
	}
}

func TestOpenAICompatibleFacadeUsesM2BridgeWithSharedTransport(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests++
		if request.URL.Path != "/chat/completions" || request.Header.Get("Authorization") != "Bearer test-secret" {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		writer.Header().Set("Content-Type", "text/event-stream")
		_, _ = writer.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":2}}\n\ndata: [DONE]\n\n"))
	}))
	defer server.Close()
	adapter := NewOpenAICompatibleAdapter(OpenAIAdapterConfig{BaseURL: server.URL, APIKey: "test-secret", Model: "wire"})
	var chunks []StreamChunk
	if err := adapter.Stream(context.Background(), GenerateOptions{Messages: []ChatMessage{{Role: RoleUser, Content: "hi"}}}, func(chunk StreamChunk) { chunks = append(chunks, chunk) }); err != nil {
		t.Fatal(err)
	}
	if requests != 1 || len(chunks) != 2 || chunks[0].Text != "ok" || chunks[1].Usage == nil || chunks[1].Usage.OutputTokens != 2 {
		t.Fatalf("requests=%d chunks=%#v", requests, chunks)
	}
}

func TestRetryFacadeMapsM2HTTPStatusForExistingRetryPolicy(t *testing.T) {
	for _, test := range []struct{ status, want int }{{http.StatusTooManyRequests, 2}, {http.StatusInternalServerError, 2}, {http.StatusBadRequest, 1}} {
		t.Run(http.StatusText(test.status), func(t *testing.T) {
			attempts := 0
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				attempts++
				writer.WriteHeader(test.status)
				_, _ = writer.Write([]byte("test-secret"))
			}))
			defer server.Close()
			base := NewOpenAICompatibleAdapter(OpenAIAdapterConfig{BaseURL: server.URL, APIKey: "test-secret", Model: "wire"})
			retry := &RetryLlmAdapter{Next: base, MaxRetries: 1, Backoff: func(int) time.Duration { return 0 }}
			err := retry.Stream(context.Background(), GenerateOptions{Messages: []ChatMessage{{Role: RoleUser, Content: "hi"}}}, func(StreamChunk) {})
			if err == nil || attempts != test.want || strings.Contains(err.Error(), "test-secret") {
				t.Fatalf("status=%d attempts=%d err=%v", test.status, attempts, err)
			}
		})
	}
}

func TestParseSSERejectsOutOfRangeToolCallIndex(t *testing.T) {
	stream := `data: {"choices":[{"delta":{"tool_calls":[{"index":1024,"id":"call-a","function":{"name":"market.quote","arguments":"{}"}}]}}]}` + "\n"
	if _, _, _, err := parseSSE([]byte(stream), func(StreamChunk) {}); err == nil {
		t.Fatal("out-of-range tool call index was accepted")
	}
}

func TestParseSSERejectsOversizedToolCallArguments(t *testing.T) {
	chunk := strings.Repeat("a", (maxOpenAIToolArgsBytes/2)+1)
	stream := strings.Join([]string{
		`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call-a","function":{"name":"market.quote","arguments":"` + chunk + `"}}]}}]}`,
		`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"` + chunk + `"}}]}}]}`,
	}, "\n")
	if _, _, _, err := parseSSE([]byte(stream), func(StreamChunk) {}); err == nil {
		t.Fatal("oversized tool call arguments were accepted")
	}
}

func TestParseSingleRejectsInvalidToolCallID(t *testing.T) {
	body := []byte(`{"choices":[{"message":{"tool_calls":[{"id":"call\n1","function":{"name":"a","arguments":"{}"}}]}}]}`)
	if _, _, _, err := parseSingle(body); err == nil {
		t.Fatal("tool call id with a newline was accepted")
	}
}
