package openai

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/cc-auto-agent/harness-core/pkg/core"
)

// These are facade-level tests: OpenAICompatibleAdapter delivers text deltas
// immediately, but keeps tool calls, usage, and finish private until a complete
// response has been parsed. RetryLlmAdapter must use what the consumer has
// actually observed, not merely bytes seen by the protocol parser.
func TestRetryFacadeDoesNotRetryAfterVisibleTextDelta(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\ndata: {not-json}\n\n")
	}))
	defer server.Close()

	retry := retryFacade(server.URL, 2)
	var chunks []StreamChunk
	err := retry.Stream(context.Background(), retryGenerateOptions(), func(chunk StreamChunk) { chunks = append(chunks, chunk) })
	if err == nil {
		t.Fatal("malformed response unexpectedly succeeded")
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("attempts=%d, want one after visible text", got)
	}
	if len(chunks) != 1 || chunks[0].Kind != StreamKindAssistant || chunks[0].Text != "partial" {
		t.Fatalf("visible chunks=%#v, want one partial assistant delta", chunks)
	}
}

// This is deliberately red against the old fallback classifier: bytes from a
// malformed provider response are not a transport failure merely because the
// facade has not yet emitted its terminal tool/usage chunk.
func TestRetryFacadeDoesNotRetryMalformedUnpublishedToolAndUsage(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := attempts.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		if attempt == 1 {
			_, _ = fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-first\",\"function\":{\"name\":\"inventory.lookup\",\"arguments\":\"{}\"}}]}}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":9,\"completion_tokens\":4}}\n\ndata: {not-json}\n\n")
			return
		}
		_, _ = fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-retry\",\"function\":{\"name\":\"inventory.lookup\",\"arguments\":\"{}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":2}}\n\ndata: [DONE]\n\n")
	}))
	defer server.Close()

	retry := retryFacade(server.URL, 1)
	var chunks []StreamChunk
	err := retry.Stream(context.Background(), retryGenerateOptions(), func(chunk StreamChunk) { chunks = append(chunks, chunk) })
	if err == nil {
		t.Fatalf("malformed provider response unexpectedly succeeded after %d attempts", attempts.Load())
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("attempts=%d, want no retry for malformed provider response", got)
	}
	if len(chunks) != 0 {
		t.Fatalf("chunks=%#v, want no terminal tool/usage delivery from malformed response", chunks)
	}
}

// Every case below fails before the facade has delivered a chunk. They must
// still remain protocol failures: retry eligibility is decided by the error's
// typed HTTP-send origin, never by an EOF or JSON error's text.
func TestRetryFacadeDoesNotRetryIncompleteProtocolResponse(t *testing.T) {
	cases := []struct {
		name        string
		contentType string
		body        string
	}{
		{name: "empty body", contentType: "application/json", body: ""},
		{name: "truncated JSON", contentType: "application/json", body: `{"choices":[`},
		{name: "SSE finish and usage without terminal marker", contentType: "text/event-stream", body: "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1}}\n\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var attempts atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				attempts.Add(1)
				w.Header().Set("Content-Type", tc.contentType)
				_, _ = fmt.Fprint(w, tc.body)
			}))
			defer server.Close()

			var chunks []StreamChunk
			err := retryFacade(server.URL, 1).Stream(context.Background(), retryGenerateOptions(), func(chunk StreamChunk) {
				chunks = append(chunks, chunk)
			})
			if err == nil {
				t.Fatalf("incomplete protocol response unexpectedly succeeded after %d attempts", attempts.Load())
			}
			if got := attempts.Load(); got != 1 {
				t.Fatalf("attempts=%d, want one for protocol failure", got)
			}
			if len(chunks) != 0 {
				t.Fatalf("chunks=%#v, want no delivery before protocol failure", chunks)
			}
		})
	}
}

// A Chat Completions stream is complete only when its terminal SSE sentinel is
// received. finish_reason can remain absent in compatible streams because the
// protocol has always inferred stop/tool completion at a valid [DONE] boundary.
func TestRetryFacadeAcceptsSSETerminatorWithoutFinishReason(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"complete\"}}]}\n\ndata: [DONE]\n\n")
	}))
	defer server.Close()

	var chunks []StreamChunk
	if err := retryFacade(server.URL, 1).Stream(context.Background(), retryGenerateOptions(), func(chunk StreamChunk) { chunks = append(chunks, chunk) }); err != nil {
		t.Fatal(err)
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("attempts=%d, want one complete stream", got)
	}
	if len(chunks) != 2 || chunks[0].Text != "complete" || chunks[1].Kind != StreamKindFinish {
		t.Fatalf("chunks=%#v, want text followed by inferred finish", chunks)
	}
}

func TestRetryFacadeArtifactRevisionBindsRetryClassifier(t *testing.T) {
	retry := retryFacade("https://example.invalid", 1)
	if got, want := retry.ArtifactRevision(), "retry/v2/openai-compatible/chat-completions/v2-terminal-sentinel"; got != want {
		t.Fatalf("ArtifactRevision()=%q, want %q", got, want)
	}
}

func TestRetryFacadeRetriesTransportFailureBeforeResponse(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := attempts.Add(1)
		if attempt == 1 {
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				t.Fatal("httptest writer does not support hijack")
			}
			conn, _, err := hijacker.Hijack()
			if err != nil {
				t.Fatal(err)
			}
			_ = conn.Close()
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"recovered\"},\"finish_reason\":\"stop\"}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1}}\n\ndata: [DONE]\n\n")
	}))
	defer server.Close()

	retry := retryFacade(server.URL, 1)
	var chunks []StreamChunk
	if err := retry.Stream(context.Background(), retryGenerateOptions(), func(chunk StreamChunk) { chunks = append(chunks, chunk) }); err != nil {
		t.Fatal(err)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("attempts=%d, want transport retry", got)
	}
	if len(chunks) != 2 || chunks[0].Text != "recovered" || chunks[1].Kind != StreamKindFinish {
		t.Fatalf("chunks=%#v, want recovered terminal response", chunks)
	}
}

func TestRetryFacadeDoesNotRetryCanceledRequest(t *testing.T) {
	entered := make(chan struct{})
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			close(entered)
		}
		select {
		case <-r.Context().Done():
		case <-time.After(time.Second):
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	retry := retryFacade(server.URL, 2)
	done := make(chan error, 1)
	go func() { done <- retry.Stream(ctx, retryGenerateOptions(), func(StreamChunk) {}) }()
	select {
	case <-entered:
		cancel()
	case <-time.After(time.Second):
		t.Fatal("request did not reach test server")
	}
	select {
	case err := <-done:
		if err == nil || !errors.Is(err, context.Canceled) {
			t.Fatalf("error=%v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled request did not return")
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("attempts=%d, want one canceled request", got)
	}
}

func retryFacade(baseURL string, maxRetries int) *RetryLlmAdapter {
	return &RetryLlmAdapter{
		Next:       NewOpenAICompatibleAdapter(OpenAIAdapterConfig{BaseURL: baseURL, APIKey: "test-secret", Model: "retry-fixture"}),
		MaxRetries: maxRetries,
		Backoff:    func(int) time.Duration { return 0 },
	}
}

func retryGenerateOptions() GenerateOptions {
	return GenerateOptions{Messages: []ChatMessage{{Role: RoleUser, Content: "retry fixture"}}}
}
