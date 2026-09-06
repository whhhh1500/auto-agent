package anthropic

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/cc-auto-agent/harness-core/pkg/app/modelcontrol"
	"github.com/cc-auto-agent/harness-core/pkg/app/modelexecution"
)

func TestMessagesStreamingTextToolUsageAndFinish(t *testing.T) {
	stream := anthSSE(
		`{"type":"message_start","message":{"usage":{"input_tokens":2}}}`,
		`{"type":"content_block_start","index":0,"content_block":{"type":"thinking"}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"private"}}`,
		`{"type":"content_block_stop","index":0}`,
		`{"type":"content_block_start","index":1,"content_block":{"type":"text"}}`,
		`{"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"hello"}}`,
		`{"type":"content_block_stop","index":1}`,
		`{"type":"content_block_start","index":2,"content_block":{"type":"tool_use","id":"toolu-1","name":"weather","input":{}}}`,
		`{"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":"{\"city\":\"Paris\"}"}}`,
		`{"type":"content_block_stop","index":2}`,
		`{"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":3}}`,
		`{"type":"message_stop"}`,
	)
	events := collect(t, stream)
	if len(events) != 5 || events[0].Text != "hello" || events[1].ToolCall.ID != "toolu-1" || len(events[1].ToolCall.ArgumentsFragment) != 0 || string(events[2].ToolCall.ArgumentsFragment) != `{"city":"Paris"}` || events[3].Kind != modelexecution.EventUsage || events[4].Finish != modelexecution.FinishToolCalls {
		t.Fatalf("events=%#v", events)
	}
}

func TestMessagesRejectsLifecycleAndUnsafeStops(t *testing.T) {
	cases := []string{
		anthSSE(`{"type":"content_block_start","index":0,"content_block":{"type":"text"}}`),
		anthSSE(`{"type":"message_start","message":{"usage":{"input_tokens":1}}}`, `{"type":"content_block_start","index":1,"content_block":{"type":"text"}}`),
		anthSSE(`{"type":"message_start","message":{"usage":{"input_tokens":1}}}`, `{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu-1","name":"weather","input":{}}}`, `{"type":"content_block_stop","index":0}`),
		anthSSE(`{"type":"message_start","message":{"usage":{"input_tokens":1}}}`, `{"type":"message_delta","delta":{"stop_reason":"max_tokens"},"usage":{"output_tokens":1}}`, `{"type":"message_stop"}`),
		anthSSE(`{"type":"message_start","message":{"usage":{"input_tokens":1}}}`, `{"type":"unknown"}`),
	}
	for index, stream := range cases {
		if err := parseMessagesResponse(strings.NewReader(stream), func(modelexecution.Event) error { return nil }); err == nil {
			t.Fatalf("case %d accepted", index)
		}
	}
}

func TestMessagesSingleAndRequestRoleMerge(t *testing.T) {
	request := modelexecution.Request{Plan: modelcontrol.ProviderPlan{Catalog: modelcontrol.CatalogModel{WireModel: "claude"}}, System: "system", Messages: []modelexecution.Message{{Role: "user", Content: "one"}, {Role: "tool", ToolCallID: "toolu-1", Content: "result"}, {Role: "user", Content: "two"}, {Role: "assistant", ToolCalls: []modelexecution.ToolCall{{ID: "toolu-2", Name: "weather", Arguments: []byte(`{"city":"Paris"}`)}}}}, Tools: []modelexecution.Tool{{Name: "weather", Parameters: []byte(`{"type":"object"}`)}}}
	body, err := marshalMessagesRequest(request, 16)
	if err != nil {
		t.Fatal(err)
	}
	var wire struct {
		Messages []wireMessage `json:"messages"`
		Tools    []struct {
			InputSchema map[string]any `json:"input_schema"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(body, &wire); err != nil || len(wire.Messages) != 2 || wire.Messages[0].Role != "user" || len(wire.Messages[0].Content) != 3 || wire.Tools[0].InputSchema["type"] != "object" {
		t.Fatalf("wire=%s err=%v", body, err)
	}
	single := `{"content":[{"type":"thinking"},{"type":"text","text":"answer"},{"type":"tool_use","id":"toolu-1","name":"weather","input":{"city":"Paris"}}],"usage":{"input_tokens":1,"output_tokens":2},"stop_reason":"tool_use"}`
	events := collect(t, single)
	if len(events) != 5 || events[0].Text != "answer" || events[1].ToolCall.ID != "toolu-1" || string(events[2].ToolCall.ArgumentsFragment) != `{"city":"Paris"}` || events[4].Finish != modelexecution.FinishToolCalls {
		t.Fatalf("events=%#v", events)
	}
}

func TestProviderProtectsHeadersBoundsRedirectAndSecrets(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("x-api-key") != "secret" || r.Header.Get("anthropic-version") != apiVersion {
			t.Errorf("headers=%v", r.Header)
		}
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, "/v1/messages", http.StatusFound)
			return
		}
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte("secret=secret"))
	}))
	defer server.Close()
	resolver := &trackingResolver{material: modelexecution.NewCredentialMaterial([]byte("secret"))}
	provider, err := NewHTTPProvider(HTTPProviderConfig{Endpoint: modelcontrol.EndpointRef{ID: "endpoint", Revision: "1"}, BaseURL: server.URL, Credentials: resolver, Client: &http.Client{}})
	if err != nil {
		t.Fatal(err)
	}
	plan := modelcontrol.ProviderPlan{Endpoint: modelcontrol.EndpointRef{ID: "endpoint", Revision: "1"}, Credential: modelcontrol.CredentialRef{ID: "key", Revision: "1"}}
	if _, err := provider.Send(context.Background(), plan, modelexecution.OutboundRequest{Method: "POST", Path: "/v1/messages", Headers: map[string]string{"x-api-key": "bad"}, MaxResponseBytes: 1}); err == nil {
		t.Fatal("header override accepted")
	}
	if _, err := provider.Send(context.Background(), plan, modelexecution.OutboundRequest{Method: "POST", Path: "/v1/messages", Headers: map[string]string{"Anthropic-Version": "bad"}, MaxResponseBytes: 1}); err == nil {
		t.Fatal("version override accepted")
	}
	_, err = provider.Send(context.Background(), plan, modelexecution.OutboundRequest{Method: "POST", Path: "/v1/messages", Body: []byte("{}"), MaxResponseBytes: 1})
	var status *HTTPStatusError
	if !errors.As(err, &status) || strings.Contains(err.Error(), "secret") {
		t.Fatalf("err=%v", err)
	}
	if got := resolver.material.Bytes(); len(got) != 6 || string(got) != "\x00\x00\x00\x00\x00\x00" {
		t.Fatalf("credential not cleared: %q", got)
	}
	if calls != 1 {
		t.Fatalf("calls=%d", calls)
	}
}

func TestProviderHonorsCanceledContext(t *testing.T) {
	resolver := &trackingResolver{material: modelexecution.NewCredentialMaterial([]byte("secret"))}
	provider, err := NewHTTPProvider(HTTPProviderConfig{Endpoint: modelcontrol.EndpointRef{ID: "endpoint", Revision: "1"}, BaseURL: "https://example.test", Credentials: resolver})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = provider.Send(ctx, modelcontrol.ProviderPlan{Endpoint: modelcontrol.EndpointRef{ID: "endpoint", Revision: "1"}, Credential: modelcontrol.CredentialRef{ID: "key", Revision: "1"}}, modelexecution.OutboundRequest{Method: "POST", Path: "/v1/messages", Body: []byte("{}"), MaxResponseBytes: 1})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel=%v", err)
	}
}

func TestProviderClearsPartialCredentialAndRejectsAuthHeadersAndAmbiguousURL(t *testing.T) {
	material := modelexecution.NewCredentialMaterial([]byte("secret"))
	resolver := &trackingResolver{material: material, err: errors.New("resolver secret")}
	provider, err := NewHTTPProvider(HTTPProviderConfig{Endpoint: modelcontrol.EndpointRef{ID: "endpoint", Revision: "1"}, BaseURL: "https://example.test/v1", Credentials: resolver})
	if err != nil {
		t.Fatal(err)
	}
	plan := modelcontrol.ProviderPlan{Endpoint: modelcontrol.EndpointRef{ID: "endpoint", Revision: "1"}, Credential: modelcontrol.CredentialRef{ID: "key", Revision: "1"}}
	_, err = provider.Send(context.Background(), plan, modelexecution.OutboundRequest{Method: "POST", Path: "/v1/messages", Body: []byte("{}"), MaxResponseBytes: 1})
	if err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatalf("err=%v", err)
	}
	if got := material.Bytes(); string(got) != "\x00\x00\x00\x00\x00\x00" {
		t.Fatalf("material was not cleared: %q", got)
	}
	resolver.err = nil
	for _, header := range []string{"Authorization", "Proxy-Authorization", "Cookie", "Host"} {
		if _, err := provider.Send(context.Background(), plan, modelexecution.OutboundRequest{Method: "POST", Path: "/v1/messages", Headers: map[string]string{header: "bad"}, MaxResponseBytes: 1}); err == nil {
			t.Fatalf("header %s accepted", header)
		}
	}
	for _, baseURL := range []string{"https://example.test/v1?key=x", "https://example.test/v1#frag"} {
		if _, err := NewHTTPProvider(HTTPProviderConfig{Endpoint: modelcontrol.EndpointRef{ID: "endpoint", Revision: "1"}, BaseURL: baseURL, Credentials: resolver}); err == nil {
			t.Fatalf("accepted base URL %q", baseURL)
		}
	}
}

func TestProviderRejectsOversizeAndRedirectsWithoutFollowing(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		http.Redirect(w, r, "/other", http.StatusFound)
	}))
	defer server.Close()
	resolver := &trackingResolver{material: modelexecution.NewCredentialMaterial([]byte("secret"))}
	provider, err := NewHTTPProvider(HTTPProviderConfig{Endpoint: modelcontrol.EndpointRef{ID: "endpoint", Revision: "1"}, BaseURL: server.URL, Credentials: resolver})
	if err != nil {
		t.Fatal(err)
	}
	plan := modelcontrol.ProviderPlan{Endpoint: modelcontrol.EndpointRef{ID: "endpoint", Revision: "1"}, Credential: modelcontrol.CredentialRef{ID: "key", Revision: "1"}}
	tooLarge := make([]byte, int(modelexecution.DefaultMaxRequestBytes)+1)
	if _, err := provider.Send(context.Background(), plan, modelexecution.OutboundRequest{Method: "POST", Path: "/v1/messages", Body: tooLarge, MaxResponseBytes: 1}); err == nil || calls != 0 {
		t.Fatalf("oversize err=%v calls=%d", err, calls)
	}
	_, err = provider.Send(context.Background(), plan, modelexecution.OutboundRequest{Method: "POST", Path: "/v1/messages", Body: []byte("{}"), MaxResponseBytes: 1})
	var status *HTTPStatusError
	if !errors.As(err, &status) || status.Status != http.StatusFound || calls != 1 {
		t.Fatalf("redirect err=%v calls=%d", err, calls)
	}
}

func TestBoundedReadCloserExactLimit(t *testing.T) {
	exact, err := io.ReadAll(&boundedReadCloser{ReadCloser: io.NopCloser(strings.NewReader("abc")), remaining: 3})
	if err != nil || string(exact) != "abc" {
		t.Fatalf("exact=%q err=%v", exact, err)
	}
	if _, err := io.ReadAll(&boundedReadCloser{ReadCloser: io.NopCloser(strings.NewReader("abcd")), remaining: 3}); err == nil {
		t.Fatal("limit+1 accepted")
	}
}

func TestProviderRegistrationIsReusableAndFactorySafe(t *testing.T) {
	resolver := &trackingResolver{material: modelexecution.NewCredentialMaterial([]byte("secret"))}
	binding := modelcontrol.ImplementationBinding{Ref: modelcontrol.Ref{ID: "anthropic", Version: "1"}, ImplementationRevision: "impl"}
	registration, err := NewHTTPProviderRegistration(binding, HTTPProviderConfig{Endpoint: modelcontrol.EndpointRef{ID: "endpoint", Revision: "1"}, BaseURL: "https://example.test", Credentials: resolver})
	if err != nil {
		t.Fatal(err)
	}
	first, err := registration.Factory()
	if err != nil {
		t.Fatal(err)
	}
	second, err := registration.Factory()
	if err != nil || first != second {
		t.Fatalf("provider not reused: %v", err)
	}
	var wait sync.WaitGroup
	for index := 0; index < 20; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			provider, err := registration.Factory()
			if err != nil || provider != first {
				t.Errorf("factory provider=%T err=%v", provider, err)
			}
		}()
	}
	wait.Wait()
}

type trackingResolver struct {
	material modelexecution.CredentialMaterial
	mu       sync.Mutex
	err      error
}

func (r *trackingResolver) ResolveCredential(ctx context.Context, _ modelcontrol.CredentialRef) (modelexecution.CredentialMaterial, error) {
	if ctx == nil {
		return modelexecution.CredentialMaterial{}, errors.New("nil")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.material, r.err
}

func collect(t *testing.T, raw string) []modelexecution.Event {
	t.Helper()
	var events []modelexecution.Event
	if err := parseMessagesResponse(strings.NewReader(raw), func(event modelexecution.Event) error { events = append(events, event); return nil }); err != nil {
		t.Fatal(err)
	}
	return events
}
func anthSSE(events ...string) string {
	var out strings.Builder
	for _, event := range events {
		out.WriteString("event: ")
		out.WriteString("ignored\n")
		out.WriteString("data: ")
		out.WriteString(event)
		out.WriteString("\n\n")
	}
	return out.String()
}
