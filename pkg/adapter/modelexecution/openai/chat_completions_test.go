package openai

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/whhhh1500/auto-agent/pkg/app/modelcontrol"
	"github.com/whhhh1500/auto-agent/pkg/app/modelexecution"
)

func TestChatCompletionsPreservesToolFragmentsAndUsageBeforeFinish(t *testing.T) {
	stream := "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-1\",\"function\":{\"name\":\"weather\",\"arguments\":\"{\\\"city\\\":\"}}]}}]}\n\n" + "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"\\\"Paris\\\"}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\n" + "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":4}}\n\ndata: [DONE]\n\n"
	var events []modelexecution.Event
	if err := parseChatResponse(strings.NewReader(stream), func(event modelexecution.Event) error { events = append(events, event); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(events) != 4 || events[0].Kind != modelexecution.EventToolCallDelta || string(events[0].ToolCall.ArgumentsFragment) != `{"city":` || events[1].Kind != modelexecution.EventToolCallDelta || events[2].Kind != modelexecution.EventUsage || events[3].Kind != modelexecution.EventFinish {
		t.Fatalf("events=%#v", events)
	}
}

func TestChatProtocolUsesProviderAndHonorsEmitError(t *testing.T) {
	p := ChatCompletionsProtocol{}
	provider := recordingProvider{body: io.NopCloser(strings.NewReader(`{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}`))}
	request := modelexecution.Request{Plan: modelcontrol.ProviderPlan{Catalog: modelcontrol.CatalogModel{WireModel: "wire"}}, Messages: []modelexecution.Message{{Role: "user", Content: "hi"}}}
	want := io.EOF
	if err := p.Execute(context.Background(), request, &provider, func(modelexecution.Event) error { return want }); err != want {
		t.Fatalf("err=%v", err)
	}
	if provider.path != "/chat/completions" || !strings.Contains(string(provider.request.Body), `"model":"wire"`) {
		t.Fatalf("outbound=%#v", provider.request)
	}
}

func TestHTTPProviderRedactsStatusBodyAndHonorsCanceledContext(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusUnauthorized)
		_, _ = writer.Write([]byte("credential=secret-value"))
	}))
	defer server.Close()
	resolver := NewStaticCredentialResolver([]byte("secret-value"))
	defer resolver.Clear()
	provider, err := NewHTTPProvider(HTTPProviderConfig{Endpoint: modelcontrol.EndpointRef{ID: "endpoint", Revision: "1"}, BaseURL: server.URL, Credentials: resolver})
	if err != nil {
		t.Fatal(err)
	}
	plan := modelcontrol.ProviderPlan{Endpoint: modelcontrol.EndpointRef{ID: "endpoint", Revision: "1"}, Credential: modelcontrol.CredentialRef{ID: "credential", Revision: "1"}}
	_, err = provider.Send(context.Background(), plan, modelexecution.OutboundRequest{Method: http.MethodPost, Path: "/x", MaxResponseBytes: 1024})
	var status *HTTPStatusError
	if !errors.As(err, &status) || strings.Contains(err.Error(), "secret-value") {
		t.Fatalf("status=%v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = provider.Send(canceled, plan, modelexecution.OutboundRequest{Method: http.MethodPost, Path: "/x", MaxResponseBytes: 1024})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel=%v", err)
	}
}

func TestHTTPProviderOnlyMarksActualTransportErrorsRetryable(t *testing.T) {
	endpoint := modelcontrol.EndpointRef{ID: "endpoint", Revision: "1"}
	plan := modelcontrol.ProviderPlan{Endpoint: endpoint}
	request := modelexecution.OutboundRequest{Method: http.MethodPost, Path: "/x", MaxResponseBytes: 1024}
	cases := []struct {
		name      string
		roundTrip error
		marked    bool
	}{
		{name: "wrapped policy error", roundTrip: &url.Error{Op: "Get", URL: "https://provider.invalid/?token=secret", Err: errors.New("redirect policy denied")}},
		{name: "wrapped EOF", roundTrip: &url.Error{Op: "Get", URL: "https://provider.invalid/?token=secret", Err: io.EOF}, marked: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			provider, err := NewHTTPProvider(HTTPProviderConfig{Endpoint: endpoint, BaseURL: "https://provider.invalid", Client: &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) { return nil, tc.roundTrip })}})
			if err != nil {
				t.Fatal(err)
			}
			_, err = provider.Send(context.Background(), plan, request)
			if err == nil {
				t.Fatal("Send unexpectedly succeeded")
			}
			var retryable interface{ Retryable() bool }
			if got := errors.As(err, &retryable) && retryable.Retryable(); got != tc.marked {
				t.Fatalf("retryable=%t, want %t: %v", got, tc.marked, err)
			}
			if tc.marked && (strings.Contains(err.Error(), "provider.invalid") || strings.Contains(err.Error(), "secret")) {
				t.Fatalf("transport error leaked endpoint details: %q", err)
			}
		})
	}
}

func TestLimitReaderAcceptsExactLimitAndRejectsOneExtraByte(t *testing.T) {
	exact, err := io.ReadAll(&limitReader{reader: strings.NewReader("abc"), remaining: 3})
	if err != nil || string(exact) != "abc" {
		t.Fatalf("exact=%q err=%v", exact, err)
	}
	_, err = io.ReadAll(&limitReader{reader: strings.NewReader("abcd"), remaining: 3})
	if err == nil {
		t.Fatal("limit+1 accepted")
	}
}

func TestHTTPProviderEnforcesExactRequestBodyLimit(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls++
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("ok"))
	}))
	defer server.Close()
	provider, err := NewHTTPProvider(HTTPProviderConfig{Endpoint: modelcontrol.EndpointRef{ID: "endpoint", Revision: "1"}, BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	plan := modelcontrol.ProviderPlan{Endpoint: modelcontrol.EndpointRef{ID: "endpoint", Revision: "1"}}
	exact := make([]byte, int(modelexecution.DefaultMaxRequestBytes))
	response, err := provider.Send(context.Background(), plan, modelexecution.OutboundRequest{Method: http.MethodPost, Path: "/x", Body: exact, MaxResponseBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if calls != 1 {
		t.Fatalf("exact calls=%d", calls)
	}
	if _, err := provider.Send(context.Background(), plan, modelexecution.OutboundRequest{Method: http.MethodPost, Path: "/x", Body: append(exact, 0), MaxResponseBytes: 1}); err == nil {
		t.Fatal("limit+1 accepted")
	}
	if calls != 1 {
		t.Fatalf("over-limit called transport: %d", calls)
	}
}

func TestHTTPProviderClearsPartialCredentialAndRejectsAmbiguousBaseURL(t *testing.T) {
	material := modelexecution.NewCredentialMaterial([]byte("secret"))
	resolver := partialErrorResolver{material: material, err: errors.New("resolver secret")}
	provider, err := NewHTTPProvider(HTTPProviderConfig{Endpoint: modelcontrol.EndpointRef{ID: "endpoint", Revision: "1"}, BaseURL: "https://example.test/v1", Credentials: resolver})
	if err != nil {
		t.Fatal(err)
	}
	plan := modelcontrol.ProviderPlan{Endpoint: modelcontrol.EndpointRef{ID: "endpoint", Revision: "1"}, Credential: modelcontrol.CredentialRef{ID: "key", Revision: "1"}}
	_, err = provider.Send(context.Background(), plan, modelexecution.OutboundRequest{Method: http.MethodPost, Path: "/chat/completions", Body: []byte("{}"), MaxResponseBytes: 1})
	if err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatalf("err=%v", err)
	}
	if got := material.Bytes(); string(got) != "\x00\x00\x00\x00\x00\x00" {
		t.Fatalf("material was not cleared: %q", got)
	}
	for _, baseURL := range []string{"https://example.test/v1?key=x", "https://example.test/v1#frag"} {
		if _, err := NewHTTPProvider(HTTPProviderConfig{Endpoint: modelcontrol.EndpointRef{ID: "endpoint", Revision: "1"}, BaseURL: baseURL}); err == nil {
			t.Fatalf("accepted base URL %q", baseURL)
		}
	}
}

type recordingProvider struct {
	request modelexecution.OutboundRequest
	path    string
	body    io.ReadCloser
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

type partialErrorResolver struct {
	material modelexecution.CredentialMaterial
	err      error
}

func (r partialErrorResolver) ResolveCredential(context.Context, modelcontrol.CredentialRef) (modelexecution.CredentialMaterial, error) {
	return r.material, r.err
}

func (p *recordingProvider) Send(_ context.Context, _ modelcontrol.ProviderPlan, request modelexecution.OutboundRequest) (modelexecution.InboundResponse, error) {
	p.request = request
	p.path = request.Path
	return modelexecution.InboundResponse{Status: 200, Body: p.body}, nil
}
