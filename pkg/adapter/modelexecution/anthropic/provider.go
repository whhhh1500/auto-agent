// Package anthropic adapts Anthropic Messages to the protocol-neutral model
// execution contract. It has no server/default registration side effects.
package anthropic

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/whhhh1500/auto-agent/pkg/app/modelcontrol"
	"github.com/whhhh1500/auto-agent/pkg/app/modelexecution"
)

const apiVersion = "2023-06-01"

type HTTPProviderConfig struct {
	Endpoint    modelcontrol.EndpointRef
	BaseURL     string
	Credentials modelexecution.CredentialResolver
	Client      *http.Client
}

// HTTPProvider owns the Anthropic endpoint, API-key resolution and transport.
// Protocols cannot replace its authentication or version headers.
type HTTPProvider struct {
	endpoint    modelcontrol.EndpointRef
	baseURL     string
	credentials modelexecution.CredentialResolver
	client      *http.Client
}

func NewHTTPProvider(config HTTPProviderConfig) (*HTTPProvider, error) {
	if config.Endpoint.ID == "" || config.Endpoint.Revision == "" || config.Credentials == nil {
		return nil, fmt.Errorf("anthropic provider configuration is incomplete")
	}
	parsed, err := url.Parse(strings.TrimSpace(config.BaseURL))
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Hostname() == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, fmt.Errorf("anthropic base URL is invalid")
	}
	client := isolatedClient(config.Client)
	return &HTTPProvider{endpoint: config.Endpoint, baseURL: strings.TrimRight(parsed.String(), "/"), credentials: config.Credentials, client: client}, nil
}

func isolatedClient(source *http.Client) *http.Client {
	if source == nil {
		source = &http.Client{Timeout: 180 * time.Second}
	}
	copyOf := *source
	copyOf.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &copyOf
}

func (p *HTTPProvider) Send(ctx context.Context, plan modelcontrol.ProviderPlan, outgoing modelexecution.OutboundRequest) (modelexecution.InboundResponse, error) {
	if p == nil || ctx == nil || p.client == nil || p.credentials == nil || plan.Endpoint != p.endpoint || plan.Credential.ID == "" || plan.Credential.Revision == "" {
		return modelexecution.InboundResponse{}, fmt.Errorf("anthropic provider binding is invalid")
	}
	if outgoing.Method != http.MethodPost || outgoing.Path != "/v1/messages" || len(outgoing.Body) > int(modelexecution.DefaultMaxRequestBytes) || outgoing.MaxResponseBytes <= 0 || outgoing.MaxResponseBytes > modelexecution.DefaultMaxResponseBytes {
		return modelexecution.InboundResponse{}, fmt.Errorf("anthropic outbound request is invalid")
	}
	for key := range outgoing.Headers {
		if strings.EqualFold(key, "x-api-key") || strings.EqualFold(key, "anthropic-version") || strings.EqualFold(key, "authorization") || strings.EqualFold(key, "proxy-authorization") || strings.EqualFold(key, "cookie") || strings.EqualFold(key, "host") {
			return modelexecution.InboundResponse{}, fmt.Errorf("anthropic protected header override")
		}
	}
	material, err := p.credentials.ResolveCredential(ctx, plan.Credential)
	defer material.Clear()
	if err != nil {
		return modelexecution.InboundResponse{}, fmt.Errorf("anthropic credential resolution failed")
	}
	secret := material.Bytes()
	defer clear(secret)
	if len(secret) == 0 {
		return modelexecution.InboundResponse{}, fmt.Errorf("anthropic credential is empty")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+outgoing.Path, bytes.NewReader(outgoing.Body))
	if err != nil {
		return modelexecution.InboundResponse{}, err
	}
	for key, value := range outgoing.Headers {
		request.Header.Set(key, value)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "text/event-stream, application/json")
	request.Header.Set("x-api-key", string(secret))
	request.Header.Set("anthropic-version", apiVersion)
	response, err := p.client.Do(request)
	if err != nil {
		return modelexecution.InboundResponse{}, err
	}
	if response == nil || response.Body == nil {
		return modelexecution.InboundResponse{}, fmt.Errorf("anthropic provider returned an empty response")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		defer response.Body.Close()
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 16<<10))
		return modelexecution.InboundResponse{}, &HTTPStatusError{Status: response.StatusCode, Body: "upstream response redacted"}
	}
	return modelexecution.InboundResponse{Status: response.StatusCode, Body: &boundedReadCloser{ReadCloser: response.Body, remaining: outgoing.MaxResponseBytes}}, nil
}

// NewHTTPProviderRegistration creates a reusable provider once. Its factory is
// intentionally cheap and has no network side effects.
func NewHTTPProviderRegistration(binding modelcontrol.ImplementationBinding, config HTTPProviderConfig) (modelexecution.ProviderRegistration, error) {
	if binding.Ref.ID == "" || binding.Ref.Version == "" || binding.ImplementationRevision == "" {
		return modelexecution.ProviderRegistration{}, fmt.Errorf("anthropic provider registration is invalid")
	}
	provider, err := NewHTTPProvider(config)
	if err != nil {
		return modelexecution.ProviderRegistration{}, err
	}
	return modelexecution.ProviderRegistration{Binding: binding, Factory: func() (modelexecution.Provider, error) { return provider, nil }}, nil
}

type HTTPStatusError struct {
	Status int
	Body   string
}

func (e *HTTPStatusError) Error() string {
	if e == nil {
		return "anthropic HTTP error"
	}
	return fmt.Sprintf("model provider status %d: %s", e.Status, e.Body)
}
func (e *HTTPStatusError) Retryable() bool {
	return e != nil && (e.Status == http.StatusTooManyRequests || e.Status >= 500)
}

type boundedReadCloser struct {
	io.ReadCloser
	remaining int64
}

func (r *boundedReadCloser) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		var probe [1]byte
		n, err := r.ReadCloser.Read(probe[:])
		if n > 0 {
			return 0, fmt.Errorf("anthropic response exceeded limit")
		}
		return 0, err
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.ReadCloser.Read(p)
	r.remaining -= int64(n)
	return n, err
}

func clear(value []byte) {
	for index := range value {
		value[index] = 0
	}
}
