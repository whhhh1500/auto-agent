// Package openai implements the OpenAI-compatible HTTP provider and Chat
// Completions protocol for the protocol-neutral modelexecution contract.
package openai

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/cc-auto-agent/harness-core/pkg/app/modelcontrol"
	"github.com/cc-auto-agent/harness-core/pkg/app/modelexecution"
)

// HTTPProviderConfig names one opaque endpoint identity and supplies an HTTP
// transport. BaseURL is adapter-private configuration, never plan evidence.
type HTTPProviderConfig struct {
	Endpoint    modelcontrol.EndpointRef
	BaseURL     string
	Credentials modelexecution.CredentialResolver
	Client      *http.Client
}

// HTTPProvider owns endpoint validation, authorization resolution, and HTTP
// connection reuse. Protocols only provide a relative path and wire bytes.
type HTTPProvider struct {
	endpoint    modelcontrol.EndpointRef
	baseURL     string
	credentials modelexecution.CredentialResolver
	client      *http.Client
}

func NewHTTPProvider(config HTTPProviderConfig) (*HTTPProvider, error) {
	if config.Endpoint.ID == "" || config.Endpoint.Revision == "" {
		return nil, fmt.Errorf("openai endpoint reference is incomplete")
	}
	parsed, err := url.Parse(strings.TrimSpace(config.BaseURL))
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Hostname() == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, fmt.Errorf("openai base URL is invalid")
	}
	client := config.Client
	if client == nil {
		client = &http.Client{Timeout: 180 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	return &HTTPProvider{endpoint: config.Endpoint, baseURL: strings.TrimRight(parsed.String(), "/"), credentials: config.Credentials, client: client}, nil
}

func (p *HTTPProvider) Send(ctx context.Context, plan modelcontrol.ProviderPlan, outgoing modelexecution.OutboundRequest) (modelexecution.InboundResponse, error) {
	if p == nil || ctx == nil || p.client == nil || plan.Endpoint != p.endpoint {
		return modelexecution.InboundResponse{}, fmt.Errorf("openai provider endpoint binding mismatch")
	}
	if outgoing.Method != http.MethodPost || !strings.HasPrefix(outgoing.Path, "/") || len(outgoing.Body) > int(modelexecution.DefaultMaxRequestBytes) || outgoing.MaxResponseBytes <= 0 || outgoing.MaxResponseBytes > modelexecution.DefaultMaxResponseBytes {
		return modelexecution.InboundResponse{}, fmt.Errorf("openai outbound request is invalid")
	}
	request, err := http.NewRequestWithContext(ctx, outgoing.Method, p.baseURL+outgoing.Path, bytes.NewReader(outgoing.Body))
	if err != nil {
		return modelexecution.InboundResponse{}, err
	}
	for key, value := range outgoing.Headers {
		request.Header.Set(key, value)
	}
	if plan.Credential != (modelcontrol.CredentialRef{}) {
		if p.credentials == nil {
			return modelexecution.InboundResponse{}, fmt.Errorf("openai credential resolver is unavailable")
		}
		material, err := p.credentials.ResolveCredential(ctx, plan.Credential)
		defer material.Clear()
		if err != nil {
			return modelexecution.InboundResponse{}, fmt.Errorf("openai credential resolution failed")
		}
		secret := material.Bytes()
		defer clear(secret)
		if len(secret) == 0 {
			return modelexecution.InboundResponse{}, fmt.Errorf("openai credential is empty")
		}
		request.Header.Set("Authorization", "Bearer "+string(secret))
	}
	response, err := p.client.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return modelexecution.InboundResponse{}, ctx.Err()
		}
		if isRetryableTransportError(err) {
			return modelexecution.InboundResponse{}, &transportError{err: err}
		}
		return modelexecution.InboundResponse{}, err
	}
	if response == nil || response.Body == nil {
		return modelexecution.InboundResponse{}, fmt.Errorf("openai provider returned an empty response")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		defer response.Body.Close()
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 16<<10))
		return modelexecution.InboundResponse{}, &HTTPStatusError{Status: response.StatusCode, Body: "upstream response redacted"}
	}
	return modelexecution.InboundResponse{Status: response.StatusCode, Body: response.Body}, nil
}

// transportError marks an error returned directly by http.Client.Do as a
// transport retry candidate. Protocol decoders must not create this marker:
// EOF and UnexpectedEOF from a response body can mean a truncated provider
// payload after the provider has already begun processing the request.
//
// It is intentionally package-private. Callers use the Retryable method
// structurally rather than importing this adapter implementation detail.
type transportError struct{ err error }

func (e *transportError) Error() string {
	return "openai HTTP transport failed"
}

func (e *transportError) Unwrap() error   { return e.err }
func (e *transportError) Retryable() bool { return e != nil && e.err != nil }

func isRetryableTransportError(err error) bool {
	for {
		urlError, ok := err.(*url.Error)
		if !ok || urlError.Err == nil {
			break
		}
		err = urlError.Err
	}
	var networkError net.Error
	return errors.As(err, &networkError) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed)
}

// HTTPStatusError reports only status and a fixed redacted bounded body label.
// It never formats provider error payloads, which may contain secrets.
type HTTPStatusError struct {
	Status int
	Body   string
}

func (e *HTTPStatusError) Error() string {
	if e == nil {
		return "openai HTTP error"
	}
	return fmt.Sprintf("model provider status %d: %s", e.Status, e.Body)
}
func (e *HTTPStatusError) Retryable() bool {
	return e != nil && (e.Status == http.StatusTooManyRequests || e.Status >= 500)
}

// StaticCredentialResolver is a compatibility-only in-memory resolver. The
// supplied bytes are copied; callers should Clear it when shutting down.
type StaticCredentialResolver struct {
	material modelexecution.CredentialMaterial
}

func NewStaticCredentialResolver(value []byte) *StaticCredentialResolver {
	return &StaticCredentialResolver{material: modelexecution.NewCredentialMaterial(value)}
}
func (r *StaticCredentialResolver) ResolveCredential(ctx context.Context, ref modelcontrol.CredentialRef) (modelexecution.CredentialMaterial, error) {
	if ctx == nil || ref == (modelcontrol.CredentialRef{}) || r == nil {
		return modelexecution.CredentialMaterial{}, fmt.Errorf("openai static credential resolution rejected")
	}
	value := r.material.Bytes()
	defer clear(value)
	return modelexecution.NewCredentialMaterial(value), nil
}
func (r *StaticCredentialResolver) Clear() {
	if r != nil {
		r.material.Clear()
	}
}
func clear(value []byte) {
	for index := range value {
		value[index] = 0
	}
}
