package execution

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// HTTPExecutor runs capabilities whose core.ExecutionSpec.Runtime is "http": the
// call goes to a remote HTTP endpoint as JSON and returns the response body.
// It is the first remote provider protocol — tenant connectors and external
// services integrate without entering the shared process.
//
// Credential injection: core.ExecutionSpec.Headers values of the form
// "$credential:<ref>" resolve the named credential from the request context
// (the manifest must declare it in RequiredCredentials). Missing credentials
// deny the call instead of sending an unauthenticated request.
type HTTPExecutor struct {
	// Client is only for trusted tests or a deployer-controlled transport. The
	// production-safe default validates the destination at connect time and
	// refuses redirects. A custom client requires AllowPrivateNetwork=true.
	Client *http.Client
	// AllowPrivateNetwork is an explicit escape hatch for trusted local tests or
	// deployments whose egress isolation is enforced outside this process.
	AllowPrivateNetwork bool
}

// defaultHTTPClient is shared by all public-network executors. DNS is resolved
// again at connection time, closing the bind-time DNS-rebinding window.
var defaultHTTPClient = newPublicHTTPClient()

func (HTTPExecutor) Runtime() string          { return "http" }
func (HTTPExecutor) ArtifactRevision() string { return "http-executor/v5-bounded-headers" }

func (e HTTPExecutor) Execute(ctx context.Context, spec core.ExecutionSpec, request core.CapabilityRequest) (core.CapabilityResult, error) {
	if spec.Entrypoint == "" {
		return deniedResult("invalid_execution", "http capability has no entrypoint URL"), nil
	}
	if !e.AllowPrivateNetwork {
		if err := ValidatePublicHTTPURL(spec.Entrypoint); err != nil {
			return deniedResult("network_denied", err.Error()), nil
		}
	}
	client := e.Client
	if client == nil {
		client = defaultHTTPClient
	} else if !e.AllowPrivateNetwork {
		return deniedResult("invalid_execution", "custom http client requires explicit private-network opt-in"), nil
	}
	method := strings.ToUpper(spec.Method)
	if method == "" {
		method = http.MethodPost
	}
	if err := ValidateHTTPMethod(method); err != nil {
		return deniedResult("invalid_execution", err.Error()), nil
	}
	if len(spec.Headers) > maxHTTPHeaders {
		return deniedResult("invalid_execution", fmt.Sprintf("http header list exceeds %d entries", maxHTTPHeaders)), nil
	}

	endpoint := spec.Entrypoint
	var body io.Reader
	if method == http.MethodGet || method == http.MethodDelete {
		parsed, err := url.Parse(endpoint)
		if err != nil {
			return deniedResult("invalid_execution", err.Error()), nil
		}
		encodedURL, err := encodeHTTPQueryURL(parsed, request.Args)
		if err != nil {
			return deniedResult("invalid_execution", err.Error()), nil
		}
		endpoint = encodedURL
	} else {
		encoded, err := json.Marshal(request.Args)
		if err != nil {
			return deniedResult("invalid_execution", fmt.Sprintf("encode http args: %v", err)), nil
		}
		if len(encoded) > maxHTTPRequestBodyBytes {
			return deniedResult("invalid_execution", fmt.Sprintf("http request body exceeds %d bytes", maxHTTPRequestBodyBytes)), nil
		}
		body = bytes.NewReader(encoded)
	}

	req, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return deniedResult("invalid_execution", err.Error()), nil
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for name, value := range spec.Headers {
		resolved, err := resolveHeaderValue(ctx, request, value)
		if err != nil {
			return deniedResult("credential_unavailable", fmt.Sprintf("header %s: %v", name, err)), nil
		}
		if err := ValidateHTTPHeader(name, resolved); err != nil {
			return deniedResult("invalid_execution", err.Error()), nil
		}
		req.Header.Set(name, resolved)
	}
	if request.IdempotencyKey != "" {
		if err := ValidateHTTPHeader("Idempotency-Key", request.IdempotencyKey); err != nil {
			return deniedResult("invalid_execution", err.Error()), nil
		}
		req.Header.Set("Idempotency-Key", request.IdempotencyKey)
	}

	resp, err := client.Do(req)
	if err != nil {
		return core.CapabilityResult{Content: err.Error(), OK: false}, nil
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if err != nil {
		return core.CapabilityResult{Content: err.Error(), OK: false}, nil
	}
	if len(payload) > 1<<20 {
		return core.CapabilityResult{
			Content: "remote endpoint response exceeded 1 MiB", OK: false,
			Metadata: map[string]any{"code": "capability_output_too_large"},
		}, nil
	}
	if resp.StatusCode/100 != 2 {
		return core.CapabilityResult{
			Content:  fmt.Sprintf("remote endpoint returned %d: %s", resp.StatusCode, truncate(string(payload), 500)),
			OK:       false,
			Metadata: map[string]any{"status": resp.StatusCode},
		}, nil
	}
	return core.CapabilityResult{Content: string(payload), OK: true}, nil
}

func newPublicHTTPClient() *http.Client {
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	return newPublicHTTPClientWithDial(net.DefaultResolver.LookupIPAddr, dialer.DialContext)
}

func newPublicHTTPClientWithDial(lookup func(context.Context, string) ([]net.IPAddr, error), dial func(context.Context, string, string) (net.Conn, error)) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// A process-wide proxy may resolve the target itself and reach networks the
	// local safety dialer cannot observe. Secure defaults therefore connect
	// directly; deployers needing a proxy must inject a separately hardened
	// custom client and explicitly opt into that trust boundary.
	transport.Proxy = nil
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, fmt.Errorf("split destination %q: %w", address, err)
		}
		addresses, err := lookup(ctx, host)
		if err != nil {
			return nil, fmt.Errorf("resolve destination %q: %w", host, err)
		}
		if len(addresses) == 0 {
			return nil, fmt.Errorf("destination %q resolved to no addresses", host)
		}
		var lastErr error
		for _, resolved := range addresses {
			if !isPubliclyRoutable(resolved.IP) {
				return nil, fmt.Errorf("destination %q resolved to non-public address %s", host, resolved.IP)
			}
			conn, err := dial(ctx, network, net.JoinHostPort(resolved.IP.String(), port))
			if err == nil {
				return conn, nil
			}
			lastErr = err
		}
		return nil, lastErr
	}
	return &http.Client{
		Transport: transport,
		Timeout:   30 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

const (
	maxHTTPQueryBytes       = 8 << 10
	maxHTTPRequestBodyBytes = 1 << 20
	maxHTTPHeaders          = 64
	maxHTTPHeaderNameBytes  = 256
	maxHTTPHeaderValueBytes = 8 << 10
)

func encodeHTTPQueryURL(parsed *url.URL, args map[string]any) (string, error) {
	query := parsed.Query()
	for key, value := range args {
		if strings.TrimSpace(key) == "" || strings.ContainsRune(key, 0) || strings.ContainsAny(key, "\r\n") {
			return "", fmt.Errorf("http query argument name is invalid")
		}
		encoded, err := encodeHTTPQueryValue(value)
		if err != nil {
			return "", err
		}
		if strings.ContainsRune(encoded, 0) {
			return "", fmt.Errorf("http query argument contains NUL")
		}
		query.Set(key, encoded)
	}
	parsed.RawQuery = query.Encode()
	if len(parsed.RawQuery) > maxHTTPQueryBytes {
		return "", fmt.Errorf("http query string exceeds %d bytes", maxHTTPQueryBytes)
	}
	return parsed.String(), nil
}

func encodeHTTPQueryValue(value any) (string, error) {
	switch typed := value.(type) {
	case nil:
		return "", nil
	case string:
		return typed, nil
	case bool:
		return strconv.FormatBool(typed), nil
	case int:
		return strconv.Itoa(typed), nil
	case int64:
		return strconv.FormatInt(typed, 10), nil
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64), nil
	case json.Number:
		return typed.String(), nil
	default:
		return "", fmt.Errorf("http query argument must be a scalar")
	}
}

func ValidateHTTPMethod(method string) error {
	switch strings.ToUpper(strings.TrimSpace(method)) {
	case "", http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return nil
	default:
		return fmt.Errorf("http method %q is not allowed", method)
	}
}

// ValidateHTTPHeader rejects names or values that cannot be placed on the
// wire without splitting the request. Bind-time checks reuse this so
// restored credentials and Go-registered specs cannot bypass it.
func ValidateHTTPHeader(name, value string) error {
	if strings.TrimSpace(name) == "" || strings.ContainsAny(name, " \t\r\n\x00:") {
		return fmt.Errorf("http header name %q is invalid", name)
	}
	if len(name) > maxHTTPHeaderNameBytes {
		return fmt.Errorf("http header name exceeds %d bytes", maxHTTPHeaderNameBytes)
	}
	if strings.ContainsAny(value, "\r\n\x00") {
		return fmt.Errorf("http header %s contains a newline", name)
	}
	if len(value) > maxHTTPHeaderValueBytes {
		return fmt.Errorf("http header %s exceeds %d bytes", name, maxHTTPHeaderValueBytes)
	}
	return nil
}

// resolveHeaderValue returns literal header values as-is and resolves
// "$credential:<ref>" through the call's least-authority accessor.
func resolveHeaderValue(ctx context.Context, request core.CapabilityRequest, value string) (string, error) {
	ref, ok := strings.CutPrefix(value, "$credential:")
	if !ok {
		return value, nil
	}
	if request.Context.Credentials == nil {
		return "", fmt.Errorf("no credentials available")
	}
	credential, err := request.Context.Credentials.Resolve(ctx, core.CredentialRef(ref))
	if err != nil {
		return "", err
	}
	return credential.Value, nil
}

func deniedResult(code, message string) core.CapabilityResult {
	return core.CapabilityResult{Content: message, OK: false, Metadata: map[string]any{"code": code}}
}

func truncate(text string, limit int) string {
	if len(text) <= limit {
		return text
	}
	return text[:limit]
}
