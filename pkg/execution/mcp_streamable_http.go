package execution

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

const (
	defaultMCPHTTPProtocolVersion = "2025-11-25"
	maxMCPHTTPBodyBytes           = maxMCPResponseLineBytes
	maxMCPHTTPSessionIDBytes      = 512
)

var supportedMCPHTTPProtocolVersions = map[string]bool{
	"2025-06-18": true,
	"2025-11-25": true,
}

// MCPStreamableHTTPConfig describes one MCP Streamable HTTP endpoint. The
// endpoint is a separate transport from MCPServerConfig's local stdio process.
// A custom client is a trusted deployment boundary and requires explicit
// private-network opt-in; the default client validates public addresses again
// when dialing.
type MCPStreamableHTTPConfig struct {
	Endpoint            string
	Namespace           string
	Version             string
	ProtocolVersion     string
	ClientName          string
	CallTimeout         time.Duration
	Client              *http.Client
	AllowPrivateNetwork bool
	Observer            ToolLibraryObserver
}

type mcpHTTPConnection struct {
	cfg MCPStreamableHTTPConfig

	mu              sync.Mutex
	gate            chan struct{}
	activeCancel    context.CancelFunc
	closed          bool
	client          *http.Client
	nextID          int64
	initialized     bool
	protocolVersion string
	sessionID       string
}

func validateMCPStreamableHTTPConfig(cfg MCPStreamableHTTPConfig) error {
	if err := core.ValidateNamespacedID(cfg.Namespace); err != nil {
		return fmt.Errorf("mcp namespace %q must be a namespaced identifier", cfg.Namespace)
	}
	endpoint, err := url.Parse(strings.TrimSpace(cfg.Endpoint))
	if err != nil {
		return fmt.Errorf("invalid mcp endpoint: %w", err)
	}
	if endpoint.Scheme != "http" && endpoint.Scheme != "https" {
		return fmt.Errorf("mcp endpoint scheme must be http or https")
	}
	if endpoint.Hostname() == "" || endpoint.User != nil || endpoint.Fragment != "" {
		return fmt.Errorf("mcp endpoint must have a host and no user information or fragment")
	}
	if cfg.Client != nil && !cfg.AllowPrivateNetwork {
		return fmt.Errorf("custom mcp http client requires explicit private-network opt-in")
	}
	if cfg.AllowPrivateNetwork && cfg.Client == nil {
		return fmt.Errorf("private-network mcp endpoint requires an explicit trusted http client")
	}
	if !cfg.AllowPrivateNetwork {
		if err := ValidatePublicHTTPURL(cfg.Endpoint); err != nil {
			return fmt.Errorf("mcp endpoint: %w", err)
		}
	}
	if cfg.ProtocolVersion != "" && !supportedMCPHTTPProtocolVersions[cfg.ProtocolVersion] {
		return fmt.Errorf("mcp protocol version %q is unsupported", cfg.ProtocolVersion)
	}
	return nil
}

func newMCPHTTPConnection(cfg MCPStreamableHTTPConfig) *mcpHTTPConnection {
	client := cfg.Client
	if client == nil {
		client = newPublicHTTPClient()
	} else {
		clone := *client
		clone.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
		client = &clone
	}
	protocol := cfg.ProtocolVersion
	if protocol == "" {
		protocol = defaultMCPHTTPProtocolVersion
	}
	gate := make(chan struct{}, 1)
	gate <- struct{}{}
	return &mcpHTTPConnection{cfg: cfg, client: client, gate: gate, protocolVersion: protocol}
}

func (c *mcpHTTPConnection) identity() mcpTransportIdentity {
	if c == nil {
		return mcpTransportIdentity{}
	}
	return mcpTransportIdentity{namespace: c.cfg.Namespace, version: c.cfg.Version, observer: c.cfg.Observer}
}

func (c *mcpHTTPConnection) call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	if err := c.acquire(ctx); err != nil {
		return nil, err
	}
	defer c.release()
	if c.isClosed() {
		return nil, fmt.Errorf("mcp http connection is closed")
	}
	if !c.initialized && method != "initialize" {
		if err := c.initializeLocked(ctx); err != nil {
			c.resetLocked()
			return nil, err
		}
	}
	result, err := c.requestLocked(ctx, method, params, method != "initialize")
	if err != nil {
		if !c.isClosed() {
			c.resetLocked()
		}
	}
	return result, err
}

func (c *mcpHTTPConnection) initializeLocked(ctx context.Context) error {
	protocol := c.protocolVersion
	name := c.cfg.ClientName
	if name == "" {
		name = "harness-core"
	}
	result, sessionID, err := c.requestWithSessionLocked(ctx, "initialize", map[string]any{
		"protocolVersion": protocol,
		"capabilities":    map[string]any{},
		"clientInfo":      map[string]any{"name": name, "version": "1"},
	}, false)
	if err != nil {
		return err
	}
	var initialized struct {
		ProtocolVersion string          `json:"protocolVersion"`
		Capabilities    json.RawMessage `json:"capabilities"`
		ServerInfo      json.RawMessage `json:"serverInfo"`
	}
	if err := json.Unmarshal(result, &initialized); err != nil {
		return fmt.Errorf("mcp initialize decode: %w", err)
	}
	if !supportedMCPHTTPProtocolVersions[initialized.ProtocolVersion] {
		return fmt.Errorf("mcp server negotiated unsupported protocol version %q", initialized.ProtocolVersion)
	}
	if !validMCPInitializeObject(initialized.Capabilities) || !validMCPServerInfo(initialized.ServerInfo) {
		return fmt.Errorf("mcp initialize result requires object capabilities and named, versioned serverInfo")
	}
	c.protocolVersion = initialized.ProtocolVersion
	if sessionID != "" {
		if err := validateMCPHTTPSessionID(sessionID); err != nil {
			return err
		}
		c.sessionID = sessionID
	}
	c.initialized = true
	if err := c.notifyLocked(ctx, "notifications/initialized", nil); err != nil {
		return err
	}
	return nil
}

func (c *mcpHTTPConnection) requestLocked(ctx context.Context, method string, params any, sendCancel bool) (json.RawMessage, error) {
	result, _, err := c.requestWithSessionLocked(ctx, method, params, sendCancel)
	return result, err
}

func (c *mcpHTTPConnection) requestWithSessionLocked(ctx context.Context, method string, params any, sendCancel bool) (json.RawMessage, string, error) {
	c.nextID++
	id := c.nextID
	frame := map[string]any{"jsonrpc": "2.0", "id": id, "method": method}
	if params != nil {
		frame["params"] = params
	}
	callCtx, cancel, err := c.beginRequest(ctx)
	if err != nil {
		return nil, "", err
	}
	result, sessionID, err := c.postRequestLocked(callCtx, frame, id, true)
	requestErr := callCtx.Err()
	c.endRequest(cancel)
	if err == nil {
		return result, sessionID, nil
	}
	if sendCancel && requestErr != nil {
		c.cancelRequestLocked(id)
	}
	if ctx.Err() != nil {
		return nil, "", ctx.Err()
	}
	if requestErr != nil {
		return nil, "", fmt.Errorf("mcp %s timed out after %s", method, c.timeout())
	}
	return nil, "", err
}

func (c *mcpHTTPConnection) notifyLocked(ctx context.Context, method string, params any) error {
	frame := map[string]any{"jsonrpc": "2.0", "method": method}
	if params != nil {
		frame["params"] = params
	}
	callCtx, cancel, err := c.beginRequest(ctx)
	if err != nil {
		return err
	}
	_, _, err = c.postRequestLocked(callCtx, frame, 0, false)
	requestErr := callCtx.Err()
	c.endRequest(cancel)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if requestErr != nil {
		return fmt.Errorf("mcp %s timed out after %s", method, c.timeout())
	}
	return err
}

func (c *mcpHTTPConnection) postRequestLocked(ctx context.Context, frame any, expectedID int64, expectsResponse bool) (json.RawMessage, string, error) {
	body, err := json.Marshal(frame)
	if err != nil {
		return nil, "", err
	}
	if len(body) > maxMCPHTTPBodyBytes {
		return nil, "", fmt.Errorf("mcp request exceeded %d bytes", maxMCPHTTPBodyBytes)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.Endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, "", err
	}
	request.Header.Set("Accept", "application/json, text/event-stream")
	request.Header.Set("Content-Type", "application/json")
	if c.initialized {
		request.Header.Set("MCP-Protocol-Version", c.protocolVersion)
	}
	if c.sessionID != "" {
		request.Header.Set("Mcp-Session-Id", c.sessionID)
	}
	response, err := c.client.Do(request)
	if err != nil {
		return nil, "", fmt.Errorf("mcp http post: %w", err)
	}
	defer response.Body.Close()
	if !expectsResponse {
		if response.StatusCode != http.StatusAccepted {
			return nil, "", fmt.Errorf("mcp notification returned HTTP %d", response.StatusCode)
		}
		payload, err := readMCPHTTPBody(response.Body)
		if err != nil {
			return nil, "", err
		}
		if len(payload) != 0 {
			return nil, "", fmt.Errorf("mcp notification returned a response body")
		}
		return nil, "", nil
	}
	if response.StatusCode/100 != 2 {
		return nil, "", fmt.Errorf("mcp request returned HTTP %d", response.StatusCode)
	}
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil {
		return nil, "", fmt.Errorf("mcp response content type: %w", err)
	}
	var result json.RawMessage
	switch strings.ToLower(mediaType) {
	case "application/json":
		payload, readErr := readMCPHTTPBody(response.Body)
		if readErr != nil {
			return nil, "", readErr
		}
		result, err = parseMCPHTTPJSONResponse(payload, expectedID)
	case "text/event-stream":
		result, err = parseMCPHTTPSSEResponse(response.Body, expectedID)
	default:
		return nil, "", fmt.Errorf("mcp response content type %q is unsupported", mediaType)
	}
	if err != nil {
		return nil, "", err
	}
	return result, response.Header.Get("Mcp-Session-Id"), nil
}

func readMCPHTTPBody(reader io.Reader) ([]byte, error) {
	payload, err := io.ReadAll(io.LimitReader(reader, maxMCPHTTPBodyBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read mcp response: %w", err)
	}
	if len(payload) > maxMCPHTTPBodyBytes {
		return nil, fmt.Errorf("mcp response exceeded %d bytes", maxMCPHTTPBodyBytes)
	}
	return payload, nil
}

func parseMCPHTTPJSONResponse(payload []byte, expectedID int64) (json.RawMessage, error) {
	return parseMCPHTTPEnvelope(payload, expectedID)
}

func parseMCPHTTPSSEResponse(source io.Reader, expectedID int64) (json.RawMessage, error) {
	reader := bufio.NewReader(&mcpHTTPBoundedReader{reader: source, remaining: maxMCPHTTPBodyBytes})
	var data bytes.Buffer
	for {
		line, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("read mcp sse: %w", err)
		}
		if err == io.EOF {
			return nil, fmt.Errorf("mcp sse stream ended before response %d", expectedID)
		}
		line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
		if line == "" {
			if data.Len() > 0 {
				if len(bytes.TrimSpace(data.Bytes())) == 0 {
					data.Reset()
					continue
				}
				result, done, parseErr := parseMCPHTTPSSEEvent(data.Bytes(), expectedID)
				if parseErr != nil {
					return nil, parseErr
				}
				if done {
					return result, nil
				}
				data.Reset()
			}
		} else if strings.HasPrefix(line, "data:") {
			value := strings.TrimPrefix(line, "data:")
			value = strings.TrimPrefix(value, " ")
			if data.Len()+len(value)+1 > maxMCPHTTPBodyBytes {
				return nil, fmt.Errorf("mcp sse event exceeded %d bytes", maxMCPHTTPBodyBytes)
			}
			data.WriteString(value)
			data.WriteByte('\n')
		}
	}
}

type mcpHTTPBoundedReader struct {
	reader    io.Reader
	remaining int64
}

func (r *mcpHTTPBoundedReader) Read(buffer []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, fmt.Errorf("mcp response exceeded %d bytes", maxMCPHTTPBodyBytes)
	}
	if int64(len(buffer)) > r.remaining {
		buffer = buffer[:r.remaining]
	}
	count, err := r.reader.Read(buffer)
	r.remaining -= int64(count)
	return count, err
}

func parseMCPHTTPSSEEvent(payload []byte, expectedID int64) (json.RawMessage, bool, error) {
	return parseMCPHTTPMessage(payload, expectedID)
}

func parseMCPHTTPEnvelope(payload []byte, expectedID int64) (json.RawMessage, error) {
	result, done, err := parseMCPHTTPMessage(payload, expectedID)
	if err != nil {
		return nil, err
	}
	if !done {
		return nil, fmt.Errorf("mcp response %d was a notification", expectedID)
	}
	return result, nil
}

func parseMCPHTTPMessage(payload []byte, expectedID int64) (json.RawMessage, bool, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		return nil, false, fmt.Errorf("mcp response decode: %w", err)
	}
	var version string
	if err := json.Unmarshal(fields["jsonrpc"], &version); err != nil || version != "2.0" {
		return nil, false, fmt.Errorf("mcp message has invalid jsonrpc version")
	}
	idRaw, hasID := fields["id"]
	methodRaw, hasMethod := fields["method"]
	result, hasResult := fields["result"]
	errorRaw, hasError := fields["error"]
	if hasID {
		if bytes.Equal(bytes.TrimSpace(idRaw), []byte("null")) {
			return nil, false, fmt.Errorf("mcp response id is invalid")
		}
		var id int64
		if err := json.Unmarshal(idRaw, &id); err != nil {
			return nil, false, fmt.Errorf("mcp response id is invalid")
		}
		if hasMethod {
			var method string
			_ = json.Unmarshal(methodRaw, &method)
			return nil, false, fmt.Errorf("mcp server request %q is unsupported", method)
		}
		if id != expectedID {
			return nil, false, fmt.Errorf("mcp response id %d does not match request %d", id, expectedID)
		}
		if hasResult == hasError {
			return nil, false, fmt.Errorf("mcp response %d must contain exactly one result or error", expectedID)
		}
		if hasError {
			var errorFields map[string]json.RawMessage
			if err := json.Unmarshal(errorRaw, &errorFields); err != nil {
				return nil, false, fmt.Errorf("mcp error response is invalid")
			}
			var rpcError mcpRPCError
			code, hasCode := errorFields["code"]
			message, hasMessage := errorFields["message"]
			if !hasCode || !hasMessage || bytes.Equal(bytes.TrimSpace(code), []byte("null")) ||
				bytes.Equal(bytes.TrimSpace(message), []byte("null")) || json.Unmarshal(code, &rpcError.Code) != nil ||
				json.Unmarshal(message, &rpcError.Message) != nil {
				return nil, false, fmt.Errorf("mcp error response is invalid")
			}
			return nil, false, &rpcError
		}
		return result, true, nil
	}
	if !hasMethod || hasResult || hasError {
		return nil, false, fmt.Errorf("mcp message is not a valid notification")
	}
	var method string
	if err := json.Unmarshal(methodRaw, &method); err != nil || method == "" {
		return nil, false, fmt.Errorf("mcp notification method is invalid")
	}
	return nil, false, nil
}

func validMCPInitializeObject(value json.RawMessage) bool {
	if len(value) == 0 {
		return false
	}
	var object map[string]json.RawMessage
	return json.Unmarshal(value, &object) == nil && object != nil
}

func validMCPServerInfo(value json.RawMessage) bool {
	var object struct {
		Name    string `json:"name"`
		Version string `json:"version"`
	}
	return json.Unmarshal(value, &object) == nil && strings.TrimSpace(object.Name) != "" && strings.TrimSpace(object.Version) != ""
}

func validateMCPHTTPSessionID(value string) error {
	if value == "" {
		return nil
	}
	if len(value) > maxMCPHTTPSessionIDBytes {
		return fmt.Errorf("mcp session id exceeded %d bytes", maxMCPHTTPSessionIDBytes)
	}
	for _, runeValue := range value {
		if runeValue < 0x21 || runeValue > 0x7e {
			return fmt.Errorf("mcp session id contains non-visible ASCII")
		}
	}
	return nil
}

func (c *mcpHTTPConnection) cancelRequestLocked(id int64) {
	ctx, cancel := context.WithTimeout(context.Background(), minMCPTimeout(c.timeout(), 2*time.Second))
	defer cancel()
	_ = c.notifyLocked(ctx, "notifications/cancelled", map[string]any{"requestId": id, "reason": "request cancelled"})
}

func (c *mcpHTTPConnection) close() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.closed = true
	activeCancel := c.activeCancel
	c.mu.Unlock()
	if activeCancel != nil {
		activeCancel()
	}
	ctx, cancel := context.WithTimeout(context.Background(), minMCPTimeout(c.timeout(), 2*time.Second))
	defer cancel()
	select {
	case <-c.gate:
	case <-ctx.Done():
		return
	}
	sessionID := c.sessionID
	protocolVersion := c.protocolVersion
	endpoint := c.cfg.Endpoint
	client := c.client
	c.resetLocked()
	c.release()
	if sessionID == "" || client == nil {
		return
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint, nil)
	if err != nil {
		return
	}
	request.Header.Set("Mcp-Session-Id", sessionID)
	request.Header.Set("MCP-Protocol-Version", protocolVersion)
	if response, err := client.Do(request); err == nil {
		_, _ = readMCPHTTPBody(response.Body)
		_ = response.Body.Close()
	}
}

func (c *mcpHTTPConnection) acquire(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.gate:
		return nil
	}
}

func (c *mcpHTTPConnection) release() {
	c.gate <- struct{}{}
}

func (c *mcpHTTPConnection) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func (c *mcpHTTPConnection) beginRequest(ctx context.Context) (context.Context, context.CancelFunc, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, nil, fmt.Errorf("mcp http connection is closed")
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	callCtx, cancel := context.WithTimeout(ctx, c.timeout())
	c.activeCancel = cancel
	return callCtx, cancel, nil
}

func (c *mcpHTTPConnection) endRequest(cancel context.CancelFunc) {
	c.mu.Lock()
	if c.activeCancel != nil {
		c.activeCancel = nil
	}
	c.mu.Unlock()
	cancel()
}

func (c *mcpHTTPConnection) resetLocked() {
	c.initialized = false
	c.sessionID = ""
	c.nextID = 0
	if c.cfg.ProtocolVersion == "" {
		c.protocolVersion = defaultMCPHTTPProtocolVersion
	} else {
		c.protocolVersion = c.cfg.ProtocolVersion
	}
}

func (c *mcpHTTPConnection) timeout() time.Duration {
	if c.cfg.CallTimeout <= 0 {
		return 30 * time.Second
	}
	return c.cfg.CallTimeout
}

func minMCPTimeout(first, second time.Duration) time.Duration {
	if first < second {
		return first
	}
	return second
}

// RegisterMCPStreamableHTTP mounts one remote Streamable HTTP MCP server as a
// progressive-disclosure tool library. It never adds the remote tools directly
// to the model's top-level tool list.
func RegisterMCPStreamableHTTP(ctx context.Context, registry *core.CapabilityRegistry, scope core.ScopePath, cfg MCPStreamableHTTPConfig) (func(), error) {
	if err := validateMCPStreamableHTTPConfig(cfg); err != nil {
		return nil, err
	}
	if cfg.Version == "" {
		cfg.Version = "1.0.0"
	}
	conn := newMCPHTTPConnection(cfg)
	tools, err := listMCPDocuments(ctx, conn)
	if err != nil {
		conn.close()
		return nil, err
	}
	if len(tools) == 0 {
		conn.close()
		return nil, fmt.Errorf("mcp server %s exposed no usable tools", cfg.Namespace)
	}
	gateway := newMCPGateway(conn, tools)
	unmount, err := registry.Mount(core.CapabilityBinding{
		Scope: scope, Mode: core.BindingProvide, Manifest: mcpLibraryManifest(cfg.Namespace, cfg.Version, len(tools)),
		Provider: gateway,
	})
	if err != nil {
		conn.close()
		return nil, err
	}
	return func() {
		unmount()
		conn.close()
	}, nil
}
