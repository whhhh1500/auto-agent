package execution

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// MCPServerConfig describes one MCP (Model Context Protocol) server process
// speaking JSON-RPC over stdio. One server contributes one tool-library
// capability at Namespace; remote tools stay in that library and are
// retrieved on demand. The connection stays warm across calls and is
// transparently restarted after a timeout or crash.
type MCPServerConfig struct {
	// Command launches the server, e.g. {"npx", "-y", "@modelcontextprotocol/server-everything"}.
	Command []string
	// Env adds environment variables for the server process.
	Env []string
	// InheritEnvironment opts into passing every parent-process environment
	// variable. It is unsafe by default because the server may hold unrelated
	// model, storage or deployment credentials.
	InheritEnvironment bool
	// Namespace is the single mounted capability id and must contain a
	// dot, e.g. "mcp.weather". Remote tools remain names inside that library.
	Namespace string
	// Version stamps the contributed manifests.
	Version string
	// ProtocolVersion defaults to "2025-03-26".
	ProtocolVersion string
	// ClientName defaults to "harness-core".
	ClientName string
	// CallTimeout bounds one JSON-RPC round trip; default 30s.
	CallTimeout time.Duration
	// Observer records search/explore/choose traces for later ranking work.
	// Optional; observation failures never fail the tool call.
	Observer ToolLibraryObserver
}

const maxMCPResponseLineBytes = 4 << 20

// mcpConnection owns one server process. All traffic is serialized under mu:
// MCP requests are matched to responses by id, notifications are skipped.
type mcpConnection struct {
	cfg MCPServerConfig

	mu          sync.Mutex
	cmd         *exec.Cmd
	stdin       *bufio.Writer
	stdout      *bufio.Reader
	nextID      int64
	alive       bool
	initialized bool
}

// mcpTransport is the private boundary between the MCP tool library and one
// JSON-RPC transport. The stdio transport remains the existing default; HTTP
// uses the same catalog and gateway without changing their capability contract.
type mcpTransport interface {
	call(context.Context, string, any) (json.RawMessage, error)
	close()
	identity() mcpTransportIdentity
}

type mcpTransportIdentity struct {
	namespace string
	version   string
	observer  ToolLibraryObserver
}

func (c *mcpConnection) identity() mcpTransportIdentity {
	if c == nil {
		return mcpTransportIdentity{}
	}
	return mcpTransportIdentity{namespace: c.cfg.Namespace, version: c.cfg.Version, observer: c.cfg.Observer}
}

func (c *mcpConnection) close() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.killLocked()
}

type mcpRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *mcpRPCError) Error() string {
	return fmt.Sprintf("mcp rpc error %d: %s", e.Code, e.Message)
}

func validateMCPConfig(cfg MCPServerConfig) error {
	if err := core.ValidateNamespacedID(cfg.Namespace); err != nil {
		return fmt.Errorf("mcp namespace %q must be a namespaced identifier", cfg.Namespace)
	}
	if len(cfg.Command) == 0 || strings.TrimSpace(cfg.Command[0]) == "" {
		return fmt.Errorf("mcp server command is empty")
	}
	if len(cfg.Command) > maxExecArgs {
		return fmt.Errorf("mcp server command exceeds %d entries", maxExecArgs)
	}
	for _, part := range cfg.Command {
		if strings.ContainsRune(part, '\x00') {
			return fmt.Errorf("mcp server command contains NUL")
		}
		if len(part) > maxExecArgBytes {
			return fmt.Errorf("mcp server command argument exceeds %d bytes", maxExecArgBytes)
		}
	}
	if len(cfg.Env) > maxExecArgs {
		return fmt.Errorf("mcp server environment exceeds %d entries", maxExecArgs)
	}
	for _, env := range cfg.Env {
		if strings.ContainsRune(env, '\x00') {
			return fmt.Errorf("mcp server environment contains NUL")
		}
		name, _, ok := strings.Cut(env, "=")
		if !ok || name == "" || strings.ContainsAny(name, " \t\r\n") {
			return fmt.Errorf("mcp server environment entry is invalid")
		}
		if len(env) > maxExecArgBytes {
			return fmt.Errorf("mcp server environment entry exceeds %d bytes", maxExecArgBytes)
		}
	}
	return nil
}

func (c *mcpConnection) startLocked() error {
	if err := validateMCPConfig(c.cfg); err != nil {
		return err
	}
	cmd := exec.Command(c.cfg.Command[0], c.cfg.Command[1:]...)
	cmd.Env = mcpEnvironment(c.cfg)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	cmd.Stderr = nil // servers log on stderr; leaving it detached avoids blocking
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start mcp server: %w", err)
	}
	c.cmd = cmd
	c.stdin = bufio.NewWriter(stdin)
	c.stdout = bufio.NewReader(stdout)
	c.alive = true
	c.initialized = false
	// Reap the process when it exits so no zombie remains.
	go func() { _ = cmd.Wait() }()
	return nil
}

func mcpEnvironment(cfg MCPServerConfig) []string {
	base := []string{}
	if cfg.InheritEnvironment {
		base = os.Environ()
	} else {
		for _, name := range []string{"PATH", "SystemRoot", "WINDIR", "HOME", "USERPROFILE", "TMP", "TEMP", "LANG"} {
			if value, ok := os.LookupEnv(name); ok {
				base = append(base, name+"="+value)
			}
		}
	}
	return append(base, cfg.Env...)
}

func (c *mcpConnection) killLocked() {
	if c.cmd != nil && c.cmd.Process != nil {
		_ = c.cmd.Process.Kill()
	}
	c.alive = false
	c.initialized = false
}

// call performs one JSON-RPC round trip. A timeout or dead process recycles
// the connection; the next call starts a fresh server.
func (c *mcpConnection) call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.callLocked(ctx, method, params)
}

func (c *mcpConnection) callLocked(ctx context.Context, method string, params any) (json.RawMessage, error) {
	if !c.alive {
		if err := c.startLocked(); err != nil {
			return nil, err
		}
	}
	if !c.initialized && method != "initialize" {
		protocol := c.cfg.ProtocolVersion
		if protocol == "" {
			protocol = "2025-03-26"
		}
		name := c.cfg.ClientName
		if name == "" {
			name = "harness-core"
		}
		// A recursive call with method "initialize" skips the init block and
		// performs exactly one request/response round trip.
		if _, err := c.callLocked(ctx, "initialize", map[string]any{
			"protocolVersion": protocol,
			"capabilities":    map[string]any{},
			"clientInfo":      map[string]any{"name": name, "version": "1"},
		}); err != nil {
			c.killLocked()
			return nil, err
		}
		if err := c.notifyLocked(ctx, "notifications/initialized", nil); err != nil {
			c.killLocked()
			return nil, err
		}
		c.initialized = true
	}

	c.nextID++
	id := c.nextID
	frame := map[string]any{"jsonrpc": "2.0", "id": id, "method": method}
	if params != nil {
		frame["params"] = params
	}
	encoded, err := json.Marshal(frame)
	if err != nil {
		return nil, err
	}
	if _, err := c.stdin.Write(append(encoded, '\n')); err != nil {
		c.killLocked()
		return nil, fmt.Errorf("mcp write: %w", err)
	}
	if err := c.stdin.Flush(); err != nil {
		c.killLocked()
		return nil, fmt.Errorf("mcp write: %w", err)
	}

	type readResult struct {
		response json.RawMessage
		err      error
	}
	timeout := c.cfg.CallTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	resultCh := make(chan readResult, 1)
	go func() {
		response, err := c.readResponseLocked(id)
		resultCh <- readResult{response, err}
	}()
	select {
	case result := <-resultCh:
		if result.err != nil {
			c.killLocked()
			return nil, result.err
		}
		return result.response, nil
	case <-ctx.Done():
		c.killLocked() // unblocks the reader goroutine
		return nil, ctx.Err()
	case <-time.After(timeout):
		c.killLocked()
		return nil, fmt.Errorf("mcp %s timed out after %s", method, timeout)
	}
}

// readResponseLocked reads lines until the response with the expected id
// arrives, skipping notifications. Called only from the per-call reader
// goroutine while mu is held.
func (c *mcpConnection) readResponseLocked(id int64) (json.RawMessage, error) {
	for {
		line, err := readMCPLine(c.stdout)
		if err != nil {
			return nil, fmt.Errorf("mcp read: %w", err)
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var envelope struct {
			ID     *int64          `json:"id"`
			Result json.RawMessage `json:"result"`
			Error  *mcpRPCError    `json:"error"`
		}
		if err := json.Unmarshal([]byte(line), &envelope); err != nil {
			continue // tolerate keepalives or garbage lines
		}
		if envelope.ID == nil || *envelope.ID != id {
			continue // notification or stale response
		}
		if envelope.Error != nil {
			return nil, envelope.Error
		}
		return envelope.Result, nil
	}
}

func readMCPLine(reader *bufio.Reader) (string, error) {
	var line bytes.Buffer
	for {
		fragment, prefix, err := reader.ReadLine()
		if line.Len()+len(fragment) > maxMCPResponseLineBytes {
			return "", fmt.Errorf("mcp response line exceeded %d bytes", maxMCPResponseLineBytes)
		}
		line.Write(fragment)
		if err != nil {
			return "", err
		}
		if !prefix {
			return line.String(), nil
		}
	}
}

func (c *mcpConnection) notifyLocked(ctx context.Context, method string, params any) error {
	frame := map[string]any{"jsonrpc": "2.0", "method": method}
	if params != nil {
		frame["params"] = params
	}
	encoded, err := json.Marshal(frame)
	if err != nil {
		return err
	}
	if _, err := c.stdin.Write(append(encoded, '\n')); err != nil {
		return err
	}
	return c.stdin.Flush()
}

type mcpContentBlock struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

func mcpTextContent(blocks []mcpContentBlock, limit int) (string, error) {
	if limit <= 0 {
		limit = core.DefaultMaxCapabilityOutputBytes
	}
	var text bytes.Buffer
	for _, block := range blocks {
		if block.Type != "text" {
			continue
		}
		if text.Len()+len(block.Text) > limit {
			return "", fmt.Errorf("mcp result exceeded %d bytes", limit)
		}
		text.WriteString(block.Text)
	}
	return text.String(), nil
}

type mcpToolDescription struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
	Triggers    []string       `json:"triggers,omitempty"`
	NotFor      []string       `json:"notFor,omitempty"`
}

func mcpCapabilityID(namespace, toolName string) (string, error) {
	if err := core.ValidateNamespacedID(namespace); err != nil {
		return "", fmt.Errorf("mcp namespace %q must be a namespaced identifier", namespace)
	}
	if strings.TrimSpace(toolName) == "" {
		return "", fmt.Errorf("mcp tool name is empty")
	}
	sanitized := strings.NewReplacer(" ", "_", "-", "_", "/", "_", "\\", "_").Replace(toolName)
	id := namespace + "." + sanitized
	if err := core.ValidateNamespacedID(id); err != nil {
		return "", err
	}
	return id, nil
}

// RegisterMCPServer connects to an MCP server, lists its tools into a
// tool-library knowledge base, and mounts one progressive-disclosure
// capability at Namespace. The returned function unmounts it and terminates
// the server process.
func RegisterMCPServer(ctx context.Context, registry *core.CapabilityRegistry, scope core.ScopePath, cfg MCPServerConfig) (func(), error) {
	if err := validateMCPConfig(cfg); err != nil {
		return nil, err
	}
	version := cfg.Version
	if version == "" {
		version = "1.0.0"
	}
	conn := &mcpConnection{cfg: cfg}
	conn.mu.Lock()
	err := conn.startLocked()
	conn.mu.Unlock()
	if err != nil {
		return nil, err
	}
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
		Scope: scope, Mode: core.BindingProvide, Manifest: mcpLibraryManifest(cfg.Namespace, version, len(tools)),
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

func listMCPDocuments(ctx context.Context, conn mcpTransport) ([]toolDocument, error) {
	if conn == nil {
		return nil, fmt.Errorf("mcp connection is nil")
	}
	result, err := conn.call(ctx, "tools/list", map[string]any{})
	if err != nil {
		return nil, err
	}
	var listing struct {
		Tools []mcpToolDescription `json:"tools"`
	}
	if err := json.Unmarshal(result, &listing); err != nil {
		return nil, fmt.Errorf("mcp tools/list decode: %w", err)
	}
	return indexMCPTools(listing.Tools), nil
}
