package execution

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

func TestMCPStreamableHTTPJSONSSESessionAndClose(t *testing.T) {
	var mu sync.Mutex
	var initialized, listed, called, deleted bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			mu.Lock()
			deleted = r.Header.Get("Mcp-Session-Id") == "session-1" && r.Header.Get("MCP-Protocol-Version") == "2025-11-25"
			mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
			return
		}
		var frame struct {
			ID     *int64          `json:"id"`
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&frame); err != nil {
			t.Fatal(err)
		}
		switch frame.Method {
		case "initialize":
			mu.Lock()
			initialized = frame.ID != nil && r.Header.Get("MCP-Protocol-Version") == ""
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Mcp-Session-Id", "session-1")
			writeMCPHTTPResponse(t, w, *frame.ID, map[string]any{"protocolVersion": "2025-11-25", "capabilities": map[string]any{}, "serverInfo": map[string]any{"name": "test", "version": "1"}})
		case "notifications/initialized":
			if r.Header.Get("Mcp-Session-Id") != "session-1" || r.Header.Get("MCP-Protocol-Version") != "2025-11-25" {
				t.Errorf("initialized headers=%v", r.Header)
			}
			w.WriteHeader(http.StatusAccepted)
		case "tools/list":
			mu.Lock()
			listed = r.Header.Get("Mcp-Session-Id") == "session-1"
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			writeMCPHTTPResponse(t, w, *frame.ID, map[string]any{"tools": []map[string]any{{"name": "weather", "description": "weather"}}})
		case "tools/call":
			mu.Lock()
			called = r.Header.Get("Mcp-Session-Id") == "session-1"
			mu.Unlock()
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("id: primed\ndata:\n\n"))
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
			_, _ = w.Write([]byte("data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\"}\n\n"))
			result, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": *frame.ID, "result": map[string]any{"content": []map[string]any{{"type": "text", "text": "sunny"}}}})
			_, _ = w.Write(append([]byte("data: "), append(result, []byte("\n\n")...)...))
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
			<-r.Context().Done()
		default:
			t.Fatalf("unexpected method %q", frame.Method)
		}
	}))
	defer server.Close()

	conn := newMCPHTTPConnection(MCPStreamableHTTPConfig{Endpoint: server.URL, Namespace: "mcp.weather", Version: "1", Client: server.Client(), AllowPrivateNetwork: true})
	tools, err := listMCPDocuments(context.Background(), conn)
	if err != nil || len(tools) != 1 {
		t.Fatalf("list tools=%#v err=%v", tools, err)
	}
	result, err := newMCPGateway(conn, tools).callTool(context.Background(), core.CapabilityRequest{Args: map[string]any{"name": "weather"}})
	if err != nil || !result.OK || result.Content != "sunny" {
		t.Fatalf("tool result=%#v err=%v", result, err)
	}
	conn.close()
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeTenant, ID: "tenant-test"})
	registry := core.NewCapabilityRegistry()
	unmount, err := RegisterMCPStreamableHTTP(context.Background(), registry, scope, MCPStreamableHTTPConfig{
		Endpoint: server.URL, Namespace: "mcp.weather", Version: "1", Client: server.Client(), AllowPrivateNetwork: true,
	})
	if err != nil {
		t.Fatalf("register streamable HTTP MCP: %v", err)
	}
	entries, err := registry.Entries(scope)
	if err != nil || len(entries) != 1 || entries[0].Manifest.ID != "mcp.weather" {
		t.Fatalf("MCP HTTP registry entries=%#v err=%v", entries, err)
	}
	unmount()
	mu.Lock()
	defer mu.Unlock()
	if !initialized || !listed || !called || !deleted {
		t.Fatalf("lifecycle initialized=%v listed=%v called=%v deleted=%v", initialized, listed, called, deleted)
	}
}

func TestMCPStreamableHTTPCancelDoesNotReuseSession(t *testing.T) {
	started := make(chan struct{}, 1)
	cancelled := make(chan struct{}, 1)
	var mu sync.Mutex
	initializations := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var frame struct {
			ID     *int64 `json:"id"`
			Method string `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&frame)
		switch frame.Method {
		case "initialize":
			mu.Lock()
			initializations++
			session := "session-" + strconv.Itoa(initializations)
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Mcp-Session-Id", session)
			writeMCPHTTPResponse(t, w, *frame.ID, map[string]any{"protocolVersion": "2025-11-25", "capabilities": map[string]any{}, "serverInfo": map[string]any{"name": "test", "version": "1"}})
		case "notifications/initialized":
			w.WriteHeader(http.StatusAccepted)
		case "tools/list":
			w.Header().Set("Content-Type", "application/json")
			writeMCPHTTPResponse(t, w, *frame.ID, map[string]any{"tools": []map[string]any{{"name": "weather"}}})
		case "tools/call":
			started <- struct{}{}
			<-r.Context().Done()
		case "notifications/cancelled":
			cancelled <- struct{}{}
			w.WriteHeader(http.StatusAccepted)
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()
	conn := newMCPHTTPConnection(MCPStreamableHTTPConfig{Endpoint: server.URL, Namespace: "mcp.weather", Client: server.Client(), AllowPrivateNetwork: true, CallTimeout: time.Second})
	if _, err := conn.call(context.Background(), "tools/list", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := conn.call(ctx, "tools/call", map[string]any{"name": "weather"}); done <- err }()
	<-started
	queuedCtx, queuedCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer queuedCancel()
	if _, err := conn.call(queuedCtx, "tools/list", map[string]any{}); err == nil {
		t.Fatal("queued call ignored its cancelled context")
	}
	cancel()
	if err := <-done; err == nil {
		t.Fatal("cancelled tools/call succeeded")
	}
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("cancellation notification was not sent")
	}
	if _, err := conn.call(context.Background(), "tools/list", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if initializations != 2 {
		t.Fatalf("initializations=%d, want fresh session after cancellation", initializations)
	}
}

func TestMCPStreamableHTTPRejectsMalformedInitializeAndServerRequest(t *testing.T) {
	for _, initializeResult := range []map[string]any{
		{"capabilities": map[string]any{}, "serverInfo": map[string]any{"name": "test", "version": "1"}},
		{"protocolVersion": "2999-01-01", "capabilities": map[string]any{}, "serverInfo": map[string]any{"name": "test", "version": "1"}},
		{"protocolVersion": "2025-11-25", "capabilities": []any{}, "serverInfo": map[string]any{"name": "test", "version": "1"}},
		{"protocolVersion": "2025-11-25", "capabilities": map[string]any{}, "serverInfo": map[string]any{"name": "test"}},
	} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var frame struct {
				ID *int64 `json:"id"`
			}
			_ = json.NewDecoder(r.Body).Decode(&frame)
			w.Header().Set("Content-Type", "application/json")
			writeMCPHTTPResponse(t, w, *frame.ID, initializeResult)
		}))
		conn := newMCPHTTPConnection(MCPStreamableHTTPConfig{Endpoint: server.URL, Namespace: "mcp.weather", Client: server.Client(), AllowPrivateNetwork: true})
		if _, err := conn.call(context.Background(), "tools/list", map[string]any{}); err == nil {
			server.Close()
			t.Fatalf("malformed initialize accepted: %#v", initializeResult)
		}
		server.Close()
	}
	if _, _, err := parseMCPHTTPSSEEvent([]byte(`{"jsonrpc":"2.0","id":9,"method":"sampling/createMessage"}`), 1); err == nil {
		t.Fatal("server request was accepted")
	}
}

func TestMCPStreamableHTTPDoesNotReplayAfterSessionExpiry(t *testing.T) {
	var mu sync.Mutex
	initializations, calls := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var frame struct {
			ID     *int64 `json:"id"`
			Method string `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&frame)
		switch frame.Method {
		case "initialize":
			mu.Lock()
			initializations++
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Mcp-Session-Id", "session")
			writeMCPHTTPResponse(t, w, *frame.ID, map[string]any{"protocolVersion": "2025-11-25", "capabilities": map[string]any{}, "serverInfo": map[string]any{"name": "test", "version": "1"}})
		case "notifications/initialized":
			w.WriteHeader(http.StatusAccepted)
		case "tools/list":
			w.Header().Set("Content-Type", "application/json")
			writeMCPHTTPResponse(t, w, *frame.ID, map[string]any{"tools": []any{}})
		case "tools/call":
			mu.Lock()
			calls++
			mu.Unlock()
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	conn := newMCPHTTPConnection(MCPStreamableHTTPConfig{Endpoint: server.URL, Namespace: "mcp.weather", Client: server.Client(), AllowPrivateNetwork: true})
	if _, err := conn.call(context.Background(), "tools/list", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.call(context.Background(), "tools/call", map[string]any{"name": "weather"}); err == nil {
		t.Fatal("expired tools/call succeeded")
	}
	mu.Lock()
	gotInitializations, gotCalls := initializations, calls
	mu.Unlock()
	if gotInitializations != 1 || gotCalls != 1 {
		t.Fatalf("request replayed: init=%d calls=%d", gotInitializations, gotCalls)
	}
	if _, err := conn.call(context.Background(), "tools/list", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if initializations != 2 || calls != 1 {
		t.Fatalf("explicit follow-up did not only reinitialize: init=%d calls=%d", initializations, calls)
	}
}

func TestMCPStreamableHTTPCloseCancelsInflightRequest(t *testing.T) {
	started := make(chan struct{}, 1)
	deleted := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			deleted <- struct{}{}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		var frame struct {
			ID     *int64 `json:"id"`
			Method string `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&frame)
		switch frame.Method {
		case "initialize":
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Mcp-Session-Id", "session")
			writeMCPHTTPResponse(t, w, *frame.ID, map[string]any{"protocolVersion": "2025-11-25", "capabilities": map[string]any{}, "serverInfo": map[string]any{"name": "test", "version": "1"}})
		case "notifications/initialized":
			w.WriteHeader(http.StatusAccepted)
		case "tools/list":
			w.Header().Set("Content-Type", "application/json")
			writeMCPHTTPResponse(t, w, *frame.ID, map[string]any{"tools": []any{}})
		case "tools/call":
			started <- struct{}{}
			<-r.Context().Done()
		}
	}))
	defer server.Close()
	conn := newMCPHTTPConnection(MCPStreamableHTTPConfig{Endpoint: server.URL, Namespace: "mcp.weather", Client: server.Client(), AllowPrivateNetwork: true, CallTimeout: time.Second})
	if _, err := conn.call(context.Background(), "tools/list", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := conn.call(context.Background(), "tools/call", map[string]any{"name": "weather"})
		done <- err
	}()
	<-started
	conn.close()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Close left tools/call successful")
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel inflight request")
	}
	select {
	case <-deleted:
	case <-time.After(time.Second):
		t.Fatal("Close did not attempt session DELETE")
	}
	if _, err := conn.call(context.Background(), "tools/list", map[string]any{}); err == nil {
		t.Fatal("closed connection accepted a later call")
	}
}

func TestMCPStreamableHTTPConfigurationRedirectAndSSEBounds(t *testing.T) {
	local := httptest.NewServer(http.NotFoundHandler())
	defer local.Close()
	if err := validateMCPStreamableHTTPConfig(MCPStreamableHTTPConfig{Endpoint: local.URL, Namespace: "mcp.weather"}); err == nil {
		t.Fatal("default loopback endpoint was accepted")
	}
	if err := validateMCPStreamableHTTPConfig(MCPStreamableHTTPConfig{Endpoint: local.URL, Namespace: "mcp.weather", Client: local.Client()}); err == nil {
		t.Fatal("custom client without explicit opt-in was accepted")
	}
	redirectTarget := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Fatal("redirect was followed") }))
	defer redirectTarget.Close()
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, redirectTarget.URL, http.StatusFound)
	}))
	defer redirector.Close()
	conn := newMCPHTTPConnection(MCPStreamableHTTPConfig{Endpoint: redirector.URL, Namespace: "mcp.weather", Client: redirector.Client(), AllowPrivateNetwork: true})
	if _, err := conn.call(context.Background(), "tools/list", map[string]any{}); err == nil {
		t.Fatal("redirect was accepted")
	}
	base := `data: {"jsonrpc":"2.0","id":1,"result":{"text":"` + `"}}` + "\n\n"
	exact := `data: {"jsonrpc":"2.0","id":1,"result":{"text":"` + strings.Repeat("x", maxMCPHTTPBodyBytes-len(base)) + `"}}` + "\n\n"
	if _, err := parseMCPHTTPSSEResponse(strings.NewReader(exact), 1); err != nil {
		t.Fatalf("exact limit rejected: %v", err)
	}
	over := `data: {"jsonrpc":"2.0","id":1,"result":{"text":"` + strings.Repeat("x", maxMCPHTTPBodyBytes-len(base)+1) + `"}}` + "\n\n"
	if _, err := parseMCPHTTPSSEResponse(strings.NewReader(over), 1); err == nil {
		t.Fatal("limit+1 accepted")
	}
}

func TestMCPStreamableHTTPSSERequiresTerminatingBlankLine(t *testing.T) {
	frame := "data: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}\n"
	for _, payload := range []string{"", frame, frame + "\n"} {
		_, err := parseMCPHTTPSSEResponse(strings.NewReader(payload), 1)
		if payload == frame+"\n" {
			if err != nil {
				t.Fatalf("double-LF SSE frame rejected: %v", err)
			}
		} else if err == nil {
			t.Fatalf("unterminated SSE frame accepted: %q", payload)
		}
	}
}

func writeMCPHTTPResponse(t *testing.T, w http.ResponseWriter, id int64, result any) {
	t.Helper()
	if err := json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": id, "result": result}); err != nil {
		t.Fatal(err)
	}
}
