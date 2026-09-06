package integration_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cc-auto-agent/harness-core/pkg/control"
	. "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/execution"
	"github.com/cc-auto-agent/harness-core/pkg/extensions/subagent"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func testScopes() (ScopePath, ScopePath, ScopePath, ScopePath) {
	global := MustScopePath(ScopeRef{Kind: ScopeGlobal, ID: "global"})
	product, _ := global.Child(ScopeRef{Kind: ScopeProduct, ID: "product"})
	tenant, _ := product.Child(ScopeRef{Kind: ScopeTenant, ID: "tenant-a"})
	user, _ := tenant.Child(ScopeRef{Kind: ScopeUser, ID: "user-a"})
	return global, product, tenant, user
}

func testPrincipal(user ScopePath) Principal {
	return Principal{SubjectID: "user-a", TenantID: "tenant-a", Scope: user,
		Grants: NewPermissionSet(PermRead, PermWrite)}
}

type staticTool struct {
	manifest CapabilityManifest
	content  string
}

func (t staticTool) Manifest() CapabilityManifest { return t.manifest }
func (t staticTool) Execute(context.Context, CapabilityRequest) (CapabilityResult, error) {
	return CapabilityResult{Content: t.content, OK: true}, nil
}

func toolManifest(id, version string) CapabilityManifest {
	return CapabilityManifest{ID: id, Version: version, Name: id, Kind: KindTool,
		Contract: "harness.tool/v1", RequiredPermissions: []Permission{PermRead}, Tool: &ToolExposure{}}
}

// --- HTTP provider ---

func TestHTTPExecutorPostsArgsAndInjectsCredential(t *testing.T) {
	type captured struct {
		auth string
		body map[string]any
	}
	captures := make(chan captured, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		captures <- captured{auth: r.Header.Get("X-Api-Key"), body: body}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"price": 101234.5}`)
	}))
	defer server.Close()

	principal := testPrincipal(mustUserScope(t))
	capabilities := NewCapabilityRegistry()
	credentials := NewCredentialRegistry()
	if err := credentials.Bind(CredentialBinding{
		Scope: principal.Scope, Ref: "remote.api_key",
		Provider: StaticCredentialProvider{Value: "sekret", Source: "test"},
	}); err != nil {
		t.Fatal(err)
	}
	manifest := CapabilityManifest{
		ID: "remote.quote", Version: "1.0.0", Name: "Remote quote", Kind: KindConnector,
		RequiredCredentials: []CredentialRef{"remote.api_key"},
		Execution: &ExecutionSpec{
			Runtime: "http", Entrypoint: server.URL, Method: http.MethodPost,
			Headers: map[string]string{"X-Api-Key": "$credential:remote.api_key"},
		},
	}
	if err := capabilities.RegisterExecutor(principal.Scope, manifest, execution.HTTPExecutor{
		Client: server.Client(), AllowPrivateNetwork: true,
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := (CapabilityResolver{Registry: capabilities, Credentials: credentials}).Resolve(principal, principal.Scope)
	if err != nil {
		t.Fatal(err)
	}
	result, err := snapshot.Execute(context.Background(), ToolCall{
		ID: "c1", Name: "remote.quote", Args: map[string]any{"symbol": "BTC"},
	})
	if err != nil || !result.OK {
		t.Fatalf("remote call failed: %#v %v", result, err)
	}
	if !strings.Contains(result.Content, "101234.5") {
		t.Fatalf("unexpected response body: %q", result.Content)
	}
	received := <-captures
	if received.auth != "sekret" {
		t.Fatalf("credential was not injected: %q", received.auth)
	}
	if received.body["symbol"] != "BTC" {
		t.Fatalf("args not delivered as JSON body: %#v", received.body)
	}
}

func mustUserScope(t *testing.T) ScopePath {
	t.Helper()
	_, _, _, user := testScopes()
	return user
}

func TestHTTPExecutorDeniesWithoutCredential(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("endpoint must not be reached without credentials")
	}))
	defer server.Close()

	principal := testPrincipal(mustUserScope(t))
	registry := NewCapabilityRegistry()
	manifest := CapabilityManifest{
		ID: "remote.secure", Version: "1.0.0", Name: "Secure", Kind: KindConnector,
		RequiredCredentials: []CredentialRef{"remote.api_key"},
		Execution: &ExecutionSpec{
			Runtime: "http", Entrypoint: server.URL,
			Headers: map[string]string{"X-Api-Key": "$credential:remote.api_key"},
		},
	}
	if err := registry.RegisterExecutor(principal.Scope, manifest, execution.HTTPExecutor{
		Client: server.Client(), AllowPrivateNetwork: true,
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := (CapabilityResolver{Registry: registry}).Resolve(principal, principal.Scope)
	if err != nil {
		t.Fatal(err)
	}
	result, _ := snapshot.Execute(context.Background(), ToolCall{ID: "c1", Name: "remote.secure"})
	if result.OK || result.Metadata["code"] != "credential_unavailable" {
		t.Fatalf("missing credential must deny the call: %#v", result)
	}
}

// --- MCP provider: a real child process speaking the protocol ---

func TestMain(m *testing.M) {
	if os.Getenv("HARNESS_MCP_CHILD") == "1" {
		runFakeMCPServer()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func runFakeMCPServer() {
	reader := bufio.NewReader(os.Stdin)
	writer := bufio.NewWriter(os.Stdout)
	initialized := false
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		var envelope struct {
			ID     *int64          `json:"id"`
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
		}
		if json.Unmarshal([]byte(line), &envelope) != nil {
			continue
		}
		if envelope.ID == nil {
			continue // notification
		}
		var result any
		switch envelope.Method {
		case "initialize":
			initialized = true
			result = map[string]any{"protocolVersion": "2025-03-26", "capabilities": map[string]any{}}
		case "tools/list":
			if !initialized {
				writeJSONLine(writer, map[string]any{"jsonrpc": "2.0", "id": *envelope.ID,
					"error": map[string]any{"code": -32000, "message": "not initialized"}})
				continue
			}
			result = map[string]any{"tools": []map[string]any{
				{
					"name": "get-forecast", "description": "Weather forecast for a city",
					"inputSchema": map[string]any{
						"type":       "object",
						"properties": map[string]any{"city": map[string]any{"type": "string"}},
						"required":   []any{"city"},
					},
				},
			}}
		case "tools/call":
			var params struct {
				Name      string         `json:"name"`
				Arguments map[string]any `json:"arguments"`
			}
			_ = json.Unmarshal(envelope.Params, &params)
			city, _ := params.Arguments["city"].(string)
			result = map[string]any{
				"content": []map[string]any{{"type": "text", "text": "forecast for " + city + ": sunny"}},
			}
		default:
			writeJSONLine(writer, map[string]any{"jsonrpc": "2.0", "id": *envelope.ID,
				"error": map[string]any{"code": -32601, "message": "unknown method"}})
			continue
		}
		writeJSONLine(writer, map[string]any{"jsonrpc": "2.0", "id": *envelope.ID, "result": result})
	}
}

func writeJSONLine(writer *bufio.Writer, value any) {
	encoded, _ := json.Marshal(value)
	writer.Write(encoded)
	writer.WriteByte('\n')
	_ = writer.Flush()
}

func TestRegisterMCPServerMountsToolsAndCalls(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	registry := NewCapabilityRegistry()

	unmount, err := execution.RegisterMCPServer(context.Background(), registry, user, execution.MCPServerConfig{
		Command:   []string{executable},
		Env:       []string{"HARNESS_MCP_CHILD=1"},
		Namespace: "mcp.weather",
		Version:   "1.0.0",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer unmount()

	entries, err := registry.Entries(user)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Manifest.ID != "mcp.weather" {
		t.Fatalf("unexpected MCP contributions: %#v", entries)
	}
	if entries[0].Manifest.Contract != execution.ContractToolLibraryV1 || entries[0].Manifest.Kind != KindKnowledge {
		t.Fatalf("MCP library manifest wrong: %#v", entries[0].Manifest)
	}

	snapshot, err := (CapabilityResolver{Registry: registry}).Resolve(principal, user)
	if err != nil {
		t.Fatal(err)
	}
	result, err := snapshot.Execute(context.Background(), ToolCall{
		ID: "c1", Name: "mcp.weather", Args: map[string]any{
			"action": "call", "name": "get-forecast",
			"arguments": map[string]any{"city": "Hangzhou"},
		},
	})
	if err != nil || !result.OK {
		t.Fatalf("mcp tool call failed: %#v %v", result, err)
	}
	if !strings.Contains(result.Content, "forecast for Hangzhou") {
		t.Fatalf("unexpected tool content: %q", result.Content)
	}

	// Unmount removes the contributions and terminates the child.
	unmount()
	entries, err = registry.Entries(user)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("unmount left contributions behind: %#v", entries)
	}
}

func TestSubagentDelegatesAndGuardsDepth(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)

	// Child capability answers instantly; parent delegates to the child.
	capabilities := NewCapabilityRegistry()
	if err := capabilities.Register(product, staticTool{manifest: toolManifest("research.search", "1.0.0"), content: "sources found"}); err != nil {
		t.Fatal(err)
	}
	profiles := NewAgentProfileRegistry()
	name := "Researcher"
	model := ModelSelection{Provider: "mock", Model: "mock-1"}
	if err := profiles.Bind(AgentProfileLayer{
		Scope: product, ProfileID: "product.researcher", Name: &name, Model: &model,
		AddCapabilities: []string{"research.search"},
	}); err != nil {
		t.Fatal(err)
	}

	runtime := &Runtime{
		Capabilities: capabilities, Profiles: profiles,
		Models: ModelResolverFunc(func(context.Context, ModelSelection) (LlmAdapter, error) {
			return MockLlmAdapter{}, nil
		}),
	}
	subagentCapability, err := subagent.NewCapability(runtime, "agent.researcher", subagent.Options{
		ProfileID: "product.researcher",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := capabilities.Register(product, subagentCapability); err != nil {
		t.Fatal(err)
	}
	if err := profiles.Bind(AgentProfileLayer{
		Scope: product, ProfileID: "product.orchestrator", Name: &name, Model: &model,
		AddCapabilities: []string{"agent.researcher"},
	}); err != nil {
		t.Fatal(err)
	}

	// Direct snapshot execution of the delegation.
	snapshot, err := (CapabilityResolver{Registry: capabilities}).Resolve(principal, user)
	if err != nil {
		t.Fatal(err)
	}
	result, err := snapshot.Execute(context.Background(), ToolCall{
		ID: "c1", Name: "agent.researcher", Args: map[string]any{"prompt": "find sources"},
	})
	if err != nil || !result.OK {
		t.Fatalf("delegation failed: %#v %v", result, err)
	}
	// The mock child answered with its tool result, proving the child ran.
	if !strings.Contains(result.Content, "sources found") {
		t.Fatalf("child agent did not run its own capability: %q", result.Content)
	}
	if result.Metadata["depth"] != 1 {
		t.Fatalf("depth not advanced: %#v", result.Metadata)
	}

	// A principal already deep in a delegation chain is refused before any
	// child runs; the guard reads the depth attribute on the frozen principal.
	deepPrincipal := principal
	deepPrincipal.Grants = principal.Grants.Clone()
	deepPrincipal.Attributes = map[string]string{}
	deepPrincipal.Attributes[subagent.AttributeDepth] = "3"
	deepSnapshot, err := (CapabilityResolver{Registry: capabilities}).Resolve(deepPrincipal, user)
	if err != nil {
		t.Fatal(err)
	}
	deepResult, _ := deepSnapshot.Execute(context.Background(), ToolCall{
		ID: "c2", Name: "agent.researcher", Args: map[string]any{"prompt": "again"},
	})
	if deepResult.OK || deepResult.Metadata["code"] != "agent_depth_exceeded" {
		t.Fatalf("depth guard did not refuse: %#v", deepResult)
	}

	// Missing prompt is an argument refusal.
	empty, _ := snapshot.Execute(context.Background(), ToolCall{ID: "c3", Name: "agent.researcher"})
	if empty.OK || empty.Metadata["code"] != CodeInvalidArgs {
		t.Fatalf("missing prompt must be denied: %#v", empty)
	}
}

func TestReleaseManagerPublishVersionRollback(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	profiles := NewAgentProfileRegistry()
	manager, err := control.NewReleaseManager(profiles)
	if err != nil {
		t.Fatal(err)
	}

	name := "Agent v1"
	model := ModelSelection{Provider: "mock", Model: "mock-1"}
	release1, err := manager.Publish(context.Background(), product, AgentProfileLayer{
		ProfileID: "product.agent", Name: &name, Model: &model,
	})
	if err != nil {
		t.Fatal(err)
	}
	if release1.Version != 1 {
		t.Fatalf("first release must be v1: %#v", release1)
	}
	name2 := "Agent v2"
	release2, err := manager.Publish(context.Background(), product, AgentProfileLayer{
		ProfileID: "product.agent", Name: &name2,
	})
	if err != nil || release2.Version != 2 {
		t.Fatalf("second release wrong: %#v %v", release2, err)
	}

	snapshot, err := profiles.Resolve(principal, user, "product.agent")
	if err != nil || snapshot.Name != "Agent v2" {
		t.Fatalf("v2 not live: %#v %v", snapshot, err)
	}

	rolledBack, err := manager.Rollback(context.Background(), "product.agent", 1)
	if err != nil || len(rolledBack) != 1 || rolledBack[0].Version != 2 {
		t.Fatalf("rollback wrong: %#v %v", rolledBack, err)
	}
	snapshot, err = profiles.Resolve(principal, user, "product.agent")
	if err != nil || snapshot.Name != "Agent v1" {
		t.Fatalf("rollback did not restore v1: %#v %v", snapshot, err)
	}

	// Rolling back to zero removes every release.
	if _, err := manager.Rollback(context.Background(), "product.agent", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := profiles.Resolve(principal, user, "product.agent"); !errors.Is(err, err) && err == nil {
		t.Fatal("profile should be gone after full rollback")
	} else if err == nil {
		t.Fatal("profile should be gone after full rollback")
	}

	history := manager.History(context.Background(), "product.agent")
	if len(history) != 2 || !history[0].RolledBack || !history[1].RolledBack {
		t.Fatalf("history not audit-complete: %#v", history)
	}
	if _, err := manager.Rollback(context.Background(), "product.agent", 5); err == nil {
		t.Fatal("rollback to unknown version must fail")
	}
}
