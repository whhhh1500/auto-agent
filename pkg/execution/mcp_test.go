package execution

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

func TestMCPEnvironmentDoesNotInheritSecretsByDefault(t *testing.T) {
	t.Setenv("HARNESS_LLM_API_KEY", "must-not-leak")
	environment := mcpEnvironment(MCPServerConfig{Env: []string{"MCP_MODE=test"}})
	joined := strings.Join(environment, "\n")
	if strings.Contains(joined, "HARNESS_LLM_API_KEY=") {
		t.Fatal("MCP server inherited an unrelated model credential")
	}
	if !strings.Contains(joined, "MCP_MODE=test") {
		t.Fatalf("explicit MCP environment missing: %q", joined)
	}
}

func TestMCPResponseLineLimit(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader(strings.Repeat("x", maxMCPResponseLineBytes+1) + "\n"))
	if _, err := readMCPLine(reader); err == nil {
		t.Fatal("oversized MCP response line was accepted")
	}
}

func TestMCPEnvironmentCanExplicitlyInherit(t *testing.T) {
	t.Setenv("MCP_PARENT_VISIBLE", "yes")
	environment := mcpEnvironment(MCPServerConfig{InheritEnvironment: true})
	if !strings.Contains(strings.Join(environment, "\n"), "MCP_PARENT_VISIBLE=yes") {
		t.Fatal("explicit environment inheritance was ignored")
	}
	_ = os.Getenv("PATH")
}

func TestMCPCapabilityID(t *testing.T) {
	id, err := mcpCapabilityID("mcp.weather", "get-forecast")
	if err != nil || id != "mcp.weather.get_forecast" {
		t.Fatalf("sanitized id=%q err=%v", id, err)
	}
	id, err = mcpCapabilityID("mcp.weather", "list cities")
	if err != nil || id != "mcp.weather.list_cities" {
		t.Fatalf("space id=%q err=%v", id, err)
	}
	if _, err := mcpCapabilityID("weather", "get"); err == nil {
		t.Fatal("namespace without a dot was accepted")
	}
	if _, err := mcpCapabilityID("mcp.weather", ""); err == nil {
		t.Fatal("empty tool name was accepted")
	}
	if _, err := mcpCapabilityID("mcp.weather", "get@forecast"); err == nil {
		t.Fatal("tool name with unsafe characters was accepted")
	}
}

func TestValidateMCPConfig(t *testing.T) {
	if err := validateMCPConfig(MCPServerConfig{Namespace: "mcp.weather"}); err == nil {
		t.Fatal("empty command was accepted")
	}
	if err := validateMCPConfig(MCPServerConfig{Namespace: "weather", Command: []string{"npx"}}); err == nil {
		t.Fatal("namespace without a dot was accepted")
	}
	if err := validateMCPConfig(MCPServerConfig{Namespace: "mcp.weather", Command: []string{"npx"}, Env: []string{"NOTANENTRY"}}); err == nil {
		t.Fatal("environment without an assignment was accepted")
	}
	if err := validateMCPConfig(MCPServerConfig{Namespace: "mcp.weather", Command: []string{"npx"}, Env: []string{"FOO=bar"}}); err != nil {
		t.Fatalf("valid config was rejected: %v", err)
	}
	if err := validateMCPConfig(MCPServerConfig{Namespace: "mcp.weather", Command: []string{"npx\x00"}}); err == nil {
		t.Fatal("NUL command was accepted")
	}
	if err := validateMCPConfig(MCPServerConfig{Namespace: "mcp.weather", Command: []string{"npx"}, Env: []string{"FOO\x00=bar"}}); err == nil {
		t.Fatal("NUL environment was accepted")
	}
	command := make([]string, maxExecArgs)
	for i := range command {
		command[i] = "npx"
	}
	if err := validateMCPConfig(MCPServerConfig{Namespace: "mcp.weather", Command: command}); err != nil {
		t.Fatalf("command at entry limit rejected: %v", err)
	}
	if err := validateMCPConfig(MCPServerConfig{Namespace: "mcp.weather", Command: append(command, "extra")}); err == nil {
		t.Fatal("oversized command list was accepted")
	}
	if err := validateMCPConfig(MCPServerConfig{Namespace: "mcp.weather", Command: []string{strings.Repeat("a", maxExecArgBytes)}}); err != nil {
		t.Fatalf("command argument at byte limit rejected: %v", err)
	}
	if err := validateMCPConfig(MCPServerConfig{Namespace: "mcp.weather", Command: []string{strings.Repeat("a", maxExecArgBytes+1)}}); err == nil {
		t.Fatal("oversized command argument was accepted")
	}
	env := make([]string, maxExecArgs)
	for i := range env {
		env[i] = "FOO=bar"
	}
	if err := validateMCPConfig(MCPServerConfig{Namespace: "mcp.weather", Command: []string{"npx"}, Env: env}); err != nil {
		t.Fatalf("environment at entry limit rejected: %v", err)
	}
	if err := validateMCPConfig(MCPServerConfig{Namespace: "mcp.weather", Command: []string{"npx"}, Env: append(env, "BAZ=qux")}); err == nil {
		t.Fatal("oversized environment list was accepted")
	}
	if err := validateMCPConfig(MCPServerConfig{Namespace: "mcp.weather", Command: []string{"npx"}, Env: []string{"FOO=" + strings.Repeat("v", maxExecArgBytes-4)}}); err != nil {
		t.Fatalf("environment entry at byte limit rejected: %v", err)
	}
	if err := validateMCPConfig(MCPServerConfig{Namespace: "mcp.weather", Command: []string{"npx"}, Env: []string{"FOO=" + strings.Repeat("v", maxExecArgBytes)}}); err == nil {
		t.Fatal("oversized environment entry was accepted")
	}
}

func TestMCPTextContentBounds(t *testing.T) {
	text, err := mcpTextContent([]mcpContentBlock{{Type: "text", Text: "hello"}, {Type: "image", Text: "ignore"}}, 16)
	if err != nil || text != "hello" {
		t.Fatalf("text=%q err=%v", text, err)
	}
	if _, err := mcpTextContent([]mcpContentBlock{{Type: "text", Text: "hello world"}}, 8); err == nil {
		t.Fatal("oversized MCP text was accepted")
	}
}

func TestMCPToolLibraryIndexesLargeCatalogWithoutInliningSchemas(t *testing.T) {
	tools := make([]mcpToolDescription, 300)
	for i := range tools {
		tools[i] = mcpToolDescription{
			Name:        fmt.Sprintf("tool-%d", i),
			Description: fmt.Sprintf("utility number %d", i),
			InputSchema: map[string]any{"type": "object", "properties": map[string]any{"n": map[string]any{"type": "integer"}}},
		}
	}
	tools[41].Name, tools[41].Description = "csv-parser", "Parse CSV files into rows"
	catalog := indexMCPTools(tools)
	if len(catalog) != 300 {
		t.Fatalf("catalog size = %d", len(catalog))
	}
	listed := mcpSummaries(catalog[:defaultMCPListLimit])
	if len(listed) != defaultMCPListLimit {
		t.Fatalf("list page = %d", len(listed))
	}
	if _, ok := listed[0]["input_schema"]; ok || listed[0]["name"] == "" {
		t.Fatalf("list leaked schema or lost names: %#v", listed[0])
	}
	hits := searchToolLibrary(catalog, "parse csv", 8)
	if len(hits) == 0 || hits[0].Name != "csv-parser" {
		t.Fatalf("search did not retrieve csv-parser: %#v", hits)
	}
	if strings.Contains(fmt.Sprintf("%#v", hits), "input_schema") {
		t.Fatal("search inlined input schemas")
	}
}

func TestMCPGatewayDescribeAndUnknownCall(t *testing.T) {
	gateway := newMCPGateway(nil, indexMCPTools([]mcpToolDescription{
		{Name: "get-forecast", Description: "Weather forecast for a city", InputSchema: map[string]any{"type": "object"}},
	}))
	described, err := gateway.describe(context.Background(), core.CapabilityRequest{CallID: "c1", Args: map[string]any{"name": "get-forecast"}})
	if err != nil || !described.OK || !strings.Contains(described.Content, "input_schema") {
		t.Fatalf("describe = %#v err=%v", described, err)
	}
	listed, err := gateway.list(context.Background(), core.CapabilityRequest{CallID: "c1", Args: map[string]any{"limit": float64(1)}})
	if err != nil || !listed.OK || strings.Contains(listed.Content, "input_schema") {
		t.Fatalf("list inlined schemas: %#v err=%v", listed, err)
	}
	searched, err := gateway.search(context.Background(), core.CapabilityRequest{CallID: "c1", Args: map[string]any{"query": "forecast"}})
	if err != nil || !searched.OK || !strings.Contains(searched.Content, "get-forecast") {
		t.Fatalf("search did not return the indexed tool: %#v err=%v", searched, err)
	}
	gateway.catalog = nil
	fallback, err := gateway.search(context.Background(), core.CapabilityRequest{CallID: "c1", Args: map[string]any{"query": "forecast"}})
	if err != nil || !fallback.OK || !strings.Contains(fallback.Content, "get-forecast") {
		t.Fatalf("search fallback did not return the tool: %#v err=%v", fallback, err)
	}
	missing, err := gateway.callTool(context.Background(), core.CapabilityRequest{CallID: "c1", Args: map[string]any{"name": "nope"}})
	if err != nil || missing.OK || missing.Metadata["code"] != "invalid_args" {
		t.Fatalf("unknown tool call = %#v err=%v", missing, err)
	}
}

func TestSearchPrefersSemanticDescriptionOverSharedName(t *testing.T) {
	tools := qualifyToolLibrary("mcp.mail", []toolDocument{
		{Name: "send", Description: "Send an email to an address"},
	})
	tools = append(tools, qualifyToolLibrary("mcp.slack", []toolDocument{
		{Name: "send", Description: "Send a message to a Slack channel"},
	})...)
	hits := searchToolLibrary(tools, "send slack channel message", 8)
	if len(hits) == 0 || hits[0].ID != "mcp.slack/send" {
		t.Fatalf("semantic send missed slack tool: %#v", hits)
	}
	for _, hit := range hits {
		if hit.ID == "mcp.mail/send" {
			t.Fatalf("email send should not rank for slack intent: %#v", hits)
		}
		if hit.Name == "send" && !hit.Ambiguous {
			t.Fatalf("shared name must be marked ambiguous: %#v", hit)
		}
	}
}

func TestAmbiguousLocalNameReturnsExploreCandidates(t *testing.T) {
	gateway := newMCPGateway(nil, nil)
	gateway.tools = qualifyToolLibrary("mcp.mail", []toolDocument{{Name: "send", Description: "Send an email"}})
	gateway.tools = append(gateway.tools, qualifyToolLibrary("mcp.slack", []toolDocument{{Name: "send", Description: "Send a Slack message"}})...)
	gateway.byID = map[string]toolDocument{}
	gateway.byName = map[string][]toolDocument{}
	for _, tool := range gateway.tools {
		gateway.byID[tool.ID] = tool
		gateway.byName[tool.Name] = append(gateway.byName[tool.Name], tool)
	}
	resolved := gateway.resolveTool(map[string]any{"name": "send"})
	if resolved.denied || len(resolved.candidates) != 2 {
		t.Fatalf("ambiguous name should stay explorable: %#v", resolved)
	}
	result, err := gateway.describe(context.Background(), core.CapabilityRequest{CallID: "c1", Args: map[string]any{"name": "send"}})
	if err != nil || !result.OK || !strings.Contains(result.Content, `"status":"explore"`) {
		t.Fatalf("describe should return explore candidates: %#v err=%v", result, err)
	}
	if result.Metadata["library.explore"] != true {
		t.Fatalf("explore must be observable: %#v", result.Metadata)
	}
	picked := gateway.resolveTool(map[string]any{"id": "mcp.slack/send"})
	if picked.denied || picked.tool.Library != "mcp.slack" {
		t.Fatalf("qualified id failed: %#v", picked)
	}
}

func TestLibraryObserverRecordsSearchThenChoose(t *testing.T) {
	observer := NewMemoryLibraryObserver()
	gateway := newMCPGateway(nil, indexMCPTools([]mcpToolDescription{
		{Name: "csv-parser", Description: "Parse CSV files into rows"},
		{Name: "send", Description: "Send an email to an address"},
	}))
	gateway.observer = observer
	gateway.tools = qualifyToolLibrary("mcp.files", gateway.tools)
	gateway.byID = map[string]toolDocument{}
	gateway.byName = map[string][]toolDocument{}
	for _, tool := range gateway.tools {
		gateway.byID[tool.ID] = tool
		gateway.byName[tool.Name] = append(gateway.byName[tool.Name], tool)
	}
	if _, err := gateway.search(context.Background(), core.CapabilityRequest{
		CallID: "c-search", Args: map[string]any{"query": "parse csv"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := gateway.describe(context.Background(), core.CapabilityRequest{
		CallID: "c-describe", Args: map[string]any{"id": "mcp.files/csv-parser"},
	}); err != nil {
		t.Fatal(err)
	}
	records := observer.Snapshot()
	if len(records) != 2 || records[0].Action != "search" || records[1].Action != "describe" {
		t.Fatalf("disclosure trace = %#v", records)
	}
	if records[0].Query != "parse csv" || records[1].ToolID != "mcp.files/csv-parser" {
		t.Fatalf("query/choose not recorded: %#v", records)
	}
}

func TestMCPGatewayKeepsCatalogWhenListingEmpty(t *testing.T) {
	gateway := newMCPGateway(nil, indexMCPTools([]mcpToolDescription{
		{Name: "get-forecast", Description: "Weather forecast for a city"},
	}))
	if gateway.catalog.Len() != 1 {
		t.Fatalf("catalog len = %d", gateway.catalog.Len())
	}
	if err := gateway.replaceListing(nil); err == nil {
		t.Fatal("empty listing wiped the catalog")
	}
	if gateway.catalog.Len() != 1 || gateway.byID["get-forecast"].Name != "get-forecast" {
		t.Fatalf("previous catalog was lost: len=%d", gateway.catalog.Len())
	}
	updated := qualifyToolLibrary("mcp.weather", indexMCPTools([]mcpToolDescription{
		{Name: "get-forecast", Description: "Weather forecast for a city, updated"},
		{Name: "alerts", Description: "Weather alerts"},
	}))
	if err := gateway.replaceListing(updated); err != nil {
		t.Fatal(err)
	}
	if gateway.catalog.Len() != 2 {
		t.Fatalf("updated catalog len = %d", gateway.catalog.Len())
	}
	if _, ok := gateway.byID["mcp.weather/alerts"]; !ok {
		t.Fatal("new tool was not added")
	}
}

func TestMCPGatewaySearchHonorsNotFor(t *testing.T) {
	gateway := newMCPGateway(nil, nil)
	docs := qualifyToolLibrary("mcp.mail", []toolDocument{{
		Name: "send", Description: "Send an email to an address", Triggers: []string{"gmail", "inbox"},
	}})
	docs = append(docs, qualifyToolLibrary("mcp.slack", []toolDocument{{
		Name: "send", Description: "Send a message to a Slack channel", NotFor: []string{"email", "gmail"},
	}})...)
	if err := gateway.replaceListing(docs); err != nil {
		t.Fatal(err)
	}
	result, err := gateway.search(context.Background(), core.CapabilityRequest{
		CallID: "c1", Args: map[string]any{"query": "send gmail inbox"},
	})
	if err != nil || !result.OK {
		t.Fatalf("search failed: %#v err=%v", result, err)
	}
	if !strings.Contains(result.Content, "mcp.mail/send") || strings.Contains(result.Content, `"id":"mcp.slack/send"`) {
		t.Fatalf("NotFor did not exclude slack send: %s", result.Content)
	}
}
