// Command verify-openapi validates the checked-in OpenAPI JSON/YAML subset and
// makes the documented HTTP surface match the routes registered by pkg/server.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

type openAPIDocument struct {
	OpenAPI    string                          `json:"openapi"`
	Info       struct{ Version string }        `json:"info"`
	Paths      map[string]map[string]operation `json:"paths"`
	Components struct {
		SecuritySchemes map[string]json.RawMessage `json:"securitySchemes"`
		Schemas         map[string]json.RawMessage `json:"schemas"`
		Parameters      map[string]parameter       `json:"parameters"`
	} `json:"components"`
}

type operation struct {
	OperationID   string                     `json:"operationId"`
	Responses     map[string]json.RawMessage `json:"responses"`
	Security      []map[string][]string      `json:"security"`
	Parameters    []parameter                `json:"parameters"`
	Reconnect     string                     `json:"x-harness-reconnect"`
	RequestBody   json.RawMessage            `json:"requestBody"`
	ContractFirst bool                       `json:"x-harness-contract-first"`
}

type parameter struct {
	Ref      string `json:"$ref"`
	Name     string `json:"name"`
	In       string `json:"in"`
	Required bool   `json:"required"`
}

var (
	routePattern       = regexp.MustCompile(`mux\.Handle(?:Func)?\("([A-Z]+) (/v1[^" ]*)`)
	consolePathPattern = regexp.MustCompile(`["'](/v1[^"']*)["']`)
)

func main() {
	if len(os.Args) != 4 {
		fmt.Fprintln(os.Stderr, "usage: verify-openapi <openapi.yaml> <server-dir> <console-index.html>")
		os.Exit(2)
	}
	document, err := readDocument(os.Args[1])
	if err != nil {
		fail(err)
	}
	if document.OpenAPI != "3.1.0" || document.Info.Version != "v1" {
		fail(fmt.Errorf("expected versioned OpenAPI 3.1.0 document for v1"))
	}
	for _, name := range []string{"BearerAuth", "RunnerBearer"} {
		if _, ok := document.Components.SecuritySchemes[name]; !ok {
			fail(fmt.Errorf("missing security scheme %s", name))
		}
	}
	for _, name := range []string{"Error", "SSEEvent", "RunRecord", "Approval"} {
		if _, ok := document.Components.Schemas[name]; !ok {
			fail(fmt.Errorf("missing contract schema %s", name))
		}
	}
	routes, err := registeredRoutes(os.Args[2])
	if err != nil {
		fail(err)
	}
	for route := range routes {
		parts := strings.SplitN(route, " ", 2)
		method, path := strings.ToLower(parts[0]), normalizePath(parts[1])
		pathItem, ok := document.Paths[path]
		if !ok {
			fail(fmt.Errorf("OpenAPI is missing registered route %s", route))
		}
		op, ok := pathItem[method]
		if !ok || op.OperationID == "" || len(op.Responses) == 0 {
			fail(fmt.Errorf("OpenAPI operation is incomplete for %s", route))
		}
		if path != "/v1/auth/login" && len(op.Security) == 0 {
			fail(fmt.Errorf("OpenAPI operation is missing authentication for %s", route))
		}
		if err := validatePathParameters(path, op, document.Components.Parameters); err != nil {
			fail(fmt.Errorf("%s: %w", route, err))
		}
	}
	if err := validateContractFirstRoutes(document, routes); err != nil {
		fail(err)
	}
	for path, pathItem := range document.Paths {
		if !strings.HasPrefix(path, "/v1/") {
			continue
		}
		for method := range pathItem {
			route := strings.ToUpper(method) + " " + path
			if !routes[route] {
				fail(fmt.Errorf("OpenAPI declares an unregistered route %s", route))
			}
		}
	}
	run := document.Paths["/v1/sessions/{id}/runs"]["post"]
	if !hasResponseContent(run, "200", "text/event-stream") || run.Reconnect == "" {
		fail(fmt.Errorf("synchronous run must document its 200 SSE response"))
	}
	if _, ok := document.Paths["/v1/sessions/{id}/runs/async"]["post"].Responses["202"]; !ok {
		fail(fmt.Errorf("async run must document its 202 response"))
	}
	events := document.Paths["/v1/sessions/{id}/events"]["get"]
	if !hasParameter(events, "after_seq", document.Components.Parameters) {
		fail(fmt.Errorf("session events must document after_seq reconnect cursor"))
	}
	if err := validateConsolePaths(os.Args[3], document.Paths); err != nil {
		fail(err)
	}
	fmt.Printf("OpenAPI contract verified: %d registered /v1 operations\n", len(routes))
}

type contractFirstRequirement struct {
	route       string
	requestRef  string
	description string
}

var contractFirstRequirements = []contractFirstRequirement{
	{route: "GET /v1/admin/profiles/{id}", description: "read the effective profile at an owned scope"},
	{route: "PUT /v1/admin/profiles/{id}", requestRef: "AdminProfileWriteRequest", description: "write one scoped AgentProfileLayer"},
	{route: "POST /v1/admin/runners/tasks/{taskID}/retry", requestRef: "RunnerTaskRetryRequest", description: "retry a failed idempotent task with explicit confirmation"},
}

var contractFirstSchemas = map[string][]string{
	"AdminProfileWriteRequest": {`"scope"`, `"layer"`},
	"AdminProfileResponse":     {`"effective"`, `AgentProfileSnapshot`},
	"CapabilityManifest":       {`"input_schema"`, `"output_schema"`, `"required_permissions"`, `"requires_approval"`, `"per_turn_budget"`, `"timeout_ms"`, `"idempotent"`, `"tool"`, `"execution"`},
	"CapabilityBindRequest":    {`"scope"`, `"manifest"`, `"execution"`},
	"RunnerTaskRetryRequest":   {`"confirm"`, `"const": true`, `"reason"`},
	"RunnerTaskRetryResponse":  {`"original"`, `"retry"`, `"created"`, `RunnerTaskSummary`},
}

// validateContractFirstRoutes keeps planned control-plane contracts deliberate:
// schemas must remain complete, and the verifier stays red until the documented
// server handlers have actually registered the operations.
func validateContractFirstRoutes(document openAPIDocument, routes map[string]bool) error {
	problems := []string{}
	for name, tokens := range contractFirstSchemas {
		raw, ok := document.Components.Schemas[name]
		if !ok {
			problems = append(problems, fmt.Sprintf("missing contract-first schema %s", name))
			continue
		}
		for _, token := range tokens {
			if !strings.Contains(string(raw), token) {
				problems = append(problems, fmt.Sprintf("contract-first schema %s is missing %s", name, token))
			}
		}
	}
	capabilityBind, ok := document.Paths["/v1/admin/capabilities/bind"]["post"]
	if !ok || !hasRequestSchema(capabilityBind, "CapabilityBindRequest") {
		problems = append(problems, "capability bind must declare CapabilityBindRequest")
	}
	for _, requirement := range contractFirstRequirements {
		parts := strings.SplitN(requirement.route, " ", 2)
		pathItem := document.Paths[parts[1]]
		op, ok := pathItem[strings.ToLower(parts[0])]
		if !ok || !op.ContractFirst {
			problems = append(problems, fmt.Sprintf("missing contract-first operation %s", requirement.route))
			continue
		}
		if requirement.requestRef != "" && !hasRequestSchema(op, requirement.requestRef) {
			problems = append(problems, fmt.Sprintf("contract-first operation %s must declare %s", requirement.route, requirement.requestRef))
		}
		if !routes[requirement.route] {
			problems = append(problems, fmt.Sprintf("missing contract-first server route registration: %s (%s)", requirement.route, requirement.description))
		}
	}
	if len(problems) == 0 {
		return nil
	}
	sort.Strings(problems)
	return fmt.Errorf("OpenAPI contract-first validation failed:\n- %s", strings.Join(problems, "\n- "))
}

func hasRequestSchema(op operation, name string) bool {
	return strings.Contains(string(op.RequestBody), `#/components/schemas/`+name+`"`)
}

func readDocument(path string) (openAPIDocument, error) {
	data, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return openAPIDocument{}, err
	}
	var document openAPIDocument
	if err := json.Unmarshal(data, &document); err != nil {
		return openAPIDocument{}, fmt.Errorf("OpenAPI must use the JSON subset of YAML for dependency-free verification: %w", err)
	}
	return document, nil
}

func registeredRoutes(root string) (map[string]bool, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	routes := map[string]bool{}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(root, entry.Name()))
		if err != nil {
			return nil, err
		}
		for _, match := range routePattern.FindAllStringSubmatch(string(data), -1) {
			routes[match[1]+" "+normalizePath(match[2])] = true
		}
	}
	if len(routes) == 0 {
		return nil, fmt.Errorf("no /v1 routes found in %s", root)
	}
	return routes, nil
}

func normalizePath(path string) string { return strings.ReplaceAll(path, "...}", "}") }

var pathParameterPattern = regexp.MustCompile(`\{([^}]+)\}`)

func validatePathParameters(path string, op operation, components map[string]parameter) error {
	needed := pathParameterPattern.FindAllStringSubmatch(path, -1)
	if len(needed) == 0 {
		return nil
	}
	provided := map[string]bool{}
	for _, parameter := range op.Parameters {
		if parameter.Ref != "" {
			const prefix = "#/components/parameters/"
			if !strings.HasPrefix(parameter.Ref, prefix) {
				return fmt.Errorf("unsupported parameter reference %q", parameter.Ref)
			}
			var ok bool
			parameter, ok = components[strings.TrimPrefix(parameter.Ref, prefix)]
			if !ok {
				return fmt.Errorf("missing parameter reference %q", parameter.Ref)
			}
		}
		if parameter.In == "path" && parameter.Required {
			provided[parameter.Name] = true
		}
	}
	for _, match := range needed {
		if !provided[match[1]] {
			return fmt.Errorf("missing required path parameter %q", match[1])
		}
	}
	return nil
}

func hasResponseContent(op operation, status, contentType string) bool {
	raw, ok := op.Responses[status]
	if !ok {
		return false
	}
	var response struct {
		Content map[string]json.RawMessage `json:"content"`
	}
	return json.Unmarshal(raw, &response) == nil && response.Content[contentType] != nil
}

func hasParameter(op operation, name string, components map[string]parameter) bool {
	for _, parameter := range op.Parameters {
		if parameter.Ref != "" {
			parameter = components[strings.TrimPrefix(parameter.Ref, "#/components/parameters/")]
		}
		if parameter.Name == name {
			return true
		}
	}
	return false
}

func validateConsolePaths(path string, paths map[string]map[string]operation) error {
	data, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return err
	}
	for _, match := range consolePathPattern.FindAllStringSubmatch(string(data), -1) {
		prefix := strings.SplitN(match[1], "?", 2)[0]
		matched := false
		for documented := range paths {
			if documentedPathMatches(prefix, documented) {
				matched = true
				break
			}
		}
		if !matched {
			return fmt.Errorf("console calls undocumented API prefix %q", prefix)
		}
	}
	return nil
}

func documentedPathMatches(actual, documented string) bool {
	actualParts := strings.Split(strings.Trim(actual, "/"), "/")
	documentedParts := strings.Split(strings.Trim(documented, "/"), "/")
	if len(actualParts) > len(documentedParts) {
		return false
	}
	for index, part := range actualParts {
		expected := documentedParts[index]
		if expected == part || (strings.HasPrefix(expected, "{") && strings.HasSuffix(expected, "}")) {
			continue
		}
		return false
	}
	return true
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
