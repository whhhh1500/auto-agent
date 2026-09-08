package main

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

func TestContractFirstSchemasAreComplete(t *testing.T) {
	document, err := readDocument(filepath.Join("..", "..", "openapi", "harness-core-v1.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	routes := map[string]bool{}
	for _, requirement := range contractFirstRequirements {
		routes[requirement.route] = true
	}
	if err := validateContractFirstRoutes(document, routes); err != nil {
		t.Fatal(err)
	}
}

func TestContractFirstMissingRoutesAreExplicit(t *testing.T) {
	document, err := readDocument(filepath.Join("..", "..", "openapi", "harness-core-v1.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	err = validateContractFirstRoutes(document, map[string]bool{})
	if err == nil {
		t.Fatal("missing contract-first registrations must fail verification")
	}
	for _, requirement := range contractFirstRequirements {
		if !strings.Contains(err.Error(), "missing contract-first server route registration: "+requirement.route) {
			t.Fatalf("verification error does not name %q: %v", requirement.route, err)
		}
	}
}

func TestCapabilityBindDocumentsSubagentConditions(t *testing.T) {
	document, err := readDocument(filepath.Join("..", "..", "openapi", "harness-core-v1.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(document.Components.Schemas["CapabilityBindExecution"])
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"subagent", "entrypoint", "workdir", "sandbox", "writes"} {
		if !strings.Contains(string(encoded), required) {
			t.Fatalf("CapabilityBindExecution is missing %q: %s", required, encoded)
		}
	}
	// Limit metadata is intentionally documented on the manifest, keeping the
	// execution transport schema reusable for every adapter.
	manifest, err := json.Marshal(document.Components.Schemas["CapabilityManifest"])
	if err != nil || !strings.Contains(string(manifest), "subagent.max_depth") {
		t.Fatalf("CapabilityManifest must retain metadata for subagent limits: %v %s", err, manifest)
	}
}

func TestNotificationTargetsDocumentTenantFencingAndPrivateConfiguration(t *testing.T) {
	document, err := readDocument(filepath.Join("..", "..", "openapi", "harness-core-v1.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	pathItem := document.Paths["/v1/admin/notification-targets"]
	expectNotificationTargetOperation(t, document, pathItem["get"], "listNotificationTargets", "", []string{"200", "400", "401", "403", "409", "501", "503", "500"})
	if !hasParameter(pathItem["get"], "tenant_id", document.Components.Parameters) {
		t.Fatal("notification target list must document its tenant_id scope selector")
	}
	expectNotificationTargetOperation(t, document, pathItem["post"], "createNotificationTarget", "NotificationTargetCreateRequest", []string{"201", "400", "401", "403", "409", "501", "503", "500"})
	expectNotificationTargetOperation(t, document, pathItem["put"], "updateNotificationTarget", "NotificationTargetUpdateRequest", []string{"200", "400", "401", "403", "404", "409", "501", "503", "500"})
	expectNotificationTargetOperation(t, document, pathItem["delete"], "deleteNotificationTarget", "NotificationTargetDeleteRequest", []string{"200", "400", "401", "403", "404", "409", "501", "503", "500"})

	create := notificationTargetSchema(t, document, "NotificationTargetCreateRequest")
	expectRequiredProperties(t, create, "target_ref", "channel_id", "channel_version", "config", "enabled")
	expectWriteOnlyProperty(t, create, "config")
	update := notificationTargetSchema(t, document, "NotificationTargetUpdateRequest")
	expectRequiredProperties(t, update, "target_ref", "channel_id", "channel_version", "enabled", "expected_revision")
	if containsString(update.Required, "config") {
		t.Fatal("notification target update must preserve configuration when config is omitted")
	}
	expectWriteOnlyProperty(t, update, "config")
	deleteRequest := notificationTargetSchema(t, document, "NotificationTargetDeleteRequest")
	expectRequiredProperties(t, deleteRequest, "target_ref", "expected_revision")
	if _, ok := deleteRequest.Properties["config"]; ok {
		t.Fatal("notification target delete request must not carry configuration")
	}
	for _, name := range []string{"NotificationTargetView", "NotificationChannelView", "NotificationTargetListResponse"} {
		response := notificationTargetSchema(t, document, name)
		if _, ok := response.Properties["config"]; ok {
			t.Fatalf("%s must not expose notification target configuration", name)
		}
	}
	view := notificationTargetSchema(t, document, "NotificationTargetView")
	expectRequiredProperties(t, view, "target_ref", "channel_id", "channel_version", "enabled", "revision")
	channel := notificationTargetSchema(t, document, "NotificationChannelView")
	expectRequiredProperties(t, channel, "id", "version")
	for _, forbidden := range []string{"config", "secret", "url", "capabilities"} {
		if _, ok := channel.Properties[forbidden]; ok {
			t.Fatalf("notification channel discovery must not expose %q", forbidden)
		}
	}
	list := notificationTargetSchema(t, document, "NotificationTargetListResponse")
	expectRequiredProperties(t, list, "targets", "channels")
	if !hasResponseSchema(pathItem["get"], "NotificationTargetListResponse") {
		t.Fatal("notification target list must return NotificationTargetListResponse")
	}
}

func TestSandboxProviderDiscoveryDocumentsReadOnlySanitizedProbeContract(t *testing.T) {
	document, err := readDocument(filepath.Join("..", "..", "openapi", "harness-core-v1.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	operation := document.Paths["/v1/admin/sandbox/providers"]["get"]
	if operation.OperationID != "listSandboxProviders" || len(operation.Security) != 1 || len(operation.Security[0]["BearerAuth"]) != 0 {
		t.Fatalf("sandbox discovery must be an authenticated platform-admin operation: %#v", operation)
	}
	for _, status := range []string{"200", "401", "403", "501"} {
		if _, ok := operation.Responses[status]; !ok {
			t.Fatalf("sandbox discovery missing %s response", status)
		}
	}
	if !hasResponseSchema(operation, "SandboxProviderListResponse") {
		t.Fatal("sandbox discovery must return SandboxProviderListResponse")
	}
	for _, schemaName := range []string{"SandboxAssurance", "SandboxProviderProbe", "SandboxProviderView", "SandboxProviderListResponse"} {
		raw, ok := document.Components.Schemas[schemaName]
		if !ok || strings.Contains(string(raw), "provider_object") || strings.Contains(string(raw), "host_path") || strings.Contains(string(raw), "secret") {
			t.Fatalf("sandbox schema %s exposes an unsafe field or is missing: %s", schemaName, raw)
		}
	}
	probe := notificationTargetSchema(t, document, "SandboxProviderProbe")
	expectRequiredProperties(t, probe, "available", "assurance", "network", "supported_networks", "limits_enforced", "mounts_enforced")
	if _, ok := probe.Properties["unavailable_cause"]; !ok {
		t.Fatal("sandbox probe must document the sanitized unavailable category")
	}
}

func hasResponseSchema(operation operation, schema string) bool {
	for _, response := range operation.Responses {
		if strings.Contains(string(response), "#/components/schemas/"+schema) {
			return true
		}
	}
	return false
}

type notificationTargetSchemaShape struct {
	Required             []string                   `json:"required"`
	Properties           map[string]json.RawMessage `json:"properties"`
	AdditionalProperties bool                       `json:"additionalProperties"`
}

func notificationTargetSchema(t *testing.T, document openAPIDocument, name string) notificationTargetSchemaShape {
	t.Helper()
	raw, ok := document.Components.Schemas[name]
	if !ok {
		t.Fatalf("missing notification target schema %s", name)
	}
	var schema notificationTargetSchemaShape
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatalf("decode notification target schema %s: %v", name, err)
	}
	if !schema.AdditionalProperties {
		return schema
	}
	t.Fatalf("%s must reject unknown properties", name)
	return notificationTargetSchemaShape{}
}

func expectNotificationTargetOperation(t *testing.T, document openAPIDocument, op operation, operationID, requestSchema string, statuses []string) {
	t.Helper()
	if op.OperationID != operationID {
		t.Fatalf("operation ID = %q, want %q", op.OperationID, operationID)
	}
	if len(op.Security) != 1 || len(op.Security[0]["BearerAuth"]) != 0 {
		t.Fatalf("%s must require BearerAuth: %#v", operationID, op.Security)
	}
	if requestSchema != "" && !hasRequestSchema(op, requestSchema) {
		t.Fatalf("%s must declare %s", operationID, requestSchema)
	}
	for _, status := range statuses {
		if _, ok := op.Responses[status]; !ok {
			t.Fatalf("%s is missing %s response", operationID, status)
		}
	}
}

func expectRequiredProperties(t *testing.T, schema notificationTargetSchemaShape, names ...string) {
	t.Helper()
	for _, name := range names {
		if !containsString(schema.Required, name) {
			t.Fatalf("required properties %v do not include %q", schema.Required, name)
		}
	}
}

func expectWriteOnlyProperty(t *testing.T, schema notificationTargetSchemaShape, name string) {
	t.Helper()
	raw, ok := schema.Properties[name]
	var property struct {
		WriteOnly bool `json:"writeOnly"`
	}
	if !ok || json.Unmarshal(raw, &property) != nil || !property.WriteOnly {
		t.Fatalf("%q must be a write-only request property: %s", name, raw)
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
