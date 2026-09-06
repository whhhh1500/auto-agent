package evaluation

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

func compatibilityCapability(id string, manifest func(*core.CapabilityManifest)) core.SnapshotCapability {
	value := core.CapabilityManifest{
		ID: id, Version: "1.0.0", Name: id, Kind: core.KindTool, Contract: "harness.tool/v1",
		RequiredPermissions: []core.Permission{core.PermRead}, Idempotent: true,
		Tool: &core.ToolExposure{Parameters: map[string]any{
			"type": "object", "properties": map[string]any{
				"query": map[string]any{"type": "string"},
			}, "required": []any{"query"}, "additionalProperties": false,
		}},
	}
	if manifest != nil {
		manifest(&value)
	}
	return core.SnapshotCapability{Manifest: value, ProviderRevision: "provider/v1"}
}

func TestCapabilityCompatibilityAcceptsIdenticalAndOptionalAdditions(t *testing.T) {
	baseline := compatibilityCapability("search.query", nil)
	candidate := compatibilityCapability("search.query", func(manifest *core.CapabilityManifest) {
		manifest.Tool.Parameters["properties"].(map[string]any)["limit"] = map[string]any{"type": "number"}
	})
	result, err := CompareCapabilitySnapshots("base", []core.SnapshotCapability{baseline}, "candidate", []core.SnapshotCapability{candidate}, CapabilityCompatibilityPolicy{})
	if err != nil || !result.Compatible || len(result.Issues) != 0 {
		t.Fatalf("optional input addition should be compatible: %#v err=%v", result, err)
	}
}

func TestCapabilityCompatibilityRejectsRemovalAndRiskyAddition(t *testing.T) {
	baseline := []core.SnapshotCapability{compatibilityCapability("search.query", nil)}
	candidate := []core.SnapshotCapability{compatibilityCapability("payments.release", func(manifest *core.CapabilityManifest) {
		manifest.Idempotent = false
		manifest.RequiresApproval = true
	})}
	result, err := CompareCapabilitySnapshots("base", baseline, "candidate", candidate, CapabilityCompatibilityPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Compatible || len(result.Removed) != 1 || len(result.Added) != 1 || len(result.Issues) != 3 {
		t.Fatalf("breaking capability set was accepted: %#v", result)
	}
	allowed, err := CompareCapabilitySnapshots("base", baseline, "candidate", candidate, CapabilityCompatibilityPolicy{
		AllowBreakingCapabilities: []string{"search.query", "payments.release"},
	})
	if err != nil || !allowed.Compatible {
		t.Fatalf("explicit compatibility override was not honored: %#v err=%v", allowed, err)
	}
	for _, issue := range allowed.Issues {
		if !issue.Allowed {
			t.Fatalf("override evidence lost allowed marker: %#v", issue)
		}
	}
}

func TestCapabilityCompatibilityRejectsSecurityAndSchemaDowngrades(t *testing.T) {
	baseline := compatibilityCapability("search.query", nil)
	candidate := compatibilityCapability("search.query", func(manifest *core.CapabilityManifest) {
		manifest.RequiredPermissions = append(manifest.RequiredPermissions, core.PermWrite)
		manifest.RequiredCredentials = []core.CredentialRef{"search.api"}
		manifest.Idempotent = false
		manifest.RequiresApproval = true
		manifest.Tool.Parameters["required"] = []any{"query", "limit"}
		manifest.Tool.Parameters["properties"].(map[string]any)["limit"] = map[string]any{"type": "number", "minimum": 1.0}
	})
	result, err := CompareCapabilitySnapshots("base", []core.SnapshotCapability{baseline}, "candidate", []core.SnapshotCapability{candidate}, CapabilityCompatibilityPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Compatible {
		t.Fatalf("security/schema downgrade was accepted: %#v", result)
	}
	codes := map[string]bool{}
	for _, issue := range result.Issues {
		codes[issue.Code] = true
	}
	for _, code := range []string{
		"required_permission_added", "required_credential_added", "idempotency_downgraded",
		"approval_requirement_added", "input_schema_narrowed",
	} {
		if !codes[code] {
			t.Fatalf("missing compatibility issue %s: %#v", code, result.Issues)
		}
	}
}

func TestCapabilityCompatibilityRejectsProviderAndContractChanges(t *testing.T) {
	baseline := compatibilityCapability("search.query", nil)
	candidate := compatibilityCapability("search.query", func(manifest *core.CapabilityManifest) {
		manifest.Contract = "acme.search/v2"
	})
	candidate.ProviderRevision = "provider/v2"
	result, err := CompareCapabilitySnapshots("base", []core.SnapshotCapability{baseline}, "candidate", []core.SnapshotCapability{candidate}, CapabilityCompatibilityPolicy{})
	if err != nil || result.Compatible {
		t.Fatalf("provider/contract change was accepted: %#v err=%v", result, err)
	}
	codes := map[string]bool{}
	for _, issue := range result.Issues {
		codes[issue.Code] = true
	}
	if !codes["contract_changed"] || !codes["provider_revision_changed"] {
		t.Fatalf("provider/contract evidence incomplete: %#v", result.Issues)
	}
}

func TestCapabilityCompatibilityRejectsUnknownCandidateConstraint(t *testing.T) {
	baseline := compatibilityCapability("search.query", nil)
	candidate := compatibilityCapability("search.query", func(manifest *core.CapabilityManifest) {
		manifest.Tool.Parameters["dependentRequired"] = map[string]any{"query": []any{"tenant"}}
	})
	result, err := CompareCapabilitySnapshots("base", []core.SnapshotCapability{baseline}, "candidate", []core.SnapshotCapability{candidate}, CapabilityCompatibilityPolicy{})
	if err != nil || result.Compatible || len(result.Issues) != 1 || result.Issues[0].Code != "input_schema_narrowed" {
		t.Fatalf("unknown candidate constraint was not rejected: %#v err=%v", result, err)
	}
}

func TestCapabilityCompatibilityChecksRemovedPropertyAgainstAdditionalSchema(t *testing.T) {
	baseline := compatibilityCapability("search.query", func(manifest *core.CapabilityManifest) {
		manifest.Tool.Parameters["additionalProperties"] = map[string]any{"type": "number"}
	})
	candidate := compatibilityCapability("search.query", func(manifest *core.CapabilityManifest) {
		delete(manifest.Tool.Parameters["properties"].(map[string]any), "query")
		manifest.Tool.Parameters["required"] = []any{}
		manifest.Tool.Parameters["additionalProperties"] = map[string]any{"type": "number"}
	})
	result, err := CompareCapabilitySnapshots("base", []core.SnapshotCapability{baseline}, "candidate", []core.SnapshotCapability{candidate}, CapabilityCompatibilityPolicy{})
	if err != nil || result.Compatible || len(result.Issues) != 1 || result.Issues[0].Code != "input_schema_narrowed" {
		t.Fatalf("removed explicit property was not checked against additionalProperties: %#v err=%v", result, err)
	}
}

func TestCapabilityCompatibilityBoundsIssueEvidence(t *testing.T) {
	baseline := make([]core.SnapshotCapability, MaxCapabilityCompatibilityIssues+20)
	for index := range baseline {
		baseline[index] = compatibilityCapability("tool."+strings.Repeat("x", 110)+fmt.Sprintf("%03d", index), nil)
	}
	result, err := CompareCapabilitySnapshots("base", baseline, "candidate", nil, CapabilityCompatibilityPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Compatible || !result.IssuesTruncated || !result.ListsTruncated ||
		len(result.Issues) != MaxCapabilityCompatibilityIssues || result.TotalIssues != len(baseline) ||
		len(result.Removed) != MaxCapabilityCompatibilityListItems || result.RemovedTotal != len(baseline) ||
		len(result.Changed) != MaxCapabilityCompatibilityListItems || result.ChangedTotal != len(baseline) {
		t.Fatalf("bounded compatibility evidence wrong: %#v", result)
	}
	firstRevision, err := CapabilityCompatibilityRevision(result)
	if err != nil {
		t.Fatal(err)
	}
	secondRevision, err := CapabilityCompatibilityRevision(result)
	if err != nil || firstRevision == "" || firstRevision != secondRevision {
		t.Fatalf("compatibility revision is unstable: first=%q second=%q err=%v", firstRevision, secondRevision, err)
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) > MaxEvaluationMetadata {
		t.Fatalf("bounded compatibility evidence is %d bytes, limit %d", len(encoded), MaxEvaluationMetadata)
	}
}

func TestCapabilityDeclarationRevisionIsOrderIndependent(t *testing.T) {
	first := compatibilityCapability("search.first", nil)
	second := compatibilityCapability("search.second", nil)
	left, err := CapabilityDeclarationRevision([]core.SnapshotCapability{first, second})
	if err != nil {
		t.Fatal(err)
	}
	right, err := CapabilityDeclarationRevision([]core.SnapshotCapability{second, first})
	if err != nil || left == "" || left != right {
		t.Fatalf("declaration revision is not stable: left=%q right=%q err=%v", left, right, err)
	}
}
