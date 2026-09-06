package evaluation

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

const (
	MaxCapabilityCompatibilityIssues    = 96
	MaxCapabilityCompatibilityListItems = 32
)

// CapabilityCompatibilityRevision is a stable digest for bounded Evaluation
// metadata and cross-artifact correlation. Full issue evidence remains in the
// Gate/Audit artifact.
func CapabilityCompatibilityRevision(result CapabilityCompatibilityResult) (string, error) {
	encoded, err := json.Marshal(result)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

// CapabilityDeclarationRevision fingerprints a Profile-selected declaration
// set independently from one Principal's grants. Entries are normalized by ID.
func CapabilityDeclarationRevision(values []core.SnapshotCapability) (string, error) {
	byID, err := capabilityManifestMap(values)
	if err != nil {
		return "", err
	}
	ids := make([]string, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	normalized := make([]core.SnapshotCapability, 0, len(ids))
	for _, id := range ids {
		normalized = append(normalized, byID[id])
	}
	encoded, err := json.Marshal(normalized)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

// CapabilityCompatibilityPolicy permits explicitly reviewed breaking changes
// while keeping the default comparison fail-closed.
type CapabilityCompatibilityPolicy struct {
	AllowBreakingCapabilities []string `json:"allow_breaking_capabilities,omitempty"`
}

type CapabilityCompatibilityIssue struct {
	CapabilityID string `json:"capability_id"`
	Code         string `json:"code"`
	Message      string `json:"message"`
	Allowed      bool   `json:"allowed,omitempty"`
}

type CapabilityCompatibilityResult struct {
	Compatible          bool                           `json:"compatible"`
	BaselineSnapshotID  string                         `json:"baseline_snapshot_id,omitempty"`
	CandidateSnapshotID string                         `json:"candidate_snapshot_id,omitempty"`
	Added               []string                       `json:"added,omitempty"`
	Removed             []string                       `json:"removed,omitempty"`
	Changed             []string                       `json:"changed,omitempty"`
	AddedTotal          int                            `json:"added_total,omitempty"`
	RemovedTotal        int                            `json:"removed_total,omitempty"`
	ChangedTotal        int                            `json:"changed_total,omitempty"`
	ListsTruncated      bool                           `json:"lists_truncated,omitempty"`
	Issues              []CapabilityCompatibilityIssue `json:"issues,omitempty"`
	TotalIssues         int                            `json:"total_issues,omitempty"`
	IssuesTruncated     bool                           `json:"issues_truncated,omitempty"`
}

// CompareCapabilitySnapshots checks whether callers and operators relying on
// the baseline declarations can safely move to the candidate declarations.
// It is intentionally conservative for security and side-effect semantics.
func CompareCapabilitySnapshots(
	baselineSnapshotID string,
	baseline []core.SnapshotCapability,
	candidateSnapshotID string,
	candidate []core.SnapshotCapability,
	policy CapabilityCompatibilityPolicy,
) (CapabilityCompatibilityResult, error) {
	allowed := map[string]bool{}
	for _, id := range policy.AllowBreakingCapabilities {
		if err := core.ValidateNamespacedID(id); err != nil {
			return CapabilityCompatibilityResult{}, fmt.Errorf("compatibility allowlist: %w", err)
		}
		allowed[id] = true
	}
	baselineByID, err := capabilityManifestMap(baseline)
	if err != nil {
		return CapabilityCompatibilityResult{}, fmt.Errorf("baseline capabilities: %w", err)
	}
	candidateByID, err := capabilityManifestMap(candidate)
	if err != nil {
		return CapabilityCompatibilityResult{}, fmt.Errorf("candidate capabilities: %w", err)
	}
	result := CapabilityCompatibilityResult{
		Compatible: true, BaselineSnapshotID: baselineSnapshotID, CandidateSnapshotID: candidateSnapshotID,
	}
	changed := map[string]bool{}
	addIssue := func(id, code, message string) {
		issue := CapabilityCompatibilityIssue{
			CapabilityID: id, Code: code, Message: message, Allowed: allowed[id],
		}
		result.TotalIssues++
		if len(result.Issues) < MaxCapabilityCompatibilityIssues {
			result.Issues = append(result.Issues, issue)
		} else {
			result.IssuesTruncated = true
		}
		changed[id] = true
		if !issue.Allowed {
			result.Compatible = false
		}
	}

	for id, baselineCapability := range baselineByID {
		candidateCapability, exists := candidateByID[id]
		if !exists {
			result.RemovedTotal++
			if len(result.Removed) < MaxCapabilityCompatibilityListItems {
				result.Removed = append(result.Removed, id)
			} else {
				result.ListsTruncated = true
			}
			addIssue(id, "capability_removed", "candidate removes a baseline capability")
			continue
		}
		compareCapabilityManifest(id, baselineCapability, candidateCapability, addIssue)
	}
	for id, candidateCapability := range candidateByID {
		if _, exists := baselineByID[id]; exists {
			continue
		}
		result.AddedTotal++
		if len(result.Added) < MaxCapabilityCompatibilityListItems {
			result.Added = append(result.Added, id)
		} else {
			result.ListsTruncated = true
		}
		if !candidateCapability.Manifest.Idempotent {
			addIssue(id, "non_idempotent_capability_added", "candidate adds a non-idempotent capability")
		}
		if candidateCapability.Manifest.RequiresApproval {
			addIssue(id, "approval_capability_added", "candidate adds a capability requiring approval")
		}
	}
	for id := range allowed {
		if _, baselineExists := baselineByID[id]; !baselineExists {
			if _, candidateExists := candidateByID[id]; !candidateExists {
				return CapabilityCompatibilityResult{}, fmt.Errorf("compatibility allowlist capability %q is not visible", id)
			}
		}
	}
	changedIDs := make([]string, 0, len(changed))
	for id := range changed {
		changedIDs = append(changedIDs, id)
	}
	sort.Strings(changedIDs)
	result.ChangedTotal = len(changedIDs)
	if len(changedIDs) > MaxCapabilityCompatibilityListItems {
		result.Changed = append(result.Changed, changedIDs[:MaxCapabilityCompatibilityListItems]...)
		result.ListsTruncated = true
	} else {
		result.Changed = append(result.Changed, changedIDs...)
	}
	sort.Strings(result.Added)
	sort.Strings(result.Removed)
	sort.Slice(result.Issues, func(i, j int) bool {
		if result.Issues[i].CapabilityID != result.Issues[j].CapabilityID {
			return result.Issues[i].CapabilityID < result.Issues[j].CapabilityID
		}
		return result.Issues[i].Code < result.Issues[j].Code
	})
	return result, nil
}

func capabilityManifestMap(values []core.SnapshotCapability) (map[string]core.SnapshotCapability, error) {
	out := map[string]core.SnapshotCapability{}
	for _, value := range values {
		id := value.Manifest.ID
		if err := core.ValidateNamespacedID(id); err != nil {
			return nil, err
		}
		if _, exists := out[id]; exists {
			return nil, fmt.Errorf("capability %q appears more than once", id)
		}
		out[id] = value
	}
	return out, nil
}

func compareCapabilityManifest(
	id string,
	baseline core.SnapshotCapability,
	candidate core.SnapshotCapability,
	issue func(string, string, string),
) {
	left, right := baseline.Manifest, candidate.Manifest
	if left.Kind != right.Kind {
		issue(id, "kind_changed", "capability kind changed")
	}
	if left.Contract != right.Contract {
		issue(id, "contract_changed", "capability contract changed")
	}
	if left.Tool != nil && right.Tool == nil {
		issue(id, "tool_exposure_removed", "model-facing tool exposure was removed")
	}
	if left.Tool == nil && right.Tool != nil {
		issue(id, "tool_exposure_added", "candidate exposes the capability as a model-facing tool")
	}
	if left.Version != right.Version {
		issue(id, "version_changed", "capability version changed")
	}
	if !left.Idempotent && right.Idempotent {
		// This is a safety improvement and remains compatible.
	} else if left.Idempotent && !right.Idempotent {
		issue(id, "idempotency_downgraded", "capability changed from idempotent to non-idempotent")
	}
	if !left.RequiresApproval && right.RequiresApproval {
		issue(id, "approval_requirement_added", "capability now requires approval")
	}
	for _, permission := range addedStrings(permissionStrings(left.RequiredPermissions), permissionStrings(right.RequiredPermissions)) {
		issue(id, "required_permission_added", "candidate requires additional permission "+permission)
	}
	for _, credential := range addedStrings(credentialStrings(left.RequiredCredentials), credentialStrings(right.RequiredCredentials)) {
		issue(id, "required_credential_added", "candidate requires additional credential "+credential)
	}
	if left.PerTurnBudget > 0 && right.PerTurnBudget > 0 && right.PerTurnBudget < left.PerTurnBudget {
		issue(id, "per_turn_budget_reduced", "candidate reduces the per-turn call budget")
	}
	if left.MaxOutputBytes > 0 && right.MaxOutputBytes > 0 && right.MaxOutputBytes < left.MaxOutputBytes {
		issue(id, "max_output_reduced", "candidate reduces the maximum output size")
	}
	if left.Execution != nil && right.Execution != nil && !left.Execution.Writes && right.Execution.Writes {
		issue(id, "write_effect_added", "candidate execution may write external state")
	}
	if !executionEquivalent(left.Execution, right.Execution) {
		issue(id, "execution_changed", "capability execution declaration changed")
	}
	if baseline.ProviderRevision != "" && baseline.ProviderRevision != candidate.ProviderRevision {
		issue(id, "provider_revision_changed", "capability provider artifact revision changed")
	}
	if left.TimeoutMs > 0 && right.TimeoutMs > 0 && right.TimeoutMs < left.TimeoutMs {
		issue(id, "timeout_reduced", "candidate reduces the capability timeout")
	}
	if compatible, reason := inputSchemaCompatible(effectiveInputSchema(left), effectiveInputSchema(right), "$", 0); !compatible {
		issue(id, "input_schema_narrowed", reason)
	}
	if !jsonEquivalent(left.OutputSchema, right.OutputSchema) {
		if len(left.OutputSchema) > 0 || len(right.OutputSchema) > 0 {
			issue(id, "output_schema_changed", "capability output schema changed")
		}
	}
}

func effectiveInputSchema(manifest core.CapabilityManifest) map[string]any {
	if manifest.Tool != nil && manifest.Tool.Parameters != nil {
		return manifest.Tool.Parameters
	}
	return manifest.InputSchema
}

func inputSchemaCompatible(baseline, candidate map[string]any, path string, depth int) (bool, string) {
	if jsonEquivalent(baseline, candidate) || len(candidate) == 0 {
		return true, ""
	}
	if len(baseline) == 0 {
		return false, path + " adds input constraints"
	}
	if depth > 32 {
		return false, path + " schema nesting exceeds compatibility limit"
	}
	if !typeSetContains(typeSet(candidate["type"]), typeSet(baseline["type"])) {
		return false, path + " narrows accepted JSON types"
	}
	baselineRequired := stringSet(baseline["required"])
	for required := range stringSet(candidate["required"]) {
		if !baselineRequired[required] {
			return false, path + "." + required + " became required"
		}
	}
	if baselineAdditional, ok := baseline["additionalProperties"].(bool); !ok || baselineAdditional {
		switch candidateAdditional := candidate["additionalProperties"].(type) {
		case bool:
			if !candidateAdditional {
				return false, path + " disables additional properties"
			}
		case map[string]any:
			return false, path + " constrains additional properties"
		}
	} else if baselineAdditionalSchema, ok := baseline["additionalProperties"].(map[string]any); ok {
		if candidateAdditionalSchema, ok := candidate["additionalProperties"].(map[string]any); ok {
			if compatible, reason := inputSchemaCompatible(baselineAdditionalSchema, candidateAdditionalSchema, path+".*", depth+1); !compatible {
				return false, reason
			}
		} else if candidateAdditional, ok := candidate["additionalProperties"].(bool); ok && !candidateAdditional {
			return false, path + " disables additional properties"
		}
	}
	baselineProperties, _ := baseline["properties"].(map[string]any)
	candidateProperties, _ := candidate["properties"].(map[string]any)
	for name, baselineProperty := range baselineProperties {
		candidateProperty, exists := candidateProperties[name]
		if !exists {
			switch additional := candidate["additionalProperties"].(type) {
			case bool:
				if !additional {
					return false, path + "." + name + " is no longer accepted"
				}
			case map[string]any:
				baselinePropertySchema, ok := baselineProperty.(map[string]any)
				if !ok {
					return false, path + "." + name + " falls back to a constrained additional-property schema"
				}
				if compatible, reason := inputSchemaCompatible(baselinePropertySchema, additional, path+"."+name, depth+1); !compatible {
					return false, reason
				}
			case nil:
				// The default accepts any JSON value.
			default:
				return false, path + "." + name + " is no longer accepted"
			}
			continue
		}
		left, leftOK := baselineProperty.(map[string]any)
		right, rightOK := candidateProperty.(map[string]any)
		if leftOK && rightOK {
			if compatible, reason := inputSchemaCompatible(left, right, path+"."+name, depth+1); !compatible {
				return false, reason
			}
		} else if !jsonEquivalent(baselineProperty, candidateProperty) {
			return false, path + "." + name + " changed incompatibly"
		}
	}
	if !enumContains(candidate["enum"], baseline["enum"]) {
		return false, path + " removes accepted enum values"
	}
	for _, constraint := range []struct {
		key     string
		higher  bool
		message string
	}{
		{"minimum", true, "raises minimum"}, {"exclusiveMinimum", true, "raises exclusive minimum"},
		{"minLength", true, "raises minimum length"}, {"minItems", true, "raises minimum item count"},
		{"minProperties", true, "raises minimum property count"},
		{"maximum", false, "lowers maximum"}, {"exclusiveMaximum", false, "lowers exclusive maximum"},
		{"maxLength", false, "lowers maximum length"}, {"maxItems", false, "lowers maximum item count"},
		{"maxProperties", false, "lowers maximum property count"},
	} {
		if tightenedNumber(baseline[constraint.key], candidate[constraint.key], constraint.higher) {
			return false, path + " " + constraint.message
		}
	}
	baselineItems, leftItems := baseline["items"].(map[string]any)
	candidateItems, rightItems := candidate["items"].(map[string]any)
	if leftItems && rightItems {
		if compatible, reason := inputSchemaCompatible(baselineItems, candidateItems, path+"[]", depth+1); !compatible {
			return false, reason
		}
	} else if leftItems != rightItems && rightItems {
		return false, path + " adds item constraints"
	}
	for _, keyword := range []string{"allOf", "anyOf", "oneOf", "not", "pattern", "format", "const"} {
		if !jsonEquivalent(baseline[keyword], candidate[keyword]) {
			return false, path + " changes " + keyword
		}
	}
	if tightenedNumber(baseline["multipleOf"], candidate["multipleOf"], true) &&
		!jsonEquivalent(baseline["multipleOf"], candidate["multipleOf"]) {
		return false, path + " changes multipleOf"
	}
	if baselineUnique, _ := baseline["uniqueItems"].(bool); !baselineUnique {
		if candidateUnique, _ := candidate["uniqueItems"].(bool); candidateUnique {
			return false, path + " requires unique items"
		}
	}
	handled := map[string]bool{
		"type": true, "required": true, "additionalProperties": true, "properties": true,
		"enum": true, "minimum": true, "exclusiveMinimum": true, "maximum": true,
		"exclusiveMaximum": true, "minLength": true, "maxLength": true,
		"minItems": true, "maxItems": true, "minProperties": true, "maxProperties": true,
		"items": true, "allOf": true, "anyOf": true, "oneOf": true, "not": true,
		"pattern": true, "format": true, "const": true, "multipleOf": true, "uniqueItems": true,
	}
	annotations := map[string]bool{
		"title": true, "description": true, "default": true, "examples": true,
		"deprecated": true, "readOnly": true, "writeOnly": true, "$schema": true, "$id": true,
	}
	for keyword, value := range candidate {
		if handled[keyword] || annotations[keyword] || jsonEquivalent(baseline[keyword], value) {
			continue
		}
		return false, path + " adds or changes unsupported constraint " + keyword
	}
	return true, ""
}

func executionEquivalent(left, right *core.ExecutionSpec) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	leftCopy, rightCopy := *left, *right
	leftCopy.Writes, rightCopy.Writes = false, false
	return jsonEquivalent(leftCopy, rightCopy)
}

func permissionStrings(values []core.Permission) []string {
	out := make([]string, len(values))
	for index, value := range values {
		out[index] = string(value)
	}
	return out
}

func credentialStrings(values []core.CredentialRef) []string {
	out := make([]string, len(values))
	for index, value := range values {
		out[index] = string(value)
	}
	return out
}

func addedStrings(baseline, candidate []string) []string {
	seen := map[string]bool{}
	for _, value := range baseline {
		seen[value] = true
	}
	out := []string{}
	for _, value := range candidate {
		if !seen[value] {
			out = append(out, value)
			seen[value] = true
		}
	}
	sort.Strings(out)
	return out
}

func stringSet(value any) map[string]bool {
	out := map[string]bool{}
	switch values := value.(type) {
	case []string:
		for _, item := range values {
			out[item] = true
		}
	case []any:
		for _, item := range values {
			if text, ok := item.(string); ok {
				out[text] = true
			}
		}
	}
	return out
}

func typeSet(value any) map[string]bool {
	out := map[string]bool{}
	switch typed := value.(type) {
	case nil:
		return out
	case string:
		out[typed] = true
	case []any:
		for _, item := range typed {
			if text, ok := item.(string); ok {
				out[text] = true
			}
		}
	case []string:
		for _, item := range typed {
			out[item] = true
		}
	}
	return out
}

func typeSetContains(candidate, baseline map[string]bool) bool {
	if len(candidate) == 0 || len(baseline) == 0 {
		return len(candidate) == 0
	}
	for value := range baseline {
		if !candidate[value] {
			return false
		}
	}
	return true
}

func enumContains(candidate, baseline any) bool {
	baselineValues := jsonArray(baseline)
	if len(baselineValues) == 0 {
		return true
	}
	candidateValues := jsonArray(candidate)
	if candidateValues == nil {
		return true
	}
	for _, expected := range baselineValues {
		found := false
		for _, actual := range candidateValues {
			if jsonEquivalent(expected, actual) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func jsonArray(value any) []any {
	switch typed := value.(type) {
	case []any:
		return typed
	case []string:
		out := make([]any, len(typed))
		for index, item := range typed {
			out[index] = item
		}
		return out
	default:
		return nil
	}
}

func tightenedNumber(baseline, candidate any, higherIsTighter bool) bool {
	left, leftOK := jsonNumber(baseline)
	right, rightOK := jsonNumber(candidate)
	if !rightOK {
		return false
	}
	if !leftOK {
		return true
	}
	if higherIsTighter {
		return right > left
	}
	return right < left
}

func jsonNumber(value any) (float64, bool) {
	switch typed := value.(type) {
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case json.Number:
		value, err := typed.Float64()
		return value, err == nil
	default:
		return 0, false
	}
}

func jsonEquivalent(left, right any) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && string(leftJSON) == string(rightJSON)
}
