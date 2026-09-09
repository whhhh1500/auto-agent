package programmatic

import (
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/whhhh1500/auto-agent/pkg/core"
	ptc "github.com/whhhh1500/auto-agent/pkg/execution/programmatic"
)

func TestProbePlanBuildsCandidateBoundChoiceTools(t *testing.T) {
	plan := testProbePlan(t)
	if !validDigest(plan.Digest()) || !validDigest(plan.ExecuteSchemaDigest()) {
		t.Fatalf("plan digests are not canonical SHA-256 values: %#v", plan)
	}
	tools, err := plan.ChoiceTools()
	if err != nil {
		t.Fatal(err)
	}
	if len(tools) != 2 || tools[0].Name != "records.detail" || tools[1].Name != DefaultExecuteToolID {
		t.Fatalf("choice tools = %#v", tools)
	}
	if tools[0].Parameters["type"] != "object" || !strings.Contains(tools[1].Description, plan.followupBinding) ||
		tools[1].Parameters["properties"].(map[string]any)["projection"].(map[string]any)["enum"] == nil {
		t.Fatalf("dynamic tool lacks fixed route material: %#v", tools[1])
	}
	if _, present := tools[1].Parameters["properties"].(map[string]any)["source"]; present {
		t.Fatal("dynamic tool exposed model-provided source")
	}
	if _, exists := tools[1].Parameters["x-harness-probe-plan"]; exists {
		t.Fatal("dynamic schema retained a gateway-sensitive extension keyword")
	}

	tools[0].Parameters["type"] = "string"
	facts := plan.Facts()
	facts.Candidates[0].Args["id"] = "forged"
	if plan.DirectToolSchema().Parameters["type"] != "object" || plan.Facts().Candidates[0].Args["id"] != "record-17" {
		t.Fatal("returned plan values aliased internal state")
	}
}

func TestProbePlanDynamicExecuteDescriptionStartsWithRouteGuidance(t *testing.T) {
	tools, err := testProbePlan(t).ChoiceTools()
	if err != nil {
		t.Fatal(err)
	}
	const wantRouteGuidance = "Routing guidance: When many or all candidates are needed, follow-up work would repeat, or outputs create context pressure, prefer a single bounded execute call. For a small, deliberate subset of candidates, call the follow-up tool directly. This is guidance: you retain the route choice and must satisfy task coverage."
	if got := strings.TrimSuffix(strings.SplitN(tools[1].Description, " The host executes a fixed bounded program", 2)[0], " "); got != wantRouteGuidance {
		t.Fatalf("dynamic route guidance=%q, want %q", got, wantRouteGuidance)
	}
}

func TestProbePlanDirectDescriptionGuidesOneLockedBatchWithoutDataLeakage(t *testing.T) {
	probe, followup, result := testProbeInputs()
	followup.Manifest.Tool.Description = "Read frozen record details."
	result.Content = "probe-content-must-not-appear"
	facts := result.Metadata[ProbeFactsMetadataKey].(map[string]any)
	facts["candidates"].([]any)[0].(map[string]any)["args"] = map[string]any{"id": "candidate-args-must-not-appear"}
	facts["candidates"].([]any)[0].(map[string]any)["facts"] = map[string]any{"note": "candidate-facts-must-not-appear"}
	plan, err := NewProbePlan(probe, followup, result, ProbePlanOptions{ProgramContract: testProbeProgramContract()})
	if err != nil {
		t.Fatal(err)
	}
	direct := plan.DirectToolSchema()
	description := direct.Description
	if !strings.HasPrefix(description, "Read frozen record details.") {
		t.Fatalf("follow-up description was not retained: %q", description)
	}
	for _, want := range []string{
		"unique Direct batch", "choosing Direct locks the route", "same assistant response", "no later tool round", "There are 1 host-approved candidates", "small, deliberate subset", "many or all candidates", "context pressure", "eligible bounded execute tool",
	} {
		if !strings.Contains(description, want) {
			t.Fatalf("direct guidance missing %q: %q", want, description)
		}
	}
	if len(description) > MaxProbeSchemaBytes {
		t.Fatalf("direct description exceeds schema bound: %d", len(description))
	}
	for _, forbidden := range []string{"candidate-args-must-not-appear", "candidate-facts-must-not-appear", "probe-content-must-not-appear", plan.followupBinding} {
		if strings.Contains(description, forbidden) {
			t.Fatalf("direct guidance leaked %q: %q", forbidden, description)
		}
	}
	if err := plan.ValidateDirect(map[string]any{"id": "candidate-args-must-not-appear"}); err != nil {
		t.Fatalf("approved direct candidate was rejected: %v", err)
	}
	if err := plan.ValidateDirect(map[string]any{"id": "forged"}); !errors.Is(err, ErrProbeCandidateRejected) {
		t.Fatalf("guidance weakened direct validation: %v", err)
	}
	otherProbe, otherFollowup, otherResult := testProbeInputs()
	otherFollowup.Manifest.Tool.Description = "Different frozen record description."
	otherResult.Content = "probe-content-must-not-appear"
	otherFacts := otherResult.Metadata[ProbeFactsMetadataKey].(map[string]any)
	otherFacts["candidates"].([]any)[0].(map[string]any)["args"] = map[string]any{"id": "candidate-args-must-not-appear"}
	otherFacts["candidates"].([]any)[0].(map[string]any)["facts"] = map[string]any{"note": "candidate-facts-must-not-appear"}
	otherPlan, err := NewProbePlan(otherProbe, otherFollowup, otherResult, ProbePlanOptions{ProgramContract: testProbeProgramContract()})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Digest() == otherPlan.Digest() {
		t.Fatal("plan digest does not include the guided direct schema")
	}
}

func TestProbePlanDirectDescriptionRequiresSingleBatchWhenExecuteIsIneligible(t *testing.T) {
	probe, followup, result := testProbeInputs()
	followup.Manifest.OutputSchema = map[string]any{"type": "object", "properties": map[string]any{"unprojectable": map[string]any{"type": "number"}}}
	plan, err := NewProbePlan(probe, followup, result, ProbePlanOptions{ProgramContract: testProbeProgramContract()})
	if err != nil {
		t.Fatal(err)
	}
	tools, err := plan.ChoiceTools()
	if err != nil || len(tools) != 1 {
		t.Fatalf("ineligible execute choice tools=%#v err=%v", tools, err)
	}
	description := tools[0].Description
	if !strings.Contains(description, "No eligible execute tool is available") || !strings.Contains(description, "same Direct response") {
		t.Fatalf("direct-only route guidance=%q", description)
	}
	if strings.Contains(description, "eligible bounded execute tool instead") {
		t.Fatalf("direct-only guidance offered execute: %q", description)
	}
}

func TestProbeFactsStrictParsingAndBounds(t *testing.T) {
	base := testProbeResult()
	for _, test := range []struct {
		name   string
		mutate func(map[string]any)
	}{
		{name: "unknown field", mutate: func(facts map[string]any) { facts["extra"] = true }},
		{name: "unknown candidate field", mutate: func(facts map[string]any) { facts["candidates"].([]any)[0].(map[string]any)["extra"] = true }},
		{name: "wrong version", mutate: func(facts map[string]any) { facts["version"] = "2" }},
		{name: "empty candidates", mutate: func(facts map[string]any) { facts["candidates"] = []any{} }},
		{name: "too long label", mutate: func(facts map[string]any) {
			facts["candidates"].([]any)[0].(map[string]any)["label"] = strings.Repeat("x", MaxProbeLabelBytes+1)
		}},
		{name: "invalid model limit", mutate: func(facts map[string]any) { facts["max_model_return_bytes"] = float64(MaxProbeModelReturnBytes + 1) }},
		{name: "non integer model limit", mutate: func(facts map[string]any) { facts["max_model_return_bytes"] = 1.5 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			result := cloneProbeResult(t, base)
			facts := result.Metadata[ProbeFactsMetadataKey].(map[string]any)
			test.mutate(facts)
			if _, err := ParseProbeFacts(result); !errors.Is(err, ErrInvalidProbeFacts) {
				t.Fatalf("ParseProbeFacts error=%v", err)
			}
		})
	}

	result := testProbeResult()
	result.Metadata["unrelated"] = "must not silently alter the canonical receipt"
	if _, err := ParseProbeFacts(result); !errors.Is(err, ErrInvalidProbeFacts) {
		t.Fatalf("unexpected result metadata error=%v", err)
	}

	cyclic := map[string]any{}
	cyclic["self"] = cyclic
	result = testProbeResult()
	result.Metadata[ProbeFactsMetadataKey] = cyclic
	if _, err := ParseProbeFacts(result); !errors.Is(err, ErrInvalidProbeFacts) {
		t.Fatalf("cyclic probe metadata error=%v", err)
	}
	result = testProbeResult()
	result.Metadata[ProbeFactsMetadataKey].(map[string]any)["candidates"].([]any)[0].(map[string]any)["facts"] = math.NaN()
	if _, err := ParseProbeFacts(result); !errors.Is(err, ErrInvalidProbeFacts) {
		t.Fatalf("NaN probe facts error=%v", err)
	}
}

func TestProbePlanEnforcesAggregateAndSchemaBounds(t *testing.T) {
	if MaxProbeFactsBytes > MaxProbeOutputBytes || MaxProbeModelReturnBytes > MaxProbeOutputBytes {
		t.Fatalf("probe context ceilings exceed frozen probe output ceiling: facts=%d return=%d output=%d", MaxProbeFactsBytes, MaxProbeModelReturnBytes, MaxProbeOutputBytes)
	}
	probe, followup, result := testProbeInputs()
	for _, test := range []struct {
		name   string
		mutate func(*core.SnapshotCapability, *core.SnapshotCapability, *core.CapabilityResult)
	}{
		{name: "probe content exceeds its frozen limit", mutate: func(probe *core.SnapshotCapability, _ *core.SnapshotCapability, result *core.CapabilityResult) {
			result.Content = strings.Repeat("x", probe.Manifest.MaxOutputBytes+1)
		}},
		{name: "candidate count", mutate: func(_ *core.SnapshotCapability, _ *core.SnapshotCapability, result *core.CapabilityResult) {
			candidates := make([]any, MaxProbeCandidates+1)
			for index := range candidates {
				candidates[index] = map[string]any{"label": "record", "args": map[string]any{"id": "record"}}
			}
			result.Metadata[ProbeFactsMetadataKey].(map[string]any)["candidates"] = candidates
		}},
		{name: "candidate encoded bytes", mutate: func(_ *core.SnapshotCapability, _ *core.SnapshotCapability, result *core.CapabilityResult) {
			result.Metadata[ProbeFactsMetadataKey].(map[string]any)["candidates"].([]any)[0].(map[string]any)["args"] = map[string]any{"id": strings.Repeat("x", MaxProbeCandidateBytes)}
		}},
		{name: "facts aggregate bytes", mutate: func(_ *core.SnapshotCapability, _ *core.SnapshotCapability, result *core.CapabilityResult) {
			candidates := make([]any, 5)
			for index := range candidates {
				candidates[index] = map[string]any{
					"label": "record", "args": map[string]any{"id": "record"}, "facts": strings.Repeat("x", MaxProbeCandidateBytes-512),
				}
			}
			result.Metadata[ProbeFactsMetadataKey].(map[string]any)["candidates"] = candidates
		}},
		{name: "facts JSON depth", mutate: func(_ *core.SnapshotCapability, _ *core.SnapshotCapability, result *core.CapabilityResult) {
			result.Metadata[ProbeFactsMetadataKey].(map[string]any)["candidates"].([]any)[0].(map[string]any)["facts"] = nestedProbeValue(MaxProbeJSONDepth + 1)
		}},
		{name: "dynamic description and parameters", mutate: func(_ *core.SnapshotCapability, followup *core.SnapshotCapability, _ *core.CapabilityResult) {
			followup.Manifest.OutputSchema = map[string]any{"type": "object", "description": strings.Repeat("x", MaxProbeSchemaBytes)}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidateProbe, candidateFollowup, candidateResult := cloneProbeInputs(t, probe, followup, result)
			test.mutate(&candidateProbe, &candidateFollowup, &candidateResult)
			plan, err := NewProbePlan(candidateProbe, candidateFollowup, candidateResult, ProbePlanOptions{ProgramContract: testProbeProgramContract()})
			if test.name == "dynamic description and parameters" {
				tools, toolErr := plan.ChoiceTools()
				if err != nil || toolErr != nil || len(tools) != 1 {
					t.Fatal("unprojectable schema did not fall back to direct")
				}
			} else if err == nil {
				t.Fatal("NewProbePlan accepted an out-of-contract bounded value")
			}
		})
	}
}

func TestProbePlanRequiresProgramContract(t *testing.T) {
	probe, followup, result := testProbeInputs()
	if _, err := NewProbePlan(probe, followup, result, ProbePlanOptions{}); !errors.Is(err, ErrInvalidProbePlan) {
		t.Fatalf("NewProbePlan without a program contract error=%v", err)
	}
}

func TestProbePlanRejectsUnsafeManifestAndCandidateContracts(t *testing.T) {
	probe, followup, result := testProbeInputs()
	for _, test := range []struct {
		name   string
		mutate func(*core.SnapshotCapability, *core.SnapshotCapability, *core.CapabilityResult)
	}{
		{name: "probe is not a tool", mutate: func(probe *core.SnapshotCapability, _ *core.SnapshotCapability, _ *core.CapabilityResult) {
			probe.Manifest.Kind = core.KindKnowledge
		}},
		{name: "probe not idempotent", mutate: func(probe *core.SnapshotCapability, _ *core.SnapshotCapability, _ *core.CapabilityResult) {
			probe.Manifest.Idempotent = false
		}},
		{name: "probe exposed programmatically", mutate: func(probe *core.SnapshotCapability, _ *core.SnapshotCapability, _ *core.CapabilityResult) {
			probe.Manifest.Metadata[ExposureKey] = ExposureVersion
		}},
		{name: "probe output ceiling", mutate: func(probe *core.SnapshotCapability, _ *core.SnapshotCapability, _ *core.CapabilityResult) {
			probe.Manifest.MaxOutputBytes = MaxProbeOutputBytes + 1
		}},
		{name: "followup approval", mutate: func(_ *core.SnapshotCapability, followup *core.SnapshotCapability, _ *core.CapabilityResult) {
			followup.Manifest.RequiresApproval = true
		}},
		{name: "followup is not a tool", mutate: func(_ *core.SnapshotCapability, followup *core.SnapshotCapability, _ *core.CapabilityResult) {
			followup.Manifest.Kind = core.KindKnowledge
		}},
		{name: "followup write permission", mutate: func(_ *core.SnapshotCapability, followup *core.SnapshotCapability, _ *core.CapabilityResult) {
			followup.Manifest.RequiredPermissions = []core.Permission{"records.write"}
		}},
		{name: "followup write sandbox", mutate: func(_ *core.SnapshotCapability, followup *core.SnapshotCapability, _ *core.CapabilityResult) {
			followup.Manifest.Execution = &core.ExecutionSpec{Sandbox: core.SandboxPolicy{Mode: core.SandboxWorkspaceWrite}}
		}},
		{name: "candidate violates input schema", mutate: func(_ *core.SnapshotCapability, _ *core.SnapshotCapability, result *core.CapabilityResult) {
			result.Metadata[ProbeFactsMetadataKey].(map[string]any)["candidates"].([]any)[0].(map[string]any)["args"] = map[string]any{"id": float64(7)}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidateProbe, candidateFollowup, candidateResult := cloneProbeInputs(t, probe, followup, result)
			test.mutate(&candidateProbe, &candidateFollowup, &candidateResult)
			if _, err := NewProbePlan(candidateProbe, candidateFollowup, candidateResult, ProbePlanOptions{ProgramContract: testProbeProgramContract()}); !errors.Is(err, ErrInvalidProbePlan) {
				t.Fatalf("NewProbePlan error=%v", err)
			}
		})
	}
}

func TestProbePlanValidatesDirectAndPreparesCandidateBoundPTC(t *testing.T) {
	plan := testProbePlan(t)
	if err := plan.ValidateDirect(map[string]any{"id": "record-17"}); err != nil {
		t.Fatal(err)
	}
	if err := plan.ValidateDirect(map[string]any{"id": "record-18"}); !errors.Is(err, ErrProbeCandidateRejected) {
		t.Fatalf("forged direct error=%v", err)
	}

	prepared, err := plan.PrepareExecute(map[string]any{"projection": "name", "selection": []any{float64(0)}})
	if err != nil {
		t.Fatal(err)
	}
	bindings := prepared["bindings"].(map[string]any)
	if len(bindings) != 1 || bindings["records.detail"] != plan.followupBinding {
		t.Fatalf("bindings=%#v", bindings)
	}
	targets := prepared["input"].(map[string]any)["targets"].([]any)
	if len(targets) != 1 || targets[0].(map[string]any)["args"].(map[string]any)["id"] != "record-17" {
		t.Fatalf("targets=%#v", targets)
	}
	targets[0].(map[string]any)["args"].(map[string]any)["id"] = "forged"
	if plan.Facts().Candidates[0].Args["id"] != "record-17" {
		t.Fatal("prepared generic call aliased plan candidate")
	}

	for _, args := range []map[string]any{
		{"projection": "name", "selection": []any{float64(1)}},
		{"projection": "name", "selection": []any{float64(0), float64(0)}},
		{"projection": "forged", "selection": []any{float64(0)}},
		{"source": testForgedPTCSource(t, "records.detail"), "projection": "name", "selection": []any{float64(0)}},
	} {
		if _, err := plan.PrepareExecute(args); !errors.Is(err, ErrInvalidProbePlan) {
			t.Fatalf("PrepareExecute(%#v) error=%v", args, err)
		}
	}
}

func TestProbePlanBoundsReturnedExecuteValue(t *testing.T) {
	plan := testProbePlan(t)
	if err := plan.ValidateReturn(core.CapabilityResult{Content: `{"value":["Item 17"],"steps":7,"tool_calls":1,"arena_bytes":9}`, OK: true}); err != nil {
		t.Fatal(err)
	}
	if err := plan.ValidateReturn(core.CapabilityResult{Content: strings.Repeat("x", plan.Facts().MaxModelReturnBytes), OK: true}); !errors.Is(err, ErrProbeReturnRejected) {
		t.Fatalf("oversized return error=%v", err)
	}
	if err := plan.ValidateReturn(core.CapabilityResult{Content: "ok", Metadata: map[string]any{"bad": make(chan int)}}); !errors.Is(err, ErrProbeReturnRejected) {
		t.Fatalf("hostile metadata error=%v", err)
	}
}

func testProbePlan(t *testing.T) ProbePlan {
	t.Helper()
	probe, followup, result := testProbeInputs()
	plan, err := NewProbePlan(probe, followup, result, ProbePlanOptions{ExecuteToolID: DefaultExecuteToolID, ProgramContract: testProbeProgramContract()})
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func testProbeInputs() (core.SnapshotCapability, core.SnapshotCapability, core.CapabilityResult) {
	schema := map[string]any{
		"type": "object", "additionalProperties": false,
		"required": []any{"id"}, "properties": map[string]any{"id": map[string]any{"type": "string", "minLength": 1}},
	}
	probe := core.SnapshotCapability{Manifest: core.CapabilityManifest{
		ID: "records.inventory", Version: "1.0.0", Name: "Inventory", Kind: core.KindTool, Idempotent: true,
		MaxOutputBytes: 4096, Tool: &core.ToolExposure{Description: "List records", Parameters: map[string]any{"type": "object", "additionalProperties": false}},
		Metadata: map[string]string{ProbeManifestKey: ProbeManifestVersion},
	}, ProviderRevision: "inventory-v1"}
	followup := core.SnapshotCapability{Manifest: core.CapabilityManifest{
		ID: "records.detail", Version: "1.0.0", Name: "Detail", Kind: core.KindTool, Idempotent: true,
		InputSchema: schema, Tool: &core.ToolExposure{Description: "Read record", Parameters: schema},
		OutputSchema: map[string]any{"type": "object", "required": []any{"id", "name", "padding"}, "additionalProperties": false, "properties": map[string]any{
			"id": map[string]any{"type": "string", "maxLength": 16}, "name": map[string]any{"type": "string", "maxLength": 64}, "padding": map[string]any{"type": "string", "maxLength": 4096},
		}},
		Metadata: map[string]string{ExposureKey: ExposureVersion},
	}, ProviderRevision: "detail-v1"}
	return probe, followup, testProbeResult()
}

func testProbeResult() core.CapabilityResult {
	return core.CapabilityResult{OK: true, Content: "inventory", Metadata: map[string]any{
		ProbeFactsMetadataKey: map[string]any{
			"version": ProbeFactsVersion, "followup_capability_id": "records.detail", "max_model_return_bytes": float64(4096),
			"candidates": []any{map[string]any{"label": "record-17", "args": map[string]any{"id": "record-17"}, "facts": map[string]any{"active": true}}},
		},
	}}
}

func testProbeProgramContract() ProbeProgramContract {
	return ProbeProgramContract{
		Version:        ptc.Version,
		LanguageGuide:  ptc.LanguageGuide,
		MaxSourceBytes: ptc.HardMaxSourceBytes,
		MaxToolCalls:   int(ptc.DefaultMaxToolCalls),
		ValidateSource: func(source string) ([]string, error) {
			program, err := ptc.Compile([]byte(source))
			if err != nil {
				return nil, err
			}
			return program.Tools(), nil
		},
	}
}

func nestedProbeValue(depth int) any {
	value := any("leaf")
	for range depth {
		value = map[string]any{"next": value}
	}
	return value
}

func testCandidatePTCSource(t *testing.T, followup string) string {
	t.Helper()
	return testPTCSource(t, followup, map[string]any{
		"op": "get", "object": map[string]any{"op": "var", "name": "target"}, "key": "args",
	})
}

func testForgedPTCSource(t *testing.T, followup string) string {
	t.Helper()
	return testPTCSource(t, followup, map[string]any{
		"op": "map", "entries": map[string]any{"id": map[string]any{"op": "literal", "value": "record-18"}},
	})
}

func testPTCSource(t *testing.T, followup string, callArgs map[string]any) string {
	t.Helper()
	value := map[string]any{"version": ptc.Version, "body": []any{
		map[string]any{"op": "for", "var": "target", "in": map[string]any{
			"op": "get", "object": map[string]any{"op": "var", "name": "input"}, "key": "targets",
		}, "body": []any{map[string]any{
			"op": "call", "assign": "detail", "tool": followup, "args": callArgs,
		}}},
		map[string]any{"op": "return", "value": map[string]any{"op": "literal", "value": nil}},
	}}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

func cloneProbeResult(t *testing.T, result core.CapabilityResult) core.CapabilityResult {
	t.Helper()
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	var copy core.CapabilityResult
	if err := json.Unmarshal(encoded, &copy); err != nil {
		t.Fatal(err)
	}
	return copy
}

func cloneProbeInputs(t *testing.T, probe, followup core.SnapshotCapability, result core.CapabilityResult) (core.SnapshotCapability, core.SnapshotCapability, core.CapabilityResult) {
	t.Helper()
	encoded, err := json.Marshal(probe.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	probeCopy := probe
	if err := json.Unmarshal(encoded, &probeCopy.Manifest); err != nil {
		t.Fatal(err)
	}
	encoded, err = json.Marshal(followup.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	followupCopy := followup
	if err := json.Unmarshal(encoded, &followupCopy.Manifest); err != nil {
		t.Fatal(err)
	}
	return probeCopy, followupCopy, cloneProbeResult(t, result)
}
