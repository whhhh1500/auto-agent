package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	appcontextassembly "github.com/whhhh1500/auto-agent/pkg/app/contextassembly"
	core "github.com/whhhh1500/auto-agent/pkg/core"
)

func TestLargeDetailFixtureCatalogContractAndExactOutputLimit(t *testing.T) {
	const activeRows = 8
	model := &typedCatalogProbeModel{}
	fixture, err := newFixtureWithDataOptions(model, true, activeRows, fixtureDataOptions{
		OutputContracts:       true,
		DetailIrrelevantBytes: 4 << 10,
	})
	if err != nil {
		t.Fatal("could not construct large-detail fixture")
	}
	result, err := fixture.runtime.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{RunID: "large-detail-catalog", Text: "inspect catalog"}, nil)
	if err != nil || result.Status != core.RunCompleted {
		t.Fatal("large-detail catalog probe did not complete")
	}
	schema := typedCatalogOutputSchema(model.catalog, detailToolID)
	if len(schema) == 0 {
		t.Fatal("large-detail catalog did not expose a detail output schema")
	}
	properties, _ := schema["properties"].(map[string]any)
	if field, _ := properties["irrelevant_read_only_text"].(map[string]any); field["type"] != "string" {
		t.Fatal("large-detail schema did not describe the irrelevant text field")
	}
	manifest := fixture.detail.Manifest()
	if manifest.MaxOutputBytes <= 4<<10 || typedCatalogMaxOutputBytes(model.catalog, detailToolID) != manifest.MaxOutputBytes {
		t.Fatal("large-detail leaf output limit was not published exactly")
	}
	maxSeen := 0
	for index := 1; index <= activeRows; index++ {
		content, err := fixture.detail.execute(map[string]any{"id": fmt.Sprintf("item-%02d", index)})
		if err != nil || len(content) > manifest.MaxOutputBytes {
			t.Fatal("large-detail payload exceeded its declared output limit")
		}
		if len(content) > maxSeen {
			maxSeen = len(content)
		}
		var value any
		if json.Unmarshal([]byte(content), &value) != nil || core.ValidateJSONValue(schema, value) != nil {
			t.Fatal("large-detail payload did not satisfy its published schema")
		}
		object, _ := value.(map[string]any)
		text, _ := object["irrelevant_read_only_text"].(string)
		if len(text) != 4<<10 || !strings.Contains(text, "synthetic_read_only_record=") || !strings.Contains(text, "checksum=") {
			t.Fatal("large-detail text was not the fixed synthetic 4KiB fixture")
		}
	}
	if maxSeen != manifest.MaxOutputBytes {
		t.Fatalf("declared detail limit=%d, largest actual output=%d", manifest.MaxOutputBytes, maxSeen)
	}

	baseline, err := newFixtureWithOutputContracts(&typedCatalogProbeModel{}, true, activeRows, true)
	if err != nil {
		t.Fatal("could not construct compact typed fixture")
	}
	compact := baseline.detail.Manifest()
	compactProperties, _ := compact.OutputSchema["properties"].(map[string]any)
	if compact.MaxOutputBytes != 0 || compactProperties["irrelevant_read_only_text"] != nil {
		t.Fatal("zero-padding fixture changed its existing manifest")
	}
}

func TestLargeDetailTwoKiBControlRetainsAllDirectResultsInFinalContext(t *testing.T) {
	const activeRows = 8
	contexts := &largeDetailContextRecorder{}
	model := &largeDetailBatchedDirectModel{}
	fixture, err := newFixtureWithDataOptions(model, true, activeRows, fixtureDataOptions{
		OutputContracts:         true,
		DetailIrrelevantBytes:   2 << 10,
		ObserveAssembledContext: contexts.observe,
	})
	if err != nil {
		t.Fatal("could not construct 2KiB direct control")
	}
	contexts.setSession(fixture.session)
	result, err := fixture.runtime.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{RunID: "large-detail-2k-direct", Text: liveLargeDetailPrompt(activeRows)}, nil)
	if err != nil || result.Status != core.RunCompleted {
		t.Fatal("2KiB direct control did not complete")
	}
	evidence := contexts.evidence()
	if !evidence.FinalContextObserved || !evidence.FinalContextAssemblySucceeded || evidence.FinalContextBudgetExceeded || !evidence.FinalContextSlotsObserved || !evidence.ModelVisibleResultsComplete || !evidence.FinalContextDroppedObserved || evidence.FinalContextDroppedGroups != 0 || evidence.ExpectedModelVisibleResults != 9 || evidence.PresentModelVisibleResults != 9 {
		t.Fatalf("2KiB direct final context did not retain all results: %#v", evidence)
	}
}

// TestLargeDetailAdapterLimitsForwardingUsesRuntimeRequestLimits proves
// liveModel forwards validated adapter limits to Runtime. The fallback arm
// uses an explicit adapter that suppresses the optional report; it does not
// rely on instrumentation accidentally dropping a supported capability.
func TestLargeDetailAdapterLimitsForwardingUsesRuntimeRequestLimits(t *testing.T) {
	const (
		reportedWindow = 128_000
		reportedOutput = 4_096
	)
	for _, test := range []struct {
		name       string
		suppress   bool
		wantWindow int
		wantOutput int
	}{
		{name: "forwarded", wantWindow: reportedWindow, wantOutput: reportedOutput},
		{name: "explicit_no_limits_fallback", suppress: true, wantWindow: 32_768, wantOutput: 4_096},
	} {
		t.Run(test.name, func(t *testing.T) {
			inner := &largeDetailLimitReporterModel{
				largeDetailSystemCaptureModel: &largeDetailSystemCaptureModel{},
				contextWindowTokens:           reportedWindow,
				maxOutputTokens:               reportedOutput,
			}
			live := newLiveModel(t, inner, "offline-adapter-limits-"+test.name, 1, nil)
			var adapter core.LlmAdapter = live
			if test.suppress {
				adapter = suppressModelContextLimitsAdapter{LlmAdapter: live}
			}
			contexts := &largeDetailContextRecorder{}
			fixture, err := newFixtureWithDataOptions(adapter, true, 8, fixtureDataOptions{
				OutputContracts:         true,
				DetailIrrelevantBytes:   2 << 10,
				ObserveAssembledContext: contexts.observe,
			})
			if err != nil {
				t.Fatal("could not construct adapter-limits fixture")
			}
			contexts.setSession(fixture.session)
			result, err := fixture.runtime.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{RunID: "offline-adapter-limits-" + test.name, Text: liveLargeDetailPrompt(8)}, nil)
			if err != nil || result.Status != core.RunCompleted {
				t.Fatal("adapter-limits fixture did not complete")
			}
			evidence := contexts.evidence()
			if !evidence.FinalContextLimitsObserved || evidence.FinalContextWindowTokens != test.wantWindow || evidence.FinalContextMaxOutputTokens != test.wantOutput {
				t.Fatalf("Runtime assembler limits=%#v, want window=%d output=%d", evidence, test.wantWindow, test.wantOutput)
			}
		})
	}
}

// TestLargeDetailAdapterLimitsRejectsFallbackAsLiveAcceptanceEvidence keeps a
// missing or malformed adapter report from being mistaken for Core's valid
// conservative fallback by the opt-in live acceptance cases.
func TestLargeDetailAdapterLimitsRejectsFallbackAsLiveAcceptanceEvidence(t *testing.T) {
	for _, test := range []struct {
		name  string
		inner core.LlmAdapter
	}{
		{name: "missing_reporter", inner: &largeDetailSystemCaptureModel{}},
		{name: "zero_window", inner: &largeDetailLimitReporterModel{largeDetailSystemCaptureModel: &largeDetailSystemCaptureModel{}, contextWindowTokens: 0, maxOutputTokens: 4_096}},
		{name: "output_equals_window", inner: &largeDetailLimitReporterModel{largeDetailSystemCaptureModel: &largeDetailSystemCaptureModel{}, contextWindowTokens: 4_096, maxOutputTokens: 4_096}},
	} {
		t.Run(test.name, func(t *testing.T) {
			live := newLiveModel(t, test.inner, "offline-invalid-adapter-limits-"+test.name, 1, nil)
			if _, ok := liveAdapterLimits(live); ok {
				t.Fatal("invalid adapter report was accepted as live adapter-limits evidence")
			}
		})
	}
}

func TestLargeDetailDirectOnlyAdapterLimitsRetainsAllFourKiBResults(t *testing.T) {
	direct := &largeDetailBatchedDirectModel{}
	inner := &largeDetailLimitReporterAdapter{
		LlmAdapter:          direct,
		contextWindowTokens: 128_000,
		maxOutputTokens:     4_096,
	}
	live := newLiveModel(t, inner, "offline-direct-only-adapter-limits", 10, nil)
	var adapter core.LlmAdapter = live
	contexts := &largeDetailContextRecorder{}
	fixture, err := newFixtureWithDataOptions(adapter, false, 8, fixtureDataOptions{
		OutputContracts:         true,
		DetailIrrelevantBytes:   4 << 10,
		ExposeOutputSize:        true,
		ObserveAssembledContext: contexts.observe,
	})
	if err != nil {
		t.Fatal("could not construct direct-only adapter-limits fixture")
	}
	contexts.setSession(fixture.session)
	if err := configureLiveLargeDetailDirectOnlyProfile(fixture, adapter.Provider(), "offline-direct-only-adapter-limits", 10, 11, "forced_batched_direct_only"); err != nil {
		t.Fatal("could not configure direct-only adapter-limits profile")
	}
	result, err := fixture.runtime.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{RunID: "offline-direct-only-adapter-limits", Text: liveLargeDetailPrompt(8)}, nil)
	if err != nil || result.Status != core.RunCompleted {
		t.Fatal("direct-only adapter-limits fixture did not complete")
	}
	if fixture.list.calls() != 1 || fixture.detail.calls() != 8 {
		t.Fatalf("direct-only fixture effects inventory=%d detail=%d, want 1 and 8", fixture.list.calls(), fixture.detail.calls())
	}
	evidence := contexts.evidence()
	if !evidence.FinalContextLimitsObserved || evidence.FinalContextWindowTokens != 128_000 || evidence.FinalContextMaxOutputTokens != 4_096 || !evidence.FinalContextAssemblySucceeded || evidence.FinalContextBudgetExceeded || !evidence.FinalContextSlotsObserved || !evidence.ModelVisibleResultsComplete || !evidence.FinalContextDroppedObserved || evidence.FinalContextDroppedGroups != 0 || evidence.ExpectedModelVisibleResults != 9 || evidence.PresentModelVisibleResults != 9 {
		t.Fatalf("direct-only adapter-limits final context=%#v, want complete nine-result context", evidence)
	}
	firstRounds := live.evidenceRounds()
	if len(firstRounds) == 0 || len(firstRounds[0].toolSchemaNames) != 2 {
		t.Fatalf("direct-only Runtime first menu=%#v, want two fixture tools", firstRounds)
	}
	menu := map[string]struct{}{}
	for _, name := range firstRounds[0].toolSchemaNames {
		menu[name] = struct{}{}
	}
	if _, found := menu[listToolID]; !found {
		t.Fatal("direct-only Runtime first menu omitted inventory")
	}
	if _, found := menu[detailToolID]; !found {
		t.Fatal("direct-only Runtime first menu omitted detail")
	}
	if strings.Contains(direct.system, "program.catalog") || strings.Contains(direct.system, "program.execute") {
		t.Fatal("direct-only Runtime assembled a program-selection guide without program capabilities")
	}
}

func TestLargeDetailFourKiBDirectControlReportsRequiredContextBudgetFailure(t *testing.T) {
	const activeRows = 8
	contexts := &largeDetailContextRecorder{}
	model := &largeDetailBatchedDirectModel{}
	fixture, err := newFixtureWithDataOptions(model, true, activeRows, fixtureDataOptions{
		OutputContracts:         true,
		DetailIrrelevantBytes:   4 << 10,
		ObserveAssembledContext: contexts.observe,
	})
	if err != nil {
		t.Fatal("could not construct 4KiB direct pressure fixture")
	}
	contexts.setSession(fixture.session)
	_, err = fixture.runtime.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{RunID: "large-detail-4k-direct", Text: liveLargeDetailPrompt(activeRows)}, nil)
	if !errors.Is(err, appcontextassembly.ErrBudgetExceeded) {
		t.Fatal("4KiB direct pressure did not fail through the bounded context budget")
	}
	evidence := contexts.evidence()
	if !evidence.FinalContextObserved || evidence.FinalContextAssemblySucceeded || !evidence.FinalContextBudgetExceeded || evidence.FinalContextSlotsObserved || evidence.FinalContextDroppedObserved || evidence.ExpectedModelVisibleResults != 9 {
		t.Fatalf("4KiB direct budget failure was represented as dropped or complete context: %#v", evidence)
	}
}

func TestLargeDetailEvidenceKeepsOnlySafeVariantAndContextCounts(t *testing.T) {
	const payloadMarker = "synthetic_read_only_record=000 checksum=never-persist"
	record := liveEvidenceFromCase(liveCase{
		name: "large-detail-safe-evidence", fixtureVariant: largeDetailFixtureVariant,
		detailPaddingBytes: 4 << 10, strategyLabel: "forced_ptc",
	}, core.TurnResult{RunID: "large-detail-evidence", Status: core.RunFailed}, "test-model", "test", "test-revision", "non-secret prompt", nil, nil, 0)
	record.LargeDetail = &liveLargeDetailEvidence{
		FinalContextObserved: true, ExpectedModelVisibleResults: 2, PresentModelVisibleResults: 2,
		ModelVisibleResultsComplete: true, FinalContextDroppedGroups: 0,
	}
	payload, err := json.Marshal(record)
	if err != nil {
		t.Fatal("could not encode large-detail evidence")
	}
	if record.FixtureVariant != largeDetailFixtureVariant || record.DetailPaddingBytes != 4<<10 || record.Strategy != "forced_ptc" || strings.Contains(string(payload), payloadMarker) || strings.Contains(string(payload), "irrelevant_read_only_text") {
		t.Fatal("large-detail evidence persisted unsafe fixture material or lost safe labels")
	}
}

func TestLargeDetailPromptAllowsAllExposedProgramTools(t *testing.T) {
	prompt := liveLargeDetailPrompt(8)
	for _, tool := range []string{"program.catalog", "program.execute"} {
		if !strings.Contains(prompt, tool) {
			t.Fatalf("large-detail prompt does not permit exposed %q", tool)
		}
	}
	if !strings.Contains(prompt, "not exposed") {
		t.Fatal("large-detail prompt no longer prohibits unexposed tools")
	}
}

func TestLargeDetailResolvedSystemUsesConditionalCatalogAndKeepsDirectBan(t *testing.T) {
	model := &largeDetailSystemCaptureModel{}
	fixture, err := newFixtureWithDataOptions(model, true, 8, fixtureDataOptions{
		OutputContracts:       true,
		DetailIrrelevantBytes: 2 << 10,
	})
	if err != nil {
		t.Fatal("could not construct large-detail system fixture")
	}
	if err := configureLiveLargeDetailProfile(fixture, model.Provider(), "offline-large-detail", 10, 11, "forced_batched_direct"); err != nil {
		t.Fatal("could not configure large-detail system fixture")
	}
	result, err := fixture.runtime.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{RunID: "large-detail-system", Text: liveLargeDetailPrompt(8)}, nil)
	if err != nil || result.Status != core.RunCompleted {
		t.Fatal("large-detail system fixture did not complete")
	}
	system := model.system
	if !strings.Contains(system, "When choosing program.execute, first obtain program bindings from program.catalog") || strings.Contains(system, ". First obtain program bindings from program.catalog") {
		t.Fatal("large-detail resolved system did not scope catalog guidance conditionally")
	}
	if !strings.Contains(system, "Do not call program.catalog or program.execute.") {
		t.Fatal("large-detail resolved system lost the forced direct prohibition")
	}
}

func TestLargeDetailOutputSizeDisclosureUsesActualFirstToolSchema(t *testing.T) {
	const activeRows = 8
	disclosures := make(map[int]string)
	for _, paddingBytes := range []int{2 << 10, 4 << 10} {
		model := &largeDetailSystemCaptureModel{}
		fixture, err := newFixtureWithDataOptions(model, true, activeRows, fixtureDataOptions{
			OutputContracts:       true,
			DetailIrrelevantBytes: paddingBytes,
			ExposeOutputSize:      true,
		})
		if err != nil {
			t.Fatal("could not construct output-size disclosure fixture")
		}
		if err := configureLiveLargeDetailProfile(fixture, model.Provider(), "offline-output-size", 10, 11, ""); err != nil {
			t.Fatal("could not configure output-size disclosure fixture")
		}
		result, err := fixture.runtime.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{RunID: fmt.Sprintf("large-detail-output-size-%d", paddingBytes), Text: liveLargeDetailPrompt(activeRows)}, nil)
		if err != nil || result.Status != core.RunCompleted {
			t.Fatal("output-size disclosure fixture did not complete")
		}
		description, found := largeDetailToolDescription(model.tools)
		if !found {
			t.Fatal("first GenerateOptions.Tools omitted fixture.detail")
		}
		limit := fixture.detail.Manifest().MaxOutputBytes
		if limit != fixtureDetailMaxOutputBytes(activeRows, paddingBytes) || !strings.Contains(description, fmt.Sprintf("Maximum output %d bytes.", limit)) {
			t.Fatal("first tool schema did not disclose the exact encoded output limit")
		}
		disclosures[paddingBytes] = description
	}
	if disclosures[2<<10] == disclosures[4<<10] {
		t.Fatal("2KiB and 4KiB first tool descriptions did not expose different output limits")
	}

	// The existing v3 large fixture leaves its description byte-for-byte as
	// before; normal compact fixture text is likewise unaffected by the v4
	// opt-in disclosure.
	for _, test := range []struct {
		options fixtureDataOptions
		want    string
	}{
		{fixtureDataOptions{OutputContracts: true, DetailIrrelevantBytes: 2 << 10}, "Read deterministic fixture detail by id. Output includes id, name, and unrelated synthetic read-only reference text."},
		{fixtureDataOptions{}, "Read deterministic fixture detail by id."},
	} {
		model := &largeDetailSystemCaptureModel{}
		fixture, err := newFixtureWithDataOptions(model, true, activeRows, test.options)
		if err != nil {
			t.Fatal("could not construct unchanged fixture")
		}
		result, err := fixture.runtime.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{RunID: "large-detail-without-output-size", Text: "observe tools"}, nil)
		if err != nil || result.Status != core.RunCompleted {
			t.Fatal("unchanged fixture did not complete")
		}
		description, found := largeDetailToolDescription(model.tools)
		if !found || description != test.want {
			t.Fatal("default or v3 fixture description changed")
		}
	}
}

func largeDetailToolDescription(tools []core.ToolSchema) (string, bool) {
	for _, tool := range tools {
		if tool.Name == detailToolID {
			return tool.Description, true
		}
	}
	return "", false
}

func typedCatalogMaxOutputBytes(catalog map[string]any, id string) int {
	tools, _ := catalog["tools"].([]any)
	for _, raw := range tools {
		descriptor, _ := raw.(map[string]any)
		schema, _ := descriptor["schema"].(map[string]any)
		if schema["name"] != id {
			continue
		}
		if value, ok := descriptor["max_output_bytes"].(float64); ok {
			return int(value)
		}
	}
	return 0
}

// largeDetailBatchedDirectModel mirrors the forced direct live control's
// shape: inventory in the first response, all eight independent details in
// the second, then a final answer response. It is only an offline context
// pressure driver and never provides a model answer or PTC source to a live
// model.
type largeDetailBatchedDirectModel struct {
	phase  int
	system string
}

func (*largeDetailBatchedDirectModel) Provider() string { return "large-detail-batched-direct-offline" }

func (m *largeDetailBatchedDirectModel) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	if m.system == "" {
		m.system = options.System
	}
	switch m.phase {
	case 0:
		m.phase++
		emitTool(emit, core.ToolCall{ID: "large-detail-list", Name: listToolID, Args: map[string]any{}})
		return nil
	case 1:
		m.phase++
		content, err := lastToolContent(options.Messages, "large-detail-list")
		if err != nil {
			return err
		}
		ids, err := activeInventoryIDs(content)
		if err != nil {
			return err
		}
		calls := make([]core.ToolCall, 0, len(ids))
		for index, id := range ids {
			calls = append(calls, core.ToolCall{ID: fmt.Sprintf("large-detail-%02d", index+1), Name: detailToolID, Args: map[string]any{"id": id}})
		}
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCalls: calls})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
		return nil
	case 2:
		m.phase++
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: "offline context observed"})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
		return nil
	default:
		return errors.New("large-detail batched direct model received an extra turn")
	}
}

type largeDetailSystemCaptureModel struct {
	system string
	tools  []core.ToolSchema
}

type largeDetailLimitReporterModel struct {
	*largeDetailSystemCaptureModel
	contextWindowTokens int
	maxOutputTokens     int
}

func (m *largeDetailLimitReporterModel) ModelContextLimits() (int, int) {
	return m.contextWindowTokens, m.maxOutputTokens
}

type largeDetailLimitReporterAdapter struct {
	core.LlmAdapter
	contextWindowTokens int
	maxOutputTokens     int
}

func (m *largeDetailLimitReporterAdapter) ModelContextLimits() (int, int) {
	return m.contextWindowTokens, m.maxOutputTokens
}

// suppressModelContextLimitsAdapter is an explicit test-only control for
// Core's conservative fallback. Its static interface intentionally omits the
// optional ModelContextLimits method even when its delegate implements it.
type suppressModelContextLimitsAdapter struct{ core.LlmAdapter }

func (*largeDetailSystemCaptureModel) Provider() string { return "large-detail-system-offline" }

func (m *largeDetailSystemCaptureModel) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	m.system = options.System
	m.tools = append([]core.ToolSchema(nil), options.Tools...)
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: "offline system observed"})
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
	return nil
}
