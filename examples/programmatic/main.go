// Command programmatic runs a deterministic protocol fixture for bounded
// programmatic tool calling. It does not contact a model provider or execute
// any external tool.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	programtools "github.com/whhhh1500/auto-agent/pkg/adapter/programmatic/toolcapability"
	appcontextassembly "github.com/whhhh1500/auto-agent/pkg/app/contextassembly"
	access "github.com/whhhh1500/auto-agent/pkg/app/programmatic"
	core "github.com/whhhh1500/auto-agent/pkg/core"
)

const (
	fixtureProfileID                = "example.programmatic"
	listToolID                      = "fixture.inventory"
	detailToolID                    = "fixture.detail"
	maxFixtureDetailIrrelevantBytes = 4 << 10
)

const selectionProgram = `{"version":"ptc-ir/v1","body":[
  {"op":"call","assign":"listed","tool":"fixture.inventory","args":{"op":"map","entries":{}}},
  {"op":"assign","name":"selected","value":{"op":"list","items":[]}},
  {"op":"for","var":"row","in":{"op":"get","object":{"op":"var","name":"listed"},"key":"data"},"body":[
    {"op":"if","cond":{"op":"cmp","kind":"eq","left":{"op":"get","object":{"op":"var","name":"row"},"key":"active"},"right":{"op":"literal","value":true}},"then":[
      {"op":"call","assign":"detail","tool":"fixture.detail","args":{"op":"map","entries":{"id":{"op":"get","object":{"op":"var","name":"row"},"key":"id"}}}},
      {"op":"append","target":"selected","value":{"op":"get","object":{"op":"var","name":"detail"},"key":"data"}}
    ]}
  ]},
  {"op":"return","value":{"op":"map","entries":{"count":{"op":"len","value":{"op":"var","name":"selected"}},"items":{"op":"var","name":"selected"}}}}
]}`

// Report is the deterministic fixture output. HistoryBytes is the sum of JSON
// encodings of model-visible messages, while ModelRequestBytes sums the
// serializable GenerateOptions. Neither is token usage or evidence about
// provider latency or model quality.
type Report struct {
	Direct       Scenario `json:"direct"`
	Programmatic Scenario `json:"programmatic"`
	SameValue    bool     `json:"same_final_value"`
}

// Scenario reports comparable deterministic fixture counters. FixtureToolCalls
// counts only the two read-only fixture capabilities, so it excludes the PTC
// catalog and parent execute capabilities.
type Scenario struct {
	ModelRounds        int      `json:"model_rounds"`
	FixtureToolCalls   int      `json:"fixture_tool_calls"`
	JournalInvocations int      `json:"journal_invocations"`
	ModelHistoryBytes  int      `json:"model_visible_history_bytes"`
	ModelRequestBytes  int      `json:"model_request_bytes"`
	FinalValue         []string `json:"final_value"`
}

func main() {
	report, err := Run(context.Background())
	if err != nil {
		panic(err)
	}
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(encoded))
}

// Run executes both fixture protocols through core.Runtime. The programmatic
// scenario uses catalog -> execute and checks that its final model call sees
// only the aggregate value returned by program.execute.
func Run(ctx context.Context) (Report, error) {
	return RunWithActiveRows(ctx, 2)
}

// RunWithActiveRows repeats this deterministic fixture with one to eight
// active rows. It helps compare simple and batch cases without making a claim
// about an external model or provider.
func RunWithActiveRows(ctx context.Context, activeRows int) (Report, error) {
	if ctx == nil {
		return Report{}, errors.New("context is nil")
	}
	if activeRows < 1 || activeRows > 8 {
		return Report{}, errors.New("active rows must be between 1 and 8")
	}
	direct, err := runDirect(ctx, activeRows)
	if err != nil {
		return Report{}, err
	}
	programmatic, err := runProgrammatic(ctx, activeRows)
	if err != nil {
		return Report{}, err
	}
	return Report{Direct: direct, Programmatic: programmatic, SameValue: sameStrings(direct.FinalValue, programmatic.FinalValue)}, nil
}

func runDirect(ctx context.Context, activeRows int) (Scenario, error) {
	model := &directModel{}
	fixture, err := newFixture(model, false, activeRows)
	if err != nil {
		return Scenario{}, err
	}
	result, err := fixture.runtime.RunTurn(ctx, fixture.principal, fixture.session, core.TurnInput{RunID: "direct-run", Text: "find active inventory names"}, nil)
	if err != nil {
		return Scenario{}, err
	}
	if result.Status != core.RunCompleted || !sameStrings(model.finalValue(), expectedValue(activeRows)) {
		return Scenario{}, fmt.Errorf("direct fixture did not complete with expected value: %#v", result)
	}
	return fixture.scenario(model.rounds(), model.historyBytes(), model.requestBytes(), model.finalValue()), nil
}

func runProgrammatic(ctx context.Context, activeRows int) (Scenario, error) {
	model := &programmaticModel{}
	fixture, err := newFixture(model, true, activeRows)
	if err != nil {
		return Scenario{}, err
	}
	result, err := fixture.runtime.RunTurn(ctx, fixture.principal, fixture.session, core.TurnInput{RunID: "programmatic-run", Text: "find active inventory names"}, nil)
	if err != nil {
		return Scenario{}, err
	}
	if result.Status != core.RunCompleted || !model.receivedAggregate() || model.finalContextHasNestedToolResult() || !sameStrings(model.finalValue(), expectedValue(activeRows)) {
		return Scenario{}, fmt.Errorf("programmatic fixture did not complete with expected aggregate: %#v", result)
	}
	return fixture.scenario(model.rounds(), model.historyBytes(), model.requestBytes(), model.finalValue()), nil
}

func expectedValue(activeRows int) []string {
	values := make([]string, activeRows)
	for index := range values {
		values[index] = fmt.Sprintf("Item %02d", index+1)
	}
	return values
}

type fixture struct {
	runtime   *core.Runtime
	principal core.Principal
	session   *core.Session
	journal   *memoryJournal
	list      *readOnlyFixtureTool
	detail    *readOnlyFixtureTool
}

func newFixture(model core.LlmAdapter, includeProgrammatic bool, activeRows int) (*fixture, error) {
	return newFixtureWithDataOptions(model, includeProgrammatic, activeRows, fixtureDataOptions{})
}

// newFixtureWithOutputContracts is a test-only fixture variation. The normal
// example deliberately remains schema-free so live experiments can compare
// catalog-only parameter knowledge with explicit tool output contracts.
func newFixtureWithOutputContracts(model core.LlmAdapter, includeProgrammatic bool, activeRows int, outputContracts bool) (*fixture, error) {
	return newFixtureWithDataOptions(model, includeProgrammatic, activeRows, fixtureDataOptions{OutputContracts: outputContracts})
}

// fixtureDataOptions is deliberately private to this example. Large detail
// payloads exist only for opt-in acceptance experiments; the default fixture
// keeps its original schema-free, compact manifests and outputs.
type fixtureDataOptions struct {
	OutputContracts         bool
	DetailIrrelevantBytes   int
	ExposeOutputSize        bool
	ObserveAssembledContext func(request, assembled core.ModelContext, err error)
}

func newFixtureWithDataOptions(model core.LlmAdapter, includeProgrammatic bool, activeRows int, options fixtureDataOptions) (*fixture, error) {
	if options.DetailIrrelevantBytes < 0 || options.DetailIrrelevantBytes > maxFixtureDetailIrrelevantBytes {
		return nil, errors.New("fixture detail irrelevant payload is invalid")
	}
	if options.DetailIrrelevantBytes > 0 && !options.OutputContracts {
		return nil, errors.New("large fixture detail payload requires output contracts")
	}
	if options.ExposeOutputSize && options.DetailIrrelevantBytes == 0 {
		return nil, errors.New("fixture output-size disclosure requires a large detail payload")
	}
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product, err := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "programmatic-example"})
	if err != nil {
		return nil, err
	}
	user, err := product.Child(core.ScopeRef{Kind: core.ScopeUser, ID: "fixture-user"})
	if err != nil {
		return nil, err
	}
	principal := core.Principal{TenantID: "fixture-tenant", SubjectID: "fixture-subject", Scope: user, Grants: core.NewPermissionSet(core.PermRead)}
	sessionScope, err := user.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "fixture-session"})
	if err != nil {
		return nil, err
	}
	session, err := core.NewSession(core.SessionOptions{ID: "fixture-session", ProfileID: fixtureProfileID, Principal: principal, Scope: sessionScope})
	if err != nil {
		return nil, err
	}

	largeDetail := options.DetailIrrelevantBytes > 0
	list := newInventoryToolWithFixtureOptions(activeRows, options.OutputContracts, largeDetail)
	detail := newDetailToolWithFixtureOptions(activeRows, options.OutputContracts, options.DetailIrrelevantBytes, options.ExposeOutputSize)
	registry := core.NewCapabilityRegistry()
	capabilities := []core.Capability{list, detail}
	if includeProgrammatic {
		catalog, err := programtools.NewCatalogCapability(programtools.CatalogID)
		if err != nil {
			return nil, err
		}
		execute, err := programtools.NewExecuteCapability(programtools.ExecuteID)
		if err != nil {
			return nil, err
		}
		capabilities = append([]core.Capability{catalog, execute}, capabilities...)
	}
	for _, capability := range capabilities {
		if err := registry.Register(product, capability); err != nil {
			return nil, err
		}
	}

	profile := core.NewAgentProfileRegistry()
	name := "Deterministic programmatic protocol fixture"
	modelSelection := core.ModelSelection{Provider: model.Provider(), Model: "fixture"}
	ids := []string{listToolID, detailToolID}
	if includeProgrammatic {
		ids = append([]string{programtools.CatalogID, programtools.ExecuteID}, ids...)
	}
	steps, toolCalls := activeRows+2, activeRows+4
	if err := profile.Bind(core.AgentProfileLayer{Scope: product, ProfileID: fixtureProfileID, Name: &name, Model: &modelSelection, AddCapabilities: ids, MaxSteps: &steps, MaxToolCalls: &toolCalls}); err != nil {
		return nil, err
	}
	journal := newMemoryJournal()
	contextAssembler, err := appcontextassembly.NewAssembler(appcontextassembly.Config{})
	if err != nil {
		return nil, err
	}
	assemble := contextAssembler.AssembleModelContext
	if options.ObserveAssembledContext != nil {
		assemble = func(ctx context.Context, request core.ModelContext) (core.ModelContext, error) {
			assembled, err := contextAssembler.AssembleModelContext(ctx, request)
			options.ObserveAssembledContext(request, assembled, err)
			return assembled, err
		}
	}
	runtime := &core.Runtime{
		Capabilities: registry, Profiles: profile, ToolJournal: journal,
		Models:           core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) { return model, nil }),
		ContextAssembler: assemble,
	}
	return &fixture{runtime: runtime, principal: principal, session: session, journal: journal, list: list, detail: detail}, nil
}

func (f *fixture) scenario(rounds, historyBytes, requestBytes int, value []string) Scenario {
	return Scenario{
		ModelRounds: rounds, FixtureToolCalls: f.list.calls() + f.detail.calls(), JournalInvocations: f.journal.completed(),
		ModelHistoryBytes: historyBytes, ModelRequestBytes: requestBytes, FinalValue: append([]string(nil), value...),
	}
}

type readOnlyFixtureTool struct {
	manifest core.CapabilityManifest
	mu       sync.Mutex
	callsN   int
	execute  func(map[string]any) (string, error)
}

func newInventoryToolWithFixtureOptions(activeRows int, outputContract, declareExactOutputLimit bool) *readOnlyFixtureTool {
	manifest := core.CapabilityManifest{
		ID: listToolID, Version: "1.0.0", Name: "Fixture inventory", Description: "Read deterministic fixture inventory.",
		Kind: core.KindKnowledge, Idempotent: true, Metadata: map[string]string{access.ExposureKey: access.ExposureVersion},
		Tool: &core.ToolExposure{Parameters: map[string]any{"type": "object", "additionalProperties": false}},
	}
	if outputContract {
		manifest.OutputSchema = inventoryOutputSchema()
	}
	if declareExactOutputLimit {
		content, err := fixtureInventoryContent(activeRows)
		if err == nil {
			manifest.MaxOutputBytes = len(content)
		}
	}
	return &readOnlyFixtureTool{manifest: manifest, execute: func(map[string]any) (string, error) {
		return fixtureInventoryContent(activeRows)
	}}
}

func newDetailToolWithFixtureOptions(activeRows int, outputContract bool, irrelevantBytes int, exposeOutputSize bool) *readOnlyFixtureTool {
	manifest := core.CapabilityManifest{
		ID: detailToolID, Version: "1.0.0", Name: "Fixture detail", Description: "Read deterministic fixture detail by id.",
		Kind: core.KindKnowledge, Idempotent: true, Metadata: map[string]string{access.ExposureKey: access.ExposureVersion},
		Tool: &core.ToolExposure{Parameters: map[string]any{"type": "object", "required": []any{"id"}, "additionalProperties": false, "properties": map[string]any{"id": map[string]any{"type": "string"}}}},
	}
	if irrelevantBytes > 0 {
		// All large-result arms see this same accurate descriptor. It describes
		// the fixture data without prescribing a direct or programmatic route.
		manifest.Description = "Read deterministic fixture detail by id. Output includes id, name, and unrelated synthetic read-only reference text."
	}
	if outputContract {
		manifest.OutputSchema = detailOutputSchemaWithIrrelevantText(irrelevantBytes > 0)
	}
	if irrelevantBytes > 0 {
		manifest.MaxOutputBytes = fixtureDetailMaxOutputBytes(activeRows, irrelevantBytes)
	}
	if exposeOutputSize {
		// This opt-in disclosure is derived from the actual manifest limit, not
		// a second payload-size calculation. It describes output shape only and
		// intentionally gives no route recommendation.
		manifest.Description += fmt.Sprintf(" Maximum output %d bytes.", manifest.MaxOutputBytes)
	}
	return &readOnlyFixtureTool{manifest: manifest, execute: func(args map[string]any) (string, error) {
		id, _ := args["id"].(string)
		return fixtureDetailContent(id, activeRows, irrelevantBytes)
	}}
}

func fixtureInventoryContent(activeRows int) (string, error) {
	rows := make([]map[string]any, 0, activeRows+1)
	for index := 1; index <= activeRows; index++ {
		rows = append(rows, map[string]any{"id": fmt.Sprintf("item-%02d", index), "active": true})
	}
	rows = append(rows, map[string]any{"id": "inactive", "active": false})
	encoded, err := json.Marshal(rows)
	return string(encoded), err
}

func fixtureDetailContent(id string, activeRows, irrelevantBytes int) (string, error) {
	var index int
	if _, err := fmt.Sscanf(id, "item-%02d", &index); err != nil || index < 1 || index > activeRows {
		return "", fmt.Errorf("unknown read-only fixture id %q", id)
	}
	name := fmt.Sprintf("Item %02d", index)
	if irrelevantBytes == 0 {
		// Keep the compact baseline byte-for-byte stable.
		return fmt.Sprintf(`{"id":%q,"name":%q}`, id, name), nil
	}
	return fmt.Sprintf(`{"id":%q,"name":%q,"irrelevant_read_only_text":%q}`, id, name, fixtureIrrelevantReadOnlyText(irrelevantBytes)), nil
}

func fixtureDetailMaxOutputBytes(activeRows, irrelevantBytes int) int {
	maximum := 0
	for index := 1; index <= activeRows; index++ {
		content, err := fixtureDetailContent(fmt.Sprintf("item-%02d", index), activeRows, irrelevantBytes)
		if err == nil && len(content) > maximum {
			maximum = len(content)
		}
	}
	return maximum
}

// fixtureIrrelevantReadOnlyText is fixed synthetic reference material, not a
// repeated byte. Every line has a stable ordinal and a distinct checksum so
// this pressure fixture cannot gain its apparent size from a single-character
// run. It is exactly the requested ASCII byte length for every arm.
func fixtureIrrelevantReadOnlyText(length int) string {
	var builder strings.Builder
	builder.Grow(length)
	for index := 0; builder.Len() < length; index++ {
		sum := sha256.Sum256([]byte(fmt.Sprintf("large-detail-fixture-record-%d", index)))
		line := fmt.Sprintf("synthetic_read_only_record=%03d checksum=%x class=non_actionable archived=true\n", index, sum[:8])
		remaining := length - builder.Len()
		if len(line) > remaining {
			line = line[:remaining]
		}
		builder.WriteString(line)
	}
	return builder.String()
}

func inventoryOutputSchema() map[string]any {
	return map[string]any{"type": "array", "items": map[string]any{"type": "object", "required": []any{"id", "active"}, "additionalProperties": false, "properties": map[string]any{
		"id": map[string]any{"type": "string"}, "active": map[string]any{"type": "boolean"},
	}}}
}

func detailOutputSchema() map[string]any {
	return detailOutputSchemaWithIrrelevantText(false)
}

func detailOutputSchemaWithIrrelevantText(includeIrrelevantText bool) map[string]any {
	required := []any{"id", "name"}
	properties := map[string]any{
		"id": map[string]any{"type": "string"}, "name": map[string]any{"type": "string"},
	}
	if includeIrrelevantText {
		required = append(required, "irrelevant_read_only_text")
		properties["irrelevant_read_only_text"] = map[string]any{"type": "string"}
	}
	return map[string]any{"type": "object", "required": required, "additionalProperties": false, "properties": properties}
}

func (t *readOnlyFixtureTool) Manifest() core.CapabilityManifest { return t.manifest }
func (t *readOnlyFixtureTool) Execute(_ context.Context, request core.CapabilityRequest) (core.CapabilityResult, error) {
	if err := core.RequireAcceptedInvocation(request); err != nil {
		return core.CapabilityResult{}, err
	}
	content, err := t.execute(request.Args)
	if err != nil {
		return core.CapabilityResult{}, err
	}
	t.mu.Lock()
	t.callsN++
	t.mu.Unlock()
	return core.CapabilityResult{Content: content, OK: true}, nil
}
func (t *readOnlyFixtureTool) calls() int { t.mu.Lock(); defer t.mu.Unlock(); return t.callsN }

type measuredModel struct {
	mu            sync.Mutex
	roundsN       int
	historyBytesN int
	requestBytesN int
	final         []string
}

func (m *measuredModel) record(options core.GenerateOptions) {
	encoded, _ := json.Marshal(options.Messages)
	request, _ := json.Marshal(options)
	m.mu.Lock()
	m.roundsN++
	m.historyBytesN += len(encoded)
	m.requestBytesN += len(request)
	m.mu.Unlock()
}
func (m *measuredModel) setFinal(value []string) {
	m.mu.Lock()
	m.final = append([]string(nil), value...)
	m.mu.Unlock()
}
func (m *measuredModel) rounds() int       { m.mu.Lock(); defer m.mu.Unlock(); return m.roundsN }
func (m *measuredModel) historyBytes() int { m.mu.Lock(); defer m.mu.Unlock(); return m.historyBytesN }
func (m *measuredModel) requestBytes() int { m.mu.Lock(); defer m.mu.Unlock(); return m.requestBytesN }
func (m *measuredModel) finalValue() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.final...)
}

// directModel receives each preceding fixture result before choosing the next
// direct tool call. It is deliberately deterministic rather than an LLM.
type directModel struct {
	measuredModel
	phase   int
	pending []string
}

func (*directModel) Provider() string { return "deterministic-direct-fixture" }
func (m *directModel) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	m.record(options)
	m.mu.Lock()
	phase := m.phase
	m.phase++
	m.mu.Unlock()
	if phase == 0 {
		emitTool(emit, core.ToolCall{ID: "direct-list", Name: listToolID, Args: map[string]any{}})
		return nil
	}
	if phase == 1 {
		content, err := lastToolContent(options.Messages, "direct-list")
		if err != nil {
			return err
		}
		ids, err := activeInventoryIDs(content)
		if err != nil {
			return fmt.Errorf("decode direct fixture inventory: %w", err)
		}
		if len(ids) == 0 {
			return errors.New("direct fixture inventory had no active rows")
		}
		m.mu.Lock()
		m.pending = ids[1:]
		m.mu.Unlock()
		return m.emitDetail(emit, ids[0])
	}

	detail, err := lastToolJSON(options.Messages, fmt.Sprintf("direct-detail-%d", phase-1))
	if err != nil {
		return err
	}
	name, err := namedValue(detail)
	if err != nil {
		return err
	}
	value := append(m.finalValue(), name)
	m.setFinal(value)
	m.mu.Lock()
	if len(m.pending) == 0 {
		m.mu.Unlock()
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: "selected: " + strings.Join(value, ", ")})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
		return nil
	}
	next := m.pending[0]
	m.pending = m.pending[1:]
	m.mu.Unlock()
	return m.emitDetail(emit, next)

}

func (m *directModel) emitDetail(emit func(core.StreamChunk), id string) error {
	callIndex := len(m.finalValue()) + 1
	emitTool(emit, core.ToolCall{ID: fmt.Sprintf("direct-detail-%d", callIndex), Name: detailToolID, Args: map[string]any{"id": id}})
	return nil
}

// programmaticModel first selects bindings from program.catalog, then submits
// a real loop/branch program to program.execute and reads its aggregate output.
type programmaticModel struct {
	measuredModel
	phase         int
	aggregate     bool
	finalMessages []core.ChatMessage
}

func (*programmaticModel) Provider() string { return "deterministic-programmatic-fixture" }
func (m *programmaticModel) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	m.record(options)
	m.mu.Lock()
	phase := m.phase
	m.phase++
	m.mu.Unlock()
	switch phase {
	case 0:
		emitTool(emit, core.ToolCall{ID: "program-catalog", Name: programtools.CatalogID, Args: map[string]any{}})
	case 1:
		catalog, err := lastToolJSON(options.Messages, "program-catalog")
		if err != nil {
			return err
		}
		bindings, err := catalogBindings(catalog, listToolID, detailToolID)
		if err != nil {
			return err
		}
		emitTool(emit, core.ToolCall{
			ID: "program-execute", Name: programtools.ExecuteID,
			Args: map[string]any{"source": selectionProgram, "bindings": bindings},
		})
	case 2:
		m.mu.Lock()
		m.finalMessages = append([]core.ChatMessage(nil), options.Messages...)
		m.mu.Unlock()
		output, err := lastToolJSON(options.Messages, "program-execute")
		if err != nil {
			return err
		}
		value, err := aggregateNames(output)
		if err != nil {
			return err
		}
		m.mu.Lock()
		m.aggregate = true
		m.mu.Unlock()
		m.setFinal(value)
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: "selected: " + strings.Join(value, ", ")})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
	default:
		return errors.New("programmatic fixture model received an unexpected extra turn")
	}
	return nil
}
func (m *programmaticModel) receivedAggregate() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.aggregate
}

// finalContextHasNestedToolResult verifies that only the model-requested
// program.execute result reaches the final model turn. Child tool results are
// durable audit evidence and must be elided by the context assembler.
func (m *programmaticModel) finalContextHasNestedToolResult() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, message := range m.finalMessages {
		if message.Role == core.RoleTool && strings.Contains(message.ToolCallID, "/") {
			return true
		}
	}
	return false
}

func emitTool(emit func(core.StreamChunk), call core.ToolCall) {
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &call})
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
}

func lastToolJSON(messages []core.ChatMessage, id string) (map[string]any, error) {
	content, err := lastToolContent(messages, id)
	if err != nil {
		return nil, err
	}
	var value map[string]any
	if err := json.Unmarshal([]byte(content), &value); err != nil {
		return nil, err
	}
	return value, nil
}

func lastToolContent(messages []core.ChatMessage, id string) (string, error) {
	for index := len(messages) - 1; index >= 0; index-- {
		message := messages[index]
		if message.Role != core.RoleTool || message.ToolCallID != id {
			continue
		}
		return message.Content, nil
	}
	return "", fmt.Errorf("fixture model did not receive tool result %q", id)
}

func catalogBindings(catalog map[string]any, ids ...string) (map[string]any, error) {
	tools, ok := catalog["tools"].([]any)
	if !ok {
		return nil, errors.New("catalog tools are missing")
	}
	wanted := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		wanted[id] = struct{}{}
	}
	bindings := map[string]any{}
	for _, raw := range tools {
		tool, ok := raw.(map[string]any)
		if !ok {
			return nil, errors.New("catalog descriptor is invalid")
		}
		schema, ok := tool["schema"].(map[string]any)
		name, nameOK := schema["name"].(string)
		digest, digestOK := tool["binding_digest"].(string)
		if ok && nameOK && digestOK {
			if _, needed := wanted[name]; needed {
				bindings[name] = digest
			}
		}
	}
	if len(bindings) != len(wanted) {
		return nil, errors.New("catalog did not expose required fixture bindings")
	}
	return bindings, nil
}

func aggregateNames(output map[string]any) ([]string, error) {
	value, ok := output["value"].(map[string]any)
	if !ok {
		return nil, errors.New("program output value is missing")
	}
	items, ok := value["items"].([]any)
	if !ok {
		return nil, errors.New("program output items are missing")
	}
	if count, ok := value["count"].(float64); !ok || int(count) != len(items) {
		return nil, errors.New("program output count is invalid")
	}
	names := make([]string, 0, len(items))
	for _, raw := range items {
		item, ok := raw.(map[string]any)
		if !ok {
			return nil, errors.New("program output item is invalid")
		}
		name, ok := item["name"].(string)
		if !ok {
			return nil, errors.New("program output name is invalid")
		}
		names = append(names, name)
	}
	return names, nil
}

func namedValue(value map[string]any) (string, error) {
	name, ok := value["name"].(string)
	if !ok {
		return "", errors.New("fixture detail result lacks name")
	}
	return name, nil
}

func activeInventoryIDs(content string) ([]string, error) {
	var rows []map[string]any
	if err := json.Unmarshal([]byte(content), &rows); err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		active, _ := row["active"].(bool)
		id, _ := row["id"].(string)
		if active && id != "" {
			ids = append(ids, id)
		}
	}
	return ids, nil
}
func sameStrings(left, right []string) bool {
	return len(left) == len(right) && strings.Join(left, "\x00") == strings.Join(right, "\x00")
}

// memoryJournal is intentionally local to this deterministic example. It
// implements the production ToolInvocationJournal contract but has no durable
// backing store; deployments must provide durable storage for crash recovery.
type memoryJournal struct {
	mu      sync.Mutex
	records map[string]core.ToolInvocationRecord
}

func newMemoryJournal() *memoryJournal {
	return &memoryJournal{records: map[string]core.ToolInvocationRecord{}}
}
func journalKey(invocation core.ToolInvocation) string {
	return invocation.SessionID + "\x00" + invocation.RunID + "\x00" + invocation.CallID
}
func (j *memoryJournal) BeginToolInvocation(_ context.Context, invocation core.ToolInvocation) (core.ToolInvocationRecord, core.ToolInvocationDecision, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	key := journalKey(invocation)
	if record, ok := j.records[key]; ok {
		if record.ToolInvocation != invocation {
			return record, core.ToolInvocationConflict, nil
		}
		if record.State == core.ToolInvocationCompleted {
			return core.CloneToolInvocationRecord(record), core.ToolInvocationReplay, nil
		}
		if record.State == core.ToolInvocationUncertain {
			return record, core.ToolInvocationUnknown, nil
		}
		return record, core.ToolInvocationExecuteRetry, nil
	}
	now := time.Now().UTC()
	record := core.ToolInvocationRecord{ToolInvocation: invocation, State: core.ToolInvocationStarted, StartedAt: now, UpdatedAt: now}
	j.records[key] = record
	return record, core.ToolInvocationExecuteNew, nil
}
func (j *memoryJournal) CompleteToolInvocation(_ context.Context, invocation core.ToolInvocation, result core.CapabilityResult) (core.ToolInvocationRecord, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	key := journalKey(invocation)
	record, ok := j.records[key]
	if !ok || record.ToolInvocation != invocation {
		return core.ToolInvocationRecord{}, errors.New("fixture journal conflict")
	}
	resultCopy := result
	now := time.Now().UTC()
	record.State, record.Result, record.CompletedAt, record.UpdatedAt = core.ToolInvocationCompleted, &resultCopy, now, now
	j.records[key] = record
	return core.CloneToolInvocationRecord(record), nil
}
func (j *memoryJournal) MarkToolInvocationUncertain(_ context.Context, invocation core.ToolInvocation, code string) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	key := journalKey(invocation)
	record, ok := j.records[key]
	if !ok || record.ToolInvocation != invocation {
		return errors.New("fixture journal conflict")
	}
	record.State, record.ErrorCode, record.UpdatedAt = core.ToolInvocationUncertain, code, time.Now().UTC()
	j.records[key] = record
	return nil
}

// GetToolInvocation is the read-only half of the in-memory journal contract.
// A matching key is not enough: callers receive a record only when every
// immutable identity field, including the argument digest, is identical.
func (j *memoryJournal) GetToolInvocation(_ context.Context, invocation core.ToolInvocation) (core.ToolInvocationRecord, bool, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	record, found := j.records[journalKey(invocation)]
	if !found || record.ToolInvocation != invocation {
		return core.ToolInvocationRecord{}, false, nil
	}
	return core.CloneToolInvocationRecord(record), true, nil
}

var _ core.ToolInvocationReader = (*memoryJournal)(nil)

func (j *memoryJournal) completed() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	count := 0
	for _, record := range j.records {
		if record.State == core.ToolInvocationCompleted {
			count++
		}
	}
	return count
}
