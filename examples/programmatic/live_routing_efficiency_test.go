package main

import (
	"context"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	programtools "github.com/whhhh1500/auto-agent/pkg/adapter/programmatic/toolcapability"
	appcontextassembly "github.com/whhhh1500/auto-agent/pkg/app/contextassembly"
	access "github.com/whhhh1500/auto-agent/pkg/app/programmatic"
	core "github.com/whhhh1500/auto-agent/pkg/core"
)

const (
	routingActiveRows      = 8
	routingRoundBudget     = 10
	routingToolCallBudget  = 11
	routingEvidenceSchema  = "harness.programmatic.live-routing-efficiency/v1"
	routingDatasetValueLen = 18
)

type routingArm string

const (
	routingSequentialReact routingArm = "sequential_react"
	routingParallelReact   routingArm = "parallel_react"
	routingPTC             routingArm = "ptc"
)

// routingArmOrders is the complete, predeclared set of three-arm Latin-square
// permutations. Six triads balance each arm across all three positions; live
// runs reject a seventh triad rather than silently cycling it.
var routingArmOrders = [][]routingArm{
	{routingSequentialReact, routingParallelReact, routingPTC},
	{routingSequentialReact, routingPTC, routingParallelReact},
	{routingParallelReact, routingSequentialReact, routingPTC},
	{routingParallelReact, routingPTC, routingSequentialReact},
	{routingPTC, routingSequentialReact, routingParallelReact},
	{routingPTC, routingParallelReact, routingSequentialReact},
}

type routingDatasetRow struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// routingDataset stays in memory for one triad. Names are unpredictable and
// deliberately never copied to evidence, logs, prompts, or test failures.
type routingDataset struct{ rows []routingDatasetRow }

func newRoutingDataset() (routingDataset, error) {
	rows := make([]routingDatasetRow, routingActiveRows)
	for index := range rows {
		value := make([]byte, routingDatasetValueLen)
		if _, err := cryptorand.Read(value); err != nil {
			return routingDataset{}, errors.New("could not create routing fixture data")
		}
		rows[index] = routingDatasetRow{ID: fmt.Sprintf("item-%02d", index+1), Name: "opaque-" + hex.EncodeToString(value)}
	}
	return routingDataset{rows: rows}, nil
}

func (d routingDataset) hash() string { return liveSHA256(d.rows) }

func (d routingDataset) names() []string {
	names := make([]string, len(d.rows))
	for index := range d.rows {
		names[index] = d.rows[index].Name
	}
	return names
}

func (d routingDataset) inventory() (string, error) {
	rows := make([]map[string]any, len(d.rows))
	for index := range d.rows {
		rows[index] = map[string]any{"id": d.rows[index].ID, "active": true}
	}
	encoded, err := json.Marshal(rows)
	return string(encoded), err
}

func (d routingDataset) detail(id string) (string, error) {
	for _, row := range d.rows {
		if row.ID == id {
			encoded, err := json.Marshal(map[string]string{"id": row.ID, "name": row.Name})
			return string(encoded), err
		}
	}
	return "", errors.New("unknown routing fixture item")
}

type routingFixtureTool struct {
	manifest core.CapabilityManifest
	run      func(map[string]any) (string, error)
	mu       sync.Mutex
	callsN   int
}

func (t *routingFixtureTool) Manifest() core.CapabilityManifest { return t.manifest }

func (t *routingFixtureTool) Execute(_ context.Context, request core.CapabilityRequest) (core.CapabilityResult, error) {
	if err := core.RequireAcceptedInvocation(request); err != nil {
		return core.CapabilityResult{}, err
	}
	content, err := t.run(request.Args)
	if err != nil {
		return core.CapabilityResult{}, err
	}
	t.mu.Lock()
	t.callsN++
	t.mu.Unlock()
	return core.CapabilityResult{Content: content, OK: true}, nil
}

func (t *routingFixtureTool) calls() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.callsN
}

func newRoutingInventoryTool(dataset routingDataset) *routingFixtureTool {
	return &routingFixtureTool{
		manifest: core.CapabilityManifest{
			ID: listToolID, Version: "1.0.0", Name: "Routing inventory", Description: "Read the experiment inventory.",
			Kind: core.KindKnowledge, Idempotent: true, Metadata: map[string]string{access.ExposureKey: access.ExposureVersion},
			Tool: &core.ToolExposure{Parameters: map[string]any{"type": "object", "additionalProperties": false}}, OutputSchema: inventoryOutputSchema(),
		},
		run: func(map[string]any) (string, error) { return dataset.inventory() },
	}
}

func newRoutingDetailTool(dataset routingDataset) *routingFixtureTool {
	return &routingFixtureTool{
		manifest: core.CapabilityManifest{
			ID: detailToolID, Version: "1.0.0", Name: "Routing detail", Description: "Read one experiment item by id.",
			Kind: core.KindKnowledge, Idempotent: true, Metadata: map[string]string{access.ExposureKey: access.ExposureVersion},
			Tool: &core.ToolExposure{Parameters: map[string]any{"type": "object", "required": []any{"id"}, "additionalProperties": false, "properties": map[string]any{"id": map[string]any{"type": "string"}}}}, OutputSchema: detailOutputSchema(),
		},
		run: func(args map[string]any) (string, error) {
			id, _ := args["id"].(string)
			return dataset.detail(id)
		},
	}
}

type routingFixture struct {
	runtime   *core.Runtime
	principal core.Principal
	session   *core.Session
	journal   *memoryJournal
	list      *routingFixtureTool
	detail    *routingFixtureTool
}

func newRoutingFixture(model core.LlmAdapter, dataset routingDataset, observe func(core.ModelContext, core.ModelContext, error)) (*routingFixture, error) {
	return newRoutingFixtureWithSessionID("fixture-session", model, dataset, observe)
}

// newRoutingFixtureWithSessionID keeps each matched live arm independently
// journaled while preserving the common profile, capability registry, and
// fixture data supplied by its caller.
func newRoutingFixtureWithSessionID(sessionID string, model core.LlmAdapter, dataset routingDataset, observe func(core.ModelContext, core.ModelContext, error)) (*routingFixture, error) {
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product, err := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "routing-efficiency-example"})
	if err != nil {
		return nil, err
	}
	user, err := product.Child(core.ScopeRef{Kind: core.ScopeUser, ID: "fixture-user"})
	if err != nil {
		return nil, err
	}
	principal := core.Principal{TenantID: "fixture-tenant", SubjectID: "fixture-subject", Scope: user, Grants: core.NewPermissionSet(core.PermRead)}
	sessionScope, err := user.Child(core.ScopeRef{Kind: core.ScopeSession, ID: sessionID})
	if err != nil {
		return nil, err
	}
	session, err := core.NewSession(core.SessionOptions{ID: sessionID, ProfileID: fixtureProfileID, Principal: principal, Scope: sessionScope})
	if err != nil {
		return nil, err
	}

	list, detail := newRoutingInventoryTool(dataset), newRoutingDetailTool(dataset)
	catalog, err := programtools.NewCatalogCapability(programtools.CatalogID)
	if err != nil {
		return nil, err
	}
	execute, err := programtools.NewExecuteCapability(programtools.ExecuteID)
	if err != nil {
		return nil, err
	}
	registry := core.NewCapabilityRegistry()
	for _, capability := range []core.Capability{catalog, execute, list, detail} {
		if err := registry.Register(product, capability); err != nil {
			return nil, err
		}
	}
	profiles := core.NewAgentProfileRegistry()
	name := "Fair routing efficiency fixture"
	selection := core.ModelSelection{Provider: model.Provider(), Model: "fixture"}
	ids := []string{programtools.CatalogID, programtools.ExecuteID, listToolID, detailToolID}
	if err := profiles.Bind(core.AgentProfileLayer{Scope: product, ProfileID: fixtureProfileID, Name: &name, Model: &selection, AddCapabilities: ids, MaxSteps: intPointer(routingRoundBudget), MaxToolCalls: intPointer(routingToolCallBudget)}); err != nil {
		return nil, err
	}
	assembler, err := appcontextassembly.NewAssembler(appcontextassembly.Config{})
	if err != nil {
		return nil, err
	}
	assemble := assembler.AssembleModelContext
	if observe != nil {
		assemble = func(ctx context.Context, request core.ModelContext) (core.ModelContext, error) {
			assembled, assembleErr := assembler.AssembleModelContext(ctx, request)
			observe(request, assembled, assembleErr)
			return assembled, assembleErr
		}
	}
	journal := newMemoryJournal()
	return &routingFixture{
		runtime:   &core.Runtime{Capabilities: registry, Profiles: profiles, ToolJournal: journal, Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) { return model, nil }), ContextAssembler: assemble},
		principal: principal, session: session, journal: journal, list: list, detail: detail,
	}, nil
}

func intPointer(value int) *int { return &value }

func routingUserPrompt() string {
	return "Use only the exposed read-only tools. Obtain each inventory item's name exactly once and then reply with exactly one line: FINAL: <names in inventory order, separated by comma and one space>."
}

func routingInstruction(arm routingArm) (string, bool) {
	switch arm {
	case routingSequentialReact:
		return "Routing experiment instruction: call fixture.inventory first, then call exactly one fixture.detail in each subsequent model response until all inventory items are read. Do not call program.catalog or program.execute.", true
	case routingParallelReact:
		return "Routing experiment instruction: call fixture.inventory first, then submit every independent fixture.detail call together in one subsequent model response. Do not call program.catalog or program.execute.", true
	case routingPTC:
		return "Routing experiment instruction: call program.catalog first, then call program.execute exactly once with a program you write from the catalog. Do not call fixture tools directly from the model.", true
	default:
		return "", false
	}
}

// configureRoutingProfile changes exactly one profile fragment between arms.
// The prompt, model selection, four-tool menu, data, and budgets are supplied
// by the shared fixture and remain outside this arm-specific layer.
func configureRoutingProfile(fixture *routingFixture, provider, modelID string, arm routingArm) error {
	instruction, ok := routingInstruction(arm)
	if !ok {
		return errors.New("routing experiment arm is invalid")
	}
	selection := core.ModelSelection{Provider: provider, Model: modelID}
	return fixture.runtime.Profiles.Bind(core.AgentProfileLayer{
		Scope: fixture.session.Scope(), ProfileID: fixtureProfileID, Model: &selection,
		MaxSteps: intPointer(routingRoundBudget), MaxToolCalls: intPointer(routingToolCallBudget),
		PutFragments: []core.PromptFragment{{ID: "routing-efficiency-arm", Section: core.PromptInstructions, Content: instruction}},
	})
}

type routingContextEvidence struct {
	Observed                 bool  `json:"observed"`
	AssemblyCalls            int   `json:"assembly_calls"`
	AssemblyFailures         int   `json:"assembly_failures"`
	FinalMessageCount        int   `json:"final_message_count"`
	FinalToolMessageCount    int   `json:"final_tool_message_count"`
	FinalChildToolMessages   int   `json:"final_child_tool_messages"`
	FinalInputBytes          int64 `json:"final_input_bytes"`
	FinalInputTokens         int64 `json:"final_input_tokens"`
	FinalDroppedGroups       int   `json:"final_dropped_groups"`
	FinalContextWindowTokens int   `json:"final_context_window_tokens"`
	FinalMaxOutputTokens     int   `json:"final_max_output_tokens"`
}

type routingContextRecorder struct {
	mu       sync.Mutex
	session  *core.Session
	evidence routingContextEvidence
}

func (r *routingContextRecorder) observe(_ core.ModelContext, assembled core.ModelContext, err error) {
	parents := r.programExecuteParents()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.evidence.Observed = true
	r.evidence.AssemblyCalls++
	if err != nil {
		r.evidence.AssemblyFailures++
		return
	}
	toolMessages, childMessages := 0, 0
	for _, message := range assembled.Messages {
		if message.Role == core.RoleTool {
			toolMessages++
			if routingIsExecutedChild(parents, message.ToolCallID) {
				childMessages++
			}
		}
	}
	r.evidence.FinalMessageCount = len(assembled.Messages)
	r.evidence.FinalToolMessageCount = toolMessages
	r.evidence.FinalChildToolMessages = childMessages
	r.evidence.FinalInputBytes = assembled.InputBytes
	r.evidence.FinalInputTokens = assembled.InputTokens
	r.evidence.FinalDroppedGroups = assembled.DroppedGroups
	r.evidence.FinalContextWindowTokens = assembled.ContextWindowTokens
	r.evidence.FinalMaxOutputTokens = assembled.MaxOutputTokens
}

func (r *routingContextRecorder) setSession(session *core.Session) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.session = session
}

func (r *routingContextRecorder) programExecuteParents() []string {
	r.mu.Lock()
	session := r.session
	r.mu.Unlock()
	if session == nil {
		return nil
	}
	calls, _, _, _ := liveSessionEvidence(session.Events())
	return routingTopLevelProgramExecuteIDs(calls)
}

func (r *routingContextRecorder) snapshot() routingContextEvidence {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.evidence
}

type routingExecutionEvidence struct {
	BoundaryCalls          int `json:"boundary_calls"`
	BoundaryResults        int `json:"boundary_results"`
	ExecutedChildCalls     int `json:"executed_child_calls"`
	ExecutedChildResults   int `json:"executed_child_results"`
	SucceededToolResults   int `json:"succeeded_tool_results"`
	FailedToolResults      int `json:"failed_tool_results"`
	BudgetDeniedToolResult int `json:"budget_denied_tool_results"`
}

func routingExecutionFromEvents(events []core.SessionEvent) routingExecutionEvidence {
	calls, results, _, _ := liveSessionEvidence(events)
	evidence := routingExecutionEvidence{}
	children := make(map[string]struct{})
	parents := routingTopLevelProgramExecuteIDs(calls)
	for _, call := range calls {
		isChild := routingIsExecutedChild(parents, call.CallID)
		if isChild {
			evidence.ExecutedChildCalls++
			children[call.CallID] = struct{}{}
		} else {
			evidence.BoundaryCalls++
		}
	}
	for _, result := range results {
		if _, child := children[result.CallID]; child {
			evidence.ExecutedChildResults++
		} else {
			evidence.BoundaryResults++
		}
		if result.OK {
			evidence.SucceededToolResults++
		} else {
			evidence.FailedToolResults++
		}
		if code, _ := result.Metadata["code"].(string); code == core.CodeBudgetExceeded {
			evidence.BudgetDeniedToolResult++
		}
	}
	return evidence
}

func routingIsExecutedChild(parents []string, callID string) bool {
	for _, parent := range parents {
		if isProgramChildCallID(parent, callID) {
			return true
		}
	}
	return false
}

func routingTopLevelProgramExecuteIDs(calls []core.ToolCallData) []string {
	parents := make([]string, 0)
	for _, candidate := range calls {
		if candidate.Name != programtools.ExecuteID {
			continue
		}
		topLevel := true
		for _, possibleParent := range calls {
			if possibleParent.Name == programtools.ExecuteID && possibleParent.CallID != candidate.CallID && isProgramChildCallID(possibleParent.CallID, candidate.CallID) {
				topLevel = false
				break
			}
		}
		if topLevel {
			parents = append(parents, candidate.CallID)
		}
	}
	return parents
}

type routingUsageEvidence struct {
	Complete     bool   `json:"complete"`
	InputTokens  *int64 `json:"input_tokens"`
	OutputTokens *int64 `json:"output_tokens"`
}

// routingUsageFromRounds returns nil token values when any ordinary adapter
// invocation is absent, malformed, or does not reconcile to the ledger. That
// represents incomplete usage as unknown rather than measured zero.
func routingUsageFromRounds(rounds []liveRound, invocations *liveInvocationEvidence) routingUsageEvidence {
	if invocations == nil || !invocations.AdapterCallsObserved || !invocations.ReportedUsageComplete || !invocations.UsageProtocolConsistent || !invocations.LedgerUsageMatched || !invocations.UniqueLedgerInvocationIDs {
		return routingUsageEvidence{}
	}
	var input, output int64
	for _, round := range rounds {
		if !round.adapterStarted || !round.hasUsage || !round.usageConsistent {
			return routingUsageEvidence{}
		}
		input += round.usage.InputTokens
		output += round.usage.OutputTokens
	}
	return routingUsageEvidence{Complete: true, InputTokens: &input, OutputTokens: &output}
}

func routingRoundsShareToolMenu(rounds []liveRound) bool {
	if len(rounds) == 0 || rounds[0].toolSchemaSHA256 == "" {
		return false
	}
	for _, round := range rounds {
		if round.toolSchemaSHA256 != rounds[0].toolSchemaSHA256 || !routingFourToolMenuNames(round.toolSchemaNames) {
			return false
		}
	}
	return true
}

func routingEvidenceRoundsShareToolMenu(rounds []liveEvidenceRound) bool {
	if len(rounds) == 0 || rounds[0].ToolSchemaSHA256 == "" {
		return false
	}
	for _, round := range rounds {
		if round.ToolSchemaSHA256 != rounds[0].ToolSchemaSHA256 || !routingFourToolMenuNames(round.ToolSchemaNames) {
			return false
		}
	}
	return true
}

func routingFourToolMenuNames(names []string) bool {
	if len(names) != 4 {
		return false
	}
	want := map[string]struct{}{programtools.CatalogID: {}, programtools.ExecuteID: {}, listToolID: {}, detailToolID: {}}
	for _, name := range names {
		if _, found := want[name]; !found {
			return false
		}
		delete(want, name)
	}
	return len(want) == 0
}

type routingEvidenceRecord struct {
	Schema           string                    `json:"schema"`
	Triad            int                       `json:"triad"`
	Arm              routingArm                `json:"arm"`
	ArmPosition      int                       `json:"arm_position"`
	RequestedModel   string                    `json:"requested_model"`
	Protocol         string                    `json:"protocol"`
	SourceRevision   string                    `json:"source_revision"`
	DatasetSHA256    string                    `json:"dataset_sha256"`
	PromptSHA256     string                    `json:"prompt_sha256"`
	ToolMenuSHA256   string                    `json:"tool_menu_sha256"`
	ModelRoundBudget int                       `json:"model_round_budget"`
	ToolCallBudget   int                       `json:"tool_call_budget"`
	RuntimeStatus    string                    `json:"runtime_status"`
	AcceptancePassed bool                      `json:"acceptance_passed"`
	TotalElapsedMS   int64                     `json:"total_elapsed_ms"`
	Rounds           []liveEvidenceRound       `json:"rounds"`
	AdapterCalls     *liveInvocationEvidence   `json:"adapter_calls"`
	Usage            routingUsageEvidence      `json:"usage"`
	Execution        routingExecutionEvidence  `json:"execution"`
	Context          routingContextEvidence    `json:"context"`
	Wire             []liveWireRequestEvidence `json:"wire"`
}

func routingEvidenceFromRun(triad, armPosition int, arm routingArm, dataset routingDataset, prompt, requestedModel string, result core.TurnResult, events []core.SessionEvent, model *liveModel, elapsed time.Duration, contextEvidence routingContextEvidence, wire []liveWireRequestEvidence, acceptancePassed bool) routingEvidenceRecord {
	record := routingEvidenceRecord{
		Schema: routingEvidenceSchema, Triad: triad, Arm: arm, ArmPosition: armPosition, RequestedModel: requestedModel, Protocol: liveProtocol(), SourceRevision: liveSourceRevision(),
		DatasetSHA256: dataset.hash(), PromptSHA256: liveSHA256(prompt), ModelRoundBudget: routingRoundBudget, ToolCallBudget: routingToolCallBudget,
		RuntimeStatus: liveRunStatus(result), AcceptancePassed: acceptancePassed, TotalElapsedMS: elapsed.Milliseconds(),
		Execution: routingExecutionFromEvents(events), Context: contextEvidence, Wire: append([]liveWireRequestEvidence(nil), wire...),
	}
	if model == nil {
		return record
	}
	rounds := model.evidenceRounds()
	record.AdapterCalls = liveInvocationEvidenceFromRounds(rounds, events)
	record.Usage = routingUsageFromRounds(rounds, record.AdapterCalls)
	for _, round := range rounds {
		record.Rounds = append(record.Rounds, liveEvidenceRound{
			Round: round.round, InputTokens: round.usage.InputTokens, OutputTokens: round.usage.OutputTokens, UsageReported: round.hasUsage,
			ToolNames: append([]string(nil), round.tools...), ToolCallCount: round.toolCalls, Finish: round.finish,
			HTTPStatus: round.status, ErrorClass: round.errorClass, ErrorType: round.errorType, ErrorCode: round.errorCode,
			PacingWaitMS: round.pacingWait.Milliseconds(), ProviderRequestMS: round.providerElapsed.Milliseconds(),
			SystemBytes: round.systemBytes, MessageBytes: round.messageBytes, ToolSchemaBytes: round.toolSchemaBytes, ToolSchemaNames: append([]string(nil), round.toolSchemaNames...), ToolSchemaSHA256: round.toolSchemaSHA256,
		})
	}
	if len(rounds) > 0 {
		record.ToolMenuSHA256 = rounds[0].toolSchemaSHA256
	}
	return record
}

func writeRoutingEvidence(directory string, record routingEvidenceRecord) error {
	if strings.TrimSpace(directory) == "" {
		return nil
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return errors.New("routing evidence directory is unavailable")
	}
	payload, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return errors.New("routing evidence encoding failed")
	}
	identity := sha256.Sum256([]byte(fmt.Sprintf("%d\x00%s\x00%s\x00%s", record.Triad, record.Arm, record.DatasetSHA256, time.Now().UTC().Format(time.RFC3339Nano))))
	return writeLiveEvidenceFile(filepath.Join(directory, "programmatic-live-routing-"+fmt.Sprintf("%x", identity[:12])+".json"), append(payload, '\n'))
}

func routingTriadsFromEnvironment(value string) (int, error) {
	if strings.TrimSpace(value) == "" {
		return 1, nil
	}
	count, err := strconv.Atoi(value)
	if err != nil || count < 1 || count > len(routingArmOrders) {
		return 0, errors.New("routing triad count is invalid")
	}
	return count, nil
}

func routingArmPosition(order []routingArm, arm routingArm) int {
	for index, candidate := range order {
		if candidate == arm {
			return index + 1
		}
	}
	return 0
}

// routingStopsSubsequentArms is deliberately narrower than a failed contract:
// liveModel sets this flag only for HTTP, transport, or Responses-protocol
// errors. Every other failed arm has already retained its cleanup evidence and
// leaves the remaining Latin-square schedule intact.
func routingStopsSubsequentArms(model *liveModel) bool {
	return model != nil && model.stopSubsequentCases()
}

// TestLiveRoutingEfficiencyTriad is an intentionally opt-in, matched routing
// experiment. One triad freezes a fresh eight-item opaque dataset and runs all
// three profile-only routing arms once. It makes real Responses requests only
// after the existing live guard is set. A failed contract arm is recorded and
// the remaining plan continues without retry; only transport/protocol failure
// stops later arms because their measurements would no longer be comparable.
func TestLiveRoutingEfficiencyTriad(t *testing.T) {
	if os.Getenv("HARNESS_PROGRAMMATIC_LIVE") != "1" && os.Getenv("HARNESS_ACCEPTANCE_LIVE_PROGRAMMATIC") != "1" {
		t.Skip("set HARNESS_PROGRAMMATIC_LIVE=1 to make intentional live model requests")
	}
	if liveProtocol() != "responses" {
		t.Skip("set HARNESS_PROGRAMMATIC_PROTOCOL=responses to retain routing wire evidence")
	}
	triads, err := routingTriadsFromEnvironment(os.Getenv("HARNESS_PROGRAMMATIC_ROUTING_TRIADS"))
	if err != nil {
		t.Fatal("routing triad count is invalid")
	}
	adapter, err := newLiveAdapter(context.Background())
	if err != nil {
		t.Fatal("live model configuration is incomplete or invalid")
	}
	modelID := strings.TrimSpace(os.Getenv("HARNESS_LLM_MODEL"))
	if modelID == "" {
		t.Fatal("live model configuration is incomplete or invalid")
	}
	interval, err := liveMinRequestInterval(os.Getenv("HARNESS_PROGRAMMATIC_MIN_REQUEST_INTERVAL"))
	if err != nil {
		t.Fatal("live request interval is invalid")
	}
	liveWireTransportMu.Lock()
	previousTransport := http.DefaultTransport
	if previousTransport == nil {
		liveWireTransportMu.Unlock()
		t.Fatal("default HTTP transport is unavailable")
	}
	transport := &liveWireAuditTransport{next: previousTransport}
	http.DefaultTransport = transport
	t.Cleanup(func() {
		http.DefaultTransport = previousTransport
		liveWireTransportMu.Unlock()
	})

	pacer := &liveRequestPacer{interval: interval}
	prompt := routingUserPrompt()
	for triad := 1; triad <= triads; triad++ {
		dataset, err := newRoutingDataset()
		if err != nil {
			t.Fatal("could not create routing fixture data")
		}
		for position, arm := range routingArmOrders[triad-1] {
			armPosition := position + 1
			stopSubsequentCases := false
			passed := t.Run(fmt.Sprintf("triad_%02d_%s", triad, arm), func(t *testing.T) {
				startWire := transport.count()
				contexts := &routingContextRecorder{}
				model := newLiveModel(t, adapter, string(arm), routingRoundBudget, pacer)
				var result core.TurnResult
				var runErr error
				var events []core.SessionEvent
				var elapsed time.Duration
				t.Cleanup(func() {
					record := routingEvidenceFromRun(triad, armPosition, arm, dataset, prompt, modelID, result, events, model, elapsed, contexts.snapshot(), transport.snapshotSince(startWire), !t.Failed() && result.Status == core.RunCompleted)
					if writeErr := writeRoutingEvidence(os.Getenv("HARNESS_PROGRAMMATIC_EVIDENCE_PATH"), record); writeErr != nil {
						t.Error("could not write routing experiment evidence")
					}
				})
				fixture, fixtureErr := newRoutingFixture(model, dataset, contexts.observe)
				if fixtureErr != nil {
					t.Fatal("could not construct routing fixture")
				}
				contexts.setSession(fixture.session)
				if fixtureErr = configureRoutingProfile(fixture, adapter.Provider(), modelID, arm); fixtureErr != nil {
					t.Fatal("could not configure routing profile")
				}
				started := time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
				defer cancel()
				result, runErr = fixture.runtime.RunTurn(ctx, fixture.principal, fixture.session, core.TurnInput{RunID: fmt.Sprintf("routing-%02d-%s", triad, arm), Text: prompt}, nil)
				elapsed = time.Since(started)
				events = fixture.session.Events()
				if runErr != nil || result.Status != core.RunCompleted {
					stopSubsequentCases = routingStopsSubsequentArms(model)
					t.Fatalf("routing arm did not complete (status=%s, model_rounds=%d)", result.Status, model.rounds())
				}
				assertRoutingArm(t, arm, fixture, dataset, result, model, contexts.snapshot(), transport.snapshotSince(startWire))
			})
			if !passed && stopSubsequentCases {
				t.Log("routing experiment stopped after an HTTP, transport, or Responses protocol failure; no retry was attempted")
				return
			}
			if !passed {
				t.Log("routing arm failed its local contract; evidence was retained and the remaining planned arms continue without retry")
			}
		}
	}
}

func assertRoutingArm(t *testing.T, arm routingArm, fixture *routingFixture, dataset routingDataset, result core.TurnResult, model *liveModel, contextEvidence routingContextEvidence, wire []liveWireRequestEvidence) {
	t.Helper()
	if model.rounds() > routingRoundBudget || fixture.list.calls() != 1 || fixture.detail.calls() != routingActiveRows {
		t.Fatal("routing arm exceeded its fixed model or fixture budget")
	}
	calls, results, usage, final := liveSessionEvidence(fixture.session.Events())
	if len(calls) > routingToolCallBudget || len(results) > routingToolCallBudget {
		t.Fatal("routing arm exceeded the fixed tool-call budget")
	}
	for _, result := range results {
		if !result.OK {
			t.Fatal("routing arm produced a failed tool result")
		}
	}
	want := "FINAL: " + strings.Join(dataset.names(), ", ")
	if strings.TrimSpace(result.Answer) != want || strings.TrimSpace(final) != want {
		t.Fatal("routing arm final answer does not match its private fixture")
	}
	modelUsage, complete := model.usage()
	if !complete || modelUsage != usage {
		t.Fatal("routing arm usage is incomplete or does not reconcile")
	}
	invocations := liveInvocationEvidenceFromRounds(model.evidenceRounds(), fixture.session.Events())
	if invocations == nil || !invocations.AdapterCallsObserved || invocations.ActualAdapterCalls != model.rounds() || !invocations.ReportedUsageComplete || !invocations.UsageProtocolConsistent || !invocations.LedgerUsageMatched || !invocations.UniqueLedgerInvocationIDs {
		t.Fatal("routing arm adapter-call evidence is incomplete")
	}
	if !contextEvidence.Observed || contextEvidence.AssemblyCalls != model.rounds() || contextEvidence.AssemblyFailures != 0 || contextEvidence.FinalContextWindowTokens <= contextEvidence.FinalMaxOutputTokens || contextEvidence.FinalMaxOutputTokens <= 0 {
		t.Fatal("routing arm context evidence is incomplete")
	}
	if len(wire) != model.rounds() {
		t.Fatal("routing arm wire count does not match actual adapter calls")
	}
	for _, request := range wire {
		if !request.BodyObserved || !request.JSONValid || request.ToolCount != 4 || !request.InstructionsPresent || !request.StreamPresent || !request.Stream || !request.ParallelToolCallsPresent || !request.ParallelToolCalls {
			t.Fatal("routing arm wire evidence does not describe the common four-tool Responses request")
		}
	}
	first := model.evidenceRounds()
	if !routingRoundsShareToolMenu(first) {
		t.Fatal("routing arm did not expose the common four-tool menu")
	}
	assertFixtureIDsOnce(t, calls, routingActiveRows)
	execution := routingExecutionFromEvents(fixture.session.Events())
	switch arm {
	case routingSequentialReact:
		if model.rounds() != routingRoundBudget || execution.BoundaryCalls != 9 || execution.ExecutedChildCalls != 0 {
			t.Fatal("sequential routing contract was not observed")
		}
	case routingParallelReact:
		if model.rounds() != 3 || model.toolCallCount(2) != routingActiveRows || execution.BoundaryCalls != 9 || execution.ExecutedChildCalls != 0 {
			t.Fatal("parallel routing contract was not observed")
		}
	case routingPTC:
		if model.rounds() != 3 || execution.BoundaryCalls != 2 || execution.ExecutedChildCalls != 9 {
			t.Fatal("PTC routing contract was not observed")
		}
		assertForcedProgramChildren(t, calls, results, model)
	default:
		t.Fatal("routing arm is invalid")
	}
}

type routingOfflineModel struct {
	arm      routingArm
	names    []string
	phase    int
	systems  []string
	menuHash []string
}

func (m *routingOfflineModel) Provider() string { return "routing-offline" }

func (m *routingOfflineModel) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	m.systems = append(m.systems, options.System)
	m.menuHash = append(m.menuHash, liveSHA256(options.Tools))
	usage := core.TokenUsage{InputTokens: 10, OutputTokens: 2}
	emitFinish := func() {
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls, Usage: &usage})
	}
	emitAnswer := func() {
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: "FINAL: " + strings.Join(m.names, ", ")})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop, Usage: &usage})
	}
	switch m.arm {
	case routingSequentialReact:
		if m.phase == 0 {
			m.phase++
			emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &core.ToolCall{ID: "seq-list", Name: listToolID, Args: map[string]any{}}})
			emitFinish()
			return nil
		}
		if m.phase <= routingActiveRows {
			id := fmt.Sprintf("item-%02d", m.phase)
			m.phase++
			emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &core.ToolCall{ID: "seq-" + id, Name: detailToolID, Args: map[string]any{"id": id}}})
			emitFinish()
			return nil
		}
		if m.phase == routingActiveRows+1 {
			m.phase++
			emitAnswer()
			return nil
		}
	case routingParallelReact:
		if m.phase == 0 {
			m.phase++
			emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &core.ToolCall{ID: "par-list", Name: listToolID, Args: map[string]any{}}})
			emitFinish()
			return nil
		}
		if m.phase == 1 {
			m.phase++
			calls := make([]core.ToolCall, routingActiveRows)
			for index := range calls {
				id := fmt.Sprintf("item-%02d", index+1)
				calls[index] = core.ToolCall{ID: "par-" + id, Name: detailToolID, Args: map[string]any{"id": id}}
			}
			emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCalls: calls})
			emitFinish()
			return nil
		}
		if m.phase == 2 {
			m.phase++
			emitAnswer()
			return nil
		}
	case routingPTC:
		if m.phase == 0 {
			m.phase++
			emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &core.ToolCall{ID: "ptc-catalog", Name: programtools.CatalogID, Args: map[string]any{}}})
			emitFinish()
			return nil
		}
		if m.phase == 1 {
			m.phase++
			catalog, err := lastToolJSON(options.Messages, "ptc-catalog")
			if err != nil {
				return err
			}
			bindings, err := catalogBindings(catalog, listToolID, detailToolID)
			if err != nil {
				return err
			}
			emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &core.ToolCall{ID: "ptc-execute", Name: programtools.ExecuteID, Args: map[string]any{"source": selectionProgram, "bindings": bindings}}})
			emitFinish()
			return nil
		}
		if m.phase == 2 {
			m.phase++
			emitAnswer()
			return nil
		}
	}
	return errors.New("routing offline model received an extra round")
}

func runOfflineRoutingArm(t *testing.T, arm routingArm, dataset routingDataset) (routingEvidenceRecord, *routingOfflineModel) {
	t.Helper()
	prompt := routingUserPrompt()
	inner := &routingOfflineModel{arm: arm, names: dataset.names()}
	model := newLiveModel(t, inner, "offline-"+string(arm), routingRoundBudget, nil)
	contexts := &routingContextRecorder{}
	fixture, err := newRoutingFixture(model, dataset, contexts.observe)
	if err != nil {
		t.Fatal("could not construct offline routing fixture")
	}
	contexts.setSession(fixture.session)
	if err := configureRoutingProfile(fixture, model.Provider(), "offline-routing-model", arm); err != nil {
		t.Fatal("could not configure offline routing profile")
	}
	started := time.Now()
	result, err := fixture.runtime.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{RunID: "offline-" + string(arm), Text: prompt}, nil)
	elapsed := time.Since(started)
	if err != nil || result.Status != core.RunCompleted {
		t.Fatal("offline routing arm did not complete")
	}
	assertRoutingArmOffline(t, arm, fixture, dataset, result, model, contexts.snapshot())
	return routingEvidenceFromRun(1, routingArmPosition(routingArmOrders[0], arm), arm, dataset, prompt, "offline-routing-model", result, fixture.session.Events(), model, elapsed, contexts.snapshot(), nil, true), inner
}

func assertRoutingArmOffline(t *testing.T, arm routingArm, fixture *routingFixture, dataset routingDataset, result core.TurnResult, model *liveModel, contextEvidence routingContextEvidence) {
	t.Helper()
	if model.rounds() > routingRoundBudget || fixture.list.calls() != 1 || fixture.detail.calls() != routingActiveRows {
		t.Fatal("offline routing arm exceeded its fixed budget")
	}
	calls, results, usage, final := liveSessionEvidence(fixture.session.Events())
	if len(calls) > routingToolCallBudget || len(results) > routingToolCallBudget || strings.TrimSpace(result.Answer) != "FINAL: "+strings.Join(dataset.names(), ", ") || strings.TrimSpace(final) != strings.TrimSpace(result.Answer) {
		t.Fatal("offline routing arm violated the fixture contract")
	}
	modelUsage, complete := model.usage()
	if !complete || modelUsage != usage || !contextEvidence.Observed || contextEvidence.AssemblyCalls != model.rounds() {
		t.Fatal("offline routing accounting is incomplete")
	}
	execution := routingExecutionFromEvents(fixture.session.Events())
	switch arm {
	case routingSequentialReact:
		if model.rounds() != routingRoundBudget || execution.BoundaryCalls != 9 || execution.ExecutedChildCalls != 0 {
			t.Fatal("offline sequential counts are wrong")
		}
	case routingParallelReact:
		if model.rounds() != 3 || model.toolCallCount(2) != routingActiveRows || execution.BoundaryCalls != 9 || execution.ExecutedChildCalls != 0 {
			t.Fatal("offline parallel counts are wrong")
		}
	case routingPTC:
		if model.rounds() != 3 || execution.BoundaryCalls != 2 || execution.ExecutedChildCalls != 9 {
			t.Fatal("offline PTC counts are wrong")
		}
		assertForcedProgramChildren(t, calls, results, model)
	}
}

func TestRoutingEfficiencyOfflineTriadIsMatchedAndSafe(t *testing.T) {
	dataset, err := newRoutingDataset()
	if err != nil {
		t.Fatal("could not create routing fixture data")
	}
	records := make([]routingEvidenceRecord, 0, 3)
	models := make([]*routingOfflineModel, 0, 3)
	for _, arm := range []routingArm{routingSequentialReact, routingParallelReact, routingPTC} {
		record, model := runOfflineRoutingArm(t, arm, dataset)
		records = append(records, record)
		models = append(models, model)
	}
	for index, record := range records {
		if record.Schema != routingEvidenceSchema || record.ArmPosition != index+1 || record.DatasetSHA256 != dataset.hash() || record.PromptSHA256 != liveSHA256(routingUserPrompt()) || record.ToolMenuSHA256 == "" || record.ToolMenuSHA256 != records[0].ToolMenuSHA256 || record.ModelRoundBudget != routingRoundBudget || record.ToolCallBudget != routingToolCallBudget || record.AdapterCalls == nil || record.AdapterCalls.ActualAdapterCalls != len(record.Rounds) || !record.AdapterCalls.ReportedUsageComplete || !record.AdapterCalls.LedgerUsageMatched || !record.Usage.Complete || record.Usage.InputTokens == nil || record.Usage.OutputTokens == nil || !record.Context.Observed || record.Wire != nil {
			t.Fatalf("offline routing record %d lost matched or accounting evidence: %#v", index, record)
		}
		if !routingEvidenceRoundsShareToolMenu(record.Rounds) {
			t.Fatal("offline routing record did not retain the same four-tool menu for every round")
		}
		payload, marshalErr := json.Marshal(record)
		if marshalErr != nil {
			t.Fatal("could not encode offline routing evidence")
		}
		for _, row := range dataset.rows {
			if strings.Contains(string(payload), row.Name) || strings.Contains(string(payload), routingUserPrompt()) || strings.Contains(string(payload), selectionProgram) {
				t.Fatal("routing evidence retained opaque data, prompt, or PTC source")
			}
		}
	}
	baseSystem := ""
	for index, model := range models {
		if len(model.systems) == 0 || len(model.menuHash) == 0 {
			t.Fatal("offline routing model did not observe a model request")
		}
		instruction, _ := routingInstruction(records[index].Arm)
		normalized := strings.ReplaceAll(model.systems[0], instruction, "<routing-arm>")
		if baseSystem == "" {
			baseSystem = normalized
		} else if normalized != baseSystem {
			t.Fatal("routing profiles differed outside their route instruction")
		}
	}
}

type routingFailureAdapter struct{ calls int }

func (*routingFailureAdapter) Provider() string { return "routing-failure" }

func (m *routingFailureAdapter) Stream(context.Context, core.GenerateOptions, func(core.StreamChunk)) error {
	m.calls++
	return errors.New("synthetic routing provider failure")
}

func TestRoutingEfficiencyOfflineFailureDoesNotRetryArm(t *testing.T) {
	dataset, err := newRoutingDataset()
	if err != nil {
		t.Fatal("could not create routing fixture data")
	}
	inner := &routingFailureAdapter{}
	model := newLiveModel(t, inner, "offline-failure", routingRoundBudget, nil)
	fixture, err := newRoutingFixture(model, dataset, nil)
	if err != nil {
		t.Fatal("could not construct failure routing fixture")
	}
	if err := configureRoutingProfile(fixture, model.Provider(), "offline-failure-model", routingParallelReact); err != nil {
		t.Fatal("could not configure failure routing profile")
	}
	result, runErr := fixture.runtime.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{RunID: "offline-failure", Text: routingUserPrompt()}, nil)
	if runErr == nil || result.Status != core.RunFailed || inner.calls != 1 || model.rounds() != 1 || fixture.list.calls() != 0 || fixture.detail.calls() != 0 {
		t.Fatal("failed routing arm was retried or executed a tool")
	}
	record := routingEvidenceFromRun(1, 2, routingParallelReact, dataset, routingUserPrompt(), "offline-failure-model", result, fixture.session.Events(), model, 0, routingContextEvidence{}, nil, false)
	if record.AdapterCalls == nil || record.AdapterCalls.ActualAdapterCalls != 1 || record.AdapterCalls.ReportedUsageComplete || record.AdapterCalls.LedgerUsageMatched || record.Usage.Complete || record.Usage.InputTokens != nil || record.Usage.OutputTokens != nil || record.Execution.BoundaryCalls != 0 || record.Execution.ExecutedChildCalls != 0 {
		t.Fatal("failed routing arm evidence hid the non-retried failure")
	}
}

func TestRoutingTriadCountEnvironment(t *testing.T) {
	for _, test := range []struct {
		value string
		want  int
		valid bool
	}{
		{"", 1, true}, {"2", 2, true}, {"0", 0, false}, {"7", 0, false}, {"not-a-number", 0, false},
	} {
		got, err := routingTriadsFromEnvironment(test.value)
		if (err == nil) != test.valid || got != test.want {
			t.Fatalf("triad count %q got=%d err=%v", test.value, got, err)
		}
	}
}

func TestRoutingFailurePolicyContinuesLogicalFailures(t *testing.T) {
	logicalFailure := &liveModel{}
	if routingStopsSubsequentArms(logicalFailure) {
		t.Fatal("logical routing failure stopped the remaining planned arms")
	}
	upstreamFailure := &liveModel{stopCases: true}
	if !routingStopsSubsequentArms(upstreamFailure) {
		t.Fatal("upstream routing failure did not stop the configured model")
	}
}

func TestRoutingArmOrdersAreBalancedAndPredeclared(t *testing.T) {
	if len(routingArmOrders) != 6 {
		t.Fatal("routing experiment did not retain all six predeclared orders")
	}
	seenOrders := map[string]struct{}{}
	positions := map[routingArm]map[int]int{}
	for _, arm := range []routingArm{routingSequentialReact, routingParallelReact, routingPTC} {
		positions[arm] = map[int]int{}
	}
	for _, order := range routingArmOrders {
		if len(order) != 3 {
			t.Fatal("routing order has the wrong arm count")
		}
		key := strings.Join([]string{string(order[0]), string(order[1]), string(order[2])}, ",")
		if _, duplicate := seenOrders[key]; duplicate {
			t.Fatal("routing order is duplicated")
		}
		seenOrders[key] = struct{}{}
		for position, arm := range order {
			if routingArmPosition(order, arm) != position+1 {
				t.Fatal("routing arm position was not derived from the predeclared order")
			}
			positions[arm][position+1]++
		}
	}
	for arm, counts := range positions {
		for position := 1; position <= 3; position++ {
			if counts[position] != 2 {
				t.Fatalf("arm %q occurs %d times in position %d, want 2", arm, counts[position], position)
			}
		}
	}
}

func TestRoutingExecutionCountsOnlyExactProgramChildren(t *testing.T) {
	parent := "program-parent"
	child := parent + "/" + strings.Repeat("a", 64)
	notChild := parent + "/not-a-valid-child"
	events := []core.SessionEvent{
		{Type: core.EvToolCall, Data: liveEvidenceJSON(t, core.ToolCallData{CallID: parent, Name: programtools.ExecuteID})},
		{Type: core.EvToolCall, Data: liveEvidenceJSON(t, core.ToolCallData{CallID: child, Name: detailToolID})},
		{Type: core.EvToolCall, Data: liveEvidenceJSON(t, core.ToolCallData{CallID: notChild, Name: detailToolID})},
		{Type: core.EvToolResult, Data: liveEvidenceJSON(t, core.ToolResultData{CallID: parent, OK: true})},
		{Type: core.EvToolResult, Data: liveEvidenceJSON(t, core.ToolResultData{CallID: child, OK: true})},
		{Type: core.EvToolResult, Data: liveEvidenceJSON(t, core.ToolResultData{CallID: notChild, OK: true})},
	}
	evidence := routingExecutionFromEvents(events)
	if evidence.BoundaryCalls != 2 || evidence.BoundaryResults != 2 || evidence.ExecutedChildCalls != 1 || evidence.ExecutedChildResults != 1 {
		t.Fatalf("routing child classification accepted a non-child slash ID: %#v", evidence)
	}
}

func TestRoutingToolMenuRequiresEveryRoundToMatch(t *testing.T) {
	valid := []liveRound{
		{toolSchemaSHA256: "menu", toolSchemaNames: []string{programtools.CatalogID, programtools.ExecuteID, listToolID, detailToolID}},
		{toolSchemaSHA256: "menu", toolSchemaNames: []string{programtools.CatalogID, programtools.ExecuteID, listToolID, detailToolID}},
	}
	if !routingRoundsShareToolMenu(valid) {
		t.Fatal("matching four-tool rounds were rejected")
	}
	invalidHash := append([]liveRound(nil), valid...)
	invalidHash[1].toolSchemaSHA256 = "different"
	if routingRoundsShareToolMenu(invalidHash) {
		t.Fatal("mismatched later-round menu hash was accepted")
	}
	invalidNames := append([]liveRound(nil), valid...)
	invalidNames[1].toolSchemaNames = []string{programtools.CatalogID, programtools.ExecuteID, listToolID}
	if routingRoundsShareToolMenu(invalidNames) {
		t.Fatal("mismatched later-round menu names were accepted")
	}
}

func TestRoutingUsageTotalsAreUnknownWhenIncomplete(t *testing.T) {
	rounds := []liveRound{{adapterStarted: true, hasUsage: true, usageConsistent: true, usage: core.TokenUsage{InputTokens: 7, OutputTokens: 3}}}
	complete := &liveInvocationEvidence{AdapterCallsObserved: true, ReportedUsageComplete: true, UsageProtocolConsistent: true, LedgerUsageMatched: true, UniqueLedgerInvocationIDs: true}
	usage := routingUsageFromRounds(rounds, complete)
	if !usage.Complete || usage.InputTokens == nil || usage.OutputTokens == nil || *usage.InputTokens != 7 || *usage.OutputTokens != 3 {
		t.Fatalf("complete routing usage totals=%#v", usage)
	}
	unknown := routingUsageFromRounds(rounds, &liveInvocationEvidence{AdapterCallsObserved: true, ReportedUsageComplete: false})
	if unknown.Complete || unknown.InputTokens != nil || unknown.OutputTokens != nil {
		t.Fatalf("incomplete routing usage was represented as a numeric total: %#v", unknown)
	}
}
