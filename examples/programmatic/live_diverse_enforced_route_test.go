package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/whhhh1500/auto-agent/internal/executionroute"
	programtools "github.com/whhhh1500/auto-agent/pkg/adapter/programmatic/toolcapability"
	access "github.com/whhhh1500/auto-agent/pkg/app/programmatic"
	"github.com/whhhh1500/auto-agent/pkg/app/runexecutor"
	core "github.com/whhhh1500/auto-agent/pkg/core"
)

const (
	diverseEnforcedRouteEvidenceSchema = "harness.programmatic.live-diverse-enforced-route/v1"
	diverseEnforcedRouteLiveEnv        = "HARNESS_PROGRAMMATIC_DIVERSE_ENFORCED_ROUTE_LIVE"
	diverseEnforcedRouteEvidenceEnv    = "HARNESS_PROGRAMMATIC_DIVERSE_ENFORCED_ROUTE_EVIDENCE_PATH"

	// Each matrix has two tasks and three route arms. Every arm is capped by
	// routingRoundBudget, so a one-model live invocation starts at most 60
	// adapter requests. The test has no retry path.
	diverseEnforcedRouteMaximumRequestsPerModel = 2 * 3 * routingRoundBudget
)

type diverseEnforcedRouteTask struct {
	name           string
	fixtureVariant string
	activeRows     int
	detailPadding  int
	prompt         string
}

func diverseEnforcedRouteTasks() []diverseEnforcedRouteTask {
	return []diverseEnforcedRouteTask{
		{
			name:           "n1_simple",
			fixtureVariant: "compact_n1_v1",
			activeRows:     1,
			prompt:         "Use only the currently exposed read-only tools. Read the one active inventory item's detail exactly once and ignore the inactive item. Reply with exactly one line: FINAL: 1.",
		},
		{
			name:           "filter_loop_large_detail",
			fixtureVariant: largeDetailFixtureVariant,
			activeRows:     8,
			detailPadding:  2 << 10,
			prompt:         "Use only the currently exposed read-only tools. For every active inventory row, read its detail exactly once and ignore the inactive row. Reply with exactly one line: FINAL: 8.",
		},
	}
}

// diverseEnforcedRouteCountProgram implements the task's data-dependent
// filter and loop inside the bounded PTC interpreter. It calls every active
// detail tool, but deliberately returns only the count. That makes the final
// PTC model context small without skipping a required protected child effect.
// It is kept in process only and is never written to evidence.
const diverseEnforcedRouteCountProgram = `{"version":"ptc-ir/v1","body":[
  {"op":"call","assign":"listed","tool":"fixture.inventory","args":{"op":"map","entries":{}}},
  {"op":"assign","name":"count","value":{"op":"literal","value":0}},
  {"op":"for","var":"row","in":{"op":"get","object":{"op":"var","name":"listed"},"key":"data"},"body":[
    {"op":"if","cond":{"op":"cmp","kind":"eq","left":{"op":"get","object":{"op":"var","name":"row"},"key":"active"},"right":{"op":"literal","value":true}},"then":[
      {"op":"call","tool":"fixture.detail","args":{"op":"map","entries":{"id":{"op":"get","object":{"op":"var","name":"row"},"key":"id"}}}},
      {"op":"assign","name":"count","value":{"op":"int","kind":"add","left":{"op":"var","name":"count"},"right":{"op":"literal","value":1}}}
    ]}
  ]},
  {"op":"return","value":{"op":"map","entries":{"active_detail_count":{"op":"var","name":"count"}}}}
]}`

// diverseEnforcedRouteEvidenceRecord persists public structure and aggregate
// measurements only. It never contains the prompt text, PTC source, fixture
// payload, model output, tool arguments/results, endpoint, or credentials.
type diverseEnforcedRouteEvidenceRecord struct {
	Schema                     string                    `json:"schema"`
	Task                       string                    `json:"task"`
	FixtureVariant             string                    `json:"fixture_variant"`
	Arm                        string                    `json:"arm"`
	RequestedModel             string                    `json:"requested_model"`
	Protocol                   string                    `json:"protocol"`
	SourceRevision             string                    `json:"source_revision"`
	DatasetSHA256              string                    `json:"dataset_sha256"`
	PromptSHA256               string                    `json:"prompt_sha256"`
	FrozenRoute                enforcedRouteFrozen       `json:"frozen_route"`
	FrozenCapabilityCount      int                       `json:"frozen_capability_count"`
	FrozenCapabilityMenuSHA256 string                    `json:"frozen_capability_menu_sha256"`
	ModelRoundBudget           int                       `json:"model_round_budget"`
	ToolCallBudget             int                       `json:"tool_call_budget"`
	ArmAttempts                int                       `json:"arm_attempts"`
	NoRetryObserved            bool                      `json:"no_retry_observed"`
	ObservedRoute              string                    `json:"observed_route"`
	RuntimeStatus              string                    `json:"runtime_status"`
	AcceptancePassed           bool                      `json:"acceptance_passed"`
	TotalElapsedMS             int64                     `json:"total_elapsed_ms"`
	Rounds                     []liveEvidenceRound       `json:"rounds"`
	AdapterCalls               *liveInvocationEvidence   `json:"adapter_calls,omitempty"`
	Usage                      routingUsageEvidence      `json:"usage"`
	Execution                  routingExecutionEvidence  `json:"execution"`
	Context                    routingContextEvidence    `json:"context"`
	Wire                       []liveWireRequestEvidence `json:"wire,omitempty"`
}

func (task diverseEnforcedRouteTask) expectedAnswer() string {
	return fmt.Sprintf("FINAL: %d", task.activeRows)
}

func (task diverseEnforcedRouteTask) datasetSHA256() string {
	inventory, inventoryErr := fixtureInventoryContent(task.activeRows)
	if inventoryErr != nil {
		return ""
	}
	values := make([]string, 0, task.activeRows+1)
	values = append(values, inventory)
	for index := 1; index <= task.activeRows; index++ {
		value, err := fixtureDetailContent(fmt.Sprintf("item-%02d", index), task.activeRows, task.detailPadding)
		if err != nil {
			return ""
		}
		values = append(values, value)
	}
	return liveSHA256(values)
}

func configureDiverseEnforcedRouteProfile(fixture *fixture, provider, modelID string, mode access.RouteMode) error {
	if fixture == nil {
		return errors.New("diverse enforced-route fixture is nil")
	}
	selection := core.ModelSelection{Provider: provider, Model: modelID}
	return fixture.runtime.Profiles.Bind(core.AgentProfileLayer{
		Scope: fixture.session.Scope(), ProfileID: fixtureProfileID, Model: &selection,
		MaxSteps: intPointer(routingRoundBudget), MaxToolCalls: intPointer(routingToolCallBudget),
		Metadata: map[string]string{
			executionroute.RouteVersionKey: executionroute.RouteVersion,
			executionroute.RouteModeKey:    string(mode),
		},
	})
}

// runDiverseEnforcedRouteArm deliberately follows the same production path as
// the server: Resolve freezes the route composition, then the default
// executor drives the Runtime. No test-local route adapter is substituted.
func runDiverseEnforcedRouteArm(ctx context.Context, fixture *fixture, runID, prompt string) (core.TurnResult, error) {
	registry, err := runexecutor.NewDefaultRegistry()
	if err != nil {
		return core.TurnResult{}, err
	}
	decision, err := executionroute.Resolve(ctx, executionroute.Request{
		Runtime: fixture.runtime, Registry: registry, Principal: fixture.principal, Session: fixture.session, RunID: runID,
	})
	if err != nil {
		return core.TurnResult{}, err
	}
	return decision.Executor.RunTurn(ctx, fixture.principal, fixture.session, core.TurnInput{
		RunID: runID, Text: prompt, CompositionMetadata: decision.CompositionMetadata,
	}, nil)
}

func diverseEnforcedRouteRecordFromRun(task diverseEnforcedRouteTask, mode access.RouteMode, requestedModel, runID string, result core.TurnResult, events []core.SessionEvent, model *liveModel, elapsed time.Duration, contextEvidence routingContextEvidence, wire []liveWireRequestEvidence, acceptancePassed bool) diverseEnforcedRouteEvidenceRecord {
	record := diverseEnforcedRouteEvidenceRecord{
		Schema: diverseEnforcedRouteEvidenceSchema, Task: task.name, FixtureVariant: task.fixtureVariant, Arm: string(mode),
		RequestedModel: requestedModel, Protocol: liveProtocol(), SourceRevision: liveSourceRevision(),
		DatasetSHA256: task.datasetSHA256(), PromptSHA256: liveSHA256(task.prompt),
		ModelRoundBudget: routingRoundBudget, ToolCallBudget: routingToolCallBudget, ArmAttempts: 1,
		RuntimeStatus: liveRunStatus(result), AcceptancePassed: acceptancePassed, TotalElapsedMS: elapsed.Milliseconds(),
		Execution: routingExecutionFromEvents(events), Context: contextEvidence, Wire: append([]liveWireRequestEvidence(nil), wire...),
	}
	record.FrozenRoute, record.FrozenCapabilityCount, record.FrozenCapabilityMenuSHA256, _ = enforcedRouteFrozenFromEvents(events, runID)
	if model == nil {
		return record
	}
	rounds := model.evidenceRounds()
	record.AdapterCalls = liveInvocationEvidenceFromRounds(rounds, events)
	record.Usage = routingUsageFromRounds(rounds, record.AdapterCalls)
	record.NoRetryObserved = record.AdapterCalls != nil && record.AdapterCalls.ActualAdapterCalls == len(rounds) && record.AdapterCalls.PacingCanceledBeforeAdapter == 0
	for _, round := range rounds {
		record.Rounds = append(record.Rounds, liveEvidenceRound{
			Round: round.round, InputTokens: round.usage.InputTokens, OutputTokens: round.usage.OutputTokens, UsageReported: round.hasUsage,
			ToolNames: append([]string(nil), round.tools...), ToolCallCount: round.toolCalls, Finish: round.finish,
			HTTPStatus: round.status, ErrorClass: round.errorClass, ErrorType: round.errorType, ErrorCode: round.errorCode,
			PacingWaitMS: round.pacingWait.Milliseconds(), ProviderRequestMS: round.providerElapsed.Milliseconds(),
			SystemBytes: round.systemBytes, MessageBytes: round.messageBytes, ToolSchemaBytes: round.toolSchemaBytes,
			ToolSchemaNames: append([]string(nil), round.toolSchemaNames...), ToolSchemaSHA256: round.toolSchemaSHA256,
		})
	}
	record.ObservedRoute, _ = classifyEnforcedRoute(events, rounds)
	return record
}

func writeDiverseEnforcedRouteEvidence(directory string, record diverseEnforcedRouteEvidenceRecord) error {
	if strings.TrimSpace(directory) == "" {
		return nil
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return errors.New("diverse enforced-route evidence directory is unavailable")
	}
	payload, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return errors.New("diverse enforced-route evidence encoding failed")
	}
	identity := liveSHA256([]string{record.Task, record.Arm, record.DatasetSHA256, time.Now().UTC().Format(time.RFC3339Nano)})
	if len(identity) < 24 {
		return errors.New("diverse enforced-route evidence identity is unavailable")
	}
	return writeLiveEvidenceFile(filepath.Join(directory, "programmatic-live-diverse-enforced-route-"+identity[:24]+".json"), append(payload, '\n'))
}

// TestLiveDiverseEnforcedRouteMatrix is a separate opt-in live acceptance
// matrix. A caller runs it once per requested model; it never loops models or
// retries an arm. The 5-second pacing floor keeps each intentional request
// visibly serialized, including the mixed auto-first-action arms.
func TestLiveDiverseEnforcedRouteMatrix(t *testing.T) {
	if os.Getenv(diverseEnforcedRouteLiveEnv) != "1" {
		t.Skip("set HARNESS_PROGRAMMATIC_DIVERSE_ENFORCED_ROUTE_LIVE=1 to make intentional live model requests")
	}
	if liveProtocol() != "responses" {
		t.Skip("set HARNESS_PROGRAMMATIC_PROTOCOL=responses to retain diverse-route wire evidence")
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
	if err != nil || interval < 5*time.Second {
		t.Fatal("diverse-route live request pacing must be at least five seconds")
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
	records := make([]diverseEnforcedRouteEvidenceRecord, 0, len(diverseEnforcedRouteTasks())*3)
	for _, task := range diverseEnforcedRouteTasks() {
		stopSubsequentArms := false
		for _, mode := range []access.RouteMode{access.RouteAutoFirstAction, access.RouteDirectOnly, access.RoutePTCOnly} {
			passed := t.Run(task.name+"/"+string(mode), func(t *testing.T) {
				runID := "diverse-enforced-route-" + task.name + "-" + string(mode)
				startWire := transport.count()
				contexts := &routingContextRecorder{}
				model := newLiveModel(t, adapter, runID, routingRoundBudget, pacer)
				var result core.TurnResult
				var events []core.SessionEvent
				var elapsed time.Duration
				var fixture *fixture
				t.Cleanup(func() {
					record := diverseEnforcedRouteRecordFromRun(task, mode, modelID, runID, result, events, model, elapsed, contexts.snapshot(), transport.snapshotSince(startWire), !t.Failed() && result.Status == core.RunCompleted)
					if err := writeDiverseEnforcedRouteEvidence(os.Getenv(diverseEnforcedRouteEvidenceEnv), record); err != nil {
						t.Error("could not write diverse enforced-route evidence")
					}
				})

				fixture, err = newFixtureWithDataOptions(model, true, task.activeRows, fixtureDataOptions{
					OutputContracts: true, DetailIrrelevantBytes: task.detailPadding, ObserveAssembledContext: contexts.observe,
				})
				if err != nil {
					t.Fatal("could not construct diverse enforced-route fixture")
				}
				contexts.setSession(fixture.session)
				if err := configureDiverseEnforcedRouteProfile(fixture, adapter.Provider(), modelID, mode); err != nil {
					t.Fatal("could not configure diverse enforced-route profile")
				}

				started := time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
				defer cancel()
				result, err = runDiverseEnforcedRouteArm(ctx, fixture, runID, task.prompt)
				elapsed = time.Since(started)
				events = fixture.session.Events()
				if err != nil || result.Status != core.RunCompleted {
					stopSubsequentArms = routingStopsSubsequentArms(model)
					t.Fatalf("diverse enforced-route arm did not complete (status=%s, model_rounds=%d)", result.Status, model.rounds())
				}
				observed := assertDiverseEnforcedRouteArm(t, task, mode, fixture, runID, result, model, contexts.snapshot(), transport.snapshotSince(startWire), true)
				if task.name == "n1_simple" && mode == access.RouteAutoFirstAction && observed != "direct" {
					t.Fatal("n1 auto arm did not choose the direct route")
				}
				records = append(records, diverseEnforcedRouteRecordFromRun(task, mode, modelID, runID, result, events, model, elapsed, contexts.snapshot(), transport.snapshotSince(startWire), true))
			})
			if !passed && stopSubsequentArms {
				t.Log("diverse-route experiment stopped after an HTTP, transport, or Responses protocol failure; no retry was attempted")
				return
			}
			if !passed {
				t.Log("diverse-route arm failed its logical contract; evidence was retained and the remaining planned arms continue without retry")
			}
		}
	}
	assertDiverseEnforcedRouteMatrix(t, records)
}

func assertDiverseEnforcedRouteArm(t *testing.T, task diverseEnforcedRouteTask, mode access.RouteMode, fixture *fixture, runID string, result core.TurnResult, model *liveModel, contextEvidence routingContextEvidence, wire []liveWireRequestEvidence, requireWire bool) string {
	t.Helper()
	if model.rounds() > routingRoundBudget || fixture.list.calls() != 1 || fixture.detail.calls() != task.activeRows {
		t.Fatal("diverse enforced-route arm exceeded its fixed model or fixture budget")
	}
	calls, results, usage, final := liveSessionEvidence(fixture.session.Events())
	if len(calls) > routingToolCallBudget || len(results) > routingToolCallBudget {
		t.Fatal("diverse enforced-route arm exceeded its fixed tool-call budget")
	}
	for _, toolResult := range results {
		if !toolResult.OK {
			t.Fatal("diverse enforced-route arm produced a failed tool result")
		}
	}
	if strings.TrimSpace(result.Answer) != task.expectedAnswer() || strings.TrimSpace(final) != task.expectedAnswer() {
		t.Fatal("diverse enforced-route arm did not return the exact task aggregate")
	}
	assertFixtureIDsOnce(t, calls, task.activeRows)
	modelUsage, complete := model.usage()
	if !complete || modelUsage != usage {
		t.Fatal("diverse enforced-route usage is incomplete or does not reconcile")
	}
	rounds := model.evidenceRounds()
	invocations := liveInvocationEvidenceFromRounds(rounds, fixture.session.Events())
	if invocations == nil || !invocations.AdapterCallsObserved || invocations.ActualAdapterCalls != model.rounds() || invocations.PacingCanceledBeforeAdapter != 0 || !invocations.ReportedUsageComplete || !invocations.UsageProtocolConsistent || !invocations.LedgerUsageMatched || !invocations.UniqueLedgerInvocationIDs {
		t.Fatal("diverse enforced-route adapter invocation accounting is incomplete")
	}
	if !contextEvidence.Observed || contextEvidence.AssemblyCalls != model.rounds() || contextEvidence.AssemblyFailures != 0 || contextEvidence.FinalInputBytes <= 0 || contextEvidence.FinalInputTokens <= 0 || contextEvidence.FinalContextWindowTokens <= contextEvidence.FinalMaxOutputTokens || contextEvidence.FinalMaxOutputTokens <= 0 {
		t.Fatal("diverse enforced-route context evidence is incomplete")
	}
	if requireWire {
		if len(wire) != model.rounds() || !enforcedRouteWireStructuralValid(enforcedRouteWireEvidence(rounds, wire)) {
			t.Fatal("diverse enforced-route wire evidence does not match the projected tool menu")
		}
	}
	for _, round := range rounds {
		if round.toolSchemaSHA256 == "" {
			t.Fatal("diverse enforced-route model round omitted actual tool-schema evidence")
		}
	}
	observed, classified := classifyEnforcedRoute(fixture.session.Events(), rounds)
	if !classified || !enforcedRouteMatchesMode(mode, observed) {
		t.Fatal("diverse enforced-route path was not derived from projected schemas and durable events")
	}
	execution := routingExecutionFromEvents(fixture.session.Events())
	fixtureCalls := task.activeRows + 1
	if observed == "direct" {
		if execution.BoundaryCalls != fixtureCalls || execution.BoundaryResults != fixtureCalls || execution.ExecutedChildCalls != 0 || execution.ExecutedChildResults != 0 || !model.selectedFixtureTool() {
			t.Fatal("diverse direct route did not retain direct-only boundaries")
		}
	}
	if observed == "catalog_execute" {
		if execution.BoundaryCalls != 2 || execution.BoundaryResults != 2 || execution.ExecutedChildCalls != fixtureCalls || execution.ExecutedChildResults != fixtureCalls || model.selectedFixtureTool() {
			t.Fatal("diverse PTC route did not retain protected filter-loop children")
		}
	}
	frozen, capabilityCount, capabilityHash, found := enforcedRouteFrozenFromEvents(fixture.session.Events(), runID)
	if !found || frozen.Version != executionroute.RouteVersion || frozen.Mode != string(mode) || frozen.CatalogToolID != programtools.CatalogID || frozen.ExecuteToolID != programtools.ExecuteID || frozen.ImplementationRevision != executionroute.RouteImplementationRevision || capabilityCount != 4 || capabilityHash == "" {
		t.Fatal("diverse enforced-route run did not freeze the shared resolver composition")
	}
	return observed
}

func assertDiverseEnforcedRouteMatrix(t *testing.T, records []diverseEnforcedRouteEvidenceRecord) {
	t.Helper()
	tasks := diverseEnforcedRouteTasks()
	if len(records) != len(tasks)*3 {
		t.Fatal("diverse enforced-route matrix did not retain every planned arm")
	}
	byTask := make(map[string]map[string]diverseEnforcedRouteEvidenceRecord, len(tasks))
	for _, record := range records {
		if !diverseEnforcedRouteRecordInvariant(record) {
			t.Fatal("diverse enforced-route record lost required safe evidence")
		}
		arms := byTask[record.Task]
		if arms == nil {
			arms = make(map[string]diverseEnforcedRouteEvidenceRecord, 3)
			byTask[record.Task] = arms
		}
		if _, duplicate := arms[record.Arm]; duplicate {
			t.Fatal("diverse enforced-route matrix repeated an arm")
		}
		arms[record.Arm] = record
	}
	for _, task := range tasks {
		arms := byTask[task.name]
		auto, autoOK := arms[string(access.RouteAutoFirstAction)]
		direct, directOK := arms[string(access.RouteDirectOnly)]
		ptc, ptcOK := arms[string(access.RoutePTCOnly)]
		if !autoOK || !directOK || !ptcOK {
			t.Fatal("diverse enforced-route task did not retain all three routes")
		}
		for _, record := range []diverseEnforcedRouteEvidenceRecord{auto, direct, ptc} {
			if record.DatasetSHA256 != task.datasetSHA256() || record.PromptSHA256 != liveSHA256(task.prompt) || record.FrozenCapabilityCount != 4 || record.FrozenCapabilityMenuSHA256 != auto.FrozenCapabilityMenuSHA256 || record.ModelRoundBudget != routingRoundBudget || record.ToolCallBudget != routingToolCallBudget {
				t.Fatal("diverse enforced-route task changed data, capability universe, or budget between arms")
			}
		}
		if task.name == "n1_simple" && auto.ObservedRoute != "direct" {
			t.Fatal("n1 auto result did not select direct")
		}
		if task.name == "filter_loop_large_detail" {
			if direct.ObservedRoute != "direct" || ptc.ObservedRoute != "catalog_execute" || direct.Context.FinalInputBytes <= ptc.Context.FinalInputBytes || direct.Context.FinalToolMessageCount <= ptc.Context.FinalToolMessageCount {
				t.Fatal("filter-loop PTC arm did not demonstrate a smaller final model context than the matched direct arm")
			}
		}
	}
}

func diverseEnforcedRouteRecordInvariant(record diverseEnforcedRouteEvidenceRecord) bool {
	if record.Schema != diverseEnforcedRouteEvidenceSchema || record.Task == "" || record.FixtureVariant == "" || record.ArmAttempts != 1 || !record.NoRetryObserved || record.DatasetSHA256 == "" || record.PromptSHA256 == "" || record.FrozenCapabilityCount != 4 || record.FrozenCapabilityMenuSHA256 == "" || record.ModelRoundBudget != routingRoundBudget || record.ToolCallBudget != routingToolCallBudget || !record.AcceptancePassed || record.RuntimeStatus != string(core.RunCompleted) {
		return false
	}
	if record.FrozenRoute.Version != executionroute.RouteVersion || record.FrozenRoute.Mode != record.Arm || record.FrozenRoute.CatalogToolID != programtools.CatalogID || record.FrozenRoute.ExecuteToolID != programtools.ExecuteID || record.FrozenRoute.ImplementationRevision != executionroute.RouteImplementationRevision {
		return false
	}
	if record.AdapterCalls == nil || !record.AdapterCalls.AdapterCallsObserved || record.AdapterCalls.PacingCanceledBeforeAdapter != 0 || !record.AdapterCalls.ReportedUsageComplete || !record.AdapterCalls.UsageProtocolConsistent || !record.AdapterCalls.LedgerUsageMatched || !record.AdapterCalls.UniqueLedgerInvocationIDs || record.AdapterCalls.ActualAdapterCalls != len(record.Rounds) || !record.Usage.Complete || record.Usage.InputTokens == nil || record.Usage.OutputTokens == nil || !record.Context.Observed || record.Context.AssemblyFailures != 0 || record.Context.FinalInputBytes <= 0 || record.Context.FinalInputTokens <= 0 {
		return false
	}
	for _, round := range record.Rounds {
		if round.ToolSchemaSHA256 == "" {
			return false
		}
	}
	if !enforcedRouteMatchesMode(access.RouteMode(record.Arm), record.ObservedRoute) {
		return false
	}
	if record.ObservedRoute == "direct" {
		return record.Execution.BoundaryCalls > 0 && record.Execution.BoundaryCalls == record.Execution.BoundaryResults && record.Execution.ExecutedChildCalls == 0 && record.Execution.ExecutedChildResults == 0
	}
	return record.ObservedRoute == "catalog_execute" && record.Execution.BoundaryCalls == 2 && record.Execution.BoundaryResults == 2 && record.Execution.ExecutedChildCalls > 0 && record.Execution.ExecutedChildCalls == record.Execution.ExecutedChildResults
}

type diverseEnforcedRouteOfflineModel struct {
	task  diverseEnforcedRouteTask
	mode  access.RouteMode
	phase int
}

func (*diverseEnforcedRouteOfflineModel) Provider() string { return "diverse-enforced-route-offline" }

func (m *diverseEnforcedRouteOfflineModel) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	usage := core.TokenUsage{InputTokens: 10, OutputTokens: 2}
	emitFinish := func() {
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls, Usage: &usage})
	}
	emitAnswer := func() {
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: m.task.expectedAnswer()})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop, Usage: &usage})
	}
	menu := liveToolSchemaNames(options.Tools)
	switch m.mode {
	case access.RoutePTCOnly:
		switch m.phase {
		case 0:
			if !exactEnforcedMenu(menu, programtools.CatalogID) {
				return errors.New("offline diverse PTC initial menu was not catalog only")
			}
			m.phase++
			emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &core.ToolCall{ID: "diverse-catalog", Name: programtools.CatalogID, Args: map[string]any{}}})
			emitFinish()
			return nil
		case 1:
			if !exactEnforcedMenu(menu, programtools.ExecuteID) {
				return errors.New("offline diverse PTC execute menu was not locked")
			}
			catalog, err := lastToolJSON(options.Messages, "diverse-catalog")
			if err != nil {
				return err
			}
			bindings, err := catalogBindings(catalog, listToolID, detailToolID)
			if err != nil {
				return err
			}
			m.phase++
			emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &core.ToolCall{ID: "diverse-execute", Name: programtools.ExecuteID, Args: map[string]any{"source": diverseEnforcedRouteCountProgram, "bindings": bindings}}})
			emitFinish()
			return nil
		case 2:
			if !exactEnforcedMenu(menu) {
				return errors.New("offline diverse PTC final menu was not closed")
			}
			m.phase++
			emitAnswer()
			return nil
		}
	case access.RouteAutoFirstAction:
		if m.phase == 0 && !exactEnforcedMenu(menu, programtools.CatalogID, listToolID, detailToolID) {
			return errors.New("offline diverse auto initial menu was not mixed discovery")
		}
		fallthrough
	case access.RouteDirectOnly:
		if m.phase > 0 && !exactEnforcedMenu(menu, listToolID, detailToolID) {
			return errors.New("offline diverse direct menu was not locked")
		}
		if m.mode == access.RouteDirectOnly && !exactEnforcedMenu(menu, listToolID, detailToolID) {
			return errors.New("offline diverse direct-only menu exposed program tools")
		}
		switch m.phase {
		case 0:
			m.phase++
			emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &core.ToolCall{ID: "diverse-list", Name: listToolID, Args: map[string]any{}}})
			emitFinish()
			return nil
		case 1:
			calls := make([]core.ToolCall, m.task.activeRows)
			for index := range calls {
				calls[index] = core.ToolCall{ID: fmt.Sprintf("diverse-detail-%02d", index+1), Name: detailToolID, Args: map[string]any{"id": fmt.Sprintf("item-%02d", index+1)}}
			}
			m.phase++
			emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCalls: calls})
			emitFinish()
			return nil
		case 2:
			m.phase++
			emitAnswer()
			return nil
		}
	}
	return errors.New("offline diverse enforced-route model received an unexpected round")
}

func runOfflineDiverseEnforcedRouteArm(t *testing.T, task diverseEnforcedRouteTask, mode access.RouteMode) diverseEnforcedRouteEvidenceRecord {
	t.Helper()
	inner := &diverseEnforcedRouteOfflineModel{task: task, mode: mode}
	model := newLiveModel(t, inner, "offline-diverse-"+task.name+"-"+string(mode), routingRoundBudget, nil)
	contexts := &routingContextRecorder{}
	fixture, err := newFixtureWithDataOptions(model, true, task.activeRows, fixtureDataOptions{
		OutputContracts: true, DetailIrrelevantBytes: task.detailPadding, ObserveAssembledContext: contexts.observe,
	})
	if err != nil {
		t.Fatal("could not construct offline diverse enforced-route fixture")
	}
	contexts.setSession(fixture.session)
	if err := configureDiverseEnforcedRouteProfile(fixture, model.Provider(), "offline-diverse-model", mode); err != nil {
		t.Fatal("could not configure offline diverse enforced-route profile")
	}
	runID := "offline-diverse-" + task.name + "-" + string(mode)
	started := time.Now()
	result, err := runDiverseEnforcedRouteArm(context.Background(), fixture, runID, task.prompt)
	elapsed := time.Since(started)
	if err != nil || result.Status != core.RunCompleted {
		t.Fatal("offline diverse enforced-route arm did not complete")
	}
	observed := assertDiverseEnforcedRouteArm(t, task, mode, fixture, runID, result, model, contexts.snapshot(), nil, false)
	if task.name == "n1_simple" && mode == access.RouteAutoFirstAction && observed != "direct" {
		t.Fatal("offline n1 auto arm did not choose direct")
	}
	return diverseEnforcedRouteRecordFromRun(task, mode, "offline-diverse-model", runID, result, fixture.session.Events(), model, elapsed, contexts.snapshot(), nil, true)
}

func TestDiverseEnforcedRouteMatrixOfflineUsesSharedResolverAndSafeEvidence(t *testing.T) {
	t.Setenv(diverseEnforcedRouteLiveEnv, "")
	records := make([]diverseEnforcedRouteEvidenceRecord, 0, len(diverseEnforcedRouteTasks())*3)
	for _, task := range diverseEnforcedRouteTasks() {
		for _, mode := range []access.RouteMode{access.RouteAutoFirstAction, access.RouteDirectOnly, access.RoutePTCOnly} {
			records = append(records, runOfflineDiverseEnforcedRouteArm(t, task, mode))
		}
	}
	assertDiverseEnforcedRouteMatrix(t, records)
	for _, record := range records {
		payload, err := json.Marshal(record)
		if err != nil {
			t.Fatal("could not encode offline diverse enforced-route evidence")
		}
		for _, task := range diverseEnforcedRouteTasks() {
			if strings.Contains(string(payload), task.prompt) || strings.Contains(string(payload), diverseEnforcedRouteCountProgram) || strings.Contains(string(payload), "irrelevant_read_only_text") || strings.Contains(string(payload), "Item 01") {
				t.Fatal("offline diverse enforced-route evidence retained prompt, source, or fixture content")
			}
		}
	}
}

func TestDiverseEnforcedRoutePromptsDoNotInstructRouting(t *testing.T) {
	for _, task := range diverseEnforcedRouteTasks() {
		prompt := strings.ToLower(task.prompt)
		for _, forbidden := range []string{"program.catalog", "program.execute", "ptc", "direct route", "route mode"} {
			if strings.Contains(prompt, forbidden) {
				t.Fatal("diverse-route prompt named a routing mechanism")
			}
		}
		if task.datasetSHA256() == "" {
			t.Fatal("diverse-route fixture dataset hash is unavailable")
		}
	}
}

func TestDiverseEnforcedRouteRequestBound(t *testing.T) {
	if diverseEnforcedRouteMaximumRequestsPerModel != 60 {
		t.Fatal("diverse-route per-model request bound drifted")
	}
}
