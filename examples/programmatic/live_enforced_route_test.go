package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
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
	enforcedRouteEvidenceSchema = "harness.programmatic.live-enforced-route/v1"
	enforcedRouteLiveEnv        = "HARNESS_PROGRAMMATIC_ENFORCED_ROUTE_LIVE"
	enforcedRouteEvidenceEnv    = "HARNESS_PROGRAMMATIC_ENFORCED_ROUTE_EVIDENCE_PATH"
)

type enforcedRouteFrozen struct {
	Version                string `json:"version"`
	Mode                   string `json:"mode"`
	CatalogToolID          string `json:"catalog_tool_id"`
	ExecuteToolID          string `json:"execute_tool_id"`
	ImplementationRevision string `json:"implementation_revision"`
}

// enforcedRouteEvidenceRecord deliberately contains no prompt, generated
// program, opaque fixture value, model text, tool arguments, or tool result.
// Tool names and frozen route IDs are public experiment structure.
type enforcedRouteEvidenceRecord struct {
	Schema                     string                    `json:"schema"`
	Triad                      int                       `json:"triad"`
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
	Wire                       []liveWireRequestEvidence `json:"wire"`
}

func configureEnforcedRouteBaseProfile(fixture *routingFixture, provider, modelID string) error {
	selection := core.ModelSelection{Provider: provider, Model: modelID}
	return fixture.runtime.Profiles.Bind(core.AgentProfileLayer{
		Scope: fixture.session.Scope(), ProfileID: fixtureProfileID, Model: &selection,
		MaxSteps: intPointer(routingRoundBudget), MaxToolCalls: intPointer(routingToolCallBudget),
	})
}

// configureEnforcedRouteProfile owns the only arm-specific profile values.
// Resolver defaults expand those two values into the complete frozen route
// envelope persisted at run/start.
func configureEnforcedRouteProfile(fixture *routingFixture, mode access.RouteMode) error {
	return fixture.runtime.Profiles.Bind(core.AgentProfileLayer{
		Scope: fixture.session.Scope(), ProfileID: fixtureProfileID,
		Metadata: map[string]string{
			executionroute.RouteVersionKey: executionroute.RouteVersion,
			executionroute.RouteModeKey:    string(mode),
		},
	})
}

func runEnforcedRouteArm(ctx context.Context, fixture *routingFixture, runID string) (core.TurnResult, error) {
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
		RunID: runID, Text: routingUserPrompt(), CompositionMetadata: decision.CompositionMetadata,
	}, nil)
}

func enforcedRouteFrozenFromEvents(events []core.SessionEvent, runID string) (enforcedRouteFrozen, int, string, bool) {
	for _, event := range events {
		if event.RunID != runID || event.Type != core.EvRunStart {
			continue
		}
		var data core.RunStartData
		if json.Unmarshal(event.Data, &data) != nil || data.Composition == nil {
			return enforcedRouteFrozen{}, 0, "", false
		}
		metadata := data.Composition.Metadata
		frozen := enforcedRouteFrozen{
			Version: metadata[executionroute.RouteVersionKey], Mode: metadata[executionroute.RouteModeKey],
			CatalogToolID: metadata[executionroute.RouteCatalogToolIDKey], ExecuteToolID: metadata[executionroute.RouteExecuteToolIDKey],
			ImplementationRevision: metadata[executionroute.RouteImplementationKey],
		}
		capabilities := make([]string, 0, len(data.Composition.Capabilities))
		for _, capability := range data.Composition.Capabilities {
			capabilities = append(capabilities, capability.Manifest.ID)
		}
		sort.Strings(capabilities)
		return frozen, len(capabilities), liveSHA256(capabilities), true
	}
	return enforcedRouteFrozen{}, 0, "", false
}

func enforcedRouteRecordFromRun(triad int, mode access.RouteMode, dataset routingDataset, requestedModel, runID string, result core.TurnResult, events []core.SessionEvent, model *liveModel, elapsed time.Duration, contextEvidence routingContextEvidence, wire []liveWireRequestEvidence, acceptancePassed bool) enforcedRouteEvidenceRecord {
	record := enforcedRouteEvidenceRecord{
		Schema: enforcedRouteEvidenceSchema, Triad: triad, Arm: string(mode), RequestedModel: requestedModel,
		Protocol: liveProtocol(), SourceRevision: liveSourceRevision(), DatasetSHA256: dataset.hash(), PromptSHA256: liveSHA256(routingUserPrompt()),
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

func writeEnforcedRouteEvidence(directory string, record enforcedRouteEvidenceRecord) error {
	if strings.TrimSpace(directory) == "" {
		return nil
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return errors.New("enforced-route evidence directory is unavailable")
	}
	payload, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return errors.New("enforced-route evidence encoding failed")
	}
	identity := sha256.Sum256([]byte(fmt.Sprintf("%d\x00%s\x00%s\x00%s", record.Triad, record.Arm, record.DatasetSHA256, time.Now().UTC().Format(time.RFC3339Nano))))
	return writeLiveEvidenceFile(filepath.Join(directory, "programmatic-live-enforced-route-"+fmt.Sprintf("%x", identity[:12])+".json"), append(payload, '\n'))
}

// TestLiveEnforcedRouteTriad intentionally makes real requests only through
// the separate opt-in switch. Each run resolves the default executor registry
// through executionroute.Resolve, which is the production shared route path.
func TestLiveEnforcedRouteTriad(t *testing.T) {
	if os.Getenv(enforcedRouteLiveEnv) != "1" {
		t.Skip("set HARNESS_PROGRAMMATIC_ENFORCED_ROUTE_LIVE=1 to make intentional live model requests")
	}
	if liveProtocol() != "responses" {
		t.Skip("set HARNESS_PROGRAMMATIC_PROTOCOL=responses to retain enforced-route wire evidence")
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
	dataset, err := newRoutingDataset()
	if err != nil {
		t.Fatal("could not create enforced-route fixture data")
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
	for _, mode := range []access.RouteMode{access.RouteAutoFirstAction, access.RouteDirectOnly, access.RoutePTCOnly} {
		stopSubsequentArms := false
		passed := t.Run(string(mode), func(t *testing.T) {
			runID := "enforced-route-" + string(mode)
			startWire := transport.count()
			contexts := &routingContextRecorder{}
			model := newLiveModel(t, adapter, runID, routingRoundBudget, pacer)
			var result core.TurnResult
			var events []core.SessionEvent
			var elapsed time.Duration
			t.Cleanup(func() {
				record := enforcedRouteRecordFromRun(1, mode, dataset, modelID, runID, result, events, model, elapsed, contexts.snapshot(), transport.snapshotSince(startWire), !t.Failed() && result.Status == core.RunCompleted)
				if writeErr := writeEnforcedRouteEvidence(os.Getenv(enforcedRouteEvidenceEnv), record); writeErr != nil {
					t.Error("could not write enforced-route evidence")
				}
			})
			fixture, fixtureErr := newRoutingFixtureWithSessionID(runID+"-session", model, dataset, contexts.observe)
			if fixtureErr != nil {
				t.Fatal("could not construct enforced-route fixture")
			}
			contexts.setSession(fixture.session)
			if err := configureEnforcedRouteBaseProfile(fixture, adapter.Provider(), modelID); err != nil {
				t.Fatal("could not configure common enforced-route profile")
			}
			if err := configureEnforcedRouteProfile(fixture, mode); err != nil {
				t.Fatal("could not configure enforced-route metadata")
			}
			started := time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			var runErr error
			result, runErr = runEnforcedRouteArm(ctx, fixture, runID)
			elapsed = time.Since(started)
			events = fixture.session.Events()
			if runErr != nil || result.Status != core.RunCompleted {
				stopSubsequentArms = routingStopsSubsequentArms(model)
				t.Fatalf("enforced-route arm did not complete (status=%s, model_rounds=%d)", result.Status, model.rounds())
			}
			assertEnforcedRouteArm(t, mode, fixture, dataset, runID, result, model, contexts.snapshot(), transport.snapshotSince(startWire))
		})
		if !passed && stopSubsequentArms {
			t.Log("enforced-route experiment stopped after an HTTP, transport, or Responses protocol failure; no retry was attempted")
			return
		}
		if !passed {
			t.Log("enforced-route arm failed its logical contract; evidence was retained and the remaining planned arms continue without retry")
		}
	}
}

func assertEnforcedRouteArm(t *testing.T, mode access.RouteMode, fixture *routingFixture, dataset routingDataset, runID string, result core.TurnResult, model *liveModel, contextEvidence routingContextEvidence, wire []liveWireRequestEvidence) {
	t.Helper()
	if model.rounds() > routingRoundBudget || fixture.list.calls() != 1 || fixture.detail.calls() != routingActiveRows {
		t.Fatal("enforced-route arm exceeded its fixed model or fixture budget")
	}
	calls, results, usage, final := liveSessionEvidence(fixture.session.Events())
	if len(calls) > routingToolCallBudget || len(results) > routingToolCallBudget {
		t.Fatal("enforced-route arm exceeded the fixed tool-call budget")
	}
	for _, toolResult := range results {
		if !toolResult.OK {
			t.Fatal("enforced-route arm produced a failed tool result")
		}
	}
	want := "FINAL: " + strings.Join(dataset.names(), ", ")
	if strings.TrimSpace(result.Answer) != want || strings.TrimSpace(final) != want {
		t.Fatal("enforced-route arm final answer does not match its private fixture")
	}
	modelUsage, complete := model.usage()
	if !complete || modelUsage != usage {
		t.Fatal("enforced-route arm usage is incomplete or does not reconcile")
	}
	rounds := model.evidenceRounds()
	for _, round := range rounds {
		if round.toolSchemaSHA256 == "" {
			t.Fatal("enforced-route model round omitted actual tool-schema evidence")
		}
	}
	invocations := liveInvocationEvidenceFromRounds(rounds, fixture.session.Events())
	if invocations == nil || !invocations.AdapterCallsObserved || invocations.ActualAdapterCalls != model.rounds() || !invocations.ReportedUsageComplete || !invocations.UsageProtocolConsistent || !invocations.LedgerUsageMatched || !invocations.UniqueLedgerInvocationIDs {
		t.Fatal("enforced-route adapter invocation accounting is incomplete")
	}
	if invocations.PacingCanceledBeforeAdapter != 0 {
		t.Fatal("enforced-route arm retried or canceled before its adapter attempt")
	}
	if !contextEvidence.Observed || contextEvidence.AssemblyCalls != model.rounds() || contextEvidence.AssemblyFailures != 0 || contextEvidence.FinalContextWindowTokens <= contextEvidence.FinalMaxOutputTokens || contextEvidence.FinalMaxOutputTokens <= 0 {
		t.Fatal("enforced-route context evidence is incomplete")
	}
	if len(wire) != model.rounds() {
		t.Fatal("enforced-route wire count does not match actual adapter calls")
	}
	if !enforcedRouteWireStructuralValid(enforcedRouteWireEvidence(rounds, wire)) {
		t.Fatal("enforced-route wire evidence does not match its projected tool menu")
	}
	assertFixtureIDsOnce(t, calls, routingActiveRows)
	observed, classified := classifyEnforcedRoute(fixture.session.Events(), rounds)
	if !classified || !enforcedRouteMatchesMode(mode, observed) {
		t.Fatal("enforced route was not derived from the actual tool schemas and session events")
	}
	execution := routingExecutionFromEvents(fixture.session.Events())
	if observed == "direct" && (execution.BoundaryCalls != 9 || execution.BoundaryResults != 9 || execution.ExecutedChildCalls != 0 || execution.ExecutedChildResults != 0) {
		t.Fatal("enforced direct route did not retain direct-only boundaries")
	}
	if observed == "catalog_execute" && (execution.BoundaryCalls != 2 || execution.BoundaryResults != 2 || execution.ExecutedChildCalls != 9 || execution.ExecutedChildResults != 9) {
		t.Fatal("enforced PTC route did not retain exact program-child coverage")
	}
	frozen, capabilityCount, capabilityHash, found := enforcedRouteFrozenFromEvents(fixture.session.Events(), runID)
	if !found || frozen.Version != executionroute.RouteVersion || frozen.Mode != string(mode) || frozen.CatalogToolID != programtools.CatalogID || frozen.ExecuteToolID != programtools.ExecuteID || frozen.ImplementationRevision != executionroute.RouteImplementationRevision || capabilityCount != 4 || capabilityHash == "" {
		t.Fatal("enforced-route run did not freeze the shared resolver composition")
	}
}

// enforcedRouteWireStructuralValid evaluates only the safe structural evidence
// persisted with an enforced-route record. It deliberately does not require a
// top-level instructions field: the shared route is derived from actual tool
// schemas and session events, and Responses may omit that optional field.
func enforcedRouteWireStructuralValid(record enforcedRouteEvidenceRecord) bool {
	if len(record.Wire) != len(record.Rounds) {
		return false
	}
	for index, request := range record.Wire {
		toolCount := len(record.Rounds[index].ToolSchemaNames)
		if !request.BodyObserved || !request.JSONValid || !request.StreamPresent || !request.Stream || !request.StorePresent || request.Store || request.ToolCount != toolCount {
			return false
		}
		if toolCount > 0 && (!request.ParallelToolCallsPresent || !request.ParallelToolCalls) {
			return false
		}
	}
	return true
}

func enforcedRouteWireEvidence(rounds []liveRound, wire []liveWireRequestEvidence) enforcedRouteEvidenceRecord {
	record := enforcedRouteEvidenceRecord{Wire: append([]liveWireRequestEvidence(nil), wire...)}
	for _, round := range rounds {
		record.Rounds = append(record.Rounds, liveEvidenceRound{ToolSchemaNames: append([]string(nil), round.toolSchemaNames...)})
	}
	return record
}

func enforcedRouteMatchesMode(mode access.RouteMode, observed string) bool {
	switch mode {
	case access.RouteDirectOnly:
		return observed == "direct"
	case access.RoutePTCOnly:
		return observed == "catalog_execute"
	case access.RouteAutoFirstAction:
		return observed == "direct" || observed == "catalog_execute"
	default:
		return false
	}
}

func exactEnforcedMenu(names []string, want ...string) bool {
	if len(names) != len(want) {
		return false
	}
	remaining := make(map[string]struct{}, len(want))
	for _, name := range want {
		remaining[name] = struct{}{}
	}
	if len(remaining) != len(want) {
		return false
	}
	for _, name := range names {
		if _, ok := remaining[name]; !ok {
			return false
		}
		delete(remaining, name)
	}
	return len(remaining) == 0
}

func enforcedTopLevelCalls(events []core.SessionEvent) []core.ToolCallData {
	calls, _, _, _ := liveSessionEvidence(events)
	parents := routingTopLevelProgramExecuteIDs(calls)
	out := make([]core.ToolCallData, 0, len(calls))
	for _, call := range calls {
		if !routingIsExecutedChild(parents, call.CallID) {
			out = append(out, call)
		}
	}
	return out
}

// classifyEnforcedRoute derives the actual route without inspecting a prompt
// or profile instruction. It accepts only a host-locked direct sequence or a
// catalog-then-execute sequence based on every projected tool schema and the
// durable call boundaries.
func classifyEnforcedRoute(events []core.SessionEvent, rounds []liveRound) (string, bool) {
	if len(rounds) == 0 {
		return "", false
	}
	boundary := enforcedTopLevelCalls(events)
	if len(boundary) == 0 {
		return "", false
	}
	directMenu := func(round liveRound) bool { return exactEnforcedMenu(round.toolSchemaNames, listToolID, detailToolID) }
	ptcMenus := func(firstCatalogOnly bool) bool {
		if len(rounds) != 3 || !exactEnforcedMenu(rounds[1].toolSchemaNames, programtools.ExecuteID) || !exactEnforcedMenu(rounds[2].toolSchemaNames) {
			return false
		}
		return !firstCatalogOnly || exactEnforcedMenu(rounds[0].toolSchemaNames, programtools.CatalogID)
	}
	directBoundary := func() bool {
		for _, call := range boundary {
			if call.Name == programtools.CatalogID || call.Name == programtools.ExecuteID {
				return false
			}
		}
		return true
	}
	ptcBoundary := func() bool {
		return len(boundary) == 2 && boundary[0].Name == programtools.CatalogID && boundary[1].Name == programtools.ExecuteID
	}

	switch {
	case directMenu(rounds[0]):
		for _, round := range rounds {
			if !directMenu(round) {
				return "", false
			}
		}
		return "direct", directBoundary()
	case exactEnforcedMenu(rounds[0].toolSchemaNames, programtools.CatalogID):
		return "catalog_execute", ptcMenus(true) && ptcBoundary()
	case exactEnforcedMenu(rounds[0].toolSchemaNames, programtools.CatalogID, listToolID, detailToolID):
		if boundary[0].Name == programtools.CatalogID {
			return "catalog_execute", ptcMenus(false) && ptcBoundary()
		}
		for index, round := range rounds {
			if index > 0 && !directMenu(round) {
				return "", false
			}
		}
		return "direct", directBoundary()
	default:
		return "", false
	}
}

type enforcedRouteOfflineModel struct {
	mode       access.RouteMode
	names      []string
	phase      int
	menuHashes []string
}

func (*enforcedRouteOfflineModel) Provider() string { return "enforced-route-offline" }

func (m *enforcedRouteOfflineModel) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	m.menuHashes = append(m.menuHashes, liveSHA256(options.Tools))
	usage := core.TokenUsage{InputTokens: 10, OutputTokens: 2}
	emitFinish := func() {
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls, Usage: &usage})
	}
	emitAnswer := func() {
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: "FINAL: " + strings.Join(m.names, ", ")})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop, Usage: &usage})
	}

	menu := liveToolSchemaNames(options.Tools)
	switch m.mode {
	case access.RoutePTCOnly:
		switch m.phase {
		case 0:
			if !exactEnforcedMenu(menu, programtools.CatalogID) {
				return errors.New("offline PTC initial menu was not catalog only")
			}
			m.phase++
			emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &core.ToolCall{ID: "enforced-catalog", Name: programtools.CatalogID, Args: map[string]any{}}})
			emitFinish()
			return nil
		case 1:
			if !exactEnforcedMenu(menu, programtools.ExecuteID) {
				return errors.New("offline PTC execute menu was not locked")
			}
			catalog, err := lastToolJSON(options.Messages, "enforced-catalog")
			if err != nil {
				return err
			}
			bindings, err := catalogBindings(catalog, listToolID, detailToolID)
			if err != nil {
				return err
			}
			m.phase++
			emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &core.ToolCall{ID: "enforced-execute", Name: programtools.ExecuteID, Args: map[string]any{"source": selectionProgram, "bindings": bindings}}})
			emitFinish()
			return nil
		case 2:
			if !exactEnforcedMenu(menu) {
				return errors.New("offline PTC final menu was not closed")
			}
			m.phase++
			emitAnswer()
			return nil
		}
	case access.RouteAutoFirstAction:
		if m.phase == 0 && !exactEnforcedMenu(menu, programtools.CatalogID, listToolID, detailToolID) {
			return errors.New("offline auto initial menu was not mixed discovery")
		}
		fallthrough
	case access.RouteDirectOnly:
		if m.phase > 0 && !exactEnforcedMenu(menu, listToolID, detailToolID) {
			return errors.New("offline direct menu was not host locked")
		}
		if m.mode == access.RouteDirectOnly && !exactEnforcedMenu(menu, listToolID, detailToolID) {
			return errors.New("offline direct-only menu exposed program tools")
		}
		if m.phase == 0 {
			m.phase++
			emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &core.ToolCall{ID: "enforced-list", Name: listToolID, Args: map[string]any{}}})
			emitFinish()
			return nil
		}
		if m.phase <= routingActiveRows {
			id := fmt.Sprintf("item-%02d", m.phase)
			m.phase++
			emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &core.ToolCall{ID: "enforced-" + id, Name: detailToolID, Args: map[string]any{"id": id}}})
			emitFinish()
			return nil
		}
		if m.phase == routingActiveRows+1 {
			m.phase++
			emitAnswer()
			return nil
		}
	}
	return errors.New("offline enforced-route model received an unexpected round")
}

func runOfflineEnforcedRouteArm(t *testing.T, mode access.RouteMode, dataset routingDataset) enforcedRouteEvidenceRecord {
	t.Helper()
	inner := &enforcedRouteOfflineModel{mode: mode, names: dataset.names()}
	model := newLiveModel(t, inner, "offline-enforced-"+string(mode), routingRoundBudget, nil)
	contexts := &routingContextRecorder{}
	runID := "offline-enforced-" + string(mode)
	fixture, err := newRoutingFixtureWithSessionID(runID+"-session", model, dataset, contexts.observe)
	if err != nil {
		t.Fatal("could not construct offline enforced-route fixture")
	}
	contexts.setSession(fixture.session)
	if err := configureEnforcedRouteBaseProfile(fixture, model.Provider(), "offline-enforced-model"); err != nil {
		t.Fatal("could not configure common offline enforced-route profile")
	}
	if err := configureEnforcedRouteProfile(fixture, mode); err != nil {
		t.Fatal("could not configure offline enforced-route metadata")
	}
	started := time.Now()
	result, err := runEnforcedRouteArm(context.Background(), fixture, runID)
	elapsed := time.Since(started)
	if err != nil || result.Status != core.RunCompleted {
		t.Fatal("offline enforced-route arm did not complete")
	}
	assertEnforcedRouteArmOffline(t, mode, fixture, dataset, runID, result, model, contexts.snapshot())
	return enforcedRouteRecordFromRun(1, mode, dataset, "offline-enforced-model", runID, result, fixture.session.Events(), model, elapsed, contexts.snapshot(), []liveWireRequestEvidence{}, true)
}

func assertEnforcedRouteArmOffline(t *testing.T, mode access.RouteMode, fixture *routingFixture, dataset routingDataset, runID string, result core.TurnResult, model *liveModel, contextEvidence routingContextEvidence) {
	t.Helper()
	if model.rounds() > routingRoundBudget || fixture.list.calls() != 1 || fixture.detail.calls() != routingActiveRows {
		t.Fatal("offline enforced-route arm exceeded its fixed budget")
	}
	calls, results, usage, final := liveSessionEvidence(fixture.session.Events())
	if len(calls) > routingToolCallBudget || len(results) > routingToolCallBudget || strings.TrimSpace(result.Answer) != "FINAL: "+strings.Join(dataset.names(), ", ") || strings.TrimSpace(final) != strings.TrimSpace(result.Answer) {
		t.Fatal("offline enforced-route arm violated the fixture contract")
	}
	modelUsage, complete := model.usage()
	if !complete || modelUsage != usage || !contextEvidence.Observed || contextEvidence.AssemblyCalls != model.rounds() {
		t.Fatal("offline enforced-route accounting is incomplete")
	}
	observed, classified := classifyEnforcedRoute(fixture.session.Events(), model.evidenceRounds())
	if !classified || !enforcedRouteMatchesMode(mode, observed) {
		t.Fatal("offline enforced-route classification did not reflect shared resolver output")
	}
	execution := routingExecutionFromEvents(fixture.session.Events())
	if observed == "direct" && (execution.BoundaryCalls != 9 || execution.ExecutedChildCalls != 0) {
		t.Fatal("offline direct route did not retain direct-only boundaries")
	}
	if observed == "catalog_execute" && (execution.BoundaryCalls != 2 || execution.ExecutedChildCalls != 9 || execution.ExecutedChildResults != 9) {
		t.Fatal("offline PTC route did not retain exact program-child coverage")
	}
	frozen, capabilities, capabilityHash, found := enforcedRouteFrozenFromEvents(fixture.session.Events(), runID)
	if !found || frozen.Mode != string(mode) || frozen.Version != executionroute.RouteVersion || capabilities != 4 || capabilityHash == "" {
		t.Fatal("offline enforced-route resolver did not freeze its composition")
	}
}

func enforcedRouteRecordInvariant(record enforcedRouteEvidenceRecord) bool {
	if record.Schema != enforcedRouteEvidenceSchema || record.ArmAttempts != 1 || !record.NoRetryObserved || record.DatasetSHA256 == "" || record.PromptSHA256 == "" || record.FrozenCapabilityCount != 4 || record.FrozenCapabilityMenuSHA256 == "" || record.ModelRoundBudget != routingRoundBudget || record.ToolCallBudget != routingToolCallBudget {
		return false
	}
	if record.FrozenRoute.Version != executionroute.RouteVersion || record.FrozenRoute.Mode != record.Arm || record.FrozenRoute.CatalogToolID != programtools.CatalogID || record.FrozenRoute.ExecuteToolID != programtools.ExecuteID || record.FrozenRoute.ImplementationRevision != executionroute.RouteImplementationRevision {
		return false
	}
	if record.AdapterCalls == nil || !record.AdapterCalls.AdapterCallsObserved || record.AdapterCalls.PacingCanceledBeforeAdapter != 0 || !record.AdapterCalls.ReportedUsageComplete || !record.AdapterCalls.UsageProtocolConsistent || !record.AdapterCalls.LedgerUsageMatched || !record.AdapterCalls.UniqueLedgerInvocationIDs || record.AdapterCalls.ActualAdapterCalls != len(record.Rounds) || !record.Usage.Complete || record.Usage.InputTokens == nil || record.Usage.OutputTokens == nil || !record.Context.Observed || record.Context.AssemblyFailures != 0 || !record.AcceptancePassed || record.RuntimeStatus != string(core.RunCompleted) {
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
		return record.Execution.BoundaryCalls == 9 && record.Execution.BoundaryResults == 9 && record.Execution.ExecutedChildCalls == 0 && record.Execution.ExecutedChildResults == 0
	}
	return record.Execution.BoundaryCalls == 2 && record.Execution.BoundaryResults == 2 && record.Execution.ExecutedChildCalls == 9 && record.Execution.ExecutedChildResults == 9
}

func TestEnforcedRouteOfflineTriadUsesSharedResolverWithoutLiveEnvironment(t *testing.T) {
	t.Setenv(enforcedRouteLiveEnv, "")
	dataset, err := newRoutingDataset()
	if err != nil {
		t.Fatal("could not create offline enforced-route data")
	}
	records := make([]enforcedRouteEvidenceRecord, 0, 3)
	for _, mode := range []access.RouteMode{access.RouteAutoFirstAction, access.RouteDirectOnly, access.RoutePTCOnly} {
		records = append(records, runOfflineEnforcedRouteArm(t, mode, dataset))
	}
	for _, record := range records {
		if !enforcedRouteRecordInvariant(record) || record.DatasetSHA256 != dataset.hash() || record.PromptSHA256 != liveSHA256(routingUserPrompt()) || len(record.Wire) != 0 {
			t.Fatal("offline enforced-route record did not retain required invariant evidence")
		}
		payload, err := json.Marshal(record)
		if err != nil {
			t.Fatal("could not encode offline enforced-route evidence")
		}
		for _, row := range dataset.rows {
			if strings.Contains(string(payload), row.Name) {
				t.Fatal("offline enforced-route evidence retained an opaque fixture value")
			}
		}
		if strings.Contains(string(payload), routingUserPrompt()) || strings.Contains(string(payload), selectionProgram) {
			t.Fatal("offline enforced-route evidence retained prompt or program source")
		}
	}
	if records[0].FrozenCapabilityMenuSHA256 != records[1].FrozenCapabilityMenuSHA256 || records[1].FrozenCapabilityMenuSHA256 != records[2].FrozenCapabilityMenuSHA256 {
		t.Fatal("enforced-route arms did not retain the same frozen capability universe")
	}
}

func TestEnforcedRouteEvidenceClassifierAndInvariants(t *testing.T) {
	dataset, err := newRoutingDataset()
	if err != nil {
		t.Fatal("could not create classifier fixture data")
	}
	record := runOfflineEnforcedRouteArm(t, access.RoutePTCOnly, dataset)
	if !enforcedRouteRecordInvariant(record) || record.ObservedRoute != "catalog_execute" {
		t.Fatal("PTC evidence was not classified from actual route structure")
	}
	invalid := record
	invalid.ObservedRoute = "direct"
	if enforcedRouteRecordInvariant(invalid) {
		t.Fatal("route invariant accepted a classification inconsistent with frozen PTC mode")
	}
	invalid = record
	invalid.FrozenRoute.Mode = string(access.RouteDirectOnly)
	if enforcedRouteRecordInvariant(invalid) {
		t.Fatal("route invariant accepted frozen metadata drift")
	}
}

func TestEnforcedRouteWireStructuralValidityAcceptsObservedResponsesShapes(t *testing.T) {
	request := func(toolCount int) liveWireRequestEvidence {
		return liveWireRequestEvidence{
			BodyObserved: true, JSONValid: true, StreamPresent: true, Stream: true,
			StorePresent: true, ToolCount: toolCount,
		}
	}
	parallelRequest := func(toolCount int) liveWireRequestEvidence {
		value := request(toolCount)
		value.ParallelToolCallsPresent, value.ParallelToolCalls = true, true
		return value
	}
	rounds := func(toolCounts ...int) []liveEvidenceRound {
		result := make([]liveEvidenceRound, 0, len(toolCounts))
		for _, count := range toolCounts {
			result = append(result, liveEvidenceRound{ToolSchemaNames: make([]string, count)})
		}
		return result
	}
	for name, record := range map[string]enforcedRouteEvidenceRecord{
		"direct only omits instructions": {
			Rounds: rounds(2, 2),
			Wire: []liveWireRequestEvidence{
				parallelRequest(2), parallelRequest(2),
			},
		},
		"auto first action omits instructions": {
			Rounds: rounds(4, 4),
			Wire: []liveWireRequestEvidence{
				parallelRequest(4), parallelRequest(4),
			},
		},
		"PTC final round omits parallel tool calls": {
			Rounds: rounds(2, 2, 0),
			Wire: []liveWireRequestEvidence{
				parallelRequest(2), parallelRequest(2),
				request(0),
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if !enforcedRouteWireStructuralValid(record) {
				t.Fatalf("safe wire evidence should be structurally valid: %#v", record)
			}
		})
	}

	invalid := enforcedRouteEvidenceRecord{
		Rounds: rounds(1),
		Wire:   []liveWireRequestEvidence{request(1)},
	}
	if enforcedRouteWireStructuralValid(invalid) {
		t.Fatal("tool-bearing round without parallel_tool_calls=true was accepted")
	}
	invalid.Wire[0].ParallelToolCallsPresent, invalid.Wire[0].ParallelToolCalls = true, true
	invalid.Wire[0].StorePresent = false
	if enforcedRouteWireStructuralValid(invalid) {
		t.Fatal("round without store=false was accepted")
	}
}

func TestMemoryJournalGetToolInvocationIsExactAndDefensive(t *testing.T) {
	journal := newMemoryJournal()
	call := core.ToolCall{ID: "reader-call", Name: listToolID, Args: map[string]any{"scope": "current"}}
	invocation, err := core.NewToolInvocation(core.RunInfo{RunID: "reader-run", SessionID: "reader-session", Principal: core.Principal{TenantID: "reader-tenant", SubjectID: "reader-subject"}}, call, true)
	if err != nil {
		t.Fatal("could not construct journal reader invocation")
	}
	if _, _, err := journal.BeginToolInvocation(context.Background(), invocation); err != nil {
		t.Fatal("could not begin journal reader invocation")
	}
	if _, err := journal.CompleteToolInvocation(context.Background(), invocation, core.CapabilityResult{Content: "private-result", OK: true, Metadata: map[string]any{"nested": map[string]any{"value": "original"}}}); err != nil {
		t.Fatal("could not complete journal reader invocation")
	}
	record, found, err := journal.GetToolInvocation(context.Background(), invocation)
	if err != nil || !found || record.Result == nil || record.Result.Content != "private-result" {
		t.Fatal("journal reader did not return its exact completed record")
	}
	record.Result.Content = "mutated"
	record.Result.Metadata["nested"].(map[string]any)["value"] = "mutated"
	again, found, err := journal.GetToolInvocation(context.Background(), invocation)
	if err != nil || !found || again.Result == nil || again.Result.Content != "private-result" || again.Result.Metadata["nested"].(map[string]any)["value"] != "original" {
		t.Fatal("journal reader did not return a defensive result clone")
	}
	wrong := invocation
	wrong.ArgsDigest = strings.Repeat("0", 64)
	if _, found, err := journal.GetToolInvocation(context.Background(), wrong); err != nil || found {
		t.Fatal("journal reader accepted a same-key but different immutable identity")
	}
	if journal.completed() != 1 {
		t.Fatal("journal reader changed durable state")
	}
}
