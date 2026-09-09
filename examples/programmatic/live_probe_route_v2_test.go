package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/whhhh1500/auto-agent/internal/executionroute"
	programtools "github.com/whhhh1500/auto-agent/pkg/adapter/programmatic/toolcapability"
	appcontextassembly "github.com/whhhh1500/auto-agent/pkg/app/contextassembly"
	"github.com/whhhh1500/auto-agent/pkg/app/programmatic"
	"github.com/whhhh1500/auto-agent/pkg/app/runexecutor"
	core "github.com/whhhh1500/auto-agent/pkg/core"
)

const (
	liveProbeRouteV2Flag         = "HARNESS_ACCEPTANCE_LIVE_PROBE_ROUTE_V2"
	liveProbeRouteV2EvidencePath = "HARNESS_ACCEPTANCE_LIVE_PROBE_ROUTE_V2_EVIDENCE_PATH"
	liveProbeRouteV2Rows         = 8
	liveProbeRouteV2Padding      = 4 << 10
)

// TestLiveProbeRouteV2Acceptance is a deliberately separate opt-in acceptance
// test. It sends real model requests only after its own flag is set; all tool
// effects remain in-memory, read-only synthetic fixture effects.
//
// It exercises the production Responses adapter, executionroute.Resolve, and
// the sequential Runtime. auto_probe_once has exactly three requests: neutral
// probe, Direct/PTC choice, and a tool-free final response. direct_only gets
// a bounded ten-round ceiling so it can split the eight independent details.
func TestLiveProbeRouteV2Acceptance(t *testing.T) {
	if os.Getenv(liveProbeRouteV2Flag) != "1" {
		t.Skip("set HARNESS_ACCEPTANCE_LIVE_PROBE_ROUTE_V2=1 to make intentional v2 live model requests")
	}
	if liveProtocol() != "responses" {
		t.Skip("set HARNESS_PROGRAMMATIC_PROTOCOL=responses for v2 wire acceptance")
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
	previous := http.DefaultTransport
	if previous == nil {
		liveWireTransportMu.Unlock()
		t.Fatal("default HTTP transport is unavailable")
	}
	transport := &liveWireAuditTransport{next: previous}
	http.DefaultTransport = transport
	t.Cleanup(func() {
		http.DefaultTransport = previous
		liveWireTransportMu.Unlock()
	})
	pacer := &liveRequestPacer{interval: interval}

	for _, arm := range []v2LiveArm{
		{name: "auto_probe_once", routeMode: programmatic.RouteAutoProbeOnce},
		{name: "direct_only", routeMode: programmatic.RouteDirectOnly},
	} {
		arm := arm
		stopAfterTransportOrProtocolFailure := false
		passed := t.Run(arm.name, func(t *testing.T) {
			startRequests := transport.count()
			var result core.TurnResult
			var events []core.SessionEvent
			selection := executionroute.SelectedRouteObservation{Route: executionroute.SelectedRouteUnavailable, Status: executionroute.SelectionStatusUnavailable}
			var acceptancePassed bool
			var inner *liveModel
			var fixture *v2LiveFixture
			t.Cleanup(func() {
				record := v2LiveEvidenceFromRun(arm, modelID, result, inner, events, transport.snapshotSince(startRequests), selection, acceptancePassed, fixture)
				if err := writeV2LiveEvidence(os.Getenv(liveProbeRouteV2EvidencePath), record); err != nil {
					t.Error("could not write v2 live acceptance evidence")
				}
			})
			adapter, err := newLiveAdapter(context.Background())
			if err != nil {
				t.Fatal("live model configuration is incomplete or invalid")
			}
			inner = newLiveModel(t, adapter, "probe-route-v2-"+arm.name, v2LiveMaxModelRounds(arm), pacer)
			fixture, err = newV2LiveFixture(inner, arm)
			if err != nil {
				t.Fatal("could not construct v2 live fixture")
			}
			runID := "live-probe-route-v2-" + arm.name

			decision, err := executionroute.Resolve(context.Background(), executionroute.Request{
				Runtime: fixture.runtime, Registry: fixture.executors, Principal: fixture.principal, Session: fixture.session, RunID: runID,
			})
			if err != nil || decision.Metadata.ID != runexecutor.SequentialID {
				t.Fatal("v2 live case did not resolve the sequential runtime")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			result, err = decision.Executor.RunTurn(ctx, fixture.principal, fixture.session, core.TurnInput{
				RunID: runID, Text: liveProbeRouteV2Prompt(), CompositionMetadata: decision.CompositionMetadata,
			}, nil)
			events = fixture.session.Events()
			if err != nil || result.Status != core.RunCompleted {
				stopAfterTransportOrProtocolFailure = inner.stopSubsequentCases()
				t.Fatal("v2 live case did not complete")
			}
			// Reconstruct the durable route before any answer or quality assertion,
			// so a bad final answer still retains the verified route observation.
			selection = executionroute.ObserveSelectedRoute(context.Background(), fixture.session, fixture.principal, fixture.journal, runID)
			assertV2LiveRun(t, arm, fixture, result, inner, events, transport.snapshotSince(startRequests), selection)
			acceptancePassed = true
		})
		if !passed && stopAfterTransportOrProtocolFailure {
			t.Log("live model HTTP, transport, or Responses protocol failure: remaining v2 route arms skipped")
			break
		}
	}
}

type v2LiveArm struct {
	name      string
	routeMode programmatic.RouteMode
}

func v2LiveMaxModelRounds(arm v2LiveArm) int {
	if arm.routeMode == programmatic.RouteAutoProbeOnce {
		return 3
	}
	return liveProbeRouteV2Rows + 2
}

func v2LiveMaxToolCalls() int { return liveProbeRouteV2Rows + 2 }

// The prompt is intentionally shared byte-for-byte across both route arms.
// The provider's Content is readable context only; authority comes solely from
// its separately validated probe_facts metadata.
func liveProbeRouteV2Prompt() string {
	return "Use only the exposed synthetic read-only fixture tools. First call fixture.inventory once. After that probe, choose exactly one currently exposed follow-up form: call fixture.detail once for every listed active item, or call program.execute once with a bounded program that obtains every listed active item's name. Do not call any other tool. After tool execution, reply with exactly one line and no Markdown: FINAL: Item 01, Item 02, Item 03, Item 04, Item 05, Item 06, Item 07, Item 08."
}

type v2LiveFixture struct {
	runtime   *core.Runtime
	principal core.Principal
	session   *core.Session
	executors *runexecutor.Registry
	journal   *memoryJournal
	probe     *v2LiveProbe
	detail    *v2LiveDetail
	context   *v2LiveContextRecorder
}

// v2LiveContextRecorder holds aggregate assembly facts only. It never keeps
// model messages, tool contents, call identities, or request hashes.
type v2LiveContextRecorder struct {
	mu       sync.Mutex
	calls    int
	failures int
	last     v2LiveContextEvidence
}

func (r *v2LiveContextRecorder) observe(request core.ModelContext, assembled core.ModelContext, err error) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if err != nil {
		r.failures++
	}
	r.last = v2LiveContextEvidence{
		FinalInputBytes:     assembled.InputBytes,
		FinalInputTokens:    assembled.InputTokens,
		ContextWindowTokens: request.ContextWindowTokens,
		MaxOutputTokens:     request.MaxOutputTokens,
		DroppedGroups:       assembled.DroppedGroups,
	}
}

func (r *v2LiveContextRecorder) evidence() v2LiveContextEvidence {
	if r == nil {
		return v2LiveContextEvidence{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	evidence := r.last
	evidence.AssemblyCalls, evidence.Failures = r.calls, r.failures
	return evidence
}

func newV2LiveFixture(model core.LlmAdapter, arm v2LiveArm) (*v2LiveFixture, error) {
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product, err := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "probe-route-v2-live"})
	if err != nil {
		return nil, err
	}
	user, err := product.Child(core.ScopeRef{Kind: core.ScopeUser, ID: "fixture-user"})
	if err != nil {
		return nil, err
	}
	sessionScope, err := user.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "fixture-" + arm.name})
	if err != nil {
		return nil, err
	}
	principal := core.Principal{TenantID: "fixture-tenant", SubjectID: "fixture-subject", Scope: user, Grants: core.NewPermissionSet(core.PermRead)}
	session, err := core.NewSession(core.SessionOptions{ID: "fixture-" + arm.name, ProfileID: "probe-route-v2-live", Principal: principal, Scope: sessionScope})
	if err != nil {
		return nil, err
	}

	journal := newMemoryJournal()
	probe := &v2LiveProbe{}
	detail := &v2LiveDetail{activeRows: liveProbeRouteV2Rows, paddingBytes: liveProbeRouteV2Padding}
	execute, err := programtools.NewExecuteCapability(programtools.ExecuteID)
	if err != nil {
		return nil, err
	}
	registry := core.NewCapabilityRegistry()
	for _, capability := range []core.Capability{probe, detail, execute} {
		if err := registry.Register(product, capability); err != nil {
			return nil, err
		}
	}
	steps, calls := v2LiveMaxModelRounds(arm), v2LiveMaxToolCalls()
	selection := core.ModelSelection{Provider: model.Provider(), Model: strings.TrimSpace(os.Getenv("HARNESS_LLM_MODEL"))}
	metadata := map[string]string{
		executionroute.RouteVersionKey:        executionroute.RouteVersion,
		executionroute.RouteModeKey:           string(programmatic.RouteDirectOnly),
		executionroute.RouteCatalogToolIDKey:  programtools.CatalogID,
		executionroute.RouteExecuteToolIDKey:  programtools.ExecuteID,
		executionroute.RouteImplementationKey: executionroute.RouteImplementationRevision,
	}
	if arm.routeMode == programmatic.RouteAutoProbeOnce {
		metadata[executionroute.RouteVersionKey] = executionroute.RouteProbeVersion
		metadata[executionroute.RouteModeKey] = string(programmatic.RouteAutoProbeOnce)
		metadata[executionroute.RouteImplementationKey] = executionroute.RouteProbeImplementationRevision
	}
	profiles := core.NewAgentProfileRegistry()
	if err := profiles.Bind(core.AgentProfileLayer{
		Scope: product, ProfileID: session.ProfileID(), Model: &selection, MaxSteps: &steps, MaxToolCalls: &calls,
		AddCapabilities: []string{"fixture.inventory", "fixture.detail", programtools.ExecuteID}, Metadata: metadata,
	}); err != nil {
		return nil, err
	}
	contextAssembler, err := appcontextassembly.NewAssembler(appcontextassembly.Config{})
	if err != nil {
		return nil, err
	}
	contextRecorder := &v2LiveContextRecorder{}
	runtime := &core.Runtime{Capabilities: registry, Profiles: profiles, ToolJournal: journal,
		Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) { return model, nil }),
		ContextAssembler: func(ctx context.Context, request core.ModelContext) (core.ModelContext, error) {
			assembled, assembleErr := contextAssembler.AssembleModelContext(ctx, request)
			contextRecorder.observe(request, assembled, assembleErr)
			return assembled, assembleErr
		},
	}
	executors, err := runexecutor.NewDefaultRegistry()
	if err != nil {
		return nil, err
	}
	return &v2LiveFixture{runtime: runtime, principal: principal, session: session, executors: executors, journal: journal, probe: probe, detail: detail, context: contextRecorder}, nil
}

// TestV2ProjectedPTCResponsesFinalRequestPairsOnlyModelVisibleCalls exercises
// the production Responses compiler through a local HTTP server. The
// program.execute route emits nested durable child calls, so this catches a
// malformed final request before a provider can reject unpaired child output.
func TestV2ProjectedPTCResponsesFinalRequestPairsOnlyModelVisibleCalls(t *testing.T) {
	capture := &v2OfflineResponsesCapture{}
	server := httptest.NewServer(http.HandlerFunc(capture.serveHTTP))
	t.Cleanup(server.Close)
	t.Setenv("HARNESS_PROGRAMMATIC_PROTOCOL", "responses")
	t.Setenv("HARNESS_LLM_BASE_URL", server.URL)
	t.Setenv("HARNESS_LLM_API_KEY", "offline-test-key")
	t.Setenv("HARNESS_LLM_MODEL", "offline-responses-model")
	t.Setenv("HARNESS_LLM_MAX_TOKENS", "4096")

	adapter, err := newLiveResponsesAdapter(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	fixture, err := newV2LiveFixture(adapter, v2LiveArm{name: "offline_ptc", routeMode: programmatic.RouteAutoProbeOnce})
	if err != nil {
		t.Fatal(err)
	}
	const runID = "offline-projected-ptc"
	decision, err := executionroute.Resolve(context.Background(), executionroute.Request{
		Runtime: fixture.runtime, Registry: fixture.executors, Principal: fixture.principal, Session: fixture.session, RunID: runID,
	})
	if err != nil || decision.Metadata.ID != runexecutor.SequentialID {
		t.Fatalf("route resolution failed: decision=%+v err=%v", decision.Metadata, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	result, err := decision.Executor.RunTurn(ctx, fixture.principal, fixture.session, core.TurnInput{
		RunID: runID, Text: liveProbeRouteV2Prompt(), CompositionMetadata: decision.CompositionMetadata,
	}, nil)
	if err != nil || result.Status != core.RunCompleted {
		t.Fatalf("offline projected PTC run failed: status=%s err=%v", result.Status, err)
	}
	if result.Answer != "FINAL: Item 01, Item 02, Item 03, Item 04, Item 05, Item 06, Item 07, Item 08" {
		t.Fatalf("final answer=%q", result.Answer)
	}
	if err := capture.err(); err != nil {
		t.Fatal(err)
	}
	requests := capture.snapshot()
	if len(requests) != 3 {
		t.Fatalf("Responses request count=%d, want 3", len(requests))
	}
	if contextEvidence := fixture.context.evidence(); contextEvidence.AssemblyCalls != 3 || contextEvidence.Failures != 0 || contextEvidence.DroppedGroups != 0 {
		t.Fatalf("auto_probe_once context evidence=%+v", contextEvidence)
	}
	assertV2FinalResponsesRequest(t, requests[2])

	events := fixture.session.Events()
	childCalls, childResults := v2NestedChildEvidence(events, "execute-call")
	if childCalls != liveProbeRouteV2Rows || childResults != liveProbeRouteV2Rows {
		t.Fatalf("durable nested evidence calls=%d results=%d, want %d", childCalls, childResults, liveProbeRouteV2Rows)
	}
	if fixture.journal.completed() != liveProbeRouteV2Rows+2 {
		t.Fatalf("durable journal completed=%d, want %d", fixture.journal.completed(), liveProbeRouteV2Rows+2)
	}
	observation := executionroute.ObserveSelectedRoute(context.Background(), fixture.session, fixture.principal, fixture.journal, runID)
	if observation.Status != executionroute.SelectionStatusVerified || observation.Route != executionroute.SelectedRoutePTC {
		t.Fatalf("durable route observation=%+v", observation)
	}
	if coverage := v2LiveCoverageFromRun(fixture, runID, events); coverage != (v2LiveCoverageEvidence{CandidateCount: liveProbeRouteV2Rows, SelectedUniqueCount: liveProbeRouteV2Rows, ExecutedCount: liveProbeRouteV2Rows, ExactOnce: true, CoverageStatus: v2LiveCoverageComplete}) {
		t.Fatalf("durable PTC coverage=%+v", coverage)
	}
}

// The fixture's model context boundary must stop rather than send a nested
// child output without its completed model-visible parent call/output pair.
func TestV2ProjectedPTCFixtureRejectsOrphanNestedChildContext(t *testing.T) {
	fixture, err := newV2LiveFixture(core.MockLlmAdapter{}, v2LiveArm{name: "orphan_context", routeMode: programmatic.RouteAutoProbeOnce})
	if err != nil {
		t.Fatal(err)
	}
	if fixture.runtime.ContextAssembler == nil {
		t.Fatal("v2 fixture is missing the model-context assembler")
	}
	_, err = fixture.runtime.ContextAssembler(context.Background(), core.ModelContext{
		ContextWindowTokens: 32768,
		MaxOutputTokens:     4096,
		Messages: []core.ChatMessage{
			{Role: core.RoleUser, Content: "process the batch", SourceSeq: 1},
			{Role: core.RoleAssistant, ToolCalls: []core.ToolCall{{ID: "execute-call", Name: programtools.ExecuteID}}, SourceSeq: 2},
			{Role: core.RoleTool, ToolCallID: "execute-call/child", Content: "unpaired nested evidence", SourceSeq: 3},
		},
	})
	if !errors.Is(err, appcontextassembly.ErrInvalid) {
		t.Fatalf("orphan nested child was not rejected: %v", err)
	}
}

// TestV2LiveModelDirectFourKiBReachesThirdRound proves the v2 live path
// retains the wrapped adapter's 128k/4k limits. Eight direct 4 KiB results
// exceed Core's conservative input budget, so the third model call is the
// observable regression boundary.
func TestV2LiveModelDirectFourKiBReachesThirdRound(t *testing.T) {
	t.Setenv("HARNESS_LLM_MODEL", "offline-v2-direct")
	inner := &v2OfflineDirectFourKiBModel{}
	arm := v2LiveArm{name: "offline_direct", routeMode: programmatic.RouteDirectOnly}
	live := newLiveModel(t, inner, "offline-v2-direct-four-kib", v2LiveMaxModelRounds(arm), nil)
	fixture, err := newV2LiveFixture(live, arm)
	if err != nil {
		t.Fatal(err)
	}
	const runID = "offline-v2-direct-four-kib"
	decision, err := executionroute.Resolve(context.Background(), executionroute.Request{
		Runtime: fixture.runtime, Registry: fixture.executors, Principal: fixture.principal, Session: fixture.session, RunID: runID,
	})
	if err != nil || decision.Metadata.ID != runexecutor.SequentialID {
		t.Fatalf("route resolution failed: decision=%+v err=%v", decision.Metadata, err)
	}
	result, err := decision.Executor.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{
		RunID: runID, Text: liveProbeRouteV2Prompt(), CompositionMetadata: decision.CompositionMetadata,
	}, nil)
	if err != nil || result.Status != core.RunCompleted {
		t.Fatalf("v2 direct 4KiB run failed: status=%s err=%v", result.Status, err)
	}
	if live.rounds() != 3 {
		t.Fatalf("v2 direct 4KiB model rounds=%d, want 3", live.rounds())
	}
	if contextEvidence := fixture.context.evidence(); contextEvidence.AssemblyCalls != 3 || contextEvidence.Failures != 0 || contextEvidence.DroppedGroups != 0 {
		t.Fatalf("v2 direct 4KiB context evidence=%+v", contextEvidence)
	}
	toolResults, resultBytes := inner.thirdRoundEvidence()
	if toolResults != liveProbeRouteV2Rows+1 || resultBytes < liveProbeRouteV2Rows*liveProbeRouteV2Padding {
		t.Fatalf("third v2 direct request results=%d bytes=%d, want 9 and at least %d", toolResults, resultBytes, liveProbeRouteV2Rows*liveProbeRouteV2Padding)
	}
	selection := executionroute.ObserveSelectedRoute(context.Background(), fixture.session, fixture.principal, fixture.journal, runID)
	if selection != (executionroute.SelectedRouteObservation{Route: executionroute.SelectedRouteUnavailable, Status: executionroute.SelectionStatusUnavailable}) {
		t.Fatalf("direct-only route observation=%+v", selection)
	}
	record := v2LiveEvidenceFromRun(arm, "offline-v2-direct-four-kib", result, live, fixture.session.Events(), nil, selection, true, fixture)
	if record.Coverage != (v2LiveCoverageEvidence{CandidateCount: liveProbeRouteV2Rows, SelectedUniqueCount: liveProbeRouteV2Rows, ExecutedCount: liveProbeRouteV2Rows, ExactOnce: true, CoverageStatus: v2LiveCoverageComplete}) {
		t.Fatalf("direct-only coverage=%+v", record.Coverage)
	}
}

type v2OfflineDirectFourKiBModel struct {
	mu                       sync.Mutex
	round                    int
	thirdRoundToolResults    int
	thirdRoundToolResultByte int
}

func (*v2OfflineDirectFourKiBModel) Provider() string { return "offline-v2-direct" }

func (*v2OfflineDirectFourKiBModel) ModelContextLimits() (int, int) { return 128_000, 4_096 }

func (m *v2OfflineDirectFourKiBModel) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	m.mu.Lock()
	round := m.round
	m.round++
	m.mu.Unlock()
	switch round {
	case 0:
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &core.ToolCall{ID: "direct-probe", Name: "fixture.inventory", Args: map[string]any{}}})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
		return nil
	case 1:
		calls := make([]core.ToolCall, 0, liveProbeRouteV2Rows)
		for index := 1; index <= liveProbeRouteV2Rows; index++ {
			calls = append(calls, core.ToolCall{ID: fmt.Sprintf("direct-detail-%02d", index), Name: "fixture.detail", Args: map[string]any{"id": fmt.Sprintf("item-%02d", index)}})
		}
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCalls: calls})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
		return nil
	case 2:
		toolResults, resultBytes := 0, 0
		for _, message := range options.Messages {
			if message.Role == core.RoleTool {
				toolResults++
				resultBytes += len(message.Content)
			}
		}
		m.mu.Lock()
		m.thirdRoundToolResults, m.thirdRoundToolResultByte = toolResults, resultBytes
		m.mu.Unlock()
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: "FINAL: Item 01, Item 02, Item 03, Item 04, Item 05, Item 06, Item 07, Item 08"})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
		return nil
	default:
		return errors.New("offline v2 direct model received an extra round")
	}
}

func (m *v2OfflineDirectFourKiBModel) thirdRoundEvidence() (toolResults, resultBytes int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.thirdRoundToolResults, m.thirdRoundToolResultByte
}

// TestV2OfflineWave51IncompleteDirectKeepsRouteVerifiedButCoverageFails
// demonstrates that route selection and fixture task coverage are separate
// acceptance facts. The model makes one valid planned Direct call, then gives
// an intentionally incomplete final answer; this is completely offline.
func TestV2OfflineWave51IncompleteDirectKeepsRouteVerifiedButCoverageFails(t *testing.T) {
	t.Setenv("HARNESS_LLM_MODEL", "offline-v2-wave51-incomplete-direct")
	arm := v2LiveArm{name: "offline_wave51_incomplete_direct", routeMode: programmatic.RouteAutoProbeOnce}
	fixture, err := newV2LiveFixture(&v2OfflineWave51IncompleteDirectModel{}, arm)
	if err != nil {
		t.Fatal(err)
	}
	const runID = "offline-v2-wave51-incomplete-direct"
	decision, err := executionroute.Resolve(context.Background(), executionroute.Request{
		Runtime: fixture.runtime, Registry: fixture.executors, Principal: fixture.principal, Session: fixture.session, RunID: runID,
	})
	if err != nil || decision.Metadata.ID != runexecutor.SequentialID {
		t.Fatalf("route resolution failed: decision=%+v err=%v", decision.Metadata, err)
	}
	result, err := decision.Executor.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{
		RunID: runID, Text: liveProbeRouteV2Prompt(), CompositionMetadata: decision.CompositionMetadata,
	}, nil)
	if err != nil || result.Status != core.RunCompleted || result.Answer != "FINAL: Item 01" {
		t.Fatalf("offline Wave51 result=%+v err=%v", result, err)
	}
	events := fixture.session.Events()
	selection := executionroute.ObserveSelectedRoute(context.Background(), fixture.session, fixture.principal, fixture.journal, runID)
	if selection != (executionroute.SelectedRouteObservation{Route: executionroute.SelectedRouteDirect, Status: executionroute.SelectionStatusVerified}) {
		t.Fatalf("offline Wave51 route observation=%+v", selection)
	}
	record := v2LiveEvidenceFromRun(arm, "offline-v2-wave51-incomplete-direct", result, nil, events, nil, selection, false, fixture)
	if record.Coverage != (v2LiveCoverageEvidence{CandidateCount: liveProbeRouteV2Rows, SelectedUniqueCount: 1, ExecutedCount: 1, ExactOnce: true, CoverageStatus: v2LiveCoverageIncomplete}) {
		t.Fatalf("offline Wave51 coverage=%+v", record.Coverage)
	}
	if record.AcceptancePassed || record.FailureCategory != v2LiveFailureAcceptance || record.Effects.DetailCalls != 1 || record.Effects.CompletedJournalInvocations != 2 {
		t.Fatalf("offline Wave51 acceptance evidence=%+v", record)
	}
}

type v2OfflineWave51IncompleteDirectModel struct {
	mu    sync.Mutex
	round int
}

func (*v2OfflineWave51IncompleteDirectModel) Provider() string { return "offline-v2-wave51" }

func (*v2OfflineWave51IncompleteDirectModel) ArtifactRevision() string {
	return "offline-v2-wave51-incomplete-direct/v1"
}

func (*v2OfflineWave51IncompleteDirectModel) ModelContextLimits() (int, int) { return 128_000, 4_096 }

func (m *v2OfflineWave51IncompleteDirectModel) Stream(_ context.Context, _ core.GenerateOptions, emit func(core.StreamChunk)) error {
	m.mu.Lock()
	round := m.round
	m.round++
	m.mu.Unlock()
	switch round {
	case 0:
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &core.ToolCall{ID: "wave51-probe", Name: "fixture.inventory", Args: map[string]any{}}})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
		return nil
	case 1:
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &core.ToolCall{ID: "wave51-detail-01", Name: "fixture.detail", Args: map[string]any{"id": "item-01"}}})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
		return nil
	case 2:
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: "FINAL: Item 01"})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
		return nil
	default:
		return errors.New("offline Wave51 model received an extra round")
	}
}

func TestV2LiveModelDirectBatchesCanUseFourRounds(t *testing.T) {
	t.Setenv("HARNESS_LLM_MODEL", "offline-v2-direct-batches")
	arm := v2LiveArm{name: "offline_direct_batches", routeMode: programmatic.RouteDirectOnly}
	inner := &v2OfflineSplitDirectModel{}
	live := newLiveModel(t, inner, "offline-v2-direct-batches", v2LiveMaxModelRounds(arm), nil)
	fixture, err := newV2LiveFixture(live, arm)
	if err != nil {
		t.Fatal(err)
	}
	const runID = "offline-v2-direct-batches"
	decision, err := executionroute.Resolve(context.Background(), executionroute.Request{
		Runtime: fixture.runtime, Registry: fixture.executors, Principal: fixture.principal, Session: fixture.session, RunID: runID,
	})
	if err != nil || decision.Metadata.ID != runexecutor.SequentialID {
		t.Fatalf("route resolution failed: decision=%+v err=%v", decision.Metadata, err)
	}
	result, err := decision.Executor.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{
		RunID: runID, Text: liveProbeRouteV2Prompt(), CompositionMetadata: decision.CompositionMetadata,
	}, nil)
	if err != nil || result.Status != core.RunCompleted || result.Answer != "FINAL: Item 01, Item 02, Item 03, Item 04, Item 05, Item 06, Item 07, Item 08" {
		t.Fatalf("split direct run=%+v err=%v", result, err)
	}
	if live.rounds() != 4 || !v2LiveRoundsValid(arm, live.rounds()) {
		t.Fatalf("split direct model rounds=%d", live.rounds())
	}
	if contextEvidence := fixture.context.evidence(); contextEvidence.AssemblyCalls != 4 || contextEvidence.Failures != 0 || contextEvidence.DroppedGroups != 0 {
		t.Fatalf("split direct context evidence=%+v", contextEvidence)
	}
	if fixture.probe.calls() != 1 || fixture.journal.completed() != liveProbeRouteV2Rows+1 {
		t.Fatalf("split direct durable effects probe=%d journal=%d", fixture.probe.calls(), fixture.journal.completed())
	}
	gotIDs := fixture.detail.idsCopy()
	sort.Strings(gotIDs)
	wantIDs := make([]string, liveProbeRouteV2Rows)
	for index := range wantIDs {
		wantIDs[index] = fmt.Sprintf("item-%02d", index+1)
	}
	if !sameStrings(gotIDs, wantIDs) {
		t.Fatalf("split direct IDs=%v want=%v", gotIDs, wantIDs)
	}
	stages := v2LiveAssistantToolStages(t, fixture.session.Events())
	if len(stages) != 3 || len(stages[0]) != 1 || stages[0][0] != "fixture.inventory" || len(stages[1]) != 4 || len(stages[2]) != 4 {
		t.Fatalf("split direct durable tool stages=%v", stages)
	}
	for _, stage := range stages[1:] {
		for _, name := range stage {
			if name != "fixture.detail" {
				t.Fatalf("split direct emitted %q after probe", name)
			}
		}
	}
	if finalTools := inner.finalToolCount(); finalTools != 2 {
		t.Fatalf("split direct final menu size=%d, want direct menu size 2", finalTools)
	}
}

// TestV2LiveModelDirectRoundsFailClosedAtTen shows the direct control cannot
// make an eleventh model request. The tenth request intentionally consumes the
// final permitted tool call, so Runtime stops at its ten-step ceiling before
// the inner model can receive another provider request.
func TestV2LiveModelDirectRoundsFailClosedAtTen(t *testing.T) {
	t.Setenv("HARNESS_LLM_MODEL", "offline-v2-direct-overflow")
	arm := v2LiveArm{name: "offline_direct_overflow", routeMode: programmatic.RouteDirectOnly}
	inner := &v2OfflineOverRoundDirectModel{}
	live := newLiveModel(t, inner, "offline-v2-direct-overflow", v2LiveMaxModelRounds(arm), nil)
	fixture, err := newV2LiveFixture(live, arm)
	if err != nil {
		t.Fatal(err)
	}
	const runID = "offline-v2-direct-overflow"
	decision, err := executionroute.Resolve(context.Background(), executionroute.Request{
		Runtime: fixture.runtime, Registry: fixture.executors, Principal: fixture.principal, Session: fixture.session, RunID: runID,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := decision.Executor.RunTurn(context.Background(), fixture.principal, fixture.session, core.TurnInput{
		RunID: runID, Text: liveProbeRouteV2Prompt(), CompositionMetadata: decision.CompositionMetadata,
	}, nil)
	if err != nil || result.Status != core.RunLimited {
		t.Fatalf("over-round direct result=%+v err=%v", result, err)
	}
	if live.rounds() != v2LiveMaxModelRounds(arm) || inner.calls() != v2LiveMaxModelRounds(arm) {
		t.Fatalf("over-round direct model calls live=%d inner=%d want=%d", live.rounds(), inner.calls(), v2LiveMaxModelRounds(arm))
	}
	if contextEvidence := fixture.context.evidence(); contextEvidence.AssemblyCalls != v2LiveMaxModelRounds(arm) || contextEvidence.Failures != 0 {
		t.Fatalf("over-round direct context evidence=%+v", contextEvidence)
	}
}

type v2OfflineSplitDirectModel struct {
	mu         sync.Mutex
	round      int
	finalTools int
}

func (*v2OfflineSplitDirectModel) Provider() string { return "offline-v2-direct" }

func (*v2OfflineSplitDirectModel) ModelContextLimits() (int, int) { return 128_000, 4_096 }

func (m *v2OfflineSplitDirectModel) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	m.mu.Lock()
	round := m.round
	m.round++
	m.mu.Unlock()
	switch round {
	case 0:
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &core.ToolCall{ID: "split-probe", Name: "fixture.inventory", Args: map[string]any{}}})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
	case 1, 2:
		start := 1 + (round-1)*4
		calls := make([]core.ToolCall, 0, 4)
		for index := start; index < start+4; index++ {
			calls = append(calls, core.ToolCall{ID: fmt.Sprintf("split-detail-%02d", index), Name: "fixture.detail", Args: map[string]any{"id": fmt.Sprintf("item-%02d", index)}})
		}
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCalls: calls})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
	case 3:
		m.mu.Lock()
		m.finalTools = len(options.Tools)
		m.mu.Unlock()
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: "FINAL: Item 01, Item 02, Item 03, Item 04, Item 05, Item 06, Item 07, Item 08"})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
	default:
		return errors.New("offline v2 split direct model received an extra round")
	}
	return nil
}

func (m *v2OfflineSplitDirectModel) finalToolCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.finalTools
}

type v2OfflineOverRoundDirectModel struct {
	mu    sync.Mutex
	round int
}

func (*v2OfflineOverRoundDirectModel) Provider() string { return "offline-v2-direct" }

func (*v2OfflineOverRoundDirectModel) ModelContextLimits() (int, int) { return 128_000, 4_096 }

func (m *v2OfflineOverRoundDirectModel) Stream(_ context.Context, _ core.GenerateOptions, emit func(core.StreamChunk)) error {
	m.mu.Lock()
	round := m.round
	m.round++
	m.mu.Unlock()
	switch {
	case round == 0:
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &core.ToolCall{ID: "overflow-probe", Name: "fixture.inventory", Args: map[string]any{}}})
	case round >= 1 && round <= liveProbeRouteV2Rows:
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &core.ToolCall{ID: fmt.Sprintf("overflow-detail-%02d", round), Name: "fixture.detail", Args: map[string]any{"id": fmt.Sprintf("item-%02d", round)}}})
	case round == liveProbeRouteV2Rows+1:
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &core.ToolCall{ID: "overflow-extra", Name: "fixture.inventory", Args: map[string]any{}}})
	default:
		return errors.New("offline v2 over-round model reached an eleventh call")
	}
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
	return nil
}

func (m *v2OfflineOverRoundDirectModel) calls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.round
}

type v2OfflineResponsesCapture struct {
	mu       sync.Mutex
	requests [][]byte
	handler  error
}

func (c *v2OfflineResponsesCapture) serveHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost || request.URL.Path != "/responses" {
		c.setError(fmt.Errorf("unexpected offline Responses request %s %s", request.Method, request.URL.Path))
		http.Error(writer, "unexpected request", http.StatusBadRequest)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(writer, request.Body, 1<<20))
	if err != nil {
		c.setError(fmt.Errorf("read offline Responses request: %w", err))
		http.Error(writer, "invalid request", http.StatusBadRequest)
		return
	}
	round := c.append(body)
	var output []map[string]any
	switch round {
	case 0:
		name, err := v2WireToolName(body, "fixture.inventory")
		if err != nil {
			c.setError(err)
			http.Error(writer, "invalid first request", http.StatusBadRequest)
			return
		}
		output = []map[string]any{{"id": "offline-fc-probe", "type": "function_call", "call_id": "probe-call", "name": name, "arguments": "{}"}}
	case 1:
		name, err := v2WireToolName(body, programtools.ExecuteID)
		if err != nil {
			c.setError(err)
			http.Error(writer, "invalid second request", http.StatusBadRequest)
			return
		}
		output = []map[string]any{{"id": "offline-fc-execute", "type": "function_call", "call_id": "execute-call", "name": name, "arguments": `{"projection":"name","selection":[0,1,2,3,4,5,6,7]}`}}
	case 2:
		output = []map[string]any{{"id": "offline-final", "type": "message", "content": []map[string]any{{"type": "output_text", "text": "FINAL: Item 01, Item 02, Item 03, Item 04, Item 05, Item 06, Item 07, Item 08"}}}}
	default:
		c.setError(fmt.Errorf("unexpected offline Responses request round %d", round+1))
		http.Error(writer, "too many requests", http.StatusBadRequest)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(writer).Encode(map[string]any{
		"id": fmt.Sprintf("offline-response-%d", round+1), "status": "completed", "output": output,
		"usage": map[string]int64{"input_tokens": 1, "output_tokens": 1},
	}); err != nil {
		c.setError(fmt.Errorf("write offline Responses response: %w", err))
	}
}

func (c *v2OfflineResponsesCapture) append(body []byte) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	copyOf := append([]byte(nil), body...)
	c.requests = append(c.requests, copyOf)
	return len(c.requests) - 1
}

func (c *v2OfflineResponsesCapture) setError(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handler == nil {
		c.handler = err
	}
}

func (c *v2OfflineResponsesCapture) err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.handler
}

func (c *v2OfflineResponsesCapture) snapshot() [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	copyOf := make([][]byte, len(c.requests))
	for index := range c.requests {
		copyOf[index] = append([]byte(nil), c.requests[index]...)
	}
	return copyOf
}

func v2WireToolName(body []byte, capabilityID string) (string, error) {
	var request struct {
		Tools []struct {
			Name        string `json:"name"`
			Description string `json:"description"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(body, &request); err != nil {
		return "", fmt.Errorf("decode offline Responses request: %w", err)
	}
	for _, tool := range request.Tools {
		if strings.Contains(tool.Description, "Internal capability ID: "+capabilityID) {
			return tool.Name, nil
		}
	}
	return "", fmt.Errorf("offline Responses request does not expose %q", capabilityID)
}

func assertV2FinalResponsesRequest(t *testing.T, body []byte) {
	t.Helper()
	var request struct {
		Input []map[string]json.RawMessage `json:"input"`
		Tools json.RawMessage              `json:"tools"`
	}
	if err := json.Unmarshal(body, &request); err != nil {
		t.Fatal(err)
	}
	if len(request.Tools) != 0 && string(request.Tools) != "null" && string(request.Tools) != "[]" {
		t.Fatalf("final Responses request exposed tools: %s", request.Tools)
	}
	called := map[string]bool{}
	outputs := map[string]int{}
	for _, item := range request.Input {
		var kind, callID, output string
		if err := json.Unmarshal(item["type"], &kind); err != nil {
			continue
		}
		switch kind {
		case "function_call":
			if err := json.Unmarshal(item["call_id"], &callID); err != nil || callID == "" {
				t.Fatalf("final Responses function call is missing call_id: %s", item)
			}
			called[callID] = true
		case "function_call_output":
			if err := json.Unmarshal(item["call_id"], &callID); err != nil || callID == "" {
				t.Fatalf("final Responses function output is missing call_id: %s", item)
			}
			if !called[callID] {
				t.Fatalf("Responses function output has no prior function call: %q", callID)
			}
			if strings.HasPrefix(callID, "execute-call/") {
				t.Fatalf("nested child output leaked into final Responses request: %q", callID)
			}
			if err := json.Unmarshal(item["output"], &output); err != nil {
				t.Fatalf("decode Responses function output: %v", err)
			}
			if strings.Contains(output, strings.Repeat("x", 128)) {
				t.Fatal("nested child payload leaked into final Responses request")
			}
			outputs[callID]++
		}
	}
	if !called["execute-call"] || outputs["execute-call"] != 1 {
		t.Fatalf("program.execute parent call/output pairing called=%v outputs=%v", called, outputs)
	}
	if len(called) != 2 || len(outputs) != 2 {
		t.Fatalf("final Responses request has unexpected function frames called=%v outputs=%v", called, outputs)
	}
}

func v2NestedChildEvidence(events []core.SessionEvent, parentID string) (calls, results int) {
	for _, event := range events {
		switch event.Type {
		case core.EvToolCall:
			var data core.ToolCallData
			if json.Unmarshal(event.Data, &data) == nil && strings.HasPrefix(data.CallID, parentID+"/") {
				calls++
			}
		case core.EvToolResult:
			var data core.ToolResultData
			if json.Unmarshal(event.Data, &data) == nil && strings.HasPrefix(data.CallID, parentID+"/") {
				results++
			}
		}
	}
	return calls, results
}

type v2LiveProbe struct {
	mu     sync.Mutex
	callsN int
}

func (*v2LiveProbe) ArtifactRevision() string { return "probe-route-v2-live-fixture/v1" }
func (*v2LiveProbe) Manifest() core.CapabilityManifest {
	return core.CapabilityManifest{
		ID: "fixture.inventory", Version: "1", Name: "Neutral fixture inventory", Kind: core.KindTool, Idempotent: true, MaxOutputBytes: 4096,
		Tool:     &core.ToolExposure{Description: "Read synthetic active inventory", Parameters: map[string]any{"type": "object", "additionalProperties": false}},
		Metadata: map[string]string{programmatic.ProbeManifestKey: programmatic.ProbeManifestVersion},
	}
}
func (p *v2LiveProbe) Execute(_ context.Context, request core.CapabilityRequest) (core.CapabilityResult, error) {
	if err := core.RequireAcceptedInvocation(request); err != nil {
		return core.CapabilityResult{}, err
	}
	p.mu.Lock()
	p.callsN++
	p.mu.Unlock()
	candidates := make([]any, 0, liveProbeRouteV2Rows)
	for index := 1; index <= liveProbeRouteV2Rows; index++ {
		id := fmt.Sprintf("item-%02d", index)
		candidates = append(candidates, map[string]any{"label": id, "args": map[string]any{"id": id}, "facts": map[string]any{"active": true}})
	}
	return core.CapabilityResult{Content: "eight active synthetic items are available: item-01 through item-08", OK: true, Metadata: map[string]any{
		programmatic.ProbeFactsMetadataKey: map[string]any{"version": programmatic.ProbeFactsVersion, "followup_capability_id": "fixture.detail", "candidates": candidates, "max_model_return_bytes": float64(4096)},
	}}, nil
}
func (p *v2LiveProbe) calls() int { p.mu.Lock(); defer p.mu.Unlock(); return p.callsN }

type v2LiveDetail struct {
	activeRows, paddingBytes int
	mu                       sync.Mutex
	ids                      []string
}

func (*v2LiveDetail) ArtifactRevision() string { return "probe-route-v2-live-detail/v1" }
func (d *v2LiveDetail) Manifest() core.CapabilityManifest {
	return core.CapabilityManifest{
		ID: "fixture.detail", Version: "1", Name: "Fixture detail", Kind: core.KindTool, Idempotent: true, MaxOutputBytes: liveProbeRouteV2Padding + 512,
		Tool:         &core.ToolExposure{Description: "Read one synthetic item detail with a large read-only payload", Parameters: map[string]any{"type": "object", "required": []any{"id"}, "additionalProperties": false, "properties": map[string]any{"id": map[string]any{"type": "string"}}}},
		OutputSchema: map[string]any{"type": "object", "required": []any{"id", "name", "padding"}, "additionalProperties": false, "properties": map[string]any{"id": map[string]any{"type": "string", "maxLength": 7}, "name": map[string]any{"type": "string", "maxLength": 7}, "padding": map[string]any{"type": "string", "maxLength": liveProbeRouteV2Padding}}},
		Metadata:     map[string]string{programmatic.ExposureKey: programmatic.ExposureVersion},
	}
}
func (d *v2LiveDetail) Execute(_ context.Context, request core.CapabilityRequest) (core.CapabilityResult, error) {
	if err := core.RequireAcceptedInvocation(request); err != nil {
		return core.CapabilityResult{}, err
	}
	id, _ := request.Args["id"].(string)
	var index int
	if _, err := fmt.Sscanf(id, "item-%02d", &index); err != nil || index < 1 || index > d.activeRows {
		return core.CapabilityResult{}, errors.New("synthetic detail input is invalid")
	}
	d.mu.Lock()
	d.ids = append(d.ids, id)
	d.mu.Unlock()
	content, err := json.Marshal(map[string]any{"id": id, "name": fmt.Sprintf("Item %02d", index), "padding": strings.Repeat("x", d.paddingBytes)})
	if err != nil {
		return core.CapabilityResult{}, err
	}
	return core.CapabilityResult{Content: string(content), OK: true}, nil
}
func (d *v2LiveDetail) idsCopy() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.ids...)
}

func assertV2LiveRun(t *testing.T, arm v2LiveArm, fixture *v2LiveFixture, result core.TurnResult, model *liveModel, events []core.SessionEvent, wire []liveWireRequestEvidence, observation executionroute.SelectedRouteObservation) {
	t.Helper()
	if result.Answer != "FINAL: Item 01, Item 02, Item 03, Item 04, Item 05, Item 06, Item 07, Item 08" {
		t.Fatal("v2 live final answer did not match the frozen fixture")
	}
	rounds := model.evidenceRounds()
	if !v2LiveRoundsValid(arm, len(rounds)) {
		t.Fatalf("v2 live %s model rounds=%d", arm.name, len(rounds))
	}
	if len(wire) != len(rounds) {
		t.Fatalf("v2 live model rounds=%d Responses requests=%d", len(rounds), len(wire))
	}
	for _, round := range rounds {
		if !round.adapterStarted || round.pacingCanceled || !round.hasUsage || !round.usageConsistent {
			t.Fatal("v2 live usage or pacing evidence was incomplete")
		}
	}
	invocations := liveInvocationEvidenceFromRounds(rounds, events)
	if invocations == nil || invocations.ActualAdapterCalls != len(rounds) || invocations.PacingCanceledBeforeAdapter != 0 || !invocations.ReportedUsageComplete || !invocations.UsageProtocolConsistent || !invocations.LedgerUsageMatched || !invocations.UniqueLedgerInvocationIDs {
		t.Fatal("v2 live usage did not reconcile against the durable ledger")
	}
	contextEvidence := fixture.context.evidence()
	if contextEvidence.AssemblyCalls != len(rounds) || contextEvidence.Failures != 0 || contextEvidence.FinalInputBytes <= 0 || contextEvidence.FinalInputTokens <= 0 || contextEvidence.ContextWindowTokens <= 0 || contextEvidence.MaxOutputTokens <= 0 || contextEvidence.MaxOutputTokens >= contextEvidence.ContextWindowTokens || contextEvidence.DroppedGroups != 0 {
		t.Fatalf("v2 live context assembly evidence=%+v", contextEvidence)
	}
	expectedMenus := v2LiveExpectedMenus(arm, len(rounds))
	for index, request := range wire {
		if !request.BodyObserved || !request.JSONValid || !request.StreamPresent || !request.Stream || !request.StorePresent || request.Store {
			t.Fatal("Responses wire audit was incomplete")
		}
		if request.ToolCount != len(expectedMenus[index]) {
			t.Fatal("Responses wire tool count did not match the projected menu")
		}
		if len(expectedMenus[index]) > 0 && (!request.ParallelToolCallsPresent || !request.ParallelToolCalls) {
			t.Fatal("Responses wire request did not enable parallel calls for a nonempty menu")
		}
	}
	for index, expected := range expectedMenus {
		if !sameStrings(rounds[index].toolSchemaNames, expected) {
			t.Fatal("model-visible tool menu differed from the expected route projection")
		}
	}
	last := rounds[len(rounds)-1]
	if last.toolCalls != 0 {
		t.Fatal("final live request called a tool")
	}
	if arm.routeMode == programmatic.RouteAutoProbeOnce && len(last.toolSchemaNames) != 0 {
		t.Fatal("final v2 live request exposed a tool")
	}
	_, _, _, durableFinal := liveSessionEvidence(events)
	if strings.TrimSpace(durableFinal) != result.Answer {
		t.Fatal("durable final answer did not match the runtime answer")
	}
	if fixture.probe.calls() != 1 {
		t.Fatal("synthetic probe effects were not exact")
	}
	gotIDs := fixture.detail.idsCopy()
	sort.Strings(gotIDs)
	wantIDs := make([]string, liveProbeRouteV2Rows)
	for index := range wantIDs {
		wantIDs[index] = fmt.Sprintf("item-%02d", index+1)
	}
	if !sameStrings(gotIDs, wantIDs) {
		t.Fatal("synthetic detail effects did not cover each frozen candidate exactly once")
	}
	stages := v2LiveAssistantToolStages(t, events)
	if len(stages) < 2 || len(stages[0]) != 1 || stages[0][0] != "fixture.inventory" {
		t.Fatal("live run did not start with exactly one inventory call")
	}
	selected := "direct"
	if arm.routeMode == programmatic.RouteAutoProbeOnce {
		if len(stages) != 2 {
			t.Fatal("auto_probe_once emitted more than one post-probe tool batch")
		}
		if len(stages[1]) == 1 && stages[1][0] == programtools.ExecuteID {
			selected = "ptc"
		} else {
			if len(stages[1]) != liveProbeRouteV2Rows {
				t.Fatal("choice stage was neither one execute nor the exact direct batch")
			}
			for _, name := range stages[1] {
				if name != "fixture.detail" {
					t.Fatal("choice stage included an unplanned tool")
				}
			}
		}
		if observation.Status != executionroute.SelectionStatusVerified || string(observation.Route) != selected {
			t.Fatalf("durable selected route=%+v, structural choice=%q", observation, selected)
		}
		if rounds[0].toolCalls != 1 || rounds[1].toolCalls != len(stages[1]) {
			t.Fatal("v2 round tool accounting disagrees with the durable choice")
		}
		if selected == "ptc" && fixture.journal.completed() != liveProbeRouteV2Rows+2 {
			t.Fatal("PTC journal effects were not exact")
		}
		if selected == "direct" && fixture.journal.completed() != liveProbeRouteV2Rows+1 {
			t.Fatal("Direct journal effects were not exact")
		}
	} else {
		detailCalls := 0
		for _, stage := range stages[1:] {
			for _, name := range stage {
				if name != "fixture.detail" {
					t.Fatal("direct-only control emitted a non-detail tool after its probe")
				}
				detailCalls++
			}
		}
		if detailCalls != liveProbeRouteV2Rows || v2LiveToolCalls(rounds) != liveProbeRouteV2Rows+1 || fixture.journal.completed() != liveProbeRouteV2Rows+1 {
			t.Fatal("direct-only control did not select the direct route exactly")
		}
	}
}

func v2LiveRoundsValid(arm v2LiveArm, rounds int) bool {
	if arm.routeMode == programmatic.RouteAutoProbeOnce {
		return rounds == 3
	}
	return rounds >= 3 && rounds <= v2LiveMaxModelRounds(arm)
}

func v2LiveExpectedMenus(arm v2LiveArm, rounds int) [][]string {
	direct := []string{"fixture.detail", "fixture.inventory"}
	if arm.routeMode == programmatic.RouteAutoProbeOnce {
		return [][]string{direct, []string{"fixture.detail", programtools.ExecuteID}, nil}
	}
	menus := make([][]string, rounds)
	for index := range menus {
		menus[index] = direct
	}
	return menus
}

func v2LiveToolCalls(rounds []liveRound) int {
	total := 0
	for _, round := range rounds {
		total += round.toolCalls
	}
	return total
}

func v2LiveAssistantToolStages(t *testing.T, events []core.SessionEvent) [][]string {
	t.Helper()
	var stages [][]string
	for _, event := range events {
		if event.Type != core.EvAssistantMessage {
			continue
		}
		var data core.AssistantMessageData
		if err := json.Unmarshal(event.Data, &data); err != nil {
			t.Fatal("could not decode durable assistant evidence")
		}
		calls := append([]core.ToolCall(nil), data.ToolCalls...)
		if data.ToolCall != nil && len(calls) == 0 {
			calls = append(calls, *data.ToolCall)
		}
		if len(calls) == 0 {
			continue
		}
		names := make([]string, 0, len(calls))
		for _, call := range calls {
			names = append(names, call.Name)
		}
		stages = append(stages, names)
	}
	return stages
}

// v2LiveEvidenceRecord intentionally cannot carry prompt text, model text,
// tool arguments/results, sources, call IDs, or provider responses. Structural
// booleans, counts, and usage are enough to
// audit the acceptance predicate without creating a second data store.
type v2LiveEvidenceRecord struct {
	Schema           string                   `json:"schema"`
	Arm              string                   `json:"arm"`
	Protocol         string                   `json:"protocol"`
	RequestedModel   string                   `json:"requested_model"`
	SourceRevision   string                   `json:"source_revision"`
	RuntimeStatus    string                   `json:"runtime_status"`
	AcceptancePassed bool                     `json:"acceptance_passed"`
	FailureCategory  string                   `json:"failure_category"`
	ModelRoundBudget int                      `json:"model_round_budget"`
	ToolCallBudget   int                      `json:"tool_call_budget"`
	SelectedRoute    string                   `json:"selected_route"`
	SelectionStatus  string                   `json:"selection_status"`
	Coverage         v2LiveCoverageEvidence   `json:"coverage"`
	Accounting       v2LiveAccountingEvidence `json:"accounting"`
	Context          v2LiveContextEvidence    `json:"context"`
	Rounds           []v2LiveRoundEvidence    `json:"rounds"`
	Wire             []v2LiveWireEvidence     `json:"wire"`
	Effects          v2LiveEffectsEvidence    `json:"effects"`
}

// v2LiveAccountingEvidence is a payload-free reconciliation projection. The
// helper that derives it compares run/step and invocation identities in
// memory, but those identities never enter the evidence record.
type v2LiveAccountingEvidence struct {
	AdapterCallCount          int  `json:"adapter_call_count"`
	ReportedUsageComplete     bool `json:"reported_usage_complete"`
	UsageProtocolConsistent   bool `json:"usage_protocol_consistent"`
	LedgerUsageMatched        bool `json:"ledger_usage_matched"`
	UniqueLedgerInvocationIDs bool `json:"unique_ledger_invocation_ids"`
}

// v2LiveContextEvidence is an aggregate-only context-assembly receipt. It
// contains capacity and accounting values, never messages, tool outputs,
// call IDs, prompt text, hashes, or provider payloads.
type v2LiveContextEvidence struct {
	AssemblyCalls       int   `json:"assembly_calls"`
	Failures            int   `json:"failures"`
	FinalInputBytes     int64 `json:"final_input_bytes"`
	FinalInputTokens    int64 `json:"final_input_tokens"`
	ContextWindowTokens int   `json:"context_window_tokens"`
	MaxOutputTokens     int   `json:"max_output_tokens"`
	DroppedGroups       int   `json:"dropped_groups"`
}

const (
	v2LiveFailureNone       = "none"
	v2LiveFailureSetup      = "setup"
	v2LiveFailureRuntime    = "runtime"
	v2LiveFailureAcceptance = "acceptance"
)

type v2LiveRoundEvidence struct {
	Stage           string          `json:"stage"`
	ToolCallCount   int             `json:"tool_call_count"`
	ToolMenuCount   int             `json:"tool_menu_count"`
	ToolMenuEmpty   bool            `json:"tool_menu_empty"`
	Usage           core.TokenUsage `json:"usage"`
	UsageReported   bool            `json:"usage_reported"`
	UsageConsistent bool            `json:"usage_consistent"`
}
type v2LiveWireEvidence struct {
	BodyObserved       bool `json:"body_observed"`
	JSONValid          bool `json:"json_valid"`
	ToolCount          int  `json:"tool_count"`
	ParallelCalls      bool `json:"parallel_calls"`
	ParallelCallsKnown bool `json:"parallel_calls_known"`
	Stream             bool `json:"stream"`
	Store              bool `json:"store"`
}
type v2LiveEffectsEvidence struct {
	ProbeCalls                  int  `json:"probe_calls"`
	DetailCalls                 int  `json:"detail_calls"`
	CompletedJournalInvocations int  `json:"completed_journal_invocations"`
	FinalToolsZero              bool `json:"final_tools_zero"`
	FinalToolMenuEmpty          bool `json:"final_tool_menu_empty"`
	AnswerMatches               bool `json:"answer_matches"`
}

// v2LiveCoverageEvidence separates verified route selection from whether the
// frozen fixture's task candidates were all selected. It contains counts only:
// candidate IDs and capability inputs are used transiently for verification
// and never leave the test process in the evidence artifact.
type v2LiveCoverageEvidence struct {
	CandidateCount      int    `json:"candidate_count"`
	SelectedUniqueCount int    `json:"selected_unique_count"`
	ExecutedCount       int    `json:"executed_count"`
	DuplicateCount      int    `json:"duplicate_count"`
	ExactOnce           bool   `json:"exact_once"`
	CoverageStatus      string `json:"coverage_status"`
}

const (
	v2LiveCoverageComplete    = "complete"
	v2LiveCoverageIncomplete  = "incomplete"
	v2LiveCoverageUnavailable = "unavailable"
)

func v2LiveEvidenceFromRun(arm v2LiveArm, modelID string, result core.TurnResult, model *liveModel, events []core.SessionEvent, wire []liveWireRequestEvidence, selection executionroute.SelectedRouteObservation, passed bool, fixture *v2LiveFixture) v2LiveEvidenceRecord {
	record := v2LiveEvidenceRecord{Schema: "harness.programmatic.live-probe-route-v2-evidence/v3", Arm: arm.name, Protocol: liveProtocol(), RequestedModel: modelID, SourceRevision: liveSourceRevision(), RuntimeStatus: liveRunStatus(result), AcceptancePassed: passed, FailureCategory: v2LiveFailureCategory(result, passed), ModelRoundBudget: v2LiveMaxModelRounds(arm), ToolCallBudget: v2LiveMaxToolCalls(), SelectedRoute: string(selection.Route), SelectionStatus: string(selection.Status), Coverage: v2LiveCoverageFromRun(fixture, result.RunID, events), Accounting: v2LiveAccountingFromRun(model, events)}
	if model != nil {
		for index, round := range model.evidenceRounds() {
			stage := "direct"
			if arm.routeMode == programmatic.RouteAutoProbeOnce {
				stage = []string{"probe", "choice", "final"}[min(index, 2)]
			}
			record.Rounds = append(record.Rounds, v2LiveRoundEvidence{Stage: stage, ToolCallCount: round.toolCalls, ToolMenuCount: len(round.toolSchemaNames), ToolMenuEmpty: len(round.toolSchemaNames) == 0, Usage: round.usage, UsageReported: round.hasUsage, UsageConsistent: round.usageConsistent})
		}
	}
	for _, request := range wire {
		record.Wire = append(record.Wire, v2LiveWireEvidence{BodyObserved: request.BodyObserved, JSONValid: request.JSONValid, ToolCount: request.ToolCount, ParallelCalls: request.ParallelToolCalls, ParallelCallsKnown: request.ParallelToolCallsPresent, Stream: request.Stream, Store: request.Store})
	}
	if fixture != nil {
		record.Effects.ProbeCalls = fixture.probe.calls()
		record.Effects.DetailCalls = len(fixture.detail.idsCopy())
		record.Effects.CompletedJournalInvocations = fixture.journal.completed()
		record.Context = fixture.context.evidence()
	}
	if len(record.Rounds) > 0 {
		last := record.Rounds[len(record.Rounds)-1]
		record.Effects.FinalToolsZero = last.ToolCallCount == 0
		record.Effects.FinalToolMenuEmpty = last.ToolMenuEmpty
	}
	record.Effects.AnswerMatches = result.Answer == "FINAL: Item 01, Item 02, Item 03, Item 04, Item 05, Item 06, Item 07, Item 08"
	return record
}

// v2LiveCoverageFromRun validates only the frozen eight-row fixture. A count
// is available when every observed fixture.detail invocation has an exact
// completed journal receipt and matches an executed provider effect. Route
// selection is recorded separately and does not affect this projection. The
// returned projection deliberately contains no candidate ID, call ID,
// capability argument, result, or model content.
func v2LiveCoverageFromRun(fixture *v2LiveFixture, runID string, events []core.SessionEvent) v2LiveCoverageEvidence {
	if fixture == nil || fixture.detail == nil || fixture.journal == nil || fixture.session == nil || fixture.detail.activeRows != liveProbeRouteV2Rows || strings.TrimSpace(runID) == "" {
		return v2LiveCoverageEvidence{CoverageStatus: v2LiveCoverageUnavailable}
	}
	evidence := v2LiveCoverageEvidence{CandidateCount: liveProbeRouteV2Rows, CoverageStatus: v2LiveCoverageUnavailable}

	expected := make(map[string]struct{}, liveProbeRouteV2Rows)
	for index := 1; index <= liveProbeRouteV2Rows; index++ {
		expected[fmt.Sprintf("item-%02d", index)] = struct{}{}
	}
	calls := map[string]core.ToolCall{}
	results := map[string]core.CapabilityResult{}
	for _, event := range events {
		if event.RunID != runID {
			continue
		}
		switch event.Type {
		case core.EvToolCall:
			var data core.ToolCallData
			if json.Unmarshal(event.Data, &data) != nil || data.Name != fixture.detail.Manifest().ID {
				continue
			}
			id, ok := data.Args["id"].(string)
			if !ok || len(data.Args) != 1 {
				return evidence
			}
			if _, allowed := expected[id]; !allowed {
				return evidence
			}
			if _, duplicate := calls[data.CallID]; duplicate {
				return evidence
			}
			calls[data.CallID] = core.ToolCall{ID: data.CallID, Name: data.Name, Args: data.Args}
		case core.EvToolResult:
			var data core.ToolResultData
			if json.Unmarshal(event.Data, &data) != nil {
				continue
			}
			if _, duplicate := results[data.CallID]; duplicate {
				return evidence
			}
			results[data.CallID] = core.CapabilityResult{Content: data.Content, OK: data.OK, Metadata: data.Metadata}
		}
	}
	selected := map[string]int{}
	for callID, call := range calls {
		result, found := results[callID]
		if !found || !result.OK {
			return evidence
		}
		invocation, err := core.NewToolInvocation(core.RunInfo{RunID: runID, SessionID: fixture.session.ID(), ProfileID: fixture.session.ProfileID(), Principal: fixture.principal}, call, fixture.detail.Manifest().Idempotent)
		if err != nil {
			return evidence
		}
		record, found, err := fixture.journal.GetToolInvocation(context.Background(), invocation)
		if err != nil || !found || record.State != core.ToolInvocationCompleted || record.Result == nil || !record.Result.OK || !reflect.DeepEqual(*record.Result, result) {
			return evidence
		}
		selected[call.Args["id"].(string)]++
	}

	// The detail provider records only accepted, executed calls. Reconcile this
	// second, independent receipt before publishing a count.
	executed := map[string]int{}
	executedIDs := fixture.detail.idsCopy()
	evidence.ExecutedCount = len(executedIDs)
	for _, id := range executedIDs {
		if _, allowed := expected[id]; !allowed {
			return evidence
		}
		executed[id]++
	}
	if !reflect.DeepEqual(selected, executed) {
		return evidence
	}
	evidence.SelectedUniqueCount = len(selected)
	for _, count := range executed {
		if count > 1 {
			evidence.DuplicateCount += count - 1
		}
	}
	evidence.ExactOnce = evidence.SelectedUniqueCount == evidence.ExecutedCount && evidence.DuplicateCount == 0
	if evidence.CandidateCount == evidence.SelectedUniqueCount && evidence.SelectedUniqueCount == evidence.ExecutedCount && evidence.DuplicateCount == 0 && evidence.ExactOnce {
		evidence.CoverageStatus = v2LiveCoverageComplete
	} else {
		evidence.CoverageStatus = v2LiveCoverageIncomplete
	}
	return evidence
}

// TestV2LiveCoverageFixtureDuplicateCandidateIsIncomplete constructs the
// evidence receipts directly because route projection correctly rejects a
// duplicate Direct batch before executing it. This checks that a historical
// or independently observed repeated effect cannot be projected as complete.
func TestV2LiveCoverageFixtureDuplicateCandidateIsIncomplete(t *testing.T) {
	fixture, err := newV2LiveFixture(core.MockLlmAdapter{}, v2LiveArm{name: "coverage_duplicate", routeMode: programmatic.RouteDirectOnly})
	if err != nil {
		t.Fatal(err)
	}
	const runID = "coverage-duplicate-run"
	events := make([]core.SessionEvent, 0, liveProbeRouteV2Rows*2+2)
	appendReceipt := func(callID, id string, ok bool) {
		t.Helper()
		call := core.ToolCall{ID: callID, Name: fixture.detail.Manifest().ID, Args: map[string]any{"id": id}}
		invocation, err := core.NewToolInvocation(core.RunInfo{RunID: runID, SessionID: fixture.session.ID(), ProfileID: fixture.session.ProfileID(), Principal: fixture.principal}, call, fixture.detail.Manifest().Idempotent)
		if err != nil {
			t.Fatal(err)
		}
		if _, decision, err := fixture.journal.BeginToolInvocation(context.Background(), invocation); err != nil || decision != core.ToolInvocationExecuteNew {
			t.Fatalf("begin duplicate coverage receipt decision=%s err=%v", decision, err)
		}
		result := core.CapabilityResult{Content: fmt.Sprintf("fixture-detail-%s", id), OK: ok}
		if _, err := fixture.journal.CompleteToolInvocation(context.Background(), invocation, result); err != nil {
			t.Fatal(err)
		}
		callData, err := json.Marshal(core.ToolCallData{CallID: call.ID, Name: call.Name, Args: call.Args})
		if err != nil {
			t.Fatal(err)
		}
		resultData, err := json.Marshal(core.ToolResultData{CallID: call.ID, Content: result.Content, OK: result.OK})
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, core.SessionEvent{RunID: runID, Type: core.EvToolCall, Data: callData}, core.SessionEvent{RunID: runID, Type: core.EvToolResult, Data: resultData})
		fixture.detail.mu.Lock()
		fixture.detail.ids = append(fixture.detail.ids, id)
		fixture.detail.mu.Unlock()
	}
	for index := 1; index <= liveProbeRouteV2Rows; index++ {
		appendReceipt(fmt.Sprintf("coverage-detail-%02d", index), fmt.Sprintf("item-%02d", index), true)
	}
	appendReceipt("coverage-detail-duplicate", "item-01", true)

	coverage := v2LiveCoverageFromRun(fixture, runID, events)
	want := v2LiveCoverageEvidence{CandidateCount: liveProbeRouteV2Rows, SelectedUniqueCount: liveProbeRouteV2Rows, ExecutedCount: liveProbeRouteV2Rows + 1, DuplicateCount: 1, CoverageStatus: v2LiveCoverageIncomplete}
	if coverage != want {
		t.Fatalf("duplicate candidate coverage=%+v want=%+v", coverage, want)
	}
}

func TestV2LiveCoverageFixtureFailedResultIsNotExactOnce(t *testing.T) {
	fixture, err := newV2LiveFixture(core.MockLlmAdapter{}, v2LiveArm{name: "coverage_failed_result", routeMode: programmatic.RouteDirectOnly})
	if err != nil {
		t.Fatal(err)
	}
	const runID = "coverage-failed-result-run"
	events := make([]core.SessionEvent, 0, liveProbeRouteV2Rows*2)
	for index := 1; index <= liveProbeRouteV2Rows; index++ {
		id := fmt.Sprintf("item-%02d", index)
		call := core.ToolCall{ID: fmt.Sprintf("coverage-failed-detail-%02d", index), Name: fixture.detail.Manifest().ID, Args: map[string]any{"id": id}}
		invocation, err := core.NewToolInvocation(core.RunInfo{RunID: runID, SessionID: fixture.session.ID(), ProfileID: fixture.session.ProfileID(), Principal: fixture.principal}, call, fixture.detail.Manifest().Idempotent)
		if err != nil {
			t.Fatal(err)
		}
		if _, decision, err := fixture.journal.BeginToolInvocation(context.Background(), invocation); err != nil || decision != core.ToolInvocationExecuteNew {
			t.Fatalf("begin failed-result coverage receipt decision=%s err=%v", decision, err)
		}
		ok := index != 1
		result := core.CapabilityResult{Content: fmt.Sprintf("fixture-detail-%s", id), OK: ok}
		if _, err := fixture.journal.CompleteToolInvocation(context.Background(), invocation, result); err != nil {
			t.Fatal(err)
		}
		callData, err := json.Marshal(core.ToolCallData{CallID: call.ID, Name: call.Name, Args: call.Args})
		if err != nil {
			t.Fatal(err)
		}
		resultData, err := json.Marshal(core.ToolResultData{CallID: call.ID, Content: result.Content, OK: result.OK})
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, core.SessionEvent{RunID: runID, Type: core.EvToolCall, Data: callData}, core.SessionEvent{RunID: runID, Type: core.EvToolResult, Data: resultData})
		fixture.detail.mu.Lock()
		fixture.detail.ids = append(fixture.detail.ids, id)
		fixture.detail.mu.Unlock()
	}
	coverage := v2LiveCoverageFromRun(fixture, runID, events)
	if coverage.ExactOnce || coverage.CoverageStatus == v2LiveCoverageComplete {
		t.Fatalf("failed detail result was counted as exact-once coverage: %+v", coverage)
	}
}

func v2LiveAccountingFromRun(model *liveModel, events []core.SessionEvent) v2LiveAccountingEvidence {
	var rounds []liveRound
	if model != nil {
		rounds = model.evidenceRounds()
	}
	evidence := liveInvocationEvidenceFromRounds(rounds, events)
	if evidence == nil {
		return v2LiveAccountingEvidence{}
	}
	return v2LiveAccountingEvidence{
		AdapterCallCount:          evidence.ActualAdapterCalls,
		ReportedUsageComplete:     evidence.ReportedUsageComplete,
		UsageProtocolConsistent:   evidence.UsageProtocolConsistent,
		LedgerUsageMatched:        evidence.LedgerUsageMatched,
		UniqueLedgerInvocationIDs: evidence.UniqueLedgerInvocationIDs,
	}
}

func v2LiveFailureCategory(result core.TurnResult, passed bool) string {
	if passed {
		return v2LiveFailureNone
	}
	if result.Status == "" {
		return v2LiveFailureSetup
	}
	if result.Status != core.RunCompleted {
		return v2LiveFailureRuntime
	}
	return v2LiveFailureAcceptance
}

func writeV2LiveEvidence(directory string, record v2LiveEvidenceRecord) error {
	if strings.TrimSpace(directory) == "" {
		return nil
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return errors.New("v2 live evidence directory is unavailable")
	}
	payload, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return errors.New("v2 live evidence encoding failed")
	}
	identity := sha256.Sum256([]byte(record.Arm + "\x00" + time.Now().UTC().Format(time.RFC3339Nano)))
	return writeLiveEvidenceFile(filepath.Join(directory, "programmatic-live-probe-route-v2-"+fmt.Sprintf("%x", identity[:12])+".json"), append(payload, '\n'))
}

func min(left, right int) int {
	if left < right {
		return left
	}
	return right
}

func TestV2LiveEvidenceStaysContentFree(t *testing.T) {
	secretPrompt := "probe-route-v2-test-secret-prompt"
	secretArgs := "probe-route-v2-test-secret-args"
	secretResult := "probe-route-v2-test-secret-result"
	secretSource := "probe-route-v2-test-secret-ptc-source"
	assistant, err := json.Marshal(core.AssistantMessageData{Text: secretPrompt, ToolCalls: []core.ToolCall{{ID: "call", Name: programtools.ExecuteID, Args: map[string]any{"source": secretSource, "input": map[string]any{"secret": secretArgs}}}}})
	if err != nil {
		t.Fatal("could not prepare content-bearing durable event")
	}
	toolCall, err := json.Marshal(core.ToolCallData{CallID: "call", Name: programtools.ExecuteID, Args: map[string]any{"source": secretSource, "input": map[string]any{"secret": secretArgs}}})
	if err != nil {
		t.Fatal("could not prepare content-bearing tool event")
	}
	toolResult, err := json.Marshal(core.ToolResultData{CallID: "call", OK: true, Content: secretResult})
	if err != nil {
		t.Fatal("could not prepare content-bearing tool result")
	}
	model := &liveModel{observations: []liveRound{{toolCalls: 1, toolSchemaNames: []string{"fixture.inventory"}, usage: core.TokenUsage{InputTokens: 1, OutputTokens: 1}, hasUsage: true, usageConsistent: true}, {toolCalls: 1, toolSchemaNames: []string{"fixture.detail"}, usage: core.TokenUsage{InputTokens: 1, OutputTokens: 1}, hasUsage: true, usageConsistent: true}, {toolCalls: 0, usage: core.TokenUsage{InputTokens: 1, OutputTokens: 1}, hasUsage: true, usageConsistent: true}}}
	result := core.TurnResult{Answer: secretResult}
	record := v2LiveEvidenceFromRun(v2LiveArm{name: "auto_probe_once", routeMode: programmatic.RouteAutoProbeOnce}, "test-model", result, model, []core.SessionEvent{{Type: core.EvUserMessage, Data: json.RawMessage(fmt.Sprintf(`{"text":%q}`, secretPrompt))}, {Type: core.EvAssistantMessage, Data: assistant}, {Type: core.EvToolCall, Data: toolCall}, {Type: core.EvToolResult, Data: toolResult}}, []liveWireRequestEvidence{{RequestBytes: 123, RequestSHA256: secretSource, ToolCount: 1}}, executionroute.SelectedRouteObservation{Route: executionroute.SelectedRouteDirect, Status: executionroute.SelectionStatusVerified}, false, nil)
	if record.Schema != "harness.programmatic.live-probe-route-v2-evidence/v3" || record.Coverage != (v2LiveCoverageEvidence{CoverageStatus: v2LiveCoverageUnavailable}) {
		t.Fatalf("v3 content-free evidence shape=%+v", record)
	}
	if record.ModelRoundBudget != 3 || record.ToolCallBudget != v2LiveMaxToolCalls() {
		t.Fatalf("auto evidence budgets rounds=%d tools=%d", record.ModelRoundBudget, record.ToolCallBudget)
	}
	payload, err := json.Marshal(record)
	if err != nil {
		t.Fatal("could not encode v2 live evidence")
	}
	encoded := string(payload)
	for _, secret := range []string{secretPrompt, secretArgs, secretResult, secretSource} {
		if strings.Contains(encoded, secret) {
			t.Fatal("v2 live evidence retained content-bearing test data")
		}
	}
	for _, forbidden := range []string{"request_sha256", "request_bytes", "message_bytes", "tool_schema_bytes", "tool_menu_sha256"} {
		if strings.Contains(encoded, forbidden) {
			t.Fatal("v2 live evidence retained a content side-channel field")
		}
	}
	for _, forbidden := range []string{"call_id", "endpoint", "api_key", "elapsed", "candidate_ids", "selected_ids", "executed_ids", "duplicate_ids"} {
		if strings.Contains(encoded, forbidden) {
			t.Fatal("v2 live evidence retained a forbidden identifier or timing field")
		}
	}
}

func TestV2LiveEvidenceDirectUsesLastRoundForFinalFacts(t *testing.T) {
	arm := v2LiveArm{name: "direct_only", routeMode: programmatic.RouteDirectOnly}
	model := &liveModel{observations: []liveRound{
		{toolCalls: 1, toolSchemaNames: []string{"fixture.detail", "fixture.inventory"}, usage: core.TokenUsage{InputTokens: 1, OutputTokens: 1}, hasUsage: true, usageConsistent: true},
		{toolCalls: 4, toolSchemaNames: []string{"fixture.detail", "fixture.inventory"}, usage: core.TokenUsage{InputTokens: 1, OutputTokens: 1}, hasUsage: true, usageConsistent: true},
		{toolCalls: 4, toolSchemaNames: []string{"fixture.detail", "fixture.inventory"}, usage: core.TokenUsage{InputTokens: 1, OutputTokens: 1}, hasUsage: true, usageConsistent: true},
		{toolCalls: 0, toolSchemaNames: []string{"fixture.detail", "fixture.inventory"}, usage: core.TokenUsage{InputTokens: 1, OutputTokens: 1}, hasUsage: true, usageConsistent: true},
	}}
	record := v2LiveEvidenceFromRun(arm, "test-model", core.TurnResult{Status: core.RunCompleted}, model, nil, nil, executionroute.SelectedRouteObservation{}, true, nil)
	if record.ModelRoundBudget != liveProbeRouteV2Rows+2 || record.ToolCallBudget != liveProbeRouteV2Rows+2 || len(record.Rounds) != 4 || !record.Effects.FinalToolsZero || record.Effects.FinalToolMenuEmpty {
		t.Fatalf("direct evidence=%+v", record)
	}
	for _, round := range record.Rounds {
		if round.Stage != "direct" {
			t.Fatalf("direct evidence stage=%q", round.Stage)
		}
	}
}

func TestV2LiveEvidenceAccountingReconcilesWithoutPersistingIDs(t *testing.T) {
	step, err := json.Marshal(core.StepData{Index: 0})
	if err != nil {
		t.Fatal("could not prepare durable step evidence")
	}
	usage, err := json.Marshal(core.RunUsageData{InputTokens: 3, OutputTokens: 2, InvocationID: "model:11"})
	if err != nil {
		t.Fatal("could not prepare durable usage evidence")
	}
	model := &liveModel{observations: []liveRound{{runID: "accounting-test", step: 0, adapterStarted: true, usage: core.TokenUsage{InputTokens: 3, OutputTokens: 2}, hasUsage: true, usageConsistent: true}}}
	record := v2LiveEvidenceFromRun(v2LiveArm{name: "auto_probe_once", routeMode: programmatic.RouteAutoProbeOnce}, "test-model", core.TurnResult{Status: core.RunCompleted}, model, []core.SessionEvent{{RunID: "accounting-test", Seq: 11, Type: core.EvStepStart, Data: step}, {RunID: "accounting-test", Seq: 12, Type: core.EvRunUsage, Data: usage}}, nil, executionroute.SelectedRouteObservation{Route: executionroute.SelectedRouteDirect, Status: executionroute.SelectionStatusVerified}, true, nil)
	if record.Accounting != (v2LiveAccountingEvidence{AdapterCallCount: 1, ReportedUsageComplete: true, UsageProtocolConsistent: true, LedgerUsageMatched: true, UniqueLedgerInvocationIDs: true}) {
		t.Fatalf("v2 accounting=%+v", record.Accounting)
	}
	if record.FailureCategory != v2LiveFailureNone {
		t.Fatalf("successful failure category=%q", record.FailureCategory)
	}
	payload, err := json.Marshal(record)
	if err != nil {
		t.Fatal("could not encode v2 accounting evidence")
	}
	if strings.Contains(string(payload), "accounting-test") || strings.Contains(string(payload), "model:11") {
		t.Fatal("v2 accounting persisted a reconciliation identifier")
	}
}

func TestV2LiveEvidenceFailureCategoryAndIncompleteAccounting(t *testing.T) {
	model := &liveModel{observations: []liveRound{{runID: "failed-accounting-test", step: 0, adapterStarted: true, usageConsistent: true}}}
	record := v2LiveEvidenceFromRun(v2LiveArm{name: "auto_probe_once", routeMode: programmatic.RouteAutoProbeOnce}, "test-model", core.TurnResult{Status: core.RunFailed}, model, nil, nil, executionroute.SelectedRouteObservation{Route: executionroute.SelectedRouteUnavailable, Status: executionroute.SelectionStatusUnavailable}, false, nil)
	if record.FailureCategory != v2LiveFailureRuntime {
		t.Fatalf("failure category=%q", record.FailureCategory)
	}
	if record.Accounting.AdapterCallCount != 1 || record.Accounting.ReportedUsageComplete || !record.Accounting.UsageProtocolConsistent || record.Accounting.LedgerUsageMatched || record.Accounting.UniqueLedgerInvocationIDs {
		t.Fatalf("failed v2 accounting=%+v", record.Accounting)
	}
	for _, test := range []struct {
		name   string
		result core.TurnResult
		passed bool
		want   string
	}{
		{name: "success", result: core.TurnResult{Status: core.RunCompleted}, passed: true, want: v2LiveFailureNone},
		{name: "setup", want: v2LiveFailureSetup},
		{name: "runtime", result: core.TurnResult{Status: core.RunCancelled}, want: v2LiveFailureRuntime},
		{name: "acceptance", result: core.TurnResult{Status: core.RunCompleted}, want: v2LiveFailureAcceptance},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := v2LiveFailureCategory(test.result, test.passed); got != test.want {
				t.Fatalf("failure category=%q want=%q", got, test.want)
			}
		})
	}
}
