package executionroute

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/whhhh1500/auto-agent/pkg/app/programmatic"
	"github.com/whhhh1500/auto-agent/pkg/app/runexecutor"
	"github.com/whhhh1500/auto-agent/pkg/core"
)

func TestV2ContinueTurnRejectsResolvedModelRevisionDriftBeforeModelCall(t *testing.T) {
	fixture := newV2RuntimeFixture(t, v2RuntimeDirect, false, false)
	runID := "run_v2_continue_drift"
	appendOpenV2Continuation(t, fixture, runID)

	drift := &v2DriftModel{inner: &v2RuntimeScriptModel{mode: v2RuntimeDirect}}
	resolver := &switchingV2ModelResolver{adapter: fixture.model}
	fixture.runtime.Models = resolver
	decision, err := Resolve(context.Background(), Request{
		Runtime: fixture.runtime, Registry: fixture.registry, Principal: fixture.principal, Session: fixture.session, RunID: runID, Resume: true,
	})
	if err != nil {
		t.Fatalf("Resolve resume route: %v", err)
	}
	resolver.set(drift)
	continuation, ok := decision.Executor.(runexecutor.ContinuingRunExecutor)
	if !ok {
		t.Fatalf("selected executor does not expose post-result continuation: %T", decision.Executor)
	}
	result, err := continuation.ContinueTurn(context.Background(), fixture.principal, fixture.session, core.ResumeInput{
		RunID: runID, CompositionMetadata: decision.CompositionMetadata,
	}, nil)
	if err == nil {
		t.Fatalf("ContinueTurn accepted model revision drift: result=%#v", result)
	}
	if result.Status != "" {
		t.Fatalf("composition-time admission failure must not append a terminal result: %#v", result)
	}
	if got := drift.calls(); got != 0 {
		t.Fatalf("drifted model was called %d times", got)
	}
}

func TestV2ContinueTurnRejectsCurrentCapabilityRevisionDriftBeforeModelCall(t *testing.T) {
	fixture := newV2RuntimeFixture(t, v2RuntimeDirect, false, false)
	const runID = "run_v2_continue_capability_drift"
	appendOpenV2Continuation(t, fixture, runID)

	decision, err := Resolve(context.Background(), Request{
		Runtime: fixture.runtime, Registry: fixture.registry, Principal: fixture.principal, Session: fixture.session, RunID: runID, Resume: true,
	})
	if err != nil {
		t.Fatalf("Resolve resume route: %v", err)
	}
	fixture.detail.setArtifactRevision("v2-runtime-detail/v2")
	continuation, ok := decision.Executor.(runexecutor.ContinuingRunExecutor)
	if !ok {
		t.Fatalf("selected executor does not expose post-result continuation: %T", decision.Executor)
	}
	result, err := continuation.ContinueTurn(context.Background(), fixture.principal, fixture.session, core.ResumeInput{
		RunID: runID, CompositionMetadata: decision.CompositionMetadata,
	}, nil)
	if err == nil || result.Status != "" {
		t.Fatalf("ContinueTurn accepted capability revision drift: result=%#v err=%v", result, err)
	}
	if got := fixture.model.calls(); got != 0 {
		t.Fatalf("model was called %d times after capability revision drift", got)
	}
}

// v2DriftModel presents a distinct resolved model artifact while preserving
// the same protocol behavior. The admission wrapper must reject it before
// forwarding a stream to the underlying adapter.
type v2DriftModel struct {
	inner *v2RuntimeScriptModel
}

func (*v2DriftModel) Provider() string         { return "v2-runtime-script" }
func (*v2DriftModel) ArtifactRevision() string { return "v2-runtime-script/v2" }
func (m *v2DriftModel) Stream(ctx context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	return m.inner.Stream(ctx, options, emit)
}
func (m *v2DriftModel) calls() int { return m.inner.calls() }

type switchingV2ModelResolver struct {
	mu      sync.Mutex
	adapter core.LlmAdapter
}

func (r *switchingV2ModelResolver) ResolveModel(context.Context, core.ModelSelection) (core.LlmAdapter, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.adapter, nil
}

func (r *switchingV2ModelResolver) set(adapter core.LlmAdapter) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.adapter = adapter
}

func appendOpenV2Continuation(t *testing.T, fixture *v2RuntimeFixture, runID string) {
	t.Helper()
	profile, err := fixture.runtime.Profiles.Resolve(fixture.principal, fixture.session.Scope(), fixture.session.ProfileID())
	if err != nil {
		t.Fatal(err)
	}
	capabilities, err := (core.CapabilityResolver{Registry: fixture.runtime.Capabilities}).Resolve(fixture.principal, fixture.session.Scope())
	if err != nil {
		t.Fatal(err)
	}
	capabilities, err = profile.FilterCapabilities(capabilities)
	if err != nil {
		t.Fatal(err)
	}
	route := defaultRouteSelection()
	route.version, route.mode, route.implementation = RouteProbeVersion, programmatic.RouteAutoProbeOnce, RouteProbeImplementationRevision
	metadata := map[string]string{}
	route.writeMetadata(metadata)
	composition := &core.RunCompositionData{
		Profile: *profile, Capabilities: capabilities.FilterByPermissions(fixture.principal.Grants).Capabilities(), EffectivePermissions: fixture.principal.Grants.Clone(),
		Model: profile.Model, ResolvedProvider: fixture.model.Provider(), ModelRevision: v2ModelArtifactRevision(fixture.model),
		MaxSteps: profile.MaxSteps, MaxToolCalls: profile.MaxToolCalls, Metadata: metadata,
	}
	compositionRevision, err := core.CompositionRevision(composition)
	if err != nil {
		t.Fatal(err)
	}
	assignmentRevision, err := core.CompositionMetadataRevision(metadata)
	if err != nil {
		t.Fatal(err)
	}
	appendEvent := func(kind core.SessionEventType, data any) core.SessionEvent {
		event, appendErr := fixture.session.Append(runID, kind, data)
		if appendErr != nil {
			t.Fatal(appendErr)
		}
		return event
	}
	appendEvent(core.EvRunStart, core.RunStartData{Composition: composition, CompositionRevision: compositionRevision, AssignmentRevision: assignmentRevision})
	appendEvent(core.EvUserMessage, core.UserMessageData{Text: "continue"})
	step := appendEvent(core.EvStepStart, core.StepData{Index: 0})
	call := core.ToolCall{ID: "prior-detail", Name: "records.detail", Args: map[string]any{"id": "record-17"}}
	appendEvent(core.EvAssistantMessage, core.AssistantMessageData{ToolCall: &call, ToolCalls: []core.ToolCall{call}})
	appendEvent(core.EvRunUsage, core.RunUsageData{InvocationID: "model:" + strconv.FormatInt(step.Seq, 10)})
	appendEvent(core.EvToolCall, core.ToolCallData{CallID: call.ID, Name: call.Name, Args: call.Args})
	appendEvent(core.EvToolResult, core.ToolResultData{CallID: call.ID, Content: `{"id":"record-17"}`, OK: true})
}

func TestChoiceVerifierAcceptsCompletedNestedPTCChildren(t *testing.T) {
	fixture := newV2RuntimeFixture(t, v2RuntimePTC, false, false)
	result, err := fixture.run(t, "run_v2_choice_runtime")
	if err != nil || result.Status != core.RunCompleted {
		t.Fatalf("PTC run result=%#v err=%v", result, err)
	}
	plan, isProbe, err := ResolveProbePlan(context.Background(), fixture.session, fixture.principal, fixture.journal, "run_v2_choice_runtime", "probe")
	if err != nil || !isProbe {
		t.Fatalf("probe plan isProbe=%t err=%v", isProbe, err)
	}
	if ok, err := VerifyChoiceResult(context.Background(), fixture.session, fixture.principal, fixture.journal, "run_v2_choice_runtime", "execute", plan, programmatic.ChoiceRouteExecute); err != nil || !ok {
		t.Fatalf("completed nested PTC proof ok=%t err=%v", ok, err)
	}
}

func TestChoiceVerifierRejectsPTCChildrenThatDuplicatePreparedTarget(t *testing.T) {
	const runID = "run_v2_choice_nested_duplicate_target"
	fixture := newV2RuntimeFixture(t, v2RuntimePTC, false, false)
	result, err := fixture.run(t, runID)
	if err != nil || result.Status != core.RunCompleted {
		t.Fatalf("PTC run result=%#v err=%v", result, err)
	}
	plan := mustV2ChoicePlan(t, fixture, runID)
	frozen, err := frozenProbeComposition(fixture.session.Events(), runID)
	if err != nil {
		t.Fatal(err)
	}
	transcript, err := choiceTranscriptFor(fixture.session.Events(), runID, frozen)
	if err != nil {
		t.Fatal(err)
	}
	batch, _, err := transcript.batchFor("execute")
	if err != nil {
		t.Fatal(err)
	}
	var firstID, secondID string
	for id := range transcript.nested {
		if firstID == "" {
			firstID = id
			continue
		}
		secondID = id
		break
	}
	if firstID == "" || secondID == "" {
		t.Fatal("PTC nested child evidence is incomplete")
	}
	first, second := transcript.nested[firstID], transcript.nested[secondID]
	firstKey, err := nestedChoiceTargetKey(first.call.Args)
	if err != nil {
		t.Fatal(err)
	}
	secondKey, err := nestedChoiceTargetKey(second.call.Args)
	if err != nil {
		t.Fatal(err)
	}
	if firstKey == secondKey {
		t.Fatal("fixture did not produce distinct prepared child targets")
	}
	duplicateArgs := make(map[string]any, len(second.call.Args))
	for key, value := range second.call.Args {
		duplicateArgs[key] = value
	}
	first.call.Args = duplicateArgs
	transcript.nested[firstID] = first
	if err := verifyNestedChoiceEvidence(context.Background(), fixture.session, fixture.principal, fixture.journal, runID, frozen, plan, transcript, batch, programmatic.ChoiceRouteExecute); err == nil || !strings.Contains(err.Error(), "nested program child coverage") {
		t.Fatalf("PTC nested proof accepted a duplicate prepared target: %v", err)
	}
}

func TestChoiceVerifierRejectsIncompleteOrConflictingDirectBatchRecords(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(map[string]core.ToolInvocationRecord, string)
	}{
		{
			name: "missing sibling record",
			mutate: func(records map[string]core.ToolInvocationRecord, key string) {
				delete(records, key)
			},
		},
		{
			name: "started sibling record",
			mutate: func(records map[string]core.ToolInvocationRecord, key string) {
				record := records[key]
				record.State, record.Result = core.ToolInvocationStarted, nil
				records[key] = record
			},
		},
		{
			name: "uncertain sibling record",
			mutate: func(records map[string]core.ToolInvocationRecord, key string) {
				record := records[key]
				record.State, record.Result = core.ToolInvocationUncertain, nil
				records[key] = record
			},
		},
		{
			name: "same key invocation conflict",
			mutate: func(records map[string]core.ToolInvocationRecord, key string) {
				record := records[key]
				record.ArgsDigest = "sha256:conflicting-invocation"
				records[key] = record
			},
		},
		{
			name: "conflicting sibling result",
			mutate: func(records map[string]core.ToolInvocationRecord, key string) {
				record := records[key]
				result := *record.Result
				result.Content = `{"id":"record-18","name":"changed"}`
				record.Result = &result
				records[key] = record
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			const runID = "run_v2_choice_batch_reject"
			fixture := newV2RuntimeFixture(t, v2RuntimeDirect, false, false)
			result, err := fixture.run(t, runID)
			if err != nil || result.Status != core.RunCompleted {
				t.Fatalf("direct run result=%#v err=%v", result, err)
			}
			plan := mustV2ChoicePlan(t, fixture, runID)
			mutateV2ChoiceRecord(t, fixture.journal, runID, "detail-18", test.mutate)

			ok, err := VerifyChoiceResult(context.Background(), fixture.session, fixture.principal, fixture.journal, runID, "detail-17", plan, programmatic.ChoiceRouteDirect)
			if err == nil || ok {
				t.Fatalf("accepted changed sibling direct result: ok=%t err=%v", ok, err)
			}
		})
	}
}

func TestChoiceVerifierRequiresSuccessfulCanonicalJournalResults(t *testing.T) {
	t.Run("direct", func(t *testing.T) {
		const runID = "run_v2_choice_direct_not_ok"
		fixture := newV2RuntimeFixture(t, v2RuntimeDirect, false, false)
		result, err := fixture.run(t, runID)
		if err != nil || result.Status != core.RunCompleted {
			t.Fatalf("direct run result=%#v err=%v", result, err)
		}
		frozen, err := frozenProbeComposition(fixture.session.Events(), runID)
		if err != nil {
			t.Fatal(err)
		}
		transcript, err := choiceTranscriptFor(fixture.session.Events(), runID, frozen)
		if err != nil {
			t.Fatal(err)
		}
		evidence := transcript.calls["detail-17"]
		evidence.output.OK = false
		mutateV2ChoiceRecord(t, fixture.journal, runID, "detail-17", func(records map[string]core.ToolInvocationRecord, key string) {
			record := records[key]
			result := *record.Result
			result.OK = false
			record.Result = &result
			records[key] = record
		})
		capability, err := frozen.capability(evidence.call.Name)
		if err != nil {
			t.Fatal(err)
		}
		if err := verifyChoiceInvocation(context.Background(), fixture.session, fixture.principal, fixture.journal, runID, evidence, capability); err == nil {
			t.Fatal("direct choice accepted a canonical journal result with OK=false")
		}
	})

	t.Run("nested child", func(t *testing.T) {
		const runID = "run_v2_choice_nested_not_ok"
		fixture := newV2RuntimeFixture(t, v2RuntimePTC, false, false)
		result, err := fixture.run(t, runID)
		if err != nil || result.Status != core.RunCompleted {
			t.Fatalf("PTC run result=%#v err=%v", result, err)
		}
		frozen, err := frozenProbeComposition(fixture.session.Events(), runID)
		if err != nil {
			t.Fatal(err)
		}
		transcript, err := choiceTranscriptFor(fixture.session.Events(), runID, frozen)
		if err != nil {
			t.Fatal(err)
		}
		childID := ""
		for id, evidence := range transcript.nested {
			childID = id
			evidence.output.OK = false
			transcript.nested[id] = evidence
			break
		}
		if childID == "" {
			t.Fatal("completed PTC child evidence is unavailable")
		}
		mutateV2ChoiceRecord(t, fixture.journal, runID, childID, func(records map[string]core.ToolInvocationRecord, key string) {
			record := records[key]
			result := *record.Result
			result.OK = false
			record.Result = &result
			records[key] = record
		})
		batch, _, err := transcript.batchFor("execute")
		if err != nil {
			t.Fatal(err)
		}
		if err := verifyNestedChoiceEvidence(context.Background(), fixture.session, fixture.principal, fixture.journal, runID, frozen, mustV2ChoicePlan(t, fixture, runID), transcript, batch, programmatic.ChoiceRouteExecute); err == nil {
			t.Fatal("nested choice accepted a canonical journal result with OK=false")
		}
	})
}

func TestChoiceVerifierRejectsDuplicateAndOrphanEventEvidence(t *testing.T) {
	for _, test := range []struct {
		name string
		kind core.SessionEventType
		data any
	}{
		{
			name: "duplicate tool call",
			kind: core.EvToolCall,
			data: core.ToolCallData{CallID: "detail-17", Name: "records.detail", Args: map[string]any{"id": "record-17"}},
		},
		{
			name: "orphan tool result",
			kind: core.EvToolResult,
			data: core.ToolResultData{CallID: "orphan-detail", Content: `{"id":"orphan-detail"}`, OK: true},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			const runID = "run_v2_choice_event_reject"
			fixture := newV2RuntimeFixture(t, v2RuntimeDirect, false, false)
			result, err := fixture.run(t, runID)
			if err != nil || result.Status != core.RunCompleted {
				t.Fatalf("direct run result=%#v err=%v", result, err)
			}
			plan := mustV2ChoicePlan(t, fixture, runID)
			if _, err := fixture.session.Append(runID, test.kind, test.data); err != nil {
				t.Fatalf("append adversarial evidence: %v", err)
			}

			ok, err := VerifyChoiceResult(context.Background(), fixture.session, fixture.principal, fixture.journal, runID, "detail-17", plan, programmatic.ChoiceRouteDirect)
			if err == nil || ok {
				t.Fatalf("accepted %s: ok=%t err=%v", test.name, ok, err)
			}
		})
	}
}

func mustV2ChoicePlan(t *testing.T, fixture *v2RuntimeFixture, runID string) programmatic.ProbePlan {
	t.Helper()
	plan, isProbe, err := ResolveProbePlan(context.Background(), fixture.session, fixture.principal, fixture.journal, runID, "probe")
	if err != nil || !isProbe {
		t.Fatalf("resolve trusted probe plan isProbe=%t err=%v", isProbe, err)
	}
	return plan
}

func mutateV2ChoiceRecord(t *testing.T, journal *v2RuntimeJournal, runID, callID string, mutate func(map[string]core.ToolInvocationRecord, string)) {
	t.Helper()
	journal.mu.Lock()
	defer journal.mu.Unlock()
	key := v2RuntimeJournalKey(core.ToolInvocation{SessionID: "v2-runtime-session", RunID: runID, CallID: callID})
	record, found := journal.records[key]
	if !found || record.State != core.ToolInvocationCompleted || record.Result == nil {
		t.Fatalf("completed journal record %q is unavailable: %#v", callID, record)
	}
	mutate(journal.records, key)
}
