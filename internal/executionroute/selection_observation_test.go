package executionroute

import (
	"context"
	"testing"

	"github.com/whhhh1500/auto-agent/pkg/app/programmatic"
	"github.com/whhhh1500/auto-agent/pkg/core"
)

func TestObserveSelectedRouteVerifiesDirectAndPTCBeforeFinalAnswerQuality(t *testing.T) {
	for _, test := range []struct {
		name  string
		mode  v2RuntimeMode
		want  SelectedRoute
		runID string
	}{
		{name: "direct", mode: v2RuntimeDirect, want: SelectedRouteDirect, runID: "observe-direct"},
		{name: "ptc", mode: v2RuntimePTC, want: SelectedRoutePTC, runID: "observe-ptc"},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newV2RuntimeFixture(t, test.mode, false, false)
			result, err := fixture.run(t, test.runID)
			if err != nil || result.Status != core.RunCompleted {
				t.Fatalf("run result=%#v err=%v", result, err)
			}
			// The observation deliberately has no final-answer input. A caller may
			// reject this answer independently without erasing the verified route.
			result.Answer = "wrong final answer"
			observation := ObserveSelectedRoute(context.Background(), fixture.session, fixture.principal, fixture.journal, test.runID)
			if observation != (SelectedRouteObservation{Route: test.want, Status: SelectionStatusVerified}) {
				t.Fatalf("observation=%+v", observation)
			}
		})
	}
}

func TestObserveSelectedRouteVerifiesSingleCandidateDirectBeforeQuality(t *testing.T) {
	fixture := newProbeResolverFixture(t)
	fixture.appendValidReceipt(t)
	choice := core.ToolCall{ID: "choice-direct", Name: fixture.followup.Manifest.ID, Args: map[string]any{"id": "record-17"}}
	result := core.CapabilityResult{OK: true, Content: `{"id":"record-17","name":"record 17"}`}
	appendObservedChoiceEvents(t, fixture, []core.ToolCall{choice}, []core.CapabilityResult{result})
	storeObservedChoiceRecord(t, fixture, choice, result)

	observation := ObserveSelectedRoute(context.Background(), fixture.session, fixture.principal, fixture.journal, fixture.runID)
	if observation != (SelectedRouteObservation{Route: SelectedRouteDirect, Status: SelectionStatusVerified}) {
		t.Fatalf("observation=%+v", observation)
	}
}

func TestObserveSelectedRouteReturnsUnavailableForMixedOrDuplicatedChoiceEvidence(t *testing.T) {
	for _, test := range []struct {
		name  string
		calls []core.ToolCall
	}{
		{
			name: "mixed direct and execute",
			calls: []core.ToolCall{
				{ID: "direct", Name: "records.detail", Args: map[string]any{"id": "record-17"}},
				{ID: "execute", Name: programmatic.DefaultExecuteToolID, Args: map[string]any{"source": "not used"}},
			},
		},
		{
			name: "duplicated direct candidate",
			calls: []core.ToolCall{
				{ID: "direct-one", Name: "records.detail", Args: map[string]any{"id": "record-17"}},
				{ID: "direct-two", Name: "records.detail", Args: map[string]any{"id": "record-17"}},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newProbeResolverFixture(t)
			fixture.appendValidReceipt(t)
			results := make([]core.CapabilityResult, len(test.calls))
			for index := range results {
				results[index] = core.CapabilityResult{OK: true, Content: "complete"}
			}
			appendObservedChoiceEvents(t, fixture, test.calls, results)
			observation := ObserveSelectedRoute(context.Background(), fixture.session, fixture.principal, fixture.journal, fixture.runID)
			if observation != (SelectedRouteObservation{Route: SelectedRouteUnavailable, Status: SelectionStatusUnavailable}) {
				t.Fatalf("observation=%+v", observation)
			}
		})
	}
}

func TestObserveSelectedRouteReturnsUnavailableForMissingOrConflictingJournalEvidence(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(map[string]core.ToolInvocationRecord, string)
	}{
		{
			name: "missing",
			mutate: func(records map[string]core.ToolInvocationRecord, key string) {
				delete(records, key)
			},
		},
		{
			name: "started",
			mutate: func(records map[string]core.ToolInvocationRecord, key string) {
				record := records[key]
				record.State, record.Result = core.ToolInvocationStarted, nil
				records[key] = record
			},
		},
		{
			name: "uncertain",
			mutate: func(records map[string]core.ToolInvocationRecord, key string) {
				record := records[key]
				record.State, record.Result = core.ToolInvocationUncertain, nil
				records[key] = record
			},
		},
		{
			name: "conflicting result",
			mutate: func(records map[string]core.ToolInvocationRecord, key string) {
				record := records[key]
				result := *record.Result
				result.Content = "conflicting result"
				record.Result = &result
				records[key] = record
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			const runID = "observe-unavailable"
			fixture := newV2RuntimeFixture(t, v2RuntimeDirect, false, false)
			result, err := fixture.run(t, runID)
			if err != nil || result.Status != core.RunCompleted {
				t.Fatalf("run result=%#v err=%v", result, err)
			}
			mutateV2ChoiceRecord(t, fixture.journal, runID, "detail-18", test.mutate)
			observation := ObserveSelectedRoute(context.Background(), fixture.session, fixture.principal, fixture.journal, runID)
			if observation != (SelectedRouteObservation{Route: SelectedRouteUnavailable, Status: SelectionStatusUnavailable}) {
				t.Fatalf("observation=%+v", observation)
			}
		})
	}
}

func TestObserveSelectedRouteReturnsUnavailableForDuplicatedEventEvidence(t *testing.T) {
	const runID = "observe-duplicated-event"
	fixture := newV2RuntimeFixture(t, v2RuntimeDirect, false, false)
	result, err := fixture.run(t, runID)
	if err != nil || result.Status != core.RunCompleted {
		t.Fatalf("run result=%#v err=%v", result, err)
	}
	if _, err := fixture.session.Append(runID, core.EvToolResult, core.ToolResultData{CallID: "detail-18", OK: true, Content: "duplicate"}); err != nil {
		t.Fatal(err)
	}
	observation := ObserveSelectedRoute(context.Background(), fixture.session, fixture.principal, fixture.journal, runID)
	if observation != (SelectedRouteObservation{Route: SelectedRouteUnavailable, Status: SelectionStatusUnavailable}) {
		t.Fatalf("observation=%+v", observation)
	}
}

func appendObservedChoiceEvents(t *testing.T, fixture *probeResolverFixture, calls []core.ToolCall, results []core.CapabilityResult) {
	t.Helper()
	if len(calls) == 0 || len(calls) != len(results) {
		t.Fatal("choice event test fixture is invalid")
	}
	fixture.append(t, core.EvAssistantMessage, core.AssistantMessageData{ToolCalls: calls})
	for index, call := range calls {
		fixture.append(t, core.EvToolCall, core.ToolCallData{CallID: call.ID, Name: call.Name, Args: call.Args})
		fixture.append(t, core.EvToolResult, core.ToolResultData{CallID: call.ID, Content: results[index].Content, OK: results[index].OK, Metadata: results[index].Metadata})
	}
}

func storeObservedChoiceRecord(t *testing.T, fixture *probeResolverFixture, call core.ToolCall, result core.CapabilityResult) {
	t.Helper()
	invocation, err := core.NewToolInvocation(core.RunInfo{RunID: fixture.runID, SessionID: fixture.session.ID(), ProfileID: fixture.session.ProfileID(), Principal: fixture.principal}, call, fixture.followup.Manifest.Idempotent)
	if err != nil {
		t.Fatal(err)
	}
	resultCopy := result
	fixture.journal.records[invocation] = core.ToolInvocationRecord{ToolInvocation: invocation, State: core.ToolInvocationCompleted, Result: &resultCopy}
}
