package core

import (
	"context"
	"encoding/json"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type continuationTool struct {
	manifest CapabilityManifest
	calls    *atomic.Int32
}

func (t continuationTool) Manifest() CapabilityManifest { return t.manifest }
func (t continuationTool) Execute(context.Context, CapabilityRequest) (CapabilityResult, error) {
	t.calls.Add(1)
	return CapabilityResult{Content: `{"ok":true}`, OK: true}, nil
}

type continuationModel struct{ calls *atomic.Int32 }

func (continuationModel) Provider() string { return "continuation-test" }
func (m continuationModel) Stream(_ context.Context, _ GenerateOptions, emit func(StreamChunk)) error {
	m.calls.Add(1)
	emit(StreamChunk{Kind: StreamKindAssistant, Text: "continued"})
	emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishStop, Usage: &TokenUsage{InputTokens: 2, OutputTokens: 1}})
	return nil
}

type continuationRate struct{ calls atomic.Int32 }

func (r *continuationRate) AllowCall(context.Context, string, string) bool {
	r.calls.Add(1)
	return true
}

type continuationJournal struct{ begins atomic.Int32 }

func (j *continuationJournal) BeginToolInvocation(context.Context, ToolInvocation) (ToolInvocationRecord, ToolInvocationDecision, error) {
	j.begins.Add(1)
	return ToolInvocationRecord{}, ToolInvocationExecuteNew, nil
}
func (*continuationJournal) CompleteToolInvocation(context.Context, ToolInvocation, CapabilityResult) (ToolInvocationRecord, error) {
	return ToolInvocationRecord{}, nil
}
func (*continuationJournal) MarkToolInvocationUncertain(context.Context, ToolInvocation, string) error {
	return nil
}

func continuationFixture(t *testing.T, requiresApproval bool) (*Runtime, Principal, *Session, []ToolCall, *atomic.Int32, *atomic.Int32, *atomic.Int32) {
	t.Helper()
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	calls := []ToolCall{{ID: "continued-one", Name: "continuation.one", Args: map[string]any{"n": 1}}, {ID: "continued-two", Name: "continuation.two", Args: map[string]any{"n": 2}}}
	first, second, modelCalls := &atomic.Int32{}, &atomic.Int32{}, &atomic.Int32{}
	registry := NewCapabilityRegistry()
	for index, counter := range []*atomic.Int32{first, second} {
		manifest := toolManifest(calls[index].Name, "1.0.0")
		if index == 1 {
			manifest.RequiresApproval = requiresApproval
		}
		if err := registry.Register(product, continuationTool{manifest: manifest, calls: counter}); err != nil {
			t.Fatal(err)
		}
	}
	profiles := NewAgentProfileRegistry()
	name := "Continuation"
	model := ModelSelection{Provider: "continuation-test", Model: "test"}
	if err := profiles.Bind(AgentProfileLayer{Scope: product, ProfileID: "continuation.agent", Name: &name, Model: &model, AddCapabilities: []string{calls[0].Name, calls[1].Name}}); err != nil {
		t.Fatal(err)
	}
	scope, err := user.Child(ScopeRef{Kind: ScopeSession, ID: "continuation-session"})
	if err != nil {
		t.Fatal(err)
	}
	session, err := NewSession(SessionOptions{ID: "continuation-session", ProfileID: "continuation.agent", Principal: principal, Scope: scope})
	if err != nil {
		t.Fatal(err)
	}
	return &Runtime{Capabilities: registry, Profiles: profiles, Models: ModelResolverFunc(func(context.Context, ModelSelection) (LlmAdapter, error) {
		return continuationModel{calls: modelCalls}, nil
	})}, principal, session, calls, first, second, modelCalls
}

func appendPostResultTail(t *testing.T, session *Session, runID string, calls []ToolCall, completed int) {
	t.Helper()
	appendEvent := func(kind SessionEventType, data any) SessionEvent {
		event, err := session.Append(runID, kind, data)
		if err != nil {
			t.Fatal(err)
		}
		return event
	}
	appendEvent(EvRunStart, RunStartData{})
	appendEvent(EvUserMessage, UserMessageData{Text: "continue"})
	step := appendEvent(EvStepStart, StepData{Index: 0})
	appendEvent(EvAssistantMessage, AssistantMessageData{Text: "tools", ToolCall: firstCall(calls), ToolCalls: calls})
	appendEvent(EvRunUsage, RunUsageData{InvocationID: "model:" + strconv.FormatInt(step.Seq, 10)})
	for _, call := range calls[:completed] {
		appendEvent(EvToolCall, ToolCallData{CallID: call.ID, Name: call.Name, Args: call.Args})
		appendEvent(EvToolResult, ToolResultData{CallID: call.ID, Content: `{"ok":true}`, OK: true})
	}
}

func TestRuntimeContinueTurnDoesNotReplayCompletedCall(t *testing.T) {
	runtime, principal, session, calls, first, _, modelCalls := continuationFixture(t, false)
	appendPostResultTail(t, session, "run-post-result", calls[:1], 1)
	rate, journal, before, after := &continuationRate{}, &continuationJournal{}, atomic.Int32{}, atomic.Int32{}
	runtime.RateLimiter, runtime.ToolJournal = rate, journal
	runtime.Hooks = &RunHooksFuncs{OnBeforeToolFn: func(context.Context, RunInfo, ToolCall) error { before.Add(1); return nil }, OnAfterToolFn: func(context.Context, RunInfo, ToolCall, CapabilityResult) { after.Add(1) }}
	metadata := map[string]string{"request": "original"}
	result, err := runtime.ContinueTurn(context.Background(), principal, session, ResumeInput{RunID: "run-post-result", CompositionMetadata: metadata}, nil)
	if err != nil || result.Status != RunCompleted || result.Answer != "continued" {
		t.Fatalf("continue result=%+v err=%v", result, err)
	}
	if first.Load() != 0 || before.Load() != 0 || after.Load() != 0 || rate.calls.Load() != 0 || journal.begins.Load() != 0 || modelCalls.Load() != 1 {
		t.Fatalf("completed call was replayed: provider=%d before=%d after=%d rate=%d journal=%d model=%d", first.Load(), before.Load(), after.Load(), rate.calls.Load(), journal.begins.Load(), modelCalls.Load())
	}
	if metadata["request"] != "original" {
		t.Fatal("continuation mutated composition metadata")
	}
	for _, event := range session.Events() {
		if event.Type == EvRunResume {
			t.Fatal("continuation appended run/resume")
		}
	}
}

func TestRuntimeContinueTurnExecutesRemainingCallAndApproval(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		runtime, principal, session, calls, first, second, _ := continuationFixture(t, false)
		appendPostResultTail(t, session, "run-remaining", calls, 1)
		result, err := runtime.ContinueTurn(context.Background(), principal, session, ResumeInput{RunID: "run-remaining"}, nil)
		if err != nil || result.Status != RunCompleted || first.Load() != 0 || second.Load() != 1 {
			t.Fatalf("result=%+v first=%d second=%d err=%v", result, first.Load(), second.Load(), err)
		}
	})
	t.Run("approval", func(t *testing.T) {
		runtime, principal, session, calls, first, second, _ := continuationFixture(t, true)
		approver := newDurableApprovalStub()
		runtime.Approver = approver
		appendPostResultTail(t, session, "run-approval-continue", calls, 1)
		result, err := runtime.ContinueTurn(context.Background(), principal, session, ResumeInput{RunID: "run-approval-continue"}, nil)
		if err != nil || result.Status != RunWaitingApproval || first.Load() != 0 || second.Load() != 0 {
			t.Fatalf("pause result=%+v first=%d second=%d err=%v", result, first.Load(), second.Load(), err)
		}
		approver.decide(ApprovalApproved)
		result, err = runtime.ResumeTurn(context.Background(), principal, session, ResumeInput{RunID: "run-approval-continue"}, nil)
		if err != nil || result.Status != RunCompleted || second.Load() != 1 {
			t.Fatalf("resume result=%+v second=%d err=%v", result, second.Load(), err)
		}
	})
}

func TestApprovalResumeLimitKeepsHeadAfterStepBehavior(t *testing.T) {
	runtime, principal, session, calls, first, second, _ := continuationFixture(t, false)
	append := func(kind SessionEventType, data any) {
		if _, err := session.Append("run-approval-limit", kind, data); err != nil {
			t.Fatal(err)
		}
	}
	append(EvRunStart, RunStartData{})
	append(EvUserMessage, UserMessageData{Text: "continue"})
	append(EvStepStart, StepData{Index: 0})
	append(EvToolCall, ToolCallData{CallID: calls[0].ID, Name: calls[0].Name, Args: calls[0].Args})
	append(EvApprovalRequested, ApprovalRequestedData{ApprovalID: "apr_0123456789abcdef0123456789abcdef", ToolCall: calls[0], ResumeCall: calls[0], RemainingCalls: []ToolCall{calls[1]}, Step: 0})
	limit := 1
	policy := NewPolicyRegistry()
	if err := policy.Bind(PolicyLayer{Scope: session.Scope(), MaxToolCalls: &limit}); err != nil {
		t.Fatal(err)
	}
	var after atomic.Int32
	runtime.Policy = policy
	runtime.Hooks = &RunHooksFuncs{OnAfterStepFn: func(context.Context, RunInfo) { after.Add(1) }}
	result, err := runtime.ResumeTurn(context.Background(), principal, session, ResumeInput{RunID: "run-approval-limit"}, nil)
	if err != nil || result.Status != RunLimited || first.Load() != 1 || second.Load() != 0 || after.Load() != 0 {
		t.Fatalf("result=%+v first=%d second=%d after=%d err=%v", result, first.Load(), second.Load(), after.Load(), err)
	}
}

func TestRuntimeContinueTurnHonorsCurrentCapabilitiesAndLimits(t *testing.T) {
	t.Run("revoked", func(t *testing.T) {
		runtime, principal, session, calls, first, _, modelCalls := continuationFixture(t, false)
		appendPostResultTail(t, session, "run-revoked", calls[:1], 1)
		principal.Grants = NewPermissionSet()
		if _, err := runtime.ContinueTurn(context.Background(), principal, session, ResumeInput{RunID: "run-revoked"}, nil); err == nil {
			t.Fatal("revoked capability continued")
		}
		if first.Load() != 0 || modelCalls.Load() != 0 {
			t.Fatal("revoked continuation performed work")
		}
	})
	t.Run("tool_limit", func(t *testing.T) {
		runtime, principal, session, calls, first, second, modelCalls := continuationFixture(t, false)
		appendPostResultTail(t, session, "run-tool-limit", calls, 1)
		limit := 1
		policy := NewPolicyRegistry()
		if err := policy.Bind(PolicyLayer{Scope: session.Scope(), MaxToolCalls: &limit}); err != nil {
			t.Fatal(err)
		}
		runtime.Policy = policy
		result, err := runtime.ContinueTurn(context.Background(), principal, session, ResumeInput{RunID: "run-tool-limit"}, nil)
		if err != nil || result.Status != RunLimited || first.Load() != 0 || second.Load() != 0 || modelCalls.Load() != 0 {
			t.Fatalf("tool limit result=%+v first=%d second=%d model=%d err=%v", result, first.Load(), second.Load(), modelCalls.Load(), err)
		}
	})
	t.Run("step_limit", func(t *testing.T) {
		runtime, principal, session, calls, first, _, modelCalls := continuationFixture(t, false)
		appendPostResultTail(t, session, "run-step-limit", calls[:1], 1)
		limit := 1
		policy := NewPolicyRegistry()
		if err := policy.Bind(PolicyLayer{Scope: session.Scope(), MaxSteps: &limit}); err != nil {
			t.Fatal(err)
		}
		runtime.Policy = policy
		result, err := runtime.ContinueTurn(context.Background(), principal, session, ResumeInput{RunID: "run-step-limit"}, nil)
		if err != nil || result.Status != RunLimited || first.Load() != 0 || modelCalls.Load() != 0 {
			t.Fatalf("step limit result=%+v provider=%d model=%d err=%v", result, first.Load(), modelCalls.Load(), err)
		}
	})
	t.Run("model_gate", func(t *testing.T) {
		runtime, principal, session, calls, first, _, models := continuationFixture(t, false)
		appendPostResultTail(t, session, "run-model-gate", calls[:1], 1)
		runtime.ModelCallGate = modelGateFunc(func(context.Context, ModelCallRequest) error { return context.Canceled })
		result, err := runtime.ContinueTurn(context.Background(), principal, session, ResumeInput{RunID: "run-model-gate"}, nil)
		if err == nil || result.Status != RunCancelled || first.Load() != 0 || models.Load() != 0 {
			t.Fatalf("result=%+v provider=%d models=%d err=%v", result, first.Load(), models.Load(), err)
		}
	})
}

func TestPostResultContinuationRejectsAmbiguousTails(t *testing.T) {
	_, _, session, calls, _, _, _ := continuationFixture(t, false)
	appendPostResultTail(t, session, "run-tail", calls, 1)
	tests := []struct {
		name   string
		mutate func([]SessionEvent) []SessionEvent
		want   string
	}{
		{"usage", func(events []SessionEvent) []SessionEvent {
			var data RunUsageData
			_ = json.Unmarshal(events[4].Data, &data)
			data.InvocationID = "model:99"
			events[4].Data, _ = json.Marshal(data)
			return events
		}, "matching model usage"},
		{"nested", func(events []SessionEvent) []SessionEvent {
			raw, _ := json.Marshal(ToolCallData{CallID: "nested", Name: calls[0].Name, Args: map[string]any{}})
			return append(events, SessionEvent{RunID: "run-tail", Type: EvToolCall, Data: raw})
		}, "tool call sequence"},
		{"duplicate-result", func(events []SessionEvent) []SessionEvent { return append(events, events[len(events)-1]) }, "tool result sequence"},
		{"duplicate-assistant-call", func(events []SessionEvent) []SessionEvent {
			var assistant AssistantMessageData
			_ = json.Unmarshal(events[3].Data, &assistant)
			assistant.ToolCalls = []ToolCall{calls[0], calls[0]}
			events[3].Data = continuationJSON(t, assistant)
			return events
		}, "malformed continuation event"},
		{"approval-requested", func(events []SessionEvent) []SessionEvent {
			return append(events, SessionEvent{RunID: "run-tail", Type: EvApprovalRequested, Data: continuationJSON(t, ApprovalRequestedData{ApprovalID: "apr_0123456789abcdef0123456789abcdef", ToolCall: calls[1], ResumeCall: calls[1], Step: 0})})
		}, "unsupported continuation events"},
		{"approval-resolved", func(events []SessionEvent) []SessionEvent {
			return append(events, SessionEvent{RunID: "run-tail", Type: EvApprovalResolved, Data: continuationJSON(t, ApprovalResolvedData{ApprovalID: "apr_0123456789abcdef0123456789abcdef", CallID: calls[0].ID, Decision: ApprovalApproved, ResolvedAt: time.Unix(1, 0).UTC()})})
		}, "unsupported continuation events"},
		{"malformed-event", func(events []SessionEvent) []SessionEvent { events[1].Data = json.RawMessage("{"); return events }, "malformed continuation event"},
		{"resume", func(events []SessionEvent) []SessionEvent {
			raw, _ := json.Marshal(RunResumeData{})
			return append(events, SessionEvent{RunID: "run-tail", Type: EvRunResume, Data: raw})
		}, "unsupported"},
		{"model-unknown", func(events []SessionEvent) []SessionEvent {
			var assistant AssistantMessageData
			_ = json.Unmarshal(events[3].Data, &assistant)
			assistant.ToolCall, assistant.ToolCalls = firstCall(calls[:1]), calls[:1]
			events[3].Data, _ = json.Marshal(assistant)
			raw, _ := json.Marshal(StepData{Index: 1})
			events = append(events, SessionEvent{RunID: "run-tail", Type: EvStepEnd, Data: continuationJSON(t, StepData{Index: 0})}, SessionEvent{RunID: "run-tail", Type: EvStepStart, Data: raw})
			return events
		}, "model_outcome_unknown"},
		{"fast-router", func([]SessionEvent) []SessionEvent {
			return []SessionEvent{{RunID: "run-tail", Type: EvRunStart, Data: continuationJSON(t, RunStartData{})}, {RunID: "run-tail", Type: EvUserMessage, Data: continuationJSON(t, UserMessageData{Text: "x"})}, {RunID: "run-tail", Type: EvToolCall, Data: continuationJSON(t, ToolCallData{CallID: calls[0].ID, Name: calls[0].Name, Args: calls[0].Args})}, {RunID: "run-tail", Type: EvToolResult, Data: continuationJSON(t, ToolResultData{CallID: calls[0].ID, Content: "ok", OK: true})}}
		}, "tool call sequence"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, _, _, err := postResultContinuation(test.mutate(session.Events()), "run-tail"); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("err=%v want %q", err, test.want)
			}
		})
	}
}

func TestRuntimeContinueTurnContainsCompositionPanic(t *testing.T) {
	runtime, principal, session, calls, _, _, _ := continuationFixture(t, false)
	appendPostResultTail(t, session, "run-panic", calls[:1], 1)
	before := session.Version()
	runtime.Models = ModelResolverFunc(func(context.Context, ModelSelection) (LlmAdapter, error) { panic("resolver") })
	if _, err := runtime.ContinueTurn(context.Background(), principal, session, ResumeInput{RunID: "run-panic"}, nil); err == nil || !strings.Contains(err.Error(), "model_resolution_failed") {
		t.Fatalf("err=%v", err)
	}
	if session.Version() != before {
		t.Fatal("composition panic appended continuation events")
	}
}

func TestRuntimeContinueTurnRejectsTailBeforeResolution(t *testing.T) {
	for _, test := range []struct {
		name   string
		append func(*Session, string) error
	}{
		{"resume", func(session *Session, runID string) error {
			_, err := session.Append(runID, EvRunResume, RunResumeData{})
			return err
		}},
		{"step-end", func(session *Session, runID string) error {
			_, err := session.Append(runID, EvStepEnd, StepData{Index: 0})
			return err
		}},
		{"next-step", func(session *Session, runID string) error {
			if _, err := session.Append(runID, EvStepEnd, StepData{Index: 0}); err != nil {
				return err
			}
			_, err := session.Append(runID, EvStepStart, StepData{Index: 1})
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime, principal, session, calls, _, _, _ := continuationFixture(t, false)
			runID := "run-reject-" + test.name
			appendPostResultTail(t, session, runID, calls[:1], 1)
			if err := test.append(session, runID); err != nil {
				t.Fatal(err)
			}
			var resolves atomic.Int32
			runtime.Models = ModelResolverFunc(func(context.Context, ModelSelection) (LlmAdapter, error) { resolves.Add(1); panic("must not resolve") })
			before := session.Version()
			if _, err := runtime.ContinueTurn(context.Background(), principal, session, ResumeInput{RunID: runID}, nil); err == nil {
				t.Fatal("continuation succeeded")
			}
			if resolves.Load() != 0 || session.Version() != before {
				t.Fatalf("resolver=%d version=%d want %d", resolves.Load(), session.Version(), before)
			}
		})
	}
}

func TestRuntimeContinueTurnDoesNotRepeatTerminalContinuation(t *testing.T) {
	runtime, principal, session, calls, first, _, models := continuationFixture(t, false)
	appendPostResultTail(t, session, "run-repeat", calls[:1], 1)
	if _, err := runtime.ContinueTurn(context.Background(), principal, session, ResumeInput{RunID: "run-repeat"}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.ContinueTurn(context.Background(), principal, session, ResumeInput{RunID: "run-repeat"}, nil); err == nil {
		t.Fatal("repeated continuation succeeded")
	}
	if first.Load() != 0 || models.Load() != 1 {
		t.Fatalf("provider=%d models=%d", first.Load(), models.Load())
	}
}

func TestRunTurnCompositionFailureKeepsEmptyUserMessage(t *testing.T) {
	runtime, principal, existing, _, _, _, _ := continuationFixture(t, false)
	segments := existing.Scope().Segments()
	scope, err := NewScopePath(segments[:len(segments)-1]...)
	if err == nil {
		scope, err = scope.Child(ScopeRef{Kind: ScopeSession, ID: "empty-composition"})
	}
	if err != nil {
		t.Fatal(err)
	}
	session, err := NewSession(SessionOptions{ID: "empty-composition", ProfileID: "missing", Principal: principal, Scope: scope})
	if err != nil {
		t.Fatal(err)
	}
	result, err := runtime.RunTurn(context.Background(), principal, session, TurnInput{RunID: "run-empty"}, nil)
	if err == nil || result.Status != RunFailed {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	events := session.Events()
	if len(events) != 4 || events[0].Type != EvRunStart || events[1].Type != EvUserMessage || events[2].Type != EvRunError || events[3].Type != EvRunEnd {
		t.Fatalf("unexpected events=%+v", events)
	}
	var user UserMessageData
	if err := json.Unmarshal(events[1].Data, &user); err != nil || user.Text != "" {
		t.Fatalf("user=%+v err=%v", user, err)
	}
}

func TestRepairInterruptedGoldenLegacySequences(t *testing.T) {
	call := ToolCall{ID: "repair-call", Name: "repair.tool", Args: map[string]any{"n": 1}}
	result := func(id, code string) ToolResultData {
		return ToolResultData{CallID: id, Content: "tool outcome lost: the run was interrupted before " + id + " completed", Metadata: map[string]any{"code": code, "repaired": true}}
	}
	end := func(id string) []SessionEvent {
		return []SessionEvent{{RunID: id, Type: EvRunError, Data: continuationJSON(t, RuntimeErrorData{Code: CodeRunInterrupted, Message: "run " + id + " was interrupted before reaching a terminal state; synthetic closers were appended on load", Retryable: true})}, {RunID: id, Type: EvRunEnd, Data: continuationJSON(t, RunEndData{Status: RunFailed})}}
	}
	closers := func(id string, code string, step bool) []SessionEvent {
		out := []SessionEvent{{RunID: id, Type: EvToolResult, Data: continuationJSON(t, result(call.ID, code))}}
		if step {
			out = append(out, SessionEvent{RunID: id, Type: EvStepEnd, Data: continuationJSON(t, StepData{Index: -1})})
		}
		return append(out, SessionEvent{RunID: id, Type: EvRunError, Data: continuationJSON(t, RuntimeErrorData{Code: CodeRunInterrupted, Message: "run " + id + " was interrupted before reaching a terminal state; synthetic closers were appended on load", Retryable: true})}, SessionEvent{RunID: id, Type: EvRunEnd, Data: continuationJSON(t, RunEndData{Status: RunFailed})})
	}
	cases := []struct {
		name   string
		events []SessionEvent
		want   []SessionEvent
	}{
		{"balanced", []SessionEvent{{RunID: "balanced", Type: EvRunStart}, {RunID: "balanced", Type: EvRunEnd}}, nil},
		{"open", []SessionEvent{{RunID: "open", Type: EvRunStart}}, end("open")},
		{"not-started", []SessionEvent{{RunID: "not-started", Type: EvRunStart}, {RunID: "not-started", Type: EvStepStart}, {RunID: "not-started", Type: EvAssistantMessage, Data: continuationJSON(t, AssistantMessageData{ToolCalls: []ToolCall{call}})}}, closers("not-started", CodeToolNotStarted, true)},
		{"inflight", []SessionEvent{{RunID: "inflight", Type: EvRunStart}, {RunID: "inflight", Type: EvStepStart}, {RunID: "inflight", Type: EvAssistantMessage, Data: continuationJSON(t, AssistantMessageData{ToolCalls: []ToolCall{call}})}, {RunID: "inflight", Type: EvToolCall, Data: continuationJSON(t, ToolCallData{CallID: call.ID, Name: call.Name, Args: call.Args})}}, closers("inflight", CodeToolOutcomeUnknown, true)},
		{"approval", []SessionEvent{{RunID: "approval", Type: EvRunStart}, {RunID: "approval", Type: EvStepStart}, {RunID: "approval", Type: EvApprovalRequested, Data: continuationJSON(t, ApprovalRequestedData{ApprovalID: "approval-id"})}}, nil},
		{"latest-open", []SessionEvent{{RunID: "older", Type: EvRunStart}, {RunID: "older", Type: EvStepStart}, {RunID: "older", Type: EvAssistantMessage, Data: continuationJSON(t, AssistantMessageData{ToolCalls: []ToolCall{call}})}, {RunID: "latest", Type: EvRunStart}, {RunID: "latest", Type: EvStepStart}, {RunID: "latest", Type: EvToolCall, Data: continuationJSON(t, ToolCallData{CallID: call.ID, Name: call.Name, Args: call.Args})}}, closers("latest", CodeToolOutcomeUnknown, true)},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if got := RepairInterrupted(test.events); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("repair mismatch\ngot:  %#v\nwant: %#v", got, test.want)
			}
		})
	}
}

func continuationJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
