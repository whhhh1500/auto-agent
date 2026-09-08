package storage

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

type nativeQueuedApprovalApprover struct {
	decision core.ApprovalDecision
}

func (a *nativeQueuedApprovalApprover) Approve(ctx context.Context, request core.ApprovalRequest) (core.ApprovalDecision, error) {
	resolution, err := a.RequestApproval(ctx, request)
	return resolution.Decision, err
}

func (a *nativeQueuedApprovalApprover) RequestApproval(context.Context, core.ApprovalRequest) (core.ApprovalResolution, error) {
	return core.ApprovalResolution{
		ApprovalID: "apr_0123456789abcdef0123456789abcdef",
		Decision:   a.decision,
		ExpiresAt:  time.Now().UTC().Add(time.Hour),
	}, nil
}

type nativeQueuedApprovalTool struct {
	manifest core.CapabilityManifest
	calls    atomic.Int32
}

func (t *nativeQueuedApprovalTool) Manifest() core.CapabilityManifest { return t.manifest }
func (t *nativeQueuedApprovalTool) Execute(context.Context, core.CapabilityRequest) (core.CapabilityResult, error) {
	t.calls.Add(1)
	return core.CapabilityResult{Content: `{"approved":true}`, OK: true}, nil
}

type nativeQueuedApprovalModel struct {
	call core.ToolCall
}

func (nativeQueuedApprovalModel) Provider() string { return "approval-model" }
func (m nativeQueuedApprovalModel) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	if options.Messages[len(options.Messages)-1].Role == core.RoleTool {
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: "approval handled"})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop, Usage: &core.TokenUsage{InputTokens: 3, OutputTokens: 1}})
		return nil
	}
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &m.call})
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls, Usage: &core.TokenUsage{InputTokens: 7, OutputTokens: 2}})
	return nil
}

type nativeQueuedApprovalHistory struct {
	options   core.SessionOptions
	principal core.Principal
	runID     string
	events    []core.SessionEvent
}

// nativeQueuedApprovalHistoryFromCore uses Runtime.RunTurn and ResumeTurn to
// produce the approval events and stops at Core's next step/start frontier,
// where a queued worker would admit its next model call.
func nativeQueuedApprovalHistoryFromCore(t *testing.T) nativeQueuedApprovalHistory {
	t.Helper()
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	call := core.ToolCall{ID: "call-approval-model", Name: "payment.release", Args: map[string]any{"amount": 10}}
	tool := &nativeQueuedApprovalTool{manifest: core.CapabilityManifest{
		ID: call.Name, Version: "1.0.0", Name: "Payment release", Kind: core.KindTool,
		Contract: "harness.tool/v1", RequiredPermissions: []core.Permission{core.PermRead}, RequiresApproval: true,
		Tool: &core.ToolExposure{Parameters: map[string]any{"type": "object"}},
	}}
	capabilities := core.NewCapabilityRegistry()
	if err := capabilities.Register(product, tool); err != nil {
		t.Fatal(err)
	}
	profiles := core.NewAgentProfileRegistry()
	name := "Native queued approval"
	selection := core.ModelSelection{Provider: "approval-model", Model: "approval-model-v1"}
	if err := profiles.Bind(core.AgentProfileLayer{
		Scope: product, ProfileID: "approval.agent", Name: &name, Model: &selection, AddCapabilities: []string{call.Name},
	}); err != nil {
		t.Fatal(err)
	}
	approver := &nativeQueuedApprovalApprover{decision: core.ApprovalPending}
	runtime := &core.Runtime{
		Capabilities: capabilities, Profiles: profiles, Approver: approver,
		Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) {
			return nativeQueuedApprovalModel{call: call}, nil
		}),
	}
	const sessionID, runID = "session-native-queued-approval", "run-native-queued-approval"
	scope, err := user.Child(core.ScopeRef{Kind: core.ScopeSession, ID: sessionID})
	if err != nil {
		t.Fatal(err)
	}
	options := core.SessionOptions{ID: sessionID, ProfileID: "approval.agent", Principal: principal, Scope: scope}
	session, err := core.NewSession(options)
	if err != nil {
		t.Fatal(err)
	}
	first, err := runtime.RunTurn(context.Background(), principal, session, core.TurnInput{
		RunID: runID, Text: "release payment", CompositionMetadata: map[string]string{"segment": "initial"},
	}, nil)
	if err != nil || first.Status != core.RunWaitingApproval || tool.calls.Load() != 0 {
		t.Fatalf("initial result=%+v tool calls=%d err=%v", first, tool.calls.Load(), err)
	}
	approver.decision = core.ApprovalApproved
	resumed, err := runtime.ResumeTurn(context.Background(), principal, session, core.ResumeInput{
		RunID: runID, CompositionMetadata: map[string]string{"segment": "resume"},
	}, nil)
	if err != nil || resumed.Status != core.RunCompleted || tool.calls.Load() != 1 {
		t.Fatalf("resume result=%+v tool calls=%d err=%v", resumed, tool.calls.Load(), err)
	}
	var prefix []core.SessionEvent
	for _, event := range session.Events() {
		if event.Type == core.EvStepStart {
			var step core.StepData
			if err := json.Unmarshal(event.Data, &step); err != nil {
				t.Fatal(err)
			}
			if step.Index == 1 {
				prefix = append(prefix, event)
				break
			}
		}
		if event.Type == core.EvRunEnd {
			break
		}
		prefix = append(prefix, event)
	}
	frontier, err := core.RestoreSession(options, prefix)
	if err != nil {
		t.Fatal(err)
	}
	return nativeQueuedApprovalHistory{options: options, principal: principal, runID: runID, events: frontier.Events()}
}

func nativeQueuedApprovalReindex(events []core.SessionEvent) []core.SessionEvent {
	copyEvents := append([]core.SessionEvent(nil), events...)
	for index := range copyEvents {
		copyEvents[index].Seq = int64(index)
	}
	return copyEvents
}

func nativeQueuedApprovalRewrite(t *testing.T, events []core.SessionEvent, kind core.SessionEventType, occurrence int, value any) []core.SessionEvent {
	t.Helper()
	copyEvents := append([]core.SessionEvent(nil), events...)
	for index := range copyEvents {
		if copyEvents[index].Type != kind {
			continue
		}
		if occurrence > 0 {
			occurrence--
			continue
		}
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		copyEvents[index].Data = data
		return copyEvents
	}
	t.Fatalf("event %q occurrence %d not found", kind, occurrence)
	return nil
}

func nativeQueuedApprovalDecode[T any](t *testing.T, events []core.SessionEvent, kind core.SessionEventType) T {
	t.Helper()
	for _, event := range events {
		if event.Type == kind {
			var value T
			if err := json.Unmarshal(event.Data, &value); err != nil {
				t.Fatal(err)
			}
			return value
		}
	}
	t.Fatalf("event %q not found", kind)
	var zero T
	return zero
}

func nativeQueuedApprovalAdmissionFixture(t *testing.T, events []core.SessionEvent) (*nativeQueuedModelFixture, error) {
	t.Helper()
	history := nativeQueuedApprovalHistoryFromCore(t)
	session, err := core.RestoreSession(history.options, nativeQueuedApprovalReindex(events))
	if err != nil {
		return nil, err
	}
	store := newTestSQLStore(t)
	if err := store.Create(context.Background(), session); err != nil {
		return nil, err
	}
	queue, err := NewSQLRunControlStore(store.db, store.dialect)
	if err != nil {
		return nil, err
	}
	if err := queue.EnqueueRun(context.Background(), QueuedRun{RunRecord: RunRecord{
		RunID: history.runID, SessionID: session.ID(), TenantID: history.principal.TenantID, SubjectID: history.principal.SubjectID,
	}, Message: "release payment", MaxAttempts: 3}); err != nil {
		return nil, err
	}
	claim, ok, err := queue.ClaimRun(context.Background(), "worker-native-approval", time.Hour)
	if err != nil || !ok {
		return nil, err
	}
	const leaseHolder = "lease-native-approval"
	if acquired, err := store.AcquireSessionLease(context.Background(), session.ID(), leaseHolder, time.Hour); err != nil || !acquired {
		return nil, err
	}
	epoch, err := store.AuthorizationEpoch(context.Background())
	if err != nil {
		return nil, err
	}
	fence := SessionWriteFence{SessionID: session.ID(), RunID: history.runID, TenantID: history.principal.TenantID, SubjectID: history.principal.SubjectID, WorkerID: claim.WorkerID, QueueGeneration: claim.Generation, LeaseHolder: leaseHolder}
	return &nativeQueuedModelFixture{fencedSQLFixture: &fencedSQLFixture{store: store, queue: queue, session: session, claim: claim, fence: fence}, version: session.Version(), input: NativeQueuedModelInvocationInput{
		Request:            core.ModelCallRequest{Principal: history.principal, Scope: session.Scope(), SessionID: session.ID(), RunID: history.runID, Step: 1, Provider: "approval-model", Model: "approval-model-v1"},
		AuthorizationEpoch: epoch, BootstrapRevision: "native-approval-v1",
	}}, nil
}

func TestSQLSessionStoreBeginNativeQueuedModelInvocationFencedAcceptsCoreApprovalResumeHistory(t *testing.T) {
	history := nativeQueuedApprovalHistoryFromCore(t)
	fixture, err := nativeQueuedApprovalAdmissionFixture(t, history.events)
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := fixture.store.BeginNativeQueuedModelInvocationFenced(context.Background(), fixture.fence, fixture.version, fixture.input)
	if err != nil || !admitted {
		t.Fatalf("admitted=%t err=%v", admitted, err)
	}
	var rows int
	if err := fixture.store.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM native_queued_model_invocations WHERE session_id = ? AND run_id = ? AND step_index = ?", fixture.session.ID(), fixture.fence.RunID, 1).Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("new model attempt rows=%d err=%v", rows, err)
	}
}

func TestSQLSessionStoreBeginNativeQueuedModelInvocationFencedRejectsInvalidCoreApprovalResumeHistory(t *testing.T) {
	history := nativeQueuedApprovalHistoryFromCore(t)
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, []core.SessionEvent) []core.SessionEvent
	}{
		{
			name: "approval_id", mutate: func(t *testing.T, events []core.SessionEvent) []core.SessionEvent {
				requested := nativeQueuedApprovalDecode[core.ApprovalRequestedData](t, events, core.EvApprovalRequested)
				requested.ApprovalID = "apr_ffffffffffffffffffffffffffffffff"
				return nativeQueuedApprovalRewrite(t, events, core.EvApprovalRequested, 0, requested)
			},
		},
		{
			name: "call_id", mutate: func(t *testing.T, events []core.SessionEvent) []core.SessionEvent {
				requested := nativeQueuedApprovalDecode[core.ApprovalRequestedData](t, events, core.EvApprovalRequested)
				requested.ResumeCall.ID = "call-replaced"
				return nativeQueuedApprovalRewrite(t, events, core.EvApprovalRequested, 0, requested)
			},
		},
		{
			name: "call_name", mutate: func(t *testing.T, events []core.SessionEvent) []core.SessionEvent {
				requested := nativeQueuedApprovalDecode[core.ApprovalRequestedData](t, events, core.EvApprovalRequested)
				requested.ResumeCall.Name = "payment.replaced"
				return nativeQueuedApprovalRewrite(t, events, core.EvApprovalRequested, 0, requested)
			},
		},
		{
			name: "call_args", mutate: func(t *testing.T, events []core.SessionEvent) []core.SessionEvent {
				requested := nativeQueuedApprovalDecode[core.ApprovalRequestedData](t, events, core.EvApprovalRequested)
				requested.ResumeCall.Args = map[string]any{"amount": 11}
				return nativeQueuedApprovalRewrite(t, events, core.EvApprovalRequested, 0, requested)
			},
		},
		{
			name: "remaining_calls", mutate: func(t *testing.T, events []core.SessionEvent) []core.SessionEvent {
				requested := nativeQueuedApprovalDecode[core.ApprovalRequestedData](t, events, core.EvApprovalRequested)
				requested.RemainingCalls = []core.ToolCall{{ID: "call-injected", Name: "payment.release", Args: map[string]any{"amount": 12}}}
				return nativeQueuedApprovalRewrite(t, events, core.EvApprovalRequested, 0, requested)
			},
		},
		{
			name: "resume_composition", mutate: func(t *testing.T, events []core.SessionEvent) []core.SessionEvent {
				resume := nativeQueuedApprovalDecode[core.RunResumeData](t, events, core.EvRunResume)
				resume.Composition.Model.Model = "tampered-model"
				var err error
				resume.CompositionRevision, err = core.CompositionRevision(resume.Composition)
				if err != nil {
					t.Fatal(err)
				}
				resume.AssignmentRevision, err = core.CompositionMetadataRevision(resume.Composition.Metadata)
				if err != nil {
					t.Fatal(err)
				}
				return nativeQueuedApprovalRewrite(t, events, core.EvRunResume, 0, resume)
			},
		},
		{
			name: "decision", mutate: func(t *testing.T, events []core.SessionEvent) []core.SessionEvent {
				resolved := nativeQueuedApprovalDecode[core.ApprovalResolvedData](t, events, core.EvApprovalResolved)
				resolved.Decision = core.ApprovalDenied
				return nativeQueuedApprovalRewrite(t, events, core.EvApprovalResolved, 0, resolved)
			},
		},
		{
			name: "order", mutate: func(_ *testing.T, events []core.SessionEvent) []core.SessionEvent {
				copyEvents := append([]core.SessionEvent(nil), events...)
				var resumeIndex, resultIndex int
				for index, event := range copyEvents {
					if event.Type == core.EvRunResume {
						resumeIndex = index
					}
					if event.Type == core.EvToolResult {
						resultIndex = index
					}
				}
				copyEvents[resumeIndex], copyEvents[resultIndex] = copyEvents[resultIndex], copyEvents[resumeIndex]
				return nativeQueuedApprovalReindex(copyEvents)
			},
		},
		{
			name: "duplicate_resolution", mutate: func(t *testing.T, events []core.SessionEvent) []core.SessionEvent {
				copyEvents := append([]core.SessionEvent(nil), events...)
				for index, event := range copyEvents {
					if event.Type == core.EvApprovalResolved {
						copyEvents = append(copyEvents[:index+1], append([]core.SessionEvent{event}, copyEvents[index+1:]...)...)
						return nativeQueuedApprovalReindex(copyEvents)
					}
				}
				t.Fatal("approval resolution not found")
				return nil
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture, err := nativeQueuedApprovalAdmissionFixture(t, test.mutate(t, history.events))
			if err != nil {
				t.Fatal(err)
			}
			admitted, err := fixture.store.BeginNativeQueuedModelInvocationFenced(context.Background(), fixture.fence, fixture.version, fixture.input)
			if admitted || !errors.Is(err, ErrCompletedToolResultProofInvalid) {
				t.Fatalf("admitted=%t err=%v", admitted, err)
			}
		})
	}
}
