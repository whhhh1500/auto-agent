package core

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type durableApprovalStub struct {
	mu       sync.Mutex
	decision ApprovalDecision
	id       string
	expires  time.Time
}

func newDurableApprovalStub() *durableApprovalStub {
	return &durableApprovalStub{
		decision: ApprovalPending,
		id:       "apr_0123456789abcdef0123456789abcdef",
		expires:  time.Now().UTC().Add(time.Hour),
	}
}

func (s *durableApprovalStub) Approve(ctx context.Context, request ApprovalRequest) (ApprovalDecision, error) {
	resolution, err := s.RequestApproval(ctx, request)
	return resolution.Decision, err
}

func (s *durableApprovalStub) RequestApproval(context.Context, ApprovalRequest) (ApprovalResolution, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return ApprovalResolution{ApprovalID: s.id, Decision: s.decision, ExpiresAt: s.expires}, nil
}

func (s *durableApprovalStub) decide(decision ApprovalDecision) {
	s.mu.Lock()
	s.decision = decision
	s.mu.Unlock()
}

type approvalProbeTool struct {
	manifest CapabilityManifest
	calls    *atomic.Int32
}

func (p approvalProbeTool) Manifest() CapabilityManifest { return p.manifest }
func (p approvalProbeTool) Execute(context.Context, CapabilityRequest) (CapabilityResult, error) {
	p.calls.Add(1)
	return CapabilityResult{Content: `{"approved":true}`, OK: true}, nil
}

type approvalModel struct{ call ToolCall }

func (approvalModel) Provider() string { return "approval-test" }
func (m approvalModel) Stream(_ context.Context, options GenerateOptions, emit func(StreamChunk)) error {
	last := options.Messages[len(options.Messages)-1]
	if last.Role == RoleTool {
		emit(StreamChunk{Kind: StreamKindAssistant, Text: "approval handled"})
		emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishStop, Usage: &TokenUsage{InputTokens: 3, OutputTokens: 1}})
		return nil
	}
	call := m.call
	emit(StreamChunk{Kind: StreamKindAssistant, ToolCall: &call})
	emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishToolCalls, Usage: &TokenUsage{InputTokens: 7, OutputTokens: 2}})
	return nil
}

func durableApprovalFixture(t *testing.T) (*Runtime, Principal, *Session, *durableApprovalStub, *atomic.Int32) {
	t.Helper()
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	call := ToolCall{ID: "call-approval", Name: "payment.release", Args: map[string]any{"amount": 10}}
	manifest := toolManifest(call.Name, "1.0.0")
	manifest.RequiresApproval = true
	manifest.OutputSchema = map[string]any{
		"type": "object", "properties": map[string]any{"approved": map[string]any{"type": "boolean"}},
		"required": []any{"approved"},
	}
	var calls atomic.Int32
	capabilities := NewCapabilityRegistry()
	if err := capabilities.Register(product, approvalProbeTool{manifest: manifest, calls: &calls}); err != nil {
		t.Fatal(err)
	}
	profiles := NewAgentProfileRegistry()
	name := "Approval Agent"
	model := ModelSelection{Provider: "approval-test", Model: "approval-test"}
	if err := profiles.Bind(AgentProfileLayer{
		Scope: product, ProfileID: "approval.agent", Name: &name, Model: &model,
		AddCapabilities: []string{call.Name},
	}); err != nil {
		t.Fatal(err)
	}
	approver := newDurableApprovalStub()
	runtime := &Runtime{
		Capabilities: capabilities, Profiles: profiles, Approver: approver,
		Models: ModelResolverFunc(func(context.Context, ModelSelection) (LlmAdapter, error) {
			return approvalModel{call: call}, nil
		}),
	}
	scope, err := user.Child(ScopeRef{Kind: ScopeSession, ID: "session-approval"})
	if err != nil {
		t.Fatal(err)
	}
	session, err := NewSession(SessionOptions{
		ID: "session-approval", ProfileID: "approval.agent", Principal: principal, Scope: scope,
	})
	if err != nil {
		t.Fatal(err)
	}
	return runtime, principal, session, approver, &calls
}

func TestDurableApprovalPausesWithoutTerminalAndResumesSameRun(t *testing.T) {
	runtime, principal, session, approver, providerCalls := durableApprovalFixture(t)
	result, err := runtime.RunTurn(context.Background(), principal, session, TurnInput{
		RunID: "run-approval", Text: "release payment",
		CompositionMetadata: map[string]string{"segment.assignment": "initial"},
	}, nil)
	if err != nil || result.Status != RunWaitingApproval {
		t.Fatalf("run did not pause: result=%#v err=%v", result, err)
	}
	if providerCalls.Load() != 0 {
		t.Fatal("provider executed before approval")
	}
	if status, exists := session.RunStatus("run-approval"); !exists || status != RunWaitingApproval {
		t.Fatalf("session status=%q exists=%t", status, exists)
	}
	if repair := RepairInterrupted(session.Events()); repair != nil {
		t.Fatalf("legitimate approval pause was treated as a crash: %#v", repair)
	}
	pending, ok, err := session.PendingApproval("run-approval")
	if err != nil || !ok || pending.ToolCall.ID != "call-approval" || pending.Step != 0 {
		t.Fatalf("pending approval checkpoint wrong: %#v ok=%t err=%v", pending, ok, err)
	}
	approver.decide(ApprovalApproved)
	resumeMetadata := map[string]string{"segment.assignment": "resume"}
	result, err = runtime.ResumeTurn(context.Background(), principal, session, ResumeInput{
		RunID: "run-approval", CompositionMetadata: resumeMetadata,
	}, nil)
	if err != nil || result.Status != RunCompleted || result.Answer != "approval handled" {
		t.Fatalf("resume failed: result=%#v err=%v", result, err)
	}
	if providerCalls.Load() != 1 {
		t.Fatalf("provider calls=%d, want 1", providerCalls.Load())
	}
	if status, exists := session.RunStatus("run-approval"); !exists || status != RunCompleted {
		t.Fatalf("terminal session status=%q exists=%t", status, exists)
	}
	if _, ok, err := session.PendingApproval("run-approval"); err != nil || ok {
		t.Fatalf("approval remained pending after resolution: ok=%t err=%v", ok, err)
	}
	resumeMetadata["segment.assignment"] = "mutated"
	var starts, resumes int
	usageIDs := map[string]bool{}
	for _, event := range session.Events() {
		switch event.Type {
		case EvRunStart:
			starts++
			var data RunStartData
			if err := json.Unmarshal(event.Data, &data); err != nil || data.Composition == nil || data.Composition.Metadata["segment.assignment"] != "initial" {
				t.Fatalf("initial composition metadata wrong: %#v err=%v", data, err)
			}
		case EvRunResume:
			resumes++
			var data RunResumeData
			if err := json.Unmarshal(event.Data, &data); err != nil || data.Composition == nil || data.Composition.Metadata["segment.assignment"] != "resume" {
				t.Fatalf("resume composition metadata wrong: %#v err=%v", data, err)
			}
		case EvRunUsage:
			var usage RunUsageData
			if err := json.Unmarshal(event.Data, &usage); err != nil || usage.InvocationID == "" || usageIDs[usage.InvocationID] {
				t.Fatalf("usage=%#v err=%v ids=%#v", usage, err, usageIDs)
			}
			usageIDs[usage.InvocationID] = true
		}
	}
	if starts != 1 || resumes != 1 {
		t.Fatalf("composition segment events: starts=%d resumes=%d", starts, resumes)
	}
	if len(usageIDs) != 2 {
		t.Fatalf("approval resume duplicated or lost model usage: %#v", usageIDs)
	}
}

func TestDurableApprovalDenialResumesAsToolResultWithoutProvider(t *testing.T) {
	runtime, principal, session, approver, providerCalls := durableApprovalFixture(t)
	result, err := runtime.RunTurn(context.Background(), principal, session, TurnInput{
		RunID: "run-approval-denied", Text: "release payment",
	}, nil)
	if err != nil || result.Status != RunWaitingApproval {
		t.Fatalf("run did not pause: %#v err=%v", result, err)
	}
	approver.decide(ApprovalDenied)
	result, err = runtime.ResumeTurn(context.Background(), principal, session, ResumeInput{RunID: "run-approval-denied"}, nil)
	if err != nil || result.Status != RunCompleted {
		t.Fatalf("denied run did not resume through the model: %#v err=%v", result, err)
	}
	if providerCalls.Load() != 0 {
		t.Fatalf("denied approval executed provider %d times", providerCalls.Load())
	}
	foundDenied := false
	for _, event := range session.Events() {
		if event.Type != EvToolResult {
			continue
		}
		var data ToolResultData
		if json.Unmarshal(event.Data, &data) == nil && data.Metadata["code"] == CodeApprovalDenied {
			foundDenied = true
		}
	}
	if !foundDenied {
		t.Fatal("denied approval was not recorded as the tool result")
	}
}
