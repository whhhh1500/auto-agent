package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	. "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/extensions/workflow"
	"github.com/whhhh1500/auto-agent/pkg/storage"

	_ "modernc.org/sqlite"
)

type workflowApprovalStub struct {
	mu       sync.Mutex
	decision ApprovalDecision
}

func (s *workflowApprovalStub) Approve(ctx context.Context, request ApprovalRequest) (ApprovalDecision, error) {
	resolution, err := s.RequestApproval(ctx, request)
	return resolution.Decision, err
}

func (s *workflowApprovalStub) RequestApproval(context.Context, ApprovalRequest) (ApprovalResolution, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return ApprovalResolution{
		ApprovalID: "apr_abcdefabcdefabcdefabcdefabcdefab",
		Decision:   s.decision,
	}, nil
}

func (s *workflowApprovalStub) decide(decision ApprovalDecision) {
	s.mu.Lock()
	s.decision = decision
	s.mu.Unlock()
}

type workflowApprovalTool struct {
	manifest CapabilityManifest
	calls    *atomic.Int32
	content  string
}

func (t workflowApprovalTool) Manifest() CapabilityManifest { return t.manifest }
func (t workflowApprovalTool) Execute(context.Context, CapabilityRequest) (CapabilityResult, error) {
	t.calls.Add(1)
	return CapabilityResult{Content: t.content, OK: true}, nil
}

type workflowApprovalModel struct{}

func (workflowApprovalModel) Provider() string { return "workflow-approval" }
func (workflowApprovalModel) Stream(_ context.Context, options GenerateOptions, emit func(StreamChunk)) error {
	last := options.Messages[len(options.Messages)-1]
	if last.Role == RoleTool {
		emit(StreamChunk{Kind: StreamKindAssistant, Text: "workflow complete"})
		emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishStop})
		return nil
	}
	call := ToolCall{ID: "call-flow", Name: "workflow.release", Args: map[string]any{}}
	emit(StreamChunk{Kind: StreamKindAssistant, ToolCall: &call})
	emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishToolCalls})
	return nil
}

func TestDurableApprovalResumesNestedWorkflowWithoutRepeatingPriorStep(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "workflow-approval.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := storage.OpenSQLSessionStore(context.Background(), db, storage.SQLDialectSQLite); err != nil {
		t.Fatal(err)
	}
	toolJournal, err := storage.NewSQLToolInvocationJournal(db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	_, product, _, user := testScopes()
	principal := Principal{
		SubjectID: "user-a", TenantID: "tenant-a", Scope: user,
		Grants: NewPermissionSet(PermRead, PermWrite),
	}
	var firstCalls, approvedCalls atomic.Int32
	firstManifest := CapabilityManifest{
		ID: "workflow.first", Version: "1.0.0", Name: "First", Kind: KindTool,
		Contract: "harness.tool/v1", RequiredPermissions: []Permission{PermWrite},
		Idempotent: true, Tool: &ToolExposure{},
	}
	approvedManifest := CapabilityManifest{
		ID: "workflow.approved", Version: "1.0.0", Name: "Approved", Kind: KindTool,
		Contract: "harness.tool/v1", RequiredPermissions: []Permission{PermWrite},
		RequiresApproval: true, Tool: &ToolExposure{},
	}
	flow, err := workflow.NewCapability("workflow.release", workflow.Definition{
		Description: "release workflow",
		Steps: []workflow.Step{
			{Name: "first", CapabilityID: firstManifest.ID},
			{Name: "approved", CapabilityID: approvedManifest.ID},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	capabilities := NewCapabilityRegistry()
	for _, capability := range []Capability{
		workflowApprovalTool{manifest: firstManifest, calls: &firstCalls, content: `{"first":true}`},
		workflowApprovalTool{manifest: approvedManifest, calls: &approvedCalls, content: `{"approved":true}`},
		flow,
	} {
		if err := capabilities.Register(product, capability); err != nil {
			t.Fatal(err)
		}
	}
	profiles := NewAgentProfileRegistry()
	name := "Workflow Approval"
	model := ModelSelection{Provider: "workflow-approval", Model: "workflow-approval"}
	if err := profiles.Bind(AgentProfileLayer{
		Scope: product, ProfileID: "workflow.approval", Name: &name, Model: &model,
		AddCapabilities: []string{flow.Manifest().ID, firstManifest.ID, approvedManifest.ID},
	}); err != nil {
		t.Fatal(err)
	}
	approver := &workflowApprovalStub{decision: ApprovalPending}
	runtime := &Runtime{
		Capabilities: capabilities, Profiles: profiles, Approver: approver, ToolJournal: toolJournal,
		Models: ModelResolverFunc(func(context.Context, ModelSelection) (LlmAdapter, error) {
			return workflowApprovalModel{}, nil
		}),
	}
	scope, err := user.Child(ScopeRef{Kind: ScopeSession, ID: "session-workflow-approval"})
	if err != nil {
		t.Fatal(err)
	}
	session, err := NewSession(SessionOptions{
		ID: "session-workflow-approval", ProfileID: "workflow.approval", Principal: principal, Scope: scope,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := runtime.RunTurn(context.Background(), principal, session, TurnInput{
		RunID: "run-workflow-approval", Text: "run workflow",
	}, nil)
	if err != nil || result.Status != RunWaitingApproval {
		t.Fatalf("workflow did not pause: %#v err=%v", result, err)
	}
	if firstCalls.Load() != 1 || approvedCalls.Load() != 0 {
		t.Fatalf("pre-approval calls first=%d approved=%d", firstCalls.Load(), approvedCalls.Load())
	}
	pending, ok, err := session.PendingApproval("run-workflow-approval")
	if err != nil || !ok || pending.ToolCall.ID != "call-flow/approved" || pending.ResumeCall.ID != "call-flow" {
		t.Fatalf("workflow checkpoint wrong: %#v ok=%t err=%v", pending, ok, err)
	}
	approver.decide(ApprovalApproved)
	result, err = runtime.ResumeTurn(context.Background(), principal, session, ResumeInput{RunID: "run-workflow-approval"}, nil)
	if err != nil || result.Status != RunCompleted || result.Answer != "workflow complete" {
		t.Fatalf("workflow resume failed: %#v err=%v", result, err)
	}
	if firstCalls.Load() != 1 || approvedCalls.Load() != 1 {
		t.Fatalf("workflow repeated or skipped a side effect: first=%d approved=%d", firstCalls.Load(), approvedCalls.Load())
	}
	toolCallCounts := map[string]int{}
	for _, event := range session.Events() {
		if event.Type != EvToolCall {
			continue
		}
		var data ToolCallData
		if json.Unmarshal(event.Data, &data) == nil {
			toolCallCounts[data.CallID]++
		}
	}
	for _, callID := range []string{"call-flow", "call-flow/first", "call-flow/approved"} {
		if toolCallCounts[callID] != 1 {
			t.Fatalf("tool call %s was logged %d times", callID, toolCallCounts[callID])
		}
	}
}
