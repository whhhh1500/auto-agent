package core

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
)

type compositionRevisionProbe struct {
	mu        sync.Mutex
	revisions []string
}

func (p *compositionRevisionProbe) Manifest() CapabilityManifest {
	return toolManifest("revision.probe", "1.0.0")
}
func (p *compositionRevisionProbe) Execute(_ context.Context, request CapabilityRequest) (CapabilityResult, error) {
	p.mu.Lock()
	p.revisions = append(p.revisions, request.Context.CompositionRevision)
	p.mu.Unlock()
	return CapabilityResult{Content: `{"ok":true}`, OK: true}, nil
}

type compositionRevisionModel struct{}

func (compositionRevisionModel) Provider() string { return "revision-test" }
func (compositionRevisionModel) Stream(_ context.Context, options GenerateOptions, emit func(StreamChunk)) error {
	if options.Messages[len(options.Messages)-1].Role == RoleTool {
		emit(StreamChunk{Kind: StreamKindAssistant, Text: "done"})
		emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishStop})
		return nil
	}
	emit(StreamChunk{Kind: StreamKindAssistant, ToolCall: &ToolCall{ID: "revision-call", Name: "revision.probe", Args: map[string]any{}}})
	emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishToolCalls})
	return nil
}

func TestCapabilityContextCompositionRevisionAcrossRunsAndDiscloseTools(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	probe := &compositionRevisionProbe{}
	caps := NewCapabilityRegistry()
	if err := caps.Register(product, probe); err != nil {
		t.Fatal(err)
	}
	profiles := NewAgentProfileRegistry()
	model := ModelSelection{Provider: "revision-test", Model: "revision-test"}
	if err := profiles.Bind(AgentProfileLayer{Scope: product, ProfileID: "revision.agent", Model: &model, AddCapabilities: []string{"revision.probe"}}); err != nil {
		t.Fatal(err)
	}
	scope, err := user.Child(ScopeRef{Kind: ScopeSession, ID: "revision-session"})
	if err != nil {
		t.Fatal(err)
	}
	session, err := NewSession(SessionOptions{ID: "revision-session", ProfileID: "revision.agent", Principal: principal, Scope: scope})
	if err != nil {
		t.Fatal(err)
	}
	runtime := &Runtime{Capabilities: caps, Profiles: profiles, DiscloseTools: true, Models: ModelResolverFunc(func(context.Context, ModelSelection) (LlmAdapter, error) { return compositionRevisionModel{}, nil }), ToolJournal: newMemoryToolInvocationJournal()}
	for i, metadata := range []map[string]string{{"assignment": "one"}, {"assignment": "two"}} {
		if _, err := runtime.RunTurn(context.Background(), principal, session, TurnInput{RunID: "revision-run-" + string(rune('a'+i)), Text: "run", CompositionMetadata: metadata}, nil); err != nil {
			t.Fatal(err)
		}
	}
	probe.mu.Lock()
	defer probe.mu.Unlock()
	if len(probe.revisions) != 2 || probe.revisions[0] == "" || probe.revisions[0] == probe.revisions[1] {
		t.Fatalf("composition revisions=%q", probe.revisions)
	}
}

func TestCapabilityContextCompositionRevisionSurvivesApprovalResume(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	probe := &compositionRevisionProbe{}
	manifest := probe.Manifest()
	manifest.RequiresApproval = true
	probeManifest := &approvalRevisionProbe{probe: probe, manifest: manifest}
	caps := NewCapabilityRegistry()
	if err := caps.Register(product, probeManifest); err != nil {
		t.Fatal(err)
	}
	profiles := NewAgentProfileRegistry()
	model := ModelSelection{Provider: "revision-test", Model: "revision-test"}
	if err := profiles.Bind(AgentProfileLayer{Scope: product, ProfileID: "revision.approval", Model: &model, AddCapabilities: []string{"revision.probe"}}); err != nil {
		t.Fatal(err)
	}
	approver := newDurableApprovalStub()
	scope, err := user.Child(ScopeRef{Kind: ScopeSession, ID: "revision-approval-session"})
	if err != nil {
		t.Fatal(err)
	}
	session, err := NewSession(SessionOptions{ID: "revision-approval-session", ProfileID: "revision.approval", Principal: principal, Scope: scope})
	if err != nil {
		t.Fatal(err)
	}
	runtime := &Runtime{Capabilities: caps, Profiles: profiles, Approver: approver, Models: ModelResolverFunc(func(context.Context, ModelSelection) (LlmAdapter, error) { return approvalRevisionModel{}, nil })}
	first, err := runtime.RunTurn(context.Background(), principal, session, TurnInput{RunID: "revision-approval-run", Text: "approve", CompositionMetadata: map[string]string{"segment": "first"}}, nil)
	if err != nil || first.Status != RunWaitingApproval {
		t.Fatalf("first=%#v err=%v", first, err)
	}
	approver.decide(ApprovalApproved)
	second, err := runtime.ResumeTurn(context.Background(), principal, session, ResumeInput{RunID: "revision-approval-run", CompositionMetadata: map[string]string{"segment": "resume"}}, nil)
	if err != nil || second.Status != RunCompleted {
		t.Fatalf("resume=%#v err=%v", second, err)
	}
	probe.mu.Lock()
	defer probe.mu.Unlock()
	if len(probe.revisions) != 1 || probe.revisions[0] == "" {
		t.Fatalf("resume composition revisions=%q", probe.revisions)
	}
	var resumeRevision string
	for _, event := range session.Events() {
		if event.Type != EvRunResume {
			continue
		}
		var data RunResumeData
		if err := json.Unmarshal(event.Data, &data); err == nil {
			resumeRevision = data.CompositionRevision
		}
	}
	if resumeRevision == "" || probe.revisions[0] != resumeRevision {
		t.Fatalf("provider revision=%q resume event revision=%q", probe.revisions[0], resumeRevision)
	}
}

type approvalRevisionProbe struct {
	probe    *compositionRevisionProbe
	manifest CapabilityManifest
}

func (p *approvalRevisionProbe) Manifest() CapabilityManifest { return p.manifest }
func (p *approvalRevisionProbe) Execute(ctx context.Context, request CapabilityRequest) (CapabilityResult, error) {
	return p.probe.Execute(ctx, request)
}

type approvalRevisionModel struct{}

func (approvalRevisionModel) Provider() string { return "revision-test" }
func (approvalRevisionModel) Stream(_ context.Context, options GenerateOptions, emit func(StreamChunk)) error {
	if options.Messages[len(options.Messages)-1].Role == RoleTool {
		emit(StreamChunk{Kind: StreamKindAssistant, Text: "done"})
		emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishStop})
		return nil
	}
	emit(StreamChunk{Kind: StreamKindAssistant, ToolCall: &ToolCall{ID: "revision-call", Name: "revision.probe", Args: map[string]any{}}})
	emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishToolCalls})
	return nil
}
