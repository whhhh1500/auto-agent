package toolcapability

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	access "github.com/whhhh1500/auto-agent/pkg/app/programmatic"
	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/extensions/subagent"
)

// This is deliberately an in-process delegation test. It proves the composed
// guarded identities, but does not claim durable child-session/link recovery.
func TestProgramExecuteDelegatesToSubagentReadOnlyTool(t *testing.T) {
	registry := core.NewCapabilityRegistry()
	product := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"}, core.ScopeRef{Kind: core.ScopeProduct, ID: "program-subagent"})
	user := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"}, core.ScopeRef{Kind: core.ScopeProduct, ID: "program-subagent"}, core.ScopeRef{Kind: core.ScopeUser, ID: "user"})
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"}, core.ScopeRef{Kind: core.ScopeProduct, ID: "program-subagent"}, core.ScopeRef{Kind: core.ScopeUser, ID: "user"}, core.ScopeRef{Kind: core.ScopeSession, ID: "program-parent"})
	principal := core.Principal{TenantID: "tenant", SubjectID: "subject", Scope: user, Grants: core.NewPermissionSet(core.PermRead)}
	catalog, err := NewCatalogCapability(CatalogID)
	if err != nil {
		t.Fatal(err)
	}
	execute, err := NewExecuteCapability(ExecuteID)
	if err != nil {
		t.Fatal(err)
	}
	profiles := core.NewAgentProfileRegistry()
	selection := core.ModelSelection{Provider: "test", Model: "deterministic"}
	parentLimit, childLimit := 8, 2
	if err := profiles.Bind(core.AgentProfileLayer{Scope: product, ProfileID: "program.parent", Model: &selection, AddCapabilities: []string{CatalogID, ExecuteID, "agent.reader"}, MaxToolCalls: &parentLimit}); err != nil {
		t.Fatal(err)
	}
	if err := profiles.Bind(core.AgentProfileLayer{Scope: product, ProfileID: "program.child", Model: &selection, AddCapabilities: []string{"child.readonly"}, MaxToolCalls: &childLimit}); err != nil {
		t.Fatal(err)
	}
	model := &programSubagentModel{}
	runtime := &core.Runtime{Capabilities: registry, Profiles: profiles, ToolJournal: newProgramJournal(), Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) { return model, nil })}
	delegate, err := subagent.NewCapability(runtime, "agent.reader", subagent.Options{ProfileID: "program.child", MaxDepth: 2, MaxChildren: 2})
	if err != nil {
		t.Fatal(err)
	}
	readOnly := &subagentReadOnlyTool{}
	for _, capability := range []core.Capability{catalog, execute, markedProgramCapability{Capability: delegate}, readOnly} {
		if err := registry.Register(product, capability); err != nil {
			t.Fatal(err)
		}
	}
	session, err := core.NewSession(core.SessionOptions{ID: "program-parent", ProfileID: "program.parent", Principal: principal, Scope: scope})
	if err != nil {
		t.Fatal(err)
	}

	snapshot, err := (core.CapabilityResolver{Registry: registry}).Resolve(principal, scope)
	if err != nil {
		t.Fatal(err)
	}
	parentProfile, err := profiles.Resolve(principal, scope, "program.parent")
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err = parentProfile.FilterCapabilities(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	projected, err := access.Project(snapshot.Capabilities())
	if err != nil {
		t.Fatal(err)
	}
	var binding string
	for _, descriptor := range projected.Descriptors() {
		if descriptor.Schema.Name == "agent.reader" {
			binding = descriptor.BindingDigest
		}
	}
	if binding == "" {
		t.Fatal("subagent capability was not exposed to program catalog")
	}
	source := `{"version":"ptc-ir/v1","body":[{"op":"call","assign":"delegation","tool":"agent.reader","args":{"op":"map","entries":{"prompt":{"op":"literal","value":"read the available record"}}}},{"op":"return","value":{"op":"get","object":{"op":"var","name":"delegation"},"key":"content"}}]}`
	model.parent = core.ToolCall{ID: "program-root", Name: ExecuteID, Args: map[string]any{"source": source, "bindings": map[string]any{"agent.reader": binding}}}
	result, err := runtime.RunTurn(context.Background(), principal, session, core.TurnInput{RunID: "program-subagent-run", Text: "delegate"}, nil)
	if err != nil || result.Status != core.RunCompleted || readOnly.calls != 1 {
		t.Fatalf("result=%#v err=%v readonly_calls=%d", result, err, readOnly.calls)
	}

	delegateCallID := toolCallID(t, session, "agent.reader")
	if delegateCallID == "" || delegateCallID[:len("program-root/")] != "program-root/" {
		t.Fatalf("delegate call identity=%q", delegateCallID)
	}
	readOnly.mu.Lock()
	childSession, childRun, childDepth, childCall := readOnly.session, readOnly.run, readOnly.depth, readOnly.call
	readOnly.mu.Unlock()
	if childSession == "" || childSession == session.ID() || childRun == "" || childCall != "child-read" || childDepth != "1" {
		t.Fatalf("child identity session=%q run=%q call=%q depth=%q", childSession, childRun, childCall, childDepth)
	}
}

func toolCallID(t *testing.T, session *core.Session, name string) string {
	t.Helper()
	for _, event := range session.Events() {
		if event.Type != core.EvToolCall {
			continue
		}
		var data core.ToolCallData
		if json.Unmarshal(event.Data, &data) == nil && data.Name == name {
			return data.CallID
		}
	}
	t.Fatalf("missing tool call %q", name)
	return ""
}

type subagentReadOnlyTool struct {
	mu                        sync.Mutex
	calls                     int
	session, run, call, depth string
}

func (*subagentReadOnlyTool) Manifest() core.CapabilityManifest {
	return core.CapabilityManifest{ID: "child.readonly", Version: "1", Name: "child.readonly", Description: "Read a fixed record.", Kind: core.KindTool, Idempotent: true,
		Tool: &core.ToolExposure{Parameters: map[string]any{"type": "object", "additionalProperties": false}}}
}

func (s *subagentReadOnlyTool) Execute(_ context.Context, request core.CapabilityRequest) (core.CapabilityResult, error) {
	if err := core.RequireAcceptedInvocation(request); err != nil {
		return core.CapabilityResult{}, err
	}
	s.mu.Lock()
	s.calls++
	s.session, s.run, s.call = request.Context.Invocation.SessionID, request.Context.Invocation.RunID, request.CallID
	s.depth = request.Context.Principal.Attributes[subagent.AttributeDepth]
	s.mu.Unlock()
	return core.CapabilityResult{Content: `{"record":"read-only"}`, OK: true}, nil
}

type programSubagentModel struct{ parent core.ToolCall }

func (*programSubagentModel) Provider() string { return "program-subagent-test" }

func (m *programSubagentModel) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	parent := false
	for _, tool := range options.Tools {
		if tool.Name == ExecuteID {
			parent = true
			break
		}
	}
	if options.Messages[len(options.Messages)-1].Role == core.RoleTool {
		text := "child read complete"
		if parent {
			text = "parent complete"
		}
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: text})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
		return nil
	}
	call := m.parent
	if !parent {
		call = core.ToolCall{ID: "child-read", Name: "child.readonly", Args: map[string]any{}}
	}
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &call})
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
	return nil
}
