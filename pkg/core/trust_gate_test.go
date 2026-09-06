package core

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type strictTool struct {
	manifest CapabilityManifest
	calls    atomic.Int32
}

type nestedCompositeTool struct {
	manifest CapabilityManifest
	inner    ToolCall
	calls    atomic.Int32
}

func (tool *nestedCompositeTool) Manifest() CapabilityManifest { return tool.manifest }

func (tool *nestedCompositeTool) Execute(ctx context.Context, request CapabilityRequest) (CapabilityResult, error) {
	if err := RequireAcceptedInvocation(request); err != nil {
		return CapabilityResult{}, err
	}
	tool.calls.Add(1)
	if request.Context.Invoker == nil {
		return CapabilityResult{}, errors.New("nested invoker is unavailable")
	}
	return request.Context.Invoker.InvokeTool(ctx, tool.inner)
}

func (tool *strictTool) Manifest() CapabilityManifest { return tool.manifest }

func (tool *strictTool) Execute(_ context.Context, request CapabilityRequest) (CapabilityResult, error) {
	if err := RequireAcceptedInvocation(request); err != nil {
		return CapabilityResult{}, err
	}
	tool.calls.Add(1)
	return CapabilityResult{Content: "accepted", OK: true}, nil
}

type modelGateFunc func(context.Context, ModelCallRequest) error

func (gate modelGateFunc) AuthorizeModelCall(ctx context.Context, request ModelCallRequest) error {
	return gate(ctx, request)
}

type strictModelAdapter struct {
	calls atomic.Int32
}

func (adapter *strictModelAdapter) Provider() string { return "strict-provider" }

func (adapter *strictModelAdapter) Stream(_ context.Context, options GenerateOptions, emit func(StreamChunk)) error {
	if err := RequireAcceptedModelCall(options); err != nil {
		return err
	}
	adapter.calls.Add(1)
	emit(StreamChunk{Kind: StreamKindAssistant, Text: "ok"})
	emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishStop})
	return nil
}

func TestCapabilitySnapshotDirectAndUnjournaledAgentCallsHaveNoAcceptance(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	manifest := toolManifest("strict.tool", "1.0.0")
	tool := &strictTool{manifest: manifest}
	registry := NewCapabilityRegistry()
	if err := registry.Register(product, tool); err != nil {
		t.Fatal(err)
	}
	sessionScope, _ := user.Child(ScopeRef{Kind: ScopeSession, ID: "strict-agent-session"})
	snapshot, err := (CapabilityResolver{Registry: registry}).Resolve(principal, sessionScope)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := snapshot.Execute(context.Background(), ToolCall{ID: "direct-call", Name: manifest.ID}); !errors.Is(err, ErrAcceptedInvocationRequired) {
		t.Fatalf("direct snapshot call error=%v", err)
	}
	if tool.calls.Load() != 0 {
		t.Fatal("direct snapshot call reached strict provider")
	}
	if _, err := runStrictAgent(t, snapshot, principal, tool, nil); err != nil {
		t.Fatal(err)
	}
	if tool.calls.Load() != 0 {
		t.Fatal("agent without journal minted an acceptance")
	}
}

func TestAcceptedInvocationBindsCallArgumentsIdentityAndScope(t *testing.T) {
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	call := ToolCall{ID: "accepted-call", Name: "strict.tool", Args: map[string]any{"value": "original"}}
	info := RunInfo{SessionID: "accepted-session", RunID: "accepted-run", Principal: principal}
	invocation, err := NewToolInvocation(info, call, true)
	if err != nil {
		t.Fatal(err)
	}
	request := CapabilityRequest{
		CallID: call.ID, Args: call.Args,
		Context: CapabilityContext{
			Principal: principal, Scope: user, CapabilityID: call.Name,
			Invocation: Invocation{SessionID: info.SessionID, RunID: info.RunID, CallID: call.ID},
			Accepted:   mintAcceptedInvocation(invocation, user, principal),
		},
	}
	if err := RequireAcceptedInvocation(request); err != nil {
		t.Fatal(err)
	}
	mutations := []func(*CapabilityRequest){
		func(value *CapabilityRequest) { value.Args["value"] = "tampered" },
		func(value *CapabilityRequest) { value.CallID = "other-call" },
		func(value *CapabilityRequest) { value.Context.CapabilityID = "other.tool" },
		func(value *CapabilityRequest) {
			value.Context.Scope = MustScopePath(ScopeRef{Kind: ScopeGlobal, ID: "other"})
		},
		func(value *CapabilityRequest) { value.Context.Principal.SubjectID = "other-subject" },
	}
	for index, mutate := range mutations {
		t.Run(fmt.Sprintf("mutation-%d", index), func(t *testing.T) {
			candidate := request
			candidate.Args = map[string]any{"value": "original"}
			mutate(&candidate)
			if err := RequireAcceptedInvocation(candidate); !errors.Is(err, ErrAcceptedInvocationRequired) {
				t.Fatalf("tampered tool proof error=%v", err)
			}
		})
	}
}

func TestAgentJournalMintsAcceptanceAndBindsScopeAndArguments(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	manifest := toolManifest("strict.journal.tool", "1.0.0")
	tool := &strictTool{manifest: manifest}
	registry := NewCapabilityRegistry()
	if err := registry.Register(product, tool); err != nil {
		t.Fatal(err)
	}
	sessionScope, _ := user.Child(ScopeRef{Kind: ScopeSession, ID: "strict-agent-session"})
	snapshot, err := (CapabilityResolver{Registry: registry}).Resolve(principal, sessionScope)
	if err != nil {
		t.Fatal(err)
	}
	call := ToolCall{ID: "journal-call", Name: manifest.ID}
	journal := newMemoryToolInvocationJournal()
	result, err := runStrictAgent(t, snapshot, principal, tool, journalCallAdapter{call: call}, journal)
	if err != nil || result.Status != RunCompleted {
		t.Fatalf("gated agent failed: %#v %v", result, err)
	}
	if tool.calls.Load() != 1 {
		t.Fatalf("strict provider calls=%d, want 1", tool.calls.Load())
	}
}

func TestNestedCompositeReentersGuardAndGetsIndependentAcceptance(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	outerManifest := toolManifest("strict.outer.tool", "1.0.0")
	innerManifest := toolManifest("strict.inner.tool", "1.0.0")
	outer := &nestedCompositeTool{manifest: outerManifest, inner: ToolCall{ID: "nested-call", Name: innerManifest.ID}}
	inner := &strictTool{manifest: innerManifest}
	registry := NewCapabilityRegistry()
	if err := registry.Register(product, outer); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(product, inner); err != nil {
		t.Fatal(err)
	}
	sessionScope, _ := user.Child(ScopeRef{Kind: ScopeSession, ID: "strict-agent-session"})
	snapshot, err := (CapabilityResolver{Registry: registry}).Resolve(principal, sessionScope)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := snapshot.Execute(context.Background(), ToolCall{ID: "direct-outer", Name: outerManifest.ID}); !errors.Is(err, ErrAcceptedInvocationRequired) {
		t.Fatalf("direct composite bypass error=%v", err)
	}
	result, err := runStrictAgent(t, snapshot, principal, inner, journalCallAdapter{call: ToolCall{ID: "outer-call", Name: outerManifest.ID}}, newMemoryToolInvocationJournal())
	if err != nil || result.Status != RunCompleted {
		t.Fatalf("nested guarded run failed: %#v %v", result, err)
	}
	if outer.calls.Load() != 1 || inner.calls.Load() != 1 {
		t.Fatalf("outer calls=%d inner calls=%d, want one each", outer.calls.Load(), inner.calls.Load())
	}
}

func TestCompletedJournalReplayDoesNotReexecuteStrictProvider(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	manifest := toolManifest("strict.replay.tool", "1.0.0")
	tool := &strictTool{manifest: manifest}
	registry := NewCapabilityRegistry()
	if err := registry.Register(product, tool); err != nil {
		t.Fatal(err)
	}
	sessionScope, _ := user.Child(ScopeRef{Kind: ScopeSession, ID: "strict-agent-session"})
	snapshot, err := (CapabilityResolver{Registry: registry}).Resolve(principal, sessionScope)
	if err != nil {
		t.Fatal(err)
	}
	call := ToolCall{ID: "replay-call", Name: manifest.ID}
	journal := newMemoryToolInvocationJournal()
	adapter := journalCallAdapter{call: call}
	if result, err := runStrictAgent(t, snapshot, principal, tool, adapter, journal); err != nil || result.Status != RunCompleted {
		t.Fatalf("first strict run failed: %#v %v", result, err)
	}
	if result, err := runStrictAgent(t, snapshot, principal, tool, adapter, journal); err != nil || result.Status != RunCompleted {
		t.Fatalf("replay strict run failed: %#v %v", result, err)
	}
	if tool.calls.Load() != 1 {
		t.Fatalf("strict provider replay calls=%d, want 1", tool.calls.Load())
	}
}

func TestModelCallGateMintsProofAndRejectsBeforeAdapter(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	registry := NewCapabilityRegistry()
	manifest := toolManifest("model.gate.tool", "1.0.0")
	if err := registry.Register(product, staticTool{manifest: manifest, content: "ok"}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := (CapabilityResolver{Registry: registry}).Resolve(principal, user)
	if err != nil {
		t.Fatal(err)
	}
	adapter := &strictModelAdapter{}
	mutatingGate := modelGateFunc(func(_ context.Context, request ModelCallRequest) error {
		request.Principal.Grants[PermWrite] = false
		if request.Principal.Attributes == nil {
			request.Principal.Attributes = map[string]string{}
		}
		request.Principal.Attributes["mutated"] = "gate"
		return nil
	})
	sessionScope, _ := user.Child(ScopeRef{Kind: ScopeSession, ID: "model-gate-session"})
	session, err := NewSession(SessionOptions{ID: "model-gate-session", ProfileID: "model-gate", Principal: principal, Scope: sessionScope})
	if err != nil {
		t.Fatal(err)
	}
	agent, err := NewAgent(AgentOptions{
		LLM: adapter, Tools: snapshot, Session: session, Provider: "strict-provider", Model: "strict-model",
		ModelCallGate: mutatingGate,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := agent.RunTurn(context.Background(), TurnInput{RunID: "model-gate-run", Text: "hello"})
	if err != nil || result.Status != RunCompleted || adapter.calls.Load() != 1 {
		t.Fatalf("gated model call failed: %#v %v calls=%d", result, err, adapter.calls.Load())
	}
	if principal.Grants[PermWrite] != true || principal.Attributes["mutated"] != "" {
		t.Fatal("gate mutation leaked into caller principal")
	}

	rejected := &strictModelAdapter{}
	denied, err := NewAgent(AgentOptions{
		LLM: rejected, Tools: snapshot, Session: session, Provider: "strict-provider", Model: "strict-model",
		ModelCallGate: modelGateFunc(func(context.Context, ModelCallRequest) error { return errors.New("denied") }),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = denied.RunTurn(context.Background(), TurnInput{RunID: "model-gate-denied", Text: "hello"})
	if rejected.calls.Load() != 0 {
		t.Fatal("model adapter called after gate rejection")
	}
}

func TestModelCallGatePanicDoesNotReachAdapter(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	registry := NewCapabilityRegistry()
	manifest := toolManifest("model.gate.panic.tool", "1.0.0")
	if err := registry.Register(product, staticTool{manifest: manifest, content: "ok"}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := (CapabilityResolver{Registry: registry}).Resolve(principal, user)
	if err != nil {
		t.Fatal(err)
	}
	scope, _ := user.Child(ScopeRef{Kind: ScopeSession, ID: "model-gate-panic-session"})
	session, err := NewSession(SessionOptions{ID: "model-gate-panic-session", ProfileID: "model-gate", Principal: principal, Scope: scope})
	if err != nil {
		t.Fatal(err)
	}
	adapter := &strictModelAdapter{}
	agent, err := NewAgent(AgentOptions{LLM: adapter, Tools: snapshot, Session: session, Provider: "strict-provider", Model: "strict-model", ModelCallGate: modelGateFunc(func(context.Context, ModelCallRequest) error { panic("gate panic") })})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = agent.RunTurn(context.Background(), TurnInput{RunID: "model-gate-panic-run", Text: "hello"})
	if adapter.calls.Load() != 0 {
		t.Fatal("model adapter called after gate panic")
	}
}

func TestAcceptedModelCallBindsFullPrincipalAndAdapterIdentity(t *testing.T) {
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	principal.Attributes = map[string]string{"plan": "pro"}
	request := ModelCallRequest{
		Principal: principal, Scope: user, SessionID: "model-session", RunID: "model-run",
		Step: 2, Provider: "provider", Model: "model", Budget: ModelCallBudget{MaxOutputTokens: 128},
	}
	request.Deadline = time.Unix(100, 0).UTC()
	options := GenerateOptions{Provider: request.Provider, Model: request.Model, ModelCall: request, AcceptedCall: mintAcceptedModelCall(request)}
	if err := RequireAcceptedModelCall(options); err != nil {
		t.Fatal(err)
	}
	mutations := []func(*ModelCallRequest){
		func(value *ModelCallRequest) {
			value.Principal.Scope = MustScopePath(ScopeRef{Kind: ScopeGlobal, ID: "other"})
		},
		func(value *ModelCallRequest) { value.Principal.Grants[PermWrite] = !value.Principal.Grants[PermWrite] },
		func(value *ModelCallRequest) { value.Principal.Attributes["plan"] = "free" },
		func(value *ModelCallRequest) { value.Step++ },
		func(value *ModelCallRequest) { value.Model = "other-model" },
	}
	for index, mutate := range mutations {
		t.Run(fmt.Sprintf("mutation-%d", index), func(t *testing.T) {
			candidate := options
			candidate.ModelCall = cloneModelCallRequest(options.ModelCall)
			candidate.ModelCall.Principal.Grants = candidate.ModelCall.Principal.Grants.Clone()
			candidate.ModelCall.Principal.Attributes = clonePrincipal(candidate.ModelCall.Principal).Attributes
			mutate(&candidate.ModelCall)
			if err := RequireAcceptedModelCall(candidate); !errors.Is(err, ErrAcceptedModelCallRequired) {
				t.Fatalf("tampered model proof error=%v", err)
			}
		})
	}
	outside := request
	outside.Scope = MustScopePath(ScopeRef{Kind: ScopeGlobal, ID: "global"})
	if err := outside.Validate(); err == nil {
		t.Fatal("model request outside principal scope was accepted")
	}
}

func TestPrepareAcceptedModelCallContract(t *testing.T) {
	_, _, _, user := testScopes()
	request := ModelCallRequest{Principal: testPrincipal(user), Scope: user, SessionID: "prepare-session", RunID: "prepare-run", Provider: "p", Model: "m"}
	empty, err := PrepareAcceptedModelCall(context.Background(), nil, request)
	if err != nil || empty.seal != nil {
		t.Fatalf("nil gate result=%#v err=%v", empty, err)
	}
	accepted, err := PrepareAcceptedModelCall(context.Background(), modelGateFunc(func(context.Context, ModelCallRequest) error { return nil }), request)
	if err != nil || accepted.seal == nil {
		t.Fatalf("gated result=%#v err=%v", accepted, err)
	}
	if err := RequireAcceptedModelCall(GenerateOptions{Provider: "p", Model: "m", ModelCall: request, AcceptedCall: accepted}); err != nil {
		t.Fatalf("accepted proof rejected: %v", err)
	}
	bad := request
	bad.RunID = ""
	if _, err := PrepareAcceptedModelCall(context.Background(), nil, bad); err == nil {
		t.Fatal("nil gate accepted invalid request")
	}
	var nilContext context.Context
	if _, err := PrepareAcceptedModelCall(nilContext, modelGateFunc(func(context.Context, ModelCallRequest) error { t.Fatal("nil context reached gate"); return nil }), request); err == nil {
		t.Fatal("nil context accepted")
	}
	if _, err := PrepareAcceptedModelCall(context.Background(), modelGateFunc(func(context.Context, ModelCallRequest) error { t.Fatal("gate called for invalid request"); return nil }), bad); err == nil {
		t.Fatal("invalid request accepted")
	}
	if _, err := PrepareAcceptedModelCall(context.Background(), modelGateFunc(func(context.Context, ModelCallRequest) error { panic("secret gate panic") }), request); err == nil || strings.Contains(err.Error(), "secret gate panic") {
		t.Fatalf("gate panic leaked: %v", err)
	}
}

func runStrictAgent(t *testing.T, snapshot *CapabilitySnapshot, principal Principal, _ *strictTool, model LlmAdapter, journals ...ToolInvocationJournal) (TurnResult, error) {
	t.Helper()
	if model == nil {
		model = journalCallAdapter{call: ToolCall{ID: "unused", Name: "strict.tool"}}
	}
	scope, err := principal.Scope.Child(ScopeRef{Kind: ScopeSession, ID: "strict-agent-session"})
	if err != nil {
		return TurnResult{}, err
	}
	session, err := NewSession(SessionOptions{ID: "strict-agent-session", ProfileID: "strict-agent", Principal: principal, Scope: scope})
	if err != nil {
		return TurnResult{}, err
	}
	opts := AgentOptions{LLM: model, Tools: snapshot, Session: session, Provider: "journal-test", Model: "journal-test"}
	if len(journals) != 0 {
		opts.ToolJournal = journals[0]
	}
	agent, err := NewAgent(opts)
	if err != nil {
		return TurnResult{}, err
	}
	return agent.RunTurn(context.Background(), TurnInput{RunID: "strict-agent-run", Text: "run"})
}
