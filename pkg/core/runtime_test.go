package core

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

type neverLLM struct{ called bool }

func (n *neverLLM) Provider() string { return "never" }

func (n *neverLLM) Stream(context.Context, GenerateOptions, func(StreamChunk)) error {
	n.called = true
	return errors.New("LLM should not be called")
}

type revisionLLM struct{ MockLlmAdapter }

func (revisionLLM) Provider() string         { return "revision-model" }
func (revisionLLM) ArtifactRevision() string { return "model-build-sensitive-label" }

type schemaCapturingLLM struct {
	tools []ToolSchema
	call  ToolCall
}

func (l *schemaCapturingLLM) Provider() string { return "schema-capturing" }

func (l *schemaCapturingLLM) Stream(_ context.Context, options GenerateOptions, emit func(StreamChunk)) error {
	l.tools = append([]ToolSchema(nil), options.Tools...)
	last := options.Messages[len(options.Messages)-1]
	if last.Role == RoleTool {
		emit(StreamChunk{Kind: StreamKindAssistant, Text: "Capability result: " + last.Content})
		emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishStop})
		return nil
	}
	emit(StreamChunk{Kind: StreamKindAssistant, ToolCall: &l.call})
	emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishToolCalls})
	return nil
}

func TestRuntimeToolDisclosureIsOptInForRunAndResume(t *testing.T) {
	for _, test := range []struct {
		name     string
		disclose bool
	}{
		{name: "default direct tools", disclose: false},
		{name: "explicit library", disclose: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, product, _, user := testScopes()
			principal := testPrincipal(user)
			capabilities := NewCapabilityRegistry()
			mail := toolManifest("notify.send", "1.0.0")
			mail.Tool = &ToolExposure{Description: "Send email", Parameters: map[string]any{"type": "object"}}
			chat := toolManifest("chat.send", "1.0.0")
			chat.Tool = &ToolExposure{Description: "Send Slack", Parameters: map[string]any{"type": "object"}}
			if err := capabilities.Register(product, staticTool{manifest: mail, content: "email-sent"}); err != nil {
				t.Fatal(err)
			}
			if err := capabilities.Register(product, staticTool{manifest: chat, content: "slack-sent"}); err != nil {
				t.Fatal(err)
			}
			profiles := NewAgentProfileRegistry()
			name := "Agent"
			model := ModelSelection{Provider: "schema-capturing", Model: "test"}
			if err := profiles.Bind(AgentProfileLayer{
				Scope: product, ProfileID: "tool-disclosure.agent", Name: &name, Model: &model,
				AddCapabilities: []string{"notify.send", "chat.send"},
			}); err != nil {
				t.Fatal(err)
			}
			sessionScope, _ := user.Child(ScopeRef{Kind: ScopeSession, ID: "tool-disclosure-session"})
			session, err := NewSession(SessionOptions{
				ID: "tool-disclosure-session", ProfileID: "tool-disclosure.agent", Principal: principal, Scope: sessionScope,
			})
			if err != nil {
				t.Fatal(err)
			}
			modelAdapter := &schemaCapturingLLM{call: ToolCall{ID: "call-1", Name: "chat.send"}}
			runtime := &Runtime{
				Capabilities: capabilities, Profiles: profiles, DiscloseTools: test.disclose,
				Models: ModelResolverFunc(func(context.Context, ModelSelection) (LlmAdapter, error) { return modelAdapter, nil }),
			}

			result, err := runtime.RunTurn(context.Background(), principal, session, TurnInput{RunID: "run-disclosure", Text: "send"}, nil)
			if err != nil || result.Answer != "Capability result: slack-sent" {
				t.Fatalf("run direct call failed: %#v %v", result, err)
			}
			assertRuntimeToolSchemas(t, modelAdapter.tools, test.disclose)

			resumed, code, err := runtime.composeResumeAgent(context.Background(), principal, session, nil, nil)
			if err != nil || code != "" {
				t.Fatalf("resume composition failed: %q %v", code, err)
			}
			assertRuntimeToolSchemas(t, resumed.opts.Tools.Schemas(), test.disclose)
			called, err := resumed.opts.Tools.Execute(context.Background(), ToolCall{ID: "resume-call", Name: "chat.send"})
			if err != nil || !called.OK || called.Content != "slack-sent" {
				t.Fatalf("resume direct call failed: %#v %v", called, err)
			}
		})
	}
}

func TestRuntimeProfilePermissionFilteringCannotBeBypassedThroughFinalSnapshot(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	principal.Grants = NewPermissionSet(PermRead)

	capabilities := NewCapabilityRegistry()
	read := toolManifest("memory.recall", "1.0.0")
	write := toolManifest("memory.remember", "1.0.0")
	write.RequiredPermissions = []Permission{PermWrite}
	for _, capability := range []staticTool{
		{manifest: read, content: "recalled"},
		{manifest: write, content: "remembered"},
	} {
		if err := capabilities.Register(product, capability); err != nil {
			t.Fatal(err)
		}
	}
	profiles := NewAgentProfileRegistry()
	name := "General"
	model := ModelSelection{Provider: "mock", Model: "mock-1"}
	if err := profiles.Bind(AgentProfileLayer{
		Scope: product, ProfileID: "general", Name: &name, Model: &model,
		AddCapabilities: []string{"memory.recall", "memory.remember"},
	}); err != nil {
		t.Fatal(err)
	}
	sessionScope, err := user.Child(ScopeRef{Kind: ScopeSession, ID: "permission-filter"})
	if err != nil {
		t.Fatal(err)
	}
	resolver := CapabilityResolver{Registry: capabilities}
	public, err := resolver.Resolve(principal, sessionScope)
	if err != nil {
		t.Fatal(err)
	}
	if public.Authorized("memory.remember") || !public.Authorized("memory.recall") {
		t.Fatalf("public resolver permission filtering changed: %#v", public.Capabilities())
	}

	profile, err := profiles.Resolve(principal, sessionScope, "general")
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := resolver.resolveForProfile(principal, sessionScope)
	if err != nil {
		t.Fatal(err)
	}
	final, err := profile.FilterCapabilities(resolved)
	if err != nil {
		t.Fatalf("exact profile validation unexpectedly failed: %v", err)
	}
	final = final.FilterByPermissions(principal.Grants)
	if len(final.Schemas()) != 1 || final.Schemas()[0].Name != "memory.recall" || final.Authorized("memory.remember") {
		t.Fatalf("final execution snapshot retained unauthorized capability: %#v", final.Schemas())
	}
	result, err := final.Execute(context.Background(), ToolCall{ID: "call-remember", Name: "memory.remember", Args: map[string]any{}})
	if err != nil || result.OK || result.Content != "capability is not available in this run" {
		t.Fatalf("final snapshot allowed direct write execution: result=%#v err=%v", result, err)
	}
}

func assertRuntimeToolSchemas(t *testing.T, schemas []ToolSchema, disclosed bool) {
	t.Helper()
	if hasToolSchema(schemas, disclosedLibraryID) != disclosed {
		t.Fatalf("library disclosure=%t schemas=%#v", disclosed, schemas)
	}
	if hasToolSchema(schemas, "notify.send") == disclosed || hasToolSchema(schemas, "chat.send") == disclosed {
		t.Fatalf("direct tool visibility disagrees with disclosure=%t: %#v", disclosed, schemas)
	}
}

func TestRuntimeAppendsMultiTurnFastPathWithAuditEvents(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	capabilities := NewCapabilityRegistry()
	profiles := NewAgentProfileRegistry()
	manifest := toolManifest("market.quote", "1.0.0")
	manifest.Execution = &ExecutionSpec{Runtime: "http", Headers: map[string]string{
		"Authorization": "Bearer must-not-enter-events",
		"X-Credential":  "$credential:market-feed",
	}}
	if err := capabilities.Register(product, staticTool{manifest: manifest, content: "42"}); err != nil {
		t.Fatal(err)
	}
	name := "Agent"
	model := ModelSelection{Provider: "never", Model: "never"}
	if err := profiles.Bind(AgentProfileLayer{
		Scope: product, ProfileID: "product.agent", Name: &name, Model: &model,
		AddCapabilities: []string{"market.quote"},
		Metadata:        map[string]string{"api_key": "profile-secret", "team": "markets"},
	}); err != nil {
		t.Fatal(err)
	}
	llm := &neverLLM{}
	runtime := &Runtime{
		Capabilities: capabilities, Profiles: profiles,
		Models: ModelResolverFunc(func(context.Context, ModelSelection) (LlmAdapter, error) { return llm, nil }),
		FastRouters: FastRouterResolverFunc(func(context.Context, *AgentProfileSnapshot) (*FastRouter, error) {
			router := &FastRouter{}
			router.Add(FastRule{Match: func(string) bool { return true }, Capability: "market.quote"})
			return router, nil
		}),
	}
	sessionScope, _ := user.Child(ScopeRef{Kind: ScopeSession, ID: "session-a"})
	session, _ := NewSession(SessionOptions{
		ID: "session-a", ProfileID: "product.agent", Principal: principal, Scope: sessionScope,
	})
	for _, runID := range []string{"run-a", "run-b"} {
		result, err := runtime.RunTurn(context.Background(), principal, session, TurnInput{RunID: runID, Text: "quote"}, nil)
		if err != nil || result.Status != RunCompleted || result.Answer != "42" {
			t.Fatalf("unexpected run result: %#v, %v", result, err)
		}
	}
	if llm.called {
		t.Fatal("fast path called the LLM")
	}
	events := session.Events()
	if len(events) != 12 || events[2].Type != EvToolCall || events[3].Type != EvToolResult || events[6].Seq != 6 {
		t.Fatalf("unexpected multi-turn event log: %#v", events)
	}
	var start RunStartData
	if err := json.Unmarshal(events[0].Data, &start); err != nil {
		t.Fatal(err)
	}
	if start.Composition == nil || start.Composition.Profile.ProfileID != "product.agent" {
		t.Fatalf("run composition did not persist the resolved profile: %#v", start)
	}
	if len(start.Composition.Capabilities) != 1 || start.Composition.Capabilities[0].Manifest.ID != "market.quote" {
		t.Fatalf("run composition did not persist filtered capabilities: %#v", start.Composition)
	}
	if start.Composition.ResolvedProvider != "never" || start.Composition.MaxSteps <= 0 || start.Composition.MaxToolCalls <= 0 {
		t.Fatalf("run composition did not persist model and limits: %#v", start.Composition)
	}
	if start.Composition.Profile.Metadata["api_key"] != "[redacted]" || start.Composition.Profile.Metadata["team"] != "markets" {
		t.Fatalf("profile audit metadata was not safely redacted: %#v", start.Composition.Profile.Metadata)
	}
	headers := start.Composition.Capabilities[0].Manifest.Execution.Headers
	if headers["Authorization"] != "[redacted]" || headers["X-Credential"] != "$credential:market-feed" {
		t.Fatalf("run composition leaked a literal header or removed a credential reference: %#v", headers)
	}
}

func TestRuntimeRejectsDuplicateRunIDAfterCompositionFailure(t *testing.T) {
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	sessionScope, _ := user.Child(ScopeRef{Kind: ScopeSession, ID: "session-duplicate"})
	session, _ := NewSession(SessionOptions{ID: "session-duplicate", ProfileID: "missing.agent", Principal: principal, Scope: sessionScope})
	runtime := &Runtime{Capabilities: NewCapabilityRegistry(), Profiles: NewAgentProfileRegistry(), Models: ModelResolverFunc(func(context.Context, ModelSelection) (LlmAdapter, error) { return MockLlmAdapter{}, nil })}
	if _, err := runtime.RunTurn(context.Background(), principal, session, TurnInput{RunID: "run-duplicate", Text: "first"}, nil); err == nil {
		t.Fatal("first composition failure unexpectedly succeeded")
	}
	version := session.Version()
	if _, err := runtime.RunTurn(context.Background(), principal, session, TurnInput{RunID: "run-duplicate", Text: "second"}, nil); err == nil {
		t.Fatal("duplicate run id was accepted")
	}
	if session.Version() != version {
		t.Fatalf("duplicate run appended events to the old run: before=%d after=%d", version, session.Version())
	}
}

func TestRuntimeCompositionFailureHasTerminalEvents(t *testing.T) {
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	sessionScope, _ := user.Child(ScopeRef{Kind: ScopeSession, ID: "session-a"})
	session, _ := NewSession(SessionOptions{
		ID: "session-a", ProfileID: "missing.profile", Principal: principal, Scope: sessionScope,
	})
	runtime := &Runtime{
		Capabilities: NewCapabilityRegistry(), Profiles: NewAgentProfileRegistry(),
		Models: ModelResolverFunc(func(context.Context, ModelSelection) (LlmAdapter, error) { return MockLlmAdapter{}, nil }),
	}
	result, err := runtime.RunTurn(context.Background(), principal, session, TurnInput{RunID: "run-a", Text: "hello"}, nil)
	if err == nil || result.Status != RunFailed {
		t.Fatalf("expected composition failure, got %#v, %v", result, err)
	}
	events := session.Events()
	if len(events) != 4 || events[2].Type != EvRunError || events[3].Type != EvRunEnd {
		t.Fatalf("composition failure did not reach a terminal state: %#v", events)
	}
}

func TestRuntimeCompositionFailurePersistsAssignmentMetadata(t *testing.T) {
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	sessionScope, _ := user.Child(ScopeRef{Kind: ScopeSession, ID: "session-failed-assignment"})
	session, _ := NewSession(SessionOptions{
		ID: "session-failed-assignment", ProfileID: "missing.profile", Principal: principal, Scope: sessionScope,
	})
	runtime := &Runtime{
		Capabilities: NewCapabilityRegistry(), Profiles: NewAgentProfileRegistry(),
		Models: ModelResolverFunc(func(context.Context, ModelSelection) (LlmAdapter, error) { return MockLlmAdapter{}, nil }),
	}
	metadata := map[string]string{"harness.canary.id": "canary-failed-assignment", "harness.canary.candidate": "true"}
	result, runErr := runtime.RunTurn(context.Background(), principal, session, TurnInput{
		RunID: "run-failed-assignment", Text: "hello", CompositionMetadata: metadata,
	}, nil)
	if runErr == nil {
		t.Fatal("missing profile unexpectedly succeeded")
	}
	if session.Version() != 4 {
		t.Fatalf("composition failure did not persist terminal events: result=%#v err=%v version=%d", result, runErr, session.Version())
	}
	metadata["harness.canary.id"] = "mutated"
	var start RunStartData
	if err := json.Unmarshal(session.Events()[0].Data, &start); err != nil {
		t.Fatal(err)
	}
	if start.Composition == nil || start.Composition.Metadata["harness.canary.id"] != "canary-failed-assignment" ||
		start.Composition.Metadata["harness.canary.candidate"] != "true" {
		t.Fatalf("failed composition lost detached assignment metadata: %#v", start.Composition)
	}
}

func TestRuntimeResumeCompositionFailurePersistsAssignmentMetadata(t *testing.T) {
	runtime, principal, session, approver, _ := durableApprovalFixture(t)
	if _, err := runtime.RunTurn(context.Background(), principal, session, TurnInput{
		RunID: "run-resume-assignment", Text: "release payment",
	}, nil); err == nil {
		// The run should suspend, not complete.
	}
	if status, exists := session.RunStatus("run-resume-assignment"); !exists || status != RunWaitingApproval {
		t.Fatalf("run did not suspend: status=%q exists=%t", status, exists)
	}
	approver.decide(ApprovalApproved)
	// Remove the selected capability before resume so composition fails before
	// the provider boundary. The Profile still names it, but the registry no
	// longer exposes an executable declaration.
	runtime.Capabilities = NewCapabilityRegistry()
	resumeMetadata := map[string]string{
		"harness.canary.id": "canary-resume-failure", "harness.canary.status": "paused",
	}
	result, err := runtime.ResumeTurn(context.Background(), principal, session, ResumeInput{
		RunID: "run-resume-assignment", CompositionMetadata: resumeMetadata,
	}, nil)
	if err == nil || result.Status != RunFailed {
		t.Fatalf("resume composition unexpectedly succeeded: %#v err=%v", result, err)
	}
	resumeMetadata["harness.canary.id"] = "mutated"
	var resumes []RunResumeData
	for _, event := range session.Events() {
		if event.Type != EvRunResume {
			continue
		}
		var data RunResumeData
		if err := json.Unmarshal(event.Data, &data); err != nil {
			t.Fatal(err)
		}
		resumes = append(resumes, data)
	}
	if len(resumes) != 1 || resumes[0].Composition == nil ||
		resumes[0].Composition.Metadata["harness.canary.id"] != "canary-resume-failure" ||
		resumes[0].Composition.Metadata["harness.canary.status"] != "paused" {
		t.Fatalf("failed resume did not persist detached assignment: %#v", resumes)
	}
}

func TestRuntimeContainsModelResolverPanic(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	profiles := NewAgentProfileRegistry()
	name := "Agent"
	model := ModelSelection{Provider: "panic", Model: "panic-1"}
	if err := profiles.Bind(AgentProfileLayer{Scope: product, ProfileID: "panic.agent", Name: &name, Model: &model}); err != nil {
		t.Fatal(err)
	}
	sessionScope, _ := user.Child(ScopeRef{Kind: ScopeSession, ID: "session-panic"})
	session, _ := NewSession(SessionOptions{ID: "session-panic", ProfileID: "panic.agent", Principal: principal, Scope: sessionScope})
	runtime := &Runtime{
		Capabilities: NewCapabilityRegistry(), Profiles: profiles,
		Models: ModelResolverFunc(func(context.Context, ModelSelection) (LlmAdapter, error) { panic("resolver panic") }),
	}
	result, err := runtime.RunTurn(context.Background(), principal, session, TurnInput{RunID: "run-resolver", Text: "hello"}, nil)
	if err == nil || result.Status != RunFailed {
		t.Fatalf("resolver panic was not contained: %#v %v", result, err)
	}
}

func TestRuntimeRejectsSameIdentityAtDifferentScope(t *testing.T) {
	_, product, tenant, user := testScopes()
	owner := testPrincipal(user)
	profiles := NewAgentProfileRegistry()
	name := "Agent"
	model := ModelSelection{Provider: "mock", Model: "mock-1"}
	if err := profiles.Bind(AgentProfileLayer{Scope: product, ProfileID: "scope.agent", Name: &name, Model: &model}); err != nil {
		t.Fatal(err)
	}
	sessionScope, _ := user.Child(ScopeRef{Kind: ScopeSession, ID: "session-scope"})
	session, _ := NewSession(SessionOptions{ID: "session-scope", ProfileID: "scope.agent", Principal: owner, Scope: sessionScope})
	wrongScope := owner
	wrongScope.Scope = tenant
	runtime := &Runtime{Capabilities: NewCapabilityRegistry(), Profiles: profiles, Models: ModelResolverFunc(func(context.Context, ModelSelection) (LlmAdapter, error) { return MockLlmAdapter{}, nil })}
	if _, err := runtime.RunTurn(context.Background(), wrongScope, session, TurnInput{RunID: "run-wrong-scope", Text: "hello"}, nil); err == nil {
		t.Fatal("same identity at a different scope accessed the session")
	}
}

func TestRunCompositionHashesModelRevision(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	profiles := NewAgentProfileRegistry()
	name := "Agent"
	model := ModelSelection{Provider: "revision", Model: "revision-1"}
	if err := profiles.Bind(AgentProfileLayer{Scope: product, ProfileID: "revision.agent", Name: &name, Model: &model}); err != nil {
		t.Fatal(err)
	}
	sessionScope, _ := user.Child(ScopeRef{Kind: ScopeSession, ID: "session-revision"})
	session, _ := NewSession(SessionOptions{ID: "session-revision", ProfileID: "revision.agent", Principal: principal, Scope: sessionScope})
	runtime := &Runtime{
		Capabilities: NewCapabilityRegistry(), Profiles: profiles,
		Models: ModelResolverFunc(func(context.Context, ModelSelection) (LlmAdapter, error) { return revisionLLM{}, nil }),
	}
	if _, err := runtime.RunTurn(context.Background(), principal, session, TurnInput{RunID: "run-revision", Text: "hello"}, nil); err != nil {
		t.Fatal(err)
	}
	var start RunStartData
	if err := json.Unmarshal(session.Events()[0].Data, &start); err != nil {
		t.Fatal(err)
	}
	if start.Composition == nil || !strings.HasPrefix(start.Composition.ModelRevision, "sha256:") {
		t.Fatalf("model revision fingerprint missing: %#v", start)
	}
	if strings.Contains(start.Composition.ModelRevision, "sensitive") {
		t.Fatal("raw model revision leaked")
	}
}

func TestRuntimePersistsDetachedCompositionMetadata(t *testing.T) {
	_, product, _, user := testScopes()
	principal := testPrincipal(user)
	profiles := NewAgentProfileRegistry()
	name := "Metadata Agent"
	model := ModelSelection{Provider: "mock", Model: "mock-1"}
	if err := profiles.Bind(AgentProfileLayer{
		Scope: product, ProfileID: "metadata.agent", Name: &name, Model: &model,
	}); err != nil {
		t.Fatal(err)
	}
	scope, _ := user.Child(ScopeRef{Kind: ScopeSession, ID: "session-composition-metadata"})
	session, _ := NewSession(SessionOptions{
		ID: "session-composition-metadata", ProfileID: "metadata.agent", Principal: principal, Scope: scope,
	})
	runtime := &Runtime{
		Capabilities: NewCapabilityRegistry(), Profiles: profiles,
		Models: ModelResolverFunc(func(context.Context, ModelSelection) (LlmAdapter, error) { return MockLlmAdapter{}, nil }),
	}
	metadata := map[string]string{"assignment": "candidate", "bucket": "42"}
	if _, err := runtime.RunTurn(context.Background(), principal, session, TurnInput{
		RunID: "run-composition-metadata", Text: "hello", CompositionMetadata: metadata,
	}, nil); err != nil {
		t.Fatal(err)
	}
	metadata["assignment"] = "mutated"
	var start RunStartData
	if err := json.Unmarshal(session.Events()[0].Data, &start); err != nil {
		t.Fatal(err)
	}
	if start.Composition == nil || start.Composition.Metadata["assignment"] != "candidate" || start.Composition.Metadata["bucket"] != "42" {
		t.Fatalf("composition metadata was not detached and persisted: %#v", start.Composition)
	}
	invalidScope, _ := user.Child(ScopeRef{Kind: ScopeSession, ID: "session-composition-invalid"})
	invalidSession, _ := NewSession(SessionOptions{
		ID: "session-composition-invalid", ProfileID: "metadata.agent", Principal: principal, Scope: invalidScope,
	})
	if _, err := runtime.RunTurn(context.Background(), principal, invalidSession, TurnInput{
		RunID: "run-composition-invalid", Text: "hello",
		CompositionMetadata: map[string]string{"bad": strings.Repeat("x", MaxRunCompositionMetadataValueBytes+1)},
	}, nil); err == nil || invalidSession.Version() != 0 {
		t.Fatalf("invalid composition metadata reached the event log: version=%d err=%v", invalidSession.Version(), err)
	}
}
