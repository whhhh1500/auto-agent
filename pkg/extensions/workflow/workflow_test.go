package workflow_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/extensions/workflow"
)

type workflowLLM struct {
	workflowID string
	args       map[string]any
}

type captureInvoker struct{ call core.ToolCall }

func (i *captureInvoker) InvokeTool(_ context.Context, call core.ToolCall) (core.CapabilityResult, error) {
	i.call = call
	return core.CapabilityResult{Content: `{}`, OK: true}, nil
}

func (l workflowLLM) Provider() string { return "workflow-test" }

func (l workflowLLM) Stream(_ context.Context, opts core.GenerateOptions, emit func(core.StreamChunk)) error {
	if opts.Messages[len(opts.Messages)-1].Role == core.RoleTool {
		emit(core.StreamChunk{Kind: "assistant", Text: "done"})
		emit(core.StreamChunk{Kind: "finish", FinishKind: "stop"})
		return nil
	}
	call := core.ToolCall{ID: "call-flow", Name: l.workflowID, Args: l.args}
	emit(core.StreamChunk{Kind: "assistant", ToolCall: &call})
	emit(core.StreamChunk{Kind: "finish", FinishKind: "tool-calls"})
	return nil
}

type protectedProbe struct {
	manifest    core.CapabilityManifest
	credential  core.CredentialRef
	calls       int
	sawDeadline bool
	sawSecret   bool
}

func (p *protectedProbe) Manifest() core.CapabilityManifest { return p.manifest }

func (p *protectedProbe) Execute(ctx context.Context, request core.CapabilityRequest) (core.CapabilityResult, error) {
	p.calls++
	_, p.sawDeadline = ctx.Deadline()
	if p.credential != "" {
		value, err := request.Context.Credentials.Resolve(ctx, p.credential)
		if err != nil {
			return core.CapabilityResult{}, err
		}
		p.sawSecret = value.Value == "workflow-secret"
	}
	encoded, err := json.Marshal(request.Args)
	if err != nil {
		return core.CapabilityResult{}, err
	}
	return core.CapabilityResult{Content: string(encoded), OK: true}, nil
}

type recordingLimiter struct{ calls []string }

func (l *recordingLimiter) AllowCall(_ context.Context, _, capabilityID string) bool {
	l.calls = append(l.calls, capabilityID)
	return true
}

func TestWorkflowStepsUseActiveRunGuardFunnel(t *testing.T) {
	product, user, principal := workflowScopes(t)
	credential := core.CredentialRef("workflow.secret")
	credentials := core.NewCredentialRegistry()
	if err := credentials.Bind(core.CredentialBinding{
		Scope: product, Ref: credential,
		Provider: core.StaticCredentialProvider{Value: "workflow-secret"},
	}); err != nil {
		t.Fatal(err)
	}

	childManifest := toolManifest("protected.child")
	childManifest.InputSchema = objectSchema(map[string]any{
		"value": map[string]any{"type": "string"},
	}, "value")
	childManifest.Tool.Parameters = childManifest.InputSchema
	childManifest.RequiredCredentials = []core.CredentialRef{credential}
	childManifest.RequiresApproval = true
	childManifest.PerTurnBudget = 1
	childManifest.TimeoutMs = 250
	child := &protectedProbe{manifest: childManifest, credential: credential}

	flow, err := workflow.NewCapability("protected.workflow", workflow.Definition{
		Steps: []workflow.Step{
			{Name: "first", CapabilityID: childManifest.ID, Args: map[string]any{"value": "one"}},
			{Name: "second", CapabilityID: childManifest.ID, Args: map[string]any{"value": "two"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	registry := core.NewCapabilityRegistry()
	if err := registry.Register(product, child); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(product, flow); err != nil {
		t.Fatal(err)
	}
	snapshot, err := (core.CapabilityResolver{Registry: registry, Credentials: credentials}).Resolve(principal, user)
	if err != nil {
		t.Fatal(err)
	}

	sessionScope, _ := user.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "workflow-session"})
	session, err := core.NewSession(core.SessionOptions{
		ID: "workflow-session", ProfileID: "workflow.profile", Principal: principal, Scope: sessionScope,
	})
	if err != nil {
		t.Fatal(err)
	}
	before := []string{}
	after := []string{}
	approvals := 0
	limiter := &recordingLimiter{}
	agent, err := core.NewAgent(core.AgentOptions{
		LLM: workflowLLM{workflowID: flow.Manifest().ID}, Tools: snapshot, Session: session,
		MaxSteps: 3, MaxToolCalls: 10, RateLimiter: limiter,
		Hooks: &core.RunHooksFuncs{
			OnBeforeToolFn: func(_ context.Context, _ core.RunInfo, call core.ToolCall) error {
				before = append(before, call.Name)
				return nil
			},
			OnAfterToolFn: func(_ context.Context, _ core.RunInfo, call core.ToolCall, _ core.CapabilityResult) {
				after = append(after, call.Name)
			},
		},
		Approver: core.ApproverFunc(func(_ context.Context, request core.ApprovalRequest) (core.ApprovalDecision, error) {
			if request.ToolCall.Name == childManifest.ID {
				approvals++
			}
			return core.ApprovalApproved, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := agent.RunTurn(context.Background(), core.TurnInput{RunID: "workflow-run", Text: "run workflow"})
	if err != nil || result.Status != core.RunCompleted {
		t.Fatalf("unexpected run result: %#v, %v", result, err)
	}

	if child.calls != 1 {
		t.Fatalf("per-turn child budget was bypassed: provider calls=%d", child.calls)
	}
	if !child.sawDeadline {
		t.Fatal("child capability did not receive its manifest timeout")
	}
	if !child.sawSecret {
		t.Fatal("child capability did not receive its declared credential")
	}
	if approvals != 1 {
		t.Fatalf("child approval count=%d, want 1", approvals)
	}
	if !contains(before, childManifest.ID) || !contains(after, childManifest.ID) {
		t.Fatalf("child hooks were bypassed: before=%v after=%v", before, after)
	}
	if !contains(limiter.calls, childManifest.ID) {
		t.Fatalf("child rate limiter was bypassed: %v", limiter.calls)
	}

	nestedCalls := map[string]bool{}
	nestedResults := map[string]core.ToolResultData{}
	for _, event := range session.Events() {
		switch event.Type {
		case core.EvToolCall:
			var data core.ToolCallData
			if json.Unmarshal(event.Data, &data) == nil {
				nestedCalls[data.CallID] = true
			}
		case core.EvToolResult:
			var data core.ToolResultData
			if json.Unmarshal(event.Data, &data) == nil {
				nestedResults[data.CallID] = data
			}
		}
	}
	if !nestedCalls["call-flow/first"] || !nestedCalls["call-flow/second"] {
		t.Fatalf("nested calls were not audited: %v", nestedCalls)
	}
	second := nestedResults["call-flow/second"]
	if second.OK || second.Metadata["code"] != core.CodeBudgetExceeded {
		t.Fatalf("second child call did not pass through budget guard: %#v", second)
	}
}

func TestWorkflowStepSchemaValidationCannotBeBypassed(t *testing.T) {
	product, user, principal := workflowScopes(t)
	manifest := toolManifest("schema.child")
	manifest.InputSchema = objectSchema(map[string]any{
		"value": map[string]any{"type": "string"},
	}, "value")
	manifest.Tool.Parameters = manifest.InputSchema
	child := &protectedProbe{manifest: manifest}
	flow, err := workflow.NewCapability("schema.workflow", workflow.Definition{
		Steps: []workflow.Step{{Name: "invalid", CapabilityID: manifest.ID}},
	})
	if err != nil {
		t.Fatal(err)
	}
	registry := core.NewCapabilityRegistry()
	if err := registry.Register(product, child); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(product, flow); err != nil {
		t.Fatal(err)
	}
	snapshot, err := (core.CapabilityResolver{Registry: registry}).Resolve(principal, user)
	if err != nil {
		t.Fatal(err)
	}
	sessionScope, _ := user.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "schema-session"})
	session, _ := core.NewSession(core.SessionOptions{
		ID: "schema-session", ProfileID: "workflow.profile", Principal: principal, Scope: sessionScope,
	})
	agent, err := core.NewAgent(core.AgentOptions{
		LLM: workflowLLM{workflowID: flow.Manifest().ID}, Tools: snapshot, Session: session,
		MaxSteps: 3, MaxToolCalls: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := agent.RunTurn(context.Background(), core.TurnInput{RunID: "schema-run", Text: "run"}); err != nil {
		t.Fatal(err)
	}
	if child.calls != 0 {
		t.Fatalf("invalid child args reached provider %d times", child.calls)
	}
	found := false
	for _, event := range session.Events() {
		if event.Type != core.EvToolResult {
			continue
		}
		var data core.ToolResultData
		if json.Unmarshal(event.Data, &data) == nil && data.CallID == "call-flow/invalid" {
			found = data.Metadata["code"] == core.CodeInvalidArgs
		}
	}
	if !found {
		t.Fatal("nested invalid arguments did not produce the guarded schema denial")
	}
}

func TestWorkflowStepsCountAgainstRunToolBudget(t *testing.T) {
	product, user, principal := workflowScopes(t)
	manifest := toolManifest("budget.child")
	child := &protectedProbe{manifest: manifest}
	flow, err := workflow.NewCapability("budget.workflow", workflow.Definition{
		Steps: []workflow.Step{{Name: "child", CapabilityID: manifest.ID}},
	})
	if err != nil {
		t.Fatal(err)
	}
	registry := core.NewCapabilityRegistry()
	if err := registry.Register(product, child); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(product, flow); err != nil {
		t.Fatal(err)
	}
	snapshot, err := (core.CapabilityResolver{Registry: registry}).Resolve(principal, user)
	if err != nil {
		t.Fatal(err)
	}
	sessionScope, _ := user.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "budget-session"})
	session, _ := core.NewSession(core.SessionOptions{
		ID: "budget-session", ProfileID: "workflow.profile", Principal: principal, Scope: sessionScope,
	})
	agent, err := core.NewAgent(core.AgentOptions{
		LLM: workflowLLM{workflowID: flow.Manifest().ID}, Tools: snapshot, Session: session,
		MaxSteps: 3, MaxToolCalls: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := agent.RunTurn(context.Background(), core.TurnInput{RunID: "budget-run", Text: "run"}); err != nil {
		t.Fatal(err)
	}
	if child.calls != 0 {
		t.Fatalf("nested child bypassed the run-wide tool budget: calls=%d", child.calls)
	}
	for _, event := range session.Events() {
		if event.Type != core.EvToolResult {
			continue
		}
		var data core.ToolResultData
		if json.Unmarshal(event.Data, &data) == nil && data.CallID == "call-flow/child" {
			if data.Metadata["code"] != core.CodeBudgetExceeded {
				t.Fatalf("unexpected nested budget result: %#v", data)
			}
			return
		}
	}
	t.Fatal("nested run-budget denial was not recorded")
}

func TestWorkflowFailsClosedWithoutProtectedInvoker(t *testing.T) {
	flow, err := workflow.NewCapability("closed.workflow", workflow.Definition{
		Steps: []workflow.Step{{Name: "step", CapabilityID: "some.tool"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := flow.Execute(context.Background(), core.CapabilityRequest{CallID: "direct"})
	if err != nil {
		t.Fatal(err)
	}
	if result.OK || result.Metadata["code"] != "workflow_invoker_unavailable" {
		t.Fatalf("direct workflow execution did not fail closed: %#v", result)
	}
}

func TestWorkflowDefinitionIsFrozenAtConstruction(t *testing.T) {
	args := map[string]any{"symbol": "BTC"}
	schema := map[string]any{"type": "object", "properties": map[string]any{"query": map[string]any{"type": "string"}}}
	flow, err := workflow.NewCapability("frozen.workflow", workflow.Definition{
		InputSchema: schema,
		Steps:       []workflow.Step{{Name: "child", CapabilityID: "child.tool", Args: args}},
	})
	if err != nil {
		t.Fatal(err)
	}
	args["symbol"] = "MUTATED"
	schema["type"] = "array"
	manifest := flow.Manifest()
	manifest.Tool.Parameters["type"] = "string"
	second := flow.Manifest()
	invoker := &captureInvoker{}
	result, err := flow.Execute(context.Background(), core.CapabilityRequest{
		CallID: "call-flow", Context: core.CapabilityContext{Invoker: invoker},
	})
	if err != nil || !result.OK {
		t.Fatalf("workflow execution failed: %#v %v", result, err)
	}
	if second.Tool.Parameters["type"] != "object" || invoker.call.Args["symbol"] != "BTC" {
		t.Fatalf("workflow definition remained mutable: manifest=%#v args=%#v", second, invoker.call.Args)
	}
}

func workflowScopes(t *testing.T) (core.ScopePath, core.ScopePath, core.Principal) {
	t.Helper()
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product, _ := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "product"})
	tenant, _ := product.Child(core.ScopeRef{Kind: core.ScopeTenant, ID: "tenant"})
	user, _ := tenant.Child(core.ScopeRef{Kind: core.ScopeUser, ID: "user"})
	principal := core.Principal{
		SubjectID: "user", TenantID: "tenant", Scope: user,
		Grants: core.NewPermissionSet(core.PermRead, core.PermWrite),
	}
	return product, user, principal
}

func toolManifest(id string) core.CapabilityManifest {
	return core.CapabilityManifest{
		ID: id, Version: "1.0.0", Name: id, Kind: core.KindTool,
		Contract: "harness.tool/v1", RequiredPermissions: []core.Permission{core.PermRead},
		Tool: &core.ToolExposure{},
	}
}

func objectSchema(properties map[string]any, required ...string) map[string]any {
	schema := map[string]any{"type": "object", "properties": properties}
	if len(required) > 0 {
		values := make([]any, len(required))
		for i, name := range required {
			values[i] = name
		}
		schema["required"] = values
	}
	return schema
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func TestWorkflowRejectsInvalidStepNames(t *testing.T) {
	for _, name := range []string{"", " spaced", "has/slash", "has\\slash", "bad\nname", strings.Repeat("s", 65)} {
		if _, err := workflow.NewCapability("flow.steps", workflow.Definition{
			Steps: []workflow.Step{{Name: name, CapabilityID: "some.tool"}},
		}); err == nil {
			t.Fatalf("invalid step name %q was accepted", name)
		}
	}
}
