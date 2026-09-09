package toolcapability

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	access "github.com/whhhh1500/auto-agent/pkg/app/programmatic"
	core "github.com/whhhh1500/auto-agent/pkg/core"
	ptc "github.com/whhhh1500/auto-agent/pkg/execution/programmatic"
	"github.com/whhhh1500/auto-agent/pkg/extensions/workflow"
)

func TestProgramExecuteUsesGuardedEnvelopeDataForLoopAndBranch(t *testing.T) {
	searchCalls, detailCalls := &atomic.Int32{}, &atomic.Int32{}
	search := &programTool{manifest: programManifest("catalog.search", false), calls: searchCalls, content: `[{"id":"off","active":false},{"id":"on","active":true}]`}
	detail := &programTool{manifest: programManifest("catalog.detail", false), calls: detailCalls, content: `{"id":"on","price":42}`}
	fixture := newProgramFixture(t, search, detail)
	if _, err := fixture.snapshot.Execute(context.Background(), core.ToolCall{ID: "direct", Name: search.manifest.ID}); !errors.Is(err, core.ErrAcceptedInvocationRequired) {
		t.Fatalf("direct provider bypass error=%v", err)
	}
	source := `{"version":"ptc-ir/v1","body":[` +
		`{"op":"call","assign":"rows","tool":"catalog.search","args":{"op":"map","entries":{}}},` +
		`{"op":"assign","name":"active","value":{"op":"list","items":[]}},` +
		`{"op":"for","var":"row","in":{"op":"get","object":{"op":"var","name":"rows"},"key":"data"},"body":[{"op":"if","cond":{"op":"cmp","kind":"eq","left":{"op":"get","object":{"op":"var","name":"row"},"key":"active"},"right":{"op":"literal","value":true}},"then":[{"op":"append","target":"active","value":{"op":"var","name":"row"}}]}]},` +
		`{"op":"if","cond":{"op":"cmp","kind":"gt","left":{"op":"len","value":{"op":"var","name":"active"}},"right":{"op":"literal","value":0}},"then":[{"op":"call","assign":"detail","tool":"catalog.detail","args":{"op":"map","entries":{"id":{"op":"get","object":{"op":"index","object":{"op":"var","name":"active"},"index":{"op":"literal","value":0}},"key":"id"}}}}],"else":[{"op":"assign","name":"detail","value":{"op":"literal","value":null}}]},` +
		`{"op":"return","value":{"op":"get","object":{"op":"var","name":"detail"},"key":"data"}}]}`
	bindings := fixture.bindings(t, "catalog.search", "catalog.detail")
	result, err := fixture.run(t, source, bindings)
	if err != nil || result.Status != core.RunCompleted || searchCalls.Load() != 1 || detailCalls.Load() != 1 {
		t.Fatalf("result=%#v err=%v search=%d detail=%d tool=%#v", result, err, searchCalls.Load(), detailCalls.Load(), fixture.resultData(t, "program-call"))
	}
	output := fixture.result(t, "program-call")
	value := output["value"].(map[string]any)
	if value["id"] != "on" || value["price"] != float64(42) {
		t.Fatalf("program output=%#v", output)
	}
	if !fixture.hasChildPrefix("program-call/") {
		t.Fatal("program children were not recorded through the guarded invoker")
	}
}

func TestProgramExecuteRejectsBindingDriftBeforeEffect(t *testing.T) {
	calls := &atomic.Int32{}
	target := &programTool{manifest: programManifest("catalog.lookup", false), calls: calls, content: `{"ok":true}`}
	fixture := newProgramFixture(t, target)
	source := `{"version":"ptc-ir/v1","body":[{"op":"call","tool":"catalog.lookup","args":{"op":"map","entries":{}}}]}`
	bindings := fixture.bindings(t, "catalog.lookup")
	bindings["catalog.lookup"] = strings.Repeat("0", 64)
	result, err := fixture.run(t, source, bindings)
	if err != nil || result.Status != core.RunCompleted || calls.Load() != 0 {
		t.Fatalf("result=%#v err=%v calls=%d", result, err, calls.Load())
	}
	data := fixture.resultData(t, "program-call")
	if data.OK || data.Metadata["code"] != "program_bindings_mismatch" {
		t.Fatalf("binding drift result=%#v", data)
	}
}

func TestProgramExecuteRejectsUnsafeToolJSON(t *testing.T) {
	calls := &atomic.Int32{}
	target := &programTool{manifest: programManifest("catalog.ambiguous", false), calls: calls, content: `{"first":true,"first":false}`}
	fixture := newProgramFixture(t, target)
	source := `{"version":"ptc-ir/v1","body":[{"op":"call","tool":"catalog.ambiguous","args":{"op":"map","entries":{}}}]}`
	result, err := fixture.run(t, source, fixture.bindings(t, "catalog.ambiguous"))
	if err != nil || result.Status != core.RunCompleted || calls.Load() != 1 {
		t.Fatalf("result=%#v err=%v calls=%d", result, err, calls.Load())
	}
	data := fixture.resultData(t, "program-call")
	if data.OK || data.Metadata["code"] != "program_tool_data_invalid" {
		t.Fatalf("unsafe provider JSON result=%#v", data)
	}
}

func TestProgramExecuteClassifiesCompileFailure(t *testing.T) {
	fixture := newProgramFixture(t)
	result, err := fixture.run(t, `{"version":"not-ptc","body":[]}`, map[string]string{})
	if err != nil || result.Status != core.RunCompleted {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	data := fixture.resultData(t, "program-call")
	if data.OK || data.Metadata["code"] != "program_invalid" || data.Metadata["diagnostic"] != "compile_version_invalid" || !strings.Contains(data.Content, "compile_version_invalid") {
		t.Fatalf("compile diagnostic=%#v", data)
	}
}

func TestProgramExecuteReturnsFixedDiagnosticWithoutLeakingProgramData(t *testing.T) {
	calls := &atomic.Int32{}
	target := &programTool{manifest: programManifest("catalog.private_probe", false), calls: calls, content: `{"public":true,"provider_secret":"tool-secret-sentinel"}`}
	fixture := newProgramFixture(t, target)
	source := `{"version":"ptc-ir/v1","body":[` +
		`{"op":"call","assign":"outcome","tool":"catalog.private_probe","args":{"op":"map","entries":{"argument_secret":{"op":"literal","value":"argument-secret-sentinel"}}}},` +
		`{"op":"return","value":{"op":"get","object":{"op":"get","object":{"op":"var","name":"outcome"},"key":"data"},"key":"missing-source-secret"}}]}`
	result, err := fixture.run(t, source, fixture.bindings(t, "catalog.private_probe"))
	if err != nil || result.Status != core.RunCompleted || calls.Load() != 1 {
		t.Fatalf("result=%#v err=%v calls=%d", result, err, calls.Load())
	}
	data := fixture.resultData(t, "program-call")
	if data.OK || data.Metadata["code"] != "program_invalid" || data.Metadata["diagnostic"] != "runtime_get_missing_key" {
		t.Fatalf("diagnostic result=%#v", data)
	}
	for _, secret := range []string{"tool-secret-sentinel", "argument-secret-sentinel", "missing-source-secret", "catalog.private_probe"} {
		if strings.Contains(data.Content, secret) {
			t.Fatalf("diagnostic leaked %q in %q", secret, data.Content)
		}
	}
	if !strings.Contains(data.Content, "Prior child effects may already have occurred") || !fixture.hasChildPrefix("program-call/") {
		t.Fatalf("diagnostic did not preserve effect warning/child identity: %#v", data)
	}
}

func TestProgramExecutePropagatesBridgeOriginErrorsWithoutDiagnosticRewrite(t *testing.T) {
	previousProgram, err := ptc.Compile([]byte(`{"version":"ptc-ir/v1","body":[{"op":"return","value":{"op":"var","name":"missing"}}]}`))
	if err != nil {
		t.Fatal(err)
	}
	_, priorVMError := previousProgram.Run(context.Background(), nil, func(context.Context, ptc.Call) (any, error) { return nil, nil }, ptc.Limits{})
	if priorVMError == nil {
		t.Fatal("expected prior VM error")
	}
	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "host sentinel", err: ptc.ErrInvalidProgram},
		{name: "previous VM error", err: priorVMError},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := &atomic.Int32{}
			target := &programTool{manifest: programManifest("catalog.host_error", false), calls: calls, err: test.err}
			fixture := newProgramFixture(t, target)
			source := `{"version":"ptc-ir/v1","body":[{"op":"call","tool":"catalog.host_error","args":{"op":"map","entries":{}}}]}`
			result, runErr := fixture.run(t, source, fixture.bindings(t, "catalog.host_error"))
			if runErr != nil || result.Status != core.RunCompleted || calls.Load() != 1 {
				t.Fatalf("result=%#v err=%v calls=%d", result, runErr, calls.Load())
			}
			data := fixture.resultData(t, "program-call")
			if data.OK || data.Metadata["code"] == "program_invalid" || strings.Contains(data.Content, "runtime_") || strings.Contains(data.Content, "compile_invalid") {
				t.Fatalf("bridge error was rewritten as a program diagnostic: %#v", data)
			}
		})
	}
}

func TestProgramExecuteValidatesParentCallIDBeforeChildEffects(t *testing.T) {
	for _, test := range []struct {
		name  string
		bytes int
		ok    bool
	}{
		{name: "191 byte parent remains representable", bytes: 191, ok: true},
		{name: "192 byte parent is rejected before child", bytes: 192},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := &atomic.Int32{}
			target := &programTool{manifest: programManifest("catalog.parentlimit", false), calls: calls, content: `{"ok":true}`}
			fixture := newProgramFixture(t, target)
			source := `{"version":"ptc-ir/v1","body":[{"op":"call","tool":"catalog.parentlimit","args":{"op":"map","entries":{}}}]}`
			call := executeCall(source, fixture.bindings(t, "catalog.parentlimit"))
			call.ID = strings.Repeat("p", test.bytes)
			fixture.model.call = call
			result, err := fixture.agent.RunTurn(context.Background(), core.TurnInput{RunID: "program-run", Text: "run"})
			if err != nil || result.Status != core.RunCompleted {
				t.Fatalf("result=%#v err=%v", result, err)
			}
			data := fixture.resultData(t, call.ID)
			if test.ok {
				if !data.OK || calls.Load() != 1 {
					t.Fatalf("191-byte parent result=%#v calls=%d", data, calls.Load())
				}
				return
			}
			if data.OK || data.Metadata["code"] != "program_invocation_invalid" || calls.Load() != 0 {
				t.Fatalf("192-byte parent result=%#v calls=%d", data, calls.Load())
			}
		})
	}
}

func TestProgramExecuteApprovalResumeRunsChildOnce(t *testing.T) {
	calls := &atomic.Int32{}
	target := &programTool{manifest: programManifest("catalog.approved", true), calls: calls, content: `{"ok":true}`}
	fixture := newProgramFixture(t, target)
	approver := &programApprover{}
	fixture.agent, _ = core.NewAgent(core.AgentOptions{LLM: fixture.model, Tools: fixture.snapshot, Session: fixture.session, ToolJournal: fixture.journal, Approver: approver, MaxSteps: 3, MaxToolCalls: 16})
	source := `{"version":"ptc-ir/v1","body":[{"op":"call","assign":"value","tool":"catalog.approved","args":{"op":"map","entries":{}}},{"op":"return","value":{"op":"var","name":"value"}}]}`
	bindings := fixture.bindings(t, "catalog.approved")
	fixture.model.call = executeCall(source, bindings)
	first, err := fixture.agent.RunTurn(context.Background(), core.TurnInput{RunID: "program-run", Text: "run"})
	if err != nil || first.Status != core.RunWaitingApproval || calls.Load() != 0 {
		t.Fatalf("first=%#v err=%v calls=%d tool=%#v", first, err, calls.Load(), fixture.resultData(t, "program-call"))
	}
	approver.approved.Store(true)
	resumed, err := fixture.agent.ResumeTurn(context.Background(), "program-run")
	if err != nil || resumed.Status != core.RunCompleted || calls.Load() != 1 {
		t.Fatalf("resumed=%#v err=%v calls=%d", resumed, err, calls.Load())
	}
}

func TestCatalogReturnsOnlyPublicDescriptorAndBothCapabilitiesRequireAcceptance(t *testing.T) {
	catalog, err := NewCatalogCapability(CatalogID)
	if err != nil {
		t.Fatal(err)
	}
	execute, err := NewExecuteCapability(ExecuteID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.Execute(context.Background(), core.CapabilityRequest{}); !errors.Is(err, core.ErrAcceptedInvocationRequired) {
		t.Fatalf("catalog direct error=%v", err)
	}
	if _, err := execute.Execute(context.Background(), core.CapabilityRequest{}); !errors.Is(err, core.ErrAcceptedInvocationRequired) {
		t.Fatalf("execute direct error=%v", err)
	}
	if _, marked := catalog.Manifest().Metadata[access.ExposureKey]; marked {
		t.Fatal("program.catalog must not be a programmatic target")
	}
	if !strings.Contains(execute.Manifest().Tool.Description, "program.catalog") || strings.Contains(execute.Manifest().Tool.Description, ptc.LanguageGuide) {
		t.Fatal("program.execute description did not provide concise catalog guidance")
	}
	if !strings.Contains(execute.Manifest().Tool.Description, "do not assume retry is safe") || !strings.Contains(ptc.LanguageGuide, "only input is predefined") || !strings.Contains(ptc.LanguageGuide, "data\nin a tool result envelope may be null") || !strings.Contains(ptc.LanguageGuide, "bare JSON array or object") {
		t.Fatal("program diagnostic repair guidance is incomplete")
	}
	for name, capability := range map[string]interface{ ArtifactRevision() string }{
		"catalog": catalog,
		"execute": execute,
	} {
		if got := capability.ArtifactRevision(); got != "programmatic-extension/v6-container-expression-guidance" {
			t.Fatalf("%s artifact revision=%q", name, got)
		}
	}
}

func TestCatalogProjectsOnlyPublicBoundDescriptor(t *testing.T) {
	target := &programTool{manifest: programManifest("catalog.public", false), calls: &atomic.Int32{}, content: `{"ok":true}`}
	fixture := newProgramFixture(t, target)
	fixture.model.call = core.ToolCall{ID: "catalog-call", Name: CatalogID, Args: map[string]any{}}
	result, err := fixture.agent.RunTurn(context.Background(), core.TurnInput{RunID: "catalog-run", Text: "catalog"})
	if err != nil || result.Status != core.RunCompleted {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	output := fixture.result(t, "catalog-call")
	if output["version"] != ptc.Version || output["language"] != ptc.LanguageGuide || !strings.Contains(output["language"].(string), "bare JSON array or object") {
		t.Fatalf("catalog grammar=%#v", output)
	}
	tools, ok := output["tools"].([]any)
	if !ok || len(tools) != 1 {
		t.Fatalf("catalog output=%#v", output)
	}
	descriptor, ok := tools[0].(map[string]any)
	if !ok || descriptor["capability_version"] != "1" || descriptor["binding_digest"] != fixture.bindings(t, "catalog.public")["catalog.public"] {
		t.Fatalf("public descriptor=%#v", tools[0])
	}
	if _, leaked := descriptor["metadata"]; leaked {
		t.Fatalf("catalog leaked manifest metadata: %#v", descriptor)
	}
}

func TestProgramExecuteCallsOptedInWorkflowThroughProtectedIdentities(t *testing.T) {
	innerCalls := &atomic.Int32{}
	inner := &programTool{manifest: core.CapabilityManifest{ID: "workflow.inner", Version: "1", Name: "workflow.inner", Kind: core.KindTool, Idempotent: true,
		Tool: &core.ToolExposure{Parameters: map[string]any{"type": "object", "additionalProperties": false}}}, calls: innerCalls, content: `{"answer":"from-workflow"}`}
	flow, err := workflow.NewCapability("workflow.checked", workflow.Definition{
		Description: "Run the checked inner action.", InputSchema: map[string]any{"type": "object", "additionalProperties": false},
		Steps: []workflow.Step{{Name: "checked", CapabilityID: "workflow.inner", Args: map[string]any{}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	fixture := newProgramFixtureCapabilities(t, inner, markedProgramCapability{Capability: flow})
	source := `{"version":"ptc-ir/v1","body":[{"op":"call","assign":"result","tool":"workflow.checked","args":{"op":"map","entries":{}}},{"op":"return","value":{"op":"get","object":{"op":"var","name":"result"},"key":"data"}}]}`
	result, err := fixture.run(t, source, fixture.bindings(t, "workflow.checked"))
	if err != nil || result.Status != core.RunCompleted || innerCalls.Load() != 1 {
		t.Fatalf("result=%#v err=%v inner=%d", result, err, innerCalls.Load())
	}
	value := fixture.result(t, "program-call")["value"].(map[string]any)
	if value["answer"] != "from-workflow" || !fixture.hasChildPrefix("program-call/") {
		t.Fatalf("workflow program output=%#v", value)
	}
}

type programFixture struct {
	t            *testing.T
	snapshot     *core.CapabilitySnapshot
	session      *core.Session
	agent        *core.Agent
	journal      *programJournal
	model        *programModel
	capabilities []core.SnapshotCapability
}

func newProgramFixture(t *testing.T, tools ...*programTool) *programFixture {
	capabilities := make([]core.Capability, len(tools))
	for index := range tools {
		capabilities[index] = tools[index]
	}
	return newProgramFixtureCapabilities(t, capabilities...)
}

func newProgramFixtureCapabilities(t *testing.T, capabilities ...core.Capability) *programFixture {
	t.Helper()
	catalog, _ := NewCatalogCapability(CatalogID)
	execute, _ := NewExecuteCapability(ExecuteID)
	registry := core.NewCapabilityRegistry()
	product := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"}, core.ScopeRef{Kind: core.ScopeProduct, ID: "product"})
	user := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"}, core.ScopeRef{Kind: core.ScopeProduct, ID: "product"}, core.ScopeRef{Kind: core.ScopeUser, ID: "user"})
	principal := core.Principal{TenantID: "tenant", SubjectID: "subject", Scope: user, Grants: core.NewPermissionSet(core.PermRead)}
	for _, capability := range append([]core.Capability{catalog, execute}, capabilities...) {
		if err := registry.Register(product, capability); err != nil {
			t.Fatal(err)
		}
	}
	sessionScope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"}, core.ScopeRef{Kind: core.ScopeProduct, ID: "product"}, core.ScopeRef{Kind: core.ScopeUser, ID: "user"}, core.ScopeRef{Kind: core.ScopeSession, ID: "program-session"})
	snapshot, err := (core.CapabilityResolver{Registry: registry}).Resolve(principal, sessionScope)
	if err != nil {
		t.Fatal(err)
	}
	session, err := core.NewSession(core.SessionOptions{ID: "program-session", ProfileID: "program-profile", Principal: principal, Scope: sessionScope})
	if err != nil {
		t.Fatal(err)
	}
	model := &programModel{}
	journal := newProgramJournal()
	agent, err := core.NewAgent(core.AgentOptions{LLM: model, Tools: snapshot, Session: session, ToolJournal: journal, MaxSteps: 3, MaxToolCalls: 16})
	if err != nil {
		t.Fatal(err)
	}
	return &programFixture{t: t, snapshot: snapshot, session: session, agent: agent, journal: journal, model: model, capabilities: snapshot.Capabilities()}
}

type markedProgramCapability struct{ core.Capability }

func (m markedProgramCapability) Manifest() core.CapabilityManifest {
	manifest := m.Capability.Manifest()
	manifest.Metadata = map[string]string{access.ExposureKey: access.ExposureVersion}
	return manifest
}

func (f *programFixture) bindings(t *testing.T, ids ...string) map[string]string {
	t.Helper()
	catalog, err := access.Project(f.capabilities)
	if err != nil {
		t.Fatal(err)
	}
	all := catalog.Descriptors()
	bindings := map[string]string{}
	for _, id := range ids {
		for _, descriptor := range all {
			if descriptor.Schema.Name == id {
				bindings[id] = descriptor.BindingDigest
			}
		}
	}
	return bindings
}

func (f *programFixture) run(t *testing.T, source string, bindings map[string]string) (core.TurnResult, error) {
	t.Helper()
	f.model.call = executeCall(source, bindings)
	return f.agent.RunTurn(context.Background(), core.TurnInput{RunID: "program-run", Text: "run"})
}

func (f *programFixture) result(t *testing.T, callID string) map[string]any {
	t.Helper()
	data := f.resultData(t, callID)
	if !data.OK {
		t.Fatalf("program result=%#v", data)
	}
	var output map[string]any
	if err := json.Unmarshal([]byte(data.Content), &output); err != nil {
		t.Fatal(err)
	}
	return output
}

func (f *programFixture) resultData(t *testing.T, callID string) core.ToolResultData {
	t.Helper()
	for _, event := range f.session.Events() {
		if event.Type != core.EvToolResult {
			continue
		}
		var data core.ToolResultData
		if json.Unmarshal(event.Data, &data) == nil && data.CallID == callID {
			return data
		}
	}
	t.Fatalf("missing tool result %q", callID)
	return core.ToolResultData{}
}

func (f *programFixture) hasChildPrefix(prefix string) bool {
	for _, event := range f.session.Events() {
		if event.Type != core.EvToolCall {
			continue
		}
		var call core.ToolCallData
		if json.Unmarshal(event.Data, &call) == nil && strings.HasPrefix(call.CallID, prefix) {
			return true
		}
	}
	return false
}

func programManifest(id string, approval bool) core.CapabilityManifest {
	return core.CapabilityManifest{ID: id, Version: "1", Name: id, Kind: core.KindTool, Idempotent: true, RequiresApproval: approval,
		Metadata: map[string]string{access.ExposureKey: access.ExposureVersion},
		Tool:     &core.ToolExposure{Parameters: map[string]any{"type": "object", "additionalProperties": true}},
	}
}

type programTool struct {
	manifest core.CapabilityManifest
	calls    *atomic.Int32
	content  string
	err      error
}

func (p *programTool) Manifest() core.CapabilityManifest { return p.manifest }
func (p *programTool) Execute(_ context.Context, request core.CapabilityRequest) (core.CapabilityResult, error) {
	if err := core.RequireAcceptedInvocation(request); err != nil {
		return core.CapabilityResult{}, err
	}
	p.calls.Add(1)
	if p.err != nil {
		return core.CapabilityResult{}, p.err
	}
	return core.CapabilityResult{Content: p.content, OK: true}, nil
}

func executeCall(source string, bindings map[string]string) core.ToolCall {
	raw := map[string]any{}
	for id, digest := range bindings {
		raw[id] = digest
	}
	return core.ToolCall{ID: "program-call", Name: ExecuteID, Args: map[string]any{"source": source, "bindings": raw}}
}

type programModel struct{ call core.ToolCall }

func (*programModel) Provider() string { return "program-test" }
func (m *programModel) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	if options.Messages[len(options.Messages)-1].Role == core.RoleTool {
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: "done"})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
		return nil
	}
	call := m.call
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &call})
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
	return nil
}

type programJournal struct {
	mu      sync.Mutex
	records map[string]core.ToolInvocationRecord
}

func newProgramJournal() *programJournal {
	return &programJournal{records: map[string]core.ToolInvocationRecord{}}
}
func journalKey(value core.ToolInvocation) string {
	return value.SessionID + "\x00" + value.RunID + "\x00" + value.CallID
}
func (j *programJournal) BeginToolInvocation(_ context.Context, invocation core.ToolInvocation) (core.ToolInvocationRecord, core.ToolInvocationDecision, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	key := journalKey(invocation)
	if record, exists := j.records[key]; exists {
		if record.ToolInvocation != invocation {
			return record, core.ToolInvocationConflict, nil
		}
		if record.State == core.ToolInvocationCompleted {
			return record, core.ToolInvocationReplay, nil
		}
		return record, core.ToolInvocationExecuteRetry, nil
	}
	now := time.Now().UTC()
	record := core.ToolInvocationRecord{ToolInvocation: invocation, State: core.ToolInvocationStarted, StartedAt: now, UpdatedAt: now}
	j.records[key] = record
	return record, core.ToolInvocationExecuteNew, nil
}
func (j *programJournal) CompleteToolInvocation(_ context.Context, invocation core.ToolInvocation, result core.CapabilityResult) (core.ToolInvocationRecord, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	key := journalKey(invocation)
	record, ok := j.records[key]
	if !ok || record.ToolInvocation != invocation {
		return core.ToolInvocationRecord{}, errors.New("journal conflict")
	}
	copyResult := result
	record.State, record.Result, record.CompletedAt, record.UpdatedAt = core.ToolInvocationCompleted, &copyResult, time.Now().UTC(), time.Now().UTC()
	j.records[key] = record
	return record, nil
}
func (j *programJournal) MarkToolInvocationUncertain(_ context.Context, invocation core.ToolInvocation, code string) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	key := journalKey(invocation)
	record, ok := j.records[key]
	if !ok || record.ToolInvocation != invocation {
		return errors.New("journal conflict")
	}
	record.State, record.ErrorCode, record.UpdatedAt = core.ToolInvocationUncertain, code, time.Now().UTC()
	j.records[key] = record
	return nil
}

type programApprover struct{ approved atomic.Bool }

func (p *programApprover) Approve(context.Context, core.ApprovalRequest) (core.ApprovalDecision, error) {
	if p.approved.Load() {
		return core.ApprovalApproved, nil
	}
	return core.ApprovalDenied, nil
}

func (p *programApprover) RequestApproval(context.Context, core.ApprovalRequest) (core.ApprovalResolution, error) {
	if p.approved.Load() {
		return core.ApprovalResolution{ApprovalID: "apr_0123456789abcdef0123456789abcdef", Decision: core.ApprovalApproved}, nil
	}
	return core.ApprovalResolution{ApprovalID: "apr_0123456789abcdef0123456789abcdef", Decision: core.ApprovalPending}, nil
}
