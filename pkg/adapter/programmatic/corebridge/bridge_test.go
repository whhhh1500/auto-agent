package corebridge

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/whhhh1500/auto-agent/pkg/app/programmatic"
	"github.com/whhhh1500/auto-agent/pkg/core"
)

func TestNewRequiresAcceptedRootInvocation(t *testing.T) {
	catalog, err := programmatic.Project([]core.SnapshotCapability{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = New(core.CapabilityRequest{}, catalog, map[string]string{"program.tool": strings.Repeat("0", 64)}, strings.Repeat("0", 64))
	if !errors.Is(err, core.ErrAcceptedInvocationRequired) {
		t.Fatalf("unaccepted bridge error=%v", err)
	}
}

func TestCanonicalArgsRejectsIntegerThatCoreWouldRound(t *testing.T) {
	_, _, err := canonicalArgs(map[string]any{"unsafe": int64(1<<53 + 1)})
	if !errors.Is(err, ErrInvalidBridgeCall) {
		t.Fatalf("unsafe integer error=%v", err)
	}
	args, encoded, err := canonicalArgs(map[string]any{"safe": int64(1<<53 - 1)})
	if err != nil || args["safe"] != float64(1<<53-1) || string(encoded) != `{"safe":9007199254740991}` {
		t.Fatalf("safe integer args=%#v encoded=%s error=%v", args, encoded, err)
	}
}

func TestOutputValidationRejectsTrailingJSONAndRoundedIntegerDecimal(t *testing.T) {
	descriptor := programmatic.Descriptor{OutputSchema: map[string]any{"type": "object", "required": []any{"ok"}, "properties": map[string]any{"ok": map[string]any{"type": "boolean"}}}, MaxOutputBytes: 1024}
	for _, content := range []string{`{"ok":true}]`, `{"ok":true} {"again":true}`, `{"ok":true,"ok":false}`} {
		if err := validateResult(descriptor, core.CapabilityResult{Content: content, OK: true}); !errors.Is(err, ErrToolRejected) {
			t.Fatalf("trailing content %q error=%v", content, err)
		}
	}
	if _, err := decodeJSONValue(`9007199254740993.0`); err == nil {
		t.Fatal("integer-valued decimal beyond exact float64 range was accepted")
	}
	for _, value := range []string{`1e-1000000000`, `0e-1000000000`} {
		if _, err := decodeJSONValue(value); err == nil {
			t.Fatalf("extreme exponent %s was accepted", value)
		}
	}
}

func TestBridgeReentersAcceptedGuardWithBoundDeterministicChild(t *testing.T) {
	var targetCalls atomic.Int32
	var targetCallID string
	root := bridgeRoot{manifest: core.CapabilityManifest{ID: "program.root", Version: "1", Name: "Program root", Kind: core.KindTool, Idempotent: true, Tool: &core.ToolExposure{Parameters: map[string]any{"type": "object"}}}}
	target := bridgeTarget{manifest: core.CapabilityManifest{
		ID: "program.target", Version: "1", Name: "Program target", Kind: core.KindTool, Idempotent: true,
		Metadata:     map[string]string{programmatic.ExposureKey: programmatic.ExposureVersion},
		Tool:         &core.ToolExposure{Parameters: map[string]any{"type": "object", "required": []any{"value"}, "additionalProperties": false, "properties": map[string]any{"value": map[string]any{"type": "string"}}}},
		OutputSchema: map[string]any{"type": "object", "required": []any{"ok"}, "properties": map[string]any{"ok": map[string]any{"type": "boolean"}}},
	}}
	target.calls, target.callID = &targetCalls, &targetCallID

	registry := core.NewCapabilityRegistry()
	product := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"}, core.ScopeRef{Kind: core.ScopeProduct, ID: "product"})
	user := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"}, core.ScopeRef{Kind: core.ScopeProduct, ID: "product"}, core.ScopeRef{Kind: core.ScopeUser, ID: "user"})
	principal := core.Principal{TenantID: "tenant", SubjectID: "subject", Scope: user, Grants: core.NewPermissionSet(core.PermRead)}
	if err := registry.Register(product, root); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(product, target); err != nil {
		t.Fatal(err)
	}
	sessionScope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"}, core.ScopeRef{Kind: core.ScopeProduct, ID: "product"}, core.ScopeRef{Kind: core.ScopeUser, ID: "user"}, core.ScopeRef{Kind: core.ScopeSession, ID: "bridge-session"})
	snapshot, err := (core.CapabilityResolver{Registry: registry}).Resolve(principal, sessionScope)
	if err != nil {
		t.Fatal(err)
	}
	session, err := core.NewSession(core.SessionOptions{ID: "bridge-session", ProfileID: "bridge-profile", Principal: principal, Scope: sessionScope})
	if err != nil {
		t.Fatal(err)
	}
	agent, err := core.NewAgent(core.AgentOptions{LLM: bridgeModel{}, Tools: snapshot, Session: session, ToolJournal: newBridgeJournal(), MaxSteps: 3})
	if err != nil {
		t.Fatal(err)
	}
	result, err := agent.RunTurn(context.Background(), core.TurnInput{RunID: "bridge-run", Text: "start"})
	if err != nil || result.Status != core.RunCompleted || targetCalls.Load() != 1 {
		t.Fatalf("result=%#v error=%v target_calls=%d", result, err, targetCalls.Load())
	}
	if !strings.HasPrefix(targetCallID, "root-call/") || len(targetCallID) > 256 {
		t.Fatalf("child call id=%q", targetCallID)
	}
}

type bridgeRoot struct{ manifest core.CapabilityManifest }

func (p bridgeRoot) Manifest() core.CapabilityManifest { return p.manifest }
func (bridgeRoot) Execute(ctx context.Context, request core.CapabilityRequest) (core.CapabilityResult, error) {
	metadata, ok := request.Context.Data.(func() []core.SnapshotCapability)
	if !ok {
		return core.CapabilityResult{}, errors.New("snapshot metadata is unavailable")
	}
	catalog, err := programmatic.Project(metadata())
	if err != nil {
		return core.CapabilityResult{}, err
	}
	tools := catalog.Descriptors()
	if len(tools) != 1 {
		return core.CapabilityResult{}, errors.New("unexpected program catalog")
	}
	bridge, err := New(request, catalog, map[string]string{tools[0].Schema.Name: tools[0].BindingDigest}, strings.Repeat("a", 64))
	if err != nil {
		return core.CapabilityResult{}, err
	}
	if _, err := bridge.CallTool(ctx, Call{CapabilityID: tools[0].Schema.Name, Ordinal: 1, Args: map[string]any{"value": "ok"}}); err != nil {
		return core.CapabilityResult{}, err
	}
	return core.CapabilityResult{Content: `{"complete":true}`, OK: true}, nil
}

type bridgeTarget struct {
	manifest core.CapabilityManifest
	calls    *atomic.Int32
	callID   *string
}

func (p bridgeTarget) Manifest() core.CapabilityManifest { return p.manifest }
func (p bridgeTarget) Execute(_ context.Context, request core.CapabilityRequest) (core.CapabilityResult, error) {
	if err := core.RequireAcceptedInvocation(request); err != nil {
		return core.CapabilityResult{}, err
	}
	p.calls.Add(1)
	*p.callID = request.CallID
	return core.CapabilityResult{Content: `{"ok":true}`, OK: true}, nil
}

type bridgeModel struct{}

func (bridgeModel) Provider() string { return "bridge-test" }
func (bridgeModel) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	if options.Messages[len(options.Messages)-1].Role == core.RoleTool {
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: "done"})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
		return nil
	}
	call := core.ToolCall{ID: "root-call", Name: "program.root", Args: map[string]any{}}
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &call})
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
	return nil
}

type bridgeJournal struct {
	mu      sync.Mutex
	records map[string]core.ToolInvocationRecord
}

func newBridgeJournal() *bridgeJournal {
	return &bridgeJournal{records: map[string]core.ToolInvocationRecord{}}
}
func bridgeJournalKey(value core.ToolInvocation) string {
	return value.SessionID + "\x00" + value.RunID + "\x00" + value.CallID
}
func (j *bridgeJournal) BeginToolInvocation(_ context.Context, invocation core.ToolInvocation) (core.ToolInvocationRecord, core.ToolInvocationDecision, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	key := bridgeJournalKey(invocation)
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
func (j *bridgeJournal) CompleteToolInvocation(_ context.Context, invocation core.ToolInvocation, result core.CapabilityResult) (core.ToolInvocationRecord, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	key := bridgeJournalKey(invocation)
	record, ok := j.records[key]
	if !ok || record.ToolInvocation != invocation {
		return core.ToolInvocationRecord{}, errors.New("journal conflict")
	}
	copyResult := result
	record.State, record.Result, record.CompletedAt, record.UpdatedAt = core.ToolInvocationCompleted, &copyResult, time.Now().UTC(), time.Now().UTC()
	j.records[key] = record
	return record, nil
}
func (j *bridgeJournal) MarkToolInvocationUncertain(_ context.Context, invocation core.ToolInvocation, code string) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	key := bridgeJournalKey(invocation)
	record, ok := j.records[key]
	if !ok || record.ToolInvocation != invocation {
		return errors.New("journal conflict")
	}
	record.State, record.ErrorCode, record.UpdatedAt = core.ToolInvocationUncertain, code, time.Now().UTC()
	j.records[key] = record
	return nil
}
