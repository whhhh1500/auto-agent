package subagent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

type blackBoxDelegationModel struct {
	calls    atomic.Int32
	toolName string
}

func (m *blackBoxDelegationModel) Provider() string { return "subagent-blackbox" }

func (m *blackBoxDelegationModel) Stream(ctx context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	m.calls.Add(1)
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	for _, message := range options.Messages {
		if message.Role == core.RoleTool {
			emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: "child complete"})
			emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
			return nil
		}
	}
	toolName := m.toolName
	if toolName == "" {
		toolName = "child.side_effect"
	}
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &core.ToolCall{ID: "child-call", Name: toolName, Args: map[string]any{"value": "once"}}})
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
	return nil
}

type blackBoxDelegationTool struct {
	calls            atomic.Int32
	requiresApproval bool
	id               string
}

func (t *blackBoxDelegationTool) Manifest() core.CapabilityManifest {
	id := t.id
	if id == "" {
		id = "child.side_effect"
	}
	return core.CapabilityManifest{ID: id, Version: "1.0.0", Name: id, Kind: core.KindTool, RequiresApproval: t.requiresApproval, Tool: &core.ToolExposure{Parameters: map[string]any{"type": "object"}}}
}

type pendingDelegationApprover struct {
	approved atomic.Bool
	requests atomic.Int32
}

func (a *pendingDelegationApprover) RequestApproval(context.Context, core.ApprovalRequest) (core.ApprovalResolution, error) {
	a.requests.Add(1)
	decision := core.ApprovalPending
	if a.approved.Load() {
		decision = core.ApprovalApproved
	}
	return core.ApprovalResolution{ApprovalID: "apr_0123456789abcdef0123456789abcdef", Decision: decision, RequestedAt: time.Now().UTC(), ExpiresAt: time.Now().UTC().Add(time.Minute), DecidedAt: func() time.Time {
		if decision == core.ApprovalApproved {
			return time.Now().UTC()
		}
		return time.Time{}
	}()}, nil
}

func (*pendingDelegationApprover) Approve(context.Context, core.ApprovalRequest) (core.ApprovalDecision, error) {
	return core.ApprovalDenied, nil
}

func (t *blackBoxDelegationTool) Execute(ctx context.Context, request core.CapabilityRequest) (core.CapabilityResult, error) {
	if err := ctx.Err(); err != nil {
		return core.CapabilityResult{}, err
	}
	t.calls.Add(1)
	return core.CapabilityResult{Content: "side effect complete", OK: true}, nil
}

// parentChildApprovalModel uses the tool surface to select the parent or
// child behavior. It exercises the real guarded Runtime boundary instead of
// calling Capability.Execute directly.
type parentChildApprovalModel struct{}

func (parentChildApprovalModel) Provider() string { return "subagent-parent-child-approval" }

func (parentChildApprovalModel) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	parent := false
	for _, schema := range options.Tools {
		if schema.Name == "agent.delegate" {
			parent = true
			break
		}
	}
	for _, message := range options.Messages {
		if message.Role == core.RoleTool {
			text := "child complete"
			if parent {
				text = "parent complete"
			}
			emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: text})
			emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
			return nil
		}
	}
	call := core.ToolCall{ID: "child-call", Name: "child.approval", Args: map[string]any{"value": "once"}}
	if parent {
		call = core.ToolCall{ID: "parent-delegate", Name: "agent.delegate", Args: map[string]any{"prompt": "approval-gated child"}}
	}
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &call})
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
	return nil
}

// approvalDelegationJournal models the durable journal decision relevant to
// an approval pause: only an idempotent started call may execute again.
type approvalDelegationJournal struct {
	records map[string]core.ToolInvocationRecord
}

func delegationJournalKey(invocation core.ToolInvocation) string {
	return invocation.SessionID + "\x00" + invocation.RunID + "\x00" + invocation.CallID
}

func (j *approvalDelegationJournal) BeginToolInvocation(_ context.Context, invocation core.ToolInvocation) (core.ToolInvocationRecord, core.ToolInvocationDecision, error) {
	if j.records == nil {
		j.records = map[string]core.ToolInvocationRecord{}
	}
	key := delegationJournalKey(invocation)
	if record, found := j.records[key]; found {
		if record.State == core.ToolInvocationCompleted {
			return core.CloneToolInvocationRecord(record), core.ToolInvocationReplay, nil
		}
		if invocation.Idempotent {
			return core.CloneToolInvocationRecord(record), core.ToolInvocationExecuteRetry, nil
		}
		return core.CloneToolInvocationRecord(record), core.ToolInvocationUnknown, nil
	}
	now := time.Now().UTC()
	record := core.ToolInvocationRecord{ToolInvocation: invocation, State: core.ToolInvocationStarted, StartedAt: now, UpdatedAt: now}
	j.records[key] = record
	return core.CloneToolInvocationRecord(record), core.ToolInvocationExecuteNew, nil
}

func (j *approvalDelegationJournal) CompleteToolInvocation(_ context.Context, invocation core.ToolInvocation, result core.CapabilityResult) (core.ToolInvocationRecord, error) {
	record := j.records[delegationJournalKey(invocation)]
	canonical := result
	record.State, record.Result, record.UpdatedAt = core.ToolInvocationCompleted, &canonical, time.Now().UTC()
	j.records[delegationJournalKey(invocation)] = record
	return core.CloneToolInvocationRecord(record), nil
}

func (j *approvalDelegationJournal) MarkToolInvocationUncertain(_ context.Context, invocation core.ToolInvocation, errorCode string) error {
	record := j.records[delegationJournalKey(invocation)]
	record.State, record.ErrorCode, record.UpdatedAt = core.ToolInvocationUncertain, errorCode, time.Now().UTC()
	j.records[delegationJournalKey(invocation)] = record
	return nil
}

type blackBoxDelegationFixture struct {
	runtime   *core.Runtime
	model     *blackBoxDelegationModel
	tool      *blackBoxDelegationTool
	sessions  *core.MemorySessionStore
	links     *MemoryDelegationLinkStore
	principal core.Principal
}

func newBlackBoxDelegationFixture(t *testing.T) blackBoxDelegationFixture {
	t.Helper()
	product := core.MustScopePath(
		core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"},
		core.ScopeRef{Kind: core.ScopeProduct, ID: "product"},
	)
	principalScope := core.MustScopePath(
		core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"},
		core.ScopeRef{Kind: core.ScopeProduct, ID: "product"},
		core.ScopeRef{Kind: core.ScopeTenant, ID: "tenant-a"},
		core.ScopeRef{Kind: core.ScopeUser, ID: "user-a"},
	)
	model := &blackBoxDelegationModel{}
	tool := &blackBoxDelegationTool{}
	profiles := core.NewAgentProfileRegistry()
	name := "child"
	selection := core.ModelSelection{Provider: model.Provider(), Model: "deterministic"}
	if err := profiles.Bind(core.AgentProfileLayer{Scope: product, ProfileID: "product.child", Name: &name, Model: &selection, AddCapabilities: []string{"child.side_effect"}}); err != nil {
		t.Fatal(err)
	}
	capabilities := core.NewCapabilityRegistry()
	if err := capabilities.Register(product, tool); err != nil {
		t.Fatal(err)
	}
	runtime := &core.Runtime{
		Capabilities: capabilities,
		Profiles:     profiles,
		Models:       core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) { return model, nil }),
	}
	return blackBoxDelegationFixture{
		runtime: runtime, model: model, tool: tool,
		sessions: core.NewMemorySessionStore(), links: NewMemoryDelegationLinkStore(),
		principal: core.Principal{SubjectID: "user-a", TenantID: "tenant-a", Scope: principalScope, Grants: core.NewPermissionSet(core.PermRead)},
	}
}

func blackBoxRequest(f blackBoxDelegationFixture, prompt string) core.CapabilityRequest {
	return core.CapabilityRequest{
		Context: core.CapabilityContext{
			Principal: f.principal, RemainingToolCalls: 8,
			Invocation: core.Invocation{SessionID: "sess-parent", RunID: "run-parent", CallID: "call-delegate"},
		},
		Args: map[string]any{"prompt": prompt},
	}
}

func metadataString(t *testing.T, result core.CapabilityResult, key string) string {
	t.Helper()
	value, ok := result.Metadata[key].(string)
	if !ok || value == "" {
		t.Fatalf("metadata %s missing: %#v", key, result.Metadata)
	}
	return value
}

func TestNewCapabilityRejectsInvalidProfileID(t *testing.T) {
	runtime := &core.Runtime{}
	if _, err := NewCapability(runtime, "agent.helper", Options{}); err == nil {
		t.Fatal("empty child profile id was accepted")
	}
	if _, err := NewCapability(runtime, "agent.helper", Options{ProfileID: "nodot"}); err == nil {
		t.Fatal("non-namespaced child profile id was accepted")
	}
	if _, err := NewCapability(nil, "agent.helper", Options{ProfileID: "product.child"}); err == nil {
		t.Fatal("nil runtime was accepted")
	}
}

func TestExecuteRequiresSessionIDAndFollowupTogether(t *testing.T) {
	capability, err := NewCapability(&core.Runtime{}, "agent.helper", Options{ProfileID: "product.child"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	request := core.CapabilityRequest{Args: map[string]any{"session_id": "sess_existing", "prompt": "replace"}}
	result, err := capability.Execute(ctx, request)
	if err != nil || result.OK || result.Metadata["code"] != core.CodeInvalidArgs {
		t.Fatalf("session_id without followup must be denied: %#v err=%v", result, err)
	}
	if !strings.Contains(result.Content, "session_id and followup") {
		t.Fatalf("unexpected denial: %q", result.Content)
	}

	result, err = capability.Execute(ctx, core.CapabilityRequest{Args: map[string]any{"followup": "continue"}})
	if err != nil || result.OK || result.Metadata["code"] != core.CodeInvalidArgs {
		t.Fatalf("followup without session_id must be denied: %#v err=%v", result, err)
	}

	result, err = capability.Execute(ctx, core.CapabilityRequest{
		Args: map[string]any{"session_id": "sess id", "followup": "continue"},
	})
	if err != nil || result.OK || result.Metadata["code"] != core.CodeInvalidArgs {
		t.Fatalf("invalid session_id must be denied: %#v err=%v", result, err)
	}
}

func TestExecuteEvictsOldestChild(t *testing.T) {
	capability, err := NewCapability(&core.Runtime{}, "agent.helper", Options{
		ProfileID: "product.child", MaxChildren: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	user := core.MustScopePath(
		core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"},
		core.ScopeRef{Kind: core.ScopeProduct, ID: "product"},
		core.ScopeRef{Kind: core.ScopeTenant, ID: "tenant-a"},
		core.ScopeRef{Kind: core.ScopeUser, ID: "user-a"},
	)
	request := core.CapabilityRequest{
		Context: core.CapabilityContext{Principal: core.Principal{
			SubjectID: "user-a", TenantID: "tenant-a", Scope: user,
			Grants: core.NewPermissionSet(core.PermRead),
		}},
		Args: map[string]any{"prompt": "first"},
	}
	first, err := capability.Execute(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	firstID, _ := first.Metadata["child_session_id"].(string)
	if firstID == "" {
		t.Fatalf("first child id missing: %#v", first)
	}
	request.Args["prompt"] = "second"
	if _, err := capability.Execute(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	continued, err := capability.Execute(context.Background(), core.CapabilityRequest{
		Context: request.Context,
		Args:    map[string]any{"session_id": firstID, "followup": "again"},
	})
	if err != nil || continued.OK || continued.Metadata["code"] != "child_session_unavailable" {
		t.Fatalf("evicted child remained continuable: %#v err=%v", continued, err)
	}
}

func TestDurableDelegationReplaysOneChildAcrossCapabilityRestart(t *testing.T) {
	f := newBlackBoxDelegationFixture(t)
	firstCap, err := NewCapability(f.runtime, "agent.delegate", Options{ProfileID: "product.child", Sessions: f.sessions, Links: f.links})
	if err != nil {
		t.Fatal(err)
	}
	first, err := firstCap.Execute(context.Background(), blackBoxRequest(f, "delegate once"))
	if err != nil || !first.OK {
		t.Fatalf("initial delegation failed: %#v err=%v", first, err)
	}
	childSessionID := metadataString(t, first, "child_session_id")
	childRunID := metadataString(t, first, "child_run_id")
	replayRequest := blackBoxRequest(f, "a changed replay prompt must not execute")
	replay, err := firstCap.Execute(context.Background(), replayRequest)
	if err != nil || !replay.OK {
		t.Fatalf("replay failed: %#v err=%v", replay, err)
	}
	if metadataString(t, replay, "child_session_id") != childSessionID || metadataString(t, replay, "child_run_id") != childRunID {
		t.Fatalf("replay returned different child: first=%#v replay=%#v", first.Metadata, replay.Metadata)
	}
	if got := f.model.calls.Load(); got != 2 {
		t.Fatalf("replay invoked model %d times, want one child turn (2 model calls)", got)
	}
	if got := f.tool.calls.Load(); got != 1 {
		t.Fatalf("replay invoked side effect %d times, want 1", got)
	}

	// A fresh Capability instance must use the shared stores rather than an
	// in-process child map to continue the same durable child.
	restarted, err := NewCapability(f.runtime, "agent.delegate", Options{ProfileID: "product.child", Sessions: f.sessions, Links: f.links})
	if err != nil {
		t.Fatal(err)
	}
	continuedRequest := core.CapabilityRequest{Context: core.CapabilityContext{Principal: f.principal, RemainingToolCalls: 8}, Args: map[string]any{"session_id": childSessionID, "followup": "continue after restart"}}
	continued, err := restarted.Execute(context.Background(), continuedRequest)
	if err != nil || !continued.OK || metadataString(t, continued, "child_session_id") != childSessionID {
		t.Fatalf("durable continuation failed: %#v err=%v", continued, err)
	}
	if metadataString(t, continued, "child_run_id") == childRunID {
		t.Fatalf("explicit continuation reused the original terminal run: %#v", continued.Metadata)
	}
	if _, err := f.sessions.Load(context.Background(), childSessionID); err != nil {
		t.Fatalf("child session was not durable: %v", err)
	}
}

func TestDurableContinuationRejectsCrossTenantAndScope(t *testing.T) {
	f := newBlackBoxDelegationFixture(t)
	capability, err := NewCapability(f.runtime, "agent.delegate", Options{ProfileID: "product.child", Sessions: f.sessions, Links: f.links})
	if err != nil {
		t.Fatal(err)
	}
	first, err := capability.Execute(context.Background(), blackBoxRequest(f, "make durable child"))
	if err != nil || !first.OK {
		t.Fatalf("initial delegation failed: %#v err=%v", first, err)
	}
	childID := metadataString(t, first, "child_session_id")

	otherTenant := f.principal
	otherTenant.TenantID = "tenant-b"
	otherTenant.Scope = core.MustScopePath(
		core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"}, core.ScopeRef{Kind: core.ScopeProduct, ID: "product"},
		core.ScopeRef{Kind: core.ScopeTenant, ID: "tenant-b"}, core.ScopeRef{Kind: core.ScopeUser, ID: "user-a"},
	)
	denied, err := capability.Execute(context.Background(), core.CapabilityRequest{Context: core.CapabilityContext{Principal: otherTenant, RemainingToolCalls: 8}, Args: map[string]any{"session_id": childID, "followup": "cross tenant"}})
	if err != nil || denied.OK || denied.Metadata["code"] != "child_session_forbidden" {
		t.Fatalf("cross-tenant continuation was not denied: %#v err=%v", denied, err)
	}

	sibling := f.principal
	sibling.Scope = core.MustScopePath(
		core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"}, core.ScopeRef{Kind: core.ScopeProduct, ID: "product"},
		core.ScopeRef{Kind: core.ScopeTenant, ID: "tenant-a"}, core.ScopeRef{Kind: core.ScopeUser, ID: "user-b"},
	)
	denied, err = capability.Execute(context.Background(), core.CapabilityRequest{Context: core.CapabilityContext{Principal: sibling, RemainingToolCalls: 8}, Args: map[string]any{"session_id": childID, "followup": "sibling scope"}})
	if err != nil || denied.OK || denied.Metadata["code"] != "child_session_forbidden" {
		t.Fatalf("sibling-scope continuation was not denied: %#v err=%v", denied, err)
	}
}

func TestDurableDelegationConcurrentReplayCreatesOneChild(t *testing.T) {
	f := newBlackBoxDelegationFixture(t)
	capability, err := NewCapability(f.runtime, "agent.delegate", Options{ProfileID: "product.child", Sessions: f.sessions, Links: f.links})
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := NewCapability(f.runtime, "agent.delegate", Options{ProfileID: "product.child", Sessions: f.sessions, Links: f.links})
	if err != nil {
		t.Fatal(err)
	}
	const callers = 20
	results := make([]core.CapabilityResult, callers)
	errs := make([]error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			current := capability
			if index%2 == 1 {
				current = restarted
			}
			results[index], errs[index] = current.Execute(context.Background(), blackBoxRequest(f, fmt.Sprintf("concurrent-%d", index)))
		}(i)
	}
	wg.Wait()
	childIDs := map[string]bool{}
	for i := range results {
		if errs[i] != nil || !results[i].OK {
			t.Fatalf("concurrent delegation %d failed: %#v err=%v", i, results[i], errs[i])
		}
		childIDs[metadataString(t, results[i], "child_session_id")] = true
	}
	if len(childIDs) != 1 || f.tool.calls.Load() != 1 {
		t.Fatalf("concurrent replay created duplicate child/effect: children=%v effects=%d models=%d", childIDs, f.tool.calls.Load(), f.model.calls.Load())
	}
}

func TestDurableDelegationFailsClosedForMissingInvocationAndStaleLink(t *testing.T) {
	f := newBlackBoxDelegationFixture(t)
	capability, err := NewCapability(f.runtime, "agent.delegate", Options{ProfileID: "product.child", Sessions: f.sessions, Links: f.links})
	if err != nil {
		t.Fatal(err)
	}
	missing, err := capability.Execute(context.Background(), core.CapabilityRequest{Context: core.CapabilityContext{Principal: f.principal, RemainingToolCalls: 8}, Args: map[string]any{"prompt": "must not use legacy path"}})
	if err != nil || missing.OK || missing.Metadata["code"] != "delegation_invocation_missing" {
		t.Fatalf("missing invocation was not fail-closed: %#v err=%v", missing, err)
	}
	request := blackBoxRequest(f, "stale link")
	key := request.Context.Invocation.SessionID + "\x00" + request.Context.Invocation.RunID + "\x00" + request.Context.Invocation.CallID
	childID := delegationID("sess_", key)
	childRunID := delegationID("run_", key)
	if _, _, err := f.links.PutIfAbsent(context.Background(), Link{ParentSessionID: request.Context.Invocation.SessionID, ParentRunID: request.Context.Invocation.RunID, ParentCallID: request.Context.Invocation.CallID, ChildSessionID: childID, ChildRunID: childRunID, TenantID: f.principal.TenantID, SubjectID: f.principal.SubjectID, Depth: 1}); err != nil {
		t.Fatal(err)
	}
	stale, err := capability.Execute(context.Background(), request)
	if err != nil || stale.OK || stale.Metadata["code"] != "child_session_unavailable" {
		t.Fatalf("stale link was not fail-closed: %#v err=%v", stale, err)
	}
	if got := f.model.calls.Load(); got != 0 {
		t.Fatalf("stale link unexpectedly ran model %d times", got)
	}
}

type failingDelegationSessionStore struct {
	*core.MemorySessionStore
	fail atomic.Bool
}

type corruptDelegationLinkStore struct{ link Link }

func (s corruptDelegationLinkStore) Get(context.Context, string) (Link, bool, error) {
	return s.link, true, nil
}
func (s corruptDelegationLinkStore) GetByChild(context.Context, string) (Link, bool, error) {
	return s.link, true, nil
}
func (s corruptDelegationLinkStore) PutIfAbsent(context.Context, Link) (Link, bool, error) {
	return s.link, false, nil
}

func (s *failingDelegationSessionStore) Save(ctx context.Context, session *core.Session, expectedVersion int64) error {
	if s.fail.Load() {
		return errors.New("injected session save failure")
	}
	return s.MemorySessionStore.Save(ctx, session, expectedVersion)
}

func TestDurableDelegationSessionSaveFailureNeverReturnsSuccess(t *testing.T) {
	f := newBlackBoxDelegationFixture(t)
	sessions := &failingDelegationSessionStore{MemorySessionStore: core.NewMemorySessionStore()}
	sessions.fail.Store(true)
	capability, err := NewCapability(f.runtime, "agent.delegate", Options{ProfileID: "product.child", Sessions: sessions, Links: f.links})
	if err != nil {
		t.Fatal(err)
	}
	result, err := capability.Execute(context.Background(), blackBoxRequest(f, "persistence failure"))
	if err != nil || result.OK || result.Metadata["code"] != "delegation_store_unavailable" {
		t.Fatalf("save failure returned pseudo-success: %#v err=%v", result, err)
	}
	if _, found, err := f.links.Get(context.Background(), blackBoxRequest(f, "").Context.Invocation.SessionID+"\x00run-parent\x00call-delegate"); err != nil || !found {
		t.Fatalf("expected durable link for safe retry: found=%t err=%v", found, err)
	}
}

func TestDurableDelegationRejectsCorruptLink(t *testing.T) {
	f := newBlackBoxDelegationFixture(t)
	links := corruptDelegationLinkStore{link: Link{ParentSessionID: "sess-parent", ParentRunID: "run-parent", ParentCallID: "call-delegate", TenantID: f.principal.TenantID, SubjectID: f.principal.SubjectID, Depth: 1}}
	capability, err := NewCapability(f.runtime, "agent.delegate", Options{ProfileID: "product.child", Sessions: f.sessions, Links: links})
	if err != nil {
		t.Fatal(err)
	}
	result, err := capability.Execute(context.Background(), blackBoxRequest(f, "corrupt link"))
	if err != nil || result.OK || result.Metadata["code"] == nil {
		t.Fatalf("corrupt link was not denied: %#v err=%v", result, err)
	}
	if got := f.model.calls.Load(); got != 0 {
		t.Fatalf("corrupt link unexpectedly ran model %d times", got)
	}
}

type cancellingDelegationModel struct {
	started chan struct{}
}

func (m *cancellingDelegationModel) Provider() string { return "subagent-cancel" }
func (m *cancellingDelegationModel) Stream(ctx context.Context, _ core.GenerateOptions, _ func(core.StreamChunk)) error {
	select {
	case <-m.started:
	default:
		close(m.started)
	}
	<-ctx.Done()
	return ctx.Err()
}

func TestDurableDelegationContextCancelFailsClosedAndLeavesRetryableState(t *testing.T) {
	f := newBlackBoxDelegationFixture(t)
	cancelling := &cancellingDelegationModel{started: make(chan struct{})}
	f.runtime.Models = core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) { return cancelling, nil })
	capability, err := NewCapability(f.runtime, "agent.delegate", Options{ProfileID: "product.child", Sessions: f.sessions, Links: f.links})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	resultCh := make(chan core.CapabilityResult, 1)
	go func() {
		result, _ := capability.Execute(ctx, blackBoxRequest(f, "cancel me"))
		resultCh <- result
	}()
	select {
	case <-cancelling.started:
		cancel()
	case <-time.After(time.Second):
		cancel()
		t.Fatal("child model did not start")
	}
	result := <-resultCh
	if result.OK || result.Metadata["code"] != "delegation_run_failed" {
		t.Fatalf("cancel returned pseudo-success: %#v", result)
	}
	key := "sess-parent\x00run-parent\x00call-delegate"
	link, found, err := f.links.Get(context.Background(), key)
	if err != nil || !found {
		t.Fatalf("cancel lost durable link: found=%t err=%v", found, err)
	}
	child, err := f.sessions.Load(context.Background(), link.ChildSessionID)
	if err != nil {
		t.Fatal(err)
	}
	if status, ok := child.RunStatus(link.ChildRunID); !ok || status == core.RunCompleted {
		t.Fatalf("cancel produced invalid terminal state status=%q exists=%t", status, ok)
	}
}

func TestDurableDelegationQuotaAndDepthBoundaries(t *testing.T) {
	f := newBlackBoxDelegationFixture(t)
	product := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"}, core.ScopeRef{Kind: core.ScopeProduct, ID: "product"})
	name := "child min"
	maxCalls := 1
	selection := core.ModelSelection{Provider: f.model.Provider(), Model: "deterministic"}
	if err := f.runtime.Profiles.Bind(core.AgentProfileLayer{Scope: product, ProfileID: "product.child.min", Name: &name, Model: &selection, MaxToolCalls: &maxCalls, AddCapabilities: []string{"child.side_effect"}}); err != nil {
		t.Fatal(err)
	}
	limited, err := NewCapability(f.runtime, "agent.delegate", Options{ProfileID: "product.child.min", Sessions: f.sessions, Links: f.links, DelegationMaxToolCalls: 2})
	if err != nil {
		t.Fatal(err)
	}
	request := blackBoxRequest(f, "quota")
	request.Context.RemainingToolCalls = 7
	result, err := limited.Execute(context.Background(), request)
	if err != nil || !result.OK {
		t.Fatalf("limited delegation failed: %#v err=%v", result, err)
	}
	childID := metadataString(t, result, "child_session_id")
	child, err := f.sessions.Load(context.Background(), childID)
	if err != nil {
		t.Fatal(err)
	}
	var starts int
	for _, event := range child.Events() {
		if event.Type != core.EvRunStart {
			continue
		}
		var start core.RunStartData
		if json.Unmarshal(event.Data, &start) == nil {
			starts++
			if start.Composition == nil || start.Composition.MaxToolCalls != 1 {
				t.Fatalf("delegation quota was not propagated: %#v", start.Composition)
			}
		}
	}
	if starts != 1 {
		t.Fatalf("run start count=%d, want 1", starts)
	}
	if _, err := NewCapability(f.runtime, "agent.invalid", Options{ProfileID: "product.child", DelegationMaxToolCalls: -1}); err == nil {
		t.Fatal("negative delegation max tool calls was accepted")
	}
	if _, err := NewCapability(f.runtime, "agent.invalid", Options{ProfileID: "product.child", DelegationMaxToolCalls: core.HardMaxToolCalls + 1}); err == nil {
		t.Fatal("over-limit delegation max tool calls was accepted")
	}
	for _, remaining := range []int{0, -1} {
		request := blackBoxRequest(f, fmt.Sprintf("remaining-%d", remaining))
		request.Context.Invocation.CallID = fmt.Sprintf("call-%d", remaining)
		request.Context.RemainingToolCalls = remaining
		denied, err := limited.Execute(context.Background(), request)
		if err != nil || denied.OK || denied.Metadata["code"] != core.CodeBudgetExceeded {
			t.Fatalf("remaining=%d was not budget-denied: %#v err=%v", remaining, denied, err)
		}
	}
	deep, err := NewCapability(f.runtime, "agent.deep", Options{ProfileID: "product.child", MaxDepth: 1, Sessions: f.sessions, Links: f.links})
	if err != nil {
		t.Fatal(err)
	}
	deepRequest := blackBoxRequest(f, "depth")
	deepRequest.Context.Principal.Attributes = map[string]string{AttributeDepth: "1"}
	denied, err := deep.Execute(context.Background(), deepRequest)
	if err != nil || denied.OK || denied.Metadata["code"] != "agent_depth_exceeded" {
		t.Fatalf("depth limit was not enforced: %#v err=%v", denied, err)
	}
}

func TestDurableDelegationApprovalWaitsAndResumesSameChildRun(t *testing.T) {
	f := newBlackBoxDelegationFixture(t)
	approvalTool := &blackBoxDelegationTool{requiresApproval: true, id: "child.approval"}
	product := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"}, core.ScopeRef{Kind: core.ScopeProduct, ID: "product"})
	if err := f.runtime.Capabilities.Register(product, approvalTool); err != nil {
		t.Fatal(err)
	}
	f.model.toolName = "child.approval"
	// Replace the profile allow-list with the approval-capable tool while
	// retaining the same durable stores and model resolver.
	name := "child"
	selection := core.ModelSelection{Provider: f.model.Provider(), Model: "deterministic"}
	if err := f.runtime.Profiles.Bind(core.AgentProfileLayer{Scope: product, ProfileID: "product.approval", Name: &name, Model: &selection, AddCapabilities: []string{"child.approval"}}); err != nil {
		t.Fatal(err)
	}
	approver := &pendingDelegationApprover{}
	f.runtime.Approver = approver
	capability, err := NewCapability(f.runtime, "agent.delegate", Options{ProfileID: "product.approval", Sessions: f.sessions, Links: f.links})
	if err != nil {
		t.Fatal(err)
	}
	request := blackBoxRequest(f, "approval-gated child")
	first, err := capability.Execute(context.Background(), request)
	pending, isPending := core.IsApprovalPending(err)
	if !isPending || first.OK || pending.Resolution.Decision != core.ApprovalPending {
		t.Fatalf("child approval did not suspend its parent call: %#v err=%v", first, err)
	}
	link, found, linkErr := f.links.Get(context.Background(), request.Context.Invocation.SessionID+"\x00"+request.Context.Invocation.RunID+"\x00"+request.Context.Invocation.CallID)
	if linkErr != nil || !found {
		t.Fatalf("child link missing after approval pause: found=%t err=%v", found, linkErr)
	}
	childID := link.ChildSessionID
	childRunID := link.ChildRunID
	waiting, err := f.sessions.Load(context.Background(), childID)
	if err != nil {
		t.Fatal(err)
	}
	if status, ok := waiting.RunStatus(childRunID); !ok || status != core.RunWaitingApproval {
		t.Fatalf("child status=%q exists=%t, want waiting approval", status, ok)
	}
	replay, err := capability.Execute(context.Background(), request)
	pending, isPending = core.IsApprovalPending(err)
	if !isPending || replay.OK || pending.Resolution.ApprovalID == "" {
		t.Fatalf("unresolved approval replay did not keep the parent suspended: %#v err=%v", replay, err)
	}
	if got := approver.requests.Load(); got != 2 {
		t.Fatalf("expected approval lookup on initial and replay, got %d", got)
	}
	approver.approved.Store(true)
	resumed, err := capability.Execute(context.Background(), request)
	if err != nil || !resumed.OK || metadataString(t, resumed, "child_run_id") != childRunID {
		t.Fatalf("approved child did not resume same run: %#v err=%v", resumed, err)
	}
	if got := approvalTool.calls.Load(); got != 1 {
		t.Fatalf("approved child side effect calls=%d, want 1", got)
	}
}

func TestDurableChildApprovalPausesAndResumesSameParentRun(t *testing.T) {
	f := newBlackBoxDelegationFixture(t)
	product := core.MustScopePath(
		core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"},
		core.ScopeRef{Kind: core.ScopeProduct, ID: "product"},
	)
	model := parentChildApprovalModel{}
	selection := core.ModelSelection{Provider: model.Provider(), Model: "deterministic"}
	childName, parentName := "approval child", "approval parent"
	if err := f.runtime.Profiles.Bind(core.AgentProfileLayer{
		Scope: product, ProfileID: "product.approval.child", Name: &childName, Model: &selection,
		AddCapabilities: []string{"child.approval"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := f.runtime.Profiles.Bind(core.AgentProfileLayer{
		Scope: product, ProfileID: "product.approval.parent", Name: &parentName, Model: &selection,
		AddCapabilities: []string{"agent.delegate"},
	}); err != nil {
		t.Fatal(err)
	}
	approvalTool := &blackBoxDelegationTool{requiresApproval: true, id: "child.approval"}
	if err := f.runtime.Capabilities.Register(product, approvalTool); err != nil {
		t.Fatal(err)
	}
	delegate, err := NewCapability(f.runtime, "agent.delegate", Options{
		ProfileID: "product.approval.child", Sessions: f.sessions, Links: f.links,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.runtime.Capabilities.Register(product, delegate); err != nil {
		t.Fatal(err)
	}
	approver := &pendingDelegationApprover{}
	f.runtime.Approver = approver
	f.runtime.ToolJournal = &approvalDelegationJournal{}
	f.runtime.Models = core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) {
		return model, nil
	})
	parentID := "sess-parent-approval"
	parentScope, err := f.principal.Scope.Child(core.ScopeRef{Kind: core.ScopeSession, ID: parentID})
	if err != nil {
		t.Fatal(err)
	}
	parent, err := core.NewSession(core.SessionOptions{
		ID: parentID, ProfileID: "product.approval.parent", Principal: f.principal, Scope: parentScope,
	})
	if err != nil {
		t.Fatal(err)
	}
	const parentRunID = "run-parent-approval"
	first, err := f.runtime.RunTurn(context.Background(), f.principal, parent, core.TurnInput{
		RunID: parentRunID, Text: "delegate with approval",
	}, nil)
	if err != nil || first.Status != core.RunWaitingApproval {
		t.Fatalf("parent did not pause for child approval: %#v err=%v", first, err)
	}
	pending, found, err := parent.PendingApproval(parentRunID)
	if err != nil || !found || pending.ResumeCall.Name != "agent.delegate" || pending.ToolCall.Name != "child.approval" {
		t.Fatalf("parent approval checkpoint did not preserve child/parent lineage: %#v found=%t err=%v", pending, found, err)
	}
	link, found, err := f.links.Get(context.Background(), parentID+"\x00"+parentRunID+"\x00parent-delegate")
	if err != nil || !found {
		t.Fatalf("child link missing: found=%t err=%v", found, err)
	}
	child, err := f.sessions.Load(context.Background(), link.ChildSessionID)
	if err != nil {
		t.Fatal(err)
	}
	if status, exists := child.RunStatus(link.ChildRunID); !exists || status != core.RunWaitingApproval {
		t.Fatalf("child did not retain its original approval run: status=%q exists=%t", status, exists)
	}
	approver.approved.Store(true)
	resumed, err := f.runtime.ResumeTurn(context.Background(), f.principal, parent, core.ResumeInput{RunID: parentRunID}, nil)
	if err != nil || resumed.Status != core.RunCompleted || resumed.RunID != parentRunID || resumed.Answer != "parent complete" {
		t.Fatalf("parent did not resume same run after child approval: %#v err=%v", resumed, err)
	}
	if got := approvalTool.calls.Load(); got != 1 {
		t.Fatalf("approved child side effect calls=%d, want 1", got)
	}
}
