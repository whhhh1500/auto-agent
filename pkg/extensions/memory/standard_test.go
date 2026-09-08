package memory

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

func TestNewStandardCapabilitiesManifestsAreSeparatedAndClosed(t *testing.T) {
	capabilities, err := NewStandardCapabilities(NewSliceStore())
	if err != nil {
		t.Fatal(err)
	}
	if len(capabilities) != 3 {
		t.Fatalf("capabilities=%d", len(capabilities))
	}
	want := map[string]struct {
		permission core.Permission
		idempotent bool
		approval   bool
		required   []string
	}{
		RecallCapabilityID:   {permission: core.PermRead, idempotent: true, required: []string{"query"}},
		RememberCapabilityID: {permission: core.PermWrite, required: []string{"key", "content"}},
		ForgetCapabilityID:   {permission: core.PermWrite, approval: true, required: []string{"id"}},
	}
	for _, capability := range capabilities {
		manifest := capability.Manifest()
		expected, ok := want[manifest.ID]
		if !ok || len(manifest.RequiredPermissions) != 1 || manifest.RequiredPermissions[0] != expected.permission || manifest.Idempotent != expected.idempotent || manifest.RequiresApproval != expected.approval {
			t.Fatalf("manifest=%#v", manifest)
		}
		parameters := manifest.Tool.Parameters
		if parameters["additionalProperties"] != false || !sameRequired(parameters["required"], expected.required) {
			t.Fatalf("schema for %s=%#v", manifest.ID, parameters)
		}
	}
}

func TestStandardCapabilitiesRequireAcceptedInvocationAndLegacyCompatibility(t *testing.T) {
	capabilities, err := NewStandardCapabilities(NewSliceStore())
	if err != nil {
		t.Fatal(err)
	}
	for _, capability := range capabilities {
		if _, err := capability.Execute(context.Background(), core.CapabilityRequest{}); !errors.Is(err, core.ErrAcceptedInvocationRequired) {
			t.Fatalf("%s accepted gate error=%v", capability.Manifest().ID, err)
		}
	}
	legacy, err := NewCapability("memory.legacy", NewSliceStore())
	if err != nil {
		t.Fatal(err)
	}
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	result, err := legacy.Execute(context.Background(), core.CapabilityRequest{Args: map[string]any{"action": "recall"}, Context: core.CapabilityContext{Principal: core.Principal{Scope: scope}}})
	if err != nil || !result.OK || legacy.Manifest().ID != "memory.legacy" {
		t.Fatalf("legacy behavior changed: result=%#v err=%v", result, err)
	}
}

func TestStandardCapabilitiesSuccessPathsAndPrincipalScope(t *testing.T) {
	store := NewSliceStore()
	fixture := newStandardFixture(t, store)
	remembered := fixture.invoke(t, RememberCapabilityID, map[string]any{"key": "city", "content": "Paris", "tags": []any{"travel"}}, nil)
	if !remembered.OK || !strings.Contains(remembered.Content, "city") {
		t.Fatalf("remember=%#v", remembered)
	}
	recalled := fixture.invoke(t, RecallCapabilityID, map[string]any{"query": "Paris", "tags": []any{"travel"}}, nil)
	if !recalled.OK || !strings.Contains(recalled.Content, "Paris") {
		t.Fatalf("recall=%#v", recalled)
	}
	var payload struct {
		Entries []Entry `json:"entries"`
	}
	if err := json.Unmarshal([]byte(recalled.Content), &payload); err != nil || len(payload.Entries) != 1 {
		t.Fatalf("recall payload=%q err=%v", recalled.Content, err)
	}
	forgotten := fixture.invoke(t, ForgetCapabilityID, map[string]any{"id": payload.Entries[0].ID}, core.ApproverFunc(func(context.Context, core.ApprovalRequest) (core.ApprovalDecision, error) {
		return core.ApprovalApproved, nil
	}))
	if !forgotten.OK {
		t.Fatalf("forget=%#v", forgotten)
	}
	if entries, err := store.Recall(context.Background(), fixture.principal.Scope, "", nil, 0); err != nil || len(entries) != 0 {
		t.Fatalf("principal scope entries=%#v err=%v", entries, err)
	}
}

func TestStandardCapabilitiesRejectScopeForgeryAndRequireForgetApproval(t *testing.T) {
	probe := &recallProbe{Store: NewSliceStore()}
	fixture := newStandardFixture(t, probe)
	result := fixture.invoke(t, RecallCapabilityID, map[string]any{"query": "x", "scope": "other", "tenant": "other", "user": "other"}, nil)
	if result.OK || result.Metadata["code"] != core.CodeInvalidArgs || probe.calls != 0 {
		t.Fatalf("scope spoof result=%#v calls=%d", result, probe.calls)
	}
	result = fixture.invoke(t, ForgetCapabilityID, map[string]any{"id": "mem_1"}, nil)
	if result.OK || result.Metadata["code"] != core.CodeApprovalUnavailable {
		t.Fatalf("forget without approval=%#v", result)
	}
}

func TestStandardCapabilitiesStoreFailuresAndPanicsDoNotLeak(t *testing.T) {
	for name, store := range map[string]Store{
		"error": errorStore{Store: NewSliceStore()},
		"panic": panicStore{Store: NewSliceStore()},
	} {
		t.Run(name, func(t *testing.T) {
			fixture := newStandardFixture(t, store)
			for _, test := range []struct {
				id   string
				args map[string]any
				code string
			}{
				{RememberCapabilityID, map[string]any{"key": "k", "content": "v"}, "memory_remember_failed"},
				{RecallCapabilityID, map[string]any{"query": "k"}, "memory_recall_failed"},
				{ForgetCapabilityID, map[string]any{"id": "mem_1"}, "memory_forget_failed"},
			} {
				result := fixture.invoke(t, test.id, test.args, core.ApproverFunc(func(context.Context, core.ApprovalRequest) (core.ApprovalDecision, error) {
					return core.ApprovalApproved, nil
				}))
				if result.OK || result.Metadata["code"] != test.code || strings.Contains(result.Content, "TOP-SECRET") {
					t.Fatalf("%s store failure leaked=%#v", test.id, result)
				}
			}
		})
	}
}

func TestStandardArgumentValidationIsActionSpecificAndClosed(t *testing.T) {
	if got, ok := integer(7); !ok || got != 7 {
		t.Fatalf("native int parse=%d ok=%t", got, ok)
	}
	if got, ok := integer(float64(7)); !ok || got != 7 {
		t.Fatalf("json float integer parse=%d ok=%t", got, ok)
	}
	for _, invalid := range []any{"7", float64(1.5), math.Inf(1), float64(math.MaxInt) * 2} {
		if _, ok := integer(invalid); ok {
			t.Fatalf("invalid integer value accepted: %#v", invalid)
		}
	}
	if _, _, _, ok := parseRecallArgs(map[string]any{"query": "x", "scope": "spoof"}); ok {
		t.Fatal("recall accepted unknown scope")
	}
	if _, _, _, ok := parseRecallArgs(map[string]any{"query": "x", "limit": 1.5}); ok {
		t.Fatal("recall accepted fractional limit")
	}
	if _, ok := parseRememberArgs(map[string]any{"key": "k", "content": "v", "id": "model-controlled"}); ok {
		t.Fatal("remember accepted a non-schema id")
	}
	if _, ok := parseForgetArgs(map[string]any{"id": "mem_1", "tenant": "spoof"}); ok {
		t.Fatal("forget accepted unknown tenant")
	}
}

type standardFixture struct {
	principal core.Principal
	registry  *core.CapabilityRegistry
}

func newStandardFixture(t *testing.T, store Store) standardFixture {
	t.Helper()
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	tenant, err := global.Child(core.ScopeRef{Kind: core.ScopeTenant, ID: "tenant"})
	if err != nil {
		t.Fatal(err)
	}
	user, err := tenant.Child(core.ScopeRef{Kind: core.ScopeUser, ID: "user"})
	if err != nil {
		t.Fatal(err)
	}
	principal := core.Principal{TenantID: "tenant", SubjectID: "user", Scope: user, Grants: core.NewPermissionSet(core.PermRead, core.PermWrite)}
	capabilities, err := NewStandardCapabilities(store)
	if err != nil {
		t.Fatal(err)
	}
	registry := core.NewCapabilityRegistry()
	for _, capability := range capabilities {
		if err := registry.Register(user, capability); err != nil {
			t.Fatal(err)
		}
	}
	return standardFixture{principal: principal, registry: registry}
}

func (f standardFixture) invoke(t *testing.T, name string, args map[string]any, approver core.Approver) core.CapabilityResult {
	t.Helper()
	sessionScope, err := f.principal.Scope.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "call-" + name[len("memory."):]})
	if err != nil {
		t.Fatal(err)
	}
	session, err := core.NewSession(core.SessionOptions{ID: "call-" + name[len("memory."):], ProfileID: "memory.standard", Principal: f.principal, Scope: sessionScope})
	if err != nil {
		t.Fatal(err)
	}
	call := core.ToolCall{ID: "call-" + name[len("memory."):], Name: name, Args: args}
	model := &standardModel{call: call}
	tools, err := (core.CapabilityResolver{Registry: f.registry}).Resolve(f.principal, sessionScope)
	if err != nil {
		t.Fatal(err)
	}
	agent, err := core.NewAgent(core.AgentOptions{LLM: model, Tools: tools, Session: session, Approver: approver, ToolJournal: newStandardJournal(), MaxSteps: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := agent.RunTurn(context.Background(), core.TurnInput{RunID: "run-" + name[len("memory."):], Text: "memory"}); err != nil {
		t.Fatal(err)
	}
	result, exists, err := session.ToolResult("run-"+name[len("memory."):], call.ID)
	if err != nil || !exists {
		t.Fatalf("tool result exists=%t err=%v", exists, err)
	}
	return result
}

type standardModel struct {
	call core.ToolCall
	sent bool
}

func (*standardModel) Provider() string { return "memory-standard-test" }
func (m *standardModel) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	if !m.sent {
		m.sent = true
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &m.call})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
		return nil
	}
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: "done"})
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
	return nil
}

type recallProbe struct {
	Store
	calls int
}

func (s *recallProbe) Recall(ctx context.Context, scope core.ScopePath, query string, tags []string, limit int) ([]Entry, error) {
	s.calls++
	return s.Store.Recall(ctx, scope, query, tags, limit)
}

type errorStore struct{ Store }

func (errorStore) Remember(context.Context, core.ScopePath, Entry) (Entry, error) {
	return Entry{}, errors.New("TOP-SECRET credential material")
}
func (errorStore) Recall(context.Context, core.ScopePath, string, []string, int) ([]Entry, error) {
	return nil, errors.New("TOP-SECRET credential material")
}
func (errorStore) Forget(context.Context, core.ScopePath, string) error {
	return errors.New("TOP-SECRET credential material")
}

type panicStore struct{ Store }

func (panicStore) Remember(context.Context, core.ScopePath, Entry) (Entry, error) {
	panic("TOP-SECRET credential material")
}
func (panicStore) Recall(context.Context, core.ScopePath, string, []string, int) ([]Entry, error) {
	panic("TOP-SECRET credential material")
}
func (panicStore) Forget(context.Context, core.ScopePath, string) error {
	panic("TOP-SECRET credential material")
}

type standardJournal struct {
	mu      sync.Mutex
	records map[string]core.ToolInvocationRecord
}

func newStandardJournal() *standardJournal {
	return &standardJournal{records: map[string]core.ToolInvocationRecord{}}
}

func standardJournalKey(invocation core.ToolInvocation) string {
	return invocation.SessionID + "\x00" + invocation.RunID + "\x00" + invocation.CallID
}

func (j *standardJournal) BeginToolInvocation(_ context.Context, invocation core.ToolInvocation) (core.ToolInvocationRecord, core.ToolInvocationDecision, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	key := standardJournalKey(invocation)
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

func (j *standardJournal) CompleteToolInvocation(_ context.Context, invocation core.ToolInvocation, result core.CapabilityResult) (core.ToolInvocationRecord, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	key := standardJournalKey(invocation)
	record, exists := j.records[key]
	if !exists || record.ToolInvocation != invocation {
		return core.ToolInvocationRecord{}, errors.New("journal identity mismatch")
	}
	if record.State != core.ToolInvocationCompleted {
		now := time.Now().UTC()
		copy := result
		record.State, record.Result, record.UpdatedAt, record.CompletedAt = core.ToolInvocationCompleted, &copy, now, now
		j.records[key] = record
	}
	return record, nil
}

func (j *standardJournal) MarkToolInvocationUncertain(_ context.Context, invocation core.ToolInvocation, code string) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	record, exists := j.records[standardJournalKey(invocation)]
	if !exists || record.ToolInvocation != invocation {
		return errors.New("journal identity mismatch")
	}
	record.State, record.ErrorCode, record.UpdatedAt = core.ToolInvocationUncertain, code, time.Now().UTC()
	j.records[standardJournalKey(invocation)] = record
	return nil
}

func sameRequired(value any, want []string) bool {
	items, ok := value.([]any)
	if !ok || len(items) != len(want) {
		return false
	}
	for index, expected := range want {
		if got, ok := items[index].(string); !ok || got != expected {
			return false
		}
	}
	return true
}
