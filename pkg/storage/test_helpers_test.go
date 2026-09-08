package storage

import (
	"context"
	"testing"

	. "github.com/whhhh1500/auto-agent/pkg/core"
)

func testScopes() (ScopePath, ScopePath, ScopePath, ScopePath) {
	global := MustScopePath(ScopeRef{Kind: ScopeGlobal, ID: "global"})
	product, _ := global.Child(ScopeRef{Kind: ScopeProduct, ID: "product"})
	tenant, _ := product.Child(ScopeRef{Kind: ScopeTenant, ID: "tenant-a"})
	user, _ := tenant.Child(ScopeRef{Kind: ScopeUser, ID: "user-a"})
	return global, product, tenant, user
}

func testPrincipal(user ScopePath) Principal {
	return Principal{SubjectID: "user-a", TenantID: "tenant-a", Scope: user,
		Grants: NewPermissionSet(PermRead, PermWrite)}
}

func mustUserScope(t *testing.T) ScopePath {
	t.Helper()
	_, _, _, user := testScopes()
	return user
}

func mustSession(t *testing.T, user ScopePath, principal Principal) *Session {
	t.Helper()
	scope, err := user.Child(ScopeRef{Kind: ScopeSession, ID: "session-a"})
	if err != nil {
		t.Fatal(err)
	}
	session, err := NewSession(SessionOptions{ID: "session-a", ProfileID: "test.agent", Principal: principal, Scope: scope})
	if err != nil {
		t.Fatal(err)
	}
	return session
}

func mustNamedSession(t *testing.T, id string) *Session {
	t.Helper()
	user := mustUserScope(t)
	principal := testPrincipal(user)
	scope, err := user.Child(ScopeRef{Kind: ScopeSession, ID: id})
	if err != nil {
		t.Fatal(err)
	}
	session, err := NewSession(SessionOptions{ID: id, ProfileID: "test.agent", Principal: principal, Scope: scope})
	if err != nil {
		t.Fatal(err)
	}
	return session
}

type staticTool struct {
	manifest CapabilityManifest
	content  string
	wait     bool
}

func (t staticTool) Manifest() CapabilityManifest { return t.manifest }
func (t staticTool) Execute(ctx context.Context, _ CapabilityRequest) (CapabilityResult, error) {
	if t.wait {
		<-ctx.Done()
		return CapabilityResult{}, ctx.Err()
	}
	return CapabilityResult{Content: t.content, OK: true}, nil
}

func toolManifest(id, version string) CapabilityManifest {
	return CapabilityManifest{ID: id, Version: version, Name: id, Kind: KindTool,
		Contract: "harness.tool/v1", RequiredPermissions: []Permission{PermRead}, Tool: &ToolExposure{}}
}

type scriptedTurn struct {
	text  string
	calls []ToolCall
	usage *TokenUsage
}

type scriptedAdapter struct {
	steps []scriptedTurn
	index int
}

func (s *scriptedAdapter) Provider() string { return "scripted" }
func (s *scriptedAdapter) Stream(_ context.Context, _ GenerateOptions, emit func(StreamChunk)) error {
	if s.index >= len(s.steps) {
		emit(StreamChunk{Kind: "assistant", Text: "done"})
		emit(StreamChunk{Kind: "finish", FinishKind: "stop"})
		return nil
	}
	step := s.steps[s.index]
	s.index++
	if step.text != "" {
		emit(StreamChunk{Kind: "assistant", Text: step.text})
	}
	for i := range step.calls {
		call := step.calls[i]
		emit(StreamChunk{Kind: "assistant", ToolCall: &call, ToolCalls: []ToolCall{call}})
	}
	finish := FinishStop
	if len(step.calls) > 0 {
		finish = FinishToolCalls
	}
	emit(StreamChunk{Kind: StreamKindFinish, FinishKind: finish, Usage: step.usage})
	return nil
}
