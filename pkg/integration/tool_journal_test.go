package integration_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"sync/atomic"
	"testing"

	. "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/storage"

	_ "modernc.org/sqlite"
)

type journalIntegrationTool struct {
	calls *atomic.Int32
}

func (journalIntegrationTool) Manifest() CapabilityManifest {
	return CapabilityManifest{
		ID: "payment.capture", Version: "1.0.0", Name: "Capture", Kind: KindTool,
		Contract: "harness.tool/v1", RequiredPermissions: []Permission{PermRead},
		Tool: &ToolExposure{Parameters: map[string]any{
			"type": "object", "properties": map[string]any{"amount": map[string]any{"type": "number"}},
		}},
	}
}

func (p journalIntegrationTool) Execute(context.Context, CapabilityRequest) (CapabilityResult, error) {
	p.calls.Add(1)
	return CapabilityResult{Content: `{"capture_id":"cap_1"}`, OK: true}, nil
}

type journalIntegrationModel struct{}

func (journalIntegrationModel) Provider() string { return "journal-integration" }
func (journalIntegrationModel) Stream(_ context.Context, options GenerateOptions, emit func(StreamChunk)) error {
	last := options.Messages[len(options.Messages)-1]
	if last.Role == RoleTool {
		emit(StreamChunk{Kind: StreamKindAssistant, Text: "captured"})
		emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishStop})
		return nil
	}
	call := ToolCall{ID: "call-capture", Name: "payment.capture", Args: map[string]any{"amount": 12}}
	emit(StreamChunk{Kind: StreamKindAssistant, ToolCall: &call})
	emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishToolCalls})
	return nil
}

func TestSQLToolJournalPreventsSideEffectReplayAfterLostSessionSuffix(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "journal.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := storage.OpenSQLSessionStore(context.Background(), db, storage.SQLDialectSQLite); err != nil {
		t.Fatal(err)
	}
	journal, err := storage.NewSQLToolInvocationJournal(db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	_, product, _, user := testScopes()
	principal := Principal{
		SubjectID: "user-a", TenantID: "tenant-a", Scope: user,
		Grants: NewPermissionSet(PermRead, PermWrite),
	}
	var providerCalls atomic.Int32
	capabilities := NewCapabilityRegistry()
	if err := capabilities.Register(product, journalIntegrationTool{calls: &providerCalls}); err != nil {
		t.Fatal(err)
	}
	profiles := NewAgentProfileRegistry()
	name := "Journal Integration"
	model := ModelSelection{Provider: "journal-integration", Model: "journal-integration"}
	if err := profiles.Bind(AgentProfileLayer{
		Scope: product, ProfileID: "journal.integration", Name: &name, Model: &model,
		AddCapabilities: []string{"payment.capture"},
	}); err != nil {
		t.Fatal(err)
	}
	runtime := &Runtime{
		Capabilities: capabilities, Profiles: profiles, ToolJournal: journal,
		Models: ModelResolverFunc(func(context.Context, ModelSelection) (LlmAdapter, error) {
			return journalIntegrationModel{}, nil
		}),
	}
	newSession := func() *Session {
		scope, scopeErr := user.Child(ScopeRef{Kind: ScopeSession, ID: "session-journal-integration"})
		if scopeErr != nil {
			t.Fatal(scopeErr)
		}
		session, sessionErr := NewSession(SessionOptions{
			ID: "session-journal-integration", ProfileID: "journal.integration", Principal: principal, Scope: scope,
		})
		if sessionErr != nil {
			t.Fatal(sessionErr)
		}
		return session
	}
	first := newSession()
	if _, err := runtime.RunTurn(context.Background(), principal, first, TurnInput{
		RunID: "run-journal-integration", Text: "capture",
	}, func(event SessionEvent) {
		if event.Type == EvToolResult {
			panic("simulate lost session suffix")
		}
	}); err == nil {
		t.Fatal("first run did not expose the simulated session failure")
	}
	second := newSession()
	result, err := runtime.RunTurn(context.Background(), principal, second, TurnInput{
		RunID: "run-journal-integration", Text: "capture",
	}, nil)
	if err != nil || result.Status != RunCompleted {
		t.Fatalf("SQL journal replay failed: result=%#v err=%v", result, err)
	}
	if providerCalls.Load() != 1 {
		t.Fatalf("external side effect ran %d times, want exactly 1", providerCalls.Load())
	}
}
