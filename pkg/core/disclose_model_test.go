package core

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

func TestAgentDisclosurePublishesDescribedSchemaAndRestores(t *testing.T) {
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	registry := NewCapabilityRegistry()
	manifest := toolManifest("inventory.lookup", "1")
	manifest.Tool = &ToolExposure{Description: "Look up inventory", Parameters: map[string]any{"type": "object"}}
	if err := registry.Register(user, staticTool{manifest: manifest, content: "proof"}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := (CapabilityResolver{Registry: registry}).Resolve(principal, user)
	if err != nil {
		t.Fatal(err)
	}
	session := mustSession(t, user, principal)
	modelCalls := 0
	model := streamAdapterFunc(func(_ context.Context, options GenerateOptions, emit func(StreamChunk)) error {
		modelCalls++
		if modelCalls == 1 {
			if hasToolSchema(options.Tools, manifest.ID) {
				return fmt.Errorf("cold tool was already exposed")
			}
			call := ToolCall{ID: "describe", Name: disclosedLibraryID, Args: map[string]any{"action": "describe", "id": manifest.ID}}
			emit(StreamChunk{Kind: StreamKindAssistant, ToolCall: &call})
			emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishToolCalls})
			return nil
		}
		if !hasToolSchema(options.Tools, manifest.ID) {
			return fmt.Errorf("described tool missing from actual model request")
		}
		emit(StreamChunk{Kind: StreamKindAssistant, Text: "ready"})
		emit(StreamChunk{Kind: StreamKindFinish, FinishKind: FinishStop})
		return nil
	})
	for i := 0; i < 2; i++ {
		// A fresh Agent must reconstruct disclosure from the retained messages.
		agent, err := NewAgent(AgentOptions{LLM: model, Tools: snapshot, Session: session, DiscloseTools: true})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := agent.RunTurn(context.Background(), TurnInput{RunID: fmt.Sprintf("disclose-%d", i), Text: "lookup"}); err != nil {
			t.Fatal(err)
		}
	}
	if modelCalls != 3 {
		t.Fatalf("model calls=%d", modelCalls)
	}
}

func pairedDisclosureMessages(call ToolCall, content string) []ChatMessage {
	return []ChatMessage{{Role: RoleAssistant, ToolCall: &call}, {Role: RoleTool, ToolCallID: call.ID, Content: content}}
}

func TestDisclosureSelectionUsesCurrentSnapshotAndPairedResults(t *testing.T) {
	disclosed := discloseToolRuntime(benchmarkCatalogSnapshot(3)).(*disclosedRuntime)
	for _, tc := range []struct {
		name, called string
		args         map[string]any
		result       string
		want         string
	}{
		{"described_id", disclosedLibraryID, map[string]any{"action": "describe", "id": "tool.001"}, `{"status":"ok","id":"tool.001","input_schema":{"type":"stale"}}`, "tool.001"},
		{"described_name", disclosedLibraryID, map[string]any{"action": "describe", "name": "001"}, `{"status":"ok","id":"tool.001"}`, "tool.001"},
		{"failed", disclosedLibraryID, map[string]any{"action": "describe", "id": "tool.001"}, `{"status":"denied","id":"tool.001"}`, ""},
		{"ambiguous", disclosedLibraryID, map[string]any{"action": "describe", "name": "001"}, `{"status":"explore","id":"tool.001"}`, ""},
		{"wrong_id", disclosedLibraryID, map[string]any{"action": "describe", "id": "tool.000"}, `{"status":"ok","id":"tool.001"}`, ""},
		{"wrong_name", disclosedLibraryID, map[string]any{"action": "describe", "name": "000"}, `{"status":"ok","id":"tool.001"}`, ""},
		{"search_is_not_describe", disclosedLibraryID, map[string]any{"action": "search"}, `{"status":"ok","id":"tool.001"}`, ""},
		{"unavailable", disclosedLibraryID, map[string]any{"action": "describe", "id": "tool.004"}, `{"status":"ok","id":"tool.004"}`, ""},
		{"elided_result", disclosedLibraryID, map[string]any{"action": "describe", "id": "tool.001"}, ElidedToolResultMarker, ""},
		{"business_cannot_describe_another_tool", "tool.000", nil, `{"status":"ok","id":"tool.001"}`, "tool.000"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			messages := pairedDisclosureMessages(ToolCall{ID: "pair", Name: tc.called, Args: tc.args}, tc.result)
			got := disclosed.schemasForMessages(messages)
			wantCount := 2 // memory.hot plus the library
			if tc.want != "" {
				wantCount++
			}
			if len(got) != wantCount || (tc.want != "" && !hasToolSchema(got, tc.want)) {
				t.Fatalf("selected schemas=%+v want=%q", got, tc.want)
			}
			if tc.want != "" {
				if got[len(got)-1].Parameters["type"] != "object" {
					t.Fatal("old tool result schema replaced the current snapshot")
				}
				got[len(got)-1].Parameters["type"] = "mutated"
				if disclosed.schemasForMessages(messages)[len(got)-1].Parameters["type"] != "object" {
					t.Fatal("schema output aliases snapshot state")
				}
			}
			if len(disclosed.schemasForMessages(messages[1:])) != 2 {
				t.Fatal("orphan tool result disclosed a tool")
			}
		})
	}
}

func TestDisclosureSelectionIsBoundedAndSnapshotScoped(t *testing.T) {
	var history []ChatMessage
	for i := 0; i < 12; i++ {
		id := fmt.Sprintf("tool.%03d", i)
		result, _ := json.Marshal(map[string]string{"status": "ok", "id": id})
		history = append(history, pairedDisclosureMessages(ToolCall{ID: id, Name: disclosedLibraryID, Args: map[string]any{"action": "describe", "id": id}}, string(result))...)
	}
	disclosed := discloseToolRuntime(benchmarkCatalogSnapshot(12)).(*disclosedRuntime)
	got := disclosed.schemasForMessages(history)
	if len(got) != 2+discloseActiveLimit || hasToolSchema(got, "tool.003") || !hasToolSchema(got, "tool.004") || !hasToolSchema(got, "tool.011") {
		t.Fatalf("recent selection not bounded: %+v", got)
	}
	history = append(history, pairedDisclosureMessages(ToolCall{ID: "use-again", Name: "tool.004"}, "done")...)
	got = disclosed.schemasForMessages(history)
	if len(got) != 2+discloseActiveLimit || got[len(got)-1].Name != "tool.004" {
		t.Fatal("reuse did not refresh selection without duplicating schemas")
	}
	// The next run's filtered snapshot is the only source of available tools.
	changed := discloseToolRuntime(benchmarkCatalogSnapshot(2)).(*disclosedRuntime)
	for _, schema := range changed.schemasForMessages(history) {
		if schema.Name != disclosedLibraryID && schema.Name != "memory.hot" && schema.Name != "tool.000" && schema.Name != "tool.001" {
			t.Fatal("old history restored a capability removed from current scope")
		}
	}
	if got := disclosed.schemasForMessages(nil); !reflect.DeepEqual(got, disclosed.Schemas()) {
		t.Fatal("selection leaked across independent projected conversations")
	}
}

func BenchmarkDisclosedSchemasFromHistory(b *testing.B) {
	disclosed := discloseToolRuntime(benchmarkCatalogSnapshot(500)).(*disclosedRuntime)
	var messages []ChatMessage
	for i := 0; i < 40; i++ {
		messages = append(messages, ChatMessage{Role: RoleUser, Content: "lookup"})
		messages = append(messages, pairedDisclosureMessages(ToolCall{ID: fmt.Sprint(i), Name: fmt.Sprintf("tool.%03d", i)}, "done")...)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = disclosed.schemasForMessages(messages)
	}
}

func TestDisclosePreservesHostLibrary(t *testing.T) {
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	registry := NewCapabilityRegistry()
	for _, id := range []string{disclosedLibraryID, "inventory.lookup"} {
		if err := registry.Register(user, staticTool{manifest: toolManifest(id, "1"), content: "host-library"}); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := (CapabilityResolver{Registry: registry}).Resolve(principal, user)
	if err != nil {
		t.Fatal(err)
	}
	wrapped := discloseToolRuntime(snapshot)
	if wrapped != snapshot {
		t.Fatal("built-in disclosure replaced a host-provided library")
	}
	result, err := wrapped.Execute(context.Background(), ToolCall{ID: "host-call", Name: disclosedLibraryID})
	if err != nil || !result.OK || result.Content != "host-library" {
		t.Fatalf("host library dispatch changed: result=%+v error=%v", result, err)
	}
}

func TestDisclosureHistoryCannotRestoreRevokedPermissionOrOldSchema(t *testing.T) {
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	principal.Grants = NewPermissionSet(PermRead, PermWrite)
	registry := NewCapabilityRegistry()
	write := toolManifest("records.write", "1")
	write.RequiredPermissions = []Permission{PermWrite}
	for _, manifest := range []CapabilityManifest{write, toolManifest("records.read", "1")} {
		if err := registry.Register(user, staticTool{manifest: manifest, content: "done"}); err != nil {
			t.Fatal(err)
		}
	}
	history := pairedDisclosureMessages(ToolCall{ID: "describe", Name: disclosedLibraryID, Args: map[string]any{"action": "describe", "id": write.ID}}, `{"status":"ok","id":"records.write","input_schema":{"type":"stale"}}`)
	resolve := func() *disclosedRuntime {
		t.Helper()
		snapshot, err := (CapabilityResolver{Registry: registry}).Resolve(principal, user)
		if err != nil {
			t.Fatal(err)
		}
		return discloseToolRuntime(snapshot).(*disclosedRuntime)
	}
	if !hasToolSchema(resolve().schemasForMessages(history), write.ID) {
		t.Fatal("authorized discovery was not restored")
	}
	principal.Grants = NewPermissionSet(PermRead)
	restricted := resolve()
	if hasToolSchema(restricted.schemasForMessages(history), write.ID) || restricted.Authorized(write.ID) {
		t.Fatal("history restored a tool after its permission was revoked")
	}
	if result, err := restricted.Execute(context.Background(), ToolCall{ID: "denied", Name: write.ID}); err != nil || result.OK {
		t.Fatalf("revoked tool executed: result=%+v error=%v", result, err)
	}
	principal.Grants = NewPermissionSet(PermRead, PermWrite)
	write.Version = "2"
	write.Tool.Parameters = map[string]any{"type": "object", "properties": map[string]any{"current": map[string]any{"type": "string"}}}
	if err := registry.Bind(CapabilityBinding{Scope: user, Mode: BindingReplace, Manifest: write, Provider: staticTool{manifest: write}}); err != nil {
		t.Fatal(err)
	}
	for _, schema := range resolve().schemasForMessages(history) {
		if schema.Name == write.ID {
			if schema.Parameters["properties"].(map[string]any)["current"] == nil {
				t.Fatal("restored selection did not use the newly registered schema")
			}
			return
		}
	}
	t.Fatal("updated visible capability was not restored")
}
