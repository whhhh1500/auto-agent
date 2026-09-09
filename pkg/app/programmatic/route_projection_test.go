package programmatic

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/whhhh1500/auto-agent/pkg/core"
)

func TestRouteProjectionAutoFirstActionAndLocks(t *testing.T) {
	config := testRouteConfig(t, RouteAutoFirstAction)
	base := []core.ToolSchema{{Name: config.catalogID}, {Name: config.executeID}, {Name: "direct.read"}}
	catalog := core.ToolCall{ID: "catalog/current", Name: config.catalogID}
	direct := core.ToolCall{ID: "direct/current", Name: "direct.read"}
	execute := core.ToolCall{ID: "execute/current", Name: config.executeID}

	for _, test := range []struct {
		name     string
		messages []core.ChatMessage
		want     []string
		wantErr  bool
	}{
		{name: "first action exposes catalog and direct", want: []string{config.catalogID, "direct.read"}},
		{name: "successful catalog locks ptc", messages: turn(core.ChatMessage{Role: core.RoleAssistant, ToolCall: &catalog}, toolResult(catalog.ID, catalogResult())), want: []string{config.executeID}},
		{name: "direct result locks direct", messages: turn(core.ChatMessage{Role: core.RoleAssistant, ToolCall: &direct}, toolResult(direct.ID, "anything")), want: []string{"direct.read"}},
		{name: "execute result forces final", messages: turn(core.ChatMessage{Role: core.RoleAssistant, ToolCall: &execute}, toolResult(execute.ID, "failure is unknown")), want: nil},
		{name: "catalog then execute forces final", messages: turn(core.ChatMessage{Role: core.RoleAssistant, ToolCall: &catalog}, toolResult(catalog.ID, catalogResult()), core.ChatMessage{Role: core.RoleAssistant, ToolCall: &execute}, toolResult(execute.ID, "done")), want: nil},
		{name: "mixed catalog and direct history fails closed", messages: turn(core.ChatMessage{Role: core.RoleAssistant, ToolCalls: []core.ToolCall{catalog, direct}}, toolResult(catalog.ID, catalogResult()), toolResult(direct.ID, "done")), wantErr: true},
		{name: "mixed catalog and direct result ordering fails closed", messages: turn(core.ChatMessage{Role: core.RoleAssistant, ToolCalls: []core.ToolCall{catalog, direct}}, toolResult(direct.ID, "done"), toolResult(catalog.ID, catalogResult())), wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			route, err := routeFromMessages(context.Background(), test.messages, "test-run", config)
			if test.wantErr {
				if err == nil {
					t.Fatal("mixed catalog history was accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := toolNames(projectForTest(base, route, config)); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("tools=%v want=%v", got, test.want)
			}
		})
	}
}

func TestRouteProjectionRejectsOldOrphanFailedAndAmbiguousPairs(t *testing.T) {
	config := testRouteConfig(t, RouteAutoFirstAction)
	catalog := core.ToolCall{ID: "call/with/slash", Name: config.catalogID}
	execute := core.ToolCall{ID: "execute", Name: config.executeID}

	for _, test := range []struct {
		name     string
		messages []core.ChatMessage
	}{
		{name: "old turn", messages: []core.ChatMessage{{Role: core.RoleUser, Content: "old"}, {Role: core.RoleAssistant, ToolCall: &catalog}, toolResult(catalog.ID, catalogResult()), {Role: core.RoleUser, Content: "new"}}},
		{name: "orphan result", messages: turn(toolResult(catalog.ID, catalogResult()))},
		{name: "malformed catalog result", messages: turn(core.ChatMessage{Role: core.RoleAssistant, ToolCall: &catalog}, toolResult(catalog.ID, "not json"))},
		{name: "null tools catalog result", messages: turn(core.ChatMessage{Role: core.RoleAssistant, ToolCall: &catalog}, toolResult(catalog.ID, `{"version":"ptc-ir/v1","language":"public grammar","tools":null}`))},
		{name: "empty tools catalog result", messages: turn(core.ChatMessage{Role: core.RoleAssistant, ToolCall: &catalog}, toolResult(catalog.ID, `{"version":"ptc-ir/v1","language":"public grammar","tools":[]}`))},
		{name: "duplicate result invalidates pair", messages: turn(core.ChatMessage{Role: core.RoleAssistant, ToolCall: &catalog}, toolResult(catalog.ID, catalogResult()), toolResult(catalog.ID, catalogResult()))},
		{name: "duplicate call invalidates pair", messages: turn(core.ChatMessage{Role: core.RoleAssistant, ToolCalls: []core.ToolCall{catalog, catalog}}, toolResult(catalog.ID, catalogResult()))},
		{name: "execute without result invalidates turn", messages: turn(core.ChatMessage{Role: core.RoleAssistant, ToolCall: &execute})},
		{name: "duplicate execute result invalidates turn", messages: turn(core.ChatMessage{Role: core.RoleAssistant, ToolCall: &execute}, toolResult(execute.ID, "done"), toolResult(execute.ID, "done"))},
		{name: "malformed execute identifier invalidates turn", messages: turn(core.ChatMessage{Role: core.RoleAssistant, ToolCall: &core.ToolCall{Name: config.executeID}}, toolResult(execute.ID, "done"))},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := routeFromMessages(context.Background(), test.messages, "test-run", config)
			if test.name == "old turn" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil {
				t.Fatal("invalid current-turn catalog evidence was accepted")
			}
		})
	}
}

func TestRouteProjectionRequiresDurableCatalogSuccess(t *testing.T) {
	config, err := newRouteProjectionConfig(RouteProjectionOptions{Mode: RouteAutoFirstAction, CatalogResultVerifier: func(context.Context, string, string) (bool, error) { return false, nil }})
	if err != nil {
		t.Fatal(err)
	}
	catalog := core.ToolCall{ID: "catalog", Name: DefaultCatalogToolID}
	messages := turn(core.ChatMessage{Role: core.RoleAssistant, ToolCall: &catalog}, toolResult(catalog.ID, catalogResult()))
	if _, err := routeFromMessages(context.Background(), messages, "test-run", config); err == nil {
		t.Fatal("failed catalog result was accepted")
	}
	withoutVerifier, err := newRouteProjectionConfig(RouteProjectionOptions{Mode: RouteAutoFirstAction})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := routeFromMessages(context.Background(), messages, "test-run", withoutVerifier); err == nil {
		t.Fatal("catalog result without verifier was accepted")
	}
}

func TestRouteProjectionRejectsInvalidCatalogEvidenceBeforeInnerAdapter(t *testing.T) {
	catalog := core.ToolCall{ID: "catalog", Name: DefaultCatalogToolID}
	valid := turn(core.ChatMessage{Role: core.RoleAssistant, ToolCall: &catalog}, toolResult(catalog.ID, catalogResult()))
	for _, test := range []struct {
		name     string
		mode     RouteMode
		messages []core.ChatMessage
		verify   CatalogResultVerifier
	}{
		{name: "auto invalid envelope", mode: RouteAutoFirstAction, messages: turn(core.ChatMessage{Role: core.RoleAssistant, ToolCall: &catalog}, toolResult(catalog.ID, "not-json")), verify: func(context.Context, string, string) (bool, error) { return true, nil }},
		{name: "auto duplicate catalog result", mode: RouteAutoFirstAction, messages: turn(core.ChatMessage{Role: core.RoleAssistant, ToolCall: &catalog}, toolResult(catalog.ID, catalogResult()), toolResult(catalog.ID, catalogResult())), verify: func(context.Context, string, string) (bool, error) { return true, nil }},
		{name: "auto orphan catalog result", mode: RouteAutoFirstAction, messages: turn(toolResult(catalog.ID, catalogResult())), verify: func(context.Context, string, string) (bool, error) { return true, nil }},
		{name: "auto missing verifier", mode: RouteAutoFirstAction, messages: valid},
		{name: "auto rejected journal proof", mode: RouteAutoFirstAction, messages: valid, verify: func(context.Context, string, string) (bool, error) { return false, nil }},
		{name: "auto journal verification error", mode: RouteAutoFirstAction, messages: valid, verify: func(context.Context, string, string) (bool, error) { return false, errors.New("journal unavailable") }},
		{name: "ptc missing verifier", mode: RoutePTCOnly, messages: valid},
	} {
		t.Run(test.name, func(t *testing.T) {
			inner := &recordingRouteAdapter{}
			adapter, err := NewLlmAdapter(inner, RouteProjectionOptions{Mode: test.mode, CatalogResultVerifier: test.verify})
			if err != nil {
				t.Fatal(err)
			}
			err = adapter.Stream(context.Background(), core.GenerateOptions{
				ModelCall: core.ModelCallRequest{RunID: "test-run"},
				Messages:  test.messages,
				Tools:     []core.ToolSchema{{Name: DefaultCatalogToolID}, {Name: DefaultExecuteToolID}, {Name: "direct.read"}},
			}, nil)
			if !errors.Is(err, ErrInvalidRouteProjection) || inner.calls != 0 {
				t.Fatalf("err=%v inner calls=%d", err, inner.calls)
			}
		})
	}
}

func TestRouteProjectionDirectHistoryDoesNotRequireCatalogVerifier(t *testing.T) {
	catalog := core.ToolCall{ID: "catalog", Name: DefaultCatalogToolID}
	inner := &recordingRouteAdapter{}
	adapter, err := NewLlmAdapter(inner, RouteProjectionOptions{Mode: RouteDirectOnly})
	if err != nil {
		t.Fatal(err)
	}
	err = adapter.Stream(context.Background(), core.GenerateOptions{
		ModelCall: core.ModelCallRequest{RunID: "test-run"},
		Messages:  turn(core.ChatMessage{Role: core.RoleAssistant, ToolCall: &catalog}, toolResult(catalog.ID, "invalid envelope")),
		Tools:     []core.ToolSchema{{Name: DefaultCatalogToolID}, {Name: DefaultExecuteToolID}, {Name: "direct.read"}},
	}, nil)
	if err != nil || inner.calls != 1 {
		t.Fatalf("err=%v inner calls=%d", err, inner.calls)
	}
	if got := toolNames(inner.options.Tools); !reflect.DeepEqual(got, []string{"direct.read"}) {
		t.Fatalf("visible tools=%v", got)
	}
}

func TestRouteProjectionModesAndRevocation(t *testing.T) {
	catalogCall := core.ToolCall{ID: "catalog", Name: DefaultCatalogToolID}
	base := []core.ToolSchema{{Name: DefaultCatalogToolID}, {Name: DefaultExecuteToolID}, {Name: "direct.read"}}
	for _, test := range []struct {
		name     string
		mode     RouteMode
		messages []core.ChatMessage
		tools    []core.ToolSchema
		want     []string
		wantErr  bool
	}{
		{name: "direct only", mode: RouteDirectOnly, want: []string{"direct.read"}},
		{name: "direct only execute result still finalizes", mode: RouteDirectOnly, messages: turn(core.ChatMessage{Role: core.RoleAssistant, ToolCall: &core.ToolCall{ID: "execute", Name: DefaultExecuteToolID}}, toolResult("execute", "unknown")), want: nil},
		{name: "ptc only starts with catalog", mode: RoutePTCOnly, want: []string{DefaultCatalogToolID}},
		{name: "ptc only failed catalog fails closed", mode: RoutePTCOnly, messages: turn(core.ChatMessage{Role: core.RoleAssistant, ToolCall: &catalogCall}, toolResult(catalogCall.ID, "failed")), wantErr: true},
		{name: "ptc only successful catalog exposes execute", mode: RoutePTCOnly, messages: turn(core.ChatMessage{Role: core.RoleAssistant, ToolCall: &catalogCall}, toolResult(catalogCall.ID, catalogResult())), want: []string{DefaultExecuteToolID}},
		{name: "ptc lock revokes missing execute", mode: RouteAutoFirstAction, messages: turn(core.ChatMessage{Role: core.RoleAssistant, ToolCall: &catalogCall}, toolResult(catalogCall.ID, catalogResult())), tools: []core.ToolSchema{{Name: DefaultCatalogToolID}, {Name: "direct.read"}}, want: nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := testRouteConfig(t, test.mode)
			tools := test.tools
			if tools == nil {
				tools = base
			}
			route, err := routeFromMessages(context.Background(), test.messages, "test-run", config)
			if test.wantErr {
				if err == nil {
					t.Fatal("invalid PTC catalog evidence was accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := toolNames(projectForTest(tools, route, config)); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("tools=%v want=%v", got, test.want)
			}
		})
	}
	withoutSnapshotProof, err := newRouteProjectionConfig(RouteProjectionOptions{Mode: RouteAutoFirstAction, CatalogResultVerifier: func(context.Context, string, string) (bool, error) { return true, nil }})
	if err != nil {
		t.Fatal(err)
	}
	messages := turn(core.ChatMessage{Role: core.RoleAssistant, ToolCall: &catalogCall}, toolResult(catalogCall.ID, catalogResult()))
	route, err := routeFromMessages(context.Background(), messages, "test-run", withoutSnapshotProof)
	if err != nil {
		t.Fatal(err)
	}
	if got := toolNames(projectForTest([]core.ToolSchema{{Name: DefaultExecuteToolID, Description: "unproved drift"}}, route, withoutSnapshotProof)); got != nil {
		t.Fatalf("execute exposed without frozen snapshot proof: %v", got)
	}
}

func TestRouteProjectionAdapterCopiesAndForwardsOptionalInterfaces(t *testing.T) {
	inner := &recordingRouteAdapter{}
	adapter, err := NewLlmAdapter(inner, RouteProjectionOptions{Mode: RouteAutoFirstAction})
	if err != nil {
		t.Fatal(err)
	}
	original := []core.ToolSchema{{Name: DefaultCatalogToolID}, {Name: DefaultExecuteToolID}, {Name: "direct.read", Parameters: map[string]any{"properties": map[string]any{"value": map[string]any{"type": "string"}}}}}
	err = adapter.Stream(context.Background(), core.GenerateOptions{Messages: turn(), Tools: original}, func(core.StreamChunk) {})
	if err != nil {
		t.Fatal(err)
	}
	if got := toolNames(inner.options.Tools); !reflect.DeepEqual(got, []string{DefaultCatalogToolID, "direct.read"}) {
		t.Fatalf("projected=%v", got)
	}
	inner.options.Tools[1].Name = "changed"
	inner.options.Tools[1].Parameters["properties"].(map[string]any)["value"].(map[string]any)["type"] = "number"
	if got := toolNames(original); !reflect.DeepEqual(got, []string{DefaultCatalogToolID, DefaultExecuteToolID, "direct.read"}) {
		t.Fatalf("caller tools mutated: %v", got)
	}
	if got := original[2].Parameters["properties"].(map[string]any)["value"].(map[string]any)["type"]; got != "string" {
		t.Fatalf("caller parameters mutated: %#v", original[2].Parameters)
	}
	if adapter.Provider() != "route-test" || adapter.ArtifactRevision() != "route-test/v1" {
		t.Fatalf("optional metadata was not forwarded")
	}
	if window, output := adapter.ModelContextLimits(); window != 123 || output != 45 {
		t.Fatalf("limits=(%d,%d)", window, output)
	}
	if reflect.TypeOf(*adapter).NumField() != 2 {
		t.Fatal("adapter unexpectedly retains route content")
	}
}

func TestRouteProjectionRejectsUnsafeProjectedSchemas(t *testing.T) {
	deep := map[string]any{}
	current := deep
	for range maxProjectedSchemaDepth + 1 {
		next := map[string]any{}
		current["nested"] = next
		current = next
	}
	for _, test := range []struct {
		name       string
		parameters map[string]any
	}{
		{name: "unsupported value", parameters: map[string]any{"bad": func() {}}},
		{name: "too deep", parameters: deep},
		{name: "too large", parameters: map[string]any{"description": strings.Repeat("x", maxProjectedSchemaBytes+1)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			inner := &recordingRouteAdapter{}
			adapter, err := NewLlmAdapter(inner, RouteProjectionOptions{Mode: RouteAutoFirstAction})
			if err != nil {
				t.Fatal(err)
			}
			err = adapter.Stream(context.Background(), core.GenerateOptions{Messages: turn(), Tools: []core.ToolSchema{{Name: "direct.read", Parameters: test.parameters}}}, nil)
			if !errors.Is(err, ErrInvalidRouteProjection) || inner.options.Tools != nil {
				t.Fatalf("err=%v inner=%#v", err, inner.options.Tools)
			}
		})
	}
}

func TestRouteProjectionRejectsAmbiguousFirstActionBeforeEmit(t *testing.T) {
	catalog := core.ToolCall{ID: "catalog", Name: DefaultCatalogToolID}
	direct := core.ToolCall{ID: "direct", Name: "direct.read"}
	for _, test := range []struct {
		name   string
		chunks []core.StreamChunk
	}{
		{name: "single chunk mixed", chunks: []core.StreamChunk{{Kind: core.StreamKindAssistant, ToolCalls: []core.ToolCall{catalog, direct}}}},
		{name: "multi chunk mixed", chunks: []core.StreamChunk{{Kind: core.StreamKindAssistant, ToolCall: &catalog}, {Kind: core.StreamKindAssistant, ToolCall: &direct}}},
		{name: "duplicate catalogs", chunks: []core.StreamChunk{{Kind: core.StreamKindAssistant, ToolCalls: []core.ToolCall{catalog, {ID: "catalog-two", Name: DefaultCatalogToolID}}}}},
		{name: "hidden execute is not direct", chunks: []core.StreamChunk{{Kind: core.StreamKindAssistant, ToolCall: &core.ToolCall{ID: "execute", Name: DefaultExecuteToolID}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			adapter, err := NewLlmAdapter(&chunkRouteAdapter{chunks: test.chunks}, RouteProjectionOptions{Mode: RouteAutoFirstAction})
			if err != nil {
				t.Fatal(err)
			}
			emits := 0
			err = adapter.Stream(context.Background(), core.GenerateOptions{Messages: turn(), Tools: []core.ToolSchema{{Name: DefaultCatalogToolID}, {Name: "direct.read"}}}, func(core.StreamChunk) { emits++ })
			if !errors.Is(err, ErrInvalidRouteProjection) || emits != 0 {
				t.Fatalf("err=%v emits=%d", err, emits)
			}
		})
	}
}

func TestRouteProjectionRejectsHallucinatedHiddenToolsInEveryRoute(t *testing.T) {
	catalog := core.ToolCall{ID: "catalog", Name: DefaultCatalogToolID}
	execute := core.ToolCall{ID: "execute", Name: DefaultExecuteToolID}
	base := []core.ToolSchema{{Name: DefaultCatalogToolID}, {Name: DefaultExecuteToolID}, {Name: "direct.read"}}
	verified := RouteProjectionOptions{
		CatalogResultVerifier: func(context.Context, string, string) (bool, error) { return true, nil },
		ExecuteToolVerifier:   func(context.Context, string, core.ToolSchema) (bool, error) { return true, nil },
	}
	for _, test := range []struct {
		name     string
		mode     RouteMode
		messages []core.ChatMessage
		call     core.ToolCall
	}{
		{name: "direct only execute", mode: RouteDirectOnly, call: execute},
		{name: "ptc catalog phase direct", mode: RoutePTCOnly, call: core.ToolCall{ID: "direct", Name: "direct.read"}},
		{name: "ptc execute phase direct", mode: RoutePTCOnly, messages: turn(core.ChatMessage{Role: core.RoleAssistant, ToolCall: &catalog}, toolResult(catalog.ID, catalogResult())), call: core.ToolCall{ID: "direct", Name: "direct.read"}},
		{name: "final rejects every tool", mode: RouteAutoFirstAction, messages: turn(core.ChatMessage{Role: core.RoleAssistant, ToolCall: &execute}, toolResult(execute.ID, "unknown")), call: core.ToolCall{ID: "direct", Name: "direct.read"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			options := verified
			options.Mode = test.mode
			adapter, err := NewLlmAdapter(&chunkRouteAdapter{chunks: []core.StreamChunk{{Kind: core.StreamKindAssistant, ToolCall: &test.call}}}, options)
			if err != nil {
				t.Fatal(err)
			}
			emits := 0
			err = adapter.Stream(context.Background(), core.GenerateOptions{ModelCall: core.ModelCallRequest{RunID: "test-run"}, Messages: test.messages, Tools: base}, func(core.StreamChunk) { emits++ })
			if !errors.Is(err, ErrInvalidRouteProjection) || emits != 0 {
				t.Fatalf("err=%v emits=%d", err, emits)
			}
		})
	}
}

func TestRouteProjectionFirstActionBoundsAndCopiesChunks(t *testing.T) {
	direct := core.ToolCall{ID: "direct", Name: "direct.read", Args: map[string]any{"nested": map[string]any{"value": "original"}}}
	copying := &chunkRouteAdapter{chunks: []core.StreamChunk{{Kind: core.StreamKindAssistant, ToolCall: &direct}}}
	copying.mutate = func() { direct.Args["nested"].(map[string]any)["value"] = "changed" }
	adapter, err := NewLlmAdapter(copying, RouteProjectionOptions{Mode: RouteAutoFirstAction})
	if err != nil {
		t.Fatal(err)
	}
	var got core.ToolCall
	if err := adapter.Stream(context.Background(), core.GenerateOptions{Messages: turn(), Tools: []core.ToolSchema{{Name: "direct.read"}}}, func(chunk core.StreamChunk) { got = *chunk.ToolCall }); err != nil {
		t.Fatal(err)
	}
	if got.Args["nested"].(map[string]any)["value"] != "original" {
		t.Fatalf("buffered args changed: %#v", got.Args)
	}

	tooManyCalls := make([]core.ToolCall, maxFirstActionCalls+1)
	for index := range tooManyCalls {
		tooManyCalls[index] = core.ToolCall{ID: fmt.Sprintf("call-%d", index), Name: "direct.read"}
	}
	for _, test := range []struct {
		name   string
		chunks []core.StreamChunk
	}{
		{name: "tool call bound", chunks: []core.StreamChunk{{Kind: core.StreamKindAssistant, ToolCalls: tooManyCalls}}},
		{name: "chunk bound", chunks: make([]core.StreamChunk, maxFirstActionChunks+1)},
		{name: "text bound", chunks: []core.StreamChunk{{Kind: core.StreamKindAssistant, Text: strings.Repeat("x", maxFirstActionBytes+1)}}},
		{name: "tool args payload bound", chunks: []core.StreamChunk{{Kind: core.StreamKindAssistant, ToolCall: &core.ToolCall{ID: "direct", Name: "direct.read", Args: map[string]any{"payload": strings.Repeat("x", maxFirstActionBytes+1)}}}}},
		{name: "continuation payload bound", chunks: []core.StreamChunk{{Kind: core.StreamKindAssistant, ToolCall: &core.ToolCall{ID: "direct", Name: "direct.read", Continuation: strings.Repeat("x", maxFirstActionBytes+1)}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			limited, err := NewLlmAdapter(&chunkRouteAdapter{chunks: test.chunks}, RouteProjectionOptions{Mode: RouteAutoFirstAction})
			if err != nil {
				t.Fatal(err)
			}
			emits := 0
			err = limited.Stream(context.Background(), core.GenerateOptions{Messages: turn(), Tools: []core.ToolSchema{{Name: "direct.read"}}}, func(core.StreamChunk) { emits++ })
			if !errors.Is(err, ErrInvalidLlmAdapter) || emits != 0 {
				t.Fatalf("err=%v emits=%d", err, emits)
			}
		})
	}
}

func TestRouteProjectionConcurrentEmitsAreSerializedAndIllegalIsStable(t *testing.T) {
	legal := make([]core.StreamChunk, 16)
	for index := range legal {
		call := core.ToolCall{ID: fmt.Sprintf("direct-%d", index), Name: "direct.read"}
		legal[index] = core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &call}
	}
	for _, test := range []struct {
		name     string
		mode     RouteMode
		chunks   []core.StreamChunk
		messages []core.ChatMessage
		wantErr  bool
	}{
		{name: "auto first action legal direct batch", mode: RouteAutoFirstAction, chunks: legal},
		{name: "direct only legal direct batch", mode: RouteDirectOnly, chunks: legal},
		{name: "direct only illegal execute", mode: RouteDirectOnly, chunks: append(legal, core.StreamChunk{Kind: core.StreamKindAssistant, ToolCalls: []core.ToolCall{{ID: "execute", Name: DefaultExecuteToolID}}}), wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			adapter, err := NewLlmAdapter(&concurrentRouteAdapter{chunks: test.chunks}, RouteProjectionOptions{Mode: test.mode})
			if err != nil {
				t.Fatal(err)
			}
			emits := 0
			err = adapter.Stream(context.Background(), core.GenerateOptions{Messages: test.messages, Tools: []core.ToolSchema{{Name: DefaultCatalogToolID}, {Name: DefaultExecuteToolID}, {Name: "direct.read"}}}, func(core.StreamChunk) { emits++ })
			if test.wantErr {
				if !errors.Is(err, ErrInvalidRouteProjection) {
					t.Fatalf("err=%v", err)
				}
				return
			}
			if err != nil || emits != len(legal) {
				t.Fatalf("err=%v emits=%d", err, emits)
			}
		})
	}
}

func TestRouteProjectionResolverAndPanicsFailClosed(t *testing.T) {
	inner := &recordingRouteAdapter{}
	resolver, err := NewModelResolver(core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) { return inner, nil }), RouteProjectionOptions{Mode: RouteDirectOnly})
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := resolver.ResolveModel(context.Background(), core.ModelSelection{})
	if err != nil {
		t.Fatal(err)
	}
	if err := resolved.Stream(context.Background(), core.GenerateOptions{Tools: []core.ToolSchema{{Name: DefaultCatalogToolID}, {Name: "direct.read"}}}, nil); err != nil {
		t.Fatal(err)
	}
	if got := toolNames(inner.options.Tools); !reflect.DeepEqual(got, []string{"direct.read"}) {
		t.Fatalf("resolved projection=%v", got)
	}
	if _, err := NewLlmAdapter(nil, RouteProjectionOptions{Mode: RouteAutoFirstAction}); !errors.Is(err, ErrInvalidLlmAdapter) {
		t.Fatalf("nil adapter err=%v", err)
	}
	if _, err := NewLlmAdapter(inner, RouteProjectionOptions{Mode: "unknown"}); !errors.Is(err, ErrInvalidRouteProjection) {
		t.Fatalf("unknown mode err=%v", err)
	}
	panicking, err := NewLlmAdapter(panickingRouteAdapter{}, RouteProjectionOptions{Mode: RouteAutoFirstAction})
	if err != nil {
		t.Fatal(err)
	}
	if err := panicking.Stream(context.Background(), core.GenerateOptions{}, nil); !errors.Is(err, ErrInvalidLlmAdapter) {
		t.Fatalf("stream panic err=%v", err)
	}
}

func testRouteConfig(t *testing.T, mode RouteMode) routeProjectionConfig {
	t.Helper()
	config, err := newRouteProjectionConfig(RouteProjectionOptions{Mode: mode, CatalogResultVerifier: func(context.Context, string, string) (bool, error) { return true, nil }, ExecuteToolVerifier: func(context.Context, string, core.ToolSchema) (bool, error) { return true, nil }})
	if err != nil {
		t.Fatal(err)
	}
	return config
}

func projectForTest(tools []core.ToolSchema, route projectedRoute, config routeProjectionConfig) []core.ToolSchema {
	projected, err := projectRouteTools(context.Background(), "test-run", tools, route, config)
	if err != nil {
		panic(err)
	}
	return projected
}

func turn(messages ...core.ChatMessage) []core.ChatMessage {
	return append([]core.ChatMessage{{Role: core.RoleUser, Content: "current"}}, messages...)
}

func toolResult(id, content string) core.ChatMessage {
	return core.ChatMessage{Role: core.RoleTool, ToolCallID: id, Content: content}
}

func catalogResult() string {
	return `{"version":"ptc-ir/v1","language":"public grammar","tools":[{"schema":{"name":"direct.read"}}]}`
}

func toolNames(tools []core.ToolSchema) []string {
	if len(tools) == 0 {
		return nil
	}
	out := make([]string, len(tools))
	for index := range tools {
		out[index] = tools[index].Name
	}
	return out
}

type recordingRouteAdapter struct {
	options core.GenerateOptions
	calls   int
}

func (*recordingRouteAdapter) Provider() string         { return "route-test" }
func (*recordingRouteAdapter) ArtifactRevision() string { return "route-test/v1" }
func (*recordingRouteAdapter) ModelContextLimits() (int, int) {
	return 123, 45
}
func (a *recordingRouteAdapter) Stream(_ context.Context, options core.GenerateOptions, _ func(core.StreamChunk)) error {
	a.calls++
	a.options = options
	return nil
}

type panickingRouteAdapter struct{}

func (panickingRouteAdapter) Provider() string { return "panic" }
func (panickingRouteAdapter) Stream(context.Context, core.GenerateOptions, func(core.StreamChunk)) error {
	panic("adapter")
}

type chunkRouteAdapter struct {
	chunks []core.StreamChunk
	mutate func()
}

type concurrentRouteAdapter struct{ chunks []core.StreamChunk }

func (*concurrentRouteAdapter) Provider() string { return "concurrent-test" }
func (a *concurrentRouteAdapter) Stream(_ context.Context, _ core.GenerateOptions, emit func(core.StreamChunk)) error {
	var wait sync.WaitGroup
	for _, chunk := range a.chunks {
		wait.Add(1)
		go func(chunk core.StreamChunk) {
			defer wait.Done()
			emit(chunk)
		}(chunk)
	}
	wait.Wait()
	return nil
}

func (*chunkRouteAdapter) Provider() string { return "chunk-test" }
func (a *chunkRouteAdapter) Stream(_ context.Context, _ core.GenerateOptions, emit func(core.StreamChunk)) error {
	for _, chunk := range a.chunks {
		emit(chunk)
	}
	if a.mutate != nil {
		a.mutate()
	}
	return nil
}
