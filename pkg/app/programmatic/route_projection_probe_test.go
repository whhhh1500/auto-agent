package programmatic

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/whhhh1500/auto-agent/pkg/core"
)

const probeRoutePTCSource = `{"version":"ptc-ir/v1","body":[{"op":"for","var":"target","in":{"op":"get","object":{"op":"var","name":"input"},"key":"targets"},"body":[{"op":"call","assign":"detail","tool":"records.detail","args":{"op":"get","object":{"op":"var","name":"target"},"key":"args"}}]},{"op":"return","value":{"op":"literal","value":null}}]}`

// These tests specify the v2 model-facing boundary.  They use a local plan
// only: a real host verifier must reconstruct the same plan from frozen
// composition, session events, and the completed tool journal.
func TestRouteProjectionProbeInitialAndChoiceSurfaces(t *testing.T) {
	plan := probeRouteTestPlan(t)
	probe := core.ToolCall{ID: "probe-1", Name: "records.inventory"}
	base := probeRouteTestTools(plan)

	for _, test := range []struct {
		name     string
		messages []core.ChatMessage
		want     []string
		choice   bool
	}{
		{
			name: "initial exposes probe catalog and ordinary direct but hides execute",
			want: []string{DefaultCatalogToolID, "records.inventory", "records.detail"},
		},
		{
			name:     "verified probe exposes only planned direct and dynamic execute",
			messages: probeRouteTurn(probe),
			want:     []string{"records.detail", DefaultExecuteToolID},
			choice:   true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			inner := &recordingRouteAdapter{}
			adapter := newProbeRouteAdapter(t, inner, probeRoutePlanVerifier(plan))
			if err := adapter.Stream(context.Background(), core.GenerateOptions{
				ModelCall: core.ModelCallRequest{RunID: "probe-route-test"},
				Messages:  test.messages,
				Tools:     base,
			}, nil); err != nil {
				t.Fatal(err)
			}
			if inner.calls != 1 {
				t.Fatalf("inner calls=%d", inner.calls)
			}
			if got := toolNames(inner.options.Tools); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("visible tools=%v want=%v", got, test.want)
			}
			if test.choice {
				want, err := plan.ChoiceTools()
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(inner.options.Tools, want) {
					t.Fatalf("choice schema drifted: got=%#v want=%#v", inner.options.Tools, want)
				}
			}
		})
	}
}

func TestRouteProjectionProbeIsNeutralUntilTheChoice(t *testing.T) {
	plan := probeRouteTestPlan(t)
	probe := core.ToolCall{ID: "probe-1", Name: "records.inventory"}
	inner := &recordingRouteAdapter{}
	adapter := newProbeRouteAdapter(t, inner, probeRoutePlanVerifier(plan))
	if err := adapter.Stream(context.Background(), core.GenerateOptions{
		ModelCall: core.ModelCallRequest{RunID: "probe-route-test"},
		Messages:  probeRouteTurn(probe),
		Tools:     probeRouteTestTools(plan),
	}, nil); err != nil {
		t.Fatal(err)
	}
	if got := toolNames(inner.options.Tools); !reflect.DeepEqual(got, []string{"records.detail", DefaultExecuteToolID}) {
		t.Fatalf("probe locked a route or exposed an extra tool: %v", got)
	}
}

func TestRouteProjectionProbeInitialStreamUsesClassifierBeforeDurablePlan(t *testing.T) {
	plan := probeRouteTestPlan(t)
	probe := core.ToolCall{ID: "new-probe", Name: "records.inventory"}
	inner := &chunkRouteAdapter{chunks: []core.StreamChunk{{Kind: core.StreamKindAssistant, ToolCall: &probe}}}
	adapter, err := NewLlmAdapter(inner, RouteProjectionOptions{
		Mode: RouteAutoProbeOnce,
		ProbePlanVerifier: func(context.Context, string, string) (ProbePlan, bool, error) {
			return ProbePlan{}, false, errors.New("new call has no durable probe receipt")
		},
		ProbeToolClassifier: func(_ context.Context, runID, toolName string) (bool, error) {
			if runID != "probe-route-test" || toolName != probe.Name {
				return false, errors.New("unexpected classifier input")
			}
			return true, nil
		},
		ChoiceResultVerifier: probeRouteChoiceVerifier,
		ChoiceAdmission:      probeRouteChoiceAdmission,
	})
	if err != nil {
		t.Fatal(err)
	}
	emits := 0
	if err := adapter.Stream(context.Background(), core.GenerateOptions{
		ModelCall: core.ModelCallRequest{RunID: "probe-route-test"},
		Messages:  turn(), Tools: probeRouteTestTools(plan),
	}, func(core.StreamChunk) { emits++ }); err != nil {
		t.Fatal(err)
	}
	if emits != 1 {
		t.Fatalf("probe stream emission count=%d", emits)
	}
	if _, err := NewLlmAdapter(&recordingRouteAdapter{}, RouteProjectionOptions{Mode: RouteAutoProbeOnce}); !errors.Is(err, ErrInvalidRouteProjection) {
		t.Fatalf("v2 route accepted missing classifier: %v", err)
	}
}

func TestRouteProjectionProbeRejectsInvalidChoiceBatchesBeforeCore(t *testing.T) {
	plan := probeRouteTestPlan(t)
	probe := core.ToolCall{ID: "probe-1", Name: "records.inventory"}
	direct := core.ToolCall{ID: "direct-1", Name: "records.detail", Args: map[string]any{"id": "record-17"}}
	execute := core.ToolCall{ID: "execute-1", Name: DefaultExecuteToolID, Args: map[string]any{
		"projection": "name", "selection": []any{float64(0)},
	}}
	preparedExecute := execute
	var err error
	preparedExecute.Args, err = plan.PrepareExecute(execute.Args)
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		name     string
		messages []core.ChatMessage
		chunks   []core.StreamChunk
	}{
		{
			name:   "probe mixed with direct",
			chunks: []core.StreamChunk{{Kind: core.StreamKindAssistant, ToolCalls: []core.ToolCall{probe, direct}}},
		},
		{
			name:     "second probe after verified probe",
			messages: probeRouteTurn(probe),
			chunks:   []core.StreamChunk{{Kind: core.StreamKindAssistant, ToolCall: &probe}},
		},
		{
			name:     "execute mixed with direct",
			messages: probeRouteTurn(probe),
			chunks:   []core.StreamChunk{{Kind: core.StreamKindAssistant, ToolCalls: []core.ToolCall{execute, direct}}},
		},
		{
			name:     "forged candidate selection",
			messages: probeRouteTurn(probe),
			chunks: []core.StreamChunk{{Kind: core.StreamKindAssistant, ToolCall: &core.ToolCall{
				ID: "forged-selection", Name: DefaultExecuteToolID,
				Args: map[string]any{"projection": "name", "selection": []any{float64(99)}},
			}}},
		},
		{
			name:     "duplicate candidate selection",
			messages: probeRouteTurn(probe),
			chunks: []core.StreamChunk{{Kind: core.StreamKindAssistant, ToolCall: &core.ToolCall{
				ID: "duplicate-selection", Name: DefaultExecuteToolID,
				Args: map[string]any{"projection": "name", "selection": []any{float64(0), float64(0)}},
			}}},
		},
		{
			name:     "forged planned direct arguments",
			messages: probeRouteTurn(probe),
			chunks: []core.StreamChunk{{Kind: core.StreamKindAssistant, ToolCall: &core.ToolCall{
				ID: "forged-direct", Name: "records.detail", Args: map[string]any{"id": "record-19"},
			}}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			inner := &chunkRouteAdapter{chunks: test.chunks}
			adapter := newProbeRouteAdapter(t, inner, probeRoutePlanVerifier(plan))
			emits := 0
			err := adapter.Stream(context.Background(), core.GenerateOptions{
				ModelCall: core.ModelCallRequest{RunID: "probe-route-test"},
				Messages:  test.messages,
				Tools:     probeRouteTestTools(plan),
			}, func(core.StreamChunk) { emits++ })
			if !errors.Is(err, ErrInvalidRouteProjection) || emits != 0 {
				t.Fatalf("err=%v emitted chunks=%d", err, emits)
			}
		})
	}
}

func TestRouteProjectionProbeRejectsOversizedExecuteReturnBeforeFinalModel(t *testing.T) {
	plan := probeRouteTestPlan(t)
	probe := core.ToolCall{ID: "probe-1", Name: "records.inventory"}
	execute := core.ToolCall{ID: "execute-1", Name: DefaultExecuteToolID, Args: map[string]any{
		"projection": "name", "selection": []any{float64(0)},
	}}
	inner := &recordingRouteAdapter{}
	adapter := newProbeRouteAdapter(t, inner, probeRoutePlanVerifier(plan))
	err := adapter.Stream(context.Background(), core.GenerateOptions{
		ModelCall: core.ModelCallRequest{RunID: "probe-route-test"},
		Messages: probeRouteTurn(probe,
			core.ChatMessage{Role: core.RoleAssistant, ToolCall: &execute},
			toolResult(execute.ID, strings.Repeat("x", plan.Facts().MaxModelReturnBytes+1))),
		Tools: probeRouteTestTools(plan),
	}, nil)
	if !errors.Is(err, ErrInvalidRouteProjection) || inner.calls != 0 {
		t.Fatalf("err=%v inner calls=%d", err, inner.calls)
	}
}

func TestRouteProjectionProbeChoiceResultsForceFinalWithoutTools(t *testing.T) {
	plan := probeRouteTestPlan(t)
	probe := core.ToolCall{ID: "probe-1", Name: "records.inventory"}
	direct := core.ToolCall{ID: "direct-1", Name: "records.detail", Args: map[string]any{"id": "record-17"}}
	directSecond := core.ToolCall{ID: "direct-2", Name: "records.detail", Args: map[string]any{"id": "record-18"}}
	execute := core.ToolCall{ID: "execute-1", Name: DefaultExecuteToolID, Args: map[string]any{
		"projection": "name", "selection": []any{float64(0)},
	}}
	preparedExecute := execute
	var err error
	preparedExecute.Args, err = plan.PrepareExecute(execute.Args)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name     string
		messages []core.ChatMessage
	}{
		{
			name:     "planned direct result",
			messages: probeRouteTurn(probe, core.ChatMessage{Role: core.RoleAssistant, ToolCall: &direct}, toolResult(direct.ID, "detail")),
		},
		{
			name: "planned direct batch result",
			messages: probeRouteTurn(probe,
				core.ChatMessage{Role: core.RoleAssistant, ToolCalls: []core.ToolCall{direct, directSecond}},
				toolResult(direct.ID, "detail"), toolResult(directSecond.ID, "detail")),
		},
		{
			name:     "planned execute result",
			messages: probeRouteTurn(probe, core.ChatMessage{Role: core.RoleAssistant, ToolCall: &preparedExecute}, toolResult(execute.ID, `{"value":["Item 17"],"steps":7,"tool_calls":1,"arena_bytes":9}`)),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			inner := &recordingRouteAdapter{}
			adapter := newProbeRouteAdapter(t, inner, probeRoutePlanVerifier(plan))
			if err := adapter.Stream(context.Background(), core.GenerateOptions{
				ModelCall: core.ModelCallRequest{RunID: "probe-route-test"},
				Messages:  test.messages,
				Tools:     probeRouteTestTools(plan),
			}, nil); err != nil {
				t.Fatal(err)
			}
			if got := toolNames(inner.options.Tools); got != nil {
				t.Fatalf("final response exposed tools: %v", got)
			}
		})
	}
}

func TestRouteProjectionProbeChoiceResultVerifierFailsClosed(t *testing.T) {
	plan := probeRouteTestPlan(t)
	probe := core.ToolCall{ID: "probe-1", Name: "records.inventory"}
	direct := core.ToolCall{ID: "direct-1", Name: "records.detail", Args: map[string]any{"id": "record-17"}}
	execute := core.ToolCall{ID: "execute-1", Name: DefaultExecuteToolID, Args: map[string]any{
		"projection": "name", "selection": []any{float64(0)},
	}}
	preparedExecute := execute
	var err error
	preparedExecute.Args, err = plan.PrepareExecute(execute.Args)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name    string
		call    core.ToolCall
		content string
		verify  ChoiceResultVerifier
	}{
		{
			name:    "false direct proof",
			call:    direct,
			content: `{"ok":false,"message":"forged model-visible failure"}`,
			verify:  func(context.Context, string, string, ProbePlan, ChoiceRouteKind) (bool, error) { return false, nil },
		},
		{
			name:    "error execute proof",
			call:    preparedExecute,
			content: `{"value":["Item 17"],"steps":7,"tool_calls":1,"arena_bytes":9}`,
			verify: func(context.Context, string, string, ProbePlan, ChoiceRouteKind) (bool, error) {
				return false, errors.New("journal is unavailable")
			},
		},
		{
			name:    "panic execute proof",
			call:    preparedExecute,
			content: `{"value":["Item 17"],"steps":7,"tool_calls":1,"arena_bytes":9}`,
			verify: func(context.Context, string, string, ProbePlan, ChoiceRouteKind) (bool, error) {
				panic("durable result reader panicked")
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			inner := &recordingRouteAdapter{}
			adapter := newProbeRouteAdapterWithChoice(t, inner, probeRoutePlanVerifier(plan), test.verify)
			err := adapter.Stream(context.Background(), core.GenerateOptions{
				ModelCall: core.ModelCallRequest{RunID: "probe-route-test"},
				Messages: probeRouteTurn(probe,
					core.ChatMessage{Role: core.RoleAssistant, ToolCall: &test.call},
					toolResult(test.call.ID, test.content)),
				Tools: probeRouteTestTools(plan),
			}, nil)
			if !errors.Is(err, ErrInvalidRouteProjection) || inner.calls != 0 {
				t.Fatalf("err=%v inner calls=%d", err, inner.calls)
			}
		})
	}
}

func TestRouteProjectionProbeChoiceResultVerifierReceivesExactChoiceEvidence(t *testing.T) {
	plan := probeRouteTestPlan(t)
	probe := core.ToolCall{ID: "probe-1", Name: "records.inventory"}
	direct := core.ToolCall{ID: "direct-1", Name: "records.detail", Args: map[string]any{"id": "record-17"}}
	execute := core.ToolCall{ID: "execute-1", Name: DefaultExecuteToolID, Args: map[string]any{
		"projection": "name", "selection": []any{float64(0)},
	}}
	preparedExecute := execute
	var err error
	preparedExecute.Args, err = plan.PrepareExecute(execute.Args)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		call core.ToolCall
		kind ChoiceRouteKind
	}{
		{name: "direct", call: direct, kind: ChoiceRouteDirect},
		{name: "execute", call: preparedExecute, kind: ChoiceRouteExecute},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			verifier := func(_ context.Context, runID, callID string, got ProbePlan, kind ChoiceRouteKind) (bool, error) {
				calls++
				if runID != "probe-route-test" || callID != test.call.ID || kind != test.kind || got.Digest() != plan.Digest() {
					t.Fatalf("verifier arguments: run=%q call=%q plan=%q kind=%q", runID, callID, got.Digest(), kind)
				}
				return true, nil
			}
			adapter := newProbeRouteAdapterWithChoice(t, &recordingRouteAdapter{}, probeRoutePlanVerifier(plan), verifier)
			if err := adapter.Stream(context.Background(), core.GenerateOptions{
				ModelCall: core.ModelCallRequest{RunID: "probe-route-test"},
				Messages: probeRouteTurn(probe,
					core.ChatMessage{Role: core.RoleAssistant, ToolCall: &test.call},
					toolResult(test.call.ID, `{"value":["Item 17"],"steps":7,"tool_calls":1,"arena_bytes":9}`)),
				Tools: probeRouteTestTools(plan),
			}, nil); err != nil {
				t.Fatal(err)
			}
			if calls != 1 {
				t.Fatalf("choice verifier calls=%d want=1", calls)
			}
		})
	}
}

func TestRouteProjectionProbeChoiceRejectsCurrentDirectSchemaDrift(t *testing.T) {
	plan := probeRouteTestPlan(t)
	probe := core.ToolCall{ID: "probe-1", Name: "records.inventory"}
	tools := probeRouteTestTools(plan)
	for index := range tools {
		if tools[index].Name == plan.DirectToolSchema().Name {
			tools[index].Description = "same name, changed schema"
		}
	}
	inner := &recordingRouteAdapter{}
	adapter := newProbeRouteAdapter(t, inner, probeRoutePlanVerifier(plan))
	err := adapter.Stream(context.Background(), core.GenerateOptions{
		ModelCall: core.ModelCallRequest{RunID: "probe-route-test"},
		Messages:  probeRouteTurn(probe),
		Tools:     tools,
	}, nil)
	if !errors.Is(err, ErrInvalidRouteProjection) || inner.calls != 0 {
		t.Fatalf("err=%v inner calls=%d", err, inner.calls)
	}
}

func TestRouteProjectionProbeChoiceRequiresOneGenericExecuteTool(t *testing.T) {
	plan := probeRouteTestPlan(t)
	probe := core.ToolCall{ID: "probe-1", Name: "records.inventory"}
	tools := probeRouteTestTools(plan)
	tools = append(tools, core.ToolSchema{Name: DefaultExecuteToolID, Description: "duplicate generic execute", Parameters: map[string]any{"type": "object"}})
	inner := &recordingRouteAdapter{}
	adapter := newProbeRouteAdapter(t, inner, probeRoutePlanVerifier(plan))
	err := adapter.Stream(context.Background(), core.GenerateOptions{
		ModelCall: core.ModelCallRequest{RunID: "probe-route-test"},
		Messages:  probeRouteTurn(probe),
		Tools:     tools,
	}, nil)
	if !errors.Is(err, ErrInvalidRouteProjection) || inner.calls != 0 {
		t.Fatalf("err=%v inner calls=%d", err, inner.calls)
	}
}

func TestRouteProjectionProbeModeRequiresAllV2ProofCallbacks(t *testing.T) {
	plan := probeRouteTestPlan(t)
	base := RouteProjectionOptions{
		Mode:                 RouteAutoProbeOnce,
		ProbePlanVerifier:    probeRoutePlanVerifier(plan),
		ProbeToolClassifier:  probeRouteToolClassifier,
		ChoiceResultVerifier: probeRouteChoiceVerifier,
		ChoiceAdmission:      probeRouteChoiceAdmission,
	}
	for _, test := range []struct {
		name   string
		mutate func(*RouteProjectionOptions)
	}{
		{name: "missing classifier", mutate: func(options *RouteProjectionOptions) { options.ProbeToolClassifier = nil }},
		{name: "missing plan verifier", mutate: func(options *RouteProjectionOptions) { options.ProbePlanVerifier = nil }},
		{name: "missing choice verifier", mutate: func(options *RouteProjectionOptions) { options.ChoiceResultVerifier = nil }},
		{name: "missing choice admission", mutate: func(options *RouteProjectionOptions) { options.ChoiceAdmission = nil }},
	} {
		t.Run(test.name, func(t *testing.T) {
			options := base
			test.mutate(&options)
			if _, err := NewLlmAdapter(&recordingRouteAdapter{}, options); !errors.Is(err, ErrInvalidRouteProjection) {
				t.Fatalf("v2 route accepted %s: %v", test.name, err)
			}
		})
	}
}

func TestRouteProjectionProbeRejectsDuplicatePlannedDirectHistoryBeforeInner(t *testing.T) {
	plan := probeRouteTestPlan(t)
	probe := core.ToolCall{ID: "probe-1", Name: "records.inventory"}
	first := core.ToolCall{ID: "direct-1", Name: "records.detail", Args: map[string]any{"id": "record-17"}}
	duplicate := core.ToolCall{ID: "direct-2", Name: "records.detail", Args: map[string]any{"id": "record-17"}}
	inner := &recordingRouteAdapter{}
	adapter := newProbeRouteAdapter(t, inner, probeRoutePlanVerifier(plan))
	err := adapter.Stream(context.Background(), core.GenerateOptions{
		ModelCall: core.ModelCallRequest{RunID: "probe-route-test"},
		Messages: probeRouteTurn(probe,
			core.ChatMessage{Role: core.RoleAssistant, ToolCalls: []core.ToolCall{first, duplicate}},
			toolResult(first.ID, "detail"), toolResult(duplicate.ID, "detail")),
		Tools: probeRouteTestTools(plan),
	}, nil)
	if !errors.Is(err, ErrInvalidRouteProjection) || inner.calls != 0 {
		t.Fatalf("err=%v inner calls=%d", err, inner.calls)
	}
}

func TestRouteProjectionProbeTransformsExecuteBeforeCore(t *testing.T) {
	plan := probeRouteTestPlan(t)
	probe := core.ToolCall{ID: "probe-1", Name: "records.inventory"}
	execute := core.ToolCall{ID: "execute-1", Name: DefaultExecuteToolID, Args: map[string]any{
		"projection": "name", "selection": []any{float64(0)},
	}}
	want, err := plan.PrepareExecute(execute.Args)
	if err != nil {
		t.Fatal(err)
	}
	adapter := newProbeRouteAdapter(t, &chunkRouteAdapter{chunks: []core.StreamChunk{{Kind: core.StreamKindAssistant, ToolCall: &execute}}}, probeRoutePlanVerifier(plan))
	var got core.ToolCall
	if err := adapter.Stream(context.Background(), core.GenerateOptions{
		ModelCall: core.ModelCallRequest{RunID: "probe-route-test"},
		Messages:  probeRouteTurn(probe),
		Tools:     probeRouteTestTools(plan),
	}, func(chunk core.StreamChunk) {
		if chunk.ToolCall != nil {
			got = *chunk.ToolCall
		}
	}); err != nil {
		t.Fatal(err)
	}
	if got.Name != DefaultExecuteToolID || !reflect.DeepEqual(got.Args, want) {
		t.Fatalf("Core received route-specific execute arguments: %#v want %#v", got, want)
	}
}

func TestRouteProjectionProbeChoiceAdmissionRejectsDirectBatchBeyondPlanBeforeEmission(t *testing.T) {
	plan := probeRouteTestPlan(t)
	plan.maxSelections = 1
	probe := core.ToolCall{ID: "probe-1", Name: "records.inventory"}
	first := core.ToolCall{ID: "direct-1", Name: "records.detail", Args: map[string]any{"id": "record-17"}}
	second := core.ToolCall{ID: "direct-2", Name: "records.detail", Args: map[string]any{"id": "record-18"}}
	inner := &chunkRouteAdapter{chunks: []core.StreamChunk{{Kind: core.StreamKindAssistant, ToolCalls: []core.ToolCall{first, second}}}}
	adapter := newProbeRouteAdapter(t, inner, probeRoutePlanVerifier(plan))
	emits := 0
	err := adapter.Stream(context.Background(), core.GenerateOptions{
		ModelCall: core.ModelCallRequest{RunID: "probe-route-test"}, Messages: probeRouteTurn(probe), Tools: probeRouteTestTools(plan),
	}, func(core.StreamChunk) { emits++ })
	if !errors.Is(err, ErrInvalidRouteProjection) || emits != 0 {
		t.Fatalf("direct batch err=%v emits=%d, want rejection before Core emission", err, emits)
	}
}

func TestRouteProjectionProbeChoiceAdmissionReceivesExecuteSelectionBeforeHostReplacement(t *testing.T) {
	plan := probeRouteTestPlan(t)
	probe := core.ToolCall{ID: "probe-1", Name: "records.inventory"}
	execute := core.ToolCall{ID: "execute-1", Name: DefaultExecuteToolID, Args: map[string]any{
		"projection": "name", "selection": []any{float64(0)},
	}}
	admitted := false
	adapter, err := NewLlmAdapter(&chunkRouteAdapter{chunks: []core.StreamChunk{{Kind: core.StreamKindAssistant, ToolCall: &execute}}}, RouteProjectionOptions{
		Mode: RouteAutoProbeOnce, ProbePlanVerifier: probeRoutePlanVerifier(plan), ProbeToolClassifier: probeRouteToolClassifier,
		ChoiceResultVerifier: probeRouteChoiceVerifier,
		ChoiceAdmission: func(_ context.Context, runID string, got ProbePlan, kind ChoiceRouteKind, calls []core.ToolCall) error {
			if runID != "probe-route-test" || got.Digest() != plan.Digest() || kind != ChoiceRouteExecute || len(calls) != 1 {
				return errors.New("unexpected execute admission")
			}
			if _, hasSelection := calls[0].Args["selection"]; !hasSelection {
				return errors.New("execute selection was replaced before admission")
			}
			if _, hasSource := calls[0].Args["source"]; hasSource {
				return errors.New("host source was exposed before admission")
			}
			admitted = true
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var emitted core.ToolCall
	if err := adapter.Stream(context.Background(), core.GenerateOptions{
		ModelCall: core.ModelCallRequest{RunID: "probe-route-test"}, Messages: probeRouteTurn(probe), Tools: probeRouteTestTools(plan),
	}, func(chunk core.StreamChunk) {
		if chunk.ToolCall != nil {
			emitted = *chunk.ToolCall
		}
	}); err != nil {
		t.Fatal(err)
	}
	if !admitted || emitted.Args["source"] == nil || emitted.Args["selection"] != nil {
		t.Fatalf("admission=%t emitted execute=%#v", admitted, emitted)
	}
}

func TestRouteProjectionProbeMissingOrConflictingPlanFailsClosedBeforeInner(t *testing.T) {
	probe := core.ToolCall{ID: "probe-1", Name: "records.inventory"}
	plan := probeRouteTestPlan(t)
	for _, test := range []struct {
		name     string
		verifier ProbePlanVerifier
	}{
		{
			name: "missing plan",
			verifier: func(context.Context, string, string) (ProbePlan, bool, error) {
				return ProbePlan{}, true, nil
			},
		},
		{
			name: "conflicting plan evidence",
			verifier: func(context.Context, string, string) (ProbePlan, bool, error) {
				return plan, true, errors.New("journal result conflicts with probe receipt")
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			inner := &recordingRouteAdapter{}
			adapter := newProbeRouteAdapter(t, inner, test.verifier)
			err := adapter.Stream(context.Background(), core.GenerateOptions{
				ModelCall: core.ModelCallRequest{RunID: "probe-route-test"},
				Messages:  probeRouteTurn(probe),
				Tools:     probeRouteTestTools(plan),
			}, nil)
			if !errors.Is(err, ErrInvalidRouteProjection) || inner.calls != 0 {
				t.Fatalf("err=%v inner calls=%d", err, inner.calls)
			}
		})
	}
}

func TestRouteProjectionProbeModeDoesNotChangeLegacyModeSurfaces(t *testing.T) {
	plan := probeRouteTestPlan(t)
	for _, test := range []struct {
		name string
		mode RouteMode
		want []string
	}{
		{name: "direct only", mode: RouteDirectOnly, want: []string{"records.inventory", "records.detail"}},
		{name: "ptc only", mode: RoutePTCOnly, want: []string{DefaultCatalogToolID}},
		{name: "auto first action", mode: RouteAutoFirstAction, want: []string{DefaultCatalogToolID, "records.inventory", "records.detail"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			called := false
			inner := &recordingRouteAdapter{}
			adapter, err := NewLlmAdapter(inner, RouteProjectionOptions{
				Mode: test.mode,
				ProbePlanVerifier: func(context.Context, string, string) (ProbePlan, bool, error) {
					called = true
					return plan, true, nil
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := adapter.Stream(context.Background(), core.GenerateOptions{Tools: probeRouteTestTools(plan)}, nil); err != nil {
				t.Fatal(err)
			}
			if called || !reflect.DeepEqual(toolNames(inner.options.Tools), test.want) {
				t.Fatalf("probe verifier called=%t visible tools=%v want=%v", called, toolNames(inner.options.Tools), test.want)
			}
		})
	}
}

func TestRouteProjectionProbeVerifierCanClassifyAnOrdinaryDirectAction(t *testing.T) {
	plan := probeRouteTestPlan(t)
	direct := core.ToolCall{ID: "direct-1", Name: "records.detail", Args: map[string]any{"id": "record-17"}}
	inner := &recordingRouteAdapter{}
	adapter := newProbeRouteAdapter(t, inner, func(_ context.Context, runID, callID string) (ProbePlan, bool, error) {
		if runID != "probe-route-test" || callID != direct.ID {
			return ProbePlan{}, false, errors.New("unexpected direct evidence")
		}
		return ProbePlan{}, false, nil
	})
	if err := adapter.Stream(context.Background(), core.GenerateOptions{
		ModelCall: core.ModelCallRequest{RunID: "probe-route-test"},
		Messages:  turn(core.ChatMessage{Role: core.RoleAssistant, ToolCall: &direct}, toolResult(direct.ID, "detail")),
		Tools:     probeRouteTestTools(plan),
	}, nil); err != nil {
		t.Fatal(err)
	}
	if got := toolNames(inner.options.Tools); !reflect.DeepEqual(got, []string{"records.detail"}) {
		t.Fatalf("ordinary direct action did not lock the legacy direct branch: %v", got)
	}
}

func TestRouteProjectionProbeDirectProjectionFailsClosedWhenClassifierErrors(t *testing.T) {
	plan := probeRouteTestPlan(t)
	direct := core.ToolCall{ID: "direct-1", Name: "records.detail", Args: map[string]any{"id": "record-17"}}
	inner := &recordingRouteAdapter{}
	adapter, err := NewLlmAdapter(inner, RouteProjectionOptions{
		Mode: RouteAutoProbeOnce,
		ProbePlanVerifier: func(context.Context, string, string) (ProbePlan, bool, error) {
			return ProbePlan{}, false, nil
		},
		ProbeToolClassifier: func(context.Context, string, string) (bool, error) {
			return false, errors.New("frozen capability snapshot is unavailable")
		},
		ChoiceResultVerifier: probeRouteChoiceVerifier,
		ChoiceAdmission:      probeRouteChoiceAdmission,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = adapter.Stream(context.Background(), core.GenerateOptions{
		ModelCall: core.ModelCallRequest{RunID: "probe-route-test"},
		Messages:  turn(core.ChatMessage{Role: core.RoleAssistant, ToolCall: &direct}, toolResult(direct.ID, "detail")),
		Tools:     probeRouteTestTools(plan),
	}, nil)
	if !errors.Is(err, ErrInvalidRouteProjection) || inner.calls != 0 {
		t.Fatalf("err=%v inner calls=%d", err, inner.calls)
	}
}

func newProbeRouteAdapter(t *testing.T, inner core.LlmAdapter, verifier ProbePlanVerifier) *LlmAdapter {
	return newProbeRouteAdapterWithChoice(t, inner, verifier, probeRouteChoiceVerifier)
}

func newProbeRouteAdapterWithChoice(t *testing.T, inner core.LlmAdapter, verifier ProbePlanVerifier, choice ChoiceResultVerifier) *LlmAdapter {
	t.Helper()
	adapter, err := NewLlmAdapter(inner, RouteProjectionOptions{
		Mode: RouteAutoProbeOnce, ProbePlanVerifier: verifier, ProbeToolClassifier: probeRouteToolClassifier, ChoiceResultVerifier: choice, ChoiceAdmission: probeRouteChoiceAdmission,
	})
	if err != nil {
		t.Fatal(err)
	}
	return adapter
}

func probeRouteToolClassifier(_ context.Context, runID, toolName string) (bool, error) {
	if runID != "probe-route-test" {
		return false, errors.New("unexpected route run")
	}
	return toolName == "records.inventory", nil
}

func probeRoutePlanVerifier(plan ProbePlan) ProbePlanVerifier {
	return func(_ context.Context, runID, callID string) (ProbePlan, bool, error) {
		if runID == "probe-route-test" && callID == "probe-1" {
			return plan, true, nil
		}
		return ProbePlan{}, false, nil
	}
}

func probeRouteChoiceVerifier(_ context.Context, runID, callID string, plan ProbePlan, kind ChoiceRouteKind) (bool, error) {
	if runID != "probe-route-test" || plan.Digest() == "" {
		return false, errors.New("unexpected choice evidence")
	}
	switch kind {
	case ChoiceRouteDirect:
		return callID == "direct-1" || callID == "direct-2", nil
	case ChoiceRouteExecute:
		return callID == "execute-1", nil
	default:
		return false, errors.New("unexpected choice route")
	}
}

func probeRouteChoiceAdmission(_ context.Context, runID string, plan ProbePlan, kind ChoiceRouteKind, calls []core.ToolCall) error {
	if runID != "probe-route-test" || plan.Digest() == "" || len(calls) == 0 {
		return errors.New("unexpected choice admission")
	}
	return nil
}

func probeRouteTurn(probe core.ToolCall, rest ...core.ChatMessage) []core.ChatMessage {
	messages := []core.ChatMessage{
		{Role: core.RoleUser, Content: "current"},
		{Role: core.RoleAssistant, ToolCall: &probe},
		toolResult(probe.ID, "inventory complete"),
	}
	return append(messages, rest...)
}

func probeRouteTestPlan(t *testing.T) ProbePlan {
	t.Helper()
	probe := core.SnapshotCapability{Manifest: core.CapabilityManifest{
		ID: "records.inventory", Version: "1.0.0", Name: "Record inventory", Kind: core.KindTool,
		Idempotent: true, MaxOutputBytes: 4096,
		Tool:     &core.ToolExposure{Description: "List records", Parameters: map[string]any{"type": "object", "additionalProperties": false}},
		Metadata: map[string]string{ProbeManifestKey: ProbeManifestVersion},
	}, ProviderRevision: "inventory-v1"}
	followup := core.SnapshotCapability{Manifest: core.CapabilityManifest{
		ID: "records.detail", Version: "1.0.0", Name: "Record detail", Kind: core.KindTool,
		Idempotent: true,
		Tool: &core.ToolExposure{Description: "Read one record", Parameters: map[string]any{
			"type": "object", "additionalProperties": false,
			"properties": map[string]any{"id": map[string]any{"type": "string"}}, "required": []any{"id"},
		}},
		OutputSchema: map[string]any{"type": "object", "required": []any{"id", "name", "padding"}, "additionalProperties": false, "properties": map[string]any{
			"id": map[string]any{"type": "string", "maxLength": 16}, "name": map[string]any{"type": "string", "maxLength": 64}, "padding": map[string]any{"type": "string", "maxLength": 4096},
		}},
		Metadata: map[string]string{ExposureKey: ExposureVersion},
	}, ProviderRevision: "detail-v1"}
	result := core.CapabilityResult{OK: true, Content: "inventory complete", Metadata: map[string]any{
		ProbeFactsMetadataKey: map[string]any{
			"version": ProbeFactsVersion, "followup_capability_id": followup.Manifest.ID,
			"candidates": []any{
				map[string]any{"label": "record-17", "args": map[string]any{"id": "record-17"}, "facts": map[string]any{"active": true}},
				map[string]any{"label": "record-18", "args": map[string]any{"id": "record-18"}, "facts": map[string]any{"active": true}},
			},
			"max_model_return_bytes": float64(4096),
		},
	}}
	plan, err := NewProbePlan(probe, followup, result, ProbePlanOptions{ExecuteToolID: DefaultExecuteToolID, ProgramContract: testProbeProgramContract()})
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func probeRouteTestTools(plan ProbePlan) []core.ToolSchema {
	return []core.ToolSchema{
		{Name: DefaultCatalogToolID, Description: "catalog", Parameters: map[string]any{"type": "object"}},
		{Name: "records.inventory", Description: "probe", Parameters: map[string]any{"type": "object"}},
		plan.directSourceSchema,
		{Name: DefaultExecuteToolID, Description: "generic execute", Parameters: map[string]any{"type": "object"}},
	}
}
