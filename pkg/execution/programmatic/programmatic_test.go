package programmatic

import (
	"context"
	"errors"
	"strings"
	"testing"
)

const branchProgram = `{
  "version":"ptc-ir/v1",
  "body":[
    {"op":"call","assign":"rows","tool":"catalog.search","args":{"op":"map","entries":{"query":{"op":"literal","value":"boots"}}}},
    {"op":"assign","name":"active","value":{"op":"list","items":[]}},
    {"op":"for","var":"row","in":{"op":"var","name":"rows"},"body":[
      {"op":"if","cond":{"op":"cmp","kind":"eq","left":{"op":"get","object":{"op":"var","name":"row"},"key":"active"},"right":{"op":"literal","value":true}},"then":[
        {"op":"append","target":"active","value":{"op":"var","name":"row"}}
      ]}
    ]},
    {"op":"if","cond":{"op":"cmp","kind":"gt","left":{"op":"len","value":{"op":"var","name":"active"}},"right":{"op":"literal","value":0}},"then":[
      {"op":"call","assign":"detail","tool":"catalog.detail","args":{"op":"map","entries":{"id":{"op":"get","object":{"op":"index","object":{"op":"var","name":"active"},"index":{"op":"literal","value":0}},"key":"id"}}}}
    ],"else":[
      {"op":"assign","name":"detail","value":{"op":"literal","value":null}}
    ]},
    {"op":"return","value":{"op":"var","name":"detail"}}
  ]
}`

func TestProgramRunsRealLoopAndBranchFromToolData(t *testing.T) {
	program, err := Compile([]byte(branchProgram))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := program.Tools(), []string{"catalog.detail", "catalog.search"}; !sameStrings(got, want) {
		t.Fatalf("tools=%v want=%v", got, want)
	}
	var calls []Call
	result, err := program.Run(context.Background(), nil, func(_ context.Context, call Call) (any, error) {
		calls = append(calls, call)
		switch call.Tool {
		case "catalog.search":
			return []any{
				map[string]any{"id": "off", "active": false},
				map[string]any{"id": "on", "active": true},
			}, nil
		case "catalog.detail":
			if got := call.Args["id"]; got != "on" {
				t.Fatalf("detail id=%#v", got)
			}
			return map[string]any{"id": "on", "price": int64(42)}, nil
		default:
			t.Fatalf("unexpected call %q", call.Tool)
			return nil, nil
		}
	}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if len(calls) != 2 || calls[0].Ordinal != 1 || calls[1].Ordinal != 2 {
		t.Fatalf("calls=%#v", calls)
	}
	value, ok := result.Value.(map[string]any)
	if !ok || value["id"] != "on" || value["price"] != int64(42) {
		t.Fatalf("result=%#v", result)
	}
	if result.ToolCalls != 2 || result.Steps == 0 || result.ArenaBytes <= 0 {
		t.Fatalf("metrics=%#v", result)
	}
}

func TestProgramBuildsMapCallArgsFromLoopRow(t *testing.T) {
	program, err := Compile([]byte(`{"version":"ptc-ir/v1","body":[
{"op":"for","var":"row","in":{"op":"get","object":{"op":"var","name":"input"},"key":"rows"},"body":[
{"op":"call","tool":"catalog.detail","args":{"op":"map","entries":{"id":{"op":"get","object":{"op":"var","name":"row"},"key":"id"}}}},
{"op":"break"}
]},
{"op":"return","value":{"op":"literal","value":null}}
]}`))
	if err != nil {
		t.Fatal(err)
	}
	var calls []Call
	_, err = program.Run(context.Background(), map[string]any{"rows": []any{map[string]any{"id": "item-01"}}}, func(_ context.Context, call Call) (any, error) {
		calls = append(calls, call)
		return nil, nil
	}, Limits{})
	if err != nil || len(calls) != 1 || calls[0].Tool != "catalog.detail" || calls[0].Args["id"] != "item-01" {
		t.Fatalf("calls=%#v err=%v", calls, err)
	}
}

func TestProgramPreservesFiniteDecimalToolData(t *testing.T) {
	program, err := Compile([]byte(`{"version":"ptc-ir/v1","body":[
{"op":"call","assign":"answer","tool":"rank","args":{"op":"map","entries":{}}},
{"op":"if","cond":{"op":"cmp","kind":"gt","left":{"op":"get","object":{"op":"var","name":"answer"},"key":"score"},"right":{"op":"literal","value":0.4}},"then":[{"op":"return","value":{"op":"literal","value":"selected"}}],"else":[{"op":"return","value":{"op":"literal","value":"rejected"}}]}
]}`))
	if err != nil {
		t.Fatal(err)
	}
	result, err := program.Run(context.Background(), nil, func(context.Context, Call) (any, error) {
		return map[string]any{"score": 0.42}, nil
	}, Limits{})
	if err != nil || result.Value != "selected" {
		t.Fatalf("result=%#v err=%v", result, err)
	}
}

func TestProgramComparesLargeIntegersExactly(t *testing.T) {
	cases := []struct {
		name string
		kind string
		want bool
	}{
		{name: "adjacent integers are not equal", kind: "eq", want: false},
		{name: "adjacent integers retain order", kind: "lt", want: true},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			source := `{"version":"ptc-ir/v1","body":[{"op":"return","value":{"op":"cmp","kind":"` + test.kind + `","left":{"op":"literal","value":9007199254740992},"right":{"op":"literal","value":9007199254740993}}}]}`
			program, err := Compile([]byte(source))
			if err != nil {
				t.Fatal(err)
			}
			result, err := program.Run(context.Background(), nil, func(context.Context, Call) (any, error) { return nil, nil }, Limits{})
			if err != nil || result.Value != test.want {
				t.Fatalf("result=%#v err=%v want=%v", result, err, test.want)
			}
		})
	}
}

func TestProgramRejectsLossyMixedLargeNumberComparison(t *testing.T) {
	program, err := Compile([]byte(`{"version":"ptc-ir/v1","body":[{"op":"return","value":{"op":"cmp","kind":"eq","left":{"op":"literal","value":9007199254740993},"right":{"op":"literal","value":9007199254740992.0}}}]}`))
	if err != nil {
		t.Fatal(err)
	}
	_, err = program.Run(context.Background(), nil, func(context.Context, Call) (any, error) { return nil, nil }, Limits{})
	if !errors.Is(err, ErrInvalidProgram) {
		t.Fatalf("lossy mixed comparison error=%v", err)
	}
}

func TestProgramPropagatesCallerErrorUnchanged(t *testing.T) {
	program, err := Compile([]byte(`{"version":"ptc-ir/v1","body":[{"op":"call","tool":"requires.approval","args":{"op":"map","entries":{}}},{"op":"return","value":{"op":"literal","value":true}}]}`))
	if err != nil {
		t.Fatal(err)
	}
	pending := errors.New("approval pending")
	_, err = program.Run(context.Background(), nil, func(context.Context, Call) (any, error) { return nil, pending }, Limits{})
	if err != pending || !errors.Is(err, pending) {
		t.Fatalf("error=%v, want original pending error", err)
	}
	if _, diagnostic := Diagnostic(err); diagnostic {
		t.Fatalf("caller error was misclassified as a VM diagnostic: %v", err)
	}
}

func TestProgramDiagnosticsAreFixedAndClassifyRuntimeFailures(t *testing.T) {
	if _, err := Compile([]byte(`{"version":"wrong","body":[]}`)); !errors.Is(err, ErrInvalidProgram) {
		t.Fatalf("compile error=%v", err)
	} else if code, ok := Diagnostic(err); !ok || code != compileVersionInvalid {
		t.Fatalf("compile diagnostic=%q ok=%t", code, ok)
	}
	cases := []struct {
		name, source, want string
	}{
		{"variable", `{"version":"ptc-ir/v1","body":[{"op":"return","value":{"op":"var","name":"missing_value"}}]}`, "runtime_variable_missing"},
		{"get", `{"version":"ptc-ir/v1","body":[{"op":"return","value":{"op":"get","object":{"op":"var","name":"input"},"key":"missing_key"}}]}`, "runtime_get_missing_key"},
		{"index", `{"version":"ptc-ir/v1","body":[{"op":"return","value":{"op":"index","object":{"op":"list","items":[]},"index":{"op":"literal","value":0}}}]}`, "runtime_index_invalid"},
		{"for", `{"version":"ptc-ir/v1","body":[{"op":"for","var":"row","in":{"op":"literal","value":"not-a-list"},"body":[{"op":"return","value":{"op":"literal","value":null}}]}]}`, "runtime_for_not_list"},
		{"append", `{"version":"ptc-ir/v1","body":[{"op":"append","target":"items","value":{"op":"literal","value":1}}]}`, "runtime_append_target_not_list"},
		{"call args", `{"version":"ptc-ir/v1","body":[{"op":"call","tool":"lookup","args":{"op":"literal","value":"not-an-object"}}]}`, "runtime_call_args_not_object"},
		{"integer", `{"version":"ptc-ir/v1","body":[{"op":"return","value":{"op":"int","kind":"div","left":{"op":"literal","value":1},"right":{"op":"literal","value":0}}}]}`, "runtime_integer_invalid"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			program, err := Compile([]byte(test.source))
			if err != nil {
				t.Fatal(err)
			}
			_, err = program.Run(context.Background(), map[string]any{}, func(context.Context, Call) (any, error) { return nil, nil }, Limits{})
			if !errors.Is(err, ErrInvalidProgram) {
				t.Fatalf("error=%v", err)
			}
			if code, ok := Diagnostic(err); !ok || code != test.want {
				t.Fatalf("diagnostic=%q ok=%t want=%q", code, ok, test.want)
			}
		})
	}
}

func TestCompileDiagnosticsClassifyFixedStagesWithoutSourceLeakage(t *testing.T) {
	limitBody := strings.Repeat(`{"op":"break"},`, HardMaxNodes+1)
	limitBody = strings.TrimSuffix(limitBody, ",")
	cases := []struct {
		name, source, want string
	}{
		{"source", string([]byte{0xff}), compileSourceInvalid},
		{"json", `{"version":"ptc-ir/v1","body":[`, compileJSONInvalid},
		{"json limit", strings.Repeat("[", HardMaxJSONDepth+2) + strings.Repeat("]", HardMaxJSONDepth+2), compileLimitExceeded},
		{"top level", `[]`, compileTopLevelInvalid},
		{"version", `{"version":"another","body":[{"op":"break"}]}`, compileVersionInvalid},
		{"body", `{"version":"ptc-ir/v1","body":{}}`, compileBodyInvalid},
		{"statement shape", `{"version":"ptc-ir/v1","body":[{"op":"assign","name":"x"}]}`, compileStatementInvalid},
		{"statement op", `{"version":"ptc-ir/v1","body":[{"op":"unknown"}]}`, compileStatementOp},
		{"expression shape", `{"version":"ptc-ir/v1","body":[{"op":"return","value":{"op":"literal"}}]}`, compileExpressionInvalid},
		{"expression op", `{"version":"ptc-ir/v1","body":[{"op":"return","value":{"op":"unknown"}}]}`, compileExpressionOp},
		{"compiler limit", `{"version":"ptc-ir/v1","body":[` + limitBody + `]}`, compileLimitExceeded},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			_, err := Compile([]byte(test.source))
			if !errors.Is(err, ErrInvalidProgram) {
				t.Fatalf("errors.Is(err, ErrInvalidProgram)=false: %v", err)
			}
			if code, ok := Diagnostic(err); !ok || code != test.want {
				t.Fatalf("diagnostic=%q ok=%t want=%q", code, ok, test.want)
			}
		})
	}

	secret := "model-private-source-value"
	_, err := Compile([]byte(`{"version":"ptc-ir/v1","body":[{"op":"unknown","private":"` + secret + `"}]}`))
	if !errors.Is(err, ErrInvalidProgram) {
		t.Fatalf("errors.Is(err, ErrInvalidProgram)=false: %v", err)
	}
	code, ok := Diagnostic(err)
	if !ok || code != compileStatementOp || strings.Contains(err.Error(), secret) || strings.Contains(code, secret) {
		t.Fatalf("compile diagnostic leaked source or changed category: code=%q err=%q", code, err)
	}
}

func TestProgramRejectsDynamicToolAndInvalidNumbers(t *testing.T) {
	cases := []string{
		`{"version":"ptc-ir/v1","body":[{"op":"call","tool":{"op":"var","name":"x"},"args":{"op":"map","entries":{}}}]}`,
		`{"version":"ptc-ir/v1","body":[{"op":"return","value":{"op":"literal","value":1e999}}]}`,
		`{"version":"ptc-ir/v1","body":[{"op":"return","value":{"op":"literal","value": [1]}}]}`,
		`{"version":"ptc-ir/v1","body":[{"op":"return","value":{"op":"literal","value": {"key":"value"}}}]}`,
		`{"version":"ptc-ir/v1","version":"ptc-ir/v1","body":[{"op":"return","value":{"op":"literal","value":null}}]}`,
		`{"version":"ptc-ir/v1","body":[{"op":"return","value":{"op":"literal","value":1,"value":1}}]}`,
	}
	for _, source := range cases {
		if _, err := Compile([]byte(source)); !errors.Is(err, ErrInvalidProgram) {
			t.Fatalf("source=%s err=%v", source, err)
		}
	}
	deep := strings.Repeat("[", HardMaxJSONDepth+1) + strings.Repeat("]", HardMaxJSONDepth+1)
	if _, err := Compile([]byte(deep)); !errors.Is(err, ErrInvalidProgram) {
		t.Fatalf("deep JSON error=%v", err)
	}
}

func TestProgramEnforcesToolLoopAndResultLimits(t *testing.T) {
	program, err := Compile([]byte(`{"version":"ptc-ir/v1","body":[
{"op":"for","var":"x","in":{"op":"get","object":{"op":"var","name":"input"},"key":"items"},"body":[{"op":"call","tool":"lookup","args":{"op":"map","entries":{}}}]},
{"op":"return","value":{"op":"literal","value":null}}
]}`))
	if err != nil {
		t.Fatal(err)
	}
	_, err = program.Run(context.Background(), map[string]any{"items": "not-a-list"}, func(context.Context, Call) (any, error) { return nil, nil }, Limits{})
	if !errors.Is(err, ErrInvalidProgram) {
		t.Fatalf("non-list loop error=%v", err)
	}
	_, err = program.Run(context.Background(), map[string]any{"items": []any{int64(1), int64(2)}}, func(context.Context, Call) (any, error) { return nil, nil }, Limits{MaxToolCalls: 1})
	if !errors.Is(err, ErrProgramLimit) {
		t.Fatalf("tool cap error=%v", err)
	}
	_, err = program.Run(context.Background(), map[string]any{"items": []any{int64(1), int64(2)}}, func(context.Context, Call) (any, error) { return nil, nil }, Limits{MaxLoopIterations: 1, MaxToolCalls: 2})
	if !errors.Is(err, ErrProgramLimit) {
		t.Fatalf("global loop cap error=%v", err)
	}
	bigResult := strings.Repeat("x", HardMaxToolResultBytes+1)
	single, err := Compile([]byte(`{"version":"ptc-ir/v1","body":[{"op":"call","assign":"x","tool":"lookup","args":{"op":"map","entries":{}}},{"op":"return","value":{"op":"var","name":"x"}}]}`))
	if err != nil {
		t.Fatal(err)
	}
	_, err = single.Run(context.Background(), nil, func(context.Context, Call) (any, error) { return map[string]any{"content": bigResult}, nil }, Limits{MaxArenaBytes: HardMaxArenaBytes})
	if !errors.Is(err, ErrProgramLimit) {
		t.Fatalf("result cap error=%v", err)
	}
}

func TestProgramRejectsExcessiveSourceAndCannotMutateCallerArgs(t *testing.T) {
	if _, err := Compile([]byte(strings.Repeat(" ", HardMaxSourceBytes+1))); !errors.Is(err, ErrInvalidProgram) {
		t.Fatalf("large source error=%v", err)
	}
	program, err := Compile([]byte(`{"version":"ptc-ir/v1","body":[{"op":"call","tool":"lookup","args":{"op":"map","entries":{"nested":{"op":"list","items":[{"op":"literal","value":1}]}}}},{"op":"call","tool":"lookup","args":{"op":"map","entries":{"nested":{"op":"list","items":[{"op":"literal","value":1}]}}}},{"op":"return","value":{"op":"literal","value":null}}]}`))
	if err != nil {
		t.Fatal(err)
	}
	callCount := 0
	_, err = program.Run(context.Background(), nil, func(_ context.Context, call Call) (any, error) {
		list := call.Args["nested"].([]any)
		callCount++
		if callCount == 2 && list[0] != int64(1) {
			t.Fatalf("caller mutation leaked into later call: %#v", list)
		}
		list[0] = int64(99)
		return nil, nil
	}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
}

func TestProgramBoundsExpandedAliasedReturnValue(t *testing.T) {
	var source strings.Builder
	source.WriteString(`{"version":"ptc-ir/v1","body":[`)
	source.WriteString(`{"op":"assign","name":"x","value":{"op":"list","items":[{"op":"literal","value":1}]}}`)
	for index := 0; index < 18; index++ {
		source.WriteString(`,{"op":"assign","name":"x","value":{"op":"list","items":[{"op":"var","name":"x"},{"op":"var","name":"x"}]}}`)
	}
	source.WriteString(`,{"op":"return","value":{"op":"var","name":"x"}}]}`)
	program, err := Compile([]byte(source.String()))
	if err != nil {
		t.Fatal(err)
	}
	_, err = program.Run(context.Background(), nil, func(context.Context, Call) (any, error) { return nil, nil }, Limits{MaxArenaBytes: 4 << 10})
	if !errors.Is(err, ErrProgramLimit) {
		t.Fatalf("aliased return error=%v, want resource limit", err)
	}
}

func sameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
