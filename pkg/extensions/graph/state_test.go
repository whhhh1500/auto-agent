package graph

import (
	"encoding/json"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestApplyStatePatchIsAtomicTypedAndCanonical(t *testing.T) {
	schema := StateSchema{Reducer: ReducerTopLevelJSONPatch, ReducerVersion: "1", Fields: []StateField{
		{Name: "required", Type: StateObject, Required: true, MaxBytes: 64},
		{Name: "optional", Type: StateString, MaxBytes: 64},
	}}
	input := State{"required": json.RawMessage(` { "z" : 1, "a" : true } `), "optional": json.RawMessage(`"old"`)}
	original := cloneState(input)
	failed, err := ApplyStatePatch(schema, input, StatePatch{Set: map[string]json.RawMessage{"required": json.RawMessage(`"not object"`)}})
	if err == nil || failed != nil || !reflect.DeepEqual(input, original) {
		t.Fatalf("failed patch changed input=%s failed=%v err=%v", input["required"], failed, err)
	}
	output, err := ApplyStatePatch(schema, input, StatePatch{Set: map[string]json.RawMessage{"optional": json.RawMessage(`"new"`)}})
	if err != nil {
		t.Fatal(err)
	}
	if got := string(output["required"]); got != `{"a":true,"z":1}` {
		t.Fatalf("got canonical required=%s", got)
	}
	if got := string(output["optional"]); got != `"new"` {
		t.Fatalf("optional=%s", got)
	}
	if string(input["optional"]) != `"old"` {
		t.Fatal("successful patch changed input")
	}
}

func TestApplyStatePatchRejectsRequiredDeleteAndUnknownField(t *testing.T) {
	schema := StateSchema{Reducer: ReducerTopLevelJSONPatch, ReducerVersion: "1", Fields: []StateField{{Name: "id", Type: StateString, Required: true, MaxBytes: 64}}}
	input := State{"id": json.RawMessage(`"a"`)}
	for _, patch := range []StatePatch{{Delete: []string{"id"}}, {Set: map[string]json.RawMessage{"unknown": json.RawMessage(`1`)}}} {
		if _, err := ApplyStatePatch(schema, input, patch); err == nil {
			t.Fatal("invalid patch accepted")
		}
	}
	if _, err := ApplyStatePatch(StateSchema{Reducer: ReducerTopLevelJSONPatch, Fields: []StateField{{Name: "id", Type: "unknown", MaxBytes: 1}}}, nil, StatePatch{}); err == nil {
		t.Fatal("invalid direct state schema accepted")
	}
}

func TestApplyStatePatchRejectsSetDeleteConflictAndCountsEncodedState(t *testing.T) {
	schema := StateSchema{Reducer: ReducerTopLevelJSONPatch, ReducerVersion: "1", Fields: []StateField{
		{Name: "id", Type: StateString, Required: true, MaxBytes: MaxStateBytes},
		{Name: "tiny", Type: StateString, MaxBytes: 8},
	}}
	input := State{"id": json.RawMessage(`"ok"`)}
	if _, err := ApplyStatePatch(schema, input, StatePatch{Set: map[string]json.RawMessage{"tiny": json.RawMessage(`"x"`)}, Delete: []string{"tiny"}}); err == nil {
		t.Fatal("set/delete conflict accepted")
	}
	// Values remain below their individual caps, but the JSON object encoding
	// includes a key, quotes, colon, braces, and the value itself.
	payload := strings.Repeat("a", MaxStateBytes-8)
	input = State{"id": json.RawMessage(`"` + payload + `"`)}
	if _, err := ApplyStatePatch(schema, input, StatePatch{}); err == nil {
		t.Fatal("encoded state overhead did not count toward total limit")
	}
	if _, err := ApplyStatePatch(StateSchema{Reducer: ReducerTopLevelJSONPatch, ReducerVersion: "1", Fields: []StateField{{Name: "id", Type: StateString, Required: true, MaxBytes: 8}}}, State{"id": json.RawMessage{'"', 0xff, '"'}}, StatePatch{}); err == nil {
		t.Fatal("invalid UTF-8 JSON state accepted")
	}
}

func TestApplyStatePatchPreflightIsAtomic(t *testing.T) {
	schema := StateSchema{Reducer: ReducerTopLevelJSONPatch, ReducerVersion: "1", Fields: []StateField{
		{Name: "id", Type: StateString, Required: true, MaxBytes: 8},
		{Name: "optional", Type: StateString, MaxBytes: 8},
	}}
	input := State{"id": json.RawMessage(`"ok"`)}
	original := cloneState(input)
	if _, err := ApplyStatePatch(schema, input, StatePatch{Set: map[string]json.RawMessage{"optional": json.RawMessage(`"too-long"`)}}); err == nil {
		t.Fatal("oversized raw patch value accepted")
	}
	if !reflect.DeepEqual(input, original) {
		t.Fatal("preflight failure mutated input")
	}
	oversized := make(State, MaxStateFields+1)
	for index := 0; index <= MaxStateFields; index++ {
		oversized["x"+string(rune('a'+index))] = json.RawMessage(`"x"`)
	}
	if _, err := ApplyStatePatch(schema, oversized, StatePatch{}); err == nil {
		t.Fatal("oversized state map accepted")
	}
}

func TestApplyStatePatchAllowsNearLimitReplacement(t *testing.T) {
	fields := make([]StateField, 8)
	for index := range fields {
		fields[index] = StateField{Name: "f" + strconv.Itoa(index), Type: StateString, MaxBytes: MaxStateFieldBytes}
	}
	schema := StateSchema{Reducer: ReducerTopLevelJSONPatch, ReducerVersion: "1", Fields: fields}
	raw := json.RawMessage(`"` + strings.Repeat("a", 60<<10) + `"`)
	input := make(State, len(fields))
	for index := range fields {
		input["f"+strconv.Itoa(index)] = raw
	}
	output, err := ApplyStatePatch(schema, input, StatePatch{Set: map[string]json.RawMessage{"f0": json.RawMessage(`"` + strings.Repeat("b", 60<<10) + `"`)}})
	if err != nil {
		t.Fatalf("near-limit replacement rejected: %v", err)
	}
	if string(output["f0"]) == string(input["f0"]) || string(input["f0"]) != string(raw) {
		t.Fatal("replacement did not preserve input atomicity")
	}
}

func TestValidateStateSupportsCustomReducerSchemaWithoutExecutingIt(t *testing.T) {
	schema := StateSchema{Reducer: "custom", ReducerVersion: "1", Fields: []StateField{{Name: "id", Type: StateString, Required: true, MaxBytes: 16}}}
	state, err := ValidateState(schema, State{"id": json.RawMessage(` "one" `)})
	if err != nil || string(state["id"]) != `"one"` {
		t.Fatalf("custom reducer state validation = %v, %v", state, err)
	}
	if _, err := ApplyStatePatch(schema, state, StatePatch{}); err == nil {
		t.Fatal("built-in patch reducer accepted a custom reducer schema")
	}
}
