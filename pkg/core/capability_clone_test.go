package core

import (
	"encoding/json"
	"math"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

func TestCloneCallArgsFastPathOwnsNestedJSONValues(t *testing.T) {
	input := map[string]any{
		"object": map[string]any{"items": []any{"before", map[string]any{"n": int64(7)}}},
	}
	clone, err := cloneCallArgs(input)
	if err != nil {
		t.Fatal(err)
	}
	input["object"].(map[string]any)["items"].([]any)[1].(map[string]any)["n"] = "changed"
	if got := clone["object"].(map[string]any)["items"].([]any)[1].(map[string]any)["n"]; got != float64(7) {
		t.Fatalf("clone shares nested state: %v", got)
	}
	clone["object"].(map[string]any)["items"].([]any)[0] = "clone-only"
	if got := input["object"].(map[string]any)["items"].([]any)[0]; got != "before" {
		t.Fatalf("input changed through clone: %v", got)
	}
}

func TestCloneCallArgsFastPathMatchesJSONRoundTripNumbers(t *testing.T) {
	input := map[string]any{
		"integer":  int64(9),
		"unsigned": uint64(17),
		"number":   json.Number("1.25"),
		"array":    []any{float32(2.5), json.Number("-0")},
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	var expected map[string]any
	if err := json.Unmarshal(encoded, &expected); err != nil {
		t.Fatal(err)
	}
	got, err := cloneCallArgs(input)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("fast path differs from JSON round trip:\n got %#v\nwant %#v", got, expected)
	}
}

func TestCloneMapFallsBackForTypedContainers(t *testing.T) {
	type typed struct {
		Value string `json:"value"`
	}
	input := map[string]any{"typed": typed{Value: "owned"}}
	clone := cloneMap(input)
	if clone == nil || clone["typed"].(map[string]any)["value"] != "owned" {
		t.Fatalf("typed container fallback failed: %#v", clone)
	}
	input["typed"] = typed{Value: "changed"}
	if clone["typed"].(map[string]any)["value"] != "owned" {
		t.Fatal("fallback clone is not independent")
	}
	custom := map[string]any{"custom": cloneMapCustomValue{}}
	clone = cloneMap(custom)
	if clone == nil || clone["custom"].(map[string]any)["value"] != "custom" {
		t.Fatalf("custom marshaler fallback failed: %#v", clone)
	}
}

type cloneMapCustomValue struct{}

func (cloneMapCustomValue) MarshalJSON() ([]byte, error) {
	return []byte(`{"value":"custom"}`), nil
}

func TestCloneMapNativeMatchesJSONRoundTrip(t *testing.T) {
	input := map[string]any{
		"text":   "safe <&>\u2028",
		"float":  math.Copysign(0, -1),
		"number": json.Number("1.25e2"),
		"nested": map[string]any{"items": []any{"before", float64(7)}},
	}
	want := cloneMapRoundTripForTest(t, input)
	got := cloneMap(input)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("native clone differs from JSON round trip:\n got %#v\nwant %#v", got, want)
	}
	if !math.Signbit(got["float"].(float64)) {
		t.Fatal("native clone lost negative zero")
	}
}

func TestCloneMapNativeOwnsNestedValues(t *testing.T) {
	input := map[string]any{"object": map[string]any{"items": []any{"before", map[string]any{"n": float64(7)}}}}
	clone := cloneMap(input)
	if clone == nil {
		t.Fatal("native clone unexpectedly failed")
	}
	input["object"].(map[string]any)["items"].([]any)[1].(map[string]any)["n"] = "changed"
	clone["object"].(map[string]any)["items"].([]any)[0] = "clone-only"
	if got := clone["object"].(map[string]any)["items"].([]any)[1].(map[string]any)["n"]; got != float64(7) {
		t.Fatalf("clone shares nested map: %v", got)
	}
	if got := input["object"].(map[string]any)["items"].([]any)[0]; got != "before" {
		t.Fatalf("input changed through clone: %v", got)
	}
}

func TestCloneMapFallsBackForInvalidNativeValues(t *testing.T) {
	if got := cloneMap(map[string]any{"text": string([]byte{0xff})}); got == nil || got["text"] != "\ufffd" {
		t.Fatalf("invalid UTF-8 did not preserve JSON round-trip behavior: %#v", got)
	}
	invalidKey := map[string]any{}
	invalidKey[string([]byte{0xff})] = "value"
	if got := cloneMap(invalidKey); got == nil || got["\ufffd"] != "value" {
		t.Fatalf("invalid UTF-8 key did not preserve JSON round-trip behavior: %#v", got)
	}
	for _, value := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		if got := cloneMap(map[string]any{"value": value}); got != nil {
			t.Fatalf("non-finite value unexpectedly cloned: %#v", got)
		}
	}
	for _, number := range []json.Number{"01", "+1", "1.", "1e", "NaN", "1e999"} {
		if got := cloneMap(map[string]any{"value": number}); got != nil {
			t.Fatalf("invalid number %q unexpectedly cloned: %#v", number, got)
		}
	}
}

func TestCloneMapDetectsCyclesAndFallsBackAfterDepthBound(t *testing.T) {
	cyclic := []any{nil}
	cyclic[0] = cyclic
	if got := cloneMap(map[string]any{"cycle": cyclic}); got != nil {
		t.Fatalf("cyclic slice unexpectedly cloned: %#v", got)
	}

	deep := map[string]any{}
	cursor := deep
	for index := 0; index < cloneJSONNativeMaxDepth+8; index++ {
		next := map[string]any{}
		cursor["next"] = next
		cursor = next
	}
	if got := cloneMap(deep); got == nil {
		t.Fatal("valid deep tree should use the Marshal fallback")
	}
}

func TestCloneMapNativeFastPathUsesFewerAllocations(t *testing.T) {
	input := map[string]any{
		"items": []any{
			map[string]any{"text": strings.Repeat("x", 1024), "value": float64(1)},
			map[string]any{"text": "second", "value": float64(2)},
		},
	}
	native := testing.AllocsPerRun(100, func() {
		clone := cloneMap(input)
		runtime.KeepAlive(clone)
	})
	roundTrip := testing.AllocsPerRun(100, func() {
		clone := cloneMapRoundTripForTest(t, input)
		runtime.KeepAlive(clone)
	})
	if native >= roundTrip {
		t.Fatalf("native fast path did not reduce allocations: native=%f roundtrip=%f", native, roundTrip)
	}
}

func TestCloneCallArgsRejectsCyclesWithoutPanic(t *testing.T) {
	input := map[string]any{}
	input["self"] = input
	if _, err := cloneCallArgs(input); err == nil {
		t.Fatal("cyclic arguments unexpectedly cloned")
	}
	if cloneMap(input) != nil {
		t.Fatal("cyclic map unexpectedly cloned")
	}
}

func cloneMapRoundTripForTest(t *testing.T, input map[string]any) map[string]any {
	t.Helper()
	data, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	return out
}
