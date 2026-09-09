package jsonvalue

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestDecodeStringRejectsAmbiguousOrOversizedStructure(t *testing.T) {
	for _, input := range []string{
		`{"id":"first","id":"second"}`,
		strings.Repeat("[", DefaultLimits.MaxDepth+1) + `0` + strings.Repeat("]", DefaultLimits.MaxDepth+1),
		`{"ok":true} {"again":true}`,
	} {
		if _, err := DecodeString(input, DefaultLimits); err == nil {
			t.Fatalf("unsafe JSON %q was accepted", input)
		}
	}
}

func TestDecodeStringPreservesNumbers(t *testing.T) {
	value, err := DecodeString(`{"whole":9007199254740993,"fraction":1.25}`, DefaultLimits)
	if err != nil {
		t.Fatal(err)
	}
	object := value.(map[string]any)
	if object["whole"].(json.Number).String() != "9007199254740993" || object["fraction"].(json.Number).String() != "1.25" {
		t.Fatalf("numbers=%#v", object)
	}
}
