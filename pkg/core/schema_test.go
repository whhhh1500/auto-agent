package core

import (
	"errors"
	"testing"
)

func schemaObject() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"symbol": map[string]any{"type": "string", "minLength": int64(1)},
			"limit":  map[string]any{"type": "integer", "minimum": float64(1)},
			"side":   map[string]any{"enum": []any{"buy", "sell"}},
			"tags":   map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
		},
		"required":             []any{"symbol"},
		"additionalProperties": false,
	}
}

func TestValidateArgsAcceptsValidInput(t *testing.T) {
	args := map[string]any{
		"symbol": "BTC", "limit": float64(5), "side": "buy",
		"tags": []any{"macro", "sol"},
	}
	if err := ValidateArgs(schemaObject(), args); err != nil {
		t.Fatalf("valid args rejected: %v", err)
	}
}

func TestValidateArgsEmptySchemaAllowsAnything(t *testing.T) {
	if err := ValidateArgs(nil, map[string]any{"any": "thing"}); err != nil {
		t.Fatalf("empty schema must not reject: %v", err)
	}
}

func TestValidateArgsRejectsMissingRequired(t *testing.T) {
	err := ValidateArgs(schemaObject(), map[string]any{})
	if err == nil {
		t.Fatal("expected missing-required rejection")
	}
	if !isInvalidArgs(err) {
		t.Fatalf("error must wrap ErrInvalidArgs: %v", err)
	}
}

func TestValidateArgsRejectsUnknownProperty(t *testing.T) {
	err := ValidateArgs(schemaObject(), map[string]any{"symbol": "BTC", "evil": true})
	if err == nil || !isInvalidArgs(err) {
		t.Fatalf("expected unknown-property rejection, got %v", err)
	}
}

func TestValidateArgsRejectsEnumAndTypeViolations(t *testing.T) {
	if err := ValidateArgs(schemaObject(), map[string]any{"symbol": "BTC", "side": "hold"}); err == nil {
		t.Fatal("expected enum rejection")
	}
	if err := ValidateArgs(schemaObject(), map[string]any{"symbol": 7}); err == nil {
		t.Fatal("expected type rejection")
	}
	if err := ValidateArgs(schemaObject(), map[string]any{"symbol": "BTC", "limit": 1.5}); err == nil {
		t.Fatal("expected non-integer rejection for integer field")
	}
}

func TestValidateSchemaRejectsMalformedSupportedKeywords(t *testing.T) {
	for name, schema := range map[string]map[string]any{
		"unknown-type": {"type": "strng"},
		"bad-pattern":  {"type": "string", "pattern": "["},
		"bad-required": {"type": "object", "required": "name"},
		"bad-range":    {"type": "number", "minimum": 10, "maximum": 1},
	} {
		t.Run(name, func(t *testing.T) {
			if err := ValidateSchema(schema); err == nil {
				t.Fatal("malformed schema was accepted")
			}
		})
	}
}

func TestValidateArgsAcceptsNativeIntegerTypes(t *testing.T) {
	schema := map[string]any{
		"type":       "object",
		"properties": map[string]any{"count": map[string]any{"type": "integer", "minimum": 1}},
		"required":   []any{"count"},
	}
	if err := ValidateArgs(schema, map[string]any{"count": 3}); err != nil {
		t.Fatalf("native Go integer was rejected: %v", err)
	}
}

func TestAdditionalPropertiesSchemaValidatesUndeclaredValues(t *testing.T) {
	schema := map[string]any{"type": "object", "properties": map[string]any{"name": map[string]any{"type": "string"}}, "additionalProperties": map[string]any{"type": "string"}}
	if err := ValidateSchema(schema); err != nil {
		t.Fatal(err)
	}
	if err := ValidateArgs(schema, map[string]any{"name": "ok", "extra": "yes"}); err != nil {
		t.Fatal(err)
	}
	if err := ValidateArgs(schema, map[string]any{"extra": 7}); err == nil {
		t.Fatal("non-string additional property accepted")
	}
}

func TestAdditionalPropertiesSchemaMalformedAndNested(t *testing.T) {
	if err := ValidateSchema(map[string]any{"additionalProperties": "yes"}); err == nil {
		t.Fatal("malformed additionalProperties accepted")
	}
	schema := map[string]any{"type": "object", "properties": map[string]any{"known": map[string]any{"type": "string"}}, "additionalProperties": map[string]any{"type": "object", "properties": map[string]any{"v": map[string]any{"type": "integer"}}, "additionalProperties": false}}
	if err := ValidateSchema(schema); err != nil {
		t.Fatal(err)
	}
	if err := ValidateArgs(schema, map[string]any{"known": "x", "nested": map[string]any{"v": 1}}); err != nil {
		t.Fatal(err)
	}
	if err := ValidateArgs(schema, map[string]any{"nested": map[string]any{"v": "bad"}}); err == nil {
		t.Fatal("nested additional schema bypassed")
	}
}

func isInvalidArgs(err error) bool {
	return errors.Is(err, ErrInvalidArgs)
}
