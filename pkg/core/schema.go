package core

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strings"
)

// ErrInvalidArgs marks a tool-call argument validation failure. Callers can
// surface the stable code without parsing message text.
var ErrInvalidArgs = fmt.Errorf("invalid tool arguments")

var knownSchemaTypes = map[string]bool{
	"string": true, "number": true, "integer": true, "boolean": true,
	"array": true, "object": true, "null": true,
}

// ValidateSchema validates the supported JSON Schema subset at registration
// time. Unknown keywords remain allowed, but malformed supported keywords and
// unknown type names are rejected.
func ValidateSchema(schema map[string]any) error {
	return validateSchemaNode(schema, "schema")
}

func validateSchemaNode(schema map[string]any, path string) error {
	if len(schema) == 0 {
		return nil
	}
	if raw, ok := schema["type"]; ok {
		if _, err := schemaTypeNames(raw); err != nil {
			return fmt.Errorf("%s.type: %w", path, err)
		}
	}
	if raw, ok := schema["enum"]; ok {
		if _, ok := raw.([]any); !ok {
			return fmt.Errorf("%s.enum must be an array", path)
		}
	}
	if pattern, ok := schema["pattern"]; ok {
		text, ok := pattern.(string)
		if !ok {
			return fmt.Errorf("%s.pattern must be a string", path)
		}
		if _, err := regexp.Compile(text); err != nil {
			return fmt.Errorf("%s.pattern is invalid: %w", path, err)
		}
	}
	for _, pair := range [][2]string{{"minLength", "maxLength"}, {"minItems", "maxItems"}, {"minimum", "maximum"}} {
		minimum, hasMinimum, err := schemaNumber(schema, pair[0])
		if err != nil {
			return fmt.Errorf("%s.%s: %w", path, pair[0], err)
		}
		maximum, hasMaximum, err := schemaNumber(schema, pair[1])
		if err != nil {
			return fmt.Errorf("%s.%s: %w", path, pair[1], err)
		}
		if hasMinimum && hasMaximum && minimum > maximum {
			return fmt.Errorf("%s: %s exceeds %s", path, pair[0], pair[1])
		}
	}
	if raw, ok := schema["additionalProperties"]; ok {
		switch additional := raw.(type) {
		case bool:
		case map[string]any:
			if err := validateSchemaNode(additional, path+".additionalProperties"); err != nil {
				return err
			}
		default:
			return fmt.Errorf("%s.additionalProperties must be a boolean or schema object", path)
		}
	}
	if raw, ok := schema["required"]; ok {
		items, ok := raw.([]any)
		if !ok {
			return fmt.Errorf("%s.required must be an array", path)
		}
		for index, item := range items {
			if _, ok := item.(string); !ok {
				return fmt.Errorf("%s.required[%d] must be a string", path, index)
			}
		}
	}
	if raw, ok := schema["properties"]; ok {
		properties, ok := raw.(map[string]any)
		if !ok {
			return fmt.Errorf("%s.properties must be an object", path)
		}
		for name, child := range properties {
			childSchema, ok := child.(map[string]any)
			if !ok {
				return fmt.Errorf("%s.properties.%s must be an object", path, name)
			}
			if err := validateSchemaNode(childSchema, path+".properties."+name); err != nil {
				return err
			}
		}
	}
	if raw, ok := schema["items"]; ok {
		items, ok := raw.(map[string]any)
		if !ok {
			return fmt.Errorf("%s.items must be an object", path)
		}
		if err := validateSchemaNode(items, path+".items"); err != nil {
			return err
		}
	}
	return nil
}

// ValidateArgs checks args against a JSON Schema subset: type, properties,
// required, additionalProperties, enum, items, minLength, maxLength, pattern,
// minimum, maximum, minItems, maxItems. Unknown schema keywords are ignored so
// providers can ship rich schemas without breaking validation.
//
// The subset is deliberate: model-facing argument validation needs to catch
// hallucinated or malformed arguments, not to be a full JSON Schema runtime.
func ValidateArgs(schema map[string]any, args map[string]any) error {
	if len(schema) == 0 {
		return nil
	}
	if err := validateValue(schema, args, ""); err != nil {
		return fmt.Errorf("%w: %s", ErrInvalidArgs, err)
	}
	return nil
}

// ValidateJSONValue validates an arbitrary decoded JSON value against the same
// deliberate JSON Schema subset used for tool arguments.
func ValidateJSONValue(schema map[string]any, value any) error {
	if len(schema) == 0 {
		return nil
	}
	return validateValue(schema, value, "value")
}

func validateValue(schema map[string]any, value any, path string) error {
	if schema == nil {
		return nil
	}
	if raw, ok := schema["type"]; ok {
		expected, err := schemaTypeNames(raw)
		if err != nil {
			return fmt.Errorf("%s: invalid schema type: %w", pathLabel(path), err)
		}
		if !matchesType(expected, value) {
			return fmt.Errorf("%s: expected type %s, got %T", pathLabel(path), strings.Join(expected, "|"), value)
		}
	}
	if raw, ok := schema["enum"].([]any); ok && len(raw) > 0 {
		if !containsValue(raw, value) {
			return fmt.Errorf("%s: value is not in the allowed set", pathLabel(path))
		}
	}

	switch typed := value.(type) {
	case map[string]any:
		return validateObject(schema, typed, path)
	case []any:
		return validateArray(schema, typed, path)
	case string:
		if v, ok := numberField(schema, "minLength"); ok && float64(len([]rune(typed))) < v {
			return fmt.Errorf("%s: length %d is below minLength %d", pathLabel(path), len([]rune(typed)), int64(v))
		}
		if v, ok := numberField(schema, "maxLength"); ok && float64(len([]rune(typed))) > v {
			return fmt.Errorf("%s: length %d exceeds maxLength %d", pathLabel(path), len([]rune(typed)), int64(v))
		}
		if pattern, ok := schema["pattern"].(string); ok && pattern != "" {
			re, err := regexp.Compile(pattern)
			if err != nil {
				return fmt.Errorf("%s: schema has invalid pattern: %v", pathLabel(path), err)
			}
			if !re.MatchString(typed) {
				return fmt.Errorf("%s: value does not match the required pattern", pathLabel(path))
			}
		}
	default:
		if number, ok, _ := numericValue(value); ok {
			if v, ok := numberField(schema, "minimum"); ok && number < v {
				return fmt.Errorf("%s: value %v is below minimum %v", pathLabel(path), number, v)
			}
			if v, ok := numberField(schema, "maximum"); ok && number > v {
				return fmt.Errorf("%s: value %v exceeds maximum %v", pathLabel(path), number, v)
			}
		}
	}
	return nil
}

func validateObject(schema map[string]any, value map[string]any, path string) error {
	if raw, ok := schema["required"].([]any); ok {
		for _, item := range raw {
			name, ok := item.(string)
			if !ok {
				continue
			}
			if _, present := value[name]; !present {
				return fmt.Errorf("%s: missing required property %q", pathLabel(path), name)
			}
		}
	}
	properties, _ := schema["properties"].(map[string]any)
	additional := schema["additionalProperties"]
	if additionalBool, ok := additional.(bool); ok && !additionalBool {
		for name := range value {
			if _, declared := properties[name]; !declared {
				return fmt.Errorf("%s: unknown property %q", pathLabel(path), name)
			}
		}
	}
	if additionalSchema, ok := additional.(map[string]any); ok {
		for name, child := range value {
			if _, declared := properties[name]; declared {
				continue
			}
			if err := validateValue(additionalSchema, child, path+"."+name); err != nil {
				return err
			}
		}
	} else if properties == nil {
		return nil
	}
	for name, sub := range properties {
		subSchema, ok := sub.(map[string]any)
		if !ok {
			continue
		}
		child, present := value[name]
		if !present {
			continue
		}
		if err := validateValue(subSchema, child, path+"."+name); err != nil {
			return err
		}
	}
	return nil
}

func validateArray(schema map[string]any, value []any, path string) error {
	if v, ok := numberField(schema, "minItems"); ok && float64(len(value)) < v {
		return fmt.Errorf("%s: %d items is below minItems %d", pathLabel(path), len(value), int64(v))
	}
	if v, ok := numberField(schema, "maxItems"); ok && float64(len(value)) > v {
		return fmt.Errorf("%s: %d items exceeds maxItems %d", pathLabel(path), len(value), int64(v))
	}
	items, ok := schema["items"].(map[string]any)
	if !ok {
		return nil
	}
	for i, item := range value {
		if err := validateValue(items, item, fmt.Sprintf("%s[%d]", path, i)); err != nil {
			return err
		}
	}
	return nil
}

func schemaTypeNames(raw any) ([]string, error) {
	switch typed := raw.(type) {
	case string:
		if !knownSchemaTypes[typed] {
			return nil, fmt.Errorf("unknown schema type %q", typed)
		}
		return []string{typed}, nil
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			name, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("type list contains %T", item)
			}
			if !knownSchemaTypes[name] {
				return nil, fmt.Errorf("unknown schema type %q", name)
			}
			out = append(out, name)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unexpected %T", raw)
	}
}

func matchesType(expected []string, value any) bool {
	for _, name := range expected {
		switch name {
		case "string":
			if _, ok := value.(string); ok {
				return true
			}
		case "number":
			if _, ok, _ := numericValue(value); ok {
				return true
			}
		case "integer":
			if _, ok, integer := numericValue(value); ok && integer {
				return true
			}
		case "boolean":
			if _, ok := value.(bool); ok {
				return true
			}
		case "array":
			if _, ok := value.([]any); ok {
				return true
			}
		case "object":
			if _, ok := value.(map[string]any); ok {
				return true
			}
		case "null":
			if value == nil {
				return true
			}
		}
	}
	return false
}

func containsValue(values []any, target any) bool {
	for _, candidate := range values {
		if equalJSON(candidate, target) {
			return true
		}
	}
	return false
}

func equalJSON(left, right any) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && string(leftJSON) == string(rightJSON)
}

func numberField(schema map[string]any, key string) (float64, bool) {
	raw, ok := schema[key]
	if !ok {
		return 0, false
	}
	switch typed := raw.(type) {
	case float64:
		return typed, true
	case int:
		return float64(typed), true
	default:
		return 0, false
	}
}

func schemaNumber(schema map[string]any, key string) (float64, bool, error) {
	raw, ok := schema[key]
	if !ok {
		return 0, false, nil
	}
	value, numeric, _ := numericValue(raw)
	if !numeric {
		return 0, false, fmt.Errorf("must be numeric, got %T", raw)
	}
	return value, true, nil
}

func numericValue(value any) (number float64, ok bool, integer bool) {
	if value == nil {
		return 0, false, false
	}
	if jsonNumber, ok := value.(json.Number); ok {
		number, err := jsonNumber.Float64()
		if err != nil {
			return 0, false, false
		}
		return number, true, !strings.ContainsAny(jsonNumber.String(), ".eE")
	}
	ref := reflect.ValueOf(value)
	switch ref.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(ref.Int()), true, true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(ref.Uint()), true, true
	case reflect.Float32, reflect.Float64:
		number = ref.Float()
		return number, true, number == math.Trunc(number)
	default:
		return 0, false, false
	}
}

func pathLabel(path string) string {
	if path == "" {
		return "args"
	}
	return strings.TrimPrefix(path, ".")
}
