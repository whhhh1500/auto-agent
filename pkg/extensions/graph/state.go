package graph

import (
	"bytes"
	"encoding/json"
	"fmt"
	"unicode/utf8"
)

// State and StatePatch are top-level JSON objects. StatePatch applies atomically:
// on validation failure, the supplied State is never changed.
type State map[string]json.RawMessage

type StatePatch struct {
	Set    map[string]json.RawMessage `json:"set,omitempty"`
	Delete []string                   `json:"delete,omitempty"`
}

// ApplyStatePatch is the one built-in deterministic reducer in Graph-G0.
// Custom Reducer implementations are intentionally not resolved or invoked.
func ApplyStatePatch(schema StateSchema, input State, patch StatePatch) (State, error) {
	if schema.Reducer != ReducerTopLevelJSONPatch || schema.ReducerVersion != "1" {
		return nil, fmt.Errorf("built-in state patch requires reducer %q version %q", ReducerTopLevelJSONPatch, "1")
	}
	fields, err := indexStateFields(schema)
	if err != nil {
		return nil, err
	}
	if err := preflightPatch(fields, patch); err != nil {
		return nil, err
	}
	if err := preflightState(fields, input); err != nil {
		return nil, err
	}
	out, err := canonicalizeAndValidateState(fields, input)
	if err != nil {
		return nil, err
	}
	for key, value := range patch.Set {
		field := fields[key]
		canonical, err := canonicalizeStateValue(field, value)
		if err != nil {
			return nil, fmt.Errorf("state patch field %q: %w", key, err)
		}
		out[key] = canonical
	}
	for _, key := range patch.Delete {
		delete(out, key)
	}
	if err := validateCanonicalState(fields, out); err != nil {
		return nil, err
	}
	return out, nil
}

// ValidateState validates and canonicalizes a complete State against the
// bounded top-level schema without resolving or executing a reducer. G1 uses
// it after a custom, immutably bound reducer returns its candidate state.
func ValidateState(schema StateSchema, input State) (State, error) {
	fields, err := indexStateFields(schema)
	if err != nil {
		return nil, err
	}
	if err := preflightState(fields, input); err != nil {
		return nil, err
	}
	return canonicalizeAndValidateState(fields, input)
}

func preflightPatch(fields map[string]StateField, patch StatePatch) error {
	if len(patch.Set) > MaxStateFields || len(patch.Delete) > MaxStateFields {
		return fmt.Errorf("state patch field count exceeds %d", MaxStateFields)
	}
	rough := int64(2)
	for key, value := range patch.Set {
		field, ok := fields[key]
		if !ok {
			return fmt.Errorf("state patch sets undeclared field %q", key)
		}
		if len(value) > field.MaxBytes {
			return fmt.Errorf("state patch field %q exceeds %d bytes", key, field.MaxBytes)
		}
		if !addBoundedBytes(&rough, len(key), MaxStateBytes) || !addBoundedBytes(&rough, len(value), MaxStateBytes) || !addBoundedBytes(&rough, 4, MaxStateBytes) {
			return fmt.Errorf("state patch rough size exceeds %d bytes", MaxStateBytes)
		}
	}
	deletes := make(map[string]struct{}, len(patch.Delete))
	for _, key := range patch.Delete {
		field, ok := fields[key]
		if !ok {
			return fmt.Errorf("state patch deletes undeclared field %q", key)
		}
		if field.Required {
			return fmt.Errorf("state patch deletes required field %q", key)
		}
		if _, duplicate := deletes[key]; duplicate {
			return fmt.Errorf("state patch deletes field %q twice", key)
		}
		deletes[key] = struct{}{}
		if _, conflict := patch.Set[key]; conflict {
			return fmt.Errorf("state patch both sets and deletes field %q", key)
		}
	}
	return nil
}

func preflightState(fields map[string]StateField, state State) error {
	if len(state) > len(fields) || len(state) > MaxStateFields {
		return fmt.Errorf("state field count exceeds schema bounds")
	}
	rough := int64(2)
	for key, value := range state {
		field, ok := fields[key]
		if !ok {
			return fmt.Errorf("state has undeclared field %q", key)
		}
		if len(value) > field.MaxBytes {
			return fmt.Errorf("state field %q exceeds %d bytes", key, field.MaxBytes)
		}
		if !addBoundedBytes(&rough, len(key), MaxStateBytes) || !addBoundedBytes(&rough, len(value), MaxStateBytes) || !addBoundedBytes(&rough, 4, MaxStateBytes) {
			return fmt.Errorf("state rough size exceeds %d bytes", MaxStateBytes)
		}
	}
	return nil
}

func indexStateFields(schema StateSchema) (map[string]StateField, error) {
	if len(schema.Fields) > MaxStateFields {
		return nil, fmt.Errorf("state field count exceeds %d", MaxStateFields)
	}
	fields := make(map[string]StateField, len(schema.Fields))
	for _, field := range schema.Fields {
		if err := validateIdentifier(field.Name, MaxNodeIDBytes, "state field"); err != nil {
			return nil, err
		}
		if _, duplicate := fields[field.Name]; duplicate {
			return nil, fmt.Errorf("state field %q is duplicate", field.Name)
		}
		switch field.Type {
		case StateString, StateNumber, StateBoolean, StateObject, StateArray:
		default:
			return nil, fmt.Errorf("state field %q has invalid type %q", field.Name, field.Type)
		}
		if field.MaxBytes <= 0 || field.MaxBytes > MaxStateFieldBytes {
			return nil, fmt.Errorf("state field %q max_bytes is out of bounds", field.Name)
		}
		fields[field.Name] = field
	}
	return fields, nil
}

func canonicalizeAndValidateState(fields map[string]StateField, state State) (State, error) {
	out := make(State, len(state))
	for key, value := range state {
		field, ok := fields[key]
		if !ok {
			return nil, fmt.Errorf("state has undeclared field %q", key)
		}
		if len(value) > field.MaxBytes {
			return nil, fmt.Errorf("state field %q exceeds %d bytes", key, field.MaxBytes)
		}
		canonical, err := canonicalizeStateValue(field, value)
		if err != nil {
			return nil, fmt.Errorf("state field %q: %w", key, err)
		}
		out[key] = canonical
	}
	if err := validateCanonicalState(fields, out); err != nil {
		return nil, err
	}
	return out, nil
}

func validateCanonicalState(fields map[string]StateField, state State) error {
	for name, field := range fields {
		if field.Required {
			if _, present := state[name]; !present {
				return fmt.Errorf("state is missing required field %q", name)
			}
		}
	}
	encoded, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("state canonical encoding: %w", err)
	}
	if len(encoded) > MaxStateBytes {
		return fmt.Errorf("state exceeds %d bytes", MaxStateBytes)
	}
	return nil
}

// canonicalizeStateValue parses one raw state value exactly once. Unlike the
// general definition-config helper, it combines UseNumber decoding, type
// validation, canonical output, and post-canonical field sizing on one path.
func canonicalizeStateValue(field StateField, raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) > field.MaxBytes {
		return nil, fmt.Errorf("state field %q exceeds %d bytes", field.Name, field.MaxBytes)
	}
	if !utf8.Valid(raw) {
		return nil, fmt.Errorf("state field %q is not valid UTF-8", field.Name)
	}
	var value any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("state field %q: %w", field.Name, err)
	}
	if offset := decoder.InputOffset(); offset < int64(len(raw)) && len(bytes.TrimSpace(raw[offset:])) != 0 {
		return nil, fmt.Errorf("state field %q has trailing JSON", field.Name)
	}
	valid := false
	switch field.Type {
	case StateString:
		_, valid = value.(string)
	case StateNumber:
		_, valid = value.(json.Number)
	case StateBoolean:
		_, valid = value.(bool)
	case StateObject:
		_, valid = value.(map[string]any)
	case StateArray:
		_, valid = value.([]any)
	}
	if !valid {
		return nil, fmt.Errorf("state field %q does not have type %q", field.Name, field.Type)
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("state field %q canonical encoding: %w", field.Name, err)
	}
	if len(canonical) > field.MaxBytes {
		return nil, fmt.Errorf("state field %q exceeds %d bytes after canonicalization", field.Name, field.MaxBytes)
	}
	return json.RawMessage(canonical), nil
}

func cloneState(input State) State {
	if input == nil {
		return State{}
	}
	out := make(State, len(input))
	for key, value := range input {
		out[key] = append(json.RawMessage(nil), value...)
	}
	return out
}

func addBoundedBytes(total *int64, amount, limit int) bool {
	if amount < 0 || int64(amount) > int64(limit)-*total {
		return false
	}
	*total += int64(amount)
	return true
}
