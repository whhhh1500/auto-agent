// Package jsonvalue decodes one JSON value with structural limits for trusted
// adapters that must not accept ambiguous provider output.
package jsonvalue

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

// Limits bounds one decoded JSON tree. Zero values select strict defaults.
type Limits struct {
	MaxBytes int
	MaxDepth int
	MaxNodes int
	MaxItems int
}

// DefaultLimits align output data with the bounded PTC value domain.
var DefaultLimits = Limits{MaxBytes: 256 << 10, MaxDepth: 32, MaxNodes: 4096, MaxItems: 1000}

// DecodeString parses exactly one JSON value, preserves numeric spellings as
// json.Number, and rejects duplicate object keys before any map value wins.
func DecodeString(input string, limits Limits) (any, error) {
	limits = resolve(limits)
	if len(input) > limits.MaxBytes {
		return nil, errors.New("JSON exceeds byte limit")
	}
	decoder := json.NewDecoder(strings.NewReader(input))
	decoder.UseNumber()
	state := decodeState{limits: limits}
	value, err := state.value(decoder, 0)
	if err != nil {
		return nil, err
	}
	if token, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("unexpected trailing JSON token %v", token)
		}
		return nil, err
	}
	return value, nil
}

type decodeState struct {
	limits Limits
	nodes  int
}

func (s *decodeState) value(decoder *json.Decoder, depth int) (any, error) {
	if depth > s.limits.MaxDepth {
		return nil, errors.New("JSON exceeds nesting limit")
	}
	s.nodes++
	if s.nodes > s.limits.MaxNodes {
		return nil, errors.New("JSON exceeds node limit")
	}
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	switch token := token.(type) {
	case json.Delim:
		switch token {
		case '{':
			return s.object(decoder, depth)
		case '[':
			return s.array(decoder, depth)
		default:
			return nil, errors.New("unexpected JSON delimiter")
		}
	case nil, bool, string, json.Number:
		return token, nil
	default:
		return nil, errors.New("unsupported JSON token")
	}
}

func (s *decodeState) object(decoder *json.Decoder, depth int) (any, error) {
	result := map[string]any{}
	for decoder.More() {
		if len(result) >= s.limits.MaxItems {
			return nil, errors.New("JSON object exceeds item limit")
		}
		key, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		name, ok := key.(string)
		if !ok {
			return nil, errors.New("JSON object key is invalid")
		}
		if _, duplicate := result[name]; duplicate {
			return nil, errors.New("JSON object has duplicate key")
		}
		value, err := s.value(decoder, depth+1)
		if err != nil {
			return nil, err
		}
		result[name] = value
	}
	if close, err := decoder.Token(); err != nil || close != json.Delim('}') {
		if err != nil {
			return nil, err
		}
		return nil, errors.New("JSON object is not closed")
	}
	return result, nil
}

func (s *decodeState) array(decoder *json.Decoder, depth int) (any, error) {
	result := make([]any, 0)
	for decoder.More() {
		if len(result) >= s.limits.MaxItems {
			return nil, errors.New("JSON array exceeds item limit")
		}
		value, err := s.value(decoder, depth+1)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	if close, err := decoder.Token(); err != nil || close != json.Delim(']') {
		if err != nil {
			return nil, err
		}
		return nil, errors.New("JSON array is not closed")
	}
	return result, nil
}

func resolve(limits Limits) Limits {
	if limits.MaxBytes == 0 {
		limits.MaxBytes = DefaultLimits.MaxBytes
	}
	if limits.MaxDepth == 0 {
		limits.MaxDepth = DefaultLimits.MaxDepth
	}
	if limits.MaxNodes == 0 {
		limits.MaxNodes = DefaultLimits.MaxNodes
	}
	if limits.MaxItems == 0 {
		limits.MaxItems = DefaultLimits.MaxItems
	}
	return limits
}
