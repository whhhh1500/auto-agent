// Package corebridge binds a programmatic catalog to the active core tool
// guard. It is a trusted adapter boundary and must never be passed to program
// code directly.
package corebridge

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"

	"github.com/whhhh1500/auto-agent/pkg/adapter/programmatic/internal/jsonvalue"
	"github.com/whhhh1500/auto-agent/pkg/app/programmatic"
	"github.com/whhhh1500/auto-agent/pkg/core"
)

const (
	maxChildCallIDBytes  = 256
	childCallIDSuffixLen = 1 + sha256.Size*2
	maxParentCallIDBytes = maxChildCallIDBytes - childCallIDSuffixLen
)

var (
	ErrInvalidBridgeCall = errors.New("invalid programmatic bridge call")
	ErrToolRejected      = errors.New("programmatic tool call was rejected")
)

// Call is the host-controlled representation of one program tool operation.
// Ordinal comes from the deterministic program runtime; identity never comes
// from the untrusted program.
type Call struct {
	CapabilityID string
	Ordinal      uint64
	Args         map[string]any
}

// Bridge exposes a fixed catalog for one accepted root capability invocation.
type Bridge struct {
	request       core.CapabilityRequest
	catalog       *programmatic.Catalog
	bindings      map[string]string
	programDigest string
}

// New proves that the root invocation passed core's guard, then freezes the
// submitted bindings. The caller must pass a catalog projected from the
// request's trusted Context.Data metadata, never from the program.
func New(request core.CapabilityRequest, catalog *programmatic.Catalog, expectedBindings map[string]string, programDigest string) (*Bridge, error) {
	if err := core.RequireAcceptedInvocation(request); err != nil {
		return nil, err
	}
	if request.Context.Invoker == nil {
		return nil, fmt.Errorf("%w: protected invoker is unavailable", ErrInvalidBridgeCall)
	}
	if !validDigest(programDigest) {
		return nil, fmt.Errorf("%w: program digest is invalid", ErrInvalidBridgeCall)
	}
	if len(expectedBindings) > 0 && len(request.CallID) > maxParentCallIDBytes {
		return nil, fmt.Errorf("%w: parent call id exceeds %d bytes for nested program calls", ErrInvalidBridgeCall, maxParentCallIDBytes)
	}
	if err := catalog.ValidateBindings(expectedBindings); err != nil {
		return nil, err
	}
	bindings := make(map[string]string, len(expectedBindings))
	for id, digest := range expectedBindings {
		bindings[id] = digest
	}
	return &Bridge{request: request, catalog: catalog, bindings: bindings, programDigest: programDigest}, nil
}

// ListTools returns only the bridge's fixed programmatic descriptors.
func (b *Bridge) ListTools(context.Context) ([]programmatic.Descriptor, error) {
	if b == nil || b.catalog == nil {
		return nil, fmt.Errorf("%w: bridge is unavailable", ErrInvalidBridgeCall)
	}
	tools := b.catalog.Descriptors()
	out := make([]programmatic.Descriptor, 0, len(b.bindings))
	for _, descriptor := range tools {
		if b.bindings[descriptor.Schema.Name] == descriptor.BindingDigest {
			out = append(out, descriptor)
		}
	}
	return out, nil
}

// CallTool validates a bound program operation and routes it back through the
// active Agent guard. A non-OK tool result is an error so a VM cannot treat a
// denial, unknown outcome, or malformed result as a catchable program value.
func (b *Bridge) CallTool(ctx context.Context, call Call) (core.CapabilityResult, error) {
	if b == nil || b.catalog == nil || ctx == nil {
		return core.CapabilityResult{}, fmt.Errorf("%w: bridge is unavailable", ErrInvalidBridgeCall)
	}
	binding, bound := b.bindings[call.CapabilityID]
	if !bound {
		return core.CapabilityResult{}, fmt.Errorf("%w: %s", programmatic.ErrCapabilityNotBound, call.CapabilityID)
	}
	descriptor, ok := b.catalog.Resolve(call.CapabilityID, binding)
	if !ok {
		return core.CapabilityResult{}, fmt.Errorf("%w: %s", programmatic.ErrBindingMismatch, call.CapabilityID)
	}
	args, encoded, err := canonicalArgs(call.Args)
	if err != nil {
		return core.CapabilityResult{}, err
	}
	if err := core.ValidateArgs(descriptor.Schema.Parameters, args); err != nil {
		return core.CapabilityResult{}, err
	}
	childID, err := childCallID(b.request.CallID, b.programDigest, call.Ordinal, call.CapabilityID, binding, encoded)
	if err != nil {
		return core.CapabilityResult{}, err
	}
	result, err := b.request.Context.Invoker.InvokeTool(ctx, core.ToolCall{ID: childID, Name: call.CapabilityID, Args: args})
	if err != nil {
		return result, err
	}
	if err := validateResult(descriptor, result); err != nil {
		return result, err
	}
	if !result.OK {
		return result, fmt.Errorf("%w: %s", ErrToolRejected, call.CapabilityID)
	}
	return result, nil
}

func canonicalArgs(args map[string]any) (map[string]any, []byte, error) {
	if args == nil {
		args = map[string]any{}
	}
	encoded, err := json.Marshal(args)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: arguments are not JSON serializable", ErrInvalidBridgeCall)
	}
	if len(encoded) > core.MaxToolArgumentBytes {
		return nil, nil, fmt.Errorf("%w: arguments exceed %d bytes", ErrInvalidBridgeCall, core.MaxToolArgumentBytes)
	}
	decoder := json.NewDecoder(strings.NewReader(string(encoded)))
	decoder.UseNumber()
	var decoded map[string]any
	if err := decoder.Decode(&decoded); err != nil {
		return nil, nil, fmt.Errorf("%w: arguments are not JSON objects", ErrInvalidBridgeCall)
	}
	normalized, err := normalizeJSONValue(decoded)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: arguments contain a non-round-trippable number", ErrInvalidBridgeCall)
	}
	cloned, ok := normalized.(map[string]any)
	if !ok {
		return nil, nil, fmt.Errorf("%w: arguments are not JSON objects", ErrInvalidBridgeCall)
	}
	// Hash the exact canonical representation passed into core. This prevents a
	// large integer from being hashed as one value and dispatched as a rounded
	// float64 after core's JSON-native defensive copy.
	encoded, err = json.Marshal(cloned)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: arguments are not JSON serializable", ErrInvalidBridgeCall)
	}
	return cloned, encoded, nil
}

func childCallID(parentID, programDigest string, ordinal uint64, capabilityID, bindingDigest string, encodedArgs []byte) (string, error) {
	if parentID == "" || !validDigest(programDigest) || !validDigest(bindingDigest) || len(encodedArgs) > core.MaxToolArgumentBytes {
		return "", fmt.Errorf("%w: child identity input", ErrInvalidBridgeCall)
	}
	canonical := struct {
		Parent, Program, Capability, Binding string
		Ordinal                              uint64
		Args                                 json.RawMessage
	}{parentID, programDigest, capabilityID, bindingDigest, ordinal, encodedArgs}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return "", fmt.Errorf("%w: child identity", ErrInvalidBridgeCall)
	}
	sum := sha256.Sum256(encoded)
	childID := parentID + "/" + hex.EncodeToString(sum[:])
	if len(childID) > maxChildCallIDBytes {
		return "", fmt.Errorf("%w: child call id exceeds %d bytes", ErrInvalidBridgeCall, maxChildCallIDBytes)
	}
	return childID, nil
}

func validateResult(descriptor programmatic.Descriptor, result core.CapabilityResult) error {
	if err := core.ValidateCapabilityResult(result); err != nil {
		return fmt.Errorf("%w: result is invalid", ErrToolRejected)
	}
	if len(result.Content) > descriptor.MaxOutputBytes {
		return fmt.Errorf("%w: output exceeds %d bytes", ErrToolRejected, descriptor.MaxOutputBytes)
	}
	if !result.OK || len(descriptor.OutputSchema) == 0 {
		return nil
	}
	output, err := decodeJSONValue(result.Content)
	if err != nil {
		return fmt.Errorf("%w: output is not JSON", ErrToolRejected)
	}
	if err := core.ValidateJSONValue(descriptor.OutputSchema, output); err != nil {
		return fmt.Errorf("%w: output schema", ErrToolRejected)
	}
	return nil
}

const maxExactJSONInteger = int64(1 << 53)

func decodeJSONValue(content string) (any, error) {
	value, err := jsonvalue.DecodeString(content, jsonvalue.DefaultLimits)
	if err != nil {
		return nil, err
	}
	return normalizeJSONValue(value)
}

func normalizeJSONValue(value any) (any, error) {
	switch value := value.(type) {
	case nil, bool, string:
		return value, nil
	case json.Number:
		text := value.String()
		if !boundedDecimalText(text) {
			return nil, errors.New("decimal exceeds exact validation limits")
		}
		if !strings.ContainsAny(text, ".eE") {
			integer, err := strconv.ParseInt(text, 10, 64)
			if err != nil || integer > maxExactJSONInteger || integer < -maxExactJSONInteger {
				return nil, errors.New("integer is not exactly representable")
			}
			return float64(integer), nil
		}
		decimal, err := strconv.ParseFloat(text, 64)
		if err != nil || math.IsNaN(decimal) || math.IsInf(decimal, 0) {
			return nil, errors.New("decimal is invalid")
		}
		// JSON decimal syntax still becomes float64 in core's schema validator.
		// Reject an integer-valued decimal outside its exact integer range rather
		// than silently changing a likely identifier or selector value.
		if math.Trunc(decimal) == decimal {
			// Rat keeps the original decimal/exponent spelling exact. ParseFloat
			// alone can turn 9007199254740993.0 into the representable 2^53.
			exact, ok := new(big.Rat).SetString(text)
			if !ok || (exact.IsInt() && new(big.Int).Abs(exact.Num()).Cmp(big.NewInt(maxExactJSONInteger)) > 0) {
				return nil, errors.New("integer-valued decimal is not exactly representable")
			}
		}
		return decimal, nil
	case []any:
		out := make([]any, len(value))
		for index, item := range value {
			normalized, err := normalizeJSONValue(item)
			if err != nil {
				return nil, err
			}
			out[index] = normalized
		}
		return out, nil
	case map[string]any:
		out := make(map[string]any, len(value))
		for key, item := range value {
			normalized, err := normalizeJSONValue(item)
			if err != nil {
				return nil, err
			}
			out[key] = normalized
		}
		return out, nil
	default:
		return nil, errors.New("unsupported JSON value")
	}
}

// boundedDecimalText prevents a short adversarial exponent such as 1e-1000000000
// from making big.Rat allocate an enormous power-of-ten denominator. JSON
// parsing already establishes the lexical grammar; this only caps resources
// before exact rational validation.
func boundedDecimalText(text string) bool {
	if len(text) == 0 || len(text) > 128 {
		return false
	}
	index := strings.IndexAny(text, "eE")
	if index < 0 {
		return true
	}
	if strings.ContainsAny(text[index+1:], "eE") {
		return false
	}
	exponent, err := strconv.ParseInt(text[index+1:], 10, 64)
	return err == nil && exponent >= -512 && exponent <= 512
}

func validDigest(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
