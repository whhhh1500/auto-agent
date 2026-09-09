// Package toolcapability composes the public programmatic catalog, bounded PTC
// interpreter, and protected core invoker into ordinary core capabilities.
// It does not provide a CodePTC runner.
package toolcapability

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"

	"github.com/whhhh1500/auto-agent/pkg/adapter/programmatic/corebridge"
	"github.com/whhhh1500/auto-agent/pkg/adapter/programmatic/internal/jsonvalue"
	access "github.com/whhhh1500/auto-agent/pkg/app/programmatic"
	core "github.com/whhhh1500/auto-agent/pkg/core"
	ptc "github.com/whhhh1500/auto-agent/pkg/execution/programmatic"
)

const (
	CatalogID = "program.catalog"
	ExecuteID = "program.execute"

	ContractV1 = "harness.programmatic/v1"

	maxCatalogOutputBytes = 256 << 10
	maxExecuteOutputBytes = 256 << 10
)

const executeToolDescription = "Use direct tools for a simple operation; use this bounded deterministic PTC interpreter for loops or branches. Call program.catalog first for grammar, public tool and output schemas, and binding digests, then bind every literal call target, including conditional branches. The program receives input through input. Each successful call assigns the complete host envelope {ok, content, data}; a descriptor OutputSchema describes only data. A for accepts a list, so when a call's data is an array, get its data member from the assign variable before for; do not loop over the envelope. data may be null. The final result contains value, steps, tool_calls, and arena_bytes. If program_invalid returns a fixed diagnostic, reread catalog language; prior child effects may already have occurred, so do not assume retry is safe. This capability does not run CodePTC."

var errToolData = errors.New("programmatic tool result data is invalid")

// bridgeInvocationError marks an error returned by the protected child-call
// boundary. It is intentionally private: a host error, including one that
// happens to equal a VM sentinel, must leave Execute unchanged.
type bridgeInvocationError struct{ cause error }

func (e *bridgeInvocationError) Error() string { return e.cause.Error() }
func (e *bridgeInvocationError) Unwrap() error { return e.cause }

// CatalogCapability returns the current run's explicitly programmatic tool
// descriptors. It is a capability itself, not an exposed program target.
type CatalogCapability struct{ id string }

// ExecuteCapability compiles and runs one bounded PTC program through the
// active protected tool invoker. It is not a CodePTC executor.
type ExecuteCapability struct{ id string }

func NewCatalogCapability(id string) (*CatalogCapability, error) {
	if err := core.ValidateNamespacedID(id); err != nil {
		return nil, err
	}
	return &CatalogCapability{id: id}, nil
}

func NewExecuteCapability(id string) (*ExecuteCapability, error) {
	if err := core.ValidateNamespacedID(id); err != nil {
		return nil, err
	}
	return &ExecuteCapability{id: id}, nil
}

func (*CatalogCapability) ArtifactRevision() string {
	return "programmatic-extension/v6-container-expression-guidance"
}
func (*ExecuteCapability) ArtifactRevision() string {
	return "programmatic-extension/v6-container-expression-guidance"
}

func (c *CatalogCapability) Manifest() core.CapabilityManifest {
	return core.CapabilityManifest{
		ID: c.id, Version: "1.0.0", Name: c.id,
		Description: "Return PTC grammar, public tool and output schemas, and frozen binding digests approved for program.execute in this run.",
		Kind:        core.KindKnowledge, Contract: ContractV1, Idempotent: true,
		MaxOutputBytes: maxCatalogOutputBytes,
		Tool:           &core.ToolExposure{Description: "Return PTC grammar, program-callable tool and output schemas, and frozen binding digests.", Parameters: emptyObjectSchema()},
		OutputSchema:   catalogOutputSchema(),
	}
}

func (c *ExecuteCapability) Manifest() core.CapabilityManifest {
	return core.CapabilityManifest{
		ID: c.id, Version: "1.0.0", Name: c.id,
		Description: executeToolDescription,
		Kind:        core.KindWorkflow, Contract: ContractV1, Idempotent: true,
		MaxOutputBytes: maxExecuteOutputBytes,
		Tool:           &core.ToolExposure{Description: executeToolDescription, Parameters: executeInputSchema()},
		OutputSchema:   executeOutputSchema(),
	}
}

func (c *CatalogCapability) Execute(ctx context.Context, request core.CapabilityRequest) (core.CapabilityResult, error) {
	if err := core.RequireAcceptedInvocation(request); err != nil {
		return core.CapabilityResult{}, err
	}
	catalog, failure := currentCatalogAccepted(request)
	if failure != nil {
		return *failure, nil
	}
	return marshalCatalog(catalog.Descriptors(), maxCatalogOutputBytes)
}

func (c *ExecuteCapability) Execute(ctx context.Context, request core.CapabilityRequest) (core.CapabilityResult, error) {
	if err := core.RequireAcceptedInvocation(request); err != nil {
		return core.CapabilityResult{}, err
	}
	source, bindings, input, failure := parseExecuteArgs(request.Args)
	if failure != nil {
		return *failure, nil
	}
	program, err := ptc.Compile([]byte(source))
	if err != nil {
		return deniedProgramDiagnostic(err), nil
	}
	if !sameToolSet(program.Tools(), bindings) {
		return denied("program_bindings_mismatch", "program tools must exactly match submitted bindings"), nil
	}
	catalog, catalogFailure := currentCatalogAccepted(request)
	if catalogFailure != nil {
		return *catalogFailure, nil
	}
	bridge, err := corebridge.New(request, catalog, bindings, program.Digest())
	if err != nil {
		if errors.Is(err, corebridge.ErrInvalidBridgeCall) {
			return denied("program_invocation_invalid", "program invocation cannot support nested calls"), nil
		}
		return denied("program_bindings_mismatch", "program bindings are not available in this run"), nil
	}
	result, err := program.Run(ctx, input, func(callCtx context.Context, call ptc.Call) (any, error) {
		toolResult, err := bridge.CallTool(callCtx, corebridge.Call{CapabilityID: call.Tool, Ordinal: call.Ordinal, Args: call.Args})
		if err != nil {
			return nil, &bridgeInvocationError{cause: err}
		}
		data, err := toolData(toolResult.Content)
		if err != nil {
			return nil, err
		}
		return map[string]any{"ok": true, "content": toolResult.Content, "data": data}, nil
	}, ptc.Limits{})
	if err != nil {
		var invocationErr *bridgeInvocationError
		if errors.As(err, &invocationErr) {
			return core.CapabilityResult{}, invocationErr.cause
		}
		if errors.Is(err, ptc.ErrInvalidProgram) {
			return deniedProgramDiagnostic(err), nil
		}
		if errors.Is(err, ptc.ErrProgramLimit) {
			return denied("program_limit_exceeded", "program resource limit was exceeded"), nil
		}
		if errors.Is(err, errToolData) {
			return denied("program_tool_data_invalid", "tool result data could not be decoded safely"), nil
		}
		// Cancellation, approval-pending, journal uncertainty, and every guarded
		// tool error keep their original identity for the outer runtime.
		return core.CapabilityResult{}, err
	}
	return marshalResult(map[string]any{"value": result.Value, "steps": result.Steps, "tool_calls": result.ToolCalls, "arena_bytes": result.ArenaBytes}, maxExecuteOutputBytes)
}

type catalogDescriptor struct {
	Schema            core.ToolSchema `json:"schema"`
	CapabilityVersion string          `json:"capability_version"`
	BindingDigest     string          `json:"binding_digest"`
	OutputSchema      map[string]any  `json:"output_schema,omitempty"`
	MaxOutputBytes    int             `json:"max_output_bytes"`
}

func currentCatalogAccepted(request core.CapabilityRequest) (*access.Catalog, *core.CapabilityResult) {
	metadata, ok := request.Context.Data.(func() []core.SnapshotCapability)
	if !ok || metadata == nil {
		return nil, resultPointer(denied("program_catalog_unavailable", "current capability metadata is unavailable"))
	}
	var capabilities []core.SnapshotCapability
	called := false
	func() {
		defer func() { _ = recover() }()
		capabilities = metadata()
		called = true
	}()
	if !called || capabilities == nil {
		return nil, resultPointer(denied("program_catalog_unavailable", "current capability metadata is unavailable"))
	}
	// The two composite capabilities must never be reintroduced as program
	// targets, even if a host accidentally adds the generic exposure marker.
	// Their IDs are configurable, so the contract identifies this boundary.
	filtered := make([]core.SnapshotCapability, 0, len(capabilities))
	for _, capability := range capabilities {
		if capability.Manifest.Contract == ContractV1 {
			if _, optedIn := capability.Manifest.Metadata[access.ExposureKey]; optedIn {
				return nil, resultPointer(denied("program_catalog_invalid", "program capabilities cannot be program targets"))
			}
			continue
		}
		filtered = append(filtered, capability)
	}
	catalog, err := access.Project(filtered)
	if err != nil {
		return nil, resultPointer(denied("program_catalog_invalid", "current programmatic catalog is invalid"))
	}
	return catalog, nil
}

func parseExecuteArgs(args map[string]any) (string, map[string]string, map[string]any, *core.CapabilityResult) {
	if err := core.ValidateArgs(executeInputSchema(), args); err != nil {
		return "", nil, nil, resultPointer(denied(core.CodeInvalidArgs, "program.execute arguments are invalid"))
	}
	source, ok := args["source"].(string)
	if !ok || source == "" {
		return "", nil, nil, resultPointer(denied(core.CodeInvalidArgs, "program source is required"))
	}
	rawBindings, ok := args["bindings"].(map[string]any)
	if !ok {
		return "", nil, nil, resultPointer(denied(core.CodeInvalidArgs, "program bindings must be an object"))
	}
	bindings := make(map[string]string, len(rawBindings))
	for id, rawDigest := range rawBindings {
		digest, ok := rawDigest.(string)
		if !ok || digest == "" {
			return "", nil, nil, resultPointer(denied(core.CodeInvalidArgs, "program binding digests must be strings"))
		}
		bindings[id] = digest
	}
	input := map[string]any{}
	if rawInput, exists := args["input"]; exists {
		var ok bool
		input, ok = rawInput.(map[string]any)
		if !ok {
			return "", nil, nil, resultPointer(denied(core.CodeInvalidArgs, "program input must be an object"))
		}
	}
	return source, bindings, input, nil
}

func toolData(content string) (any, error) {
	if len(content) > ptc.HardMaxToolResultBytes {
		return nil, errToolData
	}
	// Unstructured text remains available verbatim through content. Only a
	// complete JSON document may become data; never expose a decoded prefix.
	// Well-formed but ambiguous or oversized JSON still fails strict decoding.
	if !json.Valid([]byte(content)) {
		return nil, nil
	}
	decoded, err := jsonvalue.DecodeString(content, jsonvalue.DefaultLimits)
	if err != nil {
		return nil, errToolData
	}
	// The VM accepts json.Number and validates/converts it while charging its
	// detached caller-result copy. Keeping the UseNumber tree preserves large
	// integral values without a float64 round trip here.
	return decoded, nil
}

func marshalCatalog(descriptors []access.Descriptor, maximum int) (core.CapabilityResult, error) {
	var encoded bytes.Buffer
	encoded.Grow(minimum(maximum, 4096))
	header, err := json.Marshal(struct {
		Version  string `json:"version"`
		Language string `json:"language"`
	}{Version: ptc.Version, Language: ptc.LanguageGuide})
	if err != nil || len(header) < 2 || len(header)-1+len(`,"tools":[]}`) > maximum {
		return denied("program_output_too_large", "program output exceeds the public limit"), nil
	}
	// The header is fixed and bounded before adding any catalog entry, so a
	// large registry cannot force an unbounded aggregate marshal allocation.
	encoded.Write(header[:len(header)-1])
	encoded.WriteString(`,"tools":[`)
	for index, descriptor := range descriptors {
		public := catalogDescriptor{Schema: descriptor.Schema, CapabilityVersion: descriptor.CapabilityVersion, BindingDigest: descriptor.BindingDigest, OutputSchema: descriptor.OutputSchema, MaxOutputBytes: descriptor.MaxOutputBytes}
		item, err := json.Marshal(public)
		if err != nil || len(item) > maximum || encoded.Len()+len(item)+3 > maximum {
			return denied("program_output_too_large", "program output exceeds the public limit"), nil
		}
		if index > 0 {
			encoded.WriteByte(',')
		}
		encoded.Write(item)
	}
	if encoded.Len()+2 > maximum {
		return denied("program_output_too_large", "program output exceeds the public limit"), nil
	}
	encoded.WriteString(`]}`)
	return core.CapabilityResult{Content: encoded.String(), OK: true}, nil
}

func minimum(left, right int) int {
	if left < right {
		return left
	}
	return right
}

func marshalResult(value any, maximum int) (core.CapabilityResult, error) {
	encoded, err := json.Marshal(value)
	if err != nil || len(encoded) > maximum {
		return denied("program_output_too_large", "program output exceeds the public limit"), nil
	}
	return core.CapabilityResult{Content: string(encoded), OK: true}, nil
}

func sameToolSet(tools []string, bindings map[string]string) bool {
	if len(tools) != len(bindings) {
		return false
	}
	for _, tool := range tools {
		if _, ok := bindings[tool]; !ok {
			return false
		}
	}
	return true
}

func emptyObjectSchema() map[string]any {
	return map[string]any{"type": "object", "additionalProperties": false}
}

func executeInputSchema() map[string]any {
	return map[string]any{"type": "object", "required": []any{"source", "bindings"}, "additionalProperties": false, "properties": map[string]any{
		"source":   map[string]any{"type": "string", "minLength": 1, "maxLength": ptc.HardMaxSourceBytes},
		"bindings": map[string]any{"type": "object", "description": "Include exactly the tools referenced by source, including conditional branches; do not bind every catalog tool. Keys must be the exact program.catalog tools[].schema.name values used as literal tool names in source. Values must be the corresponding tools[].binding_digest values. Do not use a provider wire alias, a tool-call ID, or a digest as a key.", "additionalProperties": map[string]any{"type": "string", "minLength": 1, "maxLength": 64}},
		"input":    map[string]any{"type": "object"},
	}}
}

func catalogOutputSchema() map[string]any {
	return map[string]any{"type": "object", "required": []any{"version", "language", "tools"}, "additionalProperties": false, "properties": map[string]any{
		"version":  map[string]any{"type": "string"},
		"language": map[string]any{"type": "string"},
		"tools":    map[string]any{"type": "array", "items": map[string]any{"type": "object"}},
	}}
}

func executeOutputSchema() map[string]any {
	return map[string]any{"type": "object", "required": []any{"value", "steps", "tool_calls", "arena_bytes"}, "additionalProperties": false, "properties": map[string]any{
		"value": map[string]any{}, "steps": map[string]any{"type": "integer"}, "tool_calls": map[string]any{"type": "integer"}, "arena_bytes": map[string]any{"type": "integer"},
	}}
}

func denied(code, content string) core.CapabilityResult {
	return core.CapabilityResult{Content: content, OK: false, Metadata: map[string]any{"code": code}}
}

func deniedProgramDiagnostic(err error) core.CapabilityResult {
	diagnostic, ok := ptc.Diagnostic(err)
	if !ok {
		// VM-generated invalid-program failures are classified at their source.
		// This fixed fallback is intentionally not derived from error text.
		diagnostic = "runtime_invalid"
	}
	return core.CapabilityResult{
		Content:  "Program failed: " + diagnostic + ". Read program.catalog language and correct strict value usage. Prior child effects may already have occurred; do not assume retry is safe.",
		OK:       false,
		Metadata: map[string]any{"code": "program_invalid", "diagnostic": diagnostic},
	}
}

func resultPointer(value core.CapabilityResult) *core.CapabilityResult { return &value }
