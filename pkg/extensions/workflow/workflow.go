// Package workflow composes core capabilities into an optional sequential
// workflow capability.
package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/extensions/internal/support"
)

const ContractV1 = "harness.workflow/v1"

type Step struct {
	Name         string
	CapabilityID string
	Args         map[string]any
}

type Definition struct {
	Description string
	InputSchema map[string]any
	Steps       []Step
}

type Capability struct {
	id  string
	def Definition
}

func (*Capability) ArtifactRevision() string { return "workflow/v2-protected-invoker" }

func NewCapability(id string, def Definition) (*Capability, error) {
	if err := support.ValidateCapabilityID(id); err != nil {
		return nil, err
	}
	if len(def.Steps) == 0 {
		return nil, fmt.Errorf("workflow %q has no steps", id)
	}
	if len(def.Steps) > core.HardMaxToolCalls {
		return nil, fmt.Errorf("workflow %q exceeds %d steps", id, core.HardMaxToolCalls)
	}
	inputSchema, err := support.CloneJSONMap(def.InputSchema)
	if err != nil {
		return nil, fmt.Errorf("workflow %q input schema: %w", id, err)
	}
	if err := core.ValidateSchema(inputSchema); err != nil {
		return nil, fmt.Errorf("workflow %q input schema: %w", id, err)
	}
	def.InputSchema = inputSchema
	def.Steps = append([]Step(nil), def.Steps...)
	seen := map[string]bool{}
	for i, step := range def.Steps {
		if err := validateStepName(step.Name); err != nil {
			return nil, fmt.Errorf("workflow %q step %d: %w", id, i, err)
		}
		if seen[step.Name] {
			return nil, fmt.Errorf("workflow %q step %d has an empty or duplicate name %q", id, i, step.Name)
		}
		seen[step.Name] = true
		if strings.TrimSpace(step.CapabilityID) == "" {
			return nil, fmt.Errorf("workflow %q step %q has no capability ID", id, step.Name)
		}
		if err := support.ValidateCapabilityID(step.CapabilityID); err != nil {
			return nil, fmt.Errorf("workflow %q step %q: %w", id, step.Name, err)
		}
		args, err := support.CloneJSONMap(step.Args)
		if err != nil {
			return nil, fmt.Errorf("workflow %q step %q args: %w", id, step.Name, err)
		}
		def.Steps[i].Args = args
	}
	return &Capability{id: id, def: def}, nil
}

func validateStepName(name string) error {
	if name == "" || strings.TrimSpace(name) != name || len(name) > 64 {
		return fmt.Errorf("workflow step name %q is invalid", name)
	}
	if strings.ContainsAny(name, "/\\ \t\r\n\x00") {
		return fmt.Errorf("workflow step name %q is invalid", name)
	}
	return nil
}

func (w *Capability) Manifest() core.CapabilityManifest {
	parameters, _ := support.CloneJSONMap(w.def.InputSchema)
	if parameters == nil {
		parameters = support.ObjectSchema(map[string]any{})
	}
	return core.CapabilityManifest{
		ID: w.id, Version: "1.0.0", Name: w.id, Description: w.def.Description,
		Kind: core.KindWorkflow, Contract: ContractV1,
		Idempotent: true,
		Tool:       &core.ToolExposure{Description: w.def.Description, Parameters: parameters},
	}
}

func (w *Capability) Execute(ctx context.Context, request core.CapabilityRequest) (core.CapabilityResult, error) {
	if request.Context.Invoker == nil {
		return support.DeniedResult(
			"workflow_invoker_unavailable",
			"workflow requires the active run's protected tool invoker",
		), nil
	}
	vars := map[string]any{"input": request.Args}
	outcomes := make([]map[string]any, 0, len(w.def.Steps))
	var last core.CapabilityResult

	for _, step := range w.def.Steps {
		resolvedArgs, err := resolveRefs(step.Args, vars)
		if err != nil {
			return support.DeniedResult("workflow_ref_missing", fmt.Sprintf("step %s: %v", step.Name, err)), nil
		}
		args, _ := resolvedArgs.(map[string]any)
		callID := request.CallID + "/" + step.Name
		if request.CallID == "" || len(callID) > 256 {
			return support.DeniedResult(core.CodeInvalidArgs, "workflow step call id is invalid"), nil
		}
		result, execErr := request.Context.Invoker.InvokeTool(ctx, core.ToolCall{
			ID: callID, Name: step.CapabilityID, Args: args,
		})
		if execErr != nil {
			var pending *core.ApprovalPendingError
			if errors.As(execErr, &pending) {
				return core.CapabilityResult{}, execErr
			}
			result = core.CapabilityResult{Content: execErr.Error(), OK: false}
		}
		outcomes = append(outcomes, map[string]any{"name": step.Name, "ok": result.OK})

		var data any
		if err := json.Unmarshal([]byte(result.Content), &data); err != nil {
			data = nil
		}
		vars[step.Name] = map[string]any{
			"content": result.Content, "ok": result.OK, "metadata": result.Metadata, "data": data,
		}

		if !result.OK {
			return core.CapabilityResult{
				Content:  fmt.Sprintf("workflow step %s failed: %s", step.Name, result.Content),
				OK:       false,
				Metadata: map[string]any{"workflow.steps": outcomes, "workflow.failed_step": step.Name},
			}, nil
		}
		last = result
	}
	return core.CapabilityResult{
		Content: last.Content, OK: true,
		Metadata: map[string]any{"workflow.steps": outcomes},
	}, nil
}

func resolveRefs(value any, vars map[string]any) (any, error) {
	switch typed := value.(type) {
	case string:
		if path, ok := strings.CutPrefix(typed, "$ref:"); ok {
			return lookupPath(vars, path)
		}
		return typed, nil
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			resolved, err := resolveRefs(item, vars)
			if err != nil {
				return nil, err
			}
			out[key] = resolved
		}
		return out, nil
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			resolved, err := resolveRefs(item, vars)
			if err != nil {
				return nil, err
			}
			out[i] = resolved
		}
		return out, nil
	default:
		return value, nil
	}
}

func lookupPath(vars map[string]any, path string) (any, error) {
	current := any(vars)
	for _, segment := range strings.Split(path, ".") {
		switch node := current.(type) {
		case map[string]any:
			value, ok := node[segment]
			if !ok {
				return nil, fmt.Errorf("reference %q has no key %q", path, segment)
			}
			current = value
		case []any:
			index := -1
			for _, c := range segment {
				if c < '0' || c > '9' {
					index = -1
					break
				}
				if index < 0 {
					index = 0
				}
				index = index*10 + int(c-'0')
			}
			if index < 0 || index >= len(node) {
				return nil, fmt.Errorf("reference %q has no index %q", path, segment)
			}
			current = node[index]
		default:
			return nil, fmt.Errorf("reference %q is not addressable at %q", path, segment)
		}
	}
	return current, nil
}
