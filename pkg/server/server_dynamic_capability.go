package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	capabilityruntime "github.com/cc-auto-agent/harness-core/pkg/app/capabilityruntime"
	"github.com/cc-auto-agent/harness-core/pkg/execution"
	"github.com/cc-auto-agent/harness-core/pkg/extensions/runner"
	"github.com/cc-auto-agent/harness-core/pkg/extensions/subagent"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

// --- dynamic capability creation ---

// dynamicCapabilityExecution is the transport-specific portion of a dynamic
// capability binding request. Runtime is omitted by older clients and defaults
// to HTTP for compatibility.
type dynamicCapabilityExecution struct {
	Runtime    string             `json:"runtime"`
	Entrypoint string             `json:"entrypoint"`
	Workdir    string             `json:"workdir"`
	Sandbox    core.SandboxPolicy `json:"sandbox"`
	Writes     bool               `json:"writes"`
	Method     string             `json:"method"`
	Headers    map[string]string  `json:"headers"`
}

type dynamicCapabilityBindRequest struct {
	Scope     core.ScopePath             `json:"scope"`
	Manifest  core.CapabilityManifest    `json:"manifest"`
	Replace   bool                       `json:"replace"`
	Execution dynamicCapabilityExecution `json:"execution"`
}

// dynamicCapabilityMount is the normalized server-side input shared by live
// binds and journal restore. It deliberately has no JSON representation: the
// request and durable payload use their own scope encodings.
type dynamicCapabilityMount struct {
	Scope                         core.ScopePath
	Manifest                      core.CapabilityManifest
	Replace                       bool
	Runtime                       string
	RuntimeImplementationRevision string
	Entrypoint                    string
	Workdir                       string
	Sandbox                       core.SandboxPolicy
	Writes                        bool
	Method                        string
	Headers                       map[string]string
}

// dynamicCapabilityJournalPayload is intentionally runtime-neutral at the
// top level. Runner bindings retain the runtime selector but omit HTTP-only
// fields; HTTP bindings retain their validated HTTP configuration.
type dynamicCapabilityJournalPayload struct {
	Capability                    string                  `json:"capability,omitempty"`
	Scope                         string                  `json:"scope"`
	Runtime                       string                  `json:"runtime"`
	Replace                       bool                    `json:"replace"`
	Manifest                      core.CapabilityManifest `json:"manifest"`
	Entrypoint                    string                  `json:"entrypoint,omitempty"`
	Method                        string                  `json:"method,omitempty"`
	Headers                       map[string]string       `json:"headers,omitempty"`
	Workdir                       string                  `json:"workdir,omitempty"`
	Sandbox                       core.SandboxPolicy      `json:"sandbox,omitempty"`
	Writes                        bool                    `json:"writes,omitempty"`
	RuntimeImplementationRevision string                  `json:"runtime_implementation_revision,omitempty"`
}

var (
	errDynamicCapabilityRunnerUnavailable   = errors.New("runner runtime is not configured")
	errDynamicCapabilitySubagentUnavailable = errors.New("subagent runtime requires session and delegation link stores")
)

const (
	dynamicSubagentMaxDepth    = subagent.MaxDelegationDepth
	dynamicSubagentMaxChildren = 256
)

func capabilityRuntimeSelector(raw string) (string, string, error) {
	if raw == "" {
		return "http", "1", nil
	}
	parts := strings.Split(raw, "@")
	if len(parts) > 2 || parts[0] == "" || strings.TrimSpace(parts[0]) != parts[0] {
		return "", "", fmt.Errorf("invalid capability runtime selector")
	}
	version := "1"
	if len(parts) == 2 {
		version = parts[1]
	}
	if version == "" || strings.TrimSpace(version) != version {
		return "", "", fmt.Errorf("invalid capability runtime selector")
	}
	return parts[0], version, nil
}

// mountDynamicCapability validates one normalized dynamic binding, constructs
// its execution provider, and mounts it. Both the HTTP request handler and
// journal restore call this method so their runtime rules cannot drift.
func (s *Server) mountDynamicCapability(ctx context.Context, binding dynamicCapabilityMount) (dynamicCapabilityMount, func(), error) {
	if s.runtime == nil || s.runtime.Capabilities == nil {
		return dynamicCapabilityMount{}, nil, fmt.Errorf("capability registry is unavailable")
	}
	if binding.Scope.Depth() == 0 {
		return dynamicCapabilityMount{}, nil, fmt.Errorf("capability scope is required")
	}
	if binding.Manifest.ID == "" || binding.Manifest.Version == "" {
		return dynamicCapabilityMount{}, nil, fmt.Errorf("manifest requires id and version")
	}
	if binding.Manifest.Tool == nil {
		binding.Manifest.Tool = &core.ToolExposure{Description: binding.Manifest.Description}
	}
	if s.capabilityRuntimes == nil {
		return dynamicCapabilityMount{}, nil, fmt.Errorf("capability runtime registry is unavailable")
	}
	if binding.Runtime == "" {
		binding.Runtime = "http"
	}

	mode := core.BindingProvide
	if binding.Replace {
		mode = core.BindingReplace
	}

	runtimeID, runtimeVersion, err := capabilityRuntimeSelector(binding.Runtime)
	if err != nil {
		return dynamicCapabilityMount{}, nil, err
	}
	if runtimeVersion == "1" {
		binding.Runtime = runtimeID
	} else {
		binding.Runtime = runtimeID + "@" + runtimeVersion
	}
	if runtimeID != "runner" {
		if err := validateDynamicHTTPHeaders(binding.Headers); err != nil {
			return dynamicCapabilityMount{}, nil, err
		}
	}
	_, err = s.capabilityRuntimes.Resolve(runtimeID, runtimeVersion)
	if err != nil {
		return dynamicCapabilityMount{}, nil, fmt.Errorf("dynamic capability runtime %q is unsupported", binding.Runtime)
	}
	created, err := s.capabilityRuntimes.Create(ctx, runtimeID, runtimeVersion, capabilityruntime.Request{Manifest: binding.Manifest, Entrypoint: binding.Entrypoint, Workdir: binding.Workdir, Sandbox: binding.Sandbox, Writes: binding.Writes, Method: binding.Method, Headers: binding.Headers})
	if err != nil {
		return dynamicCapabilityMount{}, nil, err
	}
	result := created
	binding.Manifest = result.Manifest
	binding.RuntimeImplementationRevision = result.ImplementationRevision
	provider := result.Provider
	unmount, err := s.runtime.Capabilities.Mount(core.CapabilityBinding{
		Scope: binding.Scope, Mode: mode, Manifest: binding.Manifest, Provider: provider,
	})
	if err != nil {
		return dynamicCapabilityMount{}, nil, err
	}
	return binding, unmount, nil
}

type dynamicRuntimeFactory struct {
	id     string
	create func(capabilityruntime.Request) (capabilityruntime.Result, error)
}

func (f dynamicRuntimeFactory) ID() string                   { return f.id }
func (dynamicRuntimeFactory) Version() string                { return "1" }
func (dynamicRuntimeFactory) ImplementationRevision() string { return "builtin-dynamic-capability/v1" }
func (f dynamicRuntimeFactory) New(_ context.Context, req capabilityruntime.Request) (capabilityruntime.Result, error) {
	return f.create(req)
}

func (s *Server) builtinCapabilityRuntimeFactories() []capabilityruntime.Factory {
	return []capabilityruntime.Factory{
		dynamicRuntimeFactory{id: "http", create: func(req capabilityruntime.Request) (capabilityruntime.Result, error) {
			if err := execution.ValidatePublicHTTPURL(req.Entrypoint); err != nil {
				return capabilityruntime.Result{}, err
			}
			if err := execution.ValidateHTTPMethod(req.Method); err != nil {
				return capabilityruntime.Result{}, err
			}
			if err := validateDynamicHTTPHeaders(req.Headers); err != nil {
				return capabilityruntime.Result{}, err
			}
			req.Manifest.Execution = &core.ExecutionSpec{Runtime: "http", Entrypoint: req.Entrypoint, Method: req.Method, Headers: req.Headers}
			return capabilityruntime.Result{Provider: core.ExecutorProvider{Executor: execution.HTTPExecutor{}, Spec: *req.Manifest.Execution}, Manifest: req.Manifest}, nil
		}},
		dynamicRuntimeFactory{id: "runner", create: func(req capabilityruntime.Request) (capabilityruntime.Result, error) {
			if s.runners == nil || s.runners.Store == nil {
				return capabilityruntime.Result{}, errDynamicCapabilityRunnerUnavailable
			}
			if req.Entrypoint != "" {
				return capabilityruntime.Result{}, fmt.Errorf("runner runtime does not accept execution.entrypoint")
			}
			if req.Method != "" {
				return capabilityruntime.Result{}, fmt.Errorf("runner runtime does not accept execution.method")
			}
			if len(req.Headers) != 0 {
				return capabilityruntime.Result{}, fmt.Errorf("runner runtime does not accept execution.headers")
			}
			req.Manifest.Execution = &core.ExecutionSpec{Runtime: "runner"}
			return capabilityruntime.Result{Provider: runner.Provider{Hub: s.runners, Capability: req.Manifest.ID}, Manifest: req.Manifest}, nil
		}},
		dynamicRuntimeFactory{id: "subagent", create: func(req capabilityruntime.Request) (capabilityruntime.Result, error) {
			binding := dynamicCapabilityMount{Manifest: req.Manifest, Entrypoint: req.Entrypoint, Workdir: req.Workdir, Sandbox: req.Sandbox, Writes: req.Writes, Method: req.Method, Headers: req.Headers}
			if req.Manifest.Kind != core.KindAgent {
				return capabilityruntime.Result{}, fmt.Errorf("subagent runtime requires manifest kind %q", core.KindAgent)
			}
			if s.sessions == nil || s.delegationLinks == nil {
				return capabilityruntime.Result{}, errDynamicCapabilitySubagentUnavailable
			}
			if err := validateDynamicSubagentExecution(binding); err != nil {
				return capabilityruntime.Result{}, err
			}
			maxDepth, maxChildren, maxToolCalls, err := dynamicSubagentLimits(req.Manifest.Metadata)
			if err != nil {
				return capabilityruntime.Result{}, err
			}
			agent, err := subagent.NewCapability(s.runtime, req.Manifest.ID, subagent.Options{ProfileID: req.Entrypoint, Sessions: s.sessions, Links: s.delegationLinks, MaxDepth: maxDepth, MaxChildren: maxChildren, DelegationMaxToolCalls: maxToolCalls})
			if err != nil {
				return capabilityruntime.Result{}, err
			}
			if req.Manifest.Tool == nil || req.Manifest.Tool.Parameters == nil {
				defaults := agent.Manifest().Tool
				if req.Manifest.Tool == nil {
					req.Manifest.Tool = defaults
				} else {
					req.Manifest.Tool.Parameters = defaults.Parameters
				}
			}
			req.Manifest.Contract = subagent.ContractV1
			req.Manifest.Execution = &core.ExecutionSpec{Runtime: "subagent", Entrypoint: req.Entrypoint}
			return capabilityruntime.Result{Provider: agent, Manifest: req.Manifest}, nil
		}},
	}
}

func (binding dynamicCapabilityMount) journalPayload() dynamicCapabilityJournalPayload {
	manifest := binding.Manifest
	// Runtime selection lives at the payload top level. The registry's mounted
	// manifest retains its execution metadata, while the durable payload avoids
	// serializing empty HTTP fields for runner bindings.
	manifest.Execution = nil
	payload := dynamicCapabilityJournalPayload{
		Capability: manifest.ID, Scope: binding.Scope.String(), Runtime: binding.Runtime,
		Replace: binding.Replace, Manifest: manifest,
	}
	runtimeID, _, _ := capabilityRuntimeSelector(binding.Runtime)
	if runtimeID != "runner" {
		payload.Entrypoint = binding.Entrypoint
		payload.Method = binding.Method
		payload.Headers = binding.Headers
	}
	payload.Workdir, payload.Sandbox, payload.Writes = binding.Workdir, binding.Sandbox, binding.Writes
	payload.RuntimeImplementationRevision = binding.RuntimeImplementationRevision
	return payload
}

// handleBindCapability mounts a new HTTP- or runner-backed capability at one
// scope from the console. The binding is reversible through /v1/admin/bindings.
func (s *Server) handleBindCapability(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	var request dynamicCapabilityBindRequest
	if !s.decodeJSON(w, r, &request) {
		return
	}
	if !canMutateScope(principal, request.Scope) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "principal does not own the scope"})
		return
	}
	mounted, unmount, err := s.mountDynamicCapability(r.Context(), dynamicCapabilityMount{
		Scope: request.Scope, Manifest: request.Manifest, Replace: request.Replace,
		Runtime: request.Execution.Runtime, Entrypoint: request.Execution.Entrypoint,
		Workdir: request.Execution.Workdir, Sandbox: request.Execution.Sandbox, Writes: request.Execution.Writes,
		Method: request.Execution.Method, Headers: request.Execution.Headers,
	})
	if err != nil {
		if errors.Is(err, errDynamicCapabilityRunnerUnavailable) || errors.Is(err, errDynamicCapabilitySubagentUnavailable) {
			writeJSON(w, http.StatusNotImplemented, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	payload := mounted.journalPayload()
	id, err := s.addBinding(r.Context(), "capability", request.Scope, payload, unmount)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	s.recordAudit(r, principal, "capability.bind", mounted.Manifest.ID, map[string]any{
		"scope": request.Scope.String(), "binding_id": id, "runtime": mounted.Runtime,
	})
	writeJSON(w, http.StatusCreated, map[string]string{"capability": mounted.Manifest.ID, "binding_id": id})
}

func validateDynamicSubagentExecution(binding dynamicCapabilityMount) error {
	if err := core.ValidateNamespacedID(binding.Entrypoint); err != nil {
		return fmt.Errorf("subagent execution.entrypoint must be a child profile id: %w", err)
	}
	if binding.Method != "" || len(binding.Headers) != 0 || binding.Workdir != "" || binding.Writes || binding.Sandbox.Mode != "" {
		return fmt.Errorf("subagent runtime only accepts execution.runtime and execution.entrypoint")
	}
	if spec := binding.Manifest.Execution; spec != nil &&
		(spec.Runtime != "" || spec.Entrypoint != "" || spec.Method != "" || len(spec.Headers) != 0 || spec.Workdir != "" || spec.Writes || spec.Sandbox.Mode != "") {
		return fmt.Errorf("subagent runtime does not accept manifest.execution; use the outer execution object")
	}
	return nil
}

func dynamicSubagentLimits(metadata map[string]string) (maxDepth, maxChildren, maxToolCalls int, err error) {
	for key, raw := range metadata {
		if !strings.HasPrefix(key, "subagent.") {
			continue
		}
		var target *int
		var maximum int
		switch key {
		case "subagent.max_depth":
			target, maximum = &maxDepth, dynamicSubagentMaxDepth
		case "subagent.max_children":
			target, maximum = &maxChildren, dynamicSubagentMaxChildren
		case "subagent.max_tool_calls":
			target, maximum = &maxToolCalls, core.HardMaxToolCalls
		default:
			return 0, 0, 0, fmt.Errorf("unknown subagent metadata key %q", key)
		}
		if raw == "" || strings.TrimSpace(raw) != raw {
			return 0, 0, 0, fmt.Errorf("%s must be a decimal integer", key)
		}
		for _, r := range raw {
			if r < '0' || r > '9' {
				return 0, 0, 0, fmt.Errorf("%s must be a decimal integer", key)
			}
		}
		value, parseErr := strconv.Atoi(raw)
		if parseErr != nil || value < 1 || value > maximum {
			return 0, 0, 0, fmt.Errorf("%s must be between 1 and %d", key, maximum)
		}
		*target = value
	}
	return maxDepth, maxChildren, maxToolCalls, nil
}

func validateDynamicHTTPHeaders(headers map[string]string) error {
	for name, value := range headers {
		if err := execution.ValidateHTTPHeader(name, value); err != nil {
			return err
		}
		if strings.HasPrefix(value, "$credential:") {
			if strings.TrimPrefix(value, "$credential:") == "" {
				return fmt.Errorf("header %s has an empty credential reference", name)
			}
			continue
		}
		if !publicLiteralHTTPHeader(name) {
			return fmt.Errorf("header %s must use a $credential reference", name)
		}
	}
	return nil
}

func publicLiteralHTTPHeader(name string) bool {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "accept", "content-type", "user-agent":
		return true
	default:
		return false
	}
}
