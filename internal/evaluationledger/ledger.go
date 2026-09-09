// Package evaluationledger collects bounded, content-free execution facts for
// one evaluation case. It is deliberately internal: the public evaluation
// DTO owns the durable projection while this package only decorates a runtime
// copy used for that one case.
package evaluationledger

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"github.com/whhhh1500/auto-agent/pkg/core"
)

const (
	outcomeOK    = "ok"
	outcomeError = "error"
	gateAccepted = "accepted"
	gateDenied   = "denied"
)

// Ledger is a content-free projection of observed runtime seams. Reasons are
// fixed categories, never propagated error messages.
type Ledger struct {
	Complete          bool              `json:"complete"`
	IncompleteReasons []string          `json:"incomplete_reasons,omitempty"`
	ModelCalls        []ModelCall       `json:"model_calls,omitempty"`
	ContextAssemblies []ContextAssembly `json:"context_assemblies,omitempty"`
	GateCalls         []GateCall        `json:"gate_calls,omitempty"`
}

type Usage struct {
	InputTokens  int64 `json:"input_tokens"`
	OutputTokens int64 `json:"output_tokens"`
}

type ModelCall struct {
	Step         int    `json:"step"`
	Invoked      bool   `json:"invoked"`
	Outcome      string `json:"outcome"`
	Usage        *Usage `json:"usage,omitempty"`
	UsageReports int    `json:"usage_reports"`
}

type ContextAssembly struct {
	ContextWindowTokens int    `json:"context_window_tokens"`
	MaxOutputTokens     int    `json:"max_output_tokens"`
	InputBytes          int64  `json:"input_bytes"`
	InputTokens         int64  `json:"input_tokens"`
	DroppedGroups       int    `json:"dropped_groups"`
	Outcome             string `json:"outcome"`
}

type GateCall struct {
	Step    int    `json:"step"`
	Outcome string `json:"outcome"`
}

// Collector wraps only a shallow copy of a runtime. Its mutable state is
// private to one evaluation case and safe if an adapter emits concurrently.
type Collector struct {
	mu             sync.Mutex
	model          map[int]*ModelCall
	contexts       []ContextAssembly
	gates          []GateCall
	gateConfigured bool
	reasons        map[string]bool
}

func New() *Collector { return &Collector{model: map[int]*ModelCall{}, reasons: map[string]bool{}} }

// Runtime returns a shallow runtime copy with only the model seams decorated.
func (c *Collector) Runtime(runtime *core.Runtime) *core.Runtime {
	if runtime == nil {
		return nil
	}
	copyOf := *runtime
	copyOf.Models = modelResolver{base: runtime.Models, collector: c}
	if runtime.ContextAssembler != nil {
		copyOf.ContextAssembler = contextAssembler{base: runtime.ContextAssembler, collector: c}.assemble
	} else {
		c.incomplete("context_accounting_unavailable")
	}
	if runtime.ModelCallGate != nil {
		c.mu.Lock()
		c.gateConfigured = true
		c.mu.Unlock()
		copyOf.ModelCallGate = gate{base: runtime.ModelCallGate, collector: c}
	}
	return &copyOf
}

type modelResolver struct {
	base      core.ModelResolver
	collector *Collector
}

func (r modelResolver) ResolveModel(ctx context.Context, selection core.ModelSelection) (adapter core.LlmAdapter, err error) {
	defer func() {
		if recover() != nil {
			r.collector.incomplete("model_resolver_error")
			adapter, err = nil, fmt.Errorf("model resolver panicked")
		}
	}()
	adapter, err = r.base.ResolveModel(ctx, selection)
	if err != nil {
		r.collector.incomplete("model_resolver_error")
		return nil, err
	}
	if adapter == nil {
		r.collector.incomplete("model_resolver_nil")
		return nil, nil
	}
	return modelAdapter{base: adapter, collector: r.collector}, nil
}

type modelAdapter struct {
	base      core.LlmAdapter
	collector *Collector
}

func (a modelAdapter) Provider() string { return a.base.Provider() }
func (a modelAdapter) ArtifactRevision() string {
	if revision, ok := a.base.(interface{ ArtifactRevision() string }); ok {
		return revision.ArtifactRevision()
	}
	return ""
}
func (a modelAdapter) ModelContextLimits() (int, int) {
	if limits, ok := a.base.(interface{ ModelContextLimits() (int, int) }); ok {
		return limits.ModelContextLimits()
	}
	return 0, 0
}
func (a modelAdapter) Stream(ctx context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) (err error) {
	step := options.ModelCall.Step
	a.collector.modelStarted(step)
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("model adapter panicked")
		}
		a.collector.modelFinished(step, err == nil)
	}()
	err = a.base.Stream(ctx, options, func(chunk core.StreamChunk) {
		if chunk.Usage != nil {
			a.collector.modelUsage(step, *chunk.Usage)
		}
		emit(chunk)
	})
	return err
}

type contextAssembler struct {
	base      core.ModelContextAssembler
	collector *Collector
}

func (a contextAssembler) assemble(ctx context.Context, request core.ModelContext) (result core.ModelContext, err error) {
	defer func() {
		if recover() != nil {
			result, err = core.ModelContext{}, fmt.Errorf("model context assembler panicked")
		}
		a.collector.context(request, result, err == nil)
	}()
	result, err = a.base(ctx, request)
	return result, err
}

type gate struct {
	base      core.ModelCallGate
	collector *Collector
}

func (g gate) AuthorizeModelCall(ctx context.Context, request core.ModelCallRequest) (err error) {
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("model call gate panicked")
		}
		g.collector.gateResult(request.Step, err == nil)
	}()
	err = g.base.AuthorizeModelCall(ctx, request)
	return err
}

func (c *Collector) modelStarted(step int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.model[step]; exists {
		c.reasons["duplicate_adapter_step"] = true
		return
	}
	c.model[step] = &ModelCall{Step: step, Invoked: true, Outcome: outcomeOK}
}
func (c *Collector) modelUsage(step int, usage core.TokenUsage) {
	c.mu.Lock()
	defer c.mu.Unlock()
	call, exists := c.model[step]
	if !exists {
		c.reasons["adapter_usage_without_call"] = true
		return
	}
	call.UsageReports++
	if call.UsageReports != 1 {
		c.reasons["adapter_usage_duplicate"] = true
		return
	}
	if usage.InputTokens < 0 || usage.OutputTokens < 0 || usage.InputTokens > core.MaxReportedTokensPerCall || usage.OutputTokens > core.MaxReportedTokensPerCall {
		c.reasons["adapter_usage_invalid"] = true
		return
	}
	call.Usage = &Usage{InputTokens: usage.InputTokens, OutputTokens: usage.OutputTokens}
}
func (c *Collector) modelFinished(step int, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	call := c.model[step]
	if call == nil {
		c.reasons["adapter_finish_without_call"] = true
		return
	}
	if !ok {
		call.Outcome = outcomeError
		c.reasons["adapter_error"] = true
	}
}
func (c *Collector) context(request, result core.ModelContext, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := ContextAssembly{ContextWindowTokens: request.ContextWindowTokens, MaxOutputTokens: request.MaxOutputTokens, Outcome: outcomeOK}
	if ok {
		entry.InputBytes, entry.InputTokens, entry.DroppedGroups = result.InputBytes, result.InputTokens, result.DroppedGroups
	} else {
		entry.Outcome = outcomeError
		c.reasons["context_assembly_error"] = true
	}
	c.contexts = append(c.contexts, entry)
}
func (c *Collector) gateResult(step int, accepted bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	outcome := gateAccepted
	if !accepted {
		outcome = gateDenied
		c.reasons["model_gate_denied"] = true
	}
	c.gates = append(c.gates, GateCall{Step: step, Outcome: outcome})
}
func (c *Collector) incomplete(reason string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reasons[reason] = true
}

// Project reconciles every observed model adapter usage with the exact durable
// model:<EvStepStart.Seq> EvRunUsage evidence for that adapter's Step. Summary
// usage and other invocation identifiers are outside this model-only check.
func (c *Collector) Project(events []core.SessionEvent, runID string) (*Ledger, error) {
	if err := core.ValidateRunID(runID); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	stepInvocations := map[int]string{}
	invocationSteps := map[string]int{}
	for _, event := range events {
		if event.RunID != runID || event.Type != core.EvStepStart {
			continue
		}
		var step core.StepData
		if err := json.Unmarshal(event.Data, &step); err != nil || step.Index < 0 || event.Seq <= 0 {
			c.reasons["durable_step_decode_error"] = true
			continue
		}
		if _, exists := stepInvocations[step.Index]; exists {
			c.reasons["durable_step_duplicate"] = true
			continue
		}
		invocation := fmt.Sprintf("model:%d", event.Seq)
		stepInvocations[step.Index] = invocation
		invocationSteps[invocation] = step.Index
	}
	durable := map[string][]Usage{}
	for _, event := range events {
		if event.RunID != runID || event.Type != core.EvRunUsage {
			continue
		}
		var usage core.RunUsageData
		if err := json.Unmarshal(event.Data, &usage); err != nil {
			c.reasons["durable_usage_decode_error"] = true
			continue
		}
		if _, known := invocationSteps[usage.InvocationID]; !known {
			if len(usage.InvocationID) >= len("model:") && usage.InvocationID[:len("model:")] == "model:" {
				c.reasons["durable_usage_without_adapter"] = true
			}
			continue
		}
		if usage.InputTokens < 0 || usage.OutputTokens < 0 || usage.InputTokens > core.MaxReportedTokensPerCall || usage.OutputTokens > core.MaxReportedTokensPerCall {
			c.reasons["durable_usage_invalid"] = true
			continue
		}
		if len(durable[usage.InvocationID]) > 0 {
			c.reasons["durable_usage_duplicate"] = true
		}
		durable[usage.InvocationID] = append(durable[usage.InvocationID], Usage{InputTokens: usage.InputTokens, OutputTokens: usage.OutputTokens})
	}
	steps := make([]int, 0, len(c.model))
	for step := range c.model {
		steps = append(steps, step)
	}
	sort.Ints(steps)
	for _, step := range steps {
		call := c.model[step]
		if call.UsageReports != 1 || call.Usage == nil {
			c.reasons["adapter_usage_missing"] = true
		}
		invocation, found := stepInvocations[step]
		if !found {
			c.reasons["durable_step_missing"] = true
			continue
		}
		entries := durable[invocation]
		if len(entries) == 0 {
			c.reasons["durable_usage_missing"] = true
			continue
		}
		if len(entries) != 1 {
			c.reasons["durable_usage_duplicate"] = true
			continue
		}
		if call.Usage != nil && *call.Usage != entries[0] {
			c.reasons["durable_usage_conflict"] = true
		}
	}
	for step := range stepInvocations {
		if _, found := c.model[step]; !found {
			c.reasons["durable_step_without_adapter"] = true
		}
	}
	if len(steps) == 0 {
		c.reasons["adapter_calls_unavailable"] = true
	}
	if len(c.contexts) != len(steps) {
		c.reasons["context_accounting_unavailable"] = true
	}
	if c.gateConfigured {
		gateCounts := map[int]int{}
		for _, gateCall := range c.gates {
			gateCounts[gateCall.Step]++
			if _, found := c.model[gateCall.Step]; !found {
				c.reasons["model_gate_accounting_unavailable"] = true
			}
		}
		for _, step := range steps {
			if gateCounts[step] != 1 {
				c.reasons["model_gate_accounting_unavailable"] = true
			}
		}
		if len(c.gates) != len(steps) {
			c.reasons["model_gate_accounting_unavailable"] = true
		}
	}
	ledger := &Ledger{Complete: len(c.reasons) == 0, ContextAssemblies: append([]ContextAssembly(nil), c.contexts...), GateCalls: append([]GateCall(nil), c.gates...)}
	ledger.ModelCalls = make([]ModelCall, 0, len(steps))
	for _, step := range steps {
		entry := *c.model[step]
		if entry.Usage != nil {
			usage := *entry.Usage
			entry.Usage = &usage
		}
		ledger.ModelCalls = append(ledger.ModelCalls, entry)
	}
	for reason := range c.reasons {
		ledger.IncompleteReasons = append(ledger.IncompleteReasons, reason)
	}
	sort.Strings(ledger.IncompleteReasons)
	return ledger, nil
}
