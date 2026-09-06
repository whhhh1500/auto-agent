package core

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// guardedToolRuntime is the single enforcement funnel for tool execution in a
// run: schema validation, guardrail hooks, fail-closed approval, and per-
// capability budgets all apply before the provider sees a call. The model loop
// and deterministic fast routes share it, so no consumer can bypass a gate.
type guardedToolRuntime struct {
	agent *Agent
	inner ToolRuntime
}

type budgetReporter interface {
	BudgetFor(name string) int
}

type manifestSource interface {
	ManifestFor(name string) (CapabilityManifest, bool)
}

type protectedToolRuntime interface {
	executeProtected(context.Context, ToolCall, ProtectedToolInvoker) (CapabilityResult, error)
}

type nestedToolInvoker struct {
	agent   *Agent
	runtime *guardedToolRuntime
}

func (i nestedToolInvoker) InvokeTool(ctx context.Context, call ToolCall) (CapabilityResult, error) {
	if result, exists, err := i.agent.opts.Session.ToolResult(i.agent.runID, call.ID); err != nil {
		return CapabilityResult{}, err
	} else if exists {
		return result, nil
	}
	if exists, err := i.agent.opts.Session.HasToolCall(i.agent.runID, call.ID); err != nil {
		return CapabilityResult{}, err
	} else if !exists {
		if err := i.agent.append(i.agent.runID, EvToolCall, ToolCallData{
			CallID: call.ID, Name: call.Name, Args: call.Args,
		}); err != nil {
			return CapabilityResult{}, err
		}
	}
	result, err := i.runtime.Execute(ctx, call)
	if _, pending := IsApprovalPending(err); pending {
		return CapabilityResult{}, err
	}
	eventResult := result
	if err != nil {
		eventResult = CapabilityResult{Content: err.Error(), OK: false}
	}
	if appendErr := i.agent.append(i.agent.runID, EvToolResult, ToolResultData{
		CallID: call.ID, Content: eventResult.Content, OK: eventResult.OK, Metadata: eventResult.Metadata,
	}); appendErr != nil {
		return CapabilityResult{}, appendErr
	}
	return result, err
}

func (g *guardedToolRuntime) Schemas() (schemas []ToolSchema) {
	defer func() {
		if recover() != nil {
			schemas = nil
		}
	}()
	return g.inner.Schemas()
}

func (g *guardedToolRuntime) Authorized(name string) (authorized bool) {
	defer func() {
		if recover() != nil {
			authorized = false
		}
	}()
	return g.inner.Authorized(name)
}

func (g *guardedToolRuntime) MaxCallBudget() (budget int) {
	defer func() {
		if recover() != nil {
			budget = 0
		}
	}()
	return g.inner.MaxCallBudget()
}

func (g *guardedToolRuntime) Execute(ctx context.Context, call ToolCall) (CapabilityResult, error) {
	agent := g.agent
	resuming := agent.resumeCallIDs[call.ID]
	if resuming {
		delete(agent.resumeCallIDs, call.ID)
	}
	info := RunInfo{
		RunID: agent.runID, SessionID: agent.opts.Session.ID(),
		ProfileID: agent.opts.Session.ProfileID(), Principal: agent.opts.Session.Principal(),
	}
	if !resuming && agent.toolCalls >= agent.opts.MaxToolCalls {
		result := deniedResult(CodeBudgetExceeded,
			fmt.Sprintf("run reached its maximum of %d tool calls", agent.opts.MaxToolCalls))
		if agent.opts.Hooks != nil {
			safeHookNotify(func() { agent.opts.Hooks.OnAfterTool(ctx, info, call, result) })
		}
		return result, nil
	}
	if !resuming {
		agent.toolCalls++
	}

	manifest, hasManifest := CapabilityManifest{}, false
	if source, ok := g.inner.(manifestSource); ok {
		manifest, hasManifest = safeManifestFor(source, call.Name)
	}
	if hasManifest {
		schema := manifest.InputSchema
		if manifest.Tool != nil && manifest.Tool.Parameters != nil {
			schema = manifest.Tool.Parameters
		}
		if err := ValidateArgs(schema, call.Args); err != nil {
			result := deniedResult(CodeInvalidArgs, err.Error())
			if agent.opts.Hooks != nil {
				safeHookNotify(func() { agent.opts.Hooks.OnAfterTool(ctx, info, call, result) })
			}
			return result, nil
		}
	}

	if agent.opts.Hooks != nil {
		if err := safeHookError("OnBeforeTool", func() error { return agent.opts.Hooks.OnBeforeTool(ctx, info, call) }); err != nil {
			result := deniedResult(CodeHookDenied, err.Error())
			if agent.opts.Hooks != nil {
				safeHookNotify(func() { agent.opts.Hooks.OnAfterTool(ctx, info, call, result) })
			}
			return result, nil
		}
	}

	if hasManifest {
		if !resuming {
			if budget, ok := g.inner.(budgetReporter); ok {
				if limit := safeBudgetFor(budget, call.Name); limit > 0 && agent.counts[call.Name] >= limit {
					result := deniedResult(CodeBudgetExceeded,
						fmt.Sprintf("tool %s reached its per-run budget of %d calls", call.Name, limit))
					if agent.opts.Hooks != nil {
						safeHookNotify(func() { agent.opts.Hooks.OnAfterTool(ctx, info, call, result) })
					}
					return result, nil
				}
			}
		}
		if !resuming && agent.opts.RateLimiter != nil && !safeAllowCall(agent.opts.RateLimiter, ctx, info.Principal.TenantID, call.Name) {
			result := deniedResult(CodeRateLimited,
				fmt.Sprintf("tool %s exceeded its cross-run rate limit for this tenant", call.Name))
			if agent.opts.Hooks != nil {
				safeHookNotify(func() { agent.opts.Hooks.OnAfterTool(ctx, info, call, result) })
			}
			return result, nil
		}
		if manifest.RequiresApproval {
			result, approved, approvalErr := g.requestApproval(ctx, info, call, manifest)
			if approvalErr != nil {
				return CapabilityResult{}, approvalErr
			}
			if !approved {
				if agent.opts.Hooks != nil {
					safeHookNotify(func() { agent.opts.Hooks.OnAfterTool(ctx, info, call, result) })
				}
				return result, nil
			}
		}
	}

	if !resuming {
		agent.counts[call.Name]++
	}
	var invocation ToolInvocation
	journaled := agent.opts.ToolJournal != nil
	if journaled {
		var invocationErr error
		invocation, invocationErr = NewToolInvocation(info, call, hasManifest && manifest.Idempotent)
		if invocationErr != nil {
			result := deniedResult(CodeInvalidArgs, invocationErr.Error())
			if agent.opts.Hooks != nil {
				safeHookNotify(func() { agent.opts.Hooks.OnAfterTool(ctx, info, call, result) })
			}
			return result, nil
		}
		record, decision, journalErr := safeBeginToolInvocation(agent.opts.ToolJournal, ctx, invocation)
		if journalErr != nil {
			recordToolJournalTelemetry(agent.opts.Telemetry, ctx, call.Name, "error")
			result := deniedResult(CodeToolJournalUnavailable, "tool invocation journal is unavailable")
			if agent.opts.Hooks != nil {
				safeHookNotify(func() { agent.opts.Hooks.OnAfterTool(ctx, info, call, result) })
			}
			return result, nil
		}
		recordToolJournalTelemetry(agent.opts.Telemetry, ctx, call.Name, string(decision))
		switch decision {
		case ToolInvocationReplay:
			result, resultErr := journalReplayResult(record, invocation, manifest, hasManifest)
			if resultErr != nil {
				result = deniedResult(CodeToolJournalUnavailable, resultErr.Error())
			}
			if agent.opts.Hooks != nil {
				safeHookNotify(func() { agent.opts.Hooks.OnAfterTool(ctx, info, call, result) })
			}
			return result, nil
		case ToolInvocationUnknown:
			result := deniedResult(CodeToolOutcomeUnknown,
				fmt.Sprintf("tool %s may already have executed; automatic replay is unsafe", call.Name))
			if agent.opts.Hooks != nil {
				safeHookNotify(func() { agent.opts.Hooks.OnAfterTool(ctx, info, call, result) })
			}
			return result, nil
		case ToolInvocationConflict:
			result := deniedResult(CodeToolIdempotencyConflict,
				fmt.Sprintf("tool call identity %s was reused with different capability or arguments", call.ID))
			if agent.opts.Hooks != nil {
				safeHookNotify(func() { agent.opts.Hooks.OnAfterTool(ctx, info, call, result) })
			}
			return result, nil
		case ToolInvocationExecuteNew:
		case ToolInvocationExecuteRetry:
			if !invocation.Idempotent {
				result := deniedResult(CodeToolJournalUnavailable, "journal requested an unsafe non-idempotent retry")
				if agent.opts.Hooks != nil {
					safeHookNotify(func() { agent.opts.Hooks.OnAfterTool(ctx, info, call, result) })
				}
				return result, nil
			}
		default:
			result := deniedResult(CodeToolJournalUnavailable, "journal returned an invalid tool invocation decision")
			if agent.opts.Hooks != nil {
				safeHookNotify(func() { agent.opts.Hooks.OnAfterTool(ctx, info, call, result) })
			}
			return result, nil
		}
	}
	var result CapabilityResult
	var err error
	toolCtx, toolSpan := StartTelemetry(agent.opts.Telemetry, ctx, SpanToolCall, TelemetryAttributes{
		"run.id": info.RunID, "session.id": info.SessionID, "call.id": call.ID,
		"capability.id": call.Name, "tool.idempotent": fmt.Sprintf("%t", hasManifest && manifest.Idempotent),
		"tool.approval_required": fmt.Sprintf("%t", hasManifest && manifest.RequiresApproval),
	})
	toolCtx = withCapabilityInvocation(toolCtx, Invocation{SessionID: info.SessionID, RunID: info.RunID, CallID: call.ID}, max(0, agent.opts.MaxToolCalls-agent.toolCalls), agent.compositionRevision)
	if journaled {
		toolCtx = withAcceptedInvocation(toolCtx, mintAcceptedInvocation(invocation, agent.opts.Session.Scope(), info.Principal))
	}
	toolStarted := time.Now()
	if runtime, ok := g.inner.(protectedToolRuntime); ok {
		result, err = safeProtectedExecute(runtime, toolCtx, call, nestedToolInvoker{agent: agent, runtime: g})
	} else {
		result, err = safeToolRuntimeExecute(g.inner, toolCtx, call)
	}
	toolOutcome := telemetryToolOutcome(result, err)
	toolSpan.End(err, TelemetryAttributes{"tool.outcome": toolOutcome})
	toolMetricAttrs := TelemetryAttributes{"capability.id": call.Name, "tool.outcome": toolOutcome}
	AddTelemetryCounter(agent.opts.Telemetry, toolCtx, MetricToolCalls, 1, toolMetricAttrs)
	RecordTelemetryHistogram(agent.opts.Telemetry, toolCtx, MetricToolDuration, time.Since(toolStarted).Seconds(), "s", toolMetricAttrs)
	if journaled {
		if err != nil {
			if _, pending := IsApprovalPending(err); pending {
				return CapabilityResult{}, err
			}
			_ = safeMarkToolInvocationUncertain(agent.opts.ToolJournal, ctx, invocation, "provider_error")
		} else {
			record, completeErr := safeCompleteToolInvocation(agent.opts.ToolJournal, ctx, invocation, result)
			if completeErr != nil {
				_ = safeMarkToolInvocationUncertain(agent.opts.ToolJournal, ctx, invocation, CodeToolJournalUnavailable)
				result = deniedResult(CodeToolOutcomeUnknown,
					fmt.Sprintf("tool %s completed but its durable outcome could not be recorded", call.Name))
				err = nil
			} else {
				canonical, resultErr := journalReplayResult(record, invocation, manifest, hasManifest)
				if resultErr != nil {
					result = deniedResult(CodeToolJournalUnavailable, resultErr.Error())
				} else {
					result = canonical
				}
			}
		}
	}
	if agent.opts.Hooks != nil {
		safeHookNotify(func() { agent.opts.Hooks.OnAfterTool(ctx, info, call, result) })
	}
	return result, err
}

func recordToolJournalTelemetry(telemetry Telemetry, ctx context.Context, capability, decision string) {
	AddTelemetryCounter(telemetry, ctx, MetricToolJournalDecisions, 1, TelemetryAttributes{
		"capability.id": capability, "journal.decision": decision,
	})
}

func telemetryToolOutcome(result CapabilityResult, err error) string {
	if _, pending := IsApprovalPending(err); pending {
		return "approval_pending"
	}
	if err != nil {
		return "error"
	}
	if result.OK {
		return "ok"
	}
	if code, _ := result.Metadata["code"].(string); code != "" {
		return code
	}
	return "not_ok"
}

func safeManifestFor(source manifestSource, name string) (manifest CapabilityManifest, ok bool) {
	defer func() {
		if recover() != nil {
			manifest, ok = CapabilityManifest{}, false
		}
	}()
	return source.ManifestFor(name)
}

func journalReplayResult(record ToolInvocationRecord, invocation ToolInvocation, manifest CapabilityManifest, hasManifest bool) (CapabilityResult, error) {
	if record.ToolInvocation != invocation {
		return CapabilityResult{}, fmt.Errorf("tool invocation journal returned a result for a different identity")
	}
	if record.State != ToolInvocationCompleted || record.Result == nil {
		return CapabilityResult{}, fmt.Errorf("tool invocation journal returned a replay without a completed result")
	}
	result := cloneCapabilityResult(*record.Result)
	if err := ValidateCapabilityResult(result); err != nil {
		return CapabilityResult{}, fmt.Errorf("journaled tool result is invalid: %w", err)
	}
	limit := DefaultMaxCapabilityOutputBytes
	if hasManifest && manifest.MaxOutputBytes > 0 {
		limit = manifest.MaxOutputBytes
	}
	if len(result.Content) > limit {
		return CapabilityResult{}, fmt.Errorf("journaled tool result exceeds %d bytes", limit)
	}
	if hasManifest && result.OK && len(manifest.OutputSchema) > 0 {
		var output any
		if err := json.Unmarshal([]byte(result.Content), &output); err != nil {
			return CapabilityResult{}, fmt.Errorf("journaled tool result is not valid JSON: %w", err)
		}
		if err := ValidateJSONValue(manifest.OutputSchema, output); err != nil {
			return CapabilityResult{}, fmt.Errorf("journaled tool result violates output schema: %w", err)
		}
	}
	return result, nil
}

func safeBeginToolInvocation(journal ToolInvocationJournal, ctx context.Context, invocation ToolInvocation) (record ToolInvocationRecord, decision ToolInvocationDecision, err error) {
	defer func() {
		if recover() != nil {
			record = ToolInvocationRecord{}
			decision = ""
			err = fmt.Errorf("tool invocation journal panicked")
		}
	}()
	return journal.BeginToolInvocation(ctx, invocation)
}

func safeCompleteToolInvocation(journal ToolInvocationJournal, ctx context.Context, invocation ToolInvocation, result CapabilityResult) (record ToolInvocationRecord, err error) {
	defer func() {
		if recover() != nil {
			record = ToolInvocationRecord{}
			err = fmt.Errorf("tool invocation journal panicked")
		}
	}()
	return journal.CompleteToolInvocation(ctx, invocation, result)
}

func safeMarkToolInvocationUncertain(journal ToolInvocationJournal, ctx context.Context, invocation ToolInvocation, errorCode string) (err error) {
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("tool invocation journal panicked")
		}
	}()
	return journal.MarkToolInvocationUncertain(ctx, invocation, errorCode)
}

func safeBudgetFor(reporter budgetReporter, name string) (budget int) {
	defer func() {
		if recover() != nil {
			budget = 0
		}
	}()
	return reporter.BudgetFor(name)
}

func safeProtectedExecute(runtime protectedToolRuntime, ctx context.Context, call ToolCall, invoker ProtectedToolInvoker) (result CapabilityResult, err error) {
	defer func() {
		if recover() != nil {
			result = deniedResult("tool_runtime_panic", "tool runtime panicked")
			err = nil
		}
	}()
	return runtime.executeProtected(ctx, call, invoker)
}

func safeToolRuntimeExecute(runtime ToolRuntime, ctx context.Context, call ToolCall) (result CapabilityResult, err error) {
	defer func() {
		if recover() != nil {
			result = deniedResult("tool_runtime_panic", "tool runtime panicked")
			err = nil
		}
	}()
	return runtime.Execute(ctx, call)
}

// requestApproval consults the configured approver. Missing approver, error,
// or an out-of-vocabulary decision all deny the call — approval is fail-closed.
