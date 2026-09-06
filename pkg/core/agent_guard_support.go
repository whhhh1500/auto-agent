package core

import (
	"context"
	"fmt"
)

func (g *guardedToolRuntime) requestApproval(ctx context.Context, info RunInfo, call ToolCall, manifest CapabilityManifest) (CapabilityResult, bool, error) {
	agent := g.agent
	if agent.opts.Approver == nil {
		recordApprovalTelemetry(agent.opts.Telemetry, ctx, call.Name, ApprovalDecision("unavailable"), ApprovalResolution{})
		return approvalDeniedFor(CodeApprovalUnavailable, call), false, nil
	}
	request := ApprovalRequest{
		RunID: info.RunID, SessionID: info.SessionID, Principal: info.Principal,
		ToolCall: call, Manifest: manifest,
	}
	if durable, ok := agent.opts.Approver.(DurableApprover); ok {
		resolution, err := safeRequestApproval(durable, ctx, request)
		if err != nil {
			recordApprovalTelemetry(agent.opts.Telemetry, ctx, call.Name, ApprovalDecision("error"), ApprovalResolution{})
			return approvalDeniedFor(CodeApprovalFailed, call), false, nil
		}
		if err := ValidateApprovalID(resolution.ApprovalID); err != nil {
			recordApprovalTelemetry(agent.opts.Telemetry, ctx, call.Name, ApprovalDecision("invalid"), resolution)
			return approvalDeniedFor(CodeApprovalFailed, call), false, nil
		}
		recordApprovalTelemetry(agent.opts.Telemetry, ctx, call.Name, resolution.Decision, resolution)
		switch resolution.Decision {
		case ApprovalPending:
			return CapabilityResult{}, false, &ApprovalPendingError{Request: request, Resolution: resolution}
		case ApprovalApproved:
			agent.approvalResolutions[resolution.ApprovalID] = resolution
			return CapabilityResult{}, true, nil
		case ApprovalDenied, ApprovalExpired:
			agent.approvalResolutions[resolution.ApprovalID] = resolution
			return approvalDeniedFor(CodeApprovalDenied, call), false, nil
		default:
			return approvalDeniedFor(CodeApprovalUnavailable, call), false, nil
		}
	}
	decision, err := safeApprove(agent.opts.Approver, ctx, request)
	if err != nil {
		recordApprovalTelemetry(agent.opts.Telemetry, ctx, call.Name, ApprovalDecision("error"), ApprovalResolution{})
		return approvalDeniedFor(CodeApprovalFailed, call), false, nil
	}
	recordApprovalTelemetry(agent.opts.Telemetry, ctx, call.Name, decision, ApprovalResolution{})
	switch decision {
	case ApprovalApproved:
		return CapabilityResult{}, true, nil
	case ApprovalDenied:
		return approvalDeniedFor(CodeApprovalDenied, call), false, nil
	default:
		return approvalDeniedFor(CodeApprovalUnavailable, call), false, nil
	}
}

func recordApprovalTelemetry(telemetry Telemetry, ctx context.Context, capability string, decision ApprovalDecision, resolution ApprovalResolution) {
	attrs := TelemetryAttributes{"capability.id": capability, "approval.decision": string(decision)}
	if decision == ApprovalPending {
		AddTelemetryCounter(telemetry, ctx, MetricApprovalRequests, 1, attrs)
	} else if resolution.RequestedAt.IsZero() {
		// Immediate/synchronous approvers have no durable infrastructure path.
		AddTelemetryCounter(telemetry, ctx, MetricApprovalDecisions, 1, attrs)
	}
	if !resolution.RequestedAt.IsZero() && !resolution.DecidedAt.IsZero() && !resolution.DecidedAt.Before(resolution.RequestedAt) {
		RecordTelemetryHistogram(telemetry, ctx, MetricApprovalWait,
			resolution.DecidedAt.Sub(resolution.RequestedAt).Seconds(), "s", attrs)
	}
}

func safeHookError(name string, call func() error) (err error) {
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("run hook %s panicked", name)
		}
	}()
	return call()
}

func safeHookNotify(call func()) {
	defer func() { _ = recover() }()
	call()
}

func safeFastDispatch(router *FastRouter, ctx context.Context, text string, tools ToolRuntime) (dispatch FastDispatch, err error) {
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("fast router panicked")
		}
	}()
	return router.Dispatch(ctx, text, tools)
}

func safeEnsureSummarized(summarizer RunSummarizer, ctx context.Context, session *Session, runID string, emit func(SessionEvent), messages []ChatMessage) (out []ChatMessage, err error) {
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("run summarizer panicked")
		}
	}()
	return summarizer.EnsureSummarized(ctx, session, runID, emit, messages)
}

func safeCompact(compactor ContextCompactor, messages []ChatMessage) (out []ChatMessage, err error) {
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("context compactor panicked")
		}
	}()
	return compactor.Compact(messages), nil
}

func safeAllowCall(limiter CallRateLimiter, ctx context.Context, tenantID, capability string) (allowed bool) {
	defer func() {
		if recover() != nil {
			allowed = false
		}
	}()
	return limiter.AllowCall(ctx, tenantID, capability)
}

func safeApprove(approver Approver, ctx context.Context, request ApprovalRequest) (decision ApprovalDecision, err error) {
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("approver panicked")
		}
	}()
	return approver.Approve(ctx, request)
}

func safeRequestApproval(approver DurableApprover, ctx context.Context, request ApprovalRequest) (resolution ApprovalResolution, err error) {
	defer func() {
		if recover() != nil {
			resolution = ApprovalResolution{}
			err = fmt.Errorf("durable approver panicked")
		}
	}()
	return approver.RequestApproval(ctx, request)
}
