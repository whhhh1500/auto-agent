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
		resolution, err := safeCallValueError("durable approver panicked", func() (ApprovalResolution, error) {
			return durable.RequestApproval(ctx, request)
		})
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
	decision, err := safeCallValueError("approver panicked", func() (ApprovalDecision, error) {
		return agent.opts.Approver.Approve(ctx, request)
	})
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

func safeCallError(message string, call func() error) (err error) {
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("%s", message)
		}
	}()
	return call()
}

func safeCallValue[T any](fallback T, call func() T) (value T) {
	defer func() {
		if recover() != nil {
			value = fallback
		}
	}()
	return call()
}

func safeCallValueError[T any](message string, call func() (T, error)) (value T, err error) {
	defer func() {
		if recover() != nil {
			value = *new(T)
			err = fmt.Errorf("%s", message)
		}
	}()
	return call()
}

func safeCallNotify(call func()) {
	defer func() { _ = recover() }()
	call()
}
