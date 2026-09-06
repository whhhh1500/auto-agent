package core

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// RunHooks is the guardrail seam around a run. Implementations are automatic
// checks: they may reject input or deny one tool call, observe results, and
// meter steps. Hooks never mutate events and never talk to the model.
//
// Error semantics:
//   - OnRunStart rejects the whole run before any work (stable code input_rejected).
//   - OnBeforeTool denies exactly one tool call; the denial becomes the tool
//     result the model sees, so the loop continues instead of failing.
//   - OnBeforeStep rejects the run at a step boundary (stable code step_rejected).
//   - OnAfterTool, OnAfterStep, and OnRunEnd are observational; their errors
//     never change the run outcome.
type RunHooks interface {
	OnRunStart(ctx context.Context, info RunInfo) error
	OnBeforeStep(ctx context.Context, info RunInfo) error
	OnBeforeTool(ctx context.Context, info RunInfo, call ToolCall) error
	OnAfterTool(ctx context.Context, info RunInfo, call ToolCall, result CapabilityResult)
	OnAfterStep(ctx context.Context, info RunInfo)
	OnRunEnd(ctx context.Context, info RunInfo, status RunStatus)
}

// RunInfo identifies the run a hook observes.
type RunInfo struct {
	RunID     string    `json:"run_id"`
	SessionID string    `json:"session_id"`
	ProfileID string    `json:"profile_id"`
	Step      int       `json:"step"`
	Principal Principal `json:"principal,omitempty"`
}

// RunHooksFuncs adapts individual hook functions; nil members are skipped.
type RunHooksFuncs struct {
	OnRunStartFn   func(context.Context, RunInfo) error
	OnBeforeStepFn func(context.Context, RunInfo) error
	OnBeforeToolFn func(context.Context, RunInfo, ToolCall) error
	OnAfterToolFn  func(context.Context, RunInfo, ToolCall, CapabilityResult)
	OnAfterStepFn  func(context.Context, RunInfo)
	OnRunEndFn     func(context.Context, RunInfo, RunStatus)
}

func (h *RunHooksFuncs) OnRunStart(ctx context.Context, info RunInfo) error {
	if h.OnRunStartFn == nil {
		return nil
	}
	return h.OnRunStartFn(ctx, info)
}

func (h *RunHooksFuncs) OnBeforeStep(ctx context.Context, info RunInfo) error {
	if h.OnBeforeStepFn == nil {
		return nil
	}
	return h.OnBeforeStepFn(ctx, info)
}

func (h *RunHooksFuncs) OnBeforeTool(ctx context.Context, info RunInfo, call ToolCall) error {
	if h.OnBeforeToolFn == nil {
		return nil
	}
	return h.OnBeforeToolFn(ctx, info, call)
}

func (h *RunHooksFuncs) OnAfterTool(ctx context.Context, info RunInfo, call ToolCall, result CapabilityResult) {
	if h.OnAfterToolFn != nil {
		h.OnAfterToolFn(ctx, info, call, result)
	}
}

func (h *RunHooksFuncs) OnAfterStep(ctx context.Context, info RunInfo) {
	if h.OnAfterStepFn != nil {
		h.OnAfterStepFn(ctx, info)
	}
}

func (h *RunHooksFuncs) OnRunEnd(ctx context.Context, info RunInfo, status RunStatus) {
	if h.OnRunEndFn != nil {
		h.OnRunEndFn(ctx, info, status)
	}
}

// ApprovalDecision is the outcome of one human-in-the-loop approval request.
type ApprovalDecision string

const (
	ApprovalApproved ApprovalDecision = "approved"
	ApprovalDenied   ApprovalDecision = "denied"
	ApprovalPending  ApprovalDecision = "pending"
	ApprovalExpired  ApprovalDecision = "expired"
)

// ApprovalResolution is returned by a DurableApprover. Pending resolutions
// suspend the run without occupying its worker; terminal decisions are
// consumed when the same run resumes.
type ApprovalResolution struct {
	ApprovalID  string           `json:"approval_id"`
	Decision    ApprovalDecision `json:"decision"`
	RequestedAt time.Time        `json:"requested_at,omitempty"`
	ExpiresAt   time.Time        `json:"expires_at,omitempty"`
	DecidedAt   time.Time        `json:"decided_at,omitempty"`
	DecidedBy   string           `json:"decided_by,omitempty"`
}

// ApprovalRequest describes one capability call awaiting approval.
type ApprovalRequest struct {
	RunID     string             `json:"run_id"`
	SessionID string             `json:"session_id"`
	Principal Principal          `json:"principal"`
	ToolCall  ToolCall           `json:"tool_call"`
	Manifest  CapabilityManifest `json:"manifest"`
}

// Approver gates capability calls whose manifest declares RequiresApproval.
// It is the immediate/synchronous seam. Durable deployments additionally
// implement DurableApprover so a pending decision suspends instead of blocking
// a worker. Missing, failed, or invalid decisions remain fail-closed.
type Approver interface {
	Approve(ctx context.Context, request ApprovalRequest) (ApprovalDecision, error)
}

// DurableApprover persists pending requests and returns stable approval IDs.
// Implementations must bind decisions to the full request identity and args.
type DurableApprover interface {
	RequestApproval(ctx context.Context, request ApprovalRequest) (ApprovalResolution, error)
}

// ApproverFunc adapts a function to Approver.
type ApproverFunc func(context.Context, ApprovalRequest) (ApprovalDecision, error)

func (f ApproverFunc) Approve(ctx context.Context, request ApprovalRequest) (ApprovalDecision, error) {
	return f(ctx, request)
}

// ApprovalPendingError crosses composite capability boundaries without being
// converted to a normal tool failure. The Agent catches it at the outer run
// boundary and writes the durable approval/requested event.
type ApprovalPendingError struct {
	Request    ApprovalRequest
	Resolution ApprovalResolution
}

func (e *ApprovalPendingError) Error() string {
	if e == nil {
		return "approval is pending"
	}
	return fmt.Sprintf("approval %s is pending for tool %s", e.Resolution.ApprovalID, e.Request.ToolCall.Name)
}

// IsApprovalPending extracts a durable approval suspension.
func IsApprovalPending(err error) (*ApprovalPendingError, bool) {
	var pending *ApprovalPendingError
	if errors.As(err, &pending) {
		return pending, true
	}
	return nil, false
}

// Approval codes recorded in tool result metadata.
const (
	CodeApprovalDenied      = "approval_denied"
	CodeApprovalUnavailable = "approval_unavailable"
	CodeApprovalFailed      = "approval_failed"
	CodeInvalidArgs         = "invalid_tool_args"
	CodeBudgetExceeded      = "capability_budget_exceeded"
	CodeHookDenied          = "hook_denied"
	CodeRateLimited         = "capability_rate_limited"
)

// deniedResult builds the tool result returned to the model for a refused call.
// Refusals are results, not run failures, so the loop can recover.
func deniedResult(code, message string) CapabilityResult {
	return CapabilityResult{
		Content:  message,
		OK:       false,
		Metadata: map[string]any{"code": code},
	}
}

// approvalDeniedFor renders the fail-closed denial message for one call.
func approvalDeniedFor(code string, call ToolCall) CapabilityResult {
	switch code {
	case CodeApprovalUnavailable:
		return deniedResult(code, fmt.Sprintf("tool %s requires approval but no approver is configured", call.Name))
	case CodeApprovalFailed:
		return deniedResult(code, fmt.Sprintf("tool %s approval could not be completed", call.Name))
	default:
		return deniedResult(code, fmt.Sprintf("tool %s was not approved", call.Name))
	}
}
