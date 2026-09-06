package graph

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	contract "github.com/cc-auto-agent/harness-core/pkg/extensions/graph"
)

var (
	ErrLeaseUnavailable          = errors.New("graph segment lease is unavailable")
	ErrContextUnavailable        = errors.New("graph context planner is unavailable")
	ErrSandboxUnavailable        = errors.New("graph sandbox authorization is unavailable")
	ErrApprovalUnavailable       = errors.New("graph approval authorization is unavailable")
	ErrApprovalAuthorizationFail = errors.New("graph approval authorization failed")
	ErrInvalidApproval           = errors.New("invalid graph approval decision")
	ErrApprovalPanic             = errors.New("graph approval authorizer panicked")
)

// SegmentLease is deliberately narrower than a scheduler: it proves that one
// segment/generation may invoke side-effecting node code, and lets approval
// suspension release that authority. Checkpoint CAS never substitutes for it.
type SegmentLease interface {
	Verify(context.Context, LeaseRequest) error
	Release(context.Context, LeaseRequest) error
}

type LeaseRequest struct {
	Key            contract.CheckpointKey
	SegmentID      string
	HostGeneration uint64
}

// ContextPlanner returns references and token estimates, never prompt bodies.
// The executor validates that its result exactly matches the node declaration
// before handing the plan to node code.
type ContextPlanner interface {
	Plan(context.Context, ContextRequest) (contract.ContextPlan, error)
}

type ContextRequest struct {
	Key                contract.CheckpointKey
	DefinitionRevision string
	NodeID             string
	View               contract.ContextView
	Budget             contract.ContextBudget
	State              contract.State
	PendingApprovalID  string
	ResolvedApproval   *contract.ApprovalEvidence
}

// SandboxAuthorizer is an assurance seam only. G1 does not implement a local
// sandbox: a UsesSandbox node is rejected unless an injected authorizer proves
// the requested policy before node invocation.
type SandboxAuthorizer interface {
	Authorize(context.Context, SandboxRequest) error
}

type SandboxRequest struct {
	Lease   LeaseRequest
	NodeID  string
	Attempt string
}

// ApprovalDecision is bounded authorization evidence supplied by the upper
// layer. A bare Approved boolean is intentionally not sufficient to resume a
// suspended run. The decision is not persisted as a checkpoint field; the
// transition records the bounded approval evidence alongside the outcome.
type ApprovalDecision struct {
	TenantID   string                 `json:"tenant_id"`
	Key        contract.CheckpointKey `json:"key"`
	ApprovalID string                 `json:"approval_id"`
	Decision   ApprovalDecisionCode   `json:"decision"`
	Revision   uint64                 `json:"revision"`
	// SourceSegmentID is the segment that produced the pending approval; it is
	// distinct from the new resume segment in ApprovalAuthorizationRequest.
	SourceSegmentID    string `json:"source_segment_id"`
	ActorID            string `json:"actor_id"`
	AuthorizationBasis string `json:"authorization_basis"`
}

type ApprovalDecisionCode string

const (
	ApprovalDecisionApproved   ApprovalDecisionCode = "approved"
	ApprovalDecisionDenied     ApprovalDecisionCode = "denied"
	MaxApprovalActorBytes                           = 128
	MaxAuthorizationBasisBytes                      = 256
)

func (decision ApprovalDecision) Validate() error {
	if err := contract.ValidateCheckpointKey(decision.Key); err != nil {
		return fmt.Errorf("%w: key: %v", ErrInvalidApproval, err)
	}
	if decision.TenantID == "" || decision.TenantID != decision.Key.TenantID ||
		!validApprovalText(decision.TenantID, contract.MaxCheckpointIDBytes) {
		return fmt.Errorf("%w: tenant", ErrInvalidApproval)
	}
	if !validApprovalText(decision.ApprovalID, contract.MaxCheckpointIDBytes) || decision.Revision == 0 ||
		!validApprovalText(decision.SourceSegmentID, contract.MaxCheckpointIDBytes) ||
		!validApprovalText(decision.ActorID, MaxApprovalActorBytes) ||
		!validApprovalText(decision.AuthorizationBasis, MaxAuthorizationBasisBytes) {
		return fmt.Errorf("%w: evidence", ErrInvalidApproval)
	}
	if decision.Decision != ApprovalDecisionApproved && decision.Decision != ApprovalDecisionDenied {
		return fmt.Errorf("%w: decision", ErrInvalidApproval)
	}
	return nil
}

func validApprovalText(value string, max int) bool {
	if value == "" || len(value) > max || !utf8.ValidString(value) || strings.TrimSpace(value) != value {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) || unicode.In(r, unicode.Cf, unicode.Cs, unicode.Co) {
			return false
		}
	}
	return true
}

// ApprovalAuthorizationRequest is the exact current fact an authorizer must
// check before either an approval or denial CAS is attempted.
type ApprovalAuthorizationRequest struct {
	Key               contract.CheckpointKey
	PendingApprovalID string
	CurrentRevision   uint64
	SegmentID         string // newly acquired resume segment
	HostGeneration    uint64
	Decision          ApprovalDecision
}

// ApprovalAuthorizer verifies actor/policy evidence. Implementations must be
// idempotent: authorization may be retried when a later lease or CAS fails,
// and verification must not consume or mutate the decision. It is required
// for both approved and denied resumes; no default authorizer exists.
type ApprovalAuthorizer interface {
	Authorize(context.Context, ApprovalAuthorizationRequest) error
}

type Options struct {
	Observer Observer
	Lease    SegmentLease
	Planner  ContextPlanner
	Sandbox  SandboxAuthorizer
	Approval ApprovalAuthorizer
}
