// Package graphapproval adapts durable approval storage to the Graph seams.
package graphapproval

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	rungraph "github.com/cc-auto-agent/harness-core/pkg/adapter/runexecutor/graph"
	core "github.com/cc-auto-agent/harness-core/pkg/core"
	execgraph "github.com/cc-auto-agent/harness-core/pkg/execution/graph"
	contract "github.com/cc-auto-agent/harness-core/pkg/extensions/graph"
	storage "github.com/cc-auto-agent/harness-core/pkg/storage"
)

var (
	ErrInvalid     = errors.New("durable graph approval invalid")
	ErrUnavailable = errors.New("durable graph approval unavailable")
)

const AuthorizationBasis = "durable-approval/v1"

type Approval = storage.ApprovalRecord

type ApprovalReader interface {
	GetApproval(context.Context, string) (storage.ApprovalRecord, error)
}
type CheckpointReader interface {
	Load(context.Context, contract.CheckpointKey) (contract.Checkpoint, error)
}
type Options struct {
	Approvals   ApprovalReader
	Checkpoints CheckpointReader
}
type Adapter struct {
	approvals   ApprovalReader
	checkpoints CheckpointReader
}

var _ ApprovalReader = (*storage.SQLApprovalStore)(nil)

func New(o Options) (*Adapter, error) {
	if o.Approvals == nil || o.Checkpoints == nil {
		return nil, ErrInvalid
	}
	return &Adapter{approvals: o.Approvals, checkpoints: o.Checkpoints}, nil
}

var _ rungraph.ApprovalDecisionResolver = (*Adapter)(nil)
var _ execgraph.ApprovalAuthorizer = (*Adapter)(nil)

func (a *Adapter) Resolve(ctx context.Context, principal core.Principal, key contract.CheckpointKey, id string) (decision execgraph.ApprovalDecision, err error) {
	if err := validContext(ctx); err != nil {
		return decision, err
	}
	if err := validateResolveInput(principal, key, id); err != nil {
		return decision, err
	}
	if a == nil || a.approvals == nil || a.checkpoints == nil {
		return decision, ErrInvalid
	}
	defer func() {
		if recover() != nil {
			decision = execgraph.ApprovalDecision{}
			err = ErrUnavailable
		}
	}()
	record, err := a.approvals.GetApproval(ctx, id)
	if err != nil {
		return decision, safeDependencyError(err)
	}
	cp, err := a.checkpoints.Load(ctx, key)
	if err != nil {
		return decision, safeDependencyError(err)
	}
	if err = validateFacts(principal, key, id, record, cp, true); err != nil {
		return decision, err
	}
	return execgraph.ApprovalDecision{TenantID: key.TenantID, Key: key, ApprovalID: id, Decision: code(record.Status), Revision: cp.Revision, SourceSegmentID: cp.SegmentID, ActorID: record.DecidedBy, AuthorizationBasis: AuthorizationBasis}, nil
}

func (a *Adapter) Authorize(ctx context.Context, req execgraph.ApprovalAuthorizationRequest) (err error) {
	if err := validContext(ctx); err != nil {
		return err
	}
	if a == nil || a.approvals == nil || a.checkpoints == nil {
		return ErrInvalid
	}
	if err := contract.ValidateCheckpointKey(req.Key); err != nil || core.ValidateApprovalID(req.PendingApprovalID) != nil || req.CurrentRevision == 0 || !validText(req.SegmentID, contract.MaxCheckpointIDBytes) || req.HostGeneration == 0 || req.Decision.Validate() != nil {
		return ErrInvalid
	}
	defer func() {
		if recover() != nil {
			err = ErrUnavailable
		}
	}()
	if req.SegmentID == "" || req.HostGeneration == 0 {
		return ErrInvalid
	}
	record, err := a.approvals.GetApproval(ctx, req.PendingApprovalID)
	if err != nil {
		return safeDependencyError(err)
	}
	cp, err := a.checkpoints.Load(ctx, req.Key)
	if err != nil {
		return safeDependencyError(err)
	}
	if err = validateFacts(core.Principal{TenantID: req.Key.TenantID}, req.Key, req.PendingApprovalID, record, cp, false); err != nil {
		return err
	}
	if cp.HostGeneration >= req.HostGeneration || req.CurrentRevision != cp.Revision {
		return ErrInvalid
	}
	if req.SegmentID == cp.SegmentID {
		return ErrInvalid
	}
	want := execgraph.ApprovalDecision{TenantID: req.Key.TenantID, Key: req.Key, ApprovalID: record.ID, Decision: code(record.Status), Revision: cp.Revision, SourceSegmentID: cp.SegmentID, ActorID: record.DecidedBy, AuthorizationBasis: AuthorizationBasis}
	if req.Decision != want {
		return ErrInvalid
	}
	return nil
}

func validateResolveInput(principal core.Principal, key contract.CheckpointKey, id string) error {
	if err := contract.ValidateCheckpointKey(key); err != nil || core.ValidateApprovalID(id) != nil || !validText(principal.TenantID, contract.MaxCheckpointIDBytes) || !validText(principal.SubjectID, contract.MaxCheckpointIDBytes) {
		return ErrInvalid
	}
	return nil
}

func validText(value string, max int) bool {
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

func validateFacts(p core.Principal, key contract.CheckpointKey, id string, r Approval, cp contract.Checkpoint, checkSubject bool) error {
	if cp.Key != key || cp.Revision == 0 || !validText(cp.SegmentID, contract.MaxCheckpointIDBytes) || cp.HostGeneration == 0 || r.ID != id || r.TenantID != key.TenantID || r.SessionID != key.SessionID || r.RunID != key.RunID || !validText(r.TenantID, contract.MaxCheckpointIDBytes) || !validText(r.SessionID, contract.MaxCheckpointIDBytes) || !validText(r.RunID, contract.MaxCheckpointIDBytes) || !validText(r.SubjectID, contract.MaxCheckpointIDBytes) || checkSubject && r.SubjectID != p.SubjectID || p.TenantID != key.TenantID || cp.Status != contract.CheckpointWaitingApproval || cp.PendingApprovalID != id || !validText(r.DecidedBy, execgraph.MaxApprovalActorBytes) || r.DecidedAt.IsZero() || r.Status != core.ApprovalApproved && r.Status != core.ApprovalDenied {
		return ErrInvalid
	}
	return nil
}
func code(d core.ApprovalDecision) execgraph.ApprovalDecisionCode {
	if d == core.ApprovalApproved {
		return execgraph.ApprovalDecisionApproved
	}
	return execgraph.ApprovalDecisionDenied
}
func validContext(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalid
	}
	return ctx.Err()
}
func safeDependencyError(err error) error {
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return fmt.Errorf("%w: dependency", ErrUnavailable)
}
