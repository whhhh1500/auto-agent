package graphapproval

import (
	"context"
	"errors"
	core "github.com/whhhh1500/auto-agent/pkg/core"
	execgraph "github.com/whhhh1500/auto-agent/pkg/execution/graph"
	contract "github.com/whhhh1500/auto-agent/pkg/extensions/graph"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type fakeReader struct {
	approval Approval
	err      error
	panic    bool
	calls    *atomic.Int32
}

func (f fakeReader) GetApproval(context.Context, string) (Approval, error) {
	if f.calls != nil {
		f.calls.Add(1)
	}
	if f.panic {
		panic("secret reader")
	}
	return f.approval, f.err
}

type fakeCheckpoints struct {
	checkpoint contract.Checkpoint
	err        error
	panic      bool
	calls      *atomic.Int32
}

func (f fakeCheckpoints) Load(context.Context, contract.CheckpointKey) (contract.Checkpoint, error) {
	if f.calls != nil {
		f.calls.Add(1)
	}
	if f.panic {
		panic("secret checkpoint")
	}
	return f.checkpoint, f.err
}
func fixture() (*Adapter, core.Principal, contract.Checkpoint, Approval) {
	key := contract.CheckpointKey{TenantID: "tenant", SessionID: "session", RunID: "run"}
	r := Approval{ID: "apr_0123456789abcdef0123456789abcdef", TenantID: key.TenantID, SessionID: key.SessionID, RunID: key.RunID, SubjectID: "subject", Status: core.ApprovalApproved, DecidedAt: time.Now(), DecidedBy: "operator"}
	cp := contract.Checkpoint{Key: key, SegmentID: "segment-old", HostGeneration: 1, Revision: 7, Status: contract.CheckpointWaitingApproval, PendingApprovalID: r.ID}
	a, _ := New(Options{Approvals: fakeReader{approval: r}, Checkpoints: fakeCheckpoints{checkpoint: cp}})
	return a, core.Principal{TenantID: key.TenantID, SubjectID: r.SubjectID}, cp, r
}
func TestResolveAndAuthorizeDeniedAndMismatch(t *testing.T) {
	a, p, cp, r := fixture()
	d, err := a.Resolve(context.Background(), p, cp.Key, r.ID)
	if err != nil || d.Decision != execgraph.ApprovalDecisionApproved || d.Revision != cp.Revision || d.SourceSegmentID != cp.SegmentID {
		t.Fatalf("d=%#v err=%v", d, err)
	}
	if err = a.Authorize(context.Background(), execgraph.ApprovalAuthorizationRequest{Key: cp.Key, PendingApprovalID: r.ID, CurrentRevision: cp.Revision, SegmentID: "segment-new", HostGeneration: 2, Decision: d}); err != nil {
		t.Fatal(err)
	}
	d.Decision = execgraph.ApprovalDecisionDenied
	if err = a.Authorize(context.Background(), execgraph.ApprovalAuthorizationRequest{Key: cp.Key, PendingApprovalID: r.ID, CurrentRevision: cp.Revision, SegmentID: "segment-new", HostGeneration: 2, Decision: d}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("mismatch=%v", err)
	}
}
func TestDependencyPanicAndContextAreStable(t *testing.T) {
	_, p, cp, r := fixture()
	a, _ := New(Options{Approvals: fakeReader{approval: r, panic: true}, Checkpoints: fakeCheckpoints{checkpoint: cp}})
	if _, err := a.Resolve(context.Background(), p, cp.Key, r.ID); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("panic=%v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := a.Resolve(ctx, p, cp.Key, r.ID); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel=%v", err)
	}
	if _, err := New(Options{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil=%v", err)
	}
}

func TestResolveRejectsOwnerAndCheckpointMutations(t *testing.T) {
	base, principal, cp, record := fixture()
	cases := []struct {
		name   string
		mutate func(*Approval, *contract.Checkpoint, *core.Principal)
	}{
		{"tenant", func(r *Approval, _ *contract.Checkpoint, _ *core.Principal) { r.TenantID = "other" }},
		{"session", func(r *Approval, _ *contract.Checkpoint, _ *core.Principal) { r.SessionID = "other" }},
		{"run", func(r *Approval, _ *contract.Checkpoint, _ *core.Principal) { r.RunID = "other" }},
		{"subject", func(r *Approval, _ *contract.Checkpoint, _ *core.Principal) { r.SubjectID = "other" }},
		{"checkpoint-key", func(_ *Approval, c *contract.Checkpoint, _ *core.Principal) { c.Key.RunID = "other" }},
		{"checkpoint-status", func(_ *Approval, c *contract.Checkpoint, _ *core.Principal) { c.Status = contract.CheckpointReady }},
		{"checkpoint-pending", func(_ *Approval, c *contract.Checkpoint, _ *core.Principal) {
			c.PendingApprovalID = "apr_abcdefabcdefabcdefabcdefabcdefab"
		}},
		{"actor", func(r *Approval, _ *contract.Checkpoint, _ *core.Principal) { r.DecidedBy = " " }},
	}
	_ = base
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, p, c := record, principal, cp
			tc.mutate(&r, &c, &p)
			a, _ := New(Options{Approvals: fakeReader{approval: r}, Checkpoints: fakeCheckpoints{checkpoint: c}})
			if _, err := a.Resolve(context.Background(), p, cp.Key, r.ID); !errors.Is(err, ErrInvalid) {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestInvalidInputsDoNotReachReaders(t *testing.T) {
	_, principal, cp, record := fixture()
	cases := []struct {
		name string
		key  contract.CheckpointKey
		id   string
		p    core.Principal
	}{
		{"bad-key", contract.CheckpointKey{}, record.ID, principal},
		{"bad-id", cp.Key, "bad", principal},
		{"bad-tenant", cp.Key, record.ID, core.Principal{TenantID: "", SubjectID: principal.SubjectID}},
		{"bad-subject", cp.Key, record.ID, core.Principal{TenantID: principal.TenantID, SubjectID: ""}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ac, cc := &atomic.Int32{}, &atomic.Int32{}
			a, _ := New(Options{Approvals: fakeReader{approval: record, calls: ac}, Checkpoints: fakeCheckpoints{checkpoint: cp, calls: cc}})
			if _, err := a.Resolve(context.Background(), tc.p, tc.key, tc.id); !errors.Is(err, ErrInvalid) {
				t.Fatalf("err=%v", err)
			}
			if ac.Load() != 0 || cc.Load() != 0 {
				t.Fatalf("reader calls=%d/%d", ac.Load(), cc.Load())
			}
		})
	}
}

func TestApprovalAndCheckpointFailuresAreSeparateAndRedacted(t *testing.T) {
	_, p, cp, r := fixture()
	for _, tc := range []struct {
		name   string
		ar, cr error
		ap, cp bool
	}{
		{"approval-error", errors.New("secret approval"), nil, false, false}, {"checkpoint-error", nil, errors.New("secret checkpoint"), false, false},
		{"approval-panic", nil, nil, true, false}, {"checkpoint-panic", nil, nil, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, _ := New(Options{Approvals: fakeReader{approval: r, err: tc.ar, panic: tc.ap}, Checkpoints: fakeCheckpoints{checkpoint: cp, err: tc.cr, panic: tc.cp}})
			_, err := a.Resolve(context.Background(), p, cp.Key, r.ID)
			if err == nil || errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "secret") {
				t.Fatalf("err=%v", err)
			}
			if !errors.Is(err, ErrUnavailable) {
				t.Fatalf("class=%v", err)
			}
		})
	}
}

func TestDeniedAndRepeatedAuthorizeAreIdempotent(t *testing.T) {
	_, p, cp, r := fixture()
	r.Status = core.ApprovalDenied
	ac, cc := &atomic.Int32{}, &atomic.Int32{}
	a, _ := New(Options{Approvals: fakeReader{approval: r, calls: ac}, Checkpoints: fakeCheckpoints{checkpoint: cp, calls: cc}})
	d, err := a.Resolve(context.Background(), p, cp.Key, r.ID)
	if err != nil || d.Decision != execgraph.ApprovalDecisionDenied {
		t.Fatalf("d=%#v err=%v", d, err)
	}
	req := execgraph.ApprovalAuthorizationRequest{Key: cp.Key, PendingApprovalID: r.ID, CurrentRevision: cp.Revision, SegmentID: "segment-new", HostGeneration: 2, Decision: d}
	if err = a.Authorize(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if err = a.Authorize(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if ac.Load() != 3 || cc.Load() != 3 {
		t.Fatalf("reads=%d/%d", ac.Load(), cc.Load())
	}
}

func TestDeadlineIsPreservedBeforeReader(t *testing.T) {
	_, p, cp, r := fixture()
	ac := &atomic.Int32{}
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	a, _ := New(Options{Approvals: fakeReader{approval: r, calls: ac}, Checkpoints: fakeCheckpoints{checkpoint: cp}})
	_, err := a.Resolve(ctx, p, cp.Key, r.ID)
	if !errors.Is(err, context.DeadlineExceeded) || ac.Load() != 0 {
		t.Fatalf("err=%v calls=%d", err, ac.Load())
	}
}

func TestAuthorizeSingleFieldMutationsFailClosed(t *testing.T) {
	_, principal, cp, record := fixture()
	base, err := (&Adapter{approvals: fakeReader{approval: record}, checkpoints: fakeCheckpoints{checkpoint: cp}}).Resolve(context.Background(), principal, cp.Key, record.ID)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		mutate func(*execgraph.ApprovalAuthorizationRequest, *Approval, *contract.Checkpoint)
	}{
		{"tenant", func(r *execgraph.ApprovalAuthorizationRequest, _ *Approval, _ *contract.Checkpoint) {
			r.Decision.TenantID = "other"
		}},
		{"key", func(r *execgraph.ApprovalAuthorizationRequest, _ *Approval, _ *contract.Checkpoint) {
			r.Decision.Key.RunID = "other"
		}},
		{"approval-id", func(r *execgraph.ApprovalAuthorizationRequest, _ *Approval, _ *contract.Checkpoint) {
			r.Decision.ApprovalID = "apr_abcdefabcdefabcdefabcdefabcdefab"
		}},
		{"decision", func(r *execgraph.ApprovalAuthorizationRequest, _ *Approval, _ *contract.Checkpoint) {
			r.Decision.Decision = execgraph.ApprovalDecisionDenied
		}},
		{"revision", func(r *execgraph.ApprovalAuthorizationRequest, _ *Approval, _ *contract.Checkpoint) {
			r.Decision.Revision++
		}},
		{"source", func(r *execgraph.ApprovalAuthorizationRequest, _ *Approval, _ *contract.Checkpoint) {
			r.Decision.SourceSegmentID = "segment-other"
		}},
		{"actor", func(r *execgraph.ApprovalAuthorizationRequest, _ *Approval, _ *contract.Checkpoint) {
			r.Decision.ActorID = "other"
		}},
		{"basis", func(r *execgraph.ApprovalAuthorizationRequest, _ *Approval, _ *contract.Checkpoint) {
			r.Decision.AuthorizationBasis = "other"
		}},
		{"current-revision", func(r *execgraph.ApprovalAuthorizationRequest, _ *Approval, _ *contract.Checkpoint) {
			r.CurrentRevision++
		}},
		{"segment", func(r *execgraph.ApprovalAuthorizationRequest, _ *Approval, _ *contract.Checkpoint) {
			r.SegmentID = "segment-old"
		}},
		{"generation", func(r *execgraph.ApprovalAuthorizationRequest, _ *Approval, _ *contract.Checkpoint) {
			r.HostGeneration = 1
		}},
		{"record-status", func(_ *execgraph.ApprovalAuthorizationRequest, a *Approval, _ *contract.Checkpoint) {
			a.Status = core.ApprovalPending
		}},
		{"record-owner", func(_ *execgraph.ApprovalAuthorizationRequest, a *Approval, _ *contract.Checkpoint) {
			a.TenantID = "other"
		}},
		{"cp-revision", func(_ *execgraph.ApprovalAuthorizationRequest, _ *Approval, c *contract.Checkpoint) { c.Revision++ }},
		{"cp-source", func(_ *execgraph.ApprovalAuthorizationRequest, _ *Approval, c *contract.Checkpoint) {
			c.SegmentID = "segment-other"
		}},
		{"cp-generation", func(_ *execgraph.ApprovalAuthorizationRequest, _ *Approval, c *contract.Checkpoint) {
			c.HostGeneration++
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := execgraph.ApprovalAuthorizationRequest{Key: cp.Key, PendingApprovalID: record.ID, CurrentRevision: cp.Revision, SegmentID: "segment-new", HostGeneration: 2, Decision: base}
			a := record
			c := cp
			tc.mutate(&req, &a, &c)
			adapter, _ := New(Options{Approvals: fakeReader{approval: a}, Checkpoints: fakeCheckpoints{checkpoint: c}})
			if err := adapter.Authorize(context.Background(), req); !errors.Is(err, ErrInvalid) {
				t.Fatalf("err=%v", err)
			}
		})
	}
}
