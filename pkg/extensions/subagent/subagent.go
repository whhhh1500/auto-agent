// Package subagent provides an optional agent-as-capability implementation.
package subagent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/extensions/internal/support"
)

const (
	ContractV1         = "harness.agent/v1"
	AttributeDepth     = "agent.depth"
	DefaultMaxDepth    = 3
	DefaultMaxChildren = 32
)

type durableLockEntry struct {
	mu   sync.Mutex
	refs int
}

var durableLocks = struct {
	sync.Mutex
	entries map[string]*durableLockEntry
}{entries: map[string]*durableLockEntry{}}

// childProgressPersister makes the runtime's per-event callback compatible
// with Session batches. A callback is a notification that the child may have
// advanced; it is not evidence that exactly one event was appended. The
// persister therefore saves the complete suffix from its last durable version
// through the child's current version, then treats later notifications for the
// same suffix as no-ops.
//
// One durable delegation execution owns one instance. Its mutex serializes
// callbacks without holding any Session lock while the store is called: Clone
// and EventsFrom take their own short Session locks before persistence begins.
type childProgressPersister struct {
	store core.SessionStore
	child *core.Session

	mu           sync.Mutex
	savedVersion int64
	err          error
}

func newChildProgressPersister(store core.SessionStore, child *core.Session) *childProgressPersister {
	return &childProgressPersister{store: store, child: child, savedVersion: child.Version()}
}

func (p *childProgressPersister) emit(ctx context.Context, cancel context.CancelFunc) {
	if err := p.persist(ctx); err != nil {
		cancel()
	}
}

func (p *childProgressPersister) Err() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

func (p *childProgressPersister) persist(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return p.err
	}

	// Take an immutable snapshot before entering an adapter. This lets the
	// Save fallback persist a coherent full snapshot even if a later callback
	// appends while the adapter is in flight.
	snapshot, err := p.child.Clone()
	if err != nil {
		p.err = fmt.Errorf("clone child session for persistence: %w", err)
		return p.err
	}
	targetVersion := snapshot.Version()
	if targetVersion == p.savedVersion {
		return nil
	}
	if targetVersion < p.savedVersion {
		p.err = fmt.Errorf("child session version regressed from %d to %d", p.savedVersion, targetVersion)
		return p.err
	}
	pending := snapshot.EventsFrom(p.savedVersion)
	if int64(len(pending)) != targetVersion-p.savedVersion {
		p.err = fmt.Errorf("child session pending suffix is incomplete")
		return p.err
	}

	if appender, ok := p.store.(core.SessionAppender); ok {
		err = appender.AppendEvents(ctx, snapshot.ID(), p.savedVersion, pending)
	} else {
		err = p.store.Save(ctx, snapshot, p.savedVersion)
	}
	if err != nil {
		if convergeErr := p.confirmPersisted(ctx, snapshot); convergeErr == nil {
			p.savedVersion = targetVersion
			return nil
		} else {
			p.err = errors.Join(fmt.Errorf("persist child session: %w", err), convergeErr)
			return p.err
		}
	}
	p.savedVersion = targetVersion
	return nil
}

// confirmPersisted handles the only safe retry convergence case: the adapter
// reported an error, but a reload proves it already committed this exact child
// snapshot. A conflicting or merely longer history is never accepted because
// it could belong to another execution owner.
func (p *childProgressPersister) confirmPersisted(ctx context.Context, expected *core.Session) error {
	durable, err := p.store.Load(ctx, expected.ID())
	if err != nil {
		return fmt.Errorf("reload child session after persistence error: %w", err)
	}
	if durable.Version() != expected.Version() || !sameSessionEvents(durable.Events(), expected.Events()) {
		return fmt.Errorf("child session persistence outcome is not an exact durable match")
	}
	return nil
}

func sameSessionEvents(left, right []core.SessionEvent) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].Seq != right[index].Seq || !left[index].Time.Equal(right[index].Time) || left[index].RunID != right[index].RunID || left[index].Type != right[index].Type || !bytes.Equal(left[index].Data, right[index].Data) {
			return false
		}
	}
	return true
}

// lockDurable serializes one parent replay (or one child continuation) across
// Capability instances while allowing nested delegation with a different key.
// Reference counting removes entries after use, keeping untrusted IDs bounded.
func lockDurable(key string) func() {
	durableLocks.Lock()
	entry := durableLocks.entries[key]
	if entry == nil {
		entry = &durableLockEntry{}
		durableLocks.entries[key] = entry
	}
	entry.refs++
	durableLocks.Unlock()
	entry.mu.Lock()
	return func() {
		entry.mu.Unlock()
		durableLocks.Lock()
		entry.refs--
		if entry.refs == 0 {
			delete(durableLocks.entries, key)
		}
		durableLocks.Unlock()
	}
}

type Options struct {
	ProfileID   string
	Description string
	MaxDepth    int
	MaxChildren int
	// Sessions and Links opt into restart-safe delegation. Without both, the
	// legacy in-process continuation behavior remains available.
	Sessions               core.SessionStore
	Links                  DelegationLinkStore
	DelegationMaxToolCalls int
}

func delegationID(prefix, key string) string {
	sum := sha256.Sum256([]byte(key))
	return prefix + hex.EncodeToString(sum[:16])
}

// Capability delegates a prompt to a child core runtime session. Child
// sessions are continuable only within this process and capability instance.
type Capability struct {
	runtime *core.Runtime
	id      string
	opts    Options

	mu       sync.Mutex
	children map[string]*core.Session
	order    []string
}

func (*Capability) ArtifactRevision() string { return "subagent/v3-durable-delegation" }

func NewCapability(runtime *core.Runtime, id string, opts Options) (*Capability, error) {
	if runtime == nil {
		return nil, fmt.Errorf("subagent %q requires a runtime", id)
	}
	if opts.ProfileID == "" {
		return nil, fmt.Errorf("subagent %q requires a child profile id", id)
	}
	if err := support.ValidateCapabilityID(id); err != nil {
		return nil, err
	}
	if err := core.ValidateNamespacedID(opts.ProfileID); err != nil {
		return nil, fmt.Errorf("subagent %q child profile id: %w", id, err)
	}
	if opts.MaxDepth <= 0 {
		opts.MaxDepth = DefaultMaxDepth
	}
	if opts.MaxChildren <= 0 {
		opts.MaxChildren = DefaultMaxChildren
	}
	if opts.DelegationMaxToolCalls < 0 || opts.DelegationMaxToolCalls > core.HardMaxToolCalls {
		return nil, fmt.Errorf("subagent %q delegation max tool calls must be between 0 and %d", id, core.HardMaxToolCalls)
	}
	if opts.Description == "" {
		opts.Description = "Delegate this task to a specialist agent and return its final answer."
	}
	return &Capability{
		runtime: runtime, id: id, opts: opts,
		children: map[string]*core.Session{},
	}, nil
}

func (s *Capability) Manifest() core.CapabilityManifest {
	// A durable delegation resolves its child session/run from the immutable
	// parent invocation link, so rerunning the same outer call is safe after a
	// journaled approval pause or worker restart. The legacy in-process mode
	// does not make that guarantee and remains non-idempotent.
	durable := s.opts.Sessions != nil && s.opts.Links != nil
	return core.CapabilityManifest{
		ID: s.id, Version: "1.0.0", Name: s.id, Description: s.opts.Description,
		Kind: core.KindAgent, Contract: ContractV1,
		RequiredPermissions: []core.Permission{core.PermRead},
		PerTurnBudget:       2,
		Idempotent:          durable,
		Tool: &core.ToolExposure{Parameters: support.ObjectSchema(map[string]any{
			"prompt": map[string]any{
				"type": "string", "description": "Complete, self-contained task for the specialist agent.",
			},
			"session_id": map[string]any{
				"type": "string", "description": "Child session id from a previous delegation, to continue it.",
			},
			"followup": map[string]any{
				"type": "string", "description": "Next instruction when continuing an existing child session.",
			},
		})},
	}
}

func (s *Capability) Execute(ctx context.Context, request core.CapabilityRequest) (core.CapabilityResult, error) {
	depth := 0
	if raw, ok := request.Context.Principal.Attributes[AttributeDepth]; ok {
		if parsed, err := strconv.Atoi(raw); err == nil {
			if parsed < 0 {
				return support.DeniedResult("delegation_depth_invalid", "delegation depth cannot be negative"), nil
			}
			depth = parsed
		}
	}
	if depth >= s.opts.MaxDepth {
		return support.DeniedResult("agent_depth_exceeded",
			fmt.Sprintf("subagent delegation chain reached the maximum depth of %d", s.opts.MaxDepth)), nil
	}

	prompt, _ := request.Args["prompt"].(string)
	followup, _ := request.Args["followup"].(string)
	childSessionID, _ := request.Args["session_id"].(string)
	prompt = strings.TrimSpace(prompt)
	followup = strings.TrimSpace(followup)
	childSessionID = strings.TrimSpace(childSessionID)
	if childSessionID != "" || followup != "" {
		if childSessionID == "" || followup == "" {
			return support.DeniedResult(core.CodeInvalidArgs, "args: session_id and followup must be provided together"), nil
		}
		if err := core.ValidateSessionID(childSessionID); err != nil {
			return support.DeniedResult(core.CodeInvalidArgs, "args: session_id is invalid"), nil
		}
		if s.opts.Sessions != nil && s.opts.Links != nil {
			unlock := lockDurable("child:" + childSessionID)
			defer unlock()
			return s.continueDurableChild(ctx, request, childSessionID, followup, depth)
		}
		return s.continueChild(ctx, request, childSessionID, followup, depth)
	}
	if prompt == "" {
		return support.DeniedResult(core.CodeInvalidArgs, "args: missing required property \"prompt\""), nil
	}
	invocation := request.Context.Invocation
	if s.opts.Sessions != nil && s.opts.Links != nil && invocation.SessionID != "" && invocation.RunID != "" && invocation.CallID != "" {
		unlock := lockDurable("parent:" + invocation.SessionID + "\x00" + invocation.RunID + "\x00" + invocation.CallID)
		defer unlock()
		return s.executeDurable(ctx, request, prompt, depth)
	}
	if s.opts.Sessions != nil || s.opts.Links != nil {
		return support.DeniedResult("delegation_invocation_missing", "durable delegation requires parent session, run, and call identity"), nil
	}

	childPrincipal := support.ClonePrincipal(request.Context.Principal)
	childPrincipal.Attributes[AttributeDepth] = strconv.Itoa(depth + 1)
	childSessionID, err := core.NewID("sess_")
	if err != nil {
		return core.CapabilityResult{Content: err.Error(), OK: false}, nil
	}
	childScope, err := childPrincipal.Scope.Child(core.ScopeRef{Kind: core.ScopeSession, ID: childSessionID})
	if err != nil {
		return support.DeniedResult("invalid_execution", err.Error()), nil
	}
	childSession, err := core.NewSession(core.SessionOptions{
		ID: childSessionID, ProfileID: s.opts.ProfileID, Principal: childPrincipal, Scope: childScope,
	})
	if err != nil {
		return support.DeniedResult("invalid_execution", err.Error()), nil
	}
	s.rememberChild(childSessionID, childSession)
	runID, err := core.NewID("run_")
	if err != nil {
		return core.CapabilityResult{Content: err.Error(), OK: false}, nil
	}

	result, runErr := s.runtime.RunTurn(ctx, childPrincipal, childSession, core.TurnInput{RunID: runID, Text: prompt}, nil)
	if runErr != nil {
		return core.CapabilityResult{
			Content: fmt.Sprintf("subagent %s failed: %v", s.opts.ProfileID, runErr), OK: false,
			Metadata: map[string]any{"child_session_id": childSessionID},
		}, nil
	}
	return core.CapabilityResult{
		Content: result.Answer, OK: result.Status == core.RunCompleted,
		Metadata: map[string]any{
			"child_session_id": childSessionID, "status": string(result.Status), "depth": depth + 1,
		},
	}, nil
}

// executeDurable creates or reuses a stable child from the parent guarded
// invocation. A missing/uncertain persisted state is denied rather than
// risking a second child side effect.
func (s *Capability) executeDurable(ctx context.Context, request core.CapabilityRequest, prompt string, depth int) (core.CapabilityResult, error) {
	invocation := request.Context.Invocation
	if request.Context.RemainingToolCalls <= 0 {
		return support.DeniedResult(core.CodeBudgetExceeded, "delegation has no remaining parent tool-call budget"), nil
	}
	key := invocation.SessionID + "\x00" + invocation.RunID + "\x00" + invocation.CallID
	childSessionID, childRunID := delegationID("sess_", key), delegationID("run_", key)
	link, created, err := s.opts.Links.PutIfAbsent(ctx, Link{ParentSessionID: invocation.SessionID, ParentRunID: invocation.RunID, ParentCallID: invocation.CallID, ChildSessionID: childSessionID, ChildRunID: childRunID, TenantID: request.Context.Principal.TenantID, SubjectID: request.Context.Principal.SubjectID, Depth: depth + 1})
	if err != nil {
		return support.DeniedResult("delegation_store_unavailable", "delegation link could not be persisted"), nil
	}
	if link.ParentSessionID != invocation.SessionID || link.ParentRunID != invocation.RunID || link.ParentCallID != invocation.CallID || link.ChildSessionID == "" || link.ChildRunID == "" || link.TenantID != request.Context.Principal.TenantID || link.SubjectID != request.Context.Principal.SubjectID || link.Depth <= 0 || link.Depth > s.opts.MaxDepth {
		return support.DeniedResult("delegation_forbidden", "delegation link ownership or depth is invalid"), nil
	}
	child, err := s.opts.Sessions.Load(ctx, link.ChildSessionID)
	if err != nil {
		if !created {
			return support.DeniedResult("child_session_unavailable", "durable delegation link points to a missing child session"), nil
		}
		childPrincipal := support.ClonePrincipal(request.Context.Principal)
		childPrincipal.Attributes[AttributeDepth] = strconv.Itoa(link.Depth)
		scope, scopeErr := childPrincipal.Scope.Child(core.ScopeRef{Kind: core.ScopeSession, ID: link.ChildSessionID})
		if scopeErr != nil {
			return support.DeniedResult("invalid_execution", scopeErr.Error()), nil
		}
		child, err = core.NewSession(core.SessionOptions{ID: link.ChildSessionID, ProfileID: s.opts.ProfileID, Principal: childPrincipal, Scope: scope})
		if err != nil {
			return support.DeniedResult("invalid_execution", err.Error()), nil
		}
		if err = s.opts.Sessions.Create(ctx, child); err != nil {
			child, err = s.opts.Sessions.Load(ctx, link.ChildSessionID)
			if err != nil {
				return support.DeniedResult("delegation_store_unavailable", "child session could not be created or loaded"), nil
			}
		}
	}
	owner := child.Principal()
	if owner.TenantID != link.TenantID || owner.SubjectID != link.SubjectID {
		return support.DeniedResult("delegation_forbidden", "child session ownership changed"), nil
	}
	if status, exists := child.RunStatus(link.ChildRunID); exists {
		switch status {
		case core.RunCompleted, core.RunLimited, core.RunFailed, core.RunCancelled:
			return core.CapabilityResult{OK: status == core.RunCompleted, Metadata: delegationMetadata(link, status)}, nil
		case core.RunWaitingApproval:
			childCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			persister := newChildProgressPersister(s.opts.Sessions, child)
			emit := func(core.SessionEvent) {
				persister.emit(childCtx, cancel)
			}
			result, resumeErr := s.runtime.ResumeTurn(childCtx, owner, child, core.ResumeInput{RunID: link.ChildRunID}, emit)
			if persister.Err() != nil {
				return support.DeniedResult("delegation_store_unavailable", "child progress could not be persisted"), nil
			}
			if resumeErr != nil {
				return support.DeniedResult("delegation_resume_failed", "child approval resume failed closed"), nil
			}
			if result.Status == core.RunWaitingApproval {
				return core.CapabilityResult{}, childApprovalPending(child, link.ChildRunID)
			}
			return core.CapabilityResult{Content: result.Answer, OK: result.Status == core.RunCompleted, Metadata: delegationMetadata(link, result.Status)}, nil
		default:
			return support.DeniedResult("delegation_outcome_unknown", "child run is not terminal or approval-waiting"), nil
		}
	}
	quota := request.Context.RemainingToolCalls
	if s.opts.DelegationMaxToolCalls > 0 && s.opts.DelegationMaxToolCalls < quota {
		quota = s.opts.DelegationMaxToolCalls
	}
	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	persister := newChildProgressPersister(s.opts.Sessions, child)
	emit := func(core.SessionEvent) {
		persister.emit(childCtx, cancel)
	}
	result, runErr := s.runtime.RunTurn(childCtx, owner, child, core.TurnInput{RunID: link.ChildRunID, Text: prompt, MaxToolCallsOverride: quota}, emit)
	if persister.Err() != nil {
		return support.DeniedResult("delegation_store_unavailable", "child progress could not be persisted"), nil
	}
	if runErr != nil {
		return support.DeniedResult("delegation_run_failed", "child run failed closed"), nil
	}
	if result.Status == core.RunWaitingApproval {
		return core.CapabilityResult{}, childApprovalPending(child, link.ChildRunID)
	}
	return core.CapabilityResult{Content: result.Answer, OK: result.Status == core.RunCompleted, Metadata: delegationMetadata(link, result.Status)}, nil
}

// childApprovalPending preserves a nested durable approval as a suspension of
// the parent guarded call. This makes the server pause and later resume the
// original parent run, which re-enters this capability and resumes the same
// child run. Returning an ordinary unsuccessful result here would let the
// parent model continue and orphan the child's approval checkpoint.
func childApprovalPending(child *core.Session, runID string) error {
	pending, found, err := child.PendingApproval(runID)
	if err != nil {
		return fmt.Errorf("read child approval checkpoint: %w", err)
	}
	if !found {
		return fmt.Errorf("child run %s is waiting without an approval checkpoint", runID)
	}
	return &core.ApprovalPendingError{
		Request: core.ApprovalRequest{
			RunID: runID, SessionID: child.ID(), Principal: child.Principal(), ToolCall: pending.ToolCall,
		},
		Resolution: core.ApprovalResolution{
			ApprovalID: pending.ApprovalID, Decision: core.ApprovalPending, ExpiresAt: pending.ExpiresAt,
		},
	}
}

func (s *Capability) continueDurableChild(ctx context.Context, request core.CapabilityRequest, sessionID, followup string, depth int) (core.CapabilityResult, error) {
	link, found, err := s.opts.Links.GetByChild(ctx, sessionID)
	if err != nil || !found {
		return support.DeniedResult("child_session_unavailable", "child session is not a durable delegation"), nil
	}
	if link.ChildSessionID != sessionID || link.ChildRunID == "" || link.TenantID != request.Context.Principal.TenantID || link.SubjectID != request.Context.Principal.SubjectID || link.Depth <= 0 || link.Depth > s.opts.MaxDepth {
		return support.DeniedResult("child_session_forbidden", "child delegation is not owned by this subject"), nil
	}
	child, err := s.opts.Sessions.Load(ctx, sessionID)
	if err != nil {
		return support.DeniedResult("child_session_unavailable", "child session could not be loaded"), nil
	}
	owner := child.Principal()
	if owner.TenantID != link.TenantID || owner.SubjectID != link.SubjectID || !request.Context.Principal.Scope.IsAncestorOf(child.Scope()) {
		return support.DeniedResult("child_session_forbidden", "child session scope is no longer owned"), nil
	}
	runID, err := core.NewID("run_")
	if err != nil {
		return support.DeniedResult("invalid_execution", err.Error()), nil
	}
	quota := request.Context.RemainingToolCalls
	if quota <= 0 {
		return support.DeniedResult(core.CodeBudgetExceeded, "delegation has no remaining parent tool-call budget"), nil
	}
	if s.opts.DelegationMaxToolCalls > 0 && s.opts.DelegationMaxToolCalls < quota {
		quota = s.opts.DelegationMaxToolCalls
	}
	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	persister := newChildProgressPersister(s.opts.Sessions, child)
	emit := func(core.SessionEvent) {
		persister.emit(childCtx, cancel)
	}
	result, runErr := s.runtime.RunTurn(childCtx, owner, child, core.TurnInput{RunID: runID, Text: followup, MaxToolCallsOverride: quota}, emit)
	if persister.Err() != nil || runErr != nil {
		return support.DeniedResult("delegation_store_unavailable", "child continuation failed closed"), nil
	}
	return core.CapabilityResult{Content: result.Answer, OK: result.Status == core.RunCompleted, Metadata: delegationMetadata(Link{ParentSessionID: link.ParentSessionID, ParentRunID: link.ParentRunID, ParentCallID: link.ParentCallID, ChildSessionID: link.ChildSessionID, ChildRunID: runID, Depth: depth + 1}, result.Status)}, nil
}

func delegationMetadata(link Link, status core.RunStatus) map[string]any {
	return map[string]any{"parent_session_id": link.ParentSessionID, "parent_run_id": link.ParentRunID, "parent_call_id": link.ParentCallID, "child_session_id": link.ChildSessionID, "child_run_id": link.ChildRunID, "status": string(status), "depth": link.Depth}
}

func (s *Capability) rememberChild(id string, session *core.Session) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.children[id]; !exists {
		s.order = append(s.order, id)
	}
	s.children[id] = session
	for len(s.order) > s.opts.MaxChildren {
		oldest := s.order[0]
		s.order = s.order[1:]
		delete(s.children, oldest)
	}
}

func (s *Capability) continueChild(ctx context.Context, request core.CapabilityRequest, sessionID, followup string, depth int) (core.CapabilityResult, error) {
	s.mu.Lock()
	child := s.children[sessionID]
	s.mu.Unlock()
	if child == nil {
		return support.DeniedResult("child_session_unavailable",
			fmt.Sprintf("child session %s is not continuable in this process", sessionID)), nil
	}
	owner := child.Principal()
	if owner.SubjectID != request.Context.Principal.SubjectID || owner.TenantID != request.Context.Principal.TenantID {
		return support.DeniedResult("child_session_forbidden", "only the delegating subject may continue this child session"), nil
	}
	childPrincipal := support.ClonePrincipal(request.Context.Principal)
	childPrincipal.Attributes[AttributeDepth] = strconv.Itoa(depth + 1)
	runID, err := core.NewID("run_")
	if err != nil {
		return core.CapabilityResult{Content: err.Error(), OK: false}, nil
	}
	result, runErr := s.runtime.RunTurn(ctx, childPrincipal, child, core.TurnInput{RunID: runID, Text: followup}, nil)
	if runErr != nil {
		return core.CapabilityResult{
			Content: fmt.Sprintf("subagent continuation failed: %v", runErr), OK: false,
			Metadata: map[string]any{"child_session_id": sessionID},
		}, nil
	}
	return core.CapabilityResult{
		Content: result.Answer, OK: result.Status == core.RunCompleted,
		Metadata: map[string]any{
			"child_session_id": sessionID, "status": string(result.Status), "depth": depth + 1,
		},
	}, nil
}
