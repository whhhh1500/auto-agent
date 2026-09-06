package core

import (
	"context"
	"fmt"
	"strings"
)

// ModelResolver selects an LLM adapter for a resolved agent profile.
type ModelResolver interface {
	ResolveModel(ctx context.Context, selection ModelSelection) (LlmAdapter, error)
}

// ModelResolverFunc adapts a function to ModelResolver.
type ModelResolverFunc func(context.Context, ModelSelection) (LlmAdapter, error)

func (f ModelResolverFunc) ResolveModel(ctx context.Context, selection ModelSelection) (LlmAdapter, error) {
	return f(ctx, selection)
}

// FastRouterResolver optionally supplies deterministic routes for a profile.
type FastRouterResolver interface {
	ResolveFastRouter(ctx context.Context, profile *AgentProfileSnapshot) (*FastRouter, error)
}

// FastRouterResolverFunc adapts a function to FastRouterResolver.
type FastRouterResolverFunc func(context.Context, *AgentProfileSnapshot) (*FastRouter, error)

func (f FastRouterResolverFunc) ResolveFastRouter(ctx context.Context, profile *AgentProfileSnapshot) (*FastRouter, error) {
	return f(ctx, profile)
}

// Runtime composes profiles, capabilities and models without owning transport or persistence.
type Runtime struct {
	Capabilities *CapabilityRegistry
	Credentials  CredentialResolver
	Profiles     *AgentProfileRegistry
	Models       ModelResolver
	FastRouters  FastRouterResolver
	// Policy intersects layered restrictions (permissions, step and tool-call
	// caps) for every run. Optional; absent means no additional narrowing.
	Policy *PolicyRegistry
	// Hooks and Approver install the guardrail and human-approval seams for
	// every run. Optional.
	Hooks    RunHooks
	Approver Approver
	// RateLimiter bounds per-tenant per-capability call frequency across
	// runs. Optional.
	RateLimiter CallRateLimiter
	// ToolJournal durably fences provider side effects at the final guard
	// boundary. Optional.
	ToolJournal ToolInvocationJournal
	// ModelCallGate authorizes each model adapter call. Optional.
	ModelCallGate ModelCallGate
	// Telemetry records runtime spans and metrics through an SDK-neutral seam.
	Telemetry Telemetry
	// Compactor reduces projected history before each model request. Optional.
	Compactor ContextCompactor
	// ContextAssembler is the optional bounded final model-context selector.
	ContextAssembler ModelContextAssembler
	// Summarizer durably archives an over-budget history prefix into a
	// context/summary event. Optional; when unset, long histories are only
	// trimmed by the mechanical Compactor.
	Summarizer RunSummarizer
	// StreamChunks persists assistant/chunk events for streamed text. Optional.
	StreamChunks bool
	// DiscloseTools enables the optional tool-library presentation protocol for
	// every Agent composed by this Runtime. The zero value preserves direct
	// snapshot tool schemas and calls.
	DiscloseTools bool
}

// RunTurn resolves immutable snapshots and appends one turn to session.
func (r *Runtime) RunTurn(
	ctx context.Context,
	principal Principal,
	session *Session,
	input TurnInput,
	emit func(SessionEvent),
) (TurnResult, error) {
	if err := r.validateSessionRun(ctx, principal, session, input.RunID, input.CompositionMetadata); err != nil {
		return TurnResult{}, err
	}
	if input.MaxToolCallsOverride < 0 || input.MaxToolCallsOverride > HardMaxToolCalls {
		return TurnResult{}, fmt.Errorf("max tool calls override must be between 0 and %d", HardMaxToolCalls)
	}

	// Run-scoped bindings (one-shot capabilities) are mounted under
	// <session-scope>/run/<run-id> for the resolve step and removed
	// immediately after: the snapshot freezes their providers, so later
	// mutations cannot leak into this run.
	sessionScope := session.Scope()
	runScope, unmountRun, err := r.mountRunCapabilities(input.RunCapabilities, sessionScope, input.RunID)
	if err != nil {
		return compositionFailure(session, input, emit, "run_capability_mount_failed", err)
	}
	capabilityTarget := sessionScope
	if !runScope.Equal(sessionScope) {
		capabilityTarget = runScope
	}

	agent, code, err := r.composeAgent(ctx, principal, session, capabilityTarget, input.CompositionMetadata, input.MaxToolCallsOverride, input.CapabilityFilter, emit, unmountRun)
	if err != nil {
		return compositionFailure(session, input, emit, code, err)
	}
	return agent.RunTurn(ctx, input)
}

// ResumeTurn re-resolves the current principal, policy, capability and model
// surfaces, then continues one run suspended at approval/requested. It never
// appends a second run/start. Revoked access therefore fails the original run
// instead of executing with stale grants.
func (r *Runtime) ResumeTurn(
	ctx context.Context,
	principal Principal,
	session *Session,
	input ResumeInput,
	emit func(SessionEvent),
) (TurnResult, error) {
	if err := r.validateSessionRun(ctx, principal, session, input.RunID, input.CompositionMetadata); err != nil {
		return TurnResult{}, err
	}
	if status, exists := session.RunStatus(input.RunID); !exists || status != RunWaitingApproval {
		return TurnResult{}, fmt.Errorf("run %s is not waiting for approval", input.RunID)
	}
	agent, code, err := r.composeResumeAgent(ctx, principal, session, input.CompositionMetadata, emit)
	if err != nil {
		return resumeCompositionFailure(session, input, emit, code, err)
	}
	return agent.ResumeTurn(ctx, input.RunID)
}

// ContinueTurn resumes a verified post-result tool sequence without replaying
// its already-durable results. Remaining calls and any next model step use
// the current runtime policy and capability snapshot.
func (r *Runtime) ContinueTurn(ctx context.Context, principal Principal, session *Session, input ResumeInput, emit func(SessionEvent)) (TurnResult, error) {
	if err := r.validateSessionRun(ctx, principal, session, input.RunID, input.CompositionMetadata); err != nil {
		return TurnResult{}, err
	}
	if status, exists := session.RunStatus(input.RunID); !exists || status != "" {
		return TurnResult{}, fmt.Errorf("run %s is not an open post-result continuation", input.RunID)
	}
	if _, _, _, err := postResultContinuation(session.Events(), input.RunID); err != nil {
		return TurnResult{}, err
	}
	agent, code, err := r.composeResumeAgent(ctx, principal, session, input.CompositionMetadata, emit)
	if err != nil {
		return TurnResult{}, fmt.Errorf("%s: %w", code, err)
	}
	return agent.continueTurn(ctx, input.RunID)
}

func (r *Runtime) validateSessionRun(ctx context.Context, principal Principal, session *Session, runID string, metadata map[string]string) error {
	if ctx == nil {
		return fmt.Errorf("run context is nil")
	}
	if err := r.Validate(); err != nil {
		return err
	}
	if session == nil {
		return fmt.Errorf("session is nil")
	}
	if err := ValidateRunID(runID); err != nil {
		return err
	}
	if err := ValidateRunCompositionMetadata(metadata); err != nil {
		return err
	}
	owner := session.Principal()
	if principal.SubjectID != owner.SubjectID || principal.TenantID != owner.TenantID || !principal.Scope.Equal(owner.Scope) {
		return fmt.Errorf("principal does not own session %q", session.ID())
	}
	return nil
}

func (r *Runtime) composeResumeAgent(ctx context.Context, principal Principal, session *Session, metadata map[string]string, emit func(SessionEvent)) (*Agent, string, error) {
	return r.composeAgent(ctx, principal, session, session.Scope(), metadata, 0, nil, emit, nil)
}

func (r *Runtime) composeAgent(ctx context.Context, principal Principal, session *Session, capabilityScope ScopePath, metadata map[string]string, maxToolCallsOverride int, filter CapabilityFilter, emit func(SessionEvent), release func()) (*Agent, string, error) {
	sessionScope := session.Scope()
	profile, err := r.Profiles.Resolve(principal, sessionScope, session.ProfileID())
	if err != nil {
		if release != nil {
			release()
		}
		return nil, "profile_resolution_failed", err
	}
	capabilities, err := (CapabilityResolver{Registry: r.Capabilities, Credentials: r.Credentials}).resolveForProfile(principal, capabilityScope)
	if release != nil {
		release()
	}
	if err != nil {
		return nil, "capability_resolution_failed", err
	}
	capabilities, err = profile.FilterCapabilities(capabilities)
	if err != nil {
		return nil, "profile_capability_mismatch", err
	}
	capabilities = capabilities.FilterByPermissions(principal.Grants)
	maxSteps := profile.MaxSteps
	maxToolCalls := profile.MaxToolCalls
	if maxToolCallsOverride > 0 && maxToolCallsOverride < maxToolCalls {
		maxToolCalls = maxToolCallsOverride
	}
	effectivePermissions := principal.Grants.Clone()
	if r.Policy != nil {
		policy, err := r.Policy.Resolve(principal, sessionScope)
		if err != nil {
			return nil, "policy_resolution_failed", err
		}
		// Policy narrowing silently drops capabilities the effective
		// permissions no longer cover; composition errors stay reserved for
		// profile mistakes.
		capabilities = capabilities.FilterByPermissions(policy.Permissions)
		effectivePermissions = policy.Permissions.Clone()
		if policy.MaxSteps != nil && *policy.MaxSteps < maxSteps {
			maxSteps = *policy.MaxSteps
		}
		if policy.MaxToolCalls != nil && *policy.MaxToolCalls < maxToolCalls {
			maxToolCalls = *policy.MaxToolCalls
		}
	}
	capabilities = capabilities.FilterByPredicate(filter)
	model, err := safeResolveModel(r.Models, ctx, profile.Model)
	if err != nil {
		return nil, "model_resolution_failed", err
	}
	var fast *FastRouter
	if r.FastRouters != nil {
		fast, err = safeResolveFastRouter(r.FastRouters, ctx, profile)
		if err != nil {
			return nil, "fast_router_resolution_failed", err
		}
	}
	agent, err := NewAgent(AgentOptions{
		LLM: model, Tools: capabilities, Session: session,
		System: profile.SystemPrompt(), Provider: profile.Model.Provider, Model: profile.Model.Model,
		MaxSteps: maxSteps, MaxToolCalls: maxToolCalls, ProfileSnapshotID: profile.ID,
		CapabilitySnapshotID: capabilities.ID,
		Composition: &RunCompositionData{
			Profile: runAuditProfile(profile), Capabilities: runAuditCapabilities(capabilities),
			EffectivePermissions: effectivePermissions,
			Model:                profile.Model, ResolvedProvider: safeModelProvider(model),
			ModelRevision: artifactRevision(model), MaxSteps: maxSteps, MaxToolCalls: maxToolCalls,
			Metadata: cloneRunCompositionMetadata(metadata),
		},
		OnEvent: emit, Fast: fast,
		Hooks: r.Hooks, Approver: r.Approver, Compactor: r.Compactor, ContextAssembler: r.ContextAssembler, StreamChunks: r.StreamChunks,
		DiscloseTools: r.DiscloseTools,
		Summarizer:    r.Summarizer, RateLimiter: r.RateLimiter, ToolJournal: r.ToolJournal, ModelCallGate: r.ModelCallGate, Telemetry: r.Telemetry,
	})
	if err != nil {
		return nil, "agent_composition_failed", err
	}
	return agent, "", nil
}

// Validate checks the kernel dependencies without starting a run.
func (r *Runtime) Validate() error {
	if r == nil {
		return fmt.Errorf("runtime is nil")
	}
	if r.Capabilities == nil || r.Profiles == nil || r.Models == nil {
		return fmt.Errorf("runtime dependencies are incomplete")
	}
	return nil
}

func safeResolveModel(resolver ModelResolver, ctx context.Context, selection ModelSelection) (adapter LlmAdapter, err error) {
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("model resolver panicked")
			adapter = nil
		}
	}()
	adapter, err = resolver.ResolveModel(ctx, selection)
	if err == nil && adapter == nil {
		return nil, fmt.Errorf("model resolver returned a nil adapter")
	}
	return adapter, err
}

func safeResolveFastRouter(resolver FastRouterResolver, ctx context.Context, profile *AgentProfileSnapshot) (router *FastRouter, err error) {
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("fast router resolver panicked")
			router = nil
		}
	}()
	return resolver.ResolveFastRouter(ctx, profile)
}

func safeModelProvider(adapter LlmAdapter) (provider string) {
	defer func() {
		if recover() != nil {
			provider = "provider-unavailable"
		}
	}()
	provider = strings.TrimSpace(adapter.Provider())
	if provider == "" {
		provider = "provider-unknown"
	}
	return provider
}

// runAuditCapabilities removes literal HTTP header values from the durable
// run composition. Credential references remain useful for audit/replay while
// concrete header values never enter the session event log.
func runAuditCapabilities(snapshot *CapabilitySnapshot) []SnapshotCapability {
	capabilities := snapshot.Capabilities()
	for i := range capabilities {
		capabilities[i].Manifest = publicManifest(capabilities[i].Manifest)
	}
	return capabilities
}

func runAuditProfile(profile *AgentProfileSnapshot) AgentProfileSnapshot {
	copyOf := *profile
	copyOf.Capabilities = append([]string(nil), profile.Capabilities...)
	copyOf.Fragments = append([]ResolvedPromptFragment(nil), profile.Fragments...)
	copyOf.Metadata = redactStringMetadata(profile.Metadata)
	return copyOf
}

func redactStringMetadata(metadata map[string]string) map[string]string {
	if metadata == nil {
		return nil
	}
	out := make(map[string]string, len(metadata))
	for key, value := range metadata {
		lower := strings.ToLower(key)
		if strings.Contains(lower, "secret") || strings.Contains(lower, "password") ||
			strings.Contains(lower, "token") || strings.Contains(lower, "api_key") ||
			strings.Contains(lower, "credential") || strings.Contains(lower, "authorization") {
			out[key] = "[redacted]"
			continue
		}
		out[key] = value
	}
	return out
}

// mountRunCapabilities attaches TurnInput run-scoped bindings under
// <session-scope>/run/<run-id> and returns the run scope plus the unmount
// function. A failed mount rolls back the bindings already applied. When no
// bindings are supplied the returned scope equals the session scope.
func (r *Runtime) mountRunCapabilities(bindings []CapabilityBinding, sessionScope ScopePath, runID string) (ScopePath, func(), error) {
	if len(bindings) == 0 {
		return sessionScope, func() {}, nil
	}
	if runID == "" {
		return sessionScope, func() {}, fmt.Errorf("run capabilities require a run id")
	}
	runScope, err := sessionScope.Child(ScopeRef{Kind: ScopeRun, ID: runID})
	if err != nil {
		return sessionScope, func() {}, err
	}
	unmounts := make([]func(), 0, len(bindings))
	for _, binding := range bindings {
		binding.Scope = runScope
		unmount, err := r.Capabilities.Mount(binding)
		if err != nil {
			for _, undo := range unmounts {
				undo()
			}
			return sessionScope, func() {}, err
		}
		unmounts = append(unmounts, unmount)
	}
	return runScope, func() {
		for _, undo := range unmounts {
			undo()
		}
	}, nil
}

func compositionFailure(
	session *Session,
	input TurnInput,
	emit func(SessionEvent),
	code string,
	cause error,
) (TurnResult, error) {
	if err := appendCompositionFailure(session, input.RunID, input.CompositionMetadata, emit, EvRunStart, input.Text, code, cause); err != nil {
		return TurnResult{}, err
	}
	return TurnResult{RunID: input.RunID, Status: RunFailed}, cause
}

func resumeCompositionFailure(session *Session, input ResumeInput, emit func(SessionEvent), code string, cause error) (TurnResult, error) {
	if err := appendCompositionFailure(session, input.RunID, input.CompositionMetadata, emit, EvRunResume, "", code, cause); err != nil {
		return TurnResult{}, err
	}
	return TurnResult{RunID: input.RunID, Status: RunFailed}, cause
}

func appendCompositionFailure(session *Session, runID string, metadata map[string]string, emit func(SessionEvent), start SessionEventType, text, code string, cause error) error {
	appendEvent := func(eventType SessionEventType, data any) error {
		event, err := session.Append(runID, eventType, data)
		if err != nil {
			return err
		}
		if emit != nil {
			_ = safeEventCallback(emit, event)
		}
		return nil
	}
	composition, metadataErr := failureComposition(session, metadata)
	if metadataErr != nil {
		return metadataErr
	}
	compositionRevision, err := CompositionRevision(composition)
	if err != nil {
		return err
	}
	assignmentRevision, err := CompositionMetadataRevision(compositionMetadata(composition))
	if err != nil {
		return err
	}
	if start == EvRunStart {
		err = appendEvent(start, RunStartData{CompositionRevision: compositionRevision, AssignmentRevision: assignmentRevision, Composition: composition})
	} else {
		err = appendEvent(start, RunResumeData{CompositionRevision: compositionRevision, AssignmentRevision: assignmentRevision, Composition: composition})
	}
	if err != nil {
		return err
	}
	if start == EvRunStart {
		if err := appendEvent(EvUserMessage, UserMessageData{Text: text}); err != nil {
			return err
		}
	}
	if err := appendEvent(EvRunError, NewRuntimeErrorData(code, cause, false)); err != nil {
		return err
	}
	if err := appendEvent(EvRunEnd, RunEndData{Status: RunFailed}); err != nil {
		return err
	}
	return nil
}

func failureComposition(session *Session, metadata map[string]string) (*RunCompositionData, error) {
	composition, err := compositionWithMetadata(nil, metadata)
	if err != nil {
		return nil, err
	}
	if composition != nil && session != nil {
		composition.Profile.ProfileID = session.ProfileID()
		composition.Profile.Scope = session.Scope()
	}
	return composition, nil
}
