package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

var terminalHookTimeout = 5 * time.Second
var errFastToolCallAppend = errors.New("fast tool call append failed")

const (
	DefaultMaxSteps     = 10
	DefaultMaxToolCalls = 16
	HardMaxSteps        = 128
	HardMaxToolCalls    = 1024
)

// ToolRuntime is the model-tool consumer for a fixed run capability snapshot.
type ToolRuntime interface {
	Schemas() []ToolSchema
	Execute(ctx context.Context, call ToolCall) (CapabilityResult, error)
	Authorized(name string) bool
	MaxCallBudget() int
}

// AgentOptions supplies one fully composed, integration-neutral runtime.
type AgentOptions struct {
	LLM                  LlmAdapter
	Tools                ToolRuntime
	Session              *Session
	System               string
	Provider             string
	Model                string
	MaxSteps             int
	MaxToolCalls         int
	ProfileSnapshotID    string
	CapabilitySnapshotID string
	Composition          *RunCompositionData
	OnEvent              func(event SessionEvent)
	Fast                 *FastRouter
	// Hooks run automatic guardrail checks around the run. Optional.
	Hooks RunHooks
	// Approver gates tool calls whose manifest declares RequiresApproval.
	// A required approval without an approver is denied (fail-closed). Optional.
	Approver Approver
	// RateLimiter bounds per-tenant per-capability call frequency across
	// runs. Optional; denials return a stable rate-limited refusal.
	RateLimiter CallRateLimiter
	// ToolJournal durably fences provider side effects and replays completed
	// results for the same run/call identity. Optional.
	ToolJournal ToolInvocationJournal
	// ModelCallGate authorizes each model adapter call. Nil preserves legacy
	// behavior and deliberately mints no AcceptedModelCall.
	ModelCallGate ModelCallGate
	// Telemetry records bounded runtime spans and metrics without coupling the
	// kernel to an observability SDK. Optional.
	Telemetry Telemetry
	// Compactor reduces the projected history before each model request.
	// Optional; nil sends the full projection.
	Compactor ContextCompactor
	// Summarizer durably archives an over-budget history prefix into a
	// context/summary event before the mechanical compactor runs. Optional.
	Summarizer RunSummarizer
	// ContextAssembler makes one bounded, ephemeral selection after summary and
	// compaction but before every model call. Nil preserves the legacy payload.
	ContextAssembler ModelContextAssembler
	// StreamChunks persists assistant/chunk events for streamed text so
	// transport adapters can render token-level output. Optional; chunk events
	// are excluded from model history projection.
	StreamChunks bool
	// DiscloseTools replaces non-hot snapshot tools with the optional tool
	// library discovery protocol. It is disabled by default so existing model
	// tool schemas and direct-call behavior remain unchanged.
	DiscloseTools bool
}

// TurnInput identifies one append-only run in an existing session. RunCapabilities
// are optional one-shot bindings mounted at <session-scope>/run/<run-id> for
// this resolve only (product doc §5.8); they disappear from later runs.
type TurnInput struct {
	RunID                string
	Text                 string
	MaxToolCallsOverride int
	RunCapabilities      []CapabilityBinding
	CapabilityFilter     CapabilityFilter
	CompositionMetadata  map[string]string
}

// ResumeInput identifies one approval-resumed execution segment. Metadata is
// persisted on run/resume with the newly resolved recipe.
type ResumeInput struct {
	RunID               string
	CompositionMetadata map[string]string
}

// TurnResult is the current state returned to an interface adapter. A waiting
// approval result is non-terminal and resumes under the same Run ID.
type TurnResult struct {
	RunID  string    `json:"run_id"`
	Status RunStatus `json:"status"`
	Answer string    `json:"answer,omitempty"`
}

// Agent runs turns against one caller-owned session.
type Agent struct {
	opts                AgentOptions
	tools               ToolRuntime
	runMu               sync.Mutex
	runID               string
	counts              map[string]int
	toolCalls           int
	usage               TokenUsage
	approvalResolutions map[string]ApprovalResolution
	resumeCallIDs       map[string]bool
	compositionRevision string
}

// NewAgent validates dependencies without starting any work.
func NewAgent(opts AgentOptions) (*Agent, error) {
	if opts.LLM == nil {
		return nil, fmt.Errorf("agent LLM is nil")
	}
	if opts.Tools == nil {
		return nil, fmt.Errorf("agent tool runtime is nil")
	}
	if opts.Session == nil {
		return nil, fmt.Errorf("agent session is nil")
	}
	if opts.MaxSteps < 0 {
		return nil, fmt.Errorf("agent max steps must not be negative")
	}
	if opts.MaxToolCalls < 0 {
		return nil, fmt.Errorf("agent max tool calls must not be negative")
	}
	if opts.MaxSteps > HardMaxSteps {
		return nil, fmt.Errorf("agent max steps exceeds hard limit %d", HardMaxSteps)
	}
	if opts.MaxToolCalls > HardMaxToolCalls {
		return nil, fmt.Errorf("agent max tool calls exceeds hard limit %d", HardMaxToolCalls)
	}
	if opts.MaxSteps == 0 {
		opts.MaxSteps = DefaultMaxSteps
	}
	if opts.MaxToolCalls == 0 {
		opts.MaxToolCalls = DefaultMaxToolCalls
	}
	if err := validateRunComposition(opts.Composition); err != nil {
		return nil, err
	}
	opts.Composition = cloneRunCompositionData(opts.Composition)
	if opts.DiscloseTools {
		opts.Tools = discloseToolRuntime(opts.Tools)
	}
	agent := &Agent{
		opts: opts, counts: map[string]int{}, approvalResolutions: map[string]ApprovalResolution{},
		resumeCallIDs: map[string]bool{},
	}
	// One guarded funnel enforces validation, hooks, approval, and budgets for
	// every consumer — the model loop and deterministic fast routes alike.
	agent.tools = &guardedToolRuntime{agent: agent, inner: opts.Tools}
	return agent, nil
}

// RunTurn appends one run to the existing session. It may return
// RunWaitingApproval before run/end; ResumeTurn continues that same Run ID.
// Calls on the same Agent are serialized because its guarded tool runtime uses
// run-scoped state.
func (a *Agent) RunTurn(ctx context.Context, input TurnInput) (turnResult TurnResult, turnErr error) {
	if ctx == nil {
		return TurnResult{}, fmt.Errorf("run context is nil")
	}
	if err := ValidateRunID(input.RunID); err != nil {
		return TurnResult{}, err
	}
	if input.Text == "" {
		return TurnResult{}, fmt.Errorf("turn text is empty")
	}
	if input.MaxToolCallsOverride < 0 || input.MaxToolCallsOverride > HardMaxToolCalls {
		return TurnResult{}, fmt.Errorf("max tool calls override must be between 0 and %d", HardMaxToolCalls)
	}
	composition, err := compositionWithMetadata(a.opts.Composition, input.CompositionMetadata)
	if err != nil {
		return TurnResult{}, err
	}
	compositionRevision, err := CompositionRevision(composition)
	if err != nil {
		return TurnResult{}, err
	}
	assignmentRevision, err := CompositionMetadataRevision(compositionMetadata(composition))
	if err != nil {
		return TurnResult{}, err
	}
	a.runMu.Lock()
	defer a.runMu.Unlock()
	a.compositionRevision = compositionRevision

	session := a.opts.Session
	a.runID = input.RunID
	a.counts = make(map[string]int)
	a.toolCalls = 0
	a.usage = TokenUsage{}
	a.approvalResolutions = map[string]ApprovalResolution{}
	a.resumeCallIDs = map[string]bool{}
	info := RunInfo{
		RunID: input.RunID, SessionID: session.ID(), ProfileID: session.ProfileID(),
		Principal: session.Principal(),
	}
	ctx, runSpan := StartTelemetry(a.opts.Telemetry, ctx, SpanRunSegment, TelemetryAttributes{
		"run.id": input.RunID, "session.id": session.ID(), "profile.id": session.ProfileID(),
		"tenant.id": info.Principal.TenantID, "run.resume": "false",
	})
	runStarted := time.Now()
	defer func() {
		status := telemetryRunStatus(turnResult, turnErr)
		runSpan.End(turnErr, TelemetryAttributes{"run.status": status})
		metricAttrs := TelemetryAttributes{"run.status": status, "run.resume": "false", "profile.id": session.ProfileID()}
		AddTelemetryCounter(a.opts.Telemetry, ctx, MetricRuns, 1, metricAttrs)
		RecordTelemetryHistogram(a.opts.Telemetry, ctx, MetricRunDuration, time.Since(runStarted).Seconds(), "s", metricAttrs)
	}()

	if err := a.append(input.RunID, EvRunStart, RunStartData{
		ProfileSnapshotID: a.opts.ProfileSnapshotID, CapabilitySnapshotID: a.opts.CapabilitySnapshotID,
		CompositionRevision: compositionRevision, AssignmentRevision: assignmentRevision,
		Composition: composition,
	}); err != nil {
		return TurnResult{}, err
	}
	if err := a.append(input.RunID, EvUserMessage, UserMessageData{Text: input.Text}); err != nil {
		return a.fail(info, "event_append_failed", err, false)
	}
	if a.opts.Hooks != nil {
		if err := safeHookError("OnRunStart", func() error { return a.opts.Hooks.OnRunStart(ctx, info) }); err != nil {
			return a.fail(info, "input_rejected", err, false)
		}
	}

	if a.opts.Fast != nil {
		dispatch, err := safeFastDispatch(a.opts.Fast, ctx, input.Text, a.tools)
		if err != nil {
			if errors.Is(err, errFastToolCallAppend) {
				return a.fail(info, "event_append_failed", err, false)
			}
			if pending, ok := IsApprovalPending(err); ok && dispatch.Call != nil {
				return a.pauseForApproval(info, pending, *dispatch.Call, nil, true)
			}
			return a.fail(info, "fast_route_failed", err, isRetryable(err))
		}
		if dispatch.Matched {
			if dispatch.Result != nil && dispatch.Call != nil {
				if err := a.append(input.RunID, EvToolResult, ToolResultData{
					CallID: dispatch.Call.ID, Content: dispatch.Result.Content,
					OK: dispatch.Result.OK, Metadata: dispatch.Result.Metadata,
				}); err != nil {
					return a.fail(info, "event_append_failed", err, false)
				}
				if err := a.appendApprovalResolutions(input.RunID); err != nil {
					return a.fail(info, "event_append_failed", err, false)
				}
			}
			if err := a.append(input.RunID, EvAssistantMessage, AssistantMessageData{Text: dispatch.Answer}); err != nil {
				return a.fail(info, "event_append_failed", err, false)
			}
			return a.complete(info, RunCompleted)
		}
	}

	return a.runModelSteps(ctx, info, 0)
}

func (a *Agent) runModelSteps(ctx context.Context, info RunInfo, startStep int) (TurnResult, error) {
	session := a.opts.Session
	for step := startStep; step < a.opts.MaxSteps; step++ {
		info.Step = step
		if a.opts.Hooks != nil {
			if err := safeHookError("OnBeforeStep", func() error { return a.opts.Hooks.OnBeforeStep(ctx, info) }); err != nil {
				return a.fail(info, "step_rejected", err, false)
			}
		}
		if err := a.append(info.RunID, EvStepStart, StepData{Index: step}); err != nil {
			return a.fail(info, "event_append_failed", err, false)
		}

		var (
			messages []ChatMessage
			err      error
		)
		fused := false
		if a.opts.Summarizer == nil {
			if compactor, ok := asRecentTurnsCompactor(a.opts.Compactor); ok {
				messages, fused = session.deriveRecentCompactedMessages(compactor)
			}
		}
		if !fused {
			messages, err = session.DeriveMessages()
			if err != nil {
				_ = a.append(info.RunID, EvStepError, NewRuntimeErrorData("history_projection_failed", err, false))
				return a.fail(info, "history_projection_failed", err, false)
			}
			if a.opts.Summarizer != nil {
				messages, err = safeEnsureSummarized(a.opts.Summarizer, ctx, session, info.RunID, a.emit, messages)
				if err != nil {
					_ = a.append(info.RunID, EvStepError, NewRuntimeErrorData("summarization_failed", err, isRetryable(err)))
					return a.fail(info, "summarization_failed", err, isRetryable(err))
				}
			}
			if a.opts.Compactor != nil {
				messages, err = safeCompact(a.opts.Compactor, messages)
				if err != nil {
					_ = a.append(info.RunID, EvStepError, NewRuntimeErrorData("compaction_failed", err, false))
					return a.fail(info, "compaction_failed", err, false)
				}
			}
		}

		stream, failureCode, err := a.callModel(ctx, info, step, messages)
		if usageErr := addTokenUsage(&a.usage, stream.Usage); usageErr != nil {
			_ = a.append(info.RunID, EvStepError, NewRuntimeErrorData("usage_invalid", usageErr, false))
			return a.fail(info, "usage_invalid", usageErr, false)
		}
		if err != nil {
			if failureCode == "model_gate_rejected" {
				return a.fail(info, failureCode, err, false)
			}
			_ = a.append(info.RunID, EvStepError, NewRuntimeErrorData(failureCode, err, isRetryable(err)))
			return a.fail(info, failureCode, err, isRetryable(err))
		}
		text, calls := stream.Text, stream.ToolCalls

		if err := a.append(info.RunID, EvAssistantMessage, AssistantMessageData{
			Text: text, ToolCall: firstCall(calls), ToolCalls: calls,
		}); err != nil {
			return a.fail(info, "event_append_failed", err, false)
		}
		if len(calls) == 0 {
			if err := a.append(info.RunID, EvStepEnd, StepData{Index: step}); err != nil {
				return a.fail(info, "event_append_failed", err, false)
			}
			if a.opts.Hooks != nil {
				safeHookNotify(func() { a.opts.Hooks.OnAfterStep(ctx, info) })
			}
			return a.complete(info, RunCompleted)
		}

		for callIndex, call := range calls {
			if a.toolCalls >= a.opts.MaxToolCalls {
				if err := a.append(info.RunID, EvAssistantMessage, AssistantMessageData{Text: "Tool call budget reached; the run stopped."}); err != nil {
					return a.fail(info, "event_append_failed", err, false)
				}
				_ = a.append(info.RunID, EvStepEnd, StepData{Index: step})
				if a.opts.Hooks != nil {
					safeHookNotify(func() { a.opts.Hooks.OnAfterStep(ctx, info) })
				}
				return a.complete(info, RunLimited)
			}
			if err := a.append(info.RunID, EvToolCall, ToolCallData{CallID: call.ID, Name: call.Name, Args: call.Args}); err != nil {
				return a.fail(info, "event_append_failed", err, false)
			}
			result, executeErr := a.tools.Execute(ctx, call)
			if executeErr != nil {
				if pending, ok := IsApprovalPending(executeErr); ok {
					return a.pauseForApproval(info, pending, call, calls[callIndex+1:], false)
				}
				if errors.Is(executeErr, context.Canceled) || errors.Is(executeErr, context.DeadlineExceeded) {
					if ctx.Err() != nil {
						// The caller revoked the run: continuing would start
						// new model work the caller already cancelled.
						return a.fail(info, "tool_cancelled", executeErr, false)
					}
					// A per-tool timeout under a live run context is a tool
					// result the model can see and react to.
				}
				result = CapabilityResult{Content: boundedText(executeErr.Error(), DefaultMaxCapabilityOutputBytes), OK: false}
			}
			if err := a.append(info.RunID, EvToolResult, ToolResultData{
				CallID: call.ID, Content: result.Content, OK: result.OK, Metadata: result.Metadata,
			}); err != nil {
				return a.fail(info, "event_append_failed", err, false)
			}
			if err := a.appendApprovalResolutions(info.RunID); err != nil {
				return a.fail(info, "event_append_failed", err, false)
			}
		}
		if err := a.append(info.RunID, EvStepEnd, StepData{Index: step}); err != nil {
			return a.fail(info, "event_append_failed", err, false)
		}
		if a.opts.Hooks != nil {
			safeHookNotify(func() { a.opts.Hooks.OnAfterStep(ctx, info) })
		}
	}

	return a.complete(info, RunLimited)
}

// ResumeTurn continues one run suspended at an approval checkpoint. It does
// not append a second run/start or user/message. The pending outer call is
// rerun through the current guard funnel; completed nested calls replay from
// the Session/Tool Journal, and the approved call executes exactly once.
func (a *Agent) ResumeTurn(ctx context.Context, runID string) (turnResult TurnResult, turnErr error) {
	if ctx == nil {
		return TurnResult{}, fmt.Errorf("run context is nil")
	}
	if err := ValidateRunID(runID); err != nil {
		return TurnResult{}, err
	}
	a.runMu.Lock()
	defer a.runMu.Unlock()

	pending, ok, err := a.opts.Session.PendingApproval(runID)
	if err != nil {
		return TurnResult{}, err
	}
	if !ok {
		return TurnResult{}, fmt.Errorf("run %s has no pending approval", runID)
	}
	a.restoreRunState(runID)
	compositionRevision, revisionErr := CompositionRevision(a.opts.Composition)
	if revisionErr != nil {
		return TurnResult{}, revisionErr
	}
	a.compositionRevision = compositionRevision
	info := RunInfo{
		RunID: runID, SessionID: a.opts.Session.ID(), ProfileID: a.opts.Session.ProfileID(),
		Step: pending.Step, Principal: a.opts.Session.Principal(),
	}
	ctx, runSpan := StartTelemetry(a.opts.Telemetry, ctx, SpanRunSegment, TelemetryAttributes{
		"run.id": runID, "session.id": a.opts.Session.ID(), "profile.id": a.opts.Session.ProfileID(),
		"tenant.id": info.Principal.TenantID, "run.resume": "true",
	})
	runStarted := time.Now()
	defer func() {
		status := telemetryRunStatus(turnResult, turnErr)
		runSpan.End(turnErr, TelemetryAttributes{"run.status": status})
		metricAttrs := TelemetryAttributes{"run.status": status, "run.resume": "true", "profile.id": a.opts.Session.ProfileID()}
		AddTelemetryCounter(a.opts.Telemetry, ctx, MetricRuns, 1, metricAttrs)
		RecordTelemetryHistogram(a.opts.Telemetry, ctx, MetricRunDuration, time.Since(runStarted).Seconds(), "s", metricAttrs)
	}()
	if err := a.append(runID, EvRunResume, RunResumeData{
		ProfileSnapshotID: a.opts.ProfileSnapshotID, CapabilitySnapshotID: a.opts.CapabilitySnapshotID,
		CompositionRevision: func() string { value, _ := CompositionRevision(a.opts.Composition); return value }(),
		AssignmentRevision: func() string {
			value, _ := CompositionMetadataRevision(compositionMetadata(a.opts.Composition))
			return value
		}(),
		Composition: a.opts.Composition,
	}); err != nil {
		return TurnResult{}, err
	}
	return a.resumeApproval(ctx, info, pending)
}

func compositionMetadata(composition *RunCompositionData) map[string]string {
	if composition == nil {
		return nil
	}
	return composition.Metadata
}

func compositionWithMetadata(base *RunCompositionData, metadata map[string]string) (*RunCompositionData, error) {
	if err := ValidateRunCompositionMetadata(metadata); err != nil {
		return nil, err
	}
	out := cloneRunCompositionData(base)
	if out == nil {
		if len(metadata) == 0 {
			return nil, nil
		}
		out = &RunCompositionData{}
	}
	if out.Metadata == nil && len(metadata) > 0 {
		out.Metadata = map[string]string{}
	}
	for key, value := range metadata {
		out.Metadata[key] = value
	}
	if err := validateRunComposition(out); err != nil {
		return nil, err
	}
	return out, nil
}

func cloneRunCompositionData(input *RunCompositionData) *RunCompositionData {
	if input == nil {
		return nil
	}
	out := *input
	profile := input.Profile
	profile.Capabilities = append([]string(nil), input.Profile.Capabilities...)
	profile.Fragments = append([]ResolvedPromptFragment(nil), input.Profile.Fragments...)
	profile.Metadata = cloneRunCompositionMetadata(input.Profile.Metadata)
	out.Profile = profile
	out.Capabilities = make([]SnapshotCapability, len(input.Capabilities))
	for index, capability := range input.Capabilities {
		out.Capabilities[index] = SnapshotCapability{
			Manifest: cloneManifest(capability.Manifest), Source: capability.Source,
			ProviderRevision: capability.ProviderRevision,
		}
	}
	out.EffectivePermissions = input.EffectivePermissions.Clone()
	out.Metadata = cloneRunCompositionMetadata(input.Metadata)
	return &out
}

func telemetryRunStatus(result TurnResult, err error) string {
	if result.Status != "" {
		return string(result.Status)
	}
	if err != nil {
		return "error"
	}
	return "unknown"
}

func (a *Agent) restoreRunState(runID string) {
	a.runID = runID
	a.counts = map[string]int{}
	a.toolCalls = 0
	a.usage = TokenUsage{}
	a.approvalResolutions = map[string]ApprovalResolution{}
	a.resumeCallIDs = map[string]bool{}
	seen := map[string]bool{}
	for _, event := range a.opts.Session.Events() {
		if event.RunID != runID {
			continue
		}
		switch event.Type {
		case EvToolCall:
			var data ToolCallData
			if json.Unmarshal(event.Data, &data) != nil || seen[data.CallID] {
				continue
			}
			seen[data.CallID] = true
			a.toolCalls++
			a.counts[data.Name]++
			a.resumeCallIDs[data.CallID] = true
		case EvToolResult:
			var data ToolResultData
			if json.Unmarshal(event.Data, &data) == nil {
				delete(a.resumeCallIDs, data.CallID)
			}
		}
	}
}

func (a *Agent) resumeApproval(ctx context.Context, info RunInfo, pending ApprovalRequestedData) (TurnResult, error) {
	result, executeErr := a.tools.Execute(ctx, pending.ResumeCall)
	if executeErr != nil {
		if next, ok := IsApprovalPending(executeErr); ok {
			return a.pauseForApproval(info, next, pending.ResumeCall, pending.RemainingCalls, pending.Fast)
		}
		if errors.Is(executeErr, context.Canceled) || errors.Is(executeErr, context.DeadlineExceeded) {
			if ctx.Err() != nil {
				return a.fail(info, "tool_cancelled", executeErr, false)
			}
		}
		result = CapabilityResult{Content: boundedText(executeErr.Error(), DefaultMaxCapabilityOutputBytes), OK: false}
	}
	if err := a.append(info.RunID, EvToolResult, ToolResultData{
		CallID: pending.ResumeCall.ID, Content: result.Content, OK: result.OK, Metadata: result.Metadata,
	}); err != nil {
		return a.fail(info, "event_append_failed", err, false)
	}
	if err := a.appendApprovalResolutions(info.RunID); err != nil {
		return a.fail(info, "event_append_failed", err, false)
	}
	if pending.Fast {
		if err := a.append(info.RunID, EvAssistantMessage, AssistantMessageData{Text: result.Content}); err != nil {
			return a.fail(info, "event_append_failed", err, false)
		}
		return a.complete(info, RunCompleted)
	}
	for index, call := range pending.RemainingCalls {
		if a.toolCalls >= a.opts.MaxToolCalls {
			_ = a.append(info.RunID, EvStepEnd, StepData{Index: info.Step})
			return a.complete(info, RunLimited)
		}
		if err := a.append(info.RunID, EvToolCall, ToolCallData{CallID: call.ID, Name: call.Name, Args: call.Args}); err != nil {
			return a.fail(info, "event_append_failed", err, false)
		}
		callResult, callErr := a.tools.Execute(ctx, call)
		if callErr != nil {
			if next, ok := IsApprovalPending(callErr); ok {
				return a.pauseForApproval(info, next, call, pending.RemainingCalls[index+1:], false)
			}
			if errors.Is(callErr, context.Canceled) || errors.Is(callErr, context.DeadlineExceeded) {
				if ctx.Err() != nil {
					return a.fail(info, "tool_cancelled", callErr, false)
				}
			}
			callResult = CapabilityResult{Content: boundedText(callErr.Error(), DefaultMaxCapabilityOutputBytes), OK: false}
		}
		if err := a.append(info.RunID, EvToolResult, ToolResultData{
			CallID: call.ID, Content: callResult.Content, OK: callResult.OK, Metadata: callResult.Metadata,
		}); err != nil {
			return a.fail(info, "event_append_failed", err, false)
		}
		if err := a.appendApprovalResolutions(info.RunID); err != nil {
			return a.fail(info, "event_append_failed", err, false)
		}
	}
	if err := a.append(info.RunID, EvStepEnd, StepData{Index: info.Step}); err != nil {
		return a.fail(info, "event_append_failed", err, false)
	}
	if a.opts.Hooks != nil {
		safeHookNotify(func() { a.opts.Hooks.OnAfterStep(ctx, info) })
	}
	return a.runModelSteps(ctx, info, info.Step+1)
}

func (a *Agent) append(runID string, eventType SessionEventType, data any) error {
	event, err := a.opts.Session.Append(runID, eventType, data)
	if err != nil {
		return err
	}
	if a.opts.OnEvent != nil {
		if err := safeEventCallback(a.opts.OnEvent, event); err != nil {
			return err
		}
	}
	return nil
}

func (g *guardedToolRuntime) recordFastToolCall(call ToolCall) error {
	if err := g.agent.append(g.agent.runID, EvToolCall, ToolCallData{CallID: call.ID, Name: call.Name, Args: call.Args}); err != nil {
		return errors.Join(errFastToolCallAppend, err)
	}
	return nil
}

// emit forwards an already-appended event (for example a summary event
// appended by the RunSummarizer) to the run's event consumer.
func (a *Agent) emit(event SessionEvent) {
	if a.opts.OnEvent != nil {
		_ = safeEventCallback(a.opts.OnEvent, event)
	}
}

func safeEventCallback(callback func(SessionEvent), event SessionEvent) (err error) {
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("event callback panicked")
		}
	}()
	callback(event)
	return nil
}

func (a *Agent) complete(info RunInfo, status RunStatus) (TurnResult, error) {
	a.appendUsage(info.RunID)
	if err := a.append(info.RunID, EvRunEnd, RunEndData{Status: status}); err != nil {
		return TurnResult{}, err
	}
	if a.opts.Hooks != nil {
		a.notifyRunEnd(info, status)
	}
	return TurnResult{RunID: info.RunID, Status: status, Answer: a.opts.Session.LastAssistantText(info.RunID)}, nil
}

func (a *Agent) fail(info RunInfo, code string, cause error, retryable bool) (TurnResult, error) {
	status := RunFailed
	if errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
		status = RunCancelled
	}
	a.appendUsage(info.RunID)
	_ = a.append(info.RunID, EvRunError, NewRuntimeErrorData(code, cause, retryable))
	_ = a.append(info.RunID, EvRunEnd, RunEndData{Status: status})
	if a.opts.Hooks != nil {
		a.notifyRunEnd(info, status)
	}
	return TurnResult{RunID: info.RunID, Status: status, Answer: a.opts.Session.LastAssistantText(info.RunID)}, cause
}

// notifyRunEnd keeps an observational hook from blocking the terminal run
// result forever. A hook that ignores cancellation may outlive the timeout,
// but it cannot hold the caller open.
func (a *Agent) notifyRunEnd(info RunInfo, status RunStatus) {
	ctx, cancel := context.WithTimeout(context.Background(), terminalHookTimeout)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		safeHookNotify(func() { a.opts.Hooks.OnRunEnd(ctx, info, status) })
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

// appendUsage records this segment's ordinary model usage. Summarizer usage
// contributions and approval checkpoints are independent additive events.
func (a *Agent) appendUsage(runID string) {
	if a.usage.InputTokens == 0 && a.usage.OutputTokens == 0 {
		return
	}
	_ = a.append(runID, EvRunUsage, RunUsageData{
		InputTokens: a.usage.InputTokens, OutputTokens: a.usage.OutputTokens,
	})
}

func (a *Agent) checkpointUsage(runID string) {
	a.appendUsage(runID)
	a.usage = TokenUsage{}
}

func (a *Agent) appendApprovalResolutions(runID string) error {
	if len(a.approvalResolutions) == 0 {
		return nil
	}
	ids := make([]string, 0, len(a.approvalResolutions))
	for id := range a.approvalResolutions {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		resolution := a.approvalResolutions[id]
		resolvedAt := resolution.DecidedAt
		if resolvedAt.IsZero() {
			resolvedAt = time.Now().UTC()
		}
		if err := a.append(runID, EvApprovalResolved, ApprovalResolvedData{
			ApprovalID: id, CallID: approvalResolutionCallID(a.opts.Session, runID, id),
			Decision: resolution.Decision, ResolvedAt: resolvedAt, ResolvedBy: resolution.DecidedBy,
		}); err != nil {
			return err
		}
		delete(a.approvalResolutions, id)
	}
	return nil
}

func approvalResolutionCallID(session *Session, runID, approvalID string) string {
	for _, event := range session.Events() {
		if event.RunID != runID || event.Type != EvApprovalRequested {
			continue
		}
		var data ApprovalRequestedData
		if json.Unmarshal(event.Data, &data) == nil && data.ApprovalID == approvalID {
			return data.ToolCall.ID
		}
	}
	return "unknown"
}

func (a *Agent) pauseForApproval(info RunInfo, pending *ApprovalPendingError, resumeCall ToolCall, remaining []ToolCall, fast bool) (TurnResult, error) {
	if pending == nil {
		return a.fail(info, "approval_failed", fmt.Errorf("approval pause is missing its request"), false)
	}
	if err := a.appendApprovalResolutions(info.RunID); err != nil {
		return a.fail(info, "event_append_failed", err, false)
	}
	if existing, ok, err := a.opts.Session.PendingApproval(info.RunID); err != nil {
		return a.fail(info, "approval_state_invalid", err, false)
	} else if ok {
		if existing.ApprovalID != pending.Resolution.ApprovalID {
			return a.fail(info, "approval_state_conflict", fmt.Errorf("run already waits for approval %s", existing.ApprovalID), false)
		}
		return TurnResult{RunID: info.RunID, Status: RunWaitingApproval}, nil
	}
	a.checkpointUsage(info.RunID)
	if err := a.append(info.RunID, EvApprovalRequested, ApprovalRequestedData{
		ApprovalID: pending.Resolution.ApprovalID, ToolCall: pending.Request.ToolCall,
		ResumeCall: resumeCall, RemainingCalls: remaining, Step: info.Step,
		Fast: fast, ExpiresAt: pending.Resolution.ExpiresAt,
	}); err != nil {
		return a.fail(info, "event_append_failed", err, false)
	}
	return TurnResult{RunID: info.RunID, Status: RunWaitingApproval}, nil
}

func isRetryable(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	type retryableError interface{ Retryable() bool }
	var retryable retryableError
	return errors.As(err, &retryable) && retryable.Retryable()
}

func addTokenUsage(total *TokenUsage, delta TokenUsage) error {
	if total == nil || delta.InputTokens < 0 || delta.OutputTokens < 0 {
		return fmt.Errorf("token usage is invalid")
	}
	if delta.InputTokens > MaxReportedTokensPerRun-total.InputTokens ||
		delta.OutputTokens > MaxReportedTokensPerRun-total.OutputTokens {
		return fmt.Errorf("run token usage exceeds the hard limit")
	}
	total.InputTokens += delta.InputTokens
	total.OutputTokens += delta.OutputTokens
	return nil
}

func containsCall(calls []ToolCall, call ToolCall) bool {
	for _, existing := range calls {
		if existing.ID == call.ID && existing.Name == call.Name {
			return true
		}
	}
	return false
}
