package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
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
	// Hooks are optional run guardrails.
	Hooks RunHooks
	// Approver gates required tool approval; nil denies (fail-closed).
	Approver Approver
	// RateLimiter optionally bounds per-tenant capability calls.
	RateLimiter CallRateLimiter
	// ToolJournal optionally fences provider side effects and replays results.
	ToolJournal ToolInvocationJournal
	// ModelCallGate optionally authorizes model calls.
	ModelCallGate ModelCallGate
	// Telemetry optionally records bounded runtime observations.
	Telemetry Telemetry
	// Compactor optionally reduces history before a model request.
	Compactor ContextCompactor
	// Summarizer optionally archives over-budget history before compaction.
	Summarizer RunSummarizer
	// ContextAssembler optionally selects bounded model context.
	ContextAssembler ModelContextAssembler
	// StreamChunks persists UI chunks excluded from model history.
	StreamChunks bool
	// DiscloseTools enables optional tool-library discovery.
	DiscloseTools bool
}

// TurnInput identifies one append-only run in an existing session.
type TurnInput struct {
	RunID                string
	Text                 string
	MaxToolCallsOverride int
	RunCapabilities      []CapabilityBinding
	CapabilityFilter     CapabilityFilter
	CompositionMetadata  map[string]string
}

// ResumeInput identifies an existing run and optional composition metadata for
// one runtime resumption operation. ResumeTurn requires approval/requested;
// ContinueTurn instead requires an already durable post-result tool prefix.
type ResumeInput struct {
	RunID               string
	CompositionMetadata map[string]string
}

// TurnResult is the current state returned to an interface adapter.
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
	agent.tools = &guardedToolRuntime{agent: agent, inner: opts.Tools}
	return agent, nil
}

// RunTurn appends one serialized run and may return RunWaitingApproval.
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
		if err := safeCallError("run hook OnRunStart panicked", func() error { return a.opts.Hooks.OnRunStart(ctx, info) }); err != nil {
			return a.fail(info, "input_rejected", err, false)
		}
	}

	if a.opts.Fast != nil {
		dispatch, err := safeCallValueError("fast router panicked", func() (FastDispatch, error) {
			return a.opts.Fast.Dispatch(ctx, input.Text, a.tools)
		})
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
	if startStep >= a.opts.MaxSteps {
		return a.complete(info, RunLimited)
	}
	step := startStep
	info.Step = step
	if usageLimitReached(session.runUsageTotal(info.RunID)) {
		return a.complete(info, RunLimited)
	}
	if a.opts.Hooks != nil {
		if err := safeCallError("run hook OnBeforeStep panicked", func() error { return a.opts.Hooks.OnBeforeStep(ctx, info) }); err != nil {
			return a.fail(info, "step_rejected", err, false)
		}
	}
	if err := a.append(info.RunID, EvStepStart, StepData{Index: step}); err != nil {
		return a.fail(info, "event_append_failed", err, false)
	}
	stepStartSeq := session.Version() - 1

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
			messages, err = safeCallValueError("run summarizer panicked", func() ([]ChatMessage, error) {
				return a.opts.Summarizer.EnsureSummarized(ctx, session, info.RunID, a.emit, messages)
			})
			if err != nil {
				_ = a.append(info.RunID, EvStepError, NewRuntimeErrorData("summarization_failed", err, isRetryable(err)))
				return a.fail(info, "summarization_failed", err, isRetryable(err))
			}
			if usageLimitReached(session.runUsageTotal(info.RunID)) {
				return a.completeStep(ctx, info, RunLimited)
			}
		}
		if a.opts.Compactor != nil {
			messages, err = safeCallValueError("context compactor panicked", func() ([]ChatMessage, error) {
				return a.opts.Compactor.Compact(messages), nil
			})
			if err != nil {
				_ = a.append(info.RunID, EvStepError, NewRuntimeErrorData("compaction_failed", err, false))
				return a.fail(info, "compaction_failed", err, false)
			}
		}
	}

	stream, failureCode, err := a.callModel(ctx, info, step, messages)
	if err != nil {
		if stream.usageReported {
			if usageErr := a.appendModelUsage(info.RunID, stepStartSeq, stream.Usage); usageErr != nil {
				err = errors.Join(err, usageErr)
			}
		}
		if failureCode == "model_gate_rejected" {
			return a.fail(info, failureCode, err, false)
		}
		_ = a.append(info.RunID, EvStepError, NewRuntimeErrorData(failureCode, err, isRetryable(err)))
		return a.fail(info, failureCode, err, isRetryable(err))
	}
	text, calls := stream.Text, stream.ToolCalls

	if err := a.appendAssistantUsage(info.RunID, stepStartSeq, AssistantMessageData{
		Text: text, ToolCall: firstCall(calls), ToolCalls: calls,
	}, stream.Usage); err != nil {
		return a.fail(info, "event_append_failed", err, false)
	}
	if usageLimitReached(session.runUsageTotal(info.RunID)) {
		return a.completeStep(ctx, info, RunLimited)
	}
	if len(calls) == 0 {
		if err := a.append(info.RunID, EvStepEnd, StepData{Index: step}); err != nil {
			return a.fail(info, "event_append_failed", err, false)
		}
		if a.opts.Hooks != nil {
			safeCallNotify(func() { a.opts.Hooks.OnAfterStep(ctx, info) })
		}
		return a.complete(info, RunCompleted)
	}

	return a.continueToolCalls(ctx, info, calls, true, true)
}

// ResumeTurn continues one run suspended at an approval checkpoint.
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

func (a *Agent) continueTurn(ctx context.Context, runID string) (TurnResult, error) {
	a.runMu.Lock()
	defer a.runMu.Unlock()
	if a.opts.Fast != nil {
		return TurnResult{}, fmt.Errorf("post-result continuation does not support fast routing")
	}
	step, calls, completed, err := postResultContinuation(a.opts.Session.Events(), runID)
	if err != nil {
		return TurnResult{}, err
	}
	for _, call := range calls {
		if !a.tools.Authorized(call.Name) {
			return TurnResult{}, fmt.Errorf("current capabilities do not authorize %q", call.Name)
		}
	}
	a.restoreRunState(runID)
	if a.compositionRevision, err = CompositionRevision(a.opts.Composition); err != nil {
		return TurnResult{}, err
	}
	return a.continueToolCalls(ctx, RunInfo{RunID: runID, SessionID: a.opts.Session.ID(), ProfileID: a.opts.Session.ProfileID(), Step: step, Principal: a.opts.Session.Principal()}, calls[completed:], false, true)
}

func compositionMetadata(composition *RunCompositionData) map[string]string {
	if composition != nil {
		return composition.Metadata
	}
	return nil
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
	if result, stopped, err := a.executeLoggedToolCall(ctx, info, pending.ResumeCall, pending.RemainingCalls, pending.Fast); stopped {
		return result, err
	}
	if pending.Fast {
		result, ok, err := a.opts.Session.ToolResult(info.RunID, pending.ResumeCall.ID)
		if err != nil {
			return TurnResult{}, fmt.Errorf("read approved fast tool result: %w", err)
		}
		if !ok {
			return TurnResult{}, fmt.Errorf("approved fast tool result is unavailable")
		}
		if err := a.append(info.RunID, EvAssistantMessage, AssistantMessageData{Text: result.Content}); err != nil {
			return a.fail(info, "event_append_failed", err, false)
		}
		return a.complete(info, RunCompleted)
	}
	return a.continueToolCalls(ctx, info, pending.RemainingCalls, false, false)
}

func (a *Agent) continueToolCalls(ctx context.Context, info RunInfo, calls []ToolCall, announceLimit, notifyLimit bool) (TurnResult, error) {
	for index, call := range calls {
		if a.toolCalls >= a.opts.MaxToolCalls {
			if announceLimit {
				if err := a.append(info.RunID, EvAssistantMessage, AssistantMessageData{Text: "Tool call budget reached; the run stopped."}); err != nil {
					return a.fail(info, "event_append_failed", err, false)
				}
			}
			_ = a.append(info.RunID, EvStepEnd, StepData{Index: info.Step})
			if notifyLimit && a.opts.Hooks != nil {
				safeCallNotify(func() { a.opts.Hooks.OnAfterStep(ctx, info) })
			}
			return a.complete(info, RunLimited)
		}
		if err := a.append(info.RunID, EvToolCall, ToolCallData{CallID: call.ID, Name: call.Name, Args: call.Args}); err != nil {
			return a.fail(info, "event_append_failed", err, false)
		}
		if result, stopped, err := a.executeLoggedToolCall(ctx, info, call, calls[index+1:], false); stopped {
			return result, err
		}
	}
	if err := a.append(info.RunID, EvStepEnd, StepData{Index: info.Step}); err != nil {
		return a.fail(info, "event_append_failed", err, false)
	}
	if a.opts.Hooks != nil {
		safeCallNotify(func() { a.opts.Hooks.OnAfterStep(ctx, info) })
	}
	return a.runModelSteps(ctx, info, info.Step+1)
}

func (a *Agent) executeLoggedToolCall(ctx context.Context, info RunInfo, call ToolCall, remaining []ToolCall, fast bool) (TurnResult, bool, error) {
	result, executeErr := a.tools.Execute(ctx, call)
	if executeErr != nil {
		if pending, ok := IsApprovalPending(executeErr); ok {
			result, err := a.pauseForApproval(info, pending, call, remaining, fast)
			return result, true, err
		}
		if (errors.Is(executeErr, context.Canceled) || errors.Is(executeErr, context.DeadlineExceeded)) && ctx.Err() != nil {
			result, err := a.fail(info, "tool_cancelled", executeErr, false)
			return result, true, err
		}
		result = CapabilityResult{Content: boundedText(executeErr.Error(), DefaultMaxCapabilityOutputBytes), OK: false}
	}
	if err := a.append(info.RunID, EvToolResult, ToolResultData{CallID: call.ID, Content: result.Content, OK: result.OK, Metadata: result.Metadata}); err != nil {
		result, err := a.fail(info, "event_append_failed", err, false)
		return result, true, err
	}
	if err := a.appendApprovalResolutions(info.RunID); err != nil {
		result, err := a.fail(info, "event_append_failed", err, false)
		return result, true, err
	}
	return TurnResult{}, false, nil
}

func postResultContinuation(events []SessionEvent, runID string) (int, []ToolCall, int, error) {
	open := map[string]int{}
	for index, event := range events {
		if event.Type == EvRunStart {
			open[event.RunID] = index
		}
		if event.Type == EvRunEnd {
			delete(open, event.RunID)
		}
	}
	start, ok := open[runID]
	if !ok {
		return 0, nil, 0, fmt.Errorf("run %s is not open", runID)
	}
	for id, index := range open {
		if index > start || index == start && id != runID {
			return 0, nil, 0, fmt.Errorf("run %s is not the latest open run", runID)
		}
	}
	step, last, stepSeq, state, completed := -1, -1, int64(-1), 0, 0
	var calls []ToolCall
	seenCalls, seenResults, userSeen := map[string]bool{}, map[string]bool{}, false
	for index := start; index < len(events); index++ {
		event := events[index]
		if event.RunID != runID {
			return 0, nil, 0, fmt.Errorf("run %s has interleaved session events", runID)
		}
		if err := validateSessionEvent(event); err != nil {
			return 0, nil, 0, fmt.Errorf("run %s has malformed continuation event: %w", runID, err)
		}
		switch event.Type {
		case EvRunStart:
			if index != start {
				return 0, nil, 0, fmt.Errorf("run %s has duplicate starts", runID)
			}
		case EvUserMessage:
			if state != 0 || userSeen {
				return 0, nil, 0, fmt.Errorf("run %s has an interleaved user message", runID)
			}
			userSeen = true
		case EvStepStart:
			var data StepData
			if json.Unmarshal(event.Data, &data) != nil || state != 0 || !userSeen || data.Index != last+1 {
				return 0, nil, 0, fmt.Errorf("run %s has an invalid step sequence", runID)
			}
			step, last, stepSeq, state = data.Index, data.Index, event.Seq, 1
		case EvAssistantChunk, EvContextSummary:
			if state != 1 {
				return 0, nil, 0, fmt.Errorf("run %s has an interleaved model artifact", runID)
			}
		case EvRunUsage:
			var data RunUsageData
			if json.Unmarshal(event.Data, &data) != nil || validateRunUsageData("", data) != nil {
				return 0, nil, 0, fmt.Errorf("run %s has invalid usage", runID)
			}
			if state == 1 && len(data.InvocationID) > 8 && data.InvocationID[:8] == "summary:" {
				continue
			}
			if state != 2 || data.InvocationID != fmt.Sprintf("model:%d", stepSeq) {
				return 0, nil, 0, fmt.Errorf("run %s lacks matching model usage", runID)
			}
			state = 3
		case EvAssistantMessage:
			var data AssistantMessageData
			if json.Unmarshal(event.Data, &data) != nil || state != 1 {
				return 0, nil, 0, fmt.Errorf("run %s has an invalid assistant message", runID)
			}
			calls = cloneToolCalls(data.ToolCalls)
			if data.ToolCall != nil {
				if len(calls) == 0 {
					calls = []ToolCall{cloneToolCall(*data.ToolCall)}
				} else if !reflect.DeepEqual(calls[0], *data.ToolCall) {
					return 0, nil, 0, fmt.Errorf("run %s has conflicting legacy tool calls", runID)
				}
			}
			if len(calls) == 0 {
				return 0, nil, 0, fmt.Errorf("run %s has no assistant tool calls", runID)
			}
			for _, call := range calls {
				if validateToolCall(call) != nil || seenCalls[call.ID] {
					return 0, nil, 0, fmt.Errorf("run %s has duplicate or invalid tool calls", runID)
				}
				seenCalls[call.ID] = true
			}
			state = 2
		case EvToolCall:
			var data ToolCallData
			if json.Unmarshal(event.Data, &data) != nil || (state != 3 && state != 5) || completed >= len(calls) || calls[completed].ID != data.CallID || calls[completed].Name != data.Name || !reflect.DeepEqual(calls[completed].Args, data.Args) {
				return 0, nil, 0, fmt.Errorf("run %s has an invalid tool call sequence", runID)
			}
			state = 4
		case EvToolResult:
			var data ToolResultData
			if json.Unmarshal(event.Data, &data) != nil || state != 4 || data.CallID != calls[completed].ID || seenResults[data.CallID] {
				return 0, nil, 0, fmt.Errorf("run %s has an invalid tool result sequence", runID)
			}
			seenResults[data.CallID], completed, state = true, completed+1, 5
		case EvStepEnd:
			if state != 5 || completed != len(calls) {
				return 0, nil, 0, fmt.Errorf("run %s has an unbalanced step", runID)
			}
			calls, completed, state = nil, 0, 0
		default:
			return 0, nil, 0, fmt.Errorf("run %s has unsupported continuation events", runID)
		}
	}
	if state == 1 {
		return 0, nil, 0, fmt.Errorf("model_outcome_unknown: run %s has no assistant outcome", runID)
	}
	if state != 5 || completed == 0 {
		return 0, nil, 0, fmt.Errorf("run %s is not a post-result continuation", runID)
	}
	return step, calls, completed, nil
}

func (a *Agent) append(runID string, eventType SessionEventType, data any) error {
	return a.appendBatch([]sessionAppendEntry{{runID: runID, kind: eventType, data: data}})
}

func (a *Agent) appendBatch(entries []sessionAppendEntry) error {
	events, err := a.opts.Session.appendBatch(entries)
	if err != nil {
		return err
	}
	var first error
	if a.opts.OnEvent != nil {
		for _, event := range events {
			if err := safeEventCallback(a.opts.OnEvent, event); err != nil && first == nil {
				first = err
			}
		}
	}
	return first
}

func (g *guardedToolRuntime) recordFastToolCall(call ToolCall) error {
	if err := g.agent.append(g.agent.runID, EvToolCall, ToolCallData{CallID: call.ID, Name: call.Name, Args: call.Args}); err != nil {
		return errors.Join(errFastToolCallAppend, err)
	}
	return nil
}

// emit forwards an already-appended event to the run consumer.
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
	_ = a.append(info.RunID, EvRunError, NewRuntimeErrorData(code, cause, retryable))
	_ = a.append(info.RunID, EvRunEnd, RunEndData{Status: status})
	if a.opts.Hooks != nil {
		a.notifyRunEnd(info, status)
	}
	return TurnResult{RunID: info.RunID, Status: status, Answer: a.opts.Session.LastAssistantText(info.RunID)}, cause
}

// notifyRunEnd bounds observational hooks at the terminal boundary.
func (a *Agent) notifyRunEnd(info RunInfo, status RunStatus) {
	ctx, cancel := context.WithTimeout(context.Background(), terminalHookTimeout)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		safeCallNotify(func() { a.opts.Hooks.OnRunEnd(ctx, info, status) })
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

func (a *Agent) appendModelUsage(runID string, stepStartSeq int64, usage TokenUsage) error {
	return a.append(runID, EvRunUsage, RunUsageData{InputTokens: usage.InputTokens, OutputTokens: usage.OutputTokens, InvocationID: fmt.Sprintf("model:%d", stepStartSeq)})
}

func (a *Agent) appendAssistantUsage(runID string, stepStartSeq int64, assistant AssistantMessageData, usage TokenUsage) error {
	if err := a.appendBatch([]sessionAppendEntry{{runID: runID, kind: EvAssistantMessage, data: assistant}, {runID: runID, kind: EvRunUsage, data: RunUsageData{InputTokens: usage.InputTokens, OutputTokens: usage.OutputTokens, InvocationID: fmt.Sprintf("model:%d", stepStartSeq)}}}); err != nil {
		return err
	}
	return nil
}

func usageLimitReached(usage TokenUsage) bool {
	return usage.InputTokens >= MaxReportedTokensPerRun || usage.OutputTokens >= MaxReportedTokensPerRun
}

func (a *Agent) completeStep(ctx context.Context, info RunInfo, status RunStatus) (TurnResult, error) {
	if err := a.append(info.RunID, EvStepEnd, StepData{Index: info.Step}); err != nil {
		return a.fail(info, "event_append_failed", err, false)
	}
	if a.opts.Hooks != nil {
		safeCallNotify(func() { a.opts.Hooks.OnAfterStep(ctx, info) })
	}
	return a.complete(info, status)
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

func containsCall(calls []ToolCall, call ToolCall) bool {
	for _, existing := range calls {
		if existing.ID == call.ID && existing.Name == call.Name {
			return true
		}
	}
	return false
}
