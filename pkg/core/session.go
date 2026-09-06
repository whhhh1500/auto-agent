package core

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

var runIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

const MaxSessionEventDataBytes = 16 << 20

const MaxSessionEvents = 16384

type eventType = SessionEventType

const MaxRuntimeErrorMessageBytes = 16 << 10

const (
	MaxRunCompositionMetadataItems      = 32
	MaxRunCompositionMetadataKeyBytes   = 128
	MaxRunCompositionMetadataValueBytes = 1024
	MaxRunCompositionMetadataBytes      = 16 << 10
)

// SessionEventType identifies one durable runtime fact.
type SessionEventType string

const (
	EvRunStart          SessionEventType = "run/start"
	EvRunResume         SessionEventType = "run/resume"
	EvRunEnd            SessionEventType = "run/end"
	EvRunError          SessionEventType = "run/error"
	EvRunUsage          SessionEventType = "run/usage"
	EvStepStart         SessionEventType = "step/start"
	EvStepEnd           SessionEventType = "step/end"
	EvStepError         SessionEventType = "step/error"
	EvUserMessage       SessionEventType = "user/message"
	EvAssistantMessage  SessionEventType = "assistant/message"
	EvAssistantChunk    SessionEventType = "assistant/chunk"
	EvToolCall          SessionEventType = "tool/call"
	EvToolResult        SessionEventType = "tool/result"
	EvApprovalRequested SessionEventType = "approval/requested"
	EvApprovalResolved  SessionEventType = "approval/resolved"
	EvContextSummary    SessionEventType = "context/summary"
)

// RunStatus is the current durable state reported by an Agent turn. Most
// values are terminal; RunWaitingApproval is a resumable suspension.
type RunStatus string

const (
	RunCompleted       RunStatus = "completed"
	RunLimited         RunStatus = "limited"
	RunFailed          RunStatus = "failed"
	RunCancelled       RunStatus = "cancelled"
	RunWaitingApproval RunStatus = "waiting_approval"
)

// RunCompositionData is the durable, provider-neutral recipe selected for one
// run. It makes the run auditable after registries change: the resolved profile,
// filtered capability declarations, effective grants, model selection and
// limits are recorded without embedding provider objects or credential values.
// External provider state still requires its own versioned artifact for exact
// behavioral replay.
type RunCompositionData struct {
	Profile              AgentProfileSnapshot `json:"profile"`
	Capabilities         []SnapshotCapability `json:"capabilities"`
	EffectivePermissions PermissionSet        `json:"effective_permissions,omitempty"`
	Model                ModelSelection       `json:"model"`
	ResolvedProvider     string               `json:"resolved_provider,omitempty"`
	ModelRevision        string               `json:"model_revision,omitempty"`
	MaxSteps             int                  `json:"max_steps"`
	MaxToolCalls         int                  `json:"max_tool_calls"`
	Metadata             map[string]string    `json:"metadata,omitempty"`
}

type RunStartData struct {
	ProfileSnapshotID    string              `json:"profile_snapshot_id,omitempty"`
	CapabilitySnapshotID string              `json:"capability_snapshot_id,omitempty"`
	CompositionRevision  string              `json:"composition_revision,omitempty"`
	AssignmentRevision   string              `json:"assignment_revision,omitempty"`
	Composition          *RunCompositionData `json:"composition,omitempty"`
}

// RunResumeData records the freshly resolved recipe for one approval-resumed
// execution segment. A Run has one run/start and may have multiple resumes.
type RunResumeData struct {
	ProfileSnapshotID    string              `json:"profile_snapshot_id,omitempty"`
	CapabilitySnapshotID string              `json:"capability_snapshot_id,omitempty"`
	CompositionRevision  string              `json:"composition_revision,omitempty"`
	AssignmentRevision   string              `json:"assignment_revision,omitempty"`
	Composition          *RunCompositionData `json:"composition,omitempty"`
}

// RunCompositionEvidence is the stable cross-system link for one execution
// segment. It contains no arbitrary metadata payload, only digests.
type RunCompositionEvidence struct {
	CompositionRevision string `json:"composition_revision,omitempty"`
	AssignmentRevision  string `json:"assignment_revision,omitempty"`
}

type RunEndData struct {
	Status RunStatus `json:"status"`
}

type RuntimeErrorData struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

func NewRuntimeErrorData(code string, cause error, retryable bool) RuntimeErrorData {
	message := "unknown runtime error"
	if cause != nil {
		message = boundedText(cause.Error(), MaxRuntimeErrorMessageBytes)
	}
	return RuntimeErrorData{Code: code, Message: message, Retryable: retryable}
}

func boundedText(value string, limit int) string {
	if limit <= 0 || len(value) <= limit {
		return value
	}
	return value[:limit] + "...[truncated]"
}

type StepData struct {
	Index int `json:"index"`
}

type UserMessageData struct {
	Text string `json:"text"`
}

// AssistantMessageData records one complete model message. ToolCalls carries
// every model-requested call; ToolCall mirrors the first call so older
// consumers keep reading a stable field.
type AssistantMessageData struct {
	Text      string     `json:"text"`
	ToolCall  *ToolCall  `json:"tool_call,omitempty"`
	ToolCalls []ToolCall `json:"tool_calls,omitempty"`
}

// AssistantChunkData records one streamed text delta. Chunk events preserve UI
// and replay fidelity; model history projection uses only complete messages.
type AssistantChunkData struct {
	Text string `json:"text"`
}

// RunUsageData records aggregated token usage for one run.
type RunUsageData struct {
	InputTokens  int64 `json:"input_tokens"`
	OutputTokens int64 `json:"output_tokens"`
}

// ContextSummaryData archives a seq range of the model-visible history into
// one durable summary. It is a surface replacement, not a deletion: the
// covered events stay in the log and the projection shadows them.
type ContextSummaryData struct {
	Op      string `json:"op"`    // always "replace"
	Start   int64  `json:"start"` // first shadowed event seq (inclusive)
	End     int64  `json:"end"`   // last shadowed event seq (inclusive)
	Summary string `json:"summary"`
}

type ToolCallData struct {
	CallID string         `json:"call_id"`
	Name   string         `json:"name"`
	Args   map[string]any `json:"args,omitempty"`
}

type ToolResultData struct {
	CallID   string         `json:"call_id"`
	Content  string         `json:"content"`
	OK       bool           `json:"ok"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

// ApprovalRequestedData is a durable continuation checkpoint. ToolCall is the
// exact call requiring approval; ResumeCall is the outer guarded call to rerun
// after a decision (the same call for non-composite tools). RemainingCalls are
// model calls from the same step that have not started yet.
type ApprovalRequestedData struct {
	ApprovalID     string     `json:"approval_id"`
	ToolCall       ToolCall   `json:"tool_call"`
	ResumeCall     ToolCall   `json:"resume_call"`
	RemainingCalls []ToolCall `json:"remaining_calls,omitempty"`
	Step           int        `json:"step"`
	Fast           bool       `json:"fast,omitempty"`
	ExpiresAt      time.Time  `json:"expires_at,omitempty"`
}

type ApprovalResolvedData struct {
	ApprovalID string           `json:"approval_id"`
	CallID     string           `json:"call_id"`
	Decision   ApprovalDecision `json:"decision"`
	ResolvedAt time.Time        `json:"resolved_at"`
	ResolvedBy string           `json:"resolved_by,omitempty"`
}

// SessionEvent is a JSON-stable append-only fact.
type SessionEvent struct {
	Seq   int64            `json:"seq"`
	Time  time.Time        `json:"time"`
	RunID string           `json:"run_id"`
	Type  SessionEventType `json:"type"`
	Data  json.RawMessage  `json:"data,omitempty"`
}

// Session contains durable metadata plus an ordered event log.
type Session struct {
	mu        sync.RWMutex
	id        string
	profileID string
	principal Principal
	scope     ScopePath
	metadata  map[string]string
	events    []SessionEvent
	runStarts map[string]bool
	runIndex  map[string]*runIndexEntry
	maxEvents int
	// summaryEvents is maintained as append-only event state so a cached
	// projection does not need to rescan the entire log just to establish that
	// no summary invalidated it.
	summaryEvents int

	// Projection cache: valid while the event count and summary count are
	// unchanged. Messages are read-only after projection.
	projCount     int
	projSummaries int
	projCache     []ChatMessage
	projValid     bool
}

// runIndexEntry contains only small identifiers and event positions; payloads
// remain in the canonical event log and are decoded only when requested.
type runIndexEntry struct {
	started     bool
	status      RunStatus
	ended       bool
	pending     map[string]int64
	toolCalls   map[string]int64
	toolResults map[string]int64
}

// SessionOptions contains immutable session ownership and composition metadata.
type SessionOptions struct {
	ID        string
	ProfileID string
	Principal Principal
	Scope     ScopePath
	Metadata  map[string]string
}

// NewSession creates an empty session owned by one principal.
func NewSession(options SessionOptions) (*Session, error) {
	if err := ValidateSessionID(options.ID); err != nil {
		return nil, err
	}
	if err := ValidateProfileID(options.ProfileID); err != nil {
		return nil, fmt.Errorf("session %q: %w", options.ID, err)
	}
	if options.Scope.Depth() == 0 {
		return nil, fmt.Errorf("session %q has an empty scope", options.ID)
	}
	segments := options.Scope.Segments()
	last := segments[len(segments)-1]
	if last.Kind != ScopeSession || last.ID != options.ID {
		return nil, fmt.Errorf("session %q scope must end with session:%s", options.ID, options.ID)
	}
	if strings.TrimSpace(options.Principal.SubjectID) == "" || strings.TrimSpace(options.Principal.TenantID) == "" {
		return nil, fmt.Errorf("session %q principal identity is incomplete", options.ID)
	}
	if !options.Principal.Scope.IsAncestorOf(options.Scope) {
		return nil, fmt.Errorf("principal scope %q does not own session scope %q", options.Principal.Scope, options.Scope)
	}
	metadata := map[string]string{}
	for key, value := range options.Metadata {
		metadata[key] = value
	}
	return &Session{
		id: options.ID, profileID: options.ProfileID, principal: clonePrincipal(options.Principal),
		scope: options.Scope, metadata: metadata, runStarts: map[string]bool{}, runIndex: map[string]*runIndexEntry{},
		projValid: true, projCache: []ChatMessage{},
	}, nil
}

// RestoreSession validates and restores a stored event sequence.
func RestoreSession(options SessionOptions, events []SessionEvent) (*Session, error) {
	session, err := NewSession(options)
	if err != nil {
		return nil, err
	}
	if len(events) > session.eventCap() {
		return nil, fmt.Errorf("session %q exceeds maximum of %d events", options.ID, session.eventCap())
	}
	// Restored events bypass Append; the empty-session cache does not cover them.
	session.projValid = len(events) == 0
	for index, event := range events {
		if event.Seq != int64(index) {
			return nil, fmt.Errorf("session %q event sequence is discontinuous at %d", options.ID, index)
		}
		if err := validateSessionEvent(event); err != nil {
			return nil, fmt.Errorf("session %q event %d: %w", options.ID, index, err)
		}
		copyOf := event
		copyOf.Data = append(json.RawMessage(nil), event.Data...)
		session.events = append(session.events, copyOf)
		session.indexEvent(copyOf, nil)
		if event.Type == EvContextSummary {
			session.summaryEvents++
		}
		if event.Type == EvRunStart {
			if session.runStarts[event.RunID] {
				return nil, fmt.Errorf("session %q has duplicate run start %q", options.ID, event.RunID)
			}
			session.runStarts[event.RunID] = true
		} else if event.Type == EvRunResume && !session.runStarts[event.RunID] {
			return nil, fmt.Errorf("session %q resumes run %q before its start", options.ID, event.RunID)
		}
	}
	return session, nil
}

func (s *Session) ID() string { return s.id }

func (s *Session) ProfileID() string { return s.profileID }

func (s *Session) Principal() Principal { return clonePrincipal(s.principal) }

func (s *Session) Scope() ScopePath { return s.scope }

func (s *Session) Metadata() map[string]string {
	out := map[string]string{}
	for key, value := range s.metadata {
		out[key] = value
	}
	return out
}

// Version is the next event sequence and the optimistic persistence version.
func (s *Session) Version() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return int64(len(s.events))
}

// Events returns a deep copy suitable for persistence or transport.
func (s *Session) Events() []SessionEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]SessionEvent, len(s.events))
	for i, event := range s.events {
		out[i] = event
		out[i].Data = append(json.RawMessage(nil), event.Data...)
	}
	return out
}

// Append serializes and commits one event after the operation succeeds.
func (s *Session) Append(runID string, eventType SessionEventType, data any) (SessionEvent, error) {
	if err := ValidateRunID(runID); err != nil {
		return SessionEvent{}, err
	}
	var raw json.RawMessage
	if data != nil {
		encoded, err := json.Marshal(data)
		if err != nil {
			return SessionEvent{}, fmt.Errorf("encode %s event: %w", eventType, err)
		}
		if len(encoded) > MaxSessionEventDataBytes {
			return SessionEvent{}, fmt.Errorf("encode %s event: payload exceeds %d bytes", eventType, MaxSessionEventDataBytes)
		}
		raw = encoded
	}
	if typed, err := validateSessionEventValue(eventType, data); typed {
		if err != nil {
			return SessionEvent{}, err
		}
	} else {
		if err := validateSessionEvent(SessionEvent{RunID: runID, Type: eventType, Data: raw}); err != nil {
			return SessionEvent{}, err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if eventType == EvRunStart {
		if s.runStarts == nil {
			s.runStarts = map[string]bool{}
		}
		if s.runStarts[runID] {
			return SessionEvent{}, fmt.Errorf("run id %q already exists in session %q", runID, s.id)
		}
		s.runStarts[runID] = true
	} else if eventType == EvRunResume && !s.runStarts[runID] {
		return SessionEvent{}, fmt.Errorf("run id %q has not started in session %q", runID, s.id)
	}
	event := SessionEvent{
		Seq: int64(len(s.events)), Time: time.Now().UTC(), RunID: runID,
		Type: eventType, Data: raw,
	}
	if len(s.events) >= s.eventCap() {
		if eventType == EvRunStart {
			delete(s.runStarts, runID)
		}
		return SessionEvent{}, fmt.Errorf("session %q exceeds maximum of %d events", s.id, s.eventCap())
	}
	s.events = append(s.events, event)
	s.indexEvent(event, data)
	if event.Type == EvContextSummary {
		s.summaryEvents++
		s.projValid = false
		s.projCache = nil
		s.projCount = 0
	} else if s.projValid {
		if message, ok, applicable := projectCommittedData(event.Type, data, event.Seq); ok {
			s.projCache = append(s.projCache, message)
		} else if applicable {
			s.projValid = false
			s.projCache = nil
			s.projCount = 0
		}
		s.projCount = len(s.events)
	}
	return event, nil
}

func (s *Session) indexEvent(event SessionEvent, typed any) {
	entry := s.runIndex[event.RunID]
	if entry == nil {
		entry = &runIndexEntry{}
		s.runIndex[event.RunID] = entry
	}
	switch event.Type {
	case EvRunStart:
		entry.started = true
		entry.status = ""
	case EvRunEnd:
		if entry.ended {
			break
		}
		if data, ok := eventData[RunEndData](typed, event.Data); ok {
			entry.status, entry.ended = data.Status, true
		}
	case EvApprovalRequested:
		if data, ok := eventData[ApprovalRequestedData](typed, event.Data); ok {
			if entry.pending == nil {
				entry.pending = map[string]int64{}
			}
			entry.pending[data.ApprovalID] = event.Seq
		}
	case EvApprovalResolved:
		if data, ok := eventData[ApprovalResolvedData](typed, event.Data); ok {
			if entry.pending != nil {
				delete(entry.pending, data.ApprovalID)
			}
		}
	case EvToolCall:
		if data, ok := eventData[ToolCallData](typed, event.Data); ok {
			if entry.toolCalls == nil {
				entry.toolCalls = map[string]int64{}
			}
			entry.toolCalls[data.CallID] = event.Seq
		}
	case EvToolResult:
		if data, ok := eventData[ToolResultData](typed, event.Data); ok {
			if entry.toolResults == nil {
				entry.toolResults = map[string]int64{}
			}
			if _, exists := entry.toolResults[data.CallID]; !exists {
				entry.toolResults[data.CallID] = event.Seq
			}
		}
	}
}

func (s *Session) eventCap() int {
	if s != nil && s.maxEvents > 0 {
		return s.maxEvents
	}
	return MaxSessionEvents
}

// canonicalEventValue recognizes runtime value/pointer forms. RestoreSession
// always validates stored bytes through the strict wire path.
func canonicalEventValue[T any](value any) (T, bool) {
	if typed, ok := value.(T); ok {
		return typed, true
	}
	if typed, ok := value.(*T); ok && typed != nil {
		return *typed, true
	}
	var zero T
	return zero, false
}

func eventData[T any](typed any, raw json.RawMessage) (T, bool) {
	if data, ok := canonicalEventValue[T](typed); ok {
		return data, true
	}
	var data T
	if json.Unmarshal(raw, &data) != nil {
		return data, false
	}
	return data, true
}

type sessionEventValidator interface {
	target() any
	validate(SessionEventType, any) (bool, error)
}

type sessionEventValidatorFunc[T any] func(SessionEventType, T) error

func (sessionEventValidatorFunc[T]) target() any { return new(T) }
func (validator sessionEventValidatorFunc[T]) validate(eventType SessionEventType, value any) (bool, error) {
	if data, ok := canonicalEventValue[T](value); ok {
		return true, validator(eventType, data)
	}
	return false, nil
}

func validateSessionEventValue(eventType SessionEventType, value any) (bool, error) {
	validator, ok := sessionEventValidators[eventType]
	if !ok {
		return false, nil
	}
	return validator.validate(eventType, value)
}

func validateNoopEventData[T any](SessionEventType, T) error { return nil }
func validateRunStartData(_ eventType, data RunStartData) error {
	return validateRunCompositionEvidence(data.Composition, data.CompositionRevision, data.AssignmentRevision)
}
func validateRunResumeData(_ eventType, data RunResumeData) error {
	return validateRunCompositionEvidence(data.Composition, data.CompositionRevision, data.AssignmentRevision)
}

func invalidEvent(condition bool, format string, args ...any) error {
	if condition {
		return fmt.Errorf(format, args...)
	}
	return nil
}
func validRunStatus(status RunStatus) bool {
	return status == RunCompleted || status == RunLimited || status == RunFailed || status == RunCancelled
}
func validateRunEndData(_ eventType, data RunEndData) error {
	return invalidEvent(!validRunStatus(data.Status), "run/end has invalid status %q", data.Status)
}
func validateRuntimeErrorData(eventType eventType, data RuntimeErrorData) error {
	return invalidEvent(strings.TrimSpace(data.Code) == "" || strings.TrimSpace(data.Message) == "", "%s requires code and message", eventType)
}
func validateRunUsageData(_ eventType, data RunUsageData) error {
	return invalidEvent(data.InputTokens < 0 || data.OutputTokens < 0, "run/usage contains negative token counts")
}
func validateStepData(eventType eventType, data StepData) error {
	return invalidEvent(data.Index < -1, "%s has invalid step index %d", eventType, data.Index)
}
func validateAssistantChunkData(_ eventType, data AssistantChunkData) error {
	return invalidEvent(data.Text == "", "assistant/chunk text is empty")
}
func validateToolCallData(_ eventType, data ToolCallData) error {
	return validateToolCall(ToolCall{ID: data.CallID, Name: data.Name, Args: data.Args})
}
func validateToolResultData(_ eventType, data ToolResultData) error {
	if err := validateToolCallID(data.CallID); err != nil {
		return err
	}
	return validateResultMetadata(data.Metadata)
}
func validateContextSummaryData(_ eventType, data ContextSummaryData) error {
	return invalidEvent(data.Op != "replace" || data.Start < 0 || data.End < data.Start || strings.TrimSpace(data.Summary) == "", "context/summary has an invalid replacement range or empty summary")
}
func validateAssistantMessageData(_ eventType, data AssistantMessageData) error {
	calls := data.ToolCalls
	if data.ToolCall != nil && len(data.ToolCalls) > 0 {
		first := data.ToolCalls[0]
		if first.ID != data.ToolCall.ID || first.Name != data.ToolCall.Name {
			return fmt.Errorf("assistant/message tool_call does not match tool_calls[0]")
		}
	}
	if len(calls) == 0 && data.ToolCall != nil {
		calls = []ToolCall{*data.ToolCall}
	}
	if err := validateToolCallList(calls, "", true); err != nil {
		return err
	}
	if data.Text == "" && len(calls) == 0 {
		return fmt.Errorf("assistant/message has neither text nor tool calls")
	}
	return nil
}

func validateToolCallList(calls []ToolCall, prefix string, rejectDuplicate bool) error {
	var seen map[string]bool
	if rejectDuplicate {
		seen = make(map[string]bool, len(calls))
	}
	for _, call := range calls {
		if err := validateToolCall(call); err != nil {
			if prefix != "" {
				return fmt.Errorf("%s: %w", prefix, err)
			}
			return err
		}
		if rejectDuplicate {
			if seen[call.ID] {
				return fmt.Errorf("assistant/message repeats tool call id %q", call.ID)
			}
			seen[call.ID] = true
		}
	}
	return nil
}

func validateApprovalRequestedData(_ eventType, data ApprovalRequestedData) error {
	if err := ValidateApprovalID(data.ApprovalID); err != nil {
		return err
	}
	if err := validateToolCall(data.ToolCall); err != nil {
		return fmt.Errorf("approval tool call: %w", err)
	}
	if err := validateToolCall(data.ResumeCall); err != nil {
		return fmt.Errorf("approval resume call: %w", err)
	}
	if err := validateToolCallList(data.RemainingCalls, "approval remaining call", false); err != nil {
		return err
	}
	if data.Step < 0 {
		return fmt.Errorf("approval request has invalid step %d", data.Step)
	}
	return nil
}

func validateApprovalResolvedData(_ eventType, data ApprovalResolvedData) error {
	if err := ValidateApprovalID(data.ApprovalID); err != nil {
		return err
	}
	if err := validateToolCallID(data.CallID); err != nil {
		return err
	}
	if data.ResolvedAt.IsZero() {
		return fmt.Errorf("approval resolution time is zero")
	}
	return invalidEvent(data.Decision != ApprovalApproved && data.Decision != ApprovalDenied && data.Decision != ApprovalExpired, "approval resolution has invalid decision %q", data.Decision)
}

func validateSessionEvent(event SessionEvent) error {
	if err := ValidateRunID(event.RunID); err != nil {
		return err
	}
	if len(event.Data) > MaxSessionEventDataBytes {
		return fmt.Errorf("event payload exceeds %d bytes", MaxSessionEventDataBytes)
	}
	validator, ok := sessionEventValidators[event.Type]
	if !ok {
		return fmt.Errorf("unknown session event type %q", event.Type)
	}
	target := validator.target()
	if len(event.Data) == 0 {
		return fmt.Errorf("%s event has no data", event.Type)
	}
	if err := json.Unmarshal(event.Data, target); err != nil {
		return fmt.Errorf("decode %s event: %w", event.Type, err)
	}
	_, err := validator.validate(event.Type, target)
	return err
}

var sessionEventValidators = map[SessionEventType]sessionEventValidator{
	EvRunStart: sessionEventValidatorFunc[RunStartData](validateRunStartData), EvRunResume: sessionEventValidatorFunc[RunResumeData](validateRunResumeData), EvRunEnd: sessionEventValidatorFunc[RunEndData](validateRunEndData),
	EvRunError: sessionEventValidatorFunc[RuntimeErrorData](validateRuntimeErrorData), EvStepError: sessionEventValidatorFunc[RuntimeErrorData](validateRuntimeErrorData), EvRunUsage: sessionEventValidatorFunc[RunUsageData](validateRunUsageData),
	EvStepStart: sessionEventValidatorFunc[StepData](validateStepData), EvStepEnd: sessionEventValidatorFunc[StepData](validateStepData), EvUserMessage: sessionEventValidatorFunc[UserMessageData](validateNoopEventData[UserMessageData]),
	EvAssistantChunk: sessionEventValidatorFunc[AssistantChunkData](validateAssistantChunkData), EvAssistantMessage: sessionEventValidatorFunc[AssistantMessageData](validateAssistantMessageData), EvToolCall: sessionEventValidatorFunc[ToolCallData](validateToolCallData),
	EvToolResult: sessionEventValidatorFunc[ToolResultData](validateToolResultData), EvApprovalRequested: sessionEventValidatorFunc[ApprovalRequestedData](validateApprovalRequestedData), EvApprovalResolved: sessionEventValidatorFunc[ApprovalResolvedData](validateApprovalResolvedData), EvContextSummary: sessionEventValidatorFunc[ContextSummaryData](validateContextSummaryData),
}

func validateRunComposition(composition *RunCompositionData) error {
	if composition == nil {
		return nil
	}
	return ValidateRunCompositionMetadata(composition.Metadata)
}

func validateRunCompositionEvidence(composition *RunCompositionData, compositionRevision, assignmentRevision string) error {
	if err := validateRunComposition(composition); err != nil {
		return err
	}
	if err := validateRevision(compositionRevision, "composition revision"); err != nil {
		return err
	}
	if err := validateRevision(assignmentRevision, "assignment revision"); err != nil {
		return err
	}
	if composition == nil {
		if compositionRevision != "" || assignmentRevision != "" {
			return fmt.Errorf("composition revisions require a composition")
		}
		return nil
	}
	calculatedComposition, err := CompositionRevision(composition)
	if err != nil {
		return err
	}
	if compositionRevision != "" && compositionRevision != calculatedComposition {
		return fmt.Errorf("composition revision does not match composition")
	}
	calculatedAssignment, err := CompositionMetadataRevision(composition.Metadata)
	if err != nil {
		return err
	}
	if assignmentRevision != "" && assignmentRevision != calculatedAssignment {
		return fmt.Errorf("assignment revision does not match composition metadata")
	}
	return nil
}

// ValidateRunCompositionMetadata enforces a small, string-only durable audit
// envelope. Product adapters define their own namespaced keys; the kernel only
// owns size and character safety.
func ValidateRunCompositionMetadata(metadata map[string]string) error {
	if len(metadata) > MaxRunCompositionMetadataItems {
		return fmt.Errorf("run composition metadata exceeds %d items", MaxRunCompositionMetadataItems)
	}
	for key, value := range metadata {
		if strings.TrimSpace(key) == "" || len(key) > MaxRunCompositionMetadataKeyBytes || containsControl(key) {
			return fmt.Errorf("run composition metadata key is empty, too long, or contains control characters")
		}
		if len(value) > MaxRunCompositionMetadataValueBytes || containsControl(value) {
			return fmt.Errorf("run composition metadata value for %q is too long or contains control characters", key)
		}
	}
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	if len(encoded) > MaxRunCompositionMetadataBytes {
		return fmt.Errorf("run composition metadata exceeds %d bytes", MaxRunCompositionMetadataBytes)
	}
	return nil
}

// CompositionMetadataRevision returns a stable digest for adapter-defined
// assignment metadata. Empty metadata returns an empty revision.
func CompositionMetadataRevision(metadata map[string]string) (string, error) {
	if len(metadata) == 0 {
		return "", nil
	}
	if err := ValidateRunCompositionMetadata(metadata); err != nil {
		return "", err
	}
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return "", fmt.Errorf("encode composition metadata revision: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

// CompositionRevision returns a stable digest for the provider-neutral Run
// Composition. Ephemeral profile creation time is excluded; assignment
// metadata remains part of the full composition digest.
func CompositionRevision(composition *RunCompositionData) (string, error) {
	if composition == nil {
		return "", nil
	}
	if err := ValidateRunCompositionMetadata(composition.Metadata); err != nil {
		return "", err
	}
	copyOf := cloneRunCompositionData(composition)
	copyOf.Profile.CreatedAt = time.Time{}
	encoded, err := json.Marshal(copyOf)
	if err != nil {
		return "", fmt.Errorf("encode composition revision: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

// ExtractRunCompositionEvidence returns the latest valid Composition segment
// for a run. Approval resumes intentionally supersede the initial run/start
// evidence because the runtime may have been re-resolved while suspended.
func ExtractRunCompositionEvidence(events []SessionEvent, runID string) (RunCompositionEvidence, bool, error) {
	if err := ValidateRunID(runID); err != nil {
		return RunCompositionEvidence{}, false, err
	}
	var evidence RunCompositionEvidence
	found := false
	for _, event := range events {
		if event.RunID != runID || (event.Type != EvRunStart && event.Type != EvRunResume) {
			continue
		}
		var composition *RunCompositionData
		var explicit RunCompositionEvidence
		switch event.Type {
		case EvRunStart:
			var data RunStartData
			if err := json.Unmarshal(event.Data, &data); err != nil {
				return RunCompositionEvidence{}, false, err
			}
			composition = data.Composition
			explicit = RunCompositionEvidence{CompositionRevision: data.CompositionRevision, AssignmentRevision: data.AssignmentRevision}
		case EvRunResume:
			var data RunResumeData
			if err := json.Unmarshal(event.Data, &data); err != nil {
				return RunCompositionEvidence{}, false, err
			}
			composition = data.Composition
			explicit = RunCompositionEvidence{CompositionRevision: data.CompositionRevision, AssignmentRevision: data.AssignmentRevision}
		}
		if composition == nil {
			if explicit != (RunCompositionEvidence{}) {
				return RunCompositionEvidence{}, false, fmt.Errorf("run %s composition evidence has no composition", runID)
			}
			evidence, found = explicit, true
			continue
		}
		calculatedComposition, err := CompositionRevision(composition)
		if err != nil {
			return RunCompositionEvidence{}, false, err
		}
		calculatedAssignment, err := CompositionMetadataRevision(composition.Metadata)
		if err != nil {
			return RunCompositionEvidence{}, false, err
		}
		if explicit.CompositionRevision != "" && explicit.CompositionRevision != calculatedComposition {
			return RunCompositionEvidence{}, false, fmt.Errorf("run %s composition revision mismatch", runID)
		}
		if explicit.AssignmentRevision != "" && explicit.AssignmentRevision != calculatedAssignment {
			return RunCompositionEvidence{}, false, fmt.Errorf("run %s assignment revision mismatch", runID)
		}
		evidence = RunCompositionEvidence{CompositionRevision: calculatedComposition, AssignmentRevision: calculatedAssignment}
		found = true
	}
	return evidence, found, nil
}

func validateRevision(value, name string) error {
	if value == "" {
		return nil
	}
	if len(value) != sha256.Size*2 {
		return fmt.Errorf("%s must be a SHA-256 digest", name)
	}
	if _, err := hex.DecodeString(value); err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	return nil
}

func cloneRunCompositionMetadata(metadata map[string]string) map[string]string {
	if metadata == nil {
		return nil
	}
	out := make(map[string]string, len(metadata))
	for key, value := range metadata {
		out[key] = value
	}
	return out
}

// ValidateRunID checks the durable run-id vocabulary used by event logs and
// run-scoped capability paths.
func ValidateRunID(runID string) error {
	if !runIDPattern.MatchString(runID) {
		return fmt.Errorf("run id %q must be 1-128 characters using letters, digits, '.', '_', ':' or '-'", runID)
	}
	return nil
}

// ValidateSessionID checks identifiers embedded in session scope paths and
// persistence keys.
func ValidateSessionID(sessionID string) error {
	if !runIDPattern.MatchString(sessionID) {
		return fmt.Errorf("session id %q must be 1-128 characters using letters, digits, '.', '_', ':' or '-'", sessionID)
	}
	return nil
}

// DeriveMessages projects all model-visible messages from the event log.
// A context/summary event shadows the model-visible messages in its seq range
// and projects to one summary message positioned at the start of that range,
// where the archived context used to live. Shadowing is additive: every
// recorded range applies, and a summary whose own seq falls inside a later,
// wider range is itself shadowed.
//
// The projection is cached incrementally: appending new events only projects
// the delta, so per-step cost is O(new events) instead of O(log). The
// returned messages are independent deep copies and may be safely mutated by
// adapters or compactors without corrupting the session projection cache.
func (s *Session) DeriveMessages() ([]ChatMessage, error) {
	// The cached projection is immutable after construction. Use a read lock on
	// the common path so concurrent model preparations do not serialize while
	// each caller makes its own defensive result copy.
	s.mu.RLock()
	if s.projValid && s.projCount == len(s.events) && s.projSummaries == s.summaryEvents {
		out := cloneChatMessages(s.projCache)
		s.mu.RUnlock()
		return out, nil
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	summaryCount := s.summaryEvents

	// Fast path: nothing new since the cached projection.
	if s.projValid && s.projCount == len(s.events) && s.projSummaries == summaryCount {
		return cloneChatMessages(s.projCache), nil
	}
	// Incremental path: only appends and no new summaries — old projections
	// and shadow ranges stay valid, so project just the delta.
	if s.projValid && s.projSummaries == summaryCount && s.projCount > 0 && s.projCount < len(s.events) {
		delta := s.events[s.projCount:]
		// Equal summary counts prove the append-only delta has no summary, so
		// its old shadow ranges remain valid. The cache is private and immutable;
		// a shallow slice copy is enough while extending it with fresh projections.
		out := append([]ChatMessage(nil), s.projCache...)
		for _, event := range delta {
			if message, ok := projectEvent(event); ok {
				out = append(out, message)
			}
		}
		s.projCache = out
		s.projCount = len(s.events)
		return cloneChatMessages(out), nil
	}

	// Full rebuild.
	type summaryRange struct {
		start, end, seq int64
		data            ContextSummaryData
	}
	var ranges []summaryRange
	for _, event := range s.events {
		if event.Type != EvContextSummary {
			continue
		}
		var data ContextSummaryData
		if err := json.Unmarshal(event.Data, &data); err != nil {
			return nil, fmt.Errorf("decode context summary at seq %d: %w", event.Seq, err)
		}
		ranges = append(ranges, summaryRange{start: data.Start, end: data.End, seq: event.Seq, data: data})
	}
	sort.Slice(ranges, func(i, j int) bool {
		if ranges[i].start != ranges[j].start {
			return ranges[i].start < ranges[j].start
		}
		return ranges[i].seq < ranges[j].seq
	})
	type shadowInterval struct{ start, end int64 }
	intervals := make([]shadowInterval, 0, len(ranges))
	for _, r := range ranges {
		if len(intervals) == 0 {
			intervals = append(intervals, shadowInterval{r.start, r.end})
			continue
		}
		last := intervals[len(intervals)-1]
		adjacent := last.end != int64(^uint64(0)>>1) && r.start == last.end+1
		if r.start > last.end && !adjacent {
			intervals = append(intervals, shadowInterval{r.start, r.end})
		} else if r.end > last.end {
			intervals[len(intervals)-1].end = r.end
		}
	}
	isShadowed := func(seq int64) bool {
		index := sort.Search(len(intervals), func(index int) bool { return intervals[index].end >= seq })
		return index < len(intervals) && intervals[index].start <= seq
	}

	out := []ChatMessage{}
	nextSummary := 0
	emitSummariesBefore := func(seq int64) {
		for nextSummary < len(ranges) && ranges[nextSummary].start <= seq {
			r := ranges[nextSummary]
			nextSummary++
			if isShadowed(r.seq) {
				continue // superseded by a later, wider replacement
			}
			out = append(out, ChatMessage{
				Role: RoleUser, SourceSeq: r.seq,
				Content:    fmt.Sprintf("[conversation summary of events %d–%d]\n%s", r.data.Start, r.data.End, r.data.Summary),
				Provenance: &ContextProvenance{Kind: "summary", SourceStart: r.data.Start, SourceEnd: r.data.End, Revision: fmt.Sprintf("%d", r.seq)},
			})
		}
	}
	for _, event := range s.events {
		emitSummariesBefore(event.Seq)
		if isShadowed(event.Seq) {
			continue
		}
		if message, ok := projectEvent(event); ok {
			out = append(out, message)
		}
	}
	// Seq is append-only but may be non-contiguous after restore/import. Emit
	// every remaining summary explicitly instead of assuming len(events)+1 is
	// beyond all valid summary starts.
	for nextSummary < len(ranges) {
		r := ranges[nextSummary]
		nextSummary++
		if isShadowed(r.seq) {
			continue
		}
		out = append(out, ChatMessage{Role: RoleUser, SourceSeq: r.seq, Content: fmt.Sprintf("[conversation summary of events %d–%d]\n%s", r.data.Start, r.data.End, r.data.Summary), Provenance: &ContextProvenance{Kind: "summary", SourceStart: r.data.Start, SourceEnd: r.data.End, Revision: fmt.Sprintf("%d", r.seq)}})
	}

	// out contains only freshly decoded/projected values and has not crossed a
	// public boundary, so retain it as the private immutable cache. Callers get
	// the single defensive deep copy below.
	s.projCache = out
	s.projCount = len(s.events)
	s.projSummaries = summaryCount
	s.projValid = true
	return cloneChatMessages(out), nil
}

// deriveRecentCompactedMessages fuses the default no-summary projection with
// RecentTurnsCompactor. It is deliberately private and does not populate or
// change the full projection cache: the append-only event log remains the
// source of truth, while Agent can avoid materializing messages that the
// default compactor would immediately discard.
//
// The boolean is false when summary shadowing is present, so callers retain
// the complete DeriveMessages then Compact path for that richer semantics.
func (s *Session) deriveRecentCompactedMessages(compactor RecentTurnsCompactor) ([]ChatMessage, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.summaryEvents != 0 {
		return nil, false
	}

	firstEvent := 0
	keptMessages := len(s.events)
	if compactor.MaxMessages > 0 {
		// Scan backwards until the suffix exceeds the requested window. The
		// earliest fitting user boundary is the last boundary encountered while
		// the visible suffix is <= MaxMessages. This avoids a full-history count
		// pass for the common small-window case; the selected suffix is then
		// projected once below. Non-visible events are deliberately ignored in
		// the count, matching RecentTurnsCompactor's message semantics.
		seen := 0
		latestBoundary := -1
		candidate := -1
		exceeded := false
		for eventIndex := len(s.events) - 1; eventIndex >= 0; eventIndex-- {
			event := s.events[eventIndex]
			if event.Type == EvContextSummary {
				return nil, false
			}
			if !modelVisibleEvent(event) {
				continue
			}
			if event.Type == EvUserMessage && seen > 0 {
				if latestBoundary < 0 {
					latestBoundary = eventIndex
				}
				if seen+1 <= compactor.MaxMessages {
					candidate = eventIndex
				} else {
					exceeded = true
					break
				}
			}
			seen++
			if seen > compactor.MaxMessages && latestBoundary >= 0 {
				exceeded = true
				break
			}
		}
		if exceeded && candidate >= 0 {
			firstEvent = candidate
		} else if exceeded && latestBoundary >= 0 {
			firstEvent = latestBoundary
		}
		keptMessages = compactor.MaxMessages
	}
	// Reuse the append-maintained projection when it covers the complete log.
	// SourceSeq filtering preserves non-contiguous event sequences and skips
	// non-model events without rescanning/decoding their payloads.
	if s.projValid && s.projCount == len(s.events) && s.projSummaries == 0 {
		startSeq := int64(-1)
		if firstEvent < len(s.events) {
			startSeq = s.events[firstEvent].Seq
		}
		cached := make([]ChatMessage, 0, keptMessages)
		for _, message := range s.projCache {
			if startSeq < 0 || message.SourceSeq >= startSeq {
				cached = append(cached, cloneChatMessage(message))
			}
		}
		if lastUser := lastUserIndex(cached); lastUser > 0 {
			for i := 0; i < lastUser; i++ {
				if cached[i].Role == RoleTool {
					if content, ok := compactor.elidedToolResult(cached[i].Content); ok {
						cached[i].Content = content
					}
				}
			}
		}
		return cached, true
	}

	out := make([]ChatMessage, 0, keptMessages)
	lastUser := -1
	for _, event := range s.events[firstEvent:] {
		if event.Type == EvContextSummary {
			return nil, false
		}
		message, ok := projectEvent(event)
		if !ok {
			continue
		}
		// projectEvent decodes fresh values, but detach ToolCall maps exactly as
		// DeriveMessages does before returning messages to an adapter.
		message = cloneChatMessage(message)
		if message.Role == RoleUser {
			lastUser = len(out)
		}
		out = append(out, message)
	}
	if lastUser > 0 {
		for index := 0; index < lastUser; index++ {
			if out[index].Role != RoleTool {
				continue
			}
			if content, ok := compactor.elidedToolResult(out[index].Content); ok {
				out[index].Content = content
			}
		}
	}
	return out, true
}

func lastUserIndex(messages []ChatMessage) int {
	result := -1
	for i, m := range messages {
		if m.Role == RoleUser {
			result = i
		}
	}
	return result
}

func cloneChatMessages(messages []ChatMessage) []ChatMessage {
	out := make([]ChatMessage, len(messages))
	for index, message := range messages {
		out[index] = cloneChatMessage(message)
	}
	return out
}

func cloneChatMessage(message ChatMessage) ChatMessage {
	out := message
	if message.Provenance != nil {
		provenance := *message.Provenance
		out.Provenance = &provenance
	}
	if message.ToolCall != nil {
		call := *message.ToolCall
		call.Args = cloneMap(message.ToolCall.Args)
		out.ToolCall = &call
	}
	if message.ToolCalls != nil {
		out.ToolCalls = make([]ToolCall, len(message.ToolCalls))
		for callIndex, call := range message.ToolCalls {
			out.ToolCalls[callIndex] = call
			out.ToolCalls[callIndex].Args = cloneMap(call.Args)
		}
	}
	return out
}

func modelVisibleEvent(event SessionEvent) bool {
	switch event.Type {
	case EvUserMessage, EvAssistantMessage, EvToolResult:
		return true
	default:
		return false
	}
}

func projectCommittedData(kind SessionEventType, data any, seq int64) (ChatMessage, bool, bool) {
	switch kind {
	case EvUserMessage:
		switch v := data.(type) {
		case UserMessageData:
			return ChatMessage{Role: RoleUser, Content: v.Text, SourceSeq: seq}, true, true
		case *UserMessageData:
			if v != nil {
				return ChatMessage{Role: RoleUser, Content: v.Text, SourceSeq: seq}, true, true
			}
		}
		return ChatMessage{}, false, true
	case EvAssistantMessage:
		var v AssistantMessageData
		switch x := data.(type) {
		case AssistantMessageData:
			v = x
		case *AssistantMessageData:
			if x != nil {
				v = *x
			} else {
				return ChatMessage{}, false, true
			}
		default:
			return ChatMessage{}, false, true
		}
		calls := cloneToolCallsFast(v.ToolCalls)
		var first *ToolCall
		if v.ToolCall != nil {
			c := cloneToolCall(*v.ToolCall)
			first = &c
			if len(calls) == 0 {
				calls = []ToolCall{c}
			}
		}
		return ChatMessage{Role: RoleAssistant, Content: v.Text, ToolCall: first, ToolCalls: calls, SourceSeq: seq}, true, true
	case EvToolResult:
		switch v := data.(type) {
		case ToolResultData:
			return ChatMessage{Role: RoleTool, Content: v.Content, ToolCallID: v.CallID, SourceSeq: seq}, true, true
		case *ToolResultData:
			if v != nil {
				return ChatMessage{Role: RoleTool, Content: v.Content, ToolCallID: v.CallID, SourceSeq: seq}, true, true
			}
		}
		return ChatMessage{}, false, true
	default:
		return ChatMessage{}, false, false
	}
}
func cloneToolCall(c ToolCall) ToolCall { c.Args = cloneMap(c.Args); return c }
func cloneToolCalls(in []ToolCall) []ToolCall {
	if in == nil {
		return nil
	}
	out := make([]ToolCall, len(in))
	for i, c := range in {
		out[i] = cloneToolCall(c)
	}
	return out
}

// projectEvent projects one non-summary event into a message, if it is
// model-visible. Shared helper for the cached and rebuild paths.
func projectEvent(event SessionEvent) (ChatMessage, bool) {
	switch event.Type {
	case EvUserMessage:
		var data UserMessageData
		if err := json.Unmarshal(event.Data, &data); err != nil {
			return ChatMessage{}, false
		}
		return ChatMessage{Role: RoleUser, Content: data.Text, SourceSeq: event.Seq}, true
	case EvAssistantMessage:
		var data AssistantMessageData
		if err := json.Unmarshal(event.Data, &data); err != nil {
			return ChatMessage{}, false
		}
		calls := data.ToolCalls
		if len(calls) == 0 && data.ToolCall != nil {
			calls = []ToolCall{*data.ToolCall}
		}
		return ChatMessage{Role: RoleAssistant, Content: data.Text, ToolCall: data.ToolCall, ToolCalls: calls, SourceSeq: event.Seq}, true
	case EvToolResult:
		var data ToolResultData
		if err := json.Unmarshal(event.Data, &data); err != nil {
			return ChatMessage{}, false
		}
		return ChatMessage{Role: RoleTool, Content: data.Content, ToolCallID: data.CallID, SourceSeq: event.Seq}, true
	}
	return ChatMessage{}, false
}

// EventsFrom deep-copies the event suffix starting at version, for stores
// that persist append-only deltas without copying the whole log.
func (s *Session) EventsFrom(version int64) []SessionEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if version < 0 || version > int64(len(s.events)) {
		return nil
	}
	out := make([]SessionEvent, int64(len(s.events))-version)
	for i, event := range s.events[version:] {
		out[i] = event
		out[i].Data = append(json.RawMessage(nil), event.Data...)
	}
	return out
}

func (s *Session) LastAssistantText(runID string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for i := len(s.events) - 1; i >= 0; i-- {
		event := s.events[i]
		if event.RunID != runID || event.Type != EvAssistantMessage {
			continue
		}
		var data AssistantMessageData
		if json.Unmarshal(event.Data, &data) == nil && data.Text != "" {
			return data.Text
		}
	}
	return ""
}

// RunStatus reports whether a run has started and returns either its final
// status or RunWaitingApproval for an unresolved durable checkpoint.
func (s *Session) RunStatus(runID string) (RunStatus, bool) {
	if ValidateRunID(runID) != nil {
		return "", false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.runIndex[runID]
	if !ok {
		return "", false
	}
	if entry.ended {
		return entry.status, true
	}
	if len(entry.pending) > 0 {
		return RunWaitingApproval, true
	}
	return "", entry.isStarted()
}

func (entry *runIndexEntry) isStarted() bool { return entry != nil && entry.started }

// PendingApproval returns the most recent unresolved approval checkpoint for
// one run. The returned value is detached from the Session log.
func (s *Session) PendingApproval(runID string) (ApprovalRequestedData, bool, error) {
	if err := ValidateRunID(runID); err != nil {
		return ApprovalRequestedData{}, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.runIndex[runID]
	if !ok || len(entry.pending) == 0 {
		return ApprovalRequestedData{}, false, nil
	}
	var latest int64 = -1
	for _, seq := range entry.pending {
		if seq > latest {
			latest = seq
		}
	}
	if latest < 0 || latest >= int64(len(s.events)) {
		return ApprovalRequestedData{}, false, fmt.Errorf("pending approval index is corrupt")
	}
	var data ApprovalRequestedData
	if err := json.Unmarshal(s.events[latest].Data, &data); err != nil {
		return ApprovalRequestedData{}, false, err
	}
	data.ToolCall.Args = cloneMap(data.ToolCall.Args)
	data.ResumeCall.Args = cloneMap(data.ResumeCall.Args)
	data.RemainingCalls = cloneToolCalls(data.RemainingCalls)
	return data, true, nil
}

// HasToolCall reports whether the run already logged one logical call ID.
func (s *Session) HasToolCall(runID, callID string) (bool, error) {
	if err := ValidateRunID(runID); err != nil {
		return false, err
	}
	if err := validateToolCallID(callID); err != nil {
		return false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.runIndex[runID]
	return ok && entry != nil && entry.toolCalls != nil && func() bool { _, exists := entry.toolCalls[callID]; return exists }(), nil
}

// ToolResult returns a prior result for one logical call ID. Composite
// capabilities use it during durable resume to replay completed child steps.
func (s *Session) ToolResult(runID, callID string) (CapabilityResult, bool, error) {
	if err := ValidateRunID(runID); err != nil {
		return CapabilityResult{}, false, err
	}
	if err := validateToolCallID(callID); err != nil {
		return CapabilityResult{}, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.runIndex[runID]
	if !ok || entry.toolResults == nil {
		return CapabilityResult{}, false, nil
	}
	seq, ok := entry.toolResults[callID]
	if !ok || seq < 0 || seq >= int64(len(s.events)) {
		return CapabilityResult{}, false, nil
	}
	var data ToolResultData
	if err := json.Unmarshal(s.events[seq].Data, &data); err != nil {
		return CapabilityResult{}, false, err
	}
	return cloneCapabilityResult(CapabilityResult{Content: data.Content, OK: data.OK, Metadata: data.Metadata}), true, nil
}

func cloneToolCallsFast(calls []ToolCall) []ToolCall {
	if calls == nil {
		return nil
	}
	out := make([]ToolCall, len(calls))
	for index, call := range calls {
		out[index] = call
		out[index].Args = cloneMap(call.Args)
	}
	return out
}

// Clone creates an independent session value for optimistic transactions.
func (s *Session) Clone() (*Session, error) {
	return RestoreSession(SessionOptions{
		ID: s.id, ProfileID: s.profileID, Principal: s.principal, Scope: s.scope, Metadata: s.metadata,
	}, s.Events())
}

// NewID returns a cryptographically random public identifier.
func NewID(prefix string) (string, error) {
	if prefix == "" || len(prefix) > 32 || !runIDPattern.MatchString(prefix) {
		return "", fmt.Errorf("id prefix %q is invalid", prefix)
	}
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", fmt.Errorf("generate id: %w", err)
	}
	return prefix + hex.EncodeToString(bytes[:]), nil
}
