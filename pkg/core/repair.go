package core

import (
	"encoding/json"
	"fmt"
	"sort"
)

// Stable metadata codes recorded on synthetic repair results.
const (
	CodeRunInterrupted     = "run_interrupted"
	CodeToolOutcomeUnknown = "tool_outcome_unknown"
	CodeToolNotStarted     = "tool_not_started"
)

// RepairInterrupted scans a loaded event log for a run that never reached
// run/end and returns the deterministic synthetic events needed to close it:
// error results for tool calls that never completed, a step/end for an open
// step, then run/error + run/end. The repaired transcript is provider-valid,
// so the session can resume instead of being discarded.
//
// The returned events carry RunID, Type and Data; Seq and Time are zero and
// assigned by Session.Append when the caller commits them. A balanced log
// (every run/start closed by run/end) yields nil.
func RepairInterrupted(events []SessionEvent) []SessionEvent {
	// Track the state of the most recent, still-open run only.
	var (
		runID     string
		runOpen   bool
		stepOpen  bool
		expected  map[string]bool // assistant requested, tool/call never seen
		inFlight  map[string]bool // tool/call seen, no tool/result yet
		approvals map[string]bool // requested and not yet resolved
	)
	flush := func() {
		runID, runOpen, stepOpen = "", false, false
		expected, inFlight, approvals = nil, nil, nil
	}

	for _, event := range events {
		switch event.Type {
		case EvRunStart:
			flush()
			runID, runOpen = event.RunID, true
		case EvRunEnd:
			flush()
		case EvStepStart:
			if runOpen {
				stepOpen = true
			}
		case EvStepEnd, EvStepError:
			if runOpen {
				stepOpen = false
			}
		case EvAssistantMessage:
			if !runOpen {
				continue
			}
			var data AssistantMessageData
			if json.Unmarshal(event.Data, &data) != nil {
				continue
			}
			calls := data.ToolCalls
			if len(calls) == 0 && data.ToolCall != nil {
				calls = []ToolCall{*data.ToolCall}
			}
			if len(calls) > 0 {
				if expected == nil {
					expected = map[string]bool{}
				}
				for _, call := range calls {
					expected[call.ID] = true
				}
			}
		case EvToolCall:
			if !runOpen {
				continue
			}
			var data ToolCallData
			if json.Unmarshal(event.Data, &data) != nil {
				continue
			}
			delete(expected, data.CallID)
			if inFlight == nil {
				inFlight = map[string]bool{}
			}
			inFlight[data.CallID] = true
		case EvToolResult:
			if !runOpen {
				continue
			}
			var data ToolResultData
			if json.Unmarshal(event.Data, &data) != nil {
				continue
			}
			delete(expected, data.CallID)
			delete(inFlight, data.CallID)
		case EvApprovalRequested:
			if !runOpen {
				continue
			}
			var data ApprovalRequestedData
			if json.Unmarshal(event.Data, &data) != nil {
				continue
			}
			if approvals == nil {
				approvals = map[string]bool{}
			}
			approvals[data.ApprovalID] = true
		case EvApprovalResolved:
			if !runOpen {
				continue
			}
			var data ApprovalResolvedData
			if json.Unmarshal(event.Data, &data) == nil {
				delete(approvals, data.ApprovalID)
			}
		}
	}
	if !runOpen {
		return nil
	}
	// An unresolved durable approval is a deliberate suspension, not a crash.
	// The queue owns resumption and the open step/tool call must remain intact.
	if len(approvals) > 0 {
		return nil
	}

	synthetic := []SessionEvent{}
	result := func(callID, code string) SessionEvent {
		payload, err := json.Marshal(ToolResultData{
			CallID:   callID,
			Content:  fmt.Sprintf("tool outcome lost: the run was interrupted before %s completed", callID),
			OK:       false,
			Metadata: map[string]any{"code": code, "repaired": true},
		})
		if err != nil { // ToolResultData always marshals; kept for safety.
			return SessionEvent{}
		}
		return SessionEvent{RunID: runID, Type: EvToolResult, Data: payload}
	}
	appendCalls := func(calls map[string]bool, code string) {
		if len(calls) == 0 {
			return
		}
		ids := make([]string, 0, len(calls))
		for id := range calls {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			synthetic = append(synthetic, result(id, code))
		}
	}
	appendCalls(expected, CodeToolNotStarted)
	appendCalls(inFlight, CodeToolOutcomeUnknown)
	if stepOpen {
		payload, _ := json.Marshal(StepData{Index: -1})
		synthetic = append(synthetic, SessionEvent{RunID: runID, Type: EvStepEnd, Data: payload})
	}
	errorPayload, _ := json.Marshal(RuntimeErrorData{
		Code:      CodeRunInterrupted,
		Message:   fmt.Sprintf("run %s was interrupted before reaching a terminal state; synthetic closers were appended on load", runID),
		Retryable: true,
	})
	synthetic = append(synthetic, SessionEvent{RunID: runID, Type: EvRunError, Data: errorPayload})
	endPayload, _ := json.Marshal(RunEndData{Status: RunFailed})
	synthetic = append(synthetic, SessionEvent{RunID: runID, Type: EvRunEnd, Data: endPayload})
	return synthetic
}

// AppendRepair commits synthetic repair events to a restored session and
// emits them through emit, assigning seqs and timestamps.
func AppendRepair(session *Session, synthetic []SessionEvent, emit func(SessionEvent)) error {
	for _, event := range synthetic {
		var data any
		switch event.Type {
		case EvToolResult:
			data = &ToolResultData{}
		case EvStepEnd:
			data = &map[string]int{}
		case EvRunError:
			data = &RuntimeErrorData{}
		case EvRunEnd:
			data = &RunEndData{}
		default:
			data = &map[string]any{}
		}
		if err := json.Unmarshal(event.Data, data); err != nil {
			return fmt.Errorf("decode synthetic %s event: %w", event.Type, err)
		}
		appended, err := session.Append(event.RunID, event.Type, data)
		if err != nil {
			return err
		}
		if emit != nil {
			emit(appended)
		}
	}
	return nil
}
