package modelexecution

import "fmt"

// StreamValidator enforces bounded ordered events before a consumer observes
// them. It is per-request and has no goroutine or shared mutable state.
type StreamValidator struct {
	events    int
	textBytes int
	toolArgs  map[int]int
	toolIDs   map[int]string
	toolNames map[int]string
	tools     map[int]struct{}
	usageSeen bool
	finished  bool
}

func NewStreamValidator() *StreamValidator {
	return &StreamValidator{toolArgs: make(map[int]int), tools: make(map[int]struct{}), toolIDs: make(map[int]string), toolNames: make(map[int]string)}
}

// Accept validates and copies one event. A post-finish event, duplicate usage,
// malformed delta, or limit overflow is rejected before it reaches the sink.
func (v *StreamValidator) Accept(event Event) (Event, error) {
	if v == nil {
		return Event{}, fmt.Errorf("%w: nil validator", ErrStreamState)
	}
	if v.finished {
		return Event{}, fmt.Errorf("%w: event after finish", ErrStreamState)
	}
	v.events++
	if v.events > DefaultMaxEvents {
		return Event{}, fmt.Errorf("%w: event limit", ErrInvalidEvent)
	}
	switch event.Kind {
	case EventTextDelta:
		if event.Text == "" || event.ToolCall != nil || event.Usage != nil || event.Finish != "" {
			return Event{}, fmt.Errorf("%w: text delta", ErrInvalidEvent)
		}
		v.textBytes += len(event.Text)
		if v.textBytes > DefaultMaxTextBytes {
			return Event{}, fmt.Errorf("%w: text limit", ErrInvalidEvent)
		}
	case EventToolCallDelta:
		if event.ToolCall == nil || event.Text != "" || event.Usage != nil || event.Finish != "" {
			return Event{}, fmt.Errorf("%w: tool delta", ErrInvalidEvent)
		}
		delta := event.ToolCall
		if delta.Index < 0 || delta.Index >= DefaultMaxToolCalls || (delta.ID == "" && delta.Name == "" && len(delta.ArgumentsFragment) == 0) {
			return Event{}, fmt.Errorf("%w: tool delta fields", ErrInvalidEvent)
		}
		if (delta.ID != "" && invalidIdentifier(delta.ID)) || (delta.Name != "" && invalidIdentifier(delta.Name)) {
			return Event{}, fmt.Errorf("%w: tool delta identifier", ErrInvalidEvent)
		}
		if previous := v.toolIDs[delta.Index]; previous != "" && delta.ID != "" && previous != delta.ID {
			return Event{}, fmt.Errorf("%w: conflicting tool id", ErrInvalidEvent)
		}
		if previous := v.toolNames[delta.Index]; previous != "" && delta.Name != "" && previous != delta.Name {
			return Event{}, fmt.Errorf("%w: conflicting tool name", ErrInvalidEvent)
		}
		if delta.ID != "" {
			v.toolIDs[delta.Index] = delta.ID
		}
		if delta.Name != "" {
			v.toolNames[delta.Index] = delta.Name
		}
		v.tools[delta.Index] = struct{}{}
		if len(v.tools) > DefaultMaxToolCalls {
			return Event{}, fmt.Errorf("%w: tool count", ErrInvalidEvent)
		}
		v.toolArgs[delta.Index] += len(delta.ArgumentsFragment)
		if v.toolArgs[delta.Index] > DefaultMaxToolArgBytes {
			return Event{}, fmt.Errorf("%w: tool arguments", ErrInvalidEvent)
		}
		deltaCopy := *delta
		deltaCopy.ArgumentsFragment = append([]byte(nil), delta.ArgumentsFragment...)
		event.ToolCall = &deltaCopy
	case EventUsage:
		if event.Usage == nil || event.Text != "" || event.ToolCall != nil || event.Finish != "" || v.usageSeen || event.Usage.InputTokens < 0 || event.Usage.OutputTokens < 0 {
			return Event{}, fmt.Errorf("%w: usage", ErrInvalidEvent)
		}
		v.usageSeen = true
		usageCopy := *event.Usage
		event.Usage = &usageCopy
	case EventFinish:
		if event.Text != "" || event.ToolCall != nil || event.Usage != nil || (event.Finish != FinishStop && event.Finish != FinishToolCalls) {
			return Event{}, fmt.Errorf("%w: finish", ErrInvalidEvent)
		}
		if event.Finish == FinishToolCalls && len(v.tools) == 0 {
			return Event{}, fmt.Errorf("%w: empty tool finish", ErrInvalidEvent)
		}
		if event.Finish == FinishStop && len(v.tools) > 0 {
			return Event{}, fmt.Errorf("%w: stop with tools", ErrInvalidEvent)
		}
		v.finished = true
	default:
		return Event{}, fmt.Errorf("%w: unknown kind", ErrInvalidEvent)
	}
	return event, nil
}

func (v *StreamValidator) Finished() bool { return v != nil && v.finished }
