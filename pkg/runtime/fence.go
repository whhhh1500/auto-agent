package runtime

import (
	"context"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	MaxFenceReasonBytes   = 512
	MaxMemoryFenceRecords = 1024
)

// FenceCommand is the host-level authorization and audit identity for a
// destructive fence. It intentionally does not contain owner lease tokens.
type FenceCommand struct {
	RequestID           string
	CompositionRevision string
	ActorID             string
	Reason              string
}

// Validate rejects ambiguous or unsafe fence identities before authorization
// or audit evidence is written.
func (command FenceCommand) Validate() error {
	if err := validateID(command.RequestID, "fence request"); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidFenceCommand, err)
	}
	if err := validateID(command.CompositionRevision, "fence composition"); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidFenceCommand, err)
	}
	if err := validateID(command.ActorID, "fence actor"); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidFenceCommand, err)
	}
	if command.Reason == "" || len(command.Reason) > MaxFenceReasonBytes || !utf8.ValidString(command.Reason) || strings.TrimSpace(command.Reason) == "" {
		return fmt.Errorf("%w: reason is empty, invalid, or exceeds %d bytes", ErrInvalidFenceCommand, MaxFenceReasonBytes)
	}
	for index, character := range command.Reason {
		if unicode.IsControl(character) {
			return fmt.Errorf("%w: reason contains control at %d", ErrInvalidFenceCommand, index)
		}
	}
	return nil
}

// FenceAuthorizer decides whether an actor may issue a concrete fence. A
// missing, failed, or panicking authorizer is denied by ModuleHost.
type FenceAuthorizer interface {
	AuthorizeFence(context.Context, FenceCommand) error
}

// FenceDecision describes the durable fence-journal decision for one request.
type FenceDecision string

const (
	FenceDecisionExecute  FenceDecision = "execute"
	FenceDecisionRetry    FenceDecision = "retry"
	FenceDecisionReplay   FenceDecision = "replay"
	FenceDecisionConflict FenceDecision = "conflict"
	FenceDecisionUnknown  FenceDecision = "unknown"
)

// FenceResult records only public, non-secret completion facts.
type FenceResult struct {
	CompositionRevision string
	Completed           bool
	RemainingLeases     int
}

// FenceRecord binds one request identity to its complete command and latest
// decision/result. It contains no owner or public lease credentials.
type FenceRecord struct {
	Command  FenceCommand
	Decision FenceDecision
	Result   FenceResult
}

// FenceJournal provides durable request-idempotency evidence. Begin is called
// after authorization and before owner effects. Complete records success;
// MarkUnknown makes ambiguous work fail closed.
type FenceJournal interface {
	Begin(context.Context, FenceCommand) (FenceRecord, error)
	Complete(context.Context, FenceCommand, FenceResult) error
	MarkUnknown(context.Context, FenceCommand) error
}

// HostControls are immutable composition-root controls. They are accepted only
// during host construction; ModuleHost intentionally exposes no setter.
type HostControls struct {
	FenceAuthorizer FenceAuthorizer
	FenceJournal    FenceJournal
}

func validateHostControls(controls HostControls) error {
	if (controls.FenceAuthorizer == nil) != (controls.FenceJournal == nil) {
		return fmt.Errorf("%w: fence authorizer and journal must be provided together", ErrInvalidHost)
	}
	return nil
}
