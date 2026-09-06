package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

// ErrSessionWriteFenceLost reports that a queued worker no longer owns every
// durable authority required to append Session history. It is intentionally
// distinct from core.ErrSessionConflict: callers may retry a version conflict
// after reloading, but a lost fence must stop the stale worker.
var ErrSessionWriteFenceLost = errors.New("session write fence lost")

// SessionWriteFence identifies one live queued-run claim and its independently
// acquired Session lease. TenantID and SubjectID are the durable ownership
// fields shared by the queue and Session catalog. Profile and scope are
// immutable Session metadata, not run-claim authority: the execution layer
// must validate them before it constructs a fence. The database, rather than
// the caller, decides whether either lease is still live at append time.
type SessionWriteFence struct {
	SessionID       string
	RunID           string
	TenantID        string
	SubjectID       string
	WorkerID        string
	QueueGeneration int64
	LeaseHolder     string
}

// FencedSessionAppender is an additive optional contract for stores that can
// atomically validate queued-run ownership and append Session events in the
// same transaction. Stores backed by separate Session and queue databases must
// not implement it using best-effort cross-store checks. Empty event slices are
// no-ops and do not probe ownership; callers that need to prove a live fence
// must use their queue/session-lease renewal path instead.
type FencedSessionAppender interface {
	AppendEventsFenced(ctx context.Context, fence SessionWriteFence, expectedVersion int64, events []core.SessionEvent) error
}

func validateSessionWriteFence(fence SessionWriteFence) error {
	if err := core.ValidateSessionID(fence.SessionID); err != nil {
		return fmt.Errorf("session write fence: %w", err)
	}
	if err := core.ValidateRunID(fence.RunID); err != nil {
		return fmt.Errorf("session write fence: %w", err)
	}
	if strings.TrimSpace(fence.TenantID) == "" {
		return fmt.Errorf("session write fence tenant id is empty")
	}
	if err := validateSQLTextFilter("session write fence tenant id", fence.TenantID); err != nil {
		return err
	}
	if strings.TrimSpace(fence.SubjectID) == "" {
		return fmt.Errorf("session write fence subject id is empty")
	}
	if err := validateSQLTextFilter("session write fence subject id", fence.SubjectID); err != nil {
		return err
	}
	if err := validateWorkerID(fence.WorkerID); err != nil {
		return fmt.Errorf("session write fence: %w", err)
	}
	if fence.QueueGeneration <= 0 {
		return fmt.Errorf("session write fence queue generation must be positive")
	}
	if err := validateLeaseHolder(fence.LeaseHolder); err != nil {
		return fmt.Errorf("session write fence: %w", err)
	}
	return nil
}

func validateFencedAppend(fence SessionWriteFence, expectedVersion int64, events []core.SessionEvent) error {
	if err := validateSessionWriteFence(fence); err != nil {
		return err
	}
	if expectedVersion < 0 {
		return fmt.Errorf("session append expected version must not be negative")
	}
	if err := validateAppendEvents(expectedVersion, events); err != nil {
		return err
	}
	for index, event := range events {
		if event.RunID != fence.RunID {
			return fmt.Errorf("event %d belongs to run %q, not fenced run %q", index, event.RunID, fence.RunID)
		}
	}
	return nil
}

func sessionWriteFenceLost(reason string) error {
	return fmt.Errorf("%w: %s", ErrSessionWriteFenceLost, reason)
}

// RepairInterruptedSessionFenced is the ownership-safe counterpart of
// RepairInterruptedSession. It computes repair events from one Clone and
// persists only the synthetic suffix through AppendEventsFenced.
//
// Version conflicts retain the existing one-reload convergence behavior.
// Fence loss is returned directly and is never downgraded to a normal
// optimistic conflict. If the snapshot needs no synthetic suffix, this is a
// local no-op: no database fence validation occurs and callers must not treat
// a nil error as proof that the fence remains live.
func RepairInterruptedSessionFenced(ctx context.Context, store core.SessionStore, fence SessionWriteFence, session *core.Session) (*core.Session, bool, error) {
	if ctx == nil || store == nil || session == nil {
		return nil, false, fmt.Errorf("repair interrupted session with fence: context, store, and session are required")
	}
	if err := validateSessionWriteFence(fence); err != nil {
		return nil, false, err
	}
	if session.ID() != fence.SessionID {
		return nil, false, fmt.Errorf("repair session %q does not match fenced session %q", session.ID(), fence.SessionID)
	}
	appender, ok := store.(FencedSessionAppender)
	if !ok {
		return nil, false, fmt.Errorf("session store %T does not implement fenced append", store)
	}
	if err := ctx.Err(); err != nil {
		return nil, false, fmt.Errorf("repair interrupted session %s with fence: %w", session.ID(), err)
	}
	repaired, err := session.Clone()
	if err != nil {
		return nil, false, err
	}
	synthetic := core.RepairInterrupted(repaired.Events())
	if len(synthetic) == 0 {
		return repaired, false, nil
	}
	expectedVersion := repaired.Version()
	if err := core.AppendRepair(repaired, synthetic, nil); err != nil {
		return nil, false, err
	}
	if err := ctx.Err(); err != nil {
		return nil, false, fmt.Errorf("repair interrupted session %s with fence: %w", session.ID(), err)
	}
	err = appender.AppendEventsFenced(ctx, fence, expectedVersion, repaired.EventsFrom(expectedVersion))
	if err == nil {
		return repaired, true, nil
	}
	if errors.Is(err, ErrSessionWriteFenceLost) {
		return nil, false, fmt.Errorf("persist fenced repair for session %s: %w", session.ID(), err)
	}
	if !errors.Is(err, core.ErrSessionConflict) {
		return nil, false, fmt.Errorf("persist fenced repair for session %s: %w", session.ID(), err)
	}
	latest, loadErr := store.Load(ctx, session.ID())
	if loadErr != nil {
		return nil, false, errors.Join(err, fmt.Errorf("reload session %s after fenced repair conflict: %w", session.ID(), loadErr))
	}
	if len(core.RepairInterrupted(latest.Events())) == 0 {
		return latest, false, nil
	}
	return nil, false, fmt.Errorf("persist fenced repair for session %s at version %d: %w", session.ID(), expectedVersion, err)
}
