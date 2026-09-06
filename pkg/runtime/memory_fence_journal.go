package runtime

import (
	"context"
	"fmt"
	"sync"
)

// MemoryFenceJournal is a synchronous, bounded reference FenceJournal. It is
// intended for tests and embedded use; durable SQL persistence is separate.
type MemoryFenceJournal struct {
	mu      sync.Mutex
	records map[string]FenceRecord
}

var _ FenceJournal = (*MemoryFenceJournal)(nil)

func NewMemoryFenceJournal() *MemoryFenceJournal { return &MemoryFenceJournal{} }

func (journal *MemoryFenceJournal) Begin(ctx context.Context, command FenceCommand) (FenceRecord, error) {
	if err := fenceContextError(ctx); err != nil {
		return FenceRecord{}, err
	}
	if err := command.Validate(); err != nil {
		return FenceRecord{}, err
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.records == nil {
		journal.records = make(map[string]FenceRecord)
	}
	prior, found := journal.records[command.RequestID]
	if !found {
		if len(journal.records) >= MaxMemoryFenceRecords {
			return FenceRecord{}, ErrFenceJournalFull
		}
		record := FenceRecord{Command: command, Decision: FenceDecisionExecute}
		journal.records[command.RequestID] = record
		return record, nil
	}
	if prior.Command != command {
		return FenceRecord{Command: command, Decision: FenceDecisionConflict}, ErrFenceConflict
	}
	switch prior.Decision {
	case FenceDecisionReplay:
		return prior, nil
	case FenceDecisionUnknown:
		return prior, ErrFenceUnknown
	case FenceDecisionExecute, FenceDecisionRetry:
		// Without a durable attempt token, a second executor cannot prove that
		// the first one never reached an owner. Keep the original in-flight
		// evidence and reject the duplicate rather than issuing a retry.
		return FenceRecord{Command: command, Decision: FenceDecisionUnknown}, ErrFenceUnknown
	default:
		return FenceRecord{Command: command, Decision: FenceDecisionUnknown}, ErrFenceUnknown
	}
}

func (journal *MemoryFenceJournal) Complete(ctx context.Context, command FenceCommand, result FenceResult) error {
	if err := fenceContextError(ctx); err != nil {
		return err
	}
	if err := command.Validate(); err != nil {
		return err
	}
	if result.CompositionRevision != command.CompositionRevision || !result.Completed || result.RemainingLeases != 0 {
		return fmt.Errorf("%w: incomplete fence result", ErrInvalidFenceCommand)
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	prior, found := journal.records[command.RequestID]
	if !found || prior.Command != command {
		return ErrFenceConflict
	}
	if prior.Decision == FenceDecisionUnknown {
		return ErrFenceUnknown
	}
	if prior.Decision == FenceDecisionReplay {
		if prior.Result != result {
			return ErrFenceConflict
		}
		return nil
	}
	prior.Decision = FenceDecisionReplay
	prior.Result = result
	journal.records[command.RequestID] = prior
	return nil
}

func (journal *MemoryFenceJournal) MarkUnknown(ctx context.Context, command FenceCommand) error {
	if err := fenceContextError(ctx); err != nil {
		return err
	}
	if err := command.Validate(); err != nil {
		return err
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	prior, found := journal.records[command.RequestID]
	if !found || prior.Command != command {
		return ErrFenceConflict
	}
	if prior.Decision == FenceDecisionReplay {
		return ErrFenceConflict
	}
	prior.Decision = FenceDecisionUnknown
	prior.Result = FenceResult{}
	journal.records[command.RequestID] = prior
	return nil
}

func fenceContextError(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}
