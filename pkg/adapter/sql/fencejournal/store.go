// Package fencejournal persists runtime FenceJournal request evidence.
package fencejournal

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/whhhh1500/auto-agent/pkg/adapter/sql/sqlkit"
	"github.com/whhhh1500/auto-agent/pkg/runtime"
)

type Store struct {
	db      *sql.DB
	dialect sqlkit.Dialect
}

var _ runtime.FenceJournal = (*Store)(nil)

func New(db *sql.DB, dialect sqlkit.Dialect) (*Store, error) {
	if db == nil || !dialect.Valid() {
		return nil, fmt.Errorf("fence journal requires a database and supported dialect")
	}
	return &Store{db: db, dialect: dialect}, nil
}

func (store *Store) Begin(ctx context.Context, command runtime.FenceCommand) (runtime.FenceRecord, error) {
	if err := command.Validate(); err != nil {
		return runtime.FenceRecord{}, err
	}
	ctx = nonNilContext(ctx)
	insert := `INSERT INTO runtime_fence_journal (request_id, composition_revision, actor_id, reason, decision, created_at, updated_at)
		VALUES (?, ?, ?, ?, 'execute', ?, ?) ON CONFLICT (request_id) DO NOTHING`
	now := time.Now().UTC().UnixMilli()
	result, err := store.db.ExecContext(ctx, store.bind(insert), command.RequestID, command.CompositionRevision, command.ActorID, command.Reason, now, now)
	if err != nil {
		return runtime.FenceRecord{}, err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return runtime.FenceRecord{}, err
	}
	if changed == 1 {
		return runtime.FenceRecord{Command: command, Decision: runtime.FenceDecisionExecute}, nil
	}
	record, err := store.load(ctx, command.RequestID)
	if err != nil {
		return runtime.FenceRecord{}, err
	}
	if record.Command != command {
		return runtime.FenceRecord{Command: command, Decision: runtime.FenceDecisionConflict}, runtime.ErrFenceConflict
	}
	switch record.Decision {
	case runtime.FenceDecisionReplay:
		return record, nil
	case runtime.FenceDecisionUnknown:
		return record, runtime.ErrFenceUnknown
	default:
		// A concurrent or crashed executor is ambiguous; it is never retried
		// as another owner effect from a fresh SQL handle.
		return runtime.FenceRecord{Command: command, Decision: runtime.FenceDecisionUnknown}, runtime.ErrFenceUnknown
	}
}

func (store *Store) Complete(ctx context.Context, command runtime.FenceCommand, result runtime.FenceResult) error {
	if err := command.Validate(); err != nil {
		return err
	}
	if result.CompositionRevision != command.CompositionRevision || !result.Completed || result.RemainingLeases != 0 {
		return fmt.Errorf("%w: incomplete fence result", runtime.ErrInvalidFenceCommand)
	}
	ctx = nonNilContext(ctx)
	update := `UPDATE runtime_fence_journal SET decision = 'replay', completed = 1, remaining_leases = 0, updated_at = ?
		WHERE request_id = ? AND composition_revision = ? AND actor_id = ? AND reason = ? AND decision = 'execute'`
	changed, err := store.changed(ctx, update, time.Now().UTC().UnixMilli(), command.RequestID, command.CompositionRevision, command.ActorID, command.Reason)
	if err != nil || changed == 1 {
		return err
	}
	record, err := store.load(ctx, command.RequestID)
	if err != nil || record.Command != command {
		return runtime.ErrFenceConflict
	}
	if record.Decision == runtime.FenceDecisionReplay && record.Result == result {
		return nil
	}
	if record.Decision == runtime.FenceDecisionUnknown {
		return runtime.ErrFenceUnknown
	}
	return runtime.ErrFenceConflict
}

func (store *Store) MarkUnknown(ctx context.Context, command runtime.FenceCommand) error {
	if err := command.Validate(); err != nil {
		return err
	}
	ctx = nonNilContext(ctx)
	update := `UPDATE runtime_fence_journal SET decision = 'unknown', completed = 0, remaining_leases = 0, updated_at = ?
		WHERE request_id = ? AND composition_revision = ? AND actor_id = ? AND reason = ? AND decision = 'execute'`
	changed, err := store.changed(ctx, update, time.Now().UTC().UnixMilli(), command.RequestID, command.CompositionRevision, command.ActorID, command.Reason)
	if err != nil || changed == 1 {
		return err
	}
	record, err := store.load(ctx, command.RequestID)
	if err != nil || record.Command != command {
		return runtime.ErrFenceConflict
	}
	if record.Decision == runtime.FenceDecisionUnknown {
		return nil
	}
	return runtime.ErrFenceConflict
}

func (store *Store) changed(ctx context.Context, query string, args ...any) (int64, error) {
	result, err := store.db.ExecContext(ctx, store.bind(query), args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (store *Store) load(ctx context.Context, requestID string) (runtime.FenceRecord, error) {
	query := `SELECT composition_revision, actor_id, reason, decision, completed, remaining_leases
		FROM runtime_fence_journal WHERE request_id = ?`
	var record runtime.FenceRecord
	var decision string
	var completed int64
	if err := store.db.QueryRowContext(ctx, store.bind(query), requestID).Scan(&record.Command.CompositionRevision, &record.Command.ActorID, &record.Command.Reason, &decision, &completed, &record.Result.RemainingLeases); err != nil {
		return runtime.FenceRecord{}, err
	}
	record.Command.RequestID = requestID
	record.Decision = runtime.FenceDecision(decision)
	record.Result.CompositionRevision = record.Command.CompositionRevision
	record.Result.Completed = completed == 1
	if err := record.Command.Validate(); err != nil || record.Result.RemainingLeases < 0 {
		return runtime.FenceRecord{}, runtime.ErrFenceUnknown
	}
	switch record.Decision {
	case runtime.FenceDecisionExecute:
		if completed != 0 || record.Result.RemainingLeases != 0 {
			return runtime.FenceRecord{}, runtime.ErrFenceUnknown
		}
	case runtime.FenceDecisionReplay:
		if !record.Result.Completed || record.Result.RemainingLeases != 0 {
			return runtime.FenceRecord{}, runtime.ErrFenceUnknown
		}
	case runtime.FenceDecisionUnknown:
		if record.Result.Completed || record.Result.RemainingLeases != 0 {
			return runtime.FenceRecord{}, runtime.ErrFenceUnknown
		}
	default:
		return runtime.FenceRecord{}, runtime.ErrFenceUnknown
	}
	return record, nil
}

func (store *Store) bind(query string) string {
	bound, err := sqlkit.Bind(query, store.dialect)
	if err != nil {
		panic(err)
	}
	return bound
}

func nonNilContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}
