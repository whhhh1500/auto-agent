// Package effectjournal provides the SQL adapter for runtime.EffectJournal.
// The runtime package remains independent from database and dialect details.
package effectjournal

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cc-auto-agent/harness-core/pkg/adapter/sql/sqlkit"
	"github.com/cc-auto-agent/harness-core/pkg/runtime"
)

type Store struct {
	db      *sql.DB
	dialect sqlkit.Dialect
}

// SQLite permits only one writer. Deferred transactions can otherwise all
// establish read snapshots and then fail while upgrading to a write lock
// instead of waiting on busy_timeout. Serialize this adapter's write path in
// process; PostgreSQL retains normal concurrent behavior.
var sqliteWriteMu sync.Mutex

var _ runtime.EffectJournal = (*Store)(nil)

var (
	selectEffect = `SELECT effect_id, composition_revision, module_id,
		module_revision_major, module_revision_minor, module_revision_patch,
		phase, forward_kind, forward_target, forward_payload,
		inverse_kind, inverse_target, inverse_payload, state, ordinal,
		created_at, updated_at FROM runtime_effects WHERE effect_id = ?`
	selectComposition = `SELECT effect_id, composition_revision, module_id,
		module_revision_major, module_revision_minor, module_revision_patch,
		phase, forward_kind, forward_target, forward_payload,
		inverse_kind, inverse_target, inverse_payload, state, ordinal,
		created_at, updated_at FROM runtime_effects
		WHERE composition_revision = ? ORDER BY ordinal ASC`
	insertSequence = `INSERT INTO runtime_effect_sequences
		(composition_revision, next_ordinal) VALUES (?, 0)
		ON CONFLICT (composition_revision) DO NOTHING`
	bumpSequence = `UPDATE runtime_effect_sequences
		SET next_ordinal = next_ordinal + 1 WHERE composition_revision = ?`
	selectSequence = `SELECT next_ordinal FROM runtime_effect_sequences
		WHERE composition_revision = ?`
	insertEffect = `INSERT INTO runtime_effects
		(effect_id, composition_revision, module_id,
		 module_revision_major, module_revision_minor, module_revision_patch,
		 phase, forward_kind, forward_target, forward_payload,
		 inverse_kind, inverse_target, inverse_payload, state, ordinal,
		 created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	markApplied = `UPDATE runtime_effects SET state = 'applied', updated_at = ?
		WHERE effect_id = ? AND state = 'prepared'`
	markUnknown = `UPDATE runtime_effects SET state = 'unknown', updated_at = ?
		WHERE effect_id = ? AND state = 'prepared'`
	markReverted = `UPDATE runtime_effects SET state = 'reverted', updated_at = ?
		WHERE effect_id = ? AND state IN ('prepared', 'applied', 'unknown')`
)

func New(db *sql.DB, dialect sqlkit.Dialect) (*Store, error) {
	if db == nil {
		return nil, fmt.Errorf("effect journal requires a database handle")
	}
	if !dialect.Valid() {
		return nil, fmt.Errorf("unsupported SQL dialect %q", dialect.String())
	}
	return &Store{db: db, dialect: dialect}, nil
}

func (store *Store) Record(ctx context.Context, descriptor runtime.EffectDescriptor) (runtime.RecordedEffect, error) {
	if err := runtime.ValidateEffectDescriptor(descriptor); err != nil {
		return runtime.RecordedEffect{}, err
	}
	if store.dialect == sqlkit.SQLite {
		sqliteWriteMu.Lock()
		defer sqliteWriteMu.Unlock()
	}
	ctx = nonNilContext(ctx)
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return runtime.RecordedEffect{}, err
	}
	defer func() { _ = tx.Rollback() }()

	prior, err := scanEffect(tx.QueryRowContext(ctx, store.bind(selectEffect), string(descriptor.ID)))
	if err == nil {
		if !sameDescriptor(prior.Descriptor, descriptor) || prior.State == runtime.EffectReverted {
			return runtime.RecordedEffect{}, fmt.Errorf("%w: effect %q already has another record", runtime.ErrEffectConflict, descriptor.ID)
		}
		return cloneRecordedEffect(prior), nil
	}
	if !errors.Is(err, runtime.ErrEffectNotFound) {
		return runtime.RecordedEffect{}, err
	}

	if _, err := tx.ExecContext(ctx, store.bind(insertSequence), descriptor.CompositionRevision); err != nil {
		return runtime.RecordedEffect{}, err
	}
	if _, err := tx.ExecContext(ctx, store.bind(bumpSequence), descriptor.CompositionRevision); err != nil {
		return runtime.RecordedEffect{}, err
	}
	var ordinal int64
	if err := tx.QueryRowContext(ctx, store.bind(selectSequence), descriptor.CompositionRevision).Scan(&ordinal); err != nil {
		return runtime.RecordedEffect{}, err
	}
	now := time.Now().UTC().UnixNano()
	if _, err := tx.ExecContext(ctx, store.bind(insertEffect),
		string(descriptor.ID), descriptor.CompositionRevision, string(descriptor.ModuleID),
		descriptor.ModuleRevision.Major, descriptor.ModuleRevision.Minor, descriptor.ModuleRevision.Patch,
		string(descriptor.Phase), descriptor.Forward.Kind, descriptor.Forward.Target,
		encodePayload(descriptor.Forward.Payload), descriptor.Inverse.Kind, descriptor.Inverse.Target,
		encodePayload(descriptor.Inverse.Payload), string(runtime.EffectPrepared), ordinal, now, now,
	); err != nil {
		if !isUniqueConstraint(err) {
			return runtime.RecordedEffect{}, err
		}
		// Another writer won the effect_id race. Rollback the sequence update,
		// then read the committed winner and apply the normal identity check.
		_ = tx.Rollback()
		prior, getErr := store.get(ctx, descriptor.ID)
		if getErr != nil {
			return runtime.RecordedEffect{}, getErr
		}
		if !sameDescriptor(prior.Descriptor, descriptor) || prior.State == runtime.EffectReverted {
			return runtime.RecordedEffect{}, fmt.Errorf("%w: effect %q already has another record", runtime.ErrEffectConflict, descriptor.ID)
		}
		return cloneRecordedEffect(prior), nil
	}
	if err := tx.Commit(); err != nil {
		return runtime.RecordedEffect{}, err
	}
	return runtime.RecordedEffect{Descriptor: cloneDescriptor(descriptor), State: runtime.EffectPrepared, Ordinal: uint64(ordinal)}, nil
}

func (store *Store) MarkApplied(ctx context.Context, id runtime.EffectID) error {
	return store.mark(ctx, id, runtime.EffectApplied, markApplied)
}

func (store *Store) MarkUnknown(ctx context.Context, id runtime.EffectID) error {
	return store.mark(ctx, id, runtime.EffectUnknown, markUnknown)
}

func (store *Store) MarkReverted(ctx context.Context, id runtime.EffectID) error {
	return store.mark(ctx, id, runtime.EffectReverted, markReverted)
}

func (store *Store) mark(ctx context.Context, id runtime.EffectID, target runtime.EffectState, query string) error {
	if store.dialect == sqlkit.SQLite {
		sqliteWriteMu.Lock()
		defer sqliteWriteMu.Unlock()
	}
	ctx = nonNilContext(ctx)
	result, err := store.db.ExecContext(ctx, store.bind(query), time.Now().UTC().UnixNano(), string(id))
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed == 1 {
		return nil
	}
	prior, err := store.get(ctx, id)
	if err != nil {
		return err
	}
	if prior.State == target {
		return nil
	}
	return fmt.Errorf("%w: effect %q cannot transition from %s to %s", runtime.ErrEffectConflict, id, prior.State, target)
}

func (store *Store) List(ctx context.Context, compositionRevision string) ([]runtime.RecordedEffect, error) {
	ctx = nonNilContext(ctx)
	rows, err := store.db.QueryContext(ctx, store.bind(selectComposition), compositionRevision)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]runtime.RecordedEffect, 0)
	for rows.Next() {
		record, err := scanEffect(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, cloneRecordedEffect(record))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (store *Store) get(ctx context.Context, id runtime.EffectID) (runtime.RecordedEffect, error) {
	return scanEffect(store.db.QueryRowContext(nonNilContext(ctx), store.bind(selectEffect), string(id)))
}

type scanner interface{ Scan(...any) error }

func scanEffect(row scanner) (runtime.RecordedEffect, error) {
	var (
		id, composition, module, phase, forwardKind, forwardTarget, forwardPayload string
		inverseKind, inverseTarget, inversePayload, state                          string
		major, minor, patch, ordinal, created, updated                             int64
	)
	if err := row.Scan(&id, &composition, &module, &major, &minor, &patch, &phase,
		&forwardKind, &forwardTarget, &forwardPayload, &inverseKind, &inverseTarget,
		&inversePayload, &state, &ordinal, &created, &updated); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return runtime.RecordedEffect{}, fmt.Errorf("%w: effect", runtime.ErrEffectNotFound)
		}
		return runtime.RecordedEffect{}, err
	}
	forward, err := decodePayload(forwardPayload)
	if err != nil {
		return runtime.RecordedEffect{}, fmt.Errorf("%w: forward payload: %v", runtime.ErrInvalidEffect, err)
	}
	inverse, err := decodePayload(inversePayload)
	if err != nil {
		return runtime.RecordedEffect{}, fmt.Errorf("%w: inverse payload: %v", runtime.ErrInvalidEffect, err)
	}
	record := runtime.RecordedEffect{Descriptor: runtime.EffectDescriptor{
		ID: runtime.EffectID(id), ModuleID: runtime.ModuleID(module), CompositionRevision: composition,
		ModuleRevision: runtime.Version{Major: int(major), Minor: int(minor), Patch: int(patch)},
		Phase:          runtime.EffectPhase(phase),
		Forward:        runtime.EffectAction{Kind: forwardKind, Target: forwardTarget, Payload: forward},
		Inverse:        runtime.EffectAction{Kind: inverseKind, Target: inverseTarget, Payload: inverse},
	}, State: runtime.EffectState(state), Ordinal: uint64(ordinal)}
	if !record.State.Valid() || !record.Descriptor.Phase.Valid() || ordinal < 1 || created < 1 || updated < 1 {
		return runtime.RecordedEffect{}, fmt.Errorf("%w: invalid persisted effect %q", runtime.ErrInvalidEffect, id)
	}
	if err := runtime.ValidateEffectDescriptor(record.Descriptor); err != nil {
		return runtime.RecordedEffect{}, fmt.Errorf("%w: persisted descriptor: %v", runtime.ErrInvalidEffect, err)
	}
	return record, nil
}

func (store *Store) bind(query string) string {
	bound, err := sqlkit.Bind(query, store.dialect)
	if err != nil {
		panic("effect journal SQL binding invariant violated: " + err.Error())
	}
	return bound
}

func nonNilContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func isUniqueConstraint(err error) bool {
	if err == nil {
		return false
	}
	// Keep the adapter independent of a concrete PostgreSQL driver while using
	// its unambiguous duplicate-key SQLSTATE when available.
	var stateErr interface{ SQLState() string }
	if errors.As(err, &stateErr) {
		return stateErr.SQLState() == "23505"
	}
	message := strings.ToLower(err.Error())
	// modernc SQLite uses these specific forms for UNIQUE/PRIMARY KEY errors.
	// CHECK, FK, NOT NULL, and other constraint failures must not be treated as
	// an effect_id race.
	return strings.Contains(message, "unique constraint failed:") ||
		strings.Contains(message, "primary key must be unique") ||
		strings.Contains(message, "duplicate key value violates unique constraint")
}
