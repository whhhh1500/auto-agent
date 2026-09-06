package storage

import (
	"context"
	"errors"
	"fmt"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

// RepairInterruptedSession explicitly closes an interrupted run and persists
// the synthetic suffix with SessionStore's optimistic version check. Callers
// must hold execution ownership from the supplied Load through this call;
// ordinary SessionStore.Load calls are read-only.
//
// If another writer wins the Save race, the helper reloads once. A balanced
// log is an idempotent no-op. A still-open log keeps ErrSessionConflict so the
// caller cannot repair a moving run by retrying without reacquiring ownership.
func RepairInterruptedSession(ctx context.Context, store core.SessionStore, session *core.Session) (*core.Session, bool, error) {
	if ctx == nil || store == nil || session == nil {
		return nil, false, fmt.Errorf("repair interrupted session: context, store, and session are required")
	}
	if err := ctx.Err(); err != nil {
		return nil, false, fmt.Errorf("repair interrupted session %s: %w", session.ID(), err)
	}
	// Clone first so the repair decision, expected version, and persisted suffix
	// all come from one coherent snapshot. Reading Events and Version separately
	// from a concurrently advancing Session could otherwise combine an old tail
	// classification with a newer expected version.
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
		return nil, false, fmt.Errorf("repair interrupted session %s: %w", session.ID(), err)
	}
	if err := store.Save(ctx, repaired, expectedVersion); err == nil {
		return repaired, true, nil
	} else if !errors.Is(err, core.ErrSessionConflict) {
		return nil, false, fmt.Errorf("persist repair for session %s: %w", session.ID(), err)
	} else {
		latest, loadErr := store.Load(ctx, session.ID())
		if loadErr != nil {
			return nil, false, errors.Join(err, fmt.Errorf("reload session %s after repair conflict: %w", session.ID(), loadErr))
		}
		if len(core.RepairInterrupted(latest.Events())) == 0 {
			return latest, false, nil
		}
		return nil, false, fmt.Errorf("persist repair for session %s at version %d: %w", session.ID(), expectedVersion, err)
	}
}
