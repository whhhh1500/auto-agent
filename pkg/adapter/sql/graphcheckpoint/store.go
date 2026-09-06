// Package graphcheckpoint persists the bounded Graph-G1 checkpoint Store
// contract. It owns no executor, node implementation, lease, or server wiring.
package graphcheckpoint

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"time"

	"github.com/cc-auto-agent/harness-core/pkg/adapter/sql/sqlkit"
	"github.com/cc-auto-agent/harness-core/pkg/extensions/graph"
)

const (
	maxCheckpointDocumentBytes = 640 << 10
	maxTransitionDocumentBytes = 64 << 10
)

// Store is a SQL implementation of graph.Store. Callers must initialize the
// v41 schema through storage.OpenSQLSessionStore before using it.
type Store struct {
	db      *sql.DB
	dialect sqlkit.Dialect
}

var _ graph.Store = (*Store)(nil)
var _ graph.HistoryStore = (*Store)(nil)

func New(db *sql.DB, dialect sqlkit.Dialect) (*Store, error) {
	if db == nil {
		return nil, fmt.Errorf("graph checkpoint store requires a database handle")
	}
	if !dialect.Valid() {
		return nil, fmt.Errorf("unsupported SQL dialect %q", dialect.String())
	}
	return &Store{db: db, dialect: dialect}, nil
}

func (store *Store) Load(ctx context.Context, key graph.CheckpointKey) (graph.Checkpoint, error) {
	if ctx == nil {
		return graph.Checkpoint{}, fmt.Errorf("%w: nil context", graph.ErrInvalidCheckpoint)
	}
	if err := graph.ValidateCheckpointKey(key); err != nil {
		return graph.Checkpoint{}, err
	}
	var revision uint64
	var raw string
	err := store.db.QueryRowContext(ctx, store.bind(`SELECT revision, checkpoint_json FROM graph_checkpoints
		WHERE tenant_id = ? AND session_id = ? AND run_id = ?`), key.TenantID, key.SessionID, key.RunID).Scan(&revision, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return graph.Checkpoint{}, graph.ErrCheckpointNotFound
	}
	if err != nil {
		return graph.Checkpoint{}, err
	}
	checkpoint, err := decodeCheckpoint([]byte(raw))
	if err != nil || checkpoint.Key != key || checkpoint.Revision != revision {
		return graph.Checkpoint{}, fmt.Errorf("%w: corrupt checkpoint document", graph.ErrInvalidCheckpoint)
	}
	return checkpoint, nil
}

func (store *Store) Create(ctx context.Context, checkpoint graph.Checkpoint, transition graph.Transition) (graph.Checkpoint, graph.CommitDisposition, error) {
	if ctx == nil {
		return graph.Checkpoint{}, graph.CommitUnknown, fmt.Errorf("%w: nil context", graph.ErrInvalidCheckpoint)
	}
	checkpoint, transition, err := validatePair(checkpoint, transition, 0)
	if err != nil {
		return graph.Checkpoint{}, graph.CommitUnknown, err
	}
	if checkpoint.Status != graph.CheckpointReady {
		return graph.Checkpoint{}, graph.CommitUnknown, fmt.Errorf("%w: create requires ready checkpoint", graph.ErrInvalidCheckpoint)
	}
	return store.write(ctx, checkpoint, transition, 0, true)
}

func (store *Store) CompareAndSwap(ctx context.Context, key graph.CheckpointKey, expected uint64, checkpoint graph.Checkpoint, transition graph.Transition) (graph.Checkpoint, graph.CommitDisposition, error) {
	if ctx == nil {
		return graph.Checkpoint{}, graph.CommitUnknown, fmt.Errorf("%w: nil context", graph.ErrInvalidCheckpoint)
	}
	if err := graph.ValidateCheckpointKey(key); err != nil {
		return graph.Checkpoint{}, graph.CommitUnknown, err
	}
	if expected == 0 {
		// Initial creation has its own atomic API. Treating zero as a replayable
		// CAS would let a caller blur a missing fact and a revision-one fact.
		return graph.Checkpoint{}, graph.CommitUnknown, graph.ErrCheckpointConflict
	}
	if checkpoint.Key != key {
		return graph.Checkpoint{}, graph.CommitUnknown, fmt.Errorf("%w: checkpoint key differs", graph.ErrCheckpointConflict)
	}
	checkpoint, transition, err := validatePair(checkpoint, transition, expected)
	if err != nil {
		return graph.Checkpoint{}, graph.CommitUnknown, err
	}
	return store.write(ctx, checkpoint, transition, expected, false)
}

func (store *Store) ListTransitions(ctx context.Context, key graph.CheckpointKey, afterRevision uint64, limit int) ([]graph.Transition, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", graph.ErrInvalidCheckpoint)
	}
	if err := graph.ValidateCheckpointKey(key); err != nil {
		return nil, err
	}
	if limit < 1 || limit > graph.MaxTransitionPageSize {
		return nil, fmt.Errorf("%w: transition page limit", graph.ErrInvalidTransition)
	}
	if _, err := store.Load(ctx, key); err != nil {
		return nil, err
	}
	rows, err := store.db.QueryContext(ctx, store.bind(`SELECT revision, transition_id, transition_json FROM graph_transitions
		WHERE tenant_id = ? AND session_id = ? AND run_id = ? AND revision > ? ORDER BY revision ASC LIMIT ?`),
		key.TenantID, key.SessionID, key.RunID, afterRevision, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	transitions := make([]graph.Transition, 0, limit)
	for rows.Next() {
		var revision uint64
		var id, raw string
		if err := rows.Scan(&revision, &id, &raw); err != nil {
			return nil, err
		}
		transition, err := decodeTransition([]byte(raw))
		if err != nil || transition.Key != key || transition.Revision != revision || transition.ID != id {
			return nil, fmt.Errorf("%w: corrupt transition document", graph.ErrInvalidTransition)
		}
		transitions = append(transitions, transition)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return transitions, nil
}

// LoadVersion returns one immutable, validated checkpoint snapshot. Historical
// evidence stays readable even if the mutable current head is later damaged.
func (store *Store) LoadVersion(ctx context.Context, key graph.CheckpointKey, revision uint64) (graph.CheckpointVersion, error) {
	if ctx == nil {
		return graph.CheckpointVersion{}, fmt.Errorf("%w: nil context", graph.ErrInvalidCheckpoint)
	}
	if err := graph.ValidateCheckpointKey(key); err != nil {
		return graph.CheckpointVersion{}, err
	}
	if revision == 0 {
		return graph.CheckpointVersion{}, fmt.Errorf("%w: checkpoint version revision", graph.ErrInvalidCheckpoint)
	}
	var storedRevision uint64
	var id, parentID, origin, hash, raw string
	var createdAt int64
	err := store.db.QueryRowContext(ctx, store.bind(`SELECT revision, version_id, parent_version_id, origin, checkpoint_hash, checkpoint_json, created_at
		FROM graph_checkpoint_versions WHERE tenant_id = ? AND session_id = ? AND run_id = ? AND revision = ?`),
		key.TenantID, key.SessionID, key.RunID, revision).Scan(&storedRevision, &id, &parentID, &origin, &hash, &raw, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return graph.CheckpointVersion{}, graph.ErrCheckpointNotFound
	}
	if err != nil {
		return graph.CheckpointVersion{}, err
	}
	checkpoint, err := decodeCheckpoint([]byte(raw))
	if err != nil {
		return graph.CheckpointVersion{}, err
	}
	version, err := graph.ValidateCheckpointVersion(graph.CheckpointVersion{
		Info:       graph.CheckpointVersionInfo{ID: id, ParentID: parentID, Origin: graph.CheckpointVersionOrigin(origin), Key: key, Revision: storedRevision, CheckpointHash: hash, CreatedAt: createdAt},
		Checkpoint: checkpoint,
	})
	if err != nil || storedRevision != revision {
		return graph.CheckpointVersion{}, fmt.Errorf("%w: corrupt checkpoint version", graph.ErrInvalidCheckpoint)
	}
	return version, nil
}

// ListVersions returns metadata only, ordered by ascending revision and
// exclusive of afterRevision. It deliberately does not select checkpoint_json.
func (store *Store) ListVersions(ctx context.Context, key graph.CheckpointKey, afterRevision uint64, limit int) ([]graph.CheckpointVersionInfo, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", graph.ErrInvalidCheckpoint)
	}
	if err := graph.ValidateCheckpointKey(key); err != nil {
		return nil, err
	}
	if limit < 1 || limit > graph.MaxCheckpointVersionPageSize {
		return nil, fmt.Errorf("%w: checkpoint version page limit", graph.ErrInvalidCheckpoint)
	}
	rows, err := store.db.QueryContext(ctx, store.bind(`SELECT revision, version_id, parent_version_id, origin, checkpoint_hash, created_at
		FROM graph_checkpoint_versions WHERE tenant_id = ? AND session_id = ? AND run_id = ? AND revision > ?
		ORDER BY revision ASC LIMIT ?`), key.TenantID, key.SessionID, key.RunID, afterRevision, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	versions := make([]graph.CheckpointVersionInfo, 0, limit)
	for rows.Next() {
		var info graph.CheckpointVersionInfo
		var origin string
		if err := rows.Scan(&info.Revision, &info.ID, &info.ParentID, &origin, &info.CheckpointHash, &info.CreatedAt); err != nil {
			return nil, err
		}
		info.Origin = graph.CheckpointVersionOrigin(origin)
		info.Key = key
		validated, err := graph.ValidateCheckpointVersionInfo(info)
		if err != nil {
			return nil, fmt.Errorf("%w: corrupt checkpoint version metadata", graph.ErrInvalidCheckpoint)
		}
		versions = append(versions, validated)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if len(versions) == 0 {
		var found int
		err := store.db.QueryRowContext(ctx, store.bind(`SELECT 1 FROM graph_checkpoint_versions
			WHERE tenant_id = ? AND session_id = ? AND run_id = ? LIMIT 1`), key.TenantID, key.SessionID, key.RunID).Scan(&found)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, graph.ErrCheckpointNotFound
		}
		if err != nil {
			return nil, err
		}
	}
	return versions, nil
}

func (store *Store) write(ctx context.Context, checkpoint graph.Checkpoint, transition graph.Transition, expected uint64, creating bool) (graph.Checkpoint, graph.CommitDisposition, error) {
	const sqliteBusyRetries = 3
	for attempt := 0; ; attempt++ {
		stored, disposition, err := store.writeOnce(ctx, checkpoint, transition, expected, creating)
		if err == nil || !store.isSQLiteBusy(err) || attempt == sqliteBusyRetries {
			return stored, disposition, err
		}
		timer := time.NewTimer(time.Duration(attempt+1) * 5 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return graph.Checkpoint{}, graph.CommitUnknown, ctx.Err()
		case <-timer.C:
		}
	}
}

func (store *Store) writeOnce(ctx context.Context, checkpoint graph.Checkpoint, transition graph.Transition, expected uint64, creating bool) (graph.Checkpoint, graph.CommitDisposition, error) {
	checkpointJSON, err := encodeCheckpoint(checkpoint)
	if err != nil {
		return graph.Checkpoint{}, graph.CommitUnknown, err
	}
	transitionJSON, err := encodeTransition(transition)
	if err != nil {
		return graph.Checkpoint{}, graph.CommitUnknown, err
	}
	parentID := ""
	if !creating {
		parentID = graph.CheckpointVersionID(checkpoint.Key, expected)
	}
	version, err := checkpointVersion(checkpoint, parentID)
	if err != nil {
		return graph.Checkpoint{}, graph.CommitUnknown, err
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return graph.Checkpoint{}, graph.CommitUnknown, err
	}
	defer tx.Rollback()
	if !creating {
		current, currentErr := loadTxCheckpoint(ctx, tx, store, checkpoint.Key)
		if currentErr != nil {
			err = currentErr
		} else if current.Revision != expected {
			err = graph.ErrCheckpointConflict
		} else if sourceErr := validateTransitionSource(current, transition); sourceErr != nil {
			err = sourceErr
		} else if current.Status == graph.CheckpointWaitingApproval && checkpoint.Status == graph.CheckpointReady &&
			(checkpoint.AttemptID != current.AttemptID || (transition.OutcomeCode != "approval_approved" && transition.OutcomeCode != "approval_denied")) {
			err = fmt.Errorf("%w: approved resume must retain the suspended attempt", graph.ErrInvalidTransition)
		}
	}
	if err == nil {
		_, err = tx.ExecContext(ctx, store.bind(`INSERT INTO graph_checkpoint_versions
			(tenant_id, session_id, run_id, revision, version_id, parent_version_id, origin, checkpoint_hash, checkpoint_json, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`),
			version.Info.Key.TenantID, version.Info.Key.SessionID, version.Info.Key.RunID, version.Info.Revision,
			version.Info.ID, version.Info.ParentID, version.Info.Origin, version.Info.CheckpointHash, string(checkpointJSON), version.Info.CreatedAt)
	}
	if err == nil {
		_, err = tx.ExecContext(ctx, store.bind(`INSERT INTO graph_transitions
			(tenant_id, session_id, run_id, revision, transition_id, transition_json) VALUES (?, ?, ?, ?, ?, ?)`),
			transition.Key.TenantID, transition.Key.SessionID, transition.Key.RunID, transition.Revision, transition.ID, string(transitionJSON))
	}
	if err == nil {
		if creating {
			_, err = tx.ExecContext(ctx, store.bind(`INSERT INTO graph_checkpoints
				(tenant_id, session_id, run_id, revision, checkpoint_json) VALUES (?, ?, ?, ?, ?)`),
				checkpoint.Key.TenantID, checkpoint.Key.SessionID, checkpoint.Key.RunID, checkpoint.Revision, string(checkpointJSON))
		} else {
			var result sql.Result
			result, err = tx.ExecContext(ctx, store.bind(`UPDATE graph_checkpoints SET revision = ?, checkpoint_json = ?
				WHERE tenant_id = ? AND session_id = ? AND run_id = ? AND revision = ?`), checkpoint.Revision, string(checkpointJSON),
				checkpoint.Key.TenantID, checkpoint.Key.SessionID, checkpoint.Key.RunID, expected)
			if err == nil {
				changed, changedErr := result.RowsAffected()
				if changedErr != nil {
					return graph.Checkpoint{}, graph.CommitUnknown, changedErr
				}
				if changed != 1 {
					err = graph.ErrCheckpointConflict
				}
			}
		}
	}
	if err == nil {
		if err = tx.Commit(); err == nil {
			return checkpoint, graph.CommitApplied, nil
		}
	}
	// A duplicate key can only be accepted after a byte-for-byte equivalent
	// contract replay. This lookup is deliberately after rollback: a failed CAS
	// must leave no transition behind.
	if existing, lookupErr := store.exactReplay(ctx, checkpoint, transition, parentID); lookupErr == nil && existing {
		return checkpoint, graph.CommitReplayed, nil
	}
	if isConstraint(err) || errors.Is(err, graph.ErrCheckpointConflict) {
		return graph.Checkpoint{}, graph.CommitUnknown, graph.ErrCheckpointConflict
	}
	return graph.Checkpoint{}, graph.CommitUnknown, err
}

func (store *Store) isSQLiteBusy(err error) bool {
	if store.dialect != sqlkit.SQLite || err == nil {
		return false
	}
	message := bytes.ToLower([]byte(err.Error()))
	return bytes.Contains(message, []byte("database is locked")) ||
		bytes.Contains(message, []byte("database table is locked")) ||
		bytes.Contains(message, []byte("sqlite_busy"))
}

func loadTxCheckpoint(ctx context.Context, tx *sql.Tx, store *Store, key graph.CheckpointKey) (graph.Checkpoint, error) {
	var revision uint64
	var raw string
	err := tx.QueryRowContext(ctx, store.bind(`SELECT revision, checkpoint_json FROM graph_checkpoints
		WHERE tenant_id = ? AND session_id = ? AND run_id = ?`), key.TenantID, key.SessionID, key.RunID).Scan(&revision, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return graph.Checkpoint{}, graph.ErrCheckpointNotFound
	}
	if err != nil {
		return graph.Checkpoint{}, err
	}
	checkpoint, err := decodeCheckpoint([]byte(raw))
	if err != nil || checkpoint.Key != key || checkpoint.Revision != revision {
		return graph.Checkpoint{}, fmt.Errorf("%w: corrupt checkpoint document", graph.ErrInvalidCheckpoint)
	}
	return checkpoint, nil
}

// validateTransitionSource makes a persisted CAS describe the actual fact it
// replaces, not just a status label supplied by an arbitrary caller.
func validateTransitionSource(current graph.Checkpoint, transition graph.Transition) error {
	if transition.From != current.Status || transition.SourceSegmentID != current.SegmentID ||
		transition.SourceHostGeneration != current.HostGeneration || transition.SourceNodeID != current.CurrentNodeID ||
		transition.SourceAttemptID != current.AttemptID {
		return fmt.Errorf("%w: transition source does not match current checkpoint", graph.ErrInvalidTransition)
	}
	return nil
}

func (store *Store) exactReplay(ctx context.Context, checkpoint graph.Checkpoint, transition graph.Transition, parentID string) (bool, error) {
	current, err := store.Load(ctx, checkpoint.Key)
	if err != nil {
		return false, err
	}
	if !reflect.DeepEqual(current, checkpoint) {
		return false, graph.ErrCheckpointConflict
	}
	version, err := store.LoadVersion(ctx, checkpoint.Key, checkpoint.Revision)
	if err != nil {
		return false, err
	}
	if !reflect.DeepEqual(version.Checkpoint, checkpoint) || version.Info.ID != graph.CheckpointVersionID(checkpoint.Key, checkpoint.Revision) || version.Info.ParentID != parentID || version.Info.Origin != graph.CheckpointVersionOriginCommit {
		return false, graph.ErrCheckpointConflict
	}
	var revision uint64
	var id, raw string
	err = store.db.QueryRowContext(ctx, store.bind(`SELECT revision, transition_id, transition_json FROM graph_transitions
		WHERE tenant_id = ? AND session_id = ? AND run_id = ? AND revision = ?`),
		checkpoint.Key.TenantID, checkpoint.Key.SessionID, checkpoint.Key.RunID, transition.Revision).Scan(&revision, &id, &raw)
	if err != nil {
		return false, err
	}
	stored, err := decodeTransition([]byte(raw))
	if err != nil || stored.Revision != revision || stored.ID != id || !reflect.DeepEqual(stored, transition) {
		return false, graph.ErrCheckpointConflict
	}
	return true, nil
}

func checkpointVersion(checkpoint graph.Checkpoint, parentID string) (graph.CheckpointVersion, error) {
	hash, err := graph.CheckpointDigest(checkpoint)
	if err != nil {
		return graph.CheckpointVersion{}, err
	}
	return graph.ValidateCheckpointVersion(graph.CheckpointVersion{
		Info: graph.CheckpointVersionInfo{
			ID:             graph.CheckpointVersionID(checkpoint.Key, checkpoint.Revision),
			ParentID:       parentID,
			Origin:         graph.CheckpointVersionOriginCommit,
			Key:            checkpoint.Key,
			Revision:       checkpoint.Revision,
			CheckpointHash: hash,
			CreatedAt:      time.Now().UnixNano(),
		},
		Checkpoint: checkpoint,
	})
}

func validatePair(checkpoint graph.Checkpoint, transition graph.Transition, expected uint64) (graph.Checkpoint, graph.Transition, error) {
	if expected == 0 {
		if checkpoint.Revision != 1 || transition.From != "" {
			return graph.Checkpoint{}, graph.Transition{}, fmt.Errorf("%w: create requires revision one", graph.ErrInvalidCheckpoint)
		}
	} else if checkpoint.Revision != expected+1 {
		return graph.Checkpoint{}, graph.Transition{}, fmt.Errorf("%w: CAS revision", graph.ErrCheckpointConflict)
	}
	validated, err := graph.ValidateCheckpoint(checkpoint)
	if err != nil {
		return graph.Checkpoint{}, graph.Transition{}, err
	}
	validatedTransition, err := graph.ValidateTransition(transition, validated)
	if err != nil {
		return graph.Checkpoint{}, graph.Transition{}, err
	}
	return validated, validatedTransition, nil
}

func encodeCheckpoint(checkpoint graph.Checkpoint) ([]byte, error) {
	raw, err := json.Marshal(checkpoint)
	if err != nil || len(raw) == 0 || len(raw) > maxCheckpointDocumentBytes {
		return nil, fmt.Errorf("%w: checkpoint document size", graph.ErrInvalidCheckpoint)
	}
	return raw, nil
}

func encodeTransition(transition graph.Transition) ([]byte, error) {
	raw, err := json.Marshal(transition)
	if err != nil || len(raw) == 0 || len(raw) > maxTransitionDocumentBytes {
		return nil, fmt.Errorf("%w: transition document size", graph.ErrInvalidTransition)
	}
	return raw, nil
}

func decodeCheckpoint(raw []byte) (graph.Checkpoint, error) {
	if len(raw) == 0 || len(raw) > maxCheckpointDocumentBytes {
		return graph.Checkpoint{}, graph.ErrInvalidCheckpoint
	}
	var checkpoint graph.Checkpoint
	if err := decodeStrict(raw, &checkpoint); err != nil {
		return graph.Checkpoint{}, fmt.Errorf("%w: %v", graph.ErrInvalidCheckpoint, err)
	}
	return graph.ValidateCheckpoint(checkpoint)
}

func decodeTransition(raw []byte) (graph.Transition, error) {
	if len(raw) == 0 || len(raw) > maxTransitionDocumentBytes {
		return graph.Transition{}, graph.ErrInvalidTransition
	}
	var transition graph.Transition
	if err := decodeStrict(raw, &transition); err != nil {
		return graph.Transition{}, fmt.Errorf("%w: %v", graph.ErrInvalidTransition, err)
	}
	checkpoint := graph.Checkpoint{Key: transition.Key, Revision: transition.Revision, Status: transition.To,
		SegmentID: transition.SegmentID, CurrentNodeID: transition.CurrentNodeID, AttemptID: transition.AttemptID}
	return graph.ValidateTransition(transition, checkpoint)
}

func decodeStrict(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func (store *Store) bind(query string) string {
	bound, err := sqlkit.Bind(query, store.dialect)
	if err != nil {
		panic(err)
	}
	return bound
}

func isConstraint(err error) bool {
	if err == nil {
		return false
	}
	var state interface{ SQLState() string }
	if errors.As(err, &state) && state.SQLState() == "23505" {
		return true
	}
	message := bytes.ToLower([]byte(err.Error()))
	return bytes.Contains(message, []byte("unique constraint failed:")) ||
		bytes.Contains(message, []byte("primary key must be unique")) ||
		bytes.Contains(message, []byte("duplicate key value violates unique constraint"))
}
