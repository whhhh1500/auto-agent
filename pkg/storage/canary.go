package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/cc-auto-agent/harness-core/pkg/control"
	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/evaluation"
)

const MaxOpenCanaries = 64

type SQLCanaryStore struct {
	db      *sql.DB
	dialect SQLDialect
	maxOpen int
}

var (
	sqlInsertCanary = sqlQuery{`INSERT INTO profile_canaries
		(id, profile_id, scope, layer_json, revision, base_release_revision, status, basis_points,
		 baseline_evaluation_run_id, candidate_evaluation_run_id, gate_json, release_version, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`}
	sqlGetCanary = sqlQuery{`SELECT id, profile_id, scope, layer_json, revision, base_release_revision, status,
		basis_points, baseline_evaluation_run_id, candidate_evaluation_run_id,
		gate_json, release_version, created_at, updated_at FROM profile_canaries WHERE id = ?`}
	sqlListCanaries = sqlQuery{`SELECT id, profile_id, scope, layer_json, revision, base_release_revision, status,
		basis_points, baseline_evaluation_run_id, candidate_evaluation_run_id,
		gate_json, release_version, created_at, updated_at FROM profile_canaries`}
	sqlListOpenCanaries = sqlQuery{`SELECT id, profile_id, scope, layer_json, revision, base_release_revision, status,
		basis_points, baseline_evaluation_run_id, candidate_evaluation_run_id,
		gate_json, release_version, created_at, updated_at FROM profile_canaries
		WHERE status IN ('active', 'paused', 'promoting') ORDER BY created_at`}
	sqlCountOpenCanaries = sqlQuery{`SELECT COUNT(*) FROM profile_canaries
		WHERE status IN ('active', 'paused', 'promoting')`}
)

func NewSQLCanaryStore(db *sql.DB, dialect SQLDialect) (*SQLCanaryStore, error) {
	if db == nil {
		return nil, fmt.Errorf("canary store requires a database handle")
	}
	if err := validateSQLDialect(dialect); err != nil {
		return nil, err
	}
	return &SQLCanaryStore{db: db, dialect: dialect}, nil
}

func (s *SQLCanaryStore) openCap() int {
	if s != nil && s.maxOpen > 0 {
		return s.maxOpen
	}
	return MaxOpenCanaries
}

func (s *SQLCanaryStore) CreateCanary(ctx context.Context, record control.CanaryRecord) error {
	if err := control.ValidateCanaryRecord(record); err != nil {
		return err
	}
	layerJSON, gateJSON, err := encodeCanary(record)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var open int
	if err := tx.QueryRowContext(ctx, sqlCountOpenCanaries.bind(s.dialect)).Scan(&open); err != nil {
		return err
	}
	if open >= s.openCap() {
		return fmt.Errorf("open canaries exceed maximum of %d", s.openCap())
	}
	_, err = tx.ExecContext(ctx, sqlInsertCanary.bind(s.dialect),
		record.ID, record.ProfileID, record.Scope.String(), layerJSON, record.Revision, record.BaseReleaseRevision,
		string(record.Status), record.BasisPoints, record.BaselineEvaluationRunID,
		record.CandidateEvaluationRunID, gateJSON,
		record.ReleaseVersion,
		record.CreatedAt.UnixMilli(), record.UpdatedAt.UnixMilli(),
	)
	if err != nil {
		return duplicateAsConflict(record.ID, err)
	}
	if err := bumpControlRevision(ctx, tx, s.dialect); err != nil {
		return err
	}
	if err := bumpAuthorizationEpoch(ctx, tx, s.dialect); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SQLCanaryStore) UpdateCanary(ctx context.Context, record control.CanaryRecord, expected ...control.CanaryStatus) (bool, error) {
	if err := control.ValidateCanaryRecord(record); err != nil {
		return false, err
	}
	if len(expected) == 0 {
		return false, fmt.Errorf("canary update requires expected states")
	}
	query := `UPDATE profile_canaries SET status = ?, basis_points = ?, release_version = ?, updated_at = ? WHERE id = ? AND status IN (`
	args := []any{string(record.Status), record.BasisPoints, record.ReleaseVersion, record.UpdatedAt.UnixMilli(), record.ID}
	for index, status := range expected {
		if index > 0 {
			query += ","
		}
		query += "?"
		args = append(args, string(status))
	}
	query += ")"
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, (sqlQuery{query}).bind(s.dialect), args...)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	if err != nil || affected == 0 {
		return false, err
	}
	if err := bumpControlRevision(ctx, tx, s.dialect); err != nil {
		return false, err
	}
	if err := bumpAuthorizationEpoch(ctx, tx, s.dialect); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (s *SQLCanaryStore) ControlRevision(ctx context.Context) (int64, error) {
	return readControlRevision(ctx, s.db, s.dialect)
}

func (s *SQLCanaryStore) GetCanary(ctx context.Context, id string) (control.CanaryRecord, error) {
	if err := core.ValidateRunID(id); err != nil {
		return control.CanaryRecord{}, err
	}
	return scanCanary(s.db.QueryRowContext(ctx, sqlGetCanary.bind(s.dialect), id))
}

func (s *SQLCanaryStore) ListCanaries(ctx context.Context, profileID string, limit int) ([]control.CanaryRecord, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	query := sqlListCanaries.text
	args := []any{}
	if profileID != "" {
		if err := core.ValidateNamespacedID(profileID); err != nil {
			return nil, fmt.Errorf("canary profile id: %w", err)
		}
		query += " WHERE profile_id = ?"
		args = append(args, profileID)
	}
	query += " ORDER BY created_at DESC LIMIT ?"
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, (sqlQuery{query}).bind(s.dialect), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []control.CanaryRecord{}
	for rows.Next() {
		record, err := scanCanary(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	return out, rows.Err()
}

func (s *SQLCanaryStore) ListOpenCanaries(ctx context.Context) ([]control.CanaryRecord, error) {
	rows, err := s.db.QueryContext(ctx, sqlListOpenCanaries.bind(s.dialect))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []control.CanaryRecord{}
	for rows.Next() {
		record, err := scanCanary(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	return out, rows.Err()
}

func scanCanary(scanner runScanner) (control.CanaryRecord, error) {
	var record control.CanaryRecord
	var scope, layerJSON, status, gateJSON string
	var createdMillis, updatedMillis int64
	err := scanner.Scan(
		&record.ID, &record.ProfileID, &scope, &layerJSON, &record.Revision, &record.BaseReleaseRevision, &status,
		&record.BasisPoints, &record.BaselineEvaluationRunID, &record.CandidateEvaluationRunID,
		&gateJSON, &record.ReleaseVersion, &createdMillis, &updatedMillis,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return control.CanaryRecord{}, control.ErrCanaryNotFound
	}
	if err != nil {
		return control.CanaryRecord{}, err
	}
	parsedScope, err := ParseScopePath(scope)
	if err != nil {
		return control.CanaryRecord{}, err
	}
	var layer core.AgentProfileLayer
	if err := json.Unmarshal([]byte(layerJSON), &layer); err != nil {
		return control.CanaryRecord{}, fmt.Errorf("decode canary layer: %w", err)
	}
	layer.Scope = parsedScope
	var gate evaluation.GateResult
	if err := json.Unmarshal([]byte(gateJSON), &gate); err != nil {
		return control.CanaryRecord{}, fmt.Errorf("decode canary gate: %w", err)
	}
	record.Scope, record.Layer, record.Gate = parsedScope, &layer, gate
	record.Status = control.CanaryStatus(status)
	record.CreatedAt = time.UnixMilli(createdMillis).UTC()
	record.UpdatedAt = time.UnixMilli(updatedMillis).UTC()
	if err := control.ValidateCanaryRecord(record); err != nil {
		return control.CanaryRecord{}, err
	}
	return record, nil
}

func encodeCanary(record control.CanaryRecord) (string, string, error) {
	layer := core.CloneAgentProfileLayer(*record.Layer)
	layer.Scope = record.Scope
	layerJSON, err := json.Marshal(layer)
	if err != nil {
		return "", "", err
	}
	gateJSON, err := json.Marshal(record.Gate)
	if err != nil {
		return "", "", err
	}
	if len(layerJSON) > core.MaxProfileFragments*core.MaxPromptFragmentBytes || len(gateJSON) > evaluation.MaxEvaluationMetadata {
		return "", "", fmt.Errorf("canary artifact is too large")
	}
	return string(layerJSON), string(gateJSON), nil
}

var _ control.CanaryStore = (*SQLCanaryStore)(nil)
