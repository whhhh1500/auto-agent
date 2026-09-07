package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/evaluation"
)

type SQLEvaluationStore struct {
	db                 *sql.DB
	dialect            SQLDialect
	maxRunning         int
	maxDatasetVersions int
	maxDatasetIDs      int
	maxRuns            int
	maxCases           int
}

var (
	sqlInsertEvaluationDataset = sqlQuery{`INSERT INTO evaluation_datasets
		(id, version, revision, name, profile_id, case_count, definition_json, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`}
	sqlGetEvaluationDataset   = sqlQuery{`SELECT definition_json FROM evaluation_datasets WHERE id = ? AND version = ?`}
	sqlListEvaluationDatasets = sqlQuery{`SELECT id, version, revision, name, profile_id, case_count, created_at FROM evaluation_datasets`}
	sqlInsertEvaluationRun    = sqlQuery{`INSERT INTO evaluation_runs
		(id, dataset_id, dataset_version, dataset_revision, tenant_id, subject_id, profile_id,
		 baseline_run_id, assignment_revision, status, score, passed, total_cases, passed_cases,
		 allow_capabilities, composition_metadata_json, metadata_json, error_message, created_at, completed_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`}
	sqlUpdateEvaluationRun = sqlQuery{`UPDATE evaluation_runs SET status = ?, score = ?, passed = ?,
		passed_cases = ?, error_message = ?, completed_at = ? WHERE id = ? AND status = 'running'`}
	sqlRecordEvaluationRunError = sqlQuery{`UPDATE evaluation_runs SET error_message = ? WHERE id = ? AND status = 'running'`}
	sqlGetEvaluationRun         = sqlQuery{`SELECT id, dataset_id, dataset_version, dataset_revision,
		tenant_id, subject_id, profile_id, baseline_run_id, status, score, passed,
		total_cases, passed_cases, assignment_revision, allow_capabilities, composition_metadata_json, metadata_json, error_message,
		created_at, completed_at FROM evaluation_runs WHERE id = ?`}
	sqlGetEvaluationRunForUpdate = sqlQuery{sqlGetEvaluationRun.text + ` FOR UPDATE`}
	sqlLockEvaluationRunSQLite   = sqlQuery{`UPDATE evaluation_runs SET id = id WHERE id = ?`}
	sqlListEvaluationRuns        = sqlQuery{`SELECT id, dataset_id, dataset_version, dataset_revision,
		tenant_id, subject_id, profile_id, baseline_run_id, status, score, passed,
		total_cases, passed_cases, assignment_revision, allow_capabilities, composition_metadata_json, metadata_json, error_message,
		created_at, completed_at FROM evaluation_runs`}
	sqlInsertEvaluationCase = sqlQuery{`INSERT INTO evaluation_case_results
		(run_id, case_id, result_json, score, passed, composition_revision, assignment_revision, completed_at)
		SELECT ?, ?, ?, ?, ?, ?, ?, ? WHERE EXISTS (
			SELECT 1 FROM evaluation_runs WHERE id = ? AND status = 'running')`}
	sqlGetEvaluationCase   = sqlQuery{`SELECT result_json FROM evaluation_case_results WHERE run_id = ? AND case_id = ?`}
	sqlListEvaluationCases = sqlQuery{`SELECT result_json FROM evaluation_case_results
		WHERE run_id = ? ORDER BY completed_at, case_id`}
	sqlCountEvaluationCases           = sqlQuery{`SELECT COUNT(*) FROM evaluation_case_results WHERE run_id = ?`}
	sqlCountRunningEvaluationRuns     = sqlQuery{`SELECT COUNT(*) FROM evaluation_runs WHERE status = 'running'`}
	sqlCountEvaluationRuns            = sqlQuery{`SELECT COUNT(*) FROM evaluation_runs`}
	sqlCountEvaluationDatasetVersions = sqlQuery{`SELECT COUNT(*) FROM evaluation_datasets WHERE id = ?`}
	sqlCountEvaluationDatasetIDs      = sqlQuery{`SELECT COUNT(DISTINCT id) FROM evaluation_datasets`}
)

func NewSQLEvaluationStore(db *sql.DB, dialect SQLDialect) (*SQLEvaluationStore, error) {
	if db == nil {
		return nil, fmt.Errorf("evaluation store requires a database handle")
	}
	if err := validateSQLDialect(dialect); err != nil {
		return nil, err
	}
	return &SQLEvaluationStore{db: db, dialect: dialect}, nil
}

func (s *SQLEvaluationStore) runningCap() int {
	if s != nil && s.maxRunning > 0 {
		return s.maxRunning
	}
	return evaluation.MaxRunningEvaluationRuns
}

func (s *SQLEvaluationStore) datasetVersionCap() int {
	if s != nil && s.maxDatasetVersions > 0 {
		return s.maxDatasetVersions
	}
	return evaluation.MaxDatasetVersionsPerID
}

func (s *SQLEvaluationStore) datasetIDCap() int {
	if s != nil && s.maxDatasetIDs > 0 {
		return s.maxDatasetIDs
	}
	return evaluation.MaxDatasetIDs
}

func (s *SQLEvaluationStore) runCap() int {
	if s != nil && s.maxRuns > 0 {
		return s.maxRuns
	}
	return evaluation.MaxEvaluationRuns
}

func (s *SQLEvaluationStore) caseCap() int {
	if s != nil && s.maxCases > 0 {
		return s.maxCases
	}
	return evaluation.MaxDatasetCases
}

func (s *SQLEvaluationStore) PutDataset(ctx context.Context, dataset evaluation.Dataset) (evaluation.Dataset, bool, error) {
	if dataset.CreatedAt.IsZero() {
		dataset.CreatedAt = time.Now().UTC()
	}
	if err := evaluation.ValidateDataset(&dataset); err != nil {
		return evaluation.Dataset{}, false, err
	}
	encoded, err := json.Marshal(dataset)
	if err != nil {
		return evaluation.Dataset{}, false, err
	}
	var versions int
	if err := s.db.QueryRowContext(ctx, sqlCountEvaluationDatasetVersions.bind(s.dialect), dataset.ID).Scan(&versions); err != nil {
		return evaluation.Dataset{}, false, err
	}
	if versions == 0 {
		var ids int
		if err := s.db.QueryRowContext(ctx, sqlCountEvaluationDatasetIDs.bind(s.dialect)).Scan(&ids); err != nil {
			return evaluation.Dataset{}, false, err
		}
		if ids >= s.datasetIDCap() {
			return evaluation.Dataset{}, false, fmt.Errorf("evaluation dataset ids exceed maximum of %d", s.datasetIDCap())
		}
	}
	if versions >= s.datasetVersionCap() {
		existing, getErr := s.GetDataset(ctx, dataset.ID, dataset.Version)
		if getErr == nil {
			if existing.Revision != dataset.Revision {
				return evaluation.Dataset{}, false, fmt.Errorf("dataset %s version %d already exists with another revision", dataset.ID, dataset.Version)
			}
			return existing, false, nil
		}
		if !errors.Is(getErr, evaluation.ErrDatasetNotFound) {
			return evaluation.Dataset{}, false, getErr
		}
		return evaluation.Dataset{}, false, fmt.Errorf("dataset %s versions exceed maximum of %d", dataset.ID, s.datasetVersionCap())
	}
	_, err = s.db.ExecContext(ctx, sqlInsertEvaluationDataset.bind(s.dialect),
		dataset.ID, dataset.Version, dataset.Revision, dataset.Name, dataset.ProfileID,
		len(dataset.Cases), string(encoded), dataset.CreatedAt.UnixMilli(),
	)
	if err == nil {
		return dataset, true, nil
	}
	if !isDuplicateConstraint(err) {
		return evaluation.Dataset{}, false, err
	}
	existing, getErr := s.GetDataset(ctx, dataset.ID, dataset.Version)
	if getErr != nil {
		return evaluation.Dataset{}, false, getErr
	}
	if existing.Revision != dataset.Revision {
		return evaluation.Dataset{}, false, fmt.Errorf("dataset %s version %d already exists with another revision", dataset.ID, dataset.Version)
	}
	return existing, false, nil
}

func (s *SQLEvaluationStore) GetDataset(ctx context.Context, id string, version int) (evaluation.Dataset, error) {
	if err := core.ValidateNamespacedID(id); err != nil {
		return evaluation.Dataset{}, fmt.Errorf("dataset id: %w", err)
	}
	if version < 1 {
		return evaluation.Dataset{}, fmt.Errorf("dataset version must be positive")
	}
	var raw string
	err := s.db.QueryRowContext(ctx, sqlGetEvaluationDataset.bind(s.dialect), id, version).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return evaluation.Dataset{}, evaluation.ErrDatasetNotFound
	}
	if err != nil {
		return evaluation.Dataset{}, err
	}
	return decodeEvaluationDataset(raw)
}

func (s *SQLEvaluationStore) ListDatasets(ctx context.Context, id string, limit int) ([]evaluation.DatasetSummary, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	query := sqlListEvaluationDatasets.text
	args := []any{}
	if id != "" {
		if err := core.ValidateNamespacedID(id); err != nil {
			return nil, fmt.Errorf("dataset id: %w", err)
		}
		query += " WHERE id = ?"
		args = append(args, id)
	}
	query += " ORDER BY id, version DESC LIMIT ?"
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, (sqlQuery{query}).bind(s.dialect), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []evaluation.DatasetSummary{}
	for rows.Next() {
		var summary evaluation.DatasetSummary
		var createdMillis int64
		if err := rows.Scan(
			&summary.ID, &summary.Version, &summary.Revision, &summary.Name,
			&summary.ProfileID, &summary.CaseCount, &createdMillis,
		); err != nil {
			return nil, err
		}
		summary.CreatedAt = time.UnixMilli(createdMillis).UTC()
		out = append(out, summary)
	}
	return out, rows.Err()
}

func (s *SQLEvaluationStore) CreateRun(ctx context.Context, run evaluation.RunResult) error {
	if len(run.Cases) != 0 {
		return fmt.Errorf("new evaluation run already contains case results")
	}
	if !run.CreatedAt.IsZero() {
		run.CreatedAt = time.UnixMilli(run.CreatedAt.UnixMilli()).UTC()
	}
	if err := evaluation.ValidateRunResult(run, false); err != nil {
		return err
	}
	dataset, err := s.getEvaluationDataset(ctx, s.db, run.DatasetID, run.DatasetVersion)
	if err != nil {
		return err
	}
	if err := validateEvaluationRunDataset(run, dataset); err != nil {
		return err
	}
	if run.AssignmentRevision == "" {
		assignmentRevision, revisionErr := core.CompositionMetadataRevision(run.CompositionMetadata)
		if revisionErr != nil {
			return fmt.Errorf("compute evaluation assignment revision: %w", revisionErr)
		}
		run.AssignmentRevision = assignmentRevision
	}
	allowJSON, compositionMetadataJSON, metadataJSON, err := encodeEvaluationRunMaps(run)
	if err != nil {
		return err
	}
	var running int
	if err := s.db.QueryRowContext(ctx, sqlCountRunningEvaluationRuns.bind(s.dialect)).Scan(&running); err != nil {
		return err
	}
	var stored int
	if err := s.db.QueryRowContext(ctx, sqlCountEvaluationRuns.bind(s.dialect)).Scan(&stored); err != nil {
		return err
	}
	if running >= s.runningCap() || stored >= s.runCap() {
		existing, getErr := s.GetRun(ctx, run.ID)
		if getErr == nil {
			if !sameEvaluationRunHeader(existing, run) {
				return fmt.Errorf("evaluation run %s already exists with another definition", run.ID)
			}
			return nil
		}
		if !errors.Is(getErr, evaluation.ErrRunNotFound) {
			return getErr
		}
		if stored >= s.runCap() {
			return fmt.Errorf("evaluation runs exceed maximum of %d", s.runCap())
		}
		return fmt.Errorf("running evaluation runs exceed maximum of %d", s.runningCap())
	}
	_, err = s.db.ExecContext(ctx, sqlInsertEvaluationRun.bind(s.dialect),
		run.ID, run.DatasetID, run.DatasetVersion, run.DatasetRevision,
		run.TenantID, run.SubjectID, run.ProfileID, run.BaselineRunID,
		run.AssignmentRevision, string(run.Status), run.Score, boolInt(run.Passed), run.TotalCases, run.PassedCases,
		allowJSON, compositionMetadataJSON, metadataJSON, run.Error, run.CreatedAt.UnixMilli(), timeMillis(run.CompletedAt),
	)
	if err == nil {
		return nil
	}
	if !isDuplicateConstraint(err) {
		return err
	}
	existing, getErr := s.GetRun(ctx, run.ID)
	if getErr != nil {
		return getErr
	}
	if !sameEvaluationRunHeader(existing, run) {
		return fmt.Errorf("evaluation run %s already exists with another definition", run.ID)
	}
	return nil
}

func (s *SQLEvaluationStore) RecordCaseResult(ctx context.Context, runID string, result evaluation.CaseResult) error {
	if err := core.ValidateRunID(runID); err != nil {
		return fmt.Errorf("evaluation run id: %w", err)
	}
	if err := evaluation.ValidateCaseResult(result); err != nil {
		return err
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	run, err := s.lockEvaluationRun(ctx, tx, runID)
	if err != nil {
		return err
	}
	dataset, err := s.getEvaluationDataset(ctx, tx, run.DatasetID, run.DatasetVersion)
	if err != nil {
		return err
	}
	if err := validateEvaluationRunDataset(run, dataset); err != nil {
		return err
	}
	if !evaluationDatasetHasCase(dataset, result.CaseID) {
		return fmt.Errorf("evaluation case result %s is outside dataset", result.CaseID)
	}
	var existing string
	getErr := tx.QueryRowContext(ctx, sqlGetEvaluationCase.bind(s.dialect), runID, result.CaseID).Scan(&existing)
	if getErr == nil {
		if !sameEvaluationCaseResultJSON(existing, result) {
			return fmt.Errorf("evaluation case result %s already exists with another value", result.CaseID)
		}
		return tx.Commit()
	}
	if !errors.Is(getErr, sql.ErrNoRows) {
		return getErr
	}
	if run.Status != evaluation.RunRunning {
		return fmt.Errorf("evaluation run %s is not accepting case results", runID)
	}
	var cases int
	if err := tx.QueryRowContext(ctx, sqlCountEvaluationCases.bind(s.dialect), runID).Scan(&cases); err != nil {
		return err
	}
	if cases >= s.caseCap() {
		return fmt.Errorf("evaluation case results exceed maximum of %d", s.caseCap())
	}
	insertResult, err := tx.ExecContext(ctx, sqlInsertEvaluationCase.bind(s.dialect),
		runID, result.CaseID, string(encoded), result.Score, boolInt(result.Passed),
		result.Artifacts.CompositionRevision, result.Artifacts.AssignmentRevision, result.CompletedAt.UnixMilli(), runID,
	)
	if err != nil {
		return err
	}
	affected, err := insertResult.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return fmt.Errorf("evaluation run %s is not accepting case results", runID)
	}
	return tx.Commit()
}

func (s *SQLEvaluationStore) RecordRunError(ctx context.Context, runID, message string) error {
	if err := core.ValidateRunID(runID); err != nil {
		return fmt.Errorf("evaluation run id: %w", err)
	}
	if len(message) > evaluation.MaxEvaluationText {
		message = message[:evaluation.MaxEvaluationText]
	}
	result, err := s.db.ExecContext(ctx, sqlRecordEvaluationRunError.bind(s.dialect), message, runID)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected > 0 {
		return nil
	}
	run, err := s.GetRun(ctx, runID)
	if errors.Is(err, evaluation.ErrRunNotFound) {
		return evaluation.ErrRunNotFound
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("evaluation run %s is already %s", runID, run.Status)
}

func (s *SQLEvaluationStore) FinishRun(ctx context.Context, run evaluation.RunResult) error {
	if !run.CompletedAt.IsZero() {
		run.CompletedAt = time.UnixMilli(run.CompletedAt.UTC().UnixMilli()).UTC()
	}
	if err := evaluation.ValidateRunResult(run, true); err != nil {
		return err
	}
	if run.AssignmentRevision == "" {
		assignmentRevision, err := core.CompositionMetadataRevision(run.CompositionMetadata)
		if err != nil {
			return fmt.Errorf("compute evaluation assignment revision: %w", err)
		}
		run.AssignmentRevision = assignmentRevision
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	existing, err := s.lockEvaluationRun(ctx, tx, run.ID)
	if err != nil {
		return err
	}
	dataset, err := s.getEvaluationDataset(ctx, tx, existing.DatasetID, existing.DatasetVersion)
	if err != nil {
		return err
	}
	if err := validateEvaluationRunDataset(existing, dataset); err != nil {
		return err
	}
	if !sameEvaluationRunHeader(existing, run) {
		return fmt.Errorf("evaluation run %s has another immutable definition", run.ID)
	}
	storedCases, err := s.listStoredEvaluationCaseResults(ctx, tx, run.ID)
	if err != nil {
		return err
	}
	cases := evaluationCaseResults(storedCases)
	if err := validateEvaluationCaseMembers(dataset, cases, run.Status == evaluation.RunCompleted); err != nil {
		return err
	}
	if len(run.Cases) > 0 && !sameEvaluationCaseResults(storedCases, run.Cases) {
		return fmt.Errorf("evaluation run %s case results differ from durable state", run.ID)
	}
	if existing.Status != evaluation.RunRunning {
		if existing.Status == run.Status && existing.Score == run.Score && existing.Passed == run.Passed &&
			existing.PassedCases == run.PassedCases && existing.Error == run.Error {
			return tx.Commit()
		}
		return fmt.Errorf("evaluation run %s is already terminal with another result", run.ID)
	}
	result, err := tx.ExecContext(ctx, sqlUpdateEvaluationRun.bind(s.dialect),
		string(run.Status), run.Score, boolInt(run.Passed), run.PassedCases,
		run.Error, run.CompletedAt.UnixMilli(), run.ID,
	)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected > 0 {
		return tx.Commit()
	}
	return fmt.Errorf("evaluation run %s is not running", run.ID)
}

func (s *SQLEvaluationStore) GetRun(ctx context.Context, id string) (evaluation.RunResult, error) {
	if err := core.ValidateRunID(id); err != nil {
		return evaluation.RunResult{}, fmt.Errorf("evaluation run id: %w", err)
	}
	options := &sql.TxOptions{}
	if s.dialect == SQLDialectPostgres {
		options.Isolation = sql.LevelRepeatableRead
	}
	tx, err := s.db.BeginTx(ctx, options)
	if err != nil {
		return evaluation.RunResult{}, err
	}
	defer func() { _ = tx.Rollback() }()
	run, err := scanEvaluationRun(tx.QueryRowContext(ctx, sqlGetEvaluationRun.bind(s.dialect), id))
	if err != nil {
		return evaluation.RunResult{}, err
	}
	dataset, err := s.getEvaluationDataset(ctx, tx, run.DatasetID, run.DatasetVersion)
	if err != nil {
		return evaluation.RunResult{}, err
	}
	if err := validateEvaluationRunDataset(run, dataset); err != nil {
		return evaluation.RunResult{}, err
	}
	storedCases, err := s.listStoredEvaluationCaseResults(ctx, tx, id)
	if err != nil {
		return evaluation.RunResult{}, err
	}
	cases := evaluationCaseResults(storedCases)
	run.Cases = cases
	if err := validateEvaluationCaseMembers(dataset, cases, run.Status == evaluation.RunCompleted); err != nil {
		return evaluation.RunResult{}, err
	}
	if err := evaluation.ValidateRunResult(run, run.Status != evaluation.RunRunning); err != nil {
		return evaluation.RunResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return evaluation.RunResult{}, err
	}
	return run, nil
}

func (s *SQLEvaluationStore) ListRuns(ctx context.Context, datasetID, tenantID string, limit int) ([]evaluation.RunResult, error) {
	return s.QueryRuns(ctx, evaluation.RunQuery{DatasetID: datasetID, TenantID: tenantID, Limit: limit})
}

func (s *SQLEvaluationStore) QueryRuns(ctx context.Context, filter evaluation.RunQuery) ([]evaluation.RunResult, error) {
	if err := filter.Validate(); err != nil {
		return nil, err
	}
	if filter.Limit <= 0 {
		filter.Limit = 100
	}
	if filter.Limit > 500 {
		filter.Limit = 500
	}
	query := sqlListEvaluationRuns.text
	clauses := []string{}
	args := []any{}
	if filter.DatasetID != "" {
		clauses = append(clauses, "dataset_id = ?")
		args = append(args, filter.DatasetID)
	}
	if filter.TenantID != "" {
		clauses = append(clauses, "tenant_id = ?")
		args = append(args, filter.TenantID)
	}
	if filter.CompositionRevision != "" {
		clauses = append(clauses, "EXISTS (SELECT 1 FROM evaluation_case_results AS composition_cases WHERE composition_cases.run_id = evaluation_runs.id AND composition_cases.composition_revision = ?)")
		args = append(args, filter.CompositionRevision)
	}
	if filter.AssignmentRevision != "" {
		clauses = append(clauses, "(assignment_revision = ? OR EXISTS (SELECT 1 FROM evaluation_case_results AS assignment_cases WHERE assignment_cases.run_id = evaluation_runs.id AND assignment_cases.assignment_revision = ?))")
		args = append(args, filter.AssignmentRevision, filter.AssignmentRevision)
	}
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " ORDER BY created_at DESC LIMIT ?"
	args = append(args, filter.Limit)
	rows, err := s.db.QueryContext(ctx, (sqlQuery{query}).bind(s.dialect), args...)
	if err != nil {
		return nil, err
	}
	out := []evaluation.RunResult{}
	for rows.Next() {
		run, err := scanEvaluationRun(rows)
		if err != nil {
			_ = rows.Close()
			return nil, err
		}
		out = append(out, run)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	for index := range out {
		out[index].Cases = nil
		if err := evaluation.ValidateRunResult(out[index], out[index].Status != evaluation.RunRunning); err != nil {
			return nil, err
		}
	}
	return out, nil
}

type evaluationSQLExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (s *SQLEvaluationStore) getEvaluationDataset(ctx context.Context, executor evaluationSQLExecutor, id string, version int) (evaluation.Dataset, error) {
	var raw string
	err := executor.QueryRowContext(ctx, sqlGetEvaluationDataset.bind(s.dialect), id, version).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return evaluation.Dataset{}, evaluation.ErrDatasetNotFound
	}
	if err != nil {
		return evaluation.Dataset{}, err
	}
	return decodeEvaluationDataset(raw)
}

func (s *SQLEvaluationStore) lockEvaluationRun(ctx context.Context, tx *sql.Tx, id string) (evaluation.RunResult, error) {
	if s.dialect == SQLDialectSQLite {
		result, err := tx.ExecContext(ctx, sqlLockEvaluationRunSQLite.bind(s.dialect), id)
		if err != nil {
			return evaluation.RunResult{}, err
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return evaluation.RunResult{}, err
		}
		if affected == 0 {
			return evaluation.RunResult{}, evaluation.ErrRunNotFound
		}
		return scanEvaluationRun(tx.QueryRowContext(ctx, sqlGetEvaluationRun.bind(s.dialect), id))
	}
	return scanEvaluationRun(tx.QueryRowContext(ctx, sqlGetEvaluationRunForUpdate.bind(s.dialect), id))
}

type storedEvaluationCaseResult struct {
	raw    string
	result evaluation.CaseResult
}

func (s *SQLEvaluationStore) listStoredEvaluationCaseResults(ctx context.Context, executor evaluationSQLExecutor, runID string) ([]storedEvaluationCaseResult, error) {
	rows, err := executor.QueryContext(ctx, sqlListEvaluationCases.bind(s.dialect), runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []storedEvaluationCaseResult{}
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var result evaluation.CaseResult
		if err := json.Unmarshal([]byte(raw), &result); err != nil {
			return nil, fmt.Errorf("decode evaluation case result: %w", err)
		}
		if err := evaluation.ValidateCaseResult(result); err != nil {
			return nil, err
		}
		out = append(out, storedEvaluationCaseResult{raw: raw, result: result})
	}
	return out, rows.Err()
}

func evaluationCaseResults(stored []storedEvaluationCaseResult) []evaluation.CaseResult {
	results := make([]evaluation.CaseResult, 0, len(stored))
	for _, item := range stored {
		results = append(results, item.result)
	}
	return results
}

func validateEvaluationRunDataset(run evaluation.RunResult, dataset evaluation.Dataset) error {
	if run.DatasetID != dataset.ID || run.DatasetVersion != dataset.Version || run.DatasetRevision != dataset.Revision {
		return fmt.Errorf("evaluation run dataset definition does not match durable dataset")
	}
	if run.TotalCases != len(dataset.Cases) {
		return fmt.Errorf("evaluation run total cases does not match durable dataset")
	}
	return nil
}

func evaluationDatasetHasCase(dataset evaluation.Dataset, caseID string) bool {
	for _, item := range dataset.Cases {
		if item.ID == caseID {
			return true
		}
	}
	return false
}

func validateEvaluationCaseMembers(dataset evaluation.Dataset, cases []evaluation.CaseResult, requireComplete bool) error {
	members := make(map[string]struct{}, len(dataset.Cases))
	for _, item := range dataset.Cases {
		members[item.ID] = struct{}{}
	}
	if requireComplete && len(cases) != len(members) {
		return fmt.Errorf("evaluation run has %d of %d case results", len(cases), len(members))
	}
	seen := make(map[string]struct{}, len(cases))
	for _, result := range cases {
		if _, ok := members[result.CaseID]; !ok {
			return fmt.Errorf("evaluation run contains case %s outside dataset", result.CaseID)
		}
		if _, duplicate := seen[result.CaseID]; duplicate {
			return fmt.Errorf("evaluation run contains duplicate case %s", result.CaseID)
		}
		seen[result.CaseID] = struct{}{}
	}
	if requireComplete && len(seen) != len(members) {
		return fmt.Errorf("evaluation run does not contain every durable dataset case")
	}
	return nil
}

func sameEvaluationCaseResults(left []storedEvaluationCaseResult, right []evaluation.CaseResult) bool {
	if len(left) != len(right) {
		return false
	}
	byID := make(map[string]storedEvaluationCaseResult, len(left))
	for _, item := range left {
		if _, duplicate := byID[item.result.CaseID]; duplicate {
			return false
		}
		byID[item.result.CaseID] = item
	}
	for _, result := range right {
		stored, ok := byID[result.CaseID]
		if !ok {
			return false
		}
		if !sameEvaluationCaseResultJSON(stored.raw, result) {
			return false
		}
	}
	return true
}

func sameEvaluationCaseResultJSON(raw string, result evaluation.CaseResult) bool {
	expected, err := json.Marshal(result)
	if err != nil {
		return false
	}
	return sameEvaluationCaseJSON([]byte(raw), expected)
}

func sameEvaluationCaseJSON(left, right []byte) bool {
	decode := func(raw []byte) (any, error) {
		decoder := json.NewDecoder(strings.NewReader(string(raw)))
		decoder.UseNumber()
		var value any
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		var trailing any
		if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("multiple JSON values")
		}
		return value, nil
	}
	leftValue, leftErr := decode(left)
	rightValue, rightErr := decode(right)
	if leftErr != nil || rightErr != nil {
		return false
	}
	leftJSON, leftErr := json.Marshal(leftValue)
	rightJSON, rightErr := json.Marshal(rightValue)
	return leftErr == nil && rightErr == nil && string(leftJSON) == string(rightJSON)
}

func scanEvaluationRun(scanner runScanner) (evaluation.RunResult, error) {
	var run evaluation.RunResult
	var status, assignmentRevision, allowJSON, compositionMetadataJSON, metadataJSON string
	var passed int
	var createdMillis, completedMillis int64
	err := scanner.Scan(
		&run.ID, &run.DatasetID, &run.DatasetVersion, &run.DatasetRevision,
		&run.TenantID, &run.SubjectID, &run.ProfileID, &run.BaselineRunID,
		&status, &run.Score, &passed, &run.TotalCases, &run.PassedCases, &assignmentRevision,
		&allowJSON, &compositionMetadataJSON, &metadataJSON, &run.Error, &createdMillis, &completedMillis,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return evaluation.RunResult{}, evaluation.ErrRunNotFound
	}
	if err != nil {
		return evaluation.RunResult{}, err
	}
	run.AssignmentRevision = assignmentRevision
	run.Status, run.Passed = evaluation.RunStatus(status), passed != 0
	run.CreatedAt = time.UnixMilli(createdMillis).UTC()
	if completedMillis > 0 {
		run.CompletedAt = time.UnixMilli(completedMillis).UTC()
	}
	if err := json.Unmarshal([]byte(allowJSON), &run.AllowCapabilities); err != nil {
		return evaluation.RunResult{}, fmt.Errorf("decode evaluation allowlist: %w", err)
	}
	if err := json.Unmarshal([]byte(compositionMetadataJSON), &run.CompositionMetadata); err != nil {
		return evaluation.RunResult{}, fmt.Errorf("decode evaluation composition metadata: %w", err)
	}
	if err := json.Unmarshal([]byte(metadataJSON), &run.Metadata); err != nil {
		return evaluation.RunResult{}, fmt.Errorf("decode evaluation metadata: %w", err)
	}
	return run, nil
}

func decodeEvaluationDataset(raw string) (evaluation.Dataset, error) {
	var dataset evaluation.Dataset
	if err := json.Unmarshal([]byte(raw), &dataset); err != nil {
		return evaluation.Dataset{}, fmt.Errorf("decode evaluation dataset: %w", err)
	}
	if err := evaluation.ValidateDataset(&dataset); err != nil {
		return evaluation.Dataset{}, err
	}
	return dataset, nil
}

func encodeEvaluationRunMaps(run evaluation.RunResult) (string, string, string, error) {
	allow, err := json.Marshal(run.AllowCapabilities)
	if err != nil {
		return "", "", "", err
	}
	compositionMetadata, err := json.Marshal(run.CompositionMetadata)
	if err != nil {
		return "", "", "", err
	}
	metadata, err := json.Marshal(run.Metadata)
	if err != nil {
		return "", "", "", err
	}
	return string(allow), string(compositionMetadata), string(metadata), nil
}

func sameEvaluationRunHeader(left, right evaluation.RunResult) bool {
	left.Cases, right.Cases = nil, nil
	// CreatedAt is assigned when the run is first accepted and is persisted
	// independently of the replay request. It is not part of the immutable run
	// definition, so comparing it would reject an otherwise identical retry
	// whenever the caller supplies a fresh timestamp.
	left.CreatedAt, right.CreatedAt = time.Time{}, time.Time{}
	left.CompletedAt, right.CompletedAt = time.Time{}, time.Time{}
	left.Status, right.Status = evaluation.RunRunning, evaluation.RunRunning
	left.Score, right.Score = 0, 0
	left.Passed, right.Passed = false, false
	left.PassedCases, right.PassedCases = 0, 0
	left.Error, right.Error = "", ""
	encodedLeft, _ := json.Marshal(left)
	encodedRight, _ := json.Marshal(right)
	return string(encodedLeft) == string(encodedRight)
}

func timeMillis(value time.Time) int64 {
	if value.IsZero() {
		return 0
	}
	return value.UTC().UnixMilli()
}

var _ evaluation.Store = (*SQLEvaluationStore)(nil)
var _ evaluation.QueryStore = (*SQLEvaluationStore)(nil)
