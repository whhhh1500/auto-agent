package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"strings"
)

// backfillEvaluationRevisions upgrades v19 rows whose canonical JSON already
// contains Composition/Assignment evidence. The indexed columns are derived
// projections; result_json and composition_metadata_json remain canonical.
func backfillEvaluationRevisions(ctx context.Context, db sqlSchemaExecutor, dialect SQLDialect) error {
	runs, err := db.QueryContext(ctx, "SELECT id, composition_metadata_json FROM evaluation_runs")
	if err != nil {
		return err
	}
	for runs.Next() {
		var id, metadataJSON string
		if err := runs.Scan(&id, &metadataJSON); err != nil {
			_ = runs.Close()
			return err
		}
		if metadataJSON == "" || metadataJSON == "{}" || metadataJSON == "null" {
			continue
		}
		var metadata map[string]string
		if err := json.Unmarshal([]byte(metadataJSON), &metadata); err != nil {
			_ = runs.Close()
			return fmt.Errorf("decode run %s composition metadata: %w", id, err)
		}
		revision, err := core.CompositionMetadataRevision(metadata)
		if err != nil {
			_ = runs.Close()
			return fmt.Errorf("compute run %s assignment revision: %w", id, err)
		}
		if revision == "" {
			continue
		}
		if _, err := db.ExecContext(ctx,
			(sqlQuery{"UPDATE evaluation_runs SET assignment_revision = ? WHERE id = ? AND assignment_revision = ''"}).bind(dialect),
			revision, id,
		); err != nil {
			_ = runs.Close()
			return err
		}
	}
	if err := runs.Err(); err != nil {
		_ = runs.Close()
		return err
	}
	if err := runs.Close(); err != nil {
		return err
	}

	cases, err := db.QueryContext(ctx, "SELECT run_id, case_id, result_json FROM evaluation_case_results")
	if err != nil {
		return err
	}
	for cases.Next() {
		var runID, caseID, resultJSON string
		if err := cases.Scan(&runID, &caseID, &resultJSON); err != nil {
			_ = cases.Close()
			return err
		}
		var result struct {
			Artifacts struct {
				CompositionRevision string `json:"composition_revision"`
				AssignmentRevision  string `json:"assignment_revision"`
			} `json:"artifacts"`
		}
		if err := json.Unmarshal([]byte(resultJSON), &result); err != nil {
			_ = cases.Close()
			return fmt.Errorf("decode case %s/%s artifact: %w", runID, caseID, err)
		}
		if _, err := db.ExecContext(ctx,
			(sqlQuery{"UPDATE evaluation_case_results SET composition_revision = ?, assignment_revision = ? WHERE run_id = ? AND case_id = ? AND composition_revision = '' AND assignment_revision = ''"}).bind(dialect),
			result.Artifacts.CompositionRevision, result.Artifacts.AssignmentRevision, runID, caseID,
		); err != nil {
			_ = cases.Close()
			return err
		}
	}
	if err := cases.Err(); err != nil {
		_ = cases.Close()
		return err
	}
	return cases.Close()
}

type sessionEvidenceRow struct {
	ID        string
	TenantID  string
	SubjectID string
	ProfileID string
	Header    string
}

// backfillRunEvidence rebuilds the query projection from canonical Session
// chunks. It is intentionally idempotent: the unique segment key makes a
// repeated migration harmless.
func backfillRunEvidence(ctx context.Context, db sqlSchemaExecutor, dialect SQLDialect) error {
	rows, err := db.QueryContext(ctx, "SELECT id, tenant_id, user_id, profile_id, header FROM sessions")
	if err != nil {
		return err
	}
	sessions := []sessionEvidenceRow{}
	for rows.Next() {
		var row sessionEvidenceRow
		if err := rows.Scan(&row.ID, &row.TenantID, &row.SubjectID, &row.ProfileID, &row.Header); err != nil {
			_ = rows.Close()
			return err
		}
		sessions = append(sessions, row)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, session := range sessions {
		var header core.SessionOptions
		if err := json.Unmarshal([]byte(session.Header), &header); err != nil {
			return fmt.Errorf("decode session %s header for evidence: %w", session.ID, err)
		}
		header.ID, header.ProfileID = session.ID, session.ProfileID
		chunks, err := db.QueryContext(ctx,
			(sqlQuery{"SELECT start_seq, payload FROM event_chunks WHERE session_id = ? ORDER BY start_seq"}).bind(dialect), session.ID,
		)
		if err != nil {
			return err
		}
		events := []core.SessionEvent{}
		for chunks.Next() {
			var startSeq int64
			var payload string
			if err := chunks.Scan(&startSeq, &payload); err != nil {
				_ = chunks.Close()
				return err
			}
			for _, line := range strings.Split(payload, "\n") {
				if strings.TrimSpace(line) == "" {
					continue
				}
				var event core.SessionEvent
				if err := json.Unmarshal([]byte(line), &event); err != nil {
					_ = chunks.Close()
					return fmt.Errorf("decode session %s event %d for evidence: %w", session.ID, startSeq, err)
				}
				events = append(events, event)
			}
		}
		if err := chunks.Err(); err != nil {
			_ = chunks.Close()
			return err
		}
		if err := chunks.Close(); err != nil {
			return err
		}
		if err := insertRunEvidence(ctx, db, dialect, session.ID, header, events, MaxEvidenceRecords); err != nil {
			return err
		}
	}
	return nil
}

var (
	sqlCountRunEvidence = sqlQuery{"SELECT COUNT(*) FROM run_evidence"}
	sqlGetRunEvidence   = sqlQuery{"SELECT 1 FROM run_evidence WHERE session_id = ? AND run_id = ? AND segment_seq = ?"}
)

func insertRunEvidence(ctx context.Context, dbOrTx interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, dialect SQLDialect, sessionID string, header core.SessionOptions, events []core.SessionEvent, maxRecords int) error {
	records, err := deriveRunEvidence(sessionID, header, events)
	if err != nil {
		return err
	}
	if maxRecords <= 0 {
		maxRecords = MaxEvidenceRecords
	}
	var stored int
	if err := dbOrTx.QueryRowContext(ctx, sqlCountRunEvidence.bind(dialect)).Scan(&stored); err != nil {
		return err
	}
	query := sqlQuery{`INSERT INTO run_evidence
		(session_id, run_id, segment_seq, kind, tenant_id, subject_id, profile_id,
		 composition_revision, assignment_revision, assignment_variant, status, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (session_id, run_id, segment_seq) DO UPDATE SET
		 kind = excluded.kind, tenant_id = excluded.tenant_id,
		 subject_id = excluded.subject_id, profile_id = excluded.profile_id,
		 composition_revision = excluded.composition_revision,
		 assignment_revision = excluded.assignment_revision,
		 assignment_variant = excluded.assignment_variant,
		 status = excluded.status`}.bind(dialect)
	existsQuery := sqlGetRunEvidence.bind(dialect)
	for _, record := range records {
		var existing int
		getErr := dbOrTx.QueryRowContext(ctx, existsQuery, record.SessionID, record.ID, record.SegmentSeq).Scan(&existing)
		if getErr != nil && !errors.Is(getErr, sql.ErrNoRows) {
			return getErr
		}
		if errors.Is(getErr, sql.ErrNoRows) {
			if stored >= maxRecords {
				return fmt.Errorf("run evidence exceeds maximum of %d records", maxRecords)
			}
			stored++
		}
		if _, err := dbOrTx.ExecContext(ctx, query,
			record.SessionID, record.ID, record.SegmentSeq, string(record.Kind), record.TenantID,
			record.SubjectID, record.ProfileID, record.CompositionRevision, record.AssignmentRevision,
			string(record.AssignmentVariant), record.Status, record.CreatedAt.UnixMilli(),
		); err != nil {
			return err
		}
	}
	statusQuery := sqlQuery{"UPDATE run_evidence SET status = ? WHERE session_id = ? AND run_id = ?"}.bind(dialect)
	for runID, status := range evidenceStatusTransitions(events) {
		if _, err := dbOrTx.ExecContext(ctx, statusQuery, status, sessionID, runID); err != nil {
			return err
		}
	}
	return nil
}
