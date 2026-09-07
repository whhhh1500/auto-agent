package storage

import (
	"context"
	"database/sql"
	"fmt"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/extensions/rag"
)

const ragProjectionBatchSize = 16

// RagProjectionStats describes canonical RAG documents and their derived
// token/tag projections. Orphan counts identify rows without a canonical document.
type RagProjectionStats struct {
	CanonicalDocuments int64 `json:"canonical_documents"`
	TokenRows          int64 `json:"token_rows"`
	TagRows            int64 `json:"tag_rows"`
	TokenDocuments     int64 `json:"token_documents"`
	TagDocuments       int64 `json:"tag_documents"`
	OrphanTokenRows    int64 `json:"orphan_token_rows"`
	OrphanTagRows      int64 `json:"orphan_tag_rows"`
}

// RagProjectionMaintainer is implemented by RAG indexes that can inspect and
// rebuild derived SQL projections from canonical documents.
type RagProjectionMaintainer interface {
	ProjectionStats(ctx context.Context) (RagProjectionStats, error)
	RebuildProjection(ctx context.Context) (RagProjectionStats, error)
}

var ragProjectionSchemaStatements = []string{
	`CREATE TABLE IF NOT EXISTS rag_document_tokens (
		scope       TEXT NOT NULL,
		document_id TEXT NOT NULL,
		token       TEXT NOT NULL,
		PRIMARY KEY (scope, document_id, token)
	)`,
	`CREATE INDEX IF NOT EXISTS rag_tokens_lookup
		ON rag_document_tokens (token, scope, document_id)`,
	`CREATE TABLE IF NOT EXISTS rag_document_tags (
		scope       TEXT NOT NULL,
		document_id TEXT NOT NULL,
		tag         TEXT NOT NULL,
		PRIMARY KEY (scope, document_id, tag)
	)`,
	`CREATE INDEX IF NOT EXISTS rag_tags_lookup
		ON rag_document_tags (tag, scope, document_id)`,
}

var (
	sqlRagProjectionFirstBatch = sqlQuery{`SELECT scope, id, source, content, tags_json
		FROM rag_documents ORDER BY scope, id LIMIT ?`}
	sqlRagProjectionNextBatch = sqlQuery{`SELECT scope, id, source, content, tags_json
		FROM rag_documents
		WHERE scope > ? OR (scope = ? AND id > ?)
		ORDER BY scope, id LIMIT ?`}
	sqlRagProjectionInsertToken = sqlQuery{`INSERT INTO rag_document_tokens (scope, document_id, token)
		VALUES (?, ?, ?)`}
	sqlRagProjectionInsertTag = sqlQuery{`INSERT INTO rag_document_tags (scope, document_id, tag)
		VALUES (?, ?, ?)`}
	sqlRagProjectionStats = `SELECT
		(SELECT COUNT(*) FROM rag_documents),
		(SELECT COUNT(*) FROM rag_document_tokens),
		(SELECT COUNT(*) FROM rag_document_tags),
		(SELECT COUNT(*) FROM (
			SELECT scope, document_id FROM rag_document_tokens GROUP BY scope, document_id
		) AS token_documents),
		(SELECT COUNT(*) FROM (
			SELECT scope, document_id FROM rag_document_tags GROUP BY scope, document_id
		) AS tag_documents),
		(SELECT COUNT(*) FROM rag_document_tokens AS tokens
			WHERE NOT EXISTS (
				SELECT 1 FROM rag_documents AS documents
				WHERE documents.scope = tokens.scope AND documents.id = tokens.document_id
			)),
		(SELECT COUNT(*) FROM rag_document_tags AS tags
			WHERE NOT EXISTS (
				SELECT 1 FROM rag_documents AS documents
				WHERE documents.scope = tags.scope AND documents.id = tags.document_id
			))`
)

// ProjectionStats reads only aggregate counts and grouping keys in one
// read-only transaction. It intentionally never reads content or tags_json.
func (s *SQLRagIndex) ProjectionStats(ctx context.Context) (RagProjectionStats, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return RagProjectionStats{}, fmt.Errorf("begin rag projection stats: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	stats, err := ragProjectionStatsTx(ctx, tx)
	if err != nil {
		return RagProjectionStats{}, err
	}
	if err := tx.Commit(); err != nil {
		return RagProjectionStats{}, fmt.Errorf("commit rag projection stats: %w", err)
	}
	return stats, nil
}

// RebuildProjection atomically replaces derived projections from canonical
// rag_documents and returns statistics for the committed replacement.
func (s *SQLRagIndex) RebuildProjection(ctx context.Context) (RagProjectionStats, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return RagProjectionStats{}, fmt.Errorf("begin rag projection rebuild: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := rebuildRagProjectionTx(ctx, tx, s.dialect); err != nil {
		return RagProjectionStats{}, err
	}
	stats, err := ragProjectionStatsTx(ctx, tx)
	if err != nil {
		return RagProjectionStats{}, err
	}
	if stats.OrphanTokenRows != 0 || stats.OrphanTagRows != 0 {
		return RagProjectionStats{}, fmt.Errorf("rebuilt rag projection has orphan rows: tokens=%d tags=%d", stats.OrphanTokenRows, stats.OrphanTagRows)
	}
	if err := tx.Commit(); err != nil {
		return RagProjectionStats{}, fmt.Errorf("commit rag projection rebuild: %w", err)
	}
	return stats, nil
}

type ragProjectionExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func ensureRagProjectionSchema(ctx context.Context, exec ragProjectionExecutor) error {
	for _, statement := range ragProjectionSchemaStatements {
		if _, err := exec.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("ensure rag projection schema: %w", err)
		}
	}
	return nil
}

// rebuildRagProjectionTx requires a transaction-scoped executor. SQL callers
// use *sql.Tx; the v47 tokenizer migration uses a SQLite connection after
// BEGIN IMMEDIATE so acquiring the write lock and replacing the projection are
// one transaction.
func rebuildRagProjectionTx(ctx context.Context, exec ragProjectionExecutor, dialect SQLDialect) error {
	if err := ensureRagProjectionSchema(ctx, exec); err != nil {
		return err
	}
	if dialect == SQLDialectPostgres {
		if _, err := exec.ExecContext(ctx, `LOCK TABLE rag_documents, rag_document_tokens, rag_document_tags
			IN SHARE ROW EXCLUSIVE MODE`); err != nil {
			return fmt.Errorf("lock rag projection tables: %w", err)
		}
	}

	// On SQLite, the first DELETE obtains the write lock before any canonical
	// document SELECT runs.
	if _, err := exec.ExecContext(ctx, "DELETE FROM rag_document_tokens"); err != nil {
		return fmt.Errorf("clear rag token projection: %w", err)
	}
	if _, err := exec.ExecContext(ctx, "DELETE FROM rag_document_tags"); err != nil {
		return fmt.Errorf("clear rag tag projection: %w", err)
	}

	var cursor *ragProjectionRow
	for {
		documents, err := readRagProjectionBatch(ctx, exec, dialect, cursor)
		if err != nil {
			return err
		}
		if len(documents) == 0 {
			return nil
		}
		if err := insertRagProjectionBatch(ctx, exec, dialect, documents); err != nil {
			return err
		}
		last := documents[len(documents)-1]
		cursor = &ragProjectionRow{Scope: last.Scope, ID: last.ID}
	}
}

func readRagProjectionBatch(ctx context.Context, exec ragProjectionExecutor, dialect SQLDialect, cursor *ragProjectionRow) ([]ragProjectionRow, error) {
	query := sqlRagProjectionFirstBatch.bind(dialect)
	args := []any{ragProjectionBatchSize}
	if cursor != nil {
		query = sqlRagProjectionNextBatch.bind(dialect)
		args = []any{cursor.Scope, cursor.Scope, cursor.ID, ragProjectionBatchSize}
	}
	rows, err := exec.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("read rag documents for projection: %w", err)
	}

	documents := make([]ragProjectionRow, 0, ragProjectionBatchSize)
	for rows.Next() {
		var document ragProjectionRow
		if err := rows.Scan(&document.Scope, &document.ID, &document.Source, &document.Content, &document.TagsJSON); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan rag document for projection: %w", err)
		}
		documents = append(documents, document)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("read rag documents for projection: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close rag documents for projection: %w", err)
	}
	if len(documents) > ragProjectionBatchSize {
		return nil, fmt.Errorf("read %d rag documents in a projection batch; limit is %d", len(documents), ragProjectionBatchSize)
	}
	return documents, nil
}

func insertRagProjectionBatch(ctx context.Context, exec ragProjectionExecutor, dialect SQLDialect, documents []ragProjectionRow) error {
	insertToken := sqlRagProjectionInsertToken.bind(dialect)
	insertTag := sqlRagProjectionInsertTag.bind(dialect)
	for _, document := range documents {
		if document.Scope == "" {
			if err := rag.ValidateScope(core.ScopePath{}); err != nil {
				return err
			}
		}
		tags, err := decodeRagProjectionTags(document.TagsJSON)
		if err != nil {
			return fmt.Errorf("decode rag document %s/%s tags: %w", document.Scope, document.ID, err)
		}
		if err := rag.ValidateDocument(rag.Document{
			ID: document.ID, Source: document.Source, Content: document.Content, Tags: tags,
		}); err != nil {
			return fmt.Errorf("rag document %s/%s: %w", document.Scope, document.ID, err)
		}
		for _, token := range ragProjectionTokens(document.Content) {
			if _, err := exec.ExecContext(ctx, insertToken, document.Scope, document.ID, token); err != nil {
				return fmt.Errorf("insert rag token %s/%s/%s: %w", document.Scope, document.ID, token, err)
			}
		}
		for _, tag := range tags {
			if _, err := exec.ExecContext(ctx, insertTag, document.Scope, document.ID, tag); err != nil {
				return fmt.Errorf("insert rag tag %s/%s/%s: %w", document.Scope, document.ID, tag, err)
			}
		}
	}
	return nil
}

func ragProjectionStatsTx(ctx context.Context, tx *sql.Tx) (RagProjectionStats, error) {
	var stats RagProjectionStats
	if err := tx.QueryRowContext(ctx, sqlRagProjectionStats).Scan(
		&stats.CanonicalDocuments,
		&stats.TokenRows,
		&stats.TagRows,
		&stats.TokenDocuments,
		&stats.TagDocuments,
		&stats.OrphanTokenRows,
		&stats.OrphanTagRows,
	); err != nil {
		return RagProjectionStats{}, fmt.Errorf("read rag projection stats: %w", err)
	}
	if err := validateRagProjectionStats(stats); err != nil {
		return RagProjectionStats{}, err
	}
	return stats, nil
}

func validateRagProjectionStats(stats RagProjectionStats) error {
	for _, count := range []struct {
		name  string
		value int64
	}{
		{"canonical_documents", stats.CanonicalDocuments},
		{"token_rows", stats.TokenRows},
		{"tag_rows", stats.TagRows},
		{"token_documents", stats.TokenDocuments},
		{"tag_documents", stats.TagDocuments},
		{"orphan_token_rows", stats.OrphanTokenRows},
		{"orphan_tag_rows", stats.OrphanTagRows},
	} {
		if count.value < 0 {
			return fmt.Errorf("rag projection stats %s is negative: %d", count.name, count.value)
		}
	}
	return nil
}
