package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/extensions/memory"
	"github.com/cc-auto-agent/harness-core/pkg/extensions/rag"
)

// SQLMemoryStore backs the memory contract on the shared SQL schema.
// Key-replacement and per-scope ownership map directly onto the table.
type SQLMemoryStore struct {
	db         *sql.DB
	dialect    SQLDialect
	maxEntries int
	maxScopes  int
}

func NewSQLMemoryStore(db *sql.DB, dialect SQLDialect) (*SQLMemoryStore, error) {
	if db == nil {
		return nil, fmt.Errorf("sql memory store requires a database handle")
	}
	if err := validateSQLDialect(dialect); err != nil {
		return nil, err
	}
	return &SQLMemoryStore{db: db, dialect: dialect}, nil
}

func (s *SQLMemoryStore) entryCap() int {
	if s != nil && s.maxEntries > 0 {
		return s.maxEntries
	}
	return memory.MaxEntriesPerScope
}

func (s *SQLMemoryStore) scopeCap() int {
	if s != nil && s.maxScopes > 0 {
		return s.maxScopes
	}
	return memory.MaxScopes
}

var (
	sqlMemoryInsert = sqlQuery{`INSERT INTO memory_entries
		(scope, id, key, content, tags, tags_json, created_at, key_search, content_search)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(scope, key) DO UPDATE SET
			id = excluded.id,
			content = excluded.content,
			tags = excluded.tags,
			tags_json = excluded.tags_json,
			created_at = excluded.created_at,
			key_search = excluded.key_search,
			content_search = excluded.content_search`}
	sqlMemoryDeleteTags = sqlQuery{"DELETE FROM memory_entry_tags WHERE scope = ? AND key = ?"}
	sqlMemoryInsertTag  = sqlQuery{"INSERT INTO memory_entry_tags (scope, key, tag) VALUES (?, ?, ?)"}
	sqlMemoryForgetTags = sqlQuery{`DELETE FROM memory_entry_tags
		WHERE scope = ? AND key = (
			SELECT key FROM memory_entries WHERE scope = ? AND id = ?
		)`}
	sqlMemoryForget      = sqlQuery{"DELETE FROM memory_entries WHERE scope = ? AND id = ?"}
	sqlMemoryCountScope  = sqlQuery{"SELECT COUNT(*) FROM memory_entries WHERE scope = ?"}
	sqlMemoryCountKey    = sqlQuery{"SELECT COUNT(*) FROM memory_entries WHERE scope = ? AND key = ?"}
	sqlMemoryCountScopes = sqlQuery{"SELECT COUNT(DISTINCT scope) FROM memory_entries"}
)

func (s *SQLMemoryStore) Remember(ctx context.Context, scope core.ScopePath, entry core.MemoryEntry) (core.MemoryEntry, error) {
	if err := memory.ValidateScope(scope); err != nil {
		return core.MemoryEntry{}, err
	}
	if err := memory.ValidateEntry(entry); err != nil {
		return core.MemoryEntry{}, err
	}
	var tagsJSON string
	var err error
	entry.Tags, tagsJSON, err = encodeTags(entry.Tags)
	if err != nil {
		return core.MemoryEntry{}, fmt.Errorf("encode memory entry tags: %w", err)
	}
	if entry.ID == "" {
		id, err := core.NewID("mem_")
		if err != nil {
			return core.MemoryEntry{}, err
		}
		entry.ID = id
	}
	entry.CreatedAt = time.Now().UTC()
	projectedTags := memoryProjectionTags(entry.Tags)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return core.MemoryEntry{}, err
	}
	defer tx.Rollback()

	var existing int
	if err := tx.QueryRowContext(ctx, sqlMemoryCountKey.bind(s.dialect), scope.String(), entry.Key).Scan(&existing); err != nil {
		return core.MemoryEntry{}, err
	}
	if existing == 0 {
		var count int
		if err := tx.QueryRowContext(ctx, sqlMemoryCountScope.bind(s.dialect), scope.String()).Scan(&count); err != nil {
			return core.MemoryEntry{}, err
		}
		if count >= s.entryCap() {
			return core.MemoryEntry{}, fmt.Errorf("memory scope exceeds maximum of %d entries", s.entryCap())
		}
		if count == 0 {
			var scopes int
			if err := tx.QueryRowContext(ctx, sqlMemoryCountScopes.bind(s.dialect)).Scan(&scopes); err != nil {
				return core.MemoryEntry{}, err
			}
			if scopes >= s.scopeCap() {
				return core.MemoryEntry{}, fmt.Errorf("memory store exceeds maximum of %d scopes", s.scopeCap())
			}
		}
	}

	if _, err := tx.ExecContext(ctx, sqlMemoryInsert.bind(s.dialect),
		scope.String(), entry.ID, entry.Key, entry.Content, strings.Join(entry.Tags, ","), tagsJSON, entry.CreatedAt.UnixMilli(),
		strings.ToLower(entry.Key), strings.ToLower(entry.Content),
	); err != nil {
		return core.MemoryEntry{}, err
	}
	if _, err := tx.ExecContext(ctx, sqlMemoryDeleteTags.bind(s.dialect), scope.String(), entry.Key); err != nil {
		return core.MemoryEntry{}, err
	}
	for _, tag := range projectedTags {
		if _, err := tx.ExecContext(ctx, sqlMemoryInsertTag.bind(s.dialect), scope.String(), entry.Key, tag); err != nil {
			return core.MemoryEntry{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return core.MemoryEntry{}, err
	}
	return entry, nil
}

func (s *SQLMemoryStore) Recall(ctx context.Context, scope core.ScopePath, query string, tags []string, limit int) ([]core.MemoryEntry, error) {
	if err := memory.ValidateScope(scope); err != nil {
		return nil, err
	}
	if err := memory.ValidateRecall(query, tags, limit); err != nil {
		return nil, err
	}
	needle := strings.ToLower(strings.TrimSpace(query))
	statement, args := buildSQLMemoryRecall(s.dialect, scope.String(), needle, tags, limit)
	rows, err := s.db.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []core.MemoryEntry{}
	for rows.Next() {
		var entry core.MemoryEntry
		var tagsJSON string
		var createdMillis int64
		if err := rows.Scan(&entry.ID, &entry.Key, &entry.Content, &tagsJSON, &createdMillis); err != nil {
			return nil, err
		}
		var err error
		entry.Tags, err = decodeMemoryTags(tagsJSON)
		if err != nil {
			return nil, fmt.Errorf("decode memory entry %q tags: %w", entry.ID, err)
		}
		entry.CreatedAt = time.UnixMilli(createdMillis).UTC()
		out = append(out, entry)
	}
	return out, rows.Err()
}

func (s *SQLMemoryStore) Forget(ctx context.Context, scope core.ScopePath, id string) error {
	if err := memory.ValidateScope(scope); err != nil {
		return err
	}
	if err := memory.ValidateForgetID(id); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, sqlMemoryForgetTags.bind(s.dialect), scope.String(), scope.String(), id); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, sqlMemoryForget.bind(s.dialect), scope.String(), id)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect forgotten memory entry %q: %w", id, err)
	}
	if affected == 0 {
		return fmt.Errorf("memory entry %q not found", id)
	}
	if affected != 1 {
		return fmt.Errorf("forget memory entry %q affected %d rows", id, affected)
	}
	return tx.Commit()
}

func memoryProjectionTags(tags []string) []string {
	unique := make(map[string]bool, len(tags))
	for _, tag := range tags {
		unique[tag] = true
	}
	return sortedSet(unique)
}

func decodeMemoryTags(raw string) ([]string, error) {
	return decodeJSONStringArray(raw)
}

func buildSQLMemoryRecall(dialect SQLDialect, scope, needle string, tags []string, limit int) (string, []any) {
	projectedTags := memoryProjectionTags(tags)
	args := make([]any, 0, 1+len(projectedTags)+3)
	args = append(args, scope)

	var statement strings.Builder
	statement.WriteString(`SELECT id, key, content, tags_json, created_at
		FROM memory_entries
		WHERE scope = ?`)
	if needle != "" {
		if dialect == SQLDialectPostgres {
			statement.WriteString(" AND (strpos(key_search, ?) > 0 OR strpos(content_search, ?) > 0)")
		} else {
			statement.WriteString(" AND (instr(key_search, ?) > 0 OR instr(content_search, ?) > 0)")
		}
		args = append(args, needle, needle)
	}
	if len(projectedTags) > 0 {
		statement.WriteString(` AND EXISTS (
			SELECT 1 FROM memory_entry_tags AS entry_tags
			WHERE entry_tags.scope = memory_entries.scope
				AND entry_tags.key = memory_entries.key
				AND entry_tags.tag IN (`)
		for i, tag := range projectedTags {
			if i > 0 {
				statement.WriteString(", ")
			}
			statement.WriteByte('?')
			args = append(args, tag)
		}
		statement.WriteString(`)
		)`)
	}
	statement.WriteString(" ORDER BY created_at DESC, id ASC")
	if limit > 0 {
		statement.WriteString(" LIMIT ?")
		args = append(args, limit)
	}
	return sqlQuery{text: statement.String()}.bind(dialect), args
}

// SQLRagIndex backs the retrieval contract on the shared SQL schema. Its v27
// token and tag projections make matching, filtering, ranking, and limiting a
// database operation rather than a full-document scan in Go.
type SQLRagIndex struct {
	db           *sql.DB
	dialect      SQLDialect
	maxDocuments int
	maxScopes    int
}

func NewSQLRagIndex(db *sql.DB, dialect SQLDialect) (*SQLRagIndex, error) {
	if db == nil {
		return nil, fmt.Errorf("sql rag index requires a database handle")
	}
	if err := validateSQLDialect(dialect); err != nil {
		return nil, err
	}
	return &SQLRagIndex{db: db, dialect: dialect}, nil
}

func (s *SQLRagIndex) documentCap() int {
	if s != nil && s.maxDocuments > 0 {
		return s.maxDocuments
	}
	return rag.MaxDocumentsPerScope
}

func (s *SQLRagIndex) scopeCap() int {
	if s != nil && s.maxScopes > 0 {
		return s.maxScopes
	}
	return rag.MaxScopes
}

var (
	sqlRagInsert = sqlQuery{`INSERT INTO rag_documents (scope, id, source, content, tags, tags_json)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(scope, id) DO UPDATE SET
			source = excluded.source,
			content = excluded.content,
			tags = excluded.tags,
			tags_json = excluded.tags_json`}
	sqlRagDeleteTokens = sqlQuery{"DELETE FROM rag_document_tokens WHERE scope = ? AND document_id = ?"}
	sqlRagDeleteTags   = sqlQuery{"DELETE FROM rag_document_tags WHERE scope = ? AND document_id = ?"}
	sqlRagInsertToken  = sqlQuery{"INSERT INTO rag_document_tokens (scope, document_id, token) VALUES (?, ?, ?)"}
	sqlRagInsertTag    = sqlQuery{"INSERT INTO rag_document_tags (scope, document_id, tag) VALUES (?, ?, ?)"}
	sqlRagCountScope   = sqlQuery{"SELECT COUNT(*) FROM rag_documents WHERE scope = ?"}
	sqlRagCountID      = sqlQuery{"SELECT COUNT(*) FROM rag_documents WHERE scope = ? AND id = ?"}
	sqlRagCountScopes  = sqlQuery{"SELECT COUNT(DISTINCT scope) FROM rag_documents"}
)

func (s *SQLRagIndex) Ingest(ctx context.Context, scope core.ScopePath, document core.RagDocument) error {
	if err := rag.ValidateScope(scope); err != nil {
		return err
	}
	if err := rag.ValidateDocument(document); err != nil {
		return err
	}
	tags, tagsJSON, err := encodeTags(document.Tags)
	if err != nil {
		return fmt.Errorf("encode rag document tags: %w", err)
	}
	tokens := sortedSet(rag.Tokenize(document.Content))
	projectedTags := sortedSet(stringSet(tags))

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var existing int
	if err := tx.QueryRowContext(ctx, sqlRagCountID.bind(s.dialect), scope.String(), document.ID).Scan(&existing); err != nil {
		return err
	}
	if existing == 0 {
		var count int
		if err := tx.QueryRowContext(ctx, sqlRagCountScope.bind(s.dialect), scope.String()).Scan(&count); err != nil {
			return err
		}
		if count >= s.documentCap() {
			return fmt.Errorf("rag scope exceeds maximum of %d documents", s.documentCap())
		}
		if count == 0 {
			var scopes int
			if err := tx.QueryRowContext(ctx, sqlRagCountScopes.bind(s.dialect)).Scan(&scopes); err != nil {
				return err
			}
			if scopes >= s.scopeCap() {
				return fmt.Errorf("rag index exceeds maximum of %d scopes", s.scopeCap())
			}
		}
	}

	if _, err := tx.ExecContext(ctx, sqlRagInsert.bind(s.dialect),
		scope.String(), document.ID, document.Source, document.Content, strings.Join(tags, ","), tagsJSON,
	); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, sqlRagDeleteTokens.bind(s.dialect), scope.String(), document.ID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, sqlRagDeleteTags.bind(s.dialect), scope.String(), document.ID); err != nil {
		return err
	}
	for _, token := range tokens {
		if _, err := tx.ExecContext(ctx, sqlRagInsertToken.bind(s.dialect), scope.String(), document.ID, token); err != nil {
			return err
		}
	}
	for _, tag := range projectedTags {
		if _, err := tx.ExecContext(ctx, sqlRagInsertTag.bind(s.dialect), scope.String(), document.ID, tag); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *SQLRagIndex) Search(ctx context.Context, scope core.ScopePath, query core.RagQuery) ([]core.RagChunk, error) {
	if err := rag.ValidateScope(scope); err != nil {
		return nil, err
	}
	queryTokens, err := rag.ValidateSearch(query)
	if err != nil {
		return nil, err
	}
	// Visibility: documents at the calling scope or any of its ancestors.
	prefixes := scope.Prefixes()
	topK := query.TopK
	if topK == 0 {
		topK = 5
	}
	statement, args := buildSQLRagSearch(s.dialect, prefixes, sortedSet(queryTokens), sortedSet(stringSet(query.Tags)), topK)
	rows, err := s.db.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	hits := []core.RagChunk{}
	for rows.Next() {
		var chunk core.RagChunk
		var overlap int64
		if err := rows.Scan(&chunk.ID, &chunk.Source, &chunk.Content, &overlap); err != nil {
			return nil, err
		}
		chunk.Score = float64(overlap) / float64(len(queryTokens))
		hits = append(hits, chunk)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return hits, nil
}

func stringSet(values []string) map[string]bool {
	set := make(map[string]bool, len(values))
	for _, value := range values {
		set[value] = true
	}
	return set
}

func sortedSet(set map[string]bool) []string {
	values := make([]string, 0, len(set))
	for value := range set {
		values = append(values, value)
	}
	sort.Strings(values)
	return values
}

func buildSQLRagSearch(dialect SQLDialect, prefixes []core.ScopePath, queryTokens, queryTags []string, topK int) (string, []any) {
	visibleScopes := make([]string, 0, len(prefixes))
	args := make([]any, 0, len(prefixes)*2+len(queryTokens)+len(queryTags)+1)
	for hierarchy, prefix := range prefixes {
		// PostgreSQL otherwise infers the hierarchy placeholder in VALUES as
		// text, while SQLite accepts an untyped integer. Keep its ordering
		// numeric and bind the same portable SQL on both engines.
		visibleScopes = append(visibleScopes, "(?, CAST(? AS BIGINT))")
		args = append(args, prefix.String(), int64(hierarchy))
	}
	tokenPlaceholders := make([]string, len(queryTokens))
	for i, token := range queryTokens {
		tokenPlaceholders[i] = "?"
		args = append(args, token)
	}

	var statement strings.Builder
	statement.WriteString(`WITH visible_scopes(scope, hierarchy) AS (VALUES `)
	statement.WriteString(strings.Join(visibleScopes, ", "))
	statement.WriteString(`), matching_documents AS (
		SELECT d.id, d.source, d.content, COUNT(DISTINCT t.token) AS overlap, v.hierarchy
		FROM rag_documents d
		JOIN visible_scopes v ON v.scope = d.scope
		JOIN rag_document_tokens t ON t.scope = d.scope AND t.document_id = d.id
		WHERE t.token IN (`)
	statement.WriteString(strings.Join(tokenPlaceholders, ", "))
	statement.WriteString(`)`)
	if len(queryTags) > 0 {
		tagPlaceholders := make([]string, len(queryTags))
		for i, tag := range queryTags {
			tagPlaceholders[i] = "?"
			args = append(args, tag)
		}
		statement.WriteString(` AND EXISTS (
			SELECT 1 FROM rag_document_tags dt
			WHERE dt.scope = d.scope AND dt.document_id = d.id AND dt.tag IN (`)
		statement.WriteString(strings.Join(tagPlaceholders, ", "))
		statement.WriteString(`)
		)`)
	}
	statement.WriteString(`
		GROUP BY d.scope, d.id, d.source, d.content, v.hierarchy
	)
	SELECT id, source, content, overlap
	FROM matching_documents
	ORDER BY overlap DESC, hierarchy ASC, id ASC
	LIMIT ?`)
	args = append(args, topK)
	return sqlQuery{text: statement.String()}.bind(dialect), args
}

func encodeTags(tags []string) ([]string, string, error) {
	cloned := append([]string(nil), tags...)
	if cloned == nil {
		cloned = []string{}
	}
	encoded, err := json.Marshal(cloned)
	if err != nil {
		return nil, "", err
	}
	return cloned, string(encoded), nil
}
