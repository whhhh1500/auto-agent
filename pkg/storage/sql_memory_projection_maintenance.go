package storage

import (
	"context"
	"database/sql"
	"fmt"
)

// MemoryProjectionStats describes canonical Memory entries and their derived
// search and tag projections. Missing search fields are counted only when the
// corresponding canonical field is non-empty.
type MemoryProjectionStats struct {
	CanonicalEntries       int64 `json:"canonical_entries"`
	TagRows                int64 `json:"tag_rows"`
	TaggedKeys             int64 `json:"tagged_keys"`
	OrphanTagRows          int64 `json:"orphan_tag_rows"`
	KeySearchMissing       int64 `json:"key_search_missing"`
	ContentSearchMissing   int64 `json:"content_search_missing"`
	SearchProjectedEntries int64 `json:"search_projected_entries"`
}

// MemoryProjectionMaintainer is implemented by Memory stores that can inspect
// and rebuild their derived SQL search and tag projections.
type MemoryProjectionMaintainer interface {
	ProjectionStats(ctx context.Context) (MemoryProjectionStats, error)
	RebuildProjection(ctx context.Context) (MemoryProjectionStats, error)
}

var sqlMemoryProjectionStats = `SELECT
	(SELECT COUNT(*) FROM memory_entries),
	(SELECT COUNT(*) FROM memory_entry_tags),
	(SELECT COUNT(*) FROM (
		SELECT scope, key FROM memory_entry_tags GROUP BY scope, key
	) AS tagged_keys),
	(SELECT COUNT(*) FROM memory_entry_tags AS tags
		WHERE NOT EXISTS (
			SELECT 1 FROM memory_entries AS entries
			WHERE entries.scope = tags.scope AND entries.key = tags.key
		)),
	(SELECT COUNT(*) FROM memory_entries
		WHERE key <> '' AND key_search = ''),
	(SELECT COUNT(*) FROM memory_entries
		WHERE content <> '' AND content_search = ''),
	(SELECT COUNT(*) FROM memory_entries
		WHERE key_search <> '' AND content_search <> '')`

// ProjectionStats reads only aggregate counts and grouping keys in one
// read-only transaction. It intentionally never returns Memory key, content,
// or tag values.
func (s *SQLMemoryStore) ProjectionStats(ctx context.Context) (MemoryProjectionStats, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return MemoryProjectionStats{}, fmt.Errorf("begin memory projection stats: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	stats, err := memoryProjectionStatsTx(ctx, tx)
	if err != nil {
		return MemoryProjectionStats{}, err
	}
	if err := tx.Commit(); err != nil {
		return MemoryProjectionStats{}, fmt.Errorf("commit memory projection stats: %w", err)
	}
	return stats, nil
}

// RebuildProjection atomically replaces the derived Memory search and tag
// projections from canonical rows and returns statistics for the replacement.
func (s *SQLMemoryStore) RebuildProjection(ctx context.Context) (MemoryProjectionStats, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return MemoryProjectionStats{}, fmt.Errorf("begin memory projection rebuild: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := rebuildMemorySearchProjectionTx(ctx, tx, s.dialect); err != nil {
		return MemoryProjectionStats{}, err
	}
	stats, err := memoryProjectionStatsTx(ctx, tx)
	if err != nil {
		return MemoryProjectionStats{}, err
	}
	if stats.OrphanTagRows != 0 {
		return MemoryProjectionStats{}, fmt.Errorf("rebuilt memory projection has orphan tag rows: %d", stats.OrphanTagRows)
	}
	if err := tx.Commit(); err != nil {
		return MemoryProjectionStats{}, fmt.Errorf("commit memory projection rebuild: %w", err)
	}
	return stats, nil
}

func memoryProjectionStatsTx(ctx context.Context, tx *sql.Tx) (MemoryProjectionStats, error) {
	var stats MemoryProjectionStats
	if err := tx.QueryRowContext(ctx, sqlMemoryProjectionStats).Scan(
		&stats.CanonicalEntries,
		&stats.TagRows,
		&stats.TaggedKeys,
		&stats.OrphanTagRows,
		&stats.KeySearchMissing,
		&stats.ContentSearchMissing,
		&stats.SearchProjectedEntries,
	); err != nil {
		return MemoryProjectionStats{}, fmt.Errorf("read memory projection stats: %w", err)
	}
	if err := validateMemoryProjectionStats(stats); err != nil {
		return MemoryProjectionStats{}, err
	}
	return stats, nil
}

func validateMemoryProjectionStats(stats MemoryProjectionStats) error {
	for _, count := range []struct {
		name  string
		value int64
	}{
		{"canonical_entries", stats.CanonicalEntries},
		{"tag_rows", stats.TagRows},
		{"tagged_keys", stats.TaggedKeys},
		{"orphan_tag_rows", stats.OrphanTagRows},
		{"key_search_missing", stats.KeySearchMissing},
		{"content_search_missing", stats.ContentSearchMissing},
		{"search_projected_entries", stats.SearchProjectedEntries},
	} {
		if count.value < 0 {
			return fmt.Errorf("memory projection stats %s is negative: %d", count.name, count.value)
		}
	}
	return nil
}
