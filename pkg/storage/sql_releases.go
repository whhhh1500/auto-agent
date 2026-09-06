package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cc-auto-agent/harness-core/pkg/control"
	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"sort"
	"strings"
	"time"
)

var (
	sqlInsertRelease        = sqlQuery{"INSERT INTO profile_releases (profile_id, version, operation_id, scope, layer_json, revision, created_at, rolled_back) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"}
	sqlSelectReleases       = sqlQuery{"SELECT version, operation_id, scope, layer_json, revision, created_at, rolled_back FROM profile_releases WHERE profile_id = ? ORDER BY version"}
	sqlListReleaseProfiles  = sqlQuery{"SELECT DISTINCT profile_id FROM profile_releases ORDER BY profile_id"}
	sqlFindReleaseOperation = sqlQuery{"SELECT profile_id, version, operation_id, scope, layer_json, revision, created_at, rolled_back FROM profile_releases WHERE operation_id = ?"}
	sqlCountProfileReleases = sqlQuery{"SELECT COUNT(*) FROM profile_releases WHERE profile_id = ?"}
)

const MaxReleaseHistoryPerProfile = 64

func (s *SQLSessionStore) releaseHistoryCap() int {
	if s != nil && s.maxReleaseHistory > 0 {
		return s.maxReleaseHistory
	}
	return MaxReleaseHistoryPerProfile
}

func (s *SQLSessionStore) RecordRelease(ctx context.Context, info control.ReleaseInfo) error {
	if err := core.ValidateNamespacedID(info.ProfileID); err != nil {
		return fmt.Errorf("release profile id: %w", err)
	}
	if info.OperationID != "" {
		if err := core.ValidateRunID(info.OperationID); err != nil {
			return fmt.Errorf("release operation id: %w", err)
		}
	}
	if info.Layer == nil || info.Revision == "" {
		return fmt.Errorf("release %s v%d has no artifact", info.ProfileID, info.Version)
	}
	layer := core.CloneAgentProfileLayer(*info.Layer)
	layer.Scope = info.Scope
	revision, err := core.ProfileLayerRevision(layer)
	if err != nil || revision != info.Revision {
		return fmt.Errorf("release %s v%d artifact revision mismatch", info.ProfileID, info.Version)
	}
	encoded, err := json.Marshal(layer)
	if err != nil {
		return err
	}
	rolledBack := 0
	if info.RolledBack {
		rolledBack = 1
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var count int
	if err := tx.QueryRowContext(ctx, sqlCountProfileReleases.bind(s.dialect), info.ProfileID).Scan(&count); err != nil {
		return err
	}
	if count >= s.releaseHistoryCap() {
		return fmt.Errorf("release history for %s exceeds maximum of %d versions", info.ProfileID, s.releaseHistoryCap())
	}
	_, err = tx.ExecContext(ctx, sqlInsertRelease.bind(s.dialect),
		info.ProfileID, info.Version, info.OperationID, info.Scope.String(), string(encoded), info.Revision,
		info.CreatedAt.UnixMilli(), rolledBack,
	)
	if err != nil {
		return s.translateDuplicate(info.ProfileID, err)
	}
	if err := bumpControlRevision(ctx, tx, s.dialect); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SQLSessionStore) MarkRolledBack(ctx context.Context, profileID string, versions []int) error {
	if err := core.ValidateNamespacedID(profileID); err != nil {
		return fmt.Errorf("release profile id: %w", err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var changed int64
	for _, version := range versions {
		result, err := tx.ExecContext(ctx,
			sqlQuery{"UPDATE profile_releases SET rolled_back = 1 WHERE profile_id = ? AND version = ?"}.bind(s.dialect),
			profileID, version,
		)
		if err != nil {
			return err
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		changed += affected
	}
	if changed > 0 {
		if err := bumpControlRevision(ctx, tx, s.dialect); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *SQLSessionStore) ControlRevision(ctx context.Context) (int64, error) {
	return readControlRevision(ctx, s.db, s.dialect)
}

func (s *SQLSessionStore) LoadReleases(ctx context.Context, profileID string) ([]control.ReleaseInfo, error) {
	if err := core.ValidateNamespacedID(profileID); err != nil {
		return nil, fmt.Errorf("release profile id: %w", err)
	}
	rows, err := s.db.QueryContext(ctx, sqlSelectReleases.bind(s.dialect), profileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []control.ReleaseInfo{}
	for rows.Next() {
		var version int
		var operationID, scope, layerJSON, revision string
		var createdMillis int64
		var rolledBack int
		if err := rows.Scan(&version, &operationID, &scope, &layerJSON, &revision, &createdMillis, &rolledBack); err != nil {
			return nil, err
		}
		info, err := decodeReleaseInfo(profileID, version, operationID, scope, layerJSON, revision, createdMillis, rolledBack)
		if err != nil {
			return nil, err
		}
		out = append(out, info)
	}
	return out, rows.Err()
}

func (s *SQLSessionStore) FindReleaseByOperation(ctx context.Context, operationID string) (control.ReleaseInfo, bool, error) {
	if err := core.ValidateRunID(operationID); err != nil {
		return control.ReleaseInfo{}, false, err
	}
	var profileID, storedOperationID, scope, layerJSON, revision string
	var version int
	var createdMillis int64
	var rolledBack int
	err := s.db.QueryRowContext(ctx, sqlFindReleaseOperation.bind(s.dialect), operationID).Scan(
		&profileID, &version, &storedOperationID, &scope, &layerJSON, &revision, &createdMillis, &rolledBack,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return control.ReleaseInfo{}, false, nil
	}
	if err != nil {
		return control.ReleaseInfo{}, false, err
	}
	info, err := decodeReleaseInfo(profileID, version, storedOperationID, scope, layerJSON, revision, createdMillis, rolledBack)
	return info, err == nil, err
}

func decodeReleaseInfo(profileID string, version int, operationID, scope, layerJSON, revision string, createdMillis int64, rolledBack int) (control.ReleaseInfo, error) {
	parsedScope, err := ParseScopePath(scope)
	if err != nil {
		return control.ReleaseInfo{}, fmt.Errorf("decode release scope: %w", err)
	}
	info := control.ReleaseInfo{
		ProfileID: profileID, Version: version, OperationID: operationID, RolledBack: rolledBack == 1,
		CreatedAt: time.UnixMilli(createdMillis).UTC(), Scope: parsedScope, Revision: revision,
	}
	if layerJSON == "" || layerJSON == "{}" {
		return info, nil
	}
	var layer core.AgentProfileLayer
	if err := json.Unmarshal([]byte(layerJSON), &layer); err != nil {
		return control.ReleaseInfo{}, fmt.Errorf("decode release layer: %w", err)
	}
	layer.Scope = parsedScope
	if layer.ProfileID != profileID {
		return control.ReleaseInfo{}, fmt.Errorf("release layer profile id does not match row")
	}
	calculated, err := core.ProfileLayerRevision(layer)
	if err != nil || calculated != revision {
		return control.ReleaseInfo{}, fmt.Errorf("release %s v%d revision mismatch", profileID, version)
	}
	info.Layer = &layer
	return info, nil
}

func (s *SQLSessionStore) ListReleaseProfiles(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, sqlListReleaseProfiles.bind(s.dialect))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var profileID string
		if err := rows.Scan(&profileID); err != nil {
			return nil, err
		}
		out = append(out, profileID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Strings(out)
	return out, nil
}

// ParseScopePath rebuilds a core.ScopePath from its String() form ("kind:id/kind:id").
// Scope ids containing "/" cannot round-trip through this text form; scope
// ids in durable references are expected to follow the same naming rules as
// capability ids. It is the inverse of core.ScopePath.String.
func ParseScopePath(value string) (core.ScopePath, error) {
	segments := []core.ScopeRef{}
	for _, part := range strings.Split(value, "/") {
		if part == "" {
			continue
		}
		kind, id, found := strings.Cut(part, ":")
		if !found {
			return core.ScopePath{}, errors.New("invalid scope segment " + part)
		}
		segments = append(segments, core.ScopeRef{Kind: core.ScopeKind(kind), ID: id})
	}
	return core.NewScopePath(segments...)
}
