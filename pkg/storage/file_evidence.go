package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

func (s *FileSessionStore) evidencePath(id string) (string, error) {
	if err := ValidateSessionIDForFile(id); err != nil {
		return "", err
	}
	return filepath.Join(s.dir, id+".evidence.json"), nil
}

func (s *FileSessionStore) writeEvidence(ctx context.Context, sessionID string, version int64, header core.SessionOptions, events []core.SessionEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	path, err := s.evidencePath(sessionID)
	if err != nil {
		return err
	}
	records, err := deriveRunEvidence(sessionID, header, events)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(persistedRunEvidence{FormatVersion: runEvidenceSidecarVersion, Version: version, Records: records})
	if err != nil {
		return err
	}
	temp, err := os.CreateTemp(s.dir, ".evidence-*")
	if err != nil {
		return err
	}
	tempName := temp.Name()
	defer func() { _ = os.Remove(tempName) }()
	if err := privateChmod(tempName, 0o600); err != nil {
		_ = temp.Close()
		return err
	}
	if _, err := temp.Write(payload); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.Rename(tempName, path); err != nil {
		return err
	}
	return privateChmod(path, 0o600)
}

// QueryEvidence serves the Run/Backtest portion of the cross-object contract.
// FileSessionStore has no SQL control-plane tables, so Release/Canary and
// Evaluation references are intentionally not fabricated here.
func (s *FileSessionStore) QueryEvidence(ctx context.Context, query EvidenceQuery) ([]EvidenceRecord, error) {
	query.Cursor = ""
	page, err := s.QueryEvidencePage(ctx, query)
	return page.Records, err
}

func (s *FileSessionStore) QueryEvidencePage(ctx context.Context, query EvidenceQuery) (EvidencePage, error) {
	if err := query.Validate(); err != nil {
		return EvidencePage{}, err
	}
	result, err := s.loadRunEvidenceRecords(ctx, query)
	if err != nil {
		return EvidencePage{}, err
	}
	return paginateEvidenceRecords(query, map[string][]EvidenceRecord{"run": result})
}

func (s *FileSessionStore) RunEvidenceStats(ctx context.Context, query EvidenceQuery) (RunEvidenceStats, error) {
	query.Cursor = ""
	records, err := s.loadRunEvidenceRecords(ctx, query)
	if err != nil {
		return RunEvidenceStats{}, err
	}
	return aggregateRunEvidenceStats(records, query)
}

func (s *FileSessionStore) loadRunEvidenceRecords(ctx context.Context, query EvidenceQuery) ([]EvidenceRecord, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	result := []EvidenceRecord{}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".jsonl") {
			continue
		}
		id := strings.TrimSuffix(entry.Name(), ".jsonl")
		session, err := s.Load(ctx, id)
		if err != nil {
			return nil, err
		}
		path, err := s.evidencePath(id)
		if err != nil {
			return nil, err
		}
		var stored persistedRunEvidence
		payload, readErr := os.ReadFile(path)
		if readErr == nil {
			readErr = json.Unmarshal(payload, &stored)
		}
		if readErr != nil || stored.FormatVersion != runEvidenceSidecarVersion || stored.Version != session.Version() {
			if err := s.rebuildEvidenceForSession(ctx, session); err != nil {
				return nil, err
			}
			payload, err = os.ReadFile(path)
			if err != nil || json.Unmarshal(payload, &stored) != nil {
				return nil, fmt.Errorf("read rebuilt evidence for session %s: %w", id, err)
			}
		}
		result = append(result, filterRunEvidence(stored.Records, query, len(stored.Records))...)
	}
	sortEvidence(result)
	return result, nil
}

func ValidateSessionIDForFile(id string) error {
	if err := core.ValidateSessionID(id); err != nil {
		return err
	}
	if strings.ContainsAny(id, "/\\") {
		return fmt.Errorf("invalid session id %q", id)
	}
	return nil
}

func (s *FileSessionStore) rebuildEvidenceForSession(ctx context.Context, session *core.Session) error {
	return s.writeEvidence(ctx, session.ID(), session.Version(), sessionOptionsFromSession(session), session.Events())
}

func sessionOptionsFromSession(session *core.Session) core.SessionOptions {
	return core.SessionOptions{
		ID: session.ID(), ProfileID: session.ProfileID(), Principal: session.Principal(),
		Scope: session.Scope(), Metadata: session.Metadata(),
	}
}

var _ EvidenceStore = (*FileSessionStore)(nil)
var _ EvidencePager = (*FileSessionStore)(nil)
var _ EvidenceStatsStore = (*FileSessionStore)(nil)
