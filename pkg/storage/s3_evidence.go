package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

func s3EvidenceKey(id string) string { return s3SessionsPrefix + id + "/evidence.json" }

func (s *S3SessionStore) writeEvidence(ctx context.Context, sessionID string, version int64, header core.SessionOptions, events []core.SessionEvent) error {
	records, err := deriveRunEvidence(sessionID, header, events)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(persistedRunEvidence{FormatVersion: runEvidenceSidecarVersion, Version: version, Records: records})
	if err != nil {
		return err
	}
	_, err = s.objects.Put(ctx, s3EvidenceKey(sessionID), payload, PutOptions{})
	return err
}

func (s *S3SessionStore) loadCanonicalForEvidence(ctx context.Context, id string, meta s3SessionMeta) (*core.Session, error) {
	items, err := s.objects.List(ctx, s3EventsPrefix(id), "", 0)
	if err != nil {
		return nil, err
	}
	events := []core.SessionEvent{}
	for _, chunkKey := range sessionEventChunkKeys(id, items) {
		if int64(len(events)) >= meta.Version {
			break
		}
		body, _, err := s.objects.Get(ctx, chunkKey)
		if err != nil {
			return nil, err
		}
		for _, line := range bytes.Split(bytes.TrimSpace(body), []byte{'\n'}) {
			line = bytes.TrimSpace(line)
			if len(line) == 0 {
				continue
			}
			var event core.SessionEvent
			if err := json.Unmarshal(line, &event); err != nil {
				return nil, fmt.Errorf("decode session %s evidence event: %w", id, err)
			}
			events = append(events, event)
			if int64(len(events)) >= meta.Version {
				break
			}
		}
	}
	if int64(len(events)) < meta.Version {
		return nil, fmt.Errorf("session %s committed version %d exceeds canonical event count %d", id, meta.Version, len(events))
	}
	return core.RestoreSession(meta.Header, events[:meta.Version])
}

// QueryEvidence serves Run/Backtest evidence for object-store sessions. The
// object store has no relational Release/Evaluation tables, so those objects
// are owned by their separate stores and are not fabricated here.
func (s *S3SessionStore) QueryEvidence(ctx context.Context, query EvidenceQuery) ([]EvidenceRecord, error) {
	query.Cursor = ""
	page, err := s.QueryEvidencePage(ctx, query)
	return page.Records, err
}

func (s *S3SessionStore) QueryEvidencePage(ctx context.Context, query EvidenceQuery) (EvidencePage, error) {
	if err := query.Validate(); err != nil {
		return EvidencePage{}, err
	}
	result, err := s.loadRunEvidenceRecords(ctx, query)
	if err != nil {
		return EvidencePage{}, err
	}
	return paginateEvidenceRecords(query, map[string][]EvidenceRecord{"run": result})
}

func (s *S3SessionStore) RunEvidenceStats(ctx context.Context, query EvidenceQuery) (RunEvidenceStats, error) {
	query.Cursor = ""
	records, err := s.loadRunEvidenceRecords(ctx, query)
	if err != nil {
		return RunEvidenceStats{}, err
	}
	return aggregateRunEvidenceStats(records, query)
}

func (s *S3SessionStore) loadRunEvidenceRecords(ctx context.Context, query EvidenceQuery) ([]EvidenceRecord, error) {
	items, err := s.objects.List(ctx, s3SessionsPrefix, "", 0)
	if err != nil {
		return nil, err
	}
	ids := []string{}
	seen := map[string]bool{}
	for _, item := range items {
		if !strings.HasSuffix(item.Key, "/meta.json") || !strings.HasPrefix(item.Key, s3SessionsPrefix) {
			continue
		}
		id := strings.TrimSuffix(strings.TrimPrefix(item.Key, s3SessionsPrefix), "/meta.json")
		if id == "" || seen[id] || s.validateID(id) != nil {
			continue
		}
		seen[id] = true
		ids = append(ids, id)
	}
	sort.Strings(ids)
	result := []EvidenceRecord{}
	for _, id := range ids {
		metaPayload, _, err := s.objects.Get(ctx, s3MetaKey(id))
		if err != nil {
			return nil, err
		}
		var meta s3SessionMeta
		if err := json.Unmarshal(metaPayload, &meta); err != nil {
			return nil, fmt.Errorf("decode session %s meta for evidence: %w", id, err)
		}
		payload, _, readErr := s.objects.Get(ctx, s3EvidenceKey(id))
		var stored persistedRunEvidence
		if readErr == nil {
			readErr = json.Unmarshal(payload, &stored)
		}
		if readErr != nil || stored.FormatVersion != runEvidenceSidecarVersion || stored.Version != meta.Version {
			session, err := s.loadCanonicalForEvidence(ctx, id, meta)
			if err != nil {
				return nil, err
			}
			if err := s.writeEvidence(ctx, id, session.Version(), sessionOptionsFromSession(session), session.Events()); err != nil {
				return nil, err
			}
			payload, _, err = s.objects.Get(ctx, s3EvidenceKey(id))
			if err != nil {
				return nil, err
			}
			if err := json.Unmarshal(payload, &stored); err != nil {
				return nil, fmt.Errorf("decode rebuilt session %s evidence: %w", id, err)
			}
		}
		result = append(result, filterRunEvidence(stored.Records, query, len(stored.Records))...)
	}
	sortEvidence(result)
	return result, nil
}

func encodeS3EvidenceChunk(events []core.SessionEvent) ([]byte, error) {
	var chunk bytes.Buffer
	for _, event := range events {
		encoded, err := json.Marshal(event)
		if err != nil {
			return nil, err
		}
		chunk.Write(encoded)
		chunk.WriteByte('\n')
	}
	return chunk.Bytes(), nil
}

var _ EvidenceStore = (*S3SessionStore)(nil)
var _ EvidencePager = (*S3SessionStore)(nil)
var _ EvidenceStatsStore = (*S3SessionStore)(nil)
