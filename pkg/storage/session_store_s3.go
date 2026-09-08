package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	core "github.com/whhhh1500/auto-agent/pkg/core"
	"path"
	"sort"
	"strings"
	"time"
)

// S3SessionStore persists sessions on any ObjectStore (S3, MinIO, R2, a local
// disk) using an immutable-chunk layout:
//
//	sessions/{id}/meta.json           version + header; the commit point
//	sessions/{id}/events/{seq:012d}.jsonl   one chunk per Save batch
//
// Each Save writes the event delta as one new immutable chunk, updates the
// derived evidence sidecar, then advances meta.json with a conditional PUT
// (the CAS point). A meta failure leaves orphan objects at a seq that the next
// successful Save overwrites, so recovery is always "read meta, then trust
// chunks/evidence only up to meta.version". Create reserves meta first when
// initial events are present, preventing a duplicate create from overwriting
// an existing chunk.
type S3SessionStore struct {
	objects ObjectStore
	// DisableConditionalWrites opts out of If-Match CAS for object stores
	// without conditional-write support. Only enable it behind an external
	// single-writer lease: concurrent writers can then lose updates silently.
	DisableConditionalWrites bool
	maxSessions              int
}

func NewS3SessionStore(objects ObjectStore) (*S3SessionStore, error) {
	if objects == nil {
		return nil, fmt.Errorf("s3 session store requires an object store")
	}
	return &S3SessionStore{objects: objects}, nil
}

type s3SessionMeta struct {
	Version      int64               `json:"version"`
	Header       core.SessionOptions `json:"header"`
	UpdatedAt    time.Time           `json:"updated_at"`
	Initializing bool                `json:"initializing,omitempty"`
}

const s3SessionsPrefix = "sessions/"

func s3MetaKey(id string) string      { return s3SessionsPrefix + id + "/meta.json" }
func s3EventsPrefix(id string) string { return s3SessionsPrefix + id + "/events/" }

func (s *S3SessionStore) validateID(id string) error {
	if err := core.ValidateSessionID(id); err != nil {
		return fmt.Errorf("invalid session id: %w", err)
	}
	if strings.ContainsAny(id, "/\\") || strings.Contains(id, "..") {
		return fmt.Errorf("invalid session id %q", id)
	}
	return nil
}

func (s *S3SessionStore) sessionCap() int {
	if s != nil && s.maxSessions > 0 {
		return s.maxSessions
	}
	return MaxStoredSessions
}

func (s *S3SessionStore) sessionCount(ctx context.Context) (int, error) {
	count := 0
	startAfter := ""
	for {
		items, err := s.objects.List(ctx, s3SessionsPrefix, startAfter, 256)
		if err != nil {
			return 0, err
		}
		if len(items) == 0 {
			return count, nil
		}
		for _, item := range items {
			if strings.HasSuffix(item.Key, "/meta.json") {
				count++
				if count >= s.sessionCap() {
					return count, nil
				}
			}
			startAfter = item.Key
		}
		if len(items) < 256 {
			return count, nil
		}
	}
}

func (s *S3SessionStore) Create(ctx context.Context, session *core.Session) error {
	if err := s.validateID(session.ID()); err != nil {
		return err
	}
	if _, _, err := s.objects.Get(ctx, s3MetaKey(session.ID())); err == nil {
		return fmt.Errorf("%w: %s", core.ErrSessionConflict, session.ID())
	} else if !errors.Is(err, ErrObjectNotFound) {
		return err
	}
	stored, err := s.sessionCount(ctx)
	if err != nil {
		return err
	}
	if stored >= s.sessionCap() {
		return fmt.Errorf("stored sessions exceed maximum of %d", s.sessionCap())
	}
	meta := s3SessionMeta{
		Version: 0, Initializing: len(session.Events()) > 0,
		Header: core.SessionOptions{
			ID: session.ID(), ProfileID: session.ProfileID(), Principal: session.Principal(),
			Scope: session.Scope(), Metadata: session.Metadata(),
		},
		UpdatedAt: time.Now().UTC(),
	}
	payload, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	opts := PutOptions{IfNoneMatchStar: true}
	if s.DisableConditionalWrites {
		// Best-effort create: refuse when the meta object already exists.
		if _, _, err := s.objects.Get(ctx, s3MetaKey(session.ID())); err == nil {
			return fmt.Errorf("%w: %s", core.ErrSessionConflict, session.ID())
		} else if !errors.Is(err, ErrObjectNotFound) {
			return err
		}
		opts = PutOptions{}
	}
	metaETag, err := s.objects.Put(ctx, s3MetaKey(session.ID()), func() []byte {
		if len(session.Events()) == 0 {
			return payload
		}
		pendingPayload, _ := json.Marshal(meta)
		return pendingPayload
	}(), opts)
	if errors.Is(err, ErrPreconditionFailed) {
		return fmt.Errorf("%w: %s", core.ErrSessionConflict, session.ID())
	}
	if err != nil {
		return err
	}
	if events := session.Events(); len(events) > 0 {
		chunk, err := encodeS3EvidenceChunk(events)
		if err != nil {
			return err
		}
		if _, err := s.objects.Put(ctx, chunkKey(session.ID(), 0), chunk, PutOptions{}); err != nil {
			return err
		}
		if err := s.writeEvidence(ctx, session.ID(), int64(len(events)), meta.Header, events); err != nil {
			return err
		}
		meta.Version, meta.Initializing = int64(len(events)), false
		finalPayload, err := json.Marshal(meta)
		if err != nil {
			return err
		}
		finalOptions := PutOptions{IfMatch: metaETag}
		if s.DisableConditionalWrites {
			finalOptions = PutOptions{}
		}
		if _, err := s.objects.Put(ctx, s3MetaKey(session.ID()), finalPayload, finalOptions); err != nil {
			if errors.Is(err, ErrPreconditionFailed) {
				return fmt.Errorf("%w: %s", core.ErrSessionConflict, session.ID())
			}
			return err
		}
	}
	return nil
}

func (s *S3SessionStore) Load(ctx context.Context, id string) (*core.Session, error) {
	if err := s.validateID(id); err != nil {
		return nil, err
	}
	session, _, err := s.loadLocked(ctx, id)
	return session, err
}

// loadLocked returns the restored session plus its meta etag (unused for
// reads, useful to callers that immediately save).
func (s *S3SessionStore) loadLocked(ctx context.Context, id string) (*core.Session, string, error) {
	metaPayload, metaETag, err := s.objects.Get(ctx, s3MetaKey(id))
	if err != nil {
		if errors.Is(err, ErrObjectNotFound) {
			return nil, "", fmt.Errorf("%w: %s", core.ErrSessionNotFound, id)
		}
		return nil, "", err
	}
	var meta s3SessionMeta
	if err := json.Unmarshal(metaPayload, &meta); err != nil {
		return nil, "", fmt.Errorf("decode session %s meta: %w", id, err)
	}
	if meta.Initializing {
		return nil, "", fmt.Errorf("session %s is still initializing", id)
	}
	if err := validateCommittedSessionVersion(id, meta.Version); err != nil {
		return nil, "", err
	}

	items, err := s.objects.List(ctx, s3EventsPrefix(id), "", 0)
	if err != nil {
		return nil, "", err
	}
	events := []core.SessionEvent{}
	for _, chunkKey := range sessionEventChunkKeys(id, items) {
		if int64(len(events)) >= meta.Version {
			break
		}
		body, _, err := s.objects.Get(ctx, chunkKey)
		if err != nil {
			return nil, "", err
		}
		for _, line := range bytes.Split(bytes.TrimSpace(body), []byte{'\n'}) {
			line = bytes.TrimSpace(line)
			if len(line) == 0 {
				continue
			}
			if len(line) > core.MaxSessionEventDataBytes+(1<<20) {
				return nil, "", fmt.Errorf("decode session %s event: encoded event exceeds size limit", id)
			}
			var event core.SessionEvent
			if err := json.Unmarshal(line, &event); err != nil {
				return nil, "", fmt.Errorf("decode session %s event: %w", id, err)
			}
			events = append(events, event)
			if int64(len(events)) >= meta.Version {
				break
			}
		}
	}
	// Trust only the prefix meta.version covers: an interrupted Save can leave
	// an orphan chunk beyond the committed version.
	if int64(len(events)) < meta.Version {
		return nil, "", fmt.Errorf("session %s committed version %d exceeds restored event count %d", id, meta.Version, len(events))
	}
	session, err := core.RestoreSession(meta.Header, events)
	if err != nil {
		return nil, "", err
	}
	return session, metaETag, nil
}

func (s *S3SessionStore) Save(ctx context.Context, session *core.Session, expectedVersion int64) error {
	if session.Version() < expectedVersion {
		return fmt.Errorf("session %s history shrank from %d to %d events", session.ID(), expectedVersion, session.Version())
	}
	return s.AppendEvents(ctx, session.ID(), expectedVersion, session.EventsFrom(expectedVersion))
}

func (s *S3SessionStore) AppendEvents(ctx context.Context, sessionID string, expectedVersion int64, events []core.SessionEvent) error {
	if err := validateAppendEvents(expectedVersion, events); err != nil {
		return err
	}
	if err := s.validateID(sessionID); err != nil {
		return err
	}
	metaPayload, metaETag, err := s.objects.Get(ctx, s3MetaKey(sessionID))
	if err != nil {
		if errors.Is(err, ErrObjectNotFound) {
			return fmt.Errorf("%w: %s", core.ErrSessionNotFound, sessionID)
		}
		return err
	}
	var meta s3SessionMeta
	if err := json.Unmarshal(metaPayload, &meta); err != nil {
		return fmt.Errorf("decode session %s meta: %w", sessionID, err)
	}
	if meta.Version != expectedVersion {
		return fmt.Errorf("%w: expected %d, found %d", core.ErrSessionConflict, expectedVersion, meta.Version)
	}
	if len(events) > 0 {
		chunk, err := encodeS3EvidenceChunk(events)
		if err != nil {
			return err
		}
		// Unconditional: an orphan from a previously failed commit is
		// overwritten by this successful one.
		if _, err := s.objects.Put(ctx, chunkKey(sessionID, expectedVersion), chunk, PutOptions{}); err != nil {
			return err
		}
		existing, err := s.loadCanonicalForEvidence(ctx, sessionID, meta)
		if err != nil {
			return err
		}
		allEvents := append(existing.Events(), events...)
		if err := s.writeEvidence(ctx, sessionID, expectedVersion+int64(len(events)), meta.Header, allEvents); err != nil {
			return err
		}
	}

	updated := meta
	updated.Version = expectedVersion + int64(len(events))
	updated.UpdatedAt = time.Now().UTC()
	updatedPayload, err := json.Marshal(updated)
	if err != nil {
		return err
	}
	opts := PutOptions{IfMatch: metaETag}
	if s.DisableConditionalWrites {
		opts = PutOptions{}
	}
	if _, err := s.objects.Put(ctx, s3MetaKey(sessionID), updatedPayload, opts); err != nil {
		if errors.Is(err, ErrPreconditionFailed) {
			return fmt.Errorf("%w: expected %d, found a newer meta", core.ErrSessionConflict, expectedVersion)
		}
		return err
	}
	return nil
}

func chunkKey(id string, seq int64) string {
	return path.Join(s3EventsPrefix(id), fmt.Sprintf("%012d.jsonl", seq))
}

// sessionEventChunkKeys turns a list containing plain objects, compressed
// siblings, or both (after an interrupted sweep) into ordered logical JSONL
// keys. Reads use the logical key so ObjectStore can prefer plain data or
// transparently decode its cold sibling.
func sessionEventChunkKeys(id string, items []ObjectItem) []string {
	keys := make([]string, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		key := strings.TrimSuffix(item.Key, ".zst")
		chunkID, _, ok := sessionEventObjectKey(key)
		if !ok || chunkID != id {
			continue
		}
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
