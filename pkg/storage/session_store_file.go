package storage

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"os"
	"path/filepath"
	"strings"
)

// FileSessionStore persists sessions as one JSONL file per session in a
// directory: the first line holds session ownership metadata, following lines
// hold events. Save appends only the delta since expectedVersion, so it is
// crash-friendly and cheap, and optimistic concurrency rejects lost updates.
//
// Concurrency within one process is guarded by per-session mutexes. Multi-
// process access to the same directory is not supported; deploy one writer or
// provide a database-backed store.
const MaxFileSessionBytes = 64 << 20

type FileSessionStore struct {
	dir          string
	locks        *NamedLocks
	maxSessions  int
	maxFileBytes int
}

func (s *FileSessionStore) sessionCap() int {
	if s != nil && s.maxSessions > 0 {
		return s.maxSessions
	}
	return MaxStoredSessions
}

func (s *FileSessionStore) fileCap() int64 {
	if s != nil && s.maxFileBytes > 0 {
		return int64(s.maxFileBytes)
	}
	return MaxFileSessionBytes
}

func (s *FileSessionStore) sessionFileCount() (int, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".jsonl") {
			count++
		}
	}
	return count, nil
}

func NewFileSessionStore(dir string) (*FileSessionStore, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, fmt.Errorf("session store directory is empty")
	}
	if err := privateMkdirAll(dir); err != nil {
		return nil, fmt.Errorf("create session store directory: %w", err)
	}
	return &FileSessionStore{dir: dir, locks: NewNamedLocks()}, nil
}

type fileSessionHeader struct {
	Options core.SessionOptions `json:"session_options"`
}

func (s *FileSessionStore) sessionPath(id string) (string, error) {
	if err := ValidateSessionIDForFile(id); err != nil {
		return "", err
	}
	return filepath.Join(s.dir, id+".jsonl"), nil
}

// acquireSessionLock registers a reference-counted session lock; the
// returned release drops the reference so the entry can be recycled.
func (s *FileSessionStore) acquireSessionLock(id string) (unlock func()) {
	mutex, release := s.locks.Acquire(id)
	mutex.Lock()
	return func() {
		mutex.Unlock()
		release()
	}
}

func (s *FileSessionStore) Create(ctx context.Context, session *core.Session) error {
	path, err := s.sessionPath(session.ID())
	if err != nil {
		return err
	}
	defer s.acquireSessionLock(session.ID())()
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("%w: %s", core.ErrSessionConflict, session.ID())
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}
	count, err := s.sessionFileCount()
	if err != nil {
		return err
	}
	if count >= s.sessionCap() {
		return fmt.Errorf("stored sessions exceed maximum of %d", s.sessionCap())
	}
	header, err := json.Marshal(fileSessionHeader{Options: core.SessionOptions{
		ID: session.ID(), ProfileID: session.ProfileID(), Principal: session.Principal(),
		Scope: session.Scope(), Metadata: session.Metadata(),
	}})
	if err != nil {
		return err
	}
	var buffer bytes.Buffer
	buffer.Write(header)
	buffer.WriteByte('\n')
	for _, event := range session.Events() {
		if err := appendEventJSON(&buffer, event); err != nil {
			return err
		}
	}
	if int64(buffer.Len()) > s.fileCap() {
		return fmt.Errorf("session file exceeds maximum of %d bytes", s.fileCap())
	}
	if err := s.writeEvidence(ctx, session.ID(), session.Version(), sessionOptionsFromSession(session), session.Events()); err != nil {
		return fmt.Errorf("write session %s evidence: %w", session.ID(), err)
	}
	return privateWriteFile(path, buffer.Bytes())
}

func (s *FileSessionStore) Load(ctx context.Context, id string) (*core.Session, error) {
	path, err := s.sessionPath(id)
	if err != nil {
		return nil, err
	}
	unlock := s.acquireSessionLock(id)
	defer unlock()
	session, loadedCount, err := s.loadLocked(path, id)
	if err != nil {
		return nil, err
	}
	// Repair an interrupted tail so the transcript is provider-valid, then
	// append the synthetic closers directly to the file (never via Save,
	// which would reload under this held lock).
	synthetic := core.RepairInterrupted(session.Events())
	if len(synthetic) > 0 {
		if err := core.AppendRepair(session, synthetic, nil); err != nil {
			return nil, err
		}
		if err := privateChmod(path, 0o600); err != nil {
			return nil, err
		}
		file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			return nil, err
		}
		defer file.Close()
		writer := bufio.NewWriter(file)
		for _, event := range session.Events()[loadedCount:] {
			encoded, err := json.Marshal(event)
			if err != nil {
				return nil, err
			}
			writer.Write(encoded)
			writer.WriteByte('\n')
		}
		if err := writer.Flush(); err != nil {
			return nil, err
		}
		if err := s.rebuildEvidenceForSession(ctx, session); err != nil {
			return nil, fmt.Errorf("write session %s evidence: %w", id, err)
		}
	}
	return session, nil
}

// loadLocked reads and restores the session without any repair logic. The
// second return value is the number of events read from the file.
func (s *FileSessionStore) loadLocked(path, id string) (*core.Session, int, error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, fmt.Errorf("%w: %s", core.ErrSessionNotFound, id)
		}
		return nil, 0, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 1<<20), core.MaxSessionEventDataBytes+(1<<20))
	if !scanner.Scan() {
		return nil, 0, fmt.Errorf("session %s file is truncated", id)
	}
	var header fileSessionHeader
	if err := json.Unmarshal(scanner.Bytes(), &header); err != nil {
		return nil, 0, fmt.Errorf("decode session %s header: %w", id, err)
	}
	events := []core.SessionEvent{}
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var event core.SessionEvent
		if err := json.Unmarshal(line, &event); err != nil {
			return nil, 0, fmt.Errorf("decode session %s event: %w", id, err)
		}
		events = append(events, event)
	}
	if err := scanner.Err(); err != nil {
		return nil, 0, err
	}
	session, err := core.RestoreSession(header.Options, events)
	if err != nil {
		return nil, 0, err
	}
	return session, len(events), nil
}

func (s *FileSessionStore) Save(ctx context.Context, session *core.Session, expectedVersion int64) error {
	if session.Version() < expectedVersion {
		return fmt.Errorf("session %s history shrank from %d to %d events", session.ID(), expectedVersion, session.Version())
	}
	return s.AppendEvents(ctx, session.ID(), expectedVersion, session.EventsFrom(expectedVersion))
}

func (s *FileSessionStore) AppendEvents(ctx context.Context, sessionID string, expectedVersion int64, events []core.SessionEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateAppendEvents(expectedVersion, events); err != nil {
		return err
	}
	path, err := s.sessionPath(sessionID)
	if err != nil {
		return err
	}
	defer s.acquireSessionLock(sessionID)()

	loadedSession, loadedCount, err := s.loadLocked(path, sessionID)
	if err != nil {
		return err
	}
	storedVersion := int64(loadedCount)
	if storedVersion != expectedVersion {
		return fmt.Errorf("%w: expected %d, found %d", core.ErrSessionConflict, expectedVersion, storedVersion)
	}
	var extra bytes.Buffer
	for _, event := range events {
		if err := appendEventJSON(&extra, event); err != nil {
			return err
		}
		if extra.Len() > MaxEventChunkBytes {
			return fmt.Errorf("event chunk exceeds %d bytes", MaxEventChunkBytes)
		}
	}
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.Size()+int64(extra.Len()) > s.fileCap() {
		return fmt.Errorf("session file exceeds maximum of %d bytes", s.fileCap())
	}
	if extra.Len() > 0 {
		allEvents := append(loadedSession.Events(), events...)
		if err := s.writeEvidence(ctx, sessionID, expectedVersion+int64(len(events)), sessionOptionsFromSession(loadedSession), allEvents); err != nil {
			return fmt.Errorf("write session %s evidence: %w", sessionID, err)
		}
	}

	if err := privateChmod(path, 0o600); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err := file.Write(extra.Bytes()); err != nil {
		return err
	}
	return file.Sync()
}

func appendEventJSON(buffer *bytes.Buffer, event core.SessionEvent) error {
	encoded, err := json.Marshal(event)
	if err != nil {
		return err
	}
	buffer.Write(encoded)
	buffer.WriteByte('\n')
	return nil
}
