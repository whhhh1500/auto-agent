package core

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	ErrSessionNotFound = errors.New("session not found")
	ErrSessionConflict = errors.New("session version conflict")
)

// MaxMemorySessions bounds the in-process reference SessionStore. Durable
// adapters apply their own retention; this cap exists so a demo or test
// process cannot grow the map without limit.
const MaxMemorySessions = 4096

// SessionStore persists complete session snapshots with optimistic concurrency.
// Load is a pure read: recovery and other lifecycle transitions must be
// requested explicitly by an execution owner.
type SessionStore interface {
	Create(ctx context.Context, session *Session) error
	Load(ctx context.Context, id string) (*Session, error)
	Save(ctx context.Context, session *Session, expectedVersion int64) error
}

// SessionAppender is the efficient append-only persistence path. Stores that
// implement it receive only the new event suffix, avoiding a full Session
// clone on every write-behind flush. SessionStore.Save remains the fallback
// for simple adapters.
type SessionAppender interface {
	AppendEvents(ctx context.Context, sessionID string, expectedVersion int64, events []SessionEvent) error
}

// MemorySessionStore is a concurrency-safe reference implementation.
type MemorySessionStore struct {
	mu          sync.RWMutex
	sessions    map[string]*Session
	maxSessions int
}

func NewMemorySessionStore() *MemorySessionStore {
	return &MemorySessionStore{sessions: map[string]*Session{}}
}

func (s *MemorySessionStore) Create(_ context.Context, session *Session) error {
	copyOf, err := session.Clone()
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.sessions[session.ID()]; exists {
		return fmt.Errorf("%w: %s", ErrSessionConflict, session.ID())
	}
	if len(s.sessions) >= s.sessionCap() {
		return fmt.Errorf("memory sessions exceed maximum of %d", s.sessionCap())
	}
	s.sessions[session.ID()] = copyOf
	return nil
}

func (s *MemorySessionStore) sessionCap() int {
	if s != nil && s.maxSessions > 0 {
		return s.maxSessions
	}
	return MaxMemorySessions
}

func (s *MemorySessionStore) Load(_ context.Context, id string) (*Session, error) {
	s.mu.RLock()
	stored := s.sessions[id]
	s.mu.RUnlock()
	if stored == nil {
		return nil, fmt.Errorf("%w: %s", ErrSessionNotFound, id)
	}
	return stored.Clone()
}

func (s *MemorySessionStore) Save(_ context.Context, session *Session, expectedVersion int64) error {
	copyOf, err := session.Clone()
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	stored := s.sessions[session.ID()]
	if stored == nil {
		return fmt.Errorf("%w: %s", ErrSessionNotFound, session.ID())
	}
	if stored.Version() != expectedVersion {
		return fmt.Errorf("%w: expected %d, found %d", ErrSessionConflict, expectedVersion, stored.Version())
	}
	s.sessions[session.ID()] = copyOf
	return nil
}
