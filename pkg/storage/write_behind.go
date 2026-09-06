package storage

import (
	"context"
	"errors"
	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"sync"
	"time"
)

// WriteBehind bounds the durability window of a live run: events land in the
// session immediately, and a background flusher persists a stable ordered
// prefix to the core.SessionStore at most one batching delay behind the producer.
// A crash loses at most maxDelay of events instead of the whole run, while
// the hot path never blocks on storage.
//
// Single-writer discipline applies: the caller must already serialize runs
// per session (the HTTP adapter holds a per-session lock). Conflicting
// external writers surface as ErrSessionConflict on Flush.
type WriteBehind struct {
	store              core.SessionStore
	session            *core.Session
	maxDelay           time.Duration
	backgroundDisabled bool

	mu           sync.Mutex
	savedVersion int64
	dirty        bool
	timer        *time.Timer
	flushing     bool
	lastErr      error
	closed       bool
	changed      chan struct{}
}

var backgroundFlushTimeout = 30 * time.Second

// NewWriteBehind starts batching for one run. startVersion is the store
// version the session was loaded at; everything after it is pending. A
// negative maxDelay disables background batching so only Flush writes.
func NewWriteBehind(store core.SessionStore, session *core.Session, startVersion int64, maxDelay time.Duration) *WriteBehind {
	backgroundDisabled := maxDelay < 0
	if maxDelay == 0 {
		maxDelay = 200 * time.Millisecond
	}
	return &WriteBehind{
		store: store, session: session, maxDelay: maxDelay,
		backgroundDisabled: backgroundDisabled,
		savedVersion:       startVersion,
		changed:            make(chan struct{}),
	}
}

// MarkDirty schedules a background flush within maxDelay. It is safe to call
// for every emitted event; scheduling is idempotent while a flush is pending.
func (w *WriteBehind) MarkDirty() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return
	}
	w.dirty = true
	w.scheduleLocked()
}

// Checkpoint persists everything pending synchronously without closing the
// writer. Later MarkDirty calls continue background batching.
func (w *WriteBehind) Checkpoint(ctx context.Context) error {
	return w.flush(ctx, false)
}

// Flush persists everything pending synchronously and stops background
// flushing. It must be called before the run's transport response completes
// so a durable prefix is on disk before the client stops listening.
func (w *WriteBehind) Flush(ctx context.Context) error {
	return w.flush(ctx, true)
}

func (w *WriteBehind) flush(ctx context.Context, closeWriter bool) error {
	w.mu.Lock()
	if w.closed && !closeWriter {
		err := w.lastErr
		w.mu.Unlock()
		return err
	}
	if closeWriter {
		w.closed = true
	}
	if w.session != nil && w.session.Version() > w.savedVersion {
		w.dirty = true
	}
	if w.timer != nil {
		w.timer.Stop()
		w.timer = nil
	}
	for {
		if w.lastErr != nil {
			err := w.lastErr
			w.mu.Unlock()
			return err
		}
		if !w.dirty {
			w.mu.Unlock()
			return nil
		}
		if w.flushing {
			changed := w.changed
			w.mu.Unlock()
			select {
			case <-changed:
			case <-ctx.Done():
				return ctx.Err()
			}
			w.mu.Lock()
			continue
		}
		w.mu.Unlock()
		w.flushOnce(ctx)
		w.mu.Lock()
	}
}

// Abort stops future background flushes without persisting the remaining
// dirty suffix. It waits for an already-running flush to return so callers do
// not release a session lease while this writer can still mutate the store.
func (w *WriteBehind) Abort(ctx context.Context) error {
	w.mu.Lock()
	w.closed = true
	if w.timer != nil {
		w.timer.Stop()
		w.timer = nil
	}
	for w.flushing {
		changed := w.changed
		w.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
		w.mu.Lock()
	}
	err := w.lastErr
	w.mu.Unlock()
	return err
}

func (w *WriteBehind) flushOnce(ctx context.Context) {
	w.mu.Lock()
	if !w.dirty || w.flushing {
		w.mu.Unlock()
		return
	}
	w.flushing = true
	session, savedVersion := w.session, w.savedVersion
	w.mu.Unlock()

	events := session.EventsFrom(savedVersion)
	targetVersion := savedVersion + int64(len(events))
	var err error
	if appender, ok := w.store.(core.SessionAppender); ok {
		err = appender.AppendEvents(ctx, session.ID(), savedVersion, events)
	} else {
		var snapshot *core.Session
		snapshot, err = session.Clone()
		if err == nil {
			targetVersion = snapshot.Version()
			err = w.store.Save(ctx, snapshot, savedVersion)
		}
	}

	w.mu.Lock()
	w.flushing = false
	if err != nil {
		// Retain the failure: Flush reports it instead of silently dropping.
		if w.lastErr == nil {
			w.lastErr = err
		}
	} else {
		w.savedVersion = targetVersion
		w.dirty = session.Version() > targetVersion
		if w.timer != nil {
			w.timer.Stop()
			w.timer = nil
		}
		w.scheduleLocked()
	}
	w.signalChangedLocked()
	w.mu.Unlock()
}

func (w *WriteBehind) scheduleLocked() {
	if w.backgroundDisabled || w.closed || !w.dirty || w.flushing || w.timer != nil || w.lastErr != nil {
		return
	}
	var timer *time.Timer
	timer = time.AfterFunc(w.maxDelay, func() {
		w.mu.Lock()
		if w.timer == timer {
			w.timer = nil
		}
		w.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), backgroundFlushTimeout)
		defer cancel()
		w.flushOnce(ctx)
	})
	w.timer = timer
}

func (w *WriteBehind) signalChangedLocked() {
	close(w.changed)
	w.changed = make(chan struct{})
}

// SavedVersion reports the version known to be durable.
func (w *WriteBehind) SavedVersion() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.savedVersion
}

// IsConflict reports whether an error from Flush is a persistence conflict
// rather than an infrastructure failure.
func IsConflict(err error) bool {
	return errors.Is(err, core.ErrSessionConflict)
}
